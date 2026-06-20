//! Tier-A execution-trace projection (process-optimization design doc §3).
//!
//! A [`TraceStore`] consumes the engine's event stream — the *same* ordered
//! `Arc<Vec<Event>>` batches the read-model exporter already processes — and
//! folds the flat events into per-instance, per-element traces. This is the
//! "observe" leg of the optimization loop: a faithful record a human, an LLM and
//! a static analyzer can all read.
//!
//! Design notes:
//! - **In-memory and bounded.** T1 is observability / demo / debug, so the store
//!   keeps the most-recent `capacity` instances (a ring) rather than persisting
//!   every trace. Configure with `NANOBPMN_TRACE_CAPACITY` (default 2000).
//! - **Timing is ingestion time.** The engine does not stamp a wall clock onto
//!   most events (its clock is *injected* — the determinism property), so the
//!   projection stamps each exporter batch with the server's observation time.
//!   That is exact enough for job lifetime because the transitions
//!   (`JobCreated` → `JobCompleted`) occur in *separate* commands at genuinely
//!   different instants. Bit-exact engine-clock timing is a Tier-B (recorded-input
//!   replay) concern, not T1.
//! - **Activation is not observed (yet).** Job *activation* locks are ephemeral:
//!   `Journal::activate_jobs` applies the lock in-engine but does **not** journal
//!   or export a `JobActivated` event (a crashed lease simply re-activates on
//!   restart). So `activatedAt` / `worker` / `queueMs` / `serviceMs` stay `null`
//!   on this projection; the reliably-available signal is `waitMs`
//!   (`createdAt` → `completedAt`, the job's total parked + service time). The
//!   queue-vs-service split (design doc §3 / Fig 2) needs a dedicated
//!   activation-side trace hook and is the immediate next increment — left out of
//!   T1 because it touches the high-frequency activation path. The `JobActivated`
//!   fold below is kept so the split lights up for free once activation is
//!   exported.
//! - **No hot-path cost.** Ingestion runs on the exporter thread, already off the
//!   command-commit/ack path. The default (no-`console`) build never compiles this
//!   module.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use nanobpmn_engine_core::Event;
use serde::Serialize;

const DEFAULT_CAPACITY: usize = 2000;

/// A bounded, in-memory projection of recent process-instance execution traces.
pub struct TraceStore {
    inner: Mutex<Inner>,
}

struct Inner {
    capacity: usize,
    /// Trace per instance key.
    instances: HashMap<u64, InstanceTrace>,
    /// Instance keys in insertion order (oldest at the front) for ring eviction.
    order: VecDeque<u64>,
    /// Latest deployed version per process id (best-effort; an instance's exact
    /// definition version is not carried on `ProcessInstanceCreated`).
    versions: HashMap<String, i32>,
    /// Activations observed before their `JobCreated` was folded. Job activation
    /// is not journaled/exported (see module docs), so it is fed directly from the
    /// activation chokepoint via [`TraceStore::record_activations`]; because the
    /// exporter is async, a fast worker can activate before the create batch
    /// projects. Buffered here (keyed by job key) and drained on fold.
    pending_acts: HashMap<u64, PendingAct>,
}

/// A job activation seen before its `JobCreated` reached the projection.
struct PendingAct {
    worker: String,
    activated_at: u64,
    attempts: u32,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Active,
    Completed,
    Terminated,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Outcome::Active => "active",
            Outcome::Completed => "completed",
            Outcome::Terminated => "terminated",
        }
    }
}

#[derive(Clone)]
struct InstanceTrace {
    instance_key: u64,
    process_id: String,
    version: Option<i32>,
    business_id: Option<String>,
    tags: Vec<String>,
    started_at: u64,
    ended_at: Option<u64>,
    /// Last time any event touched this instance (used to bound open spans).
    last_at: u64,
    outcome: Outcome,
    elements: Vec<ElementTrace>,
    by_eik: HashMap<u64, usize>,
    by_job: HashMap<u64, usize>,
    incidents: Vec<IncidentRec>,
    path: Vec<String>,
}

#[derive(Clone)]
struct ElementTrace {
    element_id: String,
    element_instance_key: u64,
    scope: u64,
    entered_at: u64,
    exited_at: Option<u64>,
    incidents: u32,
    job: Option<JobTrace>,
}

#[derive(Clone)]
struct JobTrace {
    job_key: u64,
    job_type: String,
    worker: Option<String>,
    created_at: u64,
    activated_at: Option<u64>,
    completed_at: Option<u64>,
    attempts: u32,
    failures: u32,
}

#[derive(Clone)]
struct IncidentRec {
    element_id: String,
    element_instance_key: u64,
    kind: String,
    reason: String,
    raised_at: u64,
    resolved_at: Option<u64>,
}

impl TraceStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                capacity: capacity.max(1),
                instances: HashMap::new(),
                order: VecDeque::new(),
                versions: HashMap::new(),
                pending_acts: HashMap::new(),
            }),
        }
    }

    /// Builds a store sized from `NANOBPMN_TRACE_CAPACITY` (default 2000).
    pub fn from_env() -> Self {
        let cap = std::env::var("NANOBPMN_TRACE_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_CAPACITY);
        Self::new(cap)
    }

    /// Folds one exporter batch into the trace store. `now` is the server's
    /// observation time for this batch (ms since the Unix epoch).
    pub fn ingest(&self, events: &[&Event], now: u64) {
        let mut inner = self.inner.lock().unwrap();
        for ev in events {
            inner.apply(ev, now);
        }
    }

    /// Records a batch of job activations — `(instance_key, job_key)` pairs all
    /// locked to `worker` at observation time `now`. Activation locks are
    /// ephemeral (never journaled/exported), so this is fed straight from the
    /// activation chokepoint; it is what lets `queueMs` / `serviceMs` / `worker`
    /// / `attempts` populate. Tolerant of the exporter race: an activation seen
    /// before its `JobCreated` is buffered and applied on fold.
    pub fn record_activations(&self, acts: &[(u64, u64)], worker: &str, now: u64) {
        if acts.is_empty() {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        for &(instance_key, job_key) in acts {
            inner.record_activation(instance_key, job_key, worker, now);
        }
    }

    /// Recent instance summaries, most-recent first, capped at `limit`.
    pub fn list(&self, limit: usize) -> Vec<TraceSummaryDto> {
        let inner = self.inner.lock().unwrap();
        inner
            .order
            .iter()
            .rev()
            .filter_map(|k| inner.instances.get(k))
            .take(limit)
            .map(InstanceTrace::summary)
            .collect()
    }

    /// The full trace for one instance (design doc §3 shape).
    pub fn get(&self, instance_key: u64) -> Option<InstanceTraceDto> {
        let inner = self.inner.lock().unwrap();
        inner.instances.get(&instance_key).map(InstanceTrace::dto)
    }

    /// The instance trace as an OTLP/JSON trace document (resource → scope →
    /// spans): one root span for the instance, a child span per element instance,
    /// and a grandchild span per job. Ingestible by an OpenTelemetry collector.
    pub fn otel(&self, instance_key: u64) -> Option<serde_json::Value> {
        let inner = self.inner.lock().unwrap();
        inner.instances.get(&instance_key).map(InstanceTrace::otel)
    }
}

impl Inner {
    fn apply(&mut self, ev: &Event, now: u64) {
        match ev {
            Event::ProcessDeployed {
                version, process, ..
            } => {
                let entry = self.versions.entry(process.id.clone()).or_insert(*version);
                if *version > *entry {
                    *entry = *version;
                }
            }
            Event::ProcessInstanceCreated {
                instance_key,
                process_id,
                created_at,
                tags,
                business_id,
                ..
            } => {
                let version = self.versions.get(process_id).copied();
                let started = if *created_at != 0 { *created_at } else { now };
                let trace = InstanceTrace {
                    instance_key: *instance_key,
                    process_id: process_id.clone(),
                    version,
                    business_id: business_id.clone(),
                    tags: tags.clone(),
                    started_at: started,
                    ended_at: None,
                    last_at: now,
                    outcome: Outcome::Active,
                    elements: Vec::new(),
                    by_eik: HashMap::new(),
                    by_job: HashMap::new(),
                    incidents: Vec::new(),
                    path: Vec::new(),
                };
                self.insert(*instance_key, trace);
            }
            Event::ElementActivating {
                instance_key,
                element_instance_key,
                element_id,
            } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    let fresh = !t.by_eik.contains_key(element_instance_key);
                    t.element_mut(*element_instance_key, element_id, now);
                    if fresh {
                        t.path.push(element_id.clone());
                    }
                }
            }
            Event::ElementActivated {
                instance_key,
                element_instance_key,
                element_id,
                scope,
            } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    let idx = t.element_mut(*element_instance_key, element_id, now);
                    t.elements[idx].scope = *scope;
                }
            }
            Event::ElementCompleted {
                instance_key,
                element_instance_key,
                ..
            } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    if let Some(&idx) = t.by_eik.get(element_instance_key) {
                        t.elements[idx].exited_at = Some(now);
                    }
                }
            }
            Event::JobCreated {
                job_key,
                instance_key,
                element_instance_key,
                element_id,
                job_type,
                ..
            } => {
                // Apply any activation that raced ahead of this fold (see
                // `pending_acts`). Removed before borrowing the instance.
                let pending = self.pending_acts.remove(job_key);
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    let idx = t.element_mut(*element_instance_key, element_id, now);
                    let mut job = JobTrace {
                        job_key: *job_key,
                        job_type: job_type.clone(),
                        worker: None,
                        created_at: now,
                        activated_at: None,
                        completed_at: None,
                        attempts: 0,
                        failures: 0,
                    };
                    if let Some(p) = pending {
                        job.worker = Some(p.worker);
                        job.activated_at = Some(p.activated_at);
                        job.attempts = p.attempts;
                    }
                    t.elements[idx].job = Some(job);
                    t.by_job.insert(*job_key, idx);
                }
            }
            Event::JobActivated {
                job_key,
                instance_key,
                worker,
                ..
            } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    if let Some(&idx) = t.by_job.get(job_key) {
                        if let Some(job) = t.elements[idx].job.as_mut() {
                            job.attempts += 1;
                            job.worker = Some(worker.clone());
                            if job.activated_at.is_none() {
                                job.activated_at = Some(now);
                            }
                        }
                    }
                }
            }
            Event::JobCompleted {
                job_key,
                instance_key,
            } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    if let Some(&idx) = t.by_job.get(job_key) {
                        if let Some(job) = t.elements[idx].job.as_mut() {
                            job.completed_at = Some(now);
                        }
                    }
                }
            }
            Event::JobFailed {
                job_key,
                instance_key,
                ..
            } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    if let Some(&idx) = t.by_job.get(job_key) {
                        if let Some(job) = t.elements[idx].job.as_mut() {
                            job.failures += 1;
                        }
                    }
                }
            }
            Event::IncidentRaised {
                instance_key,
                element_instance_key,
                element_id,
                kind,
                reason,
                created_at,
                ..
            } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    let raised = if *created_at != 0 { *created_at } else { now };
                    if let Some(&idx) = t.by_eik.get(element_instance_key) {
                        t.elements[idx].incidents += 1;
                    }
                    t.incidents.push(IncidentRec {
                        element_id: element_id.clone(),
                        element_instance_key: *element_instance_key,
                        kind: format!("{kind:?}"),
                        reason: reason.clone(),
                        raised_at: raised,
                        resolved_at: None,
                    });
                }
            }
            Event::IncidentResolved {
                instance_key,
                resolved_at,
                ..
            } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    let when = if *resolved_at != 0 { *resolved_at } else { now };
                    if let Some(rec) = t.incidents.iter_mut().rev().find(|r| r.resolved_at.is_none())
                    {
                        rec.resolved_at = Some(when);
                    }
                }
            }
            Event::ProcessInstanceCompleted { instance_key } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    t.ended_at = Some(now);
                    t.outcome = Outcome::Completed;
                }
            }
            Event::ProcessInstanceTerminated { instance_key } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    t.ended_at = Some(now);
                    t.outcome = Outcome::Terminated;
                }
            }
            _ => {}
        }
    }

    /// Inserts a new instance trace, evicting the oldest if over capacity.
    fn insert(&mut self, key: u64, trace: InstanceTrace) {
        if self.instances.insert(key, trace).is_none() {
            self.order.push_back(key);
        }
        while self.order.len() > self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.instances.remove(&old);
            }
        }
    }

    /// Folds one job activation into its trace, or buffers it if the matching
    /// `JobCreated` has not been projected yet (exporter race). `activated_at` is
    /// set on the first activation only (so `queueMs` reflects the initial queue
    /// wait); `attempts` counts every activation (lease re-activations included).
    fn record_activation(&mut self, instance_key: u64, job_key: u64, worker: &str, now: u64) {
        if let Some(t) = self.instances.get_mut(&instance_key) {
            if let Some(&idx) = t.by_job.get(&job_key) {
                if let Some(job) = t.elements[idx].job.as_mut() {
                    job.attempts += 1;
                    job.worker = Some(worker.to_string());
                    if job.activated_at.is_none() {
                        job.activated_at = Some(now);
                    }
                    if now > t.last_at {
                        t.last_at = now;
                    }
                    return;
                }
            }
        }
        // JobCreated not folded yet (or the instance was evicted): buffer it.
        let entry = self.pending_acts.entry(job_key).or_insert(PendingAct {
            worker: worker.to_string(),
            activated_at: now,
            attempts: 0,
        });
        entry.attempts += 1;
        entry.worker = worker.to_string();
        if now < entry.activated_at {
            entry.activated_at = now;
        }
        // Defensive bound: stale entries only lose activation metadata for a
        // since-evicted job, never correctness.
        if self.pending_acts.len() > self.capacity.saturating_mul(4) {
            if let Some(&k) = self.pending_acts.keys().find(|&&k| k != job_key) {
                self.pending_acts.remove(&k);
            }
        }
    }
}

impl InstanceTrace {
    /// Returns the index of the element instance, creating it on first sight.
    fn element_mut(&mut self, eik: u64, element_id: &str, now: u64) -> usize {
        if let Some(&idx) = self.by_eik.get(&eik) {
            return idx;
        }
        let idx = self.elements.len();
        self.elements.push(ElementTrace {
            element_id: element_id.to_string(),
            element_instance_key: eik,
            scope: 0,
            entered_at: now,
            exited_at: None,
            incidents: 0,
            job: None,
        });
        self.by_eik.insert(eik, idx);
        idx
    }

    fn summary(&self) -> TraceSummaryDto {
        TraceSummaryDto {
            instance_key: self.instance_key.to_string(),
            process_id: self.process_id.clone(),
            version: self.version,
            business_id: self.business_id.clone(),
            outcome: self.outcome.as_str(),
            started_at: self.started_at,
            ended_at: self.ended_at,
            duration_ms: self.ended_at.map(|e| e.saturating_sub(self.started_at)),
            element_count: self.elements.len(),
            incident_count: self.incidents.len(),
        }
    }

    fn dto(&self) -> InstanceTraceDto {
        InstanceTraceDto {
            instance_key: self.instance_key.to_string(),
            process_id: self.process_id.clone(),
            version: self.version,
            business_id: self.business_id.clone(),
            tags: self.tags.clone(),
            started_at: self.started_at,
            ended_at: self.ended_at,
            duration_ms: self.ended_at.map(|e| e.saturating_sub(self.started_at)),
            outcome: self.outcome.as_str(),
            elements: self
                .elements
                .iter()
                .map(|e| {
                    let job = e.job.as_ref().map(|j| JobDto {
                        job_type: j.job_type.clone(),
                        worker: j.worker.clone(),
                        created_at: j.created_at,
                        activated_at: j.activated_at,
                        completed_at: j.completed_at,
                        // Total parked + service time. Reliable because both
                        // endpoints are exported (unlike activation).
                        wait_ms: j
                            .completed_at
                            .map(|c| c.saturating_sub(j.created_at)),
                        queue_ms: j
                            .activated_at
                            .map(|a| a.saturating_sub(j.created_at)),
                        service_ms: match (j.activated_at, j.completed_at) {
                            (Some(a), Some(c)) => Some(c.saturating_sub(a)),
                            _ => None,
                        },
                        attempts: j.attempts,
                        failures: j.failures,
                    });
                    ElementDto {
                        element_id: e.element_id.clone(),
                        element_instance_key: e.element_instance_key.to_string(),
                        scope: e.scope.to_string(),
                        entered_at: e.entered_at,
                        exited_at: e.exited_at,
                        duration_ms: e.exited_at.map(|x| x.saturating_sub(e.entered_at)),
                        incidents: e.incidents,
                        job,
                    }
                })
                .collect(),
            incidents: self
                .incidents
                .iter()
                .map(|i| IncidentDto {
                    element_id: i.element_id.clone(),
                    element_instance_key: i.element_instance_key.to_string(),
                    kind: i.kind.clone(),
                    reason: i.reason.clone(),
                    raised_at: i.raised_at,
                    resolved_at: i.resolved_at,
                })
                .collect(),
            path: self.path.clone(),
        }
    }

    /// Renders the trace as an OTLP/JSON trace document.
    fn otel(&self) -> serde_json::Value {
        use serde_json::json;

        let trace_id = format!("{:032x}", self.instance_key as u128);
        let root_span_id = span_id(self.instance_key);
        let inst_end = self.ended_at.unwrap_or(self.last_at);

        let mut spans = Vec::new();
        spans.push(json!({
            "traceId": trace_id,
            "spanId": root_span_id,
            "name": format!("process {}", self.process_id),
            "kind": 1,
            "startTimeUnixNano": nanos(self.started_at),
            "endTimeUnixNano": nanos(inst_end),
            "attributes": [
                attr_str("nanobpmn.process_id", &self.process_id),
                attr_str("nanobpmn.instance_key", &self.instance_key.to_string()),
                attr_str("nanobpmn.outcome", self.outcome.as_str()),
                attr_int("nanobpmn.element_count", self.elements.len() as i64),
                attr_int("nanobpmn.incident_count", self.incidents.len() as i64),
            ],
        }));

        for e in &self.elements {
            let el_span = span_id(e.element_instance_key);
            let el_end = e.exited_at.unwrap_or(self.last_at);
            spans.push(json!({
                "traceId": trace_id,
                "spanId": el_span,
                "parentSpanId": root_span_id,
                "name": e.element_id,
                "kind": 1,
                "startTimeUnixNano": nanos(e.entered_at),
                "endTimeUnixNano": nanos(el_end),
                "attributes": [
                    attr_str("nanobpmn.element_id", &e.element_id),
                    attr_int("nanobpmn.incidents", e.incidents as i64),
                ],
            }));

            if let Some(j) = &e.job {
                let j_start = j.activated_at.unwrap_or(j.created_at);
                let j_end = j.completed_at.unwrap_or(self.last_at);
                let mut attrs = vec![
                    attr_str("nanobpmn.job_type", &j.job_type),
                    attr_int("nanobpmn.attempts", j.attempts as i64),
                    attr_int("nanobpmn.failures", j.failures as i64),
                ];
                if let Some(w) = &j.worker {
                    attrs.push(attr_str("nanobpmn.worker", w));
                }
                if let Some(a) = j.activated_at {
                    attrs.push(attr_int(
                        "nanobpmn.queue_ms",
                        a.saturating_sub(j.created_at) as i64,
                    ));
                }
                if let Some(c) = j.completed_at {
                    attrs.push(attr_int(
                        "nanobpmn.wait_ms",
                        c.saturating_sub(j.created_at) as i64,
                    ));
                }
                if let (Some(a), Some(c)) = (j.activated_at, j.completed_at) {
                    attrs.push(attr_int("nanobpmn.service_ms", c.saturating_sub(a) as i64));
                }
                spans.push(json!({
                    "traceId": trace_id,
                    "spanId": span_id(j.job_key ^ 0x6a6f_6200),
                    "parentSpanId": el_span,
                    "name": format!("job {}", j.job_type),
                    "kind": 3,
                    "startTimeUnixNano": nanos(j_start),
                    "endTimeUnixNano": nanos(j_end),
                    "attributes": attrs,
                }));
            }
        }

        json!({
            "resourceSpans": [{
                "resource": {
                    "attributes": [ attr_str("service.name", "nanobpmn") ]
                },
                "scopeSpans": [{
                    "scope": { "name": "nanobpmn.engine" },
                    "spans": spans,
                }]
            }]
        })
    }
}

fn span_id(seed: u64) -> String {
    // Mix so distinct keys (instance vs element) don't collide on low bits.
    let mixed = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ (seed >> 29);
    format!("{mixed:016x}")
}

/// OTLP encodes 64-bit unix-nano timestamps as decimal strings.
fn nanos(ms: u64) -> String {
    (ms.saturating_mul(1_000_000)).to_string()
}

fn attr_str(key: &str, value: &str) -> serde_json::Value {
    serde_json::json!({ "key": key, "value": { "stringValue": value } })
}

fn attr_int(key: &str, value: i64) -> serde_json::Value {
    serde_json::json!({ "key": key, "value": { "intValue": value } })
}

// ---------------------------------------------------------------------------
// DTOs (camelCase, keys as strings — matching the console's metrics/health DTOs)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceSummaryDto {
    pub instance_key: String,
    pub process_id: String,
    pub version: Option<i32>,
    pub business_id: Option<String>,
    pub outcome: &'static str,
    pub started_at: u64,
    pub ended_at: Option<u64>,
    pub duration_ms: Option<u64>,
    pub element_count: usize,
    pub incident_count: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceTraceDto {
    pub instance_key: String,
    pub process_id: String,
    pub version: Option<i32>,
    pub business_id: Option<String>,
    pub tags: Vec<String>,
    pub started_at: u64,
    pub ended_at: Option<u64>,
    pub duration_ms: Option<u64>,
    pub outcome: &'static str,
    pub elements: Vec<ElementDto>,
    pub incidents: Vec<IncidentDto>,
    pub path: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ElementDto {
    pub element_id: String,
    pub element_instance_key: String,
    pub scope: String,
    pub entered_at: u64,
    pub exited_at: Option<u64>,
    pub duration_ms: Option<u64>,
    pub incidents: u32,
    pub job: Option<JobDto>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobDto {
    #[serde(rename = "type")]
    pub job_type: String,
    pub worker: Option<String>,
    pub created_at: u64,
    pub activated_at: Option<u64>,
    pub completed_at: Option<u64>,
    pub wait_ms: Option<u64>,
    pub queue_ms: Option<u64>,
    pub service_ms: Option<u64>,
    pub attempts: u32,
    pub failures: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IncidentDto {
    pub element_id: String,
    pub element_instance_key: String,
    pub kind: String,
    pub reason: String,
    pub raised_at: u64,
    pub resolved_at: Option<u64>,
}
