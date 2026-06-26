//! In-browser test-execution wrapper around `nanobpmn-engine-core`.
//!
//! This crate exists purely to give the web modeler a way to *run* a BPMN
//! process before deploying it. It compiles `engine-core` (zero-dependency,
//! deterministic, clock-injected) to `wasm32-unknown-unknown` and exposes a
//! tiny JSON-string API over [`wasm_bindgen`].
//!
//! Design constraints:
//!   * `engine-core` stays lean and untouched — all the binding glue lives here.
//!   * The engine never reads a wall clock; we drive a *virtual* clock so the
//!     simulation is fully deterministic and timers can be "fast-forwarded".
//!   * Every mutating call returns a full [`Snapshot`] as JSON so the UI can
//!     re-render the diagram (active tokens), variables, jobs and incidents from
//!     a single round-trip.

use nanobpmn_engine_core::{
    bpmn::parse_bpmn, Command, Engine, Event, IncidentState, JobState, ProcessInstanceState,
    TimerState, Value,
};
use serde::Serialize;
use std::collections::HashMap;
use wasm_bindgen::prelude::*;

/// A simulated engine instance bound to one modeler session.
#[wasm_bindgen]
pub struct TestEngine {
    engine: Engine,
    now: u64,
    seq: u64,
    log: Vec<LogEntry>,
}

struct LogEntry {
    seq: u64,
    now: u64,
    event: Event,
}

#[wasm_bindgen]
impl TestEngine {
    /// Create a fresh, empty simulated engine. The virtual clock starts at 0.
    #[wasm_bindgen(constructor)]
    pub fn new() -> TestEngine {
        TestEngine {
            engine: Engine::new(),
            now: 0,
            seq: 0,
            log: Vec::new(),
        }
    }

    /// The current virtual clock (milliseconds).
    #[wasm_bindgen(getter)]
    pub fn now(&self) -> f64 {
        self.now as f64
    }

    /// Parse and deploy a BPMN resource. Returns a JSON object
    /// `{ "processIds": [...], "snapshot": {...} }` on success, or throws a
    /// JS error carrying the parse/deploy failure message.
    pub fn deploy(&mut self, xml: &str) -> Result<String, JsValue> {
        let defs = parse_bpmn(xml).map_err(|e| js_err(&format!("parse error: {e}")))?;
        let ids: Vec<String> = defs.iter().map(|d| d.id.clone()).collect();
        self.apply(Command::DeployResources(defs))
            .map_err(|e| js_err(&format!("deploy error: {e}")))?;
        let snapshot = self.snapshot_value(None);
        to_json(&serde_json::json!({
            "processIds": ids,
            "snapshot": snapshot,
        }))
    }

    /// Start a new instance of `process_id`, seeding it with the given variables
    /// (a JSON object string; pass `"{}"` or `""` for none). Returns the
    /// post-run [`Snapshot`] with a top-level `created` field holding the new
    /// instance key.
    #[wasm_bindgen(js_name = createInstance)]
    pub fn create_instance(
        &mut self,
        process_id: &str,
        variables_json: &str,
    ) -> Result<String, JsValue> {
        let variables = parse_vars(variables_json)?;
        let events = self
            .apply(Command::CreateInstance {
                process_id: process_id.to_string(),
                variables,
                tags: Vec::new(),
                business_id: None,
            })
            .map_err(|e| js_err(&format!("create error: {e}")))?;
        let created = events.iter().find_map(|e| match e {
            Event::ProcessInstanceCreated { instance_key, .. } => Some(*instance_key),
            _ => None,
        });
        to_json(&self.snapshot_value(created))
    }

    /// Complete a waiting job by key, merging `variables_json` (a JSON object
    /// string) into the instance. The job is activated first if it has not been
    /// already, so the UI can complete a freshly-created job directly.
    #[wasm_bindgen(js_name = completeJob)]
    pub fn complete_job(&mut self, job_key: &str, variables_json: &str) -> Result<String, JsValue> {
        let key = parse_key(job_key)?;
        let variables = parse_vars(variables_json)?;
        self.ensure_activated(key)?;
        self.apply(Command::CompleteJob {
            job_key: key,
            variables,
        })
        .map_err(|e| js_err(&format!("complete error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Fail a waiting job by key with the given remaining `retries` and message.
    /// With no retries left this raises an incident (visible in the snapshot).
    #[wasm_bindgen(js_name = failJob)]
    pub fn fail_job(
        &mut self,
        job_key: &str,
        retries: i32,
        message: &str,
    ) -> Result<String, JsValue> {
        let key = parse_key(job_key)?;
        self.ensure_activated(key)?;
        self.apply(Command::FailJob {
            job_key: key,
            retries,
            error_message: message.to_string(),
        })
        .map_err(|e| js_err(&format!("fail error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Advance the virtual clock by `by_ms` milliseconds, firing any timers that
    /// become due and expiring any lapsed job locks.
    #[wasm_bindgen(js_name = advanceTime)]
    pub fn advance_time(&mut self, by_ms: f64) -> Result<String, JsValue> {
        let delta = if by_ms.is_finite() && by_ms > 0.0 {
            by_ms as u64
        } else {
            0
        };
        self.now = self.now.saturating_add(delta);
        let now = self.now;
        self.apply(Command::TriggerTimers { now })
            .map_err(|e| js_err(&format!("timer error: {e}")))?;
        self.apply(Command::ExpireJobs { now })
            .map_err(|e| js_err(&format!("expire error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// The current simulation state as a JSON [`Snapshot`].
    pub fn snapshot(&self) -> Result<String, JsValue> {
        to_json(&self.snapshot_value(None))
    }

    /// The full ordered event log emitted so far, as a JSON array of
    /// `{ seq, now, type, ...payload }`. Useful for a step-through / trace view.
    pub fn events(&self) -> Result<String, JsValue> {
        let arr: Vec<serde_json::Value> = self
            .log
            .iter()
            .map(|e| {
                let mut v = serde_json::to_value(&e.event).unwrap_or(serde_json::Value::Null);
                // Event serializes as an externally-tagged object `{ "Type": {..} }`;
                // flatten it to `{ "type": "Type", seq, now, ...fields }`.
                let (ty, body) = match v {
                    serde_json::Value::Object(ref mut m) if m.len() == 1 => {
                        let k = m.keys().next().cloned().unwrap();
                        let b = m.remove(&k).unwrap();
                        (k, b)
                    }
                    serde_json::Value::String(s) => (s, serde_json::Value::Null),
                    other => ("Unknown".to_string(), other),
                };
                let mut out = serde_json::Map::new();
                out.insert("seq".into(), e.seq.into());
                out.insert("now".into(), e.now.into());
                out.insert("type".into(), serde_json::Value::String(ty));
                if let serde_json::Value::Object(fields) = body {
                    for (k, val) in fields {
                        out.insert(k, val);
                    }
                }
                serde_json::Value::Object(out)
            })
            .collect();
        to_json(&arr)
    }
}

impl Default for TestEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl TestEngine {
    /// Apply a command at the current virtual clock, recording the emitted
    /// events in the log.
    fn apply(&mut self, command: Command) -> Result<Vec<Event>, nanobpmn_engine_core::EngineError> {
        let now = self.now;
        let events = self.engine.apply_command_at(command, now)?;
        for ev in &events {
            self.seq += 1;
            self.log.push(LogEntry {
                seq: self.seq,
                now,
                event: ev.clone(),
            });
        }
        Ok(events)
    }

    /// Activate the job's type so a `Created` job can be completed/failed. A job
    /// that has already been activated is left as-is.
    fn ensure_activated(&mut self, job_key: u64) -> Result<(), JsValue> {
        let job_type = self
            .engine
            .state()
            .jobs
            .get(&job_key)
            .map(|j| (j.job_type.clone(), j.state))
            .ok_or_else(|| js_err(&format!("no such job: {job_key}")))?;
        // Already activated (or terminal) — nothing to do; completion is by key.
        if job_type.1 != JobState::Created {
            return Ok(());
        }
        let now = self.now;
        self.apply(Command::ActivateJobs {
            job_type: job_type.0,
            worker: "modeler".to_string(),
            max_jobs: 1024,
            timeout: u64::MAX / 4,
            now,
        })
        .map_err(|e| js_err(&format!("activate error: {e}")))?;
        Ok(())
    }

    fn snapshot_value(&self, created: Option<u64>) -> serde_json::Value {
        let state = self.engine.state();

        let mut instances: Vec<InstanceDto> = state
            .instances
            .values()
            .map(|inst| {
                let mut active: Vec<ActiveEl> = inst
                    .active
                    .iter()
                    .map(|(k, eid)| ActiveEl {
                        key: k.to_string(),
                        element_id: eid.clone(),
                    })
                    .collect();
                active.sort_by(|a, b| a.key.cmp(&b.key));
                InstanceDto {
                    key: inst.key.to_string(),
                    process_id: inst.process_id.clone(),
                    state: instance_state(&inst.state),
                    completed: inst.state != ProcessInstanceState::Active,
                    active_elements: active,
                    variables: vars_to_json(&inst.variables),
                }
            })
            .collect();
        instances.sort_by(|a, b| a.key.cmp(&b.key));

        let mut jobs: Vec<JobDto> = state
            .jobs
            .values()
            .filter(|j| matches!(j.state, JobState::Created | JobState::Activated))
            .map(|j| JobDto {
                key: j.key.to_string(),
                instance_key: j.instance_key.to_string(),
                element_id: j.element_id.clone(),
                job_type: j.job_type.clone(),
                state: job_state(&j.state),
                retries: j.retries,
            })
            .collect();
        jobs.sort_by(|a, b| a.key.cmp(&b.key));

        let mut incidents: Vec<IncidentDto> = state
            .incidents
            .values()
            .filter(|i| i.state == IncidentState::Active)
            .map(|i| IncidentDto {
                key: i.key.to_string(),
                instance_key: i.instance_key.to_string(),
                element_id: i.element_id.clone(),
                kind: format!("{:?}", i.kind),
                reason: i.reason.clone(),
            })
            .collect();
        incidents.sort_by(|a, b| a.key.cmp(&b.key));

        let mut timers: Vec<TimerDto> = state
            .timers
            .values()
            .filter(|t| t.state == TimerState::Created)
            .map(|t| TimerDto {
                key: t.key.to_string(),
                instance_key: t.instance_key.to_string(),
                element_id: t.element_id.clone(),
                due_at: t.due_at,
                due_in_ms: t.due_at.saturating_sub(self.now),
            })
            .collect();
        timers.sort_by_key(|a| a.due_at);

        // Unions for one-shot diagram highlighting.
        let mut active_element_ids: Vec<String> = instances
            .iter()
            .flat_map(|i| i.active_elements.iter().map(|e| e.element_id.clone()))
            .collect();
        active_element_ids.sort();
        active_element_ids.dedup();
        let mut incident_element_ids: Vec<String> =
            incidents.iter().map(|i| i.element_id.clone()).collect();
        incident_element_ids.sort();
        incident_element_ids.dedup();

        let total_instances = instances.len();
        let completed_instances = instances.iter().filter(|i| i.completed).count();

        let snap = Snapshot {
            now: self.now,
            event_count: self.seq,
            created: created.map(|k| k.to_string()),
            total_instances,
            completed_instances,
            instances,
            jobs,
            incidents,
            timers,
            active_element_ids,
            incident_element_ids,
        };
        serde_json::to_value(&snap).unwrap_or(serde_json::Value::Null)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Snapshot {
    now: u64,
    event_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    created: Option<String>,
    total_instances: usize,
    completed_instances: usize,
    instances: Vec<InstanceDto>,
    jobs: Vec<JobDto>,
    incidents: Vec<IncidentDto>,
    timers: Vec<TimerDto>,
    active_element_ids: Vec<String>,
    incident_element_ids: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InstanceDto {
    key: String,
    process_id: String,
    state: String,
    completed: bool,
    active_elements: Vec<ActiveEl>,
    variables: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ActiveEl {
    key: String,
    element_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JobDto {
    key: String,
    instance_key: String,
    element_id: String,
    job_type: String,
    state: String,
    retries: i32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IncidentDto {
    key: String,
    instance_key: String,
    element_id: String,
    kind: String,
    reason: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TimerDto {
    key: String,
    instance_key: String,
    element_id: String,
    due_at: u64,
    due_in_ms: u64,
}

fn instance_state(s: &ProcessInstanceState) -> String {
    match s {
        ProcessInstanceState::Active => "Active",
        ProcessInstanceState::Completed => "Completed",
        ProcessInstanceState::Terminated => "Terminated",
    }
    .to_string()
}

fn job_state(s: &JobState) -> String {
    match s {
        JobState::Created => "Created",
        JobState::Activated => "Activated",
        JobState::Completed => "Completed",
        JobState::Failed => "Failed",
        JobState::Errored => "Errored",
        JobState::Canceled => "Canceled",
    }
    .to_string()
}

fn js_err(msg: &str) -> JsValue {
    JsValue::from_str(msg)
}

fn to_json<T: Serialize>(v: &T) -> Result<String, JsValue> {
    serde_json::to_string(v).map_err(|e| js_err(&format!("serialize error: {e}")))
}

/// Parse a decimal key string into a `u64`.
fn parse_key(s: &str) -> Result<u64, JsValue> {
    s.trim()
        .parse::<u64>()
        .map_err(|_| js_err(&format!("invalid key: {s}")))
}

/// Parse a JSON object string into engine variables. Empty/whitespace ⇒ none.
fn parse_vars(s: &str) -> Result<HashMap<String, Value>, JsValue> {
    let t = s.trim();
    if t.is_empty() {
        return Ok(HashMap::new());
    }
    let json: serde_json::Value =
        serde_json::from_str(t).map_err(|e| js_err(&format!("invalid variables JSON: {e}")))?;
    match json {
        serde_json::Value::Object(map) => Ok(map
            .into_iter()
            .map(|(k, v)| (k, json_to_value(&v)))
            .collect()),
        _ => Err(js_err("variables must be a JSON object")),
    }
}

/// Convert engine variables to a natural JSON object (not the tagged enum form).
fn vars_to_json(vars: &HashMap<String, Value>) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    let mut keys: Vec<&String> = vars.keys().collect();
    keys.sort();
    for k in keys {
        map.insert(k.clone(), value_to_json(&vars[k]));
    }
    serde_json::Value::Object(map)
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Double(d) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Str(s) => serde_json::Value::String(s.clone()),
        Value::List(items) => {
            serde_json::Value::Array(items.iter().map(value_to_json).collect())
        }
        Value::Map(entries) => {
            let mut m = serde_json::Map::new();
            for (k, val) in entries {
                m.insert(k.clone(), value_to_json(val));
            }
            serde_json::Value::Object(m)
        }
    }
}

fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(u) = n.as_u64() {
                Value::Int(u as i64)
            } else {
                Value::Double(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::Str(s.clone()),
        serde_json::Value::Array(items) => Value::List(items.iter().map(json_to_value).collect()),
        serde_json::Value::Object(map) => {
            Value::Map(map.iter().map(|(k, v)| (k.clone(), json_to_value(v))).collect())
        }
    }
}
