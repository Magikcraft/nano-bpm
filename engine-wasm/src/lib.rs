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

use std::collections::HashMap;

use nanobpmn_engine_core::{
    bpmn::parse_bpmn, Command, Engine, Event, IncidentState, JobState, MessageSubscriptionState,
    ProcessInstanceState, TimerState, UserTaskChangeset, UserTaskState, Value,
};
use serde::Serialize;
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

    /// Discard all engine state (deployed definitions, instances, jobs, timers,
    /// the event log and the virtual clock), returning the engine to the same
    /// pristine state as a freshly constructed one. Callers redeploy afterwards
    /// to start a clean run — this is what makes a re-run start from zero
    /// completed instances rather than accumulating across runs.
    pub fn reset(&mut self) {
        self.engine = Engine::new();
        self.now = 0;
        self.seq = 0;
        self.log.clear();
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
            adhoc_result: None,
            task_listener_result: None,
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

    /// Correlate a message to any instance waiting on it: publishes `message_name`
    /// with the given `correlation_key` (the value the waiting subscription's
    /// `correlationKey` expression resolved to) and merges `variables_json` (a
    /// JSON object string) into each correlated instance. This unblocks a message
    /// intermediate catch / receive task without an external broker — the
    /// in-browser equivalent of an app publishing a message. Returns the snapshot.
    #[wasm_bindgen(js_name = correlateMessage)]
    pub fn correlate_message(
        &mut self,
        message_name: &str,
        correlation_key: &str,
        variables_json: &str,
    ) -> Result<String, JsValue> {
        let variables = parse_vars(variables_json)?;
        self.apply(Command::CorrelateMessage {
            message_name: message_name.to_string(),
            correlation_key: correlation_key.to_string(),
            variables,
        })
        .map_err(|e| js_err(&format!("correlate error: {e}")))?;
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

    /// Set the engine clock to a wall-clock instant (ms), then trigger due timers
    /// and expire lapsed job locks. The embedded host calls this with `Date.now()`
    /// so `engine-core` stays clock-free while running as a real runtime. The
    /// clock never moves backwards. Returns the snapshot.
    #[wasm_bindgen(js_name = tickNow)]
    pub fn tick_now(&mut self, now_ms: f64) -> Result<String, JsValue> {
        let now = if now_ms.is_finite() && now_ms > 0.0 {
            now_ms as u64
        } else {
            0
        };
        if now > self.now {
            self.now = now;
        }
        let now = self.now;
        self.apply(Command::TriggerTimers { now })
            .map_err(|e| js_err(&format!("timer error: {e}")))?;
        self.apply(Command::ExpireJobs { now })
            .map_err(|e| js_err(&format!("expire error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Activate up to `max_jobs` `Created` jobs of `job_type`, locking them to
    /// `worker` until `now + timeout_ms`. Returns a JSON array of activated jobs
    /// (key, type, instance/element, retries, variables) for the dispatch loop to
    /// hand to worker handlers. The host owns the wall clock via `tickNow`.
    #[wasm_bindgen(js_name = activateJobs)]
    pub fn activate_jobs(
        &mut self,
        job_type: &str,
        max_jobs: u32,
        timeout_ms: f64,
        worker: &str,
    ) -> Result<String, JsValue> {
        let now = self.now;
        let timeout = if timeout_ms.is_finite() && timeout_ms > 0.0 {
            timeout_ms as u64
        } else {
            30_000
        };
        self.apply(Command::ActivateJobs {
            job_type: job_type.to_string(),
            worker: worker.to_string(),
            max_jobs: (max_jobs.max(1)) as usize,
            timeout,
            now,
        })
        .map_err(|e| js_err(&format!("activate error: {e}")))?;
        let state = self.engine.state();
        let mut out: Vec<serde_json::Value> = state
            .jobs
            .values()
            .filter(|j| {
                j.job_type == job_type
                    && j.state == JobState::Activated
                    && j.worker.as_deref() == Some(worker)
            })
            .map(|j| {
                let vars = state
                    .instances
                    .get(&j.instance_key)
                    .map(|i| vars_to_json(&i.variables))
                    .unwrap_or_default();
                serde_json::json!({
                    "key": j.key.to_string(),
                    "type": j.job_type,
                    "instanceKey": j.instance_key.to_string(),
                    "elementId": j.element_id,
                    "retries": j.retries,
                    "variables": vars,
                })
            })
            .collect();
        out.sort_by(|a, b| a["key"].as_str().cmp(&b["key"].as_str()));
        to_json(&serde_json::Value::Array(out))
    }

    /// Throw a BPMN business error from a waiting job by key. If the job's
    /// activity has a matching error boundary/event-subprocess catch it is
    /// interrupted and the error-handling path runs; otherwise an incident is
    /// raised. The job is activated first if needed, so the UI can throw an
    /// error directly from a freshly-created job. Returns the snapshot.
    #[wasm_bindgen(js_name = throwError)]
    pub fn throw_error(
        &mut self,
        job_key: &str,
        error_code: &str,
        error_message: &str,
    ) -> Result<String, JsValue> {
        let key = parse_key(job_key)?;
        self.ensure_activated(key)?;
        self.apply(Command::ThrowJobError {
            job_key: key,
            error_code: error_code.to_string(),
            error_message: error_message.to_string(),
        })
        .map_err(|e| js_err(&format!("throw error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Set a job's remaining retries by key. Used to recover a job parked on a
    /// no-retries incident before resolving that incident; does not by itself
    /// unblock the job. Returns the snapshot.
    #[wasm_bindgen(js_name = updateRetries)]
    pub fn update_retries(&mut self, job_key: &str, retries: i32) -> Result<String, JsValue> {
        let key = parse_key(job_key)?;
        self.apply(Command::UpdateJobRetries {
            job_key: key,
            retries,
        })
        .map_err(|e| js_err(&format!("update retries error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Resolve an open incident by key, retrying the work that failed (a job
    /// incident returns the parked job — which must have retries left — to the
    /// activatable pool; a gateway incident re-evaluates; an uncaught-error
    /// incident re-creates the service-task job). Returns the snapshot.
    #[wasm_bindgen(js_name = resolveIncident)]
    pub fn resolve_incident(&mut self, incident_key: &str) -> Result<String, JsValue> {
        let key = parse_key(incident_key)?;
        self.apply(Command::ResolveIncident {
            incident_key: key,
            operation_reference: None,
        })
        .map_err(|e| js_err(&format!("resolve incident error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Merge variables into a scope (a process-instance key or an element-instance
    /// key). When `local` is true the values are written strictly into the target
    /// scope; otherwise they propagate upward to the nearest ancestor scope that
    /// defines each name (Zeebe `SetVariables` semantics). Returns the snapshot.
    #[wasm_bindgen(js_name = setVariables)]
    pub fn set_variables(
        &mut self,
        scope_key: &str,
        variables_json: &str,
        local: bool,
    ) -> Result<String, JsValue> {
        let key = parse_key(scope_key)?;
        let variables = parse_vars(variables_json)?;
        self.apply(Command::SetVariables {
            scope_key: key,
            variables,
            local,
        })
        .map_err(|e| js_err(&format!("set variables error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Broadcast a signal by name to **every** open subscription that matches,
    /// across all instances, merging `variables_json` into each correlated
    /// instance. Signals correlate by name only and are not buffered. Returns
    /// the snapshot.
    #[wasm_bindgen(js_name = broadcastSignal)]
    pub fn broadcast_signal(
        &mut self,
        signal_name: &str,
        variables_json: &str,
    ) -> Result<String, JsValue> {
        let variables = parse_vars(variables_json)?;
        self.apply(Command::BroadcastSignal {
            signal_name: signal_name.to_string(),
            variables,
        })
        .map_err(|e| js_err(&format!("broadcast signal error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Cancel (terminate) a running process instance by key. Every token is
    /// discarded, pending jobs are canceled, and the instance transitions to
    /// `Terminated`. Returns the snapshot.
    #[wasm_bindgen(js_name = cancelInstance)]
    pub fn cancel_instance(&mut self, instance_key: &str) -> Result<String, JsValue> {
        let key = parse_key(instance_key)?;
        self.apply(Command::CancelInstance { instance_key: key })
            .map_err(|e| js_err(&format!("cancel instance error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Complete a waiting user task by key, merging `variables_json` into the
    /// instance before the parked token resumes. The task must be in the
    /// `Created` state. Returns the snapshot.
    #[wasm_bindgen(js_name = completeUserTask)]
    pub fn complete_user_task(
        &mut self,
        user_task_key: &str,
        variables_json: &str,
    ) -> Result<String, JsValue> {
        let key = parse_key(user_task_key)?;
        let variables = parse_vars(variables_json)?;
        self.apply(Command::CompleteUserTask {
            user_task_key: key,
            variables,
        })
        .map_err(|e| js_err(&format!("complete user task error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Assign a user task to `assignee`. When `allow_override` is false and the
    /// task already has an assignee the command is rejected (it must be
    /// unassigned first). Returns the snapshot.
    #[wasm_bindgen(js_name = assignUserTask)]
    pub fn assign_user_task(
        &mut self,
        user_task_key: &str,
        assignee: &str,
        allow_override: bool,
    ) -> Result<String, JsValue> {
        let key = parse_key(user_task_key)?;
        self.apply(Command::AssignUserTask {
            user_task_key: key,
            assignee: assignee.to_string(),
            allow_override,
        })
        .map_err(|e| js_err(&format!("assign user task error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Clear a user task's assignee. The task must be in the `Created` state.
    /// Returns the snapshot.
    #[wasm_bindgen(js_name = unassignUserTask)]
    pub fn unassign_user_task(&mut self, user_task_key: &str) -> Result<String, JsValue> {
        let key = parse_key(user_task_key)?;
        self.apply(Command::UnassignUserTask { user_task_key: key })
            .map_err(|e| js_err(&format!("unassign user task error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Update a user task's attributes from a JSON changeset object. Recognised
    /// keys (all optional): `candidateGroups` / `candidateUsers` (string arrays),
    /// `dueDate` / `followUpDate` (ISO-8601 string, or `null`/`""` to clear),
    /// `priority` (0..=100). Only present keys are changed. The task must be in
    /// the `Created` state. Returns the snapshot.
    #[wasm_bindgen(js_name = updateUserTask)]
    pub fn update_user_task(
        &mut self,
        user_task_key: &str,
        changeset_json: &str,
    ) -> Result<String, JsValue> {
        let key = parse_key(user_task_key)?;
        let changeset = parse_user_task_changeset(changeset_json)?;
        self.apply(Command::UpdateUserTask {
            user_task_key: key,
            changeset,
        })
        .map_err(|e| js_err(&format!("update user task error: {e}")))?;
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

        // User tasks parked on `userTask` elements (Created = waiting for a human;
        // Completed/Canceled retained for audit). Play renders Created tasks in
        // its task panel and lets the user complete/assign them.
        let mut user_tasks: Vec<UserTaskDto> = state
            .user_tasks
            .values()
            .map(|t| UserTaskDto {
                key: t.key.to_string(),
                instance_key: t.instance_key.to_string(),
                element_instance_key: t.element_instance_key.to_string(),
                element_id: t.element_id.clone(),
                state: user_task_state(&t.state),
                assignee: t.assignee.clone(),
                candidate_groups: t.candidate_groups.clone(),
                candidate_users: t.candidate_users.clone(),
                due_date: t.due_date.clone(),
                follow_up_date: t.follow_up_date.clone(),
                priority: t.priority,
            })
            .collect();
        user_tasks.sort_by(|a, b| a.key.cmp(&b.key));

        // Open message subscriptions (waiting catch/boundary events). Play's
        // message-correlation panel needs the name + resolved correlation key.
        let mut message_subscriptions: Vec<MessageSubscriptionDto> = state
            .message_subscriptions
            .values()
            .filter(|s| {
                matches!(
                    s.state,
                    MessageSubscriptionState::Open | MessageSubscriptionState::Opening
                )
            })
            .map(|s| MessageSubscriptionDto {
                key: s.key.to_string(),
                instance_key: s.instance_key.to_string(),
                element_id: s.element_id.clone(),
                message_name: s.message_name.clone(),
                correlation_key: s.correlation_key.clone(),
                kind: format!("{:?}", s.kind),
            })
            .collect();
        message_subscriptions.sort_by(|a, b| a.key.cmp(&b.key));

        // Open signal subscriptions (waiting signal catch/boundary events). Play's
        // signal-broadcast panel lists the signal names currently awaited.
        let mut signal_subscriptions: Vec<SignalSubscriptionDto> = state
            .signal_subscriptions
            .values()
            .filter(|s| {
                matches!(
                    s.state,
                    MessageSubscriptionState::Open | MessageSubscriptionState::Opening
                )
            })
            .map(|s| SignalSubscriptionDto {
                key: s.key.to_string(),
                instance_key: s.instance_key.to_string(),
                element_id: s.element_id.clone(),
                signal_name: s.signal_name.clone(),
                kind: format!("{:?}", s.kind),
            })
            .collect();
        signal_subscriptions.sort_by(|a, b| a.key.cmp(&b.key));

        // Per-element token statistics for diagram overlays. `active` is the
        // current live token count (from instance active elements); `completed`
        // and `incidents` are cumulative counts derived from the event log.
        let mut stats: HashMap<String, ElementStat> = HashMap::new();
        for inst in &instances {
            for el in &inst.active_elements {
                stats.entry(el.element_id.clone()).or_default().active += 1;
            }
        }
        // Cumulative sequence flows taken, as (from, to) element-id pairs, for
        // highlighting traversed connections (Play's `fetchSequenceFlows`).
        let mut taken_pairs: Vec<(String, String)> = Vec::new();
        for entry in &self.log {
            match &entry.event {
                Event::ElementCompleted { element_id, .. } => {
                    stats.entry(element_id.clone()).or_default().completed += 1;
                }
                Event::SequenceFlowTaken { from, to, .. } => {
                    taken_pairs.push((from.clone(), to.clone()));
                }
                _ => {}
            }
        }
        taken_pairs.sort();
        taken_pairs.dedup();
        let taken_sequence_flows: Vec<SequenceFlowDto> = taken_pairs
            .into_iter()
            .map(|(from, to)| SequenceFlowDto { from, to })
            .collect();
        for inc in &incidents {
            stats.entry(inc.element_id.clone()).or_default().incidents += 1;
        }
        let mut element_stats: Vec<ElementStatDto> = stats
            .into_iter()
            .map(|(element_id, s)| ElementStatDto {
                element_id,
                active: s.active,
                completed: s.completed,
                incidents: s.incidents,
            })
            .collect();
        element_stats.sort_by(|a, b| a.element_id.cmp(&b.element_id));

        // Evaluated decision instances (from `businessRuleTask`/DMN), derived from
        // the `DecisionEvaluated` audit events in the log (Play's
        // `fetchDecisionInstances`). Not part of live engine state.
        let mut decision_instances: Vec<DecisionInstanceDto> = self
            .log
            .iter()
            .filter_map(|entry| match &entry.event {
                Event::DecisionEvaluated {
                    instance_key,
                    element_id,
                    decision_key,
                    decision_id,
                    decision_output,
                    evaluated_at,
                    ..
                } => Some(DecisionInstanceDto {
                    instance_key: instance_key.to_string(),
                    element_id: element_id.clone(),
                    decision_key: decision_key.to_string(),
                    decision_id: decision_id.clone(),
                    output: value_to_json(decision_output),
                    evaluated_at: *evaluated_at,
                }),
                _ => None,
            })
            .collect();
        decision_instances.sort_by(|a, b| a.decision_key.cmp(&b.decision_key));

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
            user_tasks,
            message_subscriptions,
            signal_subscriptions,
            element_stats,
            taken_sequence_flows,
            decision_instances,
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
    user_tasks: Vec<UserTaskDto>,
    message_subscriptions: Vec<MessageSubscriptionDto>,
    signal_subscriptions: Vec<SignalSubscriptionDto>,
    element_stats: Vec<ElementStatDto>,
    taken_sequence_flows: Vec<SequenceFlowDto>,
    decision_instances: Vec<DecisionInstanceDto>,
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UserTaskDto {
    key: String,
    instance_key: String,
    element_instance_key: String,
    element_id: String,
    state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    assignee: Option<String>,
    candidate_groups: Vec<String>,
    candidate_users: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    due_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    follow_up_date: Option<String>,
    priority: i32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MessageSubscriptionDto {
    key: String,
    instance_key: String,
    element_id: String,
    message_name: String,
    correlation_key: String,
    kind: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SignalSubscriptionDto {
    key: String,
    instance_key: String,
    element_id: String,
    signal_name: String,
    kind: String,
}

/// Cumulative per-element counters accumulated while building a snapshot.
#[derive(Default)]
struct ElementStat {
    active: u64,
    completed: u64,
    incidents: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ElementStatDto {
    element_id: String,
    active: u64,
    completed: u64,
    incidents: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SequenceFlowDto {
    from: String,
    to: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DecisionInstanceDto {
    instance_key: String,
    element_id: String,
    decision_key: String,
    decision_id: String,
    output: serde_json::Value,
    evaluated_at: u64,
}

fn instance_state(s: &ProcessInstanceState) -> String {
    match s {
        ProcessInstanceState::Active => "Active",
        ProcessInstanceState::Terminating => "Terminating",
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

fn user_task_state(s: &UserTaskState) -> String {
    match s {
        UserTaskState::Created => "Created",
        UserTaskState::Completed => "Completed",
        UserTaskState::Canceled => "Canceled",
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

/// Parse a JSON changeset object for `updateUserTask` into a [`UserTaskChangeset`].
/// Only keys present in the object become `Some`; absent keys leave that
/// attribute unchanged. `dueDate`/`followUpDate` accept a string, or `null`/`""`
/// to clear the attribute.
fn parse_user_task_changeset(s: &str) -> Result<UserTaskChangeset, JsValue> {
    let t = s.trim();
    if t.is_empty() {
        return Ok(UserTaskChangeset::default());
    }
    let json: serde_json::Value =
        serde_json::from_str(t).map_err(|e| js_err(&format!("invalid changeset JSON: {e}")))?;
    let obj = match json {
        serde_json::Value::Object(map) => map,
        _ => return Err(js_err("changeset must be a JSON object")),
    };

    let string_list = |v: &serde_json::Value| -> Result<Vec<String>, JsValue> {
        match v {
            serde_json::Value::Array(items) => items
                .iter()
                .map(|i| {
                    i.as_str()
                        .map(|s| s.to_string())
                        .ok_or_else(|| js_err("candidate list entries must be strings"))
                })
                .collect(),
            _ => Err(js_err("candidate groups/users must be a JSON array")),
        }
    };
    // A nullable date field: `Some(None)` clears, `Some(Some(s))` sets, absent leaves alone.
    let opt_date = |v: &serde_json::Value| -> Result<Option<String>, JsValue> {
        match v {
            serde_json::Value::Null => Ok(None),
            serde_json::Value::String(s) if s.is_empty() => Ok(None),
            serde_json::Value::String(s) => Ok(Some(s.clone())),
            _ => Err(js_err("date fields must be a string or null")),
        }
    };

    let mut changeset = UserTaskChangeset::default();
    if let Some(v) = obj.get("candidateGroups") {
        changeset.candidate_groups = Some(string_list(v)?);
    }
    if let Some(v) = obj.get("candidateUsers") {
        changeset.candidate_users = Some(string_list(v)?);
    }
    if let Some(v) = obj.get("dueDate") {
        changeset.due_date = Some(opt_date(v)?);
    }
    if let Some(v) = obj.get("followUpDate") {
        changeset.follow_up_date = Some(opt_date(v)?);
    }
    if let Some(v) = obj.get("priority") {
        let p = v
            .as_i64()
            .ok_or_else(|| js_err("priority must be an integer"))?;
        changeset.priority = Some(p as i32);
    }
    Ok(changeset)
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
        Value::List(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
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
        serde_json::Value::Object(map) => Value::Map(
            map.iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value as J;

    use super::*;

    fn parse(s: &str) -> J {
        serde_json::from_str(s).expect("valid JSON")
    }

    const USER_TASK_XML: &str = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:userTask id="review">
            <bpmn:extensionElements><zeebe:userTask /></bpmn:extensionElements>
          </bpmn:userTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
          <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;

    const SERVICE_TASK_XML: &str = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="work">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="do-work" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="work" />
          <bpmn:sequenceFlow id="b" sourceRef="work" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;

    fn only_user_task_key(snap: &J) -> String {
        let tasks = snap["userTasks"].as_array().expect("userTasks array");
        assert_eq!(tasks.len(), 1, "expected one user task: {snap}");
        tasks[0]["key"].as_str().unwrap().to_string()
    }

    #[test]
    fn user_task_lifecycle_and_snapshot() {
        let mut eng = TestEngine::new();
        eng.deploy(USER_TASK_XML).unwrap();
        let snap = parse(&eng.create_instance("p", "{}").unwrap());

        // A Created user task is surfaced, parked on the `review` element.
        let task = &snap["userTasks"][0];
        assert_eq!(task["state"], "Created");
        assert_eq!(task["elementId"], "review");
        assert_eq!(task["priority"], 50);
        // Its element is highlighted as active and the entry flow is recorded.
        assert!(snap["activeElementIds"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "review"));
        assert!(snap["takenSequenceFlows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f["from"] == "s" && f["to"] == "review"));

        let key = only_user_task_key(&snap);

        // Assign / unassign flow.
        let snap = parse(&eng.assign_user_task(&key, "alice", false).unwrap());
        assert_eq!(snap["userTasks"][0]["assignee"], "alice");
        let snap = parse(&eng.unassign_user_task(&key).unwrap());
        assert!(snap["userTasks"][0].get("assignee").is_none());

        // Update attributes from a JSON changeset.
        let snap = parse(
            &eng.update_user_task(
                &key,
                r#"{"candidateGroups":["ops"],"priority":80,"dueDate":"2026-01-01"}"#,
            )
            .unwrap(),
        );
        assert_eq!(snap["userTasks"][0]["candidateGroups"][0], "ops");
        assert_eq!(snap["userTasks"][0]["priority"], 80);
        assert_eq!(snap["userTasks"][0]["dueDate"], "2026-01-01");

        // Completing resumes the token, completes the instance, and records the
        // exit sequence flow + element-completed statistic.
        let snap = parse(
            &eng.complete_user_task(&key, r#"{"approved":true}"#)
                .unwrap(),
        );
        assert_eq!(snap["userTasks"][0]["state"], "Completed");
        assert_eq!(snap["completedInstances"], 1);
        assert!(snap["takenSequenceFlows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f["from"] == "review" && f["to"] == "e"));
        let review_stat = snap["elementStats"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["elementId"] == "review")
            .expect("review stat");
        assert_eq!(review_stat["completed"], 1);
    }

    #[test]
    fn throw_error_without_boundary_raises_incident() {
        let mut eng = TestEngine::new();
        eng.deploy(SERVICE_TASK_XML).unwrap();
        let snap = parse(&eng.create_instance("p", "{}").unwrap());
        let job_key = snap["jobs"][0]["key"].as_str().unwrap().to_string();

        // Throwing an uncaught business error consumes the job and raises an
        // incident visible on the `work` element.
        let snap = parse(&eng.throw_error(&job_key, "BOOM", "kaboom").unwrap());
        let incidents = snap["incidents"].as_array().unwrap();
        assert_eq!(incidents.len(), 1, "expected one incident: {snap}");
        assert_eq!(incidents[0]["elementId"], "work");
        assert!(snap["incidentElementIds"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "work"));
    }

    #[test]
    fn fail_update_retries_resolve_incident_recovers_job() {
        let mut eng = TestEngine::new();
        eng.deploy(SERVICE_TASK_XML).unwrap();
        let snap = parse(&eng.create_instance("p", "{}").unwrap());
        let job_key = snap["jobs"][0]["key"].as_str().unwrap().to_string();

        // Fail with no retries left → incident.
        let snap = parse(&eng.fail_job(&job_key, 0, "nope").unwrap());
        let incident_key = snap["incidents"][0]["key"].as_str().unwrap().to_string();

        // Give the job a retry, then resolve the incident to re-create the job.
        eng.update_retries(&job_key, 1).unwrap();
        let snap = parse(&eng.resolve_incident(&incident_key).unwrap());
        assert!(
            snap["incidents"].as_array().unwrap().is_empty(),
            "incident should be resolved: {snap}"
        );
        assert!(
            !snap["jobs"].as_array().unwrap().is_empty(),
            "job should be activatable again: {snap}"
        );
    }

    #[test]
    fn set_variables_merges_into_instance() {
        let mut eng = TestEngine::new();
        eng.deploy(SERVICE_TASK_XML).unwrap();
        let snap = parse(&eng.create_instance("p", r#"{"a":1}"#).unwrap());
        let instance_key = snap["instances"][0]["key"].as_str().unwrap().to_string();

        let snap = parse(
            &eng.set_variables(&instance_key, r#"{"b":2}"#, false)
                .unwrap(),
        );
        let vars = &snap["instances"][0]["variables"];
        assert_eq!(vars["a"], 1);
        assert_eq!(vars["b"], 2);
    }

    #[test]
    fn cancel_instance_terminates() {
        let mut eng = TestEngine::new();
        eng.deploy(USER_TASK_XML).unwrap();
        let snap = parse(&eng.create_instance("p", "{}").unwrap());
        let instance_key = snap["instances"][0]["key"].as_str().unwrap().to_string();

        let snap = parse(&eng.cancel_instance(&instance_key).unwrap());
        assert_eq!(snap["instances"][0]["state"], "Terminated");
        assert!(snap["userTasks"]
            .as_array()
            .unwrap()
            .iter()
            .all(|t| t["state"] != "Created"));
    }
}
