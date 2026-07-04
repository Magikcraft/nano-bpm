//! Unit tests for the engine, extracted verbatim from the former inline `mod tests`.
use super::*;
use crate::model::{ProcessBuilder, ProcessDefinition};

fn linear_with_task() -> ProcessDefinition {
    ProcessBuilder::new("order")
        .start_event("start")
        .service_task("charge", "payment")
        .end_event("end")
        .connect("start", "charge")
        .connect("charge", "end")
        .build()
        .unwrap()
}

/// A linear process whose single service task emits `work` jobs at the given
/// (literal) priority. Distinct `proc_id`s let several share one job type.
fn task_with_priority(proc_id: &str, priority: &str) -> ProcessDefinition {
    ProcessBuilder::new(proc_id)
        .start_event("start")
        .service_task_with_priority("do", "work", Some(priority.to_string()))
        .end_event("end")
        .connect("start", "do")
        .connect("do", "end")
        .build()
        .unwrap()
}

fn create_instance_key(engine: &mut Engine, proc_id: &str) -> Key {
    engine
        .apply_command(Command::create_instance(proc_id))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap()
}

#[test]
fn higher_priority_jobs_activate_before_older_lower_priority_jobs() {
    // Two processes emit the same `work` job type at different priorities.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_priority("low", "10")))
        .unwrap();
    engine
        .apply_command(Command::DeployProcess(task_with_priority("high", "90")))
        .unwrap();
    // Create the LOW-priority instance first (older, lower key), then HIGH.
    let low = create_instance_key(&mut engine, "low");
    let high = create_instance_key(&mut engine, "high");
    // Activation order is priority-first: the newer high-priority job wins.
    let jobs = engine.activate_jobs("work", "W", 10, 1_000, 0);
    assert_eq!(jobs.len(), 2);
    assert_eq!(
        jobs[0].instance_key, high,
        "higher priority activates first despite being newer"
    );
    assert_eq!(jobs[1].instance_key, low, "lower priority follows");
}

#[test]
fn equal_priority_jobs_activate_oldest_first() {
    // Same priority (default 50) => FIFO by creation (key) is preserved.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let first = create_instance_key(&mut engine, "order");
    let second = create_instance_key(&mut engine, "order");
    let jobs = engine.activate_jobs("payment", "W", 10, 1_000, 0);
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].instance_key, first, "oldest first");
    assert_eq!(jobs[1].instance_key, second);
}

#[test]
fn job_created_at_and_default_priority_are_stamped() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command_at(Command::create_instance("order"), 12_345)
        .unwrap();
    let job = engine
        .state()
        .jobs
        .values()
        .find(|j| j.job_type == "payment")
        .expect("a payment job");
    assert_eq!(
        job.created_at, 12_345,
        "created_at carries the command clock"
    );
    assert_eq!(
        job.priority,
        state::DEFAULT_JOB_PRIORITY,
        "no priorityDefinition => default priority"
    );
}

#[test]
fn redeploying_an_identical_definition_is_idempotent() {
    // A byte-for-byte identical redeploy of the latest version reuses its
    // identity: no ProcessDeployed event, no new key, no version bump.
    let mut engine = Engine::new();
    let first = engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let deployed: Vec<_> = first
        .iter()
        .filter(|e| matches!(e, Event::ProcessDeployed { .. }))
        .collect();
    assert_eq!(deployed.len(), 1, "first deploy registers the definition");
    let (first_key, first_version) = match deployed[0] {
        Event::ProcessDeployed {
            process_definition_key,
            version,
            ..
        } => (*process_definition_key, *version),
        _ => unreachable!(),
    };
    assert_eq!(first_version, 1);

    // Redeploy the exact same definition twice more.
    let second = engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let third = engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    assert!(
        second.is_empty() && third.is_empty(),
        "an identical redeploy emits no events"
    );

    // State still holds exactly the original version and key.
    let current = engine.state().processes.get("order").unwrap();
    assert_eq!(current.version, 1, "version is not bumped");
    assert_eq!(current.key, first_key, "key is reused");
}

#[test]
fn redeploying_a_changed_definition_bumps_the_version() {
    // A different model under the same id is a new version (not idempotent).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    let changed = ProcessBuilder::new("order")
        .start_event("start")
        .service_task("charge", "payment")
        .service_task("ship", "shipping")
        .end_event("end")
        .connect("start", "charge")
        .connect("charge", "ship")
        .connect("ship", "end")
        .build()
        .unwrap();
    let events = engine
        .apply_command(Command::DeployProcess(changed))
        .unwrap();
    let version = events
        .iter()
        .find_map(|e| match e {
            Event::ProcessDeployed { version, .. } => Some(*version),
            _ => None,
        })
        .expect("a changed definition is deployed as a new version");
    assert_eq!(version, 2);
    assert_eq!(engine.state().processes.get("order").unwrap().version, 2);
}

/// Test helper: activate the first job of `job_type` (locking it) and
/// complete it by key, returning the events from completion.
fn complete_one(engine: &mut Engine, job_type: &str) -> Vec<Event> {
    let job = engine
        .activate_jobs(job_type, "test-worker", 10, 60_000, 0)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("no activatable job of type {job_type}"));
    engine
        .apply_command(Command::complete_job(job.key))
        .unwrap()
}

#[test]
fn variable_spill_selects_only_job_parked_instances() {
    // A plain service-task instance is a spill candidate (parked on a job,
    // carries variables); spilling drops the payload and rehydration restores
    // it, and a spilled instance is no longer a candidate.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("k".to_string(), Value::Int(7));
    let events = engine
        .apply_command(Command::create_instance_with("order", vars))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();

    assert_eq!(engine.spillable_instances(10), vec![key]);
    assert_eq!(engine.resident_spillable_count(), 1);

    let payload = engine.spill_variables(key).expect("spillable");
    assert!(engine.is_variables_spilled(key));
    assert!(engine.instance(key).unwrap().variables.is_empty());
    assert!(
        engine.spillable_instances(10).is_empty(),
        "an already-spilled instance is not a candidate"
    );

    engine.rehydrate_variables(key, payload);
    assert!(!engine.is_variables_spilled(key));
    assert_eq!(
        engine.instance(key).unwrap().variables.get("k"),
        Some(&Value::Int(7))
    );
}

#[test]
fn leased_job_instance_is_still_spillable() {
    // Regression pin (ADR 0012): activating (leasing) a job to a worker keeps
    // the job indexed in `jobs_by_instance` and only adds it to `activated_jobs`,
    // so the instance remains a spill candidate. The worker already holds a copy
    // of the variables from activation; rehydration on completion/redelivery
    // restores them. (Corrects the prior claim that `is_spillable` excludes
    // leased jobs.)
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("k".to_string(), Value::Int(7));
    let events = engine
        .apply_command(Command::create_instance_with("order", vars))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.resident_spillable_count(), 1);

    let jobs = engine.activate_jobs("payment", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 1, "one job activated");
    assert!(
        engine.state().activated_jobs.contains(&jobs[0].key),
        "the job is leased"
    );
    assert_eq!(
        engine.resident_spillable_count(),
        1,
        "a leased-job instance is still spillable"
    );
    assert_eq!(engine.spillable_instances(10), vec![key]);
}

#[test]
fn completing_an_instance_drops_its_variables_from_hot_state() {
    // ADR 0012: a terminal instance's variables are never read from hot state
    // again, so completion drops the payload immediately (heap reclaimed without
    // waiting for exporter-driven eviction). The instance shell remains resident
    // (queryable) until eviction, but carries no variables.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("k".to_string(), Value::Int(7));
    let events = engine
        .apply_command(Command::create_instance_with("order", vars))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(!engine.instance(key).unwrap().variables.is_empty());

    // Run the single service task to completion.
    complete_one(&mut engine, "payment");

    let instance = engine.instance(key).expect("shell still resident");
    assert_eq!(
        instance.state,
        crate::state::ProcessInstanceState::Completed,
        "instance is terminal"
    );
    assert!(
        instance.variables.is_empty(),
        "a completed instance holds no variables in hot state"
    );
    assert_eq!(
        engine.resident_variable_bytes(),
        0,
        "terminal instance contributes no resident variable bytes"
    );
}

#[test]
fn terminating_an_instance_drops_its_variables_from_hot_state() {
    // ADR 0012: cancellation (→ Terminated) also drops the payload.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("k".to_string(), Value::Int(7));
    let events = engine
        .apply_command(Command::create_instance_with("order", vars))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(!engine.instance(key).unwrap().variables.is_empty());

    engine
        .apply_command(Command::cancel_instance(key))
        .expect("cancel");

    let instance = engine.instance(key).expect("shell still resident");
    assert_eq!(
        instance.state,
        crate::state::ProcessInstanceState::Terminated
    );
    assert!(
        instance.variables.is_empty(),
        "a terminated instance holds no variables in hot state"
    );
}

#[test]
fn cold_spill_round_trips_a_job_parked_instance() {
    // Snapshotting a job-parked instance lifts it (and its job) entirely out
    // of hot state; rehydrating restores it so the job is activatable and the
    // instance completes exactly as if it had never been spilled.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("k".to_string(), Value::Int(7));
    let events = engine
        .apply_command(Command::create_instance_with("order", vars))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();

    assert_eq!(engine.cold_spillable_instances(10), vec![key]);
    assert_eq!(engine.cold_spillable_count(), 1);

    let snapshot = engine.snapshot_instance(key).expect("snapshottable");
    assert_eq!(snapshot.instance.key, key);
    assert_eq!(
        snapshot.jobs.len(),
        1,
        "the parked job travels in the snapshot"
    );
    assert_eq!(snapshot.instance.variables.get("k"), Some(&Value::Int(7)));
    // Fully out of hot state: instance, job and job index all gone.
    assert!(engine.instance(key).is_none());
    assert!(!engine.state().jobs_by_instance.contains_key(&key));
    assert!(engine
        .activate_jobs("payment", "w", 10, 60_000, 0)
        .is_empty());

    engine.rehydrate_instance(snapshot);
    assert!(engine.instance(key).is_some());
    assert_eq!(
        engine.instance(key).unwrap().variables.get("k"),
        Some(&Value::Int(7))
    );
    // Job is activatable and completable again; the instance then completes.
    let completion = complete_one(&mut engine, "payment");
    assert!(completion.iter().any(
        |e| matches!(e, Event::ProcessInstanceCompleted { instance_key } if *instance_key == key)
    ));
}

#[cfg(feature = "serde")]
#[test]
fn engine_snapshot_round_trips_state_and_key_generator() {
    // A state snapshot must reproduce the materialized state exactly and
    // resume the key generator where it left off, so a node rebuilt from a
    // snapshot serves identical state and never mints a colliding key — the
    // correctness contract for bounded, state-based Raft snapshots.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    // One instance driven to completion (terminal; pruned from hot state but
    // retained for audit)...
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    complete_one(&mut engine, "payment");
    // ...and one left parked on its job (live working state).
    let e2 = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let parked = e2.iter().find_map(|e| e.instance_key()).unwrap();

    let snapshot = engine.snapshot();
    let serialized = serde_json::to_vec(&snapshot).expect("snapshot serializes");
    let decoded: EngineSnapshot =
        serde_json::from_slice(&serialized).expect("snapshot deserializes");
    let mut restored = Engine::from_snapshot(decoded);

    assert_eq!(
        restored.state(),
        engine.state(),
        "restored state equals the source state byte-for-byte"
    );

    // The parked instance and its job survive the round-trip and remain
    // completable on the restored engine.
    assert!(restored.instance(parked).is_some());
    let done = complete_one(&mut restored, "payment");
    assert!(done.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceCompleted { instance_key } if *instance_key == parked
    )));

    // The restored engine resumes minting keys without colliding with any key
    // the source engine already assigned.
    let e3 = restored
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let k3 = e3.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(
        !engine.state().instances.contains_key(&k3),
        "next minted key {k3} must not collide with a pre-snapshot key"
    );
    assert_ne!(k3, parked);
}

#[test]
fn cold_spill_round_trips_a_message_parked_instance() {
    // The long-lived case variable spill deliberately skips: an instance
    // parked on a message intermediate catch (no job at all). Cold spill lifts
    // it out wholesale and rehydration restores the subscription so a later
    // correlated message still resumes and completes it.
    let def = ProcessBuilder::new("wait")
        .start_event("start")
        .message_intermediate_catch_event("await", "approve", "orderId")
        .end_event("end")
        .connect("start", "await")
        .connect("await", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let mut vars = HashMap::new();
    vars.insert("orderId".to_string(), Value::Str("A".to_string()));
    let events = engine
        .apply_command(Command::create_instance_with("wait", vars))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // Parked on a message, no job: still a cold-spill candidate.
    assert_eq!(engine.cold_spillable_instances(10), vec![key]);
    let snapshot = engine.snapshot_instance(key).expect("snapshottable");
    assert_eq!(snapshot.message_subscriptions.len(), 1);
    assert!(engine.instance(key).is_none());
    // While cold the subscription is out of hot state entirely.
    assert!(engine.state().message_subscriptions.is_empty());

    engine.rehydrate_instance(snapshot);
    // Now the subscription is back: correlation resumes and completes it.
    let correlated = engine
        .apply_command(Command::correlate_message("approve", "A"))
        .unwrap();
    assert!(correlated.iter().any(
        |e| matches!(e, Event::ProcessInstanceCompleted { instance_key } if *instance_key == key)
    ));
}

#[test]
fn cold_spill_excludes_instances_with_a_locked_job() {
    // An instance whose job is currently activated (a worker holds the lock)
    // is mid-task, not dormant: it must not be a cold-spill candidate.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.cold_spillable_instances(10), vec![key]);

    // Activate (lock) the job — now the instance is busy.
    let _ = engine.activate_jobs("payment", "w", 1, 60_000, 0);
    assert!(
        engine.cold_spillable_instances(10).is_empty(),
        "an instance with a locked job is not cold-spillable"
    );
    assert!(
        engine.snapshot_instance(key).is_some(),
        "snapshot_instance itself is unconditional on lock state (host gates via the selector)"
    );
}

#[test]
fn variable_spill_excludes_instances_with_an_armed_boundary_timer() {
    // An instance parked on a service-task job that ALSO has an armed boundary
    // timer must NOT be spilled: the timer can resume the flow without job
    // activation (the host's rehydration seam), which would read empty
    // variables. start -> charge (service task, boundary timer PT5S) -> end.
    let def = ProcessBuilder::new("guarded")
        .start_event("start")
        .service_task("charge", "payment")
        .timer_boundary_event("deadline", "charge", 5_000)
        .end_event("end")
        .end_event("expired")
        .connect("start", "charge")
        .connect("charge", "end")
        .connect("deadline", "expired")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let mut vars = HashMap::new();
    vars.insert("k".to_string(), Value::Int(1));
    let events = engine
        .apply_command_at(Command::create_instance_with("guarded", vars), 1_000)
        .unwrap();
    let _key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The instance is parked on the payment job AND has an armed boundary
    // timer, so it is deliberately excluded from spill candidates.
    assert_eq!(
        engine.timers().len(),
        1,
        "a boundary timer is armed while the job is parked"
    );
    assert!(
        engine.spillable_instances(10).is_empty(),
        "instance with an armed boundary timer must not be spillable"
    );
    assert_eq!(engine.resident_spillable_count(), 0);
}

#[test]
fn should_park_on_timer_then_fire_when_due() {
    // start -> charge (service task) -> wait (timer PT5S) -> end
    let def = ProcessBuilder::new("delayed")
        .start_event("start")
        .service_task("charge", "payment")
        .timer_intermediate_catch_event("wait", 5_000)
        .end_event("end")
        .connect("start", "charge")
        .connect("charge", "wait")
        .connect("wait", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    // create instance at t=1000, then run the job so the token reaches the timer.
    let events = engine
        .apply_command_at(Command::create_instance("delayed"), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    let job = engine
        .activate_jobs("payment", "w", 1, 60_000, 1_000)
        .into_iter()
        .next()
        .unwrap();
    engine
        .apply_command_at(Command::complete_job(job.key), 1_000)
        .unwrap();

    // Token now parked on the timer, armed for due_at = 1000 + 5000 = 6000.
    assert!(!engine.is_completed(instance_key));
    let timers = engine.timers();
    assert_eq!(timers.len(), 1);
    assert_eq!(timers[0].state, state::TimerState::Created);
    assert_eq!(timers[0].due_at, 6_000);

    // A tick before the due instant fires nothing.
    let fired = engine.trigger_timers(5_999);
    assert!(fired.is_empty());
    assert!(!engine.is_completed(instance_key));

    // A tick at/after the due instant fires the timer and completes the instance.
    let fired = engine.trigger_timers(6_000);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::TimerTriggered { .. })));
    assert!(engine.is_completed(instance_key));
    assert!(fired.contains(&Event::ProcessInstanceCompleted { instance_key }));

    // The timer is retained as Triggered so a later tick never re-fires it.
    assert_eq!(engine.timers()[0].state, state::TimerState::Triggered);
    assert!(engine.trigger_timers(10_000).is_empty());
}

#[test]
fn should_recover_parked_timer_via_replay() {
    let def = ProcessBuilder::new("delayed")
        .start_event("start")
        .timer_intermediate_catch_event("wait", 5_000)
        .end_event("end")
        .connect("start", "wait")
        .connect("wait", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    let mut log = Vec::new();
    log.extend(engine.apply_command(Command::DeployProcess(def)).unwrap());
    log.extend(
        engine
            .apply_command_at(Command::create_instance("delayed"), 1_000)
            .unwrap(),
    );

    // Replay the durable log into a fresh engine; the parked timer survives.
    let mut recovered = Engine::replay(log);
    let timers = recovered.timers();
    assert_eq!(timers.len(), 1);
    assert_eq!(timers[0].state, state::TimerState::Created);
    let instance_key = timers[0].instance_key;
    assert!(!recovered.is_completed(instance_key));

    // The recovered engine fires the timer on the next due tick.
    recovered.trigger_timers(6_000);
    assert!(recovered.is_completed(instance_key));
}

/// start -> charge (service task, PT5S interrupting timer boundary) -> done
///                       \--(timer "timeout")--> escalated
fn process_with_timer_boundary() -> ProcessDefinition {
    ProcessBuilder::new("ship")
        .start_event("start")
        .service_task("charge", "payment")
        .timer_boundary_event("timeout", "charge", 5_000)
        .end_event("done")
        .end_event("escalated")
        .connect("start", "charge")
        .connect("charge", "done")
        .connect("timeout", "escalated")
        .build()
        .unwrap()
}

#[test]
fn should_fire_an_interrupting_timer_boundary_and_cancel_the_job() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_timer_boundary()))
        .unwrap();

    // Start at t=1000: the token parks on the service task, a job is created,
    // and the boundary timer is armed for due_at = 6000.
    let events = engine
        .apply_command_at(Command::create_instance("ship"), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.pending_jobs().len(), 1);
    let job_key = engine.pending_jobs()[0].key;
    assert_eq!(engine.timers().len(), 1);
    assert_eq!(engine.timers()[0].due_at, 6_000);
    assert!(!engine.is_completed(instance_key));

    // A tick before the due instant does nothing.
    assert!(engine.trigger_timers(5_999).is_empty());
    assert!(!engine.is_completed(instance_key));

    // At the due instant the timer interrupts the task: the job is cancelled,
    // the boundary's outgoing flow runs, and the instance completes.
    let fired = engine.trigger_timers(6_000);
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Canceled
    );
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::JobCanceled { job_key: k, .. } if *k == job_key)));
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "escalated"
    )));
    assert!(engine.is_completed(instance_key));
    assert_eq!(engine.timers()[0].state, state::TimerState::Triggered);
}

#[test]
fn should_disarm_a_boundary_timer_when_the_job_completes_first() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_timer_boundary()))
        .unwrap();
    let events = engine
        .apply_command_at(Command::create_instance("ship"), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // Completing the job before the timer fires takes the normal flow and
    // disarms the boundary timer.
    engine.activate_jobs("payment", "w", 1, 60_000, 1_000);
    engine
        .apply_command_at(Command::complete_job(job_key), 2_000)
        .unwrap();
    assert!(engine.is_completed(instance_key));
    assert_eq!(engine.timers()[0].state, state::TimerState::Canceled);

    // A later tick past the (now disarmed) due instant does nothing.
    assert!(engine.trigger_timers(10_000).is_empty());
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Completed
    );
}

#[test]
fn should_recover_an_armed_boundary_timer_via_replay() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    log.extend(
        engine
            .apply_command(Command::DeployProcess(process_with_timer_boundary()))
            .unwrap(),
    );
    log.extend(
        engine
            .apply_command_at(Command::create_instance("ship"), 1_000)
            .unwrap(),
    );

    // Replay: the armed boundary timer and its parked job survive.
    let mut recovered = Engine::replay(log);
    assert_eq!(recovered.timers().len(), 1);
    assert_eq!(recovered.timers()[0].state, state::TimerState::Created);
    let instance_key = recovered.timers()[0].instance_key;
    assert!(!recovered.is_completed(instance_key));

    // The recovered engine fires the boundary on the next due tick.
    recovered.trigger_timers(6_000);
    assert!(recovered.is_completed(instance_key));
    let job = recovered.state().jobs.values().next().unwrap();
    assert_eq!(job.state, state::JobState::Canceled);
}

/// start -> await (message catch "payment-received", correlationKey orderId)
///       -> end
fn process_with_message_catch() -> ProcessDefinition {
    ProcessBuilder::new("await-payment")
        .start_event("start")
        .message_intermediate_catch_event("await", "payment-received", "orderId")
        .end_event("end")
        .connect("start", "await")
        .connect("await", "end")
        .build()
        .unwrap()
}

fn vars(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn process_with_signal_catch() -> ProcessDefinition {
    ProcessBuilder::new("await-signal")
        .start_event("start")
        .signal_intermediate_catch_event("await", "all-clear")
        .end_event("end")
        .connect("start", "await")
        .connect("await", "end")
        .build()
        .unwrap()
}

#[test]
fn should_park_on_signal_catch_then_broadcast() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_signal_catch()))
        .unwrap();

    // Two instances both park on the signal catch.
    let a = engine
        .apply_command(Command::create_instance("await-signal"))
        .unwrap();
    let a_key = a.iter().find_map(|e| e.instance_key()).unwrap();
    let b = engine
        .apply_command(Command::create_instance("await-signal"))
        .unwrap();
    let b_key = b.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(!engine.is_completed(a_key));
    assert!(!engine.is_completed(b_key));
    assert_eq!(engine.signal_subscriptions().len(), 2);

    // A non-matching signal correlates nothing.
    let fired = engine.broadcast_signal("other", HashMap::new(), 0);
    assert!(!fired
        .iter()
        .any(|e| matches!(e, Event::SignalCorrelated { .. })));
    assert!(!engine.is_completed(a_key));

    // The matching broadcast fans out to BOTH instances, completing them.
    let fired = engine.broadcast_signal("all-clear", HashMap::new(), 0);
    assert_eq!(
        fired
            .iter()
            .filter(|e| matches!(e, Event::SignalCorrelated { .. }))
            .count(),
        2
    );
    assert!(engine.is_completed(a_key));
    assert!(engine.is_completed(b_key));

    // A repeat broadcast never re-correlates a settled subscription.
    let fired = engine.broadcast_signal("all-clear", HashMap::new(), 0);
    assert!(!fired
        .iter()
        .any(|e| matches!(e, Event::SignalCorrelated { .. })));
}

#[test]
fn should_interrupt_an_activity_via_a_signal_boundary() {
    let def = ProcessBuilder::new("guarded")
        .start_event("start")
        .service_task("work", "do-work")
        .signal_boundary_event("abort", "work", "kill-switch")
        .end_event("done")
        .end_event("aborted")
        .connect("start", "work")
        .connect("work", "done")
        .connect("abort", "aborted")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("guarded"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The task parks on a job, with a boundary signal subscription open.
    assert!(!engine.is_completed(instance_key));
    assert_eq!(engine.signal_subscriptions().len(), 1);

    // Broadcasting the signal interrupts the task (cancels its job) and routes
    // along the boundary's outgoing flow, completing the instance.
    let fired = engine.broadcast_signal("kill-switch", HashMap::new(), 0);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::SignalCorrelated { .. })));
    assert!(fired.iter().any(|e| matches!(e, Event::JobCanceled { .. })));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn should_park_on_message_catch_then_correlate() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_catch()))
        .unwrap();

    // Create an instance whose orderId resolves the correlation value "A".
    let events = engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The token parks on the catch event, opening one open subscription.
    assert!(!engine.is_completed(instance_key));
    let subs = engine.message_subscriptions();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].state, state::MessageSubscriptionState::Open);
    assert_eq!(subs[0].message_name, "payment-received");
    assert_eq!(subs[0].correlation_key, "A");

    // A non-matching correlation key correlates nothing.
    let fired = engine.correlate_message("payment-received", "B", HashMap::new(), 0);
    assert!(!fired
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
    assert!(!engine.is_completed(instance_key));

    // The matching message releases the token and completes the instance.
    let fired = engine.correlate_message("payment-received", "A", HashMap::new(), 0);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
    assert!(engine.is_completed(instance_key));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Correlated
    );

    // A repeat message never correlates the now-settled subscription twice.
    let fired = engine.correlate_message("payment-received", "A", HashMap::new(), 0);
    assert!(!fired
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
}

#[test]
fn feel_message_name_resolves_on_activation() {
    // The message name is a FEEL expression referencing an instance variable;
    // Zeebe evaluates it when the subscription opens (on activation).
    let def = ProcessBuilder::new("await-payment")
        .start_event("start")
        .message_intermediate_catch_event("await", "=\"payment-\" + region", "orderId")
        .end_event("end")
        .connect("start", "await")
        .connect("await", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let events = engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[
                ("orderId", Value::Str("A".into())),
                ("region", Value::Str("eu".into())),
            ]),
        ))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The subscription opened under the resolved name, not the raw expression.
    let subs = engine.message_subscriptions();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].message_name, "payment-eu");
    assert_eq!(subs[0].correlation_key, "A");

    // A message for a different region does not correlate.
    let fired = engine.correlate_message("payment-us", "A", HashMap::new(), 0);
    assert!(!fired
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
    assert!(!engine.is_completed(instance_key));

    // The resolved name correlates and completes the instance.
    let fired = engine.correlate_message("payment-eu", "A", HashMap::new(), 0);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn feel_signal_name_resolves_on_activation() {
    // The signal name is a FEEL expression referencing an instance variable,
    // evaluated when the signal subscription opens (on activation).
    let def = ProcessBuilder::new("await-signal")
        .start_event("start")
        .signal_intermediate_catch_event("await", "=\"clear-\" + zone")
        .end_event("end")
        .connect("start", "await")
        .connect("await", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let created = engine
        .apply_command(Command::create_instance_with(
            "await-signal",
            vars(&[("zone", Value::Str("north".into()))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.signal_subscriptions().len(), 1);

    // A broadcast for a different zone does not correlate.
    let fired = engine.broadcast_signal("clear-south", HashMap::new(), 0);
    assert!(!fired
        .iter()
        .any(|e| matches!(e, Event::SignalCorrelated { .. })));
    assert!(!engine.is_completed(key));

    // The resolved name completes the instance.
    let fired = engine.broadcast_signal("clear-north", HashMap::new(), 0);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::SignalCorrelated { .. })));
    assert!(engine.is_completed(key));
}

fn deploy_and_create_retryable(retries: Option<&str>, vars_in: HashMap<String, Value>) -> Engine {
    let mut builder = ProcessBuilder::new("retryable")
        .start_event("start")
        .service_task("work", "do-work")
        .end_event("end")
        .connect("start", "work")
        .connect("work", "end");
    if let Some(r) = retries {
        builder = builder.with_retries("work", r);
    }
    let def = builder.build().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with("retryable", vars_in))
        .unwrap();
    engine
}

#[test]
fn static_retries_declaration_sets_initial_job_retries() {
    let engine = deploy_and_create_retryable(Some("5"), HashMap::new());
    let job = engine.state().jobs.values().next().unwrap();
    assert_eq!(job.retries, 5);
}

#[test]
fn feel_retries_expression_resolves_against_variables() {
    let engine =
        deploy_and_create_retryable(Some("=maxRetries"), vars(&[("maxRetries", Value::Int(7))]));
    let job = engine.state().jobs.values().next().unwrap();
    assert_eq!(job.retries, 7);
}

#[test]
fn missing_retries_declaration_defaults_to_three() {
    let engine = deploy_and_create_retryable(None, HashMap::new());
    let job = engine.state().jobs.values().next().unwrap();
    assert_eq!(job.retries, state::DEFAULT_JOB_RETRIES);
}

/// The cross-partition (Zeebe-style) placement protocol for an intermediate
/// catch: the instance partition parks on an `Opening` record, the host routes
/// `OpenMessageSubscription` to the message partition (`hash(correlation_key)`),
/// a published message correlates there and yields a `RemoteMessageCorrelation`,
/// and the routed `CorrelateMessageSubscription` continuation advances the token
/// back on the instance partition. Two engines, commands hand-routed.
#[test]
fn cross_partition_catch_opens_remote_then_correlates_back() {
    const N: u64 = 2;
    // A correlation value that hashes onto the *other* partition (1), so the
    // subscription is placed off the instance partition (0).
    let order = ('a'..='z')
        .map(|c| c.to_string())
        .find(|k| state::subscription_partition(k, N) == 1)
        .expect("some key hashes to partition 1");

    let mut instance_engine = Engine::with_partition(0);
    instance_engine.set_num_partitions(N);
    instance_engine
        .apply_command(Command::DeployProcess(process_with_message_catch()))
        .unwrap();

    let mut message_engine = Engine::with_partition(1);
    message_engine.set_num_partitions(N);

    // Create the instance on partition 0; its token parks on an Opening record
    // (the subscription's canonical home is partition 1).
    let created = instance_engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str(order.clone()))]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(crate::partition_of(instance_key), 0);
    assert!(!instance_engine.is_completed(instance_key));

    // Exactly one Opening event, no local Open subscription.
    let opening = created
        .iter()
        .find_map(|e| match e {
            Event::MessageSubscriptionOpening {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id,
                message_name,
                correlation_key,
                kind,
            } => Some((
                *subscription_key,
                *instance_key,
                *element_instance_key,
                element_id.clone(),
                message_name.clone(),
                correlation_key.clone(),
                kind.clone(),
            )),
            _ => None,
        })
        .expect("an Opening event was emitted");
    assert!(!created
        .iter()
        .any(|e| matches!(e, Event::MessageSubscriptionCreated { .. })));
    assert_eq!(
        instance_engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Opening
    );

    // Host routes the open to the message partition: it records the canonical
    // Open subscription.
    let (sub_key, inst_key, eik, eid, name, ckey, kind) = opening;
    assert_eq!(ckey, order);
    message_engine
        .apply_command(Command::OpenMessageSubscription {
            subscription_key: sub_key,
            instance_key: inst_key,
            element_instance_key: eik,
            element_id: eid,
            message_name: name,
            correlation_key: ckey,
            kind,
        })
        .unwrap();
    assert_eq!(
        message_engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );

    // Re-routing the same open is idempotent (no second subscription).
    message_engine
        .apply_command(Command::OpenMessageSubscription {
            subscription_key: sub_key,
            instance_key: inst_key,
            element_instance_key: eik,
            element_id: "await".into(),
            message_name: "payment-received".into(),
            correlation_key: order.clone(),
            kind: state::MessageSubscriptionKind::IntermediateCatch,
        })
        .unwrap();
    assert_eq!(message_engine.message_subscriptions().len(), 1);

    // Publish lands on the message partition. It settles the canonical sub and
    // emits a RemoteMessageCorrelation (no local token to advance there).
    let published = message_engine
        .apply_command(Command::correlate_message_with(
            "payment-received",
            order.clone(),
            vars(&[("paid", Value::Bool(true))]),
        ))
        .unwrap();
    assert!(!published
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
    let remote = published
        .iter()
        .find_map(|e| match e {
            Event::RemoteMessageCorrelation {
                subscription_key,
                message_key,
                instance_key,
                element_instance_key,
                element_id,
                kind,
                variables,
            } => Some((
                *subscription_key,
                *message_key,
                *instance_key,
                *element_instance_key,
                element_id.clone(),
                kind.clone(),
                variables.clone(),
            )),
            _ => None,
        })
        .expect("a RemoteMessageCorrelation was emitted");
    assert_eq!(
        message_engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Correlated
    );

    // Host routes the continuation back to the instance partition: the token
    // advances and the instance completes, merging the message variables.
    let (r_sub, r_msg, r_inst, r_eik, r_eid, r_kind, r_vars) = remote;
    assert_eq!(r_sub, sub_key);
    assert_eq!(crate::partition_of(r_inst), 0);
    let advanced = instance_engine
        .apply_command(Command::CorrelateMessageSubscription {
            subscription_key: r_sub,
            message_key: r_msg,
            instance_key: r_inst,
            element_instance_key: r_eik,
            element_id: r_eid,
            kind: r_kind,
            variables: r_vars,
        })
        .unwrap();
    assert!(advanced
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
    assert!(instance_engine.is_completed(instance_key));
    assert_eq!(
        instance_engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Correlated
    );

    // Re-delivering the continuation is a no-op (at-least-once safe).
    let again = instance_engine
        .apply_command(Command::CorrelateMessageSubscription {
            subscription_key: sub_key,
            message_key: r_msg,
            instance_key: r_inst,
            element_instance_key: r_eik,
            element_id: "await".into(),
            kind: state::MessageSubscriptionKind::IntermediateCatch,
            variables: HashMap::new(),
        })
        .unwrap();
    assert!(!again
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
}

/// Cancelling an instance whose catch subscription is canonically placed on
/// another partition emits a routable `MessageSubscriptionClosing` (carrying
/// the message name + correlation key the host needs to address the message
/// partition), and the local placeholder transitions to `Canceled`. The
/// canonical record is then disarmed by routing a `CloseMessageSubscription`
/// to the message partition, which settles its own copy.
#[test]
fn cross_partition_cancel_emits_a_routable_closing() {
    const N: u64 = 2;
    let order = ('a'..='z')
        .map(|c| c.to_string())
        .find(|k| state::subscription_partition(k, N) == 1)
        .expect("some key hashes to partition 1");

    let mut instance_engine = Engine::with_partition(0);
    instance_engine.set_num_partitions(N);
    instance_engine
        .apply_command(Command::DeployProcess(process_with_message_catch()))
        .unwrap();
    let mut message_engine = Engine::with_partition(1);
    message_engine.set_num_partitions(N);

    // Park the instance on partition 0 with a cross-partition Opening, then
    // open the canonical subscription on partition 1.
    let created = instance_engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str(order.clone()))]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let opening = created
        .iter()
        .find_map(|e| match e {
            Event::MessageSubscriptionOpening {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id,
                message_name,
                correlation_key,
                kind,
            } => Some(Command::OpenMessageSubscription {
                subscription_key: *subscription_key,
                instance_key: *instance_key,
                element_instance_key: *element_instance_key,
                element_id: element_id.clone(),
                message_name: message_name.clone(),
                correlation_key: correlation_key.clone(),
                kind: kind.clone(),
            }),
            _ => None,
        })
        .expect("an Opening was emitted");
    message_engine.apply_command(opening).unwrap();
    assert_eq!(
        message_engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );

    // Cancel on the instance partition: a routable Closing is emitted for the
    // off-partition placeholder, carrying the routing payload.
    let canceled = instance_engine
        .apply_command(Command::cancel_instance(instance_key))
        .unwrap();
    let closing = canceled
        .iter()
        .find_map(|e| match e {
            Event::MessageSubscriptionClosing {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id,
                message_name,
                correlation_key,
            } => Some(Command::CloseMessageSubscription {
                subscription_key: *subscription_key,
                instance_key: *instance_key,
                element_instance_key: *element_instance_key,
                element_id: {
                    assert_eq!(message_name, "payment-received");
                    assert_eq!(correlation_key, &order);
                    element_id.clone()
                },
            }),
            _ => None,
        })
        .expect("a routable Closing was emitted for the cross-partition sub");
    assert!(
        !canceled
            .iter()
            .any(|e| matches!(e, Event::MessageSubscriptionCanceled { .. })),
        "the off-partition placeholder routes a Closing, not a local Canceled"
    );
    assert_eq!(
        instance_engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Canceled
    );

    // Host routes the Close to the message partition, disarming the canonical
    // record so a later publish correlates nothing.
    message_engine.apply_command(closing).unwrap();
    assert_eq!(
        message_engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Canceled
    );
    let published = message_engine
        .apply_command(Command::correlate_message_with(
            "payment-received",
            order.clone(),
            HashMap::new(),
        ))
        .unwrap();
    assert!(
        !published
            .iter()
            .any(|e| matches!(e, Event::RemoteMessageCorrelation { .. })),
        "the disarmed canonical sub correlates nothing"
    );
}

#[test]
fn should_publish_a_message_with_no_subscription() {
    let mut engine = Engine::new();
    // With nothing subscribed, a published message is minted and dropped.
    let fired = engine.correlate_message("nobody-home", "X", HashMap::new(), 0);
    assert_eq!(fired.len(), 1);
    assert!(matches!(fired[0], Event::MessagePublished { .. }));
    assert!(engine.message_subscriptions().is_empty());
}

#[test]
fn should_merge_message_variables_on_correlation() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_catch()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    let fired = engine.correlate_message(
        "payment-received",
        "A",
        vars(&[("amount", Value::Int(42))]),
        0,
    );

    // The message payload is merged into the instance — the merge is carried on
    // the durable `VariablesUpdated` event (the exporter's source of truth). The
    // instance then runs to completion, which drops its hot-state variables
    // (ADR 0012), so the merged value is asserted on the event, not hot state.
    let merged = fired
        .iter()
        .find_map(|e| match e {
            Event::VariablesUpdated {
                instance_key: k,
                variables,
            } if *k == instance_key => variables.get("amount").cloned(),
            _ => None,
        })
        .expect("correlation emits a VariablesUpdated carrying the payload");
    assert_eq!(merged, Value::Int(42));
    assert!(engine.is_completed(instance_key));
    assert!(
        engine.state().instances[&instance_key].variables.is_empty(),
        "a completed instance holds no variables in hot state (ADR 0012)"
    );
}

#[test]
fn should_correlate_only_the_instance_with_the_matching_key() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_catch()))
        .unwrap();
    let a = engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let b = engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str("B".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Correlating "A" releases only instance A; B stays parked.
    engine.correlate_message("payment-received", "A", HashMap::new(), 0);
    assert!(engine.is_completed(a));
    assert!(!engine.is_completed(b));
}

#[test]
fn should_recover_an_open_message_subscription_via_replay() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    log.extend(
        engine
            .apply_command(Command::DeployProcess(process_with_message_catch()))
            .unwrap(),
    );
    log.extend(
        engine
            .apply_command(Command::create_instance_with(
                "await-payment",
                vars(&[("orderId", Value::Str("A".into()))]),
            ))
            .unwrap(),
    );

    // Replay: the open subscription and its parked token survive.
    let mut recovered = Engine::replay(log);
    let subs = recovered.message_subscriptions();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].state, state::MessageSubscriptionState::Open);
    let instance_key = subs[0].instance_key;
    assert!(!recovered.is_completed(instance_key));

    // The recovered engine correlates the message and completes the instance.
    recovered.correlate_message("payment-received", "A", HashMap::new(), 0);
    assert!(recovered.is_completed(instance_key));
}

/// start -> charge (service task, interrupting message boundary "cancel"
///          correlating on orderId) -> done
///                       \--(message)--> aborted
fn process_with_message_boundary() -> ProcessDefinition {
    ProcessBuilder::new("cancellable")
        .start_event("start")
        .service_task("charge", "payment")
        .message_boundary_event("cancel", "charge", "order-cancelled", "orderId")
        .end_event("done")
        .end_event("aborted")
        .connect("start", "charge")
        .connect("charge", "done")
        .connect("cancel", "aborted")
        .build()
        .unwrap()
}

#[test]
fn should_fire_an_interrupting_message_boundary_and_cancel_the_job() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_boundary()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance_with(
            "cancellable",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The token parks on the service task; a job and a boundary subscription
    // are created.
    assert_eq!(engine.pending_jobs().len(), 1);
    let job_key = engine.pending_jobs()[0].key;
    assert_eq!(engine.message_subscriptions().len(), 1);

    // Correlating the boundary message interrupts the task: the job is
    // cancelled, the boundary's outgoing flow runs, the instance completes.
    let fired = engine.correlate_message("order-cancelled", "A", HashMap::new(), 0);
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Canceled
    );
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::JobCanceled { job_key: k, .. } if *k == job_key)));
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "cancel" && to == "aborted"
    )));
    assert!(engine.is_completed(instance_key));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Correlated
    );
}

#[test]
fn should_cancel_a_message_boundary_subscription_when_the_job_completes_first() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_boundary()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance_with(
            "cancellable",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // Completing the job before any message arrives takes the normal flow and
    // cancels the boundary subscription.
    engine.activate_jobs("payment", "w", 1, 60_000, 0);
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(engine.is_completed(instance_key));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Canceled
    );

    // A later message for the (now cancelled) subscription correlates nothing.
    let fired = engine.correlate_message("order-cancelled", "A", HashMap::new(), 0);
    assert!(!fired
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
}

/// start -> charge (service task, PT5S NON-interrupting timer boundary
///                   "remind") -> done
///                       \--(timer)--> reminded
fn process_with_non_interrupting_timer_boundary() -> ProcessDefinition {
    ProcessBuilder::new("ship")
        .start_event("start")
        .service_task("charge", "payment")
        .non_interrupting_timer_boundary_event("remind", "charge", 5_000)
        .end_event("done")
        .end_event("reminded")
        .connect("start", "charge")
        .connect("charge", "done")
        .connect("remind", "reminded")
        .build()
        .unwrap()
}

#[test]
fn should_fire_a_non_interrupting_timer_boundary_without_cancelling_the_job() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_non_interrupting_timer_boundary(),
        ))
        .unwrap();

    let events = engine
        .apply_command_at(Command::create_instance("ship"), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.pending_jobs().len(), 1);
    let job_key = engine.pending_jobs()[0].key;
    assert_eq!(engine.timers()[0].due_at, 6_000);

    // At the due instant the timer fires but does NOT interrupt: the job
    // survives, the boundary's outgoing flow spawns a parallel token to
    // "reminded", and the instance stays active (the task is still running).
    let fired = engine.trigger_timers(6_000);
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Created
    );
    assert!(!fired.iter().any(|e| matches!(e, Event::JobCanceled { .. })));
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "remind" && to == "reminded"
    )));
    assert!(!engine.is_completed(instance_key));
    assert_eq!(engine.timers()[0].state, state::TimerState::Triggered);

    // Completing the job then runs the normal flow and finishes the instance.
    engine.activate_jobs("payment", "w", 1, 60_000, 0);
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(engine.is_completed(instance_key));
}

/// start -> charge (service task, NON-interrupting message boundary "notify"
///                   on message "reminder" correlating orderId) -> done
///                       \--(message)--> notified
fn process_with_non_interrupting_message_boundary() -> ProcessDefinition {
    ProcessBuilder::new("notifiable")
        .start_event("start")
        .service_task("charge", "payment")
        .non_interrupting_message_boundary_event("notify", "charge", "reminder", "orderId")
        .end_event("done")
        .end_event("notified")
        .connect("start", "charge")
        .connect("charge", "done")
        .connect("notify", "notified")
        .build()
        .unwrap()
}

#[test]
fn should_fire_a_non_interrupting_message_boundary_for_every_matching_message() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_non_interrupting_message_boundary(),
        ))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance_with(
            "notifiable",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.pending_jobs()[0].key;
    assert_eq!(engine.message_subscriptions().len(), 1);

    // First message: spawns a parallel token to "notified" without cancelling
    // the job; the subscription stays open and the instance stays active.
    let fired = engine.correlate_message("reminder", "A", HashMap::new(), 0);
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Created
    );
    assert!(!fired.iter().any(|e| matches!(e, Event::JobCanceled { .. })));
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "notify" && to == "notified"
    )));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );
    assert!(!engine.is_completed(instance_key));

    // A second matching message fires the boundary again (open subscription).
    let fired = engine.correlate_message("reminder", "A", HashMap::new(), 0);
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "notify" && to == "notified"
    )));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );

    // Completing the job runs the normal flow, finishes the instance and
    // cancels the still-open boundary subscription.
    engine.activate_jobs("payment", "w", 1, 60_000, 0);
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(engine.is_completed(instance_key));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Canceled
    );
}

#[test]
fn should_park_on_service_task_then_complete_on_job() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    let events = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    assert_eq!(engine.pending_jobs().len(), 1);
    assert!(!engine.is_completed(instance_key));

    let job_key = engine.pending_jobs()[0].key;
    engine.activate_jobs("payment", "worker-1", 10, 60_000, 0);
    let events = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();

    assert!(engine.is_completed(instance_key));
    assert!(events.contains(&Event::ProcessInstanceCompleted { instance_key }));
    assert!(engine.pending_jobs().is_empty());
}

#[test]
fn should_complete_immediately_when_no_task() {
    let def = ProcessBuilder::new("noop")
        .start_event("s")
        .end_event("e")
        .connect("s", "e")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("noop"))
        .unwrap();

    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(engine.is_completed(instance_key));
    assert!(events.contains(&Event::ProcessInstanceCompleted { instance_key }));
}

#[test]
fn should_run_parallel_split_and_join() {
    // s -> split =< a, b >= join -> e   (a and b are service tasks)
    let def = ProcessBuilder::new("par")
        .start_event("s")
        .parallel_gateway("split")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .parallel_gateway("join")
        .end_event("e")
        .connect("s", "split")
        .connect("split", "a")
        .connect("split", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect("join", "e")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("par"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // given both branches forked and both tasks are waiting
    assert_eq!(engine.pending_jobs().len(), 2);
    assert!(!engine.is_completed(instance_key));

    // when the first branch's job completes, the join must still wait
    complete_one(&mut engine, "ja");
    assert!(!engine.is_completed(instance_key));

    // when the second branch completes, the join fires and the instance ends
    let final_events = complete_one(&mut engine, "jb");
    assert!(engine.is_completed(instance_key));
    assert!(final_events.contains(&Event::ProcessInstanceCompleted { instance_key }));
    // exactly one ProcessInstanceCompleted across the whole run
    assert_eq!(
        final_events
            .iter()
            .filter(|e| matches!(e, Event::ProcessInstanceCompleted { .. }))
            .count(),
        1
    );
}

fn approval_process() -> ProcessDefinition {
    // s -> g(xor): decision==yes -> approved ; else default -> rejected
    ProcessBuilder::new("approval")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("approved")
        .end_event("rejected")
        .connect("s", "g")
        .connect_when("g", "approved", r#"decision = "yes""#)
        .connect("g", "rejected")
        .build()
        .unwrap()
}

#[test]
fn should_route_exclusive_gateway_by_variable() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(approval_process()))
        .unwrap();

    let vars = HashMap::from([("decision".to_string(), Value::Str("yes".into()))]);
    let events = engine
        .apply_command(Command::create_instance_with("approval", vars))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(engine.is_completed(instance_key));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "approved"
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "rejected"
    )));
}

#[test]
fn should_take_default_flow_when_no_condition_matches() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(approval_process()))
        .unwrap();

    let vars = HashMap::from([("decision".to_string(), Value::Str("no".into()))]);
    let events = engine
        .apply_command(Command::create_instance_with("approval", vars))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(engine.is_completed(instance_key));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "rejected"
    )));
}

#[test]
fn should_take_default_flow_even_when_listed_before_the_conditional() {
    // Regression: an exclusive gateway's explicit default flow must be a
    // fallback only — never selected by document order. Here the default
    // (g -> rejected) is connected BEFORE the conditional (g -> approved),
    // the order Camunda often serialises.
    let process = ProcessBuilder::new("approval")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("approved")
        .end_event("rejected")
        .connect("s", "g")
        .connect_default("g", "rejected")
        .connect_when("g", "approved", r#"decision = "yes""#)
        .build()
        .unwrap();

    // decision == yes -> the conditional flow wins despite appearing last.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process.clone()))
        .unwrap();
    let vars = HashMap::from([("decision".to_string(), Value::Str("yes".into()))]);
    let events = engine
        .apply_command(Command::create_instance_with("approval", vars))
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "approved"
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "rejected"
    )));

    // decision != yes -> the default flow is the fallback.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process))
        .unwrap();
    let vars = HashMap::from([("decision".to_string(), Value::Str("no".into()))]);
    let events = engine
        .apply_command(Command::create_instance_with("approval", vars))
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "rejected"
    )));
}

#[test]
fn should_route_exclusive_gateway_on_a_numeric_feel_comparison() {
    // A richer FEEL condition than equality: amount > 100 -> big ; else small.
    let def = ProcessBuilder::new("amounts")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("big")
        .end_event("small")
        .connect("s", "g")
        .connect_when("g", "big", "amount > 100")
        .connect("g", "small")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let vars = HashMap::from([("amount".to_string(), Value::Int(250))]);
    let events = engine
        .apply_command(Command::create_instance_with("amounts", vars))
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "big"
    )));
}

#[test]
fn should_raise_an_expression_incident_when_a_condition_cannot_evaluate() {
    // The condition compares a string variable to a number — a FEEL type
    // error — so the gateway raises an ExpressionEvaluation incident rather
    // than silently treating the flow as not taken.
    let def = ProcessBuilder::new("typed")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("yes_end")
        .connect("s", "g")
        .connect_when("g", "yes_end", "name > 10")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let vars = HashMap::from([("name".to_string(), Value::Str("ann".into()))]);
    let created = engine
        .apply_command(Command::create_instance_with("typed", vars))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].kind, state::IncidentKind::ExpressionEvaluation);
    assert!(!engine.is_completed(instance_key));
}

#[test]
fn should_park_on_a_user_task_then_resume_when_completed() {
    // start -> review (user task) -> end
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task("review")
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let created = engine
        .apply_command(Command::create_instance("approval"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The token parks on the user task: a UserTaskCreated event was emitted
    // and the instance is not yet complete.
    let user_task_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .expect("user task created");
    assert!(!engine.is_completed(instance_key));

    // Assigning keeps it parked; the assignee is recorded.
    engine
        .apply_command(Command::assign_user_task(user_task_key, "alice"))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&user_task_key]
            .assignee
            .as_deref(),
        Some("alice")
    );
    assert!(!engine.is_completed(instance_key));

    // Completing it (merging a variable) resumes the token to the end event,
    // completing the instance.
    let vars = HashMap::from([("approved".to_string(), Value::Bool(true))]);
    let done = engine
        .apply_command(Command::complete_user_task_with(user_task_key, vars))
        .unwrap();
    assert!(done.iter().any(|e| matches!(
        e,
        Event::UserTaskCompleted { user_task_key: k, .. } if *k == user_task_key
    )));
    assert!(engine.is_completed(instance_key));
    assert_eq!(
        engine.state().user_tasks[&user_task_key].state,
        state::UserTaskState::Completed
    );

    // Completing again is rejected: the task is no longer active.
    assert!(matches!(
        engine.apply_command(Command::complete_user_task(user_task_key)),
        Err(EngineError::UserTaskNotActive { .. })
    ));
}

#[test]
fn should_create_a_user_task_with_resolved_attributes() {
    use crate::model::UserTaskProps;
    // A user task declaring assignee/candidates/dates/priority, partly via
    // FEEL expressions evaluated against the instance variables.
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task_with(
            "review",
            UserTaskProps {
                assignee: Some("=requester".to_string()),
                candidate_groups: Some("ops,finance".to_string()),
                candidate_users: Some("=reviewers".to_string()),
                due_date: Some("2025-01-01T00:00:00Z".to_string()),
                follow_up_date: None,
                priority: Some("=urgency".to_string()),
            },
        )
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let vars = HashMap::from([
        ("requester".to_string(), Value::Str("alice".to_string())),
        (
            "reviewers".to_string(),
            Value::List(vec![
                Value::Str("bob".to_string()),
                Value::Str("carol".to_string()),
            ]),
        ),
        ("urgency".to_string(), Value::Int(80)),
    ]);
    let created = engine
        .apply_command(Command::CreateInstance {
            process_id: "approval".to_string(),
            variables: vars,
            tags: Vec::new(),
            business_id: None,
        })
        .unwrap();
    let user_task_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .expect("user task created");

    let task = &engine.state().user_tasks[&user_task_key];
    assert_eq!(task.assignee.as_deref(), Some("alice"));
    assert_eq!(task.candidate_groups, vec!["ops", "finance"]);
    assert_eq!(task.candidate_users, vec!["bob", "carol"]);
    assert_eq!(task.due_date.as_deref(), Some("2025-01-01T00:00:00Z"));
    assert_eq!(task.follow_up_date, None);
    assert_eq!(task.priority, 80);
}

#[test]
fn should_default_user_task_priority_to_fifty() {
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task("review")
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("approval"))
        .unwrap();
    let user_task_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .unwrap();
    assert_eq!(engine.state().user_tasks[&user_task_key].priority, 50);
}

#[test]
fn should_reject_reassigning_an_assigned_task_without_override() {
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task("review")
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("approval"))
        .unwrap();
    let user_task_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .unwrap();

    // First assignment succeeds.
    engine
        .apply_command(Command::assign_user_task(user_task_key, "alice"))
        .unwrap();
    // A non-override reassignment is rejected while assigned.
    assert!(matches!(
        engine.apply_command(Command::AssignUserTask {
            user_task_key,
            assignee: "bob".to_string(),
            allow_override: false,
        }),
        Err(EngineError::UserTaskAlreadyAssigned { .. })
    ));
    // Unassigning then assigning again works.
    engine
        .apply_command(Command::unassign_user_task(user_task_key))
        .unwrap();
    assert_eq!(engine.state().user_tasks[&user_task_key].assignee, None);
    engine
        .apply_command(Command::AssignUserTask {
            user_task_key,
            assignee: "bob".to_string(),
            allow_override: false,
        })
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&user_task_key]
            .assignee
            .as_deref(),
        Some("bob")
    );
}

#[test]
fn should_update_user_task_attributes_via_changeset() {
    use crate::UserTaskChangeset;
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task("review")
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("approval"))
        .unwrap();
    let user_task_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .unwrap();

    engine
        .apply_command(Command::update_user_task(
            user_task_key,
            UserTaskChangeset {
                candidate_groups: Some(vec!["ops".to_string()]),
                candidate_users: None,
                due_date: Some(Some("2025-06-01T00:00:00Z".to_string())),
                follow_up_date: None,
                priority: Some(20),
            },
        ))
        .unwrap();

    let task = &engine.state().user_tasks[&user_task_key];
    assert_eq!(task.candidate_groups, vec!["ops"]);
    assert_eq!(task.due_date.as_deref(), Some("2025-06-01T00:00:00Z"));
    assert_eq!(task.priority, 20);

    // Resetting the due date with an empty string clears it.
    engine
        .apply_command(Command::update_user_task(
            user_task_key,
            UserTaskChangeset {
                due_date: Some(Some(String::new())),
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(engine.state().user_tasks[&user_task_key].due_date, None);
}

#[test]
fn should_route_on_variables_returned_by_a_completed_job() {
    // s -> task(decide) -> g(xor): decision==yes -> approved ; else rejected
    let def = ProcessBuilder::new("review")
        .start_event("s")
        .service_task("decide", "decision")
        .exclusive_gateway("g")
        .end_event("approved")
        .end_event("rejected")
        .connect("s", "decide")
        .connect("decide", "g")
        .connect_when("g", "approved", r#"decision = "yes""#)
        .connect("g", "rejected")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("review"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // given the instance parked on the service task
    assert!(!engine.is_completed(instance_key));

    // when the worker completes the job, returning decision=yes
    let job_key = engine.activate_jobs("decision", "w", 1, 60_000, 0)[0].key;
    let vars = HashMap::from([("decision".to_string(), Value::Str("yes".into()))]);
    let events = engine
        .apply_command(Command::complete_job_with(job_key, vars))
        .unwrap();

    // then the gateway routes on the returned variable to the approved branch
    assert!(engine.is_completed(instance_key));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "approved"
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "rejected"
    )));
}

#[test]
fn should_raise_incident_when_no_exclusive_flow_matches() {
    // Both flows are conditional; neither matches -> incident, token parked.
    let def = ProcessBuilder::new("strict")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("yes_end")
        .end_event("no_end")
        .connect("s", "g")
        .connect_when("g", "yes_end", "d = true")
        .connect_when("g", "no_end", "d = false")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let vars = HashMap::from([("d".to_string(), Value::Int(7))]);
    let events = engine
        .apply_command(Command::create_instance_with("strict", vars))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(!engine.is_completed(instance_key));
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::IncidentRaised { .. })));
    assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);
}

#[test]
fn should_re_activate_a_failed_job_that_still_has_retries() {
    // given an activated job
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;

    // when the worker fails it with retries remaining
    engine
        .apply_command(Command::fail_job(job_key, 2, "transient error"))
        .unwrap();

    // then no incident is raised and the job is activatable again
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
    assert_eq!(engine.pending_jobs().len(), 1);
    let reactivated = engine.activate_jobs("payment", "B", 10, 60_000, 1);
    assert_eq!(reactivated.len(), 1);
    assert_eq!(reactivated[0].key, job_key);
    assert_eq!(reactivated[0].retries, 2);
}

#[test]
fn should_raise_an_incident_when_a_job_fails_with_no_retries_left() {
    // given an activated job
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;

    // when the worker fails it with no retries left
    let events = engine
        .apply_command(Command::fail_job(job_key, 0, "boom"))
        .unwrap();

    // then an incident is raised, the job parks, and it is not activatable
    assert!(events.iter().any(|e| matches!(
        e,
        Event::IncidentRaised { reason, .. } if reason == "boom"
    )));
    assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);
    assert!(engine.pending_jobs().is_empty());
    assert!(engine
        .activate_jobs("payment", "B", 10, 60_000, 100)
        .is_empty());

    // and the parked job can no longer be completed
    let err = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotActive { job_key });
}

#[test]
fn should_reject_failing_a_job_that_was_never_activated() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.pending_jobs()[0].key;

    let err = engine
        .apply_command(Command::fail_job(job_key, 1, "nope"))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotActivated { job_key });
}

#[test]
fn should_recover_a_parked_job_by_updating_retries_and_resolving_its_incident() {
    // given a job parked on a no-retries incident
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;
    let raised = engine
        .apply_command(Command::fail_job(job_key, 0, "boom"))
        .unwrap();
    let incident_key = raised
        .iter()
        .find_map(|e| match e {
            Event::IncidentRaised { incident_key, .. } => Some(*incident_key),
            _ => None,
        })
        .unwrap();

    // when resolving before retries are restored, it is rejected
    let err = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::IncidentNotResolvable { incident_key: k, .. } if k == incident_key
    ));

    // when retries are updated and the incident resolved
    engine
        .apply_command(Command::update_job_retries(job_key, 2))
        .unwrap();
    let resolved = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    // then the incident is retained as resolved and the job is activatable again
    assert!(resolved
        .iter()
        .any(|e| matches!(e, Event::IncidentResolved { .. })));
    assert_eq!(
        engine.incident(incident_key).unwrap().state,
        state::IncidentState::Resolved
    );
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());

    // and a worker can pick it up and drive the instance to completion
    let job_key2 = engine.activate_jobs("payment", "B", 1, 60_000, 100)[0].key;
    assert_eq!(job_key2, job_key);
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(engine.is_completed(instance_key));
}

#[test]
fn should_reject_resolving_an_unknown_incident() {
    let mut engine = Engine::new();
    let err = engine
        .apply_command(Command::resolve_incident(999))
        .unwrap_err();
    assert_eq!(err, EngineError::IncidentNotFound { incident_key: 999 });
}

#[test]
fn should_re_raise_a_gateway_incident_when_resolution_still_finds_no_flow() {
    // A non-job incident (no matching exclusive flow) is resolved by
    // re-evaluating the gateway. With the variables unchanged it still
    // matches nothing, so resolution retries the work and a *fresh* incident
    // is raised — the token stays parked rather than silently vanishing.
    let def = ProcessBuilder::new("strict")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("yes_end")
        .connect("s", "g")
        .connect_when("g", "yes_end", "d = true")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let vars = HashMap::from([("d".to_string(), Value::Int(7))]);
    let created = engine
        .apply_command(Command::create_instance_with("strict", vars))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let original = engine.incidents()[0].key;
    assert!(engine.incident(original).unwrap().job_key.is_none());

    // when resolved (the gateway is re-evaluated)
    engine
        .apply_command(Command::resolve_incident(original))
        .unwrap();

    // then the original incident is retained as resolved and a new active one
    // replaces it, and the instance has still not completed.
    assert_eq!(
        engine.incident(original).unwrap().state,
        state::IncidentState::Resolved
    );
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1);
    assert_ne!(active[0].key, original);
    assert_eq!(active[0].kind, state::IncidentKind::NoMatchingSequenceFlow);
    assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);
    assert!(!engine.is_completed(instance_key));
}

#[test]
fn should_recover_a_gateway_incident_after_fixing_variables() {
    // given an exclusive gateway parked on a no-matching-flow incident
    let def = ProcessBuilder::new("strict")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("yes_end")
        .connect("s", "g")
        .connect_when("g", "yes_end", "d = true")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let vars = HashMap::from([("d".to_string(), Value::Int(7))]);
    let created = engine
        .apply_command(Command::create_instance_with("strict", vars))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let incident_key = engine.incidents()[0].key;

    // when the operator fixes the variable then resolves the incident
    engine
        .apply_command(Command::set_variables(
            instance_key,
            HashMap::from([("d".to_string(), Value::Bool(true))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    // then the gateway re-evaluates, matches, and the instance completes; the
    // incident is retained as resolved
    assert_eq!(
        engine.incident(incident_key).unwrap().state,
        state::IncidentState::Resolved
    );
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
    assert!(engine.is_completed(instance_key));
}

#[test]
fn should_set_variables_via_an_element_instance_scope_key() {
    // given a service task parked with a known element instance key
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let element_instance_key = engine.pending_jobs()[0].element_instance_key;
    let instance_key = engine.pending_jobs()[0].instance_key;

    // when variables are set against the element instance key (not the
    // process instance key)
    engine
        .apply_command(Command::set_variables(
            element_instance_key,
            HashMap::from([("x".to_string(), Value::Int(42))]),
        ))
        .unwrap();

    // then they land in the owning instance's single variable scope
    assert_eq!(
        engine.instance(instance_key).unwrap().variables.get("x"),
        Some(&Value::Int(42))
    );
}

#[test]
fn should_reject_setting_variables_on_an_unknown_scope() {
    let mut engine = Engine::new();
    let err = engine
        .apply_command(Command::set_variables(
            404,
            HashMap::from([("x".to_string(), Value::Int(1))]),
        ))
        .unwrap_err();
    assert_eq!(err, EngineError::ScopeNotFound { scope_key: 404 });
}

#[test]
fn should_retry_the_service_task_when_an_unhandled_error_incident_is_resolved() {
    // given a service task whose worker threw an uncaught business error,
    // parking the token on an unhandled-error incident
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;
    let raised = engine
        .apply_command(Command::throw_job_error(job_key, "BOOM", "no boundary"))
        .unwrap();
    let incident_key = raised
        .iter()
        .find_map(|e| match e {
            Event::IncidentRaised { incident_key, .. } => Some(*incident_key),
            _ => None,
        })
        .unwrap();
    assert!(engine.pending_jobs().is_empty());

    // when the incident is resolved
    let resolved = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    // then a fresh job is created for the still-active service task, and a
    // worker can activate and complete it to drive the instance home.
    assert!(resolved
        .iter()
        .any(|e| matches!(e, Event::JobCreated { .. })));
    assert_eq!(
        engine.incident(incident_key).unwrap().state,
        state::IncidentState::Resolved
    );
    assert_eq!(engine.pending_jobs().len(), 1);
    let retry = engine.activate_jobs("payment", "B", 1, 60_000, 100);
    assert_eq!(retry.len(), 1);
    assert_ne!(retry[0].key, job_key);
    engine
        .apply_command(Command::complete_job(retry[0].key))
        .unwrap();
    assert!(engine.is_completed(instance_key));
}

#[test]
fn should_stamp_an_incident_with_the_command_clock() {
    // given a parked job-incident raised at a known instant
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;

    // when failed with no retries at now = 1_700_000_000_000
    let raised = engine
        .apply_command_at(Command::fail_job(job_key, 0, "boom"), 1_700_000_000_000)
        .unwrap();
    let incident_key = raised
        .iter()
        .find_map(|e| match e {
            Event::IncidentRaised { incident_key, .. } => Some(*incident_key),
            _ => None,
        })
        .unwrap();

    // then the incident records that instant
    assert_eq!(
        engine.incident(incident_key).unwrap().created_at,
        1_700_000_000_000
    );
}

#[test]
fn should_retain_a_resolved_incident_as_an_audit_record() {
    // given a parked job-incident
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;
    let raised = engine
        .apply_command(Command::fail_job(job_key, 0, "boom"))
        .unwrap();
    let incident_key = raised
        .iter()
        .find_map(|e| match e {
            Event::IncidentRaised { incident_key, .. } => Some(*incident_key),
            _ => None,
        })
        .unwrap();
    engine
        .apply_command(Command::update_job_retries(job_key, 2))
        .unwrap();

    // when resolved with an operation reference at a known instant
    engine
        .apply_command_at(
            Command::resolve_incident_with(incident_key, 4242),
            1_700_000_000_500,
        )
        .unwrap();

    // then the record is retained as resolved with audit metadata
    let incident = engine.incident(incident_key).unwrap();
    assert_eq!(incident.state, state::IncidentState::Resolved);
    assert_eq!(incident.resolved_at, Some(1_700_000_000_500));
    assert_eq!(incident.operation_reference, Some(4242));
    // and it no longer counts as active, so the instance has no open incident
    assert!(engine.active_incidents().is_empty());
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());

    // and resolving it again is rejected (already resolved)
    let err = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::IncidentNotResolvable { incident_key: k, .. } if k == incident_key
    ));
}

fn process_with_error_boundary() -> ProcessDefinition {
    // s -> charge(task) --normal--> done
    //              \--(error CARD_DECLINED)--> boundary -> declined
    ProcessBuilder::new("payment")
        .start_event("s")
        .service_task("charge", "payment")
        .error_boundary_event("boundary", "charge", "CARD_DECLINED")
        .end_event("done")
        .end_event("declined")
        .connect("s", "charge")
        .connect("charge", "done")
        .connect("boundary", "declined")
        .build()
        .unwrap()
}

#[test]
fn should_route_to_an_error_boundary_when_a_job_throws_a_matching_error() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_error_boundary()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("payment"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("payment", "w", 1, 60_000, 0)[0].key;

    // when the worker throws the caught business error
    let events = engine
        .apply_command(Command::throw_job_error(
            job_key,
            "CARD_DECLINED",
            "card was declined",
        ))
        .unwrap();

    // then the activity is interrupted and the error path runs to completion
    assert!(engine.is_completed(instance_key));
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "boundary" && to == "declined"
    )));
    // the task's normal outgoing flow was NOT taken
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "done"
    )));
    // and the job is consumed: it cannot be completed afterwards
    let err = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotActive { job_key });
}

#[test]
fn should_raise_an_incident_when_a_thrown_error_is_unhandled() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_error_boundary()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("payment"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("payment", "w", 1, 60_000, 0)[0].key;

    // when the worker throws an error no boundary catches
    let events = engine
        .apply_command(Command::throw_job_error(job_key, "UNKNOWN", "boom"))
        .unwrap();

    // then an incident is raised and the instance does not complete
    assert!(events.iter().any(|e| matches!(
        e,
        Event::IncidentRaised { reason, .. } if reason.contains("UNKNOWN")
    )));
    assert!(!engine.is_completed(instance_key));
    assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);
}

#[test]
fn should_reject_throwing_an_error_from_a_job_that_was_never_activated() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_error_boundary()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("payment"))
        .unwrap();
    let job_key = engine.pending_jobs()[0].key;

    let err = engine
        .apply_command(Command::throw_job_error(job_key, "CARD_DECLINED", "x"))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotActivated { job_key });
}

/// start -> sub[ sub_start -> inner(work) -> sub_end ] --normal--> done
///                               (sub catches BUSINESS_ERROR)
///          sub --(error boundary)--> sad(sad-flow) -> sad_end
fn process_with_subprocess_error_boundary() -> ProcessDefinition {
    ProcessBuilder::new("sub-error")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "work")
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .error_boundary_event("boundary", "sub", "BUSINESS_ERROR")
        .service_task("sad", "sad-flow")
        .end_event("done")
        .end_event("sad_end")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "done")
        .connect("boundary", "sad")
        .connect("sad", "sad_end")
        .build()
        .unwrap()
}

#[test]
fn should_run_an_embedded_subprocess_to_completion_on_the_happy_path() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_subprocess_error_boundary(),
        ))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("sub-error"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The token enters the sub-process and parks on its inner service task.
    assert!(!engine.is_completed(instance_key));
    assert_eq!(engine.pending_jobs().len(), 1);
    assert_eq!(engine.pending_jobs()[0].job_type, "work");

    // Completing the inner job drains the sub-process scope, which then
    // routes out its normal outgoing flow to the outer end event.
    let events = complete_one(&mut engine, "work");
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "sub" && to == "done"
    )));
    assert!(engine.is_completed(instance_key));
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
}

#[test]
fn should_interrupt_an_embedded_subprocess_via_its_error_boundary() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_subprocess_error_boundary(),
        ))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("sub-error"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("work", "w", 1, 60_000, 0)[0].key;

    // The inner job throws a business error the sub-process boundary catches.
    let events = engine
        .apply_command(Command::throw_job_error(job_key, "BUSINESS_ERROR", "boom"))
        .unwrap();

    // The whole sub-process is interrupted: its inner task instance is
    // completed (terminated), the sub-process completes without taking its
    // normal flow, and the boundary routes to the sad-flow path.
    assert!(events.iter().any(|e| matches!(
        e,
        Event::ElementCompleted { element_id, .. } if element_id == "inner"
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "boundary" && to == "sad"
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "done"
    )));
    // The interrupted inner job is consumed and cannot be completed.
    let err = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotActive { job_key });

    // The instance is not yet complete: it is parked on the sad-flow task.
    assert!(!engine.is_completed(instance_key));
    let sad = complete_one(&mut engine, "sad-flow");
    assert!(sad.contains(&Event::ProcessInstanceCompleted { instance_key }));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn should_raise_an_incident_when_a_subprocess_error_is_unhandled() {
    // A sub-process with no error boundary: an error thrown inside is
    // unhandled and parks on an incident (the instance does not complete).
    let def = ProcessBuilder::new("sub-plain")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "work")
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("sub-plain"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("work", "w", 1, 60_000, 0)[0].key;

    let events = engine
        .apply_command(Command::throw_job_error(job_key, "BOOM", "kaboom"))
        .unwrap();

    assert!(events.iter().any(|e| matches!(
        e,
        Event::IncidentRaised { reason, .. } if reason.contains("BOOM")
    )));
    assert!(!engine.is_completed(instance_key));
    assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);
}

/// start -> charge (service task, PT5S NON-interrupting CYCLE timer boundary
///                   "tick") -> done
///                       \--(timer, every 5s)--> ticked
fn process_with_non_interrupting_cycle_timer_boundary() -> ProcessDefinition {
    ProcessBuilder::new("ticker")
        .start_event("start")
        .service_task("charge", "payment")
        .non_interrupting_timer_cycle_boundary_event("tick", "charge", 5_000)
        .end_event("done")
        .end_event("ticked")
        .connect("start", "charge")
        .connect("charge", "done")
        .connect("tick", "ticked")
        .build()
        .unwrap()
}

#[test]
fn should_re_arm_a_non_interrupting_cycle_timer_boundary_on_every_fire() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_non_interrupting_cycle_timer_boundary(),
        ))
        .unwrap();
    let events = engine
        .apply_command_at(Command::create_instance("ticker"), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.pending_jobs()[0].key;
    assert_eq!(engine.timers()[0].due_at, 6_000);

    // First fire at t=6000: spawns a token to "ticked", does not cancel the
    // job, and re-arms a fresh timer due at 11000 (6000 + 5000).
    let fired = engine.trigger_timers(6_000);
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "tick" && to == "ticked"
    )));
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Created
    );
    let armed: Vec<u64> = engine
        .timers()
        .iter()
        .filter(|t| t.state == state::TimerState::Created)
        .map(|t| t.due_at)
        .collect();
    assert_eq!(
        armed,
        vec![11_000],
        "a fresh timer is armed for the next interval"
    );
    assert!(!engine.is_completed(instance_key));

    // Second fire at t=11000: fires again and re-arms for 16000.
    let fired = engine.trigger_timers(11_000);
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "tick" && to == "ticked"
    )));
    let armed: Vec<u64> = engine
        .timers()
        .iter()
        .filter(|t| t.state == state::TimerState::Created)
        .map(|t| t.due_at)
        .collect();
    assert_eq!(armed, vec![16_000]);

    // Completing the job runs the normal flow and disarms the pending timer.
    engine.activate_jobs("payment", "w", 1, 60_000, 0);
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(engine.is_completed(instance_key));
    assert!(engine
        .timers()
        .iter()
        .all(|t| t.state != state::TimerState::Created));
}

/// start -> sub[ sub_start -> inner(work) -> sub_end ] --normal--> done
///          sub --(PT5S interrupting timer boundary)--> escalated
fn process_with_subprocess_timer_boundary() -> ProcessDefinition {
    ProcessBuilder::new("sub-timer")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "work")
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .timer_boundary_event("timeout", "sub", 5_000)
        .end_event("done")
        .end_event("escalated")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "done")
        .connect("timeout", "escalated")
        .build()
        .unwrap()
}

#[test]
fn should_interrupt_an_embedded_subprocess_via_a_timer_boundary() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_subprocess_timer_boundary(),
        ))
        .unwrap();
    let created = engine
        .apply_command_at(Command::create_instance("sub-timer"), 1_000)
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The token parks on the inner task; the boundary timer is armed on the
    // sub-process for due_at = 6000.
    let job_key = engine.pending_jobs()[0].key;
    assert_eq!(engine.pending_jobs()[0].job_type, "work");
    assert!(engine
        .timers()
        .iter()
        .any(|t| t.due_at == 6_000 && t.element_id == "sub"));

    // At the due instant the timer interrupts the WHOLE sub-process: the
    // inner job is cancelled, the inner task and sub-process complete without
    // taking the normal flow, and the boundary routes to "escalated".
    let fired = engine.trigger_timers(6_000);
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Canceled
    );
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::ElementCompleted { element_id, .. } if element_id == "inner"
    )));
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "escalated"
    )));
    assert!(!fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "done"
    )));
    assert!(engine.is_completed(instance_key));
}

/// start -> sub[ sub_start -> inner(work) -> sub_end ] --normal--> done
///          sub --(interrupting message boundary "cancel" on orderId)--> aborted
fn process_with_subprocess_message_boundary() -> ProcessDefinition {
    ProcessBuilder::new("sub-msg")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "work")
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .message_boundary_event("cancel", "sub", "order-cancelled", "orderId")
        .end_event("done")
        .end_event("aborted")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "done")
        .connect("cancel", "aborted")
        .build()
        .unwrap()
}

#[test]
fn should_interrupt_an_embedded_subprocess_via_a_message_boundary() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_subprocess_message_boundary(),
        ))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "sub-msg",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.pending_jobs()[0].key;
    assert!(engine
        .message_subscriptions()
        .iter()
        .any(|s| s.element_id == "sub"));

    // Correlating the boundary message interrupts the whole sub-process: the
    // inner job is cancelled, the inner task and sub-process complete, and the
    // boundary routes to "aborted" instead of the normal flow.
    let fired = engine.correlate_message("order-cancelled", "A", HashMap::new(), 0);
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Canceled
    );
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::ElementCompleted { element_id, .. } if element_id == "inner"
    )));
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "cancel" && to == "aborted"
    )));
    assert!(!fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "done"
    )));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn should_reject_unknown_process() {
    let mut engine = Engine::new();
    let err = engine
        .apply_command(Command::create_instance("missing"))
        .unwrap_err();
    assert_eq!(
        err,
        EngineError::ProcessNotFound {
            process_id: "missing".into()
        }
    );
}

#[test]
fn should_resolve_feel_variable_reference_job_type_at_job_creation() {
    // given a process whose service task type is a FEEL variable reference
    let def = ProcessBuilder::new("dynamic")
        .start_event("start")
        .service_task("work", "=jobType")
        .end_event("end")
        .connect("start", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    // when an instance is created with jobType bound to a concrete value
    let mut vars = HashMap::new();
    vars.insert("jobType".to_string(), Value::Str("payment".to_string()));
    engine
        .apply_command(Command::create_instance_with("dynamic", vars))
        .unwrap();

    // then the created job carries the resolved type, not the literal "=jobType"
    let jobs = engine.pending_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_type, "payment");
    // and it is activatable by the resolved type
    assert_eq!(engine.activate_jobs("payment", "w", 10, 1_000, 0).len(), 1);
}

#[test]
fn should_fall_back_to_literal_when_job_type_variable_is_missing() {
    // given the same process but no jobType variable provided
    let def = ProcessBuilder::new("dynamic")
        .start_event("start")
        .service_task("work", "=jobType")
        .end_event("end")
        .connect("start", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance("dynamic"))
        .unwrap();

    // then the unresolved expression falls back to the literal text (no panic)
    let jobs = engine.pending_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_type, "=jobType");
}

#[test]
fn should_reject_unknown_job() {
    let mut engine = Engine::new();
    let err = engine.apply_command(Command::complete_job(42)).unwrap_err();
    assert_eq!(err, EngineError::JobNotFound { job_key: 42 });
}

#[test]
fn should_reject_completing_a_job_that_was_never_activated() {
    // given an instance parked on a service task with a created (but
    // un-activated) job
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // when it is completed without being activated first
    let err = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap_err();

    // then it is rejected
    assert_eq!(err, EngineError::JobNotActivated { job_key });
}

#[test]
fn lenient_completion_accepts_a_job_that_was_never_activated() {
    // given a replica engine in lenient-completion mode (leader-local
    // activation: this replica never saw the job activated)
    let mut engine = Engine::new();
    engine.set_lenient_completion(true);
    assert!(engine.lenient_completion());
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // when a replicated completion arrives for the un-activated job, it is
    // applied (the leader held the lock; possession of the key is the
    // capability) instead of being rejected as JobNotActivated
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();

    // then the job is gone and the instance advanced past the service task
    assert!(engine.pending_jobs().is_empty());
}

#[test]
fn recover_lease_restores_a_soft_lease_and_holds_redelivery_until_the_deadline() {
    // given a newly-promoted leader that has the job in Created state (it
    // replicated the create but, under leader-local activation, never saw the
    // previous leader's activation) and a digested lease (key, deadline=1000)
    let mut engine = Engine::new();
    engine.set_lenient_completion(true);
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // when it recovers the lease from the digest at t=0
    assert!(engine.recover_lease(job_key, 1_000, 0));

    // then the job is no longer activatable (held until the deadline), so a
    // worker activation before the deadline gets nothing
    assert!(engine.pending_jobs().is_empty());
    assert!(engine
        .activate_jobs("payment", "W", 10, 1_000, 500)
        .is_empty());

    // and once the original deadline passes, the leader-local expiry tick
    // reclaims it and it is redelivered (at-least-once, honouring the deadline)
    engine.expire_jobs(1_500);
    let reactivated = engine.activate_jobs("payment", "W", 10, 1_000, 1_500);
    assert_eq!(reactivated.len(), 1);
    assert_eq!(reactivated[0].key, job_key);
}

#[test]
fn recover_lease_is_idempotent_and_respects_an_expired_deadline() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // a lease whose deadline has already passed is not recovered
    assert!(!engine.recover_lease(job_key, 1_000, 1_000));
    assert_eq!(engine.pending_jobs().len(), 1);

    // a live lease is recovered and surfaces in activated_leases
    assert!(engine.recover_lease(job_key, 2_000, 1_000));
    assert_eq!(engine.activated_leases(), vec![(job_key, 2_000)]);

    // recovering again on an already-activated job is a no-op
    assert!(!engine.recover_lease(job_key, 3_000, 1_000));
    assert_eq!(engine.activated_leases(), vec![(job_key, 2_000)]);

    // an unknown key is a no-op
    assert!(!engine.recover_lease(999_999, 5_000, 1_000));
}

#[test]
fn should_lock_an_activated_job_until_its_deadline() {
    // given an instance parked on a service task
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();

    // when worker A activates the job at t=0 for 1000ms
    let activated = engine.activate_jobs("payment", "A", 10, 1_000, 0);
    assert_eq!(activated.len(), 1);
    assert_eq!(activated[0].worker, "A");
    assert_eq!(activated[0].deadline, 1_000);

    // then a second activation before the deadline gets nothing
    assert!(engine
        .activate_jobs("payment", "B", 10, 1_000, 500)
        .is_empty());

    // and once the lock has expired and the periodic expiry tick reclaims it,
    // the job is activatable again
    engine.expire_jobs(1_500);
    let reactivated = engine.activate_jobs("payment", "B", 10, 1_000, 1_500);
    assert_eq!(reactivated.len(), 1);
    assert_eq!(reactivated[0].worker, "B");
}

#[test]
fn should_let_a_previous_worker_complete_after_re_activation() {
    // given worker A activated the job, then its lock expired and worker B
    // re-activated it (e.g. A's work outran the activation window)
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    let job_key = engine.activate_jobs("payment", "A", 10, 1_000, 0)[0].key;
    // A's lock expires; the periodic expiry tick returns the job to the pool.
    engine.expire_jobs(1_500);
    let reactivated = engine.activate_jobs("payment", "B", 10, 1_000, 1_500);
    assert_eq!(reactivated[0].key, job_key);

    // when the slow worker A finally completes the job by key
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();

    // then completion succeeds and the instance finishes
    assert!(engine.is_completed(instance_key));

    // and B can no longer complete the already-completed job
    let err = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotActive { job_key });
}

#[test]
fn should_expire_locks_on_tick() {
    // given an activated (locked) job
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 1_000, 0)[0].key;
    assert!(engine.pending_jobs().is_empty());

    // when a tick runs after the deadline
    engine.expire_jobs(2_000);

    // then the job is activatable again
    assert_eq!(engine.pending_jobs().len(), 1);
    assert_eq!(engine.pending_jobs()[0].key, job_key);
}

#[test]
fn should_dispatch_jobs_to_a_callback_worker() {
    // given an instance parked on a service task
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // when a callback worker polls and handles the job
    let mut seen = Vec::new();
    let handled = engine.poll_jobs("payment", "cb", 10, 60_000, 0, |job| {
        seen.push(job.job_type.clone());
        Some(HashMap::new())
    });

    // then the job was dispatched and completed, finishing the instance
    assert_eq!(handled, 1);
    assert_eq!(seen, ["payment"]);
    assert!(engine.is_completed(instance_key));
}

#[test]
fn should_be_deterministic_and_replayable() {
    let run = || {
        let mut engine = Engine::new();
        let mut all = Vec::new();
        all.extend(
            engine
                .apply_command(Command::DeployProcess(linear_with_task()))
                .unwrap(),
        );
        all.extend(
            engine
                .apply_command(Command::create_instance("order"))
                .unwrap(),
        );
        let job_key = engine.pending_jobs()[0].key;
        all.extend(
            engine
                .apply_command(Command::activate_jobs("payment", "worker-1", 1, 60_000, 0))
                .unwrap(),
        );
        all.extend(
            engine
                .apply_command(Command::complete_job(job_key))
                .unwrap(),
        );
        (engine, all)
    };

    let (engine_a, log_a) = run();
    let (_engine_b, log_b) = run();
    assert_eq!(log_a, log_b);

    // Replaying the log over a fresh State reconstructs engine state exactly.
    let mut replayed = State::new();
    for event in &log_a {
        state::apply(&mut replayed, event);
    }
    assert_eq!(&replayed, engine_a.state());
}

#[test]
fn should_recover_state_and_key_generator_via_replay() {
    // given a run that deploys, starts an instance, and raises an incident
    let (engine_a, log) = {
        let mut engine = Engine::new();
        let mut log = Vec::new();
        log.extend(
            engine
                .apply_command(Command::DeployProcess(linear_with_task()))
                .unwrap(),
        );
        log.extend(
            engine
                .apply_command(Command::create_instance("order"))
                .unwrap(),
        );
        let job_key = engine.pending_jobs()[0].key;
        engine
            .apply_command(Command::activate_jobs("payment", "w", 1, 60_000, 0))
            .unwrap();
        // fail with no retries -> parks the job and raises an incident
        log.extend(
            engine
                .apply_command_at(Command::fail_job(job_key, 0, "boom"), 1_234)
                .unwrap(),
        );
        (engine, log)
    };

    // when the durable log is replayed into a fresh engine
    // (activation events are volatile and intentionally not part of `log`)
    let mut recovered = Engine::replay(log);

    // then state matches (modulo the volatile activation: the replayed job
    // is parked Failed, identical to the original after fail)
    let orig_incident = engine_a.active_incidents()[0];
    let rec_incident = recovered.active_incidents()[0];
    assert_eq!(rec_incident.key, orig_incident.key);
    assert_eq!(rec_incident.created_at, 1_234);
    assert_eq!(rec_incident.kind, state::IncidentKind::JobNoRetries);

    // and the key generator resumes past every replayed key: a new instance
    // mints a strictly higher key than anything in the recovered log
    let max_existing = recovered
        .state()
        .instances
        .keys()
        .chain(recovered.state().jobs.keys())
        .chain(recovered.state().incidents.keys())
        .copied()
        .max()
        .unwrap();
    let events = recovered
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let new_instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(
        new_instance_key > max_existing,
        "new key {new_instance_key} must exceed replayed max {max_existing}"
    );
}

// ---- message start events ----

/// (message "order-placed") --> start -> end
fn process_with_message_start() -> ProcessDefinition {
    ProcessBuilder::new("order-flow")
        .message_start_event("start", "order-placed")
        .end_event("end")
        .connect("start", "end")
        .build()
        .unwrap()
}

#[test]
fn should_open_a_message_start_subscription_at_deploy() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_start()))
        .unwrap();

    // Deploy opens a process-level subscription but creates no instance.
    assert_eq!(engine.state().message_start_subscriptions.len(), 1);
    assert!(engine.state().instances.is_empty());
    let sub = &engine.state().message_start_subscriptions["order-placed"];
    assert_eq!(sub.process_id, "order-flow");
    assert_eq!(sub.start_element_id, "start");
}

#[test]
fn should_create_an_instance_when_a_message_start_correlates() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_start()))
        .unwrap();

    // A non-matching message creates nothing.
    engine.correlate_message("other", "", HashMap::new(), 0);
    assert!(engine.state().instances.is_empty());

    // The matching message creates and runs a fresh instance to completion,
    // seeding it with the message's variables.
    let fired = engine.correlate_message("order-placed", "", vars(&[("amount", Value::Int(7))]), 0);
    let (instance_key, seeded) = fired
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated {
                instance_key,
                variables,
                ..
            } => Some((*instance_key, variables.get("amount").cloned())),
            _ => None,
        })
        .unwrap();
    // The instance is seeded with the message's variables (carried on the
    // durable ProcessInstanceCreated event) and runs to completion, which drops
    // its hot-state variables (ADR 0012).
    assert_eq!(seeded, Some(Value::Int(7)));
    assert!(engine.is_completed(instance_key));
    assert!(engine.state().instances[&instance_key].variables.is_empty());
}

#[test]
fn feel_message_start_name_resolves_at_deploy() {
    // A message-start-event name expression is evaluated at deploy time against
    // an empty context (Zeebe parity); the resolved value keys the subscription.
    let def = ProcessBuilder::new("order-flow")
        .message_start_event("start", "=\"order-\" + \"placed\"")
        .end_event("end")
        .connect("start", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    // The subscription is keyed by the resolved name, not the raw expression.
    assert_eq!(engine.state().message_start_subscriptions.len(), 1);
    assert!(engine
        .state()
        .message_start_subscriptions
        .contains_key("order-placed"));

    // A message under the resolved name creates an instance.
    let fired = engine.correlate_message("order-placed", "", HashMap::new(), 0);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceCreated { .. })));
}

#[test]
fn should_create_one_instance_per_matching_message_start() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_start()))
        .unwrap();

    // Each matching message creates a distinct instance.
    engine.correlate_message("order-placed", "", HashMap::new(), 0);
    engine.correlate_message("order-placed", "", HashMap::new(), 0);
    assert_eq!(engine.state().instances.len(), 2);
}

#[test]
fn message_start_distributes_created_instances_across_partitions() {
    // On a multi-partition deploy owner, message-start correlations must NOT
    // pile every created instance onto the deploy partition. The round-robin
    // dispatcher keeps the first inline (target == self) and emits a routable
    // `StartInstanceDispatched` (carrying the chosen target) for the rest.
    const N: u64 = 4;
    let mut engine = Engine::with_partition(0);
    engine.set_num_partitions(N);
    engine
        .apply_command(Command::DeployProcess(process_with_message_start()))
        .unwrap();

    let mut dispatched_targets = Vec::new();
    let mut inline_instances = 0;
    for _ in 0..N {
        let fired = engine.correlate_message("order-placed", "", HashMap::new(), 0);
        for e in &fired {
            match e {
                Event::ProcessInstanceCreated { .. } => inline_instances += 1,
                Event::StartInstanceDispatched {
                    process_id,
                    target_partition,
                    ..
                } => {
                    assert_eq!(process_id, "order-flow");
                    dispatched_targets.push(*target_partition);
                }
                _ => {}
            }
        }
    }

    // Exactly one lands inline (rr=0 -> target 0 == self); the other three
    // dispatch to partitions 1, 2, 3 in round-robin order.
    assert_eq!(inline_instances, 1, "the first correlation creates inline");
    assert_eq!(
        dispatched_targets,
        vec![1, 2, 3],
        "subsequent correlations dispatch round-robin to the other partitions"
    );
    assert_eq!(
        engine.state().instances.len(),
        1,
        "only the inline instance lives on the deploy partition"
    );
}

#[test]
fn dispatch_start_instance_mints_the_instance_locally() {
    // The command a routed `StartInstanceDispatched` becomes on the target
    // partition: it mints the start-triggered instance in that partition's
    // own key namespace and runs it.
    const N: u64 = 4;
    let mut target = Engine::with_partition(2);
    target.set_num_partitions(N);
    target
        .apply_command(Command::DeployProcess(process_with_message_start()))
        .unwrap();
    assert!(target.state().instances.is_empty());

    let fired = target
        .apply_command(Command::DispatchStartInstance {
            process_id: "order-flow".into(),
            start_element_id: "start".into(),
            variables: vars(&[("amount", Value::Int(9))]),
            tags: Vec::new(),
            business_id: None,
        })
        .unwrap();

    let (instance_key, seeded) = fired
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated {
                instance_key,
                variables,
                ..
            } => Some((*instance_key, variables.get("amount").cloned())),
            _ => None,
        })
        .expect("the dispatch mints an instance");
    assert_eq!(
        crate::state::partition_of(instance_key),
        2,
        "the instance is minted in the target partition's namespace"
    );
    // The dispatched variables seed the instance (carried on the durable
    // ProcessInstanceCreated event); the instance then completes and drops its
    // hot-state variables (ADR 0012), so the seed is asserted on the event.
    assert_eq!(
        seeded,
        Some(Value::Int(9)),
        "the dispatched variables seed the instance"
    );
}

#[test]
fn should_recover_a_message_start_subscription_via_replay() {
    let mut engine = Engine::new();
    let log = engine
        .apply_command(Command::DeployProcess(process_with_message_start()))
        .unwrap();

    // Replay: the process-level subscription survives and still fires.
    let mut recovered = Engine::replay(log);
    assert_eq!(recovered.state().message_start_subscriptions.len(), 1);
    let fired = recovered.correlate_message("order-placed", "", HashMap::new(), 0);
    let instance_key = fired.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(recovered.is_completed(instance_key));
}

// ---- timer start events ----

/// (timer, one-shot PT10S) --> start -> end
fn process_with_timer_start_once() -> ProcessDefinition {
    ProcessBuilder::new("delayed-start")
        .timer_start_event_once("start", 10_000)
        .end_event("end")
        .connect("start", "end")
        .build()
        .unwrap()
}

/// (timer, cycle every 10S) --> start -> end
fn process_with_timer_start_cycle() -> ProcessDefinition {
    ProcessBuilder::new("recurring-start")
        .timer_start_event_cycle("start", 10_000)
        .end_event("end")
        .connect("start", "end")
        .build()
        .unwrap()
}

#[test]
fn should_arm_a_start_timer_at_deploy() {
    let mut engine = Engine::new();
    // Deploy at t=1000: the start timer is armed for 1000 + 10000 = 11000.
    engine
        .apply_command_at(
            Command::DeployProcess(process_with_timer_start_once()),
            1_000,
        )
        .unwrap();
    assert_eq!(engine.state().start_timers.len(), 1);
    let timer = engine.state().start_timers.values().next().unwrap();
    assert_eq!(timer.due_at, Some(11_000));
    assert!(engine.state().instances.is_empty());
}

#[test]
fn should_fire_a_one_shot_start_timer_exactly_once() {
    let mut engine = Engine::new();
    engine
        .apply_command_at(
            Command::DeployProcess(process_with_timer_start_once()),
            1_000,
        )
        .unwrap();

    // A tick before the due instant creates nothing.
    assert!(engine.trigger_timers(10_999).is_empty());
    assert!(engine.state().instances.is_empty());

    // At the due instant the timer fires and creates one instance; the timer
    // is retained but has no due time, so it never fires again.
    let fired = engine.trigger_timers(11_000);
    let instance_key = fired.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(engine.is_completed(instance_key));
    assert_eq!(engine.state().instances.len(), 1);
    let timer = engine.state().start_timers.values().next().unwrap();
    assert_eq!(timer.due_at, None);

    // A later tick fires nothing more.
    assert!(engine.trigger_timers(100_000).is_empty());
    assert_eq!(engine.state().instances.len(), 1);
}

#[test]
fn should_re_arm_a_cycle_start_timer_after_each_fire() {
    let mut engine = Engine::new();
    engine
        .apply_command_at(
            Command::DeployProcess(process_with_timer_start_cycle()),
            1_000,
        )
        .unwrap();

    // First fire at 11000 creates an instance and re-arms for 21000.
    engine.trigger_timers(11_000);
    assert_eq!(engine.state().instances.len(), 1);
    let timer = engine.state().start_timers.values().next().unwrap();
    assert_eq!(timer.due_at, Some(21_000));

    // Second fire at 21000 creates another and re-arms for 31000.
    engine.trigger_timers(21_000);
    assert_eq!(engine.state().instances.len(), 2);
    let timer = engine.state().start_timers.values().next().unwrap();
    assert_eq!(timer.due_at, Some(31_000));
}

#[test]
fn should_recover_an_armed_start_timer_via_replay() {
    let mut engine = Engine::new();
    let log = engine
        .apply_command_at(
            Command::DeployProcess(process_with_timer_start_once()),
            1_000,
        )
        .unwrap();

    // Replay: the armed start timer survives and still fires on the next tick.
    let mut recovered = Engine::replay(log);
    assert_eq!(recovered.state().start_timers.len(), 1);
    assert_eq!(
        recovered
            .state()
            .start_timers
            .values()
            .next()
            .unwrap()
            .due_at,
        Some(11_000)
    );
    let fired = recovered.trigger_timers(11_000);
    let instance_key = fired.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(recovered.is_completed(instance_key));
}

#[test]
fn evicts_only_completed_instances_and_what_they_own() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // One instance that we drive to completion, and one left in-flight
    // (parked on its service-task job).
    let done = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    complete_one(&mut engine, "payment");
    assert!(engine.is_completed(done));

    let live = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert!(!engine.is_completed(live));
    // The live instance still owns an activatable job.
    assert!(engine.state().jobs.values().any(|j| j.instance_key == live));

    // Evicting an active instance is a no-op.
    assert!(!engine.evict_instance(live));
    assert!(engine.instance(live).is_some());

    // Evicting the completed one removes it and its jobs.
    assert!(engine.evict_instance(done));
    assert!(engine.instance(done).is_none());
    assert!(!engine.state().jobs.values().any(|j| j.instance_key == done));

    // The in-flight instance and its job are untouched, and the deployed
    // definition (not instance-scoped) is retained.
    assert!(engine.instance(live).is_some());
    assert!(engine.state().jobs.values().any(|j| j.instance_key == live));
    assert_eq!(engine.state().processes.len(), 1);
}

#[test]
fn evict_instances_batches_in_one_pass() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // Two completed instances and one left in-flight.
    let done1 = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    complete_one(&mut engine, "payment");
    let done2 = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    complete_one(&mut engine, "payment");
    let live = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Batch includes both completed keys, the live key (ignored, not
    // terminal) and an unknown key (ignored).
    let evicted = engine.evict_instances(&[done1, done2, live, 9_999_999]);
    assert_eq!(evicted, 2);
    assert!(engine.instance(done1).is_none());
    assert!(engine.instance(done2).is_none());
    assert!(engine.instance(live).is_some());
    assert!(!engine
        .state()
        .jobs
        .values()
        .any(|j| j.instance_key == done1 || j.instance_key == done2));
    assert!(engine.state().jobs.values().any(|j| j.instance_key == live));
}

/// The activatable index must always equal the set of jobs in state
/// `Created`/`Activated`, grouped by type. Asserts that invariant.
fn assert_job_index_consistent(engine: &Engine) {
    use std::collections::{BTreeSet, HashMap, HashSet};
    let mut expected: HashMap<String, BTreeSet<(i32, Key)>> = HashMap::new();
    let mut expected_activated: HashSet<Key> = HashSet::new();
    for job in engine.state().jobs.values() {
        if job.state == state::JobState::Created {
            expected
                .entry(job.job_type.clone())
                .or_default()
                .insert(state::activation_order(job.priority, job.key));
        }
        if job.state == state::JobState::Activated {
            expected_activated.insert(job.key);
        }
    }
    assert_eq!(
        engine.state().activatable_jobs,
        expected,
        "activatable index drifted from jobs"
    );
    assert_eq!(
        engine.state().activated_jobs,
        expected_activated,
        "activated index drifted from jobs"
    );
    let mut expected_by_instance: HashMap<Key, HashSet<Key>> = HashMap::new();
    for job in engine.state().jobs.values() {
        expected_by_instance
            .entry(job.instance_key)
            .or_default()
            .insert(job.key);
    }
    assert_eq!(
        engine.state().jobs_by_instance,
        expected_by_instance,
        "jobs_by_instance index drifted from jobs"
    );
}

#[test]
fn job_index_tracks_create_activate_complete_expire_and_evict() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // Three instances → three activatable "payment" jobs, indexed in key
    // order.
    let mut instances = Vec::new();
    for _ in 0..3 {
        let k = engine
            .apply_command(Command::create_instance("order"))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap();
        instances.push(k);
    }
    assert_eq!(engine.state().activatable_jobs["payment"].len(), 3);
    assert_job_index_consistent(&engine);

    // Activating removes the job from the activatable index (it is now
    // locked); a lock that expires re-adds it via `JobLockExpired`. The index
    // iterates by key ascending and holds only `Created` jobs.
    let first = engine.activate_jobs("payment", "A", 1, 1_000, 0);
    assert_eq!(first.len(), 1);
    assert_eq!(engine.state().activatable_jobs["payment"].len(), 2);
    assert_job_index_consistent(&engine);

    // Completing the activated job leaves the index unchanged (it was already
    // de-indexed at activation).
    engine
        .apply_command(Command::complete_job(first[0].key))
        .unwrap();
    assert_eq!(engine.state().activatable_jobs["payment"].len(), 2);
    assert_job_index_consistent(&engine);

    // Expiry of another worker's lock returns the job to the index.
    let locked = engine.activate_jobs("payment", "B", 1, 1_000, 0)[0].key;
    engine.expire_jobs(5_000);
    assert!(engine.state().activatable_jobs["payment"]
        .iter()
        .any(|&(_, k)| k == locked));
    assert_job_index_consistent(&engine);

    // Evicting a completed instance drops its (already-deindexed) job and
    // leaves the index consistent.
    engine.evict_instances(&instances);
    assert_job_index_consistent(&engine);
}

#[test]
fn evict_completed_sweeps_every_finished_instance() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    for _ in 0..3 {
        engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        complete_one(&mut engine, "payment");
    }
    // A fourth instance left in-flight.
    let live = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    assert_eq!(engine.state().instances.len(), 4);
    let evicted = engine.evict_completed();
    assert_eq!(evicted, 3);
    assert_eq!(engine.state().instances.len(), 1);
    assert!(engine.instance(live).is_some());
}

// ---- cancel process instance ----

#[test]
fn cancel_terminates_instance_and_cancels_its_job() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let instance_key = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Token parked on the service-task job.
    let job_key = engine
        .state()
        .jobs
        .values()
        .find(|j| j.instance_key == instance_key)
        .unwrap()
        .key;
    assert!(!engine.is_completed(instance_key));

    let events = engine
        .apply_command(Command::cancel_instance(instance_key))
        .unwrap();

    // The job is cancelled and the instance is terminated (not completed).
    assert!(events.contains(&Event::JobCanceled {
        job_key,
        instance_key
    }));
    assert!(events.contains(&Event::ProcessInstanceTerminated { instance_key }));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })));
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        ProcessInstanceState::Terminated
    );
    assert!(engine.instance(instance_key).unwrap().active.is_empty());
    assert_eq!(
        engine.job(job_key).unwrap().state,
        state::JobState::Canceled
    );
}

#[test]
fn cancel_disarms_a_parked_timer() {
    let def = ProcessBuilder::new("delayed")
        .start_event("start")
        .timer_intermediate_catch_event("wait", 5_000)
        .end_event("end")
        .connect("start", "wait")
        .connect("wait", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let instance_key = engine
        .apply_command_at(Command::create_instance("delayed"), 1_000)
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert_eq!(engine.timers()[0].state, state::TimerState::Created);

    engine
        .apply_command(Command::cancel_instance(instance_key))
        .unwrap();

    // The armed timer is cancelled, so a later due tick fires nothing.
    assert_eq!(engine.timers()[0].state, state::TimerState::Canceled);
    assert!(engine.trigger_timers(6_000).is_empty());
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        ProcessInstanceState::Terminated
    );
}

#[test]
fn cancel_disarms_an_open_message_subscription() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_catch()))
        .unwrap();
    let instance_key = engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str("o-1".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );

    engine
        .apply_command(Command::cancel_instance(instance_key))
        .unwrap();

    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Canceled
    );
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        ProcessInstanceState::Terminated
    );
}

#[test]
fn cancel_closes_an_active_incident() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let instance_key = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    // Drive the job to a no-retries incident.
    let job = engine
        .activate_jobs("payment", "w", 1, 60_000, 0)
        .into_iter()
        .next()
        .unwrap();
    engine
        .apply_command(Command::fail_job(job.key, 0, "boom"))
        .unwrap();
    assert_eq!(engine.active_incidents().len(), 1);

    engine
        .apply_command(Command::cancel_instance(instance_key))
        .unwrap();

    // The incident is closed and the parked job is cancelled.
    assert!(engine.active_incidents().is_empty());
    assert_eq!(
        engine.job(job.key).unwrap().state,
        state::JobState::Canceled
    );
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        ProcessInstanceState::Terminated
    );
}

#[test]
fn cancel_rejects_unknown_or_finished_instances() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // Unknown key.
    assert_eq!(
        engine.apply_command(Command::cancel_instance(999)),
        Err(EngineError::InstanceNotFound { instance_key: 999 })
    );

    // A completed instance can no longer be cancelled.
    let done = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    complete_one(&mut engine, "payment");
    assert!(engine.is_completed(done));
    assert_eq!(
        engine.apply_command(Command::cancel_instance(done)),
        Err(EngineError::InstanceNotFound { instance_key: done })
    );

    // And cancelling twice fails the second time.
    let live = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    engine
        .apply_command(Command::cancel_instance(live))
        .unwrap();
    assert_eq!(
        engine.apply_command(Command::cancel_instance(live)),
        Err(EngineError::InstanceNotFound { instance_key: live })
    );
}

#[test]
fn cancel_survives_replay() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    log.extend(
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap(),
    );
    let instance_key = {
        let events = engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        let k = events.iter().find_map(|e| e.instance_key()).unwrap();
        log.extend(events);
        k
    };
    log.extend(
        engine
            .apply_command(Command::cancel_instance(instance_key))
            .unwrap(),
    );

    let recovered = Engine::replay(log);
    assert_eq!(
        recovered.instance(instance_key).unwrap().state,
        ProcessInstanceState::Terminated
    );
    assert!(recovered.instance(instance_key).unwrap().active.is_empty());
    assert!(recovered
        .state()
        .jobs
        .values()
        .all(|j| j.state == state::JobState::Canceled));
}

#[test]
fn default_engine_mints_unpartitioned_keys() {
    // partition 0 keeps the historical 1,2,3,… sequence (zero regression).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(crate::partition_of(key), 0);
    assert!(key < (1 << 51), "partition-0 keys carry no high bits");
}

#[test]
fn partitioned_engine_embeds_partition_id_in_every_key() {
    let mut engine = Engine::with_partition(3);
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    for key in events.iter().filter_map(|e| {
        // every minted key in these events belongs to partition 3
        let k = e.max_key();
        if k != 0 {
            Some(k)
        } else {
            None
        }
    }) {
        assert_eq!(
            crate::partition_of(key),
            3,
            "key {key} routes to partition 3"
        );
    }
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(crate::partition_of(instance_key), 3);
    assert!(crate::local_of(instance_key) > 0);
}

#[test]
fn keys_from_different_partitions_never_collide() {
    let mut p1 = Engine::with_partition(1);
    let mut p2 = Engine::with_partition(2);
    for e in [&mut p1, &mut p2] {
        e.apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
    }
    let k1 = p1
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let k2 = p2
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert_ne!(k1, k2);
    assert_eq!(crate::partition_of(k1), 1);
    assert_eq!(crate::partition_of(k2), 2);
}

#[test]
fn replay_partition_recovers_local_counter_ignoring_foreign_keys() {
    // Build a partition-2 log, then replay it prefixed with a foreign
    // (partition-0) deployment event. The foreign key must NOT advance
    // partition 2's local counter, so the next minted key stays in
    // partition 2 and does not collide with the replayed instance.
    let mut p2 = Engine::with_partition(2);
    let deploy_events = p2
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let create_events = p2.apply_command(Command::create_instance("order")).unwrap();
    let replayed_key = create_events.iter().find_map(|e| e.instance_key()).unwrap();

    // A deployment minted on partition 0 (low keys) that is replicated in.
    let mut p0 = Engine::new();
    let foreign_deploy = p0
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    let mut log: Vec<Event> = foreign_deploy.to_vec();
    log.extend(deploy_events.iter().cloned());
    log.extend(create_events.iter().cloned());

    let mut recovered = Engine::replay_partition(2, log);
    let next_key = recovered
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert_eq!(crate::partition_of(next_key), 2);
    assert!(
        crate::local_of(next_key) > crate::local_of(replayed_key),
        "counter advanced past the replayed partition-2 key"
    );
}

#[test]
fn install_deployment_registers_definition_without_minting() {
    // Mint a deployment on partition 0, then install it on partition 5.
    // Partition 5 can create instances of it, the definition key is shared,
    // and partition 5's own key counter is untouched by the install.
    let mut p0 = Engine::new();
    let deploy_events = p0
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let def_key = deploy_events
        .iter()
        .find_map(|e| match e {
            Event::ProcessDeployed {
                process_definition_key,
                ..
            } => Some(*process_definition_key),
            _ => None,
        })
        .unwrap();

    let mut p5 = Engine::with_partition(5);
    p5.install_deployment(&deploy_events);
    // Definition is registered under the same shared key (partition 0).
    assert_eq!(crate::partition_of(def_key), 0);

    let events = p5.apply_command(Command::create_instance("order")).unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    // The instance is minted in partition 5, not partition 0.
    assert_eq!(crate::partition_of(instance_key), 5);
    assert_eq!(
        crate::local_of(instance_key),
        1,
        "install did not consume a local key"
    );
}

/// A single phase process: pstart -> work(job) -> pend.
fn phase_process(id: &str, job: &str) -> ProcessDefinition {
    ProcessBuilder::new(id)
        .start_event("pstart")
        .service_task("work", job)
        .end_event("pend")
        .connect("pstart", "work")
        .connect("work", "pend")
        .build()
        .unwrap()
}

#[test]
fn inline_call_activities_expands_a_call_into_an_embedded_subprocess() {
    let orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "phase")
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end")
        .build()
        .unwrap();
    let mut library = std::collections::HashMap::new();
    library.insert("phase".to_string(), phase_process("phase", "work"));

    let expanded = orchestrator.inline_call_activities(&library).unwrap();

    // The call activity is now a sub-process whose inner start is the
    // prefixed copy of the callee's start event.
    let c1 = expanded.element("c1").unwrap();
    match &c1.kind {
        ElementKind::SubProcess { start_event } => assert_eq!(start_event, "c1$pstart"),
        other => panic!("expected SubProcess, got {other:?}"),
    }
    // The callee's elements were spliced in, id-prefixed and parented to c1.
    let inner = expanded.element("c1$work").unwrap();
    assert_eq!(inner.parent.as_deref(), Some("c1"));
    match &inner.kind {
        ElementKind::ServiceTask { job_type, .. } => assert_eq!(job_type, "work"),
        other => panic!("expected ServiceTask, got {other:?}"),
    }
    // No CallActivity kind survives expansion.
    assert!(!expanded
        .elements
        .values()
        .any(|e| matches!(e.kind, ElementKind::CallActivity { .. })));
}

#[test]
fn an_expanded_call_activity_runs_to_completion_through_the_engine() {
    let orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "phase")
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end")
        .build()
        .unwrap();
    let mut library = std::collections::HashMap::new();
    library.insert("phase".to_string(), phase_process("phase", "work"));
    let expanded = orchestrator.inline_call_activities(&library).unwrap();

    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(expanded))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The token enters the inlined sub-process and parks on the phase's job.
    assert!(!engine.is_completed(instance_key));
    assert_eq!(engine.pending_jobs().len(), 1);
    assert_eq!(engine.pending_jobs()[0].job_type, "work");

    // Completing it drains the sub-process and routes out to the orchestrator end.
    let events = complete_one(&mut engine, "work");
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "c1" && to == "end"
    )));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn inline_call_activities_expands_nested_calls_with_unique_prefixes() {
    // orch -> c1(callee=middle); middle -> m(callee=leaf); leaf -> work.
    let orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "middle")
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end")
        .build()
        .unwrap();
    let middle = ProcessBuilder::new("middle")
        .start_event("pstart")
        .call_activity("m", "leaf")
        .end_event("pend")
        .connect("pstart", "m")
        .connect("m", "pend")
        .build()
        .unwrap();
    let mut library = std::collections::HashMap::new();
    library.insert("middle".to_string(), middle);
    library.insert("leaf".to_string(), phase_process("leaf", "work"));

    let expanded = orchestrator.inline_call_activities(&library).unwrap();

    // The leaf's job nests two prefixes deep and is parented to the inner call.
    let leaf_work = expanded.element("c1$m$work").unwrap();
    assert_eq!(leaf_work.parent.as_deref(), Some("c1$m"));
    // Both call activities became sub-processes; none remain as calls.
    assert!(!expanded
        .elements
        .values()
        .any(|e| matches!(e.kind, ElementKind::CallActivity { .. })));
}

#[test]
fn inline_call_activities_rejects_unknown_and_cyclic_callees() {
    let orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "missing")
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end")
        .build()
        .unwrap();
    let empty = std::collections::HashMap::new();
    assert!(orchestrator
        .inline_call_activities(&empty)
        .unwrap_err()
        .contains("unknown process 'missing'"));

    // A self-recursive callee is rejected rather than expanded forever.
    let recursive = ProcessBuilder::new("loop")
        .start_event("start")
        .call_activity("again", "loop")
        .end_event("end")
        .connect("start", "again")
        .connect("again", "end")
        .build()
        .unwrap();
    let mut library = std::collections::HashMap::new();
    library.insert("loop".to_string(), recursive.clone());
    assert!(recursive
        .inline_call_activities(&library)
        .unwrap_err()
        .contains("cycle"));
}

#[cfg(feature = "serde")]
#[test]
fn dirty_var_tracking_drains_upserts_and_forgets_for_lean_snapshot() {
    use crate::model::Value;

    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine.set_track_dirty_vars(true);

    // Create two instances with variables -> both dirty.
    let mut v = std::collections::HashMap::new();
    v.insert("amount".to_string(), Value::Int(50));
    let a = engine
        .apply_command(Command::create_instance_with("order", v.clone()))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let b = engine
        .apply_command(Command::create_instance_with("order", v.clone()))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let (upserts, forgets) = engine.drain_dirty_vars();
    let keys: std::collections::HashSet<Key> = upserts.iter().map(|(k, _)| *k).collect();
    assert_eq!(keys, [a, b].into_iter().collect());
    assert!(forgets.is_empty());
    // Draining clears the set.
    let (again, _) = engine.drain_dirty_vars();
    assert!(again.is_empty());

    // A spilled instance is skipped in the upserts (spill write-through owns it).
    let mut v2 = std::collections::HashMap::new();
    v2.insert("k".to_string(), Value::Int(1));
    engine
        .apply_command(Command::SetVariables {
            scope_key: a,
            variables: v2,
        })
        .unwrap();
    let _ = engine.spill_variables(a); // a is now spilled
    let (upserts, _) = engine.drain_dirty_vars();
    assert!(
        !upserts.iter().any(|(k, _)| *k == a),
        "spilled instance must not appear in checkpoint upserts"
    );

    // Lean snapshot carries empty variable maps; a full one carries payloads.
    let lean = engine.snapshot_control_only();
    assert!(lean
        .state
        .instances
        .values()
        .all(|i| i.variables.is_empty()));
    let full = engine.snapshot();
    assert!(full
        .state
        .instances
        .get(&b)
        .is_some_and(|i| !i.variables.is_empty()));

    // Restoring variables from the store onto a lean-recovered engine.
    let mut recovered = Engine::from_snapshot(lean);
    let mut restored = std::collections::HashMap::new();
    restored.insert("amount".to_string(), Value::Int(99));
    recovered.install_variables(b, restored);
    assert_eq!(
        recovered
            .instance(b)
            .and_then(|i| i.variables.get("amount")),
        Some(&Value::Int(99))
    );
}

// --- zeebe:ioMapping (FEEL input/output variable mappings) ------------------

fn io_var(engine: &Engine, key: Key, name: &str) -> Option<Value> {
    engine.instance(key)?.variables.get(name).cloned()
}

#[test]
fn input_mapping_merges_before_job_activation() {
    // A service task with an input mapping `y = x + 1`. On activation the mapped
    // variable is merged, so a worker that activates the job sees it.
    let def = ProcessBuilder::new("io-in")
        .start_event("s")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=x + 1".to_string(),
                    target: "y".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("e")
        .connect("s", "t")
        .connect("t", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let mut vars = HashMap::new();
    vars.insert("x".to_string(), Value::Int(1));
    let inst = engine
        .apply_command(Command::create_instance_with("io-in", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert_eq!(io_var(&engine, inst, "y"), Some(Value::Int(2)));
    // The activated job snapshots the mapped variable.
    let job = &engine.activate_jobs("work", "w", 1, 60_000, 0)[0];
    assert_eq!(job.variables.get("y"), Some(&Value::Int(2)));
}

#[test]
fn output_mapping_projects_job_result() {
    // A service task with an output mapping `approved = result.ok`. The job
    // completes with `result`, and the mapping projects a renamed variable.
    let def = ProcessBuilder::new("io-out")
        .start_event("s")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "=result.ok".to_string(),
                    target: "approved".to_string(),
                }],
            },
        )
        .end_event("e")
        .connect("s", "t")
        .connect("t", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = create_instance_key(&mut engine, "io-out");
    let job_key = engine.activate_jobs("work", "w", 1, 60_000, 0)[0].key;
    let mut result = std::collections::BTreeMap::new();
    result.insert("ok".to_string(), Value::Bool(true));
    let mut job_vars = HashMap::new();
    job_vars.insert("result".to_string(), Value::Map(result));
    let events = engine
        .apply_command(Command::complete_job_with(job_key, job_vars))
        .unwrap();
    // The process runs to completion (clearing instance variables), so assert the
    // output mapping surfaced on a VariablesUpdated event for this instance.
    let mapped = events.iter().any(|e| {
        matches!(
            e,
            Event::VariablesUpdated { instance_key, variables }
                if *instance_key == inst
                    && variables.get("approved") == Some(&Value::Bool(true))
        )
    });
    assert!(
        mapped,
        "output mapping should set approved=true; events: {events:?}"
    );
}

#[test]
fn input_mapping_with_dotted_target_builds_nested_context() {
    // A dotted target `order.total` merges into a nested context, preserving the
    // other members of an existing `order`.
    let def = ProcessBuilder::new("io-nested")
        .start_event("s")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=price * qty".to_string(),
                    target: "order.total".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("e")
        .connect("s", "t")
        .connect("t", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let mut existing_order = std::collections::BTreeMap::new();
    existing_order.insert("id".to_string(), Value::Str("A1".to_string()));
    let mut vars = HashMap::new();
    vars.insert("price".to_string(), Value::Int(3));
    vars.insert("qty".to_string(), Value::Int(4));
    vars.insert("order".to_string(), Value::Map(existing_order));
    let inst = engine
        .apply_command(Command::create_instance_with("io-nested", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let mut expected = std::collections::BTreeMap::new();
    expected.insert("id".to_string(), Value::Str("A1".to_string()));
    expected.insert("total".to_string(), Value::Int(12));
    assert_eq!(io_var(&engine, inst, "order"), Some(Value::Map(expected)));
}

// --- FEEL timer expressions -------------------------------------------------

#[test]
fn feel_duration_timer_evaluates_variable() {
    // A timer intermediate catch whose timeDuration is a FEEL expression
    // (`=waitFor`) resolves against the instance variables at timer creation.
    let def = ProcessBuilder::new("feel-timer")
        .start_event("start")
        .service_task("charge", "payment")
        .timer_intermediate_catch_event("wait", 0)
        .with_timer(
            "wait",
            crate::model::TimerDef {
                kind: crate::model::TimerDefKind::Duration,
                expr: "=waitFor".to_string(),
            },
        )
        .end_event("end")
        .connect("start", "charge")
        .connect("charge", "wait")
        .connect("wait", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let mut vars = HashMap::new();
    vars.insert("waitFor".to_string(), Value::Str("PT5S".to_string()));
    let events = engine
        .apply_command_at(Command::create_instance_with("feel-timer", vars), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    let job = engine
        .activate_jobs("payment", "w", 1, 60_000, 1_000)
        .into_iter()
        .next()
        .unwrap();
    engine
        .apply_command_at(Command::complete_job(job.key), 1_000)
        .unwrap();

    // due_at = now(1000) + FEEL("PT5S")=5000 = 6000.
    let timers = engine.timers();
    assert_eq!(timers.len(), 1);
    assert_eq!(timers[0].due_at, 6_000);
    assert!(!engine.is_completed(instance_key));
    assert!(engine.trigger_timers(5_999).is_empty());
    assert!(engine
        .trigger_timers(6_000)
        .iter()
        .any(|e| matches!(e, Event::TimerTriggered { .. })));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn feel_date_timer_fires_at_absolute_instant() {
    // A timeDate timer resolves to an absolute epoch instant, independent of the
    // engine's current clock.
    let def = ProcessBuilder::new("feel-date")
        .start_event("start")
        .service_task("charge", "payment")
        .timer_intermediate_catch_event("wait", 0)
        .with_timer(
            "wait",
            crate::model::TimerDef {
                kind: crate::model::TimerDefKind::Date,
                expr: "=dueAt".to_string(),
            },
        )
        .end_event("end")
        .connect("start", "charge")
        .connect("charge", "wait")
        .connect("wait", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let mut vars = HashMap::new();
    vars.insert(
        "dueAt".to_string(),
        Value::Str("2030-01-01T00:00:00Z".to_string()),
    );
    engine
        .apply_command_at(Command::create_instance_with("feel-date", vars), 1_000)
        .unwrap();

    let job = engine
        .activate_jobs("payment", "w", 1, 60_000, 1_000)
        .into_iter()
        .next()
        .unwrap();
    engine
        .apply_command_at(Command::complete_job(job.key), 1_000)
        .unwrap();

    // 2030-01-01T00:00:00Z = 1_893_456_000_000 ms since the Unix epoch.
    let timers = engine.timers();
    assert_eq!(timers.len(), 1);
    assert_eq!(timers[0].due_at, 1_893_456_000_000);
}

#[test]
fn feel_boundary_timer_evaluates_variable() {
    // A boundary timer with a FEEL timeDuration resolves against the instance
    // variables when the guarded activity is entered.
    let def = ProcessBuilder::new("feel-boundary")
        .start_event("start")
        .service_task("charge", "payment")
        .timer_boundary_event("deadline", "charge", 0)
        .with_timer(
            "deadline",
            crate::model::TimerDef {
                kind: crate::model::TimerDefKind::Duration,
                expr: "=deadline".to_string(),
            },
        )
        .end_event("end")
        .end_event("timedout")
        .connect("start", "charge")
        .connect("charge", "end")
        .connect("deadline", "timedout")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let mut vars = HashMap::new();
    vars.insert("deadline".to_string(), Value::Str("PT5S".to_string()));
    engine
        .apply_command_at(Command::create_instance_with("feel-boundary", vars), 1_000)
        .unwrap();

    // Activating the job parks the token on `charge` with the boundary timer armed.
    engine.activate_jobs("payment", "w", 1, 60_000, 1_000);

    // due_at = now(1000) + FEEL("PT5S")=5000 = 6000.
    let timers = engine.timers();
    assert_eq!(timers.len(), 1);
    assert_eq!(timers[0].due_at, 6_000);
}
