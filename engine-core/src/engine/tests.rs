//! Unit tests for the engine, extracted verbatim from the former inline `mod tests`.
use super::*;
use crate::model::{ProcessBuilder, ProcessDefinition};
use crate::ActivateElementInstruction;

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

// --- Part C phase 3: sub-process scopes + Zeebe variable propagation --------

/// A sub-process `sub` carrying an input mapping (`scoped = seed + 1`) around an
/// inner service task `inner` (job `work`), then a normal flow out to `done`.
fn subprocess_with_input_mapping(output: Vec<crate::model::Mapping>) -> ProcessDefinition {
    ProcessBuilder::new("sub-scope")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .with_io(
            "sub",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=seed + 1".to_string(),
                    target: "scoped".to_string(),
                }],
                outputs: output,
            },
        )
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
        .unwrap()
}

#[test]
fn subprocess_input_mapping_is_local_to_the_subprocess_scope() {
    // A sub-process is its own variable scope: its input mapping creates a
    // variable LOCAL to the sub-process, visible to a job running inside it but
    // NOT at the root scope.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // The inner job sees the sub-process-local `scoped` (5 = seed + 1)...
    let job = &engine.activate_jobs("work", "w", 1, 60_000, 0)[0];
    assert_eq!(job.variables.get("scoped"), Some(&Value::Int(5)));
    // ...but it never leaked to the root scope.
    assert_eq!(io_var(&engine, inst, "scoped"), None);
}

/// Like [`subprocess_with_input_mapping`] but the inner service task ALSO carries
/// an input mapping (`derived = scoped * 10`) whose source reads the
/// sub-process-local `scoped`. This only resolves if the inner element's inputs
/// are evaluated against its enclosing (sub-process) scope, not the root.
fn subprocess_with_nested_input_mapping() -> ProcessDefinition {
    ProcessBuilder::new("nested-scope")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .with_io(
            "sub",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=seed + 1".to_string(),
                    target: "scoped".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "work")
        .with_io(
            "inner",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=scoped * 10".to_string(),
                    target: "derived".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "done")
        .build()
        .unwrap()
}

#[test]
fn inner_element_input_mapping_resolves_against_the_enclosing_subprocess_scope() {
    // Regression: an input mapping on an element nested inside a sub-process must
    // evaluate its source against the sub-process's scoped view (which carries the
    // sub-process-local `scoped`), NOT the root variables. Evaluating against root
    // would leave `scoped` unresolved and silently drop `derived`.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            subprocess_with_nested_input_mapping(),
        ))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    engine
        .apply_command(Command::create_instance_with("nested-scope", vars))
        .unwrap();

    let job = &engine.activate_jobs("work", "w", 1, 60_000, 0)[0];
    // scoped = seed + 1 = 5 (sub-process local); derived = scoped * 10 = 50,
    // which is only correct when the inner input mapping sees the enclosing scope.
    assert_eq!(job.variables.get("scoped"), Some(&Value::Int(5)));
    assert_eq!(job.variables.get("derived"), Some(&Value::Int(50)));
}

#[test]
fn subprocess_scope_local_variable_is_dropped_when_the_subprocess_completes() {
    // Once the sub-process drains and completes, its local scope (and the
    // input-mapped `scoped`) is destroyed — never surfacing at the root.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    complete_one(&mut engine, "work");
    assert!(engine.is_completed(inst));
    let instance = engine.instance(inst).unwrap();
    assert_eq!(instance.variables.get("scoped"), None);
    assert!(instance.scope_variables.is_empty());
    assert!(instance.scope_parents.is_empty());
}

#[test]
fn subprocess_output_mapping_reads_local_scope_and_propagates_to_root() {
    // A sub-process output mapping can read the sub-process-local `scoped` and
    // its projected result propagates OUT to the (root) parent scope, surviving
    // the sub-process scope teardown.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(vec![
            crate::model::Mapping {
                source: "=scoped".to_string(),
                target: "exported".to_string(),
            },
        ])))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let events = complete_one(&mut engine, "work");
    assert!(engine.is_completed(inst));
    // The process completes (clearing root variables), so assert the sub-process
    // output mapping surfaced `exported = 5` propagated to the root scope as a
    // flat `VariablesUpdated`, and that the local `scoped` never leaked there.
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::VariablesUpdated { instance_key, variables }
                if *instance_key == inst
                    && variables.get("exported") == Some(&Value::Int(5))
        )),
        "sub-process output should export scoped=5 to root; events: {events:?}"
    );
    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::VariablesUpdated { variables, .. } if variables.contains_key("scoped")
        )),
        "the sub-process-local `scoped` must not surface at root; events: {events:?}"
    );
}

#[test]
fn job_result_updates_the_nearest_defining_ancestor_scope() {
    // A job inside a sub-process completes with a variable whose name is already
    // defined in the sub-process scope: Zeebe propagation updates that scope (the
    // nearest defining ancestor), NOT the root.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    // The sub-process scope is the parent of the inner task instance.
    let inner_eik = engine.pending_jobs()[0].element_instance_key;
    let sub_scope = *engine
        .instance(inst)
        .unwrap()
        .scopes
        .get(&inner_eik)
        .unwrap();

    // The `work` job completes writing `scoped = 99` (a name owned by the
    // sub-process scope). It must update the sub-process scope, not root.
    let job_key = engine.activate_jobs("work", "w", 1, 60_000, 0)[0].key;
    let mut job_vars = HashMap::new();
    job_vars.insert("scoped".to_string(), Value::Int(99));
    let events = engine
        .apply_command(Command::complete_job_with(job_key, job_vars))
        .unwrap();
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::ScopedVariablesUpdated { scope_key, variables, .. }
                if *scope_key == sub_scope && variables.get("scoped") == Some(&Value::Int(99))
        )),
        "job result should update the sub-process scope; events: {events:?}"
    );
    // It never landed at the root scope.
    assert_eq!(io_var(&engine, inst, "scoped"), None);
}

#[test]
fn set_variables_local_writes_only_the_target_scope() {
    // `SetVariables` with local=true writes strictly into the addressed scope.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let inner_eik = engine.pending_jobs()[0].element_instance_key;
    let sub_scope = *engine
        .instance(inst)
        .unwrap()
        .scopes
        .get(&inner_eik)
        .unwrap();

    engine
        .apply_command(Command::set_variables_scoped(
            sub_scope,
            HashMap::from([("only_here".to_string(), Value::Int(7))]),
            true,
        ))
        .unwrap();
    // The local write is in the sub-process scope, not the root.
    assert_eq!(io_var(&engine, inst, "only_here"), None);
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .scope_variables
            .get(&sub_scope)
            .and_then(|m| m.get("only_here")),
        Some(&Value::Int(7))
    );
}

#[test]
fn set_variables_non_local_propagates_to_the_root_scope() {
    // `SetVariables` with local=false against a nested scope, for a name defined
    // nowhere, creates the variable at the ROOT scope (Zeebe default).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let inner_eik = engine.pending_jobs()[0].element_instance_key;
    let sub_scope = *engine
        .instance(inst)
        .unwrap()
        .scopes
        .get(&inner_eik)
        .unwrap();

    engine
        .apply_command(Command::set_variables_scoped(
            sub_scope,
            HashMap::from([("bubbled".to_string(), Value::Int(3))]),
            false,
        ))
        .unwrap();
    // Propagated to root since no ancestor scope defines `bubbled`.
    assert_eq!(io_var(&engine, inst, "bubbled"), Some(Value::Int(3)));
}

#[test]
fn set_variables_non_local_updates_the_defining_scope_not_root() {
    // `SetVariables` with local=false for a name the target scope already defines
    // updates THAT scope, not the root.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let inner_eik = engine.pending_jobs()[0].element_instance_key;
    let sub_scope = *engine
        .instance(inst)
        .unwrap()
        .scopes
        .get(&inner_eik)
        .unwrap();

    // `scoped` is owned by the sub-process scope (from its input mapping).
    engine
        .apply_command(Command::set_variables_scoped(
            sub_scope,
            HashMap::from([("scoped".to_string(), Value::Int(42))]),
            false,
        ))
        .unwrap();
    assert_eq!(io_var(&engine, inst, "scoped"), None);
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .scope_variables
            .get(&sub_scope)
            .and_then(|m| m.get("scoped")),
        Some(&Value::Int(42))
    );
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
fn abstract_task_passes_through_to_completion() {
    // An abstract `task` has no execution semantics: it must behave as a
    // pass-through, so a `start -> task -> end` process runs to completion on
    // instance creation, with no job ever created (Zeebe/C8 parity). Before
    // support was added, the flow into the task dangled and deploy failed with
    // "unknown target element".
    let def = ProcessBuilder::new("passthrough")
        .start_event("start")
        .task("do-something")
        .end_event("end")
        .connect("start", "do-something")
        .connect("do-something", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let events = engine
        .apply_command(Command::create_instance("passthrough"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(
        events.contains(&Event::ProcessInstanceCompleted { instance_key }),
        "an abstract task must pass through so the instance completes immediately"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_id, .. } if element_id == "do-something"
        )),
        "the abstract task element should complete as a pass-through"
    );
    // No job is ever emitted for an abstract task.
    assert!(
        engine
            .activate_jobs("do-something", "W", 10, 1_000, 0)
            .is_empty(),
        "an abstract task must not create a job"
    );
}

#[test]
fn inflight_by_process_tracks_create_and_terminal_transitions() {
    // ADR-0020 Tier-2 signal L_P: per-definition in-flight instance count,
    // maintained at the logical lifecycle (create +1, terminal −1), with a
    // monotonic created counter feeding λ_P.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_priority("orders", "10")))
        .unwrap();
    assert!(
        engine.backlog_by_process().is_empty(),
        "no definitions with live instances yet"
    );

    let a = create_instance_key(&mut engine, "orders");
    let _b = create_instance_key(&mut engine, "orders");
    let snap = engine.backlog_by_process();
    let (_, inflight, created) = snap.iter().find(|(p, _, _)| p == "orders").unwrap();
    assert_eq!(*inflight, 2, "two live instances");
    assert_eq!(*created, 2, "two cumulative creates");

    // Complete instance a's single job → a reaches its end event → Completed,
    // so its in-flight count drops to 1 while `created` stays monotonic.
    let job = engine
        .activate_jobs("work", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.instance_key == a)
        .unwrap();
    engine
        .apply_command(Command::complete_job(job.key))
        .unwrap();
    let snap = engine.backlog_by_process();
    let (_, inflight, created) = snap.iter().find(|(p, _, _)| p == "orders").unwrap();
    assert_eq!(*inflight, 1, "one instance completed");
    assert_eq!(*created, 2, "created is monotonic across completion");
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
    // Since issue #47 (Option B) every deploy emits a single DeploymentCreated
    // to guarantee the response envelope carries a valid LongKey — the
    // *idempotent* invariant is now specifically that no ProcessDeployed is
    // emitted (no new version, no new state).
    for events in [&second, &third] {
        assert_eq!(
            events.len(),
            1,
            "identical redeploy emits only the DeploymentCreated envelope event",
        );
        assert!(
            matches!(events[0], Event::DeploymentCreated { .. }),
            "the sole event on an identical redeploy is DeploymentCreated",
        );
    }

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
fn spilling_a_nested_scope_instance_preserves_the_merged_job_view() {
    // Regression (Part C spill): a variable spill sheds only the ROOT payload;
    // the sub-process scope-local map stays resident. After the host rehydrates
    // the root, `element_variables` must fold both back together so a job on the
    // nested scope regains its full view (root `seed` + the sub-process-local
    // `scoped`) — not the root-only payload read back from the spill store.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let eik = engine.pending_jobs()[0].element_instance_key;

    // The full merged view before any spill: root + sub-process local.
    let before = engine.element_variables(inst, eik);
    assert_eq!(before.get("seed"), Some(&Value::Int(4)));
    assert_eq!(before.get("scoped"), Some(&Value::Int(5)));

    // Spill sheds only the root payload; the scope-local map stays resident.
    let payload = engine.spill_variables(inst).expect("spillable");
    assert!(engine.instance(inst).unwrap().variables.is_empty());
    assert!(
        !engine.instance(inst).unwrap().scope_variables.is_empty(),
        "the sub-process scope-local map stays resident through a root spill"
    );
    // While spilled the root is gone, so a naive read is scope-only — exactly
    // why the host rehydrates before it reads the worker's snapshot.
    assert_eq!(engine.element_variables(inst, eik).get("seed"), None);

    // Rehydrate the root; the merged view is whole again.
    engine.rehydrate_variables(inst, payload);
    let after = engine.element_variables(inst, eik);
    assert_eq!(after.get("seed"), Some(&Value::Int(4)));
    assert_eq!(after.get("scoped"), Some(&Value::Int(5)));
}

#[test]
fn resident_variable_bytes_counts_scope_local_maps() {
    // The resident-footprint gauge must include non-root scope-local maps, which
    // stay resident through a variable spill. A nested-scope instance therefore
    // reports MORE than the same root payload alone, and spilling the root leaves
    // the scope-local bytes still attributed as resident.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert!(!engine.instance(inst).unwrap().scope_variables.is_empty());

    let with_scope = engine.resident_variable_bytes();
    assert!(with_scope > 0);

    // After shedding the root, the scope-local bytes are still counted resident.
    let _ = engine.spill_variables(inst).expect("spillable");
    let scope_only = engine.resident_variable_bytes();
    assert!(
        scope_only > 0,
        "scope-local maps stay resident and keep contributing bytes after a root spill"
    );
    assert!(
        scope_only < with_scope,
        "shedding the root payload reduces the resident footprint"
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
fn live_job_count_excludes_completed_jobs_pending_eviction() {
    // Regression: `runnable_backlog` (the admission/governor congestion signal)
    // must count only *live* jobs — Created + Activated — never the terminal
    // jobs that linger in `jobs` after their instance completes but before the
    // exporter evicts it. A completed job is deindexed the instant it settles,
    // yet its `Job` shell (and the completed instance shell) stay resident until
    // eviction. If the exporter falls behind — or is stalled by a locked
    // read-model store — those un-evicted terminal jobs accumulate; counting
    // `jobs.len()` would fold that dead weight into the backpressure reading and
    // shed legitimate new work, a self-inflicted freeze that never clears.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // Job parked at the service task: one live (Created) job.
    assert_eq!(
        engine.state().live_job_count(),
        1,
        "a created-and-waiting job is live congestion"
    );

    // Activate (lease) it: still one live (now Activated) job.
    let job = engine.activate_jobs("payment", "w", 1, 60_000, 0)[0].clone();
    assert_eq!(
        engine.state().live_job_count(),
        1,
        "a leased/in-flight job is still live congestion"
    );

    // Complete it. The instance reaches its end event and goes terminal, but its
    // shell (and the completed job) stay resident until exporter-driven eviction.
    engine
        .apply_command(Command::complete_job(job.key))
        .unwrap();
    assert_eq!(
        engine.instance(key).expect("shell still resident").state,
        crate::state::ProcessInstanceState::Completed,
        "instance is terminal but not yet evicted"
    );
    assert_eq!(
        engine.state().jobs.len(),
        1,
        "the completed job shell lingers in `jobs` until eviction"
    );
    assert_eq!(
        engine.state().live_job_count(),
        0,
        "a completed job is NOT live congestion — the admission signal must \
         ignore it, or a lagging exporter would shed new work forever"
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

#[cfg(feature = "serde")]
#[test]
fn engine_snapshot_round_trips_scope_variables_and_parents() {
    // Part C snapshot compatibility: a nested-scope instance's scope tree
    // (`scope_variables` + `scope_parents`) must survive a serialized snapshot
    // round-trip so a node rebuilt from a snapshot serves the same merged view.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let eik = engine.pending_jobs()[0].element_instance_key;
    // The sub-process scope is populated (input mapping created `scoped`).
    assert!(!engine.instance(inst).unwrap().scope_variables.is_empty());
    assert!(!engine.instance(inst).unwrap().scope_parents.is_empty());
    let merged_before = engine.element_variables(inst, eik);

    let serialized = serde_json::to_vec(&engine.snapshot()).expect("serializes");
    let decoded: EngineSnapshot = serde_json::from_slice(&serialized).expect("deserializes");
    let restored = Engine::from_snapshot(decoded);

    assert_eq!(
        restored.state(),
        engine.state(),
        "restored state (scope tree included) equals the source byte-for-byte"
    );
    // The merged scoped view is reproduced exactly on the restored engine.
    assert_eq!(restored.element_variables(inst, eik), merged_before);
    assert_eq!(
        restored.element_variables(inst, eik).get("scoped"),
        Some(&Value::Int(5))
    );
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

#[test]
fn activated_job_carries_custom_headers_and_process_identity() {
    // A service task's static zeebe:taskHeaders, the owning instance's tags and
    // business id, and the process-definition identity (bpmnProcessId, key,
    // version) must all ride on the ActivatedJob worker snapshot — the full
    // Zeebe ActivatedJob contract, not just key/type/variables.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="charge">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="payment" />
              <zeebe:taskHeaders>
                <zeebe:header key="channel" value="card" />
              </zeebe:taskHeaders>
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="charge" />
          <bpmn:sequenceFlow id="b" sourceRef="charge" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_full(
            "p",
            HashMap::new(),
            vec!["vip".to_string(), "eu".to_string()],
            Some("order-42".to_string()),
        ))
        .unwrap();

    let deployed = engine.state().processes.get("p").unwrap();
    let expected_key = deployed.key;
    let expected_version = deployed.version;

    let activated = engine.activate_jobs("payment", "w1", 10, 60_000, 0);
    assert_eq!(activated.len(), 1);
    let job = &activated[0];

    // Custom headers surfaced verbatim.
    assert_eq!(job.custom_headers.get("channel"), Some(&"card".to_string()));
    assert_eq!(job.custom_headers.len(), 1);
    // Process-definition identity.
    assert_eq!(job.bpmn_process_id, "p");
    assert_eq!(job.process_definition_key, expected_key);
    assert_eq!(job.process_definition_version, expected_version);
    // Instance metadata.
    assert_eq!(job.tags, vec!["vip".to_string(), "eu".to_string()]);
    assert_eq!(job.business_id, Some("order-42".to_string()));
    // Priority defaults to the standard 50 when undeclared.
    assert_eq!(job.priority, state::DEFAULT_JOB_PRIORITY);

    // The read-back projection (activated_job) must agree with activate_jobs.
    let projected = engine.activated_job(job.key).unwrap();
    assert_eq!(projected.custom_headers, job.custom_headers);
    assert_eq!(projected.bpmn_process_id, "p");
    assert_eq!(projected.tags, job.tags);
    assert_eq!(projected.business_id, job.business_id);
}

#[test]
fn activated_job_has_empty_custom_headers_when_task_declares_none() {
    // A service task without zeebe:taskHeaders yields an empty header map — the
    // conservative default, never a borrowed or synthesised set.
    let engine = deploy_and_create_retryable(None, HashMap::new());
    let mut engine = engine;
    let activated = engine.activate_jobs("do-work", "w1", 10, 60_000, 0);
    assert_eq!(activated.len(), 1);
    assert!(activated[0].custom_headers.is_empty());
}

#[test]
fn bpmn_parses_zeebe_linked_resources_onto_the_service_task() {
    // A service task's zeebe:linkedResources are parsed into the model, in
    // declaration order, with binding type / ids / link names preserved.
    use crate::model::{BindingType, ElementKind};
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="run-agent" />
              <zeebe:linkedResources>
                <zeebe:linkedResource resourceId="agent-prompt.md" bindingType="latest"
                                      resourceType="GenericScript" linkName="prompt" />
                <zeebe:linkedResource resourceId="policy.md" bindingType="deployment"
                                      resourceType="GenericScript" linkName="policy" />
              </zeebe:linkedResources>
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="b" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let task = def.element("agent").expect("the service task is parsed");
    let ElementKind::ServiceTask {
        linked_resources, ..
    } = &task.kind
    else {
        panic!("expected a service task, got {:?}", task.kind);
    };
    assert_eq!(linked_resources.len(), 2);
    assert_eq!(linked_resources[0].resource_id, "agent-prompt.md");
    assert_eq!(linked_resources[0].binding_type, BindingType::Latest);
    assert_eq!(linked_resources[0].resource_type, "GenericScript");
    assert_eq!(linked_resources[0].link_name, "prompt");
    assert_eq!(linked_resources[1].resource_id, "policy.md");
    assert_eq!(linked_resources[1].binding_type, BindingType::Deployment);
    assert_eq!(linked_resources[1].link_name, "policy");
}

#[test]
fn activated_job_resolves_linked_resource_latest_binding_into_a_header() {
    // A serviceTask linking a generic resource by id (bindingType=latest) must,
    // at activation, resolve that id to the LATEST deployed resource key and
    // deliver it to the worker in the `linkedResources` custom header. An
    // undeployed link id is simply omitted.
    use crate::command::GenericResource;
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="run-agent" />
              <zeebe:linkedResources>
                <zeebe:linkedResource resourceId="agent-prompt.md" bindingType="latest"
                                      resourceType="GenericScript" linkName="prompt" />
                <zeebe:linkedResource resourceId="missing.md" bindingType="latest"
                                      resourceType="GenericScript" linkName="gone" />
              </zeebe:linkedResources>
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="b" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    // Deploy two versions of the linked resource; `latest` must pick version 2.
    let resource = |content: &str| GenericResource {
        resource_id: "agent-prompt.md".to_string(),
        resource_name: "agent-prompt.md".to_string(),
        content: content.to_string(),
    };
    engine
        .apply_command(Command::DeployGenericResources(vec![resource("# v1")]))
        .unwrap();
    engine
        .apply_command(Command::DeployGenericResources(vec![resource("# v2")]))
        .unwrap();
    let latest_key = engine.state().resources["agent-prompt.md"].key;
    assert_eq!(engine.state().resources["agent-prompt.md"].version, 2);

    engine.apply_command(Command::create_instance("p")).unwrap();
    let activated = engine.activate_jobs("run-agent", "w1", 10, 60_000, 0);
    assert_eq!(activated.len(), 1);
    let header = activated[0]
        .custom_headers
        .get("linkedResources")
        .expect("the linkedResources header is present");

    // The resolved header points at the LATEST resource key; the undeployed
    // `missing.md` link is omitted (so exactly one entry, keyed by its link).
    let expected = format!(
        "[{{\"resourceKey\":\"{latest_key}\",\"resourceType\":\"GenericScript\",\"linkName\":\"prompt\"}}]"
    );
    assert_eq!(header, &expected);

    // The read-back projection agrees with activate_jobs.
    let projected = engine.activated_job(activated[0].key).unwrap();
    assert_eq!(
        projected.custom_headers.get("linkedResources"),
        Some(header)
    );
}

#[test]
fn activated_job_omits_linked_resources_header_when_nothing_resolves() {
    // When a service task declares linkedResources but NONE of the ids are
    // deployed, the engine must emit no `linkedResources` header at all — not a
    // surprising empty `[]` — and must not clobber any author-supplied header.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="run-agent" />
              <zeebe:linkedResources>
                <zeebe:linkedResource resourceId="missing.md" bindingType="latest"
                                      resourceType="GenericScript" linkName="gone" />
              </zeebe:linkedResources>
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="b" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine.apply_command(Command::create_instance("p")).unwrap();
    let activated = engine.activate_jobs("run-agent", "w1", 10, 60_000, 0);
    assert_eq!(activated.len(), 1);
    assert!(
        !activated[0].custom_headers.contains_key("linkedResources"),
        "no header when nothing resolves (not an empty array)"
    );
}

#[test]
fn bpmn_rejects_linked_resource_missing_required_attributes() {
    // Zeebe's design-time validator requires `resourceId`, `bindingType` and
    // `resourceType` on every linkedResource and rejects the deployment
    // (INVALID_ARGUMENT -> HTTP 400) when any is absent. Nano must match that
    // parity: a link missing any required attribute is a hard parse error, not
    // a silent drop that leaves the task's `linkedResources` header empty at
    // activation. Guards the whole defect *class* (each required attribute).
    let bpmn_with = |linked: &str| {
        format!(
            r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="run-agent" />
              <zeebe:linkedResources>
                {linked}
              </zeebe:linkedResources>
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="b" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#
        )
    };

    // Each of the three Zeebe-required attributes, when omitted, is rejected
    // with an `InvalidLinkedResource` error naming the offending task and
    // attribute — never silently dropped.
    for (attribute, link) in [
        (
            "resourceType",
            r#"<zeebe:linkedResource resourceId="x.md" bindingType="latest" linkName="prompt" />"#,
        ),
        (
            "resourceId",
            r#"<zeebe:linkedResource bindingType="latest" resourceType="GenericScript" linkName="prompt" />"#,
        ),
        (
            "bindingType",
            r#"<zeebe:linkedResource resourceId="x.md" resourceType="GenericScript" linkName="prompt" />"#,
        ),
    ] {
        let err = crate::bpmn::parse_bpmn(&bpmn_with(link))
            .expect_err(&format!("missing {attribute} must be rejected"));
        match err {
            crate::bpmn::ParseError::InvalidLinkedResource {
                ref task_id,
                attribute: ref attr,
            } => {
                assert_eq!(task_id, "agent");
                assert_eq!(attr, attribute, "error names the missing attribute");
            }
            other => panic!("expected InvalidLinkedResource for {attribute}, got {other:?}"),
        }
        // The actionable message matches the issue's requested wording.
        assert_eq!(
            err.to_string(),
            format!("linkedResource on 'agent' is missing required attribute '{attribute}'")
        );
    }

    // An empty attribute value is treated as absent (Zeebe's `hasNonEmptyAttribute`).
    let err = crate::bpmn::parse_bpmn(&bpmn_with(
        r#"<zeebe:linkedResource resourceId="x.md" bindingType="latest" resourceType="" linkName="prompt" />"#,
    ))
    .expect_err("empty resourceType must be rejected");
    assert!(matches!(
        err,
        crate::bpmn::ParseError::InvalidLinkedResource { .. }
    ));

    // A fully-specified linkedResource (Zeebe's required set present) parses,
    // and `linkName` is optional per Zeebe's validator: its absence resolves to
    // an empty link name rather than a rejection.
    let def = crate::bpmn::parse_bpmn(&bpmn_with(
        r#"<zeebe:linkedResource resourceId="ok.md" bindingType="latest" resourceType="GenericScript" />"#,
    ))
    .unwrap()
    .pop()
    .unwrap();
    let ElementKind::ServiceTask {
        linked_resources, ..
    } = &def.element("agent").unwrap().kind
    else {
        panic!("agent is a service task");
    };
    assert_eq!(linked_resources.len(), 1);
    assert_eq!(linked_resources[0].resource_id, "ok.md");
    assert_eq!(linked_resources[0].resource_type, "GenericScript");
    assert_eq!(linked_resources[0].link_name, "");
}

#[test]
fn inline_script_task_evaluates_feel_and_writes_result_variable() {
    // A zeebe:script script task evaluates its FEEL expression on activation,
    // stores the result under resultVariable, and passes straight through with
    // no job. A downstream service task parks the token so the result is still
    // observable in hot state (ADR 0012 clears variables only on completion).
    let def = ProcessBuilder::new("scripted")
        .start_event("start")
        .script_task("calc", "=a + b", "sum")
        .service_task("work", "do-work")
        .end_event("end")
        .connect("start", "calc")
        .connect("calc", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let events = engine
        .apply_command(Command::create_instance_with(
            "scripted",
            vars(&[("a", Value::Int(2)), ("b", Value::Int(3))]),
        ))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The script did not create a job; only the downstream service task did.
    assert_eq!(engine.state().jobs.len(), 1);
    let job = engine.state().jobs.values().next().unwrap();
    assert_eq!(job.job_type, "do-work");

    // The token advanced past the script (parked at the service task) and the
    // computed result is present under the declared resultVariable.
    assert!(!engine.is_completed(key));
    assert_eq!(
        engine.instance(key).unwrap().variables.get("sum"),
        Some(&Value::Int(5))
    );
}

#[test]
fn inline_script_task_completes_the_instance_when_terminal() {
    // A script task with no downstream work completes the instance immediately
    // (pass-through), like an intermediate throw event.
    let def = ProcessBuilder::new("scripted-end")
        .start_event("start")
        .script_task("calc", "=x * 2", "doubled")
        .end_event("end")
        .connect("start", "calc")
        .connect("calc", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let events = engine
        .apply_command(Command::create_instance_with(
            "scripted-end",
            vars(&[("x", Value::Int(21))]),
        ))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.state().jobs.len(), 0);
    assert!(engine.is_completed(key));
}

#[test]
fn inline_script_task_raises_an_incident_when_the_expression_fails_and_recovers() {
    // A script whose FEEL expression cannot evaluate (type error: string + int)
    // raises an ExpressionEvaluation incident and parks the token — matching
    // Zeebe (and nano's exclusive gateway), not a silent pass-through. Fixing
    // the variable and resolving the incident re-evaluates the script, which
    // then writes the result and lets the instance continue.
    let def = ProcessBuilder::new("scripted-fail")
        .start_event("start")
        .script_task("calc", "=n + 1", "next")
        .service_task("work", "do-work")
        .end_event("end")
        .connect("start", "calc")
        .connect("calc", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let created = engine
        .apply_command(Command::create_instance_with(
            "scripted-fail",
            vars(&[("n", Value::Str("oops".into()))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The script failed: one active ExpressionEvaluation incident, no job, and
    // the token is parked (the downstream service task never activated).
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].kind, state::IncidentKind::ExpressionEvaluation);
    assert_eq!(engine.state().jobs.len(), 0);
    assert!(!engine.is_completed(key));
    let incident_key = engine.incidents()[0].key;

    // Fix the variable to a number and resolve the incident: the script
    // re-evaluates, writes `next`, and the token advances to the service task.
    engine
        .apply_command(Command::set_variables(
            key,
            HashMap::from([("n".to_string(), Value::Int(41))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    assert!(engine.instance(key).unwrap().incidents.is_empty());
    assert_eq!(engine.state().jobs.len(), 1);
    assert_eq!(
        engine.instance(key).unwrap().variables.get("next"),
        Some(&Value::Int(42))
    );
}

#[test]
fn inline_script_task_result_is_visible_to_output_mappings() {
    // Zeebe merges resultVariable first, then applies output mappings; a
    // zeebe:output on the script task can therefore reference the result.
    let def = ProcessBuilder::new("scripted-out")
        .start_event("start")
        .script_task("calc", "=a + b", "sum")
        .with_io(
            "calc",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "=sum".to_string(),
                    target: "total".to_string(),
                }],
            },
        )
        .service_task("work", "do-work")
        .end_event("end")
        .connect("start", "calc")
        .connect("calc", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let created = engine
        .apply_command(Command::create_instance_with(
            "scripted-out",
            vars(&[("a", Value::Int(4)), ("b", Value::Int(5))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(
        engine.instance(key).unwrap().variables.get("total"),
        Some(&Value::Int(9))
    );
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
fn catch_event_output_mapping_applies_on_correlation() {
    // A message catch event carrying a `zeebe:ioMapping` output that increments a
    // loop counter (`=round + 1 -> round`) — the shape the urban-pr-review
    // convergence loop uses to advance its round on each `review-ready`. The
    // mapping must apply when the event is triggered; before the parser attached
    // catch-event ioMappings it was silently dropped and `round` stayed at 1.
    let xml = r#"
      <bpmn:definitions
          xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:intermediateCatchEvent id="await">
            <bpmn:extensionElements>
              <zeebe:ioMapping>
                <zeebe:output source="=round + 1" target="round" />
              </zeebe:ioMapping>
            </bpmn:extensionElements>
            <bpmn:messageEventDefinition messageRef="Message_1" />
          </bpmn:intermediateCatchEvent>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="await" />
          <bpmn:sequenceFlow id="f1" sourceRef="await" targetRef="e" />
        </bpmn:process>
        <bpmn:message id="Message_1" name="review-ready">
          <bpmn:extensionElements>
            <zeebe:subscription correlationKey="=prKey" />
          </bpmn:extensionElements>
        </bpmn:message>
      </bpmn:definitions>"#;
    let process = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process))
        .unwrap();
    let instance_key = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("prKey", Value::Str("A".into())), ("round", Value::Int(1))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Triggering the catch event runs its output mapping. The instance then
    // completes and drops hot state (ADR 0012), so assert the incremented value
    // on the durable `VariablesUpdated` event rather than hot state.
    let fired = engine.correlate_message("review-ready", "A", HashMap::new(), 0);
    let bumped = fired
        .iter()
        .find_map(|e| match e {
            Event::VariablesUpdated {
                instance_key: k,
                variables,
            } if *k == instance_key => variables.get("round").cloned(),
            _ => None,
        })
        .expect("the catch event's output mapping emits a VariablesUpdated for round");
    assert_eq!(bumped, Value::Int(2));
    assert!(engine.is_completed(instance_key));
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
                form_id: None,
                external_form_reference: None,
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
            process_definition_key: None,
            version: None,
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
fn should_extend_a_job_lock_past_its_original_deadline() {
    // given worker A activated the job at t=0 for 1000ms (deadline=1000)
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 1_000, 0)[0].key;
    assert_eq!(engine.job(job_key).unwrap().deadline, Some(1_000));

    // when A extends the lock by 5000ms at t=800 (before the original deadline)
    engine
        .apply_command_at(Command::update_job_timeout(job_key, 5_000), 800)
        .unwrap();

    // then the deadline moved out to now + timeout = 5800
    assert_eq!(engine.job(job_key).unwrap().deadline, Some(5_800));

    // and the periodic expiry tick fired at the ORIGINAL deadline no longer
    // reclaims the job, so another worker still cannot activate it
    engine.expire_jobs(1_500);
    assert!(engine
        .activate_jobs("payment", "B", 10, 1_000, 1_500)
        .is_empty());

    // only once the EXTENDED deadline passes is the lock released and the job
    // redelivered
    engine.expire_jobs(5_900);
    let reactivated = engine.activate_jobs("payment", "B", 10, 1_000, 5_900);
    assert_eq!(reactivated.len(), 1);
    assert_eq!(reactivated[0].key, job_key);
}

#[test]
fn should_reject_a_lock_extension_once_the_lock_has_expired() {
    // given a job whose activation lock has expired (back in the pool, Created)
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 1_000, 0)[0].key;
    engine.expire_jobs(1_500);

    // when its (now non-existent) lock is extended
    let err = engine
        .apply_command_at(Command::update_job_timeout(job_key, 5_000), 1_600)
        .unwrap_err();

    // then it is a wrong-state error, not a silent success — a job must be
    // activated to have a lock to extend
    assert_eq!(err, EngineError::JobNotActive { job_key });
}

#[test]
fn should_reject_a_lock_extension_for_an_unknown_job() {
    let mut engine = Engine::new();
    let err = engine
        .apply_command(Command::update_job_timeout(999, 5_000))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotFound { job_key: 999 });
}

/// #592 follow-up #4: `JobUpdateRequest.operationReference` must be threaded
/// onto the emitted update events (audit correlation), for BOTH the retries and
/// the timeout changeset fields — not silently dropped.
#[test]
fn should_thread_operation_reference_onto_job_update_events() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;

    // when a retries update carries an operation reference
    let events = engine
        .apply_command(Command::update_job_retries_with_ref(job_key, 5, Some(4242)))
        .unwrap();
    // then the reference lands on the JobRetriesUpdated event
    assert!(events.iter().any(|e| matches!(
        e,
        Event::JobRetriesUpdated {
            operation_reference: Some(4242),
            ..
        }
    )));

    // and likewise for a timeout (lock-extension) update
    let events = engine
        .apply_command_at(
            Command::update_job_timeout_with_ref(job_key, 5_000, Some(9001)),
            100,
        )
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::JobTimeoutUpdated {
            operation_reference: Some(9001),
            ..
        }
    )));

    // class-scoped: an update WITHOUT a reference leaves the field None (not a
    // defaulted zero), so the audit trail distinguishes "no ref" from "ref 0".
    let events = engine
        .apply_command(Command::update_job_retries(job_key, 7))
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::JobRetriesUpdated {
            operation_reference: None,
            ..
        }
    )));
}

/// #592 follow-up: `updateJob` may carry BOTH `retries` and `timeout` in one
/// changeset, applied by the server as two engine commands. The server applies
/// `timeout` first so the combined update never partially applies then fails.
/// That ordering is only sound because of an engine invariant: any job for which
/// a lock extension (`UpdateJobTimeout`) succeeds is Activated, and an Activated
/// job is never terminal, so `UpdateJobRetries` on it also succeeds. This test
/// pins that invariant — if retries were ever tightened to reject Activated
/// jobs, the partial-apply hazard would return and this guard would fail.
#[test]
fn activated_job_accepts_both_timeout_then_retries_updates() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 1_000, 0)[0].key;

    // A lock extension succeeds only while Activated...
    engine
        .apply_command_at(Command::update_job_timeout(job_key, 5_000), 100)
        .unwrap();
    // ...and because the job is still Activated (not terminal), the retries
    // update that the server applies next is guaranteed to succeed too.
    engine
        .apply_command(Command::update_job_retries(job_key, 3))
        .unwrap();
}

/// `Command` is persisted through the Raft log (`ReplicatedCommand` serializes
/// `Command`). New fields on persisted variants must (a) deserialize from logs
/// written before the field existed (`serde(default)`) and (b) stay off the wire
/// in their empty/absent form (`skip_serializing_if`) so the documented
/// "byte-unchanged" claim holds. This guards the whole defect class for the
/// three fields added in #592: `ThrowJobError.variables`,
/// `UpdateJobRetries.operation_reference`, `UpdateJobTimeout.operation_reference`.
#[cfg(feature = "serde")]
#[test]
fn new_persisted_command_fields_are_backward_compatible_and_omit_when_empty() {
    use std::collections::HashMap;

    // (a) Old log entries lack the new fields — they must still deserialize.
    let throw: Command = serde_json::from_str(
        r#"{"ThrowJobError":{"job_key":7,"error_code":"E","error_message":"m"}}"#,
    )
    .expect("legacy ThrowJobError without variables must deserialize");
    assert!(matches!(
        throw,
        Command::ThrowJobError { ref variables, .. } if variables.is_empty()
    ));

    let retries: Command =
        serde_json::from_str(r#"{"UpdateJobRetries":{"job_key":7,"retries":3}}"#)
            .expect("legacy UpdateJobRetries without operation_reference must deserialize");
    assert!(matches!(
        retries,
        Command::UpdateJobRetries {
            operation_reference: None,
            ..
        }
    ));

    let timeout: Command =
        serde_json::from_str(r#"{"UpdateJobTimeout":{"job_key":7,"timeout":5000}}"#)
            .expect("legacy UpdateJobTimeout without operation_reference must deserialize");
    assert!(matches!(
        timeout,
        Command::UpdateJobTimeout {
            operation_reference: None,
            ..
        }
    ));

    // (b) The empty/absent form must not appear on the wire (byte-unchanged).
    let throw_empty = Command::ThrowJobError {
        job_key: 7,
        error_code: "E".into(),
        error_message: "m".into(),
        variables: HashMap::new(),
    };
    let s = serde_json::to_string(&throw_empty).unwrap();
    assert!(
        !s.contains("variables"),
        "empty variables must be skipped: {s}"
    );

    let s = serde_json::to_string(&Command::update_job_retries(7, 3)).unwrap();
    assert!(
        !s.contains("operation_reference"),
        "absent operation_reference must be skipped: {s}"
    );

    let s = serde_json::to_string(&Command::update_job_timeout(7, 5000)).unwrap();
    assert!(
        !s.contains("operation_reference"),
        "absent operation_reference must be skipped: {s}"
    );

    // Present values still round-trip.
    let s = serde_json::to_string(&Command::update_job_retries_with_ref(7, 3, Some(42))).unwrap();
    assert!(s.contains("operation_reference"));
    let back: Command = serde_json::from_str(&s).unwrap();
    assert!(matches!(
        back,
        Command::UpdateJobRetries {
            operation_reference: Some(42),
            ..
        }
    ));
}

/// start -> charge(payment) --normal--> done
///     charge --(error CARD_DECLINED boundary)--> recover(recovery) -> rec_done
fn process_with_error_boundary_to_task() -> ProcessDefinition {
    ProcessBuilder::new("payment-recover")
        .start_event("s")
        .service_task("charge", "payment")
        .error_boundary_event("boundary", "charge", "CARD_DECLINED")
        .service_task("recover", "recovery")
        .end_event("done")
        .end_event("rec_done")
        .connect("s", "charge")
        .connect("charge", "done")
        .connect("boundary", "recover")
        .connect("recover", "rec_done")
        .build()
        .unwrap()
}

/// #592 follow-up #2: `JobErrorRequest.variables` must be instantiated at the
/// local scope of the error catch event, so the error-handling path downstream
/// can read them — not silently dropped.
#[test]
fn should_seed_thrown_error_variables_at_the_catch_scope() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_error_boundary_to_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("payment-recover"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "w", 1, 60_000, 0)[0].key;

    // when the worker throws the caught error WITH variables
    let vars = HashMap::from([("reason".to_string(), Value::Str("declined".to_string()))]);
    engine
        .apply_command(Command::throw_job_error_with(
            job_key,
            "CARD_DECLINED",
            "card was declined",
            vars,
        ))
        .unwrap();

    // then the downstream recovery job (on the error-handling path) sees them
    let recover = engine.activate_jobs("recovery", "w", 1, 60_000, 0);
    assert_eq!(recover.len(), 1);
    assert_eq!(
        recover[0].variables.get("reason"),
        Some(&Value::Str("declined".to_string()))
    );
}

/// Class-scoped companion to the above: variables on an UNHANDLED thrown error
/// (one that raises an incident rather than being caught) must NOT be seeded —
/// there is no catch scope to instantiate them at.
#[test]
fn should_not_seed_variables_when_a_thrown_error_is_unhandled() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_error_boundary_to_task()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("payment-recover"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("payment", "w", 1, 60_000, 0)[0].key;

    // when an error no boundary catches is thrown with variables
    let vars = HashMap::from([("reason".to_string(), Value::Str("declined".to_string()))]);
    let events = engine
        .apply_command(Command::throw_job_error_with(
            job_key, "UNKNOWN", "boom", vars,
        ))
        .unwrap();

    // then an incident is raised and no seed variable write was emitted
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::IncidentRaised { .. })));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::VariablesUpdated { variables, .. } if variables.contains_key("reason")
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::ScopedVariablesUpdated { variables, .. } if variables.contains_key("reason")
    )));
    // the instance did not acquire the dropped variable either
    assert_eq!(
        engine
            .instance(instance_key)
            .unwrap()
            .variables
            .get("reason"),
        None
    );
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

/// `retire_instances` is the follower-side counterpart to `evict_instances`: it
/// drops the named instances from hot state regardless of their local lifecycle
/// state (a follower replica never sees the leader-local completion, so the
/// instance is still `Active` here), while ignoring keys that are absent.
#[test]
fn retire_instances_drops_active_shells_and_ignores_absent() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // Two in-flight (Active, with a Created job) instances — this models a
    // follower replica that applied the CreateInstance but never the completion.
    let a = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let b = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let keep = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Both `a` and `b` are Active, not terminal — `evict_instances` must NOT
    // touch them, proving the leak they cause.
    assert_eq!(engine.evict_instances(&[a, b]), 0);
    assert!(engine.instance(a).is_some());

    // `retire_instances` drops the two named Active shells (and their jobs),
    // ignores the unknown key, and leaves the untouched instance intact.
    let retired = engine.retire_instances(&[a, b, 9_999_999]);
    assert_eq!(retired, 2);
    assert!(engine.instance(a).is_none());
    assert!(engine.instance(b).is_none());
    assert!(engine.instance(keep).is_some());
    assert!(!engine
        .state()
        .jobs
        .values()
        .any(|j| j.instance_key == a || j.instance_key == b));
    assert!(engine.state().jobs.values().any(|j| j.instance_key == keep));
}

/// The retire-before-create race: under leader-durable the leader completes and
/// broadcasts a retirement digest before the async learner has applied the
/// instance's `CreateInstance`. `retire_instances` then finds the key absent and
/// must tombstone it so the instance is reaped the moment its create materializes
/// — otherwise it leaks as a never-retired `Active` shell.
#[test]
fn retire_before_create_tombstones_and_reaps_on_arrival() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // Minting is deterministic for an identical command sequence, but a create
    // consumes several local keys (instance + job + tokens), so the instance key
    // is not simply `prev + 1`. Learn the key the create WILL mint from a twin
    // engine driven with the identical sequence.
    let future_key = {
        let mut twin = Engine::new();
        twin.apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        twin.apply_command(Command::create_instance("order"))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap()
    };

    // Retire `future_key` BEFORE it exists on this engine, exactly as a digest
    // that raced ahead of the replicated create.
    assert_eq!(
        engine.retire_instances(&[future_key]),
        0,
        "nothing present yet"
    );
    assert!(engine.instance(future_key).is_none());

    // Now the create arrives and mints exactly that key. It must be reaped
    // immediately, not left Active.
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert_eq!(created, future_key, "the create mints the tombstoned key");
    assert!(
        engine.instance(created).is_none(),
        "the tombstoned instance is reaped on arrival (no leak)"
    );
    assert!(!engine
        .state()
        .jobs
        .values()
        .any(|j| j.instance_key == created));

    // A subsequent, untombstoned create is unaffected.
    let live = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert!(engine.instance(live).is_some());
}

/// The loss-tolerant reconciliation backstop: `retire_below` drops every resident
/// replica instance below the owner's low-water mark, and `retirement_low_water`
/// ignores the owner's own resident terminal shells so the mark keeps advancing.
#[test]
fn retire_below_reconciles_follower_to_owner_low_water() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // Model a follower replica that applied five creates but no retirements.
    let mut keys = Vec::new();
    for _ in 0..5 {
        keys.push(
            engine
                .apply_command(Command::create_instance("order"))
                .unwrap()
                .iter()
                .find_map(|e| e.instance_key())
                .unwrap(),
        );
    }
    keys.sort_unstable();
    assert!(keys.iter().all(|k| engine.instance(*k).is_some()));

    // The owner's low-water mark sits just above the 3rd instance: everything below
    // it has terminated on the owner. The sweep reaps exactly those (bounded).
    let low_water = keys[3];
    let reaped = engine.retire_below(low_water, 100);
    assert_eq!(
        reaped.len(),
        3,
        "reaps every resident instance below the mark"
    );
    assert!(engine.instance(keys[0]).is_none());
    assert!(engine.instance(keys[2]).is_none());
    assert!(
        engine.instance(keys[3]).is_some(),
        "the mark itself is kept"
    );
    assert!(engine.instance(keys[4]).is_some());

    // Idempotent: re-running with the same mark reaps nothing more.
    assert_eq!(engine.retire_below(low_water, 100).len(), 0);

    // The bound caps a single sweep so a huge backlog drains across ticks.
    let capped = engine.retire_below(keys[4] + 1, 1);
    assert_eq!(capped.len(), 1);
}

/// `retirement_low_water` is the smallest still-`Active` key (so a follower can
/// drop everything below it), and the next mintable key once nothing is active
/// (so a quiescent owner tells followers to drop their whole backlog).
#[test]
fn retirement_low_water_tracks_min_active_then_next_key() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    let first = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let second = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Two active instances -> the mark is the smaller (oldest) key.
    assert_eq!(engine.retirement_low_water(), first.min(second));
    let older = first.min(second);
    let newer = first.max(second);

    // Activate both jobs in one pass (a second pass would find nothing left).
    let jobs = engine.activate_jobs("payment", "W", 10, 1_000, 0);
    let job_older = jobs.iter().find(|j| j.instance_key == older).unwrap().key;
    let job_newer = jobs.iter().find(|j| j.instance_key == newer).unwrap().key;

    // Complete the older instance; the mark advances past it to the newer.
    engine
        .apply_command(Command::complete_job(job_older))
        .unwrap();
    assert_eq!(engine.retirement_low_water(), newer);

    // Once nothing is active the mark is above every key ever minted, so a
    // follower drops its entire resident backlog.
    let mark_before = engine.retirement_low_water();
    engine
        .apply_command(Command::complete_job(job_newer))
        .unwrap();
    let mark_empty = engine.retirement_low_water();
    assert!(
        mark_empty > mark_before,
        "a quiescent owner's mark exceeds every active key it ever held"
    );
    assert!(mark_empty > newer);
}
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

// ---- modify process instance ----

/// A linear process with two service tasks (`a` → `b`) so a modify can move a
/// token from one to the other.
fn two_service_tasks() -> ProcessDefinition {
    ProcessBuilder::new("two")
        .start_event("start")
        .service_task("a", "jobA")
        .service_task("b", "jobB")
        .end_event("end")
        .connect("start", "a")
        .connect("a", "b")
        .connect("b", "end")
        .build()
        .unwrap()
}

fn active_eik_of(engine: &Engine, instance_key: Key, element_id: &str) -> Key {
    *engine
        .instance(instance_key)
        .unwrap()
        .active
        .iter()
        .find(|(_, id)| id.as_str() == element_id)
        .map(|(k, _)| k)
        .unwrap_or_else(|| panic!("no active element instance for {element_id}"))
}

#[test]
fn modify_terminates_one_element_and_activates_another() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(two_service_tasks()))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("two"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Token parked on service task `a`.
    let a_eik = active_eik_of(&engine, inst, "a");
    let job_a = engine
        .state()
        .jobs
        .values()
        .find(|j| j.instance_key == inst)
        .unwrap()
        .key;

    let events = engine
        .apply_command(Command::modify_instance(
            inst,
            vec![ActivateElementInstruction {
                element_id: "b".into(),
                variables: HashMap::new(),
            }],
            vec![a_eik],
        ))
        .unwrap();

    // `a`'s job is cancelled and its element instance completed.
    assert!(events.contains(&Event::JobCanceled {
        job_key: job_a,
        instance_key: inst,
    }));
    assert_eq!(engine.job(job_a).unwrap().state, state::JobState::Canceled);
    // The instance stays active, now with a token (and fresh job) on `b`.
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Active
    );
    assert!(engine
        .instance(inst)
        .unwrap()
        .active
        .values()
        .any(|id| id.as_str() == "b"));
    assert!(engine
        .state()
        .jobs
        .values()
        .any(|j| j.instance_key == inst && j.state == state::JobState::Created));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceTerminated { .. })));
}

#[test]
fn modify_terminating_the_last_token_terminates_the_instance() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let charge_eik = active_eik_of(&engine, inst, "charge");
    let job = engine
        .state()
        .jobs
        .values()
        .find(|j| j.instance_key == inst)
        .unwrap()
        .key;

    let events = engine
        .apply_command(Command::modify_instance(inst, vec![], vec![charge_eik]))
        .unwrap();

    // The token's job is cancelled and the instance is terminated (not
    // auto-completed) since nothing was activated to replace the last token.
    assert!(events.contains(&Event::JobCanceled {
        job_key: job,
        instance_key: inst,
    }));
    assert!(events.contains(&Event::ProcessInstanceTerminated { instance_key: inst }));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })));
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Terminated
    );
    assert!(engine.instance(inst).unwrap().active.is_empty());
}

#[test]
fn modify_activating_an_element_merges_global_variables() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(two_service_tasks()))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("two"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let a_eik = active_eik_of(&engine, inst, "a");

    let mut vars = HashMap::new();
    vars.insert("approved".to_string(), Value::Bool(true));
    let events = engine
        .apply_command(Command::modify_instance(
            inst,
            vec![ActivateElementInstruction {
                element_id: "b".into(),
                variables: vars,
            }],
            vec![a_eik],
        ))
        .unwrap();

    assert!(events.iter().any(|e| matches!(
        e,
        Event::VariablesUpdated { instance_key, variables }
            if *instance_key == inst && variables.get("approved") == Some(&Value::Bool(true))
    )));
    assert_eq!(
        engine.instance(inst).unwrap().variables.get("approved"),
        Some(&Value::Bool(true))
    );
}

#[test]
fn modify_rejects_unknown_instance_and_element_ids() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(two_service_tasks()))
        .unwrap();

    // Unknown instance.
    assert_eq!(
        engine.apply_command(Command::modify_instance(999, vec![], vec![])),
        Err(EngineError::InstanceNotFound { instance_key: 999 })
    );

    let inst = engine
        .apply_command(Command::create_instance("two"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let a_eik = active_eik_of(&engine, inst, "a");

    // Unknown activate element id — rejected, leaving the instance untouched.
    assert_eq!(
        engine.apply_command(Command::modify_instance(
            inst,
            vec![ActivateElementInstruction {
                element_id: "nope".into(),
                variables: HashMap::new(),
            }],
            vec![],
        )),
        Err(EngineError::ElementNotFound {
            instance_key: inst,
            element_id: "nope".into(),
        })
    );

    // Unknown terminate element-instance key.
    assert_eq!(
        engine.apply_command(Command::modify_instance(inst, vec![], vec![424242])),
        Err(EngineError::ElementInstanceNotFound {
            instance_key: inst,
            element_instance_key: 424242,
        })
    );

    // The rejected commands changed nothing: the token is still on `a`.
    assert_eq!(active_eik_of(&engine, inst, "a"), a_eik);
}

#[test]
fn modify_survives_replay() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    log.extend(
        engine
            .apply_command(Command::DeployProcess(two_service_tasks()))
            .unwrap(),
    );
    let inst = {
        let events = engine
            .apply_command(Command::create_instance("two"))
            .unwrap();
        let k = events.iter().find_map(|e| e.instance_key()).unwrap();
        log.extend(events);
        k
    };
    let a_eik = active_eik_of(&engine, inst, "a");
    log.extend(
        engine
            .apply_command(Command::modify_instance(
                inst,
                vec![ActivateElementInstruction {
                    element_id: "b".into(),
                    variables: HashMap::new(),
                }],
                vec![a_eik],
            ))
            .unwrap(),
    );

    let recovered = Engine::replay(log);
    assert_eq!(
        recovered.instance(inst).unwrap().state,
        ProcessInstanceState::Active
    );
    assert!(recovered
        .instance(inst)
        .unwrap()
        .active
        .values()
        .any(|id| id.as_str() == "b"));
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
            local: false,
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

/// The value a `VariablesUpdated` event in `events` merged for `name` (last
/// wins), used to inspect variables that a completed instance no longer retains
/// in hot state.
fn merged_var(events: &[Event], name: &str) -> Option<Value> {
    events.iter().rev().find_map(|e| match e {
        Event::VariablesUpdated { variables, .. } => variables.get(name).cloned(),
        _ => None,
    })
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
    assert_eq!(
        io_var(&engine, inst, "y"),
        None,
        "an input mapping creates a variable LOCAL to the activity scope, not at the instance root"
    );
    // The activated job still sees the mapped variable (its own scope shadows root).
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
    // The dotted input mapping builds its nested context LOCAL to the activity
    // scope, seeded from the enclosing `order`, so the activated job sees the
    // merged value while the root `order` keeps only its original members.
    let job = &engine.activate_jobs("work", "w", 1, 60_000, 0)[0];
    assert_eq!(job.variables.get("order"), Some(&Value::Map(expected)));
    let mut root_order = std::collections::BTreeMap::new();
    root_order.insert("id".to_string(), Value::Str("A1".to_string()));
    assert_eq!(io_var(&engine, inst, "order"), Some(Value::Map(root_order)));
}

#[test]
fn input_mapped_local_variable_is_not_visible_to_a_later_activity() {
    // `y = x + 1` is an input mapping on task `t`. Input mappings are LOCAL to the
    // activity scope (Zeebe semantics), so `y` is dropped when `t` completes and
    // is never visible at the root scope nor to the downstream task `u`'s job.
    let def = ProcessBuilder::new("io-local")
        .start_event("s")
        .service_task("t", "work-t")
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
        .service_task("u", "work-u")
        .end_event("e")
        .connect("s", "t")
        .connect("t", "u")
        .connect("u", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let mut vars = HashMap::new();
    vars.insert("x".to_string(), Value::Int(1));
    let inst = engine
        .apply_command(Command::create_instance_with("io-local", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    // `t`'s job sees the local `y`; complete it.
    let job_t = engine.activate_jobs("work-t", "w", 1, 60_000, 0)[0].key;
    engine.apply_command(Command::complete_job(job_t)).unwrap();
    // `u` activates; its job must NOT see `y` (it was local to `t`).
    let job_u = &engine.activate_jobs("work-u", "w", 1, 60_000, 0)[0];
    assert_eq!(job_u.variables.get("y"), None);
    assert_eq!(io_var(&engine, inst, "y"), None);
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

// --- Conditional events (conditionalEventDefinition, re-evaluated on variable
// change), FEEL parity feature 6. ---

/// start -> gate (conditional catch: `= approved = true`) -> end
fn process_with_conditional_catch() -> ProcessDefinition {
    ProcessBuilder::new("await-approval")
        .start_event("start")
        .conditional_intermediate_catch_event("gate", "=approved = true")
        .end_event("end")
        .connect("start", "gate")
        .connect("gate", "end")
        .build()
        .unwrap()
}

#[test]
fn should_park_on_a_conditional_catch_until_a_variable_change_satisfies_it() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_conditional_catch()))
        .unwrap();

    // The condition is false at arrival (approved unset), so the token parks on
    // an open conditional subscription.
    let created = engine
        .apply_command(Command::create_instance("await-approval"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(!engine.is_completed(key));
    assert_eq!(engine.conditional_subscriptions().len(), 1);
    assert_eq!(
        engine.conditional_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );
    // The subscription records the variable its condition depends on.
    assert_eq!(
        engine.conditional_subscriptions()[0].referenced_vars,
        vec!["approved".to_string()]
    );

    // Setting an UNRELATED variable does not trigger it.
    engine
        .apply_command(Command::set_variables(
            key,
            vars(&[("other", Value::Bool(true))]),
        ))
        .unwrap();
    assert!(!engine.is_completed(key));

    // Setting `approved = true` flips the condition and fires the catch, which
    // completes the instance.
    let fired = engine
        .apply_command(Command::set_variables(
            key,
            vars(&[("approved", Value::Bool(true))]),
        ))
        .unwrap();
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::ConditionalTriggered { .. })));
    assert!(engine.is_completed(key));
    assert_eq!(
        engine.conditional_subscriptions()[0].state,
        state::MessageSubscriptionState::Correlated
    );
}

#[test]
fn should_pass_a_conditional_catch_immediately_when_already_true_on_arrival() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_conditional_catch()))
        .unwrap();

    // The condition already holds at creation, so the token passes straight
    // through without ever opening a subscription.
    let created = engine
        .apply_command(Command::create_instance_with(
            "await-approval",
            vars(&[("approved", Value::Bool(true))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(engine.is_completed(key));
    assert!(engine.conditional_subscriptions().is_empty());
}

/// start -> work (service task) with an interrupting conditional boundary
///          `= cancel = true` -> done ; boundary -> aborted
fn process_with_conditional_boundary(interrupting: bool) -> ProcessDefinition {
    let builder = ProcessBuilder::new("guarded-cond")
        .start_event("start")
        .service_task("work", "do-work");
    let builder = if interrupting {
        builder.conditional_boundary_event("bnd", "work", "=cancel = true")
    } else {
        builder.non_interrupting_conditional_boundary_event("bnd", "work", "=ping = true")
    };
    builder
        .end_event("done")
        .end_event("aborted")
        .connect("start", "work")
        .connect("work", "done")
        .connect("bnd", "aborted")
        .build()
        .unwrap()
}

#[test]
fn should_fire_an_interrupting_conditional_boundary_on_a_variable_change() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_conditional_boundary(
            true,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("guarded-cond"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The task parks on a job with an open conditional boundary subscription.
    assert_eq!(engine.pending_jobs().len(), 1);
    let job_key = engine.pending_jobs()[0].key;
    assert_eq!(engine.conditional_subscriptions().len(), 1);

    // Flipping `cancel` interrupts the task (cancels its job) and routes along
    // the boundary's outgoing flow, completing the instance.
    let fired = engine
        .apply_command(Command::set_variables(
            key,
            vars(&[("cancel", Value::Bool(true))]),
        ))
        .unwrap();
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::JobCanceled { job_key: k, .. } if *k == job_key
    )));
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "bnd" && to == "aborted"
    )));
    assert!(engine.is_completed(key));
}

#[test]
fn should_fire_an_interrupting_conditional_boundary_immediately_when_true_on_activation() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_conditional_boundary(
            true,
        )))
        .unwrap();
    // `cancel` is already true when the activity activates: the boundary fires on
    // the first evaluation pass, so no job survives and the instance completes.
    let created = engine
        .apply_command(Command::create_instance_with(
            "guarded-cond",
            vars(&[("cancel", Value::Bool(true))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(engine.is_completed(key));
    assert_eq!(engine.pending_jobs().len(), 0);
}

#[test]
fn should_cancel_a_conditional_boundary_subscription_when_the_job_completes_first() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_conditional_boundary(
            true,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("guarded-cond"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // Completing the job before the condition holds takes the normal flow and
    // cancels the boundary subscription.
    engine.activate_jobs("do-work", "w", 1, 60_000, 0);
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(engine.is_completed(key));
    assert_eq!(
        engine.conditional_subscriptions()[0].state,
        state::MessageSubscriptionState::Canceled
    );
}

#[test]
fn should_fire_a_non_interrupting_conditional_boundary_repeatedly() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_conditional_boundary(
            false,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("guarded-cond"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // First satisfying change spawns a parallel token without cancelling the job;
    // the subscription stays open.
    let fired = engine
        .apply_command(Command::set_variables(
            key,
            vars(&[("ping", Value::Bool(true))]),
        ))
        .unwrap();
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::ConditionalTriggered { .. })));
    assert!(!fired.iter().any(|e| matches!(e, Event::JobCanceled { .. })));
    assert!(!engine.is_completed(key));
    assert_eq!(
        engine.conditional_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );

    // A second satisfying update (ping toggled off then on) fires it again.
    engine
        .apply_command(Command::set_variables(
            key,
            vars(&[("ping", Value::Bool(false))]),
        ))
        .unwrap();
    let fired = engine
        .apply_command(Command::set_variables(
            key,
            vars(&[("ping", Value::Bool(true))]),
        ))
        .unwrap();
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::ConditionalTriggered { .. })));

    // The job is still active the whole time; completing it finishes the instance.
    engine.activate_jobs("do-work", "w", 1, 60_000, 0);
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(engine.is_completed(key));
}

// ---------------------------------------------------------------------------
// Multi-instance activities (FEEL parity 7/7)
// ---------------------------------------------------------------------------

/// A single-service-task process whose task carries multi-instance
/// characteristics driven by the `items` variable. `output_element` doubles the
/// bound `item`, collected into `results`. A trailing `sink` service task parks
/// the token after the loop so the aggregated variables remain observable in hot
/// state (ADR 0012 clears variables only on instance completion).
fn multi_instance_service_process(sequential: bool) -> ProcessDefinition {
    ProcessBuilder::new("mi")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: Some("results".to_string()),
                output_element: Some("=item * 2".to_string()),
                completion_condition: None,
                sequential,
            },
        )
        .service_task("sink", "sink-work")
        .end_event("end")
        .connect("start", "each")
        .connect("each", "sink")
        .connect("sink", "end")
        .build()
        .unwrap()
}

#[test]
fn parallel_multi_instance_fans_out_a_child_per_item() {
    // A parallel multi-instance activity spawns one child (and, for a service
    // task, one job) per item in the input collection, all at once.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(
            false,
        )))
        .unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(10), Value::Int(20), Value::Int(30)]),
            )]),
        ))
        .unwrap();

    // Three parallel children -> three jobs, each carrying its own loopCounter
    // and bound item in its local variable overlay.
    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 3, "one job per collection item");
    let mut seen: Vec<(i64, i64)> = jobs
        .iter()
        .map(|j| {
            let counter = j
                .variables
                .get("loopCounter")
                .and_then(Value::as_f64)
                .unwrap() as i64;
            let item = j.variables.get("item").and_then(Value::as_f64).unwrap() as i64;
            (counter, item)
        })
        .collect();
    seen.sort();
    assert_eq!(seen, vec![(1, 10), (2, 20), (3, 30)]);
}

#[test]
fn parallel_multi_instance_collects_output_collection_in_order() {
    // Each child's output_element is stored at its index; the body completes
    // only after every child finishes and writes the aggregate collection —
    // ordered by index regardless of completion order.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(
            false,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    // Complete out of collection order to prove index-based aggregation.
    let mut ordered = jobs.clone();
    ordered.sort_by_key(|j| {
        std::cmp::Reverse(
            j.variables
                .get("loopCounter")
                .and_then(Value::as_f64)
                .unwrap() as i64,
        )
    });
    for job in &ordered {
        assert!(!engine.is_completed(key));
        engine
            .apply_command(Command::complete_job(job.key))
            .unwrap();
    }
    // Body drained -> the token advanced to the sink task (instance not yet
    // completed, so the aggregate is still observable); results doubled each
    // item in order.
    assert!(!engine.is_completed(key));
    assert!(
        engine
            .state()
            .jobs
            .values()
            .any(|j| j.job_type == "sink-work" && matches!(j.state, state::JobState::Created)),
        "token parked at the sink task"
    );
    assert_eq!(
        engine.instance(key).unwrap().variables.get("results"),
        Some(&Value::List(vec![
            Value::Int(2),
            Value::Int(4),
            Value::Int(6)
        ]))
    );
}

#[test]
fn multi_instance_input_mapping_cannot_clobber_loop_counter() {
    // A malicious/careless `zeebe:input` mapping targeting the reserved
    // `loopCounter` binding must not corrupt the engine-owned loop counter:
    // `complete_mi_child` derives each child's output-collection index from it,
    // so a clobbered counter would misindex (or silently drop, since the slot is
    // out of range) every child's output. The engine-owned counter must win, so
    // the aggregate still lands in index order.
    let mut def = multi_instance_service_process(false);
    let each = def.elements.get_mut("each").expect("each element");
    each.io = crate::model::IoMapping {
        inputs: vec![crate::model::Mapping {
            source: "=loopCounter + 100".to_string(),
            target: "loopCounter".to_string(),
        }],
        outputs: Vec::new(),
    };

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 3, "one job per collection item");
    // Each child still sees the engine-owned loop counter (1..=3), not the
    // mapped-over value — the reserved binding is protected from the mapping.
    let mut counters: Vec<i64> = jobs
        .iter()
        .map(|j| {
            j.variables
                .get("loopCounter")
                .and_then(Value::as_f64)
                .unwrap() as i64
        })
        .collect();
    counters.sort();
    assert_eq!(counters, vec![1, 2, 3]);

    for job in &jobs {
        engine
            .apply_command(Command::complete_job(job.key))
            .unwrap();
    }
    // Every child's output landed at its correct index despite the input mapping
    // aiming at `loopCounter`.
    assert_eq!(
        engine.instance(key).unwrap().variables.get("results"),
        Some(&Value::List(vec![
            Value::Int(2),
            Value::Int(4),
            Value::Int(6)
        ]))
    );
}

#[test]
fn multi_instance_child_index_survives_worker_clobbering_loop_counter() {
    // The child's output-collection index is engine-owned runtime state, not
    // derived from the mutable `loopCounter` variable. So even a write path the
    // `zeebe:input` guard cannot see — a worker issuing
    // `SetVariables { local: true }` against its child scope to overwrite
    // `loopCounter` — must not corrupt `outputCollection` indexing: an
    // out-of-range counter would otherwise silently drop the output, and a
    // collided one would overwrite another child's slot. The aggregate must still
    // land in index order.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(
            false,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 3, "one job per collection item");

    // Every worker slams the SAME out-of-range `loopCounter` into its own child
    // scope before completing. If completion trusted this variable, all three
    // outputs would target index 99 (out of range) and be dropped.
    for job in &jobs {
        engine
            .apply_command(Command::set_variables_scoped(
                job.element_instance_key,
                vars(&[("loopCounter", Value::Int(99))]),
                true,
            ))
            .unwrap();
        engine
            .apply_command(Command::complete_job(job.key))
            .unwrap();
    }

    // Each child's output still landed at its true engine-owned index.
    assert_eq!(
        engine.instance(key).unwrap().variables.get("results"),
        Some(&Value::List(vec![
            Value::Int(2),
            Value::Int(4),
            Value::Int(6)
        ]))
    );
}

#[test]
fn sequential_multi_instance_runs_children_one_at_a_time() {
    // A sequential multi-instance activity spawns the next child only after the
    // previous one completes.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(true)))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[("items", Value::List(vec![Value::Int(5), Value::Int(6)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Only the first child is active.
    let first = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(first.len(), 1);
    assert_eq!(
        first[0]
            .variables
            .get("loopCounter")
            .and_then(Value::as_f64)
            .unwrap() as i64,
        1
    );
    engine
        .apply_command(Command::complete_job(first[0].key))
        .unwrap();

    // Completing it spawns the second, still not the whole loop at once.
    let second = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(second.len(), 1);
    assert_eq!(
        second[0]
            .variables
            .get("loopCounter")
            .and_then(Value::as_f64)
            .unwrap() as i64,
        2
    );
    engine
        .apply_command(Command::complete_job(second[0].key))
        .unwrap();

    // Loop drained -> parked at the sink; the ordered aggregate is observable.
    assert!(!engine.is_completed(key));
    assert_eq!(
        engine.instance(key).unwrap().variables.get("results"),
        Some(&Value::List(vec![Value::Int(10), Value::Int(12)]))
    );
}

#[test]
fn multi_instance_completion_condition_completes_the_body_early() {
    // A satisfied completion condition cancels the still-running children and
    // completes the body without waiting for them.
    let def = ProcessBuilder::new("mi-cc")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                // Fire as soon as the first child finishes.
                completion_condition: Some("=true".to_string()),
                sequential: false,
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-cc",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 3);
    // Completing ONE child satisfies the condition and ends the body; the other
    // two jobs are canceled with the body.
    engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();
    assert!(engine.is_completed(key));
    assert!(
        engine.state().jobs.values().all(|j| matches!(
            j.state,
            state::JobState::Canceled | state::JobState::Completed
        )),
        "remaining children canceled on early completion"
    );
}

#[test]
fn empty_input_collection_completes_the_multi_instance_body_immediately() {
    // An empty collection (or a non-list / erroring expression) yields zero
    // children; the body completes at once and the token flows on.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(
            false,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[("items", Value::List(vec![]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // No `handle` jobs created; the body completed at once and the token flowed
    // through to the sink task (parked there, aggregate still observable).
    assert_eq!(engine.activate_jobs("handle", "w", 10, 60_000, 0).len(), 0);
    assert!(!engine.is_completed(key));
    assert_eq!(
        engine.state().jobs.len(),
        1,
        "token parked at the sink task"
    );
    assert_eq!(
        engine.instance(key).unwrap().variables.get("results"),
        Some(&Value::List(vec![]))
    );
}

#[test]
fn multi_instance_children_hold_their_bindings_in_real_child_scopes() {
    // Part C: each parallel child is now a real variable scope (parented to the
    // MI body), holding its `loopCounter`/`inputElement` locally — never leaked
    // to the root instance variables.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(
            false,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(10), Value::Int(20), Value::Int(30)]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Jobs still see their per-child bindings...
    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 3);
    for job in &jobs {
        assert!(job.variables.get("item").is_some());
        assert!(job.variables.get("loopCounter").is_some());
    }

    // ...but the root scope never received them: the bindings live in dedicated
    // child scopes parented to the multi-instance body.
    let instance = engine.instance(key).unwrap();
    assert!(instance.variables.get("item").is_none());
    assert!(instance.variables.get("loopCounter").is_none());
    let body_key = *instance
        .multi_instances
        .keys()
        .next()
        .expect("an active multi-instance body");
    let children: Vec<Key> = instance
        .scope_parents
        .iter()
        .filter(|&(_, &parent)| parent == body_key)
        .map(|(&child, _)| child)
        .collect();
    assert_eq!(children.len(), 3, "one real scope per child");
    for child in children {
        let locals = instance
            .scope_variables
            .get(&child)
            .expect("child scope carries local bindings");
        assert!(locals.contains_key("item"));
        assert!(locals.contains_key("loopCounter"));
    }
}

/// A process whose embedded sub-process `wave` carries multi-instance
/// characteristics driven by `=tasks`, binding each item to `task`. Unlike
/// [`multi_instance_service_process`] the MI body is a whole SUB-PROCESS scope
/// (start -> inner service task `impl` (job `do-work`) -> end), so this exercises
/// MI fan-out over a multi-step body. `output_element` echoes the bound `task`
/// into `done_tasks`. A trailing `sink` service task parks the token after the
/// join so the aggregate stays observable in hot state. This is the primitive
/// under the agent-fleet "sequential MI over waves wrapping parallel MI over a
/// wave's tasks" pattern (Magikcraft/nano-bpm#547).
fn multi_instance_subprocess(sequential: bool) -> ProcessDefinition {
    ProcessBuilder::new("mi-sub")
        .start_event("start")
        .sub_process("wave", "wave_start")
        .with_multi_instance(
            "wave",
            crate::model::MultiInstance {
                input_collection: "=tasks".to_string(),
                input_element: Some("task".to_string()),
                output_collection: Some("done_tasks".to_string()),
                output_element: Some("=task".to_string()),
                completion_condition: None,
                sequential,
            },
        )
        .start_event("wave_start")
        .contained_in("wave_start", "wave")
        .service_task("impl", "do-work")
        .contained_in("impl", "wave")
        .end_event("wave_end")
        .contained_in("wave_end", "wave")
        .service_task("sink", "sink-work")
        .end_event("end")
        .connect("start", "wave")
        .connect("wave_start", "impl")
        .connect("impl", "wave_end")
        .connect("wave", "sink")
        .connect("sink", "end")
        .build()
        .unwrap()
}

/// The bound `task` string of an activated `do-work` job (fails loudly if the
/// MI-bound item never reached the inner sub-process scope).
fn bound_task(job: &ActivatedJob) -> String {
    match job.variables.get("task") {
        Some(Value::Str(s)) => s.clone(),
        other => panic!("expected a bound `task` string in the child scope, got {other:?}"),
    }
}

#[test]
fn parallel_multi_instance_on_a_subprocess_fans_out_one_scope_per_item() {
    // Agent-fleet / #547 wave nesting: a multi-instance attached to an EMBEDDED
    // SUB-PROCESS fans out one full sub-process scope per collection item, runs
    // each inner task with its bound item, then joins. This is the primitive
    // under "sequential MI over waves wrapping parallel MI over a wave's tasks".
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_subprocess(false)))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-sub",
            vars(&[(
                "tasks",
                Value::List(vec![
                    Value::Str("s549".into()),
                    Value::Str("s550".into()),
                    Value::Str("s551".into()),
                ]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // One inner job per item, all active at once (parallel), each carrying its
    // MI-bound `task` visible inside its own sub-process scope.
    let jobs = engine.activate_jobs("do-work", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 3, "one sub-process child per task");
    let mut seen: Vec<String> = jobs.iter().map(bound_task).collect();
    seen.sort();
    assert_eq!(seen, vec!["s549", "s550", "s551"]);

    // The join waits for every child sub-process; the token has not left the MI
    // body until the last one completes. Complete the children in a deliberately
    // shuffled order (last item first, first item last) so that if aggregation
    // accidentally depended on completion order the assert below would catch it.
    let by_task: std::collections::HashMap<String, u64> =
        jobs.iter().map(|j| (bound_task(j), j.key)).collect();
    for task in ["s551", "s549", "s550"] {
        assert!(!engine.is_completed(key));
        assert!(
            engine
                .state()
                .jobs
                .values()
                .all(|j| j.job_type != "sink-work"),
            "sink not reached until the MI sub-process joins"
        );
        engine
            .apply_command(Command::complete_job(by_task[task]))
            .unwrap();
    }

    // Joined -> token parked at the sink; aggregate collected in index order
    // regardless of completion order.
    assert!(!engine.is_completed(key));
    assert!(
        engine
            .state()
            .jobs
            .values()
            .any(|j| j.job_type == "sink-work" && matches!(j.state, state::JobState::Created)),
        "token joined the MI sub-process and parked at the sink"
    );
    assert_eq!(
        engine.instance(key).unwrap().variables.get("done_tasks"),
        Some(&Value::List(vec![
            Value::Str("s549".into()),
            Value::Str("s550".into()),
            Value::Str("s551".into()),
        ]))
    );
}

#[test]
fn sequential_multi_instance_on_a_subprocess_runs_one_scope_at_a_time() {
    // The sequential form (a "wave" that runs its sub-process bodies one after
    // another) spawns the next child sub-process only after the previous one
    // fully completes.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_subprocess(true)))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-sub",
            vars(&[(
                "tasks",
                Value::List(vec![Value::Str("a".into()), Value::Str("b".into())]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let first = engine.activate_jobs("do-work", "w", 10, 60_000, 0);
    assert_eq!(first.len(), 1, "only the first wave sub-process is active");
    assert_eq!(bound_task(&first[0]), "a");
    engine
        .apply_command(Command::complete_job(first[0].key))
        .unwrap();

    let second = engine.activate_jobs("do-work", "w", 10, 60_000, 0);
    assert_eq!(
        second.len(),
        1,
        "next sub-process spawned only after the first"
    );
    assert_eq!(bound_task(&second[0]), "b");
    engine
        .apply_command(Command::complete_job(second[0].key))
        .unwrap();

    assert!(!engine.is_completed(key));
    assert_eq!(
        engine.instance(key).unwrap().variables.get("done_tasks"),
        Some(&Value::List(vec![
            Value::Str("a".into()),
            Value::Str("b".into())
        ]))
    );
}

/// Like `multi_instance_subprocess` but the `wave` sub-process carries its own
/// `zeebe:output` mapping (`tag = n * 10`) and the loop's `outputElement` reads
/// that mapped local (`=tag`). Proves an MI-child sub-process applies its own
/// output mappings into its local scope before the `outputElement` is collected
/// (Zeebe parity: `getVariableScopeKey` writes them to the instance's own scope,
/// visible to `outputElement`, NOT propagated to the parent).
fn multi_instance_subprocess_with_output_mapping() -> ProcessDefinition {
    ProcessBuilder::new("mi-sub-out")
        .start_event("start")
        .sub_process("wave", "wave_start")
        .with_multi_instance(
            "wave",
            crate::model::MultiInstance {
                input_collection: "=nums".to_string(),
                input_element: Some("n".to_string()),
                output_collection: Some("tags".to_string()),
                output_element: Some("=tag".to_string()),
                completion_condition: None,
                sequential: false,
            },
        )
        .with_io(
            "wave",
            crate::model::IoMapping {
                inputs: vec![],
                outputs: vec![crate::model::Mapping {
                    source: "=n * 10".to_string(),
                    target: "tag".to_string(),
                }],
            },
        )
        .start_event("wave_start")
        .contained_in("wave_start", "wave")
        .service_task("impl", "do-work")
        .contained_in("impl", "wave")
        .end_event("wave_end")
        .contained_in("wave_end", "wave")
        .service_task("sink", "sink-work")
        .end_event("end")
        .connect("start", "wave")
        .connect("wave_start", "impl")
        .connect("impl", "wave_end")
        .connect("wave", "sink")
        .connect("sink", "end")
        .build()
        .unwrap()
}

#[test]
fn multi_instance_subprocess_applies_its_own_output_mapping_before_collecting() {
    // Zeebe parity: an MI-child sub-process evaluates its own `zeebe:output`
    // mappings on each instance's completion, into that instance's OWN scope, so
    // the loop's `outputElement` can read the mapped value. Here `outputElement`
    // is `=tag`, and `tag` exists ONLY because the sub-process output mapping
    // `tag = n * 10` ran — if it were skipped, `outputElement` would collect null.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            multi_instance_subprocess_with_output_mapping(),
        ))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-sub-out",
            vars(&[(
                "nums",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let jobs = engine.activate_jobs("do-work", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 3, "one sub-process child per item");
    for job in &jobs {
        engine
            .apply_command(Command::complete_job(job.key))
            .unwrap();
    }

    assert!(!engine.is_completed(key), "token parked at the sink");
    // The aggregated collection reflects the sub-process output mapping (n*10),
    // in index order.
    assert_eq!(
        engine.instance(key).unwrap().variables.get("tags"),
        Some(&Value::List(vec![
            Value::Int(10),
            Value::Int(20),
            Value::Int(30),
        ])),
        "outputElement read the sub-process-mapped local `tag`"
    );
    // The mapped local stays scoped to each child — it does NOT leak to the
    // parent instance scope (only the outputCollection propagates).
    assert_eq!(
        engine.instance(key).unwrap().variables.get("tag"),
        None,
        "the MI-child output mapping must not propagate `tag` to the parent"
    );
}

/// An MI service task `each` whose own `zeebe:input` mapping references the
/// per-child `inputElement` (`item`) and `loopCounter` — so the mapping can only
/// be evaluated correctly once those child bindings exist.
fn multi_instance_service_with_input_mapping() -> ProcessDefinition {
    ProcessBuilder::new("mi-in")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                sequential: false,
            },
        )
        .with_io(
            "each",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=item * 100 + loopCounter".to_string(),
                    target: "handle_arg".to_string(),
                }],
                outputs: vec![],
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap()
}

#[test]
fn multi_instance_task_evaluates_input_mappings_per_child_with_bindings() {
    // Zeebe parity: an MI activity's `zeebe:input` mappings are evaluated on each
    // child's activation, with `inputElement`/`loopCounter` already bound, into
    // the child's own local scope. Here `handle_arg = item * 100 + loopCounter`,
    // so each job carries a value that only exists because the mapping ran with
    // that child's bindings.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            multi_instance_service_with_input_mapping(),
        ))
        .unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "mi-in",
            vars(&[("items", Value::List(vec![Value::Int(5), Value::Int(6)]))]),
        ))
        .unwrap();

    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 2, "one job per item");
    let mut args: Vec<i64> = jobs
        .iter()
        .map(|j| match j.variables.get("handle_arg") {
            Some(Value::Int(n)) => *n,
            other => panic!("expected a per-child input-mapped `handle_arg`, got {other:?}"),
        })
        .collect();
    args.sort();
    // item=5,loopCounter=1 -> 501 ; item=6,loopCounter=2 -> 602
    assert_eq!(args, vec![501, 602]);
}

#[test]
fn set_variables_local_on_a_multi_instance_child_stays_in_its_scope() {
    // A `local` SetVariables targeting a running MI child scope writes only that
    // child's scope, invisible to the root and to sibling children.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(
            false,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    engine.activate_jobs("handle", "w", 10, 60_000, 0);

    let instance = engine.instance(key).unwrap();
    let body_key = *instance.multi_instances.keys().next().unwrap();
    let mut children: Vec<Key> = instance
        .scope_parents
        .iter()
        .filter(|&(_, &parent)| parent == body_key)
        .map(|(&child, _)| child)
        .collect();
    children.sort();
    let target = children[0];
    let sibling = children[1];

    engine
        .apply_command(Command::set_variables_scoped(
            target,
            vars(&[("note", Value::Str("child-only".into()))]),
            true,
        ))
        .unwrap();

    let instance = engine.instance(key).unwrap();
    assert_eq!(
        instance.scope_variables.get(&target).unwrap().get("note"),
        Some(&Value::Str("child-only".into())),
        "written into the target child scope"
    );
    assert!(
        instance.variables.get("note").is_none(),
        "not propagated to the root"
    );
    assert!(
        instance
            .scope_variables
            .get(&sibling)
            .map(|m| !m.contains_key("note"))
            .unwrap_or(true),
        "not visible to the sibling child"
    );
}

// ---------------------------------------------------------------------------
// Hierarchical variable scoping — machinery (Part C phase 1)
// ---------------------------------------------------------------------------

#[test]
fn job_type_expression_resolves_against_the_enclosing_sub_process_scope() {
    // A service task inside a sub-process resolves its `=FEEL` job type against
    // the sub-process's local variables (an input mapping), not just the root —
    // if scoping leaked, `region` would be unresolved and the type would fall
    // back to the literal expression text.
    let model = ProcessBuilder::new("scoped-jt")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .with_io(
            "sub",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=\"north\"".to_string(),
                    target: "region".to_string(),
                }],
                outputs: vec![],
            },
        )
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "=\"worker-\" + region")
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
    engine.apply_command(Command::DeployProcess(model)).unwrap();
    create_instance_key(&mut engine, "scoped-jt");

    // The inner job was created with the scope-resolved type.
    let jobs = engine.activate_jobs("worker-north", "w", 10, 60_000, 0);
    assert_eq!(
        jobs.len(),
        1,
        "job type resolved against the sub-process scope"
    );
    // And is NOT sitting under the literal fallback type.
    assert!(engine
        .activate_jobs("=\"worker-\" + region", "w", 10, 60_000, 0)
        .is_empty());
}

#[test]
fn multi_instance_child_job_type_resolves_against_the_child_scope() {
    // A multi-instance child resolves its `=FEEL` job type against its own scope
    // (the bound `inputElement`), so each child lands on a per-item worker type.
    let model = ProcessBuilder::new("mi-jt")
        .start_event("start")
        .service_task("each", "=\"handle-\" + item")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                sequential: false,
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(model)).unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "mi-jt",
            vars(&[(
                "items",
                Value::List(vec![Value::Str("a".into()), Value::Str("b".into())]),
            )]),
        ))
        .unwrap();

    // Each child job carries its own item-derived type.
    assert_eq!(
        engine.activate_jobs("handle-a", "w", 10, 60_000, 0).len(),
        1
    );
    assert_eq!(
        engine.activate_jobs("handle-b", "w", 10, 60_000, 0).len(),
        1
    );
    // No child fell back to the literal expression text.
    assert!(engine
        .activate_jobs("=\"handle-\" + item", "w", 10, 60_000, 0)
        .is_empty());
}

/// Applies a raw event straight to the engine's state (test-only shortcut for
/// exercising the scope appliers/helpers without a driving command).
fn apply_raw(engine: &mut Engine, event: Event) {
    crate::state::apply(&mut engine.state, &event);
}

#[test]
fn scoped_variable_resolution_walks_child_scope_to_root() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let key = create_instance_key(&mut engine, "order");
    apply_raw(
        &mut engine,
        Event::VariablesUpdated {
            instance_key: key,
            variables: vars(&[("a", Value::Int(1)), ("b", Value::Int(2))]),
        },
    );
    let child: Key = 900_001;
    apply_raw(
        &mut engine,
        Event::VariableScopeCreated {
            instance_key: key,
            scope_key: child,
            parent_scope_key: key,
        },
    );
    apply_raw(
        &mut engine,
        Event::ScopedVariablesUpdated {
            instance_key: key,
            scope_key: child,
            variables: vars(&[("b", Value::Int(20)), ("c", Value::Int(3))]),
        },
    );

    let inst = engine.state.instances.get(&key).unwrap();
    // The root scope sees only its own variables.
    let root_view = engine.visible_variables(inst, key);
    assert_eq!(root_view.get("a"), Some(&Value::Int(1)));
    assert_eq!(root_view.get("b"), Some(&Value::Int(2)));
    assert_eq!(root_view.get("c"), None);
    // The child scope shadows `b`, adds local `c`, and inherits `a` from root.
    let child_view = engine.visible_variables(inst, child);
    assert_eq!(child_view.get("a"), Some(&Value::Int(1)));
    assert_eq!(child_view.get("b"), Some(&Value::Int(20)));
    assert_eq!(child_view.get("c"), Some(&Value::Int(3)));
}

#[test]
fn variable_propagation_updates_nearest_defining_scope_else_root() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let key = create_instance_key(&mut engine, "order");
    apply_raw(
        &mut engine,
        Event::VariablesUpdated {
            instance_key: key,
            variables: vars(&[("a", Value::Int(1))]),
        },
    );
    let child: Key = 900_002;
    apply_raw(
        &mut engine,
        Event::VariableScopeCreated {
            instance_key: key,
            scope_key: child,
            parent_scope_key: key,
        },
    );
    apply_raw(
        &mut engine,
        Event::ScopedVariablesUpdated {
            instance_key: key,
            scope_key: child,
            variables: vars(&[("b", Value::Int(2))]),
        },
    );

    // Non-local merge into the child: `a` is defined only at root -> root; `b`
    // is defined locally -> child; `c` is new anywhere -> created at root.
    let writes = engine.propagate_variables(
        key,
        child,
        vars(&[
            ("a", Value::Int(9)),
            ("b", Value::Int(9)),
            ("c", Value::Int(9)),
        ]),
        false,
    );
    let mut by_scope: std::collections::HashMap<Key, Vec<String>> =
        std::collections::HashMap::new();
    for (scope, map) in writes {
        by_scope
            .entry(scope)
            .or_default()
            .extend(map.keys().cloned());
    }
    for names in by_scope.values_mut() {
        names.sort();
    }
    assert_eq!(
        by_scope.get(&key),
        Some(&vec!["a".to_string(), "c".to_string()])
    );
    assert_eq!(by_scope.get(&child), Some(&vec!["b".to_string()]));
}

#[test]
fn local_variable_write_stays_in_the_target_scope() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let key = create_instance_key(&mut engine, "order");
    apply_raw(
        &mut engine,
        Event::VariablesUpdated {
            instance_key: key,
            variables: vars(&[("a", Value::Int(1))]),
        },
    );
    let child: Key = 900_003;
    apply_raw(
        &mut engine,
        Event::VariableScopeCreated {
            instance_key: key,
            scope_key: child,
            parent_scope_key: key,
        },
    );

    // A local write pins every value to the child scope even though `a` is
    // defined at root (Zeebe `local=true` / input-mapping semantics).
    let writes = engine.propagate_variables(key, child, vars(&[("a", Value::Int(9))]), true);
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].0, child);
    assert!(writes[0].1.contains_key("a"));
}

#[test]
fn destroying_a_scope_drops_its_local_variables() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let key = create_instance_key(&mut engine, "order");
    let child: Key = 900_004;
    apply_raw(
        &mut engine,
        Event::VariableScopeCreated {
            instance_key: key,
            scope_key: child,
            parent_scope_key: key,
        },
    );
    apply_raw(
        &mut engine,
        Event::ScopedVariablesUpdated {
            instance_key: key,
            scope_key: child,
            variables: vars(&[("c", Value::Int(3))]),
        },
    );
    assert!(engine
        .state
        .instances
        .get(&key)
        .unwrap()
        .scope_variables
        .contains_key(&child));
    apply_raw(
        &mut engine,
        Event::VariableScopeDestroyed {
            instance_key: key,
            scope_key: child,
        },
    );
    let inst = engine.state.instances.get(&key).unwrap();
    assert!(!inst.scope_variables.contains_key(&child));
    assert!(!inst.scope_parents.contains_key(&child));
}

// ---------------------------------------------------------------------------
// Zeebe variable-scope parity matrix (Part C phase: tests)
//
// Rounds out the ported Zeebe scope semantics beyond the single-level cases
// above: deep (3-level) nesting with mid-level shadowing, parallel sibling
// isolation, and local-write shadowing over an inherited root variable.
// ---------------------------------------------------------------------------

#[test]
fn deep_nesting_resolves_each_level_through_its_nearest_shadow_to_root() {
    // root:   a=1, b=2, c=3
    // middle: shadows b=20, adds d=40   (parent = root)
    // leaf:   shadows c=300, adds e=500 (parent = middle)
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let key = create_instance_key(&mut engine, "order");
    apply_raw(
        &mut engine,
        Event::VariablesUpdated {
            instance_key: key,
            variables: vars(&[
                ("a", Value::Int(1)),
                ("b", Value::Int(2)),
                ("c", Value::Int(3)),
            ]),
        },
    );
    let middle: Key = 910_001;
    let leaf: Key = 910_002;
    apply_raw(
        &mut engine,
        Event::VariableScopeCreated {
            instance_key: key,
            scope_key: middle,
            parent_scope_key: key,
        },
    );
    apply_raw(
        &mut engine,
        Event::VariableScopeCreated {
            instance_key: key,
            scope_key: leaf,
            parent_scope_key: middle,
        },
    );
    apply_raw(
        &mut engine,
        Event::ScopedVariablesUpdated {
            instance_key: key,
            scope_key: middle,
            variables: vars(&[("b", Value::Int(20)), ("d", Value::Int(40))]),
        },
    );
    apply_raw(
        &mut engine,
        Event::ScopedVariablesUpdated {
            instance_key: key,
            scope_key: leaf,
            variables: vars(&[("c", Value::Int(300)), ("e", Value::Int(500))]),
        },
    );

    let inst = engine.state.instances.get(&key).unwrap();

    // The leaf sees: a from root, b from the middle shadow, c from its own
    // shadow, d inherited from middle, e local.
    let leaf_view = engine.visible_variables(inst, leaf);
    assert_eq!(leaf_view.get("a"), Some(&Value::Int(1)));
    assert_eq!(leaf_view.get("b"), Some(&Value::Int(20)));
    assert_eq!(leaf_view.get("c"), Some(&Value::Int(300)));
    assert_eq!(leaf_view.get("d"), Some(&Value::Int(40)));
    assert_eq!(leaf_view.get("e"), Some(&Value::Int(500)));

    // The middle sees its own shadow of b, root's c (NOT the leaf's), and never
    // the leaf-local e.
    let middle_view = engine.visible_variables(inst, middle);
    assert_eq!(middle_view.get("b"), Some(&Value::Int(20)));
    assert_eq!(middle_view.get("c"), Some(&Value::Int(3)));
    assert_eq!(middle_view.get("e"), None);

    // The root is untouched by any descendant shadow.
    let root_view = engine.visible_variables(inst, key);
    assert_eq!(root_view.get("b"), Some(&Value::Int(2)));
    assert_eq!(root_view.get("c"), Some(&Value::Int(3)));
    assert_eq!(root_view.get("d"), None);
}

#[test]
fn parallel_sibling_scopes_are_isolated() {
    // Two sibling scopes under the same root each shadow root `x` with a
    // different value and add a private local. Neither sees the other's
    // binding; the root is unchanged.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let key = create_instance_key(&mut engine, "order");
    apply_raw(
        &mut engine,
        Event::VariablesUpdated {
            instance_key: key,
            variables: vars(&[("x", Value::Int(1))]),
        },
    );
    let left: Key = 920_001;
    let right: Key = 920_002;
    for sib in [left, right] {
        apply_raw(
            &mut engine,
            Event::VariableScopeCreated {
                instance_key: key,
                scope_key: sib,
                parent_scope_key: key,
            },
        );
    }
    apply_raw(
        &mut engine,
        Event::ScopedVariablesUpdated {
            instance_key: key,
            scope_key: left,
            variables: vars(&[("x", Value::Int(10)), ("only_left", Value::Int(7))]),
        },
    );
    apply_raw(
        &mut engine,
        Event::ScopedVariablesUpdated {
            instance_key: key,
            scope_key: right,
            variables: vars(&[("x", Value::Int(20)), ("only_right", Value::Int(9))]),
        },
    );

    let inst = engine.state.instances.get(&key).unwrap();
    let left_view = engine.visible_variables(inst, left);
    let right_view = engine.visible_variables(inst, right);

    assert_eq!(left_view.get("x"), Some(&Value::Int(10)));
    assert_eq!(left_view.get("only_left"), Some(&Value::Int(7)));
    assert_eq!(
        left_view.get("only_right"),
        None,
        "left cannot see the sibling's local"
    );

    assert_eq!(right_view.get("x"), Some(&Value::Int(20)));
    assert_eq!(right_view.get("only_right"), Some(&Value::Int(9)));
    assert_eq!(
        right_view.get("only_left"),
        None,
        "right cannot see the sibling's local"
    );

    // Root's `x` is unchanged by either sibling's shadow.
    assert_eq!(
        engine.visible_variables(inst, key).get("x"),
        Some(&Value::Int(1))
    );
}

#[test]
fn local_write_shadows_an_inherited_root_variable_until_the_scope_is_destroyed() {
    // Zeebe input-mapping / `local=true` semantics: a local write of a name that
    // exists only at root creates a child-scope SHADOW, leaving the root value
    // intact; destroying the scope resurfaces the root value.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let key = create_instance_key(&mut engine, "order");
    apply_raw(
        &mut engine,
        Event::VariablesUpdated {
            instance_key: key,
            variables: vars(&[("x", Value::Int(1))]),
        },
    );
    let child: Key = 930_001;
    apply_raw(
        &mut engine,
        Event::VariableScopeCreated {
            instance_key: key,
            scope_key: child,
            parent_scope_key: key,
        },
    );

    // local=true pins the write to the child scope even though `x` is a root var.
    let writes = engine.propagate_variables(key, child, vars(&[("x", Value::Int(99))]), true);
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].0, child);
    for event in engine.propagated_updates(key, child, vars(&[("x", Value::Int(99))]), true) {
        apply_raw(&mut engine, event);
    }

    let inst = engine.state.instances.get(&key).unwrap();
    // Child shadows x=99; root still reads x=1.
    assert_eq!(
        engine.visible_variables(inst, child).get("x"),
        Some(&Value::Int(99))
    );
    assert_eq!(
        engine.visible_variables(inst, key).get("x"),
        Some(&Value::Int(1))
    );

    apply_raw(
        &mut engine,
        Event::VariableScopeDestroyed {
            instance_key: key,
            scope_key: child,
        },
    );
    // The shadow is gone; the child key now resolves to root and reads x=1.
    let inst = engine.state.instances.get(&key).unwrap();
    assert_eq!(
        engine.visible_variables(inst, key).get("x"),
        Some(&Value::Int(1))
    );
    assert!(!inst.scope_variables.contains_key(&child));
}

// ---------------------------------------------------------------------------
// Perf guard (Part C phase: perf)
//
// The scoping model must keep the common case — a flat, root-only instance —
// free. `element_variables` (the activation-path variable resolver) must return
// the instance's *shared* root `Arc` by pointer for a root-only instance: no
// clone, no merge, no allocation, byte-identical to the pre-scoping engine. A
// nested-scope instance instead allocates a freshly merged view. Pinning the
// pointer identity is a non-flaky structural guard against a future change
// accidentally putting an allocation on the hot flat-activation path.
// ---------------------------------------------------------------------------
#[test]
fn flat_instance_activation_returns_the_shared_root_arc_without_copying() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let key = create_instance_key(&mut engine, "order");
    apply_raw(
        &mut engine,
        Event::VariablesUpdated {
            instance_key: key,
            variables: vars(&[("a", Value::Int(1)), ("b", Value::Int(2))]),
        },
    );

    let root_arc = Arc::clone(&engine.state.instances.get(&key).unwrap().variables);

    // Flat instance: the element view is the very same allocation (zero-copy).
    let flat_view = engine.element_variables(key, key);
    assert!(
        Arc::ptr_eq(&flat_view, &root_arc),
        "flat activation must reuse the shared root Arc, not allocate a merged copy",
    );

    // Add a nested scope with a local shadow: the merged view is now a fresh
    // allocation (correctly no longer pointer-equal to root).
    let child: Key = 940_001;
    apply_raw(
        &mut engine,
        Event::VariableScopeCreated {
            instance_key: key,
            scope_key: child,
            parent_scope_key: key,
        },
    );
    apply_raw(
        &mut engine,
        Event::ScopedVariablesUpdated {
            instance_key: key,
            scope_key: child,
            variables: vars(&[("b", Value::Int(20))]),
        },
    );
    let scoped_view = engine.element_variables(key, child);
    assert!(
        !Arc::ptr_eq(&scoped_view, &root_arc),
        "a nested scope must produce a distinct merged view",
    );
    assert_eq!(scoped_view.get("a"), Some(&Value::Int(1)));
    assert_eq!(scoped_view.get("b"), Some(&Value::Int(20)));

    // The flat resolution for the root scope still returns the shared Arc even
    // after a sibling scope exists — only elements *in* a scope pay the merge.
    let still_flat = engine.element_variables(key, key);
    assert!(
        Arc::ptr_eq(&still_flat, &root_arc),
        "root-scope activation stays zero-copy even when other scopes exist",
    );
}

// --- DMN business rule task (native decision evaluation) --------------------

/// A one-decision DRG: a decision table `greeting` mapping `lang` -> a greeting
/// string, single string output (scalar result).
fn greeting_dmn() -> crate::dmn::DecisionRequirementsGraph {
    let xml = r##"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="drg" name="drg">
      <decision id="greeting" name="Greeting">
        <decisionTable hitPolicy="UNIQUE">
          <input id="i1"><inputExpression id="e1" typeRef="string"><text>lang</text></inputExpression></input>
          <output id="o1" name="result" typeRef="string" />
          <rule id="r1"><inputEntry id="ie1"><text>"en"</text></inputEntry>
            <outputEntry id="oe1"><text>"hello"</text></outputEntry></rule>
          <rule id="r2"><inputEntry id="ie2"><text>"de"</text></inputEntry>
            <outputEntry id="oe2"><text>"hallo"</text></outputEntry></rule>
        </decisionTable>
      </decision>
    </definitions>"##;
    crate::dmn::parse_dmn(xml).unwrap()
}

fn brt_process(decision_id: &str, result_variable: Option<String>) -> ProcessDefinition {
    ProcessBuilder::new("brt")
        .start_event("s")
        .business_rule_task("decide", decision_id, result_variable)
        .end_event("e")
        .connect("s", "decide")
        .connect("decide", "e")
        .build()
        .unwrap()
}

#[test]
fn business_rule_task_evaluates_decision_and_binds_result_variable() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployDecisionRequirements(vec![greeting_dmn()]))
        .unwrap();
    engine
        .apply_command(Command::DeployProcess(brt_process(
            "greeting",
            Some("greeting".to_string()),
        )))
        .unwrap();

    let mut vars = HashMap::new();
    vars.insert("lang".to_string(), Value::Str("de".into()));
    let events = engine
        .apply_command(Command::create_instance_with("brt", vars))
        .unwrap();
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The decision output was bound under the result variable...
    assert_eq!(
        merged_var(&events, "greeting"),
        Some(Value::Str("hallo".into()))
    );
    // ...a DecisionEvaluated audit record was emitted...
    assert!(events.iter().any(|e| matches!(
        e,
        Event::DecisionEvaluated { decision_id, .. } if decision_id == "greeting"
    )));
    // ...and the instance ran straight through to completion (no job, no wait).
    assert_eq!(
        engine.instance(inst).map(|i| i.state),
        Some(ProcessInstanceState::Completed)
    );
}

#[test]
fn business_rule_task_resolves_decision_id_via_feel_expression() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployDecisionRequirements(vec![greeting_dmn()]))
        .unwrap();
    engine
        .apply_command(Command::DeployProcess(brt_process(
            "=decisionToCall",
            Some("out".to_string()),
        )))
        .unwrap();

    let mut vars = HashMap::new();
    vars.insert("lang".to_string(), Value::Str("en".into()));
    vars.insert("decisionToCall".to_string(), Value::Str("greeting".into()));
    let events = engine
        .apply_command(Command::create_instance_with("brt", vars))
        .unwrap();
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(merged_var(&events, "out"), Some(Value::Str("hello".into())));
    let _ = inst;
}

#[test]
fn business_rule_task_unknown_decision_raises_incident() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(brt_process(
            "missing",
            Some("out".to_string()),
        )))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance_with("brt", HashMap::new()))
        .unwrap();
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();

    // An incident was raised and the instance is still active (parked).
    assert!(events.iter().any(|e| matches!(
        e,
        Event::IncidentRaised {
            kind: crate::state::IncidentKind::DecisionEvaluation,
            ..
        }
    )));
    assert_eq!(
        engine.instance(inst).map(|i| i.state),
        Some(ProcessInstanceState::Active)
    );
}

#[test]
fn deploy_decision_requirements_indexes_decisions_and_is_idempotent() {
    let mut engine = Engine::new();
    let events = engine
        .apply_command(Command::DeployDecisionRequirements(vec![greeting_dmn()]))
        .unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::DecisionRequirementsDeployed { .. })));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::DecisionDeployed { decision_id, version, .. } if decision_id == "greeting" && *version == 1
    )));
    assert!(engine.state().decisions.contains_key("greeting"));

    // Redeploying the identical DRG is a no-op (only DeploymentCreated).
    let again = engine
        .apply_command(Command::DeployDecisionRequirements(vec![greeting_dmn()]))
        .unwrap();
    assert!(!again
        .iter()
        .any(|e| matches!(e, Event::DecisionRequirementsDeployed { .. })));
    assert_eq!(engine.state().decision_requirements["drg"].version, 1);
}

#[test]
fn deploy_forms_mints_a_form_key_versions_per_id_and_is_idempotent() {
    use crate::command::FormResource;
    let mut engine = Engine::new();
    let form = |schema: &str| FormResource {
        id: "greeting-form".to_string(),
        resource_name: "greeting.form".to_string(),
        schema: schema.to_string(),
    };
    let v1 = r#"{"id":"greeting-form","type":"default","components":[]}"#;

    let events = engine
        .apply_command(Command::DeployForms(vec![form(v1)]))
        .unwrap();
    let deployed = events
        .iter()
        .find_map(|e| match e {
            Event::FormDeployed {
                form_key,
                version,
                form_id,
                schema,
                ..
            } => Some((*form_key, *version, form_id.clone(), schema.clone())),
            _ => None,
        })
        .expect("a FormDeployed event is emitted");
    assert_eq!(deployed.1, 1, "the first deploy is version 1");
    assert_eq!(deployed.2, "greeting-form");
    assert_eq!(deployed.3, v1, "the raw schema is carried on the event");
    let stored = &engine.state().forms["greeting-form"];
    assert_eq!(stored.key, deployed.0);
    assert_eq!(stored.version, 1);

    // Redeploying the identical form is a no-op (only DeploymentCreated).
    let again = engine
        .apply_command(Command::DeployForms(vec![form(v1)]))
        .unwrap();
    assert!(
        !again
            .iter()
            .any(|e| matches!(e, Event::FormDeployed { .. })),
        "an identical redeploy emits no FormDeployed"
    );
    assert_eq!(engine.state().forms["greeting-form"].version, 1);

    // A changed schema bumps the version and re-mints a key.
    let v2 = r#"{"id":"greeting-form","type":"default","components":[{"type":"textfield","key":"who"}]}"#;
    let changed = engine
        .apply_command(Command::DeployForms(vec![form(v2)]))
        .unwrap();
    let bumped = changed
        .iter()
        .find_map(|e| match e {
            Event::FormDeployed {
                form_key, version, ..
            } => Some((*form_key, *version)),
            _ => None,
        })
        .expect("a changed form redeploys");
    assert_eq!(bumped.1, 2, "a changed schema is version 2");
    assert_ne!(bumped.0, deployed.0, "a new form key is minted");
    assert_eq!(engine.state().forms["greeting-form"].version, 2);
}

#[test]
fn user_task_resolves_form_id_to_the_latest_form_key_and_carries_external_reference() {
    use crate::command::FormResource;
    use crate::model::UserTaskProps;

    let form = |schema: &str| FormResource {
        id: "feature-escalation".to_string(),
        resource_name: "feature-escalation.form".to_string(),
        schema: schema.to_string(),
    };
    let v1 = r#"{"id":"feature-escalation","type":"default","components":[]}"#;

    let mut engine = Engine::new();
    let deployed = engine
        .apply_command(Command::DeployForms(vec![form(v1)]))
        .unwrap();
    let form_key_v1 = deployed
        .iter()
        .find_map(|e| match e {
            Event::FormDeployed { form_key, .. } => Some(*form_key),
            _ => None,
        })
        .expect("a form key is minted");

    // A process with two user tasks: one bound to the embedded form by id, one
    // carrying an external form reference.
    let def = ProcessBuilder::new("feature")
        .start_event("start")
        .user_task_with(
            "escalation",
            UserTaskProps {
                form_id: Some("feature-escalation".to_string()),
                ..Default::default()
            },
        )
        .user_task_with(
            "external",
            UserTaskProps {
                external_form_reference: Some("https://forms.example/x".to_string()),
                ..Default::default()
            },
        )
        .end_event("end")
        .connect("start", "escalation")
        .connect("escalation", "external")
        .connect("external", "end")
        .build()
        .unwrap();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let created = engine
        .apply_command(Command::create_instance("feature"))
        .unwrap();

    // The embedded-form task resolves its formId to the deployed form's key.
    let escalation_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated {
                user_task_key,
                form_key: Some(k),
                ..
            } => Some((*user_task_key, *k)),
            _ => None,
        })
        .expect("the escalation task carries a resolved form key");
    assert_eq!(escalation_key.1, form_key_v1);
    let task = &engine.state().user_tasks[&escalation_key.0];
    assert_eq!(task.form_key, Some(form_key_v1));
    assert_eq!(task.external_form_reference, None);

    // Complete it to advance to the external-form task.
    engine
        .apply_command(Command::complete_user_task(escalation_key.0))
        .unwrap();

    let external = engine
        .state()
        .user_tasks
        .values()
        .find(|t| t.element_id == "external")
        .expect("the external task exists");
    assert_eq!(external.form_key, None);
    assert_eq!(
        external.external_form_reference.as_deref(),
        Some("https://forms.example/x")
    );
    let external_key = external.key;
    engine
        .apply_command(Command::complete_user_task(external_key))
        .unwrap();

    // Latest binding: redeploy a newer form version, then a fresh instance's
    // task resolves to the new key while the already-bound task keeps its key.
    let v2 = r#"{"id":"feature-escalation","type":"default","components":[{"type":"textfield","key":"why"}]}"#;
    let redeployed = engine
        .apply_command(Command::DeployForms(vec![form(v2)]))
        .unwrap();
    let form_key_v2 = redeployed
        .iter()
        .find_map(|e| match e {
            Event::FormDeployed { form_key, .. } => Some(*form_key),
            _ => None,
        })
        .expect("a new form key is minted");
    assert_ne!(form_key_v2, form_key_v1);

    let created2 = engine
        .apply_command(Command::create_instance("feature"))
        .unwrap();
    let escalation2 = created2
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated {
                form_key: Some(k), ..
            } => Some(*k),
            _ => None,
        })
        .expect("the new instance's escalation task resolves a form key");
    assert_eq!(
        escalation2, form_key_v2,
        "new tasks bind to the latest form"
    );
    // The first task's binding is unchanged (it kept its v1 key).
    assert_eq!(
        engine.state().user_tasks[&escalation_key.0].form_key,
        Some(form_key_v1)
    );
}

#[test]
fn user_task_with_external_reference_never_resolves_a_form_key_even_if_form_id_is_set() {
    use crate::command::FormResource;
    use crate::model::UserTaskProps;

    // A deployed form whose id would otherwise resolve, so this test proves the
    // suppression is deliberate (the form is present but must NOT be bound).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployForms(vec![FormResource {
            id: "feature-escalation".to_string(),
            resource_name: "feature-escalation.form".to_string(),
            schema: r#"{"id":"feature-escalation","type":"default","components":[]}"#.to_string(),
        }]))
        .unwrap();

    // A user task built programmatically with BOTH form_id and
    // external_form_reference set. Zeebe treats them as mutually exclusive, so
    // the engine must let the external reference win and resolve no form_key —
    // guarding the invariant independently of the BPMN parser.
    let def = ProcessBuilder::new("feature")
        .start_event("start")
        .user_task_with(
            "both",
            UserTaskProps {
                form_id: Some("feature-escalation".to_string()),
                external_form_reference: Some("https://forms.example/x".to_string()),
                ..Default::default()
            },
        )
        .end_event("end")
        .connect("start", "both")
        .connect("both", "end")
        .build()
        .unwrap();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance("feature"))
        .unwrap();

    let task = engine
        .state()
        .user_tasks
        .values()
        .find(|t| t.element_id == "both")
        .expect("the task exists");
    assert_eq!(
        task.form_key, None,
        "external reference suppresses form_key"
    );
    assert_eq!(
        task.external_form_reference.as_deref(),
        Some("https://forms.example/x")
    );
}

#[test]
fn deploy_generic_resources_mints_a_resource_key_versions_per_id_and_is_idempotent() {
    use crate::command::GenericResource;
    let mut engine = Engine::new();
    let resource = |content: &str| GenericResource {
        resource_id: "agent-prompt.md".to_string(),
        resource_name: "agent-prompt.md".to_string(),
        content: content.to_string(),
    };
    let v1 = "# Reviewer\n\nReview the PR.";

    let events = engine
        .apply_command(Command::DeployGenericResources(vec![resource(v1)]))
        .unwrap();
    let deployed = events
        .iter()
        .find_map(|e| match e {
            Event::GenericResourceDeployed {
                resource_key,
                version,
                resource_id,
                content,
                ..
            } => Some((
                *resource_key,
                *version,
                resource_id.clone(),
                content.clone(),
            )),
            _ => None,
        })
        .expect("a GenericResourceDeployed event is emitted");
    assert_eq!(deployed.1, 1, "the first deploy is version 1");
    assert_eq!(deployed.2, "agent-prompt.md", "resource_id is the filename");
    assert_eq!(deployed.3, v1, "the raw content is carried on the event");
    let stored = &engine.state().resources["agent-prompt.md"];
    assert_eq!(stored.key, deployed.0);
    assert_eq!(stored.version, 1);

    // Redeploying the identical resource (same name + content) is a no-op.
    let again = engine
        .apply_command(Command::DeployGenericResources(vec![resource(v1)]))
        .unwrap();
    assert!(
        !again
            .iter()
            .any(|e| matches!(e, Event::GenericResourceDeployed { .. })),
        "an identical redeploy emits no GenericResourceDeployed"
    );
    assert_eq!(engine.state().resources["agent-prompt.md"].version, 1);

    // Changed content under the same filename bumps the version and re-mints a key.
    let v2 = "# Reviewer\n\nReview the PR carefully and cite lines.";
    let changed = engine
        .apply_command(Command::DeployGenericResources(vec![resource(v2)]))
        .unwrap();
    let bumped = changed
        .iter()
        .find_map(|e| match e {
            Event::GenericResourceDeployed {
                resource_key,
                version,
                ..
            } => Some((*resource_key, *version)),
            _ => None,
        })
        .expect("changed content redeploys");
    assert_eq!(bumped.1, 2, "changed content is version 2");
    assert_ne!(bumped.0, deployed.0, "a new resource key is minted");
    assert_eq!(engine.state().resources["agent-prompt.md"].version, 2);
    assert_eq!(
        engine.state().resources["agent-prompt.md"].content,
        v2,
        "the latest content is stored"
    );

    // A different filename is an independent resource (its own version 1).
    let other = engine
        .apply_command(Command::DeployGenericResources(vec![GenericResource {
            resource_id: "other-prompt.md".to_string(),
            resource_name: "other-prompt.md".to_string(),
            content: "# Other".to_string(),
        }]))
        .unwrap();
    let other_v = other
        .iter()
        .find_map(|e| match e {
            Event::GenericResourceDeployed { version, .. } => Some(*version),
            _ => None,
        })
        .expect("a new filename deploys");
    assert_eq!(other_v, 1, "a different resource_id restarts at version 1");
    assert_eq!(engine.state().resources.len(), 2);
}

#[test]
fn deploying_multiple_form_versions_retains_every_version_by_key() {
    // Every deployed form version is retained under its own key in
    // `form_versions`; the latest-by-id index tracks the highest version, and
    // `form_by_key` / `form_version` resolve an OLD version.
    use crate::command::FormResource;
    let mut engine = Engine::new();
    let form = |schema: &str| FormResource {
        id: "f".to_string(),
        resource_name: "f.form".to_string(),
        schema: schema.to_string(),
    };
    let deploy = |engine: &mut Engine, schema: &str| -> (Key, i32) {
        engine
            .apply_command(Command::DeployForms(vec![form(schema)]))
            .unwrap()
            .iter()
            .find_map(|e| match e {
                Event::FormDeployed {
                    form_key, version, ..
                } => Some((*form_key, *version)),
                _ => None,
            })
            .expect("a changed form emits FormDeployed")
    };
    let (k1, v1) = deploy(&mut engine, r#"{"id":"f","components":[]}"#);
    let (k2, v2) = deploy(&mut engine, r#"{"id":"f","components":[{"key":"a"}]}"#);
    let (k3, v3) = deploy(&mut engine, r#"{"id":"f","components":[{"key":"b"}]}"#);
    assert_eq!((v1, v2, v3), (1, 2, 3));

    let st = engine.state();
    assert_eq!(st.form_versions.len(), 3, "all three versions are retained");
    assert_eq!(st.form_by_key(k1).unwrap().version, 1, "old version by key");
    assert_eq!(st.form_by_key(k2).unwrap().version, 2);
    assert_eq!(
        st.form_version("f", 1).unwrap().key,
        k1,
        "old by id+version"
    );
    assert_eq!(st.forms["f"].version, 3, "latest index tracks the newest");
    assert_eq!(st.forms["f"].key, k3);
}

#[test]
fn deploying_multiple_resource_versions_retains_every_version_by_key() {
    // Every deployed generic-resource version is retained under its own key in
    // `resource_versions`, resolvable via `resource_by_key` / `resource_version`
    // even though the latest-by-id index only holds the newest.
    use crate::command::GenericResource;
    let mut engine = Engine::new();
    let deploy = |engine: &mut Engine, content: &str| -> (Key, i32) {
        engine
            .apply_command(Command::DeployGenericResources(vec![GenericResource {
                resource_id: "p.md".to_string(),
                resource_name: "p.md".to_string(),
                content: content.to_string(),
            }]))
            .unwrap()
            .iter()
            .find_map(|e| match e {
                Event::GenericResourceDeployed {
                    resource_key,
                    version,
                    ..
                } => Some((*resource_key, *version)),
                _ => None,
            })
            .expect("changed content emits GenericResourceDeployed")
    };
    let (k1, _) = deploy(&mut engine, "v1");
    let (k2, _) = deploy(&mut engine, "v2");
    let (k3, _) = deploy(&mut engine, "v3");

    let st = engine.state();
    assert_eq!(st.resource_versions.len(), 3);
    assert_eq!(st.resource_by_key(k1).unwrap().content, "v1", "old by key");
    assert_eq!(st.resource_by_key(k2).unwrap().content, "v2");
    assert_eq!(st.resource_version("p.md", 1).unwrap().key, k1);
    assert_eq!(st.resources["p.md"].version, 3, "latest tracks the newest");
    assert_eq!(st.resources["p.md"].key, k3);
}

#[test]
fn deploying_multiple_drg_versions_retains_every_version_and_evaluates_old_by_key() {
    // Every DRG/decision version is retained by key; an EvaluateDecision pinned
    // to an OLD decision key resolves and evaluates that exact version — the
    // concrete regression the latest-only index could not serve.
    let mut engine = Engine::new();
    let deploy = |engine: &mut Engine| -> (Key, Key, i32) {
        // (decision_requirements_key, decision_key, version)
        let events = engine
            .apply_command(Command::DeployDecisionRequirements(vec![greeting_dmn()]))
            .unwrap();
        let drg_key = events
            .iter()
            .find_map(|e| match e {
                Event::DecisionRequirementsDeployed {
                    decision_requirements_key,
                    ..
                } => Some(*decision_requirements_key),
                _ => None,
            })
            .expect("DRG deployed");
        let (dkey, ver) = events
            .iter()
            .find_map(|e| match e {
                Event::DecisionDeployed {
                    decision_key,
                    version,
                    ..
                } => Some((*decision_key, *version)),
                _ => None,
            })
            .expect("decision deployed");
        (drg_key, dkey, ver)
    };
    // Deploy v1, then force a v2 by deploying a *changed* DRG (same id).
    let (_drg1, decision_k1, v1) = deploy(&mut engine);
    assert_eq!(v1, 1);
    // A changed DRG: swap an output value so the content differs and versions.
    let changed_xml = r##"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="drg" name="drg">
      <decision id="greeting" name="Greeting">
        <decisionTable hitPolicy="UNIQUE">
          <input id="i1"><inputExpression id="e1" typeRef="string"><text>lang</text></inputExpression></input>
          <output id="o1" name="result" typeRef="string" />
          <rule id="r1"><inputEntry id="ie1"><text>"en"</text></inputEntry>
            <outputEntry id="oe1"><text>"hi"</text></outputEntry></rule>
        </decisionTable>
      </decision>
    </definitions>"##;
    let changed = engine
        .apply_command(Command::DeployDecisionRequirements(vec![
            crate::dmn::parse_dmn(changed_xml).unwrap(),
        ]))
        .unwrap();
    let (decision_k2, v2) = changed
        .iter()
        .find_map(|e| match e {
            Event::DecisionDeployed {
                decision_key,
                version,
                ..
            } => Some((*decision_key, *version)),
            _ => None,
        })
        .expect("changed DRG redeploys the decision");
    assert_eq!(v2, 2);
    assert_ne!(decision_k1, decision_k2);

    let st = engine.state();
    assert_eq!(st.decision_versions.len(), 2, "both versions retained");
    assert_eq!(st.decision_requirements_versions.len(), 2);
    assert_eq!(st.decision_by_key(decision_k1).unwrap().version, 1);
    assert_eq!(st.decisions["greeting"].version, 2, "latest tracks newest");

    // Evaluate the OLD version by its key: it returns "hello" (v1), whereas the
    // latest (v2) would return "hi". This is impossible without retention.
    let inputs = vars(&[("lang", Value::Str("en".to_string()))]);
    let old = engine
        .evaluate_deployed_decision(None, Some(decision_k1), &inputs)
        .expect("old decision resolves by key");
    assert_eq!(old.version, 1);
    assert_eq!(old.result.decision_output, Value::Str("hello".to_string()));

    let latest = engine
        .evaluate_deployed_decision(Some("greeting"), None, &inputs)
        .expect("latest decision resolves by id");
    assert_eq!(latest.version, 2);
    assert_eq!(latest.result.decision_output, Value::Str("hi".to_string()));
}

#[test]
#[cfg(feature = "serde")]
fn version_retention_survives_a_snapshot_round_trip() {
    // The `*_versions` maps are part of State, so a serde snapshot round-trip
    // preserves every retained version.
    use crate::command::{FormResource, GenericResource};
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployForms(vec![FormResource {
            id: "f".to_string(),
            resource_name: "f.form".to_string(),
            schema: r#"{"id":"f","components":[]}"#.to_string(),
        }]))
        .unwrap();
    engine
        .apply_command(Command::DeployForms(vec![FormResource {
            id: "f".to_string(),
            resource_name: "f.form".to_string(),
            schema: r#"{"id":"f","components":[{"key":"a"}]}"#.to_string(),
        }]))
        .unwrap();
    engine
        .apply_command(Command::DeployGenericResources(vec![GenericResource {
            resource_id: "p.md".to_string(),
            resource_name: "p.md".to_string(),
            content: "v1".to_string(),
        }]))
        .unwrap();
    engine
        .apply_command(Command::DeployGenericResources(vec![GenericResource {
            resource_id: "p.md".to_string(),
            resource_name: "p.md".to_string(),
            content: "v2".to_string(),
        }]))
        .unwrap();

    let serialized = serde_json::to_vec(&engine.snapshot()).unwrap();
    let restored = Engine::from_snapshot(serde_json::from_slice(&serialized).unwrap());
    let restored = restored.state();
    assert_eq!(restored.form_versions.len(), 2, "form versions round-trip");
    assert_eq!(restored.resource_versions.len(), 2, "resource versions too");
    assert_eq!(restored.forms["f"].version, 2);
    assert_eq!(restored.resources["p.md"].version, 2);
}

#[test]
fn business_rule_task_spreads_map_output_without_result_variable() {
    // A two-output decision table yields a map output; with no result variable
    // its entries are spread into the instance scope.
    let xml = r##"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="drg2" name="drg2">
      <decision id="scores" name="Scores">
        <decisionTable hitPolicy="UNIQUE">
          <input id="i1"><inputExpression id="e1" typeRef="string"><text>tier</text></inputExpression></input>
          <output id="o1" name="discount" typeRef="number" />
          <output id="o2" name="priority" typeRef="number" />
          <rule id="r1"><inputEntry id="ie1"><text>"gold"</text></inputEntry>
            <outputEntry id="oe1"><text>20</text></outputEntry>
            <outputEntry id="oe2"><text>1</text></outputEntry></rule>
        </decisionTable>
      </decision>
    </definitions>"##;
    let drg = crate::dmn::parse_dmn(xml).unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployDecisionRequirements(vec![drg]))
        .unwrap();
    engine
        .apply_command(Command::DeployProcess(brt_process("scores", None)))
        .unwrap();

    let mut vars = HashMap::new();
    vars.insert("tier".to_string(), Value::Str("gold".into()));
    let events = engine
        .apply_command(Command::create_instance_with("brt", vars))
        .unwrap();
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(merged_var(&events, "discount"), Some(Value::Int(20)));
    assert_eq!(merged_var(&events, "priority"), Some(Value::Int(1)));
    let _ = inst;
}

// ---------------------------------------------------------------------------
// Ad-hoc sub-process runtime (ADR 0023 seam 2): the agentic activate-element
// loop. A JOB_WORKER ad-hoc container emits an "agent" job; the agent returns
// `activateElements[]`; the engine activates those inner tools as real element
// instances, drains them, re-emits the agent job for the next turn, and
// completes the container (writing its `outputCollection`) when the agent
// signals it is done.
// ---------------------------------------------------------------------------

fn adhoc_agent_process() -> ProcessDefinition {
    // A top-level JOB_WORKER ad-hoc container ("agent") with two service-task
    // tools. `outputElement="=result"` captures each tool's `result` output into
    // the container's `outputCollection` ("results").
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn adhoc_agent_with_user_task_tool() -> ProcessDefinition {
    // A JOB_WORKER ad-hoc container whose tool catalog mixes a service-task tool
    // (`toolA`, a `tool` job) and a native user-task tool (`ask`, a
    // human-in-the-loop tool with a static assignee). ADR 0023 lists user tasks
    // as an in-scope v1 tool kind: activating one must create a real user task
    // and park the child until it is completed — not silently pass through.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:userTask id="ask">
              <bpmn:extensionElements>
                <zeebe:assignmentDefinition assignee="alice" />
              </bpmn:extensionElements>
            </bpmn:userTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn adhoc_agent_named_tools_process() -> ProcessDefinition {
    // Like `adhoc_agent_process` but the two tools carry human `name`s, so the
    // advertised tool catalog can be checked for both `elementId` and
    // `elementName`.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA" name="Search the web">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="toolB" name="Send an email">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn activate_element(id: &str) -> crate::model::AdHocActivateElement {
    activate_element_with(id, &[])
}

fn activate_element_with(
    id: &str,
    variables: &[(&str, Value)],
) -> crate::model::AdHocActivateElement {
    crate::model::AdHocActivateElement {
        element_id: id.to_string(),
        variables: variables
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    }
}

/// Regression for Magikcraft/nano-bpm#605: an agent's
/// `activateElements[{ elementId, variables }]` must scope those `variables`
/// into the activated tool's OWN job — including tools that declare no
/// `ioMapping` (the `toolA`/`toolB` fixture tools have none). The reported bug
/// dropped the seed variables entirely, so a no-mapping tool saw only the
/// instance-root scope. This asserts the whole defect class: a plain tool AND a
/// second tool activated in the same turn each receive only their own seed
/// variables, with no cross-contamination.
#[test]
fn adhoc_activation_variables_reach_a_tool_job_without_io_mapping() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");

    // Turn 1: activate both no-ioMapping tools, each with a distinct seed var.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![
                    activate_element_with("toolA", &[("fromActivation", Value::Str("A".into()))]),
                    activate_element_with("toolB", &[("fromActivation", Value::Str("B".into()))]),
                ],
                ..Default::default()
            },
        ))
        .unwrap();

    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(
        tool_jobs.len(),
        2,
        "exactly the two activated tools emit jobs — no duplicates or extras (#605)"
    );
    let tool_a = tool_jobs
        .iter()
        .find(|j| j.element_id == "toolA")
        .expect("toolA job emitted");
    let tool_b = tool_jobs
        .iter()
        .find(|j| j.element_id == "toolB")
        .expect("toolB job emitted");

    // Gaps #2 and #8 (#614) both write LOCAL container-scope variables on
    // activation — the tool catalog `adHocSubProcessElements` (gap #2) and the
    // seeded `outputCollection` `results: []` (gap #8). Activated tools are
    // children of that scope, so — like Camunda's scope inheritance — each tool
    // job legitimately inherits BOTH alongside its own activation seed. Assert
    // the FULL scope exactly (seed + inherited catalog + inherited empty
    // collection): this still guards #605's defect class, since any root
    // bleed-through or a sibling tool's seed would break equality.
    let expected_catalog = || {
        Value::List(vec![
            Value::Map(std::collections::BTreeMap::from([
                ("elementId".to_string(), Value::Str("toolA".into())),
                ("elementName".to_string(), Value::Str(String::new())),
            ])),
            Value::Map(std::collections::BTreeMap::from([
                ("elementId".to_string(), Value::Str("toolB".into())),
                ("elementName".to_string(), Value::Str(String::new())),
            ])),
        ])
    };
    assert_eq!(
        *tool_a.variables,
        HashMap::from([
            ("fromActivation".to_string(), Value::Str("A".into())),
            ("adHocSubProcessElements".to_string(), expected_catalog()),
            ("results".to_string(), Value::List(vec![])),
        ]),
        "toolA's job scope is its own activation seed PLUS the inherited \
         container catalog (gap #2) and empty outputCollection (gap #8) — \
         reaches the job with no ioMapping, and \
         carries no root bleed-through or toolB seed (#605)"
    );
    assert_eq!(
        *tool_b.variables,
        HashMap::from([
            ("fromActivation".to_string(), Value::Str("B".into())),
            ("adHocSubProcessElements".to_string(), expected_catalog()),
            ("results".to_string(), Value::List(vec![])),
        ]),
        "toolB's job scope is its own activation seed PLUS the inherited \
         container catalog (gap #2) and empty outputCollection (gap #8) — \
         no cross-contamination from toolA's \
         concurrently-activated seed (#605)"
    );

    // The exact failure mode #605 describes: a seed merged into the shared
    // instance-root scope would still satisfy the per-job assertions above while
    // reintroducing the global-namespace leak. Guard it directly — the seeds
    // must stay scoped to their tool jobs and never surface at the root.
    assert_eq!(
        io_var(&engine, inst, "fromActivation"),
        None,
        "activation seeds must stay scoped to their tool jobs, never merged \
         into the instance-root scope (#605)"
    );

    assert!(
        !engine.is_completed(inst),
        "container still parked while the seeded tools run"
    );
}

/// Regression for Magikcraft/nano-bpm#614 gap 3 (Zeebe parity): the external
/// "activate ad-hoc activities" command
/// (`AdHocSubProcessInstructionActivateProcessor` — REST
/// `POST /element-instances/ad-hoc-activities/{key}/activation`) activates named
/// tools on an already-running ad-hoc container WITHOUT completing its agent
/// job, driving the same activation machinery the agent-job path does — and
/// rejecting a bad instruction identically. This asserts the whole defect
/// class: an unknown container key is NOT_FOUND, an unknown element id is
/// NOT_FOUND (leaving the container untouched), a valid target activates its
/// tool without consuming the agent job, and `cancelRemainingInstances`
/// completes the container.
#[test]
fn adhoc_external_command_activates_tools_without_the_agent_job() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    // Discover the container's element-instance key via its agent job, but do
    // NOT complete the job — the external command activates independently, and
    // both seams must coexist.
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;

    // An `adHocSubProcessInstanceKey` that identifies no active container is
    // rejected NOT_FOUND.
    let missing = engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: 999_999,
            activate_elements: vec![activate_element("toolA")],
            cancel_remaining: false,
        })
        .unwrap_err();
    assert!(
        matches!(missing, EngineError::AdHocSubProcessNotFound { .. }),
        "an unknown container key must be rejected (NOT_FOUND parity), got {missing:?}"
    );

    // An unknown element id is rejected NOT_FOUND — identically to the agent-job
    // path — and leaves the container untouched (no phantom child).
    let ghost = engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: container,
            activate_elements: vec![activate_element("ghost")],
            cancel_remaining: false,
        })
        .unwrap_err();
    assert!(
        matches!(&ghost, EngineError::AdHocUnknownElement { element_id, .. } if element_id == "ghost"),
        "an unknown element id must be rejected (NOT_FOUND parity), got {ghost:?}"
    );
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        0,
        "a rejected external activation leaves the container untouched"
    );

    // Happy path: activating a real tool mints its job and marks it active,
    // WITHOUT consuming the agent job (the container is still running).
    let events = engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: container,
            activate_elements: vec![activate_element("toolA")],
            cancel_remaining: false,
        })
        .expect("external activation of a real tool succeeds");
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        1,
        "the external command activated `toolA`"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::JobCreated { element_id, .. } if element_id == "toolA")),
        "the activated tool's own job was created"
    );
    assert!(
        !engine.is_completed(inst),
        "the container is still running — the external command did not complete the agent job"
    );

    // `cancelRemainingInstances` cancels the in-flight tool and completes the
    // container (which then flows to the end event, completing the instance).
    engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: container,
            activate_elements: Vec::new(),
            cancel_remaining: true,
        })
        .expect("cancel-remaining completes the container");
    assert!(
        engine.is_completed(inst),
        "cancelRemainingInstances completed the container and the instance"
    );
}

/// Regression for Magikcraft/nano-bpm#614 gap 3: the external "activate ad-hoc
/// activities" command must NOT let an empty request (`elements: []` with
/// `cancelRemainingInstances = false`) implicitly complete a parked container.
/// The REST schema permits an empty `elements` array, so without this guard a
/// client could accidentally finish a running instance by POSTing `{}`. The
/// command rejects it as INVALID_ARGUMENT (mapped to HTTP 400) and leaves the
/// container running; completion is only expressible via
/// `cancelRemainingInstances`. (The agent-job completion seam — gap 4 — is
/// unaffected: activating nothing there still ends the turn.)
#[test]
fn adhoc_external_command_rejects_empty_activation() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;

    // Empty elements with no cancel is rejected as INVALID_ARGUMENT — it must
    // not complete the parked container.
    let empty = engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: container,
            activate_elements: Vec::new(),
            cancel_remaining: false,
        })
        .unwrap_err();
    assert!(
        matches!(&empty, EngineError::AdHocNoActivationTargets { ad_hoc_instance_key } if *ad_hoc_instance_key == container),
        "an empty activation with no cancel must be rejected (INVALID_ARGUMENT), got {empty:?}"
    );
    assert!(
        !engine.is_completed(inst),
        "a rejected empty activation must leave the container running, not complete it"
    );
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        0,
        "a rejected empty activation activates nothing"
    );
}

/// Gap #2 (issue #614): a JOB_WORKER ad-hoc container must advertise its tool
/// catalog to the agent by writing a local variable `adHocSubProcessElements`
/// (Camunda `AdHocSubProcessProcessor.onActivate` →
/// `AD_HOC_SUB_PROCESS_ELEMENTS`) so the agent can discover which tools it may
/// activate — a list of `{ elementId, elementName }` entries, one per catalog
/// tool, in document order. On `main` the agent job carries no such variable.
/// Class-scoped: asserts the variable is present on the agent job, is a list of
/// exactly the container's tools, and carries each tool's id and human name.
#[test]
fn adhoc_container_advertises_its_tool_catalog_on_the_agent_job() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_named_tools_process()))
        .unwrap();
    let _inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");

    let catalog = agent
        .variables
        .get("adHocSubProcessElements")
        .expect("the agent job advertises its tool catalog (#614 gap 2)");
    let entries = match catalog {
        Value::List(items) => items,
        other => panic!("expected a list of tool metadata, got {other:?}"),
    };
    assert_eq!(
        entries.len(),
        2,
        "one catalog entry per activatable tool, in document order"
    );

    let entry = |elem_id: &str, name: &str| {
        Value::Map(
            [
                ("elementId".to_string(), Value::Str(elem_id.to_string())),
                ("elementName".to_string(), Value::Str(name.to_string())),
            ]
            .into_iter()
            .collect(),
        )
    };
    assert_eq!(
        entries,
        &vec![
            entry("toolA", "Search the web"),
            entry("toolB", "Send an email"),
        ],
        "each entry carries the tool's id and human name"
    );
}

/// Regression for the tool-catalog filter: the parser captures known
/// non-activatable inner nodes (e.g. gateways) in `def.tools` as
/// `AdHocToolKind::Other`, but the advertised `adHocSubProcessElements` catalog
/// must only surface activatable tools (Zeebe parity). The inner exclusive
/// gateway below must NOT appear in the advertised list.
#[test]
fn adhoc_container_advertised_catalog_excludes_non_activatable_nodes() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA" name="Search the web">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:exclusiveGateway id="gw" name="Not a tool" />
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let process = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);

    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process))
        .unwrap();
    let _inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");

    let catalog = agent
        .variables
        .get("adHocSubProcessElements")
        .expect("the agent job advertises its tool catalog (#614 gap 2)");
    let entries = match catalog {
        Value::List(items) => items,
        other => panic!("expected a list of tool metadata, got {other:?}"),
    };

    let entry = |elem_id: &str, name: &str| {
        Value::Map(
            [
                ("elementId".to_string(), Value::Str(elem_id.to_string())),
                ("elementName".to_string(), Value::Str(name.to_string())),
            ]
            .into_iter()
            .collect(),
        )
    };
    assert_eq!(
        entries,
        &vec![entry("toolA", "Search the web")],
        "only the activatable service task is advertised; the gateway (Other) is excluded"
    );
}

#[test]
fn adhoc_agent_activates_tools_loops_and_completes_with_output_collection() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    // The container activated and emitted its agent job on the container element
    // instance; its runtime state is registered but no tools are active yet.
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        0,
        "no tools active before the first agent turn"
    );

    // Turn 1: the agent activates both tools. The engine instantiates them as
    // real element instances (each a `tool` job) inside the container scope.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA"), activate_element("toolB")],
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        2,
        "both tools active after the agent's activate-element instruction"
    );
    assert!(
        !engine.is_completed(inst),
        "container parks while tools run"
    );

    // Drain the two tool jobs, each producing a `result`. The container captures
    // each via `outputElement`; when the last drains, the agent job re-emits.
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool_jobs.len(), 2, "both tools produced jobs");
    for (job, val) in tool_jobs.iter().zip(["A", "B"]) {
        let mut vars = HashMap::new();
        vars.insert("result".to_string(), Value::Str(val.to_string()));
        engine
            .apply_command(Command::complete_job_with(job.key, vars))
            .unwrap();
    }
    let adhoc = engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .unwrap();
    assert_eq!(adhoc.active.len(), 0, "all tools drained");
    assert_eq!(
        adhoc.iterations, 1,
        "agent job re-emitted for the next turn"
    );
    assert_eq!(
        container_output_collection(&engine, inst, container, "results").map(|v| match v {
            Value::List(l) => l.len(),
            _ => usize::MAX,
        }),
        Some(2),
        "two tool outputs accumulated in the live outputCollection",
    );
    assert!(
        !engine.is_completed(inst),
        "container still awaiting the agent"
    );

    // Turn 2: the agent returns no activations (it is done) → the container
    // completes, writes its `outputCollection`, and takes its outgoing flow to
    // the end event, completing the instance.
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job re-emitted");
    // The container writes its `outputCollection` as it completes; a terminal
    // instance drops its variable payload (ADR 0012), so we read the collection
    // off the emitted `VariablesUpdated` rather than hot state.
    let final_events = engine
        .apply_command(Command::complete_job_with_result(
            agent2.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap();

    assert!(
        engine.is_completed(inst),
        "container completed → instance done"
    );
    let results = final_events.iter().find_map(|e| match e {
        Event::VariablesUpdated { variables, .. } => variables.get("results").cloned(),
        _ => None,
    });
    let mut got = match results {
        Some(Value::List(v)) => v,
        other => panic!("expected outputCollection list, got {other:?}"),
    };
    got.sort_by_key(|v| match v {
        Value::Str(s) => s.clone(),
        _ => String::new(),
    });
    assert_eq!(
        got,
        vec![Value::Str("A".to_string()), Value::Str("B".to_string())],
        "outputCollection holds both tool results"
    );
}

/// Gap #9 (issue #614): each activated ad-hoc tool must run beneath a dedicated
/// `AD_HOC_SUB_PROCESS_INNER_INSTANCE` element (id `<container>#innerInstance`),
/// exactly as Zeebe's `BpmnAdHocSubProcessBehavior.createInnerInstance` nests
/// `container → innerInstance → tool`. On `main` nano activates the tool child
/// directly under the container (`scopes[child] == container`), so the
/// read-model element-instance tree is one level too shallow and cannot be
/// migrated/queried like a Zeebe process. This asserts the whole defect class:
/// (a) two tools activated in one turn each get their OWN inner instance whose
/// scope is the container and whose element id is the inner-instance id;
/// (b) the tool child's scope is its inner instance (not the container); and
/// (c) completing a tool tears its inner instance down too, leaving no dangling
/// element instance in the read model.
#[test]
fn adhoc_tools_run_under_a_dedicated_inner_instance_element() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;

    // Turn 1: activate both tools.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA"), activate_element("toolB")],
                ..Default::default()
            },
        ))
        .unwrap();

    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool_jobs.len(), 2, "both tools produced jobs");

    for job in &tool_jobs {
        let child = job.element_instance_key;
        // (b) the tool child hangs off an inner instance, NOT the container.
        let inner = engine.instance(inst).unwrap().scopes.get(&child).copied();
        let inner = inner.expect("tool child has a read-model scope");
        assert_ne!(
            inner, container,
            "tool child's scope is a dedicated inner instance, not the container"
        );
        // (a) the inner instance is scoped to the container and carries the
        // `<container>#innerInstance` element id.
        assert_eq!(
            engine.instance(inst).unwrap().scopes.get(&inner).copied(),
            Some(container),
            "inner instance is scoped to the container"
        );
        assert_eq!(
            engine.instance(inst).unwrap().active.get(&inner).cloned(),
            Some("agent#innerInstance".to_string()),
            "inner instance carries the ad-hoc inner-instance element id"
        );
    }

    // Each tool got its OWN inner instance (no sharing).
    let inners: Vec<Key> = tool_jobs
        .iter()
        .map(|j| {
            engine
                .instance(inst)
                .unwrap()
                .scopes
                .get(&j.element_instance_key)
                .copied()
                .unwrap()
        })
        .collect();
    assert_ne!(
        inners[0], inners[1],
        "each tool gets its own inner instance"
    );

    // (c) completing a tool tears its inner instance down — no dangling
    // element instance survives in the read model.
    let first = tool_jobs[0].element_instance_key;
    let first_inner = inners[0];
    let mut vars = HashMap::new();
    vars.insert("result".to_string(), Value::Str("A".to_string()));
    engine
        .apply_command(Command::complete_job_with(tool_jobs[0].key, vars))
        .unwrap();
    let live = engine.instance(inst).unwrap();
    assert!(
        !live.active.contains_key(&first_inner),
        "the completed tool's inner instance is torn down"
    );
    assert!(
        !live.active.contains_key(&first),
        "the completed tool child is torn down"
    );
    assert!(
        !live.scopes.contains_key(&first_inner),
        "the inner instance leaves no read-model scope entry behind"
    );
}

/// Gap #1 (issue #614): a native **user-task** tool must execute, not silently
/// pass through. Activating one has to create a real user task and PARK the
/// child inside the container scope until it is completed via `CompleteUserTask`
/// — mirroring an ordinary user-task activation. On `main` the non-service-task
/// kinds fall into `activate_adhoc_tool`'s `None` arm and are short-circuited to
/// immediate completion, so no user task is ever created and the child never
/// parks. This asserts the whole defect class: the tool child stays active with
/// a real user task, the container keeps looping, and the human's completion
/// output flows through the container's `outputElement` exactly like a
/// service-task tool's job output.
#[test]
fn adhoc_agent_activates_a_user_task_tool_and_parks_until_completed() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_with_user_task_tool()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;

    // Turn 1: the agent activates the user-task tool. It must create a user task
    // and keep the child ACTIVE — not auto-complete it.
    let activated = engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("ask")],
                ..Default::default()
            },
        ))
        .unwrap();

    let user_task_key = activated
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated {
                user_task_key,
                element_id,
                assignee,
                ..
            } if element_id == "ask" => {
                assert_eq!(
                    assignee.as_deref(),
                    Some("alice"),
                    "the tool's static assignee is resolved on activation"
                );
                Some(*user_task_key)
            }
            _ => None,
        })
        .expect("activating a user-task tool creates a real user task (#614 gap 1)");

    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        1,
        "the user-task tool child stays active — it must not auto-complete"
    );
    assert!(
        !engine.is_completed(inst),
        "container parks while the human task is open"
    );

    // Complete the human task with an output; it flows through the container's
    // `outputElement` and, as the last active tool, re-emits the agent job.
    let mut vars = HashMap::new();
    vars.insert("result".to_string(), Value::Str("approved".to_string()));
    engine
        .apply_command(Command::complete_user_task_with(user_task_key, vars))
        .unwrap();

    let adhoc = engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .unwrap();
    assert_eq!(
        adhoc.active.len(),
        0,
        "the user-task tool drained on completion"
    );
    assert_eq!(
        adhoc.iterations, 1,
        "agent job re-emitted for the next turn after the human finished"
    );
    assert!(
        matches!(
            container_output_collection(&engine, inst, container, "results"),
            Some(Value::List(ref v)) if v.len() == 1
        ),
        "the human's `result` was appended to the container's outputCollection \
         (gap #8 lifecycle: accumulated into the local scope variable, visible \
         mid-run), got {:?}",
        container_output_collection(&engine, inst, container, "results")
    );

    // Turn 2: agent is done → container completes and writes its collection.
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job re-emitted");
    let final_events = engine
        .apply_command(Command::complete_job_with_result(
            agent2.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "container completed → instance done"
    );
    let results = final_events.iter().find_map(|e| match e {
        Event::VariablesUpdated { variables, .. } => variables.get("results").cloned(),
        _ => None,
    });
    assert_eq!(
        results,
        Some(Value::List(vec![Value::Str("approved".to_string())])),
        "the user-task tool's output reached the container's outputCollection"
    );
}

// Reads the ad-hoc container's local `outputCollection` variable (`results`)
// straight off the container scope — the live value visible mid-run, before the
// container completes and propagates it outward.
fn container_output_collection(
    engine: &Engine,
    instance_key: Key,
    container: Key,
    name: &str,
) -> Option<Value> {
    engine
        .instance(instance_key)
        .unwrap()
        .scope_variables
        .get(&container)
        .and_then(|m| m.get(name))
        .cloned()
}

#[test]
fn adhoc_output_collection_is_initialised_empty_on_activation() {
    // Zeebe parity (AdHocSubProcessProcessor.onActivate): a declared
    // `outputCollection` is seeded to an empty array as a *local* container
    // variable when the container activates — so the agent (and any FEEL that
    // reads it) sees `results = []` before a single tool has run. nano used to
    // materialise the collection only at completion, so mid-run it did not exist.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let container = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted")
        .element_instance_key;

    assert_eq!(
        container_output_collection(&engine, inst, container, "results"),
        Some(Value::List(vec![])),
        "outputCollection seeded to an empty array on activation, visible in the container scope",
    );
}

#[test]
fn adhoc_output_collection_grows_and_is_visible_during_the_run() {
    // Zeebe appends each completed element's `outputElement` to the local
    // `outputCollection` as it goes (beforeExecutionPathCompleted), so a
    // mid-run agent turn can inspect the accumulated results. nano used to keep
    // them in a hidden accumulator invisible until the container completed.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA"), activate_element("toolB")],
                ..Default::default()
            },
        ))
        .unwrap();
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    for (job, val) in tool_jobs.iter().zip(["A", "B"]) {
        let mut vars = HashMap::new();
        vars.insert("result".to_string(), Value::Str(val.to_string()));
        engine
            .apply_command(Command::complete_job_with(job.key, vars))
            .unwrap();
    }

    // The container has NOT completed yet (it is awaiting the agent's next turn),
    // but the collection is already visible and holds both tool outputs.
    assert!(
        !engine.is_completed(inst),
        "container still awaiting the agent"
    );
    let mut got = match container_output_collection(&engine, inst, container, "results") {
        Some(Value::List(v)) => v,
        other => panic!("expected a live outputCollection list mid-run, got {other:?}"),
    };
    got.sort_by_key(|v| match v {
        Value::Str(s) => s.clone(),
        _ => String::new(),
    });
    assert_eq!(
        got,
        vec![Value::Str("A".to_string()), Value::Str("B".to_string())],
        "outputCollection accumulates each tool's output as the run progresses",
    );
}

fn adhoc_non_array_output_collection_process() -> ProcessDefinition {
    // A container whose own input mapping overwrites the seeded `outputCollection`
    // (`results`) with a scalar — the misconfiguration Zeebe guards against.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
              <zeebe:ioMapping>
                <zeebe:input source="=5" target="results" />
              </zeebe:ioMapping>
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

#[test]
fn adhoc_output_collection_non_array_target_raises_extract_value_incident() {
    // Zeebe (AdHocSubProcessOutputCollectionBehavior.appendToOutputCollection)
    // raises EXTRACT_VALUE_ERROR when the target is not an array. nano used to
    // silently accumulate into its hidden Vec and overwrite the scalar at
    // completion. The type guard must reject it instead — and for full parity it
    // DEFERS the tool's completion: the incident sits on the TOOL child (so
    // resolving it retries the append), the tool stays active with its scope
    // intact, and the scalar target is left uncorrupted.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_non_array_output_collection_process(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;
    // The container input mapping overwrote the seeded array with a scalar.
    assert_eq!(
        container_output_collection(&engine, inst, container, "results"),
        Some(Value::Int(5)),
        "input mapping set results to a non-array before any tool ran",
    );

    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();
    let tool = engine
        .activate_jobs("tool", "W", 10, 1_000, 0)
        .into_iter()
        .next()
        .expect("tool job");
    let mut vars = HashMap::new();
    vars.insert("result".to_string(), Value::Str("x".to_string()));
    engine
        .apply_command(Command::complete_job_with(tool.key, vars))
        .unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "exactly one incident raised");
    assert_eq!(
        active[0].kind,
        state::IncidentKind::ExpressionEvaluation,
        "non-array outputCollection maps to the EXTRACT_VALUE_ERROR taxonomy",
    );
    assert_eq!(
        active[0].element_id, "toolA",
        "incident sits on the tool child, so resolving it re-drives the append",
    );
    assert_eq!(
        active[0].element_instance_key, tool.element_instance_key,
        "incident is parked on the tool's element instance, not the container",
    );
    // The scalar is left untouched — no silent corruption into a list.
    assert_eq!(
        container_output_collection(&engine, inst, container, "results"),
        Some(Value::Int(5)),
        "the non-array target is not overwritten by a phantom collection",
    );
    assert!(
        !engine.is_completed(inst),
        "container parks on the incident"
    );
    // The tool's completion is DEFERRED (no `AdHocToolCompleted` emitted): it
    // stays in the container's `active` set with its local scope intact so that
    // resolving the incident retries the append against the corrected target —
    // nothing is discarded.
    let adhoc = engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .unwrap();
    assert!(
        adhoc.active.contains(&tool.element_instance_key),
        "the tool stays active while parked on the incident",
    );
}

#[test]
fn adhoc_output_collection_incident_resolution_retries_the_append() {
    // Full Zeebe parity for the EXTRACT_VALUE_ERROR incident: it is recoverable.
    // After correcting the `outputCollection` target to an array and resolving
    // the incident, the deferred tool completion is re-driven — its output is
    // appended and the tool drains — exactly like Zeebe's retry-on-resolve.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_non_array_output_collection_process(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();
    let tool = engine
        .activate_jobs("tool", "W", 10, 1_000, 0)
        .into_iter()
        .next()
        .expect("tool job");
    let mut vars = HashMap::new();
    vars.insert("result".to_string(), Value::Str("x".to_string()));
    engine
        .apply_command(Command::complete_job_with(tool.key, vars))
        .unwrap();

    let incident = engine.active_incidents()[0].clone();
    assert_eq!(incident.element_id, "toolA", "parked on the tool child");

    // Correct the target: overwrite the scalar with an empty array in the
    // container's local scope (as an operator would via SetVariables).
    engine
        .apply_command(Command::set_variables_scoped(
            container,
            HashMap::from([("results".to_string(), Value::List(Vec::new()))]),
            true,
        ))
        .unwrap();
    // Resolve the incident → the deferred tool completion is re-driven.
    engine
        .apply_command(Command::resolve_incident(incident.key))
        .unwrap();

    assert!(
        engine.active_incidents().is_empty(),
        "the incident is cleared once the append succeeds",
    );
    // The tool's output is now appended to the corrected array.
    assert_eq!(
        container_output_collection(&engine, inst, container, "results"),
        Some(Value::List(vec![Value::Str("x".to_string())])),
        "resolving the incident retries the append against the fixed target",
    );
    // The tool has drained from the active set (it completed on retry).
    let adhoc = engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .unwrap();
    assert!(
        !adhoc.active.contains(&tool.element_instance_key),
        "the tool drains once its deferred completion is re-driven",
    );
}

#[test]
fn adhoc_agent_cancel_remaining_instances_completes_container() {
    // An agent that immediately asks to cancel/stop completes the container
    // (v1: at a turn boundary there are no in-flight tools to cancel) and lets
    // the instance flow on to its end event.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                cancel_remaining_instances: true,
                ..Default::default()
            },
        ))
        .unwrap();
    assert!(engine.is_completed(inst), "cancel completes the container");
    assert!(
        engine
            .instance(inst)
            .map(|i| i.adhoc_instances.is_empty())
            .unwrap_or(true),
        "ad-hoc runtime state torn down on completion"
    );
}

#[test]
fn plain_job_completion_ignores_absent_adhoc_result() {
    // A completion of an ordinary (non-ad-hoc) service-task job carries no
    // ad-hoc result and resumes the token normally — the ad-hoc branch must not
    // intercept it.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "order");
    let job = engine
        .activate_jobs("payment", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.instance_key == inst)
        .unwrap();
    engine
        .apply_command(Command::complete_job(job.key))
        .unwrap();
    assert!(engine.is_completed(inst));
    assert!(engine
        .instance(inst)
        .map(|i| i.adhoc_instances.is_empty())
        .unwrap_or(true));
}

// --- ADR 0023 seam 4: completion condition + tool ioMapping ---------------

fn adhoc_completion_condition_process() -> ProcessDefinition {
    // An ad-hoc container whose `<completionCondition>` ends the loop once a tool
    // sets `done = true`. Two tools are activatable so the condition firing after
    // the first completes must cancel the second.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:completionCondition>=done = true</bpmn:completionCondition>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

#[test]
fn adhoc_completion_condition_ends_the_loop_and_cancels_remaining_tools() {
    // The declared `<completionCondition>` is evaluated after each tool completes:
    // the first tool sets `done = true`, so the container completes at once —
    // without another agent turn — and cancels the still-running second tool.
    assert_eq!(
        adhoc_completion_condition_process().adhoc[0]
            .completion_condition
            .as_deref(),
        Some("=done = true"),
        "the ad-hoc container's completionCondition is parsed onto the catalog"
    );
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_completion_condition_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;

    // Turn 1: activate both tools.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA"), activate_element("toolB")],
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        2,
        "both tools active"
    );

    // Complete only toolA, returning `done = true`; the completion condition then
    // fires and the container completes, cancelling toolB.
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    let job_a = tool_jobs
        .iter()
        .find(|j| j.element_id == "toolA")
        .expect("toolA job");
    let mut vars = HashMap::new();
    vars.insert("done".to_string(), Value::Bool(true));
    vars.insert("result".to_string(), Value::Str("A".to_string()));
    let final_events = engine
        .apply_command(Command::complete_job_with(job_a.key, vars))
        .unwrap();

    assert!(
        engine.is_completed(inst),
        "completion condition ends the container after the first tool"
    );
    assert!(
        engine
            .instance(inst)
            .map(|i| i.adhoc_instances.is_empty())
            .unwrap_or(true),
        "ad-hoc runtime state torn down"
    );
    let results = final_events.iter().find_map(|e| match e {
        Event::VariablesUpdated { variables, .. } => variables.get("results").cloned(),
        _ => None,
    });
    assert_eq!(
        results,
        Some(Value::List(vec![Value::Str("A".to_string())])),
        "only the completed tool's output is collected; toolB was cancelled"
    );
}

fn adhoc_completion_condition_defer_process() -> ProcessDefinition {
    // Identical to `adhoc_completion_condition_process`, but the container carries
    // `cancelRemainingInstances="false"`: a fulfilled `<completionCondition>` must
    // NOT cancel the still-running tool — the container defers its completion until
    // that tool drains, collecting its output too.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent" cancelRemainingInstances="false">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:completionCondition>=done = true</bpmn:completionCondition>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

#[test]
fn adhoc_completion_condition_cancel_remaining_defaults_true() {
    // The BPMN `cancelRemainingInstances` attribute defaults to `true`, so a
    // container that omits it cancels remaining tools on a fulfilled condition.
    assert!(
        adhoc_completion_condition_process().adhoc[0].cancel_remaining_instances,
        "cancelRemainingInstances defaults to true when the attribute is absent"
    );
    // And an explicit `false` is parsed onto the catalog.
    assert!(
        !adhoc_completion_condition_defer_process().adhoc[0].cancel_remaining_instances,
        "cancelRemainingInstances=\"false\" is parsed onto the catalog"
    );
}

#[test]
fn adhoc_completion_condition_defers_when_cancel_remaining_is_false() {
    // The declared `<completionCondition>` fires after toolA sets `done = true`,
    // but `cancelRemainingInstances="false"` defers container completion: toolB
    // keeps running, and the container completes only once toolB drains — with
    // BOTH tools' outputs collected (nothing cancelled).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_completion_condition_defer_process(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;

    // Turn 1: activate both tools.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA"), activate_element("toolB")],
                ..Default::default()
            },
        ))
        .unwrap();

    // Complete toolA with `done = true`: the completion condition fires, but with
    // cancelRemainingInstances=false the container defers — it stays active with
    // toolB still running and the fulfilment latched.
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 60_000, 0);
    let job_a = tool_jobs
        .iter()
        .find(|j| j.element_id == "toolA")
        .expect("toolA job")
        .clone();
    let job_b = tool_jobs
        .iter()
        .find(|j| j.element_id == "toolB")
        .expect("toolB job")
        .clone();
    let mut vars_a = HashMap::new();
    vars_a.insert("done".to_string(), Value::Bool(true));
    vars_a.insert("result".to_string(), Value::Str("A".to_string()));
    engine
        .apply_command(Command::complete_job_with(job_a.key, vars_a))
        .unwrap();

    assert!(
        !engine.is_completed(inst),
        "the container defers completion while toolB is still running"
    );
    let adhoc = engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .expect("ad-hoc container still active (deferred)")
        .clone();
    assert_eq!(
        adhoc.active.len(),
        1,
        "toolB is still active (not cancelled)"
    );
    assert!(
        adhoc.completion_condition_fulfilled,
        "the fulfilled completion condition is latched while deferring"
    );

    // Complete toolB: no active children remain, so the deferred container now
    // completes — collecting toolB's output as well.
    let mut vars_b = HashMap::new();
    vars_b.insert("result".to_string(), Value::Str("B".to_string()));
    let final_events = engine
        .apply_command(Command::complete_job_with(job_b.key, vars_b))
        .unwrap();

    assert!(
        engine.is_completed(inst),
        "the container completes once its last outstanding tool drains"
    );
    assert!(
        engine
            .instance(inst)
            .map(|i| i.adhoc_instances.is_empty())
            .unwrap_or(true),
        "ad-hoc runtime state torn down after deferred completion"
    );
    let results = final_events.iter().find_map(|e| match e {
        Event::VariablesUpdated { variables, .. } => variables.get("results").cloned(),
        _ => None,
    });
    assert_eq!(
        results,
        Some(Value::List(vec![
            Value::Str("A".to_string()),
            Value::Str("B".to_string()),
        ])),
        "both tools' outputs are collected; the deferred toolB was not cancelled"
    );
}

fn adhoc_tool_io_process() -> ProcessDefinition {
    // A container whose tool declares a `zeebe:ioMapping`: an input mapping
    // (`=base + 1` → `n`, local to the tool) and an output mapping
    // (`=result` → `status`, projected into the container scope). The container's
    // completionCondition reads that projected `status`.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:completionCondition>=status = "ok"</bpmn:completionCondition>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
                <zeebe:ioMapping>
                  <zeebe:input source="=base + 1" target="n" />
                  <zeebe:output source="=result" target="status" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

#[test]
fn adhoc_tool_io_mapping_applies_inputs_on_activation_and_outputs_on_completion() {
    let proc = adhoc_tool_io_process();
    let tool = proc.adhoc[0]
        .tools
        .iter()
        .find(|t| t.element_id == "toolA")
        .unwrap();
    assert_eq!(
        tool.io.inputs.len(),
        1,
        "tool input mapping retained on catalog"
    );
    assert_eq!(
        tool.io.outputs.len(),
        1,
        "tool output mapping retained on catalog"
    );

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(proc)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "p",
            HashMap::from([("base".to_string(), Value::Int(1))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;

    // Turn 1: activate toolA. Its input mapping `=base + 1` should resolve `n = 2`
    // local to the tool's own scope.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();
    let child = *engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .unwrap()
        .active
        .iter()
        .next()
        .expect("toolA active");
    let local = engine
        .instance(inst)
        .unwrap()
        .scope_variables
        .get(&child)
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        local.get("n"),
        Some(&Value::Int(2)),
        "input mapping evaluated into the tool's local scope"
    );

    // Complete toolA returning `result = "ok"`; the output mapping projects
    // `status = "ok"` into the container scope, which satisfies the completion
    // condition and ends the container.
    let job = engine
        .activate_jobs("tool", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolA")
        .unwrap();
    engine
        .apply_command(Command::complete_job_with(
            job.key,
            HashMap::from([("result".to_string(), Value::Str("ok".to_string()))]),
        ))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "output-mapped `status` satisfies the completion condition"
    );
}

#[test]
fn adhoc_agent_completion_flag_with_activations_is_rejected() {
    // Zeebe parity (#614 gap 4): asserting `isCompletionConditionFulfilled`
    // while ALSO returning activate-element instructions is contradictory. The
    // JOB_WORKER completion path rejects it with INVALID_ARGUMENT
    // (JobCompleteProcessor.checkAdHocSubProcessCompletionConditionNotFulfilled
    // ForElementActivation). nano previously silently superseded the
    // activations and completed the container; it now rejects the command so
    // the agent must resubmit a coherent turn.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;
    let err = engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::AdHocActivateWithCompletion { .. }),
        "completion flag + activations is rejected (INVALID_ARGUMENT parity), got {err:?}"
    );
    assert!(
        !engine.is_completed(inst),
        "the rejected command leaves the container running"
    );
    assert!(
        engine.activate_jobs("tool", "W", 10, 1_000, 0).is_empty(),
        "no tool was activated by the rejected turn"
    );
    let _ = container;
}

#[test]
fn adhoc_agent_completion_flag_alone_completes_and_cancels() {
    // The `isCompletionConditionFulfilled` flag on its own (no activations) ends
    // the loop: the container completes at once and any in-flight tool is
    // cancelled. This is the non-contradictory sibling of the rejection test
    // above.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "completion flag ends the container"
    );
    assert!(
        engine
            .instance(inst)
            .map(|i| i.adhoc_instances.is_empty())
            .unwrap_or(true),
        "runtime state torn down; no tool left active"
    );
    assert!(
        engine.activate_jobs("tool", "W", 10, 1_000, 0).is_empty(),
        "no tool job was created"
    );
    let _ = container;
}

// --- ADR 0023 v1.1: declarative (BPMN_TASK) ad-hoc sub-process --------------

fn adhoc_declarative_process() -> ProcessDefinition {
    // A declarative ad-hoc container: no `zeebe:taskDefinition` (so it is NOT a
    // job worker), only a `zeebe:adHoc activeElementsCollection` FEEL expression
    // naming the inner elements to activate. Each tool captures its `result` into
    // the container's `outputCollection` ("results").
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="sub">
            <bpmn:extensionElements>
              <zeebe:adHoc activeElementsCollection="=tools" outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="sub" />
          <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn create_instance_with_vars(
    engine: &mut Engine,
    process_id: &str,
    variables: HashMap<String, Value>,
) -> Key {
    engine
        .apply_command(Command::create_instance_with(process_id, variables))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap()
}

#[test]
fn adhoc_declarative_activates_collection_elements_without_a_job() {
    // The defect class: a declarative (BPMN_TASK) ad-hoc container is parsed but
    // never executed — on `main` it is minted as a plain job (job type = its id)
    // and its `activeElementsCollection` is ignored, so no inner element runs.
    // On the branch it evaluates the collection and activates those elements
    // directly, minting no container job.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_declarative_process()))
        .unwrap();
    let inst = create_instance_with_vars(
        &mut engine,
        "p",
        HashMap::from([(
            "tools".to_string(),
            Value::List(vec![
                Value::Str("toolA".to_string()),
                Value::Str("toolB".to_string()),
            ]),
        )]),
    );

    // No job is minted for the container itself (it is declarative, not a job
    // worker). On `main` a "sub" job would exist here.
    assert!(
        engine.activate_jobs("sub", "W", 10, 1_000, 0).is_empty(),
        "a declarative ad-hoc container mints no job"
    );

    // Both collection elements activated immediately as real element instances
    // inside the container scope, each parked on its own `tool` job.
    let container = *engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .keys()
        .next()
        .expect("container runtime state registered");
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        2,
        "both named elements active with no agent turn"
    );
    assert!(
        !engine.is_completed(inst),
        "container parks while tools run"
    );

    // Drain the two tool jobs, each producing a `result`. When the last drains,
    // the declarative container completes (no agent job re-emitted), writes its
    // `outputCollection`, and takes its outgoing flow to the end event.
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool_jobs.len(), 2, "both named tools produced jobs");
    let mut final_events = Vec::new();
    for (job, val) in tool_jobs.iter().zip(["A", "B"]) {
        let mut vars = HashMap::new();
        vars.insert("result".to_string(), Value::Str(val.to_string()));
        final_events = engine
            .apply_command(Command::complete_job_with(job.key, vars))
            .unwrap();
    }
    assert!(
        engine.is_completed(inst),
        "container completes once its collection drains → instance done"
    );
    // No agent job is ever re-emitted for a declarative container.
    assert!(
        engine.activate_jobs("sub", "W", 10, 1_000, 0).is_empty(),
        "declarative container never re-emits a container job"
    );
    let results = final_events.iter().find_map(|e| match e {
        Event::VariablesUpdated { variables, .. } => variables.get("results").cloned(),
        _ => None,
    });
    let mut got = match results {
        Some(Value::List(v)) => v,
        other => panic!("expected outputCollection list, got {other:?}"),
    };
    got.sort_by_key(|v| match v {
        Value::Str(s) => s.clone(),
        _ => String::new(),
    });
    assert_eq!(
        got,
        vec![Value::Str("A".to_string()), Value::Str("B".to_string())],
        "outputCollection holds both tool results"
    );
}

#[test]
fn adhoc_declarative_empty_collection_completes_immediately() {
    // A declarative container whose collection evaluates to an empty list has
    // nothing to run and completes at once, flowing on to the end event — it must
    // not park forever waiting on a non-existent job.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_declarative_process()))
        .unwrap();
    let inst = create_instance_with_vars(
        &mut engine,
        "p",
        HashMap::from([("tools".to_string(), Value::List(vec![]))]),
    );
    assert!(
        engine.is_completed(inst),
        "an empty active-elements collection completes the container immediately"
    );
    assert!(
        engine.activate_jobs("tool", "W", 10, 1_000, 0).is_empty(),
        "no tool was activated"
    );
    assert!(
        engine.activate_jobs("sub", "W", 10, 1_000, 0).is_empty(),
        "no container job was minted"
    );
}

#[test]
fn adhoc_declarative_ignores_unknown_collection_ids() {
    // An id in the collection that is not one of the container's inner elements
    // is not activatable (nano prunes inner tools from the executable graph), so
    // it is dropped rather than minting a phantom child. Only the real element
    // runs. (Raising the Camunda NOT_FOUND incident is a separate validation
    // gap.)
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_declarative_process()))
        .unwrap();
    let inst = create_instance_with_vars(
        &mut engine,
        "p",
        HashMap::from([(
            "tools".to_string(),
            Value::List(vec![
                Value::Str("toolA".to_string()),
                Value::Str("ghost".to_string()),
            ]),
        )]),
    );
    let container = *engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .keys()
        .next()
        .expect("container runtime state registered");
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        1,
        "only the real element activated; the unknown id was dropped"
    );
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool_jobs.len(), 1, "exactly one tool ran");
    engine
        .apply_command(Command::complete_job(tool_jobs[0].key))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "container completes after its one tool"
    );
}

// --- Execution listeners (ADR 0037) -----------------------------------------

use crate::model::{ExecutionListener, ListenerEventType};

fn el(event_type: ListenerEventType, job_type: &str) -> ExecutionListener {
    ExecutionListener {
        event_type,
        job_type: job_type.to_string(),
        retries: None,
    }
}

/// A linear process whose single service task `charge` (job `payment`) carries
/// the given start/end execution listeners.
fn task_with_listeners(
    start: Vec<ExecutionListener>,
    end: Vec<ExecutionListener>,
) -> ProcessDefinition {
    ProcessBuilder::new("order")
        .start_event("start")
        .service_task("charge", "payment")
        .end_event("end")
        .connect("start", "charge")
        .connect("charge", "end")
        .with_listeners("charge", start, end)
        .build()
        .unwrap()
}

fn kinds(events: &[Event]) -> Vec<&'static str> {
    events
        .iter()
        .map(|e| match e {
            Event::ElementActivating { .. } => "Activating",
            Event::ElementActivated { .. } => "Activated",
            Event::ElementCompleting { .. } => "Completing",
            Event::ElementCompleted { .. } => "Completed",
            Event::JobCreated { .. } => "JobCreated",
            Event::ExecutionListenerJobCreated { .. } => "ListenerJobCreated",
            Event::JobCompleted { .. } => "JobCompleted",
            Event::JobActivated { .. } => "JobActivated",
            Event::SequenceFlowTaken { .. } => "FlowTaken",
            Event::ProcessInstanceCompleted { .. } => "InstanceCompleted",
            _ => "_",
        })
        .collect()
}

#[test]
fn start_listener_runs_before_the_service_job_is_created() {
    // A `start` execution listener defers the element's own behaviour (job
    // creation): the listener job is minted at ACTIVATED time; the service job
    // only appears once the listener chain drains.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_listeners(
            vec![el(ListenerEventType::Start, "audit")],
            Vec::new(),
        )))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();

    // No service `payment` job yet — the element rests in ACTIVATING.
    assert!(
        engine
            .activate_jobs("payment", "W", 10, 1_000, 0)
            .is_empty(),
        "service job must not exist until the start listener completes"
    );
    // The start-listener job is activatable.
    let listener = engine.activate_jobs("audit", "W", 10, 1_000, 0);
    assert_eq!(listener.len(), 1, "one start-listener job");

    // Completing it drains the chain → the service job is created.
    engine
        .apply_command(Command::complete_job(listener[0].key))
        .unwrap();
    let payment = engine.activate_jobs("payment", "W", 10, 1_000, 0);
    assert_eq!(payment.len(), 1, "service job created after start listener");
}

#[test]
fn end_listener_runs_before_the_element_completes() {
    // An `end` execution listener defers ElementCompleted + the outgoing flow:
    // the element rests in COMPLETING while the listener runs, so the instance
    // is not yet finished.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_listeners(
            Vec::new(),
            vec![el(ListenerEventType::End, "audit")],
        )))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let payment = engine.activate_jobs("payment", "W", 10, 1_000, 0);
    let completing = engine
        .apply_command(Command::complete_job(payment[0].key))
        .unwrap();
    // Completing the service job emits Completing + the end-listener job, but
    // NOT ElementCompleted, and the instance is still running.
    assert!(kinds(&completing).contains(&"Completing"));
    assert!(!kinds(&completing).contains(&"Completed"));
    assert!(kinds(&completing).contains(&"ListenerJobCreated"));
    assert!(
        !engine.is_completed(inst),
        "instance parked on end listener"
    );

    // Completing the end listener finalises completion → instance done.
    let listener = engine.activate_jobs("audit", "W", 10, 1_000, 0);
    assert_eq!(listener.len(), 1, "one end-listener job");
    let done = engine
        .apply_command(Command::complete_job(listener[0].key))
        .unwrap();
    assert!(kinds(&done).contains(&"Completed"));
    assert!(
        engine.is_completed(inst),
        "instance finished after end listener"
    );
}

#[test]
fn multiple_start_listeners_run_sequentially_in_declaration_order() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_listeners(
            vec![
                el(ListenerEventType::Start, "first"),
                el(ListenerEventType::Start, "second"),
            ],
            Vec::new(),
        )))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();

    // Only the first listener is activatable; the second does not exist yet.
    assert!(engine.activate_jobs("second", "W", 10, 1_000, 0).is_empty());
    let first = engine.activate_jobs("first", "W", 10, 1_000, 0);
    assert_eq!(first.len(), 1);
    engine
        .apply_command(Command::complete_job(first[0].key))
        .unwrap();

    // Now the second listener exists; the service job still does not.
    assert!(engine
        .activate_jobs("payment", "W", 10, 1_000, 0)
        .is_empty());
    let second = engine.activate_jobs("second", "W", 10, 1_000, 0);
    assert_eq!(second.len(), 1);
    engine
        .apply_command(Command::complete_job(second[0].key))
        .unwrap();

    // Chain drained → service job created.
    assert_eq!(engine.activate_jobs("payment", "W", 10, 1_000, 0).len(), 1);
}

#[test]
fn listener_variables_merge_into_the_element_scope() {
    // A start listener that returns a variable makes it visible to the service
    // job that follows (Zeebe parity: listener completions merge forward).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_listeners(
            vec![el(ListenerEventType::Start, "audit")],
            Vec::new(),
        )))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let listener = engine.activate_jobs("audit", "W", 10, 1_000, 0);
    let mut vars = HashMap::new();
    vars.insert("approved".to_string(), Value::Bool(true));
    engine
        .apply_command(Command::complete_job_with(listener[0].key, vars))
        .unwrap();
    let payment = engine.activate_jobs("payment", "W", 10, 1_000, 0);
    assert_eq!(
        payment[0].variables.get("approved"),
        Some(&Value::Bool(true)),
        "start-listener variable is visible to the service job"
    );
}

#[test]
fn listener_free_model_journal_is_byte_identical() {
    // The critical invariant: a model with NO listeners must emit exactly the
    // same events as before execution-listener support existed. We assert the
    // full lifecycle event shape of a plain service task.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_listeners(
            Vec::new(),
            Vec::new(),
        )))
        .unwrap();
    let create = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    // Activation of the service task: Activating, Activated, JobCreated — no
    // listener events interleaved.
    assert_eq!(
        kinds(&create),
        vec![
            "_",
            "Activating",
            "Activated",
            "Completing",
            "Completed",
            "FlowTaken",
            "Activating",
            "Activated",
            "JobCreated"
        ],
        "listener-free activation journal unchanged"
    );
    let payment = engine.activate_jobs("payment", "W", 10, 1_000, 0);
    let done = engine
        .apply_command(Command::complete_job(payment[0].key))
        .unwrap();
    assert_eq!(
        kinds(&done),
        vec![
            "JobCompleted",
            "Completing",
            "Completed",
            "FlowTaken",
            "Activating",
            "Activated",
            "Completing",
            "Completed",
            "InstanceCompleted"
        ],
        "listener-free completion journal unchanged"
    );
}

#[test]
fn start_and_end_listeners_bracket_the_service_task() {
    // Full lifecycle: start listener → service job → end listener.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_listeners(
            vec![el(ListenerEventType::Start, "before")],
            vec![el(ListenerEventType::End, "after")],
        )))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let before = engine.activate_jobs("before", "W", 10, 1_000, 0);
    assert_eq!(before.len(), 1, "start listener first");
    engine
        .apply_command(Command::complete_job(before[0].key))
        .unwrap();

    let payment = engine.activate_jobs("payment", "W", 10, 1_000, 0);
    assert_eq!(payment.len(), 1, "then the service job");
    engine
        .apply_command(Command::complete_job(payment[0].key))
        .unwrap();
    assert!(!engine.is_completed(inst));

    let after = engine.activate_jobs("after", "W", 10, 1_000, 0);
    assert_eq!(after.len(), 1, "then the end listener");
    engine
        .apply_command(Command::complete_job(after[0].key))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "instance finished after end listener"
    );
}

#[test]
fn end_listener_fires_on_an_exclusive_gateway() {
    // An `end` execution listener on an exclusive gateway defers the routing
    // decision: the gateway rests in COMPLETING while the listener runs, and the
    // outgoing flow is only taken once the chain drains (ADR 0037). This closes a
    // real gap — the gateway previously short-circuited before the end gate.
    let process = ProcessBuilder::new("route")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("yes")
        .end_event("no")
        .connect("s", "g")
        .connect_when("g", "yes", "=go")
        .connect_default("g", "no")
        .with_listeners(
            "g",
            Vec::new(),
            vec![el(ListenerEventType::End, "gate-audit")],
        )
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process))
        .unwrap();
    let create = engine
        .apply_command(Command::create_instance_with(
            "route",
            vars(&[("go", Value::Bool(true))]),
        ))
        .unwrap();
    let inst = create.iter().find_map(|e| e.instance_key()).unwrap();

    // The gateway parked on its end listener: no routing flow taken yet.
    assert!(
        !create.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "yes" || to == "no"
        )),
        "gateway must not route until its end listener completes"
    );
    assert!(kinds(&create).contains(&"ListenerJobCreated"));
    assert!(
        !engine.is_completed(inst),
        "instance parked on gateway end listener"
    );

    // Completing the listener drains the chain → the conditional flow is taken.
    let audit = engine.activate_jobs("gate-audit", "W", 10, 1_000, 0);
    assert_eq!(audit.len(), 1, "one gateway end-listener job");
    let done = engine
        .apply_command(Command::complete_job(audit[0].key))
        .unwrap();
    assert!(
        done.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "yes"
        )),
        "conditional flow taken after the end listener"
    );
    assert!(engine.is_completed(inst));
}

#[test]
fn end_listener_fires_on_an_embedded_subprocess() {
    // An `end` execution listener on an embedded sub-process defers the
    // sub-process's completion (and its outgoing flow) until the listener runs.
    let process = ProcessBuilder::new("wrap")
        .start_event("s")
        .sub_process("sub", "inner-start")
        .start_event("inner-start")
        .service_task("work", "job")
        .end_event("inner-end")
        .end_event("e")
        .contained_in("inner-start", "sub")
        .contained_in("work", "sub")
        .contained_in("inner-end", "sub")
        .connect("s", "sub")
        .connect("inner-start", "work")
        .connect("work", "inner-end")
        .connect("sub", "e")
        .with_listeners(
            "sub",
            Vec::new(),
            vec![el(ListenerEventType::End, "sub-audit")],
        )
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("wrap"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let work = engine.activate_jobs("job", "W", 10, 1_000, 0);
    assert_eq!(work.len(), 1);
    engine
        .apply_command(Command::complete_job(work[0].key))
        .unwrap();

    // Inner flow drained; the sub-process parks on its end listener.
    assert!(
        !engine.is_completed(inst),
        "sub-process parked on end listener"
    );
    let audit = engine.activate_jobs("sub-audit", "W", 10, 1_000, 0);
    assert_eq!(audit.len(), 1, "one sub-process end-listener job");
    engine
        .apply_command(Command::complete_job(audit[0].key))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "instance done after the sub-process end listener"
    );
}

/// A parallel multi-instance service task `each` (job `handle`) over `items`,
/// carrying the given start/end execution listeners on the multi-instance body.
fn mi_body_with_listeners(
    start: Vec<ExecutionListener>,
    end: Vec<ExecutionListener>,
) -> ProcessDefinition {
    ProcessBuilder::new("mi")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                sequential: false,
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .with_listeners("each", start, end)
        .build()
        .unwrap()
}

#[test]
fn start_listener_fires_once_on_a_multi_instance_body_before_children() {
    // A `start` execution listener on a multi-instance activity fires ONCE on the
    // body, before ANY child is instantiated (ADR 0037). It does not double-fire
    // per child.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(mi_body_with_listeners(
            vec![el(ListenerEventType::Start, "mi-start")],
            Vec::new(),
        )))
        .unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap();

    // No child jobs until the body's start listener drains.
    assert!(
        engine.activate_jobs("handle", "W", 10, 1_000, 0).is_empty(),
        "children must not spawn until the body start listener completes"
    );
    let start = engine.activate_jobs("mi-start", "W", 10, 1_000, 0);
    assert_eq!(start.len(), 1, "exactly one body start-listener job");
    engine
        .apply_command(Command::complete_job(start[0].key))
        .unwrap();

    // Now all three children spawn, and the start listener does NOT re-fire.
    assert_eq!(
        engine.activate_jobs("handle", "W", 10, 1_000, 0).len(),
        3,
        "all children spawn after the body start listener"
    );
    assert!(
        engine
            .activate_jobs("mi-start", "W", 10, 1_000, 0)
            .is_empty(),
        "start listener must not re-fire per child"
    );
}

#[test]
fn end_listener_fires_once_on_a_multi_instance_body_after_all_children() {
    // An `end` execution listener on a multi-instance activity fires ONCE on the
    // body, only after every child has completed, before the body itself
    // completes.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(mi_body_with_listeners(
            Vec::new(),
            vec![el(ListenerEventType::End, "mi-end")],
        )))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let handle = engine.activate_jobs("handle", "W", 10, 1_000, 0);
    assert_eq!(handle.len(), 3);
    // Completing the first two children does NOT yet mint the body end listener.
    for j in &handle[..2] {
        engine.apply_command(Command::complete_job(j.key)).unwrap();
    }
    assert!(
        engine.activate_jobs("mi-end", "W", 10, 1_000, 0).is_empty(),
        "body end listener must wait for all children"
    );
    // Completing the last child parks the body on its single end listener.
    engine
        .apply_command(Command::complete_job(handle[2].key))
        .unwrap();
    assert!(
        !engine.is_completed(inst),
        "body parked on its end listener"
    );
    let end = engine.activate_jobs("mi-end", "W", 10, 1_000, 0);
    assert_eq!(end.len(), 1, "exactly one body end-listener job");
    engine
        .apply_command(Command::complete_job(end[0].key))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "instance done after the body end listener"
    );
}

#[test]
fn end_listener_fires_on_an_adhoc_container() {
    // An `end` execution listener on an ad-hoc sub-process container defers the
    // container's completion until the listener runs (natural, non-cancel path).
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc />
              <zeebe:executionListeners>
                <zeebe:executionListener eventType="end" type="agent-audit" />
              </zeebe:executionListeners>
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let process = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("p"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Drive the ad-hoc worker: activate one tool, then complete with no further
    // activations so the container drains naturally.
    let agent = engine.activate_jobs("agent-worker", "W", 10, 1_000, 0);
    assert_eq!(agent.len(), 1, "ad-hoc container job");
    engine
        .apply_command(Command::complete_job_with_result(
            agent[0].key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();
    let tool = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool.len(), 1, "activated tool job");
    engine
        .apply_command(Command::complete_job(tool[0].key))
        .unwrap();

    // The tool drained; the agent re-emits for a final turn. Returning NO further
    // activations drives the container down its natural (non-cancel) completion
    // path, where it parks on its end listener before completing.
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job re-emitted");
    engine
        .apply_command(Command::complete_job_with_result(
            agent2.key,
            HashMap::new(),
            crate::model::AdHocJobResult::default(),
        ))
        .unwrap();

    // The ad-hoc container parks on its end listener before completing.
    assert!(
        !engine.is_completed(inst),
        "container parked on end listener"
    );
    let audit = engine.activate_jobs("agent-audit", "W", 10, 1_000, 0);
    assert_eq!(audit.len(), 1, "one ad-hoc container end-listener job");
    engine
        .apply_command(Command::complete_job(audit[0].key))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "instance done after the ad-hoc container end listener"
    );
}

#[test]
fn exclusive_gateway_end_listener_reselects_and_raises_incident_if_nothing_matches() {
    // A gateway's `end` listener runs BEFORE the routing decision is finalised, so
    // a listener that rewrites a condition variable is observed by the
    // re-selection. If the rewrite makes NOTHING match (and there is no default),
    // the gateway must raise a no-matching-flow incident and keep its token — not
    // silently complete and drop the token (which would falsely finish the
    // instance).
    let process = ProcessBuilder::new("route")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("yes")
        .connect("s", "g")
        .connect_when("g", "yes", "=go")
        .with_listeners(
            "g",
            Vec::new(),
            vec![el(ListenerEventType::End, "gate-audit")],
        )
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process))
        .unwrap();
    // `go=true` at creation → the flow to "yes" is selectable, so the gateway
    // parks on its end listener.
    let inst = engine
        .apply_command(Command::create_instance_with(
            "route",
            vars(&[("go", Value::Bool(true))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let audit = engine.activate_jobs("gate-audit", "W", 10, 1_000, 0);
    assert_eq!(audit.len(), 1);

    // The listener completes, flipping `go` to false — now nothing matches on
    // re-selection.
    let done = engine
        .apply_command(Command::complete_job_with(
            audit[0].key,
            vars(&[("go", Value::Bool(false))]),
        ))
        .unwrap();
    assert!(
        done.iter().any(|e| matches!(
            e,
            Event::IncidentRaised {
                kind: state::IncidentKind::NoMatchingSequenceFlow,
                ..
            }
        )),
        "re-selection matched nothing → no-matching-flow incident"
    );
    assert!(
        !done.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "yes"
        )),
        "no flow taken when re-selection fails"
    );
    assert!(
        !engine.is_completed(inst),
        "instance parked on the gateway incident, not falsely completed"
    );
}

// --- Task listeners (ADR 0037 §6) -------------------------------------------

use crate::model::{
    TaskListener, TaskListenerEventType, TaskListenerJobResult, UserTaskCorrections,
};

fn tl(event_type: TaskListenerEventType, job_type: &str) -> TaskListener {
    TaskListener {
        event_type,
        job_type: job_type.to_string(),
        retries: None,
    }
}

/// `start -> review (user task, with the given task listeners) -> end`.
fn user_task_with_listeners(listeners: Vec<TaskListener>) -> ProcessDefinition {
    ProcessBuilder::new("approval")
        .start_event("start")
        .user_task("review")
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .with_task_listeners("review", listeners)
        .build()
        .unwrap()
}

/// Deploys `def`, creates an instance and returns `(engine, instance_key)`.
fn deploy_and_start(def: ProcessDefinition) -> (Engine, Key) {
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("approval"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    (engine, instance_key)
}

fn only_user_task_key(engine: &Engine) -> Key {
    let keys: Vec<Key> = engine.state().user_tasks.keys().copied().collect();
    assert_eq!(keys.len(), 1, "expected exactly one user task");
    keys[0]
}

#[test]
fn creating_listener_gates_the_user_task_before_it_is_available() {
    // A `creating` listener runs while the element is ACTIVATING: the user task
    // is not completable until the chain drains.
    let (mut engine, _inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Creating,
        "onCreate",
    )]));

    // The task is not yet `Created`: assigning it is rejected.
    let key = only_user_task_key(&engine);
    assert!(
        engine
            .apply_command(Command::assign_user_task(key, "alice"))
            .is_err(),
        "task must not accept an assignment while the creating listener runs"
    );

    // The creating-listener job is activatable; completing it drains the chain
    // and the task becomes `Created`.
    let jobs = engine.activate_jobs("onCreate", "W", 10, 1_000, 0);
    assert_eq!(jobs.len(), 1, "one creating-listener job");
    engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Created
    );
    engine
        .apply_command(Command::assign_user_task(key, "alice"))
        .unwrap();
}

#[test]
fn assigning_listener_defers_the_assignment_until_it_completes() {
    let (mut engine, _inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Assigning,
        "onAssign",
    )]));
    let key = only_user_task_key(&engine);

    // Assign defers behind the listener: the assignee is not yet set.
    engine
        .apply_command(Command::assign_user_task(key, "alice"))
        .unwrap();
    assert_eq!(engine.state().user_tasks[&key].assignee, None);

    let jobs = engine.activate_jobs("onAssign", "W", 10, 1_000, 0);
    assert_eq!(jobs.len(), 1);
    engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&key].assignee.as_deref(),
        Some("alice")
    );
}

#[test]
fn assigning_listener_can_deny_the_assignment() {
    let (mut engine, _inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Assigning,
        "onAssign",
    )]));
    let key = only_user_task_key(&engine);
    engine
        .apply_command(Command::assign_user_task(key, "alice"))
        .unwrap();
    let jobs = engine.activate_jobs("onAssign", "W", 10, 1_000, 0);
    engine
        .apply_command(Command::complete_job_with_task_result(
            jobs[0].key,
            TaskListenerJobResult {
                denied: true,
                denied_reason: Some("not allowed".into()),
                corrections: UserTaskCorrections::default(),
            },
        ))
        .unwrap();
    // Denied: the assignee stays unset and the task is assignable again.
    assert_eq!(engine.state().user_tasks[&key].assignee, None);
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Created
    );
    engine
        .apply_command(Command::assign_user_task(key, "bob"))
        .unwrap();
}

#[test]
fn assigning_listener_can_correct_the_assignee() {
    let (mut engine, _inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Assigning,
        "onAssign",
    )]));
    let key = only_user_task_key(&engine);
    engine
        .apply_command(Command::assign_user_task(key, "alice"))
        .unwrap();
    let jobs = engine.activate_jobs("onAssign", "W", 10, 1_000, 0);
    engine
        .apply_command(Command::complete_job_with_task_result(
            jobs[0].key,
            TaskListenerJobResult {
                denied: false,
                denied_reason: None,
                corrections: UserTaskCorrections {
                    assignee: Some("carol".into()),
                    ..Default::default()
                },
            },
        ))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&key].assignee.as_deref(),
        Some("carol")
    );
}

#[test]
fn updating_listener_defers_and_can_deny() {
    use crate::UserTaskChangeset;
    let (mut engine, _inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Updating,
        "onUpdate",
    )]));
    let key = only_user_task_key(&engine);

    let changeset = UserTaskChangeset {
        priority: Some(80),
        ..Default::default()
    };
    engine
        .apply_command(Command::update_user_task(key, changeset))
        .unwrap();
    // Deferred: priority not yet applied.
    assert_eq!(engine.state().user_tasks[&key].priority, 50);

    let jobs = engine.activate_jobs("onUpdate", "W", 10, 1_000, 0);
    engine
        .apply_command(Command::complete_job_with_task_result(
            jobs[0].key,
            TaskListenerJobResult {
                denied: true,
                denied_reason: Some("no".into()),
                corrections: UserTaskCorrections::default(),
            },
        ))
        .unwrap();
    // Denied: priority unchanged.
    assert_eq!(engine.state().user_tasks[&key].priority, 50);
}

#[test]
fn completing_listener_defers_completion_and_can_deny() {
    let (mut engine, inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Completing,
        "onComplete",
    )]));
    let key = only_user_task_key(&engine);

    engine
        .apply_command(Command::complete_user_task(key))
        .unwrap();
    // Deferred: not completed, instance still running.
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Created
    );
    assert!(!engine.is_completed(inst));

    let jobs = engine.activate_jobs("onComplete", "W", 10, 1_000, 0);
    assert_eq!(jobs.len(), 1);
    // Deny returns the task to Created; the instance keeps running.
    engine
        .apply_command(Command::complete_job_with_task_result(
            jobs[0].key,
            TaskListenerJobResult {
                denied: true,
                denied_reason: Some("blocked".into()),
                corrections: UserTaskCorrections::default(),
            },
        ))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Created
    );
    assert!(!engine.is_completed(inst));

    // A second completion now succeeds through the listener.
    engine
        .apply_command(Command::complete_user_task(key))
        .unwrap();
    let jobs = engine.activate_jobs("onComplete", "W", 10, 1_000, 0);
    engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Completed
    );
    assert!(engine.is_completed(inst));
}

#[test]
fn completing_listeners_run_sequentially_in_declaration_order() {
    let (mut engine, inst) = deploy_and_start(user_task_with_listeners(vec![
        tl(TaskListenerEventType::Completing, "first"),
        tl(TaskListenerEventType::Completing, "second"),
    ]));
    let key = only_user_task_key(&engine);
    engine
        .apply_command(Command::complete_user_task(key))
        .unwrap();

    // Only the first listener's job exists initially.
    assert!(engine.activate_jobs("second", "W", 10, 1_000, 0).is_empty());
    let first = engine.activate_jobs("first", "W", 10, 1_000, 0);
    assert_eq!(first.len(), 1);
    engine
        .apply_command(Command::complete_job(first[0].key))
        .unwrap();

    // Now the second appears; completing it drains the chain.
    let second = engine.activate_jobs("second", "W", 10, 1_000, 0);
    assert_eq!(second.len(), 1);
    engine
        .apply_command(Command::complete_job(second[0].key))
        .unwrap();
    assert!(engine.is_completed(inst));
}

#[test]
fn canceling_listener_runs_before_the_instance_terminates() {
    let (mut engine, inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Canceling,
        "onCancel",
    )]));
    let key = only_user_task_key(&engine);

    engine
        .apply_command(Command::cancel_instance(inst))
        .unwrap();
    // Deferred termination: the task is not yet canceled and the instance is
    // not yet terminated.
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Created
    );
    assert_eq!(
        engine.state().instances[&inst].state,
        state::ProcessInstanceState::Terminating
    );

    let jobs = engine.activate_jobs("onCancel", "W", 10, 1_000, 0);
    assert_eq!(jobs.len(), 1, "one canceling-listener job");
    engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();
    // The chain drained: task canceled and the instance is gone.
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Canceled
    );
    assert_eq!(
        engine.state().instances[&inst].state,
        state::ProcessInstanceState::Terminated
    );
}

#[test]
fn task_listener_job_may_not_carry_variables() {
    let (mut engine, _inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Completing,
        "onComplete",
    )]));
    let key = only_user_task_key(&engine);
    engine
        .apply_command(Command::complete_user_task(key))
        .unwrap();
    let jobs = engine.activate_jobs("onComplete", "W", 10, 1_000, 0);
    let vars = HashMap::from([("x".to_string(), Value::Bool(true))]);
    assert!(matches!(
        engine.apply_command(Command::complete_job_with(jobs[0].key, vars)),
        Err(EngineError::TaskListenerJobWithVariables { .. })
    ));
}

#[test]
fn task_listener_deny_may_not_carry_corrections() {
    let (mut engine, _inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Completing,
        "onComplete",
    )]));
    let key = only_user_task_key(&engine);
    engine
        .apply_command(Command::complete_user_task(key))
        .unwrap();
    let jobs = engine.activate_jobs("onComplete", "W", 10, 1_000, 0);
    assert!(matches!(
        engine.apply_command(Command::complete_job_with_task_result(
            jobs[0].key,
            TaskListenerJobResult {
                denied: true,
                denied_reason: Some("x".into()),
                corrections: UserTaskCorrections {
                    priority: Some(10),
                    ..Default::default()
                },
            },
        )),
        Err(EngineError::TaskListenerDenyWithCorrections { .. })
    ));
}

#[test]
fn creating_and_canceling_listeners_may_not_deny() {
    let (mut engine, _inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Creating,
        "onCreate",
    )]));
    let key = only_user_task_key(&engine);
    // The creating listener job exists while the task is ACTIVATING.
    let _ = key;
    let jobs = engine.activate_jobs("onCreate", "W", 10, 1_000, 0);
    assert!(matches!(
        engine.apply_command(Command::complete_job_with_task_result(
            jobs[0].key,
            TaskListenerJobResult {
                denied: true,
                denied_reason: Some("x".into()),
                corrections: UserTaskCorrections::default(),
            },
        )),
        Err(EngineError::TaskListenerDenyNotSupported { .. })
    ));
}

#[test]
fn user_task_without_listeners_is_byte_identical() {
    // A user task carrying no listeners must produce the exact same journal as
    // the pre-task-listener engine: assign, then complete, straight through.
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task("review")
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();
    let (mut engine, inst) = deploy_and_start(def);
    let key = only_user_task_key(&engine);

    let assign = engine
        .apply_command(Command::assign_user_task(key, "alice"))
        .unwrap();
    // No task-listener machinery leaks into the journal.
    assert!(!assign.iter().any(|e| matches!(
        e,
        Event::TaskListenerJobCreated { .. }
            | Event::UserTaskTransitionDeferred { .. }
            | Event::UserTaskTransitionResolved { .. }
    )));
    assert_eq!(
        engine.state().user_tasks[&key].assignee.as_deref(),
        Some("alice")
    );

    engine
        .apply_command(Command::complete_user_task(key))
        .unwrap();
    assert!(engine.is_completed(inst));
}

#[test]
fn cancel_without_canceling_listener_terminates_synchronously() {
    // Byte-identical cancellation: a user task with no canceling listener is
    // canceled and the instance terminated in the same command.
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task("review")
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();
    let (mut engine, inst) = deploy_and_start(def);
    let key = only_user_task_key(&engine);
    let events = engine
        .apply_command(Command::cancel_instance(inst))
        .unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceTerminated { .. })));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceTerminating { .. })));
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Canceled
    );
    assert_eq!(
        engine.state().instances[&inst].state,
        state::ProcessInstanceState::Terminated
    );
}

/// `start -> review (user task w/ props + listeners) -> end`.
fn user_task_with_props_and_listeners(
    props: crate::model::UserTaskProps,
    listeners: Vec<TaskListener>,
) -> ProcessDefinition {
    ProcessBuilder::new("approval")
        .start_event("start")
        .user_task_with("review", props)
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .with_task_listeners("review", listeners)
        .build()
        .unwrap()
}

#[test]
fn initial_assignee_fires_the_assigning_listener() {
    // An initial assignee declared on the task must route through the `assigning`
    // listener (Zeebe parity): it is stripped off the CREATED record and applied
    // only once the assigning chain drains.
    use crate::model::UserTaskProps;
    let (mut engine, _inst) = deploy_and_start(user_task_with_props_and_listeners(
        UserTaskProps {
            assignee: Some("alice".into()),
            ..Default::default()
        },
        vec![tl(TaskListenerEventType::Assigning, "onAssign")],
    ));
    let key = only_user_task_key(&engine);
    // Not yet assigned: the assigning chain gates the initial assignment.
    assert_eq!(engine.state().user_tasks[&key].assignee, None);

    let jobs = engine.activate_jobs("onAssign", "W", 10, 1_000, 0);
    assert_eq!(
        jobs.len(),
        1,
        "one assigning-listener job for the initial assignee"
    );
    // The listener may correct it.
    engine
        .apply_command(Command::complete_job_with_task_result(
            jobs[0].key,
            TaskListenerJobResult {
                corrections: UserTaskCorrections {
                    assignee: Some("carol".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&key].assignee.as_deref(),
        Some("carol")
    );
}

#[test]
fn initial_assignee_routes_through_assigning_after_creating() {
    // creating chain first, then the stripped initial assignee's assigning chain.
    use crate::model::UserTaskProps;
    let (mut engine, _inst) = deploy_and_start(user_task_with_props_and_listeners(
        UserTaskProps {
            assignee: Some("alice".into()),
            ..Default::default()
        },
        vec![
            tl(TaskListenerEventType::Creating, "onCreate"),
            tl(TaskListenerEventType::Assigning, "onAssign"),
        ],
    ));
    let key = only_user_task_key(&engine);
    assert_eq!(engine.state().user_tasks[&key].assignee, None);
    // Only the creating job exists first.
    assert!(engine
        .activate_jobs("onAssign", "W", 10, 1_000, 0)
        .is_empty());
    let create = engine.activate_jobs("onCreate", "W", 10, 1_000, 0);
    assert_eq!(create.len(), 1);
    engine
        .apply_command(Command::complete_job(create[0].key))
        .unwrap();
    // Now the assigning chain for the initial assignee starts.
    let assign = engine.activate_jobs("onAssign", "W", 10, 1_000, 0);
    assert_eq!(assign.len(), 1);
    engine
        .apply_command(Command::complete_job(assign[0].key))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&key].assignee.as_deref(),
        Some("alice")
    );
}

#[test]
fn initial_assignee_without_assigning_listener_stays_byte_identical() {
    // No assigning listeners: the initial assignee is applied directly on CREATED,
    // exactly as before task listeners existed.
    use crate::model::UserTaskProps;
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task_with(
            "review",
            UserTaskProps {
                assignee: Some("alice".into()),
                ..Default::default()
            },
        )
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();
    let (engine, _inst) = deploy_and_start(def);
    let key = only_user_task_key(&engine);
    assert_eq!(
        engine.state().user_tasks[&key].assignee.as_deref(),
        Some("alice")
    );
}

#[test]
fn canceling_a_task_mid_transition_clears_its_pending_state() {
    // A task deferring a `completing` transition that is force-cancelled by
    // instance termination must not be left with an unresolvable pending
    // transition (its listener job is cancelled with the instance's jobs).
    let (mut engine, inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Completing,
        "onComplete",
    )]));
    let key = only_user_task_key(&engine);
    engine
        .apply_command(Command::complete_user_task(key))
        .unwrap();
    // The completing chain is in flight (pending set).
    assert!(engine.state().user_tasks[&key].pending.is_some());

    // Cancelling the instance (this task has no `canceling` listener) cancels it
    // immediately and terminates.
    engine
        .apply_command(Command::cancel_instance(inst))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Canceled
    );
    assert!(
        engine.state().user_tasks[&key].pending.is_none(),
        "a cancelled task must not retain a pending transition"
    );
    assert_eq!(
        engine.state().instances[&inst].state,
        state::ProcessInstanceState::Terminated
    );
}

/// Builds the classic event-based gateway race: a gateway routing to a timer
/// intermediate catch (`onTimer`, due 5s after activation) and a message
/// intermediate catch (`onReply`, correlated on `orderId`). Whichever fires
/// first wins; the loser is withdrawn.
fn event_gateway_race() -> ProcessDefinition {
    ProcessBuilder::new("race")
        .start_event("start")
        .event_based_gateway("gw")
        .timer_intermediate_catch_event("onTimer", 5_000)
        .message_intermediate_catch_event("onReply", "reply", "orderId")
        .end_event("timedOut")
        .end_event("replied")
        .connect("start", "gw")
        .connect("gw", "onTimer")
        .connect("gw", "onReply")
        .connect("onTimer", "timedOut")
        .connect("onReply", "replied")
        .build()
        .unwrap()
}

#[test]
fn event_based_gateway_arms_all_catch_events_then_timer_wins() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(event_gateway_race()))
        .unwrap();

    let mut vars = HashMap::new();
    vars.insert("orderId".to_string(), Value::Str("A".to_string()));
    let events = engine
        .apply_command_at(Command::create_instance_with("race", vars), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The gateway split armed both catch events at once: a timer (due 6000) and
    // an open message subscription.
    assert_eq!(engine.timers().len(), 1);
    assert_eq!(engine.timers()[0].state, state::TimerState::Created);
    assert_eq!(engine.timers()[0].due_at, 6_000);
    assert_eq!(engine.message_subscriptions().len(), 1);
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );
    assert!(!engine.is_completed(instance_key));

    // The timer fires first: its branch completes the instance and the losing
    // message sibling is withdrawn (its subscription cancelled).
    let fired = engine.trigger_timers(6_000);
    assert!(fired.contains(&Event::ProcessInstanceCompleted { instance_key }));
    assert!(engine.is_completed(instance_key));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Canceled
    );

    // A message arriving after the race is over correlates nothing.
    let late = engine
        .apply_command(Command::correlate_message("reply", "A"))
        .unwrap();
    assert!(!late
        .iter()
        .any(|e| matches!(e, Event::ElementActivated { .. })));
}

#[test]
fn event_based_gateway_message_wins_and_withdraws_the_timer_sibling() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(event_gateway_race()))
        .unwrap();

    let mut vars = HashMap::new();
    vars.insert("orderId".to_string(), Value::Str("A".to_string()));
    let events = engine
        .apply_command_at(Command::create_instance_with("race", vars), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The message correlates before the timer is due: its branch completes the
    // instance and the losing timer sibling is cancelled.
    let correlated = engine
        .apply_command_at(Command::correlate_message("reply", "A"), 2_000)
        .unwrap();
    assert!(correlated.contains(&Event::ProcessInstanceCompleted { instance_key }));
    assert!(engine.is_completed(instance_key));
    assert_eq!(engine.timers()[0].state, state::TimerState::Canceled);

    // The cancelled timer never fires, even past its original due instant.
    assert!(engine.trigger_timers(6_000).is_empty());
}

// Two event-based gateways route into the same catch event `onShared`, making
// the owning race ambiguous. When `onShared` wins we must NOT withdraw the
// sibling of either gateway, since we cannot tell which race it belonged to.
fn ambiguous_event_gateway_race() -> ProcessDefinition {
    ProcessBuilder::new("ambig")
        .start_event("start")
        .event_based_gateway("gw1")
        .event_based_gateway("gw2")
        .message_intermediate_catch_event("onShared", "reply", "orderId")
        .timer_intermediate_catch_event("onTimer1", 5_000)
        .timer_intermediate_catch_event("onTimer2", 5_000)
        .end_event("sharedEnd")
        .end_event("end1")
        .end_event("end2")
        .connect("start", "gw1")
        .connect("gw1", "onShared")
        .connect("gw1", "onTimer1")
        .connect("gw2", "onShared")
        .connect("gw2", "onTimer2")
        .connect("onShared", "sharedEnd")
        .connect("onTimer1", "end1")
        .connect("onTimer2", "end2")
        .build()
        .unwrap()
}

#[test]
fn event_based_gateway_ambiguous_owner_withdraws_nothing() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(ambiguous_event_gateway_race()))
        .unwrap();

    let mut vars = HashMap::new();
    vars.insert("orderId".to_string(), Value::Str("A".to_string()));
    let events = engine
        .apply_command_at(Command::create_instance_with("ambig", vars), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // Only gw1's race is live (gw2 is never reached): its timer `onTimer1` and
    // the shared message subscription are armed.
    assert_eq!(engine.timers().len(), 1);
    assert_eq!(engine.timers()[0].state, state::TimerState::Created);
    assert_eq!(engine.message_subscriptions().len(), 1);

    // The shared catch event wins its message. Because two gateways statically
    // route into it, the owning race is ambiguous, so the guard withdraws
    // nothing: the timer sibling is left armed (not cancelled), and its lingering
    // token keeps the instance live rather than completing it.
    let correlated = engine
        .apply_command_at(Command::correlate_message("reply", "A"), 2_000)
        .unwrap();
    assert!(!correlated
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })));
    assert!(!engine.is_completed(instance_key));
    assert_eq!(
        engine.timers()[0].state,
        state::TimerState::Created,
        "ambiguous owner must not cancel the sibling timer"
    );
}

// A malformed model routes an event-based gateway into a non-catch node (a
// service task) alongside a genuine catch event. When the catch event wins, the
// service-task sibling must NOT be force-completed: doing so would emit
// `ElementCompleted` without cancelling its job, orphaning the work. Only genuine
// catch siblings are ever withdrawn.
fn malformed_event_gateway_with_service_task_sibling() -> ProcessDefinition {
    ProcessBuilder::new("malformed")
        .start_event("start")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onReply", "reply", "orderId")
        .service_task("work", "do-work")
        .end_event("replied")
        .end_event("worked")
        .connect("start", "gw")
        .connect("gw", "onReply")
        .connect("gw", "work")
        .connect("onReply", "replied")
        .connect("work", "worked")
        .build()
        .unwrap()
}

#[test]
fn event_based_gateway_never_force_completes_non_catch_sibling() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            malformed_event_gateway_with_service_task_sibling(),
        ))
        .unwrap();

    let mut vars = HashMap::new();
    vars.insert("orderId".to_string(), Value::Str("A".to_string()));
    let events = engine
        .apply_command_at(Command::create_instance_with("malformed", vars), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The gateway armed both the message catch and the service task: a job exists.
    let job_before = engine
        .state()
        .jobs
        .values()
        .find(|j| j.job_type == "do-work")
        .expect("a do-work job was created");
    let job_state_before = job_before.state;

    // The message wins. The service-task sibling is a non-catch node, so it is
    // left untouched: its job survives in the same state (never force-completed),
    // and the lingering token keeps the instance live.
    let correlated = engine
        .apply_command_at(Command::correlate_message("reply", "A"), 2_000)
        .unwrap();
    assert!(!correlated
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })));
    assert!(!engine.is_completed(instance_key));
    let job_after = engine
        .state()
        .jobs
        .values()
        .find(|j| j.job_type == "do-work")
        .expect("the do-work job must still exist");
    assert_eq!(
        job_after.state, job_state_before,
        "a non-catch sibling must not be force-completed (its job must survive)"
    );
}

// ---- Correlation-key evaluation incident (durable fix for the silent hang) ----

// A catch whose correlation key concatenates a scalar (`planKey`) with a member
// of a variable (`task.id`) that is not yet a Map when the subscription opens.
// The FEEL `+` errors ("+ not defined for string and null"), which previously
// collapsed to an empty key and parked the token forever with no error.
fn process_with_concat_correlation_key() -> ProcessDefinition {
    ProcessBuilder::new("concat-corr")
        .start_event("start")
        .message_intermediate_catch_event("await", "answered", "=planKey + \":\" + task.id")
        .end_event("end")
        .connect("start", "await")
        .connect("await", "end")
        .build()
        .unwrap()
}

// --- Process-instance migration (Zeebe-behaviour parity) --------------------

/// Deploys `def` and returns the process definition key the engine assigned it.
fn deploy_for_migration(engine: &mut Engine, def: ProcessDefinition) -> Key {
    engine
        .apply_command(Command::DeployProcess(def))
        .unwrap()
        .iter()
        .find_map(|e| match e {
            Event::ProcessDeployed {
                process_definition_key,
                ..
            } => Some(*process_definition_key),
            _ => None,
        })
        .unwrap()
}

/// A linear process `id` whose single service task `task_id` emits `job_type`
/// jobs, parked while a worker is expected to pick them up.
fn migratable_task_process(id: &str, task_id: &str, job_type: &str) -> ProcessDefinition {
    ProcessBuilder::new(id)
        .start_event("start")
        .service_task(task_id, job_type)
        .end_event("end")
        .connect("start", task_id)
        .connect(task_id, "end")
        .build()
        .unwrap()
}

#[test]
fn errored_correlation_key_raises_incident_and_reopens_on_resolve() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_concat_correlation_key()))
        .unwrap();
    // `task` is absent at open time, so `task.id` is null and the concat errors.
    let created = engine
        .apply_command(Command::create_instance_with(
            "concat-corr",
            vars(&[("planKey", Value::Str("plan#1".into()))]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // No unmatchable subscription is opened; instead an incident is raised.
    assert!(
        engine.message_subscriptions().is_empty(),
        "an unevaluable correlation key must not open a subscription"
    );
    let incident_key = created
        .iter()
        .find_map(|e| match e {
            Event::IncidentRaised {
                incident_key, kind, ..
            } if *kind == state::IncidentKind::ExpressionEvaluation => Some(*incident_key),
            _ => None,
        })
        .expect("an ExpressionEvaluation incident is raised");
    assert!(
        !engine.is_completed(instance_key),
        "the token must still be parked"
    );
    assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);

    // Correct the variable so the key can evaluate, then resolve the incident.
    let mut task_map = std::collections::BTreeMap::new();
    task_map.insert("id".to_string(), Value::Str("w1".into()));
    engine
        .apply_command(Command::set_variables(
            instance_key,
            vars(&[("task", Value::Map(task_map))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    // Resolving re-opens the subscription with the now-correct key (it does NOT
    // complete the catch and skip the wait).
    assert!(
        !engine.is_completed(instance_key),
        "the catch must keep waiting"
    );
    let subs = engine.message_subscriptions();
    assert_eq!(subs.len(), 1, "the subscription is re-opened on resolve");
    assert_eq!(subs[0].correlation_key, "plan#1:w1");
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());

    // A matching message now correlates and drives the instance to completion.
    engine
        .apply_command(Command::correlate_message("answered", "plan#1:w1"))
        .unwrap();
    assert!(engine.is_completed(instance_key));
}

fn pending_job_element(engine: &Engine, instance_key: Key) -> String {
    engine
        .state()
        .jobs
        .values()
        .find(|j| j.instance_key == instance_key)
        .map(|j| j.element_id.clone())
        .expect("instance has a pending job")
}

#[test]
fn migration_remaps_active_service_task_and_its_job() {
    // A token parked on service task `a` of `source` is migrated to `b` of
    // `target`: the instance now belongs to `target`, its active element and its
    // pending job are re-pointed at `b`, and the job keeps its type.
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, migratable_task_process("source", "a", "work"));
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));

    let inst = create_instance_key(&mut engine, "source");
    assert_eq!(pending_job_element(&engine, inst), "a");

    let events = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("a".to_string(), "b".to_string())],
        ))
        .unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceMigrated { .. })),
        "a ProcessInstanceMigrated fact is emitted"
    );

    let instance = engine.instance(inst).unwrap();
    assert_eq!(
        instance.process_id, "target",
        "instance re-homed to target id"
    );
    assert_eq!(
        instance.process_definition_key, target_key,
        "instance re-pinned to the target definition version, so `definition_for` \
         resolves execution against the migrated-to model, not the source"
    );
    assert!(
        instance.active.values().any(|e| e == "b"),
        "active token re-pointed at target element b"
    );
    assert!(
        !instance.active.values().any(|e| e == "a"),
        "no active token still points at source element a"
    );
    assert_eq!(
        pending_job_element(&engine, inst),
        "b",
        "the pending job is re-pointed at b"
    );

    // The in-flight count moved from source to target.
    assert_eq!(
        engine.state().inflight_by_process.get("source").copied(),
        None,
        "source has no live instances"
    );
    assert_eq!(
        engine.state().inflight_by_process.get("target").copied(),
        Some(1),
        "target has one live instance"
    );
}

#[test]
fn migration_completes_via_remapped_job() {
    // After migrating, completing the (re-pointed) job drives the token to the
    // TARGET's end event, so the instance completes cleanly under its new
    // definition — proving the remap is executable, not cosmetic.
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, migratable_task_process("source", "a", "work"));
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));
    let inst = create_instance_key(&mut engine, "source");
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("a".to_string(), "b".to_string())],
        ))
        .unwrap();

    let job = engine
        .activate_jobs("work", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.instance_key == inst)
        .unwrap();
    assert_eq!(job.element_id, "b");
    engine
        .apply_command(Command::complete_job(job.key))
        .unwrap();

    let instance = engine.instance(inst).unwrap();
    assert_eq!(
        instance.state,
        crate::state::ProcessInstanceState::Completed,
        "instance completed through the target's end event"
    );
}

/// A `par` process `s -> split =< a, b >= join -> e` whose branch `a` has
/// already reached the join (so the join is half-open) while branch `b` is
/// still parked, migrated to an identically-shaped `target` with renamed
/// elements. Both `join_counts` and `join_instances` are keyed by the join
/// gateway's element id, so the applier must remap them *together*; a regression
/// that remapped only `join_counts` left `join_instances` pointing at the source
/// id, desyncing `join_eik` from `join_count` after migration.
#[test]
fn migration_remaps_both_parallel_join_maps_together() {
    fn par_join_process(id: &str, split: &str, a: &str, b: &str, join: &str) -> ProcessDefinition {
        ProcessBuilder::new(id)
            .start_event("s")
            .parallel_gateway(split)
            .service_task(a, "ja")
            .service_task(b, "jb")
            .parallel_gateway(join)
            .end_event("e")
            .connect("s", split)
            .connect(split, a)
            .connect(split, b)
            .connect(a, join)
            .connect(b, join)
            .connect(join, "e")
            .build()
            .unwrap()
    }

    let mut engine = Engine::new();
    deploy_for_migration(
        &mut engine,
        par_join_process("source", "split", "a", "b", "join"),
    );
    let target_key = deploy_for_migration(
        &mut engine,
        par_join_process("target", "split2", "a2", "b2", "join2"),
    );

    let inst = create_instance_key(&mut engine, "source");
    // Drive branch `a` into the join so it opens and waits for branch `b`.
    complete_one(&mut engine, "ja");

    let instance = engine.instance(inst).unwrap();
    assert!(
        instance.join_instances.contains_key("join"),
        "the join is half-open on the source id before migration"
    );
    assert_eq!(
        instance.join_counts.get("join").copied(),
        Some(1),
        "one branch has arrived at the join before migration"
    );

    // Active elements at this point: the parked task `b` and the open `join`.
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("b".to_string(), "b2".to_string()),
                ("join".to_string(), "join2".to_string()),
            ],
        ))
        .unwrap();

    let instance = engine.instance(inst).unwrap();
    assert!(
        instance.join_counts.contains_key("join2") && !instance.join_counts.contains_key("join"),
        "join_counts re-keyed onto the target join id"
    );
    assert!(
        instance.join_instances.contains_key("join2")
            && !instance.join_instances.contains_key("join"),
        "join_instances re-keyed onto the target join id (kept in sync with join_counts)"
    );
    assert_eq!(
        instance.join_counts.get("join2").copied(),
        Some(1),
        "the arrival count survives the remap"
    );

    // Executable proof: completing the remaining branch fires the join once and
    // the instance completes cleanly under the target definition.
    complete_one(&mut engine, "jb");
    let instance = engine.instance(inst).unwrap();
    assert_eq!(
        instance.state,
        crate::state::ProcessInstanceState::Completed,
        "the remapped join fires and the instance completes under the target"
    );
}

/// An already-*open* parallel join (branch `a` has arrived, so a partial count
/// of 1 is durably recorded) cannot be migrated onto a target join with a
/// *different* number of incoming flows: the partial count is compared against
/// the target definition's incoming-flow count at fire time, so a 2-flow join
/// half-open at count 1, remapped onto a 3-flow join, would deadlock (its third
/// flow never arrives) — and a wider-to-narrower remap would early-fire.
/// Reject with `MigratedParallelJoinArityChanged`. Red/Green guard for the
/// failure-mode class "durable count state whose threshold lives in the
/// definition being migrated away".
#[test]
fn migration_rejects_open_join_with_different_incoming_arity() {
    fn par_join_2(id: &str, split: &str, a: &str, b: &str, join: &str) -> ProcessDefinition {
        ProcessBuilder::new(id)
            .start_event("s")
            .parallel_gateway(split)
            .service_task(a, "ja")
            .service_task(b, "jb")
            .parallel_gateway(join)
            .end_event("e")
            .connect("s", split)
            .connect(split, a)
            .connect(split, b)
            .connect(a, join)
            .connect(b, join)
            .connect(join, "e")
            .build()
            .unwrap()
    }
    // Target join `join2` has THREE incoming flows, versus the source's two.
    fn par_join_3(id: &str, split: &str, join: &str) -> ProcessDefinition {
        ProcessBuilder::new(id)
            .start_event("s")
            .parallel_gateway(split)
            .service_task("a2", "ja")
            .service_task("b2", "jb")
            .service_task("c2", "jc")
            .parallel_gateway(join)
            .end_event("e")
            .connect("s", split)
            .connect(split, "a2")
            .connect(split, "b2")
            .connect(split, "c2")
            .connect("a2", join)
            .connect("b2", join)
            .connect("c2", join)
            .connect(join, "e")
            .build()
            .unwrap()
    }

    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, par_join_2("source", "split", "a", "b", "join"));
    let target_key = deploy_for_migration(&mut engine, par_join_3("target", "split2", "join2"));

    let inst = create_instance_key(&mut engine, "source");
    // Open the join: branch `a` arrives, leaving the join half-open at count 1.
    complete_one(&mut engine, "ja");
    let instance = engine.instance(inst).unwrap();
    assert!(
        instance.join_instances.contains_key("join"),
        "precondition: the join is open before migration"
    );

    // Active elements now: parked task `b` and the open `join`. Map both, but
    // point the open join at the 3-flow target join.
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("b".to_string(), "b2".to_string()),
                ("join".to_string(), "join2".to_string()),
            ],
        ))
        .unwrap_err();
    assert!(
        matches!(
            &err,
            EngineError::MigratedParallelJoinArityChanged {
                source_element_id,
                target_element_id,
                source_incoming_count: 2,
                target_incoming_count: 3,
                ..
            } if source_element_id == "join" && target_element_id == "join2"
        ),
        "an open 2-flow join mapped onto a 3-flow join is rejected, got {err:?}"
    );

    // The rejection is all-or-nothing: the instance is untouched (still on the
    // source definition, join still open on the source id).
    let instance = engine.instance(inst).unwrap();
    assert_eq!(instance.process_id, "source", "instance not migrated");
    assert!(
        instance.join_instances.contains_key("join"),
        "the open join is left intact on the source id"
    );
}

#[test]
fn migration_rejects_unknown_instance() {
    let mut engine = Engine::new();
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));
    let err = engine
        .apply_command(Command::migrate_instance(
            999,
            target_key,
            vec![("a".to_string(), "b".to_string())],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::InstanceNotFound { instance_key: 999 }
    ));
}

#[test]
fn migration_rejects_unknown_target_definition() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, migratable_task_process("source", "a", "work"));
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            424_242,
            vec![("a".to_string(), "b".to_string())],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::TargetProcessDefinitionNotFound {
            process_definition_key: 424_242
        }
    ));
}

#[test]
fn migration_rejects_duplicate_source_mapping() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, migratable_task_process("source", "a", "work"));
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("a".to_string(), "b".to_string()),
                ("a".to_string(), "b".to_string()),
            ],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::DuplicateMappingSourceElement { element_id, .. } if element_id == "a"
    ));
}

#[test]
fn migration_rejects_unknown_source_element() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, migratable_task_process("source", "a", "work"));
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("nope".to_string(), "b".to_string())],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::MappingSourceElementNotFound { element_id, .. } if element_id == "nope"
    ));
}

#[test]
fn migration_rejects_unknown_target_element() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, migratable_task_process("source", "a", "work"));
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("a".to_string(), "nope".to_string())],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::MappingTargetElementNotFound { element_id, .. } if element_id == "nope"
    ));
}

#[test]
fn migration_rejects_unmapped_active_element() {
    // No mapping for the active service task `a` → the active token would be
    // orphaned, so migration is rejected (Zeebe 409 parity).
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, migratable_task_process("source", "a", "work"));
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(inst, target_key, vec![]))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::UnmappedActiveElement { element_id, .. } if element_id == "a"
    ));
}

#[test]
fn migration_rejects_type_change() {
    // Mapping the active service task to a user task changes the element type →
    // rejected (Zeebe 409 parity).
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, migratable_task_process("source", "a", "work"));
    let target_key = deploy_for_migration(
        &mut engine,
        ProcessBuilder::new("target")
            .start_event("start")
            .user_task("b")
            .end_event("end")
            .connect("start", "b")
            .connect("b", "end")
            .build()
            .unwrap(),
    );
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("a".to_string(), "b".to_string())],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::MappedElementTypeChanged {
            source_element_id,
            target_element_id,
            ..
        } if source_element_id == "a" && target_element_id == "b"
    ));
}

#[test]
fn migration_rejects_unsupported_boundary_event() {
    // The active service task carries an armed interrupting timer boundary event.
    // Boundary events are not migratable in this phase (Zeebe rejects several
    // such cases as "not supported yet") → rejected.
    let mut engine = Engine::new();
    deploy_for_migration(
        &mut engine,
        ProcessBuilder::new("source")
            .start_event("start")
            .service_task("a", "work")
            .timer_boundary_event("boundary", "a", 60_000)
            .end_event("end")
            .end_event("caught")
            .connect("start", "a")
            .connect("a", "end")
            .connect("boundary", "caught")
            .build()
            .unwrap(),
    );
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("a".to_string(), "b".to_string())],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::UnsupportedMigration { element_id, .. } if element_id == "boundary"
    ));
}

#[test]
fn migration_rejects_active_multi_instance_body() {
    // An active parallel multi-instance body keeps per-element bookkeeping in
    // `ProcessInstance.multi_instances` that this phase does not remap, so
    // migration is rejected as "not supported yet" (Zeebe parity), matching the
    // boundary-event rejection above.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(
            false,
        )))
        .unwrap();
    let target = ProcessBuilder::new("mi-target")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: Some("results".to_string()),
                output_element: Some("=item * 2".to_string()),
                completion_condition: None,
                sequential: false,
            },
        )
        .service_task("sink", "sink-work")
        .end_event("end")
        .connect("start", "each")
        .connect("each", "sink")
        .connect("sink", "end")
        .build()
        .unwrap();
    let target_key = deploy_for_migration(&mut engine, target);
    let inst = create_instance_with_vars(
        &mut engine,
        "mi",
        vars(&[(
            "items",
            Value::List(vec![Value::Int(10), Value::Int(20), Value::Int(30)]),
        )]),
    );
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("each".to_string(), "each".to_string())],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::UnsupportedMigration { element_id, reason, .. }
            if element_id == "each" && reason.contains("multi-instance")
    ));
}

#[test]
fn migration_rejects_active_token_inside_a_nested_flow_scope() {
    // Defect class (Zeebe's "flow scope unchanged" precondition): an active
    // token resting inside an embedded sub-process lives in a non-root flow
    // scope. The applier re-points element ids but does NOT remap the scope
    // tree (`scopes` / `scope_parents` / `scope_variables`), so migrating such
    // an instance would leave its scope tree pointing at the source structure.
    // Migration must be rejected as unsupported. (Here the enclosing sub-process
    // is itself active and trips the sub-process guard first; the explicit
    // flow-scope backstop guarantees the class stays rejected even if a future
    // scope-owning element type is not caught by a more specific guard.)
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    // The instance parks a token on the `work` job INSIDE sub-process `sub`, so
    // `instance.scopes` is non-empty.
    let inst =
        create_instance_with_vars(&mut engine, "sub-scope", vars(&[("seed", Value::Int(4))]));
    assert!(
        !engine.instance(inst).unwrap().scopes.is_empty(),
        "precondition: the token must sit inside a non-root flow scope"
    );

    // A flat target carrying the inner task at process level.
    let target = ProcessBuilder::new("flat-target")
        .start_event("s")
        .service_task("inner", "work")
        .end_event("e")
        .connect("s", "inner")
        .connect("inner", "e")
        .build()
        .unwrap();
    let target_key = deploy_for_migration(&mut engine, target);

    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("inner".to_string(), "inner".to_string())],
        ))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::UnsupportedMigration { .. }),
        "an instance with an active token in a nested flow scope must not migrate; got {err:?}"
    );
    // The instance stays on its source definition — nothing was migrated.
    assert_eq!(engine.instance(inst).unwrap().process_id, "sub-scope");
}

// ===========================================================================
// Issue #750 — process-definition version support (Zeebe parity).
//
// The engine retains *every* deployed version of a process definition (keyed
// by process-definition key), pins each running instance to the exact version
// it was created on, and honours the create-time version selector (by explicit
// definition key, or by process id + version number, defaulting to latest).
// ===========================================================================

/// A `payment`-emitting "order" definition whose model differs from
/// `linear_with_task` by adding `extra` service tasks after `charge`, so each
/// distinct `extra` count deploys as a new, non-idempotent version of "order".
fn order_with_extra_tasks(extra: usize) -> ProcessDefinition {
    let mut b = ProcessBuilder::new("order")
        .start_event("start")
        .service_task("charge", "payment");
    let mut prev = "charge".to_string();
    for i in 0..extra {
        let id = format!("extra{i}");
        b = b.service_task(&id, "work");
        b = b.connect(&prev, &id);
        prev = id;
    }
    b.end_event("end")
        .connect("start", "charge")
        .connect(&prev, "end")
        .build()
        .unwrap()
}

/// Deploy `def` and return its `(process_definition_key, version)`.
fn deploy_returning_key(engine: &mut Engine, def: ProcessDefinition) -> (Key, i32) {
    engine
        .apply_command(Command::DeployProcess(def))
        .unwrap()
        .iter()
        .find_map(|e| match e {
            Event::ProcessDeployed {
                process_definition_key,
                version,
                ..
            } => Some((*process_definition_key, *version)),
            _ => None,
        })
        .expect("a changed definition emits ProcessDeployed")
}

#[test]
fn deploying_multiple_versions_retains_every_version_by_key() {
    // v1, v2, v3 of the same id must all be retained in `process_versions`
    // (keyed by definition key), with monotonically increasing versions and a
    // latest-index that tracks the highest version.
    let mut engine = Engine::new();
    let (k1, v1) = deploy_returning_key(&mut engine, order_with_extra_tasks(0));
    let (k2, v2) = deploy_returning_key(&mut engine, order_with_extra_tasks(1));
    let (k3, v3) = deploy_returning_key(&mut engine, order_with_extra_tasks(2));

    assert_eq!((v1, v2, v3), (1, 2, 3), "versions increment monotonically");
    assert!(
        k1 != k2 && k2 != k3 && k1 != k3,
        "each version has a distinct definition key"
    );

    let versions = &engine.state().process_versions;
    for (k, v) in [(k1, 1), (k2, 2), (k3, 3)] {
        let d = versions
            .get(&k)
            .unwrap_or_else(|| panic!("version {v} (key {k}) is retained"));
        assert_eq!(d.version, v, "retained definition reports its own version");
    }

    // The latest-by-id index points at the highest version.
    let latest = engine.state().processes.get("order").unwrap();
    assert_eq!(latest.version, 3, "latest index tracks the newest version");
    assert_eq!(latest.key, k3);
}

#[test]
fn create_by_key_pins_instance_to_that_exact_version() {
    // Creating by the *key* of an older version must pin the instance to that
    // version even though a newer one is the latest — the key already
    // identifies the version (Zeebe by-key semantics), so any version selector
    // is irrelevant.
    let mut engine = Engine::new();
    let (k1, _) = deploy_returning_key(&mut engine, order_with_extra_tasks(0));
    let (_k2, _) = deploy_returning_key(&mut engine, order_with_extra_tasks(1)); // v2 is latest

    let events = engine
        .apply_command(Command::create_instance_versioned(
            "order",
            HashMap::new(),
            Vec::new(),
            None,
            Some(k1),
            // A version selector is ignored on a by-key create; prove it does
            // not override the key's own version.
            Some(2),
        ))
        .unwrap();
    let inst_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    let instance = engine.state().instances.get(&inst_key).unwrap();
    assert_eq!(
        instance.process_definition_key, k1,
        "the instance is pinned to the requested (older) version's key"
    );
    let def = engine
        .state()
        .definition_for(instance)
        .expect("pinned definition resolves");
    assert_eq!(
        def.version, 1,
        "execution resolves the pinned v1, not latest"
    );

    // The job it emits reports the pinned version, not the latest.
    let job = &engine.activate_jobs("payment", "w", 1, 60_000, 0)[0];
    assert_eq!(job.process_definition_version, 1);
    assert_eq!(job.process_definition_key, k1);
}

#[test]
fn create_by_id_and_version_selects_that_version_else_latest_else_errors() {
    let mut engine = Engine::new();
    let (k1, _) = deploy_returning_key(&mut engine, order_with_extra_tasks(0));
    let (k2, _) = deploy_returning_key(&mut engine, order_with_extra_tasks(1)); // v2 latest

    // Explicit version 1 → v1.
    let by_v1 = engine
        .apply_command(Command::create_instance_versioned(
            "order",
            HashMap::new(),
            Vec::new(),
            None,
            None,
            Some(1),
        ))
        .unwrap();
    let i1 = by_v1.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(
        engine
            .state()
            .instances
            .get(&i1)
            .unwrap()
            .process_definition_key,
        k1
    );

    // No version → latest (v2).
    let by_latest = engine
        .apply_command(Command::create_instance_versioned(
            "order",
            HashMap::new(),
            Vec::new(),
            None,
            None,
            None,
        ))
        .unwrap();
    let i2 = by_latest.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(
        engine
            .state()
            .instances
            .get(&i2)
            .unwrap()
            .process_definition_key,
        k2
    );

    // Unknown version → error, no instance created.
    let err = engine.apply_command(Command::create_instance_versioned(
        "order",
        HashMap::new(),
        Vec::new(),
        None,
        None,
        Some(99),
    ));
    assert!(
        matches!(err, Err(EngineError::ProcessNotFound { .. })),
        "an unknown version is rejected, not silently coerced to latest"
    );
}

#[test]
fn instance_without_a_pinned_key_resolves_to_latest() {
    // Back-compat: an instance materialized from an old snapshot (no
    // `process_definition_key`, i.e. key 0) must resolve its definition via the
    // latest-by-id index rather than failing to resolve.
    let mut engine = Engine::new();
    deploy_returning_key(&mut engine, order_with_extra_tasks(0));
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let inst_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Simulate an old-format instance by clearing its pinned key.
    // (Field is `#[serde(default)]` → 0 on legacy snapshots.)
    let mut state = engine.state().clone();
    state
        .instances
        .get_mut(&inst_key)
        .unwrap()
        .process_definition_key = 0;
    let instance = state.instances.get(&inst_key).unwrap();
    let def = state
        .definition_for(instance)
        .expect("a key-0 instance falls back to the latest-by-id definition");
    assert_eq!(def.definition.id, "order");
}

#[test]
fn every_active_instance_pins_its_own_definition_key() {
    // Class-scoped guard: no live instance may rely on the latest-by-id index
    // for execution. Even after a newer version is deployed, previously-created
    // instances keep their original pinned key and resolve their original
    // version.
    let mut engine = Engine::new();
    let (k1, _) = deploy_returning_key(&mut engine, order_with_extra_tasks(0));
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let inst_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // A newer version lands *after* the instance was created.
    let (_k2, _) = deploy_returning_key(&mut engine, order_with_extra_tasks(1));

    for (key, instance) in &engine.state().instances {
        assert_ne!(
            instance.process_definition_key, 0,
            "instance {key} must pin a concrete definition key, not fall through to latest"
        );
    }
    let instance = engine.state().instances.get(&inst_key).unwrap();
    assert_eq!(instance.process_definition_key, k1);
    let job = &engine.activate_jobs("payment", "w", 1, 60_000, 0)[0];
    assert_eq!(
        job.process_definition_version, 1,
        "the pre-existing instance still executes v1 after v2 is deployed"
    );
}

#[cfg(feature = "serde")]
#[test]
fn snapshot_round_trip_retains_all_versions_and_instance_pins() {
    // A serialized snapshot must preserve every retained version *and* each
    // instance's pinned definition key, so a node rebuilt from a snapshot
    // resolves execution against the same versions.
    let mut engine = Engine::new();
    let (k1, _) = deploy_returning_key(&mut engine, order_with_extra_tasks(0));
    let (k2, _) = deploy_returning_key(&mut engine, order_with_extra_tasks(1));
    let created = engine
        .apply_command(Command::create_instance_versioned(
            "order",
            HashMap::new(),
            Vec::new(),
            None,
            Some(k1),
            None,
        ))
        .unwrap();
    let inst_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let snapshot = engine.snapshot();
    let serialized = serde_json::to_vec(&snapshot).expect("snapshot serializes");
    let decoded: EngineSnapshot =
        serde_json::from_slice(&serialized).expect("snapshot deserializes");
    let restored = Engine::from_snapshot(decoded);

    assert_eq!(
        restored.state(),
        engine.state(),
        "restored state equals the source state, versions and pins included"
    );
    assert!(restored.state().process_versions.contains_key(&k1));
    assert!(restored.state().process_versions.contains_key(&k2));
    assert_eq!(
        restored
            .state()
            .instances
            .get(&inst_key)
            .unwrap()
            .process_definition_key,
        k1,
        "the instance's pinned key survives the round-trip"
    );
}
