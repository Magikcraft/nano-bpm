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
fn suspend_resume_lifecycle_gates_jobs_and_timers() {
    use crate::state::ProcessInstanceState;
    // start -> charge (service task, job `payment`) -> wait (timer PT5S) -> end
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

    let events = engine
        .apply_command_at(Command::create_instance("delayed"), 1_000)
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(
        engine.instance(key).unwrap().state,
        ProcessInstanceState::Active
    );

    // Suspend: the instance stops making progress. Its created `payment` job is
    // no longer activatable while suspended (Camunda parity).
    let suspend = engine
        .apply_command_at(Command::suspend_instance(key), 2_000)
        .expect("suspend");
    assert!(suspend.contains(&Event::ProcessInstanceSuspended {
        instance_key: key,
        at: 2_000,
    }));
    assert_eq!(
        engine.instance(key).unwrap().state,
        ProcessInstanceState::Suspended
    );
    assert_eq!(engine.instance(key).unwrap().suspended_at, Some(2_000));
    assert!(
        engine
            .activate_jobs("payment", "w", 1, 60_000, 2_500)
            .is_empty(),
        "a suspended instance's jobs are not activatable"
    );

    // Resume: back to Active with its exact prior running state; the job is live
    // again and `suspended_at` clears.
    let resume = engine
        .apply_command_at(Command::resume_instance(key), 3_000)
        .expect("resume");
    assert!(resume.contains(&Event::ProcessInstanceResumed { instance_key: key }));
    assert_eq!(
        engine.instance(key).unwrap().state,
        ProcessInstanceState::Active
    );
    assert_eq!(engine.instance(key).unwrap().suspended_at, None);

    let job = engine
        .activate_jobs("payment", "w", 1, 60_000, 3_000)
        .into_iter()
        .next()
        .expect("job activatable again after resume");
    engine
        .apply_command_at(Command::complete_job(job.key), 3_000)
        .unwrap();

    // Token now parked on the timer armed for due_at = 3000 + 5000 = 8000.
    assert!(!engine.is_completed(key));
    assert_eq!(engine.timers()[0].due_at, 8_000);

    // Suspend again: a due timer must NOT fire while the instance is suspended.
    engine
        .apply_command_at(Command::suspend_instance(key), 8_500)
        .expect("re-suspend");
    let fired = engine.trigger_timers(9_000);
    assert!(
        fired.is_empty(),
        "a suspended instance's timers do not fire"
    );
    assert!(!engine.is_completed(key));

    // Resume and tick again: the timer now fires and the instance completes.
    engine
        .apply_command_at(Command::resume_instance(key), 9_500)
        .expect("resume 2");
    let fired = engine.trigger_timers(10_000);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::TimerTriggered { .. })));
    assert!(engine.is_completed(key));
}

#[test]
fn suspend_drops_message_correlation_and_does_not_rebuffer() {
    use crate::state::ProcessInstanceState;
    // A suspended instance makes no progress, and the message model is
    // unbuffered — so a message correlated to a suspended instance is DROPPED
    // (not buffered) and does not re-correlate on resume. This is the
    // documented drop-on-suspend suspension semantics for messages, mirroring
    // the gate jobs and timers already have.
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

    // Parked on the message catch with one open subscription.
    assert!(!engine.is_completed(instance_key));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );

    // Suspend, then publish the matching message: it correlates NOTHING while
    // suspended and the subscription stays open (the message is dropped).
    engine
        .apply_command_at(Command::suspend_instance(instance_key), 1_000)
        .expect("suspend");
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        ProcessInstanceState::Suspended
    );
    let fired = engine.correlate_message("payment-received", "A", HashMap::new(), 1_500);
    assert!(
        !fired
            .iter()
            .any(|e| matches!(e, Event::MessageCorrelated { .. })),
        "a suspended instance does not correlate messages"
    );
    assert!(!engine.is_completed(instance_key));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open,
        "the subscription stays open; the dropped message is not buffered"
    );

    // Resume: the previously dropped message does NOT re-correlate — the token
    // is still parked on the catch.
    engine
        .apply_command_at(Command::resume_instance(instance_key), 2_000)
        .expect("resume");
    assert!(
        !engine.is_completed(instance_key),
        "the message dropped during suspension does not re-correlate on resume"
    );
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );

    // A fresh matching message after resume correlates normally and completes.
    let fired = engine.correlate_message("payment-received", "A", HashMap::new(), 2_500);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn suspend_resume_reject_illegal_transitions() {
    use crate::state::ProcessInstanceState;
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // Unknown instance: both suspend and resume are a clean not-found.
    assert!(matches!(
        engine.apply_command(Command::suspend_instance(999)),
        Err(EngineError::InstanceNotFound { instance_key: 999 })
    ));
    assert!(matches!(
        engine.apply_command(Command::resume_instance(999)),
        Err(EngineError::InstanceNotFound { instance_key: 999 })
    ));

    let events = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // Resuming an Active instance is an idempotent no-op (no error, no event).
    let noop = engine.apply_command(Command::resume_instance(key)).unwrap();
    assert!(noop.is_empty());
    assert_eq!(
        engine.instance(key).unwrap().state,
        ProcessInstanceState::Active
    );

    // Suspend, then suspending again is an idempotent no-op.
    engine
        .apply_command(Command::suspend_instance(key))
        .unwrap();
    let noop = engine
        .apply_command(Command::suspend_instance(key))
        .unwrap();
    assert!(noop.is_empty());

    engine.apply_command(Command::resume_instance(key)).unwrap();

    // Cancel to a terminal state, then neither transition is valid.
    engine.apply_command(Command::cancel_instance(key)).unwrap();
    assert_eq!(
        engine.instance(key).unwrap().state,
        ProcessInstanceState::Terminated
    );
    assert!(matches!(
        engine.apply_command(Command::suspend_instance(key)),
        Err(EngineError::InstanceTransitionInvalid {
            instance_key,
            to: "SUSPENDED",
            ..
        }) if instance_key == key
    ));
    assert!(matches!(
        engine.apply_command(Command::resume_instance(key)),
        Err(EngineError::InstanceTransitionInvalid {
            instance_key,
            to: "ACTIVE",
            ..
        }) if instance_key == key
    ));
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

#[test]
fn terminate_end_kills_sibling_branch_and_completes_the_instance() {
    // s -> split =< work (service task), trigger (service task) -> stop (terminate end) >
    //
    // A parallel split forks two branches: `work` parks on a job while
    // `trigger` reaches a terminate end. Completing `trigger` must kill the
    // still-active `work` token (cancelling its job) and COMPLETE the whole
    // top-level instance — Zeebe parity (#1085): the terminate end kills every
    // inner token, but the process instance's own terminal record is
    // `ProcessInstanceCompleted` (`PROCESS -> ELEMENT_COMPLETED`), not
    // `ProcessInstanceTerminated`. It must still not degrade to a plain end that
    // leaves `work` running.
    let def = ProcessBuilder::new("term")
        .start_event("s")
        .parallel_gateway("split")
        .service_task("work", "work-job")
        .service_task("trigger", "trigger-job")
        .terminate_end_event("stop")
        .connect("s", "split")
        .connect("split", "work")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("term"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Both branches forked; both tasks are parked on jobs.
    assert_eq!(engine.pending_jobs().len(), 2);
    assert!(!engine.is_completed(instance_key));
    let work_job = engine.activate_jobs("work-job", "w", 1, 60_000, 0)[0].key;

    // Completing the trigger job drives its token into the terminate end.
    let events = complete_one(&mut engine, "trigger-job");

    // The terminate end kills the sibling `work` token (its job cancelled) and
    // completes the whole instance — the instance's terminal record is
    // COMPLETED (Zeebe parity), not TERMINATED.
    assert!(events.contains(&Event::JobCanceled {
        job_key: work_job,
        instance_key,
    }));
    assert!(events.contains(&Event::ProcessInstanceCompleted { instance_key }));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceTerminated { .. })));
    assert!(engine.is_completed(instance_key));
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        crate::state::ProcessInstanceState::Completed
    );
    // No token survives: the instance holds no active elements after completion.
    assert!(engine.instance(instance_key).unwrap().active.is_empty());
    // The cancelled sibling job can no longer be completed.
    let err = engine
        .apply_command(Command::complete_job(work_job))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotActive { job_key: work_job });
}

#[test]
fn subprocess_terminate_end_confines_to_the_subprocess_scope() {
    // A terminate end inside an embedded sub-process kills only that
    // sub-process's tokens and lets the parent instance continue on the
    // sub-process's outgoing flow — it does NOT terminate the whole instance.
    //
    //  start -> sub -> after (service task) -> done
    //  sub: sub_start -> split =< inner_work (svc), inner_trigger (svc) -> inner_stop (terminate end) >
    let def = ProcessBuilder::new("sub-term")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .service_task("inner_work", "inner-work-job")
        .contained_in("inner_work", "sub")
        .service_task("inner_trigger", "inner-trigger-job")
        .contained_in("inner_trigger", "sub")
        .terminate_end_event("inner_stop")
        .contained_in("inner_stop", "sub")
        .service_task("after", "after-job")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "inner_work")
        .connect("split", "inner_trigger")
        .connect("inner_trigger", "inner_stop")
        .connect("sub", "after")
        .connect("after", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("sub-term"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Inside the sub-process both inner branches are parked on jobs.
    assert_eq!(engine.pending_jobs().len(), 2);
    let inner_work_job = engine.activate_jobs("inner-work-job", "w", 1, 60_000, 0)[0].key;

    // Completing the inner trigger drives its token into the terminate end.
    let events = complete_one(&mut engine, "inner-trigger-job");

    // The inner sibling token is killed (its job cancelled)...
    assert!(events.contains(&Event::JobCanceled {
        job_key: inner_work_job,
        instance_key,
    }));
    // ...and the sub-process completes and continues on its outgoing flow —
    // the parent instance is NOT terminated.
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "sub" && to == "after"
    )));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceTerminated { .. })));
    assert!(!engine.is_completed(instance_key));
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        crate::state::ProcessInstanceState::Active
    );

    // The parent proceeds normally: completing the `after` task ends the
    // instance by ordinary completion, not termination.
    let final_events = complete_one(&mut engine, "after-job");
    assert!(final_events.contains(&Event::ProcessInstanceCompleted { instance_key }));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn subprocess_terminate_end_terminates_a_call_activity_child_in_its_scope() {
    // A terminate end inside a sub-process must also reap a call-activity CHILD
    // process instance spawned from that scope — the child is a separate instance
    // `scope_descendants` cannot see, so without explicit termination it (and its
    // jobs) would keep running with no parent token.
    //
    //  orch: start -> sub -> after(svc) -> done
    //  sub:  sub_start -> split =< c1(call "leaf"), trigger(svc) -> inner_stop(terminate) >
    let leaf = ProcessBuilder::new("leaf")
        .start_event("ls")
        .service_task("leaf_work", "leaf-job")
        .end_event("le")
        .connect("ls", "leaf_work")
        .connect("leaf_work", "le")
        .build()
        .unwrap();
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .call_activity("c1", "leaf")
        .contained_in("c1", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("inner_stop")
        .contained_in("inner_stop", "sub")
        .service_task("after", "after-job")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "c1")
        .connect("split", "trigger")
        .connect("trigger", "inner_stop")
        .connect("sub", "after")
        .connect("after", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(leaf)).unwrap();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The call activity spawned a distinct child parked on its own job; the
    // trigger branch parks on its own job.
    let child_key = engine
        .pending_jobs()
        .iter()
        .find(|j| j.job_type == "leaf-job")
        .expect("leaf child parked on its job")
        .instance_key;
    assert_ne!(child_key, instance_key);
    assert!(engine
        .pending_jobs()
        .iter()
        .any(|j| j.job_type == "trigger-job"));

    // Completing the trigger drives its token into the sub-process terminate end.
    let events = complete_one(&mut engine, "trigger-job");

    // The call-activity child in the terminated scope is terminated too, and its
    // job cancelled with it.
    assert!(events.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceTerminated { instance_key: ik } if *ik == child_key
    )));
    assert_eq!(
        engine.instance(child_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminated
    );
    assert!(!engine
        .pending_jobs()
        .iter()
        .any(|j| j.job_type == "leaf-job"));

    // The parent continues past the sub-process; the parent instance itself is
    // NOT terminated.
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "sub" && to == "after"
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceTerminated { instance_key: ik } if *ik == instance_key
    )));
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        crate::state::ProcessInstanceState::Active
    );
}

#[test]
fn top_level_terminate_end_in_a_called_process_completes_the_parent_call_activity() {
    // A terminate end in a CALLED process ends only that child instance (Zeebe/
    // BPMN: terminate never propagates out of its own process). The child is the
    // root scope of its own instance, so a top-level terminate end COMPLETES it
    // (#1085 — `PROCESS -> ELEMENT_COMPLETED`, only inner tokens TERMINATED). The
    // parent's call activity then completes normally and the parent continues on
    // its outgoing flow — the parent must not be left parked forever, nor
    // terminated.
    //
    //  phase (called): ps -> psplit =< work(svc), trg(svc) -> stop(terminate) >
    //  orch:           start -> c1(call "phase") -> end
    let phase = ProcessBuilder::new("phase")
        .start_event("ps")
        .parallel_gateway("psplit")
        .service_task("work", "work-job")
        .service_task("trg", "trg-job")
        .terminate_end_event("stop")
        .connect("ps", "psplit")
        .connect("psplit", "work")
        .connect("psplit", "trg")
        .connect("trg", "stop")
        .build()
        .unwrap();
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "phase")
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(phase)).unwrap();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");
    let child_key = engine
        .pending_jobs()
        .first()
        .expect("child parked on its jobs")
        .instance_key;
    assert_ne!(child_key, parent_key);
    assert_eq!(engine.pending_jobs().len(), 2);

    // Completing the child's trg job drives its token into the top-level
    // terminate end of the called process.
    let events = complete_one(&mut engine, "trg-job");

    // The child completes (its sibling work token cancelled), and its terminal
    // record is COMPLETED, not TERMINATED (#1085)...
    assert!(events.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceCompleted { instance_key: ik } if *ik == child_key
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceTerminated { instance_key: ik } if *ik == child_key
    )));
    // ...and the terminate does NOT propagate to the parent: the call activity
    // completes and the parent runs to ordinary completion.
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceTerminated { instance_key: ik } if *ik == parent_key
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "c1" && to == "end"
    )));
    assert!(engine.is_completed(parent_key));
    assert!(engine.is_completed(child_key));
    assert_eq!(
        engine.instance(child_key).unwrap().state,
        crate::state::ProcessInstanceState::Completed
    );
}

#[test]
fn subprocess_terminate_end_cancels_a_resting_user_task_in_its_scope() {
    // A terminate end must cancel a resting user task in the torn-down scope —
    // `ElementCompleted` alone leaves it queryable as `Created` and completable.
    //
    //  sub: sub_start -> split =< ut(user task), trigger(svc) -> stop(terminate) >
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .user_task("ut")
        .contained_in("ut", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("stop")
        .contained_in("stop", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "ut")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .connect("sub", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let ut_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .expect("user task created");
    assert_eq!(
        engine.state().user_tasks[&ut_key].state,
        crate::state::UserTaskState::Created
    );

    let events = complete_one(&mut engine, "trigger-job");
    assert!(events.iter().any(|e| matches!(
        e,
        Event::UserTaskCanceled { user_task_key, .. } if *user_task_key == ut_key
    )));
    assert_eq!(
        engine.state().user_tasks[&ut_key].state,
        crate::state::UserTaskState::Canceled
    );
}

#[test]
fn subprocess_terminate_end_cancels_a_signal_subscription_in_its_scope() {
    // A terminate end must cancel an open SIGNAL subscription in the torn-down
    // scope — before, scope teardown cancelled only message subscriptions, so a
    // later broadcast could fire a token in a dead scope.
    //
    //  sub: sub_start -> split =< await(signal catch) -> await_end,
    //                             trigger(svc) -> stop(terminate) >
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .signal_intermediate_catch_event("await", "all-clear")
        .contained_in("await", "sub")
        .end_event("await_end")
        .contained_in("await_end", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("stop")
        .contained_in("stop", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "await")
        .connect("await", "await_end")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .connect("sub", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    // `signal_subscriptions()` retains cancelled records, so count only OPEN ones.
    let open_subs = |e: &Engine| {
        e.signal_subscriptions()
            .iter()
            .filter(|s| s.state == crate::state::MessageSubscriptionState::Open)
            .count()
    };
    assert_eq!(open_subs(&engine), 1);

    let events = complete_one(&mut engine, "trigger-job");
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::SignalSubscriptionCanceled { .. })));
    assert_eq!(open_subs(&engine), 0);
}

#[test]
fn subprocess_terminate_end_clears_a_multi_instance_body_in_its_scope() {
    // A terminate end must drop the runtime record of a multi-instance body torn
    // down with its scope. `ElementCompleted` removes the body token but leaves
    // `multi_instances` populated (cleared only by `MultiInstanceCompleted`), so
    // without explicit teardown the terminated scope keeps a stale active-child
    // set — and the dead-scope guard would read it as a live loop.
    //
    //  sub: sub_start -> split =< each(MI svc "handle") -> each_end,
    //                             trigger(svc) -> stop(terminate) >
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
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
        .contained_in("each", "sub")
        .end_event("each_end")
        .contained_in("each_end", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("stop")
        .contained_in("stop", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "each")
        .connect("each", "each_end")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .connect("sub", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    // The multi-instance body registered a runtime record on activation.
    assert_eq!(engine.instance(key).unwrap().multi_instances.len(), 1);

    // The sibling terminate fires when `trigger` completes.
    let events = complete_one(&mut engine, "trigger-job");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::MultiInstanceCompleted { .. })),
        "scope teardown must emit MultiInstanceCompleted for the torn-down body"
    );
    assert!(
        engine.instance(key).unwrap().multi_instances.is_empty(),
        "the multi-instance runtime record must be gone after terminate"
    );
}

/// Issue #1170 — regression guard, service-task multi-instance child.
///
/// An interrupting boundary event on a multi-instance activity is armed on the
/// BODY and must tear down the WHOLE loop: every active child (here, each MI
/// child's job), the body's `MultiInstanceState` runtime record, and route the
/// boundary's outgoing flow exactly once. Before the fix,
/// `interrupt_activity_via_boundary` had no multi-instance branch, so the body's
/// `active` set kept the (now-cancelled) child keys and the instance hung
/// `Active` forever after the boundary flow finished.
#[test]
fn interrupting_boundary_on_a_multi_instance_service_task_tears_down_the_whole_loop() {
    // start -> each(MI svc "work") --normal--> done
    // each --(interrupting timer boundary "timeout")--> escalated
    let def = ProcessBuilder::new("mi-svc")
        .start_event("start")
        .service_task("each", "work")
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
        .timer_boundary_event("timeout", "each", 5_000)
        .end_event("done")
        .end_event("escalated")
        .connect("start", "each")
        .connect("each", "done")
        .connect("timeout", "escalated")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-svc",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Two parallel children, each with a "work" job; the body registered its
    // multi-instance runtime record and armed the boundary timer on itself.
    let job_keys: Vec<_> = engine.pending_jobs().iter().map(|j| j.key).collect();
    assert_eq!(job_keys.len(), 2, "two MI children each minted a job");
    assert_eq!(engine.instance(key).unwrap().multi_instances.len(), 1);
    assert!(engine.timers().iter().any(|t| t.element_id == "each"));

    // The timer interrupts the whole loop.
    let fired = engine.trigger_timers(5_000);
    for job_key in &job_keys {
        assert_eq!(
            engine.state().jobs[job_key].state,
            state::JobState::Canceled,
            "every MI child job is cancelled by the interrupting boundary"
        );
    }
    assert_eq!(
        fired
            .iter()
            .filter(|e| matches!(
                e,
                Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "escalated"
            ))
            .count(),
        1,
        "the boundary flow is taken exactly once"
    );
    assert!(
        !fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "done"
        )),
        "the normal outgoing flow is never taken"
    );
    assert!(
        engine.instance(key).is_none() || engine.instance(key).unwrap().multi_instances.is_empty(),
        "the multi-instance runtime record is cleared (no stale active set)"
    );
    assert!(
        engine.is_completed(key),
        "the instance completes after the loop is torn down (#1170)"
    );
}

/// Issue #1170 — regression guard, embedded-sub-process multi-instance child.
///
/// Each MI child opens its own inner token scope with a running job. The
/// interrupting boundary on the body must terminate every child scope (and its
/// inner jobs), clear the body record, and route the boundary flow once.
#[test]
fn interrupting_boundary_on_a_multi_instance_subprocess_tears_down_every_child_scope() {
    // start -> each(MI sub[ sub_start -> inner("work") -> sub_end ]) --normal--> done
    // each --(interrupting timer boundary "timeout")--> escalated
    let def = ProcessBuilder::new("mi-sub")
        .start_event("start")
        .sub_process("each", "sub_start")
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
        .start_event("sub_start")
        .contained_in("sub_start", "each")
        .service_task("inner", "work")
        .contained_in("inner", "each")
        .end_event("sub_end")
        .contained_in("sub_end", "each")
        .timer_boundary_event("timeout", "each", 5_000)
        .end_event("done")
        .end_event("escalated")
        .connect("start", "each")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("each", "done")
        .connect("timeout", "escalated")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-sub",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let job_keys: Vec<_> = engine.pending_jobs().iter().map(|j| j.key).collect();
    assert_eq!(
        job_keys.len(),
        2,
        "each MI sub-process child runs an inner job"
    );
    assert_eq!(engine.instance(key).unwrap().multi_instances.len(), 1);

    let fired = engine.trigger_timers(5_000);
    for job_key in &job_keys {
        assert_eq!(
            engine.state().jobs[job_key].state,
            state::JobState::Canceled,
            "every inner job is cancelled with its child scope"
        );
    }
    assert_eq!(
        fired
            .iter()
            .filter(|e| matches!(
                e,
                Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "escalated"
            ))
            .count(),
        1,
        "the boundary flow is taken exactly once"
    );
    assert!(
        !fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "done"
        )),
        "the normal outgoing flow is never taken"
    );
    assert!(
        engine.instance(key).is_none() || engine.instance(key).unwrap().multi_instances.is_empty(),
        "the multi-instance runtime record is cleared"
    );
    assert!(engine.is_completed(key), "the instance completes (#1170)");
}

/// Issue #1170 — regression guard, call-activity multi-instance child.
///
/// Each MI child spawns its own child process instance (Zeebe parity). The
/// interrupting boundary on the body must cancel every spawned child instance,
/// clear the body record, and route the boundary flow once.
#[test]
fn interrupting_boundary_on_a_multi_instance_call_activity_cancels_every_child_instance() {
    // callee: cs -> ct("work") -> ce
    let callee = ProcessBuilder::new("callee")
        .start_event("cs")
        .service_task("ct", "work")
        .end_event("ce")
        .connect("cs", "ct")
        .connect("ct", "ce")
        .build()
        .unwrap();
    // start -> each(MI call "callee") --normal--> done
    // each --(interrupting timer boundary "timeout")--> escalated
    let def = ProcessBuilder::new("mi-call")
        .start_event("start")
        .call_activity("each", "callee")
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
        .timer_boundary_event("timeout", "each", 5_000)
        .end_event("done")
        .end_event("escalated")
        .connect("start", "each")
        .connect("each", "done")
        .connect("timeout", "escalated")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(callee))
        .unwrap();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-call",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Each MI child spawned a "callee" child instance parked on its "work" job.
    let job_keys: Vec<_> = engine.pending_jobs().iter().map(|j| j.key).collect();
    assert_eq!(
        job_keys.len(),
        2,
        "two child process instances, each on a job"
    );
    assert_eq!(engine.instance(key).unwrap().multi_instances.len(), 1);

    let fired = engine.trigger_timers(5_000);
    assert!(
        fired
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceTerminated { .. })),
        "the spawned child process instances are terminated"
    );
    for job_key in &job_keys {
        assert_eq!(
            engine.state().jobs[job_key].state,
            state::JobState::Canceled,
            "each child instance's job is cancelled"
        );
    }
    assert_eq!(
        fired
            .iter()
            .filter(|e| matches!(
                e,
                Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "escalated"
            ))
            .count(),
        1,
        "the boundary flow is taken exactly once"
    );
    assert!(
        !fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "done"
        )),
        "the normal outgoing flow is never taken"
    );
    assert!(
        engine.instance(key).is_none() || engine.instance(key).unwrap().multi_instances.is_empty(),
        "the multi-instance runtime record is cleared"
    );
    assert!(
        engine.is_completed(key),
        "the parent instance completes (#1170)"
    );
}

/// Issue #1170 — regression guard, ad-hoc (JOB_WORKER) multi-instance child.
///
/// Each MI child is a JOB_WORKER `adHocSubProcess`: it mints an agent job and
/// registers its own ad-hoc runtime record, into which the agent can activate
/// tools. The interrupting boundary on the body must tear down every child's
/// ad-hoc scope (its agent job, activated tools and their inner instances),
/// clear both the ad-hoc records and the body record, and route the boundary
/// flow once.
#[test]
fn interrupting_boundary_on_a_multi_instance_adhoc_tears_down_every_container() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="Host">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="probe-agent" />
              <zeebe:adHoc outputCollection="r" outputElement="=toolCallResult" />
            </bpmn:extensionElements>
            <bpmn:multiInstanceLoopCharacteristics>
              <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
            </bpmn:multiInstanceLoopCharacteristics>
            <bpmn:userTask id="InnerTask">
              <bpmn:extensionElements>
                <zeebe:userTask />
              </bpmn:extensionElements>
            </bpmn:userTask>
          </bpmn:adHocSubProcess>
          <bpmn:boundaryEvent id="Bnd" attachedToRef="Host">
            <bpmn:messageEventDefinition messageRef="M" />
          </bpmn:boundaryEvent>
          <bpmn:endEvent id="EndNormal" />
          <bpmn:endEvent id="EndInterrupted" />
          <bpmn:sequenceFlow id="F1" sourceRef="s" targetRef="Host" />
          <bpmn:sequenceFlow id="F2" sourceRef="Host" targetRef="EndNormal" />
          <bpmn:sequenceFlow id="F3" sourceRef="Bnd" targetRef="EndInterrupted" />
        </bpmn:process>
        <bpmn:message id="M" name="probe-cancel">
          <bpmn:extensionElements>
            <zeebe:subscription correlationKey="=customerId" />
          </bpmn:extensionElements>
        </bpmn:message>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[
                ("customerId", Value::Str("C1".into())),
                ("items", Value::List(vec![Value::Int(1), Value::Int(2)])),
            ]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Each MI child is its own ad-hoc container with its own agent job.
    let agents = engine.activate_jobs("probe-agent", "W", 10, 1_000, 0);
    assert_eq!(
        agents.len(),
        2,
        "each MI ad-hoc child minted its own agent job"
    );
    assert_eq!(engine.instance(inst).unwrap().multi_instances.len(), 1);
    // One container activates an inner user-task tool into its own scope.
    let container = agents[0].element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agents[0].key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("InnerTask")],
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
        1,
        "the tool is active in its container before the boundary fires"
    );
    assert!(!engine.is_completed(inst));

    // The cancel message fires the interrupting boundary on the whole loop.
    let fired = engine.correlate_message("probe-cancel", "C1", HashMap::new(), 0);
    assert_eq!(
        fired
            .iter()
            .filter(|e| matches!(
                e,
                Event::SequenceFlowTaken { from, to, .. } if from == "Bnd" && to == "EndInterrupted"
            ))
            .count(),
        1,
        "the boundary flow is taken exactly once"
    );
    assert!(
        engine.is_completed(inst),
        "the instance reaches EndInterrupted and completes (#1170)"
    );
    assert!(
        engine.instance(inst).is_none()
            || engine.instance(inst).unwrap().multi_instances.is_empty(),
        "the multi-instance runtime record is cleared (no stale active set)"
    );
    assert!(
        engine.instance(inst).is_none()
            || engine.instance(inst).unwrap().adhoc_instances.is_empty(),
        "every ad-hoc container record was cleared on cancel"
    );
    assert!(
        engine
            .state()
            .user_tasks
            .values()
            .all(|t| t.element_id != "InnerTask" || t.state != state::UserTaskState::Created),
        "the activated tool's open user task was cancelled, not left Created"
    );
}

/// Defect-class guard (dead-scope guard, `Step::Activate` branch): a scoped
/// terminate tears down every descendant token but deliberately leaves the
/// enclosing sub-process token active until the post-drain completion sweep. A
/// sibling branch's `Step::Activate` still queued in the same drain must NOT be
/// allowed to recreate a token inside that already-terminated scope (it would
/// keep the sub-process from ever draining — a wedge). The sub-process key is
/// still in `active`, so the guard needs the per-drain `torn_down_scopes` marker,
/// not just the `active`/MI/ad-hoc maps.
#[test]
fn dead_scope_guard_rejects_activation_into_a_torn_down_subprocess_scope() {
    // start -> sub -> after, with an inner task so `sub` is a live active scope.
    let def = ProcessBuilder::new("sub-live")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "inner-job")
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .service_task("after", "after-job")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "after")
        .connect("after", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("sub-live"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // `sub` is active (its inner task is parked on a job): find its element
    // instance key — the scope the inner tokens run in.
    let sub_eik = *engine
        .instance(key)
        .unwrap()
        .active
        .iter()
        .find(|(_, element_id)| *element_id == "sub")
        .map(|(eik, _)| eik)
        .expect("sub scope is active");

    let sibling_activation = Step::Activate {
        instance_key: key,
        element_id: "inner".to_string(),
        scope: sub_eik,
    };

    // While the scope is live the guard admits the activation.
    assert!(
        !engine.step_targets_dead_scope(&sibling_activation),
        "a live sub-process scope must admit a queued activation"
    );

    // Once the scope is marked torn down for this drain, the guard rejects the
    // still-queued sibling activation even though the sub-process token is still
    // in `active` (left for the drain sweep to complete).
    engine.torn_down_scopes.insert(sub_eik);
    assert!(
        engine.step_targets_dead_scope(&sibling_activation),
        "a queued activation into a torn-down scope must be dropped, not recreate a token that wedges the drain"
    );

    // A root-scoped activation (scope 0) is never gated by this marker.
    assert!(!engine.step_targets_dead_scope(&Step::Activate {
        instance_key: key,
        element_id: "after".to_string(),
        scope: 0,
    }));
}

/// Defect-class guard (dead-scope guard, multi-instance retry branches): a
/// resolved MI input-mapping incident re-drives the *already-activated* child /
/// body via `RetryMiChildActivation` / `RetryMiBodyActivation`, reusing existing
/// keys. If a scoped terminate tore the loop's body (and children) down first,
/// those queued retries must be dropped — dispatching them would re-drive work in
/// a dead scope. A bare instance-terminal check is insufficient because the
/// instance itself stays active.
#[test]
fn dead_scope_guard_rejects_mi_retries_after_the_body_is_torn_down() {
    //  sub: sub_start -> split =< each(MI svc "handle"), trigger(svc) -> stop(terminate) >
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
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
        .contained_in("each", "sub")
        .end_event("each_end")
        .contained_in("each_end", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("stop")
        .contained_in("stop", "sub")
        .service_task("after", "after-job")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "each")
        .connect("each", "each_end")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .connect("sub", "after")
        .connect("after", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The MI body is active with its children parked on jobs. Capture the body
    // and one child element-instance key.
    let body_key = *engine
        .instance(key)
        .unwrap()
        .multi_instances
        .keys()
        .next()
        .expect("MI body registered");
    let child_key = *engine
        .instance(key)
        .unwrap()
        .scopes
        .iter()
        .find(|(_, parent)| **parent == body_key)
        .map(|(child, _)| child)
        .expect("MI child active under the body");

    let child_retry = Step::RetryMiChildActivation {
        instance_key: key,
        element_id: "each".to_string(),
        body_key,
        child_key,
        index: 0,
    };
    let body_retry = Step::RetryMiBodyActivation {
        instance_key: key,
        element_id: "each".to_string(),
        body_key,
        scope: body_key,
    };

    // While the loop is live the guard admits both retries.
    assert!(!engine.step_targets_dead_scope(&child_retry));
    assert!(!engine.step_targets_dead_scope(&body_retry));

    // The sibling terminate tears the whole sub-process (and its MI body) down.
    complete_one(&mut engine, "trigger-job");
    assert!(engine.instance(key).unwrap().multi_instances.is_empty());
    assert_eq!(
        engine.instance(key).unwrap().state,
        crate::state::ProcessInstanceState::Active,
        "the parent instance continues past the scoped terminate"
    );

    // Now the queued retries target a dead body: the guard must drop both, even
    // though the instance is still active.
    assert!(
        engine.step_targets_dead_scope(&child_retry),
        "a child retry whose body was torn down must be dropped"
    );
    assert!(
        engine.step_targets_dead_scope(&body_retry),
        "a body retry whose body token was torn down must be dropped"
    );
}

/// Defect-class guard (dead-scope guard, `AdvanceTaskListener` branch):
/// `UserTaskCanceled` intentionally *retains* the task record (state → `Canceled`,
/// `pending` cleared). A bare existence check would let an `AdvanceTaskListener`
/// queued before a terminate teardown mint another listener job against the dead
/// task, so the guard must require the task to still be `Created`.
#[test]
fn dead_scope_guard_rejects_task_listener_advance_for_a_canceled_task() {
    //  sub: sub_start -> split =< ut(user task), trigger(svc) -> stop(terminate) >
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .user_task("ut")
        .contained_in("ut", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("stop")
        .contained_in("stop", "sub")
        .service_task("after", "after-job")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "ut")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .connect("sub", "after")
        .connect("after", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let ut_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .expect("user task created");

    let advance = Step::AdvanceTaskListener {
        user_task_key: ut_key,
        event_type: crate::model::TaskListenerEventType::Completing,
        index: 0,
    };

    // While the task is `Created` the guard admits the listener advance.
    assert!(!engine.step_targets_dead_scope(&advance));

    // The sibling terminate cancels the resting task (record retained, Canceled).
    complete_one(&mut engine, "trigger-job");
    assert_eq!(
        engine.state().user_tasks[&ut_key].state,
        crate::state::UserTaskState::Canceled
    );
    assert_eq!(
        engine.instance(key).unwrap().state,
        crate::state::ProcessInstanceState::Active
    );

    // A listener advance queued before the cancel must now be dropped rather than
    // mint another listener job against the cancelled task.
    assert!(
        engine.step_targets_dead_scope(&advance),
        "an AdvanceTaskListener for a cancelled task must be dropped"
    );
}

#[test]
fn subprocess_terminate_end_resolves_an_incident_in_its_scope() {
    // A terminate end must resolve an incident parked on a descendant of the
    // torn-down scope. `ElementCompleted` touches no incident state, so without
    // this the instance keeps a stale `hasIncident` for a vanished element and a
    // later external resolve could re-drive the dead token.
    //
    //  sub: sub_start -> split =< work(svc) -> work_end,
    //                             trigger(svc) -> stop(terminate) >
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .service_task("work", "work-job")
        .contained_in("work", "sub")
        .end_event("work_end")
        .contained_in("work_end", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("stop")
        .contained_in("stop", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "work")
        .connect("work", "work_end")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .connect("sub", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Fail the work job with no retries left to park an incident in the scope.
    let work_job = engine.activate_jobs("work-job", "W", 10, 60_000, 0)[0].key;
    engine
        .apply_command(Command::fail_job(work_job, 0, "boom"))
        .unwrap();
    assert_eq!(engine.instance(key).unwrap().incidents.len(), 1);

    // The sibling terminate fires when `trigger` completes.
    let events = complete_one(&mut engine, "trigger-job");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::IncidentResolved { .. })),
        "scope teardown must resolve the descendant incident"
    );
    assert!(
        engine.instance(key).unwrap().incidents.is_empty(),
        "the instance must carry no active incident after terminate"
    );
}

#[test]
fn scope_teardown_does_not_resurrect_a_cancelled_incident_job() {
    // Regression: forced scope teardown emits `JobCanceled` and then
    // `IncidentResolved` for the *same* failed job in one batch. `IncidentResolved`
    // must not return the just-cancelled job to `Created`, or the resolution
    // resurrects a job whose element instance is being removed — leaving an
    // activatable job pointing at a dead token. The job must stay `Canceled` and
    // out of the activatable pool.
    //
    //  sub: sub_start -> split =< work(svc) -> work_end,
    //                             trigger(svc) -> stop(terminate) >
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .service_task("work", "work-job")
        .contained_in("work", "sub")
        .end_event("work_end")
        .contained_in("work_end", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("stop")
        .contained_in("stop", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "work")
        .connect("work", "work_end")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .connect("sub", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();

    // Fail the work job with no retries left to park an incident on its element.
    let work_job = engine.activate_jobs("work-job", "W", 10, 60_000, 0)[0].key;
    engine
        .apply_command(Command::fail_job(work_job, 0, "boom"))
        .unwrap();
    assert_eq!(
        engine.state().jobs.get(&work_job).unwrap().state,
        state::JobState::Failed
    );

    // The sibling terminate fires when `trigger` completes: it cancels the failed
    // job *and* resolves its incident in the same teardown batch.
    complete_one(&mut engine, "trigger-job");

    let job = engine.state().jobs.get(&work_job).unwrap();
    assert_eq!(
        job.state,
        state::JobState::Canceled,
        "the cancelled job must stay Canceled — incident resolution must not resurrect it"
    );
    assert!(
        engine
            .activate_jobs("work-job", "W2", 10, 60_000, 0)
            .is_empty(),
        "a resurrected job would reappear in the activatable pool against a dead token"
    );
}

#[test]
fn terminate_clears_open_parallel_join_bookkeeping() {
    // A terminate that fires while a parallel join is half-open must drop the
    // join's runtime bookkeeping (`join_counts`/`join_instances`) on the terminal
    // instance — the same forced-teardown cleanup as MI/ad-hoc state. Otherwise a
    // mid-join terminate strands this bookkeeping on the terminal instance shell
    // until eviction, leaving its snapshot inconsistent.
    //
    //  start -> split =< a(svc) -> join, b(svc) -> join,
    //                    trigger(svc) -> stop(terminate) >
    //  join(parallel) -> done
    let proc = ProcessBuilder::new("join-term")
        .start_event("start")
        .parallel_gateway("split")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .parallel_gateway("join")
        .end_event("done")
        .service_task("trigger", "trigger-job")
        .terminate_end_event("stop")
        .connect("start", "split")
        .connect("split", "a")
        .connect("split", "b")
        .connect("split", "trigger")
        .connect("a", "join")
        .connect("b", "join")
        .connect("join", "done")
        .connect("trigger", "stop")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(proc)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("join-term"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Drive branch `a` into the join so it half-opens and waits for branch `b`.
    complete_one(&mut engine, "ja");
    let instance = engine.instance(key).unwrap();
    assert_eq!(
        instance.join_counts.get("join").copied(),
        Some(1),
        "the join is half-open before the terminate"
    );
    assert!(instance.join_instances.contains_key("join"));

    // The sibling terminate fires when `trigger` completes, ending the instance.
    complete_one(&mut engine, "trigger-job");
    let instance = engine.instance(key).unwrap();
    assert_eq!(
        instance.state,
        crate::state::ProcessInstanceState::Completed
    );
    assert!(
        instance.join_counts.is_empty() && instance.join_instances.is_empty(),
        "the terminal instance must not retain open-join bookkeeping"
    );
}

#[test]
fn top_level_terminate_end_clears_a_multi_instance_body() {
    // A top-level terminate ends the whole instance as COMPLETED (#1085); its
    // `ProcessInstanceCompleted` reducer must also drop the multi-instance runtime
    // record, or a large in-flight loop keeps its item/output payload on the
    // terminal instance until eviction.
    //
    //  start -> split =< each(MI svc "handle") -> each_end,
    //                    trigger(svc) -> stop(terminate) >
    let proc = ProcessBuilder::new("mi-term")
        .start_event("start")
        .parallel_gateway("split")
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
        .end_event("each_end")
        .service_task("trigger", "trigger-job")
        .terminate_end_event("stop")
        .connect("start", "split")
        .connect("split", "each")
        .connect("each", "each_end")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(proc)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-term",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.instance(key).unwrap().multi_instances.len(), 1);

    let events = complete_one(&mut engine, "trigger-job");
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })));
    assert!(
        engine.instance(key).unwrap().multi_instances.is_empty(),
        "terminal teardown must clear the multi-instance runtime record"
    );
}

#[test]
fn top_level_terminate_end_clears_an_adhoc_container() {
    // The ad-hoc analogue of the multi-instance case: a top-level terminate must
    // drop the ad-hoc container runtime record on the terminal (COMPLETED) transition.
    //
    //  s -> split =< agent(adhoc, resting) , trigger(svc) -> stop(terminate) >
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:parallelGateway id="split" />
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
          </bpmn:adHocSubProcess>
          <bpmn:serviceTask id="trigger">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="trigger-job" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="agent_end" />
          <bpmn:endEvent id="stop"><bpmn:terminateEventDefinition /></bpmn:endEvent>
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="split" />
          <bpmn:sequenceFlow id="f2" sourceRef="split" targetRef="agent" />
          <bpmn:sequenceFlow id="f3" sourceRef="split" targetRef="trigger" />
          <bpmn:sequenceFlow id="f4" sourceRef="agent" targetRef="agent_end" />
          <bpmn:sequenceFlow id="f5" sourceRef="trigger" targetRef="stop" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let proc = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(proc)).unwrap();
    let created = engine.apply_command(Command::create_instance("p")).unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    // The ad-hoc container registered a runtime record on activation and rests
    // waiting for its `agent-worker` job.
    assert_eq!(engine.instance(key).unwrap().adhoc_instances.len(), 1);

    let events = complete_one(&mut engine, "trigger-job");
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })));
    assert!(
        engine.instance(key).unwrap().adhoc_instances.is_empty(),
        "terminal teardown must clear the ad-hoc container runtime record"
    );
}

#[test]
fn top_level_terminate_end_resolves_a_root_scope_incident_and_completes() {
    // #1085: a top-level terminate end completes the instance (COMPLETED, not
    // TERMINATED). Its `ProcessInstanceCompleted` reducer deliberately does NOT
    // close incidents (a normal completion may retain one), so the terminate end
    // must resolve any incident open on the instance itself — otherwise the
    // dead, completed instance keeps a stale `hasIncident`. The cancelled job's
    // incident resolution must also not resurrect the job.
    //
    //  start -> split =< work(svc, failed w/ incident), trigger(svc) -> stop(terminate) >
    let proc = ProcessBuilder::new("term-incident")
        .start_event("start")
        .parallel_gateway("split")
        .service_task("work", "work-job")
        .service_task("trigger", "trigger-job")
        .terminate_end_event("stop")
        .connect("start", "split")
        .connect("split", "work")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(proc)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("term-incident"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Fail the work job with no retries to park an incident on its element.
    let work_job = engine.activate_jobs("work-job", "W", 10, 60_000, 0)[0].key;
    engine
        .apply_command(Command::fail_job(work_job, 0, "boom"))
        .unwrap();
    assert_eq!(engine.active_incidents().len(), 1);

    // The sibling terminate fires when `trigger` completes: it cancels the failed
    // job, resolves its incident, and completes the whole instance.
    let events = complete_one(&mut engine, "trigger-job");
    assert!(events.contains(&Event::JobCanceled {
        job_key: work_job,
        instance_key: key,
    }));
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::IncidentResolved { .. })));
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })));

    // The instance is COMPLETED with no lingering active incident, and the
    // cancelled job was not resurrected by the incident resolution.
    assert!(engine.is_completed(key));
    assert!(engine.active_incidents().is_empty());
    assert!(engine.instance(key).unwrap().incidents.is_empty());
    assert_eq!(
        engine.state().jobs.get(&work_job).unwrap().state,
        state::JobState::Canceled
    );
    assert!(engine
        .activate_jobs("work-job", "W2", 10, 60_000, 0)
        .is_empty());
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
fn should_create_a_user_task_unassigned_when_assignee_expression_is_null() {
    use crate::model::UserTaskProps;
    // Reproduces merlin instance 19153 (#900): a `=FEEL` assignee whose
    // variable is null must create the task UNASSIGNED (assignee None) rather
    // than storing the raw "=maybeNull" literal, which hides the task from
    // assignee-aware operator views. candidateGroups must survive so the task
    // stays claimable by the group.
    let def = ProcessBuilder::new("escalation")
        .start_event("start")
        .user_task_with(
            "review",
            UserTaskProps {
                assignee: Some("=maybeNull".to_string()),
                candidate_groups: Some("operators".to_string()),
                candidate_users: None,
                due_date: Some("=maybeNull".to_string()),
                follow_up_date: Some("=maybeNull".to_string()),
                priority: None,
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

    // maybeNull is explicitly null (intent: escalate unassigned).
    let vars = HashMap::from([("maybeNull".to_string(), Value::Null)]);
    let created = engine
        .apply_command(Command::CreateInstance {
            process_id: "escalation".to_string(),
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
    // The categorical fix: the null-resolving expressions are absent, never the
    // raw "=maybeNull".
    assert_eq!(
        task.assignee, None,
        "null assignee expression must be absent"
    );
    assert_eq!(
        task.due_date, None,
        "null dueDate expression must be absent"
    );
    assert_eq!(
        task.follow_up_date, None,
        "null followUpDate expression must be absent"
    );
    // The candidate group survives, so the unassigned task is still claimable.
    assert_eq!(task.candidate_groups, vec!["operators"]);

    // And it is claimable via assign_user_task (the manual unblock in #900).
    engine
        .apply_command(Command::assign_user_task(user_task_key, "operator-1"))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&user_task_key]
            .assignee
            .as_deref(),
        Some("operator-1")
    );
}

#[test]
fn should_create_a_user_task_assigned_when_assignee_expression_is_a_string() {
    use crate::model::UserTaskProps;
    // The non-null counterpart: the same model with maybeNull = "alice" assigns
    // the task to alice (expression results are unchanged by the null fix).
    let def = ProcessBuilder::new("escalation")
        .start_event("start")
        .user_task_with(
            "review",
            UserTaskProps {
                assignee: Some("=maybeNull".to_string()),
                candidate_groups: Some("operators".to_string()),
                candidate_users: None,
                due_date: None,
                follow_up_date: None,
                priority: None,
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

    let vars = HashMap::from([("maybeNull".to_string(), Value::Str("alice".to_string()))]);
    let created = engine
        .apply_command(Command::CreateInstance {
            process_id: "escalation".to_string(),
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

    assert_eq!(
        engine.state().user_tasks[&user_task_key]
            .assignee
            .as_deref(),
        Some("alice")
    );
}

#[test]
fn should_link_a_user_task_to_its_deployed_form_key() {
    use crate::command::FormResource;
    // A user task declaring a zeebe:formDefinition formId, plus a start form on
    // the process, both referencing deployed forms by id.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="forms-proc">
          <bpmn:startEvent id="s">
            <bpmn:extensionElements>
              <zeebe:formDefinition formId="start-form" />
            </bpmn:extensionElements>
          </bpmn:startEvent>
          <bpmn:userTask id="review">
            <bpmn:extensionElements>
              <zeebe:userTask />
              <zeebe:formDefinition formId="review-form" />
            </bpmn:extensionElements>
          </bpmn:userTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
          <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    assert_eq!(def.start_form_id.as_deref(), Some("start-form"));

    let form = |id: &str| FormResource {
        id: id.to_string(),
        resource_name: format!("{id}.form"),
        schema: format!(r#"{{"id":"{id}","type":"default","components":[]}}"#),
    };

    let mut engine = Engine::new();
    // Deploy the user-task form first so it is resolvable at task creation.
    let form_events = engine
        .apply_command(Command::DeployForms(vec![form("review-form")]))
        .unwrap();
    let review_form_key = form_events
        .iter()
        .find_map(|e| match e {
            Event::FormDeployed { form_key, .. } => Some(*form_key),
            _ => None,
        })
        .expect("review-form deployed");
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let created = engine
        .apply_command(Command::create_instance("forms-proc"))
        .unwrap();
    let (user_task_key, event_form_key) = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated {
                user_task_key,
                form_key,
                ..
            } => Some((*user_task_key, *form_key)),
            _ => None,
        })
        .expect("user task created");
    assert_eq!(
        event_form_key,
        Some(review_form_key),
        "the event carries the resolved form key"
    );
    let task = &engine.state().user_tasks[&user_task_key];
    assert_eq!(task.form_key, Some(review_form_key));
    assert_eq!(task.external_form_reference, None);
}

#[test]
fn should_leave_form_key_none_when_the_form_is_not_deployed() {
    // A user task whose formId references a form that was never deployed: the
    // task is still created, but with no resolved form key.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="noform-proc">
          <bpmn:startEvent id="s" />
          <bpmn:userTask id="review">
            <bpmn:extensionElements>
              <zeebe:userTask />
              <zeebe:formDefinition formId="missing-form" />
            </bpmn:extensionElements>
          </bpmn:userTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
          <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("noform-proc"))
        .unwrap();
    let user_task_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .expect("user task created");
    assert_eq!(engine.state().user_tasks[&user_task_key].form_key, None);
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
fn should_preserve_the_activating_worker_on_a_job_that_fails_with_no_retries() {
    // #959 — a terminal, incident-bearing failure must keep the last activating
    // `worker` so the incident (joined by `jobKey`) can attribute the failure to
    // the worker/host that was running it (Zeebe parity — a failed JobRecord
    // retains its `worker`).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "w1", 10, 60_000, 0)[0].key;

    // when the worker fails it with no retries left
    let events = engine
        .apply_command(Command::fail_job(job_key, 0, "boom"))
        .unwrap();

    // then the parked job still reports its activating worker
    let job = engine.job(job_key).unwrap();
    assert_eq!(job.state, state::JobState::Failed);
    assert_eq!(job.worker.as_deref(), Some("w1"));

    // and an incident is raised referencing this job's key
    assert!(events.iter().any(|e| matches!(
        e,
        Event::IncidentRaised { job_key: Some(k), .. } if *k == job_key
    )));
}

#[test]
fn should_drop_the_worker_when_a_failed_job_returns_to_the_activatable_pool() {
    // #959 — with retries remaining the job is genuinely no longer held (it goes
    // back to the activatable pool), so the activating `worker` must be cleared.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "w1", 10, 60_000, 0)[0].key;

    engine
        .apply_command(Command::fail_job(job_key, 2, "transient"))
        .unwrap();

    let job = engine.job(job_key).unwrap();
    assert_eq!(job.state, state::JobState::Created);
    assert_eq!(job.worker, None);
}

#[test]
fn should_preserve_the_activating_worker_on_a_job_that_throws_an_error_terminally() {
    // #959 — throwError with no catching boundary parks the job in `Errored` and
    // raises an incident; the activating `worker` must be retained for attribution
    // (Zeebe parity — throwError keeps the record incl. `worker`).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "w1", 10, 60_000, 0)[0].key;

    engine
        .apply_command(Command::throw_job_error(job_key, "UNCAUGHT", "kaboom"))
        .unwrap();

    let job = engine.job(job_key).unwrap();
    assert_eq!(job.state, state::JobState::Errored);
    assert_eq!(job.worker.as_deref(), Some("w1"));
}

#[test]
fn should_recover_the_activating_worker_of_a_terminally_failed_job_via_replay() {
    // #959 — activation is a volatile lease that is *not* journaled/exported, so a
    // restart replays `JobCreated` then the terminal event with no intervening
    // `JobActivated`. The worker is therefore carried *on* the terminal event so
    // it survives replay (durable across restart), not merely held in live state.
    let mut engine = Engine::new();
    let mut log: Vec<Event> = Vec::new();
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
    let job_key = engine.activate_jobs("payment", "w1", 10, 60_000, 0)[0].key;
    let fail_log = engine
        .apply_command(Command::fail_job(job_key, 0, "boom"))
        .unwrap();

    // The exported terminal event carries the activating worker (and, being
    // volatile, `activate_jobs` emitted no `JobActivated` into the durable log).
    assert!(fail_log.iter().any(|e| matches!(
        e,
        Event::JobFailed { worker: Some(w), .. } if w == "w1"
    )));
    log.extend(fail_log);
    assert!(!log.iter().any(|e| matches!(e, Event::JobActivated { .. })));

    // Replaying the durable stream — exactly as after a restart, with the volatile
    // activation lock gone — still attributes the parked job to `w1`.
    let mut replayed = State::new();
    for event in &log {
        state::apply(&mut replayed, event);
    }
    let job = replayed.jobs.get(&job_key).unwrap();
    assert_eq!(job.state, state::JobState::Failed);
    assert_eq!(job.worker.as_deref(), Some("w1"));
}

#[test]
fn should_recover_the_activating_worker_of_a_terminally_errored_job_via_replay() {
    // #959 — mirror of the failed-job replay guard for the `throwError` path: an
    // uncaught thrown error parks the job in `Errored`, and (like `JobFailed`) the
    // activating `worker` is carried *on* `JobErrorThrown` so it survives a restart
    // replay where the volatile, unexported `JobActivated` lock is gone. This guards
    // against a serialization/replay regression silently making errored jobs
    // anonymous.
    let mut engine = Engine::new();
    let mut log: Vec<Event> = Vec::new();
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
    let job_key = engine.activate_jobs("payment", "w1", 10, 60_000, 0)[0].key;
    let throw_log = engine
        .apply_command(Command::throw_job_error(job_key, "UNCAUGHT", "kaboom"))
        .unwrap();

    // The exported terminal event carries the activating worker (and, being
    // volatile, `activate_jobs` emitted no `JobActivated` into the durable log).
    assert!(throw_log.iter().any(|e| matches!(
        e,
        Event::JobErrorThrown { worker: Some(w), .. } if w == "w1"
    )));
    log.extend(throw_log);
    assert!(!log.iter().any(|e| matches!(e, Event::JobActivated { .. })));

    // Replaying the durable stream — exactly as after a restart, with the volatile
    // activation lock gone — still attributes the errored job to `w1`.
    let mut replayed = State::new();
    for event in &log {
        state::apply(&mut replayed, event);
    }
    let job = replayed.jobs.get(&job_key).unwrap();
    assert_eq!(job.state, state::JobState::Errored);
    assert_eq!(job.worker.as_deref(), Some("w1"));
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

/// start -> book(job) -> throw(compensation) -> done
///            \--(compensation boundary "book-comp")..association..> cancel(job)
///                                                    isForCompensation
fn process_with_compensation() -> ProcessDefinition {
    ProcessBuilder::new("trip")
        .start_event("s")
        .service_task("book", "book-job")
        .compensation_boundary_event("book-comp", "book", "cancel")
        .service_task("cancel", "cancel-job")
        .compensation_throw_event("throw")
        .end_event("done")
        .connect("s", "book")
        .connect("book", "throw")
        .connect("throw", "done")
        .build()
        .unwrap()
}

#[test]
fn should_run_the_compensation_handler_when_a_compensation_throw_event_fires() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_compensation()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("trip"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Complete the compensable activity: it becomes compensable and the token
    // advances to the compensation throw event, which triggers its handler.
    let book = engine.activate_jobs("book-job", "w", 1, 60_000, 0)[0].key;
    let events = engine.apply_command(Command::complete_job(book)).unwrap();
    // the handler was activated (its job now exists) but the instance is not
    // done — the throw event rests until the handler completes
    assert!(!engine.is_completed(instance_key));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::CompensationSubscriptionCreated { element_id, handler, .. }
            if element_id == "book" && handler == "cancel"
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::CompensationTriggered { throw_element_id, handlers, .. }
            if throw_element_id == "throw" && handlers == &vec!["cancel".to_string()]
    )));
    // the throw's outgoing flow has NOT been taken yet
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::SequenceFlowTaken { to, .. } if to == "done")));

    // Complete the compensation handler's job: the throw event completes, routes
    // to `done`, and the instance finishes.
    let cancel = engine.activate_jobs("cancel-job", "w", 1, 60_000, 0)[0].key;
    let events = engine.apply_command(Command::complete_job(cancel)).unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::CompensationHandlerCompleted { handler_element_id, .. }
            if handler_element_id == "cancel"
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "throw" && to == "done"
    )));
    assert!(engine.is_completed(instance_key));
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
}

#[test]
fn should_pass_through_a_compensation_throw_event_with_nothing_to_compensate() {
    // start -> throw(compensation) -> done, with a compensable activity that is
    // never reached, so the throw finds nothing to compensate.
    let def = ProcessBuilder::new("noop-comp")
        .start_event("s")
        .compensation_throw_event("throw")
        .end_event("done")
        .connect("s", "throw")
        .connect("throw", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("noop-comp"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // the throw is a pass-through: it routes straight to `done` and completes
    assert!(engine.is_completed(instance_key));
    assert!(created.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "throw" && to == "done"
    )));
    assert!(!created
        .iter()
        .any(|e| matches!(e, Event::CompensationTriggered { .. })));
}

/// Defect class: a *sub-process*-scoped terminate must also drop the compensation
/// state scoped to the torn-down scope. A completed compensable activity records
/// a `compensable` subscription tagged with the scope it ran in; the whole-
/// instance `ProcessInstanceTerminated` sweep clears both compensation maps, but
/// a scoped terminate only removes the tokens — leaving the subscription attached
/// to the still-live parent until whole-instance eviction (stale state, and a
/// handler that could run for an activity in a scope that no longer exists).
#[test]
fn subprocess_terminate_clears_scoped_compensation_state() {
    //  main: start -> sub -> after(svc) -> done
    //  sub:  sub_start -> split =< book(svc, comp-boundary "book-comp" -> "cancel")
    //                                     -> hold(svc),
    //                             trigger(svc) -> inner_stop(terminate) >
    let def = ProcessBuilder::new("comp-term")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .service_task("book", "book-job")
        .contained_in("book", "sub")
        .compensation_boundary_event("book-comp", "book", "cancel")
        .contained_in("book-comp", "sub")
        .service_task("cancel", "cancel-job")
        .contained_in("cancel", "sub")
        .service_task("hold", "hold-job")
        .contained_in("hold", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("inner_stop")
        .contained_in("inner_stop", "sub")
        .service_task("after", "after-job")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "book")
        .connect("book", "hold")
        .connect("split", "trigger")
        .connect("trigger", "inner_stop")
        .connect("sub", "after")
        .connect("after", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("comp-term"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Complete `book`: it becomes compensable (scoped to the sub-process) and its
    // token advances to `hold`, so the sub-process is still live.
    let events = complete_one(&mut engine, "book-job");
    assert!(events.iter().any(|e| matches!(
        e,
        Event::CompensationSubscriptionCreated { element_id, handler, .. }
            if element_id == "book" && handler == "cancel"
    )));
    assert_eq!(
        engine.instance(instance_key).unwrap().compensable.len(),
        1,
        "the completed compensable activity is recorded"
    );

    // The sibling `trigger` drives its token into the terminate end, tearing down
    // the sub-process scope — which must also clear the scoped compensation state.
    let events = complete_one(&mut engine, "trigger-job");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::ScopedCompensationCleared { .. })),
        "scope teardown must emit ScopedCompensationCleared"
    );
    assert!(
        engine
            .instance(instance_key)
            .unwrap()
            .compensable
            .is_empty(),
        "the compensation subscription scoped to the terminated sub-process must be dropped"
    );
    // The parent continues past the scoped terminate and finishes normally.
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        crate::state::ProcessInstanceState::Active
    );
    let final_events = complete_one(&mut engine, "after-job");
    assert!(final_events.contains(&Event::ProcessInstanceCompleted { instance_key }));
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

    // Camunda validates the requested property after its live-job gate.
    assert_eq!(
        err,
        EngineError::JobUpdateInvalid {
            job_key,
            reason: "timeout requires an active job deadline".into(),
        }
    );
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
        lease_token: None,
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

/// message-start "start"(probe-alert) -> service task "host"(agent), with an
/// interrupting message boundary "bnd"(probe-alert, correlating on customerId)
/// on the host. The SAME message name is subscribed both at the process level
/// (start event) and by the boundary on a running instance.
fn process_message_start_and_boundary() -> ProcessDefinition {
    ProcessBuilder::new("agent")
        .message_start_event("start", "probe-alert")
        .service_task("host", "agent")
        .message_boundary_event("bnd", "host", "probe-alert", "customerId")
        .end_event("running")
        .end_event("interrupted")
        .connect("start", "host")
        .connect("host", "running")
        .connect("bnd", "interrupted")
        .build()
        .unwrap()
}

#[test]
fn message_prefers_open_subscription_over_starting_a_new_instance() {
    // Issue #1156: when a message name is subscribed by BOTH a message start
    // event and an open boundary subscription on a running instance, the open
    // subscription takes precedence — the same publish must NOT also start a
    // duplicate instance.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_message_start_and_boundary()))
        .unwrap();

    // First publish: no subscription is open yet, so the message start event
    // creates a fresh instance. It parks on the service task and opens the
    // boundary subscription (correlating on the seeded customerId).
    engine.correlate_message(
        "probe-alert",
        "C1",
        vars(&[("customerId", Value::Str("C1".into()))]),
        0,
    );
    assert_eq!(engine.state().instances.len(), 1);
    let open: Vec<_> = engine
        .message_subscriptions()
        .into_iter()
        .filter(|s| s.state == state::MessageSubscriptionState::Open)
        .collect();
    assert_eq!(open.len(), 1);
    let instance_key = open[0].instance_key;

    // Second publish: the open boundary subscription on the running instance
    // claims the message. It fires the boundary (interrupting the instance) and
    // does NOT start a second instance from the message start event.
    let fired = engine.correlate_message(
        "probe-alert",
        "C1",
        vars(&[("customerId", Value::Str("C1".into()))]),
        0,
    );
    assert_eq!(
        engine.state().instances.len(),
        1,
        "the open subscription wins; no duplicate instance is created"
    );
    assert!(
        fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "bnd" && to == "interrupted"
        )),
        "the boundary event fires on the running instance"
    );
    assert!(
        !fired
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceCreated { .. })),
        "no new instance is created by the same publish"
    );
    assert!(engine.is_completed(instance_key));
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

// ---- multi-start processes (Zeebe parity, #855) ----
//
// Zeebe permits a process to declare several start events — a none start
// alongside any number of message/timer starts — and every one is *live*: the
// none start accepts CreateInstance, and each typed start opens its own
// deploy-time trigger (subscription / armed timer) that fires an independent
// instance at its own start element.

/// none-start "a_none" -> a_end ; message-start "b_msg"(order-placed) -> b_end
fn process_none_plus_message() -> ProcessDefinition {
    ProcessBuilder::new("dual-start")
        .start_event("a_none")
        .message_start_event("b_msg", "order-placed")
        .end_event("a_end")
        .end_event("b_end")
        .connect("a_none", "a_end")
        .connect("b_msg", "b_end")
        .build()
        .unwrap()
}

#[test]
fn none_plus_message_start_both_function() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_none_plus_message()))
        .unwrap();

    // Deploy wires the message start's subscription (the none start opens none),
    // and creates no instance yet.
    assert_eq!(engine.state().message_start_subscriptions.len(), 1);
    let sub = &engine.state().message_start_subscriptions["order-placed"];
    assert_eq!(sub.process_id, "dual-start");
    assert_eq!(sub.start_element_id, "b_msg");
    assert!(engine.state().instances.is_empty());

    // The none start accepts a CreateInstance and runs to completion.
    let created = engine
        .apply_command(Command::create_instance("dual-start"))
        .unwrap();
    let none_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(engine.is_completed(none_key));

    // The message start ALSO functions: a matching message creates a distinct
    // instance (seeded with the message variables) that runs to completion.
    let fired = engine.correlate_message("order-placed", "", vars(&[("amount", Value::Int(9))]), 0);
    let (msg_key, seeded) = fired
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
    assert_ne!(msg_key, none_key);
    assert_eq!(seeded, Some(Value::Int(9)));
    assert!(engine.is_completed(msg_key));
    assert_eq!(engine.state().instances.len(), 2);
}

/// none-start "a_none" -> a_end ; timer-start "b_timer"(once PT10S) -> b_end
fn process_none_plus_timer() -> ProcessDefinition {
    ProcessBuilder::new("dual-timer")
        .start_event("a_none")
        .timer_start_event_once("b_timer", 10_000)
        .end_event("a_end")
        .end_event("b_end")
        .connect("a_none", "a_end")
        .connect("b_timer", "b_end")
        .build()
        .unwrap()
}

#[test]
fn none_plus_timer_start_both_function() {
    let mut engine = Engine::new();
    // Deploy at t=1000: the timer start is armed for 11000.
    engine
        .apply_command_at(Command::DeployProcess(process_none_plus_timer()), 1_000)
        .unwrap();
    assert_eq!(engine.state().start_timers.len(), 1);
    let timer = engine.state().start_timers.values().next().unwrap();
    assert_eq!(timer.start_element_id, "b_timer");
    assert_eq!(timer.due_at, Some(11_000));
    assert!(engine.state().instances.is_empty());

    // The none start accepts a CreateInstance.
    let created = engine
        .apply_command_at(Command::create_instance("dual-timer"), 2_000)
        .unwrap();
    let none_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(engine.is_completed(none_key));

    // The timer start ALSO fires when due, creating a distinct instance.
    let fired = engine.trigger_timers(11_000);
    let timer_key = fired.iter().find_map(|e| e.instance_key()).unwrap();
    assert_ne!(timer_key, none_key);
    assert!(engine.is_completed(timer_key));
    assert_eq!(engine.state().instances.len(), 2);
}

#[test]
fn two_message_starts_open_two_subscriptions() {
    // Two message starts (distinct names) each open their own subscription and
    // fire at their own start element — no none start required.
    let def = ProcessBuilder::new("two-msg")
        .message_start_event("s_a", "msg-a")
        .message_start_event("s_b", "msg-b")
        .end_event("e_a")
        .end_event("e_b")
        .connect("s_a", "e_a")
        .connect("s_b", "e_b")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    assert_eq!(engine.state().message_start_subscriptions.len(), 2);
    assert_eq!(
        engine.state().message_start_subscriptions["msg-a"].start_element_id,
        "s_a"
    );
    assert_eq!(
        engine.state().message_start_subscriptions["msg-b"].start_element_id,
        "s_b"
    );

    // Each name fires its own start; a non-matching name fires nothing.
    engine.correlate_message("msg-a", "", HashMap::new(), 0);
    engine.correlate_message("msg-b", "", HashMap::new(), 0);
    assert_eq!(engine.state().instances.len(), 2);
}

#[test]
fn message_and_timer_starts_wire_both_triggers() {
    // A message start and a timer start (no none start) each get wired at deploy.
    let def = ProcessBuilder::new("msg-and-timer")
        .message_start_event("m", "kick")
        .timer_start_event_once("t", 5_000)
        .end_event("me")
        .end_event("te")
        .connect("m", "me")
        .connect("t", "te")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command_at(Command::DeployProcess(def), 1_000)
        .unwrap();

    assert_eq!(engine.state().message_start_subscriptions.len(), 1);
    assert_eq!(engine.state().start_timers.len(), 1);

    engine.correlate_message("kick", "", HashMap::new(), 2_000);
    engine.trigger_timers(6_000);
    assert_eq!(engine.state().instances.len(), 2);
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

/// Deploys an orchestrator with a raw (un-inlined) call activity `c1 -> phase`
/// and returns the running engine. The call activity carries the given io
/// mappings so tests can exercise cross-boundary variable propagation.
fn deploy_native_call(io: crate::model::IoMapping, child: ProcessDefinition) -> Engine {
    let mut orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "phase")
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end");
    if !io.is_empty() {
        orchestrator = orchestrator.with_io("c1", io);
    }
    let orchestrator = orchestrator.build().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(child)).unwrap();
    engine
        .apply_command(Command::DeployProcess(orchestrator))
        .unwrap();
    engine
}

#[test]
fn a_call_activity_spawns_a_distinct_child_process_instance_with_parent_linkage() {
    // Native execution (Zeebe parity): a call activity does NOT inline the
    // callee — it spawns a distinct child process instance linked back to the
    // calling instance and its call-activity element instance.
    let mut engine = deploy_native_call(Default::default(), phase_process("phase", "work"));
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");

    // The call-activity element instance the child links back to.
    let call_eik = created
        .iter()
        .find_map(|e| match e {
            Event::ElementActivated {
                element_instance_key,
                element_id,
                ..
            } if element_id == "c1" => Some(*element_instance_key),
            _ => None,
        })
        .expect("c1 activated");

    // A second, distinct process instance was created for the callee, carrying
    // the parent linkage (C8 parentProcessInstanceKey / parentElementInstanceKey).
    let (child_key, ppik, peik) = created
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated {
                instance_key,
                process_id,
                parent_process_instance_key,
                parent_element_instance_key,
                ..
            } if process_id == "phase" => Some((
                *instance_key,
                *parent_process_instance_key,
                *parent_element_instance_key,
            )),
            _ => None,
        })
        .expect("a child instance of 'phase' was created");
    assert_ne!(child_key, parent_key, "child is a distinct instance");
    assert_eq!(ppik, Some(parent_key));
    assert_eq!(peik, Some(call_eik));

    // Both instances are live; the parent's call-activity token is parked on the
    // child, and the child parks on its own (instance-scoped) job.
    assert!(!engine.is_completed(parent_key));
    assert!(!engine.is_completed(child_key));
    assert_eq!(engine.pending_jobs().len(), 1);
    assert_eq!(engine.pending_jobs()[0].instance_key, child_key);
    assert_eq!(
        engine
            .instance(child_key)
            .unwrap()
            .parent_process_instance_key,
        Some(parent_key)
    );

    // Completing the child's job runs it to its (none) end; the parent's
    // call-activity token then completes and routes out to the orchestrator end.
    let events = complete_one(&mut engine, "work");
    assert!(events.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceCompleted { instance_key } if *instance_key == child_key
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "c1" && to == "end"
    )));
    assert!(engine.is_completed(child_key));
    assert!(engine.is_completed(parent_key));
}

#[test]
fn cancelling_a_call_activity_parent_cancels_its_child() {
    let mut engine = deploy_native_call(Default::default(), phase_process("phase", "work"));
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");
    let child_key = engine
        .pending_jobs()
        .first()
        .expect("child parked on its job")
        .instance_key;
    assert_ne!(child_key, parent_key);

    let cancel = engine
        .apply_command(Command::cancel_instance(parent_key))
        .unwrap();
    // Cancelling the parent cascades to the in-flight child (Zeebe parity).
    assert!(cancel.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceTerminated { instance_key } if *instance_key == parent_key
    )));
    assert!(cancel.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceTerminated { instance_key } if *instance_key == child_key
    )));
    assert_eq!(
        engine.instance(parent_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminated
    );
    assert_eq!(
        engine.instance(child_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminated
    );
    // The child's job was cancelled with it — no activatable jobs remain.
    assert!(engine.pending_jobs().is_empty());
}

#[test]
fn interrupting_a_call_activity_via_a_boundary_cancels_its_child() {
    // A boundary event that interrupts a call activity must also cancel the
    // child process instance it spawned. Otherwise the parent token leaves via
    // the boundary flow while the child keeps running — an orphan with no
    // parent token left to complete it. The parent instance itself is NOT
    // terminated; it routes out the interrupting boundary's flow.
    let orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "phase")
        .timer_boundary_event("timeout", "c1", 5_000)
        .end_event("end")
        .end_event("escalated")
        .connect("start", "c1")
        .connect("c1", "end")
        .connect("timeout", "escalated")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(phase_process("phase", "work")))
        .unwrap();
    engine
        .apply_command(Command::DeployProcess(orchestrator))
        .unwrap();

    let created = engine
        .apply_command_at(Command::create_instance("orch"), 1_000)
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");
    let child_key = engine
        .pending_jobs()
        .first()
        .expect("child parked on its job")
        .instance_key;
    assert_ne!(child_key, parent_key);

    // At the boundary timer's due instant it interrupts the call activity,
    // cancels the child instance, and routes the parent to "escalated".
    let fired = engine.trigger_timers(6_000);
    assert!(
        fired.iter().any(|e| matches!(
            e,
            Event::ProcessInstanceTerminated { instance_key } if *instance_key == child_key
        )),
        "the boundary interrupt cancels the spawned child instead of orphaning it"
    );
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "escalated"
    )));
    assert_eq!(
        engine.instance(child_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminated
    );
    // The parent was not terminated — it completed via the boundary path.
    assert_ne!(
        engine.instance(parent_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminated
    );
    assert!(engine.is_completed(parent_key));
    // No orphaned job survives.
    assert!(engine.pending_jobs().is_empty());
}

#[test]
fn cancelling_a_parent_reaps_a_child_already_mid_termination() {
    // A cascade cancel is not interruptible, so it must also sweep a child that
    // is already `Terminating` — e.g. a child whose own cancel deferred on a
    // user-task canceling listener. If the parent terminates while the child is
    // mid-drain, sweeping only `Active` children would leave the child stuck in
    // `Terminating` forever (orphaned).
    let child_def = ProcessBuilder::new("phase")
        .start_event("pstart")
        .user_task("review")
        .end_event("pend")
        .connect("pstart", "review")
        .connect("review", "pend")
        .with_task_listeners(
            "review",
            vec![crate::model::TaskListener {
                event_type: crate::model::TaskListenerEventType::Canceling,
                job_type: "onCancel".to_string(),
                retries: None,
            }],
        )
        .build()
        .unwrap();
    let mut engine = deploy_native_call(Default::default(), child_def);
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");
    let child_key = engine
        .state()
        .instances
        .values()
        .find(|i| i.parent_process_instance_key == Some(parent_key))
        .map(|i| i.key)
        .expect("a child instance was spawned");

    // Cancel the child directly: its canceling listener defers termination, so
    // it parks in `Terminating` waiting for the listener job to drain.
    engine
        .apply_command(Command::cancel_instance(child_key))
        .unwrap();
    assert_eq!(
        engine.instance(child_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminating
    );

    // Now cancel the parent. The cascade must force-complete the child's
    // deferred drain rather than leave it orphaned in `Terminating`.
    let cancel = engine
        .apply_command(Command::cancel_instance(parent_key))
        .unwrap();
    assert!(
        cancel.iter().any(|e| matches!(
            e,
            Event::ProcessInstanceTerminated { instance_key } if *instance_key == child_key
        )),
        "the cascade reaps the still-Terminating child"
    );
    assert_eq!(
        engine.instance(child_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminated
    );
    assert!(engine.pending_jobs().is_empty());
}

#[test]
fn a_failing_called_element_expression_raises_an_expression_evaluation_incident() {
    // When `zeebe:calledElement` is a FEEL expression (leading `=`) that cannot
    // be evaluated (missing var, parse error, non-string result), the incident
    // must describe the *expression* failure — not masquerade as an "unknown
    // called process '=…'" lookup miss, which points diagnosis at the wrong
    // thing. Here `=calleeName` references an unbound variable, so evaluation
    // fails and no callee id is ever resolved.
    let orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "=calleeName")
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(phase_process("phase", "work")))
        .unwrap();
    engine
        .apply_command(Command::DeployProcess(orchestrator))
        .unwrap();

    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "the failed expression parks one incident");
    assert_eq!(active[0].kind, state::IncidentKind::ExpressionEvaluation);
    let reason = &active[0].reason;
    assert!(
        reason.contains("calledElement") && reason.contains("=calleeName"),
        "incident should name the failing calledElement expression, got: {reason}"
    );
    assert!(
        !reason.contains("unknown called process"),
        "expression failure must not be reported as an unknown-process lookup, got: {reason}"
    );
    // No child instance was spawned and the parent did not complete.
    assert!(!engine.is_completed(parent_key));
    assert!(engine
        .state()
        .instances
        .values()
        .all(|i| i.parent_process_instance_key != Some(parent_key)));
}

#[test]
fn an_unknown_called_process_raises_a_called_element_incident_not_expression_eval() {
    // A call activity whose (literal) `calledElement` process id is not deployed
    // is a missing-definition / execution problem, NOT a FEEL/type failure. It
    // must be classified as `CalledElementError` (C8 `CALLED_ELEMENT_ERROR`), so
    // clients filtering incidents by `errorType` can distinguish a missing callee
    // from a genuine expression-evaluation failure (`EXTRACT_VALUE_ERROR`).
    let orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "definitely-not-deployed")
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(orchestrator))
        .unwrap();

    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "the unknown callee parks one incident");
    assert_eq!(
        active[0].kind,
        state::IncidentKind::CalledElementError,
        "an unknown called process must be a CalledElementError, not ExpressionEvaluation"
    );
    assert!(
        active[0].reason.contains("unknown called process"),
        "incident should name the missing callee, got: {}",
        active[0].reason
    );
    assert!(!engine.is_completed(parent_key));
}

#[test]
fn a_call_activity_propagates_variables_via_io_mappings_across_isolated_scopes() {
    // The callee is a pass-through (pstart -> pend) so it completes on its seed
    // variables, letting us observe both directions of the mapping in one command.
    // Both Zeebe propagation flags are OFF here, so the *only* channel across the
    // isolated scopes is the ioMapping (the parity defaults are exercised by the
    // `call_activity_*propagate*` tests).
    let child = ProcessBuilder::new("phase")
        .start_event("pstart")
        .end_event("pend")
        .connect("pstart", "pend")
        .build()
        .unwrap();
    let io = crate::model::IoMapping {
        inputs: vec![crate::model::Mapping {
            source: "=orderId".to_string(),
            target: "childOrder".to_string(),
        }],
        outputs: vec![crate::model::Mapping {
            source: "=childOrder".to_string(),
            target: "parentEcho".to_string(),
        }],
    };
    let mut engine = deploy_native_call_with_propagation(io, child, false, false);

    let created = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[("orderId", Value::Int(42))]),
        ))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");

    // The child is seeded ONLY through the input mapping (isolated scope): it
    // sees `childOrder`, not the parent's other variable `orderId`.
    let child_seed = created
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated {
                process_id,
                variables,
                ..
            } if process_id == "phase" => Some(variables.clone()),
            _ => None,
        })
        .expect("child created");
    assert_eq!(child_seed.get("childOrder"), Some(&Value::Int(42)));
    assert!(
        !child_seed.contains_key("orderId"),
        "parent variables must not leak into the child's isolated scope"
    );

    // The whole orchestration ran to completion; the output mapping projected the
    // child's variable back into the parent as `parentEcho`. The child's own
    // `childOrder` did not leak wholesale into the parent.
    assert!(engine.is_completed(parent_key));
    assert!(created.iter().any(|e| matches!(
        e,
        Event::VariablesUpdated { instance_key, variables }
            if *instance_key == parent_key && variables.get("parentEcho") == Some(&Value::Int(42))
    )));
    assert!(
        !created.iter().any(|e| matches!(
            e,
            Event::VariablesUpdated { instance_key, variables }
                if *instance_key == parent_key && variables.contains_key("childOrder")
        )),
        "only the mapped output crosses back, not the child's raw scope"
    );
}

/// Deploys the `orch` orchestrator whose `c1` call activity invokes `phase`
/// (child) with explicit Zeebe `propagateAllParentVariables` /
/// `propagateAllChildVariables` flags, optionally with an ioMapping.
fn deploy_native_call_with_propagation(
    io: crate::model::IoMapping,
    child: ProcessDefinition,
    propagate_all_parent: bool,
    propagate_all_child: bool,
) -> Engine {
    let mut orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity_with_propagation("c1", "phase", propagate_all_parent, propagate_all_child)
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end");
    if !io.is_empty() {
        orchestrator = orchestrator.with_io("c1", io);
    }
    let orchestrator = orchestrator.build().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(child)).unwrap();
    engine
        .apply_command(Command::DeployProcess(orchestrator))
        .unwrap();
    engine
}

/// A pass-through callee that, while running, writes two of its own variables:
/// a fresh `childOnly` and an overwrite of the shared `shared` (so a merge-back
/// collision is observable — Zeebe semantics: the child's value wins).
fn propagating_child() -> ProcessDefinition {
    ProcessBuilder::new("phase")
        .start_event("pstart")
        .script_task("s1", "=99", "childOnly")
        .script_task("s2", "=\"child\"", "shared")
        .end_event("pend")
        .connect("pstart", "s1")
        .connect("s1", "s2")
        .connect("s2", "pend")
        .build()
        .unwrap()
}

/// Finds the child instance's seed variables (its `ProcessInstanceCreated`).
fn child_seed_of(events: &[Event]) -> HashMap<String, Value> {
    events
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated {
                process_id,
                variables,
                ..
            } if process_id == "phase" => Some(variables.clone()),
            _ => None,
        })
        .expect("child created")
}

/// The instance key of the `ProcessInstanceCreated` event for `process_id`.
///
/// A native call activity emits `ProcessInstanceCreated` for **both** the parent
/// and the spawned child, so selecting the first event carrying an instance key
/// is order-dependent and could latch onto the child. Match the parent's
/// `process_id` explicitly instead.
fn parent_key_of(events: &[Event], process_id: &str) -> Key {
    events
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated {
                instance_key,
                process_id: pid,
                ..
            } if pid == process_id => Some(*instance_key),
            _ => None,
        })
        .expect("parent process instance created")
}

/// The last value a variable took in the parent scope across the command's
/// `VariablesUpdated` events (`None` if it never crossed back).
fn parent_var_after<'a>(events: &'a [Event], parent_key: Key, name: &str) -> Option<&'a Value> {
    events.iter().rev().find_map(|e| match e {
        Event::VariablesUpdated {
            instance_key,
            variables,
        } if *instance_key == parent_key => variables.get(name),
        _ => None,
    })
}

#[test]
fn call_activity_default_propagates_all_parent_and_child_variables() {
    // Zeebe default (both attributes absent ⇒ true), no ioMappings: all visible
    // parent variables cross into the child, and all of the child's final
    // variables merge back into the parent (child wins on a collision).
    let mut engine =
        deploy_native_call_with_propagation(Default::default(), propagating_child(), true, true);
    let created = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[
                ("orderId", Value::Int(42)),
                ("shared", Value::Str("parent".into())),
            ]),
        ))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");

    // Parent → child: the whole visible parent scope crossed into the child.
    let seed = child_seed_of(&created);
    assert_eq!(seed.get("orderId"), Some(&Value::Int(42)));
    assert_eq!(seed.get("shared"), Some(&Value::Str("parent".into())));

    // Child → parent: the child's fresh variable crossed back, and the shared
    // collision resolved to the child's value.
    assert!(engine.is_completed(parent_key));
    assert_eq!(
        parent_var_after(&created, parent_key, "childOnly"),
        Some(&Value::Int(99))
    );
    assert_eq!(
        parent_var_after(&created, parent_key, "shared"),
        Some(&Value::Str("child".into())),
        "child wins on a merge-back name collision"
    );
}

#[test]
fn call_activity_propagate_all_parent_false_copies_only_input_mapping_results() {
    // propagateAllParentVariables="false": only the call activity's local
    // variables — its input-mapping results — cross into the child. The child
    // variables still merge back (propagateAllChildVariables defaults true).
    let io = crate::model::IoMapping {
        inputs: vec![crate::model::Mapping {
            source: "=orderId".to_string(),
            target: "childOrder".to_string(),
        }],
        outputs: Vec::new(),
    };
    let mut engine = deploy_native_call_with_propagation(io, propagating_child(), false, true);
    let created = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[
                ("orderId", Value::Int(42)),
                ("shared", Value::Str("parent".into())),
            ]),
        ))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");

    // Only the input mapping seeded the child; the parent's other variables did
    // not cross.
    let seed = child_seed_of(&created);
    assert_eq!(seed.get("childOrder"), Some(&Value::Int(42)));
    assert!(
        !seed.contains_key("orderId") && !seed.contains_key("shared"),
        "with propagateAllParentVariables=false only input-mapping results cross"
    );

    // Child → parent still merges (default true): the child's fresh variable and
    // its own (mapping-seeded) childOrder cross back.
    assert!(engine.is_completed(parent_key));
    assert_eq!(
        parent_var_after(&created, parent_key, "childOnly"),
        Some(&Value::Int(99))
    );
}

#[test]
fn call_activity_propagate_all_child_false_without_output_mappings_copies_nothing_back() {
    // propagateAllChildVariables="false" with no output mappings: nothing crosses
    // back. Parent → child still copies all visible variables (default true).
    let mut engine =
        deploy_native_call_with_propagation(Default::default(), propagating_child(), true, false);
    let created = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[
                ("orderId", Value::Int(42)),
                ("shared", Value::Str("parent".into())),
            ]),
        ))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");

    // Parent → child copied everything.
    let seed = child_seed_of(&created);
    assert_eq!(seed.get("orderId"), Some(&Value::Int(42)));

    // Child → parent copied nothing: the child's fresh variable never appears in
    // the parent, and the shared collision left the parent's value intact.
    assert!(engine.is_completed(parent_key));
    assert_eq!(
        parent_var_after(&created, parent_key, "childOnly"),
        None,
        "no child variable crosses back with propagateAllChildVariables=false and no output mappings"
    );
    assert!(
        !created.iter().any(|e| matches!(
            e,
            Event::VariablesUpdated { instance_key, variables }
                if *instance_key == parent_key
                    && variables.get("shared") == Some(&Value::Str("child".into()))
        )),
        "the child's overwrite of the shared variable must not cross back"
    );
}

#[test]
fn call_activity_propagate_all_child_false_still_applies_output_mappings() {
    // propagateAllChildVariables="false" but WITH an output mapping: only the
    // mapped output crosses back (output mappings always apply), the child's raw
    // scope does not.
    let io = crate::model::IoMapping {
        inputs: Vec::new(),
        outputs: vec![crate::model::Mapping {
            source: "=childOnly".to_string(),
            target: "echoed".to_string(),
        }],
    };
    let mut engine = deploy_native_call_with_propagation(io, propagating_child(), true, false);
    let created = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[("orderId", Value::Int(42))]),
        ))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");

    assert!(engine.is_completed(parent_key));
    // The output mapping projected childOnly into the parent as `echoed`…
    assert_eq!(
        parent_var_after(&created, parent_key, "echoed"),
        Some(&Value::Int(99))
    );
    // …but the child's raw variable did not cross back wholesale.
    assert!(
        !created.iter().any(|e| matches!(
            e,
            Event::VariablesUpdated { instance_key, variables }
                if *instance_key == parent_key && variables.contains_key("childOnly")
        )),
        "with propagateAllChildVariables=false only the mapped output crosses back"
    );
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
fn static_input_mapping_source_is_passed_through_as_literal_not_feel() {
    // #1160: a `zeebe:input` `source` WITHOUT a leading `=` is a STATIC literal
    // string (Zeebe parity), not a FEEL expression. Before the fix, engine-wasm
    // 0.9.0 evaluated every source as FEEL, so `in-process` parsed as the FEEL
    // subtraction `in - process` (incident "- not defined for null and null")
    // and `{{secrets.FOO}}` as a malformed context ("expected a context key").
    // Each must now merge verbatim, no incident, and the job must be offered.
    let def = ProcessBuilder::new("io-static")
        .start_event("s")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: vec![
                    crate::model::Mapping {
                        source: "in-process".to_string(),
                        target: "mode".to_string(),
                    },
                    crate::model::Mapping {
                        source: "{{secrets.CAMUNDA_PROVIDED_LLM_API_ENDPOINT}}".to_string(),
                        target: "endpoint".to_string(),
                    },
                    crate::model::Mapping {
                        source: "openaiCompatible".to_string(),
                        target: "provider".to_string(),
                    },
                    // A static literal is passed through VERBATIM — significant
                    // leading/trailing whitespace must be preserved, not trimmed.
                    crate::model::Mapping {
                        source: "  spaced value  ".to_string(),
                        target: "padded".to_string(),
                    },
                    // A leading `=` still selects FEEL evaluation.
                    crate::model::Mapping {
                        source: "=1 + 1".to_string(),
                        target: "sum".to_string(),
                    },
                ],
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
    let key = create_instance_key(&mut engine, "io-static");

    assert!(
        engine.active_incidents().is_empty(),
        "static ioMapping sources must not raise incidents: {:?}",
        engine.active_incidents()
    );
    assert!(!engine.is_completed(key));

    // The activated job sees each mapped value: literals verbatim, FEEL evaluated.
    let job = &engine.activate_jobs("work", "w", 1, 60_000, 0)[0];
    assert_eq!(
        job.variables.get("mode"),
        Some(&Value::Str("in-process".to_string()))
    );
    assert_eq!(
        job.variables.get("endpoint"),
        Some(&Value::Str(
            "{{secrets.CAMUNDA_PROVIDED_LLM_API_ENDPOINT}}".to_string()
        ))
    );
    assert_eq!(
        job.variables.get("provider"),
        Some(&Value::Str("openaiCompatible".to_string()))
    );
    assert_eq!(
        job.variables.get("padded"),
        Some(&Value::Str("  spaced value  ".to_string())),
        "a static literal source must be passed through verbatim, whitespace intact"
    );
    assert_eq!(job.variables.get("sum"), Some(&Value::Int(2)));
}

#[test]
fn static_output_mapping_source_is_passed_through_as_literal_not_feel() {
    // #1160 (output side): a `zeebe:output` `source` without a leading `=` is a
    // static literal string too, projected verbatim at completion rather than
    // evaluated as FEEL.
    let def = ProcessBuilder::new("io-static-out")
        .start_event("s")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "in-process".to_string(),
                    target: "mode".to_string(),
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
    let inst = create_instance_key(&mut engine, "io-static-out");
    let job_key = engine.activate_jobs("work", "w", 1, 60_000, 0)[0].key;
    let events = engine
        .apply_command(Command::complete_job_with(job_key, HashMap::new()))
        .unwrap();
    assert!(
        engine.active_incidents().is_empty(),
        "static output mapping must not raise an incident"
    );
    let mapped = events.iter().any(|e| {
        matches!(
            e,
            Event::VariablesUpdated { instance_key, variables }
                if *instance_key == inst
                    && variables.get("mode") == Some(&Value::Str("in-process".to_string()))
        )
    });
    assert!(
        mapped,
        "output mapping should set mode='in-process'; {events:?}"
    );
}

#[test]
fn input_mapping_eval_failure_raises_io_mapping_incident_and_no_job() {
    // #939: an input `zeebe:ioMapping` whose SOURCE fails to evaluate (here
    // `=x + 1` with `x` bound to a string — a FEEL type error, not a bare
    // missing reference) must raise an `IO_MAPPING_ERROR` incident and PARK the
    // element ACTIVATED — no job, no silent proceed with the target unset.
    // Before the fix the failure was swallowed (`continue`) and the token sailed
    // on. Fixing the variable and resolving the incident re-drives the activation
    // body, which re-applies the now-valid mapping and creates the job.
    let def = ProcessBuilder::new("io-in-fail")
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
    let key = engine
        .apply_command(Command::create_instance_with(
            "io-in-fail",
            vars(&[("x", Value::Str("oops".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // The input mapping failed: exactly one active IoMapping incident, no job,
    // and the token is parked (the service task never enacted its behaviour).
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert_eq!(engine.state().jobs.len(), 0, "no job while parked");
    assert!(!engine.is_completed(key));
    let incident_key = engine.incidents()[0].key;

    // Fix `x` to a number and resolve: the activation body re-runs, the mapping
    // now evaluates (`y = 42`), and the job is created with the mapped variable.
    engine
        .apply_command(Command::set_variables(
            key,
            HashMap::from([("x".to_string(), Value::Int(41))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    assert!(engine.instance(key).unwrap().incidents.is_empty());
    assert_eq!(engine.state().jobs.len(), 1, "job created on resolution");
    let job = &engine.activate_jobs("work", "w", 1, 60_000, 0)[0];
    assert_eq!(job.variables.get("y"), Some(&Value::Int(42)));
}

#[test]
fn output_mapping_eval_failure_raises_incident_and_does_not_complete() {
    // #939: an output `zeebe:ioMapping` whose SOURCE fails to evaluate at
    // completion must raise an incident and hold the element in the COMPLETING
    // phase rather than completing it with the target silently unset. Resolution
    // re-drives `Complete`, re-evaluating the mapping against the (now-fixed)
    // variables without re-running the job.
    let def = ProcessBuilder::new("io-out-fail")
        .start_event("s")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "=bad + 1".to_string(),
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
    let key = engine
        .apply_command(Command::create_instance_with(
            "io-out-fail",
            vars(&[("bad", Value::Str("oops".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let job_key = engine.activate_jobs("work", "w", 1, 60_000, 0)[0].key;
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();

    // The output mapping failed: one active `IoMapping` incident (output-phase,
    // distinguished by `redrive: Completion`; REST `IO_MAPPING_ERROR` — matching
    // Zeebe, which raises `IO_MAPPING_ERROR` for both input and output mapping
    // failures), and the element has NOT completed.
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert!(
        !engine.is_completed(key),
        "must not complete on eval failure"
    );
    let incident_key = engine.incidents()[0].key;

    // Fix `bad` and resolve: `Complete` re-drives, the mapping evaluates
    // (`approved = 42`), and the instance runs to completion.
    engine
        .apply_command(Command::set_variables(
            key,
            HashMap::from([("bad".to_string(), Value::Int(41))]),
        ))
        .unwrap();
    let events = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(engine.instance(key).unwrap().incidents.is_empty());
    assert!(engine.is_completed(key), "completes after resolution");
    assert_eq!(
        merged_var(&events, "approved"),
        Some(Value::Int(42)),
        "output mapping should surface approved=42; events: {events:?}"
    );
}

#[test]
fn message_catch_output_mapping_incident_resolves_via_complete_not_reopen() {
    // Regression: an OUTPUT `zeebe:ioMapping` failure on a message intermediate
    // catch event parks the token in the COMPLETING phase *after* the message
    // has already been correlated and consumed. Resolving the incident must
    // re-drive `Complete` (re-projecting the now-fixed output mapping for the
    // same token), NOT `ReopenCatch` — reopening the subscription would strand
    // the token waiting for a *second* message that will never arrive.
    // `ReopenCatch` is reserved for correlation-key (ACTIVATING) failures, which
    // surface as `ExpressionEvaluation`, never as an output-phase `IoMapping`.
    let def = ProcessBuilder::new("msg-out-fail")
        .start_event("s")
        .message_intermediate_catch_event("await", "approve", "orderId")
        .with_io(
            "await",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "=bad + 1".to_string(),
                    target: "approved".to_string(),
                }],
            },
        )
        .end_event("e")
        .connect("s", "await")
        .connect("await", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let key = engine
        .apply_command(Command::create_instance_with(
            "msg-out-fail",
            vars(&[
                ("orderId", Value::Str("A".into())),
                ("bad", Value::Str("oops".into())),
            ]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Parked on one open message subscription.
    let subs = engine.message_subscriptions();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].state, state::MessageSubscriptionState::Open);

    // Correlating drives completion, which applies the output mapping
    // `=bad + 1` (string + int) and fails: one output-phase `IoMapping` incident,
    // the token held in COMPLETING, and the subscription already consumed.
    engine.correlate_message("approve", "A", HashMap::new(), 0);
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert!(
        !engine.is_completed(key),
        "must not complete on eval failure"
    );
    assert!(
        engine
            .message_subscriptions()
            .iter()
            .all(|s| s.state != state::MessageSubscriptionState::Open),
        "the correlated subscription is consumed, not left open"
    );
    let incident_key = engine.incidents()[0].key;

    // Fix `bad` and resolve. The regression: resolution must re-drive `Complete`
    // (the token runs to completion), not `ReopenCatch` (which would open a
    // fresh subscription and never complete for lack of a second message).
    engine
        .apply_command(Command::set_variables(
            key,
            HashMap::from([("bad".to_string(), Value::Int(41))]),
        ))
        .unwrap();
    let events = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    assert!(engine.instance(key).unwrap().incidents.is_empty());
    assert!(
        engine.is_completed(key),
        "message-catch output-mapping incident must resolve by completing the \
         token, not reopening the catch; events: {events:?}"
    );
    assert!(
        engine
            .message_subscriptions()
            .iter()
            .all(|s| s.state != state::MessageSubscriptionState::Open),
        "resolution must not reopen the message subscription"
    );
    assert_eq!(
        merged_var(&events, "approved"),
        Some(Value::Int(42)),
        "output mapping should surface approved=42; events: {events:?}"
    );
}

#[test]
fn subprocess_output_mapping_eval_failure_raises_io_mapping_incident() {
    // #939 parity (ports Zeebe `OutputMappingIncidentTest` to a scoped element):
    // an OUTPUT `zeebe:ioMapping` failure on a *sub-process* (not the mainstream
    // service-task path) must raise the `IoMapping` incident kind (REST
    // `IO_MAPPING_ERROR` — the same taxonomy Zeebe raises for both input and
    // output mapping failures) and hold the sub-process in COMPLETING rather than
    // completing it with the target silently unset. This locks the taxonomy
    // relabel across every output path, not just the mainstream element.
    //
    // Recovery is phase-driven (#946): the incident's `Completion` re-drive
    // re-projects the now-fixed output mapping via `Complete` — the same
    // lifecycle as the mainstream output path — without re-running the inner job.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(vec![
            crate::model::Mapping {
                source: "=bad + 1".to_string(),
                target: "exported".to_string(),
            },
        ])))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "sub-scope",
            vars(&[("seed", Value::Int(4)), ("bad", Value::Str("oops".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    // Completing the inner job drives the sub-process into its output-mapping
    // projection, which fails on `=bad + 1` (string + int).
    let _ = complete_one(&mut engine, "work");

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(
        active[0].kind,
        state::IncidentKind::IoMapping,
        "a sub-process output-mapping failure must raise IO_MAPPING_ERROR, not EXTRACT_VALUE_ERROR",
    );
    assert_eq!(
        active[0].element_id, "sub",
        "the incident parks on the sub-process, not the inner task"
    );
    assert_eq!(
        active[0].redrive,
        Some(state::IoMappingRedrive::Completion),
        "a sub-process output failure re-drives the completion phase",
    );
    assert!(
        !engine.is_completed(inst),
        "the process must not complete while the output mapping is unresolved"
    );

    // Fix `bad` in the sub-process's own scope and resolve. A sub-process
    // projects its output mapping in the drain sweep
    // (`complete_drained_subprocesses`), not `complete`, so its `Completion`
    // re-drive is owned by that sweep rather than a `Complete` step: resolution
    // clears the incident (it does NOT re-drive `Complete`, which would
    // re-evaluate against the already-torn-down inner scope and re-raise a bogus
    // incident) and the instance runs to completion.
    let sub_scope = active[0].element_instance_key;
    let incident_key = active[0].key;
    engine
        .apply_command(Command::set_variables_scoped(
            sub_scope,
            HashMap::from([("bad".to_string(), Value::Int(41))]),
            true,
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(engine.instance(inst).unwrap().incidents.is_empty());
    assert!(
        engine.is_completed(inst),
        "resolving the sub-process output incident clears the block and the \
         instance completes without a spurious re-raised incident"
    );
}

#[test]
fn call_activity_input_mapping_failure_raises_io_mapping_and_respawns_on_resolve() {
    // #946 item 1 (call-activity input): an input `zeebe:ioMapping` on a call
    // activity that fails to evaluate must raise `IO_MAPPING_ERROR` (NOT
    // `ExpressionEvaluation`) and PARK the call activity without spawning the
    // child — not skip straight to `Complete`. Resolution re-drives the *spawn*
    // (`CallActivitySpawn`): the now-fixed input mapping is re-applied and the
    // child process is created, running the orchestration to completion.
    let child = ProcessBuilder::new("phase")
        .start_event("pstart")
        .end_event("pend")
        .connect("pstart", "pend")
        .build()
        .unwrap();
    let io = crate::model::IoMapping {
        inputs: vec![crate::model::Mapping {
            source: "=orderId + 1".to_string(),
            target: "childOrder".to_string(),
        }],
        outputs: Vec::new(),
    };
    let mut engine = deploy_native_call(io, child);
    let parent = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[("orderId", Value::Str("oops".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert_eq!(
        active[0].redrive,
        Some(state::IoMappingRedrive::CallActivitySpawn),
        "a call-activity input failure re-drives the child spawn, not a generic complete",
    );
    assert_eq!(active[0].element_id, "c1");
    assert!(
        !engine.is_completed(parent),
        "parked before the child spawns"
    );
    // No child process instance was created.
    assert_eq!(
        engine.state().instances.len(),
        1,
        "no child spawned while the input mapping is unresolved"
    );
    let incident_key = engine.incidents()[0].key;

    // Fix `orderId` and resolve: the spawn re-drives, the child is created and
    // (being a pass-through) completes, and the orchestration finishes.
    engine
        .apply_command(Command::set_variables(
            parent,
            HashMap::from([("orderId".to_string(), Value::Int(41))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(engine.instance(parent).unwrap().incidents.is_empty());
    assert!(
        engine.is_completed(parent),
        "the orchestration completes once the child spawns and finishes"
    );
    assert_eq!(
        engine.state().instances.len(),
        2,
        "the child process instance was spawned on resolution"
    );
}

#[test]
fn call_activity_output_mapping_failure_preserves_child_context_on_redrive() {
    // #946 item 3 (call-activity output): an output `zeebe:ioMapping` on a call
    // activity that fails must raise `IO_MAPPING_ERROR`, hold the call activity in
    // COMPLETING, and — critically — *preserve the completed child's variables* on
    // the incident so the re-drive re-projects them (`CallActivityCompletion`).
    // The old generic `Complete` re-drive neither carried the child result nor
    // called `complete_call_activity`, silently dropping the callee context.
    let child = ProcessBuilder::new("phase")
        .start_event("pstart")
        .end_event("pend")
        .connect("pstart", "pend")
        .build()
        .unwrap();
    let io = crate::model::IoMapping {
        inputs: vec![crate::model::Mapping {
            source: "=tag".to_string(),
            target: "childTag".to_string(),
        }],
        outputs: vec![crate::model::Mapping {
            // `childTag` is a string; `string + 1` is a FEEL type error.
            source: "=childTag + 1".to_string(),
            target: "echoed".to_string(),
        }],
    };
    let mut engine = deploy_native_call(io, child);
    let parent = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[("tag", Value::Str("gamma".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert_eq!(active[0].element_id, "c1");
    assert!(
        !engine.is_completed(parent),
        "must not complete while the output mapping is unresolved"
    );
    // The completed child's variables are preserved on the incident's re-drive
    // descriptor so completion can be retried against them after the gone child.
    match &active[0].redrive {
        Some(state::IoMappingRedrive::CallActivityCompletion { child_variables }) => {
            assert_eq!(
                child_variables.get("childTag"),
                Some(&Value::Str("gamma".into())),
                "the callee result is captured on the incident for the re-drive"
            );
        }
        other => panic!("expected CallActivityCompletion redrive, got {other:?}"),
    }
}

#[test]
fn multi_instance_child_input_mapping_failure_raises_io_mapping_and_reactivates() {
    // #946 item 1 (MI child input): a per-child input `zeebe:ioMapping` that fails
    // must raise `IO_MAPPING_ERROR` and park the child (NOT skip to `Complete`).
    // Resolution re-drives the child's *activation* (`MiChildActivation`),
    // re-applying the now-fixed input and minting the child's job.
    let def = ProcessBuilder::new("mi-child-io")
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
                // References the per-child binding `item` (tolerated at the body
                // level) and the root `factor` (a string ⇒ per-child eval fails).
                inputs: vec![crate::model::Mapping {
                    source: "=factor + item".to_string(),
                    target: "weighted".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "mi-child-io",
            vars(&[
                ("items", Value::List(vec![Value::Int(10)])),
                ("factor", Value::Str("oops".into())),
            ]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert!(
        matches!(
            active[0].redrive,
            Some(state::IoMappingRedrive::MiChildActivation { .. })
        ),
        "a MI child input failure re-drives the child's activation, got {:?}",
        active[0].redrive
    );
    assert_eq!(active[0].element_id, "each");
    assert_eq!(
        engine.state().jobs.len(),
        0,
        "no job while the child is parked"
    );
    let incident_key = engine.incidents()[0].key;

    // Fix `factor` and resolve: the child re-activates, the input maps
    // (`weighted = 5 + 10 = 15`), and its job is created.
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("factor".to_string(), Value::Int(5))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(engine.instance(inst).unwrap().incidents.is_empty());
    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 1, "the child's job is minted on resolution");
    assert_eq!(jobs[0].variables.get("weighted"), Some(&Value::Int(15)));
}

#[test]
fn multi_instance_body_input_mapping_genuine_failure_raises_incident_and_recovers() {
    // #946 item 2a: a MI *body* input mapping that fails for a reason NOT tied to
    // a per-child binding (`loopCounter` / `inputElement`) is a GENUINE failure —
    // it must raise `IO_MAPPING_ERROR` and spawn no children, rather than
    // `.unwrap_or_default()`-swallowing the error into an empty collection.
    // Resolution re-drives the body activation (`MiBodyActivation`).
    let def = ProcessBuilder::new("mi-body-in")
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
                // Does NOT reference a per-child binding ⇒ evaluated at the body
                // level; `badFactor` is a string, so it fails genuinely.
                inputs: vec![crate::model::Mapping {
                    source: "=badFactor + 1".to_string(),
                    target: "scaled".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "mi-body-in",
            vars(&[
                ("items", Value::List(vec![Value::Int(10), Value::Int(20)])),
                ("badFactor", Value::Str("oops".into())),
            ]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert_eq!(
        active[0].redrive,
        Some(state::IoMappingRedrive::MiBodyActivation),
        "a genuine MI body input failure re-drives the body activation",
    );
    assert_eq!(active[0].element_id, "each");
    assert_eq!(
        engine.state().jobs.len(),
        0,
        "no children fanned out on a genuine body input failure"
    );
    let incident_key = engine.incidents()[0].key;

    // Fix `badFactor` and resolve: the body fans out its two children (jobs).
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("badFactor".to_string(), Value::Int(5))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(engine.instance(inst).unwrap().incidents.is_empty());
    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(
        jobs.len(),
        2,
        "both children fan out once the body re-drives"
    );
}

#[test]
fn multi_instance_body_output_mapping_genuine_failure_raises_incident_and_recovers() {
    // #946 item 2b: a MI *body* output mapping that fails at BODY completion —
    // reached when the loop has no children to evaluate it per-child (an EMPTY
    // input collection: the body drains straight to completion) — is a GENUINE
    // failure. It must raise `IO_MAPPING_ERROR` and hold the body in COMPLETING,
    // rather than `.unwrap_or_default()`-completing with the output silently
    // unset. Resolution re-drives the body completion (`MiBodyCompletion`).
    //
    // (A non-empty loop evaluates the activity's output mapping per-child in
    // `complete_mi_child` — Zeebe applies `zeebe:output` in each child's scope —
    // so a broken source there raises the per-child `Completion` re-drive first;
    // the body-level `MiBodyCompletion` path is the zero-child aggregation case.)
    let def = ProcessBuilder::new("mi-body-out")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: Some("results".to_string()),
                output_element: Some("=item".to_string()),
                completion_condition: None,
                sequential: false,
            },
        )
        .with_io(
            "each",
            crate::model::IoMapping {
                inputs: Vec::new(),
                // Evaluated at body completion; `badOut` is a string ⇒ fails.
                outputs: vec![crate::model::Mapping {
                    source: "=badOut + 1".to_string(),
                    target: "summary".to_string(),
                }],
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    // An EMPTY input collection: the body drains straight to its output
    // aggregation with no per-child evaluation in between.
    let inst = engine
        .apply_command(Command::create_instance_with(
            "mi-body-out",
            vars(&[
                ("items", Value::List(Vec::new())),
                ("badOut", Value::Str("oops".into())),
            ]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert_eq!(
        active[0].redrive,
        Some(state::IoMappingRedrive::MiBodyCompletion),
        "a genuine MI body output failure re-drives the body completion",
    );
    assert_eq!(active[0].element_id, "each");
    assert!(!engine.is_completed(inst), "body held in COMPLETING");
    let incident_key = engine.incidents()[0].key;

    // Fix `badOut` and resolve: the body completion re-drives (`summary = 6`) and
    // the process finishes.
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("badOut".to_string(), Value::Int(5))]),
        ))
        .unwrap();
    let events = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(engine.instance(inst).unwrap().incidents.is_empty());
    assert!(
        engine.is_completed(inst),
        "the body completes once its output mapping re-drives"
    );
    assert_eq!(
        merged_var(&events, "summary"),
        Some(Value::Int(6)),
        "the re-driven body output mapping projects summary=6; events: {events:?}"
    );
}

#[test]
fn bare_missing_var_input_mapping_assigns_null_without_incident() {
    // Scope guard for #939: a bare reference to a MISSING variable (`=missing`)
    // is FEEL `null` (an `Ok`), NOT an evaluation failure — it must still assign
    // the target as `null` and raise NO incident. Only a source that genuinely
    // errors (parse/type error, operation on a missing value) halts the element.
    let def = ProcessBuilder::new("io-in-null")
        .start_event("s")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=missing".to_string(),
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
    let _ = create_instance_key(&mut engine, "io-in-null");
    assert!(
        engine.active_incidents().is_empty(),
        "a bare missing-var input (FEEL null) must not raise an incident"
    );
    let job = &engine.activate_jobs("work", "w", 1, 60_000, 0)[0];
    assert_eq!(
        job.variables.get("y"),
        Some(&Value::Null),
        "the target is assigned null, not dropped"
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

fn adhoc_agent_with_tool_input_mapping() -> ProcessDefinition {
    // A JOB_WORKER ad-hoc container whose `toolA` carries an input
    // `zeebe:ioMapping` (`weighted = base + 1`). `base` is resolved from the
    // container scope, so a bad `base` fails the tool's input mapping on
    // activation — the #946 ad-hoc-tool input path.
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
                <zeebe:ioMapping>
                  <zeebe:input source="=base + 1" target="weighted" />
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
fn adhoc_tool_input_mapping_failure_raises_io_mapping_and_reactivates() {
    // #946 item 1 (ad-hoc tool input): a tool's own input `zeebe:ioMapping` that
    // fails to evaluate must raise `IO_MAPPING_ERROR` on the container and NOT
    // create the tool child (no skip-to-`Complete`). Resolution re-drives the
    // tool's *activation* (`AdHocToolActivation`), re-applying the now-fixed input
    // and minting the tool job.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_with_tool_input_mapping()))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("base", Value::Str("oops".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    // Activate the tool: its input mapping `=base + 1` fails (string + int).
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

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert!(
        matches!(
            active[0].redrive,
            Some(state::IoMappingRedrive::AdHocToolActivation { .. })
        ),
        "an ad-hoc tool input failure re-drives the tool activation, got {:?}",
        active[0].redrive
    );
    assert_eq!(
        engine.activate_jobs("tool", "W", 10, 1_000, 0).len(),
        0,
        "no tool job while the tool is parked on the input-mapping incident"
    );
    let incident_key = engine.incidents()[0].key;

    // Fix `base` and resolve: the tool re-activates, its input maps
    // (`weighted = 5 + 1 = 6`), and its job is minted.
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("base".to_string(), Value::Int(5))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(engine.instance(inst).unwrap().incidents.is_empty());
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool_jobs.len(), 1, "the tool job is minted on resolution");
    assert_eq!(tool_jobs[0].variables.get("weighted"), Some(&Value::Int(6)));
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

fn adhoc_agent_with_interrupting_message_boundary() -> ProcessDefinition {
    // An `adHocSubProcess` (JOB_WORKER agent) carrying an interrupting message
    // boundary event, correlating on `customerId`. This is the executable shape
    // of Camunda's event-driven agent pattern (#1155): a newly-arrived event
    // must abandon the agent mid-investigation, including while its own
    // human-consult (`InnerTask`) task is open.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="Host">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="probe-agent" />
              <zeebe:adHoc outputCollection="r" outputElement="=toolCallResult" />
            </bpmn:extensionElements>
            <bpmn:userTask id="InnerTask">
              <bpmn:extensionElements>
                <zeebe:userTask />
              </bpmn:extensionElements>
            </bpmn:userTask>
          </bpmn:adHocSubProcess>
          <bpmn:boundaryEvent id="Bnd" attachedToRef="Host">
            <bpmn:messageEventDefinition messageRef="M" />
          </bpmn:boundaryEvent>
          <bpmn:endEvent id="EndNormal" />
          <bpmn:endEvent id="EndInterrupted" />
          <bpmn:sequenceFlow id="F1" sourceRef="s" targetRef="Host" />
          <bpmn:sequenceFlow id="F2" sourceRef="Host" targetRef="EndNormal" />
          <bpmn:sequenceFlow id="F3" sourceRef="Bnd" targetRef="EndInterrupted" />
        </bpmn:process>
        <bpmn:message id="M" name="probe-cancel">
          <bpmn:extensionElements>
            <zeebe:subscription correlationKey="=customerId" />
          </bpmn:extensionElements>
        </bpmn:message>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

#[test]
fn interrupting_boundary_on_adhoc_cancels_an_activated_tool() {
    // Issue #1155: an interrupting boundary event on an `adHocSubProcess` must
    // terminate the attached activity AND everything inside its scope —
    // including an activated tool and its open user task — then take its own
    // outgoing flow. Before the fix the boundary fired but left the activated
    // tool + its `#innerInstance` active, so the instance hung `Active` forever.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_with_interrupting_message_boundary(),
        ))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("customerId", Value::Str("C1".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // The agent activates the inner user-task tool: it stays active, the
    // boundary subscription is open, and the instance parks.
    let agent = engine
        .activate_jobs("probe-agent", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "Host")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("InnerTask")],
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
        1,
        "the user-task tool is active before the boundary fires"
    );
    assert!(!engine.is_completed(inst));

    // The cancel message arrives: the interrupting boundary fires and must tear
    // down the whole ad-hoc scope, then route its own outgoing flow to
    // `EndInterrupted`.
    let fired = engine.correlate_message("probe-cancel", "C1", HashMap::new(), 0);
    assert!(
        fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "Bnd" && to == "EndInterrupted"
        )),
        "the boundary event fires and takes its outgoing flow"
    );

    assert!(
        engine.is_completed(inst),
        "the instance reaches EndInterrupted and completes"
    );
    assert!(
        engine.instance(inst).is_none()
            || engine
                .instance(inst)
                .unwrap()
                .active
                .values()
                .all(|id| id != "InnerTask"
                    && !id.ends_with(crate::engine::ADHOC_INNER_INSTANCE_ID_POSTFIX)),
        "the activated tool and its #innerInstance were torn down, not orphaned"
    );
    assert!(
        engine.instance(inst).is_none()
            || engine.instance(inst).unwrap().adhoc_instances.is_empty(),
        "the ad-hoc container's runtime record was cleared on cancel"
    );
    assert!(
        engine
            .state()
            .user_tasks
            .values()
            .all(|t| t.element_id != "InnerTask" || t.state != state::UserTaskState::Created),
        "the open user task was cancelled, not left Created"
    );
}

#[test]
fn interrupting_boundary_on_adhoc_cancels_an_in_flight_agent_job() {
    // Issue #1155: the interrupting-boundary teardown must also cancel the
    // container's OWN agent job when it is still in flight — i.e. the boundary
    // fires while the ad-hoc worker is mid-turn (its job activated but not yet
    // completed). This exercises the `active_job_on(container)` /
    // `Event::JobCanceled` branch in `interrupt_activity_via_boundary`, which
    // the tool-teardown test above never reaches because it completes the agent
    // job before correlating (so `active_job_on` returns `None` there).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_with_interrupting_message_boundary(),
        ))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("customerId", Value::Str("C1".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // The agent job is activated (a worker picked it up) but NOT completed — the
    // container is mid-investigation with an in-flight job.
    let agent = engine
        .activate_jobs("probe-agent", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "Host")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;
    assert!(!engine.is_completed(inst));

    // The cancel message arrives while the agent job is still in flight: the
    // interrupting boundary must cancel that job (not leave it activated) and
    // route its own outgoing flow to `EndInterrupted`.
    let fired = engine.correlate_message("probe-cancel", "C1", HashMap::new(), 0);
    assert!(
        fired.iter().any(|e| matches!(
            e,
            Event::JobCanceled { job_key, .. } if *job_key == agent.key
        )),
        "the in-flight agent job is canceled when the boundary fires"
    );
    assert!(
        fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "Bnd" && to == "EndInterrupted"
        )),
        "the boundary event fires and takes its outgoing flow"
    );

    assert!(
        engine.is_completed(inst),
        "the instance reaches EndInterrupted and completes"
    );
    // The agent job must be gone (canceled), not left activated on a dead
    // container.
    assert!(
        engine.active_job_on(container).is_none(),
        "no agent job survives on the torn-down container"
    );
    assert!(
        engine.instance(inst).is_none()
            || engine.instance(inst).unwrap().adhoc_instances.is_empty(),
        "the ad-hoc container's runtime record was cleared on cancel"
    );
}

fn adhoc_agent_chained_tools_process() -> ProcessDefinition {
    // A JOB_WORKER ad-hoc container whose two service-task tools are joined by a
    // plain `bpmn:sequenceFlow` BETWEEN THE CONTAINER'S OWN CHILDREN (issue
    // #1154): `toolA -> toolB`. Camunda documents this "structured sequence" —
    // activating `toolA` alone must, on its completion, take the flow and run
    // `toolB`; the container re-emits its agent job only once the whole chain
    // drains. `toolA`/`toolB` carry distinct job types so each is drained
    // independently by the test.
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
                <zeebe:taskDefinition type="toolA-type" />
              </bpmn:extensionElements>
              <bpmn:outgoing>chain</bpmn:outgoing>
            </bpmn:serviceTask>
            <bpmn:sequenceFlow id="chain" sourceRef="toolA" targetRef="toolB" />
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="toolB-type" />
              </bpmn:extensionElements>
              <bpmn:incoming>chain</bpmn:incoming>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

/// Regression for Magikcraft/nano-bpm#1154: a `bpmn:sequenceFlow` between two
/// DIRECT children of an ad-hoc container must execute as a chain. Before the
/// fix the activated element ran but its outgoing flow was silently dropped —
/// the downstream element never activated and the container completed as if the
/// activated element were a leaf (the `parsed-not-executed` class #1009, for a
/// sequence flow inside an ad-hoc scope). This drives the reproduction: activate
/// `toolA` alone, and assert `toolA -> toolB` is taken, `toolB` runs, the agent
/// job re-emits only once the chain drains, and the leaf's output is the single
/// entry appended to the container's `outputCollection`.
#[test]
fn adhoc_inner_sequence_flow_chains_to_the_follow_up_tool() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_chained_tools_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;

    // Turn 1: the agent activates ONLY `toolA` (the head of the chain).
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

    // `toolB` is NOT active yet — it only runs once `toolA` completes and its
    // outgoing flow is taken.
    assert!(
        engine
            .activate_jobs("toolB-type", "W", 10, 1_000, 0)
            .is_empty(),
        "toolB must not run before toolA completes and its flow is taken"
    );

    // Complete `toolA`. This is where the defect surfaced: the flow was dropped.
    let tool_a = engine
        .activate_jobs("toolA-type", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolA")
        .expect("toolA job emitted");
    let chain_events = engine
        .apply_command(Command::complete_job_with(
            tool_a.key,
            HashMap::from([("result".to_string(), Value::Str("A".into()))]),
        ))
        .unwrap();
    assert!(
        chain_events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "toolA" && to == "toolB"
        )),
        "toolA's outgoing inner flow to toolB must be taken (issue #1154), \
         got {chain_events:?}"
    );

    // The agent job must NOT re-emit while the chain is still running: the
    // container is a single active path (now at toolB), not drained.
    assert!(
        engine
            .activate_jobs("agent-worker", "W", 10, 1_000, 0)
            .is_empty(),
        "the agent job must not re-emit mid-chain — the path is still running"
    );

    // `toolB` chained into existence and produces a job.
    let tool_b = engine
        .activate_jobs("toolB-type", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolB")
        .expect("toolB chained from toolA's completed flow (issue #1154)");
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
        "exactly one tool active during the chain (toolB, after toolA handed off)"
    );

    // Complete `toolB` (the leaf). Only NOW does the path drain and the agent
    // job re-emit for the next turn.
    engine
        .apply_command(Command::complete_job_with(
            tool_b.key,
            HashMap::from([("result".to_string(), Value::Str("B".into()))]),
        ))
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
        "the chain drained after the leaf toolB"
    );
    assert_eq!(
        adhoc.iterations, 1,
        "the agent job re-emits exactly once — only after the whole chain drained"
    );
    // `outputElement` is a per-execution-path result: the chain toolA -> toolB is
    // ONE path, so the collection has exactly ONE entry — the leaf's result.
    assert!(
        matches!(
            container_output_collection(&engine, inst, container, "results"),
            Some(Value::List(ref v)) if v.as_slice() == [Value::Str("B".into())]
        ),
        "the chain contributes ONE outputCollection entry — the leaf toolB's \
         result — not one per node, got {:?}",
        container_output_collection(&engine, inst, container, "results")
    );

    // Turn 2: the agent signals completion → the container completes and writes
    // its aggregated collection outward.
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job re-emitted for turn 2");
    engine
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
        "the container completes once the agent is done"
    );
}

/// Regression for the empty-`element_id` teardown failure mode in the mid-chain
/// hand-off (`continue_adhoc_inner_flow`). A completing tool tears down its
/// dedicated inner-instance wrapper; if that wrapper has already left `active`
/// (but still resolves via `scopes`), the old `unwrap_or_default()` emitted
/// `ElementCompleting`/`ElementCompleted` with an empty `element_id`, which
/// corrupts downstream element aggregates — the same class guarded on the cancel
/// path by `nested_adhoc_cancel_child_skips_already_completed_inner_instance`.
/// The fix routes both tool paths through `adhoc_inner_instance_teardown`, which
/// skips the teardown when the id is unresolvable.
#[test]
fn adhoc_mid_chain_handoff_skips_already_completed_inner_instance() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_chained_tools_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");

    // Turn 1: activate only `toolA` (the head of the chain).
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
    let tool_a = engine
        .activate_jobs("toolA-type", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolA")
        .expect("toolA job emitted");
    let tool_a_eik = tool_a.element_instance_key;

    // The dedicated inner wrapper `toolA` hangs off.
    let inner = engine.scope_of(inst, tool_a_eik);
    assert_ne!(inner, 0, "toolA hangs off a dedicated inner instance");

    // Simulate that inner wrapper having ALREADY been torn down: drop it from
    // `active` (so its element id no longer resolves) while its `scopes` mapping
    // still resolves it — the exact state the old `unwrap_or_default()` mishandled.
    engine
        .state
        .instances
        .get_mut(&inst)
        .unwrap()
        .active
        .remove(&inner);
    assert!(
        engine.element_id_of_instance(inst, inner).is_none(),
        "inner wrapper is no longer active"
    );
    assert_eq!(
        engine.scope_of(inst, tool_a_eik),
        inner,
        "but its scopes mapping still resolves it"
    );

    // Completing `toolA` drives the mid-chain hand-off; its events must not carry
    // an empty element_id nor fabricate a completion for the gone inner instance.
    let events = engine
        .apply_command(Command::complete_job_with(
            tool_a.key,
            HashMap::from([("result".to_string(), Value::Str("A".into()))]),
        ))
        .unwrap();
    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::ElementCompleting { element_id, .. } | Event::ElementCompleted { element_id, .. }
                if element_id.is_empty()
        )),
        "no element-completion event carries an empty element_id; events: {events:?}"
    );
    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_instance_key, .. } if *element_instance_key == inner
        )),
        "the already-completed inner instance is not torn down again; events: {events:?}"
    );
    // The chain still hands off — toolB activates despite the gone inner wrapper.
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "toolA" && to == "toolB"
        )),
        "toolA's outgoing inner flow to toolB is still taken; events: {events:?}"
    );
}

fn adhoc_agent_chained_tools_completion_condition_process() -> ProcessDefinition {
    // Like `adhoc_agent_chained_tools_process` (the `toolA -> toolB` structured
    // sequence, issue #1154) but the container also declares a
    // `<completionCondition>=done = true`. When `toolA` completes with
    // `done = true`, the container's completion condition fires MID-CHAIN: the
    // sub-process must complete at once rather than take `toolA`'s outgoing flow
    // and activate the follow-up `toolB`.
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
                <zeebe:taskDefinition type="toolA-type" />
              </bpmn:extensionElements>
              <bpmn:outgoing>chain</bpmn:outgoing>
            </bpmn:serviceTask>
            <bpmn:sequenceFlow id="chain" sourceRef="toolA" targetRef="toolB" />
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="toolB-type" />
              </bpmn:extensionElements>
              <bpmn:incoming>chain</bpmn:incoming>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

/// Regression for the PR #1164 review: a fulfilled `<completionCondition>` must
/// be honoured on the MID-CHAIN hand-off, not just on a leaf tool. Before the
/// fix `complete_adhoc_tool` early-returned into `continue_adhoc_inner_flow`
/// whenever a tool had an outgoing inner flow, skipping ALL completion-condition
/// handling — so a container whose condition became true after `toolA` would
/// still take the flow and activate `toolB` instead of completing. This drives
/// the reproduction: activate `toolA` alone, complete it with `done = true`, and
/// assert the container completes at once, the `toolA -> toolB` flow is NOT
/// taken, and `toolB` never runs.
#[test]
fn adhoc_completion_condition_fires_on_the_mid_chain_handoff() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_chained_tools_completion_condition_process(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");

    // Turn 1: the agent activates ONLY `toolA` (the head of the chain).
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

    // Complete `toolA` returning `done = true`: the completion condition fires on
    // the hand-off, so the flow to `toolB` must NOT be taken.
    let tool_a = engine
        .activate_jobs("toolA-type", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolA")
        .expect("toolA job emitted");
    let events = engine
        .apply_command(Command::complete_job_with(
            tool_a.key,
            HashMap::from([
                ("done".to_string(), Value::Bool(true)),
                ("result".to_string(), Value::Str("A".into())),
            ]),
        ))
        .unwrap();

    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "toolA" && to == "toolB"
        )),
        "a fulfilled completion condition must short-circuit the chain — \
         toolA's outgoing flow to toolB must NOT be taken, got {events:?}"
    );
    assert!(
        engine.is_completed(inst),
        "the container completes at once when its completion condition fires \
         on the mid-chain hand-off"
    );
    assert!(
        engine
            .activate_jobs("toolB-type", "W", 10, 1_000, 0)
            .is_empty(),
        "toolB must never run — the chain was cut short by completion"
    );
    // The mid-chain hand-off must NOT append `toolA`'s output to the
    // `outputCollection`: with the default `cancelRemainingInstances=true`, a
    // fulfilled completion condition CANCELS the in-flight execution path, which
    // therefore drains no leaf and contributes no entry. `outputElement` is a
    // per-execution-path result appended only at a leaf (see
    // `adhoc_inner_sequence_flow_chains_to_the_follow_up_tool`); a truncated,
    // cancelled path is not a leaf, so evaluating the container-level
    // `outputElement` against `toolA`'s non-final scope would be wrong. Lock that
    // `continue_adhoc_inner_flow` hands off with `output: None` even when the
    // condition short-circuits — guarding against a future refactor silently
    // collecting the cancelled tool's partial result ("A").
    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::AdHocToolCompleted {
                output: Some(_),
                ..
            }
        )),
        "the mid-chain short-circuit must drop the tool with output: None — a \
         cancelled path appends nothing to the outputCollection, got {events:?}"
    );
    assert!(
        !matches!(
            engine.instance(inst).unwrap().variables.get("results"),
            Some(Value::List(v)) if v.contains(&Value::Str("A".into()))
        ),
        "toolA's result must never reach the outputCollection when the completion \
         condition short-circuits the chain, got {:?}",
        engine.instance(inst).unwrap().variables.get("results")
    );
}

fn adhoc_agent_with_embedded_subprocess_tool() -> ProcessDefinition {
    // A JOB_WORKER ad-hoc container whose tool `review` is a plain embedded
    // `bpmn:subProcess` with a MULTI-ELEMENT token-flow body
    // (startEvent -> userTask `ask` -> endEvent). This is the camunda.com
    // `/orchestrate/agents/` "Loan decision review" governance construct: a
    // subprocess tool that runs a human review (and, in the full model, a
    // routing gateway) as its own inner flow before the tool completes and
    // feeds the agent loop.
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
            <bpmn:subProcess id="review">
              <bpmn:startEvent id="r_s" />
              <bpmn:userTask id="ask">
                <bpmn:extensionElements>
                  <zeebe:assignmentDefinition assignee="alice" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="r_e" />
              <bpmn:sequenceFlow id="rf1" sourceRef="r_s" targetRef="ask" />
              <bpmn:sequenceFlow id="rf2" sourceRef="ask" targetRef="r_e" />
            </bpmn:subProcess>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

/// Acceptance guard for Magikcraft/nano-bpm#872 (the deferred remainder of #631,
/// which PR #863 closed after delivering only the nested-ad-hoc / agent-of-agents
/// half). Activating an embedded `subProcess` tool must run its multi-element
/// body by token flow — creating the inner `ask` user task and parking the tool
/// until the body reaches its end event — NOT complete the tool immediately.
///
/// Today the tool is classified `AdHocToolKind::Other` and `activate_adhoc_tool`
/// passes it straight through (`Step::Complete`), so no user task is created and
/// this test's `expect` fails. It is `#[ignore]`d so it never reports a failure
/// on `main` while the capability is unimplemented; the engineer who lands #872
/// removes the `#[ignore]` to flip it green. This makes #872's done-state
/// test-defined rather than issue-state-defined (see nano-workforce#313).
#[test]
fn adhoc_agent_runs_an_embedded_subprocess_tool_body_by_token_flow() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_with_embedded_subprocess_tool(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;

    // Turn 1: the agent activates the embedded subProcess tool. Its body must
    // run — creating the inner `ask` user task — and the tool must stay ACTIVE
    // until the body completes, not auto-complete on activation.
    let activated = engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("review")],
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
                ..
            } if element_id == "ask" => Some(*user_task_key),
            _ => None,
        })
        .expect(
            "activating an embedded subProcess tool runs its body: the inner \
             `ask` user task is created (#872)",
        );

    assert!(
        !engine.is_completed(inst),
        "container parks while the tool's inner human task is open (#872)"
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
        1,
        "the subProcess tool child stays active while its body runs (#872)"
    );

    // Completing the inner user task drains the tool body to its end event,
    // which completes the tool — feeding the container's outputCollection and
    // re-emitting the agent job for the next turn.
    let mut vars = HashMap::new();
    vars.insert("result".to_string(), Value::Str("approved".to_string()));
    engine
        .apply_command(Command::complete_user_task_with(user_task_key, vars))
        .unwrap();
    let results = container_output_collection(&engine, inst, container, "results");
    assert_eq!(
        results,
        Some(Value::List(vec![Value::Str("approved".to_string())])),
        "the subProcess tool body's output reaches the container's \
         outputCollection once the body completes (#872)"
    );

    // The tool completed cleanly: its body ran to the end event, so (1) the
    // container's active set is empty again — the child actually drained,
    // rather than the outputCollection being fed while it dangles — and (2) the
    // container iteration advanced, re-emitting the agent job for the next turn.
    // Read both off the recorded container state (side-effect free — do not
    // `activate_jobs` here, which would lock/mutate the re-emitted job), the
    // same way the sibling drain guards in this file do.
    let adhoc = engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .unwrap();
    assert_eq!(
        adhoc.active.len(),
        0,
        "the subProcess tool child drains once its body completes (#872)"
    );
    assert_eq!(
        adhoc.iterations, 1,
        "completing the subProcess tool re-emits the agent job for the next \
         turn (#872)"
    );
}

/// #872 (headline shape): the camunda.com "Loan decision review" governance
/// construct — an embedded subProcess tool whose body is a MULTI-element
/// token-flow with a routing exclusive gateway (human review → gateway → one of
/// two outcomes). Activating it must run the whole body (user task, then the
/// gateway routing) and only complete the tool when the chosen branch reaches an
/// end event — proving token flow past a single leaf, through a gateway.
fn adhoc_agent_with_loan_decision_review_tool() -> ProcessDefinition {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=decision" />
            </bpmn:extensionElements>
            <bpmn:subProcess id="review">
              <bpmn:startEvent id="r_s" />
              <bpmn:userTask id="officer">
                <bpmn:extensionElements>
                  <zeebe:assignmentDefinition assignee="senior" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:exclusiveGateway id="r_gw" />
              <bpmn:endEvent id="r_offer" />
              <bpmn:endEvent id="r_decline" />
              <bpmn:sequenceFlow id="rf1" sourceRef="r_s" targetRef="officer" />
              <bpmn:sequenceFlow id="rf2" sourceRef="officer" targetRef="r_gw" />
              <bpmn:sequenceFlow id="rf3" sourceRef="r_gw" targetRef="r_offer">
                <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">=decision = "approve"</bpmn:conditionExpression>
              </bpmn:sequenceFlow>
              <bpmn:sequenceFlow id="rf4" sourceRef="r_gw" targetRef="r_decline">
                <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">=decision != "approve"</bpmn:conditionExpression>
              </bpmn:sequenceFlow>
            </bpmn:subProcess>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

#[test]
fn adhoc_subprocess_tool_body_routes_through_a_gateway_before_completing() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_with_loan_decision_review_tool(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job");
    let container = agent.element_instance_key;

    let activated = engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("review")],
                ..Default::default()
            },
        ))
        .unwrap();
    let officer_task = activated
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated {
                user_task_key,
                element_id,
                ..
            } if element_id == "officer" => Some(*user_task_key),
            _ => None,
        })
        .expect("the review body's human `officer` task is created (#872)");

    // The reviewer approves: the body must route the gateway to the offer branch
    // and drain to that end event, completing the tool.
    let mut vars = HashMap::new();
    vars.insert("decision".to_string(), Value::Str("approve".to_string()));
    engine
        .apply_command(Command::complete_user_task_with(officer_task, vars))
        .unwrap();

    assert_eq!(
        container_output_collection(&engine, inst, container, "results"),
        Some(Value::List(vec![Value::Str("approve".to_string())])),
        "the routed body's decision reaches the container's outputCollection (#872)"
    );
    let adhoc = engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .unwrap();
    assert_eq!(
        adhoc.active.len(),
        0,
        "the tool drains once the routed branch reaches its end event (#872)"
    );
    assert_eq!(
        adhoc.iterations, 1,
        "completing the routed tool body re-emits the agent job (#872)"
    );
}

#[test]
fn adhoc_cancel_remaining_tears_down_an_open_subprocess_tool_body() {
    // Defect-class guard (#872): cancelling the container while an embedded
    // subProcess tool's body is mid-flight (an open human task inside it) must
    // tear the body down — the inner user-task element instance is completed, not
    // orphaned in the read-model element-instance tree — mirroring the nested
    // ad-hoc teardown #863 added.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_with_embedded_subprocess_tool(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job");
    let container = agent.element_instance_key;
    let activated = engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("review")],
                ..Default::default()
            },
        ))
        .unwrap();
    let ask = activated
        .iter()
        .find_map(|e| match e {
            Event::ElementActivated {
                element_instance_key,
                element_id,
                ..
            } if element_id == "ask" => Some(*element_instance_key),
            _ => None,
        })
        .expect("the body's `ask` user task element is active");
    let ask_task = activated
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated {
                user_task_key,
                element_id,
                ..
            } if element_id == "ask" => Some(*user_task_key),
            _ => None,
        })
        .expect("the body's `ask` user task was created");

    // Cancel the container's remaining instances while the body's human task is
    // still open.
    let events = engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: container,
            activate_elements: Vec::new(),
            cancel_remaining: true,
        })
        .expect("cancel-remaining completes the container");

    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_instance_key, element_id, .. }
                if *element_instance_key == ask && element_id == "ask"
        )),
        "the open body user-task element instance is torn down, not orphaned; events: {events:?}"
    );
    // The parked human task inside the body must be explicitly cancelled, not
    // just its element instance completed — otherwise the `user_tasks` entry
    // stays `Created`, surfacing as an orphaned/open user task after the
    // container is cancelled (#872 cancel defect class).
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::UserTaskCanceled { user_task_key, .. } if *user_task_key == ask_task
        )),
        "the open body user task is cancelled, not left orphaned in Created; events: {events:?}"
    );
    assert_eq!(
        engine.state().user_tasks[&ask_task].state,
        crate::state::UserTaskState::Canceled,
        "the body's user task ends Canceled after the container is cancelled (#872)"
    );
    assert!(engine.is_completed(inst), "the whole instance completes");
    assert!(
        engine
            .instance(inst)
            .map(|i| i.adhoc_instances.is_empty())
            .unwrap_or(true),
        "no ad-hoc runtime state leaks after cancelling an open subProcess tool"
    );
}

#[test]
fn adhoc_subprocess_tool_read_model_tree_nests_container_inner_tool_and_body() {
    // #872 read-model nesting: outer container -> inner instance -> subProcess
    // tool -> its body's leaves. The body's `ask` task hangs off the subProcess
    // tool, which hangs off the `#innerInstance`, which is scoped to the
    // container.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_with_embedded_subprocess_tool(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job");
    let container = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("review")],
                ..Default::default()
            },
        ))
        .unwrap();

    let instance = engine.instance(inst).unwrap();
    let eik_of = |element_id: &str| -> Key {
        *instance
            .active
            .iter()
            .find(|(_, id)| id.as_str() == element_id)
            .map(|(k, _)| k)
            .unwrap_or_else(|| panic!("no active element instance for {element_id}"))
    };
    let scope_of = |eik: Key| -> Key { instance.scopes.get(&eik).copied().unwrap_or(0) };

    let ask = eik_of("ask");
    let review = eik_of("review");
    let inner = eik_of("agent#innerInstance");
    assert_eq!(
        scope_of(ask),
        review,
        "the body task hangs off the subProcess tool"
    );
    assert_eq!(
        scope_of(review),
        inner,
        "the subProcess tool hangs off its dedicated inner instance"
    );
    assert_eq!(
        scope_of(inner),
        container,
        "the inner instance is scoped to the ad-hoc container"
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

// =====================================================================
// Nested ad-hoc sub-process tools (agent-of-agents) — Magikcraft/nano-bpm#631
// =====================================================================

/// An outer JOB_WORKER ad-hoc container (`agent`) whose single tool `subagent`
/// is itself a nested `adHocSubProcess` (a second-level agent) carrying its own
/// `zeebe:taskDefinition`, `outputCollection`, and one leaf service-task tool
/// `leaf`. This is the Zeebe "agent-of-agents" shape: `isAdHocActivity` admits a
/// nested `AD_HOC_SUB_PROCESS` as an activatable tool (issue #631).
fn nested_adhoc_agent_process() -> ProcessDefinition {
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
            <bpmn:adHocSubProcess id="subagent">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="sub-agent-worker" />
                <zeebe:adHoc outputCollection="subResults" outputElement="=leafOut" />
              </bpmn:extensionElements>
              <bpmn:serviceTask id="leaf">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="leaf-tool" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
            </bpmn:adHocSubProcess>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

/// Red for #631: activating a nested `adHocSubProcess` tool must stand up a
/// real second-level container — its own agent job (`sub-agent-worker`), its own
/// registered ad-hoc scope, and its own seeded `outputCollection` — not a plain
/// opaque job. The nested container's completion then feeds the outer
/// container's `outputElement`/loop across the nesting boundary, and the outer
/// container completes normally.
#[test]
fn nested_adhoc_tool_stands_up_a_second_level_container_and_propagates_completion() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(nested_adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    // Outer container's agent job.
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("outer agent job emitted");
    let outer = agent.element_instance_key;

    // Turn 1: outer agent activates the nested `subagent` tool.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("subagent")],
                ..Default::default()
            },
        ))
        .unwrap();

    // The nested container is now an active tool of the outer container.
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&outer)
            .unwrap()
            .active
            .len(),
        1,
        "the nested container is active in the outer container"
    );

    // It must have minted its OWN agent job (second-level worker), and
    // registered its own ad-hoc scope with a seeded outputCollection.
    let sub_agent = engine
        .activate_jobs("sub-agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "subagent")
        .expect("nested container minted its own agent job (second level)");
    let nested = sub_agent.element_instance_key;
    assert!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .contains_key(&nested),
        "the nested container registered its own ad-hoc runtime scope"
    );
    assert_eq!(
        container_output_collection(&engine, inst, nested, "subResults"),
        Some(Value::List(vec![])),
        "the nested container seeded its own empty outputCollection"
    );

    // Second-level turn: the nested agent activates its `leaf` tool.
    engine
        .apply_command(Command::complete_job_with_result(
            sub_agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("leaf")],
                ..Default::default()
            },
        ))
        .unwrap();
    let leaf = engine
        .activate_jobs("leaf-tool", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "leaf")
        .expect("leaf tool job emitted inside the nested container");
    let mut leaf_vars = HashMap::new();
    leaf_vars.insert("leafOut".to_string(), Value::Str("deep".into()));
    engine
        .apply_command(Command::complete_job_with(leaf.key, leaf_vars))
        .unwrap();

    // The leaf output accumulated in the nested container's outputCollection.
    assert_eq!(
        container_output_collection(&engine, inst, nested, "subResults"),
        Some(Value::List(vec![Value::Str("deep".into())])),
        "leaf output accumulated in the nested container's collection"
    );

    // Second-level agent job re-emitted for the next turn; it signals done,
    // completing the nested container, whose completion feeds the OUTER
    // container's outputElement (=result) and loop.
    let sub_agent2 = engine
        .activate_jobs("sub-agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "subagent")
        .expect("nested agent job re-emitted");
    engine
        .apply_command(Command::complete_job_with_result(
            sub_agent2.key,
            {
                let mut m = HashMap::new();
                m.insert("result".to_string(), Value::Str("nested-done".into()));
                m
            },
            crate::model::AdHocJobResult {
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap();

    // The nested container drained out of the outer container's active set.
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&outer)
            .unwrap()
            .active
            .len(),
        0,
        "the nested container completed and left the outer active set"
    );
    // The outer agent job re-emitted for its next turn.
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("outer agent job re-emitted after nested tool completed");
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
        "outer container completed → instance done"
    );
    let results = final_events.iter().find_map(|e| match e {
        Event::VariablesUpdated { variables, .. } => variables.get("results").cloned(),
        _ => None,
    });
    assert_eq!(
        results,
        Some(Value::List(vec![Value::Str("nested-done".into())])),
        "the nested container's result fed the outer outputCollection across the boundary"
    );
}

/// #631 (read-model nesting): while a second-level tool runs, the element-
/// instance tree must nest correctly — leaf tool → its inner instance → nested
/// container → the nested container's inner instance → outer container. A flat
/// or mis-parented tree would break the console trace and Operate-parity audit
/// trail the ADR requires.
#[test]
fn nested_adhoc_read_model_element_tree_nests_correctly() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(nested_adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("outer agent job");
    let outer = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("subagent")],
                ..Default::default()
            },
        ))
        .unwrap();
    let sub_agent = engine
        .activate_jobs("sub-agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "subagent")
        .expect("nested agent job");
    let nested = sub_agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            sub_agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("leaf")],
                ..Default::default()
            },
        ))
        .unwrap();

    let instance = engine.instance(inst).unwrap();
    let eik_of = |element_id: &str| -> Key {
        *instance
            .active
            .iter()
            .find(|(_, id)| id.as_str() == element_id)
            .map(|(k, _)| k)
            .unwrap_or_else(|| panic!("no active element instance for {element_id}"))
    };
    let scope_of = |eik: Key| -> Key { instance.scopes.get(&eik).copied().unwrap_or(0) };

    let leaf = eik_of("leaf");
    let nested_inner = eik_of("subagent#innerInstance");
    let outer_inner = eik_of("agent#innerInstance");

    assert_eq!(
        scope_of(leaf),
        nested_inner,
        "leaf hangs off its inner instance"
    );
    assert_eq!(
        scope_of(nested_inner),
        nested,
        "the leaf's inner instance is scoped to the nested container"
    );
    assert_eq!(
        scope_of(nested),
        outer_inner,
        "the nested container hangs off its own inner instance in the outer container"
    );
    assert_eq!(
        scope_of(outer_inner),
        outer,
        "the nested container's inner instance is scoped to the outer container"
    );
}

/// An outer JOB_WORKER container whose nested `subagent` declares a
/// `<completionCondition>` (`=done = true`) and holds TWO leaf tools.
/// Once the first leaf drains the nested container's condition fires and — with
/// the default `cancelRemainingInstances=true` — cancels the still-running
/// second leaf, then completes and feeds the OUTER container across the nesting
/// boundary.
fn nested_adhoc_cancel_process() -> ProcessDefinition {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=done" />
            </bpmn:extensionElements>
            <bpmn:adHocSubProcess id="subagent">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="sub-agent-worker" />
                <zeebe:adHoc outputCollection="subResults" outputElement="=leafOut" />
              </bpmn:extensionElements>
              <bpmn:completionCondition>=done = true</bpmn:completionCondition>
              <bpmn:serviceTask id="leaf1">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="leaf-tool" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:serviceTask id="leaf2">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="leaf-tool" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
            </bpmn:adHocSubProcess>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

/// #631 (cancel propagation across the boundary): a nested container whose
/// `<completionCondition>` fires cancels its remaining second-level tools, then
/// completes and feeds the outer container's loop — its still-running tool is
/// not orphaned and the outer container advances.
#[test]
fn nested_adhoc_completion_condition_cancels_remaining_and_feeds_parent() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(nested_adhoc_cancel_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("outer agent job");
    let outer = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("subagent")],
                ..Default::default()
            },
        ))
        .unwrap();
    let sub_agent = engine
        .activate_jobs("sub-agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "subagent")
        .expect("nested agent job");
    let nested = sub_agent.element_instance_key;

    // Nested agent activates BOTH leaves in one turn.
    engine
        .apply_command(Command::complete_job_with_result(
            sub_agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("leaf1"), activate_element("leaf2")],
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&nested)
            .unwrap()
            .active
            .len(),
        2,
        "both nested leaves active"
    );

    // Drain only leaf1: its output makes the nested completionCondition true, so
    // the nested container cancels leaf2 and completes — crossing the boundary
    // into the outer container.
    let leaf_jobs = engine.activate_jobs("leaf-tool", "W", 10, 1_000, 0);
    let leaf1 = leaf_jobs
        .iter()
        .find(|j| j.element_id == "leaf1")
        .expect("leaf1 job");
    let mut vars = HashMap::new();
    vars.insert("leafOut".to_string(), Value::Str("x".into()));
    vars.insert("done".to_string(), Value::Bool(true));
    engine
        .apply_command(Command::complete_job_with(leaf1.key, vars))
        .unwrap();

    // The nested container is gone (completed) and left the outer active set.
    assert!(
        !engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .contains_key(&nested),
        "the nested container completed and dropped its runtime state"
    );
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&outer)
            .unwrap()
            .active
            .len(),
        0,
        "the nested container left the outer active set (cancel crossed the boundary)"
    );
    assert_eq!(
        container_output_collection(&engine, inst, outer, "results"),
        Some(Value::List(vec![Value::Bool(true)])),
        "the cancelled nested container still fed the outer outputElement"
    );
    // The nested container's OWN `outputCollection` (`subResults`) is an internal
    // detail of the nested scope — it must NOT leak across the nesting boundary
    // into the enclosing scope. Its result crosses only via the parent's
    // outputElement (`results`, asserted above). Before the fix, completing the
    // nested container propagated `subResults` out of its scope, landing it in the
    // root instance variables (no ancestor scope defines it).
    assert!(
        !engine
            .instance(inst)
            .unwrap()
            .variables
            .contains_key("subResults"),
        "the nested container's internal outputCollection did not leak into the root scope"
    );
    assert_eq!(
        container_output_collection(&engine, inst, outer, "subResults"),
        None,
        "the nested container's internal outputCollection did not leak into the outer scope"
    );

    // The outer agent job re-emitted; complete the run.
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("outer agent job re-emitted");
    engine
        .apply_command(Command::complete_job_with_result(
            agent2.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap();
    assert!(engine.is_completed(inst), "instance completes");
}

/// #631 (parent cancels a nested tool): when the OUTER container completes with
/// `cancelRemainingInstances=true` while a NESTED ad-hoc container tool is still
/// active, the nested container must be torn down RECURSIVELY — its own active
/// descendants (second-level tools + their jobs) cancelled, its element instance
/// completed, and its `adhoc_instances` runtime state dropped (`AdHocCompleted`).
/// Before the fix the outer cancel loop treated the nested container as a leaf
/// tool (`cancel_mi_child_events`), orphaning the nested container's leaf job +
/// element instance and leaking its ad-hoc state (no `AdHocCompleted`).
#[test]
fn nested_adhoc_parent_cancel_recursively_tears_down_nested_container() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(nested_adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    // Outer agent activates the nested `subagent` tool.
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("outer agent job");
    let outer = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("subagent")],
                ..Default::default()
            },
        ))
        .unwrap();

    // Nested agent activates its `leaf` tool, which mints a leaf job left
    // in-flight (never completed).
    let sub_agent = engine
        .activate_jobs("sub-agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "subagent")
        .expect("nested agent job");
    let nested = sub_agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            sub_agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("leaf")],
                ..Default::default()
            },
        ))
        .unwrap();
    let leaf_job = engine
        .activate_jobs("leaf-tool", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "leaf")
        .expect("leaf tool job in-flight inside the nested container");
    let leaf = leaf_job.element_instance_key;
    assert!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .contains_key(&nested),
        "nested container active before the parent cancels"
    );

    // Cancel the OUTER container's remaining instances while the nested container
    // (and its leaf) are still running.
    let events = engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: outer,
            activate_elements: Vec::new(),
            cancel_remaining: true,
        })
        .expect("cancel-remaining completes the outer container");

    // The nested container's ad-hoc runtime state was dropped (recursive
    // teardown), not left dangling.
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::AdHocCompleted { container_key, cancelled, .. }
                if *container_key == nested && *cancelled
        )),
        "the nested container emitted AdHocCompleted (its ad-hoc state was dropped); events: {events:?}"
    );
    // The nested container's leaf descendant's job was cancelled — not orphaned.
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::JobCanceled { job_key, .. } if *job_key == leaf_job.key
        )),
        "the nested container's in-flight leaf job was cancelled; events: {events:?}"
    );
    // The leaf descendant's element instance was completed — not left orphaned in
    // the read-model element-instance tree.
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_instance_key, element_id, .. }
                if *element_instance_key == leaf && element_id == "leaf"
        )),
        "the nested container's leaf element instance was completed; events: {events:?}"
    );
    assert!(engine.is_completed(inst), "the whole instance completes");
    assert!(
        engine
            .instance(inst)
            .map(|i| i.adhoc_instances.is_empty())
            .unwrap_or(true),
        "no ad-hoc runtime state leaks after the recursive cancel"
    );
}

/// #631 robustness (Copilot review, PR #863): the recursive cancel helper
/// `cancel_adhoc_active_child` also tears down the dedicated inner wrapper
/// instance each tool hangs off. That inner instance's `scopes` mapping can
/// linger after it has already left `active` (been completed), in which case its
/// element id no longer resolves. Emitting `ElementCompleting`/`ElementCompleted`
/// with an EMPTY element_id in that case corrupts downstream element aggregates —
/// so the teardown must be skipped when the id can't be resolved (mirroring the
/// defensive skip on the `ModifyInstance` termination path). Before the fix the
/// inner teardown used `element_id_of_instance(..).unwrap_or_default()`, emitting
/// a completion for an id of `""`.
#[test]
fn nested_adhoc_cancel_child_skips_already_completed_inner_instance() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(nested_adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    // Outer agent activates the nested `subagent` tool (which hangs off a
    // dedicated `agent#innerInstance` wrapper instance).
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("outer agent job");
    let outer = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("subagent")],
                ..Default::default()
            },
        ))
        .unwrap();
    let sub_agent = engine
        .activate_jobs("sub-agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "subagent")
        .expect("nested agent job");
    let nested = sub_agent.element_instance_key;

    // The inner wrapper the nested tool hangs off.
    let inner = engine.scope_of(inst, nested);
    assert_ne!(inner, 0, "nested tool hangs off a dedicated inner instance");
    assert_ne!(
        inner, outer,
        "the inner wrapper is distinct from the container"
    );

    // Simulate that inner wrapper having ALREADY been torn down: drop it from
    // `active` (so its element id no longer resolves) while its `scopes` mapping
    // still resolves it — the exact state the old `unwrap_or_default()` mishandled.
    engine
        .state
        .instances
        .get_mut(&inst)
        .unwrap()
        .active
        .remove(&inner);
    assert!(
        engine.element_id_of_instance(inst, inner).is_none(),
        "inner wrapper is no longer active"
    );
    assert_eq!(
        engine.scope_of(inst, nested),
        inner,
        "but its scopes mapping still resolves it"
    );

    let events = engine.cancel_adhoc_active_child(inst, outer, nested);

    // No element-completion event may carry an empty element_id.
    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::ElementCompleting { element_id, .. } | Event::ElementCompleted { element_id, .. }
                if element_id.is_empty()
        )),
        "no element-completion event carries an empty element_id; events: {events:?}"
    );
    // And it must not fabricate a completion for the already-gone inner instance.
    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_instance_key, .. } if *element_instance_key == inner
        )),
        "the already-completed inner instance is not torn down again; events: {events:?}"
    );
}

#[test]
fn subprocess_end_event_output_propagates_reached_branch_not_last_defined() {
    // Regression: several end events inside one sub-process, each carrying a
    // `zeebe:output` for the SAME target, must each attach to their own end
    // event — so when a token reaches ONE of them, only that branch's output
    // propagates to the parent scope. Previously the parser hoisted every
    // end-event mapping onto the enclosing sub-process, so the last-parsed one
    // ("escalate") clobbered the rest at sub-process completion, and a "fixed"
    // outcome routed as "escalate" (nano-workforce merge-loop #466). Driven
    // through a job so the token completes across a drain boundary, as the real
    // model does.
    let xml = r#"
      <bpmn:definitions
          xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="sp-ends" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="sub" />
          <bpmn:subProcess id="sub">
            <bpmn:startEvent id="ss" />
            <bpmn:sequenceFlow id="f1" sourceRef="ss" targetRef="work" />
            <bpmn:serviceTask id="work">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="work" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="g" />
            <bpmn:exclusiveGateway id="g" default="fB" />
            <bpmn:sequenceFlow id="fA" sourceRef="g" targetRef="endA">
              <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">=pick = "A"</bpmn:conditionExpression>
            </bpmn:sequenceFlow>
            <bpmn:sequenceFlow id="fB" sourceRef="g" targetRef="endB" />
            <bpmn:endEvent id="endA">
              <bpmn:extensionElements>
                <zeebe:ioMapping><zeebe:output source="=&#34;A&#34;" target="outcome" /></zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:endEvent>
            <bpmn:endEvent id="endB">
              <bpmn:extensionElements>
                <zeebe:ioMapping><zeebe:output source="=&#34;B&#34;" target="outcome" /></zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:endEvent>
          </bpmn:subProcess>
          <bpmn:sequenceFlow id="f3" sourceRef="sub" targetRef="done" />
          <bpmn:endEvent id="done" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    // Parser-level guard: each end event owns its mapping; the sub-process owns none.
    assert!(
        def.element("sub").unwrap().io.outputs.is_empty(),
        "mapping must NOT hoist onto the sub-process"
    );
    assert_eq!(def.element("endA").unwrap().io.outputs.len(), 1);
    assert_eq!(def.element("endB").unwrap().io.outputs.len(), 1);

    for (pick, want, unwanted) in [("A", "A", "B"), ("B", "B", "A")] {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(def.clone()))
            .unwrap();
        let mut vars = HashMap::new();
        vars.insert("pick".to_string(), Value::Str(pick.to_string()));
        engine
            .apply_command(Command::create_instance_with("sp-ends", vars))
            .unwrap();
        let events = complete_one(&mut engine, "work");
        assert!(
            events.iter().any(|e| matches!(e,
                Event::VariablesUpdated { variables, .. }
                    if variables.get("outcome") == Some(&Value::Str(want.to_string())))),
            "pick={pick}: reached end event must propagate outcome={want}; events: {events:?}"
        );
        assert!(
            !events.iter().any(|e| matches!(e,
                Event::VariablesUpdated { variables, .. }
                    if variables.get("outcome") == Some(&Value::Str(unwanted.to_string())))),
            "pick={pick}: unreached end event must NOT propagate outcome={unwanted}; events: {events:?}"
        );
    }
}

#[test]
fn agent_task_activation_creates_a_job_then_worker_registers_agent_instance() {
    // Deploying a serviceTask bearing zeebe:agentDefinition agentType="aiAgentTask"
    // creates an ordinary service-task job. Only explicit worker registration
    // mints an AgentInstance linked to the active elementInstanceKey.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="agent-proc" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:agentDefinition agentType="aiAgentTask" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="end" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let events = engine
        .apply_command(Command::create_instance("agent-proc"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The agent element activated (its token parks, like a service task).
    let agent_eik = events
        .iter()
        .find_map(|e| match e {
            Event::ElementActivated {
                element_instance_key,
                element_id,
                ..
            } if element_id == "agent" => Some(*element_instance_key),
            _ => None,
        })
        .expect("the agent element should activate");

    assert!(events
        .iter()
        .any(|e| matches!(e, Event::JobCreated { element_id, .. } if element_id == "agent")));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::AgentInstanceCreated { .. })));
    assert!(engine.state.instances[&instance_key]
        .agent_instances
        .is_empty());

    let agent_instance = register_job_backed_agent(&mut engine, "agent");
    assert_eq!(
        agent_instance.status,
        crate::agent::AgentInstanceStatus::Initializing
    );
    assert_eq!(agent_instance.element_instance_key, agent_eik);
    assert_eq!(agent_instance.process_instance_key, instance_key);
    assert_eq!(
        agent_instance.agent_type,
        crate::agent::AgentType::AiAgentTask
    );
    assert_ne!(
        agent_instance.agent_instance_key, agent_eik,
        "the AgentInstance must have its own dedicated key, distinct from the element instance"
    );
    assert_ne!(agent_instance.agent_instance_key, 0);

    // The instance is the system-of-record: it is held in engine state.
    let stored = engine
        .state
        .instances
        .get(&instance_key)
        .and_then(|pi| pi.agent_instances.get(&agent_instance.agent_instance_key))
        .expect("the AgentInstance should be stored on the process instance");
    assert_eq!(
        stored.status,
        crate::agent::AgentInstanceStatus::Initializing
    );

    let job = &engine.state.jobs[&agent_instance.job_key];
    assert_eq!(job.element_instance_key, agent_eik);
    assert_eq!(job.lease_token, Some(agent_instance.job_lease.clone()));

    let events = engine
        .apply_command(Command::CompleteAgentInstance {
            agent_instance_key: agent_instance.agent_instance_key,
        })
        .unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::AgentInstanceCompleted { .. })));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::ElementCompleted { .. })));
    assert!(engine.state.jobs.contains_key(&agent_instance.job_key));

    let events = engine
        .apply_command(
            Command::complete_job(agent_instance.job_key).with_job_lease(agent_instance.job_lease),
        )
        .unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::ElementCompleted { element_id, .. } if element_id == "agent")));
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::ElementActivated { element_id, .. } if element_id == "end")));
}

// --- external (job-backed) agent parity (Camunda 8.10, #1099) ---------------

/// Deploy an `external` agent (a `serviceTask` bearing
/// `zeebe:agentDefinition agentType="external"`), start an instance, and return
/// the engine, the process-instance key and the agent element's active
/// element-instance key. All agent tasks auto-mint **no** AgentInstance —
/// they are job-backed — so the element-instance key is read
/// from the `ElementActivated` event, not from a minted record.
fn external_agent_instance() -> (Engine, Key, Key) {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="agent-proc" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:agentDefinition agentType="external" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="end" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("agent-proc"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    let eik = events
        .iter()
        .find_map(|e| match e {
            Event::ElementActivated {
                element_instance_key,
                element_id,
                ..
            } if element_id == "agent" => Some(*element_instance_key),
            _ => None,
        })
        .expect("the agent element should activate");
    (engine, instance_key, eik)
}

#[test]
fn external_agent_activation_creates_a_job_and_no_agent_instance() {
    // Camunda parity (#1099): an `external` agent is job-backed. On activation it
    // creates a normal job (activatable through the standard job loop) and does
    // NOT auto-mint an AgentInstance — the worker mints it via CREATE.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="agent-proc" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:agentDefinition agentType="external" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="end" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("agent-proc"))
        .unwrap();

    // A job was created on activation.
    let job_created = events
        .iter()
        .any(|e| matches!(e, Event::JobCreated { element_id, .. } if element_id == "agent"));
    assert!(
        job_created,
        "an external agent must create a job on activation"
    );

    // NO AgentInstance was auto-minted.
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::AgentInstanceCreated { .. })),
        "an external agent must not auto-mint an AgentInstance"
    );

    // The job is activatable through the standard job loop (job type = element id).
    let activated = engine.activate_jobs("agent", "W", 10, 1_000, 0);
    assert_eq!(
        activated.len(),
        1,
        "the external agent's job is activatable"
    );
    assert_eq!(activated[0].element_id, "agent");
}

#[test]
fn external_agent_lease_gated_create_mints_on_a_valid_job_lease() {
    use crate::agent::{AgentDefinition, AgentInstanceStatus};
    let (mut engine, pi, eik) = external_agent_instance();

    // The worker activates the agent job (standard job loop) and learns its
    // lease deadline — the lease "token".
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("the external agent's job is activatable");

    // A lease-gated CREATE that references the ACTIVATED job with the matching
    // lease and elementInstanceKey mints the AgentInstance (INITIALIZING).
    let events = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job.key,
            job_lease: job
                .lease_token
                .clone()
                .expect("external agent job carries a lease"),
            definition: AgentDefinition {
                model: Some("gpt-4o".to_string()),
                ..Default::default()
            },
            limits: None,
            history: vec![],
        })
        .unwrap();
    let created = events
        .iter()
        .find_map(|e| match e {
            Event::AgentInstanceCreated { agent_instance, .. } => Some(agent_instance.clone()),
            _ => None,
        })
        .expect("a valid lease-gated CREATE mints the AgentInstance");
    assert_eq!(created.status, AgentInstanceStatus::Initializing);
    assert_eq!(created.element_instance_key, eik);
    assert_eq!(created.agent_type, crate::agent::AgentType::External);
    // The validated job lease is recorded on the AgentInstance.
    assert_eq!(created.job_key, job.key);
    assert_eq!(
        created.job_lease,
        job.lease_token
            .clone()
            .expect("external agent job carries a lease")
    );
    assert_eq!(
        stored_agent_instance(&engine, pi, created.agent_instance_key).status,
        AgentInstanceStatus::Initializing
    );
}

#[test]
fn external_agent_create_without_an_activated_job_is_rejected() {
    use crate::agent::AgentDefinition;
    let (mut engine, _pi, eik) = external_agent_instance();

    // The job exists but has NOT been activated: no lease to gate on → reject.
    let job_key = engine
        .state
        .jobs
        .values()
        .find(|j| j.element_instance_key == eik)
        .map(|j| j.key)
        .expect("the external agent has a job");
    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key,
            job_lease: "not-activated".to_string(),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(
        matches!(err, EngineError::AgentInstanceJobNotActive { .. }),
        "CREATE on a non-activated job must be rejected, got {err:?}"
    );
    assert!(
        engine
            .state
            .instances
            .get(&_pi)
            .map(|pi| pi.agent_instances.is_empty())
            .unwrap_or(true),
        "a rejected CREATE mints nothing"
    );
}

#[test]
fn external_agent_create_with_a_stale_lease_token_is_rejected() {
    use crate::agent::AgentDefinition;
    let (mut engine, _pi, eik) = external_agent_instance();
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("activatable");

    // Right job, right element, but a lease token (deadline) that does not match.
    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job.key,
            job_lease: "stale-lease".to_string(),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(
        matches!(err, EngineError::AgentInstanceJobLeaseMismatch { .. }),
        "a mismatched lease token must be rejected, got {err:?}"
    );
}

#[test]
fn external_agent_create_with_a_foreign_element_instance_is_rejected() {
    use crate::agent::AgentDefinition;
    let (mut engine, _pi, eik) = external_agent_instance();
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("activatable");

    // The job's element instance must match the CREATE's element_instance_key.
    // Assert with a different (inactive) element instance key → rejected as
    // inactive before it can borrow this job's lease.
    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik + 777,
            job_key: job.key,
            job_lease: job
                .lease_token
                .clone()
                .expect("external agent job carries a lease"),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            EngineError::AgentInstanceElementInstanceInactive { .. }
                | EngineError::AgentInstanceJobElementMismatch { .. }
        ),
        "a foreign element instance must be rejected, got {err:?}"
    );
    let _ = eik;
}

#[test]
fn external_agent_repeat_create_preserves_the_existing_agent_instance() {
    use crate::agent::AgentDefinition;
    let (mut engine, pi, eik) = external_agent_instance();
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("activatable");

    // First CREATE mints the AgentInstance.
    let first = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job.key,
            job_lease: job
                .lease_token
                .clone()
                .expect("external agent job carries a lease"),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap();
    let aik = first
        .iter()
        .find_map(|e| match e {
            Event::AgentInstanceCreated { agent_instance, .. } => {
                Some(agent_instance.agent_instance_key)
            }
            _ => None,
        })
        .expect("first CREATE mints");

    // A second CREATE conflicts without changing the existing registration.
    let second = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job.key,
            job_lease: job
                .lease_token
                .clone()
                .expect("external agent job carries a lease"),
            definition: AgentDefinition {
                model: Some("gpt-4o".to_string()),
                ..Default::default()
            },
            limits: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(
        matches!(second, EngineError::AgentInstanceAlreadyExists { agent_instance_key, .. } if agent_instance_key == aik)
    );
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).definition.model,
        None
    );
    assert_eq!(
        engine
            .state
            .instances
            .get(&pi)
            .map(|pi| pi.agent_instances.len())
            .unwrap_or(0),
        1,
        "exactly one AgentInstance for the element"
    );
}

#[test]
fn external_agent_job_completion_advances_the_token() {
    use crate::agent::AgentDefinition;
    let (mut engine, _pi, eik) = external_agent_instance();
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("activatable");
    engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job.key,
            job_lease: job
                .lease_token
                .clone()
                .expect("external agent job carries a lease"),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap();

    // Completing the agent job resumes the parked token and routes the outgoing
    // flow, exactly like a service task — the harness drives the standard loop.
    let events = engine
        .apply_command(Command::complete_job(job.key).with_job_lease(job.lease_token.unwrap()))
        .unwrap();
    assert!(
        events.iter().any(
            |e| matches!(e, Event::ElementCompleted { element_id, .. } if element_id == "agent")
        ),
        "the agent element completes when its job completes"
    );
    assert!(
        events.iter().any(
            |e| matches!(e, Event::ElementActivated { element_id, .. } if element_id == "end")
        ),
        "the token advances to the end event"
    );
}

#[test]
fn external_agent_history_bearing_update_is_lease_gated() {
    use crate::agent::{AgentDefinition, AgentHistoryRole};
    let (mut engine, pi, eik) = external_agent_instance();
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("activatable");
    let created = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job.key,
            job_lease: job
                .lease_token
                .clone()
                .expect("external agent job carries a lease"),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap();
    let aik = created
        .iter()
        .find_map(|e| match e {
            Event::AgentInstanceCreated { agent_instance, .. } => {
                Some(agent_instance.agent_instance_key)
            }
            _ => None,
        })
        .expect("CREATE mints");

    // A history-bearing UPDATE with a stale lease token is rejected.
    let err = engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "agent".to_string(),
            process_instance_key: pi,
            job_key: job.key,
            job_lease: "stale-lease".to_string(),
            status: None,
            metrics: Default::default(),
            tools: None,
            history: vec![history_turn(0, 200, AgentHistoryRole::Assistant)],
        })
        .unwrap_err();
    assert!(
        matches!(err, EngineError::AgentInstanceJobLeaseMismatch { .. }),
        "a history-bearing UPDATE with a bad lease is rejected, got {err:?}"
    );

    // A history-free UPDATE (pure status advance) is NOT gated.
    engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "agent".to_string(),
            process_instance_key: pi,
            job_key: 0,
            job_lease: String::new(),
            status: Some(crate::agent::AgentInstanceStatus::Thinking),
            metrics: Default::default(),
            tools: None,
            history: vec![],
        })
        .expect("a history-free UPDATE is not lease-gated");

    // A history-bearing UPDATE with the valid lease is accepted and appends.
    let ok = engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "agent".to_string(),
            process_instance_key: pi,
            job_key: job.key,
            job_lease: job
                .lease_token
                .clone()
                .expect("external agent job carries a lease"),
            status: None,
            metrics: Default::default(),
            tools: None,
            history: vec![worker_history_turn(1, 300, AgentHistoryRole::Assistant)],
        })
        .expect("a history-bearing UPDATE with a valid lease is accepted");
    assert!(
        ok.iter()
            .any(|e| matches!(e, Event::AgentHistoryCreated { .. })),
        "the valid-lease UPDATE appends a history record"
    );
}

#[test]
fn external_agent_refreshes_job_lease_across_reactivation() {
    use crate::agent::{AgentDefinition, AgentHistoryRole};
    let (mut engine, pi, eik) = external_agent_instance();

    // First activation → lease token L1; a lease-gated CREATE records it.
    let job1 = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("activatable");
    let created = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job1.key,
            job_lease: job1
                .lease_token
                .clone()
                .expect("first activation carries a lease"),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap();
    let aik = created
        .iter()
        .find_map(|e| match e {
            Event::AgentInstanceCreated { agent_instance, .. } => {
                Some(agent_instance.agent_instance_key)
            }
            _ => None,
        })
        .expect("CREATE mints");
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).job_lease,
        job1.lease_token
            .clone()
            .expect("first activation carries a lease")
    );

    // The lease expires and the worker re-activates the SAME job, learning a
    // NEW lease token L2 (an opaque token, distinct from the deadline and from
    // the previous activation's token).
    engine
        .apply_command(Command::ExpireJobs { now: job1.deadline })
        .unwrap();
    let job2 = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 5_000, lease_options())
        .pop()
        .expect("re-activatable after lease expiry");
    assert_eq!(job2.key, job1.key, "same job, fresh lease");
    assert_ne!(
        job2.lease_token, job1.lease_token,
        "re-activation mints a fresh lease token"
    );

    // CREATE still conflicts after reactivation; UPDATE owns reassociation.
    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job2.key,
            job_lease: job2
                .lease_token
                .clone()
                .expect("second activation carries a lease"),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::AgentInstanceAlreadyExists { .. }
    ));
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).job_lease,
        job1.lease_token
            .clone()
            .expect("first activation carries a lease"),
        "a rejected CREATE must not change the registration"
    );

    // A history-bearing UPDATE under L2 is accepted AND likewise records the
    // freshly-validated lease token onto the snapshot.
    engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "agent".to_string(),
            process_instance_key: pi,
            job_key: job2.key,
            job_lease: job2
                .lease_token
                .clone()
                .expect("second activation carries a lease"),
            status: None,
            metrics: Default::default(),
            tools: None,
            history: vec![worker_history_turn(1, 200, AgentHistoryRole::Assistant)],
        })
        .expect("a history-bearing UPDATE under the fresh lease is accepted");
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).job_lease,
        job2.lease_token
            .clone()
            .expect("second activation carries a lease"),
        "a history-bearing UPDATE records the validated lease token"
    );
}

/// #1106 divergence 1 — the lease token is a distinct **opaque per-activation**
/// value, NOT the job's `deadline`. The pre-#1106 engine reused `deadline` as
/// the token; a distinct token is what lets a lease survive a `deadline`-moving
/// lock extension (see the next test).
#[test]
fn external_agent_lease_token_is_distinct_from_the_deadline() {
    let (mut engine, _pi, _eik) = external_agent_instance();
    // Use a deliberately large deadline (`now + timeout`) so that the monotonic
    // lease token (minted from `mint_key()`, i.e. small keys) cannot accidentally
    // equal it — the assertion below checks semantic distinctness, not a value
    // collision.
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000_000_000, 100, lease_options())
        .pop()
        .expect("activatable");
    let token = job
        .lease_token
        .expect("an external agent job activates with a lease token");
    assert_ne!(
        token,
        job.deadline.to_string(),
        "the opaque lease token must not be the activation deadline"
    );
}

/// #1106 divergence 1 — because the lease token is independent of `deadline`, a
/// lock extension (`UpdateJobTimeout`, which moves `deadline` but is NOT a
/// re-activation) leaves the lease valid. The pre-#1106 engine, keying the lease
/// off `deadline`, would have spuriously rejected the worker's still-live lease
/// after any timeout extension.
#[test]
fn external_agent_lease_survives_a_job_timeout_extension() {
    use crate::agent::AgentDefinition;
    let (mut engine, _pi, eik) = external_agent_instance();
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("activatable");
    let lease = job.lease_token.expect("lease token");

    // Extend the lock — this moves the deadline but must NOT change the token.
    engine
        .apply_command_at(Command::update_job_timeout(job.key, 50_000), 200)
        .expect("a lock extension on an activated job succeeds");
    let moved_deadline = engine.job(job.key).and_then(|j| j.deadline);
    assert_ne!(
        moved_deadline,
        Some(job.deadline),
        "the timeout extension moved the deadline"
    );

    // The worker's original lease token still validates.
    engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job.key,
            job_lease: lease,
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .expect("the lease survives a deadline-moving lock extension");
}

/// #1106 divergence 2 — an `external` CREATE is allowed to be **jobless**
/// (`job_key == 0`) when it carries no history batch (Camunda's `jobKey == -1`
/// short-circuit), but the job becomes required the moment a batch is attached.
#[test]
fn external_agent_create_is_jobless_only_without_history() {
    use crate::agent::{AgentDefinition, AgentHistoryRole};
    let (mut engine, _pi, eik) = external_agent_instance();

    // Jobless CREATE + no history → mints (no live job required yet).
    let ok = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: 0,
            job_lease: String::new(),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .expect("a jobless, history-free CREATE is allowed");
    assert!(
        ok.iter()
            .any(|e| matches!(e, Event::AgentInstanceCreated { .. })),
        "the jobless CREATE mints the AgentInstance"
    );

    // Jobless CREATE + a history batch → rejected: a batch must be attributed to
    // the active job that produced it.
    let (mut engine, _pi, eik) = external_agent_instance();
    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: 0,
            job_lease: String::new(),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![history_turn(0, 200, AgentHistoryRole::Assistant)],
        })
        .unwrap_err();
    assert!(
        matches!(err, EngineError::AgentInstanceJobRequiredForHistory { .. }),
        "a jobless CREATE carrying history must be rejected, got {err:?}"
    );
}

/// #1106 divergence 3 — a **history-free** UPDATE that nonetheless supplies a
/// `job_key` must still validate it (active + matching lease + element). Only a
/// fully job-optional UPDATE (no job AND no history) skips the gate. The
/// pre-#1106 engine skipped validation whenever history was empty, silently
/// accepting a stale/foreign job.
#[test]
fn external_agent_history_free_update_validates_a_supplied_job() {
    use crate::agent::AgentDefinition;
    let (mut engine, pi, eik) = external_agent_instance();
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("activatable");
    let lease = job.lease_token.expect("lease token");
    let created = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job.key,
            job_lease: lease,
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap();
    let aik = created
        .iter()
        .find_map(|e| match e {
            Event::AgentInstanceCreated { agent_instance, .. } => {
                Some(agent_instance.agent_instance_key)
            }
            _ => None,
        })
        .expect("CREATE mints");

    // A history-free UPDATE that supplies a STALE lease token is rejected.
    let err = engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "agent".to_string(),
            process_instance_key: pi,
            job_key: job.key,
            job_lease: "stale-lease".to_string(),
            status: Some(crate::agent::AgentInstanceStatus::Thinking),
            metrics: Default::default(),
            tools: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(
        matches!(err, EngineError::AgentInstanceJobLeaseMismatch { .. }),
        "a history-free UPDATE with a supplied-but-stale job must be rejected, got {err:?}"
    );

    // A fully job-optional UPDATE (no job, no history) remains ungated.
    engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "agent".to_string(),
            process_instance_key: pi,
            job_key: 0,
            job_lease: String::new(),
            status: Some(crate::agent::AgentInstanceStatus::Thinking),
            metrics: Default::default(),
            tools: None,
            history: vec![],
        })
        .expect("a job-optional, history-free UPDATE is not gated");
}

/// #1106 divergence 4 — an ordinary (non-agent) job activates **lease-less**
/// (`lease_token == None`, Camunda's `!hasLeaseToken()`), so the lease
/// comparison is skipped for it. This is what makes the token-carrying gate on
/// `validate_agent_job_context` conditional rather than unconditional.
#[test]
fn ordinary_job_activates_lease_less() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:serviceTask id="svc">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="work" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="svc" />
          <bpmn:sequenceFlow id="f2" sourceRef="svc" targetRef="end" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine.apply_command(Command::create_instance("p")).unwrap();
    let job = engine
        .activate_jobs("work", "W", 1, 1_000, 100)
        .pop()
        .expect("the service task job is activatable");
    assert_eq!(
        job.lease_token, None,
        "an ordinary (non-agent) job must activate lease-less"
    );
}

// --- AgentHistory turn log (Camunda 8.10 parity, Stage 3 / slice S2) --------

/// Deploy an `aiAgentTask` service task, start an instance, register its agent,
/// and return the engine together with the owning process-instance key and the
/// `agent_instance_key` — the fixture every AgentHistory test builds on.
fn agent_instance_for_history() -> (Engine, Key, Key) {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="agent-proc" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:agentDefinition agentType="aiAgentTask" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="end" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("agent-proc"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    let agent_instance_key = register_job_backed_agent(&mut engine, "agent").agent_instance_key;
    (engine, instance_key, agent_instance_key)
}

fn lease_options() -> crate::JobActivationOptions {
    crate::JobActivationOptions {
        with_lease: true,
        ..Default::default()
    }
}

/// Activate the fixture's job and explicitly register its agent using that lease.
fn register_job_backed_agent(engine: &mut Engine, job_type: &str) -> crate::agent::AgentInstance {
    register_job_backed_agent_with_history(engine, job_type, Vec::new())
}

fn register_job_backed_agent_with_history(
    engine: &mut Engine,
    job_type: &str,
    history: Vec<crate::agent::AgentHistoryTurn>,
) -> crate::agent::AgentInstance {
    let job = engine
        .activate_jobs_with_options(job_type, "W", 1, 60_000, 0, lease_options())
        .pop()
        .expect("the agent job should be available for activation");
    engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: job.element_instance_key,
            job_key: job.key,
            job_lease: job.lease_token.expect("an activated job has a lease"),
            definition: crate::agent::AgentDefinition::default(),
            limits: None,
            history,
        })
        .unwrap()
        .into_iter()
        .find_map(|event| match event {
            Event::AgentInstanceCreated { agent_instance, .. } => Some(agent_instance),
            _ => None,
        })
        .expect("worker registration should mint an AgentInstance")
}

fn worker_history_turn(
    iteration: i32,
    produced_at: u64,
    role: crate::agent::AgentHistoryRole,
) -> crate::agent::AgentHistoryTurn {
    let mut turn = history_turn(iteration, produced_at, role);
    turn.history_item_id = Some(format!("{iteration}-{produced_at}"));
    if role == crate::agent::AgentHistoryRole::Configuration {
        turn.model = Some("gpt-4o".into());
        turn.provider = Some("test-provider".into());
        turn.system_prompt = Some(vec![crate::agent::AgentHistoryContent {
            content_type: crate::agent::AgentHistoryContentType::Text,
            text: Some("test-prompt".into()),
            document_reference: None,
            object: None,
        }]);
    }
    turn
}

fn agent_with_initial_history(history: Vec<crate::agent::AgentHistoryTurn>) -> (Engine, Key, Key) {
    let (mut engine, pi, _) = external_agent_instance();
    let aik =
        register_job_backed_agent_with_history(&mut engine, "agent", history).agent_instance_key;
    (engine, pi, aik)
}

/// A minimal AgentHistory turn carrying only the ordering-relevant fields.
fn history_turn(
    loop_iteration: i32,
    produced_at: u64,
    role: crate::agent::AgentHistoryRole,
) -> crate::agent::AgentHistoryTurn {
    crate::agent::AgentHistoryTurn {
        loop_iteration,
        produced_at,
        role,
        ..Default::default()
    }
}

/// The stored, ordered history log for `agent_instance_key`.
fn stored_history(
    engine: &Engine,
    instance_key: Key,
    agent_instance_key: Key,
) -> Vec<crate::agent::AgentHistoryRecord> {
    engine
        .state
        .instances
        .get(&instance_key)
        .and_then(|pi| pi.agent_history.get(&agent_instance_key))
        .cloned()
        .unwrap_or_default()
}

#[test]
fn agent_history_append_batch_materialises_one_pending_record_per_turn_in_order() {
    use crate::agent::{AgentHistoryCommitStatus, AgentHistoryRole};
    let (mut engine, instance_key, aik) = agent_instance_for_history();

    // A batch handed to the engine deliberately out of (loop_iteration,
    // produced_at) order — the store must re-order it deterministically.
    let turns = vec![
        history_turn(1, 200, AgentHistoryRole::Assistant),
        history_turn(0, 100, AgentHistoryRole::User),
        history_turn(0, 50, AgentHistoryRole::Configuration),
        history_turn(1, 150, AgentHistoryRole::ToolResult),
    ];
    let events = engine.append_agent_history(aik, turns);

    // One AGENT_HISTORY record per item, each PENDING and keyed by the agent
    // instance, all under the owning process instance.
    assert_eq!(events.len(), 4, "one record per appended turn");
    for event in &events {
        match event {
            Event::AgentHistoryCreated {
                instance_key: ik,
                record,
            } => {
                assert_eq!(*ik, instance_key);
                assert_eq!(record.agent_instance_key, aik);
                assert_eq!(record.process_instance_key, instance_key);
                assert_eq!(record.commit_status, AgentHistoryCommitStatus::Pending);
                assert_ne!(record.agent_history_key, 0);
            }
            other => panic!("expected AgentHistoryCreated, got {other:?}"),
        }
    }

    // Stored deterministically by (loop_iteration, produced_at).
    let stored = stored_history(&engine, instance_key, aik);
    let order: Vec<(i32, u64)> = stored
        .iter()
        .map(|r| (r.loop_iteration, r.produced_at))
        .collect();
    assert_eq!(order, vec![(0, 50), (0, 100), (1, 150), (1, 200)]);

    // Every turn got a distinct, monotonic history key.
    let mut keys: Vec<Key> = stored.iter().map(|r| r.agent_history_key).collect();
    keys.sort_unstable();
    keys.dedup();
    assert_eq!(keys.len(), 4, "history keys are unique");
}

#[test]
fn agent_history_append_ties_break_by_mint_order() {
    use crate::agent::AgentHistoryRole;
    let (mut engine, instance_key, aik) = agent_instance_for_history();

    // Two turns share an identical (loop_iteration, produced_at); the earlier
    // appended (smaller minted key) must keep the earlier slot.
    let events = engine.append_agent_history(
        aik,
        vec![
            history_turn(0, 100, AgentHistoryRole::User),
            history_turn(0, 100, AgentHistoryRole::Assistant),
        ],
    );
    let first_key = match &events[0] {
        Event::AgentHistoryCreated { record, .. } => record.agent_history_key,
        other => panic!("expected AgentHistoryCreated, got {other:?}"),
    };

    let stored = stored_history(&engine, instance_key, aik);
    assert_eq!(stored.len(), 2);
    assert_eq!(
        stored[0].agent_history_key, first_key,
        "the first-appended turn stays first on a (loop_iteration, produced_at) tie"
    );
    assert_eq!(stored[0].role, AgentHistoryRole::User);
    assert_eq!(stored[1].role, AgentHistoryRole::Assistant);
}

#[test]
fn agent_history_commit_moves_pending_turns_to_committed() {
    use crate::agent::{AgentHistoryCommitStatus, AgentHistoryRole};
    let (mut engine, instance_key, aik) = agent_instance_for_history();
    engine.append_agent_history(
        aik,
        vec![
            history_turn(0, 10, AgentHistoryRole::User),
            history_turn(0, 20, AgentHistoryRole::Assistant),
        ],
    );

    let events = engine.commit_agent_history(aik);
    assert_eq!(events.len(), 1, "a single commit event names the batch");
    match &events[0] {
        Event::AgentHistoryCommitted {
            instance_key: ik,
            agent_instance_key,
            agent_history_keys,
        } => {
            assert_eq!(*ik, instance_key);
            assert_eq!(*agent_instance_key, aik);
            assert_eq!(agent_history_keys.len(), 2);
        }
        other => panic!("expected AgentHistoryCommitted, got {other:?}"),
    }

    let stored = stored_history(&engine, instance_key, aik);
    assert!(
        stored
            .iter()
            .all(|r| r.commit_status == AgentHistoryCommitStatus::Committed),
        "every pending turn is now COMMITTED"
    );

    // Nothing is pending, so a second commit is a no-op (committed turns are
    // immutable — the log never re-touches them).
    assert!(
        engine.commit_agent_history(aik).is_empty(),
        "re-committing with no pending turns emits nothing"
    );
}

#[test]
fn agent_history_discard_moves_pending_turns_to_discarded() {
    use crate::agent::{AgentHistoryCommitStatus, AgentHistoryRole};
    let (mut engine, instance_key, aik) = agent_instance_for_history();
    engine.append_agent_history(
        aik,
        vec![
            history_turn(0, 10, AgentHistoryRole::User),
            history_turn(1, 20, AgentHistoryRole::Assistant),
        ],
    );

    let events = engine.discard_agent_history(aik);
    assert_eq!(events.len(), 1);
    match &events[0] {
        Event::AgentHistoryDiscarded {
            agent_instance_key,
            agent_history_keys,
            ..
        } => {
            assert_eq!(*agent_instance_key, aik);
            assert_eq!(agent_history_keys.len(), 2);
        }
        other => panic!("expected AgentHistoryDiscarded, got {other:?}"),
    }

    let stored = stored_history(&engine, instance_key, aik);
    assert!(
        stored
            .iter()
            .all(|r| r.commit_status == AgentHistoryCommitStatus::Discarded),
        "every pending turn is now DISCARDED"
    );
    assert!(
        engine.discard_agent_history(aik).is_empty(),
        "re-discarding with no pending turns emits nothing"
    );
}

#[test]
fn agent_history_committed_turns_are_immutable_across_later_batches() {
    use crate::agent::{AgentHistoryCommitStatus, AgentHistoryRole};
    let (mut engine, instance_key, aik) = agent_instance_for_history();

    // First batch committed.
    engine.append_agent_history(aik, vec![history_turn(0, 10, AgentHistoryRole::User)]);
    engine.commit_agent_history(aik);
    let committed_key = stored_history(&engine, instance_key, aik)[0].agent_history_key;

    // A second batch, then discard — must touch only the pending (second) turns.
    engine.append_agent_history(aik, vec![history_turn(1, 20, AgentHistoryRole::Assistant)]);
    let discard = engine.discard_agent_history(aik);
    match &discard[0] {
        Event::AgentHistoryDiscarded {
            agent_history_keys, ..
        } => assert!(
            !agent_history_keys.contains(&committed_key),
            "an already-committed turn is never named by a later discard"
        ),
        other => panic!("expected AgentHistoryDiscarded, got {other:?}"),
    }

    let stored = stored_history(&engine, instance_key, aik);
    assert_eq!(stored.len(), 2);
    // The first turn is still COMMITTED; only the second flipped to DISCARDED.
    let by_key: std::collections::HashMap<Key, AgentHistoryCommitStatus> = stored
        .iter()
        .map(|r| (r.agent_history_key, r.commit_status))
        .collect();
    assert_eq!(by_key[&committed_key], AgentHistoryCommitStatus::Committed);
    let discarded = stored
        .iter()
        .filter(|r| r.commit_status == AgentHistoryCommitStatus::Discarded)
        .count();
    assert_eq!(discarded, 1, "only the second batch was discarded");
}

#[test]
fn agent_history_append_to_unknown_instance_is_a_noop() {
    use crate::agent::AgentHistoryRole;
    let (mut engine, _instance_key, _aik) = agent_instance_for_history();
    let bogus = 999_999_999;
    assert!(
        engine
            .append_agent_history(bogus, vec![history_turn(0, 1, AgentHistoryRole::User)])
            .is_empty(),
        "appending to an unknown agent instance emits nothing"
    );
    assert!(engine.commit_agent_history(bogus).is_empty());
    assert!(engine.discard_agent_history(bogus).is_empty());
}

/// A history turn carrying a stable `historyItemId`, used to exercise the
/// idempotent-retry dedup path.
fn history_turn_with_id(
    loop_iteration: i32,
    produced_at: u64,
    role: crate::agent::AgentHistoryRole,
    history_item_id: &str,
) -> crate::agent::AgentHistoryTurn {
    crate::agent::AgentHistoryTurn {
        loop_iteration,
        produced_at,
        role,
        history_item_id: Some(history_item_id.to_string()),
        ..Default::default()
    }
}

#[test]
fn agent_history_dedups_repeated_history_item_id_within_a_batch() {
    use crate::agent::AgentHistoryRole;
    let (mut engine, instance_key, aik) = agent_instance_for_history();

    // Two turns share `historyItemId` "h1"; the second is an idempotent retry
    // of the first and must NOT materialise a second record.
    let events = engine.append_agent_history(
        aik,
        vec![
            history_turn_with_id(0, 10, AgentHistoryRole::User, "h1"),
            history_turn_with_id(0, 20, AgentHistoryRole::Assistant, "h1"),
        ],
    );

    let created_key = match &events[0] {
        Event::AgentHistoryCreated { record, .. } => {
            assert_eq!(record.history_item_id.as_deref(), Some("h1"));
            record.agent_history_key
        }
        other => panic!("expected AgentHistoryCreated first, got {other:?}"),
    };
    match &events[1] {
        Event::AgentHistoryDeduplicated {
            instance_key: ik,
            agent_instance_key,
            history_item_id,
            original_agent_history_key,
        } => {
            assert_eq!(*ik, instance_key);
            assert_eq!(*agent_instance_key, aik);
            assert_eq!(history_item_id, "h1");
            assert_eq!(
                *original_agent_history_key, created_key,
                "the duplicate resolves to the original turn's key"
            );
        }
        other => panic!("expected AgentHistoryDeduplicated second, got {other:?}"),
    }
    assert_eq!(events.len(), 2);

    // Only one record is materialised in the append-only log.
    let stored = stored_history(&engine, instance_key, aik);
    assert_eq!(stored.len(), 1, "the duplicate created no new record");
    assert_eq!(stored[0].agent_history_key, created_key);
}

#[test]
fn agent_history_dedups_history_item_id_against_a_prior_committed_batch() {
    use crate::agent::AgentHistoryRole;
    let (mut engine, instance_key, aik) = agent_instance_for_history();

    // First batch records "h1" and commits it.
    engine.append_agent_history(
        aik,
        vec![history_turn_with_id(0, 10, AgentHistoryRole::User, "h1")],
    );
    engine.commit_agent_history(aik);
    let original_key = stored_history(&engine, instance_key, aik)[0].agent_history_key;

    // A retry re-submits "h1": no new record, dedup resolves to the original.
    let events = engine.append_agent_history(
        aik,
        vec![history_turn_with_id(1, 20, AgentHistoryRole::User, "h1")],
    );
    assert_eq!(events.len(), 1);
    match &events[0] {
        Event::AgentHistoryDeduplicated {
            history_item_id,
            original_agent_history_key,
            ..
        } => {
            assert_eq!(history_item_id, "h1");
            assert_eq!(*original_agent_history_key, original_key);
        }
        other => panic!("expected AgentHistoryDeduplicated, got {other:?}"),
    }
    let stored = stored_history(&engine, instance_key, aik);
    assert_eq!(stored.len(), 1, "the retry created no new record");
}

#[test]
fn agent_history_absent_history_item_id_never_dedups() {
    use crate::agent::AgentHistoryRole;
    let (mut engine, instance_key, aik) = agent_instance_for_history();

    // Two id-less turns are indistinguishable for correlation, so both must
    // materialise fresh records — dedup applies only to carried ids.
    let events = engine.append_agent_history(
        aik,
        vec![
            history_turn(0, 10, AgentHistoryRole::User),
            history_turn(0, 20, AgentHistoryRole::Assistant),
        ],
    );
    assert!(
        events
            .iter()
            .all(|e| matches!(e, Event::AgentHistoryCreated { .. })),
        "id-less turns never dedup"
    );
    assert_eq!(stored_history(&engine, instance_key, aik).len(), 2);
}

#[test]
fn agent_history_discarded_history_item_id_is_re_recordable() {
    use crate::agent::AgentHistoryRole;
    let (mut engine, instance_key, aik) = agent_instance_for_history();

    // "h1" is appended then discarded (rejected) — it is not "already
    // recorded", so re-submitting it must create a fresh record, not dedup.
    engine.append_agent_history(
        aik,
        vec![history_turn_with_id(0, 10, AgentHistoryRole::User, "h1")],
    );
    engine.discard_agent_history(aik);

    let events = engine.append_agent_history(
        aik,
        vec![history_turn_with_id(1, 20, AgentHistoryRole::User, "h1")],
    );
    assert_eq!(events.len(), 1);
    assert!(
        matches!(&events[0], Event::AgentHistoryCreated { .. }),
        "a discarded id is re-recordable, not deduped"
    );
    // One discarded + one fresh pending record.
    assert_eq!(stored_history(&engine, instance_key, aik).len(), 2);
}

// --- AgentInstance lifecycle processors (Camunda 8.10 parity, slice S3) ------
//
// CREATE/UPDATE/COMPLETE processors with the stable/8.10 validation rules and
// AgentInstanceLimits enforcement. History application reuses the S2
// batch-append behavior. Parity reference: camunda/camunda stable/8.10
// (8.10.0-SNAPSHOT).

/// The owning `element_instance_key` of the worker-registered AgentInstance `aik`.
fn agent_element_instance_key(engine: &Engine, instance_key: Key, aik: Key) -> Key {
    engine
        .state
        .instances
        .get(&instance_key)
        .and_then(|pi| pi.agent_instances.get(&aik))
        .map(|ai| ai.element_instance_key)
        .expect("the agent instance should be stored")
}

/// The stored AgentInstance record for `aik`.
fn stored_agent_instance(
    engine: &Engine,
    instance_key: Key,
    aik: Key,
) -> crate::agent::AgentInstance {
    engine
        .state
        .instances
        .get(&instance_key)
        .and_then(|pi| pi.agent_instances.get(&aik))
        .cloned()
        .expect("the agent instance should be stored")
}

#[test]
fn agent_instance_create_from_active_agent_element_rejects_re_registration() {
    use crate::agent::{
        AgentDefinition, AgentHistoryRole, AgentInstanceLimits, AgentInstanceStatus,
    };
    let (mut engine, pi, aik) = agent_instance_for_history();
    let eik = agent_element_instance_key(&engine, pi, aik);
    let registered = stored_agent_instance(&engine, pi, aik);

    let limits = AgentInstanceLimits {
        max_tokens: 1_000,
        max_model_calls: 10,
        max_tool_calls: 5,
    };
    let before = stored_agent_instance(&engine, pi, aik);
    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: registered.job_key,
            job_lease: registered.job_lease,
            definition: AgentDefinition {
                model: Some("gpt-4o".to_string()),
                ..Default::default()
            },
            limits: Some(limits),
            history: vec![history_turn(0, 10, AgentHistoryRole::Configuration)],
        })
        .unwrap_err();
    assert!(
        matches!(err, EngineError::AgentInstanceAlreadyExists { agent_instance_key, .. } if agent_instance_key == aik)
    );
    assert_eq!(stored_agent_instance(&engine, pi, aik), before);
    assert_eq!(before.status, AgentInstanceStatus::Initializing);
    assert!(stored_history(&engine, pi, aik).is_empty());
}

#[test]
fn agent_instance_create_rejects_invalid_caller_supplied_job_attribution() {
    use crate::agent::{AgentDefinition, AgentHistoryRole, AgentInstanceLimits};
    let (mut engine, pi, eik) = external_agent_instance();
    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: 999_999,
            job_lease: "invalid-lease".to_string(),
            definition: AgentDefinition::default(),
            limits: Some(AgentInstanceLimits::default()),
            history: vec![history_turn(0, 10, AgentHistoryRole::Configuration)],
        })
        .unwrap_err();
    assert!(matches!(err, EngineError::AgentInstanceJobNotActive { .. }));
    assert!(engine.state.instances[&pi].agent_instances.is_empty());
    assert!(engine.state.instances[&pi].agent_history.is_empty());
}

#[test]
fn agent_instance_create_without_explicit_limits_defaults_to_unlimited() {
    use crate::agent::{AgentHistoryRole, AgentInstanceLimits};
    let (engine, pi, aik) = agent_with_initial_history(vec![worker_history_turn(
        1,
        1,
        AgentHistoryRole::Configuration,
    )]);
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).limits,
        AgentInstanceLimits::default(),
        "absent limits default to -1/-1/-1"
    );
}

#[test]
fn agent_instance_create_takes_limits_from_configuration_history_item() {
    use crate::agent::{AgentHistoryRole, AgentInstanceLimits};

    let cfg_limits = AgentInstanceLimits {
        max_tokens: 42,
        max_model_calls: -1,
        max_tool_calls: 7,
    };
    let mut cfg_turn = worker_history_turn(1, 5, AgentHistoryRole::Configuration);
    cfg_turn.limits = Some(cfg_limits);
    let (engine, pi, aik) = agent_with_initial_history(vec![cfg_turn]);
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).limits,
        cfg_limits,
        "limits fall back to the CONFIGURATION history item"
    );
}

#[test]
fn agent_instance_create_ignores_limits_from_non_configuration_history_item() {
    use crate::agent::{AgentHistoryRole, AgentInstanceLimits};

    // An ASSISTANT turn carrying `limits` must NOT seed the record's limits:
    // only a CONFIGURATION turn may. With no CONFIGURATION turn and no explicit
    // limits, the record must fall back to the unlimited default.
    let mut assistant_turn = worker_history_turn(2, 5, AgentHistoryRole::Assistant);
    assistant_turn.limits = Some(AgentInstanceLimits {
        max_tokens: 42,
        max_model_calls: 3,
        max_tool_calls: 7,
    });
    let (engine, pi, aik) = agent_with_initial_history(vec![
        worker_history_turn(1, 1, AgentHistoryRole::Configuration),
        assistant_turn,
    ]);
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).limits,
        AgentInstanceLimits::default(),
        "a non-CONFIGURATION turn must not seed limits; default to unlimited"
    );
}

#[test]
fn agent_instance_create_takes_limits_from_last_configuration_not_later_turn() {
    use crate::agent::{AgentHistoryRole, AgentInstanceLimits};

    // CONFIGURATION seeds limits; a LATER ASSISTANT turn carrying different
    // limits must not override the CONFIGURATION-supplied value.
    let cfg_limits = AgentInstanceLimits {
        max_tokens: 100,
        max_model_calls: 5,
        max_tool_calls: 9,
    };
    let mut cfg_turn = worker_history_turn(1, 5, AgentHistoryRole::Configuration);
    cfg_turn.limits = Some(cfg_limits);
    let mut later_assistant = worker_history_turn(2, 10, AgentHistoryRole::Assistant);
    later_assistant.limits = Some(AgentInstanceLimits {
        max_tokens: 1,
        max_model_calls: 1,
        max_tool_calls: 1,
    });
    let (engine, pi, aik) = agent_with_initial_history(vec![cfg_turn, later_assistant]);
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).limits,
        cfg_limits,
        "the CONFIGURATION turn wins; a later non-CONFIGURATION turn cannot override it"
    );
}

#[test]
fn agent_instance_create_on_inactive_element_instance_is_rejected() {
    use crate::agent::AgentDefinition;
    let (mut engine, _pi, _aik) = agent_instance_for_history();
    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: 999_999_999,
            job_key: 0,
            job_lease: String::new(),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::AgentInstanceElementInstanceInactive { .. }
    ));
}

#[test]
fn agent_instance_create_on_plain_service_task_missing_agent_definition_is_rejected() {
    use crate::agent::AgentDefinition;
    // A job-worker service task is an eligible TYPE (SERVICE_TASK) but carries no
    // agentDefinition, so CREATE rejects it as missing the agentDefinitionKey.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="plain-proc" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:serviceTask id="job">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="worker" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="job" />
          <bpmn:sequenceFlow id="f2" sourceRef="job" targetRef="end" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("plain-proc"))
        .unwrap();
    let eik = events
        .iter()
        .find_map(|e| match e {
            Event::ElementActivated {
                element_instance_key,
                element_id,
                ..
            } if element_id == "job" => Some(*element_instance_key),
            _ => None,
        })
        .expect("the service task should activate and park");

    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: 0,
            job_lease: String::new(),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::AgentInstanceMissingAgentDefinition { .. }
    ));
}

#[test]
fn agent_instance_create_on_non_eligible_element_is_rejected() {
    use crate::agent::AgentDefinition;
    // A timer intermediate catch event is active-but-not-eligible.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="catch-proc" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:intermediateCatchEvent id="await">
            <bpmn:timerEventDefinition>
              <bpmn:timeDuration>PT1H</bpmn:timeDuration>
            </bpmn:timerEventDefinition>
          </bpmn:intermediateCatchEvent>
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="await" />
          <bpmn:sequenceFlow id="f2" sourceRef="await" targetRef="end" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("catch-proc"))
        .unwrap();
    let eik = events
        .iter()
        .find_map(|e| match e {
            Event::ElementActivated {
                element_instance_key,
                element_id,
                ..
            } if element_id == "await" => Some(*element_instance_key),
            _ => None,
        })
        .expect("the catch event should activate and wait");

    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: 0,
            job_lease: String::new(),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::AgentInstanceElementNotEligible { .. }
    ));
}

/// Build an UPDATE command with the given status/metrics/history, asserting the
/// stored ownership fields (`element_id` "agent").
fn update_agent(
    engine: &Engine,
    aik: Key,
    eik: Key,
    pi: Key,
    status: crate::agent::AgentInstanceStatus,
    metrics: crate::agent::AgentInstanceMetricsDelta,
    history: Vec<crate::agent::AgentHistoryTurn>,
) -> Command {
    let (job_key, job_lease) = if history.is_empty() {
        (0, String::new())
    } else {
        let job = engine
            .state
            .jobs
            .values()
            .find(|job| job.element_instance_key == eik)
            .expect("history-bearing UPDATE requires the agent's job");
        (
            job.key,
            job.lease_token
                .clone()
                .expect("the agent job must be activated"),
        )
    };
    Command::UpdateAgentInstance {
        agent_instance_key: aik,
        element_instance_key: eik,
        element_id: "agent".to_string(),
        process_instance_key: pi,
        job_key,
        job_lease,
        status: Some(status),
        metrics,
        tools: None,
        history,
    }
}

#[test]
fn agent_instance_update_advances_status_appends_history_and_accumulates_metrics() {
    use crate::agent::{AgentHistoryRole, AgentInstanceMetricsDelta, AgentInstanceStatus};
    let (mut engine, pi, aik) = agent_instance_for_history();
    let eik = agent_element_instance_key(&engine, pi, aik);

    // Advance through each active state, appending one turn per update.
    let steps = [
        AgentInstanceStatus::ToolDiscovery,
        AgentInstanceStatus::Thinking,
        AgentInstanceStatus::ToolCalling,
        AgentInstanceStatus::Idle,
    ];
    for (i, status) in steps.iter().enumerate() {
        let mut turn =
            worker_history_turn(i as i32 + 1, (i as u64) * 10, AgentHistoryRole::Assistant);
        turn.metrics = Some(crate::agent::AgentHistoryMetrics {
            input_tokens: Some(100),
            output_tokens: Some(20),
            ..Default::default()
        });
        turn.tool_calls.push(crate::agent::AgentHistoryToolCall {
            tool_call_id: format!("call-{i}"),
            tool_name: "tool".into(),
            element_id: None,
            arguments: None,
        });
        let events = engine
            .apply_command(update_agent(
                &engine,
                aik,
                eik,
                pi,
                *status,
                AgentInstanceMetricsDelta::default(),
                vec![turn],
            ))
            .unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::AgentInstanceUpdated { .. })),
            "each UPDATE emits AgentInstanceUpdated"
        );
        assert_eq!(stored_agent_instance(&engine, pi, aik).status, *status);
    }

    // Metrics accumulate immediately; history remains pending until job completion.
    let stored = stored_agent_instance(&engine, pi, aik);
    assert_eq!(stored.metrics.input_tokens, 400);
    assert_eq!(stored.metrics.output_tokens, 80);
    assert_eq!(stored.metrics.model_calls, 4);
    assert_eq!(stored.metrics.tool_calls, 4);
    let hist = stored_history(&engine, pi, aik);
    assert_eq!(hist.len(), 4);
    assert!(hist
        .iter()
        .all(|r| r.commit_status == crate::agent::AgentHistoryCommitStatus::Pending));
    engine
        .apply_command(Command::complete_job(stored.job_key).with_job_lease(stored.job_lease))
        .unwrap();
    assert!(stored_history(&engine, pi, aik)
        .iter()
        .all(|r| r.commit_status == crate::agent::AgentHistoryCommitStatus::Committed));
}

#[test]
fn agent_instance_update_replaces_tools() {
    use crate::agent::{AgentInstanceMetricsDelta, AgentInstanceStatus, AgentTool};
    let (mut engine, pi, aik) = agent_instance_for_history();
    let eik = agent_element_instance_key(&engine, pi, aik);

    let tool = AgentTool {
        name: "search".to_string(),
        description: None,
        element_id: None,
    };
    engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "agent".to_string(),
            process_instance_key: pi,
            job_key: 0,
            job_lease: String::new(),
            status: Some(AgentInstanceStatus::ToolDiscovery),
            metrics: AgentInstanceMetricsDelta::default(),
            tools: Some(vec![tool.clone()]),
            history: vec![],
        })
        .unwrap();
    assert_eq!(stored_agent_instance(&engine, pi, aik).tools, vec![tool]);
}

#[test]
fn agent_instance_update_to_completed_is_rejected() {
    use crate::agent::{AgentInstanceMetricsDelta, AgentInstanceStatus};
    let (mut engine, pi, aik) = agent_instance_for_history();
    let eik = agent_element_instance_key(&engine, pi, aik);
    let err = engine
        .apply_command(update_agent(
            &engine,
            aik,
            eik,
            pi,
            AgentInstanceStatus::Completed,
            AgentInstanceMetricsDelta::default(),
            vec![],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::AgentInstanceStatusNotSettable { .. }
    ));
}

#[test]
fn agent_instance_update_with_wrong_element_id_or_process_instance_is_rejected() {
    use crate::agent::{AgentInstanceMetricsDelta, AgentInstanceStatus};
    let (mut engine, pi, aik) = agent_instance_for_history();
    let eik = agent_element_instance_key(&engine, pi, aik);

    let wrong_element = engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "not-agent".to_string(),
            process_instance_key: pi,
            job_key: 0,
            job_lease: String::new(),
            status: Some(AgentInstanceStatus::Thinking),
            metrics: AgentInstanceMetricsDelta::default(),
            tools: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(matches!(
        wrong_element,
        EngineError::AgentInstanceOwnershipMismatch { .. }
    ));

    let wrong_pi = engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "agent".to_string(),
            process_instance_key: 424_242,
            job_key: 0,
            job_lease: String::new(),
            status: Some(AgentInstanceStatus::Thinking),
            metrics: AgentInstanceMetricsDelta::default(),
            tools: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(matches!(
        wrong_pi,
        EngineError::AgentInstanceOwnershipMismatch { .. }
    ));
}

#[test]
fn agent_instance_update_on_inactive_element_instance_is_rejected() {
    use crate::agent::{AgentInstanceMetricsDelta, AgentInstanceStatus};
    let (mut engine, pi, aik) = agent_instance_for_history();
    let err = engine
        .apply_command(update_agent(
            &engine,
            aik,
            888_888_888,
            pi,
            AgentInstanceStatus::Thinking,
            AgentInstanceMetricsDelta::default(),
            vec![],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::AgentInstanceElementInstanceInactive { .. }
    ));
}

#[test]
fn agent_instance_update_with_conflicting_instance_is_rejected() {
    use crate::agent::{AgentInstanceMetricsDelta, AgentInstanceStatus};
    // Two activations of the same element in one process each own an agent.
    // Referencing the second activation from the first agent conflicts.
    let (mut engine, pi_a, aik_a) = agent_instance_for_history();
    let events_b = engine
        .apply_command(Command::ModifyInstance {
            instance_key: pi_a,
            activate_instructions: vec![crate::command::ActivateElementInstruction {
                element_id: "agent".into(),
                variables: HashMap::new(),
            }],
            terminate_instructions: vec![],
        })
        .unwrap();
    let pi_b = events_b.iter().find_map(|e| e.instance_key()).unwrap();
    let aik_b = register_job_backed_agent(&mut engine, "agent").agent_instance_key;
    let eik_b = agent_element_instance_key(&engine, pi_b, aik_b);

    let err = engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik_a,
            element_instance_key: eik_b,
            element_id: "agent".to_string(),
            process_instance_key: pi_a,
            job_key: 0,
            job_lease: String::new(),
            status: Some(AgentInstanceStatus::Thinking),
            metrics: AgentInstanceMetricsDelta::default(),
            tools: None,
            history: vec![],
        })
        .unwrap_err();
    match err {
        EngineError::AgentInstanceConflict {
            conflicting_agent_instance_key,
            ..
        } => assert_eq!(conflicting_agent_instance_key, aik_b),
        other => panic!("expected AgentInstanceConflict, got {other:?}"),
    }
}

#[test]
fn agent_instance_update_with_foreign_process_element_instance_is_rejected() {
    use crate::agent::{AgentInstanceMetricsDelta, AgentInstanceStatus};
    // A *different* process whose active element merely shares the id "agent"
    // but is not agent-eligible (a userTask, so no owning AgentInstance is
    // minted). Referencing that foreign, unowned element instance from an UPDATE
    // must be rejected as an ownership mismatch — it would otherwise be linked as
    // a re-entry key and corrupt ownership/re-entry tracking across process
    // instances.
    let (mut engine, pi, aik) = agent_instance_for_history();
    let foreign_xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="foreign-proc" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:userTask id="agent" />
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="end" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(foreign_xml).unwrap().remove(0);
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("foreign-proc"))
        .unwrap();
    let foreign_eik = events
        .iter()
        .find_map(|e| match e {
            Event::ElementActivated {
                element_instance_key,
                element_id,
                ..
            } if element_id == "agent" => Some(*element_instance_key),
            _ => None,
        })
        .expect("the foreign userTask should activate and wait");

    let err = engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: foreign_eik,
            element_id: "agent".to_string(),
            process_instance_key: pi,
            job_key: 0,
            job_lease: String::new(),
            status: Some(AgentInstanceStatus::Thinking),
            metrics: AgentInstanceMetricsDelta::default(),
            tools: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::AgentInstanceOwnershipMismatch { .. }
    ));
}

#[test]
fn agent_instance_update_rejects_negative_metric_deltas() {
    use crate::agent::{AgentInstanceMetricsDelta, AgentInstanceStatus};
    // First accumulate some real usage.
    let (mut engine, pi, aik) = agent_instance_for_history();
    let eik = agent_element_instance_key(&engine, pi, aik);
    engine
        .apply_command(update_agent(
            &engine,
            aik,
            eik,
            pi,
            AgentInstanceStatus::Thinking,
            AgentInstanceMetricsDelta {
                input_tokens: 100,
                output_tokens: 40,
                model_calls: 3,
                tool_calls: 2,
                ..Default::default()
            },
            vec![],
        ))
        .unwrap();

    // Invalid negative deltas reject atomically rather than refund prior usage.
    let err = engine
        .apply_command(update_agent(
            &engine,
            aik,
            eik,
            pi,
            AgentInstanceStatus::Thinking,
            AgentInstanceMetricsDelta {
                input_tokens: -1_000,
                output_tokens: -1_000,
                model_calls: -10,
                tool_calls: -10,
                ..Default::default()
            },
            vec![],
        ))
        .unwrap_err();
    assert!(matches!(err, EngineError::AgentHistoryInvalid { .. }));
    let stored = stored_agent_instance(&engine, pi, aik);
    assert_eq!(stored.metrics.input_tokens, 100);
    assert_eq!(stored.metrics.output_tokens, 40);
    assert_eq!(stored.metrics.model_calls, 3);
    assert_eq!(stored.metrics.tool_calls, 2);
}

#[test]
fn agent_instance_update_on_unknown_instance_is_rejected() {
    use crate::agent::{AgentInstanceMetricsDelta, AgentInstanceStatus};
    let (mut engine, pi, aik) = agent_instance_for_history();
    let eik = agent_element_instance_key(&engine, pi, aik);
    let err = engine
        .apply_command(update_agent(
            &engine,
            123_456_789,
            eik,
            pi,
            AgentInstanceStatus::Thinking,
            AgentInstanceMetricsDelta::default(),
            vec![],
        ))
        .unwrap_err();
    assert!(matches!(err, EngineError::AgentInstanceNotFound { .. }));
}

#[test]
fn agent_instance_update_records_usage_even_when_it_exceeds_configured_limits() {
    use crate::agent::{
        AgentHistoryRole, AgentInstanceLimits, AgentInstanceMetricsDelta, AgentInstanceStatus,
    };
    // Unlimited (-1) instance: a large batch is accepted.
    let (mut engine, pi, aik) = agent_instance_for_history();
    let eik = agent_element_instance_key(&engine, pi, aik);
    engine
        .apply_command(update_agent(
            &engine,
            aik,
            eik,
            pi,
            AgentInstanceStatus::Thinking,
            AgentInstanceMetricsDelta {
                input_tokens: 10_000,
                model_calls: 999,
                tool_calls: 999,
                ..Default::default()
            },
            vec![],
        ))
        .unwrap();
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).metrics.input_tokens,
        10_000
    );

    // The worker enforces execution limits; recording observed usage must remain possible.
    let mut configuration = worker_history_turn(1, 1, AgentHistoryRole::Configuration);
    configuration.limits = Some(AgentInstanceLimits {
        max_tokens: 50,
        max_model_calls: -1,
        max_tool_calls: -1,
    });
    let (mut engine, pi, aik) = agent_with_initial_history(vec![configuration]);
    let eik = agent_element_instance_key(&engine, pi, aik);
    let mut turn = worker_history_turn(2, 2, AgentHistoryRole::Assistant);
    turn.metrics = Some(crate::agent::AgentHistoryMetrics {
        input_tokens: Some(100),
        ..Default::default()
    });
    engine
        .apply_command(update_agent(
            &engine,
            aik,
            eik,
            pi,
            AgentInstanceStatus::Thinking,
            AgentInstanceMetricsDelta::default(),
            vec![turn],
        ))
        .unwrap();
    let after = stored_agent_instance(&engine, pi, aik);
    assert_eq!(after.metrics.input_tokens, 100);
    assert_eq!(after.limits.max_tokens, 50);
    assert_eq!(after.status, AgentInstanceStatus::Thinking);
    assert_eq!(stored_history(&engine, pi, aik).len(), 2);
}

/// Deploy a fork/join process with two parallel agent tasks and start it,
/// returning the engine, process-instance key, and the two agent-instance keys.
fn two_agent_instances() -> (Engine, Key, Vec<Key>) {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="two-agents" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:parallelGateway id="fork" />
          <bpmn:serviceTask id="agentA">
            <bpmn:extensionElements>
              <zeebe:agentDefinition agentType="aiAgentTask" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:serviceTask id="agentB">
            <bpmn:extensionElements>
              <zeebe:agentDefinition agentType="aiAgentTask" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:sequenceFlow id="f0" sourceRef="start" targetRef="fork" />
          <bpmn:sequenceFlow id="fa" sourceRef="fork" targetRef="agentA" />
          <bpmn:sequenceFlow id="fb" sourceRef="fork" targetRef="agentB" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("two-agents"))
        .unwrap();
    let pi = events.iter().find_map(|e| e.instance_key()).unwrap();
    let aiks: Vec<Key> = ["agentA", "agentB"]
        .into_iter()
        .map(|job_type| register_job_backed_agent(&mut engine, job_type).agent_instance_key)
        .collect();
    (engine, pi, aiks)
}

#[test]
fn agent_instance_complete_drives_to_completed_and_drains_remaining() {
    use crate::agent::AgentInstanceStatus;
    let (mut engine, pi, aiks) = two_agent_instances();
    assert_eq!(
        aiks.len(),
        2,
        "workers register two parallel agent instances"
    );

    let active_count = |engine: &Engine| -> usize {
        engine
            .state
            .instances
            .get(&pi)
            .map(|p| {
                p.agent_instances
                    .values()
                    .filter(|ai| ai.status.is_active())
                    .count()
            })
            .unwrap_or(0)
    };
    assert_eq!(active_count(&engine), 2);

    // Complete each by key; the active set drains one at a time.
    let events = engine
        .apply_command(Command::CompleteAgentInstance {
            agent_instance_key: aiks[0],
        })
        .unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::AgentInstanceCompleted { .. })));
    assert_eq!(
        stored_agent_instance(&engine, pi, aiks[0]).status,
        AgentInstanceStatus::Completed
    );
    assert_eq!(active_count(&engine), 1);

    engine
        .apply_command(Command::CompleteAgentInstance {
            agent_instance_key: aiks[1],
        })
        .unwrap();
    assert_eq!(
        stored_agent_instance(&engine, pi, aiks[1]).status,
        AgentInstanceStatus::Completed
    );
    assert_eq!(active_count(&engine), 0, "no active agent instances remain");
}

#[test]
fn agent_instance_complete_is_the_only_path_to_completed_and_rejects_re_completion() {
    let (mut engine, _pi, aiks) = two_agent_instances();
    engine
        .apply_command(Command::CompleteAgentInstance {
            agent_instance_key: aiks[0],
        })
        .unwrap();
    // Re-completing a terminal instance is rejected.
    let err = engine
        .apply_command(Command::CompleteAgentInstance {
            agent_instance_key: aiks[0],
        })
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::AgentInstanceAlreadyCompleted { .. }
    ));
}

#[test]
fn agent_instance_complete_on_unknown_instance_is_rejected() {
    let (mut engine, _pi, _aiks) = agent_instance_for_history();
    let err = engine
        .apply_command(Command::CompleteAgentInstance {
            agent_instance_key: 777_777_777,
        })
        .unwrap_err();
    assert!(matches!(err, EngineError::AgentInstanceNotFound { .. }));
}
/// #986 — a declared `fetchVariables` read-set supplied to `activate_jobs_with_fetch`
/// is stamped onto the durable `JobActivated` event (engine-native read
/// provenance for reification), while a fetch-all activation records none.
#[test]
fn activation_stamps_the_declared_read_set_on_job_activated() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // Declared activation: worker asks for [amount, currency].
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let events = engine
        .apply_command_at(
            Command::activate_jobs_with_fetch(
                "payment",
                "w",
                1,
                60_000,
                0,
                vec!["amount".to_string(), "currency".to_string()],
            ),
            0,
        )
        .unwrap();
    let declared = events
        .iter()
        .find_map(|e| match e {
            Event::JobActivated {
                fetch_variables, ..
            } => Some(fetch_variables.clone()),
            _ => None,
        })
        .expect("a JobActivated was emitted");
    assert_eq!(
        declared,
        vec!["amount".to_string(), "currency".to_string()],
        "the declared read-set is stamped onto the activation event"
    );

    // Fetch-all activation: a second instance, activated without a declared set,
    // records an empty (undeclared) read-set.
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let events = engine
        .apply_command_at(Command::activate_jobs("payment", "w", 1, 60_000, 0), 0)
        .unwrap();
    let undeclared = events
        .iter()
        .find_map(|e| match e {
            Event::JobActivated {
                fetch_variables, ..
            } => Some(fetch_variables.clone()),
            _ => None,
        })
        .expect("a JobActivated was emitted");
    assert!(
        undeclared.is_empty(),
        "a fetch-all activation records no declared read-set"
    );
}

/// #986 — the new `fetch_variables` field must NOT change the journal byte shape
/// of a declaration-free activation: `skip_serializing_if` omits it entirely when
/// empty, so historical/undeclared `JobActivated` records serialize identically.
#[cfg(feature = "serde")]
#[test]
fn declaration_free_job_activated_is_byte_identical() {
    let undeclared = Event::JobActivated {
        job_key: 7,
        instance_key: 3,
        durable: false,
        worker: "w".to_string(),
        deadline: 60_000,
        activated_at: Some(1),
        fetch_variables: Vec::new(),
        lease_token: None,
    };
    let json = serde_json::to_string(&undeclared).unwrap();
    assert!(
        !json.contains("fetch_variables"),
        "an empty read-set must be omitted from the serialized event: {json}"
    );
    assert!(
        !json.contains("lease_token"),
        "a lease-less activation must omit the lease token from the serialized event: {json}"
    );

    // A declared read-set IS serialized, and round-trips.
    let declared = Event::JobActivated {
        job_key: 7,
        instance_key: 3,
        durable: false,
        worker: "w".to_string(),
        deadline: 60_000,
        activated_at: Some(1),
        fetch_variables: vec!["a".to_string(), "c".to_string()],
        lease_token: None,
    };
    let json = serde_json::to_string(&declared).unwrap();
    assert!(json.contains("fetch_variables"));
    let back: Event = serde_json::from_str(&json).unwrap();
    assert_eq!(back, declared);
}

/// Regression (#1157): a link throw hands its token to the matching link catch;
/// the second half of the model (everything downstream of the catch) must
/// actually run, not silently vanish while the instance reports success.
#[test]
fn link_events_hand_the_token_from_throw_to_catch() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="links" isExecutable="true">
          <bpmn:startEvent id="LStart"><bpmn:outgoing>LF1</bpmn:outgoing></bpmn:startEvent>
          <bpmn:sequenceFlow id="LF1" sourceRef="LStart" targetRef="Throw" />
          <bpmn:intermediateThrowEvent id="Throw">
            <bpmn:incoming>LF1</bpmn:incoming>
            <bpmn:linkEventDefinition id="LD1" name="hop" />
          </bpmn:intermediateThrowEvent>
          <bpmn:intermediateCatchEvent id="Catch">
            <bpmn:outgoing>LF2</bpmn:outgoing>
            <bpmn:linkEventDefinition id="LD2" name="hop" />
          </bpmn:intermediateCatchEvent>
          <bpmn:sequenceFlow id="LF2" sourceRef="Catch" targetRef="AfterLink" />
          <bpmn:serviceTask id="AfterLink">
            <bpmn:incoming>LF2</bpmn:incoming>
            <bpmn:outgoing>LF3</bpmn:outgoing>
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="probe-after-link" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:sequenceFlow id="LF3" sourceRef="AfterLink" targetRef="LEnd" />
          <bpmn:endEvent id="LEnd"><bpmn:incoming>LF3</bpmn:incoming></bpmn:endEvent>
        </bpmn:process>
      </bpmn:definitions>"#;

    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    assert_eq!(
        def.element("Throw").unwrap().kind,
        ElementKind::LinkIntermediateThrowEvent {
            link_name: "hop".to_string()
        }
    );
    assert_eq!(
        def.element("Catch").unwrap().kind,
        ElementKind::LinkIntermediateCatchEvent {
            link_name: "hop".to_string()
        }
    );

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance("links"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // The token must reach `AfterLink` (a job appears) rather than vanishing at
    // the throw — and the instance must NOT be reported completed while that
    // second-half work is still pending (the #1157 false success).
    assert!(
        !engine.is_completed(inst),
        "instance must not complete while AfterLink is still pending"
    );
    let jobs = engine.activate_jobs("probe-after-link", "w", 5, 1_000, 0);
    assert_eq!(
        jobs.len(),
        1,
        "the link catch's downstream service task must run (got {} jobs)",
        jobs.len()
    );

    // Completing the second half drives the instance to a genuine completion.
    engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();
    assert!(engine.is_completed(inst));
}

/// A link throw hands its token to the matching catch *by name*, so multiple
/// distinct link pairs in one model each route to their own catch.
#[test]
fn link_events_route_by_matching_name() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="two-links" isExecutable="true">
          <bpmn:startEvent id="s"><bpmn:outgoing>f0</bpmn:outgoing></bpmn:startEvent>
          <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="ThrowB" />
          <bpmn:intermediateThrowEvent id="ThrowB">
            <bpmn:incoming>f0</bpmn:incoming>
            <bpmn:linkEventDefinition name="B" />
          </bpmn:intermediateThrowEvent>
          <bpmn:intermediateCatchEvent id="CatchA">
            <bpmn:outgoing>fa</bpmn:outgoing>
            <bpmn:linkEventDefinition name="A" />
          </bpmn:intermediateCatchEvent>
          <bpmn:sequenceFlow id="fa" sourceRef="CatchA" targetRef="TaskA" />
          <bpmn:serviceTask id="TaskA">
            <bpmn:incoming>fa</bpmn:incoming>
            <bpmn:extensionElements><zeebe:taskDefinition type="job-a" /></bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:intermediateCatchEvent id="CatchB">
            <bpmn:outgoing>fb</bpmn:outgoing>
            <bpmn:linkEventDefinition name="B" />
          </bpmn:intermediateCatchEvent>
          <bpmn:sequenceFlow id="fb" sourceRef="CatchB" targetRef="TaskB" />
          <bpmn:serviceTask id="TaskB">
            <bpmn:incoming>fb</bpmn:incoming>
            <bpmn:extensionElements><zeebe:taskDefinition type="job-b" /></bpmn:extensionElements>
          </bpmn:serviceTask>
        </bpmn:process>
      </bpmn:definitions>"#;

    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance("two-links"))
        .unwrap();

    // The throw named "B" must reach CatchB's task, not CatchA's.
    assert_eq!(
        engine.activate_jobs("job-a", "w", 5, 1_000, 0).len(),
        0,
        "the unrelated link 'A' catch must not fire"
    );
    assert_eq!(
        engine.activate_jobs("job-b", "w", 5, 1_000, 0).len(),
        1,
        "the throw hands off to the same-named catch 'B'"
    );
}
