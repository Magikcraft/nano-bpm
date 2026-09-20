//! The applier: the sole mutator of [`State`].
//!
//! [`apply`] mutates [`State`] purely as a function of an [`Event`]. Keeping
//! every mutation here is what makes the engine deterministic and replayable:
//! replaying the same events over a fresh [`State`] reconstructs it exactly.
//! This half of the `state` module may reference [`crate::event`]; the
//! data-model half ([`super::types`]) must not.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::event::Event;
use crate::model::{ElementId, Value};

use super::types::*;

/// Reads the process id of `instance_key` **iff** it is currently non-terminal,
/// so the caller can decrement the per-definition in-flight counter exactly once
/// (idempotent-safe against a re-delivered terminal event that finds the instance
/// already Completed/Terminated).
fn non_terminal_process_id(state: &State, instance_key: &Key) -> Option<String> {
    match state.instances.get(instance_key) {
        Some(i)
            if !matches!(
                i.state,
                ProcessInstanceState::Completed | ProcessInstanceState::Terminated
            ) =>
        {
            Some(i.process_id.clone())
        }
        _ => None,
    }
}

/// Decrements a definition's in-flight instance count on a terminal transition,
/// dropping the entry when it reaches zero (keeps the map bounded by the set of
/// definitions with live instances). `None` = the transition was a no-op (already
/// terminal / unknown instance), so nothing is decremented.
fn decrement_inflight_by_process(state: &mut State, process_id: Option<String>) {
    let Some(pid) = process_id else { return };
    if let Some(count) = state.inflight_by_process.get_mut(&pid) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            state.inflight_by_process.remove(&pid);
        }
    }
}

/// Applies a single [`Event`] to [`State`]. This is the sole mutator of engine
/// state; the processor never mutates [`State`] directly.
pub fn apply(state: &mut State, event: &Event) {
    match event {
        Event::DeploymentCreated { .. } => {
            // No-op today. The event exists so every deploy has a persisted
            // deployment key (issue #47, Option B) — the key counter is
            // advanced via mint_key() at emit time, and replay derives it
            // from the max key in the log, so simply having the event in
            // the journal is enough. Applier promotion to record deployment
            // metadata (resource keys, timestamp, audit hooks) is Option A.
        }

        Event::ProcessDeployed {
            process_definition_key,
            version,
            process,
            ..
        } => {
            let mut definition = process.clone();
            definition.normalize_legacy_agent_tasks();
            let deployed = DeployedProcess {
                key: *process_definition_key,
                version: *version,
                definition,
            };
            // Retain every version, keyed by its unique definition key, so a
            // running instance can always resolve the version it was created on.
            state
                .process_versions
                .insert(*process_definition_key, deployed.clone());
            // Maintain the latest-by-id index. In sequential replay versions
            // strictly increase, so the last applied wins; the `>=` guard makes
            // out-of-order snapshot merge (see `install_deployment_if_newer`)
            // monotonic — an older surviving durable copy never regresses the
            // latest pointer.
            let is_latest = state
                .processes
                .get(&process.id)
                .map(|existing| *version >= existing.version)
                .unwrap_or(true);
            if is_latest {
                state.processes.insert(process.id.clone(), deployed);
            }
        }

        Event::DecisionRequirementsDeployed {
            decision_requirements_key,
            version,
            drg,
            ..
        } => {
            let deployed = DeployedDrg {
                key: *decision_requirements_key,
                version: *version,
                drg: drg.clone(),
            };
            // Retain every version by its unique key so a decision that
            // references an older DRG (see the DecisionDeployed arm) resolves.
            state
                .decision_requirements_versions
                .insert(*decision_requirements_key, deployed.clone());
            // Maintain the latest-by-id index; the `>=` guard keeps an
            // out-of-order durable copy from regressing the latest pointer.
            let is_latest = state
                .decision_requirements
                .get(&drg.id)
                .map(|existing| *version >= existing.version)
                .unwrap_or(true);
            if is_latest {
                state.decision_requirements.insert(drg.id.clone(), deployed);
            }
        }

        Event::DecisionDeployed {
            decision_requirements_key,
            decision_key,
            decision_id,
            decision_name,
            version,
            ..
        } => {
            // The DRG carrying this decision was applied by the preceding
            // DecisionRequirementsDeployed event; look it up by its exact key in
            // the version-retention map (the latest-by-id index may already point
            // at a newer DRG) to bind for eval.
            if let Some(drg) = state
                .decision_requirements_versions
                .get(decision_requirements_key)
                .map(|d| d.drg.clone())
            {
                let deployed = DeployedDecision {
                    key: *decision_key,
                    version: *version,
                    decision_requirements_key: *decision_requirements_key,
                    decision_id: decision_id.clone(),
                    decision_name: decision_name.clone(),
                    drg,
                };
                // Retain every version by its unique key so an EvaluateDecision
                // pinned to an older decision key still resolves.
                state
                    .decision_versions
                    .insert(*decision_key, deployed.clone());
                let is_latest = state
                    .decisions
                    .get(decision_id)
                    .map(|existing| *version >= existing.version)
                    .unwrap_or(true);
                if is_latest {
                    state.decisions.insert(decision_id.clone(), deployed);
                }
            }
        }

        Event::FormDeployed {
            form_key,
            version,
            form_id,
            resource_name,
            schema,
            ..
        } => {
            let deployed = DeployedForm {
                key: *form_key,
                version: *version,
                form_id: form_id.clone(),
                resource_name: resource_name.clone(),
                schema: schema.clone(),
            };
            // Retain every version by its unique key so a form binding pinned to
            // a specific version resolves.
            state.form_versions.insert(*form_key, deployed.clone());
            let is_latest = state
                .forms
                .get(form_id)
                .map(|existing| *version >= existing.version)
                .unwrap_or(true);
            if is_latest {
                state.forms.insert(form_id.clone(), deployed);
            }
        }

        Event::GenericResourceDeployed {
            resource_key,
            version,
            resource_id,
            resource_name,
            content,
            ..
        } => {
            let deployed = DeployedResource {
                key: *resource_key,
                version: *version,
                resource_id: resource_id.clone(),
                resource_name: resource_name.clone(),
                content: content.clone(),
            };
            // Retain every version by its unique key so a linked-resource binding
            // pinned to a specific version (or an older key) resolves.
            state
                .resource_versions
                .insert(*resource_key, deployed.clone());
            let is_latest = state
                .resources
                .get(resource_id)
                .map(|existing| *version >= existing.version)
                .unwrap_or(true);
            if is_latest {
                state.resources.insert(resource_id.clone(), deployed);
            }
        }

        Event::DecisionEvaluated { .. } => {
            // Informational/audit only: the decision output is propagated to the
            // instance via a separate VariablesUpdated event, and the record is
            // surfaced to the exporter. No core state to mutate.
        }
        Event::DecisionInstanceDeleted { .. } => {
            // Audit/projection-only: the read model deletes the retracted decision
            // instance rows. No core engine state to mutate.
        }
        Event::ProcessInstanceCreated {
            instance_key,
            process_id,
            variables,
            created_at,
            tags,
            business_id,
            process_definition_key,
            parent_process_instance_key,
            parent_element_instance_key,
            ..
        } => {
            *state
                .inflight_by_process
                .entry(process_id.clone())
                .or_insert(0) += 1;
            *state
                .created_by_process
                .entry(process_id.clone())
                .or_insert(0) += 1;
            // Pin to the exact version the event carries; fall back to the
            // current latest for events written before version pinning (`0`).
            let pinned_key = if *process_definition_key != 0 {
                *process_definition_key
            } else {
                state.processes.get(process_id).map(|d| d.key).unwrap_or(0)
            };
            state.instances.insert(
                *instance_key,
                ProcessInstance {
                    key: *instance_key,
                    process_id: process_id.clone(),
                    process_definition_key: pinned_key,
                    state: ProcessInstanceState::Active,
                    suspended_at: None,
                    created_at: *created_at,
                    tags: tags.clone(),
                    business_id: business_id.clone(),
                    parent_process_instance_key: *parent_process_instance_key,
                    parent_element_instance_key: *parent_element_instance_key,
                    active: HashMap::new(),
                    scopes: HashMap::new(),
                    variables: Arc::new(variables.clone()),
                    join_counts: HashMap::new(),
                    join_instances: HashMap::new(),
                    incidents: Vec::new(),
                    variables_spilled: false,
                    multi_instances: HashMap::new(),
                    adhoc_instances: HashMap::new(),
                    scope_parents: HashMap::new(),
                    scope_variables: HashMap::new(),
                    compensable: Vec::new(),
                    compensation_waits: HashMap::new(),
                    agent_instances: HashMap::new(),
                    agent_history: HashMap::new(),
                },
            );
        }

        Event::VariablesUpdated {
            instance_key,
            variables,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                let map = Arc::make_mut(&mut instance.variables);
                for (k, v) in variables {
                    map.insert(k.clone(), v.clone());
                }
            }
        }

        Event::ScopedVariablesUpdated {
            instance_key,
            scope_key,
            variables,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                // A write targeting the root scope lands in the shared `variables`
                // Arc (the flat fast path); any other scope holds its own local map.
                if *scope_key == 0 || *scope_key == *instance_key {
                    let map = Arc::make_mut(&mut instance.variables);
                    for (k, v) in variables {
                        map.insert(k.clone(), v.clone());
                    }
                } else {
                    let map = instance.scope_variables.entry(*scope_key).or_default();
                    for (k, v) in variables {
                        map.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        Event::VariableScopeCreated {
            instance_key,
            scope_key,
            parent_scope_key,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.scope_parents.insert(*scope_key, *parent_scope_key);
                instance.scope_variables.entry(*scope_key).or_default();
            }
        }

        Event::VariableScopeDestroyed {
            instance_key,
            scope_key,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.scope_parents.remove(scope_key);
                instance.scope_variables.remove(scope_key);
            }
        }

        // ACTIVATING/COMPLETING are transient transitions with no state change.
        Event::ElementActivating { .. } | Event::ElementCompleting { .. } => {}

        Event::ElementActivated {
            instance_key,
            element_instance_key,
            element_id,
            scope,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance
                    .active
                    .insert(*element_instance_key, element_id.clone());
                // A non-zero scope records the enclosing sub-process instance.
                if *scope != 0 {
                    instance.scopes.insert(*element_instance_key, *scope);
                }
            }
        }

        Event::ElementCompleted {
            instance_key,
            element_instance_key,
            ..
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.active.remove(element_instance_key);
                instance.scopes.remove(element_instance_key);
                // Zeebe drops a scope's local variables (an activity's input
                // mappings, a sub-process's or multi-instance child's locals) when
                // the element completes. A no-op for elements that never opened a
                // scope.
                instance.scope_parents.remove(element_instance_key);
                instance.scope_variables.remove(element_instance_key);
            }
        }

        // Sequence flows are routing facts; token bookkeeping happens via the
        // activate/complete of the elements they connect.
        Event::SequenceFlowTaken { .. } => {}

        Event::ParallelJoinOpened {
            instance_key,
            element_instance_key,
            element_id,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance
                    .join_instances
                    .insert(element_id.clone(), *element_instance_key);
            }
        }

        Event::ParallelJoinTokenArrived {
            instance_key,
            element_id,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                *instance.join_counts.entry(element_id.clone()).or_insert(0) += 1;
            }
        }

        Event::ParallelJoinReset {
            instance_key,
            element_id,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.join_counts.remove(element_id);
                instance.join_instances.remove(element_id);
            }
        }

        Event::JobCreated {
            job_key,
            instance_key,
            element_instance_key,
            element_id,
            job_type,
            created_at,
            priority,
            retries,
        } => {
            state.jobs.insert(
                *job_key,
                Job {
                    key: *job_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    job_type: job_type.clone(),
                    state: JobState::Created,
                    worker: None,
                    deadline: None,
                    activated_at: None,
                    activation_timeout: None,
                    lease_token: None,
                    durable_activation: false,
                    activated: false,
                    retries: *retries,
                    priority: *priority,
                    created_at: *created_at,
                    kind: JobKind::BpmnElement,
                },
            );
            resync_job_index(state, *job_key);
            state
                .jobs_by_instance
                .entry(*instance_key)
                .or_default()
                .insert(*job_key);
        }

        Event::AgentInstanceCreated {
            instance_key,
            agent_instance,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance
                    .agent_instances
                    .insert(agent_instance.agent_instance_key, agent_instance.clone());
            }
        }

        // UPDATE and COMPLETE both replay as an upsert of the whole record — the
        // event carries the full post-transition value, mirroring
        // `AgentInstanceCreated`, so state rebuilds identically on replay.
        Event::AgentInstanceUpdated {
            instance_key,
            agent_instance,
        }
        | Event::AgentInstanceCompleted {
            instance_key,
            agent_instance,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance
                    .agent_instances
                    .insert(agent_instance.agent_instance_key, agent_instance.clone());
            }
        }

        Event::AgentHistoryCreated {
            instance_key,
            record,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                let log = instance
                    .agent_history
                    .entry(record.agent_instance_key)
                    .or_default();
                // Append-only, kept sorted by (loop_iteration, produced_at,
                // agent_history_key): find the insertion point rather than
                // pushing + re-sorting so replay is deterministic and cheap.
                let pos = log.partition_point(|r| r.order_key() <= record.order_key());
                log.insert(pos, record.clone());
            }
        }

        Event::AgentHistoryCommitted {
            instance_key,
            agent_instance_key,
            agent_history_keys,
        } => {
            if let Some(log) = state
                .instances
                .get_mut(instance_key)
                .and_then(|inst| inst.agent_history.get_mut(agent_instance_key))
            {
                let keys: HashSet<Key> = agent_history_keys.iter().copied().collect();
                for record in log.iter_mut() {
                    // Only PENDING turns transition; a committed/discarded turn
                    // is immutable (append-only).
                    if record.commit_status == crate::agent::AgentHistoryCommitStatus::Pending
                        && keys.contains(&record.agent_history_key)
                    {
                        record.commit_status = crate::agent::AgentHistoryCommitStatus::Committed;
                    }
                }
            }
        }

        Event::AgentHistoryDiscarded {
            instance_key,
            agent_instance_key,
            agent_history_keys,
        } => {
            if let Some(log) = state
                .instances
                .get_mut(instance_key)
                .and_then(|inst| inst.agent_history.get_mut(agent_instance_key))
            {
                let keys: HashSet<Key> = agent_history_keys.iter().copied().collect();
                for record in log.iter_mut() {
                    if record.commit_status == crate::agent::AgentHistoryCommitStatus::Pending
                        && keys.contains(&record.agent_history_key)
                    {
                        record.commit_status = crate::agent::AgentHistoryCommitStatus::Discarded;
                    }
                }
            }
        }

        // Dedup outcome only — the append-only log is intentionally left
        // untouched (no record is created for an idempotent retry).
        Event::AgentHistoryDeduplicated { .. } => {}

        Event::ExecutionListenerJobCreated {
            job_key,
            instance_key,
            element_instance_key,
            element_id,
            job_type,
            event_type,
            listener_index,
            scope,
            created_at,
            retries,
        } => {
            state.jobs.insert(
                *job_key,
                Job {
                    key: *job_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    job_type: job_type.clone(),
                    state: JobState::Created,
                    worker: None,
                    deadline: None,
                    activated_at: None,
                    activation_timeout: None,
                    lease_token: None,
                    durable_activation: false,
                    activated: false,
                    retries: *retries,
                    priority: DEFAULT_JOB_PRIORITY,
                    created_at: *created_at,
                    kind: JobKind::ExecutionListener {
                        event_type: *event_type,
                        index: *listener_index,
                        scope: *scope,
                    },
                },
            );
            resync_job_index(state, *job_key);
            state
                .jobs_by_instance
                .entry(*instance_key)
                .or_default()
                .insert(*job_key);
        }

        Event::TaskListenerJobCreated {
            job_key,
            instance_key,
            element_instance_key,
            element_id,
            user_task_key,
            job_type,
            event_type,
            listener_index,
            created_at,
            retries,
        } => {
            state.jobs.insert(
                *job_key,
                Job {
                    key: *job_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    job_type: job_type.clone(),
                    state: JobState::Created,
                    worker: None,
                    deadline: None,
                    activated_at: None,
                    activation_timeout: None,
                    lease_token: None,
                    durable_activation: false,
                    activated: false,
                    retries: *retries,
                    priority: DEFAULT_JOB_PRIORITY,
                    created_at: *created_at,
                    kind: JobKind::TaskListener {
                        event_type: *event_type,
                        index: *listener_index,
                        user_task_key: *user_task_key,
                    },
                },
            );
            resync_job_index(state, *job_key);
            state
                .jobs_by_instance
                .entry(*instance_key)
                .or_default()
                .insert(*job_key);
        }

        Event::UserTaskTransitionDeferred {
            user_task_key,
            pending,
            ..
        } => {
            if let Some(task) = state.user_tasks.get_mut(user_task_key) {
                task.pending = Some(pending.clone());
            }
        }

        Event::UserTaskCorrectionsApplied {
            user_task_key,
            corrections,
            ..
        } => {
            if let Some(task) = state.user_tasks.get_mut(user_task_key) {
                if let Some(pending) = task.pending.as_mut() {
                    pending.corrections.merge(corrections);
                }
            }
        }

        Event::UserTaskTransitionResolved { user_task_key, .. } => {
            if let Some(task) = state.user_tasks.get_mut(user_task_key) {
                task.pending = None;
            }
        }

        Event::JobActivated {
            job_key,
            durable,
            worker,
            deadline,
            activated_at,
            lease_token,
            ..
        } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.state = JobState::Activated;
                // An *empty* activation worker string is not an attribution:
                // normalize it to `None` here, at the single source, so an
                // explicitly-supplied `""` never becomes `Some("")`. This keeps
                // `Job.worker` canonical for every downstream derivation — the
                // terminal `JobCompleted`/`JobFailed`/`JobErrorThrown` events and
                // the read-model attribution bindings — so no empty attribution
                // can be stamped or `COALESCE`d into a row that should stay NULL.
                job.worker = Some(worker.clone()).filter(|w| !w.is_empty());
                job.deadline = Some(*deadline);
                job.activated_at = *activated_at;
                // Freeze the requested lock duration at activation, from the two
                // instants the event carries (deadline = activated_at + timeout).
                // Immune to later UpdateJobTimeout extensions that move `deadline`.
                job.activation_timeout = activated_at.map(|a| deadline.saturating_sub(a));
                // Restore the opaque per-activation lease token from the event
                // (minted once at command-processing, ADR 0005-810-job-lease D2):
                // replay reads it here rather than regenerating it. `None` for a
                // lease-less activation.
                job.lease_token = lease_token.clone();
                job.durable_activation |= *durable;
                job.activated = true;
            }
            resync_job_index(state, *job_key);
        }

        Event::JobLockExpired { job_key, .. } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                if job.state == JobState::Activated {
                    job.state = JobState::Created;
                    job.worker = None;
                    job.deadline = None;
                    job.activated_at = None;
                    job.activation_timeout = None;
                }
            }
            resync_job_index(state, *job_key);
        }

        Event::JobFailed {
            job_key,
            retries,
            worker,
            ..
        } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.retries = *retries;
                job.deadline = None;
                job.activated_at = None;
                job.activation_timeout = None;
                // With retries left the job returns to the activatable pool; with
                // none it parks (an incident is raised alongside this event).
                if *retries > 0 {
                    // Back to the activatable pool — no longer held, so drop the
                    // last activating worker (mirrors JobLockExpired).
                    job.worker = None;
                    job.state = JobState::Created;
                } else {
                    // Terminal, incident-bearing park: retain the activating
                    // `worker` so the incident (joined by `jobKey`) can attribute
                    // the failure to the worker/host that was running it (Zeebe
                    // parity — a failed JobRecord retains its `worker`). The event
                    // carries the worker so this survives a restart replay (where
                    // the volatile, unexported `JobActivated` lock is gone); fall
                    // back to any live value for events serialized before the field
                    // existed.
                    if worker.is_some() {
                        job.worker = worker.clone();
                    }
                    job.state = JobState::Failed;
                }
            }
            resync_job_index(state, *job_key);
        }

        Event::JobErrorThrown {
            job_key, worker, ..
        } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.state = JobState::Errored;
                job.deadline = None;
                job.activated_at = None;
                job.activation_timeout = None;
                job.lease_token = None;
                // Terminal, incident-bearing transition: retain the activating
                // `worker` for attribution (Zeebe parity — throwError retains the
                // record incl. `worker`). Carried on the event so it survives a
                // restart replay; fall back to any live value for pre-field events.
                if worker.is_some() {
                    job.worker = worker.clone();
                }
            }
            resync_job_index(state, *job_key);
        }

        Event::JobCompleted { job_key, .. } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.state = JobState::Completed;
                job.worker = None;
                job.deadline = None;
                job.activated_at = None;
                job.activation_timeout = None;
            }
            resync_job_index(state, *job_key);
        }

        Event::UserTaskCreated {
            user_task_key,
            instance_key,
            element_instance_key,
            element_id,
            created_at,
            assignee,
            candidate_groups,
            candidate_users,
            due_date,
            follow_up_date,
            priority,
            form_key,
            external_form_reference,
        } => {
            state.user_tasks.insert(
                *user_task_key,
                UserTask {
                    key: *user_task_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    state: UserTaskState::Created,
                    assignee: assignee.clone(),
                    candidate_groups: candidate_groups.clone(),
                    candidate_users: candidate_users.clone(),
                    due_date: due_date.clone(),
                    follow_up_date: follow_up_date.clone(),
                    priority: *priority,
                    form_key: *form_key,
                    external_form_reference: external_form_reference.clone(),
                    created_at: *created_at,
                    pending: None,
                },
            );
        }

        Event::UserTaskAssigned {
            user_task_key,
            assignee,
            ..
        } => {
            if let Some(task) = state.user_tasks.get_mut(user_task_key) {
                task.assignee = assignee.clone();
            }
        }

        Event::UserTaskUpdated {
            user_task_key,
            candidate_groups,
            candidate_users,
            due_date,
            follow_up_date,
            priority,
            ..
        } => {
            if let Some(task) = state.user_tasks.get_mut(user_task_key) {
                if let Some(groups) = candidate_groups {
                    task.candidate_groups = groups.clone();
                }
                if let Some(users) = candidate_users {
                    task.candidate_users = users.clone();
                }
                if let Some(due) = due_date {
                    task.due_date = due.clone();
                }
                if let Some(follow_up) = follow_up_date {
                    task.follow_up_date = follow_up.clone();
                }
                if let Some(p) = priority {
                    task.priority = *p;
                }
            }
        }

        Event::UserTaskCompleted { user_task_key, .. } => {
            if let Some(task) = state.user_tasks.get_mut(user_task_key) {
                task.state = UserTaskState::Completed;
            }
        }

        Event::IncidentRaised {
            incident_key,
            instance_key,
            element_instance_key,
            element_id,
            kind,
            reason,
            job_key,
            created_at,
            redrive,
        } => {
            state.incidents.insert(
                *incident_key,
                Incident {
                    key: *incident_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    kind: *kind,
                    redrive: redrive.clone(),
                    reason: reason.clone(),
                    job_key: *job_key,
                    created_at: *created_at,
                    state: IncidentState::Active,
                    resolved_at: None,
                    operation_reference: None,
                },
            );
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.incidents.push(*incident_key);
            }
        }

        Event::JobRetriesUpdated {
            job_key, retries, ..
        } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.retries = *retries;
            }
        }

        Event::JobTimeoutUpdated {
            job_key, deadline, ..
        } => {
            // The job stays Activated (still locked by the same worker); only its
            // lock deadline moves out. Index membership is unchanged.
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.deadline = Some(*deadline);
            }
        }

        Event::IncidentResolved {
            incident_key,
            instance_key,
            job_key,
            resolved_at,
            operation_reference,
        } => {
            // Retain the record as an audit trail: transition it to Resolved
            // rather than dropping it.
            if let Some(incident) = state.incidents.get_mut(incident_key) {
                incident.state = IncidentState::Resolved;
                incident.resolved_at = Some(*resolved_at);
                incident.operation_reference = *operation_reference;
            }
            // Remove it from the instance's *active* index so `hasIncident`
            // reflects only open incidents.
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.incidents.retain(|k| k != incident_key);
            }
            // A recoverable job-incident: return the parked job to the
            // activatable pool so a worker can pick it up again — but *only* if
            // it is still `Failed` (parked). A job already driven to a terminal
            // state (`Canceled`/`Completed`/`Errored`) by a concurrent forced
            // teardown must not be resurrected by resolving its retained
            // incident: scope teardown emits `JobCanceled` and then
            // `IncidentResolved` for the same job in one batch, so an
            // unconditional reset would return the just-cancelled job to
            // `Created` and let a worker activate it against an element instance
            // that is being removed. The normal `ResolveIncident` command only
            // reaches here with a `Failed` job (retries validated first), so
            // this guard is transparent to it.
            if let Some(job_key) = job_key {
                let is_failed = matches!(
                    state.jobs.get(job_key).map(|j| j.state),
                    Some(JobState::Failed)
                );
                if is_failed {
                    if let Some(job) = state.jobs.get_mut(job_key) {
                        job.state = JobState::Created;
                        job.worker = None;
                        job.deadline = None;
                        job.activated_at = None;
                        job.activation_timeout = None;
                        job.lease_token = None;
                    }
                    resync_job_index(state, *job_key);
                }
            }
        }

        Event::ProcessInstanceCompleted { instance_key } => {
            let terminal_pid = non_terminal_process_id(state, instance_key);
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.state = ProcessInstanceState::Completed;
                instance.suspended_at = None;
                // Clear any residual runtime bookkeeping. On a normal completion
                // these are already empty (the instance completes only with an
                // empty `active` map); a top-level terminate end event completes
                // the instance while sibling tokens/scopes are still recorded, so
                // this forced teardown (mirroring `ProcessInstanceTerminated`)
                // keeps the terminal snapshot consistent — no live scope for the
                // dead-scope guard to read, no stranded MI/ad-hoc/join/
                // compensation payload on the terminal shell. Incidents are NOT
                // closed here (unlike `ProcessInstanceTerminated`): a normal
                // completion may legitimately leave a retained incident record
                // for later resolution, and a top-level terminate end resolves
                // its own incidents explicitly (`IncidentResolved` events emitted
                // ahead of this record — see `complete_terminate_end`).
                instance.active.clear();
                instance.scopes.clear();
                instance.multi_instances.clear();
                instance.adhoc_instances.clear();
                instance.join_counts.clear();
                instance.join_instances.clear();
                instance.compensable.clear();
                instance.compensation_waits.clear();
                // A terminal instance's variables are never read from hot state
                // again — workers are done, the exporter projects from events,
                // and recovery replays the journal + durable store. Drop the
                // payload now to reclaim heap immediately, decoupling
                // terminal-state memory from exporter-driven eviction (ADR 0012).
                // The instance shell stays resident until eviction so status
                // queries still resolve during the read-model projection gap.
                if !instance.variables.is_empty() {
                    instance.variables = Arc::new(HashMap::new());
                }
                instance.variables_spilled = false;
                instance.scope_variables.clear();
                instance.scope_parents.clear();
            }
            decrement_inflight_by_process(state, terminal_pid);
        }

        Event::ProcessInstanceTerminating { instance_key } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.state = ProcessInstanceState::Terminating;
            }
        }

        Event::ProcessInstanceSuspended { instance_key, at } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.state = ProcessInstanceState::Suspended;
                instance.suspended_at = Some(*at);
            }
        }

        Event::ProcessInstanceResumed { instance_key } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.state = ProcessInstanceState::Active;
                instance.suspended_at = None;
            }
        }

        Event::ProcessInstanceMigrated {
            instance_key,
            target_process_id,
            target_process_definition_key,
            element_mappings,
        } => {
            let remap: HashMap<&str, &str> = element_mappings
                .iter()
                .map(|(s, t)| (s.as_str(), t.as_str()))
                .collect();
            let remap_id = |id: &mut ElementId| {
                if let Some(target) = remap.get(id.as_str()) {
                    *id = (*target).to_string();
                }
            };

            // Move the live-instance count from the source process id to the
            // target's, and re-point the instance itself.
            let source_process_id = state
                .instances
                .get(instance_key)
                .map(|i| i.process_id.clone());
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.process_id = target_process_id.clone();
                // Re-pin the instance to the target definition's version so
                // `definition_for` resolves execution against the migrated-to
                // model rather than the source it was created on.
                instance.process_definition_key = *target_process_definition_key;
                for element_id in instance.active.values_mut() {
                    remap_id(element_id);
                }
                // Both parallel-join maps are keyed by the join gateway's element
                // id, so they must be remapped together to stay in sync (a stale
                // `join_instances` key would make `join_eik` miss after migration
                // and re-open an already-open join). `collect()` would silently
                // drop entries if two source ids collapse onto one target id, so
                // merge deterministically instead: sum the arrival counts, and
                // keep the smallest element-instance key for the open join.
                if !instance.join_counts.is_empty() {
                    let mut remapped: HashMap<ElementId, usize> =
                        HashMap::with_capacity(instance.join_counts.len());
                    for (mut eid, count) in instance.join_counts.drain() {
                        remap_id(&mut eid);
                        *remapped.entry(eid).or_insert(0) += count;
                    }
                    instance.join_counts = remapped;
                }
                if !instance.join_instances.is_empty() {
                    let mut remapped: HashMap<ElementId, Key> =
                        HashMap::with_capacity(instance.join_instances.len());
                    for (mut eid, eik) in instance.join_instances.drain() {
                        remap_id(&mut eid);
                        remapped
                            .entry(eid)
                            .and_modify(|existing| {
                                if eik < *existing {
                                    *existing = eik;
                                }
                            })
                            .or_insert(eik);
                    }
                    instance.join_instances = remapped;
                }
            }
            if source_process_id.as_deref() != Some(target_process_id.as_str()) {
                decrement_inflight_by_process(state, source_process_id);
                *state
                    .inflight_by_process
                    .entry(target_process_id.clone())
                    .or_insert(0) += 1;
            }

            // Re-point every element instance's attached runtime. Active jobs keep
            // their type (a worker already holds the lease) — only the element id
            // moves, mirroring Zeebe.
            for job in state.jobs.values_mut() {
                if job.instance_key == *instance_key {
                    remap_id(&mut job.element_id);
                }
            }
            for user_task in state.user_tasks.values_mut() {
                if user_task.instance_key == *instance_key {
                    remap_id(&mut user_task.element_id);
                }
            }
            for timer in state.timers.values_mut() {
                if timer.instance_key == *instance_key {
                    remap_id(&mut timer.element_id);
                }
            }
            for sub in state.message_subscriptions.values_mut() {
                if sub.instance_key == *instance_key {
                    remap_id(&mut sub.element_id);
                }
            }
            for sub in state.signal_subscriptions.values_mut() {
                if sub.instance_key == *instance_key {
                    remap_id(&mut sub.element_id);
                }
            }
            for sub in state.conditional_subscriptions.values_mut() {
                if sub.instance_key == *instance_key {
                    remap_id(&mut sub.element_id);
                }
            }
            for incident in state.incidents.values_mut() {
                if incident.instance_key == *instance_key {
                    remap_id(&mut incident.element_id);
                }
            }
            // The scope tree (`scopes` / `scope_parents` / `scope_variables`) is
            // intentionally NOT remapped: the command handler rejects any
            // instance whose active tokens live inside a non-root flow scope
            // (embedded sub-process, multi-instance, or ad-hoc) as unsupported,
            // so an instance that reaches this applier is flat (root scope only)
            // and has nothing to remap. See the "flow scope unchanged"
            // precondition in `Command::MigrateInstance` validation.
        }

        Event::ProcessInstanceTerminated { instance_key } => {
            let terminal_pid = non_terminal_process_id(state, instance_key);
            // Close any incident still active on the instance: with the instance
            // gone the parked tokens are gone too, so `hasIncident` must clear.
            // The resource cancellations (jobs/timers/subscriptions) were emitted
            // as their own events ahead of this one.
            if let Some(instance) = state.instances.get(instance_key) {
                for incident_key in instance.incidents.clone() {
                    if let Some(incident) = state.incidents.get_mut(&incident_key) {
                        incident.state = IncidentState::Resolved;
                    }
                }
            }
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.state = ProcessInstanceState::Terminated;
                instance.suspended_at = None;
                instance.active.clear();
                instance.scopes.clear();
                instance.incidents.clear();
                // Drop the multi-instance / ad-hoc runtime records too: a forced
                // teardown removes the body/container token but these maps are
                // otherwise cleared only by `MultiInstanceCompleted` /
                // `AdHocCompleted`, so terminating an instance mid-loop would
                // retain the whole item/output/active-set payload on a terminal
                // instance until eviction (and leave a stale "live scope" for the
                // dead-scope guard to read). Immediate terminal cleanup (ADR 0012).
                instance.multi_instances.clear();
                instance.adhoc_instances.clear();
                // Same forced-teardown rationale for the remaining runtime
                // bookkeeping a mid-flight terminate can leave behind: an open
                // parallel join (`join_counts`/`join_instances`) or a pending
                // compensation (`compensable`/`compensation_waits`) is drained
                // naturally only on normal completion. Terminating mid-join or
                // mid-compensation would otherwise strand this bookkeeping —
                // potentially a large compensation list — on the terminal
                // instance shell until eviction, leaving its terminal snapshot
                // inconsistent with the MI/ad-hoc/variable state cleared above.
                instance.join_counts.clear();
                instance.join_instances.clear();
                instance.compensable.clear();
                instance.compensation_waits.clear();
                // Drop the variable payload on the terminal transition — see
                // `ProcessInstanceCompleted` above (ADR 0012).
                if !instance.variables.is_empty() {
                    instance.variables = Arc::new(HashMap::new());
                }
                instance.variables_spilled = false;
                instance.scope_variables.clear();
                instance.scope_parents.clear();
            }
            decrement_inflight_by_process(state, terminal_pid);
        }

        Event::TimerCreated {
            timer_key,
            instance_key,
            element_instance_key,
            element_id,
            due_at,
            kind,
        } => {
            state.timers.insert(
                *timer_key,
                Timer {
                    key: *timer_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    due_at: *due_at,
                    state: TimerState::Created,
                    kind: kind.clone(),
                },
            );
        }

        Event::TimerTriggered { timer_key, .. } => {
            if let Some(timer) = state.timers.get_mut(timer_key) {
                timer.state = TimerState::Triggered;
            }
        }

        Event::TimerCanceled { timer_key, .. } => {
            if let Some(timer) = state.timers.get_mut(timer_key) {
                timer.state = TimerState::Canceled;
            }
        }

        Event::JobCanceled { job_key, .. } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.state = JobState::Canceled;
                job.worker = None;
                job.deadline = None;
                job.activated_at = None;
                job.activation_timeout = None;
                job.lease_token = None;
            }
            resync_job_index(state, *job_key);
        }

        Event::UserTaskCanceled { user_task_key, .. } => {
            if let Some(task) = state.user_tasks.get_mut(user_task_key) {
                task.state = UserTaskState::Canceled;
                // A cancelled task has no in-flight transition: its listener job
                // (if any) was cancelled with the instance's other jobs. Clearing
                // pending keeps replay from reconstructing a terminal task with a
                // permanently unresolved transition. No-op for listener-free tasks
                // (pending is already None), so the journal stays byte-identical.
                task.pending = None;
            }
        }

        // Publishing a message is, in nano, a transient fact: messages are not
        // buffered, so there is nothing to record. The event exists only to mint
        // a deterministic message key (carried to the host for its response and
        // restored on replay) and to head the events a correlation produced.
        Event::MessagePublished { .. } => {}

        Event::MessageSubscriptionCreated {
            subscription_key,
            instance_key,
            element_instance_key,
            element_id,
            message_name,
            correlation_key,
            kind,
        } => {
            state.message_subscriptions.insert(
                *subscription_key,
                MessageSubscription {
                    key: *subscription_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    message_name: message_name.clone(),
                    correlation_key: correlation_key.clone(),
                    state: MessageSubscriptionState::Open,
                    kind: kind.clone(),
                },
            );
        }

        // The instance partition's pending view of a subscription whose canonical
        // record lives on the message partition (`hash(correlation_key)`). Holds
        // the token until a `CorrelateMessageSubscription` continuation arrives.
        Event::MessageSubscriptionOpening {
            subscription_key,
            instance_key,
            element_instance_key,
            element_id,
            message_name,
            correlation_key,
            kind,
        } => {
            state.message_subscriptions.insert(
                *subscription_key,
                MessageSubscription {
                    key: *subscription_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    message_name: message_name.clone(),
                    correlation_key: correlation_key.clone(),
                    state: MessageSubscriptionState::Opening,
                    kind: kind.clone(),
                },
            );
        }

        Event::MessageCorrelated {
            subscription_key, ..
        } => {
            if let Some(subscription) = state.message_subscriptions.get_mut(subscription_key) {
                // A non-interrupting boundary subscription stays open so every
                // matching message spawns another token; all others settle.
                if !matches!(
                    subscription.kind,
                    MessageSubscriptionKind::NonInterruptingBoundary { .. }
                ) {
                    subscription.state = MessageSubscriptionState::Correlated;
                }
            }
        }

        Event::MessageSubscriptionCanceled {
            subscription_key, ..
        } => {
            if let Some(subscription) = state.message_subscriptions.get_mut(subscription_key) {
                subscription.state = MessageSubscriptionState::Canceled;
            }
        }

        // A signal subscription was opened on a signal intermediate catch event
        // (the token rests on it) or as a signal boundary on an activity.
        Event::SignalSubscriptionCreated {
            subscription_key,
            instance_key,
            element_instance_key,
            element_id,
            signal_name,
            kind,
        } => {
            state.signal_subscriptions.insert(
                *subscription_key,
                SignalSubscription {
                    key: *subscription_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    signal_name: signal_name.clone(),
                    state: MessageSubscriptionState::Open,
                    kind: kind.clone(),
                },
            );
        }

        // A broadcast signal correlated to an open subscription. Settles it
        // (unless it is a non-interrupting boundary, which stays open so every
        // broadcast spawns another token), exactly like `MessageCorrelated`.
        Event::SignalCorrelated {
            subscription_key, ..
        } => {
            if let Some(subscription) = state.signal_subscriptions.get_mut(subscription_key) {
                if !matches!(
                    subscription.kind,
                    MessageSubscriptionKind::NonInterruptingBoundary { .. }
                ) {
                    subscription.state = MessageSubscriptionState::Correlated;
                }
            }
        }

        // An open signal subscription was cancelled because the element it
        // guarded left the flow first (mirrors `MessageSubscriptionCanceled`).
        Event::SignalSubscriptionCanceled {
            subscription_key, ..
        } => {
            if let Some(subscription) = state.signal_subscriptions.get_mut(subscription_key) {
                subscription.state = MessageSubscriptionState::Canceled;
            }
        }

        Event::ConditionalSubscriptionCreated {
            subscription_key,
            instance_key,
            element_instance_key,
            element_id,
            condition,
            referenced_vars,
            kind,
        } => {
            state.conditional_subscriptions.insert(
                *subscription_key,
                ConditionalSubscription {
                    key: *subscription_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    condition: condition.clone(),
                    referenced_vars: referenced_vars.clone(),
                    state: MessageSubscriptionState::Open,
                    kind: kind.clone(),
                },
            );
        }

        // A conditional subscription's condition became true. Settles it (unless
        // it is a non-interrupting boundary, which stays open so every satisfying
        // variable change spawns another token), exactly like `SignalCorrelated`.
        Event::ConditionalTriggered {
            subscription_key, ..
        } => {
            if let Some(subscription) = state.conditional_subscriptions.get_mut(subscription_key) {
                if !matches!(
                    subscription.kind,
                    MessageSubscriptionKind::NonInterruptingBoundary { .. }
                ) {
                    subscription.state = MessageSubscriptionState::Correlated;
                }
            }
        }

        // An open conditional subscription was cancelled because the element it
        // guarded left the flow first (mirrors `SignalSubscriptionCanceled`).
        Event::ConditionalSubscriptionCanceled {
            subscription_key, ..
        } => {
            if let Some(subscription) = state.conditional_subscriptions.get_mut(subscription_key) {
                subscription.state = MessageSubscriptionState::Canceled;
            }
        }

        // A compensable activity completed: remember it (in completion order) so
        // a later compensation throw event can run its handler.
        Event::CompensationSubscriptionCreated {
            instance_key,
            element_instance_key,
            element_id,
            handler,
            scope,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.compensable.push(CompensationSubscription {
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    handler: handler.clone(),
                    scope: *scope,
                });
            }
        }

        // A compensation throw event fired: consume the compensable subscriptions
        // it triggered and record the handlers it now waits on.
        Event::CompensationTriggered {
            instance_key,
            throw_element_instance_key,
            throw_element_id,
            scope,
            handlers,
            consumed,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance
                    .compensable
                    .retain(|c| !consumed.contains(&c.element_instance_key));
                instance.compensation_waits.insert(
                    *throw_element_instance_key,
                    CompensationWait {
                        throw_element_id: throw_element_id.clone(),
                        scope: *scope,
                        pending_handlers: handlers.clone(),
                    },
                );
            }
        }

        // A compensation handler completed: drop it from its throw's outstanding
        // set, and drop the wait entirely once the last handler is done.
        Event::CompensationHandlerCompleted {
            instance_key,
            throw_element_instance_key,
            handler_element_id,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                let done = if let Some(wait) = instance
                    .compensation_waits
                    .get_mut(throw_element_instance_key)
                {
                    if let Some(pos) = wait
                        .pending_handlers
                        .iter()
                        .position(|h| h == handler_element_id)
                    {
                        wait.pending_handlers.remove(pos);
                    }
                    wait.pending_handlers.is_empty()
                } else {
                    false
                };
                if done {
                    instance
                        .compensation_waits
                        .remove(throw_element_instance_key);
                }
            }
        }

        // A sub-process scope was torn down: drop the compensation state scoped to
        // it (or a nested scope). Mirrors the whole-instance
        // `ProcessInstanceTerminated` sweep, but bounded to the terminated
        // sub-process — `scopes` is the terminated scope plus its descendants, so
        // any completed-activity subscription or pending handler wait belonging to
        // one of them is removed, leaving the still-live parent's compensation
        // state intact.
        Event::ScopedCompensationCleared {
            instance_key,
            scopes,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                let scope_set: std::collections::HashSet<Key> = scopes.iter().copied().collect();
                instance
                    .compensable
                    .retain(|c| !scope_set.contains(&c.scope));
                instance
                    .compensation_waits
                    .retain(|_, w| !scope_set.contains(&w.scope));
            }
        }

        // A multi-instance body activated: record its runtime state so subsequent
        // child spawns/completions and the body's completion are reconstructable.
        Event::MultiInstanceActivated {
            instance_key,
            body_key,
            element_id,
            sequential,
            items,
            input_element,
            output_collection,
            output_element,
            completion_condition,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                let total = items.len();
                instance.multi_instances.insert(
                    *body_key,
                    MultiInstanceState {
                        element_id: element_id.clone(),
                        sequential: *sequential,
                        items: items.clone(),
                        output_collection: output_collection.clone(),
                        output_element: output_element.clone(),
                        completion_condition: completion_condition.clone(),
                        input_element: input_element.clone(),
                        spawned: 0,
                        active: std::collections::BTreeSet::new(),
                        child_indices: std::collections::BTreeMap::new(),
                        output_values: vec![None; total],
                    },
                );
            }
        }

        // A multi-instance child activated: register its own variable scope
        // (holding its `inputElement`/`loopCounter` bindings, parented to the
        // body scope) and mark it active in the body it belongs to.
        Event::MultiInstanceChildActivated {
            instance_key,
            body_key,
            child_key,
            index,
            local_variables,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.scope_parents.insert(*child_key, *body_key);
                instance
                    .scope_variables
                    .insert(*child_key, local_variables.clone());
                if let Some(mi) = instance.multi_instances.get_mut(body_key) {
                    mi.active.insert(*child_key);
                    mi.child_indices.insert(*child_key, *index);
                    mi.spawned = mi.spawned.max(*index + 1);
                }
            }
        }

        // A multi-instance child completed: collect its output at its index and
        // drop it from the active set. Its local scope is torn down by the
        // child's `ElementCompleted` event.
        Event::MultiInstanceChildCompleted {
            instance_key,
            body_key,
            child_key,
            index,
            output,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                if let Some(mi) = instance.multi_instances.get_mut(body_key) {
                    mi.active.remove(child_key);
                    mi.child_indices.remove(child_key);
                    if let Some(slot) = mi.output_values.get_mut(*index) {
                        *slot = output.clone();
                    }
                }
            }
        }

        // A multi-instance body completed: drop its runtime state. The aggregated
        // output and the outgoing flow are carried by surrounding events.
        Event::MultiInstanceCompleted {
            instance_key,
            body_key,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.multi_instances.remove(body_key);
            }
        }

        // An ad-hoc container activated: register its runtime state. Its variable
        // scope is registered separately by the surrounding
        // `VariableScopeCreated` (the container element instance is the scope).
        Event::AdHocActivated {
            instance_key,
            container_key,
            element_id,
            output_collection,
            output_element,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.adhoc_instances.insert(
                    *container_key,
                    AdHocState {
                        element_id: element_id.clone(),
                        output_collection: output_collection.clone(),
                        output_element: output_element.clone(),
                        active: std::collections::BTreeSet::new(),
                        iterations: 0,
                        completion_condition_fulfilled: false,
                    },
                );
            }
        }

        // An ad-hoc tool child activated: register its own variable scope (holding
        // the activate-element seed variables, parented to the container scope)
        // and mark it active in its container.
        Event::AdHocToolActivated {
            instance_key,
            container_key,
            child_key,
            local_variables,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.scope_parents.insert(*child_key, *container_key);
                instance
                    .scope_variables
                    .insert(*child_key, local_variables.clone());
                if let Some(adhoc) = instance.adhoc_instances.get_mut(container_key) {
                    adhoc.active.insert(*child_key);
                }
            }
        }

        // An ad-hoc tool child completed: append its output to the container's
        // `outputCollection` variable (the single source of truth, seeded to an
        // empty array on activation and visible in the container scope mid-run)
        // and drop it from the active set. Its local scope is torn down by the
        // child's `ElementCompleted` event. This applier only ever runs once the
        // append is known to be safe: `complete_adhoc_tool` DEFERS emitting
        // `AdHocToolCompleted` until its type guard confirms the target is an
        // array (parking a retry-on-resolve incident on the tool otherwise), so
        // the `Value::List` match below always holds for a declared collection.
        // A non-list is therefore only reachable when no collection is declared or
        // `output` is `None`, in which case nothing is appended (the output is
        // discarded, matching the pre-collection behaviour).
        Event::AdHocToolCompleted {
            instance_key,
            container_key,
            child_key,
            output,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                let name = instance
                    .adhoc_instances
                    .get(container_key)
                    .and_then(|a| a.output_collection.clone());
                if let Some(adhoc) = instance.adhoc_instances.get_mut(container_key) {
                    adhoc.active.remove(child_key);
                }
                if let (Some(name), Some(value)) = (name, output) {
                    if let Some(Value::List(list)) = instance
                        .scope_variables
                        .get_mut(container_key)
                        .and_then(|m| m.get_mut(&name))
                    {
                        list.push(value.clone());
                    }
                }
            }
        }

        // The declared completion condition was satisfied with
        // `cancelRemainingInstances=false`: latch it so the container stops
        // activating new tools and completes once its children drain.
        Event::AdHocCompletionConditionFulfilled {
            instance_key,
            container_key,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                if let Some(adhoc) = instance.adhoc_instances.get_mut(container_key) {
                    adhoc.completion_condition_fulfilled = true;
                }
            }
        }

        // The ad-hoc container's agent job re-emitted for the next turn: bump the
        // iteration counter.
        Event::AdHocIterated {
            instance_key,
            container_key,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                if let Some(adhoc) = instance.adhoc_instances.get_mut(container_key) {
                    adhoc.iterations = adhoc.iterations.saturating_add(1);
                }
            }
        }

        // An ad-hoc container completed: drop its runtime state. The aggregated
        // output and the outgoing flow are carried by surrounding events.
        Event::AdHocCompleted {
            instance_key,
            container_key,
            ..
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.adhoc_instances.remove(container_key);
            }
        }

        // A signal was broadcast: records no durable state (signals are not
        // buffered); carries the minted `signal_key` to restore the key
        // generator on replay, exactly like `MessagePublished`.
        Event::SignalBroadcast { .. } => {}

        // The instance partition tearing down a cross-partition parked
        // placeholder: mark it cancelled locally exactly like
        // `MessageSubscriptionCanceled`. The host routes a
        // `CloseMessageSubscription` to disarm the canonical record on the
        // message partition.
        Event::MessageSubscriptionClosing {
            subscription_key, ..
        } => {
            if let Some(subscription) = state.message_subscriptions.get_mut(subscription_key) {
                subscription.state = MessageSubscriptionState::Canceled;
            }
        }

        // A match found on the message partition for a subscription whose instance
        // lives on another partition: settle the canonical record exactly as
        // `MessageCorrelated` does (a non-interrupting boundary stays open). The
        // token advance happens on the instance partition, driven by the
        // `CorrelateMessageSubscription` continuation the host routes there.
        Event::RemoteMessageCorrelation {
            subscription_key, ..
        } => {
            if let Some(subscription) = state.message_subscriptions.get_mut(subscription_key) {
                if !matches!(
                    subscription.kind,
                    MessageSubscriptionKind::NonInterruptingBoundary { .. }
                ) {
                    subscription.state = MessageSubscriptionState::Correlated;
                }
            }
        }

        Event::MessageStartSubscriptionCreated {
            process_definition_key,
            process_id,
            message_name,
            start_element_id,
        } => {
            // Keyed by message name: re-deploying a process with the same start
            // message replaces the older version's subscription.
            state.message_start_subscriptions.insert(
                message_name.clone(),
                MessageStartSubscription {
                    process_definition_key: *process_definition_key,
                    process_id: process_id.clone(),
                    message_name: message_name.clone(),
                    start_element_id: start_element_id.clone(),
                },
            );
        }

        Event::ProcessStartTimerArmed {
            timer_key,
            process_definition_key,
            process_id,
            start_element_id,
            due_at,
            interval_millis,
            repeating,
        } => {
            state.start_timers.insert(
                *timer_key,
                StartTimer {
                    timer_key: *timer_key,
                    process_definition_key: *process_definition_key,
                    process_id: process_id.clone(),
                    start_element_id: start_element_id.clone(),
                    due_at: Some(*due_at),
                    interval_millis: *interval_millis,
                    repeating: *repeating,
                },
            );
        }

        Event::ProcessStartTimerFired {
            timer_key,
            next_due_at,
        } => {
            if let Some(start_timer) = state.start_timers.get_mut(timer_key) {
                // A cycle re-arms (next_due_at is Some); a one-shot is retained
                // with due_at = None so a later tick never re-fires it.
                start_timer.due_at = *next_due_at;
            }
        }

        // A routing marker on the deploy partition: the instance is created on
        // the target partition (driven by the host's DispatchStartInstance), so
        // there is no local state to mutate here.
        Event::StartInstanceDispatched { .. } => {}
    }
}

#[cfg(all(test, feature = "serde"))]
mod incident_kind_serde_compat_tests {
    use super::IncidentKind;
    use crate::event::Event;

    /// The `IoMappingOutput` taxonomy (#939) was collapsed into the single
    /// `IoMapping` kind (#946). JSON journals written by that intermediate code
    /// carry `"kind":"IoMappingOutput"`; without the `serde(alias)` they would
    /// fail to deserialize and prevent the server from booting on replay. Guard
    /// the whole defect class: the legacy name must still deserialize.
    #[test]
    fn legacy_io_mapping_output_kind_deserializes_to_io_mapping() {
        let kind: IncidentKind =
            serde_json::from_str("\"IoMappingOutput\"").expect("legacy kind deserializes");
        assert_eq!(kind, IncidentKind::IoMapping);
    }

    /// The alias is deserialize-only: we must never *emit* the retired name, so
    /// new journals stay on the canonical `IoMapping` taxonomy.
    #[test]
    fn io_mapping_kind_serializes_to_canonical_name() {
        let json = serde_json::to_string(&IncidentKind::IoMapping).expect("serializes");
        assert_eq!(json, "\"IoMapping\"");
    }

    /// A full legacy `IncidentRaised` journal line — retired `IoMappingOutput`
    /// kind and no `redrive` field — must replay, mapping to `IoMapping` with a
    /// defaulted `redrive: None`, so boot never fails on an intermediate-version
    /// journal.
    #[test]
    fn legacy_incident_raised_event_replays() {
        let line = r#"{"IncidentRaised":{"incident_key":7,"instance_key":1,"element_instance_key":2,"element_id":"task","kind":"IoMappingOutput","reason":"boom","job_key":null,"created_at":42}}"#;
        let event: Event = serde_json::from_str(line).expect("legacy event deserializes");
        match event {
            Event::IncidentRaised { kind, redrive, .. } => {
                assert_eq!(kind, IncidentKind::IoMapping);
                assert!(redrive.is_none());
            }
            other => panic!("expected IncidentRaised, got {other:?}"),
        }
    }
}

/// The ad-hoc call-activity redrive payloads (#1176) are **persisted** inside an
/// `IncidentRaised` journal line / snapshot, not just resolved in-memory — an
/// active ioMapping incident sits on the journal until the operator resolves it,
/// and migration-by-replay (#1071) rebuilds the engine by replaying that journal
/// under the current binary. So the single-pass input/output projections they
/// carry (`AdHocCallActivitySpawn.child_seed` /
/// `AdHocToolOutputCollection.precomputed_output`) must survive a serde
/// round-trip **verbatim**, or a recovered incident would respawn the tool with a
/// lost/garbled projection. These guard that replay-safety property directly.
#[cfg(all(test, feature = "serde"))]
mod adhoc_redrive_serde_tests {
    use std::collections::HashMap;

    use super::{IncidentKind, IoMappingRedrive, Value};
    use crate::event::Event;

    fn projection() -> HashMap<String, Value> {
        // A mix of scalar + nested-container values so a structural
        // serialization/recovery regression (not just a dropped key) is caught.
        HashMap::from([
            ("x".to_string(), Value::Int(1)),
            (
                "chained".to_string(),
                Value::List(vec![Value::Str("z".to_string()), Value::Bool(true)]),
            ),
        ])
    }

    fn raised_with(redrive: IoMappingRedrive) -> Event {
        Event::IncidentRaised {
            incident_key: 7,
            instance_key: 1,
            element_instance_key: 2,
            element_id: "tool".to_string(),
            kind: IncidentKind::IoMapping,
            redrive: Some(redrive),
            reason: "boom".to_string(),
            job_key: None,
            created_at: 42,
        }
    }

    /// An active `AdHocCallActivitySpawn` incident must replay with its input
    /// projection (`child_seed`) preserved verbatim, so a post-recovery respawn
    /// reuses it instead of re-evaluating chained input mappings.
    #[test]
    fn adhoc_call_activity_spawn_projection_survives_replay() {
        let seed = projection();
        let event = raised_with(IoMappingRedrive::AdHocCallActivitySpawn {
            child_seed: seed.clone(),
        });
        let line = serde_json::to_string(&event).expect("serializes");
        let back: Event = serde_json::from_str(&line).expect("replays");
        match back {
            Event::IncidentRaised {
                redrive: Some(IoMappingRedrive::AdHocCallActivitySpawn { child_seed }),
                ..
            } => assert_eq!(child_seed, seed),
            other => panic!("expected AdHocCallActivitySpawn redrive, got {other:?}"),
        }
    }

    /// An active `AdHocToolOutputCollection` incident must replay with its output
    /// projection (`precomputed_output`) preserved verbatim, so a post-recovery
    /// redrive reuses it instead of re-projecting chained output mappings.
    #[test]
    fn adhoc_tool_output_collection_projection_survives_replay() {
        let out = projection();
        let event = raised_with(IoMappingRedrive::AdHocToolOutputCollection {
            precomputed_output: out.clone(),
        });
        let line = serde_json::to_string(&event).expect("serializes");
        let back: Event = serde_json::from_str(&line).expect("replays");
        match back {
            Event::IncidentRaised {
                redrive: Some(IoMappingRedrive::AdHocToolOutputCollection { precomputed_output }),
                ..
            } => assert_eq!(precomputed_output, out),
            other => panic!("expected AdHocToolOutputCollection redrive, got {other:?}"),
        }
    }
}
