//! The engine: a single-writer command/event/applier loop.
//!
//! [`Engine::apply_command`] is the one entry point that changes anything. It:
//!
//! 1. validates the [`Command`] and emits the top-level event(s),
//! 2. drives an internal work queue of [`Step`]s — the BPMN element lifecycle —
//!    until the instance is quiescent (finished, or resting on a job/incident),
//! 3. detects process-instance completion,
//!
//! applying every event through [`state::apply`] as it goes. The processor
//! ([`Engine::process_step`]) only *reads* state and *decides*; it never mutates.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::command::Command;
use crate::event::Event;
use crate::model::{ElementId, ElementKind, ProcessDefinition, SequenceFlow, Value};
use crate::state::{self, Key, ProcessInstanceState, State};

mod api;
mod boundary;
mod memory;
mod resolve;

/// A compact, serializable capture of an [`Engine`]: its materialized [`State`]
/// plus the scalar generator and clock metadata required to resume operation
/// identically. Produced by [`Engine::snapshot`] and consumed by
/// [`Engine::from_snapshot`]; the body of a bounded Raft state-machine snapshot.
#[cfg(feature = "serde")]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EngineSnapshot {
    pub state: State,
    pub partition_id: u64,
    pub next_local: u64,
    pub num_partitions: u64,
    pub now: u64,
    pub start_dispatch_rr: u64,
}

/// Synthetic worker name stamped on a job whose activation lease was recovered
/// from a soft lease digest by a newly-promoted leader (see
/// [`Engine::recover_lease`]). It marks the lease as "restored from a digest, not
/// held by a live worker connection".
pub const LEASE_DIGEST_WORKER: &str = "__lease_digest__";

/// An embeddable BPMN engine instance.
///
/// Holds all state in memory. It is `Send` and contains no threads, locks or I/O,
/// so it can be owned by a single actor/task on a server, wrapped behind an FFI
/// boundary on mobile, or compiled to wasm.
#[derive(Debug, Default)]
pub struct Engine {
    state: State,
    /// Id of the partition this engine instance owns. Mints keys in its own
    /// namespace (see [`crate::partition_of`]); `0` for a single-partition host,
    /// which yields the historical `1, 2, 3, …` key sequence.
    partition_id: u64,
    /// Per-partition monotonic local counter. The next minted key is
    /// `compose_key(partition_id, next_local + 1)`. The single-writer loop makes
    /// plain increments deterministic.
    next_local: u64,
    /// Total number of partitions in the cluster (`1` for a single-partition
    /// host). Used to place message subscriptions on the partition owning
    /// `hash(correlation_key)` (see [`crate::subscription_partition`]); with a
    /// value of `1` every subscription is local, so behaviour is unchanged.
    num_partitions: u64,
    /// The clock reading for the command currently being processed, in the units
    /// the host supplies (Unix epoch milliseconds on the server). Set at the top
    /// of [`Engine::apply_command_at`] and read where the engine stamps a
    /// timestamp onto an event (e.g. when an incident is raised). The engine
    /// never reads a wall clock itself; replay is unaffected because the
    /// timestamp is carried on the event.
    now: u64,
    /// Round-robin cursor for spreading start-triggered (message-/timer-start)
    /// instances across the cluster's partitions. Not journaled — at RF=1 it
    /// only balances load (the chosen target is baked onto the emitted
    /// `StartInstanceDispatched` event, so replay is unaffected); a restart
    /// simply resumes the rotation from zero.
    start_dispatch_rr: u64,
    /// When `true`, a `CompleteJob` / `FailJob` / `ThrowJobError` is accepted for a
    /// job that has never been activated (the `activated` latch is not required).
    /// This supports a clustered mode where the activation lock is *leader-local*
    /// (not replicated through Raft): a follower's engine never observes the
    /// `JobActivated` event, so a replicated completion would otherwise fail the
    /// `JobNotActivated` check and diverge from the leader. Possession of the job
    /// key *is* the capability (keys are only handed out by activation), so this
    /// stays within the at-least-once contract. Runtime config (set identically on
    /// every replica from `NANOBPMN_REPLICATE_ACTIVATION`); NOT part of the
    /// snapshot, so it never affects replay/snapshot determinism. Defaults to
    /// `false` — the strict single-node/RF=1 behaviour is unchanged.
    lenient_completion: bool,
}

/// A unit of internal work in the processing loop — one transition of the BPMN
/// element lifecycle.
enum Step {
    /// Run an element through `ACTIVATING -> ACTIVATED` (and decide what comes
    /// next based on its kind).
    Activate {
        instance_key: Key,
        element_id: String,
        /// The enclosing sub-process element instance this activation runs in,
        /// or `0` for the process-level (root) scope.
        scope: Key,
    },
    /// Run an already-activated element through `COMPLETING -> COMPLETED` and take
    /// its outgoing sequence flows.
    Complete {
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    },
    /// Create a fresh job for an already-active service-task element instance.
    /// Used to retry a parked service task when its incident is resolved (the
    /// element instance stays active throughout; only a new job is minted).
    CreateJob {
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    },
}

impl Engine {
    /// Creates an empty engine for the single-partition (id `0`) namespace.
    pub fn new() -> Self {
        Self::with_partition(0)
    }

    /// Creates an empty engine that mints keys in partition `partition_id`'s
    /// namespace. Every key it produces carries `partition_id` in its high bits
    /// (see [`crate::compose_key`]), so keys are globally unique across a set of
    /// partitions and route back to their owner via [`crate::partition_of`].
    ///
    /// Panics if `partition_id` exceeds [`crate::MAX_PARTITION_ID`].
    pub fn with_partition(partition_id: u64) -> Self {
        assert!(
            partition_id <= state::MAX_PARTITION_ID,
            "partition id {partition_id} exceeds MAX_PARTITION_ID {}",
            state::MAX_PARTITION_ID
        );
        Self {
            state: State::new(),
            partition_id,
            next_local: 0,
            num_partitions: 1,
            now: 0,
            start_dispatch_rr: 0,
            lenient_completion: false,
        }
    }

    /// Read-only access to the full engine state (useful for queries and tests).
    pub fn state(&self) -> &State {
        &self.state
    }

    /// Sets the cluster-wide partition count used to place message
    /// subscriptions (see [`crate::subscription_partition`]). Defaults to `1`
    /// (single-partition, every subscription local). A clustered host calls
    /// this with `NANOBPMN_PARTITIONS` right after construction/replay so the
    /// engine routes catch-event subscriptions to the partition owning their
    /// correlation key. Must be identical on every node and stable for the life
    /// of the cluster.
    pub fn set_num_partitions(&mut self, num_partitions: u64) {
        self.num_partitions = num_partitions.max(1);
    }

    /// Enables (or disables) lenient completion: when `true`, `CompleteJob` /
    /// `FailJob` / `ThrowJobError` no longer require the job to have been activated
    /// first. A clustered host sets this from `NANOBPMN_REPLICATE_ACTIVATION=0` so
    /// the activation lock can stay leader-local (un-replicated) while replicated
    /// completions still apply cleanly on followers that never saw the activation.
    /// See the `lenient_completion` field. Must be set identically on every replica.
    pub fn set_lenient_completion(&mut self, lenient: bool) {
        self.lenient_completion = lenient;
    }

    /// Whether lenient completion is enabled (see [`Self::set_lenient_completion`]).
    pub fn lenient_completion(&self) -> bool {
        self.lenient_completion
    }

    /// The cluster-wide partition count this engine is configured with.
    pub fn num_partitions(&self) -> u64 {
        self.num_partitions
    }

    /// The partition that owns the message subscription for `correlation_key`.
    /// Always this engine's own partition when single-partition.
    pub fn subscription_partition(&self, correlation_key: &str) -> u64 {
        state::subscription_partition(correlation_key, self.num_partitions)
    }

    /// Rebuilds an engine by replaying a recorded event log.
    ///
    /// State is reconstructed through the applier ([`state::apply`]) — the same
    /// path used at runtime — and the key generator is advanced past every key
    /// the log assigned, so commands applied after recovery never mint a key
    /// that collides with a replayed one. The clock resets to `0`; timestamps
    /// are carried on the events themselves, so history (e.g. incident
    /// `created_at`) is preserved, and the next command sets the clock again.
    ///
    /// This is the durability primitive: a host persists the events returned by
    /// [`Engine::apply_command_at`] and, on restart, feeds them back here.
    pub fn replay<I>(events: I) -> Self
    where
        I: IntoIterator<Item = Event>,
    {
        Self::replay_partition(0, events)
    }

    /// Like [`Engine::replay`] but reconstructs partition `partition_id`. The key
    /// generator is advanced past every replayed key **that belongs to this
    /// partition** (`partition_of(key) == partition_id`); keys minted by other
    /// partitions (e.g. a deployment replicated in from partition 0) are applied
    /// to state but never advance this partition's local counter, so it keeps
    /// minting in its own namespace without colliding.
    pub fn replay_partition<I>(partition_id: u64, events: I) -> Self
    where
        I: IntoIterator<Item = Event>,
    {
        assert!(
            partition_id <= state::MAX_PARTITION_ID,
            "partition id {partition_id} exceeds MAX_PARTITION_ID {}",
            state::MAX_PARTITION_ID
        );
        let mut state = State::new();
        let mut next_local: u64 = 0;
        for event in events {
            let max_key = event.max_key();
            if state::partition_of(max_key) == partition_id {
                next_local = next_local.max(state::local_of(max_key));
            }
            state::apply(&mut state, &event);
        }
        Self {
            state,
            partition_id,
            next_local,
            num_partitions: 1,
            now: 0,
            start_dispatch_rr: 0,
            lenient_completion: false,
        }
    }

    fn mint_key(&mut self) -> Key {
        self.next_local += 1;
        debug_assert!(
            self.next_local <= state::LOCAL_MASK,
            "partition {} exhausted its 51-bit local key space",
            self.partition_id
        );
        state::compose_key(self.partition_id, self.next_local)
    }

    /// Installs an already-minted deployment (a slice of [`Event`]s produced by
    /// another partition's [`Command::Deploy`]) into this partition's state
    /// **without minting new keys**, so every partition registers the identical
    /// process definition under the identical `processDefinitionKey`.
    ///
    /// Only [`Event::ProcessDeployed`] events are applied: message-start
    /// subscriptions and timer-start arming are intentionally skipped so those
    /// start events remain owned by the single deployment partition (otherwise a
    /// timer-start would fire once per partition). Used by a multi-partition host
    /// to replicate partition 0's deployments to the others.
    pub fn install_deployment(&mut self, events: &[Event]) {
        for event in events {
            // Advance the local key generator past any installed key that belongs
            // to THIS partition, exactly as `replay_partition` does. This matters
            // when a partition installs a deployment it also minted (a Raft replica
            // actor for the deploy-owning partition): without it the replica's
            // instance-key counter lags the leader's by the number of keys the
            // deploy minted, so a replicated `CreateInstance` mints a divergent key
            // and the replica forks. Keys minted by OTHER partitions never advance
            // this counter (partition guard), so the common in-memory fan-out to
            // non-owning partitions is unchanged.
            let max_key = event.max_key();
            if state::partition_of(max_key) == self.partition_id {
                self.next_local = self.next_local.max(state::local_of(max_key));
            }
            if matches!(event, Event::ProcessDeployed { .. }) {
                state::apply(&mut self.state, event);
            }
        }
    }

    /// Creates a fresh process instance and queues its start event for
    /// activation. Shared by `CreateInstance`, message-start correlation, and
    /// timer-start firing.
    fn start_instance(
        &mut self,
        log: &mut Vec<Event>,
        queue: &mut VecDeque<Step>,
        process_id: String,
        start_event: ElementId,
        variables: HashMap<String, Value>,
        tags: Vec<String>,
        business_id: Option<String>,
    ) -> Key {
        let instance_key = self.mint_key();
        self.emit(
            log,
            Event::ProcessInstanceCreated {
                instance_key,
                process_id,
                variables,
                created_at: self.now,
                tags,
                business_id,
            },
        );
        queue.push_back(Step::Activate {
            instance_key,
            element_id: start_event,
            scope: 0,
        });
        instance_key
    }

    /// Places a start-triggered (message-start / timer-start) instance. On a
    /// single-partition host it creates it locally — byte-identical to the
    /// historical path. In a multi-partition cluster it round-robins a target
    /// partition: a local target creates inline; a remote target emits a routable
    /// [`Event::StartInstanceDispatched`] carrying the full creation payload, and
    /// the host routes a `DispatchStartInstance` to that partition (which mints
    /// the instance in its own namespace). This spreads start-triggered load
    /// across the cluster instead of piling every such instance onto the deploy
    /// partition.
    fn start_or_dispatch_instance(
        &mut self,
        log: &mut Vec<Event>,
        queue: &mut VecDeque<Step>,
        process_id: String,
        start_event: ElementId,
        variables: HashMap<String, Value>,
        tags: Vec<String>,
        business_id: Option<String>,
    ) {
        if self.num_partitions <= 1 {
            self.start_instance(log, queue, process_id, start_event, variables, tags, business_id);
            return;
        }
        let target = self.start_dispatch_rr % self.num_partitions;
        self.start_dispatch_rr = self.start_dispatch_rr.wrapping_add(1);
        if target == self.partition_id {
            self.start_instance(log, queue, process_id, start_event, variables, tags, business_id);
        } else {
            self.emit(
                log,
                Event::StartInstanceDispatched {
                    process_id,
                    start_element_id: start_event,
                    variables,
                    tags,
                    business_id,
                    target_partition: target,
                },
            );
        }
    }

    /// Validates and registers a batch of process definitions as one deployment.
    ///
    /// All processes are validated first, so the deployment is atomic: if any is
    /// invalid, none are emitted. Each *new or changed* process is assigned a
    /// unique process-definition key and a per-id version (latest known + 1).
    ///
    /// Deployment is idempotent: a process that is byte-for-byte identical to the
    /// current latest version of the same id (`existing.definition == process`,
    /// which includes the verbatim BPMN [`ProcessDefinition::xml`]) reuses that
    /// version's identity and emits **nothing** — no new key, no version bump, no
    /// journaled event. This mirrors Zeebe, where redeploying an unchanged
    /// resource does not create a new version, and keeps repeated idempotent
    /// deploys (e.g. on every app startup) from growing the journal. Keys are
    /// minted only for emitted events so replay (which derives the key counter
    /// from the max key seen in the log) stays in lock-step with the live engine.
    fn deploy(
        &mut self,
        log: &mut Vec<Event>,
        processes: Vec<ProcessDefinition>,
    ) -> Result<(), EngineError> {
        for process in &processes {
            if !process.elements.contains_key(&process.start_event) {
                return Err(EngineError::NoStartEvent {
                    process_id: process.id.clone(),
                });
            }
        }

        // Mint the shared deployment key lazily, only once we know at least one
        // process actually changed — a deploy of nothing but duplicates emits no
        // events and therefore must mint no keys.
        let mut deployment_key: Option<Key> = None;
        for process in processes {
            if self
                .state
                .processes
                .get(&process.id)
                .is_some_and(|existing| existing.definition == process)
            {
                // Idempotent redeploy of the latest version: reuse its identity
                // and skip it entirely (no event, no new subscription/timer).
                continue;
            }
            let deployment_key = *deployment_key.get_or_insert_with(|| self.mint_key());
            let version = self.next_version(&process.id);
            let process_definition_key = self.mint_key();
            let process_id = process.id.clone();
            let start_element_id = process.start_event.clone();
            let start_kind = process
                .elements
                .get(&process.start_event)
                .map(|e| e.kind.clone());
            self.emit(
                log,
                Event::ProcessDeployed {
                    deployment_key,
                    process_definition_key,
                    version,
                    process,
                },
            );
            match start_kind {
                Some(ElementKind::MessageStartEvent { message_name }) => {
                    self.emit(
                        log,
                        Event::MessageStartSubscriptionCreated {
                            process_definition_key,
                            process_id,
                            message_name,
                            start_element_id,
                        },
                    );
                }
                Some(ElementKind::TimerStartEvent {
                    interval_millis,
                    repeating,
                }) => {
                    let timer_key = self.mint_key();
                    let due_at = self.now.saturating_add(interval_millis);
                    self.emit(
                        log,
                        Event::ProcessStartTimerArmed {
                            timer_key,
                            process_definition_key,
                            process_id,
                            start_element_id,
                            due_at,
                            interval_millis,
                            repeating,
                        },
                    );
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// The version that the next deployment of `process_id` will receive.
    fn next_version(&self, process_id: &str) -> i32 {
        self.state
            .processes
            .get(process_id)
            .map(|p| p.version + 1)
            .unwrap_or(1)
    }

    /// Applies a command using the engine's current clock reading (see
    /// [`Engine::apply_command_at`]). Tests and hosts that do not need accurate
    /// timestamps can use this; the default clock is `0`.
    pub fn apply_command(&mut self, command: Command) -> Result<Vec<Event>, EngineError> {
        self.apply_command_at(command, self.now)
    }

    /// Applies a command, returning the ordered list of events it produced,
    /// stamping any timestamped events (e.g. raised incidents) with `now`.
    ///
    /// This is the engine's single writer: it runs to quiescence before
    /// returning, so on success the returned events are the complete record of
    /// everything that happened. `now` is the host's clock reading for this
    /// command; the engine never reads a wall clock itself.
    pub fn apply_command_at(
        &mut self,
        command: Command,
        now: u64,
    ) -> Result<Vec<Event>, EngineError> {
        self.now = now;
        let mut log: Vec<Event> = Vec::new();
        let mut queue: VecDeque<Step> = VecDeque::new();

        match command {
            Command::DeployProcess(process) => {
                self.deploy(&mut log, vec![process])?;
            }

            Command::DeployResources(processes) => {
                self.deploy(&mut log, processes)?;
            }

            Command::CreateInstance {
                process_id,
                variables,
                tags,
                business_id,
            } => {
                let process = self.state.processes.get(&process_id).ok_or_else(|| {
                    EngineError::ProcessNotFound {
                        process_id: process_id.clone(),
                    }
                })?;
                let start_event = process.definition.start_event.clone();
                self.start_instance(&mut log, &mut queue, process_id, start_event, variables, tags, business_id);
            }

            Command::CompleteJob { job_key, variables } => {
                let job = self
                    .state
                    .jobs
                    .get(&job_key)
                    .ok_or(EngineError::JobNotFound { job_key })?;
                if matches!(
                    job.state,
                    state::JobState::Completed
                        | state::JobState::Failed
                        | state::JobState::Errored
                        | state::JobState::Canceled
                ) {
                    return Err(EngineError::JobNotActive { job_key });
                }
                // Completion is by key alone, but a job must have been activated
                // at least once first. The current lock holder is irrelevant:
                // any worker that holds the key may complete it, even after the
                // lock expired and another worker re-activated it. Under lenient
                // completion (leader-local locks; see `lenient_completion`) the
                // activation may not have been replicated to this engine, so the
                // latch is not required.
                if !self.lenient_completion && !job.activated {
                    return Err(EngineError::JobNotActivated { job_key });
                }
                let instance_key = job.instance_key;
                let element_instance_key = job.element_instance_key;
                let element_id = job.element_id.clone();

                self.emit(
                    &mut log,
                    Event::JobCompleted {
                        job_key,
                        instance_key,
                    },
                );
                if !variables.is_empty() {
                    self.emit(
                        &mut log,
                        Event::VariablesUpdated {
                            instance_key,
                            variables,
                        },
                    );
                }
                // The parked service-task token resumes from ACTIVATED.
                queue.push_back(Step::Complete {
                    instance_key,
                    element_instance_key,
                    element_id,
                });
            }

            Command::AssignUserTask {
                user_task_key,
                assignee,
                allow_override,
            } => {
                let task = self
                    .state
                    .user_tasks
                    .get(&user_task_key)
                    .ok_or(EngineError::UserTaskNotFound { user_task_key })?;
                if task.state != state::UserTaskState::Created {
                    return Err(EngineError::UserTaskNotActive { user_task_key });
                }
                // Mirror Camunda: when override is disallowed and the task is
                // already assigned, reject so it must be unassigned first.
                if !allow_override && task.assignee.is_some() {
                    return Err(EngineError::UserTaskAlreadyAssigned { user_task_key });
                }
                let instance_key = task.instance_key;
                self.emit(
                    &mut log,
                    Event::UserTaskAssigned {
                        user_task_key,
                        instance_key,
                        assignee: Some(assignee),
                    },
                );
            }

            Command::UnassignUserTask { user_task_key } => {
                let task = self
                    .state
                    .user_tasks
                    .get(&user_task_key)
                    .ok_or(EngineError::UserTaskNotFound { user_task_key })?;
                if task.state != state::UserTaskState::Created {
                    return Err(EngineError::UserTaskNotActive { user_task_key });
                }
                let instance_key = task.instance_key;
                self.emit(
                    &mut log,
                    Event::UserTaskAssigned {
                        user_task_key,
                        instance_key,
                        assignee: None,
                    },
                );
            }

            Command::UpdateUserTask {
                user_task_key,
                changeset,
            } => {
                let task = self
                    .state
                    .user_tasks
                    .get(&user_task_key)
                    .ok_or(EngineError::UserTaskNotFound { user_task_key })?;
                if task.state != state::UserTaskState::Created {
                    return Err(EngineError::UserTaskNotActive { user_task_key });
                }
                let instance_key = task.instance_key;
                // Normalise empty-string dates to "reset" (None), matching the
                // REST contract ("Reset by providing an empty String").
                let normalize = |d: Option<String>| -> Option<String> {
                    d.filter(|s| !s.is_empty())
                };
                self.emit(
                    &mut log,
                    Event::UserTaskUpdated {
                        user_task_key,
                        instance_key,
                        candidate_groups: changeset.candidate_groups,
                        candidate_users: changeset.candidate_users,
                        due_date: changeset.due_date.map(normalize),
                        follow_up_date: changeset.follow_up_date.map(normalize),
                        priority: changeset.priority,
                    },
                );
            }

            Command::CompleteUserTask {
                user_task_key,
                variables,
            } => {
                let task = self
                    .state
                    .user_tasks
                    .get(&user_task_key)
                    .ok_or(EngineError::UserTaskNotFound { user_task_key })?;
                if task.state != state::UserTaskState::Created {
                    return Err(EngineError::UserTaskNotActive { user_task_key });
                }
                let instance_key = task.instance_key;
                let element_instance_key = task.element_instance_key;
                let element_id = task.element_id.clone();

                self.emit(
                    &mut log,
                    Event::UserTaskCompleted {
                        user_task_key,
                        instance_key,
                    },
                );
                if !variables.is_empty() {
                    self.emit(
                        &mut log,
                        Event::VariablesUpdated {
                            instance_key,
                            variables,
                        },
                    );
                }
                // The parked user-task token resumes from ACTIVATED.
                queue.push_back(Step::Complete {
                    instance_key,
                    element_instance_key,
                    element_id,
                });
            }

            Command::ActivateJobs {
                job_type,
                worker,
                max_jobs,
                timeout,
                now,
            } => {
                let deadline = now.saturating_add(timeout);
                // Deterministic selection: walk the per-type activatable index in
                // its order — `(−priority, key)`, i.e. highest priority first then
                // oldest (lowest key) — and take the first `max_jobs`. The index
                // holds only `Created` jobs (an `Activated` job is removed when it
                // locks and re-added by `JobLockExpired` when its lock expires), so
                // the walk is O(`max_jobs`) even with a large in-flight backlog
                // rather than rescanning and skipping every locked job on each poll.
                // The `job_activatable` check is a defensive guard against any stale
                // key (none expected).
                let keys: Vec<Key> = match self.state.activatable_jobs.get(&job_type) {
                    Some(set) => set
                        .iter()
                        .map(|&(_, k)| k)
                        .filter(|k| {
                            self.state
                                .jobs
                                .get(k)
                                .is_some_and(|j| job_activatable(j, now))
                        })
                        .take(max_jobs)
                        .collect(),
                    None => Vec::new(),
                };
                for job_key in keys {
                    let instance_key = self.state.jobs[&job_key].instance_key;
                    self.emit(
                        &mut log,
                        Event::JobActivated {
                            job_key,
                            instance_key,
                            worker: worker.clone(),
                            deadline,
                        },
                    );
                }
            }

            Command::ExpireJobs { now } => {
                // Only Activated jobs can have an expired lock, and they are
                // indexed, so iterate that small set instead of every job.
                let mut expired: Vec<(Key, Key)> = self
                    .state
                    .activated_jobs
                    .iter()
                    .filter_map(|k| {
                        let j = self.state.jobs.get(k)?;
                        (j.state == state::JobState::Activated
                            && j.deadline.is_some_and(|d| d <= now))
                        .then_some((j.key, j.instance_key))
                    })
                    .collect();
                expired.sort_unstable();
                for (job_key, instance_key) in expired {
                    self.emit(
                        &mut log,
                        Event::JobLockExpired {
                            job_key,
                            instance_key,
                        },
                    );
                }
            }

            Command::TriggerTimers { now } => {
                // Fire every due timer (deterministic order by key). An
                // intermediate catch resumes its parked token; an interrupting
                // boundary timer interrupts its activity and routes out the
                // boundary event.
                let mut due: Vec<Key> = self
                    .state
                    .timers
                    .values()
                    .filter(|t| t.state == state::TimerState::Created && t.due_at <= now)
                    .map(|t| t.key)
                    .collect();
                due.sort_unstable();
                for timer_key in due {
                    // A boundary timer earlier in this batch may have interrupted
                    // an activity that disarmed this one; re-check it is still due.
                    let timer = match self.state.timers.get(&timer_key) {
                        Some(t) if t.state == state::TimerState::Created => t,
                        _ => continue,
                    };
                    let instance_key = timer.instance_key;
                    let element_instance_key = timer.element_instance_key;
                    let element_id = timer.element_id.clone();
                    let due_at = timer.due_at;
                    let kind = timer.kind.clone();

                    self.emit(
                        &mut log,
                        Event::TimerTriggered {
                            timer_key,
                            instance_key,
                            element_instance_key,
                            element_id: element_id.clone(),
                        },
                    );

                    match kind {
                        // Catch event: completing it resumes the token along its
                        // own outgoing flow.
                        state::TimerKind::IntermediateCatch => {
                            queue.push_back(Step::Complete {
                                instance_key,
                                element_instance_key,
                                element_id,
                            });
                        }
                        // Boundary timer: interrupt the attached activity (a
                        // service task or sub-process) and run the boundary
                        // event's outgoing flow. `element_instance_key`/
                        // `element_id` are the activity here.
                        state::TimerKind::InterruptingBoundary {
                            boundary_element_id,
                        } => {
                            let scope = self.scope_of(instance_key, element_instance_key);
                            self.interrupt_activity_via_boundary(
                                &mut log,
                                instance_key,
                                element_instance_key,
                                &element_id,
                            );
                            queue.push_back(Step::Activate {
                                instance_key,
                                element_id: boundary_element_id,
                                scope,
                            });
                        }
                        // Non-interrupting boundary timer: leave the activity (and
                        // its job) running and spawn a parallel token along the
                        // boundary event's outgoing flow, in the activity's scope.
                        // A repeating (cycle) timer re-arms for the next interval.
                        state::TimerKind::NonInterruptingBoundary {
                            boundary_element_id,
                        } => {
                            queue.push_back(Step::Activate {
                                instance_key,
                                element_id: boundary_element_id.clone(),
                                scope: self.scope_of(instance_key, element_instance_key),
                            });
                            if let Some(ElementKind::TimerBoundaryEvent {
                                duration_millis,
                                repeating: true,
                                ..
                            }) = self.element_kind(instance_key, &boundary_element_id)
                            {
                                let next_timer_key = self.mint_key();
                                self.emit(
                                    &mut log,
                                    Event::TimerCreated {
                                        timer_key: next_timer_key,
                                        instance_key,
                                        element_instance_key,
                                        element_id: element_id.clone(),
                                        due_at: due_at.saturating_add(duration_millis),
                                        kind: state::TimerKind::NonInterruptingBoundary {
                                            boundary_element_id,
                                        },
                                    },
                                );
                            }
                        }
                    }
                }

                // Process-level start timers: fire every due one (deterministic
                // by key), creating a new instance. A cycle re-arms for the next
                // interval; a one-shot is retained with no due time so it never
                // fires again.
                let mut due_starts: Vec<Key> = self
                    .state
                    .start_timers
                    .values()
                    .filter(|t| t.due_at.is_some_and(|d| d <= now))
                    .map(|t| t.timer_key)
                    .collect();
                due_starts.sort_unstable();
                for timer_key in due_starts {
                    let timer = match self.state.start_timers.get(&timer_key) {
                        Some(t) => t,
                        None => continue,
                    };
                    let process_id = timer.process_id.clone();
                    let start_element_id = timer.start_element_id.clone();
                    let due_at = match timer.due_at {
                        Some(d) => d,
                        None => continue,
                    };
                    let next_due_at = if timer.repeating {
                        Some(due_at.saturating_add(timer.interval_millis))
                    } else {
                        None
                    };
                    self.emit(
                        &mut log,
                        Event::ProcessStartTimerFired {
                            timer_key,
                            next_due_at,
                        },
                    );
                    self.start_or_dispatch_instance(
                        &mut log,
                        &mut queue,
                        process_id,
                        start_element_id,
                        HashMap::new(),
                        Vec::new(),
                        None,
                    );
                }
            }

            Command::FailJob {
                job_key,
                retries,
                error_message,
            } => {
                let job = self
                    .state
                    .jobs
                    .get(&job_key)
                    .ok_or(EngineError::JobNotFound { job_key })?;
                if matches!(
                    job.state,
                    state::JobState::Completed
                        | state::JobState::Failed
                        | state::JobState::Errored
                        | state::JobState::Canceled
                ) {
                    return Err(EngineError::JobNotActive { job_key });
                }
                // Like completion, failing a job requires that it was activated
                // (unless lenient completion allows leader-local activation).
                if !self.lenient_completion && !job.activated {
                    return Err(EngineError::JobNotActivated { job_key });
                }
                let instance_key = job.instance_key;
                let element_instance_key = job.element_instance_key;
                let element_id = job.element_id.clone();
                let retries = retries.max(0);

                self.emit(
                    &mut log,
                    Event::JobFailed {
                        job_key,
                        instance_key,
                        retries,
                    },
                );
                // No retries left: park the job and raise an incident so the
                // instance stops making progress on this token.
                if retries == 0 {
                    let incident_key = self.mint_key();
                    self.emit(
                        &mut log,
                        Event::IncidentRaised {
                            incident_key,
                            instance_key,
                            element_instance_key,
                            element_id,
                            kind: state::IncidentKind::JobNoRetries,
                            reason: error_message,
                            job_key: Some(job_key),
                            created_at: self.now,
                        },
                    );
                }
            }

            Command::ThrowJobError {
                job_key,
                error_code,
                error_message,
            } => {
                let job = self
                    .state
                    .jobs
                    .get(&job_key)
                    .ok_or(EngineError::JobNotFound { job_key })?;
                if matches!(
                    job.state,
                    state::JobState::Completed
                        | state::JobState::Failed
                        | state::JobState::Errored
                        | state::JobState::Canceled
                ) {
                    return Err(EngineError::JobNotActive { job_key });
                }
                if !self.lenient_completion && !job.activated {
                    return Err(EngineError::JobNotActivated { job_key });
                }
                let instance_key = job.instance_key;
                let element_instance_key = job.element_instance_key;
                let task_element_id = job.element_id.clone();

                // The job is consumed by the thrown error either way.
                self.emit(
                    &mut log,
                    Event::JobErrorThrown {
                        job_key,
                        instance_key,
                        error_code: error_code.clone(),
                    },
                );

                match self.find_catching_error_boundary(
                    instance_key,
                    &task_element_id,
                    element_instance_key,
                    &error_code,
                ) {
                    // Caught: interrupt the catching activity (the throwing task
                    // itself, or an enclosing sub-process) and run the boundary
                    // event's error-handling path.
                    Some((boundary_id, caught_eik, caught_element_id)) => {
                        // Capture the catching activity's scope before completing
                        // it (completion clears its scope entry); the boundary
                        // event runs in that same scope.
                        let boundary_scope = self.scope_of(instance_key, caught_eik);
                        // When caught at an enclosing sub-process, terminate its
                        // whole inner scope (including the throwing task) first.
                        if caught_eik != element_instance_key {
                            self.terminate_subprocess_scope(&mut log, instance_key, caught_eik);
                        }
                        self.emit(
                            &mut log,
                            Event::ElementCompleting {
                                instance_key,
                                element_instance_key: caught_eik,
                                element_id: caught_element_id.clone(),
                            },
                        );
                        self.emit(
                            &mut log,
                            Event::ElementCompleted {
                                instance_key,
                                element_instance_key: caught_eik,
                                element_id: caught_element_id,
                            },
                        );
                        // An error boundary interrupting the activity also disarms
                        // any timer boundaries and message subscriptions on it.
                        for event in self.cancel_boundary_timers_on(caught_eik) {
                            self.emit(&mut log, event);
                        }
                        for event in self.cancel_boundary_message_subscriptions_on(caught_eik) {
                            self.emit(&mut log, event);
                        }
                        queue.push_back(Step::Activate {
                            instance_key,
                            element_id: boundary_id,
                            scope: boundary_scope,
                        });
                    }
                    // Unhandled: the token parks on an incident.
                    None => {
                        let reason = if error_message.is_empty() {
                            format!("unhandled BPMN error '{error_code}'")
                        } else {
                            format!("unhandled BPMN error '{error_code}': {error_message}")
                        };
                        let incident_key = self.mint_key();
                        self.emit(
                            &mut log,
                            Event::IncidentRaised {
                                incident_key,
                                instance_key,
                                element_instance_key,
                                element_id: task_element_id,
                                kind: state::IncidentKind::UnhandledError,
                                reason,
                                job_key: None,
                                created_at: self.now,
                            },
                        );
                    }
                }
            }

            Command::UpdateJobRetries { job_key, retries } => {
                let job = self
                    .state
                    .jobs
                    .get(&job_key)
                    .ok_or(EngineError::JobNotFound { job_key })?;
                // Retries are only meaningful while a job is still in play;
                // completed/errored jobs are terminal.
                if matches!(
                    job.state,
                    state::JobState::Completed | state::JobState::Errored | state::JobState::Canceled
                ) {
                    return Err(EngineError::JobNotActive { job_key });
                }
                let instance_key = job.instance_key;
                let retries = retries.max(0);
                self.emit(
                    &mut log,
                    Event::JobRetriesUpdated {
                        job_key,
                        instance_key,
                        retries,
                    },
                );
            }

            Command::ResolveIncident {
                incident_key,
                operation_reference,
            } => {
                let incident = self
                    .state
                    .incidents
                    .get(&incident_key)
                    .ok_or(EngineError::IncidentNotFound { incident_key })?;
                // Only an active incident can be resolved; a retained (resolved)
                // record is history, not a live parked token.
                if incident.state != state::IncidentState::Active {
                    return Err(EngineError::IncidentNotResolvable {
                        incident_key,
                        reason: "incident is already resolved".to_string(),
                    });
                }
                let instance_key = incident.instance_key;
                let element_instance_key = incident.element_instance_key;
                let element_id = incident.element_id.clone();
                let kind = incident.kind;
                let job_key = incident.job_key;
                // A job-incident can only be resolved once the parked job has
                // retries again; otherwise it would immediately re-fail.
                if let Some(job_key) = job_key {
                    let retries = self.state.jobs.get(&job_key).map_or(0, |j| j.retries);
                    if retries <= 0 {
                        return Err(EngineError::IncidentNotResolvable {
                            incident_key,
                            reason: format!(
                                "job {job_key} still has no retries; update its retries first"
                            ),
                        });
                    }
                }
                self.emit(
                    &mut log,
                    Event::IncidentResolved {
                        incident_key,
                        instance_key,
                        job_key,
                        resolved_at: self.now,
                        operation_reference,
                    },
                );
                // Resolution retries the failed work rather than merely clearing
                // the record. If the retry fails again a fresh incident is raised
                // by the same code paths that raised the original.
                match kind {
                    // Job exhausted its retries: the applier already returned the
                    // job to the activatable pool, so a worker retries it via the
                    // normal activate/complete path. Nothing more to enqueue.
                    state::IncidentKind::JobNoRetries => {}
                    // Exclusive gateway matched no flow, or a condition failed to
                    // evaluate: re-evaluate the gateway against the (possibly
                    // updated) variables.
                    state::IncidentKind::NoMatchingSequenceFlow
                    | state::IncidentKind::ExpressionEvaluation => {
                        queue.push_back(Step::Complete {
                            instance_key,
                            element_instance_key,
                            element_id,
                        });
                    }
                    // Uncaught business error: re-create a job for the still-active
                    // service task so a worker can attempt it again.
                    state::IncidentKind::UnhandledError => {
                        queue.push_back(Step::CreateJob {
                            instance_key,
                            element_instance_key,
                            element_id,
                        });
                    }
                }
            }

            Command::SetVariables {
                scope_key,
                variables,
            } => {
                let instance_key = self
                    .resolve_scope(scope_key)
                    .ok_or(EngineError::ScopeNotFound { scope_key })?;
                if !variables.is_empty() {
                    self.emit(
                        &mut log,
                        Event::VariablesUpdated {
                            instance_key,
                            variables,
                        },
                    );
                }
            }

            Command::CorrelateMessage {
                message_name,
                correlation_key,
                variables,
            } => {
                // Always mint a message key (Zeebe records every published
                // message); it is returned to the host and, carried on the
                // MessagePublished event, restores the key generator on replay.
                let message_key = self.mint_key();
                self.emit(
                    &mut log,
                    Event::MessagePublished {
                        message_key,
                        message_name: message_name.clone(),
                        correlation_key: correlation_key.clone(),
                    },
                );

                // Correlate to every matching open subscription, deterministic by
                // subscription key. Messages are not buffered: with no match the
                // message is simply dropped.
                let mut matched: Vec<Key> = self
                    .state
                    .message_subscriptions
                    .values()
                    .filter(|s| {
                        s.state == state::MessageSubscriptionState::Open
                            && s.message_name == message_name
                            && s.correlation_key == correlation_key
                    })
                    .map(|s| s.key)
                    .collect();
                matched.sort_unstable();

                for subscription_key in matched {
                    // A boundary correlation earlier in this batch may have
                    // interrupted an activity that cancelled this subscription;
                    // re-check it is still open.
                    let subscription = match self.state.message_subscriptions.get(&subscription_key)
                    {
                        Some(s) if s.state == state::MessageSubscriptionState::Open => s,
                        _ => continue,
                    };
                    let instance_key = subscription.instance_key;
                    let element_instance_key = subscription.element_instance_key;
                    let element_id = subscription.element_id.clone();
                    let kind = subscription.kind.clone();

                    if state::partition_of(instance_key) == self.partition_id {
                        // The instance lives on this partition: correlate and
                        // advance its token inline. This is the only path a
                        // single-partition host ever takes, so its log is
                        // byte-identical to the pre-placement engine.
                        self.advance_correlated_token(
                            &mut log,
                            &mut queue,
                            subscription_key,
                            message_key,
                            instance_key,
                            element_instance_key,
                            element_id,
                            kind,
                            &variables,
                        );
                    } else {
                        // The instance lives on another partition: settle the
                        // canonical subscription here and hand the token-advance to
                        // the host, which routes a `CorrelateMessageSubscription`
                        // continuation to `partition_of(instance_key)`.
                        self.emit(
                            &mut log,
                            Event::RemoteMessageCorrelation {
                                subscription_key,
                                message_key,
                                instance_key,
                                element_instance_key,
                                element_id,
                                kind,
                                variables: variables.clone(),
                            },
                        );
                    }
                }

                // Message start events: a matching message also creates a new
                // instance of every process subscribed at the process level.
                // Deterministic by process-definition key. The message's
                // variables seed the new instance.
                let mut started: Vec<(String, ElementId)> = self
                    .state
                    .message_start_subscriptions
                    .values()
                    .filter(|s| s.message_name == message_name)
                    .map(|s| (s.process_id.clone(), s.start_element_id.clone()))
                    .collect();
                started.sort_unstable();
                for (process_id, start_element_id) in started {
                    self.start_or_dispatch_instance(
                        &mut log,
                        &mut queue,
                        process_id,
                        start_element_id,
                        variables.clone(),
                        Vec::new(),
                        None,
                    );
                }
            }

            // --- Cross-partition message-subscription protocol -----------------
            //
            // These three commands are routed by the host between the instance
            // partition (where a token waits) and the message partition
            // (`hash(correlation_key)`, where the canonical subscription lives and
            // where published messages correlate). A single-partition host never
            // emits the events that drive them, so they only fire in a cluster.
            Command::OpenMessageSubscription {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id,
                message_name,
                correlation_key,
                kind,
            } => {
                // Idempotent: re-delivering an open for a subscription we already
                // hold (at-least-once retry, or a duplicate) is a no-op.
                if !self
                    .state
                    .message_subscriptions
                    .contains_key(&subscription_key)
                {
                    self.emit(
                        &mut log,
                        Event::MessageSubscriptionCreated {
                            subscription_key,
                            instance_key,
                            element_instance_key,
                            element_id,
                            message_name,
                            correlation_key,
                            kind,
                        },
                    );
                }
            }

            Command::CorrelateMessageSubscription {
                subscription_key,
                message_key,
                instance_key,
                element_instance_key,
                element_id,
                kind,
                variables,
            } => {
                // Advance only while the local record is still `Opening` (a parked
                // token). Once it settles to `Correlated`/`Canceled` — or the
                // instance is gone — a redelivered continuation is safely ignored,
                // which is what makes the routed delivery at-least-once safe. A
                // non-interrupting boundary keeps its `Opening` record open, so
                // every routed message spawns another token.
                let advance = matches!(
                    self.state
                        .message_subscriptions
                        .get(&subscription_key)
                        .map(|s| s.state),
                    Some(state::MessageSubscriptionState::Opening)
                );
                if advance {
                    self.advance_correlated_token(
                        &mut log,
                        &mut queue,
                        subscription_key,
                        message_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        kind,
                        &variables,
                    );
                }
            }

            Command::CloseMessageSubscription {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id,
            } => {
                // Disarm the canonical subscription because the instance partition
                // tore down the waiting element. Idempotent: only an open
                // subscription is cancelled.
                let open = matches!(
                    self.state
                        .message_subscriptions
                        .get(&subscription_key)
                        .map(|s| s.state),
                    Some(state::MessageSubscriptionState::Open)
                );
                if open {
                    self.emit(
                        &mut log,
                        Event::MessageSubscriptionCanceled {
                            subscription_key,
                            instance_key,
                            element_instance_key,
                            element_id,
                        },
                    );
                }
            }

            Command::CancelInstance { instance_key } => {
                // Only an active instance can be cancelled. An unknown key, or one
                // that has already completed/terminated, is rejected so the caller
                // gets a clean 404.
                match self.state.instances.get(&instance_key) {
                    Some(instance) if instance.state == ProcessInstanceState::Active => {}
                    _ => return Err(EngineError::InstanceNotFound { instance_key }),
                }

                // Discard every token: cancel the instance's in-play jobs, armed
                // timers and open message subscriptions. Collected and ordered by
                // key first so the event sequence is deterministic.
                let mut jobs: Vec<&state::Job> = self
                    .state
                    .jobs
                    .values()
                    .filter(|j| {
                        j.instance_key == instance_key
                            && matches!(
                                j.state,
                                state::JobState::Created
                                    | state::JobState::Activated
                                    | state::JobState::Failed
                            )
                    })
                    .collect();
                jobs.sort_unstable_by_key(|j| j.key);
                let job_cancels: Vec<Event> = jobs
                    .iter()
                    .map(|j| Event::JobCanceled {
                        job_key: j.key,
                        instance_key,
                    })
                    .collect();

                let mut timers: Vec<&state::Timer> = self
                    .state
                    .timers
                    .values()
                    .filter(|t| {
                        t.instance_key == instance_key && t.state == state::TimerState::Created
                    })
                    .collect();
                timers.sort_unstable_by_key(|t| t.key);
                let timer_cancels: Vec<Event> = timers
                    .iter()
                    .map(|t| Event::TimerCanceled {
                        timer_key: t.key,
                        instance_key,
                        element_instance_key: t.element_instance_key,
                        element_id: t.element_id.clone(),
                    })
                    .collect();

                let mut subs: Vec<&state::MessageSubscription> = self
                    .state
                    .message_subscriptions
                    .values()
                    .filter(|s| {
                        s.instance_key == instance_key
                            && matches!(
                                s.state,
                                state::MessageSubscriptionState::Open
                                    | state::MessageSubscriptionState::Opening
                            )
                    })
                    .collect();
                subs.sort_unstable_by_key(|s| s.key);
                let sub_cancels: Vec<Event> =
                    subs.iter().map(|s| Self::disarm_subscription_event(s)).collect();

                let mut user_tasks: Vec<&state::UserTask> = self
                    .state
                    .user_tasks
                    .values()
                    .filter(|t| {
                        t.instance_key == instance_key
                            && t.state == state::UserTaskState::Created
                    })
                    .collect();
                user_tasks.sort_unstable_by_key(|t| t.key);
                let user_task_cancels: Vec<Event> = user_tasks
                    .iter()
                    .map(|t| Event::UserTaskCanceled {
                        user_task_key: t.key,
                        instance_key,
                    })
                    .collect();

                for event in job_cancels {
                    self.emit(&mut log, event);
                }
                for event in timer_cancels {
                    self.emit(&mut log, event);
                }
                for event in sub_cancels {
                    self.emit(&mut log, event);
                }
                for event in user_task_cancels {
                    self.emit(&mut log, event);
                }
                self.emit(&mut log, Event::ProcessInstanceTerminated { instance_key });
            }

            Command::DispatchStartInstance {
                process_id,
                start_element_id,
                variables,
                tags,
                business_id,
            } => {
                // Routed from the deploy partition's StartInstanceDispatched: mint
                // the start-triggered instance here, in this partition's namespace,
                // so start-triggered load spreads across the cluster.
                self.start_instance(
                    &mut log,
                    &mut queue,
                    process_id,
                    start_element_id,
                    variables,
                    tags,
                    business_id,
                );
            }
        }

        self.run(&mut log, queue);
        self.complete_finished_instances(&mut log);
        Ok(log)
    }

    /// Drains the work queue, applying events and enqueuing follow-up steps until
    /// the instance is quiescent.
    fn run(&mut self, log: &mut Vec<Event>, mut queue: VecDeque<Step>) {
        loop {
            while let Some(step) = queue.pop_front() {
                let (events, followups) = self.process_step(step);
                for event in events {
                    self.emit(log, event);
                }
                for f in followups {
                    queue.push_back(f);
                }
            }
            // The queue is drained. Any embedded sub-process whose inner scope has
            // emptied completes now and routes along its outgoing flow; that may
            // enqueue more work (and, in turn, drain an enclosing sub-process), so
            // loop until nothing more completes.
            let followups = self.complete_drained_subprocesses(log);
            if followups.is_empty() {
                break;
            }
            queue.extend(followups);
        }
    }

    /// Records a correlation against a subscription whose **instance lives on this
    /// partition** and advances its parked token: emits [`Event::MessageCorrelated`]
    /// (settling the subscription unless it is a non-interrupting boundary), merges
    /// the message's `variables`, then enqueues the catch/boundary outcome. Shared
    /// by the inline local correlation in `CorrelateMessage` and the
    /// `CorrelateMessageSubscription` continuation routed back from a message
    /// partition, so both produce identical token-advance behaviour.
    #[allow(clippy::too_many_arguments)]
    fn advance_correlated_token(
        &mut self,
        log: &mut Vec<Event>,
        queue: &mut VecDeque<Step>,
        subscription_key: Key,
        message_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        kind: state::MessageSubscriptionKind,
        variables: &HashMap<String, Value>,
    ) {
        self.emit(
            log,
            Event::MessageCorrelated {
                subscription_key,
                message_key,
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
            },
        );
        // The message's variables (if any) are merged into the correlated
        // instance before its token advances.
        if !variables.is_empty() {
            self.emit(
                log,
                Event::VariablesUpdated {
                    instance_key,
                    variables: variables.clone(),
                },
            );
        }

        match kind {
            // Catch event: completing it resumes the token along its own outgoing
            // flow.
            state::MessageSubscriptionKind::IntermediateCatch => {
                queue.push_back(Step::Complete {
                    instance_key,
                    element_instance_key,
                    element_id,
                });
            }
            // Boundary subscription: interrupt the attached activity (a service
            // task or sub-process), then run the boundary event's outgoing flow.
            // `element_instance_key`/`element_id` are the activity here.
            state::MessageSubscriptionKind::InterruptingBoundary {
                boundary_element_id,
            } => {
                let scope = self.scope_of(instance_key, element_instance_key);
                self.interrupt_activity_via_boundary(
                    log,
                    instance_key,
                    element_instance_key,
                    &element_id,
                );
                queue.push_back(Step::Activate {
                    instance_key,
                    element_id: boundary_element_id,
                    scope,
                });
            }
            // Non-interrupting boundary subscription: leave the activity (and its
            // job) running and spawn a parallel token along the boundary event's
            // outgoing flow, in the activity's scope. The subscription stays open
            // (its applier does not settle it), so the next matching message spawns
            // another token.
            state::MessageSubscriptionKind::NonInterruptingBoundary {
                boundary_element_id,
            } => {
                queue.push_back(Step::Activate {
                    instance_key,
                    element_id: boundary_element_id,
                    scope: self.scope_of(instance_key, element_instance_key),
                });
            }
        }
    }

    /// Completes every active sub-process element instance whose inner token
    /// scope has drained (no remaining child element instances), emitting its
    /// completion events and returning the follow-up activations for its outgoing
    /// flows. Deterministic in `(instance_key, element_instance_key)` order.
    fn complete_drained_subprocesses(&mut self, log: &mut Vec<Event>) -> Vec<Step> {
        // Only an instance whose tokens moved this command can have a *newly*
        // drained sub-process scope (draining requires consuming a child token,
        // which emits an event), so restrict the sweep to the instances touched in
        // the log — exactly as `complete_finished_instances` does. Scanning every
        // active instance instead made this O(total active backlog) per command:
        // under a large undrained backlog that scan dominated the engine thread
        // (per-command cost grew linearly with the backlog).
        let touched: HashSet<Key> = log.iter().filter_map(|e| e.instance_key()).collect();
        let mut drained: Vec<(Key, Key, ElementId)> = Vec::new();
        for instance_key in &touched {
            let Some(instance) = self.state.instances.get(instance_key) else {
                continue;
            };
            if instance.state != ProcessInstanceState::Active {
                continue;
            }
            let Some(process) = self.state.processes.get(&instance.process_id) else {
                continue;
            };
            for (eik, element_id) in &instance.active {
                let is_subprocess = matches!(
                    process.definition.element(element_id).map(|e| &e.kind),
                    Some(ElementKind::SubProcess { .. })
                );
                if !is_subprocess {
                    continue;
                }
                let has_child = instance.scopes.values().any(|parent| parent == eik);
                if !has_child {
                    drained.push((instance.key, *eik, element_id.clone()));
                }
            }
        }
        drained.sort();

        let mut followups = Vec::new();
        for (instance_key, eik, element_id) in drained {
            // The sub-process completes in its own (parent) scope, captured before
            // its scope entry is cleared by `ElementCompleted`.
            let scope = self.scope_of(instance_key, eik);
            self.emit(
                log,
                Event::ElementCompleting {
                    instance_key,
                    element_instance_key: eik,
                    element_id: element_id.clone(),
                },
            );
            self.emit(
                log,
                Event::ElementCompleted {
                    instance_key,
                    element_instance_key: eik,
                    element_id: element_id.clone(),
                },
            );
            // Completing normally disarms any boundary timers/subscriptions on it.
            for event in self.cancel_boundary_timers_on(eik) {
                self.emit(log, event);
            }
            for event in self.cancel_boundary_message_subscriptions_on(eik) {
                self.emit(log, event);
            }
            for flow in self.outgoing(instance_key, &element_id) {
                self.emit(
                    log,
                    Event::SequenceFlowTaken {
                        instance_key,
                        from: element_id.clone(),
                        to: flow.to.clone(),
                    },
                );
                followups.push(Step::Activate {
                    instance_key,
                    element_id: flow.to,
                    scope,
                });
            }
        }
        followups
    }

    /// The processor: decides the events and follow-up work for one lifecycle
    /// step. Reads state, mints keys, but never mutates [`State`].
    fn process_step(&mut self, step: Step) -> (Vec<Event>, Vec<Step>) {
        match step {
            Step::Activate {
                instance_key,
                element_id,
                scope,
            } => self.activate(instance_key, element_id, scope),
            Step::Complete {
                instance_key,
                element_instance_key,
                element_id,
            } => self.complete(instance_key, element_instance_key, element_id),
            Step::CreateJob {
                instance_key,
                element_instance_key,
                element_id,
            } => self.create_job_for(instance_key, element_instance_key, element_id),
        }
    }

    fn activate(
        &mut self,
        instance_key: Key,
        element_id: String,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let kind = self.element_kind(instance_key, &element_id);

        // A parallel gateway with more than one incoming flow is a join: it
        // synchronises tokens instead of activating per arrival.
        if matches!(kind, Some(ElementKind::ParallelGateway))
            && self.incoming_count(instance_key, &element_id) > 1
        {
            return self.arrive_at_parallel_join(instance_key, element_id, scope);
        }

        let element_instance_key = self.mint_key();
        let mut events = vec![
            Event::ElementActivating {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
            },
            Event::ElementActivated {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
                scope,
            },
        ];
        let mut followups = Vec::new();

        match kind {
            // A service task creates a job and parks the token.
            Some(ElementKind::ServiceTask { job_type, priority }) => {
                let job_key = self.mint_key();
                let job_type = self.resolve_job_type(instance_key, &job_type);
                let priority = self.resolve_priority(instance_key, priority.as_deref());
                events.push(Event::JobCreated {
                    job_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                    job_type,
                    created_at: self.now,
                    priority,
                });
                // Arm timers/subscriptions for every attached boundary event.
                events.extend(self.arm_boundary_events(
                    instance_key,
                    element_instance_key,
                    &element_id,
                ));
            }
            // A user task creates a user task record and parks the token; a
            // CompleteUserTask releases it. The assignment/scheduling/priority
            // expressions declared on the element are resolved against the
            // instance variables at creation time.
            Some(ElementKind::UserTask(props)) => {
                let user_task_key = self.mint_key();
                let assignee = self
                    .resolve_user_task_string(instance_key, props.assignee.as_deref())
                    .filter(|s| !s.is_empty());
                let candidate_groups =
                    self.resolve_user_task_list(instance_key, props.candidate_groups.as_deref());
                let candidate_users =
                    self.resolve_user_task_list(instance_key, props.candidate_users.as_deref());
                let due_date = self
                    .resolve_user_task_string(instance_key, props.due_date.as_deref())
                    .filter(|s| !s.is_empty());
                let follow_up_date = self
                    .resolve_user_task_string(instance_key, props.follow_up_date.as_deref())
                    .filter(|s| !s.is_empty());
                let priority = self.resolve_priority(instance_key, props.priority.as_deref());
                events.push(Event::UserTaskCreated {
                    user_task_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                    created_at: self.now,
                    assignee,
                    candidate_groups,
                    candidate_users,
                    due_date,
                    follow_up_date,
                    priority,
                });
                events.extend(self.arm_boundary_events(
                    instance_key,
                    element_instance_key,
                    &element_id,
                ));
            }
            // A timer intermediate catch event arms a timer and parks the token;
            // a clock tick (TriggerTimers) releases it once the timer is due.
            Some(ElementKind::TimerIntermediateCatchEvent { duration_millis }) => {
                let timer_key = self.mint_key();
                events.push(Event::TimerCreated {
                    timer_key,
                    instance_key,
                    element_instance_key,
                    element_id,
                    due_at: self.now.saturating_add(duration_millis),
                    kind: state::TimerKind::IntermediateCatch,
                });
            }
            // A message intermediate catch event opens a subscription and parks
            // the token; a matching CorrelateMessage releases it.
            Some(ElementKind::MessageIntermediateCatchEvent {
                message_name,
                correlation_key,
            }) => {
                let subscription_key = self.mint_key();
                let correlation_value =
                    self.resolve_correlation_value(instance_key, &correlation_key);
                let kind = state::MessageSubscriptionKind::IntermediateCatch;
                // Zeebe-style placement: the canonical subscription lives on the
                // partition owning `hash(correlation_key)`. When that is this
                // partition (always so single-partition), open it locally; else
                // park the token on an `Opening` record and let the host route an
                // `OpenMessageSubscription` to the message partition.
                if self.subscription_partition(&correlation_value) == self.partition_id {
                    events.push(Event::MessageSubscriptionCreated {
                        subscription_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        message_name,
                        correlation_key: correlation_value,
                        kind,
                    });
                } else {
                    events.push(Event::MessageSubscriptionOpening {
                        subscription_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        message_name,
                        correlation_key: correlation_value,
                        kind,
                    });
                }
            }
            // An embedded sub-process opens a token scope: it activates its inner
            // start event inside its own scope (this element instance) and rests
            // while the inner flow runs. It completes once the scope drains (see
            // `complete_drained_subprocesses`) or is interrupted by an error
            // boundary.
            Some(ElementKind::SubProcess { start_event }) => {
                // A sub-process can also carry timer/message boundary events;
                // arm them just like a service task's.
                events.extend(self.arm_boundary_events(
                    instance_key,
                    element_instance_key,
                    &element_id,
                ));
                followups.push(Step::Activate {
                    instance_key,
                    element_id: start_event,
                    scope: element_instance_key,
                });
            }
            // Pass-through elements (events, exclusive gateway, parallel split)
            // complete immediately; routing happens at completion.
            Some(_) => {
                followups.push(Step::Complete {
                    instance_key,
                    element_instance_key,
                    element_id,
                });
            }
            // Unknown element / instance: nothing to do.
            None => {}
        }

        (events, followups)
    }

    fn complete(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    ) -> (Vec<Event>, Vec<Step>) {
        if matches!(
            self.element_kind(instance_key, &element_id),
            Some(ElementKind::ExclusiveGateway)
        ) {
            return self.complete_exclusive_gateway(instance_key, element_instance_key, element_id);
        }

        // Default behaviour: complete and take every outgoing flow (a single flow
        // for ordinary elements; all flows for a parallel split).
        let mut events = vec![
            Event::ElementCompleting {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
            },
            Event::ElementCompleted {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
            },
        ];
        // If this element was an activity guarded by interrupting boundary timers,
        // completing it normally disarms them (and any boundary message subs).
        events.extend(self.cancel_boundary_timers_on(element_instance_key));
        events.extend(self.cancel_boundary_message_subscriptions_on(element_instance_key));
        let mut followups = Vec::new();
        let scope = self.scope_of(instance_key, element_instance_key);
        for flow in self.outgoing(instance_key, &element_id) {
            events.push(Event::SequenceFlowTaken {
                instance_key,
                from: element_id.clone(),
                to: flow.to.clone(),
            });
            followups.push(Step::Activate {
                instance_key,
                element_id: flow.to,
                scope,
            });
        }
        (events, followups)
    }

    /// Mints a fresh job for an already-active service-task element instance.
    /// Used by incident resolution to retry a parked service task: the element
    /// instance is left untouched (it stays active) and a new job is created in
    /// the `Created` (activatable) state so a worker can attempt it again. A
    /// no-op if the element is not (or is no longer) a service task.
    fn create_job_for(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    ) -> (Vec<Event>, Vec<Step>) {
        match self.element_kind(instance_key, &element_id) {
            Some(ElementKind::ServiceTask { job_type, priority }) => {
                let job_key = self.mint_key();
                let job_type = self.resolve_job_type(instance_key, &job_type);
                let priority = self.resolve_priority(instance_key, priority.as_deref());
                (
                    vec![Event::JobCreated {
                        job_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        job_type,
                        created_at: self.now,
                        priority,
                    }],
                    Vec::new(),
                )
            }
            _ => (Vec::new(), Vec::new()),
        }
    }

    /// Exclusive gateway: take exactly one outgoing flow — the first whose
    /// condition holds (an unconditional flow is the default). If none qualifies,
    /// raise an incident and park the token.
    fn complete_exclusive_gateway(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    ) -> (Vec<Event>, Vec<Step>) {
        let variables = self.variables(instance_key);
        let mut selected = None;
        let mut eval_error: Option<String> = None;
        for flow in self.outgoing(instance_key, &element_id) {
            match &flow.condition {
                None => {
                    selected = Some(flow);
                    break;
                }
                Some(condition) => match condition.eval(&variables) {
                    Ok(true) => {
                        selected = Some(flow);
                        break;
                    }
                    Ok(false) => continue,
                    Err(err) => {
                        eval_error = Some(format!(
                            "failed to evaluate condition '{}' at exclusive gateway \
                             '{element_id}': {}",
                            condition.expression, err.0
                        ));
                        break;
                    }
                },
            }
        }

        if let Some(reason) = eval_error {
            let incident_key = self.mint_key();
            let events = vec![Event::IncidentRaised {
                incident_key,
                instance_key,
                element_instance_key,
                element_id,
                kind: state::IncidentKind::ExpressionEvaluation,
                reason,
                job_key: None,
                created_at: self.now,
            }];
            return (events, Vec::new());
        }

        match selected {
            Some(flow) => {
                let events = vec![
                    Event::ElementCompleting {
                        instance_key,
                        element_instance_key,
                        element_id: element_id.clone(),
                    },
                    Event::ElementCompleted {
                        instance_key,
                        element_instance_key,
                        element_id: element_id.clone(),
                    },
                    Event::SequenceFlowTaken {
                        instance_key,
                        from: element_id,
                        to: flow.to.clone(),
                    },
                ];
                let followups = vec![Step::Activate {
                    instance_key,
                    element_id: flow.to,
                    scope: self.scope_of(instance_key, element_instance_key),
                }];
                (events, followups)
            }
            None => {
                // The token stays active (parked on the incident) so the instance
                // does not falsely complete.
                let incident_key = self.mint_key();
                let events = vec![Event::IncidentRaised {
                    incident_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                    kind: state::IncidentKind::NoMatchingSequenceFlow,
                    reason: format!(
                        "no matching outgoing sequence flow at exclusive gateway '{element_id}'"
                    ),
                    job_key: None,
                    created_at: self.now,
                }];
                (events, Vec::new())
            }
        }
    }

    /// A token reached a parallel-gateway join. Open the join on the first
    /// arrival, count every arrival, and fire once a token has arrived on every
    /// incoming flow.
    fn arrive_at_parallel_join(
        &mut self,
        instance_key: Key,
        element_id: String,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let threshold = self.incoming_count(instance_key, &element_id);
        let already_open = self.join_eik(instance_key, &element_id).is_some();
        let count_before = self.join_count(instance_key, &element_id);

        let mut events = Vec::new();

        let open_eik = if already_open {
            self.join_eik(instance_key, &element_id).unwrap()
        } else {
            let eik = self.mint_key();
            events.push(Event::ElementActivating {
                instance_key,
                element_instance_key: eik,
                element_id: element_id.clone(),
            });
            events.push(Event::ElementActivated {
                instance_key,
                element_instance_key: eik,
                element_id: element_id.clone(),
                scope,
            });
            events.push(Event::ParallelJoinOpened {
                instance_key,
                element_instance_key: eik,
                element_id: element_id.clone(),
            });
            eik
        };

        events.push(Event::ParallelJoinTokenArrived {
            instance_key,
            element_id: element_id.clone(),
        });

        let mut followups = Vec::new();
        if count_before + 1 >= threshold {
            events.push(Event::ElementCompleting {
                instance_key,
                element_instance_key: open_eik,
                element_id: element_id.clone(),
            });
            events.push(Event::ElementCompleted {
                instance_key,
                element_instance_key: open_eik,
                element_id: element_id.clone(),
            });
            events.push(Event::ParallelJoinReset {
                instance_key,
                element_id: element_id.clone(),
            });
            for flow in self.outgoing(instance_key, &element_id) {
                events.push(Event::SequenceFlowTaken {
                    instance_key,
                    from: element_id.clone(),
                    to: flow.to.clone(),
                });
                followups.push(Step::Activate {
                    instance_key,
                    element_id: flow.to,
                    scope,
                });
            }
        }

        (events, followups)
    }

    /// After a command settles, any active instance with no remaining tokens has
    /// completed.
    fn complete_finished_instances(&mut self, log: &mut Vec<Event>) {
        let touched: HashSet<Key> = log.iter().filter_map(|e| e.instance_key()).collect();
        let finished: Vec<Key> = touched
            .into_iter()
            .filter(|k| {
                self.state
                    .instances
                    .get(k)
                    .map(|i| i.state == ProcessInstanceState::Active && i.active.is_empty())
                    .unwrap_or(false)
            })
            .collect();

        for instance_key in finished {
            self.emit(log, Event::ProcessInstanceCompleted { instance_key });
        }
    }

    /// Applies an event and records it in the command's event log.
    fn emit(&mut self, log: &mut Vec<Event>, event: Event) {
        state::apply(&mut self.state, &event);
        log.push(event);
    }

    fn process_of_instance(&self, instance_key: Key) -> Option<&crate::model::ProcessDefinition> {
        let instance = self.state.instances.get(&instance_key)?;
        self.state
            .processes
            .get(&instance.process_id)
            .map(|p| &p.definition)
    }

    fn element_kind(&self, instance_key: Key, element_id: &str) -> Option<ElementKind> {
        self.process_of_instance(instance_key)?
            .element(element_id)
            .map(|e| e.kind.clone())
    }
}

/// An entity owned by a single process instance, identified by `instance_key`.
/// Lets [`drain_owned`] lift every record an instance owns out of a hot-state map
/// in one generic pass during cold-spill snapshotting.
trait OwnedByInstance {
    fn instance_key(&self) -> Key;
}

impl OwnedByInstance for state::Timer {
    fn instance_key(&self) -> Key {
        self.instance_key
    }
}
impl OwnedByInstance for state::MessageSubscription {
    fn instance_key(&self) -> Key {
        self.instance_key
    }
}
impl OwnedByInstance for state::UserTask {
    fn instance_key(&self) -> Key {
        self.instance_key
    }
}
impl OwnedByInstance for state::Incident {
    fn instance_key(&self) -> Key {
        self.instance_key
    }
}

/// Removes from `map` every entity owned by `instance_key`, returning them. Used
/// to lift an instance's timers/subscriptions/user-tasks/incidents out of hot
/// state when snapshotting it for cold spill.
fn drain_owned<V: OwnedByInstance>(map: &mut HashMap<Key, V>, instance_key: Key) -> Vec<V> {
    let owned: Vec<Key> = map
        .iter()
        .filter(|(_, v)| v.instance_key() == instance_key)
        .map(|(k, _)| *k)
        .collect();
    owned
        .into_iter()
        .filter_map(|k| map.remove(&k))
        .collect()
}

/// Errors returned by [`Engine::apply_command`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EngineError {
    /// `CreateInstance` referenced a process id that was never deployed.
    ProcessNotFound { process_id: String },
    /// `DeployProcess` was given a definition whose `start_event` is not among
    /// its elements.
    NoStartEvent { process_id: String },
    /// `CompleteJob` referenced a job key that does not exist.
    JobNotFound { job_key: Key },
    /// `CompleteJob`/`FailJob` referenced a job that is not in a state where it
    /// can be acted on (already completed, or failed with an incident raised).
    JobNotActive { job_key: Key },
    /// `CompleteJob`/`FailJob` referenced a job that has never been activated. A
    /// job must be activated at least once before it can be completed or failed.
    JobNotActivated { job_key: Key },
    /// `ResolveIncident` referenced an incident key that does not exist.
    IncidentNotFound { incident_key: Key },
    /// `ResolveIncident` referenced a job-incident whose job still has no
    /// retries; the retries must be updated before it can be resolved.
    IncidentNotResolvable { incident_key: Key, reason: String },
    /// `SetVariables` referenced a scope key that is neither a process instance
    /// nor any active element instance.
    ScopeNotFound { scope_key: Key },
    /// `CancelInstance` referenced a process instance that does not exist or is
    /// no longer active (already completed or terminated).
    InstanceNotFound { instance_key: Key },
    /// `AssignUserTask`/`CompleteUserTask` referenced a user-task key that does
    /// not exist.
    UserTaskNotFound { user_task_key: Key },
    /// `AssignUserTask`/`CompleteUserTask` referenced a user task that is not in
    /// a state where it can be acted on (already completed or cancelled).
    UserTaskNotActive { user_task_key: Key },
    /// `AssignUserTask` with `allow_override = false` targeted a user task that
    /// already has an assignee; it must be unassigned first.
    UserTaskAlreadyAssigned { user_task_key: Key },
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::ProcessNotFound { process_id } => {
                write!(f, "no deployed process with id {process_id}")
            }
            EngineError::NoStartEvent { process_id } => {
                write!(
                    f,
                    "process {process_id} has no start event among its elements"
                )
            }
            EngineError::JobNotFound { job_key } => write!(f, "no job with key {job_key}"),
            EngineError::JobNotActive { job_key } => {
                write!(f, "job {job_key} is not in a state that can be acted on")
            }
            EngineError::JobNotActivated { job_key } => {
                write!(
                    f,
                    "job {job_key} must be activated before it can be completed or failed"
                )
            }
            EngineError::IncidentNotFound { incident_key } => {
                write!(f, "no incident with key {incident_key}")
            }
            EngineError::IncidentNotResolvable {
                incident_key,
                reason,
            } => {
                write!(f, "incident {incident_key} cannot be resolved: {reason}")
            }
            EngineError::ScopeNotFound { scope_key } => {
                write!(f, "no variable scope with key {scope_key}")
            }
            EngineError::InstanceNotFound { instance_key } => {
                write!(f, "no active process instance with key {instance_key}")
            }
            EngineError::UserTaskNotFound { user_task_key } => {
                write!(f, "no user task with key {user_task_key}")
            }
            EngineError::UserTaskNotActive { user_task_key } => {
                write!(
                    f,
                    "user task {user_task_key} is not in a state that can be acted on"
                )
            }
            EngineError::UserTaskAlreadyAssigned { user_task_key } => {
                write!(
                    f,
                    "user task {user_task_key} is already assigned; unassign it before assigning again"
                )
            }
        }
    }
}

impl std::error::Error for EngineError {}

/// A job handed to a worker by [`Engine::activate_jobs`]: everything the worker
/// needs to do the work and complete it by key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivatedJob {
    pub key: Key,
    pub job_type: String,
    pub instance_key: Key,
    pub element_instance_key: Key,
    pub element_id: String,
    /// The worker the job was locked to.
    pub worker: String,
    /// Logical instant at which the activation lock expires.
    pub deadline: u64,
    /// Remaining retries for this job.
    pub retries: i32,
    /// A snapshot of the instance's variables at activation time. Shared via
    /// `Arc` with the engine's instance state, so activation does not deep-clone
    /// the (up to 50 KB) value tree on the single command thread; the response
    /// mapper encodes it to JSON off-thread by borrowing.
    pub variables: Arc<HashMap<String, Value>>,
}

/// Whether a job can be activated at the logical instant `now`: it is created
/// (never activated, or its lock was released) or its current lock has expired.
/// Failed (incident-parked) and completed jobs are never activatable.
fn job_activatable(job: &state::Job, now: u64) -> bool {
    match job.state {
        state::JobState::Completed
        | state::JobState::Failed
        | state::JobState::Errored
        | state::JobState::Canceled => false,
        state::JobState::Created => true,
        state::JobState::Activated => job.deadline.is_some_and(|d| d <= now),
    }
}


#[cfg(test)]
mod tests;
