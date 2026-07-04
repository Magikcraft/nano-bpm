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
    /// When `true`, runtime event application ([`Engine::emit`]) records the
    /// instances whose variables changed into [`Engine::dirty_vars`] and terminal
    /// evictions into [`Engine::forgotten_vars`], so a host can checkpoint just
    /// the delta into an authoritative durable variable store and write a lean
    /// (control-only) snapshot. Off by default (the full-variable snapshot path is
    /// unchanged); set by the host from `NANOBPMN_LEAN_SNAPSHOT`. Not part of the
    /// snapshot — it is pure host-side bookkeeping and never affects determinism.
    track_dirty_vars: bool,
    /// Instances whose process-level variables changed since the last
    /// [`drain_dirty_vars`](Engine::drain_dirty_vars) (i.e. since the last
    /// snapshot checkpoint). Only populated when [`track_dirty_vars`] is set.
    dirty_vars: HashSet<Key>,
    /// Instances evicted (terminal) since the last checkpoint, whose durable
    /// variable rows must be deleted from the store. Only populated when
    /// [`track_dirty_vars`] is set.
    forgotten_vars: HashSet<Key>,
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
    /// Activate one child of a multi-instance body: instantiate `element_id`
    /// again in the body's scope with the `index`-th item's local bindings. The
    /// item and bindings are read from the body's runtime state.
    ActivateMiChild {
        instance_key: Key,
        element_id: String,
        body_key: Key,
        index: usize,
    },
    /// Complete a multi-instance body: aggregate the collected output, cancel any
    /// children still running (an early completion-condition fire), write the
    /// output collection, and take the activity's outgoing flow.
    CompleteMiBody { instance_key: Key, body_key: Key },
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
            track_dirty_vars: false,
            dirty_vars: HashSet::new(),
            forgotten_vars: HashSet::new(),
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
            track_dirty_vars: false,
            dirty_vars: HashSet::new(),
            forgotten_vars: HashSet::new(),
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

    /// Like [`install_deployment`](Self::install_deployment) but applies each
    /// `ProcessDeployed` **only when it is newer** than the definition already
    /// registered for its process id (absent, or a strictly lower version).
    ///
    /// Used by segmented multi-partition recovery to replay a partition-agnostic
    /// deployment (a durable replicated copy, keyed to the deployment partition)
    /// into a partition that may already hold that definition from its own
    /// snapshot at an equal or newer version. Because compaction and per-partition
    /// snapshots advance on independent watermarks, an *older* surviving durable
    /// copy could otherwise regress the definition; the version guard makes the
    /// replay idempotent and monotonic. Key generation is advanced exactly as in
    /// [`install_deployment`](Self::install_deployment).
    pub fn install_deployment_if_newer(&mut self, events: &[Event]) {
        for event in events {
            let max_key = event.max_key();
            if state::partition_of(max_key) == self.partition_id {
                self.next_local = self.next_local.max(state::local_of(max_key));
            }
            if let Event::ProcessDeployed {
                process, version, ..
            } = event
            {
                let newer = self
                    .state
                    .processes
                    .get(&process.id)
                    .map(|d| *version > d.version)
                    .unwrap_or(true);
                if newer {
                    state::apply(&mut self.state, event);
                }
            }
        }
    }

    /// Creates a fresh process instance and queues its start event for
    /// activation. Shared by `CreateInstance`, message-start correlation, and
    /// timer-start firing.
    #[allow(clippy::too_many_arguments)]
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
    #[allow(clippy::too_many_arguments)]
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
            self.start_instance(
                log,
                queue,
                process_id,
                start_event,
                variables,
                tags,
                business_id,
            );
            return;
        }
        let target = self.start_dispatch_rr % self.num_partitions;
        self.start_dispatch_rr = self.start_dispatch_rr.wrapping_add(1);
        if target == self.partition_id {
            self.start_instance(
                log,
                queue,
                process_id,
                start_event,
                variables,
                tags,
                business_id,
            );
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
            let start_timer_def = process
                .elements
                .get(&process.start_event)
                .and_then(|e| e.timer.clone());
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
                    // Per Zeebe, a start-event name expression is evaluated at
                    // deploy time against an empty context; a static name passes
                    // through unchanged.
                    let message_name = self.resolve_event_name(None, &message_name);
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
                    // Evaluate a FEEL start-timer expression against an empty
                    // context at deploy; a static literal falls back to the
                    // parsed interval. For a cycle the resolved interval is
                    // persisted so re-arming recurs on the same delay.
                    let (due_at, interval_millis) = self.resolve_timer(
                        None,
                        start_timer_def.as_ref(),
                        self.now,
                        interval_millis,
                    );
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
                self.start_instance(
                    &mut log,
                    &mut queue,
                    process_id,
                    start_event,
                    variables,
                    tags,
                    business_id,
                );
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
                let normalize =
                    |d: Option<String>| -> Option<String> { d.filter(|s| !s.is_empty()) };
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
                                // Re-evaluate a FEEL cycle against current vars so
                                // the next interval reflects any updated variable;
                                // a static cycle keeps its parsed interval.
                                let timer_def =
                                    self.timer_def_of(instance_key, &boundary_element_id);
                                let (next_due_at, _) = self.resolve_timer(
                                    Some(instance_key),
                                    timer_def.as_ref(),
                                    due_at,
                                    duration_millis,
                                );
                                self.emit(
                                    &mut log,
                                    Event::TimerCreated {
                                        timer_key: next_timer_key,
                                        instance_key,
                                        element_instance_key,
                                        element_id: element_id.clone(),
                                        due_at: next_due_at,
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
                        for event in self.cancel_boundary_signal_subscriptions_on(caught_eik) {
                            self.emit(&mut log, event);
                        }
                        for event in self.cancel_boundary_conditional_subscriptions_on(caught_eik) {
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
                    state::JobState::Completed
                        | state::JobState::Errored
                        | state::JobState::Canceled
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

            Command::BroadcastSignal {
                signal_name,
                variables,
            } => {
                // Mint a signal key (returned to the host; carried on the
                // SignalBroadcast event it restores the key generator on replay).
                let signal_key = self.mint_key();
                self.emit(
                    &mut log,
                    Event::SignalBroadcast {
                        signal_key,
                        signal_name: signal_name.clone(),
                    },
                );

                // Correlate to every matching open subscription, deterministic by
                // subscription key. Signals are not buffered: with no match the
                // signal is simply dropped.
                let mut matched: Vec<Key> = self
                    .state
                    .signal_subscriptions
                    .values()
                    .filter(|s| {
                        s.state == state::MessageSubscriptionState::Open
                            && s.signal_name == signal_name
                    })
                    .map(|s| s.key)
                    .collect();
                matched.sort_unstable();

                for subscription_key in matched {
                    // An earlier boundary correlation in this batch may have
                    // interrupted an activity that cancelled this subscription;
                    // re-check it is still open.
                    let subscription = match self.state.signal_subscriptions.get(&subscription_key)
                    {
                        Some(s) if s.state == state::MessageSubscriptionState::Open => s,
                        _ => continue,
                    };
                    let instance_key = subscription.instance_key;
                    let element_instance_key = subscription.element_instance_key;
                    let element_id = subscription.element_id.clone();
                    let kind = subscription.kind.clone();
                    self.advance_signal_correlated_token(
                        &mut log,
                        &mut queue,
                        subscription_key,
                        signal_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        kind,
                        &variables,
                    );
                }
            }

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
                let sub_cancels: Vec<Event> = subs
                    .iter()
                    .map(|s| Self::disarm_subscription_event(s))
                    .collect();

                let mut sig_subs: Vec<&state::SignalSubscription> = self
                    .state
                    .signal_subscriptions
                    .values()
                    .filter(|s| {
                        s.instance_key == instance_key
                            && s.state == state::MessageSubscriptionState::Open
                    })
                    .collect();
                sig_subs.sort_unstable_by_key(|s| s.key);
                let sig_sub_cancels: Vec<Event> = sig_subs
                    .iter()
                    .map(|s| Event::SignalSubscriptionCanceled {
                        subscription_key: s.key,
                        instance_key,
                        element_instance_key: s.element_instance_key,
                        element_id: s.element_id.clone(),
                    })
                    .collect();

                let mut cond_subs: Vec<&state::ConditionalSubscription> = self
                    .state
                    .conditional_subscriptions
                    .values()
                    .filter(|s| {
                        s.instance_key == instance_key
                            && s.state == state::MessageSubscriptionState::Open
                    })
                    .collect();
                cond_subs.sort_unstable_by_key(|s| s.key);
                let cond_sub_cancels: Vec<Event> = cond_subs
                    .iter()
                    .map(|s| Event::ConditionalSubscriptionCanceled {
                        subscription_key: s.key,
                        instance_key,
                        element_instance_key: s.element_instance_key,
                        element_id: s.element_id.clone(),
                    })
                    .collect();

                let mut user_tasks: Vec<&state::UserTask> = self
                    .state
                    .user_tasks
                    .values()
                    .filter(|t| {
                        t.instance_key == instance_key && t.state == state::UserTaskState::Created
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
                for event in sig_sub_cancels {
                    self.emit(&mut log, event);
                }
                for event in cond_sub_cancels {
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
        let mut cursor = 0usize;
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
            queue.extend(followups);
            // Re-evaluate any conditional-event subscriptions whose condition may
            // now hold: those opened, or whose referenced variables changed, since
            // the last pass. A satisfied condition fires (advancing a catch token,
            // interrupting an activity, or spawning a non-interrupting token),
            // which enqueues more work — so this runs inside the same fixpoint loop.
            cursor = self.reevaluate_conditionals(log, &mut queue, cursor);
            if queue.is_empty() {
                break;
            }
        }
    }

    /// Re-evaluates open conditional-event subscriptions against the variable
    /// changes and subscription openings recorded in `log[cursor..]`, firing every
    /// one whose FEEL condition now evaluates `true`. Returns the new cursor (the
    /// log length at entry) so the next pass only considers subsequently appended
    /// events. A no-op (cheap early return) when no conditional subscriptions
    /// exist, which is the common case.
    fn reevaluate_conditionals(
        &mut self,
        log: &mut Vec<Event>,
        queue: &mut VecDeque<Step>,
        cursor: usize,
    ) -> usize {
        let scan_end = log.len();
        if self.state.conditional_subscriptions.is_empty() {
            return scan_end;
        }
        // Which variables changed, per instance, and which conditional
        // subscriptions were opened, since the last pass.
        let mut changed: HashMap<Key, std::collections::HashSet<String>> = HashMap::new();
        let mut opened: std::collections::HashSet<Key> = std::collections::HashSet::new();
        for event in &log[cursor..scan_end] {
            match event {
                Event::VariablesUpdated {
                    instance_key,
                    variables,
                } => {
                    let entry = changed.entry(*instance_key).or_default();
                    for name in variables.keys() {
                        entry.insert(name.clone());
                    }
                }
                Event::ProcessInstanceCreated {
                    instance_key,
                    variables,
                    ..
                } => {
                    // Initial variables count as a change (a conditional event may
                    // already hold at activation).
                    let entry = changed.entry(*instance_key).or_default();
                    for name in variables.keys() {
                        entry.insert(name.clone());
                    }
                }
                Event::ConditionalSubscriptionCreated {
                    subscription_key, ..
                } => {
                    opened.insert(*subscription_key);
                }
                _ => {}
            }
        }
        if changed.is_empty() && opened.is_empty() {
            return scan_end;
        }
        // Collect the subscriptions to (re-)evaluate, deterministically by key: a
        // newly opened one, or one whose referenced variables changed.
        let mut candidates: Vec<Key> = self
            .state
            .conditional_subscriptions
            .values()
            .filter(|s| {
                s.state == state::MessageSubscriptionState::Open
                    && (opened.contains(&s.key)
                        || changed
                            .get(&s.instance_key)
                            .is_some_and(|vars| s.referenced_vars.iter().any(|v| vars.contains(v))))
            })
            .map(|s| s.key)
            .collect();
        candidates.sort_unstable();

        for key in candidates {
            // A prior fire in this batch may have interrupted the activity and
            // cancelled this subscription, so re-check it is still open.
            let Some(sub) = self.state.conditional_subscriptions.get(&key) else {
                continue;
            };
            if sub.state != state::MessageSubscriptionState::Open {
                continue;
            }
            let instance_key = sub.instance_key;
            let element_instance_key = sub.element_instance_key;
            let element_id = sub.element_id.clone();
            let condition = sub.condition.clone();
            let kind = sub.kind.clone();
            let vars = self.variables(instance_key);
            if !matches!(crate::feel::eval_bool(&condition, &vars), Ok(true)) {
                continue;
            }
            self.emit(
                log,
                Event::ConditionalTriggered {
                    subscription_key: key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                },
            );
            match kind {
                // Intermediate catch: releasing it resumes the token along its own
                // outgoing flow.
                state::MessageSubscriptionKind::IntermediateCatch => {
                    queue.push_back(Step::Complete {
                        instance_key,
                        element_instance_key,
                        element_id,
                    });
                }
                // Interrupting boundary: interrupt the attached activity, then run
                // the boundary event's outgoing flow.
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
                // Non-interrupting boundary: leave the activity running and spawn a
                // parallel token along the boundary's outgoing flow. The
                // subscription stays open (its applier does not settle it), so a
                // later change to a referenced variable can fire it again.
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
        scan_end
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

    /// Advances a token whose signal subscription just correlated, mirroring
    /// [`Self::advance_correlated_token`] but for signals (name-only, always
    /// local). Emits [`Event::SignalCorrelated`] + variable merge, then queues
    /// the catch completion or boundary interrupt/spawn.
    #[allow(clippy::too_many_arguments)]
    fn advance_signal_correlated_token(
        &mut self,
        log: &mut Vec<Event>,
        queue: &mut VecDeque<Step>,
        subscription_key: Key,
        signal_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        kind: state::MessageSubscriptionKind,
        variables: &HashMap<String, Value>,
    ) {
        self.emit(
            log,
            Event::SignalCorrelated {
                subscription_key,
                signal_key,
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
            },
        );
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
            state::MessageSubscriptionKind::IntermediateCatch => {
                queue.push_back(Step::Complete {
                    instance_key,
                    element_instance_key,
                    element_id,
                });
            }
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
            for event in self.cancel_boundary_signal_subscriptions_on(eik) {
                self.emit(log, event);
            }
            for event in self.cancel_boundary_conditional_subscriptions_on(eik) {
                self.emit(log, event);
            }
            // Output mappings on a sub-process apply as its scope drains.
            let outputs = self.io_outputs(instance_key, &element_id);
            if !outputs.is_empty() {
                let updates = self.eval_io_mappings(instance_key, &outputs);
                if !updates.is_empty() {
                    self.emit(
                        log,
                        Event::VariablesUpdated {
                            instance_key,
                            variables: updates,
                        },
                    );
                }
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
            Step::ActivateMiChild {
                instance_key,
                element_id,
                body_key,
                index,
            } => self.activate_mi_child(instance_key, element_id, body_key, index),
            Step::CompleteMiBody {
                instance_key,
                body_key,
            } => self.complete_multi_instance_body(instance_key, body_key),
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

        // A multi-instance activity: unless we are already inside its body (i.e.
        // this is one of its children, running in the body scope), this
        // activation opens the multi-instance body — it evaluates the input
        // collection and fans out one child per item instead of instantiating a
        // single activity.
        let is_mi_child = scope != 0
            && self
                .state
                .instances
                .get(&instance_key)
                .and_then(|i| i.multi_instances.get(&scope))
                .map(|mi| mi.element_id == element_id)
                .unwrap_or(false);
        if !is_mi_child {
            if let Some(mi) = self.multi_instance_of(instance_key, &element_id) {
                return self.activate_multi_instance_body(instance_key, element_id, scope, mi);
            }
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

        // Input mappings (zeebe:input): evaluate against the instance variables
        // and merge the result before the element's job/subscription is created,
        // so a later job activation snapshots the mapped values.
        let inputs = self.io_inputs(instance_key, &element_id);
        if !inputs.is_empty() {
            let updates = self.eval_io_mappings(instance_key, &inputs);
            if !updates.is_empty() {
                events.push(Event::VariablesUpdated {
                    instance_key,
                    variables: updates,
                });
            }
        }

        match kind {
            // A service task creates a job and parks the token.
            Some(ElementKind::ServiceTask { job_type, priority }) => {
                let job_key = self.mint_key();
                let job_type = self.resolve_job_type(instance_key, &job_type);
                let priority = self.resolve_priority(instance_key, priority.as_deref());
                let retries = self.resolve_retries(
                    instance_key,
                    self.retries_of(instance_key, &element_id).as_deref(),
                );
                events.push(Event::JobCreated {
                    job_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                    job_type,
                    created_at: self.now,
                    priority,
                    retries,
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
                let timer_def = self.timer_def_of(instance_key, &element_id);
                let (due_at, _) = self.resolve_timer(
                    Some(instance_key),
                    timer_def.as_ref(),
                    self.now,
                    duration_millis,
                );
                events.push(Event::TimerCreated {
                    timer_key,
                    instance_key,
                    element_instance_key,
                    element_id,
                    due_at,
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
                // The message name may be a FEEL expression evaluated on
                // activation against the instance variables (Zeebe parity).
                let message_name = self.resolve_event_name(Some(instance_key), &message_name);
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
            // A signal intermediate catch event opens a signal subscription
            // (name-only, no correlation key) and parks the token; a matching
            // BroadcastSignal releases it.
            Some(ElementKind::SignalIntermediateCatchEvent { signal_name }) => {
                let subscription_key = self.mint_key();
                // The signal name may be a FEEL expression evaluated on
                // activation against the instance variables (Zeebe parity).
                let signal_name = self.resolve_event_name(Some(instance_key), &signal_name);
                events.push(Event::SignalSubscriptionCreated {
                    subscription_key,
                    instance_key,
                    element_instance_key,
                    element_id,
                    signal_name,
                    kind: state::MessageSubscriptionKind::IntermediateCatch,
                });
            }
            // A conditional intermediate catch event evaluates its FEEL condition
            // on arrival: if already `true` the token passes straight through;
            // otherwise it parks on a conditional subscription, re-evaluated on
            // each change to a variable the condition references. A condition that
            // errors (e.g. a non-boolean result while a referenced variable is not
            // yet set) is treated as not-yet-satisfied and simply waits — a
            // conditional event is a wait-until, not an incident (a deliberate
            // flat-scope choice).
            Some(ElementKind::ConditionalIntermediateCatchEvent { condition }) => {
                let vars = self.variables(instance_key);
                if matches!(crate::feel::eval_bool(&condition, &vars), Ok(true)) {
                    followups.push(Step::Complete {
                        instance_key,
                        element_instance_key,
                        element_id,
                    });
                } else {
                    let subscription_key = self.mint_key();
                    let referenced_vars = sorted_referenced_vars(&condition);
                    events.push(Event::ConditionalSubscriptionCreated {
                        subscription_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        condition,
                        referenced_vars,
                        kind: state::MessageSubscriptionKind::IntermediateCatch,
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
            // An inline-FEEL script task is a synchronous activity: it activates
            // and immediately completes (no job). Its FEEL expression is
            // evaluated at completion (see `complete`), where a failure raises
            // an ExpressionEvaluation incident that re-evaluates on resolution.
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

    /// Opens a multi-instance body: evaluates the input collection and fans out
    /// one child of the activity per item. A non-list result or an evaluation
    /// error yields an empty loop (the body completes immediately) — a flat-scope
    /// choice mirroring the conditional-event error semantics. Children run in the
    /// body's scope; parallel bodies spawn every child at once, sequential bodies
    /// spawn the first and chain the rest at each child's completion.
    fn activate_multi_instance_body(
        &mut self,
        instance_key: Key,
        element_id: String,
        scope: Key,
        mi: crate::model::MultiInstance,
    ) -> (Vec<Event>, Vec<Step>) {
        let body_key = self.mint_key();
        let mut events = vec![
            Event::ElementActivating {
                instance_key,
                element_instance_key: body_key,
                element_id: element_id.clone(),
            },
            Event::ElementActivated {
                instance_key,
                element_instance_key: body_key,
                element_id: element_id.clone(),
                scope,
            },
        ];
        // Input mappings on the activity apply once, on body activation.
        let inputs = self.io_inputs(instance_key, &element_id);
        if !inputs.is_empty() {
            let updates = self.eval_io_mappings(instance_key, &inputs);
            if !updates.is_empty() {
                events.push(Event::VariablesUpdated {
                    instance_key,
                    variables: updates,
                });
            }
        }
        let vars = self.variables(instance_key);
        let items: Vec<Value> = match crate::feel::eval(&mi.input_collection, &vars) {
            Ok(Value::List(list)) => list,
            _ => Vec::new(),
        };
        let total = items.len();
        events.push(Event::MultiInstanceActivated {
            instance_key,
            body_key,
            element_id: element_id.clone(),
            sequential: mi.sequential,
            items,
            input_element: mi.input_element.clone(),
            output_collection: mi.output_collection.clone(),
            output_element: mi.output_element.clone(),
            completion_condition: mi.completion_condition.clone(),
        });

        let mut followups = Vec::new();
        if total == 0 {
            followups.push(Step::CompleteMiBody {
                instance_key,
                body_key,
            });
        } else if mi.sequential {
            followups.push(Step::ActivateMiChild {
                instance_key,
                element_id,
                body_key,
                index: 0,
            });
        } else {
            for index in 0..total {
                followups.push(Step::ActivateMiChild {
                    instance_key,
                    element_id: element_id.clone(),
                    body_key,
                    index,
                });
            }
        }
        (events, followups)
    }

    /// Activates one child of a multi-instance body: instantiates the activity
    /// again in the body scope, binding the `index`-th item (as `inputElement`,
    /// when named) and the 1-based `loopCounter` into the child's local variable
    /// overlay. A service-task child creates a job; any other activity kind passes
    /// straight through to completion (which routes back into the loop).
    fn activate_mi_child(
        &mut self,
        instance_key: Key,
        element_id: String,
        body_key: Key,
        index: usize,
    ) -> (Vec<Event>, Vec<Step>) {
        let (item, input_element) = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.multi_instances.get(&body_key))
        {
            Some(mi) => (
                mi.items.get(index).cloned().unwrap_or(Value::Null),
                mi.input_element.clone(),
            ),
            None => return (Vec::new(), Vec::new()),
        };

        let child_key = self.mint_key();
        let mut locals: HashMap<String, Value> = HashMap::new();
        if let Some(name) = &input_element {
            locals.insert(name.clone(), item);
        }
        locals.insert("loopCounter".to_string(), Value::Int((index as i64) + 1));

        let mut events = vec![
            Event::ElementActivating {
                instance_key,
                element_instance_key: child_key,
                element_id: element_id.clone(),
            },
            Event::ElementActivated {
                instance_key,
                element_instance_key: child_key,
                element_id: element_id.clone(),
                scope: body_key,
            },
            Event::MultiInstanceChildActivated {
                instance_key,
                body_key,
                child_key,
                index,
                local_variables: locals,
            },
        ];
        let mut followups = Vec::new();
        match self.element_kind(instance_key, &element_id) {
            Some(ElementKind::ServiceTask { job_type, priority }) => {
                let job_key = self.mint_key();
                let job_type = self.resolve_job_type(instance_key, &job_type);
                let priority = self.resolve_priority(instance_key, priority.as_deref());
                let retries = self.resolve_retries(
                    instance_key,
                    self.retries_of(instance_key, &element_id).as_deref(),
                );
                events.push(Event::JobCreated {
                    job_key,
                    instance_key,
                    element_instance_key: child_key,
                    element_id,
                    job_type,
                    created_at: self.now,
                    priority,
                    retries,
                });
            }
            _ => {
                followups.push(Step::Complete {
                    instance_key,
                    element_instance_key: child_key,
                    element_id,
                });
            }
        }
        (events, followups)
    }

    /// Completes one multi-instance child: collects its `output_element` (in the
    /// child's local scope) into the body's results at the child's index, then
    /// decides what comes next — fire the completion condition (complete the body
    /// early), spawn the next child (sequential), or complete the body once every
    /// child has finished (parallel).
    fn complete_mi_child(
        &mut self,
        instance_key: Key,
        child_eik: Key,
        element_id: String,
        body_key: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let mut events = vec![
            Event::ElementCompleting {
                instance_key,
                element_instance_key: child_eik,
                element_id: element_id.clone(),
            },
            Event::ElementCompleted {
                instance_key,
                element_instance_key: child_eik,
                element_id: element_id.clone(),
            },
        ];

        // Snapshot the loop configuration and this child's index before evaluating.
        let (
            output_element,
            completion_condition,
            sequential,
            total,
            spawned,
            active_now,
            mi_element,
        ) = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.multi_instances.get(&body_key))
        {
            Some(mi) => (
                mi.output_element.clone(),
                mi.completion_condition.clone(),
                mi.sequential,
                mi.items.len(),
                mi.spawned,
                mi.active.len(),
                mi.element_id.clone(),
            ),
            None => return (events, Vec::new()),
        };
        let index = self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.element_locals.get(&child_eik))
            .and_then(|l| l.get("loopCounter"))
            .and_then(|v| v.as_f64())
            .map(|c| (c as i64 - 1).max(0) as usize)
            .unwrap_or(0);

        // Collect this child's output (evaluated in its local scope) at its index.
        let output = output_element.as_deref().and_then(|expr| {
            let vars = self.variables_for_element(instance_key, child_eik);
            crate::feel::eval(expr, &vars).ok()
        });
        events.push(Event::MultiInstanceChildCompleted {
            instance_key,
            body_key,
            child_key: child_eik,
            index,
            output,
        });

        // After each child, a satisfied completion condition ends the body early.
        let completion_now = completion_condition
            .as_deref()
            .map(|c| {
                matches!(
                    crate::feel::eval_bool(c, &self.variables(instance_key)),
                    Ok(true)
                )
            })
            .unwrap_or(false);

        let mut followups = Vec::new();
        // `active_now` still counts this child (its removal event above is not yet
        // applied), so the parallel body is drained when only this child remains.
        let others_active = active_now.saturating_sub(1);
        if completion_now {
            followups.push(Step::CompleteMiBody {
                instance_key,
                body_key,
            });
        } else if sequential {
            if spawned < total {
                followups.push(Step::ActivateMiChild {
                    instance_key,
                    element_id: mi_element,
                    body_key,
                    index: spawned,
                });
            } else {
                followups.push(Step::CompleteMiBody {
                    instance_key,
                    body_key,
                });
            }
        } else if others_active == 0 && spawned >= total {
            followups.push(Step::CompleteMiBody {
                instance_key,
                body_key,
            });
        }
        (events, followups)
    }

    /// Completes a multi-instance body: cancels any children still running (an
    /// early completion-condition fire), writes the aggregated output collection
    /// (padding uncollected slots with `null`) into the instance scope, applies
    /// the activity's output mappings, and takes its outgoing flow.
    fn complete_multi_instance_body(
        &mut self,
        instance_key: Key,
        body_key: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let (element_id, output_collection, output_values, active): (
            ElementId,
            Option<String>,
            Vec<Value>,
            Vec<Key>,
        ) = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.multi_instances.get(&body_key))
        {
            Some(mi) => (
                mi.element_id.clone(),
                mi.output_collection.clone(),
                mi.output_values
                    .iter()
                    .map(|o| o.clone().unwrap_or(Value::Null))
                    .collect(),
                mi.active.iter().copied().collect(),
            ),
            None => return (Vec::new(), Vec::new()),
        };
        let scope = self.scope_of(instance_key, body_key);

        let mut events = Vec::new();
        // Cancel any children still running (reached here via completion condition).
        for child in &active {
            events.extend(self.cancel_mi_child_events(instance_key, *child));
        }
        // Write the aggregated output collection to the instance scope, and apply
        // the activity's output mappings against it.
        let mut collection_update: Option<HashMap<String, Value>> = None;
        if let Some(name) = output_collection {
            let mut map = HashMap::new();
            map.insert(name, Value::List(output_values));
            events.push(Event::VariablesUpdated {
                instance_key,
                variables: map.clone(),
            });
            collection_update = Some(map);
        }
        events.push(Event::ElementCompleting {
            instance_key,
            element_instance_key: body_key,
            element_id: element_id.clone(),
        });
        events.push(Event::ElementCompleted {
            instance_key,
            element_instance_key: body_key,
            element_id: element_id.clone(),
        });
        events.push(Event::MultiInstanceCompleted {
            instance_key,
            body_key,
        });
        // Output mappings (zeebe:output) on the activity apply as the body drains,
        // able to reference the just-written output collection.
        let outputs = self.io_outputs(instance_key, &element_id);
        if !outputs.is_empty() {
            let updates = match &collection_update {
                Some(update) => {
                    let mut vars = (*self.variables(instance_key)).clone();
                    vars.extend(update.clone());
                    self.eval_io_mappings_in(&vars, &outputs)
                }
                None => self.eval_io_mappings(instance_key, &outputs),
            };
            if !updates.is_empty() {
                events.push(Event::VariablesUpdated {
                    instance_key,
                    variables: updates,
                });
            }
        }
        let mut followups = Vec::new();
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

    /// Builds the events that cancel a still-running multi-instance child (its job
    /// and element instance), for an early body completion. Returns events (it
    /// does not emit) so it composes inside a `process_step` result.
    fn cancel_mi_child_events(&self, instance_key: Key, child_eik: Key) -> Vec<Event> {
        let mut events = Vec::new();
        let element_id = self
            .element_id_of_instance(instance_key, child_eik)
            .unwrap_or_default();
        if let Some(job_key) = self.active_job_on(child_eik) {
            events.push(Event::JobCanceled {
                job_key,
                instance_key,
            });
        }
        events.extend(self.cancel_all_timers_on(child_eik));
        events.extend(self.cancel_all_subscriptions_on(child_eik));
        events.push(Event::ElementCompleting {
            instance_key,
            element_instance_key: child_eik,
            element_id: element_id.clone(),
        });
        events.push(Event::ElementCompleted {
            instance_key,
            element_instance_key: child_eik,
            element_id,
        });
        events
    }

    fn complete(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    ) -> (Vec<Event>, Vec<Step>) {
        // A completing element instance whose scope is a multi-instance body is
        // one of that body's children: its completion feeds the loop's output
        // collection and completion condition rather than taking the activity's
        // outgoing flow directly.
        let scope = self.scope_of(instance_key, element_instance_key);
        if scope != 0
            && self
                .state
                .instances
                .get(&instance_key)
                .and_then(|i| i.multi_instances.get(&scope))
                .map(|mi| mi.element_id == element_id)
                .unwrap_or(false)
        {
            return self.complete_mi_child(instance_key, element_instance_key, element_id, scope);
        }

        if matches!(
            self.element_kind(instance_key, &element_id),
            Some(ElementKind::ExclusiveGateway)
        ) {
            return self.complete_exclusive_gateway(instance_key, element_instance_key, element_id);
        }

        // Inline-FEEL script task: evaluate its `zeebe:script` expression now.
        // The instance variables already include any input mappings applied when
        // the task activated. On success the result is staged under
        // `resultVariable` and written before output mappings run (so a
        // `zeebe:output` can reference it); on failure an ExpressionEvaluation
        // incident is raised and the element stays active — matching Zeebe, which
        // re-evaluates the script when the incident is resolved (the resolution
        // re-runs completion), exactly as an exclusive gateway condition does.
        let mut script_update: Option<HashMap<String, Value>> = None;
        if let Some(ElementKind::ScriptTask {
            expression,
            result_variable,
        }) = self.element_kind(instance_key, &element_id)
        {
            let vars = self.variables(instance_key);
            match crate::feel::eval(&expression, &vars) {
                Ok(value) => {
                    let mut update = HashMap::new();
                    update.insert(result_variable, value);
                    script_update = Some(update);
                }
                Err(err) => {
                    let incident_key = self.mint_key();
                    let reason = format!(
                        "failed to evaluate script expression '{expression}' at script task \
                         '{element_id}': {}",
                        err.0
                    );
                    return (
                        vec![Event::IncidentRaised {
                            incident_key,
                            instance_key,
                            element_instance_key,
                            element_id,
                            kind: state::IncidentKind::ExpressionEvaluation,
                            reason,
                            job_key: None,
                            created_at: self.now,
                        }],
                        Vec::new(),
                    );
                }
            }
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
        events.extend(self.cancel_boundary_signal_subscriptions_on(element_instance_key));
        events.extend(self.cancel_boundary_conditional_subscriptions_on(element_instance_key));
        // A script task's result is merged before output mappings so a
        // `zeebe:output` can reference/remap it (Zeebe merges `resultVariable`
        // first, then applies output mappings).
        if let Some(update) = &script_update {
            events.push(Event::VariablesUpdated {
                instance_key,
                variables: update.clone(),
            });
        }
        // Output mappings (zeebe:output): evaluate against the instance variables
        // (which already include any job/message result merged on completion, or
        // a script task's result staged above) and merge the projected result
        // before the outgoing flows are taken.
        let outputs = self.io_outputs(instance_key, &element_id);
        if !outputs.is_empty() {
            let updates = match &script_update {
                // The script result event is not applied to state until this
                // step returns, so overlay it onto the eval context by hand.
                Some(update) => {
                    let mut vars = (*self.variables(instance_key)).clone();
                    vars.extend(update.clone());
                    self.eval_io_mappings_in(&vars, &outputs)
                }
                None => self.eval_io_mappings(instance_key, &outputs),
            };
            if !updates.is_empty() {
                events.push(Event::VariablesUpdated {
                    instance_key,
                    variables: updates,
                });
            }
        }
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
                let retries = self.resolve_retries(
                    instance_key,
                    self.retries_of(instance_key, &element_id).as_deref(),
                );
                (
                    vec![Event::JobCreated {
                        job_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        job_type,
                        created_at: self.now,
                        priority,
                        retries,
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
        let mut default_flow = None;
        let mut eval_error: Option<String> = None;
        for flow in self.outgoing(instance_key, &element_id) {
            // The explicit `default` flow is a fallback only: it is never taken
            // by document order, but kept aside in case no conditional flow
            // matches.
            if flow.is_default {
                default_flow = Some(flow);
                continue;
            }
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
        // Fall back to the explicit default flow when no conditional flow matched.
        if selected.is_none() && eval_error.is_none() {
            selected = default_flow;
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
        if self.track_dirty_vars {
            match &event {
                Event::ProcessInstanceCreated { instance_key, .. }
                | Event::VariablesUpdated { instance_key, .. } => {
                    self.dirty_vars.insert(*instance_key);
                    self.forgotten_vars.remove(instance_key);
                }
                // A terminal instance drops its variables in `state::apply`
                // (ADR 0012). Mirror that in the durable store: forget the
                // payload so a snapshot taken before exporter-driven eviction
                // cannot resurrect it on recovery. Ordering is deliberate — this
                // wins over any dirty mark from the same command's earlier
                // `VariablesUpdated` (e.g. job-output merge then completion).
                Event::ProcessInstanceCompleted { instance_key }
                | Event::ProcessInstanceTerminated { instance_key } => {
                    self.dirty_vars.remove(instance_key);
                    self.forgotten_vars.insert(*instance_key);
                }
                _ => {}
            }
        }
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

    /// The multi-instance loop characteristics declared on `element_id`, if any.
    fn multi_instance_of(
        &self,
        instance_key: Key,
        element_id: &str,
    ) -> Option<crate::model::MultiInstance> {
        self.process_of_instance(instance_key)?
            .element(element_id)
            .and_then(|e| e.multi_instance.clone())
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
impl OwnedByInstance for state::SignalSubscription {
    fn instance_key(&self) -> Key {
        self.instance_key
    }
}
impl OwnedByInstance for state::ConditionalSubscription {
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

/// The sorted set of root variable names a conditional event's FEEL `condition`
/// references, used both to record the subscription's dependencies and to decide
/// which variable changes re-evaluate it. Sorted for a deterministic event body.
fn sorted_referenced_vars(condition: &str) -> Vec<String> {
    let mut vars: Vec<String> = crate::feel::referenced_variables(condition)
        .into_iter()
        .collect();
    vars.sort();
    vars
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
    owned.into_iter().filter_map(|k| map.remove(&k)).collect()
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
