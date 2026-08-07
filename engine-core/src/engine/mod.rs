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
    /// Follower-only safety net for the retirement digest (RF>1). A leader completes
    /// and exporter-evicts an instance and broadcasts its key, but under leader-durable
    /// the ack happens on the leader before the async learner has applied the
    /// instance's `CreateInstance`, so [`Engine::retire_instances`] finds the key
    /// absent. Rather than drop the retirement (and leak the instance once its create
    /// lands), the absent key is remembered here; the next `apply_command_at` that
    /// materializes it retires it immediately. Populated only on a follower replica
    /// (a leader never receives a retirement digest, so this stays empty and the
    /// per-command check is free). Bounded — it only ever holds keys in the brief
    /// window between a retirement and its create, so it drains continuously. NOT
    /// part of the snapshot; pure host-side bookkeeping, never affects determinism.
    retired_tombstones: HashSet<Key>,
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
    /// Activate one ad-hoc "tool" child: instantiate `element_id` inside the
    /// container's scope, seeded with the agent's `variables`, and mark it active
    /// in the container. Created for each activate-element instruction an ad-hoc
    /// agent job returns (ADR 0023 seam 2).
    ActivateAdHocTool {
        instance_key: Key,
        container_key: Key,
        element_id: String,
        variables: HashMap<String, Value>,
    },
    /// Complete an ad-hoc container: write its aggregated `outputCollection`,
    /// (when `cancel`) cancel any tool children still running, drop its runtime
    /// state and take the container's outgoing flow. Driven by the agent
    /// signalling completion / cancellation, or by the loop draining with no
    /// further tools requested.
    CompleteAdHoc {
        instance_key: Key,
        container_key: Key,
        cancel: bool,
    },
    /// Advance an element's execution-listener chain after one listener job
    /// completed (ADR 0037): create the next listener job, or — when the chain
    /// is drained — run the deferred lifecycle transition (a `Start` chain runs
    /// the element's normal activation behaviour; an `End` chain emits
    /// `ElementCompleted` and takes the outgoing flows).
    AdvanceListener {
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        event_type: crate::model::ListenerEventType,
        /// The 0-based index of the listener that just completed.
        index: usize,
        /// The element's enclosing flow scope.
        scope: Key,
    },
    /// Advance a user task's task-listener chain after a listener job completed
    /// (ADR 0037 §6): mint the next listener's job, or — when the chain drains —
    /// commit the deferred transition (assign/update/complete/create/cancel).
    AdvanceTaskListener {
        user_task_key: Key,
        event_type: crate::model::TaskListenerEventType,
        /// The 0-based index (within this event type's listener list) of the
        /// listener that just completed.
        index: usize,
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
            track_dirty_vars: false,
            dirty_vars: HashSet::new(),
            forgotten_vars: HashSet::new(),
            retired_tombstones: HashSet::new(),
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
            retired_tombstones: HashSet::new(),
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
            if matches!(
                event,
                Event::ProcessDeployed { .. }
                    | Event::DecisionRequirementsDeployed { .. }
                    | Event::DecisionDeployed { .. }
            ) {
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
            } else if let Event::DecisionRequirementsDeployed { drg, version, .. } = event {
                let newer = self
                    .state
                    .decision_requirements
                    .get(&drg.id)
                    .map(|d| *version > d.version)
                    .unwrap_or(true);
                if newer {
                    state::apply(&mut self.state, event);
                }
            } else if let Event::DecisionDeployed {
                decision_id,
                version,
                ..
            } = event
            {
                let newer = self
                    .state
                    .decisions
                    .get(decision_id)
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

        // Emit DeploymentCreated FIRST, unconditionally — this is the C8-spec
        // handle the client sees in the response envelope (LongKey pattern,
        // never empty) and mirrors Zeebe's DeploymentIntent.CREATED. The
        // shared deployment_key is minted here and stamped onto every
        // ProcessDeployed that follows in the same call (issue #47).
        let deployment_key = self.mint_key();
        self.emit(log, Event::DeploymentCreated { deployment_key });

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
                    let message_name = self.resolve_event_name(&HashMap::new(), &message_name);
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
                        &HashMap::new(),
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

    /// Registers one or more decision requirements graphs as a deployment,
    /// mirroring [`Engine::deploy`]: a shared deployment key, one
    /// [`Event::DecisionRequirementsDeployed`] per DRG, and one
    /// [`Event::DecisionDeployed`] per decision it contains (indexed by id).
    /// An idempotent redeploy of the identical latest DRG is skipped.
    fn deploy_decisions(
        &mut self,
        log: &mut Vec<Event>,
        graphs: Vec<crate::dmn::DecisionRequirementsGraph>,
    ) -> Result<(), EngineError> {
        let deployment_key = self.mint_key();
        self.emit(log, Event::DeploymentCreated { deployment_key });

        for drg in graphs {
            if self
                .state
                .decision_requirements
                .get(&drg.id)
                .is_some_and(|existing| existing.drg == drg)
            {
                // Idempotent redeploy of the identical latest DRG: skip.
                continue;
            }
            let version = self
                .state
                .decision_requirements
                .get(&drg.id)
                .map(|d| d.version + 1)
                .unwrap_or(1);
            let decision_requirements_key = self.mint_key();

            // Emit the DRG registration first so the DecisionDeployed applier can
            // resolve the graph it belongs to.
            let decisions = drg.decisions.clone();
            self.emit(
                log,
                Event::DecisionRequirementsDeployed {
                    deployment_key,
                    decision_requirements_key,
                    version,
                    drg,
                },
            );

            for decision in &decisions {
                let decision_version = self
                    .state
                    .decisions
                    .get(&decision.id)
                    .map(|d| d.version + 1)
                    .unwrap_or(1);
                let decision_key = self.mint_key();
                self.emit(
                    log,
                    Event::DecisionDeployed {
                        deployment_key,
                        decision_requirements_key,
                        decision_key,
                        decision_id: decision.id.clone(),
                        decision_name: decision.name.clone(),
                        version: decision_version,
                    },
                );
            }
        }
        Ok(())
    }

    /// Evaluates a deployed decision on demand for the standalone
    /// EvaluateDecision API. Resolves the decision by id (latest version) or, if
    /// `by_id` is `None`, by decision key, evaluates it against `variables`
    /// natively, and returns the deployment metadata together with the full
    /// evaluation result (including any failure). This is a pure read: it does
    /// not mint keys, emit events, or mutate state. Returns `None` when no such
    /// decision is deployed.
    pub fn evaluate_deployed_decision(
        &self,
        by_id: Option<&str>,
        by_key: Option<Key>,
        variables: &HashMap<String, Value>,
    ) -> Option<DecisionEvaluation> {
        let deployed = match (by_id, by_key) {
            (Some(id), _) => self.state.decisions.get(id)?.clone(),
            (None, Some(key)) => self
                .state
                .decisions
                .values()
                .find(|d| d.key == key)?
                .clone(),
            (None, None) => return None,
        };
        let result = crate::dmn::evaluate(&deployed.drg, &deployed.decision_id, variables);
        Some(DecisionEvaluation {
            decision_key: deployed.key,
            version: deployed.version,
            decision_id: deployed.decision_id.clone(),
            decision_name: deployed.decision_name.clone(),
            decision_requirements_key: deployed.decision_requirements_key,
            decision_requirements_id: deployed.drg.id.clone(),
            result,
        })
    }

    /// Deployment key and version of a deployed decision by id (latest version),
    /// or `None` if not deployed. Used by the EvaluateDecision API to stamp each
    /// evaluated decision in a graph with its own deployment identity.
    pub fn deployed_decision_key_version(&self, decision_id: &str) -> Option<(Key, i32)> {
        self.state
            .decisions
            .get(decision_id)
            .map(|d| (d.key, d.version))
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

            Command::DeployDecisionRequirements(graphs) => {
                self.deploy_decisions(&mut log, graphs)?;
            }

            Command::DeleteDecisionInstance {
                instance_key,
                decision_evaluation_key,
            } => {
                // Audit-only retraction: emit the event so the read-model
                // projection deletes the matching decision-instance rows. No core
                // engine state is touched (decision instances are not engine state).
                self.emit(
                    &mut log,
                    Event::DecisionInstanceDeleted {
                        instance_key,
                        decision_evaluation_key,
                    },
                );
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

            Command::CompleteJob {
                job_key,
                variables,
                adhoc_result,
                task_listener_result,
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
                let created_at = job.created_at;
                let job_type = job.job_type.clone();
                let job_kind = job.kind;

                // A task-listener job (ADR 0037 §6) gates a user task's deferred
                // transition. Validate the completion up-front so a bad one is
                // rejected wholesale: task-listener jobs may not carry variables,
                // deny is only honoured on assigning/updating/completing, and a
                // denial cannot also carry corrections (Zeebe parity).
                if let state::JobKind::TaskListener {
                    event_type,
                    user_task_key,
                    ..
                } = job_kind
                {
                    let result = task_listener_result.clone().unwrap_or_default();
                    if !variables.is_empty() {
                        return Err(EngineError::TaskListenerJobWithVariables { job_key });
                    }
                    if result.denied && !result.corrections.is_empty() {
                        return Err(EngineError::TaskListenerDenyWithCorrections { job_key });
                    }
                    if result.denied && !Self::task_event_supports_deny(event_type) {
                        return Err(EngineError::TaskListenerDenyNotSupported { job_key });
                    }
                    // A creating listener cannot correct the assignee when the
                    // task already declares an initial assignee (Zeebe parity).
                    if matches!(event_type, crate::model::TaskListenerEventType::Creating)
                        && result.corrections.assignee.is_some()
                        && self
                            .state
                            .user_tasks
                            .get(&user_task_key)
                            .map(|t| {
                                // An initial assignee may already be applied to the
                                // record, or (when `assigning` listeners exist) be
                                // stripped off and carried on the pending creating
                                // transition for later routing — either forbids a
                                // creating assignee correction (Zeebe parity).
                                t.assignee.is_some()
                                    || t.pending
                                        .as_ref()
                                        .map(|p| p.assignee.is_some())
                                        .unwrap_or(false)
                            })
                            .unwrap_or(false)
                    {
                        return Err(EngineError::TaskListenerAssigneeCorrectionOnCreating {
                            user_task_key,
                        });
                    }
                }

                // Ad-hoc agent job completion (ADR 0023 seam 2 / #614 gap 4):
                // validate the activate-element instructions up-front so a bad
                // turn is rejected wholesale (Zeebe parity) before any
                // `JobCompleted` or activation side effect applies. The check
                // only fires for a container's agent job (its element id is in
                // the ad-hoc catalog); the tools' own jobs are not, so they fall
                // through unchanged.
                if let Some(result) = adhoc_result.as_ref() {
                    if let Some(def) = self.adhoc_def_of(instance_key, &element_id) {
                        // Every activated id must name one of the container's
                        // tools (Zeebe NOT_FOUND, checked first — see
                        // JobCompleteProcessor.checkAdHocSubprocessActivationTargetsAreValid);
                        // otherwise the loop would mint a phantom child that
                        // immediately completes.
                        Self::validate_adhoc_activation_targets(
                            &def,
                            instance_key,
                            &result.activate_elements,
                        )?;
                        // Asserting the completion condition is fulfilled while
                        // also requesting activations is contradictory (Zeebe
                        // INVALID_ARGUMENT — checkAdHocSubProcessCompletionCondition
                        // NotFulfilledForElementActivation).
                        if result.completion_condition_fulfilled
                            && !result.activate_elements.is_empty()
                        {
                            return Err(EngineError::AdHocActivateWithCompletion { job_key });
                        }
                    }
                }

                self.emit(
                    &mut log,
                    Event::JobCompleted {
                        job_key,
                        instance_key,
                        created_at,
                        job_type,
                    },
                );
                if !variables.is_empty() {
                    // Job result variables propagate from the task's enclosing
                    // (flow) scope upward — each name updates the nearest ancestor
                    // scope that defines it, defaulting to root. For a root-only
                    // instance this collapses to the flat `VariablesUpdated`,
                    // byte-identical to the pre-scoping engine.
                    let flow_scope = self.scope_of(instance_key, element_instance_key);
                    for event in self.propagated_updates(instance_key, flow_scope, variables, false)
                    {
                        self.emit(&mut log, event);
                    }
                }
                // A task-listener job: its completion advances (or denies) the
                // user task's deferred transition rather than resuming a token.
                if let state::JobKind::TaskListener {
                    event_type,
                    index,
                    user_task_key,
                } = job_kind
                {
                    let result = task_listener_result.unwrap_or_default();
                    if result.denied {
                        // The transition is rejected: clear the pending state so
                        // the task returns to its prior available state. The
                        // reason is journaled on the resolution event.
                        self.emit(
                            &mut log,
                            Event::UserTaskTransitionResolved {
                                user_task_key,
                                instance_key,
                                denied: Some(result.denied_reason.unwrap_or_default()),
                            },
                        );
                    } else {
                        if !result.corrections.is_empty() {
                            self.emit(
                                &mut log,
                                Event::UserTaskCorrectionsApplied {
                                    user_task_key,
                                    instance_key,
                                    corrections: result.corrections,
                                },
                            );
                        }
                        queue.push_back(Step::AdvanceTaskListener {
                            user_task_key,
                            event_type,
                            index,
                        });
                    }
                }
                // An execution-listener job (ADR 0037): its completion advances the
                // element's listener chain rather than resuming the token. The
                // JobCompleted + variable merge above still apply (a listener may
                // contribute variables that later listeners and the element see).
                else if let state::JobKind::ExecutionListener {
                    event_type,
                    index,
                    scope,
                } = job_kind
                {
                    queue.push_back(Step::AdvanceListener {
                        instance_key,
                        element_instance_key,
                        element_id,
                        event_type,
                        index,
                        scope,
                    });
                } else if self.adhoc_def_of(instance_key, &element_id).is_some() {
                    // An ad-hoc container's agent job: instead of resuming the
                    // container token, drive the activate-element loop (ADR 0023
                    // seam 2). The container element id is in the definition's
                    // ad-hoc catalog; ordinary jobs (including the tools' own jobs)
                    // are not, so they fall through to the normal token resume.
                    let container_key = element_instance_key;
                    let result = adhoc_result.unwrap_or_default();
                    if result.completion_condition_fulfilled {
                        // The agent asserts the container's completion condition is
                        // met (Camunda `isCompletionConditionFulfilled`): complete
                        // now, cancelling any tools still running so none is orphaned
                        // (ADR 0023 seam 4). This is honoured independently of the
                        // engine-side `<completionCondition>` FEEL (evaluated per
                        // tool completion), so an agent can end the loop even when no
                        // static condition is declared.
                        queue.push_back(Step::CompleteAdHoc {
                            instance_key,
                            container_key,
                            cancel: true,
                        });
                    } else {
                        // Ordinary turn (possibly `cancelRemainingInstances`):
                        // shared with the external activate-activities command
                        // (#614 gap 3) so both seams activate identically.
                        self.enqueue_adhoc_turn(
                            &mut queue,
                            instance_key,
                            container_key,
                            result.activate_elements,
                            result.cancel_remaining_instances,
                        );
                    }
                } else {
                    // The parked service-task token resumes from ACTIVATED.
                    queue.push_back(Step::Complete {
                        instance_key,
                        element_instance_key,
                        element_id,
                    });
                }
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
                if task.state != state::UserTaskState::Created || task.pending.is_some() {
                    return Err(EngineError::UserTaskNotActive { user_task_key });
                }
                // Mirror Camunda: when override is disallowed and the task is
                // already assigned, reject so it must be unassigned first.
                if !allow_override && task.assignee.is_some() {
                    return Err(EngineError::UserTaskAlreadyAssigned { user_task_key });
                }
                let instance_key = task.instance_key;
                let element_instance_key = task.element_instance_key;
                let element_id = task.element_id.clone();
                let listeners = self.task_listeners_of(
                    instance_key,
                    &element_id,
                    crate::model::TaskListenerEventType::Assigning,
                );
                if let Some(first) = listeners.first().cloned() {
                    // Defer the assignment behind the assigning listener chain.
                    let pending = state::PendingUserTaskTransition {
                        event_type: crate::model::TaskListenerEventType::Assigning,
                        assignee: Some(assignee),
                        update: None,
                        variables: std::collections::HashMap::new(),
                        corrections: crate::model::UserTaskCorrections::default(),
                    };
                    for event in self.start_task_listener_chain(
                        user_task_key,
                        instance_key,
                        element_instance_key,
                        &element_id,
                        crate::model::TaskListenerEventType::Assigning,
                        pending,
                        &first,
                    ) {
                        self.emit(&mut log, event);
                    }
                } else {
                    self.emit(
                        &mut log,
                        Event::UserTaskAssigned {
                            user_task_key,
                            instance_key,
                            assignee: Some(assignee),
                        },
                    );
                }
            }

            Command::UnassignUserTask { user_task_key } => {
                let task = self
                    .state
                    .user_tasks
                    .get(&user_task_key)
                    .ok_or(EngineError::UserTaskNotFound { user_task_key })?;
                if task.state != state::UserTaskState::Created || task.pending.is_some() {
                    return Err(EngineError::UserTaskNotActive { user_task_key });
                }
                let instance_key = task.instance_key;
                let element_instance_key = task.element_instance_key;
                let element_id = task.element_id.clone();
                let listeners = self.task_listeners_of(
                    instance_key,
                    &element_id,
                    crate::model::TaskListenerEventType::Assigning,
                );
                if let Some(first) = listeners.first().cloned() {
                    let pending = state::PendingUserTaskTransition {
                        event_type: crate::model::TaskListenerEventType::Assigning,
                        assignee: None,
                        update: None,
                        variables: std::collections::HashMap::new(),
                        corrections: crate::model::UserTaskCorrections::default(),
                    };
                    for event in self.start_task_listener_chain(
                        user_task_key,
                        instance_key,
                        element_instance_key,
                        &element_id,
                        crate::model::TaskListenerEventType::Assigning,
                        pending,
                        &first,
                    ) {
                        self.emit(&mut log, event);
                    }
                } else {
                    self.emit(
                        &mut log,
                        Event::UserTaskAssigned {
                            user_task_key,
                            instance_key,
                            assignee: None,
                        },
                    );
                }
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
                if task.state != state::UserTaskState::Created || task.pending.is_some() {
                    return Err(EngineError::UserTaskNotActive { user_task_key });
                }
                let instance_key = task.instance_key;
                let element_instance_key = task.element_instance_key;
                let element_id = task.element_id.clone();
                // Normalise empty-string dates to "reset" (None), matching the
                // REST contract ("Reset by providing an empty String").
                let normalize =
                    |d: Option<String>| -> Option<String> { d.filter(|s| !s.is_empty()) };
                let candidate_groups = changeset.candidate_groups;
                let candidate_users = changeset.candidate_users;
                let due_date = changeset.due_date.map(normalize);
                let follow_up_date = changeset.follow_up_date.map(normalize);
                let priority = changeset.priority;
                let listeners = self.task_listeners_of(
                    instance_key,
                    &element_id,
                    crate::model::TaskListenerEventType::Updating,
                );
                if let Some(first) = listeners.first().cloned() {
                    let pending = state::PendingUserTaskTransition {
                        event_type: crate::model::TaskListenerEventType::Updating,
                        assignee: None,
                        update: Some(state::PendingUserTaskUpdate {
                            candidate_groups,
                            candidate_users,
                            due_date,
                            follow_up_date,
                            priority,
                        }),
                        variables: std::collections::HashMap::new(),
                        corrections: crate::model::UserTaskCorrections::default(),
                    };
                    for event in self.start_task_listener_chain(
                        user_task_key,
                        instance_key,
                        element_instance_key,
                        &element_id,
                        crate::model::TaskListenerEventType::Updating,
                        pending,
                        &first,
                    ) {
                        self.emit(&mut log, event);
                    }
                } else {
                    self.emit(
                        &mut log,
                        Event::UserTaskUpdated {
                            user_task_key,
                            instance_key,
                            candidate_groups,
                            candidate_users,
                            due_date,
                            follow_up_date,
                            priority,
                        },
                    );
                }
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
                if task.state != state::UserTaskState::Created || task.pending.is_some() {
                    return Err(EngineError::UserTaskNotActive { user_task_key });
                }
                let instance_key = task.instance_key;
                let element_instance_key = task.element_instance_key;
                let element_id = task.element_id.clone();

                let listeners = self.task_listeners_of(
                    instance_key,
                    &element_id,
                    crate::model::TaskListenerEventType::Completing,
                );
                if let Some(first) = listeners.first().cloned() {
                    // Defer completion behind the completing listener chain; the
                    // completion variables ride on the pending transition and are
                    // applied when the chain drains.
                    let pending = state::PendingUserTaskTransition {
                        event_type: crate::model::TaskListenerEventType::Completing,
                        assignee: None,
                        update: None,
                        variables,
                        corrections: crate::model::UserTaskCorrections::default(),
                    };
                    for event in self.start_task_listener_chain(
                        user_task_key,
                        instance_key,
                        element_instance_key,
                        &element_id,
                        crate::model::TaskListenerEventType::Completing,
                        pending,
                        &first,
                    ) {
                        self.emit(&mut log, event);
                    }
                } else {
                    self.emit(
                        &mut log,
                        Event::UserTaskCompleted {
                            user_task_key,
                            instance_key,
                        },
                    );
                    if !variables.is_empty() {
                        // User-task completion variables propagate from the task's
                        // enclosing (flow) scope upward, defaulting to root (flat
                        // `VariablesUpdated` for a root-only instance).
                        let flow_scope = self.scope_of(instance_key, element_instance_key);
                        for event in
                            self.propagated_updates(instance_key, flow_scope, variables, false)
                        {
                            self.emit(&mut log, event);
                        }
                    }
                    // The parked user-task token resumes from ACTIVATED.
                    queue.push_back(Step::Complete {
                        instance_key,
                        element_instance_key,
                        element_id,
                    });
                }
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
                                let rearm_vars =
                                    self.variables_for_element(instance_key, element_instance_key);
                                let (next_due_at, _) = self.resolve_timer(
                                    &rearm_vars,
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
                variables,
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
                        // Seed the thrown error's variables at the local scope of
                        // the catch (the boundary event's scope), so the
                        // error-handling path downstream can read them (Camunda
                        // `JobErrorRequest.variables`).
                        if !variables.is_empty() {
                            for event in self.propagated_updates(
                                instance_key,
                                boundary_scope,
                                variables,
                                true,
                            ) {
                                self.emit(&mut log, event);
                            }
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

            Command::UpdateJobRetries {
                job_key,
                retries,
                operation_reference,
            } => {
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
                        operation_reference,
                    },
                );
            }

            Command::UpdateJobTimeout {
                job_key,
                timeout,
                operation_reference,
            } => {
                let job = self
                    .state
                    .jobs
                    .get(&job_key)
                    .ok_or(EngineError::JobNotFound { job_key })?;
                // Only a currently-locked (Activated) job has a lock to extend.
                // A Created/terminal job has no active deadline to reset.
                if job.state != state::JobState::Activated {
                    return Err(EngineError::JobNotActive { job_key });
                }
                let instance_key = job.instance_key;
                let deadline = now.saturating_add(timeout);
                self.emit(
                    &mut log,
                    Event::JobTimeoutUpdated {
                        job_key,
                        instance_key,
                        deadline,
                        operation_reference,
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
                    | state::IncidentKind::ExpressionEvaluation
                    | state::IncidentKind::DecisionEvaluation => {
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
                local,
            } => {
                let instance_key = self
                    .resolve_scope(scope_key)
                    .ok_or(EngineError::ScopeNotFound { scope_key })?;
                if !variables.is_empty() {
                    // Resolve the requested scope within the instance and write:
                    // `local` keeps the values in that scope; otherwise they
                    // propagate to the nearest ancestor defining each name, else
                    // root. A root-scoped (or root-only-instance) write collapses
                    // to the flat `VariablesUpdated`.
                    for event in self.propagated_updates(instance_key, scope_key, variables, local)
                    {
                        self.emit(&mut log, event);
                    }
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
                // Split user tasks into those that must run a `canceling` listener
                // chain before cancellation (deferred) and those cancelled at once
                // (ADR 0037 §6). A task already deferring another transition is
                // cancelled immediately (its in-flight listener job is cancelled
                // with the other jobs above). Owned tuples so we can call
                // `task_listeners_of` (which borrows `self`) after this.
                let user_task_infos: Vec<(Key, Key, ElementId, bool)> = user_tasks
                    .iter()
                    .map(|t| {
                        (
                            t.key,
                            t.element_instance_key,
                            t.element_id.clone(),
                            t.pending.is_none(),
                        )
                    })
                    .collect();
                let mut immediate_user_task_cancels: Vec<Event> = Vec::new();
                let mut canceling_starts: Vec<(Key, Key, ElementId, crate::model::TaskListener)> =
                    Vec::new();
                for (key, element_instance_key, element_id, no_pending) in user_task_infos {
                    let canceling = if no_pending {
                        self.task_listeners_of(
                            instance_key,
                            &element_id,
                            crate::model::TaskListenerEventType::Canceling,
                        )
                    } else {
                        Vec::new()
                    };
                    if let Some(first) = canceling.first().cloned() {
                        canceling_starts.push((key, element_instance_key, element_id, first));
                    } else {
                        immediate_user_task_cancels.push(Event::UserTaskCanceled {
                            user_task_key: key,
                            instance_key,
                        });
                    }
                }

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
                for event in immediate_user_task_cancels {
                    self.emit(&mut log, event);
                }
                if canceling_starts.is_empty() {
                    // No canceling listeners: terminate synchronously, exactly as
                    // the pre-task-listener engine did (byte-identical).
                    self.emit(&mut log, Event::ProcessInstanceTerminated { instance_key });
                } else {
                    // Defer termination: run each user task's canceling chain; the
                    // last one to drain emits `ProcessInstanceTerminated`.
                    self.emit(&mut log, Event::ProcessInstanceTerminating { instance_key });
                    for (key, element_instance_key, element_id, first) in canceling_starts {
                        let pending = state::PendingUserTaskTransition {
                            event_type: crate::model::TaskListenerEventType::Canceling,
                            assignee: None,
                            update: None,
                            variables: std::collections::HashMap::new(),
                            corrections: crate::model::UserTaskCorrections::default(),
                        };
                        for event in self.start_task_listener_chain(
                            key,
                            instance_key,
                            element_instance_key,
                            &element_id,
                            crate::model::TaskListenerEventType::Canceling,
                            pending,
                            &first,
                        ) {
                            self.emit(&mut log, event);
                        }
                    }
                }
            }

            Command::ModifyInstance {
                instance_key,
                activate_instructions,
                terminate_instructions,
            } => {
                // Only an active instance can be modified (Zeebe parity).
                match self.state.instances.get(&instance_key) {
                    Some(instance) if instance.state == ProcessInstanceState::Active => {}
                    _ => return Err(EngineError::InstanceNotFound { instance_key }),
                }

                // Validate up front so the command is all-or-nothing: every
                // activate element id must exist in the process definition and
                // every terminate key must be a currently-active element instance.
                for a in &activate_instructions {
                    let exists = self
                        .process_of_instance(instance_key)
                        .map(|p| p.element(&a.element_id).is_some())
                        .unwrap_or(false);
                    if !exists {
                        return Err(EngineError::ElementNotFound {
                            instance_key,
                            element_id: a.element_id.clone(),
                        });
                    }
                }
                for &eik in &terminate_instructions {
                    let active = self
                        .state
                        .instances
                        .get(&instance_key)
                        .map(|i| i.active.contains_key(&eik))
                        .unwrap_or(false);
                    if !active {
                        return Err(EngineError::ElementInstanceNotFound {
                            instance_key,
                            element_instance_key: eik,
                        });
                    }
                }

                // Apply terminations first (deterministic key order), then merge
                // any global variables and queue the activations at the process
                // root scope.
                let mut terminate: Vec<Key> = terminate_instructions;
                terminate.sort_unstable();
                terminate.dedup();
                for eik in terminate {
                    // The eik was validated against `instance.active` above, so
                    // its element id must resolve. Skip defensively rather than
                    // emitting a termination with an empty element_id, which
                    // would corrupt downstream element aggregates.
                    let Some(element_id) = self.element_id_of_instance(instance_key, eik) else {
                        continue;
                    };
                    self.terminate_element_instance(&mut log, instance_key, eik, &element_id);
                }

                for a in &activate_instructions {
                    if !a.variables.is_empty() {
                        for event in self.propagated_updates(
                            instance_key,
                            instance_key,
                            a.variables.clone(),
                            false,
                        ) {
                            self.emit(&mut log, event);
                        }
                    }
                    queue.push_back(Step::Activate {
                        instance_key,
                        element_id: a.element_id.clone(),
                        scope: 0,
                    });
                }

                // If the terminations drained the last token and nothing was
                // activated to replace it, the instance is terminated (Zeebe
                // modify semantics) rather than being auto-completed by
                // `complete_finished_instances`.
                let drained = self
                    .state
                    .instances
                    .get(&instance_key)
                    .map(|i| i.active.is_empty())
                    .unwrap_or(true);
                if drained && activate_instructions.is_empty() {
                    self.emit(&mut log, Event::ProcessInstanceTerminated { instance_key });
                }
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

            Command::ActivateAdHocActivities {
                ad_hoc_instance_key,
                activate_elements,
                cancel_remaining,
            } => {
                // External (non-agent-job) ad-hoc activation (#614 gap 3, Zeebe
                // `AdHocSubProcessInstructionActivateProcessor`). Resolve the
                // owning process instance + the container's element id from the
                // element-instance key the caller supplied
                // (`adHocSubProcessInstanceKey`); an unknown/inactive key is
                // rejected NOT_FOUND.
                //
                // An empty activation with no cancel is a no-op the caller never
                // means: it would let `enqueue_adhoc_turn` implicitly complete a
                // parked container (it completes when `already_active + requested
                // == 0`), so an external client could accidentally finish an
                // instance by POSTing `{ "elements": [] }`. Only the agent-job
                // completion seam (#614 gap 4) may end a turn by activating
                // nothing; the external command rejects it as INVALID_ARGUMENT
                // (Zeebe parity). Completion via this command is only expressible
                // through `cancelRemainingInstances`.
                if activate_elements.is_empty() && !cancel_remaining {
                    return Err(EngineError::AdHocNoActivationTargets {
                        ad_hoc_instance_key,
                    });
                }
                let container_key = ad_hoc_instance_key;
                let (instance_key, element_id) = self
                    .state
                    .instances
                    .iter()
                    .find_map(|(ik, inst)| {
                        inst.adhoc_instances
                            .get(&container_key)
                            .map(|a| (*ik, a.element_id.clone()))
                    })
                    .ok_or(EngineError::AdHocSubProcessNotFound {
                        ad_hoc_instance_key,
                    })?;
                let def = self.adhoc_def_of(instance_key, &element_id).ok_or(
                    EngineError::AdHocSubProcessNotFound {
                        ad_hoc_instance_key,
                    },
                )?;
                // Same target validation as the agent-job path (unknown id →
                // NOT_FOUND, atomic), then drive the same activation turn.
                Self::validate_adhoc_activation_targets(&def, instance_key, &activate_elements)?;
                self.enqueue_adhoc_turn(
                    &mut queue,
                    instance_key,
                    container_key,
                    activate_elements,
                    cancel_remaining,
                );
            }
        }

        self.run(&mut log, queue);
        self.complete_finished_instances(&mut log);
        // Follower-only safety net: if a retirement digest raced ahead of an
        // instance's create on this replica (leader-durable async-learner lag), the
        // key was tombstoned; now that the create has materialized the instance,
        // reap it immediately so it cannot linger as a never-retired `Active` shell.
        // Near-free when nothing is pending (a leader never tombstones).
        if !self.retired_tombstones.is_empty() {
            let created: Vec<Key> = log
                .iter()
                .filter_map(|e| match e {
                    Event::ProcessInstanceCreated { instance_key, .. } => Some(*instance_key),
                    _ => None,
                })
                .collect();
            if !created.is_empty() {
                self.reap_tombstoned(&created);
            }
        }
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
        // instance before its token advances, propagating from the catching
        // element's enclosing (flow) scope upward (nearest defining ancestor,
        // else root). Root-only instances collapse to the flat `VariablesUpdated`.
        if !variables.is_empty() {
            let flow_scope = self.scope_of(instance_key, element_instance_key);
            for event in self.propagated_updates(instance_key, flow_scope, variables.clone(), false)
            {
                self.emit(log, event);
            }
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
            // Signal payload propagates from the catching element's enclosing
            // (flow) scope upward, defaulting to root.
            let flow_scope = self.scope_of(instance_key, element_instance_key);
            for event in self.propagated_updates(instance_key, flow_scope, variables.clone(), false)
            {
                self.emit(log, event);
            }
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
                if !has_child && self.active_job_on(*eik).is_none() {
                    // A sub-process resting in COMPLETING while its `end`
                    // execution-listener chain runs (ADR 0037) has also drained its
                    // children, but carries a parked listener job on its own
                    // instance — the only kind of job a sub-process instance can
                    // host, since it creates none of its own. Skip it so the sweep
                    // does not re-fire its completion; `finalize_subprocess` emits
                    // the deferred `ElementCompleted` when the chain drains.
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

            // A drained sub-process whose enclosing scope is a multi-instance body
            // (with a matching element id) is a MULTI-INSTANCE CHILD: its
            // completion must feed the loop's output collection + join / next
            // child rather than take the activity's outgoing flow. `complete`
            // detects this and delegates to `complete_mi_child`, so route it
            // there via `Step::Complete` (which emits its `ElementCompleted` and
            // tears down the child scope). Cancel any boundary events armed on the
            // child first — the MI completion path does not follow the normal
            // boundary-cleanup branch below. Its `output_element` (the MI output),
            // not the sub-process's `zeebe:ioMapping` outputs, is what aggregates.
            let is_mi_child = self
                .state
                .instances
                .get(&instance_key)
                .and_then(|i| i.multi_instances.get(&scope))
                .map(|mi| mi.element_id == element_id)
                .unwrap_or(false);
            if is_mi_child {
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
                followups.push(Step::Complete {
                    instance_key,
                    element_instance_key: eik,
                    element_id,
                });
                continue;
            }

            // Output mappings on a sub-process evaluate against the sub-process's
            // own scope view (its input-mapped locals + anything set inside it)
            // BEFORE its scope is torn down, then propagate the mapped result to
            // the enclosing (parent) scope. Capture the values now; emit them as
            // scoped writes after `ElementCompleted` has dropped the local scope.
            let outputs = self.io_outputs(instance_key, &element_id);
            let output_updates = if outputs.is_empty() {
                HashMap::new()
            } else {
                let visible = self.variables_for_element(instance_key, eik);
                self.eval_io_mappings_in(&visible, &outputs)
            };
            self.emit(
                log,
                Event::ElementCompleting {
                    instance_key,
                    element_instance_key: eik,
                    element_id: element_id.clone(),
                },
            );

            // End-listener gate (ADR 0037): the sub-process rests in COMPLETING
            // while its `end` chain runs. Its boundary events disarm and its
            // output mappings project at completing-time (Zeebe: before the
            // listeners); `finalize_subprocess` emits the deferred
            // `ElementCompleted` + outgoing flows once the chain drains. The
            // parked listener job keeps the sub-process instance active, and the
            // sweep guard above skips it so it is not re-detected as drained.
            // Only build the (cloned) listener variable view when the sub-process
            // actually declares `end` listeners — the listener-free path stays
            // allocation-free, matching the pre-listener engine operationally.
            let has_end_listeners = !self
                .listeners_of(
                    instance_key,
                    &element_id,
                    crate::model::ListenerEventType::End,
                )
                .is_empty();
            if has_end_listeners {
                let mut listener_vars = (*self.variables_for_element(instance_key, eik)).clone();
                // Zeebe runs `end` listeners after output mappings, so the first
                // listener resolves its own FEEL (job type / retries) against the
                // mapped values — the same post-output view every subsequent
                // listener sees (they re-read the scope after the propagation
                // below is applied).
                listener_vars.extend(output_updates.clone());
                if let Some(job) = self.begin_end_listener_chain(
                    instance_key,
                    eik,
                    &element_id,
                    scope,
                    &listener_vars,
                ) {
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
                    if !output_updates.is_empty() {
                        for event in
                            self.propagated_updates(instance_key, scope, output_updates, false)
                        {
                            self.emit(log, event);
                        }
                    }
                    self.emit(log, job);
                    continue;
                }
            }

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
            if !output_updates.is_empty() {
                for event in self.propagated_updates(instance_key, scope, output_updates, false) {
                    self.emit(log, event);
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
            Step::ActivateAdHocTool {
                instance_key,
                container_key,
                element_id,
                variables,
            } => self.activate_adhoc_tool(instance_key, container_key, element_id, variables),
            Step::CompleteAdHoc {
                instance_key,
                container_key,
                cancel,
            } => self.complete_adhoc_container(instance_key, container_key, cancel),
            Step::AdvanceListener {
                instance_key,
                element_instance_key,
                element_id,
                event_type,
                index,
                scope,
            } => self.advance_listener(
                instance_key,
                element_instance_key,
                element_id,
                event_type,
                index,
                scope,
            ),
            Step::AdvanceTaskListener {
                user_task_key,
                event_type,
                index,
            } => self.advance_task_listener(user_task_key, event_type, index),
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

        // Input mappings (zeebe:input): evaluate against the variables visible to
        // the activating element and create the mapped values LOCAL to the
        // element's own scope (Zeebe semantics) — visible to the element's job or
        // inner flow, not propagated to the parent, and dropped when the element
        // completes. A sub-process is itself a variable scope: it always registers
        // its scope (so writes inside it can resolve/propagate correctly) and its
        // inputs are local to that scope, exactly like a leaf activity.
        let is_sub_process = matches!(kind, Some(ElementKind::SubProcess { .. }));
        // An ad-hoc sub-process container (a job-bearing ServiceTask that appears
        // in the definition's ad-hoc catalog): like a sub-process it is a variable
        // scope — its activated tool children run inside it and its
        // `outputCollection` accumulates there — so it always registers a scope
        // (ADR 0023 seam 2).
        let adhoc_def = self.adhoc_def_of(instance_key, &element_id);
        let is_adhoc_container = adhoc_def.is_some();
        let inputs = self.io_inputs(instance_key, &element_id);

        // The scoped variable view the activating element evaluates against: both
        // its input-mapping *source* expressions and its own FEEL attributes (job
        // type, retries, priority, timer/message/signal name, correlation key,
        // user-task fields). It is the element's enclosing flow scope — a
        // root-scope element gets the shared root Arc (a cheap refcount bump), so
        // evaluation is byte-identical to the flat engine; an element inside a
        // sub-process or multi-instance body additionally sees its enclosing
        // scope's locals, so an input mapping that reads an enclosing-scope-local
        // variable resolves correctly (Zeebe parity). A sub-process's own inputs
        // are evaluated here against its *parent* scope and then applied local to
        // the new sub-process scope below.
        let element_vars = self.variables_for_element(instance_key, scope);

        let mut scope_registered = false;
        if is_sub_process || is_adhoc_container {
            events.push(Event::VariableScopeCreated {
                instance_key,
                scope_key: element_instance_key,
                parent_scope_key: scope,
            });
            scope_registered = true;
        }
        // Seed the ad-hoc container's `outputCollection` to an empty array as a
        // local variable the moment it activates (Zeebe
        // `AdHocSubProcessProcessor.onActivate`), BEFORE the container's own input
        // mappings run — so the agent can read the growing collection mid-run, and
        // a mis-mapped non-array target is observable to the append-time type
        // guard. This variable is the single source of truth for the accumulated
        // tool outputs; `complete_adhoc_tool` appends to it and
        // `complete_adhoc_container` propagates it outward.
        if let Some(name) = adhoc_def.as_ref().and_then(|d| d.output_collection.clone()) {
            events.push(Event::ScopedVariablesUpdated {
                instance_key,
                scope_key: element_instance_key,
                variables: HashMap::from([(name, Value::List(Vec::new()))]),
            });
        }
        if !inputs.is_empty() {
            let updates = self.eval_io_mappings_in(&element_vars, &inputs);
            if !updates.is_empty() {
                if !scope_registered {
                    events.push(Event::VariableScopeCreated {
                        instance_key,
                        scope_key: element_instance_key,
                        parent_scope_key: scope,
                    });
                }
                events.push(Event::ScopedVariablesUpdated {
                    instance_key,
                    scope_key: element_instance_key,
                    variables: updates,
                });
            }
        }

        // Start execution listeners (ADR 0037): before the element enacts its
        // own behaviour (creating a job, routing a gateway, opening a
        // sub-process) it runs a sequential chain of `start` listener jobs. The
        // element rests in ACTIVATING — `ElementActivated` was already emitted
        // above (nano's early-marker choice), but the *behaviour* is deferred
        // until the chain drains (see `advance_listener`). Listener-free
        // elements skip this entirely and fall straight through to
        // `run_activation_body`, so their journal is byte-identical.
        let start_listeners = self.listeners_of(
            instance_key,
            &element_id,
            crate::model::ListenerEventType::Start,
        );
        if let Some(first) = start_listeners.first() {
            let mut listener_vars = (*element_vars).clone();
            if !inputs.is_empty() {
                let updates = self.eval_io_mappings_in(&element_vars, &inputs);
                listener_vars.extend(updates);
            }
            let job_key = self.mint_key();
            let job_type = self.resolve_job_type(&listener_vars, &first.job_type);
            let retries = self.resolve_retries(&listener_vars, first.retries.as_deref());
            events.push(Event::ExecutionListenerJobCreated {
                job_key,
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
                job_type,
                event_type: crate::model::ListenerEventType::Start,
                listener_index: 0,
                scope,
                created_at: self.now,
                retries,
            });
            return (events, followups);
        }

        let (kind_events, kind_followups) =
            self.run_activation_body(instance_key, element_id, element_instance_key, scope);
        events.extend(kind_events);
        followups.extend(kind_followups);
        (events, followups)
    }

    /// Runs an element's own activation behaviour — everything after
    /// `ElementActivated`: create the service/agent/user-task job, route the
    /// gateway, open the sub-process/ad-hoc scope, arm boundary events, or
    /// schedule an immediate `Complete`. Split out of [`activate`] so it can be
    /// deferred until the element's `start` execution-listener chain drains
    /// (ADR 0037). Listener-free elements call it inline with the same key/event
    /// ordering, so their journal is unchanged.
    fn run_activation_body(
        &mut self,
        instance_key: Key,
        element_id: String,
        element_instance_key: Key,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let kind = self.element_kind(instance_key, &element_id);
        let adhoc_def = self.adhoc_def_of(instance_key, &element_id);
        let element_vars = self.variables_for_element(instance_key, scope);
        let mut events: Vec<Event> = Vec::new();
        let mut followups: Vec<Step> = Vec::new();

        match kind {
            // A service task creates a job and parks the token.
            Some(ElementKind::ServiceTask {
                job_type, priority, ..
            }) => {
                // A declarative ad-hoc container (Camunda BPMN_TASK /
                // `activeElementsCollection`) is NOT a job worker: on activation
                // it evaluates the FEEL collection to the inner element ids and
                // activates them directly — no job is minted, and the container
                // completes once those elements drain (ADR 0023 v1.1;
                // `AdHocSubProcessProcessor.readActivateElementsCollection`). The
                // agentic JOB_WORKER variant keeps the job-worker path below.
                let declarative_adhoc = adhoc_def
                    .as_ref()
                    .map(|d| d.impl_type == crate::model::AdHocImplementationType::BpmnTask)
                    .unwrap_or(false);
                if declarative_adhoc {
                    let def = adhoc_def
                        .as_ref()
                        .expect("declarative_adhoc implies adhoc_def is Some");
                    events.push(Event::AdHocActivated {
                        instance_key,
                        container_key: element_instance_key,
                        element_id: element_id.clone(),
                        output_collection: def.output_collection.clone(),
                        output_element: def.output_element.clone(),
                    });
                    let ids = def
                        .active_elements_collection
                        .as_deref()
                        .map(|expr| self.eval_adhoc_active_elements(expr, &element_vars, def))
                        .unwrap_or_default();
                    for id in &ids {
                        followups.push(Step::ActivateAdHocTool {
                            instance_key,
                            container_key: element_instance_key,
                            element_id: id.clone(),
                            variables: HashMap::new(),
                        });
                    }
                    // An empty collection has nothing to run: the container
                    // completes at once, exactly as Camunda completes an ad-hoc
                    // sub-process whose active-elements collection is empty.
                    if ids.is_empty() {
                        followups.push(Step::CompleteAdHoc {
                            instance_key,
                            container_key: element_instance_key,
                            cancel: false,
                        });
                    }
                    events.extend(self.arm_boundary_events(
                        instance_key,
                        element_instance_key,
                        scope,
                        &element_id,
                    ));
                    return (events, followups);
                }
                let job_key = self.mint_key();
                let job_type = self.resolve_job_type(&element_vars, &job_type);
                let priority = self.resolve_priority(&element_vars, priority.as_deref());
                let retries = self.resolve_retries(
                    &element_vars,
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
                // An ad-hoc container: register its runtime state alongside the
                // agent job. The container element instance is the ad-hoc scope
                // (created above); its tool children are activated by the agent's
                // activate-element instructions on job completion (ADR 0023).
                if let Some(def) = &adhoc_def {
                    events.push(Event::AdHocActivated {
                        instance_key,
                        container_key: element_instance_key,
                        element_id: element_id.clone(),
                        output_collection: def.output_collection.clone(),
                        output_element: def.output_element.clone(),
                    });
                    // Advertise the tool catalog to the agent (Camunda
                    // `AdHocSubProcessProcessor.onActivate` writes the local var
                    // `adHocSubProcessElements`): a list of `{ elementId,
                    // elementName }`, one per activatable tool in document order,
                    // so the agent can discover which tools it may activate. It is
                    // written LOCAL to the container scope (registered in
                    // `activate`), so it rides the agent job's variable snapshot
                    // without leaking to the parent instance. Per-tool
                    // documentation / `zeebe:properties` / `fromAi` parameter
                    // schema are not parsed by nano yet (deferred; see #614).
                    let entries: Vec<Value> = def
                        .tools
                        .iter()
                        .filter(|t| {
                            // Only advertise activatable ad-hoc tools. The parser
                            // captures known non-activatable inner nodes (e.g.
                            // gateways) as `AdHocToolKind::Other`; excluding them
                            // keeps the advertised catalog at Zeebe parity so the
                            // agent never sees entries it cannot activate.
                            !matches!(t.kind, crate::model::AdHocToolKind::Other)
                        })
                        .map(|t| {
                            Value::Map(
                                [
                                    ("elementId".to_string(), Value::Str(t.element_id.clone())),
                                    ("elementName".to_string(), Value::Str(t.name.clone())),
                                ]
                                .into_iter()
                                .collect(),
                            )
                        })
                        .collect();
                    let mut catalog_var = HashMap::new();
                    catalog_var.insert("adHocSubProcessElements".to_string(), Value::List(entries));
                    events.push(Event::ScopedVariablesUpdated {
                        instance_key,
                        scope_key: element_instance_key,
                        variables: catalog_var,
                    });
                }
                // Arm timers/subscriptions for every attached boundary event.
                events.extend(self.arm_boundary_events(
                    instance_key,
                    element_instance_key,
                    scope,
                    &element_id,
                ));
            }
            // A user task creates a user task record and parks the token; a
            // CompleteUserTask releases it. The assignment/scheduling/priority
            // expressions declared on the element are resolved against the
            // element's scoped view at creation time.
            Some(ElementKind::UserTask(props)) => {
                let user_task_key = self.mint_key();
                let assignee = self
                    .resolve_user_task_string(&element_vars, props.assignee.as_deref())
                    .filter(|s| !s.is_empty());
                let candidate_groups =
                    self.resolve_user_task_list(&element_vars, props.candidate_groups.as_deref());
                let candidate_users =
                    self.resolve_user_task_list(&element_vars, props.candidate_users.as_deref());
                let due_date = self
                    .resolve_user_task_string(&element_vars, props.due_date.as_deref())
                    .filter(|s| !s.is_empty());
                let follow_up_date = self
                    .resolve_user_task_string(&element_vars, props.follow_up_date.as_deref())
                    .filter(|s| !s.is_empty());
                let priority = self.resolve_priority(&element_vars, props.priority.as_deref());
                // An initial assignee (zeebe:assignmentDefinition) must fire the
                // `assigning` listeners exactly as a runtime assign does (Zeebe
                // parity: the assignee is stripped off the CREATED record and
                // routed through an assigning transition once the task is
                // available). With no assigning listeners the assignee stays on
                // CREATED, keeping listener-free/plain-assignee tasks
                // byte-identical.
                let has_assigning_listeners = !self
                    .task_listeners_of(
                        instance_key,
                        &element_id,
                        crate::model::TaskListenerEventType::Assigning,
                    )
                    .is_empty();
                let route_initial_assignee = assignee.is_some() && has_assigning_listeners;
                let (created_assignee, deferred_initial_assignee) = if route_initial_assignee {
                    (None, assignee)
                } else {
                    (assignee, None)
                };
                events.push(Event::UserTaskCreated {
                    user_task_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                    created_at: self.now,
                    assignee: created_assignee,
                    candidate_groups,
                    candidate_users,
                    due_date,
                    follow_up_date,
                    priority,
                });
                // Creating task listeners (ADR 0037 §6): the task record exists
                // but is not yet available for work until the creating chain
                // drains. The pending transition blocks assign/complete/update
                // meanwhile. Listener-free user tasks skip this entirely.
                let creating = self.task_listeners_of(
                    instance_key,
                    &element_id,
                    crate::model::TaskListenerEventType::Creating,
                );
                if let Some(first) = creating.first().cloned() {
                    let pending = state::PendingUserTaskTransition {
                        event_type: crate::model::TaskListenerEventType::Creating,
                        // Carried through the creating chain, then routed into an
                        // assigning transition when creating drains.
                        assignee: deferred_initial_assignee,
                        update: None,
                        variables: std::collections::HashMap::new(),
                        corrections: crate::model::UserTaskCorrections::default(),
                    };
                    events.extend(self.start_task_listener_chain(
                        user_task_key,
                        instance_key,
                        element_instance_key,
                        &element_id,
                        crate::model::TaskListenerEventType::Creating,
                        pending,
                        &first,
                    ));
                } else if let Some(initial) = deferred_initial_assignee {
                    // No creating listeners: the task is available at once, so the
                    // initial assignee's assigning transition starts immediately.
                    let assigning = self.task_listeners_of(
                        instance_key,
                        &element_id,
                        crate::model::TaskListenerEventType::Assigning,
                    );
                    if let Some(first) = assigning.first().cloned() {
                        let pending = state::PendingUserTaskTransition {
                            event_type: crate::model::TaskListenerEventType::Assigning,
                            assignee: Some(initial),
                            update: None,
                            variables: std::collections::HashMap::new(),
                            corrections: crate::model::UserTaskCorrections::default(),
                        };
                        events.extend(self.start_task_listener_chain(
                            user_task_key,
                            instance_key,
                            element_instance_key,
                            &element_id,
                            crate::model::TaskListenerEventType::Assigning,
                            pending,
                            &first,
                        ));
                    }
                }
                events.extend(self.arm_boundary_events(
                    instance_key,
                    element_instance_key,
                    scope,
                    &element_id,
                ));
            }
            // A timer intermediate catch event arms a timer and parks the token;
            // a clock tick (TriggerTimers) releases it once the timer is due.
            Some(ElementKind::TimerIntermediateCatchEvent { duration_millis }) => {
                let timer_key = self.mint_key();
                let timer_def = self.timer_def_of(instance_key, &element_id);
                let (due_at, _) = self.resolve_timer(
                    &element_vars,
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
                let message_name = self.resolve_event_name(&element_vars, &message_name);
                let correlation_value =
                    self.resolve_correlation_value(&element_vars, &correlation_key);
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
                let signal_name = self.resolve_event_name(&element_vars, &signal_name);
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
                    scope,
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
            // The multi-instance body is a variable scope: its children run inside
            // it and its `outputCollection` accumulates here.
            Event::VariableScopeCreated {
                instance_key,
                scope_key: body_key,
                parent_scope_key: scope,
            },
        ];
        // Input mappings on the activity apply once, on body activation, LOCAL to
        // the body scope (Zeebe semantics), so they feed the input collection and
        // the children without leaking to the parent scope. Their source
        // expressions are evaluated against the body's *enclosing* scope view, so
        // a multi-instance activity nested in a sub-process can read that
        // sub-process's locals.
        let enclosing_vars = self.variables_for_element(instance_key, scope);
        let inputs = self.io_inputs(instance_key, &element_id);
        let input_updates = if inputs.is_empty() {
            HashMap::new()
        } else {
            self.eval_io_mappings_in(&enclosing_vars, &inputs)
        };
        if !input_updates.is_empty() {
            events.push(Event::ScopedVariablesUpdated {
                instance_key,
                scope_key: body_key,
                variables: input_updates.clone(),
            });
        }
        // The input collection is evaluated in the body scope view (the enclosing
        // scope with any body input mappings overlaid).
        let vars: Arc<HashMap<String, Value>> = {
            if input_updates.is_empty() {
                enclosing_vars
            } else {
                let mut merged = (*enclosing_vars).clone();
                merged.extend(input_updates);
                Arc::new(merged)
            }
        };
        let items: Vec<Value> = match crate::feel::eval(&mi.input_collection, &vars) {
            Ok(Value::List(list)) => list,
            _ => Vec::new(),
        };
        let total = items.len();
        let sequential = mi.sequential;
        events.push(Event::MultiInstanceActivated {
            instance_key,
            body_key,
            element_id: element_id.clone(),
            sequential,
            items,
            input_element: mi.input_element.clone(),
            output_collection: mi.output_collection.clone(),
            output_element: mi.output_element.clone(),
            completion_condition: mi.completion_condition.clone(),
        });

        // Start-listener gate (ADR 0037): the body rests in ACTIVATING while its
        // `start` chain runs. Zeebe fires the activity's start listeners at the
        // body boundary, before any child is instantiated, so child spawning is
        // deferred to `advance_listener` (Start), which re-derives it via
        // `spawn_multi_instance_children` once the chain drains (by which point
        // the `MultiInstanceActivated` event above has been applied). Listener-free
        // bodies fan out inline, byte-identical to before.
        let start_listeners = self.listeners_of(
            instance_key,
            &element_id,
            crate::model::ListenerEventType::Start,
        );
        if let Some(first) = start_listeners.first() {
            let job_key = self.mint_key();
            let job_type = self.resolve_job_type(&vars, &first.job_type);
            let retries = self.resolve_retries(&vars, first.retries.as_deref());
            events.push(Event::ExecutionListenerJobCreated {
                job_key,
                instance_key,
                element_instance_key: body_key,
                element_id: element_id.clone(),
                job_type,
                event_type: crate::model::ListenerEventType::Start,
                listener_index: 0,
                scope,
                created_at: self.now,
                retries,
            });
            return (events, Vec::new());
        }

        let mut followups = Vec::new();
        if total == 0 {
            followups.push(Step::CompleteMiBody {
                instance_key,
                body_key,
            });
        } else if sequential {
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

    /// Fans out a multi-instance body's children (or completes an empty body),
    /// re-derived from the body record: an empty loop completes the body, a
    /// sequential loop starts its first child, a parallel loop starts one child
    /// per item. Split out of [`activate_multi_instance_body`] so the fan-out can
    /// be deferred behind the body's `start` execution-listener chain (ADR 0037)
    /// and resumed from [`advance_listener`] once it drains.
    fn spawn_multi_instance_children(&self, instance_key: Key, body_key: Key) -> Vec<Step> {
        let (element_id, sequential, total) = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.multi_instances.get(&body_key))
        {
            Some(mi) => (mi.element_id.clone(), mi.sequential, mi.items.len()),
            None => return Vec::new(),
        };
        let mut followups = Vec::new();
        if total == 0 {
            followups.push(Step::CompleteMiBody {
                instance_key,
                body_key,
            });
        } else if sequential {
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
        followups
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

        // The scoped view this child evaluates its own FEEL attributes (job type,
        // retries, priority) against: the multi-instance body's scope (already
        // applied) overlaid with the child's own `inputElement`/`loopCounter`
        // bindings (not yet applied — carried in `locals`). So a child job type
        // like `="worker-" + loopCounter` resolves correctly.
        let mut child_vars = (*self.variables_for_element(instance_key, body_key)).clone();
        child_vars.extend(locals.clone());

        // Apply the activity's own input mappings (`zeebe:input`) per-child,
        // evaluated with `inputElement`/`loopCounter` already bound, writing the
        // results LOCAL to this child's scope — matching Zeebe, which applies an
        // MI inner activity's input mappings on each instance's activation into
        // that instance's OWN scope (`getVariableScopeKey` returns the
        // element-instance key while the loop counter is set). `activate_mi_child`
        // hand-builds the child (bypassing `activate`), so unlike a
        // normally-activated element these mappings must be applied here; they are
        // then visible both to the child's job-type/retry FEEL resolution below
        // and, for a sub-process child, to its inner flow.
        let inputs = self.io_inputs(instance_key, &element_id);
        if !inputs.is_empty() {
            let mut mapped = self.eval_io_mappings_in(&child_vars, &inputs);
            // `loopCounter` is a reserved MI binding. The child's output-collection
            // index is now engine-owned runtime state (`MultiInstanceState::
            // child_indices`, read back by `complete_mi_child`), so a clobbered
            // counter can no longer misindex a child's output. We still drop any
            // user `zeebe:input` mapping targeting `loopCounter` so the FEEL-visible
            // reserved binding (job type, `outputElement`, etc.) keeps reporting the
            // true engine-owned counter rather than a mapped-over value.
            mapped.remove("loopCounter");
            child_vars.extend(mapped.clone());
            locals.extend(mapped);
        }

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
            Some(ElementKind::ServiceTask {
                job_type, priority, ..
            }) => {
                let job_key = self.mint_key();
                let job_type = self.resolve_job_type(&child_vars, &job_type);
                let priority = self.resolve_priority(&child_vars, priority.as_deref());
                let retries = self.resolve_retries(
                    &child_vars,
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
            // A multi-instance child that is an embedded SUB-PROCESS opens its own
            // token scope (this child element instance) and activates its inner
            // start event inside it — exactly like a normally-activated
            // sub-process (see `activate`). The child rests while its inner flow
            // runs; once that scope drains, `complete_drained_subprocesses`
            // recognises the drained instance as a multi-instance child and routes
            // it back into the loop via `complete_mi_child` (output collection +
            // join / next child) rather than following the activity's outgoing
            // flow. This is what makes the nested "wave" pattern — a sequential MI
            // over waves wrapping a parallel MI over a wave's tasks — executable.
            Some(ElementKind::SubProcess { start_event }) => {
                events.extend(self.arm_boundary_events(
                    instance_key,
                    child_key,
                    body_key,
                    &element_id,
                ));
                followups.push(Step::Activate {
                    instance_key,
                    element_id: start_event,
                    scope: child_key,
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
            .and_then(|i| i.multi_instances.get(&body_key))
            .and_then(|mi| mi.child_indices.get(&child_eik))
            .copied()
            // Fallback for instances rehydrated from a pre-`child_indices`
            // snapshot: derive from the child's `loopCounter` binding. Newly
            // activated children always hit the authoritative map above, so no
            // write path into the child scope can corrupt the index.
            .or_else(|| {
                self.state
                    .instances
                    .get(&instance_key)
                    .and_then(|i| i.scope_variables.get(&child_eik))
                    .and_then(|l| l.get("loopCounter"))
                    .and_then(|v| v.as_f64())
                    .map(|c| (c as i64 - 1).max(0) as usize)
            })
            .unwrap_or(0);

        // Collect this child's output (evaluated in its local scope) at its index.
        // First apply the MI element's own output mappings (`zeebe:output`) to the
        // child's local scope view, matching Zeebe: an MI inner activity's output
        // mappings are applied on each instance's completion into the instance's
        // OWN scope (`getVariableScopeKey` returns the element-instance key while
        // the loop counter is set), so `outputElement` can read them. They are NOT
        // propagated to the parent — only the aggregated `outputCollection` is. We
        // therefore overlay the mapped values onto the eval context in memory
        // rather than writing them into the (about-to-be-torn-down, non-propagated)
        // child scope.
        let output = output_element.as_deref().and_then(|expr| {
            let visible = self.variables_for_element(instance_key, child_eik);
            let outputs = self.io_outputs(instance_key, &element_id);
            if outputs.is_empty() {
                crate::feel::eval(expr, &visible).ok()
            } else {
                let mapped = self.eval_io_mappings_in(&visible, &outputs);
                let mut vars = (*visible).clone();
                vars.extend(mapped);
                crate::feel::eval(expr, &vars).ok()
            }
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

        // Build the aggregated output collection (padding uncollected slots with
        // null) and evaluate the activity's output mappings against the body-scope
        // view overlaid with that collection — all while the body scope is still
        // resident (its `ElementCompleted` below tears it down).
        let collection_map: Option<HashMap<String, Value>> =
            output_collection.map(|name| HashMap::from([(name, Value::List(output_values))]));
        let outputs = self.io_outputs(instance_key, &element_id);
        let output_updates = if outputs.is_empty() {
            HashMap::new()
        } else {
            let mut ctx = (*self.variables_for_element(instance_key, body_key)).clone();
            if let Some(map) = &collection_map {
                ctx.extend(map.clone());
            }
            self.eval_io_mappings_in(&ctx, &outputs)
        };

        let mut events = Vec::new();
        // Cancel any children still running (reached here via completion condition).
        for child in &active {
            events.extend(self.cancel_mi_child_events(instance_key, *child));
        }
        // The output collection propagates OUT of the body to its enclosing (flow)
        // scope — for a top-level loop that is the root, collapsing to the flat
        // `VariablesUpdated`, byte-identical to the pre-scoping engine.
        if let Some(map) = &collection_map {
            events.extend(self.propagated_updates(instance_key, scope, map.clone(), false));
        }
        events.push(Event::ElementCompleting {
            instance_key,
            element_instance_key: body_key,
            element_id: element_id.clone(),
        });

        // End-listener gate (ADR 0037): the multi-instance body rests in COMPLETING
        // once every child has finished (Zeebe fires the activity's `end` listeners
        // at the body boundary, not per child). Its output mappings run first
        // (Zeebe ordering: mappings before listeners); `finalize_multi_instance_body`
        // emits the deferred `ElementCompleted` + `MultiInstanceCompleted` + outgoing
        // flows once the chain drains.
        if !self
            .listeners_of(
                instance_key,
                &element_id,
                crate::model::ListenerEventType::End,
            )
            .is_empty()
        {
            let mut listener_vars = (*self.variables_for_element(instance_key, body_key)).clone();
            if let Some(map) = &collection_map {
                listener_vars.extend(map.clone());
            }
            // Zeebe runs `end` listeners after output mappings, so the first
            // listener sees the mapped values — consistent with subsequent
            // listeners, which re-read the scope after the propagation below.
            listener_vars.extend(output_updates.clone());
            if let Some(job) = self.begin_end_listener_chain(
                instance_key,
                body_key,
                &element_id,
                scope,
                &listener_vars,
            ) {
                if !output_updates.is_empty() {
                    events.extend(self.propagated_updates(
                        instance_key,
                        scope,
                        output_updates,
                        false,
                    ));
                }
                events.push(job);
                return (events, Vec::new());
            }
        }

        events.push(Event::ElementCompleted {
            instance_key,
            element_instance_key: body_key,
            element_id: element_id.clone(),
        });
        events.push(Event::MultiInstanceCompleted {
            instance_key,
            body_key,
        });
        // Output mappings likewise propagate their projected result to the parent
        // (flow) scope.
        if !output_updates.is_empty() {
            events.extend(self.propagated_updates(instance_key, scope, output_updates, false));
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

    /// Deferred completion of a multi-instance body whose `end` execution-listener
    /// chain has drained (ADR 0037). Its output collection + mappings already
    /// propagated when the body parked; this emits the parked `ElementCompleted` +
    /// `MultiInstanceCompleted` and takes the activity's outgoing flow, re-derived
    /// from the still-resident body record.
    fn finalize_multi_instance_body(
        &mut self,
        instance_key: Key,
        body_key: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let element_id = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.multi_instances.get(&body_key))
        {
            Some(mi) => mi.element_id.clone(),
            None => return (Vec::new(), Vec::new()),
        };
        let scope = self.scope_of(instance_key, body_key);
        let mut events = vec![
            Event::ElementCompleted {
                instance_key,
                element_instance_key: body_key,
                element_id: element_id.clone(),
            },
            Event::MultiInstanceCompleted {
                instance_key,
                body_key,
            },
        ];
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

    /// The ad-hoc catalog entry for `element_id` in `instance_key`'s definition,
    /// if that element is an ad-hoc sub-process container. Cloned so callers can
    /// hold it across the `&mut self` event emission that follows. This is the
    /// marker that gates all ad-hoc runtime behaviour (ADR 0023 seam 2), so the
    /// container needs no bespoke `ElementKind` variant.
    fn adhoc_def_of(
        &self,
        instance_key: Key,
        element_id: &str,
    ) -> Option<crate::model::AdHocSubProcessDef> {
        self.process_of_instance(instance_key)?
            .adhoc
            .iter()
            .find(|d| d.container_id == element_id)
            .cloned()
    }

    /// Validate that every activate-element instruction names one of the
    /// container's tools (Zeebe NOT_FOUND parity — see
    /// `AdHocSubProcessInstructionActivateProcessor` /
    /// `JobCompleteProcessor.checkAdHocSubprocessActivationTargetsAreValid`).
    /// Shared by the agent-job completion path (#614 gap 4) and the external
    /// activate-activities command (#614 gap 3) so an unknown id is rejected
    /// identically on both seams — otherwise the loop would mint a phantom child
    /// that immediately completes. Atomic: the first unknown id fails the whole
    /// batch, before any activation applies.
    fn validate_adhoc_activation_targets(
        def: &crate::model::AdHocSubProcessDef,
        instance_key: Key,
        activate_elements: &[crate::model::AdHocActivateElement],
    ) -> Result<(), EngineError> {
        for instr in activate_elements {
            if !def.tools.iter().any(|t| t.element_id == instr.element_id) {
                return Err(EngineError::AdHocUnknownElement {
                    instance_key,
                    element_id: instr.element_id.clone(),
                });
            }
        }
        Ok(())
    }

    /// Enqueue the steps for one ad-hoc activation "turn" — shared by the
    /// agent-job completion path (#614 gap 4) and the external
    /// activate-activities command (#614 gap 3). Targets must already be
    /// validated (see [`Self::validate_adhoc_activation_targets`]). With
    /// `cancel_remaining`, cancels any in-flight tools and completes the
    /// container; otherwise activates each requested tool, and — when neither a
    /// tool is already active nor one is requested this turn — completes the
    /// container (nothing more to run).
    fn enqueue_adhoc_turn(
        &self,
        queue: &mut VecDeque<Step>,
        instance_key: Key,
        container_key: Key,
        activate_elements: Vec<crate::model::AdHocActivateElement>,
        cancel_remaining: bool,
    ) {
        if cancel_remaining {
            queue.push_back(Step::CompleteAdHoc {
                instance_key,
                container_key,
                cancel: true,
            });
            return;
        }
        let already_active = self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&container_key))
            .map(|a| a.active.len())
            .unwrap_or(0);
        let requested = activate_elements.len();
        for instr in activate_elements {
            queue.push_back(Step::ActivateAdHocTool {
                instance_key,
                container_key,
                element_id: instr.element_id,
                variables: instr.variables,
            });
        }
        // With no tool active and none requested, the agent has nothing more to
        // run this turn (it signals completion, or simply returns no
        // activations) — complete the container. Otherwise the container parks
        // until its tools drain, then its agent job is re-emitted for the next
        // turn.
        if already_active + requested == 0 {
            queue.push_back(Step::CompleteAdHoc {
                instance_key,
                container_key,
                cancel: false,
            });
        }
    }

    /// The `zeebe:ioMapping` of one tool inside an ad-hoc container, read from the
    /// container's catalog (the tool element is pruned from the executable graph,
    /// so `io_inputs`/`io_outputs` — which look it up by element id — return
    /// nothing for it). Empty when the tool declares no mappings (ADR 0023 seam 4).
    fn adhoc_tool_io(
        &self,
        instance_key: Key,
        container_element_id: &str,
        tool_element_id: &str,
    ) -> crate::model::IoMapping {
        self.adhoc_def_of(instance_key, container_element_id)
            .and_then(|def| {
                def.tools
                    .iter()
                    .find(|t| t.element_id == tool_element_id)
                    .map(|t| t.io.clone())
            })
            .unwrap_or_default()
    }

    /// Evaluates a declarative ad-hoc container's `activeElementsCollection` FEEL
    /// expression to the ordered inner element ids to activate (Camunda BPMN_TASK
    /// variant; `AdHocSubProcessProcessor.readActivateElementsCollection`
    /// evaluates it as an array of strings). Only ids that exist in the
    /// container's tool catalog are kept: nano prunes inner tools from the
    /// executable graph, so an id absent from the catalog is not an activatable
    /// element — activating it would mint a phantom child. A non-list result, a
    /// non-string entry, or an unknown id is dropped here (v1.1 does not yet raise
    /// the Camunda `EXTRACT_VALUE_ERROR`/`NOT_FOUND` incident — tracked as the
    /// validation/rejection gaps).
    fn eval_adhoc_active_elements(
        &self,
        expr: &str,
        vars: &HashMap<String, Value>,
        def: &crate::model::AdHocSubProcessDef,
    ) -> Vec<String> {
        let names: std::collections::HashSet<&str> =
            def.tools.iter().map(|t| t.element_id.as_str()).collect();
        match crate::feel::eval(expr, vars) {
            Ok(Value::List(items)) => items
                .into_iter()
                .filter_map(|v| match v {
                    Value::Str(s) if names.contains(s.as_str()) => Some(s),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Activates one ad-hoc "tool" child (ADR 0023 seam 2): instantiates
    /// `element_id` inside the container scope, seeding the agent's
    /// activate-element `variables` as the child's local overlay, and marks it
    /// active in the container. A service-task tool creates a job; any other kind
    /// passes straight through to completion (which feeds the loop). v1 targets
    /// single-activity tools (service/connector tasks); richer tool sub-graphs
    /// are a deferred refinement.
    fn activate_adhoc_tool(
        &mut self,
        instance_key: Key,
        container_key: Key,
        element_id: String,
        variables: HashMap<String, Value>,
    ) -> (Vec<Event>, Vec<Step>) {
        let child_key = self.mint_key();
        // The scoped view the child evaluates its own FEEL attributes (job type,
        // retries, priority) against: the container scope overlaid with the
        // instruction's seed variables (applied via `AdHocToolActivated` below).
        let mut child_vars = (*self.variables_for_element(instance_key, container_key)).clone();
        child_vars.extend(variables.clone());

        // The container element id anchors both the tool's catalog lookups (job
        // type below, ioMapping here) and its scope.
        let container_element_id = self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&container_key))
            .map(|a| a.element_id.clone());

        // Tool input mappings (ADR 0023 seam 4): a tool's own `zeebe:ioMapping`
        // inputs are evaluated against the activating view (container scope +
        // agent seed variables) and folded into the child's local scope, exactly
        // like a leaf activity's inputs — but sourced from the container catalog
        // because the tool element is pruned from the executable graph.
        let mut local_variables = variables;
        if let Some(cid) = container_element_id.as_deref() {
            let inputs = self.adhoc_tool_io(instance_key, cid, &element_id).inputs;
            if !inputs.is_empty() {
                let input_updates = self.eval_io_mappings_in(&child_vars, &inputs);
                child_vars.extend(input_updates.clone());
                local_variables.extend(input_updates);
            }
        }

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
                scope: container_key,
            },
            Event::AdHocToolActivated {
                instance_key,
                container_key,
                child_key,
                local_variables,
            },
        ];
        let mut followups = Vec::new();
        // The tool's kind (and its job type / user-task props) comes from the
        // container's ad-hoc catalog, not the flat element graph: the parser
        // flattens an ad-hoc container to a single job activity and prunes its
        // inner tools, keeping each tool's id + kind in `ProcessDefinition.adhoc`
        // (so the executable element map — and the processos model round-trip —
        // stays identical to a plain container). A JOB_WORKER-style service-task
        // tool emits a job; a user-task tool creates a real user task and parks
        // the child until it is completed (ADR 0023 v1 scopes tools =
        // service/user tasks). Any other kind passes straight through to
        // completion (feeding the loop). v1 targets single-activity tools; the
        // catalog does not carry per-tool retries/priority for service tasks, so
        // those default (a later refinement).
        let tool_kind = container_element_id
            .as_deref()
            .and_then(|cid| self.adhoc_def_of(instance_key, cid))
            .and_then(|def| {
                def.tools
                    .iter()
                    .find(|t| t.element_id == element_id)
                    .map(|t| t.kind.clone())
            });
        match tool_kind {
            Some(crate::model::AdHocToolKind::ServiceTask { job_type }) => {
                let job_key = self.mint_key();
                let job_type = self.resolve_job_type(&child_vars, &job_type);
                let priority = self.resolve_priority(&child_vars, None);
                let retries = self.resolve_retries(&child_vars, None);
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
            // A user-task tool parks the child on a real user task, resolving its
            // assignment/scheduling/priority expressions against the activating
            // view (container scope + seed vars + ioMapping inputs) exactly like
            // an ordinary user-task activation. It stays active until
            // `CompleteUserTask`, whose completion then routes through
            // `complete_adhoc_tool` (the child is in the container's active set),
            // feeding the container's `outputElement`/loop like any other tool.
            // Task listeners declared on the tool are pruned with it, so v1 emits
            // the plain CREATED record without a listener chain.
            Some(crate::model::AdHocToolKind::UserTask(props)) => {
                let user_task_key = self.mint_key();
                let assignee = self
                    .resolve_user_task_string(&child_vars, props.assignee.as_deref())
                    .filter(|s| !s.is_empty());
                let candidate_groups =
                    self.resolve_user_task_list(&child_vars, props.candidate_groups.as_deref());
                let candidate_users =
                    self.resolve_user_task_list(&child_vars, props.candidate_users.as_deref());
                let due_date = self
                    .resolve_user_task_string(&child_vars, props.due_date.as_deref())
                    .filter(|s| !s.is_empty());
                let follow_up_date = self
                    .resolve_user_task_string(&child_vars, props.follow_up_date.as_deref())
                    .filter(|s| !s.is_empty());
                let priority = self.resolve_priority(&child_vars, props.priority.as_deref());
                events.push(Event::UserTaskCreated {
                    user_task_key,
                    instance_key,
                    element_instance_key: child_key,
                    element_id,
                    created_at: self.now,
                    assignee,
                    candidate_groups,
                    candidate_users,
                    due_date,
                    follow_up_date,
                    priority,
                });
            }
            // CallActivity / Other / an unlisted id: no job or task to run, so the
            // child passes straight through to completion, feeding the loop.
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

    /// Completes one ad-hoc tool child: records its output (the container's
    /// `outputElement` evaluated in the child's scope) into the container's
    /// accumulated results, drops it from the active set, and — once the last
    /// active tool of the turn completes — re-emits the container's agent job for
    /// the next activate-element turn (ADR 0023 seam 2).
    fn complete_adhoc_tool(
        &mut self,
        instance_key: Key,
        child_eik: Key,
        element_id: String,
        container_key: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let tool_element_id = element_id.clone();
        let mut events = vec![
            Event::ElementCompleting {
                instance_key,
                element_instance_key: child_eik,
                element_id: element_id.clone(),
            },
            Event::ElementCompleted {
                instance_key,
                element_instance_key: child_eik,
                element_id,
            },
        ];
        let (output_element, output_collection, active_now, container_element_id) = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&container_key))
        {
            Some(a) => (
                a.output_element.clone(),
                a.output_collection.clone(),
                a.active.len(),
                a.element_id.clone(),
            ),
            None => return (events, Vec::new()),
        };
        // Collect this tool's output (evaluated in its local scope) — an entry in
        // the agent's accumulated `outputCollection` memory.
        let output = output_element.as_deref().and_then(|expr| {
            let vars = self.variables_for_element(instance_key, child_eik);
            crate::feel::eval(expr, &vars).ok()
        });
        // Type guard (Zeebe `AdHocSubProcessOutputCollectionBehavior`): the
        // `outputCollection` target must be an array to append to. It is seeded to
        // `[]` on activation, so a non-array here means an input mapping (or the
        // agent) overwrote it with the wrong type. Raise an EXTRACT_VALUE_ERROR
        // incident on the container instead of silently corrupting it, and leave
        // the collection untouched (the append below is skipped).
        if output.is_some() {
            if let Some(name) = &output_collection {
                let current = self
                    .variables_for_element(instance_key, container_key)
                    .get(name)
                    .cloned();
                if matches!(current, Some(v) if !matches!(v, Value::List(_))) {
                    let incident_key = self.mint_key();
                    let reason = format!(
                        "the output collection '{name}' of ad-hoc sub-process \
                         '{container_element_id}' has the wrong type: expected an array"
                    );
                    events.push(Event::IncidentRaised {
                        incident_key,
                        instance_key,
                        element_instance_key: container_key,
                        element_id: container_element_id.clone(),
                        kind: state::IncidentKind::ExpressionEvaluation,
                        reason,
                        job_key: None,
                        created_at: self.now,
                    });
                    return (events, Vec::new());
                }
            }
        }
        events.push(Event::AdHocToolCompleted {
            instance_key,
            container_key,
            child_key: child_eik,
            output,
        });
        // Tool output mappings (ADR 0023 seam 4): project the tool's result into
        // the container scope, sourced from the container catalog (the pruned tool
        // has no element entry, so `io_outputs` cannot see it). Evaluated in the
        // child's local scope while it is still resident — its `ElementCompleted`
        // above tears the scope down only when the caller applies the events.
        let output_updates = {
            let outputs = self
                .adhoc_tool_io(instance_key, &container_element_id, &tool_element_id)
                .outputs;
            if outputs.is_empty() {
                HashMap::new()
            } else {
                let vars = self.variables_for_element(instance_key, child_eik);
                self.eval_io_mappings_in(&vars, &outputs)
            }
        };
        if !output_updates.is_empty() {
            events.extend(self.propagated_updates(
                instance_key,
                container_key,
                output_updates.clone(),
                false,
            ));
        }
        // Completion condition (ADR 0023 seam 4): a declared `<completionCondition>`
        // is evaluated after each tool completes, against the container scope
        // overlaid with the output mappings just projected into it.
        let completion_now = self
            .adhoc_def_of(instance_key, &container_element_id)
            .and_then(|def| def.completion_condition)
            .map(|cond| {
                let mut ctx = (*self.variables_for_element(instance_key, container_key)).clone();
                ctx.extend(output_updates);
                matches!(crate::feel::eval_bool(&cond, &ctx), Ok(true))
            })
            .unwrap_or(false);
        // `active_now` still counts this child (its removal above is not yet
        // applied), so the last tool of the turn is the one leaving one active.
        let others = active_now.saturating_sub(1);
        // Zeebe latches a satisfied condition (`ElementInstance
        // #isCompletionConditionFulfilled`), so a container that already deferred
        // keeps completing on drain even if a later tool no longer satisfies it.
        let already_fulfilled = self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&container_key))
            .map(|a| a.completion_condition_fulfilled)
            .unwrap_or(false);
        if completion_now || already_fulfilled {
            let cancel_remaining = self
                .adhoc_def_of(instance_key, &container_element_id)
                .map(|d| d.cancel_remaining_instances)
                .unwrap_or(true);
            if cancel_remaining {
                // `cancelRemainingInstances=true` (the BPMN default): complete now,
                // cancelling any tools still running this turn — exactly like a
                // multi-instance body's early completion.
                return (
                    events,
                    vec![Step::CompleteAdHoc {
                        instance_key,
                        container_key,
                        cancel: true,
                    }],
                );
            }
            // `cancelRemainingInstances=false`: defer container completion until no
            // active children/flows remain (Zeebe
            // `BpmnAdHocSubProcessBehavior#completionConditionFulfilled`). When the
            // last outstanding tool drains, complete without cancelling; otherwise
            // latch the fulfilment and park — no new agent turn, no further
            // activation — so the container completes as its children drain.
            if others == 0 {
                return (
                    events,
                    vec![Step::CompleteAdHoc {
                        instance_key,
                        container_key,
                        cancel: false,
                    }],
                );
            }
            if !already_fulfilled {
                events.push(Event::AdHocCompletionConditionFulfilled {
                    instance_key,
                    container_key,
                });
            }
            return (events, Vec::new());
        }
        if others == 0 {
            // The declarative (BPMN_TASK) variant activates its collection once
            // and completes when those elements drain — there is no agent to
            // re-emit a job for (ADR 0023 v1.1). The agentic (JOB_WORKER) variant
            // re-emits its job so the agent decides the next turn.
            let declarative = self
                .adhoc_def_of(instance_key, &container_element_id)
                .map(|d| d.impl_type == crate::model::AdHocImplementationType::BpmnTask)
                .unwrap_or(false);
            if declarative {
                return (
                    events,
                    vec![Step::CompleteAdHoc {
                        instance_key,
                        container_key,
                        cancel: false,
                    }],
                );
            }
            // Every tool this turn has drained: re-emit the agent job so the agent
            // inspects the accumulated results and decides the next turn (activate
            // more tools, or signal completion).
            events.push(Event::AdHocIterated {
                instance_key,
                container_key,
            });
            events.extend(self.adhoc_agent_job_events(
                instance_key,
                container_key,
                container_element_id,
            ));
        }
        (events, Vec::new())
    }

    /// Builds the `JobCreated` event that (re-)emits an ad-hoc container's agent
    /// job on the container element instance, so the agent worker activates it to
    /// inspect accumulated tool results and drive the next turn.
    fn adhoc_agent_job_events(
        &mut self,
        instance_key: Key,
        container_key: Key,
        container_element_id: String,
    ) -> Vec<Event> {
        let job_type = match self.element_kind(instance_key, &container_element_id) {
            Some(ElementKind::ServiceTask { job_type, .. }) => job_type,
            _ => return Vec::new(),
        };
        let container_vars = self.variables_for_element(instance_key, container_key);
        let job_type = self.resolve_job_type(&container_vars, &job_type);
        let retries = self.resolve_retries(
            &container_vars,
            self.retries_of(instance_key, &container_element_id)
                .as_deref(),
        );
        let priority = self.resolve_priority(&container_vars, None);
        let job_key = self.mint_key();
        vec![Event::JobCreated {
            job_key,
            instance_key,
            element_instance_key: container_key,
            element_id: container_element_id,
            job_type,
            created_at: self.now,
            priority,
            retries,
        }]
    }

    /// Completes an ad-hoc container (ADR 0023 seam 2): when `cancel`, cancels any
    /// tool children still running; writes the aggregated `outputCollection` into
    /// the enclosing (flow) scope; drops the container's runtime state; and takes
    /// the container's outgoing flow so the parent token continues.
    fn complete_adhoc_container(
        &mut self,
        instance_key: Key,
        container_key: Key,
        cancel: bool,
    ) -> (Vec<Event>, Vec<Step>) {
        let (element_id, output_collection, active) = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&container_key))
        {
            Some(a) => (
                a.element_id.clone(),
                a.output_collection.clone(),
                a.active.iter().copied().collect::<Vec<Key>>(),
            ),
            None => return (Vec::new(), Vec::new()),
        };
        let scope = self.scope_of(instance_key, container_key);

        let mut events = Vec::new();
        // Cancel any tools still running (a cancel-remaining-instances request).
        if cancel {
            for child in &active {
                events.extend(self.cancel_mi_child_events(instance_key, *child));
            }
        }
        // The output collection propagates OUT of the container to its enclosing
        // (flow) scope — for a top-level container that is the root, collapsing to
        // the flat `VariablesUpdated`. Its value is read from the container-scope
        // variable (the single source of truth, seeded on activation and appended
        // to as each tool completed), not re-assembled here.
        let collection_map = output_collection.map(|name| {
            let list = match self
                .variables_for_element(instance_key, container_key)
                .get(&name)
            {
                Some(Value::List(v)) => v.clone(),
                _ => Vec::new(),
            };
            HashMap::from([(name, Value::List(list))])
        });
        if let Some(map) = &collection_map {
            events.extend(self.propagated_updates(instance_key, scope, map.clone(), false));
        }
        events.push(Event::ElementCompleting {
            instance_key,
            element_instance_key: container_key,
            element_id: element_id.clone(),
        });

        // End-listener gate (ADR 0037): fires the container's `end` listeners at
        // the container boundary once its tools have drained.
        // `finalize_adhoc_container` emits the parked `ElementCompleted` +
        // `AdHocCompleted` + outgoing flows. Only the natural completion is gated;
        // a cancel-remaining-instances completion (`cancel`) stays inline — its
        // `cancelled` flag cannot be re-derived at drain time, and running end
        // listeners on an aborted container is not meaningful.
        if !cancel
            && !self
                .listeners_of(
                    instance_key,
                    &element_id,
                    crate::model::ListenerEventType::End,
                )
                .is_empty()
        {
            let mut listener_vars =
                (*self.variables_for_element(instance_key, container_key)).clone();
            if let Some(map) = &collection_map {
                listener_vars.extend(map.clone());
            }
            if let Some(job) = self.begin_end_listener_chain(
                instance_key,
                container_key,
                &element_id,
                scope,
                &listener_vars,
            ) {
                // Disarm any boundary events so none can fire during the
                // COMPLETING window the end chain opens (defensive/symmetric with
                // the sub-process path; a no-op when the container carries none).
                events.extend(self.cancel_boundary_timers_on(container_key));
                events.extend(self.cancel_boundary_message_subscriptions_on(container_key));
                events.extend(self.cancel_boundary_signal_subscriptions_on(container_key));
                events.extend(self.cancel_boundary_conditional_subscriptions_on(container_key));
                events.push(job);
                return (events, Vec::new());
            }
        }

        events.push(Event::ElementCompleted {
            instance_key,
            element_instance_key: container_key,
            element_id: element_id.clone(),
        });
        events.push(Event::AdHocCompleted {
            instance_key,
            container_key,
            cancelled: cancel,
        });
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

    /// Deferred completion of an ad-hoc sub-process container whose `end`
    /// execution-listener chain has drained (ADR 0037). Its output collection
    /// already propagated when the container parked; this emits the parked
    /// `ElementCompleted` + `AdHocCompleted` and takes the outgoing flow.
    fn finalize_adhoc_container(
        &mut self,
        instance_key: Key,
        container_key: Key,
        cancelled: bool,
    ) -> (Vec<Event>, Vec<Step>) {
        let element_id = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&container_key))
        {
            Some(a) => a.element_id.clone(),
            None => return (Vec::new(), Vec::new()),
        };
        let scope = self.scope_of(instance_key, container_key);
        let mut events = vec![
            Event::ElementCompleted {
                instance_key,
                element_instance_key: container_key,
                element_id: element_id.clone(),
            },
            Event::AdHocCompleted {
                instance_key,
                container_key,
                cancelled,
            },
        ];
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

        // A completing element instance that is a directly-activated ad-hoc tool
        // (its scope is an ad-hoc container and it is in that container's active
        // set): its completion feeds the container's output collection and the
        // activate-element loop rather than taking an outgoing flow. (v1 supports
        // single-activity tools; a tool that is a multi-element sub-graph is a
        // deferred refinement — see ADR 0023 §Subset.)
        if scope != 0
            && self
                .state
                .instances
                .get(&instance_key)
                .and_then(|i| i.adhoc_instances.get(&scope))
                .map(|a| a.active.contains(&element_instance_key))
                .unwrap_or(false)
        {
            return self.complete_adhoc_tool(instance_key, element_instance_key, element_id, scope);
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

        // Business rule task bound to a DMN decision: evaluate it natively now.
        // Like a script task this is a synchronous activity — its result is
        // staged under `result_variable` (or, when absent, a map output's entries
        // are spread) and merged before output mappings run. A failed evaluation
        // (unknown decision, FEEL/hit-policy error) raises a DecisionEvaluation
        // incident and leaves the element active, so resolving the incident
        // re-runs completion (mirrors the script-task/gateway pattern). On
        // success a `DecisionEvaluated` audit event is emitted for the exporter.
        let mut decision_event: Option<Event> = None;
        if let Some(ElementKind::BusinessRuleTask {
            decision_id,
            result_variable,
        }) = self.element_kind(instance_key, &element_id)
        {
            let vars = self.variables(instance_key);
            let resolved_id = self.resolve_job_type(&vars, &decision_id);
            let Some(deployed) = self.state.decisions.get(&resolved_id).cloned() else {
                let incident_key = self.mint_key();
                return (
                    vec![Event::IncidentRaised {
                        incident_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        kind: state::IncidentKind::DecisionEvaluation,
                        reason: format!(
                            "no deployed decision with id '{resolved_id}' for business rule task \
                             '{}'",
                            decision_id
                        ),
                        job_key: None,
                        created_at: self.now,
                    }],
                    Vec::new(),
                );
            };
            let result = crate::dmn::evaluate(&deployed.drg, &resolved_id, &vars);
            if let Some(failure) = &result.failure {
                let incident_key = self.mint_key();
                return (
                    vec![Event::IncidentRaised {
                        incident_key,
                        instance_key,
                        element_instance_key,
                        element_id: element_id.clone(),
                        kind: state::IncidentKind::DecisionEvaluation,
                        reason: format!(
                            "failed to evaluate decision '{}' at business rule task '{element_id}': \
                             {}",
                            failure.failed_decision_id, failure.message
                        ),
                        job_key: None,
                        created_at: self.now,
                    }],
                    Vec::new(),
                );
            }
            // Merge the output into the instance: an explicit result variable wraps
            // the output under that name; without one, a map output is spread into
            // the scope (Zeebe requires a resultVariable for a scalar output, but
            // spreading a context output is the natural no-name behaviour).
            let mut update = HashMap::new();
            match &result_variable {
                Some(name) if !name.is_empty() => {
                    update.insert(name.clone(), result.decision_output.clone());
                }
                _ => {
                    if let Value::Map(entries) = &result.decision_output {
                        for (k, v) in entries.iter() {
                            update.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
            if !update.is_empty() {
                script_update = Some(update);
            }
            decision_event = Some(Event::DecisionEvaluated {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
                decision_key: deployed.key,
                decision_id: resolved_id,
                decision_output: result.decision_output,
                evaluated_decisions: result.evaluated_decisions,
                evaluated_at: self.now,
            });
        }

        // Default behaviour: complete and take every outgoing flow (a single flow
        // for ordinary elements; all flows for a parallel split).
        //
        // End execution listeners (ADR 0037): when the element declares `end`
        // listeners, defer `ElementCompleted` and the outgoing flows. The element
        // rests in COMPLETING while a sequential chain of end-listener jobs runs;
        // the deferred completion is emitted by `finalize_completion` once the
        // chain drains (see `advance_listener`). The parked listener job keeps the
        // element active, so `complete_finished_instances` won't finish the
        // instance mid-chain. A listener-free element pushes `ElementCompleted`
        // immediately, keeping its journal byte-identical.
        let end_listeners = self.listeners_of(
            instance_key,
            &element_id,
            crate::model::ListenerEventType::End,
        );
        let mut events = vec![Event::ElementCompleting {
            instance_key,
            element_instance_key,
            element_id: element_id.clone(),
        }];
        if end_listeners.is_empty() {
            events.push(Event::ElementCompleted {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
            });
        }
        // Emit the decision-evaluation audit record (after ElementCompleted, before
        // the variables it produced are merged).
        if let Some(event) = decision_event {
            events.push(event);
        }
        // If this element was an activity guarded by interrupting boundary timers,
        // completing it normally disarms them (and any boundary message subs).
        events.extend(self.cancel_boundary_timers_on(element_instance_key));
        events.extend(self.cancel_boundary_message_subscriptions_on(element_instance_key));
        events.extend(self.cancel_boundary_signal_subscriptions_on(element_instance_key));
        events.extend(self.cancel_boundary_conditional_subscriptions_on(element_instance_key));
        // Event-based gateway deferred choice: when this completing element is
        // the catch event that won the race downstream of an event-based
        // gateway, withdraw the losing sibling catch events (cancel their armed
        // timers/subscriptions and consume their tokens) so only the winning
        // branch continues. A no-op for any element not fed by such a gateway.
        events.extend(self.withdraw_event_gateway_siblings(
            instance_key,
            element_instance_key,
            &element_id,
            scope,
        ));
        // A script task's (or business rule task's) result is merged before output
        // mappings so a `zeebe:output` can reference/remap it (Zeebe merges
        // `resultVariable` first, then applies output mappings).
        if let Some(update) = &script_update {
            events.push(Event::VariablesUpdated {
                instance_key,
                variables: update.clone(),
            });
        }
        // Output mappings (zeebe:output): evaluate against the variables visible
        // to this element instance — its own scope (including any input-mapped
        // locals) layered over the enclosing scopes, plus any job/message result
        // merged on completion, or a script task's result staged above — and merge
        // the projected result (at the root scope) before the outgoing flows.
        let outputs = self.io_outputs(instance_key, &element_id);
        if !outputs.is_empty() {
            let visible = self.variables_for_element(instance_key, element_instance_key);
            let updates = match &script_update {
                // The script result event is not applied to state until this
                // step returns, so overlay it onto the eval context by hand.
                Some(update) => {
                    let mut vars = (*visible).clone();
                    vars.extend(update.clone());
                    self.eval_io_mappings_in(&vars, &outputs)
                }
                None => self.eval_io_mappings_in(&visible, &outputs),
            };
            if !updates.is_empty() {
                // Output mappings propagate their result to the element's
                // enclosing (flow) scope and upward — each name updates the
                // nearest ancestor scope that defines it, defaulting to root.
                // The element's own scope is being torn down by `ElementCompleted`
                // (built earlier in this vec), so propagating from the parent
                // avoids writing into the dying scope. Root-only instances collapse
                // to a single flat `VariablesUpdated`, unchanged from the flat engine.
                let flow_scope = self.scope_of(instance_key, element_instance_key);
                events.extend(self.propagated_updates(instance_key, flow_scope, updates, false));
            }
        }
        let scope = self.scope_of(instance_key, element_instance_key);

        // End-listener gate: the element rests in COMPLETING. Mint the first
        // end-listener job (its type/retries resolved against the element's
        // completion-time variable view, including any script/DMN result and
        // output mappings staged above), and defer `ElementCompleted` + the
        // outgoing flows to `finalize_completion` when the chain drains.
        if let Some(first) = end_listeners.first() {
            let mut listener_vars =
                (*self.variables_for_element(instance_key, element_instance_key)).clone();
            if let Some(update) = &script_update {
                listener_vars.extend(update.clone());
            }
            let job_key = self.mint_key();
            let job_type = self.resolve_job_type(&listener_vars, &first.job_type);
            let retries = self.resolve_retries(&listener_vars, first.retries.as_deref());
            events.push(Event::ExecutionListenerJobCreated {
                job_key,
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
                job_type,
                event_type: crate::model::ListenerEventType::End,
                listener_index: 0,
                scope,
                created_at: self.now,
                retries,
            });
            return (events, Vec::new());
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

    /// Mints the first `end` execution-listener job for an element that has just
    /// entered COMPLETING (ADR 0037), if it declares any. Returns the
    /// `ExecutionListenerJobCreated` event when there is an end chain to run — the
    /// caller appends it, having already emitted `ElementCompleting` and its
    /// completing-time work (output mappings, result merge, boundary disarm), and
    /// then defers `ElementCompleted` + its structural downstream to the matching
    /// `finalize_*` (dispatched by [`finalize_end_transition`] once the chain
    /// drains). Returns `None` when the element has no end listeners, so the
    /// caller completes inline and its journal stays byte-identical.
    ///
    /// `vars` is the element's completion-time variable view (its own scope,
    /// including any input-mapped locals and merged job/script/DMN result), which
    /// the listener job's own FEEL attributes (`type`, `retries`) resolve against.
    fn begin_end_listener_chain(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: &str,
        scope: Key,
        vars: &HashMap<String, Value>,
    ) -> Option<Event> {
        let end_listeners = self.listeners_of(
            instance_key,
            element_id,
            crate::model::ListenerEventType::End,
        );
        let first = end_listeners.first()?;
        let job_key = self.mint_key();
        let job_type = self.resolve_job_type(vars, &first.job_type);
        let retries = self.resolve_retries(vars, first.retries.as_deref());
        Some(Event::ExecutionListenerJobCreated {
            job_key,
            instance_key,
            element_instance_key,
            element_id: element_id.to_string(),
            job_type,
            event_type: crate::model::ListenerEventType::End,
            listener_index: 0,
            scope,
            created_at: self.now,
            retries,
        })
    }

    /// Dispatches the deferred completion of an element whose `end`
    /// execution-listener chain has drained (ADR 0037) to the finalizer matching
    /// the completion site that parked it. The parked element still rests in
    /// COMPLETING with its scope resident, so each finalizer re-derives its
    /// structural tail (`ElementCompleted` + the site's own downstream) from the
    /// current state — no captured context, replay-safe, exactly like the `start`
    /// side re-derives [`run_activation_body`]. Ordinary elements (service/user/
    /// script/business-rule tasks, pass-through events) fall through to
    /// [`finalize_completion`].
    fn finalize_end_transition(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        // A multi-instance body (its own instance key is the body scope key).
        if self
            .state
            .instances
            .get(&instance_key)
            .map(|i| i.multi_instances.contains_key(&element_instance_key))
            .unwrap_or(false)
        {
            return self.finalize_multi_instance_body(instance_key, element_instance_key);
        }
        // An ad-hoc sub-process container (keyed by its own instance key).
        if self
            .state
            .instances
            .get(&instance_key)
            .map(|i| i.adhoc_instances.contains_key(&element_instance_key))
            .unwrap_or(false)
        {
            return self.finalize_adhoc_container(instance_key, element_instance_key, false);
        }
        match self.element_kind(instance_key, &element_id) {
            Some(ElementKind::ExclusiveGateway) => self.finalize_exclusive_gateway(
                instance_key,
                element_instance_key,
                element_id,
                scope,
            ),
            Some(ElementKind::SubProcess { .. }) => {
                self.finalize_subprocess(instance_key, element_instance_key, element_id, scope)
            }
            _ => self.finalize_completion(instance_key, element_instance_key, element_id, scope),
        }
    }

    /// Emits the deferred completion of an element whose `end` execution-listener
    /// chain has drained (ADR 0037): `ElementCompleted` followed by taking every
    /// outgoing flow. The completing-time work (boundary disarm, result merge,
    /// output mappings) already ran in [`complete`]; this is only the tail that
    /// was parked behind the listener chain.
    fn finalize_completion(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let mut events = vec![Event::ElementCompleted {
            instance_key,
            element_instance_key,
            element_id: element_id.clone(),
        }];
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

    /// Withdraws the losing siblings of an event-based gateway's deferred choice.
    ///
    /// An event-based gateway routes into several intermediate catch events and
    /// arms them all at once (its completion takes every outgoing flow). The
    /// first event to occur wins: this helper is called when that winning catch
    /// event completes and tears down the *other* targets of the same gateway —
    /// cancelling every armed timer and open (message/signal/conditional)
    /// subscription resting on each losing sibling, then completing its element
    /// instance so its token is consumed without taking an outgoing flow. Only
    /// active sibling instances in the same token `scope` are withdrawn, so a
    /// gateway reached again on a loop only ever withdraws the current race.
    ///
    /// Returns an empty vec — the overwhelmingly common path — when `winner`
    /// is not the immediate target of an event-based gateway, or when its owning
    /// gateway is ambiguous (more than one event-based gateway routes into the
    /// same catch event), in which case no siblings are withdrawn.
    fn withdraw_event_gateway_siblings(
        &self,
        instance_key: Key,
        winner_eik: Key,
        winner_element_id: &str,
        scope: Key,
    ) -> Vec<Event> {
        let Some(def) = self.process_of_instance(instance_key) else {
            return Vec::new();
        };
        // An event-based gateway's deferred choice is a race between intermediate
        // catch events only. `is_catch_event` gates both the winner (below) and
        // each losing sibling: it keeps the completion hot path O(1) for the
        // common (non-catch) completion, and — should a malformed model route a
        // gateway into a non-catch node (e.g. a service task) — prevents this
        // code from force-completing that node's element instance via
        // `ElementCompleted` while leaving its job/user-task uncancelled (which
        // would orphan work). Withdrawal only ever cancels timers/subscriptions
        // resting on genuine catch siblings.
        let is_catch_event = |element_id: &str| {
            matches!(
                def.elements.get(element_id).map(|e| &e.kind),
                Some(
                    ElementKind::TimerIntermediateCatchEvent { .. }
                        | ElementKind::MessageIntermediateCatchEvent { .. }
                        | ElementKind::SignalIntermediateCatchEvent { .. }
                        | ElementKind::ConditionalIntermediateCatchEvent { .. }
                )
            )
        };
        if !is_catch_event(winner_element_id) {
            return Vec::new();
        }
        // A catch event downstream of an event-based gateway has exactly one
        // incoming flow — from the gateway. If the winner has any *other*
        // incoming path (a malformed graph where a non-gateway node also routes
        // into it), we cannot be sure this token arrived via the gateway, so we
        // conservatively withdraw nothing rather than risk cancelling an
        // unrelated race in the same scope.
        let incoming_count = def
            .elements
            .values()
            .flat_map(|element| element.outgoing.iter())
            .filter(|f| f.to == winner_element_id)
            .count();
        if incoming_count != 1 {
            return Vec::new();
        }
        // Find the event-based gateway(s) that route into the winning catch
        // event. In a well-formed model a catch event has exactly one incoming
        // flow, so at most one gateway owns the race. If more than one gateway
        // routes into the same catch event the owning race is ambiguous — we
        // cannot tell which gateway's siblings to withdraw without risking the
        // cancellation of an unrelated gateway's branch — so we conservatively
        // do nothing.
        let owners: Vec<&crate::model::Element> = def
            .elements
            .values()
            .filter(|element| matches!(element.kind, ElementKind::EventBasedGateway))
            .filter(|element| element.outgoing.iter().any(|f| f.to == winner_element_id))
            .collect();
        let [owner] = owners.as_slice() else {
            return Vec::new();
        };
        // The owning gateway must itself have exactly one incoming flow. A
        // gateway reachable via multiple incoming flows (e.g. fed by both arms of
        // a parallel split) can be activated concurrently in the same scope,
        // arming several live instances of each sibling element id at once. Since
        // losers are matched by element id + scope, withdrawing here could cancel
        // a sibling instance belonging to a *different* concurrent activation of
        // the same gateway. When the owner's static in-degree is not exactly 1 we
        // cannot isolate a single race, so we conservatively withdraw nothing.
        let owner_incoming = def
            .elements
            .values()
            .flat_map(|element| element.outgoing.iter())
            .filter(|f| f.to == owner.id)
            .count();
        if owner_incoming != 1 {
            return Vec::new();
        }
        // The sibling targets are the owning gateway's *other* outgoing catch
        // events. Any non-catch target is skipped (see `is_catch_event` above).
        let sibling_ids: Vec<&str> = owner
            .outgoing
            .iter()
            .filter(|f| f.to != winner_element_id)
            .map(|f| f.to.as_str())
            .filter(|id| is_catch_event(id))
            .collect();
        if sibling_ids.is_empty() {
            return Vec::new();
        }
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return Vec::new();
        };
        // Every active element instance of a losing sibling in this token scope.
        // Sorted by element-instance key for a deterministic, replay-stable log.
        let mut losers: Vec<(Key, String)> = instance
            .active
            .iter()
            .filter(|(eik, eid)| {
                **eik != winner_eik
                    && sibling_ids.contains(&eid.as_str())
                    && self.scope_of(instance_key, **eik) == scope
            })
            .map(|(eik, eid)| (*eik, eid.clone()))
            .collect();
        losers.sort_by_key(|(eik, _)| *eik);

        let mut events = Vec::new();
        for (eik, eid) in losers {
            events.extend(self.cancel_all_timers_on(eik));
            events.extend(self.cancel_all_subscriptions_on(eik));
            events.extend(self.cancel_all_signal_subscriptions_on(eik));
            events.extend(self.cancel_all_conditional_subscriptions_on(eik));
            events.push(Event::ElementCompleting {
                instance_key,
                element_instance_key: eik,
                element_id: eid.clone(),
            });
            events.push(Event::ElementCompleted {
                instance_key,
                element_instance_key: eik,
                element_id: eid,
            });
        }
        events
    }
    /// whose `end` execution-listener chain has drained (ADR 0037). Its boundary
    /// events disarmed and its output mappings projected when it parked (in the
    /// drained-sub-process sweep); this emits the parked `ElementCompleted` and
    /// takes its outgoing flows. A newly-drained *enclosing* sub-process is picked
    /// up by the end-of-command sweep, so nesting composes.
    fn finalize_subprocess(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let mut events = vec![Event::ElementCompleted {
            instance_key,
            element_instance_key,
            element_id: element_id.clone(),
        }];
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

    /// Advances an element's execution-listener chain after one listener job
    /// completed (ADR 0037). If another listener remains in the chain, its job is
    /// created; otherwise the chain has drained and the deferred lifecycle
    /// transition runs — a `Start` chain enacts the element's activation
    /// behaviour ([`run_activation_body`]), an `End` chain emits the deferred
    /// completion ([`finalize_completion`]).
    fn advance_listener(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        event_type: crate::model::ListenerEventType,
        index: usize,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let listeners = self.listeners_of(instance_key, &element_id, event_type);
        let next_index = index + 1;
        if let Some(next) = listeners.get(next_index) {
            // The element's applied scope view (input mappings for a start chain,
            // completion-time state for an end chain, plus any variables merged by
            // preceding listener completions) is what the next listener's FEEL
            // attributes resolve against.
            let listener_vars = self.variables_for_element(instance_key, element_instance_key);
            let job_key = self.mint_key();
            let job_type = self.resolve_job_type(&listener_vars, &next.job_type);
            let retries = self.resolve_retries(&listener_vars, next.retries.as_deref());
            return (
                vec![Event::ExecutionListenerJobCreated {
                    job_key,
                    instance_key,
                    element_instance_key,
                    element_id,
                    job_type,
                    event_type,
                    listener_index: next_index,
                    scope,
                    created_at: self.now,
                    retries,
                }],
                Vec::new(),
            );
        }

        // Chain drained — run the deferred transition.
        match event_type {
            crate::model::ListenerEventType::Start => {
                // A multi-instance body deferred its child fan-out (not
                // `run_activation_body`): its activation already ran in
                // `activate_multi_instance_body`; the start chain gated only the
                // spawn, re-derived here from the body record.
                if self
                    .state
                    .instances
                    .get(&instance_key)
                    .map(|i| i.multi_instances.contains_key(&element_instance_key))
                    .unwrap_or(false)
                {
                    (
                        Vec::new(),
                        self.spawn_multi_instance_children(instance_key, element_instance_key),
                    )
                } else {
                    self.run_activation_body(instance_key, element_id, element_instance_key, scope)
                }
            }
            crate::model::ListenerEventType::End => {
                self.finalize_end_transition(instance_key, element_instance_key, element_id, scope)
            }
        }
    }

    /// Whether a task-listener event type may *deny* its transition. Only
    /// `assigning`, `updating` and `completing` support denial; `creating` and
    /// `canceling` must proceed (Zeebe parity, ADR 0037 §6).
    fn task_event_supports_deny(event_type: crate::model::TaskListenerEventType) -> bool {
        matches!(
            event_type,
            crate::model::TaskListenerEventType::Assigning
                | crate::model::TaskListenerEventType::Updating
                | crate::model::TaskListenerEventType::Completing
        )
    }

    /// Builds the events that begin a user task's task-listener chain: records
    /// the deferred transition on the task and mints the first listener's job.
    /// The caller has already checked that `first` exists (the listener-free
    /// path never calls this, keeping listener-free user tasks byte-identical).
    #[allow(clippy::too_many_arguments)]
    fn start_task_listener_chain(
        &mut self,
        user_task_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: &str,
        event_type: crate::model::TaskListenerEventType,
        pending: state::PendingUserTaskTransition,
        first: &crate::model::TaskListener,
    ) -> Vec<Event> {
        let job_event = self.mint_task_listener_job(
            user_task_key,
            instance_key,
            element_instance_key,
            element_id,
            event_type,
            0,
            first,
        );
        vec![
            Event::UserTaskTransitionDeferred {
                user_task_key,
                instance_key,
                pending,
            },
            job_event,
        ]
    }

    /// Mints a task-listener job for the listener at `index` of `event_type`'s
    /// chain, resolving its `type`/`retries` FEEL against the user task's scope.
    #[allow(clippy::too_many_arguments)]
    fn mint_task_listener_job(
        &mut self,
        user_task_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: &str,
        event_type: crate::model::TaskListenerEventType,
        index: usize,
        listener: &crate::model::TaskListener,
    ) -> Event {
        let listener_vars = self.variables_for_element(instance_key, element_instance_key);
        let job_key = self.mint_key();
        let job_type = self.resolve_job_type(&listener_vars, &listener.job_type);
        let retries = self.resolve_retries(&listener_vars, listener.retries.as_deref());
        Event::TaskListenerJobCreated {
            job_key,
            instance_key,
            element_instance_key,
            element_id: element_id.to_string(),
            user_task_key,
            job_type,
            event_type,
            listener_index: index,
            created_at: self.now,
            retries,
        }
    }

    /// Advances a user task's task-listener chain after the listener at `index`
    /// completed: mints the next listener's job, or — when the chain drains —
    /// commits the deferred transition (ADR 0037 §6).
    fn advance_task_listener(
        &mut self,
        user_task_key: Key,
        event_type: crate::model::TaskListenerEventType,
        index: usize,
    ) -> (Vec<Event>, Vec<Step>) {
        let Some(task) = self.state.user_tasks.get(&user_task_key) else {
            return (Vec::new(), Vec::new());
        };
        let instance_key = task.instance_key;
        let element_instance_key = task.element_instance_key;
        let element_id = task.element_id.clone();
        let listeners = self.task_listeners_of(instance_key, &element_id, event_type);
        let next_index = index + 1;
        if let Some(next) = listeners.get(next_index).cloned() {
            let job_event = self.mint_task_listener_job(
                user_task_key,
                instance_key,
                element_instance_key,
                &element_id,
                event_type,
                next_index,
                &next,
            );
            return (vec![job_event], Vec::new());
        }
        // Chain drained — commit the deferred transition.
        self.commit_user_task_transition(user_task_key)
    }

    /// Commits a user task's deferred transition once its listener chain has
    /// drained: applies accumulated corrections, emits the appropriate lifecycle
    /// event(s), clears the pending state and resumes the token where the
    /// transition requires it (ADR 0037 §6).
    fn commit_user_task_transition(&mut self, user_task_key: Key) -> (Vec<Event>, Vec<Step>) {
        let Some(task) = self.state.user_tasks.get(&user_task_key) else {
            return (Vec::new(), Vec::new());
        };
        let Some(pending) = task.pending.clone() else {
            return (Vec::new(), Vec::new());
        };
        let instance_key = task.instance_key;
        let element_instance_key = task.element_instance_key;
        let element_id = task.element_id.clone();
        let has_initial_assignee = task.assignee.is_some() || pending.assignee.is_some();
        let corrections = pending.corrections.clone();

        let mut events: Vec<Event> = Vec::new();
        let mut steps: Vec<Step> = Vec::new();
        let mut finish_termination = false;

        match pending.event_type {
            crate::model::TaskListenerEventType::Creating => {
                // The task becomes available. A creating listener's assignee
                // correction was already validated at job completion (rejected
                // if an initial assignee exists), so apply corrections directly.
                events.extend(self.apply_creating_corrections(
                    user_task_key,
                    instance_key,
                    &corrections,
                    has_initial_assignee,
                ));
            }
            crate::model::TaskListenerEventType::Assigning => {
                // The corrected assignee (if any) overrides the command's target.
                let assignee = match &corrections.assignee {
                    Some(a) if a.is_empty() => None,
                    Some(a) => Some(a.clone()),
                    None => pending.assignee.clone(),
                };
                events.extend(self.apply_non_assignee_corrections(
                    user_task_key,
                    instance_key,
                    &corrections,
                ));
                events.push(Event::UserTaskAssigned {
                    user_task_key,
                    instance_key,
                    assignee,
                });
            }
            crate::model::TaskListenerEventType::Updating => {
                let update = pending.update.clone().unwrap_or_default();
                // Corrections override the corresponding update fields.
                events.push(Event::UserTaskUpdated {
                    user_task_key,
                    instance_key,
                    candidate_groups: corrections
                        .candidate_groups
                        .clone()
                        .or(update.candidate_groups),
                    candidate_users: corrections
                        .candidate_users
                        .clone()
                        .or(update.candidate_users),
                    due_date: corrections
                        .due_date
                        .clone()
                        .map(|d| if d.is_empty() { None } else { Some(d) })
                        .or(update.due_date),
                    follow_up_date: corrections
                        .follow_up_date
                        .clone()
                        .map(|d| if d.is_empty() { None } else { Some(d) })
                        .or(update.follow_up_date),
                    priority: corrections.priority.or(update.priority),
                });
                if corrections.assignee.is_some() {
                    let assignee = corrections.assignee.clone().filter(|a| !a.is_empty());
                    events.push(Event::UserTaskAssigned {
                        user_task_key,
                        instance_key,
                        assignee,
                    });
                }
            }
            crate::model::TaskListenerEventType::Completing => {
                // Apply corrections to the (about-to-complete) task's data, then
                // complete it and propagate the captured completion variables.
                events.extend(self.apply_non_assignee_corrections(
                    user_task_key,
                    instance_key,
                    &corrections,
                ));
                if corrections.assignee.is_some() {
                    let assignee = corrections.assignee.clone().filter(|a| !a.is_empty());
                    events.push(Event::UserTaskAssigned {
                        user_task_key,
                        instance_key,
                        assignee,
                    });
                }
                events.push(Event::UserTaskCompleted {
                    user_task_key,
                    instance_key,
                });
                steps.push(Step::Complete {
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                });
            }
            crate::model::TaskListenerEventType::Canceling => {
                // Cancellation must proceed; corrections are ignored.
                events.push(Event::UserTaskCanceled {
                    user_task_key,
                    instance_key,
                });
                // If this was the last user task deferring cancellation on a
                // terminating instance, finish the termination now (ADR 0037 §6).
                let terminating = self
                    .state
                    .instances
                    .get(&instance_key)
                    .map(|i| i.state == ProcessInstanceState::Terminating)
                    .unwrap_or(false);
                if terminating {
                    let others_canceling = self.state.user_tasks.values().any(|t| {
                        t.instance_key == instance_key
                            && t.key != user_task_key
                            && matches!(
                                t.pending.as_ref().map(|p| p.event_type),
                                Some(crate::model::TaskListenerEventType::Canceling)
                            )
                    });
                    if !others_canceling {
                        // Emitted after the resolution event below so pending is
                        // cleared first.
                        finish_termination = true;
                    }
                }
            }
        }

        events.push(Event::UserTaskTransitionResolved {
            user_task_key,
            instance_key,
            denied: None,
        });

        if finish_termination {
            events.push(Event::ProcessInstanceTerminated { instance_key });
        }

        // Creating→assigning handoff (Zeebe parity): when the creating chain
        // carried a stripped initial assignee, route it through an assigning
        // transition now that the task is available. Emitted after the resolution
        // event above so the creating pending is cleared before the assigning
        // pending is set on replay.
        if matches!(
            pending.event_type,
            crate::model::TaskListenerEventType::Creating
        ) {
            if let Some(initial) = pending.assignee.clone() {
                let assigning = self.task_listeners_of(
                    instance_key,
                    &element_id,
                    crate::model::TaskListenerEventType::Assigning,
                );
                if let Some(first) = assigning.first().cloned() {
                    let new_pending = state::PendingUserTaskTransition {
                        event_type: crate::model::TaskListenerEventType::Assigning,
                        assignee: Some(initial),
                        update: None,
                        variables: std::collections::HashMap::new(),
                        corrections: crate::model::UserTaskCorrections::default(),
                    };
                    events.extend(self.start_task_listener_chain(
                        user_task_key,
                        instance_key,
                        element_instance_key,
                        &element_id,
                        crate::model::TaskListenerEventType::Assigning,
                        new_pending,
                        &first,
                    ));
                }
            }
        }

        // For completing, the variables must merge before the token resumes; do
        // it now (the variable events precede the resolution/step above only in
        // ordering terms — merge here so replay sees them before the resume).
        if matches!(
            pending.event_type,
            crate::model::TaskListenerEventType::Completing
        ) && !pending.variables.is_empty()
        {
            let flow_scope = self.scope_of(instance_key, element_instance_key);
            let var_events =
                self.propagated_updates(instance_key, flow_scope, pending.variables, false);
            // Insert variable merges just before the completion event so the
            // completing token sees them.
            let insert_at = events
                .iter()
                .position(|e| matches!(e, Event::UserTaskCompleted { .. }))
                .unwrap_or(events.len());
            for (offset, ev) in var_events.into_iter().enumerate() {
                events.insert(insert_at + offset, ev);
            }
        }

        (events, steps)
    }

    /// Applies the non-assignee corrections (candidates, dates, priority) of a
    /// task listener as a single `UserTaskUpdated`, if any are set. Returns an
    /// empty vec when there is nothing to update.
    fn apply_non_assignee_corrections(
        &self,
        user_task_key: Key,
        instance_key: Key,
        corrections: &crate::model::UserTaskCorrections,
    ) -> Vec<Event> {
        if corrections.candidate_groups.is_none()
            && corrections.candidate_users.is_none()
            && corrections.due_date.is_none()
            && corrections.follow_up_date.is_none()
            && corrections.priority.is_none()
        {
            return Vec::new();
        }
        vec![Event::UserTaskUpdated {
            user_task_key,
            instance_key,
            candidate_groups: corrections.candidate_groups.clone(),
            candidate_users: corrections.candidate_users.clone(),
            due_date: corrections
                .due_date
                .clone()
                .map(|d| if d.is_empty() { None } else { Some(d) }),
            follow_up_date: corrections.follow_up_date.clone().map(|d| {
                if d.is_empty() {
                    None
                } else {
                    Some(d)
                }
            }),
            priority: corrections.priority,
        }]
    }

    /// Applies a creating listener's corrections to a freshly-available user
    /// task. The assignee correction is honoured only when no initial assignee
    /// was declared (Zeebe parity, ADR 0037 §6).
    fn apply_creating_corrections(
        &self,
        user_task_key: Key,
        instance_key: Key,
        corrections: &crate::model::UserTaskCorrections,
        has_initial_assignee: bool,
    ) -> Vec<Event> {
        let mut events =
            self.apply_non_assignee_corrections(user_task_key, instance_key, corrections);
        if let Some(assignee) = &corrections.assignee {
            if !has_initial_assignee {
                events.push(Event::UserTaskAssigned {
                    user_task_key,
                    instance_key,
                    assignee: Some(assignee.clone()).filter(|a| !a.is_empty()),
                });
            }
        }
        events
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
            Some(ElementKind::ServiceTask {
                job_type, priority, ..
            }) => {
                let job_key = self.mint_key();
                // The element instance is already active (this is an incident
                // retry), so resolve its FEEL attributes against its own applied
                // scope view (input mappings + ancestors).
                let job_vars = self.variables_for_element(instance_key, element_instance_key);
                let job_type = self.resolve_job_type(&job_vars, &job_type);
                let priority = self.resolve_priority(&job_vars, priority.as_deref());
                let retries = self.resolve_retries(
                    &job_vars,
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
    /// Selects the outgoing flow an exclusive gateway takes, evaluating each
    /// conditional flow in document order against the instance variables and
    /// falling back to the explicit `default` flow when none matches. Returns the
    /// chosen flow's target id (`Ok(Some)`), `Ok(None)` when nothing matches and
    /// there is no default (the caller raises a no-matching-flow incident), or
    /// `Err(reason)` when a condition failed to evaluate (an expression incident).
    /// Pure over the current variables, so the end-listener gate can re-select at
    /// [`finalize_exclusive_gateway`] time without captured state.
    fn select_exclusive_flow(
        &self,
        instance_key: Key,
        element_id: &str,
    ) -> Result<Option<String>, String> {
        let variables = self.variables(instance_key);
        let mut default_flow = None;
        for flow in self.outgoing(instance_key, element_id) {
            // The explicit `default` flow is a fallback only: it is never taken
            // by document order, but kept aside in case no conditional flow
            // matches.
            if flow.is_default {
                default_flow = Some(flow.to);
                continue;
            }
            match &flow.condition {
                None => return Ok(Some(flow.to)),
                Some(condition) => match condition.eval(&variables) {
                    Ok(true) => return Ok(Some(flow.to)),
                    Ok(false) => continue,
                    Err(err) => {
                        return Err(format!(
                            "failed to evaluate condition '{}' at exclusive gateway \
                             '{element_id}': {}",
                            condition.expression, err.0
                        ));
                    }
                },
            }
        }
        Ok(default_flow)
    }

    fn complete_exclusive_gateway(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    ) -> (Vec<Event>, Vec<Step>) {
        let selected = match self.select_exclusive_flow(instance_key, &element_id) {
            Ok(sel) => sel,
            Err(reason) => {
                let incident_key = self.mint_key();
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
        };

        match selected {
            Some(flow_to) => {
                let scope = self.scope_of(instance_key, element_instance_key);
                let mut events = vec![Event::ElementCompleting {
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                }];
                // End-listener gate (ADR 0037): the gateway rests in COMPLETING
                // while its end chain runs; `finalize_exclusive_gateway` re-selects
                // the flow and emits the deferred completion once it drains.
                let vars = self.variables(instance_key);
                if let Some(job) = self.begin_end_listener_chain(
                    instance_key,
                    element_instance_key,
                    &element_id,
                    scope,
                    &vars,
                ) {
                    events.push(job);
                    return (events, Vec::new());
                }
                events.push(Event::ElementCompleted {
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                });
                events.push(Event::SequenceFlowTaken {
                    instance_key,
                    from: element_id,
                    to: flow_to.clone(),
                });
                (
                    events,
                    vec![Step::Activate {
                        instance_key,
                        element_id: flow_to,
                        scope,
                    }],
                )
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

    /// Deferred completion of an exclusive gateway whose `end` execution-listener
    /// chain has drained (ADR 0037). Re-selects the outgoing flow from the
    /// resident scope (the gateway rested in COMPLETING). Selection is normally
    /// deterministic, but a listener may have rewritten a condition variable; if
    /// re-selection now matches nothing (or a condition fails to evaluate) the
    /// gateway raises the same incident its non-listener path would and keeps the
    /// token active, rather than emitting `ElementCompleted` with nowhere to go
    /// (which would drop the token / falsely complete the instance).
    fn finalize_exclusive_gateway(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        match self.select_exclusive_flow(instance_key, &element_id) {
            Ok(Some(flow_to)) => (
                vec![
                    Event::ElementCompleted {
                        instance_key,
                        element_instance_key,
                        element_id: element_id.clone(),
                    },
                    Event::SequenceFlowTaken {
                        instance_key,
                        from: element_id,
                        to: flow_to.clone(),
                    },
                ],
                vec![Step::Activate {
                    instance_key,
                    element_id: flow_to,
                    scope,
                }],
            ),
            Ok(None) => {
                // No flow matches now (and no default). Keep the token parked on
                // an incident — do not complete — mirroring the non-listener path.
                let incident_key = self.mint_key();
                (
                    vec![Event::IncidentRaised {
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
                    }],
                    Vec::new(),
                )
            }
            Err(reason) => {
                let incident_key = self.mint_key();
                (
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
                )
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
    /// A `ModifyInstance` activate instruction referenced an element id that is
    /// not part of the instance's process definition.
    ElementNotFound {
        instance_key: Key,
        element_id: String,
    },
    /// A `ModifyInstance` terminate instruction referenced a key that is not an
    /// active element instance of the target process instance.
    ElementInstanceNotFound {
        instance_key: Key,
        element_instance_key: Key,
    },
    /// `AssignUserTask`/`CompleteUserTask` referenced a user-task key that does
    /// not exist.
    UserTaskNotFound { user_task_key: Key },
    /// `AssignUserTask`/`CompleteUserTask` referenced a user task that is not in
    /// a state where it can be acted on (already completed or cancelled).
    UserTaskNotActive { user_task_key: Key },
    /// `AssignUserTask` with `allow_override = false` targeted a user task that
    /// already has an assignee; it must be unassigned first.
    UserTaskAlreadyAssigned { user_task_key: Key },
    /// A task-listener job was completed with variables, which Zeebe forbids for
    /// listener jobs (ADR 0037 §6).
    TaskListenerJobWithVariables { job_key: Key },
    /// A task-listener job denied its transition while also returning
    /// corrections; the two are mutually exclusive (ADR 0037 §6).
    TaskListenerDenyWithCorrections { job_key: Key },
    /// A task-listener job denied a transition whose event type does not support
    /// denial (only `assigning`, `updating`, `completing` do; ADR 0037 §6).
    TaskListenerDenyNotSupported { job_key: Key },
    /// A `creating` task listener tried to correct the assignee of a user task
    /// that already declares an initial assignee (ADR 0037 §6, Zeebe parity).
    TaskListenerAssigneeCorrectionOnCreating { user_task_key: Key },
    /// An ad-hoc sub-process agent's `activateElements[]` instruction referenced
    /// an element id that is not one of the container's tools. Zeebe rejects the
    /// activation with NOT_FOUND (`AdHocSubProcessInstructionActivateProcessor`).
    AdHocUnknownElement {
        instance_key: Key,
        element_id: String,
    },
    /// An ad-hoc sub-process agent asserted `completionConditionFulfilled` while
    /// also requesting new element activations in the same turn. The two are
    /// contradictory; Zeebe rejects with INVALID_ARGUMENT
    /// (`AdHocSubProcessUtils.verifyCompletionConditionFulfilled`).
    AdHocActivateWithCompletion { job_key: Key },
    /// The external "activate ad-hoc activities" command (#614 gap 3) named an
    /// `adHocSubProcessInstanceKey` that does not identify an active ad-hoc
    /// sub-process container. Zeebe rejects with NOT_FOUND
    /// (`AdHocSubProcessInstructionActivateProcessor`).
    AdHocSubProcessNotFound { ad_hoc_instance_key: Key },
    /// The external "activate ad-hoc activities" command (#614 gap 3) was sent
    /// with no `elements` to activate and `cancelRemainingInstances = false`.
    /// That request is a no-op the caller cannot have intended — completing the
    /// container is only expressible via `cancelRemainingInstances` — so it is
    /// rejected as INVALID_ARGUMENT rather than silently finishing a parked
    /// container. (The agent-job completion seam, #614 gap 4, still ends a turn
    /// by activating nothing; only this external command forbids it.)
    AdHocNoActivationTargets { ad_hoc_instance_key: Key },
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
            EngineError::ElementNotFound {
                instance_key,
                element_id,
            } => {
                write!(
                    f,
                    "instance {instance_key} has no element with id {element_id}"
                )
            }
            EngineError::ElementInstanceNotFound {
                instance_key,
                element_instance_key,
            } => {
                write!(
                    f,
                    "instance {instance_key} has no active element instance {element_instance_key}"
                )
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
            EngineError::TaskListenerJobWithVariables { job_key } => {
                write!(
                    f,
                    "task-listener job {job_key} cannot be completed with variables"
                )
            }
            EngineError::TaskListenerDenyWithCorrections { job_key } => {
                write!(
                    f,
                    "task-listener job {job_key} cannot both deny the transition and return corrections"
                )
            }
            EngineError::TaskListenerDenyNotSupported { job_key } => {
                write!(
                    f,
                    "task-listener job {job_key} denied a transition whose event type does not support denial"
                )
            }
            EngineError::TaskListenerAssigneeCorrectionOnCreating { user_task_key } => {
                write!(
                    f,
                    "a creating task listener cannot correct the assignee of user task {user_task_key}: it already has an initial assignee"
                )
            }
            EngineError::AdHocUnknownElement {
                instance_key,
                element_id,
            } => {
                write!(
                    f,
                    "ad-hoc sub-process in instance {instance_key} has no activatable element with id {element_id}"
                )
            }
            EngineError::AdHocActivateWithCompletion { job_key } => {
                write!(
                    f,
                    "ad-hoc agent job {job_key} cannot both assert the completion condition is fulfilled and activate elements"
                )
            }
            EngineError::AdHocSubProcessNotFound {
                ad_hoc_instance_key,
            } => {
                write!(
                    f,
                    "no active ad-hoc sub-process container with instance key {ad_hoc_instance_key}"
                )
            }
            EngineError::AdHocNoActivationTargets {
                ad_hoc_instance_key,
            } => {
                write!(
                    f,
                    "ad-hoc sub-process activation for container {ad_hoc_instance_key} named no elements and did not cancel remaining instances"
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
    /// The BPMN process id of the job's process definition (Zeebe
    /// `ActivatedJob.bpmnProcessId`).
    pub bpmn_process_id: String,
    /// The key of the job's process definition (Zeebe
    /// `ActivatedJob.processDefinitionKey`).
    pub process_definition_key: Key,
    /// The version of the job's process definition (Zeebe
    /// `ActivatedJob.processDefinitionVersion`).
    pub process_definition_version: i32,
    /// The worker the job was locked to.
    pub worker: String,
    /// Logical instant at which the activation lock expires.
    pub deadline: u64,
    /// Remaining retries for this job.
    pub retries: i32,
    /// Activation priority (higher is activated first; Zeebe
    /// `ActivatedJob.priority`).
    pub priority: i32,
    /// Static custom headers declared on the task via `zeebe:taskHeaders`,
    /// surfaced verbatim (Zeebe `ActivatedJob.customHeaders`). Empty for jobs
    /// that are not ordinary BPMN-element jobs or whose task declares none. A
    /// `BTreeMap` for deterministic serialization order.
    pub custom_headers: std::collections::BTreeMap<String, String>,
    /// User-defined tags on the owning process instance (Zeebe
    /// `ActivatedJob.tags`).
    pub tags: Vec<String>,
    /// The owning process instance's business id, if any (Zeebe
    /// `ActivatedJob.businessId`).
    pub business_id: Option<String>,
    /// A snapshot of the instance's variables at activation time. Shared via
    /// `Arc` with the engine's instance state, so activation does not deep-clone
    /// the (up to 50 KB) value tree on the single command thread; the response
    /// mapper encodes it to JSON off-thread by borrowing.
    pub variables: Arc<HashMap<String, Value>>,
    /// Whether this is an ordinary BPMN-element job or an execution-listener job
    /// (ADR 0037), surfaced so the worker/transport can report `jobKind` and
    /// `listenerEventType` (Camunda parity).
    pub kind: state::JobKind,
}

/// The outcome of a standalone [`Engine::evaluate_deployed_decision`] call: the
/// deployment metadata of the resolved decision plus the full native DMN
/// evaluation result. Surfaced to the EvaluateDecision REST API.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecisionEvaluation {
    /// Unique key of the evaluated decision definition (and version).
    pub decision_key: Key,
    /// Version of the evaluated decision definition.
    pub version: i32,
    /// The evaluated decision's id.
    pub decision_id: String,
    /// The evaluated decision's human-readable name.
    pub decision_name: String,
    /// Unique key of the decision requirements graph it belongs to.
    pub decision_requirements_key: Key,
    /// Id of the decision requirements graph it belongs to.
    pub decision_requirements_id: String,
    /// The native DMN evaluation result (output, per-decision audit, failure).
    pub result: crate::dmn::DecisionEvaluationResult,
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
