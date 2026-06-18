//! State and the applier.
//!
//! [`State`] is the engine's working memory. [`apply`] is the **only** function
//! that mutates it, and it does so purely as a function of an [`Event`]. Keeping
//! every mutation here is what makes the engine deterministic and replayable:
//! replaying the same events over a fresh [`State`] reconstructs it exactly.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::event::Event;
use crate::model::{ElementId, ProcessDefinition, Value};

/// A globally-unique identifier for instances, element instances and jobs.
///
/// Following Zeebe, the high [`PARTITION_BITS`] bits encode the id of the
/// partition that minted the key and the low [`LOCAL_BITS`] bits are a
/// per-partition monotonic counter. This makes every key globally unique across
/// partitions *and* self-routing: the owning partition is recoverable with
/// [`partition_of`]. A single-partition engine uses partition id `0`, so its
/// keys are just `1, 2, 3, …` (identical to the pre-partitioning scheme).
pub type Key = u64;

/// Number of high bits in a [`Key`] reserved for the partition id (Zeebe uses
/// the same split). `PARTITION_BITS + LOCAL_BITS == 64`.
pub const PARTITION_BITS: u32 = 13;
/// Number of low bits in a [`Key`] holding the per-partition local counter.
pub const LOCAL_BITS: u32 = 64 - PARTITION_BITS;
/// Mask selecting the local-counter portion of a [`Key`].
pub const LOCAL_MASK: u64 = (1u64 << LOCAL_BITS) - 1;
/// Largest partition id representable in a [`Key`].
pub const MAX_PARTITION_ID: u64 = (1u64 << PARTITION_BITS) - 1;

/// Extracts the id of the partition that minted `key` (its high bits).
#[inline]
pub const fn partition_of(key: Key) -> u64 {
    key >> LOCAL_BITS
}

/// Extracts the per-partition local counter portion of `key` (its low bits).
#[inline]
pub const fn local_of(key: Key) -> u64 {
    key & LOCAL_MASK
}

/// Composes a [`Key`] from a partition id and a local counter value.
#[inline]
pub const fn compose_key(partition_id: u64, local: u64) -> Key {
    (partition_id << LOCAL_BITS) | (local & LOCAL_MASK)
}

/// Stable 64-bit FNV-1a hash of `bytes`. Deterministic across processes,
/// architectures and restarts (unlike the standard-library `DefaultHasher`,
/// which is randomly seeded), so every node in a cluster derives the same
/// placement for the same input.
#[inline]
pub const fn stable_hash(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
        i += 1;
    }
    hash
}

/// Partition that owns the message subscription for `correlation_key` in a
/// `num_partitions`-wide cluster (Zeebe-style placement: a subscription and a
/// published message land on the same partition iff they share a correlation
/// key). Deterministic via [`stable_hash`]. With `num_partitions == 1` this is
/// always partition `0`, so a single-partition host behaves exactly as before.
#[inline]
pub fn subscription_partition(correlation_key: &str, num_partitions: u64) -> u64 {
    if num_partitions <= 1 {
        return 0;
    }
    stable_hash(correlation_key.as_bytes()) % num_partitions
}

/// Lifecycle state of a process instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ProcessInstanceState {
    Active,
    Completed,
    /// Cancelled by an operator before completing: every token was discarded.
    /// Terminal, like `Completed`, but reached via [`crate::Command::CancelInstance`].
    Terminated,
}

/// Lifecycle state of a job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum JobState {
    /// Created and activatable: available for a worker to activate. A job is
    /// also back in this state once its activation lock expires.
    Created,
    /// Activated and locked to a worker until its `deadline`. While locked it
    /// cannot be activated by another worker, but completion is by key alone.
    Activated,
    /// Failed with no retries left: an incident was raised and the job is parked.
    /// It is neither activatable nor completable. Updating its retries
    /// ([`crate::Command::UpdateJobRetries`]) and then resolving the incident
    /// ([`crate::Command::ResolveIncident`]) returns it to `Created`.
    Failed,
    /// Consumed by a thrown business error: the job is terminal (the error was
    /// either caught by a boundary event or raised an incident).
    Errored,
    /// Cancelled because its activity was interrupted by a boundary event (here,
    /// an interrupting timer boundary event firing). Terminal: the job is neither
    /// activatable nor completable.
    Canceled,
    /// Completed by a worker.
    Completed,
}

/// A job created for a service task, awaiting activation and completion.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Job {
    pub key: Key,
    pub instance_key: Key,
    /// The element instance that is parked waiting on this job.
    pub element_instance_key: Key,
    pub element_id: ElementId,
    pub job_type: String,
    pub state: JobState,
    /// Name of the worker currently holding the activation lock, if any.
    pub worker: Option<String>,
    /// Logical instant at which the current activation lock expires, if locked.
    /// Compared against the caller-supplied `now`; the engine never reads a
    /// wall clock itself.
    pub deadline: Option<u64>,
    /// Whether this job has ever been activated. Completion is permitted for any
    /// job that has been activated at least once and is not yet completed —
    /// regardless of which worker currently holds (or held) the lock. This is
    /// what lets a slow worker still complete a job whose lock expired and was
    /// re-activated by someone else.
    pub activated: bool,
    /// Remaining retries. Set to [`DEFAULT_JOB_RETRIES`] when the job is created
    /// and updated by `FailJob`. A failure that drops it to zero raises an
    /// incident and parks the job ([`JobState::Failed`]).
    pub retries: i32,
}

/// Retries a job starts with when first created.
pub const DEFAULT_JOB_RETRIES: i32 = 3;

/// Lifecycle state of a (native) user task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum UserTaskState {
    /// Created and available for a human to claim/complete.
    Created,
    /// Completed. Terminal: the parked token has resumed.
    Completed,
    /// Cancelled because its activity/instance was terminated. Terminal.
    Canceled,
}

/// A user task created for a `userTask` element, awaiting human completion.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserTask {
    pub key: Key,
    pub instance_key: Key,
    /// The element instance that is parked waiting on this user task.
    pub element_instance_key: Key,
    pub element_id: ElementId,
    pub state: UserTaskState,
    /// The currently assigned user, if any.
    pub assignee: Option<String>,
    /// Candidate groups that may claim the task.
    pub candidate_groups: Vec<String>,
    /// Candidate users that may claim the task.
    pub candidate_users: Vec<String>,
    /// The due date (an ISO-8601 string), if set.
    pub due_date: Option<String>,
    /// The follow-up date (an ISO-8601 string), if set.
    pub follow_up_date: Option<String>,
    /// The task priority (0..=100); defaults to 50.
    pub priority: i32,
    /// The logical instant the task was created (the `now` carried on the
    /// activating command), in milliseconds since the Unix epoch.
    pub created_at: u64,
}

/// A running (or completed) process instance.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ProcessInstance {
    pub key: Key,
    pub process_id: String,
    pub state: ProcessInstanceState,
    /// The logical instant the instance was started (the `created_at` carried on
    /// the creating command), in milliseconds since the Unix epoch. This is the
    /// instance's start date. `0` for instances created before the engine
    /// recorded a start time.
    pub created_at: u64,
    /// User-defined tags associated with this instance. Empty for instances
    /// created before tags were supported.
    #[cfg_attr(feature = "serde", serde(default))]
    pub tags: Vec<String>,
    /// Optional user-defined business identifier for this instance. `None` for
    /// instances created without a business id or before business ids were
    /// supported.
    #[cfg_attr(feature = "serde", serde(default))]
    pub business_id: Option<String>,
    /// Currently-active element instances, keyed by element-instance key. An
    /// element instance is "active" from `ACTIVATED` until `COMPLETED`; a service
    /// task therefore stays here while its job is pending, as does a token parked
    /// on an incident. When this becomes empty the instance has no remaining
    /// tokens and is complete.
    pub active: HashMap<Key, ElementId>,
    /// Maps each active element instance to the element instance of the embedded
    /// sub-process that encloses it. Element instances in the process-level
    /// (root) scope are absent. Used to detect when a sub-process scope has
    /// drained (all its inner tokens consumed) and to terminate a scope when an
    /// error boundary interrupts it.
    pub scopes: HashMap<Key, Key>,
    /// Process variables (used by exclusive-gateway conditions). Shared via `Arc`
    /// so activating a job (which snapshots the instance's variables) is a cheap
    /// refcount bump rather than a deep clone of the (up to 50 KB) decoded value
    /// tree. A mutation (`VariablesUpdated`) copies-on-write via `Arc::make_mut`.
    pub variables: Arc<HashMap<String, Value>>,
    /// For each open parallel-gateway join: how many incoming tokens have
    /// arrived so far.
    pub join_counts: HashMap<ElementId, usize>,
    /// For each open parallel-gateway join: the element instance accumulating
    /// the arriving tokens.
    pub join_instances: HashMap<ElementId, Key>,
    /// Keys of incidents currently **active** on this instance (parked tokens).
    /// The full records live in [`State::incidents`] and are retained after
    /// resolution; resolving an incident removes its key from this active index
    /// (so `hasIncident` reflects only open incidents).
    pub incidents: Vec<Key>,
    /// `true` when this instance's `variables` payload has been spilled to the
    /// host's disk-backed store to bound hot-state memory under a large backlog.
    /// While spilled, `variables` holds an empty placeholder; the host rehydrates
    /// it (see [`crate::Engine::rehydrate_variables`]) before any command that
    /// needs the real payload — notably job activation. Purely a host-managed
    /// memory optimisation: it never changes the engine's logical state, is not
    /// journaled, and is irrelevant to replay (a replayed instance starts
    /// resident with its variables from the log).
    pub variables_spilled: bool,
}

/// Why an incident was raised. Maps to a recovery story and to the REST
/// `errorType` taxonomy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum IncidentKind {
    /// A job exhausted its retries (`FailJob` with 0 left). Recoverable by
    /// updating retries and resolving.
    JobNoRetries,
    /// An exclusive gateway found no matching outgoing sequence flow.
    NoMatchingSequenceFlow,
    /// A sequence-flow condition (or other expression) failed to evaluate to the
    /// expected type — a FEEL parse error, a type error, or a non-boolean
    /// condition result.
    ExpressionEvaluation,
    /// A thrown business error was not caught by any boundary event.
    UnhandledError,
}

/// Lifecycle state of an incident. Incidents are retained after resolution (as
/// `Resolved`) so they remain queryable as an audit trail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum IncidentState {
    /// Raised and parking a token; awaiting resolution.
    Active,
    /// Resolved: the failed work was retried. The record is kept for history.
    Resolved,
}

/// A raised incident: a token parked because something went wrong (a job
/// exhausted its retries, an exclusive gateway matched no flow, or a thrown
/// business error went uncaught). Incidents are resolved with
/// [`crate::Command::ResolveIncident`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Incident {
    pub key: Key,
    pub instance_key: Key,
    /// The parked element instance the incident sits on.
    pub element_instance_key: Key,
    pub element_id: ElementId,
    /// What went wrong.
    pub kind: IncidentKind,
    /// Human-readable explanation of why the incident was raised.
    pub reason: String,
    /// The job whose retry exhaustion caused this incident, if any. Only
    /// job-incidents (`Some`) can be recovered by updating retries and
    /// resolving; gateway/uncaught-error incidents carry `None`.
    pub job_key: Option<Key>,
    /// The logical instant at which the incident was raised, in the same units
    /// the host feeds the engine as `now` (Unix epoch milliseconds on the
    /// server). Sourced from the command's clock, recorded on the event, and so
    /// preserved exactly on replay.
    pub created_at: u64,
    /// Lifecycle state. `Active` while parking a token; `Resolved` once the
    /// failed work has been retried (the record is retained for audit).
    pub state: IncidentState,
    /// The logical instant at which the incident was resolved, if it has been.
    pub resolved_at: Option<u64>,
    /// A caller-supplied reference recorded against the resolution for
    /// traceability (the REST `operationReference`), if any.
    pub operation_reference: Option<i64>,
}

/// Lifecycle state of a timer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TimerState {
    /// Armed and waiting: it fires once a clock tick finds it due.
    Created,
    /// Fired: its token has been released. Retained so it is not re-fired.
    Triggered,
    /// Cancelled before firing because the element it guarded left the flow
    /// first (e.g. a boundary timer whose activity completed, or a sibling
    /// boundary timer when another boundary on the same activity fired).
    /// Retained for audit; never fires.
    Canceled,
}

/// What a timer guards, which decides what firing it does.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TimerKind {
    /// A timer intermediate catch event: the timer's `element_instance_key` is
    /// the catch event itself, and firing completes it, resuming the token along
    /// the event's outgoing flow.
    IntermediateCatch,
    /// An interrupting timer boundary event attached to an activity: the timer's
    /// `element_instance_key`/`element_id` are the *attached activity*, and
    /// firing cancels the activity (and any job parked on it) and takes the
    /// boundary event's outgoing flow.
    InterruptingBoundary {
        /// Id of the boundary event whose outgoing flow runs when the timer fires.
        boundary_element_id: ElementId,
    },
    /// A non-interrupting timer boundary event attached to an activity: like
    /// [`InterruptingBoundary`], but firing leaves the activity (and its job)
    /// running and merely spawns a parallel token along the boundary event's
    /// outgoing flow.
    ///
    /// [`InterruptingBoundary`]: TimerKind::InterruptingBoundary
    NonInterruptingBoundary {
        /// Id of the boundary event whose outgoing flow runs when the timer fires.
        boundary_element_id: ElementId,
    },
}

/// An armed timer holding a token on a timer intermediate catch event until its
/// `due_at` instant passes. A clock tick ([`crate::Command::TriggerTimers`])
/// fires every due timer, releasing its token along the event's outgoing flow.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Timer {
    pub key: Key,
    pub instance_key: Key,
    /// The element instance the token rests on while waiting: the catch event
    /// itself for an intermediate timer, or the attached activity for a boundary
    /// timer.
    pub element_instance_key: Key,
    pub element_id: ElementId,
    /// The logical instant at which the timer becomes due, in the host's clock
    /// units (Unix epoch milliseconds on the server). Carried on the
    /// [`crate::Event::TimerCreated`] event, so replay reconstructs it exactly.
    pub due_at: u64,
    pub state: TimerState,
    /// What the timer guards, and so what firing it does.
    pub kind: TimerKind,
}

/// Lifecycle state of a message subscription.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MessageSubscriptionState {
    /// Open and waiting: a matching correlated message releases its token.
    Open,
    /// Pending placement: the catch element's token is parked on the **instance**
    /// partition, but the canonical subscription lives on a *different* partition
    /// (`hash(correlation_key)`). This record is the instance partition's local
    /// view, awaiting a [`crate::Command::CorrelateMessageSubscription`]
    /// continuation routed back from the message partition. Only ever produced
    /// when `num_partitions > 1` and the correlation key hashes off-partition; a
    /// single-partition host never opens an `Opening` subscription.
    Opening,
    /// Correlated: a matching message arrived and released its token. Retained
    /// so it is not correlated twice.
    Correlated,
    /// Cancelled before correlation because the element it guarded left the flow
    /// first (e.g. a boundary subscription whose activity completed, or a sibling
    /// boundary subscription when another boundary on the same activity fired).
    /// Retained for audit; never correlates.
    Canceled,
}

/// What a message subscription guards, which decides what correlating it does.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MessageSubscriptionKind {
    /// A message intermediate catch event: the subscription's
    /// `element_instance_key` is the catch event itself, and correlating
    /// completes it, resuming the token along the event's outgoing flow.
    IntermediateCatch,
    /// An interrupting message boundary event attached to an activity: the
    /// subscription's `element_instance_key`/`element_id` are the *attached
    /// activity*, and correlating cancels the activity (and any job parked on it)
    /// and takes the boundary event's outgoing flow.
    InterruptingBoundary {
        /// Id of the boundary event whose outgoing flow runs when a message is
        /// correlated.
        boundary_element_id: ElementId,
    },
    /// A non-interrupting message boundary event attached to an activity: like
    /// [`InterruptingBoundary`], but correlating leaves the activity (and its
    /// job) running and merely spawns a parallel token along the boundary event's
    /// outgoing flow. The subscription stays open, so every matching message
    /// spawns another token.
    ///
    /// [`InterruptingBoundary`]: MessageSubscriptionKind::InterruptingBoundary
    NonInterruptingBoundary {
        /// Id of the boundary event whose outgoing flow runs when a message is
        /// correlated.
        boundary_element_id: ElementId,
    },
}

/// An open message subscription holding a token on a message catch element until
/// a matching message is correlated. A [`crate::Command::CorrelateMessage`] whose
/// name and correlation key match an open subscription releases its token (an
/// intermediate catch) or interrupts its activity (an interrupting boundary).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MessageSubscription {
    pub key: Key,
    pub instance_key: Key,
    /// The element instance the token rests on while waiting: the catch event
    /// itself for an intermediate subscription, or the attached activity for a
    /// boundary subscription.
    pub element_instance_key: Key,
    pub element_id: ElementId,
    /// The BPMN message name this subscription waits for.
    pub message_name: String,
    /// The resolved correlation value: the stringified value of the instance
    /// variable the catch element correlates on, captured when the subscription
    /// opened. A message correlates only when its `correlation_key` equals this.
    pub correlation_key: String,
    pub state: MessageSubscriptionState,
    /// What the subscription guards, and so what correlating it does.
    pub kind: MessageSubscriptionKind,
}

/// A process-level subscription on a **message start event**: a correlating
/// message whose name matches creates a new instance of `process_id` (starting
/// at `start_element_id`). Unlike a [`MessageSubscription`] it is not bound to an
/// instance and never settles — it stays open for the life of the deployment,
/// minting a new instance on every matching message. Keyed in [`State`] by
/// message name; re-deploying a process with the same start message replaces it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageStartSubscription {
    /// The definition (and version) whose instances this start event creates.
    pub process_definition_key: Key,
    pub process_id: String,
    /// The BPMN message name that triggers a new instance.
    pub message_name: String,
    /// The start event element a new instance begins at.
    pub start_element_id: ElementId,
}

/// A process-level **timer start event**: an armed timer that creates a new
/// instance of `process_id` when it becomes due. A `repeating` timer (a BPMN
/// cycle) re-arms for another `interval_millis` after each fire; a one-shot (a
/// BPMN duration) fires exactly once and is then retained as `due_at = None`.
/// Keyed in [`State`] by its own `timer_key`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartTimer {
    pub timer_key: Key,
    /// The definition (and version) whose instances this start event creates.
    pub process_definition_key: Key,
    pub process_id: String,
    /// The start event element a new instance begins at.
    pub start_element_id: ElementId,
    /// The instant the timer is next due, or `None` once a one-shot has fired.
    pub due_at: Option<u64>,
    /// The period between fires (and the initial delay), in the host's clock units.
    pub interval_millis: u64,
    /// Whether the timer re-arms after firing (a cycle) or fires once.
    pub repeating: bool,
}

/// A deployed process definition together with the identity the engine assigned
/// it at deploy time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeployedProcess {
    /// Unique key for this specific process definition (and version).
    pub key: Key,
    /// Version number, incremented per process id across deployments (starts 1).
    pub version: i32,
    /// The static, executable definition.
    pub definition: ProcessDefinition,
}

/// The complete working state of the engine.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct State {
    /// Latest deployed version of each process, keyed by BPMN process id. New
    /// instances created by id start the latest version.
    pub processes: HashMap<String, DeployedProcess>,
    pub instances: HashMap<Key, ProcessInstance>,
    pub jobs: HashMap<Key, Job>,
    /// User tasks created for `userTask` elements, keyed by user-task key. A
    /// completed task is retained (transitioned to [`UserTaskState::Completed`])
    /// as an audit trail.
    pub user_tasks: HashMap<Key, UserTask>,
    /// All incidents ever raised, keyed by incident key, retained after
    /// resolution as an audit trail (each carries its [`IncidentState`]).
    pub incidents: HashMap<Key, Incident>,
    /// Armed and fired timers, keyed by timer key. A fired timer is retained
    /// (transitioned to [`TimerState::Triggered`]) so a clock tick never fires
    /// it twice.
    pub timers: HashMap<Key, Timer>,
    /// Open and settled message subscriptions, keyed by subscription key. A
    /// correlated subscription is retained (transitioned to
    /// [`MessageSubscriptionState::Correlated`]) so a later message never
    /// correlates it twice.
    pub message_subscriptions: HashMap<Key, MessageSubscription>,
    /// Process-level message start subscriptions, keyed by message name. A
    /// correlating message whose name matches creates a new instance.
    pub message_start_subscriptions: HashMap<String, MessageStartSubscription>,
    /// Process-level timer start events, keyed by timer key. Each creates a new
    /// instance when due; a cycle re-arms, a one-shot is retained with
    /// `due_at = None`.
    pub start_timers: HashMap<Key, StartTimer>,
    /// Index of jobs eligible to be *considered* for activation, grouped by job
    /// type and ordered by key. A job is a member iff its state is `Created` or
    /// `Activated` (an activated job may still be re-activatable once its lock
    /// deadline passes, so it stays indexed and is filtered by deadline at
    /// activation time). This lets `ActivateJobs` serve a worker poll in roughly
    /// `O(max_jobs)` instead of scanning every job in the system — critical when
    /// a large backlog of pending jobs accumulates under load. It is fully
    /// derived from `jobs` and kept in lockstep by [`resync_job_index`].
    pub activatable_jobs: HashMap<String, BTreeSet<Key>>,
    /// The keys of all jobs currently in the `Activated` state (holding a lock).
    /// Bounded by the number of concurrently working workers, so it lets
    /// `ExpireJobs` find expired locks in `O(activated)` rather than scanning
    /// every job. Derived from `jobs`, kept in lockstep by [`resync_job_index`].
    pub activated_jobs: std::collections::HashSet<Key>,
    /// Reverse index of instance key → the keys of every job that instance owns,
    /// regardless of job state. Jobs are only removed from `jobs` in bulk when
    /// their owning instance is evicted, so this index lets eviction drop an
    /// instance's jobs in `O(jobs of that instance)` instead of scanning every
    /// job in the system — the difference between bounded and `O(total backlog)`
    /// eviction under sustained overload. Derived from `jobs`: a key is inserted
    /// when a job is created and the whole entry is removed when the instance is
    /// evicted.
    pub jobs_by_instance: HashMap<Key, std::collections::HashSet<Key>>,
}

/// A self-contained snapshot of one process instance and every entity it owns
/// (jobs, timers, message subscriptions, user tasks, incidents), captured for
/// **cold spill**: the host-managed eviction of an idle but still-running
/// instance's full control state to disk, to be rehydrated on demand when an
/// event targets it.
///
/// Distinct from variable spill (which sheds only the `variables` payload of a
/// job-parked instance, rehydrated on activation): a cold snapshot moves the
/// *whole* instance — including instances parked on a timer or a message, the
/// genuinely long-lived waits — out of hot state, so a backlog of dormant
/// instances stops costing RAM. The snapshot is authoritative and self-contained
/// (its `instance.variables` hold the real payload, never a spilled
/// placeholder), so [`crate::Engine::rehydrate_instance`] reconstructs hot state
/// exactly. Like every spill artefact it is a cache, not a system of record: the
/// journal already holds the durable history, so a lost snapshot is replayable.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InstanceSnapshot {
    pub instance: ProcessInstance,
    pub jobs: Vec<Job>,
    pub timers: Vec<Timer>,
    pub message_subscriptions: Vec<MessageSubscription>,
    pub user_tasks: Vec<UserTask>,
    pub incidents: Vec<Incident>,
}

impl State {
    /// A fresh, empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Removes `key` from both job indices (the activatable set under
    /// `job_type`, and the activated set), pruning an emptied per-type set.
    /// Used when a job is dropped from `jobs` entirely (eviction), where
    /// [`resync_job_index`] cannot run because the job is already gone.
    pub fn deindex_job(&mut self, job_type: &str, key: Key) {
        if let Some(set) = self.activatable_jobs.get_mut(job_type) {
            set.remove(&key);
            if set.is_empty() {
                self.activatable_jobs.remove(job_type);
            }
        }
        self.activated_jobs.remove(&key);
    }
}

/// Re-syncs the index membership of one job to match its current state.
/// Idempotent: a `Created` job is in the activatable index, an `Activated` job
/// is in the activated index, and anything else (or a missing job) is removed
/// from both. Call after any arm that changes a job's state. Keeping membership
/// a pure function of the job's current state means the indices can never drift
/// from `jobs`.
///
/// `Activated` jobs are deliberately *not* kept in the activatable index: a job
/// whose lock expires is returned to `Created` by [`Event::JobLockExpired`]
/// (driven by the host's periodic tick), which re-adds it here. Excluding locked
/// jobs keeps `activate_jobs`'s walk O(`max_jobs`) instead of O(in-flight
/// backlog) — without it, a large pool of locked jobs (lower keys) is rescanned
/// and skipped on every poll, which degrades into a throughput death spiral once
/// a backlog builds.
pub(crate) fn resync_job_index(state: &mut State, job_key: Key) {
    let Some(job) = state.jobs.get(&job_key) else {
        return;
    };
    let job_type = job.job_type.clone();
    let job_state = job.state;
    if job_state == JobState::Created {
        state
            .activatable_jobs
            .entry(job_type)
            .or_default()
            .insert(job_key);
    } else if let Some(set) = state.activatable_jobs.get_mut(&job_type) {
        set.remove(&job_key);
        if set.is_empty() {
            state.activatable_jobs.remove(&job_type);
        }
    }
    if job_state == JobState::Activated {
        state.activated_jobs.insert(job_key);
    } else {
        state.activated_jobs.remove(&job_key);
    }
}

/// Applies a single [`Event`] to [`State`]. This is the sole mutator of engine
/// state; the processor never mutates [`State`] directly.
pub fn apply(state: &mut State, event: &Event) {
    match event {
        Event::ProcessDeployed {
            process_definition_key,
            version,
            process,
            ..
        } => {
            state.processes.insert(
                process.id.clone(),
                DeployedProcess {
                    key: *process_definition_key,
                    version: *version,
                    definition: process.clone(),
                },
            );
        }

        Event::ProcessInstanceCreated {
            instance_key,
            process_id,
            variables,
            created_at,
            tags,
            business_id,
        } => {
            state.instances.insert(
                *instance_key,
                ProcessInstance {
                    key: *instance_key,
                    process_id: process_id.clone(),
                    state: ProcessInstanceState::Active,
                    created_at: *created_at,
                    tags: tags.clone(),
                    business_id: business_id.clone(),
                    active: HashMap::new(),
                    scopes: HashMap::new(),
                    variables: Arc::new(variables.clone()),
                    join_counts: HashMap::new(),
                    join_instances: HashMap::new(),
                    incidents: Vec::new(),
                    variables_spilled: false,
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
                    activated: false,
                    retries: DEFAULT_JOB_RETRIES,
                },
            );
            resync_job_index(state, *job_key);
            state
                .jobs_by_instance
                .entry(*instance_key)
                .or_default()
                .insert(*job_key);
        }

        Event::JobActivated {
            job_key,
            worker,
            deadline,
            ..
        } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.state = JobState::Activated;
                job.worker = Some(worker.clone());
                job.deadline = Some(*deadline);
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
                }
            }
            resync_job_index(state, *job_key);
        }

        Event::JobFailed {
            job_key, retries, ..
        } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.retries = *retries;
                job.worker = None;
                job.deadline = None;
                // With retries left the job returns to the activatable pool; with
                // none it parks (an incident is raised alongside this event).
                job.state = if *retries > 0 {
                    JobState::Created
                } else {
                    JobState::Failed
                };
            }
            resync_job_index(state, *job_key);
        }

        Event::JobErrorThrown { job_key, .. } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.state = JobState::Errored;
                job.worker = None;
                job.deadline = None;
            }
            resync_job_index(state, *job_key);
        }

        Event::JobCompleted { job_key, .. } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.state = JobState::Completed;
                job.worker = None;
                job.deadline = None;
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
                    created_at: *created_at,
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
        } => {
            state.incidents.insert(
                *incident_key,
                Incident {
                    key: *incident_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    kind: *kind,
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
            // activatable pool so a worker can pick it up again.
            if let Some(job_key) = job_key {
                if let Some(job) = state.jobs.get_mut(job_key) {
                    job.state = JobState::Created;
                    job.worker = None;
                    job.deadline = None;
                }
                resync_job_index(state, *job_key);
            }
        }

        Event::ProcessInstanceCompleted { instance_key } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.state = ProcessInstanceState::Completed;
            }
        }

        Event::ProcessInstanceTerminated { instance_key } => {
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
                instance.active.clear();
                instance.scopes.clear();
                instance.incidents.clear();
            }
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
            }
            resync_job_index(state, *job_key);
        }

        Event::UserTaskCanceled { user_task_key, .. } => {
            if let Some(task) = state.user_tasks.get_mut(user_task_key) {
                task.state = UserTaskState::Canceled;
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
    }
}

#[cfg(test)]
mod placement_tests {
    use super::{stable_hash, subscription_partition};

    #[test]
    fn stable_hash_is_deterministic_and_input_sensitive() {
        assert_eq!(stable_hash(b"order-42"), stable_hash(b"order-42"));
        assert_ne!(stable_hash(b"order-42"), stable_hash(b"order-43"));
        // FNV-1a offset basis for the empty input.
        assert_eq!(stable_hash(b""), 0xcbf2_9ce4_8422_2325);
    }

    #[test]
    fn single_partition_places_every_key_on_zero() {
        for key in ["", "a", "order-1", "customer-99"] {
            assert_eq!(subscription_partition(key, 1), 0);
        }
    }

    #[test]
    fn placement_is_stable_and_within_range() {
        let n = 4;
        for key in ["order-1", "order-2", "x", "really-long-correlation-key-value"] {
            let p = subscription_partition(key, n);
            assert!(p < n);
            // Stable across calls.
            assert_eq!(p, subscription_partition(key, n));
        }
    }

    #[test]
    fn placement_spreads_across_partitions() {
        let n = 4;
        let mut seen = [0u32; 4];
        for i in 0..1000 {
            let key = format!("correlation-{i}");
            seen[subscription_partition(&key, n) as usize] += 1;
        }
        // Every partition gets a non-trivial share (no degenerate hashing).
        for count in seen {
            assert!(count > 150, "uneven placement: {seen:?}");
        }
    }
}
