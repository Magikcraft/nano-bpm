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
    /// Cancellation is in progress but one or more user tasks are running their
    /// `canceling` task listeners (ADR 0037 §6). The instance's other tokens are
    /// already discarded; it transitions to `Terminated` once the last canceling
    /// chain drains. Not a resting state a listener-free instance ever reaches,
    /// so the ordinary synchronous cancel path is unchanged.
    Terminating,
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

/// What a job represents. Ordinary service-task jobs are [`JobKind::BpmnElement`]
/// (the default, so records serialized before this field existed load as such);
/// [`JobKind::ExecutionListener`] jobs are the sequential execution-listener
/// chain that runs on an element's activation/completion transition (ADR 0037).
/// A listener job carries the transition it fires on, its 0-based position in
/// the element's listener list, and the element's enclosing scope (so the next
/// listener or the resumed lifecycle transition can be driven on completion).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum JobKind {
    /// An ordinary job that backs a service task (or ad-hoc agent).
    #[default]
    BpmnElement,
    /// A job for one execution listener in an element's sequential chain.
    ExecutionListener {
        event_type: crate::model::ListenerEventType,
        index: usize,
        scope: Key,
    },
    /// A job for one task listener in a user task's sequential chain (ADR 0037
    /// §6). Carries the transition it fires on, its 0-based position in the
    /// user task's listener list (filtered to this event type), and the user
    /// task whose deferred transition it gates.
    TaskListener {
        event_type: crate::model::TaskListenerEventType,
        index: usize,
        user_task_key: Key,
    },
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
    /// Logical instant at which the current activation lock was acquired, if
    /// locked (the `now` carried on the activating `ActivateJobs` command).
    /// Cleared whenever the lock is released. `None` for a job that is not
    /// currently activated, or for records serialized before this field existed.
    #[cfg_attr(feature = "serde", serde(default))]
    pub activated_at: Option<u64>,
    /// The lock duration the job was activated with (the activating command's
    /// `timeout`), frozen at activation. Unlike `deadline - activated_at`, this
    /// is immune to later `UpdateJobTimeout` lock extensions (which move
    /// [`Job::deadline`] but not the originally-requested timeout), so it always
    /// reflects what the worker asked for. Cleared when the lock is released;
    /// `None` when not activated or for pre-field records.
    #[cfg_attr(feature = "serde", serde(default))]
    pub activation_timeout: Option<u64>,
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
    /// Activation priority (0..=100; default [`DEFAULT_JOB_PRIORITY`]), resolved
    /// from the service task's `zeebe:priorityDefinition` at job creation. Higher
    /// priority is activated first; equal priorities fall back to oldest-first
    /// (key order). Immutable for the job's lifetime.
    #[cfg_attr(feature = "serde", serde(default = "default_job_priority"))]
    pub priority: i32,
    /// The logical instant the job was created (the `now` carried on the creating
    /// command), in milliseconds since the Unix epoch. Used to observe activation
    /// age. `0` for jobs created before the engine carried this field.
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_at: u64,
    /// What this job represents (ordinary element job vs execution-listener job).
    /// Defaults to [`JobKind::BpmnElement`] for records serialized before
    /// execution listeners existed, so ordinary jobs are unaffected.
    #[cfg_attr(feature = "serde", serde(default))]
    pub kind: JobKind,
}

/// Default job-activation priority when no `zeebe:priorityDefinition` is declared.
pub const DEFAULT_JOB_PRIORITY: i32 = 50;

/// serde default for [`Job::priority`] / [`crate::Event::JobCreated`] on records
/// serialized before the field existed.
#[cfg(feature = "serde")]
pub fn default_job_priority() -> i32 {
    DEFAULT_JOB_PRIORITY
}

/// The ordering position of an activatable job in the per-type index: highest
/// priority first (stored negated so a `BTreeSet`'s ascending order yields it
/// first), then lowest key (oldest-first — keys are monotonic with creation
/// within a partition — as the SLA/age tiebreak).
#[inline]
pub(crate) fn activation_order(priority: i32, key: Key) -> (i32, Key) {
    (-priority, key)
}

/// Retries a job starts with when first created.
pub const DEFAULT_JOB_RETRIES: i32 = 3;

/// serde default for [`Job::retries`] / [`crate::Event::JobCreated`] on records
/// serialized before the `retries` field existed on `JobCreated`.
#[cfg(feature = "serde")]
pub fn default_job_retries() -> i32 {
    DEFAULT_JOB_RETRIES
}

/// The in-flight lifecycle transition a user task is deferring while its task
/// listeners run (ADR 0037 §6). Held on [`UserTask::pending`] between the
/// command (or termination) that triggered the transition and the moment its
/// listener chain drains and the transition commits. Durable so replay and
/// failover resume the chain exactly; absent (the common case) for
/// listener-free user tasks, which transition synchronously and are
/// byte-identical to the pre-task-listener engine.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PendingUserTaskTransition {
    /// The transition being deferred (drives which listeners run and what final
    /// event commits).
    pub event_type: crate::model::TaskListenerEventType,
    /// Target assignee for an `Assigning` transition: `Some(name)` to assign,
    /// `None` to unassign. Ignored for other transitions.
    #[cfg_attr(feature = "serde", serde(default))]
    pub assignee: Option<String>,
    /// Normalised update fields for an `Updating` transition. Ignored otherwise.
    #[cfg_attr(feature = "serde", serde(default))]
    pub update: Option<PendingUserTaskUpdate>,
    /// Completion variables for a `Completing` transition, applied to the
    /// instance when the chain drains. Ignored otherwise.
    #[cfg_attr(feature = "serde", serde(default))]
    pub variables: HashMap<String, Value>,
    /// Corrections accumulated from listeners completed so far.
    #[cfg_attr(feature = "serde", serde(default))]
    pub corrections: crate::model::UserTaskCorrections,
}

/// The normalised update payload captured on a deferred `Updating` transition
/// (mirrors [`crate::Event::UserTaskUpdated`] fields).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PendingUserTaskUpdate {
    #[cfg_attr(feature = "serde", serde(default))]
    pub candidate_groups: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub candidate_users: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub due_date: Option<Option<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub follow_up_date: Option<Option<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub priority: Option<i32>,
}

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
    /// The resolved numeric key of the task's form, if its
    /// `zeebe:formDefinition` declared a `formId` that resolved against a
    /// currently-deployed form (latest version) at creation. Surfaced as the v2
    /// user-task `formKey`; downstream `GetFormByKey` serves the schema. `None`
    /// when the task declares no embedded form (or its `formId` matched none).
    /// Snapshots written before form linkage load as `None`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub form_key: Option<Key>,
    /// The external form reference declared via `zeebe:formDefinition
    /// externalReference`, surfaced verbatim as the v2 user-task
    /// `externalFormReference`. `None` when the task declares no external form.
    /// Snapshots written before form linkage load as `None`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub external_form_reference: Option<String>,
    /// The logical instant the task was created (the `now` carried on the
    /// activating command), in milliseconds since the Unix epoch.
    pub created_at: u64,
    /// The lifecycle transition (if any) this user task is currently deferring
    /// while its task listeners run (ADR 0037 §6). `None` (the default, and the
    /// only value for listener-free user tasks) means no transition is in
    /// flight; serialized-before-task-listeners records load as `None`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub pending: Option<PendingUserTaskTransition>,
}
/// A running (or completed) process instance.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ProcessInstance {
    pub key: Key,
    pub process_id: String,
    /// The unique key of the process **definition version** this instance was
    /// created on. Resolves the instance's executable definition through
    /// [`State::process_versions`], so the instance always runs the version it
    /// started on even after a newer version is redeployed (Zeebe parity).
    /// `0` for instances created before version pinning existed (or snapshots
    /// written before this field); [`State::definition_for`] falls back to the
    /// latest-by-id index for those. `serde(default)` for snapshot back-compat.
    #[cfg_attr(feature = "serde", serde(default))]
    pub process_definition_key: Key,
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
    /// Active multi-instance bodies in this instance, keyed by the body element
    /// instance. Empty (the default) for instances with no multi-instance
    /// activity and when deserializing snapshots written before multi-instance
    /// support existed. Rides along in [`InstanceSnapshot::instance`] on spill,
    /// so it needs no separate drain/rehydrate wiring.
    #[cfg_attr(feature = "serde", serde(default))]
    pub multi_instances: HashMap<Key, MultiInstanceState>,
    /// Active ad-hoc sub-process containers in this instance, keyed by the
    /// container element instance (which is also the ad-hoc token scope). Empty
    /// (the default) for instances with no ad-hoc activity and when
    /// deserializing snapshots written before ad-hoc runtime support existed.
    /// Rides along in [`InstanceSnapshot::instance`] on spill like
    /// `multi_instances`, so it needs no separate drain/rehydrate wiring.
    #[cfg_attr(feature = "serde", serde(default))]
    pub adhoc_instances: HashMap<Key, AdHocState>,
    /// Non-root variable scopes: each scope-owning element instance (embedded
    /// sub-process, multi-instance body, multi-instance child) mapped to its
    /// parent scope-owner. The root (process-instance) scope is implicit — its
    /// key is the instance key and it is never present here. Empty for instances
    /// with only the root scope. Together with `scope_variables` this is the
    /// Zeebe-style hierarchical variable tree (Part C). `serde(default)` so
    /// snapshots written before scoping deserialize as root-only.
    #[cfg_attr(feature = "serde", serde(default))]
    pub scope_parents: HashMap<Key, Key>,
    /// Local variables held directly by each non-root scope (keyed by the
    /// scope-owning element instance). This includes a multi-instance child's
    /// `inputElement`/`loopCounter` bindings and a multi-instance body's
    /// `outputCollection`. The root scope's variables live in `variables`. A read
    /// resolves a name from the local scope upward to the root (first hit wins); a
    /// write follows Zeebe variable propagation. Empty for root-only instances,
    /// keeping the flat fast path unchanged.
    #[cfg_attr(feature = "serde", serde(default))]
    pub scope_variables: HashMap<Key, HashMap<String, Value>>,
}

/// Runtime state of an active multi-instance body (the element instance carrying
/// [`crate::model::MultiInstance`] characteristics). Reconstructed from the
/// multi-instance events, so it needs no bespoke snapshot handling beyond riding
/// along in the owning [`ProcessInstance`].
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MultiInstanceState {
    /// The activity element id this body loops over.
    pub element_id: ElementId,
    /// `true` runs children one at a time; `false` runs all in parallel.
    pub sequential: bool,
    /// The evaluated input collection driving the loop (one child per item).
    pub items: Vec<Value>,
    /// Instance-level list variable the per-child output is collected into.
    pub output_collection: Option<String>,
    /// FEEL expression collected per child into `output_collection`.
    pub output_element: Option<String>,
    /// FEEL boolean evaluated after each child; `true` completes the body early.
    pub completion_condition: Option<String>,
    /// Local variable name each item binds to in its child scope.
    pub input_element: Option<String>,
    /// How many children have been spawned so far (drives sequential's next
    /// index and detects when all items have been started).
    pub spawned: usize,
    /// Element instances of the children that are still active.
    pub active: std::collections::BTreeSet<Key>,
    /// Authoritative 0-based loop index of each child element instance, recorded
    /// at activation and read back on completion to position the child's output.
    /// The index is engine-owned runtime state, NOT derived from the child's
    /// mutable `loopCounter` variable — so no write path into the child scope
    /// (`zeebe:input` mapping, a worker `SetVariables`, an inner output mapping)
    /// can corrupt `outputCollection` indexing / join bookkeeping.
    #[cfg_attr(feature = "serde", serde(default))]
    pub child_indices: std::collections::BTreeMap<Key, usize>,
    /// Collected per-child output, positioned by the child's 0-based index.
    pub output_values: Vec<Option<Value>>,
}

/// Runtime state of an active ad-hoc sub-process container (ADR 0023 seam 2).
/// The container element instance is itself the ad-hoc token scope: activated
/// "tool" children run inside it and the container's `outputCollection`
/// accumulates here. Reconstructed from the ad-hoc events, so it rides along in
/// the owning [`ProcessInstance`] with no bespoke snapshot handling.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AdHocState {
    /// The `adHocSubProcess` container element id.
    pub element_id: ElementId,
    /// The container's `zeebe:adHoc outputCollection` result-variable name, if
    /// declared — each activated tool's `output_element` is appended here.
    pub output_collection: Option<String>,
    /// The container's `zeebe:adHoc outputElement` FEEL expression, evaluated in
    /// each completed tool's scope and appended to `output_collection`.
    pub output_element: Option<String>,
    /// Element instances of tool children still running this turn. The container
    /// re-emits its agent job once this drains (and no completion was signalled).
    pub active: std::collections::BTreeSet<Key>,
    /// How many agent-job turns have run (drives the metrics + runaway guard).
    pub iterations: u32,
    /// Latched once the declared `<completionCondition>` has been satisfied with
    /// `cancelRemainingInstances=false`: the loop stops activating new tools and
    /// the container completes once its outstanding children drain, even if a
    /// later tool's output would no longer satisfy the condition (Zeebe
    /// `ElementInstance#isCompletionConditionFulfilled`).
    #[cfg_attr(feature = "serde", serde(default))]
    pub completion_condition_fulfilled: bool,
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
    /// A `businessRuleTask` failed to evaluate its decision — the decision id was
    /// not found, or evaluation produced an error (bad FEEL, hit-policy
    /// violation, missing input). Recoverable once the definition/inputs are
    /// fixed and the incident is resolved.
    DecisionEvaluation,
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

/// An open **signal** subscription holding a token on a signal catch element (or
/// guarding an activity via a signal boundary) until a matching signal is
/// broadcast. Unlike a [`MessageSubscription`], a signal correlates by **name
/// only** (there is no correlation key): a [`crate::Command::BroadcastSignal`]
/// whose `signal_name` matches an open subscription releases its token (an
/// intermediate catch) or interrupts its activity (an interrupting boundary).
/// A broadcast fans out to **every** matching open subscription across all
/// instances. The `kind` reuses [`MessageSubscriptionKind`] (the token-advance
/// semantics are identical to messages).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SignalSubscription {
    pub key: Key,
    pub instance_key: Key,
    /// The element instance the token rests on while waiting: the catch event
    /// itself for an intermediate subscription, or the attached activity for a
    /// boundary subscription.
    pub element_instance_key: Key,
    pub element_id: ElementId,
    /// The BPMN signal name this subscription waits for.
    pub signal_name: String,
    pub state: MessageSubscriptionState,
    /// What the subscription guards, and so what correlating it does. Reuses the
    /// message subscription kind — the outcomes are identical.
    pub kind: MessageSubscriptionKind,
}

/// An open **conditional** subscription holding a token on a conditional
/// intermediate catch event (or guarding an activity via a conditional boundary)
/// until its FEEL `condition` becomes `true`. Unlike message/signal
/// subscriptions there is no external trigger: the engine evaluates `condition`
/// against the instance variables when the subscription opens and again whenever
/// one of `referenced_vars` changes, firing when it yields `true`. An
/// interrupting subscription (intermediate catch or interrupting boundary) fires
/// once; a non-interrupting boundary stays open and can fire repeatedly. The
/// `kind` reuses [`MessageSubscriptionKind`] (the token-advance semantics are
/// identical to messages/signals).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ConditionalSubscription {
    pub key: Key,
    pub instance_key: Key,
    /// The element instance the token rests on while waiting: the catch event
    /// itself for an intermediate subscription, or the attached activity for a
    /// boundary subscription.
    pub element_instance_key: Key,
    pub element_id: ElementId,
    /// The FEEL condition (as authored, `=`-marker included) evaluated to decide
    /// whether the event fires.
    pub condition: String,
    /// The root variable names `condition` references, sorted. The engine
    /// re-evaluates the condition only when a variable in this set changes; an
    /// empty set means the condition depends on no variable (evaluated on open
    /// only).
    pub referenced_vars: Vec<String>,
    pub state: MessageSubscriptionState,
    /// What the subscription guards, and so what firing it does.
    pub kind: MessageSubscriptionKind,
}

/// A process-level subscription on a **message start event**: a correlating
/// message whose name matches creates a new instance of `process_id` (starting
/// at `start_element_id`). Unlike a [`MessageSubscription`] it is not bound to an
/// instance and never settles — it stays open for the life of the deployment,
/// minting a new instance on every matching message. Keyed in [`State`] by
/// message name; re-deploying a process with the same start message replaces it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct State {
    /// Latest deployed version of each process, keyed by BPMN process id. New
    /// instances created by id (with no explicit version) start the latest
    /// version. This is a fast latest-by-id index over `process_versions`.
    pub processes: HashMap<String, DeployedProcess>,
    /// Every deployed process definition ever seen, keyed by its unique
    /// process-definition key (so historical versions are retained, not
    /// overwritten by a redeploy). A running instance resolves *its* definition
    /// through here by its pinned `process_definition_key`, so replaying it
    /// against a newer redeployed version is impossible (Zeebe parity: an
    /// instance runs the version it was created on). `serde(default)` so
    /// snapshots written before version retention deserialize empty and fall
    /// back to the latest-by-id index via [`State::definition_for`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub process_versions: HashMap<Key, DeployedProcess>,
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
    /// Open and settled **signal** subscriptions, keyed by subscription key. A
    /// correlated subscription is retained (transitioned to
    /// [`MessageSubscriptionState::Correlated`]) as an audit trail.
    pub signal_subscriptions: HashMap<Key, SignalSubscription>,
    /// Open and settled **conditional** subscriptions, keyed by subscription key.
    /// A fired interrupting subscription is retained (transitioned to
    /// [`MessageSubscriptionState::Correlated`]) so it never fires twice; a
    /// non-interrupting boundary stays [`MessageSubscriptionState::Open`].
    pub conditional_subscriptions: HashMap<Key, ConditionalSubscription>,
    /// Process-level message start subscriptions, keyed by message name. A
    /// correlating message whose name matches creates a new instance.
    pub message_start_subscriptions: HashMap<String, MessageStartSubscription>,
    /// Process-level timer start events, keyed by timer key. Each creates a new
    /// instance when due; a cycle re-arms, a one-shot is retained with
    /// `due_at = None`.
    pub start_timers: HashMap<Key, StartTimer>,
    /// Index of jobs eligible to be *considered* for activation, grouped by job
    /// type and ordered by `(−priority, key)` — highest priority first, then
    /// oldest (lowest key) as the age/SLA tiebreak. A job is a member iff its
    /// state is `Created` (an `Activated` job is removed when it locks and re-added
    /// by `JobLockExpired` when its lock expires). This lets `ActivateJobs` serve a
    /// worker poll in roughly `O(max_jobs)` instead of scanning every job in the
    /// system — critical when a large backlog of pending jobs accumulates under
    /// load. It is fully derived from `jobs` and kept in lockstep by
    /// [`resync_job_index`].
    pub activatable_jobs: HashMap<String, BTreeSet<(i32, Key)>>,
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
    /// Per-process-definition **in-flight instance count** (created but not yet
    /// terminal), keyed by BPMN process id. Maintained by [`apply_event`] at
    /// `ProcessInstanceCreated` (+1) and the terminal transitions
    /// `ProcessInstanceCompleted`/`ProcessInstanceTerminated` (−1) — the *logical*
    /// lifecycle, unaffected by cold spill/rehydrate (those never change
    /// `instance.state`). This is the ADR-0020 Tier-2 signal `L_P`: bounding it
    /// bounds each definition's e2e instance sojourn `W_P = L_P/λ_P`. A definition
    /// drops out of the map once its count returns to zero.
    #[cfg_attr(feature = "serde", serde(default))]
    pub inflight_by_process: HashMap<String, u64>,
    /// Per-process-definition cumulative **created** count (monotonic), keyed by
    /// BPMN process id. The monitor differences it across ticks to get each
    /// definition's create rate `λ_P` for the Tier-2 throughput-scaled band
    /// `L*_P = W_target·λ_P`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by_process: HashMap<String, u64>,
    /// Latest deployed decision requirements graph (parsed `.dmn`), keyed by DRG
    /// id. Versioned per DRG id across deployments, like processes. A fast
    /// latest-by-id index over [`State::decision_requirements_versions`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub decision_requirements: HashMap<String, DeployedDrg>,
    /// Every deployed DRG version ever seen, keyed by its unique
    /// decision-requirements key (so historical versions are retained, not
    /// overwritten by a redeploy). A `businessRuleTask` binding pinned to a
    /// specific version, or an EvaluateDecision by an older decision key,
    /// resolves through here. `serde(default)` so pre-retention snapshots
    /// deserialize empty and fall back to the latest-by-id index.
    #[cfg_attr(feature = "serde", serde(default))]
    pub decision_requirements_versions: HashMap<Key, DeployedDrg>,
    /// Latest deployed decision, keyed by decision id, for
    /// `businessRuleTask`/EvaluateDecision lookup. Points at the DRG it belongs
    /// to so required-decision chains resolve. A fast latest-by-id index over
    /// [`State::decision_versions`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub decisions: HashMap<String, DeployedDecision>,
    /// Every deployed decision version ever seen, keyed by its unique decision
    /// key, so an EvaluateDecision request pinned to an older decision key can
    /// still resolve the exact version. `serde(default)` for legacy snapshots.
    #[cfg_attr(feature = "serde", serde(default))]
    pub decision_versions: HashMap<Key, DeployedDecision>,
    /// Latest deployed form (`form-js` `.form` JSON), keyed by form id. Versioned
    /// per form id across deployments, like processes. The engine does not execute
    /// forms; it retains them so `GetFormByKey` can serve the stored schema. A
    /// fast latest-by-id index over [`State::form_versions`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub forms: HashMap<String, DeployedForm>,
    /// Every deployed form version ever seen, keyed by its unique form key, so a
    /// `userTask` form binding pinned to a specific version resolves the exact
    /// schema. `serde(default)` for legacy snapshots.
    #[cfg_attr(feature = "serde", serde(default))]
    pub form_versions: HashMap<Key, DeployedForm>,
    /// Latest deployed generic resource (any non-BPMN/DMN/form file, e.g. a
    /// Markdown agent prompt), keyed by `resource_id` (the filename). Versioned
    /// per `resource_id` across deployments, like forms. The engine does not
    /// execute generic resources; it retains them so `GetResourceByKey` and
    /// `searchResources` can serve the stored content. A fast latest-by-id index
    /// over [`State::resource_versions`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub resources: HashMap<String, DeployedResource>,
    /// Every deployed generic-resource version ever seen, keyed by its unique
    /// resource key, so a `zeebe:linkedResource` binding pinned to a specific
    /// version (or an older key) resolves the exact content. `serde(default)`
    /// for legacy snapshots.
    #[cfg_attr(feature = "serde", serde(default))]
    pub resource_versions: HashMap<Key, DeployedResource>,
}

/// A deployed decision requirements graph together with the identity the engine
/// assigned it at deploy time.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DeployedDrg {
    /// Unique key for this specific DRG (and version).
    pub key: Key,
    /// Version, incremented per DRG id across deployments (starts 1).
    pub version: i32,
    /// The parsed graph.
    pub drg: crate::dmn::DecisionRequirementsGraph,
}

/// A deployed decision, indexed by id for evaluation lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DeployedDecision {
    /// Unique key for this specific decision (and version).
    pub key: Key,
    /// Version, incremented per decision id across deployments (starts 1).
    pub version: i32,
    /// The DRG this decision belongs to (needed to evaluate required decisions).
    pub decision_requirements_key: Key,
    /// The decision's id (the lookup key).
    pub decision_id: String,
    /// Human-readable name.
    pub decision_name: String,
    /// The full DRG this decision is part of, so evaluation can follow
    /// `requiredDecision` references natively.
    pub drg: crate::dmn::DecisionRequirementsGraph,
}

/// A deployed form together with the identity the engine assigned it at deploy
/// time. The engine stores forms but does not execute them — they are served
/// verbatim by `GetFormByKey`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DeployedForm {
    /// Unique key for this specific form (and version).
    pub key: Key,
    /// Version, incremented per form id across deployments (starts 1).
    pub version: i32,
    /// The user-provided form identifier (the form-js document's `id`).
    pub form_id: String,
    /// The deploy resource name (e.g. `greeting.form`).
    pub resource_name: String,
    /// The verbatim form-js JSON document.
    pub schema: String,
}

/// A deployed generic resource together with the identity the engine assigned it
/// at deploy time. The engine stores generic resources but does not execute
/// them — they are served verbatim by `GetResourceByKey` / `searchResources`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DeployedResource {
    /// Unique key for this specific resource (and version).
    pub key: Key,
    /// Version, incremented per `resource_id` across deployments (starts 1).
    pub version: i32,
    /// The resource identifier (its filename, for a generic resource).
    pub resource_id: String,
    /// The deploy resource name (the filename).
    pub resource_name: String,
    /// The verbatim resource content.
    pub content: String,
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
    #[cfg_attr(feature = "serde", serde(default))]
    pub signal_subscriptions: Vec<SignalSubscription>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub conditional_subscriptions: Vec<ConditionalSubscription>,
    pub user_tasks: Vec<UserTask>,
    pub incidents: Vec<Incident>,
}

impl State {
    /// A fresh, empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// The executable definition an instance runs against — resolved through the
    /// instance's pinned `process_definition_key` so it is always the version the
    /// instance was created on, even after a newer version is redeployed (Zeebe
    /// parity). Falls back to the latest-by-id index for instances (or snapshots)
    /// created before version pinning, whose `process_definition_key` is `0`.
    pub fn definition_for(&self, instance: &ProcessInstance) -> Option<&DeployedProcess> {
        if instance.process_definition_key != 0 {
            if let Some(deployed) = self.process_versions.get(&instance.process_definition_key) {
                return Some(deployed);
            }
        }
        self.processes.get(&instance.process_id)
    }

    /// The exact deployed definition identified by its process-definition
    /// `key`, if retained. Checks the full version-retention map first, then
    /// falls back to the latest-by-id index for engines restored from
    /// pre-retention snapshots — where `process_versions` deserializes empty
    /// (`serde(default)`) but `processes` still holds the latest deployed
    /// definition (including its key). Without the fallback, a valid by-key
    /// create would spuriously 400 after an upgrade until a redeploy
    /// repopulated `process_versions`.
    pub fn process_by_key(&self, key: Key) -> Option<&DeployedProcess> {
        if let Some(deployed) = self.process_versions.get(&key) {
            return Some(deployed);
        }
        self.processes.values().find(|d| d.key == key)
    }

    /// The exact deployed version of `process_id` with version number `version`,
    /// if one is retained. Used to resolve an explicit by-id + version create
    /// request to a concrete definition. Checks the full version-retention map
    /// first, then falls back to the latest-by-id index for engines restored
    /// from pre-retention snapshots (empty `process_versions`), where only the
    /// latest version per id — the only version legacy snapshots can preserve —
    /// is still available in `processes`.
    pub fn process_version(&self, process_id: &str, version: i32) -> Option<&DeployedProcess> {
        if let Some(deployed) = self
            .process_versions
            .values()
            .find(|d| d.definition.id == process_id && d.version == version)
        {
            return Some(deployed);
        }
        self.processes
            .get(process_id)
            .filter(|d| d.version == version)
    }

    /// The exact deployed DRG identified by its unique `key`, if retained.
    /// Checks the version-retention map first, then falls back to the
    /// latest-by-id index for engines restored from pre-retention snapshots
    /// (where `decision_requirements_versions` deserializes empty).
    pub fn drg_by_key(&self, key: Key) -> Option<&DeployedDrg> {
        if let Some(deployed) = self.decision_requirements_versions.get(&key) {
            return Some(deployed);
        }
        self.decision_requirements.values().find(|d| d.key == key)
    }

    /// The exact deployed version of DRG `drg_id` with version number `version`,
    /// if retained. Falls back to the latest-by-id index for pre-retention
    /// snapshots (only the latest version is available there).
    pub fn drg_version(&self, drg_id: &str, version: i32) -> Option<&DeployedDrg> {
        if let Some(deployed) = self
            .decision_requirements_versions
            .values()
            .find(|d| d.drg.id == drg_id && d.version == version)
        {
            return Some(deployed);
        }
        self.decision_requirements
            .get(drg_id)
            .filter(|d| d.version == version)
    }

    /// The exact deployed decision identified by its unique `key`, if retained.
    /// Checks the version-retention map first, then falls back to the
    /// latest-by-id index for pre-retention snapshots.
    pub fn decision_by_key(&self, key: Key) -> Option<&DeployedDecision> {
        if let Some(deployed) = self.decision_versions.get(&key) {
            return Some(deployed);
        }
        self.decisions.values().find(|d| d.key == key)
    }

    /// The exact deployed version of decision `decision_id` with version number
    /// `version`, if retained. Falls back to the latest-by-id index for
    /// pre-retention snapshots.
    pub fn decision_version(&self, decision_id: &str, version: i32) -> Option<&DeployedDecision> {
        if let Some(deployed) = self
            .decision_versions
            .values()
            .find(|d| d.decision_id == decision_id && d.version == version)
        {
            return Some(deployed);
        }
        self.decisions
            .get(decision_id)
            .filter(|d| d.version == version)
    }

    /// The exact deployed form identified by its unique `key`, if retained.
    /// Checks the version-retention map first, then falls back to the
    /// latest-by-id index for pre-retention snapshots.
    pub fn form_by_key(&self, key: Key) -> Option<&DeployedForm> {
        if let Some(deployed) = self.form_versions.get(&key) {
            return Some(deployed);
        }
        self.forms.values().find(|f| f.key == key)
    }

    /// The exact deployed version of form `form_id` with version number
    /// `version`, if retained. Falls back to the latest-by-id index for
    /// pre-retention snapshots.
    pub fn form_version(&self, form_id: &str, version: i32) -> Option<&DeployedForm> {
        if let Some(deployed) = self
            .form_versions
            .values()
            .find(|f| f.form_id == form_id && f.version == version)
        {
            return Some(deployed);
        }
        self.forms.get(form_id).filter(|f| f.version == version)
    }

    /// The exact deployed generic resource identified by its unique `key`, if
    /// retained. Checks the version-retention map first, then falls back to the
    /// latest-by-id index for pre-retention snapshots.
    pub fn resource_by_key(&self, key: Key) -> Option<&DeployedResource> {
        if let Some(deployed) = self.resource_versions.get(&key) {
            return Some(deployed);
        }
        self.resources.values().find(|r| r.key == key)
    }

    /// The exact deployed version of resource `resource_id` with version number
    /// `version`, if retained. Falls back to the latest-by-id index for
    /// pre-retention snapshots.
    pub fn resource_version(&self, resource_id: &str, version: i32) -> Option<&DeployedResource> {
        if let Some(deployed) = self
            .resource_versions
            .values()
            .find(|r| r.resource_id == resource_id && r.version == version)
        {
            return Some(deployed);
        }
        self.resources
            .get(resource_id)
            .filter(|r| r.version == version)
    }

    /// Count of jobs that represent *live* runnable congestion — `Created`
    /// (waiting for a worker, in the activatable index) plus `Activated`
    /// (leased, in-flight at a worker, in the activated index). Terminal jobs
    /// (`Completed`/`Failed`/`Errored`) are deliberately excluded: they are
    /// deindexed from both sets the instant they settle (see
    /// [`resync_job_index`]) but linger in `jobs` until their owning instance is
    /// evicted by the exporter. Counting `jobs.len()` instead would fold those
    /// dead, un-evicted terminal jobs into the backpressure reading, so an
    /// exporter that falls behind (or is stalled by a locked read-model store)
    /// silently inflates the admission/governor congestion signal and sheds
    /// legitimate new work — a self-inflicted throughput collapse that never
    /// clears, since the leaked terminal jobs are never re-exported. The two
    /// indices are disjoint and kept in lockstep with `jobs`, so their combined
    /// size is the exact live backlog at `O(#job_types)`.
    pub fn live_job_count(&self) -> usize {
        self.activated_jobs.len()
            + self
                .activatable_jobs
                .values()
                .map(|set| set.len())
                .sum::<usize>()
    }

    /// Removes `key` from both job indices (the activatable set under
    /// `job_type`, and the activated set), pruning an emptied per-type set.
    /// Used when a job is dropped from `jobs` entirely (eviction), where
    /// [`resync_job_index`] cannot run because the job is already gone. `priority`
    /// is the dropped job's priority, needed to locate its ordering slot.
    pub fn deindex_job(&mut self, job_type: &str, key: Key, priority: i32) {
        if let Some(set) = self.activatable_jobs.get_mut(job_type) {
            set.remove(&activation_order(priority, key));
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
    let order = activation_order(job.priority, job_key);
    if job_state == JobState::Created {
        state
            .activatable_jobs
            .entry(job_type)
            .or_default()
            .insert(order);
    } else if let Some(set) = state.activatable_jobs.get_mut(&job_type) {
        set.remove(&order);
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
            let deployed = DeployedProcess {
                key: *process_definition_key,
                version: *version,
                definition: process.clone(),
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
                    multi_instances: HashMap::new(),
                    adhoc_instances: HashMap::new(),
                    scope_parents: HashMap::new(),
                    scope_variables: HashMap::new(),
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
            worker,
            deadline,
            activated_at,
            ..
        } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.state = JobState::Activated;
                job.worker = Some(worker.clone());
                job.deadline = Some(*deadline);
                job.activated_at = *activated_at;
                // Freeze the requested lock duration at activation, from the two
                // instants the event carries (deadline = activated_at + timeout).
                // Immune to later UpdateJobTimeout extensions that move `deadline`.
                job.activation_timeout = activated_at.map(|a| deadline.saturating_sub(a));
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
            job_key, retries, ..
        } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.retries = *retries;
                job.worker = None;
                job.deadline = None;
                job.activated_at = None;
                job.activation_timeout = None;
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
                job.activated_at = None;
                job.activation_timeout = None;
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
            // activatable pool so a worker can pick it up again.
            if let Some(job_key) = job_key {
                if let Some(job) = state.jobs.get_mut(job_key) {
                    job.state = JobState::Created;
                    job.worker = None;
                    job.deadline = None;
                    job.activated_at = None;
                    job.activation_timeout = None;
                }
                resync_job_index(state, *job_key);
            }
        }

        Event::ProcessInstanceCompleted { instance_key } => {
            let terminal_pid = non_terminal_process_id(state, instance_key);
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.state = ProcessInstanceState::Completed;
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
                instance.active.clear();
                instance.scopes.clear();
                instance.incidents.clear();
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
        for key in [
            "order-1",
            "order-2",
            "x",
            "really-long-correlation-key-value",
        ] {
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

#[cfg(test)]
mod version_lookup_tests {
    use super::{DeployedProcess, State};
    use crate::model::ProcessBuilder;

    fn deployed(id: &str, key: u64, version: i32) -> DeployedProcess {
        let definition = ProcessBuilder::new(id)
            .start_event("start")
            .build()
            .expect("valid definition");
        DeployedProcess {
            key,
            version,
            definition,
        }
    }

    /// A pre-retention snapshot deserializes `process_versions` empty (it is
    /// `serde(default)`), but `processes` still holds the latest deployed
    /// definition and its key. A by-key create for that key must still resolve.
    #[test]
    fn process_by_key_falls_back_to_latest_by_id_index_for_legacy_snapshots() {
        let mut state = State::default();
        let latest = deployed("order", 42, 3);
        state.processes.insert("order".to_string(), latest.clone());
        // process_versions is intentionally empty (legacy snapshot).
        assert!(state.process_versions.is_empty());

        let resolved = state
            .process_by_key(42)
            .expect("by-key resolves via fallback");
        assert_eq!(resolved.key, 42);
        assert_eq!(resolved.version, 3);
        // An unknown key is still absent.
        assert!(state.process_by_key(99).is_none());
    }

    /// By-id + version create for the *latest* version must resolve from the
    /// latest-by-id index when `process_versions` is empty (legacy snapshot);
    /// only that version is recoverable from such a snapshot.
    #[test]
    fn process_version_falls_back_to_latest_by_id_index_for_legacy_snapshots() {
        let mut state = State::default();
        let latest = deployed("order", 42, 3);
        state.processes.insert("order".to_string(), latest);
        assert!(state.process_versions.is_empty());

        let resolved = state
            .process_version("order", 3)
            .expect("latest version resolves via fallback");
        assert_eq!(resolved.version, 3);
        assert_eq!(resolved.key, 42);
        // A non-latest version is genuinely unrecoverable from a legacy snapshot.
        assert!(state.process_version("order", 2).is_none());
        assert!(state.process_version("order", 4).is_none());
    }

    /// When `process_versions` is populated it takes precedence and retains
    /// historical versions the latest-by-id index has overwritten.
    #[test]
    fn version_retention_map_takes_precedence_and_retains_history() {
        let mut state = State::default();
        let v1 = deployed("order", 10, 1);
        let v2 = deployed("order", 20, 2);
        state.processes.insert("order".to_string(), v2.clone());
        state.process_versions.insert(10, v1);
        state.process_versions.insert(20, v2);

        assert_eq!(state.process_by_key(10).expect("v1 retained").version, 1);
        assert_eq!(state.process_by_key(20).expect("v2 retained").version, 2);
        assert_eq!(state.process_version("order", 1).expect("v1").key, 10);
        assert_eq!(state.process_version("order", 2).expect("v2").key, 20);
    }
}
