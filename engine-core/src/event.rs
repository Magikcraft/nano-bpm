//! Events: immutable facts about what happened.
//!
//! Events are the engine's source of truth. They are produced by the processor
//! and consumed by [`crate::state::apply`] (the sole mutator). Because each event
//! carries enough data to rebuild state, the event stream is a replayable log —
//! persist it however you like and replay to recover.

use std::collections::HashMap;

use crate::model::{ElementId, ProcessDefinition, Value};
use crate::state::{IncidentKind, Key, MessageSubscriptionKind, TimerKind};

/// A fact emitted by the engine. The ordering of a command's returned events is
/// the order in which they occurred.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Event {
    /// A `POST /v2/deployments` command was accepted. Emitted **once per deploy
    /// call**, unconditionally, before any [`Event::ProcessDeployed`] events
    /// that may follow — mirroring Zeebe's `DeploymentIntent.CREATED` (see
    /// `DeploymentCreateProcessor.java:224`). The key identifies the API call
    /// (not the artefact), is what the client sees in the response envelope,
    /// and is the handle used by audit-log filters. The applier is a strict
    /// no-op today; state persistence for later `GET /deployments/{key}`
    /// retrieval is tracked separately (issue #47, Option A).
    ///
    /// Emitting on every call — even a pure-duplicate deploy that mints no
    /// [`Event::ProcessDeployed`] — is what keeps the spec's
    /// `DeploymentKey: LongKey` (pattern `^-?[0-9]+$`) contract satisfied. An
    /// empty deploymentKey trips `Long.parseLong` in every stock C8 client.
    DeploymentCreated { deployment_key: Key },

    /// A process definition was registered as part of a deployment. The engine
    /// assigns the `deployment_key` (shared by every resource in the same
    /// deployment), a unique `process_definition_key`, and a `version` that
    /// increments per process id across deployments.
    ProcessDeployed {
        deployment_key: Key,
        process_definition_key: Key,
        version: i32,
        process: ProcessDefinition,
    },

    /// A decision requirements graph (DRG, one parsed `.dmn` resource) was
    /// registered as part of a deployment. Carries the parsed graph so the engine
    /// can evaluate its decisions natively (`businessRuleTask`, EvaluateDecision).
    DecisionRequirementsDeployed {
        deployment_key: Key,
        decision_requirements_key: Key,
        version: i32,
        drg: crate::dmn::DecisionRequirementsGraph,
    },

    /// A single decision inside a deployed DRG was registered, indexed for lookup
    /// by id. One [`Event::DecisionRequirementsDeployed`] emits one of these per
    /// decision it contains.
    DecisionDeployed {
        deployment_key: Key,
        decision_requirements_key: Key,
        decision_key: Key,
        decision_id: String,
        decision_name: String,
        version: i32,
    },

    /// A decision was evaluated — by a `businessRuleTask` (with `instance_key` /
    /// `element_id` set) or by the standalone EvaluateDecision API (both `0` /
    /// empty). Carries the root output and the per-decision audit trail for
    /// exporter parity with Zeebe's decision-evaluation records.
    DecisionEvaluated {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        decision_key: Key,
        decision_id: String,
        decision_output: Value,
        evaluated_decisions: Vec<crate::dmn::EvaluatedDecision>,
        /// Logical instant the decision was evaluated (the engine's clock
        /// reading for the command). Defaulted to `0` so journals written before
        /// this field existed still replay.
        #[cfg_attr(feature = "serde", serde(default))]
        evaluated_at: u64,
    },

    /// A decision instance (all rows sharing `decision_evaluation_key`, i.e. the
    /// root decision key of one [`Event::DecisionEvaluated`]) was marked for
    /// deletion via the DeleteDecisionInstance management API. `instance_key` is
    /// the owning process instance, carried purely so this event is journaled and
    /// projected on the same partition/shard as the `DecisionEvaluated` it retracts
    /// (the read model deletes the matching rows). Audit/projection-only: no core
    /// engine state to mutate.
    DecisionInstanceDeleted {
        instance_key: Key,
        decision_evaluation_key: Key,
    },

    /// A new process instance was created (carries a single token at its start
    /// event) with its initial variables. `created_at` is the logical instant
    /// the instance was started, carried on the command (the engine never reads
    /// a wall clock); it is the instance's start date. Defaulted to `0` so
    /// journals written before this field existed still replay. `tags` and
    /// `business_id` are user-supplied metadata, defaulted for older journals.
    ProcessInstanceCreated {
        instance_key: Key,
        process_id: String,
        variables: HashMap<String, Value>,
        #[cfg_attr(feature = "serde", serde(default))]
        created_at: u64,
        #[cfg_attr(feature = "serde", serde(default))]
        tags: Vec<String>,
        #[cfg_attr(feature = "serde", serde(default))]
        business_id: Option<String>,
    },

    /// Variables were merged into a process instance.
    VariablesUpdated {
        instance_key: Key,
        variables: HashMap<String, Value>,
    },

    /// Variables were merged into a specific variable scope (Part C hierarchical
    /// scoping). `scope_key` names the scope-owning element instance the values
    /// land in; when it equals `instance_key` (or `0`) they land in the root
    /// scope, matching [`Event::VariablesUpdated`]. The engine resolves Zeebe
    /// variable propagation *before* emitting, so each event targets exactly one
    /// scope and the applier is a plain merge.
    ScopedVariablesUpdated {
        instance_key: Key,
        scope_key: Key,
        variables: HashMap<String, Value>,
    },
    /// A non-root variable scope was opened (Part C). Registers `scope_key`
    /// (a scope-owning element instance: sub-process, multi-instance body or
    /// child) with its `parent_scope_key` in the instance's scope tree, so reads
    /// resolve upward and local variables can be held against it.
    VariableScopeCreated {
        instance_key: Key,
        scope_key: Key,
        parent_scope_key: Key,
    },
    /// A non-root variable scope was closed (Part C): its local variables are
    /// dropped and its tree entry removed. Emitted as the owning element instance
    /// completes or is terminated.
    VariableScopeDestroyed { instance_key: Key, scope_key: Key },

    /// An element instance entered `ACTIVATING`.
    ElementActivating {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// An element instance reached `ACTIVATED`.
    ElementActivated {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        /// The element instance of the enclosing embedded sub-process this
        /// instance lives in, or `0` for the process-level (root) scope. Used to
        /// track token scopes for sub-process completion and interruption.
        /// Defaulted to `0` so journals written before sub-processes existed
        /// still replay.
        #[cfg_attr(feature = "serde", serde(default))]
        scope: Key,
    },
    /// An element instance entered `COMPLETING`.
    ElementCompleting {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// An element instance reached `COMPLETED`.
    ElementCompleted {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },

    /// A token moved along a sequence flow from one element to another.
    SequenceFlowTaken {
        instance_key: Key,
        from: ElementId,
        to: ElementId,
    },

    /// A parallel-gateway join element instance was opened on the first arriving
    /// token; subsequent tokens accumulate against it.
    ParallelJoinOpened {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// A token arrived at an open parallel-gateway join.
    ParallelJoinTokenArrived {
        instance_key: Key,
        element_id: ElementId,
    },
    /// A parallel-gateway join fired (all incoming tokens present); its counters
    /// are cleared.
    ParallelJoinReset {
        instance_key: Key,
        element_id: ElementId,
    },

    /// A job was created for a service task; the token now rests until the job
    /// is completed. `created_at` is the logical instant it was created (carried
    /// on the event so replay reconstructs the same timestamp). `priority` is the
    /// *resolved* job-activation priority (FEEL evaluated against the instance
    /// variables, or a literal; default 50) — higher priority is activated first.
    JobCreated {
        job_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        job_type: String,
        #[cfg_attr(feature = "serde", serde(default))]
        created_at: u64,
        #[cfg_attr(
            feature = "serde",
            serde(default = "crate::state::default_job_priority")
        )]
        priority: i32,
        #[cfg_attr(
            feature = "serde",
            serde(default = "crate::state::default_job_retries")
        )]
        retries: i32,
    },
    /// A job was created for one execution listener in an element's sequential
    /// listener chain (ADR 0037). Distinct from [`Event::JobCreated`] so that
    /// listener-free models never produce it and their log stays byte-identical.
    /// `event_type` is the transition it fires on, `listener_index` its 0-based
    /// position in the element's listener list for that transition, and `scope`
    /// the element's enclosing scope (carried so completing the job can drive the
    /// next listener or resume the lifecycle transition). The token rests until
    /// the job completes.
    ExecutionListenerJobCreated {
        job_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        job_type: String,
        event_type: crate::model::ListenerEventType,
        listener_index: usize,
        scope: Key,
        #[cfg_attr(feature = "serde", serde(default))]
        created_at: u64,
        #[cfg_attr(
            feature = "serde",
            serde(default = "crate::state::default_job_retries")
        )]
        retries: i32,
    },
    /// A job was activated by a worker and locked until `deadline` (a logical
    /// instant supplied by the caller). Another worker cannot activate it until
    /// the lock expires, but any holder of the key may complete it.
    JobActivated {
        job_key: Key,
        instance_key: Key,
        worker: String,
        deadline: u64,
    },
    /// A job's activation lock expired (its `deadline` passed); it becomes
    /// activatable again. Emitted by an `ExpireJobs` tick.
    JobLockExpired { job_key: Key, instance_key: Key },
    /// A worker reported that a job failed, setting its remaining `retries`. With
    /// retries left the job becomes activatable again; with none it parks and an
    /// [`Event::IncidentRaised`] follows.
    JobFailed {
        job_key: Key,
        instance_key: Key,
        retries: i32,
    },
    /// A worker threw a business error from a job. The job is consumed; either a
    /// matching error boundary event interrupts the activity, or an
    /// [`Event::IncidentRaised`] follows when no boundary catches `error_code`.
    JobErrorThrown {
        job_key: Key,
        instance_key: Key,
        error_code: String,
    },
    /// A job was completed. `created_at` is the logical instant the job was
    /// created (carried through from job state) so the server can observe the
    /// job's end-to-end sojourn (create→complete) at the completion site. `job_type`
    /// is carried so that sojourn can be reported *per job type* — the reporting
    /// surface that lets an operator localize external/worker strain to a specific
    /// process/job type (one type's sojourn stretching while the engine's internal
    /// command latency stays flat = a slow downstream for that type, not our
    /// congestion). Both are observational: `created_at` is `0` and `job_type` is
    /// empty for jobs created before the engine carried the fields; neither is used
    /// by replay.
    JobCompleted {
        job_key: Key,
        instance_key: Key,
        #[cfg_attr(feature = "serde", serde(default))]
        created_at: u64,
        #[cfg_attr(feature = "serde", serde(default))]
        job_type: String,
    },
    /// A job's remaining retries were updated (e.g. by an operator recovering a
    /// parked job before resolving its incident). Does not change job state.
    JobRetriesUpdated {
        job_key: Key,
        instance_key: Key,
        retries: i32,
        /// Optional caller audit correlation id (Camunda `operationReference`).
        #[cfg_attr(feature = "serde", serde(default))]
        operation_reference: Option<i64>,
    },

    /// A job's activation lock was extended: its `deadline` was reset to a later
    /// logical instant while it stayed `Activated` (e.g. a worker holding a
    /// long-running job open). Does not change job state; the holder keeps the
    /// lock until this new `deadline` passes.
    JobTimeoutUpdated {
        job_key: Key,
        instance_key: Key,
        deadline: u64,
        /// Optional caller audit correlation id (Camunda `operationReference`).
        #[cfg_attr(feature = "serde", serde(default))]
        operation_reference: Option<i64>,
    },

    /// A user task was created for a `userTask` element; the token now rests
    /// until the task is completed. `created_at` is the logical instant it was
    /// created, carried on the event so replay reconstructs the same timestamp.
    /// The assignment/scheduling/priority attributes are the *resolved* values
    /// (FEEL evaluated against the instance variables, or literals) declared on
    /// the BPMN element. `assignee` is `None` when no assignee was declared.
    UserTaskCreated {
        user_task_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        created_at: u64,
        assignee: Option<String>,
        candidate_groups: Vec<String>,
        candidate_users: Vec<String>,
        due_date: Option<String>,
        follow_up_date: Option<String>,
        priority: i32,
    },
    /// A user task's assignee was set (or cleared, when `assignee` is `None`).
    UserTaskAssigned {
        user_task_key: Key,
        instance_key: Key,
        assignee: Option<String>,
    },
    /// A user task's attributes were changed via the update endpoint. Each field
    /// is `Some` only when that attribute was part of the changeset; an empty
    /// list or empty/`None` date resets the attribute.
    UserTaskUpdated {
        user_task_key: Key,
        instance_key: Key,
        candidate_groups: Option<Vec<String>>,
        candidate_users: Option<Vec<String>>,
        due_date: Option<Option<String>>,
        follow_up_date: Option<Option<String>>,
        priority: Option<i32>,
    },
    /// A user task was completed; the parked token resumes along the task's
    /// outgoing flow.
    UserTaskCompleted {
        user_task_key: Key,
        instance_key: Key,
    },
    /// A user task was cancelled because its activity/instance was terminated.
    UserTaskCanceled {
        user_task_key: Key,
        instance_key: Key,
    },

    /// A task-listener job was created for one listener in a user task's
    /// sequential chain (ADR 0037 §6). Mirrors [`Event::ExecutionListenerJobCreated`]
    /// but gates a *user-task* transition rather than an element lifecycle
    /// transition. Only emitted for user tasks that declare task listeners, so
    /// listener-free user tasks are byte-identical to the pre-task-listener
    /// engine.
    TaskListenerJobCreated {
        job_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        user_task_key: Key,
        job_type: String,
        event_type: crate::model::TaskListenerEventType,
        listener_index: usize,
        #[cfg_attr(feature = "serde", serde(default))]
        created_at: u64,
        #[cfg_attr(
            feature = "serde",
            serde(default = "crate::state::default_job_retries")
        )]
        retries: i32,
    },
    /// A user task began deferring a lifecycle transition behind its task
    /// listeners (ADR 0037 §6). Records the in-flight transition on the task
    /// until its listener chain drains (commit) or a listener denies it.
    UserTaskTransitionDeferred {
        user_task_key: Key,
        instance_key: Key,
        pending: crate::state::PendingUserTaskTransition,
    },
    /// A task listener returned corrections to user-task data; they merge into
    /// the deferred transition's accumulated corrections (ADR 0037 §6).
    UserTaskCorrectionsApplied {
        user_task_key: Key,
        instance_key: Key,
        corrections: crate::model::UserTaskCorrections,
    },
    /// A user task's deferred transition was resolved (committed after its
    /// listener chain drained, or denied by a listener); the pending transition
    /// is cleared. The actual state change (assign/update/complete/cancel) is
    /// carried by the accompanying lifecycle event on commit. `denied` is
    /// `Some(reason)` when a task listener denied the transition (the task
    /// returns to its prior available state) and `None` on a normal commit.
    UserTaskTransitionResolved {
        user_task_key: Key,
        instance_key: Key,
        #[cfg_attr(feature = "serde", serde(default))]
        denied: Option<String>,
    },

    /// An incident was raised (e.g. an exclusive gateway found no matching flow,
    /// or a job exhausted its retries); the token is parked until the incident
    /// is resolved. `job_key` is `Some` only for recoverable job-incidents.
    /// `created_at` is the logical instant the incident was raised, carried on
    /// the event so replay reconstructs the same timestamp.
    IncidentRaised {
        incident_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        kind: IncidentKind,
        reason: String,
        job_key: Option<Key>,
        created_at: u64,
    },
    /// An incident was resolved; the engine then retries the work that failed
    /// (see [`crate::Command::ResolveIncident`]). For a job-incident (`job_key`
    /// is `Some`) the parked job returns to the activatable pool. The record is
    /// retained (transitioned to `Resolved`) with `resolved_at` and any
    /// `operation_reference` for audit.
    IncidentResolved {
        incident_key: Key,
        instance_key: Key,
        job_key: Option<Key>,
        resolved_at: u64,
        operation_reference: Option<i64>,
    },

    /// The last token of a process instance was consumed; the instance is done.
    ProcessInstanceCompleted { instance_key: Key },

    /// A process instance was cancelled by an operator: every token was
    /// discarded and the instance transitions to `Terminated` (it did not
    /// complete normally). The resource-cancellation events this command
    /// produced (`JobCanceled`, `TimerCanceled`, `MessageSubscriptionCanceled`)
    /// precede it; this event closes any still-active incident on the instance.
    ProcessInstanceTerminated { instance_key: Key },

    /// Cancellation of a process instance began, but one or more user tasks are
    /// running their `canceling` task listeners (ADR 0037 §6). The instance's
    /// other tokens are already discarded (their cancellation events precede
    /// this one); it moves to `Terminating` and finishes with
    /// [`Event::ProcessInstanceTerminated`] once the last canceling chain drains.
    ProcessInstanceTerminating { instance_key: Key },

    /// A timer was armed: either on a timer intermediate catch event (the token
    /// rests on it) or as an interrupting boundary timer on an activity (the
    /// activity runs as normal until the timer fires). `due_at` is the logical
    /// instant it fires and `kind` records what it guards, both carried on the
    /// event so replay reconstructs the timer exactly.
    TimerCreated {
        timer_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        due_at: u64,
        kind: TimerKind,
    },
    /// A due timer fired. For an intermediate catch event its token is released
    /// along the event's outgoing flow; for an interrupting boundary timer the
    /// attached activity is interrupted and the boundary's outgoing flow runs
    /// (the job-cancellation, element-completion and sequence-flow events follow).
    TimerTriggered {
        timer_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// An armed timer was cancelled before firing because the element it guarded
    /// left the flow first (e.g. a boundary timer whose activity completed
    /// normally, or a sibling boundary timer when another fired).
    TimerCanceled {
        timer_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// A job was cancelled because its activity was interrupted by a boundary
    /// event firing.
    JobCanceled { job_key: Key, instance_key: Key },

    /// A message was published. In nano messages are not buffered, so this
    /// records no durable state; it carries the minted `message_key` (returned to
    /// the host and used to restore the key generator on replay) and heads the
    /// [`Event::MessageCorrelated`] events the same command produced.
    MessagePublished {
        message_key: Key,
        message_name: String,
        correlation_key: String,
    },
    /// A message subscription was opened: either on a message intermediate catch
    /// event (the token rests on it) or as an interrupting message boundary on an
    /// activity (the activity runs as normal until a message is correlated).
    /// `correlation_key` is the resolved correlation value captured at open time,
    /// carried on the event so replay reconstructs the subscription exactly.
    MessageSubscriptionCreated {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        message_name: String,
        correlation_key: String,
        kind: MessageSubscriptionKind,
    },
    /// The **instance** partition parked a token on a message catch element whose
    /// canonical subscription lives on another partition (`hash(correlation_key)`,
    /// see [`crate::subscription_partition`]). This records the instance
    /// partition's pending view; the host routes an
    /// [`crate::Command::OpenMessageSubscription`] to the message partition (which
    /// records the canonical [`Event::MessageSubscriptionCreated`]) and later a
    /// [`crate::Command::CorrelateMessageSubscription`] continuation back here to
    /// advance the token. Only emitted when `num_partitions > 1` and the key
    /// hashes off-partition — a single-partition host never produces it, so its
    /// log is byte-identical to before.
    MessageSubscriptionOpening {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        message_name: String,
        correlation_key: String,
        kind: MessageSubscriptionKind,
    },
    /// A published message correlated to an open subscription. For an
    /// intermediate catch event its token is released along the event's outgoing
    /// flow; for an interrupting boundary the attached activity is interrupted and
    /// the boundary's outgoing flow runs (the job-cancellation, element-completion
    /// and sequence-flow events follow). `message_key` ties it back to the
    /// [`Event::MessagePublished`] that produced it.
    MessageCorrelated {
        subscription_key: Key,
        message_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// A published message matched a subscription on the **message** partition
    /// whose instance lives on another partition. Settles the canonical
    /// subscription (exactly like [`Event::MessageCorrelated`]) but does **not**
    /// advance the token here; it carries the full continuation payload (kind +
    /// the message's variables) so the host can route a
    /// [`crate::Command::CorrelateMessageSubscription`] to the instance partition,
    /// where the token actually advances. Only produced when `num_partitions > 1`.
    RemoteMessageCorrelation {
        subscription_key: Key,
        message_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        kind: MessageSubscriptionKind,
        variables: HashMap<String, Value>,
    },
    /// An open message subscription was cancelled before correlating because the
    /// element it guarded left the flow first (e.g. a boundary subscription whose
    /// activity completed normally, or a sibling boundary subscription when
    /// another boundary on the same activity fired).
    MessageSubscriptionCanceled {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },

    /// A signal was broadcast. Signals are not buffered, so this records no
    /// durable state; it carries the minted `signal_key` (returned to the host
    /// and used to restore the key generator on replay) and heads the
    /// [`Event::SignalCorrelated`] events the same command produced.
    SignalBroadcast {
        signal_key: Key,
        signal_name: String,
    },
    /// A signal subscription was opened: on a signal intermediate catch event
    /// (the token rests on it) or as a signal boundary on an activity. Signals
    /// correlate by **name only**, so there is no correlation key.
    SignalSubscriptionCreated {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        signal_name: String,
        kind: MessageSubscriptionKind,
    },
    /// A broadcast signal correlated to an open subscription. For an intermediate
    /// catch its token is released along the event's outgoing flow; for an
    /// interrupting boundary the attached activity is interrupted and the
    /// boundary's outgoing flow runs. `signal_key` ties it back to the
    /// [`Event::SignalBroadcast`] that produced it.
    SignalCorrelated {
        subscription_key: Key,
        signal_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// An open signal subscription was cancelled before correlating because the
    /// element it guarded left the flow first (mirrors
    /// [`Event::MessageSubscriptionCanceled`]).
    SignalSubscriptionCanceled {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// A conditional subscription was opened: on a conditional intermediate catch
    /// event (the token rests on it) or as a conditional boundary on an activity.
    /// It carries the FEEL `condition` and the set of root variable names the
    /// condition references (`referenced_vars`), so the engine re-evaluates it
    /// only when one of those variables changes. Unlike message/signal
    /// subscriptions it has no external trigger command: the engine evaluates it
    /// on open and on each change to a referenced variable, firing when the
    /// condition becomes `true` (see [`Event::ConditionalTriggered`]).
    ConditionalSubscriptionCreated {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        condition: String,
        referenced_vars: Vec<String>,
        kind: MessageSubscriptionKind,
    },
    /// A conditional subscription's condition evaluated `true`. For an
    /// intermediate catch or an interrupting boundary this settles the
    /// subscription (it fires once); for a non-interrupting boundary it stays
    /// open (each satisfying variable change spawns another token). The token
    /// advance (catch completion / boundary interrupt / parallel spawn) is
    /// carried by the surrounding events the same command produced.
    ConditionalTriggered {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// An open conditional subscription was cancelled before firing because the
    /// element it guarded left the flow first (mirrors
    /// [`Event::SignalSubscriptionCanceled`]).
    ConditionalSubscriptionCanceled {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// A multi-instance body activated: its `input_collection` was evaluated to
    /// `items` and one child of `element_id` will run per item (all at once when
    /// `sequential` is `false`, one after another when `true`). Carries the
    /// resolved loop configuration so the body's runtime state is fully
    /// reconstructable from the log. `body_key` is the body element instance (the
    /// scope the children run in).
    MultiInstanceActivated {
        instance_key: Key,
        body_key: Key,
        element_id: ElementId,
        sequential: bool,
        items: Vec<Value>,
        input_element: Option<String>,
        output_collection: Option<String>,
        output_element: Option<String>,
        completion_condition: Option<String>,
    },
    /// A multi-instance child instance was activated at `index` (0-based) into the
    /// input collection. `local_variables` are the child's scope bindings (the
    /// `input_element` item, when named, and `loopCounter`), overlaid on the
    /// instance variables when the child's job is activated or its FEEL evaluated.
    MultiInstanceChildActivated {
        instance_key: Key,
        body_key: Key,
        child_key: Key,
        index: usize,
        local_variables: HashMap<String, Value>,
    },
    /// A multi-instance child completed: its `output` (the evaluated
    /// `output_element`, if any) is recorded at `index` in the body's collected
    /// results, the child leaves the body's active set, and its local variable
    /// overlay is dropped.
    MultiInstanceChildCompleted {
        instance_key: Key,
        body_key: Key,
        child_key: Key,
        index: usize,
        output: Option<Value>,
    },
    /// A multi-instance body completed (all children finished, or a completion
    /// condition fired). Its runtime state is dropped; the aggregated
    /// `output_collection` (when named) is written by a surrounding
    /// [`Event::VariablesUpdated`] and its outgoing flow taken by the surrounding
    /// element-completion events.
    MultiInstanceCompleted { instance_key: Key, body_key: Key },
    /// An ad-hoc sub-process container was activated (ADR 0023 seam 2). The
    /// container element instance is the ad-hoc token scope; its agent job is
    /// created by a surrounding [`Event::JobCreated`]. Carries the resolved
    /// `output_collection`/`output_element` so the runtime state reconstructs
    /// from the log. `container_key` is the container element instance.
    AdHocActivated {
        instance_key: Key,
        container_key: Key,
        element_id: ElementId,
        output_collection: Option<String>,
        output_element: Option<String>,
    },
    /// An ad-hoc tool child instance was activated (by an agent activate-element
    /// instruction) inside `container_key`'s scope. `local_variables` are the
    /// child's scope bindings (the instruction's seed variables), overlaid when
    /// the child's job is activated or its FEEL evaluated. The child joins the
    /// container's active set.
    AdHocToolActivated {
        instance_key: Key,
        container_key: Key,
        child_key: Key,
        local_variables: HashMap<String, Value>,
    },
    /// An ad-hoc tool child completed: its `output` (the container's evaluated
    /// `output_element`, if any) is appended to the container's accumulated
    /// results and the child leaves the active set. Its local variable overlay
    /// is dropped by the child's surrounding `ElementCompleted`.
    AdHocToolCompleted {
        instance_key: Key,
        container_key: Key,
        child_key: Key,
        output: Option<Value>,
    },
    /// An ad-hoc container's agent job re-emitted for the next turn (a new
    /// activate-element cycle). Bumps the container's iteration counter. The new
    /// job itself is carried by a surrounding [`Event::JobCreated`].
    AdHocIterated {
        instance_key: Key,
        container_key: Key,
    },
    /// An ad-hoc container completed (the agent signalled completion, or no tools
    /// remained and none were requested, or a cancel was requested). Its runtime
    /// state is dropped; the aggregated `output_collection` (when named) is
    /// written by a surrounding [`Event::VariablesUpdated`] and its outgoing flow
    /// taken by the surrounding element-completion events. `cancelled` records
    /// whether completion was a `cancel_remaining_instances` request (which tears
    /// down any still-active tools) versus a normal agent-signalled finish, so the
    /// distinction is durable on the journal for the trace read model and metrics.
    AdHocCompleted {
        instance_key: Key,
        container_key: Key,
        #[cfg_attr(feature = "serde", serde(default))]
        cancelled: bool,
    },
    /// The **instance** partition tore down a cross-partition parked
    /// subscription (state [`crate::state::MessageSubscriptionState::Opening`],
    /// recorded by [`Event::MessageSubscriptionOpening`]) because the element it
    /// guarded left the flow (cancel, normal completion or a sibling boundary
    /// firing). Marks the local placeholder cancelled and carries
    /// `message_name` + `correlation_key` so the host routes a
    /// [`crate::Command::CloseMessageSubscription`] to the message partition
    /// (`hash(correlation_key)`) to disarm the canonical record there. Mirrors
    /// [`Event::MessageSubscriptionOpening`]: only emitted when the canonical
    /// subscription lives off-partition, so a single-partition log never
    /// produces it and stays byte-identical.
    MessageSubscriptionClosing {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        message_name: String,
        correlation_key: String,
    },
    /// A process-level message start subscription was opened at deploy time: a
    /// later [`Event::MessagePublished`] with a matching `message_name` creates a
    /// new instance of `process_id`. Carried on the log so replay reconstructs
    /// the subscription.
    MessageStartSubscriptionCreated {
        process_definition_key: Key,
        process_id: String,
        message_name: String,
        start_element_id: ElementId,
    },
    /// A process-level timer start event was armed at deploy time, first due at
    /// `due_at`. When it fires a new instance of `process_id` is created.
    ProcessStartTimerArmed {
        timer_key: Key,
        process_definition_key: Key,
        process_id: String,
        start_element_id: ElementId,
        due_at: u64,
        interval_millis: u64,
        repeating: bool,
    },
    /// A process-level timer start event fired (a `ProcessInstanceCreated` and
    /// the new instance's flow events follow). `next_due_at` is `Some` for a
    /// cycle (the timer re-arms for that instant) and `None` for a one-shot (it
    /// is retained, never to fire again).
    ProcessStartTimerFired {
        timer_key: Key,
        next_due_at: Option<u64>,
    },
    /// A start event (message-start or timer-start, both hosted on the deploy
    /// partition) fired but, in a multi-partition cluster, the new instance is
    /// placed on `target_partition` instead of being created locally — so
    /// start-triggered instances spread across the cluster rather than piling
    /// onto partition 0. Carries the full creation payload; the host routes a
    /// [`crate::Command::DispatchStartInstance`] to `target_partition`, which
    /// mints the instance in its own namespace. Only emitted when
    /// `num_partitions > 1` and the chosen target is not this partition, so a
    /// single-partition log never produces it and stays byte-identical.
    StartInstanceDispatched {
        process_id: String,
        start_element_id: ElementId,
        variables: HashMap<String, Value>,
        #[cfg_attr(feature = "serde", serde(default))]
        tags: Vec<String>,
        #[cfg_attr(feature = "serde", serde(default))]
        business_id: Option<String>,
        target_partition: u64,
    },
}

impl Event {
    /// The process-instance key of a **terminal** transition (completed or
    /// terminated), if this event is one. Used by the follower-replica apply
    /// path to reclaim the hot-state shell of an instance the moment it reaches
    /// a terminal state — a replica has no read-model exporter to drive that
    /// eviction, so without this its terminal shells would accumulate unbounded.
    pub fn terminal_instance_key(&self) -> Option<Key> {
        match self {
            Event::ProcessInstanceCompleted { instance_key }
            | Event::ProcessInstanceTerminated { instance_key } => Some(*instance_key),
            _ => None,
        }
    }

    /// The process-instance key this event relates to, if any.
    ///
    /// Used by the engine to decide which instances to check for completion
    /// after a command settles.
    pub fn instance_key(&self) -> Option<Key> {
        match self {
            Event::ProcessInstanceCreated { instance_key, .. }
            | Event::VariablesUpdated { instance_key, .. }
            | Event::ScopedVariablesUpdated { instance_key, .. }
            | Event::VariableScopeCreated { instance_key, .. }
            | Event::VariableScopeDestroyed { instance_key, .. }
            | Event::ElementActivating { instance_key, .. }
            | Event::ElementActivated { instance_key, .. }
            | Event::ElementCompleting { instance_key, .. }
            | Event::ElementCompleted { instance_key, .. }
            | Event::SequenceFlowTaken { instance_key, .. }
            | Event::ParallelJoinOpened { instance_key, .. }
            | Event::ParallelJoinTokenArrived { instance_key, .. }
            | Event::ParallelJoinReset { instance_key, .. }
            | Event::JobCreated { instance_key, .. }
            | Event::ExecutionListenerJobCreated { instance_key, .. }
            | Event::JobActivated { instance_key, .. }
            | Event::JobLockExpired { instance_key, .. }
            | Event::JobFailed { instance_key, .. }
            | Event::JobErrorThrown { instance_key, .. }
            | Event::JobCompleted { instance_key, .. }
            | Event::JobRetriesUpdated { instance_key, .. }
            | Event::JobTimeoutUpdated { instance_key, .. }
            | Event::UserTaskCreated { instance_key, .. }
            | Event::UserTaskAssigned { instance_key, .. }
            | Event::UserTaskUpdated { instance_key, .. }
            | Event::UserTaskCompleted { instance_key, .. }
            | Event::UserTaskCanceled { instance_key, .. }
            | Event::TaskListenerJobCreated { instance_key, .. }
            | Event::UserTaskTransitionDeferred { instance_key, .. }
            | Event::UserTaskCorrectionsApplied { instance_key, .. }
            | Event::UserTaskTransitionResolved { instance_key, .. }
            | Event::IncidentRaised { instance_key, .. }
            | Event::IncidentResolved { instance_key, .. }
            | Event::TimerCreated { instance_key, .. }
            | Event::TimerTriggered { instance_key, .. }
            | Event::TimerCanceled { instance_key, .. }
            | Event::JobCanceled { instance_key, .. }
            | Event::MessageSubscriptionCreated { instance_key, .. }
            | Event::MessageSubscriptionOpening { instance_key, .. }
            | Event::MessageCorrelated { instance_key, .. }
            | Event::RemoteMessageCorrelation { instance_key, .. }
            | Event::MessageSubscriptionCanceled { instance_key, .. }
            | Event::SignalSubscriptionCreated { instance_key, .. }
            | Event::SignalCorrelated { instance_key, .. }
            | Event::SignalSubscriptionCanceled { instance_key, .. }
            | Event::ConditionalSubscriptionCreated { instance_key, .. }
            | Event::ConditionalTriggered { instance_key, .. }
            | Event::ConditionalSubscriptionCanceled { instance_key, .. }
            | Event::MultiInstanceActivated { instance_key, .. }
            | Event::MultiInstanceChildActivated { instance_key, .. }
            | Event::MultiInstanceChildCompleted { instance_key, .. }
            | Event::MultiInstanceCompleted { instance_key, .. }
            | Event::AdHocActivated { instance_key, .. }
            | Event::AdHocToolActivated { instance_key, .. }
            | Event::AdHocToolCompleted { instance_key, .. }
            | Event::AdHocIterated { instance_key, .. }
            | Event::AdHocCompleted { instance_key, .. }
            | Event::MessageSubscriptionClosing { instance_key, .. }
            | Event::ProcessInstanceCompleted { instance_key }
            | Event::DecisionEvaluated { instance_key, .. }
            | Event::DecisionInstanceDeleted { instance_key, .. }
            | Event::ProcessInstanceTerminated { instance_key } => Some(*instance_key),
            Event::ProcessInstanceTerminating { instance_key } => Some(*instance_key),
            Event::ProcessDeployed { .. }
            | Event::DecisionRequirementsDeployed { .. }
            | Event::DecisionDeployed { .. }
            | Event::DeploymentCreated { .. }
            | Event::MessagePublished { .. }
            | Event::SignalBroadcast { .. }
            | Event::MessageStartSubscriptionCreated { .. }
            | Event::ProcessStartTimerArmed { .. }
            | Event::ProcessStartTimerFired { .. }
            | Event::StartInstanceDispatched { .. } => None,
        }
    }

    /// The highest [`Key`] this event references in any field.
    ///
    /// Replay uses the maximum across the whole log to restore the engine's key
    /// generator past every key the original run assigned, so newly minted keys
    /// never collide with replayed ones. (Keys are only ever minted by the
    /// engine and stamped onto events, so the log is an exact record of them —
    /// including transient ones like completed element-instance keys that no
    /// longer appear in final state.)
    pub fn max_key(&self) -> Key {
        let mut m = self.instance_key().unwrap_or(0);
        match self {
            Event::ProcessDeployed {
                deployment_key,
                process_definition_key,
                ..
            } => m = m.max(*deployment_key).max(*process_definition_key),
            Event::DecisionRequirementsDeployed {
                deployment_key,
                decision_requirements_key,
                ..
            } => m = m.max(*deployment_key).max(*decision_requirements_key),
            Event::DecisionDeployed {
                deployment_key,
                decision_requirements_key,
                decision_key,
                ..
            } => {
                m = m
                    .max(*deployment_key)
                    .max(*decision_requirements_key)
                    .max(*decision_key)
            }
            Event::DecisionEvaluated {
                element_instance_key,
                decision_key,
                ..
            } => m = m.max(*element_instance_key).max(*decision_key),
            Event::DecisionInstanceDeleted {
                decision_evaluation_key,
                ..
            } => m = m.max(*decision_evaluation_key),
            Event::DeploymentCreated { deployment_key } => m = m.max(*deployment_key),
            Event::ElementActivating {
                element_instance_key,
                ..
            }
            | Event::ElementActivated {
                element_instance_key,
                ..
            }
            | Event::ElementCompleting {
                element_instance_key,
                ..
            }
            | Event::ElementCompleted {
                element_instance_key,
                ..
            }
            | Event::ParallelJoinOpened {
                element_instance_key,
                ..
            } => m = m.max(*element_instance_key),
            Event::JobCreated {
                job_key,
                element_instance_key,
                ..
            } => m = m.max(*job_key).max(*element_instance_key),
            Event::ExecutionListenerJobCreated {
                job_key,
                element_instance_key,
                scope,
                ..
            } => m = m.max(*job_key).max(*element_instance_key).max(*scope),
            Event::TaskListenerJobCreated {
                job_key,
                element_instance_key,
                user_task_key,
                ..
            } => {
                m = m
                    .max(*job_key)
                    .max(*element_instance_key)
                    .max(*user_task_key)
            }
            Event::JobActivated { job_key, .. }
            | Event::JobLockExpired { job_key, .. }
            | Event::JobFailed { job_key, .. }
            | Event::JobErrorThrown { job_key, .. }
            | Event::JobCompleted { job_key, .. }
            | Event::JobCanceled { job_key, .. }
            | Event::JobRetriesUpdated { job_key, .. }
            | Event::JobTimeoutUpdated { job_key, .. } => m = m.max(*job_key),
            Event::IncidentRaised {
                incident_key,
                element_instance_key,
                job_key,
                ..
            } => {
                m = m.max(*incident_key).max(*element_instance_key);
                if let Some(j) = job_key {
                    m = m.max(*j);
                }
            }
            Event::IncidentResolved {
                incident_key,
                job_key,
                ..
            } => {
                m = m.max(*incident_key);
                if let Some(j) = job_key {
                    m = m.max(*j);
                }
            }
            Event::TimerCreated {
                timer_key,
                element_instance_key,
                ..
            } => m = m.max(*timer_key).max(*element_instance_key),
            Event::TimerTriggered {
                timer_key,
                element_instance_key,
                ..
            } => m = m.max(*timer_key).max(*element_instance_key),
            Event::TimerCanceled {
                timer_key,
                element_instance_key,
                ..
            } => m = m.max(*timer_key).max(*element_instance_key),
            Event::MessagePublished { message_key, .. } => m = m.max(*message_key),
            Event::SignalBroadcast { signal_key, .. } => m = m.max(*signal_key),
            Event::SignalSubscriptionCreated {
                subscription_key,
                element_instance_key,
                ..
            }
            | Event::SignalSubscriptionCanceled {
                subscription_key,
                element_instance_key,
                ..
            } => m = m.max(*subscription_key).max(*element_instance_key),
            Event::ConditionalSubscriptionCreated {
                subscription_key,
                element_instance_key,
                ..
            }
            | Event::ConditionalTriggered {
                subscription_key,
                element_instance_key,
                ..
            }
            | Event::ConditionalSubscriptionCanceled {
                subscription_key,
                element_instance_key,
                ..
            } => m = m.max(*subscription_key).max(*element_instance_key),
            Event::MultiInstanceActivated { body_key, .. }
            | Event::MultiInstanceCompleted { body_key, .. } => m = m.max(*body_key),
            Event::MultiInstanceChildActivated {
                body_key,
                child_key,
                ..
            }
            | Event::MultiInstanceChildCompleted {
                body_key,
                child_key,
                ..
            } => m = m.max(*body_key).max(*child_key),
            Event::AdHocActivated { container_key, .. }
            | Event::AdHocIterated { container_key, .. }
            | Event::AdHocCompleted { container_key, .. } => m = m.max(*container_key),
            Event::AdHocToolActivated {
                container_key,
                child_key,
                ..
            }
            | Event::AdHocToolCompleted {
                container_key,
                child_key,
                ..
            } => m = m.max(*container_key).max(*child_key),
            Event::SignalCorrelated {
                subscription_key,
                signal_key,
                element_instance_key,
                ..
            } => {
                m = m
                    .max(*subscription_key)
                    .max(*signal_key)
                    .max(*element_instance_key)
            }
            Event::MessageSubscriptionCreated {
                subscription_key,
                element_instance_key,
                ..
            }
            | Event::MessageSubscriptionOpening {
                subscription_key,
                element_instance_key,
                ..
            }
            | Event::MessageSubscriptionCanceled {
                subscription_key,
                element_instance_key,
                ..
            }
            | Event::MessageSubscriptionClosing {
                subscription_key,
                element_instance_key,
                ..
            } => m = m.max(*subscription_key).max(*element_instance_key),
            Event::MessageCorrelated {
                subscription_key,
                message_key,
                element_instance_key,
                ..
            }
            | Event::RemoteMessageCorrelation {
                subscription_key,
                message_key,
                element_instance_key,
                ..
            } => {
                m = m
                    .max(*subscription_key)
                    .max(*message_key)
                    .max(*element_instance_key)
            }
            Event::MessageStartSubscriptionCreated {
                process_definition_key,
                ..
            } => m = m.max(*process_definition_key),
            Event::ProcessStartTimerArmed {
                timer_key,
                process_definition_key,
                ..
            } => m = m.max(*timer_key).max(*process_definition_key),
            Event::ProcessStartTimerFired { timer_key, .. } => m = m.max(*timer_key),
            Event::UserTaskCreated {
                user_task_key,
                element_instance_key,
                ..
            } => m = m.max(*user_task_key).max(*element_instance_key),
            Event::UserTaskAssigned { user_task_key, .. }
            | Event::UserTaskUpdated { user_task_key, .. }
            | Event::UserTaskCompleted { user_task_key, .. }
            | Event::UserTaskCanceled { user_task_key, .. }
            | Event::UserTaskTransitionDeferred { user_task_key, .. }
            | Event::UserTaskCorrectionsApplied { user_task_key, .. }
            | Event::UserTaskTransitionResolved { user_task_key, .. } => m = m.max(*user_task_key),
            _ => {}
        }
        m
    }
}
