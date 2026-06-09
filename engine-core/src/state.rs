//! State and the applier.
//!
//! [`State`] is the engine's working memory. [`apply`] is the **only** function
//! that mutates it, and it does so purely as a function of an [`Event`]. Keeping
//! every mutation here is what makes the engine deterministic and replayable:
//! replaying the same events over a fresh [`State`] reconstructs it exactly.

use std::collections::HashMap;

use crate::event::Event;
use crate::model::{ElementId, ProcessDefinition, Value};

/// A monotonically increasing identifier for instances, element instances and
/// jobs. (Zeebe encodes the partition id into keys; nano just increments.)
pub type Key = u64;

/// Lifecycle state of a process instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessInstanceState {
    Active,
    Completed,
}

/// Lifecycle state of a job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

/// A running (or completed) process instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessInstance {
    pub key: Key,
    pub process_id: String,
    pub state: ProcessInstanceState,
    /// Currently-active element instances, keyed by element-instance key. An
    /// element instance is "active" from `ACTIVATED` until `COMPLETED`; a service
    /// task therefore stays here while its job is pending, as does a token parked
    /// on an incident. When this becomes empty the instance has no remaining
    /// tokens and is complete.
    pub active: HashMap<Key, ElementId>,
    /// Process variables (used by exclusive-gateway conditions).
    pub variables: HashMap<String, Value>,
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
    /// A thrown business error was not caught by any boundary event.
    UnhandledError,
}

/// Lifecycle state of an incident. Incidents are retained after resolution (as
/// `Resolved`) so they remain queryable as an audit trail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
}

/// An armed timer holding a token on a timer intermediate catch event until its
/// `due_at` instant passes. A clock tick ([`crate::Command::TriggerTimers`])
/// fires every due timer, releasing its token along the event's outgoing flow.
#[derive(Clone, Debug, PartialEq, Eq)]
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
    /// All incidents ever raised, keyed by incident key, retained after
    /// resolution as an audit trail (each carries its [`IncidentState`]).
    pub incidents: HashMap<Key, Incident>,
    /// Armed and fired timers, keyed by timer key. A fired timer is retained
    /// (transitioned to [`TimerState::Triggered`]) so a clock tick never fires
    /// it twice.
    pub timers: HashMap<Key, Timer>,
}

impl State {
    /// A fresh, empty state.
    pub fn new() -> Self {
        Self::default()
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
        } => {
            state.instances.insert(
                *instance_key,
                ProcessInstance {
                    key: *instance_key,
                    process_id: process_id.clone(),
                    state: ProcessInstanceState::Active,
                    active: HashMap::new(),
                    variables: variables.clone(),
                    join_counts: HashMap::new(),
                    join_instances: HashMap::new(),
                    incidents: Vec::new(),
                },
            );
        }

        Event::VariablesUpdated {
            instance_key,
            variables,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                for (k, v) in variables {
                    instance.variables.insert(k.clone(), v.clone());
                }
            }
        }

        // ACTIVATING/COMPLETING are transient transitions with no state change.
        Event::ElementActivating { .. } | Event::ElementCompleting { .. } => {}

        Event::ElementActivated {
            instance_key,
            element_instance_key,
            element_id,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance
                    .active
                    .insert(*element_instance_key, element_id.clone());
            }
        }

        Event::ElementCompleted {
            instance_key,
            element_instance_key,
            ..
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.active.remove(element_instance_key);
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
        }

        Event::JobLockExpired { job_key, .. } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                if job.state == JobState::Activated {
                    job.state = JobState::Created;
                    job.worker = None;
                    job.deadline = None;
                }
            }
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
        }

        Event::JobErrorThrown { job_key, .. } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.state = JobState::Errored;
                job.worker = None;
                job.deadline = None;
            }
        }

        Event::JobCompleted { job_key, .. } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.state = JobState::Completed;
                job.worker = None;
                job.deadline = None;
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
            }
        }

        Event::ProcessInstanceCompleted { instance_key } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.state = ProcessInstanceState::Completed;
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
        }
    }
}
