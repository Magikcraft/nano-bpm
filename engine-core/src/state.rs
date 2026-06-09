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
    /// It is neither activatable nor completable until the incident is resolved
    /// (incident resolution is not yet modelled).
    Failed,
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
    /// Reasons of incidents raised on this instance (parked tokens).
    pub incidents: Vec<String>,
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

        Event::JobCompleted { job_key, .. } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.state = JobState::Completed;
                job.worker = None;
                job.deadline = None;
            }
        }

        Event::IncidentRaised {
            instance_key,
            reason,
            ..
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.incidents.push(reason.clone());
            }
        }

        Event::ProcessInstanceCompleted { instance_key } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.state = ProcessInstanceState::Completed;
            }
        }
    }
}
