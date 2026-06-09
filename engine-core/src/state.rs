//! State and the applier.
//!
//! [`State`] is the engine's working memory. [`apply`] is the **only** function
//! that mutates it, and it does so purely as a function of an [`Event`]. Keeping
//! every mutation here is what makes the engine deterministic and replayable:
//! replaying the same events over a fresh [`State`] reconstructs it exactly.

use std::collections::HashMap;

use crate::event::Event;
use crate::model::{ElementId, ProcessDefinition};

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
    /// Created and waiting to be completed.
    Created,
    /// Completed by a worker.
    Completed,
}

/// A job created for a service task, awaiting completion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Job {
    pub key: Key,
    pub instance_key: Key,
    /// The element instance that is parked waiting on this job.
    pub element_instance_key: Key,
    pub element_id: ElementId,
    pub job_type: String,
    pub state: JobState,
}

/// A running (or completed) process instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessInstance {
    pub key: Key,
    pub process_id: String,
    pub state: ProcessInstanceState,
    /// Currently-active element instances, keyed by element-instance key. An
    /// element instance is "active" from `ACTIVATED` until `COMPLETED`; a service
    /// task therefore stays here while its job is pending. When this becomes
    /// empty the instance has no remaining tokens and is complete.
    pub active: HashMap<Key, ElementId>,
}

/// The complete working state of the engine.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct State {
    pub processes: HashMap<String, ProcessDefinition>,
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
        Event::ProcessDeployed { process } => {
            state.processes.insert(process.id.clone(), process.clone());
        }

        Event::ProcessInstanceCreated {
            instance_key,
            process_id,
        } => {
            state.instances.insert(
                *instance_key,
                ProcessInstance {
                    key: *instance_key,
                    process_id: process_id.clone(),
                    state: ProcessInstanceState::Active,
                    active: HashMap::new(),
                },
            );
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
                },
            );
        }

        Event::JobCompleted { job_key } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.state = JobState::Completed;
            }
        }

        Event::ProcessInstanceCompleted { instance_key } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.state = ProcessInstanceState::Completed;
            }
        }
    }
}
