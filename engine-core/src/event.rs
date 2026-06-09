//! Events: immutable facts about what happened.
//!
//! Events are the engine's source of truth. They are produced by the processor
//! and consumed by [`crate::state::apply`] (the sole mutator). Because each event
//! carries enough data to rebuild state, the event stream is a replayable log —
//! persist it however you like and replay to recover.

use std::collections::HashMap;

use crate::model::{ElementId, ProcessDefinition, Value};
use crate::state::{IncidentKind, Key, TimerKind};

/// A fact emitted by the engine. The ordering of a command's returned events is
/// the order in which they occurred.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Event {
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

    /// A new process instance was created (carries a single token at its start
    /// event) with its initial variables.
    ProcessInstanceCreated {
        instance_key: Key,
        process_id: String,
        variables: HashMap<String, Value>,
    },

    /// Variables were merged into a process instance.
    VariablesUpdated {
        instance_key: Key,
        variables: HashMap<String, Value>,
    },

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
    /// is completed.
    JobCreated {
        job_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        job_type: String,
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
    /// A job was completed.
    JobCompleted { job_key: Key, instance_key: Key },
    /// A job's remaining retries were updated (e.g. by an operator recovering a
    /// parked job before resolving its incident). Does not change job state.
    JobRetriesUpdated {
        job_key: Key,
        instance_key: Key,
        retries: i32,
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
}

impl Event {
    /// The process-instance key this event relates to, if any.
    ///
    /// Used by the engine to decide which instances to check for completion
    /// after a command settles.
    pub fn instance_key(&self) -> Option<Key> {
        match self {
            Event::ProcessInstanceCreated { instance_key, .. }
            | Event::VariablesUpdated { instance_key, .. }
            | Event::ElementActivating { instance_key, .. }
            | Event::ElementActivated { instance_key, .. }
            | Event::ElementCompleting { instance_key, .. }
            | Event::ElementCompleted { instance_key, .. }
            | Event::SequenceFlowTaken { instance_key, .. }
            | Event::ParallelJoinOpened { instance_key, .. }
            | Event::ParallelJoinTokenArrived { instance_key, .. }
            | Event::ParallelJoinReset { instance_key, .. }
            | Event::JobCreated { instance_key, .. }
            | Event::JobActivated { instance_key, .. }
            | Event::JobLockExpired { instance_key, .. }
            | Event::JobFailed { instance_key, .. }
            | Event::JobErrorThrown { instance_key, .. }
            | Event::JobCompleted { instance_key, .. }
            | Event::JobRetriesUpdated { instance_key, .. }
            | Event::IncidentRaised { instance_key, .. }
            | Event::IncidentResolved { instance_key, .. }
            | Event::TimerCreated { instance_key, .. }
            | Event::TimerTriggered { instance_key, .. }
            | Event::TimerCanceled { instance_key, .. }
            | Event::JobCanceled { instance_key, .. }
            | Event::ProcessInstanceCompleted { instance_key } => Some(*instance_key),
            Event::ProcessDeployed { .. } => None,
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
            Event::JobActivated { job_key, .. }
            | Event::JobLockExpired { job_key, .. }
            | Event::JobFailed { job_key, .. }
            | Event::JobErrorThrown { job_key, .. }
            | Event::JobCompleted { job_key, .. }
            | Event::JobCanceled { job_key, .. }
            | Event::JobRetriesUpdated { job_key, .. } => m = m.max(*job_key),
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
            _ => {}
        }
        m
    }
}
