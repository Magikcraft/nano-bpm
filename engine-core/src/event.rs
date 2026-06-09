//! Events: immutable facts about what happened.
//!
//! Events are the engine's source of truth. They are produced by the processor
//! and consumed by [`crate::state::apply`] (the sole mutator). Because each event
//! carries enough data to rebuild state, the event stream is a replayable log —
//! persist it however you like and replay to recover.

use std::collections::HashMap;

use crate::model::{ElementId, ProcessDefinition, Value};
use crate::state::Key;

/// A fact emitted by the engine. The ordering of a command's returned events is
/// the order in which they occurred.
#[derive(Clone, Debug, PartialEq, Eq)]
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
    /// A job was completed.
    JobCompleted { job_key: Key, instance_key: Key },

    /// An incident was raised (e.g. an exclusive gateway found no matching flow);
    /// the token is parked until the incident is resolved.
    IncidentRaised {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        reason: String,
    },

    /// The last token of a process instance was consumed; the instance is done.
    ProcessInstanceCompleted { instance_key: Key },
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
            | Event::JobCompleted { instance_key, .. }
            | Event::IncidentRaised { instance_key, .. }
            | Event::ProcessInstanceCompleted { instance_key } => Some(*instance_key),
            Event::ProcessDeployed { .. } => None,
        }
    }
}
