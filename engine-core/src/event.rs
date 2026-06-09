//! Events: immutable facts about what happened.
//!
//! Events are the engine's source of truth. They are produced by the processor
//! and consumed by [`crate::state::apply`] (the sole mutator). Because each event
//! carries enough data to rebuild state, the event stream is a replayable log —
//! persist it however you like and replay to recover.

use crate::model::{ElementId, ProcessDefinition};
use crate::state::Key;

/// A fact emitted by the engine. The ordering of a command's returned events is
/// the order in which they occurred.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// A process definition was registered.
    ProcessDeployed { process: ProcessDefinition },

    /// A new process instance was created (carries a single token at its start
    /// event).
    ProcessInstanceCreated {
        instance_key: Key,
        process_id: String,
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
    JobCompleted { job_key: Key },

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
            | Event::ElementActivating { instance_key, .. }
            | Event::ElementActivated { instance_key, .. }
            | Event::ElementCompleting { instance_key, .. }
            | Event::ElementCompleted { instance_key, .. }
            | Event::SequenceFlowTaken { instance_key, .. }
            | Event::JobCreated { instance_key, .. }
            | Event::ProcessInstanceCompleted { instance_key } => Some(*instance_key),
            Event::ProcessDeployed { .. } | Event::JobCompleted { .. } => None,
        }
    }
}
