//! Commands: the only way to drive the engine.
//!
//! A [`Command`] expresses *intent*. The engine decides whether and how to honour
//! it, emitting [`crate::Event`]s. Commands never mutate state directly.

use crate::model::ProcessDefinition;
use crate::state::Key;

/// An instruction submitted to [`crate::Engine::apply_command`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// Register a process definition so instances of it can be created.
    DeployProcess(ProcessDefinition),
    /// Start a new instance of a previously deployed process.
    CreateInstance { process_id: String },
    /// Report that the work for a job has finished, resuming the parked token.
    CompleteJob { job_key: Key },
}
