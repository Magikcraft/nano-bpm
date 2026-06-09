//! Commands: the only way to drive the engine.
//!
//! A [`Command`] expresses *intent*. The engine decides whether and how to honour
//! it, emitting [`crate::Event`]s. Commands never mutate state directly.

use std::collections::HashMap;

use crate::model::{ProcessDefinition, Value};
use crate::state::Key;

/// An instruction submitted to [`crate::Engine::apply_command`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// Register a single process definition. Deployed under its own deployment
    /// key; equivalent to a [`Command::DeployResources`] with one process.
    DeployProcess(ProcessDefinition),
    /// Atomically register one or more process definitions as a single
    /// deployment. All processes share one deployment key; each is assigned its
    /// own process-definition key and a per-id version.
    DeployResources(Vec<ProcessDefinition>),
    /// Start a new instance of a previously deployed process, seeding it with the
    /// given variables (used by exclusive-gateway conditions).
    CreateInstance {
        process_id: String,
        variables: HashMap<String, Value>,
    },
    /// Report that the work for a job has finished, optionally merging variables
    /// into the instance before the token resumes.
    CompleteJob {
        job_key: Key,
        variables: HashMap<String, Value>,
    },
}

impl Command {
    /// Convenience constructor for a `CreateInstance` with no variables.
    pub fn create_instance(process_id: impl Into<String>) -> Self {
        Command::CreateInstance {
            process_id: process_id.into(),
            variables: HashMap::new(),
        }
    }

    /// Convenience constructor for a `CreateInstance` with variables.
    pub fn create_instance_with(
        process_id: impl Into<String>,
        variables: HashMap<String, Value>,
    ) -> Self {
        Command::CreateInstance {
            process_id: process_id.into(),
            variables,
        }
    }

    /// Convenience constructor for a `CompleteJob` with no variables.
    pub fn complete_job(job_key: Key) -> Self {
        Command::CompleteJob {
            job_key,
            variables: HashMap::new(),
        }
    }

    /// Convenience constructor for a `CompleteJob` that sets variables.
    pub fn complete_job_with(job_key: Key, variables: HashMap<String, Value>) -> Self {
        Command::CompleteJob { job_key, variables }
    }
}
