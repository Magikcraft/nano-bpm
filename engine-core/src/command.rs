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
    /// into the instance before the token resumes. Completion is by key alone:
    /// any holder of the key may complete a job that has been activated.
    CompleteJob {
        job_key: Key,
        variables: HashMap<String, Value>,
    },
    /// Activate up to `max_jobs` activatable jobs of `job_type` for `worker`,
    /// locking each until `now + timeout`. `now` is a caller-supplied logical
    /// instant — the engine never reads a wall clock.
    ActivateJobs {
        job_type: String,
        worker: String,
        max_jobs: usize,
        timeout: u64,
        now: u64,
    },
    /// Release the activation lock of every job whose `deadline` is at or before
    /// `now`, making it activatable again. A periodic "tick" the host drives;
    /// keeps lock expiry deterministic and out of the engine's clock.
    ExpireJobs { now: u64 },
    /// Report that a job failed, setting its remaining `retries`. With retries
    /// left the job becomes activatable again; with none an incident is raised
    /// and the job parks. `error_message` is recorded as the incident reason.
    FailJob {
        job_key: Key,
        retries: i32,
        error_message: String,
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

    /// Convenience constructor for a `FailJob`.
    pub fn fail_job(job_key: Key, retries: i32, error_message: impl Into<String>) -> Self {
        Command::FailJob {
            job_key,
            retries,
            error_message: error_message.into(),
        }
    }

    /// Convenience constructor for an `ActivateJobs` request.
    pub fn activate_jobs(
        job_type: impl Into<String>,
        worker: impl Into<String>,
        max_jobs: usize,
        timeout: u64,
        now: u64,
    ) -> Self {
        Command::ActivateJobs {
            job_type: job_type.into(),
            worker: worker.into(),
            max_jobs,
            timeout,
            now,
        }
    }
}
