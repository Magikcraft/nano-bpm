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
    /// Fire every armed timer whose `due_at` is at or before `now`, releasing the
    /// token parked on its timer intermediate catch event along the event's
    /// outgoing flow. A periodic "tick" the host drives; keeps timer firing
    /// deterministic and out of the engine's clock.
    TriggerTimers { now: u64 },
    /// Report that a job failed, setting its remaining `retries`. With retries
    /// left the job becomes activatable again; with none an incident is raised
    /// and the job parks. `error_message` is recorded as the incident reason.
    FailJob {
        job_key: Key,
        retries: i32,
        error_message: String,
    },
    /// Throw a business error from a job. If the job's activity has a matching
    /// error boundary event it is interrupted and the error-handling path runs;
    /// otherwise an incident is raised. The job is consumed either way.
    ThrowJobError {
        job_key: Key,
        error_code: String,
        error_message: String,
    },
    /// Update a job's remaining retries. Used to recover a job parked on a
    /// no-retries incident before resolving that incident. Does not by itself
    /// unblock the job — the incident must still be resolved.
    UpdateJobRetries { job_key: Key, retries: i32 },
    /// Resolve an open incident by retrying the work that failed. A job-incident
    /// returns the parked job (which must have retries left) to the activatable
    /// pool; an exclusive-gateway incident re-evaluates the gateway against the
    /// current variables; an uncaught-error incident re-creates the service-task
    /// job. If the retry fails again a fresh incident is raised. The resolved
    /// record is retained for audit, tagged with `operation_reference` if given.
    ResolveIncident {
        incident_key: Key,
        operation_reference: Option<i64>,
    },
    /// Merge variables into a scope before, typically, resolving an incident so
    /// the retried work sees the corrected data. `scope_key` may be a process
    /// instance key or an element instance key; both resolve to the owning
    /// instance, since nano keeps a single instance-level variable scope.
    SetVariables {
        scope_key: Key,
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

    /// Convenience constructor for a `FailJob`.
    pub fn fail_job(job_key: Key, retries: i32, error_message: impl Into<String>) -> Self {
        Command::FailJob {
            job_key,
            retries,
            error_message: error_message.into(),
        }
    }

    /// Convenience constructor for a `ThrowJobError`.
    pub fn throw_job_error(
        job_key: Key,
        error_code: impl Into<String>,
        error_message: impl Into<String>,
    ) -> Self {
        Command::ThrowJobError {
            job_key,
            error_code: error_code.into(),
            error_message: error_message.into(),
        }
    }

    /// Convenience constructor for an `UpdateJobRetries`.
    pub fn update_job_retries(job_key: Key, retries: i32) -> Self {
        Command::UpdateJobRetries { job_key, retries }
    }

    /// Convenience constructor for a `ResolveIncident` with no operation
    /// reference.
    pub fn resolve_incident(incident_key: Key) -> Self {
        Command::ResolveIncident {
            incident_key,
            operation_reference: None,
        }
    }

    /// Convenience constructor for a `ResolveIncident` tagged with an operation
    /// reference for audit.
    pub fn resolve_incident_with(incident_key: Key, operation_reference: i64) -> Self {
        Command::ResolveIncident {
            incident_key,
            operation_reference: Some(operation_reference),
        }
    }

    /// Convenience constructor for a `SetVariables`.
    pub fn set_variables(scope_key: Key, variables: HashMap<String, Value>) -> Self {
        Command::SetVariables {
            scope_key,
            variables,
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
