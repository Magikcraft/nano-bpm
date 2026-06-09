//! The engine: a single-writer command/event/applier loop.
//!
//! [`Engine::apply_command`] is the one entry point that changes anything. It:
//!
//! 1. validates the [`Command`] and emits the top-level event(s),
//! 2. drives an internal work queue of [`Step`]s — the BPMN element lifecycle —
//!    until the instance is quiescent (finished, or resting on a job/incident),
//! 3. detects process-instance completion,
//!
//! applying every event through [`state::apply`] as it goes. The processor
//! ([`Engine::process_step`]) only *reads* state and *decides*; it never mutates.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::command::Command;
use crate::event::Event;
use crate::model::{ElementId, ElementKind, ProcessDefinition, SequenceFlow, Value};
use crate::state::{self, Key, ProcessInstanceState, State};

/// An embeddable BPMN engine instance.
///
/// Holds all state in memory. It is `Send` and contains no threads, locks or I/O,
/// so it can be owned by a single actor/task on a server, wrapped behind an FFI
/// boundary on mobile, or compiled to wasm.
#[derive(Debug, Default)]
pub struct Engine {
    state: State,
    /// Source of monotonic keys. The single-writer loop makes plain increments
    /// deterministic.
    next_key: Key,
    /// The clock reading for the command currently being processed, in the units
    /// the host supplies (Unix epoch milliseconds on the server). Set at the top
    /// of [`Engine::apply_command_at`] and read where the engine stamps a
    /// timestamp onto an event (e.g. when an incident is raised). The engine
    /// never reads a wall clock itself; replay is unaffected because the
    /// timestamp is carried on the event.
    now: u64,
}

/// A unit of internal work in the processing loop — one transition of the BPMN
/// element lifecycle.
enum Step {
    /// Run an element through `ACTIVATING -> ACTIVATED` (and decide what comes
    /// next based on its kind).
    Activate {
        instance_key: Key,
        element_id: String,
    },
    /// Run an already-activated element through `COMPLETING -> COMPLETED` and take
    /// its outgoing sequence flows.
    Complete {
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    },
    /// Create a fresh job for an already-active service-task element instance.
    /// Used to retry a parked service task when its incident is resolved (the
    /// element instance stays active throughout; only a new job is minted).
    CreateJob {
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    },
}

impl Engine {
    /// Creates an empty engine.
    pub fn new() -> Self {
        Self {
            state: State::new(),
            next_key: 0,
            now: 0,
        }
    }

    /// Read-only access to the full engine state (useful for queries and tests).
    pub fn state(&self) -> &State {
        &self.state
    }

    /// Rebuilds an engine by replaying a recorded event log.
    ///
    /// State is reconstructed through the applier ([`state::apply`]) — the same
    /// path used at runtime — and the key generator is advanced past every key
    /// the log assigned, so commands applied after recovery never mint a key
    /// that collides with a replayed one. The clock resets to `0`; timestamps
    /// are carried on the events themselves, so history (e.g. incident
    /// `created_at`) is preserved, and the next command sets the clock again.
    ///
    /// This is the durability primitive: a host persists the events returned by
    /// [`Engine::apply_command_at`] and, on restart, feeds them back here.
    pub fn replay<I>(events: I) -> Self
    where
        I: IntoIterator<Item = Event>,
    {
        let mut state = State::new();
        let mut max_key: Key = 0;
        for event in events {
            max_key = max_key.max(event.max_key());
            state::apply(&mut state, &event);
        }
        Self {
            state,
            next_key: max_key,
            now: 0,
        }
    }

    /// Looks up a process instance.
    pub fn instance(&self, key: Key) -> Option<&state::ProcessInstance> {
        self.state.instances.get(&key)
    }

    /// Returns `true` if the instance exists and has completed.
    pub fn is_completed(&self, key: Key) -> bool {
        matches!(
            self.state.instances.get(&key).map(|i| i.state),
            Some(ProcessInstanceState::Completed)
        )
    }

    /// Looks up a job.
    pub fn job(&self, key: Key) -> Option<&state::Job> {
        self.state.jobs.get(&key)
    }

    /// Looks up an incident by key, whether active or resolved (resolved records
    /// are retained for audit).
    pub fn incident(&self, key: Key) -> Option<&state::Incident> {
        self.state.incidents.get(&key)
    }

    /// All incidents ever raised, active and resolved (resolved records are
    /// retained as an audit trail). Filter by [`state::Incident::state`] for a
    /// specific lifecycle state.
    pub fn incidents(&self) -> Vec<&state::Incident> {
        self.state.incidents.values().collect()
    }

    /// Only the currently-active (open) incidents.
    pub fn active_incidents(&self) -> Vec<&state::Incident> {
        self.state
            .incidents
            .values()
            .filter(|i| i.state == state::IncidentState::Active)
            .collect()
    }

    /// All jobs currently awaiting activation (created, or with an expired lock).
    pub fn pending_jobs(&self) -> Vec<&state::Job> {
        self.state
            .jobs
            .values()
            .filter(|j| j.state == state::JobState::Created)
            .collect()
    }

    /// Activates up to `max_jobs` activatable jobs of `job_type` for `worker`,
    /// locking each until `now + timeout`, and returns them — the **pull** worker
    /// API for embedded use (no polling, no network). `now` is a caller-supplied
    /// logical instant. Equivalent to applying a [`Command::ActivateJobs`] and
    /// reading back the activated jobs.
    pub fn activate_jobs(
        &mut self,
        job_type: impl Into<String>,
        worker: impl Into<String>,
        max_jobs: usize,
        timeout: u64,
        now: u64,
    ) -> Vec<ActivatedJob> {
        let events = self
            .apply_command_at(
                Command::activate_jobs(job_type, worker, max_jobs, timeout, now),
                now,
            )
            .expect("ActivateJobs never fails");
        events
            .iter()
            .filter_map(|e| match e {
                Event::JobActivated {
                    job_key,
                    worker,
                    deadline,
                    ..
                } => self.job(*job_key).map(|job| ActivatedJob {
                    key: job.key,
                    job_type: job.job_type.clone(),
                    instance_key: job.instance_key,
                    element_instance_key: job.element_instance_key,
                    element_id: job.element_id.clone(),
                    worker: worker.clone(),
                    deadline: *deadline,
                    retries: job.retries,
                    variables: self.variables(job.instance_key),
                }),
                _ => None,
            })
            .collect()
    }

    /// Releases the activation lock of every job whose deadline is at or before
    /// `now`. The host drives this periodically (a "tick"); the engine itself
    /// never reads a clock.
    pub fn expire_jobs(&mut self, now: u64) {
        self.apply_command_at(Command::ExpireJobs { now }, now)
            .expect("ExpireJobs never fails");
    }

    /// Activates jobs of `job_type` and dispatches each to `handler` — the
    /// **callback** worker API for embedded use. Whatever variables the handler
    /// returns complete the job (by key); returning `None` leaves the job locked.
    /// Returns the number of jobs the handler completed. Runs entirely on the
    /// single-writer thread, so there is no concurrency to reason about.
    pub fn poll_jobs<F>(
        &mut self,
        job_type: impl Into<String>,
        worker: impl Into<String>,
        max_jobs: usize,
        timeout: u64,
        now: u64,
        mut handler: F,
    ) -> usize
    where
        F: FnMut(&ActivatedJob) -> Option<HashMap<String, Value>>,
    {
        let jobs = self.activate_jobs(job_type, worker, max_jobs, timeout, now);
        let mut completed = 0;
        for job in jobs {
            if let Some(variables) = handler(&job) {
                self.apply_command(Command::complete_job_with(job.key, variables))
                    .expect("activated job can be completed");
                completed += 1;
            }
        }
        completed
    }

    fn mint_key(&mut self) -> Key {
        self.next_key += 1;
        self.next_key
    }

    /// Validates and registers a batch of process definitions as one deployment.
    ///
    /// All processes are validated first, so the deployment is atomic: if any is
    /// invalid, none are emitted. Each process is assigned a unique
    /// process-definition key and a per-id version (latest known + 1).
    fn deploy(
        &mut self,
        log: &mut Vec<Event>,
        processes: Vec<ProcessDefinition>,
    ) -> Result<(), EngineError> {
        for process in &processes {
            if !process.elements.contains_key(&process.start_event) {
                return Err(EngineError::NoStartEvent {
                    process_id: process.id.clone(),
                });
            }
        }

        let deployment_key = self.mint_key();
        for process in processes {
            let version = self.next_version(&process.id);
            let process_definition_key = self.mint_key();
            self.emit(
                log,
                Event::ProcessDeployed {
                    deployment_key,
                    process_definition_key,
                    version,
                    process,
                },
            );
        }
        Ok(())
    }

    /// The version that the next deployment of `process_id` will receive.
    fn next_version(&self, process_id: &str) -> i32 {
        self.state
            .processes
            .get(process_id)
            .map(|p| p.version + 1)
            .unwrap_or(1)
    }

    /// Applies a command using the engine's current clock reading (see
    /// [`Engine::apply_command_at`]). Tests and hosts that do not need accurate
    /// timestamps can use this; the default clock is `0`.
    pub fn apply_command(&mut self, command: Command) -> Result<Vec<Event>, EngineError> {
        self.apply_command_at(command, self.now)
    }

    /// Applies a command, returning the ordered list of events it produced,
    /// stamping any timestamped events (e.g. raised incidents) with `now`.
    ///
    /// This is the engine's single writer: it runs to quiescence before
    /// returning, so on success the returned events are the complete record of
    /// everything that happened. `now` is the host's clock reading for this
    /// command; the engine never reads a wall clock itself.
    pub fn apply_command_at(
        &mut self,
        command: Command,
        now: u64,
    ) -> Result<Vec<Event>, EngineError> {
        self.now = now;
        let mut log: Vec<Event> = Vec::new();
        let mut queue: VecDeque<Step> = VecDeque::new();

        match command {
            Command::DeployProcess(process) => {
                self.deploy(&mut log, vec![process])?;
            }

            Command::DeployResources(processes) => {
                self.deploy(&mut log, processes)?;
            }

            Command::CreateInstance {
                process_id,
                variables,
            } => {
                let process = self.state.processes.get(&process_id).ok_or_else(|| {
                    EngineError::ProcessNotFound {
                        process_id: process_id.clone(),
                    }
                })?;
                let start_event = process.definition.start_event.clone();

                let instance_key = self.mint_key();
                self.emit(
                    &mut log,
                    Event::ProcessInstanceCreated {
                        instance_key,
                        process_id,
                        variables,
                    },
                );
                queue.push_back(Step::Activate {
                    instance_key,
                    element_id: start_event,
                });
            }

            Command::CompleteJob { job_key, variables } => {
                let job = self
                    .state
                    .jobs
                    .get(&job_key)
                    .ok_or(EngineError::JobNotFound { job_key })?;
                if matches!(
                    job.state,
                    state::JobState::Completed | state::JobState::Failed | state::JobState::Errored
                ) {
                    return Err(EngineError::JobNotActive { job_key });
                }
                // Completion is by key alone, but a job must have been activated
                // at least once first. The current lock holder is irrelevant:
                // any worker that holds the key may complete it, even after the
                // lock expired and another worker re-activated it.
                if !job.activated {
                    return Err(EngineError::JobNotActivated { job_key });
                }
                let instance_key = job.instance_key;
                let element_instance_key = job.element_instance_key;
                let element_id = job.element_id.clone();

                self.emit(
                    &mut log,
                    Event::JobCompleted {
                        job_key,
                        instance_key,
                    },
                );
                if !variables.is_empty() {
                    self.emit(
                        &mut log,
                        Event::VariablesUpdated {
                            instance_key,
                            variables,
                        },
                    );
                }
                // The parked service-task token resumes from ACTIVATED.
                queue.push_back(Step::Complete {
                    instance_key,
                    element_instance_key,
                    element_id,
                });
            }

            Command::ActivateJobs {
                job_type,
                worker,
                max_jobs,
                timeout,
                now,
            } => {
                let deadline = now.saturating_add(timeout);
                // Deterministic selection: activatable jobs of this type, by key
                // ascending, capped at max_jobs.
                let mut keys: Vec<Key> = self
                    .state
                    .jobs
                    .values()
                    .filter(|j| j.job_type == job_type && job_activatable(j, now))
                    .map(|j| j.key)
                    .collect();
                keys.sort_unstable();
                keys.truncate(max_jobs);
                for job_key in keys {
                    let instance_key = self.state.jobs[&job_key].instance_key;
                    self.emit(
                        &mut log,
                        Event::JobActivated {
                            job_key,
                            instance_key,
                            worker: worker.clone(),
                            deadline,
                        },
                    );
                }
            }

            Command::ExpireJobs { now } => {
                let mut expired: Vec<(Key, Key)> = self
                    .state
                    .jobs
                    .values()
                    .filter(|j| {
                        j.state == state::JobState::Activated
                            && j.deadline.is_some_and(|d| d <= now)
                    })
                    .map(|j| (j.key, j.instance_key))
                    .collect();
                expired.sort_unstable();
                for (job_key, instance_key) in expired {
                    self.emit(
                        &mut log,
                        Event::JobLockExpired {
                            job_key,
                            instance_key,
                        },
                    );
                }
            }

            Command::FailJob {
                job_key,
                retries,
                error_message,
            } => {
                let job = self
                    .state
                    .jobs
                    .get(&job_key)
                    .ok_or(EngineError::JobNotFound { job_key })?;
                if matches!(
                    job.state,
                    state::JobState::Completed | state::JobState::Failed | state::JobState::Errored
                ) {
                    return Err(EngineError::JobNotActive { job_key });
                }
                // Like completion, failing a job requires that it was activated.
                if !job.activated {
                    return Err(EngineError::JobNotActivated { job_key });
                }
                let instance_key = job.instance_key;
                let element_instance_key = job.element_instance_key;
                let element_id = job.element_id.clone();
                let retries = retries.max(0);

                self.emit(
                    &mut log,
                    Event::JobFailed {
                        job_key,
                        instance_key,
                        retries,
                    },
                );
                // No retries left: park the job and raise an incident so the
                // instance stops making progress on this token.
                if retries == 0 {
                    let incident_key = self.mint_key();
                    self.emit(
                        &mut log,
                        Event::IncidentRaised {
                            incident_key,
                            instance_key,
                            element_instance_key,
                            element_id,
                            kind: state::IncidentKind::JobNoRetries,
                            reason: error_message,
                            job_key: Some(job_key),
                            created_at: self.now,
                        },
                    );
                }
            }

            Command::ThrowJobError {
                job_key,
                error_code,
                error_message,
            } => {
                let job = self
                    .state
                    .jobs
                    .get(&job_key)
                    .ok_or(EngineError::JobNotFound { job_key })?;
                if matches!(
                    job.state,
                    state::JobState::Completed | state::JobState::Failed | state::JobState::Errored
                ) {
                    return Err(EngineError::JobNotActive { job_key });
                }
                if !job.activated {
                    return Err(EngineError::JobNotActivated { job_key });
                }
                let instance_key = job.instance_key;
                let element_instance_key = job.element_instance_key;
                let task_element_id = job.element_id.clone();

                // The job is consumed by the thrown error either way.
                self.emit(
                    &mut log,
                    Event::JobErrorThrown {
                        job_key,
                        instance_key,
                        error_code: error_code.clone(),
                    },
                );

                match self.find_error_boundary(instance_key, &task_element_id, &error_code) {
                    // Caught: interrupt the service task (complete its element
                    // instance without taking its normal outgoing flow) and run
                    // the boundary event's error-handling path.
                    Some(boundary_id) => {
                        self.emit(
                            &mut log,
                            Event::ElementCompleting {
                                instance_key,
                                element_instance_key,
                                element_id: task_element_id.clone(),
                            },
                        );
                        self.emit(
                            &mut log,
                            Event::ElementCompleted {
                                instance_key,
                                element_instance_key,
                                element_id: task_element_id,
                            },
                        );
                        queue.push_back(Step::Activate {
                            instance_key,
                            element_id: boundary_id,
                        });
                    }
                    // Unhandled: the token parks on an incident.
                    None => {
                        let reason = if error_message.is_empty() {
                            format!("unhandled BPMN error '{error_code}'")
                        } else {
                            format!("unhandled BPMN error '{error_code}': {error_message}")
                        };
                        let incident_key = self.mint_key();
                        self.emit(
                            &mut log,
                            Event::IncidentRaised {
                                incident_key,
                                instance_key,
                                element_instance_key,
                                element_id: task_element_id,
                                kind: state::IncidentKind::UnhandledError,
                                reason,
                                job_key: None,
                                created_at: self.now,
                            },
                        );
                    }
                }
            }

            Command::UpdateJobRetries { job_key, retries } => {
                let job = self
                    .state
                    .jobs
                    .get(&job_key)
                    .ok_or(EngineError::JobNotFound { job_key })?;
                // Retries are only meaningful while a job is still in play;
                // completed/errored jobs are terminal.
                if matches!(
                    job.state,
                    state::JobState::Completed | state::JobState::Errored
                ) {
                    return Err(EngineError::JobNotActive { job_key });
                }
                let instance_key = job.instance_key;
                let retries = retries.max(0);
                self.emit(
                    &mut log,
                    Event::JobRetriesUpdated {
                        job_key,
                        instance_key,
                        retries,
                    },
                );
            }

            Command::ResolveIncident {
                incident_key,
                operation_reference,
            } => {
                let incident = self
                    .state
                    .incidents
                    .get(&incident_key)
                    .ok_or(EngineError::IncidentNotFound { incident_key })?;
                // Only an active incident can be resolved; a retained (resolved)
                // record is history, not a live parked token.
                if incident.state != state::IncidentState::Active {
                    return Err(EngineError::IncidentNotResolvable {
                        incident_key,
                        reason: "incident is already resolved".to_string(),
                    });
                }
                let instance_key = incident.instance_key;
                let element_instance_key = incident.element_instance_key;
                let element_id = incident.element_id.clone();
                let kind = incident.kind;
                let job_key = incident.job_key;
                // A job-incident can only be resolved once the parked job has
                // retries again; otherwise it would immediately re-fail.
                if let Some(job_key) = job_key {
                    let retries = self.state.jobs.get(&job_key).map_or(0, |j| j.retries);
                    if retries <= 0 {
                        return Err(EngineError::IncidentNotResolvable {
                            incident_key,
                            reason: format!(
                                "job {job_key} still has no retries; update its retries first"
                            ),
                        });
                    }
                }
                self.emit(
                    &mut log,
                    Event::IncidentResolved {
                        incident_key,
                        instance_key,
                        job_key,
                        resolved_at: self.now,
                        operation_reference,
                    },
                );
                // Resolution retries the failed work rather than merely clearing
                // the record. If the retry fails again a fresh incident is raised
                // by the same code paths that raised the original.
                match kind {
                    // Job exhausted its retries: the applier already returned the
                    // job to the activatable pool, so a worker retries it via the
                    // normal activate/complete path. Nothing more to enqueue.
                    state::IncidentKind::JobNoRetries => {}
                    // Exclusive gateway matched no flow: re-evaluate the gateway
                    // against the (possibly updated) variables.
                    state::IncidentKind::NoMatchingSequenceFlow => {
                        queue.push_back(Step::Complete {
                            instance_key,
                            element_instance_key,
                            element_id,
                        });
                    }
                    // Uncaught business error: re-create a job for the still-active
                    // service task so a worker can attempt it again.
                    state::IncidentKind::UnhandledError => {
                        queue.push_back(Step::CreateJob {
                            instance_key,
                            element_instance_key,
                            element_id,
                        });
                    }
                }
            }

            Command::SetVariables {
                scope_key,
                variables,
            } => {
                let instance_key = self
                    .resolve_scope(scope_key)
                    .ok_or(EngineError::ScopeNotFound { scope_key })?;
                if !variables.is_empty() {
                    self.emit(
                        &mut log,
                        Event::VariablesUpdated {
                            instance_key,
                            variables,
                        },
                    );
                }
            }
        }

        self.run(&mut log, queue);
        self.complete_finished_instances(&mut log);
        Ok(log)
    }

    /// Drains the work queue, applying events and enqueuing follow-up steps until
    /// the instance is quiescent.
    fn run(&mut self, log: &mut Vec<Event>, mut queue: VecDeque<Step>) {
        while let Some(step) = queue.pop_front() {
            let (events, followups) = self.process_step(step);
            for event in events {
                self.emit(log, event);
            }
            for f in followups {
                queue.push_back(f);
            }
        }
    }

    /// The processor: decides the events and follow-up work for one lifecycle
    /// step. Reads state, mints keys, but never mutates [`State`].
    fn process_step(&mut self, step: Step) -> (Vec<Event>, Vec<Step>) {
        match step {
            Step::Activate {
                instance_key,
                element_id,
            } => self.activate(instance_key, element_id),
            Step::Complete {
                instance_key,
                element_instance_key,
                element_id,
            } => self.complete(instance_key, element_instance_key, element_id),
            Step::CreateJob {
                instance_key,
                element_instance_key,
                element_id,
            } => self.create_job_for(instance_key, element_instance_key, element_id),
        }
    }

    fn activate(&mut self, instance_key: Key, element_id: String) -> (Vec<Event>, Vec<Step>) {
        let kind = self.element_kind(instance_key, &element_id);

        // A parallel gateway with more than one incoming flow is a join: it
        // synchronises tokens instead of activating per arrival.
        if matches!(kind, Some(ElementKind::ParallelGateway))
            && self.incoming_count(instance_key, &element_id) > 1
        {
            return self.arrive_at_parallel_join(instance_key, element_id);
        }

        let element_instance_key = self.mint_key();
        let mut events = vec![
            Event::ElementActivating {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
            },
            Event::ElementActivated {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
            },
        ];
        let mut followups = Vec::new();

        match kind {
            // A service task creates a job and parks the token.
            Some(ElementKind::ServiceTask { job_type }) => {
                let job_key = self.mint_key();
                events.push(Event::JobCreated {
                    job_key,
                    instance_key,
                    element_instance_key,
                    element_id,
                    job_type,
                });
            }
            // Pass-through elements (events, exclusive gateway, parallel split)
            // complete immediately; routing happens at completion.
            Some(_) => {
                followups.push(Step::Complete {
                    instance_key,
                    element_instance_key,
                    element_id,
                });
            }
            // Unknown element / instance: nothing to do.
            None => {}
        }

        (events, followups)
    }

    fn complete(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    ) -> (Vec<Event>, Vec<Step>) {
        if matches!(
            self.element_kind(instance_key, &element_id),
            Some(ElementKind::ExclusiveGateway)
        ) {
            return self.complete_exclusive_gateway(instance_key, element_instance_key, element_id);
        }

        // Default behaviour: complete and take every outgoing flow (a single flow
        // for ordinary elements; all flows for a parallel split).
        let mut events = vec![
            Event::ElementCompleting {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
            },
            Event::ElementCompleted {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
            },
        ];
        let mut followups = Vec::new();
        for flow in self.outgoing(instance_key, &element_id) {
            events.push(Event::SequenceFlowTaken {
                instance_key,
                from: element_id.clone(),
                to: flow.to.clone(),
            });
            followups.push(Step::Activate {
                instance_key,
                element_id: flow.to,
            });
        }
        (events, followups)
    }

    /// Mints a fresh job for an already-active service-task element instance.
    /// Used by incident resolution to retry a parked service task: the element
    /// instance is left untouched (it stays active) and a new job is created in
    /// the `Created` (activatable) state so a worker can attempt it again. A
    /// no-op if the element is not (or is no longer) a service task.
    fn create_job_for(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    ) -> (Vec<Event>, Vec<Step>) {
        match self.element_kind(instance_key, &element_id) {
            Some(ElementKind::ServiceTask { job_type }) => {
                let job_key = self.mint_key();
                (
                    vec![Event::JobCreated {
                        job_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        job_type,
                    }],
                    Vec::new(),
                )
            }
            _ => (Vec::new(), Vec::new()),
        }
    }

    /// Exclusive gateway: take exactly one outgoing flow — the first whose
    /// condition holds (an unconditional flow is the default). If none qualifies,
    /// raise an incident and park the token.
    fn complete_exclusive_gateway(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    ) -> (Vec<Event>, Vec<Step>) {
        let variables = self.variables(instance_key);
        let selected =
            self.outgoing(instance_key, &element_id)
                .into_iter()
                .find(|flow| match &flow.condition {
                    None => true,
                    Some(c) => c.eval(&variables),
                });

        match selected {
            Some(flow) => {
                let events = vec![
                    Event::ElementCompleting {
                        instance_key,
                        element_instance_key,
                        element_id: element_id.clone(),
                    },
                    Event::ElementCompleted {
                        instance_key,
                        element_instance_key,
                        element_id: element_id.clone(),
                    },
                    Event::SequenceFlowTaken {
                        instance_key,
                        from: element_id,
                        to: flow.to.clone(),
                    },
                ];
                let followups = vec![Step::Activate {
                    instance_key,
                    element_id: flow.to,
                }];
                (events, followups)
            }
            None => {
                // The token stays active (parked on the incident) so the instance
                // does not falsely complete.
                let incident_key = self.mint_key();
                let events = vec![Event::IncidentRaised {
                    incident_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                    kind: state::IncidentKind::NoMatchingSequenceFlow,
                    reason: format!(
                        "no matching outgoing sequence flow at exclusive gateway '{element_id}'"
                    ),
                    job_key: None,
                    created_at: self.now,
                }];
                (events, Vec::new())
            }
        }
    }

    /// A token reached a parallel-gateway join. Open the join on the first
    /// arrival, count every arrival, and fire once a token has arrived on every
    /// incoming flow.
    fn arrive_at_parallel_join(
        &mut self,
        instance_key: Key,
        element_id: String,
    ) -> (Vec<Event>, Vec<Step>) {
        let threshold = self.incoming_count(instance_key, &element_id);
        let already_open = self.join_eik(instance_key, &element_id).is_some();
        let count_before = self.join_count(instance_key, &element_id);

        let mut events = Vec::new();

        let open_eik = if already_open {
            self.join_eik(instance_key, &element_id).unwrap()
        } else {
            let eik = self.mint_key();
            events.push(Event::ElementActivating {
                instance_key,
                element_instance_key: eik,
                element_id: element_id.clone(),
            });
            events.push(Event::ElementActivated {
                instance_key,
                element_instance_key: eik,
                element_id: element_id.clone(),
            });
            events.push(Event::ParallelJoinOpened {
                instance_key,
                element_instance_key: eik,
                element_id: element_id.clone(),
            });
            eik
        };

        events.push(Event::ParallelJoinTokenArrived {
            instance_key,
            element_id: element_id.clone(),
        });

        let mut followups = Vec::new();
        if count_before + 1 >= threshold {
            events.push(Event::ElementCompleting {
                instance_key,
                element_instance_key: open_eik,
                element_id: element_id.clone(),
            });
            events.push(Event::ElementCompleted {
                instance_key,
                element_instance_key: open_eik,
                element_id: element_id.clone(),
            });
            events.push(Event::ParallelJoinReset {
                instance_key,
                element_id: element_id.clone(),
            });
            for flow in self.outgoing(instance_key, &element_id) {
                events.push(Event::SequenceFlowTaken {
                    instance_key,
                    from: element_id.clone(),
                    to: flow.to.clone(),
                });
                followups.push(Step::Activate {
                    instance_key,
                    element_id: flow.to,
                });
            }
        }

        (events, followups)
    }

    /// After a command settles, any active instance with no remaining tokens has
    /// completed.
    fn complete_finished_instances(&mut self, log: &mut Vec<Event>) {
        let touched: HashSet<Key> = log.iter().filter_map(|e| e.instance_key()).collect();
        let finished: Vec<Key> = touched
            .into_iter()
            .filter(|k| {
                self.state
                    .instances
                    .get(k)
                    .map(|i| i.state == ProcessInstanceState::Active && i.active.is_empty())
                    .unwrap_or(false)
            })
            .collect();

        for instance_key in finished {
            self.emit(log, Event::ProcessInstanceCompleted { instance_key });
        }
    }

    /// Applies an event and records it in the command's event log.
    fn emit(&mut self, log: &mut Vec<Event>, event: Event) {
        state::apply(&mut self.state, &event);
        log.push(event);
    }

    fn process_of_instance(&self, instance_key: Key) -> Option<&crate::model::ProcessDefinition> {
        let instance = self.state.instances.get(&instance_key)?;
        self.state
            .processes
            .get(&instance.process_id)
            .map(|p| &p.definition)
    }

    fn element_kind(&self, instance_key: Key, element_id: &str) -> Option<ElementKind> {
        self.process_of_instance(instance_key)?
            .element(element_id)
            .map(|e| e.kind.clone())
    }

    /// Finds the error boundary event attached to `task_element_id` that catches
    /// `error_code`, if any. When several match (a malformed model), the one with
    /// the smallest id is chosen so selection stays deterministic.
    fn find_error_boundary(
        &self,
        instance_key: Key,
        task_element_id: &str,
        error_code: &str,
    ) -> Option<ElementId> {
        let process = self.process_of_instance(instance_key)?;
        process
            .elements
            .values()
            .filter(|e| match &e.kind {
                ElementKind::ErrorBoundaryEvent {
                    attached_to,
                    error_code: ec,
                } => attached_to == task_element_id && ec == error_code,
                _ => false,
            })
            .map(|e| e.id.clone())
            .min()
    }

    fn outgoing(&self, instance_key: Key, element_id: &str) -> Vec<SequenceFlow> {
        self.process_of_instance(instance_key)
            .and_then(|p| p.element(element_id))
            .map(|e| e.outgoing.clone())
            .unwrap_or_default()
    }

    fn incoming_count(&self, instance_key: Key, element_id: &str) -> usize {
        self.process_of_instance(instance_key)
            .map(|p| p.incoming_count(element_id))
            .unwrap_or(0)
    }

    fn variables(&self, instance_key: Key) -> HashMap<String, Value> {
        self.state
            .instances
            .get(&instance_key)
            .map(|i| i.variables.clone())
            .unwrap_or_default()
    }

    /// Resolves a variable scope key to the process instance that owns it. The
    /// key may be the process instance itself or any of its active element
    /// instances; nano keeps a single instance-level variable scope, so both map
    /// to the same instance.
    fn resolve_scope(&self, scope_key: Key) -> Option<Key> {
        if self.state.instances.contains_key(&scope_key) {
            return Some(scope_key);
        }
        self.state
            .instances
            .values()
            .find(|i| i.active.contains_key(&scope_key))
            .map(|i| i.key)
    }

    fn join_eik(&self, instance_key: Key, element_id: &str) -> Option<Key> {
        self.state
            .instances
            .get(&instance_key)?
            .join_instances
            .get(element_id)
            .copied()
    }

    fn join_count(&self, instance_key: Key, element_id: &str) -> usize {
        self.state
            .instances
            .get(&instance_key)
            .and_then(|i| i.join_counts.get(element_id).copied())
            .unwrap_or(0)
    }
}

/// Errors returned by [`Engine::apply_command`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EngineError {
    /// `CreateInstance` referenced a process id that was never deployed.
    ProcessNotFound { process_id: String },
    /// `DeployProcess` was given a definition whose `start_event` is not among
    /// its elements.
    NoStartEvent { process_id: String },
    /// `CompleteJob` referenced a job key that does not exist.
    JobNotFound { job_key: Key },
    /// `CompleteJob`/`FailJob` referenced a job that is not in a state where it
    /// can be acted on (already completed, or failed with an incident raised).
    JobNotActive { job_key: Key },
    /// `CompleteJob`/`FailJob` referenced a job that has never been activated. A
    /// job must be activated at least once before it can be completed or failed.
    JobNotActivated { job_key: Key },
    /// `ResolveIncident` referenced an incident key that does not exist.
    IncidentNotFound { incident_key: Key },
    /// `ResolveIncident` referenced a job-incident whose job still has no
    /// retries; the retries must be updated before it can be resolved.
    IncidentNotResolvable { incident_key: Key, reason: String },
    /// `SetVariables` referenced a scope key that is neither a process instance
    /// nor any active element instance.
    ScopeNotFound { scope_key: Key },
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::ProcessNotFound { process_id } => {
                write!(f, "no deployed process with id {process_id}")
            }
            EngineError::NoStartEvent { process_id } => {
                write!(
                    f,
                    "process {process_id} has no start event among its elements"
                )
            }
            EngineError::JobNotFound { job_key } => write!(f, "no job with key {job_key}"),
            EngineError::JobNotActive { job_key } => {
                write!(f, "job {job_key} is not in a state that can be acted on")
            }
            EngineError::JobNotActivated { job_key } => {
                write!(
                    f,
                    "job {job_key} must be activated before it can be completed or failed"
                )
            }
            EngineError::IncidentNotFound { incident_key } => {
                write!(f, "no incident with key {incident_key}")
            }
            EngineError::IncidentNotResolvable {
                incident_key,
                reason,
            } => {
                write!(f, "incident {incident_key} cannot be resolved: {reason}")
            }
            EngineError::ScopeNotFound { scope_key } => {
                write!(f, "no variable scope with key {scope_key}")
            }
        }
    }
}

impl std::error::Error for EngineError {}

/// A job handed to a worker by [`Engine::activate_jobs`]: everything the worker
/// needs to do the work and complete it by key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivatedJob {
    pub key: Key,
    pub job_type: String,
    pub instance_key: Key,
    pub element_instance_key: Key,
    pub element_id: String,
    /// The worker the job was locked to.
    pub worker: String,
    /// Logical instant at which the activation lock expires.
    pub deadline: u64,
    /// Remaining retries for this job.
    pub retries: i32,
    /// A snapshot of the instance's variables at activation time.
    pub variables: HashMap<String, Value>,
}

/// Whether a job can be activated at the logical instant `now`: it is created
/// (never activated, or its lock was released) or its current lock has expired.
/// Failed (incident-parked) and completed jobs are never activatable.
fn job_activatable(job: &state::Job, now: u64) -> bool {
    match job.state {
        state::JobState::Completed | state::JobState::Failed | state::JobState::Errored => false,
        state::JobState::Created => true,
        state::JobState::Activated => job.deadline.is_some_and(|d| d <= now),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Condition, ProcessBuilder, ProcessDefinition};

    fn linear_with_task() -> ProcessDefinition {
        ProcessBuilder::new("order")
            .start_event("start")
            .service_task("charge", "payment")
            .end_event("end")
            .connect("start", "charge")
            .connect("charge", "end")
            .build()
            .unwrap()
    }

    /// Test helper: activate the first job of `job_type` (locking it) and
    /// complete it by key, returning the events from completion.
    fn complete_one(engine: &mut Engine, job_type: &str) -> Vec<Event> {
        let job = engine
            .activate_jobs(job_type, "test-worker", 10, 60_000, 0)
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("no activatable job of type {job_type}"));
        engine
            .apply_command(Command::complete_job(job.key))
            .unwrap()
    }

    #[test]
    fn should_park_on_service_task_then_complete_on_job() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();

        let events = engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

        assert_eq!(engine.pending_jobs().len(), 1);
        assert!(!engine.is_completed(instance_key));

        let job_key = engine.pending_jobs()[0].key;
        engine.activate_jobs("payment", "worker-1", 10, 60_000, 0);
        let events = engine
            .apply_command(Command::complete_job(job_key))
            .unwrap();

        assert!(engine.is_completed(instance_key));
        assert!(events.contains(&Event::ProcessInstanceCompleted { instance_key }));
        assert!(engine.pending_jobs().is_empty());
    }

    #[test]
    fn should_complete_immediately_when_no_task() {
        let def = ProcessBuilder::new("noop")
            .start_event("s")
            .end_event("e")
            .connect("s", "e")
            .build()
            .unwrap();

        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let events = engine
            .apply_command(Command::create_instance("noop"))
            .unwrap();

        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
        assert!(engine.is_completed(instance_key));
        assert!(events.contains(&Event::ProcessInstanceCompleted { instance_key }));
    }

    #[test]
    fn should_run_parallel_split_and_join() {
        // s -> split =< a, b >= join -> e   (a and b are service tasks)
        let def = ProcessBuilder::new("par")
            .start_event("s")
            .parallel_gateway("split")
            .service_task("a", "ja")
            .service_task("b", "jb")
            .parallel_gateway("join")
            .end_event("e")
            .connect("s", "split")
            .connect("split", "a")
            .connect("split", "b")
            .connect("a", "join")
            .connect("b", "join")
            .connect("join", "e")
            .build()
            .unwrap();

        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let events = engine
            .apply_command(Command::create_instance("par"))
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

        // given both branches forked and both tasks are waiting
        assert_eq!(engine.pending_jobs().len(), 2);
        assert!(!engine.is_completed(instance_key));

        // when the first branch's job completes, the join must still wait
        complete_one(&mut engine, "ja");
        assert!(!engine.is_completed(instance_key));

        // when the second branch completes, the join fires and the instance ends
        let final_events = complete_one(&mut engine, "jb");
        assert!(engine.is_completed(instance_key));
        assert!(final_events.contains(&Event::ProcessInstanceCompleted { instance_key }));
        // exactly one ProcessInstanceCompleted across the whole run
        assert_eq!(
            final_events
                .iter()
                .filter(|e| matches!(e, Event::ProcessInstanceCompleted { .. }))
                .count(),
            1
        );
    }

    fn approval_process() -> ProcessDefinition {
        // s -> g(xor): decision==yes -> approved ; else default -> rejected
        ProcessBuilder::new("approval")
            .start_event("s")
            .exclusive_gateway("g")
            .end_event("approved")
            .end_event("rejected")
            .connect("s", "g")
            .connect_when(
                "g",
                "approved",
                Condition::Equals {
                    variable: "decision".into(),
                    value: Value::Str("yes".into()),
                },
            )
            .connect("g", "rejected")
            .build()
            .unwrap()
    }

    #[test]
    fn should_route_exclusive_gateway_by_variable() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(approval_process()))
            .unwrap();

        let vars = HashMap::from([("decision".to_string(), Value::Str("yes".into()))]);
        let events = engine
            .apply_command(Command::create_instance_with("approval", vars))
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

        assert!(engine.is_completed(instance_key));
        assert!(events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "approved"
        )));
        assert!(!events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "rejected"
        )));
    }

    #[test]
    fn should_take_default_flow_when_no_condition_matches() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(approval_process()))
            .unwrap();

        let vars = HashMap::from([("decision".to_string(), Value::Str("no".into()))]);
        let events = engine
            .apply_command(Command::create_instance_with("approval", vars))
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

        assert!(engine.is_completed(instance_key));
        assert!(events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "rejected"
        )));
    }

    #[test]
    fn should_route_on_variables_returned_by_a_completed_job() {
        // s -> task(decide) -> g(xor): decision==yes -> approved ; else rejected
        let def = ProcessBuilder::new("review")
            .start_event("s")
            .service_task("decide", "decision")
            .exclusive_gateway("g")
            .end_event("approved")
            .end_event("rejected")
            .connect("s", "decide")
            .connect("decide", "g")
            .connect_when(
                "g",
                "approved",
                Condition::Equals {
                    variable: "decision".into(),
                    value: Value::Str("yes".into()),
                },
            )
            .connect("g", "rejected")
            .build()
            .unwrap();

        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let created = engine
            .apply_command(Command::create_instance("review"))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

        // given the instance parked on the service task
        assert!(!engine.is_completed(instance_key));

        // when the worker completes the job, returning decision=yes
        let job_key = engine.activate_jobs("decision", "w", 1, 60_000, 0)[0].key;
        let vars = HashMap::from([("decision".to_string(), Value::Str("yes".into()))]);
        let events = engine
            .apply_command(Command::complete_job_with(job_key, vars))
            .unwrap();

        // then the gateway routes on the returned variable to the approved branch
        assert!(engine.is_completed(instance_key));
        assert!(events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "approved"
        )));
        assert!(!events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "rejected"
        )));
    }

    #[test]
    fn should_raise_incident_when_no_exclusive_flow_matches() {
        // Both flows are conditional; neither matches -> incident, token parked.
        let def = ProcessBuilder::new("strict")
            .start_event("s")
            .exclusive_gateway("g")
            .end_event("yes_end")
            .end_event("no_end")
            .connect("s", "g")
            .connect_when(
                "g",
                "yes_end",
                Condition::Equals {
                    variable: "d".into(),
                    value: Value::Bool(true),
                },
            )
            .connect_when(
                "g",
                "no_end",
                Condition::Equals {
                    variable: "d".into(),
                    value: Value::Bool(false),
                },
            )
            .build()
            .unwrap();

        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let vars = HashMap::from([("d".to_string(), Value::Int(7))]);
        let events = engine
            .apply_command(Command::create_instance_with("strict", vars))
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

        assert!(!engine.is_completed(instance_key));
        assert!(events
            .iter()
            .any(|e| matches!(e, Event::IncidentRaised { .. })));
        assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);
    }

    #[test]
    fn should_re_activate_a_failed_job_that_still_has_retries() {
        // given an activated job
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        let created = engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
        let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;

        // when the worker fails it with retries remaining
        engine
            .apply_command(Command::fail_job(job_key, 2, "transient error"))
            .unwrap();

        // then no incident is raised and the job is activatable again
        assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
        assert_eq!(engine.pending_jobs().len(), 1);
        let reactivated = engine.activate_jobs("payment", "B", 10, 60_000, 1);
        assert_eq!(reactivated.len(), 1);
        assert_eq!(reactivated[0].key, job_key);
        assert_eq!(reactivated[0].retries, 2);
    }

    #[test]
    fn should_raise_an_incident_when_a_job_fails_with_no_retries_left() {
        // given an activated job
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        let created = engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
        let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;

        // when the worker fails it with no retries left
        let events = engine
            .apply_command(Command::fail_job(job_key, 0, "boom"))
            .unwrap();

        // then an incident is raised, the job parks, and it is not activatable
        assert!(events.iter().any(|e| matches!(
            e,
            Event::IncidentRaised { reason, .. } if reason == "boom"
        )));
        assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);
        assert!(engine.pending_jobs().is_empty());
        assert!(engine
            .activate_jobs("payment", "B", 10, 60_000, 100)
            .is_empty());

        // and the parked job can no longer be completed
        let err = engine
            .apply_command(Command::complete_job(job_key))
            .unwrap_err();
        assert_eq!(err, EngineError::JobNotActive { job_key });
    }

    #[test]
    fn should_reject_failing_a_job_that_was_never_activated() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        let job_key = engine.pending_jobs()[0].key;

        let err = engine
            .apply_command(Command::fail_job(job_key, 1, "nope"))
            .unwrap_err();
        assert_eq!(err, EngineError::JobNotActivated { job_key });
    }

    #[test]
    fn should_recover_a_parked_job_by_updating_retries_and_resolving_its_incident() {
        // given a job parked on a no-retries incident
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        let created = engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
        let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;
        let raised = engine
            .apply_command(Command::fail_job(job_key, 0, "boom"))
            .unwrap();
        let incident_key = raised
            .iter()
            .find_map(|e| match e {
                Event::IncidentRaised { incident_key, .. } => Some(*incident_key),
                _ => None,
            })
            .unwrap();

        // when resolving before retries are restored, it is rejected
        let err = engine
            .apply_command(Command::resolve_incident(incident_key))
            .unwrap_err();
        assert!(matches!(
            err,
            EngineError::IncidentNotResolvable { incident_key: k, .. } if k == incident_key
        ));

        // when retries are updated and the incident resolved
        engine
            .apply_command(Command::update_job_retries(job_key, 2))
            .unwrap();
        let resolved = engine
            .apply_command(Command::resolve_incident(incident_key))
            .unwrap();

        // then the incident is retained as resolved and the job is activatable again
        assert!(resolved
            .iter()
            .any(|e| matches!(e, Event::IncidentResolved { .. })));
        assert_eq!(
            engine.incident(incident_key).unwrap().state,
            state::IncidentState::Resolved
        );
        assert!(engine.instance(instance_key).unwrap().incidents.is_empty());

        // and a worker can pick it up and drive the instance to completion
        let job_key2 = engine.activate_jobs("payment", "B", 1, 60_000, 100)[0].key;
        assert_eq!(job_key2, job_key);
        engine
            .apply_command(Command::complete_job(job_key))
            .unwrap();
        assert!(engine.is_completed(instance_key));
    }

    #[test]
    fn should_reject_resolving_an_unknown_incident() {
        let mut engine = Engine::new();
        let err = engine
            .apply_command(Command::resolve_incident(999))
            .unwrap_err();
        assert_eq!(err, EngineError::IncidentNotFound { incident_key: 999 });
    }

    #[test]
    fn should_re_raise_a_gateway_incident_when_resolution_still_finds_no_flow() {
        // A non-job incident (no matching exclusive flow) is resolved by
        // re-evaluating the gateway. With the variables unchanged it still
        // matches nothing, so resolution retries the work and a *fresh* incident
        // is raised — the token stays parked rather than silently vanishing.
        let def = ProcessBuilder::new("strict")
            .start_event("s")
            .exclusive_gateway("g")
            .end_event("yes_end")
            .connect("s", "g")
            .connect_when(
                "g",
                "yes_end",
                Condition::Equals {
                    variable: "d".into(),
                    value: Value::Bool(true),
                },
            )
            .build()
            .unwrap();
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let vars = HashMap::from([("d".to_string(), Value::Int(7))]);
        let created = engine
            .apply_command(Command::create_instance_with("strict", vars))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
        let original = engine.incidents()[0].key;
        assert!(engine.incident(original).unwrap().job_key.is_none());

        // when resolved (the gateway is re-evaluated)
        engine
            .apply_command(Command::resolve_incident(original))
            .unwrap();

        // then the original incident is retained as resolved and a new active one
        // replaces it, and the instance has still not completed.
        assert_eq!(
            engine.incident(original).unwrap().state,
            state::IncidentState::Resolved
        );
        let active = engine.active_incidents();
        assert_eq!(active.len(), 1);
        assert_ne!(active[0].key, original);
        assert_eq!(active[0].kind, state::IncidentKind::NoMatchingSequenceFlow);
        assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);
        assert!(!engine.is_completed(instance_key));
    }

    #[test]
    fn should_recover_a_gateway_incident_after_fixing_variables() {
        // given an exclusive gateway parked on a no-matching-flow incident
        let def = ProcessBuilder::new("strict")
            .start_event("s")
            .exclusive_gateway("g")
            .end_event("yes_end")
            .connect("s", "g")
            .connect_when(
                "g",
                "yes_end",
                Condition::Equals {
                    variable: "d".into(),
                    value: Value::Bool(true),
                },
            )
            .build()
            .unwrap();
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let vars = HashMap::from([("d".to_string(), Value::Int(7))]);
        let created = engine
            .apply_command(Command::create_instance_with("strict", vars))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
        let incident_key = engine.incidents()[0].key;

        // when the operator fixes the variable then resolves the incident
        engine
            .apply_command(Command::set_variables(
                instance_key,
                HashMap::from([("d".to_string(), Value::Bool(true))]),
            ))
            .unwrap();
        engine
            .apply_command(Command::resolve_incident(incident_key))
            .unwrap();

        // then the gateway re-evaluates, matches, and the instance completes; the
        // incident is retained as resolved
        assert_eq!(
            engine.incident(incident_key).unwrap().state,
            state::IncidentState::Resolved
        );
        assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
        assert!(engine.is_completed(instance_key));
    }

    #[test]
    fn should_set_variables_via_an_element_instance_scope_key() {
        // given a service task parked with a known element instance key
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        let element_instance_key = engine.pending_jobs()[0].element_instance_key;
        let instance_key = engine.pending_jobs()[0].instance_key;

        // when variables are set against the element instance key (not the
        // process instance key)
        engine
            .apply_command(Command::set_variables(
                element_instance_key,
                HashMap::from([("x".to_string(), Value::Int(42))]),
            ))
            .unwrap();

        // then they land in the owning instance's single variable scope
        assert_eq!(
            engine.instance(instance_key).unwrap().variables.get("x"),
            Some(&Value::Int(42))
        );
    }

    #[test]
    fn should_reject_setting_variables_on_an_unknown_scope() {
        let mut engine = Engine::new();
        let err = engine
            .apply_command(Command::set_variables(
                404,
                HashMap::from([("x".to_string(), Value::Int(1))]),
            ))
            .unwrap_err();
        assert_eq!(err, EngineError::ScopeNotFound { scope_key: 404 });
    }

    #[test]
    fn should_retry_the_service_task_when_an_unhandled_error_incident_is_resolved() {
        // given a service task whose worker threw an uncaught business error,
        // parking the token on an unhandled-error incident
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        let created = engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
        let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;
        let raised = engine
            .apply_command(Command::throw_job_error(job_key, "BOOM", "no boundary"))
            .unwrap();
        let incident_key = raised
            .iter()
            .find_map(|e| match e {
                Event::IncidentRaised { incident_key, .. } => Some(*incident_key),
                _ => None,
            })
            .unwrap();
        assert!(engine.pending_jobs().is_empty());

        // when the incident is resolved
        let resolved = engine
            .apply_command(Command::resolve_incident(incident_key))
            .unwrap();

        // then a fresh job is created for the still-active service task, and a
        // worker can activate and complete it to drive the instance home.
        assert!(resolved
            .iter()
            .any(|e| matches!(e, Event::JobCreated { .. })));
        assert_eq!(
            engine.incident(incident_key).unwrap().state,
            state::IncidentState::Resolved
        );
        assert_eq!(engine.pending_jobs().len(), 1);
        let retry = engine.activate_jobs("payment", "B", 1, 60_000, 100);
        assert_eq!(retry.len(), 1);
        assert_ne!(retry[0].key, job_key);
        engine
            .apply_command(Command::complete_job(retry[0].key))
            .unwrap();
        assert!(engine.is_completed(instance_key));
    }

    #[test]
    fn should_stamp_an_incident_with_the_command_clock() {
        // given a parked job-incident raised at a known instant
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;

        // when failed with no retries at now = 1_700_000_000_000
        let raised = engine
            .apply_command_at(Command::fail_job(job_key, 0, "boom"), 1_700_000_000_000)
            .unwrap();
        let incident_key = raised
            .iter()
            .find_map(|e| match e {
                Event::IncidentRaised { incident_key, .. } => Some(*incident_key),
                _ => None,
            })
            .unwrap();

        // then the incident records that instant
        assert_eq!(engine.incident(incident_key).unwrap().created_at, 1_700_000_000_000);
    }

    #[test]
    fn should_retain_a_resolved_incident_as_an_audit_record() {
        // given a parked job-incident
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        let created = engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
        let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;
        let raised = engine
            .apply_command(Command::fail_job(job_key, 0, "boom"))
            .unwrap();
        let incident_key = raised
            .iter()
            .find_map(|e| match e {
                Event::IncidentRaised { incident_key, .. } => Some(*incident_key),
                _ => None,
            })
            .unwrap();
        engine
            .apply_command(Command::update_job_retries(job_key, 2))
            .unwrap();

        // when resolved with an operation reference at a known instant
        engine
            .apply_command_at(
                Command::resolve_incident_with(incident_key, 4242),
                1_700_000_000_500,
            )
            .unwrap();

        // then the record is retained as resolved with audit metadata
        let incident = engine.incident(incident_key).unwrap();
        assert_eq!(incident.state, state::IncidentState::Resolved);
        assert_eq!(incident.resolved_at, Some(1_700_000_000_500));
        assert_eq!(incident.operation_reference, Some(4242));
        // and it no longer counts as active, so the instance has no open incident
        assert!(engine.active_incidents().is_empty());
        assert!(engine.instance(instance_key).unwrap().incidents.is_empty());

        // and resolving it again is rejected (already resolved)
        let err = engine
            .apply_command(Command::resolve_incident(incident_key))
            .unwrap_err();
        assert!(matches!(
            err,
            EngineError::IncidentNotResolvable { incident_key: k, .. } if k == incident_key
        ));
    }

    fn process_with_error_boundary() -> ProcessDefinition {
        // s -> charge(task) --normal--> done
        //              \--(error CARD_DECLINED)--> boundary -> declined
        ProcessBuilder::new("payment")
            .start_event("s")
            .service_task("charge", "payment")
            .error_boundary_event("boundary", "charge", "CARD_DECLINED")
            .end_event("done")
            .end_event("declined")
            .connect("s", "charge")
            .connect("charge", "done")
            .connect("boundary", "declined")
            .build()
            .unwrap()
    }

    #[test]
    fn should_route_to_an_error_boundary_when_a_job_throws_a_matching_error() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(process_with_error_boundary()))
            .unwrap();
        let created = engine
            .apply_command(Command::create_instance("payment"))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
        let job_key = engine.activate_jobs("payment", "w", 1, 60_000, 0)[0].key;

        // when the worker throws the caught business error
        let events = engine
            .apply_command(Command::throw_job_error(
                job_key,
                "CARD_DECLINED",
                "card was declined",
            ))
            .unwrap();

        // then the activity is interrupted and the error path runs to completion
        assert!(engine.is_completed(instance_key));
        assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
        assert!(events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "boundary" && to == "declined"
        )));
        // the task's normal outgoing flow was NOT taken
        assert!(!events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "done"
        )));
        // and the job is consumed: it cannot be completed afterwards
        let err = engine
            .apply_command(Command::complete_job(job_key))
            .unwrap_err();
        assert_eq!(err, EngineError::JobNotActive { job_key });
    }

    #[test]
    fn should_raise_an_incident_when_a_thrown_error_is_unhandled() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(process_with_error_boundary()))
            .unwrap();
        let created = engine
            .apply_command(Command::create_instance("payment"))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
        let job_key = engine.activate_jobs("payment", "w", 1, 60_000, 0)[0].key;

        // when the worker throws an error no boundary catches
        let events = engine
            .apply_command(Command::throw_job_error(job_key, "UNKNOWN", "boom"))
            .unwrap();

        // then an incident is raised and the instance does not complete
        assert!(events.iter().any(|e| matches!(
            e,
            Event::IncidentRaised { reason, .. } if reason.contains("UNKNOWN")
        )));
        assert!(!engine.is_completed(instance_key));
        assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);
    }

    #[test]
    fn should_reject_throwing_an_error_from_a_job_that_was_never_activated() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(process_with_error_boundary()))
            .unwrap();
        engine
            .apply_command(Command::create_instance("payment"))
            .unwrap();
        let job_key = engine.pending_jobs()[0].key;

        let err = engine
            .apply_command(Command::throw_job_error(job_key, "CARD_DECLINED", "x"))
            .unwrap_err();
        assert_eq!(err, EngineError::JobNotActivated { job_key });
    }

    #[test]
    fn should_reject_unknown_process() {
        let mut engine = Engine::new();
        let err = engine
            .apply_command(Command::create_instance("missing"))
            .unwrap_err();
        assert_eq!(
            err,
            EngineError::ProcessNotFound {
                process_id: "missing".into()
            }
        );
    }

    #[test]
    fn should_reject_unknown_job() {
        let mut engine = Engine::new();
        let err = engine.apply_command(Command::complete_job(42)).unwrap_err();
        assert_eq!(err, EngineError::JobNotFound { job_key: 42 });
    }

    #[test]
    fn should_reject_completing_a_job_that_was_never_activated() {
        // given an instance parked on a service task with a created (but
        // un-activated) job
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        let job_key = engine.pending_jobs()[0].key;

        // when it is completed without being activated first
        let err = engine
            .apply_command(Command::complete_job(job_key))
            .unwrap_err();

        // then it is rejected
        assert_eq!(err, EngineError::JobNotActivated { job_key });
    }

    #[test]
    fn should_lock_an_activated_job_until_its_deadline() {
        // given an instance parked on a service task
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        engine
            .apply_command(Command::create_instance("order"))
            .unwrap();

        // when worker A activates the job at t=0 for 1000ms
        let activated = engine.activate_jobs("payment", "A", 10, 1_000, 0);
        assert_eq!(activated.len(), 1);
        assert_eq!(activated[0].worker, "A");
        assert_eq!(activated[0].deadline, 1_000);

        // then a second activation before the deadline gets nothing
        assert!(engine
            .activate_jobs("payment", "B", 10, 1_000, 500)
            .is_empty());

        // and once the lock has expired the job is activatable again
        let reactivated = engine.activate_jobs("payment", "B", 10, 1_000, 1_500);
        assert_eq!(reactivated.len(), 1);
        assert_eq!(reactivated[0].worker, "B");
    }

    #[test]
    fn should_let_a_previous_worker_complete_after_re_activation() {
        // given worker A activated the job, then its lock expired and worker B
        // re-activated it (e.g. A's work outran the activation window)
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        let events = engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

        let job_key = engine.activate_jobs("payment", "A", 10, 1_000, 0)[0].key;
        let reactivated = engine.activate_jobs("payment", "B", 10, 1_000, 1_500);
        assert_eq!(reactivated[0].key, job_key);

        // when the slow worker A finally completes the job by key
        engine
            .apply_command(Command::complete_job(job_key))
            .unwrap();

        // then completion succeeds and the instance finishes
        assert!(engine.is_completed(instance_key));

        // and B can no longer complete the already-completed job
        let err = engine
            .apply_command(Command::complete_job(job_key))
            .unwrap_err();
        assert_eq!(err, EngineError::JobNotActive { job_key });
    }

    #[test]
    fn should_expire_locks_on_tick() {
        // given an activated (locked) job
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        let job_key = engine.activate_jobs("payment", "A", 10, 1_000, 0)[0].key;
        assert!(engine.pending_jobs().is_empty());

        // when a tick runs after the deadline
        engine.expire_jobs(2_000);

        // then the job is activatable again
        assert_eq!(engine.pending_jobs().len(), 1);
        assert_eq!(engine.pending_jobs()[0].key, job_key);
    }

    #[test]
    fn should_dispatch_jobs_to_a_callback_worker() {
        // given an instance parked on a service task
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        let events = engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

        // when a callback worker polls and handles the job
        let mut seen = Vec::new();
        let handled = engine.poll_jobs("payment", "cb", 10, 60_000, 0, |job| {
            seen.push(job.job_type.clone());
            Some(HashMap::new())
        });

        // then the job was dispatched and completed, finishing the instance
        assert_eq!(handled, 1);
        assert_eq!(seen, ["payment"]);
        assert!(engine.is_completed(instance_key));
    }

    #[test]
    fn should_be_deterministic_and_replayable() {
        let run = || {
            let mut engine = Engine::new();
            let mut all = Vec::new();
            all.extend(
                engine
                    .apply_command(Command::DeployProcess(linear_with_task()))
                    .unwrap(),
            );
            all.extend(
                engine
                    .apply_command(Command::create_instance("order"))
                    .unwrap(),
            );
            let job_key = engine.pending_jobs()[0].key;
            all.extend(
                engine
                    .apply_command(Command::activate_jobs("payment", "worker-1", 1, 60_000, 0))
                    .unwrap(),
            );
            all.extend(
                engine
                    .apply_command(Command::complete_job(job_key))
                    .unwrap(),
            );
            (engine, all)
        };

        let (engine_a, log_a) = run();
        let (_engine_b, log_b) = run();
        assert_eq!(log_a, log_b);

        // Replaying the log over a fresh State reconstructs engine state exactly.
        let mut replayed = State::new();
        for event in &log_a {
            state::apply(&mut replayed, event);
        }
        assert_eq!(&replayed, engine_a.state());
    }

    #[test]
    fn should_recover_state_and_key_generator_via_replay() {
        // given a run that deploys, starts an instance, and raises an incident
        let (engine_a, log) = {
            let mut engine = Engine::new();
            let mut log = Vec::new();
            log.extend(
                engine
                    .apply_command(Command::DeployProcess(linear_with_task()))
                    .unwrap(),
            );
            log.extend(
                engine
                    .apply_command(Command::create_instance("order"))
                    .unwrap(),
            );
            let job_key = engine.pending_jobs()[0].key;
            engine
                .apply_command(Command::activate_jobs("payment", "w", 1, 60_000, 0))
                .unwrap();
            // fail with no retries -> parks the job and raises an incident
            log.extend(
                engine
                    .apply_command_at(Command::fail_job(job_key, 0, "boom"), 1_234)
                    .unwrap(),
            );
            (engine, log)
        };

        // when the durable log is replayed into a fresh engine
        // (activation events are volatile and intentionally not part of `log`)
        let mut recovered = Engine::replay(log);

        // then state matches (modulo the volatile activation: the replayed job
        // is parked Failed, identical to the original after fail)
        let orig_incident = engine_a.active_incidents()[0];
        let rec_incident = recovered.active_incidents()[0];
        assert_eq!(rec_incident.key, orig_incident.key);
        assert_eq!(rec_incident.created_at, 1_234);
        assert_eq!(rec_incident.kind, state::IncidentKind::JobNoRetries);

        // and the key generator resumes past every replayed key: a new instance
        // mints a strictly higher key than anything in the recovered log
        let max_existing = recovered
            .state()
            .instances
            .keys()
            .chain(recovered.state().jobs.keys())
            .chain(recovered.state().incidents.keys())
            .copied()
            .max()
            .unwrap();
        let events = recovered
            .apply_command(Command::create_instance("order"))
            .unwrap();
        let new_instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
        assert!(
            new_instance_key > max_existing,
            "new key {new_instance_key} must exceed replayed max {max_existing}"
        );
    }
}
