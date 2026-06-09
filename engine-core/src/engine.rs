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

    /// Fires every armed timer whose due instant is at or before `now`, resuming
    /// the token parked on each. Like [`Engine::expire_jobs`], the host drives
    /// this periodically; the engine never reads a clock. Returns the events the
    /// tick produced (empty when nothing was due).
    pub fn trigger_timers(&mut self, now: u64) -> Vec<Event> {
        self.apply_command_at(Command::TriggerTimers { now }, now)
            .expect("TriggerTimers never fails")
    }

    /// All armed and fired timers (fired ones are retained so they never
    /// re-fire). Filter by [`state::Timer::state`] for only-pending timers.
    pub fn timers(&self) -> Vec<&state::Timer> {
        self.state.timers.values().collect()
    }

    /// Publishes a message and correlates it to every open subscription whose
    /// name and correlation key match, merging the message `variables` into each
    /// correlated instance. Like [`Engine::trigger_timers`], the host drives
    /// this; the engine never reads a clock. Returns the events produced — the
    /// heading [`Event::MessagePublished`] (always) plus an
    /// [`Event::MessageCorrelated`] per correlated subscription. Mirrors applying
    /// a [`Command::CorrelateMessage`].
    pub fn correlate_message(
        &mut self,
        message_name: impl Into<String>,
        correlation_key: impl Into<String>,
        variables: HashMap<String, Value>,
        now: u64,
    ) -> Vec<Event> {
        self.apply_command_at(
            Command::correlate_message_with(message_name, correlation_key, variables),
            now,
        )
        .expect("CorrelateMessage never fails")
    }

    /// All open and settled message subscriptions (correlated/cancelled ones are
    /// retained). Filter by [`state::MessageSubscription::state`] for only-open
    /// subscriptions.
    pub fn message_subscriptions(&self) -> Vec<&state::MessageSubscription> {
        self.state.message_subscriptions.values().collect()
    }

    /// Looks up a message subscription by key, whether open or settled.
    pub fn message_subscription(&self, key: Key) -> Option<&state::MessageSubscription> {
        self.state.message_subscriptions.get(&key)
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

    /// Creates a fresh process instance and queues its start event for
    /// activation. Shared by `CreateInstance`, message-start correlation, and
    /// timer-start firing.
    fn start_instance(
        &mut self,
        log: &mut Vec<Event>,
        queue: &mut VecDeque<Step>,
        process_id: String,
        start_event: ElementId,
        variables: HashMap<String, Value>,
    ) -> Key {
        let instance_key = self.mint_key();
        self.emit(
            log,
            Event::ProcessInstanceCreated {
                instance_key,
                process_id,
                variables,
                created_at: self.now,
            },
        );
        queue.push_back(Step::Activate {
            instance_key,
            element_id: start_event,
        });
        instance_key
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
            let process_id = process.id.clone();
            let start_element_id = process.start_event.clone();
            let start_kind = process
                .elements
                .get(&process.start_event)
                .map(|e| e.kind.clone());
            self.emit(
                log,
                Event::ProcessDeployed {
                    deployment_key,
                    process_definition_key,
                    version,
                    process,
                },
            );
            match start_kind {
                Some(ElementKind::MessageStartEvent { message_name }) => {
                    self.emit(
                        log,
                        Event::MessageStartSubscriptionCreated {
                            process_definition_key,
                            process_id,
                            message_name,
                            start_element_id,
                        },
                    );
                }
                Some(ElementKind::TimerStartEvent {
                    interval_millis,
                    repeating,
                }) => {
                    let timer_key = self.mint_key();
                    let due_at = self.now.saturating_add(interval_millis);
                    self.emit(
                        log,
                        Event::ProcessStartTimerArmed {
                            timer_key,
                            process_definition_key,
                            process_id,
                            start_element_id,
                            due_at,
                            interval_millis,
                            repeating,
                        },
                    );
                }
                _ => {}
            }
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
                self.start_instance(&mut log, &mut queue, process_id, start_event, variables);
            }

            Command::CompleteJob { job_key, variables } => {
                let job = self
                    .state
                    .jobs
                    .get(&job_key)
                    .ok_or(EngineError::JobNotFound { job_key })?;
                if matches!(
                    job.state,
                    state::JobState::Completed
                        | state::JobState::Failed
                        | state::JobState::Errored
                        | state::JobState::Canceled
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

            Command::TriggerTimers { now } => {
                // Fire every due timer (deterministic order by key). An
                // intermediate catch resumes its parked token; an interrupting
                // boundary timer interrupts its activity and routes out the
                // boundary event.
                let mut due: Vec<Key> = self
                    .state
                    .timers
                    .values()
                    .filter(|t| t.state == state::TimerState::Created && t.due_at <= now)
                    .map(|t| t.key)
                    .collect();
                due.sort_unstable();
                for timer_key in due {
                    // A boundary timer earlier in this batch may have interrupted
                    // an activity that disarmed this one; re-check it is still due.
                    let timer = match self.state.timers.get(&timer_key) {
                        Some(t) if t.state == state::TimerState::Created => t,
                        _ => continue,
                    };
                    let instance_key = timer.instance_key;
                    let element_instance_key = timer.element_instance_key;
                    let element_id = timer.element_id.clone();
                    let kind = timer.kind.clone();

                    self.emit(
                        &mut log,
                        Event::TimerTriggered {
                            timer_key,
                            instance_key,
                            element_instance_key,
                            element_id: element_id.clone(),
                        },
                    );

                    match kind {
                        // Catch event: completing it resumes the token along its
                        // own outgoing flow.
                        state::TimerKind::IntermediateCatch => {
                            queue.push_back(Step::Complete {
                                instance_key,
                                element_instance_key,
                                element_id,
                            });
                        }
                        // Boundary timer: interrupt the attached activity (cancel
                        // its job and disarm any sibling boundary timers), then run
                        // the boundary event's outgoing flow. `element_instance_key`
                        // / `element_id` are the activity here.
                        state::TimerKind::InterruptingBoundary {
                            boundary_element_id,
                        } => {
                            // Cancel the job parked on the activity, if any is
                            // still in play.
                            if let Some(job_key) = self.active_job_on(element_instance_key) {
                                self.emit(
                                    &mut log,
                                    Event::JobCanceled {
                                        job_key,
                                        instance_key,
                                    },
                                );
                            }
                            // Interrupt the activity element instance.
                            self.emit(
                                &mut log,
                                Event::ElementCompleting {
                                    instance_key,
                                    element_instance_key,
                                    element_id: element_id.clone(),
                                },
                            );
                            self.emit(
                                &mut log,
                                Event::ElementCompleted {
                                    instance_key,
                                    element_instance_key,
                                    element_id,
                                },
                            );
                            // Disarm any sibling boundary timers on the activity.
                            for event in self.cancel_boundary_timers_on(element_instance_key) {
                                self.emit(&mut log, event);
                            }
                            // Disarm any boundary message subscriptions on it too.
                            for event in
                                self.cancel_boundary_message_subscriptions_on(element_instance_key)
                            {
                                self.emit(&mut log, event);
                            }
                            queue.push_back(Step::Activate {
                                instance_key,
                                element_id: boundary_element_id,
                            });
                        }
                    }
                }

                // Process-level start timers: fire every due one (deterministic
                // by key), creating a new instance. A cycle re-arms for the next
                // interval; a one-shot is retained with no due time so it never
                // fires again.
                let mut due_starts: Vec<Key> = self
                    .state
                    .start_timers
                    .values()
                    .filter(|t| t.due_at.is_some_and(|d| d <= now))
                    .map(|t| t.timer_key)
                    .collect();
                due_starts.sort_unstable();
                for timer_key in due_starts {
                    let timer = match self.state.start_timers.get(&timer_key) {
                        Some(t) => t,
                        None => continue,
                    };
                    let process_id = timer.process_id.clone();
                    let start_element_id = timer.start_element_id.clone();
                    let due_at = match timer.due_at {
                        Some(d) => d,
                        None => continue,
                    };
                    let next_due_at = if timer.repeating {
                        Some(due_at.saturating_add(timer.interval_millis))
                    } else {
                        None
                    };
                    self.emit(
                        &mut log,
                        Event::ProcessStartTimerFired {
                            timer_key,
                            next_due_at,
                        },
                    );
                    self.start_instance(
                        &mut log,
                        &mut queue,
                        process_id,
                        start_element_id,
                        HashMap::new(),
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
                    state::JobState::Completed
                        | state::JobState::Failed
                        | state::JobState::Errored
                        | state::JobState::Canceled
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
                    state::JobState::Completed
                        | state::JobState::Failed
                        | state::JobState::Errored
                        | state::JobState::Canceled
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
                        // An error boundary interrupting the task also disarms any
                        // timer boundaries and message subscriptions on it.
                        for event in self.cancel_boundary_timers_on(element_instance_key) {
                            self.emit(&mut log, event);
                        }
                        for event in
                            self.cancel_boundary_message_subscriptions_on(element_instance_key)
                        {
                            self.emit(&mut log, event);
                        }
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
                    state::JobState::Completed | state::JobState::Errored | state::JobState::Canceled
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

            Command::CorrelateMessage {
                message_name,
                correlation_key,
                variables,
            } => {
                // Always mint a message key (Zeebe records every published
                // message); it is returned to the host and, carried on the
                // MessagePublished event, restores the key generator on replay.
                let message_key = self.mint_key();
                self.emit(
                    &mut log,
                    Event::MessagePublished {
                        message_key,
                        message_name: message_name.clone(),
                        correlation_key: correlation_key.clone(),
                    },
                );

                // Correlate to every matching open subscription, deterministic by
                // subscription key. Messages are not buffered: with no match the
                // message is simply dropped.
                let mut matched: Vec<Key> = self
                    .state
                    .message_subscriptions
                    .values()
                    .filter(|s| {
                        s.state == state::MessageSubscriptionState::Open
                            && s.message_name == message_name
                            && s.correlation_key == correlation_key
                    })
                    .map(|s| s.key)
                    .collect();
                matched.sort_unstable();

                for subscription_key in matched {
                    // A boundary correlation earlier in this batch may have
                    // interrupted an activity that cancelled this subscription;
                    // re-check it is still open.
                    let subscription = match self.state.message_subscriptions.get(&subscription_key)
                    {
                        Some(s) if s.state == state::MessageSubscriptionState::Open => s,
                        _ => continue,
                    };
                    let instance_key = subscription.instance_key;
                    let element_instance_key = subscription.element_instance_key;
                    let element_id = subscription.element_id.clone();
                    let kind = subscription.kind.clone();

                    self.emit(
                        &mut log,
                        Event::MessageCorrelated {
                            subscription_key,
                            message_key,
                            instance_key,
                            element_instance_key,
                            element_id: element_id.clone(),
                        },
                    );
                    // The message's variables (if any) are merged into the
                    // correlated instance before its token advances.
                    if !variables.is_empty() {
                        self.emit(
                            &mut log,
                            Event::VariablesUpdated {
                                instance_key,
                                variables: variables.clone(),
                            },
                        );
                    }

                    match kind {
                        // Catch event: completing it resumes the token along its
                        // own outgoing flow.
                        state::MessageSubscriptionKind::IntermediateCatch => {
                            queue.push_back(Step::Complete {
                                instance_key,
                                element_instance_key,
                                element_id,
                            });
                        }
                        // Boundary subscription: interrupt the attached activity
                        // (cancel its job and disarm any sibling boundary timers
                        // and message subscriptions), then run the boundary
                        // event's outgoing flow. `element_instance_key`/
                        // `element_id` are the activity here.
                        state::MessageSubscriptionKind::InterruptingBoundary {
                            boundary_element_id,
                        } => {
                            if let Some(job_key) = self.active_job_on(element_instance_key) {
                                self.emit(
                                    &mut log,
                                    Event::JobCanceled {
                                        job_key,
                                        instance_key,
                                    },
                                );
                            }
                            self.emit(
                                &mut log,
                                Event::ElementCompleting {
                                    instance_key,
                                    element_instance_key,
                                    element_id: element_id.clone(),
                                },
                            );
                            self.emit(
                                &mut log,
                                Event::ElementCompleted {
                                    instance_key,
                                    element_instance_key,
                                    element_id,
                                },
                            );
                            for event in self.cancel_boundary_timers_on(element_instance_key) {
                                self.emit(&mut log, event);
                            }
                            for event in
                                self.cancel_boundary_message_subscriptions_on(element_instance_key)
                            {
                                self.emit(&mut log, event);
                            }
                            queue.push_back(Step::Activate {
                                instance_key,
                                element_id: boundary_element_id,
                            });
                        }
                    }
                }

                // Message start events: a matching message also creates a new
                // instance of every process subscribed at the process level.
                // Deterministic by process-definition key. The message's
                // variables seed the new instance.
                let mut started: Vec<(String, ElementId)> = self
                    .state
                    .message_start_subscriptions
                    .values()
                    .filter(|s| s.message_name == message_name)
                    .map(|s| (s.process_id.clone(), s.start_element_id.clone()))
                    .collect();
                started.sort_unstable();
                for (process_id, start_element_id) in started {
                    self.start_instance(
                        &mut log,
                        &mut queue,
                        process_id,
                        start_element_id,
                        variables.clone(),
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
                    element_id: element_id.clone(),
                    job_type,
                });
                // Arm an interrupting timer for every attached timer boundary
                // event; firing one later interrupts this task.
                for (boundary_id, duration_millis) in
                    self.attached_timer_boundaries(instance_key, &element_id)
                {
                    let timer_key = self.mint_key();
                    events.push(Event::TimerCreated {
                        timer_key,
                        instance_key,
                        element_instance_key,
                        element_id: element_id.clone(),
                        due_at: self.now.saturating_add(duration_millis),
                        kind: state::TimerKind::InterruptingBoundary {
                            boundary_element_id: boundary_id,
                        },
                    });
                }
                // Open a subscription for every attached message boundary event;
                // correlating a matching message later interrupts this task.
                for (boundary_id, message_name, correlation_key) in
                    self.attached_message_boundaries(instance_key, &element_id)
                {
                    let subscription_key = self.mint_key();
                    let correlation_value =
                        self.resolve_correlation_value(instance_key, &correlation_key);
                    events.push(Event::MessageSubscriptionCreated {
                        subscription_key,
                        instance_key,
                        element_instance_key,
                        element_id: element_id.clone(),
                        message_name,
                        correlation_key: correlation_value,
                        kind: state::MessageSubscriptionKind::InterruptingBoundary {
                            boundary_element_id: boundary_id,
                        },
                    });
                }
            }
            // A timer intermediate catch event arms a timer and parks the token;
            // a clock tick (TriggerTimers) releases it once the timer is due.
            Some(ElementKind::TimerIntermediateCatchEvent { duration_millis }) => {
                let timer_key = self.mint_key();
                events.push(Event::TimerCreated {
                    timer_key,
                    instance_key,
                    element_instance_key,
                    element_id,
                    due_at: self.now.saturating_add(duration_millis),
                    kind: state::TimerKind::IntermediateCatch,
                });
            }
            // A message intermediate catch event opens a subscription and parks
            // the token; a matching CorrelateMessage releases it.
            Some(ElementKind::MessageIntermediateCatchEvent {
                message_name,
                correlation_key,
            }) => {
                let subscription_key = self.mint_key();
                let correlation_value =
                    self.resolve_correlation_value(instance_key, &correlation_key);
                events.push(Event::MessageSubscriptionCreated {
                    subscription_key,
                    instance_key,
                    element_instance_key,
                    element_id,
                    message_name,
                    correlation_key: correlation_value,
                    kind: state::MessageSubscriptionKind::IntermediateCatch,
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
        // If this element was an activity guarded by interrupting boundary timers,
        // completing it normally disarms them (and any boundary message subs).
        events.extend(self.cancel_boundary_timers_on(element_instance_key));
        events.extend(self.cancel_boundary_message_subscriptions_on(element_instance_key));
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

    /// The key of a still-in-play job parked on `element_instance_key`, if any.
    /// A job is "in play" until it is completed, errored or cancelled; this is
    /// the job an interrupting boundary event cancels. At most one such job
    /// exists per element instance.
    fn active_job_on(&self, element_instance_key: Key) -> Option<Key> {
        self.state
            .jobs
            .values()
            .find(|j| {
                j.element_instance_key == element_instance_key
                    && !matches!(
                        j.state,
                        state::JobState::Completed
                            | state::JobState::Errored
                            | state::JobState::Canceled
                    )
            })
            .map(|j| j.key)
    }

    /// All interrupting timer boundary events attached to `activity_id`, as
    /// `(boundary_id, duration_millis)` sorted by boundary id (so arming is
    /// deterministic). Empty when the activity has no timer boundaries.
    fn attached_timer_boundaries(
        &self,
        instance_key: Key,
        activity_id: &str,
    ) -> Vec<(ElementId, u64)> {
        let Some(process) = self.process_of_instance(instance_key) else {
            return Vec::new();
        };
        let mut found: Vec<(ElementId, u64)> = process
            .elements
            .values()
            .filter_map(|e| match &e.kind {
                ElementKind::TimerBoundaryEvent {
                    attached_to,
                    duration_millis,
                } if attached_to == activity_id => Some((e.id.clone(), *duration_millis)),
                _ => None,
            })
            .collect();
        found.sort();
        found
    }

    /// Cancels every armed (`Created`) interrupting boundary timer resting on
    /// `element_instance_key`, returning the `TimerCanceled` events. Called when
    /// the guarded activity leaves the flow another way (it completed normally or
    /// a different boundary interrupted it), so a stale timer never fires later.
    fn cancel_boundary_timers_on(&self, element_instance_key: Key) -> Vec<Event> {
        let mut timers: Vec<&state::Timer> = self
            .state
            .timers
            .values()
            .filter(|t| {
                t.element_instance_key == element_instance_key
                    && t.state == state::TimerState::Created
                    && matches!(t.kind, state::TimerKind::InterruptingBoundary { .. })
            })
            .collect();
        timers.sort_by_key(|t| t.key);
        timers
            .into_iter()
            .map(|t| Event::TimerCanceled {
                timer_key: t.key,
                instance_key: t.instance_key,
                element_instance_key: t.element_instance_key,
                element_id: t.element_id.clone(),
            })
            .collect()
    }

    /// All interrupting message boundary events attached to `activity_id`, as
    /// `(boundary_id, message_name, correlation_key)` sorted by boundary id (so
    /// arming is deterministic). Empty when the activity has no message
    /// boundaries.
    fn attached_message_boundaries(
        &self,
        instance_key: Key,
        activity_id: &str,
    ) -> Vec<(ElementId, String, String)> {
        let Some(process) = self.process_of_instance(instance_key) else {
            return Vec::new();
        };
        let mut found: Vec<(ElementId, String, String)> = process
            .elements
            .values()
            .filter_map(|e| match &e.kind {
                ElementKind::MessageBoundaryEvent {
                    attached_to,
                    message_name,
                    correlation_key,
                } if attached_to == activity_id => {
                    Some((e.id.clone(), message_name.clone(), correlation_key.clone()))
                }
                _ => None,
            })
            .collect();
        found.sort();
        found
    }

    /// Cancels every open boundary message subscription resting on
    /// `element_instance_key`, returning the `MessageSubscriptionCanceled`
    /// events. Called when the guarded activity leaves the flow another way (it
    /// completed normally or a different boundary interrupted it), so a stale
    /// subscription never correlates later.
    fn cancel_boundary_message_subscriptions_on(&self, element_instance_key: Key) -> Vec<Event> {
        let mut subs: Vec<&state::MessageSubscription> = self
            .state
            .message_subscriptions
            .values()
            .filter(|s| {
                s.element_instance_key == element_instance_key
                    && s.state == state::MessageSubscriptionState::Open
                    && matches!(
                        s.kind,
                        state::MessageSubscriptionKind::InterruptingBoundary { .. }
                    )
            })
            .collect();
        subs.sort_by_key(|s| s.key);
        subs.into_iter()
            .map(|s| Event::MessageSubscriptionCanceled {
                subscription_key: s.key,
                instance_key: s.instance_key,
                element_instance_key: s.element_instance_key,
                element_id: s.element_id.clone(),
            })
            .collect()
    }

    /// Resolves the correlation value a subscription captures at open time: the
    /// stringified value of the instance variable named `correlation_key`. A
    /// missing variable yields the empty string (matching the REST default
    /// `correlationKey` of `""`).
    fn resolve_correlation_value(&self, instance_key: Key, correlation_key: &str) -> String {
        self.state
            .instances
            .get(&instance_key)
            .and_then(|i| i.variables.get(correlation_key))
            .map(value_to_string)
            .unwrap_or_default()
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

/// Stringifies a [`Value`] for use as a message correlation key. Strings pass
/// through unquoted; numbers and booleans use their natural rendering. This is
/// how an instance variable's value becomes the subscription's correlation key,
/// matched against the REST `correlationKey` string.
fn value_to_string(value: &Value) -> String {
    match value {
        Value::Str(s) => s.clone(),
        Value::Int(i) => i.to_string(),
        Value::Bool(b) => b.to_string(),
    }
}

/// Whether a job can be activated at the logical instant `now`: it is created
/// (never activated, or its lock was released) or its current lock has expired.
/// Failed (incident-parked) and completed jobs are never activatable.
fn job_activatable(job: &state::Job, now: u64) -> bool {
    match job.state {
        state::JobState::Completed
        | state::JobState::Failed
        | state::JobState::Errored
        | state::JobState::Canceled => false,
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
    fn should_park_on_timer_then_fire_when_due() {
        // start -> charge (service task) -> wait (timer PT5S) -> end
        let def = ProcessBuilder::new("delayed")
            .start_event("start")
            .service_task("charge", "payment")
            .timer_intermediate_catch_event("wait", 5_000)
            .end_event("end")
            .connect("start", "charge")
            .connect("charge", "wait")
            .connect("wait", "end")
            .build()
            .unwrap();

        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();

        // create instance at t=1000, then run the job so the token reaches the timer.
        let events = engine
            .apply_command_at(Command::create_instance("delayed"), 1_000)
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

        let job = engine
            .activate_jobs("payment", "w", 1, 60_000, 1_000)
            .into_iter()
            .next()
            .unwrap();
        engine
            .apply_command_at(Command::complete_job(job.key), 1_000)
            .unwrap();

        // Token now parked on the timer, armed for due_at = 1000 + 5000 = 6000.
        assert!(!engine.is_completed(instance_key));
        let timers = engine.timers();
        assert_eq!(timers.len(), 1);
        assert_eq!(timers[0].state, state::TimerState::Created);
        assert_eq!(timers[0].due_at, 6_000);

        // A tick before the due instant fires nothing.
        let fired = engine.trigger_timers(5_999);
        assert!(fired.is_empty());
        assert!(!engine.is_completed(instance_key));

        // A tick at/after the due instant fires the timer and completes the instance.
        let fired = engine.trigger_timers(6_000);
        assert!(fired
            .iter()
            .any(|e| matches!(e, Event::TimerTriggered { .. })));
        assert!(engine.is_completed(instance_key));
        assert!(fired.contains(&Event::ProcessInstanceCompleted { instance_key }));

        // The timer is retained as Triggered so a later tick never re-fires it.
        assert_eq!(engine.timers()[0].state, state::TimerState::Triggered);
        assert!(engine.trigger_timers(10_000).is_empty());
    }

    #[test]
    fn should_recover_parked_timer_via_replay() {
        let def = ProcessBuilder::new("delayed")
            .start_event("start")
            .timer_intermediate_catch_event("wait", 5_000)
            .end_event("end")
            .connect("start", "wait")
            .connect("wait", "end")
            .build()
            .unwrap();

        let mut engine = Engine::new();
        let mut log = Vec::new();
        log.extend(engine.apply_command(Command::DeployProcess(def)).unwrap());
        log.extend(
            engine
                .apply_command_at(Command::create_instance("delayed"), 1_000)
                .unwrap(),
        );

        // Replay the durable log into a fresh engine; the parked timer survives.
        let mut recovered = Engine::replay(log);
        let timers = recovered.timers();
        assert_eq!(timers.len(), 1);
        assert_eq!(timers[0].state, state::TimerState::Created);
        let instance_key = timers[0].instance_key;
        assert!(!recovered.is_completed(instance_key));

        // The recovered engine fires the timer on the next due tick.
        recovered.trigger_timers(6_000);
        assert!(recovered.is_completed(instance_key));
    }

    /// start -> charge (service task, PT5S interrupting timer boundary) -> done
    ///                       \--(timer "timeout")--> escalated
    fn process_with_timer_boundary() -> ProcessDefinition {
        ProcessBuilder::new("ship")
            .start_event("start")
            .service_task("charge", "payment")
            .timer_boundary_event("timeout", "charge", 5_000)
            .end_event("done")
            .end_event("escalated")
            .connect("start", "charge")
            .connect("charge", "done")
            .connect("timeout", "escalated")
            .build()
            .unwrap()
    }

    #[test]
    fn should_fire_an_interrupting_timer_boundary_and_cancel_the_job() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(process_with_timer_boundary()))
            .unwrap();

        // Start at t=1000: the token parks on the service task, a job is created,
        // and the boundary timer is armed for due_at = 6000.
        let events = engine
            .apply_command_at(Command::create_instance("ship"), 1_000)
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
        assert_eq!(engine.pending_jobs().len(), 1);
        let job_key = engine.pending_jobs()[0].key;
        assert_eq!(engine.timers().len(), 1);
        assert_eq!(engine.timers()[0].due_at, 6_000);
        assert!(!engine.is_completed(instance_key));

        // A tick before the due instant does nothing.
        assert!(engine.trigger_timers(5_999).is_empty());
        assert!(!engine.is_completed(instance_key));

        // At the due instant the timer interrupts the task: the job is cancelled,
        // the boundary's outgoing flow runs, and the instance completes.
        let fired = engine.trigger_timers(6_000);
        assert_eq!(engine.state().jobs[&job_key].state, state::JobState::Canceled);
        assert!(fired
            .iter()
            .any(|e| matches!(e, Event::JobCanceled { job_key: k, .. } if *k == job_key)));
        assert!(fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "escalated"
        )));
        assert!(engine.is_completed(instance_key));
        assert_eq!(engine.timers()[0].state, state::TimerState::Triggered);
    }

    #[test]
    fn should_disarm_a_boundary_timer_when_the_job_completes_first() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(process_with_timer_boundary()))
            .unwrap();
        let events = engine
            .apply_command_at(Command::create_instance("ship"), 1_000)
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
        let job_key = engine.pending_jobs()[0].key;

        // Completing the job before the timer fires takes the normal flow and
        // disarms the boundary timer.
        engine.activate_jobs("payment", "w", 1, 60_000, 1_000);
        engine
            .apply_command_at(Command::complete_job(job_key), 2_000)
            .unwrap();
        assert!(engine.is_completed(instance_key));
        assert_eq!(engine.timers()[0].state, state::TimerState::Canceled);

        // A later tick past the (now disarmed) due instant does nothing.
        assert!(engine.trigger_timers(10_000).is_empty());
        assert_eq!(engine.state().jobs[&job_key].state, state::JobState::Completed);
    }

    #[test]
    fn should_recover_an_armed_boundary_timer_via_replay() {
        let mut engine = Engine::new();
        let mut log = Vec::new();
        log.extend(
            engine
                .apply_command(Command::DeployProcess(process_with_timer_boundary()))
                .unwrap(),
        );
        log.extend(
            engine
                .apply_command_at(Command::create_instance("ship"), 1_000)
                .unwrap(),
        );

        // Replay: the armed boundary timer and its parked job survive.
        let mut recovered = Engine::replay(log);
        assert_eq!(recovered.timers().len(), 1);
        assert_eq!(recovered.timers()[0].state, state::TimerState::Created);
        let instance_key = recovered.timers()[0].instance_key;
        assert!(!recovered.is_completed(instance_key));

        // The recovered engine fires the boundary on the next due tick.
        recovered.trigger_timers(6_000);
        assert!(recovered.is_completed(instance_key));
        let job = recovered.state().jobs.values().next().unwrap();
        assert_eq!(job.state, state::JobState::Canceled);
    }

    /// start -> await (message catch "payment-received", correlationKey orderId)
    ///       -> end
    fn process_with_message_catch() -> ProcessDefinition {
        ProcessBuilder::new("await-payment")
            .start_event("start")
            .message_intermediate_catch_event("await", "payment-received", "orderId")
            .end_event("end")
            .connect("start", "await")
            .connect("await", "end")
            .build()
            .unwrap()
    }

    fn vars(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn should_park_on_message_catch_then_correlate() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(process_with_message_catch()))
            .unwrap();

        // Create an instance whose orderId resolves the correlation value "A".
        let events = engine
            .apply_command(Command::create_instance_with(
                "await-payment",
                vars(&[("orderId", Value::Str("A".into()))]),
            ))
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

        // The token parks on the catch event, opening one open subscription.
        assert!(!engine.is_completed(instance_key));
        let subs = engine.message_subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].state, state::MessageSubscriptionState::Open);
        assert_eq!(subs[0].message_name, "payment-received");
        assert_eq!(subs[0].correlation_key, "A");

        // A non-matching correlation key correlates nothing.
        let fired = engine.correlate_message("payment-received", "B", HashMap::new(), 0);
        assert!(!fired
            .iter()
            .any(|e| matches!(e, Event::MessageCorrelated { .. })));
        assert!(!engine.is_completed(instance_key));

        // The matching message releases the token and completes the instance.
        let fired = engine.correlate_message("payment-received", "A", HashMap::new(), 0);
        assert!(fired
            .iter()
            .any(|e| matches!(e, Event::MessageCorrelated { .. })));
        assert!(engine.is_completed(instance_key));
        assert_eq!(
            engine.message_subscriptions()[0].state,
            state::MessageSubscriptionState::Correlated
        );

        // A repeat message never correlates the now-settled subscription twice.
        let fired = engine.correlate_message("payment-received", "A", HashMap::new(), 0);
        assert!(!fired
            .iter()
            .any(|e| matches!(e, Event::MessageCorrelated { .. })));
    }

    #[test]
    fn should_publish_a_message_with_no_subscription() {
        let mut engine = Engine::new();
        // With nothing subscribed, a published message is minted and dropped.
        let fired = engine.correlate_message("nobody-home", "X", HashMap::new(), 0);
        assert_eq!(fired.len(), 1);
        assert!(matches!(fired[0], Event::MessagePublished { .. }));
        assert!(engine.message_subscriptions().is_empty());
    }

    #[test]
    fn should_merge_message_variables_on_correlation() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(process_with_message_catch()))
            .unwrap();
        let events = engine
            .apply_command(Command::create_instance_with(
                "await-payment",
                vars(&[("orderId", Value::Str("A".into()))]),
            ))
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

        engine.correlate_message(
            "payment-received",
            "A",
            vars(&[("amount", Value::Int(42))]),
            0,
        );

        // The message payload is merged into the correlated instance's variables.
        assert_eq!(
            engine.state().instances[&instance_key]
                .variables
                .get("amount"),
            Some(&Value::Int(42))
        );
    }

    #[test]
    fn should_correlate_only_the_instance_with_the_matching_key() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(process_with_message_catch()))
            .unwrap();
        let a = engine
            .apply_command(Command::create_instance_with(
                "await-payment",
                vars(&[("orderId", Value::Str("A".into()))]),
            ))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap();
        let b = engine
            .apply_command(Command::create_instance_with(
                "await-payment",
                vars(&[("orderId", Value::Str("B".into()))]),
            ))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap();

        // Correlating "A" releases only instance A; B stays parked.
        engine.correlate_message("payment-received", "A", HashMap::new(), 0);
        assert!(engine.is_completed(a));
        assert!(!engine.is_completed(b));
    }

    #[test]
    fn should_recover_an_open_message_subscription_via_replay() {
        let mut engine = Engine::new();
        let mut log = Vec::new();
        log.extend(
            engine
                .apply_command(Command::DeployProcess(process_with_message_catch()))
                .unwrap(),
        );
        log.extend(
            engine
                .apply_command(Command::create_instance_with(
                    "await-payment",
                    vars(&[("orderId", Value::Str("A".into()))]),
                ))
                .unwrap(),
        );

        // Replay: the open subscription and its parked token survive.
        let mut recovered = Engine::replay(log);
        let subs = recovered.message_subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].state, state::MessageSubscriptionState::Open);
        let instance_key = subs[0].instance_key;
        assert!(!recovered.is_completed(instance_key));

        // The recovered engine correlates the message and completes the instance.
        recovered.correlate_message("payment-received", "A", HashMap::new(), 0);
        assert!(recovered.is_completed(instance_key));
    }

    /// start -> charge (service task, interrupting message boundary "cancel"
    ///          correlating on orderId) -> done
    ///                       \--(message)--> aborted
    fn process_with_message_boundary() -> ProcessDefinition {
        ProcessBuilder::new("cancellable")
            .start_event("start")
            .service_task("charge", "payment")
            .message_boundary_event("cancel", "charge", "order-cancelled", "orderId")
            .end_event("done")
            .end_event("aborted")
            .connect("start", "charge")
            .connect("charge", "done")
            .connect("cancel", "aborted")
            .build()
            .unwrap()
    }

    #[test]
    fn should_fire_an_interrupting_message_boundary_and_cancel_the_job() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(process_with_message_boundary()))
            .unwrap();
        let events = engine
            .apply_command(Command::create_instance_with(
                "cancellable",
                vars(&[("orderId", Value::Str("A".into()))]),
            ))
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

        // The token parks on the service task; a job and a boundary subscription
        // are created.
        assert_eq!(engine.pending_jobs().len(), 1);
        let job_key = engine.pending_jobs()[0].key;
        assert_eq!(engine.message_subscriptions().len(), 1);

        // Correlating the boundary message interrupts the task: the job is
        // cancelled, the boundary's outgoing flow runs, the instance completes.
        let fired = engine.correlate_message("order-cancelled", "A", HashMap::new(), 0);
        assert_eq!(
            engine.state().jobs[&job_key].state,
            state::JobState::Canceled
        );
        assert!(fired
            .iter()
            .any(|e| matches!(e, Event::JobCanceled { job_key: k, .. } if *k == job_key)));
        assert!(fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "cancel" && to == "aborted"
        )));
        assert!(engine.is_completed(instance_key));
        assert_eq!(
            engine.message_subscriptions()[0].state,
            state::MessageSubscriptionState::Correlated
        );
    }

    #[test]
    fn should_cancel_a_message_boundary_subscription_when_the_job_completes_first() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(process_with_message_boundary()))
            .unwrap();
        let events = engine
            .apply_command(Command::create_instance_with(
                "cancellable",
                vars(&[("orderId", Value::Str("A".into()))]),
            ))
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
        let job_key = engine.pending_jobs()[0].key;

        // Completing the job before any message arrives takes the normal flow and
        // cancels the boundary subscription.
        engine.activate_jobs("payment", "w", 1, 60_000, 0);
        engine
            .apply_command(Command::complete_job(job_key))
            .unwrap();
        assert!(engine.is_completed(instance_key));
        assert_eq!(
            engine.message_subscriptions()[0].state,
            state::MessageSubscriptionState::Canceled
        );

        // A later message for the (now cancelled) subscription correlates nothing.
        let fired = engine.correlate_message("order-cancelled", "A", HashMap::new(), 0);
        assert!(!fired
            .iter()
            .any(|e| matches!(e, Event::MessageCorrelated { .. })));
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

    // ---- message start events ----

    /// (message "order-placed") --> start -> end
    fn process_with_message_start() -> ProcessDefinition {
        ProcessBuilder::new("order-flow")
            .message_start_event("start", "order-placed")
            .end_event("end")
            .connect("start", "end")
            .build()
            .unwrap()
    }

    #[test]
    fn should_open_a_message_start_subscription_at_deploy() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(process_with_message_start()))
            .unwrap();

        // Deploy opens a process-level subscription but creates no instance.
        assert_eq!(engine.state().message_start_subscriptions.len(), 1);
        assert!(engine.state().instances.is_empty());
        let sub = &engine.state().message_start_subscriptions["order-placed"];
        assert_eq!(sub.process_id, "order-flow");
        assert_eq!(sub.start_element_id, "start");
    }

    #[test]
    fn should_create_an_instance_when_a_message_start_correlates() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(process_with_message_start()))
            .unwrap();

        // A non-matching message creates nothing.
        engine.correlate_message("other", "", HashMap::new(), 0);
        assert!(engine.state().instances.is_empty());

        // The matching message creates and runs a fresh instance to completion,
        // seeding it with the message's variables.
        let fired =
            engine.correlate_message("order-placed", "", vars(&[("amount", Value::Int(7))]), 0);
        let instance_key = fired
            .iter()
            .find_map(|e| match e {
                Event::ProcessInstanceCreated { instance_key, .. } => Some(*instance_key),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            engine.state().instances[&instance_key]
                .variables
                .get("amount"),
            Some(&Value::Int(7))
        );
        assert!(engine.is_completed(instance_key));
    }

    #[test]
    fn should_create_one_instance_per_matching_message_start() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(process_with_message_start()))
            .unwrap();

        // Each matching message creates a distinct instance.
        engine.correlate_message("order-placed", "", HashMap::new(), 0);
        engine.correlate_message("order-placed", "", HashMap::new(), 0);
        assert_eq!(engine.state().instances.len(), 2);
    }

    #[test]
    fn should_recover_a_message_start_subscription_via_replay() {
        let mut engine = Engine::new();
        let log = engine
            .apply_command(Command::DeployProcess(process_with_message_start()))
            .unwrap();

        // Replay: the process-level subscription survives and still fires.
        let mut recovered = Engine::replay(log);
        assert_eq!(recovered.state().message_start_subscriptions.len(), 1);
        let fired = recovered.correlate_message("order-placed", "", HashMap::new(), 0);
        let instance_key = fired.iter().find_map(|e| e.instance_key()).unwrap();
        assert!(recovered.is_completed(instance_key));
    }

    // ---- timer start events ----

    /// (timer, one-shot PT10S) --> start -> end
    fn process_with_timer_start_once() -> ProcessDefinition {
        ProcessBuilder::new("delayed-start")
            .timer_start_event_once("start", 10_000)
            .end_event("end")
            .connect("start", "end")
            .build()
            .unwrap()
    }

    /// (timer, cycle every 10S) --> start -> end
    fn process_with_timer_start_cycle() -> ProcessDefinition {
        ProcessBuilder::new("recurring-start")
            .timer_start_event_cycle("start", 10_000)
            .end_event("end")
            .connect("start", "end")
            .build()
            .unwrap()
    }

    #[test]
    fn should_arm_a_start_timer_at_deploy() {
        let mut engine = Engine::new();
        // Deploy at t=1000: the start timer is armed for 1000 + 10000 = 11000.
        engine
            .apply_command_at(
                Command::DeployProcess(process_with_timer_start_once()),
                1_000,
            )
            .unwrap();
        assert_eq!(engine.state().start_timers.len(), 1);
        let timer = engine.state().start_timers.values().next().unwrap();
        assert_eq!(timer.due_at, Some(11_000));
        assert!(engine.state().instances.is_empty());
    }

    #[test]
    fn should_fire_a_one_shot_start_timer_exactly_once() {
        let mut engine = Engine::new();
        engine
            .apply_command_at(
                Command::DeployProcess(process_with_timer_start_once()),
                1_000,
            )
            .unwrap();

        // A tick before the due instant creates nothing.
        assert!(engine.trigger_timers(10_999).is_empty());
        assert!(engine.state().instances.is_empty());

        // At the due instant the timer fires and creates one instance; the timer
        // is retained but has no due time, so it never fires again.
        let fired = engine.trigger_timers(11_000);
        let instance_key = fired.iter().find_map(|e| e.instance_key()).unwrap();
        assert!(engine.is_completed(instance_key));
        assert_eq!(engine.state().instances.len(), 1);
        let timer = engine.state().start_timers.values().next().unwrap();
        assert_eq!(timer.due_at, None);

        // A later tick fires nothing more.
        assert!(engine.trigger_timers(100_000).is_empty());
        assert_eq!(engine.state().instances.len(), 1);
    }

    #[test]
    fn should_re_arm_a_cycle_start_timer_after_each_fire() {
        let mut engine = Engine::new();
        engine
            .apply_command_at(
                Command::DeployProcess(process_with_timer_start_cycle()),
                1_000,
            )
            .unwrap();

        // First fire at 11000 creates an instance and re-arms for 21000.
        engine.trigger_timers(11_000);
        assert_eq!(engine.state().instances.len(), 1);
        let timer = engine.state().start_timers.values().next().unwrap();
        assert_eq!(timer.due_at, Some(21_000));

        // Second fire at 21000 creates another and re-arms for 31000.
        engine.trigger_timers(21_000);
        assert_eq!(engine.state().instances.len(), 2);
        let timer = engine.state().start_timers.values().next().unwrap();
        assert_eq!(timer.due_at, Some(31_000));
    }

    #[test]
    fn should_recover_an_armed_start_timer_via_replay() {
        let mut engine = Engine::new();
        let log = engine
            .apply_command_at(
                Command::DeployProcess(process_with_timer_start_once()),
                1_000,
            )
            .unwrap();

        // Replay: the armed start timer survives and still fires on the next tick.
        let mut recovered = Engine::replay(log);
        assert_eq!(recovered.state().start_timers.len(), 1);
        assert_eq!(
            recovered
                .state()
                .start_timers
                .values()
                .next()
                .unwrap()
                .due_at,
            Some(11_000)
        );
        let fired = recovered.trigger_timers(11_000);
        let instance_key = fired.iter().find_map(|e| e.instance_key()).unwrap();
        assert!(recovered.is_completed(instance_key));
    }
}
