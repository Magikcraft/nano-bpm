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
        /// The enclosing sub-process element instance this activation runs in,
        /// or `0` for the process-level (root) scope.
        scope: Key,
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

    /// Evicts a *completed* process instance and every entity it owns (jobs,
    /// timers, message subscriptions, incidents) from hot state, returning
    /// `true` if it was evicted. Process-level message-start subscriptions and
    /// timer-start events, and deployed definitions, are retained (they are not
    /// instance-scoped). No-op for an unknown or still-active instance.
    ///
    /// The engine keeps completed instances by default (so `is_completed`,
    /// `instance`, and the audit trail keep working). A host that has durably
    /// projected the instance's history into a separate read model can call
    /// this to keep hot state bounded to only in-flight work — the engine then
    /// never needs a completed instance again, because no command can target
    /// one (its jobs are settled, its timers fired, its subscriptions closed).
    pub fn evict_instance(&mut self, key: Key) -> bool {
        let terminal = matches!(
            self.state.instances.get(&key).map(|i| i.state),
            Some(ProcessInstanceState::Completed | ProcessInstanceState::Terminated)
        );
        if !terminal {
            return false;
        }
        self.state.instances.remove(&key);
        self.state.jobs.retain(|_, j| j.instance_key != key);
        self.state.timers.retain(|_, t| t.instance_key != key);
        self.state
            .message_subscriptions
            .retain(|_, s| s.instance_key != key);
        self.state.incidents.retain(|_, i| i.instance_key != key);
        true
    }

    /// Evicts every completed instance (see [`Engine::evict_instance`]) and
    /// shrinks the backing maps so freed capacity is returned. Returns the
    /// number of instances evicted. Intended to run once after a boot replay,
    /// when the read model is already caught up, so recovered hot state holds
    /// only in-flight instances rather than the whole history.
    pub fn evict_completed(&mut self) -> usize {
        let done: Vec<Key> = self
            .state
            .instances
            .iter()
            .filter(|(_, i)| {
                matches!(
                    i.state,
                    ProcessInstanceState::Completed | ProcessInstanceState::Terminated
                )
            })
            .map(|(k, _)| *k)
            .collect();
        for key in &done {
            self.evict_instance(*key);
        }
        if !done.is_empty() {
            self.shrink();
        }
        done.len()
    }

    /// Shrinks the capacity of the hot-state maps to fit their live contents,
    /// returning memory freed by eviction back to the allocator. (Rust maps
    /// never shrink on their own, so removal alone does not lower the resident
    /// footprint until this is called.)
    pub fn shrink(&mut self) {
        self.state.instances.shrink_to_fit();
        self.state.jobs.shrink_to_fit();
        self.state.timers.shrink_to_fit();
        self.state.message_subscriptions.shrink_to_fit();
        self.state.incidents.shrink_to_fit();
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
            scope: 0,
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

            Command::AssignUserTask {
                user_task_key,
                assignee,
                allow_override,
            } => {
                let task = self
                    .state
                    .user_tasks
                    .get(&user_task_key)
                    .ok_or(EngineError::UserTaskNotFound { user_task_key })?;
                if task.state != state::UserTaskState::Created {
                    return Err(EngineError::UserTaskNotActive { user_task_key });
                }
                // Mirror Camunda: when override is disallowed and the task is
                // already assigned, reject so it must be unassigned first.
                if !allow_override && task.assignee.is_some() {
                    return Err(EngineError::UserTaskAlreadyAssigned { user_task_key });
                }
                let instance_key = task.instance_key;
                self.emit(
                    &mut log,
                    Event::UserTaskAssigned {
                        user_task_key,
                        instance_key,
                        assignee: Some(assignee),
                    },
                );
            }

            Command::UnassignUserTask { user_task_key } => {
                let task = self
                    .state
                    .user_tasks
                    .get(&user_task_key)
                    .ok_or(EngineError::UserTaskNotFound { user_task_key })?;
                if task.state != state::UserTaskState::Created {
                    return Err(EngineError::UserTaskNotActive { user_task_key });
                }
                let instance_key = task.instance_key;
                self.emit(
                    &mut log,
                    Event::UserTaskAssigned {
                        user_task_key,
                        instance_key,
                        assignee: None,
                    },
                );
            }

            Command::UpdateUserTask {
                user_task_key,
                changeset,
            } => {
                let task = self
                    .state
                    .user_tasks
                    .get(&user_task_key)
                    .ok_or(EngineError::UserTaskNotFound { user_task_key })?;
                if task.state != state::UserTaskState::Created {
                    return Err(EngineError::UserTaskNotActive { user_task_key });
                }
                let instance_key = task.instance_key;
                // Normalise empty-string dates to "reset" (None), matching the
                // REST contract ("Reset by providing an empty String").
                let normalize = |d: Option<String>| -> Option<String> {
                    d.filter(|s| !s.is_empty())
                };
                self.emit(
                    &mut log,
                    Event::UserTaskUpdated {
                        user_task_key,
                        instance_key,
                        candidate_groups: changeset.candidate_groups,
                        candidate_users: changeset.candidate_users,
                        due_date: changeset.due_date.map(normalize),
                        follow_up_date: changeset.follow_up_date.map(normalize),
                        priority: changeset.priority,
                    },
                );
            }

            Command::CompleteUserTask {
                user_task_key,
                variables,
            } => {
                let task = self
                    .state
                    .user_tasks
                    .get(&user_task_key)
                    .ok_or(EngineError::UserTaskNotFound { user_task_key })?;
                if task.state != state::UserTaskState::Created {
                    return Err(EngineError::UserTaskNotActive { user_task_key });
                }
                let instance_key = task.instance_key;
                let element_instance_key = task.element_instance_key;
                let element_id = task.element_id.clone();

                self.emit(
                    &mut log,
                    Event::UserTaskCompleted {
                        user_task_key,
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
                // The parked user-task token resumes from ACTIVATED.
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
                    let due_at = timer.due_at;
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
                        // Boundary timer: interrupt the attached activity (a
                        // service task or sub-process) and run the boundary
                        // event's outgoing flow. `element_instance_key`/
                        // `element_id` are the activity here.
                        state::TimerKind::InterruptingBoundary {
                            boundary_element_id,
                        } => {
                            let scope = self.scope_of(instance_key, element_instance_key);
                            self.interrupt_activity_via_boundary(
                                &mut log,
                                instance_key,
                                element_instance_key,
                                &element_id,
                            );
                            queue.push_back(Step::Activate {
                                instance_key,
                                element_id: boundary_element_id,
                                scope,
                            });
                        }
                        // Non-interrupting boundary timer: leave the activity (and
                        // its job) running and spawn a parallel token along the
                        // boundary event's outgoing flow, in the activity's scope.
                        // A repeating (cycle) timer re-arms for the next interval.
                        state::TimerKind::NonInterruptingBoundary {
                            boundary_element_id,
                        } => {
                            queue.push_back(Step::Activate {
                                instance_key,
                                element_id: boundary_element_id.clone(),
                                scope: self.scope_of(instance_key, element_instance_key),
                            });
                            if let Some(ElementKind::TimerBoundaryEvent {
                                duration_millis,
                                repeating: true,
                                ..
                            }) = self.element_kind(instance_key, &boundary_element_id)
                            {
                                let next_timer_key = self.mint_key();
                                self.emit(
                                    &mut log,
                                    Event::TimerCreated {
                                        timer_key: next_timer_key,
                                        instance_key,
                                        element_instance_key,
                                        element_id: element_id.clone(),
                                        due_at: due_at.saturating_add(duration_millis),
                                        kind: state::TimerKind::NonInterruptingBoundary {
                                            boundary_element_id,
                                        },
                                    },
                                );
                            }
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

                match self.find_catching_error_boundary(
                    instance_key,
                    &task_element_id,
                    element_instance_key,
                    &error_code,
                ) {
                    // Caught: interrupt the catching activity (the throwing task
                    // itself, or an enclosing sub-process) and run the boundary
                    // event's error-handling path.
                    Some((boundary_id, caught_eik, caught_element_id)) => {
                        // Capture the catching activity's scope before completing
                        // it (completion clears its scope entry); the boundary
                        // event runs in that same scope.
                        let boundary_scope = self.scope_of(instance_key, caught_eik);
                        // When caught at an enclosing sub-process, terminate its
                        // whole inner scope (including the throwing task) first.
                        if caught_eik != element_instance_key {
                            self.terminate_subprocess_scope(&mut log, instance_key, caught_eik);
                        }
                        self.emit(
                            &mut log,
                            Event::ElementCompleting {
                                instance_key,
                                element_instance_key: caught_eik,
                                element_id: caught_element_id.clone(),
                            },
                        );
                        self.emit(
                            &mut log,
                            Event::ElementCompleted {
                                instance_key,
                                element_instance_key: caught_eik,
                                element_id: caught_element_id,
                            },
                        );
                        // An error boundary interrupting the activity also disarms
                        // any timer boundaries and message subscriptions on it.
                        for event in self.cancel_boundary_timers_on(caught_eik) {
                            self.emit(&mut log, event);
                        }
                        for event in self.cancel_boundary_message_subscriptions_on(caught_eik) {
                            self.emit(&mut log, event);
                        }
                        queue.push_back(Step::Activate {
                            instance_key,
                            element_id: boundary_id,
                            scope: boundary_scope,
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
                    // Exclusive gateway matched no flow, or a condition failed to
                    // evaluate: re-evaluate the gateway against the (possibly
                    // updated) variables.
                    state::IncidentKind::NoMatchingSequenceFlow
                    | state::IncidentKind::ExpressionEvaluation => {
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
                        // (a service task or sub-process), then run the boundary
                        // event's outgoing flow. `element_instance_key`/
                        // `element_id` are the activity here.
                        state::MessageSubscriptionKind::InterruptingBoundary {
                            boundary_element_id,
                        } => {
                            let scope = self.scope_of(instance_key, element_instance_key);
                            self.interrupt_activity_via_boundary(
                                &mut log,
                                instance_key,
                                element_instance_key,
                                &element_id,
                            );
                            queue.push_back(Step::Activate {
                                instance_key,
                                element_id: boundary_element_id,
                                scope,
                            });
                        }
                        // Non-interrupting boundary subscription: leave the
                        // activity (and its job) running and spawn a parallel
                        // token along the boundary event's outgoing flow, in the
                        // activity's scope. The subscription stays open (its
                        // applier does not settle it), so the next matching
                        // message spawns another token.
                        state::MessageSubscriptionKind::NonInterruptingBoundary {
                            boundary_element_id,
                        } => {
                            queue.push_back(Step::Activate {
                                instance_key,
                                element_id: boundary_element_id,
                                scope: self.scope_of(instance_key, element_instance_key),
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

            Command::CancelInstance { instance_key } => {
                // Only an active instance can be cancelled. An unknown key, or one
                // that has already completed/terminated, is rejected so the caller
                // gets a clean 404.
                match self.state.instances.get(&instance_key) {
                    Some(instance) if instance.state == ProcessInstanceState::Active => {}
                    _ => return Err(EngineError::InstanceNotFound { instance_key }),
                }

                // Discard every token: cancel the instance's in-play jobs, armed
                // timers and open message subscriptions. Collected and ordered by
                // key first so the event sequence is deterministic.
                let mut jobs: Vec<&state::Job> = self
                    .state
                    .jobs
                    .values()
                    .filter(|j| {
                        j.instance_key == instance_key
                            && matches!(
                                j.state,
                                state::JobState::Created
                                    | state::JobState::Activated
                                    | state::JobState::Failed
                            )
                    })
                    .collect();
                jobs.sort_unstable_by_key(|j| j.key);
                let job_cancels: Vec<Event> = jobs
                    .iter()
                    .map(|j| Event::JobCanceled {
                        job_key: j.key,
                        instance_key,
                    })
                    .collect();

                let mut timers: Vec<&state::Timer> = self
                    .state
                    .timers
                    .values()
                    .filter(|t| {
                        t.instance_key == instance_key && t.state == state::TimerState::Created
                    })
                    .collect();
                timers.sort_unstable_by_key(|t| t.key);
                let timer_cancels: Vec<Event> = timers
                    .iter()
                    .map(|t| Event::TimerCanceled {
                        timer_key: t.key,
                        instance_key,
                        element_instance_key: t.element_instance_key,
                        element_id: t.element_id.clone(),
                    })
                    .collect();

                let mut subs: Vec<&state::MessageSubscription> = self
                    .state
                    .message_subscriptions
                    .values()
                    .filter(|s| {
                        s.instance_key == instance_key
                            && s.state == state::MessageSubscriptionState::Open
                    })
                    .collect();
                subs.sort_unstable_by_key(|s| s.key);
                let sub_cancels: Vec<Event> = subs
                    .iter()
                    .map(|s| Event::MessageSubscriptionCanceled {
                        subscription_key: s.key,
                        instance_key,
                        element_instance_key: s.element_instance_key,
                        element_id: s.element_id.clone(),
                    })
                    .collect();

                let mut user_tasks: Vec<&state::UserTask> = self
                    .state
                    .user_tasks
                    .values()
                    .filter(|t| {
                        t.instance_key == instance_key
                            && t.state == state::UserTaskState::Created
                    })
                    .collect();
                user_tasks.sort_unstable_by_key(|t| t.key);
                let user_task_cancels: Vec<Event> = user_tasks
                    .iter()
                    .map(|t| Event::UserTaskCanceled {
                        user_task_key: t.key,
                        instance_key,
                    })
                    .collect();

                for event in job_cancels {
                    self.emit(&mut log, event);
                }
                for event in timer_cancels {
                    self.emit(&mut log, event);
                }
                for event in sub_cancels {
                    self.emit(&mut log, event);
                }
                for event in user_task_cancels {
                    self.emit(&mut log, event);
                }
                self.emit(&mut log, Event::ProcessInstanceTerminated { instance_key });
            }
        }

        self.run(&mut log, queue);
        self.complete_finished_instances(&mut log);
        Ok(log)
    }

    /// Drains the work queue, applying events and enqueuing follow-up steps until
    /// the instance is quiescent.
    fn run(&mut self, log: &mut Vec<Event>, mut queue: VecDeque<Step>) {
        loop {
            while let Some(step) = queue.pop_front() {
                let (events, followups) = self.process_step(step);
                for event in events {
                    self.emit(log, event);
                }
                for f in followups {
                    queue.push_back(f);
                }
            }
            // The queue is drained. Any embedded sub-process whose inner scope has
            // emptied completes now and routes along its outgoing flow; that may
            // enqueue more work (and, in turn, drain an enclosing sub-process), so
            // loop until nothing more completes.
            let followups = self.complete_drained_subprocesses(log);
            if followups.is_empty() {
                break;
            }
            queue.extend(followups);
        }
    }

    /// Completes every active sub-process element instance whose inner token
    /// scope has drained (no remaining child element instances), emitting its
    /// completion events and returning the follow-up activations for its outgoing
    /// flows. Deterministic in `(instance_key, element_instance_key)` order.
    fn complete_drained_subprocesses(&mut self, log: &mut Vec<Event>) -> Vec<Step> {
        let mut drained: Vec<(Key, Key, ElementId)> = Vec::new();
        for instance in self.state.instances.values() {
            if instance.state != ProcessInstanceState::Active {
                continue;
            }
            let Some(process) = self.state.processes.get(&instance.process_id) else {
                continue;
            };
            for (eik, element_id) in &instance.active {
                let is_subprocess = matches!(
                    process.definition.element(element_id).map(|e| &e.kind),
                    Some(ElementKind::SubProcess { .. })
                );
                if !is_subprocess {
                    continue;
                }
                let has_child = instance.scopes.values().any(|parent| parent == eik);
                if !has_child {
                    drained.push((instance.key, *eik, element_id.clone()));
                }
            }
        }
        drained.sort();

        let mut followups = Vec::new();
        for (instance_key, eik, element_id) in drained {
            // The sub-process completes in its own (parent) scope, captured before
            // its scope entry is cleared by `ElementCompleted`.
            let scope = self.scope_of(instance_key, eik);
            self.emit(
                log,
                Event::ElementCompleting {
                    instance_key,
                    element_instance_key: eik,
                    element_id: element_id.clone(),
                },
            );
            self.emit(
                log,
                Event::ElementCompleted {
                    instance_key,
                    element_instance_key: eik,
                    element_id: element_id.clone(),
                },
            );
            // Completing normally disarms any boundary timers/subscriptions on it.
            for event in self.cancel_boundary_timers_on(eik) {
                self.emit(log, event);
            }
            for event in self.cancel_boundary_message_subscriptions_on(eik) {
                self.emit(log, event);
            }
            for flow in self.outgoing(instance_key, &element_id) {
                self.emit(
                    log,
                    Event::SequenceFlowTaken {
                        instance_key,
                        from: element_id.clone(),
                        to: flow.to.clone(),
                    },
                );
                followups.push(Step::Activate {
                    instance_key,
                    element_id: flow.to,
                    scope,
                });
            }
        }
        followups
    }

    /// The processor: decides the events and follow-up work for one lifecycle
    /// step. Reads state, mints keys, but never mutates [`State`].
    fn process_step(&mut self, step: Step) -> (Vec<Event>, Vec<Step>) {
        match step {
            Step::Activate {
                instance_key,
                element_id,
                scope,
            } => self.activate(instance_key, element_id, scope),
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

    fn activate(
        &mut self,
        instance_key: Key,
        element_id: String,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let kind = self.element_kind(instance_key, &element_id);

        // A parallel gateway with more than one incoming flow is a join: it
        // synchronises tokens instead of activating per arrival.
        if matches!(kind, Some(ElementKind::ParallelGateway))
            && self.incoming_count(instance_key, &element_id) > 1
        {
            return self.arrive_at_parallel_join(instance_key, element_id, scope);
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
                scope,
            },
        ];
        let mut followups = Vec::new();

        match kind {
            // A service task creates a job and parks the token.
            Some(ElementKind::ServiceTask { job_type }) => {
                let job_key = self.mint_key();
                let job_type = self.resolve_job_type(instance_key, &job_type);
                events.push(Event::JobCreated {
                    job_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                    job_type,
                });
                // Arm timers/subscriptions for every attached boundary event.
                events.extend(self.arm_boundary_events(
                    instance_key,
                    element_instance_key,
                    &element_id,
                ));
            }
            // A user task creates a user task record and parks the token; a
            // CompleteUserTask releases it. The assignment/scheduling/priority
            // expressions declared on the element are resolved against the
            // instance variables at creation time.
            Some(ElementKind::UserTask(props)) => {
                let user_task_key = self.mint_key();
                let assignee = self
                    .resolve_user_task_string(instance_key, props.assignee.as_deref())
                    .filter(|s| !s.is_empty());
                let candidate_groups =
                    self.resolve_user_task_list(instance_key, props.candidate_groups.as_deref());
                let candidate_users =
                    self.resolve_user_task_list(instance_key, props.candidate_users.as_deref());
                let due_date = self
                    .resolve_user_task_string(instance_key, props.due_date.as_deref())
                    .filter(|s| !s.is_empty());
                let follow_up_date = self
                    .resolve_user_task_string(instance_key, props.follow_up_date.as_deref())
                    .filter(|s| !s.is_empty());
                let priority = self.resolve_user_task_priority(instance_key, props.priority.as_deref());
                events.push(Event::UserTaskCreated {
                    user_task_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                    created_at: self.now,
                    assignee,
                    candidate_groups,
                    candidate_users,
                    due_date,
                    follow_up_date,
                    priority,
                });
                events.extend(self.arm_boundary_events(
                    instance_key,
                    element_instance_key,
                    &element_id,
                ));
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
            // An embedded sub-process opens a token scope: it activates its inner
            // start event inside its own scope (this element instance) and rests
            // while the inner flow runs. It completes once the scope drains (see
            // `complete_drained_subprocesses`) or is interrupted by an error
            // boundary.
            Some(ElementKind::SubProcess { start_event }) => {
                // A sub-process can also carry timer/message boundary events;
                // arm them just like a service task's.
                events.extend(self.arm_boundary_events(
                    instance_key,
                    element_instance_key,
                    &element_id,
                ));
                followups.push(Step::Activate {
                    instance_key,
                    element_id: start_event,
                    scope: element_instance_key,
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
        let scope = self.scope_of(instance_key, element_instance_key);
        for flow in self.outgoing(instance_key, &element_id) {
            events.push(Event::SequenceFlowTaken {
                instance_key,
                from: element_id.clone(),
                to: flow.to.clone(),
            });
            followups.push(Step::Activate {
                instance_key,
                element_id: flow.to,
                scope,
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
                let job_type = self.resolve_job_type(instance_key, &job_type);
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
        let mut selected = None;
        let mut eval_error: Option<String> = None;
        for flow in self.outgoing(instance_key, &element_id) {
            match &flow.condition {
                None => {
                    selected = Some(flow);
                    break;
                }
                Some(condition) => match condition.eval(&variables) {
                    Ok(true) => {
                        selected = Some(flow);
                        break;
                    }
                    Ok(false) => continue,
                    Err(err) => {
                        eval_error = Some(format!(
                            "failed to evaluate condition '{}' at exclusive gateway \
                             '{element_id}': {}",
                            condition.expression, err.0
                        ));
                        break;
                    }
                },
            }
        }

        if let Some(reason) = eval_error {
            let incident_key = self.mint_key();
            let events = vec![Event::IncidentRaised {
                incident_key,
                instance_key,
                element_instance_key,
                element_id,
                kind: state::IncidentKind::ExpressionEvaluation,
                reason,
                job_key: None,
                created_at: self.now,
            }];
            return (events, Vec::new());
        }

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
                    scope: self.scope_of(instance_key, element_instance_key),
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
        scope: Key,
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
                scope,
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
                    scope,
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

    /// The element instance of the embedded sub-process that encloses
    /// `element_instance_key`, or `0` if it lives in the process-level scope.
    fn scope_of(&self, instance_key: Key, element_instance_key: Key) -> Key {
        self.state
            .instances
            .get(&instance_key)
            .and_then(|i| i.scopes.get(&element_instance_key).copied())
            .unwrap_or(0)
    }

    /// The element id of an active element instance, if it is still active.
    fn element_id_of_instance(&self, instance_key: Key, element_instance_key: Key) -> Option<ElementId> {
        self.state
            .instances
            .get(&instance_key)?
            .active
            .get(&element_instance_key)
            .cloned()
    }

    /// Finds the error boundary that catches `error_code` thrown from the
    /// activity `from_element_id` (element instance `from_eik`), propagating up
    /// enclosing sub-process scopes until one is found. Returns
    /// `(boundary_id, caught_element_instance_key, caught_element_id)` — the
    /// boundary event and the activity it is attached to (the throwing task
    /// itself or an enclosing sub-process).
    fn find_catching_error_boundary(
        &self,
        instance_key: Key,
        from_element_id: &str,
        from_eik: Key,
        error_code: &str,
    ) -> Option<(ElementId, Key, ElementId)> {
        let mut element_id = from_element_id.to_string();
        let mut eik = from_eik;
        loop {
            if let Some(boundary_id) =
                self.find_error_boundary(instance_key, &element_id, error_code)
            {
                return Some((boundary_id, eik, element_id));
            }
            // Propagate to the enclosing sub-process, if any.
            let parent = self.scope_of(instance_key, eik);
            if parent == 0 {
                return None;
            }
            element_id = self.element_id_of_instance(instance_key, parent)?;
            eik = parent;
        }
    }

    /// Every element instance transitively contained in the sub-process scope
    /// `scope_eik`, sorted by key for deterministic processing.
    fn scope_descendants(&self, instance_key: Key, scope_eik: Key) -> Vec<Key> {
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return Vec::new();
        };
        let mut result = Vec::new();
        let mut stack = vec![scope_eik];
        while let Some(parent) = stack.pop() {
            for (child, p) in &instance.scopes {
                if *p == parent {
                    result.push(*child);
                    stack.push(*child);
                }
            }
        }
        result.sort_unstable();
        result
    }

    /// Terminates a sub-process scope: cancels every in-play job, armed timer and
    /// open subscription on each element instance inside `scope_eik` (and nested
    /// scopes) and completes those element instances. The sub-process element
    /// instance itself is left for the caller to complete. Used when an
    /// interrupting error boundary catches an error inside the sub-process.
    fn terminate_subprocess_scope(
        &mut self,
        log: &mut Vec<Event>,
        instance_key: Key,
        scope_eik: Key,
    ) {
        for eik in self.scope_descendants(instance_key, scope_eik) {
            let element_id = self
                .element_id_of_instance(instance_key, eik)
                .unwrap_or_default();
            if let Some(job_key) = self.active_job_on(eik) {
                self.emit(
                    log,
                    Event::JobCanceled {
                        job_key,
                        instance_key,
                    },
                );
            }
            for event in self.cancel_all_timers_on(eik) {
                self.emit(log, event);
            }
            for event in self.cancel_all_subscriptions_on(eik) {
                self.emit(log, event);
            }
            self.emit(
                log,
                Event::ElementCompleting {
                    instance_key,
                    element_instance_key: eik,
                    element_id: element_id.clone(),
                },
            );
            self.emit(
                log,
                Event::ElementCompleted {
                    instance_key,
                    element_instance_key: eik,
                    element_id,
                },
            );
        }
    }

    /// Cancels every armed (`Created`) timer resting on `element_instance_key`,
    /// regardless of kind, returning the `TimerCanceled` events (sorted by key).
    /// Used to tear down a sub-process scope on interruption.
    fn cancel_all_timers_on(&self, element_instance_key: Key) -> Vec<Event> {
        let mut timers: Vec<&state::Timer> = self
            .state
            .timers
            .values()
            .filter(|t| {
                t.element_instance_key == element_instance_key
                    && t.state == state::TimerState::Created
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

    /// Cancels every open subscription resting on `element_instance_key`,
    /// regardless of kind, returning the `MessageSubscriptionCanceled` events
    /// (sorted by key). Used to tear down a sub-process scope on interruption.
    fn cancel_all_subscriptions_on(&self, element_instance_key: Key) -> Vec<Event> {
        let mut subs: Vec<&state::MessageSubscription> = self
            .state
            .message_subscriptions
            .values()
            .filter(|s| {
                s.element_instance_key == element_instance_key
                    && s.state == state::MessageSubscriptionState::Open
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
    /// Arms a timer and/or opens a subscription for every boundary event
    /// attached to `element_id` (a service task or sub-process), returning the
    /// `TimerCreated`/`MessageSubscriptionCreated` events. Firing one later
    /// interrupts the activity (interrupting) or spawns a parallel token beside
    /// it (non-interrupting).
    fn arm_boundary_events(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: &str,
    ) -> Vec<Event> {
        let mut events = Vec::new();
        for (boundary_id, duration_millis, interrupting) in
            self.attached_timer_boundaries(instance_key, element_id)
        {
            let timer_key = self.mint_key();
            let kind = if interrupting {
                state::TimerKind::InterruptingBoundary {
                    boundary_element_id: boundary_id,
                }
            } else {
                state::TimerKind::NonInterruptingBoundary {
                    boundary_element_id: boundary_id,
                }
            };
            events.push(Event::TimerCreated {
                timer_key,
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
                due_at: self.now.saturating_add(duration_millis),
                kind,
            });
        }
        for (boundary_id, message_name, correlation_key, interrupting) in
            self.attached_message_boundaries(instance_key, element_id)
        {
            let subscription_key = self.mint_key();
            let correlation_value = self.resolve_correlation_value(instance_key, &correlation_key);
            let kind = if interrupting {
                state::MessageSubscriptionKind::InterruptingBoundary {
                    boundary_element_id: boundary_id,
                }
            } else {
                state::MessageSubscriptionKind::NonInterruptingBoundary {
                    boundary_element_id: boundary_id,
                }
            };
            events.push(Event::MessageSubscriptionCreated {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
                message_name,
                correlation_key: correlation_value,
                kind,
            });
        }
        events
    }

    /// Whether `element_id` is an embedded sub-process (interrupting it must tear
    /// down its whole inner token scope, not just cancel a job).
    fn is_subprocess(&self, instance_key: Key, element_id: &str) -> bool {
        matches!(
            self.element_kind(instance_key, element_id),
            Some(ElementKind::SubProcess { .. })
        )
    }

    /// Interrupts the activity `element_instance_key`/`element_id` because an
    /// interrupting timer or message boundary fired on it: tears down the work it
    /// owns, completes its element instance, and disarms any sibling boundaries.
    /// A service task's job is cancelled; a sub-process's whole inner token scope
    /// is terminated. The caller then routes the boundary's outgoing flow.
    fn interrupt_activity_via_boundary(
        &mut self,
        log: &mut Vec<Event>,
        instance_key: Key,
        element_instance_key: Key,
        element_id: &str,
    ) {
        if self.is_subprocess(instance_key, element_id) {
            // Cancel every job/timer/subscription inside the sub-process and
            // complete its inner element instances first.
            self.terminate_subprocess_scope(log, instance_key, element_instance_key);
        } else if let Some(job_key) = self.active_job_on(element_instance_key) {
            self.emit(
                log,
                Event::JobCanceled {
                    job_key,
                    instance_key,
                },
            );
        }
        self.emit(
            log,
            Event::ElementCompleting {
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
            },
        );
        self.emit(
            log,
            Event::ElementCompleted {
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
            },
        );
        // Disarm any sibling boundary timers and message subscriptions on it.
        for event in self.cancel_boundary_timers_on(element_instance_key) {
            self.emit(log, event);
        }
        for event in self.cancel_boundary_message_subscriptions_on(element_instance_key) {
            self.emit(log, event);
        }
    }

    fn attached_timer_boundaries(
        &self,
        instance_key: Key,
        activity_id: &str,
    ) -> Vec<(ElementId, u64, bool)> {
        let Some(process) = self.process_of_instance(instance_key) else {
            return Vec::new();
        };
        let mut found: Vec<(ElementId, u64, bool)> = process
            .elements
            .values()
            .filter_map(|e| match &e.kind {
                ElementKind::TimerBoundaryEvent {
                    attached_to,
                    duration_millis,
                    interrupting,
                    repeating: _,
                } if attached_to == activity_id => {
                    Some((e.id.clone(), *duration_millis, *interrupting))
                }
                _ => None,
            })
            .collect();
        found.sort();
        found
    }

    /// Cancels every armed (`Created`) boundary timer resting on
    /// `element_instance_key` (interrupting or non-interrupting), returning the
    /// `TimerCanceled` events. Called when the guarded activity leaves the flow
    /// another way (it completed normally or a different boundary interrupted
    /// it), so a stale timer never fires later.
    fn cancel_boundary_timers_on(&self, element_instance_key: Key) -> Vec<Event> {
        let mut timers: Vec<&state::Timer> = self
            .state
            .timers
            .values()
            .filter(|t| {
                t.element_instance_key == element_instance_key
                    && t.state == state::TimerState::Created
                    && matches!(
                        t.kind,
                        state::TimerKind::InterruptingBoundary { .. }
                            | state::TimerKind::NonInterruptingBoundary { .. }
                    )
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
    ) -> Vec<(ElementId, String, String, bool)> {
        let Some(process) = self.process_of_instance(instance_key) else {
            return Vec::new();
        };
        let mut found: Vec<(ElementId, String, String, bool)> = process
            .elements
            .values()
            .filter_map(|e| match &e.kind {
                ElementKind::MessageBoundaryEvent {
                    attached_to,
                    message_name,
                    correlation_key,
                    interrupting,
                } if attached_to == activity_id => Some((
                    e.id.clone(),
                    message_name.clone(),
                    correlation_key.clone(),
                    *interrupting,
                )),
                _ => None,
            })
            .collect();
        found.sort();
        found
    }

    /// Cancels every open boundary message subscription resting on
    /// `element_instance_key` (interrupting or non-interrupting), returning the
    /// `MessageSubscriptionCanceled` events. Called when the guarded activity
    /// leaves the flow another way (it completed normally or a different boundary
    /// interrupted it), so a stale subscription never correlates later.
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
                            | state::MessageSubscriptionKind::NonInterruptingBoundary { .. }
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

    /// Resolves the correlation value a subscription captures at open time by
    /// evaluating the stored `correlation_key` FEEL expression against the
    /// instance variables (a bare name like `orderId` is just a variable
    /// reference; `order.id` reads a context member). A missing variable, an
    /// empty key, or an evaluation error yields the empty string (matching the
    /// REST default `correlationKey` of `""`).
    fn resolve_correlation_value(&self, instance_key: Key, correlation_key: &str) -> String {
        if correlation_key.is_empty() {
            return String::new();
        }
        let vars = self.variables(instance_key);
        crate::feel::eval_string(correlation_key, &vars).unwrap_or_default()
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

    /// Resolves a service task's job type against the instance's variables.
    ///
    /// A static type (`"payment"`) is returned verbatim. A FEEL expression
    /// (`"=jobType"`, as emitted by the Camunda modeler) is treated as a simple
    /// variable reference: the leading `=` is stripped and the named variable's
    /// value supplies the type, so the job is created with the runtime type a
    /// worker actually subscribes to. An unresolvable expression (no such
    /// variable) falls back to the literal text — nano has no FEEL evaluator to
    /// raise an incident, and the literal at least surfaces the misconfiguration.
    /// Resolves a service task's job type at job-creation time. A static type is
    /// returned verbatim. A FEEL expression (a leading `=`, e.g. `=jobType` or
    /// `="worker-" + region`) is evaluated against the instance variables via
    /// [`crate::feel`], expecting a string-like result. An expression that fails
    /// to evaluate (parse error, unresolved variable, non-string result) falls
    /// back to the literal text — nano does not raise an incident here.
    fn resolve_job_type(&self, instance_key: Key, job_type: &str) -> String {
        let trimmed = job_type.trim();
        if !trimmed.starts_with('=') {
            return job_type.to_string();
        }
        let vars = self.variables(instance_key);
        crate::feel::eval_string(trimmed, &vars).unwrap_or_else(|_| job_type.to_string())
    }

    /// Resolves a user-task string attribute (assignee, due/follow-up date)
    /// declared on the BPMN element. A literal is returned verbatim; a FEEL
    /// expression (leading `=`) is evaluated against the instance variables,
    /// falling back to the literal text when it cannot be evaluated. `None` (the
    /// attribute was not declared) resolves to `None`.
    fn resolve_user_task_string(
        &self,
        instance_key: Key,
        raw: Option<&str>,
    ) -> Option<String> {
        let raw = raw?;
        let trimmed = raw.trim();
        if !trimmed.starts_with('=') {
            return Some(raw.to_string());
        }
        let vars = self.variables(instance_key);
        Some(crate::feel::eval_string(trimmed, &vars).unwrap_or_else(|_| raw.to_string()))
    }

    /// Resolves a user-task candidate list (groups or users). A literal is a
    /// comma-separated list. A FEEL expression (leading `=`) is evaluated against
    /// the instance variables; a list result yields its string items, a string
    /// result is split on commas, anything else (or a failure) yields an empty
    /// list. `None` resolves to an empty list.
    fn resolve_user_task_list(&self, instance_key: Key, raw: Option<&str>) -> Vec<String> {
        let Some(raw) = raw else {
            return Vec::new();
        };
        let trimmed = raw.trim();
        let split = |s: &str| -> Vec<String> {
            s.split(',')
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(str::to_string)
                .collect()
        };
        if !trimmed.starts_with('=') {
            return split(raw);
        }
        let vars = self.variables(instance_key);
        match crate::feel::eval(trimmed, &vars) {
            Ok(Value::List(items)) => items
                .into_iter()
                .filter_map(|v| match v {
                    Value::Str(s) => Some(s),
                    Value::Int(i) => Some(i.to_string()),
                    _ => None,
                })
                .filter(|s| !s.is_empty())
                .collect(),
            Ok(Value::Str(s)) => split(&s),
            _ => Vec::new(),
        }
    }

    /// Resolves a user-task priority expression. A literal integer or a FEEL
    /// expression yielding a number is clamped to `0..=100`; anything
    /// unresolvable (or absent) defaults to `50`.
    fn resolve_user_task_priority(&self, instance_key: Key, raw: Option<&str>) -> i32 {
        const DEFAULT_PRIORITY: i32 = 50;
        let Some(raw) = raw else {
            return DEFAULT_PRIORITY;
        };
        let trimmed = raw.trim();
        let value = if let Some(expr) = trimmed.strip_prefix('=') {
            let vars = self.variables(instance_key);
            match crate::feel::eval(expr, &vars) {
                Ok(Value::Int(i)) => i as i32,
                Ok(Value::Double(d)) => d as i32,
                _ => return DEFAULT_PRIORITY,
            }
        } else {
            match trimmed.parse::<i32>() {
                Ok(i) => i,
                Err(_) => return DEFAULT_PRIORITY,
            }
        };
        value.clamp(0, 100)
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
    /// `CancelInstance` referenced a process instance that does not exist or is
    /// no longer active (already completed or terminated).
    InstanceNotFound { instance_key: Key },
    /// `AssignUserTask`/`CompleteUserTask` referenced a user-task key that does
    /// not exist.
    UserTaskNotFound { user_task_key: Key },
    /// `AssignUserTask`/`CompleteUserTask` referenced a user task that is not in
    /// a state where it can be acted on (already completed or cancelled).
    UserTaskNotActive { user_task_key: Key },
    /// `AssignUserTask` with `allow_override = false` targeted a user task that
    /// already has an assignee; it must be unassigned first.
    UserTaskAlreadyAssigned { user_task_key: Key },
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
            EngineError::InstanceNotFound { instance_key } => {
                write!(f, "no active process instance with key {instance_key}")
            }
            EngineError::UserTaskNotFound { user_task_key } => {
                write!(f, "no user task with key {user_task_key}")
            }
            EngineError::UserTaskNotActive { user_task_key } => {
                write!(
                    f,
                    "user task {user_task_key} is not in a state that can be acted on"
                )
            }
            EngineError::UserTaskAlreadyAssigned { user_task_key } => {
                write!(
                    f,
                    "user task {user_task_key} is already assigned; unassign it before assigning again"
                )
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
    use crate::model::{ProcessBuilder, ProcessDefinition};

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

    /// start -> charge (service task, PT5S NON-interrupting timer boundary
    ///                   "remind") -> done
    ///                       \--(timer)--> reminded
    fn process_with_non_interrupting_timer_boundary() -> ProcessDefinition {
        ProcessBuilder::new("ship")
            .start_event("start")
            .service_task("charge", "payment")
            .non_interrupting_timer_boundary_event("remind", "charge", 5_000)
            .end_event("done")
            .end_event("reminded")
            .connect("start", "charge")
            .connect("charge", "done")
            .connect("remind", "reminded")
            .build()
            .unwrap()
    }

    #[test]
    fn should_fire_a_non_interrupting_timer_boundary_without_cancelling_the_job() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(
                process_with_non_interrupting_timer_boundary(),
            ))
            .unwrap();

        let events = engine
            .apply_command_at(Command::create_instance("ship"), 1_000)
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
        assert_eq!(engine.pending_jobs().len(), 1);
        let job_key = engine.pending_jobs()[0].key;
        assert_eq!(engine.timers()[0].due_at, 6_000);

        // At the due instant the timer fires but does NOT interrupt: the job
        // survives, the boundary's outgoing flow spawns a parallel token to
        // "reminded", and the instance stays active (the task is still running).
        let fired = engine.trigger_timers(6_000);
        assert_eq!(engine.state().jobs[&job_key].state, state::JobState::Created);
        assert!(!fired
            .iter()
            .any(|e| matches!(e, Event::JobCanceled { .. })));
        assert!(fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "remind" && to == "reminded"
        )));
        assert!(!engine.is_completed(instance_key));
        assert_eq!(engine.timers()[0].state, state::TimerState::Triggered);

        // Completing the job then runs the normal flow and finishes the instance.
        engine.activate_jobs("payment", "w", 1, 60_000, 0);
        engine
            .apply_command(Command::complete_job(job_key))
            .unwrap();
        assert!(engine.is_completed(instance_key));
    }

    /// start -> charge (service task, NON-interrupting message boundary "notify"
    ///                   on message "reminder" correlating orderId) -> done
    ///                       \--(message)--> notified
    fn process_with_non_interrupting_message_boundary() -> ProcessDefinition {
        ProcessBuilder::new("notifiable")
            .start_event("start")
            .service_task("charge", "payment")
            .non_interrupting_message_boundary_event("notify", "charge", "reminder", "orderId")
            .end_event("done")
            .end_event("notified")
            .connect("start", "charge")
            .connect("charge", "done")
            .connect("notify", "notified")
            .build()
            .unwrap()
    }

    #[test]
    fn should_fire_a_non_interrupting_message_boundary_for_every_matching_message() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(
                process_with_non_interrupting_message_boundary(),
            ))
            .unwrap();
        let events = engine
            .apply_command(Command::create_instance_with(
                "notifiable",
                vars(&[("orderId", Value::Str("A".into()))]),
            ))
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
        let job_key = engine.pending_jobs()[0].key;
        assert_eq!(engine.message_subscriptions().len(), 1);

        // First message: spawns a parallel token to "notified" without cancelling
        // the job; the subscription stays open and the instance stays active.
        let fired = engine.correlate_message("reminder", "A", HashMap::new(), 0);
        assert_eq!(engine.state().jobs[&job_key].state, state::JobState::Created);
        assert!(!fired
            .iter()
            .any(|e| matches!(e, Event::JobCanceled { .. })));
        assert!(fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "notify" && to == "notified"
        )));
        assert_eq!(
            engine.message_subscriptions()[0].state,
            state::MessageSubscriptionState::Open
        );
        assert!(!engine.is_completed(instance_key));

        // A second matching message fires the boundary again (open subscription).
        let fired = engine.correlate_message("reminder", "A", HashMap::new(), 0);
        assert!(fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "notify" && to == "notified"
        )));
        assert_eq!(
            engine.message_subscriptions()[0].state,
            state::MessageSubscriptionState::Open
        );

        // Completing the job runs the normal flow, finishes the instance and
        // cancels the still-open boundary subscription.
        engine.activate_jobs("payment", "w", 1, 60_000, 0);
        engine
            .apply_command(Command::complete_job(job_key))
            .unwrap();
        assert!(engine.is_completed(instance_key));
        assert_eq!(
            engine.message_subscriptions()[0].state,
            state::MessageSubscriptionState::Canceled
        );
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
                r#"decision = "yes""#,
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
    fn should_route_exclusive_gateway_on_a_numeric_feel_comparison() {
        // A richer FEEL condition than equality: amount > 100 -> big ; else small.
        let def = ProcessBuilder::new("amounts")
            .start_event("s")
            .exclusive_gateway("g")
            .end_event("big")
            .end_event("small")
            .connect("s", "g")
            .connect_when("g", "big", "amount > 100")
            .connect("g", "small")
            .build()
            .unwrap();
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();

        let vars = HashMap::from([("amount".to_string(), Value::Int(250))]);
        let events = engine
            .apply_command(Command::create_instance_with("amounts", vars))
            .unwrap();
        assert!(events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "big"
        )));
    }

    #[test]
    fn should_raise_an_expression_incident_when_a_condition_cannot_evaluate() {
        // The condition compares a string variable to a number — a FEEL type
        // error — so the gateway raises an ExpressionEvaluation incident rather
        // than silently treating the flow as not taken.
        let def = ProcessBuilder::new("typed")
            .start_event("s")
            .exclusive_gateway("g")
            .end_event("yes_end")
            .connect("s", "g")
            .connect_when("g", "yes_end", "name > 10")
            .build()
            .unwrap();
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();

        let vars = HashMap::from([("name".to_string(), Value::Str("ann".into()))]);
        let created = engine
            .apply_command(Command::create_instance_with("typed", vars))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

        let active = engine.active_incidents();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].kind, state::IncidentKind::ExpressionEvaluation);
        assert!(!engine.is_completed(instance_key));
    }

    #[test]
    fn should_park_on_a_user_task_then_resume_when_completed() {
        // start -> review (user task) -> end
        let def = ProcessBuilder::new("approval")
            .start_event("start")
            .user_task("review")
            .end_event("end")
            .connect("start", "review")
            .connect("review", "end")
            .build()
            .unwrap();

        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();

        let created = engine
            .apply_command(Command::create_instance("approval"))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

        // The token parks on the user task: a UserTaskCreated event was emitted
        // and the instance is not yet complete.
        let user_task_key = created
            .iter()
            .find_map(|e| match e {
                Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
                _ => None,
            })
            .expect("user task created");
        assert!(!engine.is_completed(instance_key));

        // Assigning keeps it parked; the assignee is recorded.
        engine
            .apply_command(Command::assign_user_task(user_task_key, "alice"))
            .unwrap();
        assert_eq!(
            engine.state().user_tasks[&user_task_key].assignee.as_deref(),
            Some("alice")
        );
        assert!(!engine.is_completed(instance_key));

        // Completing it (merging a variable) resumes the token to the end event,
        // completing the instance.
        let vars = HashMap::from([("approved".to_string(), Value::Bool(true))]);
        let done = engine
            .apply_command(Command::complete_user_task_with(user_task_key, vars))
            .unwrap();
        assert!(done.iter().any(|e| matches!(
            e,
            Event::UserTaskCompleted { user_task_key: k, .. } if *k == user_task_key
        )));
        assert!(engine.is_completed(instance_key));
        assert_eq!(
            engine.state().user_tasks[&user_task_key].state,
            state::UserTaskState::Completed
        );

        // Completing again is rejected: the task is no longer active.
        assert!(matches!(
            engine.apply_command(Command::complete_user_task(user_task_key)),
            Err(EngineError::UserTaskNotActive { .. })
        ));
    }

    #[test]
    fn should_create_a_user_task_with_resolved_attributes() {
        use crate::model::UserTaskProps;
        // A user task declaring assignee/candidates/dates/priority, partly via
        // FEEL expressions evaluated against the instance variables.
        let def = ProcessBuilder::new("approval")
            .start_event("start")
            .user_task_with(
                "review",
                UserTaskProps {
                    assignee: Some("=requester".to_string()),
                    candidate_groups: Some("ops,finance".to_string()),
                    candidate_users: Some("=reviewers".to_string()),
                    due_date: Some("2025-01-01T00:00:00Z".to_string()),
                    follow_up_date: None,
                    priority: Some("=urgency".to_string()),
                },
            )
            .end_event("end")
            .connect("start", "review")
            .connect("review", "end")
            .build()
            .unwrap();

        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();

        let vars = HashMap::from([
            ("requester".to_string(), Value::Str("alice".to_string())),
            (
                "reviewers".to_string(),
                Value::List(vec![
                    Value::Str("bob".to_string()),
                    Value::Str("carol".to_string()),
                ]),
            ),
            ("urgency".to_string(), Value::Int(80)),
        ]);
        let created = engine
            .apply_command(Command::CreateInstance {
                process_id: "approval".to_string(),
                variables: vars,
            })
            .unwrap();
        let user_task_key = created
            .iter()
            .find_map(|e| match e {
                Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
                _ => None,
            })
            .expect("user task created");

        let task = &engine.state().user_tasks[&user_task_key];
        assert_eq!(task.assignee.as_deref(), Some("alice"));
        assert_eq!(task.candidate_groups, vec!["ops", "finance"]);
        assert_eq!(task.candidate_users, vec!["bob", "carol"]);
        assert_eq!(task.due_date.as_deref(), Some("2025-01-01T00:00:00Z"));
        assert_eq!(task.follow_up_date, None);
        assert_eq!(task.priority, 80);
    }

    #[test]
    fn should_default_user_task_priority_to_fifty() {
        let def = ProcessBuilder::new("approval")
            .start_event("start")
            .user_task("review")
            .end_event("end")
            .connect("start", "review")
            .connect("review", "end")
            .build()
            .unwrap();
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let created = engine
            .apply_command(Command::create_instance("approval"))
            .unwrap();
        let user_task_key = created
            .iter()
            .find_map(|e| match e {
                Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
                _ => None,
            })
            .unwrap();
        assert_eq!(engine.state().user_tasks[&user_task_key].priority, 50);
    }

    #[test]
    fn should_reject_reassigning_an_assigned_task_without_override() {
        let def = ProcessBuilder::new("approval")
            .start_event("start")
            .user_task("review")
            .end_event("end")
            .connect("start", "review")
            .connect("review", "end")
            .build()
            .unwrap();
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let created = engine
            .apply_command(Command::create_instance("approval"))
            .unwrap();
        let user_task_key = created
            .iter()
            .find_map(|e| match e {
                Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
                _ => None,
            })
            .unwrap();

        // First assignment succeeds.
        engine
            .apply_command(Command::assign_user_task(user_task_key, "alice"))
            .unwrap();
        // A non-override reassignment is rejected while assigned.
        assert!(matches!(
            engine.apply_command(Command::AssignUserTask {
                user_task_key,
                assignee: "bob".to_string(),
                allow_override: false,
            }),
            Err(EngineError::UserTaskAlreadyAssigned { .. })
        ));
        // Unassigning then assigning again works.
        engine
            .apply_command(Command::unassign_user_task(user_task_key))
            .unwrap();
        assert_eq!(engine.state().user_tasks[&user_task_key].assignee, None);
        engine
            .apply_command(Command::AssignUserTask {
                user_task_key,
                assignee: "bob".to_string(),
                allow_override: false,
            })
            .unwrap();
        assert_eq!(
            engine.state().user_tasks[&user_task_key].assignee.as_deref(),
            Some("bob")
        );
    }

    #[test]
    fn should_update_user_task_attributes_via_changeset() {
        use crate::UserTaskChangeset;
        let def = ProcessBuilder::new("approval")
            .start_event("start")
            .user_task("review")
            .end_event("end")
            .connect("start", "review")
            .connect("review", "end")
            .build()
            .unwrap();
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let created = engine
            .apply_command(Command::create_instance("approval"))
            .unwrap();
        let user_task_key = created
            .iter()
            .find_map(|e| match e {
                Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
                _ => None,
            })
            .unwrap();

        engine
            .apply_command(Command::update_user_task(
                user_task_key,
                UserTaskChangeset {
                    candidate_groups: Some(vec!["ops".to_string()]),
                    candidate_users: None,
                    due_date: Some(Some("2025-06-01T00:00:00Z".to_string())),
                    follow_up_date: None,
                    priority: Some(20),
                },
            ))
            .unwrap();

        let task = &engine.state().user_tasks[&user_task_key];
        assert_eq!(task.candidate_groups, vec!["ops"]);
        assert_eq!(task.due_date.as_deref(), Some("2025-06-01T00:00:00Z"));
        assert_eq!(task.priority, 20);

        // Resetting the due date with an empty string clears it.
        engine
            .apply_command(Command::update_user_task(
                user_task_key,
                UserTaskChangeset {
                    due_date: Some(Some(String::new())),
                    ..Default::default()
                },
            ))
            .unwrap();
        assert_eq!(engine.state().user_tasks[&user_task_key].due_date, None);
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
                r#"decision = "yes""#,
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
                "d = true",
            )
            .connect_when(
                "g",
                "no_end",
                "d = false",
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
                "d = true",
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
                "d = true",
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

    /// start -> sub[ sub_start -> inner(work) -> sub_end ] --normal--> done
    ///                               (sub catches BUSINESS_ERROR)
    ///          sub --(error boundary)--> sad(sad-flow) -> sad_end
    fn process_with_subprocess_error_boundary() -> ProcessDefinition {
        ProcessBuilder::new("sub-error")
            .start_event("start")
            .sub_process("sub", "sub_start")
            .start_event("sub_start")
            .contained_in("sub_start", "sub")
            .service_task("inner", "work")
            .contained_in("inner", "sub")
            .end_event("sub_end")
            .contained_in("sub_end", "sub")
            .error_boundary_event("boundary", "sub", "BUSINESS_ERROR")
            .service_task("sad", "sad-flow")
            .end_event("done")
            .end_event("sad_end")
            .connect("start", "sub")
            .connect("sub_start", "inner")
            .connect("inner", "sub_end")
            .connect("sub", "done")
            .connect("boundary", "sad")
            .connect("sad", "sad_end")
            .build()
            .unwrap()
    }

    #[test]
    fn should_run_an_embedded_subprocess_to_completion_on_the_happy_path() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(
                process_with_subprocess_error_boundary(),
            ))
            .unwrap();
        let created = engine
            .apply_command(Command::create_instance("sub-error"))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

        // The token enters the sub-process and parks on its inner service task.
        assert!(!engine.is_completed(instance_key));
        assert_eq!(engine.pending_jobs().len(), 1);
        assert_eq!(engine.pending_jobs()[0].job_type, "work");

        // Completing the inner job drains the sub-process scope, which then
        // routes out its normal outgoing flow to the outer end event.
        let events = complete_one(&mut engine, "work");
        assert!(events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "sub" && to == "done"
        )));
        assert!(engine.is_completed(instance_key));
        assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
    }

    #[test]
    fn should_interrupt_an_embedded_subprocess_via_its_error_boundary() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(
                process_with_subprocess_error_boundary(),
            ))
            .unwrap();
        let created = engine
            .apply_command(Command::create_instance("sub-error"))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
        let job_key = engine.activate_jobs("work", "w", 1, 60_000, 0)[0].key;

        // The inner job throws a business error the sub-process boundary catches.
        let events = engine
            .apply_command(Command::throw_job_error(job_key, "BUSINESS_ERROR", "boom"))
            .unwrap();

        // The whole sub-process is interrupted: its inner task instance is
        // completed (terminated), the sub-process completes without taking its
        // normal flow, and the boundary routes to the sad-flow path.
        assert!(events.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_id, .. } if element_id == "inner"
        )));
        assert!(events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "boundary" && to == "sad"
        )));
        assert!(!events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "done"
        )));
        // The interrupted inner job is consumed and cannot be completed.
        let err = engine
            .apply_command(Command::complete_job(job_key))
            .unwrap_err();
        assert_eq!(err, EngineError::JobNotActive { job_key });

        // The instance is not yet complete: it is parked on the sad-flow task.
        assert!(!engine.is_completed(instance_key));
        let sad = complete_one(&mut engine, "sad-flow");
        assert!(sad.contains(&Event::ProcessInstanceCompleted { instance_key }));
        assert!(engine.is_completed(instance_key));
    }

    #[test]
    fn should_raise_an_incident_when_a_subprocess_error_is_unhandled() {
        // A sub-process with no error boundary: an error thrown inside is
        // unhandled and parks on an incident (the instance does not complete).
        let def = ProcessBuilder::new("sub-plain")
            .start_event("start")
            .sub_process("sub", "sub_start")
            .start_event("sub_start")
            .contained_in("sub_start", "sub")
            .service_task("inner", "work")
            .contained_in("inner", "sub")
            .end_event("sub_end")
            .contained_in("sub_end", "sub")
            .end_event("done")
            .connect("start", "sub")
            .connect("sub_start", "inner")
            .connect("inner", "sub_end")
            .connect("sub", "done")
            .build()
            .unwrap();

        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let created = engine
            .apply_command(Command::create_instance("sub-plain"))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
        let job_key = engine.activate_jobs("work", "w", 1, 60_000, 0)[0].key;

        let events = engine
            .apply_command(Command::throw_job_error(job_key, "BOOM", "kaboom"))
            .unwrap();

        assert!(events.iter().any(|e| matches!(
            e,
            Event::IncidentRaised { reason, .. } if reason.contains("BOOM")
        )));
        assert!(!engine.is_completed(instance_key));
        assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);
    }

    /// start -> charge (service task, PT5S NON-interrupting CYCLE timer boundary
    ///                   "tick") -> done
    ///                       \--(timer, every 5s)--> ticked
    fn process_with_non_interrupting_cycle_timer_boundary() -> ProcessDefinition {
        ProcessBuilder::new("ticker")
            .start_event("start")
            .service_task("charge", "payment")
            .non_interrupting_timer_cycle_boundary_event("tick", "charge", 5_000)
            .end_event("done")
            .end_event("ticked")
            .connect("start", "charge")
            .connect("charge", "done")
            .connect("tick", "ticked")
            .build()
            .unwrap()
    }

    #[test]
    fn should_re_arm_a_non_interrupting_cycle_timer_boundary_on_every_fire() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(
                process_with_non_interrupting_cycle_timer_boundary(),
            ))
            .unwrap();
        let events = engine
            .apply_command_at(Command::create_instance("ticker"), 1_000)
            .unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
        let job_key = engine.pending_jobs()[0].key;
        assert_eq!(engine.timers()[0].due_at, 6_000);

        // First fire at t=6000: spawns a token to "ticked", does not cancel the
        // job, and re-arms a fresh timer due at 11000 (6000 + 5000).
        let fired = engine.trigger_timers(6_000);
        assert!(fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "tick" && to == "ticked"
        )));
        assert_eq!(engine.state().jobs[&job_key].state, state::JobState::Created);
        let armed: Vec<u64> = engine
            .timers()
            .iter()
            .filter(|t| t.state == state::TimerState::Created)
            .map(|t| t.due_at)
            .collect();
        assert_eq!(
            armed,
            vec![11_000],
            "a fresh timer is armed for the next interval"
        );
        assert!(!engine.is_completed(instance_key));

        // Second fire at t=11000: fires again and re-arms for 16000.
        let fired = engine.trigger_timers(11_000);
        assert!(fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "tick" && to == "ticked"
        )));
        let armed: Vec<u64> = engine
            .timers()
            .iter()
            .filter(|t| t.state == state::TimerState::Created)
            .map(|t| t.due_at)
            .collect();
        assert_eq!(armed, vec![16_000]);

        // Completing the job runs the normal flow and disarms the pending timer.
        engine.activate_jobs("payment", "w", 1, 60_000, 0);
        engine
            .apply_command(Command::complete_job(job_key))
            .unwrap();
        assert!(engine.is_completed(instance_key));
        assert!(engine
            .timers()
            .iter()
            .all(|t| t.state != state::TimerState::Created));
    }

    /// start -> sub[ sub_start -> inner(work) -> sub_end ] --normal--> done
    ///          sub --(PT5S interrupting timer boundary)--> escalated
    fn process_with_subprocess_timer_boundary() -> ProcessDefinition {
        ProcessBuilder::new("sub-timer")
            .start_event("start")
            .sub_process("sub", "sub_start")
            .start_event("sub_start")
            .contained_in("sub_start", "sub")
            .service_task("inner", "work")
            .contained_in("inner", "sub")
            .end_event("sub_end")
            .contained_in("sub_end", "sub")
            .timer_boundary_event("timeout", "sub", 5_000)
            .end_event("done")
            .end_event("escalated")
            .connect("start", "sub")
            .connect("sub_start", "inner")
            .connect("inner", "sub_end")
            .connect("sub", "done")
            .connect("timeout", "escalated")
            .build()
            .unwrap()
    }

    #[test]
    fn should_interrupt_an_embedded_subprocess_via_a_timer_boundary() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(
                process_with_subprocess_timer_boundary(),
            ))
            .unwrap();
        let created = engine
            .apply_command_at(Command::create_instance("sub-timer"), 1_000)
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

        // The token parks on the inner task; the boundary timer is armed on the
        // sub-process for due_at = 6000.
        let job_key = engine.pending_jobs()[0].key;
        assert_eq!(engine.pending_jobs()[0].job_type, "work");
        assert!(engine
            .timers()
            .iter()
            .any(|t| t.due_at == 6_000 && t.element_id == "sub"));

        // At the due instant the timer interrupts the WHOLE sub-process: the
        // inner job is cancelled, the inner task and sub-process complete without
        // taking the normal flow, and the boundary routes to "escalated".
        let fired = engine.trigger_timers(6_000);
        assert_eq!(
            engine.state().jobs[&job_key].state,
            state::JobState::Canceled
        );
        assert!(fired.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_id, .. } if element_id == "inner"
        )));
        assert!(fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "escalated"
        )));
        assert!(!fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "done"
        )));
        assert!(engine.is_completed(instance_key));
    }

    /// start -> sub[ sub_start -> inner(work) -> sub_end ] --normal--> done
    ///          sub --(interrupting message boundary "cancel" on orderId)--> aborted
    fn process_with_subprocess_message_boundary() -> ProcessDefinition {
        ProcessBuilder::new("sub-msg")
            .start_event("start")
            .sub_process("sub", "sub_start")
            .start_event("sub_start")
            .contained_in("sub_start", "sub")
            .service_task("inner", "work")
            .contained_in("inner", "sub")
            .end_event("sub_end")
            .contained_in("sub_end", "sub")
            .message_boundary_event("cancel", "sub", "order-cancelled", "orderId")
            .end_event("done")
            .end_event("aborted")
            .connect("start", "sub")
            .connect("sub_start", "inner")
            .connect("inner", "sub_end")
            .connect("sub", "done")
            .connect("cancel", "aborted")
            .build()
            .unwrap()
    }

    #[test]
    fn should_interrupt_an_embedded_subprocess_via_a_message_boundary() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(
                process_with_subprocess_message_boundary(),
            ))
            .unwrap();
        let created = engine
            .apply_command(Command::create_instance_with(
                "sub-msg",
                vars(&[("orderId", Value::Str("A".into()))]),
            ))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
        let job_key = engine.pending_jobs()[0].key;
        assert!(engine
            .message_subscriptions()
            .iter()
            .any(|s| s.element_id == "sub"));

        // Correlating the boundary message interrupts the whole sub-process: the
        // inner job is cancelled, the inner task and sub-process complete, and the
        // boundary routes to "aborted" instead of the normal flow.
        let fired = engine.correlate_message("order-cancelled", "A", HashMap::new(), 0);
        assert_eq!(
            engine.state().jobs[&job_key].state,
            state::JobState::Canceled
        );
        assert!(fired.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_id, .. } if element_id == "inner"
        )));
        assert!(fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "cancel" && to == "aborted"
        )));
        assert!(!fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "done"
        )));
        assert!(engine.is_completed(instance_key));
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
    fn should_resolve_feel_variable_reference_job_type_at_job_creation() {
        // given a process whose service task type is a FEEL variable reference
        let def = ProcessBuilder::new("dynamic")
            .start_event("start")
            .service_task("work", "=jobType")
            .end_event("end")
            .connect("start", "work")
            .connect("work", "end")
            .build()
            .unwrap();
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();

        // when an instance is created with jobType bound to a concrete value
        let mut vars = HashMap::new();
        vars.insert("jobType".to_string(), Value::Str("payment".to_string()));
        engine
            .apply_command(Command::create_instance_with("dynamic", vars))
            .unwrap();

        // then the created job carries the resolved type, not the literal "=jobType"
        let jobs = engine.pending_jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].job_type, "payment");
        // and it is activatable by the resolved type
        assert_eq!(engine.activate_jobs("payment", "w", 10, 1_000, 0).len(), 1);
    }

    #[test]
    fn should_fall_back_to_literal_when_job_type_variable_is_missing() {
        // given the same process but no jobType variable provided
        let def = ProcessBuilder::new("dynamic")
            .start_event("start")
            .service_task("work", "=jobType")
            .end_event("end")
            .connect("start", "work")
            .connect("work", "end")
            .build()
            .unwrap();
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        engine
            .apply_command(Command::create_instance("dynamic"))
            .unwrap();

        // then the unresolved expression falls back to the literal text (no panic)
        let jobs = engine.pending_jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].job_type, "=jobType");
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

    #[test]
    fn evicts_only_completed_instances_and_what_they_own() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();

        // One instance that we drive to completion, and one left in-flight
        // (parked on its service-task job).
        let done = engine
            .apply_command(Command::create_instance("order"))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap();
        complete_one(&mut engine, "payment");
        assert!(engine.is_completed(done));

        let live = engine
            .apply_command(Command::create_instance("order"))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap();
        assert!(!engine.is_completed(live));
        // The live instance still owns an activatable job.
        assert!(engine.state().jobs.values().any(|j| j.instance_key == live));

        // Evicting an active instance is a no-op.
        assert!(!engine.evict_instance(live));
        assert!(engine.instance(live).is_some());

        // Evicting the completed one removes it and its jobs.
        assert!(engine.evict_instance(done));
        assert!(engine.instance(done).is_none());
        assert!(!engine.state().jobs.values().any(|j| j.instance_key == done));

        // The in-flight instance and its job are untouched, and the deployed
        // definition (not instance-scoped) is retained.
        assert!(engine.instance(live).is_some());
        assert!(engine.state().jobs.values().any(|j| j.instance_key == live));
        assert_eq!(engine.state().processes.len(), 1);
    }

    #[test]
    fn evict_completed_sweeps_every_finished_instance() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();

        for _ in 0..3 {
            engine
                .apply_command(Command::create_instance("order"))
                .unwrap();
            complete_one(&mut engine, "payment");
        }
        // A fourth instance left in-flight.
        let live = engine
            .apply_command(Command::create_instance("order"))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap();

        assert_eq!(engine.state().instances.len(), 4);
        let evicted = engine.evict_completed();
        assert_eq!(evicted, 3);
        assert_eq!(engine.state().instances.len(), 1);
        assert!(engine.instance(live).is_some());
    }

    // ---- cancel process instance ----

    #[test]
    fn cancel_terminates_instance_and_cancels_its_job() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        let instance_key = engine
            .apply_command(Command::create_instance("order"))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap();

        // Token parked on the service-task job.
        let job_key = engine
            .state()
            .jobs
            .values()
            .find(|j| j.instance_key == instance_key)
            .unwrap()
            .key;
        assert!(!engine.is_completed(instance_key));

        let events = engine
            .apply_command(Command::cancel_instance(instance_key))
            .unwrap();

        // The job is cancelled and the instance is terminated (not completed).
        assert!(events.contains(&Event::JobCanceled {
            job_key,
            instance_key
        }));
        assert!(events.contains(&Event::ProcessInstanceTerminated { instance_key }));
        assert!(!events
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })));
        assert_eq!(
            engine.instance(instance_key).unwrap().state,
            ProcessInstanceState::Terminated
        );
        assert!(engine.instance(instance_key).unwrap().active.is_empty());
        assert_eq!(engine.job(job_key).unwrap().state, state::JobState::Canceled);
    }

    #[test]
    fn cancel_disarms_a_parked_timer() {
        let def = ProcessBuilder::new("delayed")
            .start_event("start")
            .timer_intermediate_catch_event("wait", 5_000)
            .end_event("end")
            .connect("start", "wait")
            .connect("wait", "end")
            .build()
            .unwrap();
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let instance_key = engine
            .apply_command_at(Command::create_instance("delayed"), 1_000)
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap();
        assert_eq!(engine.timers()[0].state, state::TimerState::Created);

        engine
            .apply_command(Command::cancel_instance(instance_key))
            .unwrap();

        // The armed timer is cancelled, so a later due tick fires nothing.
        assert_eq!(engine.timers()[0].state, state::TimerState::Canceled);
        assert!(engine.trigger_timers(6_000).is_empty());
        assert_eq!(
            engine.instance(instance_key).unwrap().state,
            ProcessInstanceState::Terminated
        );
    }

    #[test]
    fn cancel_disarms_an_open_message_subscription() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(process_with_message_catch()))
            .unwrap();
        let instance_key = engine
            .apply_command(Command::create_instance_with(
                "await-payment",
                vars(&[("orderId", Value::Str("o-1".into()))]),
            ))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap();
        assert_eq!(
            engine.message_subscriptions()[0].state,
            state::MessageSubscriptionState::Open
        );

        engine
            .apply_command(Command::cancel_instance(instance_key))
            .unwrap();

        assert_eq!(
            engine.message_subscriptions()[0].state,
            state::MessageSubscriptionState::Canceled
        );
        assert_eq!(
            engine.instance(instance_key).unwrap().state,
            ProcessInstanceState::Terminated
        );
    }

    #[test]
    fn cancel_closes_an_active_incident() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        let instance_key = engine
            .apply_command(Command::create_instance("order"))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap();
        // Drive the job to a no-retries incident.
        let job = engine
            .activate_jobs("payment", "w", 1, 60_000, 0)
            .into_iter()
            .next()
            .unwrap();
        engine
            .apply_command(Command::fail_job(job.key, 0, "boom"))
            .unwrap();
        assert_eq!(engine.active_incidents().len(), 1);

        engine
            .apply_command(Command::cancel_instance(instance_key))
            .unwrap();

        // The incident is closed and the parked job is cancelled.
        assert!(engine.active_incidents().is_empty());
        assert_eq!(engine.job(job.key).unwrap().state, state::JobState::Canceled);
        assert_eq!(
            engine.instance(instance_key).unwrap().state,
            ProcessInstanceState::Terminated
        );
    }

    #[test]
    fn cancel_rejects_unknown_or_finished_instances() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();

        // Unknown key.
        assert_eq!(
            engine.apply_command(Command::cancel_instance(999)),
            Err(EngineError::InstanceNotFound { instance_key: 999 })
        );

        // A completed instance can no longer be cancelled.
        let done = engine
            .apply_command(Command::create_instance("order"))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap();
        complete_one(&mut engine, "payment");
        assert!(engine.is_completed(done));
        assert_eq!(
            engine.apply_command(Command::cancel_instance(done)),
            Err(EngineError::InstanceNotFound { instance_key: done })
        );

        // And cancelling twice fails the second time.
        let live = engine
            .apply_command(Command::create_instance("order"))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap();
        engine
            .apply_command(Command::cancel_instance(live))
            .unwrap();
        assert_eq!(
            engine.apply_command(Command::cancel_instance(live)),
            Err(EngineError::InstanceNotFound { instance_key: live })
        );
    }

    #[test]
    fn cancel_survives_replay() {
        let mut engine = Engine::new();
        let mut log = Vec::new();
        log.extend(
            engine
                .apply_command(Command::DeployProcess(linear_with_task()))
                .unwrap(),
        );
        let instance_key = {
            let events = engine
                .apply_command(Command::create_instance("order"))
                .unwrap();
            let k = events.iter().find_map(|e| e.instance_key()).unwrap();
            log.extend(events);
            k
        };
        log.extend(
            engine
                .apply_command(Command::cancel_instance(instance_key))
                .unwrap(),
        );

        let recovered = Engine::replay(log);
        assert_eq!(
            recovered.instance(instance_key).unwrap().state,
            ProcessInstanceState::Terminated
        );
        assert!(recovered.instance(instance_key).unwrap().active.is_empty());
        assert!(recovered
            .state()
            .jobs
            .values()
            .all(|j| j.state == state::JobState::Canceled));
    }
}
