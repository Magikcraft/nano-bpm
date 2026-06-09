//! The engine: a single-writer command/event/applier loop.
//!
//! [`Engine::apply_command`] is the one entry point that changes anything. It:
//!
//! 1. validates the [`Command`] and emits the top-level event(s),
//! 2. drives an internal work queue of [`Step`]s — the BPMN element lifecycle —
//!    until the instance is quiescent (finished, or resting on a job),
//! 3. detects process-instance completion,
//!
//! applying every event through [`state::apply`] as it goes. The processor
//! ([`Engine::process_step`]) only *reads* state and *decides*; it never mutates.

use std::collections::{HashSet, VecDeque};

use crate::command::Command;
use crate::event::Event;
use crate::model::ElementKind;
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
}

impl Engine {
    /// Creates an empty engine.
    pub fn new() -> Self {
        Self {
            state: State::new(),
            next_key: 0,
        }
    }

    /// Read-only access to the full engine state (useful for queries and tests).
    pub fn state(&self) -> &State {
        &self.state
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

    /// All jobs currently awaiting completion.
    pub fn pending_jobs(&self) -> Vec<&state::Job> {
        self.state
            .jobs
            .values()
            .filter(|j| j.state == state::JobState::Created)
            .collect()
    }

    fn mint_key(&mut self) -> Key {
        self.next_key += 1;
        self.next_key
    }

    /// Applies a command, returning the ordered list of events it produced.
    ///
    /// This is the engine's single writer: it runs to quiescence before
    /// returning, so on success the returned events are the complete record of
    /// everything that happened.
    pub fn apply_command(&mut self, command: Command) -> Result<Vec<Event>, EngineError> {
        let mut log: Vec<Event> = Vec::new();
        let mut queue: VecDeque<Step> = VecDeque::new();

        match command {
            Command::DeployProcess(process) => {
                // Validation that ProcessBuilder already guarantees, re-checked
                // for definitions built by hand.
                if !process.elements.contains_key(&process.start_event) {
                    return Err(EngineError::NoStartEvent {
                        process_id: process.id,
                    });
                }
                self.emit(&mut log, Event::ProcessDeployed { process });
            }

            Command::CreateInstance { process_id } => {
                let process = self.state.processes.get(&process_id).ok_or_else(|| {
                    EngineError::ProcessNotFound {
                        process_id: process_id.clone(),
                    }
                })?;
                let start_event = process.start_event.clone();

                let instance_key = self.mint_key();
                self.emit(
                    &mut log,
                    Event::ProcessInstanceCreated {
                        instance_key,
                        process_id,
                    },
                );
                queue.push_back(Step::Activate {
                    instance_key,
                    element_id: start_event,
                });
            }

            Command::CompleteJob { job_key } => {
                let job = self
                    .state
                    .jobs
                    .get(&job_key)
                    .ok_or(EngineError::JobNotFound { job_key })?;
                if job.state != state::JobState::Created {
                    return Err(EngineError::JobNotActive { job_key });
                }
                let instance_key = job.instance_key;
                let element_instance_key = job.element_instance_key;
                let element_id = job.element_id.clone();

                self.emit(&mut log, Event::JobCompleted { job_key });
                // The parked service-task token resumes from ACTIVATED.
                queue.push_back(Step::Complete {
                    instance_key,
                    element_instance_key,
                    element_id,
                });
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
            } => {
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

                let kind = self.element_kind(instance_key, &element_id);
                let mut followups = Vec::new();
                match kind {
                    // Pass-through elements complete immediately.
                    Some(ElementKind::StartEvent) | Some(ElementKind::EndEvent) => {
                        followups.push(Step::Complete {
                            instance_key,
                            element_instance_key,
                            element_id,
                        });
                    }
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
                    // Unknown element / instance: nothing to do.
                    None => {}
                }

                (events, followups)
            }

            Step::Complete {
                instance_key,
                element_instance_key,
                element_id,
            } => {
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
                for target in self.outgoing(instance_key, &element_id) {
                    events.push(Event::SequenceFlowTaken {
                        instance_key,
                        from: element_id.clone(),
                        to: target.clone(),
                    });
                    followups.push(Step::Activate {
                        instance_key,
                        element_id: target,
                    });
                }

                (events, followups)
            }
        }
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
        self.state.processes.get(&instance.process_id)
    }

    fn element_kind(&self, instance_key: Key, element_id: &str) -> Option<ElementKind> {
        self.process_of_instance(instance_key)?
            .element(element_id)
            .map(|e| e.kind.clone())
    }

    fn outgoing(&self, instance_key: Key, element_id: &str) -> Vec<String> {
        self.process_of_instance(instance_key)
            .and_then(|p| p.element(element_id))
            .map(|e| e.outgoing.clone())
            .unwrap_or_default()
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
    /// `CompleteJob` referenced a job that is not in the `Created` state.
    JobNotActive { job_key: Key },
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
                write!(f, "job {job_key} is not active and cannot be completed")
            }
        }
    }
}

impl std::error::Error for EngineError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ProcessBuilder;

    fn linear_with_task() -> crate::model::ProcessDefinition {
        ProcessBuilder::new("order")
            .start_event("start")
            .service_task("charge", "payment")
            .end_event("end")
            .connect("start", "charge")
            .connect("charge", "end")
            .build()
            .unwrap()
    }

    #[test]
    fn should_park_on_service_task_then_complete_on_job() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();

        let events = engine
            .apply_command(Command::CreateInstance {
                process_id: "order".into(),
            })
            .unwrap();

        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

        // given a job was created and the instance is parked, not completed
        assert_eq!(engine.pending_jobs().len(), 1);
        assert!(!engine.is_completed(instance_key));
        assert!(matches!(events.last().unwrap(), Event::JobCreated { .. }));

        // when the job is completed
        let job_key = engine.pending_jobs()[0].key;
        let events = engine
            .apply_command(Command::CompleteJob { job_key })
            .unwrap();

        // then the token resumes to the end event and the instance completes
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
            .apply_command(Command::CreateInstance {
                process_id: "noop".into(),
            })
            .unwrap();

        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
        assert!(engine.is_completed(instance_key));
        assert!(events.contains(&Event::ProcessInstanceCompleted { instance_key }));
    }

    #[test]
    fn should_reject_unknown_process() {
        let mut engine = Engine::new();
        let err = engine
            .apply_command(Command::CreateInstance {
                process_id: "missing".into(),
            })
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
        let err = engine
            .apply_command(Command::CompleteJob { job_key: 42 })
            .unwrap_err();
        assert_eq!(err, EngineError::JobNotFound { job_key: 42 });
    }

    #[test]
    fn should_be_deterministic_and_replayable() {
        // Running the same commands twice yields identical event logs...
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
                    .apply_command(Command::CreateInstance {
                        process_id: "order".into(),
                    })
                    .unwrap(),
            );
            let job_key = engine.pending_jobs()[0].key;
            all.extend(
                engine
                    .apply_command(Command::CompleteJob { job_key })
                    .unwrap(),
            );
            (engine, all)
        };

        let (engine_a, log_a) = run();
        let (_engine_b, log_b) = run();
        assert_eq!(log_a, log_b);

        // ...and replaying that log over a fresh State reconstructs engine state.
        let mut replayed = State::new();
        for event in &log_a {
            state::apply(&mut replayed, event);
        }
        assert_eq!(&replayed, engine_a.state());
    }
}
