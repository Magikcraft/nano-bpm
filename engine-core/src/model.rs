//! The BPMN process model.
//!
//! This is a deliberately tiny subset of BPMN — enough to demonstrate the
//! execution architecture end to end. Parsing real BPMN 2.0 XML into this model
//! is intentionally out of scope for the core (a future `bpmn-parser` crate can
//! produce [`ProcessDefinition`]s); here processes are built programmatically via
//! [`ProcessBuilder`].

use std::collections::HashMap;

/// Identifier of a BPMN element (the BPMN `id` attribute), e.g. `"start"`.
pub type ElementId = String;

/// A process variable value.
///
/// A minimal, hashable value type — enough for exclusive-gateway conditions.
/// (A real engine would carry arbitrary JSON; that is an intentional extension
/// point.)
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Bool(bool),
    Int(i64),
    Str(String),
}

/// A boolean guard on a sequence flow, evaluated against process variables.
///
/// Only equality is supported — deliberately not a FEEL expression engine. New
/// operators plug in here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Condition {
    /// True when the variable exists and equals `value`.
    Equals { variable: String, value: Value },
}

impl Condition {
    /// Evaluates the condition against a set of variables.
    pub fn eval(&self, variables: &HashMap<String, Value>) -> bool {
        match self {
            Condition::Equals { variable, value } => variables.get(variable) == Some(value),
        }
    }
}

/// An outgoing sequence flow: a target element and an optional guard condition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequenceFlow {
    pub to: ElementId,
    /// `None` means an unconditional flow. On an exclusive gateway an
    /// unconditional flow acts as the default (place it last).
    pub condition: Option<Condition>,
}

/// The kind of a BPMN flow node.
///
/// The set is intentionally small. New element types plug in here and gain
/// behaviour in `engine::process_step` — the rest of the architecture is
/// unaffected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ElementKind {
    /// A none start event. Pass-through: activates then immediately completes.
    StartEvent,
    /// A none end event. Pass-through; consuming the last token completes the
    /// process instance.
    EndEvent,
    /// A service task. On activation it creates a job of `job_type` and the token
    /// rests until the job is completed.
    ServiceTask { job_type: String },
    /// An exclusive (XOR) gateway: takes exactly one outgoing flow, chosen by
    /// evaluating flow conditions in order (first match wins; an unconditional
    /// flow is the default). Tokens pass through independently — there is no
    /// join synchronisation.
    ExclusiveGateway,
    /// A parallel (AND) gateway. As a split it takes *all* outgoing flows; as a
    /// join (more than one incoming flow) it waits for a token on every incoming
    /// flow before producing one outgoing token.
    ParallelGateway,
}

/// A single BPMN flow node and its outgoing sequence flows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Element {
    pub id: ElementId,
    pub kind: ElementKind,
    /// Outgoing sequence flows, in declaration order.
    pub outgoing: Vec<SequenceFlow>,
}

/// An executable process definition: a set of [`Element`]s plus the id of the
/// single start event where new instances begin.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessDefinition {
    pub id: String,
    pub elements: HashMap<ElementId, Element>,
    pub start_event: ElementId,
}

impl ProcessDefinition {
    /// Looks up an element by id.
    pub fn element(&self, id: &str) -> Option<&Element> {
        self.elements.get(id)
    }

    /// Number of sequence flows across the whole process that target `id`. Used
    /// to detect parallel-gateway joins.
    pub fn incoming_count(&self, id: &str) -> usize {
        self.elements
            .values()
            .flat_map(|e| e.outgoing.iter())
            .filter(|f| f.to == id)
            .count()
    }
}

/// Ergonomic builder for [`ProcessDefinition`]s.
///
/// ```
/// use nanobpmn_engine_core::ProcessBuilder;
/// let def = ProcessBuilder::new("p")
///     .start_event("s")
///     .end_event("e")
///     .connect("s", "e")
///     .build()
///     .unwrap();
/// assert_eq!(def.start_event, "s");
/// ```
#[derive(Debug, Default)]
pub struct ProcessBuilder {
    id: String,
    elements: Vec<Element>,
    edges: Vec<(ElementId, SequenceFlow)>,
}

impl ProcessBuilder {
    /// Starts building a process with the given BPMN process id.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            elements: Vec::new(),
            edges: Vec::new(),
        }
    }

    fn add(mut self, id: impl Into<String>, kind: ElementKind) -> Self {
        self.elements.push(Element {
            id: id.into(),
            kind,
            outgoing: Vec::new(),
        });
        self
    }

    /// Adds a none start event.
    pub fn start_event(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::StartEvent)
    }

    /// Adds a none end event.
    pub fn end_event(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::EndEvent)
    }

    /// Adds a service task that creates jobs of the given `job_type`.
    pub fn service_task(self, id: impl Into<String>, job_type: impl Into<String>) -> Self {
        self.add(
            id,
            ElementKind::ServiceTask {
                job_type: job_type.into(),
            },
        )
    }

    /// Adds an exclusive (XOR) gateway.
    pub fn exclusive_gateway(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::ExclusiveGateway)
    }

    /// Adds a parallel (AND) gateway.
    pub fn parallel_gateway(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::ParallelGateway)
    }

    /// Adds an unconditional sequence flow from `from` to `to`.
    pub fn connect(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.edges.push((
            from.into(),
            SequenceFlow {
                to: to.into(),
                condition: None,
            },
        ));
        self
    }

    /// Adds a conditional sequence flow from `from` to `to`, taken (on an
    /// exclusive gateway) only when `condition` holds.
    pub fn connect_when(
        mut self,
        from: impl Into<String>,
        to: impl Into<String>,
        condition: Condition,
    ) -> Self {
        self.edges.push((
            from.into(),
            SequenceFlow {
                to: to.into(),
                condition: Some(condition),
            },
        ));
        self
    }

    /// Validates and assembles the [`ProcessDefinition`].
    ///
    /// Fails if there is not exactly one start event, or if a sequence flow
    /// references an unknown element.
    pub fn build(self) -> Result<ProcessDefinition, BuildError> {
        let mut elements: HashMap<ElementId, Element> = HashMap::new();
        for e in self.elements {
            if elements.contains_key(&e.id) {
                return Err(BuildError::DuplicateElement(e.id));
            }
            elements.insert(e.id.clone(), e);
        }

        for (from, flow) in &self.edges {
            if !elements.contains_key(&flow.to) {
                return Err(BuildError::UnknownFlowTarget {
                    from: from.clone(),
                    to: flow.to.clone(),
                });
            }
            let source = elements
                .get_mut(from)
                .ok_or_else(|| BuildError::UnknownFlowSource {
                    from: from.clone(),
                    to: flow.to.clone(),
                })?;
            source.outgoing.push(flow.clone());
        }

        let starts: Vec<&Element> = elements
            .values()
            .filter(|e| e.kind == ElementKind::StartEvent)
            .collect();
        let start_event = match starts.as_slice() {
            [single] => single.id.clone(),
            [] => return Err(BuildError::NoStartEvent),
            _ => return Err(BuildError::MultipleStartEvents),
        };

        Ok(ProcessDefinition {
            id: self.id,
            elements,
            start_event,
        })
    }
}

/// Errors produced while assembling a [`ProcessDefinition`] with [`ProcessBuilder::build`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BuildError {
    DuplicateElement(ElementId),
    UnknownFlowSource { from: ElementId, to: ElementId },
    UnknownFlowTarget { from: ElementId, to: ElementId },
    NoStartEvent,
    MultipleStartEvents,
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::DuplicateElement(id) => write!(f, "duplicate element id: {id}"),
            BuildError::UnknownFlowSource { from, to } => {
                write!(
                    f,
                    "sequence flow {from}->{to} has unknown source element {from}"
                )
            }
            BuildError::UnknownFlowTarget { from, to } => {
                write!(
                    f,
                    "sequence flow {from}->{to} has unknown target element {to}"
                )
            }
            BuildError::NoStartEvent => write!(f, "process has no start event"),
            BuildError::MultipleStartEvents => write!(f, "process has more than one start event"),
        }
    }
}

impl std::error::Error for BuildError {}
