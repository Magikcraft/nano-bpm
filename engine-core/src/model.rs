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

/// The kind of a BPMN flow node.
///
/// The set is intentionally small. New element types (gateways, intermediate
/// events, sub-processes…) plug in here and gain behaviour in
/// `engine::process_step` — the rest of the architecture is unaffected.
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
}

/// A single BPMN flow node and its outgoing sequence flows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Element {
    pub id: ElementId,
    pub kind: ElementKind,
    /// Target element ids of this element's outgoing sequence flows, in order.
    pub outgoing: Vec<ElementId>,
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
    edges: Vec<(ElementId, ElementId)>,
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

    /// Adds a sequence flow from `from` to `to`.
    pub fn connect(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.edges.push((from.into(), to.into()));
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

        for (from, to) in &self.edges {
            if !elements.contains_key(to) {
                return Err(BuildError::UnknownFlowTarget {
                    from: from.clone(),
                    to: to.clone(),
                });
            }
            let source = elements
                .get_mut(from)
                .ok_or_else(|| BuildError::UnknownFlowSource {
                    from: from.clone(),
                    to: to.clone(),
                })?;
            source.outgoing.push(to.clone());
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
