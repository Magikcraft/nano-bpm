//! The BPMN process model.
//!
//! This is a deliberately tiny subset of BPMN — enough to demonstrate the
//! execution architecture end to end. Parsing real BPMN 2.0 XML into this model
//! is intentionally out of scope for the core (a future `bpmn-parser` crate can
//! produce [`ProcessDefinition`]s); here processes are built programmatically via
//! [`ProcessBuilder`].

use std::collections::{BTreeMap, HashMap};

/// Serde default for boundary-event `interrupting` flags: older serialized
/// definitions (before non-interrupting boundaries existed) carried only
/// interrupting boundaries, so a missing field deserializes as `true`.
#[cfg(feature = "serde")]
fn default_true() -> bool {
    true
}

/// Identifier of a BPMN element (the BPMN `id` attribute), e.g. `"start"`.
pub type ElementId = String;

/// A process variable value.
///
/// A JSON-like value type spanning what FEEL operates over: `null`, booleans,
/// numbers (integers kept distinct from decimals so integral values round-trip
/// exactly), strings, lists and contexts (maps). Numbers compare numerically
/// across `Int`/`Double` inside the FEEL evaluator; the derived `PartialEq` here
/// is structural (used by event/state equality), so `Int(3) != Double(3.0)`
/// structurally — callers needing FEEL semantics go through [`crate::feel`].
///
/// `Eq` is implemented by hand (rather than derived) so the many `#[derive(Eq)]`
/// log/state types that embed a `Value` keep compiling despite the `f64` payload.
/// This is sound because the evaluator never produces `NaN` (FEEL maps invalid
/// arithmetic to `Null`), so reflexivity holds for every value we actually store.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Value {
    /// The FEEL `null` (and the result of an unresolved variable or a failed
    /// computation).
    Null,
    Bool(bool),
    /// An integral number. Kept distinct from [`Value::Double`] so whole numbers
    /// round-trip without acquiring a fractional rendering.
    Int(i64),
    /// A non-integral number.
    Double(f64),
    Str(String),
    /// An ordered list (FEEL list).
    List(Vec<Value>),
    /// A context (FEEL map): an ordered set of named entries.
    Map(BTreeMap<String, Value>),
}

impl Eq for Value {}

impl Value {
    /// The number this value holds, if it is numeric (`Int` or `Double`).
    pub fn as_f64(&self) -> Option<f64> {        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Double(d) => Some(*d),
            _ => None,
        }
    }

    /// Builds a numeric value, narrowing to [`Value::Int`] when the number is a
    /// finite integer and to [`Value::Null`] when it is not finite (FEEL has no
    /// infinity/NaN). This keeps integral arithmetic results rendering cleanly.
    pub fn number(n: f64) -> Value {
        if !n.is_finite() {
            return Value::Null;
        }
        if n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
            Value::Int(n as i64)
        } else {
            Value::Double(n)
        }
    }
}

/// Renders an `f64` the FEEL way: a finite integral value drops the fractional
/// part (`2.0` -> `"2"`), everything else uses the shortest round-trip form.
pub(crate) fn format_double(d: f64) -> String {
    if d.is_finite() && d.fract() == 0.0 && d.abs() < i64::MAX as f64 {
        (d as i64).to_string()
    } else {
        d.to_string()
    }
}

/// A boolean guard on a sequence flow: a FEEL expression evaluated against the
/// instance variables (the conditionExpression body, e.g. `= amount > 10`).
///
/// The full FEEL grammar supported by [`crate::feel`] applies — comparisons,
/// arithmetic, boolean logic, member access — not just equality.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Condition {
    /// The raw FEEL expression. A single leading `=` marker is optional (it is
    /// stripped by the evaluator).
    pub expression: String,
}

impl Condition {
    /// Wraps a raw FEEL expression as a sequence-flow condition.
    pub fn new(expression: impl Into<String>) -> Self {
        Condition {
            expression: expression.into(),
        }
    }

    /// Evaluates the condition against a set of variables, returning the FEEL
    /// error on a parse/type failure or a non-boolean result. The exclusive
    /// gateway turns such an error into an `ExpressionEvaluation` incident.
    pub fn eval(&self, variables: &HashMap<String, Value>) -> Result<bool, crate::feel::FeelError> {
        crate::feel::eval_bool(&self.expression, variables)
    }
}

/// An outgoing sequence flow: a target element and an optional guard condition.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SequenceFlow {
    pub to: ElementId,
    /// `None` means an unconditional flow. On an exclusive gateway an
    /// unconditional flow acts as the default (place it last).
    pub condition: Option<Condition>,
}

/// The (raw, un-evaluated) assignment, scheduling and priority expressions
/// declared on a `userTask` BPMN element via its Zeebe extension elements
/// (`zeebe:assignmentDefinition`, `zeebe:taskSchedule`, `zeebe:priorityDefinition`).
///
/// Each value may be a literal or a FEEL expression (a string beginning with
/// `=`). They are resolved against the instance variables when the user task is
/// created. `candidate_groups`/`candidate_users` are either a static
/// comma-separated list or a FEEL expression yielding a list (or comma string).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserTaskProps {
    /// Raw assignee expression (`zeebe:assignmentDefinition assignee`).
    pub assignee: Option<String>,
    /// Raw candidate-groups expression (`zeebe:assignmentDefinition candidateGroups`).
    pub candidate_groups: Option<String>,
    /// Raw candidate-users expression (`zeebe:assignmentDefinition candidateUsers`).
    pub candidate_users: Option<String>,
    /// Raw due-date expression (`zeebe:taskSchedule dueDate`).
    pub due_date: Option<String>,
    /// Raw follow-up-date expression (`zeebe:taskSchedule followUpDate`).
    pub follow_up_date: Option<String>,
    /// Raw priority expression (`zeebe:priorityDefinition priority`); defaults to
    /// `50` when absent or unresolvable.
    pub priority: Option<String>,
}

/// The kind of a BPMN flow node.
///
/// The set is intentionally small. New element types plug in here and gain
/// behaviour in `engine::process_step` — the rest of the architecture is
/// unaffected.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ElementKind {
    /// A none start event. Pass-through: activates then immediately completes.
    StartEvent,
    /// A none end event. Pass-through; consuming the last token completes the
    /// process instance.
    EndEvent,
    /// A service task. On activation it creates a job of `job_type` and the token
    /// rests until the job is completed. `priority` is the *raw* (un-evaluated)
    /// job-priority expression declared via `zeebe:priorityDefinition` (a literal
    /// or a FEEL expression); it is resolved against the instance variables when
    /// the job is created and controls activation order (higher first). `None`
    /// means no declaration, which resolves to the default priority.
    ServiceTask {
        job_type: String,
        priority: Option<String>,
    },
    /// A (native/Zeebe) user task. On activation it creates a user task that a
    /// human claims and completes; the token rests until the user task is
    /// completed ([`crate::Command::CompleteUserTask`]). Unlike a service task it
    /// is not activated by a job worker — it is assigned and completed directly
    /// through the user-task API. The fields carry the *raw* (un-evaluated)
    /// assignment, scheduling and priority expressions declared on the BPMN
    /// element; they are resolved against the instance variables (FEEL or literal)
    /// when the task is created.
    UserTask(UserTaskProps),
    /// An exclusive (XOR) gateway: takes exactly one outgoing flow, chosen by
    /// evaluating flow conditions in order (first match wins; an unconditional
    /// flow is the default). Tokens pass through independently — there is no
    /// join synchronisation.
    ExclusiveGateway,
    /// A parallel (AND) gateway. As a split it takes *all* outgoing flows; as a
    /// join (more than one incoming flow) it waits for a token on every incoming
    /// flow before producing one outgoing token.
    ParallelGateway,
    /// An error boundary event attached to an activity (here, a service task).
    /// It has no incoming sequence flow; instead it is triggered when a job
    /// throws a business error whose code matches `error_code`, interrupting the
    /// activity and routing the token along the boundary event's outgoing flows.
    ErrorBoundaryEvent {
        /// Id of the activity this boundary event is attached to.
        attached_to: ElementId,
        /// The BPMN error code this boundary event catches.
        error_code: String,
    },
    /// A timer intermediate catch event. On activation it arms a timer due
    /// `duration_millis` after the current clock and the token rests on it; the
    /// token resumes along the event's outgoing flow once a clock tick
    /// ([`crate::Command::TriggerTimers`]) finds the timer due. `duration_millis`
    /// is in the same units the host feeds the engine as `now`.
    TimerIntermediateCatchEvent { duration_millis: u64 },
    /// A timer boundary event attached to an activity (here, a service task). It
    /// has no incoming sequence flow; instead a timer is armed when the activity
    /// activates, and when the timer becomes due
    /// ([`crate::Command::TriggerTimers`]) it fires once. An `interrupting` timer
    /// interrupts the activity (its token and any job cancelled) and routes the
    /// token along this event's outgoing flow; a non-interrupting one
    /// (`interrupting == false`) leaves the activity running and spawns a new
    /// parallel token along the outgoing flow instead. `duration_millis` is in
    /// the same units the host feeds the engine as `now`.
    TimerBoundaryEvent {
        /// Id of the activity this boundary event is attached to.
        attached_to: ElementId,
        /// How long after the activity activates the timer fires; for a
        /// `repeating` (cycle) timer this is also the period between fires.
        duration_millis: u64,
        /// Whether firing interrupts the activity (`true`, the default for older
        /// definitions) or spawns a parallel token and leaves it running.
        #[cfg_attr(feature = "serde", serde(default = "default_true"))]
        interrupting: bool,
        /// Whether the timer re-arms after firing (a BPMN `timeCycle`) or fires
        /// once (a `timeDuration`). Only meaningful for non-interrupting timers —
        /// an interrupting timer cancels its activity on the first fire, so it
        /// never re-arms.
        #[cfg_attr(feature = "serde", serde(default))]
        repeating: bool,
    },
    /// A message intermediate catch event. On activation it opens a message
    /// subscription keyed by `message_name` and a correlation value (the
    /// stringified value of the instance variable named `correlation_key`), and
    /// the token rests on it; the token resumes along the event's outgoing flow
    /// once a [`crate::Command::CorrelateMessage`] with a matching name and
    /// correlation value arrives.
    MessageIntermediateCatchEvent {
        /// The BPMN message name this event subscribes to.
        message_name: String,
        /// Name of the instance variable whose value identifies the instance to
        /// correlate to (the subscription's correlation key).
        correlation_key: String,
    },
    /// A message boundary event attached to an activity (here, a service task).
    /// It has no incoming sequence flow; instead a message subscription is opened
    /// when the activity activates. An `interrupting` boundary, when a matching
    /// message is correlated, interrupts the activity (its token and any job
    /// cancelled) and runs the token along this event's outgoing flow; a
    /// non-interrupting one (`interrupting == false`) leaves the activity running
    /// and spawns a new parallel token along the outgoing flow for every matching
    /// message (its subscription stays open).
    MessageBoundaryEvent {
        /// Id of the activity this boundary event is attached to.
        attached_to: ElementId,
        /// The BPMN message name this event subscribes to.
        message_name: String,
        /// Name of the instance variable whose value identifies the instance to
        /// correlate to (the subscription's correlation key).
        correlation_key: String,
        /// Whether firing interrupts the activity (`true`, the default for older
        /// definitions) or spawns a parallel token and leaves it running.
        #[cfg_attr(feature = "serde", serde(default = "default_true"))]
        interrupting: bool,
    },
    /// A message start event. It has no incoming sequence flow; instead the
    /// engine opens a process-level subscription at deploy time, and a
    /// [`crate::Command::CorrelateMessage`] with a matching `message_name`
    /// **creates a new instance** (the message's variables become the instance's
    /// variables), which then runs along this event's outgoing flow. Behaves as a
    /// pass-through once an instance starts.
    MessageStartEvent {
        /// The BPMN message name that triggers a new instance.
        message_name: String,
    },
    /// A timer start event. It has no incoming sequence flow; instead the engine
    /// arms a process-level timer at deploy time (first due `interval_millis`
    /// after deployment). When a clock tick ([`crate::Command::TriggerTimers`])
    /// finds it due, a **new instance** is created and runs along this event's
    /// outgoing flow. A `repeating` timer (a BPMN cycle) re-arms for another
    /// `interval_millis`; a one-shot (a BPMN duration) fires exactly once.
    TimerStartEvent {
        /// The delay from deployment to the first fire, and — when `repeating` —
        /// the period between subsequent fires, in the host's clock units.
        interval_millis: u64,
        /// Whether the timer re-arms after firing (a cycle) or fires once.
        repeating: bool,
    },
    /// An embedded sub-process: a container holding its own flow (its inner
    /// elements carry [`Element::parent`] equal to this element's id). On
    /// activation it opens a token scope and activates its inner start event
    /// ([`start_event`]); the sub-process element instance rests while the inner
    /// flow runs. When the inner flow drains (its last inner token is consumed)
    /// the sub-process completes and routes along its outgoing flow. An
    /// interrupting error boundary event attached to it terminates the whole
    /// inner scope and routes along the boundary's outgoing flow instead.
    ///
    /// [`start_event`]: ElementKind::SubProcess::start_event
    SubProcess {
        /// Id of the sub-process's inner (none) start event, where its token
        /// scope begins.
        start_event: ElementId,
    },
    /// A none intermediate throw event. A pure pass-through: on activation it
    /// completes immediately and routes the token along its outgoing flow,
    /// exactly like a gateway with a single unconditional outgoing flow. Used
    /// for BPMN `intermediateThrowEvent`s (escalation/signal/none throws that
    /// don't block a forward path) and as the inert demotion target for the
    /// surplus start events of a process that declares more than one.
    IntermediateThrowEvent,
    /// A call activity: invokes another process (`called_process_id`) and waits
    /// for it to complete before routing along its outgoing flow. Nano consumes
    /// call activities by **inline expansion** — [`ProcessDefinition::inline_call_activities`]
    /// rewrites each call activity into an embedded [`SubProcess`] holding a copy
    /// of the called process's flow — so the runtime engine itself never executes
    /// this kind (an unexpanded call activity degrades to a pass-through).
    CallActivity {
        /// The `calledElement` / `zeebe:calledElement processId` of the invoked
        /// process definition.
        called_process_id: String,
    },
}

impl ElementKind {
    /// Whether this is a start-event kind (none, message or timer start). A
    /// process has exactly one such element — its [`ProcessDefinition::start_event`].
    pub fn is_start_event(&self) -> bool {
        matches!(
            self,
            ElementKind::StartEvent
                | ElementKind::MessageStartEvent { .. }
                | ElementKind::TimerStartEvent { .. }
        )
    }
}

/// A single BPMN flow node and its outgoing sequence flows.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Element {
    pub id: ElementId,
    pub kind: ElementKind,
    /// Outgoing sequence flows, in declaration order.
    pub outgoing: Vec<SequenceFlow>,
    /// Id of the embedded sub-process that contains this element, or `None` for
    /// elements at the process level. Used to scope tokens and to pick the
    /// process-level start event. Defaulted to `None` so models serialized
    /// before sub-processes existed still load.
    #[cfg_attr(feature = "serde", serde(default))]
    pub parent: Option<ElementId>,
}

/// An executable process definition: a set of [`Element`]s plus the id of the
/// single start event where new instances begin.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ProcessDefinition {
    pub id: String,
    pub elements: HashMap<ElementId, Element>,
    pub start_event: ElementId,
    /// The original BPMN XML this definition was parsed from, retained verbatim
    /// so it can be served back (e.g. Camunda's `getProcessDefinitionXML`, the
    /// console's diagram view). Empty for definitions built programmatically via
    /// [`ProcessBuilder`] rather than parsed from XML, and defaulted empty when
    /// deserializing journals/snapshots written before this field existed. It is
    /// non-executable metadata: it travels with the deployment (journaled and
    /// snapshotted) but no engine logic reads it.
    #[cfg_attr(feature = "serde", serde(default))]
    pub xml: String,
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

    /// Rewrites every [`ElementKind::CallActivity`] in this definition into an
    /// embedded [`ElementKind::SubProcess`] holding an inlined, id-prefixed copy
    /// of the called process's flow, resolving callees by id from `library`.
    ///
    /// Nano's corpus generator walks paths through the **real** engine, which has
    /// no first-class call-activity executor; inline expansion lets a multi-stage
    /// orchestrator (e.g. a CDD/AML refresh that calls nine phase processes) run
    /// on the existing, well-tested sub-process token-scope machinery without any
    /// new runtime state, event or snapshot variants.
    ///
    /// Expansion is recursive (a called process may itself call others) with a
    /// cycle guard. Inlined ids are prefixed `"<callId>$"` (recursively nested,
    /// so collisions across phases are impossible); `attached_to` /
    /// `start_event` references inside a callee are remapped to the prefixed ids.
    /// The call activity's own outgoing flow and parent scope are preserved, so
    /// the surrounding flow is untouched. Returns an error if a `calledElement`
    /// is missing from `library` or a call cycle is detected.
    pub fn inline_call_activities(
        &self,
        library: &HashMap<String, ProcessDefinition>,
    ) -> Result<ProcessDefinition, String> {
        let mut elements = HashMap::new();
        let mut stack = vec![self.id.clone()];
        splice_call_activities(&mut elements, &self.elements, "", None, library, &mut stack)?;
        Ok(ProcessDefinition {
            id: self.id.clone(),
            elements,
            start_event: self.start_event.clone(),
            xml: self.xml.clone(),
        })
    }
}

/// Recursively copies `src` elements into `out`, prefixing every id (and the
/// targets of outgoing flows and embedded id references) with `prefix`, and
/// expanding each [`ElementKind::CallActivity`] into a [`ElementKind::SubProcess`]
/// whose inner flow is a further-prefixed copy of the callee. `parent_override`
/// is the new parent id imposed on `src`'s process-level (parentless) elements —
/// `None` at the orchestrator's own level, `Some(callId)` when splicing a callee
/// underneath the sub-process that replaced its call activity. `stack` holds the
/// process ids on the current call path for cycle detection.
fn splice_call_activities(
    out: &mut HashMap<ElementId, Element>,
    src: &HashMap<ElementId, Element>,
    prefix: &str,
    parent_override: Option<&str>,
    library: &HashMap<String, ProcessDefinition>,
    stack: &mut Vec<String>,
) -> Result<(), String> {
    let pfx = |id: &str| format!("{prefix}{id}");
    for el in src.values() {
        let new_id = pfx(&el.id);
        let new_parent = match &el.parent {
            Some(p) => Some(pfx(p)),
            None => parent_override.map(|s| s.to_string()),
        };
        let outgoing: Vec<SequenceFlow> = el
            .outgoing
            .iter()
            .map(|f| SequenceFlow {
                to: pfx(&f.to),
                condition: f.condition.clone(),
            })
            .collect();
        if let ElementKind::CallActivity { called_process_id } = &el.kind {
            let called = library.get(called_process_id).ok_or_else(|| {
                format!(
                    "call activity '{}' references unknown process '{}'",
                    el.id, called_process_id
                )
            })?;
            if stack.iter().any(|p| p == called_process_id) {
                return Err(format!(
                    "call activity cycle detected at process '{called_process_id}'"
                ));
            }
            let inner_prefix = format!("{new_id}$");
            let inner_start = format!("{inner_prefix}{}", called.start_event);
            out.insert(
                new_id.clone(),
                Element {
                    id: new_id.clone(),
                    kind: ElementKind::SubProcess {
                        start_event: inner_start,
                    },
                    outgoing,
                    parent: new_parent,
                },
            );
            stack.push(called_process_id.clone());
            splice_call_activities(
                out,
                &called.elements,
                &inner_prefix,
                Some(&new_id),
                library,
                stack,
            )?;
            stack.pop();
        } else {
            out.insert(
                new_id.clone(),
                Element {
                    id: new_id,
                    kind: remap_kind_ids(&el.kind, &pfx),
                    outgoing,
                    parent: new_parent,
                },
            );
        }
    }
    Ok(())
}

/// Returns a clone of `kind` with every embedded element-id reference (a
/// sub-process inner start, a boundary event's `attached_to`) rewritten through
/// `pfx`. Kinds without id references are cloned unchanged.
fn remap_kind_ids(kind: &ElementKind, pfx: &impl Fn(&str) -> String) -> ElementKind {
    match kind {
        ElementKind::SubProcess { start_event } => ElementKind::SubProcess {
            start_event: pfx(start_event),
        },
        ElementKind::ErrorBoundaryEvent {
            attached_to,
            error_code,
        } => ElementKind::ErrorBoundaryEvent {
            attached_to: pfx(attached_to),
            error_code: error_code.clone(),
        },
        ElementKind::TimerBoundaryEvent {
            attached_to,
            duration_millis,
            interrupting,
            repeating,
        } => ElementKind::TimerBoundaryEvent {
            attached_to: pfx(attached_to),
            duration_millis: *duration_millis,
            interrupting: *interrupting,
            repeating: *repeating,
        },
        ElementKind::MessageBoundaryEvent {
            attached_to,
            message_name,
            correlation_key,
            interrupting,
        } => ElementKind::MessageBoundaryEvent {
            attached_to: pfx(attached_to),
            message_name: message_name.clone(),
            correlation_key: correlation_key.clone(),
            interrupting: *interrupting,
        },
        other => other.clone(),
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
    /// Recorded `(child, parent sub-process)` containment, applied in [`build`].
    ///
    /// [`build`]: ProcessBuilder::build
    parents: Vec<(ElementId, ElementId)>,
}

impl ProcessBuilder {
    /// Starts building a process with the given BPMN process id.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            elements: Vec::new(),
            edges: Vec::new(),
            parents: Vec::new(),
        }
    }

    fn add(mut self, id: impl Into<String>, kind: ElementKind) -> Self {
        self.elements.push(Element {
            id: id.into(),
            kind,
            outgoing: Vec::new(),
            parent: None,
        });
        self
    }

    /// Adds a none start event.
    pub fn start_event(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::StartEvent)
    }

    /// Adds a message start event: a [`crate::Command::CorrelateMessage`] with a
    /// matching `message_name` creates a new instance starting here.
    pub fn message_start_event(
        self,
        id: impl Into<String>,
        message_name: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::MessageStartEvent {
                message_name: message_name.into(),
            },
        )
    }

    /// Adds a one-shot timer start event: a single instance is created
    /// `duration_millis` after the process deploys.
    pub fn timer_start_event_once(self, id: impl Into<String>, duration_millis: u64) -> Self {
        self.add(
            id,
            ElementKind::TimerStartEvent {
                interval_millis: duration_millis,
                repeating: false,
            },
        )
    }

    /// Adds a recurring timer start event (a BPMN cycle): a new instance is
    /// created every `interval_millis`, the first one that long after deploy.
    pub fn timer_start_event_cycle(self, id: impl Into<String>, interval_millis: u64) -> Self {
        self.add(
            id,
            ElementKind::TimerStartEvent {
                interval_millis,
                repeating: true,
            },
        )
    }

    /// Adds a none end event.
    pub fn end_event(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::EndEvent)
    }

    /// Adds a none intermediate throw event (a pass-through; see
    /// [`ElementKind::IntermediateThrowEvent`]).
    pub fn intermediate_throw_event(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::IntermediateThrowEvent)
    }

    /// Adds a call activity invoking `called_process_id` (see
    /// [`ElementKind::CallActivity`]). Expanded inline before deployment by
    /// [`ProcessDefinition::inline_call_activities`].
    pub fn call_activity(
        self,
        id: impl Into<String>,
        called_process_id: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::CallActivity {
                called_process_id: called_process_id.into(),
            },
        )
    }

    /// Adds a service task that creates jobs of the given `job_type`.
    pub fn service_task(self, id: impl Into<String>, job_type: impl Into<String>) -> Self {
        self.add(
            id,
            ElementKind::ServiceTask {
                job_type: job_type.into(),
                priority: None,
            },
        )
    }

    /// Adds a service task whose jobs carry the given (raw) `zeebe:priorityDefinition`
    /// expression — a literal or FEEL expression resolved at job creation that
    /// controls activation order (higher priority is activated first).
    pub fn service_task_with_priority(
        self,
        id: impl Into<String>,
        job_type: impl Into<String>,
        priority: Option<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::ServiceTask {
                job_type: job_type.into(),
                priority,
            },
        )
    }

    /// Adds an exclusive (XOR) gateway.
    pub fn exclusive_gateway(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::ExclusiveGateway)
    }

    /// Adds a (native) user task: on activation it creates a user task that a
    /// human claims and completes through the user-task API.
    pub fn user_task(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::UserTask(UserTaskProps::default()))
    }

    /// Adds a (native) user task with the given assignment/scheduling/priority
    /// expressions (as declared on the BPMN element's Zeebe extension elements).
    pub fn user_task_with(self, id: impl Into<String>, props: UserTaskProps) -> Self {
        self.add(id, ElementKind::UserTask(props))
    }

    /// Adds a parallel (AND) gateway.
    pub fn parallel_gateway(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::ParallelGateway)
    }

    /// Adds an embedded sub-process whose inner token scope begins at
    /// `start_event` (the id of its inner none start event). Add the inner flow
    /// nodes/flows normally and mark each as [`contained_in`] this sub-process;
    /// connect the sub-process's own outgoing flow with [`connect`].
    ///
    /// [`contained_in`]: ProcessBuilder::contained_in
    /// [`connect`]: ProcessBuilder::connect
    pub fn sub_process(self, id: impl Into<String>, start_event: impl Into<String>) -> Self {
        self.add(
            id,
            ElementKind::SubProcess {
                start_event: start_event.into(),
            },
        )
    }

    /// Records that `child` is contained in the embedded sub-process `parent`.
    /// Inner elements must be declared this way so their tokens are scoped to the
    /// sub-process and so the process-level start event can be identified.
    pub fn contained_in(mut self, child: impl Into<String>, parent: impl Into<String>) -> Self {
        self.parents.push((child.into(), parent.into()));
        self
    }

    /// Adds an error boundary event attached to `attached_to`, catching the BPMN
    /// error `error_code`. Connect its outgoing flow(s) with [`connect`] to route
    /// the error-handling path.
    ///
    /// [`connect`]: ProcessBuilder::connect
    pub fn error_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        error_code: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::ErrorBoundaryEvent {
                attached_to: attached_to.into(),
                error_code: error_code.into(),
            },
        )
    }

    /// Adds a timer intermediate catch event that holds the token for
    /// `duration_millis` (in the host's clock units) before releasing it along
    /// its outgoing flow. The token resumes when a clock tick
    /// ([`crate::Command::TriggerTimers`]) finds the timer due.
    pub fn timer_intermediate_catch_event(
        self,
        id: impl Into<String>,
        duration_millis: u64,
    ) -> Self {
        self.add(id, ElementKind::TimerIntermediateCatchEvent { duration_millis })
    }

    /// Adds an interrupting timer boundary event attached to `attached_to`. A
    /// timer is armed for `duration_millis` (in the host's clock units) when the
    /// activity activates; when it fires the activity is interrupted and the
    /// token runs along this event's outgoing flow. Connect its outgoing flow(s)
    /// with [`connect`] to route the timeout-handling path.
    ///
    /// [`connect`]: ProcessBuilder::connect
    pub fn timer_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        duration_millis: u64,
    ) -> Self {
        self.add(
            id,
            ElementKind::TimerBoundaryEvent {
                attached_to: attached_to.into(),
                duration_millis,
                interrupting: true,
                repeating: false,
            },
        )
    }

    /// Adds a non-interrupting timer boundary event attached to `attached_to`. A
    /// timer is armed for `duration_millis` (in the host's clock units) when the
    /// activity activates; when it fires the activity keeps running and a new
    /// parallel token is spawned along this event's outgoing flow. Connect its
    /// outgoing flow(s) with [`connect`] to route the side path.
    ///
    /// [`connect`]: ProcessBuilder::connect
    pub fn non_interrupting_timer_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        duration_millis: u64,
    ) -> Self {
        self.add(
            id,
            ElementKind::TimerBoundaryEvent {
                attached_to: attached_to.into(),
                duration_millis,
                interrupting: false,
                repeating: false,
            },
        )
    }

    /// Adds a non-interrupting **cycle** timer boundary event attached to
    /// `attached_to`. Like [`non_interrupting_timer_boundary_event`], but the
    /// timer re-arms for another `interval_millis` after each fire, so it spawns a
    /// parallel token every interval while the activity runs. Connect its outgoing
    /// flow(s) with [`connect`] to route the side path.
    ///
    /// [`connect`]: ProcessBuilder::connect
    /// [`non_interrupting_timer_boundary_event`]: ProcessBuilder::non_interrupting_timer_boundary_event
    pub fn non_interrupting_timer_cycle_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        interval_millis: u64,
    ) -> Self {
        self.add(
            id,
            ElementKind::TimerBoundaryEvent {
                attached_to: attached_to.into(),
                duration_millis: interval_millis,
                interrupting: false,
                repeating: true,
            },
        )
    }

    /// Adds a message intermediate catch event named `message_name`, correlating
    /// on the instance variable named `correlation_key`. The token rests on it
    /// until a [`crate::Command::CorrelateMessage`] with a matching name and
    /// correlation value arrives, then resumes along its outgoing flow.
    pub fn message_intermediate_catch_event(
        self,
        id: impl Into<String>,
        message_name: impl Into<String>,
        correlation_key: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::MessageIntermediateCatchEvent {
                message_name: message_name.into(),
                correlation_key: correlation_key.into(),
            },
        )
    }

    /// Adds an interrupting message boundary event attached to `attached_to`,
    /// subscribing to `message_name` and correlating on the instance variable
    /// named `correlation_key`. A subscription is opened when the activity
    /// activates; when a matching message is correlated the activity is
    /// interrupted and the token runs along this event's outgoing flow. Connect
    /// its outgoing flow(s) with [`connect`] to route the handling path.
    ///
    /// [`connect`]: ProcessBuilder::connect
    pub fn message_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        message_name: impl Into<String>,
        correlation_key: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::MessageBoundaryEvent {
                attached_to: attached_to.into(),
                message_name: message_name.into(),
                correlation_key: correlation_key.into(),
                interrupting: true,
            },
        )
    }

    /// Adds a non-interrupting message boundary event attached to `attached_to`,
    /// subscribing to `message_name` and correlating on the instance variable
    /// named `correlation_key`. A subscription is opened when the activity
    /// activates; for every matching message correlated the activity keeps
    /// running and a new parallel token is spawned along this event's outgoing
    /// flow (the subscription stays open). Connect its outgoing flow(s) with
    /// [`connect`] to route the side path.
    ///
    /// [`connect`]: ProcessBuilder::connect
    pub fn non_interrupting_message_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        message_name: impl Into<String>,
        correlation_key: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::MessageBoundaryEvent {
                attached_to: attached_to.into(),
                message_name: message_name.into(),
                correlation_key: correlation_key.into(),
                interrupting: false,
            },
        )
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
    /// exclusive gateway) only when the FEEL `expression` evaluates to `true`.
    pub fn connect_when(
        mut self,
        from: impl Into<String>,
        to: impl Into<String>,
        expression: impl Into<String>,
    ) -> Self {
        self.edges.push((
            from.into(),
            SequenceFlow {
                to: to.into(),
                condition: Some(Condition::new(expression)),
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

        // Apply sub-process containment.
        for (child, parent) in &self.parents {
            if !elements.contains_key(parent) {
                return Err(BuildError::UnknownParent {
                    child: child.clone(),
                    parent: parent.clone(),
                });
            }
            match elements.get_mut(child) {
                Some(element) => element.parent = Some(parent.clone()),
                None => {
                    return Err(BuildError::UnknownChild {
                        child: child.clone(),
                        parent: parent.clone(),
                    })
                }
            }
        }

        // The process-level start event is the unique start event that is not
        // contained in any sub-process (sub-process inner start events have a
        // parent and start their own scope, not the instance).
        let starts: Vec<&Element> = elements
            .values()
            .filter(|e| e.kind.is_start_event() && e.parent.is_none())
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
            // Programmatically built definitions have no source XML; parse_bpmn
            // overwrites this with the verbatim resource for parsed deployments.
            xml: String::new(),
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
    /// A `contained_in` referenced a sub-process element that does not exist.
    UnknownParent { child: ElementId, parent: ElementId },
    /// A `contained_in` referenced a child element that does not exist.
    UnknownChild { child: ElementId, parent: ElementId },
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
            BuildError::UnknownParent { child, parent } => {
                write!(f, "element {child} is contained in unknown sub-process {parent}")
            }
            BuildError::UnknownChild { child, parent } => {
                write!(f, "unknown element {child} declared as contained in sub-process {parent}")
            }
        }
    }
}

impl std::error::Error for BuildError {}
