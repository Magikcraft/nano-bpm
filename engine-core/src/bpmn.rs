//! A minimal BPMN 2.0 XML parser.
//!
//! This turns the slice of BPMN that the engine understands into
//! [`ProcessDefinition`]s, so processes can be deployed from `.bpmn` files
//! (e.g. exported by the Camunda Modeler) instead of only being built
//! programmatically with [`crate::ProcessBuilder`].
//!
//! It is deliberately tiny and dependency-free: a hand-rolled, namespace-prefix
//! agnostic XML scanner plus a single pass that recognises the supported flow
//! nodes. It is **not** a conformant BPMN/XML implementation — it understands
//! exactly the constructs the engine can execute and ignores the rest
//! (diagram interchange, documentation, lanes, …).
//!
//! ## Supported subset
//!
//! * `process` (one or more per file) with its `id`.
//! * Flow nodes: `startEvent`, `endEvent`, `serviceTask`, `userTask`,
//!   `exclusiveGateway`, `parallelGateway`.
//! * `subProcess` (embedded): its nested flow nodes/flows are scoped to it, and
//!   a `boundaryEvent` with an `errorEventDefinition` attached to it becomes an
//!   interrupting error boundary on the sub-process.
//! * `intermediateCatchEvent` with a nested `timerEventDefinition`/`timeDuration`
//!   (timer catch) or a nested `messageEventDefinition` (message catch).
//! * `boundaryEvent` with `attachedToRef` and a nested `errorEventDefinition`
//!   `errorRef`, resolved against definitions-level `error` elements
//!   (`<error id="…" errorCode="…">`) into an error boundary event; or a nested
//!   `timerEventDefinition` (timer boundary); or a nested `messageEventDefinition`
//!   (message boundary). `cancelActivity="false"` makes a timer or message
//!   boundary **non-interrupting** (the activity keeps running and a parallel
//!   token is spawned on each fire); the default is interrupting.
//! * Definitions-level `message` elements (`<message id="…" name="…">`) with a
//!   nested `zeebe:subscription correlationKey="=var"`, referenced by message
//!   catch/boundary events via `messageRef`.
//! * A service task's job type is taken from a nested
//!   `zeebe:taskDefinition type="…"`; if absent it defaults to the task id. A
//!   FEEL expression (`type="=jobType"`, `type='="worker-" + region'`) is
//!   evaluated against the instance variables at job-creation time via
//!   [`crate::feel`]; an expression that cannot be evaluated falls back to the
//!   literal text (no incident is raised).
//! * A user task's assignment/scheduling/priority attributes come from nested
//!   `zeebe:assignmentDefinition` (`assignee`, `candidateGroups`,
//!   `candidateUsers`), `zeebe:taskSchedule` (`dueDate`, `followUpDate`) and
//!   `zeebe:priorityDefinition` (`priority`). Each may be a literal or a FEEL
//!   expression resolved against the instance variables when the task is created.
//! * `sequenceFlow` with `sourceRef`/`targetRef`, and an optional
//!   `conditionExpression` whose FEEL body is stored verbatim and evaluated by
//!   [`crate::feel`] at the exclusive gateway (comparisons, arithmetic, boolean
//!   logic, member access — not just equality). A condition that fails to
//!   evaluate to a boolean raises an `ExpressionEvaluation` incident.

use std::collections::HashMap;

use crate::model::{ProcessBuilder, ProcessDefinition};
use crate::xml::{attr, local_name, tokenize, Token};

/// An error encountered while parsing BPMN XML.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseError {
    /// The XML was malformed (unterminated tag, bad quoting, …).
    MalformedXml(String),
    /// A `process` element had no `id` attribute.
    ProcessWithoutId,
    /// A `sequenceFlow` was missing `sourceRef`/`targetRef`.
    IncompleteSequenceFlow { process_id: String },
    /// The XML contained no `process` elements.
    NoProcess,
    /// A parsed process failed validation (e.g. no/many start events, dangling
    /// flow). Carries the underlying [`crate::BuildError`] message.
    InvalidProcess { process_id: String, reason: String },
    /// A `boundaryEvent` was missing `attachedToRef`, or its
    /// `errorEventDefinition` referenced an `error` that was not declared.
    InvalidBoundaryEvent { process_id: String, reason: String },
    /// A message event (`intermediateCatchEvent`/`boundaryEvent` with a
    /// `messageEventDefinition`) referenced a `message` that was not declared, or
    /// the declared message had no `zeebe:subscription correlationKey`.
    InvalidMessageEvent { process_id: String, reason: String },
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::MalformedXml(detail) => write!(f, "malformed BPMN XML: {detail}"),
            ParseError::ProcessWithoutId => write!(f, "<process> element has no id"),
            ParseError::IncompleteSequenceFlow { process_id } => write!(
                f,
                "process {process_id} has a sequenceFlow without sourceRef/targetRef"
            ),
            ParseError::NoProcess => write!(f, "no <process> element found"),
            ParseError::InvalidProcess { process_id, reason } => {
                write!(f, "invalid process {process_id}: {reason}")
            }
            ParseError::InvalidBoundaryEvent { process_id, reason } => {
                write!(
                    f,
                    "invalid boundary event in process {process_id}: {reason}"
                )
            }
            ParseError::InvalidMessageEvent { process_id, reason } => {
                write!(f, "invalid message event in process {process_id}: {reason}")
            }
        }
    }
}

impl std::error::Error for ParseError {}

/// Parses BPMN 2.0 XML into the executable [`ProcessDefinition`]s it contains.
///
/// Returns one definition per `<process>` element, in document order.
///
/// ```
/// use nanobpmn_engine_core::bpmn::parse_bpmn;
/// let xml = r#"
///   <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
///     <bpmn:process id="p">
///       <bpmn:startEvent id="s" />
///       <bpmn:endEvent id="e" />
///       <bpmn:sequenceFlow id="f" sourceRef="s" targetRef="e" />
///     </bpmn:process>
///   </bpmn:definitions>"#;
/// let defs = parse_bpmn(xml).unwrap();
/// assert_eq!(defs.len(), 1);
/// assert_eq!(defs[0].id, "p");
/// assert_eq!(defs[0].start_event, "s");
/// ```
pub fn parse_bpmn(xml: &str) -> Result<Vec<ProcessDefinition>, ParseError> {
    let tokens = tokenize(xml).map_err(|e| ParseError::MalformedXml(e.0))?;

    let mut processes: Vec<ProcessAcc> = Vec::new();
    let mut current: Option<ProcessAcc> = None;
    // Index of the service task currently being read (to attach its job type).
    let mut cur_service_task: Option<usize> = None;
    // Index of the user task currently being read (to attach assignment,
    // scheduling and priority expressions from its Zeebe extension elements).
    let mut cur_user_task: Option<usize> = None;
    // Index of the sequence flow currently being read (to attach a condition).
    let mut cur_flow: Option<usize> = None;
    let mut condition_text: Option<String> = None;
    // A separate buffer for a conditional-event's nested `<condition>` FEEL text
    // (distinct from a sequence flow's `conditionExpression`).
    let mut event_condition_text: Option<String> = None;
    // A buffer for a multi-instance `<completionCondition>` FEEL text, and the
    // index of the activity whose `multiInstanceLoopCharacteristics` is open.
    let mut completion_condition_text: Option<String> = None;
    let mut cur_multi_instance: Option<usize> = None;
    // The boundary event currently being read (to attach its errorEventDefinition).
    let mut cur_boundary: Option<PendingBoundary> = None;
    // Index of the intermediate catch event currently being read (timer or
    // message), and a buffer for its nested `timeDuration` text while inside it.
    let mut cur_intermediate: Option<usize> = None;
    // Index of the call activity currently being read, so a nested
    // `zeebe:calledElement processId="…"` child can record its callee.
    let mut cur_call: Option<usize> = None;
    let mut duration_text: Option<String> = None;
    // Index of the start event currently being read (to attach a nested
    // messageEventDefinition or timerEventDefinition), and a buffer for a timer
    // start event's nested `timeCycle` text while inside it.
    let mut cur_start: Option<usize> = None;
    let mut cycle_text: Option<String> = None;
    // Buffer for a timer event's nested `timeDate` (an absolute FEEL/ISO instant).
    let mut date_text: Option<String> = None;
    // Definitions-level `<error id=… errorCode=…>` declarations: id -> code.
    let mut errors: HashMap<String, String> = HashMap::new();
    // Definitions-level `<message id=… name=…>` declarations, with the
    // correlation-key variable from a nested `zeebe:subscription`: id -> decl.
    let mut messages: HashMap<String, MessageDecl> = HashMap::new();
    // Id of the `<message>` currently being read (to attach its subscription).
    let mut cur_message: Option<String> = None;
    // Definitions-level `<signal id=… name=…>` declarations: id -> name.
    let mut signals: HashMap<String, String> = HashMap::new();
    // Stack of activity node indices that can carry a `zeebe:ioMapping`, so a
    // nested `zeebe:input`/`zeebe:output` attaches to the innermost open
    // activity. `in_io_mapping` gates input/output reads to a real ioMapping.
    let mut io_stack: Vec<usize> = Vec::new();
    let mut in_io_mapping = false;

    for token in &tokens {
        match token {
            Token::Start {
                name,
                attrs,
                self_closing,
            } => {
                match local_name(name) {
                    "process" => {
                        let id = attr(attrs, "id").ok_or(ParseError::ProcessWithoutId)?;
                        current = Some(ProcessAcc::new(id.to_string()));
                    }
                    // Definitions-level error declarations live outside <process>.
                    "error" => {
                        if let (Some(id), Some(code)) =
                            (attr(attrs, "id"), attr(attrs, "errorCode"))
                        {
                            errors.insert(id.to_string(), code.to_string());
                        }
                    }
                    // Definitions-level message declarations also live outside
                    // <process>; the correlation key arrives via a nested
                    // zeebe:subscription.
                    "message" => {
                        if let Some(id) = attr(attrs, "id") {
                            messages.insert(
                                id.to_string(),
                                MessageDecl {
                                    name: attr(attrs, "name").unwrap_or(id).to_string(),
                                    correlation_key: None,
                                },
                            );
                            if !self_closing {
                                cur_message = Some(id.to_string());
                            }
                        }
                    }
                    // Definitions-level signal declarations live outside
                    // <process>; signals correlate by name only.
                    "signal" => {
                        if let Some(id) = attr(attrs, "id") {
                            signals.insert(
                                id.to_string(),
                                attr(attrs, "name").unwrap_or(id).to_string(),
                            );
                        }
                    }
                    // zeebe:subscription correlationKey, nested in the current
                    // message's extensionElements.
                    "subscription" => {
                        if let (Some(mid), Some(expr)) =
                            (cur_message.as_ref(), attr(attrs, "correlationKey"))
                        {
                            if let Some(decl) = messages.get_mut(mid) {
                                decl.correlation_key = parse_correlation_key(expr);
                            }
                        }
                    }
                    tag if current.is_some() => {
                        let acc = current.as_mut().expect("current process set");
                        match tag {
                            "startEvent" => {
                                let idx = acc.add_node(attrs, NodeKind::Start);
                                if !self_closing {
                                    cur_start = idx;
                                }
                            }
                            "endEvent" => {
                                acc.add_node(attrs, NodeKind::End);
                            }
                            "exclusiveGateway" => {
                                let idx = acc.add_node(attrs, NodeKind::Exclusive);
                                // Capture the `default` flow id so it is selected
                                // only as a fallback (not by document order).
                                if let (Some(i), Some(d)) = (idx, attr(attrs, "default")) {
                                    acc.nodes[i].default_flow = Some(d.to_string());
                                }
                            }
                            "parallelGateway" => {
                                acc.add_node(attrs, NodeKind::Parallel);
                            }
                            "serviceTask" => {
                                let idx = acc.add_node(attrs, NodeKind::Service);
                                if !self_closing {
                                    cur_service_task = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // A business-rule task (DMN) and a script task are
                            // both treated as service-task-like work steps: they
                            // run a job (its type taken from a nested
                            // zeebe:taskDefinition / zeebe:calledDecision, else
                            // defaulting to the element id). Nano has no DMN/script
                            // evaluator, but for trace generation and replay the
                            // step is faithfully a single job activation.
                            //
                            // When a native DMN decision engine is built, it is to
                            // be signed for Sebastian Menski, creator of Camunda's
                            // engine-dmn (see docs: engineer-signature naming).
                            "businessRuleTask" | "scriptTask" => {
                                let idx = acc.add_node(attrs, NodeKind::Service);
                                if !self_closing {
                                    cur_service_task = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // zeebe:calledDecision decisionId="…" resultVariable="…"
                            // on a business-rule task — bind the node to a native
                            // DMN decision (evaluated in-engine, no job).
                            "calledDecision" => {
                                if let (Some(idx), Some(d)) =
                                    (cur_service_task, attr(attrs, "decisionId"))
                                {
                                    acc.nodes[idx].decision_id = Some(d.to_string());
                                    acc.nodes[idx].decision_result_variable =
                                        attr(attrs, "resultVariable").map(str::to_string);
                                }
                            }
                            "userTask" => {
                                let idx = acc.add_node(attrs, NodeKind::User);
                                if !self_closing {
                                    cur_user_task = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            "callActivity" => {
                                // A call activity invokes another process. The
                                // callee id may be a `calledElement` attribute
                                // (Camunda 7) or a nested `zeebe:calledElement
                                // processId="…"` child (Camunda 8/Zeebe), captured
                                // below. Nano expands call activities inline (see
                                // ProcessDefinition::inline_call_activities) rather
                                // than executing them natively.
                                let idx = acc.add_node(attrs, NodeKind::Call);
                                if let (Some(i), Some(c)) = (idx, attr(attrs, "calledElement")) {
                                    acc.nodes[i].called_process_id = Some(c.to_string());
                                }
                                if !self_closing {
                                    cur_call = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // zeebe:calledElement processId="…" — the Camunda 8
                            // form of a call activity's callee reference.
                            "calledElement" => {
                                if let (Some(idx), Some(p)) = (cur_call, attr(attrs, "processId")) {
                                    acc.nodes[idx].called_process_id = Some(p.to_string());
                                }
                            }
                            "subProcess" => {
                                // An embedded sub-process: register it, then push
                                // its scope so nested nodes are tagged as
                                // contained in it until its end tag.
                                let idx = acc.add_node(attrs, NodeKind::SubProcess);
                                if let Some(id) = attr(attrs, "id") {
                                    if !self_closing {
                                        acc.scope_stack.push(id.to_string());
                                    }
                                }
                                if !self_closing {
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // An ad-hoc sub-process — notably the Camunda 8 agentic
                            // AI agent (`io.camunda.agenticai:aiagent-job-worker`),
                            // which carries its own zeebe:taskDefinition. At the
                            // parent token-flow level it behaves as a single
                            // job-bearing activity: the ad-hoc worker activates, runs
                            // and its outgoing flow fires on completion. The
                            // contained "tool" activities are invoked out-of-band by
                            // the worker (dynamically, not by token flow), so they are
                            // parsed into the ad-hoc scope and pruned at build time —
                            // leaving the ad-hoc itself as one Service job (type from
                            // its taskDefinition, else its id). Push its scope so any
                            // contained elements are tagged for pruning.
                            "adHocSubProcess" => {
                                let idx = acc.add_node(attrs, NodeKind::Service);
                                if let Some(i) = idx {
                                    acc.nodes[i].is_adhoc = true;
                                }
                                if !self_closing {
                                    cur_service_task = idx;
                                    if let Some(id) = attr(attrs, "id") {
                                        acc.scope_stack.push(id.to_string());
                                    }
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            "boundaryEvent" => {
                                // Buffered until end: kept only if it carries an
                                // errorEventDefinition (error boundary), a
                                // timerEventDefinition (timer boundary) or a
                                // messageEventDefinition (message boundary).
                                // `cancelActivity="false"` marks it
                                // non-interrupting (default interrupting).
                                if let Some(id) = attr(attrs, "id") {
                                    cur_boundary = Some(PendingBoundary {
                                        id: id.to_string(),
                                        attached_to: attr(attrs, "attachedToRef")
                                            .map(str::to_string),
                                        error_ref: None,
                                        timer_duration_millis: None,
                                        timer_repeating: false,
                                        timer_expr: None,
                                        message_ref: None,
                                        interrupting: attr(attrs, "cancelActivity")
                                            != Some("false"),
                                        signal_ref: None,
                                        condition: None,
                                    });
                                }
                            }
                            "errorEventDefinition" => {
                                if let Some(boundary) = cur_boundary.as_mut() {
                                    boundary.error_ref =
                                        Some(attr(attrs, "errorRef").unwrap_or("").to_string());
                                }
                            }
                            "messageEventDefinition" => {
                                // On an intermediate catch event or a boundary
                                // event, marks it a message event referencing a
                                // definitions-level <message>.
                                let message_ref =
                                    attr(attrs, "messageRef").unwrap_or("").to_string();
                                if let Some(idx) = cur_intermediate {
                                    acc.nodes[idx].message_ref = Some(message_ref);
                                } else if let Some(boundary) = cur_boundary.as_mut() {
                                    boundary.message_ref = Some(message_ref);
                                } else if let Some(idx) = cur_start {
                                    acc.nodes[idx].message_ref = Some(message_ref);
                                }
                            }
                            "signalEventDefinition" => {
                                // On an intermediate catch event or a boundary
                                // event, marks it a signal event referencing a
                                // definitions-level <signal>.
                                let signal_ref = attr(attrs, "signalRef").unwrap_or("").to_string();
                                if let Some(idx) = cur_intermediate {
                                    acc.nodes[idx].signal_ref = Some(signal_ref);
                                } else if let Some(boundary) = cur_boundary.as_mut() {
                                    boundary.signal_ref = Some(signal_ref);
                                }
                            }
                            "taskDefinition" => {
                                // zeebe:taskDefinition type="…" retries="…" inside
                                // a service task.
                                if let Some(idx) = cur_service_task {
                                    if let Some(t) = attr(attrs, "type") {
                                        acc.nodes[idx].job_type = Some(t.to_string());
                                    }
                                    if let Some(r) = attr(attrs, "retries") {
                                        acc.nodes[idx].retries = Some(r.to_string());
                                    }
                                }
                            }
                            "script" => {
                                // zeebe:script expression="…" resultVariable="…"
                                // inside a script task: an inline FEEL script the
                                // engine evaluates on activation (no job). A
                                // scriptTask carrying BOTH attributes becomes an
                                // inline ScriptTask; one that instead declares a
                                // zeebe:taskDefinition stays job-based (Service).
                                if let Some(idx) = cur_service_task {
                                    if let (Some(expr), Some(rv)) =
                                        (attr(attrs, "expression"), attr(attrs, "resultVariable"))
                                    {
                                        acc.nodes[idx].script_expression = Some(expr.to_string());
                                        acc.nodes[idx].script_result_variable =
                                            Some(rv.to_string());
                                    }
                                }
                            }
                            "assignmentDefinition" => {
                                // zeebe:assignmentDefinition inside a user task.
                                if let Some(idx) = cur_user_task {
                                    let props = &mut acc.nodes[idx].user_task;
                                    props.assignee = attr(attrs, "assignee").map(str::to_string);
                                    props.candidate_groups =
                                        attr(attrs, "candidateGroups").map(str::to_string);
                                    props.candidate_users =
                                        attr(attrs, "candidateUsers").map(str::to_string);
                                }
                            }
                            "taskSchedule" => {
                                // zeebe:taskSchedule inside a user task.
                                if let Some(idx) = cur_user_task {
                                    let props = &mut acc.nodes[idx].user_task;
                                    props.due_date = attr(attrs, "dueDate").map(str::to_string);
                                    props.follow_up_date =
                                        attr(attrs, "followUpDate").map(str::to_string);
                                }
                            }
                            "priorityDefinition" => {
                                // zeebe:priorityDefinition inside a user task
                                // (task scheduling) or a service task (job
                                // activation priority).
                                if let Some(idx) = cur_user_task {
                                    acc.nodes[idx].user_task.priority =
                                        attr(attrs, "priority").map(str::to_string);
                                } else if let Some(idx) = cur_service_task {
                                    acc.nodes[idx].job_priority =
                                        attr(attrs, "priority").map(str::to_string);
                                }
                            }
                            "adHoc" => {
                                // zeebe:adHoc inside an adHocSubProcess: the
                                // agentic wiring — outputCollection/outputElement
                                // gather each activated tool's result, and
                                // activeElementsCollection (declarative variant)
                                // names elements to activate via FEEL. The ad-hoc
                                // container is tracked as cur_service_task.
                                if let Some(idx) = cur_service_task {
                                    let n = &mut acc.nodes[idx];
                                    n.adhoc_output_collection =
                                        attr(attrs, "outputCollection").map(str::to_string);
                                    n.adhoc_output_element =
                                        attr(attrs, "outputElement").map(str::to_string);
                                    n.adhoc_active_elements =
                                        attr(attrs, "activeElementsCollection").map(str::to_string);
                                }
                            }
                            "sequenceFlow" => {
                                let idx = acc.add_flow(attrs);
                                if !self_closing {
                                    cur_flow = idx;
                                }
                            }
                            "intermediateCatchEvent" => {
                                let idx = acc.add_node(attrs, NodeKind::IntermediateCatch);
                                if !self_closing {
                                    cur_intermediate = idx;
                                }
                            }
                            // A throw event (none/escalation/signal/message throw)
                            // is a pure pass-through for path purposes: it routes
                            // straight to its outgoing flow. Any nested event
                            // definition is ignored.
                            "intermediateThrowEvent" => {
                                acc.add_node(attrs, NodeKind::IntermediateThrow);
                            }
                            // A receive task waits for a message. Nano's trace
                            // generator has no inbound correlation, so model it as
                            // a pass-through (the awaited event is assumed to
                            // arrive) rather than a perpetual block.
                            "receiveTask" => {
                                acc.add_node(attrs, NodeKind::IntermediateThrow);
                            }
                            "timeDuration"
                                if cur_intermediate.is_some()
                                    || cur_boundary.is_some()
                                    || cur_start.is_some() =>
                            {
                                duration_text = Some(String::new());
                            }
                            "timeCycle" if cur_start.is_some() || cur_boundary.is_some() => {
                                cycle_text = Some(String::new());
                            }
                            "timeDate"
                                if cur_intermediate.is_some()
                                    || cur_boundary.is_some()
                                    || cur_start.is_some() =>
                            {
                                date_text = Some(String::new());
                            }
                            "conditionExpression" if cur_flow.is_some() => {
                                condition_text = Some(String::new());
                            }
                            // A conditional event's FEEL `<condition>` (nested in
                            // a `conditionalEventDefinition` on a catch or boundary
                            // event). Distinct from a sequence flow's
                            // `conditionExpression` above.
                            "condition" if cur_intermediate.is_some() || cur_boundary.is_some() => {
                                event_condition_text = Some(String::new());
                            }
                            // A `multiInstanceLoopCharacteristics` child of an
                            // activity: mark that activity multi-instance. The
                            // FEEL-bearing details (inputCollection/inputElement/
                            // outputCollection/outputElement) live in a nested
                            // `zeebe:loopCharacteristics`; the standard BPMN
                            // `<completionCondition>` FEEL text is captured below.
                            "multiInstanceLoopCharacteristics" => {
                                if let Some(&idx) = io_stack.last() {
                                    let sequential = attr(attrs, "isSequential")
                                        .map(|v| v == "true")
                                        .unwrap_or(false);
                                    acc.nodes[idx].multi_instance =
                                        Some(crate::model::MultiInstance {
                                            sequential,
                                            ..Default::default()
                                        });
                                    if !self_closing {
                                        cur_multi_instance = Some(idx);
                                    }
                                }
                            }
                            // zeebe:loopCharacteristics inside a multi-instance
                            // activity: the input/output collection FEEL details.
                            "loopCharacteristics" => {
                                if let Some(idx) = cur_multi_instance {
                                    if let Some(mi) = acc.nodes[idx].multi_instance.as_mut() {
                                        if let Some(v) = attr(attrs, "inputCollection") {
                                            mi.input_collection = v.to_string();
                                        }
                                        mi.input_element =
                                            attr(attrs, "inputElement").map(str::to_string);
                                        mi.output_collection =
                                            attr(attrs, "outputCollection").map(str::to_string);
                                        mi.output_element =
                                            attr(attrs, "outputElement").map(str::to_string);
                                    }
                                }
                            }
                            // The standard BPMN `<completionCondition>` FEEL text
                            // nested in `multiInstanceLoopCharacteristics`, or a
                            // direct child of an ad-hoc container (ADR 0023 seam 4).
                            "completionCondition" => {
                                completion_condition_text = Some(String::new());
                            }
                            // zeebe:ioMapping and its nested zeebe:input/output.
                            // input/output are only read inside an ioMapping that
                            // belongs to an open activity (the innermost on the
                            // io_stack).
                            "ioMapping" => {
                                in_io_mapping = true;
                            }
                            "input" | "output" if in_io_mapping => {
                                if let (Some(&idx), Some(source), Some(target)) = (
                                    io_stack.last(),
                                    attr(attrs, "source"),
                                    attr(attrs, "target"),
                                ) {
                                    let mapping = crate::model::Mapping {
                                        source: source.to_string(),
                                        target: target.to_string(),
                                    };
                                    if local_name(name) == "input" {
                                        acc.nodes[idx].io.inputs.push(mapping);
                                    } else {
                                        acc.nodes[idx].io.outputs.push(mapping);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            Token::Text(text) => {
                if let Some(buf) = condition_text.as_mut() {
                    buf.push_str(text);
                }
                if let Some(buf) = event_condition_text.as_mut() {
                    buf.push_str(text);
                }
                if let Some(buf) = duration_text.as_mut() {
                    buf.push_str(text);
                }
                if let Some(buf) = cycle_text.as_mut() {
                    buf.push_str(text);
                }
                if let Some(buf) = date_text.as_mut() {
                    buf.push_str(text);
                }
                if let Some(buf) = completion_condition_text.as_mut() {
                    buf.push_str(text);
                }
            }
            Token::End { name } => match local_name(name) {
                "process" => {
                    if let Some(acc) = current.take() {
                        processes.push(acc);
                    }
                    cur_service_task = None;
                    cur_flow = None;
                    cur_boundary = None;
                    cur_intermediate = None;
                    duration_text = None;
                    cur_start = None;
                    cycle_text = None;
                    date_text = None;
                    event_condition_text = None;
                    cur_user_task = None;
                    cur_call = None;
                    completion_condition_text = None;
                    cur_multi_instance = None;
                    io_stack.clear();
                    in_io_mapping = false;
                }
                "serviceTask" => {
                    cur_service_task = None;
                    io_stack.pop();
                }
                "businessRuleTask" | "scriptTask" => {
                    cur_service_task = None;
                    io_stack.pop();
                }
                "userTask" => {
                    cur_user_task = None;
                    io_stack.pop();
                }
                "callActivity" => {
                    cur_call = None;
                    io_stack.pop();
                }
                "subProcess" => {
                    if let Some(acc) = current.as_mut() {
                        acc.scope_stack.pop();
                    }
                    io_stack.pop();
                }
                "adHocSubProcess" => {
                    if let Some(acc) = current.as_mut() {
                        acc.scope_stack.pop();
                    }
                    cur_service_task = None;
                    io_stack.pop();
                }
                "ioMapping" => in_io_mapping = false,
                "startEvent" => cur_start = None,
                "message" => cur_message = None,
                "boundaryEvent" => {
                    // Keep error boundaries (errorEventDefinition), timer
                    // boundaries (timerEventDefinition) and message boundaries
                    // (messageEventDefinition); ignore the rest.
                    if let (Some(acc), Some(boundary)) = (current.as_mut(), cur_boundary.take()) {
                        if boundary.error_ref.is_some()
                            || boundary.timer_duration_millis.is_some()
                            || boundary.timer_expr.is_some()
                            || boundary.message_ref.is_some()
                            || boundary.signal_ref.is_some()
                            || boundary.condition.is_some()
                        {
                            acc.boundaries.push(boundary);
                        }
                    }
                }
                "conditionExpression" => {
                    if let (Some(acc), Some(idx), Some(text)) =
                        (current.as_mut(), cur_flow, condition_text.take())
                    {
                        let trimmed = text.trim();
                        acc.flows[idx].condition = if trimmed.is_empty() {
                            None
                        } else {
                            Some(trimmed.to_string())
                        };
                    }
                }
                "condition" => {
                    if let Some(text) = event_condition_text.take() {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            if let (Some(acc), Some(idx)) = (current.as_mut(), cur_intermediate) {
                                acc.nodes[idx].event_condition = Some(trimmed.to_string());
                            } else if let Some(boundary) = cur_boundary.as_mut() {
                                boundary.condition = Some(trimmed.to_string());
                            }
                        }
                    }
                }
                "intermediateCatchEvent" => cur_intermediate = None,
                "multiInstanceLoopCharacteristics" => cur_multi_instance = None,
                "completionCondition" => {
                    if let Some(text) = completion_condition_text.take() {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            if let Some(acc) = current.as_mut() {
                                if let Some(idx) = cur_multi_instance {
                                    if let Some(mi) = acc.nodes[idx].multi_instance.as_mut() {
                                        mi.completion_condition = Some(trimmed.to_string());
                                    }
                                } else if let Some(id) = acc.scope_stack.last().cloned() {
                                    // A direct child of an ad-hoc container: attach
                                    // to the open container node (ADR 0023 seam 4).
                                    if let Some(node) =
                                        acc.nodes.iter_mut().find(|n| n.id == id && n.is_adhoc)
                                    {
                                        node.adhoc_completion_condition = Some(trimmed.to_string());
                                    }
                                }
                            }
                        }
                    }
                }
                "timeDuration" => {
                    if let Some(text) = duration_text.take() {
                        let trimmed = text.trim();
                        // A `=`-prefixed timeDuration is a FEEL expression
                        // evaluated at timer creation; a bare value is a static
                        // ISO-8601 literal parsed now.
                        if let Some(feel) =
                            feel_timer_expr(trimmed, crate::model::TimerDefKind::Duration)
                        {
                            if let (Some(acc), Some(idx)) = (current.as_mut(), cur_intermediate) {
                                acc.nodes[idx].timer_expr = Some(feel);
                            } else if let Some(boundary) = cur_boundary.as_mut() {
                                boundary.timer_expr = Some(feel);
                            } else if let (Some(acc), Some(idx)) = (current.as_mut(), cur_start) {
                                acc.nodes[idx].timer_expr = Some(feel);
                                acc.nodes[idx].timer_repeating = Some(false);
                            }
                        } else {
                            let millis = parse_iso8601_duration(trimmed);
                            if let (Some(acc), Some(idx)) = (current.as_mut(), cur_intermediate) {
                                // An intermediate catch event's duration.
                                acc.nodes[idx].duration_millis = millis;
                            } else if let Some(boundary) = cur_boundary.as_mut() {
                                // A timer boundary event's duration.
                                boundary.timer_duration_millis = millis;
                            } else if let (Some(acc), Some(idx)) = (current.as_mut(), cur_start) {
                                // A one-shot timer start event's duration.
                                acc.nodes[idx].duration_millis = millis;
                                acc.nodes[idx].timer_repeating = Some(false);
                            }
                        }
                    }
                }
                "timeCycle" => {
                    if let Some(text) = cycle_text.take() {
                        let trimmed = text.trim();
                        if let Some(feel) =
                            feel_timer_expr(trimmed, crate::model::TimerDefKind::Cycle)
                        {
                            if let (Some(acc), Some(idx)) = (current.as_mut(), cur_start) {
                                acc.nodes[idx].timer_expr = Some(feel);
                                acc.nodes[idx].timer_repeating = Some(true);
                            } else if let Some(boundary) = cur_boundary.as_mut() {
                                boundary.timer_expr = Some(feel);
                                boundary.timer_repeating = true;
                            }
                        } else if let (Some(acc), Some(idx)) = (current.as_mut(), cur_start) {
                            // A recurring timer start event's cycle.
                            acc.nodes[idx].duration_millis = parse_iso8601_cycle(trimmed);
                            acc.nodes[idx].timer_repeating = Some(true);
                        } else if let Some(boundary) = cur_boundary.as_mut() {
                            // A recurring (cycle) timer boundary event.
                            boundary.timer_duration_millis = parse_iso8601_cycle(trimmed);
                            boundary.timer_repeating = true;
                        }
                    }
                }
                "timeDate" => {
                    if let Some(text) = date_text.take() {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            // A timeDate is always an absolute instant, resolved
                            // at timer creation (FEEL when `=`-prefixed, else a
                            // literal ISO-8601 date-time).
                            let def = crate::model::TimerDef {
                                kind: crate::model::TimerDefKind::Date,
                                expr: trimmed.to_string(),
                            };
                            if let (Some(acc), Some(idx)) = (current.as_mut(), cur_intermediate) {
                                acc.nodes[idx].timer_expr = Some(def);
                            } else if let Some(boundary) = cur_boundary.as_mut() {
                                boundary.timer_expr = Some(def);
                            } else if let (Some(acc), Some(idx)) = (current.as_mut(), cur_start) {
                                acc.nodes[idx].timer_expr = Some(def);
                                acc.nodes[idx].timer_repeating = Some(false);
                            }
                        }
                    }
                }
                "sequenceFlow" => cur_flow = None,
                _ => {}
            },
        }
    }

    if processes.is_empty() {
        return Err(ParseError::NoProcess);
    }

    processes
        .into_iter()
        .map(|acc| acc.build(&errors, &messages, &signals))
        // Retain the verbatim source XML on each parsed definition so it can be
        // served back (getProcessDefinitionXML / console diagram). Every process
        // in one resource shares that resource's XML.
        .map(|def| {
            def.map(|d| ProcessDefinition {
                xml: xml.to_string(),
                ..d
            })
        })
        .collect()
}

/// A flow node collected while scanning, before it becomes an [`crate::Element`].
struct NodeAcc {
    id: String,
    kind: NodeKind,
    /// For service tasks: the resolved job type (defaults to the id at build).
    job_type: Option<String>,
    /// For call activities: the `calledElement` / `zeebe:calledElement processId`
    /// of the invoked process, expanded inline at assembly time.
    called_process_id: Option<String>,
    /// For service tasks: the raw `zeebe:priorityDefinition` job-priority
    /// expression (literal or FEEL), resolved at job creation. Controls
    /// activation order; `None` means no declaration (default priority).
    job_priority: Option<String>,
    /// For timer intermediate catch events: the parsed timer duration in
    /// milliseconds (from a nested `timerEventDefinition`/`timeDuration`).
    duration_millis: Option<u64>,
    /// For message intermediate catch events: the `messageRef` of a nested
    /// `messageEventDefinition`, resolved to a name/correlation key at build.
    message_ref: Option<String>,
    /// For signal intermediate catch events: the `signalRef` of a nested
    /// `signalEventDefinition`, resolved to a signal name at build.
    signal_ref: Option<String>,
    /// For timer start events: whether the timer recurs (a `timeCycle`) or is
    /// one-shot (a `timeDuration`). `None` on a plain none start event.
    timer_repeating: Option<bool>,
    /// Id of the embedded sub-process containing this node, or `None` at the
    /// process level. Set from the scope stack as the node is scanned.
    parent: Option<String>,
    /// For user tasks: the raw assignment/scheduling/priority expressions parsed
    /// from the Zeebe extension elements.
    user_task: crate::model::UserTaskProps,
    /// True for an `adHocSubProcess`: kept as a single Service job activity while
    /// its contained elements are pruned at build (see `build`).
    is_adhoc: bool,
    /// `zeebe:adHoc outputCollection` on an ad-hoc container, if declared.
    adhoc_output_collection: Option<String>,
    /// `zeebe:adHoc outputElement` FEEL expression on an ad-hoc container.
    adhoc_output_element: Option<String>,
    /// `zeebe:adHoc activeElementsCollection` FEEL expression (declarative
    /// `BpmnTask` variant) on an ad-hoc container.
    adhoc_active_elements: Option<String>,
    /// An ad-hoc container's `<completionCondition>` FEEL text, if declared.
    adhoc_completion_condition: Option<String>,
    /// For an exclusive gateway: the id of its `default="..."` sequence flow, if
    /// declared. That flow becomes the gateway's fallback (taken only when no
    /// other outgoing condition matches), regardless of document order.
    default_flow: Option<String>,
    /// The element's `zeebe:ioMapping` (input mappings applied on activation,
    /// output mappings applied on completion), populated from nested
    /// `zeebe:input`/`zeebe:output` children.
    io: crate::model::IoMapping,
    /// A FEEL timer expression (a `=`-prefixed `timeDuration`/`timeCycle` or any
    /// `timeDate`) evaluated at timer creation; `None` for a static ISO-8601
    /// literal (which populates `duration_millis` at deploy instead).
    timer_expr: Option<crate::model::TimerDef>,
    /// The raw `zeebe:taskDefinition` `retries` expression (literal or FEEL),
    /// resolved to a number at job creation; `None` for the default of 3.
    retries: Option<String>,
    /// For a script task with an inline `zeebe:script`: the FEEL expression
    /// evaluated on activation. Both this and `script_result_variable` set
    /// makes the node an inline [`ScriptTask`](crate::model::ElementKind::ScriptTask)
    /// instead of a job-based service task.
    script_expression: Option<String>,
    /// For a script task with an inline `zeebe:script`: the `resultVariable`
    /// the expression's result is stored under.
    script_result_variable: Option<String>,
    /// For a business rule task with a `zeebe:calledDecision`: the decision id
    /// (literal or FEEL expression) to evaluate natively. Its presence makes the
    /// node a [`BusinessRuleTask`](crate::model::ElementKind::BusinessRuleTask)
    /// instead of a job-based service task.
    decision_id: Option<String>,
    /// For a business rule task with a `zeebe:calledDecision`: the
    /// `resultVariable` the decision output is stored under (`None` spreads a map
    /// output into the scope).
    decision_result_variable: Option<String>,
    /// For a conditional intermediate catch event: the FEEL `condition` (from a
    /// nested `conditionalEventDefinition`/`condition`) that must become `true`
    /// for the event to fire. Makes the node a
    /// [`ConditionalIntermediateCatchEvent`](crate::model::ElementKind::ConditionalIntermediateCatchEvent).
    event_condition: Option<String>,
    /// Multi-instance loop characteristics collected from a
    /// `multiInstanceLoopCharacteristics` child plus its
    /// `zeebe:loopCharacteristics` extension; `None` for a single-instance node.
    multi_instance: Option<crate::model::MultiInstance>,
}

#[derive(Clone, Copy)]
enum NodeKind {
    Start,
    End,
    Service,
    User,
    Exclusive,
    Parallel,
    IntermediateCatch,
    IntermediateThrow,
    SubProcess,
    /// A call activity (callee in `NodeAcc::called_process_id`); expanded inline.
    Call,
}

/// A sequence flow collected while scanning.
struct FlowAcc {
    id: Option<String>,
    source: Option<String>,
    target: Option<String>,
    condition: Option<String>,
}

/// A boundary event collected while scanning. An `error_ref` (resolved to an
/// error code at build time) makes it an error boundary; a `timer_duration_millis`
/// makes it a timer boundary; a `message_ref` (resolved to a message
/// name/correlation key) makes it a message boundary. `interrupting` reflects the
/// BPMN `cancelActivity` attribute (default `true`); error boundaries are always
/// interrupting. A boundary with none of these is ignored.
#[derive(Clone)]
struct PendingBoundary {
    id: String,
    attached_to: Option<String>,
    error_ref: Option<String>,
    timer_duration_millis: Option<u64>,
    timer_repeating: bool,
    /// A FEEL timer expression on this boundary timer (see [`NodeAcc::timer_expr`]).
    timer_expr: Option<crate::model::TimerDef>,
    message_ref: Option<String>,
    signal_ref: Option<String>,
    /// A FEEL `condition` (from a nested `conditionalEventDefinition`/`condition`)
    /// that must become `true` for this boundary to fire. Makes it a
    /// [`ConditionalBoundaryEvent`](crate::model::ElementKind::ConditionalBoundaryEvent).
    condition: Option<String>,
    interrupting: bool,
}

/// A definitions-level `<message>` declaration: its `name` and the instance
/// variable named by a nested `zeebe:subscription correlationKey`.
#[derive(Clone)]
struct MessageDecl {
    name: String,
    correlation_key: Option<String>,
}

/// Accumulates the nodes and flows of one `<process>` as it is scanned.
struct ProcessAcc {
    id: String,
    nodes: Vec<NodeAcc>,
    flows: Vec<FlowAcc>,
    boundaries: Vec<PendingBoundary>,
    /// Stack of open embedded sub-process ids, used to scope nested nodes.
    scope_stack: Vec<String>,
}

impl ProcessAcc {
    fn new(id: String) -> Self {
        Self {
            id,
            nodes: Vec::new(),
            flows: Vec::new(),
            boundaries: Vec::new(),
            scope_stack: Vec::new(),
        }
    }

    /// Adds a flow node; returns its index, or `None` if it had no `id`.
    fn add_node(&mut self, attrs: &[(String, String)], kind: NodeKind) -> Option<usize> {
        let id = attr(attrs, "id")?;
        self.nodes.push(NodeAcc {
            id: id.to_string(),
            kind,
            job_type: None,
            called_process_id: None,
            job_priority: None,
            duration_millis: None,
            message_ref: None,
            signal_ref: None,
            timer_repeating: None,
            parent: self.scope_stack.last().cloned(),
            user_task: crate::model::UserTaskProps::default(),
            is_adhoc: false,
            adhoc_output_collection: None,
            adhoc_output_element: None,
            adhoc_active_elements: None,
            adhoc_completion_condition: None,
            default_flow: None,
            io: crate::model::IoMapping::default(),
            timer_expr: None,
            retries: None,
            script_expression: None,
            script_result_variable: None,
            decision_id: None,
            decision_result_variable: None,
            event_condition: None,
            multi_instance: None,
        });
        Some(self.nodes.len() - 1)
    }

    /// Adds a sequence flow; returns its index.
    fn add_flow(&mut self, attrs: &[(String, String)]) -> Option<usize> {
        self.flows.push(FlowAcc {
            id: attr(attrs, "id").map(str::to_string),
            source: attr(attrs, "sourceRef").map(str::to_string),
            target: attr(attrs, "targetRef").map(str::to_string),
            condition: None,
        });
        Some(self.flows.len() - 1)
    }

    /// Assembles the [`ProcessDefinition`] via [`ProcessBuilder`].
    ///
    /// `errors` maps definitions-level `<error>` ids to their codes, used to
    /// resolve each boundary event's `errorRef`. `messages` maps `<message>` ids
    /// to their name and correlation-key variable, used to resolve message
    /// intermediate catch and boundary events.
    fn build(
        mut self,
        errors: &HashMap<String, String>,
        messages: &HashMap<String, MessageDecl>,
        signals: &HashMap<String, String>,
    ) -> Result<ProcessDefinition, ParseError> {
        // Ad-hoc sub-processes are kept as a single Service job activity; the
        // elements they contain (agent "tools", invoked out-of-band rather than by
        // token flow) are pruned from the executable graph, along with any
        // sequence flows or boundary events that reference them. A node is pruned
        // when its parent chain reaches an ad-hoc node, so nesting at any depth is
        // handled. Before pruning, the tools + `zeebe:adHoc` wiring are captured
        // into `adhoc_catalog` as non-executable metadata so the Camunda agentic
        // activate-element contract can be honoured later (ADR 0023) without
        // re-parsing.
        let mut adhoc_catalog: Vec<crate::model::AdHocSubProcessDef> = Vec::new();
        let adhoc_ids: std::collections::HashSet<String> = self
            .nodes
            .iter()
            .filter(|n| n.is_adhoc)
            .map(|n| n.id.clone())
            .collect();
        if !adhoc_ids.is_empty() {
            let parent_of: HashMap<&str, &str> = self
                .nodes
                .iter()
                .filter_map(|n| n.parent.as_deref().map(|p| (n.id.as_str(), p)))
                .collect();
            let inside_adhoc = |id: &str| -> bool {
                let mut cur = parent_of.get(id).copied();
                while let Some(p) = cur {
                    if adhoc_ids.contains(p) {
                        return true;
                    }
                    cur = parent_of.get(p).copied();
                }
                false
            };
            // The nearest enclosing ad-hoc container of `id`, if any.
            let nearest_adhoc = |id: &str| -> Option<String> {
                let mut cur = parent_of.get(id).copied();
                while let Some(p) = cur {
                    if adhoc_ids.contains(p) {
                        return Some(p.to_string());
                    }
                    cur = parent_of.get(p).copied();
                }
                None
            };
            let pruned: std::collections::HashSet<String> = self
                .nodes
                .iter()
                .filter(|n| inside_adhoc(&n.id))
                .map(|n| n.id.clone())
                .collect();

            // One catalog entry per ad-hoc container, in document order.
            let mut index: HashMap<String, usize> = HashMap::new();
            for n in self.nodes.iter().filter(|n| n.is_adhoc) {
                index.insert(n.id.clone(), adhoc_catalog.len());
                // A container backed by a job (taskDefinition) is the agentic
                // JOB_WORKER variant; one that only declares an
                // activeElementsCollection is the declarative BPMN_TASK variant.
                let impl_type = if n.job_type.is_none() && n.adhoc_active_elements.is_some() {
                    crate::model::AdHocImplementationType::BpmnTask
                } else {
                    crate::model::AdHocImplementationType::JobWorker
                };
                adhoc_catalog.push(crate::model::AdHocSubProcessDef {
                    container_id: n.id.clone(),
                    impl_type,
                    completion_condition: n.adhoc_completion_condition.clone(),
                    active_elements_collection: n.adhoc_active_elements.clone(),
                    output_collection: n.adhoc_output_collection.clone(),
                    output_element: n.adhoc_output_element.clone(),
                    tools: Vec::new(),
                });
            }
            // Assign each pruned tool to its nearest ad-hoc container, preserving
            // document order.
            for n in self.nodes.iter().filter(|n| pruned.contains(&n.id)) {
                let kind = match n.kind {
                    NodeKind::Service => crate::model::AdHocToolKind::ServiceTask {
                        job_type: n.job_type.clone().unwrap_or_else(|| n.id.clone()),
                    },
                    NodeKind::User => crate::model::AdHocToolKind::UserTask,
                    NodeKind::Call => crate::model::AdHocToolKind::CallActivity {
                        process_id: n.called_process_id.clone(),
                    },
                    _ => crate::model::AdHocToolKind::Other,
                };
                if let Some(pos) = nearest_adhoc(&n.id).and_then(|c| index.get(&c).copied()) {
                    adhoc_catalog[pos].tools.push(crate::model::AdHocTool {
                        element_id: n.id.clone(),
                        kind,
                        io: n.io.clone(),
                    });
                }
            }

            // ADR 0023 seam 2 (runtime): the inner "tool" activities are pruned
            // from the executable graph — an ad-hoc container is flattened to a
            // single job-bearing activity, so its tools are never reached by
            // ordinary token flow. They are NOT lost: the catalog above captures
            // each tool's id + kind (job type), which is all the runtime needs to
            // activate one when the agent's job result requests it (the container
            // scope + activate-element seeding drive execution, not the flat
            // element graph). Keeping tools out of `self.nodes`/`flows` also keeps
            // `ProcessDefinition.elements` — and thus the processos model
            // round-trip — identical to a plain container, avoiding a modeler
            // cascade over arbitrary inner activities.
            if !pruned.is_empty() {
                self.nodes.retain(|n| !pruned.contains(&n.id));
                self.flows.retain(|f| {
                    let keep = |o: &Option<String>| {
                        o.as_ref().map(|s| !pruned.contains(s)).unwrap_or(true)
                    };
                    keep(&f.source) && keep(&f.target)
                });
                self.boundaries.retain(|b| {
                    b.attached_to
                        .as_ref()
                        .map(|a| !pruned.contains(a))
                        .unwrap_or(true)
                });
            }
        }

        // A process may declare more than one start event (e.g. a "refresh batch"
        // and a "manual intake" start that merge downstream). The engine begins an
        // instance at a single process-level start event, so designate one — a
        // plain none start preferred, tie-broken by id — and demote the surplus
        // process-level starts to inert throw events. They keep their outgoing
        // flow (so it still has a valid source) but, lacking any incoming flow,
        // are never activated; instances created via CreateInstance begin at the
        // designated start.
        let proc_starts: Vec<usize> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| matches!(n.kind, NodeKind::Start) && n.parent.is_none())
            .map(|(i, _)| i)
            .collect();
        if proc_starts.len() > 1 {
            let is_none_start =
                |n: &NodeAcc| n.message_ref.is_none() && n.timer_repeating.is_none();
            let designated = proc_starts
                .iter()
                .copied()
                .filter(|&i| is_none_start(&self.nodes[i]))
                .min_by(|&a, &b| self.nodes[a].id.cmp(&self.nodes[b].id))
                .unwrap_or_else(|| {
                    proc_starts
                        .iter()
                        .copied()
                        .min_by(|&a, &b| self.nodes[a].id.cmp(&self.nodes[b].id))
                        .expect("non-empty")
                });
            for &i in &proc_starts {
                if i != designated {
                    self.nodes[i].kind = NodeKind::IntermediateThrow;
                    self.nodes[i].message_ref = None;
                    self.nodes[i].timer_repeating = None;
                    self.nodes[i].duration_millis = None;
                }
            }
        }

        let mut builder = ProcessBuilder::new(self.id.clone());
        // Map each sub-process to its inner start event (a start node whose
        // parent is the sub-process).
        let sub_starts: HashMap<String, String> = self
            .nodes
            .iter()
            .filter(|n| matches!(n.kind, NodeKind::Start))
            .filter_map(|n| n.parent.clone().map(|p| (p, n.id.clone())))
            .collect();
        // Ids of sequence flows declared as a gateway `default` flow.
        let mut default_flow_ids: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for node in self.nodes {
            let node_id = node.id.clone();
            let io_id = node.id.clone();
            let node_io = node.io.clone();
            let timer_id = node.id.clone();
            let node_timer = node.timer_expr.clone();
            let retries_id = node.id.clone();
            let node_retries = node.retries.clone();
            let mi_id = node.id.clone();
            // Only treat as multi-instance when an input collection was actually
            // declared; a bare `multiInstanceLoopCharacteristics` with no
            // `zeebe:loopCharacteristics inputCollection` degenerates to an
            // ordinary single-instance activity.
            let node_mi = node
                .multi_instance
                .clone()
                .filter(|mi| !mi.input_collection.trim().is_empty());
            let parent = node.parent.clone();
            if let Some(d) = node.default_flow.clone() {
                default_flow_ids.insert(d);
            }
            builder = match node.kind {
                NodeKind::Start => {
                    // A messageRef makes it a message start; a timer_repeating
                    // flag makes it a timer start (cycle or one-shot); otherwise
                    // a plain none start event.
                    if let Some(message_ref) = node.message_ref {
                        let decl = messages.get(&message_ref).ok_or_else(|| {
                            ParseError::InvalidMessageEvent {
                                process_id: self.id.clone(),
                                reason: format!(
                                    "start event {} references unknown message '{message_ref}'",
                                    node.id
                                ),
                            }
                        })?;
                        builder.message_start_event(node.id, decl.name.clone())
                    } else if let Some(repeating) = node.timer_repeating {
                        let interval_millis = node.duration_millis.unwrap_or(0);
                        if repeating {
                            builder.timer_start_event_cycle(node.id, interval_millis)
                        } else {
                            builder.timer_start_event_once(node.id, interval_millis)
                        }
                    } else {
                        builder.start_event(node.id)
                    }
                }
                NodeKind::End => builder.end_event(node.id),
                NodeKind::IntermediateThrow => builder.intermediate_throw_event(node.id),
                NodeKind::Exclusive => builder.exclusive_gateway(node.id),
                NodeKind::Parallel => builder.parallel_gateway(node.id),
                NodeKind::Service => {
                    // A scriptTask carrying an inline zeebe:script (expression +
                    // resultVariable) is an inline-FEEL script task, evaluated on
                    // activation with no job. A businessRuleTask carrying a
                    // zeebe:calledDecision is a native DMN business rule task,
                    // evaluated on activation with no job. Otherwise it is an
                    // ordinary job-based service task.
                    if let (Some(expr), Some(rv)) = (
                        node.script_expression.clone(),
                        node.script_result_variable.clone(),
                    ) {
                        builder.script_task(node.id, expr, rv)
                    } else if let Some(decision_id) = node.decision_id.clone() {
                        builder.business_rule_task(
                            node.id,
                            decision_id,
                            node.decision_result_variable.clone(),
                        )
                    } else {
                        let job_type = node.job_type.unwrap_or_else(|| node.id.clone());
                        builder.service_task_with_priority(node.id, job_type, node.job_priority)
                    }
                }
                NodeKind::User => builder.user_task_with(node.id, node.user_task),
                NodeKind::IntermediateCatch => {
                    // Ordering: a conditional catch (event_condition) has no
                    // message/signal/timer ref; a messageRef makes it a message
                    // catch; a signalRef a signal catch; otherwise a timer catch
                    // carrying a (possibly zero) duration.
                    if let Some(condition) = node.event_condition {
                        builder.conditional_intermediate_catch_event(node.id, condition)
                    } else if let Some(message_ref) = node.message_ref {
                        let decl = messages.get(&message_ref).ok_or_else(|| {
                            ParseError::InvalidMessageEvent {
                                process_id: self.id.clone(),
                                reason: format!(
                                    "intermediate catch event {} references unknown message '{message_ref}'",
                                    node.id
                                ),
                            }
                        })?;
                        let correlation_key = decl.correlation_key.clone().ok_or_else(|| {
                            ParseError::InvalidMessageEvent {
                                process_id: self.id.clone(),
                                reason: format!(
                                    "message '{message_ref}' has no zeebe:subscription correlationKey"
                                ),
                            }
                        })?;
                        builder.message_intermediate_catch_event(
                            node.id,
                            decl.name.clone(),
                            correlation_key,
                        )
                    } else if let Some(signal_ref) = node.signal_ref {
                        let name = signals.get(&signal_ref).cloned().ok_or_else(|| {
                            ParseError::InvalidProcess {
                                process_id: self.id.clone(),
                                reason: format!(
                                    "intermediate catch event {} references unknown signal '{signal_ref}'",
                                    node.id
                                ),
                            }
                        })?;
                        builder.signal_intermediate_catch_event(node.id, name)
                    } else {
                        let duration_millis = node.duration_millis.unwrap_or(0);
                        builder.timer_intermediate_catch_event(node.id, duration_millis)
                    }
                }
                NodeKind::SubProcess => {
                    let start = sub_starts.get(&node.id).cloned().ok_or_else(|| {
                        ParseError::InvalidProcess {
                            process_id: self.id.clone(),
                            reason: format!("sub-process {} has no start event", node.id),
                        }
                    })?;
                    builder.sub_process(node.id, start)
                }
                NodeKind::Call => {
                    // The callee is required; an unresolved call activity is a
                    // parse error so it never silently becomes an inert step.
                    let called = node.called_process_id.clone().ok_or_else(|| {
                        ParseError::InvalidProcess {
                            process_id: self.id.clone(),
                            reason: format!("call activity {} has no calledElement", node.id),
                        }
                    })?;
                    builder.call_activity(node.id, called)
                }
            };
            if let Some(parent) = parent {
                builder = builder.contained_in(node_id, parent);
            }
            if !node_io.is_empty() {
                builder = builder.with_io(io_id, node_io);
            }
            if let Some(timer) = node_timer {
                builder = builder.with_timer(timer_id, timer);
            }
            if let Some(retries) = node_retries {
                builder = builder.with_retries(retries_id, retries);
            }
            if let Some(mi) = node_mi {
                builder = builder.with_multi_instance(mi_id, mi);
            }
        }
        for boundary in self.boundaries {
            let attached_to =
                boundary
                    .attached_to
                    .ok_or_else(|| ParseError::InvalidBoundaryEvent {
                        process_id: self.id.clone(),
                        reason: format!("boundary event {} has no attachedToRef", boundary.id),
                    })?;
            // Resolve the boundary's flavour: conditional (condition), timer
            // (duration), message (messageRef), signal (signalRef), or error
            // (errorRef -> declared error).
            if let Some(condition) = boundary.condition {
                builder = if boundary.interrupting {
                    builder.conditional_boundary_event(boundary.id, attached_to, condition)
                } else {
                    builder.non_interrupting_conditional_boundary_event(
                        boundary.id,
                        attached_to,
                        condition,
                    )
                };
            } else if boundary.timer_duration_millis.is_some() || boundary.timer_expr.is_some() {
                // A FEEL timer boundary carries no static duration; build the
                // element with a zero fallback and attach the expression below.
                let duration_millis = boundary.timer_duration_millis.unwrap_or(0);
                let boundary_id = boundary.id.clone();
                let timer_expr = boundary.timer_expr.clone();
                builder = if boundary.interrupting {
                    // An interrupting timer fires once and cancels its activity,
                    // so a cycle is treated as a one-shot at the interval.
                    builder.timer_boundary_event(boundary.id, attached_to, duration_millis)
                } else if boundary.timer_repeating {
                    builder.non_interrupting_timer_cycle_boundary_event(
                        boundary.id,
                        attached_to,
                        duration_millis,
                    )
                } else {
                    builder.non_interrupting_timer_boundary_event(
                        boundary.id,
                        attached_to,
                        duration_millis,
                    )
                };
                if let Some(timer) = timer_expr {
                    builder = builder.with_timer(boundary_id, timer);
                }
            } else if let Some(message_ref) = boundary.message_ref {
                let decl =
                    messages
                        .get(&message_ref)
                        .ok_or_else(|| ParseError::InvalidMessageEvent {
                            process_id: self.id.clone(),
                            reason: format!(
                                "boundary event {} references unknown message '{message_ref}'",
                                boundary.id
                            ),
                        })?;
                let correlation_key = decl.correlation_key.clone().ok_or_else(|| {
                    ParseError::InvalidMessageEvent {
                        process_id: self.id.clone(),
                        reason: format!(
                            "message '{message_ref}' has no zeebe:subscription correlationKey"
                        ),
                    }
                })?;
                builder = if boundary.interrupting {
                    builder.message_boundary_event(
                        boundary.id,
                        attached_to,
                        decl.name.clone(),
                        correlation_key,
                    )
                } else {
                    builder.non_interrupting_message_boundary_event(
                        boundary.id,
                        attached_to,
                        decl.name.clone(),
                        correlation_key,
                    )
                };
            } else if let Some(signal_ref) = boundary.signal_ref {
                let name = signals.get(&signal_ref).cloned().ok_or_else(|| {
                    ParseError::InvalidBoundaryEvent {
                        process_id: self.id.clone(),
                        reason: format!(
                            "boundary event {} references unknown signal '{signal_ref}'",
                            boundary.id
                        ),
                    }
                })?;
                builder = if boundary.interrupting {
                    builder.signal_boundary_event(boundary.id, attached_to, name)
                } else {
                    builder.non_interrupting_signal_boundary_event(boundary.id, attached_to, name)
                };
            } else {
                let error_ref = boundary.error_ref.unwrap_or_default();
                let error_code = errors.get(&error_ref).cloned().ok_or_else(|| {
                    ParseError::InvalidBoundaryEvent {
                        process_id: self.id.clone(),
                        reason: format!(
                            "boundary event {} references unknown error '{error_ref}'",
                            boundary.id
                        ),
                    }
                })?;
                builder = builder.error_boundary_event(boundary.id, attached_to, error_code);
            }
        }
        for flow in self.flows {
            let (source, target) = match (flow.source, flow.target) {
                (Some(s), Some(t)) => (s, t),
                _ => {
                    return Err(ParseError::IncompleteSequenceFlow {
                        process_id: self.id,
                    })
                }
            };
            let is_default = flow
                .id
                .as_deref()
                .is_some_and(|id| default_flow_ids.contains(id));
            builder = match (is_default, flow.condition) {
                (true, _) => builder.connect_default(source, target),
                (false, Some(expression)) => builder.connect_when(source, target, expression),
                (false, None) => builder.connect(source, target),
            };
        }
        let mut def = builder.build().map_err(|e| ParseError::InvalidProcess {
            process_id: self.id,
            reason: e.to_string(),
        })?;
        def.adhoc = adhoc_catalog;
        Ok(def)
    }
}

/// Returns a FEEL [`crate::model::TimerDef`] of `kind` when `trimmed` is a FEEL
/// expression (a leading `=`), otherwise `None` (the caller treats it as a
/// static ISO-8601 literal).
fn feel_timer_expr(
    trimmed: &str,
    kind: crate::model::TimerDefKind,
) -> Option<crate::model::TimerDef> {
    if trimmed.starts_with('=') {
        Some(crate::model::TimerDef {
            kind,
            expr: trimmed.to_string(),
        })
    } else {
        None
    }
}

/// Parses an ISO-8601 duration (e.g. `PT5S`, `PT1M30S`, `PT2H`, `P1DT6H`,
/// `P1W`) into milliseconds. Supports weeks, days, hours, minutes and seconds
/// (the date-portion years/months are ambiguous in length and not supported).
/// Returns `None` if the string is not a recognisable duration.
pub(crate) fn parse_iso8601_duration(raw: &str) -> Option<u64> {
    let s = raw.trim();
    let s = s.strip_prefix('P')?;
    if s.is_empty() {
        return None;
    }

    let mut total_millis: u64 = 0;
    let mut in_time = false;
    let mut num = String::new();
    let mut saw_unit = false;

    for c in s.chars() {
        match c {
            'T' => in_time = true,
            '0'..='9' => num.push(c),
            _ => {
                if num.is_empty() {
                    return None;
                }
                let value: u64 = num.parse().ok()?;
                num.clear();
                let millis = match (in_time, c) {
                    (false, 'W') => value.checked_mul(7 * 24 * 60 * 60 * 1000),
                    (false, 'D') => value.checked_mul(24 * 60 * 60 * 1000),
                    (true, 'H') => value.checked_mul(60 * 60 * 1000),
                    (true, 'M') => value.checked_mul(60 * 1000),
                    (true, 'S') => value.checked_mul(1000),
                    // 'M' before 'T' is months (unsupported) and 'Y' is years.
                    _ => return None,
                }?;
                total_millis = total_millis.checked_add(millis)?;
                saw_unit = true;
            }
        }
    }

    // Trailing digits without a unit, or no units at all, are invalid.
    if !num.is_empty() || !saw_unit {
        return None;
    }
    Some(total_millis)
}

/// Parses an ISO-8601 repeating interval (a BPMN `timeCycle`, e.g. `R/PT1H` or
/// `R5/PT1H`) into the interval in milliseconds. The `Rn` repetition-count
/// prefix is accepted but ignored (the engine repeats unboundedly). A bare
/// duration without the `R[n]/` prefix is also accepted. Returns `None` if the
/// interval portion is not a recognisable duration.
pub(crate) fn parse_iso8601_cycle(raw: &str) -> Option<u64> {
    let s = raw.trim();
    let interval = match s.split_once('/') {
        Some((repeat, interval)) if repeat.starts_with('R') => interval,
        Some(_) => return None,
        None => s,
    };
    parse_iso8601_duration(interval)
}

/// Parses a `zeebe:subscription correlationKey` expression into the name of an
/// instance variable: strips the leading FEEL `=` marker and trims. Returns
/// `None` if the result is empty.
fn parse_correlation_key(raw: &str) -> Option<String> {
    let expr = raw.trim();
    let expr = expr.strip_prefix('=').unwrap_or(expr).trim();
    if expr.is_empty() {
        None
    } else {
        Some(expr.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Condition;
    use crate::model::ElementKind;

    const ORDER_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="order" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="charge">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="payment" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="done" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="charge" />
    <bpmn:sequenceFlow id="f2" sourceRef="charge" targetRef="done" />
  </bpmn:process>
</bpmn:definitions>"#;

    #[test]
    fn should_parse_a_timer_intermediate_catch_event() {
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="delayed">
              <bpmn:startEvent id="s" />
              <bpmn:intermediateCatchEvent id="wait">
                <bpmn:timerEventDefinition>
                  <bpmn:timeDuration>PT1M30S</bpmn:timeDuration>
                </bpmn:timerEventDefinition>
              </bpmn:intermediateCatchEvent>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="wait" />
              <bpmn:sequenceFlow id="b" sourceRef="wait" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];

        assert_eq!(
            def.element("wait").unwrap().kind,
            ElementKind::TimerIntermediateCatchEvent {
                duration_millis: 90_000
            }
        );
        assert_eq!(def.element("wait").unwrap().outgoing[0].to, "e");
    }

    #[test]
    fn should_parse_iso8601_durations() {
        assert_eq!(parse_iso8601_duration("PT5S"), Some(5_000));
        assert_eq!(parse_iso8601_duration("PT1M"), Some(60_000));
        assert_eq!(parse_iso8601_duration("PT2H"), Some(7_200_000));
        assert_eq!(parse_iso8601_duration("P1D"), Some(86_400_000));
        assert_eq!(parse_iso8601_duration("P1W"), Some(604_800_000));
        assert_eq!(parse_iso8601_duration("P1DT6H30M"), Some(109_800_000));
        assert_eq!(parse_iso8601_duration(" PT10S "), Some(10_000));
        // invalid / unsupported
        assert_eq!(parse_iso8601_duration("5S"), None);
        assert_eq!(parse_iso8601_duration("P"), None);
        assert_eq!(parse_iso8601_duration("PT"), None);
        assert_eq!(parse_iso8601_duration("P1Y"), None);
        assert_eq!(parse_iso8601_duration("PT5"), None);
    }

    #[test]
    fn should_parse_a_linear_process_with_a_service_task() {
        // given / when
        let defs = parse_bpmn(ORDER_BPMN).unwrap();

        // then
        assert_eq!(defs.len(), 1);
        let def = &defs[0];
        assert_eq!(def.id, "order");
        assert_eq!(def.start_event, "start");
        assert_eq!(
            def.element("charge").unwrap().kind,
            ElementKind::ServiceTask {
                job_type: "payment".to_string(),
                priority: None,
            }
        );
        assert_eq!(def.element("start").unwrap().outgoing[0].to, "charge");
    }

    #[test]
    fn should_parse_zeebe_task_definition_retries() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="retryable" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="work">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="do-work" retries="=maxRetries" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="done" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="work" />
    <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="done" />
  </bpmn:process>
</bpmn:definitions>"#;
        let defs = parse_bpmn(xml).unwrap();
        let def = &defs[0];
        // The retries expression is captured verbatim (with its `=` prefix) for
        // evaluation at job creation; the type still drives the job type.
        assert_eq!(
            def.element("work").unwrap().retries.as_deref(),
            Some("=maxRetries")
        );
        assert_eq!(
            def.element("work").unwrap().kind,
            ElementKind::ServiceTask {
                job_type: "do-work".to_string(),
                priority: None,
            }
        );
    }

    #[test]
    fn should_parse_called_decision_as_a_business_rule_task() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="rules" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:businessRuleTask id="decide">
      <bpmn:extensionElements>
        <zeebe:calledDecision decisionId="rating" resultVariable="score" />
      </bpmn:extensionElements>
    </bpmn:businessRuleTask>
    <bpmn:endEvent id="done" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="decide" />
    <bpmn:sequenceFlow id="f2" sourceRef="decide" targetRef="done" />
  </bpmn:process>
</bpmn:definitions>"#;
        let defs = parse_bpmn(xml).unwrap();
        let def = &defs[0];
        // A businessRuleTask carrying a zeebe:calledDecision becomes a native
        // BusinessRuleTask (evaluated in-engine), not a job-based service task.
        assert_eq!(
            def.element("decide").unwrap().kind,
            ElementKind::BusinessRuleTask {
                decision_id: "rating".to_string(),
                result_variable: Some("score".to_string()),
            }
        );
    }

    #[test]
    fn should_parse_business_rule_task_with_task_definition_as_a_service_task() {
        // A businessRuleTask that instead declares a job worker (zeebe:taskDefinition)
        // stays a job-based service task — only calledDecision makes it native.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="rules2" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:businessRuleTask id="decide">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="ruler" />
      </bpmn:extensionElements>
    </bpmn:businessRuleTask>
    <bpmn:endEvent id="done" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="decide" />
    <bpmn:sequenceFlow id="f2" sourceRef="decide" targetRef="done" />
  </bpmn:process>
</bpmn:definitions>"#;
        let defs = parse_bpmn(xml).unwrap();
        assert_eq!(
            defs[0].element("decide").unwrap().kind,
            ElementKind::ServiceTask {
                job_type: "ruler".to_string(),
                priority: None,
            }
        );
    }

    #[test]
    fn should_parse_zeebe_script_as_an_inline_script_task() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="scripted" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:scriptTask id="calc">
      <bpmn:extensionElements>
        <zeebe:script expression="=a + b" resultVariable="sum" />
      </bpmn:extensionElements>
    </bpmn:scriptTask>
    <bpmn:endEvent id="done" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="calc" />
    <bpmn:sequenceFlow id="f2" sourceRef="calc" targetRef="done" />
  </bpmn:process>
</bpmn:definitions>"#;
        let defs = parse_bpmn(xml).unwrap();
        let def = &defs[0];
        // A scriptTask carrying a zeebe:script becomes an inline ScriptTask, not
        // a job-based service task; the expression is captured verbatim.
        assert_eq!(
            def.element("calc").unwrap().kind,
            ElementKind::ScriptTask {
                expression: "=a + b".to_string(),
                result_variable: "sum".to_string(),
            }
        );
    }

    #[test]
    fn should_parse_a_script_task_with_task_definition_as_a_job() {
        // A scriptTask that declares a zeebe:taskDefinition (no zeebe:script) is
        // job-based, exactly like a service task.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="scripted-job" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:scriptTask id="calc">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="run-script" />
      </bpmn:extensionElements>
    </bpmn:scriptTask>
    <bpmn:endEvent id="done" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="calc" />
    <bpmn:sequenceFlow id="f2" sourceRef="calc" targetRef="done" />
  </bpmn:process>
</bpmn:definitions>"#;
        let defs = parse_bpmn(xml).unwrap();
        let def = &defs[0];
        assert_eq!(
            def.element("calc").unwrap().kind,
            ElementKind::ServiceTask {
                job_type: "run-script".to_string(),
                priority: None,
            }
        );
    }

    #[test]
    fn should_parse_an_adhoc_subprocess_as_a_single_job_and_prune_its_tools() {
        // given: the Camunda 8 agentic AI-agent shape — an <adHocSubProcess> that
        // carries its own zeebe:taskDefinition and contains "tool" activities the
        // worker invokes out-of-band (not by token flow).
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:adHocSubProcess id="agent" name="Agentic investigation">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="io.camunda.agenticai:aiagent-job-worker:1" />
                  <zeebe:adHoc outputCollection="toolCallResults"
                               outputElement="={ id: toolCall._meta.id }" />
                </bpmn:extensionElements>
                <bpmn:serviceTask id="tool_issue_credit">
                  <bpmn:extensionElements>
                    <zeebe:taskDefinition type="io.camunda:http-json:1" />
                  </bpmn:extensionElements>
                </bpmn:serviceTask>
                <bpmn:userTask id="tool_review" />
                <bpmn:exclusiveGateway id="tool_gw" />
                <bpmn:sequenceFlow id="t1" sourceRef="tool_review" targetRef="tool_gw" />
              </bpmn:adHocSubProcess>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
              <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then: the ad-hoc sub-process is one Service job (type from its own
        // taskDefinition), wired into the parent flow s -> agent -> e.
        assert_eq!(
            def.element("agent").unwrap().kind,
            ElementKind::ServiceTask {
                job_type: "io.camunda.agenticai:aiagent-job-worker:1".to_string(),
                priority: None,
            }
        );
        assert_eq!(def.element("s").unwrap().outgoing[0].to, "agent");
        assert_eq!(def.element("agent").unwrap().outgoing[0].to, "e");
        // and: the contained tool activities (and their internal flow) are pruned
        // from the executable graph.
        assert!(def.element("tool_issue_credit").is_none());
        assert!(def.element("tool_review").is_none());
        assert!(def.element("tool_gw").is_none());

        // and: the ad-hoc tool catalog + zeebe:adHoc wiring is retained as
        // metadata (ADR 0023 Tier-1 substrate).
        assert_eq!(def.adhoc.len(), 1);
        let cat = &def.adhoc[0];
        assert_eq!(cat.container_id, "agent");
        assert_eq!(
            cat.impl_type,
            crate::model::AdHocImplementationType::JobWorker
        );
        assert_eq!(cat.output_collection.as_deref(), Some("toolCallResults"));
        assert_eq!(
            cat.output_element.as_deref(),
            Some("={ id: toolCall._meta.id }")
        );
        // Tools are captured in document order with their kinds; the inner
        // gateway is not an activatable tool and is captured as `Other`.
        let ids: Vec<&str> = cat.tools.iter().map(|t| t.element_id.as_str()).collect();
        assert_eq!(ids, vec!["tool_issue_credit", "tool_review", "tool_gw"]);
        assert_eq!(
            cat.tools[0].kind,
            crate::model::AdHocToolKind::ServiceTask {
                job_type: "io.camunda:http-json:1".to_string()
            }
        );
        assert_eq!(cat.tools[1].kind, crate::model::AdHocToolKind::UserTask);
        assert_eq!(cat.tools[2].kind, crate::model::AdHocToolKind::Other);
    }

    #[test]
    fn should_retain_the_tool_catalog_from_the_camunda_golden_fixture() {
        // The unmodified Camunda AI Agent ad-hoc example (see
        // engine-core/tests/fixtures/adhoc-agent/README.md). It must parse and
        // expose its JOB_WORKER ad-hoc container + tool catalog.
        let xml = include_str!(
            "../tests/fixtures/adhoc-agent/ai-agent-chat-with-tools/ai-agent-chat-with-tools.bpmn"
        );
        let defs = parse_bpmn(xml).unwrap();
        let def = defs
            .iter()
            .find(|d| d.adhoc.iter().any(|a| a.container_id == "AI_Agent"))
            .expect("the AI_Agent ad-hoc container should be catalogued");
        let cat = def
            .adhoc
            .iter()
            .find(|a| a.container_id == "AI_Agent")
            .unwrap();
        assert_eq!(
            cat.impl_type,
            crate::model::AdHocImplementationType::JobWorker
        );
        assert_eq!(cat.output_collection.as_deref(), Some("toolCallResults"));
        // The fixture's tool set (see the fixtures README table). These are the
        // activities nested inside the AI_Agent ad-hoc container.
        for tool in [
            "LoadUserByID",
            "ListUsers",
            "Search_Recipe",
            "GetDateAndTime",
            "SuperfluxProduct",
            "SendEmail",
            "Jokes_API",
            "Fetch_URL",
            "AskHumanToSendEmail",
            "Handle_Message",
        ] {
            assert!(
                cat.tools.iter().any(|t| t.element_id == tool),
                "tool {tool} should be in the catalog"
            );
        }
        // The agent container is still one job in the executable graph, and the
        // tools are pruned from it.
        assert!(matches!(
            def.element("AI_Agent").unwrap().kind,
            ElementKind::ServiceTask { .. }
        ));
        assert!(def.element("Search_Recipe").is_none());
    }

    #[test]
    fn should_mark_the_gateway_default_flow_regardless_of_document_order() {
        // given: an exclusive gateway whose `default` flow is listed FIRST, with
        // the conditional flow second — the order Camunda often serialises.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:exclusiveGateway id="gw" default="to_default" />
              <bpmn:endEvent id="default_task" />
              <bpmn:endEvent id="cond_task" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="gw" />
              <bpmn:sequenceFlow id="to_default" sourceRef="gw" targetRef="default_task" />
              <bpmn:sequenceFlow id="to_cond" sourceRef="gw" targetRef="cond_task">
                <bpmn:conditionExpression>=isDuplicate</bpmn:conditionExpression>
              </bpmn:sequenceFlow>
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then: the gateway's outgoing flows carry the right is_default markers —
        // the default flow is flagged even though it appears first in the document.
        let gw = def.element("gw").unwrap();
        let default = gw.outgoing.iter().find(|f| f.to == "default_task").unwrap();
        let cond = gw.outgoing.iter().find(|f| f.to == "cond_task").unwrap();
        assert!(default.is_default, "default flow should be flagged");
        assert!(default.condition.is_none());
        assert!(!cond.is_default, "conditional flow should not be default");
        assert!(cond.condition.is_some());
    }

    #[test]
    fn should_parse_a_user_task_and_link_its_flows() {
        // given: start -> review (userTask) -> end, the shape the Camunda SDK's
        // user-task fixture deploys (a <bpmn:userTask> with <zeebe:userTask/>).
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then: the user task is recognised and its sequence flows resolve (a
        // regression guard against the "unknown target element" parse error).
        assert!(matches!(
            def.element("review").unwrap().kind,
            ElementKind::UserTask(_)
        ));
        assert_eq!(def.element("s").unwrap().outgoing[0].to, "review");
        assert_eq!(def.element("review").unwrap().outgoing[0].to, "e");
    }

    #[test]
    fn should_parse_user_task_assignment_schedule_and_priority() {
        // given: a user task carrying assignment/schedule/priority extension
        // elements, as the Camunda modeler emits them.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:assignmentDefinition assignee="=requester"
                      candidateGroups="ops,finance" candidateUsers="=reviewers" />
                  <zeebe:taskSchedule dueDate="2025-01-01T00:00:00Z"
                      followUpDate="=followUp" />
                  <zeebe:priorityDefinition priority="80" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then: the raw expressions are captured on the user-task props.
        let ElementKind::UserTask(props) = &def.element("review").unwrap().kind else {
            panic!("expected a user task");
        };
        assert_eq!(props.assignee.as_deref(), Some("=requester"));
        assert_eq!(props.candidate_groups.as_deref(), Some("ops,finance"));
        assert_eq!(props.candidate_users.as_deref(), Some("=reviewers"));
        assert_eq!(props.due_date.as_deref(), Some("2025-01-01T00:00:00Z"));
        assert_eq!(props.follow_up_date.as_deref(), Some("=followUp"));
        assert_eq!(props.priority.as_deref(), Some("80"));
    }

    #[test]
    fn should_parse_service_task_job_priority() {
        // given: a service task carrying a zeebe:priorityDefinition (job priority),
        // alongside its taskDefinition, as Camunda 8.10 emits it.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="payment" />
                  <zeebe:priorityDefinition priority="=urgency" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="b" sourceRef="charge" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then: the raw priority expression rides on the service task element.
        assert_eq!(
            def.element("charge").unwrap().kind,
            ElementKind::ServiceTask {
                job_type: "payment".to_string(),
                priority: Some("=urgency".to_string()),
            }
        );
    }

    #[test]
    fn should_default_service_task_priority_to_none_when_absent() {
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="payment" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="b" sourceRef="charge" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;
        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(
            def.element("charge").unwrap().kind,
            ElementKind::ServiceTask {
                job_type: "payment".to_string(),
                priority: None,
            }
        );
    }

    #[test]
    fn should_default_service_task_job_type_to_its_id() {
        // given
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="work" />
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="work" />
              <bpmn:sequenceFlow id="b" sourceRef="work" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        assert_eq!(
            def.element("work").unwrap().kind,
            ElementKind::ServiceTask {
                job_type: "work".to_string(),
                priority: None,
            }
        );
    }

    #[test]
    fn should_parse_exclusive_gateway_with_conditions() {
        // given
        let xml = r#"
          <definitions>
            <process id="route">
              <startEvent id="s" />
              <exclusiveGateway id="gw" />
              <endEvent id="yes" />
              <endEvent id="no" />
              <sequenceFlow id="f0" sourceRef="s" targetRef="gw" />
              <sequenceFlow id="f1" sourceRef="gw" targetRef="yes">
                <conditionExpression xsi:type="tFormalExpression">= decision = "yes"</conditionExpression>
              </sequenceFlow>
              <sequenceFlow id="f2" sourceRef="gw" targetRef="no" />
            </process>
          </definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        let gw = def.element("gw").unwrap();
        let to_yes = gw.outgoing.iter().find(|f| f.to == "yes").unwrap();
        assert_eq!(
            to_yes.condition,
            Some(Condition::new(r#"= decision = "yes""#))
        );
        let to_no = gw.outgoing.iter().find(|f| f.to == "no").unwrap();
        assert_eq!(to_no.condition, None);
    }

    #[test]
    fn should_parse_multiple_processes_in_one_file() {
        // given
        let xml = r#"
          <definitions>
            <process id="a"><startEvent id="s" /><endEvent id="e" />
              <sequenceFlow id="f" sourceRef="s" targetRef="e" /></process>
            <process id="b"><startEvent id="s" /><endEvent id="e" />
              <sequenceFlow id="f" sourceRef="s" targetRef="e" /></process>
          </definitions>"#;

        // when
        let defs = parse_bpmn(xml).unwrap();

        // then
        assert_eq!(
            defs.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
    }

    #[test]
    fn should_reject_a_file_without_a_process() {
        // given
        let xml = r#"<definitions xmlns="x"></definitions>"#;

        // when / then
        assert_eq!(parse_bpmn(xml), Err(ParseError::NoProcess));
    }

    #[test]
    fn should_reject_a_process_without_a_start_event() {
        // given
        let xml = r#"<definitions><process id="p"><endEvent id="e" /></process></definitions>"#;

        // when
        let err = parse_bpmn(xml).unwrap_err();

        // then
        assert!(matches!(err, ParseError::InvalidProcess { .. }));
    }

    #[test]
    fn should_parse_an_error_boundary_event() {
        // given
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="charge-card" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="done" />
              <bpmn:boundaryEvent id="declined" attachedToRef="charge">
                <bpmn:errorEventDefinition errorRef="Error_1" />
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="refunded" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="f1" sourceRef="charge" targetRef="done" />
              <bpmn:sequenceFlow id="f2" sourceRef="declined" targetRef="refunded" />
            </bpmn:process>
            <bpmn:error id="Error_1" name="Declined" errorCode="CARD_DECLINED" />
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        assert_eq!(
            def.element("declined").unwrap().kind,
            ElementKind::ErrorBoundaryEvent {
                attached_to: "charge".to_string(),
                error_code: "CARD_DECLINED".to_string(),
            }
        );
        let boundary = def.element("declined").unwrap();
        assert!(boundary.outgoing.iter().any(|f| f.to == "refunded"));
    }

    #[test]
    fn should_parse_a_timer_boundary_event() {
        // given
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="charge-card" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="done" />
              <bpmn:boundaryEvent id="timeout" attachedToRef="charge">
                <bpmn:timerEventDefinition>
                  <bpmn:timeDuration>PT5S</bpmn:timeDuration>
                </bpmn:timerEventDefinition>
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="escalated" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="f1" sourceRef="charge" targetRef="done" />
              <bpmn:sequenceFlow id="f2" sourceRef="timeout" targetRef="escalated" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        assert_eq!(
            def.element("timeout").unwrap().kind,
            ElementKind::TimerBoundaryEvent {
                attached_to: "charge".to_string(),
                duration_millis: 5_000,
                interrupting: true,
                repeating: false,
            }
        );
        let boundary = def.element("timeout").unwrap();
        assert!(boundary.outgoing.iter().any(|f| f.to == "escalated"));
    }

    #[test]
    fn should_reject_a_boundary_event_referencing_an_unknown_error() {
        // given
        let xml = r#"
          <definitions>
            <process id="p">
              <startEvent id="s" />
              <serviceTask id="t" />
              <endEvent id="e" />
              <boundaryEvent id="b" attachedToRef="t">
                <errorEventDefinition errorRef="missing" />
              </boundaryEvent>
              <endEvent id="caught" />
              <sequenceFlow id="f0" sourceRef="s" targetRef="t" />
              <sequenceFlow id="f1" sourceRef="t" targetRef="e" />
              <sequenceFlow id="f2" sourceRef="b" targetRef="caught" />
            </process>
          </definitions>"#;

        // when
        let err = parse_bpmn(xml).unwrap_err();

        // then
        assert!(matches!(err, ParseError::InvalidBoundaryEvent { .. }));
    }

    #[test]
    fn should_parse_signal_intermediate_catch_and_boundary_events() {
        // given: a signal intermediate catch and a signal boundary on a task,
        // both referencing a definitions-level <signal>.
        let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:intermediateCatchEvent id="await">
                <bpmn:signalEventDefinition signalRef="Signal_1" />
              </bpmn:intermediateCatchEvent>
              <bpmn:serviceTask id="work" />
              <bpmn:endEvent id="e" />
              <bpmn:boundaryEvent id="abort" attachedToRef="work">
                <bpmn:signalEventDefinition signalRef="Signal_1" />
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="aborted" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="await" />
              <bpmn:sequenceFlow id="f1" sourceRef="await" targetRef="work" />
              <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="e" />
              <bpmn:sequenceFlow id="f3" sourceRef="abort" targetRef="aborted" />
            </bpmn:process>
            <bpmn:signal id="Signal_1" name="all-clear" />
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        assert_eq!(
            def.element("await").unwrap().kind,
            ElementKind::SignalIntermediateCatchEvent {
                signal_name: "all-clear".to_string(),
            }
        );
        assert_eq!(
            def.element("abort").unwrap().kind,
            ElementKind::SignalBoundaryEvent {
                attached_to: "work".to_string(),
                signal_name: "all-clear".to_string(),
                interrupting: true,
            }
        );
        assert!(def
            .element("abort")
            .unwrap()
            .outgoing
            .iter()
            .any(|f| f.to == "aborted"));
    }

    #[test]
    fn should_parse_multi_instance_loop_characteristics() {
        // given: a service task carrying parallel multi-instance characteristics
        // with a zeebe:loopCharacteristics extension (input/output collection and
        // element) plus a completionCondition in the standard BPMN element text.
        let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="each">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="handle" />
                </bpmn:extensionElements>
                <bpmn:multiInstanceLoopCharacteristics isSequential="true">
                  <bpmn:extensionElements>
                    <zeebe:loopCharacteristics
                        inputCollection="=items"
                        inputElement="item"
                        outputCollection="results"
                        outputElement="=item * 2" />
                  </bpmn:extensionElements>
                  <bpmn:completionCondition>=count(results) &gt;= 2</bpmn:completionCondition>
                </bpmn:multiInstanceLoopCharacteristics>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="each" />
              <bpmn:sequenceFlow id="f1" sourceRef="each" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        let mi = def
            .element("each")
            .unwrap()
            .multi_instance
            .as_ref()
            .expect("multi-instance characteristics parsed");
        assert_eq!(mi.input_collection, "=items");
        assert_eq!(mi.input_element.as_deref(), Some("item"));
        assert_eq!(mi.output_collection.as_deref(), Some("results"));
        assert_eq!(mi.output_element.as_deref(), Some("=item * 2"));
        assert_eq!(
            mi.completion_condition.as_deref(),
            Some("=count(results) >= 2")
        );
        assert!(mi.sequential, "isSequential=true parsed");
        // The task itself still routes to a job (taskDefinition preserved).
        assert!(matches!(
            def.element("each").unwrap().kind,
            ElementKind::ServiceTask { .. }
        ));
    }

    #[test]
    fn should_parse_conditional_intermediate_catch_and_boundary_events() {
        // given: a conditional intermediate catch and a conditional boundary
        // (one interrupting, one not) whose FEEL condition lives in the nested
        // <condition> element text of a <conditionalEventDefinition>.
        let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:intermediateCatchEvent id="gate">
                <bpmn:conditionalEventDefinition>
                  <bpmn:condition xsi:type="bpmn:tFormalExpression">=approved = true</bpmn:condition>
                </bpmn:conditionalEventDefinition>
              </bpmn:intermediateCatchEvent>
              <bpmn:serviceTask id="work" />
              <bpmn:endEvent id="e" />
              <bpmn:boundaryEvent id="bnd" attachedToRef="work" cancelActivity="false">
                <bpmn:conditionalEventDefinition>
                  <bpmn:condition>=ping = true</bpmn:condition>
                </bpmn:conditionalEventDefinition>
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="pinged" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="gate" />
              <bpmn:sequenceFlow id="f1" sourceRef="gate" targetRef="work" />
              <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="e" />
              <bpmn:sequenceFlow id="f3" sourceRef="bnd" targetRef="pinged" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        assert_eq!(
            def.element("gate").unwrap().kind,
            ElementKind::ConditionalIntermediateCatchEvent {
                condition: "=approved = true".to_string(),
            }
        );
        assert_eq!(
            def.element("bnd").unwrap().kind,
            ElementKind::ConditionalBoundaryEvent {
                attached_to: "work".to_string(),
                condition: "=ping = true".to_string(),
                interrupting: false,
            }
        );
        assert!(def
            .element("bnd")
            .unwrap()
            .outgoing
            .iter()
            .any(|f| f.to == "pinged"));
    }

    #[test]
    fn should_parse_a_message_intermediate_catch_event() {
        // given
        let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:intermediateCatchEvent id="await">
                <bpmn:messageEventDefinition messageRef="Message_1" />
              </bpmn:intermediateCatchEvent>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="await" />
              <bpmn:sequenceFlow id="f1" sourceRef="await" targetRef="e" />
            </bpmn:process>
            <bpmn:message id="Message_1" name="payment-received">
              <bpmn:extensionElements>
                <zeebe:subscription correlationKey="=orderId" />
              </bpmn:extensionElements>
            </bpmn:message>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        assert_eq!(
            def.element("await").unwrap().kind,
            ElementKind::MessageIntermediateCatchEvent {
                message_name: "payment-received".to_string(),
                correlation_key: "orderId".to_string(),
            }
        );
    }

    #[test]
    fn should_parse_a_message_boundary_event() {
        // given
        let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge" />
              <bpmn:endEvent id="done" />
              <bpmn:boundaryEvent id="cancel" attachedToRef="charge">
                <bpmn:messageEventDefinition messageRef="Message_1" />
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="aborted" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="f1" sourceRef="charge" targetRef="done" />
              <bpmn:sequenceFlow id="f2" sourceRef="cancel" targetRef="aborted" />
            </bpmn:process>
            <bpmn:message id="Message_1" name="order-cancelled">
              <bpmn:extensionElements>
                <zeebe:subscription correlationKey="=orderId" />
              </bpmn:extensionElements>
            </bpmn:message>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        assert_eq!(
            def.element("cancel").unwrap().kind,
            ElementKind::MessageBoundaryEvent {
                attached_to: "charge".to_string(),
                message_name: "order-cancelled".to_string(),
                correlation_key: "orderId".to_string(),
                interrupting: true,
            }
        );
        let boundary = def.element("cancel").unwrap();
        assert!(boundary.outgoing.iter().any(|f| f.to == "aborted"));
    }

    #[test]
    fn should_parse_non_interrupting_timer_and_message_boundary_events() {
        // given: a service task with a non-interrupting timer boundary and a
        // non-interrupting message boundary (both cancelActivity="false").
        let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge" />
              <bpmn:endEvent id="done" />
              <bpmn:boundaryEvent id="remind" attachedToRef="charge" cancelActivity="false">
                <bpmn:timerEventDefinition>
                  <bpmn:timeDuration>PT5S</bpmn:timeDuration>
                </bpmn:timerEventDefinition>
              </bpmn:boundaryEvent>
              <bpmn:boundaryEvent id="notify" attachedToRef="charge" cancelActivity="false">
                <bpmn:messageEventDefinition messageRef="Message_1" />
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="reminded" />
              <bpmn:endEvent id="notified" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="f1" sourceRef="charge" targetRef="done" />
              <bpmn:sequenceFlow id="f2" sourceRef="remind" targetRef="reminded" />
              <bpmn:sequenceFlow id="f3" sourceRef="notify" targetRef="notified" />
            </bpmn:process>
            <bpmn:message id="Message_1" name="reminder">
              <bpmn:extensionElements>
                <zeebe:subscription correlationKey="=orderId" />
              </bpmn:extensionElements>
            </bpmn:message>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then: both boundaries parse as non-interrupting.
        assert_eq!(
            def.element("remind").unwrap().kind,
            ElementKind::TimerBoundaryEvent {
                attached_to: "charge".to_string(),
                duration_millis: 5_000,
                interrupting: false,
                repeating: false,
            }
        );
        assert_eq!(
            def.element("notify").unwrap().kind,
            ElementKind::MessageBoundaryEvent {
                attached_to: "charge".to_string(),
                message_name: "reminder".to_string(),
                correlation_key: "orderId".to_string(),
                interrupting: false,
            }
        );
    }

    #[test]
    fn should_parse_a_non_interrupting_cycle_timer_boundary_event() {
        // given: a service task with a non-interrupting timer boundary whose
        // timerEventDefinition carries a timeCycle (a repeating interval).
        let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge" />
              <bpmn:endEvent id="done" />
              <bpmn:boundaryEvent id="tick" attachedToRef="charge" cancelActivity="false">
                <bpmn:timerEventDefinition>
                  <bpmn:timeCycle>R/PT5S</bpmn:timeCycle>
                </bpmn:timerEventDefinition>
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="ticked" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="f1" sourceRef="charge" targetRef="done" />
              <bpmn:sequenceFlow id="f2" sourceRef="tick" targetRef="ticked" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then: it is a non-interrupting, repeating timer boundary at 5s.
        assert_eq!(
            def.element("tick").unwrap().kind,
            ElementKind::TimerBoundaryEvent {
                attached_to: "charge".to_string(),
                duration_millis: 5_000,
                interrupting: false,
                repeating: true,
            }
        );
    }

    #[test]
    fn should_reject_a_message_event_referencing_an_unknown_message() {
        // given
        let xml = r#"
          <definitions>
            <process id="p">
              <startEvent id="s" />
              <intermediateCatchEvent id="await">
                <messageEventDefinition messageRef="missing" />
              </intermediateCatchEvent>
              <endEvent id="e" />
              <sequenceFlow id="f0" sourceRef="s" targetRef="await" />
              <sequenceFlow id="f1" sourceRef="await" targetRef="e" />
            </process>
          </definitions>"#;

        // when
        let err = parse_bpmn(xml).unwrap_err();

        // then
        assert!(matches!(err, ParseError::InvalidMessageEvent { .. }));
    }

    #[test]
    fn should_parse_iso8601_cycles() {
        assert_eq!(parse_iso8601_cycle("R/PT10S"), Some(10_000));
        assert_eq!(parse_iso8601_cycle("R5/PT1H"), Some(3_600_000));
        assert_eq!(parse_iso8601_cycle(" R/PT1M30S "), Some(90_000));
        assert_eq!(parse_iso8601_cycle("PT10S"), Some(10_000));
        assert_eq!(parse_iso8601_cycle("PT10S/R"), None);
        assert_eq!(parse_iso8601_cycle("R/bogus"), None);
    }

    #[test]
    fn should_parse_a_message_start_event() {
        // given
        let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s">
                <bpmn:messageEventDefinition messageRef="Message_1" />
              </bpmn:startEvent>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="e" />
            </bpmn:process>
            <bpmn:message id="Message_1" name="order-placed" />
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        assert_eq!(
            def.element("s").unwrap().kind,
            ElementKind::MessageStartEvent {
                message_name: "order-placed".to_string(),
            }
        );
    }

    #[test]
    fn should_parse_a_one_shot_timer_start_event() {
        // given
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s">
                <bpmn:timerEventDefinition>
                  <bpmn:timeDuration>PT10S</bpmn:timeDuration>
                </bpmn:timerEventDefinition>
              </bpmn:startEvent>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        assert_eq!(
            def.element("s").unwrap().kind,
            ElementKind::TimerStartEvent {
                interval_millis: 10_000,
                repeating: false,
            }
        );
    }

    #[test]
    fn should_parse_a_recurring_timer_start_event() {
        // given
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s">
                <bpmn:timerEventDefinition>
                  <bpmn:timeCycle>R/PT1H</bpmn:timeCycle>
                </bpmn:timerEventDefinition>
              </bpmn:startEvent>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        assert_eq!(
            def.element("s").unwrap().kind,
            ElementKind::TimerStartEvent {
                interval_millis: 3_600_000,
                repeating: true,
            }
        );
    }

    #[test]
    fn should_parse_an_embedded_subprocess_with_an_error_boundary() {
        // given a process whose embedded sub-process has its own start/task/end
        // and an interrupting error boundary attached to the sub-process.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="start" />
              <bpmn:subProcess id="sub">
                <bpmn:startEvent id="sub_start" />
                <bpmn:serviceTask id="inner">
                  <bpmn:extensionElements>
                    <zeebe:taskDefinition type="work" />
                  </bpmn:extensionElements>
                </bpmn:serviceTask>
                <bpmn:endEvent id="sub_end" />
                <bpmn:sequenceFlow id="i0" sourceRef="sub_start" targetRef="inner" />
                <bpmn:sequenceFlow id="i1" sourceRef="inner" targetRef="sub_end" />
              </bpmn:subProcess>
              <bpmn:boundaryEvent id="boundary" attachedToRef="sub">
                <bpmn:errorEventDefinition errorRef="Error_1" />
              </bpmn:boundaryEvent>
              <bpmn:serviceTask id="sad">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="sad-flow" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="done" />
              <bpmn:endEvent id="sad_end" />
              <bpmn:sequenceFlow id="f0" sourceRef="start" targetRef="sub" />
              <bpmn:sequenceFlow id="f1" sourceRef="sub" targetRef="done" />
              <bpmn:sequenceFlow id="f2" sourceRef="boundary" targetRef="sad" />
              <bpmn:sequenceFlow id="f3" sourceRef="sad" targetRef="sad_end" />
            </bpmn:process>
            <bpmn:error id="Error_1" name="Business" errorCode="BUSINESS_ERROR" />
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then the process-level start event is the outer one, and the
        // sub-process points at its inner start.
        assert_eq!(def.start_event, "start");
        assert_eq!(
            def.element("sub").unwrap().kind,
            ElementKind::SubProcess {
                start_event: "sub_start".to_string(),
            }
        );
        // The inner nodes are tagged as contained in the sub-process; the outer
        // ones are not.
        assert_eq!(def.element("inner").unwrap().parent.as_deref(), Some("sub"));
        assert_eq!(
            def.element("sub_start").unwrap().parent.as_deref(),
            Some("sub")
        );
        assert_eq!(def.element("sub").unwrap().parent, None);
        assert_eq!(def.element("start").unwrap().parent, None);
        // The error boundary is attached to the sub-process and routes to sad-flow.
        assert_eq!(
            def.element("boundary").unwrap().kind,
            ElementKind::ErrorBoundaryEvent {
                attached_to: "sub".to_string(),
                error_code: "BUSINESS_ERROR".to_string(),
            }
        );
        assert!(def
            .element("sub")
            .unwrap()
            .outgoing
            .iter()
            .any(|f| f.to == "done"));
    }

    #[test]
    fn should_parse_call_activities_in_both_camunda_7_and_zeebe_forms() {
        // Two call activities: one with a `calledElement` attribute (Camunda 7),
        // one with a nested `zeebe:calledElement processId` (Camunda 8/Zeebe).
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="orch">
              <bpmn:startEvent id="start" />
              <bpmn:callActivity id="c1" calledElement="Phase01" />
              <bpmn:callActivity id="c2">
                <bpmn:extensionElements>
                  <zeebe:calledElement processId="Phase02" />
                </bpmn:extensionElements>
              </bpmn:callActivity>
              <bpmn:endEvent id="end" />
              <bpmn:sequenceFlow id="f0" sourceRef="start" targetRef="c1" />
              <bpmn:sequenceFlow id="f1" sourceRef="c1" targetRef="c2" />
              <bpmn:sequenceFlow id="f2" sourceRef="c2" targetRef="end" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(
            def.element("c1").unwrap().kind,
            ElementKind::CallActivity {
                called_process_id: "Phase01".to_string(),
            }
        );
        assert_eq!(
            def.element("c2").unwrap().kind,
            ElementKind::CallActivity {
                called_process_id: "Phase02".to_string(),
            }
        );
        // The call activity's outgoing flow is preserved for inline expansion.
        assert!(def
            .element("c1")
            .unwrap()
            .outgoing
            .iter()
            .any(|f| f.to == "c2"));
    }

    #[test]
    fn a_call_activity_without_a_callee_is_a_parse_error() {
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="orch">
              <bpmn:startEvent id="start" />
              <bpmn:callActivity id="c1" />
              <bpmn:endEvent id="end" />
              <bpmn:sequenceFlow id="f0" sourceRef="start" targetRef="c1" />
              <bpmn:sequenceFlow id="f1" sourceRef="c1" targetRef="end" />
            </bpmn:process>
          </bpmn:definitions>"#;
        assert!(parse_bpmn(xml).is_err());
    }
}

#[cfg(test)]
mod io_mapping_tests {
    use super::*;

    #[test]
    fn parses_zeebe_io_mapping_inputs_and_outputs() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="t">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="work" />
        <zeebe:ioMapping>
          <zeebe:input source="=x + 1" target="y" />
          <zeebe:input source="=a" target="order.id" />
          <zeebe:output source="=result" target="approved" />
        </zeebe:ioMapping>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
    <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let def = &parse_bpmn(xml).unwrap()[0];
        let io = &def.element("t").unwrap().io;
        assert_eq!(io.inputs.len(), 2);
        assert_eq!(io.inputs[0].source, "=x + 1");
        assert_eq!(io.inputs[0].target, "y");
        assert_eq!(io.inputs[1].target, "order.id");
        assert_eq!(io.outputs.len(), 1);
        assert_eq!(io.outputs[0].source, "=result");
        assert_eq!(io.outputs[0].target, "approved");
    }

    #[test]
    fn io_mapping_is_scoped_to_its_own_activity() {
        // Two service tasks each with their own ioMapping; the mappings must not
        // bleed across activities.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="t1">
      <bpmn:extensionElements>
        <zeebe:ioMapping><zeebe:input source="=1" target="one" /></zeebe:ioMapping>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="t2">
      <bpmn:extensionElements>
        <zeebe:ioMapping><zeebe:output source="=2" target="two" /></zeebe:ioMapping>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t1" />
    <bpmn:sequenceFlow id="f2" sourceRef="t1" targetRef="t2" />
    <bpmn:sequenceFlow id="f3" sourceRef="t2" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let def = &parse_bpmn(xml).unwrap()[0];
        let io1 = &def.element("t1").unwrap().io;
        assert_eq!(io1.inputs.len(), 1);
        assert!(io1.outputs.is_empty());
        let io2 = &def.element("t2").unwrap().io;
        assert!(io2.inputs.is_empty());
        assert_eq!(io2.outputs.len(), 1);
    }
}

#[cfg(test)]
mod feel_timer_tests {
    use super::*;
    use crate::model::TimerDefKind;

    #[test]
    fn parses_feel_timer_expressions() {
        // A `=`-prefixed timeDuration/timeCycle and any timeDate are captured as
        // FEEL timer expressions; a static ISO literal is not.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:intermediateCatchEvent id="wait">
      <bpmn:timerEventDefinition><bpmn:timeDuration>=waitFor</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:intermediateCatchEvent id="until">
      <bpmn:timerEventDefinition><bpmn:timeDate>=dueAt</bpmn:timeDate></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:intermediateCatchEvent id="fixed">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT30S</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="wait" />
    <bpmn:sequenceFlow id="f2" sourceRef="wait" targetRef="until" />
    <bpmn:sequenceFlow id="f3" sourceRef="until" targetRef="fixed" />
    <bpmn:sequenceFlow id="f4" sourceRef="fixed" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let def = &parse_bpmn(xml).unwrap()[0];

        let wait = def.element("wait").unwrap().timer.as_ref().unwrap();
        assert_eq!(wait.kind, TimerDefKind::Duration);
        assert_eq!(wait.expr, "=waitFor");

        let until = def.element("until").unwrap().timer.as_ref().unwrap();
        assert_eq!(until.kind, TimerDefKind::Date);
        assert_eq!(until.expr, "=dueAt");

        // A static ISO literal is parsed at deploy, not carried as a FEEL expr.
        assert!(def.element("fixed").unwrap().timer.is_none());
    }
}
