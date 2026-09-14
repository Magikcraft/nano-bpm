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
//! * Flow nodes: `startEvent`, `endEvent` (plain none end, or — with a nested
//!   `terminateEventDefinition` — a terminate end that kills the remaining
//!   tokens in its enclosing scope), `task`/`manualTask` (abstract
//!   pass-through), `serviceTask`, `sendTask` (a job-based service task — its
//!   throwing cousin of `receiveTask`), `userTask`, `exclusiveGateway`,
//!   `parallelGateway`, `inclusiveGateway`, `eventBasedGateway`.
//! * `subProcess` (embedded): its nested flow nodes/flows are scoped to it, and
//!   a `boundaryEvent` with an `errorEventDefinition` attached to it becomes an
//!   interrupting error boundary on the sub-process.
//! * `intermediateCatchEvent` with a nested `timerEventDefinition`/`timeDuration`
//!   (timer catch) or a nested `messageEventDefinition` (message catch).
//! * Link events: an `intermediateThrowEvent` with a `linkEventDefinition name`
//!   (a link *throw*) and an `intermediateCatchEvent` with a matching
//!   `linkEventDefinition name` (a link *catch*). The throw has no outgoing flow
//!   and the catch no incoming flow; on completion the throw hands its token to
//!   the matching catch in the same scope, which then routes onward — the
//!   standard "page-break connector" idiom. Reference integrity (every throw has
//!   a matching, unique catch) is enforced at deploy
//!   (Zeebe `ModelUtil.verifyLinkIntermediateEvents`).
//! * `boundaryEvent` with `attachedToRef` and a nested `errorEventDefinition`
//!   `errorRef`, resolved against definitions-level `error` elements
//!   (`<error id="…" errorCode="…">`) into an error boundary event; or a nested
//!   `timerEventDefinition` (timer boundary); or a nested `messageEventDefinition`
//!   (message boundary); or a nested `compensateEventDefinition` (compensation
//!   boundary — see below). `cancelActivity="false"` makes a timer or message
//!   boundary **non-interrupting** (the activity keeps running and a parallel
//!   token is spawned on each fire); the default is interrupting.
//! * Compensation: a `boundaryEvent` with a `compensateEventDefinition`, wired
//!   via an `<association sourceRef targetRef>` to an `isForCompensation` handler
//!   activity, marks its attached activity compensable. A
//!   `compensateEventDefinition` on an `intermediateThrowEvent`/`endEvent`
//!   triggers compensation: the completed compensable activities in scope have
//!   their handlers run (reverse completion order), and the throw event rests
//!   until they finish before routing onward (single-activity path).
//! * Escalation (#1173): an `escalationEventDefinition` on an
//!   `intermediateThrowEvent`/`endEvent` raises the referenced escalation code;
//!   an `escalationEventDefinition` on a `boundaryEvent` (attached to an
//!   embedded sub-process) catches a matching code raised inside that
//!   activity's scope, propagating up the enclosing scopes (an exact code beats
//!   a catch-all with no `escalationRef`). `cancelActivity="false"` (the
//!   escalation default idiom) makes the boundary non-interrupting — the
//!   activity keeps running and a parallel token is spawned; interrupting tears
//!   the activity's scope down. An uncaught escalation is ignored (no incident),
//!   matching Zeebe. Escalation is non-critical, so the throw always continues.
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
//!   Its form linkage comes from nested `zeebe:formDefinition` (`formId` for an
//!   embedded/deployment form, resolved to a numeric form key at task creation
//!   under the default `latest` binding — a `deployment`/`versionTag`
//!   `bindingType` is rejected at deploy rather than silently degraded, #1190;
//!   `externalReference` for an external form).
//! * `sequenceFlow` with `sourceRef`/`targetRef`, and an optional
//!   `conditionExpression` whose FEEL body is stored verbatim and evaluated by
//!   [`crate::feel`] at a condition-routed gateway — exclusive (XOR) or
//!   inclusive (OR) (comparisons, arithmetic, boolean logic, member access — not
//!   just equality). A condition that fails to
//!   evaluate to a boolean raises an `ExpressionEvaluation` incident.
//! * A `serviceTask` bearing a `zeebe:agentDefinition agentType="aiAgentTask"`
//!   (or `"external"`) extension marker remains an ordinary
//!   [`ServiceTask`](crate::model::ElementKind::ServiceTask), with agent metadata.
//!   It creates a normal job; the worker explicitly registers its AgentInstance
//!   via `CreateAgentInstance`. Supplied job attribution is lease-validated,
//!   and history-bearing requests require it. Its job type comes from a co-located
//!   `zeebe:taskDefinition type` (literal or FEEL), or the element id when absent.
//!   Placement mirrors Camunda's
//!   `AgentDefinitionValidator`: `aiAgentTask` is only valid on a `serviceTask`
//!   and `aiAgentSubProcess` only on an `adHocSubProcess`; the wrong placement
//!   (or an unknown `agentType`) is rejected at parse time.
//! * `zeebe:executionListeners` / nested `zeebe:executionListener`
//!   (`eventType` `start`/`end`, ADR 0037): parsed on activities (including
//!   `receiveTask`, which rides the `io_stack` like the other plain tasks),
//!   sub-processes, ad-hoc/call containers, **gateways** (exclusive/parallel/
//!   inclusive/event-based), **start events**, and **boundary events** (#1197).
//!   Each
//!   listener attaches to its own element (activities/gateways/start & end events
//!   ride the `io_stack`; a boundary event carries its listeners on the buffered
//!   boundary and re-attaches them by id at build) and fires through the shared
//!   activation/completion listener gate. A listener that could never fire is
//!   rejected at deploy rather than dropped: sequence-flow ("take") listeners
//!   (edges, not elements) and listeners on a **tool of an ad-hoc sub-process**
//!   (pruned leaf tools and directly-activated embedded tools both bypass the
//!   listener gate).

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
    /// A `zeebe:linkedResource` omitted a Zeebe-required attribute
    /// (`resourceId`, `bindingType` or `resourceType`). Zeebe rejects such a
    /// deployment (`INVALID_ARGUMENT` → HTTP 400) rather than silently dropping
    /// the link, so Nano surfaces it as a hard parse error for parity.
    InvalidLinkedResource { task_id: String, attribute: String },
    /// A user task's `zeebe:formDefinition` declared a non-`latest`
    /// `bindingType` (`deployment` or `versionTag`) on the form definition. The
    /// engine only implements the `latest` binding — it resolves a `formId` to
    /// the latest deployed form version at user-task creation. This is raised
    /// for any non-`latest` binding on the form definition, whether it carries a
    /// `formId` or not (an explicit empty `bindingType` is likewise rejected).
    /// Rather than silently degrading a `deployment`/`versionTag` binding to
    /// `latest` (a latent mis-binding, #1190), Nano rejects the deploy loudly.
    /// `binding_type` carries the offending attribute value.
    UnsupportedUserTaskFormBinding {
        task_id: String,
        binding_type: String,
    },
    /// A dangling QName/id reference: a reference site (`kind`) points at an
    /// `id` that is not declared anywhere the reference can resolve against.
    ///
    /// This is the **single** unresolved-reference variant shared across the
    /// whole deploy-validation-parity epic (#850). `kind` names the reference
    /// site — one of `"incoming"`, `"outgoing"`, `"messageRef"`, `"errorRef"`,
    /// `"signalRef"`, `"escalationRef"`, `"linkThrow"`, `"default"`,
    /// `"attachedToRef"` — and `id` is the dangling id. Zeebe resolves these
    /// QName references eagerly and rejects an unresolved one at deploy with
    /// `INVALID_ARGUMENT`, so Nano does the same rather than silently accepting.
    /// Sibling validators reuse this variant; do **not** add per-ref bespoke
    /// variants. `process_id` names the owning `<process>` and `from_node` the
    /// id of the element that *declared* the dangling reference, so a deploy
    /// failure and its regression tests can attribute the reference to its
    /// declaring element rather than losing that context.
    UnresolvedReference {
        kind: String,
        id: String,
        process_id: String,
        from_node: String,
    },
    /// An unmodelled flow-element tag or event definition that Nano's parser
    /// does not recognise (e.g. `complexGateway`, `transaction`,
    /// `cancelEventDefinition`). Zeebe only transforms known element types
    /// and rejects the rest at deploy; Nano surfaces the offending `tag` and
    /// `element_id` rather than silently dropping it. Raised by the
    /// `unsupported_elements` validator (#853).
    UnsupportedElement { tag: String, element_id: String },
    /// A gateway with more than one outgoing flow declared a non-default branch
    /// with no condition (an exclusive/inclusive gateway must gate every
    /// non-default outgoing branch with a condition, or make it the `default`).
    /// Raised by the `gateway_conditions` validator (#854).
    InvalidGateway {
        process_id: String,
        gateway_id: String,
        reason: String,
    },
    /// A `zeebe:executionListener` was declared on an element where it can never
    /// fire, so accepting it would silently store a listener that creates no job
    /// (#1197). Rather than mis-advertise support, Nano rejects the deploy. The
    /// unsupported placements are:
    /// * a **multi-incoming parallel gateway** (a join): it synchronises tokens
    ///   and completes without running the listener-aware activation body or the
    ///   end-listener chain, so neither a `start` nor an `end` listener fires.
    /// * the **`start` listener of a multi-incoming inclusive gateway** (a join):
    ///   the join fires at quiescence and short-circuits the activation body, so
    ///   its `start` listener never fires. An inclusive-join **`end`** listener IS
    ///   supported (the quiescence sweep defers the join behind the end-listener
    ///   chain) and is not rejected.
    /// * Single-incoming parallel/inclusive gateways (splits) and exclusive merges
    ///   run the normal activation body, so their listeners fire and are fine.
    /// * a **compensation boundary event**: a passive structural marker, armed
    ///   implicitly and never entered by token flow, so it has no lifecycle to
    ///   hang a listener on.
    /// * a **sequence flow**: an edge, not an `Element`, so it has no lifecycle
    ///   to run a "take" listener on. Sequence-flow ("take") listeners are a
    ///   deferred subset (#1198); until then a declared one is rejected rather
    ///   than dropped or hoisted onto the enclosing sub-process.
    /// * a **tool of an ad-hoc sub-process**: a leaf tool is pruned and a
    ///   retained embedded-sub-process tool is activated/completed with direct
    ///   lifecycle events, both bypassing the shared listener gate, so a listener
    ///   on it could never create a job. Rejected rather than accepted dead.
    UnsupportedExecutionListener {
        process_id: String,
        element_id: String,
        reason: String,
    },
    /// A process's start events violate Zeebe's start-event rules: it has no
    /// start event ("must have at least one start event"), or it declares more
    /// than one *none* start event ("multiple none start events are not
    /// allowed"). Raised by the `start_events` validator (#855).
    InvalidStartEvents { process_id: String, reason: String },
    /// An end event declares an outgoing sequence flow (an end event must have
    /// no outgoing flow). Raised by the `cheap_rules` validator (#856).
    InvalidEndEvent {
        process_id: String,
        element_id: String,
        reason: String,
    },
    /// Two start events in one process correlate on the same message or signal
    /// (no duplicate message/signal start events). `correlation_kind` is
    /// `"message"` or `"signal"` and `reference` is the shared id/name. Raised
    /// by the `cheap_rules` validator (#856).
    DuplicateStartEvent {
        process_id: String,
        correlation_kind: String,
        reference: String,
        reason: String,
    },
    /// A required `zeebe:taskDefinition` attribute is present but empty (its
    /// `type` — and `retries` when declared — must be non-empty). Raised by the
    /// `cheap_rules` validator (#856).
    InvalidTaskDefinition {
        process_id: String,
        task_id: String,
        attribute: String,
        reason: String,
    },
    /// A `zeebe:agentDefinition` extension marker was malformed or misplaced: its
    /// `agentType` was missing/unknown, or it violated the placement rules from
    /// Camunda's `AgentDefinitionValidator` — `agentType="aiAgentTask"` is only
    /// valid on a `serviceTask` and `agentType="aiAgentSubProcess"` is only valid
    /// on an `adHocSubProcess`. Zeebe rejects such a deployment at transform time
    /// (`INVALID_ARGUMENT`), so Nano surfaces it as a hard parse error for parity.
    InvalidAgentDefinition {
        process_id: String,
        element_id: String,
        reason: String,
    },
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
            ParseError::InvalidLinkedResource { task_id, attribute } => {
                write!(
                    f,
                    "linkedResource on '{task_id}' is missing required attribute '{attribute}'"
                )
            }
            ParseError::UnsupportedUserTaskFormBinding {
                task_id,
                binding_type,
            } => write!(
                f,
                "user task '{task_id}' declares an unsupported formDefinition bindingType \
'{binding_type}': only 'latest' is supported (the engine resolves a formId to the latest \
deployed form version at user-task creation)"
            ),
            ParseError::UnresolvedReference {
                kind,
                id,
                process_id,
                from_node,
            } => write!(
                f,
                "process {process_id}: unresolved {kind} reference to '{id}' on element '{from_node}': the referenced element is not declared"
            ),
            ParseError::UnsupportedElement { tag, element_id } => write!(
                f,
                "unsupported element <{tag}> (id '{element_id}'): Nano does not model this construct"
            ),
            ParseError::InvalidGateway {
                process_id,
                gateway_id,
                reason,
            } => write!(
                f,
                "process {process_id}: gateway '{gateway_id}' is invalid: {reason}"
            ),
            ParseError::UnsupportedExecutionListener {
                process_id,
                element_id,
                reason,
            } => write!(
                f,
                "process {process_id}: execution listener on '{element_id}' is not supported: {reason}"
            ),
            ParseError::InvalidStartEvents { process_id, reason } => {
                write!(f, "process {process_id}: {reason}")
            }
            ParseError::InvalidEndEvent {
                process_id,
                element_id,
                reason,
            } => write!(
                f,
                "process {process_id}: end event '{element_id}' is invalid: {reason}"
            ),
            ParseError::DuplicateStartEvent {
                process_id,
                correlation_kind,
                reference,
                reason,
            } => write!(
                f,
                "process {process_id}: duplicate {correlation_kind} start event on '{reference}': {reason}"
            ),
            ParseError::InvalidTaskDefinition {
                process_id,
                task_id,
                attribute,
                reason,
            } => write!(
                f,
                "process {process_id}: task '{task_id}' zeebe:taskDefinition attribute '{attribute}' is invalid: {reason}"
            ),
            ParseError::InvalidAgentDefinition {
                process_id,
                element_id,
                reason,
            } => write!(
                f,
                "process {process_id}: zeebe:agentDefinition on '{element_id}' is invalid: {reason}"
            ),
        }
    }
}

impl std::error::Error for ParseError {}

/// Whether a tag reaching the in-process catch-all is genuinely ignorable
/// non-flow noise (diagram interchange, documentation, extension-element
/// children Nano reads elsewhere, structural/data/collaboration BPMN, and the
/// event definitions Nano *does* model), as opposed to an unmodelled flow
/// element / event definition that must be recorded for the unsupported-element
/// validator (#853).
///
/// This is deliberately an *exclusion* list of noise: anything not listed here
/// is recorded as a candidate unmodelled element, and #853 decides — from the
/// canonical supported-element registry — which recorded candidates are
/// genuinely unsupported. Over-recording a rare non-flow tag is harmless (the
/// validator filters it); silently dropping a real flow element is the bug this
/// closes.
fn is_ignorable_tag(tag: &str) -> bool {
    matches!(
        tag,
        // Diagram interchange (BPMNDI / OMGDI / DC).
        "BPMNDiagram"
            | "BPMNPlane"
            | "BPMNShape"
            | "BPMNLabel"
            | "BPMNLabelStyle"
            | "BPMNEdge"
            | "DiagramElement"
            | "Bounds"
            | "waypoint"
            | "Label"
            | "Font"
            // Documentation / extension containers (their meaningful children are
            // matched explicitly above, or intentionally ignored).
            | "documentation"
            | "extensionElements"
            | "text"
            | "modelerTemplate"
            | "userTaskForm"
            // Event definitions Nano *does* model (timer via <timeDuration>,
            // message/signal/error/escalation/link via their refs, conditional
            // via <condition>) — not "unsupported", so never recorded.
            | "timerEventDefinition"
            | "messageEventDefinition"
            | "signalEventDefinition"
            | "errorEventDefinition"
            | "escalationEventDefinition"
            | "linkEventDefinition"
            | "conditionalEventDefinition"
            // `terminateEventDefinition` is handled by an explicit parse arm (it
            // marks its owning `<endEvent>` as a terminate end event); it never
            // reaches the fallback, so it is not listed here.
            // Nested value/config children of already-modelled constructs.
            | "condition"
            | "conditionExpression"
            | "timeDuration"
            | "timeCycle"
            | "timeDate"
            | "completionCondition"
            | "loopCardinality"
            | "activationCondition"
            | "transitionCondition"
            | "incoming"
            | "outgoing"
            // Structural / data / collaboration / resourcing elements that carry
            // no executable flow semantics for Nano.
            | "laneSet"
            | "lane"
            | "flowNodeRef"
            | "childLaneSet"
            | "dataObject"
            | "dataObjectReference"
            | "dataStore"
            | "dataStoreReference"
            | "property"
            | "dataInput"
            | "dataOutput"
            | "dataInputAssociation"
            | "dataOutputAssociation"
            | "inputSet"
            | "outputSet"
            | "ioSpecification"
            | "sourceRef"
            | "targetRef"
            | "association"
            | "group"
            | "textAnnotation"
            | "participant"
            | "collaboration"
            | "messageFlow"
            | "category"
            | "categoryValue"
            | "categoryValueRef"
            | "auditing"
            | "monitoring"
            | "relationship"
            | "resource"
            | "resourceRef"
            | "resourceAssignmentExpression"
            | "potentialOwner"
            | "humanPerformer"
            | "performer"
            | "rendering"
    )
}

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
    parse_with_captures(xml)?
        .into_iter()
        .map(|(capture, def)| {
            // Run the post-parse validators (deploy-validation parity, #850).
            crate::validate::run(&crate::validate::ValidationInput {
                def: &def,
                capture: &capture,
            })?;
            Ok(def)
        })
        .collect()
}

/// Parses `xml` into `(raw capture, built definition)` pairs, one per
/// `<process>`, *without* running the post-parse validators. This is the single
/// source of truth for the streaming parse; [`parse_bpmn`] is a thin wrapper
/// that runs validation over the pairs. Kept separate so tests can inspect the
/// raw [`ProcessCapture`](crate::validate::ProcessCapture) a validator would see
/// (e.g. unmodelled-element attribution) directly, before any validator either
/// consumes it or rejects the definition.
fn parse_with_captures(
    xml: &str,
) -> Result<Vec<(crate::validate::ProcessCapture, ProcessDefinition)>, ParseError> {
    let tokens = tokenize(xml).map_err(|e| ParseError::MalformedXml(e.0))?;

    let mut processes: Vec<ProcessAcc> = Vec::new();
    let mut current: Option<ProcessAcc> = None;
    // Index of the service task currently being read (to attach its job type).
    let mut cur_service_task: Option<usize> = None;
    // Index of the plain `task`/`manualTask` currently being read. Tracked purely
    // so its close handler can pop the io_stack only when the open handler
    // actually pushed (an id-less task is never pushed) — mirroring the
    // service/user/call trackers.
    let mut cur_plain_task: Option<usize> = None;
    // Index of the user task currently being read (to attach assignment,
    // scheduling and priority expressions from its Zeebe extension elements).
    let mut cur_user_task: Option<usize> = None;
    // Index of the sequence flow currently being read (to attach a condition).
    let mut cur_flow: Option<usize> = None;
    let mut condition_text: Option<String> = None;
    // A separate buffer for a conditional-event's nested `<condition>` FEEL text
    // (distinct from a sequence flow's `conditionExpression`).
    let mut event_condition_text: Option<String> = None;
    // Buffer for the text content of a flow node's `<incoming>`/`<outgoing>`
    // child (a QName reference to a sequenceFlow), plus the owning node id and
    // reference direction so it can be resolved against declared flows later.
    let mut flow_ref_text: Option<String> = None;
    let mut flow_ref_owner: Option<String> = None;
    let mut flow_ref_dir: &'static str = "outgoing";
    // A buffer for a multi-instance `<completionCondition>` FEEL text, and the
    // index of the activity whose `multiInstanceLoopCharacteristics` is open.
    let mut completion_condition_text: Option<String> = None;
    let mut cur_multi_instance: Option<usize> = None;
    // The boundary event currently being read (to attach its errorEventDefinition).
    let mut cur_boundary: Option<PendingBoundary> = None;
    // Index of the intermediate catch event currently being read (timer or
    // message), and a buffer for its nested `timeDuration` text while inside it.
    let mut cur_intermediate: Option<usize> = None;
    // Index of the intermediate *throw* event currently being read, so a nested
    // `linkEventDefinition` (a throw link) is captured with its source node.
    // Throw events are pass-throughs for execution, but the reference-integrity
    // validator (#851) needs the link name to check throw↔catch pairing.
    let mut cur_throw: Option<usize> = None;
    // Index of the end event currently being read, so a nested `zeebe:ioMapping`
    // (an output mapping projecting a variable when the token reaches this end
    // event, e.g. a per-branch outcome inside a sub-process) attaches to the end
    // event itself rather than falling through to the innermost enclosing
    // activity on the `io_stack` (its sub-process). Without this, several end
    // events in one sub-process — each mapping the same target — all hoist onto
    // the sub-process, and the last-parsed one clobbers the rest at sub-process
    // completion (they never attach to the reached end event that should apply).
    let mut cur_end: Option<usize> = None;
    // Index of the call activity currently being read, so a nested
    // `zeebe:calledElement processId="…"` child can record its callee.
    let mut cur_call: Option<usize> = None;
    // Index of the gateway currently being read (any of exclusive/parallel/
    // inclusive/event-based). A gateway is a pass-through element that can carry
    // `start`/`end` execution listeners (ADR 0037 — listeners apply uniformly to
    // any element); it is pushed onto the `io_stack` while open so a nested
    // `zeebe:executionListener` attaches to the gateway itself rather than being
    // dropped (top-level gateway) or mis-attached to the enclosing sub-process
    // (nested gateway) — issue #1197. Gateways never nest another flow node, so a
    // single pointer suffices to guard the balancing `io_stack` pop on close.
    let mut cur_gateway: Option<usize> = None;
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
    // Definitions-level `<escalation id=… name=…>` declarations: id -> name.
    // Captured so the reference-integrity validator (#851) can resolve an
    // `escalationRef` against a declared escalation. The parser does not model
    // escalation execution today; this only records the declarations.
    let mut escalations: HashMap<String, String> = HashMap::new();
    // Stack of activity node indices that can carry a `zeebe:ioMapping`, so a
    // nested `zeebe:input`/`zeebe:output` attaches to the innermost open
    // activity. `in_io_mapping` gates input/output reads to a real ioMapping.
    let mut io_stack: Vec<usize> = Vec::new();
    // Stack of currently-open elements, each recording the index of the flow
    // node it opened (`Some`) or `None` for a non-flow-node element. A nested
    // `<incoming>`/`<outgoing>` is attributed to the nearest enclosing flow node
    // (the top-most `Some`), not merely the most recently *added* node: for a
    // container flow node (`subProcess`/`adHocSubProcess`) whose
    // `<incoming>`/`<outgoing>` follow its nested elements, the last-added node
    // is an already-closed child, so a single pointer would misattribute the
    // reference. One entry is pushed per non-self-closing start tag and popped
    // per end tag (the tokenizer guarantees these balance), keeping the top in
    // lockstep with the open element the reference is a direct child of.
    let mut flow_node_stack: Vec<Option<usize>> = Vec::new();
    let mut in_io_mapping = false;
    // Gates `zeebe:executionListener` reads to a real `zeebe:executionListeners`
    // container; each listener attaches to the innermost open flow node on the
    // io_stack (ADR 0037) — activities, gateways, start events, and end events
    // are all pushed there, so listeners land on the correct element (#1197). A
    // boundary event is buffered in `cur_boundary` rather than on the io_stack,
    // so its listeners are routed to the pending boundary and re-attached by id
    // at build; sequence-flow ("take") listeners remain unmodelled.
    let mut in_execution_listeners = false;
    // Gates `zeebe:taskListener` reads to a real `zeebe:taskListeners` container
    // (ADR 0037 §6). Task listeners attach to the innermost open user task.
    let mut in_task_listeners = false;
    // Gates `zeebe:header` reads to a real `zeebe:taskHeaders` container; each
    // header attaches to the innermost open activity on the io_stack.
    let mut in_task_headers = false;
    // Gates `zeebe:linkedResource` reads to a real `zeebe:linkedResources`
    // container; each link attaches to the innermost open activity on the
    // io_stack. Without this a stray `linkedResource` tag elsewhere in
    // `extensionElements` (or from another namespace) would be treated as a link.
    let mut in_linked_resources = false;
    // Depth of the currently-open `<extensionElements>` subtree(s). Every child
    // of `extensionElements` is foreign-namespace vendor metadata (Zeebe's
    // `zeebe:*`, Nano's semantic `nano:*`, or any other), *not* a BPMN flow
    // element or event definition — the children Nano reads are consumed by the
    // explicit extension arms above; everything else is noise Zeebe likewise
    // ignores rather than failing the deploy. So while this is `> 0` the
    // flow-element catch-all must **not** record a tag as unmodelled, or a
    // `<nano:cost>` / stray `<zeebe:header>` would be mis-reported as an
    // unsupported element. A self-closing `<extensionElements/>` has no children
    // and emits no end tag, so it never increments this.
    let mut extension_depth: u32 = 0;

    for token in &tokens {
        // An end tag closes an element: balance the open-element stack pushed on
        // each non-self-closing start tag (see `flow_node_stack`). Done here so
        // the per-tag `Token::End` arm below stays a plain match on the tag name.
        if matches!(token, Token::End { .. }) {
            flow_node_stack.pop();
        }
        match token {
            Token::Start {
                name,
                attrs,
                self_closing,
            } => {
                // Number of flow nodes before dispatching this start tag, so we
                // can tell whether it opened a new one (see `flow_node_stack`).
                let nodes_before = current.as_ref().map(|acc| acc.nodes.len());
                match local_name(name) {
                    "process" => {
                        let id = attr(attrs, "id").ok_or(ParseError::ProcessWithoutId)?;
                        let mut acc = ProcessAcc::new(id.to_string());
                        acc.name = attr(attrs, "name").map(str::to_string);
                        current = Some(acc);
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
                    // Definitions-level escalation declarations live outside
                    // <process>; referenced by an `escalationEventDefinition
                    // escalationRef`. The value stored is the `escalationCode`
                    // (empty when absent) — the code an escalation throw raises
                    // and an escalation boundary catches (Zeebe matches by code,
                    // not name). The map key (id) drives reference-integrity
                    // resolution (#851).
                    "escalation" => {
                        if let Some(id) = attr(attrs, "id") {
                            escalations.insert(
                                id.to_string(),
                                attr(attrs, "escalationCode").unwrap_or("").to_string(),
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
                                // Push onto io_stack so a nested execution listener
                                // attaches to the start event, not the enclosing
                                // scope (#1197). A self-closing `<startEvent/>` has
                                // no children and no end tag, so only a real open
                                // element enters the io_stack (popped on
                                // `</startEvent>` when `cur_start` is set).
                                if !self_closing {
                                    cur_start = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            "endEvent" => {
                                let idx = acc.add_node(attrs, NodeKind::End);
                                // An end event can carry a `zeebe:ioMapping`
                                // (output) or execution listeners; push it so they
                                // attach to the end event, not the enclosing
                                // sub-process. A self-closing `<endEvent/>` has no
                                // children and no end tag, so only a real open
                                // element enters the io_stack (popped on
                                // `</endEvent>` when `cur_end` is set).
                                if !self_closing {
                                    cur_end = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            "exclusiveGateway" => {
                                let idx = acc.add_node(attrs, NodeKind::Exclusive);
                                // Capture the `default` flow id so it is selected
                                // only as a fallback (not by document order).
                                if let (Some(i), Some(d)) = (idx, attr(attrs, "default")) {
                                    acc.nodes[i].default_flow = Some(d.to_string());
                                }
                                // Push onto io_stack so a nested execution listener
                                // attaches to the gateway, not the enclosing scope
                                // (#1197). Self-closing gateways carry no children.
                                if !self_closing {
                                    cur_gateway = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            "parallelGateway" => {
                                let idx = acc.add_node(attrs, NodeKind::Parallel);
                                if !self_closing {
                                    cur_gateway = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // An inclusive (OR) gateway: a conditional split that
                            // may take several outgoing flows, and a synchronising
                            // join. Capture its `default` flow so it is only taken
                            // when no conditional flow matches (mirroring the
                            // exclusive gateway).
                            "inclusiveGateway" => {
                                let idx = acc.add_node(attrs, NodeKind::Inclusive);
                                if let (Some(i), Some(d)) = (idx, attr(attrs, "default")) {
                                    acc.nodes[i].default_flow = Some(d.to_string());
                                }
                                if !self_closing {
                                    cur_gateway = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            "eventBasedGateway" => {
                                let idx = acc.add_node(attrs, NodeKind::EventBased);
                                if !self_closing {
                                    cur_gateway = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
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
                            // A send task performs work via a job worker exactly
                            // like a service task: Zeebe/C8 execute it against its
                            // `zeebe:taskDefinition` (job type from that, else the
                            // element id). It is the throwing cousin of
                            // `receiveTask` (#1009); model it as a job-based
                            // service task so a flow into it resolves and it
                            // activates a single job rather than being dropped as
                            // an unmodelled element (the misleading
                            // "unknown target element" deploy error, #1168).
                            "sendTask" => {
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
                                // below. Nano executes call activities natively —
                                // on activation the engine spawns a child process
                                // instance of the callee and completes the token
                                // when the child finishes (see the `CallActivity`
                                // arm of `Engine::run_activation_body`). Inline
                                // expansion (ProcessDefinition::inline_call_activities)
                                // is a legacy opt-in used by the processos harness,
                                // not the default execution path.
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
                            // form of a call activity's callee reference. Its
                            // optional propagateAllParentVariables /
                            // propagateAllChildVariables attributes control Zeebe
                            // variable propagation across the call boundary; each
                            // defaults to `true` when absent (captured here as
                            // `Some(false)` only for an explicit `="false"`, so the
                            // builder can apply the Zeebe default).
                            "calledElement" => {
                                if let Some(idx) = cur_call {
                                    if let Some(p) = attr(attrs, "processId") {
                                        acc.nodes[idx].called_process_id = Some(p.to_string());
                                    }
                                    // Only record the explicit `="false"` case;
                                    // an explicit `="true"` is treated the same as
                                    // absent (left `None`) so the builder applies
                                    // the Zeebe `true` default, avoiding redundant
                                    // `Some(true)` values.
                                    if attr(attrs, "propagateAllParentVariables") == Some("false") {
                                        acc.nodes[idx].propagate_all_parent_variables = Some(false);
                                    }
                                    if attr(attrs, "propagateAllChildVariables") == Some("false") {
                                        acc.nodes[idx].propagate_all_child_variables = Some(false);
                                    }
                                }
                            }
                            "subProcess" => {
                                // An embedded sub-process: register it, then push
                                // its scope so nested nodes are tagged as
                                // contained in it until its end tag.
                                let idx = acc.add_node(attrs, NodeKind::SubProcess);
                                if let Some(i) = idx {
                                    acc.nodes[i].is_event_subprocess =
                                        attr(attrs, "triggeredByEvent") == Some("true");
                                }
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
                                    // BPMN `cancelRemainingInstances` defaults to
                                    // `true`; only an explicit "false" defers.
                                    acc.nodes[i].adhoc_cancel_remaining_instances =
                                        attr(attrs, "cancelRemainingInstances") != Some("false");
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
                                //
                                // Only open the pending buffer for a
                                // non-self-closing tag (#1197): a self-closing
                                // `<boundaryEvent/>` emits no matching `Token::End`
                                // to run the `cur_boundary.take()` flush, so if it
                                // seeded `cur_boundary` the buffer would stay live
                                // and capture the *next* sibling's
                                // `zeebe:executionListener` (mis-attaching it, and
                                // letting a following sequence-flow listener bypass
                                // its intended rejection). A self-closing boundary
                                // carries no event-definition child anyway, so it is
                                // never a real boundary — not buffering it is safe.
                                if !self_closing {
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
                                            compensation: false,
                                            escalation: false,
                                            escalation_ref: None,
                                            escalation_dup: false,
                                            start_listeners: Vec::new(),
                                            end_listeners: Vec::new(),
                                        });
                                    }
                                }
                            }
                            "errorEventDefinition" => {
                                let error_ref = attr(attrs, "errorRef").unwrap_or("").to_string();
                                if let Some(boundary) = cur_boundary.as_mut() {
                                    boundary.error_ref = Some(error_ref);
                                } else if let Some(idx) =
                                    flow_node_stack.iter().rev().find_map(|e| *e)
                                {
                                    // An `errorRef` on a non-boundary error event
                                    // (an error end / throw event). Recorded so the
                                    // reference-integrity validator (#851) can
                                    // resolve it against declared `<error>`s.
                                    let node_id = acc.nodes[idx].id.clone();
                                    acc.error_refs_extra.push((node_id, error_ref));
                                }
                            }
                            // `escalationEventDefinition escalationRef="…"` on an
                            // escalation throw / end / boundary event. Escalation
                            // is modelled for execution (#1173): a throw/end raises
                            // the named escalation and a boundary catches it. The
                            // `escalationRef` resolves against a definitions-level
                            // `<escalation escalationCode="…">`; a *dangling* ref is
                            // still rejected by the reference-integrity validator
                            // (#851), which runs first. An escalation carrier on any
                            // *other* placement (an escalation intermediate catch or
                            // an event-subprocess escalation start — not modelled)
                            // is recorded as unmodelled so it is cleanly rejected.
                            "escalationEventDefinition" => {
                                let escalation_ref = attr(attrs, "escalationRef");
                                if let Some(boundary) = cur_boundary.as_mut() {
                                    // A boundary carrier → an escalation boundary
                                    // event, built in `build` from the resolved
                                    // escalation code. A SECOND escalation
                                    // definition on the same boundary is a
                                    // last-wins overwrite of `escalation_ref`;
                                    // flag it so `build` rejects the ambiguous
                                    // multi-definition boundary (#1173).
                                    if boundary.escalation {
                                        boundary.escalation_dup = true;
                                    }
                                    boundary.escalation = true;
                                    boundary.escalation_ref = escalation_ref.map(str::to_string);
                                    if let Some(escalation_ref) = escalation_ref {
                                        acc.escalation_refs.push((
                                            boundary.id.clone(),
                                            escalation_ref.to_string(),
                                        ));
                                    }
                                } else if let Some(idx) =
                                    flow_node_stack.iter().rev().find_map(|e| *e)
                                {
                                    let node_id = acc.nodes[idx].id.clone();
                                    if let Some(escalation_ref) = escalation_ref {
                                        acc.escalation_refs
                                            .push((node_id.clone(), escalation_ref.to_string()));
                                    }
                                    // Only an intermediate throw or an end event
                                    // carrier is an escalation *throw*; any other
                                    // placement stays unmodelled (rejected at
                                    // deploy naming the construct).
                                    if matches!(
                                        acc.nodes[idx].kind,
                                        NodeKind::IntermediateThrow | NodeKind::End
                                    ) {
                                        // A SECOND escalation definition on the
                                        // same throw/end is a last-wins overwrite
                                        // of `escalation_ref`; flag it so `build`
                                        // rejects the ambiguous multi-definition
                                        // throw (#1173).
                                        if acc.nodes[idx].is_escalation_throw {
                                            acc.nodes[idx].escalation_throw_dup = true;
                                        }
                                        acc.nodes[idx].is_escalation_throw = true;
                                        acc.nodes[idx].escalation_ref =
                                            escalation_ref.map(str::to_string);
                                    } else {
                                        acc.unmodelled.push((
                                            "escalationEventDefinition".to_string(),
                                            node_id,
                                        ));
                                    }
                                }
                            }
                            // `linkEventDefinition name="…"` on an intermediate
                            // link throw or catch event. Recorded by direction so
                            // #851 can verify each throw link has a matching catch
                            // (Zeebe `ModelUtil.verifyLinkIntermediateEvents`).
                            // `compensateEventDefinition` on a boundary event
                            // (a compensation boundary marking its activity
                            // compensable) or on an intermediate throw / end
                            // event (a compensation throw). Modelled for
                            // execution; the boundary's handler is resolved from
                            // the `<association>` wiring at build.
                            "compensateEventDefinition" => {
                                if let Some(boundary) = cur_boundary.as_mut() {
                                    boundary.compensation = true;
                                } else if let Some(idx) =
                                    flow_node_stack.iter().rev().find_map(|e| *e)
                                {
                                    // A compensation *throw* is modelled only on an
                                    // intermediateThrowEvent or endEvent — the two
                                    // node kinds the build step interprets
                                    // `is_compensation_throw` for. On any other node
                                    // kind (e.g. a start/catch event) the flag would
                                    // be silently dropped at build and never
                                    // rejected, so record the placement as unmodelled
                                    // and let the unsupported-elements validator
                                    // (#853) reject it at deploy instead.
                                    if matches!(
                                        acc.nodes[idx].kind,
                                        NodeKind::IntermediateThrow | NodeKind::End
                                    ) {
                                        acc.nodes[idx].is_compensation_throw = true;
                                    } else {
                                        acc.unmodelled.push((
                                            "compensateEventDefinition".to_string(),
                                            acc.nodes[idx].id.clone(),
                                        ));
                                    }
                                }
                            }
                            // `terminateEventDefinition` on an `endEvent` makes it
                            // a *terminate* end event: reaching it kills every
                            // other active token in the enclosing scope and
                            // completes that scope (Zeebe/Camunda terminate
                            // semantics). Recorded on the owning end-event node so
                            // the build step maps it to
                            // `ElementKind::TerminateEndEvent`. It is only
                            // meaningful on an `endEvent`; anywhere else it is
                            // ignored (deploy-parity leniency).
                            "terminateEventDefinition" => {
                                if let Some(idx) = cur_end {
                                    acc.nodes[idx].is_terminate = true;
                                }
                            }
                            // `<association sourceRef=… targetRef=…>` — wires a
                            // compensation boundary event to its
                            // `isForCompensation` handler activity. Recorded so
                            // the boundary's handler resolves at build; ignored
                            // for any other association.
                            "association" => {
                                if let (Some(source), Some(target)) =
                                    (attr(attrs, "sourceRef"), attr(attrs, "targetRef"))
                                {
                                    acc.associations
                                        .push((source.to_string(), target.to_string()));
                                }
                            }
                            "linkEventDefinition" => {
                                let link_name = attr(attrs, "name").unwrap_or("").to_string();
                                if let Some(idx) = cur_throw {
                                    let from_node = acc.nodes[idx].id.clone();
                                    let scope = acc.nodes[idx].parent.clone();
                                    acc.nodes[idx].link_name = Some(link_name.clone());
                                    acc.link_throws.push((link_name, from_node, scope));
                                } else if let Some(idx) = cur_intermediate {
                                    let scope = acc.nodes[idx].parent.clone();
                                    acc.nodes[idx].link_name = Some(link_name.clone());
                                    acc.link_catches.push((link_name, scope));
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
                                } else if let Some(idx) = cur_start {
                                    // On a start event, marks it a *signal* start
                                    // (a typed start, distinct from a none start).
                                    // Recorded so the start-event count rule (#855)
                                    // does not mistake it for a second none start,
                                    // and so #851 can resolve its `signalRef`.
                                    acc.nodes[idx].signal_ref = Some(signal_ref);
                                }
                            }
                            "agentDefinition" => {
                                // zeebe:agentDefinition agentType="…" — the
                                // engine-native AgentInstance marker (Camunda
                                // stable/8.10). It attaches to the enclosing
                                // activity (a serviceTask or adHocSubProcess, both
                                // tracked as cur_service_task). Placement is
                                // validated at build: aiAgentTask only on a
                                // serviceTask, aiAgentSubProcess only on an
                                // adHocSubProcess (mirrors Camunda's
                                // AgentDefinitionValidator). A missing/unknown
                                // agentType, or the marker on any other element, is
                                // a hard parse error.
                                let raw = attr(attrs, "agentType").unwrap_or("");
                                let agent_type = crate::agent::AgentType::parse(raw);
                                if let Some(idx) = cur_service_task {
                                    let element_id = acc.nodes[idx].id.clone();
                                    let is_adhoc = acc.nodes[idx].is_adhoc;
                                    match agent_type {
                                        None => {
                                            let reason = if raw.is_empty() {
                                                "missing agentType attribute (expected aiAgentTask, aiAgentSubProcess or external)".to_string()
                                            } else {
                                                format!(
                                                    "unknown agentType '{raw}' (expected aiAgentTask, aiAgentSubProcess or external)"
                                                )
                                            };
                                            return Err(ParseError::InvalidAgentDefinition {
                                                process_id: acc.id.clone(),
                                                element_id,
                                                reason,
                                            });
                                        }
                                        Some(crate::agent::AgentType::AiAgentTask) if is_adhoc => {
                                            return Err(ParseError::InvalidAgentDefinition {
                                                process_id: acc.id.clone(),
                                                element_id,
                                                reason: "agentType 'aiAgentTask' is only valid on a serviceTask, not an adHocSubProcess".to_string(),
                                            });
                                        }
                                        Some(crate::agent::AgentType::AiAgentSubProcess)
                                            if !is_adhoc =>
                                        {
                                            return Err(ParseError::InvalidAgentDefinition {
                                                process_id: acc.id.clone(),
                                                element_id,
                                                reason: "agentType 'aiAgentSubProcess' is only valid on an adHocSubProcess, not a serviceTask".to_string(),
                                            });
                                        }
                                        Some(t) => acc.nodes[idx].agent_type = Some(t),
                                    }
                                } else {
                                    // Attribute the error to the nearest enclosing
                                    // flow node so it is diagnosable, rather than an
                                    // empty id (there is no owning task here).
                                    let element_id = flow_node_stack
                                        .iter()
                                        .rev()
                                        .find_map(|e| *e)
                                        .map(|idx| acc.nodes[idx].id.clone())
                                        .unwrap_or_default();
                                    return Err(ParseError::InvalidAgentDefinition {
                                        process_id: acc.id.clone(),
                                        element_id,
                                        reason: "zeebe:agentDefinition is only valid on a serviceTask or adHocSubProcess".to_string(),
                                    });
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
                            "formDefinition" => {
                                // zeebe:formDefinition inside a user task (its
                                // form) or a start event (the process start form).
                                // `formId` names an embedded / deployment form
                                // (resolved to a numeric form_key against the
                                // deployed forms at task creation);
                                // `externalReference` names an external form
                                // (surfaced verbatim). Zeebe declares exactly one
                                // of the two, so an `externalReference` wins and
                                // suppresses `formId` to keep them mutually
                                // exclusive downstream.
                                let form_id = attr(attrs, "formId")
                                    .filter(|s| !s.is_empty())
                                    .map(str::to_string);
                                let external_reference = attr(attrs, "externalReference")
                                    .filter(|s| !s.is_empty())
                                    .map(str::to_string);
                                if let Some(idx) = cur_user_task {
                                    let props = &mut acc.nodes[idx].user_task;
                                    props.external_form_reference = external_reference;
                                    props.form_id = if props.external_form_reference.is_some() {
                                        None
                                    } else {
                                        form_id
                                    };
                                    // Interim guard (#1190): the engine resolves
                                    // a user-task deployed form to the *latest*
                                    // deployed form version at task creation. The
                                    // other two Camunda bindings (`deployment`,
                                    // `versionTag`) are not implemented; rather
                                    // than silently degrading them to `latest` (a
                                    // latent mis-binding), reject the deploy
                                    // loudly. The guard keys off the *raw*
                                    // `externalReference`: an external form
                                    // resolves no deployed version, so its binding
                                    // is moot and unaffected; every *other*
                                    // (deployed) form must carry only the default
                                    // binding. Only an **absent** `bindingType`
                                    // defaults — any present value other than the
                                    // literal `latest`, including an explicitly
                                    // empty `bindingType=""`, is an explicit
                                    // unimplemented binding and is rejected rather
                                    // than silently treated as the default.
                                    if acc.nodes[idx].user_task.external_form_reference.is_none() {
                                        if let Some(binding) =
                                            attr(attrs, "bindingType").filter(|s| *s != "latest")
                                        {
                                            return Err(
                                                ParseError::UnsupportedUserTaskFormBinding {
                                                    task_id: acc.nodes[idx].id.clone(),
                                                    binding_type: binding.to_string(),
                                                },
                                            );
                                        }
                                    }
                                } else if let Some(idx) = cur_start {
                                    // Start forms are formId-only by design: the
                                    // `GetStartProcessForm` contract resolves a
                                    // *deployed* form, which an external reference
                                    // (an externally-hosted form) is not. We
                                    // intentionally do not record
                                    // `externalReference` for start events;
                                    // external start forms are out of scope until
                                    // there is a read-model/API surface for them.
                                    acc.nodes[idx].start_form_id = form_id;
                                    let _ = external_reference;
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
                                    // A catch event can carry a `zeebe:ioMapping`:
                                    // its inputs apply when the event activates and
                                    // its outputs when it is triggered (e.g. a
                                    // loop-back counter `=round + 1 -> round`). Push
                                    // it so a nested `zeebe:input`/`zeebe:output`
                                    // attaches to the event, not a dropped `None`.
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // A throw event (none/escalation/signal/message throw)
                            // is a pure pass-through for path purposes: it routes
                            // straight to its outgoing flow. Any nested event
                            // definition is ignored.
                            "intermediateThrowEvent" => {
                                let idx = acc.add_node(attrs, NodeKind::IntermediateThrow);
                                if !self_closing {
                                    cur_throw = idx;
                                    // A throw event can carry a `zeebe:ioMapping`
                                    // (e.g. a message throw). Push it onto the
                                    // io_stack — same defect class as end events —
                                    // so nested mappings attach to the throw event
                                    // rather than the enclosing activity. `cur_throw`
                                    // is Some iff the node had an id, matching the
                                    // guarded pop on `</intermediateThrowEvent>`.
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // An abstract `task` (or `manualTask`) has no
                            // execution semantics — Zeebe/C8 accept it and treat
                            // it as a pass-through. Model it as such (token
                            // completes on activation and takes its outgoing
                            // flow). Push it onto the io_stack so any
                            // `zeebe:ioMapping` on it still applies, mirroring the
                            // typed tasks.
                            "task" | "manualTask" => {
                                let idx = acc.add_node(attrs, NodeKind::Task);
                                if !self_closing {
                                    cur_plain_task = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // A receive task waits for a message. Nano's trace
                            // generator has no inbound correlation, so model it as
                            // a pass-through (the awaited event is assumed to
                            // arrive) rather than a perpetual block. Push it onto
                            // the io_stack like the other plain tasks so a
                            // `zeebe:ioMapping` or `zeebe:executionListeners`
                            // declared on it attaches to the receive task itself
                            // and fires through the shared pass-through listener
                            // gate — otherwise its listeners would fall through to
                            // `io_stack.last()` and be dropped at process root or
                            // hoisted onto the enclosing sub-process (#1197).
                            "receiveTask" => {
                                let idx = acc.add_node(attrs, NodeKind::IntermediateThrow);
                                if !self_closing {
                                    cur_plain_task = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
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
                            // A flow node's `<incoming>`/`<outgoing>` QName
                            // reference to a sequenceFlow. Its text content is the
                            // referenced flow id; captured against the owning
                            // (nearest enclosing) flow node so it can be resolved
                            // against declared flows at build time (Zeebe rejects
                            // an unresolved reference at deploy). The owner is the
                            // top-most `Some` on `flow_node_stack` — the flow node
                            // this element is a direct child of — which stays
                            // correct even when a container's `<incoming>`/
                            // `<outgoing>` follow its nested elements.
                            "incoming" | "outgoing" if !self_closing => {
                                if let Some(idx) = flow_node_stack.iter().rev().find_map(|e| *e) {
                                    flow_ref_owner = Some(acc.nodes[idx].id.clone());
                                    flow_ref_dir = if tag == "incoming" {
                                        "incoming"
                                    } else {
                                        "outgoing"
                                    };
                                    flow_ref_text = Some(String::new());
                                }
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
                            // A `<bpmn:extensionElements>` container. Its
                            // children are foreign vendor metadata (see
                            // `extension_depth`): the ones Nano reads are matched
                            // by the explicit arms above; the rest must not reach
                            // the flow-element catch-all as unmodelled elements.
                            // A self-closing container has no children, so only a
                            // real open element enters the subtree.
                            "extensionElements" => {
                                if !self_closing {
                                    extension_depth += 1;
                                }
                            }
                            // zeebe:ioMapping and its nested zeebe:input/output.
                            // input/output are only read inside an ioMapping that
                            // belongs to an open activity (the innermost on the
                            // io_stack).
                            "ioMapping" => {
                                in_io_mapping = true;
                            }
                            "taskHeaders" => {
                                // A self-closing `<zeebe:taskHeaders />` has no
                                // nested headers and emits no matching end tag, so
                                // only enter the container state for a real open
                                // element — otherwise the flag would stay stuck
                                // `true` and wrongly capture later `zeebe:header`s.
                                if !self_closing {
                                    in_task_headers = true;
                                }
                            }
                            // zeebe:header key="…" value="…" inside a
                            // zeebe:taskHeaders container: a static custom header
                            // attached to the innermost open activity. Surfaced
                            // verbatim on the activated job (Zeebe customHeaders).
                            "header" if in_task_headers => {
                                if let (Some(&idx), Some(key)) =
                                    (io_stack.last(), attr(attrs, "key"))
                                {
                                    let value = attr(attrs, "value").unwrap_or("").to_string();
                                    acc.nodes[idx].task_headers.insert(key.to_string(), value);
                                }
                            }
                            // zeebe:linkedResource inside zeebe:linkedResources: a
                            // declarative link from the innermost open job task to a
                            // deployed resource by id. Resolved to a concrete
                            // resourceKey at job activation and delivered in the
                            // `linkedResources` custom header (Zeebe parity). Gated
                            // on a real open `zeebe:linkedResources` container so a
                            // stray `linkedResource` tag elsewhere is not treated as
                            // a link (mirrors the `zeebe:taskHeaders` gate). A
                            // self-closing `<zeebe:linkedResources />` carries no
                            // children and emits no end tag, so only a real open
                            // element enters the container state.
                            "linkedResources" => {
                                if !self_closing {
                                    in_linked_resources = true;
                                }
                            }
                            "linkedResource" if in_linked_resources => {
                                // Zeebe's design-time validator requires
                                // `resourceId`, `bindingType` and `resourceType`
                                // (each non-empty) on every linkedResource. A
                                // link missing any of them is a hard deploy
                                // error: Zeebe rejects it with
                                // `INVALID_ARGUMENT` (HTTP 400) rather than
                                // silently dropping the link and leaving the
                                // task's `linkedResources` header empty at
                                // activation. Nano matches that parity here.
                                // `linkName` is intentionally *not* required
                                // (Zeebe's required set omits it); when absent it
                                // resolves to an empty link name. Like the other
                                // zeebe extension elements it attaches to the
                                // innermost open activity and is retained only
                                // where the built model keeps it (service tasks).
                                if let Some(&idx) = io_stack.last() {
                                    let nonempty =
                                        |a: &str| attr(attrs, a).filter(|v| !v.is_empty());
                                    for required in ["resourceId", "bindingType", "resourceType"] {
                                        if nonempty(required).is_none() {
                                            return Err(ParseError::InvalidLinkedResource {
                                                task_id: acc.nodes[idx].id.clone(),
                                                attribute: required.to_string(),
                                            });
                                        }
                                    }
                                    // The required-attribute loop above already
                                    // guaranteed each of these is present and
                                    // non-empty, so unwrap the invariant rather
                                    // than falling back to "" (a dead branch that
                                    // would only mask a future validation bug).
                                    let resource_id = nonempty("resourceId")
                                        .expect("resourceId validated non-empty above");
                                    let resource_type = nonempty("resourceType")
                                        .expect("resourceType validated non-empty above");
                                    let binding_type = match nonempty("bindingType")
                                        .expect("bindingType validated non-empty above")
                                    {
                                        "deployment" => crate::model::BindingType::Deployment,
                                        "versionTag" => crate::model::BindingType::VersionTag,
                                        // `latest` and any other non-empty value
                                        // default to latest (Zeebe's default).
                                        _ => crate::model::BindingType::Latest,
                                    };
                                    acc.nodes[idx].linked_resources.push(
                                        crate::model::LinkedResource {
                                            resource_id: resource_id.to_string(),
                                            binding_type,
                                            resource_type: resource_type.to_string(),
                                            version_tag: attr(attrs, "versionTag")
                                                .map(str::to_string),
                                            link_name: attr(attrs, "linkName")
                                                .unwrap_or("")
                                                .to_string(),
                                        },
                                    );
                                }
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
                            // zeebe:executionListeners and its nested
                            // zeebe:executionListener entries (ADR 0037). Each
                            // listener attaches to the innermost open activity.
                            "executionListeners" => {
                                in_execution_listeners = true;
                            }
                            "executionListener" if in_execution_listeners => {
                                if let Some(job_type) = attr(attrs, "type") {
                                    // `eventType` defaults to "start" in Zeebe when
                                    // omitted.
                                    let event_type = match attr(attrs, "eventType") {
                                        Some("end") => crate::model::ListenerEventType::End,
                                        _ => crate::model::ListenerEventType::Start,
                                    };
                                    let retries = attr(attrs, "retries").map(str::to_string);
                                    let listener = crate::model::ExecutionListener {
                                        event_type,
                                        job_type: job_type.to_string(),
                                        retries,
                                    };
                                    // A boundary event is buffered in `cur_boundary`
                                    // and never enters the `io_stack` (it is a
                                    // sibling of its host, not a nested element), so
                                    // route its listeners to the pending boundary —
                                    // otherwise `io_stack.last()` would drop them
                                    // (top-level host) or mis-attach them to the
                                    // enclosing sub-process (#1197). A boundary's
                                    // children cannot open another listener owner, so
                                    // checking `cur_boundary` first is unambiguous.
                                    let sink = if let Some(boundary) = cur_boundary.as_mut() {
                                        Some((
                                            &mut boundary.start_listeners,
                                            &mut boundary.end_listeners,
                                        ))
                                    } else if let Some(flow_idx) = cur_flow {
                                        // A `<sequenceFlow>` is an edge, not an
                                        // `Element`, so it has no lifecycle to run
                                        // an execution listener on. Rather than
                                        // silently DROP a declared take listener —
                                        // the same dead-listener failure mode this
                                        // change rejects for joins and compensation
                                        // boundaries (deploy would succeed yet the
                                        // listener could never create a job) —
                                        // reject it at deploy until #1198 adds a
                                        // real sequence-flow ("take") listener slot.
                                        // Rejecting here also forecloses the #1197
                                        // mis-attachment class: a sequence flow does
                                        // NOT push onto the `io_stack`, so a dropped
                                        // listener would otherwise fall through to
                                        // `io_stack.last()` and hoist onto the
                                        // enclosing sub-process.
                                        return Err(ParseError::UnsupportedExecutionListener {
                                            process_id: acc.id.clone(),
                                            element_id: acc.flows[flow_idx]
                                                .id
                                                .clone()
                                                .unwrap_or_else(|| "<sequenceFlow>".to_string()),
                                            reason: "a sequence flow is an edge, not an \
                                                     element, so it has no lifecycle to run \
                                                     an execution listener; sequence-flow \
                                                     (\"take\") listeners are a deferred \
                                                     subset (#1198)"
                                                .to_string(),
                                        });
                                    } else {
                                        io_stack.last().map(|&idx| {
                                            let node = &mut acc.nodes[idx];
                                            (&mut node.start_listeners, &mut node.end_listeners)
                                        })
                                    };
                                    if let Some((start, end)) = sink {
                                        match event_type {
                                            crate::model::ListenerEventType::Start => {
                                                start.push(listener)
                                            }
                                            crate::model::ListenerEventType::End => {
                                                end.push(listener)
                                            }
                                        }
                                    }
                                }
                            }
                            // zeebe:taskListeners and its nested
                            // zeebe:taskListener entries (ADR 0037 §6). Task
                            // listeners exist only on user tasks; each attaches
                            // to the innermost open activity (the user task).
                            "taskListeners" => {
                                in_task_listeners = true;
                            }
                            "taskListener" if in_task_listeners => {
                                if let (Some(&idx), Some(job_type)) =
                                    (io_stack.last(), attr(attrs, "type"))
                                {
                                    // `eventType` defaults to "creating" in Zeebe
                                    // when omitted.
                                    let event_type = match attr(attrs, "eventType") {
                                        Some("assigning") => {
                                            crate::model::TaskListenerEventType::Assigning
                                        }
                                        Some("updating") => {
                                            crate::model::TaskListenerEventType::Updating
                                        }
                                        Some("completing") => {
                                            crate::model::TaskListenerEventType::Completing
                                        }
                                        Some("canceling") => {
                                            crate::model::TaskListenerEventType::Canceling
                                        }
                                        _ => crate::model::TaskListenerEventType::Creating,
                                    };
                                    let retries = attr(attrs, "retries").map(str::to_string);
                                    acc.nodes[idx].task_listeners.push(
                                        crate::model::TaskListener {
                                            event_type,
                                            job_type: job_type.to_string(),
                                            retries,
                                        },
                                    );
                                }
                            }
                            // Any other tag inside a <process> the parser does
                            // not model. Instead of silently dropping it (which
                            // makes a flow into it fail deploy with a misleading
                            // "unknown target element" error, or an unattached
                            // element deploy clean and mis-execute), record
                            // genuinely-unmodelled flow elements / event
                            // definitions as `(tag, element_id)`. The
                            // unsupported-elements validator (#853) consumes this
                            // list; the `is_ignorable_tag` ignore-list keeps out
                            // non-flow noise (DI, documentation, structural BPMN,
                            // and the event definitions Nano does model), while the
                            // `extension_depth` guard keeps out foreign
                            // extension-element children (`zeebe:*`, `nano:*`, …)
                            // that Nano reads elsewhere or ignores — a BPMN flow
                            // element / event definition never appears inside
                            // `<extensionElements>`.
                            other => {
                                if extension_depth == 0 && !is_ignorable_tag(other) {
                                    // Prefer the tag's own `id`, but many
                                    // unmodelled constructs (notably event
                                    // definitions) carry no `id`; attribute those
                                    // to the owning element so the
                                    // unsupported-elements error stays actionable
                                    // instead of recording an empty id. Mirrors the
                                    // `errorEventDefinition`/`escalationEventDefinition`
                                    // owner-attribution above: a boundary event is
                                    // buffered in `cur_boundary` and never pushed
                                    // onto `flow_node_stack`, so an anonymous event
                                    // definition nested under one (e.g. an
                                    // unsupported `<cancelEventDefinition>` on a
                                    // `<boundaryEvent>`) must attribute to
                                    // `cur_boundary` before falling back to the open
                                    // flow-node stack — otherwise it would record a
                                    // containing flow node or an empty id.
                                    let element_id = match attr(attrs, "id") {
                                        Some(id) => id.to_string(),
                                        None => cur_boundary
                                            .as_ref()
                                            .map(|b| b.id.clone())
                                            .or_else(|| {
                                                flow_node_stack
                                                    .iter()
                                                    .rev()
                                                    .find_map(|e| *e)
                                                    .map(|i| acc.nodes[i].id.clone())
                                            })
                                            .unwrap_or_default(),
                                    };
                                    acc.unmodelled.push((other.to_string(), element_id));
                                }
                            }
                        }
                    }
                    _ => {}
                }
                // Track the open-element stack for `<incoming>`/`<outgoing>`
                // attribution. A self-closing tag never contains children, so it
                // is not pushed (and the tokenizer emits no matching end tag).
                if !self_closing {
                    let opened = match (nodes_before, current.as_ref()) {
                        (Some(before), Some(acc)) if acc.nodes.len() > before => {
                            Some(acc.nodes.len() - 1)
                        }
                        _ => None,
                    };
                    flow_node_stack.push(opened);
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
                if let Some(buf) = flow_ref_text.as_mut() {
                    buf.push_str(text);
                }
            }
            Token::End { name } => match local_name(name) {
                "process" => {
                    if let Some(acc) = current.take() {
                        processes.push(acc);
                    }
                    cur_service_task = None;
                    cur_plain_task = None;
                    cur_flow = None;
                    cur_boundary = None;
                    cur_intermediate = None;
                    cur_throw = None;
                    duration_text = None;
                    cur_start = None;
                    cycle_text = None;
                    date_text = None;
                    event_condition_text = None;
                    cur_user_task = None;
                    cur_call = None;
                    cur_gateway = None;
                    completion_condition_text = None;
                    flow_ref_text = None;
                    flow_ref_owner = None;
                    cur_multi_instance = None;
                    io_stack.clear();
                    in_io_mapping = false;
                    in_execution_listeners = false;
                    in_task_listeners = false;
                    extension_depth = 0;
                }
                "serviceTask" => {
                    // Only pop when the open handler actually pushed. The push is
                    // conditional on `add_node` returning `Some` (the task carries
                    // an `id`); an id-less task is never pushed, so an
                    // unconditional pop would detach the enclosing activity's
                    // mapping owner and misattribute later `zeebe:ioMapping` data
                    // — the same defect class the id-less event handlers guard
                    // against. Safe because a task cannot nest another activity, so
                    // `cur_service_task` here is still this task's own index.
                    if cur_service_task.is_some() {
                        io_stack.pop();
                    }
                    cur_service_task = None;
                }
                "sendTask" => {
                    if cur_service_task.is_some() {
                        io_stack.pop();
                    }
                    cur_service_task = None;
                }
                "businessRuleTask" | "scriptTask" => {
                    if cur_service_task.is_some() {
                        io_stack.pop();
                    }
                    cur_service_task = None;
                }
                "userTask" => {
                    if cur_user_task.is_some() {
                        io_stack.pop();
                    }
                    cur_user_task = None;
                }
                "task" | "manualTask" | "receiveTask" => {
                    if cur_plain_task.is_some() {
                        io_stack.pop();
                    }
                    cur_plain_task = None;
                }
                "callActivity" => {
                    if cur_call.is_some() {
                        io_stack.pop();
                    }
                    cur_call = None;
                }
                // A gateway (any flavour) balances the io_stack push done for a
                // non-self-closing open with an `id` (#1197). Guard on
                // `cur_gateway` so an id-less gateway — never pushed — does not
                // detach the enclosing activity's mapping owner.
                "exclusiveGateway" | "parallelGateway" | "inclusiveGateway"
                | "eventBasedGateway" => {
                    if cur_gateway.is_some() {
                        io_stack.pop();
                    }
                    cur_gateway = None;
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
                "extensionElements" => {
                    extension_depth = extension_depth.saturating_sub(1);
                }
                "taskHeaders" => in_task_headers = false,
                "linkedResources" => in_linked_resources = false,
                "executionListeners" => in_execution_listeners = false,
                "taskListeners" => in_task_listeners = false,
                "startEvent" => {
                    // Balance the io_stack push done for a non-self-closing start
                    // event (id-less start events are never pushed, so guard on
                    // `cur_start`) — #1197.
                    if cur_start.is_some() {
                        io_stack.pop();
                    }
                    cur_start = None;
                }
                "message" => cur_message = None,
                "boundaryEvent" => {
                    // Keep error boundaries (errorEventDefinition), timer
                    // boundaries (timerEventDefinition), message boundaries
                    // (messageEventDefinition), escalation boundaries
                    // (escalationEventDefinition, #1173) and the rest listed
                    // below; ignore boundaries carrying no recognised definition.
                    if let (Some(acc), Some(boundary)) = (current.as_mut(), cur_boundary.take()) {
                        if boundary.error_ref.is_some()
                            || boundary.timer_duration_millis.is_some()
                            || boundary.timer_expr.is_some()
                            || boundary.message_ref.is_some()
                            || boundary.signal_ref.is_some()
                            || boundary.condition.is_some()
                            || boundary.compensation
                            || boundary.escalation
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
                "incoming" | "outgoing" => {
                    if let (Some(acc), Some(owner), Some(text)) = (
                        current.as_mut(),
                        flow_ref_owner.take(),
                        flow_ref_text.take(),
                    ) {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            acc.flow_refs.push(FlowRef {
                                node_id: owner,
                                direction: flow_ref_dir,
                                flow_id: trimmed.to_string(),
                            });
                        }
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
                "intermediateCatchEvent" => {
                    // Only pop when we actually pushed. The push is conditional
                    // on `add_node` returning `Some` (i.e. the event carries an
                    // `id`); popping unconditionally would detach the parent
                    // element from `io_stack` for an id-less catch event and
                    // misattach subsequent `zeebe:ioMapping` entries.
                    if cur_intermediate.is_some() {
                        io_stack.pop();
                    }
                    cur_intermediate = None;
                }
                "intermediateThrowEvent" => {
                    // Balance the io_stack push done for a non-self-closing throw
                    // event (id-less throw events are never pushed, so guard on
                    // `cur_throw`).
                    if cur_throw.is_some() {
                        io_stack.pop();
                    }
                    cur_throw = None;
                }
                "endEvent" => {
                    // Balance the io_stack push done for a non-self-closing end
                    // event (id-less end events are never pushed, so guard on
                    // `cur_end` — mirrors the id-less catch-event handling).
                    if cur_end.is_some() {
                        io_stack.pop();
                    }
                    cur_end = None;
                }
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
        .map(|acc| {
            // Snapshot the raw parse data for post-parse validation before the
            // builder consumes the accumulator.
            let capture = acc.capture(&errors, &messages, &signals, &escalations);
            let def = acc.build(&errors, &messages, &signals, &escalations)?;
            // Retain the verbatim source XML on each parsed definition so it can
            // be served back (getProcessDefinitionXML / console diagram). Every
            // process in one resource shares that resource's XML.
            let def = ProcessDefinition {
                xml: xml.to_string(),
                ..def
            };
            Ok((capture, def))
        })
        .collect()
}

/// A flow node collected while scanning, before it becomes an [`crate::Element`].
struct NodeAcc {
    id: String,
    kind: NodeKind,
    /// The element's BPMN `name` attribute, if present. Purely descriptive;
    /// surfaced by read models (the element-instance search API's `elementName`).
    name: Option<String>,
    /// For service tasks: the resolved job type (defaults to the id at build).
    job_type: Option<String>,
    /// For call activities: the `calledElement` / `zeebe:calledElement processId`
    /// of the invoked process. Executed natively as a child process instance;
    /// inline expansion at assembly time is a legacy opt-in (used by the
    /// processos harness).
    called_process_id: Option<String>,
    /// For call activities: the `zeebe:calledElement propagateAllParentVariables`
    /// flag. `None` when the attribute is absent (defaults to `true` at build,
    /// matching Zeebe).
    propagate_all_parent_variables: Option<bool>,
    /// For call activities: the `zeebe:calledElement propagateAllChildVariables`
    /// flag. `None` when the attribute is absent (defaults to `true` at build,
    /// matching Zeebe).
    propagate_all_child_variables: Option<bool>,
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
    /// True for a `subProcess triggeredByEvent="true"` (an event sub-process). An
    /// event sub-process is triggered by its start event, not activated by token
    /// flow, so — even as a direct child of an ad-hoc container — it is NOT an
    /// activatable embedded-subProcess tool (#872) and stays on the pruned-catalog
    /// path with its inner activities.
    is_event_subprocess: bool,
    /// `zeebe:adHoc outputCollection` on an ad-hoc container, if declared.
    adhoc_output_collection: Option<String>,
    /// `zeebe:adHoc outputElement` FEEL expression on an ad-hoc container.
    adhoc_output_element: Option<String>,
    /// `zeebe:adHoc activeElementsCollection` FEEL expression (declarative
    /// `BpmnTask` variant) on an ad-hoc container.
    adhoc_active_elements: Option<String>,
    /// An ad-hoc container's `<completionCondition>` FEEL text, if declared.
    adhoc_completion_condition: Option<String>,
    /// An ad-hoc container's `cancelRemainingInstances` attribute (BPMN default
    /// `true`): whether a fulfilled completion condition cancels still-running
    /// tools or defers until they drain.
    adhoc_cancel_remaining_instances: bool,
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
    /// Execution listeners (`zeebe:executionListener`) declared on this node,
    /// split by `eventType` into start (fire on activation) and end (fire on
    /// completion) lists, in declaration order (ADR 0037).
    start_listeners: Vec<crate::model::ExecutionListener>,
    end_listeners: Vec<crate::model::ExecutionListener>,
    /// Task listeners (`zeebe:taskListener`) declared on this user task, all
    /// event types in one list in declaration order (ADR 0037 §6).
    task_listeners: Vec<crate::model::TaskListener>,
    /// Static `zeebe:taskHeaders` (`<zeebe:header key value/>`) declared on a
    /// job-based task, in a deterministic map. Surfaced verbatim on the
    /// activated job (Zeebe `ActivatedJob.customHeaders`). Empty when none.
    task_headers: std::collections::BTreeMap<String, String>,
    /// `zeebe:linkedResource`s declared on a job-based task, in declaration
    /// order. Resolved to concrete resource keys at job activation and delivered
    /// in the `linkedResources` custom header. Empty when none.
    linked_resources: Vec<crate::model::LinkedResource>,
    /// For a start event: the `zeebe:formDefinition formId` declared on it — the
    /// process's start form. `None` on other nodes and start events with no form.
    start_form_id: Option<String>,
    /// True when this `intermediateThrowEvent`/`endEvent` carries a
    /// `compensateEventDefinition`, making it a
    /// [`CompensationThrowEvent`](crate::model::ElementKind::CompensationThrowEvent).
    is_compensation_throw: bool,
    /// True when this `intermediateThrowEvent`/`endEvent` carries an
    /// `escalationEventDefinition`, making it an
    /// [`EscalationThrowEvent`](crate::model::ElementKind::EscalationThrowEvent).
    /// Its raised escalation code is resolved from `escalation_ref` at build.
    is_escalation_throw: bool,
    /// True when a SECOND `escalationEventDefinition` was seen on this
    /// throw/end event. `escalation_ref`/`is_escalation_throw` are last-wins, so
    /// a second definition would silently drop the first; `build` rejects the
    /// ambiguous multi-definition throw instead (#1173), mirroring the boundary
    /// `escalation_dup` guard.
    escalation_throw_dup: bool,
    /// The `escalationRef` on this escalation throw/end (or boundary-less) event,
    /// resolved to an `escalationCode` at build. `None` when absent.
    escalation_ref: Option<String>,
    /// The `linkEventDefinition name` on an `intermediateThrowEvent`
    /// (link *throw*) or `intermediateCatchEvent` (link *catch*), making the node
    /// a [`LinkIntermediateThrowEvent`](crate::model::ElementKind::LinkIntermediateThrowEvent)
    /// / [`LinkIntermediateCatchEvent`](crate::model::ElementKind::LinkIntermediateCatchEvent)
    /// at build. `None` on nodes without a `linkEventDefinition`.
    link_name: Option<String>,
    /// True when this `endEvent` carries a `terminateEventDefinition`, making it
    /// a [`TerminateEndEvent`](crate::model::ElementKind::TerminateEndEvent)
    /// rather than a plain none end event.
    is_terminate: bool,
    /// True when this activity is marked `isForCompensation="true"` — i.e. it is
    /// a compensation *handler*, reachable only via a compensation boundary's
    /// `<association>`, never by ordinary token flow. Used to resolve (and
    /// disambiguate) which association endpoint is the real handler.
    is_for_compensation: bool,
    /// The `agentType` from a `zeebe:agentDefinition` extension marker on this
    /// activity, if present. Classifies the ordinary job-worker element.
    /// Placement is validated
    /// at build (`aiAgentTask` only on a `serviceTask`, `aiAgentSubProcess` only
    /// on an `adHocSubProcess`), mirroring Camunda's `AgentDefinitionValidator`.
    agent_type: Option<crate::agent::AgentType>,
}

#[derive(Clone, Copy)]
enum NodeKind {
    Start,
    End,
    Service,
    User,
    Exclusive,
    Parallel,
    Inclusive,
    EventBased,
    IntermediateCatch,
    IntermediateThrow,
    /// An abstract `task`/`manualTask` — a pass-through (see [`ElementKind::Task`]).
    Task,
    SubProcess,
    /// A call activity (callee in `NodeAcc::called_process_id`); expanded inline.
    Call,
}

impl NodeKind {
    /// True for node kinds that represent a BPMN *activity* (a task-like node, a
    /// subprocess, or a call activity) — the only kinds a compensation handler
    /// (`isForCompensation="true"`) may legitimately be. Keeps boundary→handler
    /// resolution from binding a compensation `<association>` to a non-activity
    /// (e.g. a gateway or event) that stray-carries the attribute.
    fn is_activity(&self) -> bool {
        matches!(
            self,
            NodeKind::Service
                | NodeKind::User
                | NodeKind::Task
                | NodeKind::SubProcess
                | NodeKind::Call
        )
    }
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
    /// True when this boundary event carries a `compensateEventDefinition`,
    /// making it a
    /// [`CompensationBoundaryEvent`](crate::model::ElementKind::CompensationBoundaryEvent).
    /// Its handler activity is resolved from the `<association>` wiring it to the
    /// `isForCompensation` handler at build.
    compensation: bool,
    /// True when this boundary event carries an `escalationEventDefinition`,
    /// making it an
    /// [`EscalationBoundaryEvent`](crate::model::ElementKind::EscalationBoundaryEvent)
    /// (#1173). `interrupting` reflects `cancelActivity` (default `true`); the
    /// caught code is resolved from `escalation_ref` at build.
    escalation: bool,
    /// The boundary's `escalationRef`, resolved to an `escalationCode` (empty =
    /// catch-all) at build. `None` when the escalation carrier declares no ref.
    escalation_ref: Option<String>,
    /// True when a *second* `escalationEventDefinition` was seen on this same
    /// boundary. A duplicate silently overwrites `escalation_ref` (last-wins),
    /// so an exact code could be replaced by a catch-all (or vice-versa) with no
    /// diagnostic. Rejected at build as an unsupported multi-definition boundary
    /// (#1173).
    escalation_dup: bool,
    /// Execution listeners (`zeebe:executionListener`) declared on this boundary
    /// event, split into `start` (fire on activation) and `end` (fire on
    /// completion) lists (ADR 0037). A boundary event is buffered here rather
    /// than as a live `io_stack` node, so its listeners are captured on the
    /// pending boundary and re-attached to the built element in `build` — never
    /// mis-attached to the enclosing activity on the `io_stack`.
    start_listeners: Vec<crate::model::ExecutionListener>,
    end_listeners: Vec<crate::model::ExecutionListener>,
}

/// A definitions-level `<message>` declaration: its `name` and the instance
/// variable named by a nested `zeebe:subscription correlationKey`.
#[derive(Clone)]
struct MessageDecl {
    name: String,
    correlation_key: Option<String>,
}

/// A `<incoming>`/`<outgoing>` QName reference from a flow node to a
/// `sequenceFlow`, collected while scanning so it can be resolved against the
/// declared flow ids at build time (parity with Zeebe's reference resolution).
struct FlowRef {
    node_id: String,
    direction: &'static str,
    flow_id: String,
}

/// Accumulates the nodes and flows of one `<process>` as it is scanned.
struct ProcessAcc {
    id: String,
    /// The `<bpmn:process>` `name` attribute (modeller label), if present.
    name: Option<String>,
    nodes: Vec<NodeAcc>,
    flows: Vec<FlowAcc>,
    boundaries: Vec<PendingBoundary>,
    /// `<incoming>`/`<outgoing>` references declared on flow nodes, resolved
    /// against `flows` at build time.
    flow_refs: Vec<FlowRef>,
    /// `escalationRef`s declared on `escalationEventDefinition`s, as
    /// `(node_id, escalation_ref)`. Consumed by the reference-integrity
    /// validator (#851); escalation is not modelled for execution.
    escalation_refs: Vec<(String, String)>,
    /// `errorRef`s declared on `errorEventDefinition`s that are **not** on a
    /// boundary event (i.e. on end/throw error events), as `(node_id,
    /// error_ref)`. Boundary `errorRef`s are resolved in `build`; these are the
    /// extra sites the reference-integrity validator (#851) generalises over.
    error_refs_extra: Vec<(String, String)>,
    /// `(link name, throwing element id, enclosing scope)` for each
    /// `linkEventDefinition` on an intermediate *throw* event (consumed by #851's
    /// throw↔catch pairing check; the element id is the `from_node` on a rejected
    /// unpaired throw, and the scope enforces same-scope pairing at deploy).
    link_throws: Vec<(String, String, Option<String>)>,
    /// Link names declared on `linkEventDefinition`s of intermediate *catch*
    /// events, paired with their enclosing scope (consumed by #851's throw↔catch
    /// pairing check).
    link_catches: Vec<(String, Option<String>)>,
    /// Flow-element tags / event definitions the streaming parser does not
    /// model, recorded as `(tag, element_id)` instead of being silently
    /// dropped. `element_id` is the tag's own `id`, or — when the tag is
    /// anonymous (event definitions commonly are) — the id of its owning open
    /// flow node, so the unsupported-elements error stays actionable. Consumed
    /// by the unsupported-elements validator (#853).
    unmodelled: Vec<(String, String)>,
    /// `<association>` `(sourceRef, targetRef)` pairs, used to wire a
    /// compensation boundary event to its `isForCompensation` handler activity.
    associations: Vec<(String, String)>,
    /// Stack of open embedded sub-process ids, used to scope nested nodes.
    scope_stack: Vec<String>,
}

impl ProcessAcc {
    fn new(id: String) -> Self {
        Self {
            id,
            name: None,
            nodes: Vec::new(),
            flows: Vec::new(),
            boundaries: Vec::new(),
            flow_refs: Vec::new(),
            escalation_refs: Vec::new(),
            error_refs_extra: Vec::new(),
            link_throws: Vec::new(),
            link_catches: Vec::new(),
            unmodelled: Vec::new(),
            associations: Vec::new(),
            scope_stack: Vec::new(),
        }
    }

    /// Adds a flow node; returns its index, or `None` if it had no `id`.
    fn add_node(&mut self, attrs: &[(String, String)], kind: NodeKind) -> Option<usize> {
        let id = attr(attrs, "id")?;
        self.nodes.push(NodeAcc {
            id: id.to_string(),
            kind,
            name: attr(attrs, "name").map(str::to_string),
            job_type: None,
            called_process_id: None,
            propagate_all_parent_variables: None,
            propagate_all_child_variables: None,
            job_priority: None,
            duration_millis: None,
            message_ref: None,
            signal_ref: None,
            timer_repeating: None,
            parent: self.scope_stack.last().cloned(),
            user_task: crate::model::UserTaskProps::default(),
            is_adhoc: false,
            is_event_subprocess: false,
            adhoc_output_collection: None,
            adhoc_output_element: None,
            adhoc_active_elements: None,
            adhoc_completion_condition: None,
            adhoc_cancel_remaining_instances: true,
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
            start_listeners: Vec::new(),
            end_listeners: Vec::new(),
            task_listeners: Vec::new(),
            task_headers: std::collections::BTreeMap::new(),
            linked_resources: Vec::new(),
            start_form_id: None,
            is_compensation_throw: false,
            is_escalation_throw: false,
            escalation_throw_dup: false,
            escalation_ref: None,
            link_name: None,
            is_terminate: false,
            is_for_compensation: attr(attrs, "isForCompensation") == Some("true"),
            agent_type: None,
        });
        let idx = self.nodes.len() - 1;
        Some(idx)
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

    /// Snapshots the raw parse data a post-parse validator may need into a
    /// [`ProcessCapture`](crate::validate::ProcessCapture), owning all of it so
    /// the accumulator can then be consumed by [`build`](Self::build). Called
    /// *before* `build` so reference sites are seen against the raw parsed flows
    /// (before ad-hoc pruning), mirroring what Zeebe validates at deploy.
    fn capture(
        &self,
        errors: &HashMap<String, String>,
        messages: &HashMap<String, MessageDecl>,
        signals: &HashMap<String, String>,
        escalations: &HashMap<String, String>,
    ) -> crate::validate::ProcessCapture {
        use crate::validate::{FlowRefCapture, RefSite, TaskDefCapture, UnmodelledElement};

        let flow_ids = self
            .flows
            .iter()
            .filter_map(|f| f.id.clone())
            .collect::<std::collections::HashSet<String>>();

        let flow_refs = self
            .flow_refs
            .iter()
            .map(|r| FlowRefCapture {
                node_id: r.node_id.clone(),
                direction: r.direction,
                flow_id: r.flow_id.clone(),
            })
            .collect();

        // Gather every non-`<incoming>`/`<outgoing>` reference site the parser
        // recorded onto nodes, boundaries and the dedicated escalation/error
        // vecs, keyed by the reference kind #851 resolves against.
        let mut references: Vec<RefSite> = Vec::new();
        for node in &self.nodes {
            if let Some(id) = &node.message_ref {
                references.push(RefSite {
                    kind: "messageRef",
                    id: id.clone(),
                    from_node: node.id.clone(),
                });
            }
            if let Some(id) = &node.signal_ref {
                references.push(RefSite {
                    kind: "signalRef",
                    id: id.clone(),
                    from_node: node.id.clone(),
                });
            }
            if let Some(id) = &node.default_flow {
                references.push(RefSite {
                    kind: "default",
                    id: id.clone(),
                    from_node: node.id.clone(),
                });
            }
        }
        for boundary in &self.boundaries {
            if let Some(id) = &boundary.attached_to {
                references.push(RefSite {
                    kind: "attachedToRef",
                    id: id.clone(),
                    from_node: boundary.id.clone(),
                });
            }
            if let Some(id) = &boundary.error_ref {
                references.push(RefSite {
                    kind: "errorRef",
                    id: id.clone(),
                    from_node: boundary.id.clone(),
                });
            }
            if let Some(id) = &boundary.message_ref {
                references.push(RefSite {
                    kind: "messageRef",
                    id: id.clone(),
                    from_node: boundary.id.clone(),
                });
            }
            if let Some(id) = &boundary.signal_ref {
                references.push(RefSite {
                    kind: "signalRef",
                    id: id.clone(),
                    from_node: boundary.id.clone(),
                });
            }
        }
        for (node_id, escalation_ref) in &self.escalation_refs {
            references.push(RefSite {
                kind: "escalationRef",
                id: escalation_ref.clone(),
                from_node: node_id.clone(),
            });
        }
        for (node_id, error_ref) in &self.error_refs_extra {
            references.push(RefSite {
                kind: "errorRef",
                id: error_ref.clone(),
                from_node: node_id.clone(),
            });
        }

        let task_definitions = self
            .nodes
            .iter()
            .filter(|n| matches!(n.kind, NodeKind::Service))
            .map(|n| TaskDefCapture {
                task_id: n.id.clone(),
                job_type: n.job_type.clone(),
                retries: n.retries.clone(),
            })
            .collect();

        crate::validate::ProcessCapture {
            process_id: self.id.clone(),
            flow_ids,
            flow_refs,
            references,
            link_throws: self.link_throws.clone(),
            link_catches: self.link_catches.clone(),
            unmodelled: self
                .unmodelled
                .iter()
                .map(|(tag, element_id)| UnmodelledElement {
                    tag: tag.clone(),
                    element_id: element_id.clone(),
                })
                .collect(),
            declared_messages: messages.keys().cloned().collect(),
            declared_errors: errors.keys().cloned().collect(),
            declared_signals: signals.keys().cloned().collect(),
            signal_names: signals.clone(),
            declared_escalations: escalations.keys().cloned().collect(),
            task_definitions,
        }
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
        escalations: &HashMap<String, String>,
    ) -> Result<ProcessDefinition, ParseError> {
        // Reject `zeebe:executionListener`s declared where they can never fire
        // (#1197), rather than silently storing a listener that creates no job:
        //
        // * A **multi-incoming parallel gateway** is a *join*:
        //   `arrive_at_parallel_join` synchronises tokens and, once the threshold
        //   is met, emits `ElementCompleting` → `ElementCompleted` directly —
        //   running neither the listener-aware activation body (`start`) nor the
        //   end-listener chain (`end`), so *neither* phase of a listener on it can
        //   fire.
        // * A **multi-incoming inclusive gateway** is a *join* too, but it fires
        //   at token quiescence in `fire_ready_inclusive_joins`, which DOES defer
        //   the join behind its end-listener chain (`begin_end_listener_chain`) —
        //   so an `end` listener on an inclusive join fires and is supported. Only
        //   its `start` listener never fires (the join short-circuits the
        //   activation body), so we reject `start` alone.
        // * Single-incoming parallel/inclusive gateways (splits) and exclusive
        //   merges run the normal activation body, so their listeners fire — only
        //   the join placements above are unsupported.
        // * A **compensation boundary** is a passive structural marker, armed
        //   implicitly when its host completes and never entered by token flow,
        //   so it has no lifecycle to hang a listener on.
        {
            let mut incoming: HashMap<&str, usize> = HashMap::new();
            for flow in &self.flows {
                if let Some(target) = flow.target.as_deref() {
                    *incoming.entry(target).or_default() += 1;
                }
            }
            for node in &self.nodes {
                let multi_incoming = incoming.get(node.id.as_str()).copied().unwrap_or(0) > 1;
                if !multi_incoming {
                    continue;
                }
                match node.kind {
                    NodeKind::Parallel
                        if !node.start_listeners.is_empty() || !node.end_listeners.is_empty() =>
                    {
                        return Err(ParseError::UnsupportedExecutionListener {
                            process_id: self.id.clone(),
                            element_id: node.id.clone(),
                            reason: "a multi-incoming parallel gateway (a join) synchronises \
                                     tokens and completes without running the listener-aware \
                                     activation body or the end-listener chain, so neither a \
                                     `start` nor an `end` execution listener on it would fire \
                                     (a listener on a single-incoming split is supported)"
                                .to_string(),
                        });
                    }
                    NodeKind::Inclusive if !node.start_listeners.is_empty() => {
                        return Err(ParseError::UnsupportedExecutionListener {
                            process_id: self.id.clone(),
                            element_id: node.id.clone(),
                            reason: "a multi-incoming inclusive gateway (a join) fires at \
                                     token quiescence and never runs the listener-aware \
                                     activation body, so a `start` execution listener on it \
                                     would never fire (its `end` listener IS supported — the \
                                     quiescence sweep defers the join behind the end-listener \
                                     chain; a listener on a single-incoming split is supported)"
                                .to_string(),
                        });
                    }
                    _ => {}
                }
            }
            for boundary in &self.boundaries {
                if boundary.compensation
                    && (!boundary.start_listeners.is_empty() || !boundary.end_listeners.is_empty())
                {
                    return Err(ParseError::UnsupportedExecutionListener {
                        process_id: self.id.clone(),
                        element_id: boundary.id.clone(),
                        reason: "a compensation boundary event is a passive structural \
                                 marker that is never activated by token flow, so its \
                                 execution listeners would never fire"
                            .to_string(),
                    });
                }
            }
        }

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
        // Reject an execution listener declared on a *tool* of an ad-hoc
        // sub-process — a node whose direct parent is an ad-hoc container (#1197).
        // A leaf tool is pruned below (flattened into the non-executable
        // `AdHocTool` catalog and never activated through the lifecycle) and a
        // retained embedded-sub-process tool is activated/completed with direct
        // lifecycle events, both bypassing `advance_listener`, so a `start`/`end`
        // listener on the tool could never create a job. Refuse the model at
        // deploy rather than accept a dead listener that silently disappears —
        // the same reject-don't-drop contract applied to sequence flows and
        // joins. Checked pre-pruning (leaf tools are removed just below) and only
        // for the tool ITSELF: listeners on deeper elements inside a retained
        // sub-process tool's body run by ordinary token flow and DO fire, so they
        // are left alone. Document order keeps the error deterministic.
        if let Some(bad) = self.nodes.iter().find(|n| {
            (!n.start_listeners.is_empty() || !n.end_listeners.is_empty())
                && n.parent.as_deref().is_some_and(|p| adhoc_ids.contains(p))
        }) {
            return Err(ParseError::UnsupportedExecutionListener {
                process_id: self.id.clone(),
                element_id: bad.id.clone(),
                reason: "an execution listener on a tool of an ad-hoc sub-process is not \
                         supported: a leaf tool is pruned and a retained embedded tool is \
                         activated/completed with direct lifecycle events, both bypassing the \
                         listener gate, so the listener could never fire"
                    .to_string(),
            });
        }
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
            // Embedded `subProcess` tools (issue #872): a plain `bpmn:subProcess`
            // that is a DIRECT child of an ad-hoc container is a multi-element
            // token-flow tool — its body must run by ordinary token flow, so
            // (unlike a leaf service/user-task tool) neither the subProcess
            // element nor its body is pruned; both are retained in the executable
            // graph and reached only when the tool is activated. A nested
            // `adHocSubProcess` (agent-of-agents, #631) is NOT such a tool — it is
            // `is_adhoc` and keeps its own pruned-catalog handling — so exclude it
            // here.
            let subprocess_tool_ids: std::collections::HashSet<String> = self
                .nodes
                .iter()
                .filter(|n| {
                    matches!(n.kind, NodeKind::SubProcess)
                        && !n.is_adhoc
                        && !n.is_event_subprocess
                        && n.parent
                            .as_deref()
                            .map(|p| adhoc_ids.contains(p))
                            .unwrap_or(false)
                })
                .map(|n| n.id.clone())
                .collect();
            // Is `id` the subProcess tool itself or one of its body descendants?
            // Walk ancestor-or-self: a subProcess-tool ancestor means retained; an
            // ad-hoc ancestor reached first (e.g. a nested ad-hoc container's own
            // tool) means it is pruned by the ad-hoc rule, not retained.
            let in_subprocess_tool = |id: &str| -> bool {
                let mut cur = Some(id);
                while let Some(c) = cur {
                    if subprocess_tool_ids.contains(c) {
                        return true;
                    }
                    if adhoc_ids.contains(c) {
                        return false;
                    }
                    cur = parent_of.get(c).copied();
                }
                false
            };
            let pruned: std::collections::HashSet<String> = self
                .nodes
                .iter()
                .filter(|n| inside_adhoc(&n.id) && !in_subprocess_tool(&n.id))
                .map(|n| n.id.clone())
                .collect();

            // A pruned inner element is flattened into an `AdHocTool` whose
            // `AdHocToolKind` is `Other` for any node kind the catalog doesn't
            // model as an executable tool (only Service/User/Call/SubProcess are).
            // Activating an `Other` tool completes it directly, which is harmless
            // for a plain none-throw but SILENTLY DROPS the runtime side-effect of
            // an escalation/compensation *throw*: the escalation is never raised
            // (its boundary handler never fires) and the compensation is never
            // triggered. Executing these as ad-hoc tools is a larger runtime
            // feature nano does not yet support, so — rather than let the model
            // misbehave at runtime — reject the placement at deploy naming the
            // construct (mirrors the start/end-event rejections below and the
            // "reject, don't silently drop" contract). Iterate `self.nodes` in
            // document order so the error is deterministic.
            if let Some(bad) = self.nodes.iter().find(|n| {
                pruned.contains(&n.id) && (n.is_escalation_throw || n.is_compensation_throw)
            }) {
                let construct = if bad.is_escalation_throw {
                    "escalation throw event"
                } else {
                    "compensation throw event"
                };
                return Err(ParseError::InvalidProcess {
                    process_id: self.id.clone(),
                    reason: format!(
                        "ad-hoc sub-process must not contain a {construct} ({}); \
                         nano cannot execute a throw event as an ad-hoc tool, so its \
                         escalation/compensation would be silently dropped",
                        bad.id
                    ),
                });
            }

            // Deploy-time ad-hoc validation (Zeebe `AdHocSubProcessValidator`):
            // reject structurally invalid containers before they can run, so a bad
            // model fails at parse/deploy rather than misbehaving at runtime. The
            // inner elements are pruned just below, so this must inspect
            // `self.nodes` (pre-pruning). Rules mirror the oracle where nano shares
            // its semantics:
            //   * at least one inner activity;
            //   * no start/end events inside the container;
            //   * a `zeebe:taskDefinition` (job-worker variant) forbids
            //     `activeElementsCollection` (the declarative BPMN_TASK selector,
            //     which nano treats as mutually exclusive with a job — see the
            //     `impl_type` split below);
            //   * `outputElement` and `outputCollection` are both-or-neither.
            //
            // Deliberate divergence from Zeebe: nano does NOT forbid a
            // `<completionCondition>` or `cancelRemainingInstances=false` alongside
            // a `taskDefinition`. Zeebe rejects those because its JOB_WORKER path
            // carries completion/cancel purely on the job result. nano instead
            // supports an engine-side `<completionCondition>` on the JOB_WORKER
            // container (an intentional parity extension exercised by the engine
            // tests), and `cancelRemainingInstances` deferral is tracked separately
            // (issue #614 gap 7), so enforcing those two Zeebe rules here would
            // reject valid nano models. See issue #614 gap 6.
            for n in self.nodes.iter().filter(|n| n.is_adhoc) {
                let invalid = |reason: String| ParseError::InvalidProcess {
                    process_id: self.id.clone(),
                    reason,
                };
                // Zeebe validates the container's DIRECT flow elements
                // (`getFlowElements()`), not elements nested inside an inner
                // sub-process, so match on the immediate parent only.
                let inner: Vec<&NodeAcc> = self
                    .nodes
                    .iter()
                    .filter(|c| c.parent.as_deref() == Some(n.id.as_str()))
                    .collect();
                // Zeebe requires at least one *activity* (task / sub-process /
                // call activity), not merely any flow node: a container holding
                // only gateways or events is still structurally invalid. Checking
                // `inner.is_empty()` alone would wrongly accept such a model, so
                // match on the activity node kinds explicitly.
                let has_activity = inner.iter().any(|c| {
                    matches!(
                        c.kind,
                        NodeKind::Service | NodeKind::User | NodeKind::SubProcess | NodeKind::Call
                    )
                });
                if !has_activity {
                    return Err(invalid(format!(
                        "ad-hoc sub-process {} must have at least one activity",
                        n.id
                    )));
                }
                if inner.iter().any(|c| matches!(c.kind, NodeKind::Start)) {
                    return Err(invalid(format!(
                        "ad-hoc sub-process {} must not contain a start event",
                        n.id
                    )));
                }
                if inner.iter().any(|c| matches!(c.kind, NodeKind::End)) {
                    return Err(invalid(format!(
                        "ad-hoc sub-process {} must not contain an end event",
                        n.id
                    )));
                }
                // `taskDefinition` presence is authoritative here as `job_type`
                // (set only by `zeebe:taskDefinition`; not yet defaulted to the id).
                // A job-backed container ignores `activeElementsCollection` (the
                // declarative selector), so declaring both is a contradictory model.
                if n.job_type.is_some()
                    && n.adhoc_active_elements
                        .as_deref()
                        .is_some_and(|s| !s.is_empty())
                {
                    return Err(invalid(format!(
                        "ad-hoc sub-process {} must not define activeElementsCollection in combination with zeebe:taskDefinition",
                        n.id
                    )));
                }
                let output_element_empty =
                    n.adhoc_output_element.as_deref().unwrap_or("").is_empty();
                let output_collection_empty = n
                    .adhoc_output_collection
                    .as_deref()
                    .unwrap_or("")
                    .is_empty();
                if output_element_empty != output_collection_empty {
                    return Err(invalid(format!(
                        "ad-hoc sub-process {} must set outputElement and outputCollection both or neither",
                        n.id
                    )));
                }
            }

            // Embedded `subProcess` tools (#872) are activated by injecting a
            // token at their inner start event (`activate_adhoc_tool` does a
            // `Step::Activate` on it), so each MUST have exactly one direct-child
            // start event. Zero would leave the tool with no entry token — and
            // the catalog build below would silently default the start id to ""
            // (`unwrap_or_default`), so activation would `Step::Activate` a
            // non-existent element and hard-fail at runtime. More than one makes
            // the injection target ambiguous. Reject both at parse/deploy time
            // with a clear error rather than defaulting. Iterate in document
            // order (over `self.nodes`, not the `HashSet`) so the error is
            // deterministic when several tools are malformed.
            for n in self
                .nodes
                .iter()
                .filter(|n| subprocess_tool_ids.contains(&n.id))
            {
                let start_count = self
                    .nodes
                    .iter()
                    .filter(|c| {
                        matches!(c.kind, NodeKind::Start)
                            && c.parent.as_deref() == Some(n.id.as_str())
                    })
                    .count();
                if start_count != 1 {
                    return Err(ParseError::InvalidProcess {
                        process_id: self.id.clone(),
                        reason: format!(
                            "embedded subProcess tool {} must have exactly one start event, found {}",
                            n.id, start_count
                        ),
                    });
                }
            }

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
                    cancel_remaining_instances: n.adhoc_cancel_remaining_instances,
                    tools: Vec::new(),
                    inner_flows: Vec::new(),
                });
            }
            // Assign each pruned tool — and each retained embedded-subProcess
            // tool (#872) — to its nearest ad-hoc container, preserving document
            // order. A subProcess tool stays in the executable graph, so it is
            // NOT in `pruned`; it is added to the catalog here so the agent can
            // activate it by id and `activate_adhoc_tool` can inject a token at
            // its body's start event.
            for n in self
                .nodes
                .iter()
                .filter(|n| pruned.contains(&n.id) || subprocess_tool_ids.contains(&n.id))
            {
                let kind = if subprocess_tool_ids.contains(&n.id) {
                    // The inner start event (a Start node whose parent is this
                    // subProcess tool) — where a token is injected on activation.
                    // The validation loop above guarantees exactly one such node,
                    // so `find` always succeeds; the `unwrap_or_default` is only a
                    // belt-and-braces fallback that the parse-time check prevents.
                    let start_event = self
                        .nodes
                        .iter()
                        .find(|c| {
                            matches!(c.kind, NodeKind::Start)
                                && c.parent.as_deref() == Some(n.id.as_str())
                        })
                        .map(|c| c.id.clone())
                        .unwrap_or_default();
                    crate::model::AdHocToolKind::SubProcess { start_event }
                } else {
                    match n.kind {
                        NodeKind::Service => crate::model::AdHocToolKind::ServiceTask {
                            job_type: n.job_type.clone().unwrap_or_else(|| n.id.clone()),
                        },
                        NodeKind::User => {
                            crate::model::AdHocToolKind::UserTask(n.user_task.clone())
                        }
                        NodeKind::Call => crate::model::AdHocToolKind::CallActivity {
                            process_id: n.called_process_id.clone(),
                            propagate_all_parent_variables: n
                                .propagate_all_parent_variables
                                .unwrap_or(true),
                            propagate_all_child_variables: n
                                .propagate_all_child_variables
                                .unwrap_or(true),
                        },
                        _ => crate::model::AdHocToolKind::Other,
                    }
                };
                if let Some(pos) = nearest_adhoc(&n.id).and_then(|c| index.get(&c).copied()) {
                    adhoc_catalog[pos].tools.push(crate::model::AdHocTool {
                        element_id: n.id.clone(),
                        name: n.name.clone().unwrap_or_default(),
                        kind,
                        io: n.io.clone(),
                    });
                }
            }

            // Capture the `bpmn:sequenceFlow`s between the container's DIRECT
            // children (issue #1154) BEFORE the pruning below drops them. Camunda
            // lets an ad-hoc container's inner elements be "connected by a
            // sequence flow to build a structured sequence": on the source's
            // completion the flow is taken and the target runs. These flows
            // reference pruned (or retained-subprocess-tool) elements, so — like
            // the tool catalog — they are captured here and driven by the ad-hoc
            // runtime seam rather than by token flow. Only flows whose source AND
            // target are both DIRECT children of the SAME ad-hoc container are
            // captured; a flow inside an embedded-`subProcess` tool's body (parent
            // is the subProcess, not the container) is left to run by ordinary
            // token flow.
            for f in &self.flows {
                let (Some(src), Some(tgt)) = (f.source.as_deref(), f.target.as_deref()) else {
                    continue;
                };
                let src_parent = parent_of.get(src).copied();
                let tgt_parent = parent_of.get(tgt).copied();
                if let (Some(sp), Some(tp)) = (src_parent, tgt_parent) {
                    if sp == tp && adhoc_ids.contains(sp) {
                        if let Some(pos) = index.get(sp).copied() {
                            adhoc_catalog[pos]
                                .inner_flows
                                .push(crate::model::AdHocInnerFlow {
                                    from: src.to_string(),
                                    to: tgt.to_string(),
                                    condition: f
                                        .condition
                                        .clone()
                                        .map(crate::model::Condition::new),
                                });
                        }
                    }
                }
            }

            // ADR 0023 seam 2 (runtime): the inner LEAF "tool" activities
            // (service / user tasks, call activities, gateways, events) are pruned
            // from the executable graph — an ad-hoc container is flattened to a
            // single job-bearing activity, so those tools are never reached by
            // ordinary token flow. They are NOT lost: the catalog above captures
            // each tool's id + kind (job type), which is all the runtime needs to
            // activate one when the agent's job result requests it (the container
            // scope + activate-element seeding drive execution, not the flat
            // element graph). An embedded-`subProcess` tool and its body are the
            // exception (#872): they are NOT in `pruned`, so they stay in
            // `self.nodes`/`flows` and run by token flow when the tool is
            // activated. Pruning the leaf tools keeps `ProcessDefinition.elements`
            // otherwise minimal, avoiding a modeler cascade over arbitrary inner
            // leaf activities.
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
        // and a "manual intake" start that merge downstream). Zeebe permits a none
        // start alongside any number of *typed* (message/timer/signal) starts, and
        // wires EVERY typed start independently: a message start opens a
        // subscription and a timer start arms a process-level timer at deploy, each
        // firing its own instance at its own start element (issue #855). So message
        // and timer process-level starts are ALL kept here as their typed element
        // kinds — `Engine::deploy` scans them and registers a trigger for each; the
        // single process-entry start (used only for CreateInstance) is designated
        // downstream in `ProcessBuilder::build`.
        //
        // Only surplus *signal* starts are demoted to inert throw events (they keep
        // their outgoing flow, so it still has a valid source, but lacking any
        // incoming flow are never activated). Nano has no dedicated signal-start
        // element kind — a surviving signal start builds to a plain
        // `ElementKind::StartEvent`, indistinguishable from a none start, so a
        // second one would be miscounted as a second *none* start and wrongly
        // rejected by the `start_events` validator (#855); signal-start runtime is
        // out of scope. Surplus *none* starts are deliberately NOT demoted —
        // multiple none starts are illegal, and keeping them lets that validator
        // see and reject them (`InvalidStartEvents`) rather than silently
        // collapsing them.
        let proc_starts: Vec<usize> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| matches!(n.kind, NodeKind::Start) && n.parent.is_none())
            .map(|(i, _)| i)
            .collect();
        if proc_starts.len() > 1 {
            // A none start carries no message/timer/signal event definition.
            let is_none_start = |n: &NodeAcc| {
                n.message_ref.is_none() && n.timer_repeating.is_none() && n.signal_ref.is_none()
            };
            // A signal start carries a signalRef but no message/timer definition.
            let is_signal_start = |n: &NodeAcc| {
                n.signal_ref.is_some() && n.message_ref.is_none() && n.timer_repeating.is_none()
            };
            // Keep a single none/signal start as the process-entry `StartEvent`
            // (prefer a none start); any additional *signal* start is surplus.
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
                // Demote only surplus *signal* starts; keep none/message/timer (see
                // the note above — none starts are kept for the validator, and
                // message/timer starts are kept so deploy can wire their triggers).
                if i != designated && is_signal_start(&self.nodes[i]) {
                    self.nodes[i].kind = NodeKind::IntermediateThrow;
                    self.nodes[i].message_ref = None;
                    self.nodes[i].timer_repeating = None;
                    self.nodes[i].duration_millis = None;
                    self.nodes[i].signal_ref = None;
                }
            }
        }

        let mut builder = ProcessBuilder::new(self.id.clone());
        if let Some(name) = &self.name {
            builder = builder.name(name.clone());
        }
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
        // The process-level start event's form id (`zeebe:formDefinition formId`),
        // captured as nodes are consumed and set on the built definition below.
        let mut start_form_id: Option<String> = None;
        // Ids of activities marked `isForCompensation="true"` — the only valid
        // targets of a compensation boundary's `<association>`. Collected before
        // `self.nodes` is consumed below so the boundary-handler resolution can
        // reject associations that point at a non-handler node (e.g. a
        // `textAnnotation`) and detect ambiguity.
        let compensation_handler_ids: std::collections::HashSet<String> = self
            .nodes
            .iter()
            .filter(|n| n.is_for_compensation && n.kind.is_activity())
            .map(|n| n.id.clone())
            .collect();
        // Ids of every node that is the *source* of at least one sequence flow —
        // i.e. it declares an outgoing flow. Used to enforce, at the one site
        // that still knows an element came from an `<endEvent>`, that an end
        // event has no outgoing flow *regardless of its event-definition
        // flavour*. An escalation/compensation end event is remapped to a
        // *throw* element below, which the model-level
        // `cheap_rules::end_events_have_no_outgoing` rule cannot police (an
        // *intermediate* escalation/compensation throw legitimately has an
        // outgoing flow), so a malformed `<endEvent>…<escalationEventDefinition/>`
        // with an outgoing flow would otherwise silently deploy as a routing
        // intermediate throw (#1173).
        let flow_source_ids: std::collections::HashSet<String> =
            self.flows.iter().filter_map(|f| f.source.clone()).collect();
        // Ids of every node an escalation boundary event may legally attach to.
        // An escalation propagates *out* of the inner token scope it is raised
        // in, so only a container that opens such a scope — an embedded
        // sub-process or an ad-hoc sub-process — can catch it. A boundary on any
        // other activity (plain service/user task, call activity) is a dead
        // boundary that `find_catching_escalation_boundary` can never reach
        // (#1173).
        let escalation_container_ids: std::collections::HashSet<String> = self
            .nodes
            .iter()
            .filter(|n| matches!(n.kind, NodeKind::SubProcess) || n.is_adhoc)
            .map(|n| n.id.clone())
            .collect();
        // Ids of ad-hoc sub-process containers, and the parent of every node.
        // Used to reject an escalation boundary attached to an activity that is
        // itself a *tool* of an ad-hoc sub-process (its parent is an ad-hoc
        // container). The runtime arms boundary events on an embedded-subProcess
        // ad-hoc tool, but an interrupting boundary firing on such a tool cannot
        // release it back to its parent container's active set (the tool →
        // agent-re-drive completion path is not wired for boundary interruption),
        // so the container would hang `Active` forever. Refuse the model at deploy
        // rather than mis-execute it (#1173); full runtime support is a follow-up.
        let adhoc_container_ids: std::collections::HashSet<String> = self
            .nodes
            .iter()
            .filter(|n| n.is_adhoc)
            .map(|n| n.id.clone())
            .collect();
        let node_parent: HashMap<String, String> = self
            .nodes
            .iter()
            .filter_map(|n| n.parent.clone().map(|p| (n.id.clone(), p)))
            .collect();
        // Node ids carrying a non-boundary `errorEventDefinition` (an error
        // throw / end). Recorded separately from the node flags (in
        // `error_refs_extra`), so surfaced here as a set for the competing
        // event-definition guard below (#1173).
        let error_def_node_ids: std::collections::HashSet<String> = self
            .error_refs_extra
            .iter()
            .map(|(node_id, _)| node_id.clone())
            .collect();
        for node in self.nodes {
            // Reject an `endEvent` / `intermediateThrowEvent` that carries more
            // than one throw/end event definition (#1173). Multiple definitions
            // set multiple independent node flags, and the build dispatch below
            // picks the FIRST it checks (escalation → compensation → terminate →
            // link → …), silently discarding the others — e.g. an end event with
            // both an escalation and a terminate definition would model as a bare
            // escalation throw, dropping the terminate semantics. There is no
            // combined-semantics element in the supported subset, so refuse the
            // ambiguous model at deploy rather than pick one arm.
            if matches!(node.kind, NodeKind::End | NodeKind::IntermediateThrow) {
                let defs = node.is_escalation_throw as u32
                    + node.is_compensation_throw as u32
                    + node.is_terminate as u32
                    + node.link_name.is_some() as u32
                    + error_def_node_ids.contains(&node.id) as u32;
                // `defs > 1`: competing definitions of different kinds.
                // `escalation_throw_dup`: two `escalationEventDefinition`s on the
                // SAME throw/end — a last-wins overwrite of `escalation_ref` that
                // would silently drop the first (#1173), the throw/end analogue of
                // the boundary `escalation_dup` guard.
                if defs > 1 || node.escalation_throw_dup {
                    return Err(match node.kind {
                        // The `endEvent` flavour has a reason-bearing error; an
                        // `intermediateThrowEvent` reuses the throw-side
                        // `UnsupportedElement` rejection (as the wrong-placement
                        // escalation/compensation throws already do).
                        NodeKind::End if node.escalation_throw_dup => ParseError::InvalidEndEvent {
                            process_id: self.id.clone(),
                            element_id: node.id.clone(),
                            reason: "end event declares more than one \
                                         escalationEventDefinition; a multi-definition \
                                         escalation end event is not supported (declare \
                                         exactly one)"
                                .to_string(),
                        },
                        NodeKind::End => ParseError::InvalidEndEvent {
                            process_id: self.id.clone(),
                            element_id: node.id.clone(),
                            reason: "end event declares more than one event definition \
                                     (escalation/compensation/terminate/link/error); a \
                                     multi-definition end event is not supported (declare \
                                     exactly one)"
                                .to_string(),
                        },
                        _ => ParseError::UnsupportedElement {
                            tag: "intermediateThrowEvent".to_string(),
                            element_id: node.id.clone(),
                        },
                    });
                }
            }
            let node_id = node.id.clone();
            let io_id = node.id.clone();
            let node_io = node.io.clone();
            let timer_id = node.id.clone();
            let node_timer = node.timer_expr.clone();
            let retries_id = node.id.clone();
            let node_retries = node.retries.clone();
            let mi_id = node.id.clone();
            let listeners_id = node.id.clone();
            let task_listeners_id = node.id.clone();
            let name_id = node.id.clone();
            let node_name = node.name.clone();
            let node_start_listeners = node.start_listeners.clone();
            let node_end_listeners = node.end_listeners.clone();
            let node_task_listeners = node.task_listeners.clone();
            // Only treat as multi-instance when an input collection was actually
            // declared; a bare `multiInstanceLoopCharacteristics` with no
            // `zeebe:loopCharacteristics inputCollection` degenerates to an
            // ordinary single-instance activity.
            let node_mi = node
                .multi_instance
                .clone()
                .filter(|mi| !mi.input_collection.trim().is_empty());
            let parent = node.parent.clone();
            if matches!(node.kind, NodeKind::Start) && parent.is_none() {
                start_form_id = node.start_form_id.clone();
            }
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
                NodeKind::End => {
                    // An `<endEvent>` must not declare an outgoing sequence flow,
                    // whatever its event-definition flavour. The escalation and
                    // compensation flavours are remapped to *throw* elements just
                    // below, so the model-level `end_events_have_no_outgoing` rule
                    // (which only sees `EndEvent`/`TerminateEndEvent`) cannot catch
                    // them — guard the end-event flavour here, at the one site that
                    // still knows it came from an `<endEvent>` (#1173).
                    if flow_source_ids.contains(&node.id) {
                        return Err(ParseError::InvalidEndEvent {
                            process_id: self.id.clone(),
                            element_id: node.id.clone(),
                            reason: "an end event must have no outgoing sequence flow".to_string(),
                        });
                    }
                    if node.is_escalation_throw {
                        let code = node
                            .escalation_ref
                            .as_ref()
                            .and_then(|r| escalations.get(r).cloned())
                            .unwrap_or_default();
                        builder.escalation_throw_event(node.id, code)
                    } else if node.is_compensation_throw {
                        builder.compensation_throw_event(node.id)
                    } else if node.is_terminate {
                        builder.terminate_end_event(node.id)
                    } else {
                        builder.end_event(node.id)
                    }
                }
                NodeKind::IntermediateThrow => {
                    if node.is_escalation_throw {
                        let code = node
                            .escalation_ref
                            .as_ref()
                            .and_then(|r| escalations.get(r).cloned())
                            .unwrap_or_default();
                        builder.escalation_throw_event(node.id, code)
                    } else if node.is_compensation_throw {
                        builder.compensation_throw_event(node.id)
                    } else if let Some(link_name) = node.link_name.clone() {
                        builder.link_intermediate_throw_event(node.id, link_name)
                    } else {
                        builder.intermediate_throw_event(node.id)
                    }
                }
                NodeKind::Task => builder.task(node.id),
                NodeKind::Exclusive => builder.exclusive_gateway(node.id),
                NodeKind::Parallel => builder.parallel_gateway(node.id),
                NodeKind::Inclusive => builder.inclusive_gateway(node.id),
                NodeKind::EventBased => builder.event_based_gateway(node.id),
                NodeKind::Service => {
                    // Agent classification is metadata on the job worker. Placement rules
                    // (from AgentDefinitionValidator): aiAgentTask only on a
                    // serviceTask, aiAgentSubProcess only on an adHocSubProcess.
                    // Reject the wrong placement here. An `external` agent is
                    // accepted on either. When present it wins over the
                    // script/decision/job-based interpretations below.
                    if let Some(agent_type) = node.agent_type {
                        use crate::agent::AgentType;
                        match agent_type {
                            AgentType::AiAgentTask if node.is_adhoc => {
                                return Err(ParseError::InvalidAgentDefinition {
                                    process_id: self.id.clone(),
                                    element_id: node.id.clone(),
                                    reason: "agentType 'aiAgentTask' is only valid on a serviceTask, not an adHocSubProcess".to_string(),
                                });
                            }
                            AgentType::AiAgentSubProcess if !node.is_adhoc => {
                                return Err(ParseError::InvalidAgentDefinition {
                                    process_id: self.id.clone(),
                                    element_id: node.id.clone(),
                                    reason: "agentType 'aiAgentSubProcess' is only valid on an adHocSubProcess, not a serviceTask".to_string(),
                                });
                            }
                            _ => {}
                        }
                        let job_type = node.job_type.unwrap_or_else(|| node.id.clone());
                        builder.service_task_with_links_and_agent(
                            node.id,
                            job_type,
                            node.job_priority,
                            node.task_headers,
                            node.linked_resources,
                            Some(agent_type),
                        )
                    } else if let (Some(expr), Some(rv)) = (
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
                        builder.service_task_with_links(
                            node.id,
                            job_type,
                            node.job_priority,
                            node.task_headers,
                            node.linked_resources,
                        )
                    }
                }
                NodeKind::User => builder.user_task_with(node.id, node.user_task),
                NodeKind::IntermediateCatch => {
                    // Ordering: a link catch (linkEventDefinition) is a
                    // pass-through activated by its throw and carries no
                    // message/signal/timer/condition ref; a conditional catch
                    // (event_condition) has no message/signal/timer ref; a
                    // messageRef makes it a message catch; a signalRef a signal
                    // catch; otherwise a timer catch carrying a (possibly zero)
                    // duration.
                    if let Some(link_name) = node.link_name.clone() {
                        builder.link_intermediate_catch_event(node.id, link_name)
                    } else if let Some(condition) = node.event_condition {
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
                    builder.call_activity_with_propagation(
                        node.id,
                        called,
                        node.propagate_all_parent_variables.unwrap_or(true),
                        node.propagate_all_child_variables.unwrap_or(true),
                    )
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
            if !node_start_listeners.is_empty() || !node_end_listeners.is_empty() {
                builder =
                    builder.with_listeners(listeners_id, node_start_listeners, node_end_listeners);
            }
            if !node_task_listeners.is_empty() {
                builder = builder.with_task_listeners(task_listeners_id, node_task_listeners);
            }
            if let Some(name) = node_name {
                builder = builder.with_name(name_id, name);
            }
        }
        let associations = self.associations;
        // Ids of the compensation boundary events. A compensation boundary is a
        // structural marker armed implicitly when its activity completes; it is
        // never reached by ordinary token flow (see the flow validation below).
        let compensation_boundary_ids: std::collections::HashSet<String> = self
            .boundaries
            .iter()
            .filter(|b| b.compensation)
            .map(|b| b.id.clone())
            .collect();
        // Ids of every REACTIVE boundary event (error/timer/message/signal/
        // conditional/escalation — i.e. every boundary except the compensation
        // marker handled separately above). A reactive boundary is armed by its
        // own trigger and reached ONLY when that trigger fires; it legitimately
        // has an OUTGOING flow to its handler but must NEVER be the TARGET of a
        // sequenceFlow. If a model wires a token into one, `run_activation_body`
        // would treat it as an ordinary pass-through and complete it (routing its
        // handler) without any event ever being raised or matched — most acutely
        // the new escalation boundary (#1173), whose handler would fire with no
        // escalation. Reject such models at deploy (see the flow validation below)
        // rather than mis-executing them.
        let reactive_boundary_ids: std::collections::HashSet<String> = self
            .boundaries
            .iter()
            .filter(|b| !b.compensation)
            .map(|b| b.id.clone())
            .collect();
        // Execution listeners declared on boundary events (#1197). Collected
        // before the consuming loop below (which moves each `PendingBoundary`)
        // and re-attached by element id after the boundaries are built, so a
        // boundary event's `start`/`end` listeners fire on its own lifecycle
        // rather than being dropped or mis-attached during parsing.
        let boundary_listeners: Vec<(
            String,
            Vec<crate::model::ExecutionListener>,
            Vec<crate::model::ExecutionListener>,
        )> = self
            .boundaries
            .iter()
            .filter(|b| !b.start_listeners.is_empty() || !b.end_listeners.is_empty())
            .map(|b| {
                (
                    b.id.clone(),
                    b.start_listeners.clone(),
                    b.end_listeners.clone(),
                )
            })
            .collect();
        for boundary in self.boundaries {
            let attached_to =
                boundary
                    .attached_to
                    .ok_or_else(|| ParseError::InvalidBoundaryEvent {
                        process_id: self.id.clone(),
                        reason: format!("boundary event {} has no attachedToRef", boundary.id),
                    })?;
            // An escalation boundary (#1173): resolve its caught code from the
            // `escalationRef` (empty / missing = catch-all) and build it,
            // interrupting per `cancelActivity`. Built before the flavour cascade
            // below since it carries no error/timer/message/signal ref.
            if boundary.escalation {
                // Reject an AMBIGUOUS multi-definition boundary (#1173): a
                // `boundaryEvent` carrying an `escalationEventDefinition` *plus*
                // another trigger (error/timer/message/signal/conditional/
                // compensation) would be silently modelled as escalation here (the
                // escalation branch wins and `continue`s), discarding the declared
                // second trigger and changing the model's behaviour. There is no
                // OR-trigger boundary in the supported subset, so refuse the model
                // at deploy rather than drop a declared event.
                let other_trigger = boundary.error_ref.is_some()
                    || boundary.timer_duration_millis.is_some()
                    || boundary.timer_expr.is_some()
                    || boundary.message_ref.is_some()
                    || boundary.signal_ref.is_some()
                    || boundary.condition.is_some()
                    || boundary.compensation;
                if other_trigger || boundary.escalation_dup {
                    return Err(ParseError::InvalidBoundaryEvent {
                        process_id: self.id.clone(),
                        reason: format!(
                            "escalation boundary event {} declares more than one event \
                             definition; a multi-trigger boundary is not supported (declare \
                             exactly one of escalation/error/timer/message/signal/conditional/\
                             compensation)",
                            boundary.id
                        ),
                    });
                }
                // An escalation boundary can only catch an escalation raised in
                // the inner scope of the container it is attached to. Reject an
                // attachment to anything that opens no such scope (plain task,
                // call activity, …) so a dead boundary can never deploy (#1173).
                if !escalation_container_ids.contains(&attached_to) {
                    return Err(ParseError::InvalidBoundaryEvent {
                        process_id: self.id.clone(),
                        reason: format!(
                            "escalation boundary event {} must be attached to an embedded \
                             sub-process or ad-hoc sub-process (attachedToRef '{attached_to}' \
                             is not a container that can raise an escalation)",
                            boundary.id
                        ),
                    });
                }
                // Reject an escalation boundary attached to a *tool* of an ad-hoc
                // sub-process (its parent is an ad-hoc container). The runtime arms
                // boundary events on an embedded-subProcess ad-hoc tool, but an
                // interrupting boundary firing on such a tool has no wired path to
                // release the tool back to its parent container's active set, so
                // the container would remain `Active` forever after the handler
                // completes. Refuse it at deploy rather than mis-execute (#1173).
                if node_parent
                    .get(&attached_to)
                    .is_some_and(|p| adhoc_container_ids.contains(p))
                {
                    return Err(ParseError::InvalidBoundaryEvent {
                        process_id: self.id.clone(),
                        reason: format!(
                            "escalation boundary event {} is attached to '{attached_to}', a tool \
                             of an ad-hoc sub-process; an escalation boundary on an ad-hoc tool \
                             is not supported (attach it to a top-level embedded or ad-hoc \
                             sub-process instead)",
                            boundary.id
                        ),
                    });
                }
                let escalation_code = boundary
                    .escalation_ref
                    .as_ref()
                    .and_then(|r| escalations.get(r).cloned())
                    .unwrap_or_default();
                builder = if boundary.interrupting {
                    builder.escalation_boundary_event(boundary.id, attached_to, escalation_code)
                } else {
                    builder.non_interrupting_escalation_boundary_event(
                        boundary.id,
                        attached_to,
                        escalation_code,
                    )
                };
                continue;
            }
            // A compensation boundary marks its activity compensable; its
            // handler activity is the other end of the `<association>` wiring it.
            // Resolve it by scanning every association touching the boundary and
            // keeping only endpoints that are real `isForCompensation` handler
            // activities — this rejects a stray association to a `textAnnotation`
            // and rejects (rather than silently picking one of) an ambiguous set
            // of multiple candidate handlers.
            if boundary.compensation {
                let mut candidates: Vec<String> = associations
                    .iter()
                    .filter_map(|(source, target)| {
                        let other = if source == &boundary.id {
                            target
                        } else if target == &boundary.id {
                            source
                        } else {
                            return None;
                        };
                        compensation_handler_ids
                            .contains(other)
                            .then(|| other.clone())
                    })
                    .collect();
                candidates.sort();
                candidates.dedup();
                let handler = match candidates.as_slice() {
                    [single] => single.clone(),
                    [] => {
                        return Err(ParseError::InvalidBoundaryEvent {
                            process_id: self.id.clone(),
                            reason: format!(
                                "compensation boundary event {} has no associated `isForCompensation` handler activity (missing or non-handler <association>)",
                                boundary.id
                            ),
                        })
                    }
                    _ => {
                        return Err(ParseError::InvalidBoundaryEvent {
                            process_id: self.id.clone(),
                            reason: format!(
                                "compensation boundary event {} is associated with multiple handler activities ({}); exactly one is required",
                                boundary.id,
                                candidates.join(", ")
                            ),
                        })
                    }
                };
                builder = builder.compensation_boundary_event(boundary.id, attached_to, handler);
                continue;
            }
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
        // Re-attach the boundary events' execution listeners now that every
        // boundary element exists (#1197). `with_listeners` resolves by id, so a
        // boundary listener lands on its own element rather than the enclosing
        // activity it parsed adjacent to.
        for (id, start_listeners, end_listeners) in boundary_listeners {
            builder = builder.with_listeners(id, start_listeners, end_listeners);
        }
        // Compensation boundary events and their `isForCompensation` handlers are
        // structural markers, NOT part of ordinary token flow: a compensation
        // boundary is armed implicitly when its activity completes (never reached
        // by a sequenceFlow), and a handler runs only when a compensation throw
        // triggers it (never entered by an incoming token, and its completion is
        // routed back to the waiting throw rather than onward). If a model wires a
        // sequenceFlow to or from either, `run_activation_body` would treat the
        // element as an ordinary pass-through and route tokens through something
        // the engine never arms — corrupting execution. Reject such models at
        // deploy with a clear error rather than mis-executing them. (Handlers are
        // checked against every `isForCompensation` activity, not just the
        // resolved one, so an orphaned handler dragged into normal flow is caught
        // too.)
        for flow in self.flows.iter() {
            for endpoint in [flow.source.as_deref(), flow.target.as_deref()]
                .into_iter()
                .flatten()
            {
                if compensation_boundary_ids.contains(endpoint) {
                    return Err(ParseError::InvalidProcess {
                        process_id: self.id.clone(),
                        reason: format!(
                            "compensation boundary event {endpoint} must not be the source or target of a sequenceFlow"
                        ),
                    });
                }
                if compensation_handler_ids.contains(endpoint) {
                    return Err(ParseError::InvalidProcess {
                        process_id: self.id.clone(),
                        reason: format!(
                            "compensation handler activity {endpoint} (isForCompensation) must not be the source or target of a sequenceFlow"
                        ),
                    });
                }
            }
            // A reactive boundary event has an outgoing flow to its handler but is
            // itself only reached when its trigger fires — so it may be a flow
            // SOURCE but never a flow TARGET. Reject an incoming sequenceFlow so a
            // token can never drive the handler without the event being raised.
            if let Some(target) = flow.target.as_deref() {
                if reactive_boundary_ids.contains(target) {
                    return Err(ParseError::InvalidProcess {
                        process_id: self.id.clone(),
                        reason: format!(
                            "boundary event {target} must not be the target of a sequenceFlow (a boundary event is reached only when its trigger fires)"
                        ),
                    });
                }
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
        def.start_form_id = start_form_id;
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
    use crate::model::ElementId;
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
    fn should_parse_the_process_name_attribute() {
        // The `<bpmn:process>` `name` attribute (the modeller label) is captured
        // as `ProcessDefinition::name`, distinct from the executable `id`. This
        // is what the process-definition search `name` filter matches against and
        // what read models surface as the definition `name`.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="main-process" name="Main Process" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(def.id, "main-process");
        assert_eq!(def.name.as_deref(), Some("Main Process"));
    }

    #[test]
    fn should_leave_process_name_none_when_absent() {
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="simple-process" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(def.id, "simple-process");
        assert_eq!(def.name, None);
    }

    #[test]
    fn should_reject_a_dangling_outgoing_flow_reference() {
        // A flow node declaring an `<outgoing>` reference to a sequenceFlow that
        // is not declared anywhere is an unresolved QName reference. Zeebe
        // rejects such a model at deploy with `INVALID_ARGUMENT`; Nano must too,
        // rather than silently accepting it because it builds the graph only
        // from `<sequenceFlow>` elements. Regression guard for issue #849.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="invalid-process" isExecutable="true">
              <bpmn:startEvent id="Start">
                <bpmn:outgoing>Flow_missing</bpmn:outgoing>
              </bpmn:startEvent>
            </bpmn:process>
          </bpmn:definitions>"#;

        let err = parse_bpmn(xml).unwrap_err();
        assert_eq!(
            err,
            ParseError::UnresolvedReference {
                kind: "outgoing".to_string(),
                id: "Flow_missing".to_string(),
                process_id: "invalid-process".to_string(),
                from_node: "Start".to_string(),
            }
        );
    }

    #[test]
    fn should_reject_a_dangling_incoming_flow_reference() {
        // The `<incoming>` direction is validated the same way as `<outgoing>`.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="invalid-in" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:endEvent id="e">
                <bpmn:incoming>Flow_missing</bpmn:incoming>
              </bpmn:endEvent>
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let err = parse_bpmn(xml).unwrap_err();
        assert_eq!(
            err,
            ParseError::UnresolvedReference {
                kind: "incoming".to_string(),
                id: "Flow_missing".to_string(),
                process_id: "invalid-in".to_string(),
                from_node: "e".to_string(),
            }
        );
    }

    #[test]
    fn should_accept_resolved_incoming_outgoing_flow_references() {
        // A well-formed model — the modeller-exported `<incoming>`/`<outgoing>`
        // references all resolve to declared `<sequenceFlow>` ids — is
        // unaffected: it parses cleanly and the graph is built as before.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="valid-process" isExecutable="true">
              <bpmn:startEvent id="s">
                <bpmn:outgoing>a</bpmn:outgoing>
              </bpmn:startEvent>
              <bpmn:endEvent id="e">
                <bpmn:incoming>a</bpmn:incoming>
              </bpmn:endEvent>
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(def.id, "valid-process");
        assert_eq!(def.element("s").unwrap().outgoing[0].to, "e");
    }

    #[test]
    fn should_attribute_container_flow_refs_to_the_container_not_a_nested_child() {
        // A container flow node (`subProcess`/`adHocSubProcess`) whose
        // `<incoming>`/`<outgoing>` follow its nested elements must still have
        // its reference captured and validated against the container, not the
        // most recently *added* (now closed) child. Regression guard for the
        // review finding on issue #849: a single "last added node" pointer would
        // misattribute the reference to `InnerStart` and could mask it. The
        // `<outgoing>` is placed after the nested (non-self-closing) child, and
        // the dangling reference must still be rejected **and attributed to the
        // container `Sub`** (not the nested `InnerStart`) — the `from_node`
        // assertion below enforces that attribution claim directly.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="container-attr" isExecutable="true">
              <bpmn:startEvent id="Start"><bpmn:outgoing>toSub</bpmn:outgoing></bpmn:startEvent>
              <bpmn:subProcess id="Sub">
                <bpmn:startEvent id="InnerStart"><bpmn:outgoing>i1</bpmn:outgoing></bpmn:startEvent>
                <bpmn:endEvent id="InnerEnd"><bpmn:incoming>i1</bpmn:incoming></bpmn:endEvent>
                <bpmn:sequenceFlow id="i1" sourceRef="InnerStart" targetRef="InnerEnd" />
                <bpmn:outgoing>Flow_missing</bpmn:outgoing>
              </bpmn:subProcess>
              <bpmn:endEvent id="End"><bpmn:incoming>fromSub</bpmn:incoming></bpmn:endEvent>
              <bpmn:sequenceFlow id="toSub" sourceRef="Start" targetRef="Sub" />
              <bpmn:sequenceFlow id="fromSub" sourceRef="Sub" targetRef="End" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let err = parse_bpmn(xml).unwrap_err();
        assert_eq!(
            err,
            ParseError::UnresolvedReference {
                kind: "outgoing".to_string(),
                id: "Flow_missing".to_string(),
                process_id: "container-attr".to_string(),
                from_node: "Sub".to_string(),
            }
        );
    }

    #[test]
    fn should_attribute_anonymous_unmodelled_event_def_under_a_boundary_to_the_boundary() {
        // Regression guard for the suppressed review finding on PR #860: an
        // unsupported event definition that carries no `id` and is nested under a
        // `<boundaryEvent>` must be attributed to the boundary event, not to a
        // containing flow node or an empty id. Boundary events are buffered in
        // `cur_boundary` and are *never* pushed onto `flow_node_stack`, so a
        // fallback that consulted only the stack would misattribute the element —
        // making the future `UnsupportedElement { element_id }` report point at
        // the wrong (or no) element. `<cancelEventDefinition>` is an
        // unmodelled event def (Nano models none/error/timer/message/signal/
        // compensation boundaries, not cancel), and it carries no `id`. We
        // inspect the raw capture directly (the consuming #853 validator is
        // still a stub), which is exactly the data that validator will see.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="boundary-attr" isExecutable="true">
              <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
              <bpmn:serviceTask id="Task"><bpmn:incoming>f1</bpmn:incoming></bpmn:serviceTask>
              <bpmn:boundaryEvent id="Boundary" attachedToRef="Task">
                <bpmn:cancelEventDefinition />
              </bpmn:boundaryEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Task" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let capture = &parse_with_captures(xml).unwrap()[0].0;
        let compensate = capture
            .unmodelled
            .iter()
            .find(|u| u.tag == "cancelEventDefinition")
            .expect("the anonymous cancelEventDefinition must be captured as unmodelled");
        assert_eq!(
            compensate.element_id, "Boundary",
            "an anonymous unmodelled event def under a boundary event must attribute \
             to the boundary event, not a containing flow node or an empty id"
        );
    }

    #[test]
    fn should_parse_compensation_throw_and_boundary_with_its_handler() {
        // A `compensateEventDefinition` on an intermediateThrowEvent becomes a
        // CompensationThrowEvent; on a boundary event it becomes a
        // CompensationBoundaryEvent whose handler is resolved from the
        // `<association>` wiring the boundary to the `isForCompensation` activity.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="comp" isExecutable="true">
              <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
              <bpmn:serviceTask id="Book"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:serviceTask>
              <bpmn:boundaryEvent id="BookComp" attachedToRef="Book">
                <bpmn:compensateEventDefinition />
              </bpmn:boundaryEvent>
              <bpmn:serviceTask id="CancelBook" isForCompensation="true" />
              <bpmn:association id="a1" sourceRef="BookComp" targetRef="CancelBook" />
              <bpmn:intermediateThrowEvent id="Throw"><bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
                <bpmn:compensateEventDefinition />
              </bpmn:intermediateThrowEvent>
              <bpmn:endEvent id="End"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Book" />
              <bpmn:sequenceFlow id="f2" sourceRef="Book" targetRef="Throw" />
              <bpmn:sequenceFlow id="f3" sourceRef="Throw" targetRef="End" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let defs = parse_bpmn(xml).unwrap();
        let def = &defs[0];
        let throw = def
            .elements
            .get(&ElementId::from("Throw"))
            .expect("throw element");
        assert!(
            matches!(throw.kind, ElementKind::CompensationThrowEvent),
            "compensateEventDefinition on a throw event must be a CompensationThrowEvent, got {:?}",
            throw.kind
        );
        let boundary = def
            .elements
            .get(&ElementId::from("BookComp"))
            .expect("boundary element");
        match &boundary.kind {
            ElementKind::CompensationBoundaryEvent {
                attached_to,
                handler,
            } => {
                assert_eq!(attached_to.as_str(), "Book");
                assert_eq!(handler.as_str(), "CancelBook");
            }
            other => panic!("expected CompensationBoundaryEvent, got {other:?}"),
        }
    }

    #[test]
    fn should_reject_compensation_boundary_associations_that_do_not_resolve_to_a_single_handler() {
        // Failure-mode guard: a compensation boundary's handler is resolved from
        // its `<association>` wiring, but only `isForCompensation` activities are
        // valid handlers. An association to a non-handler node (e.g. a
        // `textAnnotation`) must NOT mis-bind, and multiple candidate handlers
        // must be rejected rather than nondeterministically picking one.
        let boundary = r#"
              <bpmn:serviceTask id="Book"><bpmn:incoming>f1</bpmn:incoming></bpmn:serviceTask>
              <bpmn:boundaryEvent id="BookComp" attachedToRef="Book">
                <bpmn:compensateEventDefinition />
              </bpmn:boundaryEvent>
              <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Book" />"#;
        let wrap = |body: &str| {
            format!(
                r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
                    <bpmn:process id="comp" isExecutable="true">{boundary}{body}</bpmn:process>
                   </bpmn:definitions>"#
            )
        };

        // (a) The only association points at a `textAnnotation`, not a handler.
        let annotation = wrap(
            r#"<bpmn:textAnnotation id="Note"><bpmn:text>hi</bpmn:text></bpmn:textAnnotation>
               <bpmn:association id="a1" sourceRef="BookComp" targetRef="Note" />"#,
        );
        match parse_bpmn(&annotation) {
            Err(ParseError::InvalidBoundaryEvent { reason, .. }) => {
                assert!(
                    reason.contains("isForCompensation"),
                    "expected a missing-handler error, got: {reason}"
                );
            }
            other => {
                panic!("expected InvalidBoundaryEvent for a non-handler association, got {other:?}")
            }
        }

        // (b) Two distinct `isForCompensation` handlers are associated — ambiguous.
        let ambiguous = wrap(
            r#"<bpmn:serviceTask id="CancelA" isForCompensation="true" />
               <bpmn:serviceTask id="CancelB" isForCompensation="true" />
               <bpmn:association id="a1" sourceRef="BookComp" targetRef="CancelA" />
               <bpmn:association id="a2" sourceRef="BookComp" targetRef="CancelB" />"#,
        );
        match parse_bpmn(&ambiguous) {
            Err(ParseError::InvalidBoundaryEvent { reason, .. }) => {
                assert!(
                    reason.contains("multiple handler activities"),
                    "expected an ambiguous-handler error, got: {reason}"
                );
            }
            other => {
                panic!("expected InvalidBoundaryEvent for ambiguous associations, got {other:?}")
            }
        }

        // (c) A handler association plus a harmless annotation association still
        // resolves to the single real handler (the annotation is ignored).
        let mixed = wrap(
            r#"<bpmn:serviceTask id="CancelBook" isForCompensation="true" />
               <bpmn:textAnnotation id="Note"><bpmn:text>hi</bpmn:text></bpmn:textAnnotation>
               <bpmn:association id="a1" sourceRef="BookComp" targetRef="Note" />
               <bpmn:association id="a2" sourceRef="BookComp" targetRef="CancelBook" />"#,
        );
        let defs = parse_bpmn(&mixed).expect("a single real handler must resolve");
        match &defs[0]
            .elements
            .get(&ElementId::from("BookComp"))
            .expect("boundary element")
            .kind
        {
            ElementKind::CompensationBoundaryEvent { handler, .. } => {
                assert_eq!(handler.as_str(), "CancelBook");
            }
            other => panic!("expected CompensationBoundaryEvent, got {other:?}"),
        }
    }

    #[test]
    fn should_not_treat_a_non_activity_as_a_compensation_handler() {
        // Failure-mode guard (advisory): `isForCompensation="true"` is only
        // meaningful on an *activity*. A non-activity node (e.g. a gateway) that
        // stray-carries the attribute must NOT become an eligible handler, so an
        // association pointing at it fails to resolve exactly as if no handler
        // existed — rather than silently binding the boundary to a gateway.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="comp" isExecutable="true">
              <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
              <bpmn:serviceTask id="Book"><bpmn:incoming>f1</bpmn:incoming></bpmn:serviceTask>
              <bpmn:boundaryEvent id="BookComp" attachedToRef="Book">
                <bpmn:compensateEventDefinition />
              </bpmn:boundaryEvent>
              <bpmn:exclusiveGateway id="NotAHandler" isForCompensation="true" />
              <bpmn:association id="a1" sourceRef="BookComp" targetRef="NotAHandler" />
              <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Book" />
            </bpmn:process>
          </bpmn:definitions>"#;
        match parse_bpmn(xml) {
            Err(ParseError::InvalidBoundaryEvent { reason, .. }) => {
                assert!(
                    reason.contains("isForCompensation"),
                    "expected a missing-handler error, got: {reason}"
                );
            }
            other => panic!(
                "a gateway carrying isForCompensation must not resolve as a handler, got {other:?}"
            ),
        }
    }

    #[test]
    fn should_reject_sequence_flows_touching_a_compensation_boundary_or_handler() {
        // Failure-mode guard: compensation boundary events and their
        // `isForCompensation` handlers are structural markers outside ordinary
        // token flow. A model that wires a sequenceFlow to/from either would let
        // the engine route tokens through an element it never arms, so deploy
        // must reject it rather than mis-execute.
        let wrap = |body: &str| {
            format!(
                r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
                    <bpmn:process id="comp" isExecutable="true">
                      <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                      <bpmn:serviceTask id="Book"><bpmn:incoming>f1</bpmn:incoming></bpmn:serviceTask>
                      <bpmn:boundaryEvent id="BookComp" attachedToRef="Book">
                        <bpmn:compensateEventDefinition />
                      </bpmn:boundaryEvent>
                      <bpmn:serviceTask id="CancelBook" isForCompensation="true" />
                      <bpmn:association id="a1" sourceRef="BookComp" targetRef="CancelBook" />
                      <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Book" />
                      {body}
                    </bpmn:process>
                   </bpmn:definitions>"#
            )
        };

        // (a) A sequenceFlow whose target is the compensation boundary.
        let into_boundary = wrap(
            r#"<bpmn:endEvent id="E" /><bpmn:sequenceFlow id="bad" sourceRef="Book" targetRef="BookComp" />"#,
        );
        match parse_bpmn(&into_boundary) {
            Err(ParseError::InvalidProcess { reason, .. }) => assert!(
                reason.contains("compensation boundary event") && reason.contains("sequenceFlow"),
                "expected a boundary flow rejection, got: {reason}"
            ),
            other => panic!(
                "expected InvalidProcess for a flow into a compensation boundary, got {other:?}"
            ),
        }

        // (b) A sequenceFlow whose source is the compensation boundary.
        let out_of_boundary = wrap(
            r#"<bpmn:endEvent id="E"><bpmn:incoming>bad</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="bad" sourceRef="BookComp" targetRef="E" />"#,
        );
        match parse_bpmn(&out_of_boundary) {
            Err(ParseError::InvalidProcess { reason, .. }) => assert!(
                reason.contains("compensation boundary event"),
                "expected a boundary flow rejection, got: {reason}"
            ),
            other => panic!(
                "expected InvalidProcess for a flow out of a compensation boundary, got {other:?}"
            ),
        }

        // (c) A sequenceFlow into the `isForCompensation` handler.
        let into_handler =
            wrap(r#"<bpmn:sequenceFlow id="bad" sourceRef="Book" targetRef="CancelBook" />"#);
        match parse_bpmn(&into_handler) {
            Err(ParseError::InvalidProcess { reason, .. }) => assert!(
                reason.contains("compensation handler activity")
                    && reason.contains("isForCompensation"),
                "expected a handler flow rejection, got: {reason}"
            ),
            other => panic!(
                "expected InvalidProcess for a flow into a compensation handler, got {other:?}"
            ),
        }

        // (d) A sequenceFlow out of the handler.
        let out_of_handler = wrap(
            r#"<bpmn:endEvent id="E"><bpmn:incoming>bad</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="bad" sourceRef="CancelBook" targetRef="E" />"#,
        );
        match parse_bpmn(&out_of_handler) {
            Err(ParseError::InvalidProcess { reason, .. }) => assert!(
                reason.contains("compensation handler activity"),
                "expected a handler flow rejection, got: {reason}"
            ),
            other => panic!(
                "expected InvalidProcess for a flow out of a compensation handler, got {other:?}"
            ),
        }

        // (e) The well-formed model (no stray flows) still parses.
        let ok = wrap("");
        parse_bpmn(&ok).expect("a compensation model with no stray flows must parse");
    }

    #[test]
    fn should_record_compensate_event_definition_on_an_unsupported_node_as_unmodelled() {
        // Failure-mode guard (advisory): a `compensateEventDefinition` is only
        // modelled as a compensation throw on an intermediateThrowEvent or an
        // endEvent (the two kinds the build step interprets). On any other node
        // kind the flag would be silently dropped at build, so instead the
        // placement is recorded as an unmodelled element attributed to that node
        // — exactly the data the #853 unsupported-elements validator (still a
        // stub) will reject at deploy. We inspect the raw capture directly.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="comp" isExecutable="true">
              <bpmn:startEvent id="Start">
                <bpmn:outgoing>f1</bpmn:outgoing>
                <bpmn:compensateEventDefinition />
              </bpmn:startEvent>
              <bpmn:endEvent id="End"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End" />
            </bpmn:process>
          </bpmn:definitions>"#;
        let capture = &parse_with_captures(xml).unwrap()[0].0;
        let unmodelled = capture
            .unmodelled
            .iter()
            .find(|u| u.tag == "compensateEventDefinition")
            .expect("a compensateEventDefinition on a startEvent must be captured as unmodelled");
        assert_eq!(
            unmodelled.element_id, "Start",
            "the invalid compensateEventDefinition placement must attribute to the startEvent"
        );
    }

    #[test]
    fn should_reject_the_whole_dangling_incoming_outgoing_reference_class() {
        // Class-scoped guard (red on `main`, green here): assert the whole
        // defect class — a dangling `<incoming>` or `<outgoing>` reference on any
        // flow node is rejected as an `UnresolvedReference`, while a model whose
        // references all resolve is accepted. Parametrised over direction and
        // owning-node kind so a future regression on any single site fails here.
        struct Case {
            name: &'static str,
            body: &'static str,
            expect: Option<(&'static str, &'static str, &'static str)>, // (kind, id, from_node) on reject
        }
        let cases = [
            Case {
                name: "dangling outgoing on a start event",
                body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>missing</bpmn:outgoing></bpmn:startEvent>"#,
                expect: Some(("outgoing", "missing", "s")),
            },
            Case {
                name: "dangling incoming on an end event",
                body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:endEvent id="e"><bpmn:incoming>missing</bpmn:incoming></bpmn:endEvent>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e" />"#,
                expect: Some(("incoming", "missing", "e")),
            },
            Case {
                name: "dangling outgoing on a task",
                body: r#"<bpmn:startEvent id="s" />
                         <bpmn:task id="t"><bpmn:outgoing>missing</bpmn:outgoing></bpmn:task>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="t" />"#,
                expect: Some(("outgoing", "missing", "t")),
            },
            Case {
                name: "all references resolve",
                body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:endEvent id="e"><bpmn:incoming>a</bpmn:incoming></bpmn:endEvent>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e" />"#,
                expect: None,
            },
        ];
        for case in cases {
            let xml = format!(
                r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
                     <bpmn:process id="p" isExecutable="true">{}</bpmn:process>
                   </bpmn:definitions>"#,
                case.body
            );
            match (parse_bpmn(&xml), case.expect) {
                (
                    Err(ParseError::UnresolvedReference {
                        kind,
                        id,
                        process_id,
                        from_node,
                    }),
                    Some((ek, eid, efrom)),
                ) => {
                    assert_eq!(
                        (
                            kind.as_str(),
                            id.as_str(),
                            process_id.as_str(),
                            from_node.as_str()
                        ),
                        (ek, eid, "p", efrom),
                        "case: {}",
                        case.name
                    );
                }
                (Ok(_), None) => {}
                (other, _) => panic!("case {}: unexpected result {other:?}", case.name),
            }
        }
    }

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
    fn should_parse_abstract_task_and_manual_task_as_pass_through() {
        // An abstract `bpmn:task` (and `manualTask`) has no execution semantics;
        // Zeebe/C8 accept it as a pass-through. Nano must parse it (not drop it,
        // which would dangle the inbound sequence flow with an
        // "unknown target element" deploy error).
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="abstract" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:task id="do-something" name="Do Something" />
    <bpmn:manualTask id="do-manual" />
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="do-something" />
    <bpmn:sequenceFlow id="f2" sourceRef="do-something" targetRef="do-manual" />
    <bpmn:sequenceFlow id="f3" sourceRef="do-manual" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>"#;

        let defs = parse_bpmn(xml).expect("an abstract task must parse (Zeebe parity)");
        let def = &defs[0];
        assert_eq!(def.element("do-something").unwrap().kind, ElementKind::Task);
        assert_eq!(def.element("do-manual").unwrap().kind, ElementKind::Task);
        // The `name` attribute is captured like any other element.
        assert_eq!(
            def.element("do-something").unwrap().name.as_deref(),
            Some("Do Something")
        );
        // The inbound flow resolves to the task (no dangling target).
        assert_eq!(def.element("start").unwrap().outgoing[0].to, "do-something");
        assert_eq!(
            def.element("do-something").unwrap().outgoing[0].to,
            "do-manual"
        );
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
                agent_type: None,
                custom_headers: std::collections::BTreeMap::new(),
                linked_resources: Vec::new(),
            }
        );
        assert_eq!(def.element("start").unwrap().outgoing[0].to, "charge");
    }

    #[test]
    fn should_parse_a_send_task_as_a_job_based_service_task() {
        // A `sendTask` with a `zeebe:taskDefinition` is executed by a job worker
        // exactly like a service task (its throwing cousin of `receiveTask`,
        // #1168). A flow into it must resolve to a modelled element rather than
        // failing deploy with the misleading "unknown target element" error.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="notify" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:sendTask id="send" name="Send Notification">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="notifier" retries="4" />
      </bpmn:extensionElements>
    </bpmn:sendTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="send" />
    <bpmn:sequenceFlow id="f2" sourceRef="send" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(
            def.element("send").unwrap().kind,
            ElementKind::ServiceTask {
                job_type: "notifier".to_string(),
                priority: None,
                agent_type: None,
                custom_headers: std::collections::BTreeMap::new(),
                linked_resources: Vec::new(),
            }
        );
        // The job type falls back to the element id when no taskDefinition type
        // is given, mirroring a serviceTask.
        assert_eq!(def.element("s").unwrap().outgoing[0].to, "send");
        assert_eq!(def.element("send").unwrap().outgoing[0].to, "e");
    }

    #[test]
    fn should_parse_a_bare_send_task_defaulting_job_type_to_its_id() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="notify" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:sendTask id="send" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="send" />
    <bpmn:sequenceFlow id="f2" sourceRef="send" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(
            def.element("send").unwrap().kind,
            ElementKind::ServiceTask {
                job_type: "send".to_string(),
                priority: None,
                agent_type: None,
                custom_headers: std::collections::BTreeMap::new(),
                linked_resources: Vec::new(),
            }
        );
    }

    #[test]
    fn should_parse_a_non_interrupting_escalation_boundary_and_its_throw() {
        // The #1168 probe, now modelled (#1173): a non-interrupting escalation
        // boundary on a sub-process catching an escalation thrown from inside it.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="Sub">
      <bpmn:startEvent id="ss" />
      <bpmn:intermediateThrowEvent id="Thr">
        <bpmn:escalationEventDefinition escalationRef="Esc" />
      </bpmn:intermediateThrowEvent>
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="if1" sourceRef="ss" targetRef="Thr" />
      <bpmn:sequenceFlow id="if2" sourceRef="Thr" targetRef="se" />
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="Sub" cancelActivity="false">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="Sub" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(
            def.element("Bnd").unwrap().kind,
            ElementKind::EscalationBoundaryEvent {
                attached_to: "Sub".to_string(),
                escalation_code: "OVERLOAD".to_string(),
                interrupting: false,
            }
        );
        assert_eq!(
            def.element("Thr").unwrap().kind,
            ElementKind::EscalationThrowEvent {
                escalation_code: "OVERLOAD".to_string(),
            }
        );
    }

    #[test]
    fn should_parse_an_interrupting_escalation_boundary_by_default() {
        // Absent `cancelActivity` (or `="true"`) is interrupting.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="Sub">
      <bpmn:startEvent id="ss" />
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="if1" sourceRef="ss" targetRef="se" />
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="Sub">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="Sub" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(
            def.element("Bnd").unwrap().kind,
            ElementKind::EscalationBoundaryEvent {
                attached_to: "Sub".to_string(),
                escalation_code: "OVERLOAD".to_string(),
                interrupting: true,
            }
        );
    }

    #[test]
    fn should_parse_a_catch_all_escalation_boundary_without_a_ref() {
        // A boundary escalation carrier with no `escalationRef` is a catch-all
        // (empty escalation code), catching any escalation.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="Sub">
      <bpmn:startEvent id="ss" />
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="if1" sourceRef="ss" targetRef="se" />
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="Sub" cancelActivity="false">
      <bpmn:escalationEventDefinition />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="Sub" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(
            def.element("Bnd").unwrap().kind,
            ElementKind::EscalationBoundaryEvent {
                attached_to: "Sub".to_string(),
                escalation_code: String::new(),
                interrupting: false,
            }
        );
    }

    #[test]
    fn should_parse_an_escalation_throw_event() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:intermediateThrowEvent id="Thr">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:intermediateThrowEvent>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Thr" />
    <bpmn:sequenceFlow id="f2" sourceRef="Thr" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(
            def.element("Thr").unwrap().kind,
            ElementKind::EscalationThrowEvent {
                escalation_code: "OVERLOAD".to_string(),
            }
        );
    }

    #[test]
    fn should_parse_a_ref_less_escalation_throw_as_a_codeless_escalation() {
        // A ref-less escalation carrier (no `escalationRef`) parses to an
        // escalation throw with an empty code (no dangling reference to reject).
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:intermediateThrowEvent id="Thr">
      <bpmn:escalationEventDefinition />
    </bpmn:intermediateThrowEvent>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Thr" />
    <bpmn:sequenceFlow id="f2" sourceRef="Thr" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(
            def.element("Thr").unwrap().kind,
            ElementKind::EscalationThrowEvent {
                escalation_code: String::new(),
            }
        );
    }

    #[test]
    fn should_parse_an_escalation_end_event() {
        // An escalation end event is an escalation throw carrier on an end event:
        // it raises the escalation and drains (no outgoing flow).
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:endEvent id="EscEnd">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="EscEnd" />
  </bpmn:process>
</bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(
            def.element("EscEnd").unwrap().kind,
            ElementKind::EscalationThrowEvent {
                escalation_code: "OVERLOAD".to_string(),
            }
        );
    }

    #[test]
    fn should_reject_an_escalation_end_event_with_an_outgoing_flow() {
        // Regression (#1173): an escalation `<endEvent>` is remapped to an
        // `EscalationThrowEvent`, so the model-level `end_events_have_no_outgoing`
        // rule (which only matches `EndEvent`/`TerminateEndEvent`) cannot see it.
        // A malformed escalation end event that declares an outgoing flow would
        // otherwise deploy as a *routing* intermediate throw and continue past the
        // end event. Reject it at parse time like any other end-event flavour.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:endEvent id="EscEnd">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:endEvent>
    <bpmn:endEvent id="after" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="EscEnd" />
    <bpmn:sequenceFlow id="f2" sourceRef="EscEnd" targetRef="after" />
  </bpmn:process>
</bpmn:definitions>"#;

        let err = parse_bpmn(xml).expect_err("escalation end event with outgoing flow is rejected");
        assert!(
            matches!(
                &err,
                ParseError::InvalidEndEvent { element_id, .. } if element_id == "EscEnd"
            ),
            "expected InvalidEndEvent for EscEnd, got {err:?}"
        );
    }

    #[test]
    fn should_reject_a_compensation_end_event_with_an_outgoing_flow() {
        // Same failure class as the escalation end event: a compensation
        // `<endEvent>` is remapped to a `CompensationThrowEvent`, bypassing the
        // end-event rule. Guard every end-event flavour (#1173).
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:endEvent id="CompEnd">
      <bpmn:compensateEventDefinition />
    </bpmn:endEvent>
    <bpmn:endEvent id="after" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="CompEnd" />
    <bpmn:sequenceFlow id="f2" sourceRef="CompEnd" targetRef="after" />
  </bpmn:process>
</bpmn:definitions>"#;

        let err =
            parse_bpmn(xml).expect_err("compensation end event with outgoing flow is rejected");
        assert!(
            matches!(
                &err,
                ParseError::InvalidEndEvent { element_id, .. } if element_id == "CompEnd"
            ),
            "expected InvalidEndEvent for CompEnd, got {err:?}"
        );
    }

    #[test]
    fn should_reject_an_escalation_boundary_on_a_non_container_activity() {
        // Regression (#1173): an escalation propagates out of the inner scope it
        // is raised in, so an escalation boundary attached to a plain activity
        // (here a service task) — which opens no such scope — is a dead boundary
        // `find_catching_escalation_boundary` can never reach. Reject it at deploy
        // rather than silently accepting an inert boundary.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="task">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="work" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="task">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="task" />
    <bpmn:sequenceFlow id="f2" sourceRef="task" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

        let err = parse_bpmn(xml).expect_err("escalation boundary on a service task is rejected");
        assert!(
            matches!(&err, ParseError::InvalidBoundaryEvent { reason, .. } if reason.contains("Bnd")),
            "expected InvalidBoundaryEvent for Bnd, got {err:?}"
        );
    }

    #[test]
    fn should_accept_an_escalation_boundary_on_an_adhoc_sub_process() {
        // The ad-hoc sub-process is a supported escalation container (#1173): it
        // opens an inner scope, so a boundary attached to it is reachable. It
        // flattens to a job-backed service element, so this guards that the
        // container check keys off `is_adhoc`, not only `NodeKind::SubProcess`.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:adHocSubProcess id="Agent">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="agent" />
      </bpmn:extensionElements>
      <bpmn:serviceTask id="tool">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="tool" />
        </bpmn:extensionElements>
      </bpmn:serviceTask>
    </bpmn:adHocSubProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="Agent">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Agent" />
    <bpmn:sequenceFlow id="f2" sourceRef="Agent" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

        let def =
            &parse_bpmn(xml).expect("escalation boundary on an ad-hoc sub-process is valid")[0];
        assert_eq!(
            def.element("Bnd").unwrap().kind,
            ElementKind::EscalationBoundaryEvent {
                attached_to: "Agent".to_string(),
                escalation_code: "OVERLOAD".to_string(),
                interrupting: true,
            }
        );
    }

    #[test]
    fn should_reject_a_sequence_flow_targeting_an_escalation_boundary() {
        // Regression (#1173): a reactive boundary event has an outgoing flow to
        // its handler but is itself reached ONLY when its trigger fires — it must
        // never be the TARGET of a sequenceFlow. Otherwise the generic
        // pass-through would complete the boundary and route its handler with no
        // escalation ever raised or matched. Reject an incoming flow at deploy.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="Sub">
      <bpmn:startEvent id="ss" />
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="if" sourceRef="ss" targetRef="se" />
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="Sub">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="Sub" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
    <bpmn:sequenceFlow id="f4" sourceRef="s" targetRef="Bnd" />
  </bpmn:process>
</bpmn:definitions>"#;

        let err = parse_bpmn(xml)
            .expect_err("a sequenceFlow targeting an escalation boundary is rejected");
        assert!(
            matches!(&err, ParseError::InvalidProcess { reason, .. }
                if reason.contains("Bnd") && reason.contains("must not be the target")),
            "expected InvalidProcess for the incoming flow to Bnd, got {err:?}"
        );
    }

    #[test]
    fn should_reject_a_multi_definition_escalation_boundary() {
        // Regression (#1173): a `boundaryEvent` carrying an
        // `escalationEventDefinition` PLUS another trigger (here a timer) would be
        // silently modelled as escalation, discarding the declared timer. There is
        // no OR-trigger boundary in the supported subset, so reject the ambiguous
        // multi-definition boundary at deploy rather than drop a declared event.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="Sub">
      <bpmn:startEvent id="ss" />
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="if" sourceRef="ss" targetRef="se" />
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="Sub">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
      <bpmn:timerEventDefinition>
        <bpmn:timeDuration>PT5M</bpmn:timeDuration>
      </bpmn:timerEventDefinition>
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="Sub" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

        let err = parse_bpmn(xml).expect_err("a multi-definition escalation boundary is rejected");
        assert!(
            matches!(&err, ParseError::InvalidBoundaryEvent { reason, .. }
                if reason.contains("Bnd") && reason.contains("more than one event")),
            "expected InvalidBoundaryEvent for the multi-definition Bnd, got {err:?}"
        );
    }

    #[test]
    fn should_reject_an_escalation_boundary_on_an_adhoc_tool() {
        // Regression (#1173): an embedded-subProcess ad-hoc TOOL opens an inner
        // scope (so the container check passes), and the runtime arms boundary
        // events on it — but an interrupting boundary firing on such a tool cannot
        // release it back to its parent container's active set, hanging the
        // instance. Reject the attachment at deploy rather than mis-execute.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:adHocSubProcess id="Agent">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="agent" />
      </bpmn:extensionElements>
      <bpmn:subProcess id="tool">
        <bpmn:startEvent id="ts" />
        <bpmn:endEvent id="te" />
        <bpmn:sequenceFlow id="tf" sourceRef="ts" targetRef="te" />
      </bpmn:subProcess>
    </bpmn:adHocSubProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="tool">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Agent" />
    <bpmn:sequenceFlow id="f2" sourceRef="Agent" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

        let err =
            parse_bpmn(xml).expect_err("an escalation boundary on an ad-hoc tool is rejected");
        assert!(
            matches!(&err, ParseError::InvalidBoundaryEvent { reason, .. }
                if reason.contains("Bnd") && reason.contains("ad-hoc tool")),
            "expected InvalidBoundaryEvent for the ad-hoc-tool Bnd, got {err:?}"
        );
    }

    #[test]
    fn should_reject_a_duplicate_escalation_definition_on_a_boundary() {
        // Regression (#1173, suppressed advisory): a SECOND
        // `escalationEventDefinition` on one boundary silently overwrites
        // `escalation_ref` (last-wins), so an exact code could be swapped for a
        // catch-all with no diagnostic. Reject the ambiguous multi-definition
        // boundary at deploy.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:escalation id="EscA" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:escalation id="EscB" name="Any" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="Sub">
      <bpmn:startEvent id="ss" />
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="if" sourceRef="ss" targetRef="se" />
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="Sub">
      <bpmn:escalationEventDefinition escalationRef="EscA" />
      <bpmn:escalationEventDefinition escalationRef="EscB" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="Sub" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

        let err =
            parse_bpmn(xml).expect_err("a boundary with two escalation definitions is rejected");
        assert!(
            matches!(&err, ParseError::InvalidBoundaryEvent { reason, .. }
                if reason.contains("Bnd") && reason.contains("more than one event")),
            "expected InvalidBoundaryEvent for the duplicate-escalation Bnd, got {err:?}"
        );
    }

    #[test]
    fn should_reject_an_end_event_with_escalation_and_terminate_definitions() {
        // Regression (#1173): an `endEvent` carrying BOTH an escalation and a
        // terminate definition sets both node flags, and the build dispatch picks
        // escalation first — silently discarding the terminate semantics. There is
        // no combined-semantics end event in the supported subset, so reject the
        // ambiguous multi-definition end event at deploy.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:endEvent id="Bad">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
      <bpmn:terminateEventDefinition />
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Bad" />
  </bpmn:process>
</bpmn:definitions>"#;

        let err =
            parse_bpmn(xml).expect_err("an end event with escalation + terminate is rejected");
        assert!(
            matches!(
                &err,
                ParseError::InvalidEndEvent { element_id, reason, .. }
                    if element_id == "Bad" && reason.contains("more than one event definition")
            ),
            "expected InvalidEndEvent for the multi-definition Bad, got {err:?}"
        );
    }

    #[test]
    fn should_reject_an_intermediate_throw_with_competing_definitions() {
        // Regression (#1173): the throw form of the same class — an
        // `intermediateThrowEvent` carrying BOTH an escalation and a compensation
        // definition. The build dispatch checks escalation first, discarding the
        // compensation throw. Reject it (the throw-side `UnsupportedElement`
        // rejection, matching how a wrong-placement throw is already refused).
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:intermediateThrowEvent id="Bad">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
      <bpmn:compensateEventDefinition />
    </bpmn:intermediateThrowEvent>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Bad" />
    <bpmn:sequenceFlow id="f2" sourceRef="Bad" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;

        let err = parse_bpmn(xml)
            .expect_err("an intermediate throw with escalation + compensation is rejected");
        assert!(
            matches!(
                &err,
                ParseError::UnsupportedElement { tag, element_id }
                    if tag == "intermediateThrowEvent" && element_id == "Bad"
            ),
            "expected UnsupportedElement for the multi-definition throw Bad, got {err:?}"
        );
    }

    #[test]
    fn should_reject_an_escalation_throw_inside_an_adhoc_container() {
        // Regression (#1173, critical): an escalation *throw* placed directly in
        // an ad-hoc container is pruned to `AdHocToolKind::Other`; activating that
        // tool completes it directly, so the runtime escalation hook (which only
        // runs for `ElementKind::EscalationThrowEvent`) never fires and the
        // container's escalation boundary handler is silently skipped. nano cannot
        // execute a throw event as an ad-hoc tool, so reject the placement at
        // deploy naming the construct rather than let the model misbehave.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:adHocSubProcess id="Agent">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="agent" />
      </bpmn:extensionElements>
      <bpmn:serviceTask id="tool">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="tool" />
        </bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:intermediateThrowEvent id="ThrowEsc">
        <bpmn:escalationEventDefinition escalationRef="Esc" />
      </bpmn:intermediateThrowEvent>
    </bpmn:adHocSubProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="Agent">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Agent" />
    <bpmn:sequenceFlow id="f2" sourceRef="Agent" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

        let err = parse_bpmn(xml)
            .expect_err("an escalation throw inside an ad-hoc container is rejected");
        assert!(
            matches!(&err, ParseError::InvalidProcess { reason, .. }
                if reason.contains("ThrowEsc") && reason.contains("escalation throw event")),
            "expected InvalidProcess naming the escalation throw ThrowEsc, got {err:?}"
        );
    }

    #[test]
    fn should_reject_a_duplicate_escalation_definition_on_a_throw() {
        // Regression (#1173, suppressed advisory): a SECOND
        // `escalationEventDefinition` on one intermediate throw / end event
        // silently overwrites `escalation_ref` (last-wins), swapping the raised
        // code with no diagnostic — the throw/end analogue of the boundary
        // duplicate guard. Reject the ambiguous multi-definition throw at deploy.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:escalation id="EscA" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:escalation id="EscB" name="Any" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:endEvent id="Bad">
      <bpmn:escalationEventDefinition escalationRef="EscA" />
      <bpmn:escalationEventDefinition escalationRef="EscB" />
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Bad" />
  </bpmn:process>
</bpmn:definitions>"#;

        let err =
            parse_bpmn(xml).expect_err("an end event with two escalation definitions is rejected");
        assert!(
            matches!(
                &err,
                ParseError::InvalidEndEvent { element_id, reason, .. }
                    if element_id == "Bad" && reason.contains("more than one escalationEventDefinition")
            ),
            "expected InvalidEndEvent for the duplicate-escalation end event Bad, got {err:?}"
        );
    }

    #[test]
    fn should_capture_the_element_name_attribute() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="named" isExecutable="true">
    <bpmn:startEvent id="s" name="Start Here" />
    <bpmn:serviceTask id="charge" name="Charge Card">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="payment" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="charge" />
    <bpmn:sequenceFlow id="f2" sourceRef="charge" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(
            def.element("s").unwrap().name.as_deref(),
            Some("Start Here")
        );
        assert_eq!(
            def.element("charge").unwrap().name.as_deref(),
            Some("Charge Card")
        );
        // An element with no `name` attribute leaves it unset.
        assert_eq!(def.element("e").unwrap().name, None);
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
                agent_type: None,
                custom_headers: std::collections::BTreeMap::new(),
                linked_resources: Vec::new(),
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
                agent_type: None,
                custom_headers: std::collections::BTreeMap::new(),
                linked_resources: Vec::new(),
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
                agent_type: None,
                custom_headers: std::collections::BTreeMap::new(),
                linked_resources: Vec::new(),
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
                agent_type: None,
                custom_headers: std::collections::BTreeMap::new(),
                linked_resources: Vec::new(),
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
        assert_eq!(
            cat.tools[1].kind,
            crate::model::AdHocToolKind::UserTask(crate::model::UserTaskProps::default())
        );
        assert_eq!(cat.tools[2].kind, crate::model::AdHocToolKind::Other);
        // and: the `bpmn:sequenceFlow` between the container's own children is
        // captured as an inner flow (issue #1154) — NOT silently dropped — so the
        // runtime can drive the "structured sequence" tool_review -> tool_gw.
        assert_eq!(cat.inner_flows.len(), 1);
        assert_eq!(cat.inner_flows[0].from, "tool_review");
        assert_eq!(cat.inner_flows[0].to, "tool_gw");
        assert_eq!(cat.inner_flows[0].condition, None);
    }

    // ---- Deploy-time ad-hoc validation (gap #6, Zeebe AdHocSubProcessValidator) ----
    // Each case is a full model that is REJECTED at parse/deploy on the branch and
    // (was) silently accepted on `main`. Together they assert the whole defect class.

    /// A minimal ad-hoc container XML with the given inner body / attributes /
    /// extension wiring, wired s -> agent -> e. Callers vary one facet to isolate
    /// a single validation rule.
    fn adhoc_model(container_attrs: &str, ext: &str, inner: &str) -> String {
        format!(
            r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:adHocSubProcess id="agent"{container_attrs}>
                <bpmn:extensionElements>{ext}</bpmn:extensionElements>
                {inner}
              </bpmn:adHocSubProcess>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
              <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#
        )
    }

    fn assert_rejected(xml: &str, needle: &str) {
        match parse_bpmn(xml) {
            Err(ParseError::InvalidProcess { reason, .. }) => assert!(
                reason.contains(needle),
                "expected rejection mentioning {needle:?}, got: {reason}"
            ),
            other => panic!("expected InvalidProcess mentioning {needle:?}, got {other:?}"),
        }
    }

    #[test]
    fn rejects_adhoc_subprocess_with_no_activity() {
        let td = r#"<zeebe:taskDefinition type="agent" />"#;
        let xml = adhoc_model("", td, "");
        assert_rejected(&xml, "at least one activity");
    }

    #[test]
    fn rejects_adhoc_subprocess_with_only_non_activity_children() {
        // Regression guard: a container with direct children that are NOT
        // activities (here a lone gateway) must still be rejected. A bare
        // `inner.is_empty()` check would wrongly accept this, so the validator
        // must require at least one task/sub-process/call activity.
        let td = r#"<zeebe:taskDefinition type="agent" />"#;
        let inner = r#"<bpmn:exclusiveGateway id="g" />"#;
        let xml = adhoc_model("", td, inner);
        assert_rejected(&xml, "at least one activity");
    }

    #[test]
    fn rejects_adhoc_subprocess_containing_a_start_event() {
        let td = r#"<zeebe:taskDefinition type="agent" />"#;
        let inner = r#"<bpmn:serviceTask id="tool" /><bpmn:startEvent id="inner_start" />"#;
        let xml = adhoc_model("", td, inner);
        assert_rejected(&xml, "must not contain a start event");
    }

    #[test]
    fn rejects_adhoc_subprocess_containing_an_end_event() {
        let td = r#"<zeebe:taskDefinition type="agent" />"#;
        let inner = r#"<bpmn:serviceTask id="tool" /><bpmn:endEvent id="inner_end" />"#;
        let xml = adhoc_model("", td, inner);
        assert_rejected(&xml, "must not contain an end event");
    }

    // ---- Embedded subProcess tool start-event validation (#872) ----
    // An embedded `subProcess` tool is driven by injecting a token at its inner
    // start event, so it must have exactly one. Zero or many is rejected at
    // parse/deploy time rather than silently defaulting to an empty/ambiguous
    // start id (which would `Step::Activate` a non-existent element at runtime).

    #[test]
    fn rejects_embedded_subprocess_tool_with_no_start_event() {
        let td = r#"<zeebe:taskDefinition type="agent" />"#;
        let inner = r#"<bpmn:subProcess id="review"><bpmn:userTask id="ask" /></bpmn:subProcess>"#;
        let xml = adhoc_model("", td, inner);
        assert_rejected(&xml, "must have exactly one start event");
    }

    #[test]
    fn rejects_embedded_subprocess_tool_with_multiple_start_events() {
        let td = r#"<zeebe:taskDefinition type="agent" />"#;
        let inner = r#"<bpmn:subProcess id="review">
            <bpmn:startEvent id="r_s1" />
            <bpmn:startEvent id="r_s2" />
            <bpmn:userTask id="ask" />
        </bpmn:subProcess>"#;
        let xml = adhoc_model("", td, inner);
        assert_rejected(&xml, "must have exactly one start event");
    }

    #[test]
    fn accepts_embedded_subprocess_tool_with_one_start_event() {
        let td = r#"<zeebe:taskDefinition type="agent" />"#;
        let inner = r#"<bpmn:subProcess id="review">
            <bpmn:startEvent id="r_s" />
            <bpmn:userTask id="ask" />
            <bpmn:sequenceFlow id="rf1" sourceRef="r_s" targetRef="ask" />
        </bpmn:subProcess>"#;
        let xml = adhoc_model("", td, inner);
        let def = &parse_bpmn(&xml).unwrap()[0];
        let tool = def.adhoc[0]
            .tools
            .iter()
            .find(|t| t.element_id == "review")
            .expect("embedded subProcess tool is catalogued");
        assert_eq!(
            tool.kind,
            crate::model::AdHocToolKind::SubProcess {
                start_event: "r_s".to_string(),
            }
        );
    }

    #[test]
    fn rejects_taskdefinition_with_active_elements_collection() {
        let ext = r#"<zeebe:taskDefinition type="agent" /><zeebe:adHoc activeElementsCollection="=elems" />"#;
        let inner = r#"<bpmn:serviceTask id="tool" />"#;
        let xml = adhoc_model("", ext, inner);
        assert_rejected(&xml, "activeElementsCollection");
    }

    #[test]
    fn rejects_output_element_without_output_collection() {
        let ext =
            r#"<zeebe:taskDefinition type="agent" /><zeebe:adHoc outputElement="={ id: 1 }" />"#;
        let inner = r#"<bpmn:serviceTask id="tool" />"#;
        let xml = adhoc_model("", ext, inner);
        assert_rejected(&xml, "outputElement and outputCollection");
    }

    #[test]
    fn rejects_output_collection_without_output_element() {
        let ext =
            r#"<zeebe:taskDefinition type="agent" /><zeebe:adHoc outputCollection="results" />"#;
        let inner = r#"<bpmn:serviceTask id="tool" />"#;
        let xml = adhoc_model("", ext, inner);
        assert_rejected(&xml, "outputElement and outputCollection");
    }

    #[test]
    fn accepts_a_valid_job_worker_adhoc_subprocess() {
        // A well-formed JOB_WORKER container: one activity, both output fields set,
        // cancelRemainingInstances left at its default — must parse cleanly.
        let ext = r#"<zeebe:taskDefinition type="agent" /><zeebe:adHoc outputCollection="results" outputElement="={ id: 1 }" />"#;
        let inner = r#"<bpmn:serviceTask id="tool"><bpmn:extensionElements><zeebe:taskDefinition type="http" /></bpmn:extensionElements></bpmn:serviceTask>"#;
        let xml = adhoc_model("", ext, inner);
        let def = &parse_bpmn(&xml).unwrap()[0];
        assert_eq!(def.adhoc.len(), 1);
        assert_eq!(def.adhoc[0].container_id, "agent");
    }

    #[test]
    fn accepts_job_worker_adhoc_with_completion_condition() {
        // Deliberate divergence from Zeebe (issue #614 gap 6): nano supports an
        // engine-side `<completionCondition>` on a JOB_WORKER (taskDefinition)
        // ad-hoc container, so the deploy validator must NOT reject it.
        let ext = r#"<zeebe:taskDefinition type="agent" /><zeebe:adHoc outputCollection="results" outputElement="=result" />"#;
        let inner = r#"<bpmn:serviceTask id="tool"><bpmn:extensionElements><zeebe:taskDefinition type="tool" /></bpmn:extensionElements></bpmn:serviceTask><bpmn:completionCondition>=done = true</bpmn:completionCondition>"#;
        let xml = adhoc_model("", ext, inner);
        let def = &parse_bpmn(&xml).unwrap()[0];
        assert_eq!(def.adhoc.len(), 1);
    }

    #[test]
    fn accepts_job_worker_adhoc_with_cancel_remaining_instances_false() {
        // Deliberate divergence from Zeebe (issue #614 gap 6): Zeebe's
        // `AdHocSubProcessValidator` forbids `cancelRemainingInstances="false"`
        // alongside a `zeebe:taskDefinition` (its JOB_WORKER path carries cancel
        // purely on the job result), but nano defers `cancelRemainingInstances`
        // separately (gap 7) and so must NOT reject a JOB_WORKER container that
        // carries it. This is the positive twin of
        // `accepts_job_worker_adhoc_with_completion_condition`, guarding against a
        // future refactor accidentally reintroducing Zeebe's stricter rule.
        let ext = r#"<zeebe:taskDefinition type="agent" /><zeebe:adHoc outputCollection="results" outputElement="=result" />"#;
        let inner = r#"<bpmn:serviceTask id="tool"><bpmn:extensionElements><zeebe:taskDefinition type="tool" /></bpmn:extensionElements></bpmn:serviceTask>"#;
        let xml = adhoc_model(r#" cancelRemainingInstances="false""#, ext, inner);
        let def = &parse_bpmn(&xml).unwrap()[0];
        assert_eq!(def.adhoc.len(), 1);
        assert_eq!(def.adhoc[0].container_id, "agent");
    }

    #[test]
    fn accepts_declarative_active_elements_collection_without_taskdefinition() {
        // The declarative BPMN_TASK variant legitimately declares
        // activeElementsCollection WITHOUT a taskDefinition — it must NOT be
        // rejected by the taskDefinition-combination rule.
        let ext = r#"<zeebe:adHoc activeElementsCollection="=elems" />"#;
        let inner = r#"<bpmn:serviceTask id="tool" />"#;
        let xml = adhoc_model("", ext, inner);
        let def = &parse_bpmn(&xml).unwrap()[0];
        assert_eq!(
            def.adhoc[0].impl_type,
            crate::model::AdHocImplementationType::BpmnTask
        );
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
    fn should_parse_form_definitions_on_user_task_and_start_event() {
        // given: a start event carrying a start form and a user task carrying its
        // own form, both via zeebe:formDefinition (as the Camunda modeler emits).
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s">
                <bpmn:extensionElements>
                  <zeebe:formDefinition formId="start-form" />
                </bpmn:extensionElements>
              </bpmn:startEvent>
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:formDefinition formId="review-form" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then: the start form rides on the definition, and the user-task form on
        // its props.
        assert_eq!(def.start_form_id.as_deref(), Some("start-form"));
        let ElementKind::UserTask(props) = &def.element("review").unwrap().kind else {
            panic!("expected a user task");
        };
        assert_eq!(props.form_id.as_deref(), Some("review-form"));
        assert_eq!(props.external_form_reference, None);
    }

    #[test]
    fn should_parse_external_form_reference_on_user_task() {
        // given: a user task referencing an external form (no deployed formId).
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:formDefinition externalReference="https://forms.example/x" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        let ElementKind::UserTask(props) = &def.element("review").unwrap().kind else {
            panic!("expected a user task");
        };
        assert_eq!(props.form_id, None);
        assert_eq!(
            props.external_form_reference.as_deref(),
            Some("https://forms.example/x")
        );
        assert_eq!(def.start_form_id, None);
    }

    #[test]
    fn should_let_external_reference_win_when_form_definition_declares_both() {
        // given: a (malformed but tolerated) zeebe:formDefinition that declares
        // BOTH formId and externalReference. Zeebe treats these as mutually
        // exclusive, so the parser must keep them so downstream — an external
        // reference wins and suppresses the form id, ensuring a task never
        // surfaces both a numeric formKey and an externalFormReference.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="both">
                <bpmn:extensionElements>
                  <zeebe:formDefinition formId="feature-escalation"
                                        externalReference="https://forms.example/x" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="both" />
              <bpmn:sequenceFlow id="b" sourceRef="both" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then: the external reference wins; the form id is suppressed.
        let ElementKind::UserTask(both) = &def.element("both").unwrap().kind else {
            panic!("expected a user task");
        };
        assert_eq!(both.form_id, None);
        assert_eq!(
            both.external_form_reference.as_deref(),
            Some("https://forms.example/x")
        );
    }

    #[test]
    fn should_accept_explicit_latest_binding_on_a_user_task_form() {
        // given: a user-task formDefinition that explicitly declares the default
        // `bindingType="latest"`. This is the binding the engine implements
        // (resolve `formId` to the latest deployed form version at task
        // creation), so it parses exactly like an omitted bindingType.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:formDefinition formId="review-form" bindingType="latest" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then: no regression — the form id binds latest-at-creation as before.
        let ElementKind::UserTask(props) = &def.element("review").unwrap().kind else {
            panic!("expected a user task");
        };
        assert_eq!(props.form_id.as_deref(), Some("review-form"));
    }

    #[test]
    fn should_reject_deployment_binding_on_a_user_task_form() {
        // given: a user-task formDefinition declaring `bindingType="deployment"`.
        // The engine does not implement the `deployment` binding; degrading it to
        // `latest` would silently mis-bind the form (#1190), so the deploy must
        // be rejected loudly rather than parsed.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:formDefinition formId="review-form" bindingType="deployment" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when / then
        let err = parse_bpmn(xml).expect_err("a non-latest form binding is rejected");
        assert_eq!(
            err,
            ParseError::UnsupportedUserTaskFormBinding {
                task_id: "review".to_string(),
                binding_type: "deployment".to_string(),
            }
        );
    }

    #[test]
    fn should_reject_version_tag_binding_on_a_user_task_form() {
        // given: a user-task formDefinition declaring `bindingType="versionTag"`.
        // Like `deployment`, this binding is unimplemented and must fail the
        // deploy loudly rather than degrade to `latest` (#1190).
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:formDefinition formId="review-form"
                                        bindingType="versionTag" versionTag="v2" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when / then
        let err = parse_bpmn(xml).expect_err("a versionTag form binding is rejected");
        assert_eq!(
            err,
            ParseError::UnsupportedUserTaskFormBinding {
                task_id: "review".to_string(),
                binding_type: "versionTag".to_string(),
            }
        );
    }

    #[test]
    fn should_ignore_non_latest_binding_on_an_external_reference_form() {
        // given: an externalReference form that also (redundantly) carries a
        // non-latest bindingType. The externalReference suppresses `formId` and
        // no form version is resolved, so the unimplemented binding is moot and
        // must not trip the interim guard.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:formDefinition externalReference="https://forms.example/x"
                                        bindingType="deployment" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).expect("an external-reference form is not version-bound")[0];

        // then
        let ElementKind::UserTask(props) = &def.element("review").unwrap().kind else {
            panic!("expected a user task");
        };
        assert_eq!(props.form_id, None);
        assert_eq!(
            props.external_form_reference.as_deref(),
            Some("https://forms.example/x")
        );
    }

    #[test]
    fn should_reject_explicit_empty_binding_on_a_user_task_form() {
        // given: a user-task formDefinition declaring an explicitly *empty*
        // `bindingType=""`. An empty attribute is a present, explicit value — not
        // an absent one — so it must not be silently treated as the `latest`
        // default (which would reopen the silent-degradation path #1190 closes).
        // Only an omitted `bindingType` defaults.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:formDefinition formId="review-form" bindingType="" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when / then
        let err = parse_bpmn(xml).expect_err("an explicit empty form binding is rejected");
        assert_eq!(
            err,
            ParseError::UnsupportedUserTaskFormBinding {
                task_id: "review".to_string(),
                binding_type: String::new(),
            }
        );
    }

    #[test]
    fn should_reject_non_latest_binding_when_form_id_is_absent() {
        // given: a user-task formDefinition with an unimplemented
        // `bindingType="deployment"` but no `formId` (and no `externalReference`).
        // Keying the guard off the raw `externalReference` — not off a resolved
        // `formId` — closes this path: a deployed (non-external) form must carry
        // only the default binding regardless of whether a `formId` happens to be
        // present, so this declaration is rejected rather than silently accepted.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:formDefinition bindingType="deployment" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when / then
        let err = parse_bpmn(xml).expect_err("a non-latest binding without a formId is rejected");
        assert_eq!(
            err,
            ParseError::UnsupportedUserTaskFormBinding {
                task_id: "review".to_string(),
                binding_type: "deployment".to_string(),
            }
        );
    }

    #[test]
    fn should_parse_service_task_task_headers_into_custom_headers() {
        // given: a service task carrying static zeebe:taskHeaders alongside its
        // taskDefinition, as Camunda 8 emits custom headers. Two headers, given
        // out of key order, to prove the parser captures every entry (order is
        // normalised by the BTreeMap).
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="payment" />
                  <zeebe:taskHeaders>
                    <zeebe:header key="retryBackoff" value="PT5S" />
                    <zeebe:header key="channel" value="card" />
                  </zeebe:taskHeaders>
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="b" sourceRef="charge" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then: both headers ride on the service task element verbatim.
        let mut expected = std::collections::BTreeMap::new();
        expected.insert("channel".to_string(), "card".to_string());
        expected.insert("retryBackoff".to_string(), "PT5S".to_string());
        assert_eq!(
            def.element("charge").unwrap().kind,
            ElementKind::ServiceTask {
                job_type: "payment".to_string(),
                priority: None,
                agent_type: None,
                custom_headers: expected,
                linked_resources: Vec::new(),
            }
        );
    }

    #[test]
    fn self_closing_task_headers_do_not_leak_into_later_headers() {
        // given: a first service task with a self-closing (empty)
        // `<zeebe:taskHeaders />`, then a second service task whose own
        // `zeebe:header` sits *outside* any taskHeaders container. A
        // self-closing start tag emits no matching end tag, so the
        // `in_task_headers` gate must not stay stuck open across elements.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="first">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="a" />
                  <zeebe:taskHeaders />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:serviceTask id="second">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="b" />
                  <zeebe:header key="stray" value="nope" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="first" />
              <bpmn:sequenceFlow id="f2" sourceRef="first" targetRef="second" />
              <bpmn:sequenceFlow id="f3" sourceRef="second" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then: the empty container yields no headers, and the stray header
        // that follows is *not* captured onto the second task.
        for id in ["first", "second"] {
            match &def.element(id).unwrap().kind {
                ElementKind::ServiceTask { custom_headers, .. } => {
                    assert!(
                        custom_headers.is_empty(),
                        "{id} unexpectedly captured headers: {custom_headers:?}"
                    );
                }
                other => panic!("{id} should be a service task, got {other:?}"),
            }
        }
    }

    #[test]
    fn stray_linked_resource_outside_a_container_is_not_captured() {
        // given: a first service task with a self-closing (empty)
        // `<zeebe:linkedResources />`, then a second service task whose own
        // `zeebe:linkedResource` sits *outside* any linkedResources container.
        // The `in_linked_resources` gate must reject the stray link and must
        // not stay stuck open across elements (a self-closing start tag emits
        // no matching end tag). Mirrors the `zeebe:taskHeaders` gate.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="first">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="a" />
                  <zeebe:linkedResources />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:serviceTask id="second">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="b" />
                  <zeebe:linkedResource resourceId="stray.md" bindingType="latest"
                                        resourceType="GenericScript" linkName="stray" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="first" />
              <bpmn:sequenceFlow id="f2" sourceRef="first" targetRef="second" />
              <bpmn:sequenceFlow id="f3" sourceRef="second" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then: the empty container yields no links, and the stray
        // linkedResource that follows is *not* captured onto the second task.
        for id in ["first", "second"] {
            match &def.element(id).unwrap().kind {
                ElementKind::ServiceTask {
                    linked_resources, ..
                } => {
                    assert!(
                        linked_resources.is_empty(),
                        "{id} unexpectedly captured linked resources: {linked_resources:?}"
                    );
                }
                other => panic!("{id} should be a service task, got {other:?}"),
            }
        }
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
                agent_type: None,
                custom_headers: std::collections::BTreeMap::new(),
                linked_resources: Vec::new(),
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
                custom_headers: std::collections::BTreeMap::new(),
                agent_type: None,
                linked_resources: Vec::new(),
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
                agent_type: None,
                custom_headers: std::collections::BTreeMap::new(),
                linked_resources: Vec::new(),
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
              <exclusiveGateway id="gw" default="f2" />
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
    fn should_parse_event_based_gateway_and_its_catch_events() {
        // given: an event-based gateway routing to a timer and a message
        // intermediate catch event (a classic timer-vs-message race).
        let xml = r#"
          <definitions>
            <message id="Msg_reply" name="reply">
              <extensionElements>
                <zeebe:subscription correlationKey="=orderId" />
              </extensionElements>
            </message>
            <process id="race">
              <startEvent id="s" />
              <eventBasedGateway id="gw" />
              <intermediateCatchEvent id="onTimer">
                <timerEventDefinition><timeDuration>PT1H</timeDuration></timerEventDefinition>
              </intermediateCatchEvent>
              <intermediateCatchEvent id="onReply">
                <messageEventDefinition messageRef="Msg_reply" />
              </intermediateCatchEvent>
              <endEvent id="timedOut" />
              <endEvent id="replied" />
              <sequenceFlow id="f0" sourceRef="s" targetRef="gw" />
              <sequenceFlow id="f1" sourceRef="gw" targetRef="onTimer" />
              <sequenceFlow id="f2" sourceRef="gw" targetRef="onReply" />
              <sequenceFlow id="f3" sourceRef="onTimer" targetRef="timedOut" />
              <sequenceFlow id="f4" sourceRef="onReply" targetRef="replied" />
            </process>
          </definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then: the gateway parsed as an event-based gateway with both targets.
        let gw = def.element("gw").unwrap();
        assert_eq!(gw.kind, crate::model::ElementKind::EventBasedGateway);
        let targets: std::collections::BTreeSet<&str> =
            gw.outgoing.iter().map(|f| f.to.as_str()).collect();
        assert_eq!(targets, ["onReply", "onTimer"].into_iter().collect());
    }

    #[test]
    fn should_parse_terminate_end_event() {
        // given: a plain end event and an end event carrying a
        // `terminateEventDefinition` (a terminate end).
        let xml = r#"
          <definitions>
            <process id="term">
              <startEvent id="s" />
              <parallelGateway id="split" />
              <endEvent id="plain" />
              <endEvent id="stop">
                <terminateEventDefinition />
              </endEvent>
              <sequenceFlow id="f0" sourceRef="s" targetRef="split" />
              <sequenceFlow id="f1" sourceRef="split" targetRef="plain" />
              <sequenceFlow id="f2" sourceRef="split" targetRef="stop" />
            </process>
          </definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then: the terminate end parses to the terminate kind; the plain end
        // stays a none end event.
        assert_eq!(
            def.element("stop").unwrap().kind,
            crate::model::ElementKind::TerminateEndEvent
        );
        assert_eq!(
            def.element("plain").unwrap().kind,
            crate::model::ElementKind::EndEvent
        );
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
    fn should_decode_numeric_character_references_in_a_correlation_key() {
        // Camunda Modeler emits `&#34;` (decimal) / `&#x22;` (hex) for the
        // double-quotes of a FEEL string literal placed in an attribute value.
        // The shared attribute-value decoder must expand those numeric character
        // references to `"` before the value reaches FEEL — identical to
        // `&quot;`. Regression guard for issue #885: previously the numeric forms
        // round-tripped undecoded, so the correlation FEEL reached FEEL as the raw
        // `=&#34;k&#34;` and either evaluated to `""` or raised `unexpected
        // character '&'`.
        for reference in ["&#34;", "&#x22;", "&#X22;", "&quot;"] {
            let xml = format!(
                r#"
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
                    <zeebe:subscription correlationKey="={ref}k{ref}" />
                  </bpmn:extensionElements>
                </bpmn:message>
              </bpmn:definitions>"#,
                ref = reference
            );

            let def = &parse_bpmn(&xml).unwrap()[0];

            assert_eq!(
                def.element("await").unwrap().kind,
                ElementKind::MessageIntermediateCatchEvent {
                    message_name: "payment-received".to_string(),
                    correlation_key: "\"k\"".to_string(),
                },
                "correlation key FEEL for reference {reference} should decode to \"k\""
            );
        }
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
    fn should_parse_io_mapping_on_an_intermediate_catch_event() {
        // given — a message catch event carrying a `zeebe:ioMapping` whose output
        // increments a loop counter when the event is triggered (the
        // urban-pr-review convergence loop's `=round + 1 -> round`).
        let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:intermediateCatchEvent id="await">
                <bpmn:extensionElements>
                  <zeebe:ioMapping>
                    <zeebe:input source="=round" target="prevRound" />
                    <zeebe:output source="=round + 1" target="round" />
                  </zeebe:ioMapping>
                </bpmn:extensionElements>
                <bpmn:messageEventDefinition messageRef="Message_1" />
              </bpmn:intermediateCatchEvent>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="await" />
              <bpmn:sequenceFlow id="f1" sourceRef="await" targetRef="e" />
            </bpmn:process>
            <bpmn:message id="Message_1" name="review-ready">
              <bpmn:extensionElements>
                <zeebe:subscription correlationKey="=prKey" />
              </bpmn:extensionElements>
            </bpmn:message>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then — the mapping attaches to the catch event (previously it was
        // dropped because the event was never pushed onto the io_stack).
        let io = &def.element("await").unwrap().io;
        assert_eq!(io.inputs.len(), 1);
        assert_eq!(io.inputs[0].source, "=round");
        assert_eq!(io.inputs[0].target, "prevRound");
        assert_eq!(io.outputs.len(), 1);
        assert_eq!(io.outputs[0].source, "=round + 1");
        assert_eq!(io.outputs[0].target, "round");
    }

    #[test]
    fn end_event_io_mapping_attaches_to_the_end_event_not_the_enclosing_subprocess() {
        // given — a sub-process with two end events, each carrying a
        // `zeebe:output` mapping for the same target. Each mapping must attach to
        // its OWN end event; none may hoist onto the enclosing sub-process.
        // Previously end events were never pushed onto the io_stack, so every
        // nested end-event mapping fell through to the innermost open activity
        // (the sub-process). With several end events mapping the same target, the
        // sub-process then collected them all and the last-parsed one clobbered
        // the rest at completion — a "fixed" outcome routed as "escalate" in the
        // nano-workforce merge-loop (#466).
        let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="sub" />
              <bpmn:subProcess id="sub">
                <bpmn:startEvent id="ss" />
                <bpmn:sequenceFlow id="f1" sourceRef="ss" targetRef="endA" />
                <bpmn:endEvent id="endA">
                  <bpmn:extensionElements>
                    <zeebe:ioMapping>
                      <zeebe:output source="=&#34;A&#34;" target="outcome" />
                    </zeebe:ioMapping>
                  </bpmn:extensionElements>
                </bpmn:endEvent>
                <bpmn:endEvent id="endB">
                  <bpmn:extensionElements>
                    <zeebe:ioMapping>
                      <zeebe:output source="=&#34;B&#34;" target="outcome" />
                    </zeebe:ioMapping>
                  </bpmn:extensionElements>
                </bpmn:endEvent>
              </bpmn:subProcess>
              <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="done" />
              <bpmn:endEvent id="done" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then — each end event owns exactly its own mapping, and the sub-process
        // (and the mapping-free `done` end event) own none.
        assert!(
            def.element("sub").unwrap().io.outputs.is_empty(),
            "end-event mappings must not hoist onto the enclosing sub-process"
        );
        let end_a = &def.element("endA").unwrap().io.outputs;
        assert_eq!(end_a.len(), 1);
        assert_eq!(end_a[0].source, "=\"A\"");
        assert_eq!(end_a[0].target, "outcome");
        let end_b = &def.element("endB").unwrap().io.outputs;
        assert_eq!(end_b.len(), 1);
        assert_eq!(end_b[0].source, "=\"B\"");
        assert!(def.element("done").unwrap().io.outputs.is_empty());
    }

    #[test]
    fn throw_event_io_mapping_attaches_to_the_throw_event_not_the_enclosing_subprocess() {
        // Same defect class as end events: a `zeebe:ioMapping` on a message
        // intermediate throw event nested in a sub-process must attach to the
        // throw event, not hoist onto the enclosing sub-process.
        let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="sub" />
              <bpmn:subProcess id="sub">
                <bpmn:startEvent id="ss" />
                <bpmn:sequenceFlow id="f1" sourceRef="ss" targetRef="thr" />
                <bpmn:intermediateThrowEvent id="thr">
                  <bpmn:extensionElements>
                    <zeebe:taskDefinition type="notify" />
                    <zeebe:ioMapping>
                      <zeebe:output source="=&#34;sent&#34;" target="state" />
                    </zeebe:ioMapping>
                  </bpmn:extensionElements>
                  <bpmn:messageEventDefinition id="m" />
                </bpmn:intermediateThrowEvent>
                <bpmn:sequenceFlow id="f2" sourceRef="thr" targetRef="e" />
                <bpmn:endEvent id="e" />
              </bpmn:subProcess>
            </bpmn:process>
          </bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];

        assert!(
            def.element("sub").unwrap().io.outputs.is_empty(),
            "throw-event mapping must not hoist onto the enclosing sub-process"
        );
        let thr = &def.element("thr").unwrap().io.outputs;
        assert_eq!(thr.len(), 1);
        assert_eq!(thr[0].source, "=\"sent\"");
        assert_eq!(thr[0].target, "state");
    }

    #[test]
    fn should_not_misattribute_io_mapping_after_an_id_less_catch_event() {
        // given — an intermediate catch event with no `id` (so it is never
        // pushed onto the io_stack) followed by a service task carrying a
        // `zeebe:ioMapping`. A previously unconditional pop on
        // `</intermediateCatchEvent>` would underflow/detach the stack and
        // cause the service task's mapping to attach to the wrong node.
        let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:intermediateCatchEvent>
                <bpmn:messageEventDefinition messageRef="Message_1" />
              </bpmn:intermediateCatchEvent>
              <bpmn:serviceTask id="charge">
                <bpmn:extensionElements>
                  <zeebe:ioMapping>
                    <zeebe:input source="=amount" target="chargeAmount" />
                    <zeebe:output source="=result" target="chargeResult" />
                  </zeebe:ioMapping>
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="f1" sourceRef="charge" targetRef="e" />
            </bpmn:process>
            <bpmn:message id="Message_1" name="review-ready">
              <bpmn:extensionElements>
                <zeebe:subscription correlationKey="=prKey" />
              </bpmn:extensionElements>
            </bpmn:message>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then — the mapping attaches to the service task, not a stray node.
        let io = &def.element("charge").unwrap().io;
        assert_eq!(io.inputs.len(), 1);
        assert_eq!(io.inputs[0].source, "=amount");
        assert_eq!(io.inputs[0].target, "chargeAmount");
        assert_eq!(io.outputs.len(), 1);
        assert_eq!(io.outputs[0].source, "=result");
        assert_eq!(io.outputs[0].target, "chargeResult");
    }

    #[test]
    fn should_not_misattribute_io_mapping_after_an_id_less_activity() {
        // given — a sub-process (pushed onto the io_stack) whose first child is an
        // id-less `sendTask` (so `add_node` returns `None` and the task is never
        // pushed), followed by the sub-process's own `zeebe:ioMapping`. A
        // previously unconditional pop on `</sendTask>` would remove the enclosing
        // sub-process from the io_stack, so the sub-process's own output mapping
        // would fall onto a stray node (or be dropped). Same defect class as the
        // id-less event handlers — every leaf-activity close path must guard its
        // pop on having actually pushed.
        let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="sub" />
              <bpmn:subProcess id="sub">
                <bpmn:sendTask></bpmn:sendTask>
                <bpmn:extensionElements>
                  <zeebe:ioMapping>
                    <zeebe:output source="=&#34;done&#34;" target="subOut" />
                  </zeebe:ioMapping>
                </bpmn:extensionElements>
                <bpmn:startEvent id="ss" />
                <bpmn:sequenceFlow id="f1" sourceRef="ss" targetRef="se" />
                <bpmn:endEvent id="se" />
              </bpmn:subProcess>
              <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="e" />
              <bpmn:endEvent id="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then — the mapping still attaches to the enclosing sub-process, because
        // the id-less send task never popped it off the io_stack.
        let io = &def.element("sub").unwrap().io;
        assert_eq!(
            io.outputs.len(),
            1,
            "sub-process must keep its own output mapping after an id-less child activity"
        );
        assert_eq!(io.outputs[0].source, "=\"done\"");
        assert_eq!(io.outputs[0].target, "subOut");
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
    fn none_plus_message_start_keeps_the_message_start_typed() {
        // Regression guard (#855): a none start alongside a message start must
        // keep the message start as a MessageStartEvent — NOT demote it to an
        // inert throw event — so deploy can open its subscription. The none start
        // remains the process-entry start.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="none_s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
              <bpmn:startEvent id="msg_s">
                <bpmn:messageEventDefinition messageRef="Message_1" />
                <bpmn:outgoing>f2</bpmn:outgoing>
              </bpmn:startEvent>
              <bpmn:endEvent id="e1"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
              <bpmn:endEvent id="e2"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="none_s" targetRef="e1" />
              <bpmn:sequenceFlow id="f2" sourceRef="msg_s" targetRef="e2" />
            </bpmn:process>
            <bpmn:message id="Message_1" name="order-placed" />
          </bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(def.start_event, "none_s", "none start is the process entry");
        assert_eq!(
            def.element("msg_s").unwrap().kind,
            ElementKind::MessageStartEvent {
                message_name: "order-placed".to_string(),
            },
            "surplus message start must survive un-demoted"
        );
    }

    #[test]
    fn none_plus_timer_start_keeps_the_timer_start_typed() {
        // Regression guard (#855): a surplus timer start must survive as a
        // TimerStartEvent so deploy can arm its process-level timer.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="none_s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
              <bpmn:startEvent id="timer_s">
                <bpmn:timerEventDefinition><bpmn:timeDuration>PT10S</bpmn:timeDuration></bpmn:timerEventDefinition>
                <bpmn:outgoing>f2</bpmn:outgoing>
              </bpmn:startEvent>
              <bpmn:endEvent id="e1"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
              <bpmn:endEvent id="e2"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="none_s" targetRef="e1" />
              <bpmn:sequenceFlow id="f2" sourceRef="timer_s" targetRef="e2" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(def.start_event, "none_s");
        assert_eq!(
            def.element("timer_s").unwrap().kind,
            ElementKind::TimerStartEvent {
                interval_millis: 10_000,
                repeating: false,
            },
            "surplus timer start must survive un-demoted"
        );
    }

    #[test]
    fn none_plus_signal_start_still_demotes_the_signal_start() {
        // Nano has no dedicated signal-start element kind, so a surplus signal
        // start is still demoted to an inert throw event: keeping it would make
        // it a second `ElementKind::StartEvent`, wrongly counted as a second
        // *none* start by the start-events validator (#855).
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="none_s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
              <bpmn:startEvent id="signal_s">
                <bpmn:signalEventDefinition signalRef="Signal_1" />
                <bpmn:outgoing>f2</bpmn:outgoing>
              </bpmn:startEvent>
              <bpmn:endEvent id="e1"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
              <bpmn:endEvent id="e2"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="none_s" targetRef="e1" />
              <bpmn:sequenceFlow id="f2" sourceRef="signal_s" targetRef="e2" />
            </bpmn:process>
            <bpmn:signal id="Signal_1" name="go" />
          </bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(def.start_event, "none_s");
        assert_eq!(
            def.element("signal_s").unwrap().kind,
            ElementKind::IntermediateThrowEvent,
            "surplus signal start is demoted (no dedicated signal-start kind)"
        );
    }

    #[test]
    fn should_parse_link_throw_and_catch_events() {
        // A linkEventDefinition on an intermediateThrowEvent / intermediateCatchEvent
        // parses to the dedicated link kinds (#1157), preserving the link name —
        // not to a plain throw or a timer catch.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s"><bpmn:outgoing>f0</bpmn:outgoing></bpmn:startEvent>
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="throw" />
              <bpmn:intermediateThrowEvent id="throw">
                <bpmn:incoming>f0</bpmn:incoming>
                <bpmn:linkEventDefinition name="hop" />
              </bpmn:intermediateThrowEvent>
              <bpmn:intermediateCatchEvent id="catch">
                <bpmn:outgoing>f1</bpmn:outgoing>
                <bpmn:linkEventDefinition name="hop" />
              </bpmn:intermediateCatchEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="catch" targetRef="e" />
              <bpmn:endEvent id="e"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
            </bpmn:process>
          </bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(
            def.element("throw").unwrap().kind,
            ElementKind::LinkIntermediateThrowEvent {
                link_name: "hop".to_string()
            }
        );
        assert_eq!(
            def.element("catch").unwrap().kind,
            ElementKind::LinkIntermediateCatchEvent {
                link_name: "hop".to_string()
            }
        );
        // The throw has no outgoing flow; the catch has no incoming flow.
        assert!(def.element("throw").unwrap().outgoing.is_empty());
        assert_eq!(def.incoming_count("catch"), 0);
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
                propagate_all_parent_variables: true,
                propagate_all_child_variables: true,
            }
        );
        assert_eq!(
            def.element("c2").unwrap().kind,
            ElementKind::CallActivity {
                called_process_id: "Phase02".to_string(),
                propagate_all_parent_variables: true,
                propagate_all_child_variables: true,
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
    fn should_parse_call_activity_variable_propagation_flags() {
        // Guard the silent-drop failure mode: the Zeebe
        // propagateAllParentVariables / propagateAllChildVariables attributes on
        // `zeebe:calledElement` must be captured (absent ⇒ true, explicit
        // `="false"` honored), not dropped on the floor.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="orch">
              <bpmn:startEvent id="start" />
              <bpmn:callActivity id="c_default">
                <bpmn:extensionElements>
                  <zeebe:calledElement processId="P" />
                </bpmn:extensionElements>
              </bpmn:callActivity>
              <bpmn:callActivity id="c_no_parent">
                <bpmn:extensionElements>
                  <zeebe:calledElement processId="P" propagateAllParentVariables="false" />
                </bpmn:extensionElements>
              </bpmn:callActivity>
              <bpmn:callActivity id="c_no_child">
                <bpmn:extensionElements>
                  <zeebe:calledElement processId="P" propagateAllChildVariables="false" />
                </bpmn:extensionElements>
              </bpmn:callActivity>
              <bpmn:callActivity id="c_both_false">
                <bpmn:extensionElements>
                  <zeebe:calledElement processId="P"
                                       propagateAllParentVariables="false"
                                       propagateAllChildVariables="false" />
                </bpmn:extensionElements>
              </bpmn:callActivity>
              <bpmn:endEvent id="end" />
            </bpmn:process>
          </bpmn:definitions>"#;
        let def = &parse_bpmn(xml).unwrap()[0];
        let flags = |id: &str| match &def.element(id).unwrap().kind {
            ElementKind::CallActivity {
                propagate_all_parent_variables,
                propagate_all_child_variables,
                ..
            } => (
                *propagate_all_parent_variables,
                *propagate_all_child_variables,
            ),
            other => panic!("{id} should be a call activity, got {other:?}"),
        };
        assert_eq!(flags("c_default"), (true, true), "absent ⇒ both true");
        assert_eq!(flags("c_no_parent"), (false, true));
        assert_eq!(flags("c_no_child"), (true, false));
        assert_eq!(flags("c_both_false"), (false, false));
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

    #[test]
    fn parses_zeebe_execution_listeners() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="t">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="work" />
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="start-1" />
          <zeebe:executionListener eventType="start" type="start-2" retries="5" />
          <zeebe:executionListener eventType="end" type="end-1" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
    <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let def = &parse_bpmn(xml).unwrap()[0];
        let el = def.element("t").unwrap();
        assert_eq!(el.start_listeners.len(), 2);
        assert_eq!(el.start_listeners[0].job_type, "start-1");
        assert_eq!(el.start_listeners[0].retries, None);
        assert_eq!(el.start_listeners[1].job_type, "start-2");
        assert_eq!(el.start_listeners[1].retries.as_deref(), Some("5"));
        assert_eq!(el.end_listeners.len(), 1);
        assert_eq!(el.end_listeners[0].job_type, "end-1");
        // A listener-free element carries empty lists.
        assert!(def.element("s").unwrap().start_listeners.is_empty());
        assert!(def.element("s").unwrap().end_listeners.is_empty());
    }

    #[test]
    fn execution_listener_event_type_defaults_to_start() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="t">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="work" />
        <zeebe:executionListeners>
          <zeebe:executionListener type="only" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
    <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let def = &parse_bpmn(xml).unwrap()[0];
        let el = def.element("t").unwrap();
        assert_eq!(el.start_listeners.len(), 1);
        assert!(el.end_listeners.is_empty());
    }

    #[test]
    fn parses_execution_listeners_on_gateways() {
        // #1197: start/end execution listeners on every gateway flavour must
        // attach to the gateway itself, not be dropped.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:exclusiveGateway id="xor">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="xor-start" />
          <zeebe:executionListener eventType="end" type="xor-end" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:exclusiveGateway>
    <bpmn:parallelGateway id="and">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="and-start" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:parallelGateway>
    <bpmn:inclusiveGateway id="or">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="end" type="or-end" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:inclusiveGateway>
    <bpmn:eventBasedGateway id="evt">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener type="evt-start" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:eventBasedGateway>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="xor" />
    <bpmn:sequenceFlow id="f2" sourceRef="xor" targetRef="and" />
    <bpmn:sequenceFlow id="f3" sourceRef="and" targetRef="or" />
    <bpmn:sequenceFlow id="f4" sourceRef="or" targetRef="evt" />
    <bpmn:sequenceFlow id="f5" sourceRef="evt" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let def = &parse_bpmn(xml).unwrap()[0];
        let xor = def.element("xor").unwrap();
        assert_eq!(xor.start_listeners.len(), 1);
        assert_eq!(xor.start_listeners[0].job_type, "xor-start");
        assert_eq!(xor.end_listeners.len(), 1);
        assert_eq!(xor.end_listeners[0].job_type, "xor-end");
        let and = def.element("and").unwrap();
        assert_eq!(and.start_listeners.len(), 1);
        assert_eq!(and.start_listeners[0].job_type, "and-start");
        assert!(and.end_listeners.is_empty());
        let or = def.element("or").unwrap();
        assert!(or.start_listeners.is_empty());
        assert_eq!(or.end_listeners.len(), 1);
        assert_eq!(or.end_listeners[0].job_type, "or-end");
        let evt = def.element("evt").unwrap();
        assert_eq!(evt.start_listeners.len(), 1);
        assert_eq!(evt.start_listeners[0].job_type, "evt-start");
    }

    #[test]
    fn parses_execution_listeners_on_start_event() {
        // #1197: a start event's execution listeners must attach to the start
        // event, not fall through to the enclosing scope.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="s-start" />
          <zeebe:executionListener eventType="end" type="s-end" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let def = &parse_bpmn(xml).unwrap()[0];
        let s = def.element("s").unwrap();
        assert_eq!(s.start_listeners.len(), 1);
        assert_eq!(s.start_listeners[0].job_type, "s-start");
        assert_eq!(s.end_listeners.len(), 1);
        assert_eq!(s.end_listeners[0].job_type, "s-end");
    }

    #[test]
    fn parses_execution_listeners_on_boundary_event() {
        // #1197: a boundary event's execution listeners must attach to the
        // boundary event, not to the activity it is attached to.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="t">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="work" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:boundaryEvent id="b" attachedToRef="t">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="b-start" />
          <zeebe:executionListener eventType="end" type="b-end" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT1M</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="e" />
    <bpmn:endEvent id="eb" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
    <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="b" targetRef="eb" />
  </bpmn:process>
</bpmn:definitions>"#;
        let def = &parse_bpmn(xml).unwrap()[0];
        let b = def.element("b").unwrap();
        assert_eq!(b.start_listeners.len(), 1);
        assert_eq!(b.start_listeners[0].job_type, "b-start");
        assert_eq!(b.end_listeners.len(), 1);
        assert_eq!(b.end_listeners[0].job_type, "b-end");
        // The host activity must NOT have inherited the boundary's listeners.
        let t = def.element("t").unwrap();
        assert!(t.start_listeners.is_empty());
        assert!(t.end_listeners.is_empty());
    }

    #[test]
    fn self_closing_boundary_does_not_capture_next_siblings_listener() {
        // #1197 regression: a self-closing `<boundaryEvent/>` emits no matching
        // `Token::End`, so it must NOT open the pending-boundary buffer — otherwise
        // that stale buffer stays live and swallows the NEXT sibling's
        // `zeebe:executionListener` (mis-attaching it, and letting a following
        // sequence-flow listener bypass its intended rejection). Here a
        // self-closing boundary precedes a listener-bearing service task: the task
        // must own its listener and the task's parse must succeed.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="host">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="host-work" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:boundaryEvent id="b" attachedToRef="host" />
    <bpmn:serviceTask id="next">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="next-work" />
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="next-start" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="host" />
    <bpmn:sequenceFlow id="f2" sourceRef="host" targetRef="next" />
    <bpmn:sequenceFlow id="f3" sourceRef="next" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let def = &parse_bpmn(xml).expect("self-closing boundary must not break parse")[0];
        let next = def.element("next").expect("next task present");
        assert_eq!(
            next.start_listeners.len(),
            1,
            "the sibling task must own its own listener"
        );
        assert_eq!(next.start_listeners[0].job_type, "next-start");
    }

    #[test]
    fn receive_task_execution_listener_attaches_to_itself() {
        // #1197 regression: a `receiveTask` is modelled as a pass-through, but it
        // must push onto the io_stack like the other plain tasks so a
        // `zeebe:executionListener` declared on it lands on the RECEIVE TASK —
        // not fall through to `io_stack.last()` and get dropped at process root
        // or hoisted onto the enclosing sub-process (the silent mis-attachment
        // class this PR closes). Here the listener is declared inside a receive
        // task nested in a sub-process: it must own its listener, and the
        // enclosing sub-process must NOT.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:subProcess id="sub">
      <bpmn:startEvent id="ss" />
      <bpmn:receiveTask id="rt">
        <bpmn:extensionElements>
          <zeebe:executionListeners>
            <zeebe:executionListener eventType="start" type="rt-start" />
            <zeebe:executionListener eventType="end" type="rt-end" />
          </zeebe:executionListeners>
        </bpmn:extensionElements>
      </bpmn:receiveTask>
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="a" sourceRef="ss" targetRef="rt" />
      <bpmn:sequenceFlow id="b" sourceRef="rt" targetRef="se" />
    </bpmn:subProcess>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>"#;
        let def = &parse_bpmn(xml).expect("receive-task listener must parse")[0];
        let rt = def.element("rt").expect("receive task present");
        assert_eq!(
            rt.start_listeners.len(),
            1,
            "start listener on the receive task"
        );
        assert_eq!(rt.start_listeners[0].job_type, "rt-start");
        assert_eq!(
            rt.end_listeners.len(),
            1,
            "end listener on the receive task"
        );
        assert_eq!(rt.end_listeners[0].job_type, "rt-end");
        let sub = def.element("sub").expect("sub-process present");
        assert!(
            sub.start_listeners.is_empty() && sub.end_listeners.is_empty(),
            "the receive task's listeners must NOT hoist onto the enclosing sub-process"
        );
    }

    #[test]
    fn execution_listener_on_adhoc_tool_is_rejected() {
        // #1197 reject-don't-drop: an execution listener on a *tool* of an ad-hoc
        // sub-process cannot fire — a leaf tool is pruned into the non-executable
        // catalog and a retained embedded tool is activated/completed with direct
        // lifecycle events, both bypassing the listener gate. Accepting one would
        // deploy a dead listener, so the deploy is rejected instead.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:adHocSubProcess id="agent">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="agent-worker" />
        <zeebe:adHoc outputCollection="results" outputElement="=result" />
      </bpmn:extensionElements>
      <bpmn:serviceTask id="toolA">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="tool" />
          <zeebe:executionListeners>
            <zeebe:executionListener eventType="start" type="toolA-start" />
          </zeebe:executionListeners>
        </bpmn:extensionElements>
      </bpmn:serviceTask>
    </bpmn:adHocSubProcess>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
    <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        match parse_bpmn(xml) {
            Err(ParseError::UnsupportedExecutionListener { element_id, .. }) => {
                assert_eq!(element_id, "toolA");
            }
            other => {
                panic!("expected UnsupportedExecutionListener for an ad-hoc tool, got {other:?}")
            }
        }
    }

    #[test]
    fn nested_non_activity_listener_does_not_misattach_to_enclosing_subprocess() {
        // #1197 guard against the silent io_stack mis-attachment class: a
        // listener declared on a gateway / start event / boundary event nested
        // inside a sub-process must land on that node, NOT hoist onto the
        // enclosing sub-process (the same defect class as the ioMapping bug in
        // PR #565 / #971).
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="sub">
      <bpmn:startEvent id="ss">
        <bpmn:extensionElements>
          <zeebe:executionListeners>
            <zeebe:executionListener eventType="start" type="ss-start" />
          </zeebe:executionListeners>
        </bpmn:extensionElements>
      </bpmn:startEvent>
      <bpmn:serviceTask id="st" />
      <bpmn:boundaryEvent id="sb" attachedToRef="st">
        <bpmn:extensionElements>
          <zeebe:executionListeners>
            <zeebe:executionListener eventType="start" type="sb-start" />
          </zeebe:executionListeners>
        </bpmn:extensionElements>
        <bpmn:timerEventDefinition><bpmn:timeDuration>PT1M</bpmn:timeDuration></bpmn:timerEventDefinition>
      </bpmn:boundaryEvent>
      <bpmn:exclusiveGateway id="sg">
        <bpmn:extensionElements>
          <zeebe:executionListeners>
            <zeebe:executionListener eventType="start" type="sg-start" />
          </zeebe:executionListeners>
        </bpmn:extensionElements>
      </bpmn:exclusiveGateway>
      <bpmn:endEvent id="se" />
      <bpmn:endEvent id="sbe" />
      <bpmn:sequenceFlow id="sf1" sourceRef="ss" targetRef="st" />
      <bpmn:sequenceFlow id="sf2" sourceRef="st" targetRef="sg" />
      <bpmn:sequenceFlow id="sf3" sourceRef="sg" targetRef="se" />
      <bpmn:sequenceFlow id="sf4" sourceRef="sb" targetRef="sbe" />
    </bpmn:subProcess>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let def = &parse_bpmn(xml).unwrap()[0];
        // The nested start event and gateway carry their own listeners …
        assert_eq!(def.element("ss").unwrap().start_listeners.len(), 1);
        assert_eq!(
            def.element("ss").unwrap().start_listeners[0].job_type,
            "ss-start"
        );
        assert_eq!(def.element("sg").unwrap().start_listeners.len(), 1);
        assert_eq!(
            def.element("sg").unwrap().start_listeners[0].job_type,
            "sg-start"
        );
        // … the nested boundary owns its listener — the specific nested-boundary
        // failure mode of #1197, where `io_stack.last()` is the enclosing
        // sub-process, so a mis-routed boundary listener would land on `sub`
        // rather than the boundary. It must sit on `sb`, and neither its host
        // activity `st` nor the enclosing `sub` may absorb it.
        assert_eq!(def.element("sb").unwrap().start_listeners.len(), 1);
        assert_eq!(
            def.element("sb").unwrap().start_listeners[0].job_type,
            "sb-start"
        );
        assert!(def.element("st").unwrap().start_listeners.is_empty());
        assert!(def.element("st").unwrap().end_listeners.is_empty());
        // … and the enclosing sub-process must NOT have absorbed any of them.
        let sub = def.element("sub").unwrap();
        assert!(
            sub.start_listeners.is_empty(),
            "sub-process must not inherit nested nodes' listeners: {:?}",
            sub.start_listeners
        );
        assert!(sub.end_listeners.is_empty());
    }

    #[test]
    fn self_closing_gateway_with_no_children_balances_io_stack() {
        // A self-closing `<exclusiveGateway/>` emits no end tag; the following
        // activity's ioMapping must still attach to that activity (no io_stack
        // imbalance from the gateway) — guards the #1197 push/pop balance.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:exclusiveGateway id="g" />
    <bpmn:serviceTask id="t">
      <bpmn:extensionElements>
        <zeebe:ioMapping><zeebe:input source="=1" target="one" /></zeebe:ioMapping>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="g" />
    <bpmn:sequenceFlow id="f2" sourceRef="g" targetRef="t" />
    <bpmn:sequenceFlow id="f3" sourceRef="t" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let def = &parse_bpmn(xml).unwrap()[0];
        assert!(def.element("g").unwrap().start_listeners.is_empty());
        let io = &def.element("t").unwrap().io;
        assert_eq!(io.inputs.len(), 1);
        assert_eq!(io.inputs[0].target, "one");
    }

    #[test]
    fn listener_on_parallel_join_is_rejected_at_deploy() {
        // #1197: a multi-incoming parallel gateway is a *join* — its lifecycle
        // short-circuits inside `Engine::activate` and never runs the shared
        // listener-aware activation body, so a listener on it would be parsed but
        // never fire. Reject the deploy rather than silently store a dead listener.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:parallelGateway id="fork" />
    <bpmn:serviceTask id="a" />
    <bpmn:serviceTask id="b" />
    <bpmn:parallelGateway id="join">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="join-start" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:parallelGateway>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="fork" />
    <bpmn:sequenceFlow id="f1" sourceRef="fork" targetRef="a" />
    <bpmn:sequenceFlow id="f2" sourceRef="fork" targetRef="b" />
    <bpmn:sequenceFlow id="f3" sourceRef="a" targetRef="join" />
    <bpmn:sequenceFlow id="f4" sourceRef="b" targetRef="join" />
    <bpmn:sequenceFlow id="f5" sourceRef="join" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let err = parse_bpmn(xml).unwrap_err();
        match err {
            ParseError::UnsupportedExecutionListener { element_id, .. } => {
                assert_eq!(element_id, "join");
            }
            other => panic!("expected UnsupportedExecutionListener, got {other:?}"),
        }
    }

    #[test]
    fn end_listener_on_parallel_join_is_rejected_at_deploy() {
        // #1197: unlike an inclusive join, a parallel join completes without
        // running the end-listener chain (`arrive_at_parallel_join` emits
        // `ElementCompleting` → `ElementCompleted` directly), so even an `end`
        // listener on it can never fire — reject both phases.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:parallelGateway id="fork" />
    <bpmn:serviceTask id="a" />
    <bpmn:serviceTask id="b" />
    <bpmn:parallelGateway id="join">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="end" type="join-end" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:parallelGateway>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="fork" />
    <bpmn:sequenceFlow id="f1" sourceRef="fork" targetRef="a" />
    <bpmn:sequenceFlow id="f2" sourceRef="fork" targetRef="b" />
    <bpmn:sequenceFlow id="f3" sourceRef="a" targetRef="join" />
    <bpmn:sequenceFlow id="f4" sourceRef="b" targetRef="join" />
    <bpmn:sequenceFlow id="f5" sourceRef="join" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let err = parse_bpmn(xml).unwrap_err();
        match err {
            ParseError::UnsupportedExecutionListener { element_id, .. } => {
                assert_eq!(element_id, "join");
            }
            other => panic!("expected UnsupportedExecutionListener, got {other:?}"),
        }
    }

    #[test]
    fn listener_on_inclusive_join_is_rejected_at_deploy() {
        // #1197: a multi-incoming inclusive gateway fires at token quiescence and
        // short-circuits the listener-aware activation body, so its `start`
        // listener can never fire — reject at deploy. (Its `end` listener IS
        // supported and must NOT be rejected — see
        // `end_listener_on_inclusive_join_is_supported`.)
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:inclusiveGateway id="fork" />
    <bpmn:serviceTask id="a" />
    <bpmn:serviceTask id="b" />
    <bpmn:inclusiveGateway id="join">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="join-start" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:inclusiveGateway>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="fork" />
    <bpmn:sequenceFlow id="f1" sourceRef="fork" targetRef="a" />
    <bpmn:sequenceFlow id="f2" sourceRef="fork" targetRef="b" />
    <bpmn:sequenceFlow id="f3" sourceRef="a" targetRef="join" />
    <bpmn:sequenceFlow id="f4" sourceRef="b" targetRef="join" />
    <bpmn:sequenceFlow id="f5" sourceRef="join" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let err = parse_bpmn(xml).unwrap_err();
        match err {
            ParseError::UnsupportedExecutionListener { element_id, .. } => {
                assert_eq!(element_id, "join");
            }
            other => panic!("expected UnsupportedExecutionListener, got {other:?}"),
        }
    }

    #[test]
    fn end_listener_on_inclusive_join_is_supported() {
        // #1197: the inclusive-join quiescence sweep (`fire_ready_inclusive_joins`)
        // DOES defer the join behind its end-listener chain
        // (`begin_end_listener_chain`), so an `end` listener on a multi-incoming
        // inclusive join fires and must be ACCEPTED at deploy — the same model
        // built through `ProcessBuilder` deploys and runs
        // (`end_listener_fires_on_a_multi_incoming_inclusive_join`). Guards against
        // the over-broad rejection that also refused this supported placement.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:inclusiveGateway id="fork" />
    <bpmn:serviceTask id="a" />
    <bpmn:serviceTask id="b" />
    <bpmn:inclusiveGateway id="join">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="end" type="join-end" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:inclusiveGateway>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="fork" />
    <bpmn:sequenceFlow id="f1" sourceRef="fork" targetRef="a">
      <bpmn:conditionExpression>=true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="f2" sourceRef="fork" targetRef="b">
      <bpmn:conditionExpression>=true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="f3" sourceRef="a" targetRef="join" />
    <bpmn:sequenceFlow id="f4" sourceRef="b" targetRef="join" />
    <bpmn:sequenceFlow id="f5" sourceRef="join" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let def = parse_bpmn(xml).expect("inclusive-join end listener must deploy");
        let join = def[0].element("join").expect("join element present");
        assert_eq!(
            join.end_listeners.len(),
            1,
            "the inclusive-join end listener must be stored"
        );
        assert!(
            join.start_listeners.is_empty(),
            "no start listener was declared"
        );
    }

    #[test]
    fn listener_on_single_incoming_gateway_split_is_supported() {
        // A single-incoming parallel/inclusive gateway (a *split*) runs the
        // ordinary activation body, so its listeners DO fire — only the join is
        // rejected. Guard that the join rejection does not over-reach to splits.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:parallelGateway id="fork">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="fork-start" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:parallelGateway>
    <bpmn:endEvent id="a" />
    <bpmn:endEvent id="b" />
    <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="fork" />
    <bpmn:sequenceFlow id="f1" sourceRef="fork" targetRef="a" />
    <bpmn:sequenceFlow id="f2" sourceRef="fork" targetRef="b" />
  </bpmn:process>
</bpmn:definitions>"#;
        let def = &parse_bpmn(xml).unwrap()[0];
        assert_eq!(def.element("fork").unwrap().start_listeners.len(), 1);
        assert_eq!(
            def.element("fork").unwrap().start_listeners[0].job_type,
            "fork-start"
        );
    }

    #[test]
    fn listener_on_compensation_boundary_is_rejected_at_deploy() {
        // #1197: a compensation boundary event is a passive structural marker,
        // armed implicitly when its host completes and never entered by token
        // flow, so it has no lifecycle to hang a listener on. Reject the deploy
        // rather than silently store a listener that can never create a job.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="book" />
    <bpmn:boundaryEvent id="book-comp" attachedToRef="book">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="comp-start" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
      <bpmn:compensateEventDefinition />
    </bpmn:boundaryEvent>
    <bpmn:serviceTask id="undo-book" isForCompensation="true" />
    <bpmn:association associationDirection="One" sourceRef="book-comp" targetRef="undo-book" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="book" />
    <bpmn:sequenceFlow id="f2" sourceRef="book" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let err = parse_bpmn(xml).unwrap_err();
        match err {
            ParseError::UnsupportedExecutionListener { element_id, .. } => {
                assert_eq!(element_id, "book-comp");
            }
            other => panic!("expected UnsupportedExecutionListener, got {other:?}"),
        }
    }

    #[test]
    fn listener_nested_in_sequence_flow_is_rejected_not_hoisted() {
        // #1198 deferral guard: a sequence flow is an edge, not an `Element`, so
        // a `zeebe:executionListener` nested inside a `<sequenceFlow>` has no
        // lifecycle to run on. It must be REJECTED at deploy — the same
        // dead-listener failure mode this change rejects for joins and
        // compensation boundaries — NOT silently dropped (which would let a
        // declared take listener deploy yet never fire) and NOT fall through to
        // `io_stack.last()` and hoist onto the enclosing sub-process (the #1197
        // mis-attachment class). Here the flow sits inside `sub`.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="sub">
      <bpmn:startEvent id="ss" />
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="sf1" sourceRef="ss" targetRef="se">
        <bpmn:extensionElements>
          <zeebe:executionListeners>
            <zeebe:executionListener eventType="start" type="take-listener" />
          </zeebe:executionListeners>
        </bpmn:extensionElements>
      </bpmn:sequenceFlow>
    </bpmn:subProcess>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let err = parse_bpmn(xml).expect_err("sequence-flow listener must be rejected at deploy");
        match err {
            ParseError::UnsupportedExecutionListener { element_id, .. } => {
                assert_eq!(
                    element_id, "sf1",
                    "the rejection must name the offending sequence flow"
                );
            }
            other => panic!("expected UnsupportedExecutionListener, got {other:?}"),
        }
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

    #[test]
    fn should_parse_an_ai_agent_task_service_task() {
        // A `serviceTask` bearing a `zeebe:agentDefinition agentType="aiAgentTask"`
        // marker classifies the ordinary job-based service task.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="agent-proc" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="agent">
                <bpmn:extensionElements>
                  <zeebe:agentDefinition agentType="aiAgentTask" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
              <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        let kind = &def.element("agent").unwrap().kind;
        match kind {
            crate::model::ElementKind::ServiceTask { agent_type, .. } => {
                assert_eq!(*agent_type, Some(crate::agent::AgentType::AiAgentTask));
            }
            other => panic!("expected marked ServiceTask, got {other:?}"),
        }
    }

    #[test]
    fn should_reject_ai_agent_task_on_a_non_service_task() {
        // Placement rule (Camunda AgentDefinitionValidator): `aiAgentTask` is only
        // valid on a `serviceTask`. On an `adHocSubProcess` it must be rejected.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="bad-agent" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:adHocSubProcess id="agent">
                <bpmn:extensionElements>
                  <zeebe:agentDefinition agentType="aiAgentTask" />
                </bpmn:extensionElements>
                <bpmn:task id="inner" />
              </bpmn:adHocSubProcess>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
              <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let err = parse_bpmn(xml).unwrap_err();
        assert!(
            matches!(err, ParseError::InvalidAgentDefinition { ref element_id, .. } if element_id == "agent"),
            "expected InvalidAgentDefinition, got {err:?}"
        );
    }

    #[test]
    fn should_reject_ai_agent_subprocess_on_a_service_task() {
        // Placement rule: `aiAgentSubProcess` is only valid on an
        // `adHocSubProcess`. On a plain `serviceTask` it must be rejected.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="bad-agent2" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="agent">
                <bpmn:extensionElements>
                  <zeebe:agentDefinition agentType="aiAgentSubProcess" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
              <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let err = parse_bpmn(xml).unwrap_err();
        assert!(
            matches!(err, ParseError::InvalidAgentDefinition { ref element_id, .. } if element_id == "agent"),
            "expected InvalidAgentDefinition, got {err:?}"
        );
    }

    #[test]
    fn should_accept_ai_agent_subprocess_on_an_ad_hoc_sub_process() {
        // The valid placement: `agentType="aiAgentSubProcess"` on a
        // `bpmn:adHocSubProcess`. The ad-hoc variant reuses the existing
        // ad-hoc container machinery — so the container parses into a
        // single job-bearing `ServiceTask` at the parent token-flow level, and
        // its contained "tool" activities are pruned from the executable graph
        // (invoked out-of-band by the worker, not by token flow).
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="agent-adhoc" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:adHocSubProcess id="agent">
                <bpmn:extensionElements>
                  <zeebe:agentDefinition agentType="aiAgentSubProcess" />
                </bpmn:extensionElements>
                <bpmn:serviceTask id="tool" />
              </bpmn:adHocSubProcess>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
              <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];
        // The ad-hoc container is retained as a single job-bearing ServiceTask,
        // NOT an engine-native AgentTask.
        let kind = &def.element("agent").unwrap().kind;
        assert!(
            matches!(
                kind,
                crate::model::ElementKind::ServiceTask {
                    agent_type: Some(crate::agent::AgentType::AiAgentSubProcess),
                    ..
                }
            ),
            "expected the ad-hoc agent container to be a ServiceTask, got {kind:?}"
        );
        // The contained tool activity is pruned from the executable graph.
        assert!(
            def.element("tool").is_none(),
            "expected the ad-hoc tool `tool` to be pruned from the executable graph"
        );
    }

    #[test]
    fn should_reject_an_unknown_agent_type() {
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="bad-agent3" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="agent">
                <bpmn:extensionElements>
                  <zeebe:agentDefinition agentType="wat" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
              <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let err = parse_bpmn(xml).unwrap_err();
        assert!(
            matches!(err, ParseError::InvalidAgentDefinition { ref reason, .. } if reason.contains("unknown agentType 'wat'")),
            "expected InvalidAgentDefinition naming the unknown value, got {err:?}"
        );
    }

    #[test]
    fn should_reject_a_missing_agent_type_with_a_clear_reason() {
        // An empty/absent `agentType` must not be reported as `unknown agentType ''`;
        // the reason should say the attribute is missing.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="bad-agent4" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="agent">
                <bpmn:extensionElements>
                  <zeebe:agentDefinition />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
              <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let err = parse_bpmn(xml).unwrap_err();
        assert!(
            matches!(err, ParseError::InvalidAgentDefinition { ref reason, .. } if reason.contains("missing agentType attribute")),
            "expected InvalidAgentDefinition citing a missing attribute, got {err:?}"
        );
    }

    #[test]
    fn misplaced_agent_definition_names_the_enclosing_flow_node() {
        // A `zeebe:agentDefinition` on neither a serviceTask nor an adHocSubProcess
        // (here a userTask) is rejected. The error must attribute the fault to the
        // nearest enclosing flow node so it is diagnosable, not an empty id.
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="bad-agent4" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:agentDefinition agentType="aiAgentTask" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="f2" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let err = parse_bpmn(xml).unwrap_err();
        assert!(
            matches!(err, ParseError::InvalidAgentDefinition { ref element_id, .. } if element_id == "review"),
            "expected InvalidAgentDefinition attributed to 'review', got {err:?}"
        );
    }
}
