//! **BPMN model analysis** — a structural lens on a process *definition*, independent
//! of runtime traces.
//!
//! The cockpit droid reasons about *what happened* from the trace tables (see
//! [`crate::analysis`]). This module is the complementary *what is modelled* surface: it
//! distils a `model.bpmn` into a compact structural graph ([`read_model`]) and runs a
//! battery of deterministic static checks ([`analyze_model`]) — unreachable nodes, missing
//! ends, gateway split/join hazards, unguarded service tasks, rework loops.
//!
//! Crucially the node `id`s and service-task `jobType`s reported here are the **same keys**
//! the trace tables carry (`jobs.element_id` / `jobs.job_type`, `incidents.element_id`), so
//! a persona can join structure to runtime: find a structural risk here, then measure how
//! often it bites with `query_traces`.
//!
//! Both functions take raw BPMN XML and parse it with the engine's own
//! [`parse_bpmn`](nanobpmn_engine_core::bpmn::parse_bpmn) — exactly the parser production
//! uses — so the structural view never drifts from executable semantics.

use std::collections::{BTreeMap, HashMap, HashSet};

use nanobpmn_engine_core::bpmn::parse_bpmn;
use nanobpmn_engine_core::{Condition, Element, ElementKind, ProcessDefinition, SequenceFlow};
use serde_json::{json, Value};

/// Parse `xml` and return its first process definition, or an error message.
fn first_def(xml: &str) -> Result<(ProcessDefinition, usize), String> {
    let defs = parse_bpmn(xml).map_err(|e| format!("model failed to parse: {e:?}"))?;
    let count = defs.len();
    let def = defs
        .into_iter()
        .next()
        .ok_or_else(|| "model contained no process definitions".to_string())?;
    Ok((def, count))
}

/// The short label and structural extras for an element kind.
fn kind_label(kind: &ElementKind) -> &'static str {
    match kind {
        ElementKind::StartEvent => "startEvent",
        ElementKind::EndEvent => "endEvent",
        ElementKind::ServiceTask { .. } => "serviceTask",
        ElementKind::UserTask(_) => "userTask",
        ElementKind::ExclusiveGateway => "exclusiveGateway",
        ElementKind::ParallelGateway => "parallelGateway",
        ElementKind::ErrorBoundaryEvent { .. } => "errorBoundaryEvent",
        ElementKind::TimerIntermediateCatchEvent { .. } => "timerIntermediateCatchEvent",
        ElementKind::TimerBoundaryEvent { .. } => "timerBoundaryEvent",
        ElementKind::MessageIntermediateCatchEvent { .. } => "messageIntermediateCatchEvent",
        ElementKind::MessageBoundaryEvent { .. } => "messageBoundaryEvent",
        ElementKind::MessageStartEvent { .. } => "messageStartEvent",
        ElementKind::TimerStartEvent { .. } => "timerStartEvent",
        ElementKind::SubProcess { .. } => "subProcess",
    }
}

/// The activity a boundary event is attached to, if this kind is a boundary event.
fn attached_to(kind: &ElementKind) -> Option<&str> {
    match kind {
        ElementKind::ErrorBoundaryEvent { attached_to, .. }
        | ElementKind::TimerBoundaryEvent { attached_to, .. }
        | ElementKind::MessageBoundaryEvent { attached_to, .. } => Some(attached_to.as_str()),
        _ => None,
    }
}

fn is_boundary(kind: &ElementKind) -> bool {
    attached_to(kind).is_some()
}

fn is_gateway(kind: &ElementKind) -> bool {
    matches!(
        kind,
        ElementKind::ExclusiveGateway | ElementKind::ParallelGateway
    )
}

/// A node that produces a `jobs` row at runtime (one row per executed service/user task).
/// These are the only model nodes observable in the trace tables, so conformance checking
/// works at the granularity of task-to-task transitions.
fn is_task(kind: &ElementKind) -> bool {
    matches!(
        kind,
        ElementKind::ServiceTask { .. } | ElementKind::UserTask(_)
    )
}

/// Kind-specific extra attributes for the structural view (job type, attachment, etc.).
fn kind_extras(kind: &ElementKind) -> serde_json::Map<String, Value> {
    let mut m = serde_json::Map::new();
    match kind {
        ElementKind::ServiceTask { job_type, priority } => {
            m.insert("jobType".into(), json!(job_type));
            if let Some(p) = priority {
                m.insert("priority".into(), json!(p));
            }
        }
        ElementKind::ErrorBoundaryEvent {
            attached_to,
            error_code,
        } => {
            m.insert("attachedTo".into(), json!(attached_to));
            m.insert("errorCode".into(), json!(error_code));
        }
        ElementKind::TimerIntermediateCatchEvent { duration_millis } => {
            m.insert("durationMs".into(), json!(duration_millis));
        }
        ElementKind::TimerBoundaryEvent {
            attached_to,
            duration_millis,
            interrupting,
            repeating,
        } => {
            m.insert("attachedTo".into(), json!(attached_to));
            m.insert("durationMs".into(), json!(duration_millis));
            m.insert("interrupting".into(), json!(interrupting));
            m.insert("repeating".into(), json!(repeating));
        }
        ElementKind::MessageIntermediateCatchEvent {
            message_name,
            correlation_key,
        } => {
            m.insert("messageName".into(), json!(message_name));
            m.insert("correlationKey".into(), json!(correlation_key));
        }
        ElementKind::MessageBoundaryEvent {
            attached_to,
            message_name,
            correlation_key,
            interrupting,
        } => {
            m.insert("attachedTo".into(), json!(attached_to));
            m.insert("messageName".into(), json!(message_name));
            m.insert("correlationKey".into(), json!(correlation_key));
            m.insert("interrupting".into(), json!(interrupting));
        }
        ElementKind::MessageStartEvent { message_name } => {
            m.insert("messageName".into(), json!(message_name));
        }
        ElementKind::TimerStartEvent {
            interval_millis,
            repeating,
        } => {
            m.insert("intervalMs".into(), json!(interval_millis));
            m.insert("repeating".into(), json!(repeating));
        }
        ElementKind::SubProcess { start_event } => {
            m.insert("innerStartEvent".into(), json!(start_event));
        }
        _ => {}
    }
    m
}

/// Structural edges used for reachability and loop detection: outgoing sequence flows,
/// sub-process entry (container -> inner start), and boundary attachment (activity ->
/// boundary event). The returned map is keyed by element id.
fn adjacency(def: &ProcessDefinition) -> HashMap<String, Vec<String>> {
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();
    for (id, el) in &def.elements {
        let entry = adj.entry(id.clone()).or_default();
        for flow in &el.outgoing {
            entry.push(flow.to.clone());
        }
        if let ElementKind::SubProcess { start_event } = &el.kind {
            entry.push(start_event.clone());
        }
    }
    // Boundary events have no incoming flow; they become reachable through their host.
    for (id, el) in &def.elements {
        if let Some(host) = attached_to(&el.kind) {
            adj.entry(host.to_string()).or_default().push(id.clone());
        }
    }
    adj
}

/// The set of element ids reachable from the start event over [`adjacency`].
fn reachable_from_start(
    def: &ProcessDefinition,
    adj: &HashMap<String, Vec<String>>,
) -> HashSet<String> {
    let mut seen = HashSet::new();
    let mut stack = vec![def.start_event.clone()];
    while let Some(id) = stack.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        if let Some(next) = adj.get(&id) {
            for n in next {
                if !seen.contains(n) {
                    stack.push(n.clone());
                }
            }
        }
    }
    seen
}

/// Reverse adjacency (predecessors) for ancestor queries.
fn predecessors(adj: &HashMap<String, Vec<String>>) -> HashMap<String, Vec<String>> {
    let mut rev: HashMap<String, Vec<String>> = HashMap::new();
    for (from, tos) in adj {
        for to in tos {
            rev.entry(to.clone()).or_default().push(from.clone());
        }
    }
    rev
}

/// All ancestors of `node` over the reverse graph (nodes that can reach it).
fn ancestors(node: &str, rev: &HashMap<String, Vec<String>>) -> HashSet<String> {
    let mut seen = HashSet::new();
    let mut stack: Vec<String> = rev.get(node).cloned().unwrap_or_default();
    while let Some(id) = stack.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        if let Some(preds) = rev.get(&id) {
            for p in preds {
                if !seen.contains(p) {
                    stack.push(p.clone());
                }
            }
        }
    }
    seen
}

/// Tarjan strongly-connected components over the structural graph. Returns the components
/// that constitute a loop (size > 1, or a single self-looping node).
fn loop_components(
    def: &ProcessDefinition,
    adj: &HashMap<String, Vec<String>>,
) -> Vec<Vec<String>> {
    // Index the nodes deterministically.
    let ids: Vec<String> = {
        let mut v: Vec<String> = def.elements.keys().cloned().collect();
        v.sort();
        v
    };
    let index_of: HashMap<&str, usize> = ids
        .iter()
        .enumerate()
        .map(|(i, s)| (s.as_str(), i))
        .collect();
    let n = ids.len();
    let neighbours: Vec<Vec<usize>> = ids
        .iter()
        .map(|id| {
            adj.get(id)
                .map(|tos| {
                    tos.iter()
                        .filter_map(|t| index_of.get(t.as_str()).copied())
                        .collect()
                })
                .unwrap_or_default()
        })
        .collect();

    #[derive(Default)]
    struct Tarjan {
        idx: usize,
        indices: Vec<Option<usize>>,
        low: Vec<usize>,
        on_stack: Vec<bool>,
        stack: Vec<usize>,
        sccs: Vec<Vec<usize>>,
    }
    impl Tarjan {
        fn strong(&mut self, v: usize, neighbours: &[Vec<usize>]) {
            self.indices[v] = Some(self.idx);
            self.low[v] = self.idx;
            self.idx += 1;
            self.stack.push(v);
            self.on_stack[v] = true;
            for &w in &neighbours[v] {
                if self.indices[w].is_none() {
                    self.strong(w, neighbours);
                    self.low[v] = self.low[v].min(self.low[w]);
                } else if self.on_stack[w] {
                    self.low[v] = self.low[v].min(self.indices[w].unwrap());
                }
            }
            if self.low[v] == self.indices[v].unwrap() {
                let mut comp = Vec::new();
                while let Some(w) = self.stack.pop() {
                    self.on_stack[w] = false;
                    comp.push(w);
                    if w == v {
                        break;
                    }
                }
                self.sccs.push(comp);
            }
        }
    }

    let mut t = Tarjan {
        indices: vec![None; n],
        low: vec![0; n],
        on_stack: vec![false; n],
        ..Default::default()
    };
    for v in 0..n {
        if t.indices[v].is_none() {
            t.strong(v, &neighbours);
        }
    }

    let mut loops = Vec::new();
    for comp in t.sccs {
        let self_loop = comp.len() == 1 && neighbours[comp[0]].contains(&comp[0]);
        if comp.len() > 1 || self_loop {
            let mut names: Vec<String> = comp.iter().map(|&i| ids[i].clone()).collect();
            names.sort();
            loops.push(names);
        }
    }
    loops
}

/// The model projected onto its *observable* nodes (service/user tasks). This is the
/// reference against which mined trace behaviour is checked for conformance: the trace only
/// records task executions, so the model's allowed behaviour is expressed as task-to-task
/// transitions, collapsing the gateways and events that sit between tasks.
pub(crate) struct ModelTaskGraph {
    pub process_id: String,
    /// Every service/user task id in the model.
    pub tasks: HashSet<String>,
    /// For each task, the set of tasks reachable next via paths of only non-task nodes
    /// (gateways, events, subprocess/boundary edges) — i.e. the transitions the model permits.
    pub allowed: HashMap<String, HashSet<String>>,
    /// Tasks the model can reach first from the start event (legal opening tasks).
    pub start_tasks: HashSet<String>,
    /// Tasks from which an end event is reachable without an intervening task (legal closing tasks).
    pub end_tasks: HashSet<String>,
}

/// Build the [`ModelTaskGraph`] by collapsing the structural graph onto its task nodes.
pub(crate) fn model_task_graph(xml: &str) -> Result<ModelTaskGraph, String> {
    let (def, _) = first_def(xml)?;
    let adj = adjacency(&def);
    let is_task_id = |id: &str| def.element(id).map(|e| is_task(&e.kind)).unwrap_or(false);
    let is_end_id = |id: &str| {
        matches!(
            def.element(id).map(|e| &e.kind),
            Some(ElementKind::EndEvent)
        )
    };
    let tasks: HashSet<String> = def
        .elements
        .iter()
        .filter(|(_, e)| is_task(&e.kind))
        .map(|(id, _)| id.clone())
        .collect();

    // From a starting frontier, walk forward through only non-task nodes, recording the
    // first task reached on each path (and whether any path reaches an end event).
    let next_tasks = |frontier: Vec<String>| -> (HashSet<String>, bool) {
        let mut found = HashSet::new();
        let mut reached_end = false;
        let mut visited = HashSet::new();
        let mut stack = frontier;
        while let Some(n) = stack.pop() {
            if !visited.insert(n.clone()) {
                continue;
            }
            if is_end_id(&n) {
                reached_end = true;
                continue;
            }
            if is_task_id(&n) {
                found.insert(n.clone()); // stop: don't expand past a task
                continue;
            }
            if let Some(nexts) = adj.get(&n) {
                for s in nexts {
                    stack.push(s.clone());
                }
            }
        }
        (found, reached_end)
    };

    let mut allowed = HashMap::new();
    let mut end_tasks = HashSet::new();
    for t in &tasks {
        let frontier = adj.get(t).cloned().unwrap_or_default();
        let (succ, reached_end) = next_tasks(frontier);
        if reached_end {
            end_tasks.insert(t.clone());
        }
        allowed.insert(t.clone(), succ);
    }
    let (start_tasks, _) = next_tasks(vec![def.start_event.clone()]);

    Ok(ModelTaskGraph {
        process_id: def.id.clone(),
        tasks,
        allowed,
        start_tasks,
        end_tasks,
    })
}

/// `read_model` — a compact, deterministic structural view of the process: the start
/// event, per-kind counts, and every node with its kind, key attributes (service-task
/// `jobType`, boundary attachment, timer/message details), incoming count, outgoing
/// targets (flagged conditional), reachability, and gateway split/join role.
pub fn read_model(xml: &str) -> Result<Value, String> {
    let (def, def_count) = first_def(xml)?;
    let adj = adjacency(&def);
    let reachable = reachable_from_start(&def, &adj);

    let mut ids: Vec<&String> = def.elements.keys().collect();
    ids.sort();

    let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut nodes = Vec::with_capacity(ids.len());
    for id in &ids {
        let el: &Element = &def.elements[*id];
        let label = kind_label(&el.kind);
        *counts.entry(label).or_insert(0) += 1;
        let incoming = def.incoming_count(id);
        let outgoing: Vec<Value> = el
            .outgoing
            .iter()
            .map(|f| json!({ "to": f.to, "conditional": f.condition.is_some() }))
            .collect();
        let mut node = json!({
            "id": id,
            "kind": label,
            "incoming": incoming,
            "outgoing": outgoing,
            "reachable": reachable.contains(*id),
        });
        let obj = node.as_object_mut().unwrap();
        if is_gateway(&el.kind) {
            let role = match (incoming > 1, el.outgoing.len() > 1) {
                (true, true) => "join+split",
                (true, false) => "join",
                (false, true) => "split",
                (false, false) => "pass",
            };
            obj.insert("gatewayRole".into(), json!(role));
        }
        for (k, v) in kind_extras(&el.kind) {
            obj.insert(k, v);
        }
        nodes.push(node);
    }

    Ok(json!({
        "processId": def.id,
        "startEvent": def.start_event,
        "definitionCount": def_count,
        "counts": counts,
        "nodes": nodes,
        "note": "Node ids and serviceTask jobTypes are the same keys the trace tables use \
                 (jobs.element_id / jobs.job_type, incidents.element_id) — join structure to \
                 runtime with query_traces.",
    }))
}

fn finding(severity: &str, code: &str, element: Option<&str>, message: String) -> Value {
    json!({
        "severity": severity,
        "code": code,
        "element": element,
        "message": message,
    })
}

/// `analyze_model` — deterministic static checks over the model structure. Findings are
/// **advisory** (heuristic where noted) and reference element ids that join to the trace
/// data, so the persona can quantify each structural risk against runtime.
pub fn analyze_model(xml: &str) -> Result<Value, String> {
    let (def, _) = first_def(xml)?;
    let adj = adjacency(&def);
    let rev = predecessors(&adj);
    let reachable = reachable_from_start(&def, &adj);
    let loops = loop_components(&def, &adj);

    let mut ids: Vec<&String> = def.elements.keys().collect();
    ids.sort();

    let mut findings = Vec::new();

    // No explicit end event.
    let end_events = ids
        .iter()
        .filter(|id| matches!(def.elements[**id].kind, ElementKind::EndEvent))
        .count();
    if end_events == 0 {
        findings.push(finding(
            "warn",
            "no-end-event",
            None,
            "The process has no end event; instances complete implicitly when their last \
             token is consumed, which is easy to misread."
                .into(),
        ));
    }

    for id in &ids {
        let el = &def.elements[*id];
        let kind = &el.kind;

        // Unreachable from the start event (start event itself always counts as reached).
        if !reachable.contains(*id) {
            findings.push(finding(
                "warn",
                "unreachable-node",
                Some(id),
                format!(
                    "'{id}' ({}) is not reachable from the start event — it can never execute.",
                    kind_label(kind)
                ),
            ));
        }

        // A non-end flow node with no outgoing flow silently drops its token.
        let is_event_end = matches!(kind, ElementKind::EndEvent);
        if !is_event_end && el.outgoing.is_empty() && !is_boundary(kind) {
            // Sub-process inner ends and the like aside, a task/gateway with no exit is a
            // dead end.
            if !matches!(kind, ElementKind::SubProcess { .. }) {
                findings.push(finding(
                    "warn",
                    "dead-end",
                    Some(id),
                    format!(
                        "'{id}' ({}) has no outgoing flow and is not an end event — its token \
                         vanishes (implicit end).",
                        kind_label(kind)
                    ),
                ));
            }
        }

        match kind {
            // Exclusive split with every branch guarded: if no condition matches and there
            // is no default flow, the token has nowhere to go.
            ElementKind::ExclusiveGateway if el.outgoing.len() > 1 => {
                let has_default = el.outgoing.iter().any(|f| f.condition.is_none());
                if !has_default {
                    findings.push(finding(
                        "warn",
                        "exclusive-no-default",
                        Some(id),
                        format!(
                            "Exclusive gateway '{id}' splits {} ways but every flow is \
                             conditional (no default) — if no condition holds the token gets \
                             stuck.",
                            el.outgoing.len()
                        ),
                    ));
                }
            }
            // Service task with no error/timeout boundary: a failing or hung job raises an
            // incident with no modelled recovery path.
            ElementKind::ServiceTask { job_type, .. } => {
                let guarded = def.elements.values().any(|b| {
                    attached_to(&b.kind) == Some(id.as_str())
                        && matches!(
                            b.kind,
                            ElementKind::ErrorBoundaryEvent { .. }
                                | ElementKind::TimerBoundaryEvent { .. }
                        )
                });
                if !guarded {
                    findings.push(finding(
                        "info",
                        "service-task-unguarded",
                        Some(id),
                        format!(
                            "Service task '{id}' (job type '{job_type}') has no error or timer \
                             boundary event — a failing or stuck job has no modelled recovery; \
                             check incidents for this element in the traces."
                        ),
                    ));
                }
            }
            _ => {}
        }

        // Join hazards (heuristic, via ancestry).
        if matches!(kind, ElementKind::ParallelGateway) && def.incoming_count(id) > 1 {
            let anc = ancestors(id, &rev);
            let exclusive_split_upstream = anc.iter().any(|a| {
                def.elements
                    .get(a)
                    .map(|e| {
                        matches!(e.kind, ElementKind::ExclusiveGateway) && e.outgoing.len() > 1
                    })
                    .unwrap_or(false)
            });
            if exclusive_split_upstream {
                findings.push(finding(
                    "info",
                    "parallel-join-may-deadlock",
                    Some(id),
                    format!(
                        "Parallel (AND) join '{id}' waits for all incoming branches, but an \
                         exclusive split upstream may activate only some of them — risk of a \
                         token waiting forever. Verify branch arrival in the traces."
                    ),
                ));
            }
        }
        if matches!(kind, ElementKind::ExclusiveGateway) && def.incoming_count(id) > 1 {
            let anc = ancestors(id, &rev);
            let parallel_split_upstream = anc.iter().any(|a| {
                def.elements
                    .get(a)
                    .map(|e| matches!(e.kind, ElementKind::ParallelGateway) && e.outgoing.len() > 1)
                    .unwrap_or(false)
            });
            if parallel_split_upstream {
                findings.push(finding(
                    "info",
                    "exclusive-join-of-parallel",
                    Some(id),
                    format!(
                        "Exclusive (XOR) join '{id}' merges branches a parallel split fanned \
                         out — it does NOT synchronise, so each arriving token passes through \
                         and the downstream may run more than once."
                    ),
                ));
            }
        }
    }

    // Rework / retry loops.
    for comp in &loops {
        findings.push(finding(
            "info",
            "rework-loop",
            comp.first().map(|s| s.as_str()),
            format!(
                "Elements {comp:?} form a loop (rework/retry cycle). Measure how often it \
                 re-executes per instance in the traces — a hot loop inflates latency and cost."
            ),
        ));
    }

    // Severity tallies for a quick read.
    let warns = findings.iter().filter(|f| f["severity"] == "warn").count();
    let infos = findings.iter().filter(|f| f["severity"] == "info").count();

    Ok(json!({
        "processId": def.id,
        "metrics": {
            "nodes": def.elements.len(),
            "endEvents": end_events,
            "loops": loops.len(),
        },
        "findingCount": findings.len(),
        "warnings": warns,
        "infos": infos,
        "findings": findings,
        "note": "Findings are advisory; some join hazards are heuristic. Element ids join to \
                 the trace tables — quantify each risk with query_traces before advising.",
    }))
}

/// Does this XML carry a `<…:definitions>` root element? LLM-authored variants are sometimes
/// a bare `<bpmn:process>` fragment (xmlns decls hoisted onto the process) with no definitions
/// wrapper — the engine parser and bpmn-js both reject those.
fn has_definitions_root(xml: &str) -> bool {
    for part in xml.split('<') {
        let name = part
            .trim_start_matches('/')
            .trim_start()
            .split(|c: char| c.is_whitespace() || c == '>' || c == '/')
            .next()
            .unwrap_or("");
        let local = name.rsplit(':').next().unwrap_or(name);
        if local.eq_ignore_ascii_case("definitions") {
            return true;
        }
    }
    false
}

/// Wrap a bare `<…:process>` fragment in a synthesized `<bpmn:definitions>` envelope so it can be
/// parsed and analysed. Returns `(xml, wrapped)`; a document that already has a definitions root is
/// returned unchanged. Mirrors the cockpit's `ensureBpmnDefinitions` render-side normalization.
fn ensure_definitions(xml: &str) -> (String, bool) {
    let trimmed = xml.trim();
    if trimmed.is_empty() || has_definitions_root(trimmed) {
        return (xml.to_string(), false);
    }
    let body = match trimmed.strip_prefix("<?xml") {
        Some(rest) => rest
            .find("?>")
            .map(|i| rest[i + 2..].trim_start())
            .unwrap_or(trimmed),
        None => trimmed,
    };
    let wrapped = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <bpmn:definitions xmlns:bpmn=\"http://www.omg.org/spec/BPMN/20100524/MODEL\" \
         xmlns:bpmndi=\"http://www.omg.org/spec/BPMN/20100524/DI\" \
         xmlns:dc=\"http://www.omg.org/spec/DD/20100524/DC\" \
         xmlns:di=\"http://www.omg.org/spec/DD/20100524/DI\" \
         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \
         xmlns:zeebe=\"http://camunda.org/schema/zeebe\" \
         targetNamespace=\"http://bpmn.io/schema/bpmn\">\n{body}\n</bpmn:definitions>"
    );
    (wrapped, true)
}

/// Map a known BPMN deploy/parse error to a concrete, actionable fix, so the model stops repeating
/// the same authoring mistake. Shared by `validate_model` and the experiment replay scorecard.
pub fn deploy_fix_hint(err: &str) -> Option<String> {
    if err.contains("InvalidBoundaryEvent") && err.contains("unknown error") {
        return Some(
            "The error boundary event has an empty or unknown errorRef. A BPMN error \
             boundary needs BOTH a top-level `<bpmn:error id=\"E_X\" errorCode=\"...\"/>` \
             definition AND `<bpmn:errorEventDefinition errorRef=\"E_X\"/>` on the boundary \
             event referencing that id (errorRef points at the error's `id`, not its \
             `errorCode`). To model a RETRY, prefer a timer boundary event \
             (interrupting=false) that loops back to the task, or reuse the model's existing \
             error definition — do not leave errorRef empty."
                .into(),
        );
    }
    // A sequence flow points at a node id the process never declares. The most common cause is an
    // error boundary authored as `<bpmn:errorBoundaryEvent>` (NOT a real BPMN element) — the engine
    // never creates the node, so a flow whose sourceRef/targetRef names it dangles.
    if err.contains("unknown source element") || err.contains("unknown target element") {
        return Some(
            "A sequence flow references a node id that the process never declares. Most common \
             cause: an error boundary written as `<bpmn:errorBoundaryEvent …>` — that element does \
             NOT exist in BPMN, so the node is never created and the flow referencing it dangles. \
             Use `<bpmn:boundaryEvent id=\"X\" attachedToRef=\"Task\"><bpmn:errorEventDefinition \
             errorRef=\"E\"/></bpmn:boundaryEvent>`. Otherwise check for a typo: every sequenceFlow \
             sourceRef/targetRef must match a declared node id exactly."
                .into(),
        );
    }
    None
}

/// Auto-heal the highest-frequency LLM BPMN authoring mistake **before** the model is parsed, so
/// the experimenter iterates on process *semantics* instead of thrashing on syntax. The runtime
/// error is pulled forward (and fixed) at the authoring boundary, IDE-red-squiggle style.
///
/// `<…:errorBoundaryEvent>` is not a real BPMN element — the engine models an error boundary as a
/// `boundaryEvent` carrying a nested `errorEventDefinition`. When a model uses it, the boundary node
/// is never created and every sequence flow referencing it fails with "unknown source element". We
/// rename it in place and report what changed. (`errorBoundaryEvent` never legitimately occurs as a
/// substring, so a plain rename is safe.) Returns `(healed_xml, notes)`; `notes` is empty when
/// nothing was touched.
pub fn normalize_authoring(xml: &str) -> (String, Vec<String>) {
    let mut notes = Vec::new();
    let mut out = xml.to_string();
    if out.contains("errorBoundaryEvent") {
        out = out.replace("errorBoundaryEvent", "boundaryEvent");
        notes.push(
            "Renamed <errorBoundaryEvent> to <boundaryEvent>: errorBoundaryEvent is not a valid \
             BPMN element. An error boundary is a <bpmn:boundaryEvent attachedToRef=\"Task\"> with \
             a nested <bpmn:errorEventDefinition errorRef=\"…\"/>."
                .to_string(),
        );
    }
    (out, notes)
}

/// Lint for the silent `zeebe:taskDefinition` ATTRIBUTE mistake. Written as an attribute on a
/// serviceTask (`<…:serviceTask … zeebe:taskDefinition="x">`), the engine **ignores it** and the
/// job type silently defaults to the task **id** — so the variant's worker never binds to the
/// recorded job types (it surfaces as an uncovered / brand-new worker, derailing the scorecard).
/// The engine only reads the *child element* form `<zeebe:taskDefinition type="x"/>`. The element
/// form writes `taskDefinition ` followed by ` type=`, so a literal `taskDefinition=` (no space)
/// uniquely identifies the attribute misuse. Returns one advisory finding when present.
pub(crate) fn lint_task_definition_attribute(xml: &str) -> Vec<Value> {
    let count = xml.matches("taskDefinition=").count();
    if count == 0 {
        return Vec::new();
    }
    vec![finding(
        "warn",
        "task-definition-as-attribute",
        None,
        format!(
            "{count} service task(s) declare zeebe:taskDefinition as an ATTRIBUTE \
             (zeebe:taskDefinition=\"…\"). The engine IGNORES this — the job type silently \
             defaults to the task id, so the worker will not bind to recorded job types (it shows \
             up as uncovered / a new worker). Move it to the child element form: \
             <bpmn:extensionElements><zeebe:taskDefinition type=\"your-job-type\"/></bpmn:extensionElements>."
        ),
    )]
}

/// Explain the **"the simulate engine uses the element id as the job type"** symptom.
///
/// When a candidate's serviceTask carries no parseable `zeebe:taskDefinition` *child*
/// element, the engine has nothing to bind and the job type silently defaults to the
/// task **id**. Simulate then reports that id under `uncoveredJobTypes` /
/// `requiresNewWorkers`, which reads like an engine bug ("it matched on element_id!")
/// — but it is the taskDefinition-binding mistake surfacing one step downstream.
///
/// Given the (healed) candidate XML and the job types simulate flagged as uncovered,
/// this returns an actionable hint for every uncovered job type that exactly equals a
/// serviceTask id in the model — pulling the runtime confusion forward into a crisp,
/// fixable signal. Returns an empty vec on a parse failure or when no uncovered job
/// type matches a task id (the normal, correctly-bound case).
pub(crate) fn job_type_binding_hints(xml: &str, uncovered: &[String]) -> Vec<Value> {
    if uncovered.is_empty() {
        return Vec::new();
    }
    let Ok((def, _)) = first_def(xml) else {
        return Vec::new();
    };
    let mut hints = Vec::new();
    for (id, el) in &def.elements {
        if let ElementKind::ServiceTask { job_type, .. } = &el.kind {
            // The job type defaulted to the task id *and* simulate flagged it as a
            // worker history never recorded — the binding-mistake signature.
            if job_type == id && uncovered.iter().any(|u| u == id) {
                hints.push(finding(
                    "warn",
                    "job-type-defaulted-to-task-id",
                    Some(id),
                    format!(
                        "Job type '{id}' equals the serviceTask id — this is NOT an engine bug. \
                         The engine fell back to the element id because '{id}' has no parseable \
                         <zeebe:taskDefinition> CHILD element, so its worker never bound to the \
                         recorded job types (it shows up as uncovered / requires-new-worker). Fix \
                         the binding: either set it with edit_model \
                         {{\"op\":\"set_task_job_type\",\"task\":\"{id}\",\"jobType\":\"<recorded-job-type>\"}} \
                         or write the child-element form \
                         <bpmn:extensionElements><zeebe:taskDefinition type=\"<recorded-job-type>\"/></bpmn:extensionElements> \
                         (a taskDefinition ATTRIBUTE is ignored). Use a job type that matches the \
                         recorded dataset so the existing worker output is replayed."
                    ),
                ));
            }
        }
    }
    hints.sort_by(|a, b| a["element"].as_str().cmp(&b["element"].as_str()));
    hints
}

/// `validate_model` — a cheap, dataset-independent **lint/validate** pass over a candidate BPMN
/// model. It parses the XML with the engine's own parser (the same one production deploys with),
/// surfacing structural mistakes the LLM commonly makes — a dangling `errorRef`, a missing
/// `<bpmn:definitions>` root, unparseable XML — before the model is ever simulated or deployed. A
/// bare `<bpmn:process>` fragment is wrapped for analysis (with a warning to emit a full document).
/// On a clean parse it folds in the full [`analyze_model`] structural findings.
pub fn validate_model(xml: &str) -> Result<Value, String> {
    let (healed, fixes) = normalize_authoring(xml);
    let lint = lint_task_definition_attribute(&healed);
    let (doc, wrapped) = ensure_definitions(&healed);
    match analyze_model(&doc) {
        Ok(mut v) => {
            if let Some(obj) = v.as_object_mut() {
                obj.insert("valid".into(), Value::Bool(true));
                if let Some(arr) = obj.get_mut("findings").and_then(|f| f.as_array_mut()) {
                    // Surface the silent taskDefinition-attribute mistake (advisory) and report any
                    // syntax we auto-healed, so the model learns rather than re-submitting it.
                    for l in lint.into_iter().rev() {
                        arr.insert(0, l);
                    }
                    for note in fixes.iter().rev() {
                        arr.insert(0, finding("info", "auto-fixed", None, note.clone()));
                    }
                }
                if !fixes.is_empty() {
                    obj.insert("autoFixed".into(), json!(fixes));
                }
                if wrapped {
                    if let Some(arr) = obj.get_mut("findings").and_then(|f| f.as_array_mut()) {
                        arr.insert(
                            0,
                            finding(
                                "warn",
                                "missing-definitions-root",
                                None,
                                "Model was a bare <bpmn:process> fragment with no \
                                 <bpmn:definitions> root; it was wrapped for analysis. Emit a \
                                 full <bpmn:definitions …> document so it deploys and renders \
                                 without normalization."
                                    .into(),
                            ),
                        );
                    }
                    obj.insert("wrapped".into(), Value::Bool(true));
                }
                // Recompute the headline counts so they reflect the findings we just inserted.
                if let Some(arr) = obj.get("findings").and_then(|f| f.as_array()) {
                    let (mut warns, mut infos) = (0u64, 0u64);
                    for f in arr {
                        match f.get("severity").and_then(|s| s.as_str()) {
                            Some("warn") => warns += 1,
                            Some("info") => infos += 1,
                            _ => {}
                        }
                    }
                    obj.insert("findingCount".into(), json!(arr.len()));
                    obj.insert("warnings".into(), json!(warns));
                    obj.insert("infos".into(), json!(infos));
                }
            }
            Ok(v)
        }
        Err(parse_error) => {
            let fix = deploy_fix_hint(&parse_error);
            let mut message = parse_error.clone();
            if let Some(h) = &fix {
                message.push_str("\nFix: ");
                message.push_str(h);
            }
            let mut findings = Vec::new();
            for note in &fixes {
                findings.push(finding("info", "auto-fixed", None, note.clone()));
            }
            findings.push(finding("error", "parse-error", None, message));
            Ok(json!({
                "valid": false,
                "parseError": parse_error,
                "fix": fix,
                "autoFixed": fixes,
                "findingCount": findings.len(),
                "warnings": 0,
                "infos": fixes.len(),
                "findings": findings,
                "note": "The model does not parse and cannot be deployed or simulated. Fix the \
                         error above and re-run validate_model.",
            }))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Structured authoring: serialize → edit → re-validate
//
// The Experiment Designer is good at process *ideas* but poor at one-shotting whole BPMN
// documents by hand (Investigation 1 thrashed on `<errorBoundaryEvent>` and a
// `zeebe:taskDefinition` attribute it could not see was wrong). `edit_model` flips the
// authoring contract: the model proposes high-level, *validated* operations against the
// CURRENT model and OUR code owns XML correctness — emitting engine-parseable XML and
// re-parsing it before returning. The LLM never types raw whole-document XML again; it
// composes a variant from patches that cannot produce the syntax mistakes it falls into.
// ─────────────────────────────────────────────────────────────────────────────

/// Escape the five predefined XML entities for safe emission into attribute values and text
/// (the inverse of the engine tokenizer's `unescape`). `&` first so we don't double-escape.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// A safe id fragment: keep alphanumerics and `_`/`-`, map everything else to `_`.
fn id_fragment(s: &str) -> String {
    let frag: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if frag.is_empty() {
        "x".to_string()
    } else {
        frag
    }
}

/// Render a duration in milliseconds as an ISO-8601 duration the engine parser accepts
/// (`P[nD]T[nH][nM][nS]`). Sub-second precision is not representable in that grammar, so the
/// value is floored to whole seconds; authoring timers at second granularity round-trips.
fn millis_to_iso8601(ms: u64) -> String {
    let mut rem = ms / 1000;
    let days = rem / 86_400;
    rem %= 86_400;
    let hours = rem / 3_600;
    rem %= 3_600;
    let mins = rem / 60;
    let secs = rem % 60;
    let mut date = String::from("P");
    if days > 0 {
        date.push_str(&format!("{days}D"));
    }
    let mut time = String::new();
    if hours > 0 {
        time.push_str(&format!("{hours}H"));
    }
    if mins > 0 {
        time.push_str(&format!("{mins}M"));
    }
    if secs > 0 || (days == 0 && hours == 0 && mins == 0) {
        time.push_str(&format!("{secs}S"));
    }
    if !time.is_empty() {
        date.push('T');
        date.push_str(&time);
    }
    date
}

/// Collect the definitions-level `<bpmn:error>` declarations a model needs: one per distinct
/// error code carried by an error boundary event. Returns a code → synthesized-error-id map.
fn collect_error_ids(def: &ProcessDefinition) -> BTreeMap<String, String> {
    let mut codes: BTreeMap<String, String> = BTreeMap::new();
    for el in def.elements.values() {
        if let ElementKind::ErrorBoundaryEvent { error_code, .. } = &el.kind {
            codes
                .entry(error_code.clone())
                .or_insert_with(|| format!("Error_{}", id_fragment(error_code)));
        }
    }
    codes
}

/// A definitions-level `<bpmn:message>` declaration needed by a message start/catch/boundary.
struct MessageDecl {
    id: String,
    name: String,
    correlation_key: Option<String>,
}

/// The message declarations a model needs, paired with a lookup from `(name, correlation_key)`
/// to the declaration id (so events differing only in correlation get distinct declarations).
type MessageCollection = (Vec<MessageDecl>, HashMap<(String, Option<String>), String>);

/// Collect the `<bpmn:message>` declarations a model needs, keyed for lookup by
/// `(name, correlation_key)` so events that differ in correlation get distinct declarations.
fn collect_messages(def: &ProcessDefinition) -> MessageCollection {
    let mut decls: Vec<MessageDecl> = Vec::new();
    let mut lookup: HashMap<(String, Option<String>), String> = HashMap::new();
    let mut want: Vec<(String, Option<String>)> = Vec::new();
    let mut ids: Vec<&String> = def.elements.keys().collect();
    ids.sort();
    for id in ids {
        match &def.elements[id].kind {
            ElementKind::MessageStartEvent { message_name } => {
                want.push((message_name.clone(), None));
            }
            ElementKind::MessageIntermediateCatchEvent {
                message_name,
                correlation_key,
            }
            | ElementKind::MessageBoundaryEvent {
                message_name,
                correlation_key,
                ..
            } => {
                want.push((message_name.clone(), Some(correlation_key.clone())));
            }
            _ => {}
        }
    }
    for (name, key) in want {
        let entry = (name.clone(), key.clone());
        if lookup.contains_key(&entry) {
            continue;
        }
        let mid = format!("Message_{}_{}", id_fragment(&name), decls.len());
        lookup.insert(entry, mid.clone());
        decls.push(MessageDecl {
            id: mid,
            name,
            correlation_key: key,
        });
    }
    (decls, lookup)
}

/// Serialize one element (and, for a sub-process, its contained children) as BPMN XML. Sequence
/// flows are emitted separately and flat, so this only renders the node and its event/extension
/// definitions. `errors`/`messages` provide the synthesized declaration ids to reference.
fn emit_element(
    def: &ProcessDefinition,
    id: &str,
    errors: &BTreeMap<String, String>,
    messages: &HashMap<(String, Option<String>), String>,
    children_by_parent: &HashMap<String, Vec<String>>,
    out: &mut String,
) {
    let el = &def.elements[id];
    let eid = xml_escape(id);
    match &el.kind {
        ElementKind::StartEvent => {
            out.push_str(&format!("    <bpmn:startEvent id=\"{eid}\"/>\n"));
        }
        ElementKind::EndEvent => {
            out.push_str(&format!("    <bpmn:endEvent id=\"{eid}\"/>\n"));
        }
        ElementKind::ExclusiveGateway => {
            out.push_str(&format!("    <bpmn:exclusiveGateway id=\"{eid}\"/>\n"));
        }
        ElementKind::ParallelGateway => {
            out.push_str(&format!("    <bpmn:parallelGateway id=\"{eid}\"/>\n"));
        }
        ElementKind::ServiceTask { job_type, priority } => {
            out.push_str(&format!("    <bpmn:serviceTask id=\"{eid}\">\n"));
            out.push_str("      <bpmn:extensionElements>\n");
            out.push_str(&format!(
                "        <zeebe:taskDefinition type=\"{}\"/>\n",
                xml_escape(job_type)
            ));
            if let Some(p) = priority {
                out.push_str(&format!(
                    "        <zeebe:priorityDefinition priority=\"{}\"/>\n",
                    xml_escape(p)
                ));
            }
            out.push_str("      </bpmn:extensionElements>\n");
            out.push_str("    </bpmn:serviceTask>\n");
        }
        ElementKind::UserTask(props) => {
            out.push_str(&format!("    <bpmn:userTask id=\"{eid}\">\n"));
            out.push_str("      <bpmn:extensionElements>\n");
            if props.assignee.is_some()
                || props.candidate_groups.is_some()
                || props.candidate_users.is_some()
            {
                out.push_str("        <zeebe:assignmentDefinition");
                if let Some(a) = &props.assignee {
                    out.push_str(&format!(" assignee=\"{}\"", xml_escape(a)));
                }
                if let Some(g) = &props.candidate_groups {
                    out.push_str(&format!(" candidateGroups=\"{}\"", xml_escape(g)));
                }
                if let Some(u) = &props.candidate_users {
                    out.push_str(&format!(" candidateUsers=\"{}\"", xml_escape(u)));
                }
                out.push_str("/>\n");
            }
            if props.due_date.is_some() || props.follow_up_date.is_some() {
                out.push_str("        <zeebe:taskSchedule");
                if let Some(d) = &props.due_date {
                    out.push_str(&format!(" dueDate=\"{}\"", xml_escape(d)));
                }
                if let Some(f) = &props.follow_up_date {
                    out.push_str(&format!(" followUpDate=\"{}\"", xml_escape(f)));
                }
                out.push_str("/>\n");
            }
            if let Some(p) = &props.priority {
                out.push_str(&format!(
                    "        <zeebe:priorityDefinition priority=\"{}\"/>\n",
                    xml_escape(p)
                ));
            }
            out.push_str("      </bpmn:extensionElements>\n");
            out.push_str("    </bpmn:userTask>\n");
        }
        ElementKind::ErrorBoundaryEvent {
            attached_to,
            error_code,
        } => {
            let err_id = errors
                .get(error_code)
                .cloned()
                .unwrap_or_else(|| format!("Error_{}", id_fragment(error_code)));
            out.push_str(&format!(
                "    <bpmn:boundaryEvent id=\"{eid}\" attachedToRef=\"{}\">\n",
                xml_escape(attached_to)
            ));
            out.push_str(&format!(
                "      <bpmn:errorEventDefinition errorRef=\"{}\"/>\n",
                xml_escape(&err_id)
            ));
            out.push_str("    </bpmn:boundaryEvent>\n");
        }
        ElementKind::TimerBoundaryEvent {
            attached_to,
            duration_millis,
            interrupting,
            repeating,
        } => {
            let cancel = if *interrupting {
                ""
            } else {
                " cancelActivity=\"false\""
            };
            out.push_str(&format!(
                "    <bpmn:boundaryEvent id=\"{eid}\" attachedToRef=\"{}\"{cancel}>\n",
                xml_escape(attached_to)
            ));
            out.push_str("      <bpmn:timerEventDefinition>\n");
            if *repeating && !*interrupting {
                out.push_str(&format!(
                    "        <bpmn:timeCycle>R/{}</bpmn:timeCycle>\n",
                    millis_to_iso8601(*duration_millis)
                ));
            } else {
                out.push_str(&format!(
                    "        <bpmn:timeDuration>{}</bpmn:timeDuration>\n",
                    millis_to_iso8601(*duration_millis)
                ));
            }
            out.push_str("      </bpmn:timerEventDefinition>\n");
            out.push_str("    </bpmn:boundaryEvent>\n");
        }
        ElementKind::MessageBoundaryEvent {
            attached_to,
            message_name,
            correlation_key,
            interrupting,
        } => {
            let mref = messages
                .get(&(message_name.clone(), Some(correlation_key.clone())))
                .cloned()
                .unwrap_or_default();
            let cancel = if *interrupting {
                ""
            } else {
                " cancelActivity=\"false\""
            };
            out.push_str(&format!(
                "    <bpmn:boundaryEvent id=\"{eid}\" attachedToRef=\"{}\"{cancel}>\n",
                xml_escape(attached_to)
            ));
            out.push_str(&format!(
                "      <bpmn:messageEventDefinition messageRef=\"{}\"/>\n",
                xml_escape(&mref)
            ));
            out.push_str("    </bpmn:boundaryEvent>\n");
        }
        ElementKind::TimerIntermediateCatchEvent { duration_millis } => {
            out.push_str(&format!("    <bpmn:intermediateCatchEvent id=\"{eid}\">\n"));
            out.push_str("      <bpmn:timerEventDefinition>\n");
            out.push_str(&format!(
                "        <bpmn:timeDuration>{}</bpmn:timeDuration>\n",
                millis_to_iso8601(*duration_millis)
            ));
            out.push_str("      </bpmn:timerEventDefinition>\n");
            out.push_str("    </bpmn:intermediateCatchEvent>\n");
        }
        ElementKind::MessageIntermediateCatchEvent {
            message_name,
            correlation_key,
        } => {
            let mref = messages
                .get(&(message_name.clone(), Some(correlation_key.clone())))
                .cloned()
                .unwrap_or_default();
            out.push_str(&format!("    <bpmn:intermediateCatchEvent id=\"{eid}\">\n"));
            out.push_str(&format!(
                "      <bpmn:messageEventDefinition messageRef=\"{}\"/>\n",
                xml_escape(&mref)
            ));
            out.push_str("    </bpmn:intermediateCatchEvent>\n");
        }
        ElementKind::MessageStartEvent { message_name } => {
            let mref = messages
                .get(&(message_name.clone(), None))
                .cloned()
                .unwrap_or_default();
            out.push_str(&format!("    <bpmn:startEvent id=\"{eid}\">\n"));
            out.push_str(&format!(
                "      <bpmn:messageEventDefinition messageRef=\"{}\"/>\n",
                xml_escape(&mref)
            ));
            out.push_str("    </bpmn:startEvent>\n");
        }
        ElementKind::TimerStartEvent {
            interval_millis,
            repeating,
        } => {
            out.push_str(&format!("    <bpmn:startEvent id=\"{eid}\">\n"));
            out.push_str("      <bpmn:timerEventDefinition>\n");
            if *repeating {
                out.push_str(&format!(
                    "        <bpmn:timeCycle>R/{}</bpmn:timeCycle>\n",
                    millis_to_iso8601(*interval_millis)
                ));
            } else {
                out.push_str(&format!(
                    "        <bpmn:timeDuration>{}</bpmn:timeDuration>\n",
                    millis_to_iso8601(*interval_millis)
                ));
            }
            out.push_str("      </bpmn:timerEventDefinition>\n");
            out.push_str("    </bpmn:startEvent>\n");
        }
        ElementKind::SubProcess { .. } => {
            out.push_str(&format!("    <bpmn:subProcess id=\"{eid}\">\n"));
            if let Some(kids) = children_by_parent.get(id) {
                for child in kids {
                    // Children are emitted at the same indentation; the engine parser keys
                    // containment off the scope stack, not indentation, so this is faithful.
                    emit_element(def, child, errors, messages, children_by_parent, out);
                }
            }
            out.push_str("    </bpmn:subProcess>\n");
        }
    }
}

/// Serialize a [`ProcessDefinition`] back to BPMN XML that the engine's own parser round-trips
/// (start event, every node kind, sequence flows with conditions, and the definitions-level
/// `<bpmn:error>` / `<bpmn:message>` declarations boundary and message events reference).
///
/// Diagram-interchange (`<bpmndi>`) is intentionally omitted — these are *candidate* models for
/// simulation, and the cockpit renders DI-less variants. The contract this guarantees is the one
/// that matters for authoring: `parse_bpmn(definition_to_xml(def))` reproduces `def`'s structure.
pub fn definition_to_xml(def: &ProcessDefinition) -> String {
    let errors = collect_error_ids(def);
    let (messages, msg_lookup) = collect_messages(def);

    // Group sub-process children by parent so a container emits its contents inline.
    let mut children_by_parent: HashMap<String, Vec<String>> = HashMap::new();
    for (id, el) in &def.elements {
        if let Some(parent) = &el.parent {
            children_by_parent
                .entry(parent.clone())
                .or_default()
                .push(id.clone());
        }
    }
    for kids in children_by_parent.values_mut() {
        kids.sort();
    }

    let mut out = String::new();
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.push_str(
        "<bpmn:definitions xmlns:bpmn=\"http://www.omg.org/spec/BPMN/20100524/MODEL\" \
         xmlns:zeebe=\"http://camunda.org/schema/zeebe/1.0\" \
         targetNamespace=\"http://bpmn.io/schema/bpmn\">\n",
    );
    for (code, eid) in &errors {
        out.push_str(&format!(
            "  <bpmn:error id=\"{}\" errorCode=\"{}\"/>\n",
            xml_escape(eid),
            xml_escape(code)
        ));
    }
    for m in &messages {
        if let Some(key) = &m.correlation_key {
            out.push_str(&format!(
                "  <bpmn:message id=\"{}\" name=\"{}\">\n",
                xml_escape(&m.id),
                xml_escape(&m.name)
            ));
            out.push_str("    <bpmn:extensionElements>\n");
            out.push_str(&format!(
                "      <zeebe:subscription correlationKey=\"={}\"/>\n",
                xml_escape(key)
            ));
            out.push_str("    </bpmn:extensionElements>\n");
            out.push_str("  </bpmn:message>\n");
        } else {
            out.push_str(&format!(
                "  <bpmn:message id=\"{}\" name=\"{}\"/>\n",
                xml_escape(&m.id),
                xml_escape(&m.name)
            ));
        }
    }
    out.push_str(&format!(
        "  <bpmn:process id=\"{}\" isExecutable=\"true\">\n",
        xml_escape(&def.id)
    ));

    // Emit top-level nodes (parent == None) in a stable order; sub-processes recurse.
    let mut top: Vec<&String> = def
        .elements
        .iter()
        .filter(|(_, e)| e.parent.is_none())
        .map(|(id, _)| id)
        .collect();
    top.sort();
    for id in top {
        emit_element(def, id, &errors, &msg_lookup, &children_by_parent, &mut out);
    }

    // Emit every sequence flow flat with a synthesized id. The engine parser builds flows purely
    // from sourceRef/targetRef, so flat emission (scope-independent) is faithful.
    let mut sources: Vec<&String> = def.elements.keys().collect();
    sources.sort();
    let mut n = 0usize;
    for src in sources {
        for flow in &def.elements[src].outgoing {
            n += 1;
            let fid = format!("Flow_{n}");
            match &flow.condition {
                Some(cond) => {
                    out.push_str(&format!(
                        "    <bpmn:sequenceFlow id=\"{fid}\" sourceRef=\"{}\" targetRef=\"{}\">\n",
                        xml_escape(src),
                        xml_escape(&flow.to)
                    ));
                    out.push_str(&format!(
                        "      <bpmn:conditionExpression>{}</bpmn:conditionExpression>\n",
                        xml_escape(&cond.expression)
                    ));
                    out.push_str("    </bpmn:sequenceFlow>\n");
                }
                None => {
                    out.push_str(&format!(
                        "    <bpmn:sequenceFlow id=\"{fid}\" sourceRef=\"{}\" targetRef=\"{}\"/>\n",
                        xml_escape(src),
                        xml_escape(&flow.to)
                    ));
                }
            }
        }
    }

    out.push_str("  </bpmn:process>\n");
    out.push_str("</bpmn:definitions>\n");
    out
}

/// Build a sequence-flow condition from an optional expression string. An empty/whitespace
/// expression clears the condition (an unconditional flow).
fn condition_from(expr: Option<&str>) -> Option<Condition> {
    match expr.map(str::trim) {
        Some(e) if !e.is_empty() => Some(Condition::new(e.to_string())),
        _ => None,
    }
}

/// Require a string field on an edit op, or return a clear error naming the op and field.
fn req_str<'a>(op: &'a Value, field: &str, op_name: &str) -> Result<&'a str, String> {
    op.get(field)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("op '{op_name}' requires a non-empty string '{field}'"))
}

/// Apply a single structured edit operation to `def` in place, returning a human-readable note
/// describing what changed. Each op fully owns the XML-level correctness of its change; an op that
/// references a missing node or would duplicate an id fails with an actionable message so the model
/// can correct the *operation* rather than re-typing raw XML.
fn apply_edit_op(def: &mut ProcessDefinition, op: &Value) -> Result<String, String> {
    let op_name = op
        .get("op")
        .and_then(|v| v.as_str())
        .ok_or("each edit op needs a string 'op' field")?;
    match op_name {
        "set_task_job_type" => {
            let task = req_str(op, "task", op_name)?;
            let job_type = req_str(op, "jobType", op_name)?;
            let el = def
                .elements
                .get_mut(task)
                .ok_or_else(|| format!("set_task_job_type: no node '{task}' in the model"))?;
            match &mut el.kind {
                ElementKind::ServiceTask { job_type: jt, .. } => {
                    *jt = job_type.to_string();
                    Ok(format!("Set serviceTask '{task}' jobType to '{job_type}'."))
                }
                _ => Err(format!("set_task_job_type: '{task}' is not a serviceTask")),
            }
        }
        "set_flow_condition" => {
            let from = req_str(op, "from", op_name)?;
            let to = req_str(op, "to", op_name)?;
            let cond = condition_from(op.get("condition").and_then(|v| v.as_str()));
            let el = def
                .elements
                .get_mut(from)
                .ok_or_else(|| format!("set_flow_condition: no node '{from}' in the model"))?;
            let flow = el
                .outgoing
                .iter_mut()
                .find(|f| f.to == to)
                .ok_or_else(|| format!("set_flow_condition: no flow '{from}' -> '{to}'"))?;
            flow.condition = cond.clone();
            Ok(match cond {
                Some(c) => format!("Set condition on '{from}' -> '{to}' to `{}`.", c.expression),
                None => format!("Cleared the condition on '{from}' -> '{to}'."),
            })
        }
        "insert_service_task_after" => {
            let after = req_str(op, "after", op_name)?;
            let id = req_str(op, "id", op_name)?;
            let job_type = req_str(op, "jobType", op_name)?;
            if def.elements.contains_key(id) {
                return Err(format!(
                    "insert_service_task_after: node id '{id}' already exists"
                ));
            }
            let anchor = def
                .elements
                .get_mut(after)
                .ok_or_else(|| format!("insert_service_task_after: no node '{after}'"))?;
            // The new task inherits the anchor's outgoing flows; the anchor flows unconditionally
            // into it. So: after -> NEW -> (original targets, conditions preserved).
            let moved: Vec<SequenceFlow> = std::mem::take(&mut anchor.outgoing);
            anchor.outgoing.push(SequenceFlow {
                to: id.to_string(),
                condition: None,
            });
            let parent = anchor.parent.clone();
            def.elements.insert(
                id.to_string(),
                Element {
                    id: id.to_string(),
                    kind: ElementKind::ServiceTask {
                        job_type: job_type.to_string(),
                        priority: None,
                    },
                    outgoing: moved,
                    parent,
                },
            );
            Ok(format!(
                "Inserted serviceTask '{id}' (jobType '{job_type}') immediately after '{after}'."
            ))
        }
        "add_error_boundary" => {
            let task = req_str(op, "task", op_name)?;
            let error_code = req_str(op, "errorCode", op_name)?;
            let target = req_str(op, "target", op_name)?;
            let id = op
                .get("id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| {
                    format!("Boundary_{}_{}", id_fragment(task), id_fragment(error_code))
                });
            if def.elements.contains_key(&id) {
                return Err(format!("add_error_boundary: node id '{id}' already exists"));
            }
            match def.elements.get(task).map(|e| &e.kind) {
                Some(ElementKind::ServiceTask { .. }) => {}
                Some(_) => {
                    return Err(format!("add_error_boundary: '{task}' is not a serviceTask"))
                }
                None => return Err(format!("add_error_boundary: no node '{task}'")),
            }
            if !def.elements.contains_key(target) {
                return Err(format!(
                    "add_error_boundary: target node '{target}' does not exist"
                ));
            }
            def.elements.insert(
                id.clone(),
                Element {
                    id: id.clone(),
                    kind: ElementKind::ErrorBoundaryEvent {
                        attached_to: task.to_string(),
                        error_code: error_code.to_string(),
                    },
                    outgoing: vec![SequenceFlow {
                        to: target.to_string(),
                        condition: None,
                    }],
                    parent: None,
                },
            );
            Ok(format!(
                "Added an error boundary '{id}' (errorCode '{error_code}') on '{task}', routing to '{target}'."
            ))
        }
        "reroute_flow" => {
            let from = req_str(op, "from", op_name)?;
            let to = req_str(op, "to", op_name)?;
            let new_to = req_str(op, "newTo", op_name)?;
            if !def.elements.contains_key(new_to) {
                return Err(format!(
                    "reroute_flow: newTo node '{new_to}' does not exist"
                ));
            }
            let el = def
                .elements
                .get_mut(from)
                .ok_or_else(|| format!("reroute_flow: no node '{from}'"))?;
            let flow = el
                .outgoing
                .iter_mut()
                .find(|f| f.to == to)
                .ok_or_else(|| format!("reroute_flow: no flow '{from}' -> '{to}'"))?;
            flow.to = new_to.to_string();
            Ok(format!(
                "Rerouted flow '{from}' -> '{to}' to target '{new_to}'."
            ))
        }
        "remove_node" => {
            let id = req_str(op, "id", op_name)?;
            if id == def.start_event {
                return Err("remove_node: refusing to remove the start event".to_string());
            }
            let removed = def
                .elements
                .remove(id)
                .ok_or_else(|| format!("remove_node: no node '{id}'"))?;
            // Heal flows: every predecessor that pointed at the removed node now points at each of
            // the removed node's successors (carrying the predecessor's own condition).
            let successors: Vec<SequenceFlow> = removed.outgoing;
            for el in def.elements.values_mut() {
                let mut rewired: Vec<SequenceFlow> = Vec::new();
                for flow in std::mem::take(&mut el.outgoing) {
                    if flow.to == id {
                        for succ in &successors {
                            rewired.push(SequenceFlow {
                                to: succ.to.clone(),
                                condition: flow.condition.clone(),
                            });
                        }
                    } else {
                        rewired.push(flow);
                    }
                }
                el.outgoing = rewired;
            }
            // Drop any boundary events that were attached to the removed node (they would dangle).
            let orphaned: Vec<String> = def
                .elements
                .iter()
                .filter(|(_, e)| {
                    matches!(&e.kind,
                    ElementKind::ErrorBoundaryEvent { attached_to, .. }
                    | ElementKind::TimerBoundaryEvent { attached_to, .. }
                    | ElementKind::MessageBoundaryEvent { attached_to, .. }
                    if attached_to == id)
                })
                .map(|(bid, _)| bid.clone())
                .collect();
            for bid in &orphaned {
                def.elements.remove(bid);
            }
            Ok(format!(
                "Removed node '{id}', reconnecting its predecessors to its successors{}.",
                if orphaned.is_empty() {
                    String::new()
                } else {
                    format!(
                        " (also dropped {} attached boundary event(s))",
                        orphaned.len()
                    )
                }
            ))
        }
        "add_exclusive_gateway" => {
            let id = req_str(op, "id", op_name)?;
            let after = req_str(op, "after", op_name)?;
            if def.elements.contains_key(id) {
                return Err(format!(
                    "add_exclusive_gateway: node id '{id}' already exists"
                ));
            }
            let branches = op
                .get("branches")
                .and_then(|v| v.as_array())
                .ok_or("add_exclusive_gateway requires a 'branches' array")?;
            if branches.is_empty() {
                return Err(
                    "add_exclusive_gateway: 'branches' must list at least one target".into(),
                );
            }
            let mut outgoing: Vec<SequenceFlow> = Vec::new();
            for (i, b) in branches.iter().enumerate() {
                let to = b
                    .get("to")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        format!("add_exclusive_gateway: branch {i} needs a 'to' node")
                    })?;
                if !def.elements.contains_key(to) {
                    return Err(format!(
                        "add_exclusive_gateway: branch target '{to}' does not exist"
                    ));
                }
                outgoing.push(SequenceFlow {
                    to: to.to_string(),
                    condition: condition_from(b.get("condition").and_then(|v| v.as_str())),
                });
            }
            // Splice the gateway onto the anchor's single outgoing edge: after -> G, and G fans out
            // to the requested branches. The anchor's original targets are preserved as the LAST,
            // unconditional (default) branch so no path is silently dropped.
            let anchor = def
                .elements
                .get_mut(after)
                .ok_or_else(|| format!("add_exclusive_gateway: no node '{after}'"))?;
            let original: Vec<SequenceFlow> = std::mem::take(&mut anchor.outgoing);
            anchor.outgoing.push(SequenceFlow {
                to: id.to_string(),
                condition: None,
            });
            let parent = anchor.parent.clone();
            for f in original {
                // Carried as default branches (conditions dropped: the gateway now decides).
                outgoing.push(SequenceFlow {
                    to: f.to,
                    condition: None,
                });
            }
            def.elements.insert(
                id.to_string(),
                Element {
                    id: id.to_string(),
                    kind: ElementKind::ExclusiveGateway,
                    outgoing,
                    parent,
                },
            );
            Ok(format!(
                "Inserted exclusiveGateway '{id}' after '{after}' with {} branch(es).",
                branches.len()
            ))
        }
        other => Err(format!(
            "unknown edit op '{other}'. Supported: set_task_job_type, set_flow_condition, \
             insert_service_task_after, add_error_boundary, reroute_flow, remove_node, \
             add_exclusive_gateway."
        )),
    }
}

/// `edit_model` — compose a candidate BPMN variant from a list of **validated structured
/// operations** applied to a base model, instead of one-shotting raw whole-document XML.
///
/// The base is auto-healed (`normalize_authoring`) and parsed with the engine's own parser; each
/// op mutates the parsed model and OUR serializer re-emits engine-parseable XML, which is re-parsed
/// to guarantee the result deploys. Returns the new XML, the per-op change notes, and the
/// post-edit [`analyze_model`] structural findings so the model sees the consequences immediately.
pub fn edit_model(base_xml: &str, ops: &[Value]) -> Result<Value, String> {
    if ops.is_empty() {
        return Err("edit_model needs at least one operation in 'ops'".to_string());
    }
    let (healed, heal_notes) = normalize_authoring(base_xml);
    let (wrapped, _) = ensure_definitions(&healed);
    let mut def = parse_bpmn(&wrapped)
        .map_err(|e| {
            let err = format!("{e:?}");
            match deploy_fix_hint(&err) {
                Some(h) => format!("base model failed to parse: {err}\nFix: {h}"),
                None => format!("base model failed to parse: {err}"),
            }
        })?
        .into_iter()
        .next()
        .ok_or("base model contained no process definitions")?;

    let mut applied: Vec<String> = heal_notes;
    for (i, op) in ops.iter().enumerate() {
        let note = apply_edit_op(&mut def, op).map_err(|e| format!("op {} failed: {e}", i + 1))?;
        applied.push(note);
    }

    let xml = definition_to_xml(&def);
    // The serializer owns correctness, but re-parse defensively so we never hand back XML that the
    // engine would reject at deploy time — surfacing any logical inconsistency the ops introduced.
    if let Err(e) = parse_bpmn(&xml) {
        let err = format!("{e:?}");
        let hint = deploy_fix_hint(&err);
        return Err(format!(
            "the edited model does not parse ({err}){}. This usually means an operation left a \
             dangling reference (e.g. a flow to a removed node).",
            hint.map(|h| format!("\nFix: {h}")).unwrap_or_default()
        ));
    }
    let analysis = analyze_model(&xml).unwrap_or_else(|_| json!({}));

    Ok(json!({
        "ok": true,
        "model": xml,
        "appliedOps": applied,
        "findings": analysis.get("findings").cloned().unwrap_or(json!([])),
        "metrics": analysis.get("metrics").cloned().unwrap_or(json!({})),
        "note": "This XML is engine-validated and ready to simulate. Pass it to simulate (start \
                 with limit:1) or compare_variants. Do NOT hand-edit it — apply further changes \
                 with another edit_model call.",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    // A small loan-approval-shaped model: start -> credit-check (service) -> XOR gateway
    // -> approve / reject -> end. The service task has an error boundary to a reject end.
    const LOAN_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="defs">
  <bpmn:process id="loan-approval" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f0</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="CreditCheck">
      <bpmn:extensionElements><zeebe:taskDefinition type="credit-check"/></bpmn:extensionElements>
      <bpmn:incoming>f0</bpmn:incoming><bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:exclusiveGateway id="Decision">
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:exclusiveGateway>
    <bpmn:serviceTask id="Approve">
      <bpmn:extensionElements><zeebe:taskDefinition type="approve-loan"/></bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f4</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="Reject">
      <bpmn:extensionElements><zeebe:taskDefinition type="reject-application"/></bpmn:extensionElements>
      <bpmn:incoming>f3</bpmn:incoming><bpmn:outgoing>f5</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:endEvent id="EndApproved"><bpmn:incoming>f4</bpmn:incoming></bpmn:endEvent>
    <bpmn:endEvent id="EndRejected"><bpmn:incoming>f5</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f0" sourceRef="Start" targetRef="CreditCheck"/>
    <bpmn:sequenceFlow id="f1" sourceRef="CreditCheck" targetRef="Decision"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Decision" targetRef="Approve">
      <bpmn:conditionExpression>= creditScore &gt;= 700</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="f3" sourceRef="Decision" targetRef="Reject"/>
    <bpmn:sequenceFlow id="f4" sourceRef="Approve" targetRef="EndApproved"/>
    <bpmn:sequenceFlow id="f5" sourceRef="Reject" targetRef="EndRejected"/>
  </bpmn:process>
</bpmn:definitions>"#;

    #[test]
    fn read_model_distils_nodes_kinds_and_join_keys() {
        let v = read_model(LOAN_BPMN).expect("read");
        assert_eq!(v["processId"], "loan-approval");
        assert_eq!(v["startEvent"], "Start");
        let nodes = v["nodes"].as_array().unwrap();
        // The credit-check service task surfaces its job type (the trace join key).
        let cc = nodes.iter().find(|n| n["id"] == "CreditCheck").unwrap();
        assert_eq!(cc["kind"], "serviceTask");
        assert_eq!(cc["jobType"], "credit-check");
        assert_eq!(cc["reachable"], true);
        // The gateway reports a split role.
        let gw = nodes.iter().find(|n| n["id"] == "Decision").unwrap();
        assert_eq!(gw["gatewayRole"], "split");
        // Two end events counted.
        assert_eq!(v["counts"]["endEvent"], 2);
    }

    #[test]
    fn analyze_model_flags_unguarded_service_tasks() {
        let v = analyze_model(LOAN_BPMN).expect("analyze");
        let findings = v["findings"].as_array().unwrap();
        // Default flow f3 exists, so NO exclusive-no-default finding.
        assert!(!findings.iter().any(|f| f["code"] == "exclusive-no-default"));
        // All three service tasks lack error/timer boundaries -> unguarded info.
        let unguarded: Vec<&str> = findings
            .iter()
            .filter(|f| f["code"] == "service-task-unguarded")
            .map(|f| f["element"].as_str().unwrap())
            .collect();
        assert!(unguarded.contains(&"CreditCheck"));
        assert!(unguarded.contains(&"Approve"));
        assert!(unguarded.contains(&"Reject"));
    }

    #[test]
    fn analyze_model_flags_unreachable_and_dead_end_nodes() {
        // An orphan service task with no incoming flow and no outgoing flow.
        let bpmn = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="d">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="S"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="T">
      <bpmn:extensionElements><zeebe:taskDefinition type="work"/></bpmn:extensionElements>
      <bpmn:incoming>a</bpmn:incoming>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="Orphan">
      <bpmn:extensionElements><zeebe:taskDefinition type="orphan"/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="a" sourceRef="S" targetRef="T"/>
  </bpmn:process>
</bpmn:definitions>"#;
        let v = analyze_model(bpmn).expect("analyze");
        let findings = v["findings"].as_array().unwrap();
        assert!(findings
            .iter()
            .any(|f| f["code"] == "unreachable-node" && f["element"] == "Orphan"));
        // T has no outgoing flow -> dead-end; no end event -> warn.
        assert!(findings
            .iter()
            .any(|f| f["code"] == "dead-end" && f["element"] == "T"));
        assert!(findings.iter().any(|f| f["code"] == "no-end-event"));
    }

    #[test]
    fn analyze_model_detects_a_rework_loop() {
        // start -> A -> gateway -> (retry back to A) | -> end
        let bpmn = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="d">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="S"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="A">
      <bpmn:extensionElements><zeebe:taskDefinition type="work"/></bpmn:extensionElements>
      <bpmn:incoming>a</bpmn:incoming><bpmn:incoming>retry</bpmn:incoming><bpmn:outgoing>b</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:exclusiveGateway id="G">
      <bpmn:incoming>b</bpmn:incoming><bpmn:outgoing>retry</bpmn:outgoing><bpmn:outgoing>done</bpmn:outgoing>
    </bpmn:exclusiveGateway>
    <bpmn:endEvent id="E"><bpmn:incoming>done</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="a" sourceRef="S" targetRef="A"/>
    <bpmn:sequenceFlow id="b" sourceRef="A" targetRef="G"/>
    <bpmn:sequenceFlow id="retry" sourceRef="G" targetRef="A">
      <bpmn:conditionExpression>= retry = true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="done" sourceRef="G" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;
        let v = analyze_model(bpmn).expect("analyze");
        assert_eq!(v["metrics"]["loops"], 1);
        let findings = v["findings"].as_array().unwrap();
        let loop_finding = findings
            .iter()
            .find(|f| f["code"] == "rework-loop")
            .unwrap();
        let msg = loop_finding["message"].as_str().unwrap();
        assert!(msg.contains("\"A\""));
        assert!(msg.contains("\"G\""));
    }

    #[test]
    fn rejects_unparseable_xml() {
        assert!(read_model("not bpmn").is_err());
        assert!(analyze_model("<bpmn/>").is_err());
    }

    // The exact authoring mistake from the loan-approval variant: an error boundary whose
    // errorRef points at no <bpmn:error> definition AND no <bpmn:definitions> root.
    const BARE_DANGLING_ERRORREF: &str = r#"<bpmn:process id="loan-approval" isExecutable="true" xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe"><bpmn:startEvent id="StartEvent_1"/><bpmn:sequenceFlow id="f1" sourceRef="StartEvent_1" targetRef="Task_CreditCheck"/><bpmn:serviceTask id="Task_CreditCheck" name="Credit Check"><bpmn:extensionElements><zeebe:taskDefinition type="credit-check"/></bpmn:extensionElements></bpmn:serviceTask><bpmn:sequenceFlow id="f2" sourceRef="Task_CreditCheck" targetRef="EndEvent_Done"/><bpmn:endEvent id="EndEvent_Done"/><bpmn:boundaryEvent id="BoundaryEvent_CreditError" attachedToRef="Task_CreditCheck"><bpmn:errorEventDefinition errorRef="CREDIT_BUREAU_ERROR"/></bpmn:boundaryEvent><bpmn:sequenceFlow id="f3" sourceRef="BoundaryEvent_CreditError" targetRef="EndEvent_Err"/><bpmn:endEvent id="EndEvent_Err"/></bpmn:process>"#;

    #[test]
    fn validate_model_flags_a_dangling_error_ref_with_a_fix() {
        let v = validate_model(BARE_DANGLING_ERRORREF).expect("validate returns Ok");
        assert_eq!(v["valid"], false);
        let pe = v["parseError"].as_str().unwrap();
        assert!(pe.contains("unknown error"), "got: {pe}");
        // The fix names the real cause: errorRef must point at an <bpmn:error id=…>.
        let fix = v["fix"].as_str().unwrap();
        assert!(fix.contains("errorRef"));
        assert!(fix.contains("bpmn:error"));
        assert_eq!(v["findings"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn validate_model_wraps_a_bare_process_fragment() {
        // A bare <bpmn:process> with a VALID error definition is still missing a definitions root.
        let bare = r#"<bpmn:process id="p" isExecutable="true" xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"><bpmn:startEvent id="S"><bpmn:outgoing>f0</bpmn:outgoing></bpmn:startEvent><bpmn:endEvent id="E"><bpmn:incoming>f0</bpmn:incoming></bpmn:endEvent><bpmn:sequenceFlow id="f0" sourceRef="S" targetRef="E"/></bpmn:process>"#;
        let v = validate_model(bare).expect("validate");
        assert_eq!(v["valid"], true);
        assert_eq!(v["wrapped"], true);
        let findings = v["findings"].as_array().unwrap();
        assert!(findings
            .iter()
            .any(|f| f["code"] == "missing-definitions-root"));
    }

    #[test]
    fn validate_model_passes_a_clean_document() {
        let v = validate_model(LOAN_BPMN).expect("validate");
        assert_eq!(v["valid"], true);
        assert!(v.get("wrapped").is_none() || v["wrapped"] == false);
        assert_eq!(v["processId"], "loan-approval");
    }

    // The exact loop from Investigation 1: an error boundary authored as the NON-EXISTENT element
    // `<bpmn:errorBoundaryEvent>`, so the node is never created and the flow off it dangles.
    const ERROR_BOUNDARY_MISSPELLED: &str = r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:error id="E_CREDIT" errorCode="CREDIT_BUREAU_ERROR"/>
  <bpmn:process id="loan-approval" isExecutable="true">
    <bpmn:startEvent id="S"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Task_CreditCheck"/>
    <bpmn:serviceTask id="Task_CreditCheck" name="Credit Check"><bpmn:extensionElements><zeebe:taskDefinition type="credit-check"/></bpmn:extensionElements></bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Task_CreditCheck" targetRef="EndDone"/>
    <bpmn:endEvent id="EndDone"/>
    <bpmn:errorBoundaryEvent id="BoundaryEvent_CreditError" attachedToRef="Task_CreditCheck"><bpmn:errorEventDefinition errorRef="E_CREDIT"/></bpmn:errorBoundaryEvent>
    <bpmn:sequenceFlow id="f3" sourceRef="BoundaryEvent_CreditError" targetRef="EndErr"/>
    <bpmn:endEvent id="EndErr"/>
  </bpmn:process>
</bpmn:definitions>"#;

    #[test]
    fn normalize_authoring_renames_error_boundary_event() {
        // Before: errorBoundaryEvent makes the model unparseable (dangling flow source).
        assert!(analyze_model(ERROR_BOUNDARY_MISSPELLED).is_err());
        let (healed, notes) = normalize_authoring(ERROR_BOUNDARY_MISSPELLED);
        assert!(!healed.contains("errorBoundaryEvent"));
        assert!(healed.contains("boundaryEvent"));
        assert_eq!(notes.len(), 1);
        assert!(notes[0].contains("errorBoundaryEvent"));
        // After healing it parses cleanly.
        analyze_model(&healed).expect("healed model parses");
    }

    #[test]
    fn validate_model_auto_heals_a_misspelled_error_boundary() {
        let v = validate_model(ERROR_BOUNDARY_MISSPELLED).expect("validate returns Ok");
        // The model that thrashed Investigation 1 now passes, with a note on what was fixed.
        assert_eq!(v["valid"], true);
        assert!(v["autoFixed"]
            .as_array()
            .map(|a| !a.is_empty())
            .unwrap_or(false));
        let findings = v["findings"].as_array().unwrap();
        assert!(findings.iter().any(|f| f["code"] == "auto-fixed"));
    }

    #[test]
    fn deploy_fix_hint_explains_an_unknown_source_element() {
        let err = "InvalidProcess { process_id: \"loan-approval\", reason: \"sequence flow \
                   BoundaryEvent_CreditError->EndEvent_CreditError has unknown source element \
                   BoundaryEvent_CreditError\" }";
        let hint = deploy_fix_hint(err).expect("hint");
        assert!(hint.contains("errorBoundaryEvent"));
        assert!(hint.contains("boundaryEvent"));
    }

    #[test]
    fn validate_model_flags_task_definition_used_as_an_attribute() {
        // The silent mistake: zeebe:taskDefinition as a serviceTask attribute (engine ignores it,
        // job type defaults to the task id) — parses fine, so it must be surfaced as a warning.
        let bad = r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="S"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:serviceTask id="T" name="Pre Screen" zeebe:taskDefinition="pre-screen"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
    <bpmn:endEvent id="E"/>
  </bpmn:process>
</bpmn:definitions>"#;
        let v = validate_model(bad).expect("validate");
        assert_eq!(v["valid"], true);
        let findings = v["findings"].as_array().unwrap();
        assert!(findings
            .iter()
            .any(|f| f["code"] == "task-definition-as-attribute"));
    }

    #[test]
    fn lint_task_definition_attribute_ignores_the_correct_element_form() {
        // The proper child-element form must NOT trip the lint.
        assert!(lint_task_definition_attribute(LOAN_BPMN).is_empty());
    }

    #[test]
    fn model_task_graph_collapses_gateways_onto_task_transitions() {
        let g = model_task_graph(LOAN_BPMN).expect("graph");
        assert_eq!(g.process_id, "loan-approval");
        // Only the three service tasks are observable (start/end/gateway collapse away).
        assert_eq!(g.tasks.len(), 3);
        assert!(g.tasks.contains("CreditCheck"));
        // CreditCheck -> Decision(XOR) -> {Approve, Reject}: both are permitted next tasks.
        let from_cc = &g.allowed["CreditCheck"];
        assert!(from_cc.contains("Approve"));
        assert!(from_cc.contains("Reject"));
        // Approve / Reject lead only to end events — no further task.
        assert!(g.allowed["Approve"].is_empty());
        // The model opens on CreditCheck and closes on Approve or Reject.
        assert!(g.start_tasks.contains("CreditCheck"));
        assert!(g.end_tasks.contains("Approve") && g.end_tasks.contains("Reject"));
        assert!(!g.end_tasks.contains("CreditCheck"));
    }

    // ── Structured authoring: serializer round-trip + edit_model operations ──────────────────

    /// Structural equivalence ignoring the verbatim `xml` field (which the serializer rewrites).
    fn assert_same_structure(a: &ProcessDefinition, b: &ProcessDefinition) {
        assert_eq!(a.id, b.id, "process id");
        assert_eq!(a.start_event, b.start_event, "start event");
        assert_eq!(a.elements, b.elements, "elements");
    }

    #[test]
    fn definition_to_xml_round_trips_the_loan_model() {
        // serialize -> re-parse must reproduce the exact structure (nodes, kinds, job types,
        // gateway split with a guarded branch + a default, error declarations).
        let (orig, _) = first_def(LOAN_BPMN).expect("parse loan");
        let xml = definition_to_xml(&orig);
        let reparsed = parse_bpmn(&xml).expect("serialized model re-parses");
        assert_same_structure(&orig, &reparsed[0]);
        // The guarded branch's FEEL condition (with an escaped '>=') survives the round trip.
        let decision = &reparsed[0].elements["Decision"];
        assert!(decision
            .outgoing
            .iter()
            .any(|f| f.to == "Approve" && f.condition.is_some()));
    }

    #[test]
    fn definition_to_xml_round_trips_an_error_boundary() {
        // The healed Investigation-1 shape: a serviceTask with an error boundary routing to an
        // error end. Round-tripping must re-synthesize the <bpmn:error> declaration + errorRef.
        let healed = normalize_authoring(ERROR_BOUNDARY_MISSPELLED).0;
        let (orig, _) = first_def(&healed).expect("parse healed");
        let xml = definition_to_xml(&orig);
        assert!(xml.contains("<bpmn:error "), "emits an error declaration");
        let reparsed = parse_bpmn(&xml).expect("re-parses");
        assert_same_structure(&orig, &reparsed[0]);
    }

    #[test]
    fn job_type_binding_hints_flags_task_id_as_job_type() {
        // A serviceTask with NO parseable taskDefinition child: the engine defaults its
        // job type to the element id. When simulate reports that id as uncovered, the
        // hint must fire and name the task.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Defs">
  <bpmn:process id="P" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Task_CreditCheck" />
    <bpmn:serviceTask id="Task_CreditCheck" name="Credit Check">
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Task_CreditCheck" targetRef="End" />
    <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;
        let hints = job_type_binding_hints(xml, &["Task_CreditCheck".to_string()]);
        assert_eq!(hints.len(), 1, "exactly one binding hint expected");
        assert_eq!(hints[0]["code"], "job-type-defaulted-to-task-id");
        assert_eq!(hints[0]["element"], "Task_CreditCheck");
    }

    #[test]
    fn job_type_binding_hints_silent_when_job_type_is_bound() {
        // A correctly-bound serviceTask whose job type differs from its id must not
        // trip the hint, even if its (real) job type is uncovered for other reasons.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Defs">
  <bpmn:process id="P" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Task_CreditCheck" />
    <bpmn:serviceTask id="Task_CreditCheck" name="Credit Check">
      <bpmn:extensionElements><zeebe:taskDefinition type="credit-check" /></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Task_CreditCheck" targetRef="End" />
    <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;
        let hints = job_type_binding_hints(xml, &["credit-check".to_string()]);
        assert!(hints.is_empty(), "bound job type must not trip the hint");
    }

    #[test]
    fn edit_model_sets_a_task_job_type() {
        let ops =
            vec![json!({"op":"set_task_job_type","task":"CreditCheck","jobType":"bureau-pull"})];
        let v = edit_model(LOAN_BPMN, &ops).expect("edit");
        assert_eq!(v["ok"], true);
        let xml = v["model"].as_str().unwrap();
        let def = first_def(xml).unwrap().0;
        match &def.elements["CreditCheck"].kind {
            ElementKind::ServiceTask { job_type, .. } => assert_eq!(job_type, "bureau-pull"),
            _ => panic!("CreditCheck should still be a serviceTask"),
        }
    }

    #[test]
    fn edit_model_inserts_a_service_task_after() {
        let ops = vec![json!({
            "op":"insert_service_task_after","after":"CreditCheck","id":"FraudCheck","jobType":"fraud-check"
        })];
        let v = edit_model(LOAN_BPMN, &ops).expect("edit");
        let xml = v["model"].as_str().unwrap();
        let def = first_def(xml).unwrap().0;
        // CreditCheck now flows into the new task, which flows on to the original target (Decision).
        let cc = &def.elements["CreditCheck"];
        assert!(cc.outgoing.iter().any(|f| f.to == "FraudCheck"));
        let fc = &def.elements["FraudCheck"];
        assert!(fc.outgoing.iter().any(|f| f.to == "Decision"));
    }

    #[test]
    fn edit_model_adds_an_error_boundary_that_parses() {
        // Investigation 1's intent, expressed as a validated op instead of hand-written XML.
        let base = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" targetNamespace="t">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="S"/>
    <bpmn:serviceTask id="Credit"><bpmn:extensionElements><zeebe:taskDefinition type="credit-check"/></bpmn:extensionElements></bpmn:serviceTask>
    <bpmn:endEvent id="Done"/>
    <bpmn:endEvent id="Err"/>
    <bpmn:sequenceFlow id="a" sourceRef="S" targetRef="Credit"/>
    <bpmn:sequenceFlow id="b" sourceRef="Credit" targetRef="Done"/>
  </bpmn:process>
</bpmn:definitions>"#;
        let ops = vec![json!({
            "op":"add_error_boundary","task":"Credit","errorCode":"CREDIT_BUREAU_ERROR","target":"Err"
        })];
        let v = edit_model(base, &ops).expect("edit");
        let xml = v["model"].as_str().unwrap();
        let def = first_def(xml).unwrap().0;
        let boundary = def
            .elements
            .values()
            .find(|e| matches!(&e.kind, ElementKind::ErrorBoundaryEvent { .. }))
            .expect("an error boundary was added");
        match &boundary.kind {
            ElementKind::ErrorBoundaryEvent {
                attached_to,
                error_code,
            } => {
                assert_eq!(attached_to, "Credit");
                assert_eq!(error_code, "CREDIT_BUREAU_ERROR");
            }
            _ => unreachable!(),
        }
        assert!(boundary.outgoing.iter().any(|f| f.to == "Err"));
    }

    #[test]
    fn edit_model_removes_a_node_and_heals_flows() {
        // Remove the gateway: its predecessor (CreditCheck) should reconnect to the gateway's
        // successors (Approve, Reject), so no dangling reference remains.
        let ops = vec![json!({"op":"remove_node","id":"Decision"})];
        let v = edit_model(LOAN_BPMN, &ops).expect("edit");
        let xml = v["model"].as_str().unwrap();
        let def = first_def(xml).unwrap().0;
        assert!(!def.elements.contains_key("Decision"));
        let cc = &def.elements["CreditCheck"];
        assert!(cc.outgoing.iter().any(|f| f.to == "Approve"));
        assert!(cc.outgoing.iter().any(|f| f.to == "Reject"));
    }

    #[test]
    fn edit_model_adds_an_exclusive_gateway() {
        // Splice a decision gateway after CreditCheck, branching to Approve (guarded) with the
        // original path (to Decision) preserved as the default branch.
        let ops = vec![json!({
            "op":"add_exclusive_gateway","id":"Triage","after":"CreditCheck",
            "branches":[{"to":"Approve","condition":"= score > 800"}]
        })];
        let v = edit_model(LOAN_BPMN, &ops).expect("edit");
        let xml = v["model"].as_str().unwrap();
        let def = first_def(xml).unwrap().0;
        assert!(matches!(
            def.elements["Triage"].kind,
            ElementKind::ExclusiveGateway
        ));
        let cc = &def.elements["CreditCheck"];
        assert!(cc.outgoing.iter().any(|f| f.to == "Triage"));
        let triage = &def.elements["Triage"];
        assert!(triage
            .outgoing
            .iter()
            .any(|f| f.to == "Approve" && f.condition.is_some()));
        // The original CreditCheck -> Decision path is preserved through the gateway as default.
        assert!(triage
            .outgoing
            .iter()
            .any(|f| f.to == "Decision" && f.condition.is_none()));
    }

    #[test]
    fn edit_model_auto_heals_the_base_then_applies_ops() {
        // The base uses the bogus <errorBoundaryEvent>; edit_model heals it before editing, so a
        // job-type tweak still succeeds (and the heal is reported in appliedOps).
        let ops =
            vec![json!({"op":"set_task_job_type","task":"Task_CreditCheck","jobType":"bureau"})];
        let v = edit_model(ERROR_BOUNDARY_MISSPELLED, &ops).expect("edit heals + applies");
        let applied = v["appliedOps"].as_array().unwrap();
        assert!(applied
            .iter()
            .any(|n| n.as_str().unwrap().contains("errorBoundaryEvent")));
        let xml = v["model"].as_str().unwrap();
        assert!(!xml.contains("errorBoundaryEvent"));
        parse_bpmn(xml).expect("edited model parses");
    }

    #[test]
    fn edit_model_rejects_an_unknown_node_with_a_clear_error() {
        let ops = vec![json!({"op":"set_task_job_type","task":"Nope","jobType":"x"})];
        let err = edit_model(LOAN_BPMN, &ops).expect_err("should fail");
        assert!(err.contains("op 1 failed"), "got: {err}");
        assert!(err.contains("Nope"), "names the missing node: {err}");
    }
}
