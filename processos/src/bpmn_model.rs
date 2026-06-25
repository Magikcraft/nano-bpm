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
use nanobpmn_engine_core::{Element, ElementKind, ProcessDefinition};
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
fn reachable_from_start(def: &ProcessDefinition, adj: &HashMap<String, Vec<String>>) -> HashSet<String> {
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
fn loop_components(def: &ProcessDefinition, adj: &HashMap<String, Vec<String>>) -> Vec<Vec<String>> {
    // Index the nodes deterministically.
    let ids: Vec<String> = {
        let mut v: Vec<String> = def.elements.keys().cloned().collect();
        v.sort();
        v
    };
    let index_of: HashMap<&str, usize> = ids.iter().enumerate().map(|(i, s)| (s.as_str(), i)).collect();
    let n = ids.len();
    let neighbours: Vec<Vec<usize>> = ids
        .iter()
        .map(|id| {
            adj.get(id)
                .map(|tos| tos.iter().filter_map(|t| index_of.get(t.as_str()).copied()).collect())
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
                    .map(|e| matches!(e.kind, ElementKind::ExclusiveGateway) && e.outgoing.len() > 1)
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
    let warns = findings
        .iter()
        .filter(|f| f["severity"] == "warn")
        .count();
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
        Some(rest) => rest.find("?>").map(|i| rest[i + 2..].trim_start()).unwrap_or(trimmed),
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
    None
}

/// `validate_model` — a cheap, dataset-independent **lint/validate** pass over a candidate BPMN
/// model. It parses the XML with the engine's own parser (the same one production deploys with),
/// surfacing structural mistakes the LLM commonly makes — a dangling `errorRef`, a missing
/// `<bpmn:definitions>` root, unparseable XML — before the model is ever simulated or deployed. A
/// bare `<bpmn:process>` fragment is wrapped for analysis (with a warning to emit a full document).
/// On a clean parse it folds in the full [`analyze_model`] structural findings.
pub fn validate_model(xml: &str) -> Result<Value, String> {
    let (doc, wrapped) = ensure_definitions(xml);
    match analyze_model(&doc) {
        Ok(mut v) => {
            if let Some(obj) = v.as_object_mut() {
                obj.insert("valid".into(), Value::Bool(true));
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
                    if let Some(w) = obj.get("warnings").and_then(|n| n.as_u64()) {
                        obj.insert("warnings".into(), json!(w + 1));
                    }
                    obj.insert("findingCount".into(), json!(obj.get("findings").and_then(|f| f.as_array()).map(|a| a.len()).unwrap_or(0)));
                    obj.insert("wrapped".into(), Value::Bool(true));
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
            Ok(json!({
                "valid": false,
                "parseError": parse_error,
                "fix": fix,
                "findingCount": 1,
                "warnings": 0,
                "infos": 0,
                "findings": [finding("error", "parse-error", None, message)],
                "note": "The model does not parse and cannot be deployed or simulated. Fix the \
                         error above and re-run validate_model.",
            }))
        }
    }
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
        let loop_finding = findings.iter().find(|f| f["code"] == "rework-loop").unwrap();
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
        assert!(findings.iter().any(|f| f["code"] == "missing-definitions-root"));
    }

    #[test]
    fn validate_model_passes_a_clean_document() {
        let v = validate_model(LOAN_BPMN).expect("validate");
        assert_eq!(v["valid"], true);
        assert!(v.get("wrapped").is_none() || v["wrapped"] == false);
        assert_eq!(v["processId"], "loan-approval");
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
}
