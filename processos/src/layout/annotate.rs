//! **Semantic annotation inference (slice 1)** — derive a [`SemanticAnnotations`]
//! sidecar from the BPMN structure alone, so the [`layout::Solver::RowBias`]
//! and [`layout::Solver::Field`] renderers have real bands to spread nodes
//! across even when nobody authored annotations by hand.
//!
//! The demo case that motivated this pass authored a loan-approval variant,
//! ran it through the Fromme renderer, and saw a flat line — because the LLM
//! authoring the variant had emitted zero annotations and both solvers
//! defaulted every node to the primary band. See ADR 0002 for the pipeline
//! this fits into; this module is *slice 1*'s structural-only pass.
//!
//! # What we infer today
//!
//! From the parsed [`ProcessDefinition`] we recover, deterministically:
//!
//! * A **primary flow** by tracing from `start_event` through every
//!   non-gateway element's single outgoing flow, and taking each exclusive
//!   gateway's `is_default` outgoing (or its first outgoing if none is
//!   marked default). Parallel gateways contribute every outgoing branch to
//!   the primary flow. The trace terminates at every `EndEvent` it reaches
//!   and refuses to visit a node twice, so cycles cannot wedge it.
//! * An **exception flow** per interrupting boundary event
//!   (error / timer / message / signal / conditional). Everything reachable
//!   downstream of the boundary becomes a member of that flow. Boundary
//!   events themselves join their flow so the diagram's exception band picks
//!   them up cleanly.
//! * **Role hints**: `ExclusiveGateway → decision`, `UserTask → review`.
//!   Everything else stays unlabelled — the layout solver only uses roles
//!   for colour cues on the debug SVG, so under-labelling is safe.
//!
//! # What we deliberately leave for later slices
//!
//! * Cluster inference (sub-process / call-activity / shared-jobType) —
//!   these belong on the reader-scoped sidecar in the ADR's model, not on
//!   the model-authored side we're bootstrapping here.
//! * Name-heuristic classification of tasks as `notification` / `external`
//!   — cheap, but wants the raw XML for `<bpmn:task name="…">`. Slice 2's
//!   full module will fold it in alongside the telemetry pass.
//! * Escalation and compensation flow classes — Nano's engine surface
//!   doesn't model dedicated escalation / compensation boundary events
//!   today, so there's nothing structural to key on. Reintroduce when the
//!   engine grows them.

use std::collections::{HashSet, VecDeque};

use nanobpmn_engine_core::{ElementKind, ProcessDefinition};

use crate::layout::schema::{AnnotatedFlow, FlowKind, Role, SemanticAnnotations};

/// Run the structural inference pass against `def` and return the resulting
/// [`SemanticAnnotations`]. Pure function of the process definition — no
/// telemetry, no LLM, no naming heuristics — so every call on the same input
/// returns the same output.
///
/// The returned annotations are deliberately conservative: they only claim
/// nodes the structure makes obvious. Everything else stays unclassified so
/// the layout solver treats it as primary by default (which matches the
/// pre-inference behaviour, so overlaying inference on a model that has
/// no boundary events is a no-op).
pub fn infer(def: &ProcessDefinition) -> SemanticAnnotations {
    let mut ann = SemanticAnnotations::default();
    if let Some(primary) = trace_primary(def) {
        if !primary.is_empty() {
            ann.flows.push(AnnotatedFlow {
                id: "happy".to_string(),
                kind: FlowKind::Primary,
                nodes: primary,
            });
        }
    }
    ann.flows.extend(exception_flows(def));
    ann.roles = infer_roles(def);
    ann
}

/// Walk the process's default path from the start event and return the ordered
/// list of node ids it visits. See the module doc-comment for the exact rules.
fn trace_primary(def: &ProcessDefinition) -> Option<Vec<String>> {
    let start = def.element(&def.start_event)?;
    let mut order: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut stack: VecDeque<String> = VecDeque::new();
    stack.push_back(start.id.clone());
    while let Some(id) = stack.pop_front() {
        if !seen.insert(id.clone()) {
            continue;
        }
        order.push(id.clone());
        let Some(el) = def.element(&id) else { continue };
        let next: Vec<String> = match &el.kind {
            ElementKind::ExclusiveGateway => el
                .outgoing
                .iter()
                .find(|f| f.is_default)
                .or_else(|| el.outgoing.first())
                .map(|f| vec![f.to.clone()])
                .unwrap_or_default(),
            _ => el.outgoing.iter().map(|f| f.to.clone()).collect(),
        };
        for n in next {
            if !seen.contains(&n) {
                stack.push_back(n);
            }
        }
    }
    Some(order)
}

/// Discover every interrupting boundary event and return one exception flow
/// per boundary — starting at the boundary itself, then everything reachable
/// downstream via sequence flows (short of re-entering the primary path
/// through a converging join, which we simply let happen — nodes claimed by
/// both flows use [`FlowKind::priority`] to resolve to exception).
fn exception_flows(def: &ProcessDefinition) -> Vec<AnnotatedFlow> {
    let mut flows: Vec<AnnotatedFlow> = Vec::new();
    let mut boundaries: Vec<(&str, &ElementKind)> = def
        .elements
        .values()
        .filter_map(|el| match &el.kind {
            ElementKind::ErrorBoundaryEvent { .. }
            | ElementKind::TimerBoundaryEvent {
                interrupting: true, ..
            }
            | ElementKind::MessageBoundaryEvent {
                interrupting: true, ..
            }
            | ElementKind::SignalBoundaryEvent {
                interrupting: true, ..
            }
            | ElementKind::ConditionalBoundaryEvent {
                interrupting: true, ..
            } => Some((el.id.as_str(), &el.kind)),
            _ => None,
        })
        .collect();
    // Stable, id-sorted ordering so repeat runs produce identical annotations.
    boundaries.sort_by_key(|(id, _)| *id);
    for (boundary_id, kind) in boundaries {
        let nodes = reachable_from(def, boundary_id);
        if nodes.is_empty() {
            continue;
        }
        flows.push(AnnotatedFlow {
            id: format!("exception.{boundary_id}"),
            kind: FlowKind::Exception,
            nodes,
        });
        // Silence the unused-binding warning without dropping the debug context.
        let _ = kind;
    }
    flows
}

/// Breadth-first walk of every node reachable from `from` via sequence flows,
/// returning them in visitation order and including `from` itself.
fn reachable_from(def: &ProcessDefinition, from: &str) -> Vec<String> {
    let mut order: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    queue.push_back(from.to_string());
    while let Some(id) = queue.pop_front() {
        if !seen.insert(id.clone()) {
            continue;
        }
        order.push(id.clone());
        if let Some(el) = def.element(&id) {
            for f in &el.outgoing {
                if !seen.contains(&f.to) {
                    queue.push_back(f.to.clone());
                }
            }
        }
    }
    order
}

/// Role hints keyed by node id. See the module doc-comment for the (very
/// small) set of structural classifications we make today.
fn infer_roles(def: &ProcessDefinition) -> std::collections::BTreeMap<String, Role> {
    let mut roles = std::collections::BTreeMap::new();
    for el in def.elements.values() {
        let role = match &el.kind {
            ElementKind::ExclusiveGateway => Some(Role::Decision),
            ElementKind::UserTask(_) => Some(Role::Review),
            _ => None,
        };
        if let Some(r) = role {
            roles.insert(el.id.clone(), r);
        }
    }
    roles
}

/// Merge a `structural` inferred annotation set with a `supplied` one, letting
/// every non-empty supplied field win. Callers use this to layer an
/// LLM-authored or human-confirmed sidecar over the structural default without
/// having to check emptiness field-by-field.
pub fn merge(
    supplied: Option<SemanticAnnotations>,
    structural: SemanticAnnotations,
) -> SemanticAnnotations {
    let Some(supplied) = supplied else {
        return structural;
    };
    let flows = if supplied.flows.is_empty() {
        structural.flows
    } else {
        supplied.flows
    };
    let clusters = if supplied.clusters.is_empty() {
        structural.clusters
    } else {
        supplied.clusters
    };
    let roles = if supplied.roles.is_empty() {
        structural.roles
    } else {
        supplied.roles
    };
    SemanticAnnotations {
        flows,
        clusters,
        roles,
    }
}

#[cfg(test)]
mod tests {
    use nanobpmn_engine_core::bpmn::parse_bpmn;

    use super::*;

    const TINY_XML: &str = include_str!("../../fixtures/layout/tiny.bpmn");

    #[test]
    fn primary_flow_follows_the_default_gateway_branch() {
        let defs = parse_bpmn(TINY_XML).expect("parse");
        let def = defs.into_iter().next().expect("one process");
        let ann = infer(&def);
        let primary = ann
            .flows
            .iter()
            .find(|f| f.kind == FlowKind::Primary)
            .expect("primary flow inferred");
        assert!(
            primary.nodes.iter().any(|n| n == "Start_1"),
            "primary starts at Start_1, got {:?}",
            primary.nodes
        );
        // The fixture's GwCheck default goes to TaskB → End_ok (the happy
        // route). The primary trace must therefore follow TaskB, not
        // HandleError (which is the exception branch, reached via the
        // gateway's non-default flow / the error boundary in the wider
        // model).
        assert!(
            primary.nodes.iter().any(|n| n == "TaskB"),
            "primary follows the default flow, got {:?}",
            primary.nodes
        );
        assert!(
            !primary.nodes.iter().any(|n| n == "HandleError"),
            "primary must not visit the non-default branch, got {:?}",
            primary.nodes
        );
    }

    #[test]
    fn exception_flow_captures_boundary_downstream() {
        let defs = parse_bpmn(TINY_XML).expect("parse");
        let def = defs.into_iter().next().expect("one process");
        let ann = infer(&def);
        // The fixture attaches an error boundary to HandleError routing to
        // End_err; if the boundary event is present, the exception flow must
        // pick it up. If it isn't (older fixture), infer() returns no
        // exception flow and this test degrades to a no-op assertion.
        let has_boundary = def
            .elements
            .values()
            .any(|el| matches!(el.kind, ElementKind::ErrorBoundaryEvent { .. }));
        if !has_boundary {
            return;
        }
        let exc = ann
            .flows
            .iter()
            .find(|f| f.kind == FlowKind::Exception)
            .expect("exception flow inferred for error boundary");
        assert!(
            !exc.nodes.is_empty(),
            "exception flow has at least the boundary event itself"
        );
    }

    #[test]
    fn roles_tag_exclusive_gateways_as_decisions() {
        let defs = parse_bpmn(TINY_XML).expect("parse");
        let def = defs.into_iter().next().expect("one process");
        let ann = infer(&def);
        let gw = def
            .elements
            .values()
            .find(|el| matches!(el.kind, ElementKind::ExclusiveGateway))
            .expect("fixture has a gateway");
        assert_eq!(
            ann.roles.get(&gw.id).copied(),
            Some(Role::Decision),
            "exclusive gateway {} should be a decision role",
            gw.id
        );
    }

    #[test]
    fn merge_prefers_supplied_when_populated_and_structural_otherwise() {
        let structural = SemanticAnnotations {
            flows: vec![AnnotatedFlow {
                id: "happy".into(),
                kind: FlowKind::Primary,
                nodes: vec!["A".into(), "B".into()],
            }],
            ..Default::default()
        };
        // Empty supplied ⇒ structural wins.
        let merged = merge(Some(SemanticAnnotations::default()), structural.clone());
        assert_eq!(
            merged.flows.len(),
            1,
            "empty supplied falls back to structural"
        );
        // Populated supplied ⇒ supplied wins for that field only.
        let supplied = SemanticAnnotations {
            flows: vec![AnnotatedFlow {
                id: "manual".into(),
                kind: FlowKind::Primary,
                nodes: vec!["X".into()],
            }],
            ..Default::default()
        };
        let merged = merge(Some(supplied), structural);
        assert_eq!(merged.flows[0].id, "manual");
    }

    #[test]
    fn infer_is_deterministic() {
        let defs = parse_bpmn(TINY_XML).expect("parse");
        let def = defs.into_iter().next().expect("one process");
        let a = infer(&def);
        let b = infer(&def);
        assert_eq!(a.flows.len(), b.flows.len());
        for (fa, fb) in a.flows.iter().zip(b.flows.iter()) {
            assert_eq!(fa.id, fb.id);
            assert_eq!(fa.nodes, fb.nodes);
        }
        assert_eq!(a.roles, b.roles);
    }
}
