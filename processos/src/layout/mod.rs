//! **Semantic BPMN layout** — an experimental replacement for the standard
//! left-to-right auto-layout when the LLM that authored (or classified) the
//! model can tell us which nodes belong to the happy path, which handle
//! errors, which escalate, and which compensate.
//!
//! ## Pipeline
//! ```text
//!   BPMN xml + SemanticAnnotations
//!         │
//!         ▼
//!   parse (nanobpmn_engine_core::bpmn::parse_bpmn)
//!         │
//!         ▼
//!   compute_row_bias  ← the semantic contribution lives here
//!         │
//!         ▼
//!   definition_to_xml_with_row_bias
//!     (existing Sugiyama rank + orthogonal routing +
//!      DI emit, biased by the row map)
//!         │
//!         ├──► BPMN xml with new DI
//!         └──► render_debug_svg  (band overlays, cluster hulls, colour-coded nodes)
//! ```
//!
//! ## Why this shape
//! Processos already ships a Sugiyama-ish auto-layout with orthogonal edge
//! routing (see [`crate::bpmn_model::append_diagram`]). The v0 semantic layer
//! is a *y-row bias* — a `node_id -> preferred_row` map that overrides the
//! predecessor-mean heuristic *only for the nodes the LLM annotated*. Every
//! other layout concern (x-ranking, edge routing, label placement) stays under
//! the existing solver.
//!
//! This deliberately keeps the semantic surface tiny (see [`schema`]) so the
//! LLM iteration loop is fast: change the prompt → regenerate annotations →
//! re-run layout → eyeball the debug SVG.

pub mod debug_svg;
pub mod field;
pub mod schema;
pub mod solver;

use nanobpmn_engine_core::bpmn::parse_bpmn;
#[allow(unused_imports)] // re-exported public API
pub use schema::{AnnotatedFlow, Cluster, FlowKind, Role, SemanticAnnotations};

use crate::bpmn_model::definition_to_xml_with_row_bias;

/// Which layout solver to use. `RowBias` is the deterministic
/// Sugiyama-adjacent row-bias projection (production default). `Field` is
/// the experimental unified charged-particle simulation — same semantic
/// annotations, very different aesthetics. See [`field`] for the physics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Solver {
    RowBias,
    Field,
}

impl Solver {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "rowbias" | "row-bias" | "row_bias" => Ok(Solver::RowBias),
            "field" | "physics" => Ok(Solver::Field),
            other => Err(format!(
                "unknown solver '{other}'; expected 'rowbias' or 'field'"
            )),
        }
    }
}

/// End-to-end using the default (row-bias) solver.
pub fn layout(xml: &str, ann: &SemanticAnnotations) -> Result<LayoutOutput, String> {
    layout_with(xml, ann, Solver::RowBias)
}

/// End-to-end with an explicitly chosen solver. Output shape is identical
/// regardless of solver.
pub fn layout_with(
    xml: &str,
    ann: &SemanticAnnotations,
    solver: Solver,
) -> Result<LayoutOutput, String> {
    let defs = parse_bpmn(xml).map_err(|e| format!("parse_bpmn: {e:?}"))?;
    // parse_bpmn returns a Vec<ProcessDefinition>; v0 handles the first one.
    let def = defs
        .into_iter()
        .next()
        .ok_or_else(|| "no process definition in BPMN document".to_string())?;
    let (out_xml, diagnostics) = match solver {
        Solver::RowBias => {
            let bias = solver::compute_row_bias(ann);
            let xml =
                definition_to_xml_with_row_bias(&def, &std::collections::HashMap::new(), &bias);
            (xml, None)
        }
        Solver::Field => {
            let field_out = field::simulate(&def, ann);
            let xml = emit_bpmn_from_field(&def, &field_out);
            (xml, Some(field_out.diagnostics))
        }
    };
    let svg = debug_svg::render_debug_svg(&out_xml, ann);
    Ok(LayoutOutput {
        bpmn_xml: out_xml,
        debug_svg: svg,
        field_diagnostics: diagnostics,
    })
}

/// Convert a settled field simulation into a BPMN XML document. Field
/// positions become BPMNShape bounds; settled edge chains become BPMNEdge
/// waypoints *verbatim* — no post-orthogonalisation in this pass, since the
/// point of the emergent-orthogonality experiment is to see what actually
/// settles rather than snap it to a grid.
fn emit_bpmn_from_field(
    def: &nanobpmn_engine_core::ProcessDefinition,
    out: &field::FieldOutput,
) -> String {
    use std::fmt::Write as _;
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    s.push_str(
        "<bpmn:definitions xmlns:bpmn=\"http://www.omg.org/spec/BPMN/20100524/MODEL\" \
         xmlns:bpmndi=\"http://www.omg.org/spec/BPMN/20100524/DI\" \
         xmlns:dc=\"http://www.omg.org/spec/DD/20100524/DC\" \
         xmlns:di=\"http://www.omg.org/spec/DD/20100524/DI\" ",
    );
    let _ = writeln!(
        s,
        "id=\"Definitions_{id}\" targetNamespace=\"http://bpmn.io/schema/bpmn\">",
        id = def.id
    );
    let _ = writeln!(
        s,
        "  <bpmn:process id=\"{}\" isExecutable=\"true\"/>",
        def.id
    );
    let _ = writeln!(s, "  <bpmndi:BPMNDiagram id=\"BPMNDiagram_1\">");
    let _ = writeln!(
        s,
        "    <bpmndi:BPMNPlane id=\"BPMNPlane_1\" bpmnElement=\"{}\">",
        def.id
    );
    for id in def.elements.keys() {
        if let Some(&(cx, cy)) = out.nodes.get(id) {
            let (w, h) = field_node_dims(&def.elements[id]);
            let _ = writeln!(
                s,
                "      <bpmndi:BPMNShape id=\"{0}_di\" bpmnElement=\"{0}\">\n\
                 \x20       <dc:Bounds x=\"{1:.0}\" y=\"{2:.0}\" width=\"{3:.0}\" height=\"{4:.0}\"/>\n\
                 \x20     </bpmndi:BPMNShape>",
                id,
                cx - w * 0.5,
                cy - h * 0.5,
                w,
                h,
            );
        }
    }
    for (src_id, el) in &def.elements {
        for f in &el.outgoing {
            let edge_id = format!("{}__{}", src_id, f.to);
            if let Some(waypoints) = out.edges.get(&edge_id) {
                let _ = writeln!(
                    s,
                    "      <bpmndi:BPMNEdge id=\"{eid}_di\" bpmnElement=\"{eid}\">",
                    eid = edge_id
                );
                for &(x, y) in waypoints {
                    let _ = writeln!(s, "        <di:waypoint x=\"{x:.0}\" y=\"{y:.0}\"/>");
                }
                let _ = writeln!(s, "      </bpmndi:BPMNEdge>");
            }
        }
    }
    let _ = writeln!(s, "    </bpmndi:BPMNPlane>");
    let _ = writeln!(s, "  </bpmndi:BPMNDiagram>");
    s.push_str("</bpmn:definitions>\n");
    s
}

fn field_node_dims(el: &nanobpmn_engine_core::Element) -> (f64, f64) {
    use nanobpmn_engine_core::ElementKind::*;
    match el.kind {
        StartEvent
        | EndEvent
        | IntermediateThrowEvent
        | TimerIntermediateCatchEvent { .. }
        | MessageIntermediateCatchEvent { .. }
        | SignalIntermediateCatchEvent { .. }
        | ConditionalIntermediateCatchEvent { .. }
        | MessageStartEvent { .. }
        | TimerStartEvent { .. }
        | ErrorBoundaryEvent { .. }
        | TimerBoundaryEvent { .. }
        | MessageBoundaryEvent { .. }
        | SignalBoundaryEvent { .. }
        | ConditionalBoundaryEvent { .. } => (36.0, 36.0),
        ExclusiveGateway | ParallelGateway => (50.0, 50.0),
        _ => (110.0, 80.0),
    }
}

/// Result of a semantic layout run.
pub struct LayoutOutput {
    /// BPMN 2.0 XML with an auto-generated `<bpmndi:BPMNDiagram>` reflecting
    /// the semantic bands.
    pub bpmn_xml: String,
    /// A standalone SVG debug view — band backgrounds, cluster hulls, and
    /// nodes coloured by their annotated flow kind. Never embedded in the
    /// BPMN document; write to a sidecar file for inspection.
    pub debug_svg: String,
    /// Present only when the field solver was used — steps to convergence,
    /// pinned oscillators, final kinetic energy. Handy for debugging the sim
    /// without instrumenting it.
    pub field_diagnostics: Option<field::SimDiagnostics>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// End-to-end smoke: give the pipeline a trivial 3-node BPMN and confirm
    /// the annotated exception node lands *below* its (primary) predecessor
    /// in the emitted DI — the whole point of the feature.
    #[test]
    fn exception_lands_below_primary_predecessor() {
        let bpmn = include_str!("../../fixtures/layout/tiny.bpmn");
        let ann_json = include_str!("../../fixtures/layout/tiny.annotations.json");
        let ann: SemanticAnnotations = serde_json::from_str(ann_json).expect("annotations parse");
        let out = layout(bpmn, &ann).expect("layout runs");

        let y_of = |id: &str| -> f64 {
            let needle = format!("bpmnElement=\"{id}\"");
            let s = out.bpmn_xml.find(&needle).expect("shape present");
            let after = &out.bpmn_xml[s..];
            let ys = after.find("y=\"").expect("y attr") + 3;
            let ye = after[ys..].find('"').expect("y close") + ys;
            after[ys..ye].parse().expect("y numeric")
        };

        let happy = y_of("TaskB");
        let handle = y_of("HandleError");
        assert!(
            handle > happy + 50.0,
            "exception node HandleError (y={handle}) should drop below happy-path TaskB (y={happy})"
        );
    }
}
