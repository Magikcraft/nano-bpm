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
pub mod schema;
pub mod solver;

#[allow(unused_imports)] // re-exported public API
pub use schema::{AnnotatedFlow, Cluster, FlowKind, Role, SemanticAnnotations};

use nanobpmn_engine_core::bpmn::parse_bpmn;

use crate::bpmn_model::definition_to_xml_with_row_bias;

/// End-to-end: `(bpmn_xml, annotations) -> (bpmn_xml_with_semantic_di, debug_svg)`.
///
/// The output BPMN is a full, engine-parseable document — a caller can hand
/// it straight to any BPMN renderer or the Nano gateway's deployment endpoint.
/// The debug SVG is a separate artifact intended for human eyeballing.
pub fn layout(xml: &str, ann: &SemanticAnnotations) -> Result<LayoutOutput, String> {
    let defs = parse_bpmn(xml).map_err(|e| format!("parse_bpmn: {e:?}"))?;
    // parse_bpmn returns a Vec<ProcessDefinition>; v0 handles the first one
    // (the only case the existing serializer supports). Multi-process
    // definitions can layer semantic layout later.
    let def = defs
        .into_iter()
        .next()
        .ok_or_else(|| "no process definition in BPMN document".to_string())?;
    let bias = solver::compute_row_bias(ann);
    let out_xml = definition_to_xml_with_row_bias(&def, &std::collections::HashMap::new(), &bias);
    let svg = debug_svg::render_debug_svg(&out_xml, ann);
    Ok(LayoutOutput {
        bpmn_xml: out_xml,
        debug_svg: svg,
    })
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
