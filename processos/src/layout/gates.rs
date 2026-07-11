//! **Correctness gates** — coverage + overlap ratio. Ported from the bake-off
//! `config/default.config.js`'s `GATES` block (`minCoverage: 0.95`,
//! `maxOverlapRatio: 0.08`, `penalty: 0.35`).
//!
//! The point of these gates is to prevent a solver from "winning" the
//! conformance / crossings competition by drawing fewer elements or piling
//! them on top of each other: coverage counts declared vs drawn shapes,
//! overlap counts the summed pairwise-overlap area between leaf nodes
//! divided by the total leaf area. When either gate fails, callers are
//! expected to multiply the scored quality by [`PENALTY`] (or, in the polish
//! pass's case, reject the candidate entirely).

use crate::layout::geom::{rect_overlap_area, Geom};

/// Threshold below which coverage counts as broken. Matches the bake-off.
pub const MIN_COVERAGE: f64 = 0.95;

/// Above this leaf-overlap ratio the layout is unusable regardless of any
/// other metric — matches the bake-off's `maxOverlapRatio`.
pub const MAX_OVERLAP_RATIO: f64 = 0.08;

/// Multiplicative penalty applied to the composite quality score when any
/// gate fails. Matches the bake-off's `penalty: 0.35`.
pub const PENALTY: f64 = 0.35;

#[derive(Debug, Clone, serde::Serialize)]
pub struct Gates {
    /// `drawn / declared` shape ratio in `[0, 1]`.
    pub coverage: f64,
    /// Sum of pairwise overlap area between leaf nodes divided by the total
    /// leaf area. Zero when no leaves collide.
    pub overlap_ratio: f64,
    /// True when both gates pass.
    pub passed: bool,
    /// `PENALTY` when either gate fails, else `1.0` — multiply the composite
    /// score by this to enforce the gate.
    pub penalty: f64,
    /// Reason strings for any failed gate, empty when everything passed.
    pub failures: Vec<String>,
}

/// Evaluate both gates against a laid-out geometry.
///
/// `declared_shape_count` is what the source BPMN said the process contains
/// (typically the length of `def.elements`); the caller is responsible for
/// counting, so this module stays dependency-free.
pub fn evaluate(geom: &Geom, declared_shape_count: usize) -> Gates {
    let drawn = geom.nodes.len().min(declared_shape_count.max(1));
    let coverage = if declared_shape_count == 0 {
        1.0
    } else {
        drawn as f64 / declared_shape_count as f64
    };

    let leaves: Vec<_> = geom.leaves().collect();
    let leaf_area: f64 = leaves.iter().map(|n| n.w * n.h).sum();
    let mut overlap = 0.0;
    for i in 0..leaves.len() {
        for j in (i + 1)..leaves.len() {
            overlap += rect_overlap_area(leaves[i], leaves[j]);
        }
    }
    let overlap_ratio = if leaf_area > 0.0 {
        overlap / leaf_area
    } else {
        0.0
    };

    let mut failures: Vec<String> = Vec::new();
    if coverage < MIN_COVERAGE {
        failures.push(format!(
            "coverage {:.2} < {:.2} (drew {} of {} declared shapes)",
            coverage, MIN_COVERAGE, drawn, declared_shape_count
        ));
    }
    if overlap_ratio > MAX_OVERLAP_RATIO {
        failures.push(format!(
            "overlap {:.3} > {:.3} (leaf area {:.0}, overlap area {:.0})",
            overlap_ratio, MAX_OVERLAP_RATIO, leaf_area, overlap
        ));
    }
    let passed = failures.is_empty();
    Gates {
        coverage,
        overlap_ratio,
        passed,
        penalty: if passed { 1.0 } else { PENALTY },
        failures,
    }
}

/// Convenience — the ratio-only computation, used by the polish pass to
/// decide "did this candidate make things worse".
pub fn overlap_ratio(geom: &Geom) -> f64 {
    let leaves: Vec<_> = geom.leaves().collect();
    let leaf_area: f64 = leaves.iter().map(|n| n.w * n.h).sum();
    if leaf_area == 0.0 {
        return 0.0;
    }
    let mut overlap = 0.0;
    for i in 0..leaves.len() {
        for j in (i + 1)..leaves.len() {
            overlap += rect_overlap_area(leaves[i], leaves[j]);
        }
    }
    overlap / leaf_area
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::geom::parse;

    #[test]
    fn tiny_fixture_passes_all_gates() {
        let bpmn = include_str!("../../fixtures/layout/tiny.bpmn");
        let g = parse(bpmn);
        let gates = evaluate(&g, g.nodes.len());
        assert!(gates.passed, "gates should pass: {:?}", gates.failures);
        assert_eq!(gates.penalty, 1.0);
        assert_eq!(gates.overlap_ratio, 0.0);
    }

    #[test]
    fn low_coverage_flags_penalty() {
        let bpmn = include_str!("../../fixtures/layout/tiny.bpmn");
        let g = parse(bpmn);
        // Pretend 20 shapes were declared but only ~4 were drawn.
        let gates = evaluate(&g, 20);
        assert!(!gates.passed);
        assert_eq!(gates.penalty, PENALTY);
    }
}
