//! **Crash-guarded engine wrapper** — port of the bake-off's
//! `src/engines/engine.contract.js` pattern for our two solvers.
//!
//! The rule is: an experimental solver should never take down the request.
//! A `panic!` from the Field solver's simulation or an infinite spin from a
//! future engine returns a clearly-marked fallback layout instead of a 500.
//!
//! Guarantees, in decreasing order of preference:
//!
//! 1. Run the requested solver. If it returns and the resulting BPMN has at
//!    least [`MIN_COVERAGE`](crate::layout::gates::MIN_COVERAGE) coverage
//!    against the declared element count, return its output.
//! 2. If the requested solver panics OR fails the coverage gate, retry with
//!    [`Solver::RowBias`](crate::layout::Solver::RowBias) (the deterministic
//!    baseline).
//! 3. If even RowBias panics — which shouldn't happen; it's the reference
//!    implementation — surface the underlying error so the operator sees it.
//!
//! Wall-clock budgets are not enforced here today (no `tokio::time::timeout`
//! wrapping): our solvers run in <100ms for realistic inputs and the CLI
//! path is synchronous. The wall-clock check is left as a marker for a
//! future async migration.

use std::panic::AssertUnwindSafe;

use nanobpmn_engine_core::bpmn::parse_bpmn;

use crate::layout::gates::{self, MIN_COVERAGE};
use crate::layout::geom;
use crate::layout::schema::SemanticAnnotations;
use crate::layout::{layout_with, LayoutOutput, Solver};

/// Reason we fell back to a safer solver, if we did.
#[derive(Debug, Clone, serde::Serialize)]
pub enum FallbackReason {
    /// Requested solver panicked mid-run.
    Panic {
        requested: &'static str,
        message: String,
    },
    /// Requested solver returned, but coverage was below the gate.
    LowCoverage {
        requested: &'static str,
        coverage: f64,
    },
}

/// Result of a guarded layout call.
pub struct GuardedOutput {
    pub output: LayoutOutput,
    /// The solver whose output is being returned — will differ from the
    /// requested solver when we fell back to RowBias.
    pub used_solver: Solver,
    /// Populated only when a fallback happened; otherwise `None`.
    pub fallback: Option<FallbackReason>,
}

/// Run `solver` against `xml`, falling back to RowBias on panic or coverage
/// failure. `declared_shape_count` is the number of BPMN elements in the
/// source model — used to detect solvers that "win by drawing less".
pub fn run(
    xml: &str,
    ann: &SemanticAnnotations,
    solver: Solver,
    declared_shape_count: usize,
) -> Result<GuardedOutput, String> {
    match try_solver(xml, ann, solver, declared_shape_count) {
        Ok(out) => Ok(GuardedOutput {
            output: out,
            used_solver: solver,
            fallback: None,
        }),
        Err(reason) => {
            if matches!(solver, Solver::RowBias) {
                // Nothing safer to fall back to.
                return Err(format!("{reason:?}"));
            }
            // Second attempt with the reference solver.
            let out = try_solver(xml, ann, Solver::RowBias, declared_shape_count)
                .map_err(|e| format!("both {solver:?} and RowBias failed: {e:?}"))?;
            Ok(GuardedOutput {
                output: out,
                used_solver: Solver::RowBias,
                fallback: Some(reason),
            })
        }
    }
}

/// Try a solver once with a panic guard and a coverage post-condition.
fn try_solver(
    xml: &str,
    ann: &SemanticAnnotations,
    solver: Solver,
    declared_shape_count: usize,
) -> Result<LayoutOutput, FallbackReason> {
    let name = solver_name(solver);
    let attempt = std::panic::catch_unwind(AssertUnwindSafe(|| layout_with(xml, ann, solver)));
    let out = match attempt {
        Ok(Ok(out)) => out,
        Ok(Err(e)) => {
            return Err(FallbackReason::Panic {
                requested: name,
                message: e,
            })
        }
        Err(payload) => {
            let message = panic_message(&payload);
            return Err(FallbackReason::Panic {
                requested: name,
                message,
            });
        }
    };
    // Post-condition: at least MIN_COVERAGE of declared shapes actually
    // ended up in the emitted DI. A solver that drops most of them is
    // useless even if it didn't panic.
    let g = geom::parse(&out.bpmn_xml);
    let gates = gates::evaluate(&g, declared_shape_count);
    if gates.coverage < MIN_COVERAGE {
        return Err(FallbackReason::LowCoverage {
            requested: name,
            coverage: gates.coverage,
        });
    }
    Ok(out)
}

fn solver_name(s: Solver) -> &'static str {
    match s {
        Solver::RowBias => "rowbias",
        Solver::Field => "field",
    }
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "panicked with non-string payload".to_string()
    }
}

/// Convenience: count BPMN elements declared by the source XML, so callers
/// that only hold the XML can hand a sane `declared_shape_count` to
/// [`run`]. Silently returns 0 on parse failure — the coverage gate then
/// trivially passes (`declared == 0`).
pub fn declared_shape_count(xml: &str) -> usize {
    parse_bpmn(xml)
        .ok()
        .and_then(|defs| defs.into_iter().next())
        .map(|def| def.elements.len())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rowbias_runs_and_reports_no_fallback() {
        let xml = include_str!("../../fixtures/layout/tiny.bpmn");
        let ann = SemanticAnnotations::default();
        let n = declared_shape_count(xml);
        let out = run(xml, &ann, Solver::RowBias, n).expect("guarded run should succeed");
        assert!(out.fallback.is_none());
        assert!(matches!(out.used_solver, Solver::RowBias));
    }
}
