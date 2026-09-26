//! The TokenFlow spec family's [`TraceMapping`]: how nano's `TokenFlow.tla`
//! vocabulary binds to the real engine (#1226). This is the reference
//! implementation sibling specs copy the *shape* of, while reusing the
//! `harness` driver itself.

use super::harness::{Fixture, Milestone, TraceMapping};
use nanobpmn_engine_core::{Event, ProcessBuilder, ProcessDefinition};

/// TokenFlow → engine binding.
///
/// Node kinds map to engine elements as:
///   * `start` → start event, `end` → end event
///   * `task`  → **service task** (a wait state that creates a job; the harness
///     drives it to completion), job type = the node id
///   * `and`   → parallel gateway, `or` → inclusive gateway, `xor` → exclusive
///     gateway
///
/// Only the parallel, routing-deterministic corpus is trace-anchored, so `or`/
/// `xor` never appear in a fixture graph; they are mapped for completeness (and
/// for siblings) but exercised only by the model checker, not the replay.
pub struct TokenFlowMapping;

impl TraceMapping for TokenFlowMapping {
    fn process_id(&self, fixture: &Fixture) -> String {
        fixture.model.clone()
    }

    fn build_process(&self, fixture: &Fixture) -> ProcessDefinition {
        let g = &fixture.graph;
        let mut b = ProcessBuilder::new(fixture.model.clone());
        for (id, kind) in &g.nodes {
            b = match kind.as_str() {
                "start" => b.start_event(id.clone()),
                "end" => b.end_event(id.clone()),
                "task" => b.service_task(id.clone(), id.clone()),
                "and" => b.parallel_gateway(id.clone()),
                "or" => b.inclusive_gateway(id.clone()),
                "xor" => b.exclusive_gateway(id.clone()),
                other => panic!("unmapped TokenFlow node kind {other:?}"),
            };
        }
        for (_id, from, to) in &g.flows {
            b = b.connect(from.clone(), to.clone());
        }
        b.build().expect("TokenFlow graph builds a valid process")
    }

    fn engine_milestones(&self, events: &[Event]) -> Vec<Milestone> {
        let mut out = Vec::new();
        for e in events {
            match e {
                Event::SequenceFlowTaken { from, to, .. } => out.push(Milestone::Flow {
                    from: from.to_string(),
                    to: to.to_string(),
                }),
                Event::JobCreated { element_id, .. } => out.push(Milestone::Task {
                    node: element_id.to_string(),
                }),
                Event::ParallelJoinFired { element_id, .. } => out.push(Milestone::JoinFired {
                    node: element_id.to_string(),
                }),
                Event::ProcessInstanceCompleted { .. } => out.push(Milestone::Completed),
                _ => {}
            }
        }
        out
    }
}
