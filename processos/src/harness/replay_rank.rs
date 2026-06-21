//! Replay-ranked candidate evaluation (the gradient-driven loop core, §7.9).
//!
//! Where [`super::replay::replay_dataset`] scores **one** candidate against a
//! recorded dataset, this module scores a whole **population** of candidates
//! against the *same* dataset and ranks them by the fidelity gradient — the
//! "engine as fitness function" fan-out the hypothesis loop ranks with.
//!
//! It is the Level-2 analogue of [`super::rank`] (which ranks worker-swap
//! candidates on the distributional SimRunner). Replay discriminates
//! **structural** candidates: it re-drives each candidate model against the
//! recorded job outputs (matched by job *type*) and the recorded creation
//! inputs, so a redesign that still reproduces production's observed boundary
//! ranks above one that diverges or needs workers history never exercised.
//!
//! Ranking is **fidelity-first, never a single verdict**: feasible candidates
//! (those that deploy + run on the dataset) sort ahead of invalid ones; among
//! feasible, higher `conservedRate` wins, then lower replayed latency, then
//! fewer required new workers. Every candidate keeps its full [`ReplayReport`]
//! scorecard for drill-down, so the operator decides — the harness only ranks.

use nanobpmn_engine_core::bpmn::parse_bpmn;
use nanobpmn_engine_core::ProcessDefinition;
use serde::Serialize;

use super::replay::{replay_dataset, RecordedInstance, ReplayReport};

/// A candidate model to score, as supplied by the operator or the LLM.
#[derive(Clone, Debug)]
pub struct CandidateModel {
    /// Display name for the candidate (e.g. an LLM rationale label).
    pub name: String,
    /// Why this candidate was proposed (carried through to the scorecard).
    pub rationale: Option<String>,
    /// BPMN XML of the candidate model.
    pub model: String,
}

/// One scored candidate in the ranking.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RankedCandidate {
    /// Display name.
    pub name: String,
    /// Why it was proposed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
    /// Process id the candidate was replayed under.
    pub process_id: String,
    /// Whether the candidate parsed + replayed at all (an unparsable candidate is
    /// `false` with the parse error in `report.error`).
    pub feasible: bool,
    /// The full Level-2 scorecard for this candidate.
    pub report: ReplayReport,
}

/// The ranked population, plus the dataset it was scored against.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayRanking {
    /// How many recorded instances every candidate was replayed over.
    pub dataset_size: u32,
    /// Candidates, best fidelity first.
    pub candidates: Vec<RankedCandidate>,
    /// Name of the top-ranked feasible candidate, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best: Option<String>,
}

/// Score + rank a population of candidate models against one recorded dataset.
///
/// Pure and deterministic: the dataset is fetched by the caller (I/O stays out
/// of here). An unparsable or process-less candidate is retained as an
/// *infeasible* entry (so the operator sees *why* it failed) and sorts last.
pub fn rank_candidates_by_replay(
    candidates: &[CandidateModel],
    dataset: &[RecordedInstance],
    default_process_id: Option<&str>,
) -> ReplayRanking {
    let mut ranked: Vec<RankedCandidate> = candidates
        .iter()
        .map(|c| score_candidate(c, dataset, default_process_id))
        .collect();

    ranked.sort_by(|a, b| candidate_order(a).cmp(&candidate_order(b)));

    let best = ranked
        .iter()
        .find(|c| c.feasible && c.report.error.is_none())
        .map(|c| c.name.clone());

    ReplayRanking {
        dataset_size: dataset.len() as u32,
        candidates: ranked,
        best,
    }
}

/// Score a single candidate, distilling a parse failure into an infeasible entry
/// rather than dropping it.
fn score_candidate(
    c: &CandidateModel,
    dataset: &[RecordedInstance],
    default_process_id: Option<&str>,
) -> RankedCandidate {
    match parse_bpmn(&c.model) {
        Ok(defs) if !defs.is_empty() => {
            let process_id = default_process_id
                .map(|s| s.to_string())
                .unwrap_or_else(|| defs[0].id.clone());
            let report = replay_dataset(&defs, &process_id, dataset);
            RankedCandidate {
                name: c.name.clone(),
                rationale: c.rationale.clone(),
                process_id,
                feasible: true,
                report,
            }
        }
        Ok(_) => infeasible(c, default_process_id, "candidate contained no process definitions"),
        Err(e) => infeasible(c, default_process_id, &format!("failed to parse: {e:?}")),
    }
}

/// Build an infeasible `RankedCandidate` carrying the reason in `report.error`.
/// Scored over an empty dataset (the report is just a carrier for the parse
/// error), keeping every aggregate counter at zero.
fn infeasible(c: &CandidateModel, default_process_id: Option<&str>, reason: &str) -> RankedCandidate {
    let no_defs: [ProcessDefinition; 0] = [];
    let process_id = default_process_id.unwrap_or("").to_string();
    let mut report = replay_dataset(&no_defs, &process_id, &[]);
    report.error = Some(reason.to_string());
    RankedCandidate {
        name: c.name.clone(),
        rationale: c.rationale.clone(),
        process_id,
        feasible: false,
        report,
    }
}

/// Sort key: feasible-and-valid first, then by descending fidelity, then by
/// ascending replayed latency, then by fewer required new workers, then name.
///
/// Returned as a tuple of orderable scalars (latency/fidelity scaled to integers
/// for a total order).
fn candidate_order(c: &RankedCandidate) -> (u8, i64, u64, usize, String) {
    let broken = if c.feasible && c.report.error.is_none() {
        0
    } else {
        1
    };
    // Higher conservedRate first ⇒ negate (scaled to permille for integer order).
    let fidelity = -((c.report.conserved_rate * 1000.0).round() as i64);
    let latency = c.report.avg_e2e_latency_ms.round() as u64;
    let new_workers = c.report.uncovered_job_types.len();
    (broken, fidelity, latency, new_workers, c.name.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::replay::{RecordedInstance, RecordedStimulus};
    use serde_json::json;
    use serde_json::Value as Json;
    use std::collections::HashMap;

    const TWO_TASK: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D">
  <bpmn:process id="P" isExecutable="true">
    <bpmn:startEvent id="S"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="C"/>
    <bpmn:serviceTask id="C" name="Classify">
      <bpmn:extensionElements><zeebe:taskDefinition type="classify"/></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="C" targetRef="U"/>
    <bpmn:serviceTask id="U" name="Summarize">
      <bpmn:extensionElements><zeebe:taskDefinition type="summarize"/></bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f3" sourceRef="U" targetRef="E"/>
    <bpmn:endEvent id="E"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

    const ONE_TASK: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D">
  <bpmn:process id="P" isExecutable="true">
    <bpmn:startEvent id="S"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="C"/>
    <bpmn:serviceTask id="C" name="Classify">
      <bpmn:extensionElements><zeebe:taskDefinition type="classify"/></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f3" sourceRef="C" targetRef="E"/>
    <bpmn:endEvent id="E"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

    fn map(pairs: &[(&str, Json)]) -> HashMap<String, Json> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    fn rec(creation: &[(&str, Json)], stimuli: Vec<RecordedStimulus>) -> RecordedInstance {
        RecordedInstance {
            instance_key: "1".into(),
            process_id: "P".into(),
            started_at: 1000,
            creation_variables: map(creation),
            stimuli,
        }
    }

    fn job(seq: u32, at: u64, job_type: &str, out: Option<&[(&str, Json)]>) -> RecordedStimulus {
        RecordedStimulus {
            seq,
            at,
            kind: "jobCompleted".into(),
            reference: Some(job_type.into()),
            variables: out.map(map),
        }
    }

    fn dataset() -> Vec<RecordedInstance> {
        let mk = |k: &str| {
            rec(
                &[("input", json!("x"))],
                vec![
                    job(1, 1100, "classify", Some(&[("label", json!("A"))])),
                    job(2, 1300, "summarize", Some(&[("summary", json!(k))])),
                ],
            )
        };
        vec![mk("S1"), mk("S2"), mk("S3")]
    }

    #[test]
    fn the_conserving_candidate_outranks_the_diverging_one() {
        let ds = dataset();
        let cands = vec![
            CandidateModel {
                name: "drop-summarize".into(),
                rationale: Some("fewer steps".into()),
                model: ONE_TASK.into(),
            },
            CandidateModel {
                name: "keep-both".into(),
                rationale: None,
                model: TWO_TASK.into(),
            },
        ];
        let ranking = rank_candidates_by_replay(&cands, &ds, Some("P"));
        assert_eq!(ranking.dataset_size, 3);
        assert_eq!(ranking.best.as_deref(), Some("keep-both"));
        assert_eq!(ranking.candidates[0].name, "keep-both");
        assert!((ranking.candidates[0].report.conserved_rate - 1.0).abs() < 1e-9);
        // The diverging candidate is ranked below and still carries its scorecard.
        assert_eq!(ranking.candidates[1].name, "drop-summarize");
        assert_eq!(ranking.candidates[1].report.conserved, 0);
    }

    #[test]
    fn an_unparsable_candidate_is_infeasible_and_ranks_last() {
        let ds = dataset();
        let cands = vec![
            CandidateModel {
                name: "broken".into(),
                rationale: None,
                model: "<not-bpmn/>".into(),
            },
            CandidateModel {
                name: "good".into(),
                rationale: None,
                model: TWO_TASK.into(),
            },
        ];
        let ranking = rank_candidates_by_replay(&cands, &ds, Some("P"));
        assert_eq!(ranking.best.as_deref(), Some("good"));
        assert_eq!(ranking.candidates[0].name, "good");
        let broken = &ranking.candidates[1];
        assert_eq!(broken.name, "broken");
        assert!(!broken.feasible);
        assert!(broken.report.error.is_some());
    }

    #[test]
    fn empty_population_has_no_best() {
        let ranking = rank_candidates_by_replay(&[], &dataset(), Some("P"));
        assert!(ranking.best.is_none());
        assert!(ranking.candidates.is_empty());
        assert_eq!(ranking.dataset_size, 3);
    }
}
