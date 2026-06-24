//! **The Nano Alternate Reality Engine** — speculative execution of variant
//! hypotheses. Where the other tool surfaces *observe* the process, these tools let
//! the droid *act on a counterfactual*: fork the current model into a what-if variant,
//! re-run real recorded production instances through it on an in-process engine, and
//! measure what would have happened — a whole multiverse of "what-ifs?" scored against
//! the same ground truth.
//!
//! It is built directly on the deterministic replay harness:
//! - [`simulate`] replays one candidate model against the recorded dataset and returns
//!   its fidelity scorecard (boundary conservation, latency, coverage, divergences).
//! - [`compare_variants`] scores a *population* of candidates (optionally including the
//!   current model as the `baseline`) and ranks them fidelity-first — the multiverse,
//!   ranked.
//!
//! Replay needs **recorded-input** instances (Tier-2 capture, `c8 nano --capture`): the
//! creation variables plus the ordered stimulus log. A dataset without capture is not
//! replayable, and the tools say so plainly (with skip accounting) rather than
//! fabricating a result.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::dataset::TraceSource;
use crate::harness::{
    parse_mock_workers, rank_candidates_by_replay, CandidateModel, RecordedInstance,
};

/// How many recent instances to draw into the replay dataset (bounds the per-turn I/O
/// and keeps replay fast); mirrors the replay-rank HTTP endpoint's default.
pub const RECORDED_CAP: usize = 300;

/// The recorded-input dataset distilled for a chat turn, with skip accounting so the
/// model can explain *why* instances were dropped (capture off, truncated, fetch error).
#[derive(Default, Clone)]
pub struct RecordedDataset {
    pub instances: Vec<RecordedInstance>,
    pub matched: u32,
    pub skipped: u32,
    pub skip_reasons: BTreeMap<String, u32>,
}

/// Distil up to `cap` replayable recorded instances from a trace source. Best-effort:
/// unreplayable / unfetchable instances are counted in the skip accounting, never fatal.
pub async fn build_recorded_dataset(src: &TraceSource, cap: usize) -> RecordedDataset {
    let mut ds = RecordedDataset::default();
    let summaries = match src.list_traces(cap).await {
        Ok(s) => s,
        Err(_) => return ds,
    };
    for s in summaries.iter().take(cap) {
        ds.matched += 1;
        match src.trace(&s.instance_key).await {
            Ok(t) => match RecordedInstance::from_trace(&t) {
                Ok(r) => ds.instances.push(r),
                Err(why) => {
                    ds.skipped += 1;
                    *ds.skip_reasons.entry(why.to_string()).or_insert(0) += 1;
                }
            },
            Err(_) => {
                ds.skipped += 1;
                *ds.skip_reasons.entry("trace fetch failed".into()).or_insert(0) += 1;
            }
        }
    }
    ds
}

/// The most common process id across the recorded instances (the one to start on replay).
fn default_process_id(ds: &RecordedDataset) -> Option<String> {
    let mut counts: BTreeMap<&str, u32> = BTreeMap::new();
    for r in &ds.instances {
        *counts.entry(r.process_id.as_str()).or_insert(0) += 1;
    }
    counts
        .into_iter()
        .max_by_key(|(_, n)| *n)
        .map(|(id, _)| id.to_string())
}

/// The honest "can't replay" payload when the dataset carries no replayable instances.
fn unavailable(ds: &RecordedDataset) -> Value {
    json!({
        "replayable": false,
        "reason": "no replayable recorded instances are available for this process",
        "matchedInstances": ds.matched,
        "skipped": ds.skipped,
        "skipReasons": ds.skip_reasons,
        "hint": "recorded-input replay needs Tier-2 capture — run the source gateway with \
                 `c8 nano --capture` (NANOBPMN_TRACE_STIMULI). Without it you can still reason \
                 about the model structurally (read_model / analyze_model), but the Alternate \
                 Reality Engine has no ground-truth inputs to re-run.",
    })
}

/// Drop the heavy per-instance `report.results` array from a serialized scorecard so the
/// model gets the aggregates without a flood of per-instance rows.
fn trim_report(report: &mut Value) {
    if let Some(obj) = report.as_object_mut() {
        obj.remove("results");
    }
}

/// When a candidate fails to deploy, the BPMN parser's reason is buried in
/// `scorecard.report.error` under a prominent `feasible:false` / "did not deploy". Lift it
/// to a top-level `deployError` on the scorecard and, for the recurring authoring mistakes
/// a model makes, attach an actionable `fixHint` — so the agent gets a crisp signal it can
/// act on instead of re-submitting the same broken XML.
fn surface_deploy_error(scorecard: &mut Value) {
    let err = scorecard
        .get("report")
        .and_then(|r| r.get("error"))
        .and_then(|e| e.as_str())
        .map(|s| s.to_string());
    let Some(err) = err else { return };
    if let Some(obj) = scorecard.as_object_mut() {
        if let Some(hint) = deploy_fix_hint(&err) {
            obj.insert("fixHint".into(), Value::String(hint));
        }
        obj.insert("deployError".into(), Value::String(err));
    }
}

/// Map a known BPMN deploy/parse error to a concrete fix, so the model stops repeating it.
fn deploy_fix_hint(err: &str) -> Option<String> {
    if err.contains("InvalidBoundaryEvent") && err.contains("unknown error") {
        return Some(
            "The error boundary event has an empty or unknown errorRef. A BPMN error \
             boundary needs BOTH a top-level `<bpmn:error id=\"E_X\" errorCode=\"...\"/>` \
             definition AND `<bpmn:errorEventDefinition errorRef=\"E_X\"/>` on the boundary \
             event referencing that id. To model a RETRY, prefer a timer boundary event \
             (interrupting=false) that loops back to the task, or reuse the model's existing \
             error definition — do not leave errorRef empty."
                .into(),
        );
    }
    None
}

/// `simulate` — replay one candidate model against the recorded dataset.
pub fn simulate(_base_model: Option<&str>, dataset: &RecordedDataset, args: &Value) -> Result<Value, String> {
    let model = args["model"]
        .as_str()
        .ok_or("simulate requires a string 'model' argument (the candidate BPMN XML)")?;
    if dataset.instances.is_empty() {
        return Ok(unavailable(dataset));
    }
    let name = args["name"].as_str().unwrap_or("variant").to_string();
    let rationale = args["rationale"].as_str().map(|s| s.to_string());
    let mock_workers = parse_mock_workers(&args["mockWorkers"]);
    let pid = default_process_id(dataset);
    let candidate = CandidateModel { name, rationale, model: model.to_string(), mock_workers };
    let ranking = rank_candidates_by_replay(
        std::slice::from_ref(&candidate),
        &dataset.instances,
        pid.as_deref(),
    );
    let mut v = serde_json::to_value(&ranking).map_err(|e| format!("serialise ranking: {e}"))?;
    if let Some(c) = v["candidates"].as_array_mut().and_then(|a| a.first_mut()) {
        trim_report(&mut c["report"]);
        surface_deploy_error(c);
        return Ok(json!({
            "replayable": true,
            "datasetSize": ranking.dataset_size,
            "scorecard": c,
        }));
    }
    Ok(json!({ "replayable": true, "datasetSize": ranking.dataset_size, "scorecard": Value::Null }))
}

/// `compare_variants` — score and rank a population of candidate models (optionally
/// including the current model as the `baseline`) against the same recorded dataset.
pub fn compare_variants(base_model: Option<&str>, dataset: &RecordedDataset, args: &Value) -> Result<Value, String> {
    let raw = args["candidates"]
        .as_array()
        .ok_or("compare_variants requires a 'candidates' array of {name, model, rationale?}")?;
    if dataset.instances.is_empty() {
        return Ok(unavailable(dataset));
    }
    let include_baseline = args["includeBaseline"].as_bool().unwrap_or(true);
    // A top-level `mockWorkers` applies to every candidate (the new workers
    // available in this alternate reality); a per-candidate `mockWorkers` merges
    // over it for variants that define their own.
    let shared_mocks = parse_mock_workers(&args["mockWorkers"]);

    let mut candidates: Vec<CandidateModel> = Vec::new();
    if include_baseline {
        if let Some(base) = base_model {
            candidates.push(CandidateModel {
                name: "baseline (current model)".into(),
                rationale: Some("the model currently deployed for this process".into()),
                model: base.to_string(),
                ..Default::default()
            });
        }
    }
    for (i, c) in raw.iter().enumerate() {
        let model = c["model"]
            .as_str()
            .ok_or_else(|| format!("candidate #{i} is missing a string 'model' (BPMN XML)"))?;
        let mut mock_workers = shared_mocks.clone();
        for (jt, out) in parse_mock_workers(&c["mockWorkers"]) {
            mock_workers.insert(jt, out);
        }
        candidates.push(CandidateModel {
            name: c["name"].as_str().unwrap_or(&format!("variant {}", i + 1)).to_string(),
            rationale: c["rationale"].as_str().map(|s| s.to_string()),
            model: model.to_string(),
            mock_workers,
        });
    }
    if candidates.is_empty() {
        return Err("no candidates to compare".into());
    }

    let pid = default_process_id(dataset);
    let ranking = rank_candidates_by_replay(&candidates, &dataset.instances, pid.as_deref());
    let mut v = serde_json::to_value(&ranking).map_err(|e| format!("serialise ranking: {e}"))?;
    if let Some(arr) = v["candidates"].as_array_mut() {
        for c in arr.iter_mut() {
            trim_report(&mut c["report"]);
            surface_deploy_error(c);
        }
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::RecordedStimulus;
    use serde_json::json;
    use std::collections::HashMap;

    // start -> Classify(classify) -> Summarize(summarize) -> end. Process id "P".
    const TWO_TASK: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Defs">
  <bpmn:process id="P" name="P" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Classify" />
    <bpmn:serviceTask id="Classify" name="Classify">
      <bpmn:extensionElements><zeebe:taskDefinition type="classify" /></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Classify" targetRef="Summarize" />
    <bpmn:serviceTask id="Summarize" name="Summarize">
      <bpmn:extensionElements><zeebe:taskDefinition type="summarize" /></bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f3" sourceRef="Summarize" targetRef="End" />
    <bpmn:endEvent id="End"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

    // Drops Summarize — a variant that fails to reproduce the recorded 'summary' key.
    const ONE_TASK: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Defs">
  <bpmn:process id="P" name="P" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Classify" />
    <bpmn:serviceTask id="Classify" name="Classify">
      <bpmn:extensionElements><zeebe:taskDefinition type="classify" /></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Classify" targetRef="End" />
    <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

    fn dataset() -> RecordedDataset {
        let instances = (0..3)
            .map(|i| RecordedInstance {
                instance_key: format!("k{i}"),
                process_id: "P".into(),
                started_at: 1000,
                creation_variables: [("input".to_string(), json!("x"))].into_iter().collect(),
                stimuli: vec![
                    RecordedStimulus {
                        seq: 1,
                        at: 1100,
                        kind: "jobCompleted".into(),
                        reference: Some("classify".into()),
                        variables: Some([("label".to_string(), json!("A"))].into_iter().collect::<HashMap<_, _>>()),
                    },
                    RecordedStimulus {
                        seq: 2,
                        at: 1300,
                        kind: "jobCompleted".into(),
                        reference: Some("summarize".into()),
                        variables: Some([("summary".to_string(), json!("S"))].into_iter().collect::<HashMap<_, _>>()),
                    },
                ],
            })
            .collect();
        RecordedDataset { instances, matched: 3, skipped: 0, skip_reasons: BTreeMap::new() }
    }

    #[test]
    fn simulate_scores_a_conserving_variant() {
        let ds = dataset();
        let v = simulate(None, &ds, &json!({ "model": TWO_TASK, "name": "identity" })).unwrap();
        assert_eq!(v["replayable"], true);
        assert_eq!(v["datasetSize"], 3);
        let sc = &v["scorecard"];
        assert_eq!(sc["fidelityTier"], "recorded-replay");
        assert_eq!(sc["report"]["conserved"], 3);
        assert!((sc["report"]["conservedRate"].as_f64().unwrap() - 1.0).abs() < 1e-9);
        // The heavy per-instance results were trimmed out.
        assert!(sc["report"].get("results").is_none());
    }

    #[test]
    fn simulate_is_honest_when_nothing_is_replayable() {
        let empty = RecordedDataset {
            matched: 5,
            skipped: 5,
            skip_reasons: [("trace has no recorded-input log (run the source node with c8 nano --capture)".to_string(), 5)]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let v = simulate(None, &empty, &json!({ "model": TWO_TASK })).unwrap();
        assert_eq!(v["replayable"], false);
        assert_eq!(v["skipped"], 5);
    }

    #[test]
    fn compare_variants_ranks_baseline_above_a_diverging_fork() {
        let ds = dataset();
        let v = compare_variants(
            Some(TWO_TASK),
            &ds,
            &json!({ "candidates": [ { "name": "drop-summarize", "model": ONE_TASK } ] }),
        )
        .unwrap();
        assert_eq!(v["datasetSize"], 3);
        // The conserving baseline is the best candidate; the diverging fork ranks below.
        assert_eq!(v["best"], "baseline (current model)");
        let cands = v["candidates"].as_array().unwrap();
        assert_eq!(cands.len(), 2);
        let baseline = cands.iter().find(|c| c["name"] == "baseline (current model)").unwrap();
        assert_eq!(baseline["report"]["conserved"], 3);
        let fork = cands.iter().find(|c| c["name"] == "drop-summarize").unwrap();
        assert_eq!(fork["report"]["conserved"], 0);
    }

    #[test]
    fn simulate_scores_a_new_worker_when_a_mock_is_supplied() {
        // TWO_TASK issues classify+summarize, but our dataset only recorded classify —
        // so summarize is a new worker. Supplying a mock for it makes the variant
        // scorable at the mocked-replay tier instead of requiring a generative mock.
        let ds = RecordedDataset {
            instances: vec![RecordedInstance {
                instance_key: "k0".into(),
                process_id: "P".into(),
                started_at: 1000,
                creation_variables: [("input".to_string(), json!("x"))].into_iter().collect(),
                stimuli: vec![RecordedStimulus {
                    seq: 1,
                    at: 1100,
                    kind: "jobCompleted".into(),
                    reference: Some("classify".into()),
                    variables: Some([("label".to_string(), json!("A"))].into_iter().collect::<HashMap<_, _>>()),
                }],
            }],
            matched: 1,
            skipped: 0,
            skip_reasons: BTreeMap::new(),
        };
        let v = simulate(
            None,
            &ds,
            &json!({
                "model": TWO_TASK,
                "name": "mocked",
                "mockWorkers": { "summarize": { "summary": "M" } }
            }),
        )
        .unwrap();
        assert_eq!(v["replayable"], true);
        let sc = &v["scorecard"];
        assert_eq!(sc["fidelityTier"], "mocked-replay");
        assert_eq!(sc["mockedWorkers"], json!(["summarize"]));
        assert!(sc["requiresNewWorkers"].as_array().unwrap().is_empty());
    }

    #[test]
    fn surface_deploy_error_lifts_the_parse_error_with_a_fix_hint() {
        // The Investigation 12 symptom: the deploy reason is buried in report.error.
        let mut sc = json!({
            "feasible": false,
            "confidence": "n/a — candidate did not deploy",
            "report": {
                "valid": 0,
                "error": "failed to parse: InvalidBoundaryEvent { process_id: \"loan-approval\", reason: \"boundary event BoundaryEvent_CreditError references unknown error ''\" }"
            }
        });
        surface_deploy_error(&mut sc);
        assert!(sc["deployError"].as_str().unwrap().contains("InvalidBoundaryEvent"));
        assert!(sc["fixHint"].as_str().unwrap().contains("errorRef"));
    }

    #[test]
    fn surface_deploy_error_is_a_noop_for_a_clean_scorecard() {
        let mut sc = json!({ "feasible": true, "report": { "conserved": 3 } });
        surface_deploy_error(&mut sc);
        assert!(sc.get("deployError").is_none());
        assert!(sc.get("fixHint").is_none());
    }
}
