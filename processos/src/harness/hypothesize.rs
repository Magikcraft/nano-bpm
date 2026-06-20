//! M2: LLM-backed hypothesis.
//!
//! Where the baked generator ([`super::rank::baked_candidates`]) enumerates the
//! worker-swap grid mechanically, this module asks a language model to *propose*
//! candidates given the scenario, the worker catalogue, and the measured baseline
//! — then runs each proposal through the exact same [`super::sim`] +
//! [`super::rank`] machinery. The model only generates hypotheses; the harness
//! still measures and ranks them, so a hallucinated "improvement" that doesn't
//! actually help is exposed by the numbers.
//!
//! The model is reached through the pluggable [`super::llm`] client, so the same
//! code path serves a local `llama.cpp`/Ollama/vLLM server and Anthropic alike.

use std::collections::HashMap;

use nanobpmn_engine_core::bpmn::parse_bpmn;
use serde::Deserialize;

use super::llm::{self, LlmConfig};
use super::rank::{
    self, HarnessReport, LlmMeta, RejectedCandidate,
};
use super::Scenario;

/// One candidate as proposed by the model.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProposedCandidate {
    #[serde(default)]
    name: Option<String>,
    /// `job_type -> worker_id` overrides over the scenario defaults.
    #[serde(default)]
    assignment: HashMap<String, String>,
    #[serde(default)]
    rationale: Option<String>,
    /// An optional full BPMN rewrite (structural change). Validated before use.
    #[serde(default)]
    test_model: Option<String>,
}

const SYSTEM_PROMPT: &str = "You are a process-optimization assistant. Given a BPMN \
process, a catalogue of interchangeable workers (each with a cost, latency, and \
failure rate), and the measured baseline performance, you propose candidate \
configurations that should improve the objective without violating its correctness \
and reliability gates. You reply with ONLY a JSON array of candidates and no prose. \
Each candidate is an object: {\"name\": string, \"assignment\": {job_type: worker_id}, \
\"rationale\": string}. Use only worker ids from the catalogue. Prefer cheaper/faster \
workers where they do not raise the incident rate or lower correctness.";

/// Run the LLM-hypothesis loop: prompt the model, validate + evaluate its
/// proposals, and rank them (optionally alongside the baked grid).
pub async fn run_hypothesis(
    scenario: &Scenario,
    cfg: &LlmConfig,
    include_baked: bool,
) -> Result<HarnessReport, String> {
    let (defs, process_id) = rank::prepare(scenario)?;

    // Establish the baseline first; it anchors the prompt and the comparison.
    let baseline_assignment = rank::effective_assignment(scenario, &HashMap::new());
    let baseline = rank::evaluate(
        scenario,
        &defs,
        &process_id,
        "baseline",
        "baseline",
        None,
        &baseline_assignment,
    );

    let user_prompt = build_prompt(scenario, &baseline);
    let raw = llm::complete(cfg, SYSTEM_PROMPT, &user_prompt).await?;
    let proposed = parse_candidates(&raw)?;

    let mut variants = Vec::new();
    let mut rejected: Vec<RejectedCandidate> = Vec::new();
    let mut accepted = 0usize;

    for (i, c) in proposed.iter().enumerate() {
        let name = c
            .name
            .clone()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| format!("llm-candidate-{}", i + 1));

        // Validate referenced workers exist in the catalogue.
        if let Some((jt, wid)) = c
            .assignment
            .iter()
            .find(|(_, wid)| !scenario.workers.contains_key(*wid))
        {
            rejected.push(RejectedCandidate {
                name,
                reason: format!("unknown worker '{wid}' for task '{jt}'"),
            });
            continue;
        }

        // Validate an optional structural rewrite before trusting it.
        let (cand_defs, cand_pid) = match &c.test_model {
            Some(xml) => match parse_bpmn(xml) {
                Ok(d) if !d.is_empty() => {
                    let pid = d[0].id.clone();
                    (d, pid)
                }
                Ok(_) => {
                    rejected.push(RejectedCandidate {
                        name,
                        reason: "proposed model contained no process definitions".to_string(),
                    });
                    continue;
                }
                Err(e) => {
                    rejected.push(RejectedCandidate {
                        name,
                        reason: format!("proposed model failed to parse: {e:?}"),
                    });
                    continue;
                }
            },
            None => (defs.clone(), process_id.clone()),
        };

        let assignment = rank::effective_assignment(scenario, &c.assignment);
        variants.push(rank::evaluate(
            scenario,
            &cand_defs,
            &cand_pid,
            &name,
            "llm",
            c.rationale.clone(),
            &assignment,
        ));
        accepted += 1;
    }

    let mut notes = vec![format!(
        "LLM hypothesis via {} model '{}': proposed {}, accepted {}, rejected {}.",
        cfg.provider.as_str(),
        cfg.model,
        proposed.len(),
        accepted,
        rejected.len()
    )];

    if include_baked {
        let (baked, baked_notes) = rank::baked_candidates(scenario, &defs, &process_id);
        variants.extend(baked);
        notes.push("baked worker-swap grid included for comparison.".to_string());
        notes.extend(baked_notes);
    }

    let golden = scenario.golden.as_ref().map(|g| {
        let assignment = rank::effective_assignment(scenario, &g.assignment);
        rank::evaluate(
            scenario,
            &defs,
            &process_id,
            &g.name,
            "golden",
            None,
            &assignment,
        )
    });

    let llm_meta = LlmMeta {
        provider: cfg.provider.as_str().to_string(),
        model: cfg.model.clone(),
        proposed: proposed.len(),
        accepted,
        rejected,
    };

    Ok(rank::finalize(
        scenario,
        process_id,
        baseline,
        golden,
        variants,
        notes,
        Some(llm_meta),
    ))
}

/// Build the user prompt: objective, worker catalogue, task options, baseline.
fn build_prompt(scenario: &Scenario, baseline: &rank::VariantResult) -> String {
    let obj = &scenario.objective;
    let mut s = String::new();
    s.push_str(&format!("Scenario: {}\n", scenario.name));
    s.push_str(&format!(
        "Objective: minimize {} subject to correctnessRate >= {} and incidentRate <= {}.\n\n",
        obj.minimize, obj.min_correctness, obj.max_incident_rate
    ));

    s.push_str("Worker catalogue (id: cost, latencyMs, failureRate, outputKeys):\n");
    let mut worker_ids: Vec<&String> = scenario.workers.keys().collect();
    worker_ids.sort();
    for id in worker_ids {
        let w = &scenario.workers[id];
        let mut keys: Vec<&String> = w.output.keys().collect();
        keys.sort();
        s.push_str(&format!(
            "  - {}: cost={}, latencyMs={}, failureRate={}, outputKeys=[{}]\n",
            w.id,
            w.cost,
            w.latency_ms,
            w.failure_rate,
            keys.iter().map(|k| k.as_str()).collect::<Vec<_>>().join(", ")
        ));
    }

    s.push_str("\nTasks (job_type: default worker -> candidate workers you may assign):\n");
    let mut task_types: Vec<&String> = scenario.task_workers.keys().collect();
    for t in scenario.latent_options.keys() {
        if !task_types.contains(&t) {
            task_types.push(t);
        }
    }
    task_types.sort();
    for jt in task_types {
        let default = scenario
            .task_workers
            .get(jt)
            .map(|s| s.as_str())
            .unwrap_or("(none)");
        let mut opts: Vec<String> = vec![default.to_string()];
        for o in scenario.latent_options.get(jt).into_iter().flatten() {
            if !opts.contains(o) {
                opts.push(o.clone());
            }
        }
        s.push_str(&format!("  - {}: {} -> [{}]\n", jt, default, opts.join(", ")));
    }

    s.push_str(&format!(
        "\nBaseline (default assignment) measured over {} inputs: avgCost={:.3}, \
         avgLatencyMs={:.1}, correctnessRate={:.3}, incidentRate={:.3}.\n",
        scenario.inputs.len(),
        baseline.avg_cost,
        baseline.avg_latency_ms,
        baseline.correctness_rate,
        baseline.incident_rate
    ));
    s.push_str(
        "\nPropose up to 6 candidate assignments that should beat the baseline on the \
         objective while staying within the gates. Reply with ONLY the JSON array.",
    );
    s
}

/// Parse the model's reply into candidates, tolerating markdown fences and an
/// optional `{ "candidates": [...] }` wrapper.
fn parse_candidates(text: &str) -> Result<Vec<ProposedCandidate>, String> {
    let cleaned = strip_code_fences(text);

    if let Ok(v) = serde_json::from_str::<Vec<ProposedCandidate>>(cleaned.trim()) {
        return Ok(v);
    }

    #[derive(Deserialize)]
    struct Wrap {
        #[serde(default)]
        candidates: Vec<ProposedCandidate>,
    }
    if let Ok(w) = serde_json::from_str::<Wrap>(cleaned.trim()) {
        if !w.candidates.is_empty() {
            return Ok(w.candidates);
        }
    }

    // Last resort: slice to the outermost JSON array.
    if let (Some(start), Some(end)) = (cleaned.find('['), cleaned.rfind(']')) {
        if end > start {
            if let Ok(v) = serde_json::from_str::<Vec<ProposedCandidate>>(&cleaned[start..=end]) {
                return Ok(v);
            }
        }
    }

    Err(format!(
        "could not parse candidates from LLM output: {}",
        truncate(text, 300)
    ))
}

/// If the text contains a fenced code block, return its inner content; else the text.
fn strip_code_fences(text: &str) -> String {
    if let Some(open) = text.find("```") {
        // Skip past the opening fence and its optional language tag line.
        let after = &text[open + 3..];
        let body_start = after.find('\n').map(|n| n + 1).unwrap_or(0);
        let body = &after[body_start..];
        if let Some(close) = body.find("```") {
            return body[..close].to_string();
        }
        return body.to_string();
    }
    text.to_string()
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("{}…", &s[..n])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_json_array() {
        let txt = r#"[{"name":"a","assignment":{"classify":"cheap-llm"},"rationale":"cheaper"}]"#;
        let c = parse_candidates(txt).unwrap();
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].name.as_deref(), Some("a"));
        assert_eq!(c[0].assignment.get("classify").map(|s| s.as_str()), Some("cheap-llm"));
    }

    #[test]
    fn parses_fenced_json_with_prose() {
        let txt = "Sure! Here are my ideas:\n```json\n[{\"name\":\"b\",\"assignment\":{\"x\":\"y\"}}]\n```\nHope that helps.";
        let c = parse_candidates(txt).unwrap();
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].name.as_deref(), Some("b"));
    }

    #[test]
    fn parses_candidates_wrapper_object() {
        let txt = r#"{"candidates":[{"assignment":{"x":"y"}},{"assignment":{"a":"b"}}]}"#;
        let c = parse_candidates(txt).unwrap();
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn rejects_unparsable() {
        assert!(parse_candidates("I cannot help with that.").is_err());
    }

    #[test]
    fn prompt_lists_workers_and_baseline() {
        let scenario = crate::harness::example_scenario();
        let (defs, pid) = rank::prepare(&scenario).unwrap();
        let base_assign = rank::effective_assignment(&scenario, &HashMap::new());
        let baseline = rank::evaluate(&scenario, &defs, &pid, "baseline", "baseline", None, &base_assign);
        let prompt = build_prompt(&scenario, &baseline);
        assert!(prompt.contains("cheap-llm"));
        assert!(prompt.contains("classify"));
        assert!(prompt.contains("Baseline"));
        assert!(prompt.contains("minimize cost"));
    }
}
