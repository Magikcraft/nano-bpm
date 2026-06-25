//! The cockpit — Console → Process → Experiment, the §7.8/§10 operational surface.
//!
//! An **experiment is a `pilotSelfOptimize` instance** on Nano (see
//! `pilot/pilot-self-optimize.bpmn`): the engine owns the durable body, the
//! `Evolve` service task is the droid's turn (LLM-propose + engine-prove via
//! `/api/harness/evolve`), and the `Review` user task is the pilot's turn. This
//! module reads that loop back over Nano's public v2 REST + console trace
//! contract and shapes it into the cockpit's views: the four-step stepper and the
//! persistent droid-conversation pane. ProcessOS never reaches inside the engine —
//! it observes traces/variables and drives the documented control surface.

use serde::Serialize;
use serde_json::{json, Map, Value};

use crate::contracts::NanoClient;

/// The id of the pilot process (deployed from `pilot/pilot-self-optimize.bpmn`).
/// Every experiment is an instance of this process.
pub const PILOT_PROCESS_ID: &str = "pilotSelfOptimize";

/// The fleet/experiments landing payload for the cockpit.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Overview {
    /// The client's production engine ProcessOS analyses (read-only). Kept as
    /// `nanoBaseUrl` for back-compat; equals `ownBaseUrl` in a single-Nano setup.
    pub nano_base_url: String,
    /// ProcessOS's own engine, where experiments (the pilot loop) run.
    pub own_base_url: String,
    /// Deployed target processes (the things an experiment optimizes), excluding
    /// the pilot meta-process itself.
    pub processes: Vec<ProcessCard>,
    /// Every experiment (pilot instance) seen in the recent trace window.
    pub experiments: Vec<ExperimentSummary>,
}

/// A deployed target process with the headline production stats (§7.8 Console).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessCard {
    pub process_id: String,
    pub instances: usize,
    pub completed: usize,
    pub incident_instances: usize,
    pub avg_duration_ms: Option<u64>,
    pub p95_duration_ms: Option<u64>,
    pub bottleneck: Option<String>,
    pub experiments: usize,
}

/// One experiment in the list — the pilot instance plus the few variables needed
/// to place it (its target process and where it is in the loop).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExperimentSummary {
    pub instance_key: String,
    pub target_process_id: Option<String>,
    pub outcome: String,
    pub started_at: u64,
    pub duration_ms: Option<u64>,
    pub iteration: Option<i64>,
    pub max_iterations: Option<i64>,
    pub best_name: Option<String>,
    pub best_conserved_rate: Option<f64>,
    /// The library prompt id driving the droid (the experimental variable), if set.
    pub prompt_id: Option<String>,
    /// True when a `Review` user task is open — the pilot's turn.
    pub awaiting_pilot: bool,
}

/// A step in the four-step stepper.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Step {
    pub id: String,
    pub label: String,
    /// `done` | `active` | `todo`.
    pub status: String,
    pub detail: Option<String>,
}

/// A turn in the droid-conversation pane.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Turn {
    /// `engine` | `droid` | `pilot`.
    pub role: String,
    pub text: String,
}

/// The full experiment cockpit view.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExperimentDetail {
    pub summary: ExperimentSummary,
    pub steps: Vec<Step>,
    pub conversation: Vec<Turn>,
    /// The open `Review` user-task key, if any — the pilot acts on this.
    pub pending_user_task_key: Option<String>,
    /// The per-candidate scorecard from the latest round (the shared instrument).
    pub ranking_summary: Value,
    /// The end event reached, when the loop has finished (`Accepted` | `Stopped`).
    pub final_outcome: Option<String>,
}

/// Read all variables of a process instance into a flat map, decoding each
/// `/v2/variables/search` item's JSON-stringified `value` into a real `Value`.
async fn instance_variables(nano: &NanoClient, instance_key: &str) -> Map<String, Value> {
    let body = json!({ "filter": { "processInstanceKey": instance_key } });
    let mut out = Map::new();
    let res: Value = match nano.post_json("/v2/variables/search", &body).await {
        Ok(v) => v,
        Err(_) => return out,
    };
    if let Some(items) = res.get("items").and_then(|i| i.as_array()) {
        for it in items {
            let name = match it.get("name").and_then(|n| n.as_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            let decoded = match it.get("value").and_then(|v| v.as_str()) {
                // The variable value is itself a JSON document, serialized to a string.
                Some(s) => serde_json::from_str::<Value>(s).unwrap_or(Value::String(s.to_string())),
                None => it.get("value").cloned().unwrap_or(Value::Null),
            };
            out.insert(name, decoded);
        }
    }
    out
}

/// Find an open (`CREATED`) `Review` user task for an instance, returning its key.
async fn pending_review_task(nano: &NanoClient, instance_key: &str) -> Option<String> {
    let body = json!({ "filter": { "processInstanceKey": instance_key, "state": "CREATED" } });
    let res: Value = nano.post_json("/v2/user-tasks/search", &body).await.ok()?;
    res.get("items")
        .and_then(|i| i.as_array())
        .and_then(|items| {
            items
                .iter()
                .find(|t| t.get("elementId").and_then(|e| e.as_str()) == Some("Review"))
        })
        .and_then(|t| {
            t.get("userTaskKey")
                .and_then(|k| k.as_str())
                .map(str::to_string)
        })
}

fn as_i64(v: Option<&Value>) -> Option<i64> {
    v.and_then(|x| x.as_i64().or_else(|| x.as_f64().map(|f| f as i64)))
}

fn as_f64(v: Option<&Value>) -> Option<f64> {
    v.and_then(|x| x.as_f64())
}

fn as_string(v: Option<&Value>) -> Option<String> {
    v.and_then(|x| x.as_str().map(str::to_string))
}

/// Render a candidate's §7.7 fidelity tier as a short operator-facing badge label.
fn tier_label(tier: Option<&str>) -> &'static str {
    match tier {
        Some("recorded-replay") => "L2 · replay",
        Some("requires-generative-mock") => "L3 · mock",
        Some("infeasible") => "infeasible",
        _ => "—",
    }
}

/// Build an experiment summary from a pilot instance's trace summary + variables.
fn summarize(
    instance_key: &str,
    outcome: &str,
    started_at: u64,
    duration_ms: Option<u64>,
    vars: &Map<String, Value>,
    awaiting_pilot: bool,
) -> ExperimentSummary {
    ExperimentSummary {
        instance_key: instance_key.to_string(),
        target_process_id: as_string(vars.get("processId")),
        outcome: outcome.to_string(),
        started_at,
        duration_ms,
        iteration: as_i64(vars.get("iteration")),
        max_iterations: as_i64(vars.get("maxIterations")),
        best_name: as_string(vars.get("bestName")),
        best_conserved_rate: as_f64(vars.get("bestConservedRate")),
        prompt_id: as_string(vars.get("promptId")),
        awaiting_pilot,
    }
}

/// List every experiment (pilot instance) in the recent trace window, newest
/// first, each annotated with its target process and loop position.
pub async fn list_experiments(
    nano: &NanoClient,
    limit: usize,
) -> Result<Vec<ExperimentSummary>, String> {
    let traces = nano.list_traces(limit).await?;
    let mut out = Vec::new();
    for t in traces
        .into_iter()
        .filter(|t| t.process_id == PILOT_PROCESS_ID)
    {
        let vars = instance_variables(nano, &t.instance_key).await;
        let awaiting =
            t.outcome != "completed" && pending_review_task(nano, &t.instance_key).await.is_some();
        out.push(summarize(
            &t.instance_key,
            &t.outcome,
            t.started_at,
            t.duration_ms,
            &vars,
            awaiting,
        ));
    }
    out.sort_by(|a, b| b.started_at.cmp(&a.started_at));
    Ok(out)
}

/// The cockpit overview: target-process cards (with experiment counts) + the
/// experiments list. The process cards are distilled from the client's production
/// engine (`target`) — the same insight report the `/console` dashboard uses —
/// while the experiments are `pilotSelfOptimize` instances on ProcessOS's own
/// engine (`own`). In a single-Nano dev setup the two clients address the same URL.
pub async fn overview(
    target: &NanoClient,
    own: &NanoClient,
    limit: usize,
    sample: usize,
) -> Result<Overview, String> {
    let insights = crate::report::build(target, limit, sample).await?;
    let experiments = list_experiments(own, limit).await?;

    let mut processes: Vec<ProcessCard> = insights
        .processes
        .into_iter()
        .filter(|p| p.process_id != PILOT_PROCESS_ID)
        .map(|p| {
            let experiments = experiments
                .iter()
                .filter(|e| e.target_process_id.as_deref() == Some(p.process_id.as_str()))
                .count();
            ProcessCard {
                process_id: p.process_id,
                instances: p.instances,
                completed: p.completed,
                incident_instances: p.incident_instances,
                avg_duration_ms: p.avg_duration_ms,
                p95_duration_ms: p.p95_duration_ms,
                bottleneck: p.bottleneck.map(|b| b.element_id),
                experiments,
            }
        })
        .collect();
    processes.sort_by(|a, b| b.instances.cmp(&a.instances));

    Ok(Overview {
        nano_base_url: target.base_url().to_string(),
        own_base_url: own.base_url().to_string(),
        processes,
        experiments,
    })
}

/// Build the full cockpit view for one experiment (pilot instance): the stepper
/// and the conversation, reconstructed from the instance's trace path, current
/// variables, and any open user task.
pub async fn experiment_detail(
    nano: &NanoClient,
    instance_key: &str,
) -> Result<ExperimentDetail, String> {
    let trace = nano.trace(instance_key).await?;
    if trace.process_id != PILOT_PROCESS_ID {
        return Err(format!(
            "instance {instance_key} is '{}', not an experiment ({PILOT_PROCESS_ID})",
            trace.process_id
        ));
    }
    let vars = instance_variables(nano, instance_key).await;
    let pending = pending_review_task(nano, instance_key).await;
    let completed = trace.outcome == "completed";

    let path: Vec<String> = trace
        .elements
        .iter()
        .map(|e| e.element_id.clone())
        .collect();
    let evolved = path.iter().any(|e| e == "Evolve");
    let best_name = as_string(vars.get("bestName"));
    let best_rate = as_f64(vars.get("bestConservedRate"));
    let iteration = as_i64(vars.get("iteration"));
    let max_iterations = as_i64(vars.get("maxIterations"));
    let final_outcome = if completed {
        path.iter()
            .rev()
            .find(|e| e.as_str() == "Accepted" || e.as_str() == "Stopped")
            .cloned()
    } else {
        None
    };

    let summary = summarize(
        instance_key,
        &trace.outcome,
        trace.started_at,
        trace.duration_ms,
        &vars,
        pending.is_some(),
    );

    // The four-step stepper (observe → hypothesize → test → confirm), §7.8.
    let step_done = |s: &str| Step {
        id: String::new(),
        label: String::new(),
        status: s.to_string(),
        detail: None,
    };
    let mut steps = Vec::new();
    steps.push(Step {
        id: "baseline".into(),
        label: "Baseline & analysis".into(),
        detail: summary
            .target_process_id
            .as_ref()
            .map(|t| format!("forked from {t} over its recorded dataset")),
        ..step_done("done")
    });
    steps.push(Step {
        id: "hypotheses".into(),
        label: "Hypotheses".into(),
        status: if best_name.is_some() {
            "done".into()
        } else if evolved || pending.is_none() && !completed {
            "active".into()
        } else {
            "todo".into()
        },
        detail: best_name.clone().map(|n| format!("droid proposed “{n}”")),
    });
    steps.push(Step {
        id: "rank".into(),
        label: "Stress-test & rank".into(),
        status: if best_rate.is_some() {
            "done".into()
        } else {
            "todo".into()
        },
        detail: best_rate.map(|r| format!("best conserved-rate {:.3} on real traces", r)),
    });
    steps.push(Step {
        id: "decision".into(),
        label: "Decision & cohort".into(),
        status: if completed {
            "done".into()
        } else if pending.is_some() {
            "active".into()
        } else {
            "todo".into()
        },
        detail: final_outcome
            .as_ref()
            .map(|o| match o.as_str() {
                "Accepted" => "pilot accepted the candidate".to_string(),
                "Stopped" => "loop stopped".to_string(),
                other => other.to_string(),
            })
            .or_else(|| {
                pending
                    .as_ref()
                    .map(|_| "awaiting your decision".to_string())
            }),
    });

    // The droid-conversation pane: engine framing, the droid's latest proposal,
    // each candidate's scorecard, and the pilot's pending/closing turn.
    let mut conversation = Vec::new();
    if let Some(target) = &summary.target_process_id {
        conversation.push(Turn {
            role: "engine".into(),
            text: format!(
                "Experiment forked from “{target}”. The recorded production dataset is the fitness data; the engine replays every candidate against it.",
            ),
        });
    }
    let round = iteration.unwrap_or(0);
    if let Some(name) = &best_name {
        let rate = best_rate
            .map(|r| format!("{:.1}%", r * 100.0))
            .unwrap_or_else(|| "—".into());
        conversation.push(Turn {
            role: "droid".into(),
            text: format!(
                "Round {round}: my best redesign is “{name}”. Replayed on the real traces it conserved {rate} of the production boundary.",
            ),
        });
    }
    if let Some(items) = vars.get("rankingSummary").and_then(|v| v.as_array()) {
        for c in items {
            let name = as_string(c.get("name")).unwrap_or_else(|| "candidate".into());
            let rate = as_f64(c.get("conservedRate"))
                .map(|r| format!("{:.1}%", r * 100.0))
                .unwrap_or_else(|| "—".into());
            let feasible = c.get("feasible").and_then(|f| f.as_bool()).unwrap_or(true);
            let why = as_string(c.get("rationale")).unwrap_or_default();
            let tier = tier_label(as_string(c.get("fidelityTier")).as_deref());
            let new_workers = c
                .get("requiresNewWorkers")
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            conversation.push(Turn {
                role: "droid".into(),
                text: format!(
                    "• {name} [{tier}] — conserved {rate}{}{}{}",
                    if feasible { "" } else { " (infeasible)" },
                    if new_workers.is_empty() {
                        String::new()
                    } else {
                        format!(" · needs new workers: {new_workers}")
                    },
                    if why.is_empty() {
                        String::new()
                    } else {
                        format!(" — {why}")
                    },
                ),
            });
        }
    }
    match (&final_outcome, &pending) {
        (Some(o), _) if o == "Accepted" => conversation.push(Turn {
            role: "pilot".into(),
            text: "Accepted — this candidate is human-approved and engine-proven.".into(),
        }),
        (Some(o), _) if o == "Stopped" => conversation.push(Turn {
            role: "pilot".into(),
            text: match max_iterations {
                Some(m) if iteration.map(|i| i >= m).unwrap_or(false) => {
                    format!("Loop stopped — reached the iteration budget ({m}).")
                }
                _ => "Loop stopped.".into(),
            },
        }),
        (None, Some(_)) => conversation.push(Turn {
            role: "pilot".into(),
            text: "Your turn: accept this candidate, ask the droid to iterate, or stop the loop."
                .into(),
        }),
        _ => {}
    }

    Ok(ExperimentDetail {
        summary,
        steps,
        conversation,
        pending_user_task_key: pending,
        ranking_summary: vars.get("rankingSummary").cloned().unwrap_or(Value::Null),
        final_outcome,
    })
}

/// Resolve the latest deployed BPMN XML for a target process id (used to fork an
/// experiment's baseline when the caller does not supply one).
pub async fn latest_process_xml(nano: &NanoClient, process_id: &str) -> Result<String, String> {
    let body = json!({ "filter": { "processDefinitionId": process_id } });
    let res: Value = nano
        .post_json("/v2/process-definitions/search", &body)
        .await?;
    let key = res
        .get("items")
        .and_then(|i| i.as_array())
        .and_then(|items| {
            items
                .iter()
                .max_by_key(|d| d.get("version").and_then(|v| v.as_i64()).unwrap_or(0))
        })
        .and_then(|d| d.get("processDefinitionKey").and_then(|k| k.as_str()))
        .ok_or_else(|| format!("no deployed definition for process '{process_id}'"))?;
    nano.get_text(&format!("/v2/process-definitions/{key}/xml"))
        .await
}

/// Start an experiment: create a `pilotSelfOptimize` instance for a target
/// process. `prompt_id`, when given, names the library system prompt the droid
/// uses to hypothesize (the experimental variable, §7.8 step 2). Returns the new
/// instance key.
pub async fn create_experiment(
    nano: &NanoClient,
    target_process_id: &str,
    baseline_model: String,
    max_iterations: i64,
    prompt_id: Option<String>,
) -> Result<String, String> {
    let mut variables = serde_json::Map::new();
    variables.insert("processId".into(), json!(target_process_id));
    variables.insert("baselineModel".into(), json!(baseline_model));
    variables.insert("maxIterations".into(), json!(max_iterations));
    variables.insert("iteration".into(), json!(0));
    if let Some(pid) = prompt_id.filter(|s| !s.trim().is_empty()) {
        variables.insert("promptId".into(), json!(pid));
    }
    let body = json!({
        "processDefinitionId": PILOT_PROCESS_ID,
        "variables": Value::Object(variables),
    });
    let res: Value = nano.post_json("/v2/process-instances", &body).await?;
    res.get("processInstanceKey")
        .and_then(|k| k.as_str())
        .map(str::to_string)
        .ok_or_else(|| "create instance: missing processInstanceKey".to_string())
}

/// Submit the pilot's decision at the open `Review` user task (the human turn):
/// `accept` | `iterate` | `stop`.
pub async fn submit_decision(
    nano: &NanoClient,
    instance_key: &str,
    decision: &str,
    pilot_note: Option<&str>,
) -> Result<(), String> {
    let task = pending_review_task(nano, instance_key)
        .await
        .ok_or_else(|| format!("no open Review task for experiment {instance_key}"))?;
    let mut variables = serde_json::Map::new();
    variables.insert("decision".into(), json!(decision));
    // Carry the pilot's free-text guidance into the loop so the next Evolve round can
    // condition the droid on it (the §10 pairing closes here). Always set the variable
    // so a note-less round clears any guidance left over from a prior one.
    variables.insert("pilotNote".into(), json!(pilot_note.unwrap_or("")));
    let body = json!({ "variables": Value::Object(variables) });
    nano.post_no_content(&format!("/v2/user-tasks/{task}/completion"), &body)
        .await
}
