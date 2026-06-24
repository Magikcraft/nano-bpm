//! **Investigation** — point the cockpit droid at a bound trace dataset and let it
//! form and test its own hypotheses with the [`Analysis`] SQL surface.
//!
//! This is the consumer side of the open-ended sensing goal: rather than running a
//! fixed analyzer, we give the model a `query_traces(sql)` tool over the dataset and a
//! system prompt that imposes *analytical discipline* (state a hypothesis before
//! querying, report effect size + sample size, replicate on a held-out slice before
//! concluding). The returned [`AgentRun`] doubles as a reproducible "lab notebook":
//! every query and its result is recorded.

use std::path::PathBuf;

use serde::Serialize;
use serde_json::{json, Value};

use crate::agent::{
    run_agent, run_agent_streaming, AgentEvent, AgentRun, Msg, OpenAiAgent, ToolBox, ToolSpec,
};
use crate::analysis::Analysis;
use crate::dataset::TraceSource;
use crate::harness::llm::LlmConfig;
use crate::pyrunner::{self, PyConfig, PYTHON_TOOL_DOC};

/// The optional Python escape hatch attached to a toolbox: a CSV workdir + config. The
/// workdir is removed on drop.
struct PyEnv {
    cfg: PyConfig,
    workdir: PathBuf,
}

/// A [`ToolBox`] exposing the read-only trace-analysis surface to the model, and
/// optionally a (trusted) Python escape hatch and the BPMN model-analysis tools.
pub struct AnalysisTools {
    analysis: Analysis,
    python: Option<PyEnv>,
    /// Raw `model.bpmn` XML for the bound process, when one is present. Enables the
    /// structural `read_model` / `analyze_model` tools (parsed lazily on call).
    model: Option<String>,
    /// Recorded-input instances for replay, distilled once per turn. Enables the
    /// `simulate` / `compare_variants` Alternate Reality Engine tools.
    recorded: Option<crate::experiment::RecordedDataset>,
}

impl Drop for AnalysisTools {
    fn drop(&mut self) {
        if let Some(py) = &self.python {
            let _ = std::fs::remove_dir_all(&py.workdir);
        }
    }
}

impl AnalysisTools {
    /// SQL-only toolbox.
    pub fn new(analysis: Analysis) -> Self {
        Self {
            analysis,
            python: None,
            model: None,
            recorded: None,
        }
    }

    /// Add the Python escape hatch: export the tables to a private CSV workdir the
    /// `run_python` tool reads. Falls back to SQL-only if the export fails.
    pub fn with_python(analysis: Analysis, cfg: PyConfig) -> Self {
        let workdir = std::env::temp_dir().join(format!(
            "processos-py-{}-{}",
            std::process::id(),
            now_nanos()
        ));
        let python = (|| {
            std::fs::create_dir_all(&workdir).ok()?;
            analysis.export_csv(&workdir).ok()?;
            Some(PyEnv {
                cfg,
                workdir: workdir.clone(),
            })
        })();
        if python.is_none() {
            let _ = std::fs::remove_dir_all(&workdir);
        }
        Self {
            analysis,
            python,
            model: None,
            recorded: None,
        }
    }

    /// Attach the process's BPMN model so the structural model-analysis tools become
    /// available. A `None` (or absent `model.bpmn`) leaves them off.
    pub fn set_model(&mut self, xml: Option<String>) {
        self.model = xml.filter(|x| !x.trim().is_empty());
    }

    /// Attach the recorded-input replay dataset so the Alternate Reality Engine tools
    /// (`simulate` / `compare_variants`) become available.
    pub fn set_recorded(&mut self, ds: Option<crate::experiment::RecordedDataset>) {
        self.recorded = ds;
    }
}

fn now_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

impl ToolBox for AnalysisTools {
    fn specs(&self) -> Vec<ToolSpec> {
        let mut specs = vec![ToolSpec {
            name: "query_traces".into(),
            description: format!(
                "Run a single read-only DuckDB SQL query (SELECT/WITH only) over the \
                 captured trace dataset and return the (row-capped) result.\n\n{}",
                self.analysis.schema_doc()
            ),
            parameters: json!({
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "A single read-only SELECT/WITH DuckDB query."
                    }
                },
                "required": ["sql"]
            }),
        }];
        specs.push(ToolSpec {
            name: "discover_flow".into(),
            description: "Mine the directly-follows graph the TRACE implies — independent of \
                any model. Returns the observed task nodes (with execution/instance counts), the \
                task-to-task transitions A->B (B's job ran immediately after A's within an \
                instance, ordered by capture seq) with counts and share, and the observed first/ \
                last tasks. Node ids are jobs.element_id, so this joins to the model's node ids. \
                Caveat: parallel branches are linearised in capture, so cross-branch edges are \
                artefacts. Takes no arguments."
                .into(),
            parameters: json!({ "type": "object", "properties": {} }),
        });
        if self.python.is_some() {
            specs.push(ToolSpec {
                name: "run_python".into(),
                description: PYTHON_TOOL_DOC.to_string(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "code": {
                            "type": "string",
                            "description": "Python 3 source; print() your findings."
                        }
                    },
                    "required": ["code"]
                }),
            });
        }
        if self.model.is_some() {
            specs.push(ToolSpec {
                name: "read_model".into(),
                description: "Return the structural view of this process's BPMN MODEL \
                    (independent of runtime): the start event, per-kind counts, and every \
                    node with its kind, key attributes (serviceTask jobType, boundary \
                    attachment, timer/message details), incoming count, outgoing targets \
                    (conditional flagged), reachability and gateway split/join role. Node \
                    ids and serviceTask jobTypes are the SAME keys the trace tables use \
                    (jobs.element_id / jobs.job_type, incidents.element_id) — use this to \
                    reason about structure and then join to runtime with query_traces. Takes \
                    no arguments."
                    .into(),
                parameters: json!({ "type": "object", "properties": {} }),
            });
            specs.push(ToolSpec {
                name: "analyze_model".into(),
                description: "Run deterministic static checks over the BPMN model structure \
                    and return advisory findings: unreachable nodes, dead ends, missing end \
                    events, exclusive splits with no default flow, unguarded service tasks \
                    (no error/timer boundary), gateway split/join hazards, and rework loops. \
                    Each finding references element ids that join to the trace tables, so you \
                    can quantify the risk with query_traces. Takes no arguments."
                    .into(),
                parameters: json!({ "type": "object", "properties": {} }),
            });
            specs.push(ToolSpec {
                name: "conformance_check".into(),
                description: "Replay the mined trace behaviour against the BPMN MODEL and report \
                    where reality diverges from design: a transition-fitness score, nonconformant \
                    task-to-task transitions (the model permits no path between them), tasks \
                    executed but absent from the model (undocumented), designed transitions the \
                    trace never took (unused model paths), and start/end deviations. Tasks are \
                    service/user tasks (the only nodes traced); gateways/events are collapsed. \
                    Use this to confirm whether the structure people designed is the process they \
                    actually run. Takes no arguments."
                    .into(),
                parameters: json!({ "type": "object", "properties": {} }),
            });
            specs.push(ToolSpec {
                name: "simulate".into(),
                description: "ALTERNATE REALITY ENGINE — replay a candidate (what-if) BPMN model \
                    against the REAL recorded production instances on an in-process engine, and \
                    return its fidelity scorecard: fidelityTier, boundary-conserved count and \
                    conservedRate, replayed avg/p99 end-to-end latency, per-job-type coverage, the \
                    job types it would need NEW WORKERS for (requiresNewWorkers), and which recorded \
                    output keys it failed to reproduce. Author a full variant of the current model \
                    (use read_model first) and pass its XML. If your variant adds a NEW worker (a \
                    job type history never recorded), supply a generative mock for it in \
                    `mockWorkers` so it can still be scored (fidelityTier becomes 'mocked-replay', \
                    a Level-3 assumption-based result) instead of being unscorable. Needs \
                    recorded-input capture; if none is available it returns replayable=false with \
                    the reason. Args: model (BPMN XML, required), name, rationale, mockWorkers."
                    .into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "model": { "type": "string", "description": "Candidate BPMN XML to replay." },
                        "name": { "type": "string", "description": "Short label for the variant." },
                        "rationale": { "type": "string", "description": "Why this variant was proposed." },
                        "mockWorkers": {
                            "type": "object",
                            "description": "Generative mocks for NEW workers the variant introduces. \
                                Map each new job type to EITHER a static output object (a \
                                deterministic worker), e.g. {\"fraud-check\": {\"fraudScore\": 0.1, \
                                \"isFraud\": false}}, OR a NON-DETERMINISTIC worker as a weighted \
                                distribution: {\"credit-check\": {\"outcomes\": [{\"weight\": 0.7, \
                                \"output\": {\"preApproved\": true}}, {\"weight\": 0.3, \"output\": \
                                {\"preApproved\": false}}]}}. Use the distribution form when the \
                                worker's output drives a downstream split/gateway and you want to \
                                see how the population flows down each branch (outcomes are spread \
                                across instances reproducibly, ~70/30 here). Supply one entry per \
                                new job type; existing (recorded) job types do not need a mock.",
                            "additionalProperties": { "type": "object" }
                        }
                    },
                    "required": ["model"]
                }),
            });
            specs.push(ToolSpec {
                name: "compare_variants".into(),
                description: "ALTERNATE REALITY ENGINE — score a MULTIVERSE of candidate models \
                    against the same recorded production dataset and rank them fidelity-first \
                    (conservedRate, then latency, then fewer required new workers). The current \
                    model is included as the 'baseline' by default (set includeBaseline=false to \
                    omit). Use this to decide whether a redesign actually beats today's process on \
                    real history. Returns datasetSize, the ranked candidates with their scorecards, \
                    and the best one. Needs recorded-input capture. A variant that adds a NEW \
                    worker can be scored by supplying a generative mock for it — either per \
                    candidate (mockWorkers on that item) or a top-level mockWorkers shared by all. \
                    Args: candidates (array of {name, model (BPMN XML), rationale?, mockWorkers?}), \
                    includeBaseline (bool), mockWorkers (shared map)."
                    .into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "candidates": {
                            "type": "array",
                            "description": "Variant models to score.",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": { "type": "string" },
                                    "model": { "type": "string", "description": "Candidate BPMN XML." },
                                    "rationale": { "type": "string" },
                                    "mockWorkers": {
                                        "type": "object",
                                        "description": "Generative mocks for NEW workers THIS variant \
                                            adds. Each job type maps to a static output object \
                                            (deterministic) or a {\"outcomes\":[{\"weight\",\"output\"}]} \
                                            distribution for a non-deterministic worker that drives a \
                                            downstream split.",
                                        "additionalProperties": { "type": "object" }
                                    }
                                },
                                "required": ["model"]
                            }
                        },
                        "includeBaseline": {
                            "type": "boolean",
                            "description": "Include the current model as 'baseline' (default true)."
                        },
                        "mockWorkers": {
                            "type": "object",
                            "description": "Generative mocks for new workers shared by ALL candidates. \
                                Each job type maps to a static output object (deterministic), e.g. \
                                {\"fraud-check\": {\"isFraud\": false}}, or a {\"outcomes\": [{\"weight\", \
                                \"output\"}]} distribution for a non-deterministic worker (e.g. a \
                                preApproved true/false split spread across the population).",
                            "additionalProperties": { "type": "object" }
                        }
                    },
                    "required": ["candidates"]
                }),
            });
        }
        specs
    }

    fn call(&self, name: &str, args: &Value) -> Result<String, String> {
        match name {
            "query_traces" => {
                let sql = args["sql"]
                    .as_str()
                    .ok_or("query_traces requires a string 'sql' argument")?;
                let result = self.analysis.query(sql)?;
                serde_json::to_string(&result).map_err(|e| format!("serialise result: {e}"))
            }
            "discover_flow" => {
                let v = crate::conformance::discover_flow(&self.analysis)?;
                serde_json::to_string(&v).map_err(|e| format!("serialise flow: {e}"))
            }
            "run_python" => {
                let py = self
                    .python
                    .as_ref()
                    .ok_or("run_python is not enabled for this investigation")?;
                let code = args["code"]
                    .as_str()
                    .ok_or("run_python requires a string 'code' argument")?;
                pyrunner::run_python(&py.cfg, &py.workdir, code)
            }
            "read_model" => {
                let xml = self
                    .model
                    .as_ref()
                    .ok_or("read_model is not available: this process has no BPMN model")?;
                let v = crate::bpmn_model::read_model(xml)?;
                serde_json::to_string(&v).map_err(|e| format!("serialise model: {e}"))
            }
            "analyze_model" => {
                let xml = self
                    .model
                    .as_ref()
                    .ok_or("analyze_model is not available: this process has no BPMN model")?;
                let v = crate::bpmn_model::analyze_model(xml)?;
                serde_json::to_string(&v).map_err(|e| format!("serialise findings: {e}"))
            }
            "conformance_check" => {
                let xml = self
                    .model
                    .as_ref()
                    .ok_or("conformance_check is not available: this process has no BPMN model")?;
                let v = crate::conformance::conformance_check(&self.analysis, xml)?;
                serde_json::to_string(&v).map_err(|e| format!("serialise conformance: {e}"))
            }
            "simulate" => {
                let ds = self
                    .recorded
                    .as_ref()
                    .ok_or("simulate is not available: no recorded dataset for this process")?;
                let v = crate::experiment::simulate(self.model.as_deref(), ds, args)?;
                serde_json::to_string(&v).map_err(|e| format!("serialise simulation: {e}"))
            }
            "compare_variants" => {
                let ds = self
                    .recorded
                    .as_ref()
                    .ok_or("compare_variants is not available: no recorded dataset for this process")?;
                let v = crate::experiment::compare_variants(self.model.as_deref(), ds, args)?;
                serde_json::to_string(&v).map_err(|e| format!("serialise comparison: {e}"))
            }
            other => Err(format!("unknown tool '{other}'")),
        }
    }
}

/// The system prompt that turns a chat model into a disciplined trace analyst.
pub const INVESTIGATOR_SYSTEM: &str = "\
You are a performance/process analyst investigating a captured BPMN trace dataset to \
find operational pathologies (under-provisioned worker pools, time-localised overloads, \
failure hot-spots) WITHOUT being told what they are. Your primary tool is query_traces, \
which runs read-only DuckDB SQL over the data. If a run_python tool is offered, use it \
only AFTER the SQL tool has localised a candidate, for analysis SQL cannot express \
(distribution fitting, changepoint/seasonal decomposition) — it loads the same tables \
from CSV.\n\
\n\
Method — follow it strictly:\n\
1. Orient: inspect the schema, the job types, the time span, and overall volumes.\n\
2. For each step, state ONE explicit hypothesis before you query.\n\
3. Test it with a query that reports an EFFECT SIZE and a SAMPLE SIZE (count), not just \
   an existence. Queue tails: compare quantile_cont(queue_ms, 0.99) across hour/day-of-week \
   buckets per job_type.\n\
4. Before concluding, REPLICATE the pattern on a held-out slice (e.g. a different week or \
   day) so you are not reporting noise.\n\
5. Only then conclude.\n\
\n\
When done, output a final answer as compact JSON on a single line:\n\
{\"domain\": <string>, \"bottleneckJob\": <job_type or null>, \"window\": <e.g. \
\"weekday-morning\" or null>, \"recommendation\": <string>, \"evidence\": [<short strings \
citing numbers you measured>]}.";

/// Run an investigation over a trace source with the configured (OpenAI-compatible) LLM.
///
/// When `allow_python` is set, the trusted Python escape hatch is exported and offered to
/// the model **in addition** to the SQL tool (gated behind the SQL tool by the system
/// prompt's discipline). It is off by default — SQL-only.
pub async fn run_investigation(
    src: &TraceSource,
    cfg: LlmConfig,
    py: PyConfig,
    limit: usize,
    max_rounds: usize,
    allow_python: bool,
) -> Result<InvestigationReport, String> {
    let analysis = Analysis::from_source(src, limit).await?;
    let dataset = DatasetShape {
        instances: analysis.instance_count(),
        jobs: analysis.job_count(),
        incidents: analysis.incident_count(),
    };
    let tools = if allow_python {
        AnalysisTools::with_python(analysis, py)
    } else {
        AnalysisTools::new(analysis)
    };
    let model = OpenAiAgent { cfg };
    let user = format!(
        "Investigate this dataset ({} instances, {} job executions, {} incidents). \
         Find the dominant operational pathology and localise it.",
        dataset.instances, dataset.jobs, dataset.incidents
    );
    let run = run_agent(&model, &tools, INVESTIGATOR_SYSTEM, &user, max_rounds).await?;
    Ok(InvestigationReport { dataset, run })
}

/// The system prompt for the **interactive** cockpit chat. Same analytical discipline
/// and tool usage as the one-shot investigator, but the droid answers the operator's
/// questions conversationally (prose citing the numbers it measured) across a
/// multi-turn dialogue, rather than emitting a single fixed JSON verdict.
pub const CHAT_SYSTEM: &str = "\
You are a performance/process analyst paired with an operator, investigating a captured \
BPMN trace dataset. Your job is to answer the operator's questions by actually querying \
the data — never guess or invent numbers. Your primary tool is query_traces, which runs \
read-only DuckDB SQL over the dataset. If a run_python tool is offered, use it only AFTER \
SQL has localised a candidate, for analysis SQL cannot express (distribution fitting, \
changepoint/seasonal decomposition).\n\
\n\
Investigate openly. Don't assume where the problem is or that there is one: profile \
broadly first (volumes, where time is spent across job types, failures, and how these \
move over time) and let the evidence point you to what matters. Form your own hypotheses \
from what you see rather than confirming a preconceived answer.\n\
\n\
Discipline: before you query, state the hypothesis you are testing. Prefer queries that \
report an EFFECT SIZE and a SAMPLE SIZE (count), not just existence. When you assert a \
pattern (for example a delay localised to a recurring time window), replicate it on a \
held-out slice before trusting it.\n\
\n\
Style: reply in clear prose, citing the concrete figures you measured. Stay focused on \
what the operator asked; when useful, suggest a sharp next question. Do NOT force your \
answer into JSON — write for a human reading a chat.";

/// The outcome of one interactive chat turn: the droid's reply, the tool calls it ran
/// this turn (the lab notebook), and the **full updated transcript** the caller must
/// persist so the next turn resumes with memory.
pub struct ChatTurnResult {
    pub answer: String,
    pub rounds: usize,
    pub messages: Vec<Msg>,
    pub dataset: DatasetShape,
}

/// Run one interactive chat turn over a trace source, resuming from `messages` (the
/// persisted transcript, empty on the first turn). The dataset's analytic view is built
/// fresh for the turn; the operator's `user_message` is appended and the tool-calling
/// loop runs until the droid replies. The returned `messages` is the transcript to
/// persist (it now ends with the droid's answer).
#[allow(clippy::too_many_arguments)]
pub async fn run_chat_turn(
    src: &TraceSource,
    cfg: LlmConfig,
    py: PyConfig,
    limit: usize,
    max_rounds: usize,
    allow_python: bool,
    objective: Option<&str>,
    persona_system: Option<&str>,
    model_xml: Option<String>,
    cancel: Option<&std::sync::atomic::AtomicBool>,
    steer: Option<&std::sync::Mutex<Vec<String>>>,
    sink: &mut dyn FnMut(AgentEvent),
    mut messages: Vec<Msg>,
    user_message: &str,
) -> Result<ChatTurnResult, String> {
    let analysis = Analysis::from_source(src, limit).await?;
    let dataset = DatasetShape {
        instances: analysis.instance_count(),
        jobs: analysis.job_count(),
        incidents: analysis.incident_count(),
    };
    let mut tools = if allow_python {
        AnalysisTools::with_python(analysis, py)
    } else {
        AnalysisTools::new(analysis)
    };
    // A model unlocks structural reasoning AND the Alternate Reality Engine: when one is
    // present, distil a (bounded) recorded-input dataset so simulate/compare_variants can
    // replay real instances against forked variants.
    let has_model = model_xml.as_ref().map(|x| !x.trim().is_empty()).unwrap_or(false);
    tools.set_model(model_xml);
    if has_model {
        let recorded = crate::experiment::build_recorded_dataset(src, crate::experiment::RECORDED_CAP).await;
        tools.set_recorded(Some(recorded));
    }
    let model = OpenAiAgent { cfg };

    // Seed the system message (with one-time dataset framing) only at the start of a
    // conversation; subsequent turns already carry it in the persisted transcript. The
    // standing instruction is the selected persona (defaulting to the Performance Analyst).
    if messages.is_empty() {
        let base = persona_system.unwrap_or(CHAT_SYSTEM);
        let mut sys = format!(
            "{base}\n\nDataset bound for this conversation: {} instances, {} job \
             executions, {} incidents.",
            dataset.instances, dataset.jobs, dataset.incidents
        );
        // The operator's stated objective is context, not a conclusion — surface it but
        // keep the analyst investigating openly and verifying against the data.
        if let Some(obj) = objective.map(str::trim).filter(|o| !o.is_empty()) {
            sys.push_str(&format!(
                "\n\nThe operator's stated objective for this process: \"{obj}\". Treat it \
                 as background context, not a foregone conclusion — investigate openly and \
                 let the data confirm or challenge it."
            ));
        }
        messages.push(Msg::System(sys));
    }
    messages.push(Msg::User(user_message.to_string()));

    let run =
        run_agent_streaming(&model, &tools, &mut messages, max_rounds, cancel, steer, sink).await?;
    Ok(ChatTurnResult {
        answer: run.answer,
        rounds: run.rounds,
        messages,
        dataset,
    })
}

/// Coarse shape of the dataset that was investigated.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetShape {
    pub instances: usize,
    pub jobs: usize,
    pub incidents: usize,
}
/// The full investigation result: the dataset shape + the agent's lab notebook.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvestigationReport {
    pub dataset: DatasetShape,
    pub run: AgentRun,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::{AgentStep, Msg, ToolCall, Turn};
    use crate::corpus;
    use std::cell::Cell;
    use std::path::Path;

    /// Build a real corpus + Analysis the same way the generator does, returning the
    /// tools over it.
    fn corpus_tools() -> AnalysisTools {
        let def = corpus::tests::loan_def();
        let pack = corpus::tests::loan_pack();
        let tmp = std::env::temp_dir().join(format!("invest-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        corpus::generate(&pack, &def, &tmp).expect("generate corpus");
        let src = TraceSource::Dataset(std::sync::Arc::new(
            crate::dataset::DatasetSource::open(&tmp).expect("open dataset"),
        ));
        // Build synchronously off the loaded dataset.
        let analysis = futures_block(Analysis::from_source(&src, 100_000));
        let _ = std::fs::remove_dir_all(&tmp);
        AnalysisTools::new(analysis.expect("analysis"))
    }

    fn futures_block<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn the_query_tool_surfaces_the_planted_queue_tail() {
        let tools = corpus_tools();
        // The diagnostic an analyst would run: per job_type, peak-vs-offpeak queue tail.
        let out = tools
            .call(
                "query_traces",
                &json!({"sql":
                    "SELECT job_type, \
                       quantile_cont(queue_ms, 0.99) FILTER (WHERE dow < 5 AND hour BETWEEN 9 AND 11) AS peak_p99, \
                       quantile_cont(queue_ms, 0.99) FILTER (WHERE NOT (dow < 5 AND hour BETWEEN 9 AND 11)) AS base_p99 \
                     FROM jobs GROUP BY job_type ORDER BY peak_p99 DESC"
                }),
            )
            .expect("query");
        let v: Value = serde_json::from_str(&out).unwrap();
        let rows = v["rows"].as_array().unwrap();
        // The worst peak tail belongs to credit-check, and it is far above its baseline.
        assert_eq!(rows[0][0].as_str().unwrap(), "credit-check");
        let peak: f64 = rows[0][1].as_str().unwrap().parse().unwrap();
        let base: f64 = rows[0][2].as_str().unwrap().parse().unwrap();
        assert!(
            peak > base * 3.0,
            "planted weekday-morning tail should dominate: peak {peak} vs base {base}"
        );
    }

    /// A scripted model that issues the diagnostic query, then concludes from what it
    /// saw — proving the *whole* loop (model → tool → DuckDB → result → model) carries
    /// the planted signal, deterministically and with no network.
    struct DiagnosticModel {
        idx: Cell<usize>,
    }
    impl AgentStep for DiagnosticModel {
        async fn step(&self, msgs: &[Msg], _tools: &[ToolSpec]) -> Result<Turn, String> {
            let i = self.idx.get();
            self.idx.set(i + 1);
            if i == 0 {
                return Ok(Turn::ToolCalls(vec![ToolCall {
                    id: "q1".into(),
                    name: "query_traces".into(),
                    arguments: json!({"sql":
                        "SELECT job_type, \
                           quantile_cont(queue_ms,0.99) FILTER (WHERE dow<5 AND hour BETWEEN 9 AND 11) AS peak, \
                           quantile_cont(queue_ms,0.99) FILTER (WHERE NOT (dow<5 AND hour BETWEEN 9 AND 11)) AS base \
                         FROM jobs GROUP BY job_type ORDER BY peak DESC"}),
                }]));
            }
            // The previous tool result is the last message; conclude from it.
            let last = msgs.last().expect("a message");
            let content = match last {
                Msg::Tool { content, .. } => content.clone(),
                _ => String::new(),
            };
            let v: Value = serde_json::from_str(&content).unwrap_or(json!({}));
            let job = v["rows"][0][0].as_str().unwrap_or("unknown").to_string();
            Ok(Turn::Final(
                json!({
                    "domain": "loan-origination",
                    "bottleneckJob": job,
                    "window": "weekday-morning",
                    "recommendation": "scale the credit-check worker pool",
                    "evidence": ["peak p99 queue >> baseline for the bottleneck job"]
                })
                .to_string(),
            ))
        }
    }

    #[test]
    fn agent_loop_recovers_the_fault_over_real_corpus_data() {
        let tools = corpus_tools();
        let model = DiagnosticModel { idx: Cell::new(0) };
        let run = futures_block(run_agent(
            &model,
            &tools,
            INVESTIGATOR_SYSTEM,
            "investigate",
            4,
        ))
        .expect("agent run");
        assert_eq!(run.steps.len(), 1, "should have run exactly one query");
        let answer: Value = serde_json::from_str(&run.answer).expect("answer is JSON");
        assert_eq!(answer["bottleneckJob"], "credit-check");
        assert_eq!(answer["window"], "weekday-morning");
    }

    #[allow(dead_code)]
    fn _unused(_p: &Path) {}

    /// Build the python-enabled toolbox over the same real corpus.
    fn corpus_tools_py() -> AnalysisTools {
        let def = corpus::tests::loan_def();
        let pack = corpus::tests::loan_pack();
        let tmp = std::env::temp_dir().join(format!("invest-py-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        corpus::generate(&pack, &def, &tmp).expect("generate corpus");
        let src = TraceSource::Dataset(std::sync::Arc::new(
            crate::dataset::DatasetSource::open(&tmp).expect("open dataset"),
        ));
        let analysis = futures_block(Analysis::from_source(&src, 100_000)).expect("analysis");
        let _ = std::fs::remove_dir_all(&tmp);
        AnalysisTools::with_python(analysis, crate::pyrunner::PyConfig::default())
    }

    fn python_available() -> bool {
        std::process::Command::new("python3")
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    #[test]
    fn run_python_recovers_the_planted_queue_tail_with_stdlib_only() {
        if !python_available() {
            return;
        }
        let tools = corpus_tools_py();
        // The python tool is offered only when enabled.
        assert!(
            tools.specs().iter().any(|s| s.name == "run_python"),
            "run_python should be exposed when python is enabled"
        );
        // Stdlib-only analysis (no pandas/duckdb assumed): compute peak vs off-peak mean
        // queue per job_type, find the worst, print it.
        let code = r#"
peak = {}; base = {}
def rows():
    return jobs if not HAVE_PANDAS else jobs.to_dict('records')
for r in rows():
    jt = r['job_type']; q = r.get('queue_ms')
    if q is None: continue
    is_peak = (int(r['dow']) < 5) and (9 <= int(r['hour']) <= 11)
    d = peak if is_peak else base
    d.setdefault(jt, []).append(float(q))
worst = None; worst_ratio = 0
for jt in peak:
    if jt in base and base[jt]:
        pr = sum(peak[jt])/len(peak[jt]); br = sum(base[jt])/len(base[jt])
        ratio = pr / br if br else 0
        if ratio > worst_ratio: worst_ratio = ratio; worst = jt
print('bottleneck', worst); print('ratio', round(worst_ratio, 2))
"#;
        let out = tools
            .call("run_python", &json!({ "code": code }))
            .expect("run_python");
        assert!(out.contains("bottleneck credit-check"), "got: {out}");
        let _ = out;
    }

    #[test]
    fn run_python_is_absent_when_not_enabled() {
        let tools = corpus_tools();
        assert!(
            !tools.specs().iter().any(|s| s.name == "run_python"),
            "run_python must not be exposed on the SQL-only toolbox"
        );
        let err = tools.call("run_python", &json!({"code": "print(1)"})).unwrap_err();
        assert!(err.contains("not enabled"), "got: {err}");
    }
}
