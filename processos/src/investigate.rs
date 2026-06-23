//! **Investigation** — point the cockpit droid at a bound trace dataset and let it
//! form and test its own hypotheses with the [`Analysis`] SQL surface.
//!
//! This is the consumer side of the open-ended sensing goal: rather than running a
//! fixed analyzer, we give the model a `query_traces(sql)` tool over the dataset and a
//! system prompt that imposes *analytical discipline* (state a hypothesis before
//! querying, report effect size + sample size, replicate on a held-out slice before
//! concluding). The returned [`AgentRun`] doubles as a reproducible "lab notebook":
//! every query and its result is recorded.

use serde::Serialize;
use serde_json::{json, Value};

use crate::agent::{run_agent, AgentRun, OpenAiAgent, ToolBox, ToolSpec};
use crate::analysis::Analysis;
use crate::dataset::TraceSource;
use crate::harness::llm::LlmConfig;

/// A [`ToolBox`] exposing the read-only trace-analysis surface to the model.
pub struct AnalysisTools {
    analysis: Analysis,
}

impl AnalysisTools {
    pub fn new(analysis: Analysis) -> Self {
        Self { analysis }
    }
}

impl ToolBox for AnalysisTools {
    fn specs(&self) -> Vec<ToolSpec> {
        vec![ToolSpec {
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
        }]
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
            other => Err(format!("unknown tool '{other}'")),
        }
    }
}

/// The system prompt that turns a chat model into a disciplined trace analyst.
pub const INVESTIGATOR_SYSTEM: &str = "\
You are a performance/process analyst investigating a captured BPMN trace dataset to \
find operational pathologies (under-provisioned worker pools, time-localised overloads, \
failure hot-spots) WITHOUT being told what they are. You have one tool, query_traces, \
that runs read-only DuckDB SQL over the data.\n\
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
pub async fn run_investigation(
    src: &TraceSource,
    cfg: LlmConfig,
    limit: usize,
    max_rounds: usize,
) -> Result<InvestigationReport, String> {
    let analysis = Analysis::from_source(src, limit).await?;
    let dataset = DatasetShape {
        instances: analysis.instance_count(),
        jobs: analysis.job_count(),
        incidents: analysis.incident_count(),
    };
    let tools = AnalysisTools::new(analysis);
    let model = OpenAiAgent { cfg };
    let user = format!(
        "Investigate this dataset ({} instances, {} job executions, {} incidents). \
         Find the dominant operational pathology and localise it.",
        dataset.instances, dataset.jobs, dataset.incidents
    );
    let run = run_agent(&model, &tools, INVESTIGATOR_SYSTEM, &user, max_rounds).await?;
    Ok(InvestigationReport { dataset, run })
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
}
