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
    run_agent, run_agent_streaming, AgentEvent, AgentRun, Checkpoint, Msg, OpenAiAgent, ToolBox,
    ToolSpec,
};
use crate::analysis::Analysis;
use crate::dataset::TraceSource;
use crate::harness::llm::LlmConfig;
use crate::pyrunner::{self, PyConfig, PYTHON_TOOL_DOC};
use crate::tools::{ToolBackend, ToolDef};

/// The optional Python escape hatch attached to a toolbox: a CSV workdir + config. The
/// workdir is removed on drop.
struct PyEnv {
    cfg: PyConfig,
    workdir: PathBuf,
}

/// Configuration for the `delegate` tool: the primary can hand a self-contained research task
/// to a subagent that runs in its own context with read-only tools and reports back a compact
/// digest, sparing the primary's context window. `None` ⇒ the tool is not offered.
pub struct SubAgent {
    /// The subagent's LLM connection (its own profile — often a small/fast model).
    pub cfg: LlmConfig,
    /// Display name for provenance (cockpit attribution).
    pub name: String,
    /// The subagent persona's system prompt.
    pub system: String,
    /// Tool-loop budget for one delegated task.
    pub max_rounds: usize,
    /// Hard cap (chars) on the digest returned to the primary — context protection.
    pub digest_cap: usize,
    /// The trace source + cap the subagent builds its OWN dataset from (independent DuckDB
    /// connection, so it never shares the primary's non-Send analysis across threads). Filled in
    /// by [`run_chat_turn`] from the bound dataset; the request builder leaves it `None`.
    pub src: Option<TraceSource>,
    pub limit: usize,
    /// The BPMN model XML, if any, so the subagent gets read_model too.
    pub model: Option<String>,
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
    /// When set, exposes the `delegate` tool so the primary can spawn a subagent.
    sub: Option<SubAgent>,
    /// Operator-authored custom tools (enabled defs) offered alongside the built-ins.
    custom: Vec<ToolDef>,
    /// A private CSV export dir for `subprocess` custom tools (`PROCESSOS_DATASET`). Built lazily
    /// when a subprocess tool is present and the Python workdir isn't already serving the CSVs.
    custom_data: Option<PathBuf>,
    /// The bound model XML written to a temp file for `subprocess` tools (`PROCESSOS_MODEL`).
    model_path: Option<PathBuf>,
}

impl Drop for AnalysisTools {
    fn drop(&mut self) {
        if let Some(py) = &self.python {
            let _ = std::fs::remove_dir_all(&py.workdir);
        }
        if let Some(dir) = &self.custom_data {
            let _ = std::fs::remove_dir_all(dir);
        }
        if let Some(p) = &self.model_path {
            let _ = std::fs::remove_file(p);
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
            sub: None,
            custom: Vec::new(),
            custom_data: None,
            model_path: None,
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
            sub: None,
            custom: Vec::new(),
            custom_data: None,
            model_path: None,
        }
    }

    /// Attach a subagent so the `delegate` tool becomes available. A `None` leaves it off.
    pub fn set_subagent(&mut self, sub: Option<SubAgent>) {
        self.sub = sub;
    }

    /// Attach the operator-authored custom tools (already filtered to enabled). For `subprocess`
    /// tools, exports the trace CSVs to a private dir and writes the model XML to a temp file so
    /// the external program can read them via `PROCESSOS_DATASET` / `PROCESSOS_MODEL`.
    pub fn set_custom_tools(&mut self, defs: Vec<ToolDef>) {
        let needs_data = defs.iter().any(|d| d.backend == ToolBackend::Subprocess);
        if needs_data && self.custom_data.is_none() {
            let dir = std::env::temp_dir().join(format!(
                "processos-tooldata-{}-{}",
                std::process::id(),
                now_nanos()
            ));
            if std::fs::create_dir_all(&dir).is_ok() && self.analysis.export_csv(&dir).is_ok() {
                self.custom_data = Some(dir);
            } else {
                let _ = std::fs::remove_dir_all(&dir);
            }
        }
        if needs_data && self.model_path.is_none() {
            if let Some(xml) = self.model.as_ref() {
                let p = std::env::temp_dir().join(format!(
                    "processos-toolmodel-{}-{}.bpmn",
                    std::process::id(),
                    now_nanos()
                ));
                if std::fs::write(&p, xml).is_ok() {
                    self.model_path = Some(p);
                }
            }
        }
        self.custom = defs;
    }

    /// Execute one custom tool by its definition and the model-supplied arguments.
    fn call_custom(&self, def: &ToolDef, args: &Value) -> Result<String, String> {
        match def.backend {
            ToolBackend::Sql => {
                let sql = def.render_sql(args);
                let result = self.analysis.query(&sql)?;
                serde_json::to_string(&result).map_err(|e| format!("serialise result: {e}"))
            }
            ToolBackend::Python => {
                let py = self.python.as_ref().ok_or(
                    "this Python tool requires Python to be enabled for the investigation",
                )?;
                let code = def.render_python(args);
                pyrunner::run_python(&py.cfg, &py.workdir, &code)
            }
            ToolBackend::Subprocess => self.run_subprocess(def, args),
        }
    }

    /// Run a `subprocess` custom tool: fixed argv (no shell), arguments delivered both interpolated
    /// into argv and as JSON on stdin, with the dataset/model paths exposed via the environment.
    fn run_subprocess(&self, def: &ToolDef, args: &Value) -> Result<String, String> {
        use std::io::Write;
        use std::process::{Command, Stdio};

        let argv = def.build_argv(args)?;
        let (program, rest) = argv.split_first().ok_or("subprocess tool has no command")?;
        let mut cmd = Command::new(program);
        cmd.args(rest)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(dir) = &self.custom_data {
            cmd.env("PROCESSOS_DATASET", dir);
        }
        if let Some(p) = &self.model_path {
            cmd.env("PROCESSOS_MODEL", p);
        }
        let mut child = cmd.spawn().map_err(|e| format!("spawn '{program}': {e}"))?;
        if let Some(mut stdin) = child.stdin.take() {
            let _ = stdin.write_all(def.stdin_json(args).as_bytes());
        }
        // Enforce the wall-clock budget by polling, then killing on overrun.
        let deadline = std::time::Instant::now() + def.timeout();
        loop {
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) => {
                    if std::time::Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        return Err(format!(
                            "tool '{}' exceeded its {}ms time budget",
                            def.name,
                            def.timeout().as_millis()
                        ));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                Err(e) => return Err(format!("wait '{program}': {e}")),
            }
        }
        let out = child
            .wait_with_output()
            .map_err(|e| format!("collect '{program}' output: {e}"))?;
        let mut stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        const CAP: usize = 8_000;
        if stdout.len() > CAP {
            stdout.truncate(CAP);
            stdout.push_str("\n…[output truncated]");
        }
        if out.status.success() {
            Ok(stdout)
        } else {
            let stderr = String::from_utf8_lossy(&out.stderr);
            let stderr = stderr.chars().take(2_000).collect::<String>();
            Err(format!(
                "tool '{}' exited with {}: {}",
                def.name,
                out.status,
                stderr.trim()
            ))
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
        specs.push(ToolSpec {
            name: "scale_workers".into(),
            description: "INFRASTRUCTURE WHAT-IF — answer \"how many workers does a job type \
                need, and what queue-wait would N workers deliver?\" This is the one experiment \
                simulate/compare_variants CANNOT run: those replay the recorded TIMELINE, so \
                adding workers changes nothing there. This instead fits an M/M/c queueing model \
                to the RECORDED load (per-job arrival rate from the trace window + measured mean \
                service time) and predicts the p99 queue-wait at different pool sizes. Reach for \
                it when the bottleneck is UNDER-PROVISIONING (high queue_ms / a growing backlog / \
                'worker exhausted retries' incidents) — i.e. the fix is operational scaling, not \
                a model-structure change. With no args it sizes every job type to hold p99 \
                queue-wait <= 1000ms; pass jobType to focus one, targetP99WaitMs to set the tail \
                target, and workerCounts to price specific pool sizes. Each result gives the \
                fitted arrivalPerSec, serviceMs, offeredLoadErlangs, the observed p50/p99 wait \
                (the status quo), a recommendedWorkers count, and a predictions curve (always \
                including 1 worker). Each prediction gives predictedUtilization plus the full \
                predicted QUEUE-WAIT envelope: predictedWaitMs {mean,p50,p95,p99}, and the \
                distribution parameters waitProbability (Erlang-C: chance a job queues) + \
                waitDecayPerMs (exponential tail rate). TO GET THE NEW *PROCESS* PERFORMANCE \
                ENVELOPE (end-to-end p50/p95/p99), roll these per-job waits up yourself with \
                query_traces / run_python: the jobs table has every instance's per-job queue_ms \
                & service_ms in seq order and instances.duration_ms is the recorded e2e, so a \
                first-order projection is new_e2e[i] = duration_ms[i] − Σ over scaled jobs in i \
                of (recorded queue_ms − predictedWaitMs.mean), then quantile_cont(new_e2e, {0.5, \
                0.95,0.99}); this assumes the scaled task sits on the instance's critical path \
                (true for a sequential bottleneck, optimistic on a non-critical parallel branch \
                — state the assumption). Note: worker count is deployment config, not a BPMN \
                property, so DON'T try to model scaling by editing the model (e.g. cloning the \
                task) — quantify it here, then recommend the staffing change."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "jobType": {
                        "type": "string",
                        "description": "Focus on one job type (jobs.job_type). Omit to size every \
                            job type in the dataset (worst queue tail first)."
                    },
                    "targetP99WaitMs": {
                        "type": "integer",
                        "description": "The p99 QUEUE-wait target (ms) the recommendation must \
                            hold. Default 1000. Lower = more workers."
                    },
                    "workerCounts": {
                        "type": "array",
                        "items": { "type": "integer" },
                        "description": "Explicit pool sizes to price (e.g. [2,4,8,16]) — the \
                            predictions curve returns each one's predicted p99 wait and \
                            utilization. The status-quo single worker and the recommendation are \
                            always included."
                    }
                }
            }),
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
                    attachment, timer/message details, callActivity calledElement), incoming \
                    count, outgoing targets (conditional flagged), reachability and gateway \
                    split/join role. Node ids and serviceTask jobTypes are the SAME keys the \
                    trace tables use (jobs.element_id / jobs.job_type, incidents.element_id) — \
                    use this to reason about structure and then join to runtime with \
                    query_traces. If the model is a multi-stage ORCHESTRATOR (it has \
                    callActivity nodes, flagged by `expandable:true`), the default view shows \
                    the phases as OPAQUE boxes; the trace tables address the inner tasks with \
                    `Parent$Child` ids (e.g. Phase2_DocumentRequest$Task_SendRefreshRequest). \
                    Pass expand:true to INLINE every phase into one Parent$Child graph that \
                    matches those ids (use read_model_xml to see a phase's raw BPMN). \
                    Args: expand (boolean, default false)."
                    .into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "expand": {
                            "type": "boolean",
                            "description": "Inline call activities so node ids are the \
                                Parent$Child keys the trace tables use. Default false (opaque \
                                phases / orchestrator overview)."
                        }
                    }
                }),
            });
            specs.push(ToolSpec {
                name: "read_model_xml".into(),
                description: "Return the RAW BPMN XML of the model — the ground-truth source the \
                    distilled read_model view is derived from. Use it when read_model isn't \
                    enough: to inspect a service task's full extensionElements, a flow's exact \
                    FEEL conditionExpression, multi-instance / boundary-event details, or — for a \
                    multi-stage ORCHESTRATOR — the internals of a called phase. With no argument \
                    it returns the whole document (and the list of process ids inside it). Pass \
                    process:\"<id>\" — either a `<process>` id OR a callActivity node id (resolved \
                    to its calledElement) — to isolate ONE phase's raw XML. Once you can see it, \
                    change it with edit_model process:\"<id>\" rather than hand-writing XML. \
                    Args: process (optional process id or callActivity node id)."
                    .into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "process": {
                            "type": "string",
                            "description": "A process id, or a callActivity node id (resolved to \
                                its calledElement), to isolate that phase's raw XML. Omit for the \
                                whole document."
                        }
                    }
                }),
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
                name: "validate_model".into(),
                description: "LINT / VALIDATE a candidate BPMN model BEFORE you simulate or \
                    propose it — cheap, fast, and deploys nothing. It parses the XML with the \
                    SAME engine production uses and catches the authoring mistakes that otherwise \
                    waste a simulate round-trip: a dangling errorRef (an error boundary whose \
                    errorRef points at no `<bpmn:error id=…>` definition — a common one), a bare \
                    `<bpmn:process>` fragment missing its `<bpmn:definitions>` root, or otherwise \
                    unparseable XML. It also AUTO-HEALS the most common slip — `<errorBoundaryEvent>` \
                    (not a real element) is rewritten to `<boundaryEvent>` — and FLAGS the silent \
                    `zeebe:taskDefinition` ATTRIBUTE mistake (the engine ignores it, so the job type \
                    defaults to the task id; use the `<bpmn:extensionElements><zeebe:taskDefinition \
                    type=…/></bpmn:extensionElements>` child form), reporting both under `autoFixed` \
                    / a `task-definition-as-attribute` finding. On a parse error it returns \
                    valid=false with the error and an actionable `fix`. On a clean parse it returns \
                    valid=true plus the full analyze_model structural findings (unreachable nodes, \
                    dead ends, unguarded service tasks, rework loops). ALWAYS validate a model you \
                    authored here first; only simulate models that pass. Args: model (BPMN XML; omit \
                    to validate the current process model)."
                    .into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "model": {
                            "type": "string",
                            "description": "Candidate BPMN XML to validate. Omit to validate the \
                                current process model."
                        }
                    }
                }),
            });
            specs.push(ToolSpec {
                name: "edit_model".into(),
                description: "AUTHOR a candidate BPMN variant by applying VALIDATED structured \
                    operations to the current model — instead of hand-writing whole-document XML \
                    (which is error-prone: a misspelled element or a misplaced attribute silently \
                    breaks the model). This tool OWNS XML correctness: it parses the base with the \
                    engine's parser, applies your ops to the model, and re-emits engine-validated \
                    XML, returning the new full `model` (ready to pass straight to simulate / \
                    compare_variants), the `appliedOps` notes, and the post-edit analyze_model \
                    findings. PREFER THIS over typing BPMN by hand. The serializer also gives the \
                    authored model a generated left-to-right diagram and a human `name=` on every \
                    node, so name your nodes well (see set_name / the optional `name` on insert ops) \
                    — that label is what a person reads when the variant is rendered or downloaded. \
                    Args: ops (array, applied in \
                    order), base (optional BPMN XML to edit; omit to edit the current process \
                    model). Each op is an object with an `op` field:\n\
                    • set_task_job_type {task, jobType} — change a serviceTask's job type.\n\
                    • set_name {node, name} — set a node's human-readable label (the text shown in \
                    the diagram). Use plain language, e.g. \"Run fraud screen\", not the id.\n\
                    • set_flow_condition {from, to, condition} — set/replace (empty clears) the \
                    FEEL guard on the flow from->to.\n\
                    • insert_service_task_after {after, id, jobType, name?} — splice a new \
                    serviceTask onto `after`'s outgoing edge (after -> NEW -> original targets); \
                    pass `name` to label it readably.\n\
                    • add_error_boundary {task, errorCode, target, id?, name?} — attach an error \
                    boundary \
                    to a serviceTask routing to `target` (synthesizes the <bpmn:error> + errorRef \
                    for you — the exact thing models get wrong by hand).\n\
                    • reroute_flow {from, to, newTo} — repoint the flow from->to at newTo.\n\
                    • remove_node {id} — delete a node and reconnect its predecessors to its \
                    successors (and drop any boundary events attached to it).\n\
                    • add_exclusive_gateway {id, after, branches:[{to, condition?}]} — splice an \
                    XOR gateway after a node; `after`'s original target(s) are kept as the default \
                    branch.\n\
                    For a multi-stage ORCHESTRATOR, node ids live INSIDE a called phase, not in \
                    the orchestrator. Pass `process:\"<id>\"` (a callActivity's calledElement, \
                    e.g. from read_model's calledElement or read_model_xml) to edit that phase; \
                    its node ids are then the LOCAL ones (the part after `$` in a Parent$Child \
                    trace id). Omit `process` to edit the orchestrator itself. The other phases \
                    and the overview diagram are preserved."
                    .into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "ops": {
                            "type": "array",
                            "description": "Structured edit operations, applied in order. Each is \
                                an object whose `op` field names the operation (see the tool \
                                description for the per-op fields).",
                            "items": { "type": "object" }
                        },
                        "base": {
                            "type": "string",
                            "description": "BPMN XML to edit. Omit to edit the current process \
                                model. Pass a previous edit_model `model` to chain edits."
                        },
                        "process": {
                            "type": "string",
                            "description": "For a multi-stage model: the called phase (process id / \
                                callActivity calledElement) whose LOCAL node ids the ops address. \
                                Omit to edit the orchestrator."
                        }
                    },
                    "required": ["ops"]
                }),
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
                    output keys it failed to reproduce. This is SAFE, FAST and CHEAP: it runs an \
                    in-process engine, deploys nothing to production, and cannot break anything — \
                    so REACH FOR IT EARLY and OFTEN instead of reasoning in your head. Best loop: \
                    first `limit:1` to validate the model parses and conserves a single instance, \
                    then `limit:25` to catch obvious issues on a small sample, then drop `limit` \
                    (or use compare_variants) for the full-dataset verdict. Author a full variant of \
                    the current model (use read_model first) and pass its XML. If your variant adds \
                    a NEW worker (a job type history never recorded), supply a generative mock for \
                    it in `mockWorkers` so it can still be scored (fidelityTier becomes \
                    'mocked-replay', a Level-3 assumption-based result) instead of being unscorable. \
                    Needs recorded-input capture; if none is available it returns replayable=false \
                    with the reason. Args: model (BPMN XML, required), name, rationale, mockWorkers, \
                    limit (replay only the first N instances — for a quick smoke before the full run)."
                    .into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "model": { "type": "string", "description": "Candidate BPMN XML to replay." },
                        "name": { "type": "string", "description": "Short label for the variant." },
                        "rationale": { "type": "string", "description": "Why this variant was proposed." },
                        "limit": {
                            "type": "integer",
                            "description": "Replay only the first N recorded instances (a cheap \
                                smoke). Use 1 to validate the model, ~25 for a quick sample, then \
                                omit it for the full dataset. Absent = replay everything."
                        },
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
                                across instances reproducibly, ~70/30 here). To MOCK A FAILURE \
                                (exercise an error boundary), give an outcome (or a static spec) a \
                                \"throwError\": \"<ERROR_CODE>\" instead of \"output\" — the worker \
                                raises that BPMN business error rather than completing. e.g. fail \
                                10% of the population: {\"credit-check\": {\"outcomes\": [{\"weight\": \
                                0.9, \"output\": {\"score\": 700}}, {\"weight\": 0.1, \"throwError\": \
                                \"CREDIT_DECLINED\"}]}} — pair it with an error boundary on the task \
                                whose errorRef resolves to CREDIT_DECLINED. Supply one entry per new \
                                job type; existing (recorded) job types do not need a mock.",
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
                    real history. Returns datasetSize (instances replayed) and populationTotal \
                    (the full recorded population), the ranked candidates with their scorecards, \
                    and the best one. This is SAFE, FAST and CHEAP — it deploys nothing and cannot \
                    break production; tip: validate each variant with a single `simulate(limit:1)` \
                    first, then compare the survivors here. You can also pass `limit` to compare on \
                    a small sample before the full run. Needs recorded-input capture. A variant that \
                    adds a NEW worker can be scored by supplying a generative mock for it — either \
                    per candidate (mockWorkers on that item) or a top-level mockWorkers shared by \
                    all. Args: candidates (array of {name, model (BPMN XML), rationale?, \
                    mockWorkers?}), includeBaseline (bool), mockWorkers (shared map), limit \
                    (replay only the first N instances)."
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
                                            downstream split. To mock a FAILURE, give an outcome (or a \
                                            static spec) \"throwError\":\"<ERROR_CODE>\" instead of \
                                            \"output\" — the worker raises that BPMN business error so \
                                            an error boundary is exercised.",
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
                        "limit": {
                            "type": "integer",
                            "description": "Replay only the first N recorded instances (a cheap \
                                sample) before the full comparison. Absent = compare on everything."
                        },
                        "mockWorkers": {
                            "type": "object",
                            "description": "Generative mocks for new workers shared by ALL candidates. \
                                Each job type maps to a static output object (deterministic), e.g. \
                                {\"fraud-check\": {\"isFraud\": false}}, or a {\"outcomes\": [{\"weight\", \
                                \"output\"}]} distribution for a non-deterministic worker (e.g. a \
                                preApproved true/false split spread across the population). An outcome \
                                with \"throwError\":\"<ERROR_CODE>\" (instead of \"output\") mocks a \
                                FAILURE, raising that BPMN business error to exercise an error boundary.",
                            "additionalProperties": { "type": "object" }
                        }
                    },
                    "required": ["candidates"]
                }),
            });
        }
        if self.sub.is_some() {
            specs.push(ToolSpec {
                name: "delegate".into(),
                description: "Delegate a SELF-CONTAINED research sub-task to a subagent that runs \
                    in its OWN context with read-only tools and reports back a compact digest. Use \
                    this to keep YOUR context lean: offload noisy multi-query exploration (mapping \
                    a job's queue/failure profile, scanning a time window, characterising one job \
                    type) and get back only the conclusion + key figures — not dozens of raw query \
                    results. Give a crisp task with success criteria; the subagent cannot edit the \
                    model or run Python. Returns the subagent's digest (truncated if very long)."
                    .into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "task": {
                            "type": "string",
                            "description": "The self-contained research question to investigate, \
                                with enough context to act without your transcript."
                        }
                    },
                    "required": ["task"]
                }),
            });
        }
        // Operator-authored custom tools (already filtered to enabled).
        for def in &self.custom {
            specs.push(ToolSpec {
                name: def.name.clone(),
                description: def.description.clone(),
                parameters: def.parameters.clone(),
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
            "scale_workers" => {
                let job_type = args.get("jobType").and_then(|v| v.as_str());
                let target = args
                    .get("targetP99WaitMs")
                    .and_then(|v| v.as_u64())
                    .filter(|&t| t > 0)
                    .unwrap_or(1000);
                let worker_counts: Vec<u32> = args
                    .get("workerCounts")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_u64())
                            .filter(|&n| n > 0 && n <= u32::MAX as u64)
                            .map(|n| n as u32)
                            .collect()
                    })
                    .unwrap_or_default();
                let loads = self.analysis.job_loads(job_type)?;
                if loads.is_empty() {
                    let which = job_type
                        .map(|j| format!("job type '{j}' has"))
                        .unwrap_or_else(|| "no job types have".to_string());
                    return Err(format!(
                        "scale_workers: {which} no recorded jobs to fit a queueing model from"
                    ));
                }
                let scaled: Vec<_> = loads
                    .iter()
                    .map(|l| crate::harness::queueing::scale_job(l, target, &worker_counts))
                    .collect();
                serde_json::to_string(&json!({
                    "targetP99WaitMs": target,
                    "model": "M/M/c fitted from recorded arrival rate and service time",
                    "jobs": scaled,
                }))
                .map_err(|e| format!("serialise scaling: {e}"))
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
                let expand = args
                    .get("expand")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let v = if expand {
                    crate::bpmn_model::read_model_expanded(xml)?
                } else {
                    crate::bpmn_model::read_model(xml)?
                };
                serde_json::to_string(&v).map_err(|e| format!("serialise model: {e}"))
            }
            "read_model_xml" => {
                let xml = self
                    .model
                    .as_ref()
                    .ok_or("read_model_xml is not available: this process has no BPMN model")?;
                let target = args.get("process").and_then(|v| v.as_str());
                let v = crate::bpmn_model::read_model_xml(xml, target)?;
                serde_json::to_string(&v).map_err(|e| format!("serialise model xml: {e}"))
            }
            "analyze_model" => {
                let xml = self
                    .model
                    .as_ref()
                    .ok_or("analyze_model is not available: this process has no BPMN model")?;
                let v = crate::bpmn_model::analyze_model(xml)?;
                serde_json::to_string(&v).map_err(|e| format!("serialise findings: {e}"))
            }
            "validate_model" => {
                let xml = match args["model"].as_str() {
                    Some(m) => m,
                    None => self.model.as_deref().ok_or(
                        "validate_model needs a 'model' argument: this process has no BPMN model \
                         to fall back to",
                    )?,
                };
                let v = crate::bpmn_model::validate_model(xml)?;
                serde_json::to_string(&v).map_err(|e| format!("serialise validation: {e}"))
            }
            "edit_model" => {
                let ops = args
                    .get("ops")
                    .and_then(|v| v.as_array())
                    .ok_or("edit_model requires an 'ops' array")?;
                let base = match args.get("base").and_then(|v| v.as_str()) {
                    Some(b) => b,
                    None => self.model.as_deref().ok_or(
                        "edit_model needs a 'base' argument: this process has no BPMN model to \
                         edit",
                    )?,
                };
                let process = args.get("process").and_then(|v| v.as_str());
                let v = match process {
                    Some(p) => crate::bpmn_model::edit_model_in(base, ops, Some(p))?,
                    None => crate::bpmn_model::edit_model(base, ops)?,
                };
                serde_json::to_string(&v).map_err(|e| format!("serialise edit: {e}"))
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
                let ds = self.recorded.as_ref().ok_or(
                    "compare_variants is not available: no recorded dataset for this process",
                )?;
                let v = crate::experiment::compare_variants(self.model.as_deref(), ds, args)?;
                serde_json::to_string(&v).map_err(|e| format!("serialise comparison: {e}"))
            }
            "delegate" => {
                let sub = self
                    .sub
                    .as_ref()
                    .ok_or("delegate is not enabled for this investigation")?;
                let task = args["task"]
                    .as_str()
                    .ok_or("delegate requires a string 'task' argument")?
                    .trim();
                if task.is_empty() {
                    return Err("delegate requires a non-empty 'task'".into());
                }
                // The subagent runs in its OWN context with its OWN dataset (independent DuckDB
                // connection built from a cloned source) and read-only tools — no delegate (no
                // recursion), no edit/python. We surface only its capped digest, protecting the
                // primary's context window. Build it on a fresh thread+runtime so it works under
                // either runtime flavour and never shares the non-Send analysis.
                let cfg = sub.cfg.clone();
                let system = sub.system.clone();
                let src = sub
                    .src
                    .clone()
                    .ok_or("delegate: subagent has no bound dataset")?;
                let limit = sub.limit;
                let model_xml = sub.model.clone();
                let max_rounds = sub.max_rounds;
                let task_owned = task.to_string();
                let digest = std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| format!("subagent runtime: {e}"))?;
                    rt.block_on(async move {
                        let analysis = Analysis::from_source(&src, limit).await?;
                        let research = ResearchTools {
                            analysis: &analysis,
                            model: model_xml.as_deref(),
                        };
                        let model = OpenAiAgent { cfg };
                        run_agent(&model, &research, &system, &task_owned, max_rounds)
                            .await
                            .map(|run| run.answer)
                    })
                })
                .join()
                .map_err(|_| "subagent thread panicked".to_string())??;
                let digest = clip_digest(&digest, sub.digest_cap);
                let digest = clip_digest(&digest, sub.digest_cap);
                Ok(serde_json::json!({
                    "subagent": sub.name,
                    "task": task,
                    "digest": digest,
                })
                .to_string())
            }
            other => {
                if let Some(def) = self.custom.iter().find(|d| d.name == other) {
                    self.call_custom(def, args)
                } else {
                    Err(format!("unknown tool '{other}'"))
                }
            }
        }
    }
}

/// Truncate a subagent digest to `cap` chars, appending a marker so the primary knows it was cut.
fn clip_digest(s: &str, cap: usize) -> String {
    if s.len() <= cap {
        return s.to_string();
    }
    let mut end = cap.min(s.len());
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}\n…[digest truncated to {cap} chars]", &s[..end])
}

/// A lightweight [`ToolBox`] adapter that scopes a shared [`AnalysisTools`] to an **allowlist** of
/// tool names — so the primary and a paired second model can be handed *different* surfaces over
/// the SAME underlying dataset (no DuckDB clone). `allow == None` means "all tools" (the historical
/// behaviour); `Some(set)` filters both the advertised `specs()` and the dispatchable `call()`.
/// Capability gating still applies underneath: an allowlisted tool whose prerequisite (model,
/// Python, recorded inputs, subagent) is absent simply isn't advertised by the inner box.
pub struct ScopedTools<'a> {
    inner: &'a AnalysisTools,
    allow: Option<std::collections::BTreeSet<String>>,
}

impl<'a> ScopedTools<'a> {
    /// Wrap `inner` with an optional allowlist of tool names. An empty list is treated as "all"
    /// (so a caller that forgot to choose doesn't accidentally disable every tool).
    pub fn new(inner: &'a AnalysisTools, allow: Option<Vec<String>>) -> Self {
        let allow = allow.filter(|v| !v.is_empty()).map(|v| {
            v.into_iter()
                .collect::<std::collections::BTreeSet<String>>()
        });
        Self { inner, allow }
    }

    fn allowed(&self, name: &str) -> bool {
        self.allow
            .as_ref()
            .map(|s| s.contains(name))
            .unwrap_or(true)
    }
}

impl ToolBox for ScopedTools<'_> {
    fn specs(&self) -> Vec<ToolSpec> {
        self.inner
            .specs()
            .into_iter()
            .filter(|s| self.allowed(&s.name))
            .collect()
    }

    fn call(&self, name: &str, args: &Value) -> Result<String, String> {
        if !self.allowed(name) {
            return Err(format!("tool '{name}' is not enabled for this model"));
        }
        self.inner.call(name, args)
    }
}

/// The read-only research surface a delegated subagent gets: the same trace/model/replay tools
/// as the primary, but borrowing the parent's data and OMITTING `delegate` (no recursion),
/// `run_python` and `edit_model` (no side effects). Keeps a subagent strictly an investigator.
struct ResearchTools<'a> {
    analysis: &'a Analysis,
    model: Option<&'a str>,
}

impl ToolBox for ResearchTools<'_> {
    fn specs(&self) -> Vec<ToolSpec> {
        let mut specs = vec![
            ToolSpec {
                name: "query_traces".into(),
                description: format!(
                    "Run a single read-only DuckDB SQL query (SELECT/WITH only) over the captured \
                     trace dataset and return the (row-capped) result.\n\n{}",
                    self.analysis.schema_doc()
                ),
                parameters: json!({
                    "type": "object",
                    "properties": { "sql": { "type": "string" } },
                    "required": ["sql"]
                }),
            },
            ToolSpec {
                name: "discover_flow".into(),
                description: "Mine the directly-follows graph the trace implies. Takes no \
                    arguments."
                    .into(),
                parameters: json!({ "type": "object", "properties": {} }),
            },
        ];
        if self.model.is_some() {
            specs.push(ToolSpec {
                name: "read_model".into(),
                description: "Structural view of the BPMN model. Args: expand (boolean).".into(),
                parameters: json!({ "type": "object", "properties": { "expand": {"type":"boolean"} } }),
            });
        }
        specs
    }

    fn call(&self, name: &str, args: &Value) -> Result<String, String> {
        match name {
            "query_traces" => {
                let sql = args["sql"].as_str().ok_or("query_traces requires 'sql'")?;
                serde_json::to_string(&self.analysis.query(sql)?)
                    .map_err(|e| format!("serialise result: {e}"))
            }
            "discover_flow" => {
                serde_json::to_string(&crate::conformance::discover_flow(self.analysis)?)
                    .map_err(|e| format!("serialise flow: {e}"))
            }
            "read_model" => {
                let xml = self
                    .model
                    .ok_or("read_model: this process has no BPMN model")?;
                let expand = args
                    .get("expand")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let v = if expand {
                    crate::bpmn_model::read_model_expanded(xml)?
                } else {
                    crate::bpmn_model::read_model(xml)?
                };
                serde_json::to_string(&v).map_err(|e| format!("serialise model: {e}"))
            }
            other => Err(format!("subagent has no tool '{other}'")),
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

/// One stage of a Pair AI pipeline: a reviewer agent that runs *after* the primary (and after
/// any earlier pair stages), receiving the previous stage's answer as input. A `Vec<PairStage>`
/// is an N-tier sequential chain; the cockpit currently configures one, but the orchestration
/// in [`run_chat_turn`] generalises to any number.
#[derive(Clone)]
pub struct PairStage {
    /// The reviewer persona id (provenance / display).
    pub id: String,
    /// Human-friendly reviewer name shown in the cockpit (e.g. "Skeptic / Red-Team").
    pub name: String,
    /// The reviewer's LLM connection (its own profile — typically a different model/family).
    pub cfg: LlmConfig,
    /// The reviewer persona's system prompt.
    pub system: String,
    /// Optional allowlist of tool names this reviewer may use. `None` = the full surface.
    pub tools: Option<Vec<String>>,
}

/// Frame the handoff from the previous agent to the next reviewer: the operator's question plus
/// the prior answer, presented as a claim to verify rather than a fact to trust.
fn pair_handoff(user_message: &str, prior_answer: &str) -> String {
    format!(
        "The operator asked:\n\"{}\"\n\nA primary analyst reviewed the dataset and answered \
         (treat this as a CLAIM to verify against the data, not an established fact):\n\n--- BEGIN \
         PRIMARY ANSWER ---\n{}\n--- END PRIMARY ANSWER ---\n\nNow do your job as described in your \
         instructions: investigate with the tools, then deliver your response.",
        user_message.trim(),
        prior_answer.trim()
    )
}

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
    pairs: &[PairStage],
    subagent: Option<SubAgent>,
    custom_tools: Vec<ToolDef>,
    primary_tools: Option<Vec<String>>,
    mut messages: Vec<Msg>,
    user_message: &str,
    mut checkpoint: Checkpoint<'_>,
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
    let has_model = model_xml
        .as_ref()
        .map(|x| !x.trim().is_empty())
        .unwrap_or(false);
    let model_for_sub = model_xml.clone();
    tools.set_model(model_xml);
    if has_model {
        let cap = crate::experiment::recorded_cap(src);
        let recorded = crate::experiment::build_recorded_dataset(src, cap).await;
        tools.set_recorded(Some(recorded));
    }
    tools.set_subagent(subagent.map(|mut s| {
        s.src = Some(src.clone());
        s.limit = limit;
        s.model = model_for_sub;
        s
    }));
    tools.set_custom_tools(custom_tools);
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

    let cp_fwd: Checkpoint<'_> = match checkpoint {
        Some(ref mut c) => Some(&mut **c),
        None => None,
    };
    let primary_scoped = ScopedTools::new(&tools, primary_tools);
    let run = run_agent_streaming(
        &model,
        &primary_scoped,
        &mut messages,
        max_rounds,
        cancel,
        steer,
        sink,
        cp_fwd,
    )
    .await?;
    let mut total_rounds = run.rounds;

    // Pair AI: after the primary, run each reviewer in sequence over the SAME data/model tools,
    // handing the previous answer forward. Each reviewer runs in its own sub-conversation (its
    // system prompt + the handoff), so the primary transcript stays a single coherent thread; we
    // append only the reviewer's final answer, marked for provenance and carried into next turn.
    let mut prior_answer = run.answer.clone();
    for stage in pairs {
        sink(AgentEvent::Agent {
            id: stage.id.clone(),
            name: stage.name.clone(),
            role: "pair".into(),
        });
        let reviewer = OpenAiAgent {
            cfg: stage.cfg.clone(),
        };
        let sys = format!(
            "{}\n\nDataset bound for this review: {} instances, {} job executions, {} \
             incidents. You may use the same read-only query/model tools as the primary.",
            stage.system, dataset.instances, dataset.jobs, dataset.incidents
        );
        let mut pair_msgs = vec![
            Msg::System(sys),
            Msg::User(pair_handoff(user_message, &prior_answer)),
        ];
        let pair_scoped = ScopedTools::new(&tools, stage.tools.clone());
        let pair_run = run_agent_streaming(
            &reviewer,
            &pair_scoped,
            &mut pair_msgs,
            max_rounds,
            cancel,
            steer,
            sink,
            None,
        )
        .await?;
        total_rounds += pair_run.rounds;
        prior_answer = pair_run.answer.clone();
        messages.push(Msg::Assistant {
            text: Some(crate::chat::mark_pair(&stage.name, &pair_run.answer)),
            tool_calls: Vec::new(),
        });
    }

    Ok(ChatTurnResult {
        answer: run.answer,
        rounds: total_rounds,
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
    use std::cell::Cell;
    use std::path::Path;

    use super::*;
    use crate::agent::{AgentStep, Msg, ToolCall, Turn};
    use crate::corpus;

    /// Build a real corpus + Analysis the same way the generator does, returning the
    /// tools over it.
    fn corpus_tools() -> AnalysisTools {
        let def = corpus::tests::loan_def();
        let pack = corpus::tests::loan_pack();
        // Unique per call: these tests run in parallel and each removes its own
        // dir, so a shared per-PID path would race (one test's cleanup deletes
        // another's dir mid-write).
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp = std::env::temp_dir().join(format!("invest-test-{}-{n}", std::process::id()));
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
        let err = tools
            .call("run_python", &json!({"code": "print(1)"}))
            .unwrap_err();
        assert!(err.contains("not enabled"), "got: {err}");
    }
}
