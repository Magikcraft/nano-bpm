//! ProcessOS server — Stage T1.
//!
//! The optimization plane runs as its **own process with its own webserver**,
//! separate from the Nano gateway: *Nano handles production, ProcessOS handles
//! optimization.* This binary ingests Nano's public trace/metrics export and serves
//! an Insights report (`GET /api/insights`) plus a small dashboard.
//!
//! The dependency is strictly one-way — ProcessOS reads Nano over HTTP; Nano never
//! links or knows about ProcessOS. Run a Nano cluster without this binary and
//! production is unaffected.

mod contracts;
mod cockpit;
mod harness;
mod report;

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;

use crate::contracts::NanoClient;
use crate::harness::{
    apply_calibration, build_baseline, build_cluster_summary, build_evolve_prompt,
    calibrate_from_measured, example_scenario, llm_complete, parse_structural_candidates,
    rank_candidates_by_replay, replay_dataset, replay_instance, run_hypothesis, run_scenario,
    staff_for_summary, summarize_dataset, CandidateModel, LlmConfig, LlmOverride, MeasuredJobType,
    Prompt, PromptLibrary, RecordedInstance, Scenario, DEFAULT_EVOLVE_SYSTEM_PROMPT,
    DEFAULT_PROMPT_ID, DEFAULT_SYSTEM_PROMPT,
};
use nanobpmn_engine_core::bpmn::parse_bpmn;

#[derive(Clone)]
struct AppState {
    nano: NanoClient,
    /// The prompt library — import / select / author the system prompts that drive
    /// hypothesis generation. Shared, interior-mutable so authoring is live.
    prompts: Arc<RwLock<PromptLibrary>>,
}

/// Server configuration, all overridable by environment.
struct Config {
    port: u16,
    nano_base_url: String,
    /// Optional directory to import prompts from on startup (`PROCESSOS_PROMPTS_DIR`).
    prompts_dir: Option<String>,
}

impl Config {
    fn from_env() -> Self {
        let port = std::env::var("PROCESSOS_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8090);
        let nano_base_url = std::env::var("NANO_BASE_URL")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "http://localhost:8080".to_string());
        let prompts_dir = std::env::var("PROCESSOS_PROMPTS_DIR")
            .ok()
            .filter(|s| !s.is_empty());
        Self {
            port,
            nano_base_url,
            prompts_dir,
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let cfg = Config::from_env();

    // Seed the prompt library with the built-in default, then import any prompts the
    // operator dropped in PROCESSOS_PROMPTS_DIR.
    let mut library = PromptLibrary::seeded(DEFAULT_SYSTEM_PROMPT);
    if let Some(dir) = &cfg.prompts_dir {
        match library.import_dir(std::path::Path::new(dir)) {
            Ok(n) => tracing::info!(dir = %dir, imported = n, "imported prompts"),
            Err(e) => tracing::warn!(dir = %dir, error = %e, "prompt import failed"),
        }
    }

    let state = AppState {
        nano: NanoClient::new(&cfg.nano_base_url),
        prompts: Arc::new(RwLock::new(library)),
    };

    let app = Router::new()
        .route("/", get(landing))
        .route("/features", get(features))
        .route("/console", get(dashboard))
        .route("/cockpit", get(cockpit_page))
        .route("/health", get(health))
        .route("/api/insights", get(insights))
        .route("/api/cockpit/overview", get(cockpit_overview))
        .route("/api/cockpit/experiments", get(cockpit_experiments).post(cockpit_create))
        .route("/api/cockpit/experiments/{key}", get(cockpit_experiment))
        .route("/api/cockpit/experiments/{key}/decision", post(cockpit_decision))
        .route("/harness", get(harness_dashboard))
        .route("/api/harness/example", get(harness_example))
        .route("/api/harness/example/run", get(harness_example_run))
        .route("/api/harness/run", post(harness_run))
        .route("/api/harness/calibrate", post(harness_calibrate))
        .route("/api/harness/hypothesize", post(harness_hypothesize))
        .route("/api/harness/replay", post(harness_replay))
        .route("/api/harness/replay-batch", post(harness_replay_batch))
        .route("/api/harness/replay-rank", post(harness_replay_rank))
        .route("/api/harness/evolve", post(harness_evolve))
        .route("/api/harness/production", get(harness_production))
        .route("/api/harness/cluster", get(harness_cluster))
        .route("/api/prompts", get(prompts_list).post(prompts_upsert))
        .route("/api/prompts/{id}", get(prompts_get).delete(prompts_delete))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("ProcessOS could not bind {addr}: {e}"));

    tracing::info!(
        %addr,
        nano = %cfg.nano_base_url,
        "ProcessOS (T1: Insights) listening; reading Nano over the public trace/metrics contract"
    );
    println!("PROCESSOS_PORT={}", cfg.port);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("ProcessOS server error");
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn health() -> &'static str {
    "ok"
}

#[derive(Debug, Deserialize)]
struct InsightsQuery {
    /// How many recent trace summaries to scan for totals.
    limit: Option<usize>,
    /// How many of those to pull full detail for (per-element aggregation).
    sample: Option<usize>,
}

/// `GET /api/insights` — the folded performance report. On a Nano read failure we
/// return 502 with the underlying error so the dashboard can show it plainly.
async fn insights(
    State(state): State<AppState>,
    Query(q): Query<InsightsQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    let sample = q.sample.unwrap_or(50).clamp(1, 500);
    match report::build(&state.nano, limit, sample).await {
        Ok(report) => Json(report).into_response(),
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

/// The standalone marketing landing page for Nano Process OS (self-contained:
/// inline canvas particle field, no external assets). Served at `/`.
const LANDING_HTML: &str = include_str!("landing.html");

/// The features page, leading with the two flagship capabilities. Served at
/// `/features`.
const FEATURES_HTML: &str = include_str!("features.html");

/// `GET /` — the Nano Process OS landing page.
async fn landing() -> Html<&'static str> {
    Html(LANDING_HTML)
}

/// `GET /features` — what Process OS does.
async fn features() -> Html<&'static str> {
    Html(FEATURES_HTML)
}

/// A dependency-free single-file dashboard that fetches `/api/insights` and renders
/// the report. Served at `/console`. Intentionally tiny — the richer UX is the console
/// "Optimization" tab that proxies to this server (see docs/processos-design.md §4).
async fn dashboard() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

// --- The cockpit — Console → Process → Experiment (design §7.8 / §10) ------------

/// `GET /cockpit` — the single-file cockpit app (left rail, process drilldown,
/// the four-step experiment stepper, and the persistent droid-conversation pane).
async fn cockpit_page() -> Html<&'static str> {
    Html(COCKPIT_HTML)
}

/// `GET /api/cockpit/overview` — target-process cards + the experiments list.
async fn cockpit_overview(
    State(state): State<AppState>,
    Query(q): Query<InsightsQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    let sample = q.sample.unwrap_or(50).clamp(1, 500);
    match cockpit::overview(&state.nano, limit, sample).await {
        Ok(o) => Json(o).into_response(),
        Err(e) => bad_gateway(e),
    }
}

/// `GET /api/cockpit/experiments` — every experiment (pilot instance), newest first.
async fn cockpit_experiments(
    State(state): State<AppState>,
    Query(q): Query<InsightsQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    match cockpit::list_experiments(&state.nano, limit).await {
        Ok(xs) => Json(xs).into_response(),
        Err(e) => bad_gateway(e),
    }
}

/// `GET /api/cockpit/experiments/{key}` — the full cockpit view of one experiment.
async fn cockpit_experiment(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    match cockpit::experiment_detail(&state.nano, &key).await {
        Ok(d) => Json(d).into_response(),
        Err(e) => bad_gateway(e),
    }
}

/// `POST /api/cockpit/experiments` — start an experiment for a target process. If
/// no `baselineModel` is supplied, the latest deployed BPMN for `processId` is used.
async fn cockpit_create(
    State(state): State<AppState>,
    Json(req): Json<CockpitCreateRequest>,
) -> impl IntoResponse {
    let baseline = match req.baseline_model {
        Some(b) if !b.trim().is_empty() => b,
        _ => match cockpit::latest_process_xml(&state.nano, &req.process_id).await {
            Ok(xml) => xml,
            Err(e) => return bad_gateway(e),
        },
    };
    let max_iterations = req.max_iterations.unwrap_or(2).clamp(1, 50);
    match cockpit::create_experiment(&state.nano, &req.process_id, baseline, max_iterations).await {
        Ok(key) => Json(serde_json::json!({ "instanceKey": key })).into_response(),
        Err(e) => bad_gateway(e),
    }
}

/// `POST /api/cockpit/experiments/{key}/decision` — the pilot's turn at `Review`.
async fn cockpit_decision(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(req): Json<CockpitDecisionRequest>,
) -> impl IntoResponse {
    let decision = req.decision.trim();
    if !matches!(decision, "accept" | "iterate" | "stop") {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "decision must be accept | iterate | stop" })),
        )
            .into_response();
    }
    match cockpit::submit_decision(&state.nano, &key, decision).await {
        Ok(()) => Json(serde_json::json!({ "ok": true })).into_response(),
        Err(e) => bad_gateway(e),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CockpitCreateRequest {
    process_id: String,
    #[serde(default)]
    baseline_model: Option<String>,
    #[serde(default)]
    max_iterations: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct CockpitDecisionRequest {
    decision: String,
}

fn bad_gateway(e: String) -> axum::response::Response {
    (
        StatusCode::BAD_GATEWAY,
        Json(serde_json::json!({ "error": e })),
    )
        .into_response()
}


// --- The optimization harness (MVP, design §7) ----------------------------------

/// `GET /api/harness/example` — the bundled example scenario JSON, so callers have
/// a ready template to copy and adapt.
async fn harness_example() -> impl IntoResponse {
    Json(example_scenario())
}

/// `GET /api/harness/example/run` — run the bundled example and return its ranking.
async fn harness_example_run() -> impl IntoResponse {
    run_scenario_blocking(example_scenario()).await
}

/// `POST /api/harness/run` — run a caller-supplied scenario and return its ranking.
async fn harness_run(Json(scenario): Json<Scenario>) -> impl IntoResponse {
    run_scenario_blocking(scenario).await
}

/// Request body for the calibration path: a scenario plus the measured per-job-type
/// distributions to ground its baseline workers in.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CalibrateRequest {
    scenario: Scenario,
    /// Measured per-job-type behaviour (e.g. the `byJobType` array from
    /// `GET /api/harness/cluster`, or any equivalent observation).
    #[serde(default)]
    measured: Vec<MeasuredJobType>,
}

/// `POST /api/harness/calibrate` — ground a scenario's baseline workers in measured
/// production, then rank over the calibrated model. Returns the calibration (which
/// job types were covered, and the resulting worker pool) alongside the ranking, so
/// the same loop's verdicts reflect the service times and failure rates the cluster
/// actually observed rather than hand-authored numbers. No LLM or live cluster
/// required — the caller supplies the measured distributions.
async fn harness_calibrate(Json(req): Json<CalibrateRequest>) -> impl IntoResponse {
    let calibration = calibrate_from_measured(&req.scenario, &req.measured);
    let calibrated_scenario = apply_calibration(&req.scenario, &calibration);
    let scenario = calibrated_scenario.clone();
    let ranked =
        tokio::task::spawn_blocking(move || run_scenario(&scenario)).await;
    match ranked {
        Ok(Ok(report)) => Json(serde_json::json!({
            "calibration": calibration,
            "report": report,
        }))
        .into_response(),
        Ok(Err(e)) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("join error: {e}") })),
        )
            .into_response(),
    }
}

/// Request body for the LLM hypothesis path: a scenario plus optional LLM
/// overrides and a flag to also evaluate the baked grid for comparison.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HypothesizeRequest {
    scenario: Scenario,
    #[serde(default)]
    llm: Option<LlmOverride>,
    #[serde(default)]
    include_baked: bool,
    /// Optional measured per-job-type distributions. When present, the scenario's
    /// baseline workers are calibrated from production before the LLM reasons over
    /// it and before any candidate is scored — so the bar the hypotheses must beat
    /// reflects measured reality, not hand-authored numbers.
    #[serde(default)]
    measured: Vec<MeasuredJobType>,
    /// Select a system prompt from the library by id. Defaults to the built-in
    /// `default` when absent.
    #[serde(default)]
    prompt_id: Option<String>,
    /// Inline system-prompt override (highest precedence) — author/experiment with a
    /// prompt for this single run without storing it.
    #[serde(default)]
    prompt: Option<String>,
}

/// `POST /api/harness/hypothesize` — ask the configured LLM to propose candidates,
/// evaluate + rank them with the SimRunner, and return the report. The model is
/// reached via the pluggable client (local llama.cpp / Ollama / vLLM, or Anthropic).
async fn harness_hypothesize(
    State(state): State<AppState>,
    Json(req): Json<HypothesizeRequest>,
) -> impl IntoResponse {
    let mut cfg = LlmConfig::from_env();
    if let Some(o) = &req.llm {
        cfg = cfg.with_override(o);
    }
    if !cfg.is_ready() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "no LLM model configured; set PROCESSOS_LLM_MODEL (and \
                          PROCESSOS_LLM_BASE_URL / PROCESSOS_LLM_PROVIDER as needed) \
                          or pass an `llm` object with at least `model` in the request body"
            })),
        )
            .into_response();
    }

    // Resolve the system prompt: an inline override wins; otherwise a selected
    // library prompt; otherwise the built-in default.
    let system_prompt = match (&req.prompt, &req.prompt_id) {
        (Some(inline), _) => inline.clone(),
        (None, Some(id)) => match state.prompts.read().unwrap().system_of(id) {
            Some(s) => s,
            None => {
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(serde_json::json!({ "error": format!("unknown prompt id: {id}") })),
                )
                    .into_response();
            }
        },
        (None, None) => state
            .prompts
            .read()
            .unwrap()
            .system_of(DEFAULT_PROMPT_ID)
            .unwrap_or_else(|| DEFAULT_SYSTEM_PROMPT.to_string()),
    };

    // Ground the baseline in production when measured distributions are supplied,
    // so the LLM reasons over — and every candidate is scored against — the service
    // times and failure rates the cluster actually observed.
    let scenario = if req.measured.is_empty() {
        req.scenario.clone()
    } else {
        let calibration = calibrate_from_measured(&req.scenario, &req.measured);
        apply_calibration(&req.scenario, &calibration)
    };
    match run_hypothesis(&scenario, &cfg, req.include_baked, &req.measured, &system_prompt).await {
        Ok(report) => Json(report).into_response(),
        Err(e) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

/// Request body for the recorded-input replay verifier.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplayRequest {
    /// The recorded instance to replay. Its trace must carry the Tier-2 stimulus
    /// log — capture the source cluster with `c8 nano --capture`.
    instance_key: String,
    /// BPMN XML of the candidate model to evaluate against the recorded inputs.
    candidate_model: String,
    /// Process id to start; defaults to the recorded trace's own process id.
    #[serde(default)]
    process_id: Option<String>,
    /// Override the Nano gateway base url; defaults to the server's configured one.
    #[serde(default)]
    base_url: Option<String>,
}

/// `POST /api/harness/replay` — the **Level-2 verifier** (§7.7/§7.9). Fetch one
/// recorded instance's trace, replay its creation inputs + recorded job outputs
/// against the supplied candidate model on the real engine, and return the
/// **gradient**: validity, completion, per-job-type coverage (`uncoveredJobTypes`
/// ⇒ requires new workers) and boundary-conservation `divergences`. This is the
/// idempotent, directly-callable eval an agentic loop iterates against.
async fn harness_replay(
    State(state): State<AppState>,
    Json(req): Json<ReplayRequest>,
) -> impl IntoResponse {
    let client = match &req.base_url {
        Some(url) if !url.is_empty() => NanoClient::new(url),
        _ => state.nano.clone(),
    };
    let trace = match client.trace(&req.instance_key).await {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };
    let rec = match RecordedInstance::from_trace(&trace) {
        Ok(r) => r,
        Err(why) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({
                    "error": why.to_string(),
                    "instanceKey": req.instance_key,
                })),
            )
                .into_response();
        }
    };
    let defs = match parse_bpmn(&req.candidate_model) {
        Ok(d) if !d.is_empty() => d,
        Ok(_) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({
                    "error": "candidate model contained no process definitions"
                })),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({
                    "error": format!("candidate model failed to parse: {e:?}")
                })),
            )
                .into_response();
        }
    };
    let process_id = req
        .process_id
        .clone()
        .unwrap_or_else(|| rec.process_id.clone());
    let result = replay_instance(&defs, &process_id, &rec);
    Json(result).into_response()
}

/// Request body for the batch replay verifier.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplayBatchRequest {
    /// BPMN XML of the candidate model to score against the recorded dataset.
    candidate_model: String,
    /// Process whose recorded instances form the dataset. Defaults to the
    /// candidate's first process id.
    #[serde(default)]
    process_id: Option<String>,
    /// How many recent trace summaries to scan for matching instances.
    #[serde(default)]
    limit: Option<usize>,
    /// Override the Nano gateway base url; defaults to the server's configured one.
    #[serde(default)]
    base_url: Option<String>,
}

/// `POST /api/harness/replay-batch` — score one candidate against a **recorded
/// dataset** (§7.7 Level-2 / §7.9). Fetch the recent recorded instances of a
/// process, replay each against the candidate, and fold the per-instance gradients
/// into a single scorecard (`conservedRate`, `uncoveredJobTypes`, `divergentKeys`,
/// latency). This is the candidate scorer the hypothesis loop ranks with.
async fn harness_replay_batch(
    State(state): State<AppState>,
    Json(req): Json<ReplayBatchRequest>,
) -> impl IntoResponse {
    let client = match &req.base_url {
        Some(url) if !url.is_empty() => NanoClient::new(url),
        _ => state.nano.clone(),
    };
    let defs = match parse_bpmn(&req.candidate_model) {
        Ok(d) if !d.is_empty() => d,
        Ok(_) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({
                    "error": "candidate model contained no process definitions"
                })),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({
                    "error": format!("candidate model failed to parse: {e:?}")
                })),
            )
                .into_response();
        }
    };
    let process_id = req
        .process_id
        .clone()
        .unwrap_or_else(|| defs[0].id.clone());

    let limit = req.limit.unwrap_or(200);
    let summaries = match client.list_traces(limit).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };

    // Distil the recorded instances of this process, tracking why any are skipped
    // (capture off, truncated log/snapshot) so the operator can fix the source.
    let (dataset, matched, skipped, skip_reasons) =
        distil_recorded_dataset(&client, &process_id, &summaries).await;

    let report = replay_dataset(&defs, &process_id, &dataset);
    Json(serde_json::json!({
        "processId": process_id,
        "matchedInstances": matched,
        "skipped": skipped,
        "skipReasons": skip_reasons,
        "report": report,
    }))
    .into_response()
}

/// Distil the replayable recorded instances of a process from a list of trace
/// summaries (already fetched from Nano's read contract). Returns the dataset
/// plus skip accounting (matched, skipped, reasons) so callers can tell the
/// operator *why* instances were dropped (capture off, truncated log/snapshot,
/// fetch failure). Shared by replay-batch and replay-rank.
async fn distil_recorded_dataset(
    client: &NanoClient,
    process_id: &str,
    summaries: &[crate::contracts::TraceSummary],
) -> (
    Vec<RecordedInstance>,
    u32,
    u32,
    std::collections::HashMap<String, u32>,
) {
    let mut dataset: Vec<RecordedInstance> = Vec::new();
    let mut skipped = 0u32;
    let mut skip_reasons: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    let mut matched = 0u32;
    for s in summaries.iter().filter(|s| s.process_id == process_id) {
        matched += 1;
        let trace = match client.trace(&s.instance_key).await {
            Ok(t) => t,
            Err(_) => {
                skipped += 1;
                *skip_reasons.entry("trace fetch failed".to_string()).or_insert(0) += 1;
                continue;
            }
        };
        match RecordedInstance::from_trace(&trace) {
            Ok(r) => dataset.push(r),
            Err(why) => {
                skipped += 1;
                *skip_reasons.entry(why.to_string()).or_insert(0) += 1;
            }
        }
    }
    (dataset, matched, skipped, skip_reasons)
}

/// One candidate model in a replay-rank request.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RankCandidateBody {
    /// Display name for the candidate.
    name: String,
    /// Why it was proposed (carried through to the scorecard).
    #[serde(default)]
    rationale: Option<String>,
    /// BPMN XML of the candidate model.
    model: String,
}

/// Request body for the replay-rank population scorer.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplayRankRequest {
    /// The candidate population to score against the recorded dataset.
    candidates: Vec<RankCandidateBody>,
    /// Process whose recorded instances form the dataset. Required (the dataset is
    /// process-scoped and candidates may rename their process id).
    process_id: String,
    /// How many recent trace summaries to scan for matching instances.
    #[serde(default)]
    limit: Option<usize>,
    /// Override the Nano gateway base url; defaults to the server's configured one.
    #[serde(default)]
    base_url: Option<String>,
}

/// `POST /api/harness/replay-rank` — score a **population** of candidate models
/// against the same recorded dataset and rank them by the fidelity gradient
/// (§7.9). This is the gradient-driven loop's evaluation step: fan a set of
/// proposed redesigns over real production traces, rank fidelity-first
/// (`conservedRate`, then latency, then fewer required new workers), and hand the
/// operator a scorecard per candidate to decide on. The harness ranks; the human
/// (or the pilot process) decides.
async fn harness_replay_rank(
    State(state): State<AppState>,
    Json(req): Json<ReplayRankRequest>,
) -> impl IntoResponse {
    if req.candidates.is_empty() {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": "no candidates supplied" })),
        )
            .into_response();
    }
    let client = match &req.base_url {
        Some(url) if !url.is_empty() => NanoClient::new(url),
        _ => state.nano.clone(),
    };
    let limit = req.limit.unwrap_or(200);
    let summaries = match client.list_traces(limit).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };
    let (dataset, matched, skipped, skip_reasons) =
        distil_recorded_dataset(&client, &req.process_id, &summaries).await;

    if dataset.is_empty() {
        return (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({
                "error": "no replayable recorded instances for this process \
                          (was the gateway run with --capture?)",
                "processId": req.process_id,
                "matchedInstances": matched,
                "skipped": skipped,
                "skipReasons": skip_reasons,
            })),
        )
            .into_response();
    }

    let candidates: Vec<CandidateModel> = req
        .candidates
        .into_iter()
        .map(|c| CandidateModel {
            name: c.name,
            rationale: c.rationale,
            model: c.model,
        })
        .collect();

    let ranking = rank_candidates_by_replay(&candidates, &dataset, Some(&req.process_id));
    Json(serde_json::json!({
        "processId": req.process_id,
        "matchedInstances": matched,
        "skipped": skipped,
        "skipReasons": skip_reasons,
        "ranking": ranking,
    }))
    .into_response()
}

/// Request body for the LLM-propose → replay-rank loop.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EvolveRequest {
    /// BPMN XML of the current/baseline model the LLM redesigns from.
    baseline_model: String,
    /// Process whose recorded instances form the dataset + signal. Defaults to the
    /// baseline's first process id.
    #[serde(default)]
    process_id: Option<String>,
    /// How many recent trace summaries to scan for matching instances.
    #[serde(default)]
    limit: Option<usize>,
    /// Override the Nano gateway base url; defaults to the server's configured one.
    #[serde(default)]
    base_url: Option<String>,
    /// Per-request LLM overrides (model, base_url, provider, …).
    #[serde(default)]
    llm: Option<LlmOverride>,
    /// Inline system prompt override (wins over `promptId`).
    #[serde(default)]
    prompt: Option<String>,
    /// Select a library prompt by id; falls back to the built-in evolve prompt.
    #[serde(default)]
    prompt_id: Option<String>,
}

/// `POST /api/harness/evolve` — the gradient-driven loop end-to-end (§7.9): distil
/// the recorded production signal, ask the LLM to propose structural candidate
/// models, and rank them by replaying against the *real* recorded traces. The
/// droid proposes; the engine proves. Single-shot for now (the iterative loop is
/// authored as a Nano process in a later brick).
async fn harness_evolve(
    State(state): State<AppState>,
    Json(req): Json<EvolveRequest>,
) -> impl IntoResponse {
    // Resolve LLM config (env + per-request override); a model name is required.
    let mut cfg = LlmConfig::from_env();
    if let Some(o) = &req.llm {
        cfg = cfg.with_override(o);
    }
    if !cfg.is_ready() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "no LLM model configured; set PROCESSOS_LLM_MODEL (and \
                          PROCESSOS_LLM_BASE_URL / PROCESSOS_LLM_PROVIDER as needed) \
                          or pass an `llm` object with at least `model`"
            })),
        )
            .into_response();
    }

    // The baseline must parse so we can derive the default process id and ground
    // the prompt; a broken baseline is the caller's error.
    let defs = match parse_bpmn(&req.baseline_model) {
        Ok(d) if !d.is_empty() => d,
        _ => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({ "error": "baseline model failed to parse" })),
            )
                .into_response();
        }
    };
    let process_id = req.process_id.clone().unwrap_or_else(|| defs[0].id.clone());

    // Resolve the system prompt: inline override, else library prompt, else default.
    let system_prompt = match (&req.prompt, &req.prompt_id) {
        (Some(inline), _) => inline.clone(),
        (None, Some(id)) => match state.prompts.read().unwrap().system_of(id) {
            Some(s) => s,
            None => {
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(serde_json::json!({ "error": format!("unknown prompt id: {id}") })),
                )
                    .into_response();
            }
        },
        (None, None) => DEFAULT_EVOLVE_SYSTEM_PROMPT.to_string(),
    };

    // Fetch + distil the recorded dataset (this is the fitness data).
    let client = match &req.base_url {
        Some(url) if !url.is_empty() => NanoClient::new(url),
        _ => state.nano.clone(),
    };
    let limit = req.limit.unwrap_or(200);
    let summaries = match client.list_traces(limit).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };
    let (dataset, matched, skipped, skip_reasons) =
        distil_recorded_dataset(&client, &process_id, &summaries).await;
    if dataset.is_empty() {
        return (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({
                "error": "no replayable recorded instances for this process \
                          (was the gateway run with --capture?)",
                "processId": process_id,
                "matchedInstances": matched,
                "skipped": skipped,
                "skipReasons": skip_reasons,
            })),
        )
            .into_response();
    }

    // Distil the production signal and ask the model to propose redesigns.
    let signal = summarize_dataset(&process_id, &dataset);
    let user_prompt = build_evolve_prompt(&signal, &req.baseline_model);
    let raw = match llm_complete(&cfg, &system_prompt, &user_prompt).await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": e, "signal": signal })),
            )
                .into_response();
        }
    };
    let candidates: Vec<CandidateModel> = match parse_structural_candidates(&raw) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({ "error": e, "signal": signal, "llmRaw": raw })),
            )
                .into_response();
        }
    };

    // Prove each proposal against the recorded history.
    let ranking = rank_candidates_by_replay(&candidates, &dataset, Some(&process_id));
    Json(serde_json::json!({
        "processId": process_id,
        "matchedInstances": matched,
        "skipped": skipped,
        "skipReasons": skip_reasons,
        "signal": signal,
        "proposed": candidates.len(),
        "ranking": ranking,
    }))
    .into_response()
}
/// `GET /api/prompts` — list every prompt in the library (built-in + authored).
async fn prompts_list(State(state): State<AppState>) -> impl IntoResponse {
    let list = state.prompts.read().unwrap().list();
    Json(list)
}

/// `GET /api/prompts/{id}` — one prompt, or 404.
async fn prompts_get(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match state.prompts.read().unwrap().get(&id) {
        Some(p) => Json(p).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("no such prompt: {id}") })),
        )
            .into_response(),
    }
}

/// `POST /api/prompts` — author or import a prompt (create/update by id). Returns the
/// stored prompt. 422 on a validation failure (empty id or system text).
async fn prompts_upsert(
    State(state): State<AppState>,
    Json(prompt): Json<Prompt>,
) -> impl IntoResponse {
    match state.prompts.write().unwrap().upsert(prompt) {
        Ok(stored) => (StatusCode::OK, Json(stored)).into_response(),
        Err(e) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

/// `DELETE /api/prompts/{id}` — remove an authored prompt. 422 when it is built-in or
/// unknown (the message distinguishes the two).
async fn prompts_delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.prompts.write().unwrap().remove(&id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

/// Query for the production live-source baseline path.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProductionQuery {
    /// Which process to baseline. When absent, the busiest sampled process is used.
    process_id: Option<String>,
    /// How many recent trace summaries to scan.
    limit: Option<usize>,
    /// How many of those to pull full detail for (per-element aggregation).
    sample: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusterQuery {
    /// Which process to summarize. When absent, the busiest sampled process is used.
    process_id: Option<String>,
    /// How many recent trace summaries to scan (per node).
    limit: Option<usize>,
    /// How many of those to pull full detail for (the queue/service split).
    sample: Option<usize>,
    /// When set, attach the regime-(b) queueing model's per-job-type staffing
    /// recommendation that holds this p99 queue-wait (ms).
    target_p99_ms: Option<u64>,
}

/// `GET /api/harness/production` — the **generation-skipped** production path:
/// fold the live cluster's traces (T1 contract) into the baseline a candidate
/// search must beat for one process. On a Nano read failure we return 502; on an
/// empty/unknown process we return 422.
async fn harness_production(
    State(state): State<AppState>,
    Query(q): Query<ProductionQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    let sample = q.sample.unwrap_or(50).clamp(1, 500);
    match build_baseline(&state.nano, q.process_id.as_deref(), limit, sample).await {
        Ok(baseline) => Json(baseline).into_response(),
        // A read-contract failure surfaces the underlying GET error; an empty or
        // unknown-process result is a request the caller can fix, so 422.
        Err(e) if e.starts_with("GET ") || e.starts_with("decode ") => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

/// `GET /api/harness/cluster` — the **at-scale measurement** half of the M3
/// ClusterRunner: summarize a live run (throughput, e2e tail p50/p95/p99, the
/// queue/service split under load, per-job-type breakdown, and per-node backlog
/// sensing) for one process, unioned across every cluster node. When
/// `targetP99Ms` is supplied, it also attaches `staffing`: the regime-(b) queueing
/// model's per-job-type worker-count recommendation to hold that p99 queue-wait.
/// Point it at a cluster the perf-matrix is driving. 502 on a Nano read failure, 422
/// on an empty/unknown process.
async fn harness_cluster(
    State(state): State<AppState>,
    Query(q): Query<ClusterQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(500).clamp(1, 1000);
    let sample = q.sample.unwrap_or(50).clamp(1, 500);
    match build_cluster_summary(&state.nano, q.process_id.as_deref(), limit, sample).await {
        Ok(summary) => match q.target_p99_ms {
            // Attach the staffing recommendation derived from the measured run.
            Some(target) => {
                let staffing = staff_for_summary(&summary, target);
                let mut body = serde_json::to_value(&summary).unwrap_or_default();
                if let Some(obj) = body.as_object_mut() {
                    obj.insert(
                        "staffing".to_string(),
                        serde_json::to_value(staffing).unwrap_or_default(),
                    );
                }
                Json(body).into_response()
            }
            None => Json(summary).into_response(),
        },
        Err(e) if e.starts_with("GET ") || e.starts_with("decode ") => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

/// Run a scenario off the async runtime (the SimRunner is synchronous CPU work).
async fn run_scenario_blocking(scenario: Scenario) -> axum::response::Response {
    let result = tokio::task::spawn_blocking(move || run_scenario(&scenario)).await;
    match result {
        Ok(Ok(report)) => Json(report).into_response(),
        Ok(Err(e)) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("harness task failed: {e}") })),
        )
            .into_response(),
    }
}

/// `GET /harness` — a tiny dashboard that runs the example and renders the ranking.
async fn harness_dashboard() -> Html<&'static str> {
    Html(HARNESS_HTML)
}

/// The cockpit single-file app (design §7.8 / §10), served at `/cockpit`.
const COCKPIT_HTML: &str = include_str!("cockpit.html");

const DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>ProcessOS — Insights</title>
<style>
  :root { color-scheme: dark; }
  body { margin: 0; font: 14px/1.5 system-ui, sans-serif; background: #0a0a0b; color: #e4e4e7; }
  header { padding: 16px 24px; border-bottom: 1px solid #27272a; display: flex; align-items: baseline; gap: 12px; }
  h1 { font-size: 18px; margin: 0; }
  .sub { color: #71717a; font-size: 12px; }
  main { padding: 24px; max-width: 1100px; }
  .cards { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 24px; }
  .card { background: #18181b; border: 1px solid #27272a; border-radius: 8px; padding: 12px 16px; min-width: 120px; }
  .card .n { font-size: 22px; font-weight: 600; }
  .card .l { color: #a1a1aa; font-size: 12px; }
  h2 { font-size: 14px; color: #a1a1aa; margin: 24px 0 8px; text-transform: uppercase; letter-spacing: .04em; }
  table { width: 100%; border-collapse: collapse; margin-bottom: 16px; }
  th, td { text-align: left; padding: 6px 10px; border-bottom: 1px solid #27272a; }
  th { color: #a1a1aa; font-weight: 500; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  .pid { font-family: ui-monospace, monospace; color: #a5b4fc; }
  .bottleneck { color: #fca5a5; }
  .err { color: #fca5a5; background: #2a0a0a; padding: 12px 16px; border-radius: 8px; }
  button { background: #27272a; color: #e4e4e7; border: 1px solid #3f3f46; border-radius: 6px; padding: 4px 10px; cursor: pointer; }
</style>
</head>
<body>
<header>
  <h1>ProcessOS</h1>
  <span class="sub">Insights (T1) · <span id="nano"></span></span>
  <nav style="margin-left:auto;display:flex;align-items:center;gap:14px">
    <a href="/" style="color:#a1a1aa;text-decoration:none;font-size:13px">Home</a>
    <a href="/features" style="color:#a1a1aa;text-decoration:none;font-size:13px">Features</a>
    <a href="/harness" style="color:#a1a1aa;text-decoration:none;font-size:13px">Harness</a>
    <button onclick="load()">Refresh</button>
  </nav>
</header>
<main id="out">Loading…</main>
<script>
function ms(v){ return v==null ? '—' : (v>=1000 ? (v/1000).toFixed(2)+'s' : v+'ms'); }
function esc(s){ return String(s).replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
async function load(){
  const out = document.getElementById('out');
  out.textContent = 'Loading…';
  let r;
  try { r = await fetch('/api/insights'); } catch(e){ out.innerHTML = '<div class="err">Cannot reach ProcessOS API: '+esc(e)+'</div>'; return; }
  const d = await r.json();
  if(!r.ok){ out.innerHTML = '<div class="err">Nano read failed: '+esc(d.error||r.status)+'</div>'; return; }
  document.getElementById('nano').textContent = d.nanoBaseUrl;
  const t = d.totals, live = d.live || {};
  let h = '<div class="cards">'
    + card(t.instances, 'instances') + card(t.completed, 'completed')
    + card(t.active, 'active') + card(t.incidents, 'incidents')
    + card(d.sampledInstances, 'sampled')
    + (live.activeInstances!=null ? card(live.activeInstances, 'live active') : '')
    + '</div>';

  h += '<h2>Processes</h2>';
  if(!d.processes.length){ h += '<p class="sub">No sampled traces yet. Start some instances on the cluster.</p>'; }
  for(const p of d.processes){
    h += '<h3 class="pid">'+esc(p.processId)+'</h3>';
    h += '<div class="sub">'+p.instances+' sampled · '+p.completed+' completed · '
       + p.incidentInstances+' with incidents · avg '+ms(p.avgDurationMs)+' · p95 '+ms(p.p95DurationMs)
       + (p.bottleneck ? ' · bottleneck <span class="bottleneck">'+esc(p.bottleneck.elementId)+'</span> '+ms(p.bottleneck.avgDurationMs) : '')
       + '</div>';
    h += '<table><thead><tr><th>Element</th><th class="num">count</th><th class="num">avg</th>'
       + '<th class="num">queue</th><th class="num">service</th><th class="num">incidents</th><th class="num">job fails</th></tr></thead><tbody>';
    for(const e of p.elements){
      const bn = p.bottleneck && e.elementId===p.bottleneck.elementId;
      h += '<tr'+(bn?' class="bottleneck"':'')+'><td>'+esc(e.elementId)+'</td>'
         + '<td class="num">'+e.count+'</td><td class="num">'+ms(e.avgDurationMs)+'</td>'
         + '<td class="num">'+ms(e.avgQueueMs)+'</td><td class="num">'+ms(e.avgServiceMs)+'</td>'
         + '<td class="num">'+e.incidents+'</td><td class="num">'+e.jobFailures+'</td></tr>';
    }
    h += '</tbody></table>';
  }

  if(d.incidentClusters.length){
    h += '<h2>Incident clusters</h2><table><thead><tr><th>Element</th><th>Kind</th><th class="num">count</th><th>Top reason</th></tr></thead><tbody>';
    for(const c of d.incidentClusters){
      h += '<tr><td>'+esc(c.elementId)+'</td><td>'+esc(c.kind)+'</td><td class="num">'+c.count+'</td><td>'+esc(c.topReason)+'</td></tr>';
    }
    h += '</tbody></table>';
  }
  out.innerHTML = h;
}
function card(n,l){ return '<div class="card"><div class="n">'+n+'</div><div class="l">'+l+'</div></div>'; }
load();
</script>
</body>
</html>
"#;

/// The harness dashboard: runs the bundled example and renders the ranked
/// candidates. Dependency-free; the richer UX is the console "Optimization" tab.
const HARNESS_HTML: &str = r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>ProcessOS — Optimization Harness</title>
<style>
  :root { color-scheme: dark; }
  body { margin: 0; font: 14px/1.5 system-ui, sans-serif; background: #0a0a0b; color: #e4e4e7; }
  header { padding: 20px 24px; border-bottom: 1px solid #27272a; }
  h1 { margin: 0; font-size: 18px; }
  .sub { color: #a1a1aa; margin-top: 4px; }
  main { padding: 24px; max-width: 1000px; }
  .card { background: #18181b; border: 1px solid #27272a; border-radius: 10px; padding: 16px 18px; margin-bottom: 18px; }
  .notes { color: #a1a1aa; font-size: 13px; }
  .notes li { margin: 2px 0; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid #27272a; }
  th { color: #a1a1aa; font-weight: 600; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  .ok { color: #4ade80; }
  .bad { color: #f87171; }
  .best { background: #14532d33; }
  .pill { display: inline-block; padding: 1px 8px; border-radius: 999px; font-size: 12px; border: 1px solid #27272a; }
  .pill.win { color: #4ade80; border-color: #14532d; }
  code { color: #fbbf24; }
</style>
</head>
<body>
<header>
  <h1>ProcessOS — Optimization Harness</h1>
  <div class="sub">SimRunner over the bundled example scenario (worker-swap transform space). The same loop runs in production against live Nano traces.</div>
  <div class="sub" style="margin-top:8px"><a href="/" style="color:#a5b4fc;text-decoration:none">Home</a> · <a href="/features" style="color:#a5b4fc;text-decoration:none">Features</a> · <a href="/console" style="color:#a5b4fc;text-decoration:none">Console</a></div>
</header>
<main id="root">Running the example scenario…</main>
<script>
function pct(x) { return (x * 100).toFixed(0) + "%"; }
function num(x, d) { return Number(x).toFixed(d === undefined ? 2 : d); }
async function load() {
  const root = document.getElementById("root");
  try {
    const res = await fetch("/api/harness/example/run");
    if (!res.ok) { root.textContent = "Harness error: " + res.status; return; }
    const r = await res.json();
    const golden = r.golden ? r.golden.name : "—";
    const rows = r.variants.map((v, i) => {
      const cls = i === 0 ? "best" : "";
      const feas = v.feasible ? '<span class="ok">yes</span>' : '<span class="bad">no</span>';
      return `<tr class="${cls}">
        <td>${i + 1}</td>
        <td>${v.name}${i === 0 ? ' <span class="pill win">best</span>' : ''}</td>
        <td>${v.source}</td>
        <td class="num">${num(v.avgCost)}</td>
        <td class="num">${num(v.avgLatencyMs, 0)}</td>
        <td class="num">${pct(v.correctnessRate)}</td>
        <td class="num">${pct(v.incidentRate)}</td>
        <td class="num">${pct(v.completionRate)}</td>
        <td class="num">${feas}</td>
      </tr>`;
    }).join("");
    root.innerHTML = `
      <div class="card">
        <div><strong>${r.scenario}</strong> &middot; process <code>${r.processId}</code> &middot; ${r.inputs} inputs &middot; minimize <code>${r.objective.minimize}</code></div>
        <div class="sub" style="margin-top:6px">
          Golden: <code>${golden}</code> &middot;
          recovered: ${r.recoveredGolden ? '<span class="ok">yes</span>' : '<span class="bad">no</span>'}
          ${r.goldenDistance != null ? ` (distance ${r.goldenDistance})` : ''}
          &middot; best: <code>${r.best}</code>
        </div>
      </div>
      <div class="card">
        <table>
          <thead><tr>
            <th>#</th><th>candidate</th><th>source</th><th class="num">avg cost</th><th class="num">avg latency (ms)</th>
            <th class="num">correct</th><th class="num">incidents</th><th class="num">completed</th><th class="num">feasible</th>
          </tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
      <div class="card notes">
        <strong>Notes</strong>
        <ul>${r.notes.map(n => `<li>${n}</li>`).join("")}</ul>
      </div>`;
  } catch (e) {
    root.textContent = "Harness error: " + e;
  }
}
load();
</script>
</body>
</html>
"##;
