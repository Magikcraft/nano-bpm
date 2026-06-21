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
mod harness;
mod report;

use std::net::SocketAddr;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;

use crate::contracts::NanoClient;
use crate::harness::{
    apply_calibration, build_baseline, build_cluster_summary, calibrate_from_measured,
    example_scenario, run_hypothesis, run_scenario, staff_for_summary, LlmConfig, LlmOverride,
    MeasuredJobType, Scenario,
};

#[derive(Clone)]
struct AppState {
    nano: NanoClient,
}

/// Server configuration, all overridable by environment.
struct Config {
    port: u16,
    nano_base_url: String,
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
        Self {
            port,
            nano_base_url,
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let cfg = Config::from_env();
    let state = AppState {
        nano: NanoClient::new(&cfg.nano_base_url),
    };

    let app = Router::new()
        .route("/", get(landing))
        .route("/features", get(features))
        .route("/console", get(dashboard))
        .route("/health", get(health))
        .route("/api/insights", get(insights))
        .route("/harness", get(harness_dashboard))
        .route("/api/harness/example", get(harness_example))
        .route("/api/harness/example/run", get(harness_example_run))
        .route("/api/harness/run", post(harness_run))
        .route("/api/harness/calibrate", post(harness_calibrate))
        .route("/api/harness/hypothesize", post(harness_hypothesize))
        .route("/api/harness/production", get(harness_production))
        .route("/api/harness/cluster", get(harness_cluster))
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
}

/// `POST /api/harness/hypothesize` — ask the configured LLM to propose candidates,
/// evaluate + rank them with the SimRunner, and return the report. The model is
/// reached via the pluggable client (local llama.cpp / Ollama / vLLM, or Anthropic).
async fn harness_hypothesize(Json(req): Json<HypothesizeRequest>) -> impl IntoResponse {
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
    // Ground the baseline in production when measured distributions are supplied,
    // so the LLM reasons over — and every candidate is scored against — the service
    // times and failure rates the cluster actually observed.
    let scenario = if req.measured.is_empty() {
        req.scenario.clone()
    } else {
        let calibration = calibrate_from_measured(&req.scenario, &req.measured);
        apply_calibration(&req.scenario, &calibration)
    };
    match run_hypothesis(&scenario, &cfg, req.include_baked).await {
        Ok(report) => Json(report).into_response(),
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
