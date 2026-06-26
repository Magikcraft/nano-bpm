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

mod agent;
mod analysis;
mod bpmn_model;
mod camunda_import;
mod chat;
mod chat_prompts;
mod cockpit;
mod conformance;
mod contracts;
mod conversation;
mod corpus;
mod dataset;
mod experiment;
mod harness;
mod investigate;
mod llama;
mod monitor;
mod personas;
mod pilot;
mod pyrunner;
mod reasoning;
mod report;
mod settings;
mod supervisor;
mod workspace;

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse,
    },
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;

use crate::contracts::NanoClient;
use crate::harness::{
    apply_calibration, build_baseline, build_cluster_summary, build_evolve_prompt,
    calibrate_from_measured, example_scenario, list_models, llm_complete,
    parse_structural_candidates, rank_candidates_by_replay, replay_dataset, replay_instance,
    run_hypothesis, run_scenario, staff_for_summary, summarize_dataset, CandidateModel, LlmConfig,
    LlmOverride, MeasuredJobType, Prompt, PromptLibrary, RecordedInstance, Scenario,
    DEFAULT_EVOLVE_SYSTEM_PROMPT, DEFAULT_PROMPT_ID, DEFAULT_SYSTEM_PROMPT,
};
use nanobpmn_engine_core::bpmn::parse_bpmn;

/// Per-session steering queues: an operator instruction stack drained into the running turn.
type SteerQueues =
    std::sync::Mutex<std::collections::HashMap<String, Arc<std::sync::Mutex<Vec<String>>>>>;

/// Per-session live completion ids (`chatcmpl-…`): the in-flight turn's id, used to target it
/// with the reasoning-control endpoint (end its thinking mid-generation).
type CompletionIds =
    std::sync::Mutex<std::collections::HashMap<String, Arc<std::sync::Mutex<Option<String>>>>>;

#[derive(Clone)]
struct AppState {
    /// The client's production engine — the read-only analysis TARGET. Traces,
    /// metrics, and the baseline process definition are read from here; ProcessOS
    /// never runs its own meta-workloads on the client's engine.
    target: NanoClient,
    /// ProcessOS's OWN engine — where the `pilotSelfOptimize` loop runs and its
    /// workers are deployed. The cockpit creates/reads experiments and completes
    /// the human Review task here.
    own: NanoClient,
    /// The prompt library — import / select / author the system prompts that drive
    /// hypothesis generation. Shared, interior-mutable so authoring is live.
    prompts: Arc<RwLock<PromptLibrary>>,
    /// Durable cockpit conversations (§10) — the persisted pilot ↔ droid dialogue,
    /// one append-only log per experiment.
    conversations: Arc<conversation::ConversationStore>,
    /// The pilot process (§10 plastic surface **a**) — the forkable BPMN choreography
    /// that drives the optimization loop. File-backed; the supervisor deploys it on
    /// boot, and `PUT /api/pilot` re-forks it and hot-redeploys to the own engine.
    pilot: Arc<pilot::PilotStore>,
    /// The consultant's workspace tree — workspaces > processes, each bound to a live
    /// customer Nano or a loaded trace dataset. Discovered by scanning the root.
    workspaces: workspace::WorkspaceCatalog,
    /// Operator-editable settings (LLM connection + Python interpreter), persisted to the
    /// user's config dir and layered over the environment at request time.
    settings: settings::SettingsStore,
    /// Persisted interactive cockpit chat sessions — the full droid transcript per
    /// `(workspace, process)`, so a conversation resumes with memory across turns and
    /// restarts.
    chat: Arc<chat::ChatStore>,
    /// The operator's chat prompt library (reusable compose-box message templates),
    /// persisted to the user's config dir alongside `settings.json`.
    chat_prompts: Arc<chat_prompts::ChatPromptStore>,
    /// The operator's persona library (selectable standing system prompts for chat),
    /// persisted to the user's config dir alongside `settings.json`.
    personas: Arc<personas::PersonaStore>,
    /// Wrap-up flags for in-flight chat turns, keyed by session. The cockpit's "wrap it up"
    /// control sets the flag; the agent loop checks it between rounds and reports early.
    chat_cancels: Arc<
        std::sync::Mutex<std::collections::HashMap<String, Arc<std::sync::atomic::AtomicBool>>>,
    >,
    /// Steering queues for in-flight chat turns, keyed by session. The cockpit's "Steer …"
    /// control appends an operator instruction; the agent loop drains it at the next round
    /// boundary and injects it as a user turn, redirecting an investigation without restarting it.
    chat_steers: Arc<SteerQueues>,
    /// Live OpenAI completion ids (`chatcmpl-…`) for in-flight chat turns, keyed by session. The
    /// streaming agent sets the inner cell as soon as the first chunk carries the id; the wrap-up
    /// handler and the loop monitor read it to target the live turn with the reasoning-control
    /// endpoint (end its thinking mid-generation). Cleared once the turn ends.
    chat_cmpl_ids: Arc<CompletionIds>,
    /// The exact model request bodies sent during each session's most recent turn, keyed by
    /// session (one entry per round), powering the cockpit's per-chat "Debug" tab. In-memory
    /// (not persisted) — it shows what was last sent and is cleared on restart.
    chat_debug: Arc<std::sync::Mutex<std::collections::HashMap<String, Vec<serde_json::Value>>>>,
    /// Live event buffers for in-flight chat turns, keyed by session. The streaming turn appends
    /// every SSE event here as it produces it; the original request *and* any later reattach
    /// (`GET .../chat/stream/live`, used after a page reload) replay the buffer then follow along.
    /// This decouples the running investigation from the fetch that started it, so reloading the
    /// page no longer loses the live view. Removed once the turn ends.
    chat_live: Arc<std::sync::Mutex<std::collections::HashMap<String, LiveTurn>>>,
    /// The supervised local llama.cpp `llama-server` sidecar (start/stop/status/logs). Optional at
    /// runtime — nothing runs until the operator presses Start for a `sidecar:true` profile.
    llama: llama::LlamaManager,
}

/// An append-only buffer for one in-flight chat turn's SSE events, shared between the producing
/// (blocking) agent task and any number of SSE consumers. Consumers each keep their own cursor,
/// replay `events` from 0, then wait on `notify` for more until `done` is set.
#[derive(Clone)]
struct LiveTurn {
    /// The operator message that started this turn — used to render the user bubble on reattach,
    /// since the persisted transcript doesn't yet include this in-flight turn.
    user: String,
    /// Every SSE event emitted so far this turn (including the terminal `done`/`error`).
    events: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    /// Woken whenever a new event is appended or the turn ends.
    notify: Arc<tokio::sync::Notify>,
    /// Set once a terminal (`done`/`error`) event has been appended.
    done: Arc<std::sync::atomic::AtomicBool>,
}

impl LiveTurn {
    fn new(user: String) -> Self {
        LiveTurn {
            user,
            events: Arc::new(std::sync::Mutex::new(Vec::new())),
            notify: Arc::new(tokio::sync::Notify::new()),
            done: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Append one event and wake all consumers. Terminal events also flip `done`.
    fn emit(&self, v: serde_json::Value) {
        let terminal = matches!(
            v.get("type").and_then(|t| t.as_str()),
            Some("done") | Some("error")
        );
        if let Ok(mut e) = self.events.lock() {
            e.push(v);
        }
        if terminal {
            self.done.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        self.notify.notify_waiters();
    }
}

/// Build an SSE response that replays a `LiveTurn`'s buffered events from the beginning, then
/// follows along live until the turn ends. Multiple consumers (the original request and any
/// number of reattachers) can each call this independently; each keeps its own cursor.
fn sse_from_live(
    live: LiveTurn,
) -> Sse<impl futures_util::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let stream = futures_util::stream::unfold(0usize, move |cursor| {
        let live = live.clone();
        async move {
            loop {
                // Arm the wakeup *before* inspecting the buffer so an event appended between our
                // read and our await can't be missed (lost-wakeup-safe).
                let notified = live.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let item = live.events.lock().ok().and_then(|e| e.get(cursor).cloned());
                if let Some(v) = item {
                    return Some((
                        Ok::<Event, std::convert::Infallible>(Event::default().data(v.to_string())),
                        cursor + 1,
                    ));
                }
                if live.done.load(std::sync::atomic::Ordering::Relaxed) {
                    return None;
                }
                notified.await;
            }
        }
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Server configuration, all overridable by environment.
struct Config {
    port: u16,
    /// The client's production engine ProcessOS analyses (read-only). `NANO_TARGET_URL`,
    /// falling back to `NANO_BASE_URL`, then `http://localhost:8080`.
    target_url: String,
    /// ProcessOS's own engine, where the pilot loop + workers run. `PROCESSOS_NANO_URL`,
    /// falling back to `NANO_BASE_URL`, then `http://localhost:8080`. In a single-Nano
    /// dev setup this equals `target_url`, keeping the historical behaviour.
    own_url: String,
    /// Optional directory to import prompts from on startup (`PROCESSOS_PROMPTS_DIR`).
    prompts_dir: Option<String>,
    /// Directory for durable state — currently cockpit conversations
    /// (`PROCESSOS_DATA_DIR`, default `./.processos-data`).
    data_dir: std::path::PathBuf,
}

impl Config {
    fn from_env() -> Self {
        let port = std::env::var("PROCESSOS_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8090);
        let env = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
        let (target_url, own_url) = resolve_nano_urls(
            env("NANO_BASE_URL"),
            env("NANO_TARGET_URL"),
            env("PROCESSOS_NANO_URL"),
        );
        let prompts_dir = std::env::var("PROCESSOS_PROMPTS_DIR")
            .ok()
            .filter(|s| !s.is_empty());
        Self {
            port,
            target_url,
            own_url,
            prompts_dir,
            data_dir: conversation::data_dir_from_env(),
        }
    }
}

/// Resolve the (target, own) Nano URLs from the three env sources. `NANO_BASE_URL`
/// is the back-compat alias that defaults BOTH roles; `NANO_TARGET_URL` and
/// `PROCESSOS_NANO_URL` override each role independently. With none set, both fall
/// back to the local gateway. Pure so the precedence is unit-testable.
fn resolve_nano_urls(
    base: Option<String>,
    target: Option<String>,
    own: Option<String>,
) -> (String, String) {
    let default = base.unwrap_or_else(|| "http://localhost:8080".to_string());
    let target_url = target.unwrap_or_else(|| default.clone());
    let own_url = own.unwrap_or(default);
    (target_url, own_url)
}

#[tokio::main]
async fn main() {
    // CLI subcommands run a one-shot task and exit before the server boots. This keeps
    // the default (no-args) behaviour — start the optimization-plane server — intact.
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        match args[1].as_str() {
            "gen" | "generate" => {
                run_cli_generate(&args[2..]);
                return;
            }
            "infer" => {
                run_cli_infer(&args[2..]);
                return;
            }
            "import-camunda" => {
                run_cli_import_camunda(&args[2..]);
                return;
            }
            other => {
                eprintln!("unknown subcommand '{other}'. usage:");
                eprintln!("  processos gen <pack.json> <out-dir>");
                eprintln!("  processos infer <dataset-dir> [target-p99-wait-ms]");
                eprintln!("  processos import-camunda <records.json|dir> <out-dir> [--no-tier2]");
                std::process::exit(2);
            }
        }
    }

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

    // The pilot process (§10 surface a) — file-backed under the data dir, seeded with
    // the built-in default on first boot. The supervisor deploys whatever it resolves.
    let pilot = Arc::new(pilot::PilotStore::open(&cfg.data_dir));

    // If configured, spawn ProcessOS's OWN Nano engine (the client's production
    // engine is the read-only target; ProcessOS runs its pilot loop here). The
    // spawned engine's URL overrides `own_url`. Held to shutdown so we don't orphan
    // a gateway across restarts.
    let mut own_engine: Option<supervisor::OwnNano> = None;
    let own_url = match supervisor::SpawnConfig::from_env() {
        Some(spawn_cfg) => {
            tracing::info!(
                bin = %spawn_cfg.bin.display(),
                data_dir = %spawn_cfg.data_dir.display(),
                "starting ProcessOS's own Nano engine"
            );
            match supervisor::OwnNano::spawn(&spawn_cfg, &pilot.current_xml()).await {
                Ok(engine) => {
                    let url = engine.base_url.clone();
                    let src = pilot.doc().source;
                    tracing::info!(own = %url, pilot = %src, "own Nano engine ready; pilot process deployed");
                    own_engine = Some(engine);
                    url
                }
                Err(e) => panic!("failed to start own Nano engine: {e}"),
            }
        }
        None => cfg.own_url.clone(),
    };

    let state = AppState {
        target: NanoClient::new(&cfg.target_url),
        own: NanoClient::new(&own_url),
        prompts: Arc::new(RwLock::new(library)),
        conversations: Arc::new(conversation::ConversationStore::open(&cfg.data_dir)),
        pilot,
        workspaces: workspace::WorkspaceCatalog::open(workspace::root_from_env(&cfg.data_dir)),
        settings: settings::SettingsStore::open(),
        chat: Arc::new(chat::ChatStore::open(cfg.data_dir.join("chat"))),
        chat_prompts: Arc::new(chat_prompts::ChatPromptStore::open(
            settings::config_dir().join("chat-prompts.json"),
        )),
        personas: Arc::new(personas::PersonaStore::open(
            settings::config_dir().join("personas.json"),
        )),
        chat_cancels: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        chat_steers: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        chat_cmpl_ids: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        chat_debug: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        chat_live: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        llama: llama::LlamaManager::new(),
    };

    // A handle to stop the llama sidecar on shutdown (the router takes ownership of `state`).
    let llama_for_shutdown = state.llama.clone();

    let app = Router::new()
        .route("/", get(landing))
        .route("/features", get(features))
        .route("/console", get(dashboard))
        .route("/cockpit", get(cockpit_page))
        .route("/health", get(health))
        .route("/api/insights", get(insights))
        .route("/api/cockpit/overview", get(cockpit_overview))
        .route(
            "/api/cockpit/experiments",
            get(cockpit_experiments).post(cockpit_create),
        )
        .route("/api/cockpit/experiments/{key}", get(cockpit_experiment))
        .route(
            "/api/cockpit/experiments/{key}/decision",
            post(cockpit_decision),
        )
        .route(
            "/api/cockpit/experiments/{key}/conversation",
            get(cockpit_conversation).post(cockpit_message),
        )
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
        .route("/api/pilot", get(pilot_get).put(pilot_put))
        .route("/api/pilot/reset", post(pilot_reset))
        .route("/workspace", get(workspace_page))
        .route(
            "/api/workspaces",
            get(ws_workspaces).post(ws_create_workspace),
        )
        .route("/api/workspaces/{workspace}", get(ws_workspace))
        .route("/api/workspaces/seed-demo", post(ws_seed_demo))
        .route(
            "/api/workspaces/{workspace}/processes",
            post(ws_create_process),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}",
            get(ws_process).put(ws_update_process),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/model",
            get(ws_process_model).put(ws_set_process_model),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/insights",
            get(ws_process_insights),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/investigate",
            post(ws_process_investigate),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat",
            get(cockpit_chat_load).post(cockpit_chat_send),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions",
            get(cockpit_chat_sessions_list).post(cockpit_chat_sessions_create),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}",
            get(cockpit_chat_session_load).delete(cockpit_chat_session_delete),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/rename",
            post(cockpit_chat_session_rename),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/debug",
            get(cockpit_chat_session_debug),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/simulations",
            get(cockpit_chat_session_simulations),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/stream",
            post(cockpit_chat_stream),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/stream/live",
            get(cockpit_chat_stream_live),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/reset",
            post(cockpit_chat_reset),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/wrapup",
            post(cockpit_chat_wrapup),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/steer",
            post(cockpit_chat_steer),
        )
        .route("/api/python/status", get(python_status))
        .route(
            "/api/chat-prompts",
            get(chat_prompts_list).post(chat_prompts_upsert),
        )
        .route(
            "/api/chat-prompts/{id}",
            axum::routing::delete(chat_prompts_delete),
        )
        .route("/api/personas", get(personas_list).post(personas_upsert))
        .route("/api/personas/{id}", axum::routing::delete(personas_delete))
        .route("/assets/bpmn/{file}", get(bpmn_asset))
        .route("/assets/settings.js", get(settings_js))
        .route("/api/settings", get(get_settings).put(put_settings))
        .route("/api/settings/profiles", post(post_profile))
        .route(
            "/api/settings/profiles/{id}",
            axum::routing::put(put_profile).delete(delete_profile),
        )
        .route("/api/settings/models", post(post_models))
        .route("/api/llama/status", get(llama_status))
        .route("/api/llama/ready", get(llama_ready))
        .route("/api/llama/start", post(llama_start))
        .route("/api/llama/stop", post(llama_stop))
        .route("/api/llama/logs", get(llama_logs))
        .route("/api/llama/reasoning-control", get(reasoning_control_status))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("ProcessOS could not bind {addr}: {e}"));

    tracing::info!(
        %addr,
        target = %cfg.target_url,
        own = %own_url,
        data_dir = %cfg.data_dir.display(),
        "ProcessOS listening; reading the client's production engine (target) over the public trace/metrics contract; running the pilot loop on its own engine"
    );
    println!("PROCESSOS_PORT={}", cfg.port);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("ProcessOS server error");

    // Tear down the spawned engine (if any) so it doesn't outlive ProcessOS.
    if let Some(engine) = own_engine {
        engine.shutdown().await;
    }
    // Stop all local model sidecars (if any) so llama-server doesn't linger.
    llama_for_shutdown.stop(None).await;
}

/// `processos gen <pack.json> <out-dir>` — generate a synthetic trace corpus.
fn run_cli_generate(args: &[String]) {
    if args.len() < 2 {
        eprintln!("usage: processos gen <pack.json> <out-dir>");
        std::process::exit(2);
    }
    let pack_path = std::path::Path::new(&args[0]);
    let out_dir = std::path::Path::new(&args[1]);
    let (pack, def) = corpus::load_pack(pack_path).unwrap_or_else(|e| {
        eprintln!("load pack failed: {e}");
        std::process::exit(1);
    });
    let summary = corpus::generate(&pack, &def, out_dir).unwrap_or_else(|e| {
        eprintln!("generate failed: {e}");
        std::process::exit(1);
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&summary).unwrap_or_default()
    );
}

/// `processos infer <dataset-dir> [target-p99-wait-ms]` — run the inference + score.
fn run_cli_infer(args: &[String]) {
    if args.is_empty() {
        eprintln!("usage: processos infer <dataset-dir> [target-p99-wait-ms]");
        std::process::exit(2);
    }
    let dataset_dir = std::path::Path::new(&args[0]);
    let target = args
        .get(1)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(500);
    let inference = corpus::infer(dataset_dir, target).unwrap_or_else(|e| {
        eprintln!("infer failed: {e}");
        std::process::exit(1);
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&inference).unwrap_or_default()
    );

    // Score against expected.json if it sits beside the dataset.
    let expected = dataset_dir.join("expected.json");
    if expected.is_file() {
        match corpus::score(&inference, &expected) {
            Ok(s) => println!("SCORE {}", serde_json::to_string(&s).unwrap_or_default()),
            Err(e) => eprintln!("score failed: {e}"),
        }
    }
}

/// `processos import-camunda <records.json|dir> <out-dir> [--no-tier2]` — fold a
/// Camunda 8 record export (Elasticsearch/Opensearch/debug-log JSON) into a
/// `traces.json` dataset directly loadable by `DatasetSource`.
fn run_cli_import_camunda(args: &[String]) {
    let positional: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();
    if positional.len() < 2 {
        eprintln!("usage: processos import-camunda <records.json|dir> <out-dir> [--no-tier2]");
        std::process::exit(2);
    }
    let tier2 = !args.iter().any(|a| a == "--no-tier2");
    let input = std::path::Path::new(positional[0].as_str());
    let out_dir = std::path::Path::new(positional[1].as_str());
    let summary = camunda_import::import(input, out_dir, tier2).unwrap_or_else(|e| {
        eprintln!("import-camunda failed: {e}");
        std::process::exit(1);
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&summary).unwrap_or_default()
    );
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
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
    match report::build(&state.target, limit, sample).await {
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
    match cockpit::overview(&state.target, &state.own, limit, sample).await {
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
    match cockpit::list_experiments(&state.own, limit).await {
        Ok(xs) => Json(xs).into_response(),
        Err(e) => bad_gateway(e),
    }
}

/// `GET /api/cockpit/experiments/{key}` — the full cockpit view of one experiment.
async fn cockpit_experiment(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    match cockpit::experiment_detail(&state.own, &key).await {
        Ok(mut d) => {
            // Prefer the durable dialogue: once an experiment has a persisted log
            // (engine framing at creation, a droid turn per round, the pilot's
            // notes/decisions), that log is the true running history. The
            // reconstructed snapshot remains the fallback for legacy experiments
            // created before conversations were persisted.
            let log = state.conversations.read(&key);
            if !log.is_empty() {
                d.conversation = log
                    .into_iter()
                    .map(|m| cockpit::Turn {
                        role: m.role,
                        text: m.text,
                    })
                    .collect();
            }
            Json(d).into_response()
        }
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
        _ => match cockpit::latest_process_xml(&state.target, &req.process_id).await {
            Ok(xml) => xml,
            Err(e) => return bad_gateway(e),
        },
    };
    let max_iterations = req.max_iterations.unwrap_or(2).clamp(1, 50);
    match cockpit::create_experiment(
        &state.own,
        &req.process_id,
        baseline,
        max_iterations,
        req.prompt_id.clone(),
    )
    .await
    {
        Ok(key) => {
            // Seed the durable conversation with the engine's framing turn so the log
            // is a complete dialogue from the first render, not just from round 1.
            let prompt_note = req
                .prompt_id
                .as_deref()
                .map(|p| format!(" Droid prompt: {p}."))
                .unwrap_or_default();
            state.conversations.append(
                &key,
                "engine",
                &format!(
                    "Experiment forked from “{}”. The recorded production dataset is the fitness data; the engine replays every candidate against it.{prompt_note}",
                    req.process_id
                ),
                Some(0),
            );
            Json(serde_json::json!({ "instanceKey": key })).into_response()
        }
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
    let note = req.note.as_deref().map(str::trim).filter(|s| !s.is_empty());
    // Persist the pilot's turn (decision + any free-text guidance) before acting, so
    // the dialogue is durable even if the engine call below fails.
    let pilot_text = match note {
        Some(n) => format!("[{decision}] {n}"),
        None => format!("[{decision}]"),
    };
    state.conversations.append(&key, "pilot", &pilot_text, None);
    match cockpit::submit_decision(&state.own, &key, decision, note).await {
        Ok(()) => Json(serde_json::json!({ "ok": true })).into_response(),
        Err(e) => bad_gateway(e),
    }
}

/// `GET /api/cockpit/experiments/{key}/conversation` — the persisted pilot ↔ droid log.
async fn cockpit_conversation(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    Json(state.conversations.read(&key))
}

/// `POST /api/cockpit/experiments/{key}/conversation` — append a turn to the
/// persisted conversation. Used by the pilot to chat with the droid out of band, and
/// by the worker to record the droid's per-round result. Role defaults to `pilot`.
async fn cockpit_message(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(req): Json<CockpitMessageRequest>,
) -> impl IntoResponse {
    let text = req.text.trim();
    if text.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "message text must not be empty" })),
        )
            .into_response();
    }
    let role = match req.role.as_deref().map(str::trim) {
        Some("droid") => "droid",
        Some("engine") => "engine",
        _ => "pilot",
    };
    let msg = state.conversations.append(&key, role, text, req.round);
    (StatusCode::CREATED, Json(msg)).into_response()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CockpitCreateRequest {
    process_id: String,
    #[serde(default)]
    baseline_model: Option<String>,
    #[serde(default)]
    max_iterations: Option<i64>,
    /// Library prompt id to drive the droid (the experimental variable, §7.8 step 2).
    #[serde(default)]
    prompt_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CockpitDecisionRequest {
    decision: String,
    /// Optional free-text guidance from the pilot for the next round — persisted to
    /// the conversation and threaded into the BPMN as `pilotNote` so the droid sees it.
    #[serde(default)]
    note: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CockpitMessageRequest {
    text: String,
    /// `pilot` (default), `droid`, or `engine`.
    #[serde(default)]
    role: Option<String>,
    /// The optimization round this turn belongs to, when known.
    #[serde(default)]
    round: Option<i64>,
}

fn bad_gateway(e: String) -> axum::response::Response {
    (
        StatusCode::BAD_GATEWAY,
        Json(serde_json::json!({ "error": e })),
    )
        .into_response()
}

fn unprocessable(e: String) -> axum::response::Response {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(serde_json::json!({ "error": e })),
    )
        .into_response()
}

// --- Workspaces (customer/deployment bounded contexts) > processes (the consultant's folder tree) -----------

/// The single-file workspace browser. Served at `/workspace`.
const WORKSPACE_HTML: &str = include_str!("workspace.html");

/// Vendored bpmn-js viewer assets (self-contained, served at `/assets/bpmn/*`), so
/// the workspace console can render a real BPMN diagram with no CDN/build step.
const BPMN_VIEWER_JS: &str = include_str!("../assets/bpmn/bpmn-navigated-viewer.js");
const BPMN_AUTO_LAYOUT_JS: &str = include_str!("../assets/bpmn/bpmn-auto-layout.js");
const BPMN_DIAGRAM_CSS: &str = include_str!("../assets/bpmn/diagram-js.css");
const BPMN_EMBEDDED_CSS: &str = include_str!("../assets/bpmn/bpmn-embedded.css");
const SETTINGS_JS: &str = include_str!("../assets/settings.js");

/// The bundled loan-approval demo pack + model, embedded so `seed-demo` works from
/// any working directory.
const LOAN_PACK_JSON: &str = include_str!("../corpus-packs/loan-approval/pack.json");
const LOAN_MODEL_BPMN: &str = include_str!("../corpus-packs/loan-approval/loan-approval.bpmn");

/// `GET /workspace` — browse workspaces, drill into a process, view its Insights.
async fn workspace_page() -> Html<&'static str> {
    Html(WORKSPACE_HTML)
}

#[derive(Debug, Deserialize)]
struct CreateWorkspaceBody {
    #[serde(default, alias = "displayName", alias = "name")]
    display_name: String,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateProcessBody {
    #[serde(default, alias = "displayName", alias = "name")]
    display_name: String,
    #[serde(default, alias = "targetUrl")]
    target_url: Option<String>,
    #[serde(default)]
    dataset: Option<String>,
    #[serde(default)]
    objective: Option<String>,
    #[serde(default)]
    notes: Option<String>,
}

/// `GET /api/workspaces` — every workspace (scanned from disk).
async fn ws_workspaces(State(state): State<AppState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "root": state.workspaces.root().to_string_lossy(),
        "workspaces": state.workspaces.list_workspaces(),
    }))
}

/// `POST /api/workspaces` — create a workspace from a display name.
async fn ws_create_workspace(
    State(state): State<AppState>,
    Json(body): Json<CreateWorkspaceBody>,
) -> impl IntoResponse {
    match state
        .workspaces
        .create_workspace(&body.display_name, body.notes)
    {
        Ok(c) => (StatusCode::CREATED, Json(c)).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `GET /api/workspaces/{workspace}` — a workspace + its processes.
async fn ws_workspace(
    State(state): State<AppState>,
    Path(workspace): Path<String>,
) -> impl IntoResponse {
    match state.workspaces.get_workspace(&workspace) {
        Some(c) => Json(serde_json::json!({
            "workspace": c,
            "processes": state.workspaces.list_processes(&workspace),
        }))
        .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("no such workspace: {workspace}") })),
        )
            .into_response(),
    }
}

/// `POST /api/workspaces/{workspace}/processes` — create a process.
async fn ws_create_process(
    State(state): State<AppState>,
    Path(workspace): Path<String>,
    Json(body): Json<CreateProcessBody>,
) -> impl IntoResponse {
    let config = workspace::ProcessConfig {
        display_name: body.display_name.clone(),
        target_url: body.target_url.filter(|s| !s.trim().is_empty()),
        dataset: body.dataset.filter(|s| !s.trim().is_empty()),
        objective: body.objective.filter(|s| !s.trim().is_empty()),
        notes: body.notes.filter(|s| !s.trim().is_empty()),
    };
    match state
        .workspaces
        .create_process(&workspace, &body.display_name, config)
    {
        Ok(p) => (StatusCode::CREATED, Json(p)).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `GET /api/workspaces/{workspace}/processes/{process}` — one process.
async fn ws_process(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
) -> impl IntoResponse {
    match state.workspaces.get_process(&workspace, &process) {
        Some(p) => Json(p).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("no such process: {workspace}/{process}") })),
        )
            .into_response(),
    }
}

/// `PUT /api/workspaces/{workspace}/processes/{process}` — update config.
async fn ws_update_process(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Json(config): Json<workspace::ProcessConfig>,
) -> impl IntoResponse {
    match state
        .workspaces
        .update_process(&workspace, &process, config)
    {
        Ok(p) => Json(p).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `GET /assets/bpmn/{file}` — serve a vendored bpmn-js viewer asset.
async fn bpmn_asset(Path(file): Path<String>) -> impl IntoResponse {
    let (body, ctype): (&'static str, &'static str) = match file.as_str() {
        "bpmn-navigated-viewer.js" => (BPMN_VIEWER_JS, "application/javascript; charset=utf-8"),
        "bpmn-auto-layout.js" => (BPMN_AUTO_LAYOUT_JS, "application/javascript; charset=utf-8"),
        "diagram-js.css" => (BPMN_DIAGRAM_CSS, "text/css; charset=utf-8"),
        "bpmn-embedded.css" => (BPMN_EMBEDDED_CSS, "text/css; charset=utf-8"),
        _ => return (StatusCode::NOT_FOUND, "not found").into_response(),
    };
    ([(axum::http::header::CONTENT_TYPE, ctype)], body).into_response()
}

/// Serves the shared settings panel module (`/assets/settings.js`), included by both the
/// console and the cockpit.
async fn settings_js() -> impl IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        SETTINGS_JS,
    )
}

/// `GET /api/workspaces/{workspace}/processes/{process}/model` — the process's BPMN
/// XML, for the viewer. 404 when the process has no `model.bpmn`.
async fn ws_process_model(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
) -> impl IntoResponse {
    match state.workspaces.read_model(&workspace, &process) {
        Some(xml) => (
            [(
                axum::http::header::CONTENT_TYPE,
                "application/xml; charset=utf-8",
            )],
            xml,
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no model.bpmn for this process" })),
        )
            .into_response(),
    }
}

/// `PUT /api/workspaces/{workspace}/processes/{process}/model` — set the BPMN model
/// (raw XML request body).
async fn ws_set_process_model(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    body: String,
) -> impl IntoResponse {
    if body.trim().is_empty() {
        return unprocessable("empty model body".into());
    }
    match state.workspaces.write_model(&workspace, &process, &body) {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => unprocessable(e),
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SeedDemoBody {
    /// Workspace display name (default "Northwind Bank").
    #[serde(default)]
    workspace: Option<String>,
    /// Process display name (default "Loan Approval").
    #[serde(default)]
    process: Option<String>,
}

/// `POST /api/workspaces/seed-demo` — materialise the bundled loan-approval demo: a
/// workspace + a process bound to a freshly generated trace dataset + its BPMN model.
/// Idempotent on the derived slugs. Returns the created `{workspace, process}` slugs.
async fn ws_seed_demo(
    State(state): State<AppState>,
    Json(body): Json<SeedDemoBody>,
) -> impl IntoResponse {
    let ws_name = body
        .workspace
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "Northwind Bank".to_string());
    let proc_name = body
        .process
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "Loan Approval".to_string());

    // Run the (blocking) corpus generation off the async runtime.
    let workspaces = state.workspaces.clone();
    let result = tokio::task::spawn_blocking(move || seed_demo(&workspaces, &ws_name, &proc_name))
        .await
        .map_err(|e| format!("seed task failed: {e}"));

    match result {
        Ok(Ok(slugs)) => (StatusCode::CREATED, Json(slugs)).into_response(),
        Ok(Err(e)) => unprocessable(e),
        Err(e) => bad_gateway(e),
    }
}

/// Create the workspace + process, generate the corpus into its `traces/` folder, and
/// drop the BPMN model beside it. Returns the slugs.
fn seed_demo(
    workspaces: &workspace::WorkspaceCatalog,
    ws_name: &str,
    proc_name: &str,
) -> Result<serde_json::Value, String> {
    let ws = workspaces.create_workspace(
        ws_name,
        Some("Demo engagement (bundled loan-approval corpus)".into()),
    )?;
    let proc_cfg = workspace::ProcessConfig {
        display_name: proc_name.to_string(),
        objective: Some("keep loan approvals fast and reliable as volume grows".into()),
        ..Default::default()
    };
    let proc = workspaces.create_process(&ws.slug, proc_name, proc_cfg)?;

    // Write the embedded pack + bpmn to a temp dir, then load + generate.
    let tmp = std::env::temp_dir().join(format!(
        "processos-seed-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&tmp).map_err(|e| format!("seed tmp: {e}"))?;
    std::fs::write(tmp.join("pack.json"), LOAN_PACK_JSON)
        .map_err(|e| format!("write pack: {e}"))?;
    std::fs::write(tmp.join("loan-approval.bpmn"), LOAN_MODEL_BPMN)
        .map_err(|e| format!("write bpmn: {e}"))?;

    let (pack, def) = corpus::load_pack(&tmp.join("pack.json"))?;
    let traces_dir = workspaces.process_traces_dir(&ws.slug, &proc.slug)?;
    let summary = corpus::generate(&pack, &def, &traces_dir)?;
    workspaces.write_model(&ws.slug, &proc.slug, LOAN_MODEL_BPMN)?;
    let _ = std::fs::remove_dir_all(&tmp);

    Ok(serde_json::json!({
        "workspace": ws.slug,
        "process": proc.slug,
        "instances": summary.instances,
    }))
}

/// folded Insights report for this process, read from whatever source it is bound
/// to (live customer Nano or a loaded trace dataset).
async fn ws_process_insights(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Query(q): Query<InsightsQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    let sample = q.sample.unwrap_or(50).clamp(1, 500);
    let src = match state.workspaces.resolve_source(&workspace, &process) {
        Ok(s) => s,
        Err(e) => return unprocessable(e),
    };
    match report::build_over(&src, limit, sample).await {
        Ok(report) => Json(report).into_response(),
        Err(e) => bad_gateway(e),
    }
}

/// `GET /api/settings` — the operator's persisted settings (LLM profiles + active
/// profile + Python interpreter), redacted so API keys are never echoed back.
async fn get_settings(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.settings.view())
}

/// `PUT /api/settings` — update the globals: the active profile and/or the Python
/// interpreter. Absent fields are left unchanged.
async fn put_settings(
    State(state): State<AppState>,
    Json(patch): Json<settings::GlobalsPatch>,
) -> impl IntoResponse {
    match state.settings.update_globals(patch) {
        Ok(view) => Json(view).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `POST /api/settings/profiles` — create a new LLM profile (optionally seeded from the
/// body). Returns `{ id, settings }`.
async fn post_profile(
    State(state): State<AppState>,
    Json(patch): Json<settings::ProfilePatch>,
) -> impl IntoResponse {
    match state.settings.add_profile(patch) {
        Ok((id, view)) => Json(serde_json::json!({ "id": id, "settings": view })).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `PUT /api/settings/profiles/{id}` — partial update of one profile. Absent fields are
/// left unchanged; an empty string clears a field back to the environment default.
async fn put_profile(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(patch): Json<settings::ProfilePatch>,
) -> impl IntoResponse {
    match state.settings.update_profile(&id, patch) {
        Ok(view) => Json(view).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `DELETE /api/settings/profiles/{id}` — remove a profile.
async fn delete_profile(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.settings.delete_profile(&id) {
        Ok(view) => Json(view).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `POST /api/settings/models` — query an endpoint's model list so the console can pick a
/// model. Body: `{ profileId?, provider?, baseUrl?, apiKey? }` (a saved profile supplies a
/// stored key; inline fields let the console probe an endpoint it is still editing).
async fn post_models(
    State(state): State<AppState>,
    Json(req): Json<settings::ProbeRequest>,
) -> impl IntoResponse {
    let cfg = match state.settings.probe_config(&req) {
        Ok(c) => c,
        Err(e) => return unprocessable(e),
    };
    match list_models(&cfg).await {
        Ok(models) => Json(serde_json::json!({ "models": models })).into_response(),
        Err(e) => bad_gateway(e),
    }
}

/// Bytes downloaded so far if `model`'s GGUF is mid-download in the Hugging Face cache under
/// `models_dir` (llama.cpp writes `models--org--repo/blobs/<sha>.downloadInProgress` while a
/// `-hf` model is still being fetched). Returns the largest such partial file's size, or `None`
/// when nothing is downloading. This is how we tell "the model is still downloading" apart from
/// "the server is up" — a `-hf` sidecar downloads the whole GGUF before it ever binds its port.
fn download_in_progress(models_dir: &std::path::Path, model: &str) -> Option<u64> {
    let repo = model.split(':').next().unwrap_or(model);
    let folder = format!("models--{}", repo.replace('/', "--"));
    let blobs = models_dir.join(folder).join("blobs");
    let mut best: Option<u64> = None;
    if let Ok(entries) = std::fs::read_dir(&blobs) {
        for e in entries.flatten() {
            if e.file_name()
                .to_str()
                .is_some_and(|n| n.ends_with(".downloadInProgress"))
            {
                if let Ok(m) = e.metadata() {
                    let sz = m.len();
                    best = Some(best.map_or(sz, |b| b.max(sz)));
                }
            }
        }
    }
    best
}

/// The real, operator-facing lifecycle phase of a sidecar — beyond the binary "the process is
/// running" flag, which is misleading because a freshly-spawned `llama-server` is NOT usable for
/// minutes: it first downloads the model (no port yet), then loads it into memory (`/health` 503),
/// and only then serves chat (`/health` 200). Returns `(phase, human-detail)`:
///   down · starting · downloading · loading · ready
async fn sidecar_phase(status: &llama::LlamaStatus) -> (String, Option<String>) {
    if !status.running {
        return ("down".into(), None);
    }
    if let Some(port) = status.port {
        let url = format!("http://127.0.0.1:{port}/health");
        if let Ok(client) = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(2))
            .build()
        {
            match client.get(&url).send().await {
                Ok(r) if r.status().is_success() => return ("ready".into(), None),
                // The HTTP server is up but the model is still loading into memory (llama-server
                // answers /health with 503 until the weights are mapped and warmed).
                Ok(_) => return ("loading".into(), Some("loading model into memory…".into())),
                // Connection refused / unreachable: the process is alive but hasn't bound its
                // port — almost always because it is still downloading the model.
                Err(_) => {}
            }
        }
    }
    if let (Some(dir), Some(model)) = (status.models_dir.as_deref(), status.model.as_deref()) {
        if let Some(bytes) = download_in_progress(std::path::Path::new(dir), model) {
            let gb = bytes as f64 / 1_073_741_824.0;
            return (
                "downloading".into(),
                Some(format!("downloading model… {gb:.1} GB fetched so far")),
            );
        }
    }
    ("starting".into(), Some("starting llama-server…".into()))
}

/// `GET /api/llama/status` — the local sidecar pool: every running `llama-server` (model, port,
/// pid, the equivalent terminal command) plus the capacity, so the UI knows whether another may
/// be started. Each sidecar also carries its real lifecycle `phase`/`detail` (downloading vs
/// loading vs ready) so the UI never reports a still-downloading model as usable, and `anyReady`
/// is true only when at least one sidecar can actually serve a chat.
async fn llama_status(State(state): State<AppState>) -> impl IntoResponse {
    let list = state.llama.statuses();
    let mut sidecars = Vec::with_capacity(list.sidecars.len());
    let mut any_ready = false;
    for s in &list.sidecars {
        let (phase, detail) = sidecar_phase(s).await;
        if phase == "ready" {
            any_ready = true;
        }
        let mut v = serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({}));
        if let Some(obj) = v.as_object_mut() {
            obj.insert("phase".into(), serde_json::json!(phase));
            obj.insert("detail".into(), serde_json::json!(detail));
        }
        sidecars.push(v);
    }
    Json(serde_json::json!({
        "running": list.running,
        "anyReady": any_ready,
        "count": list.count,
        "max": list.max,
        "sidecars": sidecars,
        "error": list.error,
    }))
}

/// `GET /api/llama/ready?profileId=…` — whether a sidecar is not just spawned but actually
/// answering (its model has finished loading). Probes the `llama-server` `/health` endpoint
/// (200 once ready, 503 while loading). With `profileId` it checks that specific sidecar; without
/// it, any running sidecar. Returns `{ running, ready, profileId }`. The cockpit polls this after
/// a just-in-time sidecar start, before sending a queued chat message.
async fn llama_ready(
    State(state): State<AppState>,
    Query(q): Query<LlamaProfileQuery>,
) -> impl IntoResponse {
    let status = match q.profile_id.as_deref() {
        Some(id) => state.llama.status_of(id),
        None => state
            .llama
            .statuses()
            .sidecars
            .into_iter()
            .next()
            .unwrap_or_else(|| state.llama.status_of("")),
    };
    let (phase, detail) = sidecar_phase(&status).await;
    let ready = phase == "ready";
    Json(serde_json::json!({
        "running": status.running, "ready": ready,
        "phase": phase, "detail": detail,
        "profileId": status.profile_id,
    }))
}

/// Request body for starting the sidecar: which saved profile to serve.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LlamaStartRequest {
    profile_id: String,
}

/// `POST /api/llama/start` — launch `llama-server` for the named `sidecar:true` profile. ProcessOS
/// auto-assigns a free TCP port (the operator never configures one); the assigned port is written
/// back into the profile's `base_url` so the model client talks to it. Up to [`llama::MAX_SIDECARS`]
/// run at once. The models directory is exported as `LLAMA_CACHE`.
async fn llama_start(
    State(state): State<AppState>,
    Json(req): Json<LlamaStartRequest>,
) -> impl IntoResponse {
    let snap = state.settings.snapshot();
    let profile = match snap.profiles.iter().find(|p| p.id == req.profile_id) {
        Some(p) => p.clone(),
        None => return unprocessable(format!("no such profile: {}", req.profile_id)),
    };
    if !profile.sidecar {
        return unprocessable(format!(
            "profile '{}' is not configured to use the local sidecar",
            profile.id
        ));
    }
    let models_dir = snap.effective_models_dir();
    let status = match state
        .llama
        .start_profile(&profile, &models_dir, snap.llama_bin.as_deref())
    {
        Ok(s) => s,
        Err(e) => return unprocessable(e),
    };
    // Persist the auto-assigned port into the profile's base URL so the model client (resolve_llm /
    // as_llm_override) and the reasoning-control probe all reach the actual running endpoint.
    if let Some(port) = status.port {
        let base_url = format!("http://127.0.0.1:{port}/v1");
        if profile.base_url.as_deref() != Some(base_url.as_str()) {
            let patch = settings::ProfilePatch {
                base_url: Some(base_url),
                ..Default::default()
            };
            let _ = state.settings.update_profile(&profile.id, patch);
        }
    }
    Json(status).into_response()
}

/// Request body for stopping a sidecar: an optional profile id (omit to stop all).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LlamaStopRequest {
    #[serde(default)]
    profile_id: Option<String>,
}

/// `POST /api/llama/stop` — stop one sidecar (`{profileId}`) or all of them (empty body).
/// Idempotent. Returns the resulting pool state.
async fn llama_stop(
    State(state): State<AppState>,
    body: Option<Json<LlamaStopRequest>>,
) -> impl IntoResponse {
    let profile_id = body.and_then(|Json(b)| b.profile_id);
    Json(state.llama.stop(profile_id.as_deref()).await)
}

/// Query for incremental log polling (`?profileId=…&since=<offset>`).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LlamaLogsQuery {
    #[serde(default)]
    since: u64,
    #[serde(default)]
    profile_id: Option<String>,
}

/// Query carrying just a profile id (`?profileId=…`).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LlamaProfileQuery {
    #[serde(default)]
    profile_id: Option<String>,
}

/// `GET /api/llama/logs?profileId=…&since=N` — a sidecar's captured stdout/stderr from offset `N`
/// onward, plus the next offset to poll and whether the process is still running. Powers the
/// streaming log viewer. When `profileId` is omitted, the single running sidecar is used.
async fn llama_logs(
    State(state): State<AppState>,
    Query(q): Query<LlamaLogsQuery>,
) -> impl IntoResponse {
    let profile_id = q.profile_id.or_else(|| {
        state
            .llama
            .statuses()
            .sidecars
            .into_iter()
            .next()
            .and_then(|s| s.profile_id)
    });
    let Some(profile_id) = profile_id else {
        return Json(serde_json::json!({
            "lines": [], "text": "", "nextOffset": q.since, "running": false,
        }));
    };
    let (lines, next, running) = state.llama.logs_since(&profile_id, q.since);
    let text = lines.join("\n");
    Json(serde_json::json!({
        "lines": lines, "text": text, "nextOffset": next, "running": running, "profileId": profile_id,
    }))
}

/// `GET /api/llama/reasoning-control?profileId=…` — probe whether the target endpoint exposes the
/// optional reasoning-control surface (`/v1/chat/completions/control`). When `profileId` is given
/// the named profile's endpoint is probed, otherwise the active profile's. Lets the UI tell the
/// operator whether wrap-up/monitor can halt mid-thinking or only at a round boundary (fallback).
async fn reasoning_control_status(
    State(state): State<AppState>,
    Query(q): Query<LlamaLogsQuery>,
) -> impl IntoResponse {
    let cfg = match q.profile_id.as_deref() {
        Some(pid) => resolve_llm_for_profile(&state, Some(pid), None),
        None => Some(resolve_llm(&state, None)),
    };
    let Some(cfg) = cfg.filter(|c| c.is_ready()) else {
        return Json(serde_json::json!({ "supported": false, "ready": false }));
    };
    let supported = reasoning::supports_control(&cfg.base_url).await;
    Json(serde_json::json!({
        "supported": supported,
        "ready": true,
        "baseUrl": cfg.base_url,
    }))
}

/// Resolve the effective LLM config for a request: built-in defaults → `PROCESSOS_LLM_*`
/// env → the active operator profile → per-request override. Each later layer wins when
/// present, so the console is authoritative over the environment while a one-off request
/// body can still override everything.
fn resolve_llm(state: &AppState, req: Option<&LlmOverride>) -> LlmConfig {
    let mut cfg = LlmConfig::from_env().with_override(&state.settings.snapshot().as_llm_override());
    if let Some(o) = req {
        cfg = cfg.with_override(o);
    }
    cfg
}

/// Resolve an LLM config for a **specific named profile** (not the active one): env defaults →
/// the named profile's settings → an optional one-off override. Used for Pair AI reviewers,
/// which target their own profile independent of the operator's active primary profile. Returns
/// `None` if `profile_id` is given but no such profile exists.
fn resolve_llm_for_profile(
    state: &AppState,
    profile_id: Option<&str>,
    ovr: Option<&LlmOverride>,
) -> Option<LlmConfig> {
    let mut cfg = LlmConfig::from_env();
    if let Some(pid) = profile_id.map(str::trim).filter(|s| !s.is_empty()) {
        let snap = state.settings.snapshot();
        let p = snap.profiles.iter().find(|p| p.id == pid)?;
        cfg = cfg.with_override(&p.as_llm_override());
    }
    if let Some(o) = ovr {
        cfg = cfg.with_override(o);
    }
    Some(cfg)
}

/// Build the Pair AI reviewer pipeline from a chat request. An explicit `pairs` chain wins;
/// otherwise the single `pair` (if enabled) becomes a one-stage chain. Each enabled stage must
/// resolve to a ready LLM config and a pairing persona, else the whole turn is rejected (the
/// operator asked for a reviewer — failing loudly beats silently dropping it).
fn build_pair_stages(
    state: &AppState,
    req: &ChatSendRequest,
) -> Result<Vec<investigate::PairStage>, String> {
    let specs: Vec<&PairRequest> = if !req.pairs.is_empty() {
        req.pairs.iter().collect()
    } else {
        req.pair.iter().collect()
    };
    let mut stages = Vec::new();
    for spec in specs.into_iter().filter(|p| p.enabled) {
        let cfg = resolve_llm_for_profile(state, spec.profile_id.as_deref(), spec.llm.as_ref())
            .ok_or_else(|| {
                format!(
                    "Pair AI reviewer references unknown profile '{}'",
                    spec.profile_id.as_deref().unwrap_or("")
                )
            })?;
        if !cfg.is_ready() {
            return Err(
                "Pair AI is enabled but its reviewer has no model configured — pick a profile \
                 for the Pair AI in the composer, or disable Pair AI"
                    .to_string(),
            );
        }
        let (id, name, system) = state.personas.resolve_pair(spec.persona_id.as_deref());
        stages.push(investigate::PairStage {
            id,
            name,
            cfg,
            system,
        });
    }
    Ok(stages)
}

/// Resolve the loop-monitor config from a chat request, honouring an env default. Returns the
/// `(LlmConfig, persona_system)` to run the monitor with, or `None` when monitoring is off or no
/// model can be resolved (in which case the turn simply runs without a monitor).
///
/// Enablement: the request's `monitor.enabled` wins; absent that, `PROCESSOS_MONITOR` set to a
/// truthy value (or a profile id) enables it so the monitor can be used without UI.
fn resolve_monitor(state: &AppState, mon: Option<&MonitorRequest>) -> Option<(LlmConfig, String)> {
    let env = std::env::var("PROCESSOS_MONITOR").ok();
    let (enabled, profile_id, ovr, persona_id) = match mon {
        Some(m) if m.enabled => (
            true,
            m.profile_id.clone(),
            m.llm.clone(),
            m.persona_id.clone(),
        ),
        _ => {
            // Env fallback: "0"/"off"/"false"/"" disable; anything else enables (and a non-bool
            // value is treated as the profile id to run the monitor on).
            let raw = env.as_deref().map(str::trim).unwrap_or("");
            let off = matches!(
                raw.to_ascii_lowercase().as_str(),
                "" | "0" | "off" | "false" | "no"
            );
            if off {
                return None;
            }
            let is_bool = matches!(
                raw.to_ascii_lowercase().as_str(),
                "1" | "on" | "true" | "yes"
            );
            let profile = if is_bool { None } else { Some(raw.to_string()) };
            (true, profile, None, None)
        }
    };
    if !enabled {
        return None;
    }
    let cfg = resolve_llm_for_profile(state, profile_id.as_deref(), ovr.as_ref())?;
    if !cfg.is_ready() {
        return None;
    }
    let (_id, system) = state.personas.resolve_monitor(persona_id.as_deref());
    Some((cfg, system))
}

/// How often the loop monitor re-reads the primary's transcript, and how many times it may steer
/// before escalating to a forced wrap-up. Deliberately conservative so the monitor is cheap and
/// can never itself spam the conversation.
const MONITOR_INTERVAL: std::time::Duration = std::time::Duration::from_secs(12);
const MONITOR_MAX_STEERS: usize = 3;

/// The out-of-band loop-monitor watcher. Polls the (incrementally persisted) transcript on a
/// fixed cadence; when its model says the primary is circling it pushes a steer into the running
/// turn's steer queue, and after its steer budget is spent it flips the cancel flag to force a
/// graceful wrap-up. It never blocks the primary and any monitor error is swallowed (the monitor
/// failing must never take down the investigation).
#[allow(clippy::too_many_arguments)]
async fn run_loop_monitor(
    cfg: LlmConfig,
    persona_system: String,
    chat: Arc<chat::ChatStore>,
    key: String,
    sid: String,
    steer: Arc<std::sync::Mutex<Vec<String>>>,
    cancel: Arc<std::sync::atomic::AtomicBool>,
    live: LiveTurn,
    done: Arc<std::sync::atomic::AtomicBool>,
    // Endpoint of the PRIMARY model this monitor governs (for the reasoning-control call).
    primary_base_url: String,
    // The primary turn's live completion id (`chatcmpl-…`), set once it starts replying.
    primary_cmpl_id: Arc<std::sync::Mutex<Option<String>>>,
) {
    use std::sync::atomic::Ordering;
    // Read the primary's current live completion id, if it has started replying.
    let live_cmpl = |cell: &Arc<std::sync::Mutex<Option<String>>>| -> Option<String> {
        cell.lock().ok().and_then(|c| c.clone())
    };
    let mut policy = monitor::MonitorPolicy::new(MONITOR_MAX_STEERS);
    let mut last_window = String::new();
    loop {
        tokio::time::sleep(MONITOR_INTERVAL).await;
        if done.load(Ordering::Relaxed) || cancel.load(Ordering::Relaxed) {
            return;
        }
        let messages = chat.get(&key, &sid).map(|s| s.messages).unwrap_or_default();
        let window = monitor::render_window(&messages);
        // Skip when the transcript hasn't advanced since the last evaluation — no new behaviour
        // to judge, so don't spend a monitor call (and don't risk re-flagging the same state).
        if window.is_empty() || window == last_window {
            continue;
        }
        last_window = window.clone();
        let verdict = monitor::evaluate(&cfg, &persona_system, &window).await;
        // The primary may have finished while the monitor was thinking.
        if done.load(Ordering::Relaxed) || cancel.load(Ordering::Relaxed) {
            return;
        }
        match policy.observe(&verdict) {
            monitor::MonitorAction::None => {}
            monitor::MonitorAction::Steer(text) => {
                if let Ok(mut q) = steer.lock() {
                    q.push(format!("[loop monitor] {text}"));
                }
                // If the primary supports reasoning control, end its current (circling) thinking
                // now so the steer lands sooner instead of waiting out the whole think budget.
                if let Some(id) = live_cmpl(&primary_cmpl_id) {
                    let _ = crate::reasoning::end_reasoning(&primary_base_url, &id).await;
                }
                live.emit(serde_json::json!({
                    "type": "monitor",
                    "action": "steer",
                    "reason": verdict.reason,
                    "steer": text,
                }));
            }
            monitor::MonitorAction::WrapUp(reason) => {
                cancel.store(true, Ordering::Relaxed);
                // Prefer a mid-thinking halt via the reasoning-control surface (ends the live
                // turn's reasoning so it answers now); the cancel flag is the round-boundary
                // fallback on builds without the surface or before the turn has a completion id.
                if let Some(id) = live_cmpl(&primary_cmpl_id) {
                    let _ = crate::reasoning::end_reasoning(&primary_base_url, &id).await;
                }
                live.emit(serde_json::json!({
                    "type": "monitor",
                    "action": "wrapup",
                    "reason": reason,
                }));
                return;
            }
        }
    }
}

/// Request body for an investigation: optional LLM override + bounds.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InvestigateRequest {
    #[serde(default)]
    llm: Option<LlmOverride>,
    /// Max instances to load into the analytic view (default 100k).
    #[serde(default)]
    limit: Option<usize>,
    /// Max model↔tool rounds before giving up (default 12).
    #[serde(default)]
    max_rounds: Option<usize>,
    /// Offer the trusted Python escape hatch (`run_python`) in addition to the SQL tool.
    /// Off by default; gated behind the SQL tool by prompt discipline. The tool runs
    /// ARBITRARY operator-trusted code (timeout + private workdir, not a sandbox).
    #[serde(default)]
    allow_python: bool,
}

/// `POST /api/workspaces/{workspace}/processes/{process}/investigate` — point
/// the configured LLM at this process's bound trace source and let it form and test its
/// own hypotheses over the data via the read-only `query_traces` SQL tool. Returns the
/// agent's conclusion plus the full query/result "lab notebook".
async fn ws_process_investigate(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Json(req): Json<InvestigateRequest>,
) -> impl IntoResponse {
    let cfg = resolve_llm(&state, req.llm.as_ref());
    if !cfg.is_ready() {
        return unprocessable(
            "no LLM model configured; set it in the console settings (cog, lower-left) or \
             via PROCESSOS_LLM_MODEL (and PROCESSOS_LLM_BASE_URL / PROCESSOS_LLM_PROVIDER \
             as needed), or pass an `llm` object with at least `model` in the request body"
                .to_string(),
        );
    }
    let src = match state.workspaces.resolve_source(&workspace, &process) {
        Ok(s) => s,
        Err(e) => return unprocessable(e),
    };
    let limit = req.limit.unwrap_or(100_000).clamp(1, 1_000_000);
    let max_rounds = req.max_rounds.unwrap_or(12).clamp(1, 40);
    let allow_python = req.allow_python;
    let py = state.settings.snapshot().py_config();
    // The DuckDB connection inside the analysis is !Send, so the investigation cannot
    // cross the multi-threaded handler's await boundary. Run the whole loop on a
    // dedicated current-thread runtime where the connection never leaves its thread.
    let task = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("runtime: {e}"))?;
        rt.block_on(investigate::run_investigation(
            &src,
            cfg,
            py,
            limit,
            max_rounds,
            allow_python,
        ))
    })
    .await;
    match task {
        Ok(Ok(report)) => Json(report).into_response(),
        Ok(Err(e)) => bad_gateway(e),
        Err(e) => bad_gateway(format!("investigation task failed: {e}")),
    }
}

// --- Interactive cockpit chat ----------------------------------------------------

/// Query string carrying the target chat session id (shared by load/stream/wrapup).
#[derive(Debug, Default, Deserialize)]
struct SessionQuery {
    #[serde(default)]
    session: Option<String>,
}

/// Per-session wrap-up flag key, so an in-flight turn in one session can be wrapped up
/// without touching another session of the same dataset.
fn chat_cancel_key(key: &str, session_id: &str) -> String {
    format!("{key}::{session_id}")
}

/// Resolve the session to act on: the explicit `?session=` id if it exists, else the most
/// recently active session, creating a first one if the dataset has none yet.
fn resolve_session(state: &AppState, key: &str, wanted: Option<&str>) -> chat::SessionMeta {
    if let Some(id) = wanted {
        if let Some(s) = state.chat.get(key, id) {
            return chat::SessionMeta {
                id: s.id,
                name: s.name,
                created: s.created,
                updated: s.updated,
                turns: 0,
                persona: s.persona,
            };
        }
    }
    let mut list = state.chat.list(key);
    if let Some(first) = list.drain(..).next() {
        return first;
    }
    let s = state.chat.create(key, None);
    chat::SessionMeta {
        id: s.id,
        name: s.name,
        created: s.created,
        updated: s.updated,
        turns: 0,
        persona: s.persona,
    }
}

/// `GET .../chat/sessions` — list this dataset's chat sessions (newest activity first),
/// creating an initial one so the cockpit always has a session to open.
async fn cockpit_chat_sessions_list(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let mut sessions = state.chat.list(&key);
    if sessions.is_empty() {
        state.chat.create(&key, None);
        sessions = state.chat.list(&key);
    }
    Json(serde_json::json!({ "sessions": sessions })).into_response()
}

/// Body for creating / renaming a chat session.
#[derive(Debug, Default, Deserialize)]
struct ChatSessionNameRequest {
    #[serde(default)]
    name: Option<String>,
}

/// `POST .../chat/sessions` — start a new (empty) chat session.
async fn cockpit_chat_sessions_create(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    body: Option<Json<ChatSessionNameRequest>>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let name = body.and_then(|Json(b)| b.name);
    let s = state.chat.create(&key, name);
    Json(serde_json::json!({
        "id": s.id, "name": s.name, "created": s.created, "updated": s.updated, "turns": 0,
    }))
    .into_response()
}

/// `GET .../chat/sessions/{id}` — load one session's transcript as timestamped turns.
async fn cockpit_chat_session_load(
    State(state): State<AppState>,
    Path((workspace, process, id)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    match state.chat.get(&key, &id) {
        Some(s) => Json(serde_json::json!({
            "id": s.id,
            "name": s.name,
            "turns": chat::render_view_stamped(&s.messages, &s.stamps),
        }))
        .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no such chat session" })),
        )
            .into_response(),
    }
}

/// `POST .../chat/sessions/{id}/rename` — rename a chat session.
async fn cockpit_chat_session_rename(
    State(state): State<AppState>,
    Path((workspace, process, id)): Path<(String, String, String)>,
    Json(req): Json<ChatSessionNameRequest>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let name = req.name.unwrap_or_default();
    if name.trim().is_empty() {
        return unprocessable("name must not be empty".to_string());
    }
    if state.chat.rename(&key, &id, &name) {
        StatusCode::NO_CONTENT.into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no such chat session" })),
        )
            .into_response()
    }
}

/// `GET .../chat/sessions/{id}/debug` — the exact model request payloads sent during this
/// session's most recent turn (one per round): system prompt, tool specs, and the full
/// transcript. Powers the cockpit's per-chat "Debug" tab so the operator can see everything
/// the model receives, not just their latest message. Empty until a turn has run this session.
async fn cockpit_chat_session_debug(
    State(state): State<AppState>,
    Path((workspace, process, id)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let cancel_key = chat_cancel_key(&key, &id);
    let requests = state
        .chat_debug
        .lock()
        .ok()
        .and_then(|m| m.get(&cancel_key).cloned())
        .unwrap_or_default();
    Json(serde_json::json!({ "sessionId": id, "requests": requests }))
}

/// `GET .../chat/sessions/{id}/simulations` — the Alternate Reality Engine runs
/// (`simulate` / `compare_variants`) recorded in this session's persisted transcript, each
/// pairing the candidate model(s) with their fidelity scorecard. Powers the cockpit's
/// "Simulations" tab so the operator can watch the droid's exploration — the variants it forked
/// and how each scored. Reads the persisted transcript, so it survives restarts (unlike Debug).
async fn cockpit_chat_session_simulations(
    State(state): State<AppState>,
    Path((workspace, process, id)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let runs = state
        .chat
        .get(&key, &id)
        .map(|s| chat::extract_simulations(&s.messages))
        .unwrap_or_default();
    Json(serde_json::json!({ "sessionId": id, "runs": runs }))
}

/// `DELETE .../chat/sessions/{id}` — delete a chat session.
async fn cockpit_chat_session_delete(
    State(state): State<AppState>,
    Path((workspace, process, id)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    if state.chat.delete(&key, &id) {
        if let Ok(mut m) = state.chat_debug.lock() {
            m.remove(&chat_cancel_key(&key, &id));
        }
        StatusCode::NO_CONTENT.into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no such chat session" })),
        )
            .into_response()
    }
}

/// `GET /api/workspaces/{workspace}/processes/{process}/chat` — load a chat session's
/// transcript as operator-facing turns (the `?session=` one, or the most recent).
async fn cockpit_chat_load(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Query(q): Query<SessionQuery>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let meta = resolve_session(&state, &key, q.session.as_deref());
    let session = state.chat.get(&key, &meta.id).unwrap_or_default();
    // If a turn is mid-flight for this session (e.g. the page was reloaded), surface its user
    // message so the cockpit can redraw the in-flight user bubble and reattach to the live stream.
    let cancel_key = chat_cancel_key(&key, &meta.id);
    let inflight = state.chat_live.lock().ok().and_then(|m| {
        m.get(&cancel_key)
            .map(|lt| serde_json::json!({ "user": lt.user }))
    });
    Json(serde_json::json!({
        "sessionId": meta.id,
        "turns": chat::render_view_stamped(&session.messages, &session.stamps),
        "inflight": inflight,
    }))
    .into_response()
}

/// `GET .../chat/stream/live` — reattach to an in-flight chat turn's event stream (e.g. after a
/// page reload). Replays the buffered events from the start, then follows along until the turn
/// ends. Returns 404 when no turn is running for the session, so the cockpit can fall back to the
/// persisted transcript.
async fn cockpit_chat_stream_live(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Query(q): Query<SessionQuery>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let meta = resolve_session(&state, &key, q.session.as_deref());
    let cancel_key = chat_cancel_key(&key, &meta.id);
    let live = state
        .chat_live
        .lock()
        .ok()
        .and_then(|m| m.get(&cancel_key).cloned());
    match live {
        Some(live) => sse_from_live(live).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no investigation is running for this session" })),
        )
            .into_response(),
    }
}

/// Request body for an interactive chat turn.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChatSendRequest {
    /// The operator's message to the droid.
    message: String,
    #[serde(default)]
    llm: Option<LlmOverride>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    max_rounds: Option<usize>,
    /// Offer the trusted Python escape hatch in addition to the SQL tool (off by default).
    #[serde(default)]
    allow_python: bool,
    /// The persona (standing system prompt) for this conversation. Only takes effect on the
    /// first turn of a session — afterwards the persona is baked into the persisted transcript.
    #[serde(default)]
    persona_id: Option<String>,
    /// Pair AI: a single reviewer agent that runs after the primary each turn (off unless
    /// `enabled`). Convenience for the cockpit's one-reviewer UI.
    #[serde(default)]
    pair: Option<PairRequest>,
    /// Pair AI (N-tier): an explicit chain of reviewer stages run in sequence after the primary,
    /// each handed the previous stage's answer. Takes precedence over `pair` when non-empty.
    #[serde(default)]
    pairs: Vec<PairRequest>,
    /// Loop monitor: a second model that watches this turn's live transcript and steers the
    /// primary when it goes in circles (off unless `enabled`).
    #[serde(default)]
    monitor: Option<MonitorRequest>,
}

/// One configured Pair AI reviewer in a chat request.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PairRequest {
    /// Whether this reviewer is active. The cockpit sends `enabled:false` (or omits the object)
    /// when Pair AI is switched off, so the turn runs exactly as a single-agent investigation.
    #[serde(default)]
    enabled: bool,
    /// The saved LLM profile the reviewer uses (its own model — ideally a different family from
    /// the primary). Falls back to the env default when absent.
    #[serde(default)]
    profile_id: Option<String>,
    /// A one-off LLM override layered on top of the profile (rarely needed).
    #[serde(default)]
    llm: Option<LlmOverride>,
    /// The pairing persona (reviewer system prompt); defaults to the built-in skeptic.
    #[serde(default)]
    persona_id: Option<String>,
}

/// Loop-monitor configuration for a chat turn. When `enabled`, a second model watches the
/// primary's live transcript at a fixed cadence and steers it (or, after its budget, forces a
/// wrap-up) when it goes in circles. Off unless `enabled` — existing turns are unaffected.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MonitorRequest {
    #[serde(default)]
    enabled: bool,
    /// The saved LLM profile the monitor uses (ideally a small/cheap model). Falls back to the
    /// env default when absent.
    #[serde(default)]
    profile_id: Option<String>,
    /// A one-off LLM override layered on top of the profile (rarely needed).
    #[serde(default)]
    llm: Option<LlmOverride>,
    /// The monitor persona (its system prompt); defaults to the built-in loop breaker.
    #[serde(default)]
    persona_id: Option<String>,
}

/// `POST .../chat` — send one operator message; the droid replies (running SQL/Python
/// tool calls as needed), resuming from the persisted transcript. The updated transcript
/// is saved so the next turn keeps full context.
async fn cockpit_chat_send(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Query(q): Query<SessionQuery>,
    Json(req): Json<ChatSendRequest>,
) -> impl IntoResponse {
    if req.message.trim().is_empty() {
        return unprocessable("message must not be empty".to_string());
    }
    let cfg = resolve_llm(&state, req.llm.as_ref());
    if !cfg.is_ready() {
        return unprocessable(
            "no LLM model configured; set it in the console settings (cog, lower-left) or \
             via PROCESSOS_LLM_MODEL (and PROCESSOS_LLM_BASE_URL / PROCESSOS_LLM_PROVIDER \
             as needed), or pass an `llm` object with at least `model` in the request body"
                .to_string(),
        );
    }
    let src = match state.workspaces.resolve_source(&workspace, &process) {
        Ok(s) => s,
        Err(e) => return unprocessable(e),
    };
    let limit = req.limit.unwrap_or(100_000).clamp(1, 1_000_000);
    let max_rounds = req.max_rounds.unwrap_or(12).clamp(1, 40);
    let allow_python = req.allow_python;
    let py = state.settings.snapshot().py_config();
    // The operator's stated objective for this process (if any) frames the conversation.
    let objective = state
        .workspaces
        .get_process(&workspace, &process)
        .and_then(|p| p.config.objective);
    // The process's BPMN model (if any) unlocks the structural read_model/analyze_model tools.
    let model = state.workspaces.read_model(&workspace, &process);
    let key = chat::session_key(&workspace, &process);
    let sid = resolve_session(&state, &key, q.session.as_deref()).id;
    let cancel_key = chat_cancel_key(&key, &sid);
    // Load the prior transcript BEFORE the spawn_blocking (Vec<Msg> is Send); the DuckDB
    // connection inside the analysis is !Send, so the loop runs on a current-thread runtime.
    let session = state.chat.get(&key, &sid).unwrap_or_default();
    let prior = session.messages;
    let prior_stamps = session.stamps;
    // Resolve the persona (standing system prompt). It only takes effect on the first turn;
    // record the resolved id on the session so the cockpit can show/lock it thereafter.
    let (persona_id, persona_system) = state.personas.resolve(req.persona_id.as_deref());
    if prior.is_empty() {
        state.chat.set_persona(&key, &sid, &persona_id);
    }
    let user_ts = chat_now_ms();
    let pairs = match build_pair_stages(&state, &req) {
        Ok(p) => p,
        Err(e) => return unprocessable(e),
    };
    let message = req.message;
    // Register a wrap-up flag for this in-flight turn so `POST .../chat/wrapup` can ask the
    // agent to report early. Cleared in all exit paths below.
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    if let Ok(mut m) = state.chat_cancels.lock() {
        m.insert(cancel_key.clone(), cancel.clone());
    }
    let task = {
        let cancel = cancel.clone();
        let dbg = std::sync::Arc::new(std::sync::Mutex::new(Vec::<serde_json::Value>::new()));
        let dbg_capture = dbg.clone();
        let handle = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("runtime: {e}"))?;
            let mut sink = move |ev: agent::AgentEvent| {
                if let agent::AgentEvent::Request { round, body } = ev {
                    if let Ok(mut d) = dbg_capture.lock() {
                        let ts = chat_now_ms();
                        d.push(serde_json::json!({ "round": round, "ts": ts, "body": body }));
                    }
                }
            };
            rt.block_on(investigate::run_chat_turn(
                &src,
                cfg,
                py,
                limit,
                max_rounds,
                allow_python,
                objective.as_deref(),
                Some(&persona_system),
                model,
                Some(&cancel),
                None,
                &mut sink,
                &pairs,
                prior,
                &message,
                None,
            ))
        })
        .await;
        // Stash this turn's exact request payloads for the session's Debug tab.
        let bodies = dbg.lock().map(|d| d.clone()).unwrap_or_default();
        if let Ok(mut m) = state.chat_debug.lock() {
            m.insert(cancel_key.clone(), bodies);
        }
        handle
    };
    if let Ok(mut m) = state.chat_cancels.lock() {
        m.remove(&cancel_key);
    }
    match task {
        Ok(Ok(result)) => {
            let stamps =
                chat::extend_stamps(&result.messages, prior_stamps, user_ts, chat_now_ms());
            state
                .chat
                .save(&key, &sid, result.messages.clone(), stamps.clone());
            Json(serde_json::json!({
                "sessionId": sid,
                "answer": result.answer,
                "rounds": result.rounds,
                "dataset": result.dataset,
                "turns": chat::render_view_stamped(&result.messages, &stamps),
            }))
            .into_response()
        }
        Ok(Err(e)) => bad_gateway(e),
        Err(e) => bad_gateway(format!("chat task failed: {e}")),
    }
}

/// `POST .../chat/stream` — like [`cockpit_chat_send`], but streams the turn as Server-Sent
/// Events so the cockpit can show the droid's thinking, tool calls, and answer live as they are
/// produced. Event payloads are JSON: `{type:"round",n}`, `{type:"reasoning",text}`,
/// `{type:"answer",text}`, `{type:"tool",tool,arguments}`, `{type:"toolResult",tool,result}`,
/// and a terminal `{type:"done",answer,rounds,dataset,turns}` or `{type:"error",message}`.
async fn cockpit_chat_stream(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Query(q): Query<SessionQuery>,
    Json(req): Json<ChatSendRequest>,
) -> impl IntoResponse {
    if req.message.trim().is_empty() {
        return unprocessable("message must not be empty".to_string());
    }
    let cfg = resolve_llm(&state, req.llm.as_ref());
    if !cfg.is_ready() {
        return unprocessable(
            "no LLM model configured; set it in the console settings (cog, lower-left) or \
             via PROCESSOS_LLM_MODEL (and PROCESSOS_LLM_BASE_URL / PROCESSOS_LLM_PROVIDER \
             as needed), or pass an `llm` object with at least `model` in the request body"
                .to_string(),
        );
    }
    let src = match state.workspaces.resolve_source(&workspace, &process) {
        Ok(s) => s,
        Err(e) => return unprocessable(e),
    };
    let limit = req.limit.unwrap_or(100_000).clamp(1, 1_000_000);
    let max_rounds = req.max_rounds.unwrap_or(12).clamp(1, 40);
    let allow_python = req.allow_python;
    let py = state.settings.snapshot().py_config();
    let objective = state
        .workspaces
        .get_process(&workspace, &process)
        .and_then(|p| p.config.objective);
    let model = state.workspaces.read_model(&workspace, &process);
    let key = chat::session_key(&workspace, &process);
    let sid = resolve_session(&state, &key, q.session.as_deref()).id;
    let cancel_key = chat_cancel_key(&key, &sid);
    let session = state.chat.get(&key, &sid).unwrap_or_default();
    let prior = session.messages;
    let prior_stamps = session.stamps;
    // Resolve the persona; it only takes effect on the first turn. Record it on the session.
    let (persona_id, persona_system) = state.personas.resolve(req.persona_id.as_deref());
    if prior.is_empty() {
        state.chat.set_persona(&key, &sid, &persona_id);
    }
    let user_ts = chat_now_ms();
    let pairs = match build_pair_stages(&state, &req) {
        Ok(p) => p,
        Err(e) => return unprocessable(e),
    };
    let message = req.message;
    // Register a wrap-up flag so `POST .../chat/wrapup` can ask this in-flight turn to report early.
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    if let Ok(mut m) = state.chat_cancels.lock() {
        m.insert(cancel_key.clone(), cancel.clone());
    }
    // Register a steering queue so `POST .../chat/steer` can redirect this in-flight turn.
    let steer = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    if let Ok(mut m) = state.chat_steers.lock() {
        m.insert(cancel_key.clone(), steer.clone());
    }
    // Register a cell for this turn's live completion id (`chatcmpl-…`), set by the streaming sink
    // once the model starts replying, so wrap-up / the monitor can end its thinking mid-generation.
    let cmpl_id = Arc::new(std::sync::Mutex::new(None::<String>));
    if let Ok(mut m) = state.chat_cmpl_ids.lock() {
        m.insert(cancel_key.clone(), cmpl_id.clone());
    }

    // Register a live event buffer so the streaming response — and any later reattach after a
    // page reload — can replay this turn's events. The agent task pushes here regardless of
    // whether the original fetch is still connected.
    let live = LiveTurn::new(message.clone());
    if let Ok(mut m) = state.chat_live.lock() {
        m.insert(cancel_key.clone(), live.clone());
    }

    // Loop monitor (off by default): a second model that watches this turn's transcript and
    // steers the primary when it circles. It runs out-of-band on the main runtime and writes into
    // the same steer/cancel channels as the operator's manual controls; `monitor_done` lets the
    // primary signal completion so the watcher stops. Resolved before the blocking task so a
    // misconfiguration just means "no monitor", never a failed turn.
    let monitor_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    if let Some((mon_cfg, mon_persona)) = resolve_monitor(&state, req.monitor.as_ref()) {
        tokio::spawn(run_loop_monitor(
            mon_cfg,
            mon_persona,
            state.chat.clone(),
            key.clone(),
            sid.clone(),
            steer.clone(),
            cancel.clone(),
            live.clone(),
            monitor_done.clone(),
            // The monitor controls the PRIMARY turn: it must target the primary endpoint + the
            // primary's live completion id for the reasoning-control "end thinking" call.
            cfg.base_url.clone(),
            cmpl_id.clone(),
        ));
    }

    // The agent loop runs on a blocking thread (DuckDB is !Send) and pushes events into the live
    // buffer; the SSE response(s) follow that buffer on the main runtime.
    let task_state = state.clone();
    let task_key = key.clone();
    let task_sid = sid.clone();
    let task_cancel_key = cancel_key.clone();
    let task_live = live.clone();
    let task_monitor_done = monitor_done.clone();
    let task_cmpl_id = cmpl_id.clone();
    tokio::task::spawn_blocking(move || {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                task_live.emit(
                    serde_json::json!({ "type": "error", "message": format!("runtime: {e}") }),
                );
                if let Ok(mut m) = task_state.chat_live.lock() {
                    m.remove(&task_cancel_key);
                }
                return;
            }
        };
        let live_for_sink = task_live.clone();
        // Incrementally PERSIST the running transcript (throttled to ~2s) so a turn that times
        // out, errors, or is interrupted still leaves a debuggable transcript on disk — instead
        // of the old behaviour where nothing was saved unless the whole turn completed cleanly.
        let cp_chat = task_state.chat.clone();
        let cp_key = task_key.clone();
        let cp_sid = task_sid.clone();
        let cp_prior_stamps = prior_stamps.clone();
        let cp_user_ts = user_ts;
        // Start "stale" so the first checkpoint (which carries the operator's message) saves at once.
        let mut last_save = std::time::Instant::now() - std::time::Duration::from_secs(3600);
        let mut checkpoint = move |msgs: &[agent::Msg]| {
            let now = std::time::Instant::now();
            if now.duration_since(last_save) < std::time::Duration::from_secs(2) {
                return;
            }
            last_save = now;
            let stamps =
                chat::extend_stamps(msgs, cp_prior_stamps.clone(), cp_user_ts, chat_now_ms());
            cp_chat.save(&cp_key, &cp_sid, msgs.to_vec(), stamps);
        };
        // Accumulate this turn's exact request payloads for the session's Debug tab while also
        // forwarding each over the wire so the tab can update live.
        let dbg = std::sync::Arc::new(std::sync::Mutex::new(Vec::<serde_json::Value>::new()));
        let dbg_for_sink = dbg.clone();
        let cmpl_for_sink = task_cmpl_id.clone();
        let mut sink = move |ev: agent::AgentEvent| {
            let v = match ev {
                agent::AgentEvent::Round(n) => serde_json::json!({ "type": "round", "n": n }),
                agent::AgentEvent::Completion { id } => {
                    // Stash the live completion id so wrap-up / the monitor can target this turn
                    // with the reasoning-control endpoint. Not forwarded to the cockpit (internal).
                    if let Ok(mut c) = cmpl_for_sink.lock() {
                        *c = Some(id);
                    }
                    return;
                }
                agent::AgentEvent::Request { round, body } => {
                    let ts = chat_now_ms();
                    if let Ok(mut d) = dbg_for_sink.lock() {
                        d.push(
                            serde_json::json!({ "round": round, "ts": ts, "body": body.clone() }),
                        );
                    }
                    serde_json::json!({ "type": "request", "round": round, "ts": ts, "body": body })
                }
                agent::AgentEvent::Reasoning(t) => {
                    serde_json::json!({ "type": "reasoning", "text": t })
                }
                agent::AgentEvent::Answer(t) => serde_json::json!({ "type": "answer", "text": t }),
                agent::AgentEvent::ToolCall { tool, arguments } => {
                    serde_json::json!({ "type": "tool", "tool": tool, "arguments": arguments })
                }
                agent::AgentEvent::ToolResult { tool, result } => {
                    serde_json::json!({ "type": "toolResult", "tool": tool, "result": result })
                }
                agent::AgentEvent::Agent { id, name, role } => {
                    serde_json::json!({ "type": "agent", "id": id, "name": name, "role": role })
                }
            };
            live_for_sink.emit(v);
        };
        let result = rt.block_on(investigate::run_chat_turn(
            &src,
            cfg,
            py,
            limit,
            max_rounds,
            allow_python,
            objective.as_deref(),
            Some(&persona_system),
            model,
            Some(&cancel),
            Some(&steer),
            &mut sink,
            &pairs,
            prior,
            &message,
            Some(&mut checkpoint),
        ));
        // Signal the loop monitor (if any) that the primary has finished, so it stops polling.
        task_monitor_done.store(true, std::sync::atomic::Ordering::Relaxed);
        // Persist the captured payloads regardless of outcome (a failed turn still sent a request).
        let bodies = dbg.lock().map(|d| d.clone()).unwrap_or_default();
        if let Ok(mut m) = task_state.chat_debug.lock() {
            m.insert(task_cancel_key.clone(), bodies);
        }
        match result {
            Ok(r) => {
                let stamps = chat::extend_stamps(&r.messages, prior_stamps, user_ts, chat_now_ms());
                task_state
                    .chat
                    .save(&task_key, &task_sid, r.messages.clone(), stamps.clone());
                task_live.emit(serde_json::json!({
                    "type": "done",
                    "sessionId": task_sid,
                    "answer": r.answer,
                    "rounds": r.rounds,
                    "dataset": r.dataset,
                    "turns": chat::render_view_stamped(&r.messages, &stamps),
                }));
            }
            Err(e) => {
                task_live.emit(serde_json::json!({ "type": "error", "message": e }));
            }
        }
        if let Ok(mut m) = task_state.chat_cancels.lock() {
            m.remove(&task_cancel_key);
        }
        if let Ok(mut m) = task_state.chat_steers.lock() {
            m.remove(&task_cancel_key);
        }
        if let Ok(mut m) = task_state.chat_cmpl_ids.lock() {
            m.remove(&task_cancel_key);
        }
        // Drop the live buffer last: any consumers still attached hold their own Arc clones and
        // already saw the terminal event, so this only stops *new* reattachers (which then fall
        // back to the now-complete persisted transcript).
        if let Ok(mut m) = task_state.chat_live.lock() {
            m.remove(&task_cancel_key);
        }
    });

    sse_from_live(live).into_response()
}

/// `POST .../chat/reset` — forget a conversation. With `?session=`, clears just that
/// session's transcript (keeping its name); without, forgets every session for the dataset.
async fn cockpit_chat_reset(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Query(q): Query<SessionQuery>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    match q.session {
        Some(sid) if state.chat.get(&key, &sid).is_some() => {
            state.chat.save(&key, &sid, Vec::new(), Vec::new());
        }
        _ => state.chat.clear(&key),
    }
    StatusCode::NO_CONTENT.into_response()
}

/// `POST .../chat/wrapup` — ask an in-flight chat turn to stop investigating and report its
/// findings so far. No-op (404) when no turn is running for this session.
async fn cockpit_chat_wrapup(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Query(q): Query<SessionQuery>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let sid = resolve_session(&state, &key, q.session.as_deref()).id;
    let cancel_key = chat_cancel_key(&key, &sid);
    let flagged = state
        .chat_cancels
        .lock()
        .ok()
        .and_then(|m| m.get(&cancel_key).cloned());
    match flagged {
        Some(flag) => {
            flag.store(true, std::sync::atomic::Ordering::Relaxed);
            // Prefer a mid-thinking halt via the reasoning-control surface: end the live turn's
            // reasoning now (it answers immediately) rather than only stopping at the next round
            // boundary. Falls back to the cancel flag above when there's no live completion id yet
            // or the endpoint is an older build without the surface.
            let cmpl = state
                .chat_cmpl_ids
                .lock()
                .ok()
                .and_then(|m| m.get(&cancel_key).cloned())
                .and_then(|c| c.lock().ok().and_then(|v| v.clone()));
            if let Some(id) = cmpl {
                let cfg = resolve_llm(&state, None);
                if cfg.is_ready() {
                    let _ = reasoning::end_reasoning(&cfg.base_url, &id).await;
                }
            }
            StatusCode::ACCEPTED.into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no investigation is running for this dataset" })),
        )
            .into_response(),
    }
}

/// Body for `POST .../chat/steer` — the operator's mid-investigation steering instruction.
#[derive(Debug, Deserialize)]
struct SteerRequest {
    message: String,
}

/// `POST .../chat/steer` — send a steering instruction to an in-flight chat turn. The message
/// is queued and injected as a user turn at the agent loop's next round boundary, redirecting
/// the investigation without restarting it. No-op (404) when no turn is running for this session.
async fn cockpit_chat_steer(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Query(q): Query<SessionQuery>,
    Json(req): Json<SteerRequest>,
) -> impl IntoResponse {
    if req.message.trim().is_empty() {
        return unprocessable("message must not be empty".to_string());
    }
    let key = chat::session_key(&workspace, &process);
    let sid = resolve_session(&state, &key, q.session.as_deref()).id;
    let cancel_key = chat_cancel_key(&key, &sid);
    let queue = state
        .chat_steers
        .lock()
        .ok()
        .and_then(|m| m.get(&cancel_key).cloned());
    match queue {
        Some(q) => {
            if let Ok(mut v) = q.lock() {
                v.push(req.message);
            }
            StatusCode::ACCEPTED.into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no investigation is running for this dataset" })),
        )
            .into_response(),
    }
}
fn chat_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `GET /api/python/status` — whether a Python interpreter is configured and whether the
/// optional data-science stack (pandas / numpy / duckdb / scipy) is importable in it, so the
/// console can label the Python escape-hatch control appropriately.
async fn python_status(State(state): State<AppState>) -> impl IntoResponse {
    let snap = state.settings.snapshot();
    // "Configured" = the operator set an interpreter explicitly (settings or env), as opposed
    // to falling back to the bare `python3` default.
    let configured = snap
        .python_bin
        .as_deref()
        .is_some_and(|s| !s.trim().is_empty())
        || std::env::var_os("PROCESSOS_PYTHON").is_some();
    let py = snap.py_config();
    let interpreter = py.python_bin.clone();
    let probe = tokio::task::spawn_blocking(move || pyrunner::probe_data_science(&py))
        .await
        .unwrap_or_default();
    Json(serde_json::json!({
        "configured": configured,
        "interpreter": interpreter,
        "interpreterRuns": probe.runs,
        "dataScience": probe.data_science,
        "missing": probe.missing,
    }))
    .into_response()
}

/// `GET /api/chat-prompts` — list reusable compose-box message templates.
async fn chat_prompts_list(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.chat_prompts.list()).into_response()
}

/// `POST /api/chat-prompts` — author or update a chat prompt (persisted to the config dir).
async fn chat_prompts_upsert(
    State(state): State<AppState>,
    Json(prompt): Json<chat_prompts::ChatPrompt>,
) -> impl IntoResponse {
    match state.chat_prompts.upsert(prompt) {
        Ok(p) => Json(p).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `DELETE /api/chat-prompts/{id}` — delete a non-built-in chat prompt.
async fn chat_prompts_delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.chat_prompts.delete(&id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `GET /api/personas` — list the selectable chat personas (standing system prompts).
async fn personas_list(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.personas.list()).into_response()
}

/// `POST /api/personas` — author or update a persona (persisted to the config dir).
async fn personas_upsert(
    State(state): State<AppState>,
    Json(persona): Json<personas::Persona>,
) -> impl IntoResponse {
    match state.personas.upsert(persona) {
        Ok(p) => Json(p).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `DELETE /api/personas/{id}` — delete a non-built-in persona.
async fn personas_delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.personas.delete(&id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => unprocessable(e),
    }
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
    let ranked = tokio::task::spawn_blocking(move || run_scenario(&scenario)).await;
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
    let cfg = resolve_llm(&state, req.llm.as_ref());
    if !cfg.is_ready() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "no LLM model configured; set it in the console settings (cog, \
                          lower-left) or via PROCESSOS_LLM_MODEL (and PROCESSOS_LLM_BASE_URL \
                          / PROCESSOS_LLM_PROVIDER as needed), or pass an `llm` object with \
                          at least `model` in the request body"
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
    match run_hypothesis(
        &scenario,
        &cfg,
        req.include_baked,
        &req.measured,
        &system_prompt,
    )
    .await
    {
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
        _ => state.target.clone(),
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
        _ => state.target.clone(),
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
    let process_id = req.process_id.clone().unwrap_or_else(|| defs[0].id.clone());

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
                *skip_reasons
                    .entry("trace fetch failed".to_string())
                    .or_insert(0) += 1;
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
    /// Generative mocks for new workers this candidate introduces. Each entry is
    /// either a static output object (deterministic worker) or a `{ "outcomes":
    /// [{ "weight", "output" }] }` distribution (non-deterministic worker), so a
    /// variant adding a worker — including one that drives a downstream split —
    /// can still be scored. Parsed via `harness::parse_mock_workers`.
    #[serde(default)]
    mock_workers: serde_json::Value,
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
        _ => state.target.clone(),
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
            mock_workers: crate::harness::parse_mock_workers(&c.mock_workers),
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
    /// The pilot's free-text steer for this round (§10) — threaded into the user
    /// prompt so the droid conditions its redesigns on the human's intent.
    #[serde(default)]
    pilot_note: Option<String>,
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
    // Resolve LLM config (env + persisted settings + per-request override); a model
    // name is required.
    let cfg = resolve_llm(&state, req.llm.as_ref());
    if !cfg.is_ready() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "no LLM model configured; set it in the console settings (cog, \
                          lower-left) or via PROCESSOS_LLM_MODEL (and PROCESSOS_LLM_BASE_URL \
                          / PROCESSOS_LLM_PROVIDER as needed), or pass an `llm` object with \
                          at least `model`"
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
        _ => state.target.clone(),
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
    let user_prompt = build_evolve_prompt(&signal, &req.baseline_model, req.pilot_note.as_deref());
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

/// A pilot fork submitted by the operator.
#[derive(Debug, Deserialize)]
struct PilotUpdate {
    /// The forked pilot BPMN XML.
    xml: String,
}

/// `GET /api/pilot` — the current pilot process (§10 surface a): its source
/// (`default`/`forked`), declared process ids, and XML.
async fn pilot_get(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.pilot.doc())
}

/// `PUT /api/pilot` — author a fork of the pilot process. Validates the BPMN, persists
/// it durably, then **hot-redeploys it to the own engine** so the next experiment runs
/// the operator's choreography. 422 on invalid BPMN (nothing changes); 502 if the
/// engine deploy fails after a successful save (the fork is kept). The response carries
/// the new doc plus a `warning` when the cockpit's expected process id is absent.
async fn pilot_put(
    State(state): State<AppState>,
    Json(body): Json<PilotUpdate>,
) -> impl IntoResponse {
    let doc = match state.pilot.save(&body.xml) {
        Ok(doc) => doc,
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };
    redeploy_pilot(&state, doc).await
}

/// `POST /api/pilot/reset` — restore the built-in default pilot and hot-redeploy it.
async fn pilot_reset(State(state): State<AppState>) -> impl IntoResponse {
    let doc = state.pilot.reset();
    redeploy_pilot(&state, doc).await
}

/// Deploy the (already-persisted) pilot doc to the own engine and shape the response.
async fn redeploy_pilot(state: &AppState, doc: pilot::PilotDoc) -> axum::response::Response {
    let warning = (!doc.process_ids.iter().any(|id| id == cockpit::PILOT_PROCESS_ID)).then(|| {
        format!(
            "pilot declares {:?}, not '{}' — the cockpit creates experiments on '{}', so they will fail until the process id matches",
            doc.process_ids, cockpit::PILOT_PROCESS_ID, cockpit::PILOT_PROCESS_ID
        )
    });
    match state.own.deploy_bpmn(doc.deploy_filename, &doc.xml).await {
        Ok(_) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "pilot": doc,
                "deployed": true,
                "warning": warning,
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({
                "pilot": doc,
                "deployed": false,
                "error": format!("saved, but redeploy to own engine failed: {e}"),
                "warning": warning,
            })),
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
    match build_baseline(&state.target, q.process_id.as_deref(), limit, sample).await {
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
    match build_cluster_summary(&state.target, q.process_id.as_deref(), limit, sample).await {
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
<title>Nano ProcessOS — Live instance</title>
<style>
  :root { color-scheme: dark; }
  body { margin: 0; font: 14px/1.5 system-ui, sans-serif; background: #0a0a0b; color: #e4e4e7; }
  a { color: inherit; text-decoration: none; }
  .app { display: grid; grid-template-columns: 220px 1fr; min-height: 100vh; }
  .rail { border-right: 1px solid #1f1f23; padding: 18px 14px; background: #0c0c0e; }
  .rail .brand { display: block; font-weight: 700; font-size: 16px; letter-spacing: .02em; margin-bottom: 2px; }
  .rail .brand .dot { color: #a5b4fc; }
  .rail .tag { color: #71717a; font-size: 11px; margin-bottom: 20px; }
  .rail nav a { display: block; padding: 7px 10px; border-radius: 7px; color: #a1a1aa; font-weight: 500; margin-bottom: 2px; }
  .rail nav a:hover { background: #18181b; color: #e4e4e7; }
  .rail nav a.active { background: #1d1d22; color: #c7d2fe; }
  .content { min-width: 0; }
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
<div class="app">
  <aside class="rail">
    <a class="brand" href="/">Nano Process<span class="dot">OS</span></a>
    <div class="tag">live instance</div>
    <nav>
      <a href="/console" class="active">Console</a>
      <a href="/workspace">Workspaces</a>
      <a href="/cockpit">Cockpit</a>
      <a href="/harness">Harness</a>
      <a href="/features">Features</a>
    </nav>
  </aside>
  <div class="content">
    <header>
      <h1>Live instance</h1>
      <span class="sub">&middot; <span id="nano"></span></span>
      <button onclick="load()" style="margin-left:auto">Refresh</button>
    </header>
    <main id="out">Loading…</main>
  </div>
</div>
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
<script src="/assets/settings.js"></script>
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
<title>Nano ProcessOS — Optimization Harness</title>
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
  <h1><a href="/" style="color:inherit;text-decoration:none">Nano ProcessOS</a> — Optimization Harness</h1>
  <div class="sub">SimRunner over the bundled example scenario (worker-swap transform space). The same loop runs in production against live Nano traces.</div>
  <div class="sub" style="margin-top:8px"><a href="/" style="color:#a5b4fc;text-decoration:none">Home</a> · <a href="/workspace" style="color:#a5b4fc;text-decoration:none">Workspaces</a> · <a href="/features" style="color:#a5b4fc;text-decoration:none">Features</a> · <a href="/console" style="color:#a5b4fc;text-decoration:none">Console</a></div>
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

#[cfg(test)]
mod config_tests {
    use super::resolve_nano_urls;

    #[test]
    fn defaults_both_roles_to_the_local_gateway() {
        let (t, o) = resolve_nano_urls(None, None, None);
        assert_eq!(t, "http://localhost:8080");
        assert_eq!(o, "http://localhost:8080");
    }

    #[test]
    fn nano_base_url_aliases_both_roles() {
        let (t, o) = resolve_nano_urls(Some("http://shared:9".into()), None, None);
        assert_eq!(t, "http://shared:9");
        assert_eq!(o, "http://shared:9");
    }

    #[test]
    fn role_specific_urls_override_the_alias_independently() {
        let (t, o) = resolve_nano_urls(
            Some("http://base:8080".into()),
            Some("http://client-prod:8080".into()),
            Some("http://processos-own:8081".into()),
        );
        assert_eq!(t, "http://client-prod:8080", "target = client's production");
        assert_eq!(o, "http://processos-own:8081", "own = ProcessOS's engine");
    }

    #[test]
    fn a_single_role_override_leaves_the_other_on_the_alias() {
        let (t, o) = resolve_nano_urls(
            Some("http://base:8080".into()),
            None,
            Some("http://own:8081".into()),
        );
        assert_eq!(t, "http://base:8080");
        assert_eq!(o, "http://own:8081");
    }
}

#[cfg(test)]
mod sidecar_phase_tests {
    use super::download_in_progress;
    use std::fs;

    #[test]
    fn detects_an_in_progress_hf_download_and_reports_its_size() {
        let dir = std::env::temp_dir().join(format!("se-dl-{}", std::process::id()));
        let blobs = dir
            .join("models--unsloth--Qwen3-Coder-30B-A3B-Instruct-GGUF")
            .join("blobs");
        fs::create_dir_all(&blobs).unwrap();
        fs::write(blobs.join("abc123.downloadInProgress"), vec![0u8; 4096]).unwrap();
        // A completed blob (no suffix) must NOT be mistaken for a download.
        fs::write(blobs.join("def456"), vec![0u8; 8192]).unwrap();

        let model = "unsloth/Qwen3-Coder-30B-A3B-Instruct-GGUF:UD-Q4_K_XL";
        assert_eq!(download_in_progress(&dir, model), Some(4096));

        // Once the partial file is gone, nothing is downloading.
        fs::remove_file(blobs.join("abc123.downloadInProgress")).unwrap();
        assert_eq!(download_in_progress(&dir, model), None);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn no_download_for_an_unknown_model_dir() {
        let dir = std::env::temp_dir().join("se-dl-absent");
        assert_eq!(download_in_progress(&dir, "org/repo:tag"), None);
    }
}
