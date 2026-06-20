//! Embedded web console for the self-contained single-node distribution.
//!
//! Feature-gated behind `console`. Serves a single-page app (the built Vite/React
//! bundle under `../console/dist`) at `/console` and a small JSON API under
//! `/console/api/*` that reads cluster/topology state straight off [`ServerImpl`].
//!
//! Design constraints (deliberate):
//! - This namespace is **separate** from the generated Camunda REST surface. The
//!   console API is nanobpmn-specific and MUST NOT leak into `spec/`, `generated/`,
//!   or `spec-patches/`.
//! - In debug builds `rust-embed` reads assets from disk (live frontend reload);
//!   release builds bake them into the binary for a single-file distribution.
//! - Everything here is additive and feature-gated, so the default gateway build
//!   is unaffected.

use axum::{
    Router,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    response::{
        IntoResponse, Json, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::get,
};
use futures_util::stream::{Stream, unfold};
use nanobpmn_engine_core::bpmn::parse_bpmn;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::broadcast;

use crate::ServerImpl;

pub mod workers;
pub mod workspace;

/// The built frontend bundle. Path is relative to this source file
/// (`server/src/console/`), so it points at the repo-level `console/dist`.
#[derive(RustEmbed)]
#[folder = "../console/dist"]
struct Assets;

/// The standalone marketing landing page (self-contained: inline canvas particle
/// effect, no external assets), served at `/`.
const LANDING_HTML: &str = include_str!("landing.html");

/// Mounts the console SPA and its JSON API onto the gateway.
pub fn router(server: ServerImpl) -> Router {
    Router::new()
        .route("/", get(landing))
        .route("/swagger", get(swagger_index))
        .route("/swagger/", get(swagger_index))
        .route("/swagger/{*path}", get(swagger_asset))
        .route("/console/api/topology", get(topology))
        .route("/console/api/instances", get(instances))
        .route("/console/api/instances/{key}", get(instance_detail))
        .route("/console/api/stream", get(stream))
        .route(
            "/console/api/models",
            get(models).post(model_create),
        )
        .route(
            "/console/api/models/{name}",
            get(model_get).put(model_save).delete(model_delete),
        )
        .route("/console/api/workers", get(workers_list).post(worker_create))
        .route(
            "/console/api/workers/{name}",
            get(worker_get).delete(worker_delete),
        )
        .route(
            "/console/api/workers/{name}/file",
            get(worker_file_get)
                .put(worker_file_save)
                .post(worker_file_create)
                .delete(worker_file_delete),
        )
        .route("/console/api/workers/{name}/start", axum::routing::post(worker_start))
        .route("/console/api/workers/{name}/stop", axum::routing::post(worker_stop))
        .route("/console/api/workers/{name}/logs", get(worker_logs))
        .route("/console", get(spa_index))
        .route("/console/", get(spa_index))
        .route("/console/{*path}", get(spa_asset))
        .with_state(server)
}

// The `*path` catch-all must not swallow `/console/api/*`. axum's matchit router
// ranks literal segments above wildcards, so the API routes above always win;
// the catch-all only handles SPA asset/deep-link requests. The list of
// `/console/api/...` routes is registered explicitly to keep that guarantee
// obvious rather than relying on registration order.

// ---------------------------------------------------------------------------
// Static asset serving (SPA)
// ---------------------------------------------------------------------------

/// Serves `index.html` for the SPA entry points (`/console`, `/console/`).
async fn spa_index() -> Response {
    serve_embedded("index.html")
}

/// Serves a built asset by path under `/console/`. Unknown paths that are not
/// API routes fall back to `index.html` so client-side routing (deep links like
/// `/console/explorer`) works on a full-page load.
async fn spa_asset(axum::extract::Path(path): axum::extract::Path<String>) -> Response {
    let path = path.trim_start_matches('/');
    if Assets::get(path).is_some() {
        serve_embedded(path)
    } else {
        // SPA fallback: let the client router resolve the route.
        serve_embedded("index.html")
    }
}

/// Looks an asset up in the embedded bundle and returns it with a guessed
/// content type. Returns a helpful 404 when the frontend has not been built.
fn serve_embedded(path: &str) -> Response {
    match Assets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            (
                [(header::CONTENT_TYPE, mime.as_ref().to_string())],
                content.data.into_owned(),
            )
                .into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            "console assets not found — build the frontend first (`make console` or \
             `cd console && npm install && npm run build`)",
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Landing page + Swagger UI (root-level, console feature only)
// ---------------------------------------------------------------------------

/// Serves the standalone marketing landing page at `/`.
async fn landing() -> Response {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        LANDING_HTML,
    )
        .into_response()
}

/// Serves the Swagger UI shell at `/swagger`.
async fn swagger_index() -> Response {
    serve_embedded("swagger/index.html")
}

/// Serves Swagger UI assets and the bundled OpenAPI spec under `/swagger/`. All
/// files (the UI assets and `openapi.json`) are built into the frontend bundle
/// under `dist/swagger/`.
async fn swagger_asset(axum::extract::Path(path): axum::extract::Path<String>) -> Response {
    let path = path.trim_start_matches('/');
    serve_embedded(&format!("swagger/{path}"))
}



#[derive(Serialize)]
struct TopologyDto {
    /// This gateway node's id.
    node_id: u32,
    num_nodes: u32,
    num_partitions: u64,
    replication_factor: u32,
    /// Whether per-partition Raft replication is active on this node.
    raft_enabled: bool,
    gateway_version: String,
    nodes: Vec<NodeDto>,
    partitions: Vec<PartitionDto>,
}

#[derive(Serialize)]
struct NodeDto {
    node_id: u32,
    /// `http://host:port` base URL (empty for self in a single-node cluster).
    address: String,
    is_self: bool,
}

#[derive(Serialize)]
struct PartitionDto {
    /// 1-based partition id (Camunda display convention).
    partition_id: u64,
    /// Nodes replicating this partition.
    replicas: Vec<u32>,
    /// The current serving leader: the live Raft leader when Raft is active,
    /// otherwise the static owner.
    leader: Option<u32>,
    /// Live Raft term for this partition's group (when Raft is active here).
    raft_term: Option<u64>,
}

/// `GET /console/api/topology` — the cluster/topology view's data source.
async fn topology(State(server): State<ServerImpl>) -> Json<TopologyDto> {
    let topology = server.engine.topology();
    let num_nodes = topology.num_nodes();
    let num_partitions = topology.num_partitions;
    let raft_on = crate::raft_enabled();

    let nodes: Vec<NodeDto> = (0..num_nodes)
        .map(|node| NodeDto {
            node_id: node,
            address: topology.peer_addr(node).unwrap_or("").to_string(),
            is_self: node == topology.node_id,
        })
        .collect();

    let partitions: Vec<PartitionDto> = (0..num_partitions)
        .map(|p| {
            // Prefer the live Raft leader/term when this node hosts the group;
            // fall back to the static topology leader otherwise.
            let raft_part = server.raft_registry().get(p);
            let (leader, term) = match raft_part {
                Some(part) => {
                    let m = part.raft.metrics().borrow().clone();
                    (m.current_leader.map(|id| id as u32), Some(m.current_term))
                }
                None => (Some(topology.leader_of(p)), None),
            };
            PartitionDto {
                partition_id: p + 1,
                replicas: topology.replicas_of(p),
                leader,
                raft_term: term,
            }
        })
        .collect();

    Json(TopologyDto {
        node_id: topology.node_id,
        num_nodes,
        num_partitions,
        replication_factor: topology.effective_rf(),
        raft_enabled: raft_on,
        gateway_version: env!("CARGO_PKG_VERSION").to_string(),
        nodes,
        partitions,
    })
}

// ---------------------------------------------------------------------------
// Process Instance Explorer API
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct InstanceDto {
    /// u64 engine key rendered as a string — keys exceed JS's safe integer range.
    key: String,
    process_id: String,
    process_definition_key: String,
    version: i32,
    /// `Active` | `Completed` | `Terminated`.
    state: String,
    start_date_ms: u64,
    has_incident: bool,
    business_id: Option<String>,
    tags: Vec<String>,
}

impl From<&crate::readstore::ProcessInstanceRow> for InstanceDto {
    fn from(r: &crate::readstore::ProcessInstanceRow) -> Self {
        InstanceDto {
            key: r.key.to_string(),
            process_id: r.process_id.clone(),
            process_definition_key: r.process_definition_key.clone(),
            version: r.version,
            state: format!("{:?}", r.state),
            start_date_ms: r.start_date_ms,
            has_incident: r.has_incident,
            business_id: r.business_id.clone(),
            tags: r.tags.clone(),
        }
    }
}

#[derive(Serialize)]
struct VariableDto {
    name: String,
    /// Serialized-JSON value string, mirroring Camunda's wire representation.
    value: String,
    scope_key: String,
}

#[derive(Serialize)]
struct JobDto {
    key: String,
    element_id: String,
    job_type: String,
    state: String,
    retries: i32,
    worker: Option<String>,
    deadline_ms: Option<u64>,
}

#[derive(Serialize)]
struct IncidentDto {
    key: String,
    element_id: String,
    kind: String,
    state: String,
    reason: String,
    created_at_ms: u64,
}

#[derive(Serialize)]
struct InstanceDetailDto {
    instance: InstanceDto,
    variables: Vec<VariableDto>,
    jobs: Vec<JobDto>,
    incidents: Vec<IncidentDto>,
}

/// `GET /console/api/instances` — process-instance list, newest first.
async fn instances(State(server): State<ServerImpl>) -> Json<Vec<InstanceDto>> {
    let mut rows = server.store.process_instances();
    // Newest first: most useful default ordering for an ops view.
    rows.sort_by(|a, b| b.start_date_ms.cmp(&a.start_date_ms));
    Json(rows.iter().map(InstanceDto::from).collect())
}

/// `GET /console/api/instances/{key}` — one instance with its variables, jobs,
/// and incidents. 404 when the key is malformed or unknown.
async fn instance_detail(
    State(server): State<ServerImpl>,
    Path(key): Path<String>,
) -> Response {
    let Ok(key) = key.parse::<u64>() else {
        return (StatusCode::NOT_FOUND, "invalid instance key").into_response();
    };
    let Some(row) = server.store.process_instance(key) else {
        return (StatusCode::NOT_FOUND, "no such process instance").into_response();
    };

    let variables: Vec<VariableDto> = server
        .store
        .instance_variables(key)
        .iter()
        .map(|v| VariableDto {
            name: v.name.clone(),
            value: v.value.clone(),
            scope_key: v.scope_key.to_string(),
        })
        .collect();

    let jobs: Vec<JobDto> = server
        .store
        .jobs()
        .iter()
        .filter(|j| j.instance_key == key)
        .map(|j| JobDto {
            key: j.key.to_string(),
            element_id: j.element_id.clone(),
            job_type: j.job_type.clone(),
            state: format!("{:?}", j.state),
            retries: j.retries,
            worker: j.worker.clone(),
            deadline_ms: j.deadline_ms,
        })
        .collect();

    let incidents: Vec<IncidentDto> = server
        .store
        .incidents()
        .iter()
        .filter(|i| i.instance_key == key)
        .map(|i| IncidentDto {
            key: i.key.to_string(),
            element_id: i.element_id.clone(),
            kind: format!("{:?}", i.kind),
            state: format!("{:?}", i.state),
            reason: i.reason.clone(),
            created_at_ms: i.created_at_ms,
        })
        .collect();

    Json(InstanceDetailDto {
        instance: InstanceDto::from(&row),
        variables,
        jobs,
        incidents,
    })
    .into_response()
}

/// `GET /console/api/stream` — Server-Sent Events feed for live updates.
///
/// Emits an `instances` event whenever the read model's exported position
/// advances (i.e. the projection consumed new events). The position is a cheap
/// change signal: the client reacts by refetching the list/detail it cares
/// about, so the server stays stateless about *what* changed. An initial event
/// fires immediately so the client syncs on connect; keep-alive comments keep
/// intermediaries from dropping an idle connection.
async fn stream(
    State(server): State<ServerImpl>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let store = server.store.clone();
    // `usize::MAX` as the seed guarantees the first poll differs, emitting an
    // immediate snapshot on connect.
    let s = unfold((store, usize::MAX), |(store, last)| async move {
        loop {
            let position = store.exported_position();
            if position != last {
                let active = store.active_instance_count();
                let data = format!(r#"{{"position":{position},"active":{active}}}"#);
                let event = Event::default().event("instances").data(data);
                return Some((Ok(event), (store, position)));
            }
            tokio::time::sleep(Duration::from_millis(750)).await;
        }
    });
    Sse::new(s).keep_alive(KeepAlive::default())
}

// ---------------------------------------------------------------------------
// Modeler API — workspace-backed BPMN model library
// ---------------------------------------------------------------------------
//
// Models live on disk in the console workspace (see `workspace`), which is the
// authoring source of truth and is separate from the engine data dir. The engine
// holds *deployed* definitions (with their verbatim XML); the console reconciles
// the two. Deploy/pull/duplicate are intentionally **not** endpoints here: the
// frontend deploys via the standard `POST /v2/deployments`, pulls a deployed
// model via `GET /v2/process-definitions/{key}/xml`, and duplicates client-side
// (clone + rename the process id in bpmn-js, then save as a new model). This API
// is therefore pure workspace file CRUD plus a computed deploy status.

/// `not_deployed` (no deployed definition for the model's primary process id),
/// `in_sync` (deployed XML is byte-for-byte the file), `modified` (a definition
/// is deployed but differs), or `unparsable` (the file is not valid BPMN).
fn deploy_status_of(server: &ServerImpl, xml: &str) -> ModelStatus {
    let process_ids: Vec<String> = match parse_bpmn(xml) {
        Ok(defs) => defs.iter().map(|d| d.id.clone()).collect(),
        Err(_) => {
            return ModelStatus {
                process_ids: Vec::new(),
                deploy_status: "unparsable".into(),
                deployed_version: None,
                deployed_key: None,
            };
        }
    };
    // Status is reported against the file's primary (first) process id; a
    // multi-process resource is rare in the modeler.
    let primary = process_ids.first().cloned();
    let deployed = primary
        .as_ref()
        .and_then(|id| {
            server
                .store
                .process_definitions()
                .into_iter()
                .find(|d| &d.process_id == id)
        });
    let (deploy_status, deployed_version, deployed_key) = match deployed {
        None => ("not_deployed", None, None),
        Some(row) => {
            let deployed_xml = server.store.process_definition_xml(row.key).unwrap_or_default();
            let status = if deployed_xml == xml { "in_sync" } else { "modified" };
            (status, Some(row.version), Some(row.key.to_string()))
        }
    };
    ModelStatus {
        process_ids,
        deploy_status: deploy_status.into(),
        deployed_version,
        deployed_key,
    }
}

struct ModelStatus {
    process_ids: Vec<String>,
    deploy_status: String,
    deployed_version: Option<i32>,
    deployed_key: Option<String>,
}

#[derive(Serialize)]
struct ModelSummaryDto {
    name: String,
    process_ids: Vec<String>,
    deploy_status: String,
    deployed_version: Option<i32>,
    deployed_key: Option<String>,
    updated_at_ms: u64,
    size: u64,
}

#[derive(Serialize)]
struct ModelDto {
    name: String,
    xml: String,
    process_ids: Vec<String>,
    deploy_status: String,
    deployed_version: Option<i32>,
    deployed_key: Option<String>,
}

#[derive(Deserialize)]
struct CreateModelBody {
    name: String,
    /// Initial BPMN XML; the frontend supplies a blank diagram from bpmn-js.
    xml: String,
}

/// `GET /console/api/models` — the model library, with each model's deploy
/// status relative to the engine. Sorted by name.
async fn models(State(server): State<ServerImpl>) -> Response {
    let names = match workspace::list_model_names() {
        Ok(n) => n,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("could not read workspace: {e}"),
            )
                .into_response();
        }
    };
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let Some(path) = workspace::model_path(&name) else {
            continue;
        };
        let xml = std::fs::read_to_string(&path).unwrap_or_default();
        let (updated_at_ms, size) = workspace::file_meta(&path);
        let status = deploy_status_of(&server, &xml);
        out.push(ModelSummaryDto {
            name,
            process_ids: status.process_ids,
            deploy_status: status.deploy_status,
            deployed_version: status.deployed_version,
            deployed_key: status.deployed_key,
            updated_at_ms,
            size,
        });
    }
    Json(out).into_response()
}

/// `GET /console/api/models/{name}` — one model's XML and deploy status.
async fn model_get(State(server): State<ServerImpl>, Path(name): Path<String>) -> Response {
    let Some(path) = workspace::model_path(&name) else {
        return (StatusCode::BAD_REQUEST, "invalid model name").into_response();
    };
    let xml = match std::fs::read_to_string(&path) {
        Ok(x) => x,
        Err(_) => return (StatusCode::NOT_FOUND, "no such model").into_response(),
    };
    let status = deploy_status_of(&server, &xml);
    Json(ModelDto {
        name,
        xml,
        process_ids: status.process_ids,
        deploy_status: status.deploy_status,
        deployed_version: status.deployed_version,
        deployed_key: status.deployed_key,
    })
    .into_response()
}

/// `PUT /console/api/models/{name}` — overwrite (save) a model's XML. The body
/// is the raw BPMN XML. The model must already exist (use POST to create).
async fn model_save(
    State(server): State<ServerImpl>,
    Path(name): Path<String>,
    xml: String,
) -> Response {
    let Some(path) = workspace::model_path(&name) else {
        return (StatusCode::BAD_REQUEST, "invalid model name").into_response();
    };
    if !path.exists() {
        return (StatusCode::NOT_FOUND, "no such model — create it first").into_response();
    }
    if let Err(e) = std::fs::write(&path, &xml) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not save model: {e}"),
        )
            .into_response();
    }
    let status = deploy_status_of(&server, &xml);
    Json(ModelDto {
        name,
        xml,
        process_ids: status.process_ids,
        deploy_status: status.deploy_status,
        deployed_version: status.deployed_version,
        deployed_key: status.deployed_key,
    })
    .into_response()
}

/// `POST /console/api/models` — create a new model. 409 if a model with the
/// same name already exists.
async fn model_create(
    State(server): State<ServerImpl>,
    Json(body): Json<CreateModelBody>,
) -> Response {
    let Some(path) = workspace::model_path(&body.name) else {
        return (StatusCode::BAD_REQUEST, "invalid model name").into_response();
    };
    if let Err(e) = workspace::ensure_models_dir() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not create workspace: {e}"),
        )
            .into_response();
    }
    if path.exists() {
        return (StatusCode::CONFLICT, "a model with that name already exists").into_response();
    }
    if let Err(e) = std::fs::write(&path, &body.xml) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not create model: {e}"),
        )
            .into_response();
    }
    let status = deploy_status_of(&server, &body.xml);
    (
        StatusCode::CREATED,
        Json(ModelDto {
            name: body.name,
            xml: body.xml,
            process_ids: status.process_ids,
            deploy_status: status.deploy_status,
            deployed_version: status.deployed_version,
            deployed_key: status.deployed_key,
        }),
    )
        .into_response()
}

/// `DELETE /console/api/models/{name}` — remove a model from the workspace.
/// This never touches the engine; an already-deployed definition stays deployed.
async fn model_delete(Path(name): Path<String>) -> Response {
    let Some(path) = workspace::model_path(&name) else {
        return (StatusCode::BAD_REQUEST, "invalid model name").into_response();
    };
    match std::fs::remove_file(&path) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            (StatusCode::NOT_FOUND, "no such model").into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not delete model: {e}"),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Workers API — workspace-backed worker code + a Deno subprocess supervisor
// ---------------------------------------------------------------------------
//
// A worker is a directory of source files under `workers/<name>/` (an entry
// `worker.ts` plus optional helpers and a `deno.json`). The supervisor (see
// `workers`) runs each enabled worker as a sandboxed Deno subprocess that speaks
// the command stream. This API is workspace file CRUD plus start/stop and a live
// log/metrics view; it never touches the engine data dir.

/// Default `worker.ts` scaffold for a new worker. Imports the embedded SDK via
/// the import map in `deno.json` and echoes the job's input back as output.
fn worker_scaffold_ts(job_type: &str) -> String {
    format!(
        r#"import {{ defineWorker }} from "@nanobpm/worker";

// A worker handles jobs of one BPMN job type. Return output variables to
// complete the job, or call job.fail(...) / job.error(code, msg). Throwing
// fails the job. You can `import` npm packages with `npm:` specifiers.
defineWorker({{
  type: "{job_type}",
  maxParallelJobs: 10,
  async handle(job) {{
    console.log(`handling job ${{job.jobKey}} for instance ${{job.processInstanceKey}}`);
    // ...do your work here, using job.variables...
    return {{ handledBy: "{job_type}" }};
  }},
}});
"#
    )
}

/// `deno.json` mapping the `@nanobpm/worker` specifier to the embedded SDK.
const WORKER_DENO_JSON: &str = r#"{
  "imports": {
    "@nanobpm/worker": "../../.nanobpm/worker-sdk.ts"
  }
}
"#;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerSummaryDto {
    name: String,
    files: Vec<String>,
    updated_at_ms: u64,
    runtime: workers::WorkerRuntimeDto,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateWorkerBody {
    name: String,
    /// Job type the scaffolded worker subscribes to. Defaults to the name.
    #[serde(default)]
    job_type: Option<String>,
}

#[derive(Deserialize)]
struct FilePathQuery {
    path: String,
}

#[derive(Deserialize)]
struct CreateFileBody {
    path: String,
}

async fn worker_summary(name: &str) -> Option<WorkerSummaryDto> {
    let dir = workspace::worker_dir(name)?;
    if !dir.is_dir() {
        return None;
    }
    let files = workspace::list_worker_files(name).unwrap_or_default();
    let (updated_at_ms, _) = workspace::file_meta(&dir);
    let runtime = workers::supervisor().runtime(name).await;
    Some(WorkerSummaryDto {
        name: name.to_string(),
        files,
        updated_at_ms,
        runtime,
    })
}

/// `GET /console/api/workers` — list workers with files and runtime status.
async fn workers_list() -> Response {
    let names = match workspace::list_worker_names() {
        Ok(n) => n,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("could not read workspace: {e}"),
            )
                .into_response();
        }
    };
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        if let Some(s) = worker_summary(&name).await {
            out.push(s);
        }
    }
    Json(serde_json::json!({
        "workers": out,
        "denoAvailable": workers::supervisor().deno_available(),
    }))
    .into_response()
}

/// `POST /console/api/workers` — scaffold a new worker directory.
async fn worker_create(Json(body): Json<CreateWorkerBody>) -> Response {
    let Some(dir) = workspace::worker_dir(&body.name) else {
        return (StatusCode::BAD_REQUEST, "invalid worker name").into_response();
    };
    if dir.exists() {
        return (StatusCode::CONFLICT, "a worker with that name already exists").into_response();
    }
    if let Err(e) = std::fs::create_dir_all(&dir) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not create worker: {e}"),
        )
            .into_response();
    }
    let job_type = body.job_type.unwrap_or_else(|| body.name.clone());
    if let Err(e) = std::fs::write(dir.join("worker.ts"), worker_scaffold_ts(&job_type)) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not write worker.ts: {e}"),
        )
            .into_response();
    }
    let _ = std::fs::write(dir.join("deno.json"), WORKER_DENO_JSON);
    match worker_summary(&body.name).await {
        Some(s) => (StatusCode::CREATED, Json(s)).into_response(),
        None => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// `GET /console/api/workers/{name}` — one worker's files and runtime status.
async fn worker_get(Path(name): Path<String>) -> Response {
    match worker_summary(&name).await {
        Some(s) => Json(s).into_response(),
        None => (StatusCode::NOT_FOUND, "no such worker").into_response(),
    }
}

/// `DELETE /console/api/workers/{name}` — remove a worker (must be stopped).
async fn worker_delete(Path(name): Path<String>) -> Response {
    let Some(dir) = workspace::worker_dir(&name) else {
        return (StatusCode::BAD_REQUEST, "invalid worker name").into_response();
    };
    if workers::supervisor().is_active(&name).await {
        return (StatusCode::CONFLICT, "stop the worker before deleting it").into_response();
    }
    match std::fs::remove_dir_all(&dir) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            (StatusCode::NOT_FOUND, "no such worker").into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not delete worker: {e}"),
        )
            .into_response(),
    }
}

/// `GET /console/api/workers/{name}/file?path=worker.ts` — read a worker file.
async fn worker_file_get(Path(name): Path<String>, Query(q): Query<FilePathQuery>) -> Response {
    let Some(path) = workspace::worker_file_path(&name, &q.path) else {
        return (StatusCode::BAD_REQUEST, "invalid path").into_response();
    };
    match std::fs::read_to_string(&path) {
        Ok(text) => text.into_response(),
        Err(_) => (StatusCode::NOT_FOUND, "no such file").into_response(),
    }
}

/// `PUT /console/api/workers/{name}/file?path=worker.ts` — save (create or
/// overwrite) a worker file. Body is the raw file content.
async fn worker_file_save(
    Path(name): Path<String>,
    Query(q): Query<FilePathQuery>,
    body: String,
) -> Response {
    let Some(dir) = workspace::worker_dir(&name) else {
        return (StatusCode::BAD_REQUEST, "invalid worker name").into_response();
    };
    if !dir.is_dir() {
        return (StatusCode::NOT_FOUND, "no such worker").into_response();
    }
    let Some(path) = workspace::worker_file_path(&name, &q.path) else {
        return (StatusCode::BAD_REQUEST, "invalid path").into_response();
    };
    match std::fs::write(&path, &body) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not save file: {e}"),
        )
            .into_response(),
    }
}

/// `POST /console/api/workers/{name}/file` — create a new empty worker file.
async fn worker_file_create(Path(name): Path<String>, Json(body): Json<CreateFileBody>) -> Response {
    let Some(dir) = workspace::worker_dir(&name) else {
        return (StatusCode::BAD_REQUEST, "invalid worker name").into_response();
    };
    if !dir.is_dir() {
        return (StatusCode::NOT_FOUND, "no such worker").into_response();
    }
    let Some(path) = workspace::worker_file_path(&name, &body.path) else {
        return (StatusCode::BAD_REQUEST, "invalid path").into_response();
    };
    if path.exists() {
        return (StatusCode::CONFLICT, "a file with that name already exists").into_response();
    }
    match std::fs::write(&path, "") {
        Ok(()) => StatusCode::CREATED.into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not create file: {e}"),
        )
            .into_response(),
    }
}

/// `DELETE /console/api/workers/{name}/file?path=...` — remove a worker file.
async fn worker_file_delete(Path(name): Path<String>, Query(q): Query<FilePathQuery>) -> Response {
    let Some(path) = workspace::worker_file_path(&name, &q.path) else {
        return (StatusCode::BAD_REQUEST, "invalid path").into_response();
    };
    match std::fs::remove_file(&path) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            (StatusCode::NOT_FOUND, "no such file").into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not delete file: {e}"),
        )
            .into_response(),
    }
}

/// `POST /console/api/workers/{name}/start` — start the worker subprocess.
async fn worker_start(Path(name): Path<String>) -> Response {
    let sup = workers::supervisor();
    match sup.start(&name).await {
        Ok(()) => Json(sup.runtime(&name).await).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

/// `POST /console/api/workers/{name}/stop` — stop the worker subprocess.
async fn worker_stop(Path(name): Path<String>) -> Response {
    let sup = workers::supervisor();
    match sup.stop(&name).await {
        Ok(()) => Json(sup.runtime(&name).await).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

/// Live state for the worker-log SSE stream: replay the history buffer, then
/// stream live lines from the broadcast receiver.
enum LogStreamState {
    History(
        std::vec::IntoIter<workers::LogLine>,
        broadcast::Receiver<workers::LogLine>,
    ),
    Live(broadcast::Receiver<workers::LogLine>),
}

/// `GET /console/api/workers/{name}/logs` — SSE stream of a worker's logs.
/// Replays the recent buffer on connect, then streams live lines.
async fn worker_logs(
    Path(name): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let sup = workers::supervisor();
    let history = sup.log_history(&name).await;
    let rx = sup.subscribe(&name).await;

    let stream = unfold(
        LogStreamState::History(history.into_iter(), rx),
        |st| async move {
            match st {
                LogStreamState::History(mut it, rx) => match it.next() {
                    Some(line) => Some((Ok(log_event(&line)), LogStreamState::History(it, rx))),
                    None => recv_live(rx).await,
                },
                LogStreamState::Live(rx) => recv_live(rx).await,
            }
        },
    );
    Sse::new(stream).keep_alive(KeepAlive::default())
}

fn log_event(line: &workers::LogLine) -> Event {
    let data = serde_json::to_string(line).unwrap_or_else(|_| "{}".to_string());
    Event::default().event("log").data(data)
}

/// Pulls the next live log line, skipping lag and ending the stream on close.
async fn recv_live(
    mut rx: broadcast::Receiver<workers::LogLine>,
) -> Option<(Result<Event, Infallible>, LogStreamState)> {
    loop {
        match rx.recv().await {
            Ok(line) => return Some((Ok(log_event(&line)), LogStreamState::Live(rx))),
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => return None,
        }
    }
}
