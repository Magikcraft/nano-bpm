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
    extract::{Path, State},
    http::{StatusCode, header},
    response::{
        IntoResponse, Json, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::get,
};
use futures_util::stream::{Stream, unfold};
use rust_embed::RustEmbed;
use serde::Serialize;
use std::convert::Infallible;
use std::time::Duration;

use crate::ServerImpl;

/// The built frontend bundle. Path is relative to this source file
/// (`server/src/console/`), so it points at the repo-level `console/dist`.
#[derive(RustEmbed)]
#[folder = "../console/dist"]
struct Assets;

/// Mounts the console SPA and its JSON API onto the gateway.
pub fn router(server: ServerImpl) -> Router {
    Router::new()
        .route("/console/api/topology", get(topology))
        .route("/console/api/instances", get(instances))
        .route("/console/api/instances/{key}", get(instance_detail))
        .route("/console/api/stream", get(stream))
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
// Console API
// ---------------------------------------------------------------------------

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
