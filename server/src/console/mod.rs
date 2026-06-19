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
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Json, Response},
    routing::get,
};
use rust_embed::RustEmbed;
use serde::Serialize;

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
        .route("/console", get(spa_index))
        .route("/console/", get(spa_index))
        .route("/console/{*path}", get(spa_asset))
        .with_state(server)
}

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
