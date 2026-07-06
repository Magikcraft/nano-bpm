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

use std::convert::Infallible;
use std::time::Duration;

use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{
        IntoResponse, Json, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, put},
};
use futures_util::stream::{Stream, unfold};
use nanobpmn_engine_core::bpmn::parse_bpmn;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::ServerImpl;
use crate::backpressure::SlaMode;

pub mod config;
pub mod extensions;
pub mod projects;
pub mod trace;
pub mod worker_export;
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

/// The standalone feature-comparison page (self-contained), served at
/// `/features`.
const FEATURES_HTML: &str = include_str!("features.html");

/// The standalone runtime process-optimization explainer (self-contained,
/// inline SVG diagrams), served at `/optimization`.
const OPTIMIZATION_HTML: &str = include_str!("optimization.html");

/// Mounts the console SPA and its JSON API onto the gateway.
pub fn router(server: ServerImpl) -> Router {
    Router::new()
        .route("/", get(landing))
        .route("/features", get(features))
        .route("/optimization", get(optimization))
        .route("/swagger", get(swagger_index))
        .route("/swagger/", get(swagger_index))
        .route("/swagger/{*path}", get(swagger_asset))
        .route("/asyncapi", get(asyncapi_index))
        .route("/asyncapi/", get(asyncapi_index))
        .route("/asyncapi/{*path}", get(asyncapi_asset))
        .route("/docs", get(docs_index))
        .route("/docs/", get(docs_index))
        .route("/docs/{*path}", get(docs_asset))
        .route("/whitepaper", get(whitepaper_index))
        .route("/whitepaper/", get(whitepaper_index))
        .route(
            "/console/api/gateway-proxy/{*path}",
            get(gateway_proxy)
                .post(gateway_proxy)
                .put(gateway_proxy)
                .delete(gateway_proxy)
                .patch(gateway_proxy),
        )
        .route("/console/api/topology", get(topology))
        .route("/console/api/cluster/health", get(cluster_health))
        .route("/console/api/metrics", get(metrics_snapshot))
        .route("/console/api/cluster/metrics", get(cluster_metrics))
        .route("/console/api/instances", get(instances))
        .route("/console/api/instances/{key}", get(instance_detail))
        .route("/console/api/stream", get(stream))
        .route("/console/api/traces", get(traces))
        .route("/console/api/traces/{key}", get(trace_detail))
        .route("/console/api/traces/{key}/otel", get(trace_otel))
        .route("/console/api/models", get(models).post(model_create))
        .route(
            "/console/api/models/{name}",
            get(model_get).put(model_save).delete(model_delete),
        )
        .route(
            "/console/api/workers",
            get(workers_list).post(worker_create),
        )
        .route("/console/api/worker-sdk", get(worker_sdk_source))
        .route("/console/api/deno-types", get(deno_types_source))
        .route("/console/api/config/server", get(config_server))
        .route("/console/api/config/server/sla", put(config_server_sla))
        .route("/console/api/config/ide", get(config_ide))
        .route("/console/api/lib", get(lib_list).post(lib_file_create))
        .route(
            "/console/api/lib/file",
            get(lib_file_get).put(lib_file_save).delete(lib_file_delete),
        )
        .route(
            "/console/api/export-workers-app",
            axum::routing::post(workers_export),
        )
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
        .route(
            "/console/api/workers/{name}/start",
            axum::routing::post(worker_start),
        )
        .route(
            "/console/api/workers/{name}/stop",
            axum::routing::post(worker_stop),
        )
        .route("/console/api/workers/{name}/logs", get(worker_logs))
        .route(
            "/console/api/projects",
            get(projects_list).post(project_create),
        )
        .route(
            "/console/api/projects/{name}",
            get(project_get).delete(project_delete),
        )
        .route(
            "/console/api/projects/{name}/config",
            get(project_config_get).put(project_config_put),
        )
        .route("/console/api/projects/{name}/files", get(project_files))
        .route(
            "/console/api/projects/{name}/file",
            get(project_file_get)
                .put(project_file_save)
                .post(project_path_create)
                .delete(project_path_delete),
        )
        .route(
            "/console/api/projects/{name}/run",
            axum::routing::post(project_run),
        )
        .route(
            "/console/api/projects/{name}/rename",
            axum::routing::post(project_rename),
        )
        .route(
            "/console/api/projects/{name}/stop",
            axum::routing::post(project_stop),
        )
        .route("/console/api/projects/{name}/logs", get(project_logs))
        .route(
            "/console/api/projects/{name}/compile",
            axum::routing::post(project_compile),
        )
        .route(
            "/console/api/projects/{name}/run-configs",
            get(project_run_configs_list),
        )
        .route(
            "/console/api/projects/{name}/active-run-config",
            axum::routing::put(project_active_run_config_put),
        )
        .route("/console/api/projects/{name}/export", get(project_export))
        .route("/console/api/extensions", get(extensions_list))
        .route(
            "/console/api/extensions/marketplace",
            get(extensions_marketplace),
        )
        .route(
            "/console/api/extensions/install",
            axum::routing::post(extensions_install),
        )
        .route(
            "/console/api/extensions/remove",
            axum::routing::post(extensions_remove),
        )
        .route(
            "/console/api/extensions/trust",
            axum::routing::post(extensions_trust),
        )
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
async fn spa_index(headers: HeaderMap) -> Response {
    serve_embedded("index.html", accepts_gzip(&headers))
}

/// Serves a built asset by path under `/console/`. Unknown paths that are not
/// API routes fall back to `index.html` so client-side routing (deep links like
/// `/console/explorer`) works on a full-page load.
async fn spa_asset(
    axum::extract::Path(path): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Response {
    let path = path.trim_start_matches('/');
    let gz = accepts_gzip(&headers);
    if Assets::get(path).is_some() {
        serve_embedded(path, gz)
    } else {
        // SPA fallback: let the client router resolve the route.
        serve_embedded("index.html", gz)
    }
}

/// Looks an asset up in the embedded bundle and returns it with a guessed
/// content type, gzip-compressing the body when the client accepts it and the
/// payload is worth compressing (text/JS/JSON/wasm/SVG above a small floor).
/// `flate2` is already a dependency (used by the Raft wire), so this adds no new
/// crate; the wasm bundle (~0.5 MB) drops to ~0.2 MB on the wire.
fn serve_embedded(path: &str, accept_gzip: bool) -> Response {
    match Assets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            let mime_str = mime.as_ref().to_string();
            let bytes = content.data.into_owned();
            if accept_gzip
                && is_compressible(&mime_str)
                && bytes.len() >= 1024
                && let Some(gz) = gzip(&bytes)
            {
                return (
                    [
                        (header::CONTENT_TYPE, mime_str),
                        (header::CONTENT_ENCODING, "gzip".to_string()),
                        (header::VARY, "Accept-Encoding".to_string()),
                    ],
                    gz,
                )
                    .into_response();
            }
            (
                [
                    (header::CONTENT_TYPE, mime_str),
                    (header::VARY, "Accept-Encoding".to_string()),
                ],
                bytes,
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

/// True when the client's `Accept-Encoding` lists gzip.
fn accepts_gzip(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',')
                .any(|e| e.trim().split(';').next() == Some("gzip"))
        })
}

/// Whether a MIME type benefits from gzip (text-like, JS/JSON, wasm, SVG).
/// Already-compressed binaries (png/woff2/…) are left untouched.
fn is_compressible(mime: &str) -> bool {
    mime.starts_with("text/")
        || matches!(
            mime,
            "application/javascript" | "application/json" | "application/wasm" | "image/svg+xml"
        )
}

/// Gzip a byte slice; returns `None` on the (unexpected) encoder failure so the
/// caller transparently falls back to the uncompressed body.
fn gzip(bytes: &[u8]) -> Option<Vec<u8>> {
    use std::io::Write;

    use flate2::{Compression, write::GzEncoder};
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(bytes).ok()?;
    enc.finish().ok()
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

/// Serves the standalone feature-comparison page at `/features`.
async fn features() -> Response {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        FEATURES_HTML,
    )
        .into_response()
}

/// Serves the standalone runtime process-optimization explainer at
/// `/optimization`.
async fn optimization() -> Response {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        OPTIMIZATION_HTML,
    )
        .into_response()
}

/// Serves the Swagger UI shell at `/swagger`.
async fn swagger_index(headers: HeaderMap) -> Response {
    serve_embedded("swagger/index.html", accepts_gzip(&headers))
}

/// Serves Swagger UI assets and the bundled OpenAPI spec under `/swagger/`. All
/// files (the UI assets and `openapi.json`) are built into the frontend bundle
/// under `dist/swagger/`.
async fn swagger_asset(
    axum::extract::Path(path): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Response {
    let path = path.trim_start_matches('/');
    serve_embedded(&format!("swagger/{path}"), accepts_gzip(&headers))
}

/// Serves the Falcon Protocol reference (AsyncAPI) at `/asyncapi`. The
/// page is generated at build time from `docs/falcon.asyncapi.yaml`
/// (see `console/scripts/copy-asyncapi.mjs`) into `dist/asyncapi/index.html`.
async fn asyncapi_index(headers: HeaderMap) -> Response {
    serve_embedded("asyncapi/index.html", accepts_gzip(&headers))
}

/// Serves any further assets under `/asyncapi/` (the page is currently a single
/// self-contained `index.html`, but this keeps the route shape parallel to
/// `/swagger/`).
async fn asyncapi_asset(
    axum::extract::Path(path): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Response {
    let path = path.trim_start_matches('/');
    serve_embedded(&format!("asyncapi/{path}"), accepts_gzip(&headers))
}

/// Serves the bundled documentation website at `/docs`. The pages are generated
/// at build time from `README.md` (see `console/scripts/build-docs.mjs`) into
/// `dist/docs/*.html`, one page per README H2 section.
async fn docs_index(headers: HeaderMap) -> Response {
    serve_embedded("docs/index.html", accepts_gzip(&headers))
}

/// Serves a documentation page (or asset) under `/docs/`. Page links are
/// extensionless (`/docs/usage`), so a trailing `.html` is added when the path
/// carries no file extension; explicit asset paths pass through unchanged.
async fn docs_asset(
    axum::extract::Path(path): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Response {
    let path = path.trim_start_matches('/').trim_end_matches('/');
    let gz = accepts_gzip(&headers);
    if path.is_empty() {
        return serve_embedded("docs/index.html", gz);
    }
    let last = path.rsplit('/').next().unwrap_or(path);
    let key = if last.contains('.') {
        format!("docs/{path}")
    } else {
        format!("docs/{path}.html")
    };
    serve_embedded(&key, gz)
}

/// Serves the bundled whitepaper at `/whitepaper`. The page is generated at
/// build time from `docs/whitepaper.md` (see
/// `console/scripts/build-whitepaper.mjs`) into `dist/whitepaper/index.html`, so
/// it refreshes on every console build and is embedded in the release binary.
async fn whitepaper_index(headers: HeaderMap) -> Response {
    serve_embedded("whitepaper/index.html", accepts_gzip(&headers))
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
        gateway_version: env!("NANOBPM_VERSION").to_string(),
        nodes,
        partitions,
    })
}

// ---------------------------------------------------------------------------
// Cluster health (live per-node liveness probe)
// ---------------------------------------------------------------------------

/// Live health of every node in the cluster, as seen from this gateway. Unlike
/// [`topology`] (which reports the *configured* membership), this actively
/// probes each peer's always-on `GET /v2/topology` to report whether it is
/// reachable right now, its gateway version, and the round-trip latency.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterHealthDto {
    checked_at_ms: u64,
    nodes: Vec<NodeHealthDto>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeHealthDto {
    node_id: u32,
    /// `http://host:port` base URL (empty for self in a single-node cluster).
    address: String,
    is_self: bool,
    /// Whether the node answered the probe within the timeout.
    reachable: bool,
    /// The node's reported gateway version (when reachable).
    version: Option<String>,
    /// Probe round-trip time in milliseconds (when reachable).
    latency_ms: Option<u64>,
    /// Why the probe failed (when unreachable).
    error: Option<String>,
}

/// Per-peer probe timeout. Generous enough for a loaded node to answer, short
/// enough that one dead peer doesn't stall the whole health response (all peers
/// are probed concurrently, so the endpoint resolves in ~one timeout at worst).
const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);

/// `GET /console/api/cluster/health` — probes every peer concurrently and
/// reports live reachability/version/latency. Self is reported without a network
/// round-trip (it is, by definition, up and serving this request).
async fn cluster_health(State(server): State<ServerImpl>) -> Json<ClusterHealthDto> {
    let topology = server.engine.topology();
    let self_id = topology.node_id;
    let self_version = env!("NANOBPM_VERSION").to_string();
    let num_nodes = topology.num_nodes();

    let probes = (0..num_nodes).map(|node| {
        let is_self = node == self_id;
        let address = topology.peer_addr(node).unwrap_or("").to_string();
        let self_version = self_version.clone();
        async move {
            if is_self {
                return NodeHealthDto {
                    node_id: node,
                    address,
                    is_self: true,
                    reachable: true,
                    version: Some(self_version),
                    latency_ms: Some(0),
                    error: None,
                };
            }
            match probe_peer(&address).await {
                Ok((version, latency)) => NodeHealthDto {
                    node_id: node,
                    address,
                    is_self: false,
                    reachable: true,
                    version,
                    latency_ms: Some(latency.as_millis() as u64),
                    error: None,
                },
                Err(err) => NodeHealthDto {
                    node_id: node,
                    address,
                    is_self: false,
                    reachable: false,
                    version: None,
                    latency_ms: None,
                    error: Some(err),
                },
            }
        }
    });

    let nodes = futures_util::future::join_all(probes).await;
    let checked_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    Json(ClusterHealthDto {
        checked_at_ms,
        nodes,
    })
}

/// Probes one peer's `GET {base_url}/v2/topology`, returning its reported
/// `gatewayVersion` and the round-trip latency. Plain HTTP/1.1 (peers are
/// TLS-less, like the falcon uplink). Any transport error, non-2xx
/// status, or timeout is mapped to a short human-readable string.
async fn probe_peer(base_url: &str) -> Result<(Option<String>, Duration), String> {
    use http_body_util::BodyExt;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    if base_url.is_empty() {
        return Err("no address configured".to_string());
    }

    let uri: hyper::Uri = format!("{}/v2/topology", base_url.trim_end_matches('/'))
        .parse()
        .map_err(|e| format!("bad peer url: {e}"))?;

    let client: Client<_, http_body_util::Empty<hyper::body::Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();

    let started = std::time::Instant::now();
    let fut = async {
        let resp = client.get(uri).await.map_err(|e| format!("connect: {e}"))?;
        let status = resp.status();
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| format!("read: {e}"))?
            .to_bytes();
        Ok::<_, String>((status, body))
    };

    let (status, body) = tokio::time::timeout(HEALTH_PROBE_TIMEOUT, fut)
        .await
        .map_err(|_| "timeout".to_string())??;

    if !status.is_success() {
        return Err(format!("HTTP {}", status.as_u16()));
    }

    // gatewayVersion is best-effort: a reachable node with an unparseable body
    // is still "up", just without a version string.
    let version = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| {
            v.get("gatewayVersion")
                .and_then(|s| s.as_str())
                .map(str::to_string)
        });

    Ok((version, started.elapsed()))
}

// ---------------------------------------------------------------------------
// Cross-origin gateway proxy
// ---------------------------------------------------------------------------

/// Same-origin proxy for arbitrary Camunda REST calls (e.g. `/v2/deployments`,
/// `/v2/process-instances`) targeting a **foreign** gateway — typically a
/// Camunda 8 self-managed cluster running on a different port than the Nano
/// console. Browsers block direct cross-origin `fetch()` unless the target
/// gateway serves CORS headers, which stock `c8run` does not; routing the
/// request through this proxy sidesteps that requirement by making the call
/// server-side.
///
/// Contract:
/// - Client sends `{METHOD} /console/api/gateway-proxy/{path}` with header
///   `X-Gateway-Target: http(s)://host[:port]` and the original request body.
/// - Server forwards `{METHOD} {target}/{path}` verbatim (body + content-type +
///   authorization pass through) and streams the upstream status + body back.
/// - No caching, no rewriting — this is a dumb pass-through so the Camunda
///   REST semantics are unchanged.
///
/// This is deliberately generic (one handler for all `/v2/*`) rather than
/// endpoint-specific: we don't want to grow a shim every time the Camunda REST
/// surface adds a route.
async fn gateway_proxy(
    Path(rest): Path<String>,
    method: axum::http::Method,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let target = match headers
        .get("x-gateway-target")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
    {
        Some(t) if !t.is_empty() => t.trim_end_matches('/').to_string(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                "missing or empty X-Gateway-Target header",
            )
                .into_response();
        }
    };

    if !(target.starts_with("http://") || target.starts_with("https://")) {
        return (
            StatusCode::BAD_REQUEST,
            "X-Gateway-Target must be an absolute http(s) URL",
        )
            .into_response();
    }

    let url = format!("{target}/{rest}");
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("proxy client init: {e}"),
            )
                .into_response();
        }
    };

    let up_method = match reqwest::Method::from_bytes(method.as_str().as_bytes()) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("bad method: {e}")).into_response();
        }
    };

    let mut req = client.request(up_method, &url);
    // Forward only the request-shaping headers we actually need. Hop-by-hop
    // headers (Host, Connection, Content-Length) are dropped so reqwest can
    // recompute them for the upstream connection.
    for name in [header::CONTENT_TYPE, header::ACCEPT, header::AUTHORIZATION] {
        if let Some(v) = headers.get(&name) {
            req = req.header(name, v);
        }
    }
    if !body.is_empty() {
        req = req.body(body.to_vec());
    }

    let upstream = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            return (StatusCode::BAD_GATEWAY, format!("upstream {url}: {e}")).into_response();
        }
    };

    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let ct = upstream.headers().get(header::CONTENT_TYPE).cloned();
    let cd = upstream.headers().get(header::CONTENT_DISPOSITION).cloned();
    let bytes = match upstream.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return (StatusCode::BAD_GATEWAY, format!("upstream read: {e}")).into_response();
        }
    };

    let mut out = Response::new(axum::body::Body::from(bytes));
    *out.status_mut() = status;
    if let Some(v) = ct {
        out.headers_mut().insert(header::CONTENT_TYPE, v);
    }
    if let Some(v) = cd {
        out.headers_mut().insert(header::CONTENT_DISPOSITION, v);
    }
    out
}

// ---------------------------------------------------------------------------
// Metrics dashboard API
// ---------------------------------------------------------------------------

/// A point-in-time metrics snapshot for the dashboard. Counters are monotonic;
/// the frontend derives throughput **rates** from the deltas of two successive
/// polls (so this endpoint stays a cheap, stateless reading). `activeInstances`
/// is read on demand from the read model only when this endpoint is polled — it
/// is deliberately NOT an always-on `COUNT` in the engine tick loop, so opening
/// the dashboard never perturbs a running performance demo.
#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct MetricsDto {
    /// Server clock at snapshot time (ms). The frontend uses successive
    /// timestamps as the exact dt for rate computation.
    timestamp_ms: u64,
    /// Active (non-terminal) process instances in this node's read model.
    active_instances: i64,

    // Throughput counters (monotonic, split by protocol).
    creates_rest: u64,
    creates_stream: u64,
    creates_total: u64,
    completions_rest: u64,
    completions_stream: u64,
    completions_total: u64,

    // Live gauges.
    connections_active: i64,
    commit_inflight: i64,

    // Durability counters.
    commits_total: u64,
    writes_total: u64,
    bytes_total: u64,
    credit_stalls_total: u64,

    // Derived means (ms / count) from histogram aggregates — convenient for the
    // cards; the frontend doesn't have to carry sum+count itself.
    fsync_mean_ms: f64,
    commit_wait_mean_ms: f64,
    commit_batch_mean: f64,
    frame_processing_mean_ms: f64,

    // Writer duty cycle: busy / (busy + idle) over all time. A value near 1.0
    // means the single journal writer is saturated.
    writer_busy_ratio: f64,

    /// Resident memory (jemalloc `stats.resident`, bytes) — the figure that
    /// tracks the process's real footprint. `null` on non-jemalloc targets.
    #[serde(default)]
    resident_bytes: Option<u64>,
}

/// Builds this node's metrics snapshot DTO. Shared by `GET /console/api/metrics`
/// (the local dashboard) and the self entry of the cluster-wide aggregation, so
/// both report identical numbers.
fn build_local_metrics(server: &ServerImpl) -> MetricsDto {
    let s = crate::metrics::snapshot();

    let mean_ms = |sum: f64, count: u64| {
        if count == 0 {
            0.0
        } else {
            sum / count as f64 * 1000.0
        }
    };
    let mean = |sum: f64, count: u64| if count == 0 { 0.0 } else { sum / count as f64 };
    let busy_ratio = {
        let total = s.writer_busy_seconds + s.writer_idle_seconds;
        if total == 0.0 {
            0.0
        } else {
            s.writer_busy_seconds / total
        }
    };

    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    MetricsDto {
        timestamp_ms,
        active_instances: server.store.active_instance_count() as i64,

        creates_rest: s.creates_rest,
        creates_stream: s.creates_stream,
        creates_total: s.creates_rest + s.creates_stream,
        completions_rest: s.completions_rest,
        completions_stream: s.completions_stream,
        completions_total: s.completions_rest + s.completions_stream,

        connections_active: s.stream_connections_active,
        commit_inflight: s.commit_inflight,

        commits_total: s.commits_total,
        writes_total: s.writes_total,
        bytes_total: s.bytes_total,
        credit_stalls_total: s.stream_credit_stalls_total,

        fsync_mean_ms: mean_ms(s.fsync_seconds_sum, s.fsync_count),
        commit_wait_mean_ms: mean_ms(s.commit_wait_seconds_sum, s.commit_wait_count),
        commit_batch_mean: mean(s.commit_batch_size_sum, s.commit_batch_count),
        frame_processing_mean_ms: mean_ms(s.frame_processing_seconds_sum, s.frame_processing_count),

        writer_busy_ratio: busy_ratio,

        resident_bytes: crate::memory::resident_bytes().map(|b| b as u64),
    }
}

/// `GET /console/api/metrics` — the metrics dashboard's data source. Reads the
/// process-global Prometheus handles in one pass plus the live active-instance
/// count, and maps them to a camelCase DTO with a few convenience means.
async fn metrics_snapshot(State(server): State<ServerImpl>) -> Json<MetricsDto> {
    Json(build_local_metrics(&server))
}

// ---------------------------------------------------------------------------
// Cluster-wide metrics (per-node aggregation)
// ---------------------------------------------------------------------------

/// Per-node metrics plus a cluster aggregate, for the dashboard's cluster view.
/// Each peer's `GET /console/api/metrics` is probed concurrently; unreachable
/// peers are reported with `reachable=false` and contribute nothing to the
/// aggregate. Self is read locally (no round-trip).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterMetricsDto {
    checked_at_ms: u64,
    nodes: Vec<NodeMetricsDto>,
    aggregate: AggregateMetricsDto,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeMetricsDto {
    node_id: u32,
    address: String,
    is_self: bool,
    reachable: bool,
    error: Option<String>,
    metrics: Option<MetricsDto>,
}

/// Sums of the headline counters/gauges over all reachable nodes. Cluster-wide
/// throughput is derived client-side from successive deltas of `*_total`.
#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct AggregateMetricsDto {
    reachable_nodes: u32,
    total_nodes: u32,
    active_instances: i64,
    creates_total: u64,
    completions_total: u64,
    connections_active: i64,
    commit_inflight: i64,
    resident_bytes: u64,
}

/// `GET /console/api/cluster/metrics` — probes every node's metrics and returns
/// the per-node breakdown plus a reachable-node aggregate.
async fn cluster_metrics(State(server): State<ServerImpl>) -> Json<ClusterMetricsDto> {
    let topology = server.engine.topology();
    let self_id = topology.node_id;
    let num_nodes = topology.num_nodes();

    let probes = (0..num_nodes).map(|node| {
        let is_self = node == self_id;
        let address = topology.peer_addr(node).unwrap_or("").to_string();
        let server = server.clone();
        async move {
            if is_self {
                return NodeMetricsDto {
                    node_id: node,
                    address,
                    is_self: true,
                    reachable: true,
                    error: None,
                    metrics: Some(build_local_metrics(&server)),
                };
            }
            match probe_peer_metrics(&address).await {
                Ok(metrics) => NodeMetricsDto {
                    node_id: node,
                    address,
                    is_self: false,
                    reachable: true,
                    error: None,
                    metrics: Some(metrics),
                },
                Err(err) => NodeMetricsDto {
                    node_id: node,
                    address,
                    is_self: false,
                    reachable: false,
                    error: Some(err),
                    metrics: None,
                },
            }
        }
    });

    let nodes = futures_util::future::join_all(probes).await;

    let mut aggregate = AggregateMetricsDto {
        total_nodes: num_nodes,
        ..Default::default()
    };
    for n in &nodes {
        if let Some(m) = &n.metrics {
            aggregate.reachable_nodes += 1;
            aggregate.active_instances += m.active_instances;
            aggregate.creates_total += m.creates_total;
            aggregate.completions_total += m.completions_total;
            aggregate.connections_active += m.connections_active;
            aggregate.commit_inflight += m.commit_inflight;
            aggregate.resident_bytes += m.resident_bytes.unwrap_or(0);
        }
    }

    let checked_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    Json(ClusterMetricsDto {
        checked_at_ms,
        nodes,
        aggregate,
    })
}

/// Probes one peer's `GET {base_url}/console/api/metrics` and parses its
/// [`MetricsDto`]. A peer built without the `console` feature returns 404 here
/// (mapped to an error string); the always-on health probe still reports it up.
async fn probe_peer_metrics(base_url: &str) -> Result<MetricsDto, String> {
    use http_body_util::BodyExt;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    if base_url.is_empty() {
        return Err("no address configured".to_string());
    }

    let uri: hyper::Uri = format!("{}/console/api/metrics", base_url.trim_end_matches('/'))
        .parse()
        .map_err(|e| format!("bad peer url: {e}"))?;

    let client: Client<_, http_body_util::Empty<hyper::body::Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();

    let fut = async {
        let resp = client.get(uri).await.map_err(|e| format!("connect: {e}"))?;
        let status = resp.status();
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| format!("read: {e}"))?
            .to_bytes();
        Ok::<_, String>((status, body))
    };

    let (status, body) = tokio::time::timeout(HEALTH_PROBE_TIMEOUT, fut)
        .await
        .map_err(|_| "timeout".to_string())??;

    if status.as_u16() == 404 {
        return Err("no console on peer".to_string());
    }
    if !status.is_success() {
        return Err(format!("HTTP {}", status.as_u16()));
    }

    serde_json::from_slice::<MetricsDto>(&body).map_err(|e| format!("parse: {e}"))
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

/// Query string for the paginated instance list (`?page=&pageSize=`). Both are
/// optional; defaults are applied (and `pageSize` clamped) in the handler.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstanceListQuery {
    page: Option<i64>,
    page_size: Option<i64>,
}

/// One page of process instances plus the total row count, so the console can
/// render a pager without a second request.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InstancePage {
    items: Vec<InstanceDto>,
    total: i64,
    page: i64,
    page_size: i64,
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

/// `GET /console/api/instances?page=N&pageSize=M` — one page of process
/// instances, newest first, plus the total count for the pager. Pagination is
/// pushed into SQLite (`process_instances_page`) so a node with a large read
/// model returns a bounded page instead of materializing and sorting every row
/// (which made the Process Explorer hang).
async fn instances(
    State(server): State<ServerImpl>,
    Query(q): Query<InstanceListQuery>,
) -> Json<InstancePage> {
    let page = q.page.unwrap_or(0).max(0);
    let page_size = q.page_size.unwrap_or(50).clamp(1, 500);
    let total = server.store.process_instance_count();
    let rows = server
        .store
        .process_instances_page(page_size, page.saturating_mul(page_size));
    Json(InstancePage {
        items: rows.iter().map(InstanceDto::from).collect(),
        total,
        page,
        page_size,
    })
}

/// `GET /console/api/instances/{key}` — one instance with its variables, jobs,
/// and incidents. 404 when the key is malformed or unknown.
async fn instance_detail(State(server): State<ServerImpl>, Path(key): Path<String>) -> Response {
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

/// `GET /console/api/traces?limit=N` — recent execution-trace summaries
/// (most-recent first). Backed by the in-memory [`trace::TraceStore`] folded
/// off the engine event stream (process-optimization design doc §3, Tier A).
async fn traces(
    State(server): State<ServerImpl>,
    Query(q): Query<TraceListQuery>,
) -> Json<Vec<trace::TraceSummaryDto>> {
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);
    Json(server.trace_store.list(limit))
}

/// `GET /console/api/traces/{key}` — the full per-element trace for one
/// instance. 404 when the key is malformed or no longer retained in the ring.
async fn trace_detail(State(server): State<ServerImpl>, Path(key): Path<String>) -> Response {
    let Ok(key) = key.parse::<u64>() else {
        return (StatusCode::NOT_FOUND, "invalid instance key").into_response();
    };
    match server.trace_store.get(key) {
        Some(t) => Json(t).into_response(),
        None => (StatusCode::NOT_FOUND, "no such trace").into_response(),
    }
}

/// `GET /console/api/traces/{key}/otel` — the instance trace rendered as an
/// OTLP/JSON trace document (root process span + per-element + per-job spans),
/// ingestible by an OpenTelemetry collector.
async fn trace_otel(State(server): State<ServerImpl>, Path(key): Path<String>) -> Response {
    let Ok(key) = key.parse::<u64>() else {
        return (StatusCode::NOT_FOUND, "invalid instance key").into_response();
    };
    match server.trace_store.otel(key) {
        Some(v) => Json(v).into_response(),
        None => (StatusCode::NOT_FOUND, "no such trace").into_response(),
    }
}

#[derive(Deserialize)]
struct TraceListQuery {
    limit: Option<usize>,
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
    let deployed = primary.as_ref().and_then(|id| {
        server
            .store
            .process_definitions()
            .into_iter()
            .find(|d| &d.process_id == id)
    });
    let (deploy_status, deployed_version, deployed_key) = match deployed {
        None => ("not_deployed", None, None),
        Some(row) => {
            let deployed_xml = server
                .store
                .process_definition_xml(row.key)
                .unwrap_or_default();
            let status = if deployed_xml == xml {
                "in_sync"
            } else {
                "modified"
            };
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
        return (
            StatusCode::CONFLICT,
            "a model with that name already exists",
        )
            .into_response();
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
// the Falcon protocol. This API is workspace file CRUD plus start/stop and a live
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

/// `deno.json` mapping the `@nanobpm/worker` specifier to the embedded SDK and
/// the `@lib/` alias to the shared workspace library, so a worker can both
/// `import { defineWorker } from "@nanobpm/worker"` and reuse shared logic with
/// `import { fmt } from "@lib/money.ts"`.
const WORKER_DENO_JSON: &str = r#"{
  "imports": {
    "@nanobpm/worker": "../../.nanobpm/worker-sdk.ts",
    "@lib/": "../../lib/"
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
        return (
            StatusCode::CONFLICT,
            "a worker with that name already exists",
        )
            .into_response();
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

/// `GET /console/api/worker-sdk` — the embedded worker SDK TypeScript source,
/// served so the console editor can register it as a Monaco extra-lib and offer
/// full IntelliSense for `@nanobpm/worker` (types, JSDoc, signatures).
async fn worker_sdk_source() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        worker_export::worker_sdk_source(),
    )
        .into_response()
}

/// `GET /console/api/deno-types` — the embedded Deno namespace ambient types,
/// served so the console editor can register them as a Monaco extra-lib. This
/// makes `Deno.env`, `Deno.readDir`, `Deno.serve`, etc. resolve in worker and
/// `main.ts` code instead of erroring with "Cannot find name 'Deno'". Fully
/// offline (embedded in the gateway binary).
async fn deno_types_source() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        worker_export::deno_namespace_types(),
    )
        .into_response()
}

/// Body for `POST /console/api/export-workers-app`.
#[derive(Deserialize)]
struct ExportWorkersBody {
    /// The worker names to bundle into the standalone application.
    #[serde(default)]
    workers: Vec<String>,
}

/// `POST /console/api/export-workers-app` — bundle the selected workers into a
/// standalone, runnable Deno application, returned as a downloadable `.zip`
/// (see [`worker_export`]).
async fn workers_export(Json(body): Json<ExportWorkersBody>) -> Response {
    match worker_export::build_app(&body.workers) {
        Ok(zip) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/zip".to_string()),
                (
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}\"", worker_export::zip_filename()),
                ),
                (header::CACHE_CONTROL, "no-store".to_string()),
            ],
            zip,
        )
            .into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
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
async fn worker_file_create(
    Path(name): Path<String>,
    Json(body): Json<CreateFileBody>,
) -> Response {
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

// ---------------------------------------------------------------------------
// Shared library API — reusable TS/JS files under `<workspace>/lib/`, importable
// from every worker via the `@lib/` import-map alias. Mirrors the worker file
// CRUD; the library has no runtime of its own (it is only ever imported).
// ---------------------------------------------------------------------------

/// `GET /console/api/lib` — list the shared library files.
async fn lib_list() -> Response {
    match workspace::list_lib_files() {
        Ok(files) => Json(serde_json::json!({ "files": files })).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not read library: {e}"),
        )
            .into_response(),
    }
}

/// `GET /console/api/lib/file?path=money.ts` — read a shared library file.
async fn lib_file_get(Query(q): Query<FilePathQuery>) -> Response {
    let Some(path) = workspace::lib_file_path(&q.path) else {
        return (StatusCode::BAD_REQUEST, "invalid path").into_response();
    };
    match std::fs::read_to_string(&path) {
        Ok(text) => text.into_response(),
        Err(_) => (StatusCode::NOT_FOUND, "no such file").into_response(),
    }
}

/// `PUT /console/api/lib/file?path=money.ts` — save (create or overwrite) a
/// shared library file. Body is the raw file content.
async fn lib_file_save(Query(q): Query<FilePathQuery>, body: String) -> Response {
    let Ok(_) = workspace::ensure_lib_dir() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "could not create library dir",
        )
            .into_response();
    };
    let Some(path) = workspace::lib_file_path(&q.path) else {
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

/// `POST /console/api/lib/file` — create a new empty shared library file.
async fn lib_file_create(Json(body): Json<CreateFileBody>) -> Response {
    let Ok(_) = workspace::ensure_lib_dir() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "could not create library dir",
        )
            .into_response();
    };
    let Some(path) = workspace::lib_file_path(&body.path) else {
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

/// `DELETE /console/api/lib/file?path=...` — remove a shared library file.
async fn lib_file_delete(Query(q): Query<FilePathQuery>) -> Response {
    let Some(path) = workspace::lib_file_path(&q.path) else {
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

// ---------------------------------------------------------------------------
// Projects API — the RAD environment. A project is a self-contained directory
// (resources/, workers/, lib/, main.ts, deno.json, nanobpm.project.json) that
// is itself a runnable Deno app. See `projects`.
// ---------------------------------------------------------------------------

/// `GET /console/api/projects` — list projects (tiles) with resource counts and
/// live run status.
async fn projects_list() -> Response {
    let mut list = match projects::list_projects() {
        Ok(l) => l,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("could not read projects: {e}"),
            )
                .into_response();
        }
    };
    let sup = projects::supervisor();
    let mut out = Vec::with_capacity(list.len());
    for mut p in list.drain(..) {
        p.running = sup.is_running(&p.name).await;
        out.push(p);
    }
    Json(serde_json::json!({
        "projects": out,
        "denoAvailable": sup.deno_available(),
        "platforms": projects::PLATFORMS,
        "templates": projects::project_templates(),
        "extensions": extensions_overview(),
    }))
    .into_response()
}

/// Extensions + which lang/app packs are usable on this machine.
fn extensions_overview() -> serde_json::Value {
    let exts = extensions::all_extensions();
    let trust = extensions::load_trust();
    let list: Vec<_> = exts
        .iter()
        .map(|e| {
            serde_json::json!({
                "id": e.id, "kind": e.kind, "displayName": e.display_name, "builtin": e.builtin,
                "fileTypes": e.file_types, "templates": e.templates,
                "themes": e.themes,
                "toolchainAvailable": extensions::toolchain_available(e),
                "trusted": extensions::is_trusted(&e.id),
            })
        })
        .collect();
    serde_json::json!({ "extensions": list, "yolo": trust.yolo })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateProjectBody {
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    template: Option<String>,
}

/// `POST /console/api/projects` — scaffold a new project.
async fn project_create(Json(body): Json<CreateProjectBody>) -> Response {
    let template = body.template.as_deref().unwrap_or("starter");
    match projects::create_project(&body.name, &body.description, template) {
        Ok(cfg) => (StatusCode::CREATED, Json(cfg)).into_response(),
        Err(e) if e.contains("already exists") => (StatusCode::CONFLICT, e).into_response(),
        Err(e) if e.contains("invalid") => (StatusCode::BAD_REQUEST, e).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

/// `GET /console/api/projects/{name}` — config + file tree + run state.
async fn project_get(Path(name): Path<String>) -> Response {
    project_detail(&name).await
}

/// `GET /console/api/extensions` — installed + built-in packs and trust state.
async fn extensions_list() -> Response {
    Json(extensions_overview()).into_response()
}

/// `GET /console/api/config/server` — SLA mode + read-only env-parameter registry.
async fn config_server(State(server): State<ServerImpl>) -> Response {
    Json(config::server_config_json(server.sla_mode())).into_response()
}

/// `PUT /console/api/config/server/sla` — switch the SLA mode at runtime. Body
/// `{"mode":"latency"|"admission"}`. An unrecognised mode is rejected (400)
/// rather than silently fail-safing, so an operator gets clear feedback; the
/// updated config is returned on success.
async fn config_server_sla(
    State(server): State<ServerImpl>,
    Json(body): Json<SlaModeBody>,
) -> Response {
    let mode = match body.mode.trim().to_ascii_lowercase().as_str() {
        "latency" => SlaMode::Latency,
        "admission" => SlaMode::Admission,
        other => {
            return (
                StatusCode::BAD_REQUEST,
                format!("unknown SLA mode {other:?}; expected \"latency\" or \"admission\""),
            )
                .into_response();
        }
    };
    server.switch_sla_mode(mode).await;
    Json(config::server_config_json(server.sla_mode())).into_response()
}

#[derive(Deserialize)]
struct SlaModeBody {
    mode: String,
}

/// `GET /console/api/config/ide` — toolchain dependencies + language-pack config.
/// Probing toolchains shells out (`<bin> --version`), so run it off the async
/// runtime's worker threads.
async fn config_ide() -> Response {
    match tokio::task::spawn_blocking(config::ide_config_json).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// `GET /console/api/extensions/marketplace` — packs on npm tagged `nano-ide-ext`,
/// categorised by language/app/example, with installed status.
async fn extensions_marketplace() -> Response {
    match tokio::task::spawn_blocking(extensions::marketplace).await {
        Ok(Ok(list)) => Json(serde_json::json!({ "entries": list })).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_GATEWAY, e).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct ExtPkgBody {
    pkg: String,
}

/// `POST /console/api/extensions/install` — install a `nano-ide-ext-*` pkg from npm.
async fn extensions_install(Json(b): Json<ExtPkgBody>) -> Response {
    match tokio::task::spawn_blocking(move || extensions::install_from_npm(&b.pkg)).await {
        Ok(Ok(m)) => (StatusCode::CREATED, Json(m)).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, e).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// `POST /console/api/extensions/remove` — uninstall an installed pack.
async fn extensions_remove(Json(b): Json<ExtPkgBody>) -> Response {
    match extensions::remove(&b.pkg) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrustBody {
    #[serde(default)]
    yolo: Option<bool>,
    #[serde(default)]
    approve: Option<String>,
    #[serde(default)]
    revoke: Option<String>,
}

/// `POST /console/api/extensions/trust` — toggle yolo / approve-always per pack.
async fn extensions_trust(Json(b): Json<TrustBody>) -> Response {
    let mut t = extensions::load_trust();
    if let Some(y) = b.yolo {
        t.yolo = y;
    }
    if let Some(id) = b.approve {
        t.approved.insert(id);
    }
    if let Some(id) = b.revoke {
        t.approved.remove(&id);
    }
    match extensions::save_trust(&t) {
        Ok(()) => Json(extensions_overview()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn project_detail(name: &str) -> Response {
    let Some(cfg) = projects::read_config(name) else {
        return (StatusCode::NOT_FOUND, "no such project").into_response();
    };
    let tree = projects::file_tree(name).unwrap_or_default();
    let sup = projects::supervisor();
    // Run/Compile readiness is language-aware: Deno projects need the Deno
    // runtime; a polyglot lang pack (e.g. Rust) needs its own toolchain (cargo).
    let runnable = if cfg.lang == "deno" {
        sup.deno_available()
    } else {
        extensions::lang_pack(&cfg.lang)
            .map(|p| extensions::toolchain_available(&p))
            .unwrap_or(false)
    };
    Json(serde_json::json!({
        "config": cfg,
        "files": tree,
        "runState": sup.run_state(name).await,
        "denoAvailable": sup.deno_available(),
        "runnable": runnable,
        "platforms": projects::PLATFORMS,
    }))
    .into_response()
}

/// `DELETE /console/api/projects/{name}` — remove a project (must be stopped).
async fn project_delete(Path(name): Path<String>) -> Response {
    if projects::supervisor().is_running(&name).await {
        return (
            StatusCode::CONFLICT,
            "stop the application before deleting it",
        )
            .into_response();
    }
    match projects::delete_project(&name) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            (StatusCode::NOT_FOUND, "no such project").into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not delete project: {e}"),
        )
            .into_response(),
    }
}

/// `POST /console/api/projects/{name}/rename` — rename a project (must be stopped).
async fn project_rename(Path(name): Path<String>, Json(body): Json<RenameProjectBody>) -> Response {
    if projects::supervisor().is_running(&name).await {
        return (
            StatusCode::CONFLICT,
            "stop the application before renaming it",
        )
            .into_response();
    }
    match projects::rename_project(&name, body.new_name.trim()) {
        Ok(cfg) => (StatusCode::OK, Json(cfg)).into_response(),
        Err(e) if e.contains("already exists") => (StatusCode::CONFLICT, e).into_response(),
        Err(e) if e.contains("no such") => (StatusCode::NOT_FOUND, e).into_response(),
        Err(e) if e.contains("invalid") => (StatusCode::BAD_REQUEST, e).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RenameProjectBody {
    new_name: String,
}

/// `GET /console/api/projects/{name}/config` — the project config.
async fn project_config_get(Path(name): Path<String>) -> Response {
    match projects::read_config(&name) {
        Some(cfg) => Json(cfg).into_response(),
        None => (StatusCode::NOT_FOUND, "no such project").into_response(),
    }
}

/// `PUT /console/api/projects/{name}/config` — update the project config.
async fn project_config_put(
    Path(name): Path<String>,
    Json(mut cfg): Json<projects::ProjectConfig>,
) -> Response {
    if projects::read_config(&name).is_none() {
        return (StatusCode::NOT_FOUND, "no such project").into_response();
    }
    cfg.name = name.clone();
    cfg.updated_ms = now_ms_proj();
    match projects::write_config(&name, &cfg) {
        Ok(()) => Json(cfg).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not save config: {e}"),
        )
            .into_response(),
    }
}

/// `GET /console/api/projects/{name}/run-configs` — list the named run
/// configurations snapshotted from the scaffolding pack and the id of the
/// active one (or `null` when none is set — in which case the resolver picks
/// the `default: true` entry, else the first).
async fn project_run_configs_list(Path(name): Path<String>) -> Response {
    let Some(cfg) = projects::read_config(&name) else {
        return (StatusCode::NOT_FOUND, "no such project").into_response();
    };
    let (configs, active) = match cfg.toolchain.as_ref() {
        Some(tc) => (tc.run_configs.clone(), tc.active_run_config.clone()),
        None => (vec![], None),
    };
    Json(serde_json::json!({ "runConfigs": configs, "active": active })).into_response()
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActiveRunConfigBody {
    id: Option<String>,
}

/// `PUT /console/api/projects/{name}/active-run-config` — set which run config
/// the Run/Compile buttons should use. Body: `{ "id": "stock-rest" }`; pass
/// `null` (or omit) to clear the pin and revert to `default: true` / first.
/// Rejects unknown ids so the picker can't silently persist a typo.
async fn project_active_run_config_put(
    Path(name): Path<String>,
    Json(body): Json<ActiveRunConfigBody>,
) -> Response {
    let Some(mut cfg) = projects::read_config(&name) else {
        return (StatusCode::NOT_FOUND, "no such project").into_response();
    };
    let Some(tc) = cfg.toolchain.as_mut() else {
        return (
            StatusCode::BAD_REQUEST,
            "project has no toolchain (no run configs to select)",
        )
            .into_response();
    };
    if let Some(id) = body.id.as_deref()
        && !tc.run_configs.iter().any(|rc| rc.id == id)
    {
        return (
            StatusCode::BAD_REQUEST,
            format!("no run config with id '{id}'"),
        )
            .into_response();
    }
    tc.active_run_config = body.id;
    cfg.updated_ms = now_ms_proj();
    match projects::write_config(&name, &cfg) {
        Ok(()) => Json(serde_json::json!({
            "active": cfg.toolchain.as_ref().and_then(|t| t.active_run_config.clone()),
        }))
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not save config: {e}"),
        )
            .into_response(),
    }
}

fn now_ms_proj() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `GET /console/api/projects/{name}/files` — the recursive file tree.
async fn project_files(Path(name): Path<String>) -> Response {
    match projects::file_tree(&name) {
        Some(tree) => Json(serde_json::json!({ "files": tree })).into_response(),
        None => (StatusCode::NOT_FOUND, "no such project").into_response(),
    }
}

/// `GET /console/api/projects/{name}/file?path=...` — read a file.
///
/// Text files are returned verbatim as `text/plain`. Binary files are *not*
/// streamed back (the editor cannot render them): instead the body is a small
/// JSON descriptor `{ absPath, size }` so the UI can show a placeholder. Every
/// response carries `X-File-Binary` (`true`/`false`) and `X-File-Size` (bytes).
async fn project_file_get(Path(name): Path<String>, Query(q): Query<FilePathQuery>) -> Response {
    let Some(path) = projects::safe_project_path(&name, &q.path) else {
        return (StatusCode::BAD_REQUEST, "invalid path").into_response();
    };
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(_) => return (StatusCode::NOT_FOUND, "no such file").into_response(),
    };
    let size = bytes.len();
    // A file is treated as binary if it contains a NUL byte or is not valid
    // UTF-8 — the same heuristic git uses for "is this text?".
    let text = match std::str::from_utf8(&bytes) {
        Ok(s) if !s.contains('\u{0}') => Some(s.to_owned()),
        _ => None,
    };
    match text {
        Some(s) => (
            StatusCode::OK,
            [
                (
                    header::CONTENT_TYPE,
                    "text/plain; charset=utf-8".to_string(),
                ),
                ("x-file-binary".parse().unwrap(), "false".to_string()),
                ("x-file-size".parse().unwrap(), size.to_string()),
            ],
            s,
        )
            .into_response(),
        None => {
            let abs = std::fs::canonicalize(&path)
                .unwrap_or(path)
                .to_string_lossy()
                .into_owned();
            let body = serde_json::json!({ "absPath": abs, "size": size }).to_string();
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "application/json".to_string()),
                    ("x-file-binary".parse().unwrap(), "true".to_string()),
                    ("x-file-size".parse().unwrap(), size.to_string()),
                ],
                body,
            )
                .into_response()
        }
    }
}

/// `PUT /console/api/projects/{name}/file?path=...` — save (create/overwrite).
async fn project_file_save(
    Path(name): Path<String>,
    Query(q): Query<FilePathQuery>,
    body: String,
) -> Response {
    let Some(path) = projects::safe_project_path(&name, &q.path) else {
        return (StatusCode::BAD_REQUEST, "invalid path").into_response();
    };
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    match std::fs::write(&path, &body) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not save file: {e}"),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateProjectPathBody {
    path: String,
    /// Create a directory instead of an empty file.
    #[serde(default)]
    dir: bool,
}

/// `POST /console/api/projects/{name}/file` — create an empty file or a folder.
async fn project_path_create(
    Path(name): Path<String>,
    Json(body): Json<CreateProjectPathBody>,
) -> Response {
    let Some(path) = projects::safe_project_path(&name, &body.path) else {
        return (StatusCode::BAD_REQUEST, "invalid path").into_response();
    };
    if path.exists() {
        return (StatusCode::CONFLICT, "that path already exists").into_response();
    }
    let res = if body.dir {
        std::fs::create_dir_all(&path)
    } else {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        std::fs::write(&path, "")
    };
    match res {
        Ok(()) => StatusCode::CREATED.into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not create: {e}"),
        )
            .into_response(),
    }
}

/// `DELETE /console/api/projects/{name}/file?path=...` — remove a file or folder.
async fn project_path_delete(Path(name): Path<String>, Query(q): Query<FilePathQuery>) -> Response {
    let Some(path) = projects::safe_project_path(&name, &q.path) else {
        return (StatusCode::BAD_REQUEST, "invalid path").into_response();
    };
    let res = if path.is_dir() {
        std::fs::remove_dir_all(&path)
    } else {
        std::fs::remove_file(&path)
    };
    match res {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            (StatusCode::NOT_FOUND, "no such path").into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not delete: {e}"),
        )
            .into_response(),
    }
}

/// `POST /console/api/projects/{name}/run` — deploy + start the application.
async fn project_run(Path(name): Path<String>) -> Response {
    let sup = projects::supervisor();
    match sup.run(&name).await {
        Ok(()) => Json(sup.run_state(&name).await).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

/// `POST /console/api/projects/{name}/stop` — stop the application.
async fn project_stop(Path(name): Path<String>) -> Response {
    let sup = projects::supervisor();
    match sup.stop(&name).await {
        Ok(()) => Json(sup.run_state(&name).await).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CompileBody {
    /// Deno target triples to cross-compile for. Empty = host only.
    #[serde(default)]
    targets: Vec<String>,
}

/// `POST /console/api/projects/{name}/compile` — compile the project (host or
/// cross-compile). Runs in the background; progress streams over the log SSE.
async fn project_compile(Path(name): Path<String>, Json(body): Json<CompileBody>) -> Response {
    if projects::read_config(&name).is_none() {
        return (StatusCode::NOT_FOUND, "no such project").into_response();
    }
    let targets = body.targets;
    tokio::spawn(async move {
        let _ = projects::supervisor().compile(&name, &targets).await;
    });
    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "started": true })),
    )
        .into_response()
}

/// Query for `project_export`: include compiled `dist/` binaries (default off,
/// since they are large and platform-specific).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExportQuery {
    #[serde(default)]
    dist: bool,
}

/// `GET /console/api/projects/{name}/export[?dist=true]` — download the project
/// as a zip. Source-only by default; pass `dist=true` to bundle compiled
/// binaries from `dist/`.
async fn project_export(Path(name): Path<String>, Query(q): Query<ExportQuery>) -> Response {
    match projects::export_zip(&name, q.dist) {
        Ok(zip) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/zip".to_string()),
                (
                    header::CONTENT_DISPOSITION,
                    format!(
                        "attachment; filename=\"{}\"",
                        projects::export_filename(&name)
                    ),
                ),
                (header::CACHE_CONTROL, "no-store".to_string()),
            ],
            zip,
        )
            .into_response(),
        Err(e) if e.contains("no such") => (StatusCode::NOT_FOUND, e).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

/// SSE state for a project's run/compile log stream.
enum ProjLogState {
    History(
        std::vec::IntoIter<projects::LogLine>,
        broadcast::Receiver<projects::LogLine>,
    ),
    Live(broadcast::Receiver<projects::LogLine>),
}

/// `GET /console/api/projects/{name}/logs` — SSE stream of run/compile output.
async fn project_logs(
    Path(name): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let sup = projects::supervisor();
    let history = sup.log_history(&name).await;
    let rx = sup.subscribe(&name).await;
    let stream = unfold(
        ProjLogState::History(history.into_iter(), rx),
        |st| async move {
            match st {
                ProjLogState::History(mut it, rx) => match it.next() {
                    Some(line) => Some((Ok(proj_log_event(&line)), ProjLogState::History(it, rx))),
                    None => proj_recv_live(rx).await,
                },
                ProjLogState::Live(rx) => proj_recv_live(rx).await,
            }
        },
    );
    Sse::new(stream).keep_alive(KeepAlive::default())
}

fn proj_log_event(line: &projects::LogLine) -> Event {
    let data = serde_json::to_string(line).unwrap_or_else(|_| "{}".to_string());
    Event::default().event("log").data(data)
}

async fn proj_recv_live(
    mut rx: broadcast::Receiver<projects::LogLine>,
) -> Option<(Result<Event, Infallible>, ProjLogState)> {
    loop {
        match rx.recv().await {
            Ok(line) => return Some((Ok(proj_log_event(&line)), ProjLogState::Live(rx))),
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => return None,
        }
    }
}
