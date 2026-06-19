//! Peer uplink: a node acting as a command-stream **client** to its cluster
//! peers.
//!
//! Stage 1 of the distributed-scaling design (`docs/distributed-scaling-design.md`)
//! makes every node a gateway: a client connects to any one node, which forwards
//! operations it does not own to the node that does. The forwarding transport is
//! the existing command-stream WebSocket protocol — a gateway opens a
//! [`PeerLink`] to each peer and drives that peer's engine over the same frames
//! (`CreateInstance`, `CompleteJob`, …) a normal client would send, so no new
//! node-to-node protocol or serialization is introduced.
//!
//! This module provides the transport only: a connection to one peer plus a
//! correlated request/response over it ([`PeerLink::request`]). Routing the
//! right operations to the right peer (create-forward, by-key forward, deploy
//! broadcast, job aggregation) is layered on top in the forwarding seam.
//!
//! Scope of this increment: unary request/response (`CommandResult`-answered
//! frames). Job-push subscription aggregation (`Subscribe`/`Job`) and async
//! `InstanceCompleted` await routing build on the same link later.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use futures_util::SinkExt;
use futures_util::stream::StreamExt;
use serde_json::Value;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

use crate::cluster::Topology;
use crate::command_stream::{ClientFrame, ReadKind, ServerFrame, UserTaskOp};

/// Default ceiling on how long a forwarded request waits for its peer's
/// `CommandResult` before giving up. Overridable via `NANOBPMN_PEER_TIMEOUT_MS`.
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;

/// A failure forwarding a request to a peer.
#[derive(Debug)]
pub enum PeerError {
    /// The WebSocket to the peer could not be established.
    Connect(String),
    /// The link closed (peer down, network drop) before the response arrived.
    Closed,
    /// No response within the request timeout.
    Timeout,
}

impl std::fmt::Display for PeerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerError::Connect(e) => write!(f, "peer connect failed: {e}"),
            PeerError::Closed => write!(f, "peer link closed before response"),
            PeerError::Timeout => write!(f, "peer request timed out"),
        }
    }
}

impl std::error::Error for PeerError {}

/// A peer's answer to a forwarded unary command — the `status`/`body` of its
/// `CommandResult` frame, mapped straight back to the originating client.
#[derive(Debug, Clone)]
pub struct PeerResult {
    pub status: u16,
    pub body: Option<Value>,
}

/// Outstanding forwarded requests awaiting a `CommandResult`, keyed by `corr`.
type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<PeerResult>>>>;

/// A live command-stream connection to one peer node.
///
/// Cheap to clone (shares the underlying socket writer and correlation table).
/// Allocates its own `corr` space, independent of the peer's other clients.
#[derive(Clone)]
pub struct PeerLink {
    out: mpsc::Sender<Message>,
    pending: Pending,
    next_corr: Arc<AtomicU64>,
    connected: Arc<AtomicBool>,
}

impl PeerLink {
    /// Opens a command-stream client connection to `base_url` (a peer's HTTP base
    /// URL, e.g. `http://10.0.0.2:8080`). The reader/writer tasks run until the
    /// socket closes, at which point the link is marked disconnected and every
    /// outstanding request fails with [`PeerError::Closed`].
    pub async fn connect(base_url: &str) -> Result<Self, PeerError> {
        let ws_url = ws_url(base_url);
        let (ws, _resp) = tokio_tungstenite::connect_async(&ws_url)
            .await
            .map_err(|e| PeerError::Connect(e.to_string()))?;
        let (mut sink, mut stream) = ws.split();

        let (out, mut out_rx) = mpsc::channel::<Message>(1024);
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let connected = Arc::new(AtomicBool::new(true));

        // Writer: serialize outbound frames onto the socket in submission order.
        tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                if sink.send(msg).await.is_err() {
                    break;
                }
            }
            let _ = sink.close().await;
        });

        // Reader: decode peer frames and resolve the matching pending request.
        let reader_pending = pending.clone();
        let reader_connected = connected.clone();
        tokio::spawn(async move {
            while let Some(frame) = stream.next().await {
                match frame {
                    Ok(Message::Text(txt)) => {
                        if let Ok(sf) = serde_json::from_str::<ServerFrame>(&txt) {
                            route_server_frame(sf, &reader_pending).await;
                        }
                    }
                    Ok(Message::Binary(bin)) => {
                        if let Ok(sf) = serde_json::from_slice::<ServerFrame>(&bin) {
                            route_server_frame(sf, &reader_pending).await;
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    // Ping/Pong/frame: tungstenite answers pings itself.
                    Ok(_) => {}
                }
            }
            // Link is down: fail every waiter so callers don't hang.
            reader_connected.store(false, Ordering::Relaxed);
            let mut map = reader_pending.lock().await;
            for (_, tx) in map.drain() {
                let _ = tx.send(PeerResult {
                    status: 502,
                    body: Some(Value::String("peer link closed".into())),
                });
            }
        });

        Ok(Self {
            out,
            pending,
            next_corr: Arc::new(AtomicU64::new(1)),
            connected,
        })
    }

    /// Whether the link is still up.
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    /// Forwards a unary command and awaits the peer's `CommandResult`. `build`
    /// receives the freshly-allocated `corr` and must stamp it onto the frame so
    /// the response can be matched back. Fails if the link is down, the frame
    /// cannot be queued, or no response arrives within the request timeout.
    pub async fn request<F>(&self, build: F) -> Result<PeerResult, PeerError>
    where
        F: FnOnce(u64) -> ClientFrame,
    {
        if !self.is_connected() {
            return Err(PeerError::Closed);
        }
        let corr = self.next_corr.fetch_add(1, Ordering::Relaxed);
        let frame = build(corr);
        let txt = serde_json::to_string(&frame).expect("ClientFrame serializes");

        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(corr, tx);

        if self.out.send(Message::Text(txt.into())).await.is_err() {
            self.pending.lock().await.remove(&corr);
            return Err(PeerError::Closed);
        }

        match tokio::time::timeout(request_timeout(), rx).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(PeerError::Closed),
            Err(_) => {
                self.pending.lock().await.remove(&corr);
                Err(PeerError::Timeout)
            }
        }
    }

    /// Forwards a `createProcessInstance` to this peer (it creates on one of its
    /// own partitions). `await_completion` is intentionally unsupported here —
    /// it resolves over an async `InstanceCompleted` frame, wired in a later
    /// increment.
    pub async fn create_instance(
        &self,
        process_definition_id: Option<String>,
        process_definition_key: Option<String>,
        variables: Option<serde_json::Map<String, Value>>,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::CreateInstance {
            corr,
            process_definition_id,
            process_definition_key,
            variables,
            await_completion: Some(false),
            fetch_variables: None,
            request_timeout: None,
        })
        .await
    }

    /// Forwards a `completeJob` to the peer that owns the job's partition.
    pub async fn complete_job(
        &self,
        job_key: String,
        variables: Option<serde_json::Map<String, Value>>,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::CompleteJob {
            corr,
            job_key,
            variables,
        })
        .await
    }

    /// Asks the peer to activate up to `max_jobs` of `job_type` on its OWN
    /// partitions for `worker` (job-stream aggregation). The peer leases the jobs
    /// under `timeout` and replies a `CommandResult` whose body is the JSON array
    /// of `ActivatedJobResult`.
    pub async fn activate_jobs(
        &self,
        job_type: String,
        worker: String,
        max_jobs: i64,
        timeout: u64,
        fetch_variable: Option<Vec<String>>,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::ActivateJobs {
            corr,
            job_type,
            worker,
            max_jobs,
            timeout: Some(timeout),
            fetch_variable,
        })
        .await
    }

    /// Forwards a GET-by-key read to the peer that owns the key's partition
    /// (query forwarding). The peer answers from its local read model.
    pub async fn get_by_key(&self, kind: ReadKind, key: u64) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::GetByKey { corr, kind, key })
            .await
    }

    /// Forwards a user-task by-key mutation to the peer that owns the task's
    /// partition. `payload` is the original REST request body.
    pub async fn forward_user_task(
        &self,
        op: UserTaskOp,
        user_task_key: String,
        payload: Option<Value>,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::ForwardUserTask {
            corr,
            op,
            user_task_key,
            payload,
        })
        .await
    }

    /// Forwards a client deploy to this peer (the deployment-partition owner),
    /// which processes it centrally and broadcasts it. Used when a gateway that
    /// does not own partition 0 receives a deploy.
    pub async fn deploy(
        &self,
        resources: Vec<(String, String)>,
        tenant_id: Option<String>,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::Deploy {
            corr,
            resources,
            tenant_id,
        })
        .await
    }

    /// Broadcasts an already-minted deployment's `ProcessDeployed` events to this
    /// peer so it durably installs the definition(s) on its owned partitions.
    pub async fn install_deployment(
        &self,
        events: Vec<nanobpmn_engine_core::Event>,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::InstallDeployment { corr, events })
            .await
    }

    /// Fans a published message out to this peer so it correlates against the
    /// subscriptions on its owned partitions. The peer answers with
    /// `{messageKey, correlatedInstanceKey}`.
    pub async fn publish_message(
        &self,
        name: String,
        correlation_key: String,
        variables: Option<serde_json::Map<String, Value>>,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::PublishMessage {
            corr,
            name,
            correlation_key,
            variables,
        })
        .await
    }

    /// Forwards a `failJob` to the peer that owns the job's partition.
    pub async fn fail_job(
        &self,
        job_key: String,
        retries: i32,
        error_message: String,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::FailJob {
            corr,
            job_key,
            retries: Some(retries),
            error_message: Some(error_message),
        })
        .await
    }

    /// Forwards a `throwError` to the peer that owns the job's partition.
    pub async fn throw_error(
        &self,
        job_key: String,
        error_code: String,
        error_message: String,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::ThrowError {
            corr,
            job_key,
            error_code,
            error_message: Some(error_message),
        })
        .await
    }

    /// Forwards a `cancelProcessInstance` to the peer that owns the instance.
    pub async fn cancel_instance(&self, instance_key: String) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::CancelInstance { corr, instance_key })
            .await
    }

    /// Forwards a cross-partition subscription follow-up event (a
    /// `MessageSubscriptionOpening` or `RemoteMessageCorrelation`) to the peer
    /// that owns the target partition, so it applies the corresponding routed
    /// command on its own engine. Answered by a `CommandResult` (200).
    pub async fn route_subscription(
        &self,
        event: nanobpmn_engine_core::Event,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::RouteSubscription { corr, event })
            .await
    }

    /// Forwards a job-retries update to the peer that owns the job's partition.
    pub async fn update_job_retries(
        &self,
        job_key: String,
        retries: i32,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::UpdateJobRetries {
            corr,
            job_key,
            retries,
        })
        .await
    }

    /// Forwards an incident resolution to the peer that owns the incident.
    pub async fn resolve_incident(
        &self,
        incident_key: String,
        operation_reference: Option<i64>,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::ResolveIncident {
            corr,
            incident_key,
            operation_reference,
        })
        .await
    }

    /// Forwards a by-key variable merge to the peer that owns the scope.
    pub async fn set_variables(
        &self,
        scope_key: String,
        variables: Option<serde_json::Map<String, Value>>,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::SetVariables {
            corr,
            scope_key,
            variables,
        })
        .await
    }

    /// Forwards a `createProcessInstance` to this peer for cluster-wide create
    /// placement. The peer creates on one of its own partitions and answers with
    /// the full `CreateProcessInstanceResult` JSON.
    #[allow(clippy::too_many_arguments)]
    pub async fn forward_create(
        &self,
        process_definition_id: Option<String>,
        process_definition_key: Option<String>,
        variables: Option<serde_json::Map<String, Value>>,
        tags: Vec<String>,
        business_id: Option<String>,
        await_completion: bool,
        fetch_variables: Option<Vec<String>>,
        request_timeout: Option<i64>,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::ForwardCreate {
            corr,
            process_definition_id,
            process_definition_key,
            variables,
            tags,
            business_id,
            await_completion,
            fetch_variables,
            request_timeout,
        })
        .await
    }
}

/// Resolves a `CommandResult`/`InstanceCompleted` to its waiting request; other
/// frames (job pushes, credits, heartbeats) are not used by the unary uplink.
async fn route_server_frame(frame: ServerFrame, pending: &Pending) {
    match frame {
        ServerFrame::CommandResult { corr, status, body } => {
            if let Some(tx) = pending.lock().await.remove(&corr) {
                let _ = tx.send(PeerResult { status, body });
            }
        }
        // Async await-completion: surfaces as a 200 with the completion payload.
        ServerFrame::InstanceCompleted {
            corr,
            process_instance_key,
            process_completed,
            variables,
        } => {
            if let Some(tx) = pending.lock().await.remove(&corr) {
                let _ = tx.send(PeerResult {
                    status: 200,
                    body: Some(serde_json::json!({
                        "processInstanceKey": process_instance_key,
                        "processCompleted": process_completed,
                        "variables": variables,
                    })),
                });
            }
        }
        _ => {}
    }
}

/// The set of command-stream uplinks to a node's cluster peers, built from the
/// [`Topology`]. The forwarding seam asks it for the link to a partition's owning
/// node; links are established lazily on first use and re-established
/// transparently after a drop, so a peer that is briefly down does not need a
/// restart to rejoin.
///
/// A single-node cluster has no peers, so this is empty and never dialed —
/// preserving the zero-overhead single-node path.
#[derive(Clone)]
pub struct PeerSet {
    topology: Topology,
    /// One slot per node id; `Some` once a link has been established. Guarded by
    /// an async mutex so concurrent forwards to the same peer share one dial.
    links: Arc<Mutex<HashMap<u32, PeerLink>>>,
}

impl PeerSet {
    /// Builds the uplink set for `topology`. No connections are opened until a
    /// peer is first needed.
    pub fn new(topology: Topology) -> Self {
        Self {
            topology,
            links: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Whether this node has any peers (false for a single-node cluster).
    pub fn has_peers(&self) -> bool {
        !self.topology.is_single_node()
    }

    /// Returns a live link to peer `node_id`, dialing it if there is no cached
    /// link or the cached one has dropped. Concurrent callers for the same peer
    /// share the single in-flight dial (serialized by the map lock).
    pub async fn link(&self, node_id: u32) -> Result<PeerLink, PeerError> {
        let mut links = self.links.lock().await;
        if let Some(existing) = links.get(&node_id) {
            if existing.is_connected() {
                return Ok(existing.clone());
            }
            // Stale link (peer dropped): discard and redial below.
            links.remove(&node_id);
        }
        let addr = self
            .topology
            .peer_addr(node_id)
            .ok_or_else(|| PeerError::Connect(format!("no address for node {node_id}")))?;
        let link = PeerLink::connect(addr).await?;
        links.insert(node_id, link.clone());
        Ok(link)
    }
}

/// Maps a peer's HTTP base URL to its command-stream WebSocket URL.
/// `http://h:p` → `ws://h:p/command-stream`, `https://…` → `wss://…`.
fn ws_url(base_url: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    let ws_base = if let Some(rest) = trimmed.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = trimmed.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        // Assume a bare host:port is plaintext.
        format!("ws://{trimmed}")
    };
    format!("{ws_base}/command-stream")
}

fn request_timeout() -> Duration {
    let ms = std::env::var("NANOBPMN_PEER_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_REQUEST_TIMEOUT_MS);
    Duration::from_millis(ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ws_url_maps_scheme_and_appends_path() {
        assert_eq!(ws_url("http://10.0.0.2:8080"), "ws://10.0.0.2:8080/command-stream");
        assert_eq!(ws_url("http://10.0.0.2:8080/"), "ws://10.0.0.2:8080/command-stream");
        assert_eq!(ws_url("https://node:443"), "wss://node:443/command-stream");
        assert_eq!(ws_url("host:9000"), "ws://host:9000/command-stream");
    }

    /// Serves a real command-stream endpoint on an ephemeral port and returns its
    /// HTTP base URL. Models a peer node: a `PeerLink` connects to it exactly as
    /// a forwarding gateway would in a cluster.
    async fn serve_peer() -> String {
        let server = crate::ServerImpl::default();
        let registry = crate::command_stream::Registry::new();
        crate::command_stream::spawn_dispatcher(server.clone(), registry.clone());
        let app = crate::command_stream::router(server, registry);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let port = listener.local_addr().expect("local addr").port();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://127.0.0.1:{port}")
    }

    /// The uplink drives a peer's engine over the command stream: a forwarded
    /// `createProcessInstance` runs on the peer and its `CommandResult` is mapped
    /// straight back. This is the transport every stage-1 forwarding op rides on.
    #[tokio::test]
    async fn peer_link_forwards_create_instance() {
        let base = serve_peer().await;
        let link = PeerLink::connect(&base).await.expect("connect to peer");
        assert!(link.is_connected());

        let res = link
            .create_instance(Some("demo".to_string()), None, None)
            .await
            .expect("forwarded create returns a result");
        assert_eq!(res.status, 200, "peer should accept the forwarded create");
        let body = res.body.expect("create result carries a body");
        assert!(
            body.get("processInstanceKey").is_some(),
            "result should carry the peer-minted processInstanceKey, got {body}"
        );
    }

    /// Two independent forwarded creates get distinct correlation ids and both
    /// resolve — proving the correlation table routes responses correctly.
    #[tokio::test]
    async fn peer_link_correlates_concurrent_requests() {
        let base = serve_peer().await;
        let link = PeerLink::connect(&base).await.expect("connect to peer");

        let (a, b) = tokio::join!(
            link.create_instance(Some("demo".to_string()), None, None),
            link.create_instance(Some("demo".to_string()), None, None),
        );
        let ka = a.expect("first create").body.unwrap();
        let kb = b.expect("second create").body.unwrap();
        assert_ne!(
            ka.get("processInstanceKey"),
            kb.get("processInstanceKey"),
            "two creates must mint distinct instance keys"
        );
    }

    /// A forwarded request to an unreachable peer fails cleanly (connect error),
    /// never hangs.
    #[tokio::test]
    async fn peer_link_connect_failure_is_reported() {
        // Port 1 is privileged/unused — connect must fail, not hang.
        let err = PeerLink::connect("http://127.0.0.1:1").await;
        assert!(matches!(err, Err(PeerError::Connect(_))));
    }

    /// `PeerSet` dials a peer lazily from the topology address and reuses the live
    /// link on the next call — the connection manager every forwarding op uses.
    #[tokio::test]
    async fn peer_set_dials_lazily_and_forwards() {
        let peer_base = serve_peer().await;
        // Node 0 of a 2-node cluster; node 1 is the served peer.
        let topology = crate::cluster::Topology {
            node_id: 0,
            peers: vec!["http://self-unused".to_string(), peer_base],
            num_partitions: 4,
            replication_factor: 1,
        };
        let peers = PeerSet::new(topology);
        assert!(peers.has_peers());

        let link = peers.link(1).await.expect("dial peer node 1");
        let res = link
            .create_instance(Some("demo".to_string()), None, None)
            .await
            .expect("forwarded create");
        assert_eq!(res.status, 200);

        // Second call reuses the cached, still-connected link (no redial).
        let link2 = peers.link(1).await.expect("reuse cached link");
        assert!(link2.is_connected());
    }

    /// A single-node cluster has no peers, so `PeerSet` is inert.
    #[tokio::test]
    async fn peer_set_single_node_has_no_peers() {
        let peers = PeerSet::new(crate::cluster::Topology::single(1));
        assert!(!peers.has_peers());
        assert!(peers.link(0).await.is_err(), "single node has no peer to dial");
    }
}

