//! # The Falcon Protocol
//!
//! Nano's Falcon protocol: a unified, bidirectional, credit-metered WebSocket
//! (§13 of `docs/falcon-design.md`).
//!
//! Named for **Falko Menge**, whose work on system optimisation is the genesis
//! and inspiration for this subsystem. Artists sign their work.
//!
//! One persistent socket per client multiplexes two interaction patterns over a
//! single credit-coordinated window onto the one engine thread:
//!
//! * **demand/push (jobs):** the client `Subscribe`s to a job type with a credit
//!   count; a single server-side dispatcher reacts to `jobs_available`, leases
//!   jobs round-robin across subscribers (reusing the REST activation + off-thread
//!   variable encoding path), and pushes `Job` frames while credits remain. Lease
//!   `deadline` expiry (the existing periodic tick) reclaims any job pushed to a
//!   worker that never completes it — the lease *is* the at-least-once guarantee,
//!   so a dropped socket needs no special handling.
//! * **request/response (writes):** `CreateInstance` / `CompleteJob` / `FailJob`
//!   / `ThrowError` funnel to the same engine command path as the REST handlers,
//!   each answered by a `corr`-correlated `CommandResult`. `CreateInstance` is
//!   metered by a **submission-credit** lane fed from the engine's `processing`
//!   headroom via the existing backpressure controller — under saturation the
//!   server withholds credits and the client stalls intake (no 503, no retry,
//!   no herd). Completing jobs flows unmetered (draining backlog must never be
//!   throttled). `awaitCompletion` becomes an async `InstanceCompleted` frame
//!   rather than a held request. After a reconnect a client can re-request that
//!   outcome with `AwaitInstance` (carrying a `processInstanceKey` it persisted
//!   from the original create), which resolves immediately for an already-terminal
//!   instance since the read model is durable history.
//!
//! The engine core is untouched: this is purely a new ingress to the existing
//! command path and a new consumer of `activate_jobs`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::Response;
use axum::routing::get;
use futures_util::SinkExt;
use futures_util::stream::StreamExt;
use nanobpmn_engine_core::Event;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::Notify;
use tokio::sync::mpsc;

use crate::ServerImpl;
use crate::journal::Commit;

/// Per-connection submission-credit window (creates the client may have in flight
/// before it must wait for the server to replenish). Overridable via
/// `NANOBPMN_STREAM_SUBMISSION_WINDOW`.
const DEFAULT_SUBMISSION_WINDOW: i64 = 256;
/// Dispatcher backstop interval (the §8 sweep): re-drains subscriptions to catch
/// jobs predating a subscription or freed by lease expiry, independent of edge
/// `jobs_available` wakes.
const DISPATCH_TICK_MS: u64 = 200;
/// Keepalive cadence on an idle socket.
const HEARTBEAT_MS: u64 = 15_000;
/// Max jobs leased to a single stream per dispatch tick, so a high-credit worker
/// cannot starve its peers between round-robin rotations.
const PER_STREAM_BATCH: usize = 64;
/// Default number of connections serviced concurrently per dispatch pass (see
/// [`dispatch_concurrency`]). Chosen to keep several activation round-trips in
/// flight per partition engine thread so they stay busy, while bounding the
/// High-priority activation load so it cannot crowd out completions.
const DEFAULT_DISPATCH_CONCURRENCY: usize = 64;
/// A connection is reaped as a phantom when no frame (the client's own heartbeat
/// included) has arrived for this long. Three missed client heartbeats: long
/// enough to tolerate a GC pause or a brief network blip, short enough to free a
/// dispatch slot before the 60s job lock would even expire.
const LIVENESS_TIMEOUT_MS: u64 = 3 * HEARTBEAT_MS;
/// How often the reaper scans connections for liveness.
const REAPER_INTERVAL_MS: u64 = 5_000;
/// Default job-lock duration applied when a `Subscribe` omits `timeout` (or sends
/// a non-positive one). A zero lock makes every leased job instantly re-activatable
/// (`deadline == now`), so the dispatcher re-pushes it on the next pass before the
/// worker's completion lands — manifesting as duplicate delivery. Mirrors the SDK's
/// `jobTimeoutMs` default so a lock is always meaningful regardless of the client.
const DEFAULT_JOB_LOCK_MS: u64 = 60_000;
/// Bound on the per-connection outbound frame buffer (slow-consumer guard).
const OUTBOUND_CHANNEL_CAP: usize = 1024;

type ConnId = u64;

/// One round-robin–ordered dispatch target: a live connection and its
/// subscription for a given job type.
type DispatchTarget = (Arc<Connection>, Arc<Subscription>);

// ----------------------------------------------------------------------------
// Frame protocol (tagged union; wire form is camelCase JSON over text frames).
// ----------------------------------------------------------------------------

/// `serde` `skip_serializing_if` predicate: omit a `bool` field when it is `false`.
fn is_false(b: &bool) -> bool {
    !*b
}

/// Client → server frames.
///
/// `Serialize` is derived so a node can act as a falcon *client* to its
/// cluster peers (the intra-cluster forwarding uplink), speaking the same wire
/// protocol it serves.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ClientFrame {
    /// Opt into job push for a type, granting an initial credit batch.
    #[serde(rename_all = "camelCase")]
    Subscribe {
        job_type: String,
        #[serde(default)]
        job_credits: i64,
        #[serde(default)]
        fetch_variable: Option<Vec<String>>,
        #[serde(default)]
        timeout: Option<u64>,
        #[serde(default)]
        worker: Option<String>,
    },
    /// Replenish job-push demand for a type.
    #[serde(rename_all = "camelCase")]
    JobCredits {
        job_type: String,
        n: i64,
    },
    /// Start a process instance (consumes one submission credit).
    #[serde(rename_all = "camelCase")]
    CreateInstance {
        corr: u64,
        #[serde(default)]
        process_definition_id: Option<String>,
        #[serde(default)]
        process_definition_key: Option<String>,
        #[serde(default)]
        variables: Option<Map<String, Value>>,
        #[serde(default)]
        await_completion: Option<bool>,
        #[serde(default)]
        fetch_variables: Option<Vec<String>>,
        #[serde(default)]
        request_timeout: Option<i64>,
    },
    /// Complete an activated job (unmetered drain).
    #[serde(rename_all = "camelCase")]
    CompleteJob {
        corr: u64,
        job_key: String,
        #[serde(default)]
        variables: Option<Map<String, Value>>,
    },
    /// Fail an activated job (unmetered drain).
    #[serde(rename_all = "camelCase")]
    FailJob {
        corr: u64,
        job_key: String,
        #[serde(default)]
        retries: Option<i32>,
        #[serde(default)]
        error_message: Option<String>,
    },
    /// Throw a BPMN error from an activated job (unmetered drain).
    #[serde(rename_all = "camelCase")]
    ThrowError {
        corr: u64,
        job_key: String,
        error_code: String,
        #[serde(default)]
        error_message: Option<String>,
    },
    /// Re-subscribe to completion of an already-created instance. Used for
    /// recovery: a client that persisted the `processInstanceKey` from an earlier
    /// `CreateInstance` `CommandResult` can, after a reconnect, re-request the
    /// terminal outcome over the new socket. Resolves immediately if the instance
    /// is already terminal (the read model is durable history), so it doubles as a
    /// poll. Answered by an `InstanceCompleted` correlated by `corr`.
    #[serde(rename_all = "camelCase")]
    AwaitInstance {
        corr: u64,
        process_instance_key: String,
        #[serde(default)]
        fetch_variables: Option<Vec<String>>,
        #[serde(default)]
        request_timeout: Option<i64>,
    },
    Heartbeat,
    /// **Intra-cluster only.** A non-deployment-partition gateway forwards a
    /// client's deploy to the partition-0 owner (the deploy authority), which
    /// processes it durably and broadcasts the result to every peer. Answered by
    /// a `corr`-correlated `CommandResult` carrying the deployment JSON (200) or
    /// an error (4xx). See the centralized cluster-deployment model.
    #[serde(rename_all = "camelCase")]
    Deploy {
        corr: u64,
        /// `(resourceName, bpmnXml)` pairs, exactly as received over HTTP.
        resources: Vec<(String, String)>,
        #[serde(default)]
        tenant_id: Option<String>,
    },
    /// **Intra-cluster only.** The deployment-partition owner broadcasts an
    /// already-minted deployment (its `ProcessDeployed` events) to a peer, which
    /// durably installs the definition(s) on its owned partitions without minting
    /// new keys or arming start subscriptions (those stay solely on the
    /// deployment partition). Answered by a `CommandResult` once the install is
    /// committed.
    #[serde(rename_all = "camelCase")]
    InstallDeployment {
        corr: u64,
        events: Vec<Event>,
    },
    /// **Intra-cluster only.** A gateway fans a published message out to this peer
    /// so it correlates the message against the subscriptions on *its* owned
    /// partitions (open message subscriptions are spread across the cluster with
    /// their instances; message-start subscriptions live solely on the
    /// partition-0 owner). Local-only: the peer correlates across its own
    /// partitions and does **not** re-forward, so there is no fan-out loop.
    /// Answered by a `CommandResult` carrying `{messageKey, correlatedInstanceKey}`.
    #[serde(rename_all = "camelCase")]
    PublishMessage {
        corr: u64,
        name: String,
        #[serde(default)]
        correlation_key: String,
        #[serde(default)]
        variables: Option<Map<String, Value>>,
    },
    /// **Intra-cluster only.** A gateway forwards a by-key process-instance
    /// cancellation to the peer that owns the instance's partition. Answered by a
    /// `CommandResult` (204 on success, 404 unknown instance, 4xx/5xx otherwise).
    #[serde(rename_all = "camelCase")]
    CancelInstance {
        corr: u64,
        instance_key: String,
    },
    /// **Intra-cluster only.** Routes a cross-partition subscription follow-up
    /// (a `MessageSubscriptionOpening` opening a canonical subscription on its
    /// `hash(correlationKey)` partition, or a `RemoteMessageCorrelation` advancing
    /// a parked token on its instance's partition) to the node owning the target
    /// partition. The owner applies the corresponding routed command on its own
    /// engine and drives its own pump for any further follow-ups (which may route
    /// on again), so there is no central fan-out. Answered by a `CommandResult`
    /// (200) once applied + durable.
    #[serde(rename_all = "camelCase")]
    RouteSubscription {
        corr: u64,
        event: Event,
    },
    /// **Intra-cluster only.** A gateway forwards a by-key job-retries update to
    /// the peer that owns the job's partition. Answered by a `CommandResult`.
    #[serde(rename_all = "camelCase")]
    UpdateJobRetries {
        corr: u64,
        job_key: String,
        retries: i32,
    },
    /// **Intra-cluster only.** A gateway forwards a by-key incident resolution to
    /// the peer that owns the incident's partition. Answered by a `CommandResult`.
    #[serde(rename_all = "camelCase")]
    ResolveIncident {
        corr: u64,
        incident_key: String,
        #[serde(default)]
        operation_reference: Option<i64>,
    },
    /// **Intra-cluster only.** A gateway forwards a by-key variable merge to the
    /// peer that owns the scope's partition. Answered by a `CommandResult`.
    #[serde(rename_all = "camelCase")]
    SetVariables {
        corr: u64,
        scope_key: String,
        #[serde(default)]
        variables: Option<Map<String, Value>>,
        #[serde(default)]
        local: bool,
    },
    /// **Intra-cluster only.** A gateway asks a peer to activate jobs on the peer's
    /// own partitions for a worker attached to the gateway (job-stream
    /// aggregation). The peer leases the jobs under `timeout` and replies a
    /// `CommandResult` whose body is the JSON array of `ActivatedJobResult`. The
    /// peer activates locally only (its engine owns just its partitions), so there
    /// is no fan-out loop. At-least-once is preserved by the peer's lock: if the
    /// gateway dies before the worker completes, the lease expires and the job
    /// re-activates on its owner.
    #[serde(rename_all = "camelCase")]
    ActivateJobs {
        corr: u64,
        job_type: String,
        worker: String,
        max_jobs: i64,
        #[serde(default)]
        timeout: Option<u64>,
        #[serde(default)]
        fetch_variable: Option<Vec<String>>,
    },
    /// **Intra-cluster only.** A gateway forwards a `createProcessInstance` to a
    /// peer for cluster-wide create placement (the gateway round-robins creates
    /// across every partition; a partition owned by a peer is placed via this
    /// frame). The peer creates on one of *its own* partitions and does **not**
    /// re-forward (local-only, no placement loop). Answered by a `CommandResult`
    /// carrying the full `CreateProcessInstanceResult` JSON (200) or an error.
    #[serde(rename_all = "camelCase")]
    ForwardCreate {
        corr: u64,
        #[serde(default)]
        process_definition_id: Option<String>,
        #[serde(default)]
        process_definition_key: Option<String>,
        #[serde(default)]
        variables: Option<Map<String, Value>>,
        #[serde(default)]
        tags: Vec<String>,
        #[serde(default)]
        business_id: Option<String>,
        #[serde(default)]
        await_completion: bool,
        #[serde(default)]
        fetch_variables: Option<Vec<String>>,
        #[serde(default)]
        request_timeout: Option<i64>,
    },
    /// **Intra-cluster only.** A gateway forwards a GET-by-key read to the peer
    /// that owns the key's partition. Each node only projects its own partitions
    /// into its read model, so a read for a remote key must be answered by the
    /// owner. Answered by a `CommandResult` carrying the entity JSON (200) or 404.
    #[serde(rename_all = "camelCase")]
    GetByKey {
        corr: u64,
        kind: ReadKind,
        key: u64,
    },
    /// **Intra-cluster only.** A gateway forwards a user-task by-key mutation
    /// (assign / complete / unassign / update) to the peer that owns the task's
    /// partition. `payload` is the original REST request body, re-applied locally
    /// by the owner. Answered by a `CommandResult` whose status mirrors the REST
    /// outcome (204 / 404 / 409 / 500).
    #[serde(rename_all = "camelCase")]
    ForwardUserTask {
        corr: u64,
        op: UserTaskOp,
        user_task_key: String,
        #[serde(default)]
        payload: Option<Value>,
    },
    /// **Intra-cluster only.** Carries one serialized Raft RPC (AppendEntries /
    /// Vote / InstallSnapshot) for `partition`'s replica group to the node hosting
    /// it. Answered by a `CommandResult` whose body is the serialized
    /// `RaftRpcResponse` (200) or an error status. This is the falcon
    /// transport for per-partition Raft (stage 3 leader routing).
    Raft {
        corr: u64,
        partition: u64,
        /// The `RaftRpcRequest` serialized to a JSON string and carried verbatim,
        /// so the heavy AppendEntries / InstallSnapshot payloads (journal events +
        /// variables) are encoded once on the sender and parsed once on the
        /// receiver — never materialized into an intermediate `serde_json::Value`
        /// DOM (which, for a large variables map, would allocate a node per field).
        /// When `zip` is set the string is raw-deflate + base64 (large payloads
        /// only — see [`crate::raft_net::encode_rpc_payload`]).
        rpc: String,
        /// Whether `rpc` is deflate+base64 compressed. Absent ⇒ `false` (raw JSON),
        /// so small control RPCs add no field and older raw frames still parse.
        #[serde(default, skip_serializing_if = "is_false")]
        zip: bool,
    },
    /// **Intra-cluster only.** A best-effort soft lease digest broadcast by a
    /// partition leader to its followers (leader-local activation, `digest` mode).
    /// Fire-and-forget: it carries the leader's currently-held activation leases
    /// `(jobKey, deadline)` for `partition` so that a follower, on being promoted
    /// to leader, can recover them ([`Engine::recover_lease`]) and honour each
    /// deadline before redelivering — narrowing the failover redelivery window
    /// that leader-local activation otherwise opens. It is **not** answered (no
    /// `corr`); a dropped or stale digest only costs a slightly wider window, so
    /// loss is safe by construction.
    #[serde(rename_all = "camelCase")]
    LeaseDigest {
        partition: u64,
        leases: Vec<(u64, u64)>,
        sent_at: u64,
    },
    /// Leader-durable auto-recovery announcement (ADR 0003): the sender has
    /// app-promoted itself leader of `partition` at `epoch` after detecting the
    /// previous (sole-voter) leader was lost. Recipients adopt the higher epoch,
    /// rejoin the partition as a learner of the new leader (so new writes ship to
    /// them again), and a stale leader at a lower epoch steps down (fencing).
    /// Fire-and-forget (no `corr`): the promoting leader retries `add_learner`, so a
    /// dropped announcement only delays a survivor's rejoin, never correctness.
    #[serde(rename_all = "camelCase")]
    Promote {
        partition: u64,
        epoch: u64,
        leader_node: u64,
        leader_addr: String,
    },
    /// **Intra-cluster only.** An operator switched the runtime SLA mode on one
    /// node (via the console SLA knob); that node fans the new mode out to every
    /// peer so the whole cluster applies a single, uniform admission policy (a
    /// split — some nodes shedding to preserve latency while others keep admitting
    /// — would make client behaviour depend on which node routed the create).
    /// `mode` is the [`crate::backpressure::SlaMode`] string (`"latency"` /
    /// `"admission"`). Fire-and-forget (no `corr`): the recipient applies it
    /// locally and does **not** re-broadcast (no fan-out loop); a briefly
    /// unreachable node keeps its prior mode until the next switch or a restart
    /// (which reseeds from `NANOBPMN_SLA_MODE`), which is safe for an operational
    /// knob.
    #[serde(rename_all = "camelCase")]
    SetSlaMode {
        mode: String,
    },
    /// Create-placement load gossip (ADR 0014 `balanced`): the sending node's
    /// current composite create-load index, broadcast periodically to every peer
    /// so their weighted placement can steer creates toward nodes with headroom.
    /// `node` is the sender's node id; `load` is its
    /// [`create_load_index`](crate::ServerImpl::create_load_index) (a shedding
    /// node reports [`SHED_LOAD`](crate::placement::SHED_LOAD)). Fire-and-forget
    /// (no `corr`), not re-broadcast: a stale/absent report is treated as full
    /// headroom, and the reactive shed/reroute layer is the correctness backstop.
    #[serde(rename_all = "camelCase")]
    PressureReport {
        node: u32,
        load: i64,
    },
}

/// The kind of entity a [`ClientFrame::GetByKey`] read targets, selecting which
/// read-model lookup the owning peer runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ReadKind {
    ProcessInstance,
    Incident,
    UserTask,
    Variable,
}

/// The user-task mutation a [`ClientFrame::ForwardUserTask`] carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum UserTaskOp {
    Assign,
    Complete,
    Unassign,
    Update,
}
///
/// `Deserialize` is derived so the peer uplink (a node acting as a falcon
/// client to its cluster peers) can decode a peer's responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ServerFrame {
    /// Sent once on connect: the initial submission window and heartbeat cadence.
    #[serde(rename_all = "camelCase")]
    Welcome {
        submission_credits: i64,
        heartbeat_ms: u64,
    },
    /// A pushed activated job (consumes one job-delivery credit). `job` is the
    /// same shape as a REST `ActivatedJobResult`, carried as a pre-serialized raw
    /// JSON value so the hot dispatch path serializes each job exactly once (no
    /// intermediate `serde_json::Value` tree) and the writer emits it verbatim.
    Job {
        job: Box<serde_json::value::RawValue>,
    },
    /// Ack/result for a create/complete/fail/throwError, correlated by `corr`.
    #[serde(rename_all = "camelCase")]
    CommandResult {
        corr: u64,
        status: u16,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        body: Option<Value>,
    },
    /// Async await-completion: emitted when an awaited instance reaches a terminal
    /// state, routed back by the create's `corr`.
    #[serde(rename_all = "camelCase")]
    InstanceCompleted {
        corr: u64,
        process_instance_key: String,
        process_completed: bool,
        variables: Value,
    },
    /// Grants the client additional submission (create-side) capacity.
    SubmissionCredits {
        n: i64,
    },
    /// Coarse fleet pressure signal.
    #[serde(rename_all = "camelCase")]
    Pressure {
        level: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry_after_ms: Option<u64>,
    },
    /// Server-advised worker concurrency: the active dispatch width the
    /// worker-concurrency governor has converged on. A cooperating SDK can park
    /// (idle) its subscribers down toward this target so an over-provisioned fleet
    /// stops swamping the single push dispatcher (which halves throughput past the
    /// ~50-worker/node knee). `0` = no advice (governor disabled). Edge-triggered:
    /// broadcast once each time the target changes, so clients self-size without
    /// polling. Purely advisory — the server also enforces the cap by truncating
    /// each job type's per-pass fan-out, so uncooperative clients are still bounded.
    #[serde(rename_all = "camelCase")]
    WorkerAdvice {
        recommended_concurrency: i64,
    },
    Heartbeat,
}

// ----------------------------------------------------------------------------
// Registry
// ----------------------------------------------------------------------------

/// A live job-push subscription for one (connection, job type).
struct Subscription {
    worker: String,
    timeout: u64,
    fetch_variable: Option<Vec<String>>,
    /// Outstanding job-delivery demand: push only while > 0.
    credits: AtomicI64,
}

/// One connected client.
struct Connection {
    id: ConnId,
    /// Outbound frames to the socket writer task (bounded).
    tx: mpsc::Sender<ServerFrame>,
    /// Job subscriptions, keyed by job type.
    subs: Mutex<HashMap<String, Arc<Subscription>>>,
    /// Submission credits granted but not yet consumed by a `CreateInstance`.
    submission_outstanding: AtomicI64,
    /// Target submission window this connection is topped up to.
    submission_window: i64,
    /// Bounds the number of in-flight fire-and-forget creates this connection may
    /// have spawned concurrently. A non-await create takes the non-blocking spawn
    /// path only while a permit is free; once `submission_window` creates are in
    /// flight it falls back to the inline commit-wait, bounding the per-socket
    /// balloon of pinned variable payloads to ~one window. Gating on in-flight
    /// count rather than on submission *credit* means a connection whose creation
    /// credits were withheld under server pressure still spawns — so a multiplexed
    /// producer+worker socket never head-of-line-blocks its own job completions
    /// behind a credit-starved create awaiting a Raft commit.
    create_slots: Arc<tokio::sync::Semaphore>,
    closed: AtomicBool,
    /// Set by the dispatcher when it had credited demand but the outbound socket
    /// buffer was full; the writer task clears it and wakes the dispatcher once a
    /// frame drains, so re-dispatch is push-driven instead of waiting for the
    /// backstop tick. Shared (not borrowed via the connection) so the writer task
    /// does not keep the connection's `tx` alive past teardown.
    wants_redispatch: Arc<AtomicBool>,
    /// Wall-clock millis of the last frame received from this client. Updated on
    /// every inbound message (including heartbeats); the reaper closes connections
    /// that fall silent past the liveness deadline.
    last_seen_ms: AtomicU64,
    /// Fired to break the reader loop when the connection is reaped as a phantom.
    shutdown: Notify,
}

impl Connection {
    /// Enqueues a server frame, dropping it if the socket buffer is full (a slow
    /// consumer) or the connection is gone. Returns whether it was enqueued.
    fn send(&self, frame: ServerFrame) -> bool {
        if self.closed.load(Ordering::Relaxed) {
            return false;
        }
        self.tx.try_send(frame).is_ok()
    }

    /// Grants `n` submission credits, accounting them in `submission_outstanding`
    /// only if the frame is actually enqueued. `send` drops frames when the
    /// outbound buffer is full (slow-consumer guard); incrementing `outstanding`
    /// for a dropped grant permanently inflates it, so `topup`'s
    /// `grant = window - outstanding` computes 0 forever and the client's
    /// submission window never reopens — the producer wedges. Accounting on the
    /// send result makes a dropped grant self-heal on the next top-up pass.
    /// Returns whether the grant was enqueued.
    fn grant_submission_credits(&self, n: i64) -> bool {
        if !self.send(ServerFrame::SubmissionCredits { n }) {
            return false;
        }
        self.submission_outstanding.fetch_add(n, Ordering::Relaxed);
        true
    }
}

/// Server-wide registry of falcon connections and the job-type dispatch
/// index. Lives outside `ServerImpl` (shared by the WS route and the dispatcher);
/// the engine remains unaware of it.
pub struct Registry {
    conns: Mutex<HashMap<ConnId, Arc<Connection>>>,
    /// Which connections subscribe to each job type (dispatch index).
    by_type: Mutex<HashMap<String, Vec<ConnId>>>,
    /// Round-robin cursor per job type, for fair credit spreading.
    rr: Mutex<HashMap<String, usize>>,
    next_id: AtomicU64,
}

impl Registry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            conns: Mutex::new(HashMap::new()),
            by_type: Mutex::new(HashMap::new()),
            rr: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        })
    }

    fn register(&self, conn: Arc<Connection>) {
        crate::metrics::stream_connection_inc();
        self.conns
            .lock()
            .expect("registry poisoned")
            .insert(conn.id, conn);
    }

    /// Removes a connection. Idempotent: returns `true` only the first time a
    /// given id is removed (when the disconnect is counted), `false` on a repeat
    /// call for an already-removed id. Both the reaper (on liveness timeout) and
    /// `handle_socket` (after the reader loop returns) call this for the same
    /// connection; without the guard the `nanobpm_stream_connections_active`
    /// gauge would double-decrement and drift negative.
    fn unregister(&self, id: ConnId) -> bool {
        let removed = self.conns.lock().expect("registry poisoned").remove(&id);
        let Some(conn) = removed else {
            return false;
        };
        conn.closed.store(true, Ordering::Relaxed);
        crate::metrics::stream_connection_dec();
        let mut by_type = self.by_type.lock().expect("registry poisoned");
        for ids in by_type.values_mut() {
            ids.retain(|&other| other != id);
        }
        true
    }

    /// Indexes `id` under `job_type` for dispatch (idempotent).
    fn index(&self, job_type: &str, id: ConnId) {
        let mut by_type = self.by_type.lock().expect("registry poisoned");
        let ids = by_type.entry(job_type.to_string()).or_default();
        if !ids.contains(&id) {
            ids.push(id);
        }
    }

    /// Live subscribed-worker count per job type (the roster width dispatch can
    /// fan out to). Feeds the ~1 Hz worker-provisioning monitor so a job type
    /// with waiting jobs but zero workers can be flagged as starved. A cheap
    /// snapshot under the `by_type` lock; called off the hot path.
    pub fn workers_per_type(&self) -> HashMap<String, usize> {
        self.by_type
            .lock()
            .expect("registry poisoned")
            .iter()
            .map(|(job_type, ids)| (job_type.clone(), ids.len()))
            .collect()
    }

    /// Builds a round-robin–ordered dispatch plan: for each job type, up to
    /// `per_type_cap` live subscriptions to attempt this tick (`0` = no cap → all),
    /// with the cursor advanced so a different stream leads next time. Capping the
    /// fan-out is the worker-concurrency governor's enforcement point: under an
    /// over-provisioned fleet it narrows each pass to the `per_type_cap` subscribers
    /// that keep the drain saturated, parking the rest — and because the round-robin
    /// cursor advances every pass, the parked subset rotates, so no subscriber is
    /// starved. Snapshotted under the locks; all engine work then happens lock-free.
    fn dispatch_plan(&self, per_type_cap: usize) -> Vec<(String, Vec<DispatchTarget>)> {
        let conns = self.conns.lock().expect("registry poisoned");
        let by_type = self.by_type.lock().expect("registry poisoned");
        let mut rr = self.rr.lock().expect("registry poisoned");
        let mut plan = Vec::new();
        for (job_type, ids) in by_type.iter() {
            if ids.is_empty() {
                continue;
            }
            let cursor = rr.entry(job_type.clone()).or_insert(0);
            let start = *cursor % ids.len();
            *cursor = start + 1;
            // How many of this type's subscribers to service this pass: the whole
            // roster unless the governor has narrowed the active width.
            let width = if per_type_cap == 0 {
                ids.len()
            } else {
                per_type_cap.min(ids.len())
            };
            let mut targets = Vec::with_capacity(width);
            for offset in 0..ids.len() {
                if targets.len() >= width {
                    break;
                }
                let id = ids[(start + offset) % ids.len()];
                let Some(conn) = conns.get(&id) else { continue };
                let sub = conn
                    .subs
                    .lock()
                    .expect("registry poisoned")
                    .get(job_type)
                    .cloned();
                if let Some(sub) = sub {
                    targets.push((conn.clone(), sub));
                }
            }
            if !targets.is_empty() {
                plan.push((job_type.clone(), targets));
            }
        }
        plan
    }

    /// Snapshot of all live connections (for the submission-credit top-up pass).
    fn all_connections(&self) -> Vec<Arc<Connection>> {
        self.conns
            .lock()
            .expect("registry poisoned")
            .values()
            .cloned()
            .collect()
    }
}

// ----------------------------------------------------------------------------
// Routing / connection lifecycle
// ----------------------------------------------------------------------------

#[derive(Clone)]
struct CsState {
    server: ServerImpl,
    registry: Arc<Registry>,
    submission_window: i64,
}

#[derive(Debug, Deserialize)]
struct ConnectParams {
    /// Default worker name (lease owner) for this connection's subscriptions; a
    /// `Subscribe` may override per type.
    worker: Option<String>,
}

/// Builds the router carrying the `/falcon` WebSocket endpoint, sharing
/// the engine-backed [`ServerImpl`] and the [`Registry`].
pub fn router(server: ServerImpl, registry: Arc<Registry>) -> Router {
    let state = CsState {
        server,
        registry,
        submission_window: submission_window_from_env(),
    };
    Router::new()
        .route("/falcon", get(ws_handler))
        .with_state(state)
}

fn submission_window_from_env() -> i64 {
    std::env::var("NANOBPMN_STREAM_SUBMISSION_WINDOW")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_SUBMISSION_WINDOW)
}

/// Phantom-connection liveness deadline in millis, overridable via
/// `NANOBPMN_STREAM_LIVENESS_MS` (chiefly for tests that need a fast reap).
fn liveness_timeout_ms() -> u64 {
    std::env::var("NANOBPMN_STREAM_LIVENESS_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(LIVENESS_TIMEOUT_MS)
}

/// Reaper scan interval in millis, overridable via `NANOBPMN_STREAM_REAPER_MS`.
fn reaper_interval_ms() -> u64 {
    std::env::var("NANOBPMN_STREAM_REAPER_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(REAPER_INTERVAL_MS)
}

async fn ws_handler(
    State(state): State<CsState>,
    Query(params): Query<ConnectParams>,
    ws: WebSocketUpgrade,
) -> Response {
    let worker = params.worker.unwrap_or_default();
    ws.on_upgrade(move |socket| handle_socket(socket, state, worker))
}

/// Drives one connection: spawns the writer task, registers the connection, then
/// reads client frames in arrival order (preserving per-connection ordering)
/// until the socket closes.
async fn handle_socket(socket: WebSocket, state: CsState, default_worker: String) {
    let CsState {
        server,
        registry,
        submission_window,
    } = state;

    let id = registry.next_id.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = mpsc::channel::<ServerFrame>(OUTBOUND_CHANNEL_CAP);
    let conn = Arc::new(Connection {
        id,
        tx,
        subs: Mutex::new(HashMap::new()),
        submission_outstanding: AtomicI64::new(0),
        submission_window,
        create_slots: Arc::new(tokio::sync::Semaphore::new(
            submission_window.max(0) as usize
        )),
        closed: AtomicBool::new(false),
        wants_redispatch: Arc::new(AtomicBool::new(false)),
        last_seen_ms: AtomicU64::new(now_millis()),
        shutdown: Notify::new(),
    });
    registry.register(conn.clone());

    let (sink, stream) = socket.split();
    tokio::spawn(writer_task(
        conn.wants_redispatch.clone(),
        sink,
        rx,
        server.dispatch_wake_handle(),
    ));

    // Open the submission window so the client may start sending creates, and
    // announce the connection parameters.
    conn.submission_outstanding
        .store(submission_window, Ordering::Relaxed);
    conn.send(ServerFrame::Welcome {
        submission_credits: submission_window,
        heartbeat_ms: HEARTBEAT_MS,
    });
    conn.send(ServerFrame::SubmissionCredits {
        n: submission_window,
    });

    reader_loop(stream, &server, &registry, &conn, &default_worker).await;

    // Disconnect: drop the connection from the registry. Jobs already pushed but
    // not completed are reclaimed by lease-deadline expiry (the periodic tick),
    // so there is nothing else to clean up.
    registry.unregister(id);
}

/// Drains outbound frames to the socket and emits periodic heartbeats. Exits when
/// every sender (the [`Connection`] and any await tasks) is dropped or the socket
/// errors. When a frame drains and the dispatcher had stalled on a full buffer
/// (`wants_redispatch`), wakes it so re-dispatch is push-driven, not tick-driven.
async fn writer_task(
    wants_redispatch: Arc<AtomicBool>,
    mut sink: futures_util::stream::SplitSink<WebSocket, Message>,
    mut rx: mpsc::Receiver<ServerFrame>,
    dispatch_wake: Arc<Notify>,
) {
    let mut heartbeat = tokio::time::interval(Duration::from_millis(HEARTBEAT_MS));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let frame = tokio::select! {
            frame = rx.recv() => match frame {
                Some(frame) => frame,
                None => break,
            },
            _ = heartbeat.tick() => ServerFrame::Heartbeat,
        };
        let json = match serde_json::to_string(&frame) {
            Ok(json) => json,
            Err(_) => continue,
        };
        if sink.send(Message::text(json)).await.is_err() {
            break;
        }
        // A slot just freed on the outbound buffer. If the dispatcher skipped this
        // connection because the buffer was full, wake it to retry now.
        if wants_redispatch.swap(false, Ordering::Relaxed) {
            dispatch_wake.notify_one();
        }
    }
    let _ = sink.close().await;
}

async fn reader_loop(
    mut stream: futures_util::stream::SplitStream<WebSocket>,
    server: &ServerImpl,
    registry: &Arc<Registry>,
    conn: &Arc<Connection>,
    default_worker: &str,
) {
    loop {
        let message = tokio::select! {
            // Reaped as a phantom: stop reading so the connection tears down.
            _ = conn.shutdown.notified() => break,
            next = stream.next() => match next {
                Some(Ok(message)) => message,
                // Stream ended or errored (clean close, FIN/RST): disconnect.
                Some(Err(_)) | None => break,
            },
        };
        // Any inbound frame — command or heartbeat — proves the client is alive.
        conn.last_seen_ms.store(now_millis(), Ordering::Relaxed);
        match message {
            Message::Text(text) => {
                let frame: ClientFrame = match serde_json::from_str(&text) {
                    Ok(frame) => frame,
                    Err(e) => {
                        conn.send(ServerFrame::CommandResult {
                            corr: 0,
                            status: 400,
                            body: Some(Value::String(format!("malformed frame: {e}"))),
                        });
                        continue;
                    }
                };
                handle_client_frame(server, registry, conn, default_worker, frame).await;
            }
            Message::Binary(_) => {
                // Protocol is JSON text; ignore binary frames.
            }
            Message::Close(_) => break,
            // Ping/Pong are handled by the transport.
            Message::Ping(_) | Message::Pong(_) => {}
        }
    }
}

/// Decide whether a `CreateInstance` takes the fire-and-forget **spawn** path
/// (returns `true`) or the inline **serialized** path (`false`).
///
/// Spawning lets the reader loop read the next frame immediately, giving a
/// producer connection concurrent proposes through the Raft Batcher — but each
/// spawned task pins its variables payload before the engine's admission rails
/// can see it, so an unbounded spawn rate is a per-connection memory balloon.
///
/// We only spawn when all three hold:
/// - `!awaiting`: `awaitCompletion` creates must stay inline (the completion wait
///   reads this node's read store).
/// - `raft_active`: the spawn path only exists under Raft; single-node creates are
///   already inline (and thus naturally serialized) below.
/// - `has_permit`: the connection held a free **in-flight-create slot**
///   (`create_slots`). This bounds the concurrent spawned-create balloon to
///   ~one submission window per socket. A client that over-sends past its window
///   exhausts its slots; those creates fall to the inline path, which awaits the
///   commit and blocks *this* connection's reader, applying TCP backpressure to
///   just the offending socket. Crucially the bound keys off in-flight *count*,
///   not submission *credit*: a connection whose creation credits were withheld
///   under server pressure (not by over-sending) keeps free slots, so it still
///   spawns and never head-of-line-blocks its own completions — self-enforcing
///   flow control that no longer conflates "out of credit" with "over budget".
fn should_spawn_fire_and_forget(awaiting: bool, raft_active: bool, has_permit: bool) -> bool {
    !awaiting && raft_active && has_permit
}

/// Dispatches one client frame. Engine-bound writes are awaited inline so a single
/// connection's commands keep arrival order at the journal; `awaitCompletion`
/// spawns a detached task so a long wait does not block the connection's intake.
async fn handle_client_frame(
    server: &ServerImpl,
    registry: &Arc<Registry>,
    conn: &Arc<Connection>,
    default_worker: &str,
    frame: ClientFrame,
) {
    use std::time::Instant;
    let start = Instant::now();

    // Record frame type
    let frame_type = match &frame {
        ClientFrame::Subscribe { .. } => "subscribe",
        ClientFrame::JobCredits { .. } => "job_credits",
        ClientFrame::CreateInstance { .. } => "create_instance",
        ClientFrame::CompleteJob { .. } => "complete_job",
        ClientFrame::FailJob { .. } => "fail_job",
        ClientFrame::ThrowError { .. } => "throw_error",
        ClientFrame::AwaitInstance { .. } => "await_instance",
        ClientFrame::Heartbeat => "heartbeat",
        ClientFrame::Deploy { .. } => "deploy",
        ClientFrame::InstallDeployment { .. } => "install_deployment",
        ClientFrame::PublishMessage { .. } => "publish_message",
        ClientFrame::CancelInstance { .. } => "cancel_instance",
        ClientFrame::RouteSubscription { .. } => "route_subscription",
        ClientFrame::UpdateJobRetries { .. } => "update_job_retries",
        ClientFrame::ResolveIncident { .. } => "resolve_incident",
        ClientFrame::SetVariables { .. } => "set_variables",
        ClientFrame::ActivateJobs { .. } => "activate_jobs",
        ClientFrame::GetByKey { .. } => "get_by_key",
        ClientFrame::ForwardUserTask { .. } => "forward_user_task",
        ClientFrame::ForwardCreate { .. } => "forward_create",
        ClientFrame::Raft { .. } => "raft",
        ClientFrame::LeaseDigest { .. } => "lease_digest",
        ClientFrame::Promote { .. } => "promote",
        ClientFrame::SetSlaMode { .. } => "set_sla_mode",
        ClientFrame::PressureReport { .. } => "pressure_report",
    };
    crate::metrics::record_stream_frame(frame_type);

    match frame {
        ClientFrame::Subscribe {
            job_type,
            job_credits,
            fetch_variable,
            timeout,
            worker,
        } => {
            let sub = Arc::new(Subscription {
                worker: worker.unwrap_or_else(|| {
                    if default_worker.is_empty() {
                        format!("stream-{}", conn.id)
                    } else {
                        default_worker.to_string()
                    }
                }),
                timeout: timeout.filter(|&t| t > 0).unwrap_or(DEFAULT_JOB_LOCK_MS),
                fetch_variable: fetch_variable.filter(|names| !names.is_empty()),
                credits: AtomicI64::new(job_credits.max(0)),
            });
            conn.subs
                .lock()
                .expect("registry poisoned")
                .insert(job_type.clone(), sub);
            registry.index(&job_type, conn.id);
            // A new subscription may have a backlog waiting: wake the dispatcher.
            server.dispatch_wake_handle().notify_one();
        }
        ClientFrame::JobCredits { job_type, n } => {
            if let Some(sub) = conn.subs.lock().expect("registry poisoned").get(&job_type) {
                sub.credits.fetch_add(n, Ordering::Relaxed);
            }
            server.dispatch_wake_handle().notify_one();
        }
        ClientFrame::CreateInstance {
            corr,
            process_definition_id,
            process_definition_key,
            variables,
            await_completion,
            fetch_variables,
            request_timeout,
        } => {
            // Consume a submission credit (intake metering). The client is
            // expected to hold one; we still account so the top-up pass refills.
            let before = conn.submission_outstanding.fetch_sub(1, Ordering::Relaxed);
            // If we just consumed the last credit (or went negative), the client
            // is about to stall waiting for top-up.
            if before <= 1 {
                crate::metrics::record_stream_credit_stall();
            }

            // Cluster-wide create placement (fire-and-forget only). Round-robin
            // over every partition: when the placement lands on a peer-owned
            // partition, forward the create there so a producer on one connection
            // spreads instances across the whole cluster instead of piling every
            // one onto this gateway's own partitions. `awaitCompletion` creates
            // skip placement and stay local, because the completion wait reads
            // this node's per-partition read store and cannot see a peer's
            // instance. A local placement (`None`) falls through to the in-process
            // create below. The forwarded create is counted on the owner (its
            // `create_forwarded`), so per-node metrics reflect the real placement.
            let awaiting = await_completion.unwrap_or(false);
            let raft_active = !server.raft_registry().is_empty();
            // Acquire an in-flight-create slot if this create is eligible for the
            // spawn path (non-await, under Raft). The permit is moved into the
            // spawned task and held until the create resolves, so the concurrent
            // balloon of pinned variable payloads stays bounded to ~one window per
            // socket. Gating here on slot availability (not submission credit)
            // keeps a credit-starved-by-pressure connection's completions flowing:
            // it still holds free slots and so still spawns rather than serializing
            // its reader. See `should_spawn_fire_and_forget`.
            let create_permit = if !awaiting && raft_active {
                conn.create_slots.clone().try_acquire_owned().ok()
            } else {
                None
            };
            // Under Raft, both `create_for_stream` (local propose) and
            // `create_forwarded_stream` (peer round-trip) await a full quorum
            // commit. Awaiting them inline serializes a producer's connection one
            // commit at a time (~1/commit-latency), the dominant cause of the
            // cluster create collapse. Fire-and-forget creates are independent and
            // corr-tagged, so spawn them: the reader loop reads the next frame
            // immediately and concurrent proposes batch through the per-partition
            // Raft Batcher. `awaitCompletion` creates stay on the inline path below
            // (the completion wait must read this node's read store). The non-Raft
            // single-node fast path is likewise unchanged.
            if should_spawn_fire_and_forget(awaiting, raft_active, create_permit.is_some()) {
                let permit =
                    create_permit.expect("permit is Some whenever the spawn decision is true");
                let server = server.clone();
                let conn = conn.clone();
                tokio::spawn(async move {
                    // Held for the life of the spawned create; released on drop,
                    // freeing the in-flight slot for the next create on this socket.
                    let _create_permit = permit;
                    if let Some(node) = server.stream_create_placement() {
                        match server
                            .create_forwarded_stream_rerouting(
                                node,
                                process_definition_id.clone(),
                                process_definition_key.clone(),
                                variables.clone(),
                            )
                            .await
                        {
                            Some(Ok((instance_key, sync_completed))) => {
                                conn.send(ServerFrame::CommandResult {
                                    corr,
                                    status: 200,
                                    body: Some(serde_json::json!({
                                        "processInstanceKey": instance_key.to_string(),
                                        "processCompleted": sync_completed,
                                    })),
                                });
                            }
                            Some(Err((status, message))) => {
                                conn.send(ServerFrame::CommandResult {
                                    corr,
                                    status,
                                    body: Some(Value::String(message)),
                                });
                            }
                            None => {
                                // Whole cluster shed: create locally (this node
                                // already passed its own admission gate).
                                let vars = to_engine_vars(variables);
                                match server
                                    .create_for_stream(
                                        process_definition_id,
                                        process_definition_key,
                                        vars,
                                    )
                                    .await
                                {
                                    Ok((instance_key, sync_completed)) => {
                                        crate::metrics::record_create("stream");
                                        conn.send(ServerFrame::CommandResult {
                                            corr,
                                            status: 200,
                                            body: Some(serde_json::json!({
                                                "processInstanceKey": instance_key.to_string(),
                                                "processCompleted": sync_completed,
                                            })),
                                        });
                                    }
                                    Err((status, message)) => {
                                        conn.send(ServerFrame::CommandResult {
                                            corr,
                                            status,
                                            body: Some(Value::String(message)),
                                        });
                                    }
                                }
                            }
                        }
                    } else {
                        let vars = to_engine_vars(variables);
                        match server
                            .create_for_stream(process_definition_id, process_definition_key, vars)
                            .await
                        {
                            Ok((instance_key, sync_completed)) => {
                                crate::metrics::record_create("stream");
                                conn.send(ServerFrame::CommandResult {
                                    corr,
                                    status: 200,
                                    body: Some(serde_json::json!({
                                        "processInstanceKey": instance_key.to_string(),
                                        "processCompleted": sync_completed,
                                    })),
                                });
                            }
                            Err((status, message)) => {
                                conn.send(ServerFrame::CommandResult {
                                    corr,
                                    status,
                                    body: Some(Value::String(message)),
                                });
                            }
                        }
                    }
                    grant_submission_credit_if_clear(&server, &conn, 1);
                });
                return;
            }
            if !awaiting && let Some(node) = server.stream_create_placement() {
                match server
                    .create_forwarded_stream_rerouting(
                        node,
                        process_definition_id.clone(),
                        process_definition_key.clone(),
                        variables.clone(),
                    )
                    .await
                {
                    Some(Ok((instance_key, sync_completed))) => {
                        conn.send(ServerFrame::CommandResult {
                            corr,
                            status: 200,
                            body: Some(serde_json::json!({
                                "processInstanceKey": instance_key.to_string(),
                                "processCompleted": sync_completed,
                            })),
                        });
                        grant_submission_credit_if_clear(server, conn, 1);
                    }
                    Some(Err((status, message))) => {
                        conn.send(ServerFrame::CommandResult {
                            corr,
                            status,
                            body: Some(Value::String(message)),
                        });
                        grant_submission_credit_if_clear(server, conn, 1);
                    }
                    None => {
                        let vars = to_engine_vars(variables);
                        match server
                            .create_for_stream(process_definition_id, process_definition_key, vars)
                            .await
                        {
                            Ok((instance_key, sync_completed)) => {
                                crate::metrics::record_create("stream");
                                conn.send(ServerFrame::CommandResult {
                                    corr,
                                    status: 200,
                                    body: Some(serde_json::json!({
                                        "processInstanceKey": instance_key.to_string(),
                                        "processCompleted": sync_completed,
                                    })),
                                });
                            }
                            Err((status, message)) => {
                                conn.send(ServerFrame::CommandResult {
                                    corr,
                                    status,
                                    body: Some(Value::String(message)),
                                });
                            }
                        }
                        grant_submission_credit_if_clear(server, conn, 1);
                    }
                }
                return;
            }

            let vars = to_engine_vars(variables);
            match server
                .create_for_stream(process_definition_id, process_definition_key, vars)
                .await
            {
                Ok((instance_key, sync_completed)) => {
                    crate::metrics::record_create("stream");
                    conn.send(ServerFrame::CommandResult {
                        corr,
                        status: 200,
                        body: Some(serde_json::json!({
                            "processInstanceKey": instance_key.to_string(),
                            "processCompleted": sync_completed,
                        })),
                    });
                    if await_completion.unwrap_or(false) {
                        // Emit completion asynchronously (the task returns
                        // immediately if the instance is already terminal).
                        spawn_await_completion(
                            server.clone(),
                            conn.clone(),
                            corr,
                            instance_key,
                            fetch_variables,
                            request_timeout,
                        );
                    }
                    // Replenish one submission credit if the engine has headroom.
                    grant_submission_credit_if_clear(server, conn, 1);
                }
                Err((status, message)) => {
                    conn.send(ServerFrame::CommandResult {
                        corr,
                        status,
                        body: Some(Value::String(message)),
                    });
                    grant_submission_credit_if_clear(server, conn, 1);
                }
            }
        }
        ClientFrame::CompleteJob {
            corr,
            job_key,
            variables,
        } => {
            let Some(key) = parse_job_key(conn, corr, &job_key) else {
                return;
            };
            // Cluster: a worker attached here may complete a job a peer owns
            // (job aggregation). Forward to the owner; local jobs stay on the
            // pipelined fast path. Single-node always owns every key.
            if let Some(node) = server.job_route(key) {
                crate::metrics::record_complete_outcome("route_forward");
                let server = server.clone();
                spawn_forward_stream_reply(conn, corr, async move {
                    let outcome = server
                        .forward_complete_job_stream(node, key, variables)
                        .await;
                    crate::metrics::record_complete_outcome(if outcome.0 < 300 {
                        "forward_ok"
                    } else {
                        "forward_err"
                    });
                    outcome
                });
            } else {
                crate::metrics::record_complete_outcome("route_local");
                let vars = to_engine_vars(variables);
                if server.raft_registry().is_empty() {
                    pipeline_job_command(
                        server,
                        conn,
                        corr,
                        server.complete_job_for_stream(key, vars).await,
                    );
                } else {
                    // Under Raft, `complete_job_for_stream` awaits the full quorum
                    // commit; spawn it so the reader loop isn't serialized one commit
                    // at a time (see `spawn_job_command`).
                    let server = server.clone();
                    let conn = conn.clone();
                    tokio::spawn(async move {
                        let outcome = server.complete_job_for_stream(key, vars).await;
                        pipeline_job_command(&server, &conn, corr, outcome);
                    });
                }
            }
        }
        ClientFrame::FailJob {
            corr,
            job_key,
            retries,
            error_message,
        } => {
            let Some(key) = parse_job_key(conn, corr, &job_key) else {
                return;
            };
            if let Some(node) = server.job_route(key) {
                let server = server.clone();
                let retries = retries.unwrap_or(0);
                let error_message = error_message.unwrap_or_default();
                spawn_forward_stream_reply(conn, corr, async move {
                    server
                        .forward_fail_job_stream(node, key, retries, error_message)
                        .await
                });
            } else {
                let retries = retries.unwrap_or(0);
                let error_message = error_message.unwrap_or_default();
                if server.raft_registry().is_empty() {
                    let outcome = server
                        .fail_job_for_stream(key, retries, error_message)
                        .await;
                    pipeline_job_command(server, conn, corr, outcome);
                } else {
                    let server = server.clone();
                    let conn = conn.clone();
                    tokio::spawn(async move {
                        let outcome = server
                            .fail_job_for_stream(key, retries, error_message)
                            .await;
                        pipeline_job_command(&server, &conn, corr, outcome);
                    });
                }
            }
        }
        ClientFrame::ThrowError {
            corr,
            job_key,
            error_code,
            error_message,
        } => {
            let Some(key) = parse_job_key(conn, corr, &job_key) else {
                return;
            };
            if let Some(node) = server.job_route(key) {
                let server = server.clone();
                let error_code = error_code.clone();
                let error_message = error_message.clone().unwrap_or_default();
                spawn_forward_stream_reply(conn, corr, async move {
                    server
                        .forward_throw_error_stream(node, key, error_code, error_message)
                        .await
                });
            } else {
                let error_message = error_message.unwrap_or_default();
                if server.raft_registry().is_empty() {
                    let outcome = server
                        .throw_error_for_stream(key, error_code, error_message)
                        .await;
                    pipeline_job_command(server, conn, corr, outcome);
                } else {
                    let server = server.clone();
                    let conn = conn.clone();
                    tokio::spawn(async move {
                        let outcome = server
                            .throw_error_for_stream(key, error_code, error_message)
                            .await;
                        pipeline_job_command(&server, &conn, corr, outcome);
                    });
                }
            }
        }
        ClientFrame::AwaitInstance {
            corr,
            process_instance_key,
            fetch_variables,
            request_timeout,
        } => match process_instance_key.parse::<nanobpmn_engine_core::Key>() {
            Ok(instance_key) => {
                // Reuse the same async await path as `CreateInstance{awaitCompletion}`:
                // it resolves immediately for an already-terminal (possibly evicted)
                // instance, since the read model is durable history.
                spawn_await_completion(
                    server.clone(),
                    conn.clone(),
                    corr,
                    instance_key,
                    fetch_variables,
                    request_timeout,
                );
            }
            Err(_) => {
                conn.send(ServerFrame::CommandResult {
                    corr,
                    status: 404,
                    body: Some(Value::String(format!(
                        "Process instance key '{process_instance_key}' is not a valid key."
                    ))),
                });
            }
        },
        ClientFrame::Heartbeat => {}
        ClientFrame::Deploy {
            corr,
            resources,
            tenant_id,
        } => {
            // This node owns the deployment partition (a peer only forwards a
            // Deploy here when it does not). Process it centrally — durable local
            // deploy + broadcast to every peer — and return the deployment JSON.
            match server
                .deploy_centralized(
                    resources,
                    tenant_id.unwrap_or_else(|| "<default>".to_string()),
                )
                .await
            {
                Ok(body) => conn.send(ServerFrame::CommandResult {
                    corr,
                    status: 200,
                    body: Some(body),
                }),
                Err((status, message)) => conn.send(ServerFrame::CommandResult {
                    corr,
                    status,
                    body: Some(Value::String(message)),
                }),
            };
        }
        ClientFrame::InstallDeployment { corr, events } => {
            server.install_replicated_deployment(events).await;
            conn.send(ServerFrame::CommandResult {
                corr,
                status: 200,
                body: None,
            });
        }
        ClientFrame::PublishMessage {
            corr,
            name,
            correlation_key,
            variables,
        } => {
            let vars = to_engine_vars(variables);
            let (message_key, instance) = server
                .correlate_message_local(name, correlation_key, vars)
                .await;
            // Correlation may have advanced a token onto a service task on one of
            // this peer's partitions, creating an activatable job: wake pollers.
            server.signal_jobs_available();
            conn.send(ServerFrame::CommandResult {
                corr,
                status: 200,
                body: Some(serde_json::json!({
                    "messageKey": message_key.to_string(),
                    "correlatedInstanceKey": instance.map(|k| k.to_string()),
                })),
            });
        }
        ClientFrame::CancelInstance { corr, instance_key } => {
            forward_by_key_reply(conn, corr, &instance_key, |key| async move {
                server.cancel_instance_local(key).await
            })
            .await;
        }
        ClientFrame::RouteSubscription { corr, event } => {
            // The owner applies the routed subscription command locally and drives
            // its own pump for any further follow-ups, then acknowledges.
            server.apply_routed_subscription_remote(event).await;
            conn.send(ServerFrame::CommandResult {
                corr,
                status: 200,
                body: None,
            });
        }
        ClientFrame::UpdateJobRetries {
            corr,
            job_key,
            retries,
        } => {
            forward_by_key_reply(conn, corr, &job_key, |key| async move {
                server.update_job_retries_local(key, retries).await
            })
            .await;
        }
        ClientFrame::ResolveIncident {
            corr,
            incident_key,
            operation_reference,
        } => {
            forward_by_key_reply(conn, corr, &incident_key, |key| async move {
                server
                    .resolve_incident_local(key, operation_reference)
                    .await
            })
            .await;
        }
        ClientFrame::SetVariables {
            corr,
            scope_key,
            variables,
            local,
        } => {
            let vars = to_engine_vars(variables);
            forward_by_key_reply(conn, corr, &scope_key, |key| async move {
                server.set_variables_local(key, vars, local).await
            })
            .await;
        }
        ClientFrame::ForwardCreate {
            corr,
            process_definition_id,
            process_definition_key,
            variables,
            tags,
            business_id,
            await_completion,
            fetch_variables,
            request_timeout,
        } => {
            // Local-only: create on one of THIS peer's own partitions and answer
            // with the full result JSON. The peer never re-forwards, so there is
            // no placement loop.
            //
            // Spawned (not awaited inline): a forwarded create drives a full Raft
            // commit (quorum round-trip), so awaiting it here would serialize every
            // forwarded create at this connection's read loop — one commit at a
            // time — collapsing cluster create throughput to 1/(commit latency).
            // Each forwarded create is an independent instance and the reply is
            // tagged with `corr`, so relaxed reply ordering is safe (same rationale
            // as `pipeline_job_command` / `spawn_forward_stream_reply`). Spawning
            // lets concurrent forwarded creates batch through the partition Batcher.
            let vars = to_engine_vars(variables);
            let server = server.clone();
            let conn = conn.clone();
            tokio::spawn(async move {
                match server
                    .create_forwarded(
                        process_definition_id,
                        process_definition_key,
                        vars,
                        tags,
                        business_id,
                        await_completion,
                        fetch_variables,
                        request_timeout,
                    )
                    .await
                {
                    Ok(body) => conn.send(ServerFrame::CommandResult {
                        corr,
                        status: 200,
                        body: Some(body),
                    }),
                    Err((status, message)) => conn.send(ServerFrame::CommandResult {
                        corr,
                        status,
                        body: Some(Value::String(message)),
                    }),
                };
            });
        }
        ClientFrame::ActivateJobs {
            corr,
            job_type,
            worker,
            max_jobs,
            timeout,
            fetch_variable,
        } => {
            // Peer-side of job aggregation: activate on THIS node's own partitions
            // for a worker attached to the requesting gateway, and answer with the
            // projected jobs. Local-only (the engine owns just this node's
            // partitions) ⇒ no fan-out loop. The lease is held here under `timeout`,
            // so at-least-once survives the gateway dying mid-flight.
            //
            // Spawned (not awaited inline): activation is a logged Raft command, so
            // awaiting it here would serialize every gateway's job-pull at this
            // connection's read loop — one commit at a time — throttling cluster job
            // throughput to 1/(commit latency). The reply is `corr`-tagged so
            // relaxed ordering is safe (same rationale as the forwarded-create and
            // `pipeline_job_command` paths).
            let server = server.clone();
            let conn = conn.clone();
            tokio::spawn(async move {
                let want = max_jobs.max(0) as usize;
                let jobs = if want == 0 {
                    Vec::new()
                } else {
                    server
                        .activate_for_stream(
                            &job_type,
                            &worker,
                            want,
                            timeout.filter(|&t| t > 0).unwrap_or(DEFAULT_JOB_LOCK_MS),
                            fetch_variable.as_deref().filter(|names| !names.is_empty()),
                        )
                        .await
                };
                let body = serde_json::json!({
                    "jobs": jobs,
                    // Piggybacked per-node backlog (active instances) so the
                    // requesting gateway can weight future activation toward the
                    // genuinely-deepest node (Stage 2) with no extra round-trip.
                    "backlog": server.active_backlog(),
                });
                conn.send(ServerFrame::CommandResult {
                    corr,
                    status: 200,
                    body: Some(body),
                });
            });
        }
        ClientFrame::GetByKey { corr, kind, key } => {
            // Peer-side of query forwarding: answer a read for a key this node
            // owns from its local read model.
            let (status, body) = server.read_by_key_local(kind, key);
            conn.send(ServerFrame::CommandResult { corr, status, body });
        }
        ClientFrame::ForwardUserTask {
            corr,
            op,
            user_task_key,
            payload,
        } => {
            // Peer-side of user-task forwarding: re-apply the original REST
            // mutation locally (this node owns the task's partition) and mirror
            // the REST status. Local-only ⇒ no forwarding loop.
            let (status, message) = server
                .apply_user_task_forwarded(op, &user_task_key, payload)
                .await;
            conn.send(ServerFrame::CommandResult {
                corr,
                status,
                body: message.map(Value::String),
            });
        }
        ClientFrame::Raft {
            corr,
            partition,
            rpc,
            zip,
        } => {
            // Peer-side of the Raft network: feed the inbound RPC into the local
            // replica of `partition` and answer with the serialized response.
            //
            // SPAWN, don't await inline. One `?raft=1` socket per peer multiplexes
            // every partition's AppendEntries/Vote, and `dispatch_raft_rpc` awaits
            // the follower's `raft.append_entries` (a log-store fsync + apply hop).
            // Awaiting it inline serializes all 12 replicas through this single
            // reader loop, so one partition's slow append head-of-line-blocks every
            // other partition's RPC behind it — under sustained load the queued
            // frames blow past openraft's 250ms AppendEntries deadline and both
            // followers time out bidirectionally (the observed replication
            // collapse). Spawning lets the reader loop read the next frame at once,
            // so partitions replicate concurrently. Safe: the RPC is corr-tagged
            // (the client matches the reply out of order) and openraft serializes
            // per-group internally while log-matching makes any reordered/stale
            // append idempotent — exactly a real network's concurrent delivery.
            let server = server.clone();
            let conn = conn.clone();
            tokio::spawn(async move {
                let (status, body) = match server.dispatch_raft_rpc(partition, &rpc, zip).await {
                    Ok(resp) => (200u16, Some(resp)),
                    Err((status, message)) => (status, Some(Value::String(message))),
                };
                conn.send(ServerFrame::CommandResult { corr, status, body });
            });
        }
        ClientFrame::LeaseDigest {
            partition,
            leases,
            sent_at,
        } => {
            // Best-effort soft lease digest from this partition's current leader.
            // Store it; on promotion this node recovers the leases (see the tick
            // driver). Fire-and-forget: no reply.
            server.record_lease_digest(partition, leases, sent_at);
        }
        ClientFrame::Promote {
            partition,
            epoch,
            leader_node,
            leader_addr: _,
        } => {
            // Leader-durable auto-recovery announcement: a peer promoted itself
            // leader of `partition`. Adopt the epoch and rejoin as a learner (or
            // step down if we were a stale leader). Fire-and-forget: no reply.
            server.handle_promotion(partition, epoch, leader_node).await;
        }
        ClientFrame::SetSlaMode { mode } => {
            // A peer's operator switched the runtime SLA mode; apply it locally so
            // the whole cluster runs one uniform admission policy. We do NOT
            // re-broadcast (the originator already fanned out to every peer), so
            // there is no propagation loop. Fire-and-forget: no reply.
            server.apply_remote_sla_mode(&mode);
        }
        ClientFrame::PressureReport { node, load } => {
            // A peer gossiped its current create-load index (ADR 0014 balanced).
            // Record it for weighted placement. Fire-and-forget: no reply, no
            // re-broadcast (each node gossips to every peer directly).
            server.record_peer_pressure(node, load);
        }
    }

    // Record frame processing time
    crate::metrics::record_stream_frame_processing(start.elapsed());
}

/// Completes a job-lifecycle command (completeJob / failJob / throwError) without
/// blocking the connection's read loop on durability, to maximize throughput.
///
/// ## How it works
///
/// 1. The engine actor has already applied the command and written to the journal,
///    establishing the correct ordering (the reader awaits that actor round-trip).
/// 2. **We return the `Commit` handle without awaiting it** — fsync happens later.
/// 3. A detached task awaits the fsync, then replies `200` and wakes job pollers.
///
/// ## Why: group-commit efficiency
///
/// Because the reader loop does not block on fsync, multiple connections' (and a
/// pipelining client's) completions can be in flight at once. The journal writer
/// batches all pending writes into a single fsync (**group-commit**), which is the
/// single biggest throughput lever on fsync-latency-bound disks. Measured: ~4×
/// higher throughput (2280 vs 572 writes/s) than awaiting fsync inline.
///
/// ## Durability trade-off (ack-before-fsync)
///
/// The `200` reply is sent **before** fsync completes (~5ms window). If the server
/// crashes in that window, the completion is lost from disk and the job re-activates
/// after restart (lock expires). This **preserves at-least-once semantics** —
/// handlers must already be idempotent (standard BPMN worker contract) — and the
/// durability window is negligible vs typical job lock timeouts (30–60s).
///
/// ## Safety
///
/// - **Ordering:** Journal write order is correct (engine actor serializes commands).
/// - **Correlation:** Replies are tagged with `corr`, so relaxed reply ordering is safe.
/// - **Error path:** Apply-time errors (job not found, not active) carry no commit
///   and are replied inline (synchronous failure, no durability concern).
/// - **REST API:** The `/jobs/{key}/completion` REST endpoint still awaits fsync
///   before replying; only the Falcon protocol pipelines.
///
/// Analogous to Kafka `acks=1` or RabbitMQ async confirms. See README.md "Stream
/// durability: ack-before-fsync pipelining" for full rationale.
fn pipeline_job_command(
    server: &ServerImpl,
    conn: &Arc<Connection>,
    corr: u64,
    outcome: Result<Commit, (u16, String)>,
) {
    match outcome {
        Ok(commit) => {
            server.note_job_completion("stream");
            let server = server.clone();
            let conn = conn.clone();
            tokio::spawn(async move {
                commit.wait().await;
                server.signal_jobs_available();
                conn.send(ServerFrame::CommandResult {
                    corr,
                    status: 200,
                    body: None,
                });
            });
        }
        Err((status, message)) => {
            conn.send(ServerFrame::CommandResult {
                corr,
                status,
                body: Some(Value::String(message)),
            });
        }
    }
}

fn parse_job_key(conn: &Arc<Connection>, corr: u64, raw: &str) -> Option<u64> {
    match raw.parse::<u64>() {
        Ok(key) => Some(key),
        Err(_) => {
            conn.send(ServerFrame::CommandResult {
                corr,
                status: 404,
                body: Some(Value::String(format!(
                    "Job key '{raw}' is not a valid key."
                ))),
            });
            None
        }
    }
}

/// Spawns a detached task that drives a forwarded stream job-lifecycle command
/// (complete / fail / throwError on a peer-owned job) and relays the peer's
/// `(status, body)` straight back as the connection's `CommandResult`. Detached so
/// the connection's read loop never blocks on the cross-node round-trip, matching
/// the off-thread reply of the local [`pipeline_job_command`] path.
fn spawn_forward_stream_reply<F>(conn: &Arc<Connection>, corr: u64, fut: F)
where
    F: std::future::Future<Output = (u16, Option<Value>)> + Send + 'static,
{
    let conn = conn.clone();
    tokio::spawn(async move {
        let (status, body) = fut.await;
        conn.send(ServerFrame::CommandResult { corr, status, body });
    });
}

/// Peer-side handler for a forwarded by-key mutation (cancel / update-retries /
/// resolve-incident / set-variables). Parses the key, runs `apply` on this node's
/// owning partition, awaits durability, and replies a uniform `CommandResult`:
/// `204` on success, the engine-mapped status on a domain error, `404` on an
/// unparseable key. The `apply` closure (a `ServerImpl::*_local` method) is
/// responsible for waking job pollers when its command can re-activate a job.
async fn forward_by_key_reply<F, Fut>(conn: &Arc<Connection>, corr: u64, raw: &str, apply: F)
where
    F: FnOnce(u64) -> Fut,
    Fut: std::future::Future<Output = Result<(), (u16, String)>>,
{
    let key = match raw.parse::<u64>() {
        Ok(k) => k,
        Err(_) => {
            conn.send(ServerFrame::CommandResult {
                corr,
                status: 404,
                body: Some(Value::String(format!("Key '{raw}' is not a valid key."))),
            });
            return;
        }
    };
    match apply(key).await {
        Ok(()) => conn.send(ServerFrame::CommandResult {
            corr,
            status: 204,
            body: None,
        }),
        Err((status, message)) => conn.send(ServerFrame::CommandResult {
            corr,
            status,
            body: Some(Value::String(message)),
        }),
    };
}

fn to_engine_vars(variables: Option<Map<String, Value>>) -> HashMap<String, crate::Value> {
    variables
        .map(|map| {
            map.iter()
                .map(|(name, value)| (name.clone(), crate::json_to_value(value)))
                .collect()
        })
        .unwrap_or_default()
}

/// Spawns a detached task that waits for `instance_key` to reach a terminal state
/// and emits an `InstanceCompleted` frame correlated by `corr`. Far cheaper than
/// holding an HTTP request: just a `Notify` await plus a read-model lookup.
fn spawn_await_completion(
    server: ServerImpl,
    conn: Arc<Connection>,
    corr: u64,
    instance_key: nanobpmn_engine_core::Key,
    fetch_variables: Option<Vec<String>>,
    request_timeout: Option<i64>,
) {
    tokio::spawn(async move {
        let (variables, completed) = server
            .await_completion_for_stream(instance_key, fetch_variables.as_ref(), request_timeout)
            .await;
        let variables = serde_json::to_value(&variables).unwrap_or(Value::Null);
        conn.send(ServerFrame::InstanceCompleted {
            corr,
            process_instance_key: instance_key.to_string(),
            process_completed: completed,
            variables,
        });
    });
}

/// Grants `n` submission credits to a connection iff the engine is not shedding,
/// keeping the client's intake window full under healthy load and letting it
/// drain (the client stalls) under pressure.
fn grant_submission_credit_if_clear(server: &ServerImpl, conn: &Arc<Connection>, n: i64) {
    if n <= 0 || server.create_admission_blocked() {
        return;
    }
    conn.grant_submission_credits(n);
}

// ----------------------------------------------------------------------------
// Dispatcher
// ----------------------------------------------------------------------------

/// Spawns the single server-wide dispatcher: it reacts to `jobs_available` (and a
/// periodic backstop sweep), leases jobs round-robin across subscribers, and tops
/// up submission credits as engine headroom allows.
pub fn spawn_dispatcher(server: ServerImpl, registry: Arc<Registry>) {
    let jobs_available = server.dispatch_wake_handle();
    let registry_for_reaper = registry.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(DISPATCH_TICK_MS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_pressure = false;
        let mut last_worker_target: usize = usize::MAX;
        loop {
            tokio::select! {
                _ = jobs_available.notified() => {}
                _ = tick.tick() => {}
            }
            dispatch_jobs(&server, &registry).await;
            topup_submission_credits(&server, &registry);

            // Edge-triggered fleet pressure signal: broadcast once on each
            // transition so workers can coordinate without polling. O(streams)
            // only when the state actually flips. Reflects the drain-stall guard
            // too, so a drain collapse turns the fleet red (workers back off new
            // creates) even in the admission SLA mode where latency never sheds.
            let pressure = server.create_admission_blocked();
            if pressure != last_pressure {
                let frame = if pressure {
                    ServerFrame::Pressure {
                        level: "red".to_string(),
                        retry_after_ms: Some(DISPATCH_TICK_MS),
                    }
                } else {
                    ServerFrame::Pressure {
                        level: "green".to_string(),
                        retry_after_ms: None,
                    }
                };
                broadcast(&registry, &frame);
                last_pressure = pressure;
            }

            // Edge-triggered worker-concurrency advice: whenever the governor
            // moves its active dispatch width, tell the fleet the new target so
            // cooperating clients can self-size their subscriber pools. Only
            // broadcast on a real change (the cap is stable at steady state), and
            // only when the governor is enabled (cap != 0).
            let worker_target = server.active_worker_cap();
            if worker_target != 0 && worker_target != last_worker_target {
                broadcast(
                    &registry,
                    &ServerFrame::WorkerAdvice {
                        recommended_concurrency: worker_target as i64,
                    },
                );
                last_worker_target = worker_target;
            }
        }
    });
    spawn_reaper(registry_for_reaper);
}

/// Spawns the phantom-connection reaper: a frozen client or a network partition
/// can leave a socket open (no FIN) with `reader_loop` blocked forever, holding a
/// dispatch slot and submission credits. The SDK client heartbeats on a fixed
/// cadence, so a connection that has sent nothing for [`LIVENESS_TIMEOUT_MS`] is
/// treated as dead: we mark it closed, wake its reader to tear down (releasing the
/// socket), and unregister it. Jobs it had leased were already protected by the
/// lock deadline and are reclaimed independently.
fn spawn_reaper(registry: Arc<Registry>) {
    tokio::spawn(async move {
        let timeout_ms = liveness_timeout_ms();
        let mut tick = tokio::time::interval(Duration::from_millis(reaper_interval_ms()));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let now = now_millis();
            for conn in registry.all_connections() {
                let last = conn.last_seen_ms.load(Ordering::Relaxed);
                if now.saturating_sub(last) >= timeout_ms {
                    conn.closed.store(true, Ordering::Relaxed);
                    // Break the reader loop; handle_socket then unregisters.
                    // notify_one persists a permit, so this is race-free even if
                    // the reader is not parked at this exact instant.
                    conn.shutdown.notify_one();
                    registry.unregister(conn.id);
                }
            }
        }
    });
}

/// Sends a frame to every live connection (best-effort).
fn broadcast(registry: &Arc<Registry>, frame: &ServerFrame) {
    for conn in registry.all_connections() {
        conn.send(frame.clone());
    }
}

/// Wall-clock millis since the Unix epoch (the engine itself is clock-free; this
/// is only for connection liveness, not journaled state).
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// One dispatch pass. Connections are serviced CONCURRENTLY, bounded by
/// [`dispatch_concurrency`]: each connection is an independent unit of work that
/// awaits its own activation round-trips, so many activations are in flight across
/// the partition engine threads at once instead of one connection at a time. This
/// fills the engine threads' idle headroom (profiling showed them ~16% busy under
/// the old serial loop — the dispatcher, not the engine, was the ceiling) without
/// weakening at-least-once: job leases are serialized on each engine thread, and
/// per-connection credit/socket-room accounting is owned by exactly one task
/// because the work is sharded by connection. Bounding the concurrency keeps the
/// activation (High-priority) load from swamping completions in the engine mailbox.
async fn dispatch_jobs(server: &ServerImpl, registry: &Arc<Registry>) {
    // Regroup the round-robin plan by connection so each connection's whole
    // workload (every job type it subscribes to) is handled by a single task,
    // keeping its credit and socket-room mutations race-free across the pass.
    #[allow(clippy::type_complexity)]
    let mut by_conn: Vec<(Arc<Connection>, Vec<(String, Arc<Subscription>)>)> = Vec::new();
    let mut index: HashMap<ConnId, usize> = HashMap::new();
    // The worker governor's active dispatch width: cap each job type's per-pass
    // subscriber fan-out so a fixed job supply is concentrated on the streams that
    // keep the drain saturated, instead of diluted across an over-provisioned fleet
    // (which swamps completions with High-priority activations). 0 = no cap.
    let per_type_cap = server.active_worker_cap();
    for (job_type, targets) in registry.dispatch_plan(per_type_cap) {
        for (conn, sub) in targets {
            let slot = *index.entry(conn.id).or_insert_with(|| {
                by_conn.push((conn.clone(), Vec::new()));
                by_conn.len() - 1
            });
            by_conn[slot].1.push((job_type.clone(), sub));
        }
    }
    if by_conn.is_empty() {
        return;
    }
    // Fan the per-connection work out across the runtime's worker threads rather
    // than polling it all on this single dispatcher task. Each connection is still
    // one task (credit/socket-room accounting stays race-free per pass), but the
    // synchronous CPU between awaits — job JSON serialization, frame push — now
    // spreads across cores instead of pinning the dispatcher thread (profiling
    // showed one core at 100% while the engine actors and workers had headroom).
    // A semaphore keeps the in-flight activation count bounded exactly as the old
    // `buffer_unordered` did, so completions are not swamped in the engine mailbox.
    let concurrency = dispatch_concurrency();
    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut set = tokio::task::JoinSet::new();
    for (conn, work) in by_conn {
        let server = server.clone();
        let sem = sem.clone();
        set.spawn(async move {
            let _permit = sem.acquire_owned().await;
            dispatch_to_connection(&server, conn, work).await;
        });
    }
    while set.join_next().await.is_some() {}
}

/// Serializes an activated job exactly once — directly to a raw JSON value,
/// skipping the intermediate `serde_json::Value` tree the old path allocated —
/// and enqueues it to the connection. The writer then emits the raw bytes
/// verbatim, so a job is serialized once total on the hot dispatch path. Returns
/// whether the outbound buffer accepted it.
fn send_job<T: serde::Serialize>(conn: &Connection, job: &T) -> bool {
    match serde_json::value::to_raw_value(job) {
        Ok(job) => conn.send(ServerFrame::Job { job }),
        Err(_) => false,
    }
}

/// Services one connection for a dispatch pass: leases and pushes jobs for each of
/// its subscribed job types, in round-robin order, until its credits or outbound
/// socket room run out. Because the pass shards by connection, this task is the
/// sole mutator of `sub.credits` and the connection's socket for the pass, so the
/// relaxed atomics need no cross-task coordination.
async fn dispatch_to_connection(
    server: &ServerImpl,
    conn: Arc<Connection>,
    work: Vec<(String, Arc<Subscription>)>,
) {
    if conn.closed.load(Ordering::Relaxed) {
        return;
    }
    // Remote nodes to draw jobs from once the local pool is drained (job
    // aggregation). Empty on a single-node cluster ⇒ the peer-pull loop never
    // runs and this pass is byte-identical to the pre-cluster dispatcher.
    let peers = server.peer_nodes();
    for (job_type, sub) in work {
        let credits = sub.credits.load(Ordering::Relaxed);
        if credits <= 0 {
            continue;
        }
        // Never lease more than we can immediately enqueue to this socket.
        let room = conn.tx.capacity() as i64;
        if room <= 0 {
            // Credited demand we cannot satisfy because the outbound buffer is
            // full: arm a redispatch so the writer wakes us the moment a slot
            // frees, instead of relying on the backstop tick.
            conn.wants_redispatch.store(true, Ordering::Relaxed);
            continue;
        }
        let want = credits.min(room).min(PER_STREAM_BATCH as i64) as usize;
        if want == 0 {
            continue;
        }
        let mut pushed = 0i64;

        let mode = activation_mode();
        if mode == FairnessMode::Off || peers.is_empty() {
            // Historical local-first dispatch (and the only path on a single
            // node, where `peers` is empty): drain local, then pull the
            // shortfall from peers in fixed id order.

            // 1. Local partitions first — the hot path, no network hop.
            let local = server
                .activate_for_stream(
                    &job_type,
                    &sub.worker,
                    want,
                    sub.timeout,
                    sub.fetch_variable.as_deref(),
                )
                .await;
            for job in local {
                if send_job(&conn, &job) {
                    pushed += 1;
                }
            }

            // 2. Cluster: pull the shortfall from peers' partitions so a worker
            //    attached to this gateway is fed by the whole cluster. The peer
            //    leases each job under `sub.timeout`, preserving at-least-once if
            //    this gateway dies before the worker completes (the lease expires
            //    on the owner).
            let mut remaining = want.saturating_sub(pushed as usize);
            if remaining > 0 && !peers.is_empty() {
                for &node in &peers {
                    if remaining == 0 {
                        break;
                    }
                    let room_now = conn.tx.capacity() as i64;
                    if room_now <= 0 {
                        conn.wants_redispatch.store(true, Ordering::Relaxed);
                        break;
                    }
                    let ask = remaining.min(room_now as usize);
                    let jobs = server
                        .activate_from_peer(
                            node,
                            &job_type,
                            &sub.worker,
                            ask,
                            sub.timeout,
                            sub.fetch_variable.as_deref(),
                        )
                        .await;
                    for job in jobs {
                        if send_job(&conn, &job) {
                            pushed += 1;
                            remaining = remaining.saturating_sub(1);
                        }
                    }
                }
            }
        } else {
            // Fairness-aware dispatch (multi-node). Sources are the local engine
            // (index 0) and each peer (1..=peers.len()). The plan is an ordered list
            // of `(source, cap)` probes that the loop walks until `want` is met.
            //   * Stage 1 (`=1`): `fair_plan` rotates the source order and quota-caps
            //     each source equally so a fat backlog on one node cannot consume the
            //     whole budget — purely stateless.
            //   * Stage 2 (`=2`): `fair_plan_weighted` caps each source proportional
            //     to its current backlog (local read live, peers piggybacked on their
            //     activation responses — no extra round-trip), so budget is steered
            //     toward where the jobs actually are — fewer wasted probes on
            //     shallow/empty sources and faster drain of the deepest. Collapses to
            //     Stage 1 when backlogs are balanced.
            // Leases remain exclusive on each owner's single-writer actor, so this
            // only redistributes *where* a worker draws from, never correctness.
            let num_sources = peers.len() + 1;
            let start = (DISPATCH_ROTATION.fetch_add(1, Ordering::Relaxed) as usize) % num_sources;
            let plan = if mode == FairnessMode::Stage2 {
                // Source backlogs: local is read live (cheap atomic); peers come
                // from the value each piggybacked on its last activation response.
                // A peer not yet probed is seeded with the local backlog so it is
                // not starved before its first sample (the rotation/soak laps probe
                // it and replace the seed with its real depth).
                let local = server.active_backlog();
                let hints: Vec<i64> = (0..num_sources)
                    .map(|s| {
                        if s == 0 {
                            local
                        } else {
                            read_peer_backlog(source_node(s, &peers)).unwrap_or(local)
                        }
                    })
                    .collect();
                fair_plan_weighted(want, &hints, start)
            } else {
                fair_plan(want, num_sources, start)
            };
            for (src, cap) in plan {
                let remaining = want.saturating_sub(pushed as usize);
                if remaining == 0 {
                    break;
                }
                let room_now = conn.tx.capacity() as i64;
                if room_now <= 0 {
                    conn.wants_redispatch.store(true, Ordering::Relaxed);
                    break;
                }
                let ask = cap.min(remaining).min(room_now as usize);
                if ask == 0 {
                    continue;
                }
                let jobs = if src == 0 {
                    server
                        .activate_for_stream(
                            &job_type,
                            &sub.worker,
                            ask,
                            sub.timeout,
                            sub.fetch_variable.as_deref(),
                        )
                        .await
                } else {
                    // `activate_from_peer` records the peer's piggybacked backlog
                    // into the cache, refreshing this source's hint for free.
                    server
                        .activate_from_peer(
                            peers[src - 1],
                            &job_type,
                            &sub.worker,
                            ask,
                            sub.timeout,
                            sub.fetch_variable.as_deref(),
                        )
                        .await
                };
                for job in jobs {
                    if send_job(&conn, &job) {
                        pushed += 1;
                    }
                }
            }
        }

        if pushed > 0 {
            sub.credits.fetch_sub(pushed, Ordering::Relaxed);
            crate::metrics::record_jobs_dispatched(&job_type, pushed as u64);
        }
    }
}

/// Rotates the source order (local + each peer) across activation passes so a
/// worker is not always fed local-partition-first. Process-global; a relaxed
/// counter is all the fairness rotation needs.
static DISPATCH_ROTATION: AtomicU64 = AtomicU64::new(0);

/// Plans how a worker's lease budget (`want` jobs) is spread across `num_sources`
/// activation sources (local + each peer), starting from rotated source `start`.
///
/// Returns an ordered list of `(source_index, cap)` probes the dispatcher walks,
/// stopping once `want` is met:
///   * **Lap 1** caps each source at `ceil(want / num_sources)` so no single
///     source (a fat local backlog in particular) can monopolise the budget —
///     this is what breaks the local-first starvation.
///   * **Lap 2** revisits every source with an uncapped allowance so demand left
///     unmet by sources that ran dry is soaked up, keeping the worker full when
///     the cluster as a whole has the jobs.
///
/// Pure and deterministic given its inputs; the async dispatcher executes the
/// plan, computing each actual `ask` from the live remaining demand and socket
/// room. `num_sources == 1` (single node / no peers) yields a single uncapped
/// local probe, i.e. the historical behaviour.
fn fair_plan(want: usize, num_sources: usize, start: usize) -> Vec<(usize, usize)> {
    if num_sources <= 1 {
        return vec![(0, want)];
    }
    let base_quota = want.div_ceil(num_sources).max(1);
    let mut plan = Vec::with_capacity(num_sources * 2);
    // Lap 1: rotated order, each source capped at its quota.
    for k in 0..num_sources {
        plan.push(((start + k) % num_sources, base_quota));
    }
    // Lap 2: rotated order, uncapped — soaks leftover from dry sources.
    for k in 0..num_sources {
        plan.push(((start + k) % num_sources, want));
    }
    plan
}

/// Whether (and how) to use fairness-aware activation routing across cluster
/// nodes.
///
/// `NANOBPMN_ACTIVATION_FAIRNESS` selects the mode (**defaults to `Stage2`** when
/// unset — the self-optimizing posture; the engine steers each worker's lease
/// budget toward where the jobs actually are, on its own):
///   * `0` / `off` / `false` / `no` / `none` → `Off`: strict local-first, then
///     peers in fixed id order — the historical behaviour. An explicit opt-out.
///   * `1` / `true` / `on` / `yes` → `Stage1`: stateless rotation + per-source
///     quota. Spreads a worker's lease budget evenly across `{local, peers}` so a
///     fat local backlog cannot monopolise a worker while peers' partitions
///     starve. No protocol change.
///   * unset / `2` / `weighted` / `stage2` → `Stage2` (default): backlog-weighted
///     routing. Caps each source proportional to its current backlog — local read
///     live, peers from a value each piggybacks on its activation response (no
///     extra round-trip, no new RPC) — steering budget toward where the jobs are.
///     Collapses to Stage 1 when backlogs are balanced.
///
/// Only affects multi-node clusters — with no peers there is a single source and
/// every mode collapses to the historical local-only dispatch, so the `Stage2`
/// default is byte-identical on a single node.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FairnessMode {
    Off,
    Stage1,
    Stage2,
}

fn activation_mode() -> FairnessMode {
    static M: std::sync::OnceLock<FairnessMode> = std::sync::OnceLock::new();
    *M.get_or_init(|| {
        match std::env::var("NANOBPMN_ACTIVATION_FAIRNESS")
            .ok()
            .as_deref()
            .map(str::trim)
        {
            Some("0") | Some("off") | Some("false") | Some("no") | Some("none") => {
                FairnessMode::Off
            }
            Some("1") | Some("true") | Some("on") | Some("yes") => FairnessMode::Stage1,
            // unset / "2" / "weighted" / "stage2" / anything unrecognised resolve
            // to the self-optimizing default.
            _ => FairnessMode::Stage2,
        }
    })
}

/// Sentinel node id for the local engine in the backlog-hint cache (real peer ids
/// are small; `u32::MAX` cannot collide).
const LOCAL_SOURCE_NODE: u32 = u32::MAX;

/// Maps a dispatch source index to its node id: source 0 is the local engine;
/// source `k` (k≥1) is `peers[k-1]`.
fn source_node(src: usize, peers: &[u32]) -> u32 {
    if src == 0 {
        LOCAL_SOURCE_NODE
    } else {
        peers[src - 1]
    }
}

/// Last-known per-peer backlog (active-instance count), refreshed every time we
/// activate from that peer — the peer piggybacks its current backlog on the
/// activation response, so this costs no extra round-trip. Process-global; a tiny
/// critical section under a mutex is nothing next to the network hop that fills
/// it. `None` for a peer we have not probed yet.
fn peer_backlog_cache() -> &'static Mutex<HashMap<u32, i64>> {
    static H: std::sync::OnceLock<Mutex<HashMap<u32, i64>>> = std::sync::OnceLock::new();
    H.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Records a peer's piggybacked backlog. Called from `activate_from_peer` (in
/// `main`) on every successful activation response.
pub(crate) fn record_peer_backlog(node: u32, backlog: i64) {
    peer_backlog_cache()
        .lock()
        .unwrap()
        .insert(node, backlog.max(0));
}

fn read_peer_backlog(node: u32) -> Option<i64> {
    peer_backlog_cache().lock().unwrap().get(&node).copied()
}

/// Stage 2 plan: spread a worker's lease budget (`want`) across `num_sources`
/// sources (local + each peer) *proportional to each source's backlog* `hints`,
/// walking sources in rotated order from `start`.
///
///   * **Lap 1** caps each source at `ceil(want · hint_i / Σhint)` so deeper
///     backlogs draw a larger share and shallow/empty ones are barely probed —
///     this both equalises backlogs (drains the deepest fastest, the negative
///     feedback that closes the SLA gap a static even split leaves under skew) and
///     avoids the wasted round-trips Stage 1's blind even split spends on shallow
///     sources. The rotation-start source keeps a floor of 1 so a believed-empty
///     source is periodically re-checked and its hint refreshed.
///   * **Lap 2** revisits every source uncapped (rotated order) to soak any demand
///     left unmet, keeping the worker full when the cluster has the jobs.
///
/// When all hints are 0 it falls back to the Stage 1 even split so every source is
/// probed and learns a hint. When backlogs are balanced the proportional caps
/// equal `ceil(want / num_sources)` — i.e. identical to Stage 1, so a balanced
/// cluster sees no behaviour change. Pure and deterministic.
fn fair_plan_weighted(want: usize, hints: &[i64], start: usize) -> Vec<(usize, usize)> {
    let n = hints.len();
    if n <= 1 {
        return vec![(0, want)];
    }
    let total: i64 = hints.iter().map(|h| (*h).max(0)).sum();
    if total <= 0 || want == 0 {
        // Cold start / nothing believed available: even split so we probe and learn.
        return fair_plan(want, n, start);
    }
    let mut plan = Vec::with_capacity(n * 2);
    // Lap 1: rotated order, proportional caps.
    for k in 0..n {
        let src = (start + k) % n;
        let w = hints[src].max(0);
        let mut cap = ((want as i64 * w + total - 1) / total) as usize; // ceil
        if k == 0 {
            cap = cap.max(1); // refresh the rotation-start source
        }
        if cap > 0 {
            plan.push((src, cap));
        }
    }
    // Lap 2: rotated order, uncapped — soaks leftover from sources that filled.
    for k in 0..n {
        plan.push(((start + k) % n, want));
    }
    plan
}

/// Number of connections the dispatcher services concurrently in one pass.
/// `NANOBPMN_DISPATCH_CONCURRENCY` overrides the default; values are clamped to at
/// least 1. The default fans out enough activation round-trips to keep the engine
/// threads busy (they idled at ~16% under fully serial dispatch) without an
/// unbounded flood of High-priority activations that would crowd out completions.
fn dispatch_concurrency() -> usize {
    std::env::var("NANOBPMN_DISPATCH_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|n| n.max(1))
        .unwrap_or(DEFAULT_DISPATCH_CONCURRENCY)
}

/// Refills each connection's submission window when the engine has headroom, so a
/// client that stalled under pressure resumes intake once pressure clears.
fn topup_submission_credits(server: &ServerImpl, registry: &Arc<Registry>) {
    if server.create_admission_blocked() {
        return;
    }
    for conn in registry.all_connections() {
        if conn.closed.load(Ordering::Relaxed) {
            continue;
        }
        let outstanding = conn.submission_outstanding.load(Ordering::Relaxed);
        let grant = conn.submission_window - outstanding;
        if grant > 0 {
            conn.grant_submission_credits(grant);
        }
    }
}

#[cfg(test)]
mod fair_plan_tests {
    use super::fair_plan;

    /// Executes a `fair_plan` against a per-source supply (how many jobs each
    /// source can actually yield), returning the per-source counts the worker
    /// would receive. Mirrors the dispatcher loop: each probe takes
    /// `min(cap, remaining, supply_left)` from its source.
    fn run(want: usize, supply: &[usize], start: usize) -> Vec<usize> {
        let n = supply.len();
        let mut got = vec![0usize; n];
        let mut pushed = 0usize;
        let mut left: Vec<usize> = supply.to_vec();
        for (src, cap) in fair_plan(want, n, start) {
            let remaining = want - pushed;
            if remaining == 0 {
                break;
            }
            let ask = cap.min(remaining).min(left[src]);
            got[src] += ask;
            left[src] -= ask;
            pushed += ask;
        }
        got
    }

    #[test]
    fn single_source_takes_everything_uncapped() {
        // No peers (num_sources == 1): historical local-only behaviour.
        assert_eq!(fair_plan(64, 1, 0), vec![(0, 64)]);
        assert_eq!(run(64, &[1000], 0), vec![64]);
    }

    #[test]
    fn abundant_supply_spreads_evenly_no_monopoly() {
        // Local (src 0) has a huge backlog but must not monopolise the budget:
        // each source is quota-capped to ceil(64/3)=22 on lap 1.
        let got = run(64, &[10_000, 10_000, 10_000], 0);
        assert_eq!(got.iter().sum::<usize>(), 64);
        // Every source contributes; the spread is within one quota of even.
        assert!(got.iter().all(|&c| c > 0), "no source starved: {got:?}");
        assert!(*got.iter().max().unwrap() - *got.iter().min().unwrap() <= 22);
    }

    #[test]
    fn local_backlog_does_not_starve_a_backed_up_peer() {
        // The reported bug: local always has jobs, so strict local-first never
        // reaches the peer. With fairness, peers still get drawn from.
        let got = run(60, &[10_000, 500, 500], 0);
        assert_eq!(got.iter().sum::<usize>(), 60);
        assert!(got[1] > 0 && got[2] > 0, "peers must be served: {got:?}");
    }

    #[test]
    fn leftover_from_dry_sources_is_soaked_by_others() {
        // Peers are empty; lap 2 lets the local source soak the full budget so
        // the worker still fills rather than going hungry.
        let got = run(64, &[10_000, 0, 0], 0);
        assert_eq!(got, vec![64, 0, 0]);
    }

    #[test]
    fn conserves_to_total_supply_when_cluster_is_underfull() {
        // Total available (30) < want (64): take everything, no over-count.
        let got = run(64, &[10, 10, 10], 1);
        assert_eq!(got, vec![10, 10, 10]);
    }

    #[test]
    fn rotation_changes_which_source_is_short() {
        // An uneven split (65 over 3) shorts the LAST source served by one unit;
        // rotating `start` moves that disadvantage around so no source is
        // permanently penalised over many passes.
        let a = run(65, &[10_000, 10_000, 10_000], 0); // order 0,1,2 -> src2 short
        let b = run(65, &[10_000, 10_000, 10_000], 1); // order 1,2,0 -> src0 short
        assert_eq!(a.iter().sum::<usize>(), 65);
        assert_eq!(b.iter().sum::<usize>(), 65);
        assert_eq!(a, vec![22, 22, 21]);
        assert_eq!(b, vec![21, 22, 22]);
    }
}

#[cfg(test)]
mod registry_tests {
    use super::*;

    #[test]
    fn fire_and_forget_spawns_only_with_an_in_flight_slot() {
        // Compliant producer under Raft holding a free in-flight-create slot ->
        // spawn the concurrent fire-and-forget path.
        assert!(should_spawn_fire_and_forget(false, true, true));

        // No free slot (the connection has a full window of creates already in
        // flight — genuinely over budget): fall to the inline (serialized) path so
        // the reader blocks and TCP backpressure throttles just this socket.
        assert!(
            !should_spawn_fire_and_forget(false, true, false),
            "no in-flight slot must not spawn"
        );

        // awaitCompletion always stays inline (completion wait needs local reads),
        // regardless of slot availability.
        assert!(!should_spawn_fire_and_forget(true, true, true));

        // Single-node (no Raft): the spawn path does not apply; creates are inline.
        assert!(!should_spawn_fire_and_forget(false, false, true));
    }

    fn test_connection(id: ConnId) -> Arc<Connection> {
        let (tx, _rx) = mpsc::channel::<ServerFrame>(1);
        Arc::new(Connection {
            id,
            tx,
            subs: Mutex::new(HashMap::new()),
            submission_outstanding: AtomicI64::new(0),
            submission_window: 0,
            create_slots: Arc::new(tokio::sync::Semaphore::new(0)),
            closed: AtomicBool::new(false),
            wants_redispatch: Arc::new(AtomicBool::new(false)),
            last_seen_ms: AtomicU64::new(0),
            shutdown: Notify::new(),
        })
    }

    #[test]
    fn unregister_is_idempotent_so_the_active_gauge_cannot_drift_negative() {
        // The reaper and handle_socket can both unregister the SAME connection
        // (reaper marks it dead + unregisters; the reader loop then returns and
        // handle_socket unregisters again). The disconnect must be counted exactly
        // once — the second call returns false and does NOT decrement the gauge.
        let registry = Registry::new();
        let conn = test_connection(7);
        registry.register(conn.clone());

        assert_eq!(registry.all_connections().len(), 1, "registered once");
        assert!(registry.unregister(7), "first unregister removes + counts");
        assert!(registry.all_connections().is_empty(), "connection gone");
        assert!(
            !registry.unregister(7),
            "second unregister is a no-op: it must not decrement the gauge again"
        );
        assert!(
            !registry.unregister(999),
            "unregistering an unknown id is a no-op too"
        );
    }

    fn test_connection_with_rx(
        id: ConnId,
        cap: usize,
    ) -> (Arc<Connection>, mpsc::Receiver<ServerFrame>) {
        let (tx, rx) = mpsc::channel::<ServerFrame>(cap);
        let conn = Arc::new(Connection {
            id,
            tx,
            subs: Mutex::new(HashMap::new()),
            submission_outstanding: AtomicI64::new(0),
            submission_window: 0,
            create_slots: Arc::new(tokio::sync::Semaphore::new(0)),
            closed: AtomicBool::new(false),
            wants_redispatch: Arc::new(AtomicBool::new(false)),
            last_seen_ms: AtomicU64::new(0),
            shutdown: Notify::new(),
        });
        (conn, rx)
    }

    #[test]
    fn a_dropped_credit_grant_does_not_inflate_outstanding() {
        // Regression: `send` drops frames when the outbound buffer is full. If the
        // grant is accounted before the (dropped) send, `submission_outstanding`
        // inflates permanently and `topup`'s `window - outstanding` computes 0
        // forever, wedging the producer. grant_submission_credits must account only
        // on a successful enqueue.
        let (conn, _rx) = test_connection_with_rx(1, 1);
        // tx buffer capacity is 1; fill it so the next enqueue fails.
        assert!(
            conn.send(ServerFrame::SubmissionCredits { n: 1 }),
            "buffer has room"
        );
        assert!(
            !conn.grant_submission_credits(8),
            "grant is dropped when the outbound buffer is full"
        );
        assert_eq!(
            conn.submission_outstanding.load(Ordering::Relaxed),
            0,
            "a dropped grant must NOT inflate outstanding"
        );
    }

    #[test]
    fn a_delivered_credit_grant_accounts_outstanding() {
        let (conn, _rx) = test_connection_with_rx(1, 4);
        assert!(
            conn.grant_submission_credits(5),
            "grant enqueues when the buffer has room"
        );
        assert_eq!(
            conn.submission_outstanding.load(Ordering::Relaxed),
            5,
            "a delivered grant accounts exactly n"
        );
    }
}

#[cfg(test)]
mod fair_plan_weighted_tests {
    use super::{fair_plan, fair_plan_weighted};

    /// Executes a weighted plan against a per-source supply, mirroring the
    /// dispatcher loop (each probe takes `min(cap, remaining, supply_left)`).
    fn run(want: usize, hints: &[i64], supply: &[usize], start: usize) -> Vec<usize> {
        let n = supply.len();
        let mut got = vec![0usize; n];
        let mut pushed = 0usize;
        let mut left: Vec<usize> = supply.to_vec();
        for (src, cap) in fair_plan_weighted(want, hints, start) {
            let remaining = want - pushed;
            if remaining == 0 {
                break;
            }
            let ask = cap.min(remaining).min(left[src]);
            got[src] += ask;
            left[src] -= ask;
            pushed += ask;
        }
        got
    }

    #[test]
    fn single_source_takes_everything() {
        assert_eq!(fair_plan_weighted(64, &[10_000], 0), vec![(0, 64)]);
    }

    #[test]
    fn cold_start_falls_back_to_even_split() {
        // All hints 0 (nothing learned yet): identical to the Stage 1 even split,
        // so every source is probed and learns a hint.
        assert_eq!(fair_plan_weighted(64, &[0, 0, 0], 0), fair_plan(64, 3, 0));
    }

    #[test]
    fn balanced_backlog_matches_stage1() {
        // Equal hints ⇒ proportional caps equal ceil(want/n) ⇒ no behaviour change
        // versus Stage 1 on a balanced cluster.
        let w = fair_plan_weighted(64, &[500, 500, 500], 0);
        let s1 = fair_plan(64, 3, 0);
        // Lap-1 caps match (lap-2 soak is identical by construction).
        assert_eq!(&w[..3], &s1[..3]);
    }

    #[test]
    fn skewed_backlog_steers_budget_to_the_deepest() {
        // Source 1 holds the lion's share of the backlog; it should draw the
        // largest cap and thus the most jobs, draining the deepest fastest.
        let hints = [100, 800, 100];
        let got = run(64, &hints, &[10_000, 10_000, 10_000], 0);
        assert_eq!(got.iter().sum::<usize>(), 64);
        assert!(
            got[1] > got[0] && got[1] > got[2],
            "deepest source should be served most: {got:?}"
        );
    }

    #[test]
    fn empty_sources_are_barely_probed_but_not_starved_of_refresh() {
        // Sources 1,2 believed empty; nearly all budget goes to the deep source 0,
        // but the rotation-start source still gets a refresh probe of >= 1.
        let plan = fair_plan_weighted(60, &[600, 0, 0], 0);
        // Start source (0) is deep and first; give a skewed-empty start a refresh.
        let plan2 = fair_plan_weighted(60, &[0, 600, 0], 0);
        let start_cap = plan2
            .iter()
            .find(|(s, _)| *s == 0)
            .map(|(_, c)| *c)
            .unwrap();
        assert!(
            start_cap >= 1,
            "rotation-start source keeps a refresh probe"
        );
        // The deep source still carries the bulk.
        let deep_cap: usize = plan
            .iter()
            .filter(|(s, _)| *s == 0)
            .map(|(_, c)| *c)
            .max()
            .unwrap();
        assert!(deep_cap >= 30, "deep source carries the budget: {plan:?}");
    }

    #[test]
    fn soaks_residual_when_deep_estimate_was_stale() {
        // Hint says source 0 is deep, but it only has 5 jobs now; the uncapped lap 2
        // soaks the rest from the genuinely-available peers so the worker fills.
        let got = run(60, &[1000, 10, 10], &[5, 10_000, 10_000], 0);
        assert_eq!(got.iter().sum::<usize>(), 60);
        assert_eq!(got[0], 5, "drained the shallow-but-believed-deep source");
    }
}

/// Drift guard: the public falcon protocol is hand-documented in
/// `docs/falcon.asyncapi.yaml` (a WebSocket protocol can't be modelled
/// by OpenAPI, so it is not code-generated). These tests pin the spec to the
/// `ClientFrame` / `ServerFrame` enums it claims to mirror, so the spec — and
/// the `/asyncapi` docs generated from it — cannot silently drift out of sync.
#[cfg(test)]
mod asyncapi_spec_guard {
    use std::collections::BTreeSet;

    use serde_json::json;

    use super::{ClientFrame, ServerFrame};

    /// The spec, embedded at compile time (repo `docs/`, two levels up from
    /// `server/src/`). If this path breaks, the spec moved and the docs pipeline
    /// (`console/scripts/copy-asyncapi.mjs`) needs the same update.
    const SPEC: &str = include_str!("../../docs/falcon.asyncapi.yaml");

    /// The public client→server frames. The many intra-cluster `ClientFrame`
    /// variants (deploy, raft, forwardCreate, getByKey, …) are deliberately NOT
    /// part of the public protocol and are intentionally absent from the spec.
    const PUBLIC_CLIENT: &[&str] = &[
        "subscribe",
        "jobCredits",
        "createInstance",
        "completeJob",
        "failJob",
        "throwError",
        "awaitInstance",
        "heartbeat",
    ];

    /// Every server→client frame — `ServerFrame` has no internal-only variants,
    /// so this is the full enum and the guard below derives it from the type.
    const PUBLIC_SERVER: &[&str] = &[
        "welcome",
        "job",
        "commandResult",
        "instanceCompleted",
        "submissionCredits",
        "pressure",
        "heartbeat",
    ];

    /// Collect every message discriminator the spec documents, i.e. each
    /// `const: <x>` declared on a `type` property in the schemas section.
    fn documented_discriminators() -> BTreeSet<String> {
        SPEC.lines()
            .filter_map(|l| l.trim().strip_prefix("const:"))
            .map(|c| c.trim().trim_matches('"').to_string())
            .collect()
    }

    #[test]
    fn spec_documents_exactly_the_public_protocol() {
        let documented = documented_discriminators();
        let expected: BTreeSet<String> = PUBLIC_CLIENT
            .iter()
            .chain(PUBLIC_SERVER)
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            documented, expected,
            "falcon.asyncapi.yaml documents a different frame set than the public \
             protocol contract — update the spec (and PUBLIC_CLIENT/PUBLIC_SERVER if the \
             public protocol genuinely changed)."
        );
    }

    #[test]
    fn server_frame_type_tags_match_the_spec() {
        // Construct every ServerFrame variant so the set of `type` tags is derived
        // straight from the enum: add a variant and this fails until it is documented.
        let all = [
            ServerFrame::Welcome {
                submission_credits: 1,
                heartbeat_ms: 1,
            },
            ServerFrame::Job {
                job: serde_json::value::to_raw_value(&json!({})).unwrap(),
            },
            ServerFrame::CommandResult {
                corr: 1,
                status: 200,
                body: None,
            },
            ServerFrame::InstanceCompleted {
                corr: 1,
                process_instance_key: "1".into(),
                process_completed: true,
                variables: json!({}),
            },
            ServerFrame::SubmissionCredits { n: 1 },
            ServerFrame::Pressure {
                level: "ok".into(),
                retry_after_ms: None,
            },
            ServerFrame::Heartbeat,
        ];
        let tags: BTreeSet<String> = all
            .iter()
            .map(|f| {
                serde_json::to_value(f).unwrap()["type"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        let expected: BTreeSet<String> = PUBLIC_SERVER.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            tags, expected,
            "ServerFrame's serialized `type` tags drifted from the documented server frames."
        );
    }

    #[test]
    fn public_client_frames_deserialize_under_their_documented_type() {
        // A minimal valid frame for each documented client `type` must decode into
        // a ClientFrame — proving the discriminator the spec advertises is real.
        let minimal = |t: &str| match t {
            "subscribe" => json!({"type":"subscribe","jobType":"x"}),
            "jobCredits" => json!({"type":"jobCredits","jobType":"x","n":1}),
            "createInstance" => json!({"type":"createInstance","corr":1}),
            "completeJob" => json!({"type":"completeJob","corr":1,"jobKey":"1"}),
            "failJob" => json!({"type":"failJob","corr":1,"jobKey":"1"}),
            "throwError" => json!({"type":"throwError","corr":1,"jobKey":"1","errorCode":"E"}),
            "awaitInstance" => json!({"type":"awaitInstance","corr":1,"processInstanceKey":"1"}),
            "heartbeat" => json!({"type":"heartbeat"}),
            other => panic!("no minimal frame for documented client type {other:?}"),
        };
        for &t in PUBLIC_CLIENT {
            let frame: ClientFrame = serde_json::from_value(minimal(t))
                .unwrap_or_else(|e| panic!("documented client type {t:?} no longer decodes: {e}"));
            let got = serde_json::to_value(&frame).unwrap()["type"]
                .as_str()
                .unwrap()
                .to_string();
            assert_eq!(got, t, "client type {t:?} round-trips to a different tag");
        }
    }
}
