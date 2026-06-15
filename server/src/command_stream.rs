//! Unified bidirectional command stream over WebSocket (§13 of
//! `docs/command-stream-design.md`).
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
//!   rather than a held request.
//!
//! The engine core is untouched: this is purely a new ingress to the existing
//! command path and a new consumer of `activate_jobs`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use futures_util::stream::StreamExt;
use futures_util::SinkExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::mpsc;

use crate::ServerImpl;

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
/// Bound on the per-connection outbound frame buffer (slow-consumer guard).
const OUTBOUND_CHANNEL_CAP: usize = 1024;

type ConnId = u64;

/// One round-robin–ordered dispatch target: a live connection and its
/// subscription for a given job type.
type DispatchTarget = (Arc<Connection>, Arc<Subscription>);

// ----------------------------------------------------------------------------
// Frame protocol (tagged union; wire form is camelCase JSON over text frames).
// ----------------------------------------------------------------------------

/// Client → server frames.
#[derive(Debug, Deserialize)]
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
    JobCredits { job_type: String, n: i64 },
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
    Heartbeat,
}

/// Server → client frames.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ServerFrame {
    /// Sent once on connect: the initial submission window and heartbeat cadence.
    #[serde(rename_all = "camelCase")]
    Welcome {
        submission_credits: i64,
        heartbeat_ms: u64,
    },
    /// A pushed activated job (consumes one job-delivery credit). `job` is the
    /// same shape as a REST `ActivatedJobResult`.
    Job { job: Value },
    /// Ack/result for a create/complete/fail/throwError, correlated by `corr`.
    #[serde(rename_all = "camelCase")]
    CommandResult {
        corr: u64,
        status: u16,
        #[serde(skip_serializing_if = "Option::is_none")]
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
    SubmissionCredits { n: i64 },
    /// Coarse fleet pressure signal.
    #[serde(rename_all = "camelCase")]
    Pressure {
        level: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        retry_after_ms: Option<u64>,
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
    closed: AtomicBool,
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
}

/// Server-wide registry of command-stream connections and the job-type dispatch
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
        self.conns.lock().expect("registry poisoned").insert(conn.id, conn);
    }

    fn unregister(&self, id: ConnId) {
        if let Some(conn) = self.conns.lock().expect("registry poisoned").remove(&id) {
            conn.closed.store(true, Ordering::Relaxed);
        }
        let mut by_type = self.by_type.lock().expect("registry poisoned");
        for ids in by_type.values_mut() {
            ids.retain(|&other| other != id);
        }
    }

    /// Indexes `id` under `job_type` for dispatch (idempotent).
    fn index(&self, job_type: &str, id: ConnId) {
        let mut by_type = self.by_type.lock().expect("registry poisoned");
        let ids = by_type.entry(job_type.to_string()).or_default();
        if !ids.contains(&id) {
            ids.push(id);
        }
    }

    /// Builds a round-robin–ordered dispatch plan: for each job type, the live
    /// subscriptions to attempt this tick, with the cursor advanced so a different
    /// stream leads next time. Snapshotted under the locks; all engine work then
    /// happens lock-free.
    fn dispatch_plan(&self) -> Vec<(String, Vec<DispatchTarget>)> {
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
            let mut targets = Vec::with_capacity(ids.len());
            for offset in 0..ids.len() {
                let id = ids[(start + offset) % ids.len()];
                let Some(conn) = conns.get(&id) else { continue };
                let sub = conn.subs.lock().expect("registry poisoned").get(job_type).cloned();
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

/// Builds the router carrying the `/command-stream` WebSocket endpoint, sharing
/// the engine-backed [`ServerImpl`] and the [`Registry`].
pub fn router(server: ServerImpl, registry: Arc<Registry>) -> Router {
    let state = CsState {
        server,
        registry,
        submission_window: submission_window_from_env(),
    };
    Router::new()
        .route("/command-stream", get(ws_handler))
        .with_state(state)
}

fn submission_window_from_env() -> i64 {
    std::env::var("NANOBPMN_STREAM_SUBMISSION_WINDOW")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_SUBMISSION_WINDOW)
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
        closed: AtomicBool::new(false),
    });
    registry.register(conn.clone());

    let (sink, stream) = socket.split();
    tokio::spawn(writer_task(sink, rx));

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
/// errors.
async fn writer_task(
    mut sink: futures_util::stream::SplitSink<WebSocket, Message>,
    mut rx: mpsc::Receiver<ServerFrame>,
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
    while let Some(message) = stream.next().await {
        let message = match message {
            Ok(message) => message,
            Err(_) => break,
        };
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
                timeout: timeout.unwrap_or(0),
                fetch_variable: fetch_variable.filter(|names| !names.is_empty()),
                credits: AtomicI64::new(job_credits.max(0)),
            });
            conn.subs
                .lock()
                .expect("registry poisoned")
                .insert(job_type.clone(), sub);
            registry.index(&job_type, conn.id);
            // A new subscription may have a backlog waiting: wake the dispatcher.
            server.jobs_available_handle().notify_waiters();
        }
        ClientFrame::JobCredits { job_type, n } => {
            if let Some(sub) = conn.subs.lock().expect("registry poisoned").get(&job_type) {
                sub.credits.fetch_add(n, Ordering::Relaxed);
            }
            server.jobs_available_handle().notify_waiters();
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
            conn.submission_outstanding.fetch_sub(1, Ordering::Relaxed);

            let vars = to_engine_vars(variables);
            match server
                .create_for_stream(process_definition_id, process_definition_key, vars)
                .await
            {
                Ok((instance_key, sync_completed)) => {
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
            let vars = to_engine_vars(variables);
            reply_job_command(conn, corr, server.complete_job_for_stream(key, vars).await);
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
            let outcome = server
                .fail_job_for_stream(key, retries.unwrap_or(0), error_message.unwrap_or_default())
                .await;
            reply_job_command(conn, corr, outcome);
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
            let outcome = server
                .throw_error_for_stream(key, error_code, error_message.unwrap_or_default())
                .await;
            reply_job_command(conn, corr, outcome);
        }
        ClientFrame::Heartbeat => {}
    }
}

/// Maps a job-command outcome to a `CommandResult` frame.
fn reply_job_command(conn: &Arc<Connection>, corr: u64, outcome: Result<(), (u16, String)>) {
    match outcome {
        Ok(()) => {
            conn.send(ServerFrame::CommandResult {
                corr,
                status: 200,
                body: None,
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
                body: Some(Value::String(format!("Job key '{raw}' is not a valid key."))),
            });
            None
        }
    }
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
    if n <= 0 || server.submission_pressure() {
        return;
    }
    conn.submission_outstanding.fetch_add(n, Ordering::Relaxed);
    conn.send(ServerFrame::SubmissionCredits { n });
}

// ----------------------------------------------------------------------------
// Dispatcher
// ----------------------------------------------------------------------------

/// Spawns the single server-wide dispatcher: it reacts to `jobs_available` (and a
/// periodic backstop sweep), leases jobs round-robin across subscribers, and tops
/// up submission credits as engine headroom allows.
pub fn spawn_dispatcher(server: ServerImpl, registry: Arc<Registry>) {
    let jobs_available = server.jobs_available_handle();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(DISPATCH_TICK_MS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_pressure = false;
        loop {
            tokio::select! {
                _ = jobs_available.notified() => {}
                _ = tick.tick() => {}
            }
            dispatch_jobs(&server, &registry).await;
            topup_submission_credits(&server, &registry);

            // Edge-triggered fleet pressure signal: broadcast once on each
            // transition so workers can coordinate without polling. O(streams)
            // only when the state actually flips.
            let pressure = server.submission_pressure();
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
        }
    });
}

/// Sends a frame to every live connection (best-effort).
fn broadcast(registry: &Arc<Registry>, frame: &ServerFrame) {
    for conn in registry.all_connections() {
        conn.send(frame.clone());
    }
}

/// One dispatch pass: for each job type, lease and push jobs to credited streams in
/// round-robin order until their credits, socket room, or the activatable pool runs
/// out.
async fn dispatch_jobs(server: &ServerImpl, registry: &Arc<Registry>) {
    for (job_type, targets) in registry.dispatch_plan() {
        for (conn, sub) in targets {
            if conn.closed.load(Ordering::Relaxed) {
                continue;
            }
            let credits = sub.credits.load(Ordering::Relaxed);
            if credits <= 0 {
                continue;
            }
            // Never lease more than we can immediately enqueue to this socket.
            let room = conn.tx.capacity() as i64;
            if room <= 0 {
                continue;
            }
            let want = credits.min(room).min(PER_STREAM_BATCH as i64) as usize;
            if want == 0 {
                continue;
            }
            let jobs = server
                .activate_for_stream(
                    &job_type,
                    &sub.worker,
                    want,
                    sub.timeout,
                    sub.fetch_variable.as_deref(),
                )
                .await;
            if jobs.is_empty() {
                // Pool drained for this type; stop spending effort on it.
                break;
            }
            let mut pushed = 0i64;
            for job in jobs {
                let value = serde_json::to_value(&job).unwrap_or(Value::Null);
                if conn.send(ServerFrame::Job { job: value }) {
                    pushed += 1;
                }
            }
            sub.credits.fetch_sub(pushed, Ordering::Relaxed);
        }
    }
}

/// Refills each connection's submission window when the engine has headroom, so a
/// client that stalled under pressure resumes intake once pressure clears.
fn topup_submission_credits(server: &ServerImpl, registry: &Arc<Registry>) {
    if server.submission_pressure() {
        return;
    }
    for conn in registry.all_connections() {
        if conn.closed.load(Ordering::Relaxed) {
            continue;
        }
        let outstanding = conn.submission_outstanding.load(Ordering::Relaxed);
        let grant = conn.submission_window - outstanding;
        if grant > 0 {
            conn.submission_outstanding
                .fetch_add(grant, Ordering::Relaxed);
            conn.send(ServerFrame::SubmissionCredits { n: grant });
        }
    }
}
