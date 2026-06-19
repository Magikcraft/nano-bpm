//! Stub server for the generated Orchestration Cluster REST layer.
//!
//! This binary wires the generated `nanobpm_gateway_rest` REST layer (models,
//! routes, and per-tag service traits) into a runnable `axum` server. No backend
//! services are connected yet: every operation is implemented as a stub that
//! responds with `501 Not Implemented`.
//!
//! The per-tag trait implementations live in the generated `stub_impls` module
//! (see scripts/gen-stub-server.py). This file owns only the stable pieces: the
//! `ServerImpl` type, authentication/error glue, and the server bootstrap.

mod backpressure;
mod cluster;
mod coldspill;
mod command_stream;
mod engine_actor;
mod journal;
mod memory;
mod metrics;
// Intra-cluster peer uplink (command-stream client to peers). The forwarding
// seam that drives it (create-forward, by-key forward, broadcast) lands in the
// following increments; the transport is integration-tested now.
#[allow(dead_code)]
mod peer;
mod partition;
mod query;
mod raft;
mod raft_logstore;
mod raft_net;
mod readstore;
mod stub_impls;
mod varspill;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::Multipart;
use axum::response::Response;
use nanobpm_gateway_rest::{apis, models, types};
use http::StatusCode;
use nanobpmn_engine_core::bpmn::parse_bpmn;
use nanobpmn_engine_core::{
    partition_of, ActivatedJob, Command, EngineError, Event, IncidentKind, IncidentState,
    ProcessBuilder, ProcessDefinition, ProcessInstanceState, Value, MAX_PARTITION_ID,
};

use crate::backpressure::{
    parse_backpressure_setting, AdaptiveController, Backpressure, BackpressureSetting,
};
use crate::engine_actor::EngineHandle;
use crate::partition::Partitions;
use crate::journal::{Commit, Journal, SharedWriter};
use crate::readstore::ReadStore;

/// Default long-poll window (ms) when a client passes `requestTimeout` 0.
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 5_000;

/// In a multi-node cluster, the maximum a REST `activateJobs` long poll sleeps
/// between peer re-polls. The `jobs_available` notify only fires for local job
/// arrivals, so this bounds the latency before a job that appeared on a peer's
/// partition is pulled and returned. Single-node never uses it (no peers).
const PEER_ACTIVATION_POLL_MS: u64 = 25;

/// Default window (ms) an `awaitCompletion` create request waits for the process
/// instance to finish when `requestTimeout` is absent or 0. On expiry the
/// request still returns 200 with `processCompleted=false` and the instance key.
const DEFAULT_AWAIT_COMPLETION_TIMEOUT_MS: u64 = 5_000;

/// The single type that implements every generated API trait.
///
/// It owns an embedded [`Engine`] (the `engine-core` crate), wrapped in a
/// durable [`Journal`], driven through a single-writer [`EngineHandle`] actor.
/// The engine is a single writer, so every mutating command is serialized onto
/// the actor's dedicated thread (see [`engine_actor`]); read-only
/// `search*`/`get*` projections are answered from the [`ReadStore`] and never
/// touch the engine thread, so they run concurrently across cores. Most
/// operations are still 501 stubs (see the generated `stub_impls` module); a
/// few — process-instance creation, job activation, and job completion — are
/// wired to the engine via the inherent methods below and routed from the stub
/// generator's override table. Every durable command is appended to the journal
/// so engine state survives a restart.
#[derive(Clone)]
pub struct ServerImpl {
    engine: Partitions,
    /// The read model. All `search*`/`get*` queries are answered from here
    /// (eventually consistent), never from hot engine state.
    store: Arc<ReadStore>,
    /// Notified whenever new jobs may have become activatable, so long-polling
    /// `activateJobs` requests can wake immediately instead of waiting out their
    /// full timeout.
    jobs_available: Arc<tokio::sync::Notify>,
    /// Permit-storing wake for the command-stream dispatcher. Unlike
    /// [`jobs_available`](Self::jobs_available) (a broadcast `notify_waiters`
    /// that drops signals arriving while the single dispatcher is mid-pass),
    /// this is signalled with `notify_one` so a job-available or credit-grant
    /// wake that lands during an engine-bound `dispatch_jobs` pass is retained
    /// and consumed on the next park — the dispatcher never sleeps to the
    /// backstop tick while there is pushable work. See [`signal_jobs_available`].
    dispatch_wake: Arc<tokio::sync::Notify>,
    /// Notified by the read-model exporter after every projected batch, so an
    /// `awaitCompletion` create request can wake the moment its instance reaches
    /// a terminal state (the exporter is the single point through which all
    /// completion/termination events flow).
    instances_changed: Arc<tokio::sync::Notify>,
    /// Backpressure controller for `createProcessInstance`. When its
    /// [`current_limit`](Backpressure::current_limit) is `Some(n)`, creates are
    /// rejected with `503 RESOURCE_EXHAUSTED` once the in-flight (Active,
    /// non-terminal) instance count reaches `n`. The default is **adaptive**
    /// (an AIMD limiter that sizes the watermark from the engine's measured
    /// per-command latency); a fixed watermark or fully-off are selectable via
    /// `NANOBPMN_BACKPRESSURE_MAX_INFLIGHT`. See [`crate::backpressure`].
    backpressure: Backpressure,
    /// Lock-free gauge of in-flight (Active) process instances, maintained by the
    /// read-model exporter (+1 per `ProcessInstanceCreated`, −1 per terminal
    /// event). This is the *active backlog*; it is kept for observability and
    /// hot-state eviction, but is **not** the backpressure signal — a no-drain
    /// burst would pin it even though the engine is healthy.
    inflight: Arc<AtomicUsize>,
    /// Lock-free gauge of create requests currently being *processed* by the
    /// engine (incremented when a create is submitted to the command thread,
    /// decremented the instant it is applied — typically tens of µs later). This
    /// is the backpressure signal: it measures request-processing concurrency
    /// (Zeebe/Camunda-style), so it sheds only when the engine thread genuinely
    /// can't keep up, never merely because an undrained backlog has accumulated.
    /// Memory under a large backlog is bounded by the variable-spill tier, not by
    /// this gate, so backpressure and memory safety are now independent rails.
    processing: Arc<AtomicUsize>,
    /// Monotonic counter bumped once per exported event batch — i.e. on every
    /// durable command's events flowing through the read-model exporter. It is a
    /// cheap "did anything happen" signal the idle-purge tick watches: when it
    /// stops advancing (and no creates are in flight) the server is quiescent and
    /// can compact hot state and return freed memory to the OS.
    activity: Arc<AtomicU64>,
    /// Active-instance backlog admission limit (0 = off, the default). When set,
    /// `createProcessInstance` is shed (503 `RESOURCE_EXHAUSTED`) once the active
    /// backlog (`inflight`) is at or above this value, bounding how many created-
    /// but-not-yet-terminal instances can accumulate. This is *admission control*
    /// on the backlog itself — complementary to the `processing`-concurrency
    /// `backpressure` gate: completion-priority keeps the system from collapsing
    /// under overload, and this gate keeps the resulting backlog (hence end-to-end
    /// latency and memory) bounded by fast-failing excess creates with a clean
    /// retry signal instead of queueing them for seconds. Durability and
    /// at-least-once are unaffected: a shed create is never journaled, and accepted
    /// instances' jobs retain their lease/replay guarantees.
    admission_max_backlog: usize,
    /// Create-queue-depth admission limit (0 = off, the default). When set,
    /// `createProcessInstance` is shed once the standing backlog of submitted-but-
    /// not-yet-applied creates (summed across partitions' `Low` queues) is at or
    /// above this value. With completion-priority, creates yield to completion, so
    /// under overload it is this create queue — not the active-instance backlog —
    /// that grows and inflates create latency; bounding it caps that latency with a
    /// clean retry signal. Durability/at-least-once are unaffected (a shed create
    /// is never journaled).
    admission_max_create_queue: usize,
    /// Command-stream uplinks to this node's cluster peers, built from the
    /// [`Topology`]. Empty for a single-node cluster (zero overhead). The
    /// forwarding seam consults it to reach a partition's owning node.
    // Read by the forwarding handlers landing in the following increments
    // (s1-broadcast / s1-bykey-forward); constructed and tested now.
    #[allow(dead_code)]
    peers: peer::PeerSet,
    /// The Raft groups this node hosts (one per partition it replicates), empty
    /// unless per-partition Raft is enabled. The command-stream handler dispatches
    /// inbound RPCs through it; the write path proposes through it. An empty
    /// registry means the classic single-writer path is in force — zero overhead.
    raft: Arc<crate::raft::RaftRegistry>,
    /// Engine actors for partitions this node **replicates but does not own**
    /// (followers under RF>1). The Raft state machine drives these so a follower
    /// can apply the replicated log; they are NOT part of the read-model / serving
    /// path (reads and job dispatch always go to the leader's owned actor). Empty
    /// unless per-partition Raft is enabled with RF>1 — zero overhead otherwise.
    raft_replicas: Arc<std::sync::Mutex<std::collections::HashMap<u64, EngineHandle>>>,
}

/// RAII counter for the request-processing concurrency gauge: bumps the gauge on
/// entry and restores it on drop, so every exit path (success, error, or a
/// dropped/cancelled request future) releases its slot exactly once.
struct ProcessingGuard<'a>(&'a AtomicUsize);

impl<'a> ProcessingGuard<'a> {
    fn enter(gauge: &'a AtomicUsize) -> Self {
        gauge.fetch_add(1, Ordering::Relaxed);
        ProcessingGuard(gauge)
    }
}

impl Drop for ProcessingGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl ServerImpl {
    /// Builds a server over `journals` (one single-writer engine actor per
    /// partition; `journals[i]` owns partition id `i`) and the shared read
    /// `store`. Seeds the demo process on the deployment partition only when its
    /// journal is fresh, then replicates every deployed definition to the other
    /// partitions so any of them can instantiate it. Each journal's exporter must
    /// already be wired to the shared read store so the seed deployment is
    /// projected.
    pub fn new(mut journals: Vec<Journal>, store: Arc<ReadStore>, topology: cluster::Topology) -> Self {
        assert!(!journals.is_empty(), "at least one partition is required");
        // Teach every owned partition the cluster-wide partition count so the
        // engine places message subscriptions on the partition owning their
        // correlation key (`hash(correlation_key)`). With a single partition this
        // is `1`, so placement stays local and behaviour is unchanged.
        for journal in journals.iter_mut() {
            journal.set_num_partitions(topology.num_partitions);
        }
        // The deployment partition (id 0) is the only one that seeds the demo
        // process and owns the message-/timer-start subscriptions. In a clustered
        // deployment only the node that owns partition 0 holds it (and it is its
        // smallest owned id, hence `journals[0]`); other nodes skip seeding and
        // receive deployed definitions via cross-node replication (stage 1
        // broadcast). Single-node always owns partition 0, so this is unchanged.
        let owns_deploy_partition = topology.is_local(0);
        if owns_deploy_partition && journals[0].is_fresh() {
            // Pre-deploy a demo process so `createProcessInstance` (by id "demo")
            // has something to start. A real build would deploy from BPMN XML.
            let demo = ProcessBuilder::new("demo")
                .start_event("start")
                .service_task("work", "demo-work")
                .end_event("end")
                .connect("start", "work")
                .connect("work", "end")
                .build()
                .expect("valid demo process");
            // Seed durability is non-critical: a fresh journal re-seeds on every
            // start, so we don't await the commit here.
            let _ = journals[0]
                .apply_command(Command::DeployProcess(demo))
                .expect("deploy demo process");
        }
        // Replicate the deployment partition's definitions to every other
        // partition (in-memory, not journaled — re-derived here on each restart
        // from partition 0's durable log). The deployment partition keeps the
        // sole copy of each message-start / timer-start subscription. Only the
        // node owning partition 0 can do this locally; cross-node replication to
        // peers is stage-1 broadcast.
        if owns_deploy_partition && journals.len() > 1 {
            let replication = deployment_replication_events(&journals[0]);
            if !replication.is_empty() {
                for journal in journals.iter_mut().skip(1) {
                    journal.install_deployment(&replication);
                }
            }
        }
        let inflight_seed = store.active_instance_count();
        let inflight = Arc::new(AtomicUsize::new(inflight_seed));
        // Request-processing concurrency starts at zero: nothing is mid-apply at
        // boot, regardless of how large the replayed backlog is.
        let processing = Arc::new(AtomicUsize::new(0));

        // Resolve the backpressure mode and, for adaptive mode, build the
        // latency controller that the engine thread will drive. The controller
        // owns the shared limit atomic; the server keeps the read side. The
        // controller's "is the limit being used" signal reads the processing
        // gauge (the gated quantity), not the backlog.
        let (backpressure, mut controller) = match backpressure_setting_from_env() {
            BackpressureSetting::Disabled => (Backpressure::Disabled, None),
            BackpressureSetting::Fixed(n) => (Backpressure::Fixed(n), None),
            BackpressureSetting::Adaptive => {
                let (ctrl, limit) = AdaptiveController::new(processing.clone());
                (Backpressure::Adaptive(limit), Some(ctrl))
            }
        };
        tracing::info!("backpressure: {}", backpressure.describe());

        let admission_max_backlog = admission_max_backlog_from_env();
        if admission_max_backlog > 0 {
            tracing::info!(
                "admission control: on, max active backlog {admission_max_backlog} instance(s)"
            );
        }
        let admission_max_create_queue = admission_max_create_queue_from_env();
        if admission_max_create_queue > 0 {
            tracing::info!(
                "admission control: on, max create-queue depth {admission_max_create_queue}"
            );
        }

        // Optional spill tiers, sharing one disk-backed store (one file, one WAL,
        // one durability story). Variable spill sheds the variables of a large
        // *active* (job-parked) backlog; cold spill sheds whole *dormant*
        // instances of a large *parked* backlog. Both off unless configured.
        // Keys are globally unique across partitions, so a single store serves
        // every partition without collision.
        let var_cfg = spill_from_env();
        let cold_cfg = cold_spill_from_env();
        if var_cfg.is_some() || cold_cfg.is_some() {
            let path = var_cfg
                .as_ref()
                .and_then(|(p, _)| p.clone())
                .or_else(|| resolve_data_paths().1.map(|db| db.with_file_name("var-spill.sqlite")));
            let location = path
                .as_deref()
                .map(|p| format!(", store {}", p.display()))
                .unwrap_or_else(|| " (in-memory)".to_string());
            match varspill::VarSpillStore::open(path.as_deref()) {
                Ok(store) => {
                    let store = Arc::new(store);
                    if let Some((_, budget)) = var_cfg {
                        for journal in journals.iter_mut() {
                            journal.set_spill(Arc::clone(&store), budget);
                        }
                        tracing::info!(
                            "variable spill: on, hot budget {budget} instance(s){location}"
                        );
                    }
                    if let Some((high, low)) = cold_cfg {
                        for journal in journals.iter_mut() {
                            journal.set_cold_spill(Arc::clone(&store), high, low);
                        }
                        tracing::info!(
                            "cold spill: on, high-water {:.0} MiB / low-water {:.0} MiB{location}",
                            high as f64 / (1024.0 * 1024.0),
                            low as f64 / (1024.0 * 1024.0),
                        );
                    }
                }
                Err(e) => tracing::error!("spill disabled: failed to open store: {e}"),
            }
        }

        // Spawn one engine actor per OWNED partition. The adaptive backpressure
        // controller (when present) is driven by the first owned partition's
        // command latency — a representative single sample of engine load that
        // sizes the create-admission watermark applied across all partitions.
        let owned_count = journals.len();
        let handles: Vec<EngineHandle> = journals
            .into_iter()
            .enumerate()
            .map(|(i, journal)| {
                let ctrl = if i == 0 { controller.take() } else { None };
                EngineHandle::spawn(journal, ctrl)
            })
            .collect();
        // Build peer uplinks before `topology` is consumed by the engine. A
        // single-node topology yields an empty set (never dialed).
        let peers = peer::PeerSet::new(topology.clone());
        let engine = if topology.is_single_node() {
            if owned_count > 1 {
                tracing::info!("partitions: {owned_count} (keys embed partition id)");
            }
            Partitions::new(handles)
        } else {
            tracing::info!(
                "cluster: node {}/{}, owns {} of {} partition(s) {:?}",
                topology.node_id,
                topology.num_nodes(),
                owned_count,
                topology.num_partitions,
                topology.local_partitions(),
            );
            Partitions::with_topology(topology, handles)
        };

        Self {
            engine,
            store,
            jobs_available: Arc::new(tokio::sync::Notify::new()),
            dispatch_wake: Arc::new(tokio::sync::Notify::new()),
            instances_changed: Arc::new(tokio::sync::Notify::new()),
            backpressure,
            // Seed the gauge from the read model so a journal-replay restart
            // accounts for instances still in flight; a fresh/in-memory store
            // reports zero. The exporter maintains it from here on.
            inflight,
            processing,
            activity: Arc::new(AtomicU64::new(0)),
            admission_max_backlog,
            admission_max_create_queue,
            peers,
            raft: crate::raft::RaftRegistry::new(),
            raft_replicas: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }
}

/// Synthesises the `ProcessDeployed` events needed to replicate the deployment
/// partition's process definitions to the other partitions. Reads the deployment
/// partition's engine state (its definitions survive its own journal replay) and
/// re-emits one `ProcessDeployed` per definition under the same shared
/// `processDefinitionKey`. `install_deployment` applies only these events (never
/// arming a second copy of a start subscription/timer).
fn deployment_replication_events(deploy_journal: &Journal) -> Vec<Event> {
    deploy_journal
        .engine()
        .state()
        .processes
        .values()
        .map(|deployed| Event::ProcessDeployed {
            // The deployment key is not used by the applier (definitions are
            // keyed by processDefinitionKey); 0 is a harmless placeholder.
            deployment_key: 0,
            process_definition_key: deployed.key,
            version: deployed.version,
            process: deployed.definition.clone(),
        })
        .collect()
}

/// Parses every deployment resource up front so a deploy is all-or-nothing,
/// returning the parsed process definitions and a map from process id to its
/// originating resource name. `Err` is `(title, detail)` for a 400 response.
fn parse_deploy_resources(
    resources: &[(String, String)],
) -> Result<(Vec<ProcessDefinition>, std::collections::HashMap<String, String>), (&'static str, String)>
{
    let mut processes = Vec::new();
    let mut resource_names: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for (resource_name, xml) in resources {
        match parse_bpmn(xml) {
            Ok(defs) => {
                for def in defs {
                    resource_names.insert(def.id.clone(), resource_name.clone());
                    processes.push(def);
                }
            }
            Err(e) => {
                return Err(("Invalid BPMN", format!("Failed to parse '{resource_name}': {e}.")));
            }
        }
    }
    Ok((processes, resource_names))
}

impl Default for ServerImpl {
    fn default() -> Self {
        let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
        build_server(vec![Journal::in_memory()], store, cluster::Topology::single(1))
    }
}

impl ServerImpl {
    /// The Raft groups this node hosts. Used to host a partition's group and, by
    /// the command-stream handler and write path, to reach it.
    pub fn raft_registry(&self) -> &Arc<crate::raft::RaftRegistry> {
        &self.raft
    }

    /// A [`RaftTransport`](crate::raft_net::RaftTransport) that carries this
    /// node's Raft RPCs to peers over the command stream. Built from the node's
    /// existing peer uplinks, so a target Raft node id maps straight onto a peer.
    pub fn raft_transport(&self) -> Arc<dyn crate::raft_net::RaftTransport> {
        Arc::new(crate::raft_net::PeerTransport::new(self.peers.clone()))
    }

    /// Peer-side of the Raft network: decode an inbound RPC, feed it to the local
    /// replica of `partition`, and return its serialized response. `Err((status,
    /// message))` maps to the command-stream `CommandResult` status (400 malformed,
    /// 404 not hosted here, 500 dispatch failure).
    pub async fn dispatch_raft_rpc(
        &self,
        partition: u64,
        rpc: serde_json::Value,
    ) -> Result<serde_json::Value, (u16, String)> {
        let req: crate::raft_net::RaftRpcRequest =
            serde_json::from_value(rpc).map_err(|e| (400u16, format!("malformed raft rpc: {e}")))?;
        let part = self.raft.get(partition).ok_or_else(|| {
            (
                404u16,
                format!("no raft group for partition {partition} on this node"),
            )
        })?;
        let resp = crate::raft_net::dispatch(&part.raft, req)
            .await
            .map_err(|e| (500u16, format!("raft dispatch failed: {e}")))?;
        serde_json::to_value(resp).map_err(|e| (500u16, format!("encode raft response: {e}")))
    }
}

/// Wires one shared read-model exporter across every partition's `journals`,
/// builds the [`ServerImpl`] (which seeds the demo process on partition 0 when
/// its journal is fresh — that deployment is then forwarded to the exporter),
/// and spawns the background exporter thread. Every journal sends into the same
/// channel, so the single read model aggregates all partitions (keys are
/// globally unique). The exporter must be set before `ServerImpl::new` so the
/// seed deployment is projected; the thread is spawned after so it can route
/// hot-state eviction back to the owning partition.
fn build_server(
    mut journals: Vec<Journal>,
    store: Arc<ReadStore>,
    topology: cluster::Topology,
) -> ServerImpl {
    let (tx, rx) = mpsc::channel::<Arc<Vec<Event>>>();
    for journal in journals.iter_mut() {
        journal.set_exporter(tx.clone());
    }
    drop(tx);
    let server = ServerImpl::new(journals, store.clone(), topology);
    spawn_exporter(
        rx,
        store,
        server.engine.clone(),
        server.instances_changed.clone(),
        server.inflight.clone(),
        server.activity.clone(),
    );
    server
}

/// Spawns the read-model exporter thread. It drains the channel (batching every
/// queued command's events from every partition), projects the batch into the
/// shared read store, then evicts any now-completed instances from hot engine
/// state — routed back to each instance's owning partition by its key. The
/// thread exits when the channel closes (all `ServerImpl` clones and every
/// journal are dropped). Events arrive as `Arc<Vec<Event>>` shared with the
/// command thread, so projecting them costs no deep copy of the 50 KB payloads.
fn spawn_exporter(
    rx: mpsc::Receiver<Arc<Vec<Event>>>,
    store: Arc<ReadStore>,
    engine: Partitions,
    instances_changed: Arc<tokio::sync::Notify>,
    inflight: Arc<AtomicUsize>,
    activity: Arc<AtomicU64>,
) {
    std::thread::Builder::new()
        .name("nanobpmn-exporter".into())
        .spawn(move || {
            while let Ok(first) = rx.recv() {
                let mut batch = vec![first];
                while let Ok(next) = rx.try_recv() {
                    batch.push(next);
                }
                // Signal liveness to the idle-purge tick: a batch means at least
                // one durable command was applied since the last check.
                activity.fetch_add(1, Ordering::Relaxed);
                // Borrow every command's events as a flat slice of references —
                // the payloads stay in their original `Arc`s, never copied here.
                let refs: Vec<&Event> = batch.iter().flat_map(|events| events.iter()).collect();
                let created = refs
                    .iter()
                    .filter(|e| matches!(e, Event::ProcessInstanceCreated { .. }))
                    .count();
                let completed = match store.export(&refs) {
                    Ok(keys) => keys,
                    Err(e) => {
                        tracing::error!("read-model export failed: {e}");
                        continue;
                    }
                };
                // Update the in-flight backpressure gauge: +created, −terminal.
                // Single-writer (this thread), so a plain load/store is race-free
                // for the value and saturates at zero defensively.
                let delta = created as isize - completed.len() as isize;
                if delta != 0 {
                    let next = (inflight.load(Ordering::Relaxed) as isize + delta).max(0) as usize;
                    inflight.store(next, Ordering::Relaxed);
                }
                // The read store now reflects this batch; wake any
                // `awaitCompletion` requests so they can observe a terminal
                // state. notify_waiters() is a no-op when nobody is waiting.
                instances_changed.notify_waiters();
                if !completed.is_empty() {
                    // Reclaim hot state for the whole batch; no per-batch
                    // shrink_to_fit (reallocating every map needlessly throttles
                    // command throughput — capacity is reused by new instances).
                    // Each instance lives on the partition that minted its key, so
                    // route the eviction there. Fire-and-forget: the exporter has
                    // no reply to wait for.
                    if engine.is_single() {
                        engine.all()[0].spawn_job(move |journal| {
                            journal.evict_instances(&completed);
                        });
                    } else {
                        let mut by_partition: std::collections::HashMap<usize, Vec<u64>> =
                            std::collections::HashMap::new();
                        for key in completed {
                            by_partition
                                .entry(nanobpmn_engine_core::partition_of(key) as usize)
                                .or_default()
                                .push(key);
                        }
                        for (idx, keys) in by_partition {
                            // Route by GLOBAL partition id: `all()` is the compacted
                            // slice of owned handles, so it cannot be indexed by id.
                            // A completed instance is always owned locally (it came
                            // from this node's read-store export), so this resolves.
                            if let Some(handle) = engine.local_for_partition(idx as u64) {
                                handle.spawn_job(move |journal| {
                                    journal.evict_instances(&keys);
                                });
                            }
                        }
                    }
                }
            }
        })
        .expect("spawn read-model exporter thread");
}

impl AsRef<ServerImpl> for ServerImpl {
    fn as_ref(&self) -> &ServerImpl {
        self
    }
}

fn problem(title: &str, status: u16, detail: String) -> models::ProblemDetail {
    models::ProblemDetail::new(title.to_string(), status, detail, String::new())
}

/// Parses `host` and `port` from a peer base URL like `http://10.0.0.1:8080` (or
/// bare `10.0.0.1:8080`). Returns `None` when no host/port can be extracted (e.g.
/// the empty self-address of a single-node topology), so the caller can fall back.
fn parse_host_port(url: &str) -> Option<(String, i32)> {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url);
    let authority = rest.split('/').next().unwrap_or(rest);
    let (host, port) = authority.rsplit_once(':')?;
    if host.is_empty() {
        return None;
    }
    let port: i32 = port.parse().ok()?;
    Some((host.to_string(), port))
}

/// Resolves the backpressure mode from `NANOBPMN_BACKPRESSURE_MAX_INFLIGHT`.
/// See [`parse_backpressure_setting`] for the grammar; backpressure is on (and
/// adaptive) by default.
fn backpressure_setting_from_env() -> BackpressureSetting {
    parse_backpressure_setting(std::env::var("NANOBPMN_BACKPRESSURE_MAX_INFLIGHT").ok().as_deref())
}

/// Resolves the active-instance-backlog admission limit, or `0` (off) by default.
///
/// `NANOBPMN_ADMISSION_MAX_BACKLOG=<n>` caps the number of active (created-but-not-
/// terminal) instances: once the backlog reaches `n`, `createProcessInstance` is
/// shed with a 503 `RESOURCE_EXHAUSTED` so clients back off, keeping end-to-end
/// latency and memory bounded under sustained overload. Unset or `0` disables it
/// (the default) — appropriate for workloads with a legitimately large parked
/// population (e.g. many instances waiting on timers/messages), where the backlog
/// is not a load signal. Distinct from `NANOBPMN_BACKPRESSURE_MAX_INFLIGHT`, which
/// gates on create-processing *concurrency*, not the standing backlog.
fn admission_max_backlog_from_env() -> usize {
    std::env::var("NANOBPMN_ADMISSION_MAX_BACKLOG")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

/// Resolves the create-queue-depth admission limit, or `0` (off) by default.
///
/// `NANOBPMN_ADMISSION_MAX_CREATE_QUEUE=<n>` caps the standing backlog of
/// submitted-but-not-yet-applied creates across all partitions; once it is reached,
/// `createProcessInstance` is shed with a 503 `RESOURCE_EXHAUSTED`. Because
/// completion-priority makes creates yield to completion, this queue is what grows
/// under overload, so bounding it bounds create-side latency. Unset or `0`
/// disables it (the default).
fn admission_max_create_queue_from_env() -> usize {
    std::env::var("NANOBPMN_ADMISSION_MAX_CREATE_QUEUE")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

/// Resolves the idle quiescence delay before the idle-purge tick compacts hot
/// state and returns freed memory to the OS, or `None` to disable it.
///
/// - `NANOBPMN_IDLE_PURGE_MS` unset: default 5000 ms.
/// - `NANOBPMN_IDLE_PURGE_MS=0`: disabled (never purge on idle).
/// - `NANOBPMN_IDLE_PURGE_MS=<n>`: wait `n` ms of quiescence before purging.
fn idle_purge_quiescence_from_env() -> Option<Duration> {
    match std::env::var("NANOBPMN_IDLE_PURGE_MS") {
        Ok(v) => match v.trim().parse::<u64>() {
            Ok(0) => None,
            Ok(ms) => Some(Duration::from_millis(ms)),
            Err(_) => Some(Duration::from_millis(5000)),
        },
        Err(_) => Some(Duration::from_millis(5000)),
    }
}

/// Resolves the variable-spill configuration from the environment, or `None` to
/// keep every payload resident.
///
/// Spill is **on by default in persistent mode** (when a data dir / journal is
/// configured, so the spill store has somewhere durable to live and the run is
/// memory-bound under a large active backlog). It stays off for in-memory runs
/// (no data dir): the spill store would fall back to an in-memory SQLite db,
/// doubling the payload footprint with no RAM saving.
///
/// - `NANOBPMN_VAR_SPILL` unset: on iff a persistent data path exists.
/// - `NANOBPMN_VAR_SPILL=0`/`off`/`false`/`none`/`disabled`/`no`: forced off.
/// - `NANOBPMN_VAR_SPILL=1`/`on`/`true`/`yes`: forced on (even in-memory — for tests).
/// - `NANOBPMN_VAR_SPILL_BUDGET=<n>`: hot budget (max resident spillable instances
///   before the oldest backlog is shed); default 512.
/// - The store is co-located with the read-model db (`<dir>/var-spill.sqlite`)
///   when persistent, else in-memory.
fn spill_from_env() -> Option<(Option<PathBuf>, usize)> {
    let (_, db) = resolve_data_paths();
    let enabled = match std::env::var("NANOBPMN_VAR_SPILL").ok().as_deref() {
        Some("0") | Some("off") | Some("false") | Some("none") | Some("disabled") | Some("no") => {
            false
        }
        Some("1") | Some("on") | Some("true") | Some("yes") => true,
        // Unset / unrecognised: default on only when a persistent path exists.
        _ => db.is_some(),
    };
    if !enabled {
        return None;
    }
    let budget = std::env::var("NANOBPMN_VAR_SPILL_BUDGET")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(512);
    let path = db.map(|db| db.with_file_name("var-spill.sqlite"));
    Some((path, budget))
}

/// Resolves the cold-spill configuration: the resident-byte high-/low-water marks
/// at which whole *dormant* instances are evicted to / kept on disk. Returns
/// `None` to keep every instance resident.
///
/// Cold spill is the long-lived counterpart to variable spill: where variable
/// spill sheds the variables of a job-parked instance, cold spill sheds the
/// *entire* dormant instance (control state, jobs, timers, subscriptions),
/// keeping only a slim routing index resident and rehydrating on demand. It is
/// **on by default in persistent mode** (it needs a durable store to relieve
/// RAM) but only ever fires once resident memory crosses the high-water mark, so
/// a small workload never pays for it.
///
/// - `NANOBPMN_COLD_SPILL` unset: on iff a persistent data path exists.
/// - `NANOBPMN_COLD_SPILL=0`/`off`/`false`/`none`/`disabled`/`no`: forced off.
/// - `NANOBPMN_COLD_SPILL=1`/`on`/`true`/`yes`: forced on (even in-memory — tests).
/// - `NANOBPMN_COLD_SPILL_MB=<n>`: high-water in MiB (default 384). The sweep
///   stops once resident memory falls under `low = high * 7/8`.
fn cold_spill_from_env() -> Option<(u64, u64)> {
    let (_, db) = resolve_data_paths();
    let enabled = match std::env::var("NANOBPMN_COLD_SPILL").ok().as_deref() {
        Some("0") | Some("off") | Some("false") | Some("none") | Some("disabled") | Some("no") => {
            false
        }
        Some("1") | Some("on") | Some("true") | Some("yes") => true,
        _ => db.is_some(),
    };
    if !enabled {
        return None;
    }
    let high_mb = std::env::var("NANOBPMN_COLD_SPILL_MB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(384);
    let high = high_mb * 1024 * 1024;
    let low = high / 8 * 7;
    Some((high, low))
}

/// Engine-backed implementations of selected operations. The stub generator
/// (scripts/gen-stub-server.py) emits trait methods that delegate here.
impl ServerImpl {
    async fn create_process_instance_impl(
        &self,
        body: &models::ProcessInstanceCreationInstruction,
    ) -> Result<apis::process_instance::CreateProcessInstanceResponse, ()> {
        use apis::process_instance::CreateProcessInstanceResponse as Resp;

        // Backpressure: reject new work once the engine's request-processing
        // concurrency is at or above the current limit. The gated quantity is the
        // number of creates being *applied right now* (the `processing` gauge),
        // not the active backlog — so a no-drain burst is absorbed (it converges
        // to client concurrency, like Zeebe/Camunda) and we shed only when the
        // single engine thread genuinely can't keep up. Memory under a large
        // backlog is bounded independently by the variable-spill tier. The 503
        // carries a `RESOURCE_EXHAUSTED` title that the client SDK reads as a
        // backpressure signal and answers with a retry backoff. The limit is
        // either a fixed watermark or an adaptive AIMD value sized from measured
        // latency; reading it (and the gauge) is a relaxed atomic load, so this
        // check costs no engine round-trip — a small race against concurrent
        // creates is irrelevant for an approximate limit.
        if let Some(limit) = self.backpressure.current_limit() {
            let processing = self.processing.load(Ordering::Relaxed);
            if self.backpressure.should_shed(processing) {
                return Ok(Resp::Status503_TheServiceIsCurrentlyUnavailable(problem(
                    "RESOURCE_EXHAUSTED",
                    503,
                    format!(
                        "Backpressure: {processing} creates in flight at or above the \
                         configured limit of {limit}. Retry after a backoff."
                    ),
                )));
            }
        }

        // Active-backlog admission control: independently of the concurrency gate
        // above, shed once the standing backlog of active instances reaches the
        // configured limit, so end-to-end latency and memory stay bounded under
        // sustained overload instead of the create queue growing unboundedly. Off
        // by default; a shed create is never journaled, so durability is untouched.
        if let Some(message) = self.admission_shed() {
            return Ok(Resp::Status503_TheServiceIsCurrentlyUnavailable(problem(
                "RESOURCE_EXHAUSTED",
                503,
                message,
            )));
        }

        // Pull the await-completion controls (shared by both creation variants).
        // When `awaitCompletion` is set, the request blocks until the instance
        // reaches a terminal state or `requestTimeout` elapses.
        let (await_completion, fetch_variables, request_timeout) = match body {
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionById(b) => {
                (b.await_completion, b.fetch_variables.as_ref(), b.request_timeout)
            }
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionByKey(b) => {
                (b.await_completion, b.fetch_variables.as_ref(), b.request_timeout)
            }
        };
        let await_completion = await_completion.unwrap_or(false);

        // Decode the request variables off the engine thread so the 50 KB JSON →
        // engine `Value` conversion runs in parallel rather than serially on the
        // single command thread. The variables come from the request body and so
        // are available for both creation variants without touching the engine.
        let variables = match body {
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionById(b) => {
                b.variables.as_ref()
            }
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionByKey(b) => {
                b.variables.as_ref()
            }
        }
        .map(from_object_map)
        .unwrap_or_default();

        // Extract tags and business_id from the request body (both variants have
        // these fields). Convert Option<Vec<Tag>> to Vec<String> for the engine.
        let (tags, business_id) = match body {
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionById(b) => {
                (b.tags.clone(), b.business_id.clone())
            }
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionByKey(b) => {
                (b.tags.clone(), b.business_id.clone())
            }
        };
        let tags_vec: Vec<String> = tags
            .unwrap_or_default()
            .into_iter()
            .map(|t| t.0)
            .collect();
        let business_id_str = business_id;

        // Only the by-key variant needs the engine (to resolve a deployed key to a
        // process id); capture the lookup inputs the engine thread will need.
        let (by_id, by_key) = match body {
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionById(b) => {
                (Some(b.process_definition_id.clone()), None)
            }
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionByKey(b) => {
                (None, Some(b.process_definition_key.0.clone()))
            }
        };

        // Run the command on the engine thread. The closure yields the success
        // fields plus the commit to await, or a ready `Resp` for an error path
        // (nothing written). Both arms produce `Send` values.
        //
        // The processing guard is held only across the engine round-trip (submit
        // → applied), not the later `awaitCompletion` wait, so the backpressure
        // gauge measures command-processing concurrency rather than how long a
        // client chooses to block for completion.
        type CreateOk = (String, i32, String, u64, bool, Vec<Event>, Commit);

        // Cluster-wide create placement (stage 1): round-robin across EVERY
        // partition in the cluster so a single gateway drives the whole cluster.
        // When the chosen partition is owned by a peer, forward the create there
        // and map its answer back; otherwise (always, on a single node) fall
        // through to the in-process create below. Placement is decided after the
        // node-local backpressure/admission gates above, which remain the
        // admission point for the whole request.
        if let Some(node) = self.engine.next_create_placement() {
            let wire_vars = match body {
                models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionById(b) => {
                    b.variables.as_ref()
                }
                models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionByKey(b) => {
                    b.variables.as_ref()
                }
            }
            .and_then(|m| wire_variables(Some(m)));
            return Ok(self
                .forward_create(
                    node,
                    by_id,
                    by_key,
                    wire_vars,
                    tags_vec,
                    business_id_str,
                    await_completion,
                    fetch_variables.cloned(),
                    request_timeout,
                )
                .await);
        }

        // Clone tags and business_id for use in the response after the engine
        // command completes (the closure moves them).
        let tags_for_response = tags_vec.clone();
        let business_id_for_response = business_id_str.clone();
        let outcome: Result<CreateOk, Box<Resp>> = {
            let _processing = ProcessingGuard::enter(&self.processing);
            self
            .engine
            .for_create()
            .with_low(move |engine| {
                // The engine starts processes by BPMN process id. A creation-by-key
                // request is resolved to its process id by looking up the deployed
                // definition whose key matches; an unknown key is rejected as
                // invalid input (the create endpoint has no 404 variant).
                let process_id = match (by_id, by_key) {
                    (Some(id), _) => id,
                    (None, Some(requested)) => {
                        match engine
                            .state()
                            .processes
                            .values()
                            .find(|d| d.key.to_string() == requested)
                        {
                            Some(d) => d.definition.id.clone(),
                            None => {
                                return Err(Box::new(Resp::Status400_TheProvidedDataIsNotValid(
                                    problem(
                                        "Process not found",
                                        400,
                                        format!("No deployed process with key '{requested}'."),
                                    ),
                                )));
                            }
                        }
                    }
                    (None, None) => unreachable!("one creation variant is always set"),
                };

                match engine.apply_command_at(
                    Command::create_instance_full(process_id.clone(), variables, tags_vec, business_id_str),
                    now_millis(),
                ) {
                    Ok((events, commit)) => {
                        let instance_key = events
                            .iter()
                            .find_map(Event::instance_key)
                            .expect("created instance has a key");
                        // Project the real deployed key and version now that the
                        // instance exists, so by-id and by-key requests report the
                        // same definition identity.
                        let (definition_key, version) = engine
                            .state()
                            .processes
                            .get(&process_id)
                            .map(|d| (d.key.to_string(), d.version))
                            .unwrap_or_else(|| (process_id.clone(), 1));
                        // An auto-completing process (no wait states) finishes
                        // synchronously within this create command; a process that
                        // parks on a job/timer/etc. is still running.
                        let sync_completed = engine.engine().is_completed(instance_key);
                        // Collect any cross-partition subscription follow-ups to
                        // route once durable (none single-partition).
                        let routable: Vec<Event> = if engine.engine().num_partitions() > 1 {
                            events
                                .iter()
                                .filter(|e| {
                                    matches!(
                                        e,
                                        Event::MessageSubscriptionOpening { .. }
                                            | Event::RemoteMessageCorrelation { .. }
                                            | Event::MessageSubscriptionClosing { .. }
                                            | Event::StartInstanceDispatched { .. }
                                    )
                                })
                                .cloned()
                                .collect()
                        } else {
                            Vec::new()
                        };
                        Ok((
                            process_id,
                            version,
                            definition_key,
                            instance_key,
                            sync_completed,
                            routable,
                            commit,
                        ))
                    }
                    Err(EngineError::ProcessNotFound { process_id }) => {
                        Err(Box::new(Resp::Status400_TheProvidedDataIsNotValid(problem(
                            "Process not found",
                            400,
                            format!("No deployed process with id '{process_id}'."),
                        ))))
                    }
                    Err(e) => Err(Box::new(
                        Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                            "Internal error",
                            500,
                            e.to_string(),
                        )),
                    )),
                }
            })
            .await
        };

        let (process_id, version, definition_key, instance_key, sync_completed, routable, commit) =
            match outcome {
                Ok(fields) => fields,
                Err(resp) => return Ok(*resp),
            };

        // Record REST create
        crate::metrics::record_create("rest");

        // Block on durability before acknowledging: a returned 200 means the
        // create is fsynced.
        commit.wait().await;
        // The instance may have parked on an off-partition message catch: route
        // the subscription open to its canonical partition (no-op single-node).
        if !routable.is_empty() {
            self.drive_subscription_routing(routable).await;
        }
        // Starting an instance parks it on its first service task, so new jobs
        // may now be activatable: wake any long-pollers.
        self.signal_jobs_available();

        // Resolve the final response. Without awaitCompletion we report whatever
        // completion state held synchronously and return no variables. With it,
        // we wait (off the engine lock) for the read model to show a terminal
        // state, then return the root-scope variables; on timeout we still return
        // 200 with the instance key and processCompleted=false (a deliberate
        // deviation from Camunda's 504, so callers can poll by key).
        let (variables_out, process_completed) = if await_completion {
            self.await_process_completion(instance_key, fetch_variables, request_timeout)
                .await
        } else {
            (std::collections::HashMap::new(), sync_completed)
        };

        let result = models::CreateProcessInstanceResult::new(
            process_id,
            version,
            "<default>".to_string(),
            variables_out,
            models::ProcessDefinitionKey(definition_key),
            models::ProcessInstanceKey(instance_key.to_string()),
            tags_for_response.into_iter().map(models::Tag).collect(),
            business_id_for_response.map(nanobpm_gateway_rest::types::Nullable::Present)
                .unwrap_or(nanobpm_gateway_rest::types::Nullable::Null),
            process_completed,
        );
        Ok(Resp::Status200_TheProcessInstanceWasCreated(result))
    }

    /// Waits for a created instance to reach a terminal state, returning its
    /// root-scope variables (filtered by `fetch_variables` when non-empty) and
    /// whether it completed. Never holds the engine lock: it observes the read
    /// model and is woken by the exporter via `instances_changed`. On timeout it
    /// returns `(empty, false)` — the caller still gets a 200 with the key.
    async fn await_process_completion(
        &self,
        instance_key: nanobpmn_engine_core::Key,
        fetch_variables: Option<&Vec<String>>,
        request_timeout: Option<i64>,
    ) -> (std::collections::HashMap<String, types::Object>, bool) {
        // requestTimeout is in ms; 0 / absent / negative => server default.
        let timeout_ms = match request_timeout {
            Some(ms) if ms > 0 => ms as u64,
            _ => DEFAULT_AWAIT_COMPLETION_TIMEOUT_MS,
        };

        let wait = async {
            loop {
                // Register the wake intent *before* reading, so a completion that
                // lands between the read and the await is not lost.
                let notified = self.instances_changed.notified();
                if let Some(row) = self.store.process_instance(instance_key) {
                    match row.state {
                        ProcessInstanceState::Completed => return true,
                        // Terminated/canceled instances never complete; stop
                        // waiting and report not-completed.
                        ProcessInstanceState::Terminated => return false,
                        ProcessInstanceState::Active => {}
                    }
                }
                notified.await;
            }
        };

        let completed = matches!(
            tokio::time::timeout(Duration::from_millis(timeout_ms), wait).await,
            Ok(true)
        );

        let variables = if completed {
            self.root_scope_variables(instance_key, fetch_variables)
        } else {
            std::collections::HashMap::new()
        };
        (variables, completed)
    }

    /// Builds the root-scope variable map for an instance from the read model,
    /// optionally restricted to the names in `fetch_variables` (when non-empty).
    fn root_scope_variables(
        &self,
        instance_key: nanobpmn_engine_core::Key,
        fetch_variables: Option<&Vec<String>>,
    ) -> std::collections::HashMap<String, types::Object> {
        let wanted: Option<std::collections::HashSet<&str>> = match fetch_variables {
            Some(names) if !names.is_empty() => {
                Some(names.iter().map(String::as_str).collect())
            }
            _ => None,
        };
        self.store
            .instance_variables(instance_key)
            .into_iter()
            // Only the root scope (scope == the instance itself) is "visible in
            // the root scope" per the API contract.
            .filter(|v| v.scope_key == instance_key)
            .filter(|v| wanted.as_ref().is_none_or(|w| w.contains(v.name.as_str())))
            .map(|v| {
                let value = serde_json::from_str(&v.value).unwrap_or(serde_json::Value::Null);
                (v.name, types::Object(value))
            })
            .collect()
    }

    /// `POST /v2/process-instances/{processInstanceKey}/cancellation` — cancel a
    /// running instance: discard every token (cancel pending jobs, disarm timers
    /// and message subscriptions, close any active incident) and transition the
    /// instance to `TERMINATED`. An unknown or already-finished instance is a 404.
    async fn cancel_process_instance_impl(
        &self,
        path_params: &models::CancelProcessInstancePathParams,
    ) -> Result<apis::process_instance::CancelProcessInstanceResponse, ()> {
        use apis::process_instance::CancelProcessInstanceResponse as Resp;

        let instance_key: u64 = match path_params.process_instance_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_TheProcessInstanceIsNotFound(problem(
                    "Process instance not found",
                    404,
                    format!(
                        "Process instance key '{}' is not a valid key.",
                        path_params.process_instance_key
                    ),
                )));
            }
        };

        if let Some(node) = self.remote_owner_of(instance_key) {
            return Ok(self.forward_cancel_instance(node, instance_key).await);
        }

        let result = self
            .engine
            .by_key(instance_key)
            .with(move |engine| {
                engine.apply_command_at(Command::cancel_instance(instance_key), now_millis())
            })
            .await;
        match result {
            Ok((events, commit)) => {
                commit.wait().await;
                self.spawn_routing_if_needed(&events);
                Ok(Resp::Status204_TheProcessInstanceIsCanceled)
            }
            Err(EngineError::InstanceNotFound { instance_key }) => {
                Ok(Resp::Status404_TheProcessInstanceIsNotFound(problem(
                    "Process instance not found",
                    404,
                    format!("No active process instance with key {instance_key}."),
                )))
            }
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    async fn complete_job_impl(
        &self,
        path_params: &models::CompleteJobPathParams,
        body: &Option<models::JobCompletionRequest>,
    ) -> Result<apis::job::CompleteJobResponse, ()> {
        use apis::job::CompleteJobResponse as Resp;

        let job_key: u64 = match path_params.job_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_TheJobWithTheGivenKeyWasNotFound(problem(
                    "Job not found",
                    404,
                    format!("Job key '{}' is not a valid key.", path_params.job_key),
                )));
            }
        };

        // Variables the worker returns are merged into the instance, so they can
        // drive downstream gateway routing.
        let variables = body
            .as_ref()
            .and_then(|b| b.variables.as_ref())
            .and_then(|v| match v {
                types::Nullable::Present(map) => Some(from_object_map(map)),
                types::Nullable::Null => None,
            })
            .unwrap_or_default();

        // Cluster: if a peer owns the job's partition, forward over the command
        // stream and map its answer back (single-node always owns every key).
        if let Some(node) = self.remote_owner_of(job_key) {
            let wire = body
                .as_ref()
                .and_then(|b| b.variables.as_ref())
                .and_then(|v| match v {
                    types::Nullable::Present(map) => wire_variables(Some(map)),
                    types::Nullable::Null => None,
                });
            return Ok(self.forward_complete_job(node, job_key, wire).await);
        }

        let result = self
            .engine
            .by_key(job_key)
            .with(move |engine| {
                engine.apply_command_at(Command::complete_job_with(job_key, variables), now_millis())
            })
            .await;
        match result {
            Ok((events, commit)) => {
                // Record REST job completion
                crate::metrics::record_job_completion("rest");
                // REST API: await fsync before replying (synchronous durability).
                // Contrast with command_stream::pipeline_job_command, which replies
                // immediately and awaits fsync in a detached task for throughput.
                commit.wait().await;
                // Completing a job may advance the token into an off-partition
                // message catch: route the resulting subscription open/correlate.
                self.spawn_routing_if_needed(&events);
                // Completing a job may advance the token onto a following service
                // task, creating a new activatable job: wake any long-pollers.
                self.signal_jobs_available();
                Ok(Resp::Status204_TheJobWasCompletedSuccessfully)
            }
            Err(EngineError::JobNotFound { job_key }) => {
                Ok(Resp::Status404_TheJobWithTheGivenKeyWasNotFound(problem(
                    "Job not found",
                    404,
                    format!("No job with key {job_key}."),
                )))
            }
            Err(EngineError::JobNotActive { job_key }) => Ok(
                Resp::Status409_TheJobWithTheGivenKeyIsInTheWrongStateCurrently(problem(
                    "Job not active",
                    409,
                    format!("Job {job_key} is not active and cannot be completed."),
                )),
            ),
            Err(EngineError::JobNotActivated { job_key }) => Ok(
                Resp::Status409_TheJobWithTheGivenKeyIsInTheWrongStateCurrently(problem(
                    "Job not activated",
                    409,
                    format!("Job {job_key} has not been activated and cannot be completed."),
                )),
            ),
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    async fn fail_job_impl(
        &self,
        path_params: &models::FailJobPathParams,
        body: &Option<models::JobFailRequest>,
    ) -> Result<apis::job::FailJobResponse, ()> {
        use apis::job::FailJobResponse as Resp;

        let job_key: u64 = match path_params.job_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_TheJobWithTheGivenJobKeyIsNotFound(problem(
                    "Job not found",
                    404,
                    format!("Job key '{}' is not a valid key.", path_params.job_key),
                )));
            }
        };

        let retries = body.as_ref().and_then(|b| b.retries).unwrap_or(0);
        let error_message = body
            .as_ref()
            .and_then(|b| b.error_message.clone())
            .unwrap_or_default();

        if let Some(node) = self.remote_owner_of(job_key) {
            return Ok(self.forward_fail_job(node, job_key, retries, error_message).await);
        }

        let result = self
            .engine
            .by_key(job_key)
            .with(move |engine| {
                engine.apply_command_at(Command::fail_job(job_key, retries, error_message), now_millis())
            })
            .await;
        match result {
            Ok((_, commit)) => {
                // Record REST job completion (fail also completes the job lifecycle)
                crate::metrics::record_job_completion("rest");
                commit.wait().await;
                // Failing with retries left returns the job to the activatable
                // pool, so wake any long-pollers.
                self.signal_jobs_available();
                Ok(Resp::Status204_TheJobIsFailed)
            }
            Err(EngineError::JobNotFound { job_key }) => {
                Ok(Resp::Status404_TheJobWithTheGivenJobKeyIsNotFound(problem(
                    "Job not found",
                    404,
                    format!("No job with key {job_key}."),
                )))
            }
            Err(EngineError::JobNotActive { job_key }) => Ok(
                Resp::Status409_TheJobWithTheGivenKeyIsInTheWrongState(problem(
                    "Job in wrong state",
                    409,
                    format!("Job {job_key} cannot be failed in its current state."),
                )),
            ),
            Err(EngineError::JobNotActivated { job_key }) => Ok(
                Resp::Status409_TheJobWithTheGivenKeyIsInTheWrongState(problem(
                    "Job not activated",
                    409,
                    format!("Job {job_key} has not been activated and cannot be failed."),
                )),
            ),
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    async fn throw_job_error_impl(
        &self,
        path_params: &models::ThrowJobErrorPathParams,
        body: &models::JobErrorRequest,
    ) -> Result<apis::job::ThrowJobErrorResponse, ()> {
        use apis::job::ThrowJobErrorResponse as Resp;

        let job_key: u64 = match path_params.job_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(
                    Resp::Status404_TheJobWithTheGivenKeyWasNotFoundOrIsNotActivated(problem(
                        "Job not found",
                        404,
                        format!("Job key '{}' is not a valid key.", path_params.job_key),
                    )),
                );
            }
        };

        let error_message = match body.error_message.as_ref() {
            Some(types::Nullable::Present(msg)) => msg.clone(),
            _ => String::new(),
        };
        let body_error_code = body.error_code.clone();

        if let Some(node) = self.remote_owner_of(job_key) {
            return Ok(self
                .forward_throw_error(node, job_key, body_error_code, error_message)
                .await);
        }

        let result = self
            .engine
            .by_key(job_key)
            .with(move |engine| {
                engine.apply_command_at(
                    Command::throw_job_error(job_key, body_error_code, error_message),
                    now_millis(),
                )
            })
            .await;
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                // A caught error can route the token onto a following service
                // task, creating a new activatable job: wake any long-pollers.
                self.signal_jobs_available();
                Ok(Resp::Status204_AnErrorIsThrownForTheJob)
            }
            Err(EngineError::JobNotFound { job_key }) => Ok(
                Resp::Status404_TheJobWithTheGivenKeyWasNotFoundOrIsNotActivated(problem(
                    "Job not found",
                    404,
                    format!("No job with key {job_key}."),
                )),
            ),
            Err(EngineError::JobNotActivated { job_key }) => Ok(
                Resp::Status404_TheJobWithTheGivenKeyWasNotFoundOrIsNotActivated(problem(
                    "Job not activated",
                    404,
                    format!("Job {job_key} has not been activated and cannot throw an error."),
                )),
            ),
            Err(EngineError::JobNotActive { job_key }) => Ok(
                Resp::Status409_TheJobWithTheGivenKeyIsInTheWrongStateCurrently(problem(
                    "Job in wrong state",
                    409,
                    format!("Job {job_key} is not active and cannot throw an error."),
                )),
            ),
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    async fn update_job_impl(
        &self,
        path_params: &models::UpdateJobPathParams,
        body: &models::JobUpdateRequest,
    ) -> Result<apis::job::UpdateJobResponse, ()> {
        use apis::job::UpdateJobResponse as Resp;

        let job_key: u64 = match path_params.job_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_TheJobWithTheJobKeyIsNotFound(problem(
                    "Job not found",
                    404,
                    format!("Job key '{}' is not a valid key.", path_params.job_key),
                )));
            }
        };

        // The POC only acts on the retries part of the changeset (timeout
        // updates are not modelled). A changeset without retries is a no-op.
        let retries = match body.changeset.retries.as_ref() {
            Some(types::Nullable::Present(r)) => *r,
            _ => {
                return Ok(Resp::Status204_TheJobWasUpdatedSuccessfully);
            }
        };

        if let Some(node) = self.remote_owner_of(job_key) {
            return Ok(self.forward_update_job(node, job_key, retries).await);
        }

        let result = self
            .engine
            .by_key(job_key)
            .with(move |engine| {
                engine.apply_command_at(Command::update_job_retries(job_key, retries), now_millis())
            })
            .await;
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                Ok(Resp::Status204_TheJobWasUpdatedSuccessfully)
            }
            Err(EngineError::JobNotFound { job_key }) => {
                Ok(Resp::Status404_TheJobWithTheJobKeyIsNotFound(problem(
                    "Job not found",
                    404,
                    format!("No job with key {job_key}."),
                )))
            }
            Err(EngineError::JobNotActive { job_key }) => Ok(
                Resp::Status409_TheJobWithTheGivenKeyIsInTheWrongStateCurrently(problem(
                    "Job in wrong state",
                    409,
                    format!("Job {job_key} is terminal and its retries cannot be updated."),
                )),
            ),
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    async fn resolve_incident_impl(
        &self,
        path_params: &models::ResolveIncidentPathParams,
        body: &Option<models::IncidentResolutionRequest>,
    ) -> Result<apis::incident::ResolveIncidentResponse, ()> {
        use apis::incident::ResolveIncidentResponse as Resp;

        let incident_key: u64 = match path_params.incident_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_TheIncidentWithTheIncidentKeyIsNotFound(
                    problem(
                        "Incident not found",
                        404,
                        format!(
                            "Incident key '{}' is not a valid key.",
                            path_params.incident_key
                        ),
                    ),
                ));
            }
        };

        let operation_reference = body.as_ref().and_then(|b| b.operation_reference);

        if let Some(node) = self.remote_owner_of(incident_key) {
            return Ok(self
                .forward_resolve_incident(node, incident_key, operation_reference)
                .await);
        }

        let command = Command::ResolveIncident {
            incident_key,
            operation_reference,
        };

        let result = self
            .engine
            .by_key(incident_key)
            .with(move |engine| engine.apply_command_at(command, now_millis()))
            .await;
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                // Resolving a job-incident returns the job to the activatable
                // pool, so wake any long-pollers.
                self.signal_jobs_available();
                Ok(Resp::Status204_TheIncidentIsMarkedAsResolved)
            }
            Err(EngineError::IncidentNotFound { incident_key }) => Ok(
                Resp::Status404_TheIncidentWithTheIncidentKeyIsNotFound(problem(
                    "Incident not found",
                    404,
                    format!("No incident with key {incident_key}."),
                )),
            ),
            Err(EngineError::IncidentNotResolvable { reason, .. }) => Ok(
                Resp::Status409_TheIncidentCannotBeResolvedDueToAnInvalidState(problem(
                    "Incident not resolvable",
                    409,
                    reason,
                )),
            ),
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    /// `PUT /v2/element-instances/{elementInstanceKey}/variables` — merge
    /// variables into a scope so an operator can correct the data behind an
    /// incident before resolving it. The path key may be the process instance
    /// key or an element instance key; both resolve to nano's single
    /// instance-level scope (so `local` is accepted but has no effect).
    async fn create_element_instance_variables_impl(
        &self,
        path_params: &models::CreateElementInstanceVariablesPathParams,
        body: &models::SetVariableRequest,
    ) -> Result<apis::element_instance::CreateElementInstanceVariablesResponse, ()> {
        use apis::element_instance::CreateElementInstanceVariablesResponse as Resp;

        let scope_key: u64 = match path_params.element_instance_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                    "Invalid key",
                    400,
                    format!(
                        "Element instance key '{}' is not a valid key.",
                        path_params.element_instance_key
                    ),
                )));
            }
        };

        let variables = from_object_map(&body.variables);

        if let Some(node) = self.remote_owner_of(scope_key) {
            return Ok(self
                .forward_set_variables(node, scope_key, wire_variables(Some(&body.variables)))
                .await);
        }

        let result = self
            .engine
            .by_key(scope_key)
            .with(move |engine| {
                engine.apply_command_at(Command::set_variables(scope_key, variables), now_millis())
            })
            .await;
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                Ok(Resp::Status204_TheVariablesWereUpdated)
            }
            Err(EngineError::ScopeNotFound { scope_key }) => {
                Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                    "Scope not found",
                    400,
                    format!("No process or element instance with key {scope_key}."),
                )))
            }
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    async fn get_process_instance_impl(
        &self,
        path_params: &models::GetProcessInstancePathParams,
    ) -> Result<apis::process_instance::GetProcessInstanceResponse, ()> {
        use apis::process_instance::GetProcessInstanceResponse as Resp;

        let key: u64 = match path_params.process_instance_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(
                    Resp::Status404_TheProcessInstanceWithTheGivenKeyWasNotFound(problem(
                        "Process instance not found",
                        404,
                        format!(
                            "Process instance key '{}' is not a valid key.",
                            path_params.process_instance_key
                        ),
                    )),
                );
            }
        };

        let result = self.store.process_instance(key);
        if result.is_none() {
            if let Some(node) = self.remote_owner_of(key) {
                let (status, body) = self
                    .forward_get(node, crate::command_stream::ReadKind::ProcessInstance, key)
                    .await;
                return Ok(match (status, body) {
                    (200, Some(b)) => match serde_json::from_value(b) {
                        Ok(r) => Resp::Status200_TheProcessInstanceIsSuccessfullyReturned(r),
                        Err(e) => {
                            Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                                problem("Peer error", 500, e.to_string()),
                            )
                        }
                    },
                    (404, _) => Resp::Status404_TheProcessInstanceWithTheGivenKeyWasNotFound(
                        problem("Process instance not found", 404, format!("No process instance with key {key}.")),
                    ),
                    (s, _) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                        problem("Peer error", 500, format!("peer node {node} returned status {s}")),
                    ),
                });
            }
        }
        match result {
            Some(instance) => Ok(Resp::Status200_TheProcessInstanceIsSuccessfullyReturned(
                process_instance_result(&instance),
            )),
            None => Ok(
                Resp::Status404_TheProcessInstanceWithTheGivenKeyWasNotFound(problem(
                    "Process instance not found",
                    404,
                    format!("No process instance with key {key}."),
                )),
            ),
        }
    }

    /// Reports the real cluster topology: one broker per node, each advertising
    /// the partitions it owns (deterministic `partition % num_nodes` ownership).
    /// Partition ids are surfaced 1-based (Camunda convention) over nano's 0-based
    /// internal partitions. At replication factor 1 each owned partition has a
    /// single replica, so its owner is reported as the `leader`. A single-node
    /// cluster reports one broker owning every partition — equivalent to the
    /// previous hardcoded response but with the real partition count.
    async fn get_topology_impl(&self) -> Result<apis::cluster::GetTopologyResponse, ()> {
        use apis::cluster::GetTopologyResponse as Resp;

        let version = env!("CARGO_PKG_VERSION").to_string();
        let topology = self.engine.topology();
        let num_nodes = topology.num_nodes();
        let num_partitions = topology.num_partitions;

        // host:port for a broker. Peer URLs are `http://host:port`; for a
        // single-node cluster `peers[0]` is unset, so fall back to this node's
        // bound PORT. `self` (this node) always reports its own bound port.
        let self_port: i32 = std::env::var("PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8080);
        let host_port = |node: u32| -> (String, i32) {
            if node == topology.node_id {
                return ("0.0.0.0".to_string(), self_port);
            }
            let url = topology.peers.get(node as usize).map(String::as_str).unwrap_or("");
            parse_host_port(url).unwrap_or(("0.0.0.0".to_string(), self_port))
        };

        let brokers: Vec<models::BrokerInfo> = (0..num_nodes)
            .map(|node| {
                let partitions: Vec<models::Partition> = (0..num_partitions)
                    .filter(|p| topology.owner_of(*p) == node)
                    .map(|p| models::Partition {
                        // 1-based partition id (Camunda convention).
                        partition_id: (p + 1) as i32,
                        role: "leader".to_string(),
                        health: "healthy".to_string(),
                    })
                    .collect();
                let (host, port) = host_port(node);
                models::BrokerInfo {
                    node_id: node as i32,
                    host,
                    port,
                    partitions,
                    version: version.clone(),
                }
            })
            .collect();

        let topology_response = models::TopologyResponse {
            brokers,
            cluster_id: types::Nullable::Null,
            cluster_size: num_nodes as i32,
            partitions_count: num_partitions as i32,
            replication_factor: 1,
            gateway_version: version,
            last_completed_change_id: String::new(),
        };

        Ok(Resp::Status200_ObtainsTheCurrentTopologyOfTheClusterTheGatewayIsPartOf(topology_response))
    }

    /// Correlates a message, routing it to **only** the partitions that can hold a
    /// matching subscription, and returns the combined events. With canonical
    /// placement a message's subscriptions live on exactly two well-known
    /// partitions: the intermediate/boundary catch subscriptions for this
    /// `correlation_key` sit on `subscription_partition(correlation_key)`, and the
    /// process-level message-start subscriptions live on the deployment partition
    /// (partition 0). The publish therefore needs to reach just those two (often
    /// one), not every partition — replacing the old all-partitions broadcast.
    ///
    /// Only the **local** members of that target set are applied here; a target
    /// owned by a peer is reached because the cluster publish is broadcast to
    /// every node (each node correlates its own local subset), so the union still
    /// covers both owners. With a single partition the target set is `{0}` — one
    /// round-trip, identical to the pre-partitioning path.
    async fn correlate_message_everywhere(
        &self,
        name: String,
        correlation_key: String,
        variables: std::collections::HashMap<String, Value>,
    ) -> Vec<Event> {
        let num_partitions = self.engine.topology().num_partitions;
        // The (deduplicated) partitions that can own a matching subscription: the
        // correlation-key hash partition (catch/boundary) + partition 0 (starts).
        let hash_partition =
            nanobpmn_engine_core::subscription_partition(&correlation_key, num_partitions);
        let mut targets = vec![hash_partition];
        if hash_partition != 0 {
            targets.push(0);
        }

        let mut all_events: Vec<Event> = Vec::new();
        for target in targets {
            // Skip a target this node does not own; the cluster broadcast routes
            // the publish to the owning node, which correlates it locally.
            let Some(handle) = self.engine.local_for_partition(target) else {
                continue;
            };
            let name = name.clone();
            let correlation_key = correlation_key.clone();
            let variables = variables.clone();
            let (events, commit) = handle
                .with(move |engine| {
                    engine
                        .apply_command_at(
                            Command::correlate_message_with(name, correlation_key, variables),
                            now_millis(),
                        )
                        .expect("CorrelateMessage never fails")
                })
                .await;
            commit.wait().await;
            all_events.extend(events.iter().cloned());
        }
        // Drive the token advance for any subscription whose instance lives on
        // another partition (the publish settled it here via
        // `RemoteMessageCorrelation`), plus any start-instance dispatch a
        // message-start fan-out produced. A no-op single-partition.
        if Self::has_routable_subscription_events(&all_events) {
            self.drive_subscription_routing(Self::routable_events(&all_events))
                .await;
        }
        all_events
    }

    /// Correlates a message across this node's owned partitions and returns the
    /// minted message key and the first instance it correlated to (an existing
    /// subscription or a message-start-created instance), if any. The local
    /// building block for both the gateway fan-out and the peer-side
    /// `PublishMessage` handler.
    pub(crate) async fn correlate_message_local(
        &self,
        name: String,
        correlation_key: String,
        variables: std::collections::HashMap<String, Value>,
    ) -> (u64, Option<u64>) {
        let events = self
            .correlate_message_everywhere(name, correlation_key, variables)
            .await;
        let message_key = message_key_of(&events);
        let instance = events.iter().find_map(|e| match e {
            Event::MessageCorrelated { instance_key, .. } => Some(*instance_key),
            // A cross-partition correlation: the instance advanced on its own
            // partition via the routed continuation, but the match was recorded
            // here as a remote correlation.
            Event::RemoteMessageCorrelation { instance_key, .. } => Some(*instance_key),
            Event::ProcessInstanceCreated { instance_key, .. } => Some(*instance_key),
            _ => None,
        });
        (message_key, instance)
    }

    /// Correlates a message across the **whole cluster**: this node's own
    /// partitions plus every peer (each correlates against its own partitions).
    /// An open subscription's instance can live on any node, and the message-start
    /// subscriptions live solely on the partition-0 owner, so a published message
    /// must reach every node. Returns the (locally minted) message key and the
    /// first correlated instance found across the cluster — local matches first,
    /// then peers. A peer that is unreachable is logged and skipped (best-effort,
    /// matching the no-buffer correlate-and-drop model). A single-node cluster
    /// fans only locally — byte-identical to the pre-cluster path.
    async fn correlate_message_cluster(
        &self,
        name: String,
        correlation_key: String,
        variables: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> (u64, Option<u64>) {
        let engine_vars: std::collections::HashMap<String, Value> = variables
            .as_ref()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), json_to_value(v)))
                    .collect()
            })
            .unwrap_or_default();
        let (message_key, mut instance) = self
            .correlate_message_local(name.clone(), correlation_key.clone(), engine_vars)
            .await;

        if self.peers.has_peers() {
            let topology = self.engine.topology();
            // With canonical placement a published message can only match
            // subscriptions on two partitions: the correlation-key hash partition
            // (catch/boundary subs) and partition 0 (message-start subs). So fan
            // the publish out to just the NODES owning those two partitions
            // (deduplicated, excluding self) instead of every peer.
            let hash_partition = nanobpmn_engine_core::subscription_partition(
                &correlation_key,
                topology.num_partitions,
            );
            let mut target_nodes = vec![topology.owner_of(hash_partition), topology.owner_of(0)];
            target_nodes.sort_unstable();
            target_nodes.dedup();
            for node in target_nodes {
                if node == topology.node_id {
                    continue;
                }
                match self.peers.link(node).await {
                    Ok(link) => match link
                        .publish_message(name.clone(), correlation_key.clone(), variables.clone())
                        .await
                    {
                        Ok(res) if res.status == 200 => {
                            if instance.is_none() {
                                instance = res
                                    .body
                                    .as_ref()
                                    .and_then(|b| b.get("correlatedInstanceKey"))
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<u64>().ok());
                            }
                        }
                        Ok(res) => {
                            tracing::warn!("message fan-out to node {node}: status {}", res.status)
                        }
                        Err(e) => tracing::warn!("message fan-out to node {node} failed: {e}"),
                    },
                    Err(e) => tracing::warn!("message fan-out: node {node} unreachable: {e}"),
                }
            }
        }
        (message_key, instance)
    }

    // ---- Cross-partition subscription routing (stage 2, s2-subindex) -------
    //
    // The host half of the engine's two-phase placement protocol. When a token
    // parks on a message catch/boundary whose correlation key hashes to a
    // *different* partition, the engine emits `MessageSubscriptionOpening`; when
    // a publish correlates on a message partition for an instance that lives
    // elsewhere, it emits `RemoteMessageCorrelation`. This pump routes the
    // follow-up command to the partition that must apply it (today: any partition
    // this node owns; cross-node routing is a later increment), and recurses,
    // because advancing a token can itself reach another off-partition catch.
    //
    // Single-partition (or single-node, single-partition) hosts never produce
    // those events, so `drive_subscription_routing` is an immediate no-op and the
    // hot path is unchanged.

    /// True when `events` carry any cross-partition follow-up the pump must route.
    /// Cheap early-out so callers can hand every command's output to the pump.
    fn has_routable_subscription_events(events: &[Event]) -> bool {
        events.iter().any(Self::is_routable_event)
    }

    /// Whether `e` is a cross-partition follow-up the host pump must route to
    /// another partition: a subscription open/correlate/close, or a
    /// start-instance dispatch.
    fn is_routable_event(e: &Event) -> bool {
        matches!(
            e,
            Event::MessageSubscriptionOpening { .. }
                | Event::RemoteMessageCorrelation { .. }
                | Event::MessageSubscriptionClosing { .. }
                | Event::StartInstanceDispatched { .. }
        )
    }

    /// The cross-partition follow-ups carried in `events`, cloned for routing.
    fn routable_events(events: &[Event]) -> Vec<Event> {
        events
            .iter()
            .filter(|e| Self::is_routable_event(e))
            .cloned()
            .collect()
    }

    /// Routes the cross-partition subscription follow-ups carried in `events` (and
    /// any they transitively produce) to the partition that must apply them. When
    /// the target partition is owned by **this** node it is applied locally and
    /// its follow-ups are folded back into the worklist; when it is owned by a
    /// **peer**, the source event is forwarded over the command stream and the
    /// owning node drives its own pump (so the recursion continues there). A
    /// no-op single-partition.
    async fn drive_subscription_routing(&self, events: Vec<Event>) {
        let num_partitions = self.engine.topology().num_partitions;
        if num_partitions <= 1 {
            return;
        }
        let mut work = events;
        while let Some(event) = work.pop() {
            let Some((target, command)) = Self::route_event(&event, num_partitions) else {
                continue;
            };
            match self.engine.local_for_partition(target) {
                Some(handle) => {
                    let (produced, commit) = handle
                        .with(move |engine| {
                            engine
                                .apply_command_at(command, now_millis())
                                .expect("routed subscription command never fails")
                        })
                        .await;
                    commit.wait().await;
                    work.extend(produced.iter().cloned());
                }
                None => {
                    // The target partition is owned by a peer: forward the source
                    // event so the owner applies it (and drives any further
                    // routing) on its own engine.
                    self.forward_route_subscription(target, event).await;
                }
            }
        }
    }

    /// Derives the `(target partition, command)` a cross-partition follow-up event
    /// must be applied as: a `MessageSubscriptionOpening` opens the canonical
    /// subscription on `subscription_partition(correlation_key)`; a
    /// `RemoteMessageCorrelation` advances the parked token on the instance's
    /// partition. Returns `None` for any other event.
    fn route_event(event: &Event, num_partitions: u64) -> Option<(u64, Command)> {
        match event {
            Event::MessageSubscriptionOpening {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id,
                message_name,
                correlation_key,
                kind,
            } => {
                let target =
                    nanobpmn_engine_core::subscription_partition(correlation_key, num_partitions);
                let command = Command::OpenMessageSubscription {
                    subscription_key: *subscription_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    message_name: message_name.clone(),
                    correlation_key: correlation_key.clone(),
                    kind: kind.clone(),
                };
                Some((target, command))
            }
            Event::RemoteMessageCorrelation {
                subscription_key,
                message_key,
                instance_key,
                element_instance_key,
                element_id,
                kind,
                variables,
            } => {
                let target = nanobpmn_engine_core::partition_of(*instance_key);
                let command = Command::CorrelateMessageSubscription {
                    subscription_key: *subscription_key,
                    message_key: *message_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    kind: kind.clone(),
                    variables: variables.clone(),
                };
                Some((target, command))
            }
            Event::MessageSubscriptionClosing {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id,
                correlation_key,
                ..
            } => {
                let target =
                    nanobpmn_engine_core::subscription_partition(correlation_key, num_partitions);
                let command = Command::CloseMessageSubscription {
                    subscription_key: *subscription_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                };
                Some((target, command))
            }
            Event::StartInstanceDispatched {
                process_id,
                start_element_id,
                variables,
                tags,
                business_id,
                target_partition,
            } => {
                let command = Command::DispatchStartInstance {
                    process_id: process_id.clone(),
                    start_element_id: start_element_id.clone(),
                    variables: variables.clone(),
                    tags: tags.clone(),
                    business_id: business_id.clone(),
                };
                Some((*target_partition, command))
            }
            _ => None,
        }
    }

    /// Forwards a cross-partition subscription follow-up `event` to the peer that
    /// owns `target_partition`, over the command stream. The owner applies it and
    /// drives its own pump for any further follow-ups. Best-effort: a delivery
    /// failure is logged and dropped (idempotent — the source command re-emits the
    /// event on replay; at RF=1 a lost open just leaves the instance parked until
    /// a retry/restart re-routes it).
    async fn forward_route_subscription(&self, target_partition: u64, event: Event) {
        let owner = self.engine.topology().owner_of(target_partition);
        match self.peers.link(owner).await {
            Ok(link) => {
                if let Err(e) = link.route_subscription(event).await {
                    tracing::warn!(
                        "subscription routing to node {owner} (partition {target_partition}) failed: {e}"
                    );
                }
            }
            Err(e) => tracing::warn!(
                "subscription routing: node {owner} (partition {target_partition}) unreachable: {e}"
            ),
        }
    }

    /// Peer-side handler for a forwarded subscription follow-up: the owning node
    /// drives its own pump for `event` (it now owns the target partition, so the
    /// apply is local), recursing into any further cross-node follow-ups. Wakes
    /// pollers in case the correlation advanced a token onto a service task.
    pub(crate) async fn apply_routed_subscription_remote(&self, event: Event) {
        self.drive_subscription_routing(vec![event]).await;
        self.signal_jobs_available();
    }

    /// Spawns cross-partition subscription routing for the follow-ups carried in
    /// `events`, off the caller's path. Used by the pipelined job-completion path
    /// (which acks before its own fsync): completing a job can advance a token
    /// into a message catch whose subscription is owned by another partition,
    /// emitting `MessageSubscriptionOpening`. Routing is idempotent
    /// (`OpenMessageSubscription` keys on the subscription), so fire-and-forget is
    /// safe even if the instance partition has not yet fsynced — a crash re-emits
    /// the Opening on replay and re-routes. A no-op single-partition.
    fn spawn_routing_if_needed(&self, events: &[Event]) {
        if self.engine.topology().num_partitions <= 1 {
            return;
        }
        if !Self::has_routable_subscription_events(events) {
            return;
        }
        let routable: Vec<Event> = events
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    Event::MessageSubscriptionOpening { .. }
                        | Event::RemoteMessageCorrelation { .. }
                        | Event::MessageSubscriptionClosing { .. }
                        | Event::StartInstanceDispatched { .. }
                )
            })
            .cloned()
            .collect();
        let server = self.clone();
        tokio::spawn(async move {
            server.drive_subscription_routing(routable).await;
        });
    }

    // ---- By-key forwarding (stage 1) -------------------------------------
    //
    // A client may submit a by-key mutation (complete/fail/throwError a job,
    // cancel an instance, update retries, resolve an incident, set variables)
    // to ANY gateway. If the key's partition is owned by a peer, the gateway
    // forwards the operation to that peer over the command stream; the peer
    // applies it on its owning partition (durably) and answers. The `*_local`
    // methods below are the peer-side apply step (also reused by the in-crate
    // tests); the `forward_*` methods are the gateway-side uplink + response
    // mapping. The hot, same-node path never calls either: `remote_owner`
    // returns `None` and the existing in-process handler runs unchanged.

    /// Applies a `cancelProcessInstance` on this node's owning partition,
    /// awaiting durability. Status mapping mirrors the REST handler.
    pub(crate) async fn cancel_instance_local(
        &self,
        instance_key: u64,
    ) -> Result<(), (u16, String)> {
        let result = self
            .engine
            .by_key(instance_key)
            .with(move |engine| {
                engine.apply_command_at(Command::cancel_instance(instance_key), now_millis())
            })
            .await;
        match result {
            Ok((events, commit)) => {
                commit.wait().await;
                self.spawn_routing_if_needed(&events);
                self.signal_jobs_available();
                Ok(())
            }
            Err(EngineError::InstanceNotFound { instance_key }) => Err((
                404,
                format!("No active process instance with key {instance_key}."),
            )),
            Err(e) => Err((500, e.to_string())),
        }
    }

    /// Applies a job-retries update on this node's owning partition.
    pub(crate) async fn update_job_retries_local(
        &self,
        job_key: u64,
        retries: i32,
    ) -> Result<(), (u16, String)> {
        let result = self
            .engine
            .by_key(job_key)
            .with(move |engine| {
                engine.apply_command_at(Command::update_job_retries(job_key, retries), now_millis())
            })
            .await;
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                Ok(())
            }
            Err(EngineError::JobNotFound { job_key }) => {
                Err((404, format!("No job with key {job_key}.")))
            }
            Err(EngineError::JobNotActive { job_key }) => Err((
                409,
                format!("Job {job_key} is terminal and its retries cannot be updated."),
            )),
            Err(e) => Err((500, e.to_string())),
        }
    }

    /// Resolves an incident on this node's owning partition.
    pub(crate) async fn resolve_incident_local(
        &self,
        incident_key: u64,
        operation_reference: Option<i64>,
    ) -> Result<(), (u16, String)> {
        let command = Command::ResolveIncident {
            incident_key,
            operation_reference,
        };
        let result = self
            .engine
            .by_key(incident_key)
            .with(move |engine| engine.apply_command_at(command, now_millis()))
            .await;
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                self.signal_jobs_available();
                Ok(())
            }
            Err(EngineError::IncidentNotFound { incident_key }) => {
                Err((404, format!("No incident with key {incident_key}.")))
            }
            Err(EngineError::IncidentNotResolvable { reason, .. }) => Err((409, reason)),
            Err(e) => Err((500, e.to_string())),
        }
    }

    /// Merges variables into a scope on this node's owning partition.
    pub(crate) async fn set_variables_local(
        &self,
        scope_key: u64,
        variables: std::collections::HashMap<String, Value>,
    ) -> Result<(), (u16, String)> {
        let result = self
            .engine
            .by_key(scope_key)
            .with(move |engine| {
                engine.apply_command_at(Command::set_variables(scope_key, variables), now_millis())
            })
            .await;
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                Ok(())
            }
            Err(EngineError::ScopeNotFound { scope_key }) => Err((
                400,
                format!("No process or element instance with key {scope_key}."),
            )),
            Err(e) => Err((500, e.to_string())),
        }
    }

    /// Returns the id of the peer owning `key`'s partition, or `None` when this
    /// node owns it. The single branch every by-key REST handler consults before
    /// touching the local engine.
    pub(crate) fn remote_owner_of(&self, key: u64) -> Option<u32> {
        self.engine.remote_owner(key)
    }

    /// Remote node ids this gateway forwards to (every peer that owns at least one
    /// partition this node does not). Empty on a single-node cluster, so the
    /// dispatcher's job-aggregation fan-out is a no-op and the hot path is
    /// byte-identical to the pre-cluster build.
    pub(crate) fn peer_nodes(&self) -> Vec<u32> {
        self.engine.peer_nodes()
    }

    /// Acquires the uplink to peer `node`, mapping a connect failure to a 502.
    async fn peer_link(&self, node: u32) -> Result<crate::peer::PeerLink, (u16, String)> {
        self.peers
            .link(node)
            .await
            .map_err(|e| (502, format!("peer node {node} unreachable: {e}")))
    }

    /// Peer-side of query forwarding: answers a GET-by-key read for a key this
    /// node owns from its local read model. Returns `(200, entity-json)` or
    /// `(404, None)` (or `(500, None)` on a serialization error). The gateway
    /// reconstructs the typed REST response from the status + body.
    pub(crate) fn read_by_key_local(
        &self,
        kind: crate::command_stream::ReadKind,
        key: u64,
    ) -> (u16, Option<serde_json::Value>) {
        use crate::command_stream::ReadKind;
        let body = match kind {
            ReadKind::ProcessInstance => self
                .store
                .process_instance(key)
                .map(|x| serde_json::to_value(process_instance_result(&x))),
            ReadKind::Incident => self
                .store
                .incident(key)
                .map(|x| serde_json::to_value(incident_result(&x))),
            ReadKind::UserTask => self
                .store
                .user_tasks()
                .iter()
                .find(|t| t.key == key)
                .map(|t| serde_json::to_value(user_task_result(t))),
            ReadKind::Variable => self
                .store
                .variable(key)
                .map(|v| serde_json::to_value(variable_result(&v))),
        };
        match body {
            Some(Ok(v)) => (200, Some(v)),
            Some(Err(_)) => (500, None),
            None => (404, None),
        }
    }

    /// Forwards a GET-by-key read to the peer owning the key's partition and
    /// returns its `(status, body)` for the gateway handler to map into the
    /// typed REST response.
    async fn forward_get(
        &self,
        node: u32,
        kind: crate::command_stream::ReadKind,
        key: u64,
    ) -> (u16, Option<serde_json::Value>) {
        match self.peer_link(node).await {
            Ok(link) => match link.get_by_key(kind, key).await {
                Ok(r) => (r.status, r.body),
                Err(_) => (502, None),
            },
            Err((s, _)) => (s, None),
        }
    }

    /// Peer-side of user-task forwarding: re-applies the original REST mutation
    /// locally (this node owns the task's partition, so the per-handler
    /// `remote_owner_of` check resolves Local — no forwarding loop) and reports
    /// the REST status plus an optional problem detail. The gateway maps the
    /// status back to its typed response.
    pub(crate) async fn apply_user_task_forwarded(
        &self,
        op: crate::command_stream::UserTaskOp,
        user_task_key: &str,
        payload: Option<serde_json::Value>,
    ) -> (u16, Option<String>) {
        use crate::command_stream::UserTaskOp;
        let key = user_task_key.to_string();
        match op {
            UserTaskOp::Assign => {
                use apis::user_task::AssignUserTaskResponse as R;
                let body: models::UserTaskAssignmentRequest = match payload {
                    Some(v) => match serde_json::from_value(v) {
                        Ok(b) => b,
                        Err(e) => return (400, Some(e.to_string())),
                    },
                    None => return (400, Some("missing assignment payload".to_string())),
                };
                let path = models::AssignUserTaskPathParams {
                    user_task_key: key,
                };
                match self.assign_user_task_impl(&path, &body).await {
                    Ok(R::Status204_TheUserTask) => (204, None),
                    Ok(R::Status404_TheUserTaskWithTheGivenKeyWasNotFound(p)) => {
                        (404, Some(p.detail))
                    }
                    Ok(R::Status409_TheUserTaskWithTheGivenKeyIsInTheWrongStateCurrently(p)) => {
                        (409, Some(p.detail))
                    }
                    Ok(R::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(p)) => {
                        (500, Some(p.detail))
                    }
                    _ => (500, None),
                }
            }
            UserTaskOp::Complete => {
                use apis::user_task::CompleteUserTaskResponse as R;
                let body: Option<models::UserTaskCompletionRequest> = match payload {
                    Some(v) => match serde_json::from_value(v) {
                        Ok(b) => Some(b),
                        Err(e) => return (400, Some(e.to_string())),
                    },
                    None => None,
                };
                let path = models::CompleteUserTaskPathParams {
                    user_task_key: key,
                };
                match self.complete_user_task_impl(&path, &body).await {
                    Ok(R::Status204_TheUserTaskWasCompletedSuccessfully) => (204, None),
                    Ok(R::Status404_TheUserTaskWithTheGivenKeyWasNotFound(p)) => {
                        (404, Some(p.detail))
                    }
                    Ok(R::Status409_TheUserTaskWithTheGivenKeyIsInTheWrongStateCurrently(p)) => {
                        (409, Some(p.detail))
                    }
                    Ok(R::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(p)) => {
                        (500, Some(p.detail))
                    }
                    _ => (500, None),
                }
            }
            UserTaskOp::Unassign => {
                use apis::user_task::UnassignUserTaskResponse as R;
                let path = models::UnassignUserTaskPathParams {
                    user_task_key: key,
                };
                match self.unassign_user_task_impl(&path).await {
                    Ok(R::Status204_TheUserTaskWasUnassignedSuccessfully) => (204, None),
                    Ok(R::Status404_TheUserTaskWithTheGivenKeyWasNotFound(p)) => {
                        (404, Some(p.detail))
                    }
                    Ok(R::Status409_TheUserTaskWithTheGivenKeyIsInTheWrongStateCurrently(p)) => {
                        (409, Some(p.detail))
                    }
                    Ok(R::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(p)) => {
                        (500, Some(p.detail))
                    }
                    _ => (500, None),
                }
            }
            UserTaskOp::Update => {
                use apis::user_task::UpdateUserTaskResponse as R;
                let body: Option<models::UserTaskUpdateRequest> = match payload {
                    Some(v) => match serde_json::from_value(v) {
                        Ok(b) => Some(b),
                        Err(e) => return (400, Some(e.to_string())),
                    },
                    None => None,
                };
                let path = models::UpdateUserTaskPathParams {
                    user_task_key: key,
                };
                match self.update_user_task_impl(&path, &body).await {
                    Ok(R::Status204_TheUserTaskWasUpdatedSuccessfully) => (204, None),
                    Ok(R::Status404_TheUserTaskWithTheGivenKeyWasNotFound(p)) => {
                        (404, Some(p.detail))
                    }
                    Ok(R::Status409_TheUserTaskWithTheGivenKeyIsInTheWrongStateCurrently(p)) => {
                        (409, Some(p.detail))
                    }
                    Ok(R::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(p)) => {
                        (500, Some(p.detail))
                    }
                    _ => (500, None),
                }
            }
        }
    }

    /// Forwards a user-task by-key mutation to the peer owning the task's
    /// partition. `payload` is the original REST request body JSON. Returns the
    /// peer's `(status, detail)`; the gateway handler maps it to its typed
    /// response.
    async fn forward_user_task(
        &self,
        node: u32,
        op: crate::command_stream::UserTaskOp,
        user_task_key: u64,
        payload: Option<serde_json::Value>,
    ) -> (u16, String) {
        match self.peer_link(node).await {
            Ok(link) => match link
                .forward_user_task(op, user_task_key.to_string(), payload)
                .await
            {
                Ok(r) => (
                    r.status,
                    r.body
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .unwrap_or_default(),
                ),
                Err(e) => (502, e.to_string()),
            },
            Err((s, m)) => (s, m),
        }
    }

    /// Forwards a `completeJob` to the peer owning the job and maps its answer to
    /// the REST response. Used when the job's partition is owned by another node.
    async fn forward_complete_job(
        &self,
        node: u32,
        job_key: u64,
        variables: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> apis::job::CompleteJobResponse {
        use apis::job::CompleteJobResponse as Resp;
        let res = match self.peer_link(node).await {
            Ok(link) => link.complete_job(job_key.to_string(), variables).await,
            Err((s, m)) => return Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem("Peer error", s, m)),
        };
        match res {
            Ok(r) if is_ok_status(r.status) => Resp::Status204_TheJobWasCompletedSuccessfully,
            Ok(r) if r.status == 404 => Resp::Status404_TheJobWithTheGivenKeyWasNotFound(problem(
                "Job not found",
                404,
                peer_detail(&r),
            )),
            Ok(r) if r.status == 409 => {
                Resp::Status409_TheJobWithTheGivenKeyIsInTheWrongStateCurrently(problem(
                    "Job in wrong state",
                    409,
                    peer_detail(&r),
                ))
            }
            Ok(r) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                "Peer error",
                500,
                peer_detail(&r),
            )),
            Err(e) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                "Peer error",
                502,
                e.to_string(),
            )),
        }
    }

    /// Forwards a `failJob` to the peer owning the job.
    async fn forward_fail_job(
        &self,
        node: u32,
        job_key: u64,
        retries: i32,
        error_message: String,
    ) -> apis::job::FailJobResponse {
        use apis::job::FailJobResponse as Resp;
        let res = match self.peer_link(node).await {
            Ok(link) => link.fail_job(job_key.to_string(), retries, error_message).await,
            Err((s, m)) => return Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem("Peer error", s, m)),
        };
        match res {
            Ok(r) if is_ok_status(r.status) => Resp::Status204_TheJobIsFailed,
            Ok(r) if r.status == 404 => Resp::Status404_TheJobWithTheGivenJobKeyIsNotFound(problem(
                "Job not found",
                404,
                peer_detail(&r),
            )),
            Ok(r) if r.status == 409 => Resp::Status409_TheJobWithTheGivenKeyIsInTheWrongState(
                problem("Job in wrong state", 409, peer_detail(&r)),
            ),
            Ok(r) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                "Peer error",
                500,
                peer_detail(&r),
            )),
            Err(e) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                "Peer error",
                502,
                e.to_string(),
            )),
        }
    }

    /// Forwards a `throwError` to the peer owning the job.
    async fn forward_throw_error(
        &self,
        node: u32,
        job_key: u64,
        error_code: String,
        error_message: String,
    ) -> apis::job::ThrowJobErrorResponse {
        use apis::job::ThrowJobErrorResponse as Resp;
        let res = match self.peer_link(node).await {
            Ok(link) => link.throw_error(job_key.to_string(), error_code, error_message).await,
            Err((s, m)) => return Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem("Peer error", s, m)),
        };
        match res {
            Ok(r) if is_ok_status(r.status) => Resp::Status204_AnErrorIsThrownForTheJob,
            Ok(r) if r.status == 404 => {
                Resp::Status404_TheJobWithTheGivenKeyWasNotFoundOrIsNotActivated(problem(
                    "Job not found",
                    404,
                    peer_detail(&r),
                ))
            }
            Ok(r) if r.status == 409 => {
                Resp::Status409_TheJobWithTheGivenKeyIsInTheWrongStateCurrently(problem(
                    "Job in wrong state",
                    409,
                    peer_detail(&r),
                ))
            }
            Ok(r) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                "Peer error",
                500,
                peer_detail(&r),
            )),
            Err(e) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                "Peer error",
                502,
                e.to_string(),
            )),
        }
    }

    /// Stream-path forward of a `completeJob` to the peer that owns the job's
    /// partition. Unlike [`forward_complete_job`] (which builds a typed REST
    /// response), this relays the peer's raw `(status, body)` so the command-stream
    /// handler can mirror it straight into a `CommandResult`. Used when a worker
    /// attached to this gateway completes a job that a peer owns (job aggregation).
    pub(crate) async fn forward_complete_job_stream(
        &self,
        node: u32,
        job_key: u64,
        variables: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> (u16, Option<serde_json::Value>) {
        match self.peer_link(node).await {
            Ok(link) => match link.complete_job(job_key.to_string(), variables).await {
                Ok(r) => (r.status, r.body),
                Err(e) => (502, Some(serde_json::Value::String(e.to_string()))),
            },
            Err((s, m)) => (s, Some(serde_json::Value::String(m))),
        }
    }

    /// Stream-path forward of a `failJob` to the peer owning the job. See
    /// [`forward_complete_job_stream`].
    pub(crate) async fn forward_fail_job_stream(
        &self,
        node: u32,
        job_key: u64,
        retries: i32,
        error_message: String,
    ) -> (u16, Option<serde_json::Value>) {
        match self.peer_link(node).await {
            Ok(link) => match link.fail_job(job_key.to_string(), retries, error_message).await {
                Ok(r) => (r.status, r.body),
                Err(e) => (502, Some(serde_json::Value::String(e.to_string()))),
            },
            Err((s, m)) => (s, Some(serde_json::Value::String(m))),
        }
    }

    /// Stream-path forward of a `throwError` to the peer owning the job. See
    /// [`forward_complete_job_stream`].
    pub(crate) async fn forward_throw_error_stream(
        &self,
        node: u32,
        job_key: u64,
        error_code: String,
        error_message: String,
    ) -> (u16, Option<serde_json::Value>) {
        match self.peer_link(node).await {
            Ok(link) => match link.throw_error(job_key.to_string(), error_code, error_message).await {
                Ok(r) => (r.status, r.body),
                Err(e) => (502, Some(serde_json::Value::String(e.to_string()))),
            },
            Err((s, m)) => (s, Some(serde_json::Value::String(m))),
        }
    }

    /// Stream-path activation pull from a peer: asks `node` to activate up to
    /// `max_jobs` of `job_type` on *its own* partitions for `worker`, returning the
    /// projected jobs. The peer leases them under `timeout`, so at-least-once is
    /// preserved by the owner's lock (if this gateway dies before the worker
    /// completes, the lease expires and the job re-activates on the owner). Empty on
    /// any error — the dispatcher simply moves on. Drives job aggregation: a worker
    /// attached to one gateway draws jobs from every node's partitions.
    pub(crate) async fn activate_from_peer(
        &self,
        node: u32,
        job_type: &str,
        worker: &str,
        max_jobs: usize,
        timeout: u64,
        fetch_variable: Option<&[String]>,
    ) -> Vec<models::ActivatedJobResult> {
        let link = match self.peer_link(node).await {
            Ok(l) => l,
            Err(_) => return Vec::new(),
        };
        let res = link
            .activate_jobs(
                job_type.to_string(),
                worker.to_string(),
                max_jobs as i64,
                timeout,
                fetch_variable.map(|f| f.to_vec()),
            )
            .await;
        match res {
            Ok(r) if is_ok_status(r.status) => r
                .body
                .and_then(|b| serde_json::from_value::<Vec<models::ActivatedJobResult>>(b).ok())
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    /// Forwards a `cancelProcessInstance` to the peer owning the instance.
    async fn forward_cancel_instance(
        &self,
        node: u32,
        instance_key: u64,
    ) -> apis::process_instance::CancelProcessInstanceResponse {
        use apis::process_instance::CancelProcessInstanceResponse as Resp;
        let res = match self.peer_link(node).await {
            Ok(link) => link.cancel_instance(instance_key.to_string()).await,
            Err((s, m)) => return Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem("Peer error", s, m)),
        };
        match res {
            Ok(r) if is_ok_status(r.status) => Resp::Status204_TheProcessInstanceIsCanceled,
            Ok(r) if r.status == 404 => Resp::Status404_TheProcessInstanceIsNotFound(problem(
                "Process instance not found",
                404,
                peer_detail(&r),
            )),
            Ok(r) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                "Peer error",
                500,
                peer_detail(&r),
            )),
            Err(e) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                "Peer error",
                502,
                e.to_string(),
            )),
        }
    }

    /// Forwards a job-retries update to the peer owning the job.
    async fn forward_update_job(
        &self,
        node: u32,
        job_key: u64,
        retries: i32,
    ) -> apis::job::UpdateJobResponse {
        use apis::job::UpdateJobResponse as Resp;
        let res = match self.peer_link(node).await {
            Ok(link) => link.update_job_retries(job_key.to_string(), retries).await,
            Err((s, m)) => return Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem("Peer error", s, m)),
        };
        match res {
            Ok(r) if is_ok_status(r.status) => Resp::Status204_TheJobWasUpdatedSuccessfully,
            Ok(r) if r.status == 404 => Resp::Status404_TheJobWithTheJobKeyIsNotFound(problem(
                "Job not found",
                404,
                peer_detail(&r),
            )),
            Ok(r) if r.status == 409 => {
                Resp::Status409_TheJobWithTheGivenKeyIsInTheWrongStateCurrently(problem(
                    "Job in wrong state",
                    409,
                    peer_detail(&r),
                ))
            }
            Ok(r) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                "Peer error",
                500,
                peer_detail(&r),
            )),
            Err(e) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                "Peer error",
                502,
                e.to_string(),
            )),
        }
    }

    /// Forwards an incident resolution to the peer owning the incident.
    async fn forward_resolve_incident(
        &self,
        node: u32,
        incident_key: u64,
        operation_reference: Option<i64>,
    ) -> apis::incident::ResolveIncidentResponse {
        use apis::incident::ResolveIncidentResponse as Resp;
        let res = match self.peer_link(node).await {
            Ok(link) => link.resolve_incident(incident_key.to_string(), operation_reference).await,
            Err((s, m)) => return Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem("Peer error", s, m)),
        };
        match res {
            Ok(r) if is_ok_status(r.status) => Resp::Status204_TheIncidentIsMarkedAsResolved,
            Ok(r) if r.status == 404 => Resp::Status404_TheIncidentWithTheIncidentKeyIsNotFound(
                problem("Incident not found", 404, peer_detail(&r)),
            ),
            Ok(r) if r.status == 409 => {
                Resp::Status409_TheIncidentCannotBeResolvedDueToAnInvalidState(problem(
                    "Incident not resolvable",
                    409,
                    peer_detail(&r),
                ))
            }
            Ok(r) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                "Peer error",
                500,
                peer_detail(&r),
            )),
            Err(e) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                "Peer error",
                502,
                e.to_string(),
            )),
        }
    }

    /// Forwards a by-key variable merge to the peer owning the scope.
    async fn forward_set_variables(
        &self,
        node: u32,
        scope_key: u64,
        variables: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> apis::element_instance::CreateElementInstanceVariablesResponse {
        use apis::element_instance::CreateElementInstanceVariablesResponse as Resp;
        let res = match self.peer_link(node).await {
            Ok(link) => link.set_variables(scope_key.to_string(), variables).await,
            Err((s, m)) => return Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem("Peer error", s, m)),
        };
        match res {
            Ok(r) if is_ok_status(r.status) => Resp::Status204_TheVariablesWereUpdated,
            Ok(r) if r.status == 400 => Resp::Status400_TheProvidedDataIsNotValid(problem(
                "Scope not found",
                400,
                peer_detail(&r),
            )),
            Ok(r) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                "Peer error",
                500,
                peer_detail(&r),
            )),
            Err(e) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                "Peer error",
                502,
                e.to_string(),
            )),
        }
    }

    /// Peer-side handler for a forwarded `createProcessInstance`: creates on one
    /// of THIS node's own partitions (never re-forwarding) and returns the full
    /// `CreateProcessInstanceResult` as JSON, so the originating gateway can map
    /// it straight back to its REST response. Mirrors the local REST create core
    /// + finalize. Backpressure/admission are applied at the receiving gateway,
    /// not here.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_forwarded(
        &self,
        by_id: Option<String>,
        by_key: Option<String>,
        variables: std::collections::HashMap<String, Value>,
        tags: Vec<String>,
        business_id: Option<String>,
        await_completion: bool,
        fetch_variables: Option<Vec<String>>,
        request_timeout: Option<i64>,
    ) -> Result<serde_json::Value, (u16, String)> {
        let tags_for_response = tags.clone();
        let business_id_for_response = business_id.clone();
        type CreateOk = (String, i32, String, u64, bool, Vec<Event>, Commit);
        let outcome: Result<CreateOk, (u16, String)> = {
            let _processing = ProcessingGuard::enter(&self.processing);
            self.engine
                .for_create()
                .with_low(move |engine| {
                    let process_id = match (by_id, by_key) {
                        (Some(id), _) => id,
                        (None, Some(requested)) => match engine
                            .state()
                            .processes
                            .values()
                            .find(|d| d.key.to_string() == requested)
                        {
                            Some(d) => d.definition.id.clone(),
                            None => {
                                return Err((
                                    400,
                                    format!("No deployed process with key '{requested}'."),
                                ));
                            }
                        },
                        (None, None) => {
                            return Err((
                                400,
                                "A processDefinitionId or processDefinitionKey is required."
                                    .to_string(),
                            ));
                        }
                    };
                    match engine.apply_command_at(
                        Command::create_instance_full(
                            process_id.clone(),
                            variables,
                            tags,
                            business_id,
                        ),
                        now_millis(),
                    ) {
                        Ok((events, commit)) => {
                            let instance_key = events
                                .iter()
                                .find_map(Event::instance_key)
                                .expect("created instance has a key");
                            let (definition_key, version) = engine
                                .state()
                                .processes
                                .get(&process_id)
                                .map(|d| (d.key.to_string(), d.version))
                                .unwrap_or_else(|| (process_id.clone(), 1));
                            let sync_completed = engine.engine().is_completed(instance_key);
                            let routable: Vec<Event> = if engine.engine().num_partitions() > 1 {
                                events
                                    .iter()
                                    .filter(|e| {
                                        matches!(
                                            e,
                                            Event::MessageSubscriptionOpening { .. }
                                                | Event::RemoteMessageCorrelation { .. }
                                                | Event::MessageSubscriptionClosing { .. }
                                                | Event::StartInstanceDispatched { .. }
                                        )
                                    })
                                    .cloned()
                                    .collect()
                            } else {
                                Vec::new()
                            };
                            Ok((
                                process_id,
                                version,
                                definition_key,
                                instance_key,
                                sync_completed,
                                routable,
                                commit,
                            ))
                        }
                        Err(EngineError::ProcessNotFound { process_id }) => {
                            Err((400, format!("No deployed process with id '{process_id}'.")))
                        }
                        Err(e) => Err((500, e.to_string())),
                    }
                })
                .await
        };
        let (process_id, version, definition_key, instance_key, sync_completed, routable, commit) =
            outcome?;
        crate::metrics::record_create("rest");
        commit.wait().await;
        if !routable.is_empty() {
            self.drive_subscription_routing(routable).await;
        }
        self.signal_jobs_available();
        let (variables_out, process_completed) = if await_completion {
            self.await_process_completion(instance_key, fetch_variables.as_ref(), request_timeout)
                .await
        } else {
            (std::collections::HashMap::new(), sync_completed)
        };
        let result = models::CreateProcessInstanceResult::new(
            process_id,
            version,
            "<default>".to_string(),
            variables_out,
            models::ProcessDefinitionKey(definition_key),
            models::ProcessInstanceKey(instance_key.to_string()),
            tags_for_response.into_iter().map(models::Tag).collect(),
            business_id_for_response
                .map(nanobpm_gateway_rest::types::Nullable::Present)
                .unwrap_or(nanobpm_gateway_rest::types::Nullable::Null),
            process_completed,
        );
        serde_json::to_value(&result).map_err(|e| (500, e.to_string()))
    }

    /// Gateway side of cluster-wide create placement: forwards a
    /// `createProcessInstance` to peer `node` and maps its answer to the REST
    /// response. Used when this gateway's round-robin placement lands on a
    /// partition owned by another node.
    #[allow(clippy::too_many_arguments)]
    async fn forward_create(
        &self,
        node: u32,
        by_id: Option<String>,
        by_key: Option<String>,
        variables: Option<serde_json::Map<String, serde_json::Value>>,
        tags: Vec<String>,
        business_id: Option<String>,
        await_completion: bool,
        fetch_variables: Option<Vec<String>>,
        request_timeout: Option<i64>,
    ) -> apis::process_instance::CreateProcessInstanceResponse {
        use apis::process_instance::CreateProcessInstanceResponse as Resp;
        let res = match self.peer_link(node).await {
            Ok(link) => {
                link.forward_create(
                    by_id,
                    by_key,
                    variables,
                    tags,
                    business_id,
                    await_completion,
                    fetch_variables,
                    request_timeout,
                )
                .await
            }
            Err((s, m)) => {
                return Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Peer error",
                    s,
                    m,
                ));
            }
        };
        match res {
            Ok(r) if is_ok_status(r.status) => {
                match r
                    .body
                    .and_then(|b| serde_json::from_value::<models::CreateProcessInstanceResult>(b).ok())
                {
                    Some(result) => Resp::Status200_TheProcessInstanceWasCreated(result),
                    None => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                        problem("Peer error", 500, "peer returned an unparseable create result".into()),
                    ),
                }
            }
            Ok(r) if r.status == 400 => Resp::Status400_TheProvidedDataIsNotValid(problem(
                "Invalid create",
                400,
                peer_detail(&r),
            )),
            Ok(r) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                "Peer error",
                500,
                peer_detail(&r),
            )),
            Err(e) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                "Peer error",
                502,
                e.to_string(),
            )),
        }
    }

    /// Publishes a message and correlates it to any matching open subscriptions.
    /// nanobpmn does not buffer messages (no TTL/dedup): the message is minted,
    /// correlated to every matching open subscription, then dropped. Always
    /// returns 200 with the minted message key.
    async fn publish_message_impl(
        &self,
        body: &models::MessagePublicationRequest,
    ) -> Result<apis::message::PublishMessageResponse, ()> {
        use apis::message::PublishMessageResponse as Resp;

        let correlation_key = body.correlation_key.clone().unwrap_or_default();
        let variables = wire_variables(body.variables.as_ref());

        let body_name = body.name.clone();
        let (message_key, _instance) = self
            .correlate_message_cluster(body_name, correlation_key, variables)
            .await;

        // Correlation may have advanced a token onto a service task, creating a
        // new activatable job: wake any long-pollers.
        self.signal_jobs_available();

        let result = models::MessagePublicationResult::new(
            "<default>".to_string(),
            models::MessageKey(message_key.to_string()),
        );
        Ok(Resp::Status200_TheMessageWasPublished(result))
    }

    /// Correlates a message to a matching open subscription. Unlike
    /// [`Self::publish_message_impl`], returns 404 when nothing correlates, and
    /// reports the first correlated process instance.
    async fn correlate_message_impl(
        &self,
        body: &models::MessageCorrelationRequest,
    ) -> Result<apis::message::CorrelateMessageResponse, ()> {
        use apis::message::CorrelateMessageResponse as Resp;

        let correlation_key = body.correlation_key.clone().unwrap_or_default();
        let variables = wire_variables(body.variables.as_ref());

        let body_name = body.name.clone();
        let (message_key, correlated_instance) = self
            .correlate_message_cluster(body_name, correlation_key, variables)
            .await;
        // A message correlates either to an existing instance's open subscription
        // (MessageCorrelated) or, via a message start event, by creating a new
        // instance (ProcessInstanceCreated). Either way it correlated to an
        // instance; report the first matched instance key.

        match correlated_instance {
            Some(instance_key) => {
                // Correlation may have advanced a token onto a service task,
                // creating a new activatable job: wake any long-pollers.
                self.signal_jobs_available();
                let result = models::MessageCorrelationResult::new(
                    "<default>".to_string(),
                    models::MessageKey(message_key.to_string()),
                    models::ProcessInstanceKey(instance_key.to_string()),
                );
                Ok(Resp::Status200_TheMessageIsCorrelatedToOneOrMoreProcessInstances(result))
            }
            None => Ok(Resp::Status404_NotFound(problem(
                "Message not correlated",
                404,
                format!(
                    "No open subscription matched message '{}' with the given correlation key.",
                    body.name
                ),
            ))),
        }
    }

    async fn get_incident_impl(
        &self,
        path_params: &models::GetIncidentPathParams,
    ) -> Result<apis::incident::GetIncidentResponse, ()> {
        use apis::incident::GetIncidentResponse as Resp;

        let key: u64 = match path_params.incident_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_TheIncidentWithTheGivenKeyWasNotFound(
                    problem(
                        "Incident not found",
                        404,
                        format!(
                            "Incident key '{}' is not a valid key.",
                            path_params.incident_key
                        ),
                    ),
                ));
            }
        };

        let result = self.store.incident(key);
        if result.is_none() {
            if let Some(node) = self.remote_owner_of(key) {
                let (status, body) = self
                    .forward_get(node, crate::command_stream::ReadKind::Incident, key)
                    .await;
                return Ok(match (status, body) {
                    (200, Some(b)) => match serde_json::from_value(b) {
                        Ok(r) => Resp::Status200_TheIncidentIsSuccessfullyReturned(r),
                        Err(e) => {
                            Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                                problem("Peer error", 500, e.to_string()),
                            )
                        }
                    },
                    (404, _) => Resp::Status404_TheIncidentWithTheGivenKeyWasNotFound(problem(
                        "Incident not found",
                        404,
                        format!("No incident with key {key}."),
                    )),
                    (s, _) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                        problem("Peer error", 500, format!("peer node {node} returned status {s}")),
                    ),
                });
            }
        }
        match result {
            Some(incident) => Ok(Resp::Status200_TheIncidentIsSuccessfullyReturned(
                incident_result(&incident),
            )),
            None => Ok(Resp::Status404_TheIncidentWithTheGivenKeyWasNotFound(
                problem(
                    "Incident not found",
                    404,
                    format!("No incident with key {key}."),
                ),
            )),
        }
    }

    async fn search_incidents_impl(
        &self,
        body: &Option<models::IncidentSearchQuery>,
    ) -> Result<apis::incident::SearchIncidentsResponse, ()> {
        use apis::incident::SearchIncidentsResponse as Resp;

        let filter = body.as_ref().and_then(|q| q.filter.as_ref());
        let incidents = self.store.incidents();

        // Apply the filter algebra over each incident's string projections. The
        // process-definition identity is denormalized onto the row at projection
        // time, so no cross-entity lookup is needed here.
        let mut matched: Vec<&readstore::IncidentRow> = incidents
            .iter()
            .filter(|inc| match filter {
                None => true,
                Some(f) => {
                    query::match_basic_string(
                        &f.incident_key,
                        &inc.key.to_string(),
                    ) && query::match_process_instance_key(
                        &f.process_instance_key,
                        &inc.instance_key.to_string(),
                    ) && query::match_element_instance_key(
                        &f.element_instance_key,
                        &inc.element_instance_key.to_string(),
                    ) && query::match_process_definition_key(
                        &f.process_definition_key,
                        &inc.process_definition_key,
                    ) && match &f.job_key {
                        None => true,
                        some => query::match_job_key(
                            some,
                            &inc.job_key.map(|k| k.to_string()).unwrap_or_default(),
                        ),
                    } && query::match_incident_state(
                        &f.state,
                        &incident_state_enum(inc.state).to_string(),
                    ) && query::match_incident_error_type(
                        &f.error_type,
                        &incident_error_type_enum(inc.kind).to_string(),
                    ) && query::match_string(&f.element_id, &inc.element_id)
                        && query::match_string(&f.error_message, &inc.reason)
                }
            })
            .collect();

        // Sort: known fields, defaulting to incidentKey; entity key tiebreaks.
        let sort = query::sort_keys(
            body.as_ref().and_then(|q| q.sort.as_ref()),
            |r: &models::IncidentSearchQuerySortRequest| (r.field.clone(), r.order),
        );
        query::sort_items(
            &mut matched,
            &sort,
            |inc, field| match field {
                "creationTime" => query::SortVal::Num(inc.created_at_ms as i64),
                "state" => query::SortVal::Str(incident_state_enum(inc.state).to_string()),
                "errorType" => {
                    query::SortVal::Str(incident_error_type_enum(inc.kind).to_string())
                }
                "processInstanceKey" => query::SortVal::Num(inc.instance_key as i64),
                "elementId" => query::SortVal::Str(inc.element_id.clone()),
                _ => query::SortVal::Num(inc.key as i64),
            },
            |inc| inc.key,
        );

        let sorted: Vec<(u64, &readstore::IncidentRow)> =
            matched.into_iter().map(|inc| (inc.key, inc)).collect();
        let page = query::paginate(sorted, body.as_ref().and_then(|q| q.page.as_ref()));
        let items: Vec<models::IncidentResult> =
            page.items.into_iter().map(incident_result).collect();

        Ok(Resp::Status200_TheIncidentSearchResult(
            models::IncidentSearchQueryResult::new(page.response, items),
        ))
    }

    async fn search_process_instances_impl(
        &self,
        body: &Option<models::ProcessInstanceSearchQuery>,
    ) -> Result<apis::process_instance::SearchProcessInstancesResponse, ()> {
        use apis::process_instance::SearchProcessInstancesResponse as Resp;

        let filter = body.as_ref().and_then(|q| q.filter.as_ref());
        let instances = self.store.process_instances();

        let mut matched: Vec<&readstore::ProcessInstanceRow> = instances
            .iter()
            .filter(|inst| match filter {
                None => true,
                Some(f) => {
                    let state_str = process_instance_state_enum(inst.state).to_string();
                    query::match_process_instance_key(
                        &f.process_instance_key,
                        &inst.key.to_string(),
                    ) && query::match_process_definition_key(
                        &f.process_definition_key,
                        &inst.process_definition_key,
                    ) && query::match_string(&f.process_definition_id, &inst.process_definition_id)
                        && query::match_process_instance_state(&f.state, &state_str)
                        && f.has_incident.is_none_or(|want| want == inst.has_incident)
                }
            })
            .collect();

        let sort = query::sort_keys(
            body.as_ref().and_then(|q| q.sort.as_ref()),
            |r: &models::ProcessInstanceSearchQuerySortRequest| (r.field.clone(), r.order),
        );
        query::sort_items(
            &mut matched,
            &sort,
            |inst, field| match field {
                "processDefinitionId" => {
                    query::SortVal::Str(inst.process_definition_id.clone())
                }
                "processDefinitionKey" => {
                    query::SortVal::Num(inst.process_definition_key.parse().unwrap_or(0))
                }
                "state" => {
                    query::SortVal::Str(process_instance_state_enum(inst.state).to_string())
                }
                _ => query::SortVal::Num(inst.key as i64),
            },
            |inst| inst.key,
        );

        let sorted: Vec<(u64, &readstore::ProcessInstanceRow)> =
            matched.into_iter().map(|inst| (inst.key, inst)).collect();
        let page = query::paginate(sorted, body.as_ref().and_then(|q| q.page.as_ref()));
        // Build result models only for the returned page, never the whole
        // (potentially very large) matched set.
        let items: Vec<models::ProcessInstanceResult> =
            page.items.into_iter().map(process_instance_result).collect();

        Ok(Resp::Status200_TheProcessInstanceSearchResult(
            models::ProcessInstanceSearchQueryResult::new(page.response, items),
        ))
    }

    async fn search_jobs_impl(
        &self,
        body: &Option<models::JobSearchQuery>,
    ) -> Result<apis::job::SearchJobsResponse, ()> {
        use apis::job::SearchJobsResponse as Resp;

        let filter = body.as_ref().and_then(|q| q.filter.as_ref());
        let jobs = self.store.jobs();

        let mut matched: Vec<&readstore::JobRow> = jobs
            .iter()
            .filter(|job| match filter {
                None => true,
                Some(f) => {
                    query::match_job_key(&f.job_key, &job.key.to_string())
                        && query::match_process_instance_key(
                            &f.process_instance_key,
                            &job.instance_key.to_string(),
                        )
                        && query::match_process_definition_key(
                            &f.process_definition_key,
                            &job.process_definition_key,
                        )
                        && query::match_element_instance_key(
                            &f.element_instance_key,
                            &job.element_instance_key.to_string(),
                        )
                        && query::match_string(&f.r_type, &job.job_type)
                        && query::match_string(&f.element_id, &job.element_id)
                        && query::match_job_state(
                            &f.state,
                            &job_state_enum(job.state).to_string(),
                        )
                }
            })
            .collect();

        let sort = query::sort_keys(
            body.as_ref().and_then(|q| q.sort.as_ref()),
            |r: &models::JobSearchQuerySortRequest| (r.field.clone(), r.order),
        );
        query::sort_items(
            &mut matched,
            &sort,
            |job, field| match field {
                "processInstanceKey" => query::SortVal::Num(job.instance_key as i64),
                "elementId" => query::SortVal::Str(job.element_id.clone()),
                "type" => query::SortVal::Str(job.job_type.clone()),
                "state" => query::SortVal::Str(job_state_enum(job.state).to_string()),
                "retries" => query::SortVal::Num(job.retries as i64),
                _ => query::SortVal::Num(job.key as i64),
            },
            |job| job.key,
        );

        let sorted: Vec<(u64, &readstore::JobRow)> =
            matched.into_iter().map(|job| (job.key, job)).collect();
        let page = query::paginate(sorted, body.as_ref().and_then(|q| q.page.as_ref()));
        let items: Vec<models::JobSearchResult> =
            page.items.into_iter().map(job_search_result).collect();

        Ok(Resp::Status200_TheJobSearchResult(
            models::JobSearchQueryResult::new(page.response, items),
        ))
    }

    async fn search_user_tasks_impl(
        &self,
        body: &Option<models::UserTaskSearchQuery>,
    ) -> Result<apis::user_task::SearchUserTasksResponse, ()> {
        use apis::user_task::SearchUserTasksResponse as Resp;

        let filter = body.as_ref().and_then(|q| q.filter.as_ref());
        let tasks = self.store.user_tasks();

        let mut matched: Vec<&readstore::UserTaskRow> = tasks
            .iter()
            .filter(|task| match filter {
                None => true,
                Some(f) => {
                    let assignee = task.assignee.clone().unwrap_or_default();
                    f.user_task_key
                        .as_ref()
                        .is_none_or(|k| k.0 == task.key.to_string())
                        && query::match_process_instance_key(
                            &f.process_instance_key,
                            &task.instance_key.to_string(),
                        )
                        && query::match_process_definition_key(
                            &f.process_definition_key,
                            &task.process_definition_key,
                        )
                        && f.element_id.as_ref().is_none_or(|e| e == &task.element_id)
                        && query::match_string(&f.assignee, &assignee)
                        && query::match_user_task_state(
                            &f.state,
                            &user_task_state_enum(task.state).to_string(),
                        )
                }
            })
            .collect();

        let sort = query::sort_keys(
            body.as_ref().and_then(|q| q.sort.as_ref()),
            |r: &models::UserTaskSearchQuerySortRequest| (r.field.clone(), r.order),
        );
        query::sort_items(
            &mut matched,
            &sort,
            |task, field| match field {
                "processInstanceKey" => query::SortVal::Num(task.instance_key as i64),
                "elementId" => query::SortVal::Str(task.element_id.clone()),
                "state" => {
                    query::SortVal::Str(user_task_state_enum(task.state).to_string())
                }
                "creationDate" => query::SortVal::Num(task.created_at_ms as i64),
                _ => query::SortVal::Num(task.key as i64),
            },
            |task| task.key,
        );

        let sorted: Vec<(u64, &readstore::UserTaskRow)> =
            matched.into_iter().map(|task| (task.key, task)).collect();
        let page = query::paginate(sorted, body.as_ref().and_then(|q| q.page.as_ref()));
        let items: Vec<models::UserTaskResult> =
            page.items.into_iter().map(user_task_result).collect();

        Ok(Resp::Status200_TheUserTaskSearchResult(
            models::UserTaskSearchQueryResult::new(page.response, items),
        ))
    }

    async fn assign_user_task_impl(
        &self,
        path_params: &models::AssignUserTaskPathParams,
        body: &models::UserTaskAssignmentRequest,
    ) -> Result<apis::user_task::AssignUserTaskResponse, ()> {
        use apis::user_task::AssignUserTaskResponse as Resp;

        let user_task_key: u64 = match path_params.user_task_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(
                    problem(
                        "User task not found",
                        404,
                        format!(
                            "User task key '{}' is not a valid key.",
                            path_params.user_task_key
                        ),
                    ),
                ));
            }
        };

        let assignee = body.assignee.clone().unwrap_or_default();
        // Per the v2 contract, assignment overrides an existing assignee unless
        // the caller explicitly opts out with allowOverride = false.
        let allow_override = match &body.allow_override {
            Some(types::Nullable::Present(v)) => *v,
            _ => true,
        };
        if let Some(node) = self.remote_owner_of(user_task_key) {
            let payload = serde_json::to_value(body).ok();
            let (status, detail) = self
                .forward_user_task(
                    node,
                    crate::command_stream::UserTaskOp::Assign,
                    user_task_key,
                    payload,
                )
                .await;
            return Ok(match status {
                204 => Resp::Status204_TheUserTask,
                404 => Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(problem(
                    "User task not found",
                    404,
                    detail,
                )),
                409 => Resp::Status409_TheUserTaskWithTheGivenKeyIsInTheWrongStateCurrently(
                    problem("User task in wrong state", 409, detail),
                ),
                s => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Peer error",
                    500,
                    if detail.is_empty() {
                        format!("peer node {node} returned status {s}")
                    } else {
                        detail
                    },
                )),
            });
        }
        let command = Command::AssignUserTask {
            user_task_key,
            assignee,
            allow_override,
        };
        let result = self
            .engine
            .by_key(user_task_key)
            .with(move |engine| engine.apply_command_at(command, now_millis()))
            .await;
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                Ok(Resp::Status204_TheUserTask)
            }
            Err(EngineError::UserTaskNotFound { user_task_key }) => {
                Ok(Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(
                    problem(
                        "User task not found",
                        404,
                        format!("No user task with key {user_task_key}."),
                    ),
                ))
            }
            Err(EngineError::UserTaskNotActive { user_task_key }) => Ok(
                Resp::Status409_TheUserTaskWithTheGivenKeyIsInTheWrongStateCurrently(problem(
                    "User task not active",
                    409,
                    format!("User task {user_task_key} is not active and cannot be assigned."),
                )),
            ),
            Err(EngineError::UserTaskAlreadyAssigned { user_task_key }) => Ok(
                Resp::Status409_TheUserTaskWithTheGivenKeyIsInTheWrongStateCurrently(problem(
                    "User task already assigned",
                    409,
                    format!(
                        "User task {user_task_key} is already assigned; unassign it before \
                         assigning again."
                    ),
                )),
            ),
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    async fn complete_user_task_impl(
        &self,
        path_params: &models::CompleteUserTaskPathParams,
        body: &Option<models::UserTaskCompletionRequest>,
    ) -> Result<apis::user_task::CompleteUserTaskResponse, ()> {
        use apis::user_task::CompleteUserTaskResponse as Resp;

        let user_task_key: u64 = match path_params.user_task_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(
                    problem(
                        "User task not found",
                        404,
                        format!(
                            "User task key '{}' is not a valid key.",
                            path_params.user_task_key
                        ),
                    ),
                ));
            }
        };

        // Variables the human submits are merged into the instance so they can
        // drive downstream gateway routing.
        if let Some(node) = self.remote_owner_of(user_task_key) {
            let payload = serde_json::to_value(body).ok();
            let (status, detail) = self
                .forward_user_task(
                    node,
                    crate::command_stream::UserTaskOp::Complete,
                    user_task_key,
                    payload,
                )
                .await;
            return Ok(match status {
                204 => Resp::Status204_TheUserTaskWasCompletedSuccessfully,
                404 => Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(problem(
                    "User task not found",
                    404,
                    detail,
                )),
                409 => Resp::Status409_TheUserTaskWithTheGivenKeyIsInTheWrongStateCurrently(
                    problem("User task in wrong state", 409, detail),
                ),
                s => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Peer error",
                    500,
                    if detail.is_empty() {
                        format!("peer node {node} returned status {s}")
                    } else {
                        detail
                    },
                )),
            });
        }
        let variables = body
            .as_ref()
            .and_then(|b| b.variables.as_ref())
            .and_then(|v| match v {
                types::Nullable::Present(map) => Some(from_object_map(map)),
                types::Nullable::Null => None,
            })
            .unwrap_or_default();

        let command = Command::complete_user_task_with(user_task_key, variables);
        let result = self
            .engine
            .by_key(user_task_key)
            .with(move |engine| engine.apply_command_at(command, now_millis()))
            .await;
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                // Completing a user task advances the token, which may create a
                // following job: wake any long-pollers.
                self.signal_jobs_available();
                Ok(Resp::Status204_TheUserTaskWasCompletedSuccessfully)
            }
            Err(EngineError::UserTaskNotFound { user_task_key }) => {
                Ok(Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(
                    problem(
                        "User task not found",
                        404,
                        format!("No user task with key {user_task_key}."),
                    ),
                ))
            }
            Err(EngineError::UserTaskNotActive { user_task_key }) => Ok(
                Resp::Status409_TheUserTaskWithTheGivenKeyIsInTheWrongStateCurrently(problem(
                    "User task not active",
                    409,
                    format!("User task {user_task_key} is not active and cannot be completed."),
                )),
            ),
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    /// Returns a single user task from the read model by key.
    async fn get_user_task_impl(
        &self,
        path_params: &models::GetUserTaskPathParams,
    ) -> Result<apis::user_task::GetUserTaskResponse, ()> {
        use apis::user_task::GetUserTaskResponse as Resp;

        let user_task_key: u64 = match path_params.user_task_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(
                    problem(
                        "User task not found",
                        404,
                        format!(
                            "User task key '{}' is not a valid key.",
                            path_params.user_task_key
                        ),
                    ),
                ));
            }
        };

        match self
            .store
            .user_tasks()
            .iter()
            .find(|t| t.key == user_task_key)
        {
            Some(task) => Ok(Resp::Status200_TheUserTaskIsSuccessfullyReturned(
                user_task_result(task),
            )),
            None => {
                if let Some(node) = self.remote_owner_of(user_task_key) {
                    let (status, body) = self
                        .forward_get(node, crate::command_stream::ReadKind::UserTask, user_task_key)
                        .await;
                    return Ok(match (status, body) {
                        (200, Some(b)) => match serde_json::from_value(b) {
                            Ok(r) => Resp::Status200_TheUserTaskIsSuccessfullyReturned(r),
                            Err(e) => {
                                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                                    problem("Peer error", 500, e.to_string()),
                                )
                            }
                        },
                        (404, _) => Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(problem(
                            "User task not found",
                            404,
                            format!("No user task with key {user_task_key}."),
                        )),
                        (s, _) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                            problem("Peer error", 500, format!("peer node {node} returned status {s}")),
                        ),
                    });
                }
                Ok(Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(
                    problem(
                        "User task not found",
                        404,
                        format!("No user task with key {user_task_key}."),
                    ),
                ))
            }
        }
    }

    /// Clears a user task's assignee.
    async fn unassign_user_task_impl(
        &self,
        path_params: &models::UnassignUserTaskPathParams,
    ) -> Result<apis::user_task::UnassignUserTaskResponse, ()> {
        use apis::user_task::UnassignUserTaskResponse as Resp;

        let user_task_key: u64 = match path_params.user_task_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(
                    problem(
                        "User task not found",
                        404,
                        format!(
                            "User task key '{}' is not a valid key.",
                            path_params.user_task_key
                        ),
                    ),
                ));
            }
        };

        let command = Command::unassign_user_task(user_task_key);
        if let Some(node) = self.remote_owner_of(user_task_key) {
            let (status, detail) = self
                .forward_user_task(
                    node,
                    crate::command_stream::UserTaskOp::Unassign,
                    user_task_key,
                    None,
                )
                .await;
            return Ok(match status {
                204 => Resp::Status204_TheUserTaskWasUnassignedSuccessfully,
                404 => Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(problem(
                    "User task not found",
                    404,
                    detail,
                )),
                409 => Resp::Status409_TheUserTaskWithTheGivenKeyIsInTheWrongStateCurrently(
                    problem("User task in wrong state", 409, detail),
                ),
                s => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Peer error",
                    500,
                    if detail.is_empty() {
                        format!("peer node {node} returned status {s}")
                    } else {
                        detail
                    },
                )),
            });
        }
        let result = self
            .engine
            .by_key(user_task_key)
            .with(move |engine| engine.apply_command_at(command, now_millis()))
            .await;
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                Ok(Resp::Status204_TheUserTaskWasUnassignedSuccessfully)
            }
            Err(EngineError::UserTaskNotFound { user_task_key }) => {
                Ok(Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(
                    problem(
                        "User task not found",
                        404,
                        format!("No user task with key {user_task_key}."),
                    ),
                ))
            }
            Err(EngineError::UserTaskNotActive { user_task_key }) => Ok(
                Resp::Status409_TheUserTaskWithTheGivenKeyIsInTheWrongStateCurrently(problem(
                    "User task not active",
                    409,
                    format!("User task {user_task_key} is not active and cannot be unassigned."),
                )),
            ),
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    /// Updates a user task's attributes (candidate groups/users, due/follow-up
    /// date, priority) from the request changeset.
    async fn update_user_task_impl(
        &self,
        path_params: &models::UpdateUserTaskPathParams,
        body: &Option<models::UserTaskUpdateRequest>,
    ) -> Result<apis::user_task::UpdateUserTaskResponse, ()> {
        use apis::user_task::UpdateUserTaskResponse as Resp;

        let user_task_key: u64 = match path_params.user_task_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(
                    problem(
                        "User task not found",
                        404,
                        format!(
                            "User task key '{}' is not a valid key.",
                            path_params.user_task_key
                        ),
                    ),
                ));
            }
        };

        if let Some(node) = self.remote_owner_of(user_task_key) {
            let payload = serde_json::to_value(body).ok();
            let (status, detail) = self
                .forward_user_task(
                    node,
                    crate::command_stream::UserTaskOp::Update,
                    user_task_key,
                    payload,
                )
                .await;
            return Ok(match status {
                204 => Resp::Status204_TheUserTaskWasUpdatedSuccessfully,
                404 => Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(problem(
                    "User task not found",
                    404,
                    detail,
                )),
                409 => Resp::Status409_TheUserTaskWithTheGivenKeyIsInTheWrongStateCurrently(
                    problem("User task in wrong state", 409, detail),
                ),
                s => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Peer error",
                    500,
                    if detail.is_empty() {
                        format!("peer node {node} returned status {s}")
                    } else {
                        detail
                    },
                )),
            });
        }

        // Translate the REST changeset into the engine changeset. A `Present`
        // value sets the attribute; an explicit `Null` resets it (empty list /
        // cleared date); an absent field leaves it unchanged.
        let changeset = body
            .as_ref()
            .and_then(|b| b.changeset.as_ref())
            .and_then(|cs| match cs {
                types::Nullable::Present(cs) => Some(cs),
                types::Nullable::Null => None,
            });

        let to_list = |n: &Option<types::Nullable<Vec<String>>>| match n {
            Some(types::Nullable::Present(v)) => Some(v.clone()),
            Some(types::Nullable::Null) => Some(Vec::new()),
            None => None,
        };
        let to_date = |n: &Option<types::Nullable<chrono::DateTime<chrono::Utc>>>| match n {
            Some(types::Nullable::Present(dt)) => Some(Some(dt.to_rfc3339())),
            Some(types::Nullable::Null) => Some(None),
            None => None,
        };
        let to_priority = |n: &Option<types::Nullable<u8>>| match n {
            Some(types::Nullable::Present(p)) => Some(*p as i32),
            _ => None,
        };

        let engine_changeset = match changeset {
            Some(cs) => nanobpmn_engine_core::UserTaskChangeset {
                candidate_groups: to_list(&cs.candidate_groups),
                candidate_users: to_list(&cs.candidate_users),
                due_date: to_date(&cs.due_date),
                follow_up_date: to_date(&cs.follow_up_date),
                priority: to_priority(&cs.priority),
            },
            None => nanobpmn_engine_core::UserTaskChangeset::default(),
        };

        let command = Command::update_user_task(user_task_key, engine_changeset);
        let result = self
            .engine
            .by_key(user_task_key)
            .with(move |engine| engine.apply_command_at(command, now_millis()))
            .await;
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                Ok(Resp::Status204_TheUserTaskWasUpdatedSuccessfully)
            }
            Err(EngineError::UserTaskNotFound { user_task_key }) => {
                Ok(Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(
                    problem(
                        "User task not found",
                        404,
                        format!("No user task with key {user_task_key}."),
                    ),
                ))
            }
            Err(EngineError::UserTaskNotActive { user_task_key }) => Ok(
                Resp::Status409_TheUserTaskWithTheGivenKeyIsInTheWrongStateCurrently(problem(
                    "User task not active",
                    409,
                    format!("User task {user_task_key} is not active and cannot be updated."),
                )),
            ),
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    /// Searches variables in the read model. nano keeps a single instance-level
    /// scope, so every variable's `scopeKey` equals its `processInstanceKey`.
    async fn search_variables_impl(
        &self,
        query_params: &models::SearchVariablesQueryParams,
        body: &Option<models::VariableSearchQuery>,
    ) -> Result<apis::variable::SearchVariablesResponse, ()> {
        use apis::variable::SearchVariablesResponse as Resp;

        // `truncateValues` defaults to true: long values are truncated and the
        // result flags `isTruncated`.
        let truncate = query_params.truncate_values.unwrap_or(true);
        let filter = body.as_ref().and_then(|q| q.filter.as_ref());
        let vars = self.store.variables();

        let mut matched: Vec<&readstore::VariableRow> = vars
            .iter()
            .filter(|v| match filter {
                None => true,
                Some(f) => {
                    let tenant_ok =
                        f.tenant_id.as_ref().is_none_or(|t| t == "<default>");
                    let truncated = value_is_truncated(&v.value, truncate);
                    tenant_ok
                        && query::match_string(&f.name, &v.name)
                        && query::match_string(&f.value, &v.value)
                        && query::match_variable_key(&f.variable_key, &v.key.to_string())
                        && query::match_scope_key(&f.scope_key, &v.scope_key.to_string())
                        && query::match_process_instance_key(
                            &f.process_instance_key,
                            &v.instance_key.to_string(),
                        )
                        && f.is_truncated.is_none_or(|want| want == truncated)
                }
            })
            .collect();

        let sort = query::sort_keys(
            body.as_ref().and_then(|q| q.sort.as_ref()),
            |r: &models::VariableSearchQuerySortRequest| (r.field.clone(), r.order),
        );
        query::sort_items(
            &mut matched,
            &sort,
            |v, field| match field {
                "name" => query::SortVal::Str(v.name.clone()),
                "value" => query::SortVal::Str(v.value.clone()),
                "tenantId" => query::SortVal::Str("<default>".to_string()),
                "scopeKey" => query::SortVal::Num(v.scope_key as i64),
                "processInstanceKey" => query::SortVal::Num(v.instance_key as i64),
                _ => query::SortVal::Num(v.key as i64),
            },
            |v| v.key,
        );

        let sorted: Vec<(u64, &readstore::VariableRow)> =
            matched.into_iter().map(|v| (v.key, v)).collect();
        let page = query::paginate(sorted, body.as_ref().and_then(|q| q.page.as_ref()));
        let items: Vec<models::VariableSearchResult> = page
            .items
            .into_iter()
            .map(|v| variable_search_result(v, truncate))
            .collect();

        Ok(Resp::Status200_TheVariableSearchResult(
            models::VariableSearchQueryResult::new(page.response, items),
        ))
    }

    /// Returns a single variable by its (read-model assigned) key, with its full
    /// untruncated value.
    async fn get_variable_impl(
        &self,
        path_params: &models::GetVariablePathParams,
    ) -> Result<apis::variable::GetVariableResponse, ()> {
        use apis::variable::GetVariableResponse as Resp;

        let key: u64 = match path_params.variable_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_NotFound(problem(
                    "Variable not found",
                    404,
                    format!(
                        "Variable key '{}' is not a valid key.",
                        path_params.variable_key
                    ),
                )));
            }
        };

        match self.store.variable(key) {
            Some(v) => Ok(Resp::Status200_TheVariableIsSuccessfullyReturned(
                variable_result(&v),
            )),
            None => {
                if let Some(node) = self.remote_owner_of(key) {
                    let (status, body) = self
                        .forward_get(node, crate::command_stream::ReadKind::Variable, key)
                        .await;
                    return Ok(match (status, body) {
                        (200, Some(b)) => match serde_json::from_value(b) {
                            Ok(r) => Resp::Status200_TheVariableIsSuccessfullyReturned(r),
                            Err(e) => {
                                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                                    problem("Peer error", 500, e.to_string()),
                                )
                            }
                        },
                        (404, _) => Resp::Status404_NotFound(problem(
                            "Variable not found",
                            404,
                            format!("No variable with key {key}."),
                        )),
                        (s, _) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                            problem("Peer error", 500, format!("peer node {node} returned status {s}")),
                        ),
                    });
                }
                Ok(Resp::Status404_NotFound(problem(
                    "Variable not found",
                    404,
                    format!("No variable with key {key}."),
                )))
            }
        }
    }

    /// Searches deployed process definitions. The engine keeps only the latest
    /// version of each process id (in `state.processes`), so every entry is the
    /// latest version; older versions are not retained and therefore not
    /// searchable.
    async fn search_process_definitions_impl(
        &self,
        body: &Option<models::ProcessDefinitionSearchQuery>,
    ) -> Result<apis::process_definition::SearchProcessDefinitionsResponse, ()> {
        use apis::process_definition::SearchProcessDefinitionsResponse as Resp;

        let filter = body.as_ref().and_then(|q| q.filter.as_ref());
        let definitions = self.store.process_definitions();

        let mut matched: Vec<&readstore::ProcessDefinitionRow> = definitions
            .iter()
            .filter(|d| match filter {
                None => true,
                Some(f) => {
                    let id = &d.process_id;
                    // The engine stores no display name, resource name, or
                    // version tag, so those filters match against the best
                    // available proxy (the id) or exclude when we hold no value.
                    query::match_string(&f.name, id)
                        && query::match_string(&f.process_definition_id, id)
                        && f.process_definition_key
                            .as_ref()
                            .is_none_or(|k| k.0 == d.key.to_string())
                        && f.version.is_none_or(|v| v == d.version)
                        && f.resource_name
                            .as_ref()
                            .is_none_or(|r| *r == resource_name(id))
                        && f.version_tag.is_none()
                        && f.has_start_form.is_none_or(|want| !want)
                        // Only latest versions are retained, so they are all
                        // "latest"; an explicit `false` therefore matches none.
                        && f.is_latest_version.is_none_or(|want| want)
                }
            })
            .collect();

        let sort = query::sort_keys(
            body.as_ref().and_then(|q| q.sort.as_ref()),
            |r: &models::ProcessDefinitionSearchQuerySortRequest| (r.field.clone(), r.order),
        );
        query::sort_items(
            &mut matched,
            &sort,
            |d, field| match field {
                "processDefinitionId" | "name" => {
                    query::SortVal::Str(d.process_id.clone())
                }
                "version" => query::SortVal::Num(d.version as i64),
                _ => query::SortVal::Num(d.key as i64),
            },
            |d| d.key,
        );

        let sorted: Vec<(u64, &readstore::ProcessDefinitionRow)> =
            matched.into_iter().map(|d| (d.key, d)).collect();
        let page = query::paginate(sorted, body.as_ref().and_then(|q| q.page.as_ref()));
        let items: Vec<models::ProcessDefinitionResult> = page
            .items
            .into_iter()
            .map(process_definition_result)
            .collect();

        Ok(Resp::Status200_TheProcessDefinitionSearchResult(
            models::ProcessDefinitionSearchQueryResult::new(page.response, items),
        ))
    }

    async fn create_deployment_impl(
        &self,
        mut body: Multipart,
    ) -> Result<apis::resource::CreateDeploymentResponse, ()> {
        use apis::resource::CreateDeploymentResponse as Resp;

        // Drain the multipart body first: reading fields is async and we must not
        // hold the engine lock across an `.await`.
        let mut resources: Vec<(String, String)> = Vec::new();
        let mut tenant_id = "<default>".to_string();
        loop {
            match body.next_field().await {
                Ok(Some(field)) => {
                    let name = field.name().unwrap_or_default().to_string();
                    let file_name = field.file_name().map(str::to_string);
                    match field.bytes().await {
                        Ok(bytes) => {
                            if name == "tenantId" {
                                if let Ok(text) = std::str::from_utf8(&bytes) {
                                    let text = text.trim();
                                    if !text.is_empty() {
                                        tenant_id = text.to_string();
                                    }
                                }
                            } else {
                                match String::from_utf8(bytes.to_vec()) {
                                    Ok(xml) => {
                                        let resource_name = file_name.unwrap_or_else(|| {
                                            format!("resource-{}.bpmn", resources.len())
                                        });
                                        resources.push((resource_name, xml));
                                    }
                                    Err(_) => {
                                        return Ok(Resp::Status400_TheProvidedDataIsNotValid(
                                            problem(
                                                "Invalid resource",
                                                400,
                                                "A deployment resource was not valid UTF-8 BPMN XML.".to_string(),
                                            ),
                                        ));
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            return Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                                "Invalid request",
                                400,
                                format!("Could not read a deployment resource: {e}."),
                            )));
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    return Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                        "Invalid request",
                        400,
                        format!("Malformed multipart request: {e}."),
                    )));
                }
            }
        }

        if resources.is_empty() {
            return Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                "No resources",
                400,
                "At least one deployment resource is required.".to_string(),
            )));
        }

        // Centralized cluster deployment: the partition-0 owner is the deploy
        // authority. If this gateway owns partition 0, deploy locally (durable)
        // and broadcast the result to every peer so all nodes can instantiate the
        // process. Otherwise forward the whole deploy to the owner over the
        // command stream and return its answer. Single-node always owns
        // partition 0, so this is the unchanged local path.
        if self.engine.topology().is_local(0) {
            let (processes, resource_names) = match parse_deploy_resources(&resources) {
                Ok(parsed) => parsed,
                Err((title, detail)) => {
                    return Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                        title, 400, detail,
                    )));
                }
            };
            match self
                .deploy_resources_locally(processes, &resource_names, &tenant_id)
                .await
            {
                Ok((result, events)) => {
                    self.broadcast_deployment(&events).await;
                    Ok(Resp::Status200_TheResourcesAreDeployed(result))
                }
                Err((title, detail)) => Ok(Resp::Status400_TheProvidedDataIsNotValid(
                    problem(title, 400, detail),
                )),
            }
        } else {
            match self.forward_deploy(resources, tenant_id).await {
                Ok(result) => Ok(Resp::Status200_TheResourcesAreDeployed(result)),
                Err((status, detail)) => Ok(Resp::Status400_TheProvidedDataIsNotValid(
                    problem("Deployment failed", status, detail),
                )),
            }
        }
    }

    /// Deploys already-parsed `processes` on this node's deployment partition
    /// (durable) and replicates the definition(s) in-memory to its other owned
    /// partitions. Returns the typed deployment result and the minted
    /// `ProcessDeployed` events (for cross-node broadcast). The caller must be the
    /// partition-0 owner. `Err` carries `(title, detail)` for a 400.
    async fn deploy_resources_locally(
        &self,
        processes: Vec<ProcessDefinition>,
        resource_names: &std::collections::HashMap<String, String>,
        tenant_id: &str,
    ) -> Result<(models::DeploymentResult, Arc<Vec<Event>>), (&'static str, String)> {
        let deploy_result: Result<(Arc<Vec<Event>>, Commit), String> = self
            .engine
            .deploy_partition()
            .with(move |engine| {
                engine
                    .apply_command(Command::DeployResources(processes))
                    .map_err(|e| e.to_string())
            })
            .await;
        let (events, commit) = match deploy_result {
            Ok(pair) => pair,
            Err(e) => return Err(("Invalid deployment", e)),
        };
        // Replicate the new definition(s) to the other local partitions so any of
        // them can instantiate the process (the deployment itself is journaled
        // only on partition 0; replication is in-memory and re-derived on restart).
        self.replicate_deployment(&events).await;
        // Under Raft (RF>1) also fan into any follower replica engine actors so a
        // replicated create of this definition applies on every replica.
        self.install_into_raft_replicas(&events).await;

        let mut deployment_key = String::new();
        let mut deployments = Vec::new();
        for event in events.iter() {
            if let Event::ProcessDeployed {
                deployment_key: dk,
                process_definition_key,
                version,
                process,
            } = event
            {
                deployment_key = dk.to_string();
                let resource_name = resource_names.get(&process.id).cloned().unwrap_or_default();
                let process_result = models::DeploymentProcessResult::new(
                    process.id.clone(),
                    *version,
                    resource_name,
                    tenant_id.to_string(),
                    models::ProcessDefinitionKey(process_definition_key.to_string()),
                );
                deployments.push(models::DeploymentMetadataResult::new(
                    nanobpm_gateway_rest::types::Nullable::Present(process_result),
                    nanobpm_gateway_rest::types::Nullable::Null,
                    nanobpm_gateway_rest::types::Nullable::Null,
                    nanobpm_gateway_rest::types::Nullable::Null,
                    nanobpm_gateway_rest::types::Nullable::Null,
                ));
            }
        }

        let result = models::DeploymentResult::new(
            models::DeploymentKey(deployment_key),
            tenant_id.to_string(),
            deployments,
        );
        // Deployments don't create jobs, but a freshly available process means a
        // later createProcessInstance can; nothing to notify here.
        commit.wait().await;
        Ok((result, events))
    }

    /// Runs a forwarded deploy on the partition-0 owner: parses, deploys locally,
    /// broadcasts to peers, and returns the deployment JSON. Invoked by the
    /// `Deploy` command-stream frame handler. `Err` is `(status, detail)`.
    pub async fn deploy_centralized(
        &self,
        resources: Vec<(String, String)>,
        tenant_id: String,
    ) -> Result<serde_json::Value, (u16, String)> {
        let (processes, resource_names) =
            parse_deploy_resources(&resources).map_err(|(_, detail)| (400u16, detail))?;
        let (result, events) = self
            .deploy_resources_locally(processes, &resource_names, &tenant_id)
            .await
            .map_err(|(_, detail)| (400u16, detail))?;
        self.broadcast_deployment(&events).await;
        Ok(serde_json::to_value(result).expect("deployment result serializes"))
    }

    /// Durably installs a deployment broadcast from the partition-0 owner onto
    /// this node's owned partitions: a single durable copy (on the first owned
    /// partition) plus an in-memory copy on the rest. The restart demux replays
    /// the durable `ProcessDeployed` into every owned partition, so the
    /// definition survives this node's independent restart on all of them.
    /// Invoked by the `InstallDeployment` command-stream frame handler.
    pub async fn install_replicated_deployment(&self, events: Vec<Event>) {
        let handles = self.engine.all();
        let events = Arc::new(events);
        let durable = {
            let events = Arc::clone(&events);
            handles[0]
                .with(move |journal| journal.install_deployment_durable(&events))
                .await
        };
        for handle in handles.iter().skip(1) {
            let events = Arc::clone(&events);
            handle
                .with(move |journal| journal.install_deployment(&events))
                .await;
        }
        // Under Raft (RF>1) also fan into any follower replica engine actors.
        self.install_into_raft_replicas(&events).await;
        durable.wait().await;
    }

    /// Forwards a deploy to the partition-0 owner over the command stream and
    /// decodes its deployment JSON. Used when a gateway that does not own
    /// partition 0 receives an HTTP deploy. `Err` is `(status, detail)`.
    async fn forward_deploy(
        &self,
        resources: Vec<(String, String)>,
        tenant_id: String,
    ) -> Result<models::DeploymentResult, (u16, String)> {
        let owner = self.engine.topology().owner_of(0);
        let link = self
            .peers
            .link(owner)
            .await
            .map_err(|e| (502u16, format!("deploy-partition owner unreachable: {e}")))?;
        let res = link
            .deploy(resources, Some(tenant_id))
            .await
            .map_err(|e| (502u16, format!("deploy forward failed: {e}")))?;
        if res.status != 200 {
            let detail = res
                .body
                .as_ref()
                .and_then(|b| b.as_str().map(str::to_string))
                .unwrap_or_else(|| "deployment rejected by owner".to_string());
            return Err((res.status, detail));
        }
        let body = res
            .body
            .ok_or((502u16, "owner returned no deployment body".to_string()))?;
        serde_json::from_value(body)
            .map_err(|e| (502u16, format!("malformed deployment response: {e}")))
    }

    /// Broadcasts an already-minted deployment's `ProcessDeployed` events to every
    /// peer so each can durably install the definition(s). Best-effort: a peer
    /// that is briefly unreachable is logged and skipped (a runtime deploy assumes
    /// the cluster is up; the startup seed broadcast retries until acknowledged).
    /// A no-op for a single-node cluster.
    async fn broadcast_deployment(&self, events: &Arc<Vec<Event>>) {
        if !self.peers.has_peers() {
            return;
        }
        let deployed: Vec<Event> = events
            .iter()
            .filter(|e| matches!(e, Event::ProcessDeployed { .. }))
            .cloned()
            .collect();
        if deployed.is_empty() {
            return;
        }
        let topology = self.engine.topology();
        for node in 0..topology.num_nodes() {
            if node == topology.node_id {
                continue;
            }
            match self.peers.link(node).await {
                Ok(link) => {
                    if let Err(e) = link.install_deployment(deployed.clone()).await {
                        tracing::warn!("deploy broadcast to node {node} failed: {e}");
                    }
                }
                Err(e) => tracing::warn!("deploy broadcast: node {node} unreachable: {e}"),
            }
        }
    }

    /// Spawns a background task — on the partition-0 owner of a clustered node —
    /// that broadcasts this node's current deployment definitions to every peer,
    /// retrying each until it acknowledges. Run at startup so peers receive the
    /// seeded/recovered definitions even when they boot after this node (the
    /// runtime `broadcast_deployment` is best-effort/single-shot and assumes the
    /// cluster is already up). A no-op for a single-node cluster or a non-owner.
    fn spawn_seed_broadcast(&self) {
        if !self.peers.has_peers() || !self.engine.topology().is_local(0) {
            return;
        }
        let server = self.clone();
        tokio::spawn(async move {
            let events: Vec<Event> = server
                .engine
                .deploy_partition()
                .with(|journal| deployment_replication_events(journal))
                .await;
            if events.is_empty() {
                return;
            }
            let topology = server.engine.topology().clone();
            for node in 0..topology.num_nodes() {
                if node == topology.node_id {
                    continue;
                }
                loop {
                    match server.peers.link(node).await {
                        Ok(link) => match link.install_deployment(events.clone()).await {
                            Ok(_) => {
                                tracing::info!(
                                    "seed deployment broadcast to node {node} acknowledged"
                                );
                                break;
                            }
                            Err(e) => {
                                tracing::warn!("seed broadcast to node {node} failed: {e}; retrying")
                            }
                        },
                        Err(e) => tracing::debug!(
                            "seed broadcast: node {node} not yet reachable: {e}; retrying"
                        ),
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                }
            }
        });
    }

    /// Env-gated (`NANOBPMN_RAFT`) per-partition Raft bootstrap. For every
    /// partition this node replicates, host a Raft group member whose RPCs ride
    /// the command stream; then, for the partitions this node leads, form the
    /// group from its replica set. A no-op unless `NANOBPMN_RAFT` is set, so the
    /// default single-writer path is untouched.
    ///
    /// Runs as a background task because the local command-stream endpoint isn't
    /// serving until `main()` calls `axum::serve`, and peers may still be booting.
    /// A member that can't yet be reached is simply retried by openraft's network
    /// (an unreachable peer slows the group, never loses an entry), and the
    /// leader's `initialize` is idempotent (skipped on an already-formed group),
    /// so the multi-process startup race is tolerated rather than coordinated.
    fn spawn_raft_bootstrap(&self) {
        if !raft_enabled() {
            return;
        }
        let server = self.clone();
        tokio::spawn(async move {
            server.raft_bootstrap().await;
        });
    }

    /// The body of [`Self::spawn_raft_bootstrap`], factored out so tests can drive
    /// it directly (without the env gate or a detached task). Hosts a Raft member
    /// for every partition this node replicates, then forms the groups it leads.
    async fn raft_bootstrap(&self) {
        let server = self;
        {
            let topology = server.engine.topology().clone();
            let transport = server.raft_transport();

            // Host a member for every partition this node replicates. For a
            // partition this node OWNS, the Raft state machine drives the SAME
            // engine actor the rest of the server reads/dispatches/times from
            // (leader path) — log and served state share one materialized copy.
            // For a partition this node replicates but does NOT own (a follower
            // under RF>1), there is no owned actor, so we build a dedicated
            // replica engine actor here (seeded with the current deployments) for
            // the state machine to apply the replicated log into.
            for p in topology.replica_partitions() {
                let engine = match server.engine.local_for_partition(p) {
                    Some(owned) => owned.clone(),
                    None => server.replica_engine_for(p).await,
                };
                match crate::raft::RaftPartition::bootstrap_member(
                    topology.node_id as u64,
                    p,
                    engine,
                    transport.clone(),
                )
                .await
                {
                    Ok(part) => {
                        server.raft_registry().insert(Arc::new(part));
                        tracing::info!(
                            "raft: node {} hosting a member for partition {p}",
                            topology.node_id
                        );
                    }
                    Err(e) => {
                        tracing::error!("raft: failed to host partition {p}: {e}");
                    }
                }
            }

            // Form each group this node leads from its replica set. `initialize`
            // is idempotent and does not require peers to be up (they catch up via
            // replication), but we retry to ride out a transient failure.
            for p in topology.replica_partitions() {
                if topology.leader_of(p) != topology.node_id {
                    continue;
                }
                let Some(part) = server.raft_registry().get(p) else {
                    continue;
                };
                let members: std::collections::BTreeMap<u64, openraft::BasicNode> = topology
                    .replicas_of(p)
                    .into_iter()
                    .map(|n| {
                        let addr = topology.peer_addr(n).unwrap_or("").to_string();
                        (n as u64, openraft::BasicNode::new(addr))
                    })
                    .collect();
                loop {
                    match part.initialize(members.clone()).await {
                        Ok(()) => {
                            tracing::info!(
                                "raft: node {} formed the group for partition {p} (members {:?})",
                                topology.node_id,
                                topology.replicas_of(p)
                            );
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(
                                "raft: initialize partition {p} failed: {e}; retrying"
                            );
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                }
            }
        }
    }

    /// Replicates a deployment's process definitions to every partition other
    /// than the deployment partition (0), so a `createProcessInstance` routed to
    /// any partition finds the definition. A no-op for a single partition. Only
    /// the `ProcessDeployed` events are installed (start subscriptions/timers stay
    /// owned by partition 0); `install_deployment` filters the rest.
    async fn replicate_deployment(&self, events: &Arc<Vec<Event>>) {
        if self.engine.is_single() {
            return;
        }
        for handle in self.engine.all().iter().skip(1) {
            let events = Arc::clone(events);
            handle
                .with(move |journal| journal.install_deployment(&events))
                .await;
        }
    }

    /// The `ProcessDeployed` events for every definition currently known to this
    /// node, read from one of its owned engine actors. Used to seed a freshly
    /// built replica engine actor (a follower partition under RF>1) so it can
    /// apply `CreateInstance` for already-deployed processes.
    async fn current_deployment_events(&self) -> Vec<Event> {
        let handles = self.engine.all();
        if handles.is_empty() {
            return Vec::new();
        }
        handles[0]
            .with(|journal| deployment_replication_events(journal))
            .await
    }

    /// Returns (building if necessary) the dedicated engine actor for a partition
    /// this node **replicates but does not own**. The Raft state machine drives it
    /// to apply the replicated log on a follower; it is not part of the read-model
    /// / serving path. Seeded with the current deployments so it can apply creates
    /// of already-deployed processes; later deploys fan in via
    /// [`Self::install_into_raft_replicas`].
    async fn replica_engine_for(&self, p: u64) -> EngineHandle {
        if let Some(h) = self.raft_replicas.lock().unwrap().get(&p) {
            return h.clone();
        }
        let mut journal = Journal::in_memory_partition(p);
        journal.set_num_partitions(self.engine.topology().num_partitions);
        let seed = self.current_deployment_events().await;
        if !seed.is_empty() {
            journal.install_deployment(&seed);
        }
        let handle = EngineHandle::spawn(journal, None);
        self.raft_replicas
            .lock()
            .unwrap()
            .entry(p)
            .or_insert(handle)
            .clone()
    }

    /// Fans a deployment into every replica engine actor (followers under RF>1) so
    /// a replicated `CreateInstance` for the new definition applies successfully on
    /// every replica. A no-op (and zero overhead) when this node hosts no replica
    /// actors, i.e. single-node, RF=1, or Raft disabled.
    async fn install_into_raft_replicas(&self, events: &Arc<Vec<Event>>) {
        let handles: Vec<EngineHandle> = {
            let map = self.raft_replicas.lock().unwrap();
            if map.is_empty() {
                return;
            }
            map.values().cloned().collect()
        };
        for handle in handles {
            let events = Arc::clone(events);
            handle
                .with(move |journal| journal.install_deployment(&events))
                .await;
        }
    }

    async fn activate_jobs_impl(
        &self,
        body: &models::JobActivationRequest,
    ) -> Result<apis::job::ActivateJobsResponse, ()> {
        use apis::job::ActivateJobsResponse as Resp;

        let job_type = body.r_type.clone();
        let worker = body.worker.clone().unwrap_or_else(|| "default".to_string());
        let max_jobs = body.max_jobs_to_activate.max(0) as usize;
        let timeout = body.timeout.max(0) as u64;

        // Optional projection of the job's variables: an empty or absent list
        // returns every visible variable, a non-empty list returns only the named
        // ones (names not present are simply omitted).
        let fetch_variable = body
            .fetch_variable
            .as_ref()
            .filter(|names| !names.is_empty())
            .cloned();

        // Long-poll window: None/0 -> default; >0 -> that window; <0 -> no waiting.
        let request_timeout = body.request_timeout.unwrap_or(0);
        let long_poll_until = if request_timeout < 0 {
            None
        } else if request_timeout == 0 {
            Some(Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS))
        } else {
            Some(Duration::from_millis(request_timeout as u64))
        };
        let deadline = long_poll_until.map(|d| tokio::time::Instant::now() + d);

        // Remote nodes to draw the shortfall from once local partitions are
        // drained (REST job aggregation, the analog of the stream dispatcher's
        // peer pull). Empty on a single-node cluster ⇒ the whole peer path is
        // skipped and this method is byte-identical to the pre-cluster long poll.
        let peers = self.peer_nodes();

        loop {
            let mut jobs = self
                .try_activate(
                    &job_type,
                    &worker,
                    max_jobs,
                    timeout,
                    fetch_variable.as_deref(),
                )
                .await;

            // Cluster: top up from peers' partitions so a REST worker hitting one
            // gateway is fed by the whole cluster. The peer leases each job under
            // `timeout`, so at-least-once survives this gateway dying before the
            // worker completes (the lease expires and the job re-activates on its
            // owner); completions route back via the cluster-aware REST complete.
            if jobs.len() < max_jobs && !peers.is_empty() {
                for &node in &peers {
                    if jobs.len() >= max_jobs {
                        break;
                    }
                    let want = max_jobs - jobs.len();
                    let more = self
                        .activate_from_peer(
                            node,
                            &job_type,
                            &worker,
                            want,
                            timeout,
                            fetch_variable.as_deref(),
                        )
                        .await;
                    jobs.extend(more);
                }
            }

            if !jobs.is_empty() {
                return Ok(Resp::Status200_TheListOfActivatedJobs(
                    models::JobActivationResult::new(jobs),
                ));
            }

            // No jobs right now. Either return immediately (long polling off) or
            // wait until either new jobs are signalled or the window elapses.
            match deadline {
                None => {
                    return Ok(Resp::Status200_TheListOfActivatedJobs(
                        models::JobActivationResult::new(Vec::new()),
                    ));
                }
                Some(deadline) => {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        return Ok(Resp::Status200_TheListOfActivatedJobs(
                            models::JobActivationResult::new(Vec::new()),
                        ));
                    }
                    // Wait for a wake-up or the remaining window, then retry. The
                    // `jobs_available` notify only fires for LOCAL job arrivals, so
                    // when peers exist we cap the wait to a short poll interval to
                    // re-pull from them within bounded latency. Single-node keeps
                    // the original unbounded wait (no peers, nothing to re-poll).
                    let remaining = deadline - now;
                    let wait = if peers.is_empty() {
                        remaining
                    } else {
                        remaining.min(Duration::from_millis(PEER_ACTIVATION_POLL_MS))
                    };
                    let notified = self.jobs_available.notified();
                    let _ = tokio::time::timeout(wait, notified).await;
                }
            }
        }
    }

    /// Activates up to `max_jobs` jobs of `job_type` and maps them into the
    /// generated REST result type. Activation mutates volatile lease state, so it
    /// runs on the engine actor even though nothing is journaled. The engine
    /// thread does only the cheap work — leasing the jobs and resolving each
    /// one's definition identity — while the 50 KB variable encoding runs here,
    /// off the engine thread, in parallel across cores.
    ///
    /// With multiple partitions the request fans out across them in turn,
    /// accumulating up to `max_jobs` total (jobs of a type can live on any
    /// partition). A single partition takes exactly one pass — identical to the
    /// pre-partitioning path.
    async fn try_activate(
        &self,
        job_type: &str,
        worker: &str,
        max_jobs: usize,
        timeout: u64,
        fetch_variable: Option<&[String]>,
    ) -> Vec<models::ActivatedJobResult> {
        let handles = self.engine.all();
        let n = handles.len();
        let activated: Vec<ActivatedJobWithIdentity> = if n == 1 {
            self.activate_on(&handles[0], job_type, worker, max_jobs, timeout)
                .await
        } else {
            // Fan out across partitions CONCURRENTLY so every partition's engine
            // thread runs its activation pass in parallel, instead of one
            // round-trip at a time (which serialized N engine threads behind the
            // single dispatcher and added N× round-trip latency per activation —
            // the chief reason partitioning did not lift dispatch throughput).
            //
            // Each partition is asked for an exact share of `max_jobs` (an even
            // `base`, with the remainder handed to the first few in rotated
            // order) so the shares sum to exactly `max_jobs` and NO partition
            // ever leases more than its slice. Over-leasing would be worse than
            // under-delivering: a job leased here but not returned to the worker
            // is locked and cannot be redelivered until its lease expires.
            // CreateInstance round-robins across partitions, so the pool is
            // balanced and the even split rarely under-delivers; when it does,
            // continuous dispatch tops it up on the next pass.
            let start = self.engine.activate_start();
            let base = max_jobs / n;
            let rem = max_jobs % n;
            let mut futures = Vec::with_capacity(n);
            for off in 0..n {
                let want = base + usize::from(off < rem);
                if want == 0 {
                    continue;
                }
                let handle = &handles[(start + off) % n];
                futures.push(self.activate_on(handle, job_type, worker, want, timeout));
            }
            futures_util::future::join_all(futures)
                .await
                .into_iter()
                .flatten()
                .collect()
        };

        activated
            .into_iter()
            .map(|activated| activated_job_result(activated, fetch_variable))
            .collect()
    }

    /// Activates up to `want` jobs of `job_type` on a single partition's engine
    /// thread, resolving each job's deployed-process identity in the same pass
    /// (so the lookup runs on the engine thread that owns the state, not the
    /// caller). The serde-heavy variable projection is deferred to
    /// [`activated_job_result`] off the engine thread.
    async fn activate_on(
        &self,
        handle: &EngineHandle,
        job_type: &str,
        worker: &str,
        want: usize,
        timeout: u64,
    ) -> Vec<ActivatedJobWithIdentity> {
        let job_type = job_type.to_string();
        let worker = worker.to_string();
        handle
            .with(move |engine| {
                let now = now_millis();
                engine
                    .activate_jobs(&job_type, &worker, want, timeout, now)
                    .into_iter()
                    .map(|job| {
                        let (process_id, version, process_definition_key) = engine
                            .instance(job.instance_key)
                            .and_then(|instance| engine.state().processes.get(&instance.process_id))
                            .map(|deployed| {
                                (
                                    deployed.definition.id.clone(),
                                    deployed.version,
                                    deployed.key.to_string(),
                                )
                            })
                            .unwrap_or_else(|| (String::new(), 1, String::new()));
                        ActivatedJobWithIdentity {
                            job,
                            process_id,
                            version,
                            process_definition_key,
                        }
                    })
                    .collect()
            })
            .await
    }
}

/// Engine-facing helpers used by the WebSocket command stream (`command_stream`).
/// They mirror the core of the REST handlers above but return plain data instead
/// of the generated response envelope, so the stream can build its own frames.
/// They live here (not in `command_stream`) to keep all engine command issuing
/// next to the REST handlers that share the same command path and `jobs_available`
/// wake discipline.
impl ServerImpl {
    /// Maps the result of a job lifecycle command (complete/fail/throw) into a
    /// `(status, message)` outcome, awaiting durability and waking job pollers on
    /// success. Mirrors the REST handlers' success/error arms.
    fn map_job_outcome(
        result: Result<(Arc<Vec<Event>>, Commit), EngineError>,
    ) -> Result<Commit, (u16, String)> {
        match result {
            Ok((_, commit)) => Ok(commit),
            Err(EngineError::JobNotFound { job_key }) => {
                Err((404, format!("No job with key {job_key}.")))
            }
            Err(EngineError::JobNotActive { job_key }) => {
                Err((409, format!("Job {job_key} is not active.")))
            }
            Err(EngineError::JobNotActivated { job_key }) => {
                Err((409, format!("Job {job_key} has not been activated.")))
            }
            Err(e) => Err((500, e.to_string())),
        }
    }

    /// Stream `CreateInstance`: applies the create command and returns the new
    /// instance key plus whether it completed synchronously. The processing guard
    /// is held only across the engine round-trip (the stream meters intake via
    /// submission credits, not a 503).
    pub(crate) async fn create_for_stream(
        &self,
        by_id: Option<String>,
        by_key: Option<String>,
        variables: std::collections::HashMap<String, Value>,
    ) -> Result<(nanobpmn_engine_core::Key, bool), (u16, String)> {
        // Per-partition Raft (experimental): when this node hosts Raft groups
        // (populated only by the env-gated `raft_bootstrap`), the create is
        // replicated through the partition leader's log instead of applied
        // directly. Empty registry (the default) => byte-identical fast path.
        if !self.raft.is_empty() {
            return self.create_via_raft(by_id, by_key, variables).await;
        }
        // Active-backlog admission control (off by default): shed before doing any
        // engine work when the standing active-instance backlog is at/above the
        // limit, keeping end-to-end latency and memory bounded under overload. The
        // stream client reads the 503 `RESOURCE_EXHAUSTED` as a retry signal. A
        // shed create is never journaled, so durability/at-least-once are intact.
        if let Some(message) = self.admission_shed() {
            return Err((503, message));
        }
        let outcome: Result<(nanobpmn_engine_core::Key, bool, Vec<Event>, Commit), (u16, String)> = {
            let _processing = ProcessingGuard::enter(&self.processing);
            self.engine
                .for_create()
                .with_low(move |engine| {
                    let process_id = match (by_id, by_key) {
                        (Some(id), _) => id,
                        (None, Some(requested)) => match engine
                            .state()
                            .processes
                            .values()
                            .find(|d| d.key.to_string() == requested)
                        {
                            Some(d) => d.definition.id.clone(),
                            None => {
                                return Err((
                                    400,
                                    format!("No deployed process with key '{requested}'."),
                                ));
                            }
                        },
                        (None, None) => {
                            return Err((
                                400,
                                "A processDefinitionId or processDefinitionKey is required."
                                    .to_string(),
                            ));
                        }
                    };
                    match engine.apply_command_at(
                        Command::create_instance_with(process_id.clone(), variables),
                        now_millis(),
                    ) {
                        Ok((events, commit)) => {
                            let instance_key = events
                                .iter()
                                .find_map(Event::instance_key)
                                .expect("created instance has a key");
                            let sync_completed = engine.engine().is_completed(instance_key);
                            // Collect any cross-partition subscription follow-ups
                            // to route once durable (none single-partition).
                            let routable = if engine.engine().num_partitions() > 1 {
                                events
                                    .iter()
                                    .filter(|e| {
                                        matches!(
                                            e,
                                            Event::MessageSubscriptionOpening { .. }
                                                | Event::RemoteMessageCorrelation { .. }
                                                | Event::MessageSubscriptionClosing { .. }
                                                | Event::StartInstanceDispatched { .. }
                                        )
                                    })
                                    .cloned()
                                    .collect()
                            } else {
                                Vec::new()
                            };
                            Ok((instance_key, sync_completed, routable, commit))
                        }
                        Err(EngineError::ProcessNotFound { process_id }) => {
                            Err((400, format!("No deployed process with id '{process_id}'.")))
                        }
                        Err(e) => Err((500, e.to_string())),
                    }
                })
                .await
        };
        let (instance_key, sync_completed, routable, commit) = outcome?;
        commit.wait().await;
        if !routable.is_empty() {
            self.drive_subscription_routing(routable).await;
        }
        self.signal_jobs_available();
        Ok((instance_key, sync_completed))
    }

    /// The Raft create path (experimental): resolve the process id, then replicate
    /// `CreateInstance` through the chosen partition's Raft leader. The state
    /// machine applies the committed command to the same engine actor the rest of
    /// the server reads from, so durability and serving share one materialized
    /// copy. Returns the minted instance key and whether it completed
    /// synchronously (no async jobs), matching [`Self::create_for_stream`].
    async fn create_via_raft(
        &self,
        by_id: Option<String>,
        by_key: Option<String>,
        variables: std::collections::HashMap<String, Value>,
    ) -> Result<(nanobpmn_engine_core::Key, bool), (u16, String)> {
        if let Some(message) = self.admission_shed() {
            return Err((503, message));
        }
        let p = self.engine.for_create_partition();
        let Some(handle) = self.engine.local_for_partition(p) else {
            return Err((500, format!("no local engine actor for partition {p}")));
        };

        // Resolve the process-definition id (a by-key create needs a read of the
        // engine's deployed-process table) before proposing — the command carries
        // a concrete `process_id`.
        let resolve = handle
            .with(move |journal| match (by_id, by_key) {
                (Some(id), _) => Ok(id),
                (None, Some(requested)) => journal
                    .state()
                    .processes
                    .values()
                    .find(|d| d.key.to_string() == requested)
                    .map(|d| d.definition.id.clone())
                    .ok_or((400, format!("No deployed process with key '{requested}'."))),
                (None, None) => Err((
                    400,
                    "A processDefinitionId or processDefinitionKey is required.".to_string(),
                )),
            })
            .await;
        let process_id = resolve?;

        let Some(part) = self.raft.get(p) else {
            return Err((500, format!("partition {p} has no Raft group")));
        };
        let node_id = self.engine.topology().node_id as u64;
        if part.raft.metrics().borrow().current_leader != Some(node_id) {
            // A non-leader replica cannot accept writes. Under the static
            // leader_of map this only happens transiently during an election;
            // the stream client retries on 503.
            return Err((503, format!("partition {p} leader unavailable; retry")));
        }

        let response = part
            .propose_result(
                Command::create_instance_with(process_id, variables),
                now_millis(),
            )
            .await
            .map_err(|e| (500, format!("raft propose failed: {e}")))?;
        if let Some((status, message)) = response.error {
            return Err((status, message));
        }
        let events = response.events;

        let instance_key = events
            .iter()
            .find_map(Event::instance_key)
            .ok_or((500, "raft create produced no instance key".to_string()))?;
        let sync_completed = events.iter().any(|e| {
            matches!(e, Event::ProcessInstanceCompleted { instance_key: k } if *k == instance_key)
        });
        let routable: Vec<Event> = events
            .into_iter()
            .filter(|e| {
                matches!(
                    e,
                    Event::MessageSubscriptionOpening { .. }
                        | Event::RemoteMessageCorrelation { .. }
                        | Event::MessageSubscriptionClosing { .. }
                        | Event::StartInstanceDispatched { .. }
                )
            })
            .collect();
        if !routable.is_empty() {
            self.drive_subscription_routing(routable).await;
        }
        self.signal_jobs_available();
        Ok((instance_key, sync_completed))
    }

    /// The Raft job-mutation path (experimental): replicate `command` through the
    /// owning partition's Raft leader, returning a ready [`Commit`] (durability is
    /// already awaited inside the state-machine apply). Surfaces engine rejections
    /// (404/409) via the replicated response, matching the direct path's statuses.
    async fn propose_job_for_stream(
        &self,
        job_key: u64,
        command: Command,
    ) -> Result<Commit, (u16, String)> {
        let p = partition_of(job_key);
        let Some(part) = self.raft.get(p) else {
            return Err((500, format!("partition {p} has no Raft group")));
        };
        let node_id = self.engine.topology().node_id as u64;
        if part.raft.metrics().borrow().current_leader != Some(node_id) {
            return Err((503, format!("partition {p} leader unavailable; retry")));
        }
        let response = part
            .propose_result(command, now_millis())
            .await
            .map_err(|e| (500, format!("raft propose failed: {e}")))?;
        if let Some((status, message)) = response.error {
            return Err((status, message));
        }
        self.spawn_routing_if_needed(&response.events);
        Ok(Commit::ready())
    }

    /// Stream `CompleteJob`: applies the command on the engine actor (establishing
    /// journal order) and returns the [`Commit`] WITHOUT awaiting durability.
    ///
    /// The caller (`command_stream::pipeline_job_command`) awaits the commit in a
    /// detached task off the connection's read path, so multiple completions from
    /// many connections can be in flight at once, letting the journal's group-commit
    /// coalesce their fsyncs into larger batches. This **ack-before-fsync pipelining**
    /// delivers ~4× higher throughput (measured: 2280 vs 572 writes/s) on fsync-bound
    /// disks.
    ///
    /// Journal arrival order is still correct (frame order) because the engine actor
    /// round-trip below is awaited inline by the reader loop before returning the
    /// commit handle. If the server crashes after replying `200` but before the fsync
    /// (~5ms window), the job re-activates on restart (lock expires), preserving
    /// at-least-once semantics. See README.md "Stream durability: ack-before-fsync
    /// pipelining" and `command_stream::pipeline_job_command` for full rationale.
    pub(crate) async fn complete_job_for_stream(
        &self,
        job_key: u64,
        variables: std::collections::HashMap<String, Value>,
    ) -> Result<Commit, (u16, String)> {
        if !self.raft.is_empty() {
            return self
                .propose_job_for_stream(job_key, Command::complete_job_with(job_key, variables))
                .await;
        }
        let result = self
            .engine
            .by_key(job_key)
            .with(move |engine| {
                engine.apply_command_at(Command::complete_job_with(job_key, variables), now_millis())
            })
            .await;
        if let Ok((events, _)) = &result {
            self.spawn_routing_if_needed(events);
        }
        Self::map_job_outcome(result)
    }

    /// Stream `FailJob`. Returns the [`Commit`] for off-path pipelining; see
    /// [`Self::complete_job_for_stream`].
    pub(crate) async fn fail_job_for_stream(
        &self,
        job_key: u64,
        retries: i32,
        error_message: String,
    ) -> Result<Commit, (u16, String)> {
        if !self.raft.is_empty() {
            return self
                .propose_job_for_stream(
                    job_key,
                    Command::fail_job(job_key, retries, error_message),
                )
                .await;
        }
        let result = self
            .engine
            .by_key(job_key)
            .with(move |engine| {
                engine.apply_command_at(Command::fail_job(job_key, retries, error_message), now_millis())
            })
            .await;
        if let Ok((events, _)) = &result {
            self.spawn_routing_if_needed(events);
        }
        Self::map_job_outcome(result)
    }

    /// Stream `ThrowError`. Returns the [`Commit`] for off-path pipelining; see
    /// [`Self::complete_job_for_stream`].
    pub(crate) async fn throw_error_for_stream(
        &self,
        job_key: u64,
        error_code: String,
        error_message: String,
    ) -> Result<Commit, (u16, String)> {
        if !self.raft.is_empty() {
            return self
                .propose_job_for_stream(
                    job_key,
                    Command::throw_job_error(job_key, error_code, error_message),
                )
                .await;
        }
        let result = self
            .engine
            .by_key(job_key)
            .with(move |engine| {
                engine.apply_command_at(
                    Command::throw_job_error(job_key, error_code, error_message),
                    now_millis(),
                )
            })
            .await;
        if let Ok((events, _)) = &result {
            self.spawn_routing_if_needed(events);
        }
        Self::map_job_outcome(result)
    }

    /// Activates up to `max_jobs` of `job_type` for `worker` and returns the
    /// projected REST job results. Thin wrapper over [`Self::try_activate`] so the
    /// stream dispatcher reuses the exact activation + off-thread variable
    /// encoding path as the REST `activateJobs`.
    pub(crate) async fn activate_for_stream(
        &self,
        job_type: &str,
        worker: &str,
        max_jobs: usize,
        timeout: u64,
        fetch_variable: Option<&[String]>,
    ) -> Vec<models::ActivatedJobResult> {
        self.try_activate(job_type, worker, max_jobs, timeout, fetch_variable)
            .await
    }

    /// Read access to the create-side backpressure controller for the stream's
    /// submission-credit policy.
    pub(crate) fn submission_pressure(&self) -> bool {
        let processing = self.processing.load(Ordering::Relaxed);
        self.backpressure.should_shed(processing)
    }

    /// Active-backlog / create-queue admission gate. When either
    /// `NANOBPMN_ADMISSION_MAX_BACKLOG` or `NANOBPMN_ADMISSION_MAX_CREATE_QUEUE` is
    /// set (> 0), returns `Some(reason)` once the corresponding signal is at or
    /// above its limit, signalling the create should be shed; `None` when both are
    /// off or have headroom. Relaxed atomic loads — no engine round-trip; an
    /// approximate bound is fine. The create-queue gate fires first under overload
    /// (completion-priority diverts the pile-up there); the active-backlog gate is
    /// the complementary memory bound for worker-starved workloads.
    pub(crate) fn admission_shed(&self) -> Option<String> {
        let cq_limit = self.admission_max_create_queue;
        if cq_limit > 0 {
            let depth = self.engine.pending_create_queue();
            if depth >= cq_limit {
                return Some(format!(
                    "Admission control: create queue depth {depth} at or above the \
                     configured limit of {cq_limit}. Retry after a backoff."
                ));
            }
        }
        let backlog_limit = self.admission_max_backlog;
        if backlog_limit > 0 {
            let backlog = self.inflight.load(Ordering::Relaxed);
            if backlog >= backlog_limit {
                return Some(format!(
                    "Admission control: {backlog} active instances at or above the \
                     configured backlog limit of {backlog_limit}. Retry after a backoff."
                ));
            }
        }
        None
    }

    /// Awaits a created instance reaching a terminal state for the stream's async
    /// `InstanceCompleted` frame. Reuses the REST await path verbatim.
    pub(crate) async fn await_completion_for_stream(
        &self,
        instance_key: nanobpmn_engine_core::Key,
        fetch_variables: Option<&Vec<String>>,
        request_timeout: Option<i64>,
    ) -> (std::collections::HashMap<String, types::Object>, bool) {
        self.await_process_completion(instance_key, fetch_variables, request_timeout)
            .await
    }

    /// Signals that new jobs may have become activatable. Wakes both the
    /// long-polling `activateJobs` REST waiters (broadcast) and the
    /// command-stream dispatcher (permit-storing, so the wake survives an
    /// in-flight dispatch pass).
    pub(crate) fn signal_jobs_available(&self) {
        self.jobs_available.notify_waiters();
        self.dispatch_wake.notify_one();
    }

    /// Handle to the permit-storing dispatcher wake, so the command stream can
    /// wake the dispatcher after a new subscription or credit grant without the
    /// signal being lost mid-pass.
    pub(crate) fn dispatch_wake_handle(&self) -> Arc<tokio::sync::Notify> {
        self.dispatch_wake.clone()
    }
}

/// Current wall-clock time in milliseconds since the Unix epoch. The engine is
/// clock-free; the server owns the real clock and feeds it logical instants.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A placeholder timestamp. The clock-free engine does not record wall-clock
/// times for process instances, so their read projections report the Unix
/// epoch. (Incidents do carry a real `created_at` fed in at command time.)
fn epoch() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).expect("epoch is valid")
}

/// Maps an engine [`IncidentKind`] to the REST `errorType` taxonomy.
fn incident_error_type_enum(kind: IncidentKind) -> models::IncidentErrorTypeEnum {
    match kind {
        IncidentKind::JobNoRetries => models::IncidentErrorTypeEnum::JobNoRetries,
        IncidentKind::NoMatchingSequenceFlow => models::IncidentErrorTypeEnum::ConditionError,
        IncidentKind::ExpressionEvaluation => models::IncidentErrorTypeEnum::ExtractValueError,
        IncidentKind::UnhandledError => models::IncidentErrorTypeEnum::UnhandledErrorEvent,
    }
}

/// Maps an engine [`IncidentState`] to the REST incident `state` enum.
fn incident_state_enum(state: IncidentState) -> models::IncidentStateEnum {
    match state {
        IncidentState::Active => models::IncidentStateEnum::Active,
        IncidentState::Resolved => models::IncidentStateEnum::Resolved,
    }
}

/// Projects an [`IncidentRow`] into the generated `IncidentResult`. The
/// process-definition identity is denormalized onto the row at projection time.
fn incident_result(incident: &readstore::IncidentRow) -> models::IncidentResult {
    let process_definition_id = incident.process_definition_id.clone();
    let process_definition_key = incident.process_definition_key.clone();

    let error_type = incident_error_type_enum(incident.kind);
    let job_key = match incident.job_key {
        Some(k) => types::Nullable::Present(models::JobKey(k.to_string())),
        None => types::Nullable::Null,
    };
    let creation_time = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
        incident.created_at_ms as i64,
    )
    .unwrap_or_else(epoch);

    let incident_state = incident_state_enum(incident.state);

    models::IncidentResult::new(
        process_definition_id,
        error_type,
        incident.reason.clone(),
        incident.element_id.clone(),
        creation_time,
        incident_state,
        "<default>".to_string(),
        models::IncidentKey(incident.key.to_string()),
        models::ProcessDefinitionKey(process_definition_key),
        models::ProcessInstanceKey(incident.instance_key.to_string()),
        types::Nullable::Null,
        models::ElementInstanceKey(incident.element_instance_key.to_string()),
        job_key,
    )
}

/// Projects a [`ProcessInstanceRow`] into the generated `ProcessInstanceResult`.
fn process_instance_result(
    instance: &readstore::ProcessInstanceRow,
) -> models::ProcessInstanceResult {
    let process_definition_id = instance.process_definition_id.clone();
    let version = instance.version;
    let process_definition_key = instance.process_definition_key.clone();

    let state_enum = process_instance_state_enum(instance.state);

    let start_date = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
        instance.start_date_ms as i64,
    )
    .unwrap_or_else(epoch);

    models::ProcessInstanceResult::new(
        process_definition_id,
        types::Nullable::Null,
        version,
        types::Nullable::Null,
        start_date,
        types::Nullable::Null,
        state_enum,
        instance.has_incident,
        "<default>".to_string(),
        models::ProcessInstanceKey(instance.key.to_string()),
        models::ProcessDefinitionKey(process_definition_key),
        types::Nullable::Null,
        types::Nullable::Null,
        types::Nullable::Null,
        instance.tags.clone().into_iter().map(models::Tag).collect(),
        instance.business_id.clone().map(types::Nullable::Present).unwrap_or(types::Nullable::Null),
    )
}

/// A synthesized resource (file) name for a process id. The engine does not
/// retain the original deployment resource name, so derive a stable `.bpmn`
/// name from the id.
fn resource_name(process_id: &str) -> String {
    format!("{process_id}.bpmn")
}

/// Projects a [`ProcessDefinitionRow`] into the generated
/// `ProcessDefinitionResult`. The engine stores no display name or version tag,
/// so `name` mirrors the id and `versionTag` is null.
fn process_definition_result(
    deployed: &readstore::ProcessDefinitionRow,
) -> models::ProcessDefinitionResult {
    let id = deployed.process_id.clone();
    models::ProcessDefinitionResult::new(
        types::Nullable::Present(id.clone()),
        resource_name(&id),
        deployed.version,
        types::Nullable::Null,
        id,
        "<default>".to_string(),
        models::ProcessDefinitionKey(deployed.key.to_string()),
        false,
    )
}

/// The byte length beyond which a variable value is truncated in search results
/// (when `truncateValues` is on). Mirrors the order of magnitude of Camunda's
/// variable value preview; nano's typical values are far shorter.
const VARIABLE_VALUE_PREVIEW_LEN: usize = 8192;

/// Whether `value` would be truncated given the current `truncate` setting.
fn value_is_truncated(value: &str, truncate: bool) -> bool {
    truncate && value.len() > VARIABLE_VALUE_PREVIEW_LEN
}

/// Truncates `value` to the preview length on a char boundary when `truncate` is
/// on, returning the (possibly shortened) value and whether it was truncated.
fn truncate_value(value: &str, truncate: bool) -> (String, bool) {
    if value_is_truncated(value, truncate) {
        let mut end = VARIABLE_VALUE_PREVIEW_LEN;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        (value[..end].to_string(), true)
    } else {
        (value.to_string(), false)
    }
}

/// Projects a [`VariableRow`] into the generated `VariableSearchResult`, applying
/// value truncation per the request's `truncateValues` setting.
fn variable_search_result(
    v: &readstore::VariableRow,
    truncate: bool,
) -> models::VariableSearchResult {
    let (value, is_truncated) = truncate_value(&v.value, truncate);
    models::VariableSearchResult::new(
        v.name.clone(),
        "<default>".to_string(),
        models::VariableKey(v.key.to_string()),
        models::ScopeKey(v.scope_key.to_string()),
        models::ProcessInstanceKey(v.instance_key.to_string()),
        types::Nullable::Null,
        value,
        is_truncated,
    )
}

/// Projects a [`VariableRow`] into the generated `VariableResult` (single-get),
/// always carrying the full untruncated value.
fn variable_result(v: &readstore::VariableRow) -> models::VariableResult {
    models::VariableResult::new(
        v.name.clone(),
        "<default>".to_string(),
        models::VariableKey(v.key.to_string()),
        models::ScopeKey(v.scope_key.to_string()),
        models::ProcessInstanceKey(v.instance_key.to_string()),
        types::Nullable::Null,
        v.value.clone(),
    )
}

/// Maps an engine [`ProcessInstanceState`] to the REST state enum.
fn process_instance_state_enum(
    state: ProcessInstanceState,
) -> models::ProcessInstanceStateEnum {
    match state {
        ProcessInstanceState::Active => models::ProcessInstanceStateEnum::Active,
        ProcessInstanceState::Completed => models::ProcessInstanceStateEnum::Completed,
        ProcessInstanceState::Terminated => models::ProcessInstanceStateEnum::Terminated,
    }
}

/// Maps an engine [`nanobpmn_engine_core::JobState`] to the REST job state enum.
/// The engine's transient `Activated` (locked to a worker) has no distinct wire
/// state, so it projects to `CREATED` like any other activatable job.
fn job_state_enum(state: nanobpmn_engine_core::JobState) -> models::JobStateEnum {
    use nanobpmn_engine_core::JobState;
    match state {
        JobState::Created | JobState::Activated => models::JobStateEnum::Created,
        JobState::Failed => models::JobStateEnum::Failed,
        JobState::Errored => models::JobStateEnum::ErrorThrown,
        JobState::Completed => models::JobStateEnum::Completed,
        JobState::Canceled => models::JobStateEnum::Canceled,
    }
}

/// Projects a [`JobRow`] into the generated `JobSearchResult`. The
/// process-definition identity is denormalized onto the row at projection time.
fn job_search_result(job: &readstore::JobRow) -> models::JobSearchResult {
    let process_definition_id = job.process_definition_id.clone();
    let process_definition_key = job.process_definition_key.clone();

    let deadline = match job.deadline_ms {
        Some(ms) => chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms as i64)
            .map(types::Nullable::Present)
            .unwrap_or(types::Nullable::Null),
        None => types::Nullable::Null,
    };

    models::JobSearchResult::new(
        std::collections::HashMap::new(),
        deadline,
        types::Nullable::Null,
        types::Nullable::Present(job.element_id.clone()),
        models::ElementInstanceKey(job.element_instance_key.to_string()),
        types::Nullable::Null,
        types::Nullable::Null,
        types::Nullable::Null,
        false,
        types::Nullable::Null,
        models::JobKey(job.key.to_string()),
        models::JobKindEnum::BpmnElement,
        models::JobListenerEventTypeEnum::Unspecified,
        process_definition_id,
        models::ProcessDefinitionKey(process_definition_key),
        models::ProcessInstanceKey(job.instance_key.to_string()),
        types::Nullable::Null,
        job.retries,
        job_state_enum(job.state),
        "<default>".to_string(),
        job.job_type.clone(),
        job.worker.clone().unwrap_or_default(),
        types::Nullable::Null,
        types::Nullable::Null,
        0,
    )
}

/// Maps an engine [`nanobpmn_engine_core::UserTaskState`] to the REST user-task
/// state enum.
fn user_task_state_enum(
    state: nanobpmn_engine_core::UserTaskState,
) -> models::UserTaskStateEnum {
    use nanobpmn_engine_core::UserTaskState;
    match state {
        UserTaskState::Created => models::UserTaskStateEnum::Created,
        UserTaskState::Completed => models::UserTaskStateEnum::Completed,
        UserTaskState::Canceled => models::UserTaskStateEnum::Canceled,
    }
}

/// Projects a [`readstore::UserTaskRow`] into the generated `UserTaskResult`. The
/// process-definition identity is denormalized onto the row at projection time.
fn user_task_result(task: &readstore::UserTaskRow) -> models::UserTaskResult {
    let creation_date =
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(task.created_at_ms as i64)
            .unwrap_or_else(chrono::Utc::now);

    let parse_date = |d: &Option<String>| -> types::Nullable<chrono::DateTime<chrono::Utc>> {
        match d.as_deref().map(|s| s.parse::<chrono::DateTime<chrono::Utc>>()) {
            Some(Ok(dt)) => types::Nullable::Present(dt),
            _ => types::Nullable::Null,
        }
    };

    let mut result = models::UserTaskResult::new(
        types::Nullable::Null,
        user_task_state_enum(task.state),
        match &task.assignee {
            Some(a) => types::Nullable::Present(a.clone()),
            None => types::Nullable::Null,
        },
        task.element_id.clone(),
        task.candidate_groups.clone(),
        task.candidate_users.clone(),
        task.process_definition_id.clone(),
        creation_date,
        types::Nullable::Null,
        parse_date(&task.follow_up_date),
        parse_date(&task.due_date),
        "<default>".to_string(),
        types::Nullable::Null,
        task.process_definition_version,
        std::collections::HashMap::new(),
        models::UserTaskKey(task.key.to_string()),
        models::ElementInstanceKey(task.element_instance_key.to_string()),
        types::Nullable::Null,
        models::ProcessDefinitionKey(task.process_definition_key.clone()),
        models::ProcessInstanceKey(task.instance_key.to_string()),
        types::Nullable::Null,
        types::Nullable::Null,
        Vec::new(),
    );
    result.priority = task.priority.clamp(0, 100) as u8;
    result
}

/// Maps an engine [`ActivatedJob`] into the generated `ActivatedJobResult`,
/// resolving process-definition identity from engine state. When `fetch_variable`
/// is `Some`, only the named variables are returned; `None` returns all of the
/// job's visible variables.
/// An activated job paired with its resolved process-definition identity, looked
/// up on the engine thread so the off-thread response mapper needs no engine
/// access. The job still owns its variable snapshot, which is encoded to JSON in
/// [`activated_job_result`] off the single engine thread.
struct ActivatedJobWithIdentity {
    job: ActivatedJob,
    process_id: String,
    version: i32,
    process_definition_key: String,
}

fn activated_job_result(
    activated: ActivatedJobWithIdentity,
    fetch_variable: Option<&[String]>,
) -> models::ActivatedJobResult {
    let ActivatedJobWithIdentity {
        job,
        process_id,
        version,
        process_definition_key,
    } = activated;

    // Project the job's variables into the REST object map off the engine
    // thread. `job.variables` is an `Arc` shared with engine state, so reading it
    // here neither blocks the command thread nor deep-clones the value tree.
    let variables = match fetch_variable {
        Some(names) => {
            let filtered: std::collections::HashMap<String, Value> = job
                .variables
                .iter()
                .filter(|(name, _)| names.iter().any(|n| n == *name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            to_object_map(&filtered)
        }
        None => to_object_map(&job.variables),
    };

    models::ActivatedJobResult::new(
        job.job_type,
        process_id,
        version,
        job.element_id,
        std::collections::HashMap::new(),
        job.worker,
        job.retries,
        job.deadline as i64,
        variables,
        "<default>".to_string(),
        models::JobKey(job.key.to_string()),
        models::ProcessInstanceKey(job.instance_key.to_string()),
        models::ProcessDefinitionKey(process_definition_key),
        models::ElementInstanceKey(job.element_instance_key.to_string()),
        models::JobKindEnum::BpmnElement,
        models::JobListenerEventTypeEnum::Unspecified,
        nanobpm_gateway_rest::types::Nullable::Null,
        Vec::new(),
        nanobpm_gateway_rest::types::Nullable::Null,
        0,
    )
}

/// Converts engine variables into the generated `Object` (JSON) map used by the
/// REST models.
fn to_object_map(
    variables: &std::collections::HashMap<String, Value>,
) -> std::collections::HashMap<String, types::Object> {
    variables
        .iter()
        .map(|(name, value)| (name.clone(), types::Object(value_to_json(value))))
        .collect()
}

/// Converts a REST `Object` (JSON) variable map into engine variables.
fn from_object_map(
    variables: &std::collections::HashMap<String, types::Object>,
) -> std::collections::HashMap<String, Value> {
    variables
        .iter()
        .map(|(name, object)| (name.clone(), json_to_value(&object.0)))
        .collect()
}

/// Converts an optional REST variables map into the plain JSON map carried over
/// the command stream when a message is fanned out to cluster peers. Preserves
/// the original JSON exactly so the peer re-derives identical engine values.
fn wire_variables(
    variables: Option<&std::collections::HashMap<String, types::Object>>,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    variables.map(|m| m.iter().map(|(k, o)| (k.clone(), o.0.clone())).collect())
}

/// Whether a forwarded by-key command's peer status counts as success. The
/// job-lifecycle handlers reply `200` (ack-before-fsync pipeline) and the other
/// by-key handlers reply `204`; both mean the command applied.
fn is_ok_status(status: u16) -> bool {
    status == 200 || status == 204
}

/// Extracts a human-readable detail from a peer's error `CommandResult` body
/// (a JSON string), for the `problem(...)` detail field surfaced to the client.
fn peer_detail(res: &crate::peer::PeerResult) -> String {
    res.body
        .as_ref()
        .and_then(|b| b.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("peer returned status {}", res.status))
}

/// Converts a JSON value into the engine [`Value`] tree, preserving numbers
/// (integral vs. decimal), lists and objects so FEEL can operate on them.
pub(crate) fn json_to_value(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else {
                Value::number(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::Str(s.clone()),
        serde_json::Value::Array(items) => {
            Value::List(items.iter().map(json_to_value).collect())
        }
        serde_json::Value::Object(entries) => Value::Map(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect(),
        ),
    }
}

/// Converts an engine [`Value`] tree back into JSON for the REST wire.
pub(crate) fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Double(d) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Str(s) => serde_json::Value::String(s.clone()),
        Value::List(items) => {
            serde_json::Value::Array(items.iter().map(value_to_json).collect())
        }
        Value::Map(entries) => serde_json::Value::Object(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect(),
        ),
    }
}

/// The minted message key from a `CorrelateMessage`'s events: the heading
/// [`Event::MessagePublished`] always carries it.
fn message_key_of(events: &[Event]) -> u64 {
    events
        .iter()
        .find_map(|e| match e {
            Event::MessagePublished { message_key, .. } => Some(*message_key),
            _ => None,
        })
        .expect("CorrelateMessage always emits MessagePublished")
}

/// Maps the stub `Err(())` returned by every operation to `501 Not Implemented`.
#[async_trait::async_trait]
impl apis::ErrorHandler<()> for ServerImpl {
    async fn handle_error(
        &self,
        _method: &http::Method,
        _host: &headers::Host,
        _cookies: &axum_extra::extract::CookieJar,
        _error: (),
    ) -> Result<Response, StatusCode> {
        Response::builder()
            .status(StatusCode::NOT_IMPLEMENTED)
            .header(http::header::CONTENT_TYPE, "text/plain")
            .body(Body::from(
                "Not implemented: backend services are not wired yet.\n",
            ))
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }
}

/// Accepts any credentials. Authentication is not enforced in the stub server;
/// real claim extraction will be added when authentication is wired.
#[async_trait::async_trait]
impl apis::ApiAuthBasic for ServerImpl {
    type Claims = ();

    async fn extract_claims_from_auth_header(
        &self,
        _kind: apis::BasicAuthKind,
        _headers: &http::header::HeaderMap,
        _key: &str,
    ) -> Option<Self::Claims> {
        Some(())
    }
}

/// Maximum number of body bytes rendered in a `DEBUG_REST` log line. Larger
/// bodies are truncated in the log; the full body still reaches the handler.
const REST_LOG_BODY_PREVIEW: usize = 4096;

/// Whether `DEBUG_REST` requests verbose REST request/response logging. Accepts
/// the usual truthy spellings (`1`, `true`, `yes`, `on`); unset or anything
/// else leaves it off.
/// `GET /metrics` — Prometheus text exposition of the durability hot-path
/// metrics (commit batch size, fsync/commit-wait latency, pipeline depth). Served
/// unauthenticated alongside the REST API; scrape it while benchmarking to see
/// how many writes share each fsync.
async fn metrics_handler() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(
            http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )
        .body(Body::from(metrics::gather()))
        .expect("metrics response builds")
}

fn debug_rest_enabled() -> bool {
    std::env::var("DEBUG_REST")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Whether per-partition Raft replication is enabled (env `NANOBPMN_RAFT`). Off
/// by default, so the classic single-writer path is byte-identical and carries
/// zero Raft overhead unless a deployer explicitly opts in.
fn raft_enabled() -> bool {
    std::env::var("NANOBPMN_RAFT")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Renders a body as a single-line, length-prefixed preview for logging,
/// truncating long payloads and collapsing newlines so each request stays on
/// one log line. An empty body renders as the empty string.
fn body_preview(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    let shown = bytes.len().min(REST_LOG_BODY_PREVIEW);
    let text = String::from_utf8_lossy(&bytes[..shown])
        .replace('\n', " ")
        .replace('\r', "");
    let ellipsis = if bytes.len() > shown { "…" } else { "" };
    format!(" [{} bytes] {text}{ellipsis}", bytes.len())
}

/// axum middleware, mounted only when `DEBUG_REST` is enabled, that logs each
/// REST request and its response (method, URI, status, latency, and a preview
/// of both bodies). Bodies are buffered so they can be logged and then handed
/// on unchanged — intentionally opt-in, since buffering defeats streaming.
async fn log_rest(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();

    let (parts, body) = req.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .unwrap_or_default();
    tracing::info!(target: "rest", "--> {method} {uri}{}", body_preview(&bytes));
    let req = axum::extract::Request::from_parts(parts, Body::from(bytes));

    let started = std::time::Instant::now();
    let resp = next.run(req).await;
    let elapsed = started.elapsed();

    let status = resp.status();
    let (parts, body) = resp.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .unwrap_or_default();
    tracing::info!(
        target: "rest",
        "<-- {method} {uri} {status} ({elapsed:.1?}){}",
        body_preview(&bytes)
    );
    Response::from_parts(parts, Body::from(bytes))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();
    // Enable jemalloc's background page-decay thread where supported (Linux), so
    // freed memory returns to the OS automatically; on macOS the idle-purge tick
    // forces it instead.
    memory::enable_background_thread();
    // Resolve where the journal (durable event log) and read-model database live.
    let (journal_path, db_path) = resolve_data_paths();

    let server = match journal_path {
        Some(journal_path) => {
            // Persistent run: the read store is a derived projection of the
            // journal(s), so reconcile it against the log before serving.
            let store = Arc::new(
                ReadStore::open(db_path.as_deref()).unwrap_or_else(|e| {
                    let at = db_path
                        .as_deref()
                        .map(|p| format!(" at {}", p.display()))
                        .unwrap_or_default();
                    panic!("failed to open read model{at} (is the file or its directory writable?): {e}")
                }),
            );
            let partitions = partition_count_from_env();
            let topology = cluster::Topology::from_env(partitions as u64);
            let (journals, recovered) = if topology.is_single_node() && partitions == 1 {
                // Single partition: warm-start fast by replaying only the events
                // the store has not yet projected (a full rebuild on a
                // fresh/reset store). The journal file keeps its historical name.
                let events = Journal::read_events(&journal_path).unwrap_or_else(|e| {
                    panic!("failed to read journal {}: {e}", journal_path.display())
                });
                let mut pos = store.exported_position();
                if pos > events.len() {
                    // The store is ahead of the log (truncated/corrupt journal):
                    // rebuild from scratch.
                    store.reset().expect("reset read store");
                    pos = 0;
                }
                if pos < events.len() {
                    let refs: Vec<&Event> = events[pos..].iter().collect();
                    store
                        .export(&refs)
                        .expect("catch up read model from journal");
                }
                let journal = Journal::open(&journal_path).unwrap_or_else(|e| {
                    panic!("failed to open journal {}: {e}", journal_path.display())
                });
                let recovered = !journal.is_fresh();
                (vec![journal], recovered)
            } else if topology.is_single_node() {
                // Multi-partition: ONE shared group-commit WAL for every
                // partition (a single [`SharedWriter`]: one file, one writer
                // thread, one fsync stream). Funneling all partitions through one
                // writer eliminates per-partition fsync fragmentation — the chief
                // multi-partition throughput cap — while the partitions stay fully
                // independent for processing (each its own engine actor and key
                // namespace). Events are tagged by partition via their keys, so
                // the single log is demultiplexed back to the owning partition on
                // replay.
                //
                // The read model is rebuilt from scratch: the runtime exporter
                // interleaves partitions in projection order, which need not match
                // the shared log's commit order, so the single `exported_position`
                // cursor can't track it incrementally. Projection is
                // order-independent across the (independent) partitions, and the
                // boot-time deployment on partition 0 is written first, so a full
                // replay in log order is correct.
                let events = Journal::read_events(&journal_path).unwrap_or_else(|e| {
                    panic!("failed to read journal {}: {e}", journal_path.display())
                });
                store
                    .reset()
                    .expect("reset read store for multi-partition rebuild");
                if !events.is_empty() {
                    let refs: Vec<&Event> = events.iter().collect();
                    store
                        .export(&refs)
                        .expect("catch up read model from journal");
                }

                // Split the shared log into each partition's own events by the
                // owning partition encoded in every event's key. A key whose
                // partition is out of range (e.g. a log from a larger partition
                // layout) falls back to partition 0 so replay never panics.
                let mut per_partition: Vec<Vec<Event>> =
                    (0..partitions).map(|_| Vec::new()).collect();
                for event in events {
                    let p = nanobpmn_engine_core::partition_of(event.max_key()) as usize;
                    per_partition[p.min(partitions - 1)].push(event);
                }

                let shared = SharedWriter::open(&journal_path).unwrap_or_else(|e| {
                    panic!(
                        "failed to open shared journal {}: {e}",
                        journal_path.display()
                    )
                });
                let recovered = per_partition.iter().any(|evs| !evs.is_empty());
                let journals: Vec<Journal> = per_partition
                    .into_iter()
                    .enumerate()
                    .map(|(i, evs)| Journal::from_events_shared(i as u64, evs, &shared))
                    .collect();
                (journals, recovered)
            } else {
                // Clustered: this node owns only a SUBSET of the cluster's
                // partitions (`partition_id % num_nodes == node_id`). Its journal
                // file therefore holds only its own partitions' events; rebuild
                // its read model from them and open one engine actor per owned
                // partition (each keyed by its GLOBAL partition id so keys stay
                // globally unique across the cluster). Partitions owned by peers
                // are reached by forwarding (handled by the routing seam), not
                // replayed here.
                let owned = topology.local_partitions();
                assert!(
                    !owned.is_empty(),
                    "clustered node {} owns no partitions (NANOBPMN_PARTITIONS={} must exceed node count, or fix NANOBPMN_NODE_ID)",
                    topology.node_id,
                    partitions,
                );
                let events = Journal::read_events(&journal_path).unwrap_or_else(|e| {
                    panic!("failed to read journal {}: {e}", journal_path.display())
                });
                store
                    .reset()
                    .expect("reset read store for clustered rebuild");
                if !events.is_empty() {
                    let refs: Vec<&Event> = events.iter().collect();
                    store
                        .export(&refs)
                        .expect("catch up read model from journal");
                }
                // Demultiplex the node's log into its owned partitions by the
                // partition id encoded in every key. Any event for a partition
                // this node does not own (a stray from a re-sharded layout) is
                // dropped — its owner replays it from its own journal.
                //
                // `ProcessDeployed` is the exception: a deployment definition is
                // partition-agnostic (it mints no instance state and arms no
                // subscriptions) and its key belongs to the deployment partition
                // (0), which a peer node does not own. So every `ProcessDeployed`
                // is replayed into *every* owned partition, reconstructing the
                // definition cluster-wide from a single durable copy (see
                // `Journal::install_deployment_durable`). Start subscriptions /
                // timers keep their partition-0 keys and demux normally, so they
                // are only ever rebuilt on the deployment partition's owner.
                let mut per_owned: std::collections::HashMap<u64, Vec<Event>> =
                    owned.iter().map(|p| (*p, Vec::new())).collect();
                for event in events {
                    if matches!(event, Event::ProcessDeployed { .. }) {
                        for bucket in per_owned.values_mut() {
                            bucket.push(event.clone());
                        }
                        continue;
                    }
                    let p = nanobpmn_engine_core::partition_of(event.max_key());
                    if let Some(bucket) = per_owned.get_mut(&p) {
                        bucket.push(event);
                    }
                }
                let shared = SharedWriter::open(&journal_path).unwrap_or_else(|e| {
                    panic!(
                        "failed to open shared journal {}: {e}",
                        journal_path.display()
                    )
                });
                let recovered = per_owned.values().any(|evs| !evs.is_empty());
                let journals: Vec<Journal> = owned
                    .iter()
                    .map(|p| {
                        let evs = per_owned.remove(p).unwrap_or_default();
                        Journal::from_events_shared(*p, evs, &shared)
                    })
                    .collect();
                (journals, recovered)
            };

            let server = build_server(journals, store, topology);

            // The read model now has every completed instance, so shed them from
            // hot engine state to bound memory. Each partition evicts its own.
            let mut evicted = 0usize;
            for handle in server.engine.all() {
                evicted += handle.with(|journal| journal.evict_completed()).await;
            }
            if evicted > 0 {
                tracing::info!("evicted {evicted} completed instance(s) from hot state");
            }

            if recovered {
                tracing::info!(
                    "recovered engine state by replaying journal at {}",
                    journal_path.display()
                );
            } else {
                tracing::info!("started a fresh journal at {}", journal_path.display());
            }
            server
        }
        None => {
            tracing::info!(
                "no journal configured; running in-memory (state is not persisted)"
            );
            let store = Arc::new(
                ReadStore::open(db_path.as_deref()).expect("open in-memory read store"),
            );
            let partitions = partition_count_from_env();
            let topology = cluster::Topology::from_env(partitions as u64);
            let journals: Vec<Journal> = if topology.is_single_node() {
                (0..partitions)
                    .map(|i| Journal::in_memory_partition(i as u64))
                    .collect()
            } else {
                topology
                    .local_partitions()
                    .iter()
                    .map(|p| Journal::in_memory_partition(*p))
                    .collect()
            };
            build_server(journals, store, topology)
        }
    };

    // Capture handles for the background tick before `server` is moved into the
    // router.
    let tick_engine = server.engine.clone();
    let tick_jobs_available = server.jobs_available.clone();
    let tick_dispatch_wake = server.dispatch_wake.clone();
    let idle_engine = server.engine.clone();
    let idle_activity = server.activity.clone();
    let idle_processing = server.processing.clone();
    // A server handle for the timer tick to route any cross-partition
    // subscription opens a fired timer advances a token into (no-op single-node).
    let tick_server = server.clone();

    // The unified bidirectional command stream (WebSocket) shares the engine via a
    // clone of `server` and a registry of connections; a single dispatcher pushes
    // jobs and the existing periodic tick reclaims expired leases.
    let cs_registry = command_stream::Registry::new();
    command_stream::spawn_dispatcher(server.clone(), cs_registry.clone());
    let cs_router = command_stream::router(server.clone(), cs_registry);

    // Clustered partition-0 owner: push the seeded/recovered deployment
    // definitions to every peer so the whole cluster can instantiate them,
    // retrying until each peer (which may still be booting) acknowledges.
    // No-op for a single-node cluster.
    server.spawn_seed_broadcast();

    // Env-gated (NANOBPMN_RAFT): bring up this node's per-partition Raft groups
    // over the command stream and form the ones it leads. No-op by default.
    server.spawn_raft_bootstrap();

    let mut app = nanobpm_gateway_rest::server::new::<ServerImpl, ServerImpl, (), ()>(server)
        .merge(cs_router)
        .route("/metrics", axum::routing::get(metrics_handler));

    if debug_rest_enabled() {
        app = app.layer(axum::middleware::from_fn(log_rest));
        tracing::info!("DEBUG_REST enabled: logging every REST request and response");
    }

    // Background "tick": drives the host clock into the engine so timers fire and
    // activation locks expire without an inbound request. Timer firing is durable
    // (journaled); lock expiry is volatile (not journaled). Wakes any long-polling
    // activateJobs when a tick produced events (a fired timer may create jobs).
    {
        let engine = tick_engine;
        let jobs_available = tick_jobs_available;
        let dispatch_wake = tick_dispatch_wake;
        let tick_server = tick_server;
        let multi_partition = engine.topology().num_partitions > 1;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                interval.tick().await;
                let now = now_millis();
                // Drive every partition's clock so timers fire and leases expire.
                // Fan out concurrently: each partition's tick is independent, so
                // running them in parallel keeps the sweep off the critical path
                // instead of serializing N engine round-trips every 500ms.
                let outcomes = futures_util::future::join_all(engine.all().iter().map(|handle| {
                    handle.with(move |journal| {
                        let (fired, _commit) = journal.trigger_timers(now);
                        let expired = journal.expire_jobs(now);
                        // Shed dormant instances to disk if hot RAM is over the
                        // high-water mark (cheap no-op below it / when unset).
                        journal.maybe_cold_spill();
                        // A fired timer may advance a token into an off-partition
                        // message catch: surface those follow-ups for routing.
                        let routable: Vec<Event> = if multi_partition {
                            fired
                                .iter()
                                .filter(|e| {
                                    matches!(
                                        e,
                                        Event::MessageSubscriptionOpening { .. }
                                            | Event::RemoteMessageCorrelation { .. }
                                            | Event::MessageSubscriptionClosing { .. }
                                            | Event::StartInstanceDispatched { .. }
                                    )
                                })
                                .cloned()
                                .collect()
                        } else {
                            Vec::new()
                        };
                        // Either a fired timer (may create a job) or a reclaimed
                        // job lease (frees a job for redelivery) means there is
                        // pushable work — wake dispatch instead of waiting for
                        // its own backstop tick.
                        (!fired.is_empty() || !expired.is_empty(), routable)
                    })
                }))
                .await;
                let mut produced = false;
                let mut routable: Vec<Event> = Vec::new();
                for (p, mut r) in outcomes {
                    produced |= p;
                    routable.append(&mut r);
                }
                if !routable.is_empty() {
                    tick_server.drive_subscription_routing(routable).await;
                }
                if produced {
                    jobs_available.notify_waiters();
                    dispatch_wake.notify_one();
                }
            }
        });
    }

    // Idle-purge tick: when the engine goes quiescent after a burst, compact the
    // hot-state maps (which eviction left at peak capacity) and force the
    // allocator to return the freed pages to the OS, so an idle server's resident
    // footprint falls back down instead of pinning its peak. Gated on
    // NANOBPMN_IDLE_PURGE_MS (default 5000; set 0 to disable). It fires once per
    // active→idle transition, never while work is flowing.
    if let Some(quiescence) = idle_purge_quiescence_from_env() {
        let engine = idle_engine;
        let activity = idle_activity;
        let processing = idle_processing;
        tokio::spawn(async move {
            let check = std::time::Duration::from_millis(1000);
            let needed_idle_checks = quiescence.as_millis().div_ceil(1000).max(1) as u32;
            let mut interval = tokio::time::interval(check);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last_activity = activity.load(Ordering::Relaxed);
            let mut idle_checks = 0u32;
            let mut purged = true; // nothing to reclaim until work has happened.
            loop {
                interval.tick().await;
                let now_activity = activity.load(Ordering::Relaxed);
                let busy = now_activity != last_activity || processing.load(Ordering::Relaxed) > 0;
                last_activity = now_activity;
                if busy {
                    idle_checks = 0;
                    purged = false;
                    continue;
                }
                idle_checks += 1;
                if idle_checks < needed_idle_checks || purged {
                    continue;
                }
                // Quiescent and not yet reclaimed since the last burst: compact +
                // purge, exactly once until activity resumes.
                let before = memory::resident_bytes();
                for handle in engine.all() {
                    handle.with(|journal| journal.shrink()).await;
                }
                let purged_ok = memory::purge();
                purged = true;
                if purged_ok {
                    match (before, memory::resident_bytes()) {
                        (Some(before), Some(after)) if before > after => {
                            tracing::info!(
                                "idle: compacted hot state, returned {:.1} MiB to the OS \
                                 ({:.1} -> {:.1} MiB resident)",
                                (before - after) as f64 / (1024.0 * 1024.0),
                                before as f64 / (1024.0 * 1024.0),
                                after as f64 / (1024.0 * 1024.0),
                            );
                        }
                        _ => tracing::debug!("idle: compacted hot state and purged allocator"),
                    }
                }
            }
        });
    }

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));

    // The actual bound port (may differ from `port` when `PORT=0`, i.e. the OS
    // assigns a free one). Print it on stdout so a supervising process (the e2e
    // harness) can learn it without racing on a pre-reserved port.
    let local_port = listener
        .local_addr()
        .map(|a| a.port())
        .unwrap_or(port);
    println!("LISTENING_PORT={local_port}");
    let _ = std::io::Write::flush(&mut std::io::stdout());

    tracing::info!(
        "NanoBPM gateway REST stub server listening on http://{addr}{}",
        nanobpm_gateway_rest::BASE_PATH
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}

/// Resolves the journal and read-model database paths from the environment.
///
/// - `NANOBPMN_DATA_DIR=<dir>` co-locates both under one directory:
///   `<dir>/journal.jsonl` and `<dir>/read-model.sqlite`. The directory is
///   created if absent, but only a *single* missing level (its parent must
///   already exist) — see [`ensure_data_dir`].
/// - Otherwise `NANOBPMN_JOURNAL=<file>` (back-compat) selects the journal; the
///   database is `NANOBPMN_READ_DB` if set, else a sibling `read-model.sqlite`
///   next to the journal. The journal's parent directory is subject to the same
///   one-level-deep policy.
/// - With neither set, both are `None`: an in-memory journal and an in-memory
///   (`:memory:`) read store (nothing is persisted).
fn resolve_data_paths() -> (Option<PathBuf>, Option<PathBuf>) {
    if let Ok(dir) = std::env::var("NANOBPMN_DATA_DIR")
        && !dir.is_empty()
    {
        let dir = PathBuf::from(dir);
        if let Err(e) = ensure_data_dir(&dir) {
            panic!("{e}");
        }
        return (
            Some(dir.join("journal.jsonl")),
            Some(dir.join("read-model.sqlite")),
        );
    }

    match std::env::var("NANOBPMN_JOURNAL") {
        Ok(path) if !path.is_empty() => {
            let journal = PathBuf::from(path);
            // The journal lives in a directory; hold it to the same policy so a
            // mistyped path fails loudly instead of erroring out deeper in
            // (journal open / read-model export) with a confusing message.
            if let Some(parent) = journal.parent()
                && !parent.as_os_str().is_empty()
                && let Err(e) = ensure_data_dir(parent)
            {
                panic!("{e}");
            }
            let db = match std::env::var("NANOBPMN_READ_DB") {
                Ok(db) if !db.is_empty() => PathBuf::from(db),
                _ => journal.with_file_name("read-model.sqlite"),
            };
            (Some(journal), Some(db))
        }
        _ => (None, None),
    }
}

/// Number of engine partitions to run, from `NANOBPMN_PARTITIONS`.
///
/// Defaults to 1 (single-writer, today's behavior exactly — partition 0 mints
/// keys `1,2,3…` for zero regression). Values are clamped to `[1, MAX]` where
/// `MAX = MAX_PARTITION_ID + 1` (partition ids are 0-based, so the highest id
/// `N-1` must be `<= MAX_PARTITION_ID`). Unset/unparseable => 1.
fn partition_count_from_env() -> usize {
    let max = MAX_PARTITION_ID as usize + 1;
    match std::env::var("NANOBPMN_PARTITIONS") {
        Ok(v) => match v.trim().parse::<usize>() {
            Ok(n) if n >= 1 => n.min(max),
            _ => 1,
        },
        Err(_) => 1,
    }
}

/// Ensures `dir` is usable as a data directory: it must already exist as a
/// directory, or be creatable as a *single* new level beneath an already
/// existing parent.
///
/// This deliberately does **not** use `create_dir_all`: silently materialising
/// an arbitrarily deep path turns a typo'd `NANOBPMN_DATA_DIR` into a brand-new
/// tree in an unexpected place, which then surfaces much later as a baffling
/// failure (e.g. a read-model export landing somewhere unwritable and logging
/// "attempt to write a readonly database" on every batch). Requiring the parent
/// to exist makes such mistakes fail fast and legibly at startup.
fn ensure_data_dir(dir: &Path) -> Result<(), String> {
    match std::fs::metadata(dir) {
        Ok(meta) if meta.is_dir() => Ok(()),
        Ok(_) => Err(format!(
            "data dir {} exists but is not a directory",
            dir.display()
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let parent_exists = match dir.parent() {
                // Relative leaf (e.g. "data"): the parent is the current
                // working directory, which exists by definition.
                Some(parent) if parent.as_os_str().is_empty() => true,
                Some(parent) => parent.is_dir(),
                None => false,
            };
            if !parent_exists {
                return Err(format!(
                    "data dir {} does not exist and neither does its parent; create the parent \
                     directory first (only a single new directory level is created automatically)",
                    dir.display()
                ));
            }
            std::fs::create_dir(dir)
                .map_err(|e| format!("failed to create data dir {}: {e}", dir.display()))
        }
        Err(e) => Err(format!(
            "failed to access data dir {}: {e}",
            dir.display()
        )),
    }
}

#[cfg(test)]
mod clustered_startup_tests {
    use super::*;

    /// Builds an in-memory clustered `ServerImpl` for `node_id` of a 2-node,
    /// 4-partition cluster (node 0 owns partitions 0 & 2; node 1 owns 1 & 3),
    /// exactly as `main()` would on that node.
    fn clustered_node(node_id: u32) -> ServerImpl {
        let topology = cluster::Topology {
            node_id,
            peers: vec!["http://n0".into(), "http://n1".into()],
            num_partitions: 4,
            replication_factor: 1,
        };
        let journals: Vec<Journal> = topology
            .local_partitions()
            .iter()
            .map(|p| Journal::in_memory_partition(*p))
            .collect();
        let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
        build_server(journals, store, topology)
    }

    #[test]
    fn clustered_node_owns_only_its_partition_subset() {
        let node0 = clustered_node(0);
        let node1 = clustered_node(1);
        // Each node spawns an engine actor only for the two partitions it owns.
        assert_eq!(node0.engine.all().len(), 2);
        assert_eq!(node1.engine.all().len(), 2);
        // The cluster is not collapsed to the single-partition fast path.
        assert!(!node0.engine.is_single());
        assert_eq!(node0.engine.len(), 4); // total cluster partitions
    }

    #[test]
    fn for_create_stays_on_owned_partitions() {
        // `createProcessInstance` on a clustered node must mint keys only on the
        // partitions that node owns — never on a peer's partition (which it holds
        // no engine actor for). Round-robining `for_create` many times must only
        // ever return one of this node's own handles.
        let node0 = clustered_node(0);
        let owned: Vec<*const EngineHandle> =
            node0.engine.all().iter().map(|h| h as *const _).collect();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..16 {
            let h = node0.engine.for_create() as *const EngineHandle;
            assert!(
                owned.contains(&h),
                "for_create returned a handle this node does not own"
            );
            seen.insert(h);
        }
        // Over many calls it must exercise BOTH owned partitions (round-robin),
        // not collapse onto one.
        assert_eq!(seen.len(), 2, "for_create should spread across both owned partitions");
    }

    /// The `ProcessDeployed` events the deployment-partition owner broadcasts: the
    /// definitions registered on a fresh single-node server (which seeds `demo`).
    async fn demo_deployment_events() -> Vec<Event> {
        let owner = ServerImpl::default();
        owner
            .engine
            .deploy_partition()
            .with(|journal| deployment_replication_events(journal))
            .await
    }

    #[tokio::test]
    async fn peer_cannot_create_until_deployment_is_installed() {
        // A peer owns no deployment partition, so it has no demo definition and a
        // create is rejected — the live-observed node-1 create→400.
        let node1 = clustered_node(1);
        assert!(
            node1
                .create_for_stream(Some("demo".into()), None, Default::default())
                .await
                .is_err(),
            "peer must reject a create before any deployment reaches it"
        );

        // Installing the broadcast deployment makes the peer able to create.
        let events = demo_deployment_events().await;
        assert!(!events.is_empty());
        node1.install_replicated_deployment(events).await;

        let (key, completed) = node1
            .create_for_stream(Some("demo".into()), None, Default::default())
            .await
            .expect("peer creates after the deployment is installed");
        assert!(!completed, "the demo parks at its service task");
        // The instance lives on one of node 1's owned partitions (1 or 3) — the
        // definition is partition-agnostic but the instance is minted locally.
        let p = nanobpmn_engine_core::partition_of(key);
        assert!(p == 1 || p == 3, "instance must live on an owned partition, got {p}");
    }

    #[tokio::test]
    async fn deploy_broadcast_over_the_wire_reaches_a_peer() {
        // Serve a real peer node (node 1) on an ephemeral command-stream endpoint.
        let node1 = clustered_node(1);
        let registry = command_stream::Registry::new();
        command_stream::spawn_dispatcher(node1.clone(), registry.clone());
        let app = command_stream::router(node1.clone(), registry);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let port = listener.local_addr().expect("local addr").port();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let node1_url = format!("http://127.0.0.1:{port}");

        // Build the deployment-partition owner (node 0) pointing at the served
        // peer. It seeds `demo` on partition 0 and broadcasts it to node 1.
        let topology = cluster::Topology {
            node_id: 0,
            peers: vec!["http://unused".into(), node1_url],
            num_partitions: 4,
            replication_factor: 1,
        };
        let journals: Vec<Journal> = topology
            .local_partitions()
            .iter()
            .map(|p| Journal::in_memory_partition(*p))
            .collect();
        let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
        let node0 = build_server(journals, store, topology);

        // The peer has no definition yet.
        assert!(
            node1
                .create_for_stream(Some("demo".into()), None, Default::default())
                .await
                .is_err()
        );

        // Broadcast node 0's seeded deployment to its peers over the command stream.
        let events = node0
            .engine
            .deploy_partition()
            .with(|journal| deployment_replication_events(journal))
            .await;
        node0.broadcast_deployment(&Arc::new(events)).await;

        // The peer can now create the demo (the install rode the wire end-to-end).
        let (_, completed) = node1
            .create_for_stream(Some("demo".into()), None, Default::default())
            .await
            .expect("peer creates after the wire broadcast");
        assert!(!completed);
    }

    /// Serves a node's command-stream endpoint on an ephemeral port and returns
    /// its HTTP base URL, so a peer can forward to it exactly as in a cluster.
    async fn serve_node(server: &ServerImpl) -> String {
        let registry = command_stream::Registry::new();
        command_stream::spawn_dispatcher(server.clone(), registry.clone());
        let app = command_stream::router(server.clone(), registry);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let port = listener.local_addr().expect("local addr").port();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://127.0.0.1:{port}")
    }

    #[tokio::test]
    async fn published_message_fans_out_to_the_owners_message_start_subscription() {
        // The deployment-partition owner (node 0) holds every message-start
        // subscription. A message published at a DIFFERENT gateway (node 1) must
        // still reach it, creating an instance — the cross-node fan-out.
        let node0 = clustered_node(0);
        let proc = ProcessBuilder::new("order-flow")
            .message_start_event("start", "order-placed")
            .end_event("end")
            .connect("start", "end")
            .build()
            .expect("valid message-start process");
        let mut names = std::collections::HashMap::new();
        names.insert("order-flow".to_string(), "order.bpmn".to_string());
        node0
            .deploy_resources_locally(vec![proc], &names, "<default>")
            .await
            .expect("deploy message-start process on the owner");

        let node0_url = serve_node(&node0).await;

        // node 1 is the gateway the client hits; it points at the served owner.
        let topology = cluster::Topology {
            node_id: 1,
            peers: vec![node0_url, "http://unused".into()],
            num_partitions: 4,
            replication_factor: 1,
        };
        let journals: Vec<Journal> = topology
            .local_partitions()
            .iter()
            .map(|p| Journal::in_memory_partition(*p))
            .collect();
        let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
        let node1 = build_server(journals, store, topology);

        // Publishing at node 1 fans out to node 0, whose message-start
        // subscription fires and creates a new instance on partition 0.
        let (_message_key, instance) = node1
            .correlate_message_cluster("order-placed".into(), String::new(), None)
            .await;
        let instance = instance.expect("message-start must create an instance via fan-out");
        assert_eq!(
            nanobpmn_engine_core::partition_of(instance),
            0,
            "the message-start instance lives on the owner's partition 0"
        );
    }

    #[tokio::test]
    async fn cross_node_message_catch_routes_open_and_correlation_over_the_wire() {
        // The full canonical-placement protocol across a NODE boundary:
        //  - the instance is created on node 0 (instance owner),
        //  - its correlation key hashes to a partition owned by node 1 (the
        //    subscription's canonical home), so the Opening is routed node0->node1,
        //  - the publish, fanned to node 1, correlates the canonical sub and emits
        //    a RemoteMessageCorrelation whose continuation is routed node1->node0,
        //    advancing the parked token to completion.
        // Both nodes must serve each other, so we pre-bind both listeners, embed
        // each other's real URL, then serve.
        let l0 = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind node 0");
        let p0 = l0.local_addr().expect("addr0").port();
        let l1 = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind node 1");
        let p1 = l1.local_addr().expect("addr1").port();
        let peers = vec![
            format!("http://127.0.0.1:{p0}"),
            format!("http://127.0.0.1:{p1}"),
        ];

        let build_node = |node_id: u32| {
            let topology = cluster::Topology {
                node_id,
                peers: peers.clone(),
                num_partitions: 4,
                replication_factor: 1,
            };
            let journals: Vec<Journal> = topology
                .local_partitions()
                .iter()
                .map(|p| Journal::in_memory_partition(*p))
                .collect();
            let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
            build_server(journals, store, topology)
        };
        let node0 = build_node(0);
        let node1 = build_node(1);

        // Deploy the message-catch process on the deployment owner (node 0) and
        // replicate the definition to node 1 so it can mint instances too.
        let proc = ProcessBuilder::new("await-payment")
            .start_event("s")
            .message_intermediate_catch_event("await", "payment-received", "orderId")
            .end_event("e")
            .connect("s", "await")
            .connect("await", "e")
            .build()
            .expect("valid message-catch process");
        let mut names = std::collections::HashMap::new();
        names.insert("await-payment".to_string(), "await.bpmn".to_string());
        let (_result, events) = node0
            .deploy_resources_locally(vec![proc], &names, "<default>")
            .await
            .expect("deploy on the owner");
        node1.install_replicated_deployment(events.to_vec()).await;

        // Serve both nodes' command-stream endpoints on their pre-bound ports.
        for (server, listener) in [(node0.clone(), l0), (node1.clone(), l1)] {
            let registry = command_stream::Registry::new();
            command_stream::spawn_dispatcher(server.clone(), registry.clone());
            let app = command_stream::router(server.clone(), registry);
            tokio::spawn(async move {
                axum::serve(listener, app).await.ok();
            });
        }

        // A correlation key that hashes onto a node-1 partition (1 or 3), so the
        // canonical subscription is placed off the instance's node.
        let order = (0..)
            .map(|i| format!("ord-{i}"))
            .find(|k| {
                let p = nanobpmn_engine_core::subscription_partition(k, 4);
                p == 1 || p == 3
            })
            .expect("a key hashing to a node-1 partition exists");

        // Create the instance via node 0; it lands on a node-0 partition (0 or 2)
        // and parks at the catch. The create path routes the Opening to node 1.
        let mut variables = std::collections::HashMap::new();
        variables.insert("orderId".to_string(), Value::Str(order.clone()));
        let (instance_key, completed) = node0
            .create_for_stream(Some("await-payment".into()), None, variables)
            .await
            .expect("create on node 0");
        assert!(!completed, "the instance parks at the message catch");
        let p_inst = nanobpmn_engine_core::partition_of(instance_key);
        assert!(p_inst == 0 || p_inst == 2, "instance on a node-0 partition, got {p_inst}");

        // Publish at node 0: it has no local match (the canonical sub is on node
        // 1), so the publish fans to node 1, which correlates and routes the
        // continuation back to node 0 to advance the parked token.
        let (_message_key, correlated) = node0
            .correlate_message_cluster("payment-received".into(), order.clone(), None)
            .await;
        assert_eq!(
            correlated,
            Some(instance_key),
            "the cross-node publish correlates the parked instance"
        );

        // The token really advanced on node 0 (the instance owner): it completes.
        let (_vars, completed) = node0
            .await_completion_for_stream(instance_key, None, Some(2000))
            .await;
        assert!(
            completed,
            "the instance completes after the correlation is routed back over the wire"
        );
    }

    #[tokio::test]
    async fn message_start_dispatches_instances_across_the_node_boundary() {
        // Message-start subscriptions live only on the deploy owner (node 0). The
        // round-robin start dispatcher must spread the created instances over the
        // WHOLE cluster, routing a StartInstanceDispatched to node 1 (which owns
        // partitions 1 & 3) over the command stream so node 1 mints its share.
        let l0 = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind node 0");
        let p0 = l0.local_addr().expect("addr0").port();
        let l1 = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind node 1");
        let p1 = l1.local_addr().expect("addr1").port();
        let peers = vec![
            format!("http://127.0.0.1:{p0}"),
            format!("http://127.0.0.1:{p1}"),
        ];

        let build_node = |node_id: u32| {
            let topology = cluster::Topology {
                node_id,
                peers: peers.clone(),
                num_partitions: 4,
                replication_factor: 1,
            };
            let journals: Vec<Journal> = topology
                .local_partitions()
                .iter()
                .map(|p| Journal::in_memory_partition(*p))
                .collect();
            let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
            build_server(journals, store, topology)
        };
        let node0 = build_node(0);
        let node1 = build_node(1);

        // Deploy a message-start process (parks at a service task so the created
        // instances persist) on the owner and replicate the definition to node 1.
        let proc = ProcessBuilder::new("intake")
            .message_start_event("start", "order-placed")
            .service_task("work", "do-work")
            .end_event("end")
            .connect("start", "work")
            .connect("work", "end")
            .build()
            .expect("valid message-start process");
        let mut names = std::collections::HashMap::new();
        names.insert("intake".to_string(), "intake.bpmn".to_string());
        let (_result, events) = node0
            .deploy_resources_locally(vec![proc], &names, "<default>")
            .await
            .expect("deploy on the owner");
        node1.install_replicated_deployment(events.to_vec()).await;

        // Serve both nodes' command-stream endpoints on their pre-bound ports.
        for (server, listener) in [(node0.clone(), l0), (node1.clone(), l1)] {
            let registry = command_stream::Registry::new();
            command_stream::spawn_dispatcher(server.clone(), registry.clone());
            let app = command_stream::router(server.clone(), registry);
            tokio::spawn(async move {
                axum::serve(listener, app).await.ok();
            });
        }

        // Publish many distinctly-keyed messages at the owner; each fires the
        // message-start subscription on partition 0 and dispatches the created
        // instance round-robin across all four partitions (1 & 3 on node 1).
        for i in 0..16u32 {
            node0
                .correlate_message_cluster("order-placed".into(), format!("order-{i}"), None)
                .await;
        }

        let count_instances = |server: ServerImpl| async move {
            let mut total = 0usize;
            for handle in server.engine.all() {
                total += handle
                    .with(|engine| engine.engine().state().instances.len())
                    .await;
            }
            total
        };
        let on_node0 = count_instances(node0.clone()).await;
        let on_node1 = count_instances(node1.clone()).await;
        assert_eq!(
            on_node0 + on_node1,
            16,
            "every publish created exactly one instance (n0={on_node0}, n1={on_node1})"
        );
        assert!(
            on_node1 > 0,
            "the dispatcher must place some start instances on node 1 over the wire \
             (n0={on_node0}, n1={on_node1})"
        );
        assert!(
            on_node0 > 0,
            "the dispatcher must also keep some on node 0 (n0={on_node0}, n1={on_node1})"
        );
    }


    #[tokio::test]
    async fn complete_job_forwards_to_the_owning_peer() {
        // A worker's completeJob may land on ANY gateway. When the job's partition
        // is owned by a peer, the gateway forwards the completion over the command
        // stream; the peer applies it durably on its own partition and answers.
        //
        // node 0 owns partitions 0 & 2 and seeds the demo. Create an instance and
        // activate its service-task job there, so the job key lives on an
        // node-0-owned partition. node 1 (a different gateway) must forward the
        // completion back to node 0.
        let node0 = clustered_node(0);
        let (_instance, _) = node0
            .create_for_stream(Some("demo".into()), None, Default::default())
            .await
            .expect("owner creates the demo instance");
        let jobs = node0
            .activate_for_stream("demo-work", "w", 10, 60_000, None)
            .await;
        assert_eq!(jobs.len(), 1, "the demo parks exactly one service-task job");
        let job_key_str = jobs[0].job_key.0.clone();
        let job_key: u64 = job_key_str.parse().expect("job key is numeric");

        let node0_url = serve_node(&node0).await;

        // node 1 is the gateway the worker hits; it points at the served owner.
        let topology = cluster::Topology {
            node_id: 1,
            peers: vec![node0_url, "http://unused".into()],
            num_partitions: 4,
            replication_factor: 1,
        };
        let journals: Vec<Journal> = topology
            .local_partitions()
            .iter()
            .map(|p| Journal::in_memory_partition(*p))
            .collect();
        let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
        let node1 = build_server(journals, store, topology);

        // The job lives on a partition node 1 does NOT own — it must forward.
        let owner = node1
            .remote_owner_of(job_key)
            .expect("the job's partition is owned by a peer, not node 1");
        assert_eq!(owner, 0, "node 0 owns the job's partition");

        // Forward the completion to node 0 over the wire and map its answer back.
        use apis::job::CompleteJobResponse as R;
        let resp = node1.forward_complete_job(owner, job_key, None).await;
        assert!(
            matches!(resp, R::Status204_TheJobWasCompletedSuccessfully),
            "the forwarded completion should succeed (204)"
        );

        // The completion really mutated node 0's state: completing the same job
        // again is rejected (it is no longer an activated job).
        let again = node1.forward_complete_job(owner, job_key, None).await;
        assert!(
            !matches!(again, R::Status204_TheJobWasCompletedSuccessfully),
            "re-completing an already-completed job must not return 204, got a success"
        );
    }

    #[tokio::test]
    async fn cancel_instance_forwards_to_the_owning_peer() {
        // cancelProcessInstance by key must reach the node owning the instance's
        // partition, even when submitted to a different gateway.
        let node0 = clustered_node(0);
        let (instance, _) = node0
            .create_for_stream(Some("demo".into()), None, Default::default())
            .await
            .expect("owner creates the demo instance");
        let instance_key: u64 = instance;

        let node0_url = serve_node(&node0).await;
        let topology = cluster::Topology {
            node_id: 1,
            peers: vec![node0_url, "http://unused".into()],
            num_partitions: 4,
            replication_factor: 1,
        };
        let journals: Vec<Journal> = topology
            .local_partitions()
            .iter()
            .map(|p| Journal::in_memory_partition(*p))
            .collect();
        let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
        let node1 = build_server(journals, store, topology);

        let owner = node1
            .remote_owner_of(instance_key)
            .expect("the instance's partition is owned by a peer");
        use apis::process_instance::CancelProcessInstanceResponse as R;
        let resp = node1.forward_cancel_instance(owner, instance_key).await;
        assert!(
            matches!(resp, R::Status204_TheProcessInstanceIsCanceled),
            "the forwarded cancel should succeed (204)"
        );
        // Cancelling again is rejected (the instance is already terminal),
        // proving the first cancel took effect on the owner.
        let again = node1.forward_cancel_instance(owner, instance_key).await;
        assert!(
            matches!(again, R::Status404_TheProcessInstanceIsNotFound(_)),
            "re-cancelling a terminated instance must 404"
        );
    }

    #[tokio::test]
    async fn get_process_instance_forwards_to_the_owning_peer() {
        // A GET-by-key read for a remote-owned instance must be answered by the
        // owner: each node only projects its OWN partitions into its read model,
        // so without forwarding the non-owning gateway would 404.
        let node0 = clustered_node(0);
        let (instance, _) = node0
            .create_for_stream(Some("demo".into()), None, Default::default())
            .await
            .expect("owner creates the demo instance");

        let node0_url = serve_node(&node0).await;
        let topology = cluster::Topology {
            node_id: 1,
            peers: vec![node0_url, "http://unused".into()],
            num_partitions: 4,
            replication_factor: 1,
        };
        let journals: Vec<Journal> = topology
            .local_partitions()
            .iter()
            .map(|p| Journal::in_memory_partition(*p))
            .collect();
        let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
        let node1 = build_server(journals, store, topology);

        assert_eq!(
            node1.remote_owner_of(instance),
            Some(0),
            "node 0 owns the instance's partition"
        );

        use apis::process_instance::GetProcessInstanceResponse as R;
        let path = models::GetProcessInstancePathParams {
            process_instance_key: instance.to_string(),
        };
        match node1.get_process_instance_impl(&path).await.unwrap() {
            R::Status200_TheProcessInstanceIsSuccessfullyReturned(r) => {
                assert_eq!(
                    r.process_instance_key.0,
                    instance.to_string(),
                    "the forwarded read returns the owner's instance"
                );
            }
            other => panic!("expected a forwarded 200, got {other:?}"),
        }

        // A genuinely-unknown remote key (partition 0, owned by node 0) still
        // 404s through the forward — proving the forward, not a local hit.
        let missing = models::GetProcessInstancePathParams {
            process_instance_key: "1".to_string(),
        };
        match node1.get_process_instance_impl(&missing).await.unwrap() {
            R::Status404_TheProcessInstanceWithTheGivenKeyWasNotFound(_) => {}
            other => panic!("expected a forwarded 404 for an unknown remote key, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn assign_user_task_forwards_to_the_owning_peer() {
        // A user-task by-key mutation submitted to ANY gateway forwards to the
        // node owning the task's partition, which re-runs the mutation locally.
        let node0 = clustered_node(0);
        let proc = ProcessBuilder::new("review")
            .start_event("start")
            .user_task("task")
            .end_event("end")
            .connect("start", "task")
            .connect("task", "end")
            .build()
            .expect("valid user-task process");
        let mut names = std::collections::HashMap::new();
        names.insert("review".to_string(), "review.bpmn".to_string());
        node0
            .deploy_resources_locally(vec![proc], &names, "<default>")
            .await
            .expect("deploy the user-task process on the owner");
        let (instance, _) = node0
            .create_for_stream(Some("review".into()), None, Default::default())
            .await
            .expect("owner creates the review instance");

        // The read model projects asynchronously off the commit; poll briefly
        // for the parked user task to avoid a projection-timing race.
        let mut task_key = None;
        for _ in 0..200 {
            if let Some(t) = node0
                .store
                .user_tasks()
                .iter()
                .find(|t| t.instance_key == instance)
            {
                task_key = Some(t.key);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let task_key = task_key.expect("the instance parks a user task");

        let node0_url = serve_node(&node0).await;
        let topology = cluster::Topology {
            node_id: 1,
            peers: vec![node0_url, "http://unused".into()],
            num_partitions: 4,
            replication_factor: 1,
        };
        let journals: Vec<Journal> = topology
            .local_partitions()
            .iter()
            .map(|p| Journal::in_memory_partition(*p))
            .collect();
        let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
        let node1 = build_server(journals, store, topology);

        assert_eq!(
            node1.remote_owner_of(task_key),
            Some(0),
            "node 0 owns the user task's partition"
        );

        use apis::user_task::AssignUserTaskResponse as R;
        let path = models::AssignUserTaskPathParams {
            user_task_key: task_key.to_string(),
        };
        let body = models::UserTaskAssignmentRequest {
            assignee: Some("alice".into()),
            allow_override: None,
            action: None,
        };
        let resp = node1.assign_user_task_impl(&path, &body).await.unwrap();
        assert!(
            matches!(resp, R::Status204_TheUserTask),
            "the forwarded assign should succeed (204)"
        );

        // The assign really mutated node 0's state: re-assigning with
        // allowOverride=false is now rejected as already-assigned (409).
        let body_no_override = models::UserTaskAssignmentRequest {
            assignee: Some("bob".into()),
            allow_override: Some(types::Nullable::Present(false)),
            action: None,
        };
        let again = node1
            .assign_user_task_impl(&path, &body_no_override)
            .await
            .unwrap();
        assert!(
            matches!(
                again,
                R::Status409_TheUserTaskWithTheGivenKeyIsInTheWrongStateCurrently(_)
            ),
            "re-assigning an assigned task without override must 409 (proves the forward took effect)"
        );
    }

    #[tokio::test]
    async fn topology_reports_every_node_and_its_partitions() {
        // A 2-node, 4-partition cluster: ownership is p % num_nodes, so node 0
        // owns partitions 0 & 2 and node 1 owns 1 & 3. The topology response must
        // surface BOTH brokers, each listing only its own partitions (1-based).
        let node0 = clustered_node(0);
        use apis::cluster::GetTopologyResponse as Resp;
        let t = match node0.get_topology_impl().await.unwrap() {
            Resp::Status200_ObtainsTheCurrentTopologyOfTheClusterTheGatewayIsPartOf(t) => t,
            other => panic!("expected 200 topology, got {other:?}"),
        };

        assert_eq!(t.cluster_size, 2, "two nodes");
        assert_eq!(t.partitions_count, 4, "four partitions cluster-wide");
        assert_eq!(t.replication_factor, 1, "stage 2 is still RF=1");
        assert_eq!(t.brokers.len(), 2, "one broker per node");

        let mut by_node: std::collections::HashMap<i32, Vec<i32>> =
            std::collections::HashMap::new();
        for b in &t.brokers {
            assert_eq!(b.partitions.iter().filter(|p| p.role != "leader").count(), 0);
            by_node.insert(
                b.node_id,
                b.partitions.iter().map(|p| p.partition_id).collect(),
            );
        }
        // 1-based partition ids: node 0 owns internal {0,2} -> {1,3}; node 1 {1,3} -> {2,4}.
        assert_eq!(by_node.get(&0), Some(&vec![1, 3]), "node 0 owns partitions 1 & 3 (1-based)");
        assert_eq!(by_node.get(&1), Some(&vec![2, 4]), "node 1 owns partitions 2 & 4 (1-based)");
    }

    #[test]
    fn parse_host_port_extracts_authority() {
        assert_eq!(
            parse_host_port("http://10.0.0.1:8080"),
            Some(("10.0.0.1".to_string(), 8080))
        );
        assert_eq!(
            parse_host_port("https://node-2.svc:9000/v2"),
            Some(("node-2.svc".to_string(), 9000))
        );
        assert_eq!(
            parse_host_port("127.0.0.1:7000"),
            Some(("127.0.0.1".to_string(), 7000))
        );
        // No usable host/port (single-node self address) -> None, caller falls back.
        assert_eq!(parse_host_port(""), None);
        assert_eq!(parse_host_port("http://"), None);
    }

    #[test]
    fn next_create_placement_spreads_across_the_cluster() {
        // node 0 of a 2-node, 4-partition cluster owns partitions 0 & 2; node 1
        // owns 1 & 3. Cluster-wide placement round-robins over ALL four
        // partitions, so half of node 0's placements are local (None) and half
        // are remote, owned by node 1 (Some(1)).
        let node0 = clustered_node(0);
        let mut local = 0;
        let mut remote_to_1 = 0;
        for _ in 0..8 {
            match node0.engine.next_create_placement() {
                None => local += 1,
                Some(1) => remote_to_1 += 1,
                Some(other) => panic!("unexpected remote placement to node {other}"),
            }
        }
        assert_eq!(local, 4, "half of 8 placements (partitions 0,2) are local");
        assert_eq!(remote_to_1, 4, "half (partitions 1,3) forward to node 1");
    }

    #[test]
    fn single_node_create_placement_is_always_local() {
        // A single-node cluster owns every partition, so placement never forwards
        // — the create path stays byte-identical to the pre-cluster fast path.
        let solo = ServerImpl::default();
        for _ in 0..16 {
            assert!(
                solo.engine.next_create_placement().is_none(),
                "single node must always place creates locally"
            );
        }
    }

    #[tokio::test]
    async fn create_forwards_to_a_peer_partition() {
        // A create whose cluster placement lands on a peer's partition is
        // forwarded to that peer, which mints the instance on one of ITS OWN
        // partitions and returns the full result. node 1 owns partitions 1 & 3.
        let node1 = clustered_node(1);
        // node 1 owns no deployment partition, so install the demo over the wire
        // path (mirrors the centralized broadcast) before it can create.
        node1.install_replicated_deployment(demo_deployment_events().await).await;
        let node1_url = serve_node(&node1).await;

        // node 0 is the gateway the client hits; it points at the served node 1.
        let topology = cluster::Topology {
            node_id: 0,
            peers: vec!["http://unused".into(), node1_url],
            num_partitions: 4,
            replication_factor: 1,
        };
        let journals: Vec<Journal> = topology
            .local_partitions()
            .iter()
            .map(|p| Journal::in_memory_partition(*p))
            .collect();
        let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
        let node0 = build_server(journals, store, topology);

        use apis::process_instance::CreateProcessInstanceResponse as R;
        let resp = node0
            .forward_create(1, Some("demo".into()), None, None, vec![], None, false, None, None)
            .await;
        let result = match resp {
            R::Status200_TheProcessInstanceWasCreated(r) => r,
            other => panic!("expected a forwarded 200 create, got {other:?}"),
        };
        let instance_key: u64 = result.process_instance_key.0.parse().expect("numeric key");
        let p = nanobpmn_engine_core::partition_of(instance_key);
        assert!(
            p == 1 || p == 3,
            "the forwarded instance must live on a node-1-owned partition (1 or 3), got {p}"
        );
    }

    #[tokio::test]
    async fn worker_pulls_and_completes_a_peer_owned_job() {
        // Job aggregation: a worker attached to one gateway is fed jobs from a
        // peer's partitions, and its completion is routed back to that peer.
        //
        // node 0 owns partitions 0 & 2, seeds the demo, and parks a `demo-work`
        // job on one of its partitions. node 1 (a different gateway) pulls that
        // job over the command stream (activate_from_peer) and completes it via
        // the stream-completion forward.
        let node0 = clustered_node(0);
        let (_instance, _) = node0
            .create_for_stream(Some("demo".into()), None, Default::default())
            .await
            .expect("owner creates the demo instance");
        let node0_url = serve_node(&node0).await;

        let topology = cluster::Topology {
            node_id: 1,
            peers: vec![node0_url, "http://unused".into()],
            num_partitions: 4,
            replication_factor: 1,
        };
        let journals: Vec<Journal> = topology
            .local_partitions()
            .iter()
            .map(|p| Journal::in_memory_partition(*p))
            .collect();
        let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
        let node1 = build_server(journals, store, topology);

        // node 1 owns no demo job locally; it must pull from node 0.
        let local = node1.activate_for_stream("demo-work", "w", 10, 60_000, None).await;
        assert!(local.is_empty(), "node 1 owns no demo-work job of its own");

        let pulled = node1
            .activate_from_peer(0, "demo-work", "w", 10, 60_000, None)
            .await;
        assert_eq!(pulled.len(), 1, "node 1 pulls the peer's parked job");
        let job_key: u64 = pulled[0].job_key.0.parse().expect("numeric job key");
        let p = nanobpmn_engine_core::partition_of(job_key);
        assert!(p == 0 || p == 2, "the pulled job lives on a node-0 partition, got {p}");

        // The owner owns the job's partition, so the completion must forward.
        let owner = node1
            .remote_owner_of(job_key)
            .expect("the job's partition is owned by node 0");
        assert_eq!(owner, 0);
        let (status, _) = node1.forward_complete_job_stream(owner, job_key, None).await;
        assert!(is_ok_status(status), "forwarded completion succeeds, got {status}");

        // Re-completing the same job is rejected — proof it mutated node 0's state.
        let (again, _) = node1.forward_complete_job_stream(owner, job_key, None).await;
        assert!(!is_ok_status(again), "re-completing must not succeed, got {again}");
    }

    #[test]
    fn single_node_has_no_peers_to_aggregate() {
        // A single-node cluster owns every partition, so job aggregation finds no
        // peers and the dispatcher's fan-out loop never runs (zero overhead).
        let solo = ServerImpl::default();
        assert!(
            solo.peer_nodes().is_empty(),
            "a single node must report no remote peers"
        );
    }

    #[test]
    fn clustered_node_reports_its_peers() {
        // node 0 of a 2-node cluster owns partitions 0 & 2; node 1 owns 1 & 3, so
        // node 0 reports exactly node 1 as its job-aggregation peer.
        let node0 = clustered_node(0);
        assert_eq!(node0.peer_nodes(), vec![1]);
    }

    #[tokio::test]
    async fn rest_activate_jobs_aggregates_a_peer_owned_job() {
        // The REST `activateJobs` long poll, like the stream dispatcher, tops up
        // from peers: a REST worker hitting one gateway is fed jobs that live on
        // another node's partitions.
        //
        // node 0 owns partitions 0 & 2, seeds the demo, and parks a `demo-work`
        // job. node 1 (a different gateway) owns no such job locally but must
        // aggregate it from node 0.
        let node0 = clustered_node(0);
        let (_instance, _) = node0
            .create_for_stream(Some("demo".into()), None, Default::default())
            .await
            .expect("owner creates the demo instance");
        let node0_url = serve_node(&node0).await;

        let topology = cluster::Topology {
            node_id: 1,
            peers: vec![node0_url, "http://unused".into()],
            num_partitions: 4,
            replication_factor: 1,
        };
        let journals: Vec<Journal> = topology
            .local_partitions()
            .iter()
            .map(|p| Journal::in_memory_partition(*p))
            .collect();
        let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
        let node1 = build_server(journals, store, topology);

        // Long polling disabled (requestTimeout < 0): the only way this returns a
        // job is by aggregating from node 0 in the same pass.
        let mut req = models::JobActivationRequest::new("demo-work".into(), 60_000, 10);
        req.request_timeout = Some(-1);
        let resp = node1.activate_jobs_impl(&req).await.expect("activate ok");
        use apis::job::ActivateJobsResponse as R;
        let jobs = match resp {
            R::Status200_TheListOfActivatedJobs(r) => r.jobs,
            other => panic!("expected 200 list, got {other:?}"),
        };
        assert_eq!(jobs.len(), 1, "node 1 aggregates the peer's parked job over REST");
        let job_key: u64 = jobs[0].job_key.0.parse().expect("numeric job key");
        let p = nanobpmn_engine_core::partition_of(job_key);
        assert!(p == 0 || p == 2, "the aggregated job lives on a node-0 partition, got {p}");
    }

    #[tokio::test]
    async fn raft_rpcs_replicate_a_command_across_two_nodes_over_the_command_stream() {
        // Proves the command-stream Raft binding: two served nodes host a 2-voter
        // Raft group for partition 0, carry AppendEntries/Vote RPCs over the real
        // command stream (PeerTransport -> ClientFrame::Raft -> dispatch_raft_rpc),
        // and a command proposed on the leader commits via quorum and applies on
        // BOTH nodes.
        use crate::raft::RaftPartition;
        use openraft::BasicNode;
        use std::collections::BTreeMap;

        // Pre-bind both listeners so each node can embed the other's real URL.
        let l0 = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind node 0");
        let p0 = l0.local_addr().expect("addr0").port();
        let l1 = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind node 1");
        let p1 = l1.local_addr().expect("addr1").port();
        let peers = vec![
            format!("http://127.0.0.1:{p0}"),
            format!("http://127.0.0.1:{p1}"),
        ];

        let build_node = |node_id: u32| {
            let topology = cluster::Topology {
                node_id,
                peers: peers.clone(),
                num_partitions: 4,
                replication_factor: 2,
            };
            let journals: Vec<Journal> = topology
                .local_partitions()
                .iter()
                .map(|p| Journal::in_memory_partition(*p))
                .collect();
            let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
            build_server(journals, store, topology)
        };
        let node0 = build_node(0);
        let node1 = build_node(1);

        // Serve both nodes' command-stream endpoints so the PeerTransport can reach
        // them. (Serve BEFORE bootstrapping voters so inbound RPCs are accepted as
        // soon as the group starts electing.)
        for (server, listener) in [(node0.clone(), l0), (node1.clone(), l1)] {
            let registry = command_stream::Registry::new();
            command_stream::spawn_dispatcher(server.clone(), registry.clone());
            let app = command_stream::router(server.clone(), registry);
            tokio::spawn(async move {
                axum::serve(listener, app).await.ok();
            });
        }

        // Host a Raft group for partition 0 on BOTH nodes, each driving its peer
        // over its own PeerTransport (the production command-stream carrier). A
        // voter must be able to RECEIVE AppendEntries before the group forms, so
        // construct + register every member first, then initialize once.
        let part0 = Arc::new(
            RaftPartition::bootstrap_member(
                0,
                0,
                EngineHandle::spawn(Journal::in_memory_partition(0), None),
                node0.raft_transport(),
            )
            .await
            .expect("boot raft member on node 0"),
        );
        node0.raft_registry().insert(part0.clone());

        let part1 = Arc::new(
            RaftPartition::bootstrap_member(
                1,
                0,
                EngineHandle::spawn(Journal::in_memory_partition(0), None),
                node1.raft_transport(),
            )
            .await
            .expect("boot raft member on node 1"),
        );
        node1.raft_registry().insert(part1.clone());

        // Form the {0,1} group on node 0 and let it win the initial election —
        // every Vote/AppendEntries to node 1 rides the real command stream.
        let mut members = BTreeMap::new();
        members.insert(0u64, BasicNode::new(peers[0].clone()));
        members.insert(1u64, BasicNode::new(peers[1].clone()));
        part0.initialize(members).await.expect("form group");

        let wait_until = |part: Arc<RaftPartition>, want: u64| async move {
            for _ in 0..300 {
                if part.raft.metrics().borrow().current_leader == Some(want) {
                    return true;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            false
        };
        assert!(
            wait_until(part0.clone(), 0).await,
            "node 0 should win the initial election over the wire"
        );

        // Deploy a process by proposing through the leader: with RF=2 this commits
        // only once node 1 acks the entry over the command stream.
        let proc = ProcessBuilder::new("raft-demo")
            .start_event("s")
            .end_event("e")
            .connect("s", "e")
            .build()
            .expect("valid process");
        let events = part0
            .propose(Command::DeployProcess(proc), 1_000)
            .await
            .expect("propose deploy via the leader");
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::ProcessDeployed { .. })),
            "the deploy committed via quorum and applied on the leader (got {events:?})"
        );

        let target = part0
            .raft
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0);
        assert!(target >= 1, "leader applied at least the deploy entry");

        // Node 1 converges to the same applied index — the entry replicated to and
        // applied on the follower purely over the command-stream Raft binding.
        let mut applied = false;
        for _ in 0..300 {
            let idx = part1
                .raft
                .metrics()
                .borrow()
                .last_applied
                .map(|l| l.index)
                .unwrap_or(0);
            if idx >= target {
                applied = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            applied,
            "node 1 did not apply up to index {target} over the command stream"
        );

        part0.raft.shutdown().await.expect("clean shutdown node 0");
        part1.raft.shutdown().await.expect("clean shutdown node 1");
    }

    #[tokio::test]
    async fn raft_bootstrap_forms_every_group_and_elects_leaders_across_two_nodes() {
        // L2a: the env-gated startup orchestration. Two served nodes (RF=2, so
        // each replicates all 4 partitions) run `raft_bootstrap`; afterwards every
        // partition must have formed its group and elected its owner as leader —
        // entirely over the command stream, with the multi-process startup race
        // (a peer still booting when initialize runs) absorbed by retry.
        use crate::raft::RaftPartition;

        let l0 = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind node 0");
        let p0 = l0.local_addr().expect("addr0").port();
        let l1 = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind node 1");
        let p1 = l1.local_addr().expect("addr1").port();
        let peers = vec![
            format!("http://127.0.0.1:{p0}"),
            format!("http://127.0.0.1:{p1}"),
        ];

        let build_node = |node_id: u32| {
            let topology = cluster::Topology {
                node_id,
                peers: peers.clone(),
                num_partitions: 4,
                replication_factor: 2,
            };
            let journals: Vec<Journal> = topology
                .local_partitions()
                .iter()
                .map(|p| Journal::in_memory_partition(*p))
                .collect();
            let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
            build_server(journals, store, topology)
        };
        let node0 = build_node(0);
        let node1 = build_node(1);

        for (server, listener) in [(node0.clone(), l0), (node1.clone(), l1)] {
            let registry = command_stream::Registry::new();
            command_stream::spawn_dispatcher(server.clone(), registry.clone());
            let app = command_stream::router(server.clone(), registry);
            tokio::spawn(async move {
                axum::serve(listener, app).await.ok();
            });
        }

        // Both nodes bootstrap concurrently — exactly as the two `main()`s would.
        tokio::join!(node0.raft_bootstrap(), node1.raft_bootstrap());

        // Each node hosts a member for all 4 partitions (RF=2, 2 nodes).
        for (node, who) in [(&node0, 0u32), (&node1, 1u32)] {
            for p in 0..4u64 {
                assert!(
                    node.raft_registry().get(p).is_some(),
                    "node {who} should host a member for partition {p}"
                );
            }
        }

        // Every partition's owner becomes its leader, observed via that owner's
        // hosted member — the whole 4-group cluster converged over the wire.
        let leader_of = |node: &ServerImpl, p: u64| -> Option<u64> {
            node.raft_registry()
                .get(p)
                .and_then(|part: Arc<RaftPartition>| part.raft.metrics().borrow().current_leader)
        };
        for p in 0..4u64 {
            let owner = (p % 2) as u64; // owner_of(p) for 2 nodes
            let owner_node = if owner == 0 { &node0 } else { &node1 };
            let mut elected = false;
            for _ in 0..400 {
                if leader_of(owner_node, p) == Some(owner) {
                    elected = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            assert!(
                elected,
                "partition {p} did not elect its owner (node {owner}) as leader"
            );
        }

        // Clean up every hosted group on both nodes.
        for node in [&node0, &node1] {
            for p in 0..4u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
    }

    #[tokio::test]
    async fn raft_routed_create_and_complete_commit_via_quorum_across_two_nodes() {
        // L2b: client writes route through the partition leader's Raft log and
        // commit via QUORUM (RF=2, so both nodes must ack). A create + complete
        // submitted to node 0 replicate to node 1 and apply on the leader's engine
        // actor (the same actor the server serves reads from). Node 1 hosts a
        // dedicated replica engine actor for node 0's partitions so it can apply
        // the replicated log.
        use crate::raft::RaftPartition;

        let l0 = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind node 0");
        let p0 = l0.local_addr().expect("addr0").port();
        let l1 = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind node 1");
        let p1 = l1.local_addr().expect("addr1").port();
        let peers = vec![
            format!("http://127.0.0.1:{p0}"),
            format!("http://127.0.0.1:{p1}"),
        ];

        let build_node = |node_id: u32| {
            let topology = cluster::Topology {
                node_id,
                peers: peers.clone(),
                num_partitions: 4,
                replication_factor: 2,
            };
            let journals: Vec<Journal> = topology
                .local_partitions()
                .iter()
                .map(|p| Journal::in_memory_partition(*p))
                .collect();
            let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
            build_server(journals, store, topology)
        };
        let node0 = build_node(0);
        let node1 = build_node(1);

        // Deploy a service-task process on the owner (parks at the job) and
        // replicate the definition to node 1's owned partitions.
        let proc = ProcessBuilder::new("intake")
            .start_event("start")
            .service_task("work", "do-work")
            .end_event("end")
            .connect("start", "work")
            .connect("work", "end")
            .build()
            .expect("valid process");
        let mut names = std::collections::HashMap::new();
        names.insert("intake".to_string(), "intake.bpmn".to_string());
        let (_r, events) = node0
            .deploy_resources_locally(vec![proc], &names, "<default>")
            .await
            .expect("deploy on the owner");
        node1.install_replicated_deployment(events.to_vec()).await;

        for (server, listener) in [(node0.clone(), l0), (node1.clone(), l1)] {
            let registry = command_stream::Registry::new();
            command_stream::spawn_dispatcher(server.clone(), registry.clone());
            let app = command_stream::router(server.clone(), registry);
            tokio::spawn(async move {
                axum::serve(listener, app).await.ok();
            });
        }

        // Both nodes bootstrap (building follower replica engine actors, seeded
        // with the deployment) and form every group over the command stream.
        tokio::join!(node0.raft_bootstrap(), node1.raft_bootstrap());

        // Wait until node 0 leads its owned partitions (0 & 2).
        let leads = |node: &ServerImpl, p: u64, who: u64| -> bool {
            node.raft_registry()
                .get(p)
                .and_then(|part: Arc<RaftPartition>| part.raft.metrics().borrow().current_leader)
                == Some(who)
        };
        for p in [0u64, 2] {
            let mut ok = false;
            for _ in 0..400 {
                if leads(&node0, p, 0) {
                    ok = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            assert!(ok, "node 0 must lead partition {p}");
        }

        // Drive a create through node 0. The create replicates to node 1 and
        // commits via quorum (the registry is populated, so the write path routes
        // through Raft — no env needed in tests).
        let create = node0
            .create_for_stream(Some("intake".into()), None, Default::default())
            .await;
        let (instance_key, completed) = match create {
            Ok(v) => v,
            Err(e) => {
                panic!("raft-routed create should commit via quorum, got {e:?}");
            }
        };
        assert!(!completed, "instance parks at the service task");
        let part = nanobpmn_engine_core::partition_of(instance_key);
        assert!(
            part == 0 || part == 2,
            "create lands on a node-0 partition (got {part})"
        );

        // The committed instance is materialized on the leader's engine actor.
        let present = node0
            .engine
            .local_for_partition(part)
            .expect("leader owns the partition")
            .with(move |journal| journal.engine().state().instances.contains_key(&instance_key))
            .await;
        assert!(present, "the committed instance is visible on the leader");

        // Activate the parked job locally, then complete it through the Raft log.
        let mut job_key = None;
        for _ in 0..50 {
            let jobs = node0
                .activate_for_stream("do-work", "w", 10, 60_000, None)
                .await;
            if let Some(j) = jobs.into_iter().next() {
                job_key = Some(j.job_key.0.parse::<u64>().expect("numeric job key"));
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let job_key = job_key.expect("the parked job activates on the leader");

        let commit = node0
            .complete_job_for_stream(job_key, Default::default())
            .await
            .expect("raft-routed complete commits via quorum");
        commit.wait().await;

        // Re-completing the same job is rejected THROUGH the Raft log, proving the
        // first completion mutated the leader's durable state via propose().
        let err = match node0
            .complete_job_for_stream(job_key, Default::default())
            .await
        {
            Ok(_) => panic!("re-complete of a completed job must be rejected"),
            Err(e) => e,
        };
        assert!(
            err.0 == 404 || err.0 == 409,
            "re-complete should be a 404/409, got {err:?}"
        );

        for node in [&node0, &node1] {
            for p in 0..4u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
    }
}

#[cfg(test)]
mod subscription_placement_tests {
    use super::*;

    /// A single node owning ALL 4 partitions (num_nodes = 1), so every partition
    /// is local and the host pump can route subscription open/correlate across
    /// them without a peer hop. This exercises the cross-partition placement
    /// machinery end-to-end inside one process.
    fn single_node_multi_partition() -> ServerImpl {
        let topology = cluster::Topology {
            node_id: 0,
            peers: vec!["http://n0".into()],
            num_partitions: 4,
            replication_factor: 1,
        };
        let journals: Vec<Journal> = topology
            .local_partitions()
            .iter()
            .map(|p| Journal::in_memory_partition(*p))
            .collect();
        let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
        build_server(journals, store, topology)
    }

    /// A process whose only wait state is a message intermediate catch keyed on
    /// `orderId` — the canonical subscription lives on `hash(orderId) % P`, which
    /// is generally a different partition than the instance.
    const MESSAGE_CATCH_BPMN: &str = r#"
      <bpmn:definitions
          xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="await-payment">
          <bpmn:startEvent id="s" />
          <bpmn:intermediateCatchEvent id="await">
            <bpmn:messageEventDefinition messageRef="Message_1" />
          </bpmn:intermediateCatchEvent>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="await" />
          <bpmn:sequenceFlow id="f1" sourceRef="await" targetRef="e" />
        </bpmn:process>
        <bpmn:message id="Message_1" name="payment-received">
          <bpmn:extensionElements>
            <zeebe:subscription correlationKey="=orderId" />
          </bpmn:extensionElements>
        </bpmn:message>
      </bpmn:definitions>"#;

    #[tokio::test]
    async fn cross_partition_message_catch_completes_via_the_pump() {
        let server = single_node_multi_partition();
        assert_eq!(server.engine.all().len(), 4, "node owns all 4 partitions");
        assert!(!server.engine.is_single());

        server
            .deploy_centralized(
                vec![("await-payment.bpmn".into(), MESSAGE_CATCH_BPMN.into())],
                "<default>".into(),
            )
            .await
            .expect("the message-catch process deploys onto every partition");

        // Create instances with unique correlation keys until one lands on a
        // partition DIFFERENT from where its subscription is canonically placed —
        // the cross-partition case the host pump must stitch together. Each
        // instance carries a unique key, so a later publish matches exactly one.
        let mut chosen: Option<(String, nanobpmn_engine_core::Key)> = None;
        for i in 0..64u32 {
            let order = format!("order-{i}");
            let mut variables = std::collections::HashMap::new();
            variables.insert("orderId".to_string(), Value::Str(order.clone()));
            let (key, completed) = server
                .create_for_stream(Some("await-payment".into()), None, variables)
                .await
                .expect("create succeeds");
            assert!(!completed, "the instance parks at the message catch");
            let p_inst = nanobpmn_engine_core::partition_of(key);
            let p_sub = nanobpmn_engine_core::subscription_partition(&order, 4);
            if p_sub != p_inst {
                chosen = Some((order, key));
                break;
            }
        }
        let (order, instance_key) =
            chosen.expect("a cross-partition placement appears within 64 creates");

        // Publishing the message must (a) reach the subscription partition where
        // the pump routed the Open, match the canonical sub, and (b) route the
        // resulting correlation back to the instance partition to advance and
        // complete the parked token.
        let (_message_key, correlated) = server
            .correlate_message_local(
                "payment-received".into(),
                order.clone(),
                std::collections::HashMap::new(),
            )
            .await;
        assert_eq!(
            correlated,
            Some(instance_key),
            "the publish correlates the cross-partition parked instance"
        );

        let (_vars, completed) = server
            .await_completion_for_stream(instance_key, None, Some(2000))
            .await;
        assert!(
            completed,
            "the instance completes after the cross-partition correlation is routed back"
        );
    }

    #[tokio::test]
    async fn cross_partition_cancel_disarms_the_canonical_subscription() {
        // Cancelling an instance whose catch subscription is canonically placed
        // on another partition must disarm that remote record (routing a
        // CloseMessageSubscription), so a later publish on the key correlates
        // nothing — no token is advanced on the terminated instance.
        let server = single_node_multi_partition();
        server
            .deploy_centralized(
                vec![("await-payment.bpmn".into(), MESSAGE_CATCH_BPMN.into())],
                "<default>".into(),
            )
            .await
            .expect("deploy");

        let mut chosen: Option<(String, nanobpmn_engine_core::Key)> = None;
        for i in 0..64u32 {
            let order = format!("cancel-{i}");
            let mut variables = std::collections::HashMap::new();
            variables.insert("orderId".to_string(), Value::Str(order.clone()));
            let (key, completed) = server
                .create_for_stream(Some("await-payment".into()), None, variables)
                .await
                .expect("create succeeds");
            assert!(!completed, "the instance parks at the message catch");
            if nanobpmn_engine_core::subscription_partition(&order, 4)
                != nanobpmn_engine_core::partition_of(key)
            {
                chosen = Some((order, key));
                break;
            }
        }
        let (order, instance_key) =
            chosen.expect("a cross-partition placement appears within 64 creates");

        // Cancel and synchronously drive the resulting routing (production
        // fire-and-forgets the Close), disarming the canonical subscription.
        let (events, commit) = server
            .engine
            .by_key(instance_key)
            .with(move |engine| {
                engine.apply_command_at(Command::cancel_instance(instance_key), now_millis())
            })
            .await
            .expect("cancel succeeds");
        commit.wait().await;
        server.drive_subscription_routing(events.to_vec()).await;

        // The canonical subscription is gone, so the publish finds no match.
        let (_message_key, correlated) = server
            .correlate_message_local(
                "payment-received".into(),
                order.clone(),
                std::collections::HashMap::new(),
            )
            .await;
        assert_eq!(
            correlated, None,
            "the publish correlates nothing after the remote subscription is disarmed"
        );
    }

    /// start -> serviceTask "work" with an interrupting message boundary keyed on
    /// `orderId` -> end. The boundary subscription opens when the task activates;
    /// when its canonical home is another partition, the publish must interrupt
    /// the activity across the partition boundary and run the boundary flow.
    #[tokio::test]
    async fn cross_partition_message_boundary_interrupts_via_the_pump() {
        let server = single_node_multi_partition();
        let proc = ProcessBuilder::new("guarded")
            .start_event("s")
            .service_task("work", "do-work")
            .message_boundary_event("cancel-it", "work", "abort-order", "orderId")
            .end_event("done-normally")
            .end_event("aborted")
            .connect("s", "work")
            .connect("work", "done-normally")
            .connect("cancel-it", "aborted")
            .build()
            .expect("valid boundary process");
        let mut names = std::collections::HashMap::new();
        names.insert("guarded".to_string(), "guarded.bpmn".to_string());
        server
            .deploy_resources_locally(vec![proc], &names, "<default>")
            .await
            .expect("deploy the boundary process on every owned partition");

        // Find an instance whose boundary subscription is canonically off its own
        // partition. The token parks on the service-task job; the boundary sub is
        // opened (and its Open routed) as soon as the task activates.
        let mut chosen: Option<(String, nanobpmn_engine_core::Key)> = None;
        for i in 0..64u32 {
            let order = format!("abort-{i}");
            let mut variables = std::collections::HashMap::new();
            variables.insert("orderId".to_string(), Value::Str(order.clone()));
            let (key, completed) = server
                .create_for_stream(Some("guarded".into()), None, variables)
                .await
                .expect("create succeeds");
            assert!(!completed, "the instance parks on the service task");
            if nanobpmn_engine_core::subscription_partition(&order, 4)
                != nanobpmn_engine_core::partition_of(key)
            {
                chosen = Some((order, key));
                break;
            }
        }
        let (order, instance_key) =
            chosen.expect("a cross-partition boundary placement appears within 64 creates");

        // Publishing the boundary message interrupts the activity across the
        // partition boundary: the canonical sub matches on the hash partition,
        // and the correlation is routed back to cancel the job and run the
        // boundary flow to the instance's completion.
        let (_message_key, correlated) = server
            .correlate_message_local(
                "abort-order".into(),
                order.clone(),
                std::collections::HashMap::new(),
            )
            .await;
        assert_eq!(
            correlated,
            Some(instance_key),
            "the publish interrupts the cross-partition guarded activity"
        );

        let (_vars, completed) = server
            .await_completion_for_stream(instance_key, None, Some(2000))
            .await;
        assert!(
            completed,
            "the instance completes down the boundary path after the cross-partition interrupt"
        );
    }

    #[tokio::test]
    async fn message_start_distributes_instances_off_the_deploy_partition() {
        // Message-start subscriptions live solely on the deploy partition (0), so
        // every publish fires there. Without distribution every created instance
        // would pile onto partition 0; the round-robin dispatcher must spread them
        // across all partitions (routed via the host pump as DispatchStartInstance).
        let server = single_node_multi_partition();
        let proc = ProcessBuilder::new("intake")
            .message_start_event("start", "order-placed")
            .service_task("work", "do-work")
            .end_event("end")
            .connect("start", "work")
            .connect("work", "end")
            .build()
            .expect("valid message-start process");
        let mut names = std::collections::HashMap::new();
        names.insert("intake".to_string(), "intake.bpmn".to_string());
        server
            .deploy_resources_locally(vec![proc], &names, "<default>")
            .await
            .expect("deploy the message-start process on every owned partition");

        // Publish many distinctly-keyed messages; each fires the message-start
        // subscription on partition 0 and dispatches the created instance round-
        // robin across the four partitions.
        for i in 0..16u32 {
            server
                .correlate_message_everywhere(
                    "order-placed".into(),
                    format!("order-{i}"),
                    std::collections::HashMap::new(),
                )
                .await;
        }

        // Count the parked instances per partition directly from each engine.
        let mut per_partition = Vec::new();
        for handle in server.engine.all() {
            let n = handle
                .with(|engine| engine.engine().state().instances.len())
                .await;
            per_partition.push(n);
        }
        let total: usize = per_partition.iter().sum();
        assert_eq!(total, 16, "every publish created exactly one instance");
        let partitions_used = per_partition.iter().filter(|&&n| n > 0).count();
        assert!(
            partitions_used >= 2,
            "instances must spread off partition 0 (per-partition counts: {per_partition:?})"
        );
        assert!(
            per_partition[0] < 16,
            "partition 0 must NOT hold every instance (per-partition counts: {per_partition:?})"
        );
    }


    /// token reaches the catch only when the job COMPLETES, exercising the
    /// completion-path pump (not the create-path pump).
    const SERVICE_THEN_CATCH_BPMN: &str = r#"
      <bpmn:definitions
          xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="work-then-wait">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="work">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="work" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:intermediateCatchEvent id="await">
            <bpmn:messageEventDefinition messageRef="Message_1" />
          </bpmn:intermediateCatchEvent>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="work" />
          <bpmn:sequenceFlow id="f1" sourceRef="work" targetRef="await" />
          <bpmn:sequenceFlow id="f2" sourceRef="await" targetRef="e" />
        </bpmn:process>
        <bpmn:message id="Message_1" name="payment-received">
          <bpmn:extensionElements>
            <zeebe:subscription correlationKey="=orderId" />
          </bpmn:extensionElements>
        </bpmn:message>
      </bpmn:definitions>"#;

    #[tokio::test]
    async fn cross_partition_catch_after_job_completion_completes() {
        let server = single_node_multi_partition();
        server
            .deploy_centralized(
                vec![("work-then-wait.bpmn".into(), SERVICE_THEN_CATCH_BPMN.into())],
                "<default>".into(),
            )
            .await
            .expect("deploy succeeds");

        // Find a create that parks the (eventual) subscription off the instance
        // partition. The token is still at the service task here; the catch — and
        // its Opening — is only reached when the job completes below.
        let mut chosen: Option<(String, nanobpmn_engine_core::Key)> = None;
        for i in 0..64u32 {
            let order = format!("svc-order-{i}");
            let mut variables = std::collections::HashMap::new();
            variables.insert("orderId".to_string(), Value::Str(order.clone()));
            let (key, _completed) = server
                .create_for_stream(Some("work-then-wait".into()), None, variables)
                .await
                .expect("create succeeds");
            let p_inst = nanobpmn_engine_core::partition_of(key);
            let p_sub = nanobpmn_engine_core::subscription_partition(&order, 4);
            if p_sub != p_inst {
                chosen = Some((order, key));
                break;
            }
        }
        let (order, instance_key) = chosen.expect("a cross-partition placement within 64 creates");

        // Activate and complete this instance's job: the token advances onto the
        // message catch, emitting an Opening the completion-path pump must route.
        let p_inst = nanobpmn_engine_core::partition_of(instance_key);
        let jobs = server
            .activate_for_stream("work", "w", 64, 60_000, None)
            .await;
        let job_key = jobs
            .iter()
            .map(|j| j.job_key.0.parse::<u64>().expect("numeric job key"))
            .find(|k| nanobpmn_engine_core::partition_of(*k) == p_inst)
            .expect("our instance's job is activatable");
        server
            .complete_job_for_stream(job_key, std::collections::HashMap::new())
            .await
            .expect("complete succeeds")
            .wait()
            .await;

        // Routing is fire-and-forget off the pipelined completion path, so the
        // canonical Open may not be recorded the instant we publish; retry until
        // the publish correlates (or give up after a bounded number of tries).
        let mut correlated = None;
        for _ in 0..50 {
            let (_mk, c) = server
                .correlate_message_local(
                    "payment-received".into(),
                    order.clone(),
                    std::collections::HashMap::new(),
                )
                .await;
            if c == Some(instance_key) {
                correlated = c;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            correlated,
            Some(instance_key),
            "the publish correlates after the completion-path pump opens the sub"
        );

        let (_vars, completed) = server
            .await_completion_for_stream(instance_key, None, Some(2000))
            .await;
        assert!(completed, "instance completes after the post-job-completion correlation");
    }

    #[tokio::test]
    async fn message_start_correlation_reaches_partition_zero() {
        // The route-to-one publish target set always includes partition 0, where
        // message-start subscriptions live. A publish whose correlation key hashes
        // to a NON-zero partition must still create a start instance on p0.
        const START_BPMN: &str = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="on-order">
              <bpmn:startEvent id="s">
                <bpmn:messageEventDefinition messageRef="Message_1" />
              </bpmn:startEvent>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="e" />
            </bpmn:process>
            <bpmn:message id="Message_1" name="order-placed" />
          </bpmn:definitions>"#;

        let server = single_node_multi_partition();
        server
            .deploy_centralized(
                vec![("on-order.bpmn".into(), START_BPMN.into())],
                "<default>".into(),
            )
            .await
            .expect("deploy succeeds");

        // Pick a correlation key that hashes OFF partition 0, proving p0 is hit
        // even though the hash target is elsewhere.
        let key = (0..)
            .map(|i| format!("k{i}"))
            .find(|k| nanobpmn_engine_core::subscription_partition(k, 4) != 0)
            .expect("a non-zero-hashing key exists");

        let (_message_key, instance) = server
            .correlate_message_local(
                "order-placed".into(),
                key,
                std::collections::HashMap::new(),
            )
            .await;
        let instance_key = instance.expect("the message-start created an instance on p0");
        assert_eq!(
            nanobpmn_engine_core::partition_of(instance_key),
            0,
            "message-start instances are minted on the deployment partition"
        );
    }

    #[tokio::test]
    async fn single_partition_message_catch_is_byte_identical() {
        // With one partition the pump must never engage: the engine takes the
        // inline local path, correlation completes the instance directly.
        let server = ServerImpl::default();
        assert!(server.engine.is_single());
        server
            .deploy_centralized(
                vec![("await-payment.bpmn".into(), MESSAGE_CATCH_BPMN.into())],
                "<default>".into(),
            )
            .await
            .expect("deploy succeeds");

        let mut variables = std::collections::HashMap::new();
        variables.insert("orderId".to_string(), Value::Str("order-x".into()));
        let (instance_key, completed) = server
            .create_for_stream(Some("await-payment".into()), None, variables)
            .await
            .expect("create succeeds");
        assert!(!completed, "parks at the catch");

        let (_message_key, correlated) = server
            .correlate_message_local(
                "payment-received".into(),
                "order-x".into(),
                std::collections::HashMap::new(),
            )
            .await;
        assert_eq!(correlated, Some(instance_key));

        let (_vars, completed) = server
            .await_completion_for_stream(instance_key, None, Some(2000))
            .await;
        assert!(completed, "single-partition correlation completes inline");
    }
}

#[cfg(test)]
mod data_dir_tests {
    use super::ensure_data_dir;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    /// A unique, non-existent path under the system temp dir. Caller owns cleanup.
    fn scratch(suffix: &str) -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "nanobpm-ddir-{}-{}-{}",
            std::process::id(),
            n,
            suffix
        ))
    }

    #[test]
    fn existing_directory_is_accepted() {
        let dir = scratch("existing");
        std::fs::create_dir(&dir).unwrap();
        assert!(ensure_data_dir(&dir).is_ok());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn single_missing_level_is_created() {
        let dir = scratch("leaf");
        assert!(!dir.exists());
        assert!(ensure_data_dir(&dir).is_ok());
        assert!(dir.is_dir());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn multi_level_missing_path_is_rejected() {
        let base = scratch("missing-parent");
        let deep = base.join("a").join("b");
        let err = ensure_data_dir(&deep).expect_err("should reject a missing parent");
        assert!(err.contains("neither does its parent"), "got: {err}");
        // Nothing should have been created.
        assert!(!base.exists());
    }

    #[test]
    fn path_pointing_at_a_file_is_rejected() {
        let file = scratch("not-a-dir");
        std::fs::write(&file, b"x").unwrap();
        let err = ensure_data_dir(&file).expect_err("a file is not a usable data dir");
        assert!(err.contains("not a directory"), "got: {err}");
        std::fs::remove_file(&file).ok();
    }
}
