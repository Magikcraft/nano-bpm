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
    ActivatedJob, Command, EngineError, Event, IncidentKind, IncidentState, ProcessBuilder,
    ProcessDefinition, ProcessInstanceState, Value, MAX_PARTITION_ID,
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
        type CreateOk = (String, i32, String, u64, bool, Commit);
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
                        Ok((
                            process_id,
                            version,
                            definition_key,
                            instance_key,
                            sync_completed,
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

        let (process_id, version, definition_key, instance_key, sync_completed, commit) =
            match outcome {
                Ok(fields) => fields,
                Err(resp) => return Ok(*resp),
            };

        // Record REST create
        crate::metrics::record_create("rest");

        // Block on durability before acknowledging: a returned 200 means the
        // create is fsynced.
        commit.wait().await;
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

        let result = self
            .engine
            .by_key(instance_key)
            .with(move |engine| {
                engine.apply_command_at(Command::cancel_instance(instance_key), now_millis())
            })
            .await;
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
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

        let result = self
            .engine
            .by_key(job_key)
            .with(move |engine| {
                engine.apply_command_at(Command::complete_job_with(job_key, variables), now_millis())
            })
            .await;
        match result {
            Ok((_, commit)) => {
                // Record REST job completion
                crate::metrics::record_job_completion("rest");
                // REST API: await fsync before replying (synchronous durability).
                // Contrast with command_stream::pipeline_job_command, which replies
                // immediately and awaits fsync in a detached task for throughput.
                commit.wait().await;
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

    /// Reports the cluster topology. nanobpmn is a single-writer, single-partition
    /// embedded engine, so it always advertises a one-broker, one-partition cluster
    /// with this gateway acting as the healthy leader of partition 1. The broker and
    /// gateway versions both report the server crate version.
    async fn get_topology_impl(&self) -> Result<apis::cluster::GetTopologyResponse, ()> {
        use apis::cluster::GetTopologyResponse as Resp;

        let version = env!("CARGO_PKG_VERSION").to_string();
        let port: i32 = std::env::var("PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8080);

        let partition = models::Partition {
            partition_id: 1,
            role: "leader".to_string(),
            health: "healthy".to_string(),
        };

        let broker = models::BrokerInfo {
            node_id: 0,
            host: "0.0.0.0".to_string(),
            port,
            partitions: vec![partition],
            version: version.clone(),
        };

        let topology = models::TopologyResponse {
            brokers: vec![broker],
            cluster_id: types::Nullable::Null,
            cluster_size: 1,
            partitions_count: 1,
            replication_factor: 1,
            gateway_version: version,
            last_completed_change_id: String::new(),
        };

        Ok(Resp::Status200_ObtainsTheCurrentTopologyOfTheClusterTheGatewayIsPartOf(topology))
    }

    /// Correlates a message across **all** partitions and returns the combined
    /// events. A waiting subscription can sit on any partition (instances are
    /// spread across them), and the message-start subscriptions live on partition
    /// 0, so the message must reach every partition. Each partition mints its own
    /// message key and correlates against its own subscriptions; with a single
    /// partition this is one round-trip, identical to the pre-partitioning path.
    async fn correlate_message_everywhere(
        &self,
        name: String,
        correlation_key: String,
        variables: std::collections::HashMap<String, Value>,
    ) -> Vec<Event> {
        let mut all_events: Vec<Event> = Vec::new();
        for handle in self.engine.all() {
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
            for node in 0..topology.num_nodes() {
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
            None => Ok(Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(
                problem(
                    "User task not found",
                    404,
                    format!("No user task with key {user_task_key}."),
                ),
            )),
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
            None => Ok(Resp::Status404_NotFound(problem(
                "Variable not found",
                404,
                format!("No variable with key {key}."),
            ))),
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

        loop {
            let jobs = self
                .try_activate(
                    &job_type,
                    &worker,
                    max_jobs,
                    timeout,
                    fetch_variable.as_deref(),
                )
                .await;
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
                    // Wait for a wake-up or the remaining window, then retry.
                    let notified = self.jobs_available.notified();
                    let _ = tokio::time::timeout(deadline - now, notified).await;
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
        // Active-backlog admission control (off by default): shed before doing any
        // engine work when the standing active-instance backlog is at/above the
        // limit, keeping end-to-end latency and memory bounded under overload. The
        // stream client reads the 503 `RESOURCE_EXHAUSTED` as a retry signal. A
        // shed create is never journaled, so durability/at-least-once are intact.
        if let Some(message) = self.admission_shed() {
            return Err((503, message));
        }
        let outcome: Result<(nanobpmn_engine_core::Key, bool, Commit), (u16, String)> = {
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
                            Ok((instance_key, sync_completed, commit))
                        }
                        Err(EngineError::ProcessNotFound { process_id }) => {
                            Err((400, format!("No deployed process with id '{process_id}'.")))
                        }
                        Err(e) => Err((500, e.to_string())),
                    }
                })
                .await
        };
        let (instance_key, sync_completed, commit) = outcome?;
        commit.wait().await;
        self.signal_jobs_available();
        Ok((instance_key, sync_completed))
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
        let result = self
            .engine
            .by_key(job_key)
            .with(move |engine| {
                engine.apply_command_at(Command::complete_job_with(job_key, variables), now_millis())
            })
            .await;
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
        let result = self
            .engine
            .by_key(job_key)
            .with(move |engine| {
                engine.apply_command_at(Command::fail_job(job_key, retries, error_message), now_millis())
            })
            .await;
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
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                interval.tick().await;
                let now = now_millis();
                // Drive every partition's clock so timers fire and leases expire.
                // Fan out concurrently: each partition's tick is independent, so
                // running them in parallel keeps the sweep off the critical path
                // instead of serializing N engine round-trips every 500ms.
                let produced = futures_util::future::join_all(engine.all().iter().map(|handle| {
                    handle.with(move |journal| {
                        let (fired, _commit) = journal.trigger_timers(now);
                        let expired = journal.expire_jobs(now);
                        // Shed dormant instances to disk if hot RAM is over the
                        // high-water mark (cheap no-op below it / when unset).
                        journal.maybe_cold_spill();
                        // Either a fired timer (may create a job) or a reclaimed
                        // job lease (frees a job for redelivery) means there is
                        // pushable work — wake dispatch instead of waiting for
                        // its own backstop tick.
                        !fired.is_empty() || !expired.is_empty()
                    })
                }))
                .await
                .into_iter()
                .any(|p| p);
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
