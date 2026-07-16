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
mod cmd_profile;
mod coldspill;
#[cfg(feature = "console")]
mod console;
mod deepthi;
mod drain_guard;
mod falcon;
mod journal;
mod memory;
mod metrics;
// Intra-cluster peer uplink (falcon client to peers). The forwarding
// seam that drives it (create-forward, by-key forward, broadcast) lands in the
// following increments; the transport is integration-tested now.
mod partition;
#[allow(dead_code)]
mod peer;
mod placement;
mod query;
mod raft;
mod raft_logstore;
mod raft_net;
mod readstore;
mod recovery_throttle;
mod seglog;
mod stub_impls;
mod submission_governor;
mod varspill;
mod varstore;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::Multipart;
use axum::response::Response;
use http::StatusCode;
use nanobpm_gateway_rest::{apis, models, types};
use nanobpmn_engine_core::bpmn::parse_bpmn;
use nanobpmn_engine_core::{
    ActivatedJob, Command, EngineError, Event, IncidentKind, IncidentState, Key, MAX_PARTITION_ID,
    ProcessBuilder, ProcessDefinition, ProcessInstanceState, Value, partition_of,
};

use crate::backpressure::{
    AdaptiveController, Backpressure, BackpressureSetting, CONGESTION_RATIO, GovernorObs,
    SharedSlaMode, SlaMode, parse_backpressure_setting, parse_sla_mode,
};
use crate::deepthi::DeepthiHandle;
use crate::journal::{Commit, ExportBatch, Journal, SharedWriter};
use crate::partition::Partitions;
use crate::readstore::{ExportOutcome, ReadModel, ReadStore};

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
/// durable [`Journal`], driven through a single-writer [`DeepthiHandle`] actor.
/// The engine is a single writer, so every mutating command is serialized onto
/// the actor's dedicated thread (see [`deepthi`]); read-only
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
    /// The read model — a per-partition sharded aggregate. All `search*`/`get*`
    /// queries are answered from here (eventually consistent), never from hot
    /// engine state.
    store: Arc<ReadModel>,
    /// Notified whenever new jobs may have become activatable, so long-polling
    /// `activateJobs` requests can wake immediately instead of waiting out their
    /// full timeout.
    jobs_available: Arc<tokio::sync::Notify>,
    /// Permit-storing wake for the falcon dispatcher. Unlike
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
    /// Behaviour selected at the saturation ceiling (seeded from
    /// `NANOBPMN_SLA_MODE`, then **switchable at runtime** via the console).
    /// [`SlaMode::Latency`] (default) sheds admission to preserve end-to-end
    /// latency; [`SlaMode::Admission`] suppresses the latency-preservation gates
    /// (the AIMD concurrency limiter and the active-backlog gate) to keep
    /// admitting instances, accepting higher latency. It never relaxes the
    /// memory-safety rails (create-queue, in-flight-payload, resident-memory
    /// watermarks), which guard against OOM in both modes. Held behind a
    /// [`SharedSlaMode`] so an operator toggle reaches every request handler
    /// without a restart. See [`crate::backpressure::SlaMode`].
    sla_mode: SharedSlaMode,
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
    /// The live per-node active-backlog admission cap the gate compares the
    /// *runnable* (task-job) backlog against; `0` = off. Held behind an atomic
    /// because in `AdmissionBacklog::Auto` mode the engine thread's
    /// [`crate::backpressure::AdaptiveController`] backlog governor retunes it
    /// each latency window to hold the system just left of the congestion-collapse
    /// knee. In `Fixed`/`Off` mode it is a constant. Read on the hot admission
    /// path with a relaxed load.
    backlog_cap: Arc<AtomicUsize>,
    /// When the active-backlog cap is governed live (`AdmissionBacklog::Auto`),
    /// the governor's static bounds (`floor` ≈ knee, `ceiling` = memory backstop)
    /// and a [`GovernorObs`] read handle onto its self-calibrated latency baseline
    /// and last-window latency. `None` in `Fixed`/`Off` mode (the cap is then a
    /// plain constant). Surfaced by the ~1 Hz monitor as
    /// `nanobpm_backlog_governor_*` metrics and used to explain the auto-tuned
    /// cap in the `active_backlog` / `create_backlog` shed message.
    backlog_gov: Option<BacklogGovernor>,
    /// This node's runnable (task-job) backlog: the count of created-but-not-yet-
    /// completed *service-task* jobs, summed across owned partitions. This is the
    /// parked-excluded load signal the admission gate and the backlog governor
    /// read — instances parked on timers/messages create subscriptions/timers,
    /// not jobs, so they never appear here and are never shed against. Refreshed
    /// by the ~1 Hz monitor tick; read with a relaxed load.
    runnable_backlog: Arc<AtomicUsize>,
    /// The live **unified admission setpoint**: the effective per-node active-backlog
    /// cap the throttle converges intake to, `min(latency_cap, memory_cap)` clamped
    /// to `[backlog_cap_floor, backlog_cap_ceiling]`. `0` = no active setpoint (the
    /// backlog cap is disabled — `NANOBPMN_ADMISSION_MAX_BACKLOG=off`), in which case
    /// the drain-guard servo falls back to its absolute band. Computed each ~1 Hz
    /// monitor tick from the latency governor's `backlog_cap` and the live memory
    /// headroom, so it is adaptive to *both* latency and memory. The drain-guard
    /// servo bands against it (pacing submission credits to hold the backlog here in
    /// both SLA modes) and the post-credit backlog shed fires only above it — the
    /// single throttle both the client and server converge on. See
    /// [`Self::backlog_shed_level`].
    effective_backlog_cap: Arc<AtomicUsize>,
    /// The live **memory-only** backlog backstop: the count of active instances the
    /// remaining resident-memory headroom can hold before the watermark
    /// (`active_backlog + (mem_watermark − resident)/NOMINAL_ACTIVE_BYTES`), clamped
    /// to `[backlog_cap_floor, backlog_cap_ceiling]`. This is what the post-credit
    /// active-backlog shed fires against — deliberately **not** the latency-clamped
    /// `effective_backlog_cap`. The completion-paced credit servo owns the latency
    /// operating band (it holds the backlog at `effective_backlog_cap` + its burst
    /// envelope); the shed must sit *above* that envelope so it never collides with
    /// the servo — it only bites when the servo has failed to hold and memory is
    /// genuinely filling (or a create burst bypassed the credit window). When memory
    /// is abundant this sits at the ceiling (shed effectively off, servo in charge);
    /// as RAM fills it shrinks toward the current backlog (memory protection kicks
    /// in). `0` when the backlog cap is disabled. Recomputed each ~1 Hz monitor tick.
    backlog_shed_cap: Arc<AtomicUsize>,
    /// Lower bound for the unified setpoint (`effective_backlog_cap`): the memory
    /// clamp can pull the setpoint down, but never below this (the governor knee
    /// floor, or the fixed cap's floor). `0` when the backlog cap is disabled.
    backlog_cap_floor: usize,
    /// Upper bound for the unified setpoint (`effective_backlog_cap`): the memory
    /// backstop / governor ceiling (or the fixed cap). `0` when disabled.
    backlog_cap_ceiling: usize,
    /// The live **recovery admission cap** published by the adaptive recovery
    /// throttle ([`crate::recovery_throttle`]): while this node is a failover
    /// incumbent / returning owner with a saturating Raft-log disk, this is the
    /// backlog the throttle paces intake to so the disk stays under its `fsync`
    /// knee (durability-preserving — no `fsync` is deferred). `0` = no recovery
    /// clamp (steady state, or the throttle disabled). Folded as a `min` into
    /// [`Self::effective_backlog_cap`] in *all* SLA modes (a recovery liveness rail),
    /// and honoured even when the general backlog cap is off. Recomputed each ~1 Hz
    /// monitor tick.
    recovery_backlog_cap: Arc<AtomicUsize>,
    /// The live **per-producer submission-window cap** published by the adaptive
    /// submission governor ([`crate::submission_governor`]): the TCP-style
    /// congestion window on create admission credits. Under capacity loss the
    /// governor shrinks this below the per-connection `submission_window` (AIMD on
    /// create-accept latency) so fewer creates are admitted and the cluster holds a
    /// stable reduced-capacity throughput instead of limit-cycling; on recovery it
    /// grows back to the ceiling. Initialised to (and held at) the ceiling when
    /// healthy, so `min(conn.submission_window, cap)` is a no-op at steady state.
    /// Read by the credit top-up / grant path in `falcon`. Recomputed each ~1 Hz
    /// monitor tick.
    submission_window_cap: Arc<AtomicI64>,
    /// The live per-job-type active dispatch width the push dispatcher caps its
    /// per-pass subscriber fan-out at; `0` = no cap (dispatch to all subscribers).
    /// Held behind an atomic because in [`WorkerConcurrency::Auto`] mode the engine
    /// thread's [`crate::backpressure::AdaptiveController`] worker governor retunes
    /// it each latency window, holding the fan-out just left of the point where
    /// High-priority activation swamps completions. In `Fixed`/`Off` mode it is a
    /// constant. Read on the dispatch path with a relaxed load.
    active_worker_cap: Arc<AtomicUsize>,
    /// Create-queue-depth admission limit (0 = off, the default). When set,
    /// `createProcessInstance` is shed once the standing backlog of submitted-but-
    /// not-yet-applied creates (summed across partitions' `Low` queues) is at or
    /// above this value. With completion-priority, creates yield to completion, so
    /// under overload it is this create queue — not the active-instance backlog —
    /// that grows and inflates create latency; bounding it caps that latency with a
    /// clean retry signal. Durability/at-least-once are unaffected (a shed create
    /// is never journaled).
    admission_max_create_queue: usize,
    /// Memory-pressure admission watermark in bytes (0 = off). When set,
    /// `createProcessInstance` is shed (503 `RESOURCE_EXHAUSTED`) while the
    /// process's resident memory (`mem_pressure_bytes`, sampled by a background
    /// tick) is at or above this value, so the transient live heap of in-flight
    /// large-variable payloads — request bodies, event serialization, journal
    /// write buffers, replication and exporter batches — can drain before more
    /// creates are admitted. This bounds the RSS balloon a worker-starved
    /// large-payload burst produces (measured 12–16 GB) at the cost of burst
    /// throughput, trading throughput for a steady memory ceiling. Default is a
    /// fraction of the detected cgroup/host memory limit; `NANOBPMN_MEM_WATERMARK_MB`
    /// overrides it and `NANOBPMN_MEM_WATERMARK=off` disables it. Durability and
    /// at-least-once are unaffected: a shed create is never journaled.
    mem_watermark_bytes: u64,
    /// Cached resident-memory sample (bytes) refreshed by a background tick every
    /// ~250 ms, so the hot admission path reads one relaxed atomic instead of
    /// advancing jemalloc's stats epoch per create. Zero until the first sample.
    mem_pressure_bytes: Arc<AtomicU64>,
    /// Lock-free gauge of the payload bytes of creates currently in the
    /// submit→apply window (incremented by a create's estimated variable-payload
    /// size when it is submitted to the engine thread, decremented the instant it
    /// is applied). This is the *precise, proactive* memory rail: with
    /// completion-priority, worker-starved creates pile up in the engine's `Low`
    /// (creation) mailbox — an unbounded queue of closures each capturing a full
    /// copy of its variables — so under a large-payload flood this queue is the
    /// dominant live-heap balloon. The count-based `processing`/`backlog` gates
    /// permit gigabytes here when payloads are large; this byte gauge bounds it
    /// directly, independent of payload size. Zero when idle.
    pipeline_bytes: Arc<AtomicU64>,
    /// In-flight create-payload byte watermark (0 = off). When set,
    /// `createProcessInstance` is shed (503 `RESOURCE_EXHAUSTED`) once
    /// `pipeline_bytes` is at or above it, bounding the engine-mailbox balloon
    /// under a worker-starved large-payload burst *before* it inflates resident
    /// memory — a tighter, payload-specific bound than the coarse
    /// `mem_watermark_bytes` OOM backstop. Adaptive by default (a small fraction
    /// of the detected memory limit); `NANOBPMN_PIPELINE_BYTES_MB` overrides,
    /// `off` disables. Durability is unaffected: a shed create is never journaled.
    pipeline_bytes_watermark: u64,
    /// Drain-stall admission guard — protection against the create-flood wedge
    /// (creates and completes share one FIFO Raft log per partition; a create
    /// flood can starve the completion drain into congestion collapse). Sampled
    /// ~1 Hz by the monitor supervisor and read (relaxed) by the create-admission
    /// gates: it blocks new-instance admission while the drain is falling behind
    /// (soft throttle) or has stalled with the backlog rising (hard valve). A
    /// liveness rail, so it applies in *both* SLA modes. See [`crate::drain_guard`].
    drain_guard: Arc<crate::drain_guard::DrainGuard>,
    /// Falcon uplinks to this node's cluster peers, built from the
    /// [`Topology`]. Empty for a single-node cluster (zero overhead). The
    /// forwarding seam consults it to reach a partition's owning node.
    // Read by the forwarding handlers landing in the following increments
    // (s1-broadcast / s1-bykey-forward); constructed and tested now.
    #[allow(dead_code)]
    peers: peer::PeerSet,
    /// The Raft groups this node hosts (one per partition it replicates), empty
    /// unless per-partition Raft is enabled. The falcon handler dispatches
    /// inbound RPCs through it; the write path proposes through it. An empty
    /// registry means the classic single-writer path is in force — zero overhead.
    raft: Arc<crate::raft::RaftRegistry>,
    /// Engine actors for partitions this node **replicates but does not own**
    /// (followers under RF>1). The Raft state machine drives these so a follower
    /// can apply the replicated log; they are NOT part of the read-model / serving
    /// path (reads and job dispatch always go to the leader's owned actor). Empty
    /// unless per-partition Raft is enabled with RF>1 — zero overhead otherwise.
    raft_replicas: Arc<std::sync::Mutex<std::collections::HashMap<u64, DeepthiHandle>>>,
    /// Spill-tier configuration (variable + cold) captured at startup, applied to
    /// every engine actor this node hosts — owned partitions AND lazily-created
    /// Raft replica engines. `None` when spill is disabled. Wiring it into replica
    /// engines is what lets a follower reclaim hot RAM like its leader instead of
    /// pinning the entire replicated working set resident (the RF>1 follower
    /// memory imbalance). See [`SpillConfig`].
    spill_config: Option<SpillConfig>,
    /// How the per-job activation lock is replicated ([`ActivationPolicy`], from
    /// `NANOBPMN_REPLICATE_ACTIVATION`). Resolved once at startup; the per-partition
    /// answer is [`Self::replicate_activation_for`].
    ///
    /// `Always` is the historical fully-replicated lifecycle: `ActivateJobs` and
    /// lock-expiry go through the log, so every replica holds the lease (3 quorum
    /// commits per job). The leader-local variants (`LeaderLocal`/`Digest`/`Auto`)
    /// do NOT propose `ActivateJobs`: the leader
    /// locks jobs in its own engine actor only and lock-expiry stays local, so each
    /// job costs 2 quorum commits and per-worker activation stops fragmenting the
    /// commit budget. Replicas then run with lenient completion (see
    /// [`Engine::set_lenient_completion`]) so a replicated completion applies even
    /// though they never saw the activation.
    ///
    /// No effect on a single node / RF=1 (no Raft). DURABILITY TRADE-OFF in the
    /// leader-local variants: the lease is leader-RAM-only and does NOT survive
    /// failover — a new leader re-dispatches in-flight jobs immediately (vs. waiting
    /// for the replicated deadline), narrowed by the soft digest under
    /// `Digest`/`Auto`. All variants are at-least-once.
    activation_policy: ActivationPolicy,
    /// Best-effort soft lease digest mode (`NANOBPMN_REPLICATE_ACTIVATION=digest`).
    /// Layered on top of leader-local activation (so `replicate_activation` is
    /// also `false`): a partition leader periodically broadcasts its currently-held
    /// activation leases to its followers (fire-and-forget), and a follower
    /// recovers them on promotion so the new leader honours each lease deadline
    /// before redelivering — narrowing (not closing) the failover redelivery
    /// window that plain leader-local activation opens, with no per-job quorum cost
    /// and no external infrastructure. `false` everywhere else (single node / RF=1
    /// / default), so zero overhead. See [`lease_digest_from_env`].
    lease_digest: bool,
    /// Soft lease table: the latest lease digest received from each partition's
    /// leader, keyed by partition id. Consulted on leadership takeover to recover
    /// in-flight leases (see the tick driver). Soft state, never journaled; bounded
    /// by the number of partitions this node replicates. Empty unless `lease_digest`
    /// is on.
    lease_digests: Arc<std::sync::Mutex<std::collections::HashMap<u64, ReceivedDigest>>>,
    /// Replication durability tier for the partition Raft log (`NANOBPMN_REPLICATION`,
    /// ADR 0003). [`ReplicationMode::Quorum`] (default) acks after majority commit;
    /// [`ReplicationMode::LeaderDurable`] forms each led group with the leader as the
    /// sole voter and the rest as learners, so the ack does not wait for follower
    /// quorum (async log shipping). Consumed in [`Self::raft_bootstrap`] when forming
    /// groups; no effect without Raft (single node / RF=1).
    replication_mode: ReplicationMode,
    /// Per-partition promotion fence for leader-durable auto-recovery (ADR 0003),
    /// stored as `(epoch, leader_node)`. The epoch is monotonic, bumped each time
    /// this node app-promotes a leaderless partition or adopts a peer's winning
    /// promotion; `leader_node` records who holds the partition at that epoch. Used
    /// to (a) dedupe / avoid re-promoting, (b) fence a stale leader (a node leading
    /// at a LOWER epoch steps down on learning of a higher one), and (c) break a
    /// SAME-epoch collision deterministically by lowest node id, so a symmetric
    /// multi-way split that produces two equal-epoch promotions still reconverges to
    /// a single leader. Empty (epoch 0 implied) unless leader-durable recovery has
    /// fired. Soft state, never journaled.
    promotion_epoch: Arc<std::sync::Mutex<std::collections::HashMap<u64, (u64, u64)>>>,
    /// Opt-in (`NANOBPMN_RECLAIM_HANDOFF=1`): on rejoin, reclaim a statically-owned
    /// partition led by a reachable failover incumbent by REQUESTING an openraft
    /// leadership hand-off (the incumbent adds us as a learner, catches us up, then
    /// `change_membership`s leadership to us and steps down) instead of forming a
    /// competing fresh single-voter group. One raft lineage throughout, so there is
    /// no two-group election war (the term storm) under sustained load. Off by
    /// default -> byte-identical to the legacy self-promote reclaim.
    reclaim_via_handoff: bool,
    /// Incumbent side of an in-flight leadership hand-off: the partitions for which
    /// THIS node (the failover leader) is currently executing a hand-off to a
    /// returning owner, each mapped to its completion-pause deadline (ADR 0019).
    /// Presence is both the per-partition hand-off LEASE (a second concurrent
    /// request is declined) and the create WRITE-GATE (new creates are steered off
    /// this partition while the learner catches up). Until the mapped deadline,
    /// job-mutation writes (completions/fails/errors) to the partition are also
    /// paused (retryable) so the raft log fully quiesces and the catch-up can reach
    /// zero lag; the deadline bounds that pause (`NANOBPMN_HANDOFF_WRITE_PAUSE_MS`).
    /// Empty otherwise — zero overhead on the hot path.
    handoff_gated: Arc<std::sync::Mutex<std::collections::HashMap<u64, std::time::Instant>>>,
    /// Bounded ceiling (milliseconds) on the per-partition completion write-pause
    /// during a leadership hand-off catch-up (`NANOBPMN_HANDOFF_WRITE_PAUSE_MS`,
    /// default 2000; `0` disables the completion pause, leaving only the
    /// create-steer = Zeebe-style best-effort). Paused completions are retryable
    /// (at-least-once), so no work is lost — the log just stops growing long enough
    /// to converge. Atomic only so tests can set a short deterministic window; it is
    /// read once per hand-off (cold path).
    handoff_write_pause_ms: Arc<std::sync::atomic::AtomicU64>,
    /// Absolute ceiling (milliseconds) on the hand-off catch-up loop
    /// (`NANOBPMN_HANDOFF_CATCHUP_MS`, default 30000 — see
    /// [`HANDOFF_CATCHUP_CEILING_DEFAULT_MS`]). Also the floor the completion
    /// write-pause is clamped up to in [`ServerImpl::acquire_handoff_lease`], so
    /// the head stays frozen for the whole catch-up. Atomic only so tests can set
    /// a short deterministic window; read on the cold hand-off path.
    handoff_catchup_ceiling_ms: Arc<std::sync::atomic::AtomicU64>,
    /// Requester side of an in-flight leadership hand-off: partitions for which this
    /// (rejoining owner) node has asked the incumbent to hand leadership back,
    /// keyed by partition. Suppresses the legacy self-promote while the hand-off is
    /// in flight so the two paths can't race into competing groups. See
    /// [`HandoffPending`].
    handoff_pending: Arc<std::sync::Mutex<std::collections::HashMap<u64, HandoffPending>>>,
    /// Cluster create-placement mode (`NANOBPMN_CREATE_PLACEMENT`, ADR 0014).
    /// [`PlacementMode::Off`] (default) keeps blind round-robin placement with
    /// forwarded creates ungated — byte-identical to the historical path.
    /// [`PlacementMode::Protect`] gates a forwarded create on the receiving
    /// owner's admission rails and reroutes it around a shedding owner;
    /// [`PlacementMode::Balanced`] additionally makes placement load-aware,
    /// weighting owners by a gossiped composite load index. No effect on a single
    /// node (no peers to steer between / forward to).
    placement_mode: crate::placement::PlacementMode,
    /// Latest composite create-load index gossiped by each peer node, keyed by
    /// node id (see [`placement`](crate::placement)). Populated only in
    /// [`PlacementMode::Balanced`] by the pressure-gossip tick; weighted placement
    /// reads it to steer creates toward nodes with headroom. A missing *or stale*
    /// entry is treated as full headroom (an unprobed peer still receives
    /// traffic). Each entry is stamped with its arrival [`Instant`] so a peer that
    /// stops gossiping — dead, restarting, or its gossip link starved under load —
    /// expires after [`peer_pressure_ttl`] instead of pinning weighted placement to
    /// its last pre-silence value forever (the rejoin zero-creates trap: a node
    /// that shed just before death would otherwise be steered away from
    /// indefinitely). Empty otherwise — zero overhead.
    peer_pressure: Arc<std::sync::Mutex<std::collections::HashMap<u32, (i64, std::time::Instant)>>>,
    /// Persistent smoothing state for the smooth weighted round-robin (SWRR) that
    /// drives load-aware create placement ([`PlacementMode::Balanced`]). One slot
    /// per partition; carried across placement decisions so equal weights yield an
    /// exact round-robin and skewed loads spread smoothly (see
    /// [`crate::placement::swrr_pick`]). Only touched in `Balanced` mode.
    placement_swrr: Arc<std::sync::Mutex<Vec<i128>>>,
    /// Tier-A execution-trace projection, folded off the engine event stream by
    /// the exporter thread (process-optimization design doc §3). In-memory and
    /// bounded; served under `/console/api/traces`. Console builds only.
    #[cfg(feature = "console")]
    pub trace_store: Arc<console::trace::TraceStore>,
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

/// RAII meter for the in-flight create-payload byte gauge: adds `bytes` on entry
/// and subtracts the same on drop, so every exit path (success, error, or a
/// dropped/cancelled request future) releases its bytes exactly once. Held across
/// the same submit→apply window as [`ProcessingGuard`], so the gauge measures the
/// payload bytes resident in the engine's creation mailbox rather than how long a
/// client blocks for completion.
struct ByteGuard<'a>(&'a AtomicU64, u64);

impl<'a> ByteGuard<'a> {
    fn enter(gauge: &'a AtomicU64, bytes: u64) -> Self {
        gauge.fetch_add(bytes, Ordering::Relaxed);
        ByteGuard(gauge, bytes)
    }
}

impl Drop for ByteGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(self.1, Ordering::Relaxed);
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
    pub fn new(
        mut journals: Vec<Journal>,
        store: Arc<ReadModel>,
        topology: cluster::Topology,
    ) -> Self {
        assert!(!journals.is_empty(), "at least one partition is required");
        // Teach every owned partition the cluster-wide partition count so the
        // engine places message subscriptions on the partition owning their
        // correlation key (`hash(correlation_key)`). With a single partition this
        // is `1`, so placement stays local and behaviour is unchanged.
        let replication_mode = replication_mode_from_env();
        let activation_policy = activation_policy_from_env(replication_mode);
        // The soft lease digest broadcasts under `digest` and `auto` (where an
        // activation is leader-local and so needs the digest to cover failover).
        let lease_digest = activation_policy.broadcasts_lease_digest();
        for journal in journals.iter_mut() {
            journal.set_num_partitions(topology.num_partitions);
            // Leader-local activation (any policy that can lock leader-only): replicas
            // must accept a replicated completion for a job they never saw activated
            // (the lock is not replicated). Harmless on a single node (no follower
            // ever applies a completion for an un-activated job in practice, but the
            // relaxed check is still correct there).
            if activation_policy.may_be_leader_local() {
                journal.set_lenient_completion(true);
            }
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
        // Reconcile the persisted read model against authoritative engine state
        // before seeding the in-flight gauge. A read row can be stranded in
        // `Active` when its CREATE was projected here but the matching terminal
        // event never was (leadership moved mid-instance, so the completion was
        // exported by the new leader). Such orphans inflate the active-backlog
        // gauge — biasing admission control and Stage-2 fairness routing — even
        // though the engine, having driven those instances to a terminal state
        // and evicted them, holds no such live instance. Build the engine's live
        // set (hot ∪ cold across every replayed partition) and mark every Active
        // read row absent from it as Completed, then seed the gauge from the
        // reconciled (true) count.
        let mut live_keys: std::collections::HashSet<Key> = std::collections::HashSet::new();
        for journal in journals.iter() {
            journal.collect_live_instance_keys(&mut live_keys);
        }
        let reconciled = store.reconcile_orphaned_active(&live_keys);
        if reconciled > 0 {
            tracing::info!(
                reconciled,
                live = live_keys.len(),
                "read-model reconcile: marked orphaned Active rows Completed at boot \
                 (stranded creates whose terminal event was never projected)"
            );
        }
        let inflight_seed = store.active_instance_count();
        let inflight = Arc::new(AtomicUsize::new(inflight_seed));
        // Request-processing concurrency starts at zero: nothing is mid-apply at
        // boot, regardless of how large the replayed backlog is.
        let processing = Arc::new(AtomicUsize::new(0));

        // Resolve the backpressure mode and the admission-backlog policy, then
        // build the single latency controller the engine thread drives. It hosts
        // up to two AIMD limiters off the one per-command latency signal: the
        // create-concurrency watermark (adaptive backpressure) and the
        // self-optimizing active-backlog governor (auto admission-backlog). The
        // controller owns the shared atomics; the server keeps the read sides.
        let mut controller = AdaptiveController::new();
        let backpressure = match backpressure_setting_from_env() {
            BackpressureSetting::Disabled => Backpressure::Disabled,
            BackpressureSetting::Fixed(n) => Backpressure::Fixed(n),
            BackpressureSetting::Adaptive => {
                Backpressure::Adaptive(controller.with_create_limiter(processing.clone()))
            }
        };
        tracing::info!("backpressure: {}", backpressure.describe());

        let sla_mode = parse_sla_mode(std::env::var("NANOBPMN_SLA_MODE").ok().as_deref());
        tracing::info!("SLA mode at ceiling: {}", sla_mode.describe());
        let sla_mode = SharedSlaMode::new(sla_mode);

        // Runnable (task-job) backlog: the parked-excluded load signal the
        // admission gate and the backlog governor read. Refreshed by the ~1 Hz
        // monitor tick from `activatable_job_counts` (parked instances create no
        // jobs, so they are excluded by construction). Seeded at 0.
        let runnable_backlog = Arc::new(AtomicUsize::new(0));
        // `backlog_cap` is the live active-backlog admission cap the gate reads
        // (0 = off). Its value comes from one of three policies:
        //  - Off:   a fixed 0 (never sheds on backlog).
        //  - Fixed: a fixed operator-set cap.
        //  - Auto:  a self-optimizing governor tunes it between the knee floor and
        //           the memory-derived ceiling from the engine's latency signal.
        // In Auto mode we also keep the governor's bounds + latency read handle
        // (`backlog_gov`) so the monitor and shed message can explain the cap.
        let mut backlog_gov: Option<BacklogGovernor> = None;
        // Bounds for the unified admission setpoint (`effective_backlog_cap`): the
        // memory clamp moves the setpoint within `[floor, ceiling]`. `0` means the
        // backlog cap is disabled (Off) — the servo then uses its absolute band.
        let mut backlog_cap_floor: usize = 0;
        let mut backlog_cap_ceiling: usize = 0;
        let backlog_cap = match admission_backlog_from_env() {
            AdmissionBacklog::Off => Arc::new(AtomicUsize::new(0)),
            AdmissionBacklog::Fixed(n) => {
                tracing::info!(
                    "admission control: on, fixed active-backlog cap {n} runnable job(s)/node"
                );
                // A fixed cap is the ceiling; the memory clamp may still pull the
                // unified setpoint below it (down to the knee floor) for safety.
                backlog_cap_floor = MIN_BACKLOG_GOVERNOR_CAP.min(n);
                backlog_cap_ceiling = n;
                Arc::new(AtomicUsize::new(n))
            }
            AdmissionBacklog::Auto { floor, ceiling } => {
                tracing::info!(
                    "admission control: on, self-optimizing active-backlog governor \
                     (floor {floor}, ceiling {ceiling} runnable jobs/node)"
                );
                backlog_cap_floor = floor;
                backlog_cap_ceiling = ceiling;
                let (cap, obs) =
                    controller.with_backlog_governor(floor, ceiling, runnable_backlog.clone());
                backlog_gov = Some(BacklogGovernor {
                    floor,
                    ceiling,
                    obs,
                });
                cap
            }
        };
        // The unified setpoint, recomputed each monitor tick. Seed at 0 (no clamp)
        // until the first tick folds in the live latency + memory signals.
        let effective_backlog_cap = Arc::new(AtomicUsize::new(0));
        // The memory-only shed backstop, recomputed each tick. Seed at the ceiling
        // (shed effectively off) so the servo owns admission until the first memory
        // sample lands — never shed before we know the live headroom.
        let backlog_shed_cap = Arc::new(AtomicUsize::new(backlog_cap_ceiling));
        // The recovery admission cap, published by the adaptive recovery throttle
        // each monitor tick. Seed at 0 (no clamp) — only engages inside a recovery
        // window with a saturating Raft-log disk.
        let recovery_backlog_cap = Arc::new(AtomicUsize::new(0));
        // Submission-window governor cap: initialised at the governor ceiling so it
        // is inert (a no-op `min` against each connection's `submission_window`)
        // until the monitor tick shrinks it under create-accept latency pressure.
        let submission_window_cap = Arc::new(AtomicI64::new(
            crate::submission_governor::SubmissionGovernorCfg::from_env().ceiling,
        ));
        // `active_worker_cap` is the live per-job-type active dispatch width the
        // push dispatcher reads (0 = no cap). Resolved from one of three policies,
        // mirroring the backlog governor: Off (no cap), Fixed, or a self-optimizing
        // governor tuned off the same engine latency signal, gated on the runnable
        // backlog (grow the fan-out only while there is work to drain).
        let active_worker_cap = match worker_concurrency_from_env() {
            WorkerConcurrency::Off => Arc::new(AtomicUsize::new(0)),
            WorkerConcurrency::Fixed(n) => {
                tracing::info!(
                    "worker concurrency: on, fixed active dispatch width {n} subscriber(s)/job type"
                );
                Arc::new(AtomicUsize::new(n))
            }
            WorkerConcurrency::Auto { floor, ceiling } => {
                tracing::info!(
                    "worker concurrency: on, self-optimizing worker governor \
                     (floor {floor}, ceiling {ceiling} subscribers/job type)"
                );
                controller.with_worker_governor(floor, ceiling, runnable_backlog.clone())
            }
        };
        let mut controller = if controller.is_active() {
            Some(controller)
        } else {
            None
        };
        let admission_max_create_queue = admission_max_create_queue_from_env();
        if admission_max_create_queue > 0 {
            tracing::info!(
                "admission control: on, max create-queue depth {admission_max_create_queue}"
            );
        }
        let mem_watermark_bytes = mem_watermark_bytes_from_env();
        if mem_watermark_bytes > 0 {
            tracing::info!(
                "admission control: memory-pressure watermark {:.0} MiB (shed creates above)",
                mem_watermark_bytes as f64 / (1024.0 * 1024.0),
            );
        }

        let pipeline_bytes_watermark = pipeline_bytes_watermark_from_env();
        if pipeline_bytes_watermark > 0 {
            tracing::info!(
                "admission control: in-flight create-payload watermark {:.0} MiB \
                 (shed creates above)",
                pipeline_bytes_watermark as f64 / (1024.0 * 1024.0),
            );
        }

        let placement_mode = crate::placement::parse_placement_mode(
            std::env::var("NANOBPMN_CREATE_PLACEMENT").ok().as_deref(),
        );
        if placement_mode != crate::placement::PlacementMode::Off {
            tracing::info!("create placement: {}", placement_mode.describe());
        }

        // Optional spill tiers, sharing one disk-backed store (one file, one WAL,
        // one durability story). Variable spill sheds the variables of a large
        // *active* (job-parked) backlog; cold spill sheds whole *dormant*
        // instances of a large *parked* backlog. Both off unless configured.
        // Keys are globally unique across partitions, so a single store serves
        // every partition without collision.
        let var_cfg = spill_from_env();
        let cold_cfg = cold_spill_from_env();
        // Captured spill tiers, applied to owned journals below and re-applied to
        // lazily-created Raft replica engines (see `replica_engine_for`) so a
        // follower reclaims hot RAM exactly like its leader.
        let mut spill_config: Option<SpillConfig> = None;
        if var_cfg.is_some() || cold_cfg.is_some() {
            let path = var_cfg.as_ref().and_then(|(p, _)| p.clone()).or_else(|| {
                resolve_data_paths()
                    .1
                    .map(|db| db.with_file_name("var-spill.sqlite"))
            });
            let location = path
                .as_deref()
                .map(|p| format!(", store {}", p.display()))
                .unwrap_or_else(|| " (in-memory)".to_string());
            match varspill::VarSpillStore::open(path.as_deref()) {
                Ok(store) => {
                    let config = SpillConfig {
                        store: Arc::new(store),
                        var: var_cfg.map(|(_, cfg)| cfg),
                        cold: cold_cfg,
                    };
                    for journal in journals.iter_mut() {
                        config.apply(journal);
                    }
                    if let Some(cfg) = &config.var {
                        match cfg {
                            VarSpillCfg::Budget(budget) => tracing::info!(
                                "variable spill: on (fixed budget), hot budget {budget} instance(s){location}"
                            ),
                            VarSpillCfg::Adaptive {
                                floor,
                                high,
                                low,
                                reserve,
                                hard_cap,
                            } => tracing::info!(
                                "variable spill: on (adaptive, memory-driven), pressure \
                                 high-water {:.0} MiB / low-water {:.0} MiB, reserve {:.0} MiB \
                                 free (reclaim to floor {:.0} MiB under reserve breach), \
                                 hard-cap {hard_cap} instance(s){location}",
                                *high as f64 / (1024.0 * 1024.0),
                                *low as f64 / (1024.0 * 1024.0),
                                *reserve as f64 / (1024.0 * 1024.0),
                                *floor as f64 / (1024.0 * 1024.0),
                            ),
                        }
                    }
                    if let Some((high, low)) = config.cold {
                        tracing::info!(
                            "cold spill: on, high-water {:.0} MiB / low-water {:.0} MiB{location}",
                            high as f64 / (1024.0 * 1024.0),
                            low as f64 / (1024.0 * 1024.0),
                        );
                    }
                    spill_config = Some(config);
                }
                Err(e) => tracing::error!("spill disabled: failed to open store: {e}"),
            }
        }

        // Spawn one engine actor per OWNED partition. The adaptive backpressure
        // controller (when present) is driven by the first owned partition's
        // command latency — a representative single sample of engine load that
        // sizes the create-admission watermark applied across all partitions.
        let owned_count = journals.len();
        let handles: Vec<DeepthiHandle> = journals
            .into_iter()
            .enumerate()
            .map(|(i, journal)| {
                let ctrl = if i == 0 { controller.take() } else { None };
                let partition = journal.partition_id();
                DeepthiHandle::spawn(journal, partition, ctrl)
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
            sla_mode,
            // Seed the gauge from the read model so a journal-replay restart
            // accounts for instances still in flight; a fresh/in-memory store
            // reports zero. The exporter maintains it from here on.
            inflight,
            processing,
            activity: Arc::new(AtomicU64::new(0)),
            backlog_cap,
            backlog_gov,
            backlog_cap_floor,
            backlog_cap_ceiling,
            recovery_backlog_cap,
            submission_window_cap,
            effective_backlog_cap,
            backlog_shed_cap,
            runnable_backlog,
            active_worker_cap,
            admission_max_create_queue,
            mem_watermark_bytes,
            mem_pressure_bytes: Arc::new(AtomicU64::new(0)),
            pipeline_bytes: Arc::new(AtomicU64::new(0)),
            pipeline_bytes_watermark,
            drain_guard: Arc::new(crate::drain_guard::DrainGuard::new(
                crate::drain_guard::DrainGuardCfg::from_env(),
            )),
            peers,
            raft: crate::raft::RaftRegistry::new(),
            raft_replicas: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            spill_config,
            activation_policy,
            lease_digest,
            lease_digests: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            replication_mode,
            promotion_epoch: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            reclaim_via_handoff: std::env::var("NANOBPMN_RECLAIM_HANDOFF")
                .ok()
                .as_deref()
                .map(|v| matches!(v.trim(), "1" | "true" | "on" | "yes"))
                .unwrap_or(false),
            handoff_gated: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            handoff_write_pause_ms: Arc::new(std::sync::atomic::AtomicU64::new(
                handoff_write_pause_from_env().as_millis() as u64,
            )),
            handoff_catchup_ceiling_ms: Arc::new(std::sync::atomic::AtomicU64::new(
                handoff_catchup_ceiling_from_env().as_millis() as u64,
            )),
            handoff_pending: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            placement_mode,
            peer_pressure: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            placement_swrr: Arc::new(std::sync::Mutex::new(Vec::new())),
            #[cfg(feature = "console")]
            trace_store: Arc::new(console::trace::TraceStore::from_env()),
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
#[allow(clippy::type_complexity)]
fn parse_deploy_resources(
    resources: &[(String, String)],
) -> Result<
    (
        Vec<ProcessDefinition>,
        std::collections::HashMap<String, String>,
    ),
    (&'static str, String),
> {
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
                return Err((
                    "Invalid BPMN",
                    format!("Failed to parse '{resource_name}': {e}."),
                ));
            }
        }
    }
    Ok((processes, resource_names))
}

impl Default for ServerImpl {
    fn default() -> Self {
        build_server_in_memory(vec![Journal::in_memory()], cluster::Topology::single(1))
    }
}

impl ServerImpl {
    /// The Raft groups this node hosts. Used to host a partition's group and, by
    /// the falcon handler and write path, to reach it.
    pub fn raft_registry(&self) -> &Arc<crate::raft::RaftRegistry> {
        &self.raft
    }

    /// A [`RaftTransport`](crate::raft_net::RaftTransport) that carries this
    /// node's Raft RPCs to peers over the Falcon protocol. Built from the node's
    /// existing peer uplinks, so a target Raft node id maps straight onto a peer.
    pub fn raft_transport(&self) -> Arc<dyn crate::raft_net::RaftTransport> {
        Arc::new(crate::raft_net::PeerTransport::new(self.peers.clone()))
    }

    /// Peer-side of the Raft network: decode an inbound RPC, feed it to the local
    /// replica of `partition`, and return its serialized response. `Err((status,
    /// message))` maps to the falcon `CommandResult` status (400 malformed,
    /// 404 not hosted here, 500 dispatch failure).
    pub async fn dispatch_raft_rpc(
        &self,
        partition: u64,
        rpc: &str,
        zip: bool,
    ) -> Result<serde_json::Value, (u16, String)> {
        // Decompress (large payloads ride deflate+base64) before parsing the RPC
        // straight from JSON — no intermediate `serde_json::Value` DOM.
        let rpc = crate::raft_net::decode_rpc_payload(rpc, zip)
            .map_err(|e| (400u16, format!("malformed raft rpc: {e}")))?;
        let req: crate::raft_net::RaftRpcRequest =
            serde_json::from_str(&rpc).map_err(|e| (400u16, format!("malformed raft rpc: {e}")))?;
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
/// Catches the read model up to a segmented multi-partition recovery's global
/// log tail. The shared writer feeds the exporter in log (fsync) order, so a
/// single `exported_position` is a true global prefix: the surviving events span
/// `[first_index, total_events)`, everything before `first_index` was already
/// projected (the exporter watermark gates compaction), and only the suffix past
/// `exported_position` needs replaying. No `reset()` — the projection is
/// resumed, not rebuilt.
/// Catches a sharded read model up to a segmented multi-partition recovery. Each
/// shard owns ONE partition and tracks that partition's projected event count, so
/// catch-up demuxes the surviving events by their write tag and resumes each shard
/// from its own persisted `exported_position` (accounting for the compacted prefix
/// via `recovery.pp_base`). A shard behind the compacted prefix or ahead of the
/// surviving tail (truncated/corrupt) is reset and rebuilt from what survives.
fn catch_up_read_model(shards: &[(u64, Arc<ReadStore>)], recovery: &seglog::MultiSegRecovery) {
    for (pid, shard) in shards {
        let base_p = recovery.pp_base.get(*pid as usize).copied().unwrap_or(0);
        let p_events: Vec<&Event> = recovery
            .tagged
            .iter()
            .filter(|(tag, _)| tag == pid)
            .map(|(_, e)| e)
            .collect();
        let mut projected = shard.exported_position() as u64;
        if projected < base_p || projected > base_p + p_events.len() as u64 {
            shard.reset().expect("reset read store shard");
            projected = base_p;
        }
        let skip = (projected - base_p) as usize;
        if skip < p_events.len() {
            shard
                .export(&p_events[skip..])
                .expect("catch up read model shard from segmented multi-partition journal");
        }
    }
}

/// Rebuilds a sharded read model from a legacy (non-segmented) full event log by
/// resetting every shard and replaying each partition's events into its shard. A
/// `ProcessDeployed` is partition-agnostic and its key belongs to the deployment
/// partition (0), which a clustered peer may not own, so it is replayed into
/// EVERY owned shard; other events route to `partition_of(key)`'s shard.
fn rebuild_read_model_legacy(
    shards: &[(u64, Arc<ReadStore>)],
    events: &[Event],
    num_partitions: usize,
) {
    let mut per_shard: std::collections::HashMap<u64, Vec<&Event>> =
        shards.iter().map(|(p, _)| (*p, Vec::new())).collect();
    for e in events {
        if matches!(e, Event::ProcessDeployed { .. }) {
            for bucket in per_shard.values_mut() {
                bucket.push(e);
            }
            continue;
        }
        let p = (nanobpmn_engine_core::partition_of(e.max_key()) as usize)
            .min(num_partitions.saturating_sub(1)) as u64;
        if let Some(bucket) = per_shard.get_mut(&p) {
            bucket.push(e);
        }
    }
    for (pid, shard) in shards {
        shard
            .reset()
            .expect("reset read store shard for legacy rebuild");
        if let Some(evs) = per_shard.get(pid)
            && !evs.is_empty()
        {
            shard
                .export(evs)
                .expect("rebuild read model shard from legacy journal");
        }
    }
}

/// Sibling shard db path for partition `pid`: `read-model.sqlite` becomes
/// `read-model.p<pid>.sqlite`. For the single-partition path (`single`) the base
/// path is used unchanged to preserve the existing on-disk file and its warm
/// restart. `None` (in-memory) stays `None`.
fn shard_db_path(db_path: Option<&Path>, pid: u64, single: bool) -> Option<std::path::PathBuf> {
    let base = db_path?;
    if single {
        return Some(base.to_path_buf());
    }
    let stem = base
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "read-model".into());
    let name = match base.extension() {
        Some(ext) => format!("{stem}.p{pid}.{}", ext.to_string_lossy()),
        None => format!("{stem}.p{pid}"),
    };
    Some(base.with_file_name(name))
}

/// Opens one read-store shard per owned partition (file-backed when `db_path` is
/// set, else in-memory) and wraps them in a [`ReadModel`]. Returns the model plus
/// the shard handles paired with their partition ids (for boot catch-up).
fn open_sharded_read_model(
    db_path: Option<&Path>,
    owned: &[u64],
    single: bool,
) -> (Arc<ReadModel>, Vec<(u64, Arc<ReadStore>)>) {
    let shards: Vec<(u64, Arc<ReadStore>)> = owned
        .iter()
        .map(|&p| {
            let path = shard_db_path(db_path, p, single);
            let store = ReadStore::open(path.as_deref()).unwrap_or_else(|e| {
                let at = path
                    .as_deref()
                    .map(|p| format!(" at {}", p.display()))
                    .unwrap_or_default();
                panic!(
                    "failed to open read model shard{at} (is the file or its directory writable?): {e}"
                )
            });
            (p, Arc::new(store))
        })
        .collect();
    let model = Arc::new(ReadModel::from_shards(shards.clone()));
    (model, shards)
}

/// A read-model shard paired with the export channel feeding it and the
/// partition's exporter-queue byte gauge (shared with the writer, which
/// increments it, and the create-admission path, which reads it).
type ShardChannel = (Arc<ReadStore>, mpsc::Receiver<ExportBatch>, Arc<AtomicU64>);

/// Wires a per-partition sharded read model to its journals and spawns one
/// exporter thread per shard, so read-model projection scales with cores instead
/// of funnelling every partition through a single thread (opening #1). Each owned
/// journal's events are routed to its partition's shard (in the shared-writer path
/// via the per-partition exporter cell; otherwise via `persist()`), and the
/// exporter must be wired before `ServerImpl::new` so a fresh journal's seed
/// deployment is projected. Threads are spawned after so they can route hot-state
/// eviction back to the owning partition.
fn build_server(
    mut journals: Vec<Journal>,
    store: Arc<ReadModel>,
    topology: cluster::Topology,
) -> ServerImpl {
    let shards = store.shards();
    let mut senders: std::collections::HashMap<u64, (mpsc::Sender<ExportBatch>, Arc<AtomicU64>)> =
        std::collections::HashMap::with_capacity(shards.len());
    let mut pending: Vec<ShardChannel> = Vec::with_capacity(shards.len());
    // Per-partition exporter-queue byte gauges: one `Arc<AtomicU64>` shared by
    // the writer (increments on forward), the exporter thread (decrements after
    // projection), and the create-admission path (reads to steer/shed). Keyed by
    // partition for wiring; collected in ascending-partition order below for the
    // steering vector.
    let mut gauges: std::collections::HashMap<u64, Arc<AtomicU64>> =
        std::collections::HashMap::with_capacity(pending.capacity());
    for (pid, shard) in shards {
        let (tx, rx) = mpsc::channel::<ExportBatch>();
        let queued = Arc::new(AtomicU64::new(0));
        gauges.insert(pid, queued.clone());
        senders.insert(pid, (tx, queued.clone()));
        pending.push((shard, rx, queued));
    }
    for journal in journals.iter_mut() {
        let pid = journal.partition_id();
        if let Some((tx, queued)) = senders.get(&pid) {
            journal.set_exporter(tx.clone(), queued.clone());
        } else {
            debug_assert!(false, "journal partition {pid} has no read-model shard");
        }
    }
    // The journals now hold the only senders (in the shared exporter cell or their
    // own field); drop ours so each shard's receiver closes on server shutdown.
    drop(senders);
    let shard_count = pending.len().max(1);
    // Read-model retention (opt-in): split the budget across shards so the TOTAL
    // retained history stays bounded by the configured maximum.
    let shard_retention = match retention_from_env() {
        RetentionCfg::Off => ShardRetention::Off,
        RetentionCfg::Fixed(cap) => ShardRetention::Fixed((cap / shard_count).max(1)),
        RetentionCfg::Adaptive { total_high_bytes } => ShardRetention::Adaptive {
            high_bytes: (total_high_bytes / shard_count as u64).max(1),
        },
    };
    match shard_retention {
        ShardRetention::Off => {}
        ShardRetention::Fixed(cap) => tracing::info!(
            "read-model retention: fixed, {cap} terminal instance(s)/shard ({shard_count} shard(s))"
        ),
        ShardRetention::Adaptive { high_bytes } => tracing::info!(
            "read-model retention: adaptive, {} MiB/shard budget ({shard_count} shard(s))",
            high_bytes / 1024 / 1024
        ),
    }
    // Exporter-queue backpressure (adaptive by default): the per-shard byte
    // budget the create path holds the resident export backlog under. `None`
    // disables it (unbounded queue — the pre-backpressure behaviour).
    let exporter_queue_high = match exporter_queue_from_env() {
        ExporterQueueCfg::Off => None,
        ExporterQueueCfg::PerShard(high) => Some(high),
        ExporterQueueCfg::Adaptive { total_high_bytes } => Some(clamp_exporter_queue_per_shard(
            (total_high_bytes / shard_count as u64).max(1),
        )),
    };
    match exporter_queue_high {
        None => tracing::info!("exporter-queue backpressure: off (unbounded)"),
        Some(high) => tracing::info!(
            "exporter-queue backpressure: {} MiB/shard budget ({shard_count} shard(s))",
            high / 1024 / 1024
        ),
    }
    let server = ServerImpl::new(journals, store, topology);
    // Wire the exporter-queue gauges into the create-admission path (steer +
    // shed) once the engine exists. Ordered by ascending partition id to match
    // `Partitions::local_handles`, which `for_create` steers over by index.
    if let Some(high) = exporter_queue_high {
        let mut ordered: Vec<u64> = gauges.keys().copied().collect();
        ordered.sort_unstable();
        let local_gauges: Vec<Arc<AtomicU64>> = ordered.iter().map(|p| gauges[p].clone()).collect();
        server.engine.set_exporter_backpressure(local_gauges, high);
    }
    for (shard, rx, queued) in pending {
        // Adaptive (disk-pressure) retention prunes in a dedicated per-shard
        // thread so eviction does not compete for CPU with projection inside the
        // saturated exporter thread. `Off`/`Fixed` need no such thread.
        if let ShardRetention::Adaptive { high_bytes } = shard_retention {
            spawn_adaptive_pruner(shard.clone(), high_bytes);
        }
        spawn_exporter(
            rx,
            shard,
            queued,
            shard_retention,
            server.engine.clone(),
            server.instances_changed.clone(),
            server.inflight.clone(),
            server.activity.clone(),
            #[cfg(feature = "console")]
            server.trace_store.clone(),
        );
    }
    server
}

/// Convenience for the in-memory / test paths: one in-memory shard per journal
/// partition.
fn build_server_in_memory(journals: Vec<Journal>, topology: cluster::Topology) -> ServerImpl {
    let owned: Vec<u64> = journals.iter().map(|j| j.partition_id()).collect();
    let store = Arc::new(ReadModel::in_memory_partitions(&owned));
    build_server(journals, store, topology)
}

/// Applies a signed delta to an in-flight instance gauge atomically, saturating
/// at zero. Used by the read-model exporter (per-batch net create/terminal
/// delta) and by the reconciliation sweep (orphan retirement), which are
/// concurrent writers of the same gauge; a compare-exchange loop lets both
/// compose without losing an update, while still clamping at zero defensively.
fn inflight_saturating_add_signed(gauge: &AtomicUsize, delta: i64) {
    let mut cur = gauge.load(Ordering::Relaxed);
    loop {
        let next = if delta >= 0 {
            cur.saturating_add(delta as usize)
        } else {
            cur.saturating_sub(delta.unsigned_abs() as usize)
        };
        match gauge.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => cur = observed,
        }
    }
}

/// Retries `op` with capped exponential backoff (5ms doubling to a 1s ceiling)
/// until it returns `Ok`, calling `on_retry(attempt, err)` before each
/// re-attempt and `sleep(backoff)` between attempts. Never gives up: the read
/// model must stay a faithful projection of the durable journal, so its exporter
/// can wait out transient store contention but must never skip a batch (which
/// would desync the projection irrecoverably). `sleep` is injected so tests drive
/// the loop without real delay. See the call site in [`spawn_exporter`].
fn retry_until_ok<T, E>(
    mut op: impl FnMut() -> Result<T, E>,
    mut on_retry: impl FnMut(u32, E),
    mut sleep: impl FnMut(std::time::Duration),
) -> T {
    let mut backoff = std::time::Duration::from_millis(5);
    let mut attempt = 0u32;
    loop {
        match op() {
            Ok(value) => return value,
            Err(err) => {
                attempt += 1;
                on_retry(attempt, err);
                sleep(backoff);
                backoff = (backoff * 2).min(std::time::Duration::from_secs(1));
            }
        }
    }
}

/// Spawns one read-model exporter thread for a single partition's shard. It
/// drains the shard's channel (batching queued commands' events), projects the
/// batch into the shard's [`ReadStore`], then evicts any now-completed instances
/// from hot engine state — routed back to the instance's owning partition by its
/// key. With per-partition sharding there is one such thread per owned partition,
/// so projection (the former single-thread ceiling) scales with cores. The thread
/// exits when the channel closes (all `ServerImpl` clones and every journal are
/// dropped). Events arrive as `Arc<Vec<Event>>` shared with the command thread, so
/// projecting them costs no deep copy of the payloads.
#[allow(clippy::too_many_arguments)]
fn spawn_exporter(
    rx: mpsc::Receiver<ExportBatch>,
    store: Arc<ReadStore>,
    queued: Arc<AtomicU64>,
    retention: ShardRetention,
    engine: Partitions,
    instances_changed: Arc<tokio::sync::Notify>,
    inflight: Arc<AtomicUsize>,
    activity: Arc<AtomicU64>,
    #[cfg(feature = "console")] trace_store: Arc<console::trace::TraceStore>,
) {
    // Read-model history retention (per shard): how many terminal
    // (Completed/Terminated) instances to retain before the oldest are evicted
    // (with their variables/jobs/etc.). Bounds the read model to the working set
    // so a long-running engine's on-disk store does not climb indefinitely as
    // completed instances accumulate. Pruned in this thread (the shard's single
    // SQLite writer), throttled by accumulated completions so the transaction cost
    // is amortized. `Off` = unbounded (default); `Fixed` caps by instance count;
    // `Adaptive` prunes under disk pressure to hold the store near a byte budget.
    let prune_threshold = match retention {
        ShardRetention::Fixed(cap) => (cap / 4).clamp(64, 4096),
        _ => 2048,
    };
    std::thread::Builder::new()
        .name("nanobpmn-exporter".into())
        .spawn(move || {
            let mut since_prune = 0usize;
            // Exporter profiling (NANOBPMN_EXPORTER_PROFILE): every 5 s log the
            // projection rate, busy%, and — critically — the share of busy time
            // spent inside `store.export` (the single SQLite writer). If busy≈100%
            // and export dominates, this per-node thread is the throughput ceiling
            // and sharding it (openings #1/#2) is the lever; if busy≪100%, the
            // exporter is starved and the ceiling is upstream.
            let profile = std::env::var_os("NANOBPMN_EXPORTER_PROFILE").is_some();
            let mut p_idle = std::time::Duration::ZERO;
            let mut p_busy = std::time::Duration::ZERO;
            let mut p_export = std::time::Duration::ZERO;
            let mut p_events: u64 = 0;
            let mut p_batches: u64 = 0;
            let mut p_window = std::time::Instant::now();
            loop {
                let before_recv = std::time::Instant::now();
                let Ok(first) = rx.recv() else { break };
                if profile {
                    p_idle += before_recv.elapsed();
                }
                let before_batch = std::time::Instant::now();
                let mut batch = vec![first];
                while let Ok(next) = rx.try_recv() {
                    batch.push(next);
                }
                // Total resident bytes this batch accounts for in the exporter
                // queue gauge (0 on the non-shared path). Released once projected.
                let batch_bytes: u64 = batch.iter().map(|b| b.bytes as u64).sum();
                // Signal liveness to the idle-purge tick: a batch means at least
                // one durable command was applied since the last check.
                activity.fetch_add(1, Ordering::Relaxed);
                // Borrow every command's events as a flat slice of references —
                // the payloads stay in their original `Arc`s, never copied here.
                let refs: Vec<&Event> = batch.iter().flat_map(|b| b.events.iter()).collect();
                // Tier-A trace projection (process-optimization design doc §3).
                // Folded here because the exporter is the single ordered point all
                // events flow through, and it is already off the command-commit/ack
                // hot path. Stamp the batch with the server's observation time: the
                // engine clock is injected (not on most events), and ingestion time
                // is exact for the key queue-vs-service diagnostic (those transitions
                // are separate commands at genuinely different instants).
                #[cfg(feature = "console")]
                trace_store.ingest(&refs, now_millis());
                let before_export = std::time::Instant::now();
                // The exporter is the single writer of this shard's read model,
                // so it derives the exact in-flight delta from genuine state
                // transitions (see ExportOutcome) rather than counting raw
                // create/terminal event occurrences — which double-counted under
                // idempotent re-delivery and drifted the gauge.
                //
                // The exporter must NEVER skip a batch. `exported_position`
                // advances by event COUNT and events arrive as a consecutive,
                // log-ordered stream, so dropping a batch permanently desyncs the
                // projection from the journal: later successful batches advance
                // the position past the gap, burying the un-projected events below
                // the exporter watermark where compaction reclaims them —
                // unrecoverable read-model loss (stuck-Active / missing instances /
                // wrong awaitCompletion), even across a restart. A store write
                // failure here is almost always transient lock contention with the
                // retention pruner / WAL checkpoint (SQLite `database is locked`
                // outliving `busy_timeout`), so retry with capped exponential
                // backoff until it clears. Blocking is the correct backpressure:
                // the in-order channel backs up and the exporter-queue budget sheds
                // new creates, exactly as a genuine projection lag would. The batch
                // bytes stay accounted (no early `fetch_sub`) so that backpressure
                // holds while we retry. The engine's durable state is unaffected —
                // the exporter is downstream of fsync; only the projection waits.
                let ExportOutcome {
                    terminal_keys: completed,
                    inflight_delta: delta,
                } = retry_until_ok(
                    || store.export(&refs),
                    |attempt, e| {
                        crate::metrics::record_read_model_export_retry();
                        // Rate-limit the log: the first failure, then once per ~64
                        // attempts (a few seconds at the 1s backoff ceiling).
                        if attempt == 1 || attempt.is_multiple_of(64) {
                            tracing::warn!(
                                attempt,
                                "read-model export failed ({e}); retrying \
                                 (the batch is never dropped)"
                            );
                        }
                    },
                    std::thread::sleep,
                );
                // Projected: this batch no longer occupies the exporter queue, so
                // release its bytes from the create-admission backpressure gauge.
                if batch_bytes > 0 {
                    queued.fetch_sub(batch_bytes, Ordering::Relaxed);
                }
                if profile {
                    p_export += before_export.elapsed();
                    p_events += refs.len() as u64;
                    p_batches += 1;
                }
                // Update the in-flight backpressure gauge by the exact net delta
                // (+genuine creates − genuine terminals). The read-model
                // reconciliation sweep is a second writer (it decrements the
                // gauge for orphaned Active rows it retires), so the update goes
                // through an atomic saturating add rather than a plain load/store
                // — both writers compose without losing an update, and it still
                // saturates at zero defensively.
                if delta != 0 {
                    inflight_saturating_add_signed(&inflight, delta);
                }
                // The read store now reflects this batch; wake any
                // `awaitCompletion` requests so they can observe a terminal
                // state. notify_waiters() is a no-op when nobody is waiting.
                instances_changed.notify_waiters();
                since_prune += completed.len();
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
                // History retention: for the count-capped `Fixed` policy this
                // runs inline (low volume, small cap, cheap). The disk-pressure
                // `Adaptive` policy is pruned by a dedicated per-shard thread
                // (see `spawn_adaptive_pruner`) so its eviction never competes for
                // CPU with projection inside this saturated writer thread.
                if since_prune >= prune_threshold {
                    since_prune = 0;
                    let target_keep = match retention {
                        ShardRetention::Off | ShardRetention::Adaptive { .. } => 0,
                        ShardRetention::Fixed(cap) => cap,
                    };
                    if target_keep != 0 {
                        match store.prune_terminal_instances(target_keep, PRUNE_BATCH_MAX) {
                            Ok(n) if n > 0 => {
                                tracing::debug!("history retention: evicted {n} terminal instances")
                            }
                            Ok(_) => {}
                            Err(e) => tracing::warn!("history retention prune failed: {e}"),
                        }
                    }
                }
                if profile {
                    p_busy += before_batch.elapsed();
                    let window = p_window.elapsed();
                    if window >= std::time::Duration::from_secs(5) {
                        let total = p_busy + p_idle;
                        let busy_pct = if total.is_zero() {
                            0.0
                        } else {
                            p_busy.as_secs_f64() / total.as_secs_f64() * 100.0
                        };
                        let export_pct = if p_busy.is_zero() {
                            0.0
                        } else {
                            p_export.as_secs_f64() / p_busy.as_secs_f64() * 100.0
                        };
                        let eps = p_events as f64 / window.as_secs_f64();
                        let avg_batch = if p_batches > 0 {
                            p_events as f64 / p_batches as f64
                        } else {
                            0.0
                        };
                        tracing::info!(
                            "exporter: {eps:.0} events/s, busy {busy_pct:.1}% (export {export_pct:.1}% of busy), \
                             {p_batches} batches/window, avg {avg_batch:.0} events/batch"
                        );
                        p_idle = std::time::Duration::ZERO;
                        p_busy = std::time::Duration::ZERO;
                        p_export = std::time::Duration::ZERO;
                        p_events = 0;
                        p_batches = 0;
                        p_window = std::time::Instant::now();
                    }
                }
            }
        })
        .expect("spawn read-model exporter thread");
}

/// Spawns the decoupled adaptive-retention pruner for one shard (opening #1
/// follow-up). Disk-pressure pruning is expensive relative to a single insert
/// (it must find and delete the oldest terminal instances), and the exporter
/// thread is already saturated projecting events at the single-writer ceiling —
/// so pruning inline there loses the race and the store grows past budget. This
/// thread owns a *second* SQLite connection to the same shard file and evicts on
/// its own timer; its small delete transactions interleave with the exporter's
/// inserts at SQLite's write-lock granularity, giving eviction a fair share of
/// the writer regardless of the export backlog. Exits when the store is dropped.
fn spawn_adaptive_pruner(store: Arc<ReadStore>, high_bytes: u64) {
    // Hold live data between `low_bytes` and `high_bytes` (7/8 hysteresis, matching
    // the former keep-target). Small per-batch/per-wake bounds keep each write-lock
    // acquisition short so the exporter is never starved; the wake cadence lets the
    // pruner run many small batches per second while remaining idle-cheap.
    let low_bytes = high_bytes / 8 * 7;
    const BATCH: usize = 4_096;
    const MAX_DELETES_PER_WAKE: usize = 65_536;
    let interval = std::time::Duration::from_millis(200);
    let mut conn = match store.prune_connection() {
        Ok(Some(c)) => c,
        // In-memory store (tests): a second connection is a distinct database, so
        // there is nothing to prune here.
        Ok(None) => return,
        Err(e) => {
            tracing::warn!("adaptive pruner: cannot open prune connection: {e}");
            return;
        }
    };
    std::thread::Builder::new()
        .name("nanobpmn-pruner".into())
        .spawn(move || {
            loop {
                std::thread::sleep(interval);
                // The store is dropped on shutdown; nothing else holds a strong ref
                // to *this* shard once the server is gone. Use the weak-count as the
                // exit signal: when the ServerImpl/ReadModel drop their Arc we stop.
                if Arc::strong_count(&store) <= 1 {
                    break;
                }
                match store.adaptive_prune_once(
                    &mut conn,
                    high_bytes,
                    low_bytes,
                    BATCH,
                    MAX_DELETES_PER_WAKE,
                ) {
                    Ok(n) if n > 0 => {
                        tracing::debug!("adaptive retention: evicted {n} terminal instances")
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!("adaptive retention prune failed: {e}"),
                }
                // Size-gated WAL checkpoint on the pruner thread, every wake and
                // independent of whether we pruned: concentrates all checkpoint
                // copy-back into infrequent coalesced passes (off the exporter's
                // hot path) instead of a per-delete-sweep TRUNCATE storm, and keeps
                // the WAL bounded even while the store sits under budget.
                store.maybe_checkpoint_wal(&conn);
            }
        })
        .expect("spawn read-model pruner thread");
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
    parse_backpressure_setting(
        std::env::var("NANOBPMN_BACKPRESSURE_MAX_INFLIGHT")
            .ok()
            .as_deref(),
    )
}

/// How the per-job activation lock is replicated (`NANOBPMN_REPLICATE_ACTIVATION`).
/// The activation *command* itself is small, but under `quorum` every activation is
/// an extra majority commit. That extra commit collapses completion throughput
/// whenever the commit pipeline is under pressure — either by BYTES (e.g. 50 KB
/// variables: ~125× collapse in the 2026-07-11 GCP A/B) or by COUNT (negligible
/// payload at tens of thousands of jobs/s wedges the same way). Because the cost
/// is a per-activation commit, it is unsafe at any non-trivial throughput, not just
/// at large payloads. Leader-local activation keeps the lock off the Raft log
/// entirely (2 commits/job instead of 3) at the cost of a wider failover redelivery
/// window; the soft lease digest narrows that window back. See PERFORMANCE.md.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActivationPolicy {
    /// `1`/`true`/`on`/`yes`/`quorum`: always replicate the activation lock through
    /// Raft (fully node-loss-durable lease; 3 quorum commits/job). The historical
    /// `quorum` default; unusable at large payloads (throughput collapse).
    Always,
    /// `0`/`false`/`off`/`no`/`leader-local`: never replicate; the lock lives only
    /// in the leader's engine actor, followers run lenient completion. No digest
    /// broadcast, so failover redelivers in-flight jobs immediately.
    LeaderLocal,
    /// `digest`: leader-local activation PLUS the periodic soft lease-digest
    /// broadcast so a promoted follower honours in-flight deadlines before
    /// redelivering. Payload-independent throughput; the right pick for large
    /// payloads under quorum.
    Digest,
    /// `auto` (the **default** under `quorum`): a zero-config alias that resolves to
    /// leader-local activation plus the soft lease digest — behaviourally identical
    /// to [`ActivationPolicy::Digest`]. The operator never has to set a flag: this
    /// default is validated healthy across the whole payload/throughput range
    /// (2,400/s @ 50 KB and ~36k/s @ negligible payload; see PERFORMANCE.md).
    ///
    /// An earlier design flipped per partition between the strict replicated lease
    /// (small payloads) and leader-local (large payloads) using a payload-byte EWMA,
    /// but that was unsound: the strict lease's cost is a per-activation quorum
    /// commit, which wedges at high throughput *regardless* of payload size (a
    /// negligible-payload soak at ~61k/s wedged in the 2026-07-11 GCP validation).
    /// `auto` therefore never keeps the strict replicated lease.
    Auto,
}

impl ActivationPolicy {
    /// The soft lease-digest broadcast runs under `digest` and `auto` (where an
    /// activation may be leader-local and thus needs the digest to cover failover).
    fn broadcasts_lease_digest(self) -> bool {
        matches!(self, ActivationPolicy::Digest | ActivationPolicy::Auto)
    }

    /// Whether an activation can ever be leader-local under this policy — i.e.
    /// followers must run with lenient completion. True for every policy except
    /// [`ActivationPolicy::Always`].
    fn may_be_leader_local(self) -> bool {
        !matches!(self, ActivationPolicy::Always)
    }
}

/// Resolves the activation-replication policy from `NANOBPMN_REPLICATE_ACTIVATION`.
/// When unset the default is mode-dependent: `leader-durable` defaults to
/// [`ActivationPolicy::LeaderLocal`] (that tier already acks leader-locally with
/// lenient follower completion; replicating activation there leaks leases under
/// load — proven by the 2026-07-08 A/B), while `quorum` defaults to
/// [`ActivationPolicy::Auto`] (leader-local + soft digest) so workloads dodge the
/// completion collapse without the operator having to know the knob exists.
fn activation_policy_from_env(replication_mode: ReplicationMode) -> ActivationPolicy {
    parse_activation_policy(
        std::env::var("NANOBPMN_REPLICATE_ACTIVATION")
            .ok()
            .as_deref(),
        replication_mode,
    )
}

/// Pure resolution of [`ActivationPolicy`] from a raw `NANOBPMN_REPLICATE_ACTIVATION`
/// value (or `None` when unset), split out so it is unit-testable without touching
/// the process environment.
fn parse_activation_policy(
    raw: Option<&str>,
    replication_mode: ReplicationMode,
) -> ActivationPolicy {
    match raw.map(|v| v.trim().to_ascii_lowercase()) {
        Some(ref v) => match v.as_str() {
            "1" | "true" | "on" | "yes" | "quorum" | "replicate" => ActivationPolicy::Always,
            "digest" => ActivationPolicy::Digest,
            "auto" => ActivationPolicy::Auto,
            // "0"/"false"/"off"/"no"/"leader-local"/"local" and anything
            // unrecognised fall back to plain leader-local (the conservative
            // off switch), matching the historical parse.
            _ => ActivationPolicy::LeaderLocal,
        },
        None => match replication_mode {
            ReplicationMode::LeaderDurable => ActivationPolicy::LeaderLocal,
            ReplicationMode::Quorum => ActivationPolicy::Auto,
        },
    }
}

/// The replication durability tier for the partition Raft log (ADR 0003), the
/// sibling of the local `NANOBPMN_DURABILITY=sync|async` knob but on the
/// *replication* axis. Read once at startup from `NANOBPMN_REPLICATION`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplicationMode {
    /// `quorum` (the default, and the original behaviour): every durable command
    /// is acked only after a majority of voters has committed AND applied it.
    /// Survives node loss (a minority can fail with no data loss); RF=1 / single
    /// node is unaffected (the leader is the only voter, so quorum is itself).
    Quorum,
    /// `leader-durable`: form each partition group with the **leader as the sole
    /// voter** and the other replicas as **learners**. The leader acks after its
    /// own local durable append + apply (quorum = 1), and ships the log to the
    /// learners asynchronously in the background — the Kafka `acks=1` model
    /// applied to the workflow command log. This takes the cross-node quorum
    /// round-trip off the client critical path (the biggest win at low
    /// concurrency, where group commit cannot amortize it).
    ///
    /// TRADE-OFF (ADR 0003): a command acked by the leader but not yet shipped to
    /// a learner is lost if that leader is lost before catch-up — a bounded tail,
    /// the same shape as the local `DURABILITY=async` fsync window but on the
    /// replication axis. Consistent with the system's at-least-once contract: a
    /// lost completion tail redelivers (idempotent workers tolerate it); a lost
    /// create tail was never durably admitted (the at-least-once producer
    /// retries). Single voter means openraft cannot auto-elect a new leader on
    /// leader loss — learner promotion / longest-log election is the option-2
    /// follow-on; this tier is the cheap, measurable option-1 spike. No effect
    /// without Raft (single node / RF=1).
    LeaderDurable,
}

/// Resolves the replication durability tier from `NANOBPMN_REPLICATION`. Default
/// (absent or unrecognised) is [`ReplicationMode::Quorum`] — the strong,
/// node-loss-durable behaviour every existing benchmark and CI run is validated
/// against. `leader-durable` (also accepted: `leader_durable`, `acks=1`, `acks1`)
/// selects the leader-only-voter tier. See [`ReplicationMode`].
fn replication_mode_from_env() -> ReplicationMode {
    match std::env::var("NANOBPMN_REPLICATION")
        .ok()
        .as_deref()
        .map(|v| v.trim().to_ascii_lowercase())
    {
        Some(ref v)
            if v == "leader-durable"
                || v == "leader_durable"
                || v == "leaderdurable"
                || v == "acks=1"
                || v == "acks1" =>
        {
            ReplicationMode::LeaderDurable
        }
        _ => ReplicationMode::Quorum,
    }
}

/// Consecutive leaderless supervisor passes (≈500 ms each) before leader-durable
/// auto-recovery promotes a partition. Default 3 (~1.5 s) so a brief
/// heartbeat/election flutter never triggers a needless promotion; env-tunable via
/// `NANOBPMN_LEADER_DURABLE_GRACE_TICKS`. Floored at 1.
fn leader_durable_recovery_grace_ticks() -> u32 {
    std::env::var("NANOBPMN_LEADER_DURABLE_GRACE_TICKS")
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .unwrap_or(3)
        .max(1)
}

/// Recovery-tick passes to hold a partition down after a self-promote before it is
/// eligible to promote again. Damps the reclaim epoch-climb: once this node
/// promotes partition `p` at epoch E, a lagging metrics view (the fresh group's
/// self-election not yet reflected, or the failover leader not yet stepped down)
/// must not trigger an immediate re-promote at E+1 — the tight climb loop that,
/// under sustained writes, spun the raft term up and produced the `leader_reject`
/// storm. A promote that genuinely took clears the hold-down early (leadership
/// reads as ours); one that is still contested simply re-solicits and retries
/// after the window, capping reclaim to ~one round per hold-down instead of a
/// per-tick climb.
const LEADER_DURABLE_PROMOTE_HOLDDOWN_TICKS: u32 = 2;

/// Requester-side state for an in-flight leadership hand-off (see
/// [`ServerImpl::handoff_pending`]). Tracks how long to keep suppressing the
/// legacy self-promote while waiting for the incumbent to complete the openraft
/// membership change, and whether the incumbent reported a *joint-config
/// suspected* failure — in which case the owner must NOT fall back to forming a
/// fresh competing group (that could diverge a partially-migrated lineage) and
/// instead keeps waiting for the incumbent to finish or recover.
#[derive(Default, Clone, Copy)]
struct HandoffPending {
    /// Recovery-tick passes remaining before giving up and falling back to the
    /// legacy self-promote (only when NOT joint-suspected).
    deadline_ticks: u32,
    /// The incumbent reported it may have committed the joint config but not the
    /// final uniform one; never self-promote over this — wait it out.
    joint_suspected: bool,
}

/// Recovery-tick passes a returning owner waits for an in-flight hand-off before
/// re-sending (it never self-promotes while a reachable incumbent leads the
/// partition — see [`ServerImpl::request_handoff_or_wait`]). At ~500 ms/pass this
/// is ~45 s, kept safely longer than the incumbent's catch-up absolute ceiling
/// ([`HANDOFF_CATCHUP_CEILING_DEFAULT_MS`]) plus the membership change, so a
/// progressing hand-off is never pre-empted or needlessly resent mid-catch-up.
const HANDOFF_PENDING_TICKS: u32 = 90;

/// Absolute ceiling on how long the incumbent polls a hand-off learner toward
/// zero replication lag before aborting. This is a *safety cap*, not the normal
/// exit: the catch-up loop ([`ServerImpl::perform_handoff`], via
/// [`evaluate_catchup`]) succeeds the instant the learner reaches
/// [`HANDOFF_LAG_THRESHOLD`] and aborts EARLY the instant a post-install learner
/// stops advancing for [`HANDOFF_CATCHUP_STALL_DEFAULT_MS`] — so a dead learner
/// never holds the write-pause for the full ceiling, and a *progressing* one is
/// never guillotined mid-stream by a blind fixed cutoff (the old 10 s bug: a
/// from-empty snapshot install under load can't land in 10 s, so every attempt
/// aborted and re-added the learner, re-triggering the install forever).
///
/// Sized to cover ONE full state-machine snapshot install under sustained load
/// (the returning owner boots empty, so catch-up streams the incumbent's whole
/// resident state, chunked at the snapshot transport rate — see ADR 0019). The
/// completion write-pause freezes the log head for the whole of this window (see
/// [`HANDOFF_WRITE_PAUSE_DEFAULT_MS`], clamped `>=` this ceiling in
/// [`ServerImpl::acquire_handoff_lease`]) so the snapshot point stops moving and
/// the install can finish and the tail drain to within the threshold.
/// Overridable via `NANOBPMN_HANDOFF_CATCHUP_MS`.
const HANDOFF_CATCHUP_CEILING_DEFAULT_MS: u64 = 30000;

/// How long a hand-off learner that has *started* matching (a snapshot install
/// landed, tail streaming) may go WITHOUT advancing its matched index before the
/// catch-up aborts early. Distinguishes a genuinely stuck learner (abort, free
/// the write-pause) from one still installing a snapshot (matched not yet
/// reported — bounded only by [`HANDOFF_CATCHUP_CEILING_DEFAULT_MS`]) or steadily
/// draining a tail (advancing — keep going). Overridable via
/// `NANOBPMN_HANDOFF_STALL_MS`.
const HANDOFF_CATCHUP_STALL_DEFAULT_MS: u64 = 8000;

/// Resolve the hand-off catch-up absolute ceiling from `NANOBPMN_HANDOFF_CATCHUP_MS`.
fn handoff_catchup_ceiling_from_env() -> std::time::Duration {
    let ms = std::env::var("NANOBPMN_HANDOFF_CATCHUP_MS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .unwrap_or(HANDOFF_CATCHUP_CEILING_DEFAULT_MS);
    std::time::Duration::from_millis(ms)
}

/// Resolve the hand-off catch-up post-install stall grace from
/// `NANOBPMN_HANDOFF_STALL_MS`.
fn handoff_catchup_stall_from_env() -> std::time::Duration {
    let ms = std::env::var("NANOBPMN_HANDOFF_STALL_MS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .unwrap_or(HANDOFF_CATCHUP_STALL_DEFAULT_MS);
    std::time::Duration::from_millis(ms)
}

/// Absolute HARD cap on a hand-off catch-up, past which the attempt aborts even
/// while a snapshot install is actively transferring. The [soft ceiling]
/// ([`HANDOFF_CATCHUP_CEILING_DEFAULT_MS`]) is the *normal* budget; when a
/// snapshot install is still streaming bytes at the soft ceiling
/// ([`RaftPartition::snapshot_bytes_sent`](crate::raft::RaftPartition::snapshot_bytes_sent)
/// advancing), the deadline EXTENDS up to this hard cap instead of guillotining a
/// large-but-progressing install — the snapshot-transfer-aware adaptive deadline
/// (ADR 0019). Bounds a pathologically slow/huge transfer so it can't hold the
/// completion write-pause forever. Default 6× the soft ceiling; must be `>=` it.
/// Overridable via `NANOBPMN_HANDOFF_CATCHUP_MAX_MS`.
const HANDOFF_CATCHUP_MAX_DEFAULT_MS: u64 = 180000;

/// Resolve the hand-off catch-up absolute hard cap from
/// `NANOBPMN_HANDOFF_CATCHUP_MAX_MS`, clamped to at least the soft ceiling so the
/// extension window is never negative.
fn handoff_catchup_max_from_env(ceiling: std::time::Duration) -> std::time::Duration {
    let ms = std::env::var("NANOBPMN_HANDOFF_CATCHUP_MAX_MS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .unwrap_or(HANDOFF_CATCHUP_MAX_DEFAULT_MS);
    std::time::Duration::from_millis(ms).max(ceiling)
}

/// One decision of the hand-off catch-up loop, computed by the pure
/// [`evaluate_catchup`] from the learner's current replication signals.
#[derive(Debug, PartialEq, Eq)]
enum CatchupStep {
    /// Learner is within threshold — promote it to voter.
    Done,
    /// Keep polling; the learner is installing/streaming and making progress.
    Continue,
    /// Give up this attempt with a reason (learner stalled or ceiling hit).
    Abort(&'static str),
}

/// Pure catch-up decision for [`ServerImpl::perform_handoff`] — kept side-effect
/// free (all clock/metric reads happen in the caller) so the adaptive
/// deadline/stall logic is deterministically unit-testable.
///
/// - `lag`: current replication lag in entries (`None` = no record / a snapshot
///   install still in flight); at/under `threshold` ⇒ [`CatchupStep::Done`].
/// - `matched`: the learner's matched index (`None` until an install lands). Each
///   time it advances past `best_matched`, `last_advance` is reset to `now`.
/// - `snapshot_bytes`: cumulative bytes streamed to the learner during an
///   `InstallSnapshot` (`None` = no install started). This is the signal that a
///   large install is *actively transferring* even while `matched` is still
///   `None` — each time it advances past `best_bytes`, `last_advance` resets too.
/// - Aborts EARLY (`"learner stalled"`) only once progress has begun (matching
///   started OR bytes flowing) and then goes quiet for `stall_grace`, so a
///   healthy-but-slow install is never killed prematurely.
/// - `soft_deadline` is the normal budget. Past it the attempt CONTINUES only
///   while a snapshot install is actively streaming (bytes advanced within
///   `stall_grace`) — the snapshot-transfer-aware extension — bounded by the
///   absolute `hard_deadline`. A plain log-tail catch-up (no install bytes) still
///   aborts at the soft deadline; every attempt aborts at the hard deadline.
#[allow(clippy::too_many_arguments)]
fn evaluate_catchup(
    lag: Option<u64>,
    matched: Option<u64>,
    snapshot_bytes: Option<u64>,
    best_matched: &mut Option<u64>,
    best_bytes: &mut u64,
    last_advance: &mut std::time::Instant,
    now: std::time::Instant,
    soft_deadline: std::time::Instant,
    hard_deadline: std::time::Instant,
    threshold: u64,
    stall_grace: std::time::Duration,
) -> CatchupStep {
    if let Some(l) = lag
        && l <= threshold
    {
        return CatchupStep::Done;
    }
    // Any forward progress — a growing matched index (tail streaming) or a growing
    // snapshot byte count (install streaming) — resets the stall clock.
    if let Some(m) = matched
        && best_matched.map(|b| m > b).unwrap_or(true)
    {
        *best_matched = Some(m);
        *last_advance = now;
    }
    if let Some(b) = snapshot_bytes
        && b > *best_bytes
    {
        *best_bytes = b;
        *last_advance = now;
    }
    // Absolute hard cap: never extend past this, even mid-transfer, so a
    // pathological install can't pin the completion write-pause forever.
    if now >= hard_deadline {
        return CatchupStep::Abort("learner catch-up ceiling exceeded");
    }
    // Genuine stall: progress had begun (matched or bytes) then went quiet.
    let progress_began = best_matched.is_some() || *best_bytes > 0;
    if progress_began && now.duration_since(*last_advance) >= stall_grace {
        return CatchupStep::Abort("learner catch-up stalled");
    }
    // Soft ceiling: past the normal budget, keep going ONLY while a snapshot
    // install is actively streaming (bytes advanced within the stall grace);
    // otherwise abort. This is the snapshot-transfer-aware extension.
    if now >= soft_deadline {
        let streaming = *best_bytes > 0 && now.duration_since(*last_advance) < stall_grace;
        if !streaming {
            return CatchupStep::Abort("learner catch-up ceiling exceeded");
        }
    }
    CatchupStep::Continue
}

/// Pure catch-up-hold decision for the recovery admission throttle. Given this
/// tick's per-`(partition, peer)` `(lag, progress)` observations
/// ([`ServerImpl::catchup_feed_observations`]), the running best-progress map,
/// the (window-derived) lag `threshold` and the `stall_grace`, this updates the
/// map in place and reports whether any peer is in an *advancing* bulk catch-up
/// (lag ≥ `threshold` AND its progress scalar advanced within `stall_grace`),
/// plus the max qualifying lag (for logging). A peer whose progress has gone
/// quiet past the grace is treated as stalled/dead and ignored, so a wedged
/// async learner can't pin the throttle. Clock is injected (`now`) so the
/// stall/advance logic is deterministically unit-testable.
fn catchup_hold_active(
    observations: &[((u64, u64), u64, u128)],
    progress: &mut std::collections::HashMap<(u64, u64), (u128, std::time::Instant)>,
    threshold: u64,
    stall_grace: std::time::Duration,
    now: std::time::Instant,
) -> (bool, u64) {
    let mut active = false;
    let mut max_lag = 0u64;
    for &(key, lag, prog) in observations {
        let slot = progress.entry(key).or_insert((prog, now));
        if prog > slot.0 {
            slot.0 = prog;
            slot.1 = now;
        }
        let advancing = now.duration_since(slot.1) < stall_grace;
        if lag >= threshold && advancing {
            active = true;
            max_lag = max_lag.max(lag);
        }
    }
    (active, max_lag)
}

/// Poll interval for the incumbent's learner-lag catch-up loop.
const HANDOFF_LAG_POLL: std::time::Duration = std::time::Duration::from_millis(50);

/// Replication lag (in log entries) at or below which a hand-off learner is
/// considered caught up enough to promote to voter. Small non-zero slack so a
/// steady trickle of writes doesn't make the loop chase a perpetually-moving
/// last-log index. During the completion write-pause (ADR 0019) the log head
/// freezes, so the learner converges well inside this slack.
const HANDOFF_LAG_THRESHOLD: u64 = 64;

/// Phase E (boot-as-receiver) probe window: on (re)boot a node solicits its
/// co-replicas for owned partitions a peer currently leads (a live failover
/// incumbent) before forming its own groups. Bounded so a cold start — where no
/// peer answers because none has promoted — proceeds to normal `initialize` after
/// at most this delay. A rejoin discovers the incumbent well within it (a solicit
/// reply is one partition-network RTT).
const HANDOFF_PROBE_WINDOW: std::time::Duration = std::time::Duration::from_millis(1500);

/// Poll/re-solicit interval for the Phase E boot incumbent probe.
const HANDOFF_PROBE_POLL: std::time::Duration = std::time::Duration::from_millis(100);

/// Default bounded ceiling on the per-partition completion write-pause during a
/// leadership hand-off catch-up (ADR 0019). Overridable via
/// `NANOBPMN_HANDOFF_WRITE_PAUSE_MS`; `0` disables the completion pause (leaving
/// only the create-steer = Zeebe-style best-effort reclaim).
///
/// Set at/above the [`HANDOFF_CATCHUP_CEILING_DEFAULT_MS`] so completions stay
/// paused for the ENTIRE catch-up attempt: the log head must stay frozen through
/// the whole snapshot install, or the leader's snapshot point keeps advancing and
/// the learner re-snapshots forever (a catch-up livelock). The lease is released
/// the instant the hand-off completes or aborts, so the real stall is only as
/// long as the catch-up actually takes — this is just the safety ceiling.
/// [`ServerImpl::acquire_handoff_lease`] additionally clamps the effective pause
/// up to the catch-up ceiling so the two can never drift out of order.
const HANDOFF_WRITE_PAUSE_DEFAULT_MS: u64 = 32000;

/// Resolve the leadership hand-off completion write-pause ceiling from
/// `NANOBPMN_HANDOFF_WRITE_PAUSE_MS` (default [`HANDOFF_WRITE_PAUSE_DEFAULT_MS`],
/// on by default). A non-numeric value falls back to the default; `0` disables it.
fn handoff_write_pause_from_env() -> std::time::Duration {
    parse_handoff_write_pause(
        std::env::var("NANOBPMN_HANDOFF_WRITE_PAUSE_MS")
            .ok()
            .as_deref(),
    )
}

/// Pure parser for [`handoff_write_pause_from_env`]: `None`/blank/non-numeric →
/// the default; a numeric value (incl. `0`, which disables the pause) → that many
/// milliseconds.
fn parse_handoff_write_pause(v: Option<&str>) -> std::time::Duration {
    let ms = v
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(HANDOFF_WRITE_PAUSE_DEFAULT_MS);
    std::time::Duration::from_millis(ms)
}

/// Cross-pass state for the leader-durable recovery supervisor
/// ([`ServerImpl::leader_durable_recovery_tick`]). One instance lives for the
/// whole supervisor loop (or a test's drive sequence).
#[derive(Default)]
struct RecoveryState {
    /// Per-partition consecutive-leaderless counter (reset by a live leader).
    misses: std::collections::HashMap<u64, u32>,
    /// Partitions observed with a real (formed) leader at least once — only an
    /// established partition is a genuine failover candidate (cold-start guard).
    established: std::collections::HashSet<u64>,
    /// Per-partition post-promote hold-down countdown (Option C damping).
    holddown: std::collections::HashMap<u64, u32>,
}

/// The latest lease digest received from a partition's leader: the leases it held
/// `(job_key, deadline)`. Held in the soft lease table and consumed by a
/// newly-promoted leader to recover in-flight leases.
#[derive(Clone, Debug)]
struct ReceivedDigest {
    leases: Vec<(u64, u64)>,
}

/// The resolved active-backlog admission policy (see
/// [`admission_backlog_from_env`]). The quantity bounded is the **runnable
/// (task-job) backlog** per node — parked instances (timers/messages) create no
/// jobs and are excluded by construction, so the cap never sheds against a
/// legitimately parked population.
enum AdmissionBacklog {
    /// Never shed on backlog (`off`/`0`/`false`/`no`).
    Off,
    /// A fixed operator-set cap on the runnable backlog.
    Fixed(usize),
    /// Self-optimizing: a latency-driven governor tunes the cap between `floor`
    /// (≈ the throughput knee) and `ceiling` (the memory-derived backstop).
    Auto { floor: usize, ceiling: usize },
}

/// Live state of the auto-mode active-backlog governor, kept on the [`Server`] so
/// the monitor and the shed message can explain *why* the cap sits where it does.
/// `floor`/`ceiling` are the static AIMD bounds; `obs` is the read side of the
/// governor's self-calibrated latency baseline and last-window mean latency, which
/// the engine thread republishes each window.
#[derive(Clone)]
struct BacklogGovernor {
    floor: usize,
    ceiling: usize,
    obs: GovernorObs,
}

/// Resolves the active-backlog admission policy from `NANOBPMN_ADMISSION_MAX_BACKLOG`.
///
/// The cap bounds the **runnable (task-job) backlog** per node: once it is
/// reached, `createProcessInstance` is shed with a 503 `RESOURCE_EXHAUSTED` so
/// clients back off, keeping end-to-end latency and memory bounded under
/// sustained overload. Because only service tasks create jobs, parked instances
/// (waiting on timers/messages) are never counted and never shed against.
///
/// - `off` (or `0`/`false`/`no`): [`AdmissionBacklog::Off`] — disabled.
/// - `=<n>`: [`AdmissionBacklog::Fixed`], an explicit per-node cap. Tune to the
///   **throughput knee** (a few thousand/node) to pin the system at its peak
///   sustained throughput — bounding the runnable set keeps the engine actor's
///   per-command cost off its O(active) tail (see PERFORMANCE.md, congestion
///   collapse).
/// - unset / `adaptive` / `auto` / `on`: [`AdmissionBacklog::Auto`] — **the
///   default**. A self-optimizing governor (the engine thread's
///   [`crate::backpressure::AdaptiveController`]) tunes the cap from the measured
///   per-command latency, holding the system just left of the congestion knee so
///   the peak sustained throughput is delivered *by default* — no manual knee
///   tuning. It floors at [`MIN_BACKLOG_GOVERNOR_CAP`] (never starving the
///   workers or shedding a modest parked burst) and ceilings at the memory-derived
///   [`active_backlog_cap_default_from_limit`] (the OOM backstop). Only sheds in
///   [`SlaMode::Latency`]; the memory-safety rails handle admission mode.
fn admission_backlog_from_env() -> AdmissionBacklog {
    if let Ok(v) = std::env::var("NANOBPMN_ADMISSION_MAX_BACKLOG") {
        let t = v.trim().to_ascii_lowercase();
        if matches!(t.as_str(), "off" | "false" | "no") {
            return AdmissionBacklog::Off;
        }
        if let Ok(n) = t.parse::<usize>() {
            // Explicit number wins, including `0` = off.
            return if n == 0 {
                AdmissionBacklog::Off
            } else {
                AdmissionBacklog::Fixed(n)
            };
        }
        // "on"/"adaptive"/"auto"/anything else falls through to the auto governor.
    }
    let ceiling = detect_memory_limit_bytes()
        .map(active_backlog_cap_default_from_limit)
        .unwrap_or(MIN_ACTIVE_BACKLOG_CAP);
    // The governor floor is the knee target, but never above the memory ceiling
    // (on a tiny host the ceiling could clamp below the nominal floor).
    let floor = MIN_BACKLOG_GOVERNOR_CAP.min(ceiling);
    AdmissionBacklog::Auto { floor, ceiling }
}

/// The self-optimizing floor for the [`AdmissionBacklog::Auto`] governor: the
/// lowest cap it will tune down to under congestion. Set near the measured
/// throughput knee (a few thousand runnable jobs/node — see PERFORMANCE.md) so
/// the governor holds the system just left of the congestion-collapse point
/// without starving the workers or shedding a modest parked/burst backlog.
const MIN_BACKLOG_GOVERNOR_CAP: usize = 2_000;

/// The resolved worker-concurrency (active dispatch width) policy — see
/// [`worker_concurrency_from_env`]. Bounds how many subscribers the push
/// dispatcher fans a given job type out to per pass. The push dispatcher, not the
/// engine CPU, is the throughput ceiling (activation is High-priority in the
/// engine mailbox and swamps completions when spread across too many
/// subscribers), so right-sizing this width is the primary lever for sustained
/// completion throughput. Excess subscribers are parked, rotated round-robin so
/// none is starved.
enum WorkerConcurrency {
    /// No cap — dispatch to every subscriber every pass (the historical behavior).
    Off,
    /// A fixed operator-set active-dispatch width per job type.
    Fixed(usize),
    /// Self-optimizing: a latency-driven governor tunes the width between `floor`
    /// and `ceiling` from the engine's per-command latency, gated on the runnable
    /// backlog (grow only while there is work to drain).
    Auto { floor: usize, ceiling: usize },
}

/// The self-optimizing floor for the [`WorkerConcurrency::Auto`] governor: the
/// fewest subscribers per job type the dispatcher will narrow to under congestion.
///
/// This is the **measured drain knee**, not an arbitrary small value. Two
/// independent live sweeps on the RF=3 cluster land on the same point: a
/// subscribed-worker sweep peaks at 50 workers/node (46.7k/s, vs 12–24k at
/// 100–400), and a fixed per-pass-width calibration at 400 over-provisioned
/// workers/node peaks sharply at width 50 (**42.0k/s, p99 9.3s** — vs 13–17k and
/// p99 65–75s at off/100/200/800). Because the single-writer engine keeps latency
/// inflated under sustained overload, the AIMD grow path cannot climb to the knee
/// from below — so, exactly like the backlog governor, the floor must *be* the
/// knee. Pinned here, a worst-case over-provisioned fleet is throttled back to the
/// throughput optimum instead of collapsing (activation swamping completions).
const MIN_WORKER_GOVERNOR_WIDTH: usize = 50;
/// The ceiling for the [`WorkerConcurrency::Auto`] governor: effectively "all
/// subscribers" for any realistic fleet, so a genuinely healthy, drain-bound
/// workload is never throttled below the number of workers that keep completing.
const MAX_WORKER_GOVERNOR_WIDTH: usize = 4_096;

/// Resolves the worker-concurrency (active dispatch width) policy from
/// `NANOBPMN_WORKER_CONCURRENCY`.
///
/// - `off` (or `0`/`false`/`no`): [`WorkerConcurrency::Off`] — no cap (dispatch to
///   every subscriber every pass).
/// - `=<n>`: [`WorkerConcurrency::Fixed`], an explicit per-job-type active width.
/// - unset / `adaptive` / `auto` / `on`: [`WorkerConcurrency::Auto`] — **the
///   default**. A self-optimizing governor (the engine thread's
///   [`crate::backpressure::AdaptiveController`], third limiter) tunes the width
///   from the measured per-command latency, holding the fan-out just left of the
///   point where activation swamps completions. Floors at
///   [`MIN_WORKER_GOVERNOR_WIDTH`], ceilings at [`MAX_WORKER_GOVERNOR_WIDTH`].
fn worker_concurrency_from_env() -> WorkerConcurrency {
    if let Ok(v) = std::env::var("NANOBPMN_WORKER_CONCURRENCY") {
        let t = v.trim().to_ascii_lowercase();
        if matches!(t.as_str(), "off" | "false" | "no") {
            return WorkerConcurrency::Off;
        }
        if let Ok(n) = t.parse::<usize>() {
            return if n == 0 {
                WorkerConcurrency::Off
            } else {
                WorkerConcurrency::Fixed(n)
            };
        }
        // "on"/"adaptive"/"auto"/anything else falls through to the auto governor.
    }
    WorkerConcurrency::Auto {
        floor: MIN_WORKER_GOVERNOR_WIDTH,
        ceiling: MAX_WORKER_GOVERNOR_WIDTH,
    }
}

/// Nominal resident bytes charged per active (created-but-not-terminal) instance
/// when deriving the default backlog cap from the memory budget: an instance
/// record plus a small live variable set and a parked job. Larger than
/// [`NOMINAL_CREATE_BYTES`] because an active instance is longer-lived and carries
/// more resting state; deliberately conservative so the derived count is a
/// generous safety backstop rather than a tight throughput clip.
const NOMINAL_ACTIVE_BYTES: u64 = 16 * 1024;
/// Full-scale value of the graded create-acceptance headroom index
/// ([`ServerImpl::create_occupancy_index`]). Chosen to sit in the numeric band
/// [`crate::placement::placement_weight`] is tuned for (`WEIGHT_SCALE/(load+1)`):
/// `0` (idle) → weight `1e6`; `~CREATE_OCCUPANCY_SCALE` (near-saturation) → a few
/// hundred — a strong-but-smooth steer toward headroom, while genuine saturation
/// is caught by the hard `SHED_LOAD` shed (weight 0) rather than this graded band.
const CREATE_OCCUPANCY_SCALE: i64 = 4096;
/// Never auto-derive a backlog cap below this — a lower floor would shed against a
/// legitimately large parked population (timers/messages) or a modest burst on a
/// small host. Well above the create-queue floor because parked instances are a
/// normal steady state, not a load signal.
const MIN_ACTIVE_BACKLOG_CAP: usize = 50_000;
/// Never auto-derive a backlog cap above this — beyond it the coarse resident-
/// memory / pipeline-byte rails are the right OOM backstop.
const MAX_ACTIVE_BACKLOG_CAP: usize = 1_000_000;

/// Computes the default per-node active-backlog cap from a detected memory
/// `limit`: budget the same fraction the in-flight-byte rail uses, expressed as a
/// *count* of nominal active instances, clamped to
/// `[MIN_ACTIVE_BACKLOG_CAP, MAX_ACTIVE_BACKLOG_CAP]`. Pure so it can be
/// unit-tested without the environment.
fn active_backlog_cap_default_from_limit(limit_bytes: u64) -> usize {
    let budget = pipeline_bytes_watermark_default_from_limit(limit_bytes);
    ((budget / NOMINAL_ACTIVE_BYTES) as usize).clamp(MIN_ACTIVE_BACKLOG_CAP, MAX_ACTIVE_BACKLOG_CAP)
}

/// Nominal resident bytes charged per submitted-but-unapplied create when
/// deriving the default create-queue depth cap from the memory budget: a small
/// create envelope plus a few variables. Deliberately conservative so the
/// count cap lands at a bounded, safe footprint.
const NOMINAL_CREATE_BYTES: u64 = 8 * 1024;
/// Never auto-derive a create-queue cap below this — a tiny cap would shed
/// against legitimate short bursts on a small host.
const MIN_CREATE_QUEUE_CAP: usize = 20_000;
/// Never auto-derive a create-queue cap above this — beyond it the coarse
/// pipeline-byte / resident-memory rails are the right backstop.
const MAX_CREATE_QUEUE_CAP: usize = 500_000;

/// Computes the default create-queue depth cap from a detected memory `limit`:
/// budget the same fraction the in-flight-byte rail uses, expressed as a *count*
/// of nominal creates, clamped to `[MIN_CREATE_QUEUE_CAP, MAX_CREATE_QUEUE_CAP]`.
/// This proactive count rail trips *earlier* than the byte rail — it counts
/// submitted creates before their payloads are all resident — so a flood is shed
/// before memory balloons. Pure so it can be unit-tested without the environment.
fn create_queue_cap_default_from_limit(limit_bytes: u64) -> usize {
    let budget = pipeline_bytes_watermark_default_from_limit(limit_bytes);
    ((budget / NOMINAL_CREATE_BYTES) as usize).clamp(MIN_CREATE_QUEUE_CAP, MAX_CREATE_QUEUE_CAP)
}

/// Resolves the create-queue-depth admission limit.
///
/// `NANOBPMN_ADMISSION_MAX_CREATE_QUEUE=<n>` caps the standing backlog of
/// submitted-but-not-yet-applied creates across all partitions; once it is reached,
/// `createProcessInstance` is shed with a 503 `RESOURCE_EXHAUSTED`. Because
/// completion-priority makes creates yield to completion, this queue is what grows
/// under overload, so bounding it bounds create-side latency **and** caps the
/// resident balloon of unapplied-create payloads before an OOM.
///
/// - `NANOBPMN_ADMISSION_MAX_CREATE_QUEUE=off` (or `0`/`false`/`no`): disabled.
/// - `=<n>`: explicit depth cap.
/// - unset / `adaptive` / `on`: a count derived from the detected cgroup/host
///   memory limit ([`create_queue_cap_default_from_limit`]), or
///   [`MIN_CREATE_QUEUE_CAP`] when no limit can be read. **On by default**: this
///   is the proactive rail that makes an arrival flood *shed* rather than gather
///   in memory, in both SLA modes.
fn admission_max_create_queue_from_env() -> usize {
    if let Ok(v) = std::env::var("NANOBPMN_ADMISSION_MAX_CREATE_QUEUE") {
        let t = v.trim().to_ascii_lowercase();
        if matches!(t.as_str(), "off" | "false" | "no") {
            return 0;
        }
        if let Ok(n) = t.parse::<usize>() {
            // Explicit number wins, including `0` = off.
            return n;
        }
        // "on"/"adaptive"/anything else falls through to the adaptive default.
    }
    detect_memory_limit_bytes()
        .map(create_queue_cap_default_from_limit)
        .unwrap_or(MIN_CREATE_QUEUE_CAP)
}

/// Default memory-pressure admission watermark as a percentage of the detected
/// memory limit. Higher than the spill high-water (65%): spill should engage
/// first to offload resting variables to disk, and this last-resort create-shed
/// only bites when live memory keeps climbing past that toward an OOM. Leaves
/// ~20% headroom for non-jemalloc RSS (thread stacks, SQLite page cache, kernel
/// socket buffers) before the OOM killer would engage.
const MEM_WATERMARK_FRACTION_PCT: u64 = 80;
/// Never auto-derive a watermark below this — on a tiny limit a sub-256 MiB
/// ceiling would shed against the engine's own baseline working set.
const MIN_MEM_WATERMARK_BYTES: u64 = 256 * 1024 * 1024;

/// Computes the default memory-pressure watermark from a detected memory
/// `limit`: a fixed fraction of the limit, floored at [`MIN_MEM_WATERMARK_BYTES`]
/// and never above the limit itself. Pure so it can be unit-tested without
/// touching the filesystem.
fn mem_watermark_default_from_limit(limit_bytes: u64) -> u64 {
    let frac = limit_bytes / 100 * MEM_WATERMARK_FRACTION_PCT;
    frac.max(MIN_MEM_WATERMARK_BYTES).min(limit_bytes)
}

/// Resolves the memory-pressure admission watermark in bytes, or `0` (off).
///
/// - `NANOBPMN_MEM_WATERMARK=off` (or `0`): disabled.
/// - `NANOBPMN_MEM_WATERMARK_MB=<n>`: explicit watermark in MiB (clamped to a
///   floor of [`MIN_MEM_WATERMARK_BYTES`]).
/// - unset / `adaptive` / `on`: [`MEM_WATERMARK_FRACTION_PCT`]% of the detected
///   cgroup/host memory limit, or off when no limit can be read (e.g. non-Linux,
///   where `memory::stats` is unavailable anyway).
fn mem_watermark_bytes_from_env() -> u64 {
    if let Ok(mb) = std::env::var("NANOBPMN_MEM_WATERMARK_MB")
        && let Ok(n) = mb.trim().parse::<u64>()
    {
        if n == 0 {
            return 0;
        }
        return (n.saturating_mul(1024 * 1024)).max(MIN_MEM_WATERMARK_BYTES);
    }
    if let Ok(v) = std::env::var("NANOBPMN_MEM_WATERMARK") {
        let v = v.trim().to_ascii_lowercase();
        if matches!(v.as_str(), "off" | "0" | "false" | "no") {
            return 0;
        }
        // "on"/"adaptive"/anything else falls through to the adaptive default.
    }
    detect_memory_limit_bytes()
        .map(mem_watermark_default_from_limit)
        .unwrap_or(0)
}

/// Default in-flight create-payload watermark as a percentage of the detected
/// memory limit. Small: this bounds only the *transient* submit→apply payload
/// backlog (the engine `Low`-mailbox balloon), not the resting working set, so a
/// few percent of RAM is ample headroom for healthy large-payload flow (which
/// drains through submit→apply in tens of µs and never accumulates) while capping
/// a worker-starved burst well below the coarse `mem_watermark` OOM backstop.
const PIPELINE_BYTES_FRACTION_PCT: u64 = 8;
/// Never auto-derive a watermark below this — a sub-512 MiB ceiling would shed
/// against legitimate concurrent large-payload flow on a small host.
const MIN_PIPELINE_BYTES: u64 = 512 * 1024 * 1024;
/// Never auto-derive a watermark above this — beyond a few GB of *in-flight*
/// create payload the coarse `mem_watermark` OOM rail is the right backstop.
const MAX_PIPELINE_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Computes the default in-flight create-payload watermark from a detected memory
/// `limit`: [`PIPELINE_BYTES_FRACTION_PCT`]% of the limit, clamped to
/// `[MIN_PIPELINE_BYTES, MAX_PIPELINE_BYTES]` and never above the limit itself.
/// Pure so it can be unit-tested without touching the environment.
fn pipeline_bytes_watermark_default_from_limit(limit_bytes: u64) -> u64 {
    let frac = limit_bytes / 100 * PIPELINE_BYTES_FRACTION_PCT;
    frac.clamp(MIN_PIPELINE_BYTES, MAX_PIPELINE_BYTES)
        .min(limit_bytes)
}

/// Resolves the in-flight create-payload admission watermark in bytes, or `0`
/// (off).
///
/// - `NANOBPMN_PIPELINE_BYTES=off` (or `0`): disabled.
/// - `NANOBPMN_PIPELINE_BYTES_MB=<n>`: explicit watermark in MiB (0 disables).
/// - unset / `adaptive` / `on`: [`PIPELINE_BYTES_FRACTION_PCT`]% of the detected
///   cgroup/host memory limit, or off when no limit can be read (e.g. non-Linux,
///   where the metering still runs but never sheds).
fn pipeline_bytes_watermark_from_env() -> u64 {
    if let Ok(mb) = std::env::var("NANOBPMN_PIPELINE_BYTES_MB")
        && let Ok(n) = mb.trim().parse::<u64>()
    {
        if n == 0 {
            return 0;
        }
        return n.saturating_mul(1024 * 1024);
    }
    if let Ok(v) = std::env::var("NANOBPMN_PIPELINE_BYTES") {
        let v = v.trim().to_ascii_lowercase();
        if matches!(v.as_str(), "off" | "0" | "false" | "no") {
            return 0;
        }
        // "on"/"adaptive"/anything else falls through to the adaptive default.
    }
    detect_memory_limit_bytes()
        .map(pipeline_bytes_watermark_default_from_limit)
        .unwrap_or(0)
}

/// How many terminal (Completed/Terminated) process instances the read model
/// retains before the oldest are evicted with all their dependent rows
/// (variables, jobs, incidents, user tasks). Bounds read-model memory to the
/// working set: without it, every completed instance — and its full variable
/// payload — is kept forever, so a long-running engine's memory climbs
/// indefinitely even with no active processes. Unset or `0` = unbounded history
/// (the default, byte-for-byte today's behaviour). Active instances are never
/// evicted regardless of this cap.
fn history_max_instances_from_env() -> usize {
    std::env::var("NANOBPMN_HISTORY_MAX_INSTANCES")
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

/// Fallback spill high-water in MiB when the process's memory limit can't be
/// detected (non-Linux dev boxes, or an unlimited cgroup with no `/proc`).
const DEFAULT_SPILL_MB: u64 = 384;
/// Default spill high-water as a percentage of the detected memory limit. Leaves
/// headroom for non-jemalloc allocations (stacks, mmaps, page cache charged to
/// the cgroup) before the OOM killer would engage.
const SPILL_LIMIT_FRACTION_PCT: u64 = 65;
/// Adaptive-hybrid **floor** (burst budget) as a percentage of the detected
/// memory limit: the resident level a large-payload burst may reach before the
/// hybrid spill starts reclaiming a *growing* backlog. Well below the 65 %
/// pressure high-water so a runaway is bounded to ~this budget (steady memory
/// pressure) long before the OOM guard would engage, while a workload whose
/// resident set stays under it never spills at all.
const SPILL_FLOOR_FRACTION_PCT: u64 = 10;
/// Adaptive-hybrid **reserve** as a percentage of the detected memory limit: the
/// amount of live system-available memory the sweep tries to keep free. When
/// free memory falls below this (e.g. another tenant is eating RAM), spill
/// reclaims to the floor regardless of the backlog trend. A safety guard for
/// constrained/multi-tenant boxes; on a big dedicated box the growth signal
/// bounds the burst long before free memory gets this low.
const SPILL_RESERVE_FRACTION_PCT: u64 = 12;
/// Never auto-derive a high-water below this — on a tiny limit a sub-100 MiB
/// watermark would thrash against the engine's own baseline working set.
const MIN_SPILL_HIGH_BYTES: u64 = 128 * 1024 * 1024;
/// Never auto-derive a floor below this — a sub-256 MiB burst budget would spill
/// a trivial working set and thrash. The floor is also always clamped below the
/// low-water mark so the two bands never invert.
const MIN_SPILL_FLOOR_BYTES: u64 = 256 * 1024 * 1024;

/// Reads `MemTotal` from `/proc/meminfo` in bytes, or `None` off Linux.
fn read_meminfo_total_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb.saturating_mul(1024));
        }
    }
    None
}

/// Detects the memory limit this process is subject to, in bytes: the cgroup
/// limit when running under one (v2 `memory.max`, else v1
/// `memory.limit_in_bytes`), capped by host `MemTotal`. Returns `None` when no
/// limit can be read (non-Linux). A cgroup that reports "unlimited" (v2 `max`,
/// or a v1 near-`u64::MAX` sentinel) falls back to `MemTotal` via the cap, so a
/// container sees its own limit while a bare-metal node sees host RAM.
fn detect_memory_limit_bytes() -> Option<u64> {
    let mut cgroup: Option<u64> = None;
    // cgroup v2
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let s = s.trim();
        if s != "max"
            && let Ok(v) = s.parse::<u64>()
        {
            cgroup = Some(v);
        }
    }
    // cgroup v1
    if cgroup.is_none()
        && let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        && let Ok(v) = s.trim().parse::<u64>()
    {
        cgroup = Some(v);
    }
    let mem_total = read_meminfo_total_bytes();
    match (cgroup, mem_total) {
        // A cgroup can report the host max (or a near-u64 sentinel) as its
        // limit; capping by MemTotal turns "unlimited" into host RAM.
        (Some(c), Some(m)) => Some(c.min(m)),
        (Some(c), None) => Some(c),
        (None, Some(m)) => Some(m),
        (None, None) => None,
    }
}

/// Computes the default spill high-water from a detected memory `limit`: a fixed
/// fraction of the limit, floored at [`MIN_SPILL_HIGH_BYTES`] and never above the
/// limit itself. Pure so it can be unit-tested without touching the filesystem.
fn spill_default_from_limit(limit_bytes: u64) -> u64 {
    let frac = limit_bytes / 100 * SPILL_LIMIT_FRACTION_PCT;
    frac.max(MIN_SPILL_HIGH_BYTES).min(limit_bytes)
}

/// The default spill high-water in bytes when no explicit `*_MB` override is set:
/// RAM-relative when a memory limit can be detected, else [`DEFAULT_SPILL_MB`].
fn default_spill_high_bytes() -> u64 {
    match detect_memory_limit_bytes() {
        Some(limit) => spill_default_from_limit(limit),
        None => DEFAULT_SPILL_MB * 1024 * 1024,
    }
}

/// Computes the adaptive-hybrid spill *floor* (burst budget) from a detected
/// memory `limit`: [`SPILL_FLOOR_FRACTION_PCT`] of it, floored at
/// [`MIN_SPILL_FLOOR_BYTES`] and clamped strictly below `low` so the floor and
/// pressure bands never invert. Pure, for unit tests.
fn spill_floor_from_limit(limit_bytes: u64, low: u64) -> u64 {
    let frac = limit_bytes / 100 * SPILL_FLOOR_FRACTION_PCT;
    let floor = frac.max(MIN_SPILL_FLOOR_BYTES);
    // Keep the floor under the low-water mark (at most half of it) so a growth
    // reclaim to the floor always sits below the pressure band.
    floor.min(low / 2).max(1)
}

/// The default hybrid floor in bytes given the resolved `low`-water mark:
/// RAM-relative when a memory limit is detected, else a fraction of the fallback
/// default, always clamped below `low`.
fn default_spill_floor_bytes(low: u64) -> u64 {
    match detect_memory_limit_bytes() {
        Some(limit) => spill_floor_from_limit(limit, low),
        None => (DEFAULT_SPILL_MB * 1024 * 1024 / 4).min(low / 2).max(1),
    }
}

/// The default hybrid reserve (system-available-memory floor) in bytes:
/// [`SPILL_RESERVE_FRACTION_PCT`] of the detected limit, or `0` (guard disabled)
/// when no limit can be read.
fn default_spill_reserve_bytes() -> u64 {
    match detect_memory_limit_bytes() {
        Some(limit) => spill_reserve_from_limit(limit),
        None => 0,
    }
}

/// Pure form of [`default_spill_reserve_bytes`] for unit tests.
fn spill_reserve_from_limit(limit_bytes: u64) -> u64 {
    limit_bytes / 100 * SPILL_RESERVE_FRACTION_PCT
}

/// Fallback read-model retention budget in MiB when adaptive mode is selected but
/// the data-dir filesystem capacity can't be detected (non-Unix, or no data dir).
const DEFAULT_HISTORY_MB: u64 = 8192;
/// Adaptive read-model retention budget as a percentage of the data-dir
/// filesystem capacity. Leaves headroom for the journal, the WAL, the
/// variable-spill store, and the OS before the disk fills (an ENOSPC would abort
/// the node to preserve durability — see the journal write-failure path).
const HISTORY_DISK_FRACTION_PCT: u64 = 60;

/// Total filesystem capacity (bytes) of the volume holding `path`, via
/// `statvfs`, or `None` when it can't be read.
#[cfg(unix)]
fn detect_disk_capacity_bytes(path: &std::path::Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: `st` is zeroed then filled by statvfs; `c` is a valid NUL-terminated
    // path pointer that outlives the call. We check the return code before use.
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c.as_ptr(), &mut st) };
    if rc != 0 {
        return None;
    }
    Some(st.f_blocks as u64 * st.f_frsize as u64)
}

#[cfg(not(unix))]
fn detect_disk_capacity_bytes(_path: &std::path::Path) -> Option<u64> {
    None
}

/// The default read-model retention byte budget (total, across all shards) when
/// no explicit `NANOBPMN_HISTORY_RETENTION_MB` override is set: a fraction of the
/// data-dir filesystem capacity, else [`DEFAULT_HISTORY_MB`].
fn default_history_high_bytes() -> u64 {
    let (_, db) = resolve_data_paths();
    let cap = db
        .as_deref()
        .and_then(|p| p.parent())
        .and_then(detect_disk_capacity_bytes);
    match cap {
        Some(bytes) => {
            (bytes / 100 * HISTORY_DISK_FRACTION_PCT).max(DEFAULT_HISTORY_MB * 1024 * 1024)
        }
        None => DEFAULT_HISTORY_MB * 1024 * 1024,
    }
}

/// Resolved read-model history retention policy (see [`retention_from_env`]).
enum RetentionCfg {
    /// No pruning — history grows with cumulative throughput (the default).
    Off,
    /// Cap the retained terminal set at a fixed total instance count.
    Fixed(usize),
    /// Prune terminal instances under disk pressure so the read-model store's
    /// total on-disk size tracks `total_high_bytes` instead of growing without
    /// bound.
    Adaptive { total_high_bytes: u64 },
}

/// Per-shard slice of a [`RetentionCfg`], handed to each exporter thread.
#[derive(Clone, Copy)]
enum ShardRetention {
    Off,
    Fixed(usize),
    Adaptive { high_bytes: u64 },
}

/// Upper bound on terminal instances evicted per prune sweep (per shard). Keeps
/// each prune transaction small and quick so the exporter thread never blocks
/// long enough for its unbounded event channel to back up (which manifested as a
/// multi-gigabyte RSS spike when a store first crossed budget with a large
/// backlog). A backlog is worked down across successive sweeps; steady-state
/// sweeps delete far fewer than this cap.
const PRUNE_BATCH_MAX: usize = 16_384;

/// Resolves the read-model retention policy from the environment.
///
/// Retention bounds the *read model* (projected completed-instance history),
/// which is otherwise unbounded and — under sustained high throughput — the
/// dominant disk consumer (the journal is compacted independently). It is
/// **off by default** (byte-for-byte today's behaviour: full history retained).
///
/// - `NANOBPMN_HISTORY_RETENTION=adaptive`/`auto`/`dynamic`: disk-pressure-driven
///   pruning. The store's total on-disk size is held near
///   `NANOBPMN_HISTORY_RETENTION_MB` (default: [`HISTORY_DISK_FRACTION_PCT`]% of
///   the data-dir filesystem capacity, or [`DEFAULT_HISTORY_MB`] MiB when the
///   capacity can't be detected). Opt-in.
/// - Otherwise: the legacy fixed cap via `NANOBPMN_HISTORY_MAX_INSTANCES`
///   (unset/`0` = unbounded, the default; `>0` = cap the terminal set at that
///   many instances).
fn retention_from_env() -> RetentionCfg {
    match std::env::var("NANOBPMN_HISTORY_RETENTION")
        .ok()
        .as_deref()
        .map(str::trim)
    {
        Some("adaptive") | Some("auto") | Some("dynamic") => {
            let high = std::env::var("NANOBPMN_HISTORY_RETENTION_MB")
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .filter(|n| *n > 0)
                .map(|mb| mb * 1024 * 1024)
                .unwrap_or_else(default_history_high_bytes);
            RetentionCfg::Adaptive {
                total_high_bytes: high,
            }
        }
        _ => match history_max_instances_from_env() {
            0 => RetentionCfg::Off,
            cap => RetentionCfg::Fixed(cap),
        },
    }
}

/// The resolved exporter-queue backpressure policy (see
/// [`exporter_queue_from_env`]).
enum ExporterQueueCfg {
    /// Unbounded exporter queue — the pre-backpressure behaviour.
    Off,
    /// Adaptive: the total resident export backlog is held near
    /// `total_high_bytes`, split evenly across shards.
    Adaptive { total_high_bytes: u64 },
    /// Explicit per-shard byte budget (`NANOBPMN_EXPORTER_QUEUE_MB`).
    PerShard(u64),
}

/// Fallback exporter-queue budget (total, across shards) in MiB when adaptive
/// mode is selected but the memory limit can't be detected (non-Linux dev box).
const DEFAULT_EXPORTER_QUEUE_MB: u64 = 512;
/// Adaptive exporter-queue budget as a percentage of the detected memory limit.
/// The queue is a transient buffer smoothing projection bursts, not durable
/// state, so it takes a small slice of RAM well below the spill watermark.
const EXPORTER_QUEUE_LIMIT_FRACTION_PCT: u64 = 6;
/// Never auto-derive a per-shard exporter-queue budget above this. Caps the
/// worst-case resident backlog on very large boxes so "adaptive" still means
/// bounded, not "a couple of GB per shard".
const MAX_EXPORTER_QUEUE_PER_SHARD_BYTES: u64 = 512 * 1024 * 1024;
/// Never auto-derive a per-shard budget below this — too small a queue sheds on
/// every micro-burst and needlessly caps throughput.
const MIN_EXPORTER_QUEUE_PER_SHARD_BYTES: u64 = 64 * 1024 * 1024;

/// The default adaptive exporter-queue budget (total, across all shards) when no
/// explicit `NANOBPMN_EXPORTER_QUEUE_MB` override is set: a small fraction of the
/// detected memory limit, else [`DEFAULT_EXPORTER_QUEUE_MB`]. The per-shard slice
/// (computed by the caller as `total / shard_count`) is clamped to
/// `[MIN_EXPORTER_QUEUE_PER_SHARD_BYTES, MAX_EXPORTER_QUEUE_PER_SHARD_BYTES]`.
fn default_exporter_queue_high_bytes() -> u64 {
    match detect_memory_limit_bytes() {
        Some(limit) => (limit / 100 * EXPORTER_QUEUE_LIMIT_FRACTION_PCT)
            .max(DEFAULT_EXPORTER_QUEUE_MB * 1024 * 1024),
        None => DEFAULT_EXPORTER_QUEUE_MB * 1024 * 1024,
    }
}

/// Clamps a resolved per-shard exporter-queue budget to sane bounds, so both the
/// adaptive split and an explicit override stay in a range that bounds memory
/// without shedding on every micro-burst.
fn clamp_exporter_queue_per_shard(high: u64) -> u64 {
    high.clamp(
        MIN_EXPORTER_QUEUE_PER_SHARD_BYTES,
        MAX_EXPORTER_QUEUE_PER_SHARD_BYTES,
    )
}

/// Resolves the exporter-queue backpressure policy from the environment.
///
/// The read-model exporter is fed by an in-memory queue of committed events
/// awaiting projection. Under a large-variable flood that outruns the single
/// SQLite writer, that queue — holding a full copy of each event's variables —
/// is the dominant RAM balloon (independent of variable spill, which sheds the
/// engine's copy, not the exporter's). This bounds the queue's resident size by
/// steering creates away from a saturated shard and shedding once every local
/// shard is at budget, so memory tracks the watermark instead of the backlog.
///
/// **Adaptive by default** (RAM-relative, self-sizing to the box):
/// - `NANOBPMN_EXPORTER_QUEUE=off`/`0`/`none`: unbounded (pre-backpressure).
/// - `NANOBPMN_EXPORTER_QUEUE=adaptive`/`auto`/`dynamic` (or unset): per-shard
///   budget = [`EXPORTER_QUEUE_LIMIT_FRACTION_PCT`]% of the detected memory limit
///   split across shards, clamped to
///   `[MIN..MAX]_EXPORTER_QUEUE_PER_SHARD_BYTES`.
/// - `NANOBPMN_EXPORTER_QUEUE_MB=<n>`: explicit **per-shard** budget in MiB
///   (also clamped), overriding the adaptive default.
fn exporter_queue_from_env() -> ExporterQueueCfg {
    let mode = std::env::var("NANOBPMN_EXPORTER_QUEUE")
        .ok()
        .map(|v| v.trim().to_ascii_lowercase());
    match mode.as_deref() {
        Some("off") | Some("0") | Some("none") | Some("false") => ExporterQueueCfg::Off,
        _ => {
            // An explicit per-shard MB override wins in any non-off mode.
            if let Some(mb) = std::env::var("NANOBPMN_EXPORTER_QUEUE_MB")
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .filter(|n| *n > 0)
            {
                return ExporterQueueCfg::PerShard(clamp_exporter_queue_per_shard(
                    mb * 1024 * 1024,
                ));
            }
            ExporterQueueCfg::Adaptive {
                total_high_bytes: default_exporter_queue_high_bytes(),
            }
        }
    }
}

/// The resolved variable-spill mode (see [`spill_from_env`]).
#[derive(Clone)]
enum VarSpillCfg {
    /// Legacy fixed instance-count budget, checked per command.
    Budget(usize),
    /// Adaptive **hybrid**: shed on the periodic sweep only when it helps — a
    /// growing backlog above the `floor` budget, or real memory pressure
    /// (`high`/`reserve`) — with a per-command instance-count backstop
    /// (`hard_cap`) for runaways between sweeps. See [`VarSpillTrigger::Adaptive`].
    Adaptive {
        floor: u64,
        high: u64,
        low: u64,
        reserve: u64,
        hard_cap: usize,
    },
}

/// The resolved spill-tier configuration (variable + cold), captured once at
/// startup so it can be applied uniformly to **every** engine actor this node
/// hosts — the statically owned partitions AND the lazily-created Raft replica
/// engines for partitions this node only follows.
///
/// Spill (`maybe_var_spill_pressure` / `maybe_cold_spill`) is a purely LOCAL
/// memory-reclamation operation: it mints no keys, emits no events and proposes
/// nothing through Raft, so it is safe and correct on any replica. Historically
/// only owned/led engines were configured with a spill store (and only the
/// leader's clock tick ran the spill gate), so a follower held its entire
/// replicated working set in hot RAM — the leader/follower memory imbalance
/// where a follower of a hot partition ballooned while its leader stayed small.
/// Wiring the same store into replica engines (and running the gate on followed
/// partitions in the tick loop) lets a follower reclaim RAM exactly like its
/// leader, rehydrating on demand when a replicated command targets a cold
/// instance (see [`Journal::maybe_cold_spill`] / `ensure_resident_for_command`).
#[derive(Clone)]
struct SpillConfig {
    store: Arc<varspill::VarSpillStore>,
    var: Option<VarSpillCfg>,
    cold: Option<(u64, u64)>,
}

impl SpillConfig {
    /// Installs the configured spill tiers onto `journal`. Idempotent per
    /// journal; call once per engine actor at construction.
    fn apply(&self, journal: &mut Journal) {
        if let Some(cfg) = &self.var {
            match cfg {
                VarSpillCfg::Budget(budget) => {
                    journal.set_spill(Arc::clone(&self.store), *budget);
                }
                VarSpillCfg::Adaptive {
                    floor,
                    high,
                    low,
                    reserve,
                    hard_cap,
                } => {
                    journal.set_var_spill_adaptive(
                        Arc::clone(&self.store),
                        *floor,
                        *high,
                        *low,
                        *reserve,
                        *hard_cap,
                    );
                }
            }
        }
        if let Some((high, low)) = self.cold {
            journal.set_cold_spill(Arc::clone(&self.store), high, low);
        }
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
/// - `NANOBPMN_VAR_SPILL` unset: **adaptive** iff a persistent data path exists.
/// - `NANOBPMN_VAR_SPILL=0`/`off`/`false`/`none`/`disabled`/`no`: forced off.
/// - `NANOBPMN_VAR_SPILL=adaptive`/`auto`/`dynamic`: adaptive-hybrid mode.
/// - `NANOBPMN_VAR_SPILL=1`/`on`/`true`/`yes`: forced on in the legacy fixed-budget
///   mode (even in-memory — for tests).
/// - `NANOBPMN_VAR_SPILL_BUDGET=<n>`: fixed-mode hot budget (max resident spillable
///   instances before the oldest backlog is shed); default 512.
/// - `NANOBPMN_VAR_SPILL_MB=<n>`: adaptive pressure high-water in MiB; overrides
///   the default, which is RAM-relative (~65% of the detected cgroup/host memory
///   limit, floored at 128 MiB, or 384 MiB when no limit can be detected). A
///   stable set that merely brushes this cap is relaxed to `low = high * 7/8`.
/// - `NANOBPMN_VAR_SPILL_FLOOR_MB=<n>`: adaptive-hybrid *floor* (burst budget) in
///   MiB — the resident level a growing backlog may reach before spill reclaims
///   to it. Default RAM-relative (~10% of the detected limit, floored at 256 MiB,
///   always clamped below the low-water mark). Below the floor spill never fires.
/// - `NANOBPMN_VAR_SPILL_RESERVE_MB=<n>`: adaptive-hybrid *reserve* in MiB — spill
///   reclaims to the floor when live system-available memory drops below this,
///   regardless of the backlog trend. Default ~12% of the detected limit; `0`
///   disables the available-memory guard.
/// - `NANOBPMN_VAR_SPILL_HARDCAP=<n>`: adaptive per-command instance-count backstop
///   for a runaway between sweeps (default 262144; 0 disables it).
/// - The store is co-located with the read-model db (`<dir>/var-spill.sqlite`)
///   when persistent, else in-memory.
fn spill_from_env() -> Option<(Option<PathBuf>, VarSpillCfg)> {
    let (_, db) = resolve_data_paths();
    // (on, adaptive): whether spill is enabled, and if so whether it is the
    // adaptive RSS-watermark mode (vs the legacy fixed-budget mode).
    let (on, adaptive) = match std::env::var("NANOBPMN_VAR_SPILL").ok().as_deref() {
        Some("0") | Some("off") | Some("false") | Some("none") | Some("disabled") | Some("no") => {
            (false, false)
        }
        Some("adaptive") | Some("auto") | Some("dynamic") => (true, true),
        Some("1") | Some("on") | Some("true") | Some("yes") => (true, false),
        // Unset / unrecognised: default to adaptive only when a persistent path
        // exists (an in-memory spill store would double the payload footprint).
        _ => (db.is_some(), db.is_some()),
    };
    if !on {
        return None;
    }
    let path = db.map(|db| db.with_file_name("var-spill.sqlite"));
    let cfg = if adaptive {
        let high = std::env::var("NANOBPMN_VAR_SPILL_MB")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or_else(default_spill_high_bytes);
        let low = high / 8 * 7;
        let floor = std::env::var("NANOBPMN_VAR_SPILL_FLOOR_MB")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .map(|mb| (mb * 1024 * 1024).min(low.saturating_sub(1)).max(1))
            .unwrap_or_else(|| default_spill_floor_bytes(low));
        let reserve = std::env::var("NANOBPMN_VAR_SPILL_RESERVE_MB")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or_else(default_spill_reserve_bytes);
        let hard_cap = std::env::var("NANOBPMN_VAR_SPILL_HARDCAP")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(262_144);
        VarSpillCfg::Adaptive {
            floor,
            high,
            low,
            reserve,
            hard_cap,
        }
    } else {
        let budget = std::env::var("NANOBPMN_VAR_SPILL_BUDGET")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(512);
        VarSpillCfg::Budget(budget)
    };
    Some((path, cfg))
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
/// - `NANOBPMN_COLD_SPILL_MB=<n>`: high-water in MiB; overrides the default,
///   which is RAM-relative (~65% of the detected cgroup/host memory limit,
///   floored at 128 MiB, or 384 MiB when no limit can be detected). The sweep
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
    let high = std::env::var("NANOBPMN_COLD_SPILL_MB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .map(|mb| mb * 1024 * 1024)
        .unwrap_or_else(default_spill_high_bytes);
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

        // Backpressure (latency-preservation gate): reject new work once the
        // engine's request-processing concurrency is at or above the current
        // limit. The gated quantity is the number of creates being *applied right
        // now* (the `processing` gauge), not the active backlog — so a no-drain
        // burst is absorbed (it converges to client concurrency, like
        // Zeebe/Camunda) and we shed only when the single engine thread genuinely
        // can't keep up. Memory under a large backlog is bounded independently by
        // the variable-spill tier. The 503 carries a `RESOURCE_EXHAUSTED` title
        // that the client SDK reads as a backpressure signal and answers with a
        // retry backoff. The limit is either a fixed watermark or an adaptive AIMD
        // value sized from measured latency; reading it (and the gauge) is a
        // relaxed atomic load, so this check costs no engine round-trip — a small
        // race against concurrent creates is irrelevant for an approximate limit.
        //
        // Armed in BOTH SLA modes. This is an engine-overload guard, not a backlog
        // bound: because creates and completions share the single writer, it sheds
        // creates when create-*processing* concurrency saturates, protecting the
        // writer thread. It does NOT bound the accumulated backlog (it keys off
        // processing concurrency, which stays low even while completions fall
        // behind), so under sustained overload the backlog grows to the
        // memory-safety rails regardless — the active-backlog governor (latency
        // mode only, in `admission_shed`) is the sole tight backlog bound. Keeping
        // this guard armed in `admission` still pays off: measured ~+6% throughput
        // and a ~40% tighter p90 tail vs suppressing it, at no cost (GCP 3-node
        // fresh-journal A/B; see PERFORMANCE.md 2026-07-10 / ADR 0013 Addendum). So
        // `admission` relaxes only the proactive backlog governor, not this guard.
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
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionById(
                b,
            ) => (
                b.await_completion,
                b.fetch_variables.as_ref(),
                b.request_timeout,
            ),
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionByKey(
                b,
            ) => (
                b.await_completion,
                b.fetch_variables.as_ref(),
                b.request_timeout,
            ),
        };
        let await_completion = await_completion.unwrap_or(false);

        // Decode the request variables off the engine thread so the 50 KB JSON →
        // engine `Value` conversion runs in parallel rather than serially on the
        // single command thread. The variables come from the request body and so
        // are available for both creation variants without touching the engine.
        let variables = match body {
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionById(
                b,
            ) => b.variables.as_ref(),
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionByKey(
                b,
            ) => b.variables.as_ref(),
        }
        .map(from_object_map)
        .unwrap_or_default();

        // Extract tags and business_id from the request body (both variants have
        // these fields). Convert Option<Vec<Tag>> to Vec<String> for the engine.
        let (tags, business_id) = match body {
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionById(
                b,
            ) => (b.tags.clone(), b.business_id.clone()),
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionByKey(
                b,
            ) => (b.tags.clone(), b.business_id.clone()),
        };
        let tags_vec: Vec<String> = tags.unwrap_or_default().into_iter().map(|t| t.0).collect();
        let business_id_str = business_id;

        // Only the by-key variant needs the engine (to resolve a deployed key to a
        // process id); capture the lookup inputs the engine thread will need.
        let (by_id, by_key) = match body {
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionById(
                b,
            ) => (Some(b.process_definition_id.clone()), None),
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionByKey(
                b,
            ) => (None, Some(b.process_definition_key.0.clone())),
        };

        // Under Raft (RF>=2) the create must be REPLICATED through a partition's
        // Raft log and placed by *leadership*, not statically-owned round-robin:
        // route through the shared Raft create core (leadership-following + leader
        // forward) instead of the stage-1 direct-apply path below. This closes the
        // durability gap where a locally-placed REST create was applied without a
        // quorum and would be lost on this node's failure. The Raft-off path below
        // is left untouched (byte-identical single-node / RF=1 behaviour).
        if !self.raft.is_empty() {
            let wire_vars = match body {
                models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionById(b) => {
                    b.variables.as_ref()
                }
                models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionByKey(b) => {
                    b.variables.as_ref()
                }
            }
            .and_then(|m| wire_variables(Some(m)));
            // Cluster-wide create placement under Raft, mirroring the Raft-off
            // path below: round-robin across EVERY partition in the cluster so a
            // single gateway spreads creates over the whole cluster instead of
            // concentrating them on the partitions THIS node leads. A placement
            // that lands on a peer-owned partition is forwarded there; the peer's
            // `create_forwarded` is Raft-aware and replicates it through its own
            // partition leader's log (durability preserved). A local placement
            // (`None`) falls through to `create_rest_via_raft`, which proposes on
            // a led partition. Without this, a producer connected to one gateway
            // placed every instance on that node's led partitions only, starving
            // the rest of the cluster (RF>=2 create imbalance).
            //
            // With placement protection (ADR 0014) enabled the forward runs
            // through the reroute loop: a saturated owner sheds the create back
            // and it is re-placed onto an owner with headroom; `None` means fall
            // through to a local Raft create.
            if self.placement_mode.protects() {
                let first = if self.placement_mode.balances() {
                    self.next_create_placement_weighted(&[])
                } else {
                    self.engine.next_create_placement()
                };
                if let Some(resp) = self
                    .forward_create_rerouting(
                        first,
                        by_id.clone(),
                        by_key.clone(),
                        wire_vars.clone(),
                        tags_vec.clone(),
                        business_id_str.clone(),
                        await_completion,
                        fetch_variables.cloned(),
                        request_timeout,
                    )
                    .await
                {
                    return Ok(resp);
                }
            } else if let Some(node) = self.engine.next_create_placement() {
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
            return Ok(self
                .create_rest_via_raft(
                    by_id,
                    by_key,
                    variables,
                    wire_vars,
                    tags_vec,
                    business_id_str,
                    await_completion,
                    fetch_variables.cloned(),
                    request_timeout,
                )
                .await);
        }

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
        //
        // With placement protection (ADR 0014) enabled the forward runs through
        // the reroute loop: a saturated owner sheds the create back and it is
        // re-placed onto an owner with headroom (weighted by gossiped load in
        // `balanced`); `None` falls through to the local in-process create.
        if self.placement_mode.protects() {
            let wire_vars = match body {
                models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionById(b) => {
                    b.variables.as_ref()
                }
                models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionByKey(b) => {
                    b.variables.as_ref()
                }
            }
            .and_then(|m| wire_variables(Some(m)));
            let first = if self.placement_mode.balances() {
                self.next_create_placement_weighted(&[])
            } else {
                self.engine.next_create_placement()
            };
            if let Some(resp) = self
                .forward_create_rerouting(
                    first,
                    by_id.clone(),
                    by_key.clone(),
                    wire_vars,
                    tags_vec.clone(),
                    business_id_str.clone(),
                    await_completion,
                    fetch_variables.cloned(),
                    request_timeout,
                )
                .await
            {
                return Ok(resp);
            }
        } else if let Some(node) = self.engine.next_create_placement() {
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
            let payload_bytes = engine_vars_bytes(&variables);
            let _processing = ProcessingGuard::enter(&self.processing);
            let _bytes = ByteGuard::enter(&self.pipeline_bytes, payload_bytes);
            self.engine
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
                                    return Err(Box::new(
                                        Resp::Status400_TheProvidedDataIsNotValid(problem(
                                            "Process not found",
                                            400,
                                            format!("No deployed process with key '{requested}'."),
                                        )),
                                    ));
                                }
                            }
                        }
                        (None, None) => unreachable!("one creation variant is always set"),
                    };

                    match engine.apply_command_at(
                        Command::create_instance_full(
                            process_id.clone(),
                            variables,
                            tags_vec,
                            business_id_str,
                        ),
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
                        Err(EngineError::ProcessNotFound { process_id }) => Err(Box::new(
                            Resp::Status400_TheProvidedDataIsNotValid(problem(
                                "Process not found",
                                400,
                                format!("No deployed process with id '{process_id}'."),
                            )),
                        )),
                        Err(e) => Err(Box::new(
                            Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                                problem("Internal error", 500, e.to_string()),
                            ),
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
            business_id_for_response
                .map(nanobpm_gateway_rest::types::Nullable::Present)
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
            Some(names) if !names.is_empty() => Some(names.iter().map(String::as_str).collect()),
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

        if let Some(node) = self.route_by_leader(instance_key) {
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
        if let Some(node) = self.route_by_leader(job_key) {
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
                engine
                    .apply_command_at(Command::complete_job_with(job_key, variables), now_millis())
            })
            .await;
        match result {
            Ok((events, commit)) => {
                // Record REST job completion (drain-side; also feeds the drain guard).
                self.note_job_completion("rest");
                // REST API: await fsync before replying (synchronous durability).
                // Contrast with falcon::pipeline_job_command, which replies
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

        if let Some(node) = self.route_by_leader(job_key) {
            return Ok(self
                .forward_fail_job(node, job_key, retries, error_message)
                .await);
        }

        let result = self
            .engine
            .by_key(job_key)
            .with(move |engine| {
                engine.apply_command_at(
                    Command::fail_job(job_key, retries, error_message),
                    now_millis(),
                )
            })
            .await;
        match result {
            Ok((_, commit)) => {
                // Record REST job completion (fail also completes the job lifecycle;
                // drain-side, so it feeds the drain guard too).
                self.note_job_completion("rest");
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

        if let Some(node) = self.route_by_leader(job_key) {
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

        if let Some(node) = self.route_by_leader(job_key) {
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

        if let Some(node) = self.route_by_leader(incident_key) {
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
        let local = body.local.unwrap_or(false);

        if let Some(node) = self.route_by_leader(scope_key) {
            return Ok(self
                .forward_set_variables(
                    node,
                    scope_key,
                    wire_variables(Some(&body.variables)),
                    local,
                )
                .await);
        }

        let result = self
            .engine
            .by_key(scope_key)
            .with(move |engine| {
                engine.apply_command_at(
                    Command::set_variables_scoped(scope_key, variables, local),
                    now_millis(),
                )
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
        if result.is_none()
            && let Some(node) = self.read_route(key)
        {
            let (status, body) = self
                .forward_get(node, crate::falcon::ReadKind::ProcessInstance, key)
                .await;
            return Ok(match (status, body) {
                (200, Some(b)) => match serde_json::from_value(b) {
                    Ok(r) => Resp::Status200_TheProcessInstanceIsSuccessfullyReturned(r),
                    Err(e) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                        problem("Peer error", 500, e.to_string()),
                    ),
                },
                (404, _) => Resp::Status404_TheProcessInstanceWithTheGivenKeyWasNotFound(problem(
                    "Process instance not found",
                    404,
                    format!("No process instance with key {key}."),
                )),
                (s, _) => {
                    Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                        "Peer error",
                        500,
                        format!("peer node {node} returned status {s}"),
                    ))
                }
            });
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

    /// Serves the verbatim BPMN XML for a deployed process definition
    /// (`getProcessDefinitionXML`). Read from the read model, which projects it
    /// from the journaled `ProcessDeployed` event (so it is durable and the same
    /// on every node). Only the latest version per process id is retained, so an
    /// older version's key yields 404. A definition built programmatically (no
    /// source XML) is reported as 204.
    async fn get_process_definition_xml_impl(
        &self,
        path_params: &models::GetProcessDefinitionXmlPathParams,
    ) -> Result<apis::process_definition::GetProcessDefinitionXmlResponse, ()> {
        use apis::process_definition::GetProcessDefinitionXmlResponse as Resp;

        let key: u64 = match path_params.process_definition_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(
                    Resp::Status404_TheProcessDefinitionWithTheGivenKeyWasNotFound(problem(
                        "Process definition not found",
                        404,
                        format!(
                            "Process definition key '{}' is not a valid key.",
                            path_params.process_definition_key
                        ),
                    )),
                );
            }
        };

        match self.store.process_definition_xml(key) {
            Some(xml) if !xml.is_empty() => {
                Ok(Resp::Status200_TheXMLOfTheProcessDefinitionIsSuccessfullyReturned(xml))
            }
            Some(_) => {
                Ok(Resp::Status204_TheProcessDefinitionWasFoundButDoesNotHaveXML(String::new()))
            }
            None => Ok(
                Resp::Status404_TheProcessDefinitionWithTheGivenKeyWasNotFound(problem(
                    "Process definition not found",
                    404,
                    format!("No process definition with key {key}."),
                )),
            ),
        }
    }

    /// Reports the real cluster topology: one broker per node, each advertising
    /// every partition it is a replica of (deterministic `replicas_of` placement).
    /// Partition ids are surfaced 1-based (Camunda convention) over nano's 0-based
    /// internal partitions. The replica that leads a partition (`leader_of`, the
    /// replica-set head today) is reported as `leader`; the other replicas under
    /// RF>1 are reported as `follower`. `replication_factor` is the cluster's
    /// `effective_rf()`. A single-node cluster reports one broker leading every
    /// partition at RF=1.
    async fn get_topology_impl(&self) -> Result<apis::cluster::GetTopologyResponse, ()> {
        use apis::cluster::GetTopologyResponse as Resp;

        let version = env!("NANOBPM_VERSION").to_string();
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
            let url = topology
                .peers
                .get(node as usize)
                .map(String::as_str)
                .unwrap_or("");
            parse_host_port(url).unwrap_or(("0.0.0.0".to_string(), self_port))
        };

        let brokers: Vec<models::BrokerInfo> = (0..num_nodes)
            .map(|node| {
                // Every partition this node is a replica of (leader OR follower),
                // not just the ones it owns. `leader_of` (== owner today) marks the
                // leader; the other replicas in `replicas_of(p)` are followers. This
                // makes replication visible in the topology under RF>1 instead of
                // the old owner-only, all-"leader" view.
                let partitions: Vec<models::Partition> = (0..num_partitions)
                    .filter(|p| topology.replicas_of(*p).contains(&node))
                    .map(|p| models::Partition {
                        // 1-based partition id (Camunda convention).
                        partition_id: (p + 1) as i32,
                        role: if topology.leader_of(p) == node {
                            "leader".to_string()
                        } else {
                            "follower".to_string()
                        },
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
            replication_factor: topology.effective_rf() as i32,
            gateway_version: version.clone(),
            last_completed_change_id: String::new(),
            // Advertise that this is a nanobpmn gateway (a superset of the Camunda
            // Orchestration Cluster API). Its presence lets SDK clients detect nano
            // from a single /v2/topology call and upgrade to the Falcon protocol.
            nano: Some(models::NanoEngineInfo {
                engine: "nanobpmn".to_string(),
                version: Some(version),
                falcon_path: "/falcon".to_string(),
            }),
        };

        Ok(
            Resp::Status200_ObtainsTheCurrentTopologyOfTheClusterTheGatewayIsPartOf(
                topology_response,
            ),
        )
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
    /// **peer**, the source event is forwarded over the Falcon protocol and the
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
    /// owns `target_partition`, over the Falcon protocol. The owner applies it and
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
    // forwards the operation to that peer over the Falcon protocol; the peer
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
        local: bool,
    ) -> Result<(), (u16, String)> {
        let result = self
            .engine
            .by_key(scope_key)
            .with(move |engine| {
                engine.apply_command_at(
                    Command::set_variables_scoped(scope_key, variables, local),
                    now_millis(),
                )
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

    /// Routing target for a by-key job command (complete / fail / throw): `None`
    /// means handle it locally, `Some(node)` means forward to peer `node`.
    ///
    /// When this node hosts Raft groups, routing follows the partition's CURRENT
    /// Raft leader rather than the static owner map, so a command for a partition
    /// whose leadership moved (after the owner failed and a replica was elected)
    /// reaches the new leader instead of the dead owner. A transient no-leader
    /// window routes locally so [`propose_job_for_stream`](Self::propose_job_for_stream)
    /// returns a retryable 503. With Raft off this is exactly `remote_owner_of`,
    /// so the non-Raft path is unchanged.
    ///
    /// NOTE (s3-perf/robustness): the forwarded peer re-routes via this same map,
    /// so during an election flux two nodes with stale, disagreeing metrics could
    /// briefly ping-pong a command. A forward hop-limit is future work; in the
    /// stable post-election state every node agrees on the leader and routes once.
    pub(crate) fn job_route(&self, key: u64) -> Option<u32> {
        self.route_by_leader(key)
    }

    /// Shared by-key routing that follows the partition's CURRENT Raft leader
    /// when this node hosts the group, falling back to the static owner map
    /// otherwise. `None` = handle locally, `Some(node)` = forward to peer `node`.
    /// With Raft off this is exactly `remote_owner_of`, so the non-Raft path is
    /// byte-identical.
    fn route_by_leader(&self, key: u64) -> Option<u32> {
        if !self.raft.is_empty() {
            let p = partition_of(key);
            if let Some(part) = self.raft.get(p) {
                let node_id = self.engine.topology().node_id as u64;
                return match part.raft.metrics().borrow().current_leader {
                    Some(l) if l == node_id => None,
                    Some(l) => Some(l as u32),
                    // Leader momentarily unknown (an election in flight during a
                    // failover). "Handle locally" (`None`) is only safe when THIS
                    // node actually hosts the partition's engine; for a partition it
                    // does not own, `None` would drive a by-key op onto the wrong
                    // engine (`local_for` panics under debug_assert, or silently
                    // targets partition 0 in release). Fall back to the static-owner
                    // forward so the op leaves this node instead of mis-applying;
                    // the caller/client retries until the new leader is known.
                    None => self.remote_owner_of(key),
                };
            }
        }
        self.remote_owner_of(key)
    }

    /// Routing target for a by-key READ (GET process-instance / incident /
    /// user-task / variable): `None` = serve from the local read model,
    /// `Some(node)` = forward the read to peer `node`. Reads follow the
    /// partition's CURRENT Raft leader, so after a leadership move the read
    /// reaches the node whose applied read model is freshest rather than the
    /// dead static owner. Callers consult this only AFTER missing their local
    /// store, so a self-leader / no-leader result (`None`) correctly yields a
    /// genuine 404 from local state. With Raft off this is exactly
    /// `remote_owner_of`, leaving the non-Raft read path unchanged.
    pub(crate) fn read_route(&self, key: u64) -> Option<u32> {
        self.route_by_leader(key)
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
        kind: crate::falcon::ReadKind,
        key: u64,
    ) -> (u16, Option<serde_json::Value>) {
        use crate::falcon::ReadKind;
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
        kind: crate::falcon::ReadKind,
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
    /// locally (the forward targets the partition's current leader, so the
    /// per-handler `route_by_leader` check resolves Local — no forwarding loop
    /// in the stable post-election state) and reports the REST status plus an
    /// optional problem detail. The gateway maps the status back to its typed
    /// response.
    pub(crate) async fn apply_user_task_forwarded(
        &self,
        op: crate::falcon::UserTaskOp,
        user_task_key: &str,
        payload: Option<serde_json::Value>,
    ) -> (u16, Option<String>) {
        use crate::falcon::UserTaskOp;
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
                let path = models::AssignUserTaskPathParams { user_task_key: key };
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
                let path = models::CompleteUserTaskPathParams { user_task_key: key };
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
                let path = models::UnassignUserTaskPathParams { user_task_key: key };
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
                let path = models::UpdateUserTaskPathParams { user_task_key: key };
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
        op: crate::falcon::UserTaskOp,
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
        let res =
            match self.peer_link(node).await {
                Ok(link) => link.complete_job(job_key.to_string(), variables).await,
                Err((s, m)) => {
                    return Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                        problem("Peer error", s, m),
                    );
                }
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
        let res =
            match self.peer_link(node).await {
                Ok(link) => {
                    link.fail_job(job_key.to_string(), retries, error_message)
                        .await
                }
                Err((s, m)) => {
                    return Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                        problem("Peer error", s, m),
                    );
                }
            };
        match res {
            Ok(r) if is_ok_status(r.status) => Resp::Status204_TheJobIsFailed,
            Ok(r) if r.status == 404 => Resp::Status404_TheJobWithTheGivenJobKeyIsNotFound(
                problem("Job not found", 404, peer_detail(&r)),
            ),
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
        let res =
            match self.peer_link(node).await {
                Ok(link) => {
                    link.throw_error(job_key.to_string(), error_code, error_message)
                        .await
                }
                Err((s, m)) => {
                    return Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                        problem("Peer error", s, m),
                    );
                }
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
    /// response), this relays the peer's raw `(status, body)` so the falcon
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
            Ok(link) => match link
                .fail_job(job_key.to_string(), retries, error_message)
                .await
            {
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
            Ok(link) => match link
                .throw_error(job_key.to_string(), error_code, error_message)
                .await
            {
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
            Ok(r) if is_ok_status(r.status) => {
                let body = match r.body {
                    Some(b) => b,
                    None => return Vec::new(),
                };
                // Tolerant parse: the current wire shape is
                // `{ jobs: [...], backlog: N }`; fall back to a bare jobs array for
                // safety. The piggybacked backlog feeds Stage 2 fairness weighting.
                if let Some(backlog) = body.get("backlog").and_then(|v| v.as_i64()) {
                    crate::falcon::record_peer_backlog(node, backlog);
                }
                let jobs_val = body.get("jobs").cloned().unwrap_or(body);
                serde_json::from_value::<Vec<models::ActivatedJobResult>>(jobs_val)
                    .unwrap_or_default()
            }
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
        let res =
            match self.peer_link(node).await {
                Ok(link) => link.cancel_instance(instance_key.to_string()).await,
                Err((s, m)) => {
                    return Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                        problem("Peer error", s, m),
                    );
                }
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
        let res =
            match self.peer_link(node).await {
                Ok(link) => link.update_job_retries(job_key.to_string(), retries).await,
                Err((s, m)) => {
                    return Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                        problem("Peer error", s, m),
                    );
                }
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
        let res =
            match self.peer_link(node).await {
                Ok(link) => {
                    link.resolve_incident(incident_key.to_string(), operation_reference)
                        .await
                }
                Err((s, m)) => {
                    return Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                        problem("Peer error", s, m),
                    );
                }
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
        local: bool,
    ) -> apis::element_instance::CreateElementInstanceVariablesResponse {
        use apis::element_instance::CreateElementInstanceVariablesResponse as Resp;
        let res =
            match self.peer_link(node).await {
                Ok(link) => {
                    link.set_variables(scope_key.to_string(), variables, local)
                        .await
                }
                Err((s, m)) => {
                    return Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                        problem("Peer error", s, m),
                    );
                }
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
    /// it straight back to its REST response. Mirrors the local REST create
    /// core + finalize. Backpressure/admission are applied at the receiving
    /// gateway, not here.
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
        // Placement protection (ADR 0014): a node that is saturated sheds the
        // forwarded create back to the ingress node (503 with the placement-shed
        // marker) so ingress can reroute it to an owner with headroom, instead of
        // being overrun by work placed on it. This closes the gap where forwarded
        // creates bypassed the owner's own admission gates. Off by default.
        if self.placement_mode.protects()
            && let Some(reason) = self.create_should_shed()
        {
            return Err((503, format!("{PLACEMENT_SHED_MARKER} {reason}")));
        }
        let tags_for_response = tags.clone();
        let business_id_for_response = business_id.clone();
        type CreateOk = (String, i32, String, u64, bool, Vec<Event>, Commit);
        // Under Raft, a forwarded create must be REPLICATED through this node's
        // Raft log, not applied directly to the local engine — otherwise the
        // instance would not survive this node's failure. Route it through the
        // shared Raft create core (durability is awaited inside the propose, so
        // the returned commit is already ready).
        let outcome: Result<CreateOk, (u16, String)> = if !self.raft.is_empty() {
            self.raft_create_core(by_id, by_key, variables, tags, business_id)
                .await
                .map(
                    |(
                        process_id,
                        version,
                        definition_key,
                        instance_key,
                        sync_completed,
                        routable,
                    )| {
                        (
                            process_id,
                            version,
                            definition_key,
                            instance_key,
                            sync_completed,
                            routable,
                            Commit::ready(),
                        )
                    },
                )
        } else {
            let payload_bytes = engine_vars_bytes(&variables);
            let _processing = ProcessingGuard::enter(&self.processing);
            let _bytes = ByteGuard::enter(&self.pipeline_bytes, payload_bytes);
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
        let outcome = self
            .forward_create_once(
                node,
                by_id,
                by_key,
                variables,
                tags,
                business_id,
                await_completion,
                fetch_variables,
                request_timeout,
            )
            .await;
        match outcome {
            ForwardCreateOutcome::Created(result) => {
                Resp::Status200_TheProcessInstanceWasCreated(result)
            }
            ForwardCreateOutcome::Reject400(detail) => {
                Resp::Status400_TheProvidedDataIsNotValid(problem("Invalid create", 400, detail))
            }
            // Without placement protection a peer never placement-sheds, so this
            // is only reached defensively; surface it as a retryable error.
            ForwardCreateOutcome::Shed(detail) => {
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Peer error",
                    503,
                    detail,
                ))
            }
            ForwardCreateOutcome::Unreachable(detail) => {
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Peer error",
                    502,
                    detail,
                ))
            }
            ForwardCreateOutcome::Error(status, detail) => {
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Peer error",
                    status,
                    detail,
                ))
            }
        }
    }

    /// Forwards a `createProcessInstance` to peer `node` and classifies the
    /// answer into a [`ForwardCreateOutcome`], distinguishing a rerouteable
    /// **placement shed** (the peer is saturated, ADR 0014 `protect`) and an
    /// unreachable peer from a terminal result. The ingress reroute loop uses the
    /// `Shed`/`Unreachable` outcomes to re-place onto another owner;
    /// [`forward_create`](Self::forward_create) maps the outcome straight to an
    /// HTTP response for the non-protected path.
    #[allow(clippy::too_many_arguments)]
    async fn forward_create_once(
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
    ) -> ForwardCreateOutcome {
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
            Err((_, m)) => return ForwardCreateOutcome::Unreachable(m),
        };
        match res {
            Ok(r) if is_ok_status(r.status) => {
                match r.body.and_then(|b| {
                    serde_json::from_value::<models::CreateProcessInstanceResult>(b).ok()
                }) {
                    Some(result) => ForwardCreateOutcome::Created(result),
                    None => ForwardCreateOutcome::Error(
                        500,
                        "peer returned an unparseable create result".into(),
                    ),
                }
            }
            Ok(r) if r.status == 400 => ForwardCreateOutcome::Reject400(peer_detail(&r)),
            // The peer self-protected and shed this create back for rerouting
            // (ADR 0014): a 503 tagged with the placement-shed marker.
            Ok(r) if r.status == 503 => {
                let detail = peer_detail(&r);
                if detail.starts_with(PLACEMENT_SHED_MARKER) {
                    ForwardCreateOutcome::Shed(detail)
                } else {
                    ForwardCreateOutcome::Error(503, detail)
                }
            }
            Ok(r) => ForwardCreateOutcome::Error(500, peer_detail(&r)),
            // A transport error AFTER the request was sent is ambiguous — the peer
            // may have applied the create — so it is NOT rerouted (that would risk
            // a duplicate instance); it surfaces as a retryable error instead.
            Err(e) => ForwardCreateOutcome::Error(502, e.to_string()),
        }
    }

    /// Load-aware weighted create placement (ADR 0014 `balanced`). Enumerates
    /// every partition's owner, weights each inversely to its gossiped composite
    /// load (a shedding, or already-`tried`, owner gets weight 0 and is never
    /// picked; an unprobed peer is treated as full headroom), and selects one via
    /// smooth weighted round-robin ([`crate::placement::swrr_pick`]) over the
    /// persistent per-partition smoothing state. Equal loads therefore yield an
    /// exact round-robin (creates spread evenly); skewed loads steer smoothly
    /// toward headroom. Returns `Some(node)` to forward to a peer, or `None` to
    /// create locally (a local slot won the weighting, or no eligible remote owner
    /// remained).
    fn next_create_placement_weighted(&self, tried: &[u32]) -> Option<u32> {
        let n = self.engine.partition_count();
        if n <= 1 {
            return None;
        }
        let owners: Vec<Option<u32>> = (0..n as u64).map(|p| self.engine.owner_of(p)).collect();
        let local_weight = crate::placement::placement_weight(self.create_load_index());
        let weights: Vec<u128> = owners
            .iter()
            .map(|owner| match owner {
                None => local_weight,
                Some(node) if tried.contains(node) => 0,
                Some(node) => {
                    // A peer that has not gossiped yet is treated as full
                    // headroom (load 0) so it still receives traffic; the
                    // reactive shed/reroute layer corrects an over-optimistic
                    // guess.
                    crate::placement::placement_weight(self.peer_load(*node).unwrap_or(0))
                }
            })
            .collect();
        let mut cur = self
            .placement_swrr
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if cur.len() != n {
            cur.resize(n, 0);
        }
        crate::placement::swrr_pick(&weights, &mut cur).and_then(|i| owners[i])
    }

    /// Ingress reroute loop for create-placement protection (ADR 0014). Forwards
    /// the create to `first`, and if the owner placement-sheds (or is
    /// unreachable) re-places onto another owner — blind round-robin skipping
    /// tried owners in `protect`, load-weighted in `balanced` — until a peer
    /// accepts, a terminal result arrives, or every owner is exhausted. Returns
    /// `Some(resp)` when the create is resolved remotely (success / 400 / error /
    /// final shed), or `None` to fall through to a **local** create — either
    /// because placement chose this node, or as the last-resort backstop when the
    /// whole cluster shed (the ingress node already passed its own admission gate,
    /// so a local create is the honest floor rather than a spurious 503).
    #[allow(clippy::too_many_arguments)]
    async fn forward_create_rerouting(
        &self,
        first: Option<u32>,
        by_id: Option<String>,
        by_key: Option<String>,
        variables: Option<serde_json::Map<String, serde_json::Value>>,
        tags: Vec<String>,
        business_id: Option<String>,
        await_completion: bool,
        fetch_variables: Option<Vec<String>>,
        request_timeout: Option<i64>,
    ) -> Option<apis::process_instance::CreateProcessInstanceResponse> {
        use apis::process_instance::CreateProcessInstanceResponse as Resp;
        let mut tried: Vec<u32> = Vec::new();
        let bound = self.engine.partition_count().max(1) + 1;
        let mut target = first;
        for _ in 0..bound {
            let Some(node) = target else {
                // Local placement (or exhausted) -> caller does a local create.
                return None;
            };
            let outcome = self
                .forward_create_once(
                    node,
                    by_id.clone(),
                    by_key.clone(),
                    variables.clone(),
                    tags.clone(),
                    business_id.clone(),
                    await_completion,
                    fetch_variables.clone(),
                    request_timeout,
                )
                .await;
            match outcome {
                ForwardCreateOutcome::Created(result) => {
                    return Some(Resp::Status200_TheProcessInstanceWasCreated(result));
                }
                ForwardCreateOutcome::Reject400(detail) => {
                    return Some(Resp::Status400_TheProvidedDataIsNotValid(problem(
                        "Invalid create",
                        400,
                        detail,
                    )));
                }
                ForwardCreateOutcome::Error(status, detail) => {
                    return Some(
                        Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                            "Peer error",
                            status,
                            detail,
                        )),
                    );
                }
                // Rerouteable: the owner shed (saturated) or was unreachable.
                ForwardCreateOutcome::Shed(_) | ForwardCreateOutcome::Unreachable(_) => {
                    tried.push(node);
                    target = if self.placement_mode.balances() {
                        self.next_create_placement_weighted(&tried)
                    } else {
                        self.engine.next_create_placement_avoiding(&tried)
                    };
                }
            }
        }
        // Every candidate shed: fall through to a local create (last resort).
        None
    }

    /// Forwards a non-`await_completion` create to the current create-leader with
    /// a short per-attempt deadline ([`write_forward_timeout`]) and leader
    /// re-resolution across a total budget ([`write_forward_retry_budget`]). When
    /// the targeted leader has just failed (e.g. a node left the network), the
    /// per-attempt forward times out fast instead of pinning the caller for the
    /// 30s general peer timeout; the loop then re-resolves the leader — which the
    /// surviving replicas elect within the election timeout — and retries against
    /// it. Converts a single-node loss from a closed-loop throughput collapse into
    /// a brief blip while leadership moves. Business/validation rejections (400/409)
    /// short-circuit; transient transport/leadership errors retry until the budget
    /// is spent, then surface a retryable 503.
    #[allow(clippy::too_many_arguments)]
    async fn forward_create_bounded(
        &self,
        by_id: Option<String>,
        by_key: Option<String>,
        wire_vars: Option<serde_json::Map<String, serde_json::Value>>,
        tags: Vec<String>,
        business_id: Option<String>,
        fetch_variables: Option<Vec<String>>,
        request_timeout: Option<i64>,
    ) -> apis::process_instance::CreateProcessInstanceResponse {
        use apis::process_instance::CreateProcessInstanceResponse as Resp;
        let started = std::time::Instant::now();
        let budget = write_forward_retry_budget();
        let per_try = write_forward_timeout();
        let mut last_detail = "no partition leader reachable; retry".to_string();

        loop {
            let Some(node) = self.leader_node_for_create() else {
                // No known leader yet (mid-election): wait briefly, then retry
                // until the budget is spent.
                if started.elapsed() >= budget {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            };

            let link = match self.peer_link(node).await {
                Ok(link) => link,
                Err((_, m)) => {
                    last_detail = m;
                    if started.elapsed() >= budget {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
            };

            let res = link
                .forward_create_within(
                    per_try,
                    by_id.clone(),
                    by_key.clone(),
                    wire_vars.clone(),
                    tags.clone(),
                    business_id.clone(),
                    fetch_variables.clone(),
                    request_timeout,
                )
                .await;

            match res {
                Ok(r) if is_ok_status(r.status) => {
                    return match r.body.and_then(|b| {
                        serde_json::from_value::<models::CreateProcessInstanceResult>(b).ok()
                    }) {
                        Some(result) => Resp::Status200_TheProcessInstanceWasCreated(result),
                        None => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                            problem(
                                "Peer error",
                                500,
                                "peer returned an unparseable create result".into(),
                            ),
                        ),
                    };
                }
                // Deterministic client rejection: do not retry.
                Ok(r) if r.status == 400 => {
                    return Resp::Status400_TheProvidedDataIsNotValid(problem(
                        "Invalid create",
                        400,
                        peer_detail(&r),
                    ));
                }
                Ok(r) if r.status == 409 => {
                    return Resp::Status409_TheProcessInstanceCreationWasRejectedDueToABusinessIDUniquenessConflict(
                        problem("Conflict", 409, peer_detail(&r)),
                    );
                }
                // Transient (peer 5xx, timeout, closed): re-resolve leader & retry.
                Ok(r) => last_detail = peer_detail(&r),
                Err(e) => last_detail = e.to_string(),
            }

            if started.elapsed() >= budget {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }

        Resp::Status503_TheServiceIsCurrentlyUnavailable(problem(
            "RESOURCE_EXHAUSTED",
            503,
            format!("create could not reach a partition leader; retry ({last_detail})"),
        ))
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
        if result.is_none()
            && let Some(node) = self.read_route(key)
        {
            let (status, body) = self
                .forward_get(node, crate::falcon::ReadKind::Incident, key)
                .await;
            return Ok(match (status, body) {
                (200, Some(b)) => match serde_json::from_value(b) {
                    Ok(r) => Resp::Status200_TheIncidentIsSuccessfullyReturned(r),
                    Err(e) => Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                        problem("Peer error", 500, e.to_string()),
                    ),
                },
                (404, _) => Resp::Status404_TheIncidentWithTheGivenKeyWasNotFound(problem(
                    "Incident not found",
                    404,
                    format!("No incident with key {key}."),
                )),
                (s, _) => {
                    Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                        "Peer error",
                        500,
                        format!("peer node {node} returned status {s}"),
                    ))
                }
            });
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
                    query::match_basic_string(&f.incident_key, &inc.key.to_string())
                        && query::match_process_instance_key(
                            &f.process_instance_key,
                            &inc.instance_key.to_string(),
                        )
                        && query::match_element_instance_key(
                            &f.element_instance_key,
                            &inc.element_instance_key.to_string(),
                        )
                        && query::match_process_definition_key(
                            &f.process_definition_key,
                            &inc.process_definition_key,
                        )
                        && match &f.job_key {
                            None => true,
                            some => query::match_job_key(
                                some,
                                &inc.job_key.map(|k| k.to_string()).unwrap_or_default(),
                            ),
                        }
                        && query::match_incident_state(
                            &f.state,
                            &incident_state_enum(inc.state).to_string(),
                        )
                        && query::match_incident_error_type(
                            &f.error_type,
                            &incident_error_type_enum(inc.kind).to_string(),
                        )
                        && query::match_string(&f.element_id, &inc.element_id)
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
                "errorType" => query::SortVal::Str(incident_error_type_enum(inc.kind).to_string()),
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
                "processDefinitionId" => query::SortVal::Str(inst.process_definition_id.clone()),
                "processDefinitionKey" => {
                    query::SortVal::Num(inst.process_definition_key.parse().unwrap_or(0))
                }
                "state" => query::SortVal::Str(process_instance_state_enum(inst.state).to_string()),
                _ => query::SortVal::Num(inst.key as i64),
            },
            |inst| inst.key,
        );

        let sorted: Vec<(u64, &readstore::ProcessInstanceRow)> =
            matched.into_iter().map(|inst| (inst.key, inst)).collect();
        let page = query::paginate(sorted, body.as_ref().and_then(|q| q.page.as_ref()));
        // Build result models only for the returned page, never the whole
        // (potentially very large) matched set.
        let items: Vec<models::ProcessInstanceResult> = page
            .items
            .into_iter()
            .map(process_instance_result)
            .collect();

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
                        && query::match_job_state(&f.state, &job_state_enum(job.state).to_string())
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
                "state" => query::SortVal::Str(user_task_state_enum(task.state).to_string()),
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
        if let Some(node) = self.route_by_leader(user_task_key) {
            let payload = serde_json::to_value(body).ok();
            let (status, detail) = self
                .forward_user_task(
                    node,
                    crate::falcon::UserTaskOp::Assign,
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
            Err(EngineError::UserTaskNotFound { user_task_key }) => Ok(
                Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(problem(
                    "User task not found",
                    404,
                    format!("No user task with key {user_task_key}."),
                )),
            ),
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
        if let Some(node) = self.route_by_leader(user_task_key) {
            let payload = serde_json::to_value(body).ok();
            let (status, detail) = self
                .forward_user_task(
                    node,
                    crate::falcon::UserTaskOp::Complete,
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
            Err(EngineError::UserTaskNotFound { user_task_key }) => Ok(
                Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(problem(
                    "User task not found",
                    404,
                    format!("No user task with key {user_task_key}."),
                )),
            ),
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
                if let Some(node) = self.read_route(user_task_key) {
                    let (status, body) = self
                        .forward_get(node, crate::falcon::ReadKind::UserTask, user_task_key)
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
                            problem(
                                "Peer error",
                                500,
                                format!("peer node {node} returned status {s}"),
                            ),
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
        if let Some(node) = self.route_by_leader(user_task_key) {
            let (status, detail) = self
                .forward_user_task(
                    node,
                    crate::falcon::UserTaskOp::Unassign,
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
            Err(EngineError::UserTaskNotFound { user_task_key }) => Ok(
                Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(problem(
                    "User task not found",
                    404,
                    format!("No user task with key {user_task_key}."),
                )),
            ),
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

        if let Some(node) = self.route_by_leader(user_task_key) {
            let payload = serde_json::to_value(body).ok();
            let (status, detail) = self
                .forward_user_task(
                    node,
                    crate::falcon::UserTaskOp::Update,
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
            Err(EngineError::UserTaskNotFound { user_task_key }) => Ok(
                Resp::Status404_TheUserTaskWithTheGivenKeyWasNotFound(problem(
                    "User task not found",
                    404,
                    format!("No user task with key {user_task_key}."),
                )),
            ),
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

    /// Searches variables in the read model. Each variable is reported under the
    /// `scopeKey` of the scope that holds it — the process instance for root-scope
    /// variables, or a sub-process / multi-instance body / child element instance
    /// for a nested scope (Part C hierarchical scoping).
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
                    let tenant_ok = f.tenant_id.as_ref().is_none_or(|t| t == "<default>");
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
                if let Some(node) = self.read_route(key) {
                    let (status, body) = self
                        .forward_get(node, crate::falcon::ReadKind::Variable, key)
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
                            problem(
                                "Peer error",
                                500,
                                format!("peer node {node} returned status {s}"),
                            ),
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
                "processDefinitionId" | "name" => query::SortVal::Str(d.process_id.clone()),
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
        // Falcon protocol and return its answer. Single-node always owns
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
                Err((title, detail)) => Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                    title, 400, detail,
                ))),
            }
        } else {
            match self.forward_deploy(resources, tenant_id).await {
                Ok(result) => Ok(Resp::Status200_TheResourcesAreDeployed(result)),
                Err((status, detail)) => Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                    "Deployment failed",
                    status,
                    detail,
                ))),
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
        let requested_ids: Vec<String> = processes.iter().map(|p| p.id.clone()).collect();
        // The closure returns, besides the emitted events and commit, the
        // resolved (key, version) for every *requested* process id read back from
        // post-apply state. An idempotent redeploy emits no event but must still
        // be reported with its existing version, so the response is built from
        // this resolution rather than from the events alone.
        type Resolved = Vec<(String, u64, i32)>;
        let deploy_result: Result<(Arc<Vec<Event>>, Commit, Resolved), String> = self
            .engine
            .deploy_partition()
            .with(move |engine| {
                let (events, commit) = engine
                    .apply_command(Command::DeployResources(processes))
                    .map_err(|e| e.to_string())?;
                let resolved: Resolved = requested_ids
                    .iter()
                    .filter_map(|id| {
                        engine
                            .state()
                            .processes
                            .get(id)
                            .map(|d| (id.clone(), d.key, d.version))
                    })
                    .collect();
                Ok((events, commit, resolved))
            })
            .await;
        let (events, commit, resolved) = match deploy_result {
            Ok(triple) => triple,
            Err(e) => return Err(("Invalid deployment", e)),
        };
        // Replicate the new definition(s) to the other local partitions so any of
        // them can instantiate the process (the deployment itself is journaled
        // only on partition 0; replication is in-memory and re-derived on restart).
        // An idempotent redeploy emits no events, so these are no-ops.
        self.replicate_deployment(&events).await;
        // Under Raft (RF>1) also fan into any follower replica engine actors so a
        // replicated create of this definition applies on every replica.
        self.install_into_raft_replicas(&events).await;

        // Every deploy emits a DeploymentCreated first (issue #47, Option B):
        // the engine mints the shared deployment key unconditionally so the
        // response envelope always carries a valid LongKey — even on a pure
        // idempotent redeploy that produces no ProcessDeployed events.
        let deployment_key = events
            .iter()
            .find_map(|e| match e {
                Event::DeploymentCreated { deployment_key } => Some(deployment_key.to_string()),
                _ => None,
            })
            .unwrap_or_else(|| "0".to_string());
        let deployments = resolved
            .into_iter()
            .map(|(process_id, process_definition_key, version)| {
                let resource_name = resource_names.get(&process_id).cloned().unwrap_or_default();
                let process_result = models::DeploymentProcessResult::new(
                    process_id,
                    version,
                    resource_name,
                    tenant_id.to_string(),
                    models::ProcessDefinitionKey(process_definition_key.to_string()),
                );
                models::DeploymentMetadataResult::new(
                    nanobpm_gateway_rest::types::Nullable::Present(process_result),
                    nanobpm_gateway_rest::types::Nullable::Null,
                    nanobpm_gateway_rest::types::Nullable::Null,
                    nanobpm_gateway_rest::types::Nullable::Null,
                    nanobpm_gateway_rest::types::Nullable::Null,
                )
            })
            .collect();

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
    /// `Deploy` falcon frame handler. `Err` is `(status, detail)`.
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
    /// Invoked by the `InstallDeployment` falcon frame handler.
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

    /// Forwards a deploy to the partition-0 owner over the Falcon protocol and
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
                                tracing::warn!(
                                    "seed broadcast to node {node} failed: {e}; retrying"
                                )
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
    /// the Falcon protocol; then, for the partitions this node leads, form the
    /// group from its replica set. A no-op unless `NANOBPMN_RAFT` is set, so the
    /// default single-writer path is untouched.
    ///
    /// Runs as a background task because the local falcon endpoint isn't
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
        // Reclaim orphaned snapshot staging dirs from dead nano processes before
        // hosting any partition, so a receiver/failover member's temp snapshots
        // (and any multi-GB aborted-install partials) from prior boots don't
        // accumulate on disk. Off the hot path; runs on a blocking thread so a
        // large `remove_dir_all` never stalls the runtime.
        tokio::task::spawn_blocking(crate::raft::sweep_orphaned_snapshot_dirs);
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
                // Phase E hand-off (NANOBPMN_RECLAIM_HANDOFF): defer hosting a
                // partition this node OWNS until AFTER the incumbent probe below.
                // Hosting it now with its restored on-disk log resurrects node's
                // OLD single-voter lineage, which (a) immediately campaigns against
                // a live failover incumbent (the "lost leadership during catch-up"
                // symptom) and (b) cannot be reconciled with the incumbent's newer
                // lineage by AppendEntries. For an incumbent-led partition we host a
                // FRESH receiver (empty log) after the probe instead; a partition
                // with no incumbent resumes its on-disk lineage there. Followers
                // (non-owned replicas) host now — a learner never campaigns.
                if server.reclaim_via_handoff && topology.leader_of(p) == topology.node_id {
                    continue;
                }
                // A partition this node OWNS is served + exported locally; its
                // exporter drives terminal-instance eviction. A partition this
                // node only REPLICATES (follower under RF>1) has no exporter, so
                // the state machine must evict terminal shells itself or they
                // grow without bound (the RF>1 hot-state leak).
                let owned = server.engine.local_for_partition(p);
                let evict_terminal = owned.is_none();
                let engine = match owned {
                    Some(owned) => owned.clone(),
                    None => server.replica_engine_for(p).await,
                };
                // Purge-hole → snapshot fallback (issue #111) applies only to a
                // partition this node REPLICATES but does not own: such a partition
                // always has a leader elsewhere (its owner or a failover incumbent)
                // to install the snapshot. An OWNED partition reaching this loop
                // (only when the hand-off flag is off) has no other snapshot source,
                // so it must still resume its on-disk lineage and re-form its group.
                let log_dir = if evict_terminal {
                    purge_hole_aware_log_dir(p)
                } else {
                    raft_log_dir_for(p)
                };
                match crate::raft::RaftPartition::bootstrap_member(
                    topology.node_id as u64,
                    p,
                    engine,
                    transport.clone(),
                    log_dir,
                    evict_terminal,
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

            // Settle barrier (ADR 0003): before forming any group, wait until the
            // peers this node replicates with are reachable. A staggered startup
            // otherwise lets a leader `initialize` + `add_learner` as the SOLE
            // reachable voter, committing membership entries into the void while
            // peers are still down; when a peer later boots and its fresh
            // per-partition Raft briefly self-votes before catching the leader's
            // append, the cluster hits a term-`T` split-brain (two committed
            // leaders in one term) that trips openraft's `has_log_id` invariant —
            // a permanent wedge in release (the debug_assert is compiled out, so
            // replication silently stalls). Holding until peers are up collapses a
            // staggered boot into the simultaneous-boot case, which forms cleanly.
            server.settle_before_forming(&topology).await;

            // Phase E (boot-as-receiver, NANOBPMN_RECLAIM_HANDOFF): before forming
            // our own groups, probe co-replicas for owned partitions a peer
            // currently leads (a live failover incumbent from a recent outage). For
            // those we must NOT `initialize` a competing single-voter group — that
            // is the two-lineage election war that spins raft terms up under load.
            // We leave the member uninitialized (a receiver) so the recovery tick
            // drives an openraft leadership hand-off back to us instead. Owned
            // partitions with no incumbent (cold start / genuine ownership) are
            // initialized normally. Empty (no skips) when the flag is off.
            let handoff_incumbents = if server.reclaim_via_handoff {
                server.probe_incumbents_for_owned(&topology).await
            } else {
                std::collections::HashSet::new()
            };

            // Phase E: now host each OWNED partition that was deferred past the
            // probe (skipped in the loop above when the flag is on). An owned
            // partition a reachable incumbent leads is hosted as a FRESH receiver
            // (empty in-memory log) so the incumbent's authoritative lineage
            // replicates cleanly via the hand-off — the divergent on-disk log is
            // discarded (sound in leader-durable: the sole voter's un-shipped tail
            // was already accepted bounded loss at failover). An owned partition
            // with NO incumbent resumes its durable on-disk lineage and is
            // initialized by the loop below. Skipped entirely when the flag is off
            // (those partitions were already hosted above).
            if server.reclaim_via_handoff {
                for p in topology.replica_partitions() {
                    if topology.leader_of(p) != topology.node_id {
                        continue;
                    }
                    // `handle_promotion` (adopting the incumbent's epoch during the
                    // probe) may already have hosted a deferred partition as a fresh
                    // receiver — don't clobber it.
                    if server.raft_registry().get(p).is_some() {
                        continue;
                    }
                    let Some(engine) = server.engine.local_for_partition(p).cloned() else {
                        continue;
                    };
                    let deferred = handoff_incumbents.contains(&p);
                    let log_dir = if deferred { None } else { raft_log_dir_for(p) };
                    match crate::raft::RaftPartition::bootstrap_member(
                        topology.node_id as u64,
                        p,
                        engine,
                        transport.clone(),
                        log_dir,
                        false, // owned: has an exporter, never evicts in `apply`
                    )
                    .await
                    {
                        Ok(part) => {
                            server.raft_registry().insert(Arc::new(part));
                            tracing::info!(
                                "raft: node {} hosting owned partition {p} ({})",
                                topology.node_id,
                                if deferred {
                                    "fresh receiver, deferred to leadership hand-off"
                                } else {
                                    "resuming on-disk lineage"
                                },
                            );
                        }
                        Err(e) => {
                            tracing::error!("raft: failed to host owned partition {p}: {e}");
                        }
                    }
                }
            }

            // Form each group this node leads from its replica set. `initialize`
            // is idempotent and does not require peers to be up (they catch up via
            // replication), but we retry to ride out a transient failure.
            //
            // ADR 0003 replication tier: in `quorum` mode every replica is a voter,
            // so a write commits on a majority. In `leader-durable` mode the leader
            // forms the group as the SOLE voter and adds the other replicas as
            // learners (below), so a write acks on the leader alone and ships to the
            // learners asynchronously — `acks=1` for the workflow log.
            let leader_durable = server.replication_mode == ReplicationMode::LeaderDurable;
            for p in topology.replica_partitions() {
                if topology.leader_of(p) != topology.node_id {
                    continue;
                }
                let Some(part) = server.raft_registry().get(p) else {
                    continue;
                };
                // Phase E: a reachable peer leads this owned partition — defer to a
                // leadership hand-off (recovery tick) instead of forming a competing
                // group. Leave the member an uninitialized receiver.
                if handoff_incumbents.contains(&p) {
                    tracing::info!(
                        "raft: node {} deferring partition {p} to leadership hand-off (a peer leads it)",
                        topology.node_id,
                    );
                    continue;
                }
                let all_replicas = topology.replicas_of(p);
                // Voter set: every replica in `quorum`, leader-only in
                // `leader-durable`.
                let members: std::collections::BTreeMap<u64, openraft::BasicNode> = all_replicas
                    .iter()
                    .copied()
                    .filter(|&n| !leader_durable || n == topology.node_id)
                    .map(|n| {
                        let addr = topology.peer_addr(n).unwrap_or("").to_string();
                        (n as u64, openraft::BasicNode::new(addr))
                    })
                    .collect();
                loop {
                    match part.initialize(members.clone()).await {
                        Ok(()) => {
                            tracing::info!(
                                "raft: node {} formed the group for partition {p} (mode {:?}, voters {:?})",
                                topology.node_id,
                                server.replication_mode,
                                members.keys().collect::<Vec<_>>(),
                            );
                            break;
                        }
                        Err(e) => {
                            tracing::warn!("raft: initialize partition {p} failed: {e}; retrying");
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                }
                // Leader-durable: register the remaining replicas as non-voting
                // learners so they tail the log without gating the write quorum.
                if leader_durable {
                    for n in all_replicas.iter().copied() {
                        if n == topology.node_id {
                            continue;
                        }
                        let addr = topology.peer_addr(n).unwrap_or("").to_string();
                        match part
                            .add_learner(n as u64, openraft::BasicNode::new(addr))
                            .await
                        {
                            Ok(()) => tracing::info!(
                                "raft: node {} added node {n} as a learner for partition {p}",
                                topology.node_id,
                            ),
                            Err(e) => tracing::warn!(
                                "raft: add_learner node {n} for partition {p} failed: {e}"
                            ),
                        }
                    }
                }
            }

            // All hosted members are registered; start the compaction governor
            // (once per node) to bound the payload-bearing Raft log by bytes and
            // reclaim it at idle — beyond what the entry-count snapshot policy does.
            crate::raft::spawn_compaction_governor(server.raft_registry().clone());
        }
    }

    /// Phase E boot incumbent probe (NANOBPMN_RECLAIM_HANDOFF). Before a (re)booting
    /// node forms its own single-voter groups, it asks each co-replica for the
    /// promotion epochs that peer currently raft-leads ([`solicit_promotions_from`]).
    /// A reply naming a peer as leader of a partition WE own is a live failover
    /// incumbent from our recent outage; adopting it ([`handle_promotion`]) also
    /// rebuilds our member for that partition as a receiver. Returns the set of
    /// owned partitions with a live, reachable incumbent — the caller skips
    /// `initialize` for those so no competing lineage forms (the recovery tick then
    /// requests an openraft leadership hand-off instead). Bounded by
    /// [`HANDOFF_PROBE_WINDOW`]: on a cold start no peer answers (none has promoted),
    /// so the set is empty and every owned group forms normally after the window.
    async fn probe_incumbents_for_owned(
        &self,
        topology: &crate::cluster::Topology,
    ) -> std::collections::HashSet<u64> {
        use std::collections::HashSet;
        let me = topology.node_id as u64;
        let owned: Vec<u64> = topology
            .replica_partitions()
            .into_iter()
            .filter(|&p| topology.leader_of(p) == topology.node_id)
            .collect();
        if owned.is_empty() {
            return HashSet::new();
        }
        // Distinct co-replica peers to solicit for our owned partitions.
        let mut targets: Vec<u32> = owned
            .iter()
            .flat_map(|&p| topology.replicas_of(p))
            .filter(|&n| n != topology.node_id)
            .collect();
        targets.sort_unstable();
        targets.dedup();
        if targets.is_empty() {
            return HashSet::new();
        }
        let deadline = std::time::Instant::now() + HANDOFF_PROBE_WINDOW;
        loop {
            for &t in &targets {
                if self.peer_reachable(t).await {
                    self.solicit_promotions_from(t).await;
                }
            }
            tokio::time::sleep(HANDOFF_PROBE_POLL).await;
            // Owned partitions whose adopted epoch names a peer (not us).
            let candidates: Vec<(u64, u32)> = {
                let map = self.promotion_epoch.lock().unwrap();
                owned
                    .iter()
                    .filter_map(|&p| match map.get(&p) {
                        Some(&(_, l)) if l != me => Some((p, l as u32)),
                        _ => None,
                    })
                    .collect()
            };
            let mut live = HashSet::new();
            for (p, l) in candidates {
                if self.peer_reachable(l).await {
                    live.insert(p);
                }
            }
            if live.len() == owned.len() || std::time::Instant::now() >= deadline {
                return live;
            }
        }
    }

    /// Settle barrier for Raft group formation (ADR 0003). Blocks until every
    /// peer this node shares a Raft group with is reachable (its Falcon endpoint
    /// answers a `link`), then waits a short grace so those peers can finish
    /// hosting their own per-partition members before this node `initialize`s and
    /// `add_learner`s. This prevents the staggered-startup term-split-brain that
    /// wedges openraft (see the call site). Bounded by a deadline so a genuinely
    /// absent peer never hangs boot — past the deadline we proceed best-effort
    /// (openraft's own retry/replication then rides out the laggard).
    ///
    /// Tunables: `NANOBPMN_RAFT_SETTLE_MS` (max wait, default 30000; `0` disables
    /// the barrier) and `NANOBPMN_RAFT_SETTLE_GRACE_MS` (post-reachability grace,
    /// default 1500).
    async fn settle_before_forming(&self, topology: &crate::cluster::Topology) {
        let deadline_ms: u64 = std::env::var("NANOBPMN_RAFT_SETTLE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30_000);
        if deadline_ms == 0 || !self.peers.has_peers() {
            return;
        }
        // The distinct set of peer nodes this node forms Raft groups with.
        let mut peer_nodes: Vec<u32> = topology
            .replica_partitions()
            .into_iter()
            .flat_map(|p| topology.replicas_of(p))
            .filter(|&n| n != topology.node_id)
            .collect();
        peer_nodes.sort_unstable();
        peer_nodes.dedup();
        if peer_nodes.is_empty() {
            return;
        }

        let grace_ms: u64 = std::env::var("NANOBPMN_RAFT_SETTLE_GRACE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_500);
        let start = std::time::Instant::now();
        let deadline = start + std::time::Duration::from_millis(deadline_ms);
        let mut pending = peer_nodes.clone();
        while !pending.is_empty() {
            let mut still = Vec::new();
            for &n in &pending {
                if self.peers.link(n).await.is_err() {
                    still.push(n);
                }
            }
            pending = still;
            if pending.is_empty() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                tracing::warn!(
                    "raft settle: node {} proceeding after {}ms with peers {:?} still unreachable; \
                     forming best-effort",
                    topology.node_id,
                    deadline_ms,
                    pending,
                );
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        tracing::info!(
            "raft settle: node {} sees all {} raft peer(s) reachable after {}ms; \
             grace {}ms then forming",
            topology.node_id,
            peer_nodes.len(),
            start.elapsed().as_millis(),
            grace_ms,
        );
        if grace_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(grace_ms)).await;
        }
    }

    /// follow-on). A no-op unless Raft is enabled AND the replication tier is
    /// [`ReplicationMode::LeaderDurable`] with real peers: in every other
    /// configuration failover is either irrelevant (single node / RF=1) or already
    /// handled natively by openraft's voter-majority election (`quorum` mode).
    ///
    /// In leader-durable mode each group has a SINGLE voter (the leader), so when
    /// that leader is lost openraft cannot elect a successor — the learners have no
    /// vote and no node can change membership without a leader. This supervisor
    /// fills that gap: it watches each partition this node replicates and, when the
    /// partition is leaderless and this node is the deterministic surviving
    /// successor, app-promotes it (see [`Self::promote_partition`]).
    fn spawn_leader_durable_recovery(&self) {
        if !raft_enabled()
            || self.replication_mode != ReplicationMode::LeaderDurable
            || !self.peers.has_peers()
        {
            return;
        }
        let server = self.clone();
        tokio::spawn(async move {
            // A few consecutive leaderless observations before acting, so a brief
            // election/heartbeat flutter never triggers a needless promotion.
            let grace_ticks = leader_durable_recovery_grace_ticks();
            let mut state = RecoveryState::default();
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                server
                    .leader_durable_recovery_tick(grace_ticks, &mut state)
                    .await;
                // Engage/clear the Raft-log fsync-relief window from the live
                // leadership picture: while this node carries a down peer's
                // partitions (or is catching its own back up) it runs ~double its
                // steady Raft load on one disk, so coalescing its `sync`-mode
                // fsyncs for the window keeps the shared disk from saturating and
                // turning commits (and the completion-paced admission servo)
                // bursty. Reverts to strict per-round fsync the moment recovery
                // clears. A no-op under `async` durability or when the feature is
                // disabled.
                crate::raft_logstore::set_recovery_fsync_relief(
                    server.recovery_fsync_load_active(),
                );
            }
        });
    }

    /// True when this node carries recovery Raft load worth relieving `fsync` for:
    /// it leads a partition it does not statically own (a failover incumbent
    /// covering a down peer), or it owns a partition currently led by a peer (a
    /// returning owner catching back up). In both cases the node runs roughly
    /// double its steady Raft load on one shared disk. Cheap: borrows each hosted
    /// group's metrics watch. Mirrors the console recovery indicator's signal.
    fn recovery_fsync_load_active(&self) -> bool {
        if !raft_enabled() {
            return false;
        }
        let topology = self.engine.topology();
        let me = topology.node_id;
        if topology.num_nodes() <= 1 {
            return false;
        }
        for p in 0..topology.num_partitions {
            let owner = topology.owner_of(p);
            let leader = self
                .raft_registry()
                .get(p)
                .and_then(|part| part.raft.metrics().borrow().current_leader);
            match leader {
                // We lead a partition we don't own: a failover incumbent.
                Some(l) if l as u32 == me && owner != me => return true,
                // We own it but a peer leads it: a returning owner catching up.
                // (Leaderless — cold-start formation — does not count.)
                Some(l) if l as u32 != me && owner == me => return true,
                _ => {}
            }
        }
        false
    }

    /// Per-`(partition, peer)` catch-up feed observations for every partition
    /// this node currently **leads**: the replication `lag` (entries the peer is
    /// behind the log head) and a monotone `progress` scalar (matched index +
    /// cumulative snapshot bytes) the caller diffs across ticks to distinguish an
    /// *advancing* catch-up from a *stalled/dead* peer. Empty when Raft is
    /// disabled or this is a single node.
    ///
    /// This is the signal that closes the post-hand-off oscillation gap:
    /// [`recovery_fsync_load_active`](Self::recovery_fsync_load_active) keys only
    /// on leadership *displacement*, so it clears the instant a rejoined peer
    /// reclaims its partitions — but that peer is then a badly-lagging learner,
    /// and streaming its retained-log backlog keeps THIS leader's Raft disk
    /// saturated well past the displacement window. The recovery admission
    /// throttle folds these observations in so it stays engaged through the
    /// catch-up (peer lag above a window-derived threshold while still
    /// advancing), then releases once the peer is caught up.
    fn catchup_feed_observations(&self) -> Vec<((u64, u64), u64, u128)> {
        let mut out = Vec::new();
        if !raft_enabled() {
            return out;
        }
        let topology = self.engine.topology();
        if topology.num_nodes() <= 1 {
            return out;
        }
        for p in 0..topology.num_partitions {
            let Some(part) = self.raft_registry().get(p) else {
                continue;
            };
            // Snapshot the leader's per-target matched indices, dropping the
            // metrics borrow before touching the snapshot-progress map.
            let (last, targets): (u64, Vec<(u64, u64)>) = {
                let metrics = part.raft.metrics();
                let m = metrics.borrow();
                if m.state != openraft::ServerState::Leader {
                    continue;
                }
                let last = m.last_log_index.unwrap_or(0);
                let targets = match m.replication.as_ref() {
                    Some(map) => map
                        .iter()
                        .map(|(n, l)| (*n, l.as_ref().map(|id| id.index).unwrap_or(0)))
                        .collect(),
                    None => Vec::new(),
                };
                (last, targets)
            };
            for (node, matched) in targets {
                let lag = last.saturating_sub(matched);
                let bytes = part.snapshot_bytes_sent(node).unwrap_or(0);
                let progress = matched as u128 + bytes as u128;
                out.push(((p, node), lag, progress));
            }
        }
        out
    }

    /// One pass of the leader-durable recovery supervisor. For every partition this
    /// node replicates: if the group is leaderless (no current leader, and the
    /// original leader's peer link is down) for `grace_ticks` consecutive passes,
    /// the partition has already been ESTABLISHED (seen a real leader at least
    /// once), and this node is the deterministic surviving successor, promote it.
    /// `state` carries, across passes: the per-partition consecutive-leaderless
    /// counter, the set of partitions ever seen with a leader, and the
    /// post-promote hold-down countdowns (see [`RecoveryState`]).
    ///
    /// The establishment gate is what keeps recovery from firing during a
    /// staggered cold start: before a partition's owner has formed the group,
    /// every replica sees it as leaderless, and each would otherwise self-promote
    /// the partitions it is the first-reachable replica of — racing formation into
    /// a term split-brain that trips openraft's `has_log_id` invariant.
    ///
    /// Factored out (and not gated on the env) so a test can drive recovery
    /// deterministically without spawning the loop.
    async fn leader_durable_recovery_tick(&self, grace_ticks: u32, state: &mut RecoveryState) {
        let topology = self.engine.topology().clone();
        let me = topology.node_id as u64;
        for p in topology.replica_partitions() {
            let leader = self
                .raft
                .get(p)
                .and_then(|part| part.raft.metrics().borrow().current_leader);
            // Any named leader (live or since-dead) proves the group was formed
            // once -> this partition is established and thus a failover candidate.
            if leader.is_some() {
                state.established.insert(p);
            }
            // Post-promote hold-down (Option C): after we self-promote `p`, damp the
            // reclaim epoch-climb. If leadership now reads as ours the promote took —
            // clear everything and move on. Otherwise it is still settling (or the
            // failover leader is contesting): wait out the window before acting again
            // instead of immediately re-promoting at the next epoch (the climb that
            // spun the term up under load). A dropped/contested promote simply
            // re-solicits and retries after the window.
            if let Some(hd) = state.holddown.get_mut(&p) {
                if leader == Some(me) {
                    state.holddown.remove(&p);
                    state.misses.remove(&p);
                    continue;
                }
                *hd = hd.saturating_sub(1);
                if *hd > 0 {
                    continue;
                }
                state.holddown.remove(&p);
            }
            // A live leader resets the counter. "Live" means present AND, if it is
            // a peer, reachable — a metric still naming a dead leader does not count.
            //
            // EXCEPTION — reclaim of a statically-owned partition: routing is
            // static (`leader_of == owner_of`), so every create/activation for a
            // partition this node owns is sent HERE regardless of who actually
            // leads the Raft group. If a *peer* leads a partition we own, it is a
            // stale failover leader from our recent outage: the owner is back but
            // traffic routed to it hits a mere learner and is `leader_reject`ed —
            // the partition takes zero creates and cannot drain (observed: a
            // rejoined owner stranded a subset of its partitions because a race let
            // the failover leader's replication reach it before it self-promoted).
            // Treat "a peer leads a partition I own" as NOT live so the reclaim
            // path below fires: the owner self-promotes at `incumbent_epoch + 1`
            // (it adopted the failover leader's epoch as a learner, so its next
            // epoch strictly wins the fence) and the old leader steps down. During
            // normal operation the owner leads its own partitions (`l == me`), so
            // this never triggers; it is purely a post-failover reclaim.
            let leader_live = match leader {
                Some(l) if l == me => true,
                Some(_) if topology.is_local(p) => false,
                Some(l) => self.peer_reachable(l as u32).await,
                None => false,
            };
            if leader_live {
                state.misses.remove(&p);
                continue;
            }
            // Leadership hand-off reclaim (opt-in, NANOBPMN_RECLAIM_HANDOFF): a
            // REACHABLE peer is the failover incumbent for a partition we own.
            // Rather than form a competing fresh single-voter group (two lineages
            // fighting elections under load = the term storm), ask the incumbent to
            // hand leadership back via an openraft membership change (it catches us
            // up as a learner, then change_membership's the vote to us and steps
            // down). While that is in flight we suppress the legacy self-promote
            // below; if the incumbent declines or the hand-off times out (without a
            // joint-config suspicion) we fall through to the legacy path.
            //
            // The incumbent is the local raft leader if it is a reachable peer,
            // ELSE the app-epoch map's named leader if reachable. The map covers the
            // Phase E boot-as-receiver case: our member is an uninitialized receiver
            // (no competing group formed at boot), so `current_leader` is not yet the
            // peer — but the boot probe / solicit adopted the incumbent epoch, so the
            // map names it. Without the map fallback the hand-off would never fire for
            // a boot-deferred partition and it would self-promote a competing group.
            if self.reclaim_via_handoff && topology.is_local(p) {
                let candidate: Option<u32> = match leader {
                    Some(l) if l != me => Some(l as u32),
                    _ => self
                        .promotion_epoch
                        .lock()
                        .unwrap()
                        .get(&p)
                        .map(|&(_, l)| l)
                        .filter(|&l| l != me)
                        .map(|l| l as u32),
                };
                let incumbent = match candidate {
                    Some(l) if self.peer_reachable(l).await => Some(l),
                    _ => None,
                };
                if let Some(inc) = incumbent {
                    // Best-effort reclaim (ADR 0019, Zeebe-aligned): while a
                    // REACHABLE incumbent still leads this owned partition, keep
                    // requesting the leadership hand-off and NEVER fall back to a
                    // competing self-promote — that fallback is the two-lineage
                    // election storm. If a bounded attempt lapses,
                    // `request_handoff_or_wait` re-arms and resends. We only reach
                    // the self-promote path when NO reachable incumbent leads `p`
                    // (a genuine failover / cold owner).
                    self.solicit_promotions_from(inc).await;
                    self.request_handoff_or_wait(p, inc).await;
                    continue;
                } else if leader.is_none() {
                    // Leaderless with no incumbent yet known: solicit ALL reachable
                    // co-replicas so a live incumbent is discovered (and handed off
                    // to) before we self-promote. Only if none answers across the
                    // grace window do we fall through to a fresh self-promote.
                    for n in topology.replicas_of(p) {
                        if n != topology.node_id && self.peer_reachable(n).await {
                            self.solicit_promotions_from(n).await;
                        }
                    }
                }
            }
            // Reclaim solicitation (Option A): a peer leads a partition we own.
            // Solicit its promotion epoch NOW, during the grace window, so we adopt
            // it (via `handle_promotion`) before we promote — then `next_promotion_epoch`
            // yields `incumbent + 1`, winning the fence in a single round. Without
            // this the epoch (in-memory, reset on restart) starts at 1, loses to the
            // higher-epoch failover leader, and we climb one epoch per tick — the
            // load-sensitive `leader_reject` storm. Re-solicited each grace pass (at
            // most `grace_ticks` fire-and-forget frames) so a dropped or pre-link
            // solicit still lands before promotion.
            if let Some(l) = leader
                && l != me
                && topology.is_local(p)
            {
                self.solicit_promotions_from(l as u32).await;
            }
            let n = state.misses.entry(p).or_insert(0);
            *n += 1;
            if *n < grace_ticks {
                continue;
            }
            // Never fail over a partition still in initial formation: only an
            // established group (its owner formed it, then its leader was lost) is
            // a genuine failover. This is the cold-start split-brain guard.
            if !state.established.contains(&p) {
                continue;
            }
            // Leaderless past the grace window. Promote iff this node is the
            // deterministic surviving successor for `p`.
            if self.designated_successor(p).await == Some(me as u32) {
                let next_epoch = self.next_promotion_epoch(p);
                tracing::warn!(
                    "leader-durable: partition {p} leaderless; node {me} self-promoting (epoch {next_epoch})"
                );
                self.promote_partition(p, next_epoch).await;
                state.misses.remove(&p);
                // No reachable incumbent remained, so any stale hand-off request
                // for `p` is moot — clear it so a later rejoin starts clean.
                self.handoff_pending.lock().unwrap().remove(&p);
                // Hold the partition down for a settle window so a lagging metrics
                // view cannot trigger an immediate re-promote at the next epoch.
                state
                    .holddown
                    .insert(p, LEADER_DURABLE_PROMOTE_HOLDDOWN_TICKS);
            }
        }
    }

    /// Whether peer `node` is currently reachable (a live falcon uplink can
    /// be established). Used as the failure detector for leader-durable recovery: a
    /// node whose link cannot be dialed is treated as down. `true` for this node
    /// itself.
    async fn peer_reachable(&self, node: u32) -> bool {
        if node == self.engine.topology().node_id {
            return true;
        }
        matches!(self.peers.link(node).await, Ok(link) if link.is_connected())
    }

    /// The deterministic surviving successor for partition `p`: the first node in
    /// `replicas_of(p)` order (leader first) that is currently reachable. Because
    /// the replica order is identical on every node, all survivors independently
    /// agree on the same successor with no coordination — so at most one node
    /// promotes. Returns `None` if no replica is reachable (this node included,
    /// which cannot happen since `self` is always reachable to itself).
    async fn designated_successor(&self, p: u64) -> Option<u32> {
        for n in self.engine.topology().replicas_of(p) {
            if self.peer_reachable(n).await {
                return Some(n);
            }
        }
        None
    }

    /// Reserves the next promotion epoch for partition `p` (current max + 1) and
    /// records this node as the leader at that epoch. Monotonic per partition.
    fn next_promotion_epoch(&self, p: u64) -> u64 {
        let me = self.engine.topology().node_id as u64;
        let mut map = self.promotion_epoch.lock().unwrap();
        let next = map.get(&p).map(|(e, _)| *e).unwrap_or(0) + 1;
        map.insert(p, (next, me));
        next
    }

    /// App-promotes leaderless partition `p` on this node (leader-durable
    /// auto-recovery, ADR 0003). Rebuilds the partition's Raft group as a fresh
    /// single-voter group led by this node, seeded from the engine actor that
    /// already holds the replicated state (its replica engine, or the owned actor),
    /// so all committed-and-shipped progress carries over and the partition resumes
    /// serving writes immediately. Then announces the promotion to peers so the
    /// other survivors rejoin as learners (durability for new writes) and any stale
    /// leader at a lower epoch steps down (fencing).
    ///
    /// LOSS WINDOW (the leader-durable trade): any tail the dead leader acked but
    /// had not yet shipped to this node's replica engine is gone — bounded,
    /// at-least-once (a lost completion redelivers; a lost create was never durably
    /// admitted, so the producer retries). SPLIT-BRAIN under a pure network
    /// partition is the inherent acks=1 limit: a partitioned-but-alive old leader
    /// may keep acking writes that are later discarded when it sees the higher
    /// epoch — bounded loss, never permanent divergence (higher epoch always wins).
    async fn promote_partition(&self, p: u64, epoch: u64) {
        use crate::raft::RaftPartition;
        let topology = self.engine.topology().clone();
        let me = topology.node_id as u64;

        // The engine actor holding the replicated state for `p` on this node.
        let engine = match self.engine_handle_for(p) {
            Some(h) => h,
            None => self.replica_engine_for(p).await,
        };

        // Tear down the stale group (a learner of the dead leader) before replacing
        // it, so its openraft task and sockets are released.
        if let Some(old) = self.raft.get(p) {
            old.raft.shutdown().await.ok();
        }

        // Form a fresh single-voter group on a clean in-memory log (the engine's
        // own journal remains the local durability source). Initialize with this
        // node as the sole voter so it elects itself immediately, then add the
        // reachable survivors as learners so new writes ship to them.
        let transport = self.raft_transport();
        // No local exporter unless this node statically owns `p`; a promoted
        // replica must evict terminal shells itself (see bootstrap_member).
        let evict_terminal = self.engine.local_for_partition(p).is_none();
        let part =
            match RaftPartition::bootstrap_member(me, p, engine, transport, None, evict_terminal)
                .await
            {
                Ok(part) => Arc::new(part),
                Err(e) => {
                    tracing::error!(
                        "leader-durable: promote partition {p} failed to build group: {e}"
                    );
                    return;
                }
            };
        let mut members = std::collections::BTreeMap::new();
        members.insert(
            me,
            openraft::BasicNode::new(topology.peer_addr(me as u32).unwrap_or("").to_string()),
        );
        if let Err(e) = part.initialize(members).await {
            tracing::error!("leader-durable: promote partition {p} failed to initialize: {e}");
            return;
        }
        self.raft.insert(part.clone());
        metrics::record_promote(p);
        tracing::info!(
            "leader-durable: node {me} promoted itself leader of partition {p} (epoch {epoch})"
        );

        // Announce so peers rejoin as learners and any stale leader steps down,
        // then (best-effort) add the reachable survivors as learners.
        self.broadcast_promotion(p, epoch).await;
        for n in topology.replicas_of(p) {
            if n as u64 == me {
                continue;
            }
            if self.peer_reachable(n).await {
                let addr = topology.peer_addr(n).unwrap_or("").to_string();
                part.add_learner(n as u64, openraft::BasicNode::new(addr))
                    .await
                    .ok();
            }
        }
    }

    /// Fire-and-forget a [`ClientFrame::Promote`] to every reachable peer, telling
    /// them this node is now the leader of partition `p` at `epoch`.
    async fn broadcast_promotion(&self, p: u64, epoch: u64) {
        let topology = self.engine.topology().clone();
        let me = topology.node_id;
        let addr = topology.peer_addr(me).unwrap_or("").to_string();
        for n in 0..topology.num_nodes() {
            if n == me {
                continue;
            }
            if let Ok(link) = self.peers.link(n).await {
                link.send_promote(p, epoch, me as u64, addr.clone())
                    .await
                    .ok();
            }
        }
    }

    /// Ask peer `target` to re-announce the promotion epochs it currently leads
    /// (leader-durable reclaim, [`ClientFrame::SolicitPromotions`]). Fire-and-forget:
    /// the peer replies with its standing [`ClientFrame::Promote`] frames, which we
    /// adopt in [`handle_promotion`](Self::handle_promotion) — seeding the incumbent
    /// epoch so the next reclaim promote lands at `incumbent + 1` and wins the fence
    /// in one round instead of climbing epochs under load.
    async fn solicit_promotions_from(&self, target: u32) {
        let me = self.engine.topology().node_id;
        if target == me {
            return;
        }
        if let Ok(link) = self.peers.link(target).await {
            link.send_solicit_promotions(me as u64).await.ok();
        }
    }

    /// Whether this node is the **current raft leader** of partition `p` (its
    /// openraft core reports `state == Leader` and names itself the leader). This
    /// is the authoritative "am I actually serving writes for `p`" signal — as
    /// opposed to the app-level `promotion_epoch` map, which can name a node that
    /// has since been demoted (e.g. after a leadership hand-off). Callers that
    /// advertise leadership to peers (solicit replies, promotion re-announcements)
    /// MUST gate on this so a demoted learner never claims to lead — a stale claim
    /// can make the real leader step back down and undo a completed hand-off.
    fn i_lead_raft(&self, p: u64) -> bool {
        let me = self.engine.topology().node_id as u64;
        self.raft
            .get(p)
            .map(|part| {
                let m = part.raft.metrics().borrow().clone();
                m.state == openraft::ServerState::Leader && m.current_leader == Some(me)
            })
            .unwrap_or(false)
    }

    /// Answer a peer's [`ClientFrame::SolicitPromotions`]: re-announce to `from_node`
    /// every partition this node currently app-leads (its `promotion_epoch` names
    /// us) **and still actually raft-leads**, so a rejoining owner learns the
    /// incumbent epoch and reclaims at `incumbent + 1`. A no-op if we lead nothing
    /// or the solicit is our own. The raft-leadership gate ([`i_lead_raft`]) is
    /// critical: after a leadership hand-off our `promotion_epoch` map may still
    /// name us for `p` while openraft has moved leadership elsewhere — replying
    /// then would make the returning owner adopt a stale epoch and re-demote the
    /// new leader, undoing the hand-off.
    pub(crate) async fn answer_promotion_solicit(&self, from_node: u64) {
        let topology = self.engine.topology().clone();
        let me = topology.node_id as u64;
        if from_node == me {
            return;
        }
        let led: Vec<(u64, u64)> = {
            let map = self.promotion_epoch.lock().unwrap();
            map.iter()
                .filter(|(_, (_, leader))| *leader == me)
                .map(|(p, (epoch, _))| (*p, *epoch))
                .collect()
        };
        // Only re-announce partitions we STILL raft-lead (a demoted learner must
        // not advertise itself as leader).
        let led: Vec<(u64, u64)> = led
            .into_iter()
            .filter(|(p, _)| self.i_lead_raft(*p))
            .collect();
        if led.is_empty() {
            return;
        }
        let addr = topology.peer_addr(me as u32).unwrap_or("").to_string();
        if let Ok(link) = self.peers.link(from_node as u32).await {
            for (p, epoch) in led {
                link.send_promote(p, epoch, me, addr.clone()).await.ok();
            }
        }
    }

    /// Incumbent side of the leadership hand-off. A rejoining owner asked us to
    /// hand `partition` back via an openraft membership change instead of forming
    /// a competing group. We must ACTUALLY raft-lead `partition` to hand it off;
    /// we reserve the per-partition hand-off lease (also the create write-gate),
    /// ack the requester, run [`perform_handoff`](Self::perform_handoff), then
    /// report the terminal outcome. Concurrency-safe: a second request while one
    /// is in flight is declined.
    pub(crate) async fn handle_handoff_request(
        &self,
        partition: u64,
        requester_node: u64,
        requester_addr: String,
    ) {
        // Must genuinely lead the group to hand it off.
        if !self.i_lead_raft(partition) {
            self.reply_handoff_ack(requester_node, partition, 0, false)
                .await;
            return;
        }
        // Reserve the per-partition hand-off lease (and engage the create
        // write-gate). Declined if a hand-off for this partition is already in
        // flight — never run two concurrent membership changes on one group.
        if !self.acquire_handoff_lease(partition) {
            let epoch = self.current_app_epoch(partition);
            self.reply_handoff_ack(requester_node, partition, epoch, false)
                .await;
            return;
        }
        let incumbent_epoch = self.current_app_epoch(partition);
        self.reply_handoff_ack(requester_node, partition, incumbent_epoch, true)
            .await;

        let result = self
            .perform_handoff(partition, requester_node, requester_addr, incumbent_epoch)
            .await;

        // Release the lease / lift the write-gate regardless of outcome.
        self.release_handoff_lease(partition);

        match result {
            Ok(new_epoch) => {
                tracing::info!(
                    partition,
                    requester_node,
                    new_epoch,
                    "leadership hand-off complete: transferred to returning owner"
                );
                self.reply_handoff_complete(requester_node, partition, new_epoch)
                    .await;
            }
            Err((joint_suspected, reason)) => {
                tracing::warn!(
                    partition,
                    requester_node,
                    joint_suspected,
                    %reason,
                    "leadership hand-off aborted"
                );
                self.reply_handoff_failed(requester_node, partition, joint_suspected, reason)
                    .await;
            }
        }
    }

    /// Execute the leadership hand-off for `partition` to `requester_node`: add it
    /// as a learner, poll it to within [`HANDOFF_LAG_THRESHOLD`] of our log (the
    /// create write-gate is engaged so the log quiesces), then
    /// `change_membership` the sole voter to it (which demotes us to a learner and
    /// steps us down), and finally advance our epoch fence to `(incumbent + 1,
    /// requester)` so a stale promote can't undo the transfer. Returns the new
    /// epoch on success, or `(joint_suspected, reason)` on abort — where
    /// `joint_suspected` means the membership change may be half-applied and the
    /// requester must NOT fall back to forming a fresh group.
    async fn perform_handoff(
        &self,
        partition: u64,
        requester_node: u64,
        requester_addr: String,
        incumbent_epoch: u64,
    ) -> Result<u64, (bool, String)> {
        let Some(part) = self.raft.get(partition) else {
            return Err((false, format!("no raft group for partition {partition}")));
        };
        let node = openraft::BasicNode::new(requester_addr);
        // Set up replication to the returning owner (non-blocking).
        if let Err(e) = part.add_learner(requester_node, node).await {
            return Err((false, format!("add_learner: {e}")));
        }
        // Poll the learner toward zero lag with an ADAPTIVE, snapshot-transfer-aware
        // deadline: succeed the instant it reaches HANDOFF_LAG_THRESHOLD; keep going
        // while it is still installing/streaming (matched OR snapshot bytes making
        // progress); abort on a genuine stall or the absolute hard cap (see
        // [`evaluate_catchup`]). The soft ceiling is the normal budget; while a
        // snapshot install is actively transferring at the soft ceiling we EXTEND
        // (up to the hard cap) rather than guillotine a large-but-progressing
        // install — and extend the completion write-pause in lockstep so the log
        // head stays frozen for the whole extended install (else the snapshot point
        // moves and the learner re-snapshots forever). The write-gate keeps new
        // creates off this partition throughout.
        let soft_deadline = std::time::Instant::now() + self.handoff_catchup_ceiling();
        let hard_deadline = std::time::Instant::now()
            + handoff_catchup_max_from_env(self.handoff_catchup_ceiling());
        let stall_grace = handoff_catchup_stall_from_env();
        let mut best_matched: Option<u64> = None;
        let mut best_bytes: u64 = 0;
        let mut last_advance = std::time::Instant::now();
        let mut pause_extended = false;
        loop {
            if !self.i_lead_raft(partition) {
                return Err((false, "lost leadership during catch-up".to_string()));
            }
            let lag = part.replication_lag(requester_node);
            let matched = part.learner_matched(requester_node);
            let snapshot_bytes = part.snapshot_bytes_sent(requester_node);
            let now = std::time::Instant::now();
            // Once we cross the soft ceiling while an install is still streaming,
            // the catch-up may run up to the hard cap — so extend the completion
            // write-pause to cover it (idempotent), keeping the log head frozen for
            // the whole extended install so the snapshot point can't move.
            if !pause_extended && now >= soft_deadline && snapshot_bytes.is_some() {
                self.extend_handoff_pause(partition, hard_deadline);
                pause_extended = true;
            }
            match evaluate_catchup(
                lag,
                matched,
                snapshot_bytes,
                &mut best_matched,
                &mut best_bytes,
                &mut last_advance,
                now,
                soft_deadline,
                hard_deadline,
                HANDOFF_LAG_THRESHOLD,
                stall_grace,
            ) {
                CatchupStep::Done => break,
                CatchupStep::Abort(reason) => return Err((false, reason.to_string())),
                CatchupStep::Continue => {}
            }
            tokio::time::sleep(HANDOFF_LAG_POLL).await;
        }
        // Transfer the vote to the requester. retain=true demotes us to a learner
        // and steps us down; the requester becomes the sole voter/leader.
        if let Err(e) = part.change_voters_to(vec![requester_node]).await {
            // A failure may have left the group in the transient joint config
            // (needs a quorum of BOTH voter sets) — flag it so the requester waits
            // rather than diverging with a fresh group.
            let joint = part.in_joint_config();
            return Err((joint, format!("change_membership: {e}")));
        }
        // Advance our fence to (incumbent + 1, requester) so a later stale
        // Promote/SolicitPromotions naming us can't re-adopt and undo the transfer.
        let new_epoch = incumbent_epoch + 1;
        {
            let mut map = self.promotion_epoch.lock().unwrap();
            let cur = map.get(&partition).map(|(e, _)| *e).unwrap_or(0);
            if new_epoch >= cur {
                map.insert(partition, (new_epoch, requester_node));
            }
        }
        Ok(new_epoch)
    }

    /// Requester side: the incumbent acknowledged (or declined) our hand-off
    /// request. On accept we make sure our member for `partition` is a plain
    /// receiver so the incumbent can replicate to us as a learner (our own
    /// competing group would otherwise reject its log). On a decline
    /// (`accepted = false`) we clear the pending marker so the recovery tick's
    /// legacy self-promote can proceed.
    pub(crate) async fn handle_handoff_ack(
        &self,
        partition: u64,
        incumbent_epoch: u64,
        accepted: bool,
    ) {
        tracing::debug!(partition, incumbent_epoch, accepted, "hand-off ack");
        if !accepted {
            self.handoff_pending.lock().unwrap().remove(&partition);
            return;
        }
        // Rebuild as a receiver ONLY when we don't already have a receiver in
        // place: either we hold no member for the partition, or we still lead a
        // competing group (which would reject the incumbent's log). If we are
        // already hosting the partition as a non-leader (a learner/follower —
        // e.g. from the fresh-receiver rejoin path, ADR 0019 part 1, or a prior
        // ack), we MUST NOT rebuild: `rebuild_as_receiver` shuts the member down
        // and re-bootstraps it with an empty log store, discarding everything the
        // incumbent has already replicated. Under sustained load the incumbent's
        // log outgrows what a from-scratch learner can drain inside a single
        // catch-up window, so wiping on every retry makes the catch-up livelock
        // forever. Keeping the receiver lets replication accumulate across
        // attempts until the learner converges and the vote transfers.
        let need_rebuild = match self.raft.get(partition) {
            None => true,
            Some(_) => self.i_lead_raft(partition),
        };
        if need_rebuild {
            let incumbent = self
                .promotion_epoch
                .lock()
                .unwrap()
                .get(&partition)
                .map(|(_, leader)| *leader)
                .unwrap_or(u64::MAX);
            self.rebuild_as_receiver(partition, incumbent).await;
        }
    }

    /// Requester side: the incumbent completed the hand-off — we are now the sole
    /// voter/leader of `partition`. Adopt the new epoch fence (so a stale promote
    /// can't undo it) and clear the pending marker.
    pub(crate) async fn handle_handoff_complete(
        &self,
        partition: u64,
        epoch: u64,
        new_leader: u64,
    ) {
        {
            let mut map = self.promotion_epoch.lock().unwrap();
            let cur = map.get(&partition).map(|(e, _)| *e).unwrap_or(0);
            if epoch >= cur {
                map.insert(partition, (epoch, new_leader));
            }
        }
        self.handoff_pending.lock().unwrap().remove(&partition);
        tracing::info!(
            partition,
            epoch,
            "leadership hand-off received: this node now leads the partition"
        );
    }

    /// Requester side: the incumbent aborted the hand-off. If it may have left the
    /// group in a joint config (`joint_suspected`), we keep the pending marker
    /// (and flag it) so we do NOT fall back to a fresh self-promote over a
    /// partially-migrated lineage; otherwise we clear it so the recovery tick's
    /// legacy self-promote can proceed.
    pub(crate) async fn handle_handoff_failed(
        &self,
        partition: u64,
        joint_suspected: bool,
        reason: String,
    ) {
        tracing::warn!(partition, joint_suspected, %reason, "hand-off failed");
        let mut pending = self.handoff_pending.lock().unwrap();
        if joint_suspected {
            if let Some(hp) = pending.get_mut(&partition) {
                hp.joint_suspected = true;
            }
        } else {
            pending.remove(&partition);
        }
    }

    /// The app-promotion epoch this node currently holds for `partition` if it
    /// names us as leader, else 0. Used by the incumbent to tell the requester
    /// which epoch to fence above.
    fn current_app_epoch(&self, partition: u64) -> u64 {
        let me = self.engine.topology().node_id as u64;
        self.promotion_epoch
            .lock()
            .unwrap()
            .get(&partition)
            .filter(|(_, leader)| *leader == me)
            .map(|(e, _)| *e)
            .unwrap_or(0)
    }

    /// Reserve the per-partition incumbent hand-off lease (and engage the create
    /// write-gate + the bounded completion write-pause). Returns `false` if a
    /// hand-off for `partition` is already in flight.
    fn acquire_handoff_lease(&self, partition: u64) -> bool {
        let configured = std::time::Duration::from_millis(
            self.handoff_write_pause_ms
                .load(std::sync::atomic::Ordering::Relaxed),
        );
        // Clamp the pause up to the catch-up ceiling (unless explicitly disabled
        // with 0): completions must stay paused for the WHOLE catch-up, or the log
        // head unfreezes mid-install and the learner re-snapshots forever. This
        // keeps the two windows from drifting out of order even if the pause env
        // is set below the ceiling.
        let pause = if configured.is_zero() {
            configured
        } else {
            configured.max(self.handoff_catchup_ceiling())
        };
        let deadline = std::time::Instant::now() + pause;
        let mut gated = self.handoff_gated.lock().unwrap();
        if gated.contains_key(&partition) {
            return false;
        }
        gated.insert(partition, deadline);
        true
    }

    /// Extend the completion write-pause for `partition` to at least `until`, so
    /// the log head stays frozen while a snapshot-transfer-aware catch-up runs past
    /// the soft ceiling (up to the hard cap). Idempotent and monotonic — only ever
    /// pushes the pause deadline later, never earlier, and is a no-op if the lease
    /// was released or the pause is disabled (no entry present). Without this a
    /// hand-off that extends its catch-up would outlive its write-pause, the head
    /// would move mid-install, and the learner would re-snapshot forever.
    fn extend_handoff_pause(&self, partition: u64, until: std::time::Instant) {
        let mut gated = self.handoff_gated.lock().unwrap();
        if let Some(deadline) = gated.get_mut(&partition)
            && until > *deadline
        {
            *deadline = until;
        }
    }

    /// Test hook: set the completion write-pause ceiling to a short, deterministic
    /// window so a hand-off test isn't at the mercy of the 2 s production default.
    #[cfg(test)]
    fn set_handoff_write_pause_for_test(&self, d: std::time::Duration) {
        self.handoff_write_pause_ms
            .store(d.as_millis() as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// The hand-off catch-up absolute ceiling (per-instance, seeded from
    /// `NANOBPMN_HANDOFF_CATCHUP_MS`). Read on the cold hand-off path by both the
    /// catch-up loop and the write-pause clamp so the two windows stay ordered.
    fn handoff_catchup_ceiling(&self) -> std::time::Duration {
        std::time::Duration::from_millis(
            self.handoff_catchup_ceiling_ms
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Test hook: shrink the catch-up ceiling to a short deterministic window so a
    /// hand-off test (and the write-pause clamp) isn't gated on the 30 s default.
    #[cfg(test)]
    fn set_handoff_catchup_ceiling_for_test(&self, d: std::time::Duration) {
        self.handoff_catchup_ceiling_ms
            .store(d.as_millis() as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// Release the incumbent hand-off lease and lift the write-gate/pause for
    /// `partition`.
    fn release_handoff_lease(&self, partition: u64) {
        self.handoff_gated.lock().unwrap().remove(&partition);
    }

    /// Whether `partition` is currently create-write-gated by an in-flight
    /// incumbent hand-off (new creates are steered off it so its log quiesces).
    fn handoff_write_gated(&self, partition: u64) -> bool {
        let gated = self.handoff_gated.lock().unwrap();
        !gated.is_empty() && gated.contains_key(&partition)
    }

    /// Whether job-mutation writes (completions/fails/errors) to `partition` are
    /// currently paused by an in-flight incumbent hand-off — true only while the
    /// bounded pause window (`NANOBPMN_HANDOFF_WRITE_PAUSE_MS`) is open. Rejected
    /// writes are retryable; the pause lets the catch-up learner reach zero lag.
    fn handoff_completion_paused(&self, partition: u64) -> bool {
        let gated = self.handoff_gated.lock().unwrap();
        if gated.is_empty() {
            return false;
        }
        matches!(gated.get(&partition), Some(&deadline) if std::time::Instant::now() < deadline)
    }

    /// Fire-and-forget a hand-off ack to the requesting owner.
    async fn reply_handoff_ack(
        &self,
        to: u64,
        partition: u64,
        incumbent_epoch: u64,
        accepted: bool,
    ) {
        if let Ok(link) = self.peers.link(to as u32).await {
            link.send_handoff_ack(partition, incumbent_epoch, accepted)
                .await
                .ok();
        }
    }

    /// Fire-and-forget a hand-off completion to the requesting owner.
    async fn reply_handoff_complete(&self, to: u64, partition: u64, epoch: u64) {
        if let Ok(link) = self.peers.link(to as u32).await {
            link.send_handoff_complete(partition, epoch, to).await.ok();
        }
    }

    /// Fire-and-forget a hand-off failure to the requesting owner.
    async fn reply_handoff_failed(
        &self,
        to: u64,
        partition: u64,
        joint_suspected: bool,
        reason: String,
    ) {
        if let Ok(link) = self.peers.link(to as u32).await {
            link.send_handoff_failed(partition, joint_suspected, reason)
                .await
                .ok();
        }
    }

    /// Requester side, driven by the recovery tick: a reachable failover incumbent
    /// `incumbent` leads our owned `partition`. Ask it for a leadership hand-off,
    /// or re-send a fresh request if a prior bounded attempt lapsed. Best-effort
    /// (ADR 0019): as long as a reachable incumbent leads the partition the caller
    /// keeps calling this and never self-promotes, so this never "gives up" — it
    /// re-arms the deadline and resends instead.
    async fn request_handoff_or_wait(&self, partition: u64, incumbent: u32) {
        // Fast path: the transfer already landed and we now lead — clear and stop.
        if self.i_lead_raft(partition) {
            self.handoff_pending.lock().unwrap().remove(&partition);
            return;
        }
        let resend = {
            let mut pending = self.handoff_pending.lock().unwrap();
            match pending.get_mut(&partition) {
                None => {
                    pending.insert(
                        partition,
                        HandoffPending {
                            deadline_ticks: HANDOFF_PENDING_TICKS,
                            joint_suspected: false,
                        },
                    );
                    true
                }
                Some(hp) => {
                    if hp.joint_suspected {
                        // A membership change may be half-applied; wait it out
                        // quietly rather than resend or diverge.
                        false
                    } else {
                        hp.deadline_ticks = hp.deadline_ticks.saturating_sub(1);
                        if hp.deadline_ticks == 0 {
                            // The prior attempt lapsed. Re-arm and resend rather
                            // than give up — the incumbent is still reachable and
                            // leading, so self-promote would restart the storm.
                            hp.deadline_ticks = HANDOFF_PENDING_TICKS;
                            true
                        } else {
                            false
                        }
                    }
                }
            }
        };
        if resend {
            let me = self.engine.topology().node_id as u64;
            let addr = self
                .engine
                .topology()
                .peer_addr(me as u32)
                .unwrap_or("")
                .to_string();
            if let Ok(link) = self.peers.link(incumbent).await {
                link.send_request_handoff(partition, me, addr).await.ok();
            }
        }
    }

    /// Switch the runtime SLA mode from an operator action on THIS node (the
    /// console SLA knob), then fan the new mode out to every peer so the whole
    /// cluster runs one uniform admission policy. Applied locally first (so the
    /// originating node reflects it immediately even if peers are unreachable),
    /// then best-effort broadcast — the same fire-and-forget model as
    /// [`Self::broadcast_promotion`]/deployment fan-out.
    #[cfg(feature = "console")]
    pub(crate) async fn switch_sla_mode(&self, mode: SlaMode) {
        self.set_sla_mode(mode);
        self.broadcast_sla_mode(mode).await;
    }

    /// Fire-and-forget the SLA mode to every reachable peer. A peer that is
    /// briefly unreachable keeps its prior mode until the next switch or a restart
    /// (which reseeds from `NANOBPMN_SLA_MODE`) — acceptable for an operational
    /// knob, consistent with how deployments/promotions propagate.
    #[cfg(feature = "console")]
    async fn broadcast_sla_mode(&self, mode: SlaMode) {
        let topology = self.engine.topology().clone();
        let me = topology.node_id;
        for n in 0..topology.num_nodes() {
            if n == me {
                continue;
            }
            if let Ok(link) = self.peers.link(n).await {
                link.send_sla_mode(mode.as_str().to_string()).await.ok();
            }
        }
    }

    /// Fire-and-forget this node's current create-load index to every peer (ADR
    /// 0004 `balanced` pressure gossip). A briefly unreachable peer simply keeps
    /// the last value it received (or none, treated as full headroom) until the
    /// next tick — acceptable for a routing hint that the reactive shed/reroute
    /// layer already backstops.
    async fn broadcast_pressure(&self, load: i64) {
        let topology = self.engine.topology().clone();
        let me = topology.node_id;
        for n in 0..topology.num_nodes() {
            if n == me {
                continue;
            }
            if let Ok(link) = self.peers.link(n).await {
                link.send_pressure(me, load).await.ok();
            }
        }
    }

    /// Spawns the create-load gossip tick (ADR 0014 `balanced`). Every
    /// [`placement_gossip_interval`] it broadcasts this node's composite
    /// create-load index to its peers, so their weighted placement can steer
    /// creates toward nodes with headroom. A no-op unless placement is in
    /// `balanced` mode with real peers — zero overhead in every other
    /// configuration (default `off`, `protect`, single node).
    fn spawn_pressure_gossip(&self) {
        if !self.placement_mode.balances() || !self.peers.has_peers() {
            return;
        }
        let server = self.clone();
        let interval = placement_gossip_interval();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let load = server.create_load_index();
                server.broadcast_pressure(load).await;
            }
        });
    }

    /// Apply an SLA mode switched on a PEER (inbound [`ClientFrame::SetSlaMode`]).
    /// Sets it locally only — we do not re-broadcast (the originator already fanned
    /// out to every peer), so there is no propagation loop. Fail-safe parse: any
    /// unrecognised string resolves to [`SlaMode::Latency`].
    pub(crate) fn apply_remote_sla_mode(&self, mode: &str) {
        self.set_sla_mode(parse_sla_mode(Some(mode)));
    }

    /// Handles an inbound [`ClientFrame::Promote`]: a peer (`leader_node`) has
    /// app-promoted itself leader of partition `p` at `epoch` (leader-durable
    /// recovery). We adopt the announcement when it *wins the fence*: a strictly
    /// higher epoch, or the SAME epoch from a lower node id (the deterministic
    /// tiebreak that collapses a symmetric multi-way split — two survivors that each
    /// promote at the same epoch — back to one leader; lowest node id wins). On
    /// adopting, if we host a now-stale group for `p` that we do not lead at this
    /// epoch, we rebuild it as a fresh receiver member so the new leader's
    /// replication (its `add_learner`) lands, and so a stale leader at a lower (or
    /// tie-losing) epoch steps down (fencing).
    ///
    /// If instead WE are the current leader for `p` at this epoch and the inbound
    /// announcement is a tie-loser (or a stale duplicate from a survivor that
    /// promoted concurrently), we keep leadership but `add_learner` the sender so it
    /// rejoins our group for durability — closing the survivor-rejoin gap for the
    /// collision case. Idempotent otherwise.
    pub(crate) async fn handle_promotion(&self, p: u64, epoch: u64, leader_node: u64) {
        let me = self.engine.topology().node_id as u64;
        // Fence decision under the lock: wins(epoch, leader) iff strictly newer, or
        // same epoch with a lower node id. `cur_leader` defaults to u64::MAX so any
        // real promotion at epoch >= 1 beats the implicit (0, _) incumbent state.
        let (adopt, i_lead_here) = {
            let mut map = self.promotion_epoch.lock().unwrap();
            let (cur_epoch, cur_leader) = map.get(&p).copied().unwrap_or((0, u64::MAX));
            let wins = epoch > cur_epoch || (epoch == cur_epoch && leader_node < cur_leader);
            if wins {
                map.insert(p, (epoch, leader_node));
                (true, false)
            } else {
                (false, cur_epoch == epoch && cur_leader == me)
            }
        };

        if !adopt {
            // We did not adopt. If we are the standing leader for `p` at this epoch
            // and the sender is a tie-loser/concurrent promoter, pull it back in as a
            // learner so it stops diverging and resumes receiving our log.
            if i_lead_here
                && leader_node != me
                && let Some(addr) = self
                    .engine
                    .topology()
                    .peer_addr(leader_node as u32)
                    .map(str::to_string)
                && let Some(part) = self.raft.get(p)
            {
                part.add_learner(leader_node, openraft::BasicNode::new(addr))
                    .await
                    .ok();
            }
            return; // stale / duplicate / tie-loser
        }

        if leader_node == me {
            return;
        }
        // Rebuild our member for `p` as a fresh receiver so the new leader can
        // replicate to us (a learner of the OLD group would reject the new leader's
        // lower-term, fresh log). No initialize: we only receive.
        if self.rebuild_as_receiver(p, leader_node).await {
            tracing::info!(
                "leader-durable: node {me} rejoined partition {p} as a learner of node {leader_node} (epoch {epoch})"
            );
        } else {
            tracing::error!(
                "leader-durable: node {me} failed to rejoin partition {p} after promotion"
            );
        }
    }

    /// Rebuild this node's member for `p` as a fresh receiver (uninitialized —
    /// receive-only) of `leader_node`'s group, replacing any existing member. A
    /// learner of an OLD lineage would reject a new leader's lower-term fresh log,
    /// so on adopting a new leader (a promotion or a leadership hand-off) we tear
    /// our member down and rebuild it clean so replication resumes. Returns whether
    /// the rebuild succeeded.
    async fn rebuild_as_receiver(&self, p: u64, leader_node: u64) -> bool {
        use crate::raft::RaftPartition;
        let me = self.engine.topology().node_id as u64;
        let engine = match self.engine_handle_for(p) {
            Some(h) => h,
            None => self.replica_engine_for(p).await,
        };
        if let Some(old) = self.raft.get(p) {
            old.raft.shutdown().await.ok();
        }
        let transport = self.raft_transport();
        // Rejoining as a follower/receiver of the new leader: evict terminal
        // shells locally unless this node statically owns `p` (has an exporter).
        let evict_terminal = self.engine.local_for_partition(p).is_none();
        match RaftPartition::bootstrap_member(me, p, engine, transport, None, evict_terminal).await
        {
            Ok(part) => {
                self.raft.insert(Arc::new(part));
                let _ = leader_node;
                true
            }
            Err(e) => {
                tracing::error!("node {me} failed to rebuild partition {p} as receiver: {e}");
                false
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

    /// Whether partition `p`'s activation lock is replicated through Raft. Static:
    /// only [`ActivationPolicy::Always`] replicates the lock. `Auto` (like
    /// `LeaderLocal`/`Digest`) never does — the strict replicated lease costs a
    /// per-activation quorum commit that wedges at high throughput regardless of
    /// payload size, so the zero-config default stays leader-local and lets the soft
    /// digest cover failover. The `p` argument is retained for call-site symmetry.
    fn replicate_activation_for(&self, _p: u64) -> bool {
        match self.activation_policy {
            ActivationPolicy::Always => true,
            ActivationPolicy::LeaderLocal | ActivationPolicy::Digest | ActivationPolicy::Auto => {
                false
            }
        }
    }

    /// Returns (building if necessary) the dedicated engine actor for a partition
    /// this node **replicates but does not own**. The Raft state machine drives it
    /// to apply the replicated log on a follower; it is not part of the read-model
    /// / serving path. Seeded with the current deployments so it can apply creates
    /// of already-deployed processes; later deploys fan in via
    /// [`Self::install_into_raft_replicas`].
    async fn replica_engine_for(&self, p: u64) -> DeepthiHandle {
        if let Some(h) = self.raft_replicas.lock().unwrap().get(&p) {
            return h.clone();
        }
        let mut journal = Journal::in_memory_partition(p);
        journal.set_num_partitions(self.engine.topology().num_partitions);
        if self.activation_policy.may_be_leader_local() {
            journal.set_lenient_completion(true);
        }
        // Wire the same spill tiers the owned engines got, so this follower
        // replica reclaims hot RAM under pressure instead of pinning the whole
        // replicated working set resident. Spill is local memory management (no
        // key mint / no events / no proposal); a replicated command rehydrates a
        // cold instance on demand via `ensure_resident_for_command`.
        if let Some(config) = &self.spill_config {
            config.apply(&mut journal);
        }
        let seed = self.current_deployment_events().await;
        if !seed.is_empty() {
            journal.install_deployment(&seed);
        }
        let handle = DeepthiHandle::spawn(journal, p, None);
        self.raft_replicas
            .lock()
            .unwrap()
            .entry(p)
            .or_insert(handle)
            .clone()
    }

    /// The partitions this node currently LEADS: every partition for which it
    /// hosts a Raft group whose current leader is this node. In steady state this
    /// equals the statically owned set; after a failover it also includes
    /// partitions this node only replicates but was elected to lead. The serving
    /// paths (activation, clock tick) fan out over THIS set so serving follows
    /// leadership, not static ownership — the new leader of a failed partition
    /// drives its jobs and timers.
    fn led_partitions(&self) -> Vec<u64> {
        let node_id = self.engine.topology().node_id as u64;
        (0..self.engine.topology().num_partitions)
            .filter(|&p| {
                self.raft.get(p).is_some_and(|part| {
                    part.raft.metrics().borrow().current_leader == Some(node_id)
                })
            })
            .collect()
    }

    /// The engine actor materializing partition `p` on this node: the owned actor
    /// if this node owns `p`, else the Raft replica actor (present when this node
    /// replicates `p`). `None` if this node neither owns nor replicates `p`. Used
    /// by the serving paths to reach a partition this node leads after a failover
    /// even though it does not statically own it.
    fn engine_handle_for(&self, p: u64) -> Option<DeepthiHandle> {
        if let Some(h) = self.engine.local_for_partition(p) {
            return Some(h.clone());
        }
        self.raft_replicas.lock().unwrap().get(&p).cloned()
    }

    /// One read-model reconciliation sweep. Rebuilds the engine's authoritative
    /// live-instance set (hot ∪ cold) across every partition this node hosts and
    /// retires any read-model `Active` row absent from it — an orphan whose CREATE
    /// was projected here but whose terminal event never was (e.g. leadership
    /// moved mid-instance, so the completion was exported by the new leader).
    /// Left unreconciled such rows inflate the active-backlog gauge forever,
    /// biasing admission control and Stage-2 fairness routing.
    ///
    /// Skipped while creates are mid-apply (`processing > 0`) so it runs only when
    /// the node is quiescent: the residual it targets forms *after* a load drains,
    /// and gating keeps the (worst-case O(active)) sweep off the hot path. The
    /// gauge is decremented by the number retired via the same atomic saturating
    /// add the exporter uses, so a genuinely in-flight completion that races the
    /// sweep (idempotent under the read model's `WHERE state = 0` guard) can never
    /// double-count. Returns the number of rows reconciled.
    async fn reconcile_orphans_once(&self) -> usize {
        if self.processing.load(Ordering::Relaxed) != 0 {
            return 0;
        }
        let num = self.engine.topology().num_partitions;
        let mut live: std::collections::HashSet<Key> = std::collections::HashSet::new();
        for p in 0..num {
            let Some(handle) = self.engine_handle_for(p) else {
                continue;
            };
            let keys = handle
                .with(|journal| {
                    let mut s = std::collections::HashSet::new();
                    journal.collect_live_instance_keys(&mut s);
                    s
                })
                .await;
            live.extend(keys);
        }
        let reconciled = self.store.reconcile_orphaned_active(&live);
        if reconciled > 0 {
            inflight_saturating_add_signed(&self.inflight, -(reconciled as i64));
            tracing::info!(
                reconciled,
                live = live.len(),
                "read-model reconcile: retired orphaned Active rows (runtime sweep)"
            );
        }
        reconciled
    }

    /// Stores the latest best-effort lease digest received from `partition`'s
    /// leader (soft state; never journaled). Consulted on leadership takeover by
    /// [`Self::run_lease_digest`]. A no-op-cost overwrite: only the most recent
    /// digest per partition is retained.
    pub fn record_lease_digest(&self, partition: u64, leases: Vec<(u64, u64)>, _sent_at: u64) {
        if !self.lease_digest {
            return;
        }
        self.lease_digests
            .lock()
            .unwrap()
            .insert(partition, ReceivedDigest { leases });
    }

    /// One pass of the soft lease-digest protocol, driven by the 500ms tick when
    /// `NANOBPMN_REPLICATE_ACTIVATION=digest`. For every partition this node leads:
    ///
    /// 1. **Recover** any digest received from the previous leader — for each
    ///    in-flight lease still `Created` here (this node followed the partition
    ///    and never saw the leader-local activation), mark it `Activated` until the
    ///    original deadline ([`Journal::recover_lease`]). A long-stable leader holds
    ///    no received digest for its own led partitions, so this fires only just
    ///    after a promotion. Idempotent across ticks (already-activated jobs are
    ///    skipped); the stored digest is evicted once all its deadlines have passed.
    /// 2. **Broadcast** this node's current leases to the partition's followers
    ///    (fire-and-forget) so a future leader can in turn recover them.
    ///
    /// Soft/lossy by design: a dropped or stale digest only widens the failover
    /// redelivery window slightly; it never affects correctness (at-least-once is
    /// preserved by the leases themselves and lenient completion).
    async fn run_lease_digest(&self, now: u64) {
        let led = self.led_partitions();
        let node_id = self.engine.topology().node_id;
        for p in led {
            let handle = match self.engine_handle_for(p) {
                Some(h) => h,
                None => continue,
            };
            // 1. Recover a received digest (only present just after a promotion).
            let stored = self.lease_digests.lock().unwrap().get(&p).cloned();
            if let Some(digest) = stored {
                let leases = digest.leases.clone();
                let recover_handle = handle.clone();
                recover_handle
                    .with(move |journal| {
                        for (job_key, deadline) in &leases {
                            journal.recover_lease(*job_key, *deadline, now);
                        }
                    })
                    .await;
                // Drop the digest once every lease in it has expired: there is
                // nothing left to recover and we must not pin stale state.
                let max_deadline = digest.leases.iter().map(|(_, d)| *d).max().unwrap_or(0);
                if max_deadline <= now {
                    self.lease_digests.lock().unwrap().remove(&p);
                }
            }
            // 2. Broadcast our current leases to this partition's followers.
            let leases = handle.with(|journal| journal.activated_leases()).await;
            let followers: Vec<u32> = self
                .engine
                .topology()
                .replicas_of(p)
                .into_iter()
                .filter(|n| *n != node_id)
                .collect();
            for follower in followers {
                if let Ok(link) = self.peers.link(follower).await {
                    // Fire-and-forget: a failed send just skips this round.
                    let _ = link.send_lease_digest(p, leases.clone(), now).await;
                }
            }
        }
    }

    /// Fans a deployment into every replica engine actor (followers under RF>1) so
    /// a replicated `CreateInstance` for the new definition applies successfully on
    /// every replica. A no-op (and zero overhead) when this node hosts no replica
    /// actors, i.e. single-node, RF=1, or Raft disabled.
    async fn install_into_raft_replicas(&self, events: &Arc<Vec<Event>>) {
        let handles: Vec<DeepthiHandle> = {
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
                crate::metrics::record_jobs_dispatched(&job_type, jobs.len() as u64);
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
        // Raft path: activation is a state mutation (it locks jobs), so when this
        // node hosts Raft groups it MUST replicate through the leader's log — else
        // the lock never reaches followers and a later replicated `CompleteJob`
        // hits `JobNotActivated` on them, diverging the replica. We fan out the
        // per-partition shares to `activate_on_raft`, which proposes `ActivateJobs`
        // on partitions this node leads and skips the rest (their leader activates).
        if !self.raft.is_empty() {
            let led = self.led_partitions();
            let ln = led.len();
            if ln == 0 {
                return Vec::new();
            }
            let start = self.engine.activate_start() % ln;
            use futures_util::FutureExt;
            let base = max_jobs / ln;
            let rem = max_jobs % ln;
            let mut futures = Vec::with_capacity(ln);
            for off in 0..ln {
                let want = base + usize::from(off < rem);
                if want == 0 {
                    continue;
                }
                let p = led[(start + off) % ln];
                if self.replicate_activation_for(p) {
                    futures.push(
                        self.activate_on_raft(p, job_type, worker, want, timeout)
                            .boxed(),
                    );
                } else {
                    // Leader-local activation: lock the jobs directly on this
                    // leader's engine actor WITHOUT a Raft round-trip. The lock is
                    // ephemeral leader state; followers learn of the job only when
                    // its (replicated) completion arrives, which they apply under
                    // lenient completion. Saves one quorum commit per activation and
                    // keeps per-worker activation off the partition's commit budget.
                    let Some(handle) = self.engine_handle_for(p) else {
                        continue;
                    };
                    futures.push(
                        self.activate_on_local(handle, job_type, worker, want, timeout)
                            .boxed(),
                    );
                }
            }
            let activated: Vec<ActivatedJobWithIdentity> = futures_util::future::join_all(futures)
                .await
                .into_iter()
                .flatten()
                .collect();
            return activated
                .into_iter()
                .map(|activated| activated_job_result(activated, fetch_variable))
                .collect();
        }
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
        handle: &DeepthiHandle,
        job_type: &str,
        worker: &str,
        want: usize,
        timeout: u64,
    ) -> Vec<ActivatedJobWithIdentity> {
        let job_type = job_type.to_string();
        let worker = worker.to_string();
        #[cfg(feature = "console")]
        let worker_for_trace = worker.clone();
        let activated: Vec<ActivatedJobWithIdentity> = handle
            .with(move |engine| {
                // Leader-local activation bypasses the Raft `apply_command_at`
                // path, so it is profiled here directly on the engine thread.
                let timer = cmd_profile::start();
                let now = now_millis();
                let out: Vec<ActivatedJobWithIdentity> = engine
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
                    .collect();
                cmd_profile::finish(timer, "activate_jobs");
                out
            })
            .await;
        #[cfg(feature = "console")]
        self.record_trace_activations(&activated, &worker_for_trace);
        activated
    }

    /// Owned-handle variant of [`Self::activate_on`] for the leader-local Raft
    /// activation path (`NANOBPMN_REPLICATE_ACTIVATION=0`): locks jobs directly on
    /// the supplied engine actor without proposing through Raft. Takes the handle
    /// by value so it can be awaited inside a `join_all` over the led partitions.
    async fn activate_on_local(
        &self,
        handle: DeepthiHandle,
        job_type: &str,
        worker: &str,
        want: usize,
        timeout: u64,
    ) -> Vec<ActivatedJobWithIdentity> {
        self.activate_on(&handle, job_type, worker, want, timeout)
            .await
    }

    /// `p`'s leader so the activation lock is committed to the log and applied on
    /// every replica's engine actor (keeping followers in lockstep for a later
    /// `CompleteJob`). Only the leader activates; a non-leader replica returns
    /// empty (the partition's actual leader runs its own dispatch). After the
    /// commit, the activated jobs are projected for the worker by a read on the
    /// leader's engine actor (the same copy the log was applied to).
    async fn activate_on_raft(
        &self,
        p: u64,
        job_type: &str,
        worker: &str,
        want: usize,
        timeout: u64,
    ) -> Vec<ActivatedJobWithIdentity> {
        let Some(part) = self.raft.get(p) else {
            return Vec::new();
        };
        let node_id = self.engine.topology().node_id as u64;
        if part.raft.metrics().borrow().current_leader != Some(node_id) {
            return Vec::new();
        }
        // A single logical instant drives both the command (job-lock deadlines)
        // and the journal apply, so leader and followers mint identical state.
        let now = now_millis();
        let response = match part
            .propose_result(
                Command::activate_jobs(job_type, worker, want, timeout, now),
                now,
            )
            .await
        {
            Ok(r) if r.error.is_none() => r,
            _ => return Vec::new(),
        };
        let job_keys: Vec<Key> = response
            .events
            .iter()
            .filter_map(|e| match e {
                Event::JobActivated { job_key, .. } => Some(*job_key),
                _ => None,
            })
            .collect();
        if job_keys.is_empty() {
            return Vec::new();
        }
        let Some(handle) = self.engine_handle_for(p) else {
            return Vec::new();
        };
        let activated: Vec<ActivatedJobWithIdentity> = handle
            .with(move |journal| {
                let engine = journal.engine();
                job_keys
                    .into_iter()
                    .filter_map(|jk| {
                        let job = engine.activated_job(jk)?;
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
                        Some(ActivatedJobWithIdentity {
                            job,
                            process_id,
                            version,
                            process_definition_key,
                        })
                    })
                    .collect()
            })
            .await;
        #[cfg(feature = "console")]
        self.record_trace_activations(&activated, worker);
        activated
    }

    /// Feeds a batch of just-activated jobs into the console trace projection.
    /// Job activation locks are ephemeral — never journaled or exported — so the
    /// exporter-fed projection cannot observe them; this is the dedicated hook
    /// that lets a trace show `worker` / `queueMs` / `serviceMs` / `attempts`.
    /// Cheap: a `Vec` of key pairs and one mutex-guarded fold, off the journal
    /// commit path. Console builds only.
    #[cfg(feature = "console")]
    fn record_trace_activations(&self, activated: &[ActivatedJobWithIdentity], worker: &str) {
        if activated.is_empty() {
            return;
        }
        let acts: Vec<(u64, u64)> = activated
            .iter()
            .map(|a| (a.job.instance_key, a.job.key))
            .collect();
        self.trace_store
            .record_activations(&acts, worker, now_millis());
    }

    /// The Raft clock tick for partition `p`: when this node leads `p`, replicate
    /// `TriggerTimers` and `ExpireJobs` through the log so every replica fires the
    /// same timers / reclaims the same leases at the same logical `now`, in the
    /// same order as client writes. Mutations that mint state (a fired timer may
    /// create a job) MUST be logged or follower key allocation diverges. A cheap
    /// local pre-check skips proposing an empty tick (no due timers / leases), so
    /// an idle partition adds no log entries. Cold-spill stays a local read-model
    /// op (not replicated). Returns `(produced, routable)` like the direct tick.
    async fn tick_partition_via_raft(
        &self,
        p: u64,
        now: u64,
        multi_partition: bool,
    ) -> (bool, Vec<Event>) {
        let Some(part) = self.raft.get(p) else {
            return (false, Vec::new());
        };
        let node_id = self.engine.topology().node_id as u64;
        if part.raft.metrics().borrow().current_leader != Some(node_id) {
            return (false, Vec::new());
        }
        let Some(handle) = self.engine_handle_for(p) else {
            return (false, Vec::new());
        };
        // One read on the leader: variable/cold spill + what (if anything) is due
        // now. Only the leader runs this gate; followers never propose, so safe.
        let (timers_due, jobs_due) = handle
            .with(move |journal| {
                // The tick pre-check gate runs on the single-writer engine actor
                // every tick per led partition; profiled here to attribute its
                // share of the actor hold under load.
                let timer = cmd_profile::start();
                // Shed active-backlog variables first (cheaper, instance stays
                // live), then whole dormant instances — both gated on RAM pressure.
                journal.maybe_var_spill_pressure();
                journal.maybe_cold_spill();
                let state = journal.engine().state();
                let timers_due = state.timers.values().any(|t| t.due_at <= now);
                // Only `Activated` jobs hold a lease deadline, and `ExpireJobs`
                // reclaims exactly that set (it iterates `activated_jobs`). Gate on
                // the same index rather than scanning every job: this pre-check runs
                // on the single-writer engine actor every tick (~2 Hz) per led
                // partition, so a full `jobs.values()` walk is O(total backlog) —
                // O(active) — and starves activation/completion on the actor as the
                // in-flight backlog grows (the congestion-collapse hot path). The
                // indexed walk is O(activated), bounded by concurrently-leased jobs.
                let jobs_due = state.activated_jobs.iter().any(|k| {
                    state
                        .jobs
                        .get(k)
                        .and_then(|j| j.deadline)
                        .is_some_and(|d| d <= now)
                });
                cmd_profile::finish(timer, "tick_precheck");
                (timers_due, jobs_due)
            })
            .await;

        let mut produced = false;
        let mut routable: Vec<Event> = Vec::new();
        if timers_due
            && let Ok(resp) = part
                .propose_result(Command::TriggerTimers { now }, now)
                .await
            && resp.error.is_none()
            && !resp.events.is_empty()
        {
            produced = true;
            if multi_partition {
                routable.extend(
                    resp.events
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
                        .cloned(),
                );
            }
        }
        if jobs_due {
            if self.replicate_activation_for(p) {
                if let Ok(resp) = part.propose_result(Command::ExpireJobs { now }, now).await
                    && resp.error.is_none()
                    && !resp.events.is_empty()
                {
                    produced = true;
                }
            } else {
                // Leader-local activation: the job lock lives ONLY on this leader's
                // engine actor (it was never replicated), so expiring leases must
                // stay leader-local too. Proposing `ExpireJobs` through Raft would
                // emit `JobLockExpired` on the leader (job is Activated) but nothing
                // on followers (their job is still Created), diverging the replicated
                // event stream. Expire directly on the leader's engine actor.
                let expired = handle
                    .with(move |journal| {
                        let timer = cmd_profile::start();
                        let expired = journal.expire_jobs(now);
                        cmd_profile::finish(timer, "expire_jobs");
                        expired
                    })
                    .await;
                if !expired.is_empty() {
                    produced = true;
                }
            }
        }
        (produced, routable)
    }
}

/// Engine-facing helpers used by the WebSocket Falcon protocol (`falcon`).
/// They mirror the core of the REST handlers above but return plain data instead
/// of the generated response envelope, so the stream can build its own frames.
/// They live here (not in `falcon`) to keep all engine command issuing
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
        #[allow(clippy::type_complexity)]
        let outcome: Result<
            (nanobpmn_engine_core::Key, bool, Vec<Event>, Commit),
            (u16, String),
        > = {
            let payload_bytes = engine_vars_bytes(&variables);
            let _processing = ProcessingGuard::enter(&self.processing);
            let _bytes = ByteGuard::enter(&self.pipeline_bytes, payload_bytes);
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

    /// Cluster-wide create *placement* for the falcon create path, the
    /// stream sibling of the REST `create_process_instance` seam. Round-robins
    /// over every partition in the cluster and returns the remote node that owns
    /// the chosen one, or `None` when the placement is local (create here).
    ///
    /// Without this, a stream create only ever ran on the entry gateway's own
    /// partitions (`for_create`), so a producer on a single falcon
    /// connection concentrated *every* instance on one node — while REST creates,
    /// which already use [`next_create_placement`](crate::partition::Partitions::next_create_placement),
    /// spread across the whole cluster. The per-node console metrics faithfully
    /// reported that real imbalance ("all instances created on one node").
    ///
    /// The Raft path keeps its own leader-aware forwarding in
    /// [`create_via_raft`](Self::create_via_raft), so placement is disabled when
    /// this node hosts Raft groups; the non-Raft cluster path is the one balanced
    /// here.
    pub(crate) fn stream_create_placement(&self) -> Option<u32> {
        if !self.raft.is_empty() {
            return None;
        }
        if self.placement_mode.balances() {
            self.next_create_placement_weighted(&[])
        } else {
            self.engine.next_create_placement()
        }
    }

    /// Forwards a fire-and-forget stream create to `first`, rerouting it to
    /// another owner if that owner placement-sheds (ADR 0014 `protect`/`balanced`).
    /// Returns `Some(result)` when the create resolves on a peer (success, a
    /// client rejection, or a non-shed error), or `None` to fall through to a
    /// **local** create — placement chose this node, protection is off, or every
    /// owner shed. Only the explicit placement-shed signal triggers a reroute, so
    /// an ambiguous post-send transport error can never duplicate an instance.
    pub(crate) async fn create_forwarded_stream_rerouting(
        &self,
        first: u32,
        by_id: Option<String>,
        by_key: Option<String>,
        variables: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Option<Result<(nanobpmn_engine_core::Key, bool), (u16, String)>> {
        if !self.placement_mode.protects() {
            return Some(
                self.create_forwarded_stream(first, by_id, by_key, variables)
                    .await,
            );
        }
        let mut tried: Vec<u32> = Vec::new();
        let bound = self.engine.partition_count().max(1) + 1;
        let mut node = first;
        for _ in 0..bound {
            match self
                .create_forwarded_stream(node, by_id.clone(), by_key.clone(), variables.clone())
                .await
            {
                Err((503, msg)) if msg.starts_with(PLACEMENT_SHED_MARKER) => {
                    tried.push(node);
                    let next = if self.placement_mode.balances() {
                        self.next_create_placement_weighted(&tried)
                    } else {
                        self.engine.next_create_placement_avoiding(&tried)
                    };
                    match next {
                        Some(n) => node = n,
                        None => return None, // exhausted -> local create
                    }
                }
                other => return Some(other),
            }
        }
        None
    }

    /// Falcon sibling of [`forward_create`](Self::forward_create): forwards
    /// a **fire-and-forget** stream create to the peer that owns the placed
    /// partition (over the shared `ForwardCreate` seam, which the peer answers via
    /// [`create_forwarded`](Self::create_forwarded)) and returns the minted
    /// `(instance_key, sync_completed)` so the stream handler can answer its
    /// `CommandResult`.
    ///
    /// Fire-and-forget only: `awaitCompletion` stream creates stay local because
    /// the completion wait ([`await_process_completion`](Self::await_process_completion))
    /// reads this node's per-partition read store and cannot observe an instance
    /// that lives on a peer. The stream handler enforces that split.
    pub(crate) async fn create_forwarded_stream(
        &self,
        node: u32,
        by_id: Option<String>,
        by_key: Option<String>,
        variables: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Result<(nanobpmn_engine_core::Key, bool), (u16, String)> {
        let link = self.peer_link(node).await?;
        let res = link
            .forward_create(
                by_id,
                by_key,
                variables,
                Vec::new(),
                None,
                false,
                None,
                None,
            )
            .await
            .map_err(|e| (502u16, e.to_string()))?;
        if !is_ok_status(res.status) {
            return Err((res.status, peer_detail(&res)));
        }
        let body = res
            .body
            .ok_or((502u16, "peer returned no create result body".to_string()))?;
        let instance_key = body
            .get("processInstanceKey")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<nanobpmn_engine_core::Key>().ok())
            .ok_or((
                502u16,
                "peer create result missing processInstanceKey".to_string(),
            ))?;
        let sync_completed = body
            .get("processCompleted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        Ok((instance_key, sync_completed))
    }

    /// The Raft create path (experimental): place the create cluster-wide across
    /// EVERY partition (leader-aware, via [`Self::stream_leader_placement`]) and
    /// replicate `CreateInstance` through the chosen partition's Raft log. The
    /// state machine applies the committed command to the same engine actor the
    /// rest of the server reads from, so durability and serving share one
    /// materialized copy. Returns the minted instance key and whether it completed
    /// synchronously (no async jobs), matching [`Self::create_for_stream`].
    ///
    /// Placement forwards each create to the partition's CURRENT leader, so a
    /// producer attached to one gateway drives the whole cluster — not just the
    /// partitions this node happens to lead (the RF>=2 stream imbalance that left a
    /// recovered node with no attached producer receiving zero creates). When a
    /// placement lands on a partition THIS node leads it commits locally among the
    /// *led* partitions (`for_create_among`); when this node leads NO partition (a
    /// transient window right after losing every leadership) the create is
    /// FORWARDED to a peer that leads one rather than returning a retryable 503.
    /// Cluster-wide, leader-aware create placement for the Raft stream create
    /// path. Advances the shared round-robin cursor to a partition and resolves
    /// its CURRENT Raft leader: returns `Some(node)` when a *remote* node leads it
    /// (forward the create there), or `None` when this node leads it, its leader
    /// is unknown, or the cluster is single-partition (create locally).
    ///
    /// Routing to the live LEADER — not the static owner ([`stream_create_placement`]
    /// / `next_create_placement`) — is what keeps placement failover-safe: while an
    /// owner is down its partitions resolve to the incumbent leader, and once the
    /// owner returns and reclaims leadership they resolve back to it. This closes
    /// the RF>=2 stream-create imbalance where a producer attached to ONE gateway
    /// only ever committed on the partitions THIS node led, starving peer-led
    /// partitions — most visibly a freshly recovered node with no directly
    /// attached producer, which reclaimed leadership but received zero creates.
    /// The REST path already spreads via `next_create_placement`; this gives the
    /// falcon stream path the same spread, but leader-aware.
    fn stream_leader_placement(&self) -> Option<u32> {
        let p = self.engine.next_create_partition()?;
        let node_id = self.engine.topology().node_id as u64;
        match self
            .raft
            .get(p)
            .and_then(|part| part.raft.metrics().borrow().current_leader)
        {
            Some(leader) if leader != node_id => Some(leader as u32),
            _ => None,
        }
    }

    async fn create_via_raft(
        &self,
        by_id: Option<String>,
        by_key: Option<String>,
        variables: std::collections::HashMap<String, Value>,
    ) -> Result<(nanobpmn_engine_core::Key, bool), (u16, String)> {
        if let Some(message) = self.admission_shed() {
            return Err((503, message));
        }
        // Cluster-wide, leader-aware placement (see `stream_leader_placement`):
        // spread stream creates across EVERY partition and forward each to its
        // current leader, so a producer on one gateway drives the whole cluster —
        // including a recovered node that reclaimed leadership but has no directly
        // attached producer. A local/own-leader placement (`None`) falls through to
        // the local propose below.
        if let Some(leader) = self.stream_leader_placement() {
            let wire_vars = if variables.is_empty() {
                None
            } else {
                Some(
                    variables
                        .iter()
                        .map(|(k, v)| (k.clone(), value_to_json(v)))
                        .collect(),
                )
            };
            return match self
                .create_forwarded_stream(leader, by_id.clone(), by_key.clone(), wire_vars)
                .await
            {
                Ok(res) => Ok(res),
                // A genuine client rejection is returned as-is; any other failure
                // (an unreachable or just-lost leader) becomes a retryable 503 so
                // the client re-places onto a healthy leader. We deliberately do
                // NOT fall through to a local create here, so an ambiguous
                // post-send transport error can never mint a duplicate instance.
                Err((400, m)) => Err((400, m)),
                Err((409, m)) => Err((409, m)),
                Err((_, m)) => Err((503, m)),
            };
        }
        // This node leads nothing right now: forward to a peer leader instead of
        // shedding a 503 the client would have to retry.
        if self.led_partitions().is_empty() {
            return self
                .forward_create_to_leader(by_id, by_key, variables)
                .await;
        }
        // Attempt a local quorum-commit on a led partition. Retain the inputs
        // (cheap clone of small/empty maps, off the single-writer thread) so that
        // if our leadership view turns out to be stale — e.g. metrics still name
        // us leader of a group that has since stopped or moved — we fall back to
        // forwarding to the current leader instead of surfacing a 500 the client
        // would have to retry.
        match self
            .raft_create_core(
                by_id.clone(),
                by_key.clone(),
                variables.clone(),
                Vec::new(),
                None,
            )
            .await
        {
            Ok((
                _process_id,
                _version,
                _definition_key,
                instance_key,
                sync_completed,
                routable,
            )) => {
                if !routable.is_empty() {
                    self.drive_subscription_routing(routable).await;
                }
                self.signal_jobs_available();
                Ok((instance_key, sync_completed))
            }
            Err(e) if Self::create_should_forward(&e) => {
                self.forward_create_to_leader(by_id, by_key, variables)
                    .await
            }
            Err(e) => Err(e),
        }
    }

    /// Whether a failed local Raft create should be retried by forwarding to a
    /// peer leader. A leadership/propose failure (this node's view of leading the
    /// chosen partition was stale — the group stopped, stepped down, or the
    /// leadership moved) is forwardable; a genuine client rejection (a 400/409
    /// from validation or the engine state machine) must be returned as-is.
    fn create_should_forward(err: &(u16, String)) -> bool {
        let (status, message) = err;
        *status == 503
            || (*status == 500
                && (message.contains("raft propose failed")
                    || message.contains("has no Raft group")
                    || message.contains("no local engine actor")))
    }

    /// Forwards a REST create to a peer leader, returning the full REST response
    /// (the peer threads `awaitCompletion`). Used by
    /// [`create_rest_via_raft`](Self::create_rest_via_raft) when this node leads
    /// nothing or its local propose failed on a stale leadership view. Sheds a
    /// retryable 503 when no partition has a reachable leader yet.
    #[allow(clippy::too_many_arguments)]
    async fn forward_rest_create_to_leader(
        &self,
        by_id: Option<String>,
        by_key: Option<String>,
        wire_vars: Option<serde_json::Map<String, serde_json::Value>>,
        tags: Vec<String>,
        business_id: Option<String>,
        await_completion: bool,
        fetch_variables: Option<Vec<String>>,
        request_timeout: Option<i64>,
    ) -> apis::process_instance::CreateProcessInstanceResponse {
        use apis::process_instance::CreateProcessInstanceResponse as Resp;
        // Non-await creates use the bounded, leader-re-resolving forward so a
        // create racing a leader failure fails fast and retries on the new
        // leader instead of pinning for the 30s peer timeout. Await-completion
        // creates legitimately block until the instance completes, so they keep
        // the long-lived forward bounded only by the client's request timeout.
        if !await_completion {
            return self
                .forward_create_bounded(
                    by_id,
                    by_key,
                    wire_vars,
                    tags,
                    business_id,
                    fetch_variables,
                    request_timeout,
                )
                .await;
        }
        match self.leader_node_for_create() {
            Some(node) => {
                self.forward_create(
                    node,
                    by_id,
                    by_key,
                    wire_vars,
                    tags,
                    business_id,
                    await_completion,
                    fetch_variables,
                    request_timeout,
                )
                .await
            }
            None => Resp::Status503_TheServiceIsCurrentlyUnavailable(problem(
                "RESOURCE_EXHAUSTED",
                503,
                "no partition leader reachable; retry".to_string(),
            )),
        }
    }

    /// REST `createProcessInstance` through Raft (RF>=2). Mirrors the stream
    /// [`create_via_raft`](Self::create_via_raft) — leadership-following placement
    /// over the led partitions, with a leader-forward fallback when this node
    /// leads nothing or its leadership view of the chosen partition was stale —
    /// but returns the full REST result (definition identity + `awaitCompletion`
    /// variables) instead of just `(key, sync)`. Replication closes the durability
    /// gap of the legacy stage-1 direct-apply local path: a REST-created instance
    /// now survives this node's failure exactly like a stream-created one.
    ///
    /// `awaitCompletion` observes the read model by key
    /// ([`await_process_completion`](Self::await_process_completion)) and is wholly
    /// decoupled from how the instance was created, so it behaves identically to
    /// the direct-apply path with no lost-wakeup race: its register-before-read
    /// loop catches a synchronously-completed instance on the first iteration.
    #[allow(clippy::too_many_arguments)]
    async fn create_rest_via_raft(
        &self,
        by_id: Option<String>,
        by_key: Option<String>,
        variables: std::collections::HashMap<String, Value>,
        wire_vars: Option<serde_json::Map<String, serde_json::Value>>,
        tags: Vec<String>,
        business_id: Option<String>,
        await_completion: bool,
        fetch_variables: Option<Vec<String>>,
        request_timeout: Option<i64>,
    ) -> apis::process_instance::CreateProcessInstanceResponse {
        use apis::process_instance::CreateProcessInstanceResponse as Resp;

        // This node leads nothing right now: forward instead of shedding a 503.
        if self.led_partitions().is_empty() {
            return self
                .forward_rest_create_to_leader(
                    by_id,
                    by_key,
                    wire_vars,
                    tags,
                    business_id,
                    await_completion,
                    fetch_variables,
                    request_timeout,
                )
                .await;
        }

        let core = self
            .raft_create_core(
                by_id.clone(),
                by_key.clone(),
                variables,
                tags.clone(),
                business_id.clone(),
            )
            .await;
        let (process_id, version, definition_key, instance_key, sync_completed, routable) =
            match core {
                Ok(fields) => fields,
                // Stale leadership / leads-nothing-now: forward to the current
                // leader rather than surface a retryable error.
                Err(e) if Self::create_should_forward(&e) => {
                    return self
                        .forward_rest_create_to_leader(
                            by_id,
                            by_key,
                            wire_vars,
                            tags,
                            business_id,
                            await_completion,
                            fetch_variables,
                            request_timeout,
                        )
                        .await;
                }
                Err((409, message)) => {
                    return Resp::Status409_TheProcessInstanceCreationWasRejectedDueToABusinessIDUniquenessConflict(
                        problem("Conflict", 409, message),
                    );
                }
                Err((400, message)) => {
                    return Resp::Status400_TheProvidedDataIsNotValid(problem(
                        "The provided data is not valid",
                        400,
                        message,
                    ));
                }
                Err((status, message)) => {
                    return Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(
                        problem("Internal error", status, message),
                    );
                }
            };

        crate::metrics::record_create("rest");
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
            tags.into_iter().map(models::Tag).collect(),
            business_id
                .map(nanobpm_gateway_rest::types::Nullable::Present)
                .unwrap_or(nanobpm_gateway_rest::types::Nullable::Null),
            process_completed,
        );
        Resp::Status200_TheProcessInstanceWasCreated(result)
    }

    /// Shared Raft create core: pick a partition this node leads, resolve the
    /// process-definition id, replicate `CreateInstance` through that partition's
    /// Raft log, and return the rich result fields both the stream create
    /// ([`create_via_raft`](Self::create_via_raft)) and the forwarded REST create
    /// ([`create_forwarded`](Self::create_forwarded)) need. Returns a retryable
    /// 503 if this node leads no partition (the caller decides whether to forward
    /// or shed).
    #[allow(clippy::type_complexity)]
    async fn raft_create_core(
        &self,
        by_id: Option<String>,
        by_key: Option<String>,
        variables: std::collections::HashMap<String, Value>,
        tags: Vec<String>,
        business_id: Option<String>,
    ) -> Result<
        (
            String,
            i32,
            String,
            nanobpmn_engine_core::Key,
            bool,
            Vec<Event>,
        ),
        (u16, String),
    > {
        let led = self.led_partitions();
        // Create write-gate: while this node (as a failover incumbent) is handing a
        // partition back to its returning owner, steer new creates OFF that
        // partition so its raft log quiesces and the hand-off learner can catch up
        // to zero lag. Cheap: the gate set is empty on the hot path.
        let led: Vec<u64> = led
            .into_iter()
            .filter(|&p| !self.handoff_write_gated(p))
            .collect();
        let Some(p) = self.engine.for_create_among(&led) else {
            return Err((503, "this node leads no partition; retry".to_string()));
        };
        let Some(handle) = self.engine_handle_for(p) else {
            return Err((500, format!("no local engine actor for partition {p}")));
        };

        // Resolve the process-definition id (a by-key create needs a read of the
        // engine's deployed-process table) before proposing — the command carries
        // a concrete `process_id`.
        let process_id = handle
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
            .await?;

        let Some(part) = self.raft.get(p) else {
            return Err((500, format!("partition {p} has no Raft group")));
        };
        let response = part
            .propose_result(
                Command::create_instance_full(process_id.clone(), variables, tags, business_id),
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
        // Project the deployed key + version now the instance exists, so by-id and
        // by-key creates report the same definition identity.
        let pid_for_read = process_id.clone();
        let (definition_key, version) = handle
            .with(move |journal| {
                journal
                    .state()
                    .processes
                    .get(&pid_for_read)
                    .map(|d| (d.key.to_string(), d.version))
                    .unwrap_or((pid_for_read, 1))
            })
            .await;
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
        Ok((
            process_id,
            version,
            definition_key,
            instance_key,
            sync_completed,
            routable,
        ))
    }

    /// Forwards a stream create to a peer that currently leads a partition, used
    /// when THIS node leads none. Picks any partition with a known remote leader
    /// and routes the create over the cluster create-forwarding seam (the peer
    /// commits it through its own Raft), mapping the peer's REST result back to
    /// the `(key, sync_completed)` shape the stream create returns. Falls back to
    /// a retryable 503 only when no partition has a reachable leader yet.
    /// The node id of a partition leader other than this node, if any partition
    /// currently has a reachable remote leader. Used by the create-forward paths
    /// (stream and REST) when this node leads nothing — or its leadership view of
    /// a chosen partition turned out to be stale — to pick a peer that can commit
    /// the create through its own Raft. Returns `None` when no partition has a
    /// known remote leader yet (the caller sheds a retryable 503).
    fn leader_node_for_create(&self) -> Option<u32> {
        let node_id = self.engine.topology().node_id as u64;
        (0..self.engine.topology().num_partitions)
            .find_map(|p| {
                self.raft
                    .get(p)
                    .and_then(|part| part.raft.metrics().borrow().current_leader)
                    .filter(|l| *l != node_id)
            })
            .map(|l| l as u32)
    }

    async fn forward_create_to_leader(
        &self,
        by_id: Option<String>,
        by_key: Option<String>,
        variables: std::collections::HashMap<String, Value>,
    ) -> Result<(nanobpmn_engine_core::Key, bool), (u16, String)> {
        use apis::process_instance::CreateProcessInstanceResponse as R;
        let wire_vars = if variables.is_empty() {
            None
        } else {
            Some(
                variables
                    .iter()
                    .map(|(k, v)| (k.clone(), value_to_json(v)))
                    .collect(),
            )
        };
        match self
            .forward_create_bounded(by_id, by_key, wire_vars, Vec::new(), None, None, None)
            .await
        {
            R::Status200_TheProcessInstanceWasCreated(result) => result
                .process_instance_key
                .0
                .parse::<u64>()
                .map(|key| (key, result.process_completed))
                .map_err(|_| (500, "peer returned a non-numeric instance key".to_string())),
            R::Status400_TheProvidedDataIsNotValid(p) => Err((400, p.detail)),
            R::Status409_TheProcessInstanceCreationWasRejectedDueToABusinessIDUniquenessConflict(p) => {
                Err((409, p.detail))
            }
            R::Status503_TheServiceIsCurrentlyUnavailable(p) => Err((503, p.detail)),
            R::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(p) => Err((500, p.detail)),
            _ => Err((500, "unexpected peer create response".to_string())),
        }
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
        // Bounded completion write-pause (ADR 0019): while this node is handing
        // `p` back to its returning owner, pause job-mutation writes so the raft
        // log fully quiesces and the catch-up learner can reach zero lag. Retryable
        // (at-least-once) — the worker redelivers once the brief pause lifts.
        if self.handoff_completion_paused(p) {
            crate::metrics::record_complete_outcome("handoff_pause");
            return Err((503, format!("partition {p} handing off; retry")));
        }
        let Some(part) = self.raft.get(p) else {
            return Err((500, format!("partition {p} has no Raft group")));
        };
        let node_id = self.engine.topology().node_id as u64;
        if part.raft.metrics().borrow().current_leader != Some(node_id) {
            crate::metrics::record_complete_outcome("leader_reject");
            return Err((503, format!("partition {p} leader unavailable; retry")));
        }
        let response = part
            .propose_result(command, now_millis())
            .await
            .map_err(|e| {
                crate::metrics::record_complete_outcome("propose_err");
                (500, format!("raft propose failed: {e}"))
            })?;
        if let Some((status, message)) = response.error {
            crate::metrics::record_complete_outcome("apply_err");
            return Err((status, message));
        }
        self.spawn_routing_if_needed(&response.events);
        Ok(Commit::ready())
    }

    /// Stream `CompleteJob`: applies the command on the engine actor (establishing
    /// journal order) and returns the [`Commit`] WITHOUT awaiting durability.
    ///
    /// The caller (`falcon::pipeline_job_command`) awaits the commit in a
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
    /// pipelining" and `falcon::pipeline_job_command` for full rationale.
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
                engine
                    .apply_command_at(Command::complete_job_with(job_key, variables), now_millis())
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
                .propose_job_for_stream(job_key, Command::fail_job(job_key, retries, error_message))
                .await;
        }
        let result = self
            .engine
            .by_key(job_key)
            .with(move |engine| {
                engine.apply_command_at(
                    Command::fail_job(job_key, retries, error_message),
                    now_millis(),
                )
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

    /// The SLA mode currently in effect. Read live from the shared handle, so it
    /// reflects any runtime switch.
    #[cfg(feature = "console")]
    pub(crate) fn sla_mode(&self) -> SlaMode {
        self.sla_mode.get()
    }

    /// Switch the SLA mode at runtime (e.g. from the console SLA knob). Takes
    /// effect on the next admission decision across all handlers.
    pub(crate) fn set_sla_mode(&self, mode: SlaMode) {
        let previous = self.sla_mode.get();
        self.sla_mode.set(mode);
        if previous != mode {
            tracing::info!(
                "SLA mode switched at runtime: {} -> {}",
                previous.as_str(),
                mode.describe()
            );
        }
    }

    /// Read access to the create-side backpressure controller for the stream's
    /// submission-credit policy. Honours the SLA mode: in
    /// [`SlaMode::Admission`](crate::backpressure::SlaMode::Admission) the
    /// latency-driven concurrency limit does not withhold submission credit
    /// (admission is preferred over latency); the memory-safety rails still apply.
    pub(crate) fn submission_pressure(&self) -> bool {
        if !self.sla_mode.get().sheds_for_latency() {
            return false;
        }
        let processing = self.processing.load(Ordering::Relaxed);
        self.backpressure.should_shed(processing)
    }

    /// Records one drain-side (completion-family) command apply: bumps the
    /// `nanobpm_job_completions_total` metric *and* the drain-stall guard's
    /// completion counter, so the guard's drain-rate estimate can never drift
    /// from the exported metric. Called from every completion site (REST + Falcon
    /// stream). Hot-path cheap (a metric inc + one relaxed atomic add).
    #[inline]
    pub(crate) fn note_job_completion(&self, protocol: &str) {
        crate::metrics::record_job_completion(protocol);
        self.drain_guard.note_completion();
    }

    /// Handle to the drain-stall guard (the monitor supervisor drives it; the
    /// admission gates read it).
    pub(crate) fn drain_guard(&self) -> &Arc<crate::drain_guard::DrainGuard> {
        &self.drain_guard
    }

    /// The live per-producer submission-window cap published by the adaptive
    /// submission governor ([`crate::submission_governor`]). The credit top-up /
    /// grant path caps each connection's effective window to
    /// `min(conn.submission_window, this)`, so create intake shrinks under
    /// capacity-loss latency and reopens to the ceiling on recovery. Equals the
    /// governor ceiling (a no-op cap) at steady state.
    pub(crate) fn submission_window_cap(&self) -> i64 {
        self.submission_window_cap.load(Ordering::Relaxed)
    }

    /// Whether new-instance admission is currently blocked by *either* the
    /// create-side latency backpressure ([`submission_pressure`](Self::submission_pressure))
    /// or the drain-stall guard. The submission-credit lanes and the fleet
    /// `Pressure` broadcast gate on this so create intake dries up under a drain
    /// stall in **both** SLA modes (the guard is a liveness rail, not a latency
    /// policy).
    pub(crate) fn create_admission_blocked(&self) -> bool {
        self.submission_pressure() || self.drain_guard.blocks_creates()
    }

    /// Drain-stall guard admission rail: sheds a create while the guard's hard
    /// valve is engaged. Evaluated in both SLA modes (it guards liveness, not
    /// latency). The soft servo (option 3) does **not** shed here — it paces
    /// intake at the submission-credit layer via
    /// [`crate::drain_guard::DrainGuard::take_credits`], not by rejecting creates —
    /// so only the hard valve surfaces as an admission shed. Returns the
    /// client-facing retry reason.
    fn drain_guard_shed(&self) -> Option<String> {
        if self.drain_guard.is_halted() {
            crate::metrics::record_admission_shed("drain_halt");
            return Some(
                "Admission control: completion drain stalled (~0 completions/s while a \
                 large active backlog is held); halting new instance creation until the \
                 drain recovers. Retry after a backoff."
                    .to_string(),
            );
        }
        None
    }

    /// This node's active (non-terminal) instance count — the cheap, live gauge
    /// maintained for admission control. Used as a per-node backlog proxy for
    /// Stage 2 fairness routing (piggybacked to peers on activation responses). A
    /// relaxed atomic load; no engine round-trip.
    pub(crate) fn active_backlog(&self) -> i64 {
        self.inflight.load(Ordering::Relaxed) as i64
    }

    /// Recomputes the unified admission caps from the live latency + memory signals
    /// and stores them. Called each ~1 Hz monitor tick. Returns the servo setpoint
    /// ([`Self::effective_backlog_cap`]). This is the single throttle the whole
    /// system converges on:
    ///
    /// * `latency_cap` = the backlog governor's live output (the latency knee). Only
    ///   participates in `SlaMode::Latency`; admission mode prefers admitting over
    ///   bounding latency, so there the servo paces on memory alone.
    /// * `memory_cap` = the current backlog plus the number of additional nominal
    ///   active instances that fit in the remaining memory headroom before the
    ///   resident watermark. As RAM fills this shrinks toward the current backlog.
    ///
    /// Two caps come out of it, and keeping them separate is what stops the servo
    /// and the shed from colliding (the collision that caused the goodput collapse):
    /// * **servo setpoint** = `min(latency_cap, memory_cap)` clamped to
    ///   `[floor, ceiling]` — the completion-paced credit servo bands against this,
    ///   so it owns the latency operating band.
    /// * **shed backstop** = `memory_cap` alone, clamped — the post-credit shed fires
    ///   against this. Because it excludes the (low, latency-pinned) latency term it
    ///   sits *above* the servo's burst envelope, so the shed only bites when memory
    ///   is genuinely filling or a burst bypassed the credit window — never inside
    ///   the servo's normal operating range.
    ///
    /// Both are `0` (disabled) when the backlog cap is off, leaving the servo on its
    /// absolute band and the shed following the raw governor cap.
    fn refresh_effective_backlog_cap(&self, active_backlog: i64) -> usize {
        // The recovery throttle's cap (0 = no clamp) is honoured in *both* SLA modes
        // and even when the general backlog cap is off — it is a recovery liveness
        // rail, not a latency policy.
        let recovery_cap = self.recovery_backlog_cap.load(Ordering::Relaxed);
        let ceiling = self.backlog_cap_ceiling;
        if ceiling == 0 {
            // General backlog cap disabled (Off): no latency/memory setpoint. Still
            // honour a live recovery clamp so intake is paced while the failover disk
            // saturates; the drain servo bands against the published setpoint.
            self.effective_backlog_cap
                .store(recovery_cap, Ordering::Relaxed);
            self.backlog_shed_cap.store(0, Ordering::Relaxed);
            return recovery_cap;
        }
        let latency_cap = self.backlog_cap.load(Ordering::Relaxed);
        let latency_component = if self.sla_mode.get().sheds_for_latency() && latency_cap > 0 {
            latency_cap
        } else {
            usize::MAX
        };
        let memory_cap = if self.mem_watermark_bytes > 0 {
            let used = self.mem_pressure_bytes.load(Ordering::Relaxed);
            let headroom_bytes = self.mem_watermark_bytes.saturating_sub(used);
            let headroom_insts = (headroom_bytes / NOMINAL_ACTIVE_BYTES) as usize;
            (active_backlog.max(0) as usize).saturating_add(headroom_insts)
        } else {
            usize::MAX
        };
        // Recovery clamp joins the setpoint min (below `ceiling`, above `floor`). A
        // `0` recovery cap means no clamp (treated as unbounded here).
        let recovery_component = if recovery_cap > 0 {
            recovery_cap
        } else {
            usize::MAX
        };
        // Servo setpoint: latency ∧ memory ∧ recovery. Shed backstop: memory only
        // (sits above the servo's operating band so it cannot collide with the
        // credit servo).
        let eff = latency_component
            .min(memory_cap)
            .min(recovery_component)
            .clamp(self.backlog_cap_floor, ceiling);
        let shed = memory_cap.clamp(self.backlog_cap_floor, ceiling);
        self.effective_backlog_cap.store(eff, Ordering::Relaxed);
        self.backlog_shed_cap.store(shed, Ordering::Relaxed);
        eff
    }

    /// The active-backlog / create-backlog shed threshold: the live memory-only
    /// backstop ([`Self::backlog_shed_cap`]), which sits above the completion-paced
    /// credit servo's operating band so the shed is a memory/burst backstop, not a
    /// latency rail that collides with the servo. Falls back to the raw governor cap
    /// when there is no active backstop (backlog cap disabled, or the first monitor
    /// tick has not run yet).
    fn backlog_shed_level(&self) -> usize {
        let shed = self.backlog_shed_cap.load(Ordering::Relaxed);
        if shed > 0 {
            shed
        } else {
            self.backlog_cap.load(Ordering::Relaxed)
        }
    }

    /// Trailing sentence for an active-backlog / create-backlog shed message that
    /// explains *why* the cap is what it is — so an operator isn't left staring at
    /// a shed threshold they never configured. In `Auto` mode the cap is the live
    /// output of the AIMD latency governor, so we name it as auto-tuned, give its
    /// floor/ceiling bounds, and (once a window has folded) report the baseline vs
    /// current per-command latency and the congestion threshold that drove the
    /// last backoff. In `Fixed`/`Off` mode the cap is a plain operator setting, so
    /// we just point at the tuning lever.
    fn backlog_cap_explainer(&self) -> String {
        let Some(gov) = &self.backlog_gov else {
            return " This is a fixed cap (NANOBPMN_ADMISSION_MAX_BACKLOG); \
                    raise it, or set NANOBPMN_SLA_MODE=admission to accept latency \
                    instead of shedding. Retry after a backoff."
                .to_string();
        };
        let baseline = gov.obs.baseline_us.load(Ordering::Relaxed);
        let window = gov.obs.window_avg_us.load(Ordering::Relaxed);
        let bounds = format!(
            " This cap is auto-tuned by the latency governor (floor {}, ceiling {} \
             runnable jobs) to hold per-command latency near its baseline",
            gov.floor, gov.ceiling
        );
        let latency = if baseline > 0 {
            let threshold = (baseline as f64 * CONGESTION_RATIO) as u64;
            format!(
                "; it backed the cap off because window latency {window}µs vs \
                 baseline {baseline}µs neared the {threshold}µs congestion threshold."
            )
        } else {
            ".".to_string()
        };
        format!(
            "{bounds}{latency} Retry after a backoff, set NANOBPMN_ADMISSION_MAX_BACKLOG \
             for a fixed cap, or NANOBPMN_SLA_MODE=admission to accept latency instead \
             of shedding."
        )
    }

    /// Admission gate combining several signals. Returns `Some(reason)` once any
    /// active signal is at/above its limit (the create should be shed); `None`
    /// when all have headroom. Relaxed atomic loads — no engine round-trip; an
    /// approximate bound is fine.
    ///
    /// The signals split into two classes by [`SlaMode`](crate::backpressure::SlaMode):
    /// - **Latency-preservation gate** — the active-backlog limit
    ///   (`NANOBPMN_ADMISSION_MAX_BACKLOG`). Suppressed in `SlaMode::Admission`
    ///   (that mode prefers admitting instances over bounding latency).
    /// - **Memory-safety rails** — the create-queue depth
    ///   (`NANOBPMN_ADMISSION_MAX_CREATE_QUEUE`, bounding the pre-apply mailbox of
    ///   variable-carrying closures), the exporter-queue saturation gate, the
    ///   in-flight create-payload watermark, and the resident-memory watermark.
    ///   These guard against OOM and therefore apply in **both** SLA modes — even
    ///   "start every process" cannot outrun physical memory + disk. Terminal
    ///   state now frees on completion (ADR 0012) and live variables spill to
    ///   disk, so in admission mode these rails bite far later than the
    ///   latency gate would have.
    pub(crate) fn admission_shed(&self) -> Option<String> {
        // Drain-stall guard hard valve (always on, both SLA modes): the liveness
        // rail. If the completion drain has fully stalled with a large backlog
        // held, shed new creates *first* — before any latency/memory rail — so
        // create entries stop crowding completions out of the shared Raft log and
        // the drain can recover. (The soft servo does not shed here; it paces
        // intake at the submission-credit layer.) Cheapest possible check (a
        // relaxed atomic load on a flag the ~1 Hz monitor publishes).
        if let Some(reason) = self.drain_guard_shed() {
            return Some(reason);
        }
        let cq_limit = self.admission_max_create_queue;
        // Post-credit backlog shed threshold: a burst backstop a margin *above* the
        // unified admission setpoint (the servo's operating band), not the raw
        // governor cap — so the completion-paced credit servo holds the backlog and
        // this shed only catches an overshoot the credit throttle could not.
        let backlog_limit = self.backlog_shed_level();
        let latency_mode = self.sla_mode.get().sheds_for_latency();
        // The standing create-queue depth (submitted-but-not-yet-applied creates)
        // is the backlog that actually grows under overload — completion-priority
        // makes creates yield, so this queue is where excess arrival piles up and
        // holds resident memory. Both the memory rail and the latency rail key off
        // it. Computed at most once, and only when a rail that needs it is armed,
        // so the fully-unconfigured hot path stays a few relaxed atomic loads.
        let need_create_queue = cq_limit > 0 || (latency_mode && backlog_limit > 0);
        let create_queue = if need_create_queue {
            self.engine.pending_create_queue()
        } else {
            0
        };

        // Memory rail (always on, both SLA modes): a proactive, count-based cap on
        // the unapplied-create backlog. It sheds *before* those creates' payloads
        // become resident, so an arrival flood is rejected early rather than
        // gathered in memory toward an OOM — the coarse resident-byte rails below
        // are the late backstop, this is the early one.
        if cq_limit > 0 && create_queue >= cq_limit {
            crate::metrics::record_admission_shed("create_queue");
            return Some(format!(
                "Admission control: create queue depth {create_queue} at or above the \
                 configured limit of {cq_limit}. Retry after a backoff."
            ));
        }
        // Latency-preservation rail (latency SLA mode only): shed once either the
        // runnable (task-job) backlog *or* the create-side backlog reaches the
        // cap, so end-to-end latency stays bounded. The runnable-backlog term is
        // the congestion-collapse guard: it is the parked-excluded load signal
        // (only service tasks create jobs), so bounding it holds the engine
        // actor's per-command cost off its O(active) tail *without* shedding a
        // legitimately parked population. The create-side term is the one that
        // bites for fast create->complete workloads: their runnable backlog drains
        // as fast as it's created, so bounding the create queue bounds
        // create->apply latency, the dominant queue an overloaded producer waits
        // behind. In `AdmissionBacklog::Auto` mode `backlog_limit` is retuned live
        // by the backlog governor to sit just left of the throughput knee.
        if latency_mode && backlog_limit > 0 {
            let backlog = self.runnable_backlog.load(Ordering::Relaxed);
            if backlog >= backlog_limit {
                crate::metrics::record_admission_shed("active_backlog");
                return Some(format!(
                    "Admission control: {backlog} runnable jobs at or above the \
                     active-backlog cap of {backlog_limit}.{}",
                    self.backlog_cap_explainer()
                ));
            }
            if create_queue >= backlog_limit {
                crate::metrics::record_admission_shed("create_backlog");
                return Some(format!(
                    "Admission control: create backlog {create_queue} at or above the \
                     active-backlog cap of {backlog_limit}.{}",
                    self.backlog_cap_explainer()
                ));
            }
        }
        // Exporter-queue backpressure: shed once every local read-model shard's
        // export queue is at budget, so the resident backlog of committed-but-
        // unprojected events (each holding a full copy of its variables) stays
        // bounded under a large-variable flood. `for_create` steers to a shard
        // with headroom first, so this only fires when the whole node is
        // saturated — never blocking the shared journal writer. A shed create is
        // never journaled, so durability/at-least-once are intact.
        if self.engine.exporter_all_saturated() {
            crate::metrics::record_admission_shed("exporter");
            return Some(
                "Admission control: all read-model export queues are at capacity. \
                 Retry after a backoff."
                    .to_string(),
            );
        }
        // In-flight create-payload gate: shed once the estimated payload bytes of
        // creates in the submit→apply window are at/above the watermark. This is
        // the precise, proactive memory rail — it bounds the engine `Low`-mailbox
        // balloon (an unbounded queue of creation closures each holding a full
        // copy of its variables) under a worker-starved large-payload burst
        // *before* those copies inflate resident memory, so it bites far earlier
        // and more cheaply than the coarse resident-memory backstop below. A shed
        // create is never journaled, so durability/at-least-once are intact.
        if self.pipeline_bytes_watermark > 0 {
            let bytes = self.pipeline_bytes.load(Ordering::Relaxed);
            if bytes >= self.pipeline_bytes_watermark {
                crate::metrics::record_admission_shed("pipeline_bytes");
                return Some(format!(
                    "Admission control: in-flight create payload {} MiB at or above the \
                     configured watermark of {} MiB. Retry after a backoff.",
                    bytes / (1024 * 1024),
                    self.pipeline_bytes_watermark / (1024 * 1024),
                ));
            }
        }
        // Memory-pressure gate: shed while resident memory is at/above the
        // watermark, so the transient live heap of in-flight large-variable
        // payloads (request bodies, event serialization, journal write buffers,
        // replication and exporter batches) can drain before more creates are
        // admitted. This is the last-resort rail beyond variable-spill: spill
        // offloads resting variables to disk, but the payloads still flow as full
        // live copies through ingest -> journal -> replication -> projection, and
        // a worker-starved large-payload burst can pile those up faster than they
        // drain (measured 12-16 GB RSS). Reads one cached atomic (refreshed by the
        // mem-pressure tick) so the hot path never touches jemalloc's stats epoch.
        // A shed create is never journaled, so durability/at-least-once are intact.
        if self.mem_watermark_bytes > 0 {
            let resident = self.mem_pressure_bytes.load(Ordering::Relaxed);
            if resident >= self.mem_watermark_bytes {
                crate::metrics::record_admission_shed("mem_watermark");
                return Some(format!(
                    "Admission control: resident memory {} MiB at or above the \
                     configured watermark of {} MiB. Retry after a backoff.",
                    resident / (1024 * 1024),
                    self.mem_watermark_bytes / (1024 * 1024),
                ));
            }
        }
        None
    }

    /// Composite "should this node shed an incoming create right now?" decision
    /// for cluster create-placement protection (ADR 0014). Returns `Some(reason)`
    /// when the node is saturated (either the create-side concurrency backpressure
    /// via [`submission_pressure`](Self::submission_pressure) or any
    /// [`admission_shed`](Self::admission_shed) rail is tripped), else `None`.
    ///
    /// It composes exactly the gates the ingress REST/stream paths already apply,
    /// so an owner shedding a *forwarded* create matches what it would have done
    /// to a *local* one — closing the "forwarded creates skip admission" gap. Used
    /// by the receiving-peer create path (to shed back to the ingress node for
    /// rerouting) and by [`create_load_index`](Self::create_load_index).
    pub(crate) fn create_should_shed(&self) -> Option<String> {
        if self.submission_pressure() {
            return Some("Backpressure: create-processing concurrency at capacity.".to_string());
        }
        self.admission_shed()
    }

    /// The capacity ceilings this node is currently pressed against — the
    /// compressor/limiter LEDs of ADR 0013, surfaced as Prometheus gauges by the
    /// monitor tick. Returns `(throughput, memory)`:
    /// - **throughput** — create-processing concurrency at/above the AIMD limit,
    ///   or the active-backlog latency gate at/above its limit. Reported in
    ///   *both* SLA modes: the ceiling is equally real whether the mode clips
    ///   (latency) or lets latency grow (admission); the LED reflects the
    ///   physical limit, not the policy response.
    /// - **memory** — any always-in-circuit memory-safety rail at/above its
    ///   limit (create-queue depth, exporter saturation, in-flight pipeline
    ///   bytes, resident-memory watermark) — the same rails
    ///   [`admission_shed`](Self::admission_shed) enforces.
    ///
    /// Cheap: relaxed atomic loads plus the same cheap partition sums the
    /// admission gate already uses; no engine round-trip. Called from the ~1 Hz
    /// monitor tick, never the hot path.
    pub(crate) fn ceiling_state(&self) -> (bool, bool) {
        let processing = self.processing.load(Ordering::Relaxed);
        let mut throughput = self.backpressure.should_shed(processing);
        let backlog_limit = self.backlog_cap.load(Ordering::Relaxed);
        if !throughput && backlog_limit > 0 && self.sla_mode.get().sheds_for_latency() {
            // Mirror the latency-preservation rail in `admission_shed`: it trips on
            // either the runnable (task-job) backlog or the create-side backlog
            // (the term that bites for fast create->complete workloads).
            throughput = self.runnable_backlog.load(Ordering::Relaxed) >= backlog_limit
                || self.engine.pending_create_queue() >= backlog_limit;
        }

        let mut memory = false;
        let cq_limit = self.admission_max_create_queue;
        if cq_limit > 0 {
            memory = self.engine.pending_create_queue() >= cq_limit;
        }
        if !memory {
            memory = self.engine.exporter_all_saturated();
        }
        if !memory && self.pipeline_bytes_watermark > 0 {
            memory = self.pipeline_bytes.load(Ordering::Relaxed) >= self.pipeline_bytes_watermark;
        }
        if !memory && self.mem_watermark_bytes > 0 {
            memory = self.mem_pressure_bytes.load(Ordering::Relaxed) >= self.mem_watermark_bytes;
        }
        (throughput, memory)
    }

    /// Graded **create-acceptance headroom** occupancy in `[0, CREATE_OCCUPANCY_SCALE]`
    /// — `0` = full headroom, higher = tighter. It is the MAX over the configured
    /// create-admission rails of how full each is *right now*:
    /// * create-processing concurrency vs the AIMD watermark
    ///   ([`Backpressure::current_limit`](crate::backpressure::Backpressure::current_limit)),
    /// * standing create-queue depth vs [`Self::admission_max_create_queue`],
    /// * resident memory vs [`Self::mem_watermark_bytes`].
    ///
    /// It deliberately does **not** include the resident active-instance backlog:
    /// a recovered node holding a deep but idle backlog (workers not yet draining
    /// it) has full create-acceptance capacity and must stay eligible for new
    /// creates. The hard shed rails (memory watermark / create-queue / submission
    /// pressure, via [`create_should_shed`](Self::create_should_shed)) remain the
    /// OOM/liveness backstop; this index only steers *below* the shed line. Cheap:
    /// a few relaxed atomic loads plus one create-queue read.
    pub(crate) fn create_occupancy_index(&self) -> i64 {
        // occ(num, den) = fraction of `den` used, scaled to CREATE_OCCUPANCY_SCALE,
        // clamped to the scale (the >=1.0 case is the shed rails' job, not the
        // graded steer). A rail with no configured limit contributes nothing.
        fn occ(num: u64, den: u64) -> i64 {
            if den == 0 {
                return 0;
            }
            let n = num.min(den) as u128;
            ((n * CREATE_OCCUPANCY_SCALE as u128) / den as u128) as i64
        }
        let mut load = 0i64;
        // Create-processing concurrency vs the adaptive/fixed watermark.
        if let Some(limit) = self.backpressure.current_limit() {
            let processing = self.processing.load(Ordering::Relaxed) as u64;
            load = load.max(occ(processing, limit as u64));
        }
        // Standing create-queue depth vs its memory-safety cap.
        if self.admission_max_create_queue > 0 {
            let create_queue = self.engine.pending_create_queue() as u64;
            load = load.max(occ(create_queue, self.admission_max_create_queue as u64));
        }
        // Resident memory vs the watermark.
        if self.mem_watermark_bytes > 0 {
            let used = self.mem_pressure_bytes.load(Ordering::Relaxed);
            load = load.max(occ(used, self.mem_watermark_bytes));
        }
        load
    }

    /// This node's composite create-load index for load-aware placement (ADR
    /// 0014, `PlacementMode::Balanced`). A shedding node reports
    /// [`SHED_LOAD`](crate::placement::SHED_LOAD) (weight 0 — never placed on);
    /// otherwise it reports its [`create_occupancy_index`](Self::create_occupancy_index)
    /// — create-acceptance headroom, *not* accumulated active backlog — so
    /// weighted placement steers creates toward nodes with real intake capacity
    /// (a recovered node draining a deep backlog stays eligible). Cheap: a couple
    /// of relaxed atomic loads plus the admission-gate checks, no engine
    /// round-trip beyond the create-queue gauge.
    pub(crate) fn create_load_index(&self) -> i64 {
        if self.create_should_shed().is_some() {
            crate::placement::SHED_LOAD
        } else {
            self.create_occupancy_index()
        }
    }

    /// Record a peer's gossiped create-load index (ADR 0014 pressure gossip).
    /// Overwrites the previous value for that node and restamps its freshness;
    /// weighted placement reads the latest within [`peer_pressure_ttl`]. Only
    /// invoked in `PlacementMode::Balanced`.
    pub(crate) fn record_peer_pressure(&self, node: u32, load: i64) {
        if let Ok(mut map) = self.peer_pressure.lock() {
            map.insert(node, (load, std::time::Instant::now()));
        }
    }

    /// The latest create-load index gossiped by peer `node`, or `None` if that
    /// peer has not reported *recently* (never reported, or its last report is
    /// older than [`peer_pressure_ttl`]) — both cases treated by placement as
    /// full headroom. The TTL is what lets a rejoining node re-enter create
    /// placement within a couple of gossip intervals: its stale pre-death value
    /// (often SHED) expires instead of steering all creates away forever.
    pub(crate) fn peer_load(&self, node: u32) -> Option<i64> {
        let ttl = peer_pressure_ttl();
        self.peer_pressure
            .lock()
            .ok()
            .and_then(|m| m.get(&node).copied())
            .filter(|(_, at)| at.elapsed() < ttl)
            .map(|(load, _)| load)
    }

    /// Diagnostic snapshot of this node's gossiped peer-pressure view: for each
    /// peer we have ever heard from, `(node, load, age_ms, expired)` where
    /// `expired` is true once the reading is older than [`peer_pressure_ttl`]
    /// (i.e. placement now treats it as full headroom). Read-only; used by the
    /// `/debug/peers` route to confirm whether a rejoining peer is being pinned
    /// out of create placement by a stale SHED reading under sustained load.
    pub(crate) fn peer_pressure_snapshot(&self) -> Vec<(u32, i64, u128, bool)> {
        let ttl = peer_pressure_ttl();
        let mut rows: Vec<(u32, i64, u128, bool)> = self
            .peer_pressure
            .lock()
            .map(|m| {
                m.iter()
                    .map(|(node, (load, at))| {
                        let age = at.elapsed();
                        (*node, *load, age.as_millis(), age >= ttl)
                    })
                    .collect()
            })
            .unwrap_or_default();
        rows.sort_by_key(|(node, ..)| *node);
        rows
    }

    /// Test-only: record a peer's create-load index stamped at an explicit
    /// instant, so freshness/TTL expiry can be exercised deterministically
    /// without sleeping or racing the wall clock.
    #[cfg(test)]
    pub(crate) fn record_peer_pressure_at(&self, node: u32, load: i64, at: std::time::Instant) {
        if let Ok(mut map) = self.peer_pressure.lock() {
            map.insert(node, (load, at));
        }
    }

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
    /// falcon dispatcher (permit-storing, so the wake survives an
    /// in-flight dispatch pass).
    pub(crate) fn signal_jobs_available(&self) {
        self.jobs_available.notify_waiters();
        self.dispatch_wake.notify_one();
    }

    /// Handle to the permit-storing dispatcher wake, so the Falcon protocol can
    /// wake the dispatcher after a new subscription or credit grant without the
    /// signal being lost mid-pass.
    pub(crate) fn dispatch_wake_handle(&self) -> Arc<tokio::sync::Notify> {
        self.dispatch_wake.clone()
    }

    /// The live per-job-type active dispatch width the push dispatcher caps its
    /// per-pass subscriber fan-out at; `0` = no cap. In [`WorkerConcurrency::Auto`]
    /// mode the engine thread's worker governor retunes this each latency window
    /// to hold the fan-out just left of activation swamping completions. Read with
    /// a relaxed load on the dispatch path.
    pub(crate) fn active_worker_cap(&self) -> usize {
        self.active_worker_cap.load(Ordering::Relaxed)
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
    let creation_time =
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(incident.created_at_ms as i64)
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

    let start_date =
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(instance.start_date_ms as i64)
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
        instance
            .business_id
            .clone()
            .map(types::Nullable::Present)
            .unwrap_or(types::Nullable::Null),
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
fn process_instance_state_enum(state: ProcessInstanceState) -> models::ProcessInstanceStateEnum {
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
fn user_task_state_enum(state: nanobpmn_engine_core::UserTaskState) -> models::UserTaskStateEnum {
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
        match d
            .as_deref()
            .map(|s| s.parse::<chrono::DateTime<chrono::Utc>>())
        {
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
/// the Falcon protocol when a message is fanned out to cluster peers. Preserves
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

/// Body prefix a receiving peer uses on the 503 it returns when it
/// placement-sheds a forwarded create (ADR 0014 `protect`). The ingress node
/// recognises it to distinguish a rerouteable placement shed from an ordinary
/// 503, and reroutes the create to another owner instead of failing the client.
const PLACEMENT_SHED_MARKER: &str = "PLACEMENT_SHED:";

/// Classification of a forwarded-create attempt (ADR 0014). Lets the ingress
/// reroute loop distinguish a rerouteable **placement shed** / unreachable owner
/// from a terminal success, client rejection, or peer error.
enum ForwardCreateOutcome {
    /// The peer created the instance and returned its result.
    Created(models::CreateProcessInstanceResult),
    /// The peer rejected the request as invalid (400) — deterministic, no reroute.
    Reject400(String),
    /// The peer self-protected and shed this create for rerouting.
    Shed(String),
    /// The peer could not be reached (link/transport error) — reroute to another.
    Unreachable(String),
    /// Any other peer error; surfaced to the client as a 5xx.
    Error(u16, String),
}

/// Cheap O(n) byte-size proxy for a create's engine-`Value` variable map, used to
/// meter in-flight create payloads for byte-aware admission control. Sums each
/// key's length plus its value's [`Value::approx_bytes`].
fn engine_vars_bytes(vars: &std::collections::HashMap<String, Value>) -> u64 {
    vars.iter()
        .map(|(k, v)| k.len() as u64 + v.approx_bytes())
        .sum()
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
        serde_json::Value::Array(items) => Value::List(items.iter().map(json_to_value).collect()),
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
        Value::List(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
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
/// Read-on-demand diagnostic dump of every hosted Raft partition's replication
/// indices, for root-causing load-induced commit stalls. Deliberately reads the
/// live openraft metrics watch (no sampler) so it's accurate even when the
/// engine actor is wedged. Compare across nodes to classify a stall:
///   - leader `last_log` flat            → propose/append stall (nothing enters the log)
///   - follower `last_log` lags leader's → replication stall (followers not appending)
///   - `applied` lags `last_log`         → state-machine/apply (engine-actor) stall
fn raft_debug_body(reg: &crate::raft::RaftRegistry) -> Response {
    use std::fmt::Write as _;
    let mut body = String::new();
    for part in reg.all() {
        let m = part.raft.metrics().borrow().clone();
        let last_log = m.last_log_index.map(|i| i as i128).unwrap_or(-1);
        let applied = m.last_applied.map(|l| l.index as i128).unwrap_or(-1);
        let snapshot = m.snapshot.map(|l| l.index as i128).unwrap_or(-1);
        let purged = m.purged.map(|l| l.index as i128).unwrap_or(-1);
        let leader = m.current_leader.map(|n| n as i128).unwrap_or(-1);
        let _ = writeln!(
            body,
            "partition={} node={} state={:?} term={} leader={} last_log={} applied={} apply_lag={} snapshot={} purged={}",
            part.partition_id,
            part.node_id,
            m.state,
            m.current_term,
            leader,
            last_log,
            applied,
            last_log - applied,
            snapshot,
            purged,
        );
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(body))
        .expect("raft debug response builds")
}

/// Diagnostic dump of this node's gossiped peer-pressure (create-load) view, plus
/// its own locally-computed create-load index. Read-only. Answers "is a rejoining
/// healthy peer being pinned out of create placement by a stale/expired SHED
/// reading?": each row is `peer=<node> load=<idx> age_ms=<ms> expired=<bool>
/// weight=<placement_weight>`. A peer with `expired=true` is treated as full
/// headroom by placement; a fresh `load=SHED` (weight 0) peer is steered away.
fn peers_debug_body(server: &ServerImpl) -> Response {
    use std::fmt::Write as _;
    let me = server.engine.topology().node_id;
    let my_load = server.create_load_index();
    let ttl_ms = peer_pressure_ttl().as_millis();
    let mut body = String::new();
    let _ = writeln!(
        body,
        "node={me} self_load={my_load} self_weight={} peer_pressure_ttl_ms={ttl_ms}",
        crate::placement::placement_weight(my_load),
    );
    for (node, load, age_ms, expired) in server.peer_pressure_snapshot() {
        // Placement uses `peer_load` (TTL-filtered): an expired reading counts as
        // full headroom, so its effective weight is the full-headroom weight.
        let effective = if expired {
            crate::placement::placement_weight(0)
        } else {
            crate::placement::placement_weight(load)
        };
        let _ = writeln!(
            body,
            "peer={node} load={load} age_ms={age_ms} expired={expired} effective_weight={effective}"
        );
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(body))
        .expect("peers debug response builds")
}

/// Diagnostic dump of non-terminal instance / job state, per partition this node
/// hosts (owned or replica). Characterizes "wedged" instances that remain
/// `Active` after load drains: for each partition it reports state histograms
/// (instances, jobs), the activated/activatable index sizes, active incidents,
/// and a sample of `Active` instances with their parked element ids and each
/// owning job's state / activation / lease-due flag. Read-only.
async fn instances_debug_body(server: &ServerImpl) -> Response {
    use std::collections::BTreeMap;
    use std::fmt::Write as _;

    let now = now_millis();
    let node_id = server.engine.topology().node_id as u64;
    let num_parts = server.engine.topology().num_partitions;

    let mut body = String::new();
    let _ = writeln!(body, "node={node_id} now={now} partitions={num_parts}");

    for p in 0..num_parts {
        let owned = server.engine.local_for_partition(p).is_some();
        let led = server
            .raft_registry()
            .get(p)
            .is_some_and(|part| part.raft.metrics().borrow().current_leader == Some(node_id));
        let Some(handle) = server.engine_handle_for(p) else {
            continue;
        };
        let summary = handle
            .with(move |journal| {
                let s = journal.engine().state();
                let mut inst_states: BTreeMap<&'static str, usize> = BTreeMap::new();
                let mut active_no_job = 0usize; // Active instance, no owning job at all
                let mut active_with_incident = 0usize;
                let mut sample = String::new();
                let mut sampled = 0usize;
                for inst in s.instances.values() {
                    let label = match inst.state {
                        ProcessInstanceState::Active => "Active",
                        ProcessInstanceState::Completed => "Completed",
                        ProcessInstanceState::Terminated => "Terminated",
                    };
                    *inst_states.entry(label).or_default() += 1;
                    if !matches!(inst.state, ProcessInstanceState::Active) {
                        continue;
                    }
                    if !inst.incidents.is_empty() {
                        active_with_incident += 1;
                    }
                    // Jobs owned by this instance and their live state.
                    let job_keys = s.jobs_by_instance.get(&inst.key);
                    let has_job = job_keys.is_some_and(|js| !js.is_empty());
                    if !has_job {
                        active_no_job += 1;
                    }
                    if sampled < 20 {
                        sampled += 1;
                        let elems: Vec<String> =
                            inst.active.values().map(|e| e.to_string()).collect();
                        let jobs: Vec<String> = job_keys
                            .map(|js| {
                                js.iter()
                                    .filter_map(|k| s.jobs.get(k))
                                    .map(|j| {
                                        let due = j
                                            .deadline
                                            .map(|d| if d <= now { "DUE" } else { "future" })
                                            .unwrap_or("none");
                                        format!(
                                            "{:?}(act={} dl={:?} due={} worker={:?})",
                                            j.state, j.activated, j.deadline, due, j.worker
                                        )
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        let _ = writeln!(
                            sample,
                            "    inst={} elems={:?} incidents={} jobs={:?}",
                            inst.key,
                            elems,
                            inst.incidents.len(),
                            jobs
                        );
                    }
                }
                let mut job_states: BTreeMap<&'static str, usize> = BTreeMap::new();
                for j in s.jobs.values() {
                    let label = match j.state {
                        nanobpmn_engine_core::JobState::Created => "Created",
                        nanobpmn_engine_core::JobState::Activated => "Activated",
                        nanobpmn_engine_core::JobState::Failed => "Failed",
                        nanobpmn_engine_core::JobState::Errored => "Errored",
                        nanobpmn_engine_core::JobState::Canceled => "Canceled",
                        nanobpmn_engine_core::JobState::Completed => "Completed",
                    };
                    *job_states.entry(label).or_default() += 1;
                }
                let activated_due = s
                    .activated_jobs
                    .iter()
                    .filter(|k| {
                        s.jobs
                            .get(*k)
                            .and_then(|j| j.deadline)
                            .is_some_and(|d| d <= now)
                    })
                    .count();
                let activatable: usize = s.activatable_jobs.values().map(|set| set.len()).sum();
                format!(
                    "  instances={inst_states:?} jobs={job_states:?} \
                     activated_idx={} activated_due={activated_due} activatable_idx={activatable} \
                     active_no_job={active_no_job} active_with_incident={active_with_incident} \
                     active_incidents_total={}\n{sample}",
                    s.activated_jobs.len(),
                    s.incidents
                        .values()
                        .filter(|i| matches!(i.state, IncidentState::Active))
                        .count(),
                )
            })
            .await;
        let _ = writeln!(body, "partition={p} owned={owned} led={led}\n{summary}");
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(body))
        .expect("instances debug response builds")
}

async fn metrics_handler() -> Response {
    let mut body = metrics::gather();
    // jemalloc memory decomposition: resident (≈RSS) vs allocated (true live
    // heap). A large resident−allocated gap = allocator-retained dirty pages
    // (reclaimable), not live data — the key signal for diagnosing RSS balloons.
    if let Some(m) = memory::stats() {
        use std::fmt::Write as _;
        let _ = write!(
            body,
            "# HELP nanobpm_jemalloc_bytes jemalloc memory accounting by kind.\n\
             # TYPE nanobpm_jemalloc_bytes gauge\n\
             nanobpm_jemalloc_bytes{{kind=\"allocated\"}} {}\n\
             nanobpm_jemalloc_bytes{{kind=\"active\"}} {}\n\
             nanobpm_jemalloc_bytes{{kind=\"resident\"}} {}\n\
             nanobpm_jemalloc_bytes{{kind=\"mapped\"}} {}\n\
             nanobpm_jemalloc_bytes{{kind=\"retained\"}} {}\n",
            m.allocated, m.active, m.resident, m.mapped, m.retained,
        );
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(
            http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )
        .body(Body::from(body))
        .expect("metrics response builds")
}

/// `GET /v2/system/memory` — current resident memory of the engine process, as
/// jemalloc accounts it (the most meaningful figure: `ps`/RSS under-reports on
/// macOS). Used by the Throughput Explorer demo to report peak memory; cheap to
/// poll. Returns `{ residentBytes }` (omitted/null if jemalloc isn't available).
async fn system_memory_handler() -> Response {
    let bytes = memory::resident_bytes();
    let body = serde_json::json!({ "residentBytes": bytes }).to_string();
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .expect("memory response builds")
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

/// Per-attempt deadline for forwarding a (non-`await_completion`) create to a
/// partition leader, env `NANOBPMN_WRITE_FORWARD_TIMEOUT_MS` (default 2500ms).
/// Deliberately a little above the Raft election ceiling (election_timeout_max
/// default 1000ms) so a create racing a leader failure can still land on the
/// incumbent if it survives, yet fails fast — instead of pinning the producer
/// for the 30s general peer timeout — when the leader is truly gone, so the
/// write retries on the newly elected leader.
fn write_forward_timeout() -> std::time::Duration {
    let ms = std::env::var("NANOBPMN_WRITE_FORWARD_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(2500);
    std::time::Duration::from_millis(ms)
}

/// Total budget for retrying a forwarded create across leader re-resolution,
/// env `NANOBPMN_WRITE_FORWARD_RETRY_MS` (default 5000ms). Spans at least one
/// election so a create in flight when a leader fails is re-pointed at the new
/// leader rather than shed; exhausting it returns a retryable 503.
fn write_forward_retry_budget() -> std::time::Duration {
    let ms = std::env::var("NANOBPMN_WRITE_FORWARD_RETRY_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(5000);
    std::time::Duration::from_millis(ms)
}

/// Interval between create-load gossip broadcasts (ADR 0014 `balanced`), env
/// `NANOBPMN_CREATE_PLACEMENT_GOSSIP_MS` (default 500ms). Short enough that a
/// node's load hint stays fresh under a fast-moving workload, long enough that
/// the fire-and-forget fan-out is negligible overhead.
fn placement_gossip_interval() -> std::time::Duration {
    let ms = std::env::var("NANOBPMN_CREATE_PLACEMENT_GOSSIP_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(500);
    std::time::Duration::from_millis(ms)
}

/// Number of gossip intervals a peer-pressure reading stays authoritative.
const PEER_PRESSURE_TTL_INTERVALS: u32 = 4;

/// How long a gossiped peer-pressure reading stays authoritative before weighted
/// placement reverts that peer to full-headroom. Derived from the gossip interval
/// so it scales with the configured cadence: several intervals of tolerance so a
/// single skipped tick (the gossip loop uses `MissedTickBehavior::Skip` under
/// load) does not expire a live peer, yet a peer that truly goes silent — killed,
/// restarting, or its gossip frame starved on a saturated link — clears within a
/// second or two. This is the freshness guard that lets a rejoining node
/// re-enter create placement promptly instead of being pinned out by the SHED
/// value it gossiped just before it died.
fn peer_pressure_ttl() -> std::time::Duration {
    placement_gossip_interval().saturating_mul(PEER_PRESSURE_TTL_INTERVALS)
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

/// An [`axum::serve::Listener`] wrapper that disables Nagle (`TCP_NODELAY`) on
/// every accepted connection. The gateway's WebSocket surfaces — the SDK command
/// stream and the inter-node peer/Raft lane — exchange small, latency-sensitive
/// request/response frames; with Nagle + delayed-ACK each round-trip can stall
/// ~40 ms, which collapses Raft commit and job-stream throughput. The frames are
/// explicitly length-delimited, so there is nothing to gain from TCP-level
/// coalescing. (The client/dialling side sets the same option in [`crate::peer`].)
struct NoDelayListener(tokio::net::TcpListener);

impl axum::serve::Listener for NoDelayListener {
    type Io = tokio::net::TcpStream;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.0.accept().await {
                Ok((stream, addr)) => {
                    let _ = stream.set_nodelay(true);
                    return (stream, addr);
                }
                // Mirror axum's own TcpListener accept: a transient accept error
                // (e.g. fd exhaustion) is retried after a short backoff rather
                // than tearing down the server.
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(1)).await,
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.0.local_addr()
    }
}

/// Reported binary name for `--version` / `--help`.
const GATEWAY_NAME: &str = "nanobpm-gateway-rest-server";

/// Intercept `--version`/`-V` and `--help`/`-h` before the async runtime spins
/// up the server. Unknown args are ignored — the server is configured via
/// environment variables, not positional CLI arguments.
fn handle_cli_flags() {
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "-V" | "--version" => {
                println!("{GATEWAY_NAME} {}", env!("NANOBPM_VERSION"));
                std::process::exit(0);
            }
            "-h" | "--help" => {
                print!("{}", gateway_usage());
                std::process::exit(0);
            }
            _ => {}
        }
    }
}

/// Help text. The gateway is a long-running server configured entirely through
/// environment variables; this lists the most common ones and points at the
/// docs for the full set.
fn gateway_usage() -> String {
    format!(
        "{name} {ver}\n\
         Nano BPM gateway — a BPMN orchestration engine with a REST API.\n\
         Advanced Research Prototype.\n\n\
         USAGE:\n  \
         {name} [OPTIONS]\n\n\
         Starts the gateway server. Configuration is via environment variables.\n\n\
         OPTIONS:\n  \
         -h, --help       Print this help\n  \
         -V, --version    Print version\n\n\
         COMMON ENVIRONMENT VARIABLES:\n  \
         PORT                  TCP port to listen on (default 8080)\n  \
         NANOBPMN_DATA_DIR     Directory for the journal + read-model database\n  \
         NANOBPMN_PARTITIONS   Partition count for the engine\n  \
         NANOBPMN_IDLE_PURGE_MS  Idle memory-purge interval ms (0 = off)\n  \
         NANOBPMN_MEM_WATERMARK_MB  Shed creates above this resident MiB (off, or a\n                        \
         fraction of detected RAM when unset; NANOBPMN_MEM_WATERMARK=off disables)\n  \
         NANOBPMN_PIPELINE_BYTES_MB  Shed creates above this in-flight create-payload\n                        \
         MiB (adaptive fraction of RAM when unset; NANOBPMN_PIPELINE_BYTES=off disables)\n  \
         NANOBPMN_VAR_SPILL_FLOOR_MB  Adaptive-hybrid var-spill burst budget: spill a\n                        \
         GROWING backlog above this resident MiB (adaptive ~10%% of RAM when unset)\n  \
         NANOBPMN_VAR_SPILL_RESERVE_MB  Also spill when live system-available memory\n                        \
         drops below this MiB (adaptive ~12%% of RAM when unset; 0 disables)\n",
        name = GATEWAY_NAME,
        ver = env!("NANOBPM_VERSION"),
    )
}

#[tokio::main]
async fn main() {
    handle_cli_flags();
    // Structured logging. Route through a non-blocking (lossy) writer so a burst
    // of log lines can never stall the emitting task on synchronous stdout/journald
    // I/O — critical on the Raft replication hot path, where openraft can emit
    // thousands of WARN/ERROR lines under transient load and a blocked write would
    // push AppendEntries past its RPC deadline, wedging commits. The filter honors
    // RUST_LOG and otherwise defaults to a quiet-but-useful level (openraft capped
    // at WARN so per-RPC replication chatter stays off the hot path). The returned
    // guard must outlive the program so buffered lines flush on shutdown.
    let (log_writer, _log_guard) = tracing_appender::non_blocking(std::io::stdout());
    let log_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,openraft=warn"));
    tracing_subscriber::fmt()
        .with_env_filter(log_filter)
        .with_writer(log_writer)
        .init();
    install_panic_hook();
    // Enable jemalloc's background page-decay thread where supported (Linux), so
    // freed memory returns to the OS automatically; on macOS the idle-purge tick
    // forces it instead.
    memory::enable_background_thread();
    // Resolve where the journal (durable event log) and read-model database live.
    let (journal_path, db_path) = resolve_data_paths();

    let server = match journal_path {
        Some(journal_path) => {
            // Persistent run: the read model is a per-partition sharded projection
            // of the journal(s), so reconcile every shard against the log before
            // serving. Each branch opens its shards (file-backed under `db_path`)
            // and yields the assembled `ReadModel`.
            let partitions = partition_count_from_env();
            let topology = cluster::Topology::from_env(partitions as u64);
            let mut seg_shared: Option<Arc<seglog::SegShared>> = None;
            // Bounded-disk state for the single-node multi-partition segmented
            // path: the shared seal state plus the global partition count that
            // sizes the per-partition compaction watermark vector.
            let mut multi_seg: Option<(Arc<seglog::SegShared>, u64)> = None;
            // Lean-snapshot mode: an authoritative durable variable store sited
            // next to the journal. Opened BEFORE recovery so `recover_multi` can
            // install each instance's variables from it between `from_snapshot`
            // and the tail replay (the lean snapshot itself carries no variables).
            // Only the segmented multi-partition paths honour it; other boot paths
            // keep full snapshots and leave this `None`.
            let varstore: Option<Arc<varstore::VarStore>> = if varstore::lean_snapshot_enabled()
                && seglog::segmented_enabled()
            {
                let dir = journal_path
                    .parent()
                    .map(std::path::Path::to_path_buf)
                    .unwrap_or_else(|| std::path::PathBuf::from("."));
                let path = dir.join("var-store.sqlite");
                match varstore::VarStore::open(Some(&path)) {
                    Ok(vs) => {
                        tracing::info!(
                            "lean snapshots enabled: authoritative var store at {} ({} instance(s))",
                            path.display(),
                            vs.len()
                        );
                        Some(Arc::new(vs))
                    }
                    Err(e) => {
                        tracing::error!(
                            "failed to open var store at {}: {e}; falling back to full snapshots",
                            path.display()
                        );
                        None
                    }
                }
            } else {
                None
            };
            let (mut journals, recovered, store) = if topology.is_single_node() && partitions == 1 {
                // Single partition: the bounded-disk segmented journal (snapshot
                // + segment rotation + compaction) unless explicitly disabled, in
                // which case the legacy single-file journal is used. Either way we
                // warm-start by replaying only the events the read store has not
                // yet projected.
                let (read_model, shards) = open_sharded_read_model(db_path.as_deref(), &[0], true);
                let shard = Arc::clone(&shards[0].1);
                if seglog::segmented_enabled() {
                    let dir = journal_path
                        .parent()
                        .map(std::path::Path::to_path_buf)
                        .unwrap_or_else(|| std::path::PathBuf::from("."));
                    let (journal, recovery) = Journal::open_segmented(&dir).unwrap_or_else(|e| {
                        panic!("failed to open segmented journal at {}: {e}", dir.display())
                    });
                    seg_shared = Some(Arc::clone(&recovery.shared));
                    // Read-store catch-up over absolute event positions. The
                    // surviving events span `[first_index, total_events)`; events
                    // compacted before `first_index` were already projected (the
                    // exporter watermark gates compaction), so the store is never
                    // behind the compacted prefix.
                    let mut pos = shard.exported_position() as u64;
                    if pos > recovery.total_events {
                        // Store ahead of the log (truncated/corrupt): rebuild from
                        // whatever survives.
                        shard.reset().expect("reset read store");
                        pos = recovery.first_index;
                    }
                    let skip = pos.saturating_sub(recovery.first_index) as usize;
                    if skip < recovery.events.len() {
                        let refs: Vec<&Event> = recovery.events[skip..].iter().collect();
                        shard
                            .export(&refs)
                            .expect("catch up read model from segmented journal");
                    }
                    let recovered = !journal.is_fresh();
                    (vec![journal], recovered, read_model)
                } else {
                    let events = Journal::read_events(&journal_path).unwrap_or_else(|e| {
                        panic!("failed to read journal {}: {e}", journal_path.display())
                    });
                    let mut pos = shard.exported_position();
                    if pos > events.len() {
                        // The store is ahead of the log (truncated/corrupt journal):
                        // rebuild from scratch.
                        shard.reset().expect("reset read store");
                        pos = 0;
                    }
                    if pos < events.len() {
                        let refs: Vec<&Event> = events[pos..].iter().collect();
                        shard
                            .export(&refs)
                            .expect("catch up read model from journal");
                    }
                    let journal = Journal::open(&journal_path).unwrap_or_else(|e| {
                        panic!("failed to open journal {}: {e}", journal_path.display())
                    });
                    let recovered = !journal.is_fresh();
                    (vec![journal], recovered, read_model)
                }
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
                if seglog::segmented_enabled() {
                    // Bounded-disk segmented path: per-partition snapshots +
                    // segment rotation + compaction gated by the exporter
                    // watermark AND every partition's snapshot. The writer feeds
                    // the read model in log order, so `exported_position` stays a
                    // true global prefix and boot catch-up is incremental (no
                    // reset).
                    let dir = journal_path
                        .parent()
                        .map(std::path::Path::to_path_buf)
                        .unwrap_or_else(|| std::path::PathBuf::from("."));
                    let owned: Vec<u64> = (0..partitions as u64).collect();
                    let (shared, recovery) = SharedWriter::open_segmented(
                        &dir,
                        &owned,
                        partitions as u64,
                        varstore.as_deref(),
                    )
                    .unwrap_or_else(|e| {
                        panic!(
                            "failed to open segmented multi-partition journal at {}: {e}",
                            dir.display()
                        )
                    });
                    multi_seg = Some((Arc::clone(&recovery.shared), partitions as u64));
                    let (read_model, shards) =
                        open_sharded_read_model(db_path.as_deref(), &owned, false);
                    catch_up_read_model(&shards, &recovery);
                    let recovered = !recovery.fresh;
                    let mut engines: std::collections::HashMap<u64, nanobpmn_engine_core::Engine> =
                        recovery.engines.into_iter().collect();
                    let journals: Vec<Journal> = (0..partitions as u64)
                        .map(|p| {
                            let engine = engines
                                .remove(&p)
                                .unwrap_or_else(|| nanobpmn_engine_core::Engine::with_partition(p));
                            Journal::from_engine_shared(p, engine, !recovered, &shared)
                        })
                        .collect();
                    (journals, recovered, read_model)
                } else {
                    // Legacy single-file multi-partition path: the read model is
                    // rebuilt from scratch because the runtime exporter interleaves
                    // partitions in projection order (not the log's commit order),
                    // so the single `exported_position` cursor can't track it
                    // incrementally. Projection is order-independent across the
                    // (independent) partitions, and the boot-time deployment on
                    // partition 0 is written first, so a full replay in log order
                    // is correct.
                    let events = Journal::read_events(&journal_path).unwrap_or_else(|e| {
                        panic!("failed to read journal {}: {e}", journal_path.display())
                    });
                    let owned: Vec<u64> = (0..partitions as u64).collect();
                    let (read_model, shards) =
                        open_sharded_read_model(db_path.as_deref(), &owned, false);
                    rebuild_read_model_legacy(&shards, &events, partitions);

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
                    (journals, recovered, read_model)
                }
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
                if seglog::segmented_enabled() {
                    // Bounded-disk segmented clustered path: like the single-node
                    // multi-partition path, but this node owns only a SUBSET of
                    // partitions. The shared writer tags every write with its
                    // GLOBAL partition id, so recovery demultiplexes by the
                    // partition that produced the write — which is how a durable
                    // replicated `ProcessDeployed` (journaled under this node's
                    // first-owned partition but keyed to the deployment partition)
                    // is routed back and re-installed across the node's partitions.
                    let dir = journal_path
                        .parent()
                        .map(std::path::Path::to_path_buf)
                        .unwrap_or_else(|| std::path::PathBuf::from("."));
                    let (shared, recovery) = SharedWriter::open_segmented(
                        &dir,
                        &owned,
                        partitions as u64,
                        varstore.as_deref(),
                    )
                    .unwrap_or_else(|e| {
                        panic!(
                            "failed to open segmented clustered journal at {}: {e}",
                            dir.display()
                        )
                    });
                    multi_seg = Some((Arc::clone(&recovery.shared), partitions as u64));
                    let (read_model, shards) =
                        open_sharded_read_model(db_path.as_deref(), &owned, false);
                    catch_up_read_model(&shards, &recovery);
                    let recovered = !recovery.fresh;
                    let mut engines: std::collections::HashMap<u64, nanobpmn_engine_core::Engine> =
                        recovery.engines.into_iter().collect();
                    let journals: Vec<Journal> = owned
                        .iter()
                        .map(|&p| {
                            let engine = engines
                                .remove(&p)
                                .unwrap_or_else(|| nanobpmn_engine_core::Engine::with_partition(p));
                            Journal::from_engine_shared(p, engine, !recovered, &shared)
                        })
                        .collect();
                    (journals, recovered, read_model)
                } else {
                    // Legacy clustered path: this node owns only a SUBSET of the
                    // cluster's partitions (`partition_id % num_nodes == node_id`).
                    // Its journal file therefore holds only its own partitions'
                    // events; rebuild its read model from them and open one engine
                    // actor per owned partition (each keyed by its GLOBAL partition
                    // id so keys stay globally unique across the cluster).
                    // Partitions owned by peers are reached by forwarding (handled
                    // by the routing seam), not replayed here.
                    let events = Journal::read_events(&journal_path).unwrap_or_else(|e| {
                        panic!("failed to read journal {}: {e}", journal_path.display())
                    });
                    let (read_model, shards) =
                        open_sharded_read_model(db_path.as_deref(), &owned, false);
                    // The read model demuxes exactly like the engines below:
                    // `ProcessDeployed` (partition-agnostic, keyed to the deployment
                    // partition a peer may not own) into every owned shard; other
                    // events to `partition_of(key)`'s shard.
                    rebuild_read_model_legacy(&shards, &events, partitions);
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
                    (journals, recovered, read_model)
                }
            };

            // Lean-snapshot wiring: make the durable var store authoritative for
            // every owned partition's journal (enables engine dirty-var tracking,
            // routes spill write-through / rehydrate / terminal-forget through the
            // store). Only the segmented multi-partition boot paths set both
            // `varstore` and `multi_seg`; the other paths keep full snapshots.
            if let Some(vs) = &varstore
                && multi_seg.is_some()
            {
                for journal in &mut journals {
                    journal.set_varstore(Arc::clone(vs));
                }
            }

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

            // Bounded-disk maintenance: periodically snapshot the engine, seal the
            // active segment at the snapshot boundary, and compact (delete) sealed
            // segments the snapshot AND the read model no longer need. Only the
            // segmented single-partition path arms this.
            if let Some(shared) = seg_shared.clone()
                && let Some(interval) = seglog::snapshot_interval_from_env()
                && let Some(handle) = server.engine.all().first().cloned()
            {
                let store = server.store.clone();
                tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(interval);
                    // Skip the immediate first tick.
                    ticker.tick().await;
                    loop {
                        ticker.tick().await;
                        let snapped = handle.with(|journal| journal.snapshot_and_rotate()).await;
                        let Some((snap, covered)) = snapped else {
                            continue;
                        };
                        if let Err(e) = seglog::write_snapshot(&shared.dir, snap, covered) {
                            tracing::warn!("journal snapshot write failed: {e}");
                            continue;
                        }
                        // Never delete a segment the read model has not yet
                        // projected: bound compaction by the exporter watermark
                        // as well as the snapshot.
                        let watermark = covered.min(store.exported_position() as u64);
                        let removed = seglog::compact(&shared, watermark);
                        if removed > 0 {
                            tracing::debug!(
                                "journal compaction removed {removed} sealed segment(s) (watermark {watermark})"
                            );
                        }
                    }
                });
                tracing::info!(
                    "segmented journal enabled (snapshot/compaction every {:?})",
                    interval
                );
            }

            // Bounded-disk maintenance for the single-node multi-partition
            // segmented path: each tick snapshots EVERY owned partition (sealing
            // the shared active segment at each snapshot boundary), writes one
            // combined snapshot, then compacts sealed segments below BOTH the
            // exporter watermark and every partition's snapshot boundary.
            if let Some((shared, num_partitions)) = multi_seg.clone()
                && let Some(interval) = seglog::snapshot_interval_from_env()
            {
                let handles: Vec<DeepthiHandle> = server.engine.all().to_vec();
                let store = server.store.clone();
                let lean = varstore.is_some();
                let varstore = varstore.clone();
                tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(interval);
                    // Skip the immediate first tick.
                    ticker.tick().await;
                    // Periodic truncating WAL checkpoint of the durable var-store:
                    // SQLite's auto-checkpoint never shrinks the `-wal` file, so a
                    // sustained write load grows it unbounded (~843 MB observed).
                    // Truncate it off this maintenance thread on its own cadence.
                    let wal_interval = varstore::wal_checkpoint_interval();
                    let mut last_wal_truncate = std::time::Instant::now();
                    loop {
                        ticker.tick().await;
                        // Snapshot each owned partition; a `None` (writer gone)
                        // aborts this tick so compaction never runs on a partial
                        // watermark set.
                        let mut entries: Vec<(u64, u64, nanobpmn_engine_core::EngineSnapshot)> =
                            Vec::with_capacity(handles.len());
                        let mut covered = vec![0u64; num_partitions as usize];
                        // Per-partition durable-variable position. `u64::MAX` means
                        // "no variable constraint" (non-lean, or a partition this
                        // node does not own); a lean checkpoint sets it to the
                        // covered boundary once this partition's variables are
                        // durable, gating compaction on the var store too.
                        let mut var_position = vec![u64::MAX; num_partitions as usize];
                        let mut ok = true;
                        for handle in &handles {
                            let pid = handle.with(|journal| journal.partition_id()).await;
                            if let Some(vs) = &varstore {
                                // Lean path: drain this partition's variable delta,
                                // capture a control-only snapshot, seal — all on the
                                // engine thread — then persist the delta to the
                                // authoritative store off-thread BEFORE the snapshot
                                // is written, so the store never lags the lean
                                // snapshot that omits those variables.
                                match handle
                                    .with(|journal| journal.snapshot_and_rotate_lean())
                                    .await
                                {
                                    Some((snap, covered_p, upserts, forgets)) => {
                                        let ups: Vec<(nanobpmn_engine_core::Key, &_)> =
                                            upserts.iter().map(|(k, v)| (*k, v.as_ref())).collect();
                                        if let Err(e) =
                                            vs.checkpoint(pid, covered_p, &ups, &forgets)
                                        {
                                            tracing::warn!(
                                                "var store checkpoint failed (partition {pid}): {e}"
                                            );
                                            ok = false;
                                            break;
                                        }
                                        if (pid as usize) < covered.len() {
                                            covered[pid as usize] = covered_p;
                                            var_position[pid as usize] = covered_p;
                                        }
                                        entries.push((pid, covered_p, snap));
                                    }
                                    None => {
                                        ok = false;
                                        break;
                                    }
                                }
                            } else {
                                match handle.with(|journal| journal.snapshot_and_rotate()).await {
                                    Some((snap, covered_p)) => {
                                        if (pid as usize) < covered.len() {
                                            covered[pid as usize] = covered_p;
                                        }
                                        entries.push((pid, covered_p, snap));
                                    }
                                    None => {
                                        ok = false;
                                        break;
                                    }
                                }
                            }
                        }
                        if !ok {
                            continue;
                        }
                        if let Err(e) = seglog::write_multi_snapshot(&shared.dir, entries) {
                            tracing::warn!("multi-partition snapshot write failed: {e}");
                            continue;
                        }
                        let exported = store.exported_watermarks(num_partitions as usize);
                        // Compaction may delete a sealed segment only once every
                        // partition has projected it (read model) AND, in lean mode,
                        // its variables are durable in the var store — hence the
                        // per-partition min of the two watermarks.
                        let gate: Vec<u64> = exported
                            .iter()
                            .zip(var_position.iter())
                            .map(|(e, v)| (*e).min(*v))
                            .collect();
                        let removed = seglog::compact_multi(&shared, &covered, &gate);
                        if removed > 0 {
                            tracing::debug!(
                                "multi-partition journal compaction removed {removed} sealed segment(s) (exported {exported:?})"
                            );
                        }

                        // Bound the durable var-store WAL: truncate it back to zero
                        // on its own cadence so a sustained write load can't grow it
                        // without limit. A `SQLITE_BUSY` from a concurrent reader is
                        // harmless — the next tick retries.
                        if let (Some(vs), Some(wal_every)) = (&varstore, wal_interval)
                            && last_wal_truncate.elapsed() >= wal_every
                        {
                            if let Err(e) = vs.checkpoint_wal() {
                                tracing::debug!("var-store WAL checkpoint(TRUNCATE) skipped: {e}");
                            }
                            last_wal_truncate = std::time::Instant::now();
                        }
                    }
                });
                tracing::info!(
                    "segmented multi-partition journal enabled (snapshot/compaction every {:?}, {num_partitions} partitions{})",
                    interval,
                    if lean { ", lean snapshots" } else { "" }
                );
            }

            server
        }
        None => {
            tracing::info!("no journal configured; running in-memory (state is not persisted)");
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
            let owned: Vec<u64> = journals.iter().map(|j| j.partition_id()).collect();
            let single = owned.len() == 1 && owned[0] == 0;
            let (store, _shards) = open_sharded_read_model(db_path.as_deref(), &owned, single);
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

    // Memory-pressure sampler: refresh the cached resident-memory gauge the
    // admission path reads, so `admission_shed` never advances jemalloc's stats
    // epoch per create. Captured here (before `server` is moved into the router)
    // and only armed when a watermark is set; ~250 ms catches a large-payload
    // burst's climb at negligible overhead.
    if server.mem_watermark_bytes > 0 || server.pipeline_bytes_watermark > 0 {
        let pressure = server.mem_pressure_bytes.clone();
        let pipeline = server.pipeline_bytes.clone();
        let engine = server.engine.clone();
        let sample_resident = server.mem_watermark_bytes > 0;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut tick: u64 = 0;
            loop {
                interval.tick().await;
                tick = tick.wrapping_add(1);
                if sample_resident && let Some(bytes) = memory::resident_bytes() {
                    pressure.store(bytes as u64, Ordering::Relaxed);
                }
                // Publish the in-flight create-payload gauge for observability
                // (off the hot path); the gate itself reads the atomic directly.
                metrics::set_pipeline_bytes(pipeline.load(Ordering::Relaxed));
                // Publish the resident export-backlog gauge (in-flight pipeline
                // attribution for the RSS balloon); a cheap relaxed sum.
                metrics::set_exporter_queue_bytes(engine.exporter_queued_bytes_total());
                // Publish the resident variable-payload gauge (~1 Hz): an O(N)
                // per-partition scan at Low priority, decisive attribution of the
                // burst balloon (resident variables vs in-flight pipeline copies).
                if tick.is_multiple_of(4) {
                    metrics::set_resident_var_bytes(engine.resident_variable_bytes_total().await);
                }
            }
        });
    }

    // The unified bidirectional Falcon protocol (WebSocket) shares the engine via a
    // clone of `server` and a registry of connections; a single dispatcher pushes
    // jobs and the existing periodic tick reclaims expired leases.
    let cs_registry = falcon::Registry::new();
    falcon::spawn_dispatcher(server.clone(), cs_registry.clone());
    let monitor_registry = cs_registry.clone();
    let monitor_server = server.clone();
    let cs_router = falcon::router(server.clone(), cs_registry);

    // Clustered partition-0 owner: push the seeded/recovered deployment
    // definitions to every peer so the whole cluster can instantiate them,
    // retrying until each peer (which may still be booting) acknowledges.
    // No-op for a single-node cluster.
    server.spawn_seed_broadcast();

    // Env-gated (NANOBPMN_RAFT): bring up this node's per-partition Raft groups
    // over the Falcon protocol and form the ones it leads. No-op by default.
    server.spawn_raft_bootstrap();

    // Leader-durable auto-recovery (ADR 0003): when the replication tier is
    // leader-durable, each group has a single voter, so openraft cannot elect on
    // leader loss — this supervisor app-promotes a deterministic survivor. No-op
    // in every other configuration (single node / RF=1 / quorum mode).
    server.spawn_leader_durable_recovery();

    // Create-load gossip (ADR 0014): in `balanced` placement mode, periodically
    // broadcast this node's composite create-load index to peers so their
    // weighted placement steers creates toward nodes with headroom. No-op unless
    // NANOBPMN_CREATE_PLACEMENT=balanced with real peers.
    server.spawn_pressure_gossip();

    // Self-contained single-node distribution: build the embedded web console
    // router (SPA + /console/api/*) before `server` is moved into the generated
    // router. Feature-gated; the default gateway build never includes it and the
    // non-console path keeps consuming `server` directly (byte-identical).
    #[cfg(feature = "console")]
    let console_router = crate::console::router(server.clone());
    // Build the generated (spec-first) console router while `server` is still
    // available — it is moved into the gateway router below.
    #[cfg(feature = "console")]
    let gen_console_router =
        nanobpm_console_api::server::new::<ServerImpl, ServerImpl, ()>(server.clone());

    // Captured for the /debug/raft diagnostic route before `server` is moved into
    // the generated router below.
    let raft_reg_dbg = server.raft_registry().clone();
    // Captured for the /debug/instances diagnostic route (non-terminal instance /
    // job state breakdown per led partition — used to characterize wedged
    // instances that never reach a terminal state after load drains).
    let dbg_server = server.clone();
    // Captured for the /debug/peers diagnostic route (this node's gossiped
    // peer-pressure view + own create-load index — used to confirm whether a
    // rejoining peer is pinned out of create placement by a stale SHED reading).
    let peers_dbg = server.clone();

    // Raft partition-liveness supervisor. An openraft core can enter `Shutdown`
    // (e.g. on an unrecoverable storage error) and then silently stop applying,
    // stranding that partition's share of instances/jobs — the mechanism behind
    // the RF>1 completion-freeze. This makes the failure LOUD: it polls every
    // partition on a slow cadence, publishes the `nanobpm_raft_partition_shutdown`
    // gauge, and logs a rate-limited ERROR (once per partition per shutdown edge)
    // so a dead partition can never again fail silently.
    {
        let reg = server.raft_registry().clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
            let mut alarmed: std::collections::HashSet<u64> = std::collections::HashSet::new();
            loop {
                ticker.tick().await;
                if reg.is_empty() {
                    continue;
                }
                for part in reg.all() {
                    let pid = part.partition_id;
                    let down = part.is_shutdown();
                    metrics::set_raft_partition_shutdown(pid, down);
                    if down {
                        if alarmed.insert(pid) {
                            let m = part.raft.metrics().borrow().clone();
                            let applied = m.last_applied.map(|l| l.index as i128).unwrap_or(-1);
                            let last_log = m.last_log_index.map(|i| i as i128).unwrap_or(-1);
                            tracing::error!(
                                partition = pid,
                                term = m.current_term,
                                last_log = last_log as i64,
                                applied = applied as i64,
                                "raft partition core is SHUTDOWN and no longer applying; \
                                 its instances/jobs are stranded (RF>1 completion-freeze) — \
                                 the node must be restarted (clean) to recover this partition"
                            );
                        }
                    } else {
                        alarmed.remove(&pid);
                    }
                }
            }
        });
    }

    // Read-model reconciliation sweep. Retires orphaned Active rows (creates whose
    // terminal event was never projected — the leadership-churn residual) against
    // authoritative engine state on a slow cadence, keeping the active-backlog
    // gauge honest for a long-running cluster that never restarts. Boot already
    // reconciles once from the replayed journals; this catches orphans that form
    // afterwards. Gated on quiescence inside `reconcile_orphans_once`.
    {
        let recon_server = server.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(60));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                recon_server.reconcile_orphans_once().await;
            }
        });
    }

    let mut app = nanobpm_gateway_rest::server::new::<ServerImpl, ServerImpl, (), ()>(server)
        .merge(cs_router)
        .route("/metrics", axum::routing::get(metrics_handler))
        .route(
            "/debug/raft",
            axum::routing::get(move || {
                let reg = raft_reg_dbg.clone();
                async move { raft_debug_body(&reg) }
            }),
        )
        .route(
            "/debug/instances",
            axum::routing::get(move || {
                let srv = dbg_server.clone();
                async move { instances_debug_body(&srv).await }
            }),
        )
        .route(
            "/debug/peers",
            axum::routing::get(move || {
                let srv = peers_dbg.clone();
                async move { peers_debug_body(&srv) }
            }),
        )
        .route(
            "/debug/heap",
            axum::routing::get(|| async { crate::memory::stats_print() }),
        )
        .route(
            "/debug/heap/prof",
            axum::routing::get(|| async {
                let path = format!(
                    "{}/nano-heap-{}.prof",
                    std::env::var("HOME").unwrap_or_else(|_| "/tmp".into()),
                    std::process::id()
                );
                if crate::memory::prof_dump(&path) {
                    format!("dumped {path}\n")
                } else {
                    "prof unavailable: build with --features heapprof\n".to_string()
                }
            }),
        )
        .route(
            "/v2/system/memory",
            axum::routing::get(system_memory_handler),
        );

    #[cfg(feature = "console")]
    {
        // The spec-first typed `/console/api/*` routes are served by the
        // generated rust-axum router; the reduced hand-written `console_router`
        // keeps only the streaming/binary/proxy/static routes excluded from the
        // spec. axum merges the two: they only share the
        // `/console/api/projects/{name}/file` path, on disjoint methods (GET is
        // hand-wired; PUT/POST/DELETE are generated), so there is no collision.
        app = app.merge(console_router).merge(gen_console_router);
        tracing::info!("console enabled: web UI at /console, API under /console/api");
    }

    if debug_rest_enabled() {
        app = app.layer(axum::middleware::from_fn(log_rest));
        tracing::info!("DEBUG_REST enabled: logging every REST request and response");
    }

    // CORS: the gateway is often addressed cross-origin (Nano IDE Deno GUI on
    // its own port, vite dev at :5173, a hosted console). The REST /v2 surface
    // is a *dev-target* API — the same-origin restriction browsers apply by
    // default is more friction than protection here (the alternative is asking
    // every consumer to run its own proxy). Permissive by default; disable
    // with NANOBPM_CORS=off if you're deploying to an untrusted origin.
    if std::env::var("NANOBPM_CORS")
        .map(|v| v.to_ascii_lowercase())
        .ok()
        .as_deref()
        != Some("off")
    {
        use tower_http::cors::{Any, CorsLayer};
        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any)
            .expose_headers(Any);
        app = app.layer(cors);
        tracing::info!(
            "CORS enabled on all routes (Access-Control-Allow-Origin: *). Set NANOBPM_CORS=off to disable."
        );
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
                //
                // Raft path: tick-driven mutations MINT state (a fired timer may
                // create a job) and must keep the same deterministic log order +
                // single `now` as client writes, so they go through the leader's
                // Raft log (followers apply in lockstep). Non-Raft uses the direct
                // local path below, byte-identical to the pre-cluster behaviour.
                let outcomes: Vec<(bool, Vec<Event>)> = if !tick_server.raft.is_empty() {
                    let led = tick_server.led_partitions();
                    // Reclaim hot RAM on the partitions this node FOLLOWS (hosts a
                    // replica of but does not currently lead). The leader spills its
                    // led partitions inside `tick_partition_via_raft`'s precheck, but
                    // a follower never proposes, so its replica engine would never run
                    // the spill gate and would pin the entire replicated working set
                    // resident — the leader/follower memory imbalance. Spill is a
                    // pure-local memory op (no key mint, no events, no proposal), so it
                    // is safe on any replica; a later replicated command rehydrates a
                    // cold instance on demand. Fan out concurrently, off the led path.
                    let led_set: std::collections::HashSet<u64> = led.iter().copied().collect();
                    let followed: Vec<u64> = (0..tick_server.engine.topology().num_partitions)
                        .filter(|p| !led_set.contains(p))
                        .collect();
                    futures_util::future::join_all(followed.into_iter().filter_map(|p| {
                        tick_server.engine_handle_for(p).map(|handle| async move {
                            handle
                                .with(|journal| {
                                    journal.maybe_var_spill_pressure();
                                    journal.maybe_cold_spill();
                                })
                                .await;
                        })
                    }))
                    .await;
                    futures_util::future::join_all(
                        led.iter()
                            .map(|&p| tick_server.tick_partition_via_raft(p, now, multi_partition)),
                    )
                    .await
                } else {
                    futures_util::future::join_all(engine.all().iter().map(|handle| {
                        handle.with(move |journal| {
                            let (fired, _commit) = journal.trigger_timers(now);
                            let expired = journal.expire_jobs(now);
                            // Shed to disk if hot RAM is over the high-water mark
                            // (cheap no-op below it / when unset): active-backlog
                            // variables first, then whole dormant instances.
                            journal.maybe_var_spill_pressure();
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
                    .await
                };
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
                // Best-effort lease-digest pass (digest mode only): recover leases
                // on a freshly-promoted leader and broadcast current leases to
                // followers. Empty / single-node / non-digest => zero work.
                if tick_server.lease_digest && !tick_server.raft.is_empty() {
                    tick_server.run_lease_digest(now).await;
                }
            }
        });
    }

    // Monitor tick: publishes the operational "LED" metrics an operator watches
    // on a dashboard — the capacity-ceiling gauges (ADR 0013's compressor/limiter
    // LEDs: throughput vs memory) and the per-job-type worker-provisioning /
    // starvation hints. Always on (independent of the mem-pressure sampler's
    // watermark gating), ~1 Hz, off the hot path: a couple of relaxed atomic
    // reads plus cheap `Low`-priority partition walks + a roster snapshot. Never
    // touches the create/complete critical path.
    {
        let monitor_server = monitor_server;
        let monitor_registry = monitor_registry;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(1000));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Rising-edge state for the ceiling-hit counters, and the set of job
            // types published last tick so a type that drained to nothing is
            // reset to 0 instead of leaving a stale non-zero series.
            let mut throughput_lit = false;
            let mut memory_lit = false;
            let mut seen_job_types: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            // Drain-stall guard supervisor state (options 3+4). The state machine
            // owns the edge/hysteresis counters; here we track the deltas it needs:
            // the completion count (rate = drain throughput) and the active backlog
            // level (the servo's pressure band), plus the wall instant to turn the
            // completion delta into a per-second rate even if a tick is skipped
            // under load.
            let mut drain_sm = crate::drain_guard::DrainStateMachine::new(
                crate::drain_guard::DrainGuardCfg::from_env(),
            );
            let mut prev_completions = monitor_server.drain_guard().completions();
            let mut prev_drain_instant = std::time::Instant::now();
            let mut drain_meter_lit = false;
            let mut drain_halt_lit = false;
            // Adaptive recovery admission throttle: paces intake while this node is a
            // failover incumbent / returning owner with a saturating Raft-log disk,
            // so the disk stays under its fsync knee without deferring any fsync
            // (durability-preserving). Driven by the windowed Raft-log fsync latency.
            let mut recovery_throttle = crate::recovery_throttle::RecoveryThrottle::new(
                crate::recovery_throttle::RecoveryThrottleCfg::from_env(),
            );
            let (mut prev_raft_fsync_sum, mut prev_raft_fsync_count) =
                crate::metrics::raft_fsync_sum_count();
            let mut recovery_throttle_engaged = false;
            // Adaptive submission-window governor: a TCP-style congestion window on
            // producer create credits, AIMD on create-accept latency. Holds a stable
            // reduced-capacity intake window while a peer is down (so the cluster
            // settles instead of limit-cycling) and reopens to the ceiling on
            // recovery. Inert at steady state (window == ceiling == no-op cap).
            let mut submission_governor = crate::submission_governor::SubmissionGovernor::new(
                crate::submission_governor::SubmissionGovernorCfg::from_env(),
            );
            let (mut prev_create_accept_sum, mut prev_create_accept_count) =
                crate::metrics::create_accept_sum_count();
            let mut submission_governor_engaged = false;
            // Catch-up hold: keep the recovery throttle engaged while this node is
            // still feeding a rejoined peer's post-hand-off learner catch-up (which
            // saturates the Raft disk after leadership displacement has cleared).
            // The lag threshold is auto-derived from the retained-log window so
            // operators don't have to guess it: a peer counts as "in bulk catch-up"
            // once it lags the log head by >5% of that window (floored), or is mid
            // snapshot install (matched 0 ⇒ lag ≈ head). `NANOBPMN_RECOVERY_CATCHUP_LAG`
            // overrides the derived value.
            let catchup_lag_threshold: u64 = {
                let retain = std::env::var("NANOBPMN_RAFT_LAGGING_RETAIN")
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(400_000);
                let derived = (retain / 20).max(20_000);
                std::env::var("NANOBPMN_RECOVERY_CATCHUP_LAG")
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(derived)
            };
            // A lagging peer whose progress scalar (matched + snapshot bytes) has
            // not advanced for this long is treated as stalled/dead, not catching
            // up, so a wedged learner can't pin the throttle indefinitely (the
            // leader-durable model tolerates a lost async learner).
            let catchup_stall_grace = std::time::Duration::from_secs(5);
            let mut catchup_progress: std::collections::HashMap<
                (u64, u64),
                (u128, std::time::Instant),
            > = std::collections::HashMap::new();
            loop {
                interval.tick().await;

                let (throughput, memory) = monitor_server.ceiling_state();
                crate::metrics::set_ceiling_active("throughput", throughput, throughput_lit);
                crate::metrics::set_ceiling_active("memory", memory, memory_lit);
                throughput_lit = throughput;
                memory_lit = memory;

                // Publish the raw input signals + configured thresholds behind the
                // ceiling LED, so a dashboard can see pressure climb toward each shed
                // point (and how much headroom is left) rather than only a 0/1 LED.
                crate::metrics::set_admission_signals(
                    monitor_server.engine.pending_create_queue() as i64,
                    monitor_server.active_backlog(),
                    monitor_server.mem_pressure_bytes.load(Ordering::Relaxed) as i64,
                    monitor_server.runnable_backlog.load(Ordering::Relaxed) as i64,
                );
                crate::metrics::set_admission_limit(
                    "backlog",
                    monitor_server.backlog_cap.load(Ordering::Relaxed) as i64,
                );
                // In Auto mode, publish the governor's bounds + live latency signal
                // so a dashboard (and the shed message) can explain where the cap
                // sits and why it moved there, rather than only the bare cap value.
                if let Some(gov) = &monitor_server.backlog_gov {
                    crate::metrics::set_backlog_governor("floor", gov.floor as i64);
                    crate::metrics::set_backlog_governor("ceiling", gov.ceiling as i64);
                    crate::metrics::set_backlog_governor(
                        "baseline_latency_us",
                        gov.obs.baseline_us.load(Ordering::Relaxed) as i64,
                    );
                    crate::metrics::set_backlog_governor(
                        "window_latency_us",
                        gov.obs.window_avg_us.load(Ordering::Relaxed) as i64,
                    );
                }
                crate::metrics::set_admission_limit(
                    "create_queue",
                    monitor_server.admission_max_create_queue as i64,
                );
                crate::metrics::set_admission_limit(
                    "pipeline_bytes",
                    monitor_server.pipeline_bytes_watermark as i64,
                );
                crate::metrics::set_admission_limit(
                    "mem_watermark",
                    monitor_server.mem_watermark_bytes as i64,
                );
                crate::metrics::set_active_worker_target(
                    monitor_server.active_worker_cap.load(Ordering::Relaxed) as i64,
                );

                let activatable = monitor_server.engine.activatable_job_counts().await;
                // Refresh the runnable (task-job) backlog the admission gate and
                // backlog governor read: the total count of task jobs the engine
                // holds (Created + Activated), summed across owned partitions.
                // Parked instances create no jobs, so this excludes them by
                // construction — and it counts leased-but-uncompleted jobs, so a
                // worker-starved backlog that has drained into the activated set is
                // still seen (activatable alone would miss it). The governor can
                // pull the cap toward the knee without ever shedding a legitimately
                // parked population.
                let runnable = monitor_server.engine.job_backlog().await;
                monitor_server
                    .runnable_backlog
                    .store(runnable, Ordering::Relaxed);
                let workers = monitor_registry.workers_per_type();
                let mut current: std::collections::HashSet<String> =
                    std::collections::HashSet::with_capacity(activatable.len() + workers.len());
                for job_type in activatable.keys().chain(workers.keys()) {
                    current.insert(job_type.clone());
                }
                for job_type in &current {
                    let waiting = *activatable.get(job_type).unwrap_or(&0) as i64;
                    let workers = *workers.get(job_type).unwrap_or(&0) as i64;
                    crate::metrics::set_job_type_provisioning(job_type, waiting, workers);
                }
                // Zero out job types that disappeared this tick so their gauges
                // don't linger at a stale value.
                for job_type in seen_job_types.difference(&current) {
                    crate::metrics::set_job_type_provisioning(job_type, 0, 0);
                }
                seen_job_types = current;

                // Engine-actor (deepthi) heartbeat per owned partition: the
                // dead/wedged/idle discriminator for the sustained-load
                // completion-freeze.
                let mut any_actor_alive = false;
                for handle in monitor_server.engine.all() {
                    let s = handle.stats();
                    let alive = s.alive.load(std::sync::atomic::Ordering::Relaxed);
                    any_actor_alive |= alive;
                    crate::metrics::set_actor_stats(
                        s.partition,
                        alive,
                        s.jobs.load(std::sync::atomic::Ordering::Relaxed),
                        s.current_job_ms(),
                        s.hi_depth.load(std::sync::atomic::Ordering::Relaxed),
                        s.lo_depth.load(std::sync::atomic::Ordering::Relaxed),
                    );
                }

                // Drain-stall admission guard (options 3+4): derive the drain
                // throughput (completions/s) and read the active backlog, fold them
                // into the guard state machine, publish the servo/valve decision,
                // and (when not metering) re-pin the completion-paced token bucket
                // full so entering the pressure band starts with a fresh burst.
                {
                    let now = std::time::Instant::now();
                    let dt = now.duration_since(prev_drain_instant).as_secs_f64();
                    prev_drain_instant = now;
                    let completions_now = monitor_server.drain_guard().completions();
                    let completed = completions_now.saturating_sub(prev_completions);
                    prev_completions = completions_now;
                    let completes_per_sec = if dt > 0.0 { completed as f64 / dt } else { 0.0 };
                    let backlog = monitor_server.active_backlog();

                    // Adaptive recovery throttle: fold the windowed Raft-log fsync
                    // latency (the failover disk-saturation signal) into the AIMD
                    // controller, gated on whether this node is actually in a recovery
                    // window, and publish the resulting admission cap (0 = no clamp).
                    // Recomputed before the setpoint so the min below sees it.
                    {
                        let (sum_now, count_now) = crate::metrics::raft_fsync_sum_count();
                        let d_count = count_now.saturating_sub(prev_raft_fsync_count);
                        let d_sum = (sum_now - prev_raft_fsync_sum).max(0.0);
                        prev_raft_fsync_sum = sum_now;
                        prev_raft_fsync_count = count_now;
                        // Window-mean fsync latency in µs (0 when no fsyncs this window).
                        let fsync_avg_us = if d_count > 0 {
                            d_sum / d_count as f64 * 1_000_000.0
                        } else {
                            0.0
                        };
                        let displaced = monitor_server.recovery_fsync_load_active();
                        // Fold in the post-hand-off catch-up hold: stay engaged
                        // while any led partition is still feeding a peer that lags
                        // the log head beyond the derived threshold AND is still
                        // advancing (matched/snapshot-bytes progress within the
                        // stall grace). A stalled/dead peer is ignored so it can't
                        // pin the throttle.
                        let now_ct = std::time::Instant::now();
                        let observations = monitor_server.catchup_feed_observations();
                        let (catchup_active, catchup_max_lag) = catchup_hold_active(
                            &observations,
                            &mut catchup_progress,
                            catchup_lag_threshold,
                            catchup_stall_grace,
                            now_ct,
                        );
                        let recovering = displaced || catchup_active;
                        let cap = recovery_throttle.observe(fsync_avg_us, recovering);
                        monitor_server
                            .recovery_backlog_cap
                            .store(cap.unwrap_or(0), Ordering::Relaxed);
                        crate::metrics::set_admission_limit(
                            "backlog_recovery",
                            cap.map(|c| c as i64).unwrap_or(0),
                        );
                        // Log engagement transitions so the recovery window is legible
                        // in the ops log alongside the console recovery indicator.
                        let engaged = recovery_throttle.is_engaged();
                        if engaged != recovery_throttle_engaged {
                            recovery_throttle_engaged = engaged;
                            if engaged {
                                let cause = if displaced {
                                    "failover disk load"
                                } else {
                                    "peer catch-up disk load"
                                };
                                tracing::info!(
                                    fsync_avg_us,
                                    cap = cap.unwrap_or(0),
                                    catchup_max_lag,
                                    cause,
                                    "recovery admission throttle engaged"
                                );
                            } else {
                                tracing::info!(
                                    "recovery admission throttle released (recovery cleared)"
                                );
                            }
                        }
                    }

                    // Adaptive submission-window governor: fold the windowed
                    // create-accept latency (the closed-loop overpressure signal)
                    // into the AIMD congestion window and publish the resulting
                    // per-producer submission-window cap. Under capacity loss the
                    // window shrinks so fewer creates are admitted (the cluster
                    // holds a stable lower throughput instead of limit-cycling);
                    // on recovery it grows back to the ceiling (a no-op cap).
                    // Independent of the recovery throttle: it engages on latency
                    // alone, whether or not this node is a failover incumbent.
                    {
                        let (sum_now, count_now) = crate::metrics::create_accept_sum_count();
                        let d_count = count_now.saturating_sub(prev_create_accept_count);
                        let d_sum = (sum_now - prev_create_accept_sum).max(0.0);
                        prev_create_accept_sum = sum_now;
                        prev_create_accept_count = count_now;
                        // Window-mean create-accept latency in µs (0 when no creates
                        // this window -> governor holds).
                        let create_accept_avg_us = if d_count > 0 {
                            d_sum / d_count as f64 * 1_000_000.0
                        } else {
                            0.0
                        };
                        let window = submission_governor.observe(create_accept_avg_us, d_count);
                        monitor_server
                            .submission_window_cap
                            .store(window, Ordering::Relaxed);
                        crate::metrics::set_admission_limit("submission_window", window);
                        let engaged = submission_governor.is_engaged();
                        if engaged != submission_governor_engaged {
                            submission_governor_engaged = engaged;
                            if engaged {
                                tracing::info!(
                                    create_accept_avg_us,
                                    window,
                                    "submission-window governor engaged (create-accept latency \
                                     pressure; shrinking producer credit window)"
                                );
                            } else {
                                tracing::info!(
                                    window,
                                    "submission-window governor released (window restored)"
                                );
                            }
                        }
                    }

                    // Recompute the unified admission setpoint (latency ∧ memory ∧
                    // recovery) from the live signals, publish it, and band the servo
                    // against it so intake is paced to the *current* cap in both SLA
                    // modes.
                    let effective_cap = monitor_server.refresh_effective_backlog_cap(backlog);
                    crate::metrics::set_admission_limit("backlog_effective", effective_cap as i64);

                    let decision = drain_sm.observe(crate::drain_guard::DrainSample {
                        completes_per_sec,
                        backlog,
                        backlog_cap: effective_cap as i64,
                        actor_alive: any_actor_alive,
                    });
                    let guard = monitor_server.drain_guard();
                    guard.publish(decision.metering, decision.halted);
                    if !decision.metering {
                        // Below the pressure band: keep the bucket topped up so the
                        // servo never meters healthy load and always engages with a
                        // full burst.
                        guard.refill_full();
                    }
                    crate::metrics::set_drain_guard(
                        decision.metering,
                        decision.halted,
                        completes_per_sec,
                        guard.budget(),
                    );
                    // Log only on state transitions (edge-triggered) so a healthy
                    // server stays quiet and a wedge is a single, greppable event.
                    if decision.halted != drain_halt_lit {
                        if decision.halted {
                            tracing::warn!(
                                completes_per_sec,
                                backlog,
                                "drain-stall guard: HARD VALVE engaged — completion drain \
                                 stalled with a large backlog held; halting create admission \
                                 until drain recovers"
                            );
                        } else {
                            tracing::info!(
                                completes_per_sec,
                                backlog,
                                "drain-stall guard: hard valve released — drain recovered, \
                                 resuming create admission"
                            );
                        }
                        drain_halt_lit = decision.halted;
                    }
                    if decision.metering != drain_meter_lit {
                        if decision.metering {
                            tracing::info!(
                                completes_per_sec,
                                backlog,
                                budget = guard.budget(),
                                "drain-stall guard: servo engaged — backlog in the pressure \
                                 band; pacing create admission to the completion rate"
                            );
                        } else {
                            tracing::info!(
                                backlog,
                                "drain-stall guard: servo released — backlog back below the \
                                 pressure band"
                            );
                        }
                        drain_meter_lit = decision.metering;
                    }
                }

                // Engine-state cardinality per partition — the independent
                // variable the per-command cost (nanobpm_cmd_*) is regressed
                // against to localize the create/complete collapse's O(active)
                // term. Only sampled when NANOBPM_CMD_PROFILE is set, to avoid a
                // per-partition actor round-trip every tick otherwise.
                if crate::cmd_profile::enabled() {
                    for handle in monitor_server.engine.all() {
                        let partition = handle.stats().partition;
                        let (instances, jobs, activated) = handle
                            .with(|journal| {
                                let engine = journal.engine();
                                let state = engine.state();
                                (
                                    engine.resident_instance_count(),
                                    state.jobs.len(),
                                    state.activated_jobs.len(),
                                )
                            })
                            .await;
                        crate::metrics::set_engine_cardinality(
                            partition, instances, jobs, activated,
                        );
                    }
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
    let local_port = listener.local_addr().map(|a| a.port()).unwrap_or(port);
    println!("LISTENING_PORT={local_port}");
    let _ = std::io::Write::flush(&mut std::io::stdout());

    // Tell the console worker supervisor which port to dial for the command
    // stream when it spawns Deno worker subprocesses.
    #[cfg(feature = "console")]
    crate::console::workers::set_gateway_port(local_port);

    tracing::info!(
        "NanoBPM gateway REST stub server listening on http://{addr}{}",
        nanobpm_gateway_rest::BASE_PATH
    );

    // A friendly, copy-pasteable summary of where the human-facing surfaces live.
    // Console feature only, so the default gateway build's startup output is
    // unchanged. Printed to stdout so it shows up plainly when starting the
    // self-contained distribution.
    #[cfg(feature = "console")]
    {
        let base = format!("http://127.0.0.1:{local_port}");
        println!("\nNano BPM is up:");
        println!("  Landing page   {base}/");
        println!("  Why Nano BPM   {base}/features");
        println!("  Roadmap        {base}/optimization");
        println!("  Web console    {base}/console");
        println!("  API reference  {base}/swagger");
        println!("  REST API       {base}{}", nanobpm_gateway_rest::BASE_PATH);
        println!("  Metrics        {base}/metrics");
        println!();
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }

    axum::serve(NoDelayListener(listener), app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
}

/// Routes every panic through `tracing::error!` (thread name, location, payload,
/// backtrace) *before* delegating to the default hook, so a panic on a detached
/// worker thread — above all the **`nanobpmn-engine` single-writer actor**, whose
/// death silently freezes all completions on its partition — is captured in the
/// structured log (journald) instead of vanishing to a stderr nobody reads. This
/// is the other half of the [`deepthi::ActorStats::alive`] alarm: the gauge says
/// *that* the writer died; this hook says *why*.
fn install_panic_hook() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let thread = std::thread::current();
        let name = thread.name().unwrap_or("<unnamed>");
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let payload = info
            .payload()
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<non-string panic payload>".to_string());
        let backtrace = std::backtrace::Backtrace::force_capture();
        tracing::error!(
            thread = name,
            location = %location,
            payload = %payload,
            "PANIC on thread '{name}' at {location}: {payload}\n{backtrace}"
        );
        default(info);
    }));
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

/// The per-partition Raft log directory for this node, or `None` when no data
/// directory is configured (an in-memory deployment, where the Raft log stays in
/// [`MemLogStore`](crate::raft::MemLogStore) like the engine journal).
///
/// Derived from the same root as the engine journal/read-model (see
/// [`resolve_data_paths`]): each partition replica gets its own subdirectory
/// `<data_dir>/raft/p<partition>`, so a node hosting several replicas keeps their
/// durable logs separated. Returning `Some(dir)` routes the multi-voter path
/// through the crash-durable [`RaftLogStore`](crate::raft_logstore::RaftLogStore),
/// so a follower recovers its replicated log after a restart instead of losing
/// everything it had replicated.
fn raft_log_dir_for(partition: u64) -> Option<PathBuf> {
    let (journal, _) = resolve_data_paths();
    let root = journal?.parent()?.to_path_buf();
    Some(root.join("raft").join(format!("p{partition}")))
}

/// Whether the boot **purge-hole → snapshot fallback** is enabled
/// (`NANOBPMN_RAFT_PURGE_HOLE_FALLBACK`, default **on**). When a node rejoins
/// after being down longer than the leader's log-retention window, its on-disk
/// log can no longer replay `(last_applied, committed]` (the entries were purged),
/// so hosting the partition from that log trips openraft's defensive
/// `LogIndexNotFound` and the member fails to host. With the fallback on, such a
/// partition is instead hosted as a fresh receiver so the leader installs a
/// snapshot (see [`crate::raft::durable_log_has_purge_hole`], issue #111). Set to
/// `0`/`false`/`off` to restore the raw resume-on-disk behavior.
fn raft_purge_hole_fallback_enabled() -> bool {
    match std::env::var("NANOBPMN_RAFT_PURGE_HOLE_FALLBACK") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off"
        ),
        Err(_) => true,
    }
}

/// The durable log dir to host `partition` from, or `None` to host a fresh
/// receiver. Returns `None` when the on-disk log has a purge-hole
/// ([`crate::raft::durable_log_has_purge_hole`]) and the fallback is enabled, so a
/// node that outslept the retention window installs a snapshot from the leader
/// rather than failing to host the partition. Otherwise returns the on-disk dir.
fn purge_hole_aware_log_dir(partition: u64) -> Option<PathBuf> {
    let dir = raft_log_dir_for(partition)?;
    if raft_purge_hole_fallback_enabled() && crate::raft::durable_log_has_purge_hole(&dir) {
        tracing::warn!(
            partition,
            "raft: durable log has a purge-hole (committed beyond the local snapshot, reapply \
             range purged); hosting a fresh receiver to install a snapshot from the leader \
             (purge-hole → snapshot fallback, issue #111)"
        );
        return None;
    }
    Some(dir)
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
        Err(e) => Err(format!("failed to access data dir {}: {e}", dir.display())),
    }
}

#[cfg(test)]
mod clustered_startup_tests {
    use super::*;

    #[test]
    fn retry_until_ok_retries_transient_failures_then_returns_without_dropping() {
        // The read-model exporter must never drop a batch on a transient store
        // write failure (SQLite `database is locked`) — dropping desyncs the
        // projection from the durable journal irrecoverably. Assert the retry
        // combinator re-attempts until success, sleeps once per failure (never
        // after success), and surfaces the eventual value.
        use std::cell::Cell;
        let calls = Cell::new(0u32);
        let retries = Cell::new(0u32);
        let sleeps = Cell::new(0u32);

        let out = retry_until_ok(
            || {
                let n = calls.get() + 1;
                calls.set(n);
                // Fail twice (as a transient lock would), then succeed.
                if n < 3 {
                    Err("database is locked")
                } else {
                    Ok(42u32)
                }
            },
            |attempt, _e: &str| {
                retries.set(retries.get() + 1);
                assert_eq!(
                    attempt,
                    retries.get(),
                    "attempt count is 1-based and monotonic"
                );
            },
            |_backoff| sleeps.set(sleeps.get() + 1),
        );

        assert_eq!(
            out, 42,
            "the eventual Ok value is returned, batch not dropped"
        );
        assert_eq!(calls.get(), 3, "op invoked until it succeeded");
        assert_eq!(retries.get(), 2, "on_retry fired once per failure");
        assert_eq!(
            sleeps.get(),
            2,
            "backoff slept per failure, never after success"
        );
    }

    #[test]
    fn spill_default_scales_with_memory_limit() {
        // 65% of the limit, above the floor.
        let limit = 8 * 1024 * 1024 * 1024; // 8 GiB
        assert_eq!(spill_default_from_limit(limit), limit / 100 * 65);

        // 200 MiB -> 65% = 130 MiB, still above the 128 MiB floor.
        let small = 200 * 1024 * 1024;
        assert_eq!(spill_default_from_limit(small), small / 100 * 65);

        // 100 MiB -> 65% = 65 MiB, floored to 128 MiB but capped at the limit.
        let tiny = 100 * 1024 * 1024;
        assert_eq!(spill_default_from_limit(tiny), tiny);

        // Large host: watermark is a big fraction, so spill stays dormant under
        // a normal working set.
        let big = 64 * 1024 * 1024 * 1024; // 64 GiB
        assert_eq!(spill_default_from_limit(big), big / 100 * 65);
        assert!(spill_default_from_limit(big) > MIN_SPILL_HIGH_BYTES);
    }

    #[test]
    fn activation_policy_parses_explicit_values() {
        use ActivationPolicy::*;
        // Explicit values resolve the same regardless of replication mode.
        for mode in [ReplicationMode::Quorum, ReplicationMode::LeaderDurable] {
            assert_eq!(parse_activation_policy(Some("1"), mode), Always);
            assert_eq!(parse_activation_policy(Some("true"), mode), Always);
            assert_eq!(parse_activation_policy(Some("quorum"), mode), Always);
            assert_eq!(parse_activation_policy(Some(" ON "), mode), Always);
            assert_eq!(parse_activation_policy(Some("digest"), mode), Digest);
            assert_eq!(parse_activation_policy(Some("Auto"), mode), Auto);
            assert_eq!(parse_activation_policy(Some("0"), mode), LeaderLocal);
            assert_eq!(parse_activation_policy(Some("off"), mode), LeaderLocal);
            assert_eq!(
                parse_activation_policy(Some("leader-local"), mode),
                LeaderLocal
            );
            // Unrecognised -> conservative off switch (leader-local).
            assert_eq!(parse_activation_policy(Some("banana"), mode), LeaderLocal);
        }
    }

    #[test]
    fn activation_policy_default_is_mode_dependent() {
        use ActivationPolicy::*;
        // Unset: quorum auto-tunes; leader-durable stays leader-local.
        assert_eq!(parse_activation_policy(None, ReplicationMode::Quorum), Auto);
        assert_eq!(
            parse_activation_policy(None, ReplicationMode::LeaderDurable),
            LeaderLocal
        );
    }

    #[test]
    fn activation_policy_structural_flags() {
        use ActivationPolicy::*;
        // Digest broadcast runs under digest and auto (both can go leader-local).
        assert!(Digest.broadcasts_lease_digest());
        assert!(Auto.broadcasts_lease_digest());
        assert!(!Always.broadcasts_lease_digest());
        assert!(!LeaderLocal.broadcasts_lease_digest());
        // Followers must run lenient completion for every policy except Always.
        assert!(!Always.may_be_leader_local());
        assert!(LeaderLocal.may_be_leader_local());
        assert!(Digest.may_be_leader_local());
        assert!(Auto.may_be_leader_local());
    }

    #[test]
    fn spill_floor_scales_and_stays_below_low() {
        // 64 GiB host: floor ~10% (6.4 GiB) — well below the 65% pressure band and
        // its 7/8 low-water, so a growth reclaim to the floor bounds a burst far
        // under the OOM guard.
        let big = 64 * 1024 * 1024 * 1024;
        let high = spill_default_from_limit(big);
        let low = high / 8 * 7;
        let floor = spill_floor_from_limit(big, low);
        assert_eq!(floor, big / 100 * SPILL_FLOOR_FRACTION_PCT);
        assert!(floor < low, "floor must sit below the low-water band");
        assert!(floor >= MIN_SPILL_FLOOR_BYTES);

        // Tiny limit: the 10% fraction underflows the 256 MiB floor, but the
        // clamp to low/2 keeps the two bands from inverting.
        let tiny = 1024 * 1024 * 1024; // 1 GiB
        let low_t = spill_default_from_limit(tiny) / 8 * 7;
        let floor_t = spill_floor_from_limit(tiny, low_t);
        assert!(floor_t <= low_t / 2);
        assert!(floor_t >= 1);

        // Reserve is a smaller fraction of the limit (the free-memory guard).
        assert_eq!(
            spill_reserve_from_limit(big),
            big / 100 * SPILL_RESERVE_FRACTION_PCT
        );
    }

    #[test]
    fn mem_watermark_default_scales_with_memory_limit() {
        // 80% of the limit, above the floor.
        let limit = 8 * 1024 * 1024 * 1024; // 8 GiB
        assert_eq!(
            mem_watermark_default_from_limit(limit),
            limit / 100 * MEM_WATERMARK_FRACTION_PCT
        );

        // 64 GiB host: 80% = ~51 GiB, well above the floor and below the limit,
        // so a normal working set never sheds while a runaway burst does.
        let big = 64 * 1024 * 1024 * 1024;
        let wm = mem_watermark_default_from_limit(big);
        assert_eq!(wm, big / 100 * MEM_WATERMARK_FRACTION_PCT);
        assert!(wm > MIN_MEM_WATERMARK_BYTES);
        assert!(wm < big);

        // Tiny limit: 80% below the 256 MiB floor -> floored, but never above
        // the limit itself.
        let tiny = 128 * 1024 * 1024; // 128 MiB
        assert_eq!(mem_watermark_default_from_limit(tiny), tiny);
    }

    #[test]
    fn pipeline_bytes_watermark_default_scales_and_clamps() {
        // 64 GiB host: 8% = ~5.1 GiB, clamped to the 8 GiB ceiling? No — 8% of
        // 64 GiB is 5.12 GiB, within [512 MiB, 8 GiB], so it passes through.
        let big = 64 * 1024 * 1024 * 1024;
        let wm = pipeline_bytes_watermark_default_from_limit(big);
        assert_eq!(wm, big / 100 * PIPELINE_BYTES_FRACTION_PCT);
        assert!((MIN_PIPELINE_BYTES..=MAX_PIPELINE_BYTES).contains(&wm));
        // Far below the coarse OOM watermark (80% of RAM), so the two rails are
        // ordered: the precise byte gate bites well before the resident backstop.
        assert!(wm < mem_watermark_default_from_limit(big));

        // Huge host: 8% would exceed the 8 GiB ceiling -> clamped down.
        let huge = 256 * 1024 * 1024 * 1024; // 256 GiB
        assert_eq!(
            pipeline_bytes_watermark_default_from_limit(huge),
            MAX_PIPELINE_BYTES
        );

        // Small host: 8% below the 512 MiB floor -> floored up, but never above
        // the limit itself.
        let small = 2 * 1024 * 1024 * 1024; // 2 GiB, 8% = 160 MiB < floor
        let wsmall = pipeline_bytes_watermark_default_from_limit(small);
        assert_eq!(wsmall, MIN_PIPELINE_BYTES);
        assert!(wsmall <= small);

        // Tiny limit below the floor -> capped at the limit, never above it.
        let tiny = 128 * 1024 * 1024; // 128 MiB
        assert_eq!(pipeline_bytes_watermark_default_from_limit(tiny), tiny);
    }

    #[test]
    fn create_queue_cap_default_scales_and_clamps() {
        // 64 GiB host: byte budget 8% = ~5.1 GiB; /8 KiB per create is ~670k,
        // clamped down to the 500k ceiling.
        let big = 64 * 1024 * 1024 * 1024;
        assert_eq!(
            create_queue_cap_default_from_limit(big),
            MAX_CREATE_QUEUE_CAP
        );

        // Mid host where the derived count lands inside the band: 4 GiB -> byte
        // rail floored at MIN_PIPELINE_BYTES (512 MiB) -> 512 MiB / 8 KiB = 65_536,
        // within [20k, 500k].
        let mid = 4 * 1024 * 1024 * 1024;
        let cap = create_queue_cap_default_from_limit(mid);
        assert_eq!(cap, (MIN_PIPELINE_BYTES / NOMINAL_CREATE_BYTES) as usize);
        assert!((MIN_CREATE_QUEUE_CAP..=MAX_CREATE_QUEUE_CAP).contains(&cap));

        // Tiny limit -> byte budget = the whole tiny limit; /8 KiB is far below
        // the 20k floor -> clamped up to MIN_CREATE_QUEUE_CAP.
        let tiny = 64 * 1024 * 1024; // 64 MiB
        assert_eq!(
            create_queue_cap_default_from_limit(tiny),
            MIN_CREATE_QUEUE_CAP
        );
    }

    #[test]
    fn active_backlog_cap_default_scales_and_clamps() {
        // 64 GiB host: byte budget 8% = ~5.1 GiB; /16 KiB per active is ~335k,
        // within [50k, 1M].
        let big = 64 * 1024 * 1024 * 1024;
        let cap = active_backlog_cap_default_from_limit(big);
        assert_eq!(
            cap,
            (pipeline_bytes_watermark_default_from_limit(big) / NOMINAL_ACTIVE_BYTES) as usize
        );
        assert!((MIN_ACTIVE_BACKLOG_CAP..=MAX_ACTIVE_BACKLOG_CAP).contains(&cap));

        // Mid host: 4 GiB -> byte budget floored at MIN_PIPELINE_BYTES (512 MiB);
        // 512 MiB / 16 KiB = 32_768, below the 50k floor -> clamped up.
        let mid = 4 * 1024 * 1024 * 1024;
        assert_eq!(
            active_backlog_cap_default_from_limit(mid),
            ((MIN_PIPELINE_BYTES / NOMINAL_ACTIVE_BYTES) as usize)
                .clamp(MIN_ACTIVE_BACKLOG_CAP, MAX_ACTIVE_BACKLOG_CAP)
        );

        // Tiny limit -> budget is the whole tiny limit; /16 KiB is far below the
        // 50k floor -> clamped up to MIN_ACTIVE_BACKLOG_CAP.
        let tiny = 64 * 1024 * 1024; // 64 MiB
        assert_eq!(
            active_backlog_cap_default_from_limit(tiny),
            MIN_ACTIVE_BACKLOG_CAP
        );
    }

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
        build_server_in_memory(journals, topology)
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
        let owned: Vec<*const DeepthiHandle> =
            node0.engine.all().iter().map(|h| h as *const _).collect();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..16 {
            let h = node0.engine.for_create() as *const DeepthiHandle;
            assert!(
                owned.contains(&h),
                "for_create returned a handle this node does not own"
            );
            seen.insert(h);
        }
        // Over many calls it must exercise BOTH owned partitions (round-robin),
        // not collapse onto one.
        assert_eq!(
            seen.len(),
            2,
            "for_create should spread across both owned partitions"
        );
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
        assert!(
            p == 1 || p == 3,
            "instance must live on an owned partition, got {p}"
        );
    }

    #[tokio::test]
    async fn deploy_broadcast_over_the_wire_reaches_a_peer() {
        // Serve a real peer node (node 1) on an ephemeral falcon endpoint.
        let node1 = clustered_node(1);
        let registry = falcon::Registry::new();
        falcon::spawn_dispatcher(node1.clone(), registry.clone());
        let app = falcon::router(node1.clone(), registry);
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
        let node0 = build_server_in_memory(journals, topology);

        // The peer has no definition yet.
        assert!(
            node1
                .create_for_stream(Some("demo".into()), None, Default::default())
                .await
                .is_err()
        );

        // Broadcast node 0's seeded deployment to its peers over the Falcon protocol.
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

    /// A runtime SLA switch on one node propagates over the Falcon peer link to
    /// its peers, so the whole cluster converges on one admission policy.
    #[cfg(feature = "console")]
    #[tokio::test]
    async fn sla_mode_switch_propagates_over_the_wire_to_a_peer() {
        // node 1 serves its falcon endpoint; it starts in the default Latency mode.
        let node1 = clustered_node(1);
        assert_eq!(node1.sla_mode(), crate::backpressure::SlaMode::Latency);
        let node1_url = serve_node(&node1).await;

        // node 0 points at the served peer and is the node an operator switches.
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
        let node0 = build_server_in_memory(journals, topology);

        // The operator flips node 0 to Admission; it applies locally and fans out.
        node0
            .switch_sla_mode(crate::backpressure::SlaMode::Admission)
            .await;
        assert_eq!(node0.sla_mode(), crate::backpressure::SlaMode::Admission);

        // The switch rode the wire: the peer adopted Admission too. Poll briefly —
        // the fan-out is fire-and-forget over an async socket.
        let mut adopted = false;
        for _ in 0..50 {
            if node1.sla_mode() == crate::backpressure::SlaMode::Admission {
                adopted = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(adopted, "peer should adopt the cluster-wide SLA switch");
    }

    #[test]
    fn placement_avoiding_skips_tried_owners_then_falls_back_local() {
        // On node 0 (owns 2 of 4 partitions), the only remote owner is node 1.
        let node0 = clustered_node(0);
        // Avoiding node 1 (the sole remote owner) must fall back to a local
        // placement (`None`) rather than loop forever or return a tried owner.
        for _ in 0..8 {
            assert_eq!(node0.engine.next_create_placement_avoiding(&[1]), None);
        }
        // With nothing excluded, a full rotation still places on the remote owner.
        let mut saw_remote = false;
        for _ in 0..8 {
            if node0.engine.next_create_placement_avoiding(&[]) == Some(1) {
                saw_remote = true;
            }
        }
        assert!(
            saw_remote,
            "without exclusions the remote owner is still a placement target"
        );
    }

    #[test]
    fn create_load_index_reflects_intake_headroom_not_resident_backlog() {
        use super::CREATE_OCCUPANCY_SCALE;

        // A recovered node holding a HUGE resident active-instance backlog but
        // with an idle create pipeline and RAM below any watermark has full
        // create-acceptance capacity: its load index must be ~0 (eligible for
        // creates), NOT the raw backlog count that would freeze it out of
        // weighted placement.
        let mut node = clustered_node(0);
        node.inflight.store(5_000_000, Ordering::Relaxed);
        assert_eq!(
            node.active_backlog(),
            5_000_000,
            "active_backlog still reports the raw resident count (unchanged)"
        );
        let idle = node.create_load_index();
        assert!(
            idle < CREATE_OCCUPANCY_SCALE && idle < node.active_backlog(),
            "a deep-but-idle backlog must yield a low intake-headroom index \
             (got {idle}), not the resident backlog"
        );
        assert_ne!(
            idle,
            crate::placement::SHED_LOAD,
            "an idle recovered node must not be shed out of placement"
        );

        // Graded steer: at ~50% of the memory watermark the index sits mid-band
        // (between full headroom and the hard shed), so placement steers *some*
        // creates away without freezing the node.
        node.mem_watermark_bytes = 1000;
        node.mem_pressure_bytes.store(500, Ordering::Relaxed);
        let mid = node.create_load_index();
        assert_eq!(
            mid,
            CREATE_OCCUPANCY_SCALE / 2,
            "50% memory occupancy must map to half the occupancy scale"
        );
        assert!(mid > 0 && mid < crate::placement::SHED_LOAD);

        // The hard OOM backstop is unchanged: resident at/above the watermark
        // reports SHED_LOAD (weight 0 — never placed on), regardless of intake
        // headroom.
        node.mem_watermark_bytes = 1;
        node.mem_pressure_bytes.store(1 << 20, Ordering::Relaxed);
        assert_eq!(
            node.create_load_index(),
            crate::placement::SHED_LOAD,
            "a node past its memory watermark must still hard-shed"
        );
    }

    #[test]
    fn weighted_placement_steers_away_from_loaded_and_shedding_peers() {
        let node0 = clustered_node(0);

        // A shedding peer (SHED_LOAD) is never placed on: weighted placement
        // returns a local slot every time.
        node0.record_peer_pressure(1, crate::placement::SHED_LOAD);
        for _ in 0..32 {
            assert_eq!(
                node0.next_create_placement_weighted(&[]),
                None,
                "a shedding peer must never receive a weighted placement"
            );
        }

        // A healthy peer (load 0) receives a meaningful share of creates...
        node0.record_peer_pressure(1, 0);
        let healthy = (0..1_000)
            .filter(|_| node0.next_create_placement_weighted(&[]) == Some(1))
            .count();
        assert!(healthy > 0, "a healthy peer must receive some creates");

        // ...but a heavily-loaded peer receives strictly fewer than a healthy one,
        // so creates are steered toward the node with headroom.
        node0.record_peer_pressure(1, 100_000);
        let loaded = (0..1_000)
            .filter(|_| node0.next_create_placement_weighted(&[]) == Some(1))
            .count();
        assert!(
            loaded < healthy,
            "a loaded peer ({loaded}) must receive fewer creates than a healthy one ({healthy})"
        );
    }

    #[test]
    fn stale_peer_pressure_expires_to_full_headroom_on_rejoin() {
        // Rejoin regression (#1 zero-creates): a peer that shed (SHED_LOAD) just
        // before it died must not be steered away from forever. Once its last
        // gossip is older than the TTL, weighted placement reverts it to full
        // headroom so a returning node re-enters create placement — even if its
        // fresh post-restart gossip is briefly delayed on a saturated link.
        let node0 = clustered_node(0);

        // Fresh SHED reading → never placed on (baseline: the pre-death state).
        node0.record_peer_pressure(1, crate::placement::SHED_LOAD);
        assert_eq!(node0.peer_load(1), Some(crate::placement::SHED_LOAD));
        assert_eq!(
            node0.next_create_placement_weighted(&[]),
            None,
            "a freshly-shedding peer must not receive weighted placement"
        );

        // Backdate that SHED reading beyond the TTL: the peer has gone silent.
        let stale = std::time::Instant::now() - (peer_pressure_ttl() + Duration::from_secs(1));
        node0.record_peer_pressure_at(1, crate::placement::SHED_LOAD, stale);
        assert_eq!(
            node0.peer_load(1),
            None,
            "a peer-pressure reading older than the TTL must read as absent (full headroom)"
        );

        // With the stale SHED expired, the returning peer is treated as headroom
        // and receives a meaningful share of creates again.
        let recovered = (0..1_000)
            .filter(|_| node0.next_create_placement_weighted(&[]) == Some(1))
            .count();
        assert!(
            recovered > 0,
            "a peer whose stale SHED expired must re-enter create placement ({recovered} creates)"
        );
    }

    #[test]
    fn peer_pressure_snapshot_flags_expired_readings_for_debug_peers() {
        // The /debug/peers diagnostic must faithfully report whether a peer's
        // gossiped reading has aged past the TTL — that expired flag is exactly
        // what tells an operator "placement now treats this peer as headroom",
        // distinguishing a still-pinned fresh SHED from an expired-and-eligible
        // one during a rejoin soak.
        let node0 = clustered_node(0);

        // A fresh reading is present and not expired.
        node0.record_peer_pressure(1, crate::placement::SHED_LOAD);
        let snap = node0.peer_pressure_snapshot();
        let row = snap
            .iter()
            .find(|(n, ..)| *n == 1)
            .expect("peer 1 present in snapshot");
        assert_eq!(row.1, crate::placement::SHED_LOAD, "load reported verbatim");
        assert!(!row.3, "a fresh reading must not be flagged expired");

        // Backdate it past the TTL: the snapshot must now flag it expired.
        let stale = std::time::Instant::now() - (peer_pressure_ttl() + Duration::from_secs(1));
        node0.record_peer_pressure_at(1, crate::placement::SHED_LOAD, stale);
        let row = node0
            .peer_pressure_snapshot()
            .into_iter()
            .find(|(n, ..)| *n == 1)
            .expect("peer 1 still present in snapshot");
        assert!(
            row.3,
            "a reading older than the TTL must be flagged expired in the debug snapshot"
        );
        assert!(
            row.2 >= peer_pressure_ttl().as_millis(),
            "reported age must reflect the backdated timestamp"
        );
    }

    #[tokio::test]
    async fn protected_create_reroutes_around_a_shedding_owner() {
        // A saturated owner, in `protect` mode, sheds a forwarded create back to
        // the ingress node, which reroutes it to a node with headroom (here,
        // itself) — so the client sees a successful create, never the owner's
        // saturation. Without protection the forwarded create would be applied on
        // the saturated owner regardless (the gap ADR 0014 closes).
        let mut node1 = clustered_node(1);
        // Force node 1 to shed every create via the resident-memory admission rail
        // (watermark 1 byte, sampled resident well above it), and enable protection
        // so it sheds *forwarded* creates back for rerouting.
        node1.placement_mode = crate::placement::PlacementMode::Protect;
        node1.mem_watermark_bytes = 1;
        node1.mem_pressure_bytes.store(1 << 20, Ordering::Relaxed);
        let node1_url = serve_node(&node1).await;

        // node 0 is the deployment-partition owner (seeds `demo` on partition 0),
        // the ingress the client hits, also in protect mode, pointing at node 1.
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
        let mut node0 = build_server_in_memory(journals, topology);
        node0.placement_mode = crate::placement::PlacementMode::Protect;

        let body =
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionById(
                models::ProcessInstanceCreationInstructionById::new("demo".to_string()),
            );

        // Drive enough creates that placement lands on node 1 several times; every
        // one must succeed (200) — rerouted to node 0 when node 1 sheds — rather
        // than surfacing node 1's 503.
        for i in 0..8 {
            let resp = node0
                .create_process_instance_impl(&body)
                .await
                .expect("create returns a response");
            assert!(
                matches!(
                    resp,
                    apis::process_instance::CreateProcessInstanceResponse::Status200_TheProcessInstanceWasCreated(_)
                ),
                "create #{i} must succeed via reroute, got a non-200 response"
            );
        }
    }

    /// Serves a node's falcon endpoint on an ephemeral port and returns
    /// its HTTP base URL, so a peer can forward to it exactly as in a cluster.
    async fn serve_node(server: &ServerImpl) -> String {
        let registry = falcon::Registry::new();
        falcon::spawn_dispatcher(server.clone(), registry.clone());
        let app = falcon::router(server.clone(), registry);
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
        let node1 = build_server_in_memory(journals, topology);

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
            build_server_in_memory(journals, topology)
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

        // Serve both nodes' falcon endpoints on their pre-bound ports.
        for (server, listener) in [(node0.clone(), l0), (node1.clone(), l1)] {
            let registry = falcon::Registry::new();
            falcon::spawn_dispatcher(server.clone(), registry.clone());
            let app = falcon::router(server.clone(), registry);
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
        assert!(
            p_inst == 0 || p_inst == 2,
            "instance on a node-0 partition, got {p_inst}"
        );

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
        // partitions 1 & 3) over the Falcon protocol so node 1 mints its share.
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
            build_server_in_memory(journals, topology)
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

        // Serve both nodes' falcon endpoints on their pre-bound ports.
        for (server, listener) in [(node0.clone(), l0), (node1.clone(), l1)] {
            let registry = falcon::Registry::new();
            falcon::spawn_dispatcher(server.clone(), registry.clone());
            let app = falcon::router(server.clone(), registry);
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
        let node1 = build_server_in_memory(journals, topology);

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
        let node1 = build_server_in_memory(journals, topology);

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
        let node1 = build_server_in_memory(journals, topology);

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
        let node1 = build_server_in_memory(journals, topology);

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
            assert_eq!(
                b.partitions.iter().filter(|p| p.role != "leader").count(),
                0
            );
            by_node.insert(
                b.node_id,
                b.partitions.iter().map(|p| p.partition_id).collect(),
            );
        }
        // 1-based partition ids: node 0 owns internal {0,2} -> {1,3}; node 1 {1,3} -> {2,4}.
        assert_eq!(
            by_node.get(&0),
            Some(&vec![1, 3]),
            "node 0 owns partitions 1 & 3 (1-based)"
        );
        assert_eq!(
            by_node.get(&1),
            Some(&vec![2, 4]),
            "node 1 owns partitions 2 & 4 (1-based)"
        );

        // The response advertises that this is a nanobpmn gateway so SDK clients
        // can detect the engine and upgrade to the Falcon protocol.
        let nano = t
            .nano
            .expect("nanobpmn topology must advertise the `nano` object");
        assert_eq!(nano.engine, "nanobpmn");
        assert_eq!(nano.falcon_path, "/falcon");
        assert!(
            nano.version.is_some(),
            "nano advertises the gateway version"
        );
    }

    #[tokio::test]
    async fn topology_reports_replication_factor_and_follower_roles() {
        // A 3-node, 3-partition cluster at RF=3: every node is a replica of every
        // partition, leading the one it owns and following the other two. The
        // topology must surface RF=3 and a leader/follower role per partition —
        // not the old hardcoded RF=1, owner-only, all-"leader" view.
        let topology = cluster::Topology {
            node_id: 0,
            peers: vec!["http://n0".into(), "http://n1".into(), "http://n2".into()],
            num_partitions: 3,
            replication_factor: 3,
        };
        let journals: Vec<Journal> = topology
            .local_partitions()
            .iter()
            .map(|p| Journal::in_memory_partition(*p))
            .collect();
        let node0 = build_server_in_memory(journals, topology);

        use apis::cluster::GetTopologyResponse as Resp;
        let t = match node0.get_topology_impl().await.unwrap() {
            Resp::Status200_ObtainsTheCurrentTopologyOfTheClusterTheGatewayIsPartOf(t) => t,
            other => panic!("expected 200 topology, got {other:?}"),
        };

        assert_eq!(t.replication_factor, 3, "RF=3 is reported, not hardcoded 1");
        assert_eq!(t.brokers.len(), 3, "one broker per node");

        // Every broker replicates all 3 partitions, leading exactly one of them.
        for b in &t.brokers {
            assert_eq!(
                b.partitions.len(),
                3,
                "node {} replicates all 3 partitions under RF=3",
                b.node_id
            );
            let leaders = b.partitions.iter().filter(|p| p.role == "leader").count();
            let followers = b.partitions.iter().filter(|p| p.role == "follower").count();
            assert_eq!(leaders, 1, "node {} leads exactly one partition", b.node_id);
            assert_eq!(followers, 2, "node {} follows the other two", b.node_id);
            // The partition a node leads is the one it owns (1-based).
            let led: Vec<i32> = b
                .partitions
                .iter()
                .filter(|p| p.role == "leader")
                .map(|p| p.partition_id)
                .collect();
            assert_eq!(led, vec![b.node_id + 1], "node leads its owned partition");
        }
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
    fn next_create_partition_round_robins_every_partition() {
        // The leader-aware stream create placement (`stream_leader_placement`)
        // rides this cursor: it must sweep EVERY partition in the cluster, not
        // just the ones this node owns, so a producer on one gateway can drive
        // instances on peer-led partitions (incl. a recovered node's). node 0 of a
        // 2-node, 4-partition cluster still sees all four ids come round.
        let node0 = clustered_node(0);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..8 {
            seen.insert(
                node0
                    .engine
                    .next_create_partition()
                    .expect("multi-partition cluster yields a placement partition"),
            );
        }
        assert_eq!(
            seen,
            std::collections::HashSet::from([0, 1, 2, 3]),
            "placement sweeps every partition in the cluster"
        );
    }

    #[test]
    fn single_node_next_create_partition_is_local() {
        // A single-node cluster owns every partition, so the leader-aware stream
        // placement is always local (None) — no forwarding, byte-identical fast
        // path.
        let solo = ServerImpl::default();
        for _ in 0..16 {
            assert!(
                solo.engine.next_create_partition().is_none(),
                "single node never forwards a stream create"
            );
        }
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

    #[test]
    fn drain_guard_gates_admission_when_engaged() {
        // Wiring test: the drain-stall guard's published state must actually gate
        // the create-admission path. The hard valve sheds via `admission_shed` /
        // blocks via `create_admission_blocked`; the soft servo only *meters*
        // (paces credits) and must NOT hard-block or shed. State is published by
        // the ~1 Hz monitor; here we publish directly.
        let server = ServerImpl::default();
        let guard = server.drain_guard().clone();

        // Hard valve engaged: creates are shed with the halt reason and intake is
        // blocked, regardless of any other rail's state.
        guard.publish(true, true);
        assert!(guard.is_halted());
        assert!(server.create_admission_blocked());
        let reason = server.admission_shed().expect("halt must shed");
        assert!(
            reason.contains("halting new instance creation"),
            "halt reason should win: {reason}"
        );

        // Servo metering only (no halt): intake is paced, not blocked or shed.
        guard.publish(true, false);
        assert!(guard.is_metering() && !guard.is_halted());
        assert!(
            !server.create_admission_blocked(),
            "the servo paces credits; it must not hard-block creates"
        );
        assert!(
            server.admission_shed().is_none(),
            "the servo must not surface as an admission shed"
        );

        // Cleared: the guard no longer blocks creates.
        guard.publish(false, false);
        assert!(!server.drain_guard().blocks_creates());
    }

    #[tokio::test]
    async fn create_forwards_to_a_peer_partition() {
        // A create whose cluster placement lands on a peer's partition is
        // forwarded to that peer, which mints the instance on one of ITS OWN
        // partitions and returns the full result. node 1 owns partitions 1 & 3.
        let node1 = clustered_node(1);
        // node 1 owns no deployment partition, so install the demo over the wire
        // path (mirrors the centralized broadcast) before it can create.
        node1
            .install_replicated_deployment(demo_deployment_events().await)
            .await;
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
        let node0 = build_server_in_memory(journals, topology);

        use apis::process_instance::CreateProcessInstanceResponse as R;
        let resp = node0
            .forward_create(
                1,
                Some("demo".into()),
                None,
                None,
                vec![],
                None,
                false,
                None,
                None,
            )
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
        // job over the Falcon protocol (activate_from_peer) and completes it via
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
        let node1 = build_server_in_memory(journals, topology);

        // node 1 owns no demo job locally; it must pull from node 0.
        let local = node1
            .activate_for_stream("demo-work", "w", 10, 60_000, None)
            .await;
        assert!(local.is_empty(), "node 1 owns no demo-work job of its own");

        let pulled = node1
            .activate_from_peer(0, "demo-work", "w", 10, 60_000, None)
            .await;
        assert_eq!(pulled.len(), 1, "node 1 pulls the peer's parked job");
        let job_key: u64 = pulled[0].job_key.0.parse().expect("numeric job key");
        let p = nanobpmn_engine_core::partition_of(job_key);
        assert!(
            p == 0 || p == 2,
            "the pulled job lives on a node-0 partition, got {p}"
        );

        // The owner owns the job's partition, so the completion must forward.
        let owner = node1
            .remote_owner_of(job_key)
            .expect("the job's partition is owned by node 0");
        assert_eq!(owner, 0);
        let (status, _) = node1
            .forward_complete_job_stream(owner, job_key, None)
            .await;
        assert!(
            is_ok_status(status),
            "forwarded completion succeeds, got {status}"
        );

        // Re-completing the same job is rejected — proof it mutated node 0's state.
        let (again, _) = node1
            .forward_complete_job_stream(owner, job_key, None)
            .await;
        assert!(
            !is_ok_status(again),
            "re-completing must not succeed, got {again}"
        );
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
        let node1 = build_server_in_memory(journals, topology);

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
        assert_eq!(
            jobs.len(),
            1,
            "node 1 aggregates the peer's parked job over REST"
        );
        let job_key: u64 = jobs[0].job_key.0.parse().expect("numeric job key");
        let p = nanobpmn_engine_core::partition_of(job_key);
        assert!(
            p == 0 || p == 2,
            "the aggregated job lives on a node-0 partition, got {p}"
        );
    }

    #[tokio::test]
    async fn raft_rpcs_replicate_a_command_across_two_nodes_over_the_falcon() {
        // Proves the falcon Raft binding: two served nodes host a 2-voter
        // Raft group for partition 0, carry AppendEntries/Vote RPCs over the real
        // Falcon protocol (PeerTransport -> ClientFrame::Raft -> dispatch_raft_rpc),
        // and a command proposed on the leader commits via quorum and applies on
        // BOTH nodes.
        use std::collections::BTreeMap;

        use openraft::BasicNode;

        use crate::raft::RaftPartition;

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
            build_server_in_memory(journals, topology)
        };
        let node0 = build_node(0);
        let node1 = build_node(1);

        // Serve both nodes' falcon endpoints so the PeerTransport can reach
        // them. (Serve BEFORE bootstrapping voters so inbound RPCs are accepted as
        // soon as the group starts electing.)
        for (server, listener) in [(node0.clone(), l0), (node1.clone(), l1)] {
            let registry = falcon::Registry::new();
            falcon::spawn_dispatcher(server.clone(), registry.clone());
            let app = falcon::router(server.clone(), registry);
            tokio::spawn(async move {
                axum::serve(listener, app).await.ok();
            });
        }

        // Host a Raft group for partition 0 on BOTH nodes, each driving its peer
        // over its own PeerTransport (the production falcon carrier). A
        // voter must be able to RECEIVE AppendEntries before the group forms, so
        // construct + register every member first, then initialize once.
        let part0 = Arc::new(
            RaftPartition::bootstrap_member(
                0,
                0,
                DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None),
                node0.raft_transport(),
                None,
                false,
            )
            .await
            .expect("boot raft member on node 0"),
        );
        node0.raft_registry().insert(part0.clone());

        let part1 = Arc::new(
            RaftPartition::bootstrap_member(
                1,
                0,
                DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None),
                node1.raft_transport(),
                None,
                false,
            )
            .await
            .expect("boot raft member on node 1"),
        );
        node1.raft_registry().insert(part1.clone());

        // Form the {0,1} group on node 0 and let it win the initial election —
        // every Vote/AppendEntries to node 1 rides the real Falcon protocol.
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
        // only once node 1 acks the entry over the Falcon protocol.
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
        // applied on the follower purely over the falcon Raft binding.
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
            "node 1 did not apply up to index {target} over the Falcon protocol"
        );

        part0.raft.shutdown().await.expect("clean shutdown node 0");
        part1.raft.shutdown().await.expect("clean shutdown node 1");
    }

    #[tokio::test]
    async fn raft_bootstrap_forms_every_group_and_elects_leaders_across_two_nodes() {
        // L2a: the env-gated startup orchestration. Two served nodes (RF=2, so
        // each replicates all 4 partitions) run `raft_bootstrap`; afterwards every
        // partition must have formed its group and elected its owner as leader —
        // entirely over the Falcon protocol, with the multi-process startup race
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
            build_server_in_memory(journals, topology)
        };
        let node0 = build_node(0);
        let node1 = build_node(1);

        for (server, listener) in [(node0.clone(), l0), (node1.clone(), l1)] {
            let registry = falcon::Registry::new();
            falcon::spawn_dispatcher(server.clone(), registry.clone());
            let app = falcon::router(server.clone(), registry);
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
            let owner = p % 2; // owner_of(p) for 2 nodes
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
            build_server_in_memory(journals, topology)
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
            let registry = falcon::Registry::new();
            falcon::spawn_dispatcher(server.clone(), registry.clone());
            let app = falcon::router(server.clone(), registry);
            tokio::spawn(async move {
                axum::serve(listener, app).await.ok();
            });
        }

        // Both nodes bootstrap (building follower replica engine actors, seeded
        // with the deployment) and form every group over the Falcon protocol.
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
            .with(move |journal| {
                journal
                    .engine()
                    .state()
                    .instances
                    .contains_key(&instance_key)
            })
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

    #[tokio::test]
    async fn a_raft_routed_complete_converges_the_follower_replica_actor() {
        // s3-failover correctness: because activation is now a LOGGED command, the
        // follower's replica engine actor locks the job in lockstep, so a later
        // replicated `CompleteJob` applies cleanly there too and the instance
        // COMPLETES on the follower — not stuck parked at the service task (the
        // pre-fix divergence, where the follower swallowed `JobNotActivated`).
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
            build_server_in_memory(journals, topology)
        };
        let node0 = build_node(0);
        let node1 = build_node(1);

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
            let registry = falcon::Registry::new();
            falcon::spawn_dispatcher(server.clone(), registry.clone());
            let app = falcon::router(server.clone(), registry);
            tokio::spawn(async move {
                axum::serve(listener, app).await.ok();
            });
        }
        tokio::join!(node0.raft_bootstrap(), node1.raft_bootstrap());

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

        let (instance_key, _completed) = node0
            .create_for_stream(Some("intake".into()), None, Default::default())
            .await
            .expect("raft-routed create commits");
        let part = nanobpmn_engine_core::partition_of(instance_key);

        // Grab the follower's replica engine actor up front so we can watch it
        // apply each replicated command in lockstep.
        let replica = {
            let map = node1.raft_replicas.lock().unwrap();
            map.get(&part).cloned()
        }
        .expect("node 1 hosts a replica engine actor for the leader's partition");

        // Phase 1: the create must replicate and apply on the follower, parking
        // the instance at the service task. It stays parked until we activate +
        // complete below, so this reliably observes it present before eviction.
        let mut saw_parked = false;
        for _ in 0..400 {
            if replica
                .with(move |journal| journal.engine().instance(instance_key).is_some())
                .await
            {
                saw_parked = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            saw_parked,
            "the follower replica applied the create and parked the instance"
        );

        // Activate through the leader (a LOGGED ActivateJobs), then complete.
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
        node0
            .complete_job_for_stream(job_key, Default::default())
            .await
            .expect("raft-routed complete commits via quorum")
            .wait()
            .await;

        // Phase 2: the follower's REPLICA engine actor must converge to the
        // instance being COMPLETED — proof it applied the replicated `CompleteJob`
        // cleanly (the pre-fix divergence swallowed `JobNotActivated` and left the
        // instance stuck PARKED forever). A follower has no exporter, so `apply`
        // then reclaims the terminal shell (the RF>1 leak fix); since we already
        // saw it parked, its disappearance is proof it reached terminal — the only
        // follower removal path is terminal eviction (a diverged instance would
        // stay parked and present, never evicting).
        let mut converged = false;
        for _ in 0..400 {
            let (present, done) = replica
                .with(move |journal| {
                    let e = journal.engine();
                    (
                        e.instance(instance_key).is_some(),
                        e.is_completed(instance_key),
                    )
                })
                .await;
            if done || !present {
                converged = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        if !converged {
            let diag = replica
                .with(move |journal| {
                    let e = journal.engine();
                    let inst = e.instance(instance_key).map(|i| format!("{:?}", i.state));
                    let njobs = e.state().jobs.len();
                    let ninst = e.state().instances.len();
                    let job = e
                        .job(job_key)
                        .map(|j| format!("{:?} activated={}", j.state, j.activated));
                    format!("instance={inst:?} njobs={njobs} ninst={ninst} job={job:?}")
                })
                .await;
            panic!("follower did not converge: {diag}");
        }
        assert!(
            converged,
            "follower replica must converge: the instance completes there too \
             (logged activation keeps the replica in lockstep for the complete), \
             then its terminal shell is evicted since a follower has no exporter"
        );

        for node in [&node0, &node1] {
            for p in 0..4u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn killing_the_leader_keeps_the_partition_serving_on_the_new_leader() {
        // s3-failover continuity: a 3-node RF=3 group commits an instance through
        // partition 0's leader (node 0). We then SHUT DOWN node 0's Raft groups.
        // The two survivors {1,2} re-elect a leader for partition 0 (quorum 2/3).
        // Because serving now follows leadership (`led_partitions()` /
        // `engine_handle_for()`), the new leader — which only REPLICATED partition 0
        // before — can now activate the parked job and complete the instance over a
        // fresh quorum. Proves data survival + write continuity past a leader loss.
        use crate::raft::RaftPartition;

        let mut listeners = Vec::new();
        let mut ports = Vec::new();
        for i in 0..3u32 {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .unwrap_or_else(|_| panic!("bind node {i}"));
            ports.push(l.local_addr().expect("addr").port());
            listeners.push(l);
        }
        let peers: Vec<String> = ports
            .iter()
            .map(|p| format!("http://127.0.0.1:{p}"))
            .collect();

        let build_node = |node_id: u32| {
            let topology = cluster::Topology {
                node_id,
                peers: peers.clone(),
                num_partitions: 3,
                replication_factor: 3,
            };
            let journals: Vec<Journal> = topology
                .local_partitions()
                .iter()
                .map(|p| Journal::in_memory_partition(*p))
                .collect();
            build_server_in_memory(journals, topology)
        };
        let node0 = build_node(0);
        let node1 = build_node(1);
        let node2 = build_node(2);

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
        node2.install_replicated_deployment(events.to_vec()).await;

        let served = [
            (node0.clone(), listeners.remove(0)),
            (node1.clone(), listeners.remove(0)),
            (node2.clone(), listeners.remove(0)),
        ];
        for (server, listener) in served {
            let registry = falcon::Registry::new();
            falcon::spawn_dispatcher(server.clone(), registry.clone());
            let app = falcon::router(server.clone(), registry);
            tokio::spawn(async move {
                axum::serve(listener, app).await.ok();
            });
        }
        tokio::join!(
            node0.raft_bootstrap(),
            node1.raft_bootstrap(),
            node2.raft_bootstrap()
        );

        let leader_of = |node: &ServerImpl, p: u64| -> Option<u64> {
            node.raft_registry()
                .get(p)
                .and_then(|part: Arc<RaftPartition>| part.raft.metrics().borrow().current_leader)
        };

        // Node 0 must lead partition 0 before we create through it.
        let mut ok = false;
        for _ in 0..500 {
            if leader_of(&node0, 0) == Some(0) {
                ok = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(ok, "node 0 must lead partition 0");

        let (instance_key, _completed) = node0
            .create_for_stream(Some("intake".into()), None, Default::default())
            .await
            .expect("raft-routed create commits via quorum");
        assert_eq!(
            nanobpmn_engine_core::partition_of(instance_key),
            0,
            "the instance is minted on node 0's owned partition 0"
        );

        // Kill the leader: shut down ALL of node 0's Raft groups.
        for p in 0..3u64 {
            if let Some(part) = node0.raft_registry().get(p) {
                part.raft.shutdown().await.ok();
            }
        }

        // The survivors {1,2} must elect a new leader for partition 0.
        let survivors = [&node1, &node2];
        let mut new_leader: Option<&ServerImpl> = None;
        'elect: for _ in 0..500 {
            for node in survivors {
                match leader_of(node, 0) {
                    Some(l) if l != 0 => {
                        new_leader = Some(if l == 1 { &node1 } else { &node2 });
                        break 'elect;
                    }
                    _ => {}
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let new_leader = new_leader.expect("survivors must re-elect a leader for partition 0");

        // Data survived the failover: the parked instance exists on the new leader's
        // engine handle for partition 0 (which it only replicated before).
        let survived = new_leader
            .engine_handle_for(0)
            .expect("new leader materializes partition 0")
            .with(move |journal| journal.engine().instance(instance_key).is_some())
            .await;
        assert!(
            survived,
            "the committed instance must survive the leader loss"
        );

        // Write continuity: activate + complete on the NEW leader over a fresh quorum.
        let mut job_key = None;
        for _ in 0..200 {
            let jobs = new_leader
                .activate_for_stream("do-work", "w", 10, 60_000, None)
                .await;
            if let Some(j) = jobs.into_iter().next() {
                job_key = Some(j.job_key.0.parse::<u64>().expect("numeric job key"));
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let job_key = job_key.expect("the parked job activates on the new leader after failover");
        new_leader
            .complete_job_for_stream(job_key, Default::default())
            .await
            .expect("complete commits via the new quorum")
            .wait()
            .await;

        let handle = new_leader
            .engine_handle_for(0)
            .expect("new leader materializes partition 0");
        let mut completed = false;
        for _ in 0..400 {
            if handle
                .with(move |journal| journal.engine().is_completed(instance_key))
                .await
            {
                completed = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            completed,
            "the instance completes on the NEW leader after the original leader was killed"
        );

        for node in [&node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
    }

    /// Boots a 3-node RF=3 cluster (3 partitions; node i owns partition i, every
    /// node replicates every partition) hosting the `intake` process, serves all
    /// three over the real Falcon protocol, bootstraps the Raft groups, and waits
    /// until node 0 leads partition 0. Returns the three nodes. Used by the
    /// broadened failover tests below.
    async fn boot_rf3_intake_cluster() -> (ServerImpl, ServerImpl, ServerImpl) {
        let (n0, n1, n2, _h) = boot_rf3_intake_cluster_cfg2(false, false).await;
        (n0, n1, n2)
    }

    /// As [`boot_rf3_intake_cluster`], but the cluster runs in leader-durable
    /// replication mode (`NANOBPMN_REPLICATION=leader-durable`, ADR 0003): each led
    /// group is formed with the leader as the sole voter and the other replicas as
    /// learners, so writes ack on the leader without follower quorum.
    async fn boot_rf3_leader_durable_cluster() -> (ServerImpl, ServerImpl, ServerImpl) {
        let (n0, n1, n2, _h) = boot_rf3_intake_cluster_cfg2(false, true).await;
        (n0, n1, n2)
    }

    /// As [`boot_rf3_intake_cluster`], but when `digest` is set the cluster runs in
    /// best-effort lease-digest mode (`NANOBPMN_REPLICATE_ACTIVATION=digest`):
    /// leader-local activation plus a soft lease broadcast. Tests can't set the env
    /// var (it would race other parallel tests), so the relevant per-node state is
    /// configured directly before the nodes are served and bootstrapped.
    async fn boot_rf3_intake_cluster_cfg(digest: bool) -> (ServerImpl, ServerImpl, ServerImpl) {
        let (n0, n1, n2, _h) = boot_rf3_intake_cluster_cfg2(digest, false).await;
        (n0, n1, n2)
    }

    /// Backing helper for the RF=3 cluster boots: `digest` enables lease-digest
    /// mode (ADR 0002 B) and `leader_durable` enables leader-durable replication
    /// (ADR 0003). Both default off (plain `quorum` + fully-replicated activation).
    /// Per-node state is set directly because the env readers can't be used safely
    /// under parallel tests. Also returns the three serve-task handles (node order)
    /// so a test can `abort()` a node's falcon server to make it genuinely
    /// unreachable (the leader-durable failure detector keys on peer reachability).
    async fn boot_rf3_intake_cluster_cfg2(
        digest: bool,
        leader_durable: bool,
    ) -> (
        ServerImpl,
        ServerImpl,
        ServerImpl,
        Vec<tokio::task::JoinHandle<()>>,
    ) {
        use crate::raft::RaftPartition;

        let mut listeners = Vec::new();
        let mut ports = Vec::new();
        for i in 0..3u32 {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .unwrap_or_else(|_| panic!("bind node {i}"));
            ports.push(l.local_addr().expect("addr").port());
            listeners.push(l);
        }
        let peers: Vec<String> = ports
            .iter()
            .map(|p| format!("http://127.0.0.1:{p}"))
            .collect();

        let build_node = |node_id: u32| {
            let topology = cluster::Topology {
                node_id,
                peers: peers.clone(),
                num_partitions: 3,
                replication_factor: 3,
            };
            let journals: Vec<Journal> = topology
                .local_partitions()
                .iter()
                .map(|p| Journal::in_memory_partition(*p))
                .collect();
            build_server_in_memory(journals, topology)
        };
        let mut node0 = build_node(0);
        let mut node1 = build_node(1);
        let mut node2 = build_node(2);

        // Digest mode = leader-local activation PLUS the soft lease broadcast.
        // Configure the policy directly (the env reader can't be used safely under
        // parallel tests) and relax the completion check on every owned engine
        // actor, exactly as `ServerImpl::new` would have for a leader-local policy.
        // The follower replica engines built during `raft_bootstrap` then pick up
        // lenient completion automatically (they read the now-leader-local policy).
        if digest {
            for node in [&mut node0, &mut node1, &mut node2] {
                node.activation_policy = ActivationPolicy::Digest;
                node.lease_digest = true;
                for handle in node.engine.all() {
                    handle
                        .with(|journal| journal.set_lenient_completion(true))
                        .await;
                }
            }
        }

        // Leader-durable replication (ADR 0003): set the tier directly before
        // bootstrap so each leader forms its group as the sole voter + learners.
        // `replication_mode` is a plain `Copy` field read in `raft_bootstrap`, so it
        // must be set before that runs (and before the `clone()` that serves each
        // node).
        if leader_durable {
            for node in [&mut node0, &mut node1, &mut node2] {
                node.replication_mode = ReplicationMode::LeaderDurable;
            }
        }

        let proc = ProcessBuilder::new("intake")
            .start_event("start")
            .service_task("work", "do-work")
            .end_event("end")
            .connect("start", "work")
            .connect("work", "end")
            .build()
            .expect("valid process");
        // An auto-completing process (start -> end, no wait state) so tests can
        // exercise the synchronous-completion / awaitCompletion path through Raft.
        let auto = ProcessBuilder::new("auto")
            .start_event("start")
            .end_event("end")
            .connect("start", "end")
            .build()
            .expect("valid auto process");
        let mut names = std::collections::HashMap::new();
        names.insert("intake".to_string(), "intake.bpmn".to_string());
        names.insert("auto".to_string(), "auto.bpmn".to_string());
        let (_r, events) = node0
            .deploy_resources_locally(vec![proc, auto], &names, "<default>")
            .await
            .expect("deploy on the owner");
        node1.install_replicated_deployment(events.to_vec()).await;
        node2.install_replicated_deployment(events.to_vec()).await;

        let served = [
            (node0.clone(), listeners.remove(0)),
            (node1.clone(), listeners.remove(0)),
            (node2.clone(), listeners.remove(0)),
        ];
        let mut serve_handles = Vec::new();
        for (server, listener) in served {
            let registry = falcon::Registry::new();
            falcon::spawn_dispatcher(server.clone(), registry.clone());
            let app = falcon::router(server.clone(), registry);
            serve_handles.push(tokio::spawn(async move {
                axum::serve(listener, app).await.ok();
            }));
        }
        tokio::join!(
            node0.raft_bootstrap(),
            node1.raft_bootstrap(),
            node2.raft_bootstrap()
        );

        // Every partition must have an elected leader before creates fan out
        // round-robin across all three; otherwise a create routed to a partition
        // whose leader hasn't been elected yet (e.g. on a slow CI runner) comes
        // back non-200. Check each partition from its owning node (node `p` leads
        // partition `p` in this symmetric topology) — node 0 is only a learner for
        // partitions 1 and 2 under leader-durable replication, so it can't observe
        // their leaders.
        let nodes = [&node0, &node1, &node2];
        let leads_own = |p: usize| -> bool {
            nodes[p]
                .raft_registry()
                .get(p as u64)
                .and_then(|part: Arc<RaftPartition>| part.raft.metrics().borrow().current_leader)
                == Some(p as u64)
        };
        let mut ok = false;
        for _ in 0..500 {
            if (0..3).all(leads_own) {
                ok = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            ok,
            "every partition must have its owning node as leader before creates"
        );
        (node0, node1, node2, serve_handles)
    }

    /// Re-election helper: polls survivors {n1, n2} until partition `p` has a leader
    /// that is not the killed node, returning the survivor that won.
    async fn wait_new_leader<'a>(n1: &'a ServerImpl, n2: &'a ServerImpl, p: u64) -> &'a ServerImpl {
        use crate::raft::RaftPartition;
        let leader_of = |node: &ServerImpl, p: u64| -> Option<u64> {
            node.raft_registry()
                .get(p)
                .and_then(|part: Arc<RaftPartition>| part.raft.metrics().borrow().current_leader)
        };
        for _ in 0..500 {
            for node in [n1, n2] {
                if let Some(l) = leader_of(node, p)
                    && l != 0
                {
                    return if l == n1.engine.topology().node_id as u64 {
                        n1
                    } else {
                        n2
                    };
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("survivors must re-elect a leader for partition {p}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failover_preserves_every_in_flight_instance() {
        // s3-test: quorum-scale survival. Many instances commit through partition
        // 0's leader; after the leader is killed, the NEW leader must activate and
        // complete EVERY one of them (no data loss across the failover, not just a
        // single instance).
        let (node0, node1, node2) = boot_rf3_intake_cluster().await;

        const N: usize = 12;
        // Pin every create to partition 0 by proposing directly to its Raft
        // group. Cluster-wide leader-aware placement now spreads stream creates
        // across EVERY partition (a producer on one gateway drives the whole
        // cluster), so `create_for_stream` no longer lands all creates on
        // partition 0. This test specifically exercises failover durability for
        // the instances committed through partition 0's leader, so it targets
        // that partition explicitly.
        let part0 = node0
            .raft_registry()
            .get(0)
            .expect("node0 leads partition 0");
        let mut instances = Vec::new();
        for _ in 0..N {
            let item = part0
                .propose_result(
                    Command::create_instance_full("intake", Default::default(), Vec::new(), None),
                    now_millis(),
                )
                .await
                .expect("raft-routed create commits via quorum");
            assert!(item.error.is_none(), "create rejected: {:?}", item.error);
            let key = item
                .events
                .iter()
                .find_map(Event::instance_key)
                .expect("create produced an instance key");
            assert_eq!(nanobpmn_engine_core::partition_of(key), 0);
            instances.push(key);
        }

        // Kill the leader.
        for p in 0..3u64 {
            if let Some(part) = node0.raft_registry().get(p) {
                part.raft.shutdown().await.ok();
            }
        }
        let new_leader = wait_new_leader(&node1, &node2, 0).await;

        // Drain + complete every parked job on the new leader.
        let mut completed = 0usize;
        for _ in 0..2000 {
            let jobs = new_leader
                .activate_for_stream("do-work", "w", N, 60_000, None)
                .await;
            for j in jobs {
                let job_key = j.job_key.0.parse::<u64>().expect("numeric job key");
                new_leader
                    .complete_job_for_stream(job_key, Default::default())
                    .await
                    .expect("complete commits via the new quorum")
                    .wait()
                    .await;
                completed += 1;
            }
            if completed >= N {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(completed, N, "every job re-offers on the new leader");

        let handle = new_leader
            .engine_handle_for(0)
            .expect("new leader materializes partition 0");
        for key in instances {
            let mut done = false;
            for _ in 0..400 {
                if handle
                    .with(move |journal| journal.engine().is_completed(key))
                    .await
                {
                    done = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            assert!(done, "instance {key} must complete on the new leader");
        }

        for node in [&node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reads_follow_leadership_across_a_failover() {
        // s3-query-follow: a by-key READ must route to the partition's CURRENT
        // Raft leader, not the static owner. Before failover a follower forwards
        // the read to the owner/leader (node 0) while the leader serves locally;
        // after the owner is killed and the survivors re-elect, the read must
        // follow to the NEW leader so it hits a node whose applied read model is
        // up to date rather than the dead owner.
        let (node0, node1, node2) = boot_rf3_intake_cluster().await;

        let (instance_key, _c) = node0
            .create_for_stream(Some("intake".into()), None, Default::default())
            .await
            .expect("raft-routed create commits via quorum");
        assert_eq!(nanobpmn_engine_core::partition_of(instance_key), 0);

        // Baseline: the leader (node 0) serves the read locally; the followers
        // forward it to the leader.
        assert_eq!(
            node0.read_route(instance_key),
            None,
            "the leader serves the read from its own read model"
        );
        assert_eq!(
            node1.read_route(instance_key),
            Some(0),
            "a follower forwards the read to the current leader (node 0)"
        );
        assert_eq!(node2.read_route(instance_key), Some(0));

        // Kill the leader/owner: shut down all of node 0's Raft groups.
        for p in 0..3u64 {
            if let Some(part) = node0.raft_registry().get(p) {
                part.raft.shutdown().await.ok();
            }
        }
        let new_leader = wait_new_leader(&node1, &node2, 0).await;
        let new_leader_id = new_leader.engine.topology().node_id;

        // The new leader now serves the read locally; the other survivor forwards
        // to the NEW leader — never back to the dead owner (node 0).
        assert_eq!(
            new_leader.read_route(instance_key),
            None,
            "the new leader serves the read locally after failover"
        );
        let other = if new_leader_id == node1.engine.topology().node_id {
            &node2
        } else {
            &node1
        };
        assert_eq!(
            other.read_route(instance_key),
            Some(new_leader_id),
            "the surviving follower's read follows leadership to the new leader, not the dead owner"
        );
        assert_ne!(
            other.read_route(instance_key),
            Some(0),
            "the read must not route to the killed owner"
        );

        for node in [&node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rest_mutations_follow_leadership_across_a_failover() {
        // s3-query-follow: a by-key REST MUTATION (here completeJob) issued
        // against a node that is NOT the partition's leader must forward to the
        // CURRENT leader, not the dead static owner. After the owner is killed,
        // a REST completeJob sent to the surviving follower must reach the
        // re-elected leader and drive the instance to completion.
        let (node0, node1, node2) = boot_rf3_intake_cluster().await;

        let (instance_key, _c) = node0
            .create_for_stream(Some("intake".into()), None, Default::default())
            .await
            .expect("raft-routed create commits via quorum");
        assert_eq!(nanobpmn_engine_core::partition_of(instance_key), 0);

        // Kill the leader/owner: shut down all of node 0's Raft groups.
        for p in 0..3u64 {
            if let Some(part) = node0.raft_registry().get(p) {
                part.raft.shutdown().await.ok();
            }
        }
        let new_leader = wait_new_leader(&node1, &node2, 0).await;
        let new_leader_id = new_leader.engine.topology().node_id;
        let other = if new_leader_id == node1.engine.topology().node_id {
            &node2
        } else {
            &node1
        };

        // `other` (the survivor we drive the REST mutation through) may momentarily
        // lag the election: its partition-0 view can still read leaderless right after
        // `wait_new_leader` observed the leader on the other survivor. Issuing the
        // by-key mutation in that window would route it nowhere useful (the owner is
        // dead). Wait until `other` agrees on the new leader so `route_by_leader`
        // forwards to it — the exact behaviour this test asserts.
        let mut other_converged = false;
        for _ in 0..400 {
            if other
                .raft_registry()
                .get(0)
                .and_then(|part| part.raft.metrics().borrow().current_leader)
                == Some(new_leader_id as u64)
            {
                other_converged = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            other_converged,
            "the follower survivor converges on the new leader before the REST mutation"
        );

        // Activate the parked job on the new leader to obtain its key.
        let mut job_key = None;
        for _ in 0..400 {
            let jobs = new_leader
                .activate_for_stream("do-work", "w", 10, 60_000, None)
                .await;
            if let Some(j) = jobs.into_iter().next() {
                job_key = Some(j.job_key.0.parse::<u64>().expect("numeric job key"));
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let job_key = job_key.expect("the parked job activates on the new leader");

        // Complete the job via the REST handler on the NON-leader survivor. Its
        // route_by_leader must forward to the new leader (not the dead owner),
        // so the call succeeds rather than 502-ing against node 0.
        let path = models::CompleteJobPathParams {
            job_key: job_key.to_string(),
        };
        let resp = other
            .complete_job_impl(&path, &None)
            .await
            .expect("complete_job_impl returns Ok");
        assert!(
            matches!(
                resp,
                apis::job::CompleteJobResponse::Status204_TheJobWasCompletedSuccessfully
            ),
            "a REST completeJob on a follower forwards to the new leader and succeeds after failover"
        );

        // The instance reaches completion on the new leader.
        let handle = new_leader
            .engine_handle_for(0)
            .expect("new leader materializes partition 0");
        let mut completed = false;
        for _ in 0..400 {
            if handle
                .with(move |journal| journal.engine().is_completed(instance_key))
                .await
            {
                completed = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            completed,
            "the instance completes after a REST mutation routed through the new leader"
        );

        for node in [&node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_create_on_a_node_that_leads_nothing_forwards_to_a_peer_leader() {
        // s3-create-forward: when a node leads NO partition (here because its Raft
        // groups were shut down while its gateway/peer links stay up), a stream
        // create must FORWARD to a peer that leads a partition and commit there,
        // instead of shedding a retryable 503. Exercises both the leads-nothing
        // forward and the Raft-aware peer-side create_forwarded path.
        let (node0, node1, node2) = boot_rf3_intake_cluster().await;

        // Shut down node 0's Raft groups: node 0 now leads nothing, but its
        // ServerImpl and peer uplinks remain alive. Survivors re-elect.
        for p in 0..3u64 {
            if let Some(part) = node0.raft_registry().get(p) {
                part.raft.shutdown().await.ok();
            }
        }
        // Every partition must have a survivor leader before node 0 forwards.
        for p in 0..3u64 {
            let _ = wait_new_leader(&node1, &node2, p).await;
        }

        // The create on node 0 (which leads nothing) must succeed by forwarding to
        // a peer leader rather than returning 503/500.
        let (instance_key, _sync) = node0
            .create_for_stream(Some("intake".into()), None, Default::default())
            .await
            .expect("a create on a node that leads nothing forwards to a peer leader");

        // The instance is committed on a survivor that leads its partition.
        let p = nanobpmn_engine_core::partition_of(instance_key);
        let leader = wait_new_leader(&node1, &node2, p).await;
        let handle = leader
            .engine_handle_for(p)
            .expect("the leader materializes the instance's partition");
        let mut present = false;
        for _ in 0..200 {
            if handle
                .with(move |journal| journal.engine().instance(instance_key).is_some())
                .await
            {
                present = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            present,
            "the forwarded create is committed on the peer leader's partition"
        );

        for node in [&node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_rest_create_replicates_through_raft_and_awaits_completion() {
        // s3-rest-create: the REST createProcessInstance path must go through Raft
        // under RF>=2 (not the legacy stage-1 direct apply), so the instance is
        // quorum-replicated and survives a node failure. Also exercises the
        // awaitCompletion path through Raft for an auto-completing process: the
        // synchronous-completion wait observes the read model by key and must
        // report processCompleted=true with no lost wakeup.
        use apis::process_instance::CreateProcessInstanceResponse as Resp;
        let (node0, node1, node2) = boot_rf3_intake_cluster().await;

        let mut instr = models::ProcessInstanceCreationInstructionById::new("auto".to_string());
        instr.await_completion = Some(true);
        instr.request_timeout = Some(5000);
        let body = models::ProcessInstanceCreationInstruction::from(instr);

        let resp = node0
            .create_process_instance_impl(&body)
            .await
            .expect("rest create returns a response");
        let result = match resp {
            Resp::Status200_TheProcessInstanceWasCreated(r) => r,
            other => panic!("rest create through raft should be 200, got {other:?}"),
        };
        assert!(
            result.process_completed,
            "the auto-completing process reports completion via awaitCompletion through Raft"
        );
        let instance_key: u64 = result
            .process_instance_key
            .0
            .parse()
            .expect("numeric instance key");

        // The instance's partition must APPLY on a FOLLOWER too — proof the REST
        // create replicated through Raft rather than applying only locally. We
        // check the follower's Raft applied index converges to the leader's
        // (lockstep apply) rather than engine residency: the process
        // auto-completes, and a follower has no exporter, so `apply` reclaims the
        // terminal shell the instant the complete applies (the RF>1 leak fix) —
        // it is correctly gone from the follower's hot state.
        let p = nanobpmn_engine_core::partition_of(instance_key);
        let leader_part = node0
            .raft_registry()
            .get(p)
            .expect("node 0 hosts partition p");
        let follower_part = if leader_part.raft.metrics().borrow().current_leader == Some(1) {
            node2.raft_registry().get(p)
        } else {
            node1.raft_registry().get(p)
        }
        .expect("a follower hosts the instance's partition");
        let target = leader_part
            .raft
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0);
        let mut present = false;
        for _ in 0..200 {
            let applied = follower_part
                .raft
                .metrics()
                .borrow()
                .last_applied
                .map(|l| l.index)
                .unwrap_or(0);
            if applied >= target {
                present = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            present,
            "the REST-created instance's log replicated and applied on a follower's partition"
        );

        for node in [&node0, &node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_rest_create_on_a_node_that_leads_nothing_forwards_to_a_peer_leader() {
        // s3-rest-create: a REST create on a node that leads NO partition (its Raft
        // groups shut down while its gateway stays up) must FORWARD to a peer
        // leader and commit there, returning 200 — not shed a 503.
        use apis::process_instance::CreateProcessInstanceResponse as Resp;
        let (node0, node1, node2) = boot_rf3_intake_cluster().await;

        for p in 0..3u64 {
            if let Some(part) = node0.raft_registry().get(p) {
                part.raft.shutdown().await.ok();
            }
        }
        for p in 0..3u64 {
            let _ = wait_new_leader(&node1, &node2, p).await;
        }

        let body = models::ProcessInstanceCreationInstruction::from(
            models::ProcessInstanceCreationInstructionById::new("intake".to_string()),
        );
        let resp = node0
            .create_process_instance_impl(&body)
            .await
            .expect("rest create returns a response");
        let result = match resp {
            Resp::Status200_TheProcessInstanceWasCreated(r) => r,
            other => panic!("a leads-nothing rest create should forward and 200, got {other:?}"),
        };
        let instance_key: u64 = result
            .process_instance_key
            .0
            .parse()
            .expect("numeric instance key");

        let p = nanobpmn_engine_core::partition_of(instance_key);
        let leader = wait_new_leader(&node1, &node2, p).await;
        let handle = leader
            .engine_handle_for(p)
            .expect("the leader materializes the instance's partition");
        let mut present = false;
        for _ in 0..200 {
            if handle
                .with(move |journal| journal.engine().instance(instance_key).is_some())
                .await
            {
                present = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            present,
            "the forwarded REST create is committed on the peer leader's partition"
        );

        for node in [&node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rest_creates_under_raft_spread_across_the_whole_cluster() {
        // Regression for the RF>=2 create-imbalance bug: a producer connected to
        // ONE gateway must spread createProcessInstance across EVERY partition in
        // the cluster (forwarding peer-owned placements to their leaders), not
        // concentrate every instance on the partitions THIS node leads. Before the
        // fix the Raft REST create path placed only among `led_partitions()`, so
        // all creates from node 0 landed on partition 0, starving nodes 1 and 2.
        use apis::process_instance::CreateProcessInstanceResponse as Resp;
        let (node0, node1, node2) = boot_rf3_intake_cluster().await;

        // 12 creates over 3 partitions round-robin -> 4 each, so every partition
        // (and thus every node's leader) must receive instances.
        let mut partitions = std::collections::BTreeSet::new();
        for _ in 0..12 {
            let body = models::ProcessInstanceCreationInstruction::from(
                models::ProcessInstanceCreationInstructionById::new("intake".to_string()),
            );
            let resp = node0
                .create_process_instance_impl(&body)
                .await
                .expect("rest create returns a response");
            let result = match resp {
                Resp::Status200_TheProcessInstanceWasCreated(r) => r,
                other => panic!("rest create through raft should be 200, got {other:?}"),
            };
            let instance_key: u64 = result
                .process_instance_key
                .0
                .parse()
                .expect("numeric instance key");
            partitions.insert(nanobpmn_engine_core::partition_of(instance_key));
        }

        assert_eq!(
            partitions,
            std::collections::BTreeSet::from([0u64, 1, 2]),
            "creates from one gateway must spread across all partitions/nodes, got {partitions:?}"
        );

        for node in [&node0, &node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_job_leased_on_the_killed_leader_re_activates_on_the_new_leader() {
        // s3-test: at-least-once ACROSS failover. A job activated (leased) on the
        // old leader but NOT completed before it dies must NOT be lost: the new
        // leader expires the stale lease (logged ExpireJobs via the Raft tick) and
        // re-offers the job, then completes the instance. Because activation is a
        // LOGGED command, the lease state replicated to the survivors, so the new
        // leader knows the job was outstanding.
        let (node0, node1, node2) = boot_rf3_intake_cluster().await;

        let (instance_key, _c) = node0
            .create_for_stream(Some("intake".into()), None, Default::default())
            .await
            .expect("raft-routed create commits via quorum");
        assert_eq!(nanobpmn_engine_core::partition_of(instance_key), 0);

        // Lease the job on the OLD leader (short timeout) and DO NOT complete it.
        let mut leased = false;
        for _ in 0..200 {
            let jobs = node0
                .activate_for_stream("do-work", "w", 10, 500, None)
                .await;
            if !jobs.is_empty() {
                leased = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(leased, "the job leases on the original leader");

        // Kill the leader before the job is completed.
        for p in 0..3u64 {
            if let Some(part) = node0.raft_registry().get(p) {
                part.raft.shutdown().await.ok();
            }
        }
        let new_leader = wait_new_leader(&node1, &node2, 0).await;

        // The new leader expires the stale lease via a LOGGED ExpireJobs tick (the
        // tick loop is not spawned in-test, so drive one partition tick directly
        // with a far-future `now`), which returns the job to the activatable index.
        let multi_partition = new_leader.engine.topology().num_partitions > 1;
        let far_future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 600_000;
        for _ in 0..50 {
            new_leader
                .tick_partition_via_raft(0, far_future, multi_partition)
                .await;
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        // The re-offered job must now activate on the new leader and complete.
        let mut job_key = None;
        for _ in 0..200 {
            let jobs = new_leader
                .activate_for_stream("do-work", "w", 10, 60_000, None)
                .await;
            if let Some(j) = jobs.into_iter().next() {
                job_key = Some(j.job_key.0.parse::<u64>().expect("numeric job key"));
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let job_key = job_key.expect("the leased-but-uncompleted job re-activates after failover");
        new_leader
            .complete_job_for_stream(job_key, Default::default())
            .await
            .expect("complete commits via the new quorum")
            .wait()
            .await;

        let handle = new_leader
            .engine_handle_for(0)
            .expect("new leader materializes partition 0");
        let mut done = false;
        for _ in 0..400 {
            if handle
                .with(move |journal| journal.engine().is_completed(instance_key))
                .await
            {
                done = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            done,
            "the job survives the leader loss (at-least-once) and the instance completes"
        );

        for node in [&node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_lease_digest_holds_failover_redelivery_until_the_deadline() {
        // ADR 0002 Part B: with `NANOBPMN_REPLICATE_ACTIVATION=digest`, the
        // activation lock is leader-local (NOT replicated), so a follower's replica
        // engine holds an in-flight job as `Created`. The leader periodically
        // broadcasts its held leases; a follower recovers them on promotion so the
        // NEW leader honours the original deadline before redelivering — narrowing
        // the immediate-redelivery window plain leader-local activation opens.
        let (node0, node1, node2) = boot_rf3_intake_cluster_cfg(true).await;

        let (instance_key, _c) = node0
            .create_for_stream(Some("intake".into()), None, Default::default())
            .await
            .expect("raft-routed create commits via quorum");
        assert_eq!(nanobpmn_engine_core::partition_of(instance_key), 0);

        // Lease the job on the leader with a LONG deadline (leader-local; not
        // replicated). The lease lives only in node 0's engine RAM.
        let mut leased = false;
        for _ in 0..200 {
            let jobs = node0
                .activate_for_stream("do-work", "w", 10, 600_000, None)
                .await;
            if !jobs.is_empty() {
                leased = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(leased, "the job leases on the original leader");
        assert_eq!(
            node0.led_partitions(),
            vec![0],
            "node 0 leads only partition 0"
        );

        // Broadcast the held lease to the followers (fire-and-forget over the
        // Falcon protocol), then give it a moment to be recorded on the peers.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        node0.run_lease_digest(now).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Kill the leader while the job is still leased (and not completed).
        for p in 0..3u64 {
            if let Some(part) = node0.raft_registry().get(p) {
                part.raft.shutdown().await.ok();
            }
        }
        let new_leader = wait_new_leader(&node1, &node2, 0).await;

        // Wait until the committed create has replicated to the new leader's
        // replica engine (so there is a `Created` job for the digest to recover).
        let handle = new_leader
            .engine_handle_for(0)
            .expect("new leader materializes partition 0");
        let mut present = false;
        for _ in 0..400 {
            if handle
                .with(move |journal| {
                    journal
                        .engine()
                        .state()
                        .instances
                        .contains_key(&instance_key)
                })
                .await
            {
                present = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(present, "the create replicates to the new leader");

        // Recover the digest on the new leader. The in-flight job transitions
        // Created -> Activated until the ORIGINAL deadline, so it is NOT
        // immediately redeliverable.
        let recover_now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        new_leader.run_lease_digest(recover_now).await;

        let jobs = new_leader
            .activate_for_stream("do-work", "w", 10, 60_000, None)
            .await;
        assert!(
            jobs.is_empty(),
            "the recovered lease holds redelivery until the deadline (digest narrowed the window)"
        );

        // Past the deadline, the normal expiry tick reclaims the recovered lease
        // and the job is re-offered (at-least-once still holds).
        let multi_partition = new_leader.engine.topology().num_partitions > 1;
        let far_future = recover_now + 1_200_000;
        for _ in 0..50 {
            new_leader
                .tick_partition_via_raft(0, far_future, multi_partition)
                .await;
            let jobs = new_leader
                .activate_for_stream("do-work", "w", 10, 60_000, None)
                .await;
            if !jobs.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let reoffered = new_leader
            .engine_handle_for(0)
            .expect("new leader materializes partition 0")
            .with(move |journal| {
                journal
                    .engine()
                    .state()
                    .instances
                    .contains_key(&instance_key)
            })
            .await;
        assert!(
            reoffered,
            "after the deadline the job is reclaimed and remains available (at-least-once)"
        );

        for node in [&node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leader_durable_acks_on_the_sole_voter_and_ships_to_learners() {
        // ADR 0003: with `NANOBPMN_REPLICATION=leader-durable`, the partition leader
        // forms its group as the SOLE voter and the other replicas as learners. A
        // write therefore acks on the leader's own durable append+apply (quorum = 1,
        // no follower round-trip) yet still ships to the learners asynchronously, so
        // their replica engines catch up in the background.
        let (node0, node1, node2) = boot_rf3_leader_durable_cluster().await;

        // Membership for partition 0 (led by node 0): exactly one voter (node 0) and
        // two learners (nodes 1 and 2). This is what takes follower quorum off the
        // critical path.
        let part0 = node0
            .raft_registry()
            .get(0)
            .expect("node 0 hosts partition 0");
        let metrics = part0.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership().clone();
        let voters: Vec<u64> = membership.voter_ids().collect();
        let learners: Vec<u64> = membership.learner_ids().collect();
        assert_eq!(
            voters,
            vec![0],
            "only the leader is a voter in leader-durable"
        );
        assert_eq!(
            {
                let mut l = learners.clone();
                l.sort_unstable();
                l
            },
            vec![1, 2],
            "the other two replicas are learners"
        );

        // A create acks on the leader alone (no follower quorum gates it).
        let (instance_key, _c) = node0
            .create_for_stream(Some("intake".into()), None, Default::default())
            .await
            .expect("leader-durable create acks on the sole voter");
        assert_eq!(nanobpmn_engine_core::partition_of(instance_key), 0);

        // Despite acking without quorum, the entry is shipped to the learners in the
        // background: a learner's replica engine eventually materializes the
        // instance, proving async log shipping (durability/catch-up) still happens.
        let learner_handle = node1
            .engine_handle_for(0)
            .expect("node 1 materializes a replica engine for partition 0");
        let mut replicated = false;
        for _ in 0..400 {
            if learner_handle
                .with(move |journal| {
                    journal
                        .engine()
                        .state()
                        .instances
                        .contains_key(&instance_key)
                })
                .await
            {
                replicated = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            replicated,
            "the acked entry ships to the learner asynchronously (leader-durable still replicates)"
        );

        for node in [&node0, &node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leader_durable_auto_recovers_a_leaderless_partition_without_manual_intervention() {
        // ADR 0003 (option-2 follow-on): in leader-durable mode a partition group
        // has a single voter, so openraft cannot elect when that leader is lost.
        // The leader-durable recovery supervisor fills the gap: it detects the
        // leaderless partition and the deterministic surviving successor app-promotes
        // itself, seeded from the replica engine that already holds the shipped
        // state — restoring write availability with NO manual intervention.
        let (node0, node1, node2, mut handles) = boot_rf3_intake_cluster_cfg2(false, true).await;

        // Create an instance on partition 0 (led by node 0, the sole voter). It acks
        // on node 0 and ships to the learners; wait until node 1's replica engine has
        // it, so the post-promotion state carry-over is observable.
        let (instance_key, _c) = node0
            .create_for_stream(Some("intake".into()), None, Default::default())
            .await
            .expect("leader-durable create acks on the sole voter");
        assert_eq!(nanobpmn_engine_core::partition_of(instance_key), 0);

        let node1_p0 = node1
            .engine_handle_for(0)
            .expect("node 1 materializes a replica engine for partition 0");
        let mut shipped = false;
        for _ in 0..400 {
            if node1_p0
                .with(move |journal| {
                    journal
                        .engine()
                        .state()
                        .instances
                        .contains_key(&instance_key)
                })
                .await
            {
                shipped = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            shipped,
            "the create ships to node 1 (learner) before the leader dies"
        );

        // Kill node 0 entirely: stop its Raft groups AND mark it unreachable from
        // the survivors. Aborting node 0's serve task is not enough on its own —
        // `axum::serve` drives each accepted connection on a detached task that
        // outlives the aborted listener, so a survivor's already-established uplink
        // to node 0 keeps reporting `is_connected`. The fault-injection seam makes
        // node 0 unreachable from each survivor's `PeerSet`, faithfully simulating
        // the post-mortem state (links dropped, redials refused) the recovery
        // failure detector keys on.
        for p in 0..3u64 {
            if let Some(part) = node0.raft_registry().get(p) {
                part.raft.shutdown().await.ok();
            }
        }
        handles[0].abort();
        node1.peers.fail_node(0).await;
        node2.peers.fail_node(0).await;

        // Drive the recovery supervisor on the survivors (the spawned loop is not
        // started in-test). node 1 is the deterministic successor for partition 0
        // (replicas_of(0) = [0,1,2]; node 0 is down), so it self-promotes; node 2
        // sees node 1 alive and stands down. Use grace_ticks = 1 for a prompt test.
        let mut state1 = RecoveryState::default();
        let mut state2 = RecoveryState::default();
        let mut promoted = false;
        for _ in 0..200 {
            node1.leader_durable_recovery_tick(1, &mut state1).await;
            node2.leader_durable_recovery_tick(1, &mut state2).await;
            if node1
                .raft_registry()
                .get(0)
                .and_then(|part| part.raft.metrics().borrow().current_leader)
                == Some(1)
            {
                promoted = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            promoted,
            "node 1 auto-promotes itself leader of the leaderless partition 0"
        );
        assert!(
            node1.led_partitions().contains(&0),
            "serving follows the auto-promoted leadership"
        );
        // node 2 must NOT also promote (single promoter — deterministic successor).
        assert_ne!(
            node2.raft_registry().get(0).and_then(|part| part
                .raft
                .metrics()
                .borrow()
                .current_leader),
            Some(2),
            "only the deterministic successor promotes; node 2 stands down"
        );

        // Write availability is restored: the new leader serves writes for partition
        // 0. Activate and complete the in-flight job (carried over from the dead
        // leader's shipped state) and confirm the instance completes on node 1.
        let mut job_key = None;
        for _ in 0..200 {
            let jobs = node1
                .activate_for_stream("do-work", "w", 10, 60_000, None)
                .await;
            if let Some(j) = jobs.into_iter().next() {
                job_key = Some(j.job_key.0.parse::<u64>().expect("numeric job key"));
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let job_key =
            job_key.expect("the auto-promoted leader serves activateJobs for partition 0");
        node1
            .complete_job_for_stream(job_key, Default::default())
            .await
            .expect("the auto-promoted leader commits the completion")
            .wait()
            .await;

        let mut completed = false;
        for _ in 0..400 {
            if node1_p0
                .with(move |journal| journal.engine().is_completed(instance_key))
                .await
            {
                completed = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            completed,
            "the instance completes on the auto-promoted leader (write availability recovered)"
        );

        for node in [&node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
        for h in handles.drain(..) {
            h.abort();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rejoined_owner_reclaims_a_partition_a_peer_leads() {
        // Reclaim on rejoin. Routing is static (`leader_of == owner_of`), so every
        // create/activation for a partition is sent to its OWNER regardless of which
        // node actually leads the Raft group. After an outage a survivor self-promotes
        // the owner's partition (failover); when the owner comes back, a race can let
        // that failover leader's replication reach the owner FIRST, so the owner
        // rebuilds as a mere learner reporting the peer as leader. Static routing then
        // funnels the partition's traffic to the owner, which `leader_reject`s it as a
        // learner: the partition takes zero creates and cannot drain (observed on a
        // rejoined node: a subset of its owned partitions stranded their backlog). The
        // recovery supervisor must treat "a peer leads a partition I OWN" as not-live
        // so the owner reclaims (self-promotes) it. This proves that reclaim fires.
        let (node0, node1, node2, mut handles) = boot_rf3_intake_cluster_cfg2(false, true).await;

        // Seed partition-0 state and ship it to node 1 so the survivor can promote
        // from a warm replica engine.
        let (instance_key, _c) = node0
            .create_for_stream(Some("intake".into()), None, Default::default())
            .await
            .expect("leader-durable create acks on the sole voter");
        assert_eq!(nanobpmn_engine_core::partition_of(instance_key), 0);
        let node1_p0 = node1
            .engine_handle_for(0)
            .expect("node 1 materializes a replica engine for partition 0");
        let mut shipped = false;
        for _ in 0..400 {
            if node1_p0
                .with(move |journal| {
                    journal
                        .engine()
                        .state()
                        .instances
                        .contains_key(&instance_key)
                })
                .await
            {
                shipped = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(shipped, "the create ships to node 1 before the outage");

        // Outage: node 0's Raft groups go down and it is fault-injected unreachable
        // from the survivors so node 1 becomes the deterministic successor. Unlike the
        // auto-recovery test we do NOT abort node 0's serve task: the owner must stay
        // dial-able so it can rejoin as a learner when it comes back (modelling the
        // rejoin race, not a permanent death).
        for p in 0..3u64 {
            if let Some(part) = node0.raft_registry().get(p) {
                part.raft.shutdown().await.ok();
            }
        }
        node1.peers.fail_node(0).await;
        node2.peers.fail_node(0).await;

        // Survivors run recovery; node 1 self-promotes partition 0 (node 0 is owner but
        // unreachable, so node 1 is the successor). node 0's tick is NOT driven during
        // the outage.
        let mut state1 = RecoveryState::default();
        let mut state2 = RecoveryState::default();
        let mut promoted = false;
        for _ in 0..200 {
            node1.leader_durable_recovery_tick(1, &mut state1).await;
            node2.leader_durable_recovery_tick(1, &mut state2).await;
            if node1
                .raft_registry()
                .get(0)
                .and_then(|part| part.raft.metrics().borrow().current_leader)
                == Some(1)
            {
                promoted = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(promoted, "node 1 fails over partition 0 during the outage");

        // Rejoin: node 0 comes back. Restore reachability, then reproduce the losing
        // side of the race — node 1's failover leadership reaches node 0 first, so
        // node 0 rebuilds as a LEARNER of node 1 (adopts epoch (1,1)) and its metrics
        // report node 1 as the leader of a partition node 0 OWNS.
        node1.peers.heal_node(0).await;
        node2.peers.heal_node(0).await;
        node0.handle_promotion(0, 1, 1).await;
        let node0_addr = node1
            .engine
            .topology()
            .peer_addr(0)
            .expect("node 0 address")
            .to_string();
        node1
            .raft_registry()
            .get(0)
            .expect("node 1 leads partition 0")
            .add_learner(0, openraft::BasicNode::new(node0_addr))
            .await
            .ok();
        let mut learner_ready = false;
        for _ in 0..600 {
            if node0
                .raft_registry()
                .get(0)
                .and_then(|part| part.raft.metrics().borrow().current_leader)
                == Some(1)
            {
                learner_ready = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            learner_ready,
            "the rejoined owner ends up a learner reporting the peer as leader (the strand)"
        );

        // Drive the owner's recovery tick. WITHOUT the reclaim fix it would see a
        // reachable peer leader and reset the miss counter forever, leaving the
        // partition stranded. WITH the fix, a peer leading an OWNED partition counts as
        // not-live, so the owner self-promotes at the next epoch and reclaims it.
        let mut state0 = RecoveryState::default();
        // The recovery supervisor's `established` set persists for the whole process
        // life: node 0 formed partition 0 before its outage, so it is already marked
        // established when the reclaim ticks run. (Seeding it faithfully; otherwise a
        // reclaim tick that momentarily reads a leaderless learner would trip the
        // never-established cold-start guard.)
        state0.established.insert(0u64);
        let mut reclaimed = false;
        for _ in 0..600 {
            node0.leader_durable_recovery_tick(1, &mut state0).await;
            if node0
                .raft_registry()
                .get(0)
                .and_then(|part| part.raft.metrics().borrow().current_leader)
                == Some(0)
            {
                reclaimed = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            reclaimed,
            "the rejoined owner reclaims (self-promotes) the partition a peer was leading"
        );
        assert!(
            node0.led_partitions().contains(&0),
            "serving follows the reclaimed leadership on the owner"
        );
        // The reclaim used a strictly higher epoch than the failover leader's (1,1) ->
        // (2,0), so the fence resolves cleanly in the owner's favour.
        assert_eq!(
            node0.promotion_epoch.lock().unwrap().get(&0).copied(),
            Some((2, 0)),
            "the owner reclaims at incumbent_epoch + 1, naming itself"
        );

        for node in [&node0, &node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
        for h in handles.drain(..) {
            h.abort();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rejoined_owner_solicits_incumbent_epoch_for_single_round_reclaim() {
        // Option A wiring: the reclaim epoch fence only resolves in one round if the
        // rejoining owner knows the incumbent (failover) epoch before it promotes.
        // That epoch is in-memory and reset on restart, and the failover leader's
        // original Promote was broadcast while the owner was DOWN — so on rejoin the
        // owner's map is empty and, without help, it would promote at epoch 1, lose
        // the fence to the higher-epoch incumbent, and climb one epoch per tick (the
        // load-sensitive leader_reject storm). The owner therefore SOLICITS the
        // incumbent on rejoin; the incumbent re-announces the promotions it leads;
        // the owner adopts the epoch and its next promote lands at incumbent + 1.
        // This proves the solicit -> answer -> adopt round-trip over the real peer
        // links, isolated from the raft failover dance.
        let (node0, node1, node2, mut handles) = boot_rf3_intake_cluster_cfg2(false, true).await;

        // The incumbent: node 1 becomes the REAL failover raft leader of partition 0
        // at epoch 1 (as a failover would when node 0 was down) — a fresh single-voter
        // group it actually leads, not just an app-map entry. The leadership gate on
        // `answer_promotion_solicit` requires genuine raft leadership: a node that only
        // has a stale `promotion_epoch` entry (e.g. one demoted by a later hand-off)
        // must NOT advertise itself as leader, so the incumbent here must truly lead.
        assert_eq!(node1.next_promotion_epoch(0), 1);
        assert_eq!(
            node1.promotion_epoch.lock().unwrap().get(&0).copied(),
            Some((1, 1)),
            "node 1 is the incumbent leader of partition 0 at epoch 1"
        );
        node1.promote_partition(0, 1).await;
        let mut leads = false;
        for _ in 0..300 {
            if node1.i_lead_raft(0) {
                leads = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(leads, "the incumbent genuinely raft-leads partition 0");

        // Model the rejoined owner: promote_partition broadcast a Promote to node 0,
        // which it may have adopted. Wait for that to drain, then RESET node 0's view
        // so it has NOT heard the incumbent epoch — exactly the post-restart state
        // (its in-memory map was wiped). The solicit round-trip below must re-deliver.
        for _ in 0..300 {
            if node0.promotion_epoch.lock().unwrap().get(&0).copied() == Some((1, 1)) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        node0.promotion_epoch.lock().unwrap().remove(&0);

        // Solicit the incumbent. node 1 STILL raft-leads, so the leadership gate lets
        // it answer with a Promote for partition 0; node 0's falcon handler adopts it
        // via handle_promotion.
        node0.solicit_promotions_from(1).await;

        let mut adopted = false;
        for _ in 0..300 {
            if node0.promotion_epoch.lock().unwrap().get(&0).copied() == Some((1, 1)) {
                adopted = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            adopted,
            "the owner adopts the incumbent epoch (1,1) from the solicited re-announcement"
        );

        // With the incumbent epoch adopted, the owner's next promote lands at
        // incumbent + 1 (epoch 2, naming itself) — winning the fence in one round.
        assert_eq!(
            node0.next_promotion_epoch(0),
            2,
            "the owner reclaims at incumbent_epoch + 1 after soliciting, not epoch 1"
        );

        for node in [&node0, &node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
        for h in handles.drain(..) {
            h.abort();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn solicit_reply_is_gated_on_real_raft_leadership_not_a_stale_epoch_entry() {
        // Regression for the leadership hand-off: after leadership moves off a node
        // (e.g. it was demoted to a learner), its in-memory `promotion_epoch` map may
        // STILL name it as the leader of a partition. If it answered a solicit on that
        // stale entry it would make the returning owner adopt a dead epoch and could
        // re-demote the genuine leader — undoing the hand-off. `answer_promotion_solicit`
        // must therefore gate on ACTUAL raft leadership (`i_lead_raft`), not the map.
        let (node0, node1, node2, mut handles) = boot_rf3_intake_cluster_cfg2(false, true).await;

        // Forge a stale entry on node 1 claiming it leads partition 0 at epoch 1,
        // WITHOUT it ever raft-leading partition 0 (node 0 is the real owner/leader).
        node1.promotion_epoch.lock().unwrap().insert(0, (1, 1));
        assert!(
            !node1.i_lead_raft(0),
            "node 1 does not actually raft-lead partition 0 despite the forged epoch entry"
        );
        node0.promotion_epoch.lock().unwrap().remove(&0);

        // Solicit node 1. The gate must suppress the reply, so node 0 learns nothing.
        node0.solicit_promotions_from(1).await;

        for _ in 0..25 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            assert!(
                node0.promotion_epoch.lock().unwrap().get(&0).is_none(),
                "the owner must NOT adopt an epoch from a node that only has a stale map \
                 entry and does not actually raft-lead the partition"
            );
        }

        for node in [&node0, &node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
        for h in handles.drain(..) {
            h.abort();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leadership_handoff_transfers_a_partition_back_to_the_returning_owner() {
        // The leadership hand-off (NANOBPMN_RECLAIM_HANDOFF): when a rejoining owner
        // reclaims a partition led by a reachable failover incumbent, the incumbent
        // hands leadership back via an openraft membership change — ONE raft lineage
        // throughout — instead of the owner forming a competing group. This proves
        // the end-to-end incumbent path: add the owner as a learner, catch it up,
        // change_membership the vote to it, step down, and advance the epoch fence.
        let (node0, node1, node2, mut handles) = boot_rf3_intake_cluster_cfg2(false, true).await;

        // node 1 is the failover incumbent: it genuinely raft-leads partition 0 at
        // epoch 1 (node 0, the static owner, is treated as having been down). The
        // promote broadcast pulls node 0 in as a receiver of node 1's group.
        assert_eq!(node1.next_promotion_epoch(0), 1);
        node1.promote_partition(0, 1).await;
        let mut leads = false;
        for _ in 0..300 {
            if node1.i_lead_raft(0) {
                leads = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(leads, "the incumbent genuinely raft-leads partition 0");

        // The returning owner requests the hand-off (simulated: invoke the
        // incumbent's request handler directly with node 0 as the requester).
        let owner_addr = node0
            .engine
            .topology()
            .peer_addr(0)
            .expect("node 0 has an address")
            .to_string();
        node1.handle_handoff_request(0, 0, owner_addr).await;

        // The owner ends up the sole voter/leader of partition 0.
        let mut owner_leads = false;
        for _ in 0..400 {
            if node0.i_lead_raft(0) {
                owner_leads = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            owner_leads,
            "the returning owner leads partition 0 after the hand-off"
        );

        // The incumbent stepped down (no longer raft-leads) and both nodes fence at
        // (incumbent_epoch + 1, owner) so a stale promote can't undo the transfer.
        assert!(
            !node1.i_lead_raft(0),
            "the incumbent stepped down to a learner after handing off"
        );
        assert_eq!(
            node1.promotion_epoch.lock().unwrap().get(&0).copied(),
            Some((2, 0)),
            "the incumbent advanced its fence to (2, owner)"
        );
        let mut owner_fence = None;
        for _ in 0..200 {
            owner_fence = node0.promotion_epoch.lock().unwrap().get(&0).copied();
            if owner_fence == Some((2, 0)) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(
            owner_fence,
            Some((2, 0)),
            "the owner adopted the (2, owner) fence from the hand-off completion"
        );

        for node in [&node0, &node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
        for h in handles.drain(..) {
            h.abort();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_tick_hands_off_a_boot_deferred_partition_from_the_epoch_map() {
        // Phase E (boot-as-receiver): a rejoining owner that deferred forming its own
        // group for an owned partition (a peer leads it) is a RECEIVER — its local
        // raft member has no `current_leader` yet, but the boot probe/solicit adopted
        // the incumbent epoch into the app map. The recovery tick must derive the
        // incumbent FROM THE MAP (not just local `current_leader`) and drive an
        // openraft leadership hand-off, NOT self-promote a competing group (the
        // two-lineage election war). This proves the recovery tick reclaims a
        // boot-deferred partition end-to-end via hand-off, with no fresh self-promote.
        let (node0, node1, node2, mut handles) = boot_rf3_intake_cluster_cfg2(false, true).await;

        // node 1 is the failover incumbent: it genuinely raft-leads partition 0 at
        // epoch 1 (node 0, the static owner, treated as having been down).
        assert_eq!(node1.next_promotion_epoch(0), 1);
        node1.promote_partition(0, 1).await;
        let mut leads = false;
        for _ in 0..300 {
            if node1.i_lead_raft(0) {
                leads = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(leads, "the incumbent genuinely raft-leads partition 0");

        // The promote broadcast makes node 0 adopt (1,1) and rebuild as a receiver —
        // exactly the Phase E boot-deferred state: node 0 does NOT lead partition 0,
        // its map names the incumbent, and it never formed a competing group.
        for _ in 0..300 {
            if node0.promotion_epoch.lock().unwrap().get(&0).copied() == Some((1, 1)) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(
            node0.promotion_epoch.lock().unwrap().get(&0).copied(),
            Some((1, 1)),
            "the returning owner adopted the incumbent epoch as a receiver"
        );
        assert!(
            !node0.i_lead_raft(0),
            "the returning owner is a receiver, not a competing leader, pre-reclaim"
        );

        // Drive node 0's recovery tick. With reclaim-via-handoff on, it derives the
        // incumbent (node 1) from the map, requests the hand-off, and node 1 transfers
        // leadership via an openraft membership change — no self-promote.
        let mut state0 = RecoveryState::default();
        let mut owner_leads = false;
        for _ in 0..400 {
            node0.leader_durable_recovery_tick(3, &mut state0).await;
            if node0.i_lead_raft(0) {
                owner_leads = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            owner_leads,
            "the recovery tick reclaims the boot-deferred partition via hand-off"
        );

        // ONE lineage: the incumbent stepped down, and the fence advanced to
        // (incumbent_epoch + 1, owner) — never a fresh self-promote at epoch 1.
        assert!(
            !node1.i_lead_raft(0),
            "the incumbent stepped down to a learner after handing off"
        );
        assert_eq!(
            node0
                .promotion_epoch
                .lock()
                .unwrap()
                .get(&0)
                .map(|&(_, l)| l),
            Some(0),
            "the fence names the owner (node 0) as leader after the hand-off"
        );
        assert!(
            node0
                .promotion_epoch
                .lock()
                .unwrap()
                .get(&0)
                .map(|&(e, _)| e >= 2)
                .unwrap_or(false),
            "the owner reclaimed at incumbent_epoch + 1 (>= 2), not a fresh epoch-1 self-promote"
        );

        for node in [&node0, &node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
        for h in handles.drain(..) {
            h.abort();
        }
    }

    #[test]
    fn handoff_write_pause_parses_env_with_a_default_and_a_disable() {
        use std::time::Duration;
        // Absent / blank / non-numeric -> the on-by-default ceiling.
        assert_eq!(
            parse_handoff_write_pause(None),
            Duration::from_millis(HANDOFF_WRITE_PAUSE_DEFAULT_MS),
            "absent -> default (on by default)"
        );
        assert_eq!(
            parse_handoff_write_pause(Some("   ")),
            Duration::from_millis(HANDOFF_WRITE_PAUSE_DEFAULT_MS),
            "blank -> default"
        );
        assert_eq!(
            parse_handoff_write_pause(Some("nope")),
            Duration::from_millis(HANDOFF_WRITE_PAUSE_DEFAULT_MS),
            "non-numeric -> default"
        );
        // Explicit values, including 0 which disables the completion pause.
        assert_eq!(
            parse_handoff_write_pause(Some("500")),
            Duration::from_millis(500)
        );
        assert_eq!(
            parse_handoff_write_pause(Some(" 1500 ")),
            Duration::from_millis(1500),
            "surrounding whitespace is trimmed"
        );
        assert_eq!(
            parse_handoff_write_pause(Some("0")),
            Duration::ZERO,
            "0 disables the completion pause (Zeebe-style best-effort)"
        );
    }

    #[test]
    fn evaluate_catchup_succeeds_extends_on_progress_and_aborts_on_stall_or_ceiling() {
        use std::time::{Duration, Instant};
        let threshold = HANDOFF_LAG_THRESHOLD;
        let stall = Duration::from_millis(8000);
        let t0 = Instant::now();
        let soft = t0 + Duration::from_millis(30000);
        let hard = t0 + Duration::from_millis(180000);

        // Caught up (lag within threshold) -> Done, regardless of matched.
        {
            let mut best = None;
            let mut bytes = 0u64;
            let mut adv = t0;
            assert_eq!(
                evaluate_catchup(
                    Some(threshold),
                    Some(100),
                    None,
                    &mut best,
                    &mut bytes,
                    &mut adv,
                    t0,
                    soft,
                    hard,
                    threshold,
                    stall,
                ),
                CatchupStep::Done
            );
        }

        // Snapshot install in flight (matched None, no bytes yet, big lag): NOT
        // stalled even far past the stall grace, because progress has not begun —
        // bounded only by the deadlines. The old-10s-cutoff bug the change fixes.
        {
            let mut best = None;
            let mut bytes = 0u64;
            let mut adv = t0;
            assert_eq!(
                evaluate_catchup(
                    Some(1_000_000),
                    None,
                    None,
                    &mut best,
                    &mut bytes,
                    &mut adv,
                    t0 + Duration::from_millis(20000),
                    soft,
                    hard,
                    threshold,
                    stall,
                ),
                CatchupStep::Continue,
                "a long-but-still-installing learner (no progress yet) is not killed early"
            );
        }

        // Snapshot bytes streaming: cumulative bytes advance -> Continue, and the
        // stall clock resets on the byte advance even though matched is still None.
        {
            let mut best = None;
            let mut bytes = 50_000_000u64;
            let mut adv = t0;
            let now = t0 + Duration::from_millis(7000);
            assert_eq!(
                evaluate_catchup(
                    Some(1_000_000),
                    None,
                    Some(90_000_000),
                    &mut best,
                    &mut bytes,
                    &mut adv,
                    now,
                    soft,
                    hard,
                    threshold,
                    stall,
                ),
                CatchupStep::Continue
            );
            assert_eq!(bytes, 90_000_000, "best bytes advanced");
            assert_eq!(adv, now, "the stall clock reset on the byte advance");
        }

        // Snapshot-transfer-aware EXTENSION: past the soft ceiling but bytes are
        // still advancing (within the stall grace) -> Continue, not Abort. This is
        // the enhancement: a large install that outlasts the soft ceiling keeps
        // going toward the hard cap instead of being guillotined.
        {
            let mut best = None;
            let mut bytes = 100_000_000u64;
            let now = soft + Duration::from_millis(5000);
            let mut adv = now; // bytes just advanced -> actively streaming
            assert_eq!(
                evaluate_catchup(
                    Some(1_000_000),
                    None,
                    Some(120_000_000),
                    &mut best,
                    &mut bytes,
                    &mut adv,
                    now,
                    soft,
                    hard,
                    threshold,
                    stall,
                ),
                CatchupStep::Continue,
                "an actively-streaming install extends past the soft ceiling"
            );
        }

        // Past the soft ceiling with NO install bytes (plain log-tail catch-up)
        // -> abort at the soft ceiling (only snapshot installs get the extension).
        {
            let mut best = Some(1200u64);
            let mut bytes = 0u64;
            let now = soft + Duration::from_millis(1);
            let mut adv = now; // matched just advanced (not a stall) yet no bytes
            assert_eq!(
                evaluate_catchup(
                    Some(200),
                    Some(1201),
                    None,
                    &mut best,
                    &mut bytes,
                    &mut adv,
                    now,
                    soft,
                    hard,
                    threshold,
                    stall,
                ),
                CatchupStep::Abort("learner catch-up ceiling exceeded"),
                "a non-install catch-up is not extended past the soft ceiling"
            );
        }

        // Tail streaming: matched advances -> Continue, and last_advance resets so
        // the stall clock restarts from each advance.
        {
            let mut best = Some(500u64);
            let mut bytes = 0u64;
            let mut adv = t0;
            let now = t0 + Duration::from_millis(7000);
            assert_eq!(
                evaluate_catchup(
                    Some(200),
                    Some(1200),
                    None,
                    &mut best,
                    &mut bytes,
                    &mut adv,
                    now,
                    soft,
                    hard,
                    threshold,
                    stall,
                ),
                CatchupStep::Continue
            );
            assert_eq!(best, Some(1200), "best matched advanced");
            assert_eq!(adv, now, "the stall clock reset on the advance");
        }

        // Post-install stall: progress began (matched Some) but has not advanced
        // for >= stall_grace -> abort EARLY (free the write-pause), before the soft
        // ceiling. Also fires for a wedged snapshot transfer (bytes stop flowing).
        {
            let mut best = Some(1200u64);
            let mut bytes = 0u64;
            let mut adv = t0;
            assert_eq!(
                evaluate_catchup(
                    Some(200),
                    Some(1200),
                    None,
                    &mut best,
                    &mut bytes,
                    &mut adv,
                    t0 + stall,
                    soft,
                    hard,
                    threshold,
                    stall,
                ),
                CatchupStep::Abort("learner catch-up stalled")
            );
        }

        // Wedged snapshot transfer: bytes began then went quiet for >= stall_grace
        // (matched still None) -> abort (a stuck install must not extend forever).
        {
            let mut best = None;
            let mut bytes = 100_000_000u64;
            let mut adv = t0;
            assert_eq!(
                evaluate_catchup(
                    Some(1_000_000),
                    None,
                    Some(100_000_000), // no advance since best_bytes
                    &mut best,
                    &mut bytes,
                    &mut adv,
                    t0 + stall,
                    soft,
                    hard,
                    threshold,
                    stall,
                ),
                CatchupStep::Abort("learner catch-up stalled"),
                "a wedged install (bytes flatlined) aborts on the stall grace"
            );
        }

        // Absolute HARD cap wins even while actively streaming (safety cap).
        {
            let mut best = None;
            let mut bytes = 100_000_000u64;
            let mut adv = hard; // "just advanced" — not a stall
            assert_eq!(
                evaluate_catchup(
                    Some(1_000_000),
                    None,
                    Some(200_000_000),
                    &mut best,
                    &mut bytes,
                    &mut adv,
                    hard,
                    soft,
                    hard,
                    threshold,
                    stall,
                ),
                CatchupStep::Abort("learner catch-up ceiling exceeded"),
                "the hard cap bounds even an actively-streaming install"
            );
        }
    }

    #[test]
    fn catchup_hold_engages_while_a_lagging_peer_advances_and_releases_when_caught_up_or_stalled() {
        use std::collections::HashMap;
        use std::time::{Duration, Instant};
        let threshold = 60_000u64;
        let grace = Duration::from_secs(5);
        let t0 = Instant::now();

        // A peer lagging above the threshold, first sighting -> engaged (a fresh
        // observation counts as advancing for the first grace window).
        {
            let mut prog: HashMap<(u64, u64), (u128, Instant)> = HashMap::new();
            let obs = vec![((0u64, 18u64), 800_000u64, 1_000u128)];
            let (active, max_lag) = catchup_hold_active(&obs, &mut prog, threshold, grace, t0);
            assert!(
                active,
                "a freshly-seen bulk-lagging peer holds the throttle"
            );
            assert_eq!(max_lag, 800_000);
        }

        // A peer within the threshold -> NOT engaged (steady-state learner jitter).
        {
            let mut prog: HashMap<(u64, u64), (u128, Instant)> = HashMap::new();
            let obs = vec![((0u64, 18u64), 64u64, 1_000u128)];
            let (active, _) = catchup_hold_active(&obs, &mut prog, threshold, grace, t0);
            assert!(
                !active,
                "a nearly-caught-up peer does not hold the throttle"
            );
        }

        // Still lagging but ADVANCING across ticks (progress grows) -> stays engaged,
        // and the stall clock resets on each advance.
        {
            let mut prog: HashMap<(u64, u64), (u128, Instant)> = HashMap::new();
            let _ = catchup_hold_active(
                &[((0u64, 18u64), 800_000u64, 1_000u128)],
                &mut prog,
                threshold,
                grace,
                t0,
            );
            let later = t0 + Duration::from_secs(4);
            let (active, _) = catchup_hold_active(
                &[((0u64, 18u64), 600_000u64, 200_000u128)],
                &mut prog,
                threshold,
                grace,
                later,
            );
            assert!(
                active,
                "an advancing bulk catch-up keeps the throttle engaged"
            );
            assert_eq!(
                prog[&(0, 18)].1,
                later,
                "the stall clock reset on the advance"
            );
        }

        // Lagging but STALLED: progress frozen past the grace -> released (a wedged
        // or dead async learner must not pin the throttle in the leader-durable model).
        {
            let mut prog: HashMap<(u64, u64), (u128, Instant)> = HashMap::new();
            let _ = catchup_hold_active(
                &[((0u64, 18u64), 800_000u64, 1_000u128)],
                &mut prog,
                threshold,
                grace,
                t0,
            );
            let much_later = t0 + Duration::from_secs(6);
            let (active, _) = catchup_hold_active(
                &[((0u64, 18u64), 800_000u64, 1_000u128)], // progress unchanged
                &mut prog,
                threshold,
                grace,
                much_later,
            );
            assert!(
                !active,
                "a stalled peer past the grace releases the throttle"
            );
        }

        // A snapshot install (progress carried by cumulative bytes) above threshold,
        // advancing -> engaged, exercising the bytes-driven progress path.
        {
            let mut prog: HashMap<(u64, u64), (u128, Instant)> = HashMap::new();
            let _ = catchup_hold_active(
                &[((1u64, 18u64), 1_000_000u64, 50_000_000u128)],
                &mut prog,
                threshold,
                grace,
                t0,
            );
            let later = t0 + Duration::from_secs(3);
            let (active, max_lag) = catchup_hold_active(
                &[((1u64, 18u64), 1_000_000u64, 90_000_000u128)],
                &mut prog,
                threshold,
                grace,
                later,
            );
            assert!(active, "a streaming snapshot install holds the throttle");
            assert_eq!(max_lag, 1_000_000);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handoff_lease_engages_create_gate_and_a_bounded_completion_pause() {
        // The incumbent hand-off lease must engage BOTH the create write-gate and a
        // bounded completion write-pause (ADR 0019). The create-gate holds for the
        // whole lease (creates steer off the partition); the completion-pause is
        // time-bounded by the pause ceiling and lifts on its own so job-mutation
        // writes resume even if the lease lingers. Release clears both.
        let (node0, _n1, _n2, mut handles) = boot_rf3_intake_cluster_cfg2(false, true).await;

        // Deterministic short pause so the test is fast and non-flaky. The pause is
        // clamped up to the catch-up ceiling (so completions stay paused for the
        // whole catch-up), so shrink the ceiling too or the 120ms pause would be
        // raised to the 30s production ceiling and never lift inside the test.
        let short = std::time::Duration::from_millis(120);
        node0.set_handoff_catchup_ceiling_for_test(short);
        node0.set_handoff_write_pause_for_test(short);

        assert!(
            !node0.handoff_write_gated(7),
            "no gate before acquiring the lease"
        );
        assert!(
            !node0.handoff_completion_paused(7),
            "no pause before acquiring the lease"
        );

        assert!(
            node0.acquire_handoff_lease(7),
            "first acquire wins the lease"
        );
        assert!(
            !node0.acquire_handoff_lease(7),
            "a second concurrent hand-off for the same partition is declined"
        );
        assert!(
            node0.handoff_write_gated(7),
            "create-gate engaged while the lease is held"
        );
        assert!(
            node0.handoff_completion_paused(7),
            "completion-pause engaged inside the bounded window"
        );

        // After the bounded window the completion-pause lifts, but the create-gate
        // (the lease) still holds until release.
        tokio::time::sleep(short + std::time::Duration::from_millis(60)).await;
        assert!(
            !node0.handoff_completion_paused(7),
            "completion-pause lifts once the bounded window elapses"
        );
        assert!(
            node0.handoff_write_gated(7),
            "the create-gate still holds until the lease is released"
        );

        node0.release_handoff_lease(7);
        assert!(
            !node0.handoff_write_gated(7),
            "release lifts the create-gate"
        );
        assert!(
            !node0.handoff_completion_paused(7),
            "release lifts the completion-pause"
        );

        for h in handles.drain(..) {
            h.abort();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leader_durable_split_brain_reconverges_to_one_leader_via_epoch_tiebreak() {
        // A symmetric multi-way split can make TWO survivors each self-promote the
        // same leaderless partition at the SAME epoch (each isolated from the other,
        // so each is its own deterministic successor). The epoch fence must still
        // collapse this back to a single leader: same-epoch ties break by lowest
        // node id, and the tie-loser steps down to a learner of the winner. Without
        // the tiebreak both would keep leading at equal epochs forever (permanent
        // split-brain). This proves reconvergence.
        let (node0, node1, node2, mut handles) = boot_rf3_intake_cluster_cfg2(false, true).await;

        // Seed partition-0 state and let it ship to BOTH survivors' replica engines
        // so each has something to promote from.
        let (instance_key, _c) = node0
            .create_for_stream(Some("intake".into()), None, Default::default())
            .await
            .expect("leader-durable create acks on the sole voter");
        assert_eq!(nanobpmn_engine_core::partition_of(instance_key), 0);
        for node in [&node1, &node2] {
            let h = node
                .engine_handle_for(0)
                .expect("survivor materializes a replica engine for partition 0");
            let mut shipped = false;
            for _ in 0..400 {
                if h.with(move |journal| {
                    journal
                        .engine()
                        .state()
                        .instances
                        .contains_key(&instance_key)
                })
                .await
                {
                    shipped = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            assert!(
                shipped,
                "the create ships to both survivors before the split"
            );
        }

        // Symmetric split: node 0 is gone, AND node 1 / node 2 cannot see each other.
        // Each survivor is therefore its own deterministic successor for partition 0.
        for p in 0..3u64 {
            if let Some(part) = node0.raft_registry().get(p) {
                part.raft.shutdown().await.ok();
            }
        }
        handles[0].abort();
        node1.peers.fail_node(0).await;
        node1.peers.fail_node(2).await;
        node2.peers.fail_node(0).await;
        node2.peers.fail_node(1).await;

        // Drive recovery on both: each promotes partition 0 at epoch 1. (Broadcasts
        // can't cross the split — the peers are fault-injected down — so no
        // cross-delivery happens yet; both end up leaders. That is the split-brain.)
        let mut state1 = RecoveryState::default();
        let mut state2 = RecoveryState::default();
        let leads = |node: &ServerImpl, who: u64| -> bool {
            node.raft_registry()
                .get(0)
                .and_then(|part| part.raft.metrics().borrow().current_leader)
                == Some(who)
        };
        let mut both = false;
        for _ in 0..200 {
            node1.leader_durable_recovery_tick(1, &mut state1).await;
            node2.leader_durable_recovery_tick(1, &mut state2).await;
            if leads(&node1, 1) && leads(&node2, 2) {
                both = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            both,
            "the symmetric split produces two equal-epoch leaders (split-brain to be resolved)"
        );
        // Both promoted at epoch 1, each naming itself.
        assert_eq!(
            node1.promotion_epoch.lock().unwrap().get(&0).copied(),
            Some((1, 1))
        );
        assert_eq!(
            node2.promotion_epoch.lock().unwrap().get(&0).copied(),
            Some((1, 2))
        );

        // Heal: each side now learns of the other's promotion (same epoch). Deliver
        // both announcements. The lowest-id winner (node 1) keeps leadership; node 2
        // adopts (1, 1) and steps down to a learner.
        node1.handle_promotion(0, 1, 2).await; // tie-loser announcement: node 1 keeps lead
        node2.handle_promotion(0, 1, 1).await; // winning announcement: node 2 yields

        // Convergence: both agree the leader is node 1 at epoch 1; node 2 no longer
        // leads partition 0 (it rebuilt as a receiver/learner). One history, one
        // leader — split-brain resolved.
        assert_eq!(
            node1.promotion_epoch.lock().unwrap().get(&0).copied(),
            Some((1, 1)),
            "the lowest-id node keeps leadership"
        );
        assert_eq!(
            node2.promotion_epoch.lock().unwrap().get(&0).copied(),
            Some((1, 1)),
            "the tie-loser adopts the winner's fence (deterministic node-id tiebreak)"
        );
        assert!(leads(&node1, 1), "node 1 remains leader of partition 0");
        assert!(
            !leads(&node2, 2),
            "node 2 stands down — no longer a competing leader"
        );

        for node in [&node1, &node2] {
            for p in 0..3u64 {
                if let Some(part) = node.raft_registry().get(p) {
                    part.raft.shutdown().await.ok();
                }
            }
        }
        for h in handles.drain(..) {
            h.abort();
        }
    }

    /// Regression: the recovery supervisor must NOT promote a partition that has
    /// never been established (no leader ever), even when this node is the
    /// first-reachable replica and its owner is unreachable — that is initial
    /// formation, not failover. Promoting it would race the owner's `initialize`
    /// into a term split-brain that trips openraft's `has_log_id` invariant and
    /// permanently wedges the partition (the cold-start bug this fix targets).
    #[tokio::test]
    async fn leader_durable_recovery_ignores_a_never_established_partition() {
        use crate::raft::RaftPartition;

        // A lone node 0 in an RF=3, 3-partition cluster. It replicates every
        // partition but OWNS only partition 0; partition 1's owner is node 1.
        let topology = cluster::Topology {
            node_id: 0,
            peers: vec![
                "http://127.0.0.1:1".into(),
                "http://127.0.0.1:2".into(),
                "http://127.0.0.1:3".into(),
            ],
            num_partitions: 3,
            replication_factor: 3,
        };
        let journals: Vec<Journal> = topology
            .local_partitions()
            .iter()
            .map(|p| Journal::in_memory_partition(*p))
            .collect();
        let mut node0 = build_server_in_memory(journals, topology);
        node0.replication_mode = ReplicationMode::LeaderDurable;

        // Host a Raft member for partition 1 (node 0 is a mere replica/learner of
        // it) but NEVER form the group — its owner (node 1) is absent, exactly as
        // in a staggered cold start. Its `current_leader` therefore stays `None`.
        let engine = node0.replica_engine_for(1).await;
        let part =
            RaftPartition::bootstrap_member(0, 1, engine, node0.raft_transport(), None, false)
                .await
                .expect("host a replica member for partition 1");
        node0.raft_registry().insert(Arc::new(part));

        // The other replicas are unreachable, so node 0 IS the first-reachable
        // replica for partition 1 (replicas_of(1) = [1, 2, 0]) — without the
        // establishment gate it would self-promote here.
        node0.peers.fail_node(1).await;
        node0.peers.fail_node(2).await;

        let mut state = RecoveryState::default();
        for _ in 0..50 {
            node0.leader_durable_recovery_tick(1, &mut state).await;
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        assert!(
            !state.established.contains(&1),
            "a never-led partition is never marked established"
        );
        assert!(
            node0.promotion_epoch.lock().unwrap().get(&1).is_none(),
            "the never-established partition is NOT promoted (no epoch reserved)"
        );
        assert_eq!(
            node0
                .raft_registry()
                .get(1)
                .and_then(|p| p.raft.metrics().borrow().current_leader),
            None,
            "the partition stays leaderless — recovery leaves initial formation alone"
        );

        if let Some(part) = node0.raft_registry().get(1) {
            part.raft.shutdown().await.ok();
        }
    }

    /// A follower must reclaim hot RAM like its leader: the engine actor a node
    /// builds for a partition it only REPLICATES has to inherit the same spill
    /// tiers as an owned engine, otherwise it pins the entire replicated working
    /// set resident (the RF>1 leader/follower memory imbalance). Regression for
    /// the fix that threads `SpillConfig` into `replica_engine_for`.
    #[tokio::test]
    async fn a_replica_engine_inherits_the_configured_cold_spill() {
        let topology = cluster::Topology {
            node_id: 0,
            peers: vec![
                "http://127.0.0.1:1".into(),
                "http://127.0.0.1:2".into(),
                "http://127.0.0.1:3".into(),
            ],
            num_partitions: 3,
            replication_factor: 3,
        };
        let journals: Vec<Journal> = topology
            .local_partitions()
            .iter()
            .map(|p| Journal::in_memory_partition(*p))
            .collect();
        let mut node0 = build_server_in_memory(journals, topology);
        node0.replication_mode = ReplicationMode::LeaderDurable;

        // Without a configured spill tier, a replica engine has no cold store —
        // exactly the pre-fix behaviour that stranded the follower working set
        // in hot RAM.
        let bare = node0.replica_engine_for(1).await;
        assert!(
            !bare.with(|j| j.cold_spill_configured()).await,
            "no spill configured => replica has no cold store"
        );

        // Configure spill (in-memory store) as startup would, then a freshly
        // built replica engine must carry the cold tier.
        let store =
            Arc::new(varspill::VarSpillStore::open(None).expect("in-memory var-spill store"));
        node0.spill_config = Some(SpillConfig {
            store,
            var: None,
            cold: Some((64 * 1024 * 1024, 32 * 1024 * 1024)),
        });
        let replica = node0.replica_engine_for(2).await;
        assert!(
            replica.with(|j| j.cold_spill_configured()).await,
            "configured spill => replica engine inherits the cold store"
        );
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
        build_server_in_memory(journals, topology)
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
        assert!(
            completed,
            "instance completes after the post-job-completion correlation"
        );
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
            .correlate_message_local("order-placed".into(), key, std::collections::HashMap::new())
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
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::ensure_data_dir;

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

#[cfg(test)]
mod variable_record_projection_tests {
    use super::*;

    fn row(instance_key: u64, scope_key: u64, name: &str, value: &str) -> readstore::VariableRow {
        readstore::VariableRow {
            key: 9000,
            instance_key,
            scope_key,
            name: name.to_string(),
            value: value.to_string(),
            process_definition_id: "loan".to_string(),
            process_definition_key: "42".to_string(),
        }
    }

    /// A Zeebe VARIABLE record carries `scopeKey` distinct from
    /// `processInstanceKey` for a nested scope (sub-process / MI body / child
    /// element instance). Both REST projections must surface that split rather
    /// than collapsing the scope onto the instance.
    #[test]
    fn variable_search_result_reports_the_nested_scope_key() {
        let v = row(1001, 2001, "approved", "true");
        let r = variable_search_result(&v, false);
        assert_eq!(r.scope_key, models::ScopeKey("2001".to_string()));
        assert_eq!(
            r.process_instance_key,
            models::ProcessInstanceKey("1001".to_string())
        );
        assert_ne!(r.scope_key.0, r.process_instance_key.0);
    }

    #[test]
    fn variable_result_reports_the_nested_scope_key() {
        let v = row(1001, 2001, "approved", "true");
        let r = variable_result(&v);
        assert_eq!(r.scope_key, models::ScopeKey("2001".to_string()));
        assert_eq!(
            r.process_instance_key,
            models::ProcessInstanceKey("1001".to_string())
        );
    }

    /// Root-scope variables report `scopeKey == processInstanceKey`, matching
    /// Zeebe's shape for process-level variables.
    #[test]
    fn root_scope_variable_reports_scope_equal_to_instance() {
        let v = row(1001, 1001, "amount", "500");
        let r = variable_search_result(&v, false);
        assert_eq!(r.scope_key.0, r.process_instance_key.0);
        assert_eq!(r.scope_key, models::ScopeKey("1001".to_string()));
    }
}
