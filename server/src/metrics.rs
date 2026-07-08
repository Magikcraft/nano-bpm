//! Lightweight Prometheus instrumentation for the durability hot path.
//!
//! Phase 1 deliberately covers only the **journal writer** and the **commit
//! pipeline** — the part of the system whose behaviour we most need to see when
//! tuning group-commit (batch size), partition count, and the linger window. The
//! `commit_batch_size` histogram is the headline metric: it directly answers
//! "how many writes share one fsync?", which throughput numbers can only hint
//! at.
//!
//! Everything here is off the allocation path: metrics are process-global
//! (`LazyLock`), recording is a handful of atomics (histogram `observe`, counter
//! `inc`), and there are **no labels**, so there is no per-event map lookup or
//! string work. The text encoding cost is paid only when `/metrics` is scraped.

use std::sync::LazyLock;
use std::time::Duration;

use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Registry, TextEncoder};

/// The process-wide metrics registry and the Phase-1 handles.
struct Metrics {
    registry: Registry,
    /// Writes coalesced into each group-commit (one fsync). Batch size ≈ 1 means
    /// the pipeline is serialized upstream and group-commit can't amortize.
    commit_batch_size: Histogram,
    /// Wall time of each `write` + `fsync` group-commit. On macOS `sync_all`
    /// issues `F_FULLFSYNC`, a true media barrier, so this is typically ms-scale.
    fsync_seconds: Histogram,
    /// Time a caller spends awaiting its commit's durability (queueing behind
    /// other commits + the fsync itself). The closed-loop latency clients feel.
    commit_wait_seconds: Histogram,
    /// Group-commits performed (i.e. number of fsyncs).
    commits_total: IntCounter,
    /// Individual durable writes acknowledged (sum of all batch sizes).
    writes_total: IntCounter,
    /// Journal bytes written (pre-fsync), across all partitions.
    bytes_total: IntCounter,
    /// Writes enqueued but not yet fsynced — the live commit-pipeline depth.
    inflight: IntGauge,
    /// In-flight create-payload bytes in the submit→apply window (the engine
    /// creation-mailbox balloon). Published by the mem-pressure sampler tick; the
    /// byte-aware admission gate sheds creates when this crosses its watermark.
    pipeline_bytes: IntGauge,
    /// Cumulative wall time the writer thread spent blocked in `recv` with no
    /// work (idle). Paired with `writer_busy_seconds`, a delta-scrape gives the
    /// writer's duty cycle: `busy / (busy + idle)`. If idle ≈ 0 the single
    /// writer is saturated and is the hard throughput ceiling.
    writer_idle_seconds: prometheus::Counter,
    /// Cumulative wall time the writer thread spent doing work (drain + linger +
    /// serialize + fsync + ack). The non-fsync remainder (`busy − fsync_sum`) is
    /// the writer's CPU cost; if that dominates, the ceiling is CPU not fsync.
    writer_busy_seconds: prometheus::Counter,

    // ---- Phase 2: falcon and protocol metrics ----
    /// Falcon WebSocket frames processed, by frame type.
    stream_frames_total: prometheus::IntCounterVec,
    /// How many times a streaming client stalled waiting for submission credits.
    stream_credit_stalls_total: IntCounter,
    /// Active falcon WebSocket connections.
    stream_connections_active: IntGauge,
    /// Time spent processing each falcon frame (read + apply + reply).
    stream_frame_processing_seconds: Histogram,

    /// Process instance creates, split by protocol (rest vs stream).
    creates_total: prometheus::IntCounterVec,
    /// Job completions, split by protocol (rest vs stream).
    job_completions_total: prometheus::IntCounterVec,
    /// Diagnostic: stream CompleteJob outcomes by decision point, to localize a
    /// load-induced completion freeze (route_forward|route_local|leader_reject|
    /// propose_err|apply_err|forward_ok|forward_err).
    stream_complete_outcome_total: prometheus::IntCounterVec,

    /// Serialized bytes of all uncompacted Raft log entries currently held in the
    /// in-memory log indexes, summed across every owned partition. Under a burst
    /// this is byte-unbounded (snapshot policy counts entries, not bytes), so it
    /// is a prime suspect for the RSS balloon.
    raft_log_bytes: IntGauge,
    /// Count of uncompacted Raft log entries in memory across all owned partitions.
    raft_log_entries: IntGauge,
    /// Resident read-model export backlog bytes (forwarded but not yet projected).
    exporter_queue_bytes: IntGauge,
    /// Approx resident instance variable-payload bytes (burst-balloon attribution).
    resident_var_bytes: IntGauge,
    /// Serialized event bytes queued to the journal writer but not yet fsynced+acked.
    journal_inflight_bytes: IntGauge,

    // ---- Capacity ceilings (the compressor/limiter LEDs, ADR 0013) ----
    /// The limiter "lit LED": 1 while this node is currently pressed against a
    /// capacity ceiling, else 0, labelled by `ceiling` (`throughput` = the
    /// create-processing concurrency / active-backlog limiter; `memory` = the
    /// always-in-circuit memory-safety rails — create-queue depth, exporter
    /// saturation, in-flight pipeline bytes, resident-memory watermark).
    ceiling_active: prometheus::IntGaugeVec,
    /// Cumulative count of ceiling "hits" — incremented on each rising edge
    /// (headroom → at-limit) per `ceiling`. Lets a dashboard show how often the
    /// limiter engaged over a window, like a peak-hold on a gain-reduction meter.
    ceiling_hits_total: prometheus::IntCounterVec,

    // ---- Worker provisioning per job type ----
    /// Activatable (waiting) jobs per `job_type` across all owned partitions —
    /// the depth workers still have to drain.
    job_type_activatable: prometheus::IntGaugeVec,
    /// Live subscribed stream workers per `job_type` (falcon roster).
    job_type_workers: prometheus::IntGaugeVec,
    /// Under-provisioning hint per `job_type`: 1 when jobs are waiting but no
    /// worker is subscribed to drain them (hard starvation), else 0. Pair with
    /// `job_type_activatable` / `job_type_workers` to spot soft under-provisioning
    /// (workers present but backlog growing).
    job_type_starved: prometheus::IntGaugeVec,
    /// Cumulative jobs actually dispatched to a worker per `job_type` — the drain
    /// throughput. Counted where a job is delivered to the worker socket (stream)
    /// or returned to the REST client, so peer-pulled jobs are attributed once, at
    /// the gateway that feeds the worker. Delta-scraping gives the per-type drain
    /// rate D; combined with the backlog level + slope and the server-saturation
    /// signals it answers "are workers the bottleneck for this type, and would more
    /// help?" (Little's Law) rather than just "is the backlog growing?".
    job_type_dispatched_total: prometheus::IntCounterVec,
    /// Per-partition Raft liveness alarm: 1 when the partition's openraft core has
    /// entered `Shutdown` (terminated, e.g. on a storage error) and is no longer
    /// applying, else 0. A stuck-at-1 partition strands its share of instances and
    /// jobs — the signal behind the RF>1 completion-freeze. Labelled by partition.
    raft_partition_shutdown: prometheus::IntGaugeVec,

    /// Per-partition engine-actor (deepthi) heartbeat: `1` while the single-writer
    /// thread is alive, `0` the instant it exits/panics. A stuck-at-0 partition
    /// has a DEAD single writer — every create/complete on it hangs forever (the
    /// sustained-load completion-freeze). Labelled by partition.
    actor_alive: prometheus::IntGaugeVec,
    /// Per-partition cumulative count of engine-actor jobs executed. Flat under
    /// any freeze; its delta is the actor's true throughput. Labelled by partition.
    actor_jobs_total: prometheus::IntGaugeVec,
    /// Per-partition elapsed milliseconds of the engine actor's currently-running
    /// job (`0` when idle/parked). An unbounded climb is the signature of a
    /// *wedged* single writer (stuck inside one command); flat-at-0 with a frozen
    /// `actor_jobs_total` means the stall is upstream (idle actor, no work
    /// arriving). Labelled by partition.
    actor_current_job_ms: prometheus::IntGaugeVec,
    /// Per-partition depth of the engine actor's High (completion/read) queue.
    /// Piling up while `actor_jobs_total` is frozen confirms a wedge with work
    /// queued behind it. Labelled by partition.
    actor_hi_depth: prometheus::IntGaugeVec,
    /// Per-partition depth of the engine actor's Low (creation) queue. Labelled by
    /// partition.
    actor_lo_depth: prometheus::IntGaugeVec,

    /// Cumulative count of admission sheds — one per `createProcessInstance`
    /// rejected by [`admission_shed`](crate::AppServer::admission_shed), labelled
    /// by `reason` (which rail tripped: `create_queue`, `active_backlog`,
    /// `create_backlog`, `exporter`, `pipeline_bytes`, `mem_watermark`). Lets a
    /// dashboard confirm that overload is being *shed* rather than silently
    /// accumulated in memory, and which rail is doing the shedding.
    admission_shed_total: prometheus::IntCounterVec,

    // ---- Admission-ceiling input signals (the numbers behind the LED) ----
    /// Live depth of the submitted-but-not-yet-applied create queue — the
    /// create-apply backlog that holds resident memory under an arrival flood and
    /// the `create_queue` / `create_backlog` shed signal. The single most useful
    /// number for "is the engine gathering creates toward OOM?"; plot against
    /// `nanobpm_admission_limit{limit="create_queue"}`.
    pending_create_queue: IntGauge,
    /// Live active-instance backlog (the read-model exporter's projected
    /// `created − completed`) — the `active_backlog` latency-rail signal. Stays ~0
    /// for fast create→complete workloads; climbs when instances park (absent/slow
    /// workers, timers, waiting events). Plot against
    /// `nanobpm_admission_limit{limit="backlog"}`.
    active_backlog: IntGauge,
    /// Cached resident-memory estimate (refreshed by the mem-pressure tick) that the
    /// `mem_watermark` rail keys off. Plot against
    /// `nanobpm_admission_limit{limit="mem_watermark"}`.
    mem_pressure_bytes: IntGauge,
    /// The configured admission thresholds the ceiling rails trip at, labelled by
    /// `limit` (`backlog`, `create_queue` — counts; `pipeline_bytes`,
    /// `mem_watermark` — bytes; `0` = rail disabled). Reference lines so a dashboard
    /// can show each pressure signal's headroom to its shed point.
    admission_limit: prometheus::IntGaugeVec,
}

static METRICS: LazyLock<Metrics> = LazyLock::new(|| {
    let registry = Registry::new();

    let commit_batch_size = Histogram::with_opts(
        HistogramOpts::new(
            "nanobpm_journal_commit_batch_size",
            "Number of writes coalesced into one group-commit (fsync).",
        )
        .buckets(vec![
            1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0,
        ]),
    )
    .expect("valid histogram opts");

    // ~50µs .. 500ms, covering fast Linux fdatasync through slow macOS F_FULLFSYNC.
    let latency_buckets = vec![
        0.00005, 0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5,
    ];

    let fsync_seconds = Histogram::with_opts(
        HistogramOpts::new(
            "nanobpm_journal_fsync_seconds",
            "Wall time of each journal write+fsync group-commit.",
        )
        .buckets(latency_buckets.clone()),
    )
    .expect("valid histogram opts");

    let commit_wait_seconds = Histogram::with_opts(
        HistogramOpts::new(
            "nanobpm_commit_wait_seconds",
            "Time a caller awaits its commit becoming durable.",
        )
        .buckets(latency_buckets),
    )
    .expect("valid histogram opts");

    let commits_total = IntCounter::new(
        "nanobpm_journal_commits_total",
        "Group-commits (fsyncs) performed.",
    )
    .expect("valid counter");
    let writes_total = IntCounter::new(
        "nanobpm_journal_writes_total",
        "Individual durable writes acknowledged.",
    )
    .expect("valid counter");
    let bytes_total = IntCounter::new(
        "nanobpm_journal_bytes_total",
        "Journal bytes written before fsync.",
    )
    .expect("valid counter");
    let inflight = IntGauge::new(
        "nanobpm_commit_inflight",
        "Durable writes enqueued but not yet fsynced (pipeline depth).",
    )
    .expect("valid gauge");

    let pipeline_bytes = IntGauge::new(
        "nanobpm_pipeline_bytes",
        "In-flight create-payload bytes (engine creation-mailbox balloon).",
    )
    .expect("valid gauge");

    let writer_idle_seconds = prometheus::Counter::new(
        "nanobpm_journal_writer_idle_seconds",
        "Cumulative wall time the journal writer thread was idle (blocked in recv).",
    )
    .expect("valid counter");
    let writer_busy_seconds = prometheus::Counter::new(
        "nanobpm_journal_writer_busy_seconds",
        "Cumulative wall time the journal writer thread was busy (drain+linger+fsync+ack).",
    )
    .expect("valid counter");

    // Phase 2: falcon and protocol metrics
    use prometheus::IntCounterVec;
    use prometheus::Opts;

    let stream_frames_total = IntCounterVec::new(
        Opts::new(
            "nanobpm_stream_frames_total",
            "Falcon frames processed by type.",
        ),
        &["type"],
    )
    .expect("valid counter vec");

    let stream_credit_stalls_total = IntCounter::new(
        "nanobpm_stream_credit_stalls_total",
        "Streaming clients stalled waiting for submission credits.",
    )
    .expect("valid counter");

    let stream_connections_active = IntGauge::new(
        "nanobpm_stream_connections_active",
        "Active falcon WebSocket connections.",
    )
    .expect("valid gauge");

    let stream_frame_processing_seconds = Histogram::with_opts(
        HistogramOpts::new(
            "nanobpm_stream_frame_processing_seconds",
            "Time to process each falcon frame (read+apply+reply).",
        )
        .buckets(vec![
            0.00001, 0.00002, 0.00005, 0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01,
        ]),
    )
    .expect("valid histogram");

    let creates_total = IntCounterVec::new(
        Opts::new(
            "nanobpm_creates_total",
            "Process instance creates by protocol (rest|stream).",
        ),
        &["protocol"],
    )
    .expect("valid counter vec");

    let job_completions_total = IntCounterVec::new(
        Opts::new(
            "nanobpm_job_completions_total",
            "Job completions by protocol (rest|stream).",
        ),
        &["protocol"],
    )
    .expect("valid counter vec");

    let stream_complete_outcome_total = IntCounterVec::new(
        Opts::new(
            "nanobpm_stream_complete_outcome_total",
            "Stream CompleteJob outcomes by decision point (diagnostic).",
        ),
        &["outcome"],
    )
    .expect("valid counter vec");

    let raft_log_bytes = IntGauge::new(
        "nanobpm_raft_log_bytes",
        "Serialized bytes of uncompacted in-memory Raft log entries (all partitions).",
    )
    .expect("valid gauge");
    let raft_log_entries = IntGauge::new(
        "nanobpm_raft_log_entries",
        "Uncompacted in-memory Raft log entries (all partitions).",
    )
    .expect("valid gauge");

    let exporter_queue_bytes = IntGauge::new(
        "nanobpm_exporter_queue_bytes",
        "Resident read-model export backlog bytes (events forwarded but not yet projected, all shards).",
    )
    .expect("valid gauge");

    let resident_var_bytes = IntGauge::new(
        "nanobpm_resident_var_bytes",
        "Approx resident instance variable-payload bytes across all local partitions (burst-balloon attribution).",
    )
    .expect("valid gauge");

    let journal_inflight_bytes = IntGauge::new(
        "nanobpm_journal_inflight_bytes",
        "Serialized event bytes queued to the background journal writer but not yet fsynced+acked (engine->writer in-flight; events_arc roughly doubles the true heap).",
    )
    .expect("valid gauge");

    let ceiling_active = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_ceiling_active",
            "Capacity-ceiling LED: 1 while pressed against the limit, else 0 (ceiling=throughput|memory).",
        ),
        &["ceiling"],
    )
    .expect("valid gauge vec");

    let ceiling_hits_total = IntCounterVec::new(
        Opts::new(
            "nanobpm_ceiling_hits_total",
            "Rising-edge count of capacity-ceiling hits (ceiling=throughput|memory).",
        ),
        &["ceiling"],
    )
    .expect("valid counter vec");

    let job_type_activatable = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_job_type_activatable",
            "Activatable (waiting) jobs per job type across all owned partitions.",
        ),
        &["job_type"],
    )
    .expect("valid gauge vec");

    let job_type_workers = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_job_type_workers",
            "Live subscribed stream workers per job type.",
        ),
        &["job_type"],
    )
    .expect("valid gauge vec");

    let job_type_starved = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_job_type_starved",
            "Worker under-provisioning hint: 1 when jobs are waiting but no worker is subscribed to drain them, else 0.",
        ),
        &["job_type"],
    )
    .expect("valid gauge vec");

    let job_type_dispatched_total = IntCounterVec::new(
        Opts::new(
            "nanobpm_job_type_dispatched_total",
            "Cumulative jobs dispatched to a worker per job type (the drain throughput); delta-scrape for the per-type drain rate.",
        ),
        &["job_type"],
    )
    .expect("valid counter vec");

    let raft_partition_shutdown = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_raft_partition_shutdown",
            "1 when a partition's Raft core has entered Shutdown (terminated, no longer applying) and is stranding its jobs/instances, else 0.",
        ),
        &["partition"],
    )
    .expect("valid gauge vec");
    let actor_alive = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_actor_alive",
            "1 while a partition's engine-actor (single-writer) thread is alive; 0 the instant it exits/panics (a dead single writer freezes all completions on that partition).",
        ),
        &["partition"],
    )
    .expect("valid gauge vec");
    let actor_jobs_total = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_actor_jobs_total",
            "Cumulative engine-actor jobs executed per partition; flat under any completion-freeze, its delta is the actor's true throughput.",
        ),
        &["partition"],
    )
    .expect("valid gauge vec");
    let actor_current_job_ms = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_actor_current_job_ms",
            "Elapsed milliseconds of the engine actor's currently-running job per partition (0 = idle). An unbounded climb is a wedged single writer.",
        ),
        &["partition"],
    )
    .expect("valid gauge vec");
    let actor_hi_depth = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_actor_hi_depth",
            "Depth of the engine actor's High (completion/read) queue per partition.",
        ),
        &["partition"],
    )
    .expect("valid gauge vec");
    let actor_lo_depth = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_actor_lo_depth",
            "Depth of the engine actor's Low (creation) queue per partition.",
        ),
        &["partition"],
    )
    .expect("valid gauge vec");
    let admission_shed_total = prometheus::IntCounterVec::new(
        Opts::new(
            "nanobpm_admission_shed_total",
            "Cumulative createProcessInstance sheds by admission control, labelled by the rail that tripped.",
        ),
        &["reason"],
    )
    .expect("valid counter vec");

    let pending_create_queue = IntGauge::new(
        "nanobpm_pending_create_queue",
        "Submitted-but-not-yet-applied create-queue depth: the create-apply backlog that holds resident memory under an arrival flood (the create_queue/create_backlog shed signal).",
    )
    .expect("valid gauge");
    let active_backlog = IntGauge::new(
        "nanobpm_active_backlog",
        "Active-instance backlog (exporter-projected created-minus-completed); the active_backlog latency-rail signal. ~0 for fast create->complete; climbs when instances park (absent/slow workers).",
    )
    .expect("valid gauge");
    let mem_pressure_bytes = IntGauge::new(
        "nanobpm_mem_pressure_bytes",
        "Cached resident-memory estimate the mem_watermark admission rail keys off.",
    )
    .expect("valid gauge");
    let admission_limit = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_admission_limit",
            "Configured admission shed thresholds (limit=backlog|create_queue are counts; pipeline_bytes|mem_watermark are bytes; 0 = disabled). Reference lines for each pressure signal's headroom.",
        ),
        &["limit"],
    )
    .expect("valid gauge vec");

    registry
        .register(Box::new(commit_batch_size.clone()))
        .and(registry.register(Box::new(fsync_seconds.clone())))
        .and(registry.register(Box::new(commit_wait_seconds.clone())))
        .and(registry.register(Box::new(commits_total.clone())))
        .and(registry.register(Box::new(writes_total.clone())))
        .and(registry.register(Box::new(bytes_total.clone())))
        .and(registry.register(Box::new(inflight.clone())))
        .and(registry.register(Box::new(pipeline_bytes.clone())))
        .and(registry.register(Box::new(writer_idle_seconds.clone())))
        .and(registry.register(Box::new(writer_busy_seconds.clone())))
        .and(registry.register(Box::new(stream_frames_total.clone())))
        .and(registry.register(Box::new(stream_credit_stalls_total.clone())))
        .and(registry.register(Box::new(stream_connections_active.clone())))
        .and(registry.register(Box::new(stream_frame_processing_seconds.clone())))
        .and(registry.register(Box::new(creates_total.clone())))
        .and(registry.register(Box::new(job_completions_total.clone())))
        .and(registry.register(Box::new(stream_complete_outcome_total.clone())))
        .and(registry.register(Box::new(raft_log_bytes.clone())))
        .and(registry.register(Box::new(raft_log_entries.clone())))
        .and(registry.register(Box::new(exporter_queue_bytes.clone())))
        .and(registry.register(Box::new(resident_var_bytes.clone())))
        .and(registry.register(Box::new(journal_inflight_bytes.clone())))
        .and(registry.register(Box::new(ceiling_active.clone())))
        .and(registry.register(Box::new(ceiling_hits_total.clone())))
        .and(registry.register(Box::new(job_type_activatable.clone())))
        .and(registry.register(Box::new(job_type_workers.clone())))
        .and(registry.register(Box::new(job_type_starved.clone())))
        .and(registry.register(Box::new(job_type_dispatched_total.clone())))
        .and(registry.register(Box::new(raft_partition_shutdown.clone())))
        .and(registry.register(Box::new(actor_alive.clone())))
        .and(registry.register(Box::new(actor_jobs_total.clone())))
        .and(registry.register(Box::new(actor_current_job_ms.clone())))
        .and(registry.register(Box::new(actor_hi_depth.clone())))
        .and(registry.register(Box::new(actor_lo_depth.clone())))
        .and(registry.register(Box::new(admission_shed_total.clone())))
        .and(registry.register(Box::new(pending_create_queue.clone())))
        .and(registry.register(Box::new(active_backlog.clone())))
        .and(registry.register(Box::new(mem_pressure_bytes.clone())))
        .and(registry.register(Box::new(admission_limit.clone())))
        .expect("register metrics");

    Metrics {
        registry,
        commit_batch_size,
        fsync_seconds,
        commit_wait_seconds,
        commits_total,
        writes_total,
        bytes_total,
        inflight,
        pipeline_bytes,
        writer_idle_seconds,
        writer_busy_seconds,
        stream_frames_total,
        stream_credit_stalls_total,
        stream_connections_active,
        stream_frame_processing_seconds,
        creates_total,
        job_completions_total,
        stream_complete_outcome_total,
        raft_log_bytes,
        raft_log_entries,
        exporter_queue_bytes,
        resident_var_bytes,
        journal_inflight_bytes,
        ceiling_active,
        ceiling_hits_total,
        job_type_activatable,
        job_type_workers,
        job_type_starved,
        job_type_dispatched_total,
        raft_partition_shutdown,
        actor_alive,
        actor_jobs_total,
        actor_current_job_ms,
        actor_hi_depth,
        actor_lo_depth,
        admission_shed_total,
        pending_create_queue,
        active_backlog,
        mem_pressure_bytes,
        admission_limit,
    }
});

/// Records one completed group-commit: its batch size, fsync duration, and bytes.
pub fn record_commit(batch_size: usize, fsync: Duration, bytes: usize) {
    record_write_batch(batch_size, bytes);
    record_fsync(batch_size, fsync);
}

/// Records a batch of durable writes appended (write_all) but not necessarily yet
/// fsynced. Used by the async-durability writer, which acks after the append and
/// fsyncs on a separate cadence. `record_commit` delegates here for the bytes/
/// writes counters.
pub fn record_write_batch(writes: usize, bytes: usize) {
    let m = &*METRICS;
    m.writes_total.inc_by(writes as u64);
    m.bytes_total.inc_by(bytes as u64);
}

/// Records one fsync (group-commit barrier): the number of writes it made durable
/// and its wall time. In sync mode `writes` == the batch; in async mode it is all
/// writes appended since the previous fsync.
pub fn record_fsync(writes: usize, fsync: Duration) {
    let m = &*METRICS;
    m.commit_batch_size.observe(writes as f64);
    m.fsync_seconds.observe(fsync.as_secs_f64());
    m.commits_total.inc();
}

/// Records how long a caller waited for its commit to become durable.
pub fn record_commit_wait(wait: Duration) {
    METRICS.commit_wait_seconds.observe(wait.as_secs_f64());
}

/// A durable write was enqueued (pipeline depth +1).
pub fn inflight_inc() {
    METRICS.inflight.inc();
}

/// `n` durable writes were fsynced and acknowledged (pipeline depth -n).
pub fn inflight_sub(n: usize) {
    METRICS.inflight.sub(n as i64);
}

/// Publishes the current in-flight create-payload byte gauge (called from the
/// mem-pressure sampler tick, off the hot path).
pub fn set_pipeline_bytes(bytes: u64) {
    METRICS.pipeline_bytes.set(bytes as i64);
}

/// Adjusts the aggregate in-memory Raft-log gauges by a signed delta. Each
/// partition's [`RaftLogStore`](crate::raft_logstore::RaftLogStore) reports the
/// change to its own in-memory index on append/purge/truncate; the gauges sum
/// across partitions to expose the total live Raft-log footprint.
pub fn raft_log_delta(entries_delta: i64, bytes_delta: i64) {
    METRICS.raft_log_entries.add(entries_delta);
    METRICS.raft_log_bytes.add(bytes_delta);
}

/// Publishes the aggregate resident read-model export-backlog byte gauge (summed
/// across shards), sampled off the hot path to attribute the in-flight pipeline
/// share of the RSS balloon.
pub fn set_exporter_queue_bytes(bytes: u64) {
    METRICS.exporter_queue_bytes.set(bytes as i64);
}

/// Publishes the aggregate resident instance variable-payload byte gauge (summed
/// across local partitions), sampled off the hot path. The decisive attribution
/// gauge for the burst RSS balloon: compare its peak against
/// `nanobpm_jemalloc_bytes{kind="allocated"}` — a match means the resident
/// instance variables ARE the balloon, a large shortfall means the balloon is
/// in-flight pipeline copies, not resident variables.
pub fn set_resident_var_bytes(bytes: u64) {
    METRICS.resident_var_bytes.set(bytes as i64);
}

/// `n` serialized event bytes were handed to the journal writer (in-flight +n).
pub fn journal_inflight_add(n: usize) {
    METRICS.journal_inflight_bytes.add(n as i64);
}

/// `n` serialized event bytes were fsynced+acked by the journal writer (-n). The
/// engine->writer in-flight byte gauge; a WHERE-in-the-pipeline attribution for
/// the burst RSS balloon (does the journal write backlog hold the ~12 GB?).
pub fn journal_inflight_sub(n: usize) {
    METRICS.journal_inflight_bytes.sub(n as i64);
}

/// Sets the capacity-ceiling LED for `ceiling` ("throughput"|"memory") and, on
/// a rising edge (previously below the limit, now at it), bumps its hit counter.
/// `previously_active` is the gauge value from the prior monitor tick; the caller
/// threads it so the rising-edge detection needs no extra state read.
pub fn set_ceiling_active(ceiling: &str, active: bool, previously_active: bool) {
    METRICS
        .ceiling_active
        .with_label_values(&[ceiling])
        .set(i64::from(active));
    if active && !previously_active {
        METRICS
            .ceiling_hits_total
            .with_label_values(&[ceiling])
            .inc();
    }
}

/// Publishes the admission-ceiling input signals — the raw numbers behind the
/// `nanobpm_ceiling_active` LED: the create-apply queue depth, the active-instance
/// backlog, and the resident-memory estimate. Called ~1 Hz from the monitor loop,
/// off the hot path. Pair each with its `nanobpm_admission_limit` reference line to
/// watch pressure climb toward (and headroom shrink to) the shed point.
pub fn set_admission_signals(
    pending_create_queue: i64,
    active_backlog: i64,
    mem_pressure_bytes: i64,
) {
    METRICS.pending_create_queue.set(pending_create_queue);
    METRICS.active_backlog.set(active_backlog);
    METRICS.mem_pressure_bytes.set(mem_pressure_bytes);
}

/// Publishes one configured admission threshold as a reference line
/// (`backlog`/`create_queue` are counts, `pipeline_bytes`/`mem_watermark` are
/// bytes; `0` = that rail is disabled).
pub fn set_admission_limit(limit: &str, value: i64) {
    METRICS
        .admission_limit
        .with_label_values(&[limit])
        .set(value);
}

/// Publishes the per-job-type worker-provisioning gauges: waiting jobs, live
/// subscribed workers, and the hard-starvation hint (waiting jobs but no worker).
pub fn set_job_type_provisioning(job_type: &str, activatable: i64, workers: i64) {
    METRICS
        .job_type_activatable
        .with_label_values(&[job_type])
        .set(activatable);
    METRICS
        .job_type_workers
        .with_label_values(&[job_type])
        .set(workers);
    let starved = i64::from(activatable > 0 && workers == 0);
    METRICS
        .job_type_starved
        .with_label_values(&[job_type])
        .set(starved);
}

/// Records `n` jobs dispatched to a worker for `job_type` — the per-type drain
/// throughput. Called once per stream dispatch pass (with the jobs sent to the
/// socket) and once per REST activation (with the jobs returned), so a job is
/// counted exactly once, on the gateway that fed the worker. A no-op when `n==0`
/// to avoid instantiating series for types that never actually drained.
pub fn record_jobs_dispatched(job_type: &str, n: u64) {
    if n == 0 {
        return;
    }
    METRICS
        .job_type_dispatched_total
        .with_label_values(&[job_type])
        .inc_by(n);
}

/// Publishes the per-partition Raft `Shutdown` alarm: `down = true` sets the gauge
/// to 1 (the partition's core has terminated and stopped applying), else 0.
pub fn set_raft_partition_shutdown(partition: u64, down: bool) {
    METRICS
        .raft_partition_shutdown
        .with_label_values(&[&partition.to_string()])
        .set(i64::from(down));
}

/// Publishes one partition's engine-actor (deepthi) heartbeat, sampled ~1 Hz by
/// the metrics monitor. Together these gauges make the sustained-load
/// completion-freeze diagnosable at a glance: `alive=0` = the single writer died
/// (see the actor-exit error log + panic hook); `alive=1` with a frozen `jobs`
/// and a climbing `current_job_ms` = wedged inside one command; `alive=1`, frozen
/// `jobs`, `current_job_ms=0`, depths 0 = idle (the stall is upstream in Raft
/// commit, not the actor).
pub fn set_actor_stats(
    partition: u64,
    alive: bool,
    jobs: u64,
    current_job_ms: u64,
    hi_depth: usize,
    lo_depth: usize,
) {
    let p = partition.to_string();
    METRICS
        .actor_alive
        .with_label_values(&[&p])
        .set(i64::from(alive));
    METRICS
        .actor_jobs_total
        .with_label_values(&[&p])
        .set(jobs as i64);
    METRICS
        .actor_current_job_ms
        .with_label_values(&[&p])
        .set(current_job_ms as i64);
    METRICS
        .actor_hi_depth
        .with_label_values(&[&p])
        .set(hi_depth as i64);
    METRICS
        .actor_lo_depth
        .with_label_values(&[&p])
        .set(lo_depth as i64);
}

/// Records one admission shed (a `createProcessInstance` rejected to protect
/// latency or memory), labelled by the rail that tripped. Delta-scraping this
/// counter shows shed rate and which rail is active — the observability that
/// makes "is the cluster shedding or silently gathering?" answerable.
pub fn record_admission_shed(reason: &str) {
    METRICS
        .admission_shed_total
        .with_label_values(&[reason])
        .inc();
}

/// Accounts one writer-loop iteration: `idle` is the time blocked awaiting the
/// first request, `busy` is the time spent draining/lingering/fsyncing/acking
/// that batch. Delta-scraping the two counters yields the writer's duty cycle.
pub fn record_writer_cycle(idle: Duration, busy: Duration) {
    let m = &*METRICS;
    m.writer_idle_seconds.inc_by(idle.as_secs_f64());
    m.writer_busy_seconds.inc_by(busy.as_secs_f64());
}

/// Renders the registry in the Prometheus text exposition format.
pub fn gather() -> String {
    let mut buf = String::new();
    let families = METRICS.registry.gather();
    TextEncoder::new()
        .encode_utf8(&families, &mut buf)
        .expect("encode metrics");
    buf
}

// ---- Phase 2: falcon and protocol metrics ----

/// Records a falcon frame processed (by frame type).
pub fn record_stream_frame(frame_type: &str) {
    METRICS
        .stream_frames_total
        .with_label_values(&[frame_type])
        .inc();
}

/// Records a streaming client stalling for submission credits.
pub fn record_stream_credit_stall() {
    METRICS.stream_credit_stalls_total.inc();
}

/// Falcon connection opened (+1).
pub fn stream_connection_inc() {
    METRICS.stream_connections_active.inc();
}

/// Falcon connection closed (-1).
pub fn stream_connection_dec() {
    METRICS.stream_connections_active.dec();
}

/// Records time spent processing one falcon frame.
pub fn record_stream_frame_processing(elapsed: Duration) {
    METRICS
        .stream_frame_processing_seconds
        .observe(elapsed.as_secs_f64());
}

/// Records a process instance create (by protocol: "rest" or "stream").
pub fn record_create(protocol: &str) {
    METRICS.creates_total.with_label_values(&[protocol]).inc();
}

/// Records a job completion (by protocol: "rest" or "stream").
pub fn record_job_completion(protocol: &str) {
    METRICS
        .job_completions_total
        .with_label_values(&[protocol])
        .inc();
}

/// Diagnostic: records where a stream CompleteJob landed (route_forward,
/// route_local, leader_reject, propose_err, apply_err, forward_ok, forward_err).
pub fn record_complete_outcome(outcome: &str) {
    METRICS
        .stream_complete_outcome_total
        .with_label_values(&[outcome])
        .inc();
}

/// A plain, dependency-free snapshot of the current metric values, taken in one
/// pass over the process-global registry handles. The console maps this to a
/// JSON DTO; throughput **rates** are derived client-side from the deltas of two
/// successive snapshots (so this stays a pure point-in-time reading).
///
/// Every field is a cheap atomic load (`get`) or a histogram sum/count read — no
/// text encoding, no allocation, no labels lookup beyond the two protocol-split
/// counters. Safe to poll at ~1 Hz from the dashboard with negligible overhead.
#[derive(Clone, Debug, Default)]
#[cfg_attr(not(feature = "console"), allow(dead_code))]
pub struct MetricsSnapshot {
    // Throughput counters (monotonic; rates derived from deltas).
    pub creates_rest: u64,
    pub creates_stream: u64,
    pub completions_rest: u64,
    pub completions_stream: u64,

    // Live gauges.
    pub stream_connections_active: i64,
    pub commit_inflight: i64,

    // Journal / durability counters.
    pub commits_total: u64,
    pub writes_total: u64,
    pub bytes_total: u64,
    pub stream_credit_stalls_total: u64,

    // Histogram aggregates (sum is in the metric's unit; mean = sum/count).
    pub fsync_seconds_sum: f64,
    pub fsync_count: u64,
    pub commit_wait_seconds_sum: f64,
    pub commit_wait_count: u64,
    pub commit_batch_size_sum: f64,
    pub commit_batch_count: u64,
    pub frame_processing_seconds_sum: f64,
    pub frame_processing_count: u64,

    // Writer duty-cycle counters (busy / (busy+idle) = saturation).
    pub writer_idle_seconds: f64,
    pub writer_busy_seconds: f64,

    // Capacity-ceiling LED + admission-ceiling input signals (ADR 0013). The
    // `*_active` bools are the lit "clipping" LEDs; the rest are the numbers
    // behind them so a dashboard can show pressure vs. its shed threshold.
    pub ceiling_throughput_active: bool,
    pub ceiling_memory_active: bool,
    pub pending_create_queue: i64,
    pub active_backlog: i64,
    pub admission_backlog_limit: i64,
    pub admission_create_queue_limit: i64,
    pub admission_shed_total: u64,
}

/// The shed rails (labels of `nanobpm_admission_shed_total`), summed into a
/// single total for the dashboard's "shed since boot" counter.
const SHED_REASONS: [&str; 6] = [
    "create_queue",
    "active_backlog",
    "create_backlog",
    "exporter",
    "pipeline_bytes",
    "mem_watermark",
];

/// Reads every metric handle once and returns a point-in-time snapshot.
#[cfg_attr(not(feature = "console"), allow(dead_code))]
pub fn snapshot() -> MetricsSnapshot {
    let m = &*METRICS;
    MetricsSnapshot {
        creates_rest: m.creates_total.with_label_values(&["rest"]).get(),
        creates_stream: m.creates_total.with_label_values(&["stream"]).get(),
        completions_rest: m.job_completions_total.with_label_values(&["rest"]).get(),
        completions_stream: m.job_completions_total.with_label_values(&["stream"]).get(),

        stream_connections_active: m.stream_connections_active.get(),
        commit_inflight: m.inflight.get(),

        commits_total: m.commits_total.get(),
        writes_total: m.writes_total.get(),
        bytes_total: m.bytes_total.get(),
        stream_credit_stalls_total: m.stream_credit_stalls_total.get(),

        fsync_seconds_sum: m.fsync_seconds.get_sample_sum(),
        fsync_count: m.fsync_seconds.get_sample_count(),
        commit_wait_seconds_sum: m.commit_wait_seconds.get_sample_sum(),
        commit_wait_count: m.commit_wait_seconds.get_sample_count(),
        commit_batch_size_sum: m.commit_batch_size.get_sample_sum(),
        commit_batch_count: m.commit_batch_size.get_sample_count(),
        frame_processing_seconds_sum: m.stream_frame_processing_seconds.get_sample_sum(),
        frame_processing_count: m.stream_frame_processing_seconds.get_sample_count(),

        writer_idle_seconds: m.writer_idle_seconds.get(),
        writer_busy_seconds: m.writer_busy_seconds.get(),

        ceiling_throughput_active: m.ceiling_active.with_label_values(&["throughput"]).get() != 0,
        ceiling_memory_active: m.ceiling_active.with_label_values(&["memory"]).get() != 0,
        pending_create_queue: m.pending_create_queue.get(),
        active_backlog: m.active_backlog.get(),
        admission_backlog_limit: m.admission_limit.with_label_values(&["backlog"]).get(),
        admission_create_queue_limit: m.admission_limit.with_label_values(&["create_queue"]).get(),
        admission_shed_total: SHED_REASONS
            .iter()
            .map(|r| m.admission_shed_total.with_label_values(&[r]).get())
            .sum(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_type_starvation_flags_waiting_jobs_with_no_workers() {
        // Unique label so the assertion is isolated from the shared registry.
        set_job_type_provisioning("test-starved-type", 7, 0);
        set_job_type_provisioning("test-served-type", 7, 3);
        set_job_type_provisioning("test-idle-type", 0, 2);
        let out = gather();
        assert!(out.contains("nanobpm_job_type_starved{job_type=\"test-starved-type\"} 1"));
        assert!(out.contains("nanobpm_job_type_activatable{job_type=\"test-starved-type\"} 7"));
        // Workers present -> not starved even with a backlog.
        assert!(out.contains("nanobpm_job_type_starved{job_type=\"test-served-type\"} 0"));
        // No waiting jobs -> not starved even with idle workers.
        assert!(out.contains("nanobpm_job_type_starved{job_type=\"test-idle-type\"} 0"));
    }

    #[test]
    fn dispatched_counter_accumulates_per_type_and_ignores_zero() {
        // A zero-count dispatch must not instantiate a series (keeps the metric
        // surface free of types that never actually drained).
        record_jobs_dispatched("test-dispatch-zero", 0);
        assert!(!gather().contains("test-dispatch-zero"));

        // Non-zero dispatches accumulate for the type (drain throughput).
        record_jobs_dispatched("test-dispatch-type", 5);
        record_jobs_dispatched("test-dispatch-type", 3);
        assert!(
            gather()
                .contains("nanobpm_job_type_dispatched_total{job_type=\"test-dispatch-type\"} 8")
        );
    }

    #[test]
    fn ceiling_hits_count_only_rising_edges() {
        // Drive a full low -> high -> high -> low -> high cycle and confirm the
        // hit counter advances by exactly one per rising edge, while the gauge
        // tracks the live state each tick.
        let before = ceiling_hits_total_for("test-ceiling");
        set_ceiling_active("test-ceiling", false, false); // stays low
        set_ceiling_active("test-ceiling", true, false); // rising edge (+1)
        set_ceiling_active("test-ceiling", true, true); // held high (no count)
        set_ceiling_active("test-ceiling", false, true); // falling edge
        set_ceiling_active("test-ceiling", true, false); // rising edge (+1)
        assert_eq!(ceiling_hits_total_for("test-ceiling"), before + 2);
        assert!(gather().contains("nanobpm_ceiling_active{ceiling=\"test-ceiling\"} 1"));
    }

    fn ceiling_hits_total_for(ceiling: &str) -> u64 {
        METRICS
            .ceiling_hits_total
            .with_label_values(&[ceiling])
            .get()
    }
}
