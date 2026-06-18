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

use prometheus::{
    Histogram, HistogramOpts, IntCounter, IntGauge, Registry, TextEncoder,
};

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
    /// Cumulative wall time the writer thread spent blocked in `recv` with no
    /// work (idle). Paired with `writer_busy_seconds`, a delta-scrape gives the
    /// writer's duty cycle: `busy / (busy + idle)`. If idle ≈ 0 the single
    /// writer is saturated and is the hard throughput ceiling.
    writer_idle_seconds: prometheus::Counter,
    /// Cumulative wall time the writer thread spent doing work (drain + linger +
    /// serialize + fsync + ack). The non-fsync remainder (`busy − fsync_sum`) is
    /// the writer's CPU cost; if that dominates, the ceiling is CPU not fsync.
    writer_busy_seconds: prometheus::Counter,

    // ---- Phase 2: command-stream and protocol metrics ----
    
    /// Command-stream WebSocket frames processed, by frame type.
    stream_frames_total: prometheus::IntCounterVec,
    /// How many times a streaming client stalled waiting for submission credits.
    stream_credit_stalls_total: IntCounter,
    /// Active command-stream WebSocket connections.
    stream_connections_active: IntGauge,
    /// Time spent processing each command-stream frame (read + apply + reply).
    stream_frame_processing_seconds: Histogram,
    
    /// Process instance creates, split by protocol (rest vs stream).
    creates_total: prometheus::IntCounterVec,
    /// Job completions, split by protocol (rest vs stream).
    job_completions_total: prometheus::IntCounterVec,
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

    // Phase 2: command-stream and protocol metrics
    use prometheus::IntCounterVec;
    use prometheus::Opts;

    let stream_frames_total = IntCounterVec::new(
        Opts::new(
            "nanobpm_stream_frames_total",
            "Command-stream frames processed by type.",
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
        "Active command-stream WebSocket connections.",
    )
    .expect("valid gauge");

    let stream_frame_processing_seconds = Histogram::with_opts(
        HistogramOpts::new(
            "nanobpm_stream_frame_processing_seconds",
            "Time to process each command-stream frame (read+apply+reply).",
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

    registry
        .register(Box::new(commit_batch_size.clone()))
        .and(registry.register(Box::new(fsync_seconds.clone())))
        .and(registry.register(Box::new(commit_wait_seconds.clone())))
        .and(registry.register(Box::new(commits_total.clone())))
        .and(registry.register(Box::new(writes_total.clone())))
        .and(registry.register(Box::new(bytes_total.clone())))
        .and(registry.register(Box::new(inflight.clone())))
        .and(registry.register(Box::new(writer_idle_seconds.clone())))
        .and(registry.register(Box::new(writer_busy_seconds.clone())))
        .and(registry.register(Box::new(stream_frames_total.clone())))
        .and(registry.register(Box::new(stream_credit_stalls_total.clone())))
        .and(registry.register(Box::new(stream_connections_active.clone())))
        .and(registry.register(Box::new(stream_frame_processing_seconds.clone())))
        .and(registry.register(Box::new(creates_total.clone())))
        .and(registry.register(Box::new(job_completions_total.clone())))
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
        writer_idle_seconds,
        writer_busy_seconds,
        stream_frames_total,
        stream_credit_stalls_total,
        stream_connections_active,
        stream_frame_processing_seconds,
        creates_total,
        job_completions_total,
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

// ---- Phase 2: command-stream and protocol metrics ----

/// Records a command-stream frame processed (by frame type).
pub fn record_stream_frame(frame_type: &str) {
    METRICS.stream_frames_total.with_label_values(&[frame_type]).inc();
}

/// Records a streaming client stalling for submission credits.
pub fn record_stream_credit_stall() {
    METRICS.stream_credit_stalls_total.inc();
}

/// Command-stream connection opened (+1).
pub fn stream_connection_inc() {
    METRICS.stream_connections_active.inc();
}

/// Command-stream connection closed (-1).
pub fn stream_connection_dec() {
    METRICS.stream_connections_active.dec();
}

/// Records time spent processing one command-stream frame.
pub fn record_stream_frame_processing(elapsed: Duration) {
    METRICS.stream_frame_processing_seconds.observe(elapsed.as_secs_f64());
}

/// Records a process instance create (by protocol: "rest" or "stream").
pub fn record_create(protocol: &str) {
    METRICS.creates_total.with_label_values(&[protocol]).inc();
}

/// Records a job completion (by protocol: "rest" or "stream").
pub fn record_job_completion(protocol: &str) {
    METRICS.job_completions_total.with_label_values(&[protocol]).inc();
}
