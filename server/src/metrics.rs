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

    registry
        .register(Box::new(commit_batch_size.clone()))
        .and(registry.register(Box::new(fsync_seconds.clone())))
        .and(registry.register(Box::new(commit_wait_seconds.clone())))
        .and(registry.register(Box::new(commits_total.clone())))
        .and(registry.register(Box::new(writes_total.clone())))
        .and(registry.register(Box::new(bytes_total.clone())))
        .and(registry.register(Box::new(inflight.clone())))
        .expect("register Phase-1 metrics");

    Metrics {
        registry,
        commit_batch_size,
        fsync_seconds,
        commit_wait_seconds,
        commits_total,
        writes_total,
        bytes_total,
        inflight,
    }
});

/// Records one completed group-commit: its batch size, fsync duration, and bytes.
pub fn record_commit(batch_size: usize, fsync: Duration, bytes: usize) {
    let m = &*METRICS;
    m.commit_batch_size.observe(batch_size as f64);
    m.fsync_seconds.observe(fsync.as_secs_f64());
    m.commits_total.inc();
    m.writes_total.inc_by(batch_size as u64);
    m.bytes_total.inc_by(bytes as u64);
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

/// Renders the registry in the Prometheus text exposition format.
pub fn gather() -> String {
    let mut buf = String::new();
    let families = METRICS.registry.gather();
    TextEncoder::new()
        .encode_utf8(&families, &mut buf)
        .expect("encode metrics");
    buf
}
