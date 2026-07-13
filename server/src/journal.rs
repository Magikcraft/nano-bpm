//! An append-only event journal that makes the embedded engine durable.
//!
//! The engine itself is in-memory and event-sourced ([`Engine::apply_command_at`]
//! returns the complete, ordered list of events a command produced). This
//! [`Journal`] wraps the engine and persists those events to a newline-delimited
//! JSON log. Writes are handed to a dedicated background thread that
//! **group-commits** — batching every concurrently in-flight command into a
//! single `write` + `fsync` — and a command is acknowledged only once its events
//! are fsynced, so anything the server returns `200` for survives a crash. On
//! startup the log is replayed through [`Engine::replay`] to reconstruct state
//! and the key generator.
//!
//! **Activation locks are deliberately not journaled.** Job activation
//! (`activateJobs`) and lock expiry are *volatile lease state*: a crash forfeits
//! every lock, returning uncompleted jobs to the activatable pool. Recording
//! only durable business facts keeps the log small and gives a clean recovery
//! semantic — after a restart, workers simply re-activate.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use nanobpmn_engine_core::{
    ActivatedJob, Command, Engine, EngineError, Event, Incident, Key, ProcessInstance, State, Value,
};
use tokio::sync::oneshot;

use crate::coldspill::ColdIndex;
use crate::seglog::{ActiveSegment, SegShared};
use crate::varspill::VarSpillStore;
use crate::varstore::VarStore;

/// The result of a lean-snapshot checkpoint (see
/// [`Journal::snapshot_and_rotate_lean`]): the control-only snapshot, the event
/// count it covers, the changed instances' current variable maps to upsert into
/// the durable var store (cheap `Arc` clones — the deep read happens off the
/// engine thread), and the terminal instances' keys to delete from it.
type LeanCheckpoint = (
    nanobpmn_engine_core::EngineSnapshot,
    u64,
    Vec<(Key, Arc<HashMap<String, Value>>)>,
    Vec<Key>,
);

/// A durable-write request handed to the background journal writer: the
/// newline-terminated, serialized bytes for one command's events, the number of
/// events they encode (so the segmented writer can track absolute positions),
/// plus a one-shot sender signalled once those bytes are fsynced to disk.
/// Shared cell holding the read-model exporter senders for a segmented shared
/// writer, keyed by global partition id. The writer thread and every
/// shared-backed [`Journal`] hold an `Arc` clone; a shared-backed
/// [`Journal::set_exporter`] registers its partition's sender, and the writer
/// forwards each committed command's events through the matching partition's
/// sender in fsync (log) order (see [`forward_shared_export`]). Sharding the map
/// by partition lets each owned partition drive its own read-store shard +
/// exporter thread, so projection scales with cores. Empty for the non-shared
/// journals (which forward from `persist()` and leave this untouched).
///
/// The value pairs each partition's sender with its exporter-queue byte gauge
/// (`Arc<AtomicU64>`): the writer increments it by each forwarded command's
/// serialized size before the send, and the exporter thread decrements it after
/// the batch is projected. The create-admission path reads the same gauge to
/// steer creates away from — and, once every local shard is saturated, shed —
/// so the resident exporter backlog stays bounded under a large-variable flood
/// (see `ExporterBackpressure`).
type ExporterCell = Arc<Mutex<HashMap<u64, (Sender<ExportBatch>, Arc<AtomicU64>)>>>;

/// A read-model export item handed to an exporter thread: a command's events
/// plus `bytes`, an estimate of their resident cost (the serialized journal
/// bytes for those events) used to account the exporter queue's size for
/// backpressure. `bytes` is `0` on the non-shared (in-memory / legacy
/// single-file) path, which does not participate in exporter-queue accounting.
pub struct ExportBatch {
    pub events: Arc<Vec<Event>>,
    pub bytes: u32,
}

struct WriteRequest {
    bytes: Vec<u8>,
    events: usize,
    /// Global partition id that produced these events (0 on the single-partition
    /// path). Lets the shared multi-partition writer attribute each write to a
    /// partition's cumulative count for per-partition seal boundaries.
    partition: u64,
    /// The command's events, shared by `Arc`. `Some` only for a journal backed by
    /// the shared multi-partition writer with a wired exporter cell: the writer
    /// forwards them to the read-model exporter in fsync (log) order, so
    /// `exported_position` stays a true global prefix. `None` everywhere else
    /// (those paths forward from `persist()` directly).
    events_arc: Option<Arc<Vec<Event>>>,
    ack: oneshot::Sender<()>,
}

/// A message to the background journal writer: either a durable write, or a
/// control request to **seal** (rotate) the active segment at the current
/// boundary and report it. Routing both through one ordered channel guarantees
/// a rotate is processed exactly between the writes before and after it, so a
/// snapshot taken on the engine actor covers precisely the sealed segment(s).
enum WriterMsg {
    Write(WriteRequest),
    /// Seal the active segment now; reply with the sealed boundary.
    Rotate(oneshot::Sender<crate::seglog::SealInfo>),
}

/// A handle to the durability of a single write. Awaiting [`Commit::wait`]
/// resolves once the command's events have been group-committed (written and
/// fsynced) by the background writer thread. A `Ready` commit is already durable
/// (an in-memory journal, or a command that produced no events).
#[must_use = "await the commit to guarantee the write is durable before responding"]
pub struct Commit(CommitInner);

enum CommitInner {
    Ready,
    Pending(oneshot::Receiver<()>),
}

impl Commit {
    pub(crate) fn ready() -> Self {
        Commit(CommitInner::Ready)
    }

    /// Waits until the write backing this commit has been fsynced. If the writer
    /// thread is gone (shutdown), it resolves immediately rather than hanging.
    pub async fn wait(self) {
        if let CommitInner::Pending(rx) = self.0 {
            let start = std::time::Instant::now();
            let _ = rx.await;
            crate::metrics::record_commit_wait(start.elapsed());
        }
    }
}

/// The engine plus its durable event log.
pub struct Journal {
    engine: Engine,
    /// Global partition id this journal's engine owns (0 for single-partition).
    /// Tags each durable write so the shared multi-partition writer can track
    /// per-partition cumulative counts, and selects this partition's covered
    /// count out of a seal boundary.
    partition_id: u64,
    /// `None` for an in-memory (non-persistent) journal; otherwise the channel to
    /// the background writer thread that owns the log file.
    writer: Option<Sender<WriterMsg>>,
    /// Joined on drop so any not-yet-acked writes are flushed before the journal
    /// goes away (matters for synchronous callers that never await the commit).
    writer_thread: Option<JoinHandle<()>>,
    /// Shared seal/boundary state when this journal is backed by a *segmented*
    /// log (the single-partition bounded-disk path). `None` for in-memory and the
    /// legacy single-file paths. Lets the snapshot/compaction task read segment
    /// boundaries and drives [`Journal::snapshot_and_rotate`].
    seg: Option<Arc<SegShared>>,
    /// Channel to the read-model exporter thread, if one is wired. Every command's
    /// journaled events are forwarded here (in command order: the actor applies
    /// commands serially) to be projected into the read store. The events are
    /// shared with the command's caller via `Arc`, so forwarding them costs only a
    /// refcount bump — the 50 KB variable payloads are never deep-copied on the
    /// single command thread.
    exporter: Option<Sender<ExportBatch>>,
    /// For a journal backed by the shared multi-partition writer: the writer's
    /// exporter cell. `set_exporter` stores the exporter here instead of in
    /// `self.exporter`, so the SHARED WRITER forwards events to the read model in
    /// log order (making `exported_position` a true global prefix) and
    /// `persist()` does not double-project. `None` for every other path (which
    /// forward from `persist()`).
    shared_exporter: Option<ExporterCell>,
    /// `true` when the journal started with no prior log, so the host knows it
    /// should seed any initial deployments.
    fresh: bool,
    /// Optional disk-backed variable spill. Sheds the variables of the oldest
    /// *active* backlog instances to disk (rehydrated on job activation), bounding
    /// the RAM held by a large active backlog. `None` keeps every payload resident
    /// (the original behaviour). The [`VarSpillTrigger`] decides *when* to shed —
    /// a fixed instance-count budget, or adaptively under real RAM pressure.
    spill: Option<VarSpill>,
    /// Optional cold spill: evicts whole *dormant* instances (control state and
    /// all) to the same disk-backed store, keeping only a slim resident routing
    /// index ([`ColdIndex`]) so an event can rehydrate the owning instance on
    /// demand. Triggered by hot-RAM pressure (resident bytes crossing
    /// `high_water`), it sheds the oldest dormant instances until back under
    /// `low_water`. `None` keeps every instance resident. This is the long-lived,
    /// low-throughput counterpart to variable spill: where variable spill bounds a
    /// large *active* backlog, cold spill bounds a large *parked* one.
    cold: Option<ColdSpill>,
    /// The authoritative durable variable store, when lean-snapshot mode is on.
    /// Under lean mode the periodic snapshot carries only control state; every
    /// live instance's current variables live here instead. Variable spill
    /// write-through, job-activation rehydration, terminal forget, and the
    /// periodic checkpoint all target this store. `None` keeps the classic
    /// full-variable snapshot (and the destructive [`VarSpillStore`] spill cache).
    varstore: Option<Arc<VarStore>>,
    /// Countdown that amortises the O(N) spillable scan in [`maybe_spill`]. While
    /// the resident set stays over the spill budget, the precise scan/shed runs
    /// only once every [`SPILL_CHECK_INTERVAL`] commands instead of on every
    /// apply; between runs the hot path is O(1). Reset to zero whenever the
    /// resident set drops back within budget so the next overflow is caught
    /// immediately.
    spill_check_skip: u32,
}

/// Cold-spill state held by a [`Journal`]: the shared disk store, the resident
/// routing index, and the resident-byte watermarks that gate the sweep.
struct ColdSpill {
    store: Arc<VarSpillStore>,
    index: ColdIndex,
    high_water: u64,
    low_water: u64,
    /// Hysteresis latch: set once resident crosses `high_water` and cleared only
    /// when it falls back under `low_water`. While latched the budgeted sweep
    /// keeps shedding across ticks toward `low_water` (the OOM guard's reclaim
    /// target) rather than stopping the moment resident dips under `high_water`,
    /// which would flap the sweep on and off right at the mark.
    shedding: bool,
}

/// Variable-spill state held by a [`Journal`]: the shared disk store and the
/// policy that decides *when* the oldest active-backlog variables are shed.
struct VarSpill {
    store: Arc<VarSpillStore>,
    trigger: VarSpillTrigger,
}

/// When variable spill sheds an active instance's variables to disk.
enum VarSpillTrigger {
    /// Fixed instance-count budget, evaluated on every command. Simple and
    /// deterministic, but a *count* is a poor proxy for *bytes* (512 tiny-var
    /// instances is trivial RAM; 512 large-var instances is not), so it must be
    /// tuned conservatively and can throttle throughput needlessly.
    Budget(usize),
    /// Adaptive: shed strictly on **measured memory** so a healthy large working
    /// set keeps full throughput while genuine memory pressure is bounded to the
    /// reclaim target. Instance count is not a signal (a poor proxy for bytes).
    ///
    /// - Below `high_water` with free RAM: never spill — full throughput, however
    ///   large the (stable) working set.
    /// - Resident `>= high_water`: shed toward `low_water` — the OOM guard.
    /// - Live system-available memory below `reserve`: shed toward `floor` (free
    ///   RAM is contended by another tenant, so reclaim harder).
    ///
    /// `floor` is the aggressive reclaim target under a `reserve` breach;
    /// `low_water` the gentle one under a plain high-water breach (hysteresis).
    /// `hard_cap` is the per-command instance-count backstop for a create-flood
    /// *between* the 500 ms sweeps (`0` disables it, relying solely on the
    /// sweep). `reserve == 0` disables the live-available-memory guard.
    Adaptive {
        floor: u64,
        high_water: u64,
        low_water: u64,
        reserve: u64,
        hard_cap: usize,
    },
}

impl VarSpillTrigger {
    /// The resident spill-candidate count above which the *per-command* path
    /// sheds. `Budget` sheds at its budget; `Adaptive` sheds only at its emergency
    /// `hard_cap` (its normal trigger is the RSS sweep). `None` means the
    /// per-command path never sheds.
    fn per_command_cap(&self) -> Option<usize> {
        match self {
            VarSpillTrigger::Budget(budget) => Some(*budget),
            VarSpillTrigger::Adaptive { hard_cap, .. } => (*hard_cap != 0).then_some(*hard_cap),
        }
    }
}

/// Upper bound on how many writes one group-commit batch will accumulate before
/// forcing the fsync, regardless of the linger window. Bounds worst-case commit
/// latency and the staging buffer when offered load is very high.
const MAX_GROUP_BATCH: usize = 8192;

/// How many commands the journal skips between precise spillable scans while the
/// resident set stays over the spill budget. Variable spill is a soft memory
/// bound (payloads stay durable in the journal), so amortising the O(N) scan
/// across this many applies keeps the hot path O(1) under a deep backlog while
/// bounding memory overshoot to at most this many commands' worth of newly
/// spillable variables.
const SPILL_CHECK_INTERVAL: u32 = 256;

/// How many instances' variables the adaptive RAM-pressure sweep sheds per batch
/// before re-measuring resident bytes. Keeps the shed responsive to the
/// watermark (stop as soon as we drop under `low_water`) without re-reading RSS —
/// an epoch-advance + stat — after every single instance.
const VAR_SPILL_SWEEP_BATCH: usize = 512;

/// Futile-shed guard for the memory-driven spill sweep: variable spill only runs
/// when the resident variable payloads it *could* shed are at least
/// `overshoot / VAR_SPILL_MIN_RELIEF_DIVISOR` bytes — i.e. spilling can relieve
/// at least ~1/8 of the amount by which resident memory overshoots the reclaim
/// target. Below that, variables are not the memory driver (control state is, or
/// the payloads are tiny), so shedding them would only thrash the backlog for
/// negligible relief; the residual is cold spill / admission backpressure's job.
/// This replaces the old per-batch RSS-delta bail, which mis-fired under
/// concurrent inbound allocation (new allocations masked the shed). Purely a
/// *byte* comparison — instance count never enters the decision.
const VAR_SPILL_MIN_RELIEF_DIVISOR: u64 = 8;

/// Per-tick wall-clock budget for the adaptive RAM-pressure spill sweeps
/// ([`Journal::maybe_var_spill_pressure`] / [`Journal::maybe_cold_spill`]). Both
/// run on the single-writer engine actor via the periodic tick. The previous
/// design looped — shedding a batch, then `shrink()` (ten `shrink_to_fit` map
/// reallocs) + jemalloc `purge()` + a resident re-read after *every* batch —
/// until resident memory fell under the reclaim target. Under a large active
/// backlog that held the writer for >100 ms per tick (measured 123 ms/call,
/// 2.4 MB alloc/call), starving instance creation and job completion: the
/// congestion-collapse hot path. Bounding each sweep to this budget caps the
/// hold; successive ticks resume shedding where the last left off, so relief is
/// spread across ticks instead of one multi-hundred-ms stall. Bounded intake
/// (admission control) keeps the backlog from outrunning the per-tick shed.
const SPILL_TICK_BUDGET: std::time::Duration = std::time::Duration::from_millis(8);

/// The background journal writer: blocks for the next request, drains every
/// other request already queued, then **group-commits** the whole batch in a
/// single `write` + `fsync` before signalling each command's commit. Batching
/// amortizes one fsync across all concurrently in-flight writes.
///
/// `linger` is an optional group-commit delay (à la Postgres `commit_delay` /
/// MySQL binlog group commit). When non-zero, after draining the instantly
/// available writes the writer waits up to `linger` for *more* writes to arrive
/// before committing. This breaks the closed-loop pathology where each client
/// blocks on its own `fsync` ack, so only one write is ever queued at a time and
/// the batch never grows past 1 — leaving throughput pinned at one commit per
/// fsync latency. A small linger lets many in-flight clients coalesce into one
/// fsync, trading a bounded latency increase for a large throughput gain on
/// fsync-latency-bound workloads. Zero (the default) preserves the original
/// drain-only behaviour exactly.
///
/// A write or fsync failure is unrecoverable — the in-memory engine has already
/// advanced past the durable log — so the process is aborted rather than risk
/// acknowledging or serving non-durable state (mirrors the previous
/// panic-on-I/O-error contract).
/// Forwards a shared-writer-backed command's events to the read-model exporter
/// (if wired) in fsync/log order. A no-op when `events_arc` is `None` (the
/// non-shared paths forward from `persist()` instead). An `events_arc` present
/// with no wired exporter cell is an ordering bug: the exporter must be wired
/// before any shared write is served; we debug-assert and skip (boot catch-up
/// covers everything written before the wire, so this can only be a real bug).
/// Forwards a shared-writer-backed command's events to the read-model exporter
/// shard for `partition` (if wired) in fsync/log order. A no-op when `events_arc`
/// is `None` (the non-shared paths forward from `persist()` instead). An
/// `events_arc` present with no wired exporter sender for the partition is an
/// ordering bug: every owned partition's exporter must be wired before any shared
/// write is served; we debug-assert and skip (boot catch-up covers everything
/// written before the wire, so this can only be a real bug).
fn forward_shared_export(
    exporter: &Option<ExporterCell>,
    partition: u64,
    events_arc: Option<Arc<Vec<Event>>>,
    bytes: u32,
) {
    let Some(events) = events_arc else { return };
    if events.is_empty() {
        return;
    }
    let Some(cell) = exporter else {
        debug_assert!(false, "shared write with no exporter cell wired");
        return;
    };
    if let Some((tx, queued)) = cell.lock().unwrap().get(&partition) {
        // Account the queued bytes BEFORE the send so the create-admission gauge
        // never lags behind a queue the exporter has not yet drained; the
        // exporter subtracts the same amount once the batch is projected.
        queued.fetch_add(bytes as u64, Ordering::Relaxed);
        let _ = tx.send(ExportBatch { events, bytes });
    } else {
        debug_assert!(false, "shared write before exporter was set for partition");
    }
}

fn writer_loop(
    mut seg: ActiveSegment,
    rx: Receiver<WriterMsg>,
    linger: Duration,
    exporter: Option<ExporterCell>,
) {
    loop {
        let idle_start = Instant::now();
        let Ok(first) = rx.recv() else { break };
        let idle = idle_start.elapsed();
        let busy_start = Instant::now();

        let mut batch: Vec<WriteRequest> = Vec::new();
        let mut pending_rotate: Option<oneshot::Sender<crate::seglog::SealInfo>> = None;
        match first {
            WriterMsg::Write(w) => batch.push(w),
            WriterMsg::Rotate(reply) => pending_rotate = Some(reply),
        }

        // Drain queued writes into this batch, stopping at a rotate (handled
        // after the batch is durable) or the group-commit cap.
        if pending_rotate.is_none() {
            while batch.len() < MAX_GROUP_BATCH {
                match rx.try_recv() {
                    Ok(WriterMsg::Write(w)) => batch.push(w),
                    Ok(WriterMsg::Rotate(reply)) => {
                        pending_rotate = Some(reply);
                        break;
                    }
                    Err(_) => break,
                }
            }
        }

        // Optional group-commit linger: hold the fsync briefly so more
        // concurrently in-flight writes can join this batch. Bounded by both the
        // window and a hard batch cap so a steady flood can't defer a commit
        // indefinitely. A rotate ends the linger (the boundary must be exact).
        if pending_rotate.is_none() && !linger.is_zero() && batch.len() < MAX_GROUP_BATCH {
            let deadline = Instant::now() + linger;
            while batch.len() < MAX_GROUP_BATCH {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    break;
                };
                match rx.recv_timeout(remaining) {
                    Ok(WriterMsg::Write(w)) => batch.push(w),
                    Ok(WriterMsg::Rotate(reply)) => {
                        pending_rotate = Some(reply);
                        break;
                    }
                    Err(RecvTimeoutError::Timeout) => break,
                    // All senders gone: flush what we have; the outer `recv`
                    // will then observe the disconnect and exit.
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        }

        if !batch.is_empty() {
            let mut buf = Vec::new();
            let mut events: u64 = 0;
            let num_partitions = seg.shared().partitions();
            let mut deltas = vec![0u64; num_partitions];
            for req in &batch {
                buf.extend_from_slice(&req.bytes);
                events += req.events as u64;
                if num_partitions > 0 && (req.partition as usize) < num_partitions {
                    deltas[req.partition as usize] += req.events as u64;
                }
            }

            let fsync_start = Instant::now();
            if let Err(e) = seg.write_all(&buf, events).and_then(|()| seg.fsync()) {
                tracing::error!(
                    "journal write failed: {e}; aborting to avoid serving non-durable state"
                );
                std::process::abort();
            }
            // Advance per-partition cumulative counts BEFORE any seal, so the
            // sealed boundary's per-partition end matches the bytes just written
            // (no-op on the legacy/single-partition path).
            if num_partitions > 0 {
                seg.shared().add_partition_events(&deltas);
            }
            crate::metrics::record_commit(batch.len(), fsync_start.elapsed(), buf.len());
            crate::metrics::inflight_sub(batch.len());
            crate::metrics::journal_inflight_sub(buf.len());

            for req in batch {
                // The receiver is gone for fire-and-forget writes (the background
                // tick and startup seeding never await their commit); that's fine.
                let _ = req.ack.send(());
                // Forward to the read-model exporter shard for this write's
                // partition in log (fsync) order. `None` events_arc means this
                // journal forwards via `persist()` instead (single-partition /
                // in-memory / legacy). The serialized size (`req.bytes.len()`) is
                // the exporter-queue byte estimate for backpressure accounting.
                let export_bytes = req.bytes.len().min(u32::MAX as usize) as u32;
                forward_shared_export(&exporter, req.partition, req.events_arc, export_bytes);
            }

            // Size-based segment seal (no-op in the legacy single-file path,
            // whose threshold is infinite).
            if let Err(e) = seg.maybe_seal() {
                tracing::error!("journal segment seal failed: {e}; aborting");
                std::process::abort();
            }
        }

        // A rotate seals the active segment at the exact boundary AFTER the
        // preceding writes are durable, then reports it to the snapshot caller.
        if let Some(reply) = pending_rotate {
            match seg.seal() {
                Ok(info) => {
                    let _ = reply.send(info);
                }
                Err(e) => {
                    tracing::error!("journal rotate failed: {e}; aborting");
                    std::process::abort();
                }
            }
        }
        crate::metrics::record_writer_cycle(idle, busy_start.elapsed());
    }
}

/// The **async-durability** writer loop. Like [`writer_loop`] it group-commits a
/// batch with a single `write_all`, but it **acknowledges callers immediately
/// after the write reaches the OS page cache** and defers `fsync` to an
/// amortized cadence (every [`AsyncFlush::interval`] or
/// [`AsyncFlush::max_bytes`], whichever first). This removes the fsync media
/// barrier from every commit's critical path and lets fsyncs coalesce across
/// many batches, trading a bounded power-loss window for throughput. A process
/// crash loses nothing — the appended bytes survive in the page cache / file and
/// are replayed on restart; only an OS crash / power loss can lose the unfsynced
/// tail. When the producer goes quiet the loop wakes on a bounded
/// `recv_timeout` to flush that tail, so the window is always bounded by
/// `interval`. Write/fsync errors abort, identical to the sync path.
fn writer_loop_async(
    mut seg: ActiveSegment,
    rx: Receiver<WriterMsg>,
    linger: Duration,
    flush: AsyncFlush,
    exporter: Option<ExporterCell>,
) {
    let mut last_fsync = Instant::now();
    let mut unsynced_bytes: usize = 0;
    let mut unsynced_writes: usize = 0;

    // Force a durability barrier for everything written since the last fsync.
    fn do_fsync(
        seg: &mut ActiveSegment,
        bytes: &mut usize,
        writes: &mut usize,
        last: &mut Instant,
    ) {
        if *bytes == 0 {
            return;
        }
        let fsync_start = Instant::now();
        if let Err(e) = seg.fsync() {
            tracing::error!(
                "journal fsync failed: {e}; aborting to avoid serving non-durable state"
            );
            std::process::abort();
        }
        crate::metrics::record_fsync(*writes, fsync_start.elapsed());
        *bytes = 0;
        *writes = 0;
        *last = Instant::now();
    }

    loop {
        // Wait for the next message. While an unfsynced tail is outstanding,
        // bound the wait so a quiet producer can't leave it unflushed past the
        // configured interval.
        let idle_start = Instant::now();
        let first = if unsynced_bytes > 0 {
            let budget = flush
                .interval
                .checked_sub(last_fsync.elapsed())
                .unwrap_or_default();
            match rx.recv_timeout(budget) {
                Ok(msg) => msg,
                Err(RecvTimeoutError::Timeout) => {
                    do_fsync(
                        &mut seg,
                        &mut unsynced_bytes,
                        &mut unsynced_writes,
                        &mut last_fsync,
                    );
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        } else {
            match rx.recv() {
                Ok(msg) => msg,
                Err(_) => break,
            }
        };
        let idle = idle_start.elapsed();
        let busy_start = Instant::now();

        let mut batch: Vec<WriteRequest> = Vec::new();
        let mut pending_rotate: Option<oneshot::Sender<crate::seglog::SealInfo>> = None;
        match first {
            WriterMsg::Write(w) => batch.push(w),
            WriterMsg::Rotate(reply) => pending_rotate = Some(reply),
        }

        if pending_rotate.is_none() {
            while batch.len() < MAX_GROUP_BATCH {
                match rx.try_recv() {
                    Ok(WriterMsg::Write(w)) => batch.push(w),
                    Ok(WriterMsg::Rotate(reply)) => {
                        pending_rotate = Some(reply);
                        break;
                    }
                    Err(_) => break,
                }
            }
        }
        // Linger still fattens the *write* batch (fewer write_all syscalls), even
        // though it no longer gates an fsync on the caller's behalf.
        if pending_rotate.is_none() && !linger.is_zero() && batch.len() < MAX_GROUP_BATCH {
            let deadline = Instant::now() + linger;
            while batch.len() < MAX_GROUP_BATCH {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    break;
                };
                match rx.recv_timeout(remaining) {
                    Ok(WriterMsg::Write(w)) => batch.push(w),
                    Ok(WriterMsg::Rotate(reply)) => {
                        pending_rotate = Some(reply);
                        break;
                    }
                    Err(RecvTimeoutError::Timeout) => break,
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        }

        if !batch.is_empty() {
            let mut buf = Vec::new();
            let mut events: u64 = 0;
            let num_partitions = seg.shared().partitions();
            let mut deltas = vec![0u64; num_partitions];
            for req in &batch {
                buf.extend_from_slice(&req.bytes);
                events += req.events as u64;
                if num_partitions > 0 && (req.partition as usize) < num_partitions {
                    deltas[req.partition as usize] += req.events as u64;
                }
            }
            if let Err(e) = seg.write_all(&buf, events) {
                tracing::error!(
                    "journal write failed: {e}; aborting to avoid serving non-durable state"
                );
                std::process::abort();
            }
            if num_partitions > 0 {
                seg.shared().add_partition_events(&deltas);
            }
            unsynced_bytes += buf.len();
            unsynced_writes += batch.len();
            crate::metrics::record_write_batch(batch.len(), buf.len());

            // Acknowledge now: the write is in the page cache and will survive a
            // process restart; the amortized fsync below upgrades it to
            // power-loss-durable.
            crate::metrics::inflight_sub(batch.len());
            crate::metrics::journal_inflight_sub(buf.len());
            for req in batch {
                let _ = req.ack.send(());
                let export_bytes = req.bytes.len().min(u32::MAX as usize) as u32;
                forward_shared_export(&exporter, req.partition, req.events_arc, export_bytes);
            }

            if unsynced_bytes >= flush.max_bytes || last_fsync.elapsed() >= flush.interval {
                do_fsync(
                    &mut seg,
                    &mut unsynced_bytes,
                    &mut unsynced_writes,
                    &mut last_fsync,
                );
            }

            // A size-based seal flushes the segment it closes (seal fsyncs), so
            // the unsynced tail is now durable; reset the window.
            match seg.maybe_seal() {
                Ok(Some(_)) => {
                    unsynced_bytes = 0;
                    unsynced_writes = 0;
                    last_fsync = Instant::now();
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!("journal segment seal failed: {e}; aborting");
                    std::process::abort();
                }
            }
        }

        // A rotate seals the active segment at the exact boundary. Sealing
        // fsyncs the closed segment, so the unsynced tail (now in that segment)
        // is durable; reset the window.
        if let Some(reply) = pending_rotate {
            match seg.seal() {
                Ok(info) => {
                    unsynced_bytes = 0;
                    unsynced_writes = 0;
                    last_fsync = Instant::now();
                    let _ = reply.send(info);
                }
                Err(e) => {
                    tracing::error!("journal rotate failed: {e}; aborting");
                    std::process::abort();
                }
            }
        }
        crate::metrics::record_writer_cycle(idle, busy_start.elapsed());
    }

    // Drain on shutdown: fsync whatever tail was acked but not yet synced.
    do_fsync(
        &mut seg,
        &mut unsynced_bytes,
        &mut unsynced_writes,
        &mut last_fsync,
    );
}

/// Group-commit linger window from `NANOBPMN_JOURNAL_LINGER_US` (microseconds).
/// Default 0 = off (drain-only group commit, original behaviour). A small value
/// (e.g. 200–2000µs) coalesces fsyncs on latency-bound workloads. Clamped to a
/// 50ms ceiling so a fat-fingered value can't stall durability.
fn journal_linger_from_env() -> Duration {
    const MAX_US: u64 = 50_000;
    match std::env::var("NANOBPMN_JOURNAL_LINGER_US") {
        Ok(v) => match v.trim().parse::<u64>() {
            Ok(us) => Duration::from_micros(us.min(MAX_US)),
            Err(_) => Duration::ZERO,
        },
        Err(_) => Duration::ZERO,
    }
}

/// Durability mode for the journal writer.
///
/// - [`Sync`](DurabilityMode::Sync) (default): a write is acknowledged only
///   after it has been `fsync`ed. The strongest contract — anything the server
///   returns `200` for survives power loss — at the cost of putting the ~4 ms
///   media barrier on every commit's critical path.
/// - [`Async`](DurabilityMode::Async): a write is acknowledged once it is in the
///   OS page cache (after `write_all`), and `fsync` is amortized onto a periodic
///   cadence in the background. This takes the fsync latency off the caller's
///   critical path and lets fsyncs coalesce far more aggressively (the writer no
///   longer blocks a commit per fsync), at the cost of a weaker durability
///   window: a **process** crash loses nothing (the page cache, hence the log
///   file, survives and is replayed on restart), but an **OS crash or power
///   loss** can lose the unfsynced tail (bounded by the flush interval/bytes).
///   This mirrors Zeebe's async-exporter model: ack from the in-memory/appended
///   log, guarantee durability by journal replay on restart.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DurabilityMode {
    Sync,
    Async,
}

/// Async-durability flush policy: `fsync` fires when either the bytes written
/// since the last fsync reach `max_bytes`, or `interval` elapses (whichever
/// comes first). Tunable via `NANOBPMN_ASYNC_FLUSH_MS` / `NANOBPMN_ASYNC_FLUSH_BYTES`.
#[derive(Clone, Copy)]
struct AsyncFlush {
    interval: Duration,
    max_bytes: usize,
}

/// Durability mode from `NANOBPMN_DURABILITY` (`sync` | `async`). Defaults to
/// `sync` so the strong fsync-before-ack contract — and every existing test —
/// is unchanged unless async is explicitly opted into.
fn durability_mode_from_env() -> DurabilityMode {
    match std::env::var("NANOBPMN_DURABILITY") {
        Ok(v) if v.trim().eq_ignore_ascii_case("async") => DurabilityMode::Async,
        _ => DurabilityMode::Sync,
    }
}

/// Async flush policy from env. `NANOBPMN_ASYNC_FLUSH_MS` (default 10, clamped to
/// 1s) bounds the unfsynced time window; `NANOBPMN_ASYNC_FLUSH_BYTES` (default
/// 8 MiB) bounds the unfsynced byte window. Either trigger forces an fsync.
fn async_flush_from_env() -> AsyncFlush {
    let interval = match std::env::var("NANOBPMN_ASYNC_FLUSH_MS") {
        Ok(v) => match v.trim().parse::<u64>() {
            Ok(ms) => Duration::from_millis(ms.clamp(1, 1000)),
            Err(_) => Duration::from_millis(10),
        },
        Err(_) => Duration::from_millis(10),
    };
    let max_bytes = match std::env::var("NANOBPMN_ASYNC_FLUSH_BYTES") {
        Ok(v) => v.trim().parse::<usize>().unwrap_or(8 << 20).max(4096),
        Err(_) => 8 << 20,
    };
    AsyncFlush {
        interval,
        max_bytes,
    }
}

/// Spawns the journal writer thread for `seg`/`rx`, selecting the sync or async
/// commit loop from `NANOBPMN_DURABILITY`. Shared by [`SharedWriter::open`],
/// [`Journal::open_partition`] and the segmented path so all honour the same
/// durability configuration.
fn spawn_writer(
    seg: ActiveSegment,
    rx: Receiver<WriterMsg>,
    exporter: Option<ExporterCell>,
) -> io::Result<JoinHandle<()>> {
    let linger = journal_linger_from_env();
    let mode = durability_mode_from_env();
    let flush = async_flush_from_env();
    thread::Builder::new()
        .name("nanobpmn-journal-writer".into())
        .spawn(move || match mode {
            DurabilityMode::Sync => writer_loop(seg, rx, linger, exporter),
            DurabilityMode::Async => writer_loop_async(seg, rx, linger, flush, exporter),
        })
}

/// file+thread. Because every partition's in-flight commands land in the *same*
/// group-commit batch, a single `fsync` drains them all — the batch coalesces
/// across the whole node's write concurrency.
///
/// This is the fix for **per-partition fsync fragmentation**: with one journal
/// file per partition, each writer only ever sees ~1/N of the node's concurrent
/// writes, so its group-commit batches are N× smaller and it must `fsync` N×
/// more often (and the disk's concurrent-fsync ceiling is hit sooner). Funneling
/// every partition through one writer makes the fsync rate independent of the
/// partition count while keeping the partitions fully independent for
/// *processing* (each still has its own single-writer engine actor and key
/// namespace; events are tagged by partition via their keys, so the shared log
/// is demultiplexed back to the owning partition on replay).
///
/// The writer thread is detached (its [`JoinHandle`] is dropped): like the
/// engine threads, durability never depends on a clean shutdown — a command is
/// acked only after its [`Commit`] resolves, i.e. after the write is fsynced —
/// so the thread can simply be reclaimed by the OS on exit. It stays alive as
/// long as any partition journal (holding a cloned sender) is alive.
pub struct SharedWriter {
    tx: Sender<WriterMsg>,
    /// Segmented seal/boundary state when this is the bounded-disk
    /// multi-partition writer; `None` for the legacy single-file shared log.
    seg: Option<Arc<SegShared>>,
    /// Exporter cell shared with the writer thread: the read-model exporter
    /// sender is dropped in here (via a shared-backed [`Journal::set_exporter`])
    /// so the writer forwards events in fsync/log order. `None` for the legacy
    /// path (which forwards from `persist()`).
    exporter: Option<ExporterCell>,
}

impl SharedWriter {
    /// Opens (creating if absent, positioned to append) the shared log at `path`
    /// and spawns its group-commit writer thread, reading the same
    /// `NANOBPMN_JOURNAL_LINGER_US` linger window as a per-partition journal.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let shared = SegShared::legacy(path.as_ref().to_path_buf());
        let seg = ActiveSegment::open(shared)?;
        let (tx, rx) = mpsc::channel::<WriterMsg>();
        spawn_writer(seg, rx, None).expect("spawn shared journal writer thread");
        Ok(Self {
            tx,
            seg: None,
            exporter: None,
        })
    }

    /// Opens (or recovers) the bounded-disk **segmented multi-partition** shared
    /// log rooted at directory `dir`, sized to `num_partitions`. Rebuilds every
    /// owned partition's engine from the combined snapshot + surviving tail (see
    /// [`crate::seglog::recover_multi`]) and spawns the segment-aware writer with
    /// an exporter cell so the read model is fed a single global prefix in log
    /// order. Returns the writer plus the per-partition recovery.
    pub fn open_segmented(
        dir: impl AsRef<Path>,
        owned: &[u64],
        num_partitions: u64,
        varstore: Option<&VarStore>,
    ) -> io::Result<(Self, crate::seglog::MultiSegRecovery)> {
        let recovery =
            crate::seglog::recover_multi(dir.as_ref(), owned, num_partitions as usize, varstore)?;
        let shared = Arc::clone(&recovery.shared);
        let seg_active = ActiveSegment::open(Arc::clone(&shared))?;
        let (tx, rx) = mpsc::channel::<WriterMsg>();
        let exporter: ExporterCell = Arc::new(Mutex::new(HashMap::new()));
        spawn_writer(seg_active, rx, Some(Arc::clone(&exporter)))
            .expect("spawn shared journal writer thread");
        Ok((
            Self {
                tx,
                seg: Some(shared),
                exporter: Some(exporter),
            },
            recovery,
        ))
    }

    /// A fresh sender into the shared writer, for one partition's [`Journal`].
    /// The writer thread lives until every such sender is dropped.
    fn sender(&self) -> Sender<WriterMsg> {
        self.tx.clone()
    }

    /// The segmented seal state, if this is the bounded-disk multi-partition
    /// writer (used to drive per-partition snapshot/rotate and compaction).
    pub fn seg_shared(&self) -> Option<Arc<SegShared>> {
        self.seg.clone()
    }
}

impl Journal {
    /// A non-persistent journal: the engine runs purely in memory and nothing is
    /// written. Used for tests and ephemeral runs.
    pub fn in_memory() -> Self {
        Self::in_memory_partition(0)
    }

    /// Like [`Journal::in_memory`] but the engine mints keys in `partition_id`'s
    /// namespace (see [`nanobpmn_engine_core::partition_of`]).
    pub fn in_memory_partition(partition_id: u64) -> Self {
        Self {
            engine: Engine::with_partition(partition_id),
            partition_id,
            writer: None,
            writer_thread: None,
            seg: None,
            exporter: None,
            shared_exporter: None,
            fresh: true,
            spill: None,
            cold: None,
            varstore: None,
            spill_check_skip: 0,
        }
    }

    /// Like [`Journal::in_memory_partition`] but seeds the engine by replaying
    /// `events` (non-persistent). Used to rebuild a volatile state machine from a
    /// snapshot body (a serialized event history) — the same replay path as crash
    /// recovery, minus the writer.
    pub fn in_memory_from_events(partition_id: u64, events: Vec<Event>) -> Self {
        let engine = if events.is_empty() {
            Engine::with_partition(partition_id)
        } else {
            Engine::replay_partition(partition_id, events)
        };
        Self {
            engine,
            partition_id,
            writer: None,
            writer_thread: None,
            seg: None,
            exporter: None,
            shared_exporter: None,
            fresh: false,
            spill: None,
            cold: None,
            varstore: None,
            spill_check_skip: 0,
        }
    }

    /// Like [`Journal::in_memory_from_events`] but rebuilds the engine from a
    /// compact [`EngineSnapshot`](nanobpmn_engine_core::EngineSnapshot) instead of
    /// replaying an event log — the state-based install path for a bounded Raft
    /// state-machine snapshot.
    pub fn in_memory_from_snapshot(snapshot: nanobpmn_engine_core::EngineSnapshot) -> Self {
        Self {
            engine: Engine::from_snapshot(snapshot),
            partition_id: 0,
            writer: None,
            writer_thread: None,
            seg: None,
            exporter: None,
            shared_exporter: None,
            fresh: false,
            spill: None,
            cold: None,
            varstore: None,
            spill_check_skip: 0,
        }
    }

    /// Reads and deserializes every event from the journal log at `path` (an
    /// empty vec if the file does not exist). Shared by [`Journal::open`] (to
    /// replay into the engine) and the boot-time read-model catch-up.
    pub fn read_events(path: impl AsRef<Path>) -> io::Result<Vec<Event>> {
        let path = path.as_ref();
        let mut events = Vec::new();
        if path.exists() {
            for line in BufReader::new(File::open(path)?).lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let event: Event = serde_json::from_str(&line)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                events.push(event);
            }
        }
        Ok(events)
    }

    /// Opens (creating if absent) the journal at `path`, replaying any existing
    /// log to reconstruct engine state, then spawns the background writer thread
    /// positioned to append.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_partition(path, 0)
    }

    /// Like [`Journal::open`] but the engine owns `partition_id`'s key namespace.
    /// Replay only advances this partition's local key counter (foreign keys, e.g.
    /// a deployment replicated from partition 0, are applied to state but do not
    /// advance the counter — see [`Engine::replay_partition`]).
    pub fn open_partition(path: impl AsRef<Path>, partition_id: u64) -> io::Result<Self> {
        let path = path.as_ref();
        let mut engine = Engine::with_partition(partition_id);
        let mut fresh = true;

        let events = Self::read_events(path)?;
        if !events.is_empty() {
            engine = Engine::replay_partition(partition_id, events);
            fresh = false;
        }

        let shared = SegShared::legacy(path.to_path_buf());
        let seg_active = ActiveSegment::open(shared)?;
        let (tx, rx) = mpsc::channel::<WriterMsg>();
        let writer_thread =
            spawn_writer(seg_active, rx, None).expect("spawn journal writer thread");

        Ok(Self {
            engine,
            partition_id,
            writer: Some(tx),
            writer_thread: Some(writer_thread),
            seg: None,
            exporter: None,
            shared_exporter: None,
            fresh,
            spill: None,
            cold: None,
            varstore: None,
            spill_check_skip: 0,
        })
    }

    /// Opens (or recovers) a **segmented** journal rooted at directory `dir` —
    /// the bounded-disk single-partition path. Loads the latest snapshot, replays
    /// only the events it did not cover, and spawns the segment-aware writer so
    /// the active segment continues from the recovered boundary. The returned
    /// surviving event list + `first_index` let the caller catch the read model
    /// up (events compacted before `first_index` are already in the read model).
    pub fn open_segmented(dir: impl AsRef<Path>) -> io::Result<(Self, crate::seglog::SegRecovery)> {
        let (engine, recovery) = crate::seglog::recover(dir.as_ref())?;
        let fresh = recovery.fresh;
        let shared = Arc::clone(&recovery.shared);
        let seg_active = ActiveSegment::open(Arc::clone(&shared))?;
        let (tx, rx) = mpsc::channel::<WriterMsg>();
        let writer_thread =
            spawn_writer(seg_active, rx, None).expect("spawn journal writer thread");

        let journal = Self {
            engine,
            partition_id: 0,
            writer: Some(tx),
            writer_thread: Some(writer_thread),
            seg: Some(shared),
            exporter: None,
            shared_exporter: None,
            fresh,
            spill: None,
            cold: None,
            varstore: None,
            spill_check_skip: 0,
        };
        Ok((journal, recovery))
    }

    /// Builds a partition journal that persists through a [`SharedWriter`] (one
    /// fsync stream shared across every partition) instead of a private
    /// file+thread. `events` are *this* partition's events, already split out of
    /// the shared log by the caller (which reads the single log once and routes
    /// each event to `partition_of(key)`); they reconstruct the engine exactly as
    /// [`Journal::open_partition`] would. The journal owns a cloned sender into
    /// the shared writer, so the writer thread stays alive as long as it does.
    pub fn from_events_shared(
        partition_id: u64,
        events: Vec<Event>,
        shared: &SharedWriter,
    ) -> Self {
        let fresh = events.is_empty();
        let engine = if fresh {
            Engine::with_partition(partition_id)
        } else {
            Engine::replay_partition(partition_id, events)
        };
        Self {
            engine,
            partition_id,
            writer: Some(shared.sender()),
            writer_thread: None,
            seg: shared.seg.clone(),
            exporter: None,
            shared_exporter: shared.exporter.clone(),
            fresh,
            spill: None,
            cold: None,
            varstore: None,
            spill_check_skip: 0,
        }
    }

    /// Builds a partition journal that persists through a segmented
    /// [`SharedWriter`] from an already-reconstructed `engine` (the bounded-disk
    /// multi-partition recovery path). Unlike [`Journal::from_events_shared`],
    /// which replays this partition's full event history, the engine here was
    /// rebuilt from a combined snapshot + surviving-tail replay by
    /// [`crate::seglog::recover_multi`]. Inherits the writer's segmented seal
    /// state and exporter cell so snapshots/compaction and read-model forwarding
    /// work for this partition.
    pub fn from_engine_shared(
        partition_id: u64,
        engine: Engine,
        fresh: bool,
        shared: &SharedWriter,
    ) -> Self {
        Self {
            engine,
            partition_id,
            writer: Some(shared.sender()),
            writer_thread: None,
            seg: shared.seg.clone(),
            exporter: None,
            shared_exporter: shared.exporter.clone(),
            fresh,
            spill: None,
            cold: None,
            varstore: None,
            spill_check_skip: 0,
        }
    }

    /// Installs an already-minted deployment (the [`Event`]s from a `Deploy`
    /// command processed on another partition) into this partition's engine without minting new keys, so every partition shares the identical
    /// process definition under the identical key. Not journaled here: a
    /// multi-partition host re-derives the replication on restart from the
    /// deployment partition's log (which is the single durable record of the
    /// deployment). See [`Engine::install_deployment`].
    pub fn install_deployment(&mut self, events: &[Event]) {
        self.engine.install_deployment(events);
    }

    /// Durably installs a deployment replicated from another node: registers the
    /// definition(s) (no key minting, no start subscriptions — `ProcessDeployed`
    /// events only) **and** journals them so they survive this node's
    /// independent restart. Unlike [`install_deployment`](Self::install_deployment)
    /// (in-memory only, re-derived on restart from the local deployment
    /// partition's log), a clustered peer owns no deployment partition, so it must
    /// record its own durable copy. The journaled `ProcessDeployed` events carry
    /// the deployment partition's keys; the restart demux replays every
    /// `ProcessDeployed` into every owned partition (definitions are partition-
    /// agnostic), so a single durable copy reconstructs the definition on all of
    /// this node's partitions. Returns a [`Commit`] the caller awaits before
    /// acknowledging the install.
    pub fn install_deployment_durable(&mut self, events: &[Event]) -> Commit {
        self.engine.install_deployment(events);
        let deployed: Vec<Event> = events
            .iter()
            .filter(|e| matches!(e, Event::ProcessDeployed { .. }))
            .cloned()
            .collect();
        self.persist(&Arc::new(deployed))
    }

    /// Wires the disk-backed variable spill. `budget` is the maximum number of
    /// resident instances allowed to hold their variables in hot RAM; once the
    /// active backlog exceeds it, each command that grows the backlog sheds the
    /// oldest instances' variables to `store` (rehydrated on job activation). Set
    /// before serving. A `budget` of 0 spills aggressively (keeps nothing extra
    /// resident); leaving the spill unset keeps the original all-resident
    /// behaviour.
    pub fn set_spill(&mut self, store: Arc<VarSpillStore>, budget: usize) {
        self.spill = Some(VarSpill {
            store,
            trigger: VarSpillTrigger::Budget(budget),
        });
    }

    /// Wires the authoritative durable variable store (lean-snapshot mode) and
    /// enables engine dirty-variable tracking so the periodic checkpoint can write
    /// just the delta. Under lean mode the snapshot carries only control state;
    /// variable spill write-through, job-activation rehydration and terminal
    /// forget all target this store instead of the destructive spill cache. Set
    /// before serving.
    pub fn set_varstore(&mut self, store: Arc<VarStore>) {
        self.engine.set_track_dirty_vars(true);
        // Seed the dirty set with every currently-resident instance so the FIRST
        // lean checkpoint writes the full current variable state into the store.
        // This migrates a deployment booting from a pre-existing full snapshot
        // (var store empty) and is a harmless idempotent re-write on a normal
        // lean restart (recovery already installed the same maps).
        self.engine.mark_all_dirty();
        self.varstore = Some(store);
    }

    /// Whether lean-snapshot mode (authoritative durable var store) is wired.
    pub fn has_varstore(&self) -> bool {
        self.varstore.is_some()
    }

    /// Wires *adaptive* variable spill onto `store`. The oldest active-backlog
    /// variables are shed strictly on **measured memory** (see
    /// [`VarSpillTrigger::Adaptive`]):
    ///
    /// - below `high_water` with free RAM: never spill — full throughput;
    /// - resident `>= high_water`: reclaim to `low_water` — the OOM guard;
    /// - live system available memory below `reserve`: reclaim to `floor` (free
    ///   RAM contended — reclaim harder).
    ///
    /// The memory check runs on the periodic sweep
    /// ([`maybe_var_spill_pressure`](Journal::maybe_var_spill_pressure));
    /// `hard_cap` is a per-command instance-count backstop for a runaway between
    /// sweeps (0 disables it); `reserve == 0` disables the available-memory guard.
    /// Set before serving.
    pub fn set_var_spill_adaptive(
        &mut self,
        store: Arc<VarSpillStore>,
        floor: u64,
        high_water: u64,
        low_water: u64,
        reserve: u64,
        hard_cap: usize,
    ) {
        self.spill = Some(VarSpill {
            store,
            trigger: VarSpillTrigger::Adaptive {
                floor,
                high_water,
                low_water,
                reserve,
                hard_cap,
            },
        });
    }

    /// Wires cold spill onto the same disk-backed `store`. Once the engine's
    /// resident memory crosses `high_water` bytes, the periodic sweep
    /// ([`maybe_cold_spill`](Journal::maybe_cold_spill)) evicts whole dormant
    /// instances to the store — oldest first — until resident memory falls back
    /// under `low_water`, keeping only a slim routing index resident. A cold
    /// instance is rehydrated on demand the moment an event targets it. Set
    /// before serving; leaving it unset keeps every instance resident.
    pub fn set_cold_spill(&mut self, store: Arc<VarSpillStore>, high_water: u64, low_water: u64) {
        self.cold = Some(ColdSpill {
            store,
            index: ColdIndex::default(),
            high_water,
            low_water,
            shedding: false,
        });
    }

    /// How many instances are currently cold (off-heap). Zero when cold spill is
    /// unset or nothing is parked. Exposed for observability/tests.
    pub fn cold_count(&self) -> usize {
        self.cold.as_ref().map(|c| c.index.len()).unwrap_or(0)
    }

    /// Collects every live (non-terminal) instance key the engine holds for this
    /// partition — hot resident instances plus cold-spilled ones — into `out`.
    /// This is the authoritative set reconciliation compares the read model's
    /// `Active` rows against: any read row whose key is absent here corresponds to
    /// an instance the engine has already driven to a terminal state and evicted,
    /// so its read row is an orphan (its terminal event was never projected).
    pub fn collect_live_instance_keys(&self, out: &mut std::collections::HashSet<Key>) {
        out.extend(self.state().instances.keys().copied());
        if let Some(cold) = self.cold.as_ref() {
            out.extend(cold.index.keys());
        }
    }

    /// Rehydrates the cold instance `key` back into hot state: takes its snapshot
    /// from the store, restores it into the engine, and drops its routing-index
    /// entries. A no-op (returns `false`) if cold spill is unset or `key` is not
    /// cold. A store miss (snapshot lost) still clears the stale index entry so a
    /// later event cannot loop trying to rehydrate a vanished instance — the
    /// journal remains the durable source of truth.
    fn rehydrate_cold(&mut self, key: Key) -> bool {
        let store = match self.cold.as_ref() {
            Some(cold) if cold.index.contains(key) => Arc::clone(&cold.store),
            _ => return false,
        };
        let snapshot = store.take_cold(key);
        if let Some(cold) = self.cold.as_mut() {
            cold.index.remove(key);
        }
        match snapshot {
            Some(snapshot) => {
                self.engine.rehydrate_instance(snapshot);
                true
            }
            None => false,
        }
    }

    /// Rehydrates any cold instance a command is about to target, so the engine
    /// processes the command against complete hot state. Cheap when cold spill is
    /// unset or nothing relevant is cold (a few index lookups). Routing mirrors
    /// the engine's own: by job key (`CompleteJob`/`FailJob`/...), user-task key,
    /// incident key, message name+correlation (`CorrelateMessage`), timer due-time
    /// (`TriggerTimers`), the instance key (`CancelInstance`), or an instance-root
    /// or element-instance scope key (`SetVariables`).
    fn ensure_resident_for_command(&mut self, command: &Command) {
        let Some(cold) = self.cold.as_ref() else {
            return;
        };
        let mut targets: Vec<Key> = Vec::new();
        match command {
            Command::CompleteJob { job_key, .. }
            | Command::FailJob { job_key, .. }
            | Command::ThrowJobError { job_key, .. }
            | Command::UpdateJobRetries { job_key, .. } => {
                targets.extend(cold.index.instance_for_job(*job_key));
            }
            Command::AssignUserTask { user_task_key, .. }
            | Command::UnassignUserTask { user_task_key }
            | Command::UpdateUserTask { user_task_key, .. }
            | Command::CompleteUserTask { user_task_key, .. } => {
                targets.extend(cold.index.instance_for_user_task(*user_task_key));
            }
            Command::ResolveIncident { incident_key, .. } => {
                targets.extend(cold.index.instance_for_incident(*incident_key));
            }
            Command::SetVariables { scope_key, .. } => {
                // The scope may be the instance root or one of its active
                // element-instance (token) scopes; both can address a cold
                // instance and must page it back in before the update applies.
                if cold.index.contains(*scope_key) {
                    targets.push(*scope_key);
                } else {
                    targets.extend(cold.index.instance_for_scope(*scope_key));
                }
            }
            Command::CancelInstance { instance_key } => {
                if cold.index.contains(*instance_key) {
                    targets.push(*instance_key);
                }
            }
            Command::CorrelateMessage {
                message_name,
                correlation_key,
                ..
            } => {
                targets.extend(
                    cold.index
                        .instances_for_message(message_name, correlation_key),
                );
            }
            Command::TriggerTimers { now } => {
                targets.extend(cold.index.instances_due(*now));
            }
            _ => {}
        }
        for key in targets {
            self.rehydrate_cold(key);
        }
    }

    /// Sheds dormant instances to disk when hot RAM crosses the high-water mark.
    /// Cheap below the mark (one resident-bytes read). Above it, snapshots the
    /// oldest cold-spillable instances in bounded batches — lifting each whole
    /// instance (control state, jobs, timers, subscriptions) out of the engine
    /// and into the store, recording only its routing facets in the resident
    /// index — until resident memory falls under the low-water mark or no dormant
    /// instance remains. Run from the background tick on the engine thread. The
    /// snapshots are derived from the durable journal, so a lost one is
    /// reconstructable: this is a memory cache, not a system of record.
    pub fn maybe_cold_spill(&mut self) {
        let (high, low, shedding) = match self.cold.as_ref() {
            Some(cold) if cold.high_water > 0 => (cold.high_water, cold.low_water, cold.shedding),
            _ => return,
        };
        let Some(resident) = crate::memory::resident_bytes() else {
            return;
        };
        let resident = resident as u64;
        // Hysteresis: engage at `high_water`, then keep shedding across ticks until
        // resident falls under `low_water` (the OOM guard's reclaim target).
        let threshold = if shedding { low } else { high };
        if resident < threshold {
            if shedding {
                self.cold.as_mut().expect("cold set").shedding = false;
            }
            return;
        }
        self.cold.as_mut().expect("cold set").shedding = true;
        let store = Arc::clone(&self.cold.as_ref().expect("cold set").store);
        // Evict dormant instances under a bounded per-tick wall-clock budget
        // ([`SPILL_TICK_BUDGET`]), then purge ONCE — same rationale as the variable
        // sweep: holding the single writer through a `shrink()` + `purge()` per
        // 64-instance batch until resident fell under the low-water mark stalled
        // create/complete under a deep backlog. Successive ticks resume where this
        // one left off (latched by `shedding`).
        let deadline = std::time::Instant::now() + SPILL_TICK_BUDGET;
        let mut spilled = 0usize;
        loop {
            let batch = self.engine.cold_spillable_instances(64);
            if batch.is_empty() {
                break;
            }
            let mut spilled_in_batch = 0usize;
            for key in batch {
                let Some(snapshot) = self.engine.snapshot_instance(key) else {
                    continue;
                };
                if store.put_cold(key, &snapshot).is_ok() {
                    self.cold
                        .as_mut()
                        .expect("cold set")
                        .index
                        .insert(&snapshot);
                    spilled_in_batch += 1;
                } else {
                    // Store write failed: keep the instance resident rather than
                    // drop it from hot state with no durable cold copy.
                    self.engine.rehydrate_instance(snapshot);
                }
            }
            spilled += spilled_in_batch;
            if spilled_in_batch == 0 || std::time::Instant::now() >= deadline {
                break;
            }
        }
        if spilled > 0 {
            let _ = crate::memory::purge();
            tracing::info!(
                "cold spill: shed {spilled} dormant instance(s) to disk ({} now cold, budgeted)",
                self.cold.as_ref().expect("cold set").index.len(),
            );
            // Clear the latch once this sweep has dropped resident under the
            // low-water target. Cold spill removes *whole* instances (unlike a
            // variable spill), so the engine's state maps now carry dead capacity:
            // compact them ONCE here, at the end of the shedding episode, rather
            // than `shrink_to_fit`-ing all ten maps on every firing tick.
            if crate::memory::resident_bytes().is_some_and(|now| (now as u64) < low) {
                self.cold.as_mut().expect("cold set").shedding = false;
                self.engine.shrink();
            }
        }
    }

    /// Test hook: whether cold-spill is configured (a cold store is wired). Used
    /// to assert that lazily-created Raft replica engines inherit the same spill
    /// tiers as owned engines, so a follower can reclaim hot RAM.
    #[cfg(test)]
    pub fn cold_spill_configured(&self) -> bool {
        self.cold.is_some()
    }

    /// Test hook: cold-spills every currently dormant instance regardless of the
    /// memory watermark, returning the number shed. Lets tests exercise the
    /// spill/rehydrate seams deterministically without driving real RAM pressure.
    #[cfg(test)]
    pub fn force_cold_spill_all(&mut self) -> usize {
        let store = match self.cold.as_ref() {
            Some(cold) => Arc::clone(&cold.store),
            None => return 0,
        };
        let mut spilled = 0usize;
        loop {
            let batch = self.engine.cold_spillable_instances(64);
            if batch.is_empty() {
                break;
            }
            let mut spilled_in_batch = 0usize;
            for key in batch {
                let Some(snapshot) = self.engine.snapshot_instance(key) else {
                    continue;
                };
                if store.put_cold(key, &snapshot).is_ok() {
                    self.cold
                        .as_mut()
                        .expect("cold set")
                        .index
                        .insert(&snapshot);
                    spilled_in_batch += 1;
                } else {
                    self.engine.rehydrate_instance(snapshot);
                }
            }
            spilled += spilled_in_batch;
            if spilled_in_batch == 0 {
                break;
            }
        }
        spilled
    }

    /// Wires the read-model exporter channel. Set before any command is applied
    /// (including demo seeding) so every journaled event is projected.
    ///
    /// For a journal backed by a **segmented shared writer** the exporter is
    /// dropped into the writer's cell instead of `self.exporter`, so the writer
    /// forwards events to the read model in fsync (log) order — keeping
    /// `exported_position` a true global prefix across partitions — and
    /// `persist()` does not also forward (which would double-project).
    ///
    /// `queued` is the partition's exporter-queue byte gauge, wired into the
    /// shared cell so the writer can account each forwarded command's size for
    /// create-admission backpressure. It is unused on the non-shared path (which
    /// does not participate in exporter-queue accounting; `bytes` is always 0
    /// there).
    pub fn set_exporter(&mut self, exporter: Sender<ExportBatch>, queued: Arc<AtomicU64>) {
        if let Some(cell) = self.shared_exporter.as_ref() {
            cell.lock()
                .unwrap()
                .insert(self.partition_id, (exporter, queued));
        } else {
            self.exporter = Some(exporter);
        }
    }

    /// Evicts a batch of completed instances in a single pass over hot state.
    /// Mirrors [`Engine::evict_instances`]. Used on the steady-state exporter
    /// path once the read model has the completions. Also drops any spilled
    /// variable / cold rows the evicted (now terminal) instances left behind, so
    /// the spill store stays bounded to the live backlog rather than accumulating
    /// orphan payloads for every completed or cancelled instance.
    pub fn evict_instances(&mut self, keys: &[Key]) -> usize {
        if let Some(store) = self.spill_store() {
            store.forget(keys);
        }
        self.engine.evict_instances(keys)
    }

    /// The shared disk-backed spill/cold store, if either tier is wired. Both
    /// tiers share one [`VarSpillStore`] (one file, one WAL), so either handle
    /// reaches the same `spill` and `cold` tables.
    fn spill_store(&self) -> Option<Arc<VarSpillStore>> {
        if let Some(vs) = self.spill.as_ref() {
            return Some(Arc::clone(&vs.store));
        }
        self.cold.as_ref().map(|c| Arc::clone(&c.store))
    }

    /// Evicts every completed instance from hot state (used after a boot replay,
    /// once the read model is caught up). Mirrors [`Engine::evict_completed`].
    pub fn evict_completed(&mut self) -> usize {
        self.engine.evict_completed()
    }

    /// Shrinks the hot-state map capacities to fit their live contents, returning
    /// the bucket arrays freed by prior eviction back to the allocator. Mirrors
    /// [`Engine::shrink`]; called on the idle-purge path, where steady-state
    /// eviction has already removed the entries but left the maps at peak
    /// capacity.
    pub fn shrink(&mut self) {
        self.engine.shrink();
    }

    /// Whether the journal started empty (no prior log).
    pub fn is_fresh(&self) -> bool {
        self.fresh
    }

    /// Serializes `events` for the durable writer and forwards them to the
    /// read-model exporter, returning a [`Commit`] that resolves once they are
    /// fsynced. The events are shared via `Arc`, so the exporter handoff is a
    /// refcount bump rather than a deep copy of the (up to 50 KB) payloads — the
    /// single command thread never clones them. Empty batches and in-memory
    /// journals are already durable, so they return a ready commit.
    fn persist(&self, events: &Arc<Vec<Event>>) -> Commit {
        if events.is_empty() {
            return Commit::ready();
        }

        // Forward to the read-model exporter in command order (the actor applies
        // commands serially). Independent of disk persistence, so the in-memory
        // journal still feeds an in-memory read store. `Arc::clone` is cheap.
        //
        // A segmented shared-writer-backed journal does NOT forward here: its
        // writer forwards in fsync (log) order via the exporter cell (see
        // `set_exporter`), so `exported_position` stays a true global prefix
        // across partitions. Forwarding here too would double-project.
        let shared_backed = self.shared_exporter.is_some();
        if !shared_backed && let Some(exporter) = self.exporter.as_ref() {
            // Non-shared path (in-memory / legacy single-file): forward with a
            // zero byte estimate — this path has no exporter-queue backpressure
            // gauge, so it never accounts (and never sheds) here.
            let _ = exporter.send(ExportBatch {
                events: Arc::clone(events),
                bytes: 0,
            });
        }

        let Some(writer) = self.writer.as_ref() else {
            return Commit::ready();
        };

        // The shared multi-partition segmented writer records each event line
        // with its GLOBAL write-partition tag (`<partition>\t<json>`), so
        // recovery can demultiplex by the partition that actually produced the
        // write rather than by the event's key. This matters for a clustered
        // node's durable replicated `ProcessDeployed`: it is journaled under the
        // node's first-owned partition but keyed to the deployment partition (0),
        // which a node not owning partition 0 could not otherwise route back. The
        // single-partition / legacy / in-memory paths keep the bare `<json>` line
        // format (their reader does not expect a tag).
        let tagged = shared_backed;
        let mut bytes = Vec::new();
        for event in events.iter() {
            if tagged {
                bytes.extend_from_slice(self.partition_id.to_string().as_bytes());
                bytes.push(b'\t');
            }
            let line = serde_json::to_string(event).expect("event serializes");
            bytes.extend_from_slice(line.as_bytes());
            bytes.push(b'\n');
        }

        let (ack, rx) = oneshot::channel();
        let events_count = events.len();
        let bytes_len = bytes.len();
        // Only the segmented shared-writer path needs the events Arc (for
        // log-order exporter forwarding) and partition tagging; everything else
        // leaves `events_arc` `None` to avoid an extra refcount/clone.
        let events_arc = shared_backed.then(|| Arc::clone(events));
        match writer.send(WriterMsg::Write(WriteRequest {
            bytes,
            events: events_count,
            partition: self.partition_id,
            events_arc,
            ack,
        })) {
            Ok(()) => {
                crate::metrics::inflight_inc();
                crate::metrics::journal_inflight_add(bytes_len);
                Commit(CommitInner::Pending(rx))
            }
            // The writer thread is gone (shutting down); treat as already settled
            // so callers never hang.
            Err(_) => Commit::ready(),
        }
    }

    /// Applies a durable command at logical instant `now`, journaling its events
    /// on success. Returns the events (shared via `Arc` with the read-model
    /// exporter) and a [`Commit`] the caller should await before acknowledging the
    /// request. Mirrors [`Engine::apply_command_at`].
    pub fn apply_command_at(
        &mut self,
        command: Command,
        now: u64,
    ) -> Result<(Arc<Vec<Event>>, Commit), EngineError> {
        self.ensure_resident_for_command(&command);
        let events = Arc::new(self.engine.apply_command_at(command, now)?);
        let commit = self.persist(&events);
        self.maybe_spill();
        Ok((events, commit))
    }

    /// Applies a durable command using the engine's current clock, journaling its
    /// events on success. Mirrors [`Engine::apply_command`].
    pub fn apply_command(
        &mut self,
        command: Command,
    ) -> Result<(Arc<Vec<Event>>, Commit), EngineError> {
        self.ensure_resident_for_command(&command);
        let events = Arc::new(self.engine.apply_command(command)?);
        let commit = self.persist(&events);
        self.maybe_spill();
        Ok((events, commit))
    }

    /// Per-command variable-spill check. Sheds the oldest backlog's variables to
    /// the spill store when the resident spill-candidate set exceeds the mode's
    /// per-command cap (the fixed budget in `Budget` mode, or the emergency
    /// `hard_cap` in `Adaptive` mode). The variables are already durable in the
    /// journal, so a spill write that is later lost is reconstructable — it is a
    /// cache, not a system of record, which makes the exact shed timing a soft
    /// bound we can amortise.
    ///
    /// Hot-path cost: an O(1) resident-count guard skips everything while the
    /// whole resident set fits the cap (the steady state — and in `Adaptive` mode
    /// the normal case, since its real trigger is the RSS sweep). Once
    /// persistently over the cap, the precise O(N) spillable scan/shed runs only
    /// once every [`SPILL_CHECK_INTERVAL`] commands rather than on every apply, so
    /// a deep backlog no longer pays an O(N) scan per command (which was quadratic
    /// in the backlog and drove congestion collapse).
    /// Approximate resident variable-payload bytes held by this partition's
    /// instances (spilled instances contribute ~0). Attribution gauge for the
    /// burst RSS balloon — see [`Engine::resident_variable_bytes`]. O(N); called
    /// off the hot path by the mem-pressure sampler.
    pub fn resident_variable_bytes(&self) -> u64 {
        self.engine.resident_variable_bytes()
    }

    fn maybe_spill(&mut self) {
        let Some(cap) = self
            .spill
            .as_ref()
            .and_then(|vs| vs.trigger.per_command_cap())
        else {
            return;
        };
        // O(1) fast path: spill candidates are a subset of resident instances, so
        // when the resident set already fits the cap nothing can be over it.
        if self.engine.resident_instance_count() <= cap {
            self.spill_check_skip = 0;
            return;
        }
        // Over the cheap bound: amortise the precise scan/shed across commands.
        if self.spill_check_skip > 0 {
            self.spill_check_skip -= 1;
            return;
        }
        self.spill_check_skip = SPILL_CHECK_INTERVAL;
        self.shed_variables(cap);
    }

    /// Sheds the oldest spill-candidate instances' variables to disk until the
    /// resident spill-candidate count is back down to `target`. Returns how many
    /// instances were shed. Shared by the per-command cap check and the adaptive
    /// RAM-pressure sweep so the two triggers never diverge on *how* they shed.
    fn shed_variables(&mut self, target: usize) -> usize {
        let resident = self.engine.resident_spillable_count();
        if resident <= target {
            return 0;
        }
        // Lean mode: spill write-through targets the authoritative durable store
        // (the row must be durable before the payload leaves RAM, since the store
        // is the only copy recovery reads). Classic mode uses the destructive
        // spill cache.
        let varstore = self.varstore.clone();
        let spill_store = self.spill.as_ref().map(|vs| Arc::clone(&vs.store));
        if varstore.is_none() && spill_store.is_none() {
            return 0;
        }
        let over = resident - target;
        let mut shed = 0;
        for key in self.engine.spillable_instances(over) {
            if let Some(vars) = self.engine.spill_variables(key) {
                let ok = if let Some(vstore) = varstore.as_ref() {
                    vstore.put_current(key, &vars).is_ok()
                } else {
                    spill_store.as_ref().unwrap().put(key, &vars).is_ok()
                };
                if ok {
                    shed += 1;
                } else {
                    // Spill failed: keep the payload resident rather than lose it.
                    self.engine.rehydrate_variables(key, vars);
                }
            }
        }
        shed
    }

    /// Adaptive variable spill — the RAM-pressure tier. Run on the periodic sweep
    /// (alongside [`maybe_cold_spill`](Journal::maybe_cold_spill), and *before* it:
    /// shedding an instance's variables keeps it live and is cheaper than evicting
    /// the whole instance). Only active in [`Adaptive`](VarSpillTrigger::Adaptive)
    /// mode; a no-op otherwise (`Budget`/off self-regulate per command).
    ///
    /// **Memory is the sole trigger.** Spill exists to bound resident memory, so
    /// it fires strictly on measured memory — jemalloc `resident` over the
    /// `high_water` mark, or live system-available memory below `reserve` — and
    /// sheds toward the reclaim target. Instance *count* is deliberately not a
    /// signal: a count is a poor proxy for bytes (N tiny-var instances is trivial
    /// RAM, N large-var instances is not), and steering on it throttles throughput
    /// for a vanity metric. A large but *stable* working set below the mark keeps
    /// every variable resident at full throughput.
    ///
    /// Once over the mark it sheds the oldest active-backlog variables in batches
    /// — compacting and purging between batches so the reading reflects the shed —
    /// until resident memory falls under the reclaim target or nothing spillable
    /// remains. There is no RSS-delta bail: under concurrent inbound allocation a
    /// shed batch may not visibly drop RSS even though it helped, so the loop
    /// steers to the memory target and stops only when genuinely under it (or out
    /// of candidates). A single up-front byte check
    /// ([`VAR_SPILL_MIN_RELIEF_DIVISOR`]) skips the sweep when variables are too
    /// small a fraction of the overshoot to matter, so a tiny-payload backlog is
    /// not thrashed for negligible relief.
    pub fn maybe_var_spill_pressure(&mut self) {
        let (floor, high, low, reserve) = match self.spill.as_ref().map(|vs| &vs.trigger) {
            Some(VarSpillTrigger::Adaptive {
                floor,
                high_water,
                low_water,
                reserve,
                ..
            }) if *high_water > 0 => (*floor, *high_water, *low_water, *reserve),
            _ => return,
        };
        let Some(resident) = crate::memory::resident_bytes() else {
            return;
        };
        let resident = resident as u64;

        // Pure memory trigger: resident heap over the high-water OOM mark, or live
        // system-available memory below the reserve (another tenant eating RAM).
        // Nothing else — no instance-count trend.
        let avail_pressure =
            reserve > 0 && crate::memory::available_bytes().is_some_and(|a| a < reserve);
        if resident < high && !avail_pressure {
            // Below the mark: use free RAM at full throughput.
            return;
        }
        // Reclaim to the floor budget on external (reserve) pressure — free RAM is
        // contended, get aggressive; otherwise relax only to the low-water mark
        // (hysteresis below high-water).
        let target_low = if avail_pressure { floor } else { low };

        // Futile-shed guard (byte-based, not count-based): if the resident variable
        // payloads we could shed are a small fraction of the overshoot, variables
        // are not the memory driver — shedding them thrashes the backlog for ~no
        // relief. Hand the residual to cold spill / admission backpressure. Decided
        // once, up front, from a direct byte measurement rather than a noisy
        // per-batch RSS delta.
        let overshoot = resident.saturating_sub(target_low);
        let spillable_bytes = self.engine.resident_variable_bytes();
        if spillable_bytes < overshoot / VAR_SPILL_MIN_RELIEF_DIVISOR {
            return;
        }

        // Shed the oldest active-backlog variables under a bounded per-tick
        // wall-clock budget ([`SPILL_TICK_BUDGET`]), then purge ONCE. The original
        // design ran `shrink()` + jemalloc `purge()` + a resident re-read after
        // *every* batch, looping until resident fell under the target — a >100 ms
        // single-writer hold per tick under a deep backlog that starved
        // create/complete. Now the hold is bounded: shed what fits the budget,
        // then return the freed pages to the OS once so the next sweep's resident
        // reading is accurate (jemalloc's background thread would otherwise defer
        // the page return past its decay window and over-fire the RSS gate).
        //
        // No `shrink()` here: a variable spill keeps the *instance* resident (only
        // its `variables` map is emptied), so the engine's state maps do not
        // shrink — a `shrink_to_fit` across all ten of them would reallocate huge
        // maps for zero relief. The relief is the freed variable payloads, which
        // `purge()` returns to the OS. Successive sweeps converge under the mark.
        let deadline = std::time::Instant::now() + SPILL_TICK_BUDGET;
        let mut total = 0usize;
        loop {
            let resident_candidates = self.engine.resident_spillable_count();
            if resident_candidates == 0 {
                break;
            }
            let target = resident_candidates.saturating_sub(VAR_SPILL_SWEEP_BATCH);
            let shed = self.shed_variables(target);
            total += shed;
            if shed == 0 || std::time::Instant::now() >= deadline {
                break;
            }
        }
        if total > 0 {
            let _ = crate::memory::purge();
            tracing::info!(
                "variable spill (adaptive): shed {total} instance(s)' variables \
                 (resident {} MiB -> target {} MiB, budgeted)",
                resident / (1024 * 1024),
                target_low / (1024 * 1024),
            );
        }
    }

    /// Test-only: force an immediate, un-throttled spill scan. Production spills
    /// are amortised (see [`maybe_spill`]/[`SPILL_CHECK_INTERVAL`]), so tests that
    /// assert on the fully-shed state drive it deterministically through here
    /// instead of depending on the amortisation cadence.
    #[cfg(test)]
    fn force_spill_scan(&mut self) {
        self.spill_check_skip = 0;
        self.maybe_spill();
    }

    /// Activates jobs **without** journaling: activation locks are volatile lease
    /// state, forfeited on restart. Mirrors [`Engine::activate_jobs`].
    ///
    /// When variable spill is wired, an activated job's instance may have had its
    /// variables shed to disk. Activation is exactly the moment they are needed
    /// again (the worker receives them, and completion may follow), so each
    /// spilled instance is rehydrated here: its payload is taken back from the
    /// store, restored into hot state, and used to fill the activated job. This
    /// is the read side of the memory/disk fusion — a single keyed SQLite read,
    /// served from the page cache for a warm working set.
    pub fn activate_jobs(
        &mut self,
        job_type: impl Into<String>,
        worker: impl Into<String>,
        max_jobs: usize,
        timeout: u64,
        now: u64,
    ) -> Vec<ActivatedJob> {
        let job_type = job_type.into();
        // Rehydrate up to `max_jobs` cold instances holding an activatable job of
        // this type, so a worker poll can reach a parked-then-cold backlog. Only
        // pays a SQLite read when something of this type is actually cold.
        if self.cold.is_some() {
            let candidates = self
                .cold
                .as_ref()
                .expect("cold set")
                .index
                .instances_for_job_type(&job_type, max_jobs);
            for key in candidates {
                self.rehydrate_cold(key);
            }
        }
        let mut activated = self
            .engine
            .activate_jobs(&job_type, worker, max_jobs, timeout, now);
        // Rehydrate any spilled variables the activated jobs need. In lean mode
        // the authoritative store is read **non-destructively** (`get`) — the row
        // must survive for the next recovery; the spilled flag is cleared so the
        // instance is resident again and the next checkpoint keeps its (unchanged)
        // row. Classic mode `take`s from the destructive spill cache.
        if self.varstore.is_some() || self.spill.is_some() {
            let varstore = self.varstore.clone();
            let spill_store = self.spill.as_ref().map(|vs| Arc::clone(&vs.store));
            for job in activated.iter_mut() {
                if !self.engine.is_variables_spilled(job.instance_key) {
                    continue;
                }
                let restored = if let Some(vstore) = varstore.as_ref() {
                    vstore.get(job.instance_key)
                } else {
                    spill_store.as_ref().and_then(|s| s.take(job.instance_key))
                };
                if let Some(vars) = restored {
                    let vars = Arc::new(vars);
                    self.engine
                        .rehydrate_variables(job.instance_key, Arc::clone(&vars));
                    // Root is resident again; recompute the merged scoped view so a
                    // job on a nested scope (sub-process / MI-child) regains its
                    // scope-local bindings — those stayed resident through the spill,
                    // but the worker's snapshot must fold the freshly restored root
                    // back in. A flat instance yields the same root `Arc` back.
                    job.variables = self
                        .engine
                        .element_variables(job.instance_key, job.element_instance_key);
                }
            }
        }
        activated
    }

    /// Fires every due timer at logical instant `now`, journaling the resulting
    /// events (timers are durable business facts). Mirrors
    /// [`Engine::trigger_timers`]; returns the events produced (empty if none
    /// were due) and their [`Commit`].
    pub fn trigger_timers(&mut self, now: u64) -> (Arc<Vec<Event>>, Commit) {
        // Rehydrate any cold instance whose armed timer is now due, so the tick
        // can fire it. No-op when nothing cold is due.
        if self.cold.is_some() {
            let due = self
                .cold
                .as_ref()
                .expect("cold set")
                .index
                .instances_due(now);
            for key in due {
                self.rehydrate_cold(key);
            }
        }
        let events = Arc::new(self.engine.trigger_timers(now));
        let commit = self.persist(&events);
        (events, commit)
    }

    /// Releases expired activation locks at logical instant `now`, **without**
    /// journaling: like activation, lock expiry is volatile lease state. Mirrors
    /// [`Engine::expire_jobs`]. Returns the reclaimed-job events so the caller can
    /// wake dispatch when a lease frees a job for redelivery.
    pub fn expire_jobs(&mut self, now: u64) -> Vec<Event> {
        self.engine.expire_jobs(now)
    }

    /// Read-only access to the underlying engine (for projections that take an
    /// `&Engine`).
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Sets the cluster-wide partition count on the underlying engine so it
    /// places message subscriptions on the partition owning their correlation
    /// key (see [`Engine::set_num_partitions`]). A no-op effect when `1`.
    pub fn set_num_partitions(&mut self, num_partitions: u64) {
        self.engine.set_num_partitions(num_partitions);
    }

    /// Enables leftover-tolerant ("lenient") job completion on the underlying
    /// engine so a replicated `CompleteJob`/`FailJob`/`ThrowJobError` applies even
    /// when this engine never observed the (leader-local) activation. See
    /// [`Engine::set_lenient_completion`]. Set identically on every replica from
    /// `NANOBPMN_REPLICATE_ACTIVATION=0`.
    pub fn set_lenient_completion(&mut self, lenient: bool) {
        self.engine.set_lenient_completion(lenient);
    }

    /// The currently-held activation leases `(job_key, deadline)` on the
    /// underlying engine — the body of a best-effort lease digest broadcast (see
    /// [`Engine::activated_leases`]). Pure read.
    pub fn activated_leases(&self) -> Vec<(u64, u64)> {
        self.engine.activated_leases()
    }

    /// Recovers a soft activation lease from a digest on the underlying engine
    /// (Created → Activated until `deadline`). Soft state only — journals nothing.
    /// See [`Engine::recover_lease`].
    pub fn recover_lease(&mut self, job_key: u64, deadline: u64, now: u64) -> bool {
        self.engine.recover_lease(job_key, deadline, now)
    }

    // --- Read delegations mirroring the engine API the server uses. ---

    pub fn state(&self) -> &State {
        self.engine.state()
    }

    /// Captures a compact, serializable snapshot of the engine's live state (see
    /// [`Engine::snapshot`]). Used to build a bounded Raft state-machine snapshot
    /// whose size tracks the working set rather than the full event history.
    pub fn engine_snapshot(&self) -> nanobpmn_engine_core::EngineSnapshot {
        self.engine.snapshot()
    }

    /// Whether this journal is backed by a segmented (bounded-disk) log.
    pub fn is_segmented(&self) -> bool {
        self.seg.is_some()
    }

    /// The global partition id this journal's engine owns (0 for single-partition).
    pub fn partition_id(&self) -> u64 {
        self.partition_id
    }

    /// Captures the engine snapshot and seals (rotates) the active segment at the
    /// exact boundary the snapshot covers, returning `(snapshot, covered_events)`
    /// — the absolute count of events the snapshot subsumes. `None` for a journal
    /// that is not segmented (or whose writer is gone).
    ///
    /// Must run **on the engine actor thread** (e.g. via `DeepthiHandle::with`):
    /// the snapshot reflects every command applied so far, and routing the rotate
    /// through the same ordered writer channel — after the writes those commands
    /// already enqueued — makes the sealed boundary line up exactly with the
    /// snapshot. No command runs between the snapshot and the seal because the
    /// actor is single-threaded.
    pub fn snapshot_and_rotate(&self) -> Option<(nanobpmn_engine_core::EngineSnapshot, u64)> {
        self.seg.as_ref()?;
        let writer = self.writer.as_ref()?;
        let snap = self.engine.snapshot();
        let (reply_tx, reply_rx) = oneshot::channel();
        if writer.send(WriterMsg::Rotate(reply_tx)).is_err() {
            return None;
        }
        let info = reply_rx.blocking_recv().ok()?;
        // On the multi-partition path the seal carries per-partition boundaries;
        // this journal's covered count is its own partition's cumulative total.
        // On the single-partition path `per_partition_end` is empty and the whole
        // log belongs to partition 0, so `info.end` is the covered count.
        let covered = info
            .per_partition_end
            .get(self.partition_id as usize)
            .copied()
            .unwrap_or(info.end);
        Some((snap, covered))
    }

    /// The lean-snapshot counterpart of
    /// [`snapshot_and_rotate`](Journal::snapshot_and_rotate): drains this
    /// partition's variable delta since the last checkpoint (the current maps of
    /// instances whose variables changed, plus the keys of instances that reached
    /// a terminal state), captures a **control-only** snapshot (empty variable
    /// maps), and seals the active segment at the covered boundary — all on the
    /// engine actor thread, so the delta, snapshot and seal describe the same
    /// point. The caller writes the delta to the durable var store and the lean
    /// snapshot to disk (both off the engine thread). Returns
    /// `(lean_snapshot, covered_events, upserts, forgets)`, or `None` if not
    /// segmented / the writer is gone. Only meaningful when a var store is wired
    /// ([`set_varstore`](Journal::set_varstore)); the drain is empty otherwise.
    pub fn snapshot_and_rotate_lean(&mut self) -> Option<LeanCheckpoint> {
        self.seg.as_ref()?;
        // Drain BEFORE the snapshot/seal so the delta reflects exactly the state
        // the control-only snapshot captures.
        let (upserts, forgets) = self.engine.drain_dirty_vars();
        let snap = self.engine.snapshot_control_only();
        let writer = self.writer.as_ref()?;
        let (reply_tx, reply_rx) = oneshot::channel();
        if writer.send(WriterMsg::Rotate(reply_tx)).is_err() {
            return None;
        }
        let info = reply_rx.blocking_recv().ok()?;
        let covered = info
            .per_partition_end
            .get(self.partition_id as usize)
            .copied()
            .unwrap_or(info.end);
        Some((snap, covered, upserts, forgets))
    }

    pub fn instance(&self, key: Key) -> Option<&ProcessInstance> {
        self.engine.instance(key)
    }

    pub fn incident(&self, key: Key) -> Option<&Incident> {
        self.engine.incident(key)
    }

    pub fn incidents(&self) -> Vec<&Incident> {
        self.engine.incidents()
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        // Close the channel so the writer thread drains any remaining requests
        // and exits, then join it to guarantee fire-and-forget writes hit disk.
        self.writer = None;
        if let Some(handle) = self.writer_thread.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use nanobpmn_engine_core::ProcessBuilder;

    use super::*;

    fn demo() -> nanobpmn_engine_core::ProcessDefinition {
        ProcessBuilder::new("demo")
            .start_event("start")
            .service_task("work", "demo-work")
            .end_event("end")
            .connect("start", "work")
            .connect("work", "end")
            .build()
            .expect("valid demo process")
    }

    #[test]
    fn reopening_a_journal_replays_persisted_state() {
        // given a journal file in a temp dir with a deploy + an instance
        let dir = std::env::temp_dir().join(format!("nanobpmn-journal-{}", std::process::id()));
        let path = dir.join("test.journal");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let instance_key = {
            let mut journal = Journal::open(&path).unwrap();
            assert!(journal.is_fresh());
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let (events, _) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            events.iter().find_map(|e| e.instance_key()).unwrap()
        };

        // when a fresh journal is opened over the same file
        let reopened = Journal::open(&path).unwrap();

        // then it is not fresh and the instance is recovered
        assert!(!reopened.is_fresh());
        assert!(reopened.instance(instance_key).is_some());
        // and the demo process is deployed exactly once (still version 1)
        assert_eq!(reopened.state().processes.len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn cold_journal() -> Journal {
        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        // Watermarks are irrelevant: tests drive spill via force_cold_spill_all.
        journal.set_cold_spill(store, 1, 0);
        journal
    }

    fn deploy_and_create(journal: &mut Journal) -> Key {
        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let (events, _) = journal
            .apply_command(Command::create_instance("demo"))
            .unwrap();
        events.iter().find_map(|e| e.instance_key()).unwrap()
    }

    #[test]
    fn cold_spilled_instance_rehydrates_on_job_activation_and_completes() {
        let mut journal = cold_journal();
        let key = deploy_and_create(&mut journal);

        // the instance is parked on the "demo-work" job: dormant and spillable
        assert!(journal.instance(key).is_some());
        assert_eq!(journal.force_cold_spill_all(), 1);
        assert_eq!(journal.cold_count(), 1);
        // its whole control state is now off-heap
        assert!(journal.instance(key).is_none());

        // a worker poll rehydrates it from the cold store and hands back the job
        let jobs = journal.activate_jobs("demo-work", "w", 1, 1_000, 0);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].instance_key, key);
        assert_eq!(journal.cold_count(), 0);
        assert!(journal.instance(key).is_some());

        // completing the rehydrated job drives the instance to completion
        let (events, _) = journal
            .apply_command(Command::complete_job(jobs[0].key))
            .unwrap();
        assert!(events
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceCompleted { instance_key } if *instance_key == key)));
    }

    #[test]
    fn set_variables_on_an_element_scope_rehydrates_a_cold_instance() {
        use nanobpmn_engine_core::Value;

        let mut journal = cold_journal();
        let key = deploy_and_create(&mut journal);

        // the active token scope at the service task — a valid SetVariables target
        // that is NOT the instance root key
        let scope_key = *journal
            .instance(key)
            .unwrap()
            .active
            .keys()
            .find(|k| **k != key)
            .expect("an element-instance scope");

        assert_eq!(journal.force_cold_spill_all(), 1);
        assert!(journal.instance(key).is_none());

        // SetVariables addresses the element scope: ensure_resident_for_command
        // must route it back to the cold instance and page it in (else the engine
        // would raise ScopeNotFound)
        let mut vars = std::collections::HashMap::new();
        vars.insert("approved".to_string(), Value::Bool(true));
        let (events, _) = journal
            .apply_command(Command::set_variables(scope_key, vars))
            .unwrap();
        assert_eq!(journal.cold_count(), 0);
        assert!(journal.instance(key).is_some());
        assert!(events.iter().any(
            |e| matches!(e, Event::VariablesUpdated { instance_key, .. } if *instance_key == key)
        ));
    }

    #[test]
    fn command_targeting_a_cold_instance_rehydrates_it_first() {
        let mut journal = cold_journal();
        let key = deploy_and_create(&mut journal);

        assert_eq!(journal.force_cold_spill_all(), 1);
        assert!(journal.instance(key).is_none());

        // CancelInstance targets a cold instance by key: ensure_resident_for_command
        // must page it back in before the engine applies the command
        let (events, _) = journal
            .apply_command(Command::cancel_instance(key))
            .unwrap();
        assert_eq!(journal.cold_count(), 0);
        assert!(events
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceTerminated { instance_key } if *instance_key == key)));
    }

    #[test]
    fn evicting_a_terminal_instance_drops_its_spilled_variable_row() {
        use nanobpmn_engine_core::Value;

        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        // Budget 0: any dormant, variable-carrying instance spills immediately.
        journal.set_spill(Arc::clone(&store), 0);

        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let mut vars = std::collections::HashMap::new();
        vars.insert("data".to_string(), Value::Str("payload".to_string()));

        // Two instances, both parked on the demo-work job with non-empty variables,
        // so maybe_spill sheds both their payloads to the store.
        let mut keys = Vec::new();
        for _ in 0..2 {
            let (events, _) = journal
                .apply_command(Command::create_instance_with("demo", vars.clone()))
                .unwrap();
            keys.push(events.iter().find_map(|e| e.instance_key()).unwrap());
        }
        let (a, b) = (keys[0], keys[1]);
        // The first over-budget command sheds eagerly; subsequent shedding is
        // amortised, so drive the scan deterministically to reach the fully-shed
        // state this test asserts on.
        journal.force_spill_scan();
        assert!(journal.engine.is_variables_spilled(a), "a spilled");
        assert!(journal.engine.is_variables_spilled(b), "b spilled");

        // Evicting the (now terminal) instance a must drop its spill row, while
        // leaving the still-live instance b's payload in the store.
        journal.evict_instances(&[a]);

        assert!(
            store.take(a).is_none(),
            "evicted instance's spill row is gone"
        );
        assert!(
            store.take(b).is_some(),
            "a live instance's spill row is untouched"
        );
    }

    #[test]
    fn adaptive_spill_keeps_everything_resident_below_pressure() {
        use nanobpmn_engine_core::Value;

        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        // Adaptive with an unreachable high-water and the per-command backstop
        // disabled (hard_cap 0): below RAM pressure nothing should ever spill, so
        // the workload runs at full throughput with every payload resident.
        journal.set_var_spill_adaptive(Arc::clone(&store), u64::MAX, u64::MAX, u64::MAX, 0, 0);

        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let mut vars = std::collections::HashMap::new();
        vars.insert("data".to_string(), Value::Str("payload".to_string()));

        let mut keys = Vec::new();
        for _ in 0..3 {
            let (events, _) = journal
                .apply_command(Command::create_instance_with("demo", vars.clone()))
                .unwrap();
            keys.push(events.iter().find_map(|e| e.instance_key()).unwrap());
        }
        // Even a forced per-command scan is a no-op: the adaptive trigger only
        // sheds on the RSS sweep (or the hard-cap backstop, here disabled).
        journal.force_spill_scan();
        assert_eq!(
            journal.engine.resident_spillable_count(),
            3,
            "no instance spills below the memory watermark"
        );
        for k in &keys {
            assert!(!journal.engine.is_variables_spilled(*k));
        }
    }

    #[test]
    fn spilled_nested_scope_instance_activates_with_the_merged_view() {
        use nanobpmn_engine_core::{ProcessBuilder, Value};

        // A sub-process with an input mapping is its own variable scope: the
        // mapping creates a scope-local `scoped` (= seed + 1) that a job inside
        // the sub-process sees merged over the root `seed`.
        fn nested() -> nanobpmn_engine_core::ProcessDefinition {
            ProcessBuilder::new("nested")
                .start_event("start")
                .sub_process("sub", "sub_start")
                .with_io(
                    "sub",
                    nanobpmn_engine_core::IoMapping {
                        inputs: vec![nanobpmn_engine_core::Mapping {
                            source: "=seed + 1".to_string(),
                            target: "scoped".to_string(),
                        }],
                        outputs: Vec::new(),
                    },
                )
                .start_event("sub_start")
                .contained_in("sub_start", "sub")
                .service_task("inner", "inner-work")
                .contained_in("inner", "sub")
                .end_event("sub_end")
                .contained_in("sub_end", "sub")
                .end_event("done")
                .connect("start", "sub")
                .connect("sub_start", "inner")
                .connect("inner", "sub_end")
                .connect("sub", "done")
                .build()
                .expect("valid nested process")
        }

        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        // Budget 0: the job-parked instance spills its root payload immediately.
        journal.set_spill(Arc::clone(&store), 0);

        let _ = journal
            .apply_command(Command::DeployProcess(nested()))
            .unwrap();
        let mut vars = std::collections::HashMap::new();
        vars.insert("seed".to_string(), Value::Int(4));
        let (events, _) = journal
            .apply_command(Command::create_instance_with("nested", vars))
            .unwrap();
        let key = events.iter().find_map(|e| e.instance_key()).unwrap();

        // Shed the root payload; the sub-process scope-local map stays resident.
        journal.force_spill_scan();
        assert!(
            journal.engine.is_variables_spilled(key),
            "the job-parked nested-scope instance spilled its root payload"
        );

        // Activating the inner job rehydrates the root AND recomputes the merged
        // scoped view: the worker must receive BOTH the restored root `seed` and
        // the resident sub-process-local `scoped` — not the root-only payload.
        let activated = journal.activate_jobs("inner-work", "w", 1, 60_000, 0);
        assert_eq!(activated.len(), 1, "the inner job activates");
        let job = &activated[0];
        assert_eq!(
            job.variables.get("seed"),
            Some(&Value::Int(4)),
            "root variable restored from the spill store"
        );
        assert_eq!(
            job.variables.get("scoped"),
            Some(&Value::Int(5)),
            "sub-process scope-local variable folded back into the worker's view"
        );
        assert!(!journal.engine.is_variables_spilled(key), "rehydrated");
    }

    #[test]
    fn adaptive_hard_cap_backstop_sheds_down_to_the_cap() {
        use nanobpmn_engine_core::Value;

        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        // Unreachable high-water (so the RSS sweep never fires in the test), but a
        // hard_cap of 1: the per-command backstop must keep at most 1 resident
        // spill candidate even without RAM pressure — the inter-sweep OOM guard.
        journal.set_var_spill_adaptive(Arc::clone(&store), u64::MAX, u64::MAX, u64::MAX, 0, 1);

        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let mut vars = std::collections::HashMap::new();
        vars.insert("data".to_string(), Value::Str("payload".to_string()));

        for _ in 0..3 {
            let (_events, _commit) = journal
                .apply_command(Command::create_instance_with("demo", vars.clone()))
                .unwrap();
        }
        journal.force_spill_scan();
        assert_eq!(
            journal.engine.resident_spillable_count(),
            1,
            "the hard-cap backstop sheds the backlog down to hard_cap"
        );
    }
}
