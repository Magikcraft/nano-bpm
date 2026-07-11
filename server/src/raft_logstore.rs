//! A crash-durable Raft log store (openraft v2 [`RaftLogStorage`]) — milestone B
//! of stage 3.
//!
//! Milestone A used an in-memory log ([`MemLogStore`](crate::raft::MemLogStore)),
//! so a client-acked command was only as durable as the applied engine state. By
//! the Raft model, the **log is the source of truth**: once an entry is fsynced
//! here and committed, a restart replays the durable log back into a fresh
//! (volatile) state machine to reconstruct engine state. That makes this store
//! the real durability boundary — the engine [`Journal`](crate::journal::Journal)
//! used by the state machine can stay in-memory, because nothing acked is lost as
//! long as it is in *this* log.
//!
//! # On-disk layout (one directory per partition replica)
//!
//! - `seg-<start>.ndjson` — the log entries, one serialized [`Entry`] per line, in
//!   index order, split into segments that roll at a byte threshold. Appended to
//!   (with `fsync`) on [`append`](RaftLogStorage::append). On
//!   [`purge`](RaftLogStorage::purge) whole sealed segments whose every entry is
//!   below the purge point are **unlinked** — never rewritten — so compaction is
//!   `O(1)` file deletes off the raft core's critical path rather than a full-log
//!   rewrite. [`truncate`](RaftLogStorage::truncate) (rare, log-conflict only)
//!   rewrites at most the single straddled segment.
//! - `vote.json` — the persisted [`Vote`]. Rewritten atomically (temp + rename +
//!   dir-fsync) on every [`save_vote`](RaftLogStorage::save_vote), because Raft
//!   correctness requires a vote to be on disk before it is acted on.
//! - `state.json` — the `last_purged` and `committed` markers, rewritten
//!   atomically when either changes. The `last_purged` marker is the durable
//!   record of a purge; physical segment deletion is best-effort and reconciled on
//!   restart (entries at or below `last_purged` are filtered out on replay), so a
//!   crash mid-purge never resurrects purged entries.
//!
//! # Durability vs. concurrency
//!
//! Writes `fsync` synchronously inside the async method (the same idiom as
//! openraft's own file/rocks example stores). openraft batches a replication
//! round's entries into a single `append`, so this is one `fsync` per round.
//! A future optimization could offload the `fsync` to a dedicated writer thread
//! (mirroring [`Journal`]'s group-commit writer) to decouple it from the raft
//! core task.

// Additive until leader routing mounts it; see `raft.rs` for the rationale.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, StorageError, StorageIOError, Vote};
use serde::{Deserialize, Serialize};

use crate::raft::{NodeId, RaftConfig};

/// Durability mode for the Raft log store, mirroring the engine journal
/// ([`crate::journal`]) so a node honours one `NANOBPMN_DURABILITY` setting for
/// both its applied-state journal and its replicated Raft log.
///
/// - `Sync` (default): every `append` is `fsync`ed before its openraft flush
///   callback fires, and the committed/purge markers are `fsync`ed on write. The
///   strongest contract, at the cost of a media barrier (an `F_FULLFSYNC` on
///   macOS) on every replication round.
/// - `Async`: an `append` is acknowledged once it reaches the OS page cache, and
///   `fsync` is amortised onto a background cadence (every [`AsyncFlush::interval`]
///   or [`AsyncFlush::max_bytes`]). The committed marker — an *optional* openraft
///   optimisation, re-derived from the log on restart — is likewise deferred.
///   A **process** crash loses nothing (the appended bytes survive in the page
///   cache and replay on restart); only an **OS crash / power loss** can lose the
///   unfsynced tail, bounded by the flush interval. The leader's election vote is
///   still `fsync`ed synchronously in both modes, because Raft safety requires a
///   vote to be on disk before it is acted on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DurabilityMode {
    Sync,
    Async,
}

/// Async-durability flush policy: `fsync` fires when either the bytes appended
/// since the last fsync reach `max_bytes`, or `interval` elapses (whichever is
/// first). Reuses the journal's `NANOBPMN_ASYNC_FLUSH_MS` / `_BYTES` knobs.
#[derive(Clone, Copy)]
struct AsyncFlush {
    interval: Duration,
    max_bytes: usize,
}

/// Durability mode from `NANOBPMN_DURABILITY` (`sync` | `async`). Defaults to
/// `sync` so the strong fsync-before-ack contract is unchanged unless async is
/// explicitly opted into (same default and variable as the journal).
fn durability_mode_from_env() -> DurabilityMode {
    match std::env::var("NANOBPMN_DURABILITY") {
        Ok(v) if v.trim().eq_ignore_ascii_case("async") => DurabilityMode::Async,
        _ => DurabilityMode::Sync,
    }
}

/// Async flush policy from env. `NANOBPMN_ASYNC_FLUSH_MS` (default 10, clamped to
/// 1s) bounds the unfsynced time window; `NANOBPMN_ASYNC_FLUSH_BYTES` (default
/// 8 MiB) bounds the unfsynced byte window. Shared with the journal.
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

/// The durable markers persisted in `state.json`.
#[derive(Default, Serialize, Deserialize)]
struct PersistedState {
    last_purged: Option<LogId<NodeId>>,
    committed: Option<LogId<NodeId>>,
}

/// A log entry held in the in-memory index, tagged with its serialized on-disk
/// byte length so purge/truncate can adjust the live-byte footprint in `O(dropped)`
/// without re-serializing anything.
struct Stored {
    entry: Entry<RaftConfig>,
    len: usize,
}

/// One on-disk log segment (`seg-<start>.ndjson`). Entries are appended in index
/// order to the *active* (last) segment; older segments are sealed and, once every
/// entry they hold has been purged, unlinked wholesale — no segment is ever
/// rewritten by `purge`, so log compaction is `O(1)` file deletes off the raft
/// core's critical path rather than a full-log rewrite.
struct SegMeta {
    /// The index this segment starts at (also encoded in its file name).
    start: u64,
    /// The highest index appended to this segment, or `None` while it is empty.
    last: Option<u64>,
}

/// Default active-segment roll threshold: seal the active segment and start a new
/// one once it reaches this many bytes. Bounds both the purge granule (a whole
/// sealed segment is reclaimed at once) and the worst-case `truncate` rewrite (only
/// the single straddled segment is rewritten). Overridable via `NANOBPMN_RAFT_SEG_BYTES`.
const DEFAULT_SEG_MAX_BYTES: usize = 64 << 20;

/// Active-segment roll threshold from `NANOBPMN_RAFT_SEG_BYTES` (bytes), clamped to
/// a 1 MiB floor. Read once at open.
fn seg_max_bytes_from_env() -> usize {
    std::env::var("NANOBPMN_RAFT_SEG_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .map(|n| n.max(1 << 20))
        .unwrap_or(DEFAULT_SEG_MAX_BYTES)
}

fn seg_path(dir: &Path, start: u64) -> PathBuf {
    // Zero-padded so the file names also sort lexically in index order.
    dir.join(format!("seg-{start:020}.ndjson"))
}

/// Parses a segment file name (`seg-<start>.ndjson`) back to its start index.
fn parse_seg_start(name: &str) -> Option<u64> {
    name.strip_prefix("seg-")?
        .strip_suffix(".ndjson")?
        .parse()
        .ok()
}

struct Inner {
    dir: PathBuf,
    /// Open append handle on the *active* (last) segment. Replaced when the active
    /// segment rolls or is rewritten by `truncate`.
    active_file: File,
    /// Sealed + active segments, sorted by `start`; the last element is active.
    segments: Vec<SegMeta>,
    /// Serialized byte size of the active segment, for roll decisions.
    active_bytes: usize,
    /// Roll the active segment once `active_bytes` reaches this.
    seg_max_bytes: usize,
    /// All present (non-purged) log entries, keyed by index, for fast reads. Each
    /// carries its serialized byte length so purge/truncate can keep `log_bytes` in
    /// step without re-serializing.
    log: BTreeMap<u64, Stored>,
    /// Serialized byte footprint of the in-memory `log` (kept in step with it),
    /// published to the aggregate `nanobpm_raft_log_bytes` gauge for RSS-balloon
    /// attribution.
    log_bytes: usize,
    last_purged: Option<LogId<NodeId>>,
    committed: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
    /// Durability mode (`sync`/`async`), fixed at open from the environment.
    mode: DurabilityMode,
    /// Async flush thresholds (unused in sync mode).
    flush: AsyncFlush,
    /// Async only: bytes appended to the active segment since the last `fsync`.
    unsynced_bytes: usize,
    /// Async only: the committed/purge markers changed in memory but `state.json`
    /// has not yet been durably rewritten. The background flusher persists it.
    state_dirty: bool,
}

impl Inner {
    /// Rolls the active segment: seals the current one and opens a fresh
    /// `seg-<start>.ndjson` as the new active append target. Called when the active
    /// segment reaches `seg_max_bytes`, so a single segment never grows without
    /// bound and purge/truncate stay cheap.
    fn roll_active(&mut self, start: u64) -> io::Result<()> {
        let path = seg_path(&self.dir, start);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        // A fresh empty file needs the directory entry itself to be durable.
        fsync_dir(&self.dir)?;
        self.active_file = file;
        self.active_bytes = 0;
        self.segments.push(SegMeta { start, last: None });
        Ok(())
    }

    /// Async only: `fsync` the appended tail (if any) and durably persist the
    /// committed/purge markers (if changed) since the last flush. A no-op when
    /// nothing is outstanding, so the idle-tick path is cheap.
    fn flush_async(&mut self) -> io::Result<()> {
        if self.unsynced_bytes > 0 {
            self.active_file.sync_all()?;
            self.unsynced_bytes = 0;
        }
        if self.state_dirty {
            let bytes = serde_json::to_vec(&PersistedState {
                last_purged: self.last_purged,
                committed: self.committed,
            })?;
            atomic_write(&self.dir, &state_path(&self.dir), &bytes)?;
            self.state_dirty = false;
        }
        Ok(())
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Best-effort flush of the async tail on shutdown so a clean stop leaves
        // nothing unfsynced (sync mode already fsynced everything inline).
        if self.mode == DurabilityMode::Async {
            let _ = self.flush_async();
        }
    }
}

/// A crash-durable Raft log store. Cloning shares the same underlying log (the
/// [`RaftLogReader`] handed to openraft is a clone), so all access is serialized
/// through a single [`Mutex`].
#[derive(Clone)]
pub struct RaftLogStore {
    inner: Arc<Mutex<Inner>>,
    /// Per-partition live (non-purged) log byte footprint, republished after every
    /// append/truncate/purge. The openraft `Raft` handle erases the concrete log
    /// store type, so the compaction governor cannot read `Inner::log_bytes`
    /// directly; this shared handle is the bridge it reads to make byte-based
    /// snapshot/compaction decisions for the partition.
    bytes: Arc<AtomicI64>,
}

fn log_path(dir: &Path) -> PathBuf {
    dir.join("log.ndjson")
}
fn vote_path(dir: &Path) -> PathBuf {
    dir.join("vote.json")
}
fn state_path(dir: &Path) -> PathBuf {
    dir.join("state.json")
}

/// `fsync` the directory so a preceding `rename` is itself durable.
fn fsync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Atomically replace `path`'s contents with `bytes`: write a sibling temp file,
/// `fsync` it, `rename` over the target, then `fsync` the directory.
fn atomic_write(dir: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    fsync_dir(dir)
}

/// Migrates a pre-segmentation `log.ndjson` into a `seg-<start>.ndjson` segment so
/// an in-place binary upgrade keeps its durable log. Renames the legacy file to a
/// segment named for its first entry's index; a no-op when the file is absent, and
/// a cleanup when segments already exist alongside it.
fn migrate_legacy_log(dir: &Path) -> io::Result<()> {
    let legacy = log_path(dir);
    if !legacy.exists() {
        return Ok(());
    }
    let has_segments = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_str().and_then(parse_seg_start).is_some());
    if has_segments {
        // Segments are authoritative; the legacy file is stale.
        let _ = fs::remove_file(&legacy);
        return Ok(());
    }
    let mut first: Option<u64> = None;
    let reader = BufReader::new(File::open(&legacy)?);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let entry: Entry<RaftConfig> = serde_json::from_str(&line)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        first = Some(entry.log_id.index);
        break;
    }
    match first {
        Some(start) => {
            fs::rename(&legacy, seg_path(dir, start))?;
            fsync_dir(dir)?;
        }
        None => {
            let _ = fs::remove_file(&legacy);
        }
    }
    Ok(())
}

impl RaftLogStore {
    /// A shared handle to this partition's live log byte footprint, kept current
    /// after every append/truncate/purge. Read by the compaction governor to make
    /// byte-based snapshot decisions (the openraft `Raft` handle hides the store
    /// type, so this is the only way to observe per-partition log bytes).
    pub fn bytes_handle(&self) -> Arc<AtomicI64> {
        self.bytes.clone()
    }

    /// Opens (creating if absent) a durable log store rooted at `dir`, replaying
    /// any existing `seg-*.ndjson` segments (or a legacy `log.ndjson`), `vote.json`
    /// and `state.json` to reconstruct the in-memory index, the persisted vote and
    /// the purge/commit markers.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let vote: Option<Vote<NodeId>> = read_json(&vote_path(&dir))?;
        let state: PersistedState = read_json(&state_path(&dir))?.unwrap_or_default();
        let purged_upto = state.last_purged.map(|l| l.index);

        // Migrate a legacy single-file `log.ndjson` (pre-segmentation format) into a
        // segment named for its first entry's index, so an in-place binary upgrade
        // keeps its durable log.
        migrate_legacy_log(&dir)?;

        // Discover segment files, sorted by start index.
        let mut starts: Vec<u64> = Vec::new();
        for ent in fs::read_dir(&dir)? {
            let ent = ent?;
            if let Some(start) = ent.file_name().to_str().and_then(parse_seg_start) {
                starts.push(start);
            }
        }
        starts.sort_unstable();

        // Replay every segment into the in-memory index. Entries at or below the
        // durable `last_purged` marker are physically present (deletion is
        // best-effort) but filtered out here so a crash mid-purge never resurrects
        // purged entries. A sealed segment entirely at or below the marker is pure
        // garbage — unlink it now to reclaim the space.
        let mut log: BTreeMap<u64, Stored> = BTreeMap::new();
        let mut log_bytes: usize = 0;
        let mut segments: Vec<SegMeta> = Vec::new();
        let mut active_bytes: usize = 0;
        for (pos, &start) in starts.iter().enumerate() {
            let is_last = pos + 1 == starts.len();
            let path = seg_path(&dir, start);
            let mut seg_bytes: usize = 0;
            let mut last: Option<u64> = None;
            let reader = BufReader::new(File::open(&path)?);
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let len = line.len() + 1;
                seg_bytes += len;
                let entry: Entry<RaftConfig> = serde_json::from_str(&line)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let index = entry.log_id.index;
                last = Some(index);
                if purged_upto.map(|p| index > p).unwrap_or(true) {
                    log_bytes += len;
                    log.insert(index, Stored { entry, len });
                }
            }
            let fully_purged = matches!((last, purged_upto), (Some(l), Some(p)) if l <= p);
            if fully_purged && !is_last {
                let _ = fs::remove_file(&path);
                continue;
            }
            if is_last {
                active_bytes = seg_bytes;
            }
            segments.push(SegMeta { start, last });
        }

        // Ensure there is always an active segment to append to.
        if segments.is_empty() {
            segments.push(SegMeta {
                start: 0,
                last: None,
            });
            active_bytes = 0;
        }
        let active_start = segments.last().map(|s| s.start).unwrap_or(0);
        let active_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(seg_path(&dir, active_start))?;

        let mode = durability_mode_from_env();
        let entries = log.len() as i64;
        let store = Self {
            inner: Arc::new(Mutex::new(Inner {
                dir,
                active_file,
                segments,
                active_bytes,
                seg_max_bytes: seg_max_bytes_from_env(),
                log,
                log_bytes,
                last_purged: state.last_purged,
                committed: state.committed,
                vote,
                mode,
                flush: async_flush_from_env(),
                unsynced_bytes: 0,
                state_dirty: false,
            })),
            bytes: Arc::new(AtomicI64::new(log_bytes as i64)),
        };
        crate::metrics::raft_log_delta(entries, log_bytes as i64);

        // Async mode amortises fsync off the append critical path; a background
        // ticker bounds the unfsynced window even when the partition goes quiet
        // (an idle follower would otherwise hold an unflushed tail indefinitely).
        // The ticker holds a `Weak`, so it exits once the Raft instance drops the
        // store — no explicit shutdown handshake needed.
        if mode == DurabilityMode::Async {
            store.spawn_flusher();
        }

        Ok(store)
    }

    /// Spawns the async-durability background flusher (async mode only). Wakes
    /// every `flush.interval` and `fsync`s the appended tail / persists the
    /// committed marker if anything is outstanding. Exits when the last strong
    /// reference to the store is dropped.
    fn spawn_flusher(&self) {
        let weak: Weak<Mutex<Inner>> = Arc::downgrade(&self.inner);
        let interval = {
            let inner = self.inner.lock().unwrap();
            inner.flush.interval
        };
        std::thread::Builder::new()
            .name("nanobpmn-raft-log-flusher".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(interval);
                    let Some(inner) = weak.upgrade() else {
                        break; // store dropped — stop ticking
                    };
                    let mut inner = inner.lock().unwrap();
                    if let Err(e) = inner.flush_async() {
                        tracing::error!(
                            "raft log flusher fsync failed: {e}; aborting to avoid \
                             serving non-durable replicated state"
                        );
                        std::process::abort();
                    }
                }
            })
            .ok();
    }

    /// Rebuilds the on-disk segments from the in-memory index after a `truncate`
    /// removed a tail of entries. Only the (at most one) segment straddling the
    /// truncation point is rewritten and any segments entirely above it are
    /// unlinked, so the cost is bounded by a single segment — not the whole log.
    #[allow(clippy::result_large_err)]
    fn rewrite_after_truncate(inner: &mut Inner, since: u64) -> Result<(), StorageError<NodeId>> {
        // Drop segments that start at or after the truncation point (fully removed).
        let mut kept: Vec<SegMeta> = Vec::new();
        for seg in std::mem::take(&mut inner.segments) {
            if seg.start >= since {
                let _ = fs::remove_file(seg_path(&inner.dir, seg.start));
            } else {
                kept.push(seg);
            }
        }
        // If nothing survives, start a fresh active segment at the truncation point.
        if kept.is_empty() {
            let path = seg_path(&inner.dir, since);
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .map_err(io_err)?;
            file.sync_all().map_err(io_err)?;
            fsync_dir(&inner.dir).map_err(io_err)?;
            inner.active_file = file;
            inner.active_bytes = 0;
            inner.unsynced_bytes = 0;
            inner.segments = vec![SegMeta {
                start: since,
                last: None,
            }];
            return Ok(());
        }
        // Rewrite the straddling (now last-kept) segment from the live entries it
        // still holds; its predecessors are untouched.
        let active = kept.last().unwrap();
        let active_start = active.start;
        let mut bytes = Vec::new();
        let mut last: Option<u64> = None;
        for (idx, stored) in inner.log.range(active_start..) {
            serde_json::to_writer(&mut bytes, &stored.entry).map_err(io_err)?;
            bytes.push(b'\n');
            last = Some(*idx);
        }
        let path = seg_path(&inner.dir, active_start);
        atomic_write(&inner.dir, &path, &bytes).map_err(io_err)?;
        let active_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(io_err)?;
        inner.active_file = active_file;
        inner.active_bytes = bytes.len();
        inner.unsynced_bytes = 0;
        let n = kept.len();
        kept[n - 1].last = last;
        inner.segments = kept;
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn persist_state(inner: &Inner) -> Result<(), StorageError<NodeId>> {
        let state = PersistedState {
            last_purged: inner.last_purged,
            committed: inner.committed,
        };
        let bytes = serde_json::to_vec(&state).map_err(io_err)?;
        atomic_write(&inner.dir, &state_path(&inner.dir), &bytes).map_err(io_err)
    }
}

/// Reads and deserializes a JSON file, returning `None` if it does not exist.
fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> io::Result<Option<T>> {
    match fs::read(path) {
        Ok(bytes) if bytes.is_empty() => Ok(None),
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Maps any `io`/serde error into an openraft read/write storage error.
fn io_err<E: std::error::Error + 'static>(e: E) -> StorageError<NodeId> {
    StorageIOError::write(&e).into()
}

impl RaftLogReader<RaftConfig> for RaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<RaftConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .log
            .range(range)
            .map(|(_, s)| s.entry.clone())
            .collect())
    }
}

impl RaftLogStorage<RaftConfig> for RaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<RaftConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
        let last_log_id = inner
            .log
            .values()
            .next_back()
            .map(|s| s.entry.log_id)
            .or(inner.last_purged);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
        // The vote must hit disk before openraft acts on it.
        let bytes = serde_json::to_vec(vote).map_err(io_err)?;
        atomic_write(&inner.dir, &vote_path(&inner.dir), &bytes).map_err(io_err)?;
        drop(inner);
        self.inner.lock().unwrap().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().unwrap().vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<RaftConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<RaftConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.inner.lock().unwrap();
        let mut added = 0i64;
        let mut new_bytes = 0usize;
        for entry in entries {
            let mut line = Vec::new();
            serde_json::to_writer(&mut line, &entry).map_err(io_err)?;
            line.push(b'\n');
            let len = line.len();
            let index = entry.log_id.index;

            // Roll to a fresh segment once the active one is full (and non-empty, so
            // a single oversized entry still lands somewhere). The outgoing segment
            // is fsynced before the switch so a sealed segment is always durable
            // regardless of durability mode.
            if inner.active_bytes > 0 && inner.active_bytes + len > inner.seg_max_bytes {
                inner.active_file.sync_all().map_err(io_err)?;
                inner.unsynced_bytes = 0;
                inner.roll_active(index).map_err(io_err)?;
            }

            inner.active_file.write_all(&line).map_err(io_err)?;
            inner.active_bytes += len;
            if let Some(seg) = inner.segments.last_mut() {
                seg.last = Some(index);
            }
            inner.log.insert(index, Stored { entry, len });
            inner.log_bytes += len;
            inner.unsynced_bytes += len;
            new_bytes += len;
            added += 1;
        }
        match inner.mode {
            // Sync: fsync before acknowledging — a flushed entry is power-loss
            // durable. The media barrier (an `F_FULLFSYNC` on macOS) is on the
            // critical path of every replication round.
            DurabilityMode::Sync => {
                inner.active_file.sync_all().map_err(io_err)?;
                inner.unsynced_bytes = 0;
            }
            // Async: the bytes are in the page cache (process-crash durable);
            // acknowledge now and let the background flusher amortise the fsync.
            // A byte-bounded inline flush caps the unfsynced window under a flood,
            // when the periodic tick alone could fall behind.
            DurabilityMode::Async => {
                if inner.unsynced_bytes >= inner.flush.max_bytes {
                    inner.active_file.sync_all().map_err(io_err)?;
                    inner.unsynced_bytes = 0;
                }
            }
        }
        crate::metrics::raft_log_delta(added, new_bytes as i64);
        self.bytes.store(inner.log_bytes as i64, Ordering::Relaxed);
        drop(inner);
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Remove everything from `log_id.index` onward (inclusive), then rewrite at
        // most the single straddled segment (log conflicts are rare and bounded).
        let mut inner = self.inner.lock().unwrap();
        let before_entries = inner.log.len() as i64;
        let before_bytes = inner.log_bytes as i64;
        let removed = inner.log.split_off(&log_id.index);
        let removed_bytes: usize = removed.values().map(|s| s.len).sum();
        inner.log_bytes = inner.log_bytes.saturating_sub(removed_bytes);
        Self::rewrite_after_truncate(&mut inner, log_id.index)?;
        crate::metrics::raft_log_delta(
            inner.log.len() as i64 - before_entries,
            inner.log_bytes as i64 - before_bytes,
        );
        self.bytes.store(inner.log_bytes as i64, Ordering::Relaxed);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Drop everything up to and including `log_id.index`, keep the rest. This is
        // deliberately cheap: split the in-memory index, reclaim whole sealed
        // segments by unlinking them (never a rewrite), and persist the small purge
        // marker. It runs inline on openraft's core task, so it must not do
        // unbounded work — the durable record of the purge is the `state.json`
        // marker; physical segment deletion is best-effort and reconciled on
        // restart, so a failed unlink or a crash mid-purge never resurrects entries.
        let mut inner = self.inner.lock().unwrap();
        let before_entries = inner.log.len() as i64;
        let before_bytes = inner.log_bytes as i64;
        inner.last_purged = Some(log_id);
        let keep = inner.log.split_off(&(log_id.index + 1));
        let dropped = std::mem::replace(&mut inner.log, keep);
        let dropped_bytes: usize = dropped.values().map(|s| s.len).sum();
        inner.log_bytes = inner.log_bytes.saturating_sub(dropped_bytes);

        // Reclaim whole sealed segments now entirely below the purge point.
        let upto = log_id.index;
        let dir = inner.dir.clone();
        let seg_count = inner.segments.len();
        let mut kept: Vec<SegMeta> = Vec::with_capacity(seg_count);
        for (i, seg) in std::mem::take(&mut inner.segments).into_iter().enumerate() {
            let is_active = i + 1 == seg_count;
            let fully_purged = seg.last.map(|l| l <= upto).unwrap_or(false);
            if fully_purged && !is_active {
                let _ = fs::remove_file(seg_path(&dir, seg.start));
            } else {
                kept.push(seg);
            }
        }
        inner.segments = kept;

        crate::metrics::raft_log_delta(
            inner.log.len() as i64 - before_entries,
            inner.log_bytes as i64 - before_bytes,
        );
        self.bytes.store(inner.log_bytes as i64, Ordering::Relaxed);
        // Persist the durable purge marker (a small atomic write whose directory
        // fsync also makes the segment unlinks durable). No log rewrite.
        inner.state_dirty = false;
        Self::persist_state(&inner)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().unwrap();
        inner.committed = committed;
        match inner.mode {
            // Sync: persist the committed marker durably (atomic write + fsync).
            DurabilityMode::Sync => Self::persist_state(&inner),
            // Async: the committed marker is an *optional* openraft optimisation
            // (re-derived from the log + membership on restart), so defer it to
            // the background flusher rather than fsyncing on every commit.
            DurabilityMode::Async => {
                inner.state_dirty = true;
                Ok(())
            }
        }
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().unwrap().committed)
    }
}

#[cfg(test)]
mod tests {
    use openraft::storage::{RaftLogReader, RaftLogStorage};
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

    use super::*;

    fn log_id(index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(1, 0), index)
    }

    fn entry(index: u64) -> Entry<RaftConfig> {
        Entry {
            log_id: log_id(index),
            payload: EntryPayload::Blank,
        }
    }

    /// Lays down a segment file `seg-<start>.ndjson` holding `indices`, exactly as
    /// the store would have written it, so `open`/`purge`/`truncate` can be driven
    /// without the `pub(crate)` `LogFlushed` append callback.
    fn write_segment(dir: &Path, start: u64, indices: &[u64]) {
        let mut bytes = Vec::new();
        for &i in indices {
            serde_json::to_writer(&mut bytes, &entry(i)).unwrap();
            bytes.push(b'\n');
        }
        std::fs::write(seg_path(dir, start), bytes).unwrap();
    }

    fn write_state(dir: &Path, last_purged: Option<u64>) {
        let state = PersistedState {
            last_purged: last_purged.map(log_id),
            committed: None,
        };
        std::fs::write(state_path(dir), serde_json::to_vec(&state).unwrap()).unwrap();
    }

    fn tmp_dir(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let d = std::env::temp_dir().join(format!(
            "nanobpmn-logstore-{}-{tag}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    async fn indices_in(store: &mut RaftLogStore) -> Vec<u64> {
        store
            .try_get_log_entries(..)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.log_id.index)
            .collect()
    }

    #[tokio::test]
    async fn open_filters_purged_entries_and_unlinks_fully_purged_segments() {
        let dir = tmp_dir("open-reconcile");
        // Three sealed segments; the durable marker says everything up to index 5 is
        // purged, so seg-0 (0..2) and seg-3 (3..5) are pure garbage.
        write_segment(&dir, 0, &[0, 1, 2]);
        write_segment(&dir, 3, &[3, 4, 5]);
        write_segment(&dir, 6, &[6, 7, 8]);
        write_state(&dir, Some(5));

        let mut store = RaftLogStore::open(&dir).unwrap();

        // Purged entries never enter the in-memory index.
        assert_eq!(indices_in(&mut store).await, vec![6, 7, 8]);
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(5)));
        assert_eq!(state.last_log_id, Some(log_id(8)));

        // The two fully-purged sealed segments were reclaimed on open; the live one
        // (also the active segment) survives.
        assert!(!seg_path(&dir, 0).exists(), "seg-0 should be unlinked");
        assert!(!seg_path(&dir, 3).exists(), "seg-3 should be unlinked");
        assert!(seg_path(&dir, 6).exists(), "seg-6 is live and must survive");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn purge_unlinks_sealed_segments_without_rewriting_the_live_one() {
        let dir = tmp_dir("purge");
        write_segment(&dir, 0, &[0, 1, 2]);
        write_segment(&dir, 3, &[3, 4, 5]);
        write_segment(&dir, 6, &[6, 7, 8]);

        let mut store = RaftLogStore::open(&dir).unwrap();
        assert_eq!(
            indices_in(&mut store).await,
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8]
        );

        // Capture the live segment's bytes to prove purge never rewrites it.
        let live_before = std::fs::read(seg_path(&dir, 6)).unwrap();

        store.purge(log_id(5)).await.unwrap();

        // In-memory prefix dropped; the live segment file is byte-identical (no
        // rewrite), and the two fully-purged sealed segments are unlinked.
        assert_eq!(indices_in(&mut store).await, vec![6, 7, 8]);
        assert!(!seg_path(&dir, 0).exists());
        assert!(!seg_path(&dir, 3).exists());
        assert!(seg_path(&dir, 6).exists());
        assert_eq!(
            std::fs::read(seg_path(&dir, 6)).unwrap(),
            live_before,
            "purge must not rewrite the live segment"
        );

        // The purge marker is durable: a reopen sees the same trimmed log even
        // though seg-6 still physically contains no purged entries here.
        let mut reopened = RaftLogStore::open(&dir).unwrap();
        assert_eq!(indices_in(&mut reopened).await, vec![6, 7, 8]);
        assert_eq!(
            reopened.get_log_state().await.unwrap().last_purged_log_id,
            Some(log_id(5))
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn purge_marker_survives_a_crash_before_physical_deletion() {
        // Simulate a crash mid-purge: the state marker was persisted (index 2) but
        // the segment file still physically holds 0..5. Reopen must NOT resurrect
        // the purged entries.
        let dir = tmp_dir("crash-purge");
        write_segment(&dir, 0, &[0, 1, 2, 3, 4, 5]);
        write_state(&dir, Some(2));

        let mut store = RaftLogStore::open(&dir).unwrap();
        assert_eq!(indices_in(&mut store).await, vec![3, 4, 5]);
        assert_eq!(
            store.get_log_state().await.unwrap().last_purged_log_id,
            Some(log_id(2))
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn truncate_rewrites_only_the_straddled_segment() {
        let dir = tmp_dir("truncate");
        write_segment(&dir, 0, &[0, 1, 2]);
        write_segment(&dir, 3, &[3, 4, 5]);
        write_segment(&dir, 6, &[6, 7, 8]);

        let mut store = RaftLogStore::open(&dir).unwrap();
        let untouched_before = std::fs::read(seg_path(&dir, 0)).unwrap();

        // Conflict at index 4: everything >= 4 goes.
        store.truncate(log_id(4)).await.unwrap();

        assert_eq!(indices_in(&mut store).await, vec![0, 1, 2, 3]);
        // Segment starting at/after the cut is unlinked; the earlier untouched
        // segment is byte-identical; the straddled seg-3 was rewritten to just {3}.
        assert!(
            !seg_path(&dir, 6).exists(),
            "seg-6 (>=4) should be unlinked"
        );
        assert_eq!(
            std::fs::read(seg_path(&dir, 0)).unwrap(),
            untouched_before,
            "a segment entirely below the cut must not be rewritten"
        );
        assert!(seg_path(&dir, 3).exists());

        // A reopen reconstructs exactly the truncated log.
        let mut reopened = RaftLogStore::open(&dir).unwrap();
        assert_eq!(indices_in(&mut reopened).await, vec![0, 1, 2, 3]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn open_migrates_a_legacy_single_file_log() {
        let dir = tmp_dir("legacy");
        // A pre-segmentation node left a `log.ndjson`.
        let mut bytes = Vec::new();
        for i in 0..4u64 {
            serde_json::to_writer(&mut bytes, &entry(i)).unwrap();
            bytes.push(b'\n');
        }
        std::fs::write(log_path(&dir), bytes).unwrap();

        let mut store = RaftLogStore::open(&dir).unwrap();

        assert_eq!(indices_in(&mut store).await, vec![0, 1, 2, 3]);
        assert!(
            !log_path(&dir).exists(),
            "legacy file should be migrated away"
        );
        assert!(
            seg_path(&dir, 0).exists(),
            "migrated into a start-0 segment"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
