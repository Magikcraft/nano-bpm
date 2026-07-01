//! Segmented engine journal with periodic snapshots and compaction — the
//! bounded-disk durability format for the **single-partition** persistent path.
//!
//! # Why
//!
//! The legacy journal ([`crate::journal`]) is one append-only `journal.jsonl`
//! that is never truncated, and boot recovery replays it in full. That makes
//! on-disk size grow without bound on a long-running workload, and there is no
//! safe way to truncate the *prefix* of a file the writer thread is actively
//! appending to. This module solves both with the standard log-structured
//! approach (Zeebe / Raft / Kafka):
//!
//! - The log is a **directory of segments**. The active segment keeps the
//!   historical name `journal.jsonl` (so an existing single-file data dir is
//!   adopted as-is). When it grows past `NANOBPMN_JOURNAL_SEGMENT_BYTES` — or
//!   when a snapshot rotates it — it is *sealed*: renamed to
//!   `journal.seg.<startIndex>.jsonl`, where `startIndex` is the absolute index
//!   of its first event, and a fresh empty `journal.jsonl` is opened.
//! - Periodically the engine's compact [`EngineSnapshot`] is persisted to
//!   `snapshot.<coveredEvents>.bin` after rotating the active segment, so the
//!   snapshot covers exactly the events in the sealed segments.
//! - **Compaction** deletes any sealed segment whose events are *both* covered
//!   by the latest snapshot *and* already projected into the read model (the
//!   `exported_position` watermark — the Zeebe exporter bound). The active
//!   segment is never touched.
//! - Boot = load the latest snapshot, then replay only the events the snapshot
//!   did not cover (the surviving sealed tail + the active segment).
//!
//! Every segment's start index is encoded in its filename, so absolute event
//! positions survive a crash at any point without a separate index file (the
//! one optional `journal.head` file only records the active segment's start so
//! it is recoverable when *all* sealed segments have been compacted away).

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use nanobpmn_engine_core::{Engine, EngineSnapshot, Event, partition_of};

/// The active segment keeps the historical journal name so an existing
/// single-file data dir is adopted unchanged.
pub const ACTIVE_NAME: &str = "journal.jsonl";
const SEG_PREFIX: &str = "journal.seg.";
const SEG_SUFFIX: &str = ".jsonl";
const SNAP_PREFIX: &str = "snapshot.";
const SNAP_SUFFIX: &str = ".bin";
const HEAD_NAME: &str = "journal.head";
/// Combined per-partition snapshot for the multi-partition shared-WAL path.
const MULTI_SNAP_NAME: &str = "msnapshot.bin";
/// Per-partition head: the active segment's per-partition cumulative start counts
/// (recovers `per_partition_active_start` when every sealed segment is compacted).
const PPHEAD_NAME: &str = "journal.pphead";
/// Per-sealed-segment sidecar suffix carrying that segment's per-partition
/// cumulative START counts (its `end` counts are derived by demuxing on recovery).
const PP_META_SUFFIX: &str = ".ppmeta";

/// Default seal threshold for the active segment (128 MiB). Tunable via
/// `NANOBPMN_JOURNAL_SEGMENT_BYTES`; `0` disables size-based sealing (segments
/// then roll only when a snapshot rotates them).
const DEFAULT_SEGMENT_BYTES: u64 = 128 << 20;

/// Seal threshold from `NANOBPMN_JOURNAL_SEGMENT_BYTES` (bytes). `0` = no
/// size-based sealing. A finite value, clamped to a 1 MiB floor so a tiny value
/// can't seal every batch, otherwise the default.
pub fn segment_bytes_from_env() -> u64 {
    match std::env::var("NANOBPMN_JOURNAL_SEGMENT_BYTES") {
        Ok(v) => match v.trim().parse::<u64>() {
            Ok(0) => u64::MAX, // size-based sealing off
            Ok(n) => n.max(1 << 20),
            Err(_) => DEFAULT_SEGMENT_BYTES,
        },
        Err(_) => DEFAULT_SEGMENT_BYTES,
    }
}

/// Whether the segmented journal is enabled for the persistent single-partition
/// path. On by default; set `NANOBPMN_JOURNAL_SEGMENTED=0` to fall back to the
/// legacy single-file journal (no compaction).
pub fn segmented_enabled() -> bool {
    match std::env::var("NANOBPMN_JOURNAL_SEGMENTED") {
        Ok(v) => !matches!(v.trim(), "0" | "false" | "off" | "no"),
        Err(_) => true,
    }
}

/// Snapshot + compaction cadence from `NANOBPMN_SNAPSHOT_INTERVAL_MS` (default
/// 60 s, floored at 1 s). `0` disables periodic snapshots/compaction entirely
/// (the journal then grows unbounded, as in the legacy path). Returns `None`
/// when disabled.
pub fn snapshot_interval_from_env() -> Option<std::time::Duration> {
    match std::env::var("NANOBPMN_SNAPSHOT_INTERVAL_MS") {
        Ok(v) => match v.trim().parse::<u64>() {
            Ok(0) => None,
            Ok(ms) => Some(std::time::Duration::from_millis(ms.max(1000))),
            Err(_) => Some(std::time::Duration::from_secs(60)),
        },
        Err(_) => Some(std::time::Duration::from_secs(60)),
    }
}

/// A sealed (immutable) segment: the absolute event-index range it covers and
/// the file backing it.
#[derive(Clone, Debug)]
pub struct SealedSeg {
    /// Absolute index of this segment's first event.
    pub start: u64,
    /// Absolute index one past this segment's last event (== next segment start).
    pub end: u64,
    pub path: PathBuf,
    /// Per-partition cumulative event count at this segment's END, indexed by
    /// global partition id. Empty for the single-partition (legacy) path. Used
    /// by multi-partition compaction: a segment is deletable only once every
    /// partition `p`'s snapshot covers `per_partition_end[p]`.
    pub per_partition_end: Vec<u64>,
}

/// The boundary produced by sealing the active segment: the absolute event
/// count it now covers (`== next segment start`), plus (multi-partition only)
/// the per-partition cumulative counts at that boundary.
#[derive(Clone, Debug)]
pub struct SealInfo {
    pub end: u64,
    /// Per-partition cumulative event count at the seal boundary, indexed by
    /// global partition id. Empty for the single-partition (legacy) path. The
    /// snapshot maintenance tick reads this partition's entry as its snapshot's
    /// covered count.
    pub per_partition_end: Vec<u64>,
}

/// State shared between the writer thread (which seals segments) and the
/// snapshot/compaction maintenance task (which reads boundaries and deletes
/// covered segments). Lives in an `Arc`.
pub struct SegShared {
    /// Directory holding the segments, snapshots and head file.
    pub dir: PathBuf,
    /// Absolute path of the active segment (`<dir>/journal.jsonl`).
    pub active_path: PathBuf,
    /// Cumulative count of events durably appended across all segments
    /// (sealed + active). Advanced by the writer after each batch.
    pub total_events: AtomicU64,
    /// Absolute index of the active segment's first event (advanced on each seal).
    pub active_start: AtomicU64,
    /// Sealed segments, ascending by `start`. Mutated by the writer on seal and
    /// by compaction on delete.
    pub sealed: Mutex<Vec<SealedSeg>>,
    /// Active-segment seal threshold in bytes (`u64::MAX` disables it).
    pub segment_bytes: u64,
    /// Per-partition cumulative event count across all segments (sealed +
    /// active), indexed by global partition id. Advanced by the writer per
    /// batch. Empty for the single-partition (legacy) path — its presence is
    /// what makes the writer track partitions and seal-time sidecars.
    pub per_partition_total: Vec<AtomicU64>,
    /// Per-partition cumulative count at the ACTIVE segment's first event
    /// (advanced on each seal), indexed by global partition id. Persisted to
    /// [`PPHEAD_NAME`] so `base_p` is recoverable when every sealed segment has
    /// been compacted away. Empty for the single-partition path.
    pub per_partition_active_start: Vec<AtomicU64>,
}

impl SegShared {
    /// A non-segmenting shared state for the legacy single-file paths
    /// (`Journal::open_partition`, `SharedWriter`): an infinite seal threshold
    /// means the writer never rotates, so the file keeps its given path and
    /// behaves exactly as before. The active path is the caller's journal file.
    pub fn legacy(active_path: PathBuf) -> Arc<Self> {
        let dir = active_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        Arc::new(Self {
            dir,
            active_path,
            total_events: AtomicU64::new(0),
            active_start: AtomicU64::new(0),
            sealed: Mutex::new(Vec::new()),
            segment_bytes: u64::MAX,
            per_partition_total: Vec::new(),
            per_partition_active_start: Vec::new(),
        })
    }

    fn sealed_name(&self, start: u64) -> PathBuf {
        self.dir
            .join(format!("{SEG_PREFIX}{start:020}{SEG_SUFFIX}"))
    }

    fn head_path(&self) -> PathBuf {
        self.dir.join(HEAD_NAME)
    }

    /// Number of partitions this log tracks (0 for the single-partition legacy
    /// path, which does no per-partition bookkeeping).
    pub fn partitions(&self) -> usize {
        self.per_partition_total.len()
    }

    /// Advances the per-partition cumulative counts by `deltas` (indexed by
    /// global partition id). A no-op for the legacy path (empty vectors).
    pub fn add_partition_events(&self, deltas: &[u64]) {
        for (slot, delta) in self.per_partition_total.iter().zip(deltas) {
            if *delta != 0 {
                slot.fetch_add(*delta, Ordering::Release);
            }
        }
    }

    /// Snapshot of the current per-partition cumulative totals.
    fn per_partition_totals(&self) -> Vec<u64> {
        self.per_partition_total
            .iter()
            .map(|c| c.load(Ordering::Acquire))
            .collect()
    }

    /// Snapshot of the per-partition active-segment start counts.
    fn per_partition_starts(&self) -> Vec<u64> {
        self.per_partition_active_start
            .iter()
            .map(|c| c.load(Ordering::Acquire))
            .collect()
    }

    fn pp_meta_path(&self, start: u64) -> PathBuf {
        self.dir.join(format!(
            "{SEG_PREFIX}{start:020}{SEG_SUFFIX}{PP_META_SUFFIX}"
        ))
    }

    fn pphead_path(&self) -> PathBuf {
        self.dir.join(PPHEAD_NAME)
    }
}

/// The active segment owned by the writer thread: the open append handle plus
/// the bookkeeping needed to seal it. Used by both writer loops (legacy and
/// segmented) so segmentation is transparent to the group-commit machinery; the
/// legacy path simply uses `segment_bytes == u64::MAX` and never rotates.
pub struct ActiveSegment {
    shared: Arc<SegShared>,
    file: File,
    bytes: u64,
}

impl ActiveSegment {
    /// Opens (creating, positioned to append) the active segment for `shared`.
    pub fn open(shared: Arc<SegShared>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&shared.active_path)?;
        let bytes = file.metadata().map(|m| m.len()).unwrap_or(0);
        Ok(Self {
            shared,
            file,
            bytes,
        })
    }

    /// Access to the shared seal/boundary state, for the writer loop to bump
    /// per-partition counters and consult segment boundaries.
    pub fn shared(&self) -> &Arc<SegShared> {
        &self.shared
    }

    /// Appends a group-committed batch of `events` (already serialized to
    /// newline-terminated `buf`) to the active segment.
    pub fn write_all(&mut self, buf: &[u8], events: u64) -> io::Result<()> {
        self.file.write_all(buf)?;
        self.bytes += buf.len() as u64;
        self.shared
            .total_events
            .fetch_add(events, Ordering::Release);
        Ok(())
    }

    /// Forces a durability barrier on the active segment.
    pub fn fsync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Seals the active segment if it has grown past the size threshold,
    /// returning the boundary when a seal happened.
    pub fn maybe_seal(&mut self) -> io::Result<Option<SealInfo>> {
        if self.bytes >= self.shared.segment_bytes && self.bytes > 0 {
            Ok(Some(self.seal()?))
        } else {
            Ok(None)
        }
    }

    /// Seals the active segment: fsync, rename it to its sealed name (keyed by
    /// its start index), record the boundary, persist the new active start, and
    /// open a fresh empty active segment. Returns the sealed boundary.
    pub fn seal(&mut self) -> io::Result<SealInfo> {
        // Flush everything we are about to make immutable.
        self.file.sync_all()?;

        let start = self.shared.active_start.load(Ordering::Acquire);
        let end = self.shared.total_events.load(Ordering::Acquire);
        // Per-partition boundary counts (empty on the single-partition path).
        let per_partition_end = self.shared.per_partition_totals();
        let per_partition_start = self.shared.per_partition_starts();

        // An empty active segment has nothing to seal; just report the boundary.
        if end == start {
            return Ok(SealInfo {
                end,
                per_partition_end,
            });
        }

        let sealed_path = self.shared.sealed_name(start);
        // Persist this segment's per-partition START counts (its END is derived
        // by demuxing on recovery) BEFORE the rename is made durable, so a
        // surviving segment always has its sidecar for `base_p` recovery.
        if !per_partition_start.is_empty() {
            write_pp_meta(&self.shared.pp_meta_path(start), &per_partition_start);
        }
        fs::rename(&self.shared.active_path, &sealed_path)?;

        // Open a fresh active segment and make the rename durable.
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.shared.active_path)?;
        self.bytes = 0;
        fsync_dir(&self.shared.dir);

        self.shared.active_start.store(end, Ordering::Release);
        write_head(&self.shared.head_path(), end);
        // Advance the per-partition active start to this seal's end and persist
        // it, so `base_p` survives even once every sealed segment is compacted.
        if !per_partition_end.is_empty() {
            for (slot, v) in self
                .shared
                .per_partition_active_start
                .iter()
                .zip(&per_partition_end)
            {
                slot.store(*v, Ordering::Release);
            }
            write_pphead(&self.shared.pphead_path(), &per_partition_end);
        }

        self.shared
            .sealed
            .lock()
            .expect("sealed lock")
            .push(SealedSeg {
                start,
                end,
                path: sealed_path,
                per_partition_end: per_partition_end.clone(),
            });

        Ok(SealInfo {
            end,
            per_partition_end,
        })
    }
}

/// fsync a directory so a contained rename/create is durable. Best-effort:
/// directory fsync is unsupported on some platforms (e.g. Windows), where the
/// rename is durable by other means.
fn fsync_dir(dir: &Path) {
    if let Ok(f) = File::open(dir) {
        let _ = f.sync_all();
    }
}

/// Atomically (tmp + rename) writes the active-segment start index to the head
/// file, so it is recoverable even when every sealed segment has been compacted.
fn write_head(path: &Path, active_start: u64) {
    let tmp = path.with_extension("head.tmp");
    if fs::write(&tmp, active_start.to_string().as_bytes()).is_ok() {
        let _ = fs::rename(&tmp, path);
    }
}

fn read_head(path: &Path) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse::<u64>().ok()
}

/// Serializes a per-partition count vector as a compact comma-separated line.
fn encode_counts(counts: &[u64]) -> String {
    counts
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_counts(s: &str) -> Option<Vec<u64>> {
    let s = s.trim();
    if s.is_empty() {
        return Some(Vec::new());
    }
    s.split(',').map(|p| p.trim().parse::<u64>().ok()).collect()
}

/// Atomically writes a sealed segment's per-partition START counts sidecar.
fn write_pp_meta(path: &Path, per_partition_start: &[u64]) {
    let tmp = path.with_extension("ppmeta.tmp");
    if fs::write(&tmp, encode_counts(per_partition_start).as_bytes()).is_ok() {
        let _ = fs::rename(&tmp, path);
    }
}

fn read_pp_meta(path: &Path) -> Option<Vec<u64>> {
    decode_counts(&fs::read_to_string(path).ok()?)
}

/// Atomically writes the active segment's per-partition start counts, so
/// `base_p` is recoverable when every sealed segment has been compacted.
fn write_pphead(path: &Path, per_partition_active_start: &[u64]) {
    let tmp = path.with_extension("pphead.tmp");
    if fs::write(&tmp, encode_counts(per_partition_active_start).as_bytes()).is_ok() {
        let _ = fs::rename(&tmp, path);
    }
}

fn read_pphead(path: &Path) -> Option<Vec<u64>> {
    decode_counts(&fs::read_to_string(path).ok()?)
}

/// A persisted snapshot: the engine's compact state plus the absolute event
/// count it covers (== the sealed-segment boundary at snapshot time).
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedSnapshot {
    covered_events: u64,
    engine: EngineSnapshot,
}

fn is_seg_file(name: &str) -> Option<u64> {
    let rest = name.strip_prefix(SEG_PREFIX)?.strip_suffix(SEG_SUFFIX)?;
    rest.parse::<u64>().ok()
}

fn is_snap_file(name: &str) -> Option<u64> {
    let rest = name.strip_prefix(SNAP_PREFIX)?.strip_suffix(SNAP_SUFFIX)?;
    rest.parse::<u64>().ok()
}

/// Lists sealed segment files in `dir`, ascending by start index, pairing each
/// with the absolute event count it contains (by reading it). The active
/// segment is excluded.
fn list_sealed(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut segs: Vec<(u64, PathBuf)> = Vec::new();
    if dir.exists() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(start) = is_seg_file(&name) {
                segs.push((start, entry.path()));
            }
        }
    }
    segs.sort_by_key(|(start, _)| *start);
    Ok(segs)
}

/// Reads and deserializes every event from a single segment/log file (empty if
/// absent).
pub fn read_segment_events(path: &Path) -> io::Result<Vec<Event>> {
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

/// Loads the latest valid persisted snapshot in `dir`, if any.
fn load_latest_snapshot(dir: &Path) -> Option<(EngineSnapshot, u64)> {
    let mut best: Option<(u64, PathBuf)> = None;
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name();
        if let Some(covered) = is_snap_file(&name.to_string_lossy())
            && best.as_ref().map(|(c, _)| covered > *c).unwrap_or(true)
        {
            best = Some((covered, entry.path()));
        }
    }
    let (_, path) = best?;
    let bytes = fs::read(&path).ok()?;
    let snap: PersistedSnapshot = serde_json::from_slice(&bytes).ok()?;
    Some((snap.engine, snap.covered_events))
}

/// The outcome of recovering a segmented journal directory.
pub struct SegRecovery {
    /// `false` when prior durable state was recovered.
    pub fresh: bool,
    /// The shared seal/boundary state to hand to the writer.
    pub shared: Arc<SegShared>,
    /// All surviving events, ascending, spanning `[first_index, total_events)`.
    pub events: Vec<Event>,
    /// Absolute index of the first surviving event (events compacted before this
    /// are only present in the snapshot / read model).
    pub first_index: u64,
    /// Absolute count of all events ever durably appended.
    pub total_events: u64,
}

/// Recovers (or initialises) the segmented journal in `dir`: loads the latest
/// snapshot, reads the surviving sealed tail + active segment, rebuilds the
/// engine (snapshot + tail, or full replay when there is no snapshot), and
/// returns the restored engine plus the shared state the writer continues from.
pub fn recover(dir: &Path) -> io::Result<(Engine, SegRecovery)> {
    fs::create_dir_all(dir)?;
    let active_path = dir.join(ACTIVE_NAME);

    let sealed_files = list_sealed(dir)?;

    // Read every surviving segment in order, tracking absolute indices from each
    // segment's start (encoded in its filename). Gaps cannot occur: compaction
    // only ever removes a contiguous prefix of sealed segments.
    let first_index = sealed_files.first().map(|(s, _)| *s);

    let mut sealed: Vec<SealedSeg> = Vec::with_capacity(sealed_files.len());
    let mut events: Vec<Event> = Vec::new();
    let mut cursor = first_index.unwrap_or(0);
    for (start, path) in &sealed_files {
        let seg_events = read_segment_events(path)?;
        let end = start + seg_events.len() as u64;
        sealed.push(SealedSeg {
            start: *start,
            end,
            path: path.clone(),
            per_partition_end: Vec::new(),
        });
        events.extend(seg_events);
        cursor = end;
    }

    // The active segment begins where the last sealed segment ended; if there
    // are no sealed segments, fall back to the persisted head (covers the case
    // where every sealed segment was compacted away), else 0.
    let active_start = if sealed.is_empty() {
        read_head(&dir.join(HEAD_NAME)).unwrap_or(0)
    } else {
        cursor
    };
    let active_events = read_segment_events(&active_path)?;
    let total_events = active_start + active_events.len() as u64;
    let first_index = first_index.unwrap_or(active_start);
    events.extend(active_events);

    let fresh = total_events == 0 && load_latest_snapshot(dir).is_none();

    // Rebuild the engine: snapshot (if any) + replay of the events the snapshot
    // did not cover; otherwise a full replay of the surviving events.
    let engine = match load_latest_snapshot(dir) {
        Some((snap, covered)) => {
            let mut engine = Engine::from_snapshot(snap);
            // Replay only events after the snapshot boundary.
            let skip = covered.saturating_sub(first_index) as usize;
            if skip < events.len() {
                engine.apply_replayed_events(events[skip..].iter().cloned());
            }
            engine
        }
        None => Engine::replay_partition(0, events.iter().cloned()),
    };

    let shared = Arc::new(SegShared {
        dir: dir.to_path_buf(),
        active_path,
        total_events: AtomicU64::new(total_events),
        active_start: AtomicU64::new(active_start),
        sealed: Mutex::new(sealed),
        segment_bytes: segment_bytes_from_env(),
        per_partition_total: Vec::new(),
        per_partition_active_start: Vec::new(),
    });

    Ok((
        engine,
        SegRecovery {
            fresh,
            shared,
            events,
            first_index,
            total_events,
        },
    ))
}

/// Persists `snap` (covering `covered_events`) atomically to `dir`, then removes
/// any older snapshot files. Called by the maintenance task after rotating.
pub fn write_snapshot(dir: &Path, snap: EngineSnapshot, covered_events: u64) -> io::Result<()> {
    let payload = PersistedSnapshot {
        covered_events,
        engine: snap,
    };
    let bytes =
        serde_json::to_vec(&payload).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let final_path = dir.join(format!("{SNAP_PREFIX}{covered_events:020}{SNAP_SUFFIX}"));
    let tmp = dir.join(format!(
        "{SNAP_PREFIX}{covered_events:020}{SNAP_SUFFIX}.tmp"
    ));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &final_path)?;
    fsync_dir(dir);

    // Drop superseded snapshots (keep only the newest covered count).
    if let Ok(rd) = fs::read_dir(dir) {
        for entry in rd.flatten() {
            let name = entry.file_name();
            if let Some(covered) = is_snap_file(&name.to_string_lossy())
                && covered < covered_events
            {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
    Ok(())
}

/// Deletes every sealed segment fully covered by `watermark` — the lesser of the
/// latest snapshot's covered count and the read model's exported position — so a
/// segment is removed only once *both* the engine snapshot and the read model no
/// longer need it. The active segment is never touched. Returns the number of
/// segments removed.
pub fn compact(shared: &SegShared, watermark: u64) -> usize {
    let mut sealed = shared.sealed.lock().expect("sealed lock");
    let mut removed = 0usize;
    // Sealed segments are kept ascending; remove the covered prefix.
    while let Some(seg) = sealed.first() {
        if seg.end <= watermark {
            let _ = fs::remove_file(&seg.path);
            sealed.remove(0);
            removed += 1;
        } else {
            break;
        }
    }
    if removed > 0 {
        fsync_dir(&shared.dir);
    }
    removed
}

// ----------------------------------------------------------------------------
// Multi-partition (shared-WAL) bounded-disk path.
//
// One shared log carries every partition's events, interleaved in commit order.
// Positions (segment start/end, first_index, total_events, exported_position)
// are GLOBAL event indices exactly as in the single-partition path, so the read
// model stays a single global prefix. Compaction adds a per-partition SNAPSHOT
// gate: a sealed segment is deletable only once every partition's snapshot
// covers its own events within that segment. Per-partition counts are keyed by
// GLOBAL partition id (a node owning a subset of partitions leaves the rest at
// zero), which unifies single-node multi-partition and clustered.
// ----------------------------------------------------------------------------

/// One partition's entry in the combined snapshot.
#[derive(serde::Serialize, serde::Deserialize)]
struct MultiSnapshotEntry {
    partition: u64,
    covered: u64,
    engine: EngineSnapshot,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct MultiPersistedSnapshot {
    entries: Vec<MultiSnapshotEntry>,
}

/// Persists the combined per-partition snapshot atomically (tmp + rename +
/// fsync). `entries` is `(global_partition_id, covered_count, snapshot)` for
/// every owned partition. Overwrites the previous combined snapshot.
pub fn write_multi_snapshot(
    dir: &Path,
    entries: Vec<(u64, u64, EngineSnapshot)>,
) -> io::Result<()> {
    let payload = MultiPersistedSnapshot {
        entries: entries
            .into_iter()
            .map(|(partition, covered, engine)| MultiSnapshotEntry {
                partition,
                covered,
                engine,
            })
            .collect(),
    };
    let bytes =
        serde_json::to_vec(&payload).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let final_path = dir.join(MULTI_SNAP_NAME);
    let tmp = dir.join(format!("{MULTI_SNAP_NAME}.tmp"));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &final_path)?;
    fsync_dir(dir);
    Ok(())
}

/// Loads the combined per-partition snapshot, as a map from global partition id
/// to `(covered_count, snapshot)`. `None` when absent or unreadable.
fn load_multi_snapshot(
    dir: &Path,
) -> Option<std::collections::HashMap<u64, (u64, EngineSnapshot)>> {
    let bytes = fs::read(dir.join(MULTI_SNAP_NAME)).ok()?;
    let snap: MultiPersistedSnapshot = serde_json::from_slice(&bytes).ok()?;
    Some(
        snap.entries
            .into_iter()
            .map(|e| (e.partition, (e.covered, e.engine)))
            .collect(),
    )
}

/// The outcome of recovering a multi-partition segmented journal directory.
pub struct MultiSegRecovery {
    /// `false` when prior durable state was recovered.
    pub fresh: bool,
    /// The shared seal/boundary state to hand to the shared writer.
    pub shared: Arc<SegShared>,
    /// All surviving events, ascending, spanning global `[first_index, total_events)`.
    pub events: Vec<Event>,
    /// Absolute (global) index of the first surviving event.
    pub first_index: u64,
    /// Absolute (global) count of all events ever durably appended.
    pub total_events: u64,
    /// Rebuilt engine per OWNED partition, keyed by global partition id.
    pub engines: Vec<(u64, Engine)>,
}

/// Recovers (or initialises) the multi-partition segmented journal in `dir` for
/// the partitions in `owned` (global ids). `num_partitions` is the global
/// partition count (sizes the per-partition vectors). Rebuilds each owned
/// partition's engine from the combined snapshot + its surviving tail (or a full
/// replay when there is no snapshot), and returns the shared state plus the
/// surviving events for the caller's global read-model catch-up.
pub fn recover_multi(
    dir: &Path,
    owned: &[u64],
    num_partitions: usize,
) -> io::Result<MultiSegRecovery> {
    fs::create_dir_all(dir)?;
    let active_path = dir.join(ACTIVE_NAME);
    let sealed_files = list_sealed(dir)?;

    // `base_p`: per-partition cumulative counts BEFORE the first surviving event
    // (i.e. the count compacted away). For the first surviving sealed segment it
    // is its sidecar; with no sealed segments it is the per-partition head; with
    // nothing compacted it is zero.
    let first_sealed_start = sealed_files.first().map(|(s, _)| *s);
    let zeros = || vec![0u64; num_partitions];
    let pp_base: Vec<u64> = match first_sealed_start {
        Some(start) => read_pp_meta(&dir.join(format!(
            "{SEG_PREFIX}{start:020}{SEG_SUFFIX}{PP_META_SUFFIX}"
        )))
        .filter(|v| v.len() == num_partitions)
        .unwrap_or_else(zeros),
        None => read_pphead(&dir.join(PPHEAD_NAME))
            .filter(|v| v.len() == num_partitions)
            .unwrap_or_else(zeros),
    };

    // Read every surviving sealed segment in order, tracking global indices and
    // per-partition cumulative counts (from `pp_base`).
    let first_index_opt = sealed_files.first().map(|(s, _)| *s);
    let mut sealed: Vec<SealedSeg> = Vec::with_capacity(sealed_files.len());
    let mut events: Vec<Event> = Vec::new();
    let mut cursor = first_index_opt.unwrap_or(0);
    let mut pp_running = pp_base.clone();
    for (start, path) in &sealed_files {
        let seg_events = read_segment_events(path)?;
        let end = start + seg_events.len() as u64;
        for e in &seg_events {
            let p = (partition_of(e.max_key()) as usize).min(num_partitions.saturating_sub(1));
            if p < pp_running.len() {
                pp_running[p] += 1;
            }
        }
        sealed.push(SealedSeg {
            start: *start,
            end,
            path: path.clone(),
            per_partition_end: pp_running.clone(),
        });
        events.extend(seg_events);
        cursor = end;
    }

    // The active segment begins where the last sealed segment ended; with none,
    // fall back to the persisted head (every sealed segment compacted), else 0.
    let active_start = if sealed.is_empty() {
        read_head(&dir.join(HEAD_NAME)).unwrap_or(0)
    } else {
        cursor
    };
    // Per-partition active start == last sealed end (== pp_running), or the
    // per-partition head when no sealed segments survive.
    let per_partition_active_start: Vec<u64> = if sealed.is_empty() {
        pp_base.clone()
    } else {
        pp_running.clone()
    };

    let active_events = read_segment_events(&active_path)?;
    let total_events = active_start + active_events.len() as u64;
    let first_index = first_index_opt.unwrap_or(active_start);
    // Per-partition totals = active start + active-segment per-partition counts.
    let mut per_partition_total = per_partition_active_start.clone();
    for e in &active_events {
        let p = (partition_of(e.max_key()) as usize).min(num_partitions.saturating_sub(1));
        if p < per_partition_total.len() {
            per_partition_total[p] += 1;
        }
    }
    events.extend(active_events);

    let combined = load_multi_snapshot(dir);
    let fresh = total_events == 0 && combined.is_none();

    // Rebuild each owned partition's engine: snapshot + its surviving tail, or a
    // full replay of its surviving events when there is no snapshot for it.
    let engines: Vec<(u64, Engine)> = owned
        .iter()
        .map(|&p| {
            let p_events: Vec<Event> = events
                .iter()
                .filter(|e| {
                    (partition_of(e.max_key()) as usize).min(num_partitions.saturating_sub(1))
                        == p as usize
                })
                .cloned()
                .collect();
            let base_p = pp_base.get(p as usize).copied().unwrap_or(0);
            let engine = match combined.as_ref().and_then(|m| m.get(&p)) {
                Some((covered, snap)) => {
                    let mut engine = Engine::from_snapshot(snap.clone());
                    let skip = covered.saturating_sub(base_p) as usize;
                    if skip < p_events.len() {
                        engine.apply_replayed_events(p_events[skip..].iter().cloned());
                    }
                    engine
                }
                None => Engine::replay_partition(p, p_events),
            };
            (p, engine)
        })
        .collect();

    let shared = Arc::new(SegShared {
        dir: dir.to_path_buf(),
        active_path,
        total_events: AtomicU64::new(total_events),
        active_start: AtomicU64::new(active_start),
        sealed: Mutex::new(sealed),
        segment_bytes: segment_bytes_from_env(),
        per_partition_total: per_partition_total
            .into_iter()
            .map(AtomicU64::new)
            .collect(),
        per_partition_active_start: per_partition_active_start
            .into_iter()
            .map(AtomicU64::new)
            .collect(),
    });

    Ok(MultiSegRecovery {
        fresh,
        shared,
        events,
        first_index,
        total_events,
        engines,
    })
}

/// Deletes every sealed segment that BOTH the read model and every partition's
/// snapshot no longer need: `seg.end <= exported_position` (read model has
/// projected all of it — valid because the shared writer feeds the exporter in
/// log order) AND for every partition `p`, `covered[p] >= seg.per_partition_end[p]`
/// (each partition's snapshot subsumes its events in the segment). `covered` is
/// indexed by global partition id (0 for partitions that never snapshotted).
/// Removes each segment's per-partition sidecar with it. Returns the count removed.
pub fn compact_multi(shared: &SegShared, covered: &[u64], exported_position: u64) -> usize {
    let mut sealed = shared.sealed.lock().expect("sealed lock");
    let mut removed = 0usize;
    while let Some(seg) = sealed.first() {
        let export_ok = seg.end <= exported_position;
        let snap_ok = seg.per_partition_end.len() <= covered.len()
            && seg
                .per_partition_end
                .iter()
                .enumerate()
                .all(|(p, end)| covered.get(p).copied().unwrap_or(0) >= *end);
        if export_ok && snap_ok {
            let _ = fs::remove_file(&seg.path);
            let _ = fs::remove_file(shared.pp_meta_path(seg.start));
            sealed.remove(0);
            removed += 1;
        } else {
            break;
        }
    }
    if removed > 0 {
        fsync_dir(&shared.dir);
    }
    removed
}

#[cfg(test)]
mod tests {
    use nanobpmn_engine_core::{Command, ProcessBuilder};

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

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "nanobpmn-seglog-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    /// A fresh segmented dir adopts and recovers a deploy + instance across a
    /// reopen (back-compat: the active segment keeps the legacy `journal.jsonl`
    /// name).
    #[test]
    fn reopening_a_segmented_journal_replays_persisted_state() {
        let dir = temp_dir("roundtrip");

        let key = {
            let (mut journal, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            assert!(recovery.fresh);
            assert!(journal.is_segmented());
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let (events, _) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            // The active segment keeps the historical name.
            assert!(dir.join(ACTIVE_NAME).exists());
            events.iter().find_map(|e| e.instance_key()).unwrap()
        };

        let (reopened, recovery) =
            crate::journal::Journal::open_segmented(&dir).expect("reopen segmented");
        assert!(!recovery.fresh);
        assert!(reopened.instance(key).is_some());
        assert_eq!(reopened.state().processes.len(), 1);

        let _ = fs::remove_dir_all(&dir);
    }

    /// Forcing a snapshot+rotate seals the active segment, persists a snapshot
    /// covering exactly that boundary, and compaction (at the snapshot
    /// watermark) deletes the sealed segment — while a reopen still recovers all
    /// state from snapshot + the surviving tail.
    #[test]
    fn snapshot_rotate_then_compaction_bounds_disk_and_recovers() {
        let dir = temp_dir("compaction");

        let (key1, key2, shared) = {
            let (mut journal, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            let shared = Arc::clone(&recovery.shared);

            // First instance: goes into the active segment.
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let (events, _) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let key1 = events.iter().find_map(|e| e.instance_key()).unwrap();

            // Snapshot + rotate: seals the active segment at the exact covered
            // boundary, so a sealed segment file now exists.
            let (snap, covered) = journal.snapshot_and_rotate().expect("snapshot+rotate");
            assert!(covered > 0);
            assert_eq!(
                shared.sealed.lock().unwrap().len(),
                1,
                "rotate seals the active segment"
            );
            write_snapshot(&dir, snap, covered).expect("write snapshot");

            // Compaction at the snapshot watermark drops the sealed segment whose
            // events the snapshot now subsumes.
            let removed = compact(&shared, covered);
            assert_eq!(removed, 1, "the sealed prefix is compacted away");
            assert!(shared.sealed.lock().unwrap().is_empty());
            assert!(list_sealed(&dir).unwrap().is_empty());

            // Second instance: lands in the fresh active segment, after the
            // compacted prefix.
            let (events, _) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let key2 = events.iter().find_map(|e| e.instance_key()).unwrap();

            (key1, key2, shared)
        };
        drop(shared);

        // Reopen: recovers from snapshot (covers key1's events, which were
        // compacted off disk) + active tail (key2).
        let (reopened, recovery) =
            crate::journal::Journal::open_segmented(&dir).expect("reopen segmented");
        assert!(!recovery.fresh);
        assert!(
            reopened.instance(key1).is_some(),
            "snapshot restores the compacted instance"
        );
        assert!(
            reopened.instance(key2).is_some(),
            "the active tail restores the post-compaction instance"
        );
        assert_eq!(reopened.state().processes.len(), 1);

        let _ = fs::remove_dir_all(&dir);
    }

    /// Compaction never deletes a sealed segment the read model has not yet
    /// projected: a watermark below the segment boundary leaves it intact.
    #[test]
    fn compaction_respects_the_watermark() {
        let dir = temp_dir("watermark");

        let (mut journal, recovery) =
            crate::journal::Journal::open_segmented(&dir).expect("open segmented");
        let shared = Arc::clone(&recovery.shared);

        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let _ = journal
            .apply_command(Command::create_instance("demo"))
            .unwrap();
        let (_snap, covered) = journal.snapshot_and_rotate().expect("snapshot+rotate");
        assert_eq!(shared.sealed.lock().unwrap().len(), 1);

        // A watermark of 0 (read model has exported nothing) removes nothing.
        assert_eq!(compact(&shared, 0), 0);
        assert_eq!(shared.sealed.lock().unwrap().len(), 1);

        // Just below the segment boundary: still retained.
        assert_eq!(compact(&shared, covered - 1), 0);
        assert_eq!(shared.sealed.lock().unwrap().len(), 1);

        // At the boundary: removed.
        assert_eq!(compact(&shared, covered), 1);
        assert!(shared.sealed.lock().unwrap().is_empty());

        drop(journal);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Recovery from snapshot + tail reconstructs exactly the same engine state
    /// as a full replay of every event (segment roundtrip + snapshot fidelity).
    #[test]
    fn boot_from_snapshot_matches_full_replay() {
        let dir = temp_dir("fidelity");

        let (keys, all_events) = {
            let (mut journal, _recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            let (deploy_events, _) = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let mut keys = Vec::new();
            let mut all_events: Vec<Event> = deploy_events.iter().cloned().collect();
            for _ in 0..5 {
                let (events, _) = journal
                    .apply_command(Command::create_instance("demo"))
                    .unwrap();
                keys.push(events.iter().find_map(|e| e.instance_key()).unwrap());
                all_events.extend(events.iter().cloned());
            }
            // Snapshot + rotate midway, so recovery must fuse snapshot + tail.
            let (snap, covered) = journal.snapshot_and_rotate().expect("snapshot+rotate");
            write_snapshot(&dir, snap, covered).expect("write snapshot");
            // A couple more instances after the snapshot boundary.
            for _ in 0..2 {
                let (events, _) = journal
                    .apply_command(Command::create_instance("demo"))
                    .unwrap();
                keys.push(events.iter().find_map(|e| e.instance_key()).unwrap());
                all_events.extend(events.iter().cloned());
            }
            (keys, all_events)
        };

        // Full-replay reference engine.
        let reference = Engine::replay_partition(0, all_events);

        // Snapshot+tail recovery via reopen.
        let (journal, _recovery) =
            crate::journal::Journal::open_segmented(&dir).expect("reopen segmented");

        for key in &keys {
            assert_eq!(
                journal.instance(*key).is_some(),
                reference.instance(*key).is_some(),
                "instance {key:?} presence must match full replay"
            );
        }
        assert_eq!(
            journal.state().processes.len(),
            reference.state().processes.len()
        );

        drop(journal);
        let _ = fs::remove_dir_all(&dir);
    }

    /// End-to-end multi-partition bounded-disk cycle: two partitions share one
    /// segmented WAL; a snapshot of every partition + a combined snapshot lets
    /// per-partition compaction drop the sealed prefix, and a reopen restores
    /// every partition's instances from the combined snapshot.
    #[test]
    fn multi_partition_snapshot_compaction_and_recovery() {
        let dir = temp_dir("multi-roundtrip");
        // Keep the exporter receiver alive so shared writes have a wired cell.
        let (tx, _rx) = std::sync::mpsc::channel::<Arc<Vec<Event>>>();

        let (key0, key1) = {
            let (writer, recovery) =
                crate::journal::SharedWriter::open_segmented(&dir, &[0, 1], 2).expect("open multi");
            let seg = Arc::clone(&recovery.shared);
            assert!(recovery.fresh);
            let mut engines: std::collections::HashMap<u64, Engine> =
                recovery.engines.into_iter().collect();
            let mut j0 = crate::journal::Journal::from_engine_shared(
                0,
                engines.remove(&0).unwrap(),
                true,
                &writer,
            );
            let mut j1 = crate::journal::Journal::from_engine_shared(
                1,
                engines.remove(&1).unwrap(),
                true,
                &writer,
            );
            j0.set_exporter(tx.clone());
            j1.set_exporter(tx.clone());

            // Deploy on partition 0, replicate the definition in-memory to p1.
            let (deploy_events, _) = j0.apply_command(Command::DeployProcess(demo())).unwrap();
            j1.install_deployment(&deploy_events);

            let (e0, _) = j0.apply_command(Command::create_instance("demo")).unwrap();
            let key0 = e0.iter().find_map(|e| e.instance_key()).unwrap();
            let (e1, _) = j1.apply_command(Command::create_instance("demo")).unwrap();
            let key1 = e1.iter().find_map(|e| e.instance_key()).unwrap();
            assert_eq!(nanobpmn_engine_core::partition_of(key1), 1);

            // Snapshot each partition (each seals the shared active segment; only
            // the first seal produces a non-empty sealed segment).
            let (snap0, covered0) = j0.snapshot_and_rotate().expect("snapshot p0");
            let (snap1, covered1) = j1.snapshot_and_rotate().expect("snapshot p1");
            assert_eq!(seg.sealed.lock().unwrap().len(), 1);

            write_multi_snapshot(&dir, vec![(0, covered0, snap0), (1, covered1, snap1)])
                .expect("write combined snapshot");

            // Below partition 1's watermark: retained (the snapshot for p1 does
            // not yet subsume its events in the sealed segment).
            let held_back = [covered0, covered1.saturating_sub(1)];
            assert_eq!(compact_multi(&seg, &held_back, u64::MAX), 0);
            assert_eq!(seg.sealed.lock().unwrap().len(), 1);

            // With every partition's watermark met AND the read model past the
            // segment: compacted away.
            let covered = [covered0, covered1];
            assert_eq!(compact_multi(&seg, &covered, u64::MAX), 1);
            assert!(seg.sealed.lock().unwrap().is_empty());

            (key0, key1)
        };

        // Reopen: both partitions restore purely from the combined snapshot (the
        // sealed prefix was compacted off disk).
        let recovery = recover_multi(&dir, &[0, 1], 2).expect("reopen multi");
        assert!(!recovery.fresh);
        let engines: std::collections::HashMap<u64, Engine> =
            recovery.engines.into_iter().collect();
        assert!(
            engines[&0].instance(key0).is_some(),
            "partition 0 instance restored from snapshot"
        );
        assert!(
            engines[&1].instance(key1).is_some(),
            "partition 1 instance restored from snapshot"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Multi-partition recovery fuses the combined snapshot with the surviving
    /// tail: instances created AFTER the snapshot boundary (uncompacted) are
    /// replayed on top of the per-partition snapshots.
    #[test]
    fn multi_partition_recovery_fuses_snapshot_and_tail() {
        let dir = temp_dir("multi-tail");
        let (tx, _rx) = std::sync::mpsc::channel::<Arc<Vec<Event>>>();

        let (pre0, post1) = {
            let (writer, recovery) =
                crate::journal::SharedWriter::open_segmented(&dir, &[0, 1], 2).expect("open multi");
            let mut engines: std::collections::HashMap<u64, Engine> =
                recovery.engines.into_iter().collect();
            let mut j0 = crate::journal::Journal::from_engine_shared(
                0,
                engines.remove(&0).unwrap(),
                true,
                &writer,
            );
            let mut j1 = crate::journal::Journal::from_engine_shared(
                1,
                engines.remove(&1).unwrap(),
                true,
                &writer,
            );
            j0.set_exporter(tx.clone());
            j1.set_exporter(tx.clone());

            let (deploy_events, _) = j0.apply_command(Command::DeployProcess(demo())).unwrap();
            j1.install_deployment(&deploy_events);

            // Pre-snapshot instance on partition 0.
            let (e0, _) = j0.apply_command(Command::create_instance("demo")).unwrap();
            let pre0 = e0.iter().find_map(|e| e.instance_key()).unwrap();

            let (snap0, covered0) = j0.snapshot_and_rotate().expect("snapshot p0");
            let (snap1, covered1) = j1.snapshot_and_rotate().expect("snapshot p1");
            write_multi_snapshot(&dir, vec![(0, covered0, snap0), (1, covered1, snap1)])
                .expect("write combined snapshot");

            // Post-snapshot instance on partition 1 (lands in the fresh active
            // tail, not covered by any snapshot).
            let (e1, _) = j1.apply_command(Command::create_instance("demo")).unwrap();
            let post1 = e1.iter().find_map(|e| e.instance_key()).unwrap();

            (pre0, post1)
        };

        let recovery = recover_multi(&dir, &[0, 1], 2).expect("reopen multi");
        assert!(!recovery.fresh);
        let engines: std::collections::HashMap<u64, Engine> =
            recovery.engines.into_iter().collect();
        assert!(
            engines[&0].instance(pre0).is_some(),
            "pre-snapshot instance from snapshot"
        );
        assert!(
            engines[&1].instance(post1).is_some(),
            "post-snapshot instance from the surviving tail"
        );

        let _ = fs::remove_dir_all(&dir);
    }
}
