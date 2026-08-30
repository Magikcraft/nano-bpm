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
use std::io::{self, BufWriter, Write};
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

/// Whether the segmented journal should frame-compress its durable writes.
/// Off by default; set `NANOBPMN_JOURNAL_COMPRESS=1` to enable. This is the
/// "value codec": the writer deflates each group-commit batch *before* it hits
/// the active segment, shrinking the physical write bandwidth that bounds
/// large-payload (50 KB-class) throughput — the 264 MB/s per-node disk-write
/// wall. Only the *segmented* path honours it; the legacy single-file journal
/// always writes plaintext.
pub fn journal_compress_from_env() -> bool {
    matches!(
        std::env::var("NANOBPMN_JOURNAL_COMPRESS").as_deref(),
        Ok("1") | Ok("true") | Ok("on") | Ok("yes")
    )
}

/// Frame magic: ASCII RS (record separator, `0x1E`). A plaintext journal record
/// always begins with a digit (`<partition>\t…`) or `{` (bare event JSON), never
/// `0x1E`, so a reader can tell a compressed frame from a legacy line by looking
/// at one byte — which lets a single segment freely interleave the two (the
/// compression flag can flip across a restart while the same active segment is
/// still open).
const FRAME_MAGIC: u8 = 0x1E;
/// Frame carries the batch verbatim (compression declined but framing kept).
const CODEC_RAW: u8 = 0;
/// Frame body is raw-deflate (mirrors the Raft wire codec in `raft_net.rs`).
const CODEC_DEFLATE: u8 = 1;
/// Frame header: `magic(1) | codec(1) | raw_len(u32 LE) | comp_len(u32 LE)`.
const FRAME_HEADER_LEN: usize = 10;
/// Don't frame batches below this — the header + deflate cost isn't worth it,
/// and (crucially) the negligible/high-rate regime commits small batches that
/// must stay verbatim so the single journal-writer thread is never taxed.
const MIN_FRAME_BYTES: usize = 16 * 1024;
/// Only compress when the batch's *mean* event is at least this big. This gates
/// compression to the big-payload regime and skips high-rate small-event
/// batches even when they aggregate past `MIN_FRAME_BYTES`.
const MIN_AVG_EVENT_BYTES: usize = 1024;

/// Frames a group-commit `buf` for durable append, deflating it when it is worth
/// it. Returns `None` when the batch should be written verbatim (too small, mean
/// event too small, deflate failed, or the result didn't actually shrink it) —
/// mixing framed and plaintext records in one segment is expected and the reader
/// tolerates it. Never errors: compression is best-effort, durability is not.
fn frame_compress(buf: &[u8], events: u64) -> Option<Vec<u8>> {
    if buf.len() < MIN_FRAME_BYTES {
        return None;
    }
    if buf.len() / (events.max(1) as usize) < MIN_AVG_EVENT_BYTES {
        return None;
    }
    use std::io::Write;

    use flate2::{Compression, write::DeflateEncoder};
    let mut enc = DeflateEncoder::new(Vec::with_capacity(buf.len() / 2), Compression::fast());
    if enc.write_all(buf).is_err() {
        return None;
    }
    let comp = enc.finish().ok()?;
    // Only worth a frame if it meaningfully shrinks the physical write.
    if comp.len().checked_add(FRAME_HEADER_LEN)? >= buf.len() {
        return None;
    }
    let raw_len = u32::try_from(buf.len()).ok()?;
    let comp_len = u32::try_from(comp.len()).ok()?;
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + comp.len());
    frame.push(FRAME_MAGIC);
    frame.push(CODEC_DEFLATE);
    frame.extend_from_slice(&raw_len.to_le_bytes());
    frame.extend_from_slice(&comp_len.to_le_bytes());
    frame.extend_from_slice(&comp);
    Some(frame)
}

/// Decodes a segment file into its logical plaintext bytes — the concatenated
/// newline-terminated records the writer appended — transparently inflating any
/// compressed frames. Walks the file record-by-record: a [`FRAME_MAGIC`] byte
/// begins a frame, anything else begins a legacy plaintext line, so a segment
/// may freely interleave the two. A torn trailing frame header/body (a crash
/// mid-append that was never fsynced, hence never acked) is dropped, matching
/// the writer's ack-after-write-before-fsync durability contract.
fn decode_segment_bytes(path: &Path) -> io::Result<Vec<u8>> {
    let raw = fs::read(path)?;
    let mut out: Vec<u8> = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == FRAME_MAGIC {
            if i + FRAME_HEADER_LEN > raw.len() {
                break; // torn header tail — nothing durable past here
            }
            let codec = raw[i + 1];
            let raw_len = u32::from_le_bytes(raw[i + 2..i + 6].try_into().unwrap()) as usize;
            let comp_len = u32::from_le_bytes(raw[i + 6..i + 10].try_into().unwrap()) as usize;
            let start = i + FRAME_HEADER_LEN;
            let Some(end) = start.checked_add(comp_len).filter(|e| *e <= raw.len()) else {
                break; // torn frame body
            };
            let payload = &raw[start..end];
            match codec {
                CODEC_RAW => out.extend_from_slice(payload),
                CODEC_DEFLATE => {
                    use std::io::Read;

                    use flate2::read::DeflateDecoder;
                    let before = out.len();
                    DeflateDecoder::new(payload).read_to_end(&mut out)?;
                    if out.len() - before != raw_len {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "journal frame inflated to unexpected length",
                        ));
                    }
                }
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown journal frame codec {other}"),
                    ));
                }
            }
            i = end;
        } else {
            // Legacy plaintext record: copy through the next newline (inclusive).
            // An unterminated trailing line is a crash mid-append that was never
            // newline-terminated — hence never fsynced/acked — so it is dropped,
            // matching the torn-frame contract above and the writer's
            // ack-after-write-before-fsync durability contract. Copying it through
            // would hand a truncated JSON record to the segment reader and panic
            // boot recovery ("EOF while parsing a string").
            match raw[i..].iter().position(|&b| b == b'\n') {
                Some(nl) => {
                    out.extend_from_slice(&raw[i..=i + nl]);
                    i += nl + 1;
                }
                None => break,
            }
        }
    }
    Ok(out)
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

/// Opt-in escape hatch for the unrecoverable read-model compaction gap (see
/// [`CatchUpPlan::CompactedGap`] and [`catch_up_shard`]). When set truthy via
/// `NANOBPMN_READ_MODEL_LOSSY_REBUILD`, a boot that finds the read model below
/// the journal's compaction floor rebuilds it from the surviving journal tail —
/// **permanently dropping** the compacted-away history — instead of aborting.
/// Default (`false`) fails fast so the data loss is never silent.
pub fn read_model_lossy_rebuild_enabled() -> bool {
    match std::env::var("NANOBPMN_READ_MODEL_LOSSY_REBUILD") {
        Ok(v) => matches!(v.trim(), "1" | "true" | "on" | "yes"),
        Err(_) => false,
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
    /// Whether the writer frame-compresses group-commit batches before they hit
    /// the active segment (the value codec). Set from
    /// [`journal_compress_from_env`] on the segmented path; always `false` on the
    /// legacy single-file path.
    pub compress: bool,
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
            compress: false,
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
    /// newline-terminated `buf`) to the active segment. When the shared state
    /// has `compress` set, the batch is frame-compressed first (see
    /// [`frame_compress`]) so the *physical* write — the disk-bandwidth wall for
    /// large payloads — shrinks; `self.bytes` therefore tracks on-disk bytes, so
    /// size-based sealing rotates on physical size.
    pub fn write_all(&mut self, buf: &[u8], events: u64) -> io::Result<()> {
        let written = if self.shared.compress
            && let Some(frame) = frame_compress(buf, events)
        {
            self.file.write_all(&frame)?;
            frame.len() as u64
        } else {
            self.file.write_all(buf)?;
            buf.len() as u64
        };
        self.bytes += written;
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

/// Index of the last line that carries content (any non-whitespace, non-NUL
/// byte), or `None` when every line is blank. Only this final line may be a torn
/// tail — a crash mid-append can leave an unterminated record or NUL padding at
/// end-of-file — so segment readers tolerate a parse failure *there* (dropping
/// it) while still hard-erroring on genuine mid-file corruption.
fn last_content_line(lines: &[&[u8]]) -> Option<usize> {
    lines
        .iter()
        .rposition(|l| l.iter().any(|&b| b != 0 && !b.is_ascii_whitespace()))
}

/// Reads and deserializes every event from a single segment/log file (empty if
/// absent).
pub fn read_segment_events(path: &Path) -> io::Result<Vec<Event>> {
    let mut events = Vec::new();
    if path.exists() {
        let decoded = decode_segment_bytes(path)?;
        let lines: Vec<&[u8]> = decoded.split(|&b| b == b'\n').collect();
        let last = last_content_line(&lines);
        for (idx, line) in lines.iter().enumerate() {
            let torn_tail = Some(idx) == last;
            let line = match std::str::from_utf8(line) {
                Ok(s) => s,
                Err(e) if torn_tail => {
                    tracing::warn!("dropping torn journal tail in {}: {e}", path.display());
                    break;
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            };
            if line.trim().is_empty() {
                continue;
            }
            let event: Event = match serde_json::from_str(line) {
                Ok(ev) => ev,
                Err(e) if torn_tail => {
                    tracing::warn!("dropping torn journal tail in {}: {e}", path.display());
                    break;
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            };
            events.push(event);
        }
    }
    Ok(events)
}

/// Reads a segment/log file written by the shared multi-partition writer, where
/// each line is `<partition>\t<event-json>` — the GLOBAL partition that produced
/// the write (see [`crate::journal::Journal::persist`]). Returns each event with
/// its write-partition tag.
///
/// Tolerates a bare `<event-json>` line (no tab): it falls back to the event's
/// key partition, so a directory written by the pre-tag multi-partition format
/// still recovers with the same demux behaviour it had then.
fn read_segment_events_tagged(path: &Path, num_partitions: usize) -> io::Result<Vec<(u64, Event)>> {
    let mut events = Vec::new();
    if path.exists() {
        let decoded = decode_segment_bytes(path)?;
        let lines: Vec<&[u8]> = decoded.split(|&b| b == b'\n').collect();
        let last = last_content_line(&lines);
        for (idx, line) in lines.iter().enumerate() {
            let torn_tail = Some(idx) == last;
            let line = match std::str::from_utf8(line) {
                Ok(s) => s,
                Err(e) if torn_tail => {
                    tracing::warn!("dropping torn journal tail in {}: {e}", path.display());
                    break;
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            };
            if line.trim().is_empty() {
                continue;
            }
            let (tag, json) = match line.split_once('\t') {
                Some((t, rest)) => (t.parse::<u64>().ok(), rest),
                None => (None, line),
            };
            let event: Event = match serde_json::from_str(json) {
                Ok(ev) => ev,
                Err(e) if torn_tail => {
                    tracing::warn!("dropping torn journal tail in {}: {e}", path.display());
                    break;
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            };
            let tag = tag.unwrap_or_else(|| {
                (partition_of(event.max_key()) as usize).min(num_partitions.saturating_sub(1))
                    as u64
            });
            events.push((tag, event));
        }
    }
    Ok(events)
}

/// Loads the latest persisted snapshot in `dir`.
///
/// Returns `Ok(None)` **only** when no snapshot file exists. A snapshot file
/// that is present but cannot be read is a fatal error carrying the underlying
/// OS error kind (e.g. `PermissionDenied`), one that cannot be deserialized is
/// a fatal `InvalidData` error, and errors listing the directory (or its
/// individual entries) propagate as-is: silently treating any of these as "no
/// snapshot" would fall back to a truncated-tail replay that rewinds the
/// engine key generator and drops the state the snapshot covered (see issue
/// #1065). Surfacing the error lets the operator roll back to a compatible
/// binary or migrate the snapshot rather than corrupt the key space.
fn load_latest_snapshot(dir: &Path) -> io::Result<Option<(EngineSnapshot, u64)>> {
    let mut best: Option<(u64, PathBuf)> = None;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if let Some(covered) = is_snap_file(&name.to_string_lossy())
            && best.as_ref().map(|(c, _)| covered > *c).unwrap_or(true)
        {
            best = Some((covered, entry.path()));
        }
    }
    let Some((_, path)) = best else {
        return Ok(None);
    };
    let bytes = fs::read(&path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "snapshot {} is present but unreadable ({e}); refusing to recover from a \
                 truncated journal, which would rewind the engine key generator and drop \
                 covered state. Restore a compatible binary or remove the snapshot \
                 deliberately. See issue #1065.",
                path.display()
            ),
        )
    })?;
    let snap: PersistedSnapshot = serde_json::from_slice(&bytes).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "snapshot {} failed to deserialize ({e}); the on-disk snapshot schema is \
                 likely incompatible with this binary (schema drift / downgrade). Refusing to \
                 recover from a truncated journal, which would rewind the engine key generator \
                 and drop covered state. Restore a compatible binary or migrate the snapshot. \
                 See issue #1065.",
                path.display()
            ),
        )
    })?;
    Ok(Some((snap.engine, snap.covered_events)))
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

    let snapshot = load_latest_snapshot(dir)?;
    let fresh = total_events == 0 && snapshot.is_none();

    // A compacted journal (`first_index > 0`) has had its event prefix deleted;
    // the only complete source for that prefix is the snapshot. With no loadable
    // snapshot, replaying just the surviving tail would rewind the key generator
    // and silently drop the compacted state — refuse instead (see issue #1065;
    // same "refuse rather than silently resume" principle as #600).
    if snapshot.is_none() && first_index > 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "journal is compacted (first surviving event index {first_index}) but no \
                 loadable snapshot is present; refusing to recover from a truncated tail, \
                 which would rewind the engine key generator and drop compacted state. See \
                 issue #1065."
            ),
        ));
    }

    // Rebuild the engine: snapshot (if any) + replay of the events the snapshot
    // did not cover; otherwise a full replay of the surviving events.
    let engine = match snapshot {
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
        compress: journal_compress_from_env(),
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
    let final_path = dir.join(format!("{SNAP_PREFIX}{covered_events:020}{SNAP_SUFFIX}"));
    let tmp = dir.join(format!(
        "{SNAP_PREFIX}{covered_events:020}{SNAP_SUFFIX}.tmp"
    ));
    {
        // Stream to the file (BufWriter) rather than building a full `Vec<u8>`
        // first; see `write_multi_snapshot` for why the intermediate buffer is
        // a multi-GB transient under a large backlog.
        let f = File::create(&tmp)?;
        let mut w = BufWriter::new(f);
        serde_json::to_writer(&mut w, &payload)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let f = w.into_inner()?;
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
    let final_path = dir.join(MULTI_SNAP_NAME);
    let tmp = dir.join(format!("{MULTI_SNAP_NAME}.tmp"));
    {
        // Stream the JSON straight to the file through a BufWriter instead of
        // materialising the whole snapshot into a `Vec<u8>` first: under a large
        // active backlog that intermediate buffer is multi-GB (all resident
        // variable payloads serialized at once) and was a major driver of the
        // transient RSS balloon during the 60s snapshot tick. The snapshots
        // share the live variables by Arc, so only this serialization ever
        // duplicated them.
        let f = File::create(&tmp)?;
        let mut w = BufWriter::new(f);
        serde_json::to_writer(&mut w, &payload)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let f = w.into_inner()?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &final_path)?;
    fsync_dir(dir);
    Ok(())
}

/// Loads the combined per-partition snapshot, as a map from global partition id
/// to `(covered_count, snapshot)`. `Ok(None)` **only** when the multi-snapshot
/// file is absent; a present-but-unreadable file is a fatal error carrying the
/// underlying OS error kind, and an undeserializable one a fatal `InvalidData`
/// error (see [`load_latest_snapshot`] and issue #1065) — treating schema
/// drift as "no snapshot" would rewind the partition key generators.
fn load_multi_snapshot(
    dir: &Path,
) -> io::Result<Option<std::collections::HashMap<u64, (u64, EngineSnapshot)>>> {
    let path = dir.join(MULTI_SNAP_NAME);
    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(io::Error::new(
                e.kind(),
                format!(
                    "multi-partition snapshot {} is present but unreadable ({e}); refusing to \
                     recover from a truncated journal, which would rewind partition key \
                     generators. Restore a compatible binary. See issue #1065.",
                    path.display()
                ),
            ));
        }
    };
    let snap: MultiPersistedSnapshot = serde_json::from_slice(&bytes).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "multi-partition snapshot {} failed to deserialize ({e}); the on-disk snapshot \
                 schema is likely incompatible with this binary (schema drift / downgrade). \
                 Refusing to recover from a truncated journal, which would rewind partition key \
                 generators and drop covered state. See issue #1065.",
                path.display()
            ),
        )
    })?;
    Ok(Some(
        snap.entries
            .into_iter()
            .map(|e| (e.partition, (e.covered, e.engine)))
            .collect(),
    ))
}

/// The outcome of recovering a multi-partition segmented journal directory.
pub struct MultiSegRecovery {
    /// `false` when prior durable state was recovered.
    pub fresh: bool,
    /// The shared seal/boundary state to hand to the shared writer.
    pub shared: Arc<SegShared>,
    /// All surviving events, ascending, spanning global `[first_index, total_events)`.
    pub events: Vec<Event>,
    /// Surviving events paired with the GLOBAL partition that produced each write
    /// (the write tag), in global log order. Drives the sharded read model's
    /// per-partition boot catch-up: shard `p` resumes from the events tagged `p`
    /// past its own persisted `exported_position` (see [`Self::pp_base`]).
    pub tagged: Vec<(u64, Event)>,
    /// Per-partition cumulative event counts BEFORE the first surviving event
    /// (i.e. the counts compacted away), indexed by global partition id. A shard's
    /// persisted `exported_position` minus `pp_base[p]` is how many surviving
    /// tagged events it has already projected.
    pub pp_base: Vec<u64>,
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
    varstore: Option<&crate::varstore::VarStore>,
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
    // per-partition cumulative counts (keyed by each write's GLOBAL partition
    // TAG, from `pp_base`). Demultiplexing by the persisted write tag — not the
    // event's key partition — is what lets a clustered node route its durable
    // replicated `ProcessDeployed` (written under its first-owned partition but
    // keyed to the deployment partition) back to the partition that produced it.
    let first_index_opt = sealed_files.first().map(|(s, _)| *s);
    let mut sealed: Vec<SealedSeg> = Vec::with_capacity(sealed_files.len());
    // Surviving events with their write-partition tag, in global log order.
    let mut tagged: Vec<(u64, Event)> = Vec::new();
    let mut cursor = first_index_opt.unwrap_or(0);
    let mut pp_running = pp_base.clone();
    for (start, path) in &sealed_files {
        let seg_events = read_segment_events_tagged(path, num_partitions)?;
        let end = start + seg_events.len() as u64;
        for (tag, _) in &seg_events {
            let p = (*tag as usize).min(num_partitions.saturating_sub(1));
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
        tagged.extend(seg_events);
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

    let active_events = read_segment_events_tagged(&active_path, num_partitions)?;
    let total_events = active_start + active_events.len() as u64;
    let first_index = first_index_opt.unwrap_or(active_start);
    // Per-partition totals = active start + active-segment per-partition counts.
    let mut per_partition_total = per_partition_active_start.clone();
    for (tag, _) in &active_events {
        let p = (*tag as usize).min(num_partitions.saturating_sub(1));
        if p < per_partition_total.len() {
            per_partition_total[p] += 1;
        }
    }
    tagged.extend(active_events);

    // `events` (untagged, global order) drives the caller's read-model catch-up.
    let events: Vec<Event> = tagged.iter().map(|(_, e)| e.clone()).collect();

    // Replicated `ProcessDeployed` broadcast set: a durable deployment copy whose
    // write tag differs from its key partition (a clustered peer journaled it
    // under its first-owned partition, keyed to the deployment partition). It is
    // partition-agnostic, so every owned partition OTHER than the one that
    // produced it needs it installed. Applied version-guarded on recovery so an
    // older surviving copy never regresses a newer snapshot-held definition. In
    // single-node mode a deployment's tag equals its key partition, so this set
    // is empty and nothing is broadcast.
    let broadcast: Vec<(u64, Event)> = tagged
        .iter()
        .filter(|(tag, e)| {
            matches!(e, Event::ProcessDeployed { .. })
                && *tag
                    != (partition_of(e.max_key()) as usize).min(num_partitions.saturating_sub(1))
                        as u64
        })
        .cloned()
        .collect();

    let combined = load_multi_snapshot(dir)?;
    let fresh = total_events == 0 && combined.is_none();

    // A compacted journal (`first_index > 0`) has had its event prefix deleted;
    // with no loadable multi-snapshot, replaying just the surviving tail would
    // rewind the partition key generators and silently drop compacted state —
    // refuse instead (see issue #1065).
    if combined.is_none() && first_index > 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "multi-partition journal is compacted (first surviving event index \
                 {first_index}) but no loadable snapshot is present; refusing to recover from \
                 a truncated tail, which would rewind the partition key generators and drop \
                 compacted state. See issue #1065."
            ),
        ));
    }

    // Rebuild each owned partition's engine: snapshot + its surviving tail, or a
    // full replay of its surviving events when there is no snapshot for it. Demux
    // by the write TAG (not the key partition). Then install any replicated
    // deployment broadcast produced under a DIFFERENT partition.
    //
    // Lean-snapshot recovery: when a `varstore` is supplied the combined snapshot
    // is control-only (no variable payloads), so the authoritative durable store
    // holds every live instance's variables as of the snapshot boundary. Load
    // them once and install them onto each from-snapshot engine *before* replaying
    // its tail, so the tail's variable merges/creates apply on the correct base
    // (installing after the tail would regress instances the tail updated). The
    // full-replay branch needs no install — it reconstructs variables from the
    // surviving `ProcessInstanceCreated`/`VariablesUpdated` events directly.
    let stored_vars = varstore.map(|vs| vs.load_all());
    let engines: Vec<(u64, Engine)> = owned
        .iter()
        .map(|&p| {
            let p_events: Vec<Event> = tagged
                .iter()
                .filter(|(tag, _)| *tag == p)
                .map(|(_, e)| e.clone())
                .collect();
            let base_p = pp_base.get(p as usize).copied().unwrap_or(0);
            let mut engine = match combined.as_ref().and_then(|m| m.get(&p)) {
                Some((covered, snap)) => {
                    let mut engine = Engine::from_snapshot(snap.clone());
                    if let Some(all) = stored_vars.as_ref() {
                        for (key, vars) in all {
                            if partition_of(*key) == p {
                                engine.install_variables(*key, vars.clone());
                            }
                        }
                    }
                    let skip = covered.saturating_sub(base_p) as usize;
                    if skip < p_events.len() {
                        engine.apply_replayed_events(p_events[skip..].iter().cloned());
                    }
                    engine
                }
                None => Engine::replay_partition(p, p_events),
            };
            // Install partition-agnostic replicated deployments produced under
            // another partition (skips those this partition already replayed as
            // its own tagged events). Version-guarded so it is monotonic.
            let broadcast_for_p: Vec<Event> = broadcast
                .iter()
                .filter(|(tag, _)| *tag != p)
                .map(|(_, e)| e.clone())
                .collect();
            if !broadcast_for_p.is_empty() {
                engine.install_deployment_if_newer(&broadcast_for_p);
            }
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
        compress: journal_compress_from_env(),
    });

    Ok(MultiSegRecovery {
        fresh,
        shared,
        events,
        tagged,
        pp_base,
        first_index,
        total_events,
        engines,
    })
}

/// Deletes every sealed segment that BOTH the read model and every partition's
/// snapshot no longer need: for every partition `p`, `exported[p] >=
/// seg.per_partition_end[p]` (that partition's read-model shard has projected all
/// of the segment's events — each shard consumes only its partition's events, in
/// log order) AND `covered[p] >= seg.per_partition_end[p]` (each partition's
/// snapshot subsumes its events in the segment). Both `exported` and `covered`
/// are indexed by global partition id (0 for partitions that never advanced).
///
/// The per-partition export gate replaces the earlier single global
/// `exported_position` scalar: with a sharded read model (one exporter thread +
/// store per partition) the shards advance independently, so a global sum could
/// pass while a lagging shard still needs a segment's events — deleting it would
/// lose data on that shard's boot re-projection.
///
/// Removes each segment's per-partition sidecar with it. Returns the count removed.
pub fn compact_multi(shared: &SegShared, covered: &[u64], exported: &[u64]) -> usize {
    let mut sealed = shared.sealed.lock().expect("sealed lock");
    let mut removed = 0usize;
    while let Some(seg) = sealed.first() {
        let export_ok = seg
            .per_partition_end
            .iter()
            .enumerate()
            .all(|(p, end)| exported.get(p).copied().unwrap_or(0) >= *end);
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

/// How a boot read-model catch-up must treat one shard, given its persisted
/// `exported_position` relative to the surviving journal window
/// `[floor, floor + surviving)`. `floor` is the shard's compaction boundary:
/// `SegRecovery::first_index` for the single-partition path, `pp_base[p]` for a
/// per-partition shard. Everything below `floor` has been compacted out of the
/// journal (folded into the engine snapshot) and no longer exists to replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatchUpPlan {
    /// The store sits inside the surviving window: resume projection by skipping
    /// the `skip` surviving events it has already projected, then project the rest.
    Resume { skip: usize },
    /// The store is *ahead* of the surviving log (a truncated/corrupt journal):
    /// reset it and rebuild from whatever survives.
    RebuildFromSurviving,
    /// The store sits *below* the compaction floor: the `missing` events in
    /// `[exported, floor)` it still needs were compacted out of the journal and
    /// cannot be replayed. Resuming would silently drop them — this is the
    /// data-loss failure mode of issue #600 and must never be handled silently.
    CompactedGap { missing: u64 },
}

/// Classifies a read-model shard's catch-up situation. Pure and total so the
/// three boot catch-up sites share one canonical decision (no drift), and the
/// compaction-gap detection is unit-testable in isolation.
///
/// In normal operation `exported >= floor` always holds — compaction is gated on
/// the exporter watermark, so the journal never discards events the read model
/// has not projected. `exported < floor` therefore only arises when the read
/// model was independently wiped or reset (a schema-fingerprint change across a
/// binary upgrade, an unreadable `read-model.sqlite`, or [`ReadStore::reset`])
/// while the journal had already been compacted.
pub fn plan_catch_up(exported: u64, floor: u64, surviving: u64) -> CatchUpPlan {
    if exported < floor {
        CatchUpPlan::CompactedGap {
            missing: floor - exported,
        }
    } else if exported > floor.saturating_add(surviving) {
        CatchUpPlan::RebuildFromSurviving
    } else {
        CatchUpPlan::Resume {
            skip: (exported - floor) as usize,
        }
    }
}

/// Catches one read-model shard up from a segmented recovery, using the single
/// canonical [`plan_catch_up`] decision. `surviving` are this shard's surviving
/// events (log-ordered, spanning `[floor, floor + surviving.len())`).
///
/// `reseed_state` is this shard's authoritative engine [`State`] as rebuilt for
/// boot (snapshot + surviving tail). When a [`CatchUpPlan::CompactedGap`] is hit —
/// the read model has been wiped/reset (e.g. a schema-fingerprint change across a
/// binary upgrade, or an unreadable `read-model.sqlite`) below the journal
/// compaction floor, so the events that would replay into it are gone — the shard
/// is REPROJECTED from that engine state instead of aborting (issue #732). The
/// compaction invariant guarantees the snapshot covers everything below the floor,
/// so every operationally-live entity is recovered losslessly; only terminal audit
/// history the engine already evicted (which only ever lived in the read model) is
/// not restored. This is logged loudly.
///
/// If no engine state is available (`reseed_state` is `None`), the historical
/// behaviour applies: abort with a panic, unless `NANOBPMN_READ_MODEL_LOSSY_REBUILD=1`
/// opts into a lossy rebuild from the (possibly empty) surviving tail — see #600.
pub fn catch_up_shard(
    shard: &crate::readstore::ReadStore,
    floor: u64,
    surviving: &[&Event],
    reseed_state: Option<&nanobpmn_engine_core::State>,
) {
    let exported = shard.exported_position() as u64;
    match plan_catch_up(exported, floor, surviving.len() as u64) {
        CatchUpPlan::Resume { skip } => {
            if skip < surviving.len() {
                shard
                    .export(&surviving[skip..])
                    .expect("catch up read model from segmented journal");
            }
        }
        CatchUpPlan::RebuildFromSurviving => {
            rebuild_from_surviving_tail(
                shard,
                floor,
                surviving,
                "rebuild read model from surviving journal tail",
            );
        }
        CatchUpPlan::CompactedGap { missing } => {
            if let Some(state) = reseed_state {
                let total = floor.saturating_add(surviving.len() as u64);
                tracing::warn!(
                    exported,
                    floor,
                    missing,
                    total,
                    "read model sits below the journal compaction floor (wiped or reset while \
                     the journal was compacted): {missing} events were compacted out of the \
                     journal and cannot be replayed. Reprojecting the read model from the \
                     authoritative engine snapshot instead (issue #732) — all live process \
                     instances, jobs, incidents, user tasks, variables, subscriptions and \
                     definitions are recovered. Terminal/completed audit history that predates \
                     this boot is then restored from the durable terminal-audit archive where \
                     available (issue #831); any history never written to the archive (e.g. \
                     completed before the archive existed) is NOT restored."
                );
                reseed_from_engine_state(shard, total, state);
            } else if read_model_lossy_rebuild_enabled() {
                tracing::error!(
                    exported,
                    floor,
                    missing,
                    "read model sits below the journal compaction floor (wiped or reset while \
                     the journal was compacted): {missing} events were compacted out of the \
                     journal and cannot be replayed. No engine snapshot is available to \
                     reproject from and NANOBPMN_READ_MODEL_LOSSY_REBUILD is set — rebuilding \
                     from the surviving journal tail and PERMANENTLY DROPPING the compacted \
                     history."
                );
                rebuild_from_surviving_tail(
                    shard,
                    floor,
                    surviving,
                    "lossy rebuild read model from surviving journal tail",
                );
            } else {
                panic!(
                    "read model at exported_position={exported} is below the journal compaction \
                     floor={floor}: {missing} events were compacted out of the journal (folded \
                     into the engine snapshot) and can no longer be replayed to rebuild the read \
                     model, and no engine snapshot was available to reproject from. This happens \
                     when the read model is wiped or reset — e.g. a schema-fingerprint change \
                     across a binary upgrade, or an unreadable read-model.sqlite — while the \
                     segmented journal has already been compacted. Proceeding would SILENTLY lose \
                     every pre-compaction process instance. Restore the read model \
                     (read-model.sqlite) from a backup, or set NANOBPMN_READ_MODEL_LOSSY_REBUILD=1 \
                     to rebuild from the surviving journal tail and accept the loss. See issues \
                     #600 and #732."
                );
            }
        }
    }
}

/// Resets a shard and reprojects it from the authoritative boot engine `state`,
/// then plants the absolute cursor at `total` (== `floor + surviving.len()`, the
/// full projected event count this state already reflects). Setting the cursor is
/// essential: `exported_position` is an ABSOLUTE event index checked against the
/// compaction floor on every boot, so leaving it below the floor would re-trip
/// [`CatchUpPlan::CompactedGap`] on the next boot and reproject forever.
fn reseed_from_engine_state(
    shard: &crate::readstore::ReadStore,
    total: u64,
    state: &nanobpmn_engine_core::State,
) {
    shard.reset().expect("reset read store");
    shard
        .seed_from_engine_state(state)
        .expect("reproject read model from engine snapshot");
    // Replay the durable terminal-audit archive on top of the live snapshot
    // (issue #831): the snapshot only carries live instances, so completed/
    // terminal history — which lived only in the read model — is restored here
    // from its durable home. Best-effort: an archive read failure must not abort
    // boot recovery of the (already reprojected) live state.
    match shard.replay_terminal_archive() {
        Ok(restored) if restored > 0 => tracing::info!(
            restored,
            "restored {restored} terminal/completed process instances from the durable \
             terminal-audit archive on top of the engine-snapshot reprojection (issue #831)"
        ),
        Ok(_) => {}
        Err(e) => tracing::warn!(
            error = %e,
            "could not replay the durable terminal-audit archive during reprojection \
             (issue #831); live instances are still recovered from the engine snapshot"
        ),
    }
    if total > 0 {
        shard
            .advance_exported(total as usize)
            .expect("advance exported_position to the recovered event count");
    }
}

/// Resets a shard and rebuilds it from just the surviving tail. Because
/// `exported_position` is an ABSOLUTE event index (compared against the
/// compaction `floor` on every boot), the cursor is advanced to `floor` before
/// the tail is projected — so it lands at `floor + surviving.len()`
/// (== `total_events`), not a relative `surviving.len()`. Skipping this would
/// leave the cursor below `floor` and re-trip [`CatchUpPlan::CompactedGap`] on
/// the very next boot, so even the opt-in lossy rebuild would never stick.
fn rebuild_from_surviving_tail(
    shard: &crate::readstore::ReadStore,
    floor: u64,
    surviving: &[&Event],
    export_ctx: &str,
) {
    shard.reset().expect("reset read store");
    if floor > 0 {
        shard
            .advance_exported(floor as usize)
            .expect("advance exported_position to the compaction floor");
    }
    if !surviving.is_empty() {
        shard.export(surviving).expect(export_ctx);
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

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

    /// Builds a segmented dir whose journal prefix has been compacted away:
    /// deploy + instance -> snapshot + rotate + compaction (drops the sealed
    /// prefix) -> a second instance in the fresh active segment. Returns the dir
    /// (all writer handles dropped, ready to reopen) and both instance keys.
    /// After this, the only complete source for the compacted prefix is the
    /// snapshot — exactly the state that turns a swallowed snapshot-decode error
    /// into a key-generator rewind (issue #1065).
    fn compacted_dir_with_two_instances(tag: &str) -> (PathBuf, u64, u64) {
        let dir = temp_dir(tag);
        let (key1, key2) = {
            let (mut journal, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            let shared = Arc::clone(&recovery.shared);
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let (events, _) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let key1 = events.iter().find_map(|e| e.instance_key()).unwrap();

            let (snap, covered) = journal.snapshot_and_rotate().expect("snapshot+rotate");
            write_snapshot(&dir, snap, covered).expect("write snapshot");
            assert_eq!(
                compact(&shared, covered),
                1,
                "the sealed prefix is compacted"
            );
            assert!(list_sealed(&dir).unwrap().is_empty());

            let (events, _) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let key2 = events.iter().find_map(|e| e.instance_key()).unwrap();
            (key1, key2)
        };
        (dir, key1, key2)
    }

    /// Returns the path of the (single) persisted snapshot file in `dir`.
    fn snapshot_file(dir: &Path) -> PathBuf {
        fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .and_then(is_snap_file)
                    .is_some()
            })
            .expect("a snapshot file exists")
    }

    /// RED/GREEN for issue #1065: a snapshot file that is *present but
    /// undeserializable* (schema drift after a binary upgrade) must abort
    /// recovery, not be silently swallowed. Swallowing it fell through to a
    /// replay of only the compacted tail, which rewound the engine key generator
    /// (minting a fresh low `instance 41` while live keys were ~70000) and
    /// dropped the state the snapshot covered.
    #[test]
    fn recover_rejects_a_present_but_undeserializable_snapshot() {
        let (dir, _key1, _key2) = compacted_dir_with_two_instances("corrupt-snap");

        // Simulate schema drift: the snapshot file is still there, but this
        // binary can no longer deserialize it.
        fs::write(snapshot_file(&dir), b"{\"not\":\"a valid snapshot\"}").unwrap();

        let err = match crate::journal::Journal::open_segmented(&dir) {
            Ok(_) => {
                panic!(
                    "recovery must refuse an undeserializable snapshot, not rewind the key space"
                )
            }
            Err(e) => e,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let _ = fs::remove_dir_all(&dir);
    }

    /// RED/GREEN for the #1065 defect (raised in the #1066 review): a snapshot that is
    /// present but *unreadable by the OS* (e.g. wrong permissions) must fail loud **with
    /// the underlying OS error kind preserved** — operators need the actionable
    /// cause (`PermissionDenied`), not a re-mapped `InvalidData` that looks
    /// like schema drift.
    #[cfg(unix)]
    #[test]
    fn recover_reports_the_os_cause_for_an_unreadable_snapshot() {
        let (dir, _key1, _key2) = compacted_dir_with_two_instances("unreadable-snap");
        let snap = snapshot_file(&dir);
        fs::set_permissions(&snap, fs::Permissions::from_mode(0o000)).unwrap();

        let err = match crate::journal::Journal::open_segmented(&dir) {
            Ok(_) => {
                panic!("recovery must refuse an unreadable snapshot, not rewind the key space")
            }
            Err(e) => e,
        };
        assert_eq!(
            err.kind(),
            io::ErrorKind::PermissionDenied,
            "the OS error kind must survive the fail-loud wrapping: {err}"
        );

        fs::set_permissions(&snap, fs::Permissions::from_mode(0o600)).unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    /// Same guard for the multi-partition snapshot path: a present-but-unreadable
    /// multi-snapshot must fail loud with the OS error kind preserved.
    #[cfg(unix)]
    #[test]
    fn load_multi_snapshot_reports_the_os_cause_for_an_unreadable_file() {
        let dir = temp_dir("unreadable-msnap");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(MULTI_SNAP_NAME);
        fs::write(&path, b"opaque").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();

        let err = load_multi_snapshot(&dir)
            .expect_err("an unreadable multi-snapshot must fail loud, not vanish");
        assert_eq!(
            err.kind(),
            io::ErrorKind::PermissionDenied,
            "the OS error kind must survive the fail-loud wrapping: {err}"
        );

        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    /// A compacted journal (`first_index > 0`) with no snapshot at all is
    /// unrecoverable: replaying only the surviving tail would rewind the key
    /// generator and drop the compacted prefix. Recovery must refuse rather than
    /// silently resume (issue #1065; same principle as issue #600 for the read
    /// store).
    #[test]
    fn recover_rejects_a_compacted_journal_with_no_snapshot() {
        let (dir, _key1, _key2) = compacted_dir_with_two_instances("missing-snap");

        fs::remove_file(snapshot_file(&dir)).unwrap();

        let err = match crate::journal::Journal::open_segmented(&dir) {
            Ok(_) => panic!(
                "recovery must refuse a compacted journal with no snapshot, not rewind the key space"
            ),
            Err(e) => e,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let _ = fs::remove_dir_all(&dir);
    }

    /// GREEN guard against the key rewind itself: a *valid* snapshot recovery
    /// preserves the key high-water, so the next minted instance key is strictly
    /// greater than the last one minted before the restart — never a rewound low
    /// key like the incident's `41`.
    #[test]
    fn recover_preserves_the_key_high_water_across_a_valid_snapshot() {
        let (dir, key1, key2) = compacted_dir_with_two_instances("high-water");
        assert!(key2 > key1);

        let next_key = {
            let (mut reopened, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("reopen segmented");
            assert!(!recovery.fresh);
            assert!(reopened.instance(key1).is_some(), "snapshot restores key1");
            assert!(
                reopened.instance(key2).is_some(),
                "active tail restores key2"
            );
            let (events, _) = reopened
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            events.iter().find_map(|e| e.instance_key()).unwrap()
        };
        assert!(
            next_key > key2,
            "the key generator must not rewind across recovery: minted {next_key} <= prior {key2}"
        );

        let _ = fs::remove_dir_all(&dir);
    }
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
        let (tx, _rx) = std::sync::mpsc::channel::<crate::journal::ExportBatch>();

        let (key0, key1) = {
            let (writer, recovery) =
                crate::journal::SharedWriter::open_segmented(&dir, &[0, 1], 2, None)
                    .expect("open multi");
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
            j0.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));
            j1.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));

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
            assert_eq!(compact_multi(&seg, &held_back, &[u64::MAX; 2]), 0);
            assert_eq!(seg.sealed.lock().unwrap().len(), 1);

            // With every partition's watermark met AND the read model past the
            // segment: compacted away.
            let covered = [covered0, covered1];
            // Snapshot subsumes the segment for every partition, but p1's
            // read-model shard has not yet projected its events in the segment:
            // the per-partition export gate keeps it.
            let exported_lag = [u64::MAX, covered1.saturating_sub(1)];
            assert_eq!(compact_multi(&seg, &covered, &exported_lag), 0);
            assert_eq!(seg.sealed.lock().unwrap().len(), 1);
            assert_eq!(compact_multi(&seg, &covered, &[u64::MAX; 2]), 1);
            assert!(seg.sealed.lock().unwrap().is_empty());

            (key0, key1)
        };

        // Reopen: both partitions restore purely from the combined snapshot (the
        // sealed prefix was compacted off disk).
        let recovery = recover_multi(&dir, &[0, 1], 2, None).expect("reopen multi");
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
        let (tx, _rx) = std::sync::mpsc::channel::<crate::journal::ExportBatch>();

        let (pre0, post1) = {
            let (writer, recovery) =
                crate::journal::SharedWriter::open_segmented(&dir, &[0, 1], 2, None)
                    .expect("open multi");
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
            j0.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));
            j1.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));

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
            // tail, not covered by any snapshot). Await its commit so the write
            // is durable before we drop the writer and recover (otherwise it
            // races the background group-commit thread).
            let (e1, commit) = j1.apply_command(Command::create_instance("demo")).unwrap();
            let post1 = e1.iter().find_map(|e| e.instance_key()).unwrap();
            commit.blocking_wait();

            (pre0, post1)
        };

        let recovery = recover_multi(&dir, &[0, 1], 2, None).expect("reopen multi");
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

    /// Lean-snapshot recovery: with an authoritative var store wired, the
    /// periodic checkpoint captures a **control-only** snapshot (no variables) and
    /// writes the variable delta to the store. Recovery must fuse the control
    /// snapshot + the store's variables + the journal tail so the restored
    /// variables exactly match a full replay — including a `SetVariables` applied
    /// AFTER the checkpoint (which merges onto the store's base on replay).
    #[test]
    fn lean_snapshot_recovery_matches_full_replay() {
        use std::collections::HashMap;

        use nanobpmn_engine_core::Value;

        let dir = temp_dir("lean-roundtrip");
        let (tx, _rx) = std::sync::mpsc::channel::<crate::journal::ExportBatch>();
        // Authoritative, boot-surviving store. In-process the same Arc models the
        // durable store surviving the journal reopen.
        let varstore = Arc::new(crate::varstore::VarStore::open(None).expect("open var store"));

        let key = {
            let (writer, recovery) =
                crate::journal::SharedWriter::open_segmented(&dir, &[0, 1], 2, Some(&varstore))
                    .expect("open multi");
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
            j0.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));
            j1.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));
            // Lean mode: the store is authoritative for variables on both journals.
            j0.set_varstore(Arc::clone(&varstore));
            j1.set_varstore(Arc::clone(&varstore));

            let (deploy_events, _) = j0.apply_command(Command::DeployProcess(demo())).unwrap();
            j1.install_deployment(&deploy_events);

            // Create with initial variables, then merge a second batch — all
            // BEFORE the checkpoint (so they live only in the store + control
            // snapshot, never in a variable-bearing snapshot).
            let mut init = HashMap::new();
            init.insert("a".to_string(), Value::Int(1i64));
            let (e0, _) = j0
                .apply_command(Command::create_instance_with("demo", init))
                .unwrap();
            let key = e0.iter().find_map(|e| e.instance_key()).unwrap();
            let mut pre = HashMap::new();
            pre.insert("b".to_string(), Value::Int(2i64));
            let _ = j0.apply_command(Command::set_variables(key, pre)).unwrap();

            // Lean checkpoint on both partitions: drain + control-only snapshot +
            // seal, then persist the delta to the store BEFORE writing the snapshot.
            let mut entries = Vec::new();
            for (j, pid) in [(&mut j0, 0u64), (&mut j1, 1u64)] {
                let (snap, covered, upserts, forgets) =
                    j.snapshot_and_rotate_lean().expect("lean checkpoint");
                let ups: Vec<(nanobpmn_engine_core::Key, &HashMap<String, Value>)> =
                    upserts.iter().map(|(k, v)| (*k, v.as_ref())).collect();
                varstore.checkpoint(pid, covered, &ups, &forgets).unwrap();
                entries.push((pid, covered, snap));
            }
            write_multi_snapshot(&dir, entries).expect("write lean snapshot");

            // Post-checkpoint variable merge: lands in the fresh active tail (NOT
            // covered by the snapshot, NOT yet in the store's checkpoint) — the
            // merge-onto-base case recovery must get right. Await its commit so
            // the write is durable in the segment before we drop the writer and
            // recover (otherwise it races the background group-commit thread).
            let mut post = HashMap::new();
            post.insert("c".to_string(), Value::Int(3i64));
            let (_, commit) = j0.apply_command(Command::set_variables(key, post)).unwrap();
            commit.blocking_wait();

            key
        };

        // Reopen: control from the lean snapshot, variables installed from the
        // store, then the tail `SetVariables` merged on top.
        let recovery = recover_multi(&dir, &[0, 1], 2, Some(&varstore)).expect("reopen multi");
        assert!(!recovery.fresh);
        let engines: std::collections::HashMap<u64, Engine> =
            recovery.engines.into_iter().collect();
        let inst = engines[&0].instance(key).expect("instance restored");
        assert_eq!(
            inst.variables.get("a"),
            Some(&Value::Int(1i64)),
            "create-time variable from the store"
        );
        assert_eq!(
            inst.variables.get("b"),
            Some(&Value::Int(2i64)),
            "pre-checkpoint merge from the store"
        );
        assert_eq!(
            inst.variables.get("c"),
            Some(&Value::Int(3i64)),
            "post-checkpoint merge from the journal tail"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Clustered recovery: a node owning partitions {1,2} (NOT the deployment
    /// partition 0) journals a single durable replicated `ProcessDeployed` under
    /// its first-owned partition (1), keyed to partition 0. On recovery, the
    /// write TAG routes it back to partition 1, and the version-guarded broadcast
    /// re-installs the partition-agnostic definition into partition 2, so both
    /// partitions can restore instances that reference it.
    #[test]
    fn clustered_replicated_deployment_recovers_across_owned_partitions() {
        let dir = temp_dir("cluster-deploy");
        let (tx, _rx) = std::sync::mpsc::channel::<crate::journal::ExportBatch>();

        // Mint a deployment on partition 0 (the definition's home) to obtain the
        // partition-0-keyed `ProcessDeployed` events a peer node would receive.
        let deploy_events: Vec<Event> = {
            let mut p0 = crate::journal::Journal::in_memory_partition(0);
            let (evs, _) = p0.apply_command(Command::DeployProcess(demo())).unwrap();
            evs.iter().cloned().collect()
        };
        assert!(deploy_events.iter().all(|e| partition_of(e.max_key()) == 0));

        let (inst1, inst2) = {
            // This node owns {1,2}; the shared segmented WAL spans a 3-partition
            // cluster.
            let (writer, recovery) =
                crate::journal::SharedWriter::open_segmented(&dir, &[1, 2], 3, None)
                    .expect("open clustered");
            let mut engines: std::collections::HashMap<u64, Engine> =
                recovery.engines.into_iter().collect();
            let mut j1 = crate::journal::Journal::from_engine_shared(
                1,
                engines.remove(&1).unwrap(),
                true,
                &writer,
            );
            let mut j2 = crate::journal::Journal::from_engine_shared(
                2,
                engines.remove(&2).unwrap(),
                true,
                &writer,
            );
            j1.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));
            j2.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));

            // Durable copy on the first-owned partition (tagged 1, keyed 0);
            // in-memory on the rest. Dropping the writer at block end flushes it.
            let _ = j1.install_deployment_durable(&deploy_events);
            j2.install_deployment(&deploy_events);

            let (e1, _) = j1.apply_command(Command::create_instance("demo")).unwrap();
            let inst1 = e1.iter().find_map(|e| e.instance_key()).unwrap();
            let (e2, _) = j2.apply_command(Command::create_instance("demo")).unwrap();
            let inst2 = e2.iter().find_map(|e| e.instance_key()).unwrap();
            assert_eq!(partition_of(inst1), 1);
            assert_eq!(partition_of(inst2), 2);

            // Flush the detached writer deterministically: a rotate is a writer
            // barrier (blocks until every prior write is fsynced). We seal but do
            // NOT write a combined snapshot, so recovery rebuilds by full replay —
            // exercising the broadcast as partition 2's SOLE definition source.
            let _ = j1.snapshot_and_rotate().expect("flush via rotate");

            (inst1, inst2)
        };

        // Reopen as the same clustered node: both partitions must restore the
        // definition (p1 from its tagged durable copy, p2 from the broadcast) and
        // their instances.
        let recovery = recover_multi(&dir, &[1, 2], 3, None).expect("reopen clustered");
        assert!(!recovery.fresh);
        let engines: std::collections::HashMap<u64, Engine> =
            recovery.engines.into_iter().collect();
        assert!(
            engines[&1].instance(inst1).is_some(),
            "partition 1 instance restored"
        );
        assert!(
            engines[&2].instance(inst2).is_some(),
            "partition 2 instance restored (definition arrived via broadcast)"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// A large, compressible group-commit batch survives the value-codec
    /// round-trip: `frame_compress` shrinks it (the point) and
    /// `decode_segment_bytes` reconstructs the exact original bytes.
    #[test]
    fn journal_frame_compress_roundtrips_and_shrinks() {
        let dir = temp_dir("frame-roundtrip");
        fs::create_dir_all(&dir).unwrap();

        // Realistic-ish repeated JSON lines: highly compressible, one big event.
        let mut buf = Vec::new();
        for i in 0..64 {
            buf.extend_from_slice(
                format!(
                    "0\t{{\"seq\":{i},\"payload\":\"{}\"}}\n",
                    "abcdefgh".repeat(256)
                )
                .as_bytes(),
            );
        }
        assert!(buf.len() >= MIN_FRAME_BYTES, "batch big enough to frame");

        let frame = frame_compress(&buf, 64).expect("large compressible batch frames");
        assert!(
            frame.len() < buf.len(),
            "frame shrank the physical write: {} -> {}",
            buf.len(),
            frame.len()
        );
        assert_eq!(frame[0], FRAME_MAGIC);
        assert_eq!(frame[1], CODEC_DEFLATE);

        let path = dir.join("frame.bin");
        fs::write(&path, &frame).unwrap();
        assert_eq!(
            decode_segment_bytes(&path).unwrap(),
            buf,
            "exact round-trip"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// The size/mean-event gates keep the negligible/high-rate regime verbatim:
    /// a small batch (or one of tiny events) declines framing, so the writer
    /// thread is never taxed and the segment stays legacy plaintext.
    #[test]
    fn journal_frame_compress_declines_small_batches() {
        // Below the byte floor.
        assert!(frame_compress(b"0\t{}\n", 1).is_none());
        // Past the byte floor but the mean event is tiny (many small events).
        let many_small: Vec<u8> = std::iter::repeat_n(b"0\t{\"x\":1}\n", 4096)
            .flatten()
            .copied()
            .collect();
        assert!(many_small.len() >= MIN_FRAME_BYTES);
        assert!(
            frame_compress(&many_small, 4096).is_none(),
            "tiny mean event skips compression"
        );
    }

    /// A segment may interleave legacy plaintext lines and compressed frames
    /// (the flag can flip across a restart while the same active segment is
    /// open). `decode_segment_bytes` walks record-by-record and reconstructs the
    /// concatenated logical stream regardless of the mix.
    #[test]
    fn journal_decode_handles_interleaved_plaintext_and_frames() {
        let dir = temp_dir("frame-interleave");
        fs::create_dir_all(&dir).unwrap();

        let head = b"0\t{\"seq\":\"head\"}\n".to_vec();
        let mut mid = Vec::new();
        for i in 0..40 {
            mid.extend_from_slice(
                format!("0\t{{\"seq\":{i},\"p\":\"{}\"}}\n", "z".repeat(1400)).as_bytes(),
            );
        }
        let tail = b"0\t{\"seq\":\"tail\"}\n".to_vec();

        let frame = frame_compress(&mid, 40).expect("mid batch frames");
        let mut file = Vec::new();
        file.extend_from_slice(&head); // legacy plaintext
        file.extend_from_slice(&frame); // compressed frame
        file.extend_from_slice(&tail); // legacy plaintext again

        let path = dir.join("mixed.jsonl");
        fs::write(&path, &file).unwrap();

        let mut expected = head.clone();
        expected.extend_from_slice(&mid);
        expected.extend_from_slice(&tail);
        assert_eq!(decode_segment_bytes(&path).unwrap(), expected);

        let _ = fs::remove_dir_all(&dir);
    }

    /// A torn trailing frame (crash mid-append, never fsynced/acked) is dropped:
    /// decode returns the durable prefix and never errors, matching the writer's
    /// ack-after-write-before-fsync contract.
    #[test]
    fn journal_decode_drops_a_torn_frame_tail() {
        let dir = temp_dir("frame-torn");
        fs::create_dir_all(&dir).unwrap();

        let durable = b"0\t{\"seq\":\"durable\"}\n".to_vec();
        let mut big = Vec::new();
        for i in 0..40 {
            big.extend_from_slice(
                format!("0\t{{\"seq\":{i},\"p\":\"{}\"}}\n", "q".repeat(1400)).as_bytes(),
            );
        }
        let frame = frame_compress(&big, 40).expect("frames");

        // Truncate the frame mid-body to simulate a torn write.
        let mut file = durable.clone();
        file.extend_from_slice(&frame[..frame.len() - 4]);

        let path = dir.join("torn.jsonl");
        fs::write(&path, &file).unwrap();
        assert_eq!(
            decode_segment_bytes(&path).unwrap(),
            durable,
            "durable prefix survives, torn frame dropped"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// End-to-end through the segment reader: a compressed active segment written
    /// by `ActiveSegment::write_all` (with `compress` on) recovers the exact same
    /// events as the plaintext path.
    #[test]
    fn compressed_active_segment_recovers_events() {
        let dir = temp_dir("frame-active");
        fs::create_dir_all(&dir).unwrap();

        // Serialize real events by minting them through an in-memory journal, so
        // the reader exercises the true event JSON shape.
        let mut j = crate::journal::Journal::in_memory_partition(0);
        let (deploy, _) = j.apply_command(Command::DeployProcess(demo())).unwrap();
        let mut lines = Vec::new();
        let mut n_events: u64 = 0;
        for e in deploy.iter() {
            lines.extend_from_slice(serde_json::to_string(e).unwrap().as_bytes());
            lines.push(b'\n');
            n_events += 1;
        }
        for _ in 0..80 {
            let mut vars = std::collections::HashMap::new();
            vars.insert(
                "payload".to_string(),
                nanobpmn_engine_core::Value::Str("y".repeat(16384)),
            );
            let (evs, _) = j
                .apply_command(Command::create_instance_with("demo", vars))
                .unwrap();
            for e in evs.iter() {
                lines.extend_from_slice(serde_json::to_string(e).unwrap().as_bytes());
                lines.push(b'\n');
                n_events += 1;
            }
        }

        // Baseline: plaintext segment.
        let plain_path = dir.join("plain.jsonl");
        fs::write(&plain_path, &lines).unwrap();
        let baseline = read_segment_events(&plain_path).unwrap();
        assert_eq!(baseline.len() as u64, n_events);

        // Compressed active segment via the real writer path.
        let active_path = dir.join(ACTIVE_NAME);
        let shared = Arc::new(SegShared {
            dir: dir.clone(),
            active_path: active_path.clone(),
            total_events: AtomicU64::new(0),
            active_start: AtomicU64::new(0),
            sealed: Mutex::new(Vec::new()),
            segment_bytes: u64::MAX,
            per_partition_total: Vec::new(),
            per_partition_active_start: Vec::new(),
            compress: true,
        });
        {
            let mut seg = ActiveSegment::open(Arc::clone(&shared)).unwrap();
            seg.write_all(&lines, n_events).unwrap();
            seg.fsync().unwrap();
        }
        // The on-disk segment must actually be a compressed frame, not plaintext.
        let on_disk = fs::read(&active_path).unwrap();
        assert_eq!(
            on_disk[0], FRAME_MAGIC,
            "active segment is frame-compressed"
        );
        assert!(on_disk.len() < lines.len(), "physical write shrank");

        let recovered = read_segment_events(&active_path).unwrap();
        assert_eq!(recovered.len(), baseline.len());
        // The strongest guarantee: the compressed segment decodes to the exact
        // bytes a plaintext write would have produced (re-serializing parsed
        // events would be flaky — the process definition's element map has
        // nondeterministic iteration order).
        assert_eq!(
            decode_segment_bytes(&active_path).unwrap(),
            lines,
            "compressed active segment decodes byte-for-byte to the plaintext batch"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// A crash mid-append of a plaintext batch leaves the final record without a
    /// terminating newline. `decode_segment_bytes` drops that torn tail (never
    /// hands a truncated line to the reader) and keeps the durable prefix.
    #[test]
    fn journal_decode_drops_a_torn_plaintext_tail() {
        let dir = temp_dir("plain-torn");
        fs::create_dir_all(&dir).unwrap();

        let durable = b"0\t{\"seq\":\"one\"}\n0\t{\"seq\":\"two\"}\n".to_vec();
        let mut file = durable.clone();
        // Unterminated (torn) trailing line — no `\n`.
        file.extend_from_slice(b"0\t{\"seq\":\"tor");

        let path = dir.join("torn-plain.jsonl");
        fs::write(&path, &file).unwrap();
        assert_eq!(
            decode_segment_bytes(&path).unwrap(),
            durable,
            "durable prefix survives, torn unterminated line dropped"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Boot recovery must never panic on a torn journal tail. Both a truncated
    /// final record and NUL padding at end-of-file (a preallocated file killed
    /// mid-write) recover the durable prefix instead of erroring — but genuine
    /// corruption before the final line still errors.
    #[test]
    fn read_segment_tolerates_torn_and_nul_padded_tail() {
        let dir = temp_dir("seg-torn-tail");
        fs::create_dir_all(&dir).unwrap();

        // Mint two real events so the reader exercises the true JSON shape.
        let mut j = crate::journal::Journal::in_memory_partition(0);
        let (deploy, _) = j.apply_command(Command::DeployProcess(demo())).unwrap();
        let (create, _) = j
            .apply_command(Command::create_instance_with(
                "demo",
                std::collections::HashMap::new(),
            ))
            .unwrap();
        let mut good = Vec::new();
        let mut n: u64 = 0;
        for e in deploy.iter().chain(create.iter()) {
            good.extend_from_slice(serde_json::to_string(e).unwrap().as_bytes());
            good.push(b'\n');
            n += 1;
        }

        // Case 1: truncated final record (no newline).
        let mut torn = good.clone();
        torn.extend_from_slice(br#"{"CreateInstance":{"process":"de"#);
        let p1 = dir.join("torn.jsonl");
        fs::write(&p1, &torn).unwrap();
        assert_eq!(
            read_segment_events(&p1).unwrap().len() as u64,
            n,
            "truncated tail dropped, durable events recovered"
        );

        // Case 2: a fully newline-terminated but unparseable final line — the
        // shape a torn write leaves when reused-block garbage contains a `\n`, so
        // it survives `decode_segment_bytes` and must be dropped by the reader.
        let mut garbage = good.clone();
        garbage.extend_from_slice(b"}\x00torngarbage{not-json\n");
        let p2 = dir.join("garbage.jsonl");
        fs::write(&p2, &garbage).unwrap();
        assert_eq!(
            read_segment_events(&p2).unwrap().len() as u64,
            n,
            "terminated-but-unparseable tail dropped, durable events recovered"
        );

        // Case 3: corruption BEFORE the final valid line still errors — only the
        // tail is allowed to be torn.
        let mut mid_corrupt = Vec::new();
        mid_corrupt.extend_from_slice(b"{ this is not valid json");
        mid_corrupt.push(b'\n');
        mid_corrupt.extend_from_slice(&good);
        let p3 = dir.join("midcorrupt.jsonl");
        fs::write(&p3, &mid_corrupt).unwrap();
        assert!(
            read_segment_events(&p3).is_err(),
            "mid-file corruption is not silently dropped"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// The catch-up classifier is the single source of truth for the three boot
    /// sites. In the normal window it resumes; above the tail it rebuilds; and —
    /// the issue #600 guard — a store *below* the compaction floor is a
    /// `CompactedGap`, never a silent `Resume`.
    #[test]
    fn plan_catch_up_classifies_every_position() {
        // Fresh dir (no compaction): floor 0, resume from the start.
        assert_eq!(plan_catch_up(0, 0, 5), CatchUpPlan::Resume { skip: 0 });
        // Warm resume inside the surviving window.
        assert_eq!(
            plan_catch_up(4850, 4848, 10),
            CatchUpPlan::Resume { skip: 2 }
        );
        // Exactly at the floor: resume, replaying the whole surviving tail.
        assert_eq!(
            plan_catch_up(4848, 4848, 10),
            CatchUpPlan::Resume { skip: 0 }
        );
        // Exactly at the tail end: resume with nothing left to project.
        assert_eq!(
            plan_catch_up(4858, 4848, 10),
            CatchUpPlan::Resume { skip: 10 }
        );
        // Past the tail (truncated/corrupt log): rebuild from what survives.
        assert_eq!(
            plan_catch_up(4859, 4848, 10),
            CatchUpPlan::RebuildFromSurviving
        );
        // Below the floor (wiped/reset read model over a compacted journal): the
        // defect. Must be a gap, NOT `Resume { skip: 0 }` (which silently drops
        // the compacted-away history — issue #600).
        assert_eq!(
            plan_catch_up(0, 4848, 10),
            CatchUpPlan::CompactedGap { missing: 4848 }
        );
        assert_eq!(
            plan_catch_up(4000, 4848, 10),
            CatchUpPlan::CompactedGap { missing: 848 }
        );
    }

    /// End-to-end reproduction of issue #600: a read model wiped below the
    /// journal's compaction floor is **refused** (default) rather than silently
    /// resumed into a partial/empty projection; with the opt-in escape hatch it
    /// rebuilds loudly from the surviving tail.
    #[test]
    fn wiped_read_model_over_compacted_journal_is_refused() {
        use nanobpmn_engine_core::Command;

        use crate::readstore::ReadStore;

        let dir = temp_dir("readmodel-gap-600");

        // Build a segmented journal, snapshot+rotate+compact so history moves
        // into the snapshot and `first_index > 0`, then add a self-consistent
        // post-compaction tail (re-deploy + instance) that projects cleanly.
        {
            let (mut journal, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            let shared = Arc::clone(&recovery.shared);
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let _ = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let (snap, covered) = journal.snapshot_and_rotate().expect("snapshot+rotate");
            write_snapshot(&dir, snap, covered).expect("write snapshot");
            assert_eq!(compact(&shared, covered), 1, "sealed prefix compacted away");
            // Post-compaction, self-consistent tail.
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let _ = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
        }

        // Reopen: the surviving tail sits above a non-zero compaction floor.
        let (_engine, recovery) =
            crate::journal::Journal::open_segmented(&dir).expect("reopen segmented");
        assert!(
            recovery.first_index > 0,
            "compaction must leave a non-zero floor to reproduce the gap"
        );
        let surviving: Vec<&Event> = recovery.events.iter().collect();

        // A freshly wiped read model reports `exported_position == 0`, i.e. below
        // the compaction floor — the exact state after a schema-fingerprint wipe.
        let store = ReadStore::open(None).expect("fresh in-memory read store");
        assert_eq!(store.exported_position(), 0);

        // Default: refuse. Suppress the panic hook so the expected abort doesn't
        // spam the test log.
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            catch_up_shard(&store, recovery.first_index, &surviving, None);
        }));
        std::panic::set_hook(prev);
        assert!(
            refused.is_err(),
            "a read model below the compaction floor must abort, not silently resume"
        );
        assert_eq!(
            store.exported_position(),
            0,
            "the refused catch-up must not have advanced the read model"
        );

        // Opt-in escape hatch: rebuild from the surviving tail (lossy, but loud).
        // Safe from cross-test env races: this is the only test touching this var.
        unsafe { std::env::set_var("NANOBPMN_READ_MODEL_LOSSY_REBUILD", "1") };
        let rebuilt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            catch_up_shard(&store, recovery.first_index, &surviving, None);
        }));
        unsafe { std::env::remove_var("NANOBPMN_READ_MODEL_LOSSY_REBUILD") };
        assert!(rebuilt.is_ok(), "the escape hatch must rebuild, not abort");
        assert_eq!(
            store.exported_position() as u64,
            recovery.total_events,
            "lossy rebuild must leave the cursor at the ABSOLUTE tail end \
             (floor + surviving.len() == total_events), not a relative surviving.len()"
        );
        // Regression guard for the re-abort loop: with the cursor now at the
        // absolute tail, a subsequent boot resumes cleanly instead of re-tripping
        // CompactedGap — even without the escape hatch set.
        assert_eq!(
            plan_catch_up(
                store.exported_position() as u64,
                recovery.first_index,
                surviving.len() as u64,
            ),
            CatchUpPlan::Resume {
                skip: surviving.len()
            },
            "the rebuilt cursor must resume, not re-abort, on the next boot"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Issue #732: a read model wiped below the journal compaction floor is
    /// REPROJECTED from the authoritative engine snapshot (the default when the
    /// boot engine state is available) — no panic, no lossy env flag — recovering
    /// every live entity, and lands the absolute cursor at `total_events`.
    #[test]
    fn wiped_read_model_reprojects_from_engine_snapshot() {
        use nanobpmn_engine_core::Command;

        use crate::readstore::ReadStore;

        let dir = temp_dir("readmodel-reproject-732");

        // Same setup as the #600 test: compact history into the snapshot so
        // `first_index > 0`, then leave a self-consistent post-compaction tail
        // holding a live instance (its service task => a live job).
        {
            let (mut journal, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            let shared = Arc::clone(&recovery.shared);
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let _ = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let (snap, covered) = journal.snapshot_and_rotate().expect("snapshot+rotate");
            write_snapshot(&dir, snap, covered).expect("write snapshot");
            assert_eq!(compact(&shared, covered), 1, "sealed prefix compacted away");
            // Post-compaction tail: a second live instance.
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let _ = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
        }

        // Reopen: the reconstructed engine (snapshot + surviving tail) is the
        // authoritative live state; the surviving tail sits above a non-zero floor.
        let (engine, recovery) =
            crate::journal::Journal::open_segmented(&dir).expect("reopen segmented");
        assert!(recovery.first_index > 0, "must reproduce a non-zero floor");
        let surviving: Vec<&Event> = recovery.events.iter().collect();
        let live_instances = engine.engine_state().instances.len();
        let live_jobs = engine.engine_state().jobs.len();
        assert!(
            live_instances >= 2 && live_jobs >= 2,
            "the engine must hold the live instances/jobs to reproject"
        );

        // A freshly wiped read model (exported_position == 0, below the floor).
        let store = ReadStore::open(None).expect("fresh in-memory read store");
        assert_eq!(store.exported_position(), 0);

        // Default path (no env flag): reproject from the engine snapshot. No panic.
        catch_up_shard(
            &store,
            recovery.first_index,
            &surviving,
            Some(engine.engine_state()),
        );

        assert_eq!(
            store.exported_position() as u64,
            recovery.total_events,
            "reprojection must plant the absolute cursor at total_events"
        );
        assert_eq!(
            store.process_instances().len(),
            live_instances,
            "every live process instance must be recovered from the snapshot"
        );
        assert_eq!(
            store.jobs().len(),
            live_jobs,
            "every live job must be recovered from the snapshot"
        );

        // Regression guard: the planted cursor resumes cleanly on the next boot.
        assert_eq!(
            plan_catch_up(
                store.exported_position() as u64,
                recovery.first_index,
                surviving.len() as u64,
            ),
            CatchUpPlan::Resume {
                skip: surviving.len()
            },
            "the reprojected cursor must resume, not re-abort, on the next boot"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn reprojection_restores_terminal_history_from_durable_archive() {
        // Issue #831 end-to-end at the reprojection entrypoint: a terminal instance
        // is archived durably when it completes, and a below-floor reprojection
        // (whose engine snapshot holds only live state) restores that completed
        // history from the archive rather than losing it (the merlin.local defect).
        use crate::readstore::ReadStore;

        let dir = temp_dir("readmodel-archive-reproject-831");
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let store_path = dir.join("read-model.sqlite");
        let store = ReadStore::open(Some(&store_path)).expect("file-backed read store");

        // Complete an instance so `export` writes it to the durable archive.
        let created = Event::ProcessInstanceCreated {
            instance_key: 999,
            process_id: "demo".to_string(),
            variables: std::collections::HashMap::new(),
            created_at: 0,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: 0,
            version: 0,
            parent_process_instance_key: None,
            parent_element_instance_key: None,
        };
        let done = Event::ProcessInstanceCompleted { instance_key: 999 };
        store
            .export(&[&created, &done])
            .expect("export terminal instance");
        assert_eq!(
            store.process_instance(999).map(|r| r.state),
            Some(nanobpmn_engine_core::ProcessInstanceState::Completed)
        );

        // Wipe the read model (stand-in for the fingerprint/upgrade wipe) so it
        // sits below the compaction floor, exactly the CompactedGap condition.
        store.reset().expect("wipe read model");
        assert!(store.process_instance(999).is_none());
        assert_eq!(store.exported_position(), 0);

        // The engine snapshot for the reprojection holds NO live instances (the
        // terminal one was long evicted), so only the archive can restore it.
        let empty_dir = temp_dir("readmodel-archive-reproject-831-engine");
        let (engine, _recovery) =
            crate::journal::Journal::open_segmented(&empty_dir).expect("open empty engine");
        assert!(engine.engine_state().instances.is_empty());

        // Drive the public reprojection entrypoint with a non-zero floor.
        catch_up_shard(&store, 100, &[], Some(engine.engine_state()));

        assert_eq!(
            store.process_instance(999).map(|r| r.state),
            Some(nanobpmn_engine_core::ProcessInstanceState::Completed),
            "terminal/completed history must be restored from the durable archive \
             during a below-floor reprojection (issue #831)"
        );

        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&empty_dir);
    }
}
