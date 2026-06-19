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
//! - `log.ndjson` — the log entries, one serialized [`Entry`] per line, in index
//!   order. Appended to (with `fsync`) on [`append`](RaftLogStorage::append);
//!   rewritten atomically on [`truncate`](RaftLogStorage::truncate) /
//!   [`purge`](RaftLogStorage::purge).
//! - `vote.json` — the persisted [`Vote`]. Rewritten atomically (temp + rename +
//!   dir-fsync) on every [`save_vote`](RaftLogStorage::save_vote), because Raft
//!   correctness requires a vote to be on disk before it is acted on.
//! - `state.json` — the `last_purged` and `committed` markers, rewritten
//!   atomically when either changes.
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
use std::sync::{Arc, Mutex};

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, StorageError, StorageIOError, Vote};
use serde::{Deserialize, Serialize};

use crate::raft::{NodeId, RaftConfig};

/// The durable markers persisted in `state.json`.
#[derive(Default, Serialize, Deserialize)]
struct PersistedState {
    last_purged: Option<LogId<NodeId>>,
    committed: Option<LogId<NodeId>>,
}

struct Inner {
    dir: PathBuf,
    /// Open append handle on `log.ndjson`. Replaced after an atomic rewrite.
    log_file: File,
    /// All present (non-purged) log entries, keyed by index, for fast reads.
    log: BTreeMap<u64, Entry<RaftConfig>>,
    last_purged: Option<LogId<NodeId>>,
    committed: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
}

/// A crash-durable Raft log store. Cloning shares the same underlying log (the
/// [`RaftLogReader`] handed to openraft is a clone), so all access is serialized
/// through a single [`Mutex`].
#[derive(Clone)]
pub struct RaftLogStore {
    inner: Arc<Mutex<Inner>>,
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

impl RaftLogStore {
    /// Opens (creating if absent) a durable log store rooted at `dir`, replaying
    /// any existing `log.ndjson`, `vote.json` and `state.json` to reconstruct the
    /// in-memory index, the persisted vote and the purge/commit markers.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Replay the entry log into the in-memory index.
        let mut log = BTreeMap::new();
        let lpath = log_path(&dir);
        if lpath.exists() {
            let reader = BufReader::new(File::open(&lpath)?);
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let entry: Entry<RaftConfig> = serde_json::from_str(&line)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                log.insert(entry.log_id.index, entry);
            }
        }

        let vote: Option<Vote<NodeId>> = read_json(&vote_path(&dir))?;
        let state: PersistedState = read_json(&state_path(&dir))?.unwrap_or_default();

        // Keep an append handle open for the common write path.
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&lpath)?;

        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                dir,
                log_file,
                log,
                last_purged: state.last_purged,
                committed: state.committed,
                vote,
            })),
        })
    }

    /// Rewrites `log.ndjson` from the in-memory index atomically and reopens the
    /// append handle. Used by `truncate`/`purge`, which remove entries that pure
    /// append cannot express.
    fn rewrite_log(inner: &mut Inner) -> Result<(), StorageError<NodeId>> {
        let mut bytes = Vec::new();
        for entry in inner.log.values() {
            serde_json::to_writer(&mut bytes, entry).map_err(io_err)?;
            bytes.push(b'\n');
        }
        let lpath = log_path(&inner.dir);
        atomic_write(&inner.dir, &lpath, &bytes).map_err(io_err)?;
        inner.log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&lpath)
            .map_err(io_err)?;
        Ok(())
    }

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
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
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
            .map(|e| e.log_id)
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
        let mut bytes = Vec::new();
        let mut staged = Vec::new();
        for entry in entries {
            serde_json::to_writer(&mut bytes, &entry).map_err(io_err)?;
            bytes.push(b'\n');
            staged.push(entry);
        }
        // Persist to disk before acknowledging: append the serialized lines and
        // fsync, then publish to the in-memory index and signal completion.
        inner.log_file.write_all(&bytes).map_err(io_err)?;
        inner.log_file.sync_all().map_err(io_err)?;
        for entry in staged {
            inner.log.insert(entry.log_id.index, entry);
        }
        drop(inner);
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Remove everything from `log_id.index` onward (inclusive), then persist.
        let mut inner = self.inner.lock().unwrap();
        let _removed = inner.log.split_off(&log_id.index);
        Self::rewrite_log(&mut inner)
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Drop everything up to and including `log_id.index`, keep the rest.
        let mut inner = self.inner.lock().unwrap();
        inner.last_purged = Some(log_id);
        inner.log = inner.log.split_off(&(log_id.index + 1));
        Self::rewrite_log(&mut inner)?;
        Self::persist_state(&inner)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().unwrap();
        inner.committed = committed;
        Self::persist_state(&inner)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().unwrap().committed)
    }
}
