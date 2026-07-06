//! Per-partition Raft (stage 3): every partition is a Raft group with a movable
//! leader; a command commits when a quorum of the replica set has the log entry,
//! then applies to the engine. This module wires [openraft] over our existing
//! durable [`Journal`] (the state machine) and — in a later milestone — the
//! Falcon protocol (the network).
//!
//! # Milestone status (RF=1, single voter)
//!
//! This first slice proves the integration end to end for a single-node Raft
//! group: a [`ReplicatedCommand`] flows through `client_write` → the replicated
//! log → [`RaftStateMachine::apply`], which applies it to the partition's
//! [`Journal`] and awaits its durable [`Commit`]. With one voter the quorum is
//! itself, so commit is immediate and the network layer is never exercised.
//!
//! Deliberately **additive**: this does not yet replace the server's
//! [`DeepthiHandle`](crate::deepthi::DeepthiHandle) write path.
//!
//! # Milestone B: crash-durable log (done)
//!
//! [`bootstrap_single_durable`](RaftPartition::bootstrap_single_durable) backs the
//! Raft log with [`RaftLogStore`](crate::raft_logstore::RaftLogStore), an
//! `fsync`-on-append disk log. By the Raft model the **log is the source of
//! truth**: a client-acked command is durable once it is in that log, and a
//! restart replays the durable log back through the (volatile) state machine to
//! reconstruct engine state — so the engine [`Journal`] itself can stay
//! in-memory. [`bootstrap_single`](RaftPartition::bootstrap_single) keeps the
//! original in-memory [`MemLogStore`] for tests that don't need durability.
//!
//! # Milestone C: multi-voter network (done)
//!
//! [`bootstrap_member`](RaftPartition::bootstrap_member) +
//! [`initialize`](RaftPartition::initialize) form an RF>1 group whose replicas
//! exchange AppendEntries/Vote/InstallSnapshot through a
//! [`RaftTransport`](crate::raft_net::RaftTransport) (see [`crate::raft_net`]).
//! The transport is pluggable: the in-process
//! [`LocalCluster`](crate::raft_net::LocalCluster) proves replication + commit
//! across a real 3-voter group, and a falcon-backed carrier mounts the
//! same network onto the cluster WebSocket once the server hosts the Raft groups.
//!
//! # Remaining
//!
//! Leader routing: host the Raft groups in the server, carry the
//! [`RaftTransport`](crate::raft_net::RaftTransport) over the Falcon protocol, and
//! route client writes to the partition leader — replacing the additive
//! [`DeepthiHandle`](crate::deepthi::DeepthiHandle) write path.

// This Raft subsystem (raft / raft_logstore / raft_net) is built up across
// stage-3 milestones and is deliberately *additive*: it is fully exercised by
// its own unit tests but not yet mounted on the server's production write path
// (that lands with leader routing). Until then, several public items are unused
// in a plain `cargo build`, so dead-code is allowed at the module level.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use nanobpmn_engine_core::{Command, Event};
use openraft::storage::{
    LogFlushed, LogState, RaftLogReader, RaftLogStorage, RaftStateMachine, Snapshot,
};
use openraft::{
    BasicNode, Config, Entry, EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};

use crate::deepthi::DeepthiHandle;
use crate::journal::Journal;
use crate::raft_net::{NullTransport, PartitionNetwork, RaftTransport};

/// Raft node id. We key the cluster by the topology's `node_id` (a `u32`),
/// widened to openraft's expected `u64`.
pub type NodeId = u64;

openraft::declare_raft_types!(
    /// The Raft type configuration for a nanobpmn partition group.
    pub RaftConfig:
        D = ReplicatedBatch,
        R = ReplicatedResponse,
        SnapshotData = SnapshotFile,
);

/// The unit replicated through the Raft log: an engine [`Command`] plus the
/// wall-clock `now` the leader stamped it with. Carrying `now` keeps `apply`
/// deterministic across replicas (the engine's time-dependent logic replays
/// identically), so re-applying the log on any replica yields identical state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicatedCommand {
    pub command: Command,
    pub now: u64,
}

/// A **batch** of commands replicated as a single Raft log entry. Coalescing
/// many concurrently-proposed commands into one entry amortizes openraft's
/// per-entry overhead (one append + one replication round-trip + one apply
/// round-trip + one engine-actor hop for the whole batch) across all of them —
/// the dominant write-path cost under load. A batch of one (the default for a
/// lone proposer, e.g. tests or deploy) is byte-for-byte the prior behavior.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicatedBatch {
    pub items: Vec<ReplicatedCommand>,
}

impl ReplicatedBatch {
    /// A single-command batch (the convenience path for `propose`).
    pub fn single(command: Command, now: u64) -> Self {
        Self {
            items: vec![ReplicatedCommand { command, now }],
        }
    }
}

/// The per-command outcome within a committed batch: the events the command
/// produced (so the caller can drive read-model export, routing and completion
/// exactly as the direct engine path does), or — when the engine *rejected* the
/// command (e.g. a complete on a non-existent job) — the mapped `(http_status,
/// message)`. A rejected command is still consumed on every replica (as a no-op)
/// so replicas stay in lockstep; only the leader surfaces the rejection.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplicatedItem {
    pub events: Vec<Event>,
    #[serde(default)]
    pub error: Option<(u16, String)>,
}

/// The result handed back to the `client_write` caller on the leader: one
/// [`ReplicatedItem`] per command in the proposed batch, in submission order.
/// Only meaningful on the applying leader; followers discard it.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplicatedResponse {
    pub items: Vec<ReplicatedItem>,
}

/// Maps an engine rejection to the `(http_status, message)` the client sees,
/// matching the direct (non-Raft) write path's status codes.
fn engine_error_status(e: &nanobpmn_engine_core::EngineError) -> (u16, String) {
    use nanobpmn_engine_core::EngineError as E;
    match e {
        E::ProcessNotFound { process_id } => {
            (400, format!("No deployed process with id '{process_id}'."))
        }
        E::JobNotFound { job_key } => (404, format!("No job with key {job_key}.")),
        E::JobNotActive { job_key } => (409, format!("Job {job_key} is not active.")),
        E::JobNotActivated { job_key } => (409, format!("Job {job_key} has not been activated.")),
        other => (500, other.to_string()),
    }
}

/// In-memory Raft log store (v2 `RaftLogStorage`). Holds the log entries, the
/// persisted vote, and the committed marker in memory.
///
/// NOTE (milestone A): in-memory means the *replicated log* is volatile — only
/// the applied engine state is durable (via the state machine's `Commit`). A
/// later milestone swaps this for a `Journal`-backed store so the log itself is
/// crash-durable; the trait seam here is exactly that swap point.
#[derive(Clone, Default)]
pub struct MemLogStore {
    inner: Arc<Mutex<MemLogInner>>,
}

#[derive(Default)]
struct MemLogInner {
    log: BTreeMap<u64, Entry<RaftConfig>>,
    last_purged: Option<LogId<NodeId>>,
    committed: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
}

impl RaftLogReader<RaftConfig> for MemLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<RaftConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<RaftConfig> for MemLogStore {
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
        {
            let mut inner = self.inner.lock().unwrap();
            for entry in entries {
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        // In-memory: the write is immediately "durable", so report completion now.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Remove everything from `log_id.index` onward (inclusive).
        let mut inner = self.inner.lock().unwrap();
        let _removed = inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Drop everything up to and including `log_id.index`, keep the rest.
        let mut inner = self.inner.lock().unwrap();
        inner.last_purged = Some(log_id);
        inner.log = inner.log.split_off(&(log_id.index + 1));
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().unwrap().committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().unwrap().committed)
    }
}

/// File-backed [`SnapshotData`](RaftConfig::SnapshotData) so a partition snapshot
/// is serialized to / streamed from disk instead of being materialized as a
/// `Cursor<Vec<u8>>` in RAM. This keeps snapshot build, cache and transfer memory
/// bounded (a handful of chunk buffers) rather than holding a full multi-gigabyte
/// copy of every resident variable — *twice*, once for the returned reader and
/// once for the cached `current_snapshot` — per partition. That eager double copy
/// (`serde_json::to_vec` + `data.clone()`) was the "fat snapshot" the lean design
/// removes. The `path` rides along with the tokio [`File`](tokio::fs::File) so the
/// state machine can persist / reopen the exact file openraft hands back through
/// [`install_snapshot`](RaftStateMachine::install_snapshot).
///
/// All three async traits simply delegate to the inner file, which is `Unpin`, so
/// the wrapper is `Unpin` too and can be pin-projected with [`Pin::new`].
pub struct SnapshotFile {
    file: tokio::fs::File,
    path: PathBuf,
}

impl tokio::io::AsyncRead for SnapshotFile {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().file).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for SnapshotFile {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().file).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().file).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().file).poll_shutdown(cx)
    }
}

impl tokio::io::AsyncSeek for SnapshotFile {
    fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        Pin::new(&mut self.get_mut().file).start_seek(position)
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Pin::new(&mut self.get_mut().file).poll_complete(cx)
    }
}

/// A persisted snapshot: the metadata plus the on-disk path of the serialized
/// [`EngineSnapshot`] that reconstructs the engine directly (state-based, not
/// event-replay — its size tracks the live working set rather than growing with
/// every command ever applied, so the Raft log can be compacted without unbounded
/// memory growth). The body lives on disk (not a cached `Vec<u8>`) so holding the
/// current snapshot for the follower catch-up path costs a path, not a full copy
/// of the state in RAM.
#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    path: PathBuf,
}

/// Metadata held by the Raft state machine: the last applied log id and
/// membership. The materialized engine state itself lives on the partition's
/// [`DeepthiHandle`] (driven by [`apply`](RaftStateMachine::apply)) and is
/// captured on demand for snapshots, so the state machine retains no event
/// history of its own.
struct SmMeta {
    partition_id: u64,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
}

/// The Raft state machine for one partition. Committed commands are applied to
/// the partition's [`DeepthiHandle`] — the *same* single-writer engine actor the
/// rest of the server reads, dispatches jobs from, and runs timers on — so the
/// replicated log and the served state share one materialized copy. Wrapped in
/// an `Arc` so openraft can share it with the snapshot builder.
pub struct PartitionStateMachine {
    /// The partition's engine actor: `apply` forwards each committed command to
    /// it. Held outside the metadata `Mutex` so `apply` can `.await` the engine
    /// round-trip without holding a std lock across the await point.
    engine: DeepthiHandle,
    inner: Mutex<SmMeta>,
    snapshot_idx: AtomicU64,
    /// Monotonic sequence for uniquely naming in-flight received snapshot files
    /// (one partition can receive successive snapshots over its lifetime).
    recv_idx: AtomicU64,
    current_snapshot: Mutex<Option<StoredSnapshot>>,
    /// Directory holding this partition's snapshot files (both the current cached
    /// snapshot and transient incoming ones). Created on construction.
    snapshot_dir: PathBuf,
}

impl PartitionStateMachine {
    fn new(
        engine: DeepthiHandle,
        partition_id: u64,
        snapshot_dir: PathBuf,
    ) -> std::io::Result<Self> {
        std::fs::create_dir_all(&snapshot_dir)?;
        // Clear any stale snapshot files left by a previous process: on boot the
        // engine state is reconstructed by replaying the durable log (or a fresh
        // network install), and `current_snapshot` starts empty, so any file on
        // disk here is dead. Removing it also reconciles a crash mid-install that
        // left an orphan `incoming-*` file.
        if let Ok(entries) = std::fs::read_dir(&snapshot_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let ours = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("snap-") || n.starts_with("incoming-"))
                    .unwrap_or(false);
                if ours {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
        Ok(Self {
            engine,
            inner: Mutex::new(SmMeta {
                partition_id,
                last_applied: None,
                last_membership: StoredMembership::default(),
            }),
            snapshot_idx: AtomicU64::new(0),
            recv_idx: AtomicU64::new(0),
            current_snapshot: Mutex::new(None),
            snapshot_dir,
        })
    }

    /// A unique per-process, per-partition snapshot directory under the system
    /// temp dir, for in-memory deployments and tests that have no durable log dir
    /// to anchor snapshots to.
    fn temp_snapshot_dir(partition_id: u64) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "nanobpmn-raftsnap-{}-p{partition_id}-{nanos}",
            std::process::id()
        ))
    }

    /// [`new`](Self::new) with a fresh temp snapshot directory. Used by the
    /// in-memory (volatile-log) bootstraps and the unit tests.
    fn new_temp(engine: DeepthiHandle, partition_id: u64) -> std::io::Result<Self> {
        Self::new(engine, partition_id, Self::temp_snapshot_dir(partition_id))
    }
}

/// A point-in-time snapshot builder: holds an [`EngineSnapshot`] and metadata
/// captured atomically (relative to `apply`) when the state-machine worker minted
/// it, so [`build_snapshot`](RaftSnapshotBuilder::build_snapshot) only has to
/// serialize an already-consistent state — no engine round-trip, no race with a
/// concurrent apply.
pub struct PartitionSnapshotBuilder {
    sm: Arc<PartitionStateMachine>,
    captured: nanobpmn_engine_core::EngineSnapshot,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
}

impl RaftSnapshotBuilder<RaftConfig> for PartitionSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<RaftConfig>, StorageError<NodeId>> {
        let snapshot_idx = self.sm.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = self.last_applied {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{snapshot_idx}")
        };

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        // Stream the state capture straight to disk (bounded memory) rather than
        // building a full `Vec<u8>` plus a second cached clone. `serde_json` here
        // runs the same blocking serialize the old `to_vec` did — but into a
        // buffered writer, so the peak transient is one buffer, not the whole
        // serialized state twice.
        let path = self
            .sm
            .snapshot_dir
            .join(format!("snap-{snapshot_idx}.bin"));
        let file = std::fs::File::create(&path)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        let mut writer = std::io::BufWriter::new(file);
        serde_json::to_writer(&mut writer, &self.captured)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        let file = writer
            .into_inner()
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e.into_error()))?;
        // Durable enough to serve to a follower even across a crash: the log is
        // still the authoritative tier, but a torn snapshot must never be shipped.
        file.sync_all()
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;

        // Publish as the current snapshot and unlink the file it replaces.
        let previous = self
            .sm
            .current_snapshot
            .lock()
            .unwrap()
            .replace(StoredSnapshot {
                meta: meta.clone(),
                path: path.clone(),
            });
        if let Some(previous) = previous.filter(|p| p.path != path) {
            let _ = std::fs::remove_file(&previous.path);
        }

        let tokio_file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(SnapshotFile {
                file: tokio_file,
                path,
            }),
        })
    }
}

impl RaftStateMachine<RaftConfig> for Arc<PartitionStateMachine> {
    type SnapshotBuilder = PartitionSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let inner = self.inner.lock().unwrap();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<ReplicatedResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<RaftConfig>> + Send,
    {
        let mut responses = Vec::new();
        // Each Normal entry is a BATCH of commands. We apply the whole batch in a
        // single engine-actor round-trip (the actor runs them in submission order),
        // collecting each command's events + durable-commit barrier, then await all
        // the barriers together — so the journal's group-commit writer coalesces the
        // batch into one write+fsync instead of one fsync per command. We never hold
        // the std::Mutex across an engine `.await`.
        for entry in entries {
            let log_id = entry.log_id;
            match entry.payload {
                EntryPayload::Blank => {
                    self.inner.lock().unwrap().last_applied = Some(log_id);
                    responses.push(ReplicatedResponse::default());
                }
                EntryPayload::Normal(batch) => {
                    // Phase 1: apply every command in the batch in ONE actor hop,
                    // returning per-command (events, commit) or the engine rejection.
                    type ApplyOutcome = Result<
                        (Arc<Vec<Event>>, crate::journal::Commit),
                        nanobpmn_engine_core::EngineError,
                    >;
                    let outcomes: Vec<ApplyOutcome> = self
                        .engine
                        .with(move |journal| {
                            batch
                                .items
                                .into_iter()
                                .map(|ReplicatedCommand { command, now }| {
                                    journal.apply_command_at(command, now)
                                })
                                .collect()
                        })
                        .await;

                    // Phase 2: await the durable barriers (now coalesced by the
                    // group-commit writer) and build the per-command responses.
                    let mut items = Vec::with_capacity(outcomes.len());
                    for outcome in outcomes {
                        match outcome {
                            Ok((events, commit)) => {
                                commit.wait().await;
                                items.push(ReplicatedItem {
                                    events: events.to_vec(),
                                    error: None,
                                });
                            }
                            // A rejected command produced no events; the log entry is
                            // still consumed so every replica stays in lockstep. The
                            // leader surfaces the mapped rejection to its client.
                            Err(e) => {
                                items.push(ReplicatedItem {
                                    events: Vec::new(),
                                    error: Some(engine_error_status(&e)),
                                });
                            }
                        }
                    }
                    self.inner.lock().unwrap().last_applied = Some(log_id);
                    responses.push(ReplicatedResponse { items });
                }
                EntryPayload::Membership(mem) => {
                    let mut inner = self.inner.lock().unwrap();
                    inner.last_applied = Some(log_id);
                    inner.last_membership = StoredMembership::new(Some(log_id), mem);
                    responses.push(ReplicatedResponse::default());
                }
            }
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // Capture the engine's materialized state and the applied metadata as a
        // consistent pair. This runs on the state-machine worker, which drives
        // `apply` and snapshot building sequentially, so no command is applied
        // between the engine read and the `last_applied`/membership read — the
        // captured state corresponds exactly to `last_applied`.
        let captured = self.engine.with(|journal| journal.engine_snapshot()).await;
        let (last_applied, last_membership) = {
            let inner = self.inner.lock().unwrap();
            (inner.last_applied, inner.last_membership.clone())
        };
        PartitionSnapshotBuilder {
            sm: self.clone(),
            captured,
            last_applied,
            last_membership,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<SnapshotFile>, StorageError<NodeId>> {
        // A fresh, empty on-disk file that openraft streams the incoming snapshot
        // chunks into (AsyncWrite + AsyncSeek), so the receiving side never buffers
        // the whole snapshot in RAM either.
        let seq = self.recv_idx.fetch_add(1, Ordering::Relaxed);
        let path = self
            .snapshot_dir
            .join(format!("incoming-{}-{seq}.tmp", std::process::id()));
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .await
            .map_err(|e| StorageIOError::write_snapshot(None, &e))?;
        Ok(Box::new(SnapshotFile { file, path }))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<SnapshotFile>,
    ) -> Result<(), StorageError<NodeId>> {
        let SnapshotFile { file, path } = *snapshot;

        // Stream-deserialize the received file from a blocking task (bounded
        // memory: a BufReader, not the whole body as a `Vec<u8>`). openraft leaves
        // the write cursor at the end, so rewind first.
        let std_file = file.into_std().await;
        let captured: nanobpmn_engine_core::EngineSnapshot = tokio::task::spawn_blocking(
            move || -> std::io::Result<nanobpmn_engine_core::EngineSnapshot> {
                use std::io::Seek;
                let mut f = std_file;
                f.seek(std::io::SeekFrom::Start(0))?;
                serde_json::from_reader(std::io::BufReader::new(f))
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            },
        )
        .await
        .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?
        .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        // Rebuild the engine actor's state directly from the captured snapshot.
        // The engine journal is in-memory under Raft (the Raft log is the durable
        // tier), so replacing it wholesale is the install.
        self.engine
            .with(move |journal| {
                *journal = Journal::in_memory_from_snapshot(captured);
            })
            .await;

        {
            let mut inner = self.inner.lock().unwrap();
            inner.last_applied = meta.last_log_id;
            inner.last_membership = meta.last_membership.clone();
        }

        // Promote the received file to the current snapshot (a rename within the
        // same dir — cheap, no re-serialize, no extra copy) and drop the old one.
        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let current_path = self
            .snapshot_dir
            .join(format!("snap-installed-{snapshot_idx}.bin"));
        std::fs::rename(&path, &current_path)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        let previous = self
            .current_snapshot
            .lock()
            .unwrap()
            .replace(StoredSnapshot {
                meta: meta.clone(),
                path: current_path,
            });
        if let Some(previous) = previous {
            let _ = std::fs::remove_file(&previous.path);
        }
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<RaftConfig>>, StorageError<NodeId>> {
        // Copy the small (meta, path) pair out from under the lock so the file
        // open can `.await` without holding the std mutex.
        let entry = {
            let guard = self.current_snapshot.lock().unwrap();
            guard.as_ref().map(|s| (s.meta.clone(), s.path.clone()))
        };
        match entry {
            Some((meta, path)) => {
                let file = tokio::fs::File::open(&path)
                    .await
                    .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
                Ok(Some(Snapshot {
                    meta,
                    snapshot: Box::new(SnapshotFile { file, path }),
                }))
            }
            None => Ok(None),
        }
    }
}

fn raft_env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// The per-partition snapshot cadence, in applied log entries, with a bounded
/// deterministic jitter so the partition replicas a single node hosts do not all
/// cross their snapshot threshold in the same instant.
///
/// Each partition's engine runs on its own single-threaded actor, and building a
/// snapshot briefly blocks that actor on an `O(working set)` `state.clone()` (the
/// serialize itself already runs off the actor, in `build_snapshot`). With a
/// uniform `LogsSinceLast(N)` every co-hosted partition reaches `N` at nearly the
/// same wall-clock time under steady load, so all of their engine actors stall
/// their creates/completes at once and aggregate throughput drops to a sharp
/// notch. Spreading the threshold by a per-partition-deterministic offset
/// staggers those clones so at most one or two partitions pause at a time — the
/// notch flattens into ripple.
///
/// The jitter is a percentage of the base (`NANOBPMN_RAFT_SNAPSHOT_JITTER_PCT`,
/// default 25, capped at 90; `0` disables it for an exact base, which keeps tests
/// that pin a small `NANOBPMN_RAFT_SNAPSHOT_LOGS` deterministic). It is centered
/// on the base, so the average snapshot frequency — and thus the memory/IO vs
/// log-length trade-off — is unchanged.
fn snapshot_logs_for_partition(partition_id: u64) -> u64 {
    let base = raft_env_u64("NANOBPMN_RAFT_SNAPSHOT_LOGS", 5000).max(1);
    let pct = raft_env_u64("NANOBPMN_RAFT_SNAPSHOT_JITTER_PCT", 25).min(90);
    jitter_snapshot_logs(base, pct, partition_id)
}

/// The pure, env-free core of [`snapshot_logs_for_partition`]: offset `base` by a
/// bounded, per-partition-deterministic amount within `± base * pct%`, centered on
/// `base`. `pct == 0` returns `base` unchanged. Split out so the jitter's
/// properties (bounded, centered, deterministic, well-spread) are unit-testable
/// without touching process-wide env.
fn jitter_snapshot_logs(base: u64, pct: u64, partition_id: u64) -> u64 {
    let base = base.max(1);
    let pct = pct.min(90);
    if pct == 0 {
        return base;
    }
    // ± this many entries around the base.
    let range = (base.saturating_mul(pct) / 100).max(1);
    // A Knuth multiplicative hash spreads consecutive partition ids evenly across
    // the whole [-range, +range] window, so neighbouring partitions (which a node
    // hosts as a contiguous block) land far apart rather than adjacent. The offset
    // arithmetic is done in i128 so it stays exact across the full u64 input
    // domain (a `base` near u64::MAX would overflow i64), then clamped back into
    // [1, u64::MAX].
    let span = range.saturating_mul(2).saturating_add(1);
    let hashed = partition_id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let offset = (hashed % span) as i128 - range as i128;
    (base as i128 + offset).clamp(1, u64::MAX as i128) as u64
}

/// The shared openraft tuning for a nanobpmn partition group: a brisk cadence so
/// elections settle quickly. All three timings are env-overridable for tuning
/// (read once at bootstrap, never in the hot path) — on a heavily contended box a
/// calmer cadence can avoid heartbeat-miss election churn, but the brisk defaults
/// are what the failover tests and the A/B benchmark are validated against. The
/// snapshot cadence is jittered per partition (see
/// [`snapshot_logs_for_partition`]) so co-hosted partitions do not snapshot in
/// lockstep.
fn raft_config(partition_id: u64) -> Config {
    Config {
        heartbeat_interval: raft_env_u64("NANOBPMN_RAFT_HEARTBEAT_MS", 250),
        election_timeout_min: raft_env_u64("NANOBPMN_RAFT_ELECTION_MIN_MS", 500),
        election_timeout_max: raft_env_u64("NANOBPMN_RAFT_ELECTION_MAX_MS", 1000),
        // Snapshot every N applied log entries to compact the log (openraft
        // default 5000). Env-tunable so a deployment can trade snapshot frequency
        // (memory/IO) against log length, and so tests can force the snapshot
        // build/install path with a small threshold.
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(snapshot_logs_for_partition(
            partition_id,
        )),
        ..Default::default()
    }
}

/// Upper bound on commands coalesced into a single Raft log entry. Caps per-entry
/// apply work and entry size; under a steady flood the batch fills toward this and
/// openraft's per-entry overhead is amortized across the whole batch.
const MAX_PROPOSE_BATCH: usize = 1024;

/// One queued command awaiting placement into a batched Raft entry, plus the
/// one-shot the batcher fulfils with that command's [`ReplicatedItem`] (or a
/// propose error) once the entry commits and applies.
struct Submission {
    item: ReplicatedCommand,
    resp: tokio::sync::oneshot::Sender<anyhow::Result<ReplicatedItem>>,
}

/// Command intake classification for the propose batcher's two-tier priority.
///
/// Returns `true` for fresh *demand entering* the system — process creation,
/// job **activation** (a poll, which Zeebe likewise does NOT whitelist), and
/// start-event instance dispatch. These take the low-priority lane. Everything
/// else — job/user-task finalization, cancellation, incident resolution, timer
/// and lock-expiry ticks, deploys, and message/signal correlation — is *progress
/// on already-admitted work* and takes the high lane.
///
/// Two properties make this safe in both directions, mirroring Zeebe's
/// `WhiteListedCommands`:
/// - **Drain can't be starved by intake:** completes never sit in the
///   Raft log behind a backlog of creates, so the cluster always frees the
///   resources of work it accepted (which reopens admission).
/// - **Intake can't be starved by drain:** the high lane's volume is bounded by
///   low-lane admission — you cannot complete/correlate more work than you
///   created — so a create can never be permanently starved. Crucially,
///   *activation* is intake, not drain: a flood of empty activation polls from
///   idle workers stays on the low lane and interleaves with creates FIFO
///   instead of monopolising the high lane and starving creation.
///
/// It matches the engine actor's High/Low mailbox (`deepthi::Priority`) one layer
/// down, so the two agree end to end.
fn is_creation_intake(command: &Command) -> bool {
    matches!(
        command,
        Command::CreateInstance { .. }
            | Command::ActivateJobs { .. }
            | Command::DispatchStartInstance { .. }
    )
}

/// Coalesces concurrently-proposed commands for one partition into batched Raft
/// log entries. A single background task drains every submission that queued
/// while the previous `client_write` was in flight into the next entry — classic
/// group commit: end-to-end latency stays one commit round-trip while throughput
/// scales with batch size, because one append + one replication round-trip + one
/// apply hop now carry up to [`MAX_PROPOSE_BATCH`] commands. A lone proposer
/// (tests, deploy) simply forms batches of one — byte-identical to the prior
/// one-command-per-entry path.
///
/// Two lanes give the drain path priority over creation intake (see
/// [`is_creation_intake`]): every batch is filled from the `hi` lane first, so
/// completes always ride the next entry even while a backlog of
/// creates waits in the `lo` lane. Creation is admitted only with the batch
/// capacity the drain path leaves — the log-layer analogue of Zeebe's
/// `WhiteListedCommands`, and the fix for the credit-starvation latch where a
/// create flood at the single FIFO starved job completion.
struct Batcher {
    hi_tx: tokio::sync::mpsc::UnboundedSender<Submission>,
    lo_tx: tokio::sync::mpsc::UnboundedSender<Submission>,
}

impl Batcher {
    fn spawn(raft: openraft::Raft<RaftConfig>) -> Self {
        let (hi_tx, mut hi_rx) = tokio::sync::mpsc::unbounded_channel::<Submission>();
        let (lo_tx, mut lo_rx) = tokio::sync::mpsc::unbounded_channel::<Submission>();
        tokio::spawn(async move {
            loop {
                // Block until at least one submission is queued on either lane.
                // `biased` polls the high-priority (drain) lane first, so when
                // both lanes have work waiting, the batch starts with drain
                // commands. `else` fires only once BOTH senders have dropped
                // (partition teardown), ending the task.
                let first = tokio::select! {
                    biased;
                    Some(s) = hi_rx.recv() => s,
                    Some(s) = lo_rx.recv() => s,
                    else => break,
                };
                let mut subs = vec![first];
                // Drain ALL pending high-priority (drain) commands into this
                // batch first, bounded by the cap — so a completion
                // never queues behind a backlog of creates in a later entry.
                while subs.len() < MAX_PROPOSE_BATCH {
                    match hi_rx.try_recv() {
                        Ok(s) => subs.push(s),
                        Err(_) => break,
                    }
                }
                // Fill any remaining batch capacity with low-priority creation
                // intake. Under a sustained drain flood creation yields entirely
                // (the intended backpressure); a completion can never outnumber
                // the creates that produced its jobs, so this is self-limiting and
                // does not permanently starve admission.
                while subs.len() < MAX_PROPOSE_BATCH {
                    match lo_rx.try_recv() {
                        Ok(s) => subs.push(s),
                        Err(_) => break,
                    }
                }
                let items: Vec<ReplicatedCommand> = subs.iter().map(|s| s.item.clone()).collect();
                let n = items.len();
                match raft.client_write(ReplicatedBatch { items }).await {
                    Ok(res) => {
                        let mut out = res.data.items;
                        if out.len() == n {
                            for (s, item) in subs.into_iter().zip(out.drain(..)) {
                                let _ = s.resp.send(Ok(item));
                            }
                        } else {
                            // apply returns exactly one item per command; an arity
                            // mismatch is a bug, surface it rather than mis-pair.
                            for s in subs {
                                let _ = s.resp.send(Err(anyhow::anyhow!(
                                    "raft batch response arity mismatch ({} != {n})",
                                    out.len()
                                )));
                            }
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        for s in subs {
                            let _ = s.resp.send(Err(anyhow::anyhow!("{msg}")));
                        }
                    }
                }
            }
        });
        Self { hi_tx, lo_tx }
    }

    async fn submit(&self, command: Command, now: u64) -> anyhow::Result<ReplicatedItem> {
        let (resp, rx) = tokio::sync::oneshot::channel();
        // Route fresh creation intake to the low-priority lane; the drain path
        // (completes, fails, ticks, admin) takes the high lane so it
        // is never queued behind a backlog of creates in the Raft log.
        let tx = if is_creation_intake(&command) {
            &self.lo_tx
        } else {
            &self.hi_tx
        };
        tx.send(Submission {
            item: ReplicatedCommand { command, now },
            resp,
        })
        .map_err(|_| anyhow::anyhow!("raft propose batcher stopped"))?;
        rx.await
            .map_err(|_| anyhow::anyhow!("raft propose batcher dropped the response"))?
    }
}

/// A Raft-managed partition: the openraft instance plus handles to its stores.
pub struct RaftPartition {
    pub raft: openraft::Raft<RaftConfig>,
    pub node_id: NodeId,
    pub partition_id: u64,
    batcher: Batcher,
}

impl RaftPartition {
    /// Boots a single-voter (RF=1) Raft group for `partition_id` on `node_id`,
    /// backed by `engine`, and initializes it so it elects itself leader. The
    /// returned partition is ready to accept [`propose`](Self::propose).
    pub async fn bootstrap_single(
        node_id: NodeId,
        partition_id: u64,
        addr: String,
        engine: DeepthiHandle,
    ) -> anyhow::Result<Self> {
        // RF=1 single voter: the brisk cadence lets the self-election complete
        // promptly (no peers means no real heartbeating), and the shared config
        // also carries the (jittered) snapshot policy so a solo replica compacts
        // its log on the same env-tunable cadence as a group member.
        let config = Arc::new(raft_config(partition_id).validate()?);

        let log_store = MemLogStore::default();
        let state_machine = Arc::new(PartitionStateMachine::new_temp(engine, partition_id)?);
        let network = PartitionNetwork::new(Arc::new(NullTransport), partition_id);
        let raft = openraft::Raft::new(node_id, config, network, log_store, state_machine).await?;

        let mut members = BTreeMap::new();
        members.insert(node_id, BasicNode::new(addr));
        raft.initialize(members).await?;

        let batcher = Batcher::spawn(raft.clone());
        Ok(Self {
            raft,
            node_id,
            partition_id,
            batcher,
        })
    }

    /// Boots a single-voter (RF=1) Raft group whose **log is crash-durable**,
    /// stored under `log_dir` (milestone B). Unlike [`bootstrap_single`], the
    /// replicated log survives a restart: reopening the same `log_dir` replays the
    /// durable entries back through the (volatile) state machine to reconstruct
    /// engine state. `initialize` is skipped when the log already exists, so this
    /// is the same call for a first boot and a recovery boot.
    pub async fn bootstrap_single_durable(
        node_id: NodeId,
        partition_id: u64,
        addr: String,
        engine: DeepthiHandle,
        log_dir: impl AsRef<std::path::Path>,
    ) -> anyhow::Result<Self> {
        let config = Arc::new(raft_config(partition_id).validate()?);

        let log_dir = log_dir.as_ref().to_path_buf();
        let log_store = crate::raft_logstore::RaftLogStore::open(&log_dir)?;
        let state_machine = Arc::new(PartitionStateMachine::new(
            engine,
            partition_id,
            log_dir.join("snapshots"),
        )?);
        let network = PartitionNetwork::new(Arc::new(NullTransport), partition_id);
        let raft = openraft::Raft::new(node_id, config, network, log_store, state_machine).await?;

        // A fresh log needs the one-shot membership bootstrap; a recovered log
        // already carries it, so initializing again would be an error.
        if !raft.is_initialized().await? {
            let mut members = BTreeMap::new();
            members.insert(node_id, BasicNode::new(addr));
            raft.initialize(members).await?;
        }

        let batcher = Batcher::spawn(raft.clone());
        Ok(Self {
            raft,
            node_id,
            partition_id,
            batcher,
        })
    }

    /// Boots one **voter** of a multi-node Raft group (RF>1, milestone C) over a
    /// shared [`RaftTransport`], without initializing membership. The caller boots
    /// every member, registers their handles with the transport, then calls
    /// [`initialize`](Self::initialize) once on a single member to form the group.
    /// Splitting construction from initialization is required because a voter must
    /// be able to *receive* AppendEntries/Vote before the group is formed.
    ///
    /// `log_dir` selects the log store: `Some(dir)` uses the crash-durable
    /// file-backed [`RaftLogStore`](crate::raft_logstore::RaftLogStore) (one
    /// directory per partition replica), so a voter — leader *or* follower —
    /// recovers its replicated log after a restart instead of losing everything
    /// it had replicated. `None` falls back to the volatile [`MemLogStore`], used
    /// by in-memory deployments and tests. Either way the log is compacted by
    /// snapshots (see [`raft_config`]'s `snapshot_policy`), so it does not grow
    /// without bound.
    pub async fn bootstrap_member(
        node_id: NodeId,
        partition_id: u64,
        engine: DeepthiHandle,
        transport: Arc<dyn RaftTransport>,
        log_dir: Option<std::path::PathBuf>,
    ) -> anyhow::Result<Self> {
        let config = Arc::new(raft_config(partition_id).validate()?);
        // Anchor snapshots next to the durable log when there is one, else a temp
        // dir for the volatile (in-memory-log) deployments.
        let snapshot_dir = match log_dir.as_ref() {
            Some(dir) => dir.join("snapshots"),
            None => PartitionStateMachine::temp_snapshot_dir(partition_id),
        };
        let state_machine = Arc::new(PartitionStateMachine::new(
            engine,
            partition_id,
            snapshot_dir,
        )?);
        let network = PartitionNetwork::new(transport, partition_id);
        // One `Raft` handle, two possible log stores. The handle erases the log
        // storage type, so both arms yield the same `RaftPartition`; building the
        // `Raft` inside each arm avoids needing a common concrete store type.
        let raft = match log_dir {
            Some(dir) => {
                let log_store = crate::raft_logstore::RaftLogStore::open(dir)?;
                openraft::Raft::new(node_id, config, network, log_store, state_machine).await?
            }
            None => {
                let log_store = MemLogStore::default();
                openraft::Raft::new(node_id, config, network, log_store, state_machine).await?
            }
        };
        let batcher = Batcher::spawn(raft.clone());
        Ok(Self {
            raft,
            node_id,
            partition_id,
            batcher,
        })
    }

    /// Forms the Raft group from `members` (node id → address). Call once, on one
    /// member, after every voter has been booted and registered with the shared
    /// transport. A no-op (skipped) if the group is already initialized.
    pub async fn initialize(&self, members: BTreeMap<NodeId, BasicNode>) -> anyhow::Result<()> {
        if !self.raft.is_initialized().await? {
            self.raft.initialize(members).await?;
        }
        Ok(())
    }

    /// Adds `node` as a **learner** (non-voting replica) of this group — the
    /// leader-durable path (ADR 0003). A learner receives the replicated log in the
    /// background but does NOT count toward the write quorum, so the leader (sole
    /// voter) acks without waiting for it. Idempotent in effect: re-adding an
    /// existing learner is a cheap no-op error we swallow. `blocking = false` so the
    /// call returns immediately rather than waiting for the learner to catch up —
    /// catch-up proceeds asynchronously, which is the whole point of the tier.
    pub async fn add_learner(&self, node_id: NodeId, node: BasicNode) -> anyhow::Result<()> {
        match self.raft.add_learner(node_id, node, false).await {
            Ok(_) => Ok(()),
            // Already a member (learner or voter): nothing to do.
            Err(e) if e.to_string().contains("already") => Ok(()),
            Err(e) => Err(anyhow::anyhow!("add_learner({node_id}): {e}")),
        }
    }

    /// Replicates `command` (stamped with `now`) through the Raft log and applies
    /// it once committed, returning the events it produced. At RF=1 this commits
    /// as soon as the local log write lands. Routed through the per-partition
    /// [`Batcher`], so a flood of concurrent proposes coalesces into batched
    /// entries; a lone proposer forms a batch of one.
    pub async fn propose(&self, command: Command, now: u64) -> anyhow::Result<Vec<Event>> {
        Ok(self.batcher.submit(command, now).await?.events)
    }

    /// Like [`propose`](Self::propose) but returns the full per-command
    /// [`ReplicatedItem`] so the caller can distinguish a successful apply
    /// (events) from an engine rejection (`error`). Used by the server write path
    /// to map 404/409 statuses through the Raft log.
    pub async fn propose_result(
        &self,
        command: Command,
        now: u64,
    ) -> anyhow::Result<ReplicatedItem> {
        self.batcher.submit(command, now).await
    }

    /// Whether this partition's Raft core has entered `Shutdown` — it has
    /// terminated (e.g. on an unrecoverable storage error) and no longer applies
    /// committed entries, so every instance/job routed here is stranded. A healthy
    /// partition is `Learner`/`Follower`/`Candidate`/`Leader`; only a dead one is
    /// `Shutdown`. Read on-demand from the openraft metrics watch (cheap borrow).
    pub fn is_shutdown(&self) -> bool {
        self.raft.metrics().borrow().state == openraft::ServerState::Shutdown
    }
}

/// The set of Raft groups this node hosts, keyed by partition id. A node hosts a
/// group for every partition it is a replica of; the falcon handler looks
/// up the target partition here to feed it an inbound RPC, and the write path
/// looks up the partition to propose through its leader. Empty by default — only
/// populated when per-partition Raft is enabled — so the non-Raft path is
/// untouched.
#[derive(Default)]
pub struct RaftRegistry {
    partitions: Mutex<HashMap<u64, Arc<RaftPartition>>>,
}

impl RaftRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Hosts `part`, keyed by its partition id.
    pub fn insert(&self, part: Arc<RaftPartition>) {
        self.partitions
            .lock()
            .unwrap()
            .insert(part.partition_id, part);
    }

    /// The hosted group for `partition`, if this node replicates it.
    pub fn get(&self, partition: u64) -> Option<Arc<RaftPartition>> {
        self.partitions.lock().unwrap().get(&partition).cloned()
    }

    /// Whether this node hosts no Raft groups (the non-Raft default).
    pub fn is_empty(&self) -> bool {
        self.partitions.lock().unwrap().is_empty()
    }

    /// A snapshot of every hosted partition, ordered by partition id. Used by the
    /// `/debug/raft` diagnostic endpoint to dump per-partition replication indices.
    pub fn all(&self) -> Vec<Arc<RaftPartition>> {
        let mut parts: Vec<Arc<RaftPartition>> =
            self.partitions.lock().unwrap().values().cloned().collect();
        parts.sort_by_key(|p| p.partition_id);
        parts
    }
}

#[cfg(test)]
mod tests {
    use nanobpmn_engine_core::ProcessBuilder;

    use super::*;

    fn deploy_command() -> Command {
        let proc = ProcessBuilder::new("p")
            .start_event("s")
            .end_event("e")
            .connect("s", "e")
            .build()
            .expect("valid process");
        Command::DeployProcess(proc)
    }

    #[test]
    fn snapshot_jitter_is_bounded_centered_and_desynchronizes_partitions() {
        let base = 5000u64;
        let pct = 25u64;
        let range = base * pct / 100; // ±1250

        // Bounded: every partition stays within ± range of the base.
        let vals: Vec<u64> = (0..12)
            .map(|p| jitter_snapshot_logs(base, pct, p))
            .collect();
        for (p, &v) in vals.iter().enumerate() {
            assert!(
                v >= base - range && v <= base + range,
                "partition {p} jittered to {v}, outside [{}, {}]",
                base - range,
                base + range
            );
        }

        // Deterministic: same inputs → same output.
        assert_eq!(vals[3], jitter_snapshot_logs(base, pct, 3));

        // Desynchronizes: the 12 co-hosted partitions do not all share one
        // threshold — the whole point of the jitter. Expect a wide spread.
        let mut sorted = vals.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert!(
            sorted.len() >= 10,
            "expected the 12 partitions to spread across distinct thresholds, got {sorted:?}"
        );

        // Roughly centered: the mean offset should be near zero, not skewed to
        // one side (which would defeat the "average cadence unchanged" property).
        let sum: i64 = vals.iter().map(|&v| v as i64 - base as i64).sum();
        let mean = sum / vals.len() as i64;
        assert!(
            mean.abs() < range as i64 / 2,
            "jitter mean {mean} too skewed"
        );

        // pct == 0 disables jitter for an exact, test-pinnable base.
        for p in 0..12 {
            assert_eq!(jitter_snapshot_logs(base, 0, p), base);
        }

        // Never returns 0 even with an absurdly small base (openraft would reject
        // a zero snapshot threshold).
        assert!(jitter_snapshot_logs(1, 90, 7) >= 1);
    }

    #[test]
    fn creation_and_activation_are_low_priority_intake() {
        use std::collections::HashMap;
        // Fresh demand entering the system — process creation AND job activation
        // (a poll) — takes the low-priority lane. Putting activation on the high
        // lane lets a flood of empty polls from idle workers starve creation,
        // which is exactly what Zeebe avoids by NOT whitelisting JobBatch.ACTIVATE.
        assert!(is_creation_intake(&Command::CreateInstance {
            process_id: "p".into(),
            variables: HashMap::new(),
            tags: vec![],
            business_id: None,
        }));
        assert!(is_creation_intake(&Command::activate_jobs(
            "t", "w", 1, 1, 0
        )));

        // The drain / progress path (finalization, cancellation, maintenance)
        // takes the high-priority lane; its volume is bounded by low-lane
        // admission, so it can never permanently starve a create.
        assert!(!is_creation_intake(&Command::complete_job_with(
            1,
            HashMap::new()
        )));
        assert!(!is_creation_intake(&Command::fail_job(1, 0, "e")));
        assert!(!is_creation_intake(&Command::ExpireJobs { now: 0 }));
        assert!(!is_creation_intake(&Command::TriggerTimers { now: 0 }));
        assert!(!is_creation_intake(&Command::CancelInstance {
            instance_key: 1
        }));
        assert!(!is_creation_intake(&deploy_command()));
    }

    #[tokio::test]
    async fn single_voter_replicates_and_applies_a_command() {
        let part = RaftPartition::bootstrap_single(
            0,
            0,
            "http://self".into(),
            DeepthiHandle::spawn(Journal::in_memory_partition(0), None),
        )
        .await
        .expect("bootstrap single-voter raft");

        // Deploy through the Raft log, then create an instance through it.
        let deploy_events = part.propose(deploy_command(), 1_000).await.expect("deploy");
        assert!(
            deploy_events
                .iter()
                .any(|e| matches!(e, Event::ProcessDeployed { .. })),
            "the deploy command replicated and applied (got {deploy_events:?})"
        );

        let create_events = part
            .propose(
                Command::CreateInstance {
                    process_id: "p".into(),
                    variables: Default::default(),
                    tags: Vec::new(),
                    business_id: None,
                },
                2_000,
            )
            .await
            .expect("create");
        assert!(
            create_events
                .iter()
                .any(|e| matches!(e, Event::ProcessInstanceCreated { .. })),
            "the create command replicated and applied (got {create_events:?})"
        );

        // The Raft metrics confirm this node is the leader of a committed log.
        let metrics = part.raft.metrics().borrow().clone();
        assert_eq!(metrics.current_leader, Some(0));
        assert!(
            metrics.last_applied.map(|l| l.index).unwrap_or(0) >= 2,
            "at least the deploy + create entries committed and applied"
        );

        part.raft.shutdown().await.expect("clean shutdown");
    }

    #[tokio::test]
    async fn snapshot_captures_compact_state_and_installs_into_a_fresh_replica() {
        // A source state machine accrues live state directly through its engine
        // actor (the same effect `apply` has), then snapshots it.
        let src = DeepthiHandle::spawn(Journal::in_memory_partition(0), None);
        src.with(|j| {
            let _ = j
                .apply_command_at(deploy_command(), 1)
                .expect("deploy applies");
        })
        .await;
        src.with(|j| {
            let _ = j
                .apply_command_at(
                    Command::CreateInstance {
                        process_id: "p".into(),
                        variables: Default::default(),
                        tags: Vec::new(),
                        business_id: None,
                    },
                    2,
                )
                .expect("create applies");
        })
        .await;

        let mut src_sm: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(src.clone(), 0).expect("snapshot dir"));
        let mut builder = src_sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.expect("build snapshot");

        // The body is a compact EngineSnapshot on disk, not an event log: reading
        // the file-backed snapshot back deserializes straight into an
        // EngineSnapshot.
        let mut reader = snap.snapshot;
        let mut bytes = Vec::new();
        {
            use tokio::io::AsyncReadExt;
            reader
                .read_to_end(&mut bytes)
                .await
                .expect("read snapshot body");
        }
        let _: nanobpmn_engine_core::EngineSnapshot =
            serde_json::from_slice(&bytes).expect("snapshot body is a state capture");

        // A brand-new, empty replica installs the snapshot and ends up with
        // byte-for-byte identical engine state — the cross-node catch-up path.
        // Drive the receive→write→install sequence openraft's chunked transfer
        // performs: begin a receiving file, stream the body in, then install.
        let dst = DeepthiHandle::spawn(Journal::in_memory_partition(0), None);
        let mut dst_sm: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(dst.clone(), 0).expect("snapshot dir"));
        let mut received = dst_sm
            .begin_receiving_snapshot()
            .await
            .expect("begin receiving snapshot");
        {
            use tokio::io::AsyncWriteExt;
            received
                .write_all(&bytes)
                .await
                .expect("write snapshot body");
            received.flush().await.expect("flush snapshot body");
        }
        dst_sm
            .install_snapshot(&snap.meta, received)
            .await
            .expect("install snapshot");

        let src_state = src.with(|j| j.state().clone()).await;
        let dst_state = dst.with(|j| j.state().clone()).await;
        assert_eq!(
            src_state, dst_state,
            "the installed replica's state matches the source exactly"
        );
        assert!(
            !dst_state.processes.is_empty(),
            "the deployed definition transferred in the snapshot"
        );
    }

    fn unique_log_dir(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "nanobpmn-raftlog-{}-{tag}-{nanos}",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn durable_log_survives_restart_and_replays() {
        let log_dir = unique_log_dir("restart");

        // Boot 1: deploy a process through a crash-durable Raft log, then stop the
        // node (simulating a crash) — the engine state machine is volatile, so all
        // that persists is the durable log under `log_dir`.
        {
            let part = RaftPartition::bootstrap_single_durable(
                0,
                0,
                "http://self".into(),
                DeepthiHandle::spawn(Journal::in_memory_partition(0), None),
                &log_dir,
            )
            .await
            .expect("bootstrap durable raft");

            let deploy_events = part.propose(deploy_command(), 1_000).await.expect("deploy");
            assert!(
                deploy_events
                    .iter()
                    .any(|e| matches!(e, Event::ProcessDeployed { .. })),
                "deploy applied on first boot (got {deploy_events:?})"
            );
            part.raft.shutdown().await.expect("clean shutdown");
        }

        // Boot 2: a brand-new, EMPTY engine + state machine reopens the same log
        // directory. If the durable log replays correctly, the previously deployed
        // process is known again — so creating an instance of it must succeed even
        // though nothing about the deploy lived in this process's memory.
        {
            let part = RaftPartition::bootstrap_single_durable(
                0,
                0,
                "http://self".into(),
                DeepthiHandle::spawn(Journal::in_memory_partition(0), None),
                &log_dir,
            )
            .await
            .expect("recover durable raft");

            let create_events = part
                .propose(
                    Command::CreateInstance {
                        process_id: "p".into(),
                        variables: Default::default(),
                        tags: Vec::new(),
                        business_id: None,
                    },
                    2_000,
                )
                .await
                .expect("create after recovery");
            assert!(
                create_events
                    .iter()
                    .any(|e| matches!(e, Event::ProcessInstanceCreated { .. })),
                "the deploy replayed from the durable log, so create succeeded \
                 after restart (got {create_events:?})"
            );
            part.raft.shutdown().await.expect("clean shutdown");
        }

        let _ = std::fs::remove_dir_all(&log_dir);
    }

    /// Polls `cond` until it holds or `timeout_ms` elapses.
    async fn wait_until(timeout_ms: u64, mut cond: impl FnMut() -> bool) -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        loop {
            if cond() {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn three_voters_replicate_and_commit() {
        use crate::raft_net::LocalCluster;

        // An in-process transport routes RPCs straight into the peers' Raft
        // instances, so this is a genuine 3-voter group (real elections, real
        // AppendEntries, real quorum), just without the network bytes.
        let cluster = LocalCluster::default();
        let transport: Arc<dyn RaftTransport> = Arc::new(cluster.clone());

        let mut parts = Vec::new();
        for id in 0u64..3 {
            let p = RaftPartition::bootstrap_member(
                id,
                0,
                DeepthiHandle::spawn(Journal::in_memory_partition(0), None),
                transport.clone(),
                None,
            )
            .await
            .expect("boot member");
            cluster.register(0, id, p.raft.clone());
            parts.push(p);
        }

        // Form the group once, then let node 0 win the initial election.
        let mut members = BTreeMap::new();
        for id in 0u64..3 {
            members.insert(id, BasicNode::new(format!("local-{id}")));
        }
        parts[0].initialize(members).await.expect("form group");
        assert!(
            wait_until(3_000, || parts[0].raft.metrics().borrow().current_leader
                == Some(0))
            .await,
            "node 0 should win the initial election"
        );

        // Propose on the leader: with RF=3 this commits only once a quorum (2 of
        // 3) has the entry, exercising the network end to end.
        let deploy_events = parts[0]
            .propose(deploy_command(), 1_000)
            .await
            .expect("deploy");
        assert!(
            deploy_events
                .iter()
                .any(|e| matches!(e, Event::ProcessDeployed { .. })),
            "the deploy committed via quorum and applied (got {deploy_events:?})"
        );

        let target = parts[0]
            .raft
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0);
        assert!(target >= 1, "leader applied at least the deploy entry");

        // Every follower converges to the same applied index — proof the entry
        // replicated to and applied on all three voters.
        for (id, p) in parts.iter().enumerate() {
            let raft = &p.raft;
            let applied = wait_until(3_000, || {
                raft.metrics()
                    .borrow()
                    .last_applied
                    .map(|l| l.index)
                    .unwrap_or(0)
                    >= target
            })
            .await;
            assert!(applied, "node {id} did not apply up to index {target}");
        }

        for p in parts {
            p.raft.shutdown().await.expect("clean shutdown");
        }
    }
}
