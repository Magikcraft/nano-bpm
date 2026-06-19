//! Per-partition Raft (stage 3): every partition is a Raft group with a movable
//! leader; a command commits when a quorum of the replica set has the log entry,
//! then applies to the engine. This module wires [openraft] over our existing
//! durable [`Journal`] (the state machine) and — in a later milestone — the
//! command stream (the network).
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
//! [`EngineHandle`](crate::engine_actor::EngineHandle) write path.
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
//! across a real 3-voter group, and a command-stream-backed carrier mounts the
//! same network onto the cluster WebSocket once the server hosts the Raft groups.
//!
//! # Remaining
//!
//! Leader routing: host the Raft groups in the server, carry the
//! [`RaftTransport`](crate::raft_net::RaftTransport) over the command stream, and
//! route client writes to the partition leader — replacing the additive
//! [`EngineHandle`](crate::engine_actor::EngineHandle) write path.

// This Raft subsystem (raft / raft_logstore / raft_net) is built up across
// stage-3 milestones and is deliberately *additive*: it is fully exercised by
// its own unit tests but not yet mounted on the server's production write path
// (that lands with leader routing). Until then, several public items are unused
// in a plain `cargo build`, so dead-code is allowed at the module level.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use nanobpmn_engine_core::{Command, Event};
use openraft::storage::{
    LogFlushed, LogState, RaftLogReader, RaftLogStorage, RaftStateMachine, Snapshot,
};
use openraft::{
    BasicNode, Config, Entry, EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};

use crate::engine_actor::EngineHandle;
use crate::journal::Journal;
use crate::raft_net::{NullTransport, PartitionNetwork, RaftTransport};

/// Raft node id. We key the cluster by the topology's `node_id` (a `u32`),
/// widened to openraft's expected `u64`.
pub type NodeId = u64;

openraft::declare_raft_types!(
    /// The Raft type configuration for a nanobpmn partition group.
    pub RaftConfig:
        D = ReplicatedCommand,
        R = ReplicatedResponse,
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

/// The result handed back to the `client_write` caller on the leader: the events
/// the command produced (so the caller can drive read-model export, routing and
/// completion exactly as the direct engine path does). Only meaningful on the
/// applying leader; followers discard it.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplicatedResponse {
    pub events: Vec<Event>,
    /// On the leader, set when the command was *rejected* by the engine (e.g. a
    /// complete on a non-existent job): the mapped `(http_status, message)`. The
    /// log entry is still consumed on every replica (as a no-op) so replicas stay
    /// in lockstep; only the leader surfaces the rejection to its client.
    #[serde(default)]
    pub error: Option<(u16, String)>,
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
        E::JobNotActivated { job_key } => {
            (409, format!("Job {job_key} has not been activated."))
        }
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

/// A persisted snapshot: the metadata plus the serialized event history that
/// reconstructs the engine via replay (we snapshot the event log, not the
/// in-memory `State`, because `Event` is `serde` and `Journal` already rebuilds
/// from events — the same mechanism as crash recovery).
#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

/// Metadata held by the Raft state machine: the last applied log id and
/// membership, plus the full applied-event history used to build snapshots. The
/// materialized engine state itself lives on the partition's [`EngineHandle`]
/// (driven by [`apply`](RaftStateMachine::apply)), not here.
struct SmMeta {
    partition_id: u64,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    /// Every event applied so far, in apply order — the replayable snapshot body.
    history: Vec<Event>,
}

/// The Raft state machine for one partition. Committed commands are applied to
/// the partition's [`EngineHandle`] — the *same* single-writer engine actor the
/// rest of the server reads, dispatches jobs from, and runs timers on — so the
/// replicated log and the served state share one materialized copy. Wrapped in
/// an `Arc` so openraft can share it with the snapshot builder.
pub struct PartitionStateMachine {
    /// The partition's engine actor: `apply` forwards each committed command to
    /// it. Held outside the metadata `Mutex` so `apply` can `.await` the engine
    /// round-trip without holding a std lock across the await point.
    engine: EngineHandle,
    inner: Mutex<SmMeta>,
    snapshot_idx: AtomicU64,
    current_snapshot: Mutex<Option<StoredSnapshot>>,
}

impl PartitionStateMachine {
    fn new(engine: EngineHandle, partition_id: u64) -> Self {
        Self {
            engine,
            inner: Mutex::new(SmMeta {
                partition_id,
                last_applied: None,
                last_membership: StoredMembership::default(),
                history: Vec::new(),
            }),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: Mutex::new(None),
        }
    }
}

impl RaftSnapshotBuilder<RaftConfig> for Arc<PartitionStateMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<RaftConfig>, StorageError<NodeId>> {
        let (data, last_applied, last_membership) = {
            let inner = self.inner.lock().unwrap();
            let data = serde_json::to_vec(&inner.history)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            (data, inner.last_applied, inner.last_membership.clone())
        };

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{snapshot_idx}")
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };
        *self.current_snapshot.lock().unwrap() = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<RaftConfig> for Arc<PartitionStateMachine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let inner = self.inner.lock().unwrap();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ReplicatedResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<RaftConfig>> + Send,
    {
        let mut responses = Vec::new();
        // Each Normal entry is forwarded to the partition's engine actor and its
        // durable commit awaited. We never hold the std::Mutex across the engine
        // `.await`: the metadata (last_applied/history) is updated only after the
        // engine round-trip returns.
        for entry in entries {
            let log_id = entry.log_id;
            match entry.payload {
                EntryPayload::Blank => {
                    self.inner.lock().unwrap().last_applied = Some(log_id);
                    responses.push(ReplicatedResponse::default());
                }
                EntryPayload::Normal(rc) => {
                    let ReplicatedCommand { command, now } = rc;
                    // Run the command on the single-writer engine actor (the same
                    // actor that serves reads/dispatch/timers) and await its
                    // durable commit before acking the apply.
                    let outcome = self
                        .engine
                        .with(move |journal| match journal.apply_command_at(command, now) {
                            Ok((events, commit)) => Ok((events, commit)),
                            Err(e) => Err(e),
                        })
                        .await;
                    match outcome {
                        Ok((events, commit)) => {
                            commit.wait().await;
                            {
                                let mut inner = self.inner.lock().unwrap();
                                inner.last_applied = Some(log_id);
                                inner.history.extend(events.iter().cloned());
                            }
                            responses.push(ReplicatedResponse {
                                events: events.to_vec(),
                                error: None,
                            });
                        }
                        // A rejected command is journaled as a no-op (it produced
                        // no events); the log entry is still consumed so every
                        // replica stays in lockstep. The leader surfaces the
                        // mapped rejection to its client via the response.
                        Err(e) => {
                            self.inner.lock().unwrap().last_applied = Some(log_id);
                            responses.push(ReplicatedResponse {
                                events: Vec::new(),
                                error: Some(engine_error_status(&e)),
                            });
                        }
                    }
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
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let history: Vec<Event> = serde_json::from_slice(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        let partition_id = self.inner.lock().unwrap().partition_id;
        // Rebuild the engine actor's state from the snapshot's event history. The
        // engine journal is in-memory under Raft (the Raft log is the durable
        // tier), so replacing it wholesale is the install.
        let hist = history.clone();
        self.engine
            .with(move |journal| {
                *journal = Journal::in_memory_from_events(partition_id, hist);
            })
            .await;

        {
            let mut inner = self.inner.lock().unwrap();
            inner.history = history;
            inner.last_applied = meta.last_log_id;
            inner.last_membership = meta.last_membership.clone();
        }

        *self.current_snapshot.lock().unwrap() = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<RaftConfig>>, StorageError<NodeId>> {
        Ok(self
            .current_snapshot
            .lock()
            .unwrap()
            .as_ref()
            .map(|s| Snapshot {
                meta: s.meta.clone(),
                snapshot: Box::new(Cursor::new(s.data.clone())),
            }))
    }
}

/// The shared openraft tuning for a nanobpmn partition group: a brisk cadence so
/// elections settle quickly. Returned unvalidated so the caller `?`s `validate`.
fn raft_config() -> Config {
    Config {
        heartbeat_interval: 250,
        election_timeout_min: 500,
        election_timeout_max: 1000,
        ..Default::default()
    }
}

/// A Raft-managed partition: the openraft instance plus handles to its stores.
pub struct RaftPartition {
    pub raft: openraft::Raft<RaftConfig>,
    pub node_id: NodeId,
    pub partition_id: u64,
}

impl RaftPartition {
    /// Boots a single-voter (RF=1) Raft group for `partition_id` on `node_id`,
    /// backed by `engine`, and initializes it so it elects itself leader. The
    /// returned partition is ready to accept [`propose`](Self::propose).
    pub async fn bootstrap_single(
        node_id: NodeId,
        partition_id: u64,
        addr: String,
        engine: EngineHandle,
    ) -> anyhow::Result<Self> {
        let config = Arc::new(
            Config {
                // RF=1 single voter: keep the cadence brisk so the self-election
                // completes promptly; no peers means no real heartbeating.
                heartbeat_interval: 250,
                election_timeout_min: 500,
                election_timeout_max: 1000,
                ..Default::default()
            }
            .validate()?,
        );

        let log_store = MemLogStore::default();
        let state_machine = Arc::new(PartitionStateMachine::new(engine, partition_id));
        let network = PartitionNetwork::new(Arc::new(NullTransport), partition_id);
        let raft = openraft::Raft::new(node_id, config, network, log_store, state_machine).await?;

        let mut members = BTreeMap::new();
        members.insert(node_id, BasicNode::new(addr));
        raft.initialize(members).await?;

        Ok(Self {
            raft,
            node_id,
            partition_id,
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
        engine: EngineHandle,
        log_dir: impl AsRef<std::path::Path>,
    ) -> anyhow::Result<Self> {
        let config = Arc::new(
            Config {
                heartbeat_interval: 250,
                election_timeout_min: 500,
                election_timeout_max: 1000,
                ..Default::default()
            }
            .validate()?,
        );

        let log_store = crate::raft_logstore::RaftLogStore::open(log_dir)?;
        let state_machine = Arc::new(PartitionStateMachine::new(engine, partition_id));
        let network = PartitionNetwork::new(Arc::new(NullTransport), partition_id);
        let raft = openraft::Raft::new(node_id, config, network, log_store, state_machine).await?;

        // A fresh log needs the one-shot membership bootstrap; a recovered log
        // already carries it, so initializing again would be an error.
        if !raft.is_initialized().await? {
            let mut members = BTreeMap::new();
            members.insert(node_id, BasicNode::new(addr));
            raft.initialize(members).await?;
        }

        Ok(Self {
            raft,
            node_id,
            partition_id,
        })
    }

    /// Boots one **voter** of a multi-node Raft group (RF>1, milestone C) over a
    /// shared [`RaftTransport`], without initializing membership. The caller boots
    /// every member, registers their handles with the transport, then calls
    /// [`initialize`](Self::initialize) once on a single member to form the group.
    /// Splitting construction from initialization is required because a voter must
    /// be able to *receive* AppendEntries/Vote before the group is formed.
    pub async fn bootstrap_member(
        node_id: NodeId,
        partition_id: u64,
        engine: EngineHandle,
        transport: Arc<dyn RaftTransport>,
    ) -> anyhow::Result<Self> {
        let config = Arc::new(raft_config().validate()?);
        let log_store = MemLogStore::default();
        let state_machine = Arc::new(PartitionStateMachine::new(engine, partition_id));
        let network = PartitionNetwork::new(transport, partition_id);
        let raft = openraft::Raft::new(node_id, config, network, log_store, state_machine).await?;
        Ok(Self {
            raft,
            node_id,
            partition_id,
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

    /// Replicates `command` (stamped with `now`) through the Raft log and applies
    /// it once committed, returning the events it produced. At RF=1 this commits
    /// as soon as the local log write lands.
    pub async fn propose(
        &self,
        command: Command,
        now: u64,
    ) -> anyhow::Result<Vec<Event>> {
        let res = self
            .raft
            .client_write(ReplicatedCommand { command, now })
            .await?;
        Ok(res.data.events)
    }

    /// Like [`propose`](Self::propose) but returns the full
    /// [`ReplicatedResponse`] so the caller can distinguish a successful apply
    /// (events) from an engine rejection (`error`). Used by the server write path
    /// to map 404/409 statuses through the Raft log.
    pub async fn propose_result(
        &self,
        command: Command,
        now: u64,
    ) -> anyhow::Result<ReplicatedResponse> {
        let res = self
            .raft
            .client_write(ReplicatedCommand { command, now })
            .await?;
        Ok(res.data)
    }
}

/// The set of Raft groups this node hosts, keyed by partition id. A node hosts a
/// group for every partition it is a replica of; the command-stream handler looks
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use nanobpmn_engine_core::ProcessBuilder;

    fn deploy_command() -> Command {
        let proc = ProcessBuilder::new("p")
            .start_event("s")
            .end_event("e")
            .connect("s", "e")
            .build()
            .expect("valid process");
        Command::DeployProcess(proc)
    }

    #[tokio::test]
    async fn single_voter_replicates_and_applies_a_command() {
        let part = RaftPartition::bootstrap_single(
            0,
            0,
            "http://self".into(),
            EngineHandle::spawn(Journal::in_memory_partition(0), None),
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
                EngineHandle::spawn(Journal::in_memory_partition(0), None),
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
                EngineHandle::spawn(Journal::in_memory_partition(0), None),
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
                EngineHandle::spawn(Journal::in_memory_partition(0), None),
                transport.clone(),
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
            wait_until(3_000, || parts[0].raft.metrics().borrow().current_leader == Some(0)).await,
            "node 0 should win the initial election"
        );

        // Propose on the leader: with RF=3 this commits only once a quorum (2 of
        // 3) has the entry, exercising the network end to end.
        let deploy_events = parts[0].propose(deploy_command(), 1_000).await.expect("deploy");
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
