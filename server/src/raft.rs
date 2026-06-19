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
//! [`EngineHandle`](crate::engine_actor::EngineHandle) write path. Two pieces
//! remain for the durable, multi-node story (separate milestones):
//!   1. Replace the in-memory [`MemLogStore`] log with a `RaftLogStorage`
//!      backed by our `Journal`, so the *replicated log itself* is crash-durable
//!      (today only the applied engine state is, via `Commit`; the memstore log
//!      is volatile — acceptable only because RF=1 single-voter never recovers
//!      from a peer).
//!   2. Implement the [`RaftNetwork`] over the command stream so followers
//!      receive AppendEntries/Vote, raising RF to 3 without touching routing.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use nanobpmn_engine_core::{Command, Event};
use openraft::error::{InstallSnapshotError, NetworkError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{
    LogFlushed, LogState, RaftLogReader, RaftLogStorage, RaftStateMachine, Snapshot,
};
use openraft::{
    BasicNode, Config, Entry, EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};

use crate::journal::Journal;

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
}

type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
type RPCError<E = openraft::error::Infallible> =
    openraft::error::RPCError<NodeId, BasicNode, RaftError<E>>;

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

/// State held by the Raft state machine: the partition's engine journal, the
/// last applied log id and membership, and the full applied-event history used
/// to build snapshots.
struct SmInner {
    journal: Journal,
    partition_id: u64,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    /// Every event applied so far, in apply order — the replayable snapshot body.
    history: Vec<Event>,
}

/// The Raft state machine for one partition, applying committed commands to a
/// [`Journal`]. Wrapped in an `Arc` so openraft can share it with the snapshot
/// builder.
pub struct PartitionStateMachine {
    inner: Mutex<SmInner>,
    snapshot_idx: AtomicU64,
    current_snapshot: Mutex<Option<StoredSnapshot>>,
}

impl PartitionStateMachine {
    fn new(journal: Journal, partition_id: u64) -> Self {
        Self {
            inner: Mutex::new(SmInner {
                journal,
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
        // Each iteration takes the lock for the synchronous engine apply only,
        // then releases it to await the durable commit — never holding the
        // std::Mutex across `.await`.
        for entry in entries {
            let log_id = entry.log_id;
            match entry.payload {
                EntryPayload::Blank => {
                    self.inner.lock().unwrap().last_applied = Some(log_id);
                    responses.push(ReplicatedResponse::default());
                }
                EntryPayload::Normal(rc) => {
                    let outcome = {
                        let mut inner = self.inner.lock().unwrap();
                        inner.last_applied = Some(log_id);
                        match inner.journal.apply_command_at(rc.command, rc.now) {
                            Ok((events, commit)) => {
                                inner.history.extend(events.iter().cloned());
                                Some((events, commit))
                            }
                            // A rejected command is journaled as a no-op (it
                            // produced no events); the log entry is still
                            // consumed so every replica stays in lockstep.
                            Err(_) => None,
                        }
                    };
                    match outcome {
                        Some((events, commit)) => {
                            commit.wait().await;
                            responses.push(ReplicatedResponse {
                                events: events.to_vec(),
                            });
                        }
                        None => responses.push(ReplicatedResponse::default()),
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

        let mut inner = self.inner.lock().unwrap();
        let partition_id = inner.partition_id;
        inner.journal = Journal::in_memory_from_events(partition_id, history.clone());
        inner.history = history;
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        drop(inner);

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

/// The Raft network factory. Stage-3 milestone A is single-voter (RF=1), so no
/// inter-node RPC is ever sent; every method reports the peer unreachable. A
/// later milestone implements these over the command stream to raise RF to 3.
#[derive(Clone)]
pub struct PartitionNetwork;

impl RaftNetworkFactory<RaftConfig> for PartitionNetwork {
    type Network = PartitionNetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        PartitionNetworkConnection {
            target,
            _node: node.clone(),
        }
    }
}

pub struct PartitionNetworkConnection {
    target: NodeId,
    _node: BasicNode,
}

impl PartitionNetworkConnection {
    fn unreachable<E: std::error::Error + 'static>(&self) -> RPCError<E> {
        RPCError::Unreachable(Unreachable::new(&NetworkError::new(
            &std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("raft network not wired yet (target node {})", self.target),
            ),
        )))
    }
}

impl RaftNetwork<RaftConfig> for PartitionNetworkConnection {
    async fn append_entries(
        &mut self,
        _req: AppendEntriesRequest<RaftConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError> {
        Err(self.unreachable())
    }

    async fn install_snapshot(
        &mut self,
        _req: InstallSnapshotRequest<RaftConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RPCError<InstallSnapshotError>> {
        Err(self.unreachable())
    }

    async fn vote(
        &mut self,
        _req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError> {
        Err(self.unreachable())
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
    /// backed by `journal`, and initializes it so it elects itself leader. The
    /// returned partition is ready to accept [`propose`](Self::propose).
    pub async fn bootstrap_single(
        node_id: NodeId,
        partition_id: u64,
        addr: String,
        journal: Journal,
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
        let state_machine = Arc::new(PartitionStateMachine::new(journal, partition_id));
        let raft = openraft::Raft::new(
            node_id,
            config,
            PartitionNetwork,
            log_store,
            state_machine,
        )
        .await?;

        let mut members = BTreeMap::new();
        members.insert(node_id, BasicNode::new(addr));
        raft.initialize(members).await?;

        Ok(Self {
            raft,
            node_id,
            partition_id,
        })
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
            Journal::in_memory_partition(0),
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
}
