//! The Raft network (stage 3 milestone C): a transport-abstracted
//! [`RaftNetwork`] that replaces the milestone-A `Unreachable` stub and lets a
//! partition's replicas exchange AppendEntries / Vote / InstallSnapshot, raising
//! the replication factor from 1 to 3.
//!
//! # Transport seam
//!
//! openraft's network is split into *what* to send (the typed RPCs) and *how* to
//! carry the bytes. This module owns the *what* — [`RaftRpcRequest`] /
//! [`RaftRpcResponse`] and the [`PartitionNetwork`] that serializes a call into
//! them — and delegates the *how* to a [`RaftTransport`]. That keeps the carrier
//! pluggable:
//!
//! - [`LocalCluster`] dispatches in-process (used by the multi-voter tests here),
//!   proving replication and commit across a real 3-voter group.
//! - A command-stream-backed transport (a node sending [`RaftRpcRequest`] frames
//!   to the peer that hosts the target replica) mounts the same `PartitionNetwork`
//!   onto the cluster's existing WebSocket protocol; that binding lands with
//!   leader routing, where the server actually hosts the Raft groups.
//!
//! The receiving side is symmetric: [`dispatch`] feeds an inbound
//! [`RaftRpcRequest`] into the local [`openraft::Raft`] and returns its
//! [`RaftRpcResponse`], whatever transport delivered it.

// Additive until leader routing mounts it; see `raft.rs` for the rationale.
#![allow(dead_code)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use openraft::error::{NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::{Deserialize, Serialize};

use crate::raft::{NodeId, RaftConfig};

/// A boxed, `Send` future — the return shape of the dyn-compatible
/// [`RaftTransport::send`].
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// One of the three Raft RPCs, serialized for transport. Carrying the typed
/// openraft requests (rather than opaque bytes) keeps the wire format
/// self-describing and lets any transport reuse the same serde.
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftRpcRequest {
    AppendEntries(AppendEntriesRequest<RaftConfig>),
    Vote(VoteRequest<NodeId>),
    InstallSnapshot(InstallSnapshotRequest<RaftConfig>),
}

/// The matching response to a [`RaftRpcRequest`].
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftRpcResponse {
    AppendEntries(AppendEntriesResponse<NodeId>),
    Vote(VoteResponse<NodeId>),
    InstallSnapshot(InstallSnapshotResponse<NodeId>),
}

/// A transport failure — the peer could not be reached or did not answer. Raft
/// treats this as retryable (it backs off and retries), so a transient peer
/// outage degrades to slower commit, never lost data.
#[derive(Debug)]
pub struct TransportError(pub String);

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "raft transport error: {}", self.0)
    }
}
impl std::error::Error for TransportError {}

/// Carries a partition's Raft RPCs to a target replica. The single method routes
/// `req` to the node hosting replica `target` of `partition` and returns its
/// answer. Implementations: [`LocalCluster`] (in-process) and, with leader
/// routing, a command-stream-backed carrier.
pub trait RaftTransport: Send + Sync + 'static {
    fn send<'a>(
        &'a self,
        target: NodeId,
        partition: u64,
        req: RaftRpcRequest,
    ) -> BoxFuture<'a, Result<RaftRpcResponse, TransportError>>;
}

/// Feeds an inbound RPC into a local Raft instance and returns its response. The
/// receiving half of any transport — the command-stream server handler will call
/// this with the partition's hosted Raft once leader routing mounts the groups.
pub async fn dispatch(
    raft: &openraft::Raft<RaftConfig>,
    req: RaftRpcRequest,
) -> anyhow::Result<RaftRpcResponse> {
    Ok(match req {
        RaftRpcRequest::AppendEntries(r) => {
            RaftRpcResponse::AppendEntries(raft.append_entries(r).await?)
        }
        RaftRpcRequest::Vote(r) => RaftRpcResponse::Vote(raft.vote(r).await?),
        RaftRpcRequest::InstallSnapshot(r) => {
            RaftRpcResponse::InstallSnapshot(raft.install_snapshot(r).await?)
        }
    })
}

/// The openraft [`RaftNetworkFactory`] for one partition: every connection it
/// mints carries that partition's RPCs over the shared [`RaftTransport`].
#[derive(Clone)]
pub struct PartitionNetwork {
    transport: Arc<dyn RaftTransport>,
    partition: u64,
}

impl PartitionNetwork {
    pub fn new(transport: Arc<dyn RaftTransport>, partition: u64) -> Self {
        Self {
            transport,
            partition,
        }
    }
}

impl RaftNetworkFactory<RaftConfig> for PartitionNetwork {
    type Network = PartitionConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        PartitionConnection {
            transport: self.transport.clone(),
            partition: self.partition,
            target,
        }
    }
}

/// A connection to one target replica of one partition.
pub struct PartitionConnection {
    transport: Arc<dyn RaftTransport>,
    partition: u64,
    target: NodeId,
}

impl PartitionConnection {
    /// Maps a transport failure to a retryable openraft [`Unreachable`] error.
    fn unreachable<E: std::error::Error + 'static>(&self, e: TransportError) -> RPCError<NodeId, BasicNode, RaftError<NodeId, E>> {
        RPCError::Unreachable(Unreachable::new(&NetworkError::new(&e)))
    }

    /// A response of the wrong RPC kind is a protocol bug; surface it as a
    /// retryable network error rather than panicking the raft core.
    fn mismatch<E: std::error::Error + 'static>(&self) -> RPCError<NodeId, BasicNode, RaftError<NodeId, E>> {
        let e = TransportError(format!(
            "raft transport returned a mismatched response kind (partition {}, target {})",
            self.partition, self.target
        ));
        RPCError::Unreachable(Unreachable::new(&NetworkError::new(&e)))
    }
}

impl RaftNetwork<RaftConfig> for PartitionConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<RaftConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let resp = self
            .transport
            .send(self.target, self.partition, RaftRpcRequest::AppendEntries(req))
            .await
            .map_err(|e| self.unreachable(e))?;
        match resp {
            RaftRpcResponse::AppendEntries(r) => Ok(r),
            _ => Err(self.mismatch()),
        }
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let resp = self
            .transport
            .send(self.target, self.partition, RaftRpcRequest::Vote(req))
            .await
            .map_err(|e| self.unreachable(e))?;
        match resp {
            RaftRpcResponse::Vote(r) => Ok(r),
            _ => Err(self.mismatch()),
        }
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<RaftConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        let resp = self
            .transport
            .send(
                self.target,
                self.partition,
                RaftRpcRequest::InstallSnapshot(req),
            )
            .await
            .map_err(|e| self.unreachable(e))?;
        match resp {
            RaftRpcResponse::InstallSnapshot(r) => Ok(r),
            _ => Err(self.mismatch()),
        }
    }
}

/// A transport that always fails — used for a single-voter group, which never
/// sends an RPC (its quorum is itself), so `send` is unreachable in practice.
pub struct NullTransport;

impl RaftTransport for NullTransport {
    fn send<'a>(
        &'a self,
        target: NodeId,
        partition: u64,
        _req: RaftRpcRequest,
    ) -> BoxFuture<'a, Result<RaftRpcResponse, TransportError>> {
        Box::pin(async move {
            Err(TransportError(format!(
                "no raft transport configured (partition {partition}, target {target})"
            )))
        })
    }
}

/// An in-process transport that routes RPCs directly into peers' Raft instances.
/// Registered handles are looked up by `(partition, node)`; cloning shares the
/// same registry, so all members and the transport see one map.
#[derive(Clone, Default)]
pub struct LocalCluster {
    nodes: Arc<Mutex<HashMap<(u64, NodeId), openraft::Raft<RaftConfig>>>>,
}

impl LocalCluster {
    /// Publishes a node's Raft handle so peers can reach it. Call once per member
    /// after `Raft::new` and before driving the group.
    pub fn register(&self, partition: u64, node: NodeId, raft: openraft::Raft<RaftConfig>) {
        self.nodes.lock().unwrap().insert((partition, node), raft);
    }
}

impl RaftTransport for LocalCluster {
    fn send<'a>(
        &'a self,
        target: NodeId,
        partition: u64,
        req: RaftRpcRequest,
    ) -> BoxFuture<'a, Result<RaftRpcResponse, TransportError>> {
        let raft = self
            .nodes
            .lock()
            .unwrap()
            .get(&(partition, target))
            .cloned();
        Box::pin(async move {
            let raft = raft.ok_or_else(|| {
                TransportError(format!("no registered node {target} for partition {partition}"))
            })?;
            dispatch(&raft, req)
                .await
                .map_err(|e| TransportError(e.to_string()))
        })
    }
}
