//! Cluster topology: which node owns which partition, and how to reach peers.
//!
//! A nanobpmn deployment is one or more **nodes** (processes). Every node runs
//! two roles: a **broker** owning a subset of the cluster's partitions (an engine
//! actor + journal per owned partition), and a **gateway** that accepts client
//! connections and addresses the *whole* cluster, forwarding operations it does
//! not own to the node that does. See `docs/distributed-scaling-design.md`.
//!
//! Partitions are assigned to nodes deterministically (`partition_id %
//! num_nodes`), so every node computes the same ownership map from static config
//! with no coordinator. Because `createProcessInstance` round-robins partitions
//! and partitions round-robin nodes, fresh instances spread evenly across nodes.
//!
//! Config (env):
//! - `NANOBPMN_PARTITIONS` — total partitions in the cluster (default 1).
//! - `NANOBPMN_NODES` — comma-separated peer base URLs, **index = node id**
//!   (e.g. `http://10.0.0.1:8080,http://10.0.0.2:8080`). Unset ⇒ single node.
//! - `NANOBPMN_NODE_ID` — this node's id (index into `NANOBPMN_NODES`). Default 0.
//! - `NANOBPMN_RF` — replication factor (default 1). Each partition is hosted by
//!   `RF` consecutive nodes (the replica set); the first is its leader. RF=1 is
//!   today's single-homed behaviour. RF is clamped to `[1, num_nodes]`.
//!
//! When `NANOBPMN_NODES` is unset (or one entry) the topology is single-node and
//! every partition is local — byte-for-byte today's behaviour.

/// Per-node partition leadership/recovery counts, derived purely from engine +
/// Raft state (topology ownership vs. live leaders). Base-build (non-console) so
/// both the Prometheus `/metrics` exporter and the console's richer
/// `RecoveryDto` share one source of truth. Cheap: a borrow of each hosted
/// partition's Raft metrics watch. All-zero in steady single-node / off-Raft.
#[derive(Debug, Clone, Copy, Default)]
pub struct RecoveryCounts {
    /// Partitions this node statically owns (its steady-state leadership set).
    pub owned: u32,
    /// Owned partitions this node currently leads again (reclaimed / steady).
    pub reclaimed: u32,
    /// Owned partitions currently led by a peer failover incumbent — the ones
    /// this node is still catching up on.
    pub catching_up: u32,
    /// Partitions this node leads on behalf of a peer owner (this node is the
    /// failover incumbent, handing leadership back).
    pub handing_off: u32,
    /// Largest replication lag (log entries) of a returning owner this node is
    /// handing a partition back to, when known (incumbent side only).
    pub handoff_lag_entries: Option<u64>,
}

/// The cluster's static topology, computed identically on every node.
#[derive(Clone, Debug)]
pub struct Topology {
    /// This node's id (index into [`peers`](Self::peers)).
    pub node_id: u32,
    /// Base URLs of every node, indexed by node id. `peers[node_id]` is self.
    pub peers: Vec<String>,
    /// Total number of partitions across the whole cluster.
    pub num_partitions: u64,
    /// Replication factor: how many nodes host each partition (the replica set).
    /// 1 = single-homed (today's behaviour). Always clamped to `[1, num_nodes]`.
    /// Defaults to 1 (use [`with_rf`](Self::with_rf) or `NANOBPMN_RF` to raise it).
    pub replication_factor: u32,
}

impl Topology {
    /// A single-node cluster owning all `num_partitions` partitions — today's
    /// behaviour. `num_partitions` must be ≥ 1.
    pub fn single(num_partitions: u64) -> Self {
        debug_assert!(num_partitions >= 1);
        Self {
            node_id: 0,
            peers: vec![String::new()], // self address unused single-node
            num_partitions: num_partitions.max(1),
            replication_factor: 1,
        }
    }

    /// Builds the topology from environment config. Falls back to a single-node
    /// topology (every partition local) when `NANOBPMN_NODES` is unset or names
    /// a single node, so the default deployment is unchanged.
    pub fn from_env(num_partitions: u64) -> Self {
        let peers: Vec<String> = match std::env::var("NANOBPMN_NODES") {
            Ok(v) => v
                .split(',')
                .map(|s| s.trim().trim_end_matches('/').to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            Err(_) => Vec::new(),
        };
        if peers.len() <= 1 {
            return Self::single(num_partitions);
        }
        let node_id = std::env::var("NANOBPMN_NODE_ID")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .filter(|id| (*id as usize) < peers.len())
            .unwrap_or(0);
        let num_nodes = peers.len() as u32;
        let replication_factor = std::env::var("NANOBPMN_RF")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .unwrap_or(1)
            .clamp(1, num_nodes.max(1));
        Self {
            node_id,
            peers,
            num_partitions: num_partitions.max(1),
            replication_factor,
        }
    }

    /// Number of nodes in the cluster.
    pub fn num_nodes(&self) -> u32 {
        self.peers.len() as u32
    }

    /// True when this is a single-node cluster (every partition is local).
    pub fn is_single_node(&self) -> bool {
        self.peers.len() <= 1
    }

    /// The id of the node that owns partition `p`. Deterministic round-robin
    /// (`p % num_nodes`) so every node agrees with no coordination.
    pub fn owner_of(&self, partition: u64) -> u32 {
        (partition % self.num_nodes() as u64) as u32
    }

    /// Whether this node owns partition `p`.
    pub fn is_local(&self, partition: u64) -> bool {
        self.owner_of(partition) == self.node_id
    }

    /// The partition ids this node owns, in ascending order. These are the
    /// partitions for which the node spawns a local engine actor + journal.
    pub fn local_partitions(&self) -> Vec<u64> {
        (0..self.num_partitions)
            .filter(|p| self.is_local(*p))
            .collect()
    }

    /// The base URL of node `id`, or `None` if out of range.
    #[allow(dead_code)] // consumed by the stage-1 peer-forwarding layer
    pub fn peer_addr(&self, id: u32) -> Option<&str> {
        self.peers.get(id as usize).map(String::as_str)
    }

    /// The effective replication factor, clamped to `[1, num_nodes]`. A literal
    /// `Topology` may carry any `replication_factor`; this is the value the
    /// placement functions actually use (you can never have more replicas than
    /// nodes, and never fewer than one).
    pub fn effective_rf(&self) -> u32 {
        self.replication_factor.clamp(1, self.num_nodes().max(1))
    }

    /// The replica set of partition `p`: the `RF` consecutive nodes
    /// `[owner, owner+1, …, owner+RF-1] (mod num_nodes)`, where `owner = p %
    /// num_nodes`. The first entry is the partition's leader at RF=1 and the
    /// initial/preferred leader at higher RF. Deterministic, so every node
    /// computes the same set with no coordination. At RF=1 this is just
    /// `[owner_of(p)]` — today's single-homed placement.
    pub fn replicas_of(&self, p: u64) -> Vec<u32> {
        let n = self.num_nodes().max(1);
        let rf = self.effective_rf();
        let owner = self.owner_of(p);
        (0..rf).map(|i| (owner + i) % n).collect()
    }

    /// The node currently considered the leader of partition `p`. Today this is
    /// the static replica-set head (`owner_of(p)`); stage-3 failover will make it
    /// dynamic (the elected leader), at which point only this resolver changes —
    /// routing and replication hang off it.
    pub fn leader_of(&self, p: u64) -> u32 {
        self.owner_of(p)
    }

    /// Whether this node is in partition `p`'s replica set (leader or follower).
    /// At RF=1 this is exactly [`is_local`](Self::is_local).
    pub fn is_replica(&self, p: u64) -> bool {
        self.replicas_of(p).contains(&self.node_id)
    }

    /// The partition ids this node replicates (as leader or follower), ascending.
    /// At RF=1 this equals [`local_partitions`](Self::local_partitions); at higher
    /// RF it additionally includes the partitions this node follows.
    pub fn replica_partitions(&self) -> Vec<u64> {
        (0..self.num_partitions)
            .filter(|p| self.is_replica(*p))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_node_owns_everything() {
        let t = Topology::single(4);
        assert!(t.is_single_node());
        assert_eq!(t.num_nodes(), 1);
        assert_eq!(t.local_partitions(), vec![0, 1, 2, 3]);
        for p in 0..4 {
            assert!(t.is_local(p));
            assert_eq!(t.owner_of(p), 0);
        }
    }

    #[test]
    fn ownership_is_round_robin_across_nodes() {
        // 3 nodes, 7 partitions: node i owns partitions p where p % 3 == i.
        let peers = vec!["a".into(), "b".into(), "c".into()];
        let node = |id: u32| Topology {
            node_id: id,
            peers: peers.clone(),
            num_partitions: 7,
            replication_factor: 1,
        };
        assert_eq!(node(0).local_partitions(), vec![0, 3, 6]);
        assert_eq!(node(1).local_partitions(), vec![1, 4]);
        assert_eq!(node(2).local_partitions(), vec![2, 5]);
        // Every partition is owned by exactly one node.
        for p in 0..7u64 {
            let owners: Vec<u32> = (0..3).filter(|id| node(*id).is_local(p)).collect();
            assert_eq!(owners.len(), 1, "partition {p} must have exactly one owner");
            assert_eq!(node(0).owner_of(p), owners[0]);
        }
    }

    #[test]
    fn peer_addr_resolves_by_id() {
        let t = Topology {
            node_id: 0,
            peers: vec!["http://n0".into(), "http://n1".into()],
            num_partitions: 2,
            replication_factor: 1,
        };
        assert_eq!(t.peer_addr(1), Some("http://n1"));
        assert_eq!(t.peer_addr(9), None);
    }

    #[test]
    fn rf1_replica_set_is_single_homed() {
        // RF=1: a partition's replica set is just its owner; leader == owner;
        // replica_partitions == local_partitions. Byte-for-byte today's placement.
        let peers = vec!["a".into(), "b".into(), "c".into()];
        let node = |id: u32| Topology {
            node_id: id,
            peers: peers.clone(),
            num_partitions: 7,
            replication_factor: 1,
        };
        for id in 0..3 {
            let t = node(id);
            for p in 0..7u64 {
                assert_eq!(t.replicas_of(p), vec![t.owner_of(p)]);
                assert_eq!(t.leader_of(p), t.owner_of(p));
                assert_eq!(t.is_replica(p), t.is_local(p));
            }
            assert_eq!(t.replica_partitions(), t.local_partitions());
        }
    }

    #[test]
    fn rf3_replica_set_is_consecutive_nodes() {
        // RF=3 over 3 nodes: every partition is hosted by all three nodes, the
        // set being the 3 consecutive nodes starting at the owner (wrap-around).
        let peers = vec!["a".into(), "b".into(), "c".into()];
        let t = Topology {
            node_id: 0,
            peers,
            num_partitions: 6,
            replication_factor: 3,
        };
        assert_eq!(t.replicas_of(0), vec![0, 1, 2]);
        assert_eq!(t.replicas_of(1), vec![1, 2, 0]);
        assert_eq!(t.replicas_of(2), vec![2, 0, 1]);
        assert_eq!(t.replicas_of(3), vec![0, 1, 2]);
        // Leader is always the replica-set head (the owner).
        for p in 0..6u64 {
            assert_eq!(t.leader_of(p), t.replicas_of(p)[0]);
        }
        // With RF == num_nodes every node replicates every partition.
        assert_eq!(t.replica_partitions(), vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn rf_is_clamped_to_node_count() {
        // RF can never exceed the number of nodes (no duplicate replicas) nor
        // drop below 1.
        let peers = vec!["a".into(), "b".into()];
        let over = Topology {
            node_id: 0,
            peers: peers.clone(),
            num_partitions: 4,
            replication_factor: 9,
        };
        assert_eq!(over.effective_rf(), 2);
        assert_eq!(over.replicas_of(0), vec![0, 1]);
        let zero = Topology {
            node_id: 0,
            peers,
            num_partitions: 4,
            replication_factor: 0,
        };
        assert_eq!(zero.effective_rf(), 1);
        assert_eq!(zero.replicas_of(1), vec![zero.owner_of(1)]);
    }
}
