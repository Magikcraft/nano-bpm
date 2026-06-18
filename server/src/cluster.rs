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
//!
//! When `NANOBPMN_NODES` is unset (or one entry) the topology is single-node and
//! every partition is local — byte-for-byte today's behaviour.

/// The cluster's static topology, computed identically on every node.
#[derive(Clone, Debug)]
pub struct Topology {
    /// This node's id (index into [`peers`](Self::peers)).
    pub node_id: u32,
    /// Base URLs of every node, indexed by node id. `peers[node_id]` is self.
    pub peers: Vec<String>,
    /// Total number of partitions across the whole cluster.
    pub num_partitions: u64,
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
        Self {
            node_id,
            peers,
            num_partitions: num_partitions.max(1),
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
        let node = |id: u32| Topology { node_id: id, peers: peers.clone(), num_partitions: 7 };
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
        let t = Topology { node_id: 0, peers: vec!["http://n0".into(), "http://n1".into()], num_partitions: 2 };
        assert_eq!(t.peer_addr(1), Some("http://n1"));
        assert_eq!(t.peer_addr(9), None);
    }
}
