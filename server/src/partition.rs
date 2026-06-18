//! Multi-partition routing over a set of single-writer engine actors.
//!
//! Following Zeebe, a node may run several partitions, each its own
//! single-writer [`EngineHandle`] (a dedicated engine thread + journal). Keys
//! carry their owning partition in their high bits (see
//! [`nanobpmn_engine_core::partition_of`]), so a command that targets an
//! existing key routes to exactly one partition, while a fresh
//! `createProcessInstance` is balanced round-robin across them. Queries are
//! answered from the single shared read model and never touch a partition.
//!
//! The default is a single partition (id `0`), which preserves the historical
//! behaviour exactly: one engine thread, one journal, the `1, 2, 3, …` key
//! sequence. Set `NANOBPMN_PARTITIONS=<n>` to run `n` partitions.
//!
//! # Distributed-scaling seam
//!
//! Routing goes through a [`PartitionRouter`], which maps every [`PartitionId`]
//! to a [`Location`]: either `Local` (an engine actor owned by this node) or
//! `Remote` (owned by another node, addressed by [`NodeId`]). Today every
//! partition resolves to `Local` — this is a single process — so the indirection
//! is behaviour-preserving and free. It is the load-bearing seam for distributed
//! scaling (see `docs/distributed-scaling-design.md`): stage 1 turns some slots
//! into `Remote` and routes those commands over the network, without disturbing
//! the engine core or the local fast path.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use nanobpmn_engine_core::{Key, partition_of};

use crate::cluster::Topology;
use crate::engine_actor::EngineHandle;

/// Identifies a partition by its id — the value encoded in the high bits of
/// every [`Key`] it mints (see [`nanobpmn_engine_core::partition_of`]).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct PartitionId(pub u64);

/// Identifies a node in the cluster (an index into [`Topology::peers`]). In a
/// single-node cluster every partition is [`Location::Local`], so no `NodeId` is
/// ever produced.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[allow(dead_code)] // the inner id is read by the stage-1 forwarding layer
pub struct NodeId(pub u32);

/// Where a partition's leader currently lives, as resolved by
/// [`PartitionRouter::resolve`]. A single-node cluster only ever yields
/// [`Location::Local`].
#[allow(dead_code)] // Remote's NodeId is read by the stage-1 forwarding layer
pub enum Location<'a> {
    /// The partition is owned by this node; here is its engine actor.
    Local(&'a EngineHandle),
    /// The partition is owned by a remote node; forward to it (its base URL is
    /// [`Topology::peer_addr`]).
    Remote(NodeId),
}

/// Owner of a partition slot. `Local(i)` indexes into [`PartitionRouter::local`];
/// `Remote(node)` names the owning node.
#[derive(Clone, Copy)]
enum Owner {
    Local(usize),
    Remote(NodeId),
}

/// Maps each [`PartitionId`] to its current [`Location`].
///
/// A node owns a subset of the cluster's partitions ([`Topology::local_partitions`])
/// and holds an [`EngineHandle`] for each; the rest resolve `Remote(NodeId)`. The
/// router is the single resolution point for all command routing, so the gateway
/// forwarding layer only has to handle the [`Location::Remote`] arm — the local
/// fast path and the engine core are untouched. In a single-node cluster every
/// slot is `Local`, identical to pre-cluster behaviour.
pub struct PartitionRouter {
    /// Engine actors for the partitions this node owns, in ascending partition-id
    /// order. `local[i]` is referenced by an `Owner::Local(i)` slot.
    local: Vec<EngineHandle>,
    /// One entry per partition id (`owners[p]` owns partition `p`): `Local(i)`
    /// when this node owns it, `Remote(node)` otherwise.
    owners: Vec<Owner>,
    /// The cluster topology this router was built from (node ids → addresses,
    /// total partition count, ownership map).
    #[allow(dead_code)] // read via topology() once clustered startup is wired
    topology: Topology,
}

impl PartitionRouter {
    /// Builds a single-node router that owns every partition locally.
    /// `handles[i]` becomes the owner of [`PartitionId`] `i`. Must be non-empty.
    fn single_node(handles: Vec<EngineHandle>) -> Self {
        assert!(!handles.is_empty(), "at least one partition is required");
        let owners = (0..handles.len()).map(Owner::Local).collect();
        let topology = Topology::single(handles.len() as u64);
        Self {
            local: handles,
            owners,
            topology,
        }
    }

    /// Builds a router from a cluster [`Topology`]. `local_handles` are the engine
    /// actors for this node's owned partitions, in the same ascending order as
    /// [`Topology::local_partitions`]; every other partition resolves to the
    /// `Remote` node that owns it. A single-node topology is equivalent to
    /// [`single_node`](Self::single_node).
    fn from_topology(topology: Topology, local_handles: Vec<EngineHandle>) -> Self {
        let owned = topology.local_partitions();
        assert_eq!(
            owned.len(),
            local_handles.len(),
            "expected one engine handle per owned partition ({} owned, {} handles)",
            owned.len(),
            local_handles.len(),
        );
        // Map each owned partition id to its index in `local_handles`.
        let mut owners: Vec<Owner> = (0..topology.num_partitions)
            .map(|p| Owner::Remote(NodeId(topology.owner_of(p))))
            .collect();
        for (i, &p) in owned.iter().enumerate() {
            owners[p as usize] = Owner::Local(i);
        }
        assert!(!owners.is_empty(), "at least one partition is required");
        Self {
            local: local_handles,
            owners,
            topology,
        }
    }

    /// Total number of partitions in the cluster (local + remote). Single-node:
    /// equal to the number of local engine actors.
    fn partition_count(&self) -> usize {
        self.owners.len()
    }

    /// Resolves a partition to its current [`Location`]. An out-of-range id
    /// (a malformed key) falls back to partition 0 so it surfaces as a clean
    /// engine "not found" rather than a panic — preserving the historical
    /// single-partition behaviour.
    pub fn resolve(&self, p: PartitionId) -> Location<'_> {
        match self.owners.get(p.0 as usize) {
            Some(Owner::Local(i)) => Location::Local(&self.local[*i]),
            Some(Owner::Remote(n)) => Location::Remote(*n),
            None => Location::Local(&self.local[0]),
        }
    }

    /// The engine actor owning `p`, for the single-process fast path. Resolves
    /// through [`resolve`](Self::resolve) and unwraps the local case; a `Remote`
    /// slot is unreachable while running as a single process (stage 1 migrates
    /// the affected call sites to handle [`Location::Remote`] explicitly).
    fn local_for(&self, p: PartitionId) -> &EngineHandle {
        match self.resolve(p) {
            Location::Local(h) => h,
            Location::Remote(_) => {
                debug_assert!(false, "local_for called on a remote partition; callers must check resolve()/locate() and forward Remote");
                &self.local[0]
            }
        }
    }

    /// All engine actors owned by this node (in partition-id order). Used by the
    /// operations that fan out locally: job activation, message correlation,
    /// timer ticks, eviction, idle compaction.
    fn local_handles(&self) -> &[EngineHandle] {
        &self.local
    }

    /// The cluster topology backing this router.
    #[allow(dead_code)] // surfaced via Partitions::topology() in the next increment
    fn topology(&self) -> &Topology {
        &self.topology
    }
}

/// A cloneable router over one engine actor per partition.
///
/// Cheap to clone (it shares the underlying [`PartitionRouter`] and the
/// round-robin counters). Routing is a pure index computation; no locking.
#[derive(Clone)]
pub struct Partitions {
    router: Arc<PartitionRouter>,
    /// Round-robin cursor for balancing `createProcessInstance` across
    /// partitions. Relaxed is fine: it only needs to spread load, not be exact.
    next_create: Arc<AtomicUsize>,
    /// Round-robin cursor for the partition at which a job-activation pass begins
    /// its probe. Without it every activation starts at partition 0, so under
    /// load all workers hammer partition 0's engine thread while the others idle
    /// for activation. Rotating the start spreads activation evenly across every
    /// partition's writer; a pass still probes onward when its start partition is
    /// empty, so no job is ever left unactivated (no starvation). Relaxed: it
    /// only needs to spread load, not be exact.
    next_activate: Arc<AtomicUsize>,
    /// Round-robin cursor for cluster-wide create *placement*: it cycles over
    /// every partition in the cluster (local and remote), so a single gateway
    /// spreads creates across the whole cluster instead of only its own
    /// partitions. Drives [`next_create_placement`](Self::next_create_placement).
    /// Distinct from `next_create` (which balances among local handles once a
    /// create lands locally). Relaxed: spread, not exact.
    next_place: Arc<AtomicUsize>,
}

impl Partitions {
    /// Wraps one engine actor per partition for a single-node cluster.
    /// `handles[i]` owns partition id `i`. Must be non-empty.
    pub fn new(handles: Vec<EngineHandle>) -> Self {
        Self {
            router: Arc::new(PartitionRouter::single_node(handles)),
            next_create: Arc::new(AtomicUsize::new(0)),
            next_activate: Arc::new(AtomicUsize::new(0)),
            next_place: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Builds a router for a clustered node from its [`Topology`]. `local_handles`
    /// are the engine actors for this node's owned partitions, in ascending
    /// partition-id order (matching [`Topology::local_partitions`]); every other
    /// partition resolves to the remote node that owns it.
    pub fn with_topology(topology: Topology, local_handles: Vec<EngineHandle>) -> Self {
        Self {
            router: Arc::new(PartitionRouter::from_topology(topology, local_handles)),
            next_create: Arc::new(AtomicUsize::new(0)),
            next_activate: Arc::new(AtomicUsize::new(0)),
            next_place: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The cluster topology (node addresses, ownership map, partition count).
    #[allow(dead_code)] // consumed by the stage-1 forwarding layer
    pub fn topology(&self) -> &Topology {
        self.router.topology()
    }

    /// The local engine actor that owns global partition id `p`, or `None` when
    /// `p` is owned by a remote node (or out of range). Use this to route an
    /// operation addressed by *global partition id* (e.g. evicting a completed
    /// instance by its key's partition) — unlike indexing [`all`](Self::all),
    /// which is the compacted slice of owned handles, not indexed by global id.
    pub fn local_for_partition(&self, p: u64) -> Option<&EngineHandle> {
        match self.router.resolve(PartitionId(p)) {
            Location::Local(h) => Some(h),
            Location::Remote(_) => None,
        }
    }

    /// Number of partitions.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.router.partition_count()
    }

    /// True when running a single partition (the common, zero-overhead case).
    pub fn is_single(&self) -> bool {
        self.router.partition_count() == 1
    }

    /// Resolves a key to the [`Location`] of its owning partition. The routing
    /// seam stage 1 builds on: today every key resolves [`Location::Local`].
    #[allow(dead_code)]
    pub fn locate(&self, key: Key) -> Location<'_> {
        self.router.resolve(PartitionId(partition_of(key)))
    }

    /// The handle owning `key`, decoded from the key's partition bits. A key
    /// whose partition is out of range (malformed input) falls back to
    /// partition 0 so a bad key surfaces as a clean engine "not found" rather
    /// than a panic.
    pub fn by_key(&self, key: Key) -> &EngineHandle {
        self.router.local_for(PartitionId(partition_of(key)))
    }

    /// The id of the remote node owning `key`'s partition, or `None` when this
    /// node owns it (the local fast path) — the by-key forwarding seam. A
    /// gateway uses this to decide whether a by-key operation
    /// (complete/fail/cancel/…) must be forwarded to a peer over the command
    /// stream. Single-node clusters always return `None`.
    pub fn remote_owner(&self, key: Key) -> Option<u32> {
        match self.router.resolve(PartitionId(partition_of(key))) {
            Location::Local(_) => None,
            Location::Remote(NodeId(node)) => Some(node),
        }
    }

    /// The set of remote nodes this gateway can forward to: every node that owns
    /// at least one partition not owned locally, in ascending id order. Empty on a
    /// single-node cluster (every partition is `Local`), so the dispatcher's
    /// job-aggregation fan-out becomes a no-op and the hot path stays unchanged.
    pub fn peer_nodes(&self) -> Vec<u32> {
        let mut nodes: Vec<u32> = (0..self.router.partition_count() as u64)
            .filter_map(|p| match self.router.resolve(PartitionId(p)) {
                Location::Local(_) => None,
                Location::Remote(NodeId(node)) => Some(node),
            })
            .collect();
        nodes.sort_unstable();
        nodes.dedup();
        nodes
    }


    /// round-robin across the partitions **this node owns**. An instance lives on
    /// the partition that created it for its whole life (its key embeds the
    /// partition). In a single-node cluster the owned set is every partition in
    /// id order, so this is byte-identical to the pre-cluster round-robin; in a
    /// multi-node cluster each node creates only on its own partitions (the hot
    /// path needs no cross-node forwarding — clients spread across gateways).
    pub fn for_create(&self) -> &EngineHandle {
        let locals = self.router.local_handles();
        if locals.len() == 1 {
            return &locals[0];
        }
        let i = self.next_create.fetch_add(1, Ordering::Relaxed) % locals.len();
        &locals[i]
    }

    /// Cluster-wide create *placement*: round-robins over **every** partition in
    /// the cluster and returns the remote node that owns the chosen partition, or
    /// `None` when it is local (create here, the fast path). This lets a single
    /// gateway spread `createProcessInstance` across the whole cluster — the
    /// stage-1 create-forwarding seam — rather than only its own partitions, so
    /// one client connection can drive every node. A remote placement is forwarded
    /// to the owner over the command stream; a local one runs in-process via
    /// [`for_create`](Self::for_create).
    ///
    /// Single-node (and any node owning every partition) always returns `None`:
    /// the placement is always local, so the create path is byte-identical to the
    /// pre-cluster behaviour with zero forwarding overhead.
    pub fn next_create_placement(&self) -> Option<u32> {
        let n = self.router.partition_count();
        if n <= 1 {
            return None;
        }
        let p = self.next_place.fetch_add(1, Ordering::Relaxed) % n;
        match self.router.resolve(PartitionId(p as u64)) {
            Location::Local(_) => None,
            Location::Remote(NodeId(node)) => Some(node),
        }
    }

    /// The local-partition index at which the next job-activation pass should
    /// begin probing, chosen round-robin. A pass probes this node's owned
    /// partitions in wrap-around order from here, so activation load spreads
    /// evenly across every local engine thread instead of concentrating on the
    /// first. Returns 0 for a single owned partition (the probe order is trivial).
    pub fn activate_start(&self) -> usize {
        let n = self.router.local_handles().len();
        if n <= 1 {
            return 0;
        }
        self.next_activate.fetch_add(1, Ordering::Relaxed) % n
    }

    /// The partition that owns deployments. Deployments are processed and
    /// journaled here, then replicated in-memory to the others (so every
    /// partition can instantiate the definition). Partition 0 also owns the
    /// single copy of each message-start / timer-start subscription.
    pub fn deploy_partition(&self) -> &EngineHandle {
        self.router.local_for(PartitionId(0))
    }

    /// All partition handles, for operations that must fan out (job activation,
    /// message correlation, timer ticks, eviction, idle compaction).
    pub fn all(&self) -> &[EngineHandle] {
        self.router.local_handles()
    }

    /// Total depth of every partition's `Low` (creation) queue: the standing
    /// backlog of submitted-but-not-yet-applied creates across the node. The
    /// create-admission gate bounds this to cap create-side latency under overload.
    pub fn pending_create_queue(&self) -> usize {
        self.router
            .local_handles()
            .iter()
            .map(EngineHandle::pending_low)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::Journal;
    use nanobpmn_engine_core::compose_key;

    fn spawn_partitions(n: u64) -> Partitions {
        let handles = (0..n)
            .map(|i| EngineHandle::spawn(Journal::in_memory_partition(i), None))
            .collect();
        Partitions::new(handles)
    }

    #[test]
    fn router_resolves_every_partition_locally() {
        let parts = spawn_partitions(4);
        assert_eq!(parts.len(), 4);
        assert!(!parts.is_single());
        for i in 0..4u64 {
            match parts.router.resolve(PartitionId(i)) {
                Location::Local(_) => {}
                Location::Remote(_) => {
                    panic!("partition {i} should be local in single-process mode")
                }
            }
        }
    }

    #[test]
    fn by_key_routes_to_the_partition_in_the_key() {
        let parts = spawn_partitions(4);
        // A key minted by partition 2 must resolve to the same handle as all()[2]
        // (both borrow router.local[2]).
        let key = compose_key(2, 7);
        assert_eq!(partition_of(key), 2);
        assert!(std::ptr::eq(parts.by_key(key), &parts.all()[2]));
    }

    #[test]
    fn out_of_range_key_falls_back_to_partition_zero() {
        let parts = spawn_partitions(2);
        // Partition id 9 doesn't exist (only 0, 1); must fall back, not panic.
        let key = compose_key(9, 1);
        assert!(std::ptr::eq(parts.by_key(key), &parts.all()[0]));
        match parts.router.resolve(PartitionId(9)) {
            Location::Local(_) => {}
            Location::Remote(_) => panic!("fallback must be local"),
        }
    }

    #[test]
    fn single_partition_is_single() {
        let parts = spawn_partitions(1);
        assert!(parts.is_single());
        assert_eq!(parts.len(), 1);
        assert_eq!(parts.activate_start(), 0);
    }

    #[test]
    fn clustered_router_resolves_owned_local_and_others_remote() {
        // Node 0 of a 2-node, 4-partition cluster owns partitions 0 and 2; it
        // holds engine handles only for those. Partitions 1 and 3 must resolve
        // Remote(node 1); 0 and 2 must resolve Local.
        let topology = Topology {
            node_id: 0,
            peers: vec!["http://n0".into(), "http://n1".into()],
            num_partitions: 4,
        };
        let owned = topology.local_partitions();
        assert_eq!(owned, vec![0, 2]);
        let handles: Vec<EngineHandle> = owned
            .iter()
            .map(|p| EngineHandle::spawn(Journal::in_memory_partition(*p), None))
            .collect();
        let parts = Partitions::with_topology(topology, handles);

        assert_eq!(parts.len(), 4);
        assert!(!parts.is_single());
        // local() / all() only holds the owned partitions.
        assert_eq!(parts.all().len(), 2);

        for p in [0u64, 2] {
            match parts.router.resolve(PartitionId(p)) {
                Location::Local(_) => {}
                Location::Remote(_) => panic!("partition {p} should be Local on node 0"),
            }
        }
        for p in [1u64, 3] {
            match parts.router.resolve(PartitionId(p)) {
                Location::Remote(NodeId(1)) => {}
                Location::Remote(NodeId(other)) => panic!("partition {p} owner should be node 1, got {other}"),
                Location::Local(_) => panic!("partition {p} should be Remote on node 0"),
            }
        }
        // by_key of an owned partition resolves locally; both 0 and 2 present.
        assert!(std::ptr::eq(parts.by_key(compose_key(2, 1)), &parts.all()[1]));
    }
}
