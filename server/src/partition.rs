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

use crate::engine_actor::EngineHandle;

/// Identifies a partition by its id — the value encoded in the high bits of
/// every [`Key`] it mints (see [`nanobpmn_engine_core::partition_of`]).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct PartitionId(pub u64);

/// Identifies a node in the cluster. Single-node today: every partition is
/// [`Location::Local`], so a `NodeId` is never constructed until distributed
/// transport (stage 1) lands.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[allow(dead_code)]
pub struct NodeId(pub u32);

/// Where a partition's leader currently lives, as resolved by
/// [`PartitionRouter::resolve`]. Stage 0 only ever yields [`Location::Local`].
pub enum Location<'a> {
    /// The partition is owned by this node; here is its engine actor.
    Local(&'a EngineHandle),
    /// The partition is owned by a remote node (distributed mode). Unreachable
    /// while running as a single process.
    #[allow(dead_code)]
    Remote(NodeId),
}

/// Owner of a partition slot. `Local(i)` indexes into [`PartitionRouter::local`];
/// `Remote(node)` names the owning node (distributed mode only).
#[derive(Clone, Copy)]
enum Owner {
    Local(usize),
    #[allow(dead_code)]
    Remote(NodeId),
}

/// Maps each [`PartitionId`] to its current [`Location`].
///
/// In a single process this owns every partition's [`EngineHandle`] and every
/// slot is `Local`. The router is the single resolution point for all command
/// routing, so distributed mode (stage 1) only has to populate some slots with
/// `Remote(NodeId)` and teach the remote arm to forward over the network — the
/// local fast path and the engine core are untouched.
pub struct PartitionRouter {
    /// Engine actors for the partitions this node owns. `local[i]` is referenced
    /// by an `Owner::Local(i)` slot.
    local: Vec<EngineHandle>,
    /// One entry per partition id, in id order: who owns partition `id` is
    /// `owners[id]`. Today `owners[i] == Owner::Local(i)` for every partition.
    owners: Vec<Owner>,
}

impl PartitionRouter {
    /// Builds a single-node router that owns every partition locally.
    /// `handles[i]` becomes the owner of [`PartitionId`] `i`. Must be non-empty.
    fn single_node(handles: Vec<EngineHandle>) -> Self {
        assert!(!handles.is_empty(), "at least one partition is required");
        let owners = (0..handles.len()).map(Owner::Local).collect();
        Self {
            local: handles,
            owners,
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
                debug_assert!(false, "remote partition in single-process mode");
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
}

impl Partitions {
    /// Wraps one engine actor per partition. `handles[i]` owns partition id `i`.
    /// Must be non-empty.
    pub fn new(handles: Vec<EngineHandle>) -> Self {
        Self {
            router: Arc::new(PartitionRouter::single_node(handles)),
            next_create: Arc::new(AtomicUsize::new(0)),
            next_activate: Arc::new(AtomicUsize::new(0)),
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

    /// The next partition to receive a fresh `createProcessInstance`, chosen
    /// round-robin. An instance lives on the partition that created it for its
    /// whole life (its key embeds the partition).
    pub fn for_create(&self) -> &EngineHandle {
        let n = self.router.partition_count();
        if n == 1 {
            return self.router.local_for(PartitionId(0));
        }
        let i = self.next_create.fetch_add(1, Ordering::Relaxed) % n;
        self.router.local_for(PartitionId(i as u64))
    }

    /// The partition index at which the next job-activation pass should begin
    /// probing, chosen round-robin. A pass probes partitions in wrap-around order
    /// from here, so activation load spreads evenly across every partition's
    /// engine thread instead of concentrating on partition 0. Returns 0 for a
    /// single partition (the probe order is trivial).
    pub fn activate_start(&self) -> usize {
        let n = self.router.partition_count();
        if n == 1 {
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
}
