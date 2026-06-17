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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use nanobpmn_engine_core::{Key, partition_of};

use crate::engine_actor::EngineHandle;

/// A cloneable router over one engine actor per partition.
///
/// Cheap to clone (it shares the underlying handles and the round-robin
/// counter). Routing is a pure index computation; no locking.
#[derive(Clone)]
pub struct Partitions {
    handles: Arc<Vec<EngineHandle>>,
    /// Round-robin cursor for balancing `createProcessInstance` across
    /// partitions. Relaxed is fine: it only needs to spread load, not be exact.
    next_create: Arc<AtomicUsize>,
}

impl Partitions {
    /// Wraps one engine actor per partition. `handles[i]` owns partition id `i`.
    /// Must be non-empty.
    pub fn new(handles: Vec<EngineHandle>) -> Self {
        assert!(!handles.is_empty(), "at least one partition is required");
        Self {
            handles: Arc::new(handles),
            next_create: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Number of partitions.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    /// True when running a single partition (the common, zero-overhead case).
    pub fn is_single(&self) -> bool {
        self.handles.len() == 1
    }

    /// The handle owning `key`, decoded from the key's partition bits. A key
    /// whose partition is out of range (malformed input) falls back to
    /// partition 0 so a bad key surfaces as a clean engine "not found" rather
    /// than a panic.
    pub fn by_key(&self, key: Key) -> &EngineHandle {
        let idx = partition_of(key) as usize;
        self.handles.get(idx).unwrap_or(&self.handles[0])
    }

    /// The next partition to receive a fresh `createProcessInstance`, chosen
    /// round-robin. An instance lives on the partition that created it for its
    /// whole life (its key embeds the partition).
    pub fn for_create(&self) -> &EngineHandle {
        if self.handles.len() == 1 {
            return &self.handles[0];
        }
        let i = self.next_create.fetch_add(1, Ordering::Relaxed) % self.handles.len();
        &self.handles[i]
    }

    /// The partition that owns deployments. Deployments are processed and
    /// journaled here, then replicated in-memory to the others (so every
    /// partition can instantiate the definition). Partition 0 also owns the
    /// single copy of each message-start / timer-start subscription.
    pub fn deploy_partition(&self) -> &EngineHandle {
        &self.handles[0]
    }

    /// All partition handles, for operations that must fan out (job activation,
    /// message correlation, timer ticks, eviction, idle compaction).
    pub fn all(&self) -> &[EngineHandle] {
        &self.handles
    }
}
