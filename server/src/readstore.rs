//! Server-side facade over the shared read model.
//!
//! The SQLite projection and read-query surface now live in the standalone
//! [`nanobpmn_read_model`] crate (package `nanobpmn-read-model`) so the exact
//! same schema, FNV-1a fingerprint, event projection and read queries can compile
//! both here (feature `native`, a real bundled C SQLite — what this gateway ships)
//! and, for the in-browser WASM test engine, against an in-memory SQLite. This
//! module is now a thin shim that:
//!
//! * re-exports the shared [`ReadStore`] and every `*Row` / `ExportOutcome` result
//!   type so existing `crate::readstore::*` paths keep resolving unchanged, and
//! * keeps the **server-only orchestration** that never belonged in the shared
//!   crate: the sharded [`ReadModel`] fan-out and the [`ProjectionSink`] exporter
//!   seam. The per-shard WAL/checkpoint and adaptive-pruning methods stay on
//!   [`ReadStore`] itself but are compiled only under the shared crate's `native`
//!   feature (which this crate always enables).

use std::collections::HashMap;
use std::sync::Arc;

use nanobpmn_engine_core::{Event, Key, ReadQuery, partition_of};
// Re-export the shared read-model surface so `crate::readstore::ProcessInstanceRow`
// (and friends) resolve exactly as before the extraction.
pub use nanobpmn_read_model::*;

/// Names the [`ReadStore`] query method each gateway REST read routes through.
///
/// This is the gateway-side anchor for the shared [`ReadQuery`] surface enum
/// (defined in `engine-core` alongside `Command`). It exists so the gateway
/// *references* every read it serves through the one shared enum: the match is
/// exhaustive and wildcard-free, so adding a `ReadQuery` variant — the gateway
/// beginning to serve a new read — forces this map to be extended here as well
/// as classified in the wasm parity gate (`engine-wasm/src/surface_parity.rs`).
/// It changes no observable REST behaviour; the concrete handlers still call the
/// named [`ReadStore`] methods directly.
#[allow(dead_code)]
pub(crate) fn read_store_method(query: ReadQuery) -> &'static str {
    match query {
        ReadQuery::GetFormByKey => "form_by_key",
        ReadQuery::GetResourceByKey => "resource_by_key",
        ReadQuery::SearchResources => "resources_meta",
        ReadQuery::SearchProcessInstances => "process_instances",
        ReadQuery::GetProcessInstance => "process_instance",
        ReadQuery::SearchUserTasks => "user_tasks",
        ReadQuery::GetUserTask => "user_task",
        ReadQuery::SearchVariables => "variables",
        ReadQuery::GetVariable => "variable",
        ReadQuery::SearchJobs => "jobs",
        ReadQuery::SearchIncidents => "incidents",
        ReadQuery::GetIncident => "incident",
        ReadQuery::SearchElementInstances => "element_instances",
        ReadQuery::GetElementInstance => "element_instance",
        ReadQuery::SearchMessageSubscriptions => "message_subscriptions",
        ReadQuery::SearchCorrelatedMessageSubscriptions => "correlated_message_subscriptions",
        ReadQuery::SearchProcessDefinitions => "process_definitions",
        ReadQuery::GetProcessDefinitionXml => "process_definition_xml",
        ReadQuery::SearchDecisionInstances => "decision_instances",
        ReadQuery::GetDecisionInstance => "decision_instance",
        ReadQuery::SearchDecisionDefinitions => "decision_definitions",
        ReadQuery::GetDecisionDefinitionXml => "decision_definition_xml",
        ReadQuery::SearchDecisionRequirements => "decision_requirements",
        ReadQuery::GetDecisionRequirementsXml => "decision_requirements_xml",
    }
}

/// The projection sink the per-shard exporter thread writes into. Abstracts the
/// read model behind the single `export` seam so the sink can be the built-in
/// local SQLite store (the default) or, in later milestones, a tee/remote sink
/// that streams the record log into an external system (data lake / warehouse)
/// and decouples read-model disk IOPS from the node (see issue #133).
///
/// The exporter thread is the one ordered point every projected event flows
/// through, in strict log (fsync) order, off the command-commit/ack hot path.
/// Any implementation MUST uphold the invariants the exporter relies on:
///
/// * **Idempotent** — projecting an overlapping prefix again (e.g. after a
///   restart replays from the last durable watermark) must be a no-op for the
///   already-applied events and yield an `inflight_delta`/`terminal_keys` that
///   count only *genuine* state transitions, never raw event occurrences.
/// * **Never lose a batch** — `export` must fully apply the batch or return an
///   error (so the exporter retries); it must not partially apply and report
///   success. `exported_position` (the compaction watermark) advances by event
///   count on the exporter thread only after `export` succeeds, so a silently
///   dropped batch is unrecoverable read-model loss.
/// * **Per-shard order** — events within a shard arrive log-ordered; the sink
///   must preserve that order.
pub trait ProjectionSink: Send + Sync {
    /// Projects a batch of consecutive, log-ordered journal events, returning the
    /// exact in-flight delta and the keys of instances that genuinely reached a
    /// terminal state in this batch. See the trait-level invariants.
    fn export(&self, events: &[&Event]) -> anyhow::Result<ExportOutcome>;

    /// Caps retained terminal instances at `max_keep`, deleting up to
    /// `max_delete` of the oldest beyond the cap (0 = unbounded). Returns the
    /// number evicted. A no-op for append-only sinks that don't retain state.
    fn prune_terminal(&self, max_keep: usize, max_delete: usize) -> anyhow::Result<usize> {
        let _ = (max_keep, max_delete);
        Ok(0)
    }
}

impl ProjectionSink for ReadStore {
    fn export(&self, events: &[&Event]) -> anyhow::Result<ExportOutcome> {
        Ok(ReadStore::export(self, events)?)
    }

    fn prune_terminal(&self, max_keep: usize, max_delete: usize) -> anyhow::Result<usize> {
        Ok(ReadStore::prune_terminal_instances(
            self, max_keep, max_delete,
        )?)
    }
}

/// A sharded read model: one [`ReadStore`] per owned partition, presenting the
/// single-store query API by routing point lookups to the owning partition's
/// shard and merging scans/counts across shards. This is opening #1 — the read
/// model was the per-node throughput ceiling because ONE exporter thread +
/// `Mutex<Connection>` projected every partition's events on a single core;
/// sharding by partition lets projection (and the read store's SQLite writer)
/// scale with cores.
///
/// Invariants: each shard only ever sees its own partition's events (the shared
/// journal writer routes by partition and boot catch-up demuxes by partition),
/// so a shard's `exported_position` is exactly its partition's projected event
/// count. Process definitions are replicated to every owned partition under the
/// same (partition-0) key, so any shard answers a definition query.
pub struct ReadModel {
    /// Shards indexed positionally; `slot_by_partition` maps a global partition
    /// id to its index here.
    shards: Vec<Arc<ReadStore>>,
    slot_by_partition: HashMap<u64, usize>,
}

impl ReadModel {
    /// Builds a read model from `(global_partition_id, shard)` pairs. Requires at
    /// least one shard (a node always owns at least one partition).
    pub fn from_shards(shards: Vec<(u64, Arc<ReadStore>)>) -> Self {
        assert!(!shards.is_empty(), "read model needs at least one shard");
        let mut slot_by_partition = HashMap::with_capacity(shards.len());
        let mut list = Vec::with_capacity(shards.len());
        for (pid, store) in shards {
            let prev = slot_by_partition.insert(pid, list.len());
            assert!(
                prev.is_none(),
                "read model got duplicate partition id {pid}; owned partition list must be unique"
            );
            list.push(store);
        }
        Self {
            shards: list,
            slot_by_partition,
        }
    }

    /// A single in-memory shard for partition 0 — the trivial (single-partition /
    /// test) case.
    pub fn single_in_memory() -> Self {
        Self::from_shards(vec![(
            0,
            Arc::new(ReadStore::open(None).expect("open in-memory read store")),
        )])
    }

    /// In-memory shards, one per partition in `owned`.
    pub fn in_memory_partitions(owned: &[u64]) -> Self {
        let shards = owned
            .iter()
            .map(|&p| {
                (
                    p,
                    Arc::new(ReadStore::open(None).expect("open in-memory read store")),
                )
            })
            .collect();
        Self::from_shards(shards)
    }

    /// The shards paired with their global partition ids, for wiring exporter
    /// threads and gathering per-partition compaction watermarks.
    pub fn shards(&self) -> Vec<(u64, Arc<ReadStore>)> {
        let mut out = vec![None; self.shards.len()];
        for (&pid, &idx) in &self.slot_by_partition {
            out[idx] = Some((pid, Arc::clone(&self.shards[idx])));
        }
        out.into_iter().flatten().collect()
    }

    /// Number of shards (owned partitions).
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn shard_for(&self, key: Key) -> Option<&ReadStore> {
        self.slot_by_partition
            .get(&partition_of(key))
            .map(|&i| self.shards[i].as_ref())
    }

    // --- point lookups: route to the key's owning partition shard ---

    pub fn process_instance(&self, key: Key) -> Option<ProcessInstanceRow> {
        self.shard_for(key)?.process_instance(key)
    }

    pub fn incident(&self, key: Key) -> Option<IncidentRow> {
        self.shard_for(key)?.incident(key)
    }

    pub fn element_instance(&self, key: Key) -> Option<ElementInstanceRow> {
        self.shard_for(key)?.element_instance(key)
    }

    pub fn variable(&self, key: Key) -> Option<VariableRow> {
        self.shard_for(key)?.variable(key)
    }

    pub fn instance_variables(&self, instance_key: Key) -> Vec<VariableRow> {
        self.shard_for(instance_key)
            .map(|s| s.instance_variables(instance_key))
            .unwrap_or_default()
    }

    /// The `Active` element instances for one process instance, routed to the
    /// instance's owning shard and selected by the `instance_key` index.
    pub fn active_element_instances(&self, instance_key: Key) -> Vec<ElementInstanceRow> {
        self.shard_for(instance_key)
            .map(|s| s.active_element_instances(instance_key))
            .unwrap_or_default()
    }

    // --- definitions: replicated to every owned partition under the same key ---

    pub fn process_definitions(&self) -> Vec<ProcessDefinitionRow> {
        // Every shard holds every definition (replicated on deploy); read from
        // the first shard, falling back if it has none yet (mid-catch-up).
        for s in &self.shards {
            let defs = s.process_definitions();
            if !defs.is_empty() {
                return defs;
            }
        }
        Vec::new()
    }

    /// Fetches a single process definition by key across shards (every shard
    /// holds every definition, so the first shard that knows the key answers).
    pub fn process_definition_by_key(&self, key: Key) -> Option<ProcessDefinitionRow> {
        for s in &self.shards {
            if let Some(row) = s.process_definition_by_key(key) {
                return Some(row);
            }
        }
        None
    }

    pub fn process_definition_xml(&self, key: Key) -> Option<String> {
        for s in &self.shards {
            if let Some(xml) = s.process_definition_xml(key) {
                return Some(xml);
            }
        }
        None
    }

    pub fn process_definition_start_form_id(&self, key: Key) -> Option<Option<String>> {
        for s in &self.shards {
            if let Some(row) = s.process_definition_start_form_id(key) {
                return Some(row);
            }
        }
        None
    }

    pub fn decision_requirements(&self) -> Vec<DecisionRequirementsRow> {
        for s in &self.shards {
            let defs = s.decision_requirements();
            if !defs.is_empty() {
                return defs;
            }
        }
        Vec::new()
    }

    pub fn decision_requirements_by_key(&self, key: Key) -> Option<DecisionRequirementsRow> {
        for s in &self.shards {
            if let Some(row) = s.decision_requirements_by_key(key) {
                return Some(row);
            }
        }
        None
    }

    pub fn decision_requirements_xml(&self, key: Key) -> Option<String> {
        for s in &self.shards {
            if let Some(xml) = s.decision_requirements_xml(key) {
                return Some(xml);
            }
        }
        None
    }

    pub fn decision_definitions(&self) -> Vec<DecisionDefinitionRow> {
        for s in &self.shards {
            let defs = s.decision_definitions();
            if !defs.is_empty() {
                return defs;
            }
        }
        Vec::new()
    }

    pub fn decision_definition_by_key(&self, key: Key) -> Option<DecisionDefinitionRow> {
        for s in &self.shards {
            if let Some(row) = s.decision_definition_by_key(key) {
                return Some(row);
            }
        }
        None
    }

    pub fn decision_definition_xml(&self, key: Key) -> Option<String> {
        for s in &self.shards {
            if let Some(xml) = s.decision_definition_xml(key) {
                return Some(xml);
            }
        }
        None
    }

    pub fn form_by_key(&self, key: Key) -> Option<FormRow> {
        for s in &self.shards {
            if let Some(row) = s.form_by_key(key) {
                return Some(row);
            }
        }
        None
    }

    pub fn form_by_id(&self, form_id: &str) -> Option<FormRow> {
        for s in &self.shards {
            if let Some(row) = s.form_by_id(form_id) {
                return Some(row);
            }
        }
        None
    }

    pub fn resource_by_key(&self, key: Key) -> Option<ResourceRow> {
        for s in &self.shards {
            if let Some(row) = s.resource_by_key(key) {
                return Some(row);
            }
        }
        None
    }

    pub fn resource_by_key_meta(&self, key: Key) -> Option<ResourceMetaRow> {
        for s in &self.shards {
            if let Some(row) = s.resource_by_key_meta(key) {
                return Some(row);
            }
        }
        None
    }

    pub fn resources_meta(&self) -> Vec<ResourceMetaRow> {
        self.shards
            .iter()
            .flat_map(|s| s.resources_meta())
            .collect()
    }

    pub fn process_instances(&self) -> Vec<ProcessInstanceRow> {
        self.shards
            .iter()
            .flat_map(|s| s.process_instances())
            .collect()
    }

    pub fn jobs(&self) -> Vec<JobRow> {
        self.shards.iter().flat_map(|s| s.jobs()).collect()
    }

    pub fn user_tasks(&self) -> Vec<UserTaskRow> {
        self.shards.iter().flat_map(|s| s.user_tasks()).collect()
    }

    pub fn user_task(&self, key: Key) -> Option<UserTaskRow> {
        self.shards.iter().find_map(|s| s.user_task(key))
    }

    pub fn incidents(&self) -> Vec<IncidentRow> {
        self.shards.iter().flat_map(|s| s.incidents()).collect()
    }

    pub fn element_instances(&self) -> Vec<ElementInstanceRow> {
        self.shards
            .iter()
            .flat_map(|s| s.element_instances())
            .collect()
    }

    /// Every open message subscription across all shards (MESSAGE wait states).
    pub fn message_subscriptions(&self) -> Vec<MessageSubscriptionRow> {
        self.shards
            .iter()
            .flat_map(|s| s.message_subscriptions())
            .collect()
    }

    /// Every correlated (historical) message subscription across all shards.
    pub fn correlated_message_subscriptions(&self) -> Vec<CorrelatedMessageSubscriptionRow> {
        self.shards
            .iter()
            .flat_map(|s| s.correlated_message_subscriptions())
            .collect()
    }

    /// Every decision-instance row across all shards. Decision instances live in
    /// the shard of their owning process instance (routed by `max_key`), so a
    /// full listing must concatenate across shards.
    pub fn decision_instances(&self) -> Vec<DecisionInstanceRow> {
        self.shards
            .iter()
            .flat_map(|s| s.decision_instances())
            .collect()
    }

    /// A single decision-instance by its composite `<key>-<idx>` id. The row is
    /// keyed by a string (not a partition-encoding numeric key), so its shard is
    /// unknown — scan every shard for the first match.
    pub fn decision_instance(&self, eval_instance_key: &str) -> Option<DecisionInstanceRow> {
        for s in &self.shards {
            if let Some(row) = s.decision_instance(eval_instance_key) {
                return Some(row);
            }
        }
        None
    }

    /// Every decision-instance row for a `decision_evaluation_key`, concatenated
    /// across shards (the evaluation's rows live in a single shard, but which one
    /// is unknown from the key alone).
    pub fn decision_instances_by_evaluation_key(
        &self,
        decision_evaluation_key: Key,
    ) -> Vec<DecisionInstanceRow> {
        self.shards
            .iter()
            .flat_map(|s| s.decision_instances_by_evaluation_key(decision_evaluation_key))
            .collect()
    }

    pub fn variables(&self) -> Vec<VariableRow> {
        self.shards.iter().flat_map(|s| s.variables()).collect()
    }

    // --- counts: sum across shards ---

    pub fn active_instance_count(&self) -> usize {
        self.shards.iter().map(|s| s.active_instance_count()).sum()
    }

    /// Reconciles orphaned `Active` rows across every shard against the engine's
    /// authoritative live-instance set (`live` = hot ∪ cold keys for all owned
    /// partitions). Because instance keys are globally unique (they encode the
    /// partition), a single global `live` set is safe to apply to every shard: a
    /// key that is genuinely live on its own partition is present in `live` and is
    /// never reconciled. Returns the total rows reconciled — the amount by which
    /// the in-flight gauge was over-counting. See
    /// [`ReadStore::reconcile_orphaned_active`].
    pub fn reconcile_orphaned_active(&self, live: &std::collections::HashSet<Key>) -> usize {
        self.shards
            .iter()
            .map(|s| s.reconcile_orphaned_active(live))
            .sum()
    }

    pub fn process_instance_count(&self) -> i64 {
        self.shards.iter().map(|s| s.process_instance_count()).sum()
    }

    /// Sum of every shard's `exported_position`. Monotonic across all shards, so
    /// it is a valid change cursor for the console's instance stream. NOTE: this
    /// is NOT the compaction watermark — segment deletion uses the per-partition
    /// vector from [`ReadModel::exported_watermarks`] (a global sum could pass
    /// while a lagging shard still needs the segment).
    pub fn exported_position(&self) -> usize {
        self.shards.iter().map(|s| s.exported_position()).sum()
    }

    /// Per-partition exported watermarks indexed by global partition id (length
    /// `num_partitions`; non-owned partitions stay 0). Feeds
    /// [`crate::seglog::compact_multi`]'s per-partition export gate.
    pub fn exported_watermarks(&self, num_partitions: usize) -> Vec<u64> {
        let mut v = vec![0u64; num_partitions];
        for (&pid, &idx) in &self.slot_by_partition {
            if (pid as usize) < num_partitions {
                v[pid as usize] = self.shards[idx].exported_position() as u64;
            }
        }
        v
    }

    // --- paged: single-shard pushes down to SQL; multi merges + slices ---

    pub fn process_instances_page(&self, limit: i64, offset: i64) -> Vec<ProcessInstanceRow> {
        if self.shards.len() == 1 {
            return self.shards[0].process_instances_page(limit, offset);
        }
        let mut all = self.process_instances();
        // Newest-first by key (keys are monotonic per partition), matching the
        // single-store `ORDER BY key DESC`.
        all.sort_by_key(|b| std::cmp::Reverse(b.key));
        let start = offset.max(0) as usize;
        let take = limit.max(0) as usize;
        all.into_iter().skip(start).take(take).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard() -> Arc<ReadStore> {
        Arc::new(ReadStore::open(None).expect("open in-memory read store"))
    }

    #[test]
    #[should_panic(expected = "duplicate partition id")]
    fn from_shards_rejects_duplicate_partition_ids() {
        // A duplicate partition id would overwrite the slot mapping while still
        // pushing both shard handles, leaving one shard unreachable/misrouted.
        // The constructor must fail fast instead of silently misrouting.
        ReadModel::from_shards(vec![(1, shard()), (1, shard())]);
    }

    #[test]
    fn from_shards_accepts_distinct_partition_ids() {
        let model = ReadModel::from_shards(vec![(0, shard()), (2, shard()), (5, shard())]);
        let mut pids: Vec<u64> = model.shards().into_iter().map(|(pid, _)| pid).collect();
        pids.sort_unstable();
        assert_eq!(pids, vec![0, 2, 5]);
    }
}
