//! Remote read-model projection sinks (pluggable-exporter epic, issue #133, M2).
//!
//! Two sinks are provided on top of the [`ProjectionSink`] seam:
//!
//! * [`RemoteSink`] — computes the engine-facing [`ExportOutcome`] from a tiny
//!   in-RAM in-flight index (no disk), ships the batch's events to a central
//!   exporter over a [`BatchTransport`], and advances only the local shard's
//!   compaction watermark. This is the "decoupled disk IOPS / data-lake" mode:
//!   the heavy projection (instances, variables, jobs) leaves the node entirely.
//! * [`TeeSink`] — projects to a local (authoritative) sink AND mirrors the same
//!   batch to a remote sink. The local outcome is returned, so in-flight
//!   accounting, queries, and the compaction watermark stay exactly as today
//!   while a durable copy is streamed downstream. The safe intermediate.
//!
//! The projection contract (idempotent, never-drop-a-batch, per-shard order) is
//! documented on [`ProjectionSink`]; both sinks uphold it. Remote **delivery**
//! is currently best-effort with respect to durability: the watermark advances
//! once a batch is enqueued for the transport, not once the remote target has
//! acknowledged it. Ack-gated advancement is a later milestone (#133).

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

use nanobpmn_engine_core::{Event, Key};

use crate::readstore::{ExportOutcome, ProjectionSink, ReadStore};

/// Ephemeral, in-RAM index of a shard's currently-Active instance keys.
///
/// Reproduces the SQLite projection's *genuine-transition* semantics (see
/// `readstore::project`) so an [`ExportOutcome`] can be derived without a
/// disk-backed read store: a `ProcessInstanceCreated` for a key not already
/// Active is `+1`; a `ProcessInstanceCompleted`/`Terminated` for a currently
/// Active key is `-1` and evicts it. Every other event is irrelevant to the
/// in-flight gauge and terminal eviction, so it is ignored. The set is
/// idempotent under replay: a re-delivered create (`insert` returns `false`) or
/// a re-delivered terminal (`remove` returns `false`) contributes zero, exactly
/// as the `INSERT ... DO NOTHING` / `UPDATE ... AND state = 0` guards do in SQL.
///
/// Memory is one `Key` (8 bytes) per *currently in-flight* instance only —
/// terminals are removed immediately — so it tracks the live working set, not
/// cumulative throughput.
#[derive(Default)]
pub struct InflightIndex {
    active: Mutex<HashSet<Key>>,
}

impl InflightIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies a log-ordered batch, returning the exact net in-flight delta and
    /// the keys that made a genuine Active->terminal transition in this batch.
    pub fn apply(&self, events: &[&Event]) -> ExportOutcome {
        let mut active = self.active.lock().expect("in-flight index poisoned");
        let mut inflight_delta: i64 = 0;
        let mut terminal_keys = Vec::new();
        for &event in events {
            match event {
                Event::ProcessInstanceCreated { instance_key, .. } => {
                    if active.insert(*instance_key) {
                        inflight_delta += 1;
                    }
                }
                Event::ProcessInstanceCompleted { instance_key }
                | Event::ProcessInstanceTerminated { instance_key }
                    if active.remove(instance_key) =>
                {
                    inflight_delta -= 1;
                    terminal_keys.push(*instance_key);
                }
                _ => {}
            }
        }
        ExportOutcome {
            terminal_keys,
            inflight_delta,
        }
    }

    /// Number of instances currently tracked as Active.
    pub fn active_count(&self) -> usize {
        self.active.lock().expect("in-flight index poisoned").len()
    }
}

/// Ships a serialized, log-ordered event batch for one shard to a central
/// exporter. Implementations MUST preserve per-shard ordering and MUST apply
/// backpressure (block or bounded-buffer) rather than silently dropping — the
/// record stream must not lose a batch. `partition` is the shard's global
/// partition id (the ordering/routing key downstream).
pub trait BatchTransport: Send + Sync {
    /// Enqueue one shard batch (already serialized) for delivery.
    fn send(&self, partition: u64, payload: Vec<u8>);

    /// Best-effort count of batches still buffered, for metrics/backpressure.
    /// Consumed by the remote-exporter metrics wiring (M3, issue #133).
    #[allow(dead_code)]
    fn queued(&self) -> usize {
        0
    }
}

/// A transport that discards batches — used when remote export is selected but
/// no endpoint is configured (or in tests). Counts drops so misconfiguration is
/// visible rather than silent.
#[derive(Default)]
pub struct NullTransport {
    dropped: std::sync::atomic::AtomicU64,
}

impl BatchTransport for NullTransport {
    fn send(&self, _partition: u64, _payload: Vec<u8>) {
        self.dropped
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl NullTransport {
    #[cfg(test)]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// One shard batch queued for HTTP delivery to the central exporter.
struct HttpBatch {
    partition: u64,
    payload: Vec<u8>,
}

/// Ships shard batches to a central exporter over HTTP `POST {endpoint}`.
///
/// The per-shard exporter runs on a plain OS thread (outside any tokio context)
/// and `reqwest` here is async-only, so delivery is driven by ONE dedicated
/// sender thread hosting a current-thread runtime. `send` enqueues onto a
/// bounded [`SyncSender`]; a full queue BLOCKS the caller (backpressure), never
/// drops — upholding the never-lose-a-batch invariant. A single consumer
/// preserves per-shard order. Failed POSTs are retried with a capped backoff so
/// a transient outage of the central service stalls (and backpressures) the
/// pipeline rather than losing data.
pub struct HttpBatchTransport {
    tx: std::sync::mpsc::SyncSender<HttpBatch>,
    depth: Arc<std::sync::atomic::AtomicUsize>,
}

impl HttpBatchTransport {
    /// Spawns the sender thread. `endpoint` is the central exporter's batch URL
    /// (e.g. `http://exporter-host:9200/ingest`); `capacity` bounds the in-RAM
    /// queue of undelivered batches (backpressure threshold).
    pub fn new(endpoint: String, capacity: usize) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel::<HttpBatch>(capacity.max(1));
        let depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let depth_bg = depth.clone();
        std::thread::Builder::new()
            .name("nanobpmn-exporter-http".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        tracing::error!("remote exporter: failed to build runtime: {e}");
                        return;
                    }
                };
                let client = reqwest::Client::new();
                rt.block_on(async move {
                    while let Ok(batch) = rx.recv() {
                        depth_bg.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                        let mut backoff = std::time::Duration::from_millis(50);
                        loop {
                            let res = client
                                .post(&endpoint)
                                .header("x-nano-partition", batch.partition)
                                .header(reqwest::header::CONTENT_TYPE, "application/json")
                                .body(batch.payload.clone())
                                .send()
                                .await;
                            match res {
                                Ok(r) if r.status().is_success() => break,
                                Ok(r) => tracing::warn!(
                                    "remote exporter: partition {} POST -> {}",
                                    batch.partition,
                                    r.status()
                                ),
                                Err(e) => tracing::warn!(
                                    "remote exporter: partition {} POST failed: {e}",
                                    batch.partition
                                ),
                            }
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(std::time::Duration::from_secs(5));
                        }
                    }
                });
            })
            .expect("spawn remote exporter thread");
        Self { tx, depth }
    }
}

impl BatchTransport for HttpBatchTransport {
    fn send(&self, partition: u64, payload: Vec<u8>) {
        self.depth
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.tx.send(HttpBatch { partition, payload }).is_err() {
            // Receiver gone (shutdown): undo the depth bump.
            self.depth
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn queued(&self) -> usize {
        self.depth.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Remote-only projection sink: derive the engine-facing outcome locally from
/// the in-flight index, hand the batch to the transport, and advance only the
/// local shard's compaction watermark. See the module docs.
pub struct RemoteSink {
    partition: u64,
    index: InflightIndex,
    /// Local shard retained ONLY to persist `exported_position` (the journal
    /// compaction watermark). Never receives the projected payload — that is
    /// offloaded to the remote target — so its per-batch disk cost is a single
    /// integer `UPDATE`.
    watermark: Arc<ReadStore>,
    transport: Arc<dyn BatchTransport>,
}

impl RemoteSink {
    pub fn new(
        partition: u64,
        watermark: Arc<ReadStore>,
        transport: Arc<dyn BatchTransport>,
    ) -> Self {
        Self {
            partition,
            index: InflightIndex::new(),
            watermark,
            transport,
        }
    }

    /// Currently in-flight instance count on this shard (from the index).
    /// Exposed for the remote-exporter metrics wiring (M3, issue #133).
    #[allow(dead_code)]
    pub fn active_count(&self) -> usize {
        self.index.active_count()
    }
}

impl ProjectionSink for RemoteSink {
    fn export(&self, events: &[&Event]) -> anyhow::Result<ExportOutcome> {
        // Engine-facing outcome first: this drives terminal eviction and the
        // in-flight gauge, and must be exact and synchronous regardless of the
        // remote target's state.
        let outcome = self.index.apply(events);
        // Serialize as a JSON array of events (`&[&Event]` serializes as an
        // array of the referenced events) and hand to the transport. The
        // transport owns delivery ordering and backpressure.
        let payload = serde_json::to_vec(&events)?;
        self.transport.send(self.partition, payload);
        // Advance the compaction watermark by the batch's event count so the
        // journal can still compact past a handed-off prefix. Cheap integer
        // write — the payload never touches this shard.
        self.watermark.advance_exported(events.len())?;
        Ok(outcome)
    }

    // `prune_terminal` keeps the default no-op: the remote target owns retention
    // of its own store; this node holds no projected history to prune.
}

/// Tee sink: project to a local authoritative sink AND mirror each batch to a
/// remote transport. Returns the LOCAL outcome, so in-flight accounting,
/// terminal eviction, queries, and the compaction watermark are byte-for-byte as
/// the local sink alone — the remote copy is a decoupled downstream feed. The
/// remote side ships the serialized batch only; it does NOT touch the watermark
/// (the local sink owns it) and needs no in-flight index (the local outcome is
/// authoritative).
pub struct TeeSink {
    partition: u64,
    local: Arc<dyn ProjectionSink>,
    transport: Arc<dyn BatchTransport>,
}

impl TeeSink {
    pub fn new(
        partition: u64,
        local: Arc<dyn ProjectionSink>,
        transport: Arc<dyn BatchTransport>,
    ) -> Self {
        Self {
            partition,
            local,
            transport,
        }
    }
}

impl ProjectionSink for TeeSink {
    fn export(&self, events: &[&Event]) -> anyhow::Result<ExportOutcome> {
        // Local is authoritative: its result is what the engine acts on, and a
        // local failure must surface (retried by the exporter loop) so the
        // never-drop-a-batch invariant holds for the canonical read model. Only
        // mirror to the remote once the local projection succeeded.
        let outcome = self.local.export(events)?;
        match serde_json::to_vec(&events) {
            Ok(payload) => self.transport.send(self.partition, payload),
            Err(e) => {
                crate::metrics::record_read_model_export_retry();
                tracing::warn!("tee: remote mirror serialization failed (local unaffected): {e}");
            }
        }
        Ok(outcome)
    }

    fn prune_terminal(&self, max_keep: usize, max_delete: usize) -> anyhow::Result<usize> {
        // Retention applies to the local authoritative store; the remote target
        // manages its own history.
        self.local.prune_terminal(max_keep, max_delete)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nanobpmn_engine_core::Event;

    use super::*;

    fn created(key: Key) -> Event {
        Event::ProcessInstanceCreated {
            instance_key: key,
            process_id: "p".into(),
            variables: HashMap::new(),
            created_at: 0,
            tags: Vec::new(),
            business_id: None,
        }
    }

    fn completed(key: Key) -> Event {
        Event::ProcessInstanceCompleted { instance_key: key }
    }

    fn terminated(key: Key) -> Event {
        Event::ProcessInstanceTerminated { instance_key: key }
    }

    fn outcome_of(idx: &InflightIndex, evs: &[Event]) -> ExportOutcome {
        let refs: Vec<&Event> = evs.iter().collect();
        idx.apply(&refs)
    }

    #[test]
    fn create_then_complete_nets_to_zero_inflight() {
        let idx = InflightIndex::new();
        let o = outcome_of(&idx, &[created(1), created(2), completed(1)]);
        assert_eq!(o.inflight_delta, 1); // +2 creates, -1 completion
        assert_eq!(o.terminal_keys, vec![1]);
        assert_eq!(idx.active_count(), 1); // key 2 still active
    }

    #[test]
    fn redelivered_create_is_idempotent() {
        let idx = InflightIndex::new();
        assert_eq!(outcome_of(&idx, &[created(7)]).inflight_delta, 1);
        // Re-delivery of the same create must not move the gauge.
        let o = outcome_of(&idx, &[created(7)]);
        assert_eq!(o.inflight_delta, 0);
        assert!(o.terminal_keys.is_empty());
        assert_eq!(idx.active_count(), 1);
    }

    #[test]
    fn redelivered_terminal_is_idempotent() {
        let idx = InflightIndex::new();
        outcome_of(&idx, &[created(3)]);
        let first = outcome_of(&idx, &[completed(3)]);
        assert_eq!(first.inflight_delta, -1);
        assert_eq!(first.terminal_keys, vec![3]);
        // Re-delivered completion: already evicted, must be a no-op.
        let second = outcome_of(&idx, &[completed(3)]);
        assert_eq!(second.inflight_delta, 0);
        assert!(second.terminal_keys.is_empty());
        assert_eq!(idx.active_count(), 0);
    }

    #[test]
    fn terminated_counts_as_terminal() {
        let idx = InflightIndex::new();
        outcome_of(&idx, &[created(9)]);
        let o = outcome_of(&idx, &[terminated(9)]);
        assert_eq!(o.inflight_delta, -1);
        assert_eq!(o.terminal_keys, vec![9]);
    }

    #[test]
    fn terminal_without_matching_active_is_ignored() {
        let idx = InflightIndex::new();
        // Completion for a never-seen key: not a genuine transition.
        let o = outcome_of(&idx, &[completed(42)]);
        assert_eq!(o.inflight_delta, 0);
        assert!(o.terminal_keys.is_empty());
    }

    #[test]
    fn remote_sink_serializes_and_advances_watermark() {
        let store = Arc::new(ReadStore::open(None).expect("open in-memory store"));
        let transport = Arc::new(NullTransport::default());
        let sink = RemoteSink::new(0, store.clone(), transport.clone());
        let start = store.exported_position();
        let evs = [created(1), created(2), completed(1)];
        let refs: Vec<&Event> = evs.iter().collect();
        let o = sink.export(&refs).expect("remote export");
        assert_eq!(o.inflight_delta, 1);
        assert_eq!(o.terminal_keys, vec![1]);
        // Watermark advanced by event count; one batch shipped to transport.
        assert_eq!(store.exported_position(), start + 3);
        assert_eq!(transport.dropped(), 1);
    }

    #[test]
    fn tee_returns_local_outcome_and_mirrors() {
        let local = Arc::new(ReadStore::open(None).unwrap());
        let transport = Arc::new(NullTransport::default());
        let tee = TeeSink::new(
            0,
            local.clone() as Arc<dyn ProjectionSink>,
            transport.clone(),
        );
        let evs = [created(5)];
        let refs: Vec<&Event> = evs.iter().collect();
        let o = tee.export(&refs).expect("tee export");
        assert_eq!(o.inflight_delta, 1);
        // Local projected (watermark advanced exactly once — no double count).
        assert_eq!(local.exported_position(), 1);
        // Batch mirrored to the transport exactly once.
        assert_eq!(transport.dropped(), 1);
    }
}
