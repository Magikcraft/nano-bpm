//! A disk-backed store for spilled process-instance variables.
//!
//! The dominant cost of a large *active* backlog (instances created and parked
//! on a job, waiting for a worker) is the 50 KB-class `variables` payload each
//! one holds in hot RAM. [`crate::Journal`] moves the variables of such cold
//! instances here, out of the engine, and rehydrates them at job-activation
//! time. This bounds resident memory the way Zeebe's RocksDB-backed state does,
//! while keeping in-memory speed for the working set.
//!
//! The backing store is a single SQLite table. SQLite's page cache (plus the OS
//! page cache underneath it) gives the tiering for free: a working set that fits
//! the cache is served at memory speed, and only a genuinely large spill touches
//! the disk — exactly the "memory/fs fusion" the spill is after, with no manual
//! eviction logic. `synchronous=NORMAL` is safe here because the spill store is a
//! *derived* cache: the variables are already durable in the journal (and the
//! read model), so a lost spill page is reconstructable, never authoritative.
//!
//! ## Returning freed space to the OS
//!
//! Destructive reads ([`VarSpillStore::take`]) and terminal eviction
//! ([`VarSpillStore::forget`]) keep the *live* row set bounded to the still-cold
//! backlog — but SQLite never returns freed pages to the OS on its own, so the
//! *file* would otherwise plateau at the high-water mark of the largest backlog
//! ever seen (a single spike parks ~payload × peak-backlog on disk for the life of
//! the process — e.g. 100+ GB after one bad soak). The store therefore runs in
//! `INCREMENTAL` auto-vacuum mode so freed pages land on a reclaimable freelist.
//!
//! Reclaiming that freelist is fsync-heavy (`wal_checkpoint(TRUNCATE)` +
//! `incremental_vacuum` copy-back). Doing it inline on the eviction path was a
//! throughput regression: on a shared disk those fsyncs contend with the engine's
//! raft-log fsync and repeatedly trip its ADR-0020 saturation guard, so creates get
//! shed under sustained load. Instead a **background reclaim thread** drains the
//! freelist only while the store is *quiescent* — which is exactly the post-spike
//! moment the reclaim targets (the huge backlog has drained; nothing is spilling or
//! rehydrating). Under steady load the store is never idle, so the thread never
//! fires and adds zero fsync pressure; the file simply tracks the live working set.
//! The drain runs in bounded passes, releasing the connection lock between them, so
//! resuming load preempts it immediately.
//!
//! `NANOBPMN_VARSPILL_RECLAIM_MB` sets the freelist gate (0 disables reclaim
//! entirely, and skips the thread). `NANOBPMN_VARSPILL_RECLAIM_IDLE_MS` sets how
//! long the store must be quiet before a drain begins.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use nanobpmn_engine_core::{InstanceSnapshot, Key, Value};
use rusqlite::{Connection, OptionalExtension, params};

use crate::sqlite_space::{
    enable_incremental_auto_vacuum, freelist_bytes, page_stats, reclaim_freelist_step,
};

/// Freelist bytes that must accumulate before the background reclaim thread spends
/// an `incremental_vacuum` + `wal_checkpoint(TRUNCATE)` to return them to the OS.
/// Default 64 MiB — high enough that steady-state churn (bounded live backlog)
/// never crosses it, low enough to cap a drained file near `live + 64 MiB` rather
/// than the historical peak. `NANOBPMN_VARSPILL_RECLAIM_MB` overrides; 0 disables
/// reclaim entirely (pre-fix high-water behaviour, and skips the thread).
fn reclaim_threshold_bytes() -> u64 {
    std::env::var("NANOBPMN_VARSPILL_RECLAIM_MB")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(64)
        * 1024
        * 1024
}

/// How long the store must be free of mutations before the background thread starts
/// draining the freelist, in milliseconds. Keeps reclaim off active-load windows so
/// its fsyncs never contend with the engine's raft-log fsync. Default 2000 ms.
fn reclaim_idle_ms() -> u64 {
    std::env::var("NANOBPMN_VARSPILL_RECLAIM_IDLE_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(2000)
}

/// Rows the forget worker deletes between its own `wal_checkpoint(TRUNCATE)`
/// backstop passes. With `wal_autocheckpoint=0` the WAL only truncates off the
/// hot path; this bounds WAL growth under sustained retirement (when the idle
/// reclaim thread never runs) without checkpointing so often that its fsync
/// contends with live raft-log fsync. Tunable via NANOBPMN_VARSPILL_WAL_CKPT_KEYS.
fn wal_checkpoint_keys() -> u64 {
    std::env::var("NANOBPMN_VARSPILL_WAL_CKPT_KEYS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(262_144)
}

/// Pages reclaimed per bounded background pass (~256 MiB at a 4 KiB page). Small
/// enough that a pass holds the connection lock only briefly (so resuming load
/// preempts a drain), large enough to clear even a 100 GB post-spike freelist within
/// a couple of minutes of idle.
const RECLAIM_CHUNK_PAGES: u32 = 65_536;

/// Keys deleted per background-forget transaction. Small enough that the
/// forget worker holds the connection lock only briefly per commit, so it
/// interleaves with the engine actor's own spill/rehydrate access instead of
/// stalling it with one huge multi-hundred-thousand-row transaction.
const FORGET_CHUNK: usize = 4_096;

/// Background thread that applies retirement/eviction `forget` deletes off the
/// engine actor. The follower retirement backstop can reap up to 100k cold
/// victims per tick; deleting them inline on the single-writer replica actor
/// (a synchronous SQLite transaction that fsyncs) stalls live raft replication
/// and roughly halves 50 KB-payload throughput. Offloading the deletes here
/// keeps the actor free while still reclaiming the rows (keys are unique and
/// never reused, so a queued forget can never clobber a later re-spill).
struct ForgetWorker {
    /// `None` after [`Drop`] has closed the channel. Sending a batch is a cheap,
    /// non-blocking handoff; the worker drains and deletes in [`FORGET_CHUNK`]s.
    tx: Option<Sender<ForgetMsg>>,
    handle: Option<JoinHandle<()>>,
}

/// Work item for the [`ForgetWorker`]: a batch of keys to delete, or a flush
/// barrier that acks once every batch queued before it has been applied (used by
/// tests and graceful shutdown to observe the async deletes deterministically).
enum ForgetMsg {
    Keys(Vec<Key>),
    Flush(Sender<()>),
}

impl Drop for ForgetWorker {
    fn drop(&mut self) {
        // Closing the channel lets the worker drain any queued batches (so no
        // forget is lost on shutdown) and then exit its `recv` loop; join it.
        drop(self.tx.take());
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Background thread that drains the SQLite freelist while the store is quiescent.
struct ReclaimWorker {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Drop for ReclaimWorker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// A SQLite-backed key → variables map for spilled instance payloads.
pub struct VarSpillStore {
    conn: Arc<Mutex<Connection>>,
    /// Mutation counter shared by [`put`](Self::put) / [`take`](Self::take) /
    /// `forget` / the background forget worker; the reclaim thread treats a stable
    /// count as "idle" and holds off its fsync-heavy vacuum while the store churns.
    activity: Arc<AtomicU64>,
    /// Present iff reclaim is enabled (file-backed store with a non-zero gate).
    /// Held only to keep the background reclaim thread alive; its [`Drop`] stops
    /// and joins the thread.
    #[allow(dead_code)]
    reclaim: Option<ReclaimWorker>,
    /// Present iff file-backed: applies retirement/eviction forgets off the actor.
    /// Absent for in-memory stores (tests), where `forget_async` runs inline so
    /// assertions observe the delete synchronously.
    forget: Option<ForgetWorker>,
}

impl VarSpillStore {
    /// Opens (creating if absent) the spill store at `path`, or an in-memory
    /// store when `path` is `None` (tests / ephemeral runs).
    pub fn open(path: Option<&Path>) -> rusqlite::Result<Self> {
        Self::open_with_reclaim_threshold(path, reclaim_threshold_bytes())
    }

    /// [`open`](Self::open) with an explicit freelist reclaim gate, so tests can
    /// force reclaim without racing the process-global `NANOBPMN_VARSPILL_RECLAIM_MB`.
    fn open_with_reclaim_threshold(
        path: Option<&Path>,
        reclaim_threshold_bytes: u64,
    ) -> rusqlite::Result<Self> {
        Self::open_with_params(path, reclaim_threshold_bytes, reclaim_idle_ms())
    }

    /// Full-control constructor (gate + idle window), used by tests to drive the
    /// background reclaimer deterministically without touching process-global env.
    fn open_with_params(
        path: Option<&Path>,
        reclaim_threshold_bytes: u64,
        idle_ms: u64,
    ) -> rusqlite::Result<Self> {
        let conn = match path {
            Some(p) => Connection::open(p)?,
            None => Connection::open_in_memory()?,
        };
        conn.execute_batch(
            // wal_autocheckpoint=0 disables SQLite's implicit 1000-page checkpoint.
            // Under sustained retirement the forget worker deletes at a high rate;
            // an auto-checkpoint fires an fsync-heavy `wal_checkpoint` on that
            // thread, and because the spill file shares the physical disk with the
            // raft log, that checkpoint I/O inflates live raft-log fsync latency
            // ~7x (measured), halving create throughput. With auto-checkpoint off,
            // deletes are cheap WAL appends (synchronous=NORMAL never fsyncs a
            // commit); the WAL is truncated off the hot path — by the idle-gated
            // reclaim thread, and by the forget worker's own size-gated backstop
            // (see spawn_forget_worker) so it can never grow without bound.
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE IF NOT EXISTS spill (key INTEGER PRIMARY KEY, vars TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS cold (key INTEGER PRIMARY KEY, snapshot TEXT NOT NULL);
             DELETE FROM spill;
             DELETE FROM cold;",
        )?;
        // Convert to INCREMENTAL auto-vacuum now that the tables are empty (the
        // wipe above), so the VACUUM is near-free and freed pages can later be
        // handed back to the OS instead of plateauing at the high-water mark.
        enable_incremental_auto_vacuum(&conn)?;
        let conn = Arc::new(Mutex::new(conn));
        let activity = Arc::new(AtomicU64::new(0));

        // Spawn the background reclaim thread only when it can do useful work: an
        // in-memory store has no file to shrink, and a zero gate disables reclaim.
        let reclaim = if path.is_some() && reclaim_threshold_bytes > 0 {
            Some(Self::spawn_reclaim_worker(
                Arc::clone(&conn),
                Arc::clone(&activity),
                reclaim_threshold_bytes,
                idle_ms,
            ))
        } else {
            None
        };
        // Offload retirement/eviction forgets off the actor for file-backed stores.
        // In-memory stores forget inline (see `forget_async`) so tests stay
        // deterministic and there is no cross-actor contention to relieve.
        let forget = if path.is_some() {
            Some(Self::spawn_forget_worker(
                Arc::clone(&conn),
                Arc::clone(&activity),
            ))
        } else {
            None
        };
        Ok(Self {
            conn,
            activity,
            reclaim,
            forget,
        })
    }

    /// Starts the idle-gated background freelist drainer. It polls activity; once the
    /// store has been quiet for `idle_ms` and the freelist exceeds `threshold_bytes`,
    /// it reclaims in [`RECLAIM_CHUNK_PAGES`]-sized passes, re-checking for resumed
    /// activity between passes so live load preempts the drain.
    fn spawn_reclaim_worker(
        conn: Arc<Mutex<Connection>>,
        activity: Arc<AtomicU64>,
        threshold_bytes: u64,
        idle_ms: u64,
    ) -> ReclaimWorker {
        let stop = Arc::new(AtomicBool::new(false));
        let poll = Duration::from_millis((idle_ms / 2).clamp(100, 1000));
        let idle_ticks = (idle_ms as f64 / poll.as_millis() as f64).ceil().max(1.0) as u32;

        let handle = {
            let activity = Arc::clone(&activity);
            let stop = Arc::clone(&stop);
            std::thread::Builder::new()
                .name("varspill-reclaim".into())
                .spawn(move || {
                    let mut last_seen = activity.load(Ordering::Relaxed);
                    let mut quiet = 0u32;
                    while !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(poll);
                        let now = activity.load(Ordering::Relaxed);
                        if now != last_seen {
                            last_seen = now;
                            quiet = 0;
                            continue;
                        }
                        quiet = quiet.saturating_add(1);
                        if quiet < idle_ticks {
                            continue;
                        }
                        // Quiescent: drain one bounded pass. Skip cheaply if there is
                        // nothing worth a copy-back. Any activity during/after the
                        // pass resets the idle counter above on the next tick.
                        let Ok(conn) = conn.lock() else { return };
                        if freelist_bytes(&conn) >= threshold_bytes {
                            let _ = reclaim_freelist_step(&conn, RECLAIM_CHUNK_PAGES);
                        }
                    }
                })
                .expect("spawn varspill-reclaim thread")
        };
        ReclaimWorker {
            stop,
            handle: Some(handle),
        }
    }

    /// Records a mutation so the background reclaimer holds off while the store is
    /// active. Cheap (a relaxed increment).
    #[inline]
    fn note_activity(&self) {
        self.activity.fetch_add(1, Ordering::Relaxed);
    }

    /// Starts the background forget worker: receives batches of keys and deletes
    /// their spill/cold rows off the engine actor, in [`FORGET_CHUNK`]-sized
    /// transactions so each commit holds the connection lock only briefly.
    fn spawn_forget_worker(conn: Arc<Mutex<Connection>>, activity: Arc<AtomicU64>) -> ForgetWorker {
        let (tx, rx) = channel::<ForgetMsg>();
        let handle = std::thread::Builder::new()
            .name("varspill-forget".into())
            .spawn(move || {
                // With wal_autocheckpoint disabled, the WAL only truncates off the
                // hot path. The idle-gated reclaim thread handles the post-load
                // drain, but under *sustained* retirement (never idle) the WAL would
                // grow unbounded, so the worker itself truncates once it has deleted
                // WAL_CKPT_KEYS rows since the last checkpoint. That bound is large
                // and the checkpoint infrequent, so its fsync barely perturbs raft
                // fsync (unlike the per-1000-page auto-checkpoint it replaces), while
                // still capping WAL disk growth and keeping cold reads fast.
                let ckpt_keys = wal_checkpoint_keys();
                let mut since_ckpt: u64 = 0;
                // Exits when the channel closes (every sender dropped), after
                // draining any batches still queued — so no forget is lost.
                while let Ok(msg) = rx.recv() {
                    match msg {
                        ForgetMsg::Keys(keys) => {
                            let n = keys.len() as u64;
                            // Bump activity *before* the delete so a long batch keeps
                            // the store non-idle for the whole time it holds the
                            // connection, preventing the reclaim worker from starting
                            // a pass while deletes are in-flight.
                            activity.fetch_add(1, Ordering::Relaxed);
                            Self::delete_keys(&conn, &keys);
                            since_ckpt = since_ckpt.saturating_add(n);
                            if since_ckpt >= ckpt_keys {
                                since_ckpt = 0;
                                if let Ok(guard) = conn.lock() {
                                    let _ = guard.query_row(
                                        "PRAGMA wal_checkpoint(TRUNCATE)",
                                        [],
                                        |_| Ok(()),
                                    );
                                }
                            }
                        }
                        ForgetMsg::Flush(ack) => {
                            let _ = ack.send(());
                        }
                    }
                }
            })
            .expect("spawn varspill-forget thread");
        ForgetWorker {
            tx: Some(tx),
            handle: Some(handle),
        }
    }

    /// Deletes the spill and cold rows for `keys`, chunked into short transactions
    /// so a large batch never holds the connection lock (and its fsync) for long.
    fn delete_keys(conn: &Mutex<Connection>, keys: &[Key]) {
        for chunk in keys.chunks(FORGET_CHUNK) {
            // A poisoned lock means another thread panicked mid-mutation, leaving the
            // connection in an unknown state; treat it as fatal (matching `put`/`take`)
            // rather than silently skipping deletes and leaking orphan cold rows.
            let mut guard = conn.lock().expect("spill store poisoned");
            let tx = match guard.transaction() {
                Ok(tx) => tx,
                Err(_) => return,
            };
            {
                let mut del_spill = match tx.prepare_cached("DELETE FROM spill WHERE key = ?1") {
                    Ok(s) => s,
                    Err(_) => return,
                };
                let mut del_cold = match tx.prepare_cached("DELETE FROM cold WHERE key = ?1") {
                    Ok(s) => s,
                    Err(_) => return,
                };
                for &key in chunk {
                    let _ = del_spill.execute(params![key as i64]);
                    let _ = del_cold.execute(params![key as i64]);
                }
            }
            let _ = tx.commit();
        }
    }

    /// Persists `vars` under `key`, replacing any prior payload.
    pub fn put(&self, key: Key, vars: &HashMap<String, Value>) -> rusqlite::Result<()> {
        let json = serde_json::to_string(vars).expect("variables serialize to JSON");
        let conn = self.conn.lock().expect("spill store poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO spill (key, vars) VALUES (?1, ?2)",
            params![key as i64, json],
        )?;
        drop(conn);
        self.note_activity();
        Ok(())
    }

    /// Removes and returns the payload for `key`, or `None` if absent. A spilled
    /// instance is rehydrated exactly once (on activation), so taking the row on
    /// read keeps the store bounded to the still-cold backlog.
    pub fn take(&self, key: Key) -> Option<HashMap<String, Value>> {
        let conn = self.conn.lock().expect("spill store poisoned");
        let json: Option<String> = conn
            .query_row(
                "SELECT vars FROM spill WHERE key = ?1",
                params![key as i64],
                |r| r.get(0),
            )
            .optional()
            .ok()?;
        let json = json?;
        let _ = conn.execute("DELETE FROM spill WHERE key = ?1", params![key as i64]);
        drop(conn);
        self.note_activity();
        serde_json::from_str(&json).ok()
    }

    /// Persists a whole-instance cold [`InstanceSnapshot`] under `key`, replacing
    /// any prior snapshot. Shares the connection (and thus the WAL) with the
    /// variable spill above, so the two tiers live in one file and one durability
    /// story — the reason cold spill reuses this store rather than a second DB.
    pub fn put_cold(&self, key: Key, snapshot: &InstanceSnapshot) -> rusqlite::Result<()> {
        let json = serde_json::to_string(snapshot).expect("snapshot serializes to JSON");
        let conn = self.conn.lock().expect("spill store poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO cold (key, snapshot) VALUES (?1, ?2)",
            params![key as i64, json],
        )?;
        drop(conn);
        self.note_activity();
        Ok(())
    }

    /// Removes and returns the cold snapshot for `key`, or `None` if absent.
    /// Destructive on read (like [`take`](VarSpillStore::take)): rehydrating an
    /// instance takes its snapshot back out, so the cold table holds only the
    /// still-dormant backlog.
    pub fn take_cold(&self, key: Key) -> Option<InstanceSnapshot> {
        let conn = self.conn.lock().expect("spill store poisoned");
        let json: Option<String> = conn
            .query_row(
                "SELECT snapshot FROM cold WHERE key = ?1",
                params![key as i64],
                |r| r.get(0),
            )
            .optional()
            .ok()?;
        let json = json?;
        let _ = conn.execute("DELETE FROM cold WHERE key = ?1", params![key as i64]);
        drop(conn);
        self.note_activity();
        serde_json::from_str(&json).ok()
    }

    /// Drops any spilled variable and cold-snapshot rows for `keys`, in one
    /// transaction. Called when instances reach a terminal state and are evicted
    /// from hot state: their spilled payloads are now dead and would otherwise
    /// accumulate as orphan rows (the store is destructive only on *rehydration*,
    /// and a terminal instance is never rehydrated). Absent keys are no-ops, so
    /// this is safe to call for every evicted instance whether or not it spilled.
    ///
    /// The freed pages land on the freelist; returning them to the OS is left to the
    /// background reclaim thread, which drains only while the store is quiescent — so
    /// this eviction path stays free of the fsync-heavy vacuum/checkpoint that would
    /// otherwise contend with the engine's raft-log fsync under load.
    pub fn forget(&self, keys: &[Key]) {
        if keys.is_empty() {
            return;
        }
        Self::delete_keys(&self.conn, keys);
        self.note_activity();
    }

    /// Like [`forget`](Self::forget), but hands the deletes to the background
    /// [`ForgetWorker`] instead of running them on the caller's thread. Used by the
    /// follower retirement paths ([`crate::journal::Journal::retire_below`] /
    /// `retire_instances`), which run on the single-writer replica actor and would
    /// otherwise stall live raft replication with a large synchronous DELETE
    /// transaction every retirement tick. Falls back to a synchronous
    /// [`forget`](Self::forget) for in-memory stores (no worker) so tests observe
    /// the delete immediately; keys are unique and never reused, so a queued forget
    /// can never clobber a later re-spill of the same key.
    pub fn forget_async(&self, keys: &[Key]) {
        if keys.is_empty() {
            return;
        }
        match self.forget.as_ref().and_then(|w| w.tx.as_ref()) {
            // If the worker has gone away, fall back to a synchronous delete.
            Some(tx) if tx.send(ForgetMsg::Keys(keys.to_vec())).is_ok() => {}
            _ => self.forget(keys),
        }
    }

    /// Blocks until every batch queued on the background forget worker before this
    /// call has been applied. A no-op for in-memory stores (forgets run inline).
    /// Used by tests and graceful shutdown to make the async deletes observable.
    pub fn flush_forgets(&self) {
        let Some(tx) = self.forget.as_ref().and_then(|w| w.tx.as_ref()) else {
            return;
        };
        let (ack_tx, ack_rx) = channel();
        if tx.send(ForgetMsg::Flush(ack_tx)).is_ok() {
            let _ = ack_rx.recv();
        }
    }

    /// The store's on-disk size as `(file_bytes, live_bytes)` (see
    /// [`crate::sqlite_space::page_stats`]): `file_bytes` is the whole allocated
    /// file (freelist included), `live_bytes` the pages holding actual data. With
    /// incremental auto-vacuum + background reclaim, `file_bytes` settles toward the
    /// live cold backlog once a spike drains, rather than plateauing at the peak.
    pub fn db_page_stats(&self) -> (u64, u64) {
        let conn = self.conn.lock().expect("spill store poisoned");
        page_stats(&conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(payload: &str) -> HashMap<String, Value> {
        let mut m = HashMap::new();
        m.insert("data".to_string(), Value::Str(payload.to_string()));
        m
    }

    #[test]
    fn round_trips_a_payload() {
        let store = VarSpillStore::open(None).unwrap();
        store.put(7, &vars("hello")).unwrap();
        let got = store.take(7).expect("payload present");
        assert_eq!(got.get("data"), Some(&Value::Str("hello".to_string())));
    }

    #[test]
    fn take_is_destructive_and_absent_is_none() {
        let store = VarSpillStore::open(None).unwrap();
        store.put(1, &vars("x")).unwrap();
        assert!(store.take(1).is_some());
        assert!(store.take(1).is_none(), "second take sees nothing");
        assert!(store.take(999).is_none(), "absent key is None");
    }

    #[test]
    fn cold_snapshot_round_trips() {
        use std::sync::Arc;

        use nanobpmn_engine_core::{ProcessInstance, ProcessInstanceState};

        let store = VarSpillStore::open(None).unwrap();
        let snapshot = InstanceSnapshot {
            instance: ProcessInstance {
                key: 42,
                process_id: "order".to_string(),
                state: ProcessInstanceState::Active,
                created_at: 1,
                tags: Vec::new(),
                business_id: None,
                active: HashMap::new(),
                scopes: HashMap::new(),
                variables: Arc::new(vars("payload")),
                join_counts: HashMap::new(),
                join_instances: HashMap::new(),
                incidents: Vec::new(),
                variables_spilled: false,
                multi_instances: HashMap::new(),
                adhoc_instances: HashMap::new(),
                scope_parents: HashMap::new(),
                scope_variables: HashMap::new(),
            },
            jobs: Vec::new(),
            timers: Vec::new(),
            message_subscriptions: Vec::new(),
            signal_subscriptions: Vec::new(),
            conditional_subscriptions: Vec::new(),
            user_tasks: Vec::new(),
            incidents: Vec::new(),
        };
        store.put_cold(42, &snapshot).unwrap();
        let got = store.take_cold(42).expect("snapshot present");
        assert_eq!(got, snapshot);
        assert!(store.take_cold(42).is_none(), "take_cold is destructive");
        assert!(store.take_cold(7).is_none(), "absent key is None");
    }

    #[test]
    fn forget_drops_spill_and_cold_rows_and_ignores_absent_keys() {
        let store = VarSpillStore::open(None).unwrap();
        store.put(1, &vars("a")).unwrap();
        store.put(2, &vars("b")).unwrap();
        store.put(3, &vars("c")).unwrap();

        // Forgetting terminal instances drops their rows; an absent key (99) is a
        // no-op, and an untouched key (3) survives.
        store.forget(&[1, 2, 99]);

        assert!(store.take(1).is_none(), "forgotten spill row gone");
        assert!(store.take(2).is_none(), "forgotten spill row gone");
        assert!(store.take(3).is_some(), "untouched spill row survives");

        // forget also clears the cold tier for the same key.
        store.put(4, &vars("d")).unwrap();
        store.forget(&[4]);
        assert!(store.take(4).is_none(), "forget clears spill tier for key");

        // Empty slice is a cheap no-op.
        store.forget(&[]);
    }

    #[test]
    fn forget_then_idle_reclaims_file_space_to_the_os() {
        // A file-backed store so we can observe the on-disk high-water shrink. The
        // reclaim gate is 1 byte (any freelist qualifies) and the idle window is
        // short so the background drainer fires quickly once we stop mutating.
        let dir = std::env::temp_dir();
        let path = dir.join(format!("varspill-reclaim-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let store = VarSpillStore::open_with_params(Some(&path), 1, 100).unwrap();

        // Spill a batch large enough to grow the file well past its empty size.
        let big = "x".repeat(8 * 1024);
        let keys: Vec<Key> = (0..4000).collect();
        for &k in &keys {
            store.put(k, &vars(&big)).unwrap();
        }
        let (file_peak, live_peak) = store.db_page_stats();
        assert!(live_peak > 0 && file_peak > 0);

        // Terminal eviction frees the rows onto the freelist but does NOT reclaim
        // inline any more — the file still holds the high-water right after forget.
        store.forget(&keys);
        let (file_right_after, live_after) = store.db_page_stats();
        assert!(
            live_after < live_peak / 4,
            "live bytes should collapse after forgetting the batch (peak={live_peak}, after={live_after})"
        );

        // Once the store goes quiet, the background reclaimer drains the freelist and
        // the file shrinks back toward the live set. Poll until it does (bounded).
        let mut file_after = file_right_after;
        for _ in 0..100 {
            std::thread::sleep(std::time::Duration::from_millis(50));
            file_after = store.db_page_stats().0;
            if file_after < file_peak / 2 {
                break;
            }
        }
        assert!(
            file_after < file_peak / 2,
            "background reclaim should shrink the file toward the live set once idle (peak={file_peak}, after={file_after})"
        );

        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn file_backed_store_disables_wal_autocheckpoint() {
        // Regression for the #287 50KB throughput regression: the forget worker's
        // cold-row DELETEs must never trigger SQLite's implicit 1000-page
        // wal_checkpoint, whose fsync (on the shared disk) inflates live raft-log
        // fsync ~7x. The store pins wal_autocheckpoint=0 so the WAL only truncates
        // off the hot path (idle reclaim + the size-gated forget-worker backstop).
        let dir = std::env::temp_dir();
        let path = dir.join(format!("varspill-noautockpt-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let store = VarSpillStore::open_with_params(Some(&path), 1, 100).unwrap();
        let autockpt: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row("PRAGMA wal_autocheckpoint", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            autockpt, 0,
            "auto-checkpoint must be disabled on the hot path"
        );

        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn forget_async_deletes_rows_off_thread_and_flush_makes_them_observable() {
        // A file-backed store spawns the background forget worker; `forget_async`
        // hands the deletes to it, and `flush_forgets` blocks until they land.
        let dir = std::env::temp_dir();
        let path = dir.join(format!("varspill-forget-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let store = VarSpillStore::open_with_params(Some(&path), 0, 2000).unwrap();
        store.put(1, &vars("a")).unwrap();
        store.put(2, &vars("b")).unwrap();
        store.put(3, &vars("c")).unwrap();

        // Enqueue the forget; the actual DELETEs run on the worker thread.
        store.forget_async(&[1, 2]);
        // A flush barrier acks only after every batch queued before it is applied.
        store.flush_forgets();

        assert!(
            store.take(1).is_none(),
            "async-forgotten row gone after flush"
        );
        assert!(
            store.take(2).is_none(),
            "async-forgotten row gone after flush"
        );
        assert!(store.take(3).is_some(), "untouched row survives");

        // Empty slice is a cheap no-op (no message queued).
        store.forget_async(&[]);
        store.flush_forgets();

        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn forget_async_falls_back_to_sync_for_in_memory_stores() {
        // In-memory stores have no worker; `forget_async` must delete inline so
        // callers/tests observe the effect immediately, with no flush needed.
        let store = VarSpillStore::open(None).unwrap();
        store.put(5, &vars("x")).unwrap();
        store.forget_async(&[5]);
        assert!(
            store.take(5).is_none(),
            "in-memory forget_async is synchronous"
        );
        // flush is a no-op when there is no worker.
        store.flush_forgets();
    }
}
