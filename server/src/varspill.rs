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
//! `INCREMENTAL` auto-vacuum mode and, on the batched eviction path, hands freed
//! pages back to the OS once enough have accumulated to be worth a copy-back (see
//! [`crate::sqlite_space`]). The file then tracks the *live* backlog rather than the
//! historical peak. `NANOBPMN_VARSPILL_RECLAIM_MB` tunes the gate (0 disables).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use nanobpmn_engine_core::{InstanceSnapshot, Key, Value};
use rusqlite::{Connection, OptionalExtension, params};

use crate::sqlite_space::{enable_incremental_auto_vacuum, page_stats, reclaim_freelist};

/// Freelist bytes that must accumulate before [`VarSpillStore::forget`] spends an
/// `incremental_vacuum` + `wal_checkpoint(TRUNCATE)` to return them to the OS.
/// Default 64 MiB — high enough that steady-state churn (bounded live backlog)
/// rarely triggers a reclaim, low enough to cap the file near `live + 64 MiB`
/// rather than the historical peak. `NANOBPMN_VARSPILL_RECLAIM_MB` overrides;
/// 0 disables reclaim entirely (pre-fix high-water behaviour, for A/B).
fn reclaim_threshold_bytes() -> u64 {
    std::env::var("NANOBPMN_VARSPILL_RECLAIM_MB")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(64)
        * 1024
        * 1024
}

/// A SQLite-backed key → variables map for spilled instance payloads.
pub struct VarSpillStore {
    conn: Mutex<Connection>,
    /// Freelist byte gate for on-eviction reclaim; 0 disables it.
    reclaim_threshold_bytes: u64,
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
        let conn = match path {
            Some(p) => Connection::open(p)?,
            None => Connection::open_in_memory()?,
        };
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS spill (key INTEGER PRIMARY KEY, vars TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS cold (key INTEGER PRIMARY KEY, snapshot TEXT NOT NULL);
             DELETE FROM spill;
             DELETE FROM cold;",
        )?;
        // Convert to INCREMENTAL auto-vacuum now that the tables are empty (the
        // wipe above), so the VACUUM is near-free and later evictions can hand
        // freed pages back to the OS instead of plateauing at the high-water mark.
        enable_incremental_auto_vacuum(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            reclaim_threshold_bytes,
        })
    }

    /// Persists `vars` under `key`, replacing any prior payload.
    pub fn put(&self, key: Key, vars: &HashMap<String, Value>) -> rusqlite::Result<()> {
        let json = serde_json::to_string(vars).expect("variables serialize to JSON");
        let conn = self.conn.lock().expect("spill store poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO spill (key, vars) VALUES (?1, ?2)",
            params![key as i64, json],
        )?;
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
        serde_json::from_str(&json).ok()
    }

    /// Drops any spilled variable and cold-snapshot rows for `keys`, in one
    /// transaction. Called when instances reach a terminal state and are evicted
    /// from hot state: their spilled payloads are now dead and would otherwise
    /// accumulate as orphan rows (the store is destructive only on *rehydration*,
    /// and a terminal instance is never rehydrated). Absent keys are no-ops, so
    /// this is safe to call for every evicted instance whether or not it spilled.
    pub fn forget(&self, keys: &[Key]) {
        if keys.is_empty() {
            return;
        }
        let mut conn = self.conn.lock().expect("spill store poisoned");
        let tx = match conn.transaction() {
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
            for &key in keys {
                let _ = del_spill.execute(params![key as i64]);
                let _ = del_cold.execute(params![key as i64]);
            }
        }
        let _ = tx.commit();
        // Terminal eviction is the natural, batched moment to return freed pages
        // to the OS: `take` deletions (rehydration) also land on the freelist and
        // are reclaimed here. Gated so this runs only when enough space has piled
        // up to be worth the copy-back — keeping the file near the live backlog
        // instead of the historical high-water mark.
        if self.reclaim_threshold_bytes > 0 {
            let _ = reclaim_freelist(&conn, self.reclaim_threshold_bytes);
        }
    }

    /// The store's on-disk size as `(file_bytes, live_bytes)` (see
    /// [`crate::sqlite_space::page_stats`]): `file_bytes` is the whole allocated
    /// file (freelist included), `live_bytes` the pages holding actual data. With
    /// incremental auto-vacuum + on-eviction reclaim, `file_bytes` tracks the live
    /// cold backlog rather than plateauing at the peak-backlog high-water.
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
    fn forget_reclaims_file_space_to_the_os() {
        // A file-backed store so we can observe the on-disk high-water shrink;
        // the reclaim gate is set to 1 byte so any freelist triggers it.
        let dir = std::env::temp_dir();
        let path = dir.join(format!("varspill-reclaim-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let store = VarSpillStore::open_with_reclaim_threshold(Some(&path), 1).unwrap();

        // Spill a batch large enough to grow the file well past its empty size.
        let big = "x".repeat(8 * 1024);
        let keys: Vec<Key> = (0..4000).collect();
        for &k in &keys {
            store.put(k, &vars(&big)).unwrap();
        }
        let (file_peak, live_peak) = store.db_page_stats();
        assert!(live_peak > 0 && file_peak > 0);

        // Terminal eviction of the whole batch must hand the freed pages back.
        store.forget(&keys);
        let (file_after, live_after) = store.db_page_stats();

        assert!(
            live_after < live_peak / 4,
            "live bytes should collapse after forgetting the batch (peak={live_peak}, after={live_after})"
        );
        assert!(
            file_after < file_peak / 2,
            "file should shrink back toward the live set, not hold the high-water (peak={file_peak}, after={file_after})"
        );

        drop(store);
        let _ = std::fs::remove_file(&path);
    }
}
