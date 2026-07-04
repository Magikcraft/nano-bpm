//! The SQLite-backed read model (the query side of a CQRS split).
//!
//! Camunda 8 keeps the broker's execution state separate from the data Operate
//! and the Query API read: an exporter streams the record log into an external
//! store, and reads are served from there, eventually consistent. This module is
//! that store for nanobpmn. The engine's hot [`crate::journal::Journal`] holds
//! only *live* execution state (completed instances are evicted once exported);
//! every `search*`/`get*` query is answered from here instead.
//!
//! The store is a pure projection of the engine's event stream: each event is
//! upserted into denormalized tables (so a row already carries the
//! process-definition identity a result needs, with no cross-table joins at read
//! time). Because it is fully derived, it can always be rebuilt by replaying the
//! journal — the journal remains the single source of truth. A persisted
//! `exported_position` lets a warm restart skip rows it already holds; if the
//! database is missing, stale, or a schema mismatch, it is recreated and
//! rebuilt from scratch.
//!
//! A non-persistent (`:memory:`) store backs the engine's in-memory mode so
//! reads still work while nothing is persisted.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use nanobpmn_engine_core::{
    Event, IncidentKind, IncidentState, JobState, Key, ProcessInstanceState, UserTaskState, Value,
    partition_of,
};
use rusqlite::{Connection, OptionalExtension, params};

/// Bumped whenever the schema or projection changes; a stored database with a
/// different version is dropped and rebuilt from the journal.
const SCHEMA_VERSION: i64 = 5;

const SCHEMA: &str = "
CREATE TABLE process_definitions (
    process_id TEXT PRIMARY KEY,
    key        INTEGER NOT NULL,
    version    INTEGER NOT NULL,
    xml        TEXT NOT NULL DEFAULT ''
);
CREATE TABLE process_instances (
    key                    INTEGER PRIMARY KEY,
    process_id             TEXT NOT NULL,
    process_definition_id  TEXT NOT NULL,
    process_definition_key TEXT NOT NULL,
    version                INTEGER NOT NULL,
    state                  INTEGER NOT NULL,
    start_date_ms          INTEGER NOT NULL,
    has_incident           INTEGER NOT NULL,
    tags                   TEXT NOT NULL,
    business_id            TEXT
);
CREATE TABLE jobs (
    key                    INTEGER PRIMARY KEY,
    instance_key           INTEGER NOT NULL,
    element_instance_key   INTEGER NOT NULL,
    element_id             TEXT NOT NULL,
    job_type               TEXT NOT NULL,
    state                  INTEGER NOT NULL,
    retries                INTEGER NOT NULL,
    worker                 TEXT,
    deadline_ms            INTEGER,
    process_definition_id  TEXT NOT NULL,
    process_definition_key TEXT NOT NULL
);
CREATE TABLE incidents (
    key                    INTEGER PRIMARY KEY,
    instance_key           INTEGER NOT NULL,
    element_instance_key   INTEGER NOT NULL,
    element_id             TEXT NOT NULL,
    kind                   INTEGER NOT NULL,
    state                  INTEGER NOT NULL,
    reason                 TEXT NOT NULL,
    job_key                INTEGER,
    created_at_ms          INTEGER NOT NULL,
    process_definition_id  TEXT NOT NULL,
    process_definition_key TEXT NOT NULL
);
CREATE TABLE meta (k TEXT PRIMARY KEY, v INTEGER NOT NULL);
CREATE TABLE user_tasks (
    key                    INTEGER PRIMARY KEY,
    instance_key           INTEGER NOT NULL,
    element_instance_key   INTEGER NOT NULL,
    element_id             TEXT NOT NULL,
    state                  INTEGER NOT NULL,
    assignee               TEXT,
    candidate_groups       TEXT NOT NULL DEFAULT '[]',
    candidate_users        TEXT NOT NULL DEFAULT '[]',
    due_date               TEXT,
    follow_up_date         TEXT,
    priority               INTEGER NOT NULL DEFAULT 50,
    created_at_ms          INTEGER NOT NULL,
    process_definition_id  TEXT NOT NULL,
    process_definition_key TEXT NOT NULL,
    process_definition_version INTEGER NOT NULL
);
CREATE TABLE variables (
    key                    INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_key           INTEGER NOT NULL,
    scope_key              INTEGER NOT NULL,
    name                   TEXT NOT NULL,
    value                  TEXT NOT NULL,
    process_definition_id  TEXT NOT NULL,
    process_definition_key TEXT NOT NULL,
    UNIQUE(scope_key, name)
);
";

// --- enum <-> integer code mappings (kept beside the engine enums) ---

fn instance_state_code(s: ProcessInstanceState) -> i64 {
    match s {
        ProcessInstanceState::Active => 0,
        ProcessInstanceState::Completed => 1,
        ProcessInstanceState::Terminated => 2,
    }
}
fn instance_state_from(code: i64) -> ProcessInstanceState {
    match code {
        1 => ProcessInstanceState::Completed,
        2 => ProcessInstanceState::Terminated,
        _ => ProcessInstanceState::Active,
    }
}

fn job_state_code(s: JobState) -> i64 {
    match s {
        JobState::Created => 0,
        JobState::Activated => 1,
        JobState::Failed => 2,
        JobState::Errored => 3,
        JobState::Completed => 4,
        JobState::Canceled => 5,
    }
}
fn job_state_from(code: i64) -> JobState {
    match code {
        1 => JobState::Activated,
        2 => JobState::Failed,
        3 => JobState::Errored,
        4 => JobState::Completed,
        5 => JobState::Canceled,
        _ => JobState::Created,
    }
}

fn user_task_state_code(s: UserTaskState) -> i64 {
    match s {
        UserTaskState::Created => 0,
        UserTaskState::Completed => 1,
        UserTaskState::Canceled => 2,
    }
}
fn user_task_state_from(code: i64) -> UserTaskState {
    match code {
        1 => UserTaskState::Completed,
        2 => UserTaskState::Canceled,
        _ => UserTaskState::Created,
    }
}

fn incident_state_code(s: IncidentState) -> i64 {
    match s {
        IncidentState::Active => 0,
        IncidentState::Resolved => 1,
    }
}
fn incident_state_from(code: i64) -> IncidentState {
    match code {
        1 => IncidentState::Resolved,
        _ => IncidentState::Active,
    }
}

fn incident_kind_code(k: IncidentKind) -> i64 {
    match k {
        IncidentKind::JobNoRetries => 0,
        IncidentKind::NoMatchingSequenceFlow => 1,
        IncidentKind::UnhandledError => 2,
        IncidentKind::ExpressionEvaluation => 3,
    }
}
fn incident_kind_from(code: i64) -> IncidentKind {
    match code {
        1 => IncidentKind::NoMatchingSequenceFlow,
        2 => IncidentKind::UnhandledError,
        3 => IncidentKind::ExpressionEvaluation,
        _ => IncidentKind::JobNoRetries,
    }
}

// --- denormalized read rows (carry everything a result projection needs) ---

pub struct ProcessInstanceRow {
    pub key: Key,
    pub process_id: String,
    pub process_definition_id: String,
    pub process_definition_key: String,
    pub version: i32,
    pub state: ProcessInstanceState,
    pub start_date_ms: u64,
    pub has_incident: bool,
    pub tags: Vec<String>,
    pub business_id: Option<String>,
}

pub struct JobRow {
    pub key: Key,
    pub instance_key: Key,
    pub element_instance_key: Key,
    pub element_id: String,
    pub job_type: String,
    pub state: JobState,
    pub retries: i32,
    pub worker: Option<String>,
    pub deadline_ms: Option<u64>,
    pub process_definition_id: String,
    pub process_definition_key: String,
}

pub struct UserTaskRow {
    pub key: Key,
    pub instance_key: Key,
    pub element_instance_key: Key,
    pub element_id: String,
    pub state: UserTaskState,
    pub assignee: Option<String>,
    pub candidate_groups: Vec<String>,
    pub candidate_users: Vec<String>,
    pub due_date: Option<String>,
    pub follow_up_date: Option<String>,
    pub priority: i32,
    pub created_at_ms: u64,
    pub process_definition_id: String,
    pub process_definition_key: String,
    pub process_definition_version: i32,
}

pub struct IncidentRow {
    pub key: Key,
    pub instance_key: Key,
    pub element_instance_key: Key,
    pub element_id: String,
    pub kind: IncidentKind,
    pub state: IncidentState,
    pub reason: String,
    pub job_key: Option<Key>,
    pub created_at_ms: u64,
    pub process_definition_id: String,
    pub process_definition_key: String,
}

pub struct ProcessDefinitionRow {
    pub key: Key,
    pub process_id: String,
    pub version: i32,
}

pub struct VariableRow {
    pub key: Key,
    pub instance_key: Key,
    pub scope_key: Key,
    pub name: String,
    /// The variable's value as a serialized-JSON string (e.g. `"text"`, `42`,
    /// `true`), mirroring Camunda's wire representation.
    pub value: String,
    pub process_definition_id: String,
    pub process_definition_key: String,
}

/// Reads a SQLite database's size as `(file_bytes, live_bytes)` from its header:
/// `file_bytes = page_count × page_size` (the whole allocated file, freelist
/// included) and `live_bytes = (page_count − freelist_count) × page_size` (the
/// pages holding actual data). All three PRAGMAs are O(1) header reads, so this
/// is cheap enough for the pruner's hot loop.
fn page_stats(conn: &Connection) -> (u64, u64) {
    let page_count: i64 = conn
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap_or(0);
    let freelist: i64 = conn
        .query_row("PRAGMA freelist_count", [], |r| r.get(0))
        .unwrap_or(0);
    let page_size: i64 = conn
        .query_row("PRAGMA page_size", [], |r| r.get(0))
        .unwrap_or(4096);
    let ps = page_size.max(0) as u64;
    let file = page_count.max(0) as u64 * ps;
    let live = (page_count - freelist).max(0) as u64 * ps;
    (file, live)
}

/// Evicts up to `batch` of the **oldest** terminal instances (and their child
/// rows) in a single small transaction, returning how many were deleted.
///
/// Unlike [`ReadStore::prune_terminal_instances`] this takes no keep-count and
/// does **no `OFFSET` scan**: `ORDER BY key ASC LIMIT batch` walks the primary
/// key index from the oldest key, and since the oldest instances are the ones
/// that completed long ago it collects a full batch after scanning ~`batch`
/// rows (plus any still-active stragglers). That makes each sweep O(batch)
/// rather than O(keep_target), so the decoupled pruner can outpace inserts even
/// while the exporter saturates the writer. Operates on a caller-owned
/// connection (the pruner's second connection to the shard) so it never blocks
/// the exporter's mutex; the two serialize only at SQLite's write lock, briefly,
/// per small batch.
fn prune_oldest_terminal(conn: &mut Connection, batch: usize) -> rusqlite::Result<usize> {
    if batch == 0 {
        return Ok(0);
    }
    let tx = conn.transaction()?;
    tx.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS _evict(key INTEGER PRIMARY KEY);
         DELETE FROM _evict;",
    )?;
    let evicted = tx.execute(
        "INSERT INTO _evict(key) \
         SELECT key FROM process_instances WHERE state IN (1, 2) \
         ORDER BY key ASC LIMIT ?1",
        params![batch as i64],
    )?;
    if evicted == 0 {
        tx.commit()?;
        return Ok(0);
    }
    for table in ["variables", "jobs", "incidents", "user_tasks"] {
        tx.execute(
            &format!("DELETE FROM {table} WHERE instance_key IN (SELECT key FROM _evict)"),
            [],
        )?;
    }
    tx.execute(
        "DELETE FROM process_instances WHERE key IN (SELECT key FROM _evict)",
        [],
    )?;
    tx.commit()?;
    Ok(evicted)
}

/// The read model. Wraps a single SQLite connection behind a mutex: SQLite
/// serializes writes anyway, and this keeps the projection (exporter thread) and
/// the queries (request handlers) on one shared database — including for the
/// `:memory:` backend, where separate connections would not see each other's
/// data. The mutex is independent of the engine lock, so reads never contend
/// with engine writes.
pub struct ReadStore {
    conn: Mutex<Connection>,
    /// The shard's on-disk path (None for `:memory:`). Retained so the decoupled
    /// adaptive pruner can open its own second connection to the same WAL file and
    /// evict on an independent schedule, rather than competing for CPU with
    /// projection inside the single exporter thread.
    path: Option<PathBuf>,
}

impl ReadStore {
    /// Opens the read store at `path`, or an in-memory database when `path` is
    /// `None`. A persistent database whose schema version does not match (or
    /// that cannot be read) is dropped and recreated, so a rebuild from the
    /// journal repopulates it.
    pub fn open(path: Option<&Path>) -> rusqlite::Result<Self> {
        let conn = match path {
            Some(p) => Connection::open(p)?,
            None => Connection::open_in_memory()?,
        };
        // The read store is a *derived* projection: on boot it is rebuilt from the
        // journal (the source of truth) by replaying events past `exported_position`,
        // so its own fsync durability is not load-bearing. For a file-backed store we
        // therefore run WAL + `synchronous=NORMAL`: this removes the per-commit fsync
        // media barrier from the exporter's hot loop (the single per-node projection
        // thread all partitions funnel through) while still surviving app crashes; an
        // OS/power loss at worst rewinds the projection, which boot catch-up rebuilds.
        // A tie in these numbers is the read-model exporter's throughput ceiling, so
        // this is the cheapest lever on it. `synchronous` can be overridden via
        // `NANOBPMN_READ_SYNC` (e.g. FULL to restore the old durability, OFF to
        // isolate fsync cost in a spike). In-memory stores skip this (no journal file).
        if path.is_some() {
            let sync = std::env::var("NANOBPMN_READ_SYNC").unwrap_or_else(|_| "NORMAL".into());
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", &sync)?;
            conn.busy_timeout(std::time::Duration::from_secs(5))?;
        }
        let store = Self {
            conn: Mutex::new(conn),
            path: path.map(|p| p.to_path_buf()),
        };
        store.ensure_schema()?;
        // A persistent store whose schema already matched is opened without any
        // write so far, so a read-only file (or directory) would not surface
        // until the first exporter batch — where it logs "attempt to write a
        // readonly database" every time and silently never advances the read
        // model. Probe writability now so that case fails fast at startup.
        if path.is_some() {
            store.check_writable()?;
        }
        Ok(store)
    }

    /// Performs a trivial no-op write to confirm the database (and the directory
    /// it lives in) are writable. A self-assignment changes no data but still
    /// opens a write transaction and creates the rollback journal, exercising
    /// both file and directory permissions.
    fn check_writable(&self) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.execute("UPDATE meta SET v = v WHERE k = 'schema_version'", [])?;
        Ok(())
    }

    fn ensure_schema(&self) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("read store poisoned");
        let version: Option<i64> = conn
            .query_row("SELECT v FROM meta WHERE k = 'schema_version'", [], |r| {
                r.get(0)
            })
            .optional()
            .unwrap_or(None);
        if version == Some(SCHEMA_VERSION) {
            return Ok(());
        }
        // Fresh, or a stale/foreign schema: (re)create from scratch.
        conn.execute_batch(
            "DROP TABLE IF EXISTS process_definitions;
             DROP TABLE IF EXISTS process_instances;
             DROP TABLE IF EXISTS jobs;
             DROP TABLE IF EXISTS incidents;
             DROP TABLE IF EXISTS user_tasks;
             DROP TABLE IF EXISTS variables;
             DROP TABLE IF EXISTS meta;",
        )?;
        conn.execute_batch(SCHEMA)?;
        conn.execute(
            "INSERT INTO meta (k, v) VALUES ('schema_version', ?1)",
            params![SCHEMA_VERSION],
        )?;
        conn.execute(
            "INSERT INTO meta (k, v) VALUES ('exported_position', 0)",
            [],
        )?;
        Ok(())
    }

    /// How many journal events have already been durably projected. A boot
    /// catch-up replays only events at or after this offset.
    pub fn exported_position(&self) -> usize {
        let conn = self.conn.lock().expect("read store poisoned");
        let v: i64 = conn
            .query_row(
                "SELECT v FROM meta WHERE k = 'exported_position'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        v.max(0) as usize
    }

    /// Drops and recreates the schema, resetting `exported_position` to 0. Used
    /// when the persisted position is ahead of the journal (a corrupt or
    /// truncated log), forcing a full rebuild by replay.
    pub fn reset(&self) -> rusqlite::Result<()> {
        {
            let conn = self.conn.lock().expect("read store poisoned");
            conn.execute_batch(
                "DROP TABLE IF EXISTS process_definitions;
                 DROP TABLE IF EXISTS process_instances;
                 DROP TABLE IF EXISTS jobs;
                 DROP TABLE IF EXISTS incidents;
             DROP TABLE IF EXISTS user_tasks;
                 DROP TABLE IF EXISTS variables;
                 DROP TABLE IF EXISTS meta;",
            )?;
        }
        self.ensure_schema()?;
        Ok(())
    }

    /// Projects a batch of consecutive journal `events` into the store in one
    /// transaction and advances `exported_position` by `events.len()`. Returns
    /// the keys of instances that completed in this batch, so the caller can
    /// evict them from hot engine state. Projection is idempotent, so replaying
    /// an overlapping prefix is safe. Takes event references so a caller batching
    /// several `Arc<Vec<Event>>` can project them without deep-copying payloads.
    pub fn export(&self, events: &[&Event]) -> rusqlite::Result<Vec<Key>> {
        let mut conn = self.conn.lock().expect("read store poisoned");
        let tx = conn.transaction()?;
        let mut completed = Vec::new();
        for &event in events {
            if let Event::ProcessInstanceCompleted { instance_key }
            | Event::ProcessInstanceTerminated { instance_key } = event
            {
                completed.push(*instance_key);
            }
            project(&tx, event)?;
        }
        tx.cexecute(
            "UPDATE meta SET v = v + ?1 WHERE k = 'exported_position'",
            params![events.len() as i64],
        )?;
        tx.commit()?;
        Ok(completed)
    }

    /// Caps retained *terminal* (Completed/Terminated) process instances at
    /// `max_keep`, deleting the oldest beyond the cap together with all their
    /// dependent rows (variables, jobs, incidents, user tasks). Active instances
    /// are never touched. Returns the number of instances evicted.
    ///
    /// This bounds read-model memory to the working set instead of letting it
    /// grow without limit with cumulative throughput: without it, every
    /// completed instance — and its full variable payload — is retained forever,
    /// so a long-running engine's memory climbs indefinitely even with no active
    /// processes. An in-memory (`:memory:`) store never returns freed pages to
    /// the OS, so the win is *prevention* — pruning continuously keeps the page
    /// arena from ballooning in the first place (freed pages are reused by new
    /// instances). `max_keep == 0` disables pruning (unbounded history, the
    /// default — see `NANOBPMN_HISTORY_MAX_INSTANCES`).
    ///
    /// Terminal instances are ordered by `key`, which is monotonic in creation
    /// order, so the most recently created terminal instances are retained.
    ///
    /// `max_delete` bounds a single sweep to the *oldest* `max_delete` terminal
    /// instances beyond `max_keep` (0 = unbounded). This is essential when a
    /// store that has grown well past budget first crosses the retention
    /// watermark: pruning the entire backlog in one transaction would build a
    /// multi-gigabyte WAL and block this shard's single exporter thread for
    /// seconds, during which the unbounded exporter channel backs up with events
    /// and process RSS explodes. Bounding each sweep keeps every prune transaction
    /// small and quick so the exporter stays responsive; a backlog is worked down
    /// gently across successive sweeps, while steady-state sweeps (only a
    /// prune-threshold's worth of new completions exceed `max_keep`) delete a
    /// small batch and the store holds flat. After a non-empty sweep the WAL is
    /// checkpoint-truncated so it does not accumulate the freed pages.
    pub fn prune_terminal_instances(
        &self,
        max_keep: usize,
        max_delete: usize,
    ) -> rusqlite::Result<usize> {
        if max_keep == 0 {
            return Ok(0);
        }
        let mut conn = self.conn.lock().expect("read store poisoned");
        let tx = conn.transaction()?;
        // Materialize the keys to evict: of the terminal instances beyond the most
        // recent `max_keep` (inner `LIMIT -1 OFFSET max_keep` = "all but the newest
        // max_keep"), take the OLDEST `max_delete` of them (outer `ORDER BY key ASC
        // LIMIT`). `max_delete == 0` => `LIMIT -1` (unbounded).
        tx.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS _evict(key INTEGER PRIMARY KEY);
             DELETE FROM _evict;",
        )?;
        let del_limit: i64 = if max_delete == 0 {
            -1
        } else {
            max_delete as i64
        };
        let evicted = tx.execute(
            "INSERT INTO _evict(key) \
             SELECT key FROM ( \
               SELECT key FROM process_instances WHERE state IN (1, 2) \
               ORDER BY key DESC LIMIT -1 OFFSET ?1 \
             ) ORDER BY key ASC LIMIT ?2",
            params![max_keep as i64, del_limit],
        )?;
        if evicted == 0 {
            tx.commit()?;
            return Ok(0);
        }
        for table in ["variables", "jobs", "incidents", "user_tasks"] {
            tx.execute(
                &format!("DELETE FROM {table} WHERE instance_key IN (SELECT key FROM _evict)"),
                [],
            )?;
        }
        tx.execute(
            "DELETE FROM process_instances WHERE key IN (SELECT key FROM _evict)",
            [],
        )?;
        tx.commit()?;
        // Return the WAL's freed pages to a bounded size. Without this the WAL
        // grows with each prune (hundreds of MB observed) and never shrinks while
        // the store is under load, inflating both disk and mapped memory. TRUNCATE
        // is best-effort: a concurrent reader can hold it back, and that is fine —
        // the next sweep retries.
        let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
        Ok(evicted)
    }

    // --- queries used by the search/get handlers ---

    /// The on-disk size of this shard's SQLite database in bytes, as
    /// `(file_bytes, live_bytes)`: `file_bytes` is the total allocated file
    /// (`page_count × page_size`, including freelist pages SQLite keeps for
    /// reuse and does not return to the OS without `VACUUM`); `live_bytes`
    /// excludes the freelist (`(page_count − freelist_count) × page_size`) and
    /// tracks the actual data. Adaptive retention keeps `live_bytes` near its
    /// budget by evicting old terminal instances; `file_bytes` stays at the
    /// high-watermark (freed pages are reused, not returned to the OS) and so
    /// plateaus rather than growing without bound.
    pub fn db_page_stats(&self) -> (u64, u64) {
        let conn = self.conn.lock().expect("read store poisoned");
        page_stats(&conn)
    }

    /// Opens a second connection to this shard's database file for the decoupled
    /// adaptive pruner (see [`prune_oldest_terminal`]). Returns `Ok(None)` for an
    /// in-memory store (a second connection would be a distinct empty database),
    /// so the caller keeps pruning inline in that case. WAL mode lets this
    /// connection's small delete transactions interleave with the exporter's
    /// insert transactions at SQLite's write-lock granularity; `busy_timeout`
    /// makes each side wait for the lock rather than erroring under contention.
    pub fn prune_connection(&self) -> rusqlite::Result<Option<Connection>> {
        let Some(path) = self.path.as_ref() else {
            return Ok(None);
        };
        let conn = Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        Ok(Some(conn))
    }

    /// Runs one adaptive-prune wake on the pruner's own `conn`. Cheap when under
    /// budget: a single O(1) `page_stats` read and return. When `live_bytes`
    /// reaches `high_bytes`, evicts the oldest terminal instances in `batch`
    /// chunks until `live_bytes` falls to `low_bytes` (hysteresis prevents
    /// per-insert thrashing), or `max_deletes` rows have been evicted this wake
    /// (bounds how long the write lock is held away from the exporter), or no
    /// terminal instances remain. Returns the number evicted; checkpoint-truncates
    /// the WAL if it deleted anything so freed pages do not accumulate there.
    pub fn adaptive_prune_once(
        conn: &mut Connection,
        high_bytes: u64,
        low_bytes: u64,
        batch: usize,
        max_deletes: usize,
    ) -> rusqlite::Result<usize> {
        let (_, live) = page_stats(conn);
        if live < high_bytes {
            return Ok(0);
        }
        let mut total = 0usize;
        while total < max_deletes {
            let (_, live) = page_stats(conn);
            if live <= low_bytes {
                break;
            }
            let want = batch.min(max_deletes - total);
            let evicted = prune_oldest_terminal(conn, want)?;
            if evicted == 0 {
                break;
            }
            total += evicted;
        }
        if total > 0 {
            let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
        }
        Ok(total)
    }

    /// Total number of process instances (active + terminal) in this shard, via
    /// a live `COUNT(*)`. This is O(rows), so the adaptive-retention caller must
    /// only invoke it when a shard is over its byte budget (never on the
    /// below-budget ramp) and throttle it in time — a naive event-count proxy is
    /// unsafe here because projection is idempotent/re-delivered, so counting
    /// `ProcessInstanceCreated` events over-counts the deduplicated rows and
    /// inflates the keep target until pruning silently evicts nothing.
    pub fn instance_count(&self) -> usize {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row("SELECT COUNT(*) FROM process_instances", [], |r| {
            r.get::<_, i64>(0)
        })
        .map(|n| n.max(0) as usize)
        .unwrap_or(0)
    }

    /// The number of non-terminal (Active) process instances currently in the
    /// read model. Used once at startup to seed the in-flight backpressure gauge
    /// after a journal replay, so the watermark reflects recovered work.
    pub fn active_instance_count(&self) -> usize {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT COUNT(*) FROM process_instances WHERE state = 0",
            [],
            |r| r.get::<_, i64>(0),
        )
        .map(|n| n as usize)
        .unwrap_or(0)
    }

    pub fn process_instances(&self) -> Vec<ProcessInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT key, process_id, process_definition_id, process_definition_key, \
                 version, state, start_date_ms, has_incident, tags, business_id FROM process_instances",
            )
            .expect("prepare process_instances");
        let rows = stmt
            .query_map([], map_instance)
            .expect("query process_instances");
        rows.filter_map(Result::ok).collect()
    }

    /// Total number of process instances in the read model — the page count for
    /// the console's paginated instance list. Cheap `COUNT(*)` on the table.
    pub fn process_instance_count(&self) -> i64 {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row("SELECT COUNT(*) FROM process_instances", [], |r| r.get(0))
            .unwrap_or(0)
    }

    /// One page of process instances, newest first. Orders by `key DESC` — keys
    /// are monotonic so this is newest-first (the same ordering the retention
    /// prune uses) and rides the integer PRIMARY KEY index, so it is
    /// `O(limit + offset)` in SQLite rather than loading and sorting every row
    /// in memory (which is what made the console hang on large datasets).
    pub fn process_instances_page(&self, limit: i64, offset: i64) -> Vec<ProcessInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT key, process_id, process_definition_id, process_definition_key, \
                 version, state, start_date_ms, has_incident, tags, business_id \
                 FROM process_instances ORDER BY key DESC LIMIT ?1 OFFSET ?2",
            )
            .expect("prepare process_instances_page");
        let rows = stmt
            .query_map(params![limit, offset], map_instance)
            .expect("query process_instances_page");
        rows.filter_map(Result::ok).collect()
    }

    pub fn process_instance(&self, key: Key) -> Option<ProcessInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT key, process_id, process_definition_id, process_definition_key, \
             version, state, start_date_ms, has_incident, tags, business_id FROM process_instances WHERE key = ?1",
            params![key as i64],
            map_instance,
        )
        .optional()
        .expect("query process_instance")
    }

    pub fn jobs(&self) -> Vec<JobRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT key, instance_key, element_instance_key, element_id, job_type, state, \
                 retries, worker, deadline_ms, process_definition_id, process_definition_key \
                 FROM jobs",
            )
            .expect("prepare jobs");
        let rows = stmt.query_map([], map_job).expect("query jobs");
        rows.filter_map(Result::ok).collect()
    }

    pub fn user_tasks(&self) -> Vec<UserTaskRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT key, instance_key, element_instance_key, element_id, state, \
                 assignee, candidate_groups, candidate_users, due_date, follow_up_date, \
                 priority, created_at_ms, process_definition_id, process_definition_key, \
                 process_definition_version \
                 FROM user_tasks",
            )
            .expect("prepare user_tasks");
        let rows = stmt.query_map([], map_user_task).expect("query user_tasks");
        rows.filter_map(Result::ok).collect()
    }

    pub fn incidents(&self) -> Vec<IncidentRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT key, instance_key, element_instance_key, element_id, kind, state, \
                 reason, job_key, created_at_ms, process_definition_id, process_definition_key \
                 FROM incidents",
            )
            .expect("prepare incidents");
        let rows = stmt.query_map([], map_incident).expect("query incidents");
        rows.filter_map(Result::ok).collect()
    }

    pub fn incident(&self, key: Key) -> Option<IncidentRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT key, instance_key, element_instance_key, element_id, kind, state, \
             reason, job_key, created_at_ms, process_definition_id, process_definition_key \
             FROM incidents WHERE key = ?1",
            params![key as i64],
            map_incident,
        )
        .optional()
        .expect("query incident")
    }

    pub fn process_definitions(&self) -> Vec<ProcessDefinitionRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare("SELECT key, process_id, version FROM process_definitions")
            .expect("prepare process_definitions");
        let rows = stmt
            .query_map([], |r| {
                Ok(ProcessDefinitionRow {
                    key: r.get::<_, i64>(0)? as Key,
                    process_id: r.get(1)?,
                    version: r.get(2)?,
                })
            })
            .expect("query process_definitions");
        rows.filter_map(Result::ok).collect()
    }

    /// The verbatim BPMN XML for the process definition with `key`, or `None`
    /// when no such definition is projected (only the latest version per process
    /// id is retained, mirroring the engine). Empty-string XML (a definition
    /// built programmatically rather than parsed) is returned as `Some("")`.
    pub fn process_definition_xml(&self, key: Key) -> Option<String> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT xml FROM process_definitions WHERE key = ?1",
            params![key as i64],
            |r| r.get::<_, String>(0),
        )
        .optional()
        .expect("query process_definition_xml")
    }

    pub fn variables(&self) -> Vec<VariableRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT key, instance_key, scope_key, name, value, \
                 process_definition_id, process_definition_key FROM variables",
            )
            .expect("prepare variables");
        let rows = stmt.query_map([], map_variable).expect("query variables");
        rows.filter_map(Result::ok).collect()
    }

    pub fn variable(&self, key: Key) -> Option<VariableRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT key, instance_key, scope_key, name, value, \
             process_definition_id, process_definition_key FROM variables WHERE key = ?1",
            params![key as i64],
            map_variable,
        )
        .optional()
        .expect("query variable")
    }

    /// Returns every variable belonging to a process instance, ordered by name
    /// for deterministic results. Used to assemble the variable payload returned
    /// by an `awaitCompletion` create request.
    pub fn instance_variables(&self, instance_key: Key) -> Vec<VariableRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT key, instance_key, scope_key, name, value, \
                 process_definition_id, process_definition_key FROM variables \
                 WHERE instance_key = ?1 ORDER BY name",
            )
            .expect("prepare instance variables");
        let rows = stmt
            .query_map(params![instance_key as i64], map_variable)
            .expect("query instance variables");
        rows.filter_map(Result::ok).collect()
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
            slot_by_partition.insert(pid, list.len());
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

    pub fn variable(&self, key: Key) -> Option<VariableRow> {
        self.shard_for(key)?.variable(key)
    }

    pub fn instance_variables(&self, instance_key: Key) -> Vec<VariableRow> {
        self.shard_for(instance_key)
            .map(|s| s.instance_variables(instance_key))
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

    pub fn process_definition_xml(&self, key: Key) -> Option<String> {
        for s in &self.shards {
            if let Some(xml) = s.process_definition_xml(key) {
                return Some(xml);
            }
        }
        None
    }

    // --- scans: concatenate across shards (partition-disjoint) ---

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

    pub fn incidents(&self) -> Vec<IncidentRow> {
        self.shards.iter().flat_map(|s| s.incidents()).collect()
    }

    pub fn variables(&self) -> Vec<VariableRow> {
        self.shards.iter().flat_map(|s| s.variables()).collect()
    }

    // --- counts: sum across shards ---

    pub fn active_instance_count(&self) -> usize {
        self.shards.iter().map(|s| s.active_instance_count()).sum()
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

fn map_instance(r: &rusqlite::Row) -> rusqlite::Result<ProcessInstanceRow> {
    let tags_str: String = r.get(8)?;
    let tags = if tags_str.is_empty() {
        Vec::new()
    } else {
        tags_str.split(',').map(|s| s.to_string()).collect()
    };
    Ok(ProcessInstanceRow {
        key: r.get::<_, i64>(0)? as Key,
        process_id: r.get(1)?,
        process_definition_id: r.get(2)?,
        process_definition_key: r.get(3)?,
        version: r.get(4)?,
        state: instance_state_from(r.get(5)?),
        start_date_ms: r.get::<_, i64>(6)? as u64,
        has_incident: r.get::<_, i64>(7)? != 0,
        tags,
        business_id: r.get(9)?,
    })
}

fn map_job(r: &rusqlite::Row) -> rusqlite::Result<JobRow> {
    Ok(JobRow {
        key: r.get::<_, i64>(0)? as Key,
        instance_key: r.get::<_, i64>(1)? as Key,
        element_instance_key: r.get::<_, i64>(2)? as Key,
        element_id: r.get(3)?,
        job_type: r.get(4)?,
        state: job_state_from(r.get(5)?),
        retries: r.get(6)?,
        worker: r.get(7)?,
        deadline_ms: r.get::<_, Option<i64>>(8)?.map(|v| v as u64),
        process_definition_id: r.get(9)?,
        process_definition_key: r.get(10)?,
    })
}

fn map_user_task(r: &rusqlite::Row) -> rusqlite::Result<UserTaskRow> {
    let candidate_groups: String = r.get(6)?;
    let candidate_users: String = r.get(7)?;
    Ok(UserTaskRow {
        key: r.get::<_, i64>(0)? as Key,
        instance_key: r.get::<_, i64>(1)? as Key,
        element_instance_key: r.get::<_, i64>(2)? as Key,
        element_id: r.get(3)?,
        state: user_task_state_from(r.get(4)?),
        assignee: r.get(5)?,
        candidate_groups: serde_json::from_str(&candidate_groups).unwrap_or_default(),
        candidate_users: serde_json::from_str(&candidate_users).unwrap_or_default(),
        due_date: r.get(8)?,
        follow_up_date: r.get(9)?,
        priority: r.get(10)?,
        created_at_ms: r.get::<_, i64>(11)? as u64,
        process_definition_id: r.get(12)?,
        process_definition_key: r.get(13)?,
        process_definition_version: r.get(14)?,
    })
}

fn map_incident(r: &rusqlite::Row) -> rusqlite::Result<IncidentRow> {
    Ok(IncidentRow {
        key: r.get::<_, i64>(0)? as Key,
        instance_key: r.get::<_, i64>(1)? as Key,
        element_instance_key: r.get::<_, i64>(2)? as Key,
        element_id: r.get(3)?,
        kind: incident_kind_from(r.get(4)?),
        state: incident_state_from(r.get(5)?),
        reason: r.get(6)?,
        job_key: r.get::<_, Option<i64>>(7)?.map(|v| v as Key),
        created_at_ms: r.get::<_, i64>(8)? as u64,
        process_definition_id: r.get(9)?,
        process_definition_key: r.get(10)?,
    })
}

fn map_variable(r: &rusqlite::Row) -> rusqlite::Result<VariableRow> {
    Ok(VariableRow {
        key: r.get::<_, i64>(0)? as Key,
        instance_key: r.get::<_, i64>(1)? as Key,
        scope_key: r.get::<_, i64>(2)? as Key,
        name: r.get(3)?,
        value: r.get(4)?,
        process_definition_id: r.get(5)?,
        process_definition_key: r.get(6)?,
    })
}

/// Serializes an engine [`Value`] to the serialized-JSON string Camunda uses on
/// the wire: strings are JSON-quoted (so a string `myValue` becomes `"myValue"`),
/// numbers and booleans render bare, and lists/objects render as JSON.
fn json_value(value: &Value) -> String {
    crate::value_to_json(value).to_string()
}

/// Upserts a batch of variables into a single variable scope. For a root-scope
/// write (`VariablesUpdated`) the caller passes `scope_key == instance_key`; for
/// a nested scope (`ScopedVariablesUpdated` — a sub-process, multi-instance body
/// or child) it passes the scope-owning element instance key. Names are sorted
/// so the autoincrement variable keys are assigned deterministically on a rebuild
/// (a `VariablesUpdated`/`ProcessInstanceCreated` event carries an unordered map).
/// An already-known (scope, name) keeps its key and has its value overwritten.
/// Prepared-statement caching for the projection hot path. Plain
/// `Connection::execute` / `query_row` recompile the SQL text on every call; the
/// exporter runs one statement per projected event, so under load that
/// (re)parsing dominated a CPU profile (`sqlite3RunParser` / `sqlite3GetToken` /
/// `yy_reduce` were the exporter's top self-time symbols). Routing the hot
/// statements through `prepare_cached` compiles each SQL string once per
/// connection and reuses the cached plan, keeping only bytecode execution on the
/// per-event path. Semantics are identical — same SQL, same params.
trait CachedSql {
    fn cexecute<P: rusqlite::Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize>;
    fn cquery_row<T, P, F>(&self, sql: &str, params: P, f: F) -> rusqlite::Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>;
}

impl CachedSql for rusqlite::Connection {
    fn cexecute<P: rusqlite::Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.prepare_cached(sql)?.execute(params)
    }

    fn cquery_row<T, P, F>(&self, sql: &str, params: P, f: F) -> rusqlite::Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        self.prepare_cached(sql)?.query_row(params, f)
    }
}

fn upsert_variables(
    tx: &rusqlite::Transaction,
    instance_key: Key,
    scope_key: Key,
    variables: &std::collections::HashMap<String, Value>,
) -> rusqlite::Result<()> {
    if variables.is_empty() {
        return Ok(());
    }
    let (def_id, def_key) = instance_def(tx, instance_key);
    let mut entries: Vec<(&String, &Value)> = variables.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    for (name, value) in entries {
        tx.cexecute(
            "INSERT INTO variables (instance_key, scope_key, name, value, \
             process_definition_id, process_definition_key) VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(scope_key, name) DO UPDATE SET value = excluded.value",
            params![
                instance_key as i64,
                scope_key as i64,
                name,
                json_value(value),
                def_id,
                def_key
            ],
        )?;
    }
    Ok(())
}

/// The process-definition identity (`process_definition_id`,
/// `process_definition_key`) carried by an instance row, used to denormalize
/// jobs and incidents onto their owning definition. Defaults to empty values
/// when the instance is unknown (it always precedes its jobs/incidents in the
/// event order, so this is only a safety net).
fn instance_def(tx: &rusqlite::Transaction, instance_key: Key) -> (String, String) {
    tx.cquery_row(
        "SELECT process_definition_id, process_definition_key FROM process_instances WHERE key = ?1",
        params![instance_key as i64],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .optional()
    .ok()
    .flatten()
    .unwrap_or_default()
}

/// The deployed version of the definition behind an instance (defaults to 1
/// when the instance row is not yet present).
fn instance_version(tx: &rusqlite::Transaction, instance_key: Key) -> i32 {
    tx.cquery_row(
        "SELECT version FROM process_instances WHERE key = ?1",
        params![instance_key as i64],
        |r| r.get(0),
    )
    .optional()
    .ok()
    .flatten()
    .unwrap_or(1)
}

/// Applies a single event to the read model. Only events that surface in a
/// `search*`/`get*` projection are materialized; the rest (element lifecycle,
/// sequence flows, timers, message subscriptions, start subscriptions) carry no
/// queryable read-model state and are ignored.
fn project(tx: &rusqlite::Transaction, event: &Event) -> rusqlite::Result<()> {
    match event {
        Event::ProcessDeployed {
            process_definition_key,
            version,
            process,
            ..
        } => {
            // Only the latest version of a process id is searchable, mirroring
            // the engine's `state.processes` (keyed by id); a redeploy replaces.
            // The verbatim BPMN XML rides on the deploy event (engine state) and
            // is projected here so getProcessDefinitionXML / the console diagram
            // can serve it by key without querying the engine actor.
            tx.cexecute(
                "INSERT INTO process_definitions (process_id, key, version, xml) VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(process_id) DO UPDATE SET key = excluded.key, version = excluded.version, xml = excluded.xml",
                params![process.id, *process_definition_key as i64, version, process.xml],
            )?;
        }

        Event::ProcessInstanceCreated {
            instance_key,
            process_id,
            created_at,
            variables,
            tags,
            business_id,
        } => {
            // Resolve the deployed identity now (defaults mirror
            // `process_instance_result` when no definition is on record).
            let (def_key, version): (String, i32) = tx
                .query_row(
                    "SELECT key, version FROM process_definitions WHERE process_id = ?1",
                    params![process_id],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i32>(1)?)),
                )
                .optional()?
                .map(|(k, v)| (k.to_string(), v))
                .unwrap_or_else(|| ("-1".to_string(), 0));
            // Serialize tags as comma-separated string for storage
            let tags_str = tags.join(",");
            tx.cexecute(
                "INSERT INTO process_instances (key, process_id, process_definition_id, \
                 process_definition_key, version, state, start_date_ms, has_incident, tags, business_id) \
                 VALUES (?1, ?2, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8) \
                 ON CONFLICT(key) DO UPDATE SET process_id = excluded.process_id, \
                 process_definition_id = excluded.process_definition_id, \
                 process_definition_key = excluded.process_definition_key, \
                 version = excluded.version, start_date_ms = excluded.start_date_ms, \
                 tags = excluded.tags, business_id = excluded.business_id",
                params![
                    *instance_key as i64,
                    process_id,
                    def_key,
                    version,
                    instance_state_code(ProcessInstanceState::Active),
                    *created_at as i64,
                    tags_str,
                    business_id.as_ref(),
                ],
            )?;
            // Variables the instance was created with (the process-instance row
            // exists now, so the scope's denormalized definition resolves).
            upsert_variables(tx, *instance_key, *instance_key, variables)?;
        }

        Event::ProcessInstanceCompleted { instance_key } => {
            tx.cexecute(
                "UPDATE process_instances SET state = ?2 WHERE key = ?1",
                params![
                    *instance_key as i64,
                    instance_state_code(ProcessInstanceState::Completed)
                ],
            )?;
        }

        Event::ProcessInstanceTerminated { instance_key } => {
            tx.cexecute(
                "UPDATE process_instances SET state = ?2, has_incident = 0 WHERE key = ?1",
                params![
                    *instance_key as i64,
                    instance_state_code(ProcessInstanceState::Terminated)
                ],
            )?;
            // Close any incident still active on the terminated instance, so it
            // no longer surfaces as open in incident search.
            tx.cexecute(
                "UPDATE incidents SET state = ?2 WHERE instance_key = ?1 AND state = ?3",
                params![
                    *instance_key as i64,
                    incident_state_code(IncidentState::Resolved),
                    incident_state_code(IncidentState::Active),
                ],
            )?;
        }

        Event::JobCreated {
            job_key,
            instance_key,
            element_instance_key,
            element_id,
            job_type,
            retries,
            ..
        } => {
            let (def_id, def_key) = instance_def(tx, *instance_key);
            tx.cexecute(
                "INSERT INTO jobs (key, instance_key, element_instance_key, element_id, job_type, \
                 state, retries, worker, deadline_ms, process_definition_id, process_definition_key) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8, ?9) \
                 ON CONFLICT(key) DO UPDATE SET state = excluded.state, retries = excluded.retries, \
                 worker = NULL, deadline_ms = NULL",
                params![
                    *job_key as i64,
                    *instance_key as i64,
                    *element_instance_key as i64,
                    element_id,
                    job_type,
                    job_state_code(JobState::Created),
                    *retries,
                    def_id,
                    def_key,
                ],
            )?;
        }

        Event::JobActivated {
            job_key,
            worker,
            deadline,
            ..
        } => {
            tx.cexecute(
                "UPDATE jobs SET state = ?2, worker = ?3, deadline_ms = ?4 WHERE key = ?1",
                params![
                    *job_key as i64,
                    job_state_code(JobState::Activated),
                    worker,
                    *deadline as i64,
                ],
            )?;
        }

        Event::JobLockExpired { job_key, .. } => {
            tx.cexecute(
                "UPDATE jobs SET state = ?2, worker = NULL, deadline_ms = NULL \
                 WHERE key = ?1 AND state = ?3",
                params![
                    *job_key as i64,
                    job_state_code(JobState::Created),
                    job_state_code(JobState::Activated),
                ],
            )?;
        }

        Event::JobFailed {
            job_key, retries, ..
        } => {
            let state = if *retries > 0 {
                JobState::Created
            } else {
                JobState::Failed
            };
            tx.cexecute(
                "UPDATE jobs SET state = ?2, retries = ?3, worker = NULL, deadline_ms = NULL \
                 WHERE key = ?1",
                params![*job_key as i64, job_state_code(state), retries],
            )?;
        }

        Event::JobErrorThrown { job_key, .. } => {
            tx.cexecute(
                "UPDATE jobs SET state = ?2, worker = NULL, deadline_ms = NULL WHERE key = ?1",
                params![*job_key as i64, job_state_code(JobState::Errored)],
            )?;
        }

        Event::JobCompleted { job_key, .. } => {
            tx.cexecute(
                "UPDATE jobs SET state = ?2, worker = NULL, deadline_ms = NULL WHERE key = ?1",
                params![*job_key as i64, job_state_code(JobState::Completed)],
            )?;
        }

        Event::JobCanceled { job_key, .. } => {
            tx.cexecute(
                "UPDATE jobs SET state = ?2, worker = NULL, deadline_ms = NULL WHERE key = ?1",
                params![*job_key as i64, job_state_code(JobState::Canceled)],
            )?;
        }

        Event::JobRetriesUpdated {
            job_key, retries, ..
        } => {
            tx.cexecute(
                "UPDATE jobs SET retries = ?2 WHERE key = ?1",
                params![*job_key as i64, retries],
            )?;
        }

        Event::UserTaskCreated {
            user_task_key,
            instance_key,
            element_instance_key,
            element_id,
            created_at,
            assignee,
            candidate_groups,
            candidate_users,
            due_date,
            follow_up_date,
            priority,
        } => {
            let (def_id, def_key) = instance_def(tx, *instance_key);
            let version = instance_version(tx, *instance_key);
            let groups = serde_json::to_string(candidate_groups).unwrap_or_else(|_| "[]".into());
            let users = serde_json::to_string(candidate_users).unwrap_or_else(|_| "[]".into());
            tx.cexecute(
                "INSERT INTO user_tasks (key, instance_key, element_instance_key, element_id, \
                 state, assignee, candidate_groups, candidate_users, due_date, follow_up_date, \
                 priority, created_at_ms, process_definition_id, process_definition_key, \
                 process_definition_version) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15) \
                 ON CONFLICT(key) DO UPDATE SET state = excluded.state",
                params![
                    *user_task_key as i64,
                    *instance_key as i64,
                    *element_instance_key as i64,
                    element_id,
                    user_task_state_code(UserTaskState::Created),
                    assignee,
                    groups,
                    users,
                    due_date,
                    follow_up_date,
                    *priority,
                    *created_at as i64,
                    def_id,
                    def_key,
                    version,
                ],
            )?;
        }

        Event::UserTaskAssigned {
            user_task_key,
            assignee,
            ..
        } => {
            tx.cexecute(
                "UPDATE user_tasks SET assignee = ?2 WHERE key = ?1",
                params![*user_task_key as i64, assignee],
            )?;
        }

        Event::UserTaskUpdated {
            user_task_key,
            candidate_groups,
            candidate_users,
            due_date,
            follow_up_date,
            priority,
            ..
        } => {
            if let Some(groups) = candidate_groups {
                let json = serde_json::to_string(groups).unwrap_or_else(|_| "[]".into());
                tx.cexecute(
                    "UPDATE user_tasks SET candidate_groups = ?2 WHERE key = ?1",
                    params![*user_task_key as i64, json],
                )?;
            }
            if let Some(users) = candidate_users {
                let json = serde_json::to_string(users).unwrap_or_else(|_| "[]".into());
                tx.cexecute(
                    "UPDATE user_tasks SET candidate_users = ?2 WHERE key = ?1",
                    params![*user_task_key as i64, json],
                )?;
            }
            if let Some(due) = due_date {
                tx.cexecute(
                    "UPDATE user_tasks SET due_date = ?2 WHERE key = ?1",
                    params![*user_task_key as i64, due],
                )?;
            }
            if let Some(follow_up) = follow_up_date {
                tx.cexecute(
                    "UPDATE user_tasks SET follow_up_date = ?2 WHERE key = ?1",
                    params![*user_task_key as i64, follow_up],
                )?;
            }
            if let Some(p) = priority {
                tx.cexecute(
                    "UPDATE user_tasks SET priority = ?2 WHERE key = ?1",
                    params![*user_task_key as i64, p],
                )?;
            }
        }

        Event::UserTaskCompleted { user_task_key, .. } => {
            tx.cexecute(
                "UPDATE user_tasks SET state = ?2 WHERE key = ?1",
                params![
                    *user_task_key as i64,
                    user_task_state_code(UserTaskState::Completed)
                ],
            )?;
        }

        Event::UserTaskCanceled { user_task_key, .. } => {
            tx.cexecute(
                "UPDATE user_tasks SET state = ?2 WHERE key = ?1",
                params![
                    *user_task_key as i64,
                    user_task_state_code(UserTaskState::Canceled)
                ],
            )?;
        }

        Event::IncidentRaised {
            incident_key,
            instance_key,
            element_instance_key,
            element_id,
            kind,
            reason,
            job_key,
            created_at,
        } => {
            let (def_id, def_key) = instance_def(tx, *instance_key);
            tx.cexecute(
                "INSERT INTO incidents (key, instance_key, element_instance_key, element_id, kind, \
                 state, reason, job_key, created_at_ms, process_definition_id, process_definition_key) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
                 ON CONFLICT(key) DO UPDATE SET state = excluded.state",
                params![
                    *incident_key as i64,
                    *instance_key as i64,
                    *element_instance_key as i64,
                    element_id,
                    incident_kind_code(*kind),
                    incident_state_code(IncidentState::Active),
                    reason,
                    job_key.map(|k| k as i64),
                    *created_at as i64,
                    def_id,
                    def_key,
                ],
            )?;
            tx.cexecute(
                "UPDATE process_instances SET has_incident = 1 WHERE key = ?1",
                params![*instance_key as i64],
            )?;
        }

        Event::IncidentResolved {
            incident_key,
            instance_key,
            job_key,
            resolved_at: _,
            operation_reference: _,
        } => {
            tx.cexecute(
                "UPDATE incidents SET state = ?2 WHERE key = ?1",
                params![
                    *incident_key as i64,
                    incident_state_code(IncidentState::Resolved)
                ],
            )?;
            // `hasIncident` reflects only still-active incidents.
            let active: i64 = tx.cquery_row(
                "SELECT COUNT(*) FROM incidents WHERE instance_key = ?1 AND state = ?2",
                params![
                    *instance_key as i64,
                    incident_state_code(IncidentState::Active)
                ],
                |r| r.get(0),
            )?;
            tx.cexecute(
                "UPDATE process_instances SET has_incident = ?2 WHERE key = ?1",
                params![*instance_key as i64, i64::from(active > 0)],
            )?;
            // A recoverable job-incident returns its parked job to the pool.
            if let Some(job_key) = job_key {
                tx.cexecute(
                    "UPDATE jobs SET state = ?2, worker = NULL, deadline_ms = NULL WHERE key = ?1",
                    params![*job_key as i64, job_state_code(JobState::Created)],
                )?;
            }
        }

        Event::VariablesUpdated {
            instance_key,
            variables,
        } => {
            upsert_variables(tx, *instance_key, *instance_key, variables)?;
        }

        // A write to a nested variable scope (sub-process, multi-instance body or
        // child): materialize it under its own `scope_key` so `searchVariables`
        // reports the Zeebe-correct `scopeKey`. The scope's local variables are
        // retained after the scope tears down (the read model keeps history, like
        // an instance's variables after it completes).
        Event::ScopedVariablesUpdated {
            instance_key,
            scope_key,
            variables,
        } => {
            upsert_variables(tx, *instance_key, *scope_key, variables)?;
        }

        // Events with no queryable read-model projection.
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod writability_tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::ReadStore;

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    fn scratch_db() -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "nanobpm-readstore-{}-{}.sqlite",
            std::process::id(),
            n
        ))
    }

    #[test]
    fn open_succeeds_on_a_writable_db() {
        let path = scratch_db();
        ReadStore::open(Some(&path)).expect("fresh writable db opens");
        // Re-open (schema already matches) still validates writability.
        ReadStore::open(Some(&path)).expect("existing writable db re-opens");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn file_backed_store_runs_in_wal_mode() {
        // The read model is a derived projection rebuilt from the journal, so it
        // runs WAL + synchronous=NORMAL to keep the exporter's per-commit fsync off
        // the projection hot path. Lock that in so a future change can't silently
        // revert to the default (DELETE journal + synchronous=FULL) durability.
        let path = scratch_db();
        let store = ReadStore::open(Some(&path)).expect("open file-backed db");
        let (mode, sync): (String, i64) = {
            let conn = store.conn.lock().unwrap();
            let mode = conn
                .query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))
                .unwrap();
            let sync = conn
                .query_row("PRAGMA synchronous", [], |r| r.get::<_, i64>(0))
                .unwrap();
            (mode, sync)
        };
        assert_eq!(
            mode.to_lowercase(),
            "wal",
            "read store must use WAL journal"
        );
        assert_eq!(sync, 1, "read store must use synchronous=NORMAL (1)");
        drop(store);
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[cfg(unix)]
    #[test]
    fn open_fails_fast_on_a_readonly_db() {
        use std::os::unix::fs::PermissionsExt;

        let path = scratch_db();
        // Create a healthy db with the current schema, then drop the handle.
        ReadStore::open(Some(&path)).expect("seed db");
        // Make the file itself read-only: re-open finds a matching schema (so it
        // writes nothing during open) and must fail on the writability probe.
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o444);
        std::fs::set_permissions(&path, perms).unwrap();

        let err = match ReadStore::open(Some(&path)) {
            Ok(_) => panic!("read-only db must fail fast"),
            Err(e) => e,
        };
        assert!(
            err.to_string().to_lowercase().contains("readonly"),
            "expected a readonly error, got: {err}"
        );

        // Restore perms so cleanup can remove the file.
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o644);
        std::fs::set_permissions(&path, perms).unwrap();
        std::fs::remove_file(&path).ok();
    }
}

#[cfg(test)]
mod definition_xml_tests {
    use nanobpmn_engine_core::{Event, ProcessBuilder, ProcessDefinition};

    use super::ReadStore;

    fn deployed_event(key: u64, xml: &str) -> Event {
        let mut def: ProcessDefinition = ProcessBuilder::new("p")
            .start_event("s")
            .end_event("e")
            .connect("s", "e")
            .build()
            .unwrap();
        def.xml = xml.to_string();
        Event::ProcessDeployed {
            deployment_key: 1,
            process_definition_key: key,
            version: 1,
            process: def,
        }
    }

    #[test]
    fn projects_and_serves_the_deployment_xml_by_key() {
        let store = ReadStore::open(None).unwrap();
        let xml = "<bpmn:definitions>…verbatim…</bpmn:definitions>";
        let event = deployed_event(42, xml);
        store.export(&[&event]).unwrap();

        assert_eq!(store.process_definition_xml(42).as_deref(), Some(xml));
        // Unknown key has no XML.
        assert_eq!(store.process_definition_xml(999), None);
    }

    #[test]
    fn programmatic_definition_has_empty_xml() {
        let store = ReadStore::open(None).unwrap();
        let event = deployed_event(7, "");
        store.export(&[&event]).unwrap();
        // Present but empty — the handler maps this to a 204, not a 404.
        assert_eq!(store.process_definition_xml(7).as_deref(), Some(""));
    }

    fn created_event(instance_key: super::Key) -> Event {
        Event::ProcessInstanceCreated {
            instance_key,
            process_id: "p".to_string(),
            variables: std::collections::HashMap::new(),
            created_at: 0,
            tags: Vec::new(),
            business_id: None,
        }
    }

    #[test]
    fn prune_terminal_instances_caps_history_keeping_active_and_newest() {
        let store = ReadStore::open(None).unwrap();
        // 5 terminal instances (keys 1..=5) + 2 active (keys 100, 101).
        for k in 1..=5u64 {
            let created = created_event(k);
            let done = Event::ProcessInstanceCompleted { instance_key: k };
            store.export(&[&created, &done]).unwrap();
        }
        for k in [100u64, 101] {
            let created = created_event(k);
            store.export(&[&created]).unwrap();
        }

        // max_keep == 0 disables pruning.
        assert_eq!(store.prune_terminal_instances(0, 0).unwrap(), 0);
        assert!(store.process_instance(1).is_some());
        // COUNT(*) over all instances: 5 terminal + 2 active = 7.
        assert_eq!(store.instance_count(), 7);

        // Batched: with a delete cap of 1, only the oldest terminal beyond the
        // cap (key 1) is evicted this sweep; keys 2,3 remain until later sweeps.
        assert_eq!(store.prune_terminal_instances(2, 1).unwrap(), 1);
        assert!(store.process_instance(1).is_none());
        assert!(store.process_instance(2).is_some());
        assert!(store.process_instance(3).is_some());
        assert_eq!(store.instance_count(), 6);

        // Unbounded (max_delete == 0): evict the remaining overflow (keys 2, 3),
        // keeping the 2 newest terminal instances (keys 4, 5).
        let evicted = store.prune_terminal_instances(2, 0).unwrap();
        assert_eq!(evicted, 2);
        assert!(store.process_instance(2).is_none());
        assert!(store.process_instance(3).is_none());
        assert!(store.process_instance(4).is_some());
        assert!(store.process_instance(5).is_some());
        // Active instances are never evicted.
        assert!(store.process_instance(100).is_some());
        assert!(store.process_instance(101).is_some());
        assert_eq!(store.active_instance_count(), 2);
        // 2 newest terminal (4,5) + 2 active (100,101) = 4.
        assert_eq!(store.instance_count(), 4);

        // Re-pruning at the same cap is a no-op (nothing beyond the cap).
        assert_eq!(store.prune_terminal_instances(2, 0).unwrap(), 0);
    }

    #[test]
    fn adaptive_prune_once_evicts_oldest_terminal_in_bounded_batches() {
        // File-backed so the pruner can open its own second connection.
        let path = std::env::temp_dir().join(format!(
            "nanobpm-pruner-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = ReadStore::open(Some(&path)).unwrap();
        // 5 terminal (keys 1..=5) + 2 active (100, 101).
        for k in 1..=5u64 {
            let created = created_event(k);
            let done = Event::ProcessInstanceCompleted { instance_key: k };
            store.export(&[&created, &done]).unwrap();
        }
        for k in [100u64, 101] {
            store.export(&[&created_event(k)]).unwrap();
        }
        let mut conn = store.prune_connection().unwrap().expect("file-backed");

        // Under budget (huge high-water): a cheap no-op, evicts nothing.
        assert_eq!(
            ReadStore::adaptive_prune_once(&mut conn, u64::MAX, u64::MAX, 4096, 4096).unwrap(),
            0
        );
        assert_eq!(store.instance_count(), 7);

        // Over budget (high=low=1 forces eviction), capped at 2 deletes this wake:
        // the OLDEST two terminal (keys 1, 2) go first.
        assert_eq!(
            ReadStore::adaptive_prune_once(&mut conn, 1, 1, 4096, 2).unwrap(),
            2
        );
        assert!(store.process_instance(1).is_none());
        assert!(store.process_instance(2).is_none());
        assert!(store.process_instance(3).is_some());
        assert_eq!(store.instance_count(), 5);

        // Next wake with a generous cap drains the remaining terminal (3,4,5)…
        assert_eq!(
            ReadStore::adaptive_prune_once(&mut conn, 1, 1, 4096, 4096).unwrap(),
            3
        );
        // …but never the active instances.
        assert!(store.process_instance(100).is_some());
        assert!(store.process_instance(101).is_some());
        assert_eq!(store.active_instance_count(), 2);
        assert_eq!(store.instance_count(), 2);

        // Nothing terminal left: a no-op even while "over budget".
        assert_eq!(
            ReadStore::adaptive_prune_once(&mut conn, 1, 1, 4096, 4096).unwrap(),
            0
        );

        drop(conn);
        drop(store);
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn process_instances_page_returns_newest_first_bounded_pages() {
        let store = ReadStore::open(None).unwrap();
        // Keys 1..=5, created oldest→newest; keys are monotonic so newest = key 5.
        for k in 1..=5u64 {
            store.export(&[&created_event(k)]).unwrap();
        }

        assert_eq!(store.process_instance_count(), 5);

        // First page: the 2 newest, descending by key.
        let page0 = store.process_instances_page(2, 0);
        assert_eq!(page0.iter().map(|r| r.key).collect::<Vec<_>>(), vec![5, 4]);

        // Second page picks up where the first left off.
        let page1 = store.process_instances_page(2, 2);
        assert_eq!(page1.iter().map(|r| r.key).collect::<Vec<_>>(), vec![3, 2]);

        // Final partial page.
        let page2 = store.process_instances_page(2, 4);
        assert_eq!(page2.iter().map(|r| r.key).collect::<Vec<_>>(), vec![1]);

        // Offset past the end yields nothing.
        assert!(store.process_instances_page(2, 6).is_empty());
    }

    #[test]
    fn scoped_variables_are_projected_under_their_own_scope_key() {
        use nanobpmn_engine_core::Value;

        let store = ReadStore::open(None).unwrap();
        let instance_key: super::Key = 1;
        let scope_key: super::Key = 50; // a sub-process / MI-child element instance
        store.export(&[&created_event(instance_key)]).unwrap();

        // A root-scope write and a nested-scope write of the SAME name.
        store
            .export(&[
                &Event::VariablesUpdated {
                    instance_key,
                    variables: std::collections::HashMap::from([(
                        "amount".to_string(),
                        Value::Int(10),
                    )]),
                },
                &Event::ScopedVariablesUpdated {
                    instance_key,
                    scope_key,
                    variables: std::collections::HashMap::from([
                        ("amount".to_string(), Value::Int(20)),
                        ("item".to_string(), Value::Int(7)),
                    ]),
                },
            ])
            .unwrap();

        let mut rows = store.instance_variables(instance_key);
        rows.sort_by_key(|a| (a.scope_key, a.name.clone()));
        // Three distinct rows: root `amount`, scoped `amount`, scoped `item` —
        // the same name coexists across scopes because UNIQUE is (scope_key, name).
        assert_eq!(rows.len(), 3);

        let root_amount = rows
            .iter()
            .find(|r| r.scope_key == instance_key && r.name == "amount")
            .expect("root amount");
        assert_eq!(root_amount.value, "10");

        let scoped_amount = rows
            .iter()
            .find(|r| r.scope_key == scope_key && r.name == "amount")
            .expect("scoped amount");
        assert_eq!(scoped_amount.value, "20");
        assert_eq!(scoped_amount.instance_key, instance_key);

        let scoped_item = rows
            .iter()
            .find(|r| r.scope_key == scope_key && r.name == "item")
            .expect("scoped item");
        assert_eq!(scoped_item.value, "7");

        // Re-writing the nested scope updates in place (keeps its key/scope).
        let before = scoped_amount.key;
        store
            .export(&[&Event::ScopedVariablesUpdated {
                instance_key,
                scope_key,
                variables: std::collections::HashMap::from([(
                    "amount".to_string(),
                    Value::Int(99),
                )]),
            }])
            .unwrap();
        let after = store
            .instance_variables(instance_key)
            .into_iter()
            .find(|r| r.scope_key == scope_key && r.name == "amount")
            .unwrap();
        assert_eq!(after.key, before, "upsert keeps the variable key");
        assert_eq!(after.value, "99");
    }
}
