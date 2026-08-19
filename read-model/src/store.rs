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

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use nanobpmn_engine_core::{
    Event, IncidentKind, IncidentState, JobKind, JobState, Key, ListenerEventType,
    ProcessInstanceState, TaskListenerEventType, UserTaskState, Value, partition_of,
};
use rusqlite::{Connection, OptionalExtension, params};

use crate::backend;

/// Monotonic read-model schema version, recorded in `meta(schema_version)`.
///
/// **Bump this by one whenever [`SCHEMA`] changes** (the CI drift guard
/// `schema_edit_requires_version_bump` fails the build if you forget). It lets an
/// already-current database short-circuit the additive reconcile on open, and it
/// is the monotonic ladder the issue #831 fix is built around.
const SCHEMA_VERSION: i64 = 2;

/// The content fingerprint of [`SCHEMA`] as of the current [`SCHEMA_VERSION`].
///
/// This is **only** a CI/test drift assertion — the guard test
/// `schema_edit_requires_version_bump` asserts `schema_fingerprint()` still
/// equals this constant, so any edit to `SCHEMA` fails the build until the author
/// bumps [`SCHEMA_VERSION`] and refreshes this value. It is **never** a runtime
/// wipe trigger (that destructive behaviour was the root cause of issue #831).
#[cfg(test)]
const SCHEMA_FINGERPRINT: i64 = -1950486211382609378;

/// The read model is a SQLite projection of the engine's event stream. Its
/// on-disk schema used to be identified by a content fingerprint of [`SCHEMA`],
/// and **any** edit to `SCHEMA` (even a purely additive column) made
/// [`ReadStore::ensure_schema`] DROP every table and recreate from scratch on the
/// next open. That was the root cause of issue #831: once the journal has
/// compacted (the steady state), a wiped read model sits below the compaction
/// floor, so the #732 boot recovery can only reproject *live* instances from the
/// engine snapshot — every completed/terminal instance, which lived **only** in
/// the read model, is silently and unrecoverably lost. It recurred on every
/// schema-changing release.
///
/// The schema now evolves via **non-destructive additive migration**
/// ([`ReadStore::reconcile_to_schema`]): on open the live database is brought
/// *up to* [`SCHEMA`] by adding any missing tables, columns and indexes
/// (`CREATE TABLE`, `ALTER TABLE ADD COLUMN`, `CREATE INDEX`) — **existing rows
/// are never dropped**. The target shape is *derived* from `SCHEMA` itself
/// (introspected from a throwaway in-memory database built from it), so there is
/// no hand-maintained migration ladder to drift out of sync: adding a column or
/// table to `SCHEMA` *is* the migration. Destructive rebuild is reserved for the
/// explicit [`ReadStore::reset`] path (a corrupt/truncated journal forcing a full
/// replay), where a reprojection restores the data anyway.
fn schema_fingerprint() -> i64 {
    fnv1a_64(SCHEMA.as_bytes())
}

/// FNV-1a (64-bit). Split out from [`schema_fingerprint`] so the hash itself is
/// unit-testable against known vectors and can't silently change behaviour.
fn fnv1a_64(bytes: &[u8]) -> i64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash as i64
}

pub(crate) const SCHEMA: &str = "
CREATE TABLE process_definitions (
    key        INTEGER PRIMARY KEY,
    process_id TEXT NOT NULL,
    version    INTEGER NOT NULL,
    name       TEXT,
    xml        TEXT NOT NULL DEFAULT '',
    start_form_id TEXT
);
-- UNIQUE enforces one row per (process_id, version) so a redeploy of the same
-- version can never create ambiguous \"latest version per id\" rows, and the
-- index also backs the MAX(version)/ORDER BY lookups below.
CREATE UNIQUE INDEX idx_process_definitions_id_version
    ON process_definitions(process_id, version);
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
    business_id            TEXT,
    parent_process_instance_key INTEGER,
    parent_element_instance_key INTEGER
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
    process_definition_key TEXT NOT NULL,
    job_kind               INTEGER NOT NULL DEFAULT 0,
    listener_event_type    INTEGER NOT NULL DEFAULT 0
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
    process_definition_version INTEGER NOT NULL,
    form_key               INTEGER,
    external_form_reference TEXT
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
CREATE TABLE decision_requirements (
    drg_id        TEXT PRIMARY KEY,
    drg_key       INTEGER NOT NULL,
    name          TEXT NOT NULL,
    version       INTEGER NOT NULL,
    resource_name TEXT NOT NULL DEFAULT '',
    xml           TEXT NOT NULL DEFAULT ''
);
CREATE TABLE decision_definitions (
    decision_id                   TEXT PRIMARY KEY,
    decision_key                  INTEGER NOT NULL,
    name                          TEXT NOT NULL,
    version                       INTEGER NOT NULL,
    decision_requirements_key     INTEGER NOT NULL,
    decision_requirements_id      TEXT NOT NULL,
    decision_requirements_name    TEXT NOT NULL DEFAULT '',
    decision_requirements_version INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE decision_instances (
    eval_instance_key         TEXT PRIMARY KEY,
    decision_evaluation_key   INTEGER NOT NULL,
    idx                       INTEGER NOT NULL,
    decision_id               TEXT NOT NULL,
    decision_key              INTEGER NOT NULL,
    decision_name             TEXT NOT NULL,
    decision_type             TEXT NOT NULL,
    version                   INTEGER NOT NULL,
    decision_requirements_id  TEXT NOT NULL,
    decision_requirements_key INTEGER NOT NULL,
    root_decision_key         INTEGER NOT NULL,
    instance_key              INTEGER NOT NULL,
    element_instance_key      INTEGER NOT NULL,
    process_definition_key    TEXT NOT NULL DEFAULT '',
    state                     TEXT NOT NULL,
    evaluation_failure        TEXT,
    evaluation_date_ms        INTEGER NOT NULL,
    result_json               TEXT NOT NULL,
    inputs_json               TEXT NOT NULL,
    rules_json                TEXT NOT NULL,
    tenant_id                 TEXT NOT NULL
);
CREATE TABLE definition_elements (
    process_definition_key INTEGER NOT NULL,
    element_id             TEXT NOT NULL,
    element_type           TEXT NOT NULL,
    element_name           TEXT,
    PRIMARY KEY (process_definition_key, element_id)
);
CREATE TABLE element_instances (
    element_instance_key   INTEGER PRIMARY KEY,
    instance_key           INTEGER NOT NULL,
    process_definition_id  TEXT NOT NULL,
    process_definition_key TEXT NOT NULL,
    element_id             TEXT NOT NULL,
    element_name           TEXT,
    element_type           TEXT NOT NULL,
    state                  INTEGER NOT NULL,
    start_date_ms          INTEGER NOT NULL,
    end_date_ms            INTEGER,
    scope_key              INTEGER NOT NULL DEFAULT 0,
    incident_key           INTEGER,
    has_incident           INTEGER NOT NULL DEFAULT 0,
    tenant_id              TEXT NOT NULL DEFAULT '<default>'
);
CREATE INDEX idx_element_instances_instance ON element_instances(instance_key);
CREATE TABLE message_subscriptions (
    subscription_key       INTEGER PRIMARY KEY,
    instance_key           INTEGER NOT NULL,
    element_instance_key   INTEGER NOT NULL,
    element_id             TEXT NOT NULL,
    message_name           TEXT NOT NULL,
    correlation_key        TEXT NOT NULL,
    created_at_ms          INTEGER NOT NULL DEFAULT 0,
    non_interrupting       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_message_subscriptions_instance ON message_subscriptions(instance_key);
CREATE TABLE correlated_message_subscriptions (
    message_key            INTEGER NOT NULL,
    subscription_key       INTEGER NOT NULL,
    instance_key           INTEGER NOT NULL,
    element_instance_key   INTEGER NOT NULL,
    element_id             TEXT NOT NULL,
    message_name           TEXT NOT NULL,
    correlation_key        TEXT NOT NULL,
    correlation_time_ms    INTEGER NOT NULL,
    partition_id           INTEGER NOT NULL,
    PRIMARY KEY (message_key, subscription_key)
);
CREATE INDEX idx_correlated_message_subscriptions_instance ON correlated_message_subscriptions(instance_key);
CREATE TABLE forms (
    form_key      INTEGER PRIMARY KEY,
    form_id       TEXT NOT NULL,
    version       INTEGER NOT NULL,
    schema        TEXT NOT NULL,
    resource_name TEXT NOT NULL DEFAULT '',
    tenant_id     TEXT NOT NULL DEFAULT '<default>'
);
CREATE INDEX idx_forms_id ON forms(form_id);
CREATE TABLE resources (
    resource_key   INTEGER PRIMARY KEY,
    resource_id    TEXT NOT NULL,
    resource_name  TEXT NOT NULL,
    version        INTEGER NOT NULL,
    version_tag    TEXT,
    content        TEXT NOT NULL,
    tenant_id      TEXT NOT NULL DEFAULT '<default>'
);
CREATE INDEX idx_resources_id ON resources(resource_id);
";

// --- read-model schema migration (issue #831) ---
//
// The additive-migration machinery below is deliberately *derivation-based*: the
// target shape is introspected from a throwaway in-memory database built from
// [`SCHEMA`], so there is a single source of truth (`SCHEMA`) and no
// hand-maintained migration ladder that could drift out of sync with it.

/// A column as reported by `PRAGMA table_info` — enough to reconstruct a legal
/// `ALTER TABLE ADD COLUMN` for any *additive* column.
struct ColumnShape {
    name: String,
    decl_type: String,
    notnull: bool,
    dflt: Option<String>,
}

/// The introspected shape of a database: table name -> (create statement, columns
/// in declared order), plus non-auto index name -> create statement.
struct SchemaShape {
    tables: std::collections::BTreeMap<String, (String, Vec<ColumnShape>)>,
    indexes: std::collections::BTreeMap<String, String>,
}

/// Introspects the shape of the database behind `conn` (its user tables, their
/// columns, and their explicit indexes).
fn introspect_shape(conn: &Connection) -> rusqlite::Result<SchemaShape> {
    let mut tables = std::collections::BTreeMap::new();
    let table_meta: Vec<(String, String)> = {
        let mut stmt = conn.prepare(
            "SELECT name, sql FROM sqlite_master \
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    for (name, create_sql) in table_meta {
        let mut cols = Vec::new();
        let mut stmt = conn.prepare(&format!(
            "PRAGMA table_info(\"{}\")",
            name.replace('"', "\"\"")
        ))?;
        let rows = stmt.query_map([], |r| {
            Ok(ColumnShape {
                name: r.get::<_, String>(1)?,
                decl_type: r.get::<_, String>(2)?,
                notnull: r.get::<_, i64>(3)? != 0,
                dflt: r.get::<_, Option<String>>(4)?,
            })
        })?;
        for col in rows {
            cols.push(col?);
        }
        tables.insert(name, (create_sql, cols));
    }
    let mut indexes = std::collections::BTreeMap::new();
    {
        // Only indexes with an explicit `sql` (created by a CREATE INDEX
        // statement); auto-indexes backing UNIQUE/PRIMARY KEY have a NULL sql and
        // are recreated implicitly with their table.
        let mut stmt = conn.prepare(
            "SELECT name, sql FROM sqlite_master \
             WHERE type = 'index' AND sql IS NOT NULL AND name NOT LIKE 'sqlite_%'",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        for row in rows {
            let (name, sql) = row?;
            indexes.insert(name, sql);
        }
    }
    Ok(SchemaShape { tables, indexes })
}

/// The introspected shape of [`SCHEMA`], built by executing it into a throwaway
/// in-memory database. This is the migration *target* (single source of truth).
fn target_shape() -> rusqlite::Result<SchemaShape> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch(SCHEMA)?;
    introspect_shape(&conn)
}

/// Non-destructively brings the live database at `conn` up to [`SCHEMA`]: creates
/// any missing table, adds any missing column (`ALTER TABLE ADD COLUMN`), and
/// creates any missing index. **Never drops or rewrites existing data** — this is
/// the core of the issue #831 fix. Idempotent: a partially-applied run is
/// completed on the next open.
fn reconcile_to_schema(conn: &Connection) -> rusqlite::Result<()> {
    let target = target_shape()?;
    let live = introspect_shape(conn)?;
    let mut ddl = String::new();
    for (table, (create_sql, target_cols)) in &target.tables {
        match live.tables.get(table) {
            None => {
                // Missing table: create it verbatim from the target statement,
                // preserving UNIQUE / AUTOINCREMENT / PRIMARY KEY that a
                // reconstructed DDL would lose.
                ddl.push_str(create_sql);
                ddl.push_str(";\n");
            }
            Some((_, live_cols)) => {
                let have: std::collections::HashSet<&str> =
                    live_cols.iter().map(|c| c.name.as_str()).collect();
                for col in target_cols {
                    if !have.contains(col.name.as_str()) {
                        ddl.push_str(&add_column_ddl(table, col));
                        ddl.push('\n');
                    }
                }
            }
        }
    }
    for (name, create_sql) in &target.indexes {
        if !live.indexes.contains_key(name) {
            ddl.push_str(create_sql);
            ddl.push_str(";\n");
        }
    }
    if !ddl.is_empty() {
        conn.execute_batch(&ddl)?;
    }
    Ok(())
}

/// Builds a legal `ALTER TABLE ADD COLUMN` for an additive column. SQLite
/// requires a NOT NULL column added to a (possibly non-empty) table to carry a
/// non-NULL default; an additive `SCHEMA` change must therefore give new NOT NULL
/// columns a `DEFAULT`, which this faithfully reproduces from the target shape.
fn add_column_ddl(table: &str, col: &ColumnShape) -> String {
    let mut s = format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" {}",
        table.replace('"', "\"\""),
        col.name.replace('"', "\"\""),
        col.decl_type
    );
    if let Some(d) = &col.dflt {
        s.push_str(" DEFAULT ");
        s.push_str(d);
    }
    if col.notnull {
        s.push_str(" NOT NULL");
    }
    s.push(';');
    s
}

/// User tables (excluding SQLite's internal `sqlite_%` tables) present in `conn`.
fn list_user_tables(conn: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master \
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    )?;
    let names = stmt.query_map([], |r| r.get::<_, String>(0))?;
    names.collect::<rusqlite::Result<Vec<_>>>()
}

/// Drops every user table in `conn` (derived from `sqlite_master`, so it can
/// never fall behind `SCHEMA`). Used only by the destructive [`ReadStore::reset`].
fn drop_all_user_tables(conn: &Connection) -> rusqlite::Result<()> {
    let mut drop_sql = String::new();
    for name in list_user_tables(conn)? {
        drop_sql.push_str(&format!(
            "DROP TABLE IF EXISTS \"{}\";",
            name.replace('"', "\"\"")
        ));
    }
    if !drop_sql.is_empty() {
        conn.execute_batch(&drop_sql)?;
    }
    Ok(())
}

/// Creates the full [`SCHEMA`] on an empty database and stamps the current
/// version/fingerprint with `exported_position = 0`.
fn create_fresh_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(SCHEMA)?;
    conn.execute(
        "INSERT INTO meta (k, v) VALUES ('schema_version', ?1) \
         ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        params![SCHEMA_VERSION],
    )?;
    conn.execute(
        "INSERT INTO meta (k, v) VALUES ('schema_fingerprint', ?1) \
         ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        params![schema_fingerprint()],
    )?;
    conn.execute(
        "INSERT INTO meta (k, v) VALUES ('exported_position', 0) \
         ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        [],
    )?;
    Ok(())
}

// --- durable terminal-audit archive (issue #831) ---
//
// Completed/terminal instances live ONLY in the read model, so a below-floor
// snapshot reprojection (#732 — which recovers only *live* instances from the
// engine snapshot) silently loses them. The archive is a co-located SQLite file
// sharing [`SCHEMA`] (so it evolves via the same additive migrations and is never
// destructively wiped) into which terminal instances are copied as they become
// terminal, and replayed back on top of a reprojection.

/// Instance-scoped dependent tables copied alongside a terminal
/// `process_instances` row (keyed by `instance_key`).
const TERMINAL_ARCHIVE_DEP_TABLES: [&str; 3] = ["user_tasks", "variables", "decision_instances"];

/// Attaches the terminal-audit archive database at `archive` to `conn` under the
/// schema name `terminal_archive`. The filename is bound as a parameter so no
/// path escaping is needed.
fn attach_archive(conn: &Connection, archive: &Path) -> rusqlite::Result<()> {
    conn.execute(
        "ATTACH DATABASE ?1 AS terminal_archive",
        params![archive.to_string_lossy()],
    )?;
    // The archive is the *durable source of truth* for completed history: unlike
    // the derived read model (rebuildable from the journal), a terminal instance
    // evicted below the snapshot floor lives ONLY here, so losing a recently
    // archived commit on power/OS loss is unrecoverable. `synchronous` is a
    // per-attached-database setting, so we give `terminal_archive` its own
    // power-safe profile (default FULL) on every attach rather than inheriting the
    // read model's throughput-tuned NORMAL from the main connection. Harmless on
    // the read-only replay path; archive writes are infrequent (only as instances
    // become terminal), so the per-commit fsync is off the read model's hot path.
    // See `archive_sync_pragma`.
    conn.pragma_update(
        Some(rusqlite::DatabaseName::Attached("terminal_archive")),
        "synchronous",
        archive_sync_pragma(),
    )?;
    Ok(())
}

/// `synchronous` durability level for the terminal-audit archive (issue #831).
/// Defaults to `FULL` (per-commit fsync, power-loss safe) because the archive is
/// the durable source of truth for completed history and is *not* rebuildable
/// from the journal once an instance has been evicted below the snapshot floor —
/// so it must not inherit the read model's throughput-tuned `NORMAL`.
/// `NANOBPMN_ARCHIVE_SYNC` overrides (e.g. `NORMAL` to trade archive durability
/// for speed, matching the read model).
fn archive_sync_pragma() -> String {
    std::env::var("NANOBPMN_ARCHIVE_SYNC").unwrap_or_else(|_| "FULL".into())
}

/// Column names common to the `table` copies in the two attached databases
/// `src_schema` and `dst_schema` (e.g. `main` and `terminal_archive`), quoted
/// and comma-joined for use in a cross-database `INSERT (<cols>) SELECT <cols>`.
///
/// Additive migrations (`ALTER TABLE ADD COLUMN`) append columns to *existing*
/// tables, so two copies of the same logical table — a freshly-created archive
/// vs. a migrated live DB, or vice versa — can end up with different *physical*
/// column orders, or one may (transiently) carry a column the other lacks. A
/// positional `SELECT *` copy would then silently write values into the wrong
/// columns and corrupt the destination; enumerating the shared columns by name
/// makes the copy order-independent and drift-safe (issue #831).
fn shared_column_list(
    conn: &Connection,
    src_schema: &str,
    dst_schema: &str,
    table: &str,
) -> rusqlite::Result<String> {
    let columns_of = |schema: &str| -> rusqlite::Result<Vec<String>> {
        let mut stmt = conn.prepare(&format!(
            "PRAGMA {schema}.table_info(\"{}\")",
            table.replace('"', "\"\"")
        ))?;
        let cols = stmt
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(cols)
    };
    let dst: std::collections::HashSet<String> = columns_of(dst_schema)?.into_iter().collect();
    let list = columns_of(src_schema)?
        .into_iter()
        .filter(|c| dst.contains(c))
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(list)
}

/// Copies the given terminal instances (and their instance-scoped dependent rows)
/// from the live store on `conn` into the durable archive at `archive`
/// (`INSERT OR REPLACE`, append-only in effect since keys are unique and
/// monotonic). Runs outside the export transaction as autocommit statements: the
/// archive is a best-effort durability backstop, so cross-file atomicity is not
/// required (a torn capture is simply re-captured, and reprojection degrades to
/// the prior #732 behaviour for anything unarchived).
fn copy_terminal_to_archive(
    conn: &Connection,
    archive: &Path,
    keys: &[Key],
) -> rusqlite::Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    attach_archive(conn, archive)?;
    let result = (|| -> rusqlite::Result<()> {
        // Materialize the keys into a temp table rather than string-joining them
        // into a single inline `IN (...)` literal: a large catch-up/rebuild batch
        // can hold very many terminal keys, and an inline list would build an
        // enormous SQL statement (slow, and eventually past SQLite's SQL-length
        // limit). A temp table keeps every copy statement a fixed size regardless
        // of batch size, mirroring the eviction path's `_evict` pattern.
        conn.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS _terminal_copy(key INTEGER PRIMARY KEY);
             DELETE FROM _terminal_copy;",
        )?;
        {
            let mut insert =
                conn.prepare("INSERT OR IGNORE INTO _terminal_copy(key) VALUES (?1)")?;
            for k in keys {
                insert.execute(params![*k as i64])?;
            }
        }
        let pi_cols = shared_column_list(conn, "main", "terminal_archive", "process_instances")?;
        conn.execute(
            &format!(
                "INSERT OR REPLACE INTO terminal_archive.process_instances ({pi_cols}) \
                 SELECT {pi_cols} FROM main.process_instances \
                 WHERE key IN (SELECT key FROM _terminal_copy)"
            ),
            [],
        )?;
        for table in TERMINAL_ARCHIVE_DEP_TABLES {
            let cols = shared_column_list(conn, "main", "terminal_archive", table)?;
            conn.execute(
                &format!(
                    "INSERT OR REPLACE INTO terminal_archive.{table} ({cols}) \
                     SELECT {cols} FROM main.{table} \
                     WHERE instance_key IN (SELECT key FROM _terminal_copy)"
                ),
                [],
            )?;
        }
        conn.execute_batch("DELETE FROM _terminal_copy")?;
        Ok(())
    })();
    // Always detach, even on error, so a later attach does not fail with
    // "database terminal_archive is already in use".
    let _ = conn.execute_batch("DETACH DATABASE terminal_archive");
    result
}

// --- enum <-> integer code mappings (kept beside the engine enums) ---

const fn instance_state_code(s: ProcessInstanceState) -> i64 {
    match s {
        ProcessInstanceState::Active => 0,
        ProcessInstanceState::Completed => 1,
        ProcessInstanceState::Terminated => 2,
        // A transient cancelling state (ADR 0037 §6): the instance's tokens are
        // discarded and a `canceling` task-listener chain is draining before it
        // becomes `Terminated`. Projected as its own code so the read model can
        // show "cancelling".
        ProcessInstanceState::Terminating => 3,
    }
}

/// The terminal process-instance state codes (`Completed`, `Terminated`),
/// **derived** from the canonical [`instance_state_code`] mapping rather than
/// hard-coded. Every terminal-selection query (eviction, adaptive pruning,
/// terminal-archive copy/backfill — issue #831) builds its `state IN (...)`
/// predicate from this single source via [`terminal_state_predicate`], so the
/// SQL can never drift from the enum-to-int codes.
const TERMINAL_INSTANCE_STATE_CODES: [i64; 2] = [
    instance_state_code(ProcessInstanceState::Completed),
    instance_state_code(ProcessInstanceState::Terminated),
];

/// Builds a `<column> IN (<terminal codes>)` SQL predicate from
/// [`TERMINAL_INSTANCE_STATE_CODES`]. Use this instead of writing a literal
/// `state IN (1, 2)`, so the terminal-state code set has one source of truth.
fn terminal_state_predicate(column: &str) -> String {
    let [completed, terminated] = TERMINAL_INSTANCE_STATE_CODES;
    format!("{column} IN ({completed}, {terminated})")
}
fn instance_state_from(code: i64) -> ProcessInstanceState {
    match code {
        1 => ProcessInstanceState::Completed,
        2 => ProcessInstanceState::Terminated,
        3 => ProcessInstanceState::Terminating,
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

/// Lifecycle state of an element (flow-node) instance, mirroring Camunda 8's
/// `ElementInstanceStateEnum`. Stored as the integer code below.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ElementInstanceState {
    Active,
    Completed,
    Terminated,
}

fn element_instance_state_code(s: ElementInstanceState) -> i64 {
    match s {
        ElementInstanceState::Active => 0,
        ElementInstanceState::Completed => 1,
        ElementInstanceState::Terminated => 2,
    }
}
fn element_instance_state_from(code: i64) -> ElementInstanceState {
    match code {
        1 => ElementInstanceState::Completed,
        2 => ElementInstanceState::Terminated,
        _ => ElementInstanceState::Active,
    }
}

/// Wall-clock milliseconds since the Unix epoch, used to stamp element-instance
/// start/end dates at projection time (the lifecycle events carry no
/// engine-authored timestamp — see [`ElementInstanceRow`]).
///
/// The clock source is per-platform: the same projection runs on the gateway
/// server (`native`) and the in-browser test engine (`wasm`), which have
/// different clocks. Only the *source* differs; the value semantics (Unix-epoch
/// milliseconds) are identical, so the projection code above is unchanged.
#[cfg(not(target_arch = "wasm32"))]
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `wasm32-unknown-unknown` has no platform clock — `std::time::SystemTime::now()`
/// unconditionally panics there ("time not implemented on this platform"), which
/// would abort the projection on its very first `export`. Read the wall clock
/// from JavaScript's `Date.now()` (available in both the browser and node)
/// instead: the exact browser/node analog of the native Unix-epoch millisecond
/// clock, so element-instance timestamps stay meaningful and the projection runs
/// without panicking.
#[cfg(target_arch = "wasm32")]
fn now_ms() -> u64 {
    js_sys::Date::now() as u64
}

/// Encodes a [`JobKind`] into the `(job_kind, listener_event_type)` column pair
/// stored on the jobs read-model row (ADR 0037). Ordinary element jobs store
/// `(0, 0)`; execution-listener jobs store `(1, start=0/end=1)`; task-listener
/// jobs store `(2, creating=0/assigning=1/updating=2/completing=3/canceling=4)`.
/// The listener index/scope are not projected.
fn job_kind_codes(kind: &JobKind) -> (i64, i64) {
    match kind {
        JobKind::BpmnElement => (0, 0),
        JobKind::ExecutionListener { event_type, .. } => (
            1,
            match event_type {
                ListenerEventType::Start => 0,
                ListenerEventType::End => 1,
            },
        ),
        JobKind::TaskListener { event_type, .. } => (
            2,
            match event_type {
                TaskListenerEventType::Creating => 0,
                TaskListenerEventType::Assigning => 1,
                TaskListenerEventType::Updating => 2,
                TaskListenerEventType::Completing => 3,
                TaskListenerEventType::Canceling => 4,
            },
        ),
    }
}

/// Reconstructs the display-relevant [`JobKind`] from the stored column pair.
/// The listener index/scope/user-task key are not persisted, so they default
/// to `0`.
fn job_kind_from(job_kind: i64, listener_event_type: i64) -> JobKind {
    match job_kind {
        1 => JobKind::ExecutionListener {
            event_type: if listener_event_type == 1 {
                ListenerEventType::End
            } else {
                ListenerEventType::Start
            },
            index: 0,
            scope: 0,
        },
        2 => JobKind::TaskListener {
            event_type: match listener_event_type {
                1 => TaskListenerEventType::Assigning,
                2 => TaskListenerEventType::Updating,
                3 => TaskListenerEventType::Completing,
                4 => TaskListenerEventType::Canceling,
                _ => TaskListenerEventType::Creating,
            },
            index: 0,
            user_task_key: 0,
        },
        _ => JobKind::BpmnElement,
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
        IncidentKind::DecisionEvaluation => 4,
        IncidentKind::CalledElementError => 5,
    }
}
fn incident_kind_from(code: i64) -> IncidentKind {
    match code {
        1 => IncidentKind::NoMatchingSequenceFlow,
        2 => IncidentKind::UnhandledError,
        3 => IncidentKind::ExpressionEvaluation,
        4 => IncidentKind::DecisionEvaluation,
        5 => IncidentKind::CalledElementError,
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
    /// C8 parent linkage for a call-activity **child** process instance: the
    /// calling instance's key and the spawning call-activity element instance
    /// key. Both `None` for a top-level instance. Surfaced so the C8
    /// `parentProcessInstanceKey` field/filter return real data.
    pub parent_process_instance_key: Option<Key>,
    pub parent_element_instance_key: Option<Key>,
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
    /// Ordinary BPMN-element job or an execution-listener job (ADR 0037). Only
    /// the display-relevant discriminant (kind + listener event type) is
    /// preserved in the read model; the listener index/scope are not projected.
    pub kind: JobKind,
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
    pub form_key: Option<Key>,
    pub external_form_reference: Option<String>,
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

/// A projected element (flow-node) instance record, materialized from the
/// engine's per-element lifecycle events (`ElementActivating`/`ElementActivated`
/// /`ElementCompleted`) for the element-instance query API. One row per element
/// instance the engine activates.
///
/// `start_date_ms`/`end_date_ms` are stamped at projection time (the lifecycle
/// events carry no engine-authored timestamp), so a full read-model rebuild from
/// the journal re-dates them; engine-authored element timestamps are a follow-up.
pub struct ElementInstanceRow {
    pub element_instance_key: Key,
    pub instance_key: Key,
    pub process_definition_id: String,
    pub process_definition_key: String,
    pub element_id: String,
    pub element_name: Option<String>,
    /// Camunda element `type` spelling (e.g. `SERVICE_TASK`), resolved from the
    /// deployed model via `definition_elements`; `UNKNOWN` when unresolved.
    pub element_type: String,
    pub state: ElementInstanceState,
    pub start_date_ms: u64,
    pub end_date_ms: Option<u64>,
    /// The scope-owning element instance (enclosing sub-process/multi-instance
    /// body), or `0` for the process-level scope.
    pub scope_key: Key,
    pub incident_key: Option<Key>,
    pub has_incident: bool,
    pub tenant_id: String,
}

/// A projected open message subscription: a running element instance parked on a
/// message catch (intermediate catch event, receive task or message boundary),
/// materialized from `MessageSubscriptionCreated`. Interrupting subscriptions are
/// dropped on `MessageCorrelated`/`RemoteMessageCorrelation`; a non-interrupting
/// boundary subscription stays open (it can correlate repeatedly) and is dropped
/// only on `MessageSubscriptionCanceled` or instance termination. Each correlation
/// is additionally recorded in `correlated_message_subscriptions`.
/// Feeds the MESSAGE variant of the element-instance wait-state API.
pub struct MessageSubscriptionRow {
    pub subscription_key: Key,
    pub instance_key: Key,
    pub element_instance_key: Key,
    pub element_id: String,
    pub message_name: String,
    pub correlation_key: String,
    /// Projection-time timestamp (ms since epoch) of when this subscription row
    /// was first materialised; surfaces as `lastUpdatedDate` in the search API.
    pub created_at_ms: u64,
}

/// A projected *correlated* message subscription: the historical record of a
/// message that correlated to an instance-scoped subscription, materialized from
/// `MessageCorrelated`/`RemoteMessageCorrelation` (capturing the message name and
/// correlation key from the open subscription row before it is dropped). Feeds
/// the `searchCorrelatedMessageSubscriptions` API. Unlike open subscriptions,
/// these rows are retained after the subscription settles.
pub struct CorrelatedMessageSubscriptionRow {
    pub message_key: Key,
    pub subscription_key: Key,
    pub instance_key: Key,
    pub element_instance_key: Key,
    pub element_id: String,
    pub message_name: String,
    pub correlation_key: String,
    /// Projection-time timestamp (ms since epoch) of the correlation.
    pub correlation_time_ms: u64,
    /// The id of the partition that correlated the message.
    pub partition_id: i32,
}

pub struct ProcessDefinitionRow {
    pub key: Key,
    pub process_id: String,
    pub version: i32,
    pub name: Option<String>,
    pub is_latest: bool,
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

/// A projected decision-instance record (one per evaluated decision in a
/// `businessRuleTask`'s decision evaluation), materialized from
/// [`Event::DecisionEvaluated`] for the DecisionInstance query API.
pub struct DecisionInstanceRow {
    /// `<decisionEvaluationKey>-<index>`, the decision instance's unique id.
    pub eval_instance_key: String,
    pub decision_evaluation_key: Key,
    /// 1-based index of this decision within its evaluation.
    pub idx: i64,
    pub decision_id: String,
    pub decision_key: Key,
    pub decision_name: String,
    /// Camunda decision type spelling (e.g. `DECISION_TABLE`).
    pub decision_type: String,
    pub version: i32,
    pub decision_requirements_id: String,
    pub decision_requirements_key: Key,
    pub root_decision_key: Key,
    pub instance_key: Key,
    pub element_instance_key: Key,
    /// The owning process definition key (decimal string), or empty when the
    /// instance row is not colocated in this shard.
    pub process_definition_key: String,
    /// Camunda decision-instance state spelling (`EVALUATED` / `FAILED`).
    pub state: String,
    pub evaluation_failure: Option<String>,
    pub evaluation_date_ms: u64,
    /// The decision output as a JSON-document string.
    pub result_json: String,
    /// Serialized `Vec<EvaluatedInput>` (engine-core DMN audit).
    pub inputs_json: String,
    /// Serialized `Vec<MatchedRule>` (engine-core DMN audit).
    pub rules_json: String,
    pub tenant_id: String,
}

/// A projected decision-requirements-graph (one per DRG id, latest version).
#[derive(Debug, Clone)]
pub struct DecisionRequirementsRow {
    pub drg_id: String,
    pub drg_key: Key,
    pub name: String,
    pub version: i32,
    /// Synthesized `{drg_id}.dmn` (the engine does not retain the original name).
    pub resource_name: String,
    /// The verbatim DMN XML the graph was parsed from (empty for graphs built
    /// programmatically rather than parsed).
    pub xml: String,
}

/// A projected decision definition (one per decision id, latest version) with its
/// owning DRG's id/name/version denormalized in for querying.
#[derive(Debug, Clone)]
pub struct DecisionDefinitionRow {
    pub decision_id: String,
    pub decision_key: Key,
    pub name: String,
    pub version: i32,
    pub decision_requirements_key: Key,
    pub decision_requirements_id: String,
    pub decision_requirements_name: String,
    pub decision_requirements_version: i32,
}

/// A projected form row, one per deployed form version (keyed by `form_key`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormRow {
    pub form_id: String,
    pub form_key: Key,
    pub version: i32,
    pub schema: String,
    pub resource_name: String,
    pub tenant_id: String,
}

/// A projected generic-resource row, one per deployed resource version (keyed by
/// `resource_key`). For a generic resource `resource_id` equals `resource_name`
/// (the filename); versions increment per `resource_id`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceRow {
    pub resource_key: Key,
    pub resource_id: String,
    pub resource_name: String,
    pub version: i32,
    pub version_tag: Option<String>,
    pub content: String,
    pub tenant_id: String,
}

/// The metadata-only projection of a generic-resource row — every column of
/// [`ResourceRow`] except the (potentially large) `content` blob. Used by the
/// list/search path (`searchResources`), whose response returns only metadata,
/// so it never pays to read `content` for every projected version. Full content
/// is reserved for the by-key endpoints (`getResource*`), mirroring how process
/// definitions list only `key/process_id/version` and fetch the BPMN `xml`
/// separately by key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceMetaRow {
    pub resource_key: Key,
    pub resource_id: String,
    pub resource_name: String,
    pub version: i32,
    pub version_tag: Option<String>,
    pub tenant_id: String,
}

/// Minimum `-wal` sidecar size (bytes) before the adaptive pruner spends a
/// `wal_checkpoint(TRUNCATE)`. Below this the raised autocheckpoint keeps the WAL
/// bounded, so the pruner skips the extra copy-back+truncate — cutting the former
/// ~5/s TRUNCATE storm (each a full-WAL copy-back into the random-access main DB)
/// down to an occasional file-space reclaim. Default 32 MiB, deliberately below
/// the autocheckpoint backstop so the pruner (off the exporter thread) does the
/// checkpointing first and the exporter's inline autocheckpoint rarely fires.
/// `NANOBPMN_READ_WAL_TRUNCATE_MB` overrides (0 = checkpoint on every pruner wake,
/// the pre-throttle behavior, for A/B).
#[cfg(feature = "native")]
fn read_wal_truncate_bytes() -> u64 {
    std::env::var("NANOBPMN_READ_WAL_TRUNCATE_MB")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(32)
        * 1024
        * 1024
}

/// Reads a SQLite database's size as `(file_bytes, live_bytes)` from its header:
/// `file_bytes = page_count × page_size` (the whole allocated file, freelist
/// included) and `live_bytes = (page_count − freelist_count) × page_size` (the
/// pages holding actual data). All three PRAGMAs are O(1) header reads, so this
/// is cheap enough for a hot loop.
///
/// This is the single canonical implementation of read-model space accounting.
/// The `server` crate re-exports it as `crate::sqlite_space::page_stats` so its
/// disk-relative retention / var-spill sizing reuse the exact same derivation
/// (no drift surface). Native-only: it underpins the server-only pruning and
/// disk-sizing methods below, none of which exist on the wasm (in-memory) build.
#[cfg(feature = "native")]
pub fn page_stats(conn: &Connection) -> (u64, u64) {
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
#[cfg(feature = "native")]
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
        &format!(
            "INSERT INTO _evict(key) \
             SELECT key FROM process_instances WHERE {} \
             ORDER BY key ASC LIMIT ?1",
            terminal_state_predicate("state")
        ),
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
    /// In-process write-coordination lock shared by the two independent SQLite
    /// writers on this shard's WAL file: the exporter (`export`, on `conn`) and
    /// the decoupled adaptive pruner (`adaptive_prune_once`, on the separate
    /// connection from `prune_connection`). WAL permits only one writer, so a
    /// long pruner delete + `wal_checkpoint(TRUNCATE)` would otherwise trip the
    /// other connection's `busy_timeout` and surface as `database is locked`
    /// (dropped/retried export batches — see #96/#97). Gating both writers on
    /// this mutex turns that cross-connection SQLite lock race into a cheap
    /// in-process wait, so their writes interleave cleanly and never error.
    /// Held only around actual write statements and short enough (the pruner
    /// acquires it per delete chunk) that neither side is starved; reads
    /// (`page_stats`, request-handler queries) never take it.
    write_lock: Mutex<()>,
    /// The shard's on-disk path (None for `:memory:`). Retained so the decoupled
    /// adaptive pruner can open its own second connection to the same WAL file and
    /// evict on an independent schedule, rather than competing for CPU with
    /// projection inside the single exporter thread. Only the `native` backend's
    /// server-only orchestration reads it; the wasm (in-memory) build never has a
    /// path, so the field is inert there.
    #[cfg_attr(not(feature = "native"), allow(dead_code))]
    path: Option<PathBuf>,
    /// Path of the co-located **durable terminal-audit archive** (issue #831), or
    /// `None` for `:memory:` stores and for the archive store itself (which never
    /// nests an archive). Completed/terminal instances are copied here as they
    /// become terminal, so that history survives a below-floor snapshot
    /// reprojection (#732) — the reprojection only recovers *live* instances from
    /// the engine snapshot, and terminal instances lived **only** in the read
    /// model. See [`ReadStore::archive_terminal_instances`] /
    /// [`ReadStore::replay_terminal_archive`].
    archive_path: Option<PathBuf>,
}

/// Result of projecting a batch of events into the read model.
pub struct ExportOutcome {
    /// Keys of instances that made a *genuine* Active->terminal transition in
    /// this batch (idempotent re-deliveries excluded). Used to evict hot engine
    /// state exactly once per instance.
    pub terminal_keys: Vec<Key>,
    /// Exact net change to the in-flight instance gauge for this batch:
    /// `+genuine_creates - genuine_terminals`. Because it counts only real state
    /// transitions (never raw event occurrences), re-delivered create/terminal
    /// events contribute zero, so the gauge cannot drift under idempotent replay.
    pub inflight_delta: i64,
}

impl ReadStore {
    /// Opens the read store at `path`, or an in-memory database when `path` is
    /// `None`. A persistent database whose schema version lags is brought current
    /// via **non-destructive additive migration** ([`reconcile_to_schema`]), so
    /// existing rows are preserved (issue #831). A destructive drop-and-recreate
    /// is reserved for the case where the live schema is genuinely incompatible
    /// with an additive migration (or cannot be read); a subsequent rebuild from
    /// the journal then repopulates it.
    pub fn open(path: Option<&Path>) -> rusqlite::Result<Self> {
        Self::open_inner(path, true)
    }

    /// Co-located durable terminal-audit archive path for a read-model file:
    /// `read-model.sqlite` -> `read-model.terminal-archive.sqlite` (issue #831).
    fn terminal_archive_path(base: &Path) -> PathBuf {
        let stem = base
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "read-model".into());
        let name = match base.extension() {
            Some(ext) => format!("{stem}.terminal-archive.{}", ext.to_string_lossy()),
            None => format!("{stem}.terminal-archive"),
        };
        base.with_file_name(name)
    }

    /// Opens the read store. When `with_archive` and the store is file-backed, a
    /// co-located durable terminal-audit archive is opened once (creating/migrating
    /// its schema — it shares [`SCHEMA`], so it benefits from the same additive
    /// migrations and is never destructively wiped) and its path is retained.
    fn open_inner(path: Option<&Path>, with_archive: bool) -> rusqlite::Result<Self> {
        let conn = backend::open_connection(path)?;
        let archive_path = match (with_archive, path) {
            (true, Some(p)) => {
                let ap = Self::terminal_archive_path(p);
                // Open once to create/migrate the archive schema and prove it is
                // writable, then drop the connection — capture/replay re-attach it.
                Self::open_inner(Some(&ap), false)?;
                Some(ap)
            }
            _ => None,
        };
        let store = Self {
            conn: Mutex::new(conn),
            write_lock: Mutex::new(()),
            path: path.map(|p| p.to_path_buf()),
            archive_path,
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
        // One-time capture of any terminal history that predates the archive, so
        // it is durable from the first boot of this fix (issue #831).
        if with_archive && path.is_some() {
            store.backfill_terminal_archive();
        }
        Ok(store)
    }

    /// Performs a trivial no-op write to confirm the database (and the directory
    /// it lives in) are writable. A self-assignment changes no data but still
    /// opens a write transaction and creates the rollback journal, exercising
    /// both file and directory permissions.
    fn check_writable(&self) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.execute("UPDATE meta SET v = v WHERE k = 'schema_fingerprint'", [])?;
        Ok(())
    }

    fn ensure_schema(&self) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("read store poisoned");
        Self::ensure_schema_on(&conn)
    }

    /// Brings the database at `conn` up to [`SCHEMA`] **non-destructively**
    /// (issue #831). Three cases:
    ///
    /// * **Fresh** (no user tables): create the whole schema from [`SCHEMA`] and
    ///   stamp `schema_version`/`exported_position = 0`.
    /// * **Already current** (`meta.schema_version == SCHEMA_VERSION`): nothing to
    ///   do — the fast path on every warm restart.
    /// * **Older, or a legacy fingerprint-only database**: additively reconcile
    ///   the live schema up to [`SCHEMA`] (add missing tables/columns/indexes,
    ///   never dropping data) and stamp the new version. `exported_position` is
    ///   preserved, so the projection is **not** reset below the compaction floor
    ///   — this is exactly the case that used to wipe completed history.
    fn ensure_schema_on(conn: &Connection) -> rusqlite::Result<()> {
        let user_tables = list_user_tables(conn)?;
        if user_tables.is_empty() {
            create_fresh_schema(conn)?;
            return Ok(());
        }
        let stored_version: Option<i64> = conn
            .query_row("SELECT v FROM meta WHERE k = 'schema_version'", [], |r| {
                r.get(0)
            })
            .optional()
            .unwrap_or(None);
        if stored_version == Some(SCHEMA_VERSION) {
            return Ok(());
        }
        // Older (or legacy fingerprint-only) database: migrate forward without
        // dropping any data. If the live schema is genuinely incompatible with an
        // additive migration (e.g. a foreign table missing a NOT NULL column that
        // cannot be back-filled), fall back to a destructive rebuild — the #732
        // below-floor recovery then reprojects live instances from the engine
        // snapshot. Additive evolution (the common case) never reaches this.
        if let Err(migrate_err) = reconcile_to_schema(conn) {
            tracing::warn!(
                error = %migrate_err,
                "read-model schema could not be additively migrated to the current \
                 version; rebuilding from scratch (live instances are recovered by the \
                 engine-snapshot reprojection, issue #732/#831)"
            );
            drop_all_user_tables(conn)?;
            create_fresh_schema(conn)?;
            return Ok(());
        }
        // Stamp version monotonically (never downgrade a database written by a
        // newer binary) and keep the fingerprint row in sync for tooling. Seed
        // `exported_position` only if absent — a warm database keeps its cursor,
        // so the projection is never reset below the compaction floor.
        let new_version = stored_version.map_or(SCHEMA_VERSION, |v| v.max(SCHEMA_VERSION));
        conn.execute(
            "INSERT INTO meta (k, v) VALUES ('schema_version', ?1) \
             ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            params![new_version],
        )?;
        conn.execute(
            "INSERT INTO meta (k, v) VALUES ('schema_fingerprint', ?1) \
             ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            params![schema_fingerprint()],
        )?;
        conn.execute(
            "INSERT OR IGNORE INTO meta (k, v) VALUES ('exported_position', 0)",
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

    /// Advances `exported_position` by `n` events **without** projecting them
    /// into the instance/variable tables. Used by the remote-only projection
    /// sink: the heavy projection is offloaded to a remote target, but this
    /// shard is still the journal-compaction watermark keeper, so it must track
    /// how far the log has been handed off. This is a single integer `UPDATE`
    /// (tens of bytes of WAL) per batch — negligible next to full projection —
    /// so it removes the read model's dominant disk cost while keeping the
    /// compaction watermark honest. `n == 0` is a no-op.
    ///
    /// NOTE: like `export`, the watermark advances once the batch has been
    /// handed to the sink, not once a remote target has durably acknowledged it;
    /// ack-gated advancement (so a crash cannot compact past an un-acked batch)
    /// is a later milestone (see issue #133).
    pub fn advance_exported(&self, n: usize) -> rusqlite::Result<()> {
        if n == 0 {
            return Ok(());
        }
        let _write = self
            .write_lock
            .lock()
            .expect("read store write lock poisoned");
        let conn = self.conn.lock().expect("read store poisoned");
        conn.execute(
            "UPDATE meta SET v = v + ?1 WHERE k = 'exported_position'",
            params![n as i64],
        )?;
        Ok(())
    }

    /// Drops **every** user table and recreates the schema from scratch,
    /// resetting `exported_position` to 0. This is the sole remaining
    /// *destructive* rebuild path (contrast [`ReadStore::ensure_schema`], which is
    /// now additive — issue #831). It is used only when the persisted position is
    /// ahead of the journal (a corrupt or truncated log), forcing a full rebuild
    /// by replay, or by the below-floor reprojection recovery (issue #732), which
    /// immediately reseeds from the authoritative engine snapshot afterwards.
    ///
    /// The drop set is derived from `sqlite_master` (not a hand-maintained list,
    /// which silently drifts as `SCHEMA` gains tables), so it can never fall
    /// behind `SCHEMA`.
    pub fn reset(&self) -> rusqlite::Result<()> {
        // Serialize against the adaptive pruner's separate connection in-process
        // (see `write_lock`), exactly like `export`/`advance_exported`: `reset`
        // performs destructive DDL, so running it concurrently with a pruning /
        // export / replay WAL writer would race SQLite's lock and trip
        // `database is locked`.
        let _write = self
            .write_lock
            .lock()
            .expect("read store write lock poisoned");
        let conn = self.conn.lock().expect("read store poisoned");
        drop_all_user_tables(&conn)?;
        create_fresh_schema(&conn)?;
        Ok(())
    }

    /// Projects a batch of consecutive journal `events` into the store in one
    /// transaction and advances `exported_position` by `events.len()`. Returns
    /// the keys of instances that completed in this batch, so the caller can
    /// evict them from hot engine state. Projection is idempotent, so replaying
    /// an overlapping prefix is safe. Takes event references so a caller batching
    /// several `Arc<Vec<Event>>` can project them without deep-copying payloads.
    pub fn export(&self, events: &[&Event]) -> rusqlite::Result<ExportOutcome> {
        // Serialize against the adaptive pruner's separate connection in-process
        // (see `write_lock`) so the two WAL writers never race SQLite's lock and
        // trip `database is locked`; this is a cheap uncontended lock on the
        // common path (pruner idle) and a short wait when the pruner is active.
        let _write = self
            .write_lock
            .lock()
            .expect("read store write lock poisoned");
        let mut conn = self.conn.lock().expect("read store poisoned");
        let tx = conn.transaction()?;
        let mut terminal_keys = Vec::new();
        let mut inflight_delta: i64 = 0;
        let now = now_ms();
        for &event in events {
            let d = project(&tx, event, now)?;
            inflight_delta += d;
            // Collect only GENUINE terminal transitions (d < 0) for hot-state
            // eviction; a re-delivered terminal (d == 0) was already evicted.
            if d < 0
                && let Event::ProcessInstanceCompleted { instance_key }
                | Event::ProcessInstanceTerminated { instance_key } = event
            {
                terminal_keys.push(*instance_key);
            }
        }
        tx.cexecute(
            "UPDATE meta SET v = v + ?1 WHERE k = 'exported_position'",
            params![events.len() as i64],
        )?;
        tx.commit()?;
        // Durably archive any newly-terminal instances (issue #831) so their
        // audit history survives a below-floor snapshot reprojection, which can
        // only recover *live* instances from the engine snapshot. Best-effort: a
        // capture failure must never fail the (already-committed) projection or
        // stall the exporter, so it is logged and swallowed — the read model still
        // holds the terminal rows until they are pruned, and the next reprojection
        // path degrades to the prior (#732) behaviour for anything unarchived.
        if !terminal_keys.is_empty()
            && let Some(archive) = &self.archive_path
            && let Err(e) = copy_terminal_to_archive(&conn, archive, &terminal_keys)
        {
            tracing::warn!(
                error = %e,
                count = terminal_keys.len(),
                "failed to write terminal instances to the durable audit archive \
                 (issue #831); the read model still holds them until pruned"
            );
        }
        Ok(ExportOutcome {
            terminal_keys,
            inflight_delta,
        })
    }

    /// Copies every terminal instance currently in the read model into the
    /// durable archive **once** (issue #831), gated by a `meta` flag so it runs a
    /// single time per (re)built read model. This captures history that completed
    /// *before* the archive existed — the exact merlin.local data that was
    /// unrecoverable — so it becomes durable on the first boot of a binary carrying
    /// this fix, not only for instances that complete afterwards. Best-effort: a
    /// failure is logged and swallowed so it never blocks startup.
    fn backfill_terminal_archive(&self) {
        let Some(archive) = &self.archive_path else {
            return;
        };
        let conn = self.conn.lock().expect("read store poisoned");
        let done: i64 = conn
            .query_row(
                "SELECT v FROM meta WHERE k = 'terminal_archive_backfilled'",
                [],
                |r| r.get(0),
            )
            .optional()
            .unwrap_or(None)
            .unwrap_or(0);
        if done != 0 {
            return;
        }
        let result = (|| -> rusqlite::Result<()> {
            attach_archive(&conn, archive)?;
            let copy = (|| -> rusqlite::Result<()> {
                let terminal = terminal_state_predicate("state");
                let pi_cols =
                    shared_column_list(&conn, "main", "terminal_archive", "process_instances")?;
                conn.execute(
                    &format!(
                        "INSERT OR IGNORE INTO terminal_archive.process_instances ({pi_cols}) \
                         SELECT {pi_cols} FROM main.process_instances WHERE {terminal}"
                    ),
                    [],
                )?;
                for table in TERMINAL_ARCHIVE_DEP_TABLES {
                    let cols = shared_column_list(&conn, "main", "terminal_archive", table)?;
                    conn.execute(
                        &format!(
                            "INSERT OR IGNORE INTO terminal_archive.{table} ({cols}) \
                             SELECT {cols} FROM main.{table} WHERE instance_key IN \
                             (SELECT key FROM main.process_instances WHERE {terminal})"
                        ),
                        [],
                    )?;
                }
                Ok(())
            })();
            let _ = conn.execute_batch("DETACH DATABASE terminal_archive");
            copy?;
            conn.execute(
                "INSERT INTO meta (k, v) VALUES ('terminal_archive_backfilled', 1) \
                 ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                [],
            )?;
            Ok(())
        })();
        if let Err(e) = result {
            tracing::warn!(
                error = %e,
                "failed to backfill pre-existing terminal history into the durable \
                 audit archive (issue #831); newly-completing instances are still archived"
            );
        }
    }

    /// Replays the durable terminal-audit archive (issue #831) into this shard,
    /// restoring completed/terminal instances (and their user tasks, variables and
    /// decision evaluations) that a snapshot reprojection could not recover from
    /// the live-only engine snapshot. `INSERT OR IGNORE` so a live row from the
    /// snapshot is never clobbered by an older archived copy. Returns the number
    /// of process instances restored. A no-op for `:memory:` stores or when the
    /// archive file does not yet exist.
    pub fn replay_terminal_archive(&self) -> rusqlite::Result<usize> {
        let Some(archive) = &self.archive_path else {
            return Ok(0);
        };
        if !archive.exists() {
            return Ok(0);
        }
        let _write = self
            .write_lock
            .lock()
            .expect("read store write lock poisoned");
        let conn = self.conn.lock().expect("read store poisoned");
        attach_archive(&conn, archive)?;
        let restored = (|| -> rusqlite::Result<usize> {
            let pi_cols =
                shared_column_list(&conn, "terminal_archive", "main", "process_instances")?;
            let n = conn.execute(
                &format!(
                    "INSERT OR IGNORE INTO main.process_instances ({pi_cols}) \
                     SELECT {pi_cols} FROM terminal_archive.process_instances"
                ),
                [],
            )?;
            for table in TERMINAL_ARCHIVE_DEP_TABLES {
                let cols = shared_column_list(&conn, "terminal_archive", "main", table)?;
                conn.execute(
                    &format!(
                        "INSERT OR IGNORE INTO main.{table} ({cols}) \
                         SELECT {cols} FROM terminal_archive.{table}"
                    ),
                    [],
                )?;
            }
            Ok(n)
        })();
        let _ = conn.execute_batch("DETACH DATABASE terminal_archive");
        restored
    }

    /// Rebuilds this (reset) shard's rows from a boot engine [`State`] snapshot,
    /// WITHOUT advancing `exported_position` — the caller then plants the cursor
    /// at the absolute event count this `State` already reflects (typically
    /// `total_events`). Used only by the below-compaction-floor recovery
    /// path (issue #732), where the journal events that would replay into the read
    /// model have been compacted away but the authoritative engine snapshot still
    /// holds every live entity. Idempotent — reused rows are guarded with
    /// `ON CONFLICT ... DO NOTHING` (mirroring the event projector), so it is
    /// safe over a freshly `reset()` shard.
    pub fn seed_from_engine_state(
        &self,
        state: &nanobpmn_engine_core::State,
    ) -> rusqlite::Result<()> {
        let _write = self
            .write_lock
            .lock()
            .expect("read store write lock poisoned");
        let mut conn = self.conn.lock().expect("read store poisoned");
        let tx = conn.transaction()?;
        project_engine_state(&tx, state, now_ms())?;
        tx.commit()?;
        Ok(())
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
    #[cfg(feature = "native")]
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
            &format!(
                "INSERT INTO _evict(key) \
                 SELECT key FROM ( \
                   SELECT key FROM process_instances WHERE {} \
                   ORDER BY key DESC LIMIT -1 OFFSET ?1 \
                 ) ORDER BY key ASC LIMIT ?2",
                terminal_state_predicate("state")
            ),
            params![max_keep as i64, del_limit],
        )?;
        if evicted == 0 {
            tx.commit()?;
            return Ok(0);
        }
        for table in [
            "variables",
            "jobs",
            "incidents",
            "user_tasks",
            "element_instances",
        ] {
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
        // Return the WAL's freed pages to a bounded size — but only once it has
        // grown meaningfully. TRUNCATE-ing after every sweep copies the whole WAL
        // back into the random-access main DB and was a dominant write-amplifier;
        // size-gating it (default 32 MiB) keeps the WAL bounded via the raised
        // autocheckpoint and reclaims file space only occasionally. TRUNCATE is
        // best-effort: a concurrent reader can hold it back, and that is fine —
        // the next sweep retries.
        if self.wal_len_bytes() >= read_wal_truncate_bytes() {
            let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
        }
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
    #[cfg(feature = "native")]
    pub fn db_page_stats(&self) -> (u64, u64) {
        let conn = self.conn.lock().expect("read store poisoned");
        page_stats(&conn)
    }

    /// Opens a second connection to this shard's database file for the decoupled
    /// adaptive pruner (see [`prune_oldest_terminal`]). Returns `Ok(None)` for an
    /// in-memory store (a second connection would be a distinct empty database),
    /// so the caller keeps pruning inline in that case. This connection's delete
    /// transactions are serialized against the exporter by the shared in-process
    /// [`ReadStore::write_lock`] (see [`ReadStore::adaptive_prune_once`]), so the
    /// two writers never race SQLite's WAL lock; `busy_timeout` remains only as a
    /// backstop for any writer this process does not coordinate (e.g. an external
    /// reader holding a checkpoint back).
    #[cfg(feature = "native")]
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
    #[cfg(feature = "native")]
    pub fn adaptive_prune_once(
        &self,
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
            // Gate each delete chunk on the shared in-process write lock so the
            // pruner's connection never writes to the WAL while the exporter's
            // does (no `database is locked`); the lock is released between chunks
            // so the exporter interleaves and is never starved for a whole wake.
            let evicted = {
                let _write = self
                    .write_lock
                    .lock()
                    .expect("read store write lock poisoned");
                prune_oldest_terminal(conn, want)?
            };
            if evicted == 0 {
                break;
            }
            total += evicted;
        }
        if total > 0 {
            // Checkpointing is deferred to the size-gated `maybe_checkpoint_wal`
            // (called every pruner wake): TRUNCATE-ing the whole WAL back into the
            // random-access main DB after *every* delete sweep (~5/s under
            // sustained pressure) was a dominant source of read-model write
            // amplification. The deletes' freed pages sit in the WAL until the
            // next size-gated checkpoint, bounded by the raised autocheckpoint.
        }
        Ok(total)
    }

    /// Size of this shard's `-wal` sidecar file in bytes (0 if absent / in-memory).
    /// An O(1) `stat`; cheap enough for the pruner's per-wake gate.
    #[cfg(feature = "native")]
    fn wal_len_bytes(&self) -> u64 {
        let Some(path) = self.path.as_ref() else {
            return 0;
        };
        let mut wal = path.clone().into_os_string();
        wal.push("-wal");
        std::fs::metadata(std::path::PathBuf::from(wal))
            .map(|m| m.len())
            .unwrap_or(0)
    }

    /// Size-gated WAL checkpoint, run once per pruner wake on the pruner's own
    /// connection (off the exporter's hot path). When the `-wal` sidecar has grown
    /// to at least [`read_wal_truncate_bytes`], TRUNCATE-checkpoints it back into
    /// the main DB and reclaims the WAL file space; otherwise a no-op. This
    /// concentrates all checkpoint copy-back into infrequent, coalesced passes
    /// instead of a per-delete-sweep storm, and keeps those passes off the single
    /// exporter thread so projection never stalls mid-checkpoint. Returns whether
    /// a checkpoint ran. Best-effort: a concurrent reader can hold TRUNCATE back,
    /// which is fine — the next wake retries.
    #[cfg(feature = "native")]
    pub fn maybe_checkpoint_wal(&self, conn: &Connection) -> bool {
        self.checkpoint_wal_if_larger_than(conn, read_wal_truncate_bytes())
    }

    /// Core of [`maybe_checkpoint_wal`] with an explicit byte threshold (so tests
    /// can exercise the gate without racing a process-global env var).
    #[cfg(feature = "native")]
    fn checkpoint_wal_if_larger_than(&self, conn: &Connection, threshold: u64) -> bool {
        if self.path.is_none() {
            return false;
        }
        if self.wal_len_bytes() < threshold {
            return false;
        }
        let _write = self
            .write_lock
            .lock()
            .expect("read store write lock poisoned");
        let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
        true
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

    /// Reconciles orphaned `Active` rows against authoritative engine state.
    ///
    /// A read row can be stranded in `Active` (`state = 0`) when its CREATE was
    /// projected here but the matching terminal event never was — e.g. this shard
    /// projected the create while it led the partition, then leadership moved and
    /// the completion was applied+exported by the *new* leader, so the terminal
    /// transition never reached this read model. Such a row inflates
    /// [`active_instance_count`](Self::active_instance_count) — and hence the
    /// in-flight admission gauge it seeds at boot — forever, even though the
    /// engine holds no such live instance (the engine evicts an instance the
    /// instant it reaches a terminal state).
    ///
    /// `live` is the set of instance keys the engine actually holds (hot ∪ cold)
    /// for the partitions this shard covers. Every `Active` row whose key is
    /// absent from `live` is transitioned to `Completed` (best effort: the engine
    /// evicted it on reaching a terminal state, and completion is the dominant
    /// drain path). The `WHERE state = 0` guard makes this idempotent and safe to
    /// race with a genuinely in-flight completion event: whichever applies first
    /// transitions the row, the other is a no-op, so the instance is counted
    /// exactly once. Returns the number of rows reconciled — the amount by which
    /// the in-flight gauge was over-counting.
    pub fn reconcile_orphaned_active(&self, live: &std::collections::HashSet<Key>) -> usize {
        let mut conn = self.conn.lock().expect("read store poisoned");
        let active: Vec<i64> = {
            let mut stmt = match conn.prepare("SELECT key FROM process_instances WHERE state = 0") {
                Ok(s) => s,
                Err(_) => return 0,
            };
            let rows = match stmt.query_map([], |r| r.get::<_, i64>(0)) {
                Ok(r) => r,
                Err(_) => return 0,
            };
            rows.filter_map(|r| r.ok()).collect()
        };
        let orphans: Vec<i64> = active
            .into_iter()
            .filter(|k| !live.contains(&(*k as Key)))
            .collect();
        if orphans.is_empty() {
            return 0;
        }
        let tx = match conn.transaction() {
            Ok(t) => t,
            Err(_) => return 0,
        };
        let completed = instance_state_code(ProcessInstanceState::Completed);
        let resolved = incident_state_code(IncidentState::Resolved);
        let active_inc = incident_state_code(IncidentState::Active);
        let mut reconciled = 0usize;
        for k in &orphans {
            if let Ok(1) = tx.cexecute(
                "UPDATE process_instances SET state = ?2, has_incident = 0 \
                 WHERE key = ?1 AND state = 0",
                params![*k, completed],
            ) {
                reconciled += 1;
            }
            // Close any still-open incident so the reconciled instance does not
            // surface as having an open incident.
            let _ = tx.cexecute(
                "UPDATE incidents SET state = ?2 WHERE instance_key = ?1 AND state = ?3",
                params![*k, resolved, active_inc],
            );
        }
        if tx.commit().is_err() {
            return 0;
        }
        reconciled
    }

    pub fn process_instances(&self) -> Vec<ProcessInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT key, process_id, process_definition_id, process_definition_key, \
                 version, state, start_date_ms, has_incident, tags, business_id, parent_process_instance_key, parent_element_instance_key FROM process_instances",
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
                 version, state, start_date_ms, has_incident, tags, business_id, \
                 parent_process_instance_key, parent_element_instance_key \
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
             version, state, start_date_ms, has_incident, tags, business_id, parent_process_instance_key, parent_element_instance_key FROM process_instances WHERE key = ?1",
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
                 retries, worker, deadline_ms, process_definition_id, process_definition_key, \
                 job_kind, listener_event_type \
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
                 process_definition_version, form_key, external_form_reference \
                 FROM user_tasks",
            )
            .expect("prepare user_tasks");
        let rows = stmt.query_map([], map_user_task).expect("query user_tasks");
        rows.filter_map(Result::ok).collect()
    }

    pub fn user_task(&self, key: Key) -> Option<UserTaskRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT key, instance_key, element_instance_key, element_id, state, \
             assignee, candidate_groups, candidate_users, due_date, follow_up_date, \
             priority, created_at_ms, process_definition_id, process_definition_key, \
             process_definition_version, form_key, external_form_reference \
             FROM user_tasks WHERE key = ?1",
            params![key as i64],
            map_user_task,
        )
        .optional()
        .expect("query user_task")
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

    /// All element-instance rows in this shard.
    pub fn element_instances(&self) -> Vec<ElementInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {ELEMENT_INSTANCE_COLS} FROM element_instances"
            ))
            .expect("prepare element_instances");
        let rows = stmt
            .query_map([], map_element_instance)
            .expect("query element_instances");
        rows.filter_map(Result::ok).collect()
    }

    /// A single element instance by its key.
    pub fn element_instance(&self, key: Key) -> Option<ElementInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            &format!(
                "SELECT {ELEMENT_INSTANCE_COLS} FROM element_instances WHERE element_instance_key = ?1"
            ),
            params![key as i64],
            map_element_instance,
        )
        .optional()
        .expect("query element_instance")
    }

    /// The `Active` element instances for one process instance (its live token
    /// positions). Selects by `instance_key` via `idx_element_instances_instance`
    /// and filters to the `Active` state code in SQL, so this stays O(rows for
    /// this instance) rather than scanning every element instance in the shard.
    pub fn active_element_instances(&self, instance_key: Key) -> Vec<ElementInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {ELEMENT_INSTANCE_COLS} FROM element_instances \
                 WHERE instance_key = ?1 AND state = ?2"
            ))
            .expect("prepare active_element_instances");
        let rows = stmt
            .query_map(
                params![
                    instance_key as i64,
                    element_instance_state_code(ElementInstanceState::Active)
                ],
                map_element_instance,
            )
            .expect("query active_element_instances");
        rows.filter_map(Result::ok).collect()
    }

    /// All open message subscriptions in this shard (MESSAGE wait states).
    pub fn message_subscriptions(&self) -> Vec<MessageSubscriptionRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {MESSAGE_SUBSCRIPTION_COLS} FROM message_subscriptions"
            ))
            .expect("prepare message_subscriptions");
        let rows = stmt
            .query_map([], map_message_subscription)
            .expect("query message_subscriptions");
        rows.filter_map(Result::ok).collect()
    }

    /// All correlated (historical) message subscriptions in this shard.
    pub fn correlated_message_subscriptions(&self) -> Vec<CorrelatedMessageSubscriptionRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {CORRELATED_MESSAGE_SUBSCRIPTION_COLS} FROM correlated_message_subscriptions"
            ))
            .expect("prepare correlated_message_subscriptions");
        let rows = stmt
            .query_map([], map_correlated_message_subscription)
            .expect("query correlated_message_subscriptions");
        rows.filter_map(Result::ok).collect()
    }

    /// All decision-instance rows in this shard.
    pub fn decision_instances(&self) -> Vec<DecisionInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let sql = format!("SELECT {DECISION_INSTANCE_COLS} FROM decision_instances");
        let mut stmt = conn.prepare(&sql).expect("prepare decision_instances");
        let rows = stmt
            .query_map([], map_decision_instance)
            .expect("query decision_instances");
        rows.filter_map(Result::ok).collect()
    }

    /// A single decision-instance by its `<decisionEvaluationKey>-<index>` id.
    pub fn decision_instance(&self, eval_instance_key: &str) -> Option<DecisionInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let sql = format!(
            "SELECT {DECISION_INSTANCE_COLS} FROM decision_instances WHERE eval_instance_key = ?1"
        );
        conn.query_row(&sql, params![eval_instance_key], map_decision_instance)
            .optional()
            .expect("query decision_instance")
    }

    /// Every decision-instance row sharing a `decision_evaluation_key` (one per
    /// evaluated decision in that evaluation), ordered by within-evaluation index.
    /// Used by the DeleteDecisionInstance handler to resolve the owning process
    /// instance (for partition routing) and to detect a not-found evaluation.
    pub fn decision_instances_by_evaluation_key(
        &self,
        decision_evaluation_key: Key,
    ) -> Vec<DecisionInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let sql = format!(
            "SELECT {DECISION_INSTANCE_COLS} FROM decision_instances \
             WHERE decision_evaluation_key = ?1 ORDER BY idx"
        );
        let mut stmt = conn
            .prepare(&sql)
            .expect("prepare decision_instances_by_evaluation_key");
        let rows = stmt
            .query_map(
                params![decision_evaluation_key as i64],
                map_decision_instance,
            )
            .expect("query decision_instances_by_evaluation_key");
        rows.filter_map(Result::ok).collect()
    }

    pub fn process_definitions(&self) -> Vec<ProcessDefinitionRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        // Return EVERY deployed version (Camunda/Zeebe parity: each version is a
        // distinct, searchable process definition, e.g. so `version` filters and
        // by-key lookups resolve superseded versions). `is_latest` marks the
        // highest version per id for callers that want only the current one.
        let mut stmt = conn
            .prepare(
                "SELECT key, process_id, version, name, \
                 (version = MAX(version) OVER (PARTITION BY process_id)) AS is_latest \
                 FROM process_definitions",
            )
            .expect("prepare process_definitions");
        let rows = stmt
            .query_map([], |r| {
                Ok(ProcessDefinitionRow {
                    key: r.get::<_, i64>(0)? as Key,
                    process_id: r.get(1)?,
                    version: r.get(2)?,
                    name: r.get(3)?,
                    is_latest: r.get::<_, i64>(4)? != 0,
                })
            })
            .expect("query process_definitions");
        rows.filter_map(Result::ok).collect()
    }

    /// Fetches a single process definition by its `processDefinitionKey`,
    /// resolving any version (not just the latest), or `None` if no such key was
    /// ever deployed. Backs the get-by-key endpoint.
    pub fn process_definition_by_key(&self, key: Key) -> Option<ProcessDefinitionRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT key, process_id, version, name, \
                 (version = (SELECT MAX(version) FROM process_definitions \
                             WHERE process_id = pd.process_id)) AS is_latest \
                 FROM process_definitions pd WHERE key = ?1",
            )
            .expect("prepare process_definition_by_key");
        stmt.query_row([key as i64], |r| {
            Ok(ProcessDefinitionRow {
                key: r.get::<_, i64>(0)? as Key,
                process_id: r.get(1)?,
                version: r.get(2)?,
                name: r.get(3)?,
                is_latest: r.get::<_, i64>(4)? != 0,
            })
        })
        .optional()
        .expect("query process_definition_by_key")
    }

    pub fn decision_requirements(&self) -> Vec<DecisionRequirementsRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {DECISION_REQUIREMENTS_COLS} FROM decision_requirements"
            ))
            .expect("prepare decision_requirements");
        let rows = stmt
            .query_map([], map_decision_requirements)
            .expect("query decision_requirements");
        rows.filter_map(Result::ok).collect()
    }

    /// A single decision-requirements graph by its numeric key.
    pub fn decision_requirements_by_key(&self, key: Key) -> Option<DecisionRequirementsRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            &format!(
                "SELECT {DECISION_REQUIREMENTS_COLS} FROM decision_requirements WHERE drg_key = ?1"
            ),
            params![key as i64],
            map_decision_requirements,
        )
        .optional()
        .expect("query decision_requirements_by_key")
    }

    /// The verbatim DMN XML for the DRG with `key`, or `None` when no such graph
    /// is projected. Empty-string XML (a graph built programmatically rather than
    /// parsed) is returned as `Some("")`.
    pub fn decision_requirements_xml(&self, key: Key) -> Option<String> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT xml FROM decision_requirements WHERE drg_key = ?1",
            params![key as i64],
            |r| r.get::<_, String>(0),
        )
        .optional()
        .expect("query decision_requirements_xml")
    }

    pub fn decision_definitions(&self) -> Vec<DecisionDefinitionRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {DECISION_DEFINITION_COLS} FROM decision_definitions"
            ))
            .expect("prepare decision_definitions");
        let rows = stmt
            .query_map([], map_decision_definition)
            .expect("query decision_definitions");
        rows.filter_map(Result::ok).collect()
    }

    /// A single decision definition by its numeric key.
    pub fn decision_definition_by_key(&self, key: Key) -> Option<DecisionDefinitionRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            &format!("SELECT {DECISION_DEFINITION_COLS} FROM decision_definitions WHERE decision_key = ?1"),
            params![key as i64],
            map_decision_definition,
        )
        .optional()
        .expect("query decision_definition_by_key")
    }

    /// The DMN XML of the DRG owning the decision definition with `key`, or `None`
    /// when no such decision is projected.
    pub fn decision_definition_xml(&self, key: Key) -> Option<String> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT r.xml FROM decision_definitions d \
             JOIN decision_requirements r ON r.drg_key = d.decision_requirements_key \
             WHERE d.decision_key = ?1",
            params![key as i64],
            |r| r.get::<_, String>(0),
        )
        .optional()
        .expect("query decision_definition_xml")
    }

    /// A single deployed form by its per-version numeric key. Each deployed form
    /// version is retained under its own `form_key`, so a redeploy that mints a
    /// new key never invalidates an earlier one. `None` when no such form is
    /// projected.
    pub fn form_by_key(&self, key: Key) -> Option<FormRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            &format!("SELECT {FORM_COLS} FROM forms WHERE form_key = ?1"),
            params![key as i64],
            map_form,
        )
        .optional()
        .expect("query form_by_key")
    }

    /// The latest deployed form for a given form id (highest version), used to
    /// resolve a process start form (`GetStartProcessForm`) by its declared
    /// `formId`. `None` when no form with that id is projected.
    pub fn form_by_id(&self, form_id: &str) -> Option<FormRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            &format!(
                "SELECT {FORM_COLS} FROM forms WHERE form_id = ?1 \
                 ORDER BY version DESC LIMIT 1"
            ),
            params![form_id],
            map_form,
        )
        .optional()
        .expect("query form_by_id")
    }

    /// A single deployed generic resource by its per-version numeric key. Each
    /// deployed version is retained under its own `resource_key`, so a redeploy
    /// that mints a new key never invalidates an earlier one. `None` when no such
    /// resource is projected.
    pub fn resource_by_key(&self, key: Key) -> Option<ResourceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            &format!("SELECT {RESOURCE_COLS} FROM resources WHERE resource_key = ?1"),
            params![key as i64],
            map_resource,
        )
        .optional()
        .expect("query resource_by_key")
    }

    /// Metadata (no `content`) for a single generic resource by key, for the
    /// by-key metadata endpoint (`getResource`), which returns no content.
    pub fn resource_by_key_meta(&self, key: Key) -> Option<ResourceMetaRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            &format!("SELECT {RESOURCE_META_COLS} FROM resources WHERE resource_key = ?1"),
            params![key as i64],
            map_resource_meta,
        )
        .optional()
        .expect("query resource_by_key_meta")
    }

    /// Metadata (no `content`) for every projected generic-resource row, for the
    /// list/search path. Omits the `content` blob so a search over many/large
    /// resources does not read every version's full body. Callers apply search
    /// filters / sort / pagination in the gateway.
    pub fn resources_meta(&self) -> Vec<ResourceMetaRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {RESOURCE_META_COLS} FROM resources ORDER BY resource_key"
            ))
            .expect("prepare resources_meta");
        stmt.query_map([], map_resource_meta)
            .expect("query resources_meta")
            .filter_map(Result::ok)
            .collect()
    }

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

    /// The `zeebe:formDefinition formId` declared on a process definition's start
    /// event (its start form), by process-definition key. The outer `Option` is
    /// `None` when no such definition is projected; the inner is `None` when the
    /// definition exists but declares no start form.
    pub fn process_definition_start_form_id(&self, key: Key) -> Option<Option<String>> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT start_form_id FROM process_definitions WHERE key = ?1",
            params![key as i64],
            |r| r.get::<_, Option<String>>(0),
        )
        .optional()
        .expect("query process_definition_start_form_id")
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
        parent_process_instance_key: r.get::<_, Option<i64>>(10)?.map(|k| k as Key),
        parent_element_instance_key: r.get::<_, Option<i64>>(11)?.map(|k| k as Key),
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
        kind: job_kind_from(r.get(11)?, r.get(12)?),
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
        form_key: r.get::<_, Option<i64>>(15)?.map(|k| k as Key),
        external_form_reference: r.get(16)?,
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

/// Column list for `element_instances` selects, shared by scan and point lookup.
const ELEMENT_INSTANCE_COLS: &str = "element_instance_key, instance_key, process_definition_id, \
     process_definition_key, element_id, element_name, element_type, state, start_date_ms, \
     end_date_ms, scope_key, incident_key, has_incident, tenant_id";

fn map_element_instance(r: &rusqlite::Row) -> rusqlite::Result<ElementInstanceRow> {
    Ok(ElementInstanceRow {
        element_instance_key: r.get::<_, i64>(0)? as Key,
        instance_key: r.get::<_, i64>(1)? as Key,
        process_definition_id: r.get(2)?,
        process_definition_key: r.get(3)?,
        element_id: r.get(4)?,
        element_name: r.get(5)?,
        element_type: r.get(6)?,
        state: element_instance_state_from(r.get(7)?),
        start_date_ms: r.get::<_, i64>(8)? as u64,
        end_date_ms: r.get::<_, Option<i64>>(9)?.map(|v| v as u64),
        scope_key: r.get::<_, i64>(10)? as Key,
        incident_key: r.get::<_, Option<i64>>(11)?.map(|v| v as Key),
        has_incident: r.get::<_, i64>(12)? != 0,
        tenant_id: r.get(13)?,
    })
}

/// Column list for `message_subscriptions` selects.
const MESSAGE_SUBSCRIPTION_COLS: &str = "subscription_key, instance_key, element_instance_key, \
     element_id, message_name, correlation_key, created_at_ms";

fn map_message_subscription(r: &rusqlite::Row) -> rusqlite::Result<MessageSubscriptionRow> {
    Ok(MessageSubscriptionRow {
        subscription_key: r.get::<_, i64>(0)? as Key,
        instance_key: r.get::<_, i64>(1)? as Key,
        element_instance_key: r.get::<_, i64>(2)? as Key,
        element_id: r.get(3)?,
        message_name: r.get(4)?,
        correlation_key: r.get(5)?,
        created_at_ms: r.get::<_, i64>(6)?.max(0) as u64,
    })
}

/// Column list for `correlated_message_subscriptions` selects.
const CORRELATED_MESSAGE_SUBSCRIPTION_COLS: &str = "message_key, subscription_key, instance_key, element_instance_key, element_id, \
     message_name, correlation_key, correlation_time_ms, partition_id";

fn map_correlated_message_subscription(
    r: &rusqlite::Row,
) -> rusqlite::Result<CorrelatedMessageSubscriptionRow> {
    Ok(CorrelatedMessageSubscriptionRow {
        message_key: r.get::<_, i64>(0)? as Key,
        subscription_key: r.get::<_, i64>(1)? as Key,
        instance_key: r.get::<_, i64>(2)? as Key,
        element_instance_key: r.get::<_, i64>(3)? as Key,
        element_id: r.get(4)?,
        message_name: r.get(5)?,
        correlation_key: r.get(6)?,
        correlation_time_ms: r.get::<_, i64>(7)? as u64,
        partition_id: r.get::<_, i64>(8)? as i32,
    })
}

/// Column list for `decision_instances` selects, shared by scan and point lookup.
const DECISION_INSTANCE_COLS: &str = "eval_instance_key, decision_evaluation_key, idx, decision_id, \
     decision_key, decision_name, decision_type, version, decision_requirements_id, \
     decision_requirements_key, root_decision_key, instance_key, element_instance_key, \
     process_definition_key, state, evaluation_failure, evaluation_date_ms, result_json, \
     inputs_json, rules_json, tenant_id";

fn map_decision_instance(r: &rusqlite::Row) -> rusqlite::Result<DecisionInstanceRow> {
    Ok(DecisionInstanceRow {
        eval_instance_key: r.get(0)?,
        decision_evaluation_key: r.get::<_, i64>(1)? as Key,
        idx: r.get(2)?,
        decision_id: r.get(3)?,
        decision_key: r.get::<_, i64>(4)? as Key,
        decision_name: r.get(5)?,
        decision_type: r.get(6)?,
        version: r.get(7)?,
        decision_requirements_id: r.get(8)?,
        decision_requirements_key: r.get::<_, i64>(9)? as Key,
        root_decision_key: r.get::<_, i64>(10)? as Key,
        instance_key: r.get::<_, i64>(11)? as Key,
        element_instance_key: r.get::<_, i64>(12)? as Key,
        process_definition_key: r.get(13)?,
        state: r.get(14)?,
        evaluation_failure: r.get(15)?,
        evaluation_date_ms: r.get::<_, i64>(16)? as u64,
        result_json: r.get(17)?,
        inputs_json: r.get(18)?,
        rules_json: r.get(19)?,
        tenant_id: r.get(20)?,
    })
}

const DECISION_REQUIREMENTS_COLS: &str = "drg_id, drg_key, name, version, resource_name, xml";

fn map_decision_requirements(r: &rusqlite::Row) -> rusqlite::Result<DecisionRequirementsRow> {
    Ok(DecisionRequirementsRow {
        drg_id: r.get(0)?,
        drg_key: r.get::<_, i64>(1)? as Key,
        name: r.get(2)?,
        version: r.get(3)?,
        resource_name: r.get(4)?,
        xml: r.get(5)?,
    })
}

const DECISION_DEFINITION_COLS: &str = "decision_id, decision_key, name, version, \
     decision_requirements_key, decision_requirements_id, decision_requirements_name, \
     decision_requirements_version";

fn map_decision_definition(r: &rusqlite::Row) -> rusqlite::Result<DecisionDefinitionRow> {
    Ok(DecisionDefinitionRow {
        decision_id: r.get(0)?,
        decision_key: r.get::<_, i64>(1)? as Key,
        name: r.get(2)?,
        version: r.get(3)?,
        decision_requirements_key: r.get::<_, i64>(4)? as Key,
        decision_requirements_id: r.get(5)?,
        decision_requirements_name: r.get(6)?,
        decision_requirements_version: r.get(7)?,
    })
}

const FORM_COLS: &str = "form_id, form_key, version, schema, resource_name, tenant_id";

fn map_form(r: &rusqlite::Row) -> rusqlite::Result<FormRow> {
    Ok(FormRow {
        form_id: r.get(0)?,
        form_key: r.get::<_, i64>(1)? as Key,
        version: r.get(2)?,
        schema: r.get(3)?,
        resource_name: r.get(4)?,
        tenant_id: r.get(5)?,
    })
}

const RESOURCE_COLS: &str =
    "resource_key, resource_id, resource_name, version, version_tag, content, tenant_id";

fn map_resource(r: &rusqlite::Row) -> rusqlite::Result<ResourceRow> {
    Ok(ResourceRow {
        resource_key: r.get::<_, i64>(0)? as Key,
        resource_id: r.get(1)?,
        resource_name: r.get(2)?,
        version: r.get(3)?,
        version_tag: r.get(4)?,
        content: r.get(5)?,
        tenant_id: r.get(6)?,
    })
}

const RESOURCE_META_COLS: &str =
    "resource_key, resource_id, resource_name, version, version_tag, tenant_id";

fn map_resource_meta(r: &rusqlite::Row) -> rusqlite::Result<ResourceMetaRow> {
    Ok(ResourceMetaRow {
        resource_key: r.get::<_, i64>(0)? as Key,
        resource_id: r.get(1)?,
        resource_name: r.get(2)?,
        version: r.get(3)?,
        version_tag: r.get(4)?,
        tenant_id: r.get(5)?,
    })
}
/// Serializes an engine [`Value`] to the serialized-JSON string Camunda uses on
/// the wire: strings are JSON-quoted (so a string `myValue` becomes `"myValue"`),
/// numbers and booleans render bare, and lists/objects render as JSON.
fn json_value(value: &Value) -> String {
    crate::value_to_json(value).to_string()
}

/// The byte length beyond which a variable value is truncated in search results
/// (when `truncateValues` is on), and `isTruncated` is flagged. Single source of
/// truth for the gateway's `server` crate and the in-browser `engine-wasm`
/// `TestEngine`, both of which import this rather than redeclaring it, so the two
/// REST surfaces can never drift on the preview length. Mirrors the order of
/// magnitude of Camunda's variable value preview; nano's typical values are far
/// shorter, so it only fires for pathologically large payloads.
pub const VARIABLE_VALUE_PREVIEW_LEN: usize = 8192;

/// Converts an engine [`Value`] into a `serde_json::Value`. Shared by the read
/// model's projection (variable/DMN JSON encoding here) and the gateway's REST
/// result mapping in the `server` crate, which re-exports this as
/// `crate::value_to_json` so both sides encode identically (single source of
/// truth, no drift).
pub fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Double(d) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Str(s) => serde_json::Value::String(s.clone()),
        Value::List(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
        Value::Map(entries) => serde_json::Value::Object(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect(),
        ),
    }
}

/// The Zeebe/Camunda REST name for a DMN decision logic type. Shared by the read
/// model's decision-instance projection and the gateway's REST mapping (which
/// re-exports it as `crate::dmn_decision_type_name`).
pub fn dmn_decision_type_name(kind: &nanobpmn_engine_core::dmn::DecisionType) -> &'static str {
    use nanobpmn_engine_core::dmn::DecisionType::*;
    match kind {
        DecisionTable => "DECISION_TABLE",
        LiteralExpression => "LITERAL_EXPRESSION",
        Context => "CONTEXT",
        Invocation => "INVOCATION",
        List => "LIST",
        Relation => "RELATION",
        Unknown => "UNKNOWN",
    }
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

/// Inserts (or refreshes) an ACTIVE element-instance row, resolving its owning
/// process definition and its `type`/`elementName` from `definition_elements`.
/// `scope` (from `ElementActivated`) is stamped when known; a re-delivery keeps
/// the row's terminal state (only `scope_key` is refreshed).
fn upsert_element_instance(
    tx: &rusqlite::Transaction,
    now_ms: u64,
    instance_key: Key,
    element_instance_key: Key,
    element_id: &str,
    scope: Option<Key>,
) -> rusqlite::Result<()> {
    let (def_id, def_key) = instance_def(tx, instance_key);
    let def_key_int: i64 = def_key.parse().unwrap_or(-1);
    // An ad-hoc sub-process's synthetic inner instance (`<container>#innerInstance`)
    // has no entry in the deployed model, so it is resolved by its id postfix
    // rather than a `definition_elements` lookup — matching Zeebe's
    // `AD_HOC_SUB_PROCESS_INNER_INSTANCE` element type. The postfix is owned by
    // engine-core (the single source of truth shared with the engine that mints
    // these instances).
    let (element_type, element_name): (String, Option<String>) =
        if element_id.ends_with(nanobpmn_engine_core::ADHOC_INNER_INSTANCE_ID_POSTFIX) {
            ("AD_HOC_SUB_PROCESS_INNER_INSTANCE".to_string(), None)
        } else {
            tx.cquery_row(
                "SELECT element_type, element_name FROM definition_elements \
                 WHERE process_definition_key = ?1 AND element_id = ?2",
                params![def_key_int, element_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .ok()
            .flatten()
            .unwrap_or_else(|| ("UNKNOWN".to_string(), None))
        };
    tx.cexecute(
        "INSERT INTO element_instances (element_instance_key, instance_key, process_definition_id, \
         process_definition_key, element_id, element_name, element_type, state, start_date_ms, \
         end_date_ms, scope_key, incident_key, has_incident) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10, NULL, 0) \
         ON CONFLICT(element_instance_key) DO UPDATE SET \
         scope_key = CASE WHEN ?11 THEN excluded.scope_key ELSE element_instances.scope_key END",
        params![
            element_instance_key as i64,
            instance_key as i64,
            def_id,
            def_key,
            element_id,
            element_name,
            element_type,
            element_instance_state_code(ElementInstanceState::Active),
            now_ms as i64,
            scope.unwrap_or(0) as i64,
            scope.is_some(),
        ],
    )?;
    Ok(())
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

/// Projects the LIVE materialized engine [`State`] (from a boot snapshot) into an
/// empty read model, row-for-row matching what the event projector [`project`]
/// would have produced — the recovery path for a read model that has fallen below
/// the journal compaction floor (issue #732). The engine snapshot is the
/// authoritative capture the compaction invariant guarantees covers everything
/// below the floor, so this rebuilds every operationally-live entity losslessly.
///
/// Deliberately NOT reconstructable here (only ever lived in the read model,
/// already evicted from the engine): terminal-instance audit history and decision
/// evaluation history. Insertion order mirrors the event stream's causal order so
/// the denormalizing `instance_def`/`instance_version`/`definition_elements`
/// lookups resolve: definitions and decisions first, then instances (+ their
/// variables), then per-instance element instances / jobs / incidents / user
/// tasks / message subscriptions.
fn project_engine_state(
    tx: &rusqlite::Transaction,
    state: &nanobpmn_engine_core::State,
    now_ms: u64,
) -> rusqlite::Result<()> {
    // 1) Process definitions + their element metadata. `state.process_versions`
    //    retains EVERY deployed version keyed by process-definition key, while
    //    `state.processes` is only the latest-by-id index over it. Project the
    //    full version-retention map so superseded definitions survive a
    //    compaction-floor recovery — matching the read model's "every version is
    //    searchable / get-by-key resolves superseded versions" semantics. A
    //    pre-retention snapshot deserializes `process_versions` empty
    //    (`serde(default)`); fall back to the latest-by-id index there, where the
    //    latest version per id is the only version that was ever preserved.
    let deployed_defs: Vec<&nanobpmn_engine_core::DeployedProcess> =
        if state.process_versions.is_empty() {
            state.processes.values().collect()
        } else {
            state.process_versions.values().collect()
        };
    for deployed in deployed_defs {
        let def = &deployed.definition;
        tx.cexecute(
            "INSERT INTO process_definitions (process_id, key, version, name, xml, start_form_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(key) DO UPDATE SET process_id = excluded.process_id, version = excluded.version, name = excluded.name, xml = excluded.xml, start_form_id = excluded.start_form_id",
            params![
                def.id,
                deployed.key as i64,
                deployed.version,
                def.name.as_ref(),
                def.xml,
                def.start_form_id.as_ref()
            ],
        )?;
        for (element_id, element) in &def.elements {
            tx.cexecute(
                "INSERT INTO definition_elements (process_definition_key, element_id, element_type, element_name) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(process_definition_key, element_id) DO UPDATE SET \
                 element_type = excluded.element_type, element_name = excluded.element_name",
                params![
                    deployed.key as i64,
                    element_id,
                    element.kind.type_name(),
                    element.name.as_ref(),
                ],
            )?;
        }
    }

    // 2) Decision requirements graphs (before decisions: the decision row's
    //    denormalized DRG identity is resolved from this table).
    for dep in state.decision_requirements.values() {
        tx.cexecute(
            "INSERT INTO decision_requirements (drg_id, drg_key, name, version, resource_name, xml) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(drg_id) DO UPDATE SET drg_key = excluded.drg_key, \
             name = excluded.name, version = excluded.version, \
             resource_name = excluded.resource_name, xml = excluded.xml",
            // `resource_name` is not retained in engine state; default to '' as
            // the column does (it only backs a display field).
            params![dep.drg.id, dep.key as i64, dep.drg.name, dep.version, "", dep.drg.xml],
        )?;
    }

    // 3) Decision definitions.
    for dep in state.decisions.values() {
        let (drg_id, drg_name, drg_version): (String, String, i32) = tx
            .cquery_row(
                "SELECT drg_id, name, version FROM decision_requirements WHERE drg_key = ?1",
                params![dep.decision_requirements_key as i64],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?
            .unwrap_or_default();
        tx.cexecute(
            "INSERT INTO decision_definitions \
             (decision_id, decision_key, name, version, decision_requirements_key, \
              decision_requirements_id, decision_requirements_name, decision_requirements_version) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
             ON CONFLICT(decision_id) DO UPDATE SET decision_key = excluded.decision_key, \
             name = excluded.name, version = excluded.version, \
             decision_requirements_key = excluded.decision_requirements_key, \
             decision_requirements_id = excluded.decision_requirements_id, \
             decision_requirements_name = excluded.decision_requirements_name, \
             decision_requirements_version = excluded.decision_requirements_version",
            params![
                dep.decision_id,
                dep.key as i64,
                dep.decision_name,
                dep.version,
                dep.decision_requirements_key as i64,
                drg_id,
                drg_name,
                drg_version,
            ],
        )?;
    }

    // 4) Forms.
    for dep in state.forms.values() {
        tx.cexecute(
            "INSERT INTO forms (form_key, form_id, version, schema, resource_name, tenant_id) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(form_key) DO UPDATE SET form_id = excluded.form_id, \
             version = excluded.version, schema = excluded.schema, \
             resource_name = excluded.resource_name, tenant_id = excluded.tenant_id",
            // `tenant_id` is not modeled in engine state; default to '<default>'.
            params![
                dep.key as i64,
                dep.form_id,
                dep.version,
                dep.schema,
                dep.resource_name,
                "<default>"
            ],
        )?;
    }

    // 4b) Generic resources.
    for res in state.resources.values() {
        tx.cexecute(
            "INSERT INTO resources (resource_key, resource_id, resource_name, version, \
             version_tag, content, tenant_id) \
             VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6) \
             ON CONFLICT(resource_key) DO UPDATE SET resource_id = excluded.resource_id, \
             resource_name = excluded.resource_name, version = excluded.version, \
             version_tag = excluded.version_tag, content = excluded.content, \
             tenant_id = excluded.tenant_id",
            // `tenant_id` is not modeled in engine state; default to '<default>'.
            params![
                res.key as i64,
                res.resource_id,
                res.resource_name,
                res.version,
                res.content,
                "<default>"
            ],
        )?;
    }

    // 5) Process instances (+ their variables). Resolve the deployed identity the
    //    same way the create projection does (latest version on record).
    for inst in state.instances.values() {
        let (def_key, version): (String, i32) = tx
            .cquery_row(
                "SELECT key, version FROM process_definitions WHERE process_id = ?1 \
                 ORDER BY version DESC LIMIT 1",
                params![inst.process_id],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i32>(1)?)),
            )
            .optional()?
            .map(|(k, v)| (k.to_string(), v))
            .unwrap_or_else(|| ("-1".to_string(), 0));
        tx.cexecute(
            "INSERT INTO process_instances (key, process_id, process_definition_id, \
             process_definition_key, version, state, start_date_ms, has_incident, tags, business_id, \
             parent_process_instance_key, parent_element_instance_key) \
             VALUES (?1, ?2, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8, ?9, ?10) \
             ON CONFLICT(key) DO NOTHING",
            params![
                inst.key as i64,
                inst.process_id,
                def_key,
                version,
                instance_state_code(inst.state),
                inst.created_at as i64,
                inst.tags.join(","),
                inst.business_id.as_ref(),
                inst.parent_process_instance_key.map(|k| k as i64),
                inst.parent_element_instance_key.map(|k| k as i64),
            ],
        )?;
        // Process-level variables (scope == instance key), then each nested scope.
        // A spilled instance carries no variables in the snapshot (they live in
        // the authoritative var store); those are re-materialized on demand, not
        // here.
        upsert_variables(tx, inst.key, inst.key, inst.variables.as_ref())?;
        for (scope, vars) in &inst.scope_variables {
            upsert_variables(tx, inst.key, *scope, vars)?;
        }
    }

    // 6) Live element instances (all ACTIVE — the engine only tracks open tokens).
    for inst in state.instances.values() {
        for (eik, element_id) in &inst.active {
            upsert_element_instance(
                tx,
                now_ms,
                inst.key,
                *eik,
                element_id.as_str(),
                inst.scopes.get(eik).copied(),
            )?;
        }
    }

    // 7) Jobs (carry their current state/worker/deadline/kind directly).
    for job in state.jobs.values() {
        let (def_id, def_key) = instance_def(tx, job.instance_key);
        let (kind_code, event_code) = job_kind_codes(&job.kind);
        tx.cexecute(
            "INSERT INTO jobs (key, instance_key, element_instance_key, element_id, job_type, \
             state, retries, worker, deadline_ms, process_definition_id, process_definition_key, \
             job_kind, listener_event_type) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13) \
             ON CONFLICT(key) DO UPDATE SET state = excluded.state, retries = excluded.retries, \
             worker = excluded.worker, deadline_ms = excluded.deadline_ms",
            params![
                job.key as i64,
                job.instance_key as i64,
                job.element_instance_key as i64,
                job.element_id,
                job.job_type,
                job_state_code(job.state),
                job.retries,
                job.worker.as_ref(),
                job.deadline.map(|d| d as i64),
                def_id,
                def_key,
                kind_code,
                event_code,
            ],
        )?;
    }

    // 8) Incidents (+ surface their flag on the owning instance/element while active).
    for inc in state.incidents.values() {
        let (def_id, def_key) = instance_def(tx, inc.instance_key);
        tx.cexecute(
            "INSERT INTO incidents (key, instance_key, element_instance_key, element_id, kind, \
             state, reason, job_key, created_at_ms, process_definition_id, process_definition_key) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
             ON CONFLICT(key) DO UPDATE SET state = excluded.state",
            params![
                inc.key as i64,
                inc.instance_key as i64,
                inc.element_instance_key as i64,
                inc.element_id,
                incident_kind_code(inc.kind),
                incident_state_code(inc.state),
                inc.reason,
                inc.job_key.map(|k| k as i64),
                inc.created_at as i64,
                def_id,
                def_key,
            ],
        )?;
        if inc.state == IncidentState::Active {
            tx.cexecute(
                "UPDATE process_instances SET has_incident = 1 WHERE key = ?1",
                params![inc.instance_key as i64],
            )?;
            tx.cexecute(
                "UPDATE element_instances SET has_incident = 1, incident_key = ?2 \
                 WHERE element_instance_key = ?1",
                params![inc.element_instance_key as i64, inc.key as i64],
            )?;
        }
    }

    // 9) User tasks.
    for ut in state.user_tasks.values() {
        let (def_id, def_key) = instance_def(tx, ut.instance_key);
        let version = instance_version(tx, ut.instance_key);
        let groups = serde_json::to_string(&ut.candidate_groups).unwrap_or_else(|_| "[]".into());
        let users = serde_json::to_string(&ut.candidate_users).unwrap_or_else(|_| "[]".into());
        tx.cexecute(
            "INSERT INTO user_tasks (key, instance_key, element_instance_key, element_id, \
             state, assignee, candidate_groups, candidate_users, due_date, follow_up_date, \
             priority, created_at_ms, process_definition_id, process_definition_key, \
             process_definition_version, form_key, external_form_reference) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17) \
             ON CONFLICT(key) DO UPDATE SET state = excluded.state",
            params![
                ut.key as i64,
                ut.instance_key as i64,
                ut.element_instance_key as i64,
                ut.element_id,
                user_task_state_code(ut.state),
                ut.assignee.as_ref(),
                groups,
                users,
                ut.due_date.as_ref(),
                ut.follow_up_date.as_ref(),
                ut.priority,
                ut.created_at as i64,
                def_id,
                def_key,
                version,
                ut.form_key.map(|k| k as i64),
                ut.external_form_reference.as_ref(),
            ],
        )?;
    }

    // 10) Open message subscriptions (waiting states). A settled subscription
    //     (Correlated/Canceled) is retained in engine state as an audit trail but
    //     is not a live wait, so the event projector deletes its read-model row;
    //     mirror that by projecting only the open ones.
    for sub in state.message_subscriptions.values() {
        let open = matches!(
            sub.state,
            nanobpmn_engine_core::MessageSubscriptionState::Open
                | nanobpmn_engine_core::MessageSubscriptionState::Opening
        );
        if open && sub.element_instance_key != 0 {
            let non_interrupting = matches!(
                sub.kind,
                nanobpmn_engine_core::MessageSubscriptionKind::NonInterruptingBoundary { .. }
            );
            tx.cexecute(
                "INSERT INTO message_subscriptions (subscription_key, instance_key, \
                 element_instance_key, element_id, message_name, correlation_key, created_at_ms, \
                 non_interrupting) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
                 ON CONFLICT(subscription_key) DO UPDATE SET \
                 element_instance_key = excluded.element_instance_key, \
                 element_id = excluded.element_id, \
                 message_name = excluded.message_name, \
                 correlation_key = excluded.correlation_key, \
                 non_interrupting = excluded.non_interrupting",
                params![
                    sub.key as i64,
                    sub.instance_key as i64,
                    sub.element_instance_key as i64,
                    sub.element_id,
                    sub.message_name,
                    sub.correlation_key,
                    now_ms as i64,
                    non_interrupting as i64,
                ],
            )?;
        }
    }

    Ok(())
}

/// Applies a single event to the read model. Only events that surface in a
/// `search*`/`get*` projection are materialized; the rest (element lifecycle,
/// sequence flows, timers, message subscriptions, start subscriptions) carry no
/// queryable read-model state and are ignored.
/// Applies `event` to the read model and returns its exact contribution to the
/// in-flight instance gauge: `+1` when it genuinely creates a new active
/// instance, `-1` when it genuinely transitions an active instance to terminal,
/// and `0` otherwise — crucially including idempotent re-deliveries, which must
/// not move the gauge (they were the source of the historical `active_backlog`
/// drift where re-delivered creates permanently inflated the counter).
fn project(tx: &rusqlite::Transaction, event: &Event, now_ms: u64) -> rusqlite::Result<i64> {
    let mut delta: i64 = 0;
    match event {
        Event::ProcessDeployed {
            process_definition_key,
            version,
            process,
            ..
        } => {
            // Retain EVERY deployed version, keyed by processDefinitionKey (one
            // row per version), so getProcessDefinitionXML / the console diagram
            // can serve any version's verbatim BPMN — including versions that a
            // later redeploy has superseded but whose instances are still around
            // (a redeploy no longer overwrites the prior version's XML, which
            // previously blanked the Explorer diagram of every older-version
            // instance). Search returns every retained version and marks the latest
            // per id with `is_latest`. `ON CONFLICT(key)` refreshes idempotently on
            // replay/re-delivery of the same ProcessDeployed event.
            tx.cexecute(
                "INSERT INTO process_definitions (process_id, key, version, name, xml, start_form_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                 ON CONFLICT(key) DO UPDATE SET process_id = excluded.process_id, version = excluded.version, name = excluded.name, xml = excluded.xml, start_form_id = excluded.start_form_id",
                params![
                    process.id,
                    *process_definition_key as i64,
                    version,
                    process.name.as_ref(),
                    process.xml,
                    process.start_form_id.as_ref()
                ],
            )?;
            // Element metadata (type + BPMN name) keyed by (definition, element
            // id): the per-element lifecycle events carry only an element id, so
            // the element-instance read model resolves `type`/`elementName` here,
            // from the deployed model (the single source of truth).
            for (element_id, element) in &process.elements {
                tx.cexecute(
                    "INSERT INTO definition_elements (process_definition_key, element_id, element_type, element_name) \
                     VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT(process_definition_key, element_id) DO UPDATE SET \
                     element_type = excluded.element_type, element_name = excluded.element_name",
                    params![
                        *process_definition_key as i64,
                        element_id,
                        element.kind.type_name(),
                        element.name.as_ref(),
                    ],
                )?;
            }
        }

        Event::ProcessInstanceCreated {
            instance_key,
            process_id,
            created_at,
            variables,
            tags,
            business_id,
            process_definition_key,
            version,
            parent_process_instance_key,
            parent_element_instance_key,
        } => {
            // Prefer the definition identity the event carries — it pins the
            // instance to the exact version it was created on (a by-key or
            // by-id+version create may target a non-latest version). Fall back to
            // the latest-deployed-so-far lookup for events written before version
            // pinning (`process_definition_key == 0`): during an ordered replay
            // only versions deployed before this create are on record, so the
            // highest version is the one the instance was created on.
            let (def_key, version): (String, i32) = if *process_definition_key != 0 {
                (process_definition_key.to_string(), *version)
            } else {
                tx.query_row(
                    "SELECT key, version FROM process_definitions WHERE process_id = ?1 \
                     ORDER BY version DESC LIMIT 1",
                    params![process_id],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i32>(1)?)),
                )
                .optional()?
                .map(|(k, v)| (k.to_string(), v))
                .unwrap_or_else(|| ("-1".to_string(), 0))
            };
            // Serialize tags as comma-separated string for storage
            let tags_str = tags.join(",");
            // `DO NOTHING` (not `DO UPDATE`): a create is the first event for a
            // key, so the only conflict is an idempotent re-delivery carrying
            // identical fields — refreshing them would be a no-op. `DO NOTHING`
            // lets the row count distinguish a genuine new instance (1 row) from
            // a re-delivery (0 rows), which is what keeps the in-flight gauge
            // exact under replay.
            let inserted = tx.cexecute(
                "INSERT INTO process_instances (key, process_id, process_definition_id, \
                 process_definition_key, version, state, start_date_ms, has_incident, tags, business_id, \
                 parent_process_instance_key, parent_element_instance_key) \
                 VALUES (?1, ?2, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8, ?9, ?10) \
                 ON CONFLICT(key) DO NOTHING",
                params![
                    *instance_key as i64,
                    process_id,
                    def_key,
                    version,
                    instance_state_code(ProcessInstanceState::Active),
                    *created_at as i64,
                    tags_str,
                    business_id.as_ref(),
                    parent_process_instance_key.map(|k| k as i64),
                    parent_element_instance_key.map(|k| k as i64),
                ],
            )? == 1;
            if inserted {
                delta = 1;
                // Variables the instance was created with (the process-instance
                // row exists now, so the scope's denormalized definition
                // resolves). Skipped on re-delivery: the row already carries
                // them and the instance may since have been pruned/spilled, so
                // re-upserting could resurrect reclaimed variables.
                upsert_variables(tx, *instance_key, *instance_key, variables)?;
            }
        }

        Event::ProcessInstanceCompleted { instance_key } => {
            // `AND state = 0` (Active): only a genuine Active->terminal transition
            // updates a row (1 change => delta -1); a re-delivered completion
            // finds the row already terminal (0 changes) and must not move the
            // gauge.
            let transitioned = tx.cexecute(
                "UPDATE process_instances SET state = ?2 WHERE key = ?1 AND state = 0",
                params![
                    *instance_key as i64,
                    instance_state_code(ProcessInstanceState::Completed)
                ],
            )? == 1;
            if transitioned {
                delta = -1;
            }
            // A completed instance holds no open message subscriptions; drop any
            // so they stop surfacing as MESSAGE wait states.
            tx.cexecute(
                "DELETE FROM message_subscriptions WHERE instance_key = ?1",
                params![*instance_key as i64],
            )?;
        }

        Event::ProcessInstanceTerminated { instance_key } => {
            // `AND state = 0`: count a genuine Active->Terminated transition only
            // (see ProcessInstanceCompleted).
            let transitioned = tx.cexecute(
                "UPDATE process_instances SET state = ?2, has_incident = 0 WHERE key = ?1 AND state = 0",
                params![
                    *instance_key as i64,
                    instance_state_code(ProcessInstanceState::Terminated)
                ],
            )? == 1;
            if transitioned {
                delta = -1;
            }
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
            // Every element instance still ACTIVE when the process is terminated
            // transitions to TERMINATED (the engine emits no per-element terminate
            // event — termination is a process-scope event).
            tx.cexecute(
                "UPDATE element_instances SET state = ?2, end_date_ms = ?3, has_incident = 0, \
                 incident_key = NULL WHERE instance_key = ?1 AND state = ?4",
                params![
                    *instance_key as i64,
                    element_instance_state_code(ElementInstanceState::Terminated),
                    now_ms as i64,
                    element_instance_state_code(ElementInstanceState::Active),
                ],
            )?;
            // A terminated instance holds no open message subscriptions.
            tx.cexecute(
                "DELETE FROM message_subscriptions WHERE instance_key = ?1",
                params![*instance_key as i64],
            )?;
        }

        Event::ElementActivating {
            instance_key,
            element_instance_key,
            element_id,
        } => {
            upsert_element_instance(
                tx,
                now_ms,
                *instance_key,
                *element_instance_key,
                element_id,
                None,
            )?;
        }

        Event::ElementActivated {
            instance_key,
            element_instance_key,
            element_id,
            scope,
        } => {
            // `ElementActivating` may have been pruned/spilled or never observed
            // (older journals); upsert so the row exists, and stamp the scope.
            upsert_element_instance(
                tx,
                now_ms,
                *instance_key,
                *element_instance_key,
                element_id,
                Some(*scope),
            )?;
        }

        Event::ElementCompleted {
            element_instance_key,
            ..
        } => {
            // Only a genuine Active->Completed transition stamps an end date; a
            // re-delivery finds the row already terminal and is a no-op.
            tx.cexecute(
                "UPDATE element_instances SET state = ?2, end_date_ms = ?3 \
                 WHERE element_instance_key = ?1 AND state = ?4",
                params![
                    *element_instance_key as i64,
                    element_instance_state_code(ElementInstanceState::Completed),
                    now_ms as i64,
                    element_instance_state_code(ElementInstanceState::Active),
                ],
            )?;
        }

        // --- Message subscriptions (MESSAGE wait states) ---------------------
        // Only instance-scoped subscriptions (a running element instance parked
        // on a message catch) are tracked; message-*start* subscriptions carry
        // no element instance and are not element-instance wait states.
        Event::MessageSubscriptionCreated {
            subscription_key,
            instance_key,
            element_instance_key,
            element_id,
            message_name,
            correlation_key,
            kind,
        } => {
            if *element_instance_key != 0 {
                let non_interrupting = matches!(
                    kind,
                    nanobpmn_engine_core::MessageSubscriptionKind::NonInterruptingBoundary { .. }
                );
                tx.cexecute(
                    "INSERT INTO message_subscriptions (subscription_key, instance_key, \
                     element_instance_key, element_id, message_name, correlation_key, created_at_ms, \
                     non_interrupting) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
                     ON CONFLICT(subscription_key) DO UPDATE SET \
                     element_instance_key = excluded.element_instance_key, \
                     element_id = excluded.element_id, \
                     message_name = excluded.message_name, \
                     correlation_key = excluded.correlation_key, \
                     non_interrupting = excluded.non_interrupting",
                    params![
                        *subscription_key as i64,
                        *instance_key as i64,
                        *element_instance_key as i64,
                        element_id,
                        message_name,
                        correlation_key,
                        now_ms as i64,
                        non_interrupting as i64,
                    ],
                )?;
            }
        }

        // A message correlated to an open subscription: record it in the
        // correlated (history) table. The open row still holds the message name,
        // correlation key and interrupting flag at this point (none of which are
        // carried on the correlation event), so capture them before deciding
        // whether to drop it. `RemoteMessageCorrelation` is the multi-partition
        // counterpart of `MessageCorrelated` — when the process instance lives on
        // another partition, the message partition settles the canonical
        // subscription with this event instead. In both cases `subscription_key`
        // is the canonical (message-partition) subscription, so its partition is
        // the one that correlated the message.
        Event::MessageCorrelated {
            subscription_key,
            message_key,
            instance_key,
            element_instance_key,
            element_id,
        }
        | Event::RemoteMessageCorrelation {
            subscription_key,
            message_key,
            instance_key,
            element_instance_key,
            element_id,
            ..
        } => {
            // Instance-scoped correlations (a running element instance parked on a
            // catch) are the only ones the open read model tracks; mirror that.
            if *element_instance_key != 0 {
                // Recover the message name / correlation key / interrupting flag
                // from the open row. If the open row is absent (out-of-order replay
                // or partial seeding), skip the history insert entirely rather than
                // record a row with an empty message name / correlation key that
                // would silently corrupt `searchCorrelatedMessageSubscriptions`.
                let captured: Option<(String, String, bool)> = tx
                    .query_row(
                        "SELECT message_name, correlation_key, non_interrupting \
                         FROM message_subscriptions WHERE subscription_key = ?1",
                        params![*subscription_key as i64],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get::<_, i64>(2)? != 0)),
                    )
                    .optional()?;
                if let Some((message_name, correlation_key, non_interrupting)) = captured {
                    tx.cexecute(
                        "INSERT INTO correlated_message_subscriptions (message_key, subscription_key, \
                         instance_key, element_instance_key, element_id, message_name, correlation_key, \
                         correlation_time_ms, partition_id) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
                         ON CONFLICT(message_key, subscription_key) DO NOTHING",
                        params![
                            *message_key as i64,
                            *subscription_key as i64,
                            *instance_key as i64,
                            *element_instance_key as i64,
                            element_id,
                            message_name,
                            correlation_key,
                            now_ms as i64,
                            // 1-based partition id (Camunda/Zeebe convention), to
                            // match the topology mapping in main.rs.
                            (partition_of(*subscription_key) + 1) as i64,
                        ],
                    )?;
                    // A non-interrupting boundary subscription stays open in the
                    // engine so it can correlate again (each correlation spawns a
                    // parallel token); keep its open read-model row so subsequent
                    // correlations are still recorded. Interrupting boundaries and
                    // intermediate catches settle, so their open row is dropped.
                    if non_interrupting {
                        return Ok(delta);
                    }
                }
            }
            tx.cexecute(
                "DELETE FROM message_subscriptions WHERE subscription_key = ?1",
                params![*subscription_key as i64],
            )?;
        }

        // A cancelled subscription is no longer waiting and did not correlate: drop
        // the open row without recording a correlation.
        Event::MessageSubscriptionCanceled {
            subscription_key, ..
        } => {
            tx.cexecute(
                "DELETE FROM message_subscriptions WHERE subscription_key = ?1",
                params![*subscription_key as i64],
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

        Event::ExecutionListenerJobCreated {
            job_key,
            instance_key,
            element_instance_key,
            element_id,
            job_type,
            event_type,
            retries,
            ..
        } => {
            let (def_id, def_key) = instance_def(tx, *instance_key);
            let (kind_code, event_code) = job_kind_codes(&JobKind::ExecutionListener {
                event_type: *event_type,
                index: 0,
                scope: 0,
            });
            tx.cexecute(
                "INSERT INTO jobs (key, instance_key, element_instance_key, element_id, job_type, \
                 state, retries, worker, deadline_ms, process_definition_id, process_definition_key, \
                 job_kind, listener_event_type) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8, ?9, ?10, ?11) \
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
                    kind_code,
                    event_code,
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

        Event::JobTimeoutUpdated {
            job_key, deadline, ..
        } => {
            tx.cexecute(
                "UPDATE jobs SET deadline_ms = ?2 WHERE key = ?1",
                params![*job_key as i64, *deadline as i64],
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
            form_key,
            external_form_reference,
        } => {
            let (def_id, def_key) = instance_def(tx, *instance_key);
            let version = instance_version(tx, *instance_key);
            let groups = serde_json::to_string(candidate_groups).unwrap_or_else(|_| "[]".into());
            let users = serde_json::to_string(candidate_users).unwrap_or_else(|_| "[]".into());
            tx.cexecute(
                "INSERT INTO user_tasks (key, instance_key, element_instance_key, element_id, \
                 state, assignee, candidate_groups, candidate_users, due_date, follow_up_date, \
                 priority, created_at_ms, process_definition_id, process_definition_key, \
                 process_definition_version, form_key, external_form_reference) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17) \
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
                    form_key.map(|k| k as i64),
                    external_form_reference.as_ref(),
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
            // Surface the incident on its element instance too.
            tx.cexecute(
                "UPDATE element_instances SET has_incident = 1, incident_key = ?2 \
                 WHERE element_instance_key = ?1",
                params![*element_instance_key as i64, *incident_key as i64],
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
            // Clear the incident flag on the element instance it was raised on
            // (only when this incident is the one currently referenced there).
            tx.cexecute(
                "UPDATE element_instances SET has_incident = 0, incident_key = NULL \
                 WHERE incident_key = ?1",
                params![*incident_key as i64],
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

        Event::DecisionRequirementsDeployed {
            decision_requirements_key,
            version,
            drg,
            ..
        } => {
            // Latest version per DRG id (a redeploy replaces), mirroring the
            // engine's `state.decision_requirements`. The resource name is not
            // retained by the engine, so it is synthesized from the DRG id (as the
            // process read model does for BPMN); the raw XML is carried on the DRG.
            let resource_name = format!("{}.dmn", drg.id);
            tx.cexecute(
                "INSERT INTO decision_requirements (drg_id, drg_key, name, version, resource_name, xml) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                 ON CONFLICT(drg_id) DO UPDATE SET drg_key = excluded.drg_key, \
                 name = excluded.name, version = excluded.version, \
                 resource_name = excluded.resource_name, xml = excluded.xml",
                params![
                    drg.id,
                    *decision_requirements_key as i64,
                    drg.name,
                    version,
                    resource_name,
                    drg.xml,
                ],
            )?;
        }

        Event::DecisionDeployed {
            decision_requirements_key,
            decision_key,
            decision_id,
            decision_name,
            version,
            ..
        } => {
            // Resolve the owning DRG's id/name/version (its
            // DecisionRequirementsDeployed was projected first, in emission order).
            let (drg_id, drg_name, drg_version): (String, String, i32) = tx
                .cquery_row(
                    "SELECT drg_id, name, version FROM decision_requirements WHERE drg_key = ?1",
                    params![*decision_requirements_key as i64],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()
                .ok()
                .flatten()
                .unwrap_or_default();
            tx.cexecute(
                "INSERT INTO decision_definitions \
                 (decision_id, decision_key, name, version, decision_requirements_key, \
                  decision_requirements_id, decision_requirements_name, decision_requirements_version) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
                 ON CONFLICT(decision_id) DO UPDATE SET decision_key = excluded.decision_key, \
                 name = excluded.name, version = excluded.version, \
                 decision_requirements_key = excluded.decision_requirements_key, \
                 decision_requirements_id = excluded.decision_requirements_id, \
                 decision_requirements_name = excluded.decision_requirements_name, \
                 decision_requirements_version = excluded.decision_requirements_version",
                params![
                    decision_id,
                    *decision_key as i64,
                    decision_name,
                    version,
                    *decision_requirements_key as i64,
                    drg_id,
                    drg_name,
                    drg_version,
                ],
            )?;
        }

        Event::FormDeployed {
            form_key,
            version,
            form_id,
            resource_name,
            schema,
            ..
        } => {
            // One row per deployed form version, keyed by its unique form_key so
            // GetFormByKey resolves every version. The upsert is idempotent on a
            // journal replay (the same event re-projects identical data).
            tx.cexecute(
                "INSERT INTO forms (form_key, form_id, version, schema, resource_name, tenant_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                 ON CONFLICT(form_key) DO UPDATE SET form_id = excluded.form_id, \
                 version = excluded.version, schema = excluded.schema, \
                 resource_name = excluded.resource_name, tenant_id = excluded.tenant_id",
                params![
                    *form_key as i64,
                    form_id,
                    version,
                    schema,
                    resource_name,
                    "<default>",
                ],
            )?;
        }

        Event::GenericResourceDeployed {
            resource_key,
            version,
            resource_id,
            resource_name,
            content,
            ..
        } => {
            // One row per deployed generic-resource version, keyed by its unique
            // resource_key so GetResourceByKey resolves every version. The upsert
            // is idempotent on a journal replay.
            tx.cexecute(
                "INSERT INTO resources (resource_key, resource_id, resource_name, version, \
                 version_tag, content, tenant_id) \
                 VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6) \
                 ON CONFLICT(resource_key) DO UPDATE SET resource_id = excluded.resource_id, \
                 resource_name = excluded.resource_name, version = excluded.version, \
                 version_tag = excluded.version_tag, content = excluded.content, \
                 tenant_id = excluded.tenant_id",
                params![
                    *resource_key as i64,
                    resource_id,
                    resource_name,
                    version,
                    content,
                    "<default>",
                ],
            )?;
        }

        Event::DecisionEvaluated {
            instance_key,
            element_instance_key,
            decision_key: root_decision_key,
            evaluated_decisions,
            evaluated_at,
            ..
        } => {
            // The owning process definition key (join within this shard; the
            // businessRuleTask instance is projected here). Empty when absent.
            let process_definition_key: String = tx
                .cquery_row(
                    "SELECT process_definition_key FROM process_instances WHERE key = ?1",
                    params![*instance_key as i64],
                    |r| r.get(0),
                )
                .optional()
                .ok()
                .flatten()
                .unwrap_or_default();
            // One decision-instance row per evaluated decision (required
            // decisions first, root decision last), indexed 1-based within the
            // evaluation, mirroring Zeebe's decision-instance records.
            for (i, ed) in evaluated_decisions.iter().enumerate() {
                let idx = (i + 1) as i64;
                let eval_instance_key = format!("{root_decision_key}-{idx}");
                let (decision_key, version, drg_id, drg_key): (i64, i32, String, i64) = tx
                    .cquery_row(
                        "SELECT decision_key, version, decision_requirements_id, \
                         decision_requirements_key FROM decision_definitions WHERE decision_id = ?1",
                        params![ed.decision_id],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                    )
                    .optional()
                    .ok()
                    .flatten()
                    .unwrap_or((*root_decision_key as i64, 1, String::new(), 0));
                let result_json = serde_json::to_string(&crate::value_to_json(&ed.decision_output))
                    .unwrap_or_else(|_| "null".to_string());
                let inputs_json = serde_json::to_string(
                    &ed.evaluated_inputs
                        .iter()
                        .map(|inp| {
                            serde_json::json!({
                                "inputId": inp.input_id,
                                "inputName": inp.input_name,
                                "inputValue": serde_json::to_string(&crate::value_to_json(&inp.input_value))
                                    .unwrap_or_else(|_| "null".to_string()),
                            })
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap_or_else(|_| "[]".to_string());
                let rules_json = serde_json::to_string(
                    &ed.matched_rules
                        .iter()
                        .map(|rule| {
                            serde_json::json!({
                                "ruleId": rule.rule_id,
                                "ruleIndex": rule.rule_index,
                                "evaluatedOutputs": rule
                                    .evaluated_outputs
                                    .iter()
                                    .map(|out| {
                                        serde_json::json!({
                                            "outputId": out.output_id,
                                            "outputName": out.output_name,
                                            "outputValue": serde_json::to_string(&crate::value_to_json(&out.output_value))
                                                .unwrap_or_else(|_| "null".to_string()),
                                        })
                                    })
                                    .collect::<Vec<_>>(),
                            })
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap_or_else(|_| "[]".to_string());
                let decision_type = crate::dmn_decision_type_name(&ed.decision_type);
                tx.cexecute(
                    "INSERT INTO decision_instances \
                     (eval_instance_key, decision_evaluation_key, idx, decision_id, decision_key, \
                      decision_name, decision_type, version, decision_requirements_id, \
                      decision_requirements_key, root_decision_key, instance_key, \
                      element_instance_key, process_definition_key, state, evaluation_failure, \
                      evaluation_date_ms, result_json, inputs_json, rules_json, tenant_id) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, \
                      ?16, ?17, ?18, ?19, ?20, ?21) \
                     ON CONFLICT(eval_instance_key) DO NOTHING",
                    params![
                        eval_instance_key,
                        *root_decision_key as i64,
                        idx,
                        ed.decision_id,
                        decision_key,
                        ed.decision_name,
                        decision_type,
                        version,
                        drg_id,
                        drg_key,
                        *root_decision_key as i64,
                        *instance_key as i64,
                        *element_instance_key as i64,
                        process_definition_key,
                        "EVALUATED",
                        Option::<String>::None,
                        *evaluated_at as i64,
                        result_json,
                        inputs_json,
                        rules_json,
                        "<default>",
                    ],
                )?;
            }
        }

        Event::DecisionInstanceDeleted {
            decision_evaluation_key,
            ..
        } => {
            // Retract every decision-instance row of this evaluation (one per
            // evaluated decision). Idempotent: a replay or a broadcast to a shard
            // that never held the rows deletes nothing. Because this is journaled
            // on the owning instance's partition (same shard as the originating
            // DecisionEvaluated), the deletion survives replay/rebuild.
            tx.cexecute(
                "DELETE FROM decision_instances WHERE decision_evaluation_key = ?1",
                params![*decision_evaluation_key as i64],
            )?;
        }

        Event::ProcessInstanceMigrated {
            instance_key,
            target_process_id,
            target_process_definition_key,
            element_mappings,
        } => {
            // Re-home the read model onto the target definition, mirroring the
            // engine applier (`state::apply`): the instance's definition
            // identity moves, and every LIVE runtime row's `element_id` is
            // remapped by its ORIGINAL id (never chained). Completed/terminal
            // history rows keep the definition + element id they ran under.
            let ik = *instance_key as i64;
            let target_key_str = target_process_definition_key.to_string();
            let target_key_i = *target_process_definition_key as i64;
            let target_version: i32 = tx
                .query_row(
                    "SELECT version FROM process_definitions WHERE key = ?1",
                    params![target_key_i],
                    |r| r.get::<_, i32>(0),
                )
                .optional()?
                .ok_or_else(|| {
                    // The engine validated the target definition is deployed
                    // before emitting this event, so a missing row is read-model
                    // corruption — fail loudly rather than writing version=0.
                    rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some(format!(
                            "migration projection: target process definition key {target_key_i} \
                             missing from process_definitions"
                        )),
                    )
                })?;

            // 1) Re-point the instance row itself.
            tx.cexecute(
                "UPDATE process_instances SET process_id = ?2, process_definition_id = ?2, \
                 process_definition_key = ?3, version = ?4 WHERE key = ?1",
                params![ik, target_process_id, target_key_str, target_version],
            )?;

            // 2) Remap live runtime element ids in a single logical pass. A
            //    control-char (`SOH`) temp namespace — illegal in a BPMN NCName
            //    id — makes the two SQL passes equivalent to the engine's
            //    remap-by-original-id map even when a target id equals another
            //    mapping's source (loops / swaps). Job types are preserved (the
            //    worker keeps its lease), matching the applier.
            let active_el = element_instance_state_code(ElementInstanceState::Active);
            let tmp = |target: &str| format!("\u{1}mig:{target}");

            // Phase A — stamp matched live rows with a collision-free temp id.
            for (source_id, target_id) in element_mappings {
                let t = tmp(target_id);
                tx.cexecute(
                    "UPDATE element_instances SET element_id = ?3 \
                     WHERE instance_key = ?1 AND element_id = ?2 AND state = ?4",
                    params![ik, source_id, t, active_el],
                )?;
                // No state filter on jobs / user_tasks / incidents: the engine
                // applier re-points `element_id` on *every* such row the instance
                // owns — it loops all of `state.jobs`, `state.user_tasks` and
                // `state.incidents` filtering only by `instance_key`, and never
                // removes terminal rows — so a job that is live-but-parked
                // (`Failed` with retries=0) or terminal (`Errored`/`Completed`/
                // `Canceled`), a user task that is `Completed`/`Canceled`, and a
                // `Resolved` incident are all remapped there too. Filtering to a
                // "live" state here (`Created` user tasks / `Active` incidents)
                // would be a narrower, divergent notion of "live" than the single
                // source of truth (the engine) and would silently leave terminal
                // rows pointing at stale source element ids (and, for those tables
                // re-homed in Phase B, stale definition identity) — and adding a
                // new state would silently widen that drift. Mirror the applier
                // exactly and remap by original element id alone. (Only the
                // `element_instances` remap keeps a state filter, because the
                // applier likewise remaps only *active* element instances — the
                // ids in `instance.active`.)
                tx.cexecute(
                    "UPDATE jobs SET element_id = ?3 \
                     WHERE instance_key = ?1 AND element_id = ?2",
                    params![ik, source_id, t],
                )?;
                tx.cexecute(
                    "UPDATE user_tasks SET element_id = ?3 \
                     WHERE instance_key = ?1 AND element_id = ?2",
                    params![ik, source_id, t],
                )?;
                tx.cexecute(
                    "UPDATE incidents SET element_id = ?3 \
                     WHERE instance_key = ?1 AND element_id = ?2",
                    params![ik, source_id, t],
                )?;
                // Message subscriptions in the read model are all live (rows are
                // dropped on correlation/cancel/termination), and carry no
                // definition-identity columns, so remap by original element id
                // with no state filter. A migrated instance waiting on a message
                // catch event must re-home its subscription alongside the token.
                tx.cexecute(
                    "UPDATE message_subscriptions SET element_id = ?3 \
                     WHERE instance_key = ?1 AND element_id = ?2",
                    params![ik, source_id, t],
                )?;
            }

            // Phase B — resolve temp ids to the target id + re-home the
            //    definition identity (once per distinct target).
            let mut resolved: std::collections::HashSet<&str> = std::collections::HashSet::new();
            for (_source_id, target_id) in element_mappings {
                if !resolved.insert(target_id.as_str()) {
                    continue;
                }
                let t = tmp(target_id);
                let (t_name, t_type): (Option<String>, String) = tx
                    .query_row(
                        "SELECT element_name, element_type FROM definition_elements \
                         WHERE process_definition_key = ?1 AND element_id = ?2",
                        params![target_key_i, target_id],
                        |r| Ok((r.get::<_, Option<String>>(0)?, r.get::<_, String>(1)?)),
                    )
                    .optional()?
                    .ok_or_else(|| {
                        // The engine validated every mapped target element exists
                        // before emitting this event, so missing metadata is read
                        // model corruption — fail loudly rather than overwriting
                        // `element_type` with an empty string.
                        rusqlite::Error::SqliteFailure(
                            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                            Some(format!(
                                "migration projection: target element '{target_id}' missing from \
                                 definition_elements for process definition key {target_key_i}"
                            )),
                        )
                    })?;
                tx.cexecute(
                    "UPDATE element_instances SET element_id = ?2, element_name = ?3, \
                     element_type = ?4, process_definition_id = ?5, process_definition_key = ?6 \
                     WHERE instance_key = ?1 AND element_id = ?7",
                    params![
                        ik,
                        target_id,
                        t_name,
                        t_type,
                        target_process_id,
                        target_key_str,
                        t
                    ],
                )?;
                tx.cexecute(
                    "UPDATE jobs SET element_id = ?2, process_definition_id = ?3, \
                     process_definition_key = ?4 WHERE instance_key = ?1 AND element_id = ?5",
                    params![ik, target_id, target_process_id, target_key_str, t],
                )?;
                tx.cexecute(
                    "UPDATE user_tasks SET element_id = ?2, process_definition_id = ?3, \
                     process_definition_key = ?4, process_definition_version = ?5 \
                     WHERE instance_key = ?1 AND element_id = ?6",
                    params![
                        ik,
                        target_id,
                        target_process_id,
                        target_key_str,
                        target_version,
                        t
                    ],
                )?;
                tx.cexecute(
                    "UPDATE incidents SET element_id = ?2, process_definition_id = ?3, \
                     process_definition_key = ?4 WHERE instance_key = ?1 AND element_id = ?5",
                    params![ik, target_id, target_process_id, target_key_str, t],
                )?;
                // Resolve the temp-stamped message subscriptions (they carry no
                // definition identity, so only the element id moves).
                tx.cexecute(
                    "UPDATE message_subscriptions SET element_id = ?2 \
                     WHERE instance_key = ?1 AND element_id = ?3",
                    params![ik, target_id, t],
                )?;
            }

            // Variables carry the definition identity but no element id, so
            // re-home them wholesale for the instance.
            tx.cexecute(
                "UPDATE variables SET process_definition_id = ?2, process_definition_key = ?3 \
                 WHERE instance_key = ?1",
                params![ik, target_process_id, target_key_str],
            )?;
        }

        // Events with no queryable read-model projection.
        _ => {}
    }
    Ok(delta)
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

    #[test]
    fn archive_attaches_with_power_safe_full_synchronous() {
        // Defect-class guard (issue #831): the terminal-audit archive is the
        // durable source of truth for completed history and — unlike the derived
        // read model — is NOT rebuildable from the journal once an instance is
        // evicted below the snapshot floor. It must therefore NOT silently inherit
        // the read model's throughput-tuned synchronous=NORMAL (which can drop a
        // recently-archived completion on power/OS loss). `attach_archive` must
        // give the attached archive its own power-safe FULL profile regardless of
        // the main connection's NORMAL.
        let path = scratch_db();
        let store = ReadStore::open(Some(&path)).expect("open file-backed db (creates archive)");
        let archive = ReadStore::terminal_archive_path(&path);
        let archive_sync: i64 = {
            let conn = store.conn.lock().unwrap();
            let main_sync = conn
                .query_row("PRAGMA synchronous", [], |r| r.get::<_, i64>(0))
                .unwrap();
            assert_eq!(
                main_sync, 1,
                "precondition: main read model runs NORMAL (1)"
            );
            super::attach_archive(&conn, &archive).expect("attach archive");
            let s = conn
                .query_row("PRAGMA terminal_archive.synchronous", [], |r| {
                    r.get::<_, i64>(0)
                })
                .unwrap();
            let _ = conn.execute_batch("DETACH DATABASE terminal_archive");
            s
        };
        assert_eq!(
            archive_sync, 2,
            "archive must attach with power-safe synchronous=FULL (2), not the read model's NORMAL (1)"
        );
        drop(store);
        let cleanup = |p: &std::path::Path| {
            std::fs::remove_file(p).ok();
            std::fs::remove_file(p.with_extension("sqlite-wal")).ok();
            std::fs::remove_file(p.with_extension("sqlite-shm")).ok();
        };
        cleanup(&path);
        cleanup(&archive);
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

    // --- schema drift guards (the "derive, don't duplicate" invariant) ---------

    #[test]
    fn fnv1a_is_deterministic_and_content_sensitive() {
        use super::fnv1a_64;
        // Known FNV-1a/64 vectors (offset basis for empty input; a canonical
        // "hello" vector) pin the algorithm so a refactor can't silently change
        // the fingerprint of every existing database.
        assert_eq!(fnv1a_64(b"") as u64, 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a_64(b"hello") as u64, 0xa430_d846_80aa_bd0b);
        // Any change to the hashed bytes must change the digest (this is the
        // property `ensure_schema` relies on to notice a SCHEMA edit).
        assert_ne!(fnv1a_64(b"jobs(a,b)"), fnv1a_64(b"jobs(a,b,c)"));
    }

    #[test]
    fn schema_edit_requires_version_bump() {
        // CI drift guard (issue #831): the fingerprint is no longer a runtime wipe
        // trigger, but it still pins SCHEMA to a recorded value. If you edit
        // SCHEMA, this fails until you bump SCHEMA_VERSION and update
        // SCHEMA_FINGERPRINT — the "fingerprint changed => a migration was added"
        // invariant, enforced in the test suite instead of by dropping tables.
        assert_eq!(
            super::schema_fingerprint(),
            super::SCHEMA_FINGERPRINT,
            "SCHEMA changed: bump SCHEMA_VERSION (currently {}) and set \
             SCHEMA_FINGERPRINT = {}",
            super::SCHEMA_VERSION,
            super::schema_fingerprint(),
        );
    }

    #[test]
    fn additive_schema_change_preserves_rows() {
        // The core issue #831 guarantee: an additive SCHEMA change across an
        // upgrade preserves every existing read-model row (and does not reset the
        // exported_position below the compaction floor). Seed rows under a vN
        // schema, then reopen after an additive (ADD COLUMN + new TABLE) change and
        // assert nothing was dropped.
        let path = scratch_db();
        {
            let store = ReadStore::open(Some(&path)).expect("fresh open");
            let conn = store.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO process_instances \
                 (key, process_id, process_definition_id, process_definition_key, \
                  version, state, start_date_ms, has_incident, tags, business_id) \
                 VALUES (7, 'p', 'p', '1', 1, 1, 0, 0, '[]', NULL)",
                [],
            )
            .unwrap();
            conn.execute("UPDATE meta SET v = 500 WHERE k = 'exported_position'", [])
                .unwrap();
        }

        // Simulate the vN+1 binary: additively evolve the live database exactly as
        // `reconcile_to_schema` would for an additive SCHEMA edit, and clear the
        // stored `schema_version` so the next open actually takes the migration
        // (reconcile) branch of `ensure_schema_on` rather than the "already
        // current" fast path — otherwise this test never exercises the invariant
        // it claims to (that an additive reconcile preserves `exported_position`
        // while stamping the new version).
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "ALTER TABLE process_instances ADD COLUMN priority INTEGER NOT NULL DEFAULT 50;
                 CREATE TABLE new_feature (id INTEGER PRIMARY KEY, note TEXT);
                 DELETE FROM meta WHERE k = 'schema_version';",
            )
            .unwrap();
        }

        // Reopen with the current binary: the additive columns/tables are kept and
        // the seeded row + cursor survive (no destructive wipe), and the reconcile
        // path re-stamps the current schema version.
        let store = ReadStore::open(Some(&path)).expect("additive reopen self-heals");
        let (count, stamped_version): (i64, i64) = {
            let conn = store.conn.lock().unwrap();
            let count = conn
                .query_row("SELECT COUNT(*) FROM process_instances", [], |r| r.get(0))
                .unwrap();
            let version = conn
                .query_row("SELECT v FROM meta WHERE k = 'schema_version'", [], |r| {
                    r.get(0)
                })
                .unwrap();
            (count, version)
        };
        assert_eq!(count, 1, "additive migration must preserve existing rows");
        assert_eq!(
            stamped_version,
            super::SCHEMA_VERSION,
            "additive reconcile must stamp the current schema version"
        );
        assert_eq!(
            store.exported_position(),
            500,
            "additive migration must NOT reset the exported_position (compaction floor)"
        );
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn ensure_schema_migrates_additively_without_dropping() {
        // Guard: a database missing a purely-additive column (a genuine prior-nano
        // read model) is brought up to SCHEMA by ADD COLUMN — never by dropping the
        // table (which is what silently destroyed completed history, issue #831).
        // We reproduce a jobs table lacking only the additive `listener_event_type`
        // column (which carries a DEFAULT) and assert its row survives.
        let path = scratch_db();
        {
            let store = ReadStore::open(Some(&path)).expect("fresh open");
            let conn = store.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO jobs \
                 (key, instance_key, element_instance_key, element_id, job_type, \
                  state, retries, process_definition_id, process_definition_key) \
                 VALUES (11, 1, 1, 'e', 't', 0, 3, 'p', '1')",
                [],
            )
            .unwrap();
        }
        // Regress the schema to before an additive column existed, and clear the
        // version stamp so the next open runs the migration path.
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "ALTER TABLE jobs DROP COLUMN listener_event_type;
                 DELETE FROM meta WHERE k = 'schema_version';",
            )
            .unwrap();
        }
        let store = ReadStore::open(Some(&path)).expect("additive migration on open");
        let cols: Vec<String> = {
            let conn = store.conn.lock().unwrap();
            let mut stmt = conn.prepare("PRAGMA table_info(jobs)").unwrap();
            stmt.query_map([], |r| r.get::<_, String>(1))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert!(
            cols.iter().any(|c| c == "listener_event_type"),
            "migrated jobs table must regain listener_event_type, got {cols:?}"
        );
        let count: i64 = {
            let conn = store.conn.lock().unwrap();
            conn.query_row("SELECT COUNT(*) FROM jobs", [], |r| r.get(0))
                .unwrap()
        };
        assert_eq!(count, 1, "additive migration must preserve the jobs row");
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn schema_fingerprint_is_stable_within_a_build() {
        // Derived purely from SCHEMA, so it is constant across calls and never
        // hand-maintained.
        assert_eq!(super::schema_fingerprint(), super::schema_fingerprint());
    }

    #[test]
    fn incompatible_schema_is_rebuilt() {
        // Reproduces the production incident: a database left behind by an older
        // build whose `jobs` table lacks the `job_kind`/`listener_event_type`
        // columns *and* several NOT NULL columns that cannot be back-filled
        // additively. Such a genuinely-incompatible schema falls back to a
        // destructive rebuild (which #732 reprojection then recovers live data
        // for), NOT a half-shaped table that panics on the first query.
        let path = scratch_db();
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE meta (k TEXT PRIMARY KEY, v INTEGER NOT NULL);
                 INSERT INTO meta (k, v) VALUES ('schema_fingerprint', 1);
                 INSERT INTO meta (k, v) VALUES ('exported_position', 42);
                 CREATE TABLE jobs (
                     key INTEGER PRIMARY KEY,
                     job_type TEXT NOT NULL
                 );
                 INSERT INTO jobs (key, job_type) VALUES (1, 'stale');",
            )
            .unwrap();
        }

        let store = ReadStore::open(Some(&path)).expect("incompatible schema self-heals on open");

        // The rebuilt `jobs` table carries the current columns...
        let cols: Vec<String> = {
            let conn = store.conn.lock().unwrap();
            let mut stmt = conn.prepare("PRAGMA table_info(jobs)").unwrap();
            stmt.query_map([], |r| r.get::<_, String>(1))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert!(
            cols.iter().any(|c| c == "job_kind"),
            "rebuilt jobs table must have job_kind, got {cols:?}"
        );
        assert!(cols.iter().any(|c| c == "listener_event_type"));

        // ...the reads that used to panic on the missing column now succeed...
        assert_eq!(store.active_instance_count(), 0);
        // ...the stale projection state was reset for a clean journal re-replay...
        assert_eq!(store.exported_position(), 0);
        // ...and the stored version now equals the current one, so a second open is
        // a no-op (no rebuild).
        drop(store);
        let store2 = ReadStore::open(Some(&path)).unwrap();
        let stored: Option<i64> = {
            let conn = store2.conn.lock().unwrap();
            conn.query_row("SELECT v FROM meta WHERE k = 'schema_version'", [], |r| {
                r.get(0)
            })
            .ok()
        };
        assert_eq!(stored, Some(super::SCHEMA_VERSION));
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn matching_version_does_not_rebuild() {
        // When the stored version already matches, open must NOT drop the
        // projection: durable derived state (e.g. exported_position) has to
        // survive a restart, or every boot would needlessly re-replay the journal.
        let path = scratch_db();
        let store = ReadStore::open(Some(&path)).unwrap();
        {
            let conn = store.conn.lock().unwrap();
            conn.execute("UPDATE meta SET v = 99 WHERE k = 'exported_position'", [])
                .unwrap();
        }
        drop(store);

        let store2 = ReadStore::open(Some(&path)).unwrap();
        assert_eq!(
            store2.exported_position(),
            99,
            "a schema that already matches must not be rebuilt"
        );
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }
}

#[cfg(test)]
mod definition_xml_tests {
    use nanobpmn_engine_core::{Event, ProcessBuilder, ProcessDefinition};

    use super::ReadStore;

    fn deployed_event(key: u64, xml: &str) -> Event {
        deployed_event_versioned("p", key, 1, xml)
    }

    fn deployed_event_versioned(process_id: &str, key: u64, version: i32, xml: &str) -> Event {
        let mut def: ProcessDefinition = ProcessBuilder::new(process_id)
            .start_event("s")
            .end_event("e")
            .connect("s", "e")
            .build()
            .unwrap();
        def.xml = xml.to_string();
        Event::ProcessDeployed {
            deployment_key: 1,
            process_definition_key: key,
            version,
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

    #[test]
    fn redeploy_retains_every_version_xml_by_key() {
        let store = ReadStore::open(None).unwrap();
        // Deploy v1 (key 6) then a new version v2 (key 297) of the same process id.
        let v1 = deployed_event_versioned("p", 6, 1, "<xml>v1</xml>");
        let v2 = deployed_event_versioned("p", 297, 2, "<xml>v2</xml>");
        store.export(&[&v1]).unwrap();
        store.export(&[&v2]).unwrap();

        // Both versions' XML remain servable by key: an older-version instance's
        // Explorer diagram survives a redeploy (the bug this fixes).
        assert_eq!(
            store.process_definition_xml(6).as_deref(),
            Some("<xml>v1</xml>")
        );
        assert_eq!(
            store.process_definition_xml(297).as_deref(),
            Some("<xml>v2</xml>")
        );

        // Search now surfaces every version (Camunda parity), with `is_latest`
        // marking the highest version per id.
        let mut defs = store.process_definitions();
        defs.sort_by_key(|d| d.version);
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].version, 1);
        assert_eq!(defs[0].key, 6);
        assert!(!defs[0].is_latest);
        assert_eq!(defs[1].version, 2);
        assert_eq!(defs[1].key, 297);
        assert!(defs[1].is_latest);

        // Get-by-key resolves any version, including the superseded one.
        let v1_row = store.process_definition_by_key(6).expect("v1 by key");
        assert_eq!(v1_row.version, 1);
        assert!(!v1_row.is_latest);
        assert!(store.process_definition_by_key(999).is_none());
    }

    #[test]
    fn seed_from_engine_state_projects_every_retained_version() {
        use nanobpmn_engine_core::{DeployedProcess, State};

        // A below-compaction-floor recovery rebuilds the read model from the live
        // engine snapshot. `State::process_versions` retains EVERY deployed
        // version; the projection must surface all of them (not just the
        // latest-by-id index in `state.processes`), or a superseded definition
        // would silently vanish from search / get-by-key after recovery.
        let mk = |key: u64, version: i32, xml: &str| {
            let mut def: ProcessDefinition = ProcessBuilder::new("p")
                .start_event("s")
                .end_event("e")
                .connect("s", "e")
                .build()
                .unwrap();
            def.xml = xml.to_string();
            DeployedProcess {
                key,
                version,
                definition: def,
            }
        };

        let mut state = State::default();
        let v1 = mk(6, 1, "<xml>v1</xml>");
        let v2 = mk(297, 2, "<xml>v2</xml>");
        state.process_versions.insert(6, v1.clone());
        state.process_versions.insert(297, v2.clone());
        // `processes` is the latest-by-id index — v2 only. If the projection read
        // this instead of `process_versions`, v1 would be dropped.
        state.processes.insert("p".to_string(), v2.clone());

        let store = ReadStore::open(None).unwrap();
        store.seed_from_engine_state(&state).unwrap();

        let mut defs = store.process_definitions();
        defs.sort_by_key(|d| d.version);
        assert_eq!(defs.len(), 2, "both retained versions must be projected");
        assert_eq!(
            (defs[0].version, defs[0].key, defs[0].is_latest),
            (1, 6, false)
        );
        assert_eq!(
            (defs[1].version, defs[1].key, defs[1].is_latest),
            (2, 297, true)
        );
        // The superseded version resolves by key after recovery.
        assert_eq!(
            store.process_definition_xml(6).as_deref(),
            Some("<xml>v1</xml>")
        );
        assert_eq!(
            store.process_definition_by_key(6).map(|d| d.version),
            Some(1)
        );
    }

    #[test]
    fn seed_from_engine_state_falls_back_to_latest_index_for_legacy_snapshots() {
        use nanobpmn_engine_core::{DeployedProcess, State};

        // A pre-retention snapshot deserializes `process_versions` empty
        // (`serde(default)`); the projection must fall back to the latest-by-id
        // index so the latest definition still recovers.
        let mut def: ProcessDefinition = ProcessBuilder::new("p")
            .start_event("s")
            .end_event("e")
            .connect("s", "e")
            .build()
            .unwrap();
        def.xml = "<xml>latest</xml>".to_string();

        let mut state = State::default();
        state.processes.insert(
            "p".to_string(),
            DeployedProcess {
                key: 42,
                version: 3,
                definition: def,
            },
        );
        assert!(state.process_versions.is_empty());

        let store = ReadStore::open(None).unwrap();
        store.seed_from_engine_state(&state).unwrap();

        let defs = store.process_definitions();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].key, 42);
        assert_eq!(defs[0].version, 3);
        assert!(defs[0].is_latest);
        assert_eq!(
            store.process_definition_xml(42).as_deref(),
            Some("<xml>latest</xml>")
        );
    }

    #[test]
    fn export_inflight_delta_is_exact_under_idempotent_redelivery() {
        let store = ReadStore::open(None).unwrap();

        // A genuine create contributes +1.
        let created = created_event(1);
        assert_eq!(store.export(&[&created]).unwrap().inflight_delta, 1);
        // Re-delivering the same create must NOT move the gauge (this is the
        // historical drift: idempotent projection double-counted raw events).
        let out = store.export(&[&created]).unwrap();
        assert_eq!(out.inflight_delta, 0);
        assert!(out.terminal_keys.is_empty());

        // A genuine completion contributes -1 and reports the terminal key once.
        let done = Event::ProcessInstanceCompleted { instance_key: 1 };
        let out = store.export(&[&done]).unwrap();
        assert_eq!(out.inflight_delta, -1);
        assert_eq!(out.terminal_keys, vec![1]);
        // Re-delivering the completion (or a late create re-delivery) is inert.
        let out = store.export(&[&done]).unwrap();
        assert_eq!(out.inflight_delta, 0);
        assert!(out.terminal_keys.is_empty());
        assert_eq!(store.export(&[&created]).unwrap().inflight_delta, 0);

        // Net gauge over the whole life is zero, and the row is terminal.
        assert_eq!(store.active_instance_count(), 0);
    }

    #[test]
    fn reconcile_orphaned_active_retires_rows_absent_from_the_live_set() {
        use nanobpmn_engine_core::ProcessInstanceState;
        let store = ReadStore::open(None).unwrap();
        // Three creates, none completed: all Active.
        for k in [10u64, 11, 12] {
            store.export(&[&created_event(k)]).unwrap();
        }
        assert_eq!(store.active_instance_count(), 3);

        // Engine truth: only 11 is genuinely live (e.g. cold-spilled). 10 and 12
        // are orphans — their CREATE was projected but the terminal event never
        // was — so the engine holds no such instance.
        let mut live = std::collections::HashSet::new();
        live.insert(11u64);

        let reconciled = store.reconcile_orphaned_active(&live);
        assert_eq!(reconciled, 2, "10 and 12 are retired; 11 is live");
        assert_eq!(store.active_instance_count(), 1);
        // The live instance is untouched and still Active; the orphans are now
        // Completed (not deleted — the row is preserved for queries).
        assert_eq!(
            store.process_instance(11).map(|r| r.state),
            Some(ProcessInstanceState::Active)
        );
        assert_eq!(
            store.process_instance(10).map(|r| r.state),
            Some(ProcessInstanceState::Completed)
        );

        // Idempotent: a second sweep with the same live set retires nothing.
        assert_eq!(store.reconcile_orphaned_active(&live), 0);

        // A late genuine completion re-delivery for an already-reconciled orphan
        // is inert (the `WHERE state = 0` guard), so the gauge never double-counts.
        let done = Event::ProcessInstanceCompleted { instance_key: 10 };
        assert_eq!(store.export(&[&done]).unwrap().inflight_delta, 0);
        assert_eq!(store.active_instance_count(), 1);
    }

    #[test]
    fn export_inflight_delta_sums_a_mixed_batch() {
        let store = ReadStore::open(None).unwrap();
        // Two creates + one completion in one batch => net +1.
        let c2 = created_event(2);
        let c3 = created_event(3);
        let done2 = Event::ProcessInstanceCompleted { instance_key: 2 };
        let out = store.export(&[&c2, &c3, &done2]).unwrap();
        assert_eq!(out.inflight_delta, 1);
        assert_eq!(out.terminal_keys, vec![2]);
        assert_eq!(store.active_instance_count(), 1);
    }

    #[test]
    fn preexisting_terminal_history_is_backfilled_into_the_archive() {
        // History that completed BEFORE the archive existed (the merlin.local data)
        // is captured once on the first boot carrying this fix, so it too survives a
        // later reprojection — not only instances that complete afterwards.
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "nanobpm-backfill-{}-{}.sqlite",
            std::process::id(),
            n
        ));
        let cleanup = |p: &std::path::Path| {
            let _ = std::fs::remove_file(p);
            let _ = std::fs::remove_file(p.with_extension("sqlite-wal"));
            let _ = std::fs::remove_file(p.with_extension("sqlite-shm"));
        };
        cleanup(&path);

        // A read model that already holds a terminal instance which was NEVER
        // captured (inserted directly, as if it completed under an older binary),
        // and whose backfill has not run.
        {
            let store = ReadStore::open(Some(&path)).unwrap();
            let conn = store.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO process_instances \
                 (key, process_id, process_definition_id, process_definition_key, \
                  version, state, start_date_ms, has_incident, tags, business_id) \
                 VALUES (77, 'legacy', 'legacy', '1', 1, 1, 0, 0, '[]', NULL)",
                [],
            )
            .unwrap();
            conn.execute(
                "DELETE FROM meta WHERE k = 'terminal_archive_backfilled'",
                [],
            )
            .unwrap();
        }

        // Reopen: the one-time backfill copies the pre-existing terminal instance
        // into the durable archive.
        let store = ReadStore::open(Some(&path)).unwrap();
        // Wipe (reprojection stand-in) and replay: the legacy history is restored.
        store.reset().unwrap();
        assert!(store.process_instance(77).is_none());
        let restored = store.replay_terminal_archive().unwrap();
        assert_eq!(
            restored, 1,
            "the pre-existing terminal instance was backfilled"
        );
        assert_eq!(
            store.process_instance(77).map(|r| r.state),
            Some(nanobpmn_engine_core::ProcessInstanceState::Completed),
            "legacy completed history survives reprojection after backfill (issue #831)"
        );

        drop(store);
        cleanup(&path);
        let archive = path.with_file_name(format!(
            "{}.terminal-archive.sqlite",
            path.file_stem().unwrap().to_string_lossy()
        ));
        cleanup(&archive);
    }

    #[test]
    fn terminal_history_survives_reprojection_via_durable_archive() {
        // The issue #831 durability guarantee: a terminal instance is copied to the
        // durable terminal-audit archive as it completes, so that after a below-floor
        // snapshot reprojection wipes the read model (which the engine snapshot can
        // only refill with *live* instances), the completed history is restored from
        // the archive. Reproduces the merlin.local loss and asserts it no longer
        // occurs.
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "nanobpm-archive-{}-{}.sqlite",
            std::process::id(),
            n
        ));
        let cleanup = |p: &std::path::Path| {
            let _ = std::fs::remove_file(p);
            let _ = std::fs::remove_file(p.with_extension("sqlite-wal"));
            let _ = std::fs::remove_file(p.with_extension("sqlite-shm"));
        };
        cleanup(&path);

        let store = ReadStore::open(Some(&path)).unwrap();
        // Create and complete an instance: the completion is a genuine terminal
        // transition, so `export` archives it durably.
        let created = created_event(42);
        let done = Event::ProcessInstanceCompleted { instance_key: 42 };
        let out = store.export(&[&created, &done]).unwrap();
        assert_eq!(out.terminal_keys, vec![42]);
        assert_eq!(
            store.process_instance(42).map(|r| r.state),
            Some(nanobpmn_engine_core::ProcessInstanceState::Completed),
            "instance completed and present before the wipe"
        );

        // Simulate the below-floor reprojection: the read model is reset (wiped),
        // and the engine snapshot holds only live instances — so the terminal
        // instance is NOT reprojected and would be lost without the archive.
        store.reset().unwrap();
        assert!(
            store.process_instance(42).is_none(),
            "reset wipes the read model (stands in for the reprojection)"
        );

        // Replaying the durable archive restores the completed history.
        let restored = store.replay_terminal_archive().unwrap();
        assert_eq!(
            restored, 1,
            "one terminal instance restored from the archive"
        );
        assert_eq!(
            store.process_instance(42).map(|r| r.state),
            Some(nanobpmn_engine_core::ProcessInstanceState::Completed),
            "terminal/completed history is queryable again after reprojection (issue #831)"
        );

        drop(store);
        cleanup(&path);
        let archive = path.with_file_name(format!(
            "{}.terminal-archive.sqlite",
            path.file_stem().unwrap().to_string_lossy()
        ));
        cleanup(&archive);
    }

    fn created_event(instance_key: super::Key) -> Event {
        Event::ProcessInstanceCreated {
            instance_key,
            process_id: "p".to_string(),
            variables: std::collections::HashMap::new(),
            created_at: 0,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: 0,
            version: 0,
            parent_process_instance_key: None,
            parent_element_instance_key: None,
        }
    }

    /// A create event that pins the instance to an explicit definition
    /// (`key`/`version`) — the on-the-wire shape a by-key or by-id+version
    /// create produces.
    fn created_event_pinned(
        instance_key: super::Key,
        process_id: &str,
        process_definition_key: super::Key,
        version: i32,
    ) -> Event {
        Event::ProcessInstanceCreated {
            instance_key,
            process_id: process_id.to_string(),
            variables: std::collections::HashMap::new(),
            created_at: 0,
            tags: Vec::new(),
            business_id: None,
            process_definition_key,
            version,
            parent_process_instance_key: None,
            parent_element_instance_key: None,
        }
    }

    #[test]
    fn read_model_reports_each_instance_its_pinned_version() {
        // Two versions of the same process id are deployed; one instance is
        // created against the *older* version (by-key) and one against the
        // latest. The read model must report the version each instance was
        // actually created on — not merely the newest deployed.
        let store = ReadStore::open(None).unwrap();
        let v1 = deployed_event_versioned("order", 6, 1, "<xml>v1</xml>");
        let v2 = deployed_event_versioned("order", 297, 2, "<xml>v2</xml>");
        store.export(&[&v1, &v2]).unwrap();

        // Instance A pins v1 (key 6); instance B pins v2 (key 297).
        let a = created_event_pinned(1000, "order", 6, 1);
        let b = created_event_pinned(1001, "order", 297, 2);
        store.export(&[&a, &b]).unwrap();

        let row_a = store.process_instance(1000).expect("instance A present");
        assert_eq!(row_a.version, 1, "A reports the version it was created on");
        assert_eq!(row_a.process_definition_key, "6");

        let row_b = store.process_instance(1001).expect("instance B present");
        assert_eq!(row_b.version, 2, "B reports the latest version");
        assert_eq!(row_b.process_definition_key, "297");
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
            store
                .adaptive_prune_once(&mut conn, u64::MAX, u64::MAX, 4096, 4096)
                .unwrap(),
            0
        );
        assert_eq!(store.instance_count(), 7);

        // Over budget (high=low=1 forces eviction), capped at 2 deletes this wake:
        // the OLDEST two terminal (keys 1, 2) go first.
        assert_eq!(
            store.adaptive_prune_once(&mut conn, 1, 1, 4096, 2).unwrap(),
            2
        );
        assert!(store.process_instance(1).is_none());
        assert!(store.process_instance(2).is_none());
        assert!(store.process_instance(3).is_some());
        assert_eq!(store.instance_count(), 5);

        // Next wake with a generous cap drains the remaining terminal (3,4,5)…
        assert_eq!(
            store
                .adaptive_prune_once(&mut conn, 1, 1, 4096, 4096)
                .unwrap(),
            3
        );
        // …but never the active instances.
        assert!(store.process_instance(100).is_some());
        assert!(store.process_instance(101).is_some());
        assert_eq!(store.active_instance_count(), 2);
        assert_eq!(store.instance_count(), 2);

        // Nothing terminal left: a no-op even while "over budget".
        assert_eq!(
            store
                .adaptive_prune_once(&mut conn, 1, 1, 4096, 4096)
                .unwrap(),
            0
        );

        drop(conn);
        drop(store);
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn wal_checkpoint_is_size_gated() {
        // The size-gate is what turns the former ~5/s TRUNCATE storm (a dominant
        // read-model write amplifier) into an occasional file-space reclaim: below
        // the threshold `maybe_checkpoint_wal` must be a no-op; at/above it must
        // TRUNCATE the WAL back into the main DB (shrinking the -wal sidecar).
        let path = std::env::temp_dir().join(format!(
            "nanobpm-ckptgate-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = ReadStore::open(Some(&path)).unwrap();
        // Write enough committed rows to grow the WAL sidecar past zero. The raised
        // autocheckpoint (48 MiB) will not have truncated it for this small volume.
        for k in 1..=200u64 {
            store.export(&[&created_event(k)]).unwrap();
        }
        let conn = store.prune_connection().unwrap().expect("file-backed");
        let wal_before = store.wal_len_bytes();
        assert!(wal_before > 0, "WAL should hold uncheckpointed frames");

        // Threshold above the current WAL size → gated off, no checkpoint, WAL unchanged.
        assert!(!store.checkpoint_wal_if_larger_than(&conn, wal_before + 1));
        assert_eq!(
            store.wal_len_bytes(),
            wal_before,
            "no-op must not shrink WAL"
        );

        // Threshold at/below the WAL size → checkpoint runs and TRUNCATE shrinks it.
        assert!(store.checkpoint_wal_if_larger_than(&conn, 1));
        assert!(
            store.wal_len_bytes() < wal_before,
            "TRUNCATE checkpoint must reclaim WAL file space"
        );

        drop(conn);
        drop(store);
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn export_waits_on_the_shared_write_lock_instead_of_erroring() {
        // Regression for #97: the exporter and the adaptive pruner are two
        // independent WAL writers. With only SQLite's `busy_timeout` a long
        // pruner write would surface to the exporter as `database is locked`
        // (dropped/retried batch). The shared in-process `write_lock` must turn
        // that into a clean wait: while the lock is held, `export` blocks and
        // then succeeds — it never errors.
        let path = std::env::temp_dir().join(format!(
            "nanobpm-writelock-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = std::sync::Arc::new(ReadStore::open(Some(&path)).unwrap());

        // Hold the write lock, mimicking a pruner mid delete+checkpoint.
        let guard = store.write_lock.lock().expect("write lock");

        let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handle = {
            let store = store.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                let created = created_event(1);
                // Blocks on `write_lock` until the main thread releases it; must
                // return Ok (no `database is locked`).
                store.export(&[&created]).expect("export must not error");
                done.store(true, std::sync::atomic::Ordering::SeqCst);
            })
        };

        // While the lock is held the export cannot have completed.
        std::thread::sleep(std::time::Duration::from_millis(150));
        assert!(
            !done.load(std::sync::atomic::Ordering::SeqCst),
            "export completed while the write lock was held — it did not serialize"
        );

        // Release; the export now proceeds and commits.
        drop(guard);
        handle.join().expect("export thread panicked");
        assert!(done.load(std::sync::atomic::Ordering::SeqCst));
        assert!(store.process_instance(1).is_some());

        drop(store);
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn reset_waits_on_the_shared_write_lock_instead_of_erroring() {
        // Regression guard: `reset` runs destructive DDL (drop-all + recreate)
        // and is one more independent WAL writer alongside the exporter and the
        // adaptive pruner. Like `export`/`advance_exported` it must serialize on
        // the shared in-process `write_lock`, so a `reset` racing a pruner mid
        // delete+checkpoint blocks cleanly instead of tripping
        // `database is locked`.
        let path = std::env::temp_dir().join(format!(
            "nanobpm-resetlock-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = std::sync::Arc::new(ReadStore::open(Some(&path)).unwrap());

        let guard = store.write_lock.lock().expect("write lock");

        let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handle = {
            let store = store.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                store.reset().expect("reset must not error");
                done.store(true, std::sync::atomic::Ordering::SeqCst);
            })
        };

        std::thread::sleep(std::time::Duration::from_millis(150));
        assert!(
            !done.load(std::sync::atomic::Ordering::SeqCst),
            "reset completed while the write lock was held — it did not serialize"
        );

        drop(guard);
        handle.join().expect("reset thread panicked");
        assert!(done.load(std::sync::atomic::Ordering::SeqCst));

        drop(store);
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn terminal_state_predicate_is_derived_from_the_canonical_state_codes() {
        use nanobpmn_engine_core::ProcessInstanceState;

        use super::{
            TERMINAL_INSTANCE_STATE_CODES, instance_state_code, instance_state_from,
            terminal_state_predicate,
        };

        // Drift guard (issue #831): every terminal-selection query (eviction,
        // adaptive pruning, terminal-archive copy/backfill) must build its
        // `state IN (...)` predicate from `TERMINAL_INSTANCE_STATE_CODES`, which
        // is itself derived from `instance_state_code`. If the enum-to-int codes
        // ever change, this predicate follows automatically instead of a magic
        // `state IN (1, 2)` literal silently drifting out of sync.
        assert_eq!(
            TERMINAL_INSTANCE_STATE_CODES,
            [
                instance_state_code(ProcessInstanceState::Completed),
                instance_state_code(ProcessInstanceState::Terminated),
            ]
        );
        // The two codes must map back to genuinely terminal states, never to a
        // live (`Active`) or transient (`Terminating`) one.
        for code in TERMINAL_INSTANCE_STATE_CODES {
            assert!(matches!(
                instance_state_from(code),
                ProcessInstanceState::Completed | ProcessInstanceState::Terminated
            ));
        }
        let [completed, terminated] = TERMINAL_INSTANCE_STATE_CODES;
        assert_eq!(
            terminal_state_predicate("state"),
            format!("state IN ({completed}, {terminated})")
        );
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

#[cfg(test)]
mod decision_deletion_tests {
    use nanobpmn_engine_core::dmn::{DecisionType, EvaluatedDecision};
    use nanobpmn_engine_core::{Event, Value};

    use super::ReadStore;

    /// A DecisionEvaluated for process instance `instance_key`, whose root decision
    /// (definition) key — the `decisionEvaluationKey` — is `eval_key`, carrying
    /// `n` evaluated decisions (so it projects `n` decision-instance rows).
    fn evaluated_event(instance_key: u64, eval_key: u64, n: usize) -> Event {
        let evaluated_decisions = (0..n)
            .map(|i| EvaluatedDecision {
                decision_id: format!("d{i}"),
                decision_name: format!("Decision {i}"),
                decision_type: DecisionType::DecisionTable,
                decision_output: Value::Int(i as i64),
                evaluated_inputs: Vec::new(),
                matched_rules: Vec::new(),
            })
            .collect();
        Event::DecisionEvaluated {
            instance_key,
            element_instance_key: instance_key + 1,
            element_id: "brt".to_string(),
            decision_key: eval_key,
            decision_id: "d0".to_string(),
            decision_output: Value::Int(0),
            evaluated_decisions,
            evaluated_at: 123,
        }
    }

    #[test]
    fn decision_instance_deleted_retracts_all_rows_of_the_evaluation() {
        let store = ReadStore::open(None).unwrap();
        // Two evaluations: eval_key 100 (2 decisions) and eval_key 200 (1 decision).
        store.export(&[&evaluated_event(5, 100, 2)]).unwrap();
        store.export(&[&evaluated_event(9, 200, 1)]).unwrap();

        assert_eq!(store.decision_instances_by_evaluation_key(100).len(), 2);
        assert!(store.decision_instance("100-1").is_some());
        assert!(store.decision_instance("100-2").is_some());
        assert_eq!(store.decision_instances_by_evaluation_key(200).len(), 1);

        // Delete evaluation 100: both of its rows go, evaluation 200 is untouched.
        store
            .export(&[&Event::DecisionInstanceDeleted {
                instance_key: 5,
                decision_evaluation_key: 100,
            }])
            .unwrap();

        assert!(store.decision_instances_by_evaluation_key(100).is_empty());
        assert!(store.decision_instance("100-1").is_none());
        assert!(store.decision_instance("100-2").is_none());
        assert_eq!(
            store.decision_instances_by_evaluation_key(200).len(),
            1,
            "deleting one evaluation must not touch another"
        );
    }

    #[test]
    fn decision_instance_deleted_is_idempotent() {
        let store = ReadStore::open(None).unwrap();
        store.export(&[&evaluated_event(5, 100, 2)]).unwrap();

        let del = Event::DecisionInstanceDeleted {
            instance_key: 5,
            decision_evaluation_key: 100,
        };
        store.export(&[&del]).unwrap();
        // Re-delivery (replay/broadcast) of the same deletion is inert, not an error.
        store.export(&[&del]).unwrap();
        // A deletion for an evaluation that never existed is also a harmless no-op.
        store
            .export(&[&Event::DecisionInstanceDeleted {
                instance_key: 7,
                decision_evaluation_key: 999,
            }])
            .unwrap();

        assert!(store.decision_instances_by_evaluation_key(100).is_empty());
    }
}

#[cfg(test)]
mod element_instance_tests {
    use std::collections::HashMap;

    use nanobpmn_engine_core::{Event, JobState, Key, ProcessBuilder};

    use super::{ElementInstanceState, ReadStore};

    const DEF_KEY: u64 = 500;
    const INST: u64 = 1000;
    const TASK_EI: u64 = 1001;

    /// Deploys a process `p` with a named service task `t`, so the projector can
    /// resolve the task's `type`/`elementName` from `definition_elements`.
    fn deploy() -> Event {
        let def = ProcessBuilder::new("p")
            .start_event("s")
            .service_task("t", "worker")
            .with_name("t", "My Task")
            .end_event("e")
            .connect("s", "t")
            .connect("t", "e")
            .build()
            .unwrap();
        Event::ProcessDeployed {
            deployment_key: 1,
            process_definition_key: DEF_KEY,
            version: 1,
            process: def,
        }
    }

    fn created() -> Event {
        Event::ProcessInstanceCreated {
            instance_key: INST,
            process_id: "p".to_string(),
            variables: HashMap::new(),
            created_at: 1,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: 0,
            version: 0,
            parent_process_instance_key: None,
            parent_element_instance_key: None,
        }
    }

    #[test]
    fn projects_call_activity_parent_linkage_into_the_process_instance_row() {
        // The C8 `parentProcessInstanceKey` / `parentElementInstanceKey` surface is
        // consumer-facing, so a regression that dropped these columns from the
        // projection would be silent. Assert a `ProcessInstanceCreated` carrying
        // parent linkage round-trips into the row (and that the default top-level
        // create leaves both `None`).
        let store = ReadStore::open(None).unwrap();
        const CHILD: u64 = 2000;
        const CALL_EI: u64 = 1002;
        store
            .export(&[
                &deploy(),
                &created(),
                &Event::ProcessInstanceCreated {
                    instance_key: CHILD,
                    process_id: "p".to_string(),
                    variables: HashMap::new(),
                    created_at: 2,
                    tags: Vec::new(),
                    business_id: None,
                    process_definition_key: DEF_KEY,
                    version: 1,
                    parent_process_instance_key: Some(INST),
                    parent_element_instance_key: Some(CALL_EI),
                },
            ])
            .unwrap();

        let child = store.process_instance(CHILD).expect("child row exists");
        assert_eq!(child.parent_process_instance_key, Some(INST));
        assert_eq!(child.parent_element_instance_key, Some(CALL_EI));

        let parent = store.process_instance(INST).expect("parent row exists");
        assert_eq!(parent.parent_process_instance_key, None);
        assert_eq!(parent.parent_element_instance_key, None);
    }

    #[test]
    fn projects_activate_complete_terminate_and_incident_linkage() {
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();

        // Activation materializes an ACTIVE row with resolved type + name.
        store
            .export(&[
                &Event::ElementActivating {
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                },
                &Event::ElementActivated {
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    scope: 0,
                },
            ])
            .unwrap();

        let row = store.element_instance(TASK_EI).expect("row exists");
        assert_eq!(row.instance_key, INST);
        assert_eq!(row.element_id, "t");
        assert_eq!(row.element_name.as_deref(), Some("My Task"));
        assert_eq!(row.element_type, "SERVICE_TASK");
        assert_eq!(row.state, ElementInstanceState::Active);
        assert_eq!(row.process_definition_key, DEF_KEY.to_string());
        assert!(row.end_date_ms.is_none());
        assert!(!row.has_incident);

        // An incident on the element links back by key and flips has_incident.
        store
            .export(&[&Event::IncidentRaised {
                incident_key: 7,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "t".to_string(),
                kind: nanobpmn_engine_core::IncidentKind::JobNoRetries,
                reason: "boom".to_string(),
                job_key: Some(42),
                created_at: 5,
            }])
            .unwrap();
        let row = store.element_instance(TASK_EI).unwrap();
        assert!(row.has_incident);
        assert_eq!(row.incident_key, Some(7));

        // Resolving the incident clears the flag.
        store
            .export(&[&Event::IncidentResolved {
                incident_key: 7,
                instance_key: INST,
                job_key: Some(42),
                resolved_at: 6,
                operation_reference: None,
            }])
            .unwrap();
        let row = store.element_instance(TASK_EI).unwrap();
        assert!(!row.has_incident);
        assert_eq!(row.incident_key, None);

        // Completion transitions to COMPLETED and stamps an end date.
        store
            .export(&[&Event::ElementCompleted {
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "t".to_string(),
            }])
            .unwrap();
        let row = store.element_instance(TASK_EI).unwrap();
        assert_eq!(row.state, ElementInstanceState::Completed);
        assert!(row.end_date_ms.is_some());
    }

    #[test]
    fn terminating_the_process_terminates_still_active_elements() {
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();
        store
            .export(&[&Event::ElementActivated {
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "t".to_string(),
                scope: 0,
            }])
            .unwrap();

        store
            .export(&[&Event::ProcessInstanceTerminated { instance_key: INST }])
            .unwrap();
        let row = store.element_instance(TASK_EI).unwrap();
        assert_eq!(row.state, ElementInstanceState::Terminated);
        assert!(row.end_date_ms.is_some());
    }

    #[test]
    fn unresolved_element_type_falls_back_to_unknown() {
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();
        // An element id not present in the deployed model (e.g. an inlined
        // call-activity child) resolves to UNKNOWN with no name.
        store
            .export(&[&Event::ElementActivated {
                instance_key: INST,
                element_instance_key: 2002,
                element_id: "mystery".to_string(),
                scope: 0,
            }])
            .unwrap();
        let row = store.element_instance(2002).unwrap();
        assert_eq!(row.element_type, "UNKNOWN");
        assert_eq!(row.element_name, None);
    }

    /// Gap #9 (issue #614): the synthetic ad-hoc inner instance
    /// (`<container>#innerInstance`) is not in the deployed model, so it must be
    /// resolved to the `AD_HOC_SUB_PROCESS_INNER_INSTANCE` element type by its id
    /// postfix rather than falling back to UNKNOWN — matching Zeebe's read model.
    #[test]
    fn adhoc_inner_instance_resolves_to_its_element_type() {
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();
        let inner_id = nanobpmn_engine_core::adhoc_inner_instance_id("agent");
        store
            .export(&[&Event::ElementActivated {
                instance_key: INST,
                element_instance_key: 2003,
                element_id: inner_id,
                scope: 0,
            }])
            .unwrap();
        let row = store.element_instance(2003).unwrap();
        assert_eq!(row.element_type, "AD_HOC_SUB_PROCESS_INNER_INSTANCE");
        assert_eq!(row.element_name, None);
    }

    /// `active_element_instances` returns only the `Active` rows for the given
    /// instance — the live token positions the explorer overlays. Completed or
    /// terminated elements, and elements belonging to other instances, are
    /// excluded.
    #[test]
    fn active_element_instances_returns_only_active_rows_for_the_instance() {
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();

        // Two elements activate on INST; one then completes.
        store
            .export(&[
                &Event::ElementActivated {
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    scope: 0,
                },
                &Event::ElementActivated {
                    instance_key: INST,
                    element_instance_key: TASK_EI + 1,
                    element_id: "t".to_string(),
                    scope: 0,
                },
                &Event::ElementCompleted {
                    instance_key: INST,
                    element_instance_key: TASK_EI + 1,
                    element_id: "t".to_string(),
                },
            ])
            .unwrap();

        // A different instance's active element must not leak in.
        store
            .export(&[
                &Event::ProcessInstanceCreated {
                    instance_key: INST + 100,
                    process_id: "p".to_string(),
                    variables: HashMap::new(),
                    created_at: 1,
                    tags: Vec::new(),
                    business_id: None,
                    process_definition_key: 0,
                    version: 0,
                    parent_process_instance_key: None,
                    parent_element_instance_key: None,
                },
                &Event::ElementActivated {
                    instance_key: INST + 100,
                    element_instance_key: TASK_EI + 2,
                    element_id: "t".to_string(),
                    scope: 0,
                },
            ])
            .unwrap();

        let active = store.active_element_instances(INST);
        assert_eq!(active.len(), 1, "only the still-active element on INST");
        assert_eq!(active[0].element_instance_key, TASK_EI);
        assert!(
            active
                .iter()
                .all(|e| e.state == ElementInstanceState::Active && e.instance_key == INST)
        );
    }

    #[test]
    fn message_subscriptions_are_projected_and_dropped_on_correlate_and_terminate() {
        use nanobpmn_engine_core::MessageSubscriptionKind;
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();

        // An instance-scoped subscription (element_instance_key != 0) materializes
        // a MESSAGE wait-state row.
        store
            .export(&[&Event::MessageSubscriptionCreated {
                subscription_key: 3001,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "await".to_string(),
                message_name: "OrderPlaced".to_string(),
                correlation_key: "A1".to_string(),
                kind: MessageSubscriptionKind::IntermediateCatch,
            }])
            .unwrap();
        let subs = store.message_subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].subscription_key, 3001);
        assert_eq!(subs[0].element_instance_key, TASK_EI);
        assert_eq!(subs[0].message_name, "OrderPlaced");
        assert_eq!(subs[0].correlation_key, "A1");

        // A subscription with no element instance (element_instance_key == 0,
        // e.g. a message-start subscription) is not an element-instance wait
        // state and must not be projected.
        store
            .export(&[&Event::MessageSubscriptionCreated {
                subscription_key: 3002,
                instance_key: INST,
                element_instance_key: 0,
                element_id: "start".to_string(),
                message_name: "Kickoff".to_string(),
                correlation_key: String::new(),
                kind: MessageSubscriptionKind::IntermediateCatch,
            }])
            .unwrap();
        assert_eq!(store.message_subscriptions().len(), 1);

        // Correlation releases the token and drops the subscription.
        store
            .export(&[&Event::MessageCorrelated {
                subscription_key: 3001,
                message_key: 9,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "await".to_string(),
            }])
            .unwrap();
        assert!(store.message_subscriptions().is_empty());
        // Correlation is recorded in the history read model, capturing the message
        // name and correlation key from the (now-dropped) open subscription row.
        let corr = store.correlated_message_subscriptions();
        assert_eq!(corr.len(), 1);
        assert_eq!(corr[0].message_key, 9);
        assert_eq!(corr[0].subscription_key, 3001);
        assert_eq!(corr[0].instance_key, INST);
        assert_eq!(corr[0].element_instance_key, TASK_EI);
        assert_eq!(corr[0].message_name, "OrderPlaced");
        assert_eq!(corr[0].correlation_key, "A1");

        // A subscription still open when the process terminates is cleaned up.
        store
            .export(&[&Event::MessageSubscriptionCreated {
                subscription_key: 3003,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "await".to_string(),
                message_name: "OrderPlaced".to_string(),
                correlation_key: "A2".to_string(),
                kind: MessageSubscriptionKind::IntermediateCatch,
            }])
            .unwrap();
        assert_eq!(store.message_subscriptions().len(), 1);
        store
            .export(&[&Event::ProcessInstanceTerminated { instance_key: INST }])
            .unwrap();
        assert!(store.message_subscriptions().is_empty());

        // A remote correlation (multi-partition: the instance lives elsewhere)
        // settles the canonical subscription with RemoteMessageCorrelation and
        // must also clear the row.
        store
            .export(&[&Event::MessageSubscriptionCreated {
                subscription_key: 3004,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "await".to_string(),
                message_name: "OrderPlaced".to_string(),
                correlation_key: "A3".to_string(),
                kind: MessageSubscriptionKind::IntermediateCatch,
            }])
            .unwrap();
        assert_eq!(store.message_subscriptions().len(), 1);
        store
            .export(&[&Event::RemoteMessageCorrelation {
                subscription_key: 3004,
                message_key: 10,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "await".to_string(),
                kind: MessageSubscriptionKind::IntermediateCatch,
                variables: std::collections::HashMap::new(),
            }])
            .unwrap();
        assert!(store.message_subscriptions().is_empty());
    }

    #[test]
    fn migration_remaps_live_message_subscription_onto_target_element() {
        use nanobpmn_engine_core::MessageSubscriptionKind;
        // An instance waiting on a message catch event carries a live
        // `message_subscriptions` row. Migrating it must re-home that row's
        // `element_id` onto the mapped target element, alongside the token —
        // otherwise the read model points at the source element id the engine no
        // longer runs under.
        let store = ReadStore::open(None).unwrap();

        // Target definition `p2` carries the mapped target element `await2` as a
        // message intermediate catch event (matching the source token's
        // `IntermediateCatch` subscription kind and the realistic
        // `definition_elements` metadata/type), so its metadata is resolvable.
        let target = ProcessBuilder::new("p2")
            .start_event("s2")
            .message_intermediate_catch_event("await2", "OrderPlaced", "=orderId")
            .end_event("e2")
            .connect("s2", "await2")
            .connect("await2", "e2")
            .build()
            .unwrap();
        let target_key: u64 = 600;
        store
            .export(&[
                &deploy(),
                &created(),
                &Event::ProcessDeployed {
                    deployment_key: 2,
                    process_definition_key: target_key,
                    version: 3,
                    process: target,
                },
                &Event::MessageSubscriptionCreated {
                    subscription_key: 4001,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "await".to_string(),
                    message_name: "OrderPlaced".to_string(),
                    correlation_key: "A1".to_string(),
                    kind: MessageSubscriptionKind::IntermediateCatch,
                },
            ])
            .unwrap();
        assert_eq!(store.message_subscriptions()[0].element_id, "await");

        store
            .export(&[&Event::ProcessInstanceMigrated {
                instance_key: INST,
                target_process_id: "p2".to_string(),
                target_process_definition_key: target_key,
                element_mappings: vec![("await".to_string(), "await2".to_string())],
            }])
            .unwrap();

        // The subscription re-homes onto the target element id; the instance row
        // re-homes onto the target definition + its version (looked up loudly).
        let subs = store.message_subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].element_id, "await2");
        let inst = store.process_instance(INST).unwrap();
        assert_eq!(inst.process_definition_id, "p2");
        assert_eq!(inst.process_definition_key, target_key.to_string());
        assert_eq!(inst.version, 3);
    }

    #[test]
    fn migration_remaps_parked_and_terminal_job_element_ids() {
        // The engine applier re-points `element_id` on *every* job the instance
        // owns — it loops all of `state.jobs` and never removes terminal rows —
        // so a parked (`Failed`, retries=0) or terminal (`Errored`) job is
        // remapped there too. The read model must mirror that, or a job row is
        // left pointing at the source `element_id` the engine no longer runs
        // under. This guards the whole defect class (any job state, not just the
        // two the projection used to allow-list) against silent drift.
        let store = ReadStore::open(None).unwrap();

        let target = ProcessBuilder::new("p2")
            .start_event("s2")
            .service_task("t2", "worker")
            .with_name("t2", "My Task 2")
            .end_event("e2")
            .connect("s2", "t2")
            .connect("t2", "e2")
            .build()
            .unwrap();
        let target_key: u64 = 600;

        store
            .export(&[
                &deploy(),
                &created(),
                &Event::ProcessDeployed {
                    deployment_key: 2,
                    process_definition_key: target_key,
                    version: 3,
                    process: target,
                },
                // A parked job (retries exhausted) on the source element `t`.
                &Event::JobCreated {
                    job_key: 7001,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "worker".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 1,
                },
                &Event::JobFailed {
                    job_key: 7001,
                    instance_key: INST,
                    retries: 0,
                },
                // A terminal errored job on the same source element.
                &Event::JobCreated {
                    job_key: 7002,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "worker".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 1,
                },
                &Event::JobErrorThrown {
                    job_key: 7002,
                    instance_key: INST,
                    error_code: "BOOM".to_string(),
                },
            ])
            .unwrap();
        let before: HashMap<Key, JobState> =
            store.jobs().into_iter().map(|j| (j.key, j.state)).collect();
        assert_eq!(before.get(&7001), Some(&JobState::Failed));
        assert_eq!(before.get(&7002), Some(&JobState::Errored));

        store
            .export(&[&Event::ProcessInstanceMigrated {
                instance_key: INST,
                target_process_id: "p2".to_string(),
                target_process_definition_key: target_key,
                element_mappings: vec![("t".to_string(), "t2".to_string())],
            }])
            .unwrap();

        // Both the parked and the terminal job re-home onto the target element
        // id and pick up the target definition identity — matching the engine.
        for job in store.jobs() {
            assert_eq!(
                job.element_id, "t2",
                "job {} left pointing at stale source element id",
                job.key
            );
            assert_eq!(job.process_definition_id, "p2");
            assert_eq!(job.process_definition_key, target_key.to_string());
        }
    }

    #[test]
    fn non_interrupting_boundary_stays_open_and_records_each_correlation() {
        use nanobpmn_engine_core::MessageSubscriptionKind;
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();

        // A non-interrupting message boundary subscription: correlating spawns a
        // parallel token but leaves the subscription open, so it can correlate
        // again for every matching message.
        store
            .export(&[&Event::MessageSubscriptionCreated {
                subscription_key: 4001,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "task".to_string(),
                message_name: "Ping".to_string(),
                correlation_key: "K1".to_string(),
                kind: MessageSubscriptionKind::NonInterruptingBoundary {
                    boundary_element_id: "boundary".to_string(),
                },
            }])
            .unwrap();
        assert_eq!(store.message_subscriptions().len(), 1);

        // First correlation: history recorded AND the open row is kept.
        store
            .export(&[&Event::MessageCorrelated {
                subscription_key: 4001,
                message_key: 11,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "task".to_string(),
            }])
            .unwrap();
        assert_eq!(
            store.message_subscriptions().len(),
            1,
            "non-interrupting subscription stays open after correlating"
        );
        assert_eq!(store.correlated_message_subscriptions().len(), 1);

        // Second correlation (different message): another history row, still open.
        store
            .export(&[&Event::MessageCorrelated {
                subscription_key: 4001,
                message_key: 12,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "task".to_string(),
            }])
            .unwrap();
        assert_eq!(store.message_subscriptions().len(), 1);
        let corr = store.correlated_message_subscriptions();
        assert_eq!(corr.len(), 2, "each correlation is recorded in history");
        // partition_id is stored 1-based (Camunda/Zeebe convention).
        assert!(corr.iter().all(|c| c.partition_id == 1));
        assert!(corr.iter().all(|c| c.message_name == "Ping"));

        // Cancelling (activity completes / instance ends) finally drops the row.
        store
            .export(&[&Event::MessageSubscriptionCanceled {
                subscription_key: 4001,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "task".to_string(),
            }])
            .unwrap();
        assert!(store.message_subscriptions().is_empty());
        // History survives the cancel.
        assert_eq!(store.correlated_message_subscriptions().len(), 2);
    }

    #[test]
    fn correlation_without_an_open_row_records_no_history() {
        // A correlation event whose open subscription row is absent (out-of-order
        // replay / partial seeding) must not fabricate a history row with an empty
        // message name / correlation key.
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();
        store
            .export(&[&Event::MessageCorrelated {
                subscription_key: 5001,
                message_key: 13,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "await".to_string(),
            }])
            .unwrap();
        assert!(
            store.correlated_message_subscriptions().is_empty(),
            "no history row without a captured open subscription"
        );
    }
}

/// Read-surface parity checks for the shared projection, exercising the exact
/// readstore-shaped queries the in-browser (wasm) test engine will serve through
/// this same crate: `GetFormByKey` and `searchUserTasks` (with its open/closed
/// `state` filter). The projection and SQL are backend-agnostic — identical on
/// `native` and `wasm` — so proving them here (natively runnable, on the default
/// backend) proves the query surface the wasm backend answers byte-for-byte.
///
/// This is the acceptance coverage for the wasm read-model backend (epic
/// Magikcraft/nano-bpm#796): it mirrors the epic's spike, so a regression in the
/// shared read surface fails a plain `cargo test` regardless of backend.
#[cfg(test)]
mod read_surface_tests {
    use nanobpmn_engine_core::{Event, UserTaskState};

    use super::{Key, ReadStore};

    fn form_deployed(form_key: Key, form_id: &str, version: i32, schema: &str) -> Event {
        Event::FormDeployed {
            deployment_key: 1,
            form_key,
            version,
            form_id: form_id.to_string(),
            resource_name: format!("{form_id}.form"),
            schema: schema.to_string(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn user_task_created(user_task_key: Key, instance_key: Key, element_id: &str) -> Event {
        Event::UserTaskCreated {
            user_task_key,
            instance_key,
            element_instance_key: instance_key,
            element_id: element_id.to_string(),
            created_at: 0,
            assignee: None,
            candidate_groups: Vec::new(),
            candidate_users: Vec::new(),
            due_date: None,
            follow_up_date: None,
            priority: 0,
            form_key: None,
            external_form_reference: None,
        }
    }

    /// `GetFormByKey` resolves each deployed form version by its unique key, and a
    /// redeploy of the same `form_id` yields the *latest* schema at the latest
    /// version — the spike's "form_by_key returns the latest schema".
    #[test]
    fn form_by_key_serves_the_latest_deployed_schema() {
        let store = ReadStore::open(None).expect("in-memory read store opens");

        // Deploy v1 then a new version v2 of the same form id, each with a
        // distinct schema and its own unique form key.
        let v1 = form_deployed(10, "greeting", 1, r#"{"schemaVersion":1}"#);
        let v2 = form_deployed(11, "greeting", 2, r#"{"schemaVersion":2}"#);
        store.export(&[&v1]).expect("project form v1");
        store.export(&[&v2]).expect("project form v2");

        // The latest key resolves the latest schema/version.
        let latest = store.form_by_key(11).expect("latest form version resolves");
        assert_eq!(latest.form_id, "greeting");
        assert_eq!(latest.version, 2);
        assert_eq!(latest.schema, r#"{"schemaVersion":2}"#);

        // Every prior version remains servable by its own key (Zeebe parity).
        let older = store
            .form_by_key(10)
            .expect("older form version still resolves");
        assert_eq!(older.version, 1);
        assert_eq!(older.schema, r#"{"schemaVersion":1}"#);

        // An unknown key has no form.
        assert!(store.form_by_key(999).is_none());
    }

    /// The projection records a `state` per user task, distinguishing open
    /// (`Created`) from completed tasks off the exact projected data — the spike's
    /// second assertion. This validates the *state projection*: `user_tasks()` is
    /// unfiltered, so the open/completed split is asserted here in Rust, which is
    /// exactly the data `searchUserTasks({state:'CREATED'})` filters on downstream.
    #[test]
    fn user_tasks_projection_records_open_and_completed_state() {
        let store = ReadStore::open(None).expect("in-memory read store opens");

        // Two user tasks are created (both open); one is then completed.
        let open = user_task_created(100, 1, "review");
        let closing = user_task_created(101, 1, "approve");
        store.export(&[&open]).expect("project open task");
        store.export(&[&closing]).expect("project task to complete");
        let completed = Event::UserTaskCompleted {
            user_task_key: 101,
            instance_key: 1,
        };
        store.export(&[&completed]).expect("project completion");

        let tasks = store.user_tasks();
        assert_eq!(tasks.len(), 2, "both tasks remain projected");

        // Filtering to the open state (what searchUserTasks({state:'CREATED'})
        // does) yields exactly the un-completed task.
        let open_only: Vec<Key> = tasks
            .iter()
            .filter(|t| t.state == UserTaskState::Created)
            .map(|t| t.key)
            .collect();
        assert_eq!(open_only, vec![100]);

        // The completed task carries the terminal state, so it is excluded above
        // and included by a complementary filter.
        let completed_only: Vec<Key> = tasks
            .iter()
            .filter(|t| t.state == UserTaskState::Completed)
            .map(|t| t.key)
            .collect();
        assert_eq!(completed_only, vec![101]);
    }
}
