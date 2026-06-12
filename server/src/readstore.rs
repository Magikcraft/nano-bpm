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

use std::path::Path;
use std::sync::Mutex;

use nanobpmn_engine_core::{
    DEFAULT_JOB_RETRIES, Event, IncidentKind, IncidentState, JobState, Key, ProcessInstanceState,
    UserTaskState, Value,
};
use rusqlite::{Connection, OptionalExtension, params};

/// Bumped whenever the schema or projection changes; a stored database with a
/// different version is dropped and rebuilt from the journal.
const SCHEMA_VERSION: i64 = 3;

const SCHEMA: &str = "
CREATE TABLE process_definitions (
    process_id TEXT PRIMARY KEY,
    key        INTEGER NOT NULL,
    version    INTEGER NOT NULL
);
CREATE TABLE process_instances (
    key                    INTEGER PRIMARY KEY,
    process_id             TEXT NOT NULL,
    process_definition_id  TEXT NOT NULL,
    process_definition_key TEXT NOT NULL,
    version                INTEGER NOT NULL,
    state                  INTEGER NOT NULL,
    start_date_ms          INTEGER NOT NULL,
    has_incident           INTEGER NOT NULL
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

/// The read model. Wraps a single SQLite connection behind a mutex: SQLite
/// serializes writes anyway, and this keeps the projection (exporter thread) and
/// the queries (request handlers) on one shared database — including for the
/// `:memory:` backend, where separate connections would not see each other's
/// data. The mutex is independent of the engine lock, so reads never contend
/// with engine writes.
pub struct ReadStore {
    conn: Mutex<Connection>,
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
        let store = Self {
            conn: Mutex::new(conn),
        };
        store.ensure_schema()?;
        Ok(store)
    }

    fn ensure_schema(&self) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("read store poisoned");
        let version: Option<i64> = conn
            .query_row(
                "SELECT v FROM meta WHERE k = 'schema_version'",
                [],
                |r| r.get(0),
            )
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
        self.ensure_schema()
    }

    /// Projects a batch of consecutive journal `events` into the store in one
    /// transaction and advances `exported_position` by `events.len()`. Returns
    /// the keys of instances that completed in this batch, so the caller can
    /// evict them from hot engine state. Projection is idempotent, so replaying
    /// an overlapping prefix is safe.
    pub fn export(&self, events: &[Event]) -> rusqlite::Result<Vec<Key>> {
        let mut conn = self.conn.lock().expect("read store poisoned");
        let tx = conn.transaction()?;
        let mut completed = Vec::new();
        for event in events {
            if let Event::ProcessInstanceCompleted { instance_key }
            | Event::ProcessInstanceTerminated { instance_key } = event
            {
                completed.push(*instance_key);
            }
            project(&tx, event)?;
        }
        tx.execute(
            "UPDATE meta SET v = v + ?1 WHERE k = 'exported_position'",
            params![events.len() as i64],
        )?;
        tx.commit()?;
        Ok(completed)
    }

    // --- queries used by the search/get handlers ---

    pub fn process_instances(&self) -> Vec<ProcessInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT key, process_id, process_definition_id, process_definition_key, \
                 version, state, start_date_ms, has_incident FROM process_instances",
            )
            .expect("prepare process_instances");
        let rows = stmt
            .query_map([], map_instance)
            .expect("query process_instances");
        rows.filter_map(Result::ok).collect()
    }

    pub fn process_instance(&self, key: Key) -> Option<ProcessInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT key, process_id, process_definition_id, process_definition_key, \
             version, state, start_date_ms, has_incident FROM process_instances WHERE key = ?1",
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
                 assignee, created_at_ms, process_definition_id, process_definition_key, \
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
    Ok(ProcessInstanceRow {
        key: r.get::<_, i64>(0)? as Key,
        process_id: r.get(1)?,
        process_definition_id: r.get(2)?,
        process_definition_key: r.get(3)?,
        version: r.get(4)?,
        state: instance_state_from(r.get(5)?),
        start_date_ms: r.get::<_, i64>(6)? as u64,
        has_incident: r.get::<_, i64>(7)? != 0,
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
    Ok(UserTaskRow {
        key: r.get::<_, i64>(0)? as Key,
        instance_key: r.get::<_, i64>(1)? as Key,
        element_instance_key: r.get::<_, i64>(2)? as Key,
        element_id: r.get(3)?,
        state: user_task_state_from(r.get(4)?),
        assignee: r.get(5)?,
        created_at_ms: r.get::<_, i64>(6)? as u64,
        process_definition_id: r.get(7)?,
        process_definition_key: r.get(8)?,
        process_definition_version: r.get(9)?,
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

/// Upserts a batch of variables into the single instance-level scope (nano keeps
/// one scope per instance, so `scopeKey == processInstanceKey`). Names are sorted
/// so the autoincrement variable keys are assigned deterministically on a rebuild
/// (a `VariablesUpdated`/`ProcessInstanceCreated` event carries an unordered map).
/// An already-known name keeps its key and has its value overwritten.
fn upsert_variables(
    tx: &rusqlite::Transaction,
    instance_key: Key,
    variables: &std::collections::HashMap<String, Value>,
) -> rusqlite::Result<()> {
    if variables.is_empty() {
        return Ok(());
    }
    let (def_id, def_key) = instance_def(tx, instance_key);
    let mut entries: Vec<(&String, &Value)> = variables.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    for (name, value) in entries {
        tx.execute(
            "INSERT INTO variables (instance_key, scope_key, name, value, \
             process_definition_id, process_definition_key) VALUES (?1, ?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT(scope_key, name) DO UPDATE SET value = excluded.value",
            params![instance_key as i64, name, json_value(value), def_id, def_key],
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
    tx.query_row(
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
    tx.query_row(
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
            tx.execute(
                "INSERT INTO process_definitions (process_id, key, version) VALUES (?1, ?2, ?3) \
                 ON CONFLICT(process_id) DO UPDATE SET key = excluded.key, version = excluded.version",
                params![process.id, *process_definition_key as i64, version],
            )?;
        }

        Event::ProcessInstanceCreated {
            instance_key,
            process_id,
            created_at,
            variables,
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
            tx.execute(
                "INSERT INTO process_instances (key, process_id, process_definition_id, \
                 process_definition_key, version, state, start_date_ms, has_incident) \
                 VALUES (?1, ?2, ?2, ?3, ?4, ?5, ?6, 0) \
                 ON CONFLICT(key) DO UPDATE SET process_id = excluded.process_id, \
                 process_definition_id = excluded.process_definition_id, \
                 process_definition_key = excluded.process_definition_key, \
                 version = excluded.version, start_date_ms = excluded.start_date_ms",
                params![
                    *instance_key as i64,
                    process_id,
                    def_key,
                    version,
                    instance_state_code(ProcessInstanceState::Active),
                    *created_at as i64,
                ],
            )?;
            // Variables the instance was created with (the process-instance row
            // exists now, so the scope's denormalized definition resolves).
            upsert_variables(tx, *instance_key, variables)?;
        }

        Event::ProcessInstanceCompleted { instance_key } => {
            tx.execute(
                "UPDATE process_instances SET state = ?2 WHERE key = ?1",
                params![
                    *instance_key as i64,
                    instance_state_code(ProcessInstanceState::Completed)
                ],
            )?;
        }

        Event::ProcessInstanceTerminated { instance_key } => {
            tx.execute(
                "UPDATE process_instances SET state = ?2, has_incident = 0 WHERE key = ?1",
                params![
                    *instance_key as i64,
                    instance_state_code(ProcessInstanceState::Terminated)
                ],
            )?;
            // Close any incident still active on the terminated instance, so it
            // no longer surfaces as open in incident search.
            tx.execute(
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
        } => {
            let (def_id, def_key) = instance_def(tx, *instance_key);
            tx.execute(
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
                    DEFAULT_JOB_RETRIES,
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
            tx.execute(
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
            tx.execute(
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
            tx.execute(
                "UPDATE jobs SET state = ?2, retries = ?3, worker = NULL, deadline_ms = NULL \
                 WHERE key = ?1",
                params![*job_key as i64, job_state_code(state), retries],
            )?;
        }

        Event::JobErrorThrown { job_key, .. } => {
            tx.execute(
                "UPDATE jobs SET state = ?2, worker = NULL, deadline_ms = NULL WHERE key = ?1",
                params![*job_key as i64, job_state_code(JobState::Errored)],
            )?;
        }

        Event::JobCompleted { job_key, .. } => {
            tx.execute(
                "UPDATE jobs SET state = ?2, worker = NULL, deadline_ms = NULL WHERE key = ?1",
                params![*job_key as i64, job_state_code(JobState::Completed)],
            )?;
        }

        Event::JobCanceled { job_key, .. } => {
            tx.execute(
                "UPDATE jobs SET state = ?2, worker = NULL, deadline_ms = NULL WHERE key = ?1",
                params![*job_key as i64, job_state_code(JobState::Canceled)],
            )?;
        }

        Event::JobRetriesUpdated {
            job_key, retries, ..
        } => {
            tx.execute(
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
        } => {
            let (def_id, def_key) = instance_def(tx, *instance_key);
            let version = instance_version(tx, *instance_key);
            tx.execute(
                "INSERT INTO user_tasks (key, instance_key, element_instance_key, element_id, \
                 state, assignee, created_at_ms, process_definition_id, process_definition_key, \
                 process_definition_version) \
                 VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?8, ?9) \
                 ON CONFLICT(key) DO UPDATE SET state = excluded.state",
                params![
                    *user_task_key as i64,
                    *instance_key as i64,
                    *element_instance_key as i64,
                    element_id,
                    user_task_state_code(UserTaskState::Created),
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
            tx.execute(
                "UPDATE user_tasks SET assignee = ?2 WHERE key = ?1",
                params![*user_task_key as i64, assignee],
            )?;
        }

        Event::UserTaskCompleted { user_task_key, .. } => {
            tx.execute(
                "UPDATE user_tasks SET state = ?2 WHERE key = ?1",
                params![
                    *user_task_key as i64,
                    user_task_state_code(UserTaskState::Completed)
                ],
            )?;
        }

        Event::UserTaskCanceled { user_task_key, .. } => {
            tx.execute(
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
            tx.execute(
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
            tx.execute(
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
            tx.execute(
                "UPDATE incidents SET state = ?2 WHERE key = ?1",
                params![
                    *incident_key as i64,
                    incident_state_code(IncidentState::Resolved)
                ],
            )?;
            // `hasIncident` reflects only still-active incidents.
            let active: i64 = tx.query_row(
                "SELECT COUNT(*) FROM incidents WHERE instance_key = ?1 AND state = ?2",
                params![
                    *instance_key as i64,
                    incident_state_code(IncidentState::Active)
                ],
                |r| r.get(0),
            )?;
            tx.execute(
                "UPDATE process_instances SET has_incident = ?2 WHERE key = ?1",
                params![*instance_key as i64, i64::from(active > 0)],
            )?;
            // A recoverable job-incident returns its parked job to the pool.
            if let Some(job_key) = job_key {
                tx.execute(
                    "UPDATE jobs SET state = ?2, worker = NULL, deadline_ms = NULL WHERE key = ?1",
                    params![*job_key as i64, job_state_code(JobState::Created)],
                )?;
            }
        }

        Event::VariablesUpdated {
            instance_key,
            variables,
        } => {
            upsert_variables(tx, *instance_key, variables)?;
        }

        // Events with no queryable read-model projection.
        _ => {}
    }
    Ok(())
}
