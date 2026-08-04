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
    Event, IncidentKind, IncidentState, JobKind, JobState, Key, ListenerEventType,
    ProcessInstanceState, TaskListenerEventType, UserTaskState, Value, partition_of,
};
use rusqlite::{Connection, OptionalExtension, params};

/// The read store is a *derived* projection, so its on-disk schema needs no
/// hand-maintained version number to bump — and that number was exactly the
/// thing that drifted: columns were added to [`SCHEMA`] without incrementing it,
/// so a live database kept a "matching" version yet lacked the new columns, and
/// the first query for one panicked (poisoning the connection mutex, which
/// bricked every subsequent read — metrics, explorer, everything).
///
/// Instead the schema *identity* is **derived** from [`SCHEMA`]: a stable
/// content fingerprint. Any edit to `SCHEMA` — a new column, table, or type —
/// changes the fingerprint, so [`ReadStore::ensure_schema`] recreates the
/// projection automatically on the next open. There is nothing to remember to
/// bump and nothing to keep in sync, so this drift class cannot recur.
///
/// FNV-1a (64-bit) is used because it is dependency-free and deterministic
/// across runs, Rust versions, and architectures — unlike `DefaultHasher`,
/// whose algorithm may change between toolchains and would then spuriously
/// rebuild every store on a compiler upgrade.
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

const SCHEMA: &str = "
CREATE TABLE process_definitions (
    key        INTEGER PRIMARY KEY,
    process_id TEXT NOT NULL,
    version    INTEGER NOT NULL,
    xml        TEXT NOT NULL DEFAULT ''
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
    correlation_key        TEXT NOT NULL
);
CREATE INDEX idx_message_subscriptions_instance ON message_subscriptions(instance_key);
";

// --- enum <-> integer code mappings (kept beside the engine enums) ---

fn instance_state_code(s: ProcessInstanceState) -> i64 {
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
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
    }
}
fn incident_kind_from(code: i64) -> IncidentKind {
    match code {
        1 => IncidentKind::NoMatchingSequenceFlow,
        2 => IncidentKind::UnhandledError,
        3 => IncidentKind::ExpressionEvaluation,
        4 => IncidentKind::DecisionEvaluation,
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
/// materialized from `MessageSubscriptionCreated` and dropped on
/// `MessageCorrelated`/`MessageSubscriptionCanceled` or instance termination.
/// Feeds the MESSAGE variant of the element-instance wait-state API.
pub struct MessageSubscriptionRow {
    pub subscription_key: Key,
    pub instance_key: Key,
    pub element_instance_key: Key,
    pub element_id: String,
    pub message_name: String,
    pub correlation_key: String,
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

/// WAL autocheckpoint threshold in pages for the read-model store. Default 12288
/// (~48 MiB at the 4 KiB page size), 12x SQLite's built-in 1000, chosen to
/// coalesce repeated dirties of hot pages before they are copied back into the
/// main DB. Acts as the always-safe backstop bound on the WAL (works even when no
/// adaptive pruner runs). `NANOBPMN_READ_WAL_AUTOCHECKPOINT` overrides; 0 disables
/// SQLite's automatic checkpoint entirely (only safe when a pruner checkpoints).
fn read_wal_autocheckpoint_pages() -> i64 {
    std::env::var("NANOBPMN_READ_WAL_AUTOCHECKPOINT")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|v| *v >= 0)
        .unwrap_or(12288)
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
fn read_wal_truncate_bytes() -> u64 {
    std::env::var("NANOBPMN_READ_WAL_TRUNCATE_MB")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(32)
        * 1024
        * 1024
}

/// Read-model store space accounting reuses the shared SQLite helper so the
/// `(file_bytes, live_bytes)` derivation lives in one place (see
/// [`crate::sqlite_space::page_stats`]).
use crate::sqlite_space::page_stats;

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
    /// projection inside the single exporter thread.
    path: Option<PathBuf>,
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
            // WAL autocheckpoint threshold (pages). SQLite's built-in default is
            // 1000 (~4 MiB), which under a sustained projection flood checkpoints
            // hot pages back into the multi-hundred-MB main DB extremely often —
            // rewriting the same index / freelist / recently-inserted leaf pages
            // over and over and amplifying *physical* disk writes ~100x over the
            // logical change. A larger window coalesces repeated dirties of a page
            // into a single copy-back, cutting checkpoint write volume (and thus
            // disk saturation and the fsync-latency it inflates on a shared disk)
            // roughly in proportion. This is the backstop bound; when the adaptive
            // pruner runs it checkpoints off the exporter thread at a lower
            // threshold (see `maybe_checkpoint_wal`) so this rarely fires inline.
            // `NANOBPMN_READ_WAL_AUTOCHECKPOINT` overrides (0 disables auto).
            conn.pragma_update(None, "wal_autocheckpoint", read_wal_autocheckpoint_pages())?;
            conn.busy_timeout(std::time::Duration::from_secs(5))?;
        }
        let store = Self {
            conn: Mutex::new(conn),
            write_lock: Mutex::new(()),
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
        conn.execute("UPDATE meta SET v = v WHERE k = 'schema_fingerprint'", [])?;
        Ok(())
    }

    fn ensure_schema(&self) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("read store poisoned");
        let want = schema_fingerprint();
        let current: Option<i64> = conn
            .query_row(
                "SELECT v FROM meta WHERE k = 'schema_fingerprint'",
                [],
                |r| r.get(0),
            )
            .optional()
            .unwrap_or(None);
        if current == Some(want) {
            return Ok(());
        }
        // Fresh, or a stale/foreign schema: (re)create from scratch. The read
        // store is a derived projection rebuilt from the journal, so wiping it is
        // always safe. We drop *every* existing user table discovered in
        // `sqlite_master` rather than a hand-maintained list: a static list
        // silently drifts as `SCHEMA` gains tables (it previously omitted the
        // `decision_*` tables), and a missed drop makes the subsequent
        // `CREATE TABLE` fail with "table already exists", bricking startup. This
        // is the durable structural guard against that drift class — the drop set
        // is derived from the live database, so it can never fall behind `SCHEMA`.
        let existing_tables: Vec<String> = {
            let mut stmt = conn.prepare(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
            )?;
            let names = stmt.query_map([], |r| r.get::<_, String>(0))?;
            names.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut drop_sql = String::new();
        for name in &existing_tables {
            // Names come from sqlite_master (our own tables); quote defensively.
            drop_sql.push_str(&format!(
                "DROP TABLE IF EXISTS \"{}\";",
                name.replace('"', "\"\"")
            ));
        }
        if !drop_sql.is_empty() {
            conn.execute_batch(&drop_sql)?;
        }
        conn.execute_batch(SCHEMA)?;
        conn.execute(
            "INSERT INTO meta (k, v) VALUES ('schema_fingerprint', ?1)",
            params![want],
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
             DROP TABLE IF EXISTS definition_elements;
             DROP TABLE IF EXISTS element_instances;
             DROP TABLE IF EXISTS message_subscriptions;
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
        Ok(ExportOutcome {
            terminal_keys,
            inflight_delta,
        })
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
    pub fn maybe_checkpoint_wal(&self, conn: &Connection) -> bool {
        self.checkpoint_wal_if_larger_than(conn, read_wal_truncate_bytes())
    }

    /// Core of [`maybe_checkpoint_wal`] with an explicit byte threshold (so tests
    /// can exercise the gate without racing a process-global env var).
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
        // Search surfaces only the latest version per process id (the endpoint's
        // `isLatestVersion` filter and Zeebe-parity list semantics), even though
        // every version's XML is retained for by-key diagram lookups.
        let mut stmt = conn
            .prepare(
                "SELECT key, process_id, version FROM process_definitions pd \
                 WHERE version = (SELECT MAX(version) FROM process_definitions \
                                  WHERE process_id = pd.process_id)",
            )
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
     element_id, message_name, correlation_key";

fn map_message_subscription(r: &rusqlite::Row) -> rusqlite::Result<MessageSubscriptionRow> {
    Ok(MessageSubscriptionRow {
        subscription_key: r.get::<_, i64>(0)? as Key,
        instance_key: r.get::<_, i64>(1)? as Key,
        element_instance_key: r.get::<_, i64>(2)? as Key,
        element_id: r.get(3)?,
        message_name: r.get(4)?,
        correlation_key: r.get(5)?,
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
    let (element_type, element_name): (String, Option<String>) = tx
        .cquery_row(
            "SELECT element_type, element_name FROM definition_elements \
             WHERE process_definition_key = ?1 AND element_id = ?2",
            params![def_key_int, element_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .ok()
        .flatten()
        .unwrap_or_else(|| ("UNKNOWN".to_string(), None));
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
            // instance). Search stays scoped to the latest version per id (see
            // `process_definitions`). `ON CONFLICT(key)` refreshes idempotently on
            // replay/re-delivery of the same ProcessDeployed event.
            tx.cexecute(
                "INSERT INTO process_definitions (process_id, key, version, xml) VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(key) DO UPDATE SET process_id = excluded.process_id, version = excluded.version, xml = excluded.xml",
                params![process.id, *process_definition_key as i64, version, process.xml],
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
        } => {
            // Resolve the deployed identity now (defaults mirror
            // `process_instance_result` when no definition is on record). With
            // every version retained, pick the latest deployed so far — during an
            // ordered replay only versions deployed before this create are on
            // record, so MAX(version) is the version the instance was created on.
            let (def_key, version): (String, i32) = tx
                .query_row(
                    "SELECT key, version FROM process_definitions WHERE process_id = ?1 \
                     ORDER BY version DESC LIMIT 1",
                    params![process_id],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i32>(1)?)),
                )
                .optional()?
                .map(|(k, v)| (k.to_string(), v))
                .unwrap_or_else(|| ("-1".to_string(), 0));
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
                 process_definition_key, version, state, start_date_ms, has_incident, tags, business_id) \
                 VALUES (?1, ?2, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8) \
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
            ..
        } => {
            if *element_instance_key != 0 {
                tx.cexecute(
                    "INSERT INTO message_subscriptions (subscription_key, instance_key, \
                     element_instance_key, element_id, message_name, correlation_key) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                     ON CONFLICT(subscription_key) DO UPDATE SET \
                     element_instance_key = excluded.element_instance_key, \
                     element_id = excluded.element_id, \
                     message_name = excluded.message_name, \
                     correlation_key = excluded.correlation_key",
                    params![
                        *subscription_key as i64,
                        *instance_key as i64,
                        *element_instance_key as i64,
                        element_id,
                        message_name,
                        correlation_key,
                    ],
                )?;
            }
        }

        // A correlated or cancelled subscription is no longer waiting: drop it.
        // `RemoteMessageCorrelation` is the multi-partition counterpart of
        // `MessageCorrelated` — when the process instance lives on another
        // partition, the message partition settles the canonical subscription
        // with this event instead, so it must clear the row too.
        Event::MessageCorrelated {
            subscription_key, ..
        }
        | Event::RemoteMessageCorrelation {
            subscription_key, ..
        }
        | Event::MessageSubscriptionCanceled {
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
    fn schema_fingerprint_is_stable_within_a_build() {
        // Derived purely from SCHEMA, so it is constant across calls and never
        // hand-maintained.
        assert_eq!(super::schema_fingerprint(), super::schema_fingerprint());
    }

    #[test]
    fn stale_on_disk_schema_is_rebuilt() {
        // Reproduces the production incident: a database left behind by an older
        // build whose `jobs` table lacks the `job_kind`/`listener_event_type`
        // columns, yet whose stored identity looks "current". Opening it must
        // rebuild the projection (SCHEMA drift is detected via the derived
        // fingerprint), NOT leave a half-shaped table that panics — and poisons
        // the connection — on the first query for a missing column.
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

        let store = ReadStore::open(Some(&path)).expect("stale schema self-heals on open");

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
        // ...and the stored identity now equals the derived fingerprint, so a
        // second open is a no-op (no rebuild).
        drop(store);
        let store2 = ReadStore::open(Some(&path)).unwrap();
        let stored: Option<i64> = {
            let conn = store2.conn.lock().unwrap();
            conn.query_row(
                "SELECT v FROM meta WHERE k = 'schema_fingerprint'",
                [],
                |r| r.get(0),
            )
            .ok()
        };
        assert_eq!(stored, Some(super::schema_fingerprint()));
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn matching_fingerprint_does_not_rebuild() {
        // When the stored fingerprint already matches, open must NOT drop the
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

        // Both versions' XML remain serveable by key: an older-version instance's
        // Explorer diagram survives a redeploy (the bug this fixes).
        assert_eq!(
            store.process_definition_xml(6).as_deref(),
            Some("<xml>v1</xml>")
        );
        assert_eq!(
            store.process_definition_xml(297).as_deref(),
            Some("<xml>v2</xml>")
        );

        // Search remains scoped to the latest version per id.
        let defs = store.process_definitions();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].key, 297);
        assert_eq!(defs[0].version, 2);
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

    use nanobpmn_engine_core::{Event, ProcessBuilder};

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
        }
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
}
