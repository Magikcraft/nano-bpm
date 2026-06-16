//! An append-only event journal that makes the embedded engine durable.
//!
//! The engine itself is in-memory and event-sourced ([`Engine::apply_command_at`]
//! returns the complete, ordered list of events a command produced). This
//! [`Journal`] wraps the engine and persists those events to a newline-delimited
//! JSON log. Writes are handed to a dedicated background thread that
//! **group-commits** — batching every concurrently in-flight command into a
//! single `write` + `fsync` — and a command is acknowledged only once its events
//! are fsynced, so anything the server returns `200` for survives a crash. On
//! startup the log is replayed through [`Engine::replay`] to reconstruct state
//! and the key generator.
//!
//! **Activation locks are deliberately not journaled.** Job activation
//! (`activateJobs`) and lock expiry are *volatile lease state*: a crash forfeits
//! every lock, returning uncompleted jobs to the activatable pool. Recording
//! only durable business facts keeps the log small and gives a clean recovery
//! semantic — after a restart, workers simply re-activate.

use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

use nanobpmn_engine_core::{
    ActivatedJob, Command, Engine, EngineError, Event, Incident, Key, ProcessInstance, State,
};
use tokio::sync::oneshot;

use crate::coldspill::ColdIndex;
use crate::varspill::VarSpillStore;

/// A durable-write request handed to the background journal writer: the
/// newline-terminated, serialized bytes for one command's events, plus a
/// one-shot sender signalled once those bytes are fsynced to disk.
struct WriteRequest {
    bytes: Vec<u8>,
    ack: oneshot::Sender<()>,
}

/// A handle to the durability of a single write. Awaiting [`Commit::wait`]
/// resolves once the command's events have been group-committed (written and
/// fsynced) by the background writer thread. A `Ready` commit is already durable
/// (an in-memory journal, or a command that produced no events).
#[must_use = "await the commit to guarantee the write is durable before responding"]
pub struct Commit(CommitInner);

enum CommitInner {
    Ready,
    Pending(oneshot::Receiver<()>),
}

impl Commit {
    fn ready() -> Self {
        Commit(CommitInner::Ready)
    }

    /// Waits until the write backing this commit has been fsynced. If the writer
    /// thread is gone (shutdown), it resolves immediately rather than hanging.
    pub async fn wait(self) {
        if let CommitInner::Pending(rx) = self.0 {
            let _ = rx.await;
        }
    }
}

/// The engine plus its durable event log.
pub struct Journal {
    engine: Engine,
    /// `None` for an in-memory (non-persistent) journal; otherwise the channel to
    /// the background writer thread that owns the log file.
    writer: Option<Sender<WriteRequest>>,
    /// Joined on drop so any not-yet-acked writes are flushed before the journal
    /// goes away (matters for synchronous callers that never await the commit).
    writer_thread: Option<JoinHandle<()>>,
    /// Channel to the read-model exporter thread, if one is wired. Every command's
    /// journaled events are forwarded here (in command order: the actor applies
    /// commands serially) to be projected into the read store. The events are
    /// shared with the command's caller via `Arc`, so forwarding them costs only a
    /// refcount bump — the 50 KB variable payloads are never deep-copied on the
    /// single command thread.
    exporter: Option<Sender<Arc<Vec<Event>>>>,
    /// `true` when the journal started with no prior log, so the host knows it
    /// should seed any initial deployments.
    fresh: bool,
    /// Optional disk-backed store for spilled instance variables, with the hot
    /// budget (max resident spillable instances) above which the engine sheds the
    /// oldest backlog's variables to disk. `None` keeps every payload resident
    /// (the original behaviour).
    spill: Option<(Arc<VarSpillStore>, usize)>,
    /// Optional cold spill: evicts whole *dormant* instances (control state and
    /// all) to the same disk-backed store, keeping only a slim resident routing
    /// index ([`ColdIndex`]) so an event can rehydrate the owning instance on
    /// demand. Triggered by hot-RAM pressure (resident bytes crossing
    /// `high_water`), it sheds the oldest dormant instances until back under
    /// `low_water`. `None` keeps every instance resident. This is the long-lived,
    /// low-throughput counterpart to variable spill: where variable spill bounds a
    /// large *active* backlog, cold spill bounds a large *parked* one.
    cold: Option<ColdSpill>,
}

/// Cold-spill state held by a [`Journal`]: the shared disk store, the resident
/// routing index, and the resident-byte watermarks that gate the sweep.
struct ColdSpill {
    store: Arc<VarSpillStore>,
    index: ColdIndex,
    high_water: u64,
    low_water: u64,
}

/// The background journal writer: blocks for the next request, drains every
/// other request already queued, then **group-commits** the whole batch in a
/// single `write` + `fsync` before signalling each command's commit. Batching
/// amortizes one fsync across all concurrently in-flight writes.
///
/// A write or fsync failure is unrecoverable — the in-memory engine has already
/// advanced past the durable log — so the process is aborted rather than risk
/// acknowledging or serving non-durable state (mirrors the previous
/// panic-on-I/O-error contract).
fn writer_loop(mut file: File, rx: Receiver<WriteRequest>) {
    while let Ok(first) = rx.recv() {
        let mut batch = vec![first];
        while let Ok(next) = rx.try_recv() {
            batch.push(next);
        }

        let mut buf = Vec::new();
        for req in &batch {
            buf.extend_from_slice(&req.bytes);
        }

        if let Err(e) = file.write_all(&buf).and_then(|()| file.sync_all()) {
            tracing::error!(
                "journal write failed: {e}; aborting to avoid serving non-durable state"
            );
            std::process::abort();
        }

        for req in batch {
            // The receiver is gone for fire-and-forget writes (the background
            // tick and startup seeding never await their commit); that's fine.
            let _ = req.ack.send(());
        }
    }
}

impl Journal {
    /// A non-persistent journal: the engine runs purely in memory and nothing is
    /// written. Used for tests and ephemeral runs.
    pub fn in_memory() -> Self {
        Self {
            engine: Engine::new(),
            writer: None,
            writer_thread: None,
            exporter: None,
            fresh: true,
            spill: None,
            cold: None,
        }
    }

    /// Reads and deserializes every event from the journal log at `path` (an
    /// empty vec if the file does not exist). Shared by [`Journal::open`] (to
    /// replay into the engine) and the boot-time read-model catch-up.
    pub fn read_events(path: impl AsRef<Path>) -> io::Result<Vec<Event>> {
        let path = path.as_ref();
        let mut events = Vec::new();
        if path.exists() {
            for line in BufReader::new(File::open(path)?).lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let event: Event = serde_json::from_str(&line)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                events.push(event);
            }
        }
        Ok(events)
    }

    /// Opens (creating if absent) the journal at `path`, replaying any existing
    /// log to reconstruct engine state, then spawns the background writer thread
    /// positioned to append.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let mut engine = Engine::new();
        let mut fresh = true;

        let events = Self::read_events(path)?;
        if !events.is_empty() {
            engine = Engine::replay(events);
            fresh = false;
        }

        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let (tx, rx) = mpsc::channel::<WriteRequest>();
        let writer_thread = thread::Builder::new()
            .name("nanobpmn-journal-writer".into())
            .spawn(move || writer_loop(file, rx))
            .expect("spawn journal writer thread");

        Ok(Self {
            engine,
            writer: Some(tx),
            writer_thread: Some(writer_thread),
            exporter: None,
            fresh,
            spill: None,
            cold: None,
        })
    }

    /// Wires the disk-backed variable spill. `budget` is the maximum number of
    /// resident instances allowed to hold their variables in hot RAM; once the
    /// active backlog exceeds it, each command that grows the backlog sheds the
    /// oldest instances' variables to `store` (rehydrated on job activation). Set
    /// before serving. A `budget` of 0 spills aggressively (keeps nothing extra
    /// resident); leaving the spill unset keeps the original all-resident
    /// behaviour.
    pub fn set_spill(&mut self, store: Arc<VarSpillStore>, budget: usize) {
        self.spill = Some((store, budget));
    }

    /// Wires cold spill onto the same disk-backed `store`. Once the engine's
    /// resident memory crosses `high_water` bytes, the periodic sweep
    /// ([`maybe_cold_spill`](Journal::maybe_cold_spill)) evicts whole dormant
    /// instances to the store — oldest first — until resident memory falls back
    /// under `low_water`, keeping only a slim routing index resident. A cold
    /// instance is rehydrated on demand the moment an event targets it. Set
    /// before serving; leaving it unset keeps every instance resident.
    pub fn set_cold_spill(&mut self, store: Arc<VarSpillStore>, high_water: u64, low_water: u64) {
        self.cold = Some(ColdSpill {
            store,
            index: ColdIndex::default(),
            high_water,
            low_water,
        });
    }

    /// How many instances are currently cold (off-heap). Zero when cold spill is
    /// unset or nothing is parked. Exposed for observability/tests.
    pub fn cold_count(&self) -> usize {
        self.cold.as_ref().map(|c| c.index.len()).unwrap_or(0)
    }

    /// Rehydrates the cold instance `key` back into hot state: takes its snapshot
    /// from the store, restores it into the engine, and drops its routing-index
    /// entries. A no-op (returns `false`) if cold spill is unset or `key` is not
    /// cold. A store miss (snapshot lost) still clears the stale index entry so a
    /// later event cannot loop trying to rehydrate a vanished instance — the
    /// journal remains the durable source of truth.
    fn rehydrate_cold(&mut self, key: Key) -> bool {
        let store = match self.cold.as_ref() {
            Some(cold) if cold.index.contains(key) => Arc::clone(&cold.store),
            _ => return false,
        };
        let snapshot = store.take_cold(key);
        if let Some(cold) = self.cold.as_mut() {
            cold.index.remove(key);
        }
        match snapshot {
            Some(snapshot) => {
                self.engine.rehydrate_instance(snapshot);
                true
            }
            None => false,
        }
    }

    /// Rehydrates any cold instance a command is about to target, so the engine
    /// processes the command against complete hot state. Cheap when cold spill is
    /// unset or nothing relevant is cold (a few index lookups). Routing mirrors
    /// the engine's own: by job key (`CompleteJob`/`FailJob`/...), user-task key,
    /// incident key, message name+correlation (`CorrelateMessage`), timer due-time
    /// (`TriggerTimers`), the instance key (`CancelInstance`), or an instance-root
    /// or element-instance scope key (`SetVariables`).
    fn ensure_resident_for_command(&mut self, command: &Command) {
        let Some(cold) = self.cold.as_ref() else {
            return;
        };
        let mut targets: Vec<Key> = Vec::new();
        match command {
            Command::CompleteJob { job_key, .. }
            | Command::FailJob { job_key, .. }
            | Command::ThrowJobError { job_key, .. }
            | Command::UpdateJobRetries { job_key, .. } => {
                targets.extend(cold.index.instance_for_job(*job_key));
            }
            Command::AssignUserTask { user_task_key, .. }
            | Command::UnassignUserTask { user_task_key }
            | Command::UpdateUserTask { user_task_key, .. }
            | Command::CompleteUserTask { user_task_key, .. } => {
                targets.extend(cold.index.instance_for_user_task(*user_task_key));
            }
            Command::ResolveIncident { incident_key, .. } => {
                targets.extend(cold.index.instance_for_incident(*incident_key));
            }
            Command::SetVariables { scope_key, .. } => {
                // The scope may be the instance root or one of its active
                // element-instance (token) scopes; both can address a cold
                // instance and must page it back in before the update applies.
                if cold.index.contains(*scope_key) {
                    targets.push(*scope_key);
                } else {
                    targets.extend(cold.index.instance_for_scope(*scope_key));
                }
            }
            Command::CancelInstance { instance_key } => {
                if cold.index.contains(*instance_key) {
                    targets.push(*instance_key);
                }
            }
            Command::CorrelateMessage {
                message_name,
                correlation_key,
                ..
            } => {
                targets.extend(cold.index.instances_for_message(message_name, correlation_key));
            }
            Command::TriggerTimers { now } => {
                targets.extend(cold.index.instances_due(*now));
            }
            _ => {}
        }
        for key in targets {
            self.rehydrate_cold(key);
        }
    }

    /// Sheds dormant instances to disk when hot RAM crosses the high-water mark.
    /// Cheap below the mark (one resident-bytes read). Above it, snapshots the
    /// oldest cold-spillable instances in bounded batches — lifting each whole
    /// instance (control state, jobs, timers, subscriptions) out of the engine
    /// and into the store, recording only its routing facets in the resident
    /// index — until resident memory falls under the low-water mark or no dormant
    /// instance remains. Run from the background tick on the engine thread. The
    /// snapshots are derived from the durable journal, so a lost one is
    /// reconstructable: this is a memory cache, not a system of record.
    pub fn maybe_cold_spill(&mut self) {
        let (high, low) = match self.cold.as_ref() {
            Some(cold) if cold.high_water > 0 => (cold.high_water, cold.low_water),
            _ => return,
        };
        let Some(resident) = crate::memory::resident_bytes() else {
            return;
        };
        let resident = resident as u64;
        if resident < high {
            return;
        }
        let store = Arc::clone(&self.cold.as_ref().expect("cold set").store);
        let mut spilled = 0usize;
        loop {
            let batch = self.engine.cold_spillable_instances(64);
            if batch.is_empty() {
                break;
            }
            let mut spilled_in_batch = 0usize;
            for key in batch {
                let Some(snapshot) = self.engine.snapshot_instance(key) else {
                    continue;
                };
                if store.put_cold(key, &snapshot).is_ok() {
                    self.cold.as_mut().expect("cold set").index.insert(&snapshot);
                    spilled_in_batch += 1;
                } else {
                    // Store write failed: keep the instance resident rather than
                    // drop it from hot state with no durable cold copy.
                    self.engine.rehydrate_instance(snapshot);
                }
            }
            spilled += spilled_in_batch;
            if spilled_in_batch == 0 {
                break;
            }
            // Compact + purge so the resident reading reflects the eviction, then
            // re-check against the low-water mark.
            self.engine.shrink();
            let _ = crate::memory::purge();
            match crate::memory::resident_bytes() {
                Some(now) if (now as u64) < low => break,
                _ => {}
            }
        }
        if spilled > 0 {
            tracing::info!(
                "cold spill: shed {spilled} dormant instance(s) to disk ({} now cold)",
                self.cold.as_ref().expect("cold set").index.len(),
            );
        }
    }

    /// Test hook: cold-spills every currently dormant instance regardless of the
    /// memory watermark, returning the number shed. Lets tests exercise the
    /// spill/rehydrate seams deterministically without driving real RAM pressure.
    #[cfg(test)]
    pub fn force_cold_spill_all(&mut self) -> usize {
        let store = match self.cold.as_ref() {
            Some(cold) => Arc::clone(&cold.store),
            None => return 0,
        };
        let mut spilled = 0usize;
        loop {
            let batch = self.engine.cold_spillable_instances(64);
            if batch.is_empty() {
                break;
            }
            let mut spilled_in_batch = 0usize;
            for key in batch {
                let Some(snapshot) = self.engine.snapshot_instance(key) else {
                    continue;
                };
                if store.put_cold(key, &snapshot).is_ok() {
                    self.cold.as_mut().expect("cold set").index.insert(&snapshot);
                    spilled_in_batch += 1;
                } else {
                    self.engine.rehydrate_instance(snapshot);
                }
            }
            spilled += spilled_in_batch;
            if spilled_in_batch == 0 {
                break;
            }
        }
        spilled
    }

    /// Wires the read-model exporter channel. Set before any command is applied
    /// (including demo seeding) so every journaled event is projected.
    pub fn set_exporter(&mut self, exporter: Sender<Arc<Vec<Event>>>) {
        self.exporter = Some(exporter);
    }

    /// Evicts a batch of completed instances in a single pass over hot state.
    /// Mirrors [`Engine::evict_instances`]. Used on the steady-state exporter
    /// path once the read model has the completions. Also drops any spilled
    /// variable / cold rows the evicted (now terminal) instances left behind, so
    /// the spill store stays bounded to the live backlog rather than accumulating
    /// orphan payloads for every completed or cancelled instance.
    pub fn evict_instances(&mut self, keys: &[Key]) -> usize {
        if let Some(store) = self.spill_store() {
            store.forget(keys);
        }
        self.engine.evict_instances(keys)
    }

    /// The shared disk-backed spill/cold store, if either tier is wired. Both
    /// tiers share one [`VarSpillStore`] (one file, one WAL), so either handle
    /// reaches the same `spill` and `cold` tables.
    fn spill_store(&self) -> Option<Arc<VarSpillStore>> {
        if let Some((store, _)) = self.spill.as_ref() {
            return Some(Arc::clone(store));
        }
        self.cold.as_ref().map(|c| Arc::clone(&c.store))
    }

    /// Evicts every completed instance from hot state (used after a boot replay,
    /// once the read model is caught up). Mirrors [`Engine::evict_completed`].
    pub fn evict_completed(&mut self) -> usize {
        self.engine.evict_completed()
    }

    /// Shrinks the hot-state map capacities to fit their live contents, returning
    /// the bucket arrays freed by prior eviction back to the allocator. Mirrors
    /// [`Engine::shrink`]; called on the idle-purge path, where steady-state
    /// eviction has already removed the entries but left the maps at peak
    /// capacity.
    pub fn shrink(&mut self) {
        self.engine.shrink();
    }

    /// Whether the journal started empty (no prior log).
    pub fn is_fresh(&self) -> bool {
        self.fresh
    }

    /// Serializes `events` for the durable writer and forwards them to the
    /// read-model exporter, returning a [`Commit`] that resolves once they are
    /// fsynced. The events are shared via `Arc`, so the exporter handoff is a
    /// refcount bump rather than a deep copy of the (up to 50 KB) payloads — the
    /// single command thread never clones them. Empty batches and in-memory
    /// journals are already durable, so they return a ready commit.
    fn persist(&self, events: &Arc<Vec<Event>>) -> Commit {
        if events.is_empty() {
            return Commit::ready();
        }

        // Forward to the read-model exporter in command order (the actor applies
        // commands serially). Independent of disk persistence, so the in-memory
        // journal still feeds an in-memory read store. `Arc::clone` is cheap.
        if let Some(exporter) = self.exporter.as_ref() {
            let _ = exporter.send(Arc::clone(events));
        }

        let Some(writer) = self.writer.as_ref() else {
            return Commit::ready();
        };

        let mut bytes = Vec::new();
        for event in events.iter() {
            let line = serde_json::to_string(event).expect("event serializes");
            bytes.extend_from_slice(line.as_bytes());
            bytes.push(b'\n');
        }

        let (ack, rx) = oneshot::channel();
        match writer.send(WriteRequest { bytes, ack }) {
            Ok(()) => Commit(CommitInner::Pending(rx)),
            // The writer thread is gone (shutting down); treat as already settled
            // so callers never hang.
            Err(_) => Commit::ready(),
        }
    }

    /// Applies a durable command at logical instant `now`, journaling its events
    /// on success. Returns the events (shared via `Arc` with the read-model
    /// exporter) and a [`Commit`] the caller should await before acknowledging the
    /// request. Mirrors [`Engine::apply_command_at`].
    pub fn apply_command_at(
        &mut self,
        command: Command,
        now: u64,
    ) -> Result<(Arc<Vec<Event>>, Commit), EngineError> {
        self.ensure_resident_for_command(&command);
        let events = Arc::new(self.engine.apply_command_at(command, now)?);
        let commit = self.persist(&events);
        self.maybe_spill();
        Ok((events, commit))
    }

    /// Applies a durable command using the engine's current clock, journaling its
    /// events on success. Mirrors [`Engine::apply_command`].
    pub fn apply_command(
        &mut self,
        command: Command,
    ) -> Result<(Arc<Vec<Event>>, Commit), EngineError> {
        self.ensure_resident_for_command(&command);
        let events = Arc::new(self.engine.apply_command(command)?);
        let commit = self.persist(&events);
        self.maybe_spill();
        Ok((events, commit))
    }

    /// Sheds the oldest backlog's variables to the spill store when the resident
    /// spillable set exceeds the hot budget. Cheap when within budget (one
    /// counter read); only a genuinely growing backlog pays the spill writes.
    /// The variables are already durable in the journal, so a spill write that
    /// is later lost is reconstructable — it is a cache, not a system of record.
    fn maybe_spill(&mut self) {
        let Some((store, budget)) = self.spill.as_ref() else {
            return;
        };
        let resident = self.engine.resident_spillable_count();
        if resident <= *budget {
            return;
        }
        let store = Arc::clone(store);
        let over = resident - *budget;
        for key in self.engine.spillable_instances(over) {
            if let Some(vars) = self.engine.spill_variables(key)
                && store.put(key, &vars).is_err()
            {
                // Spill failed: keep the payload resident rather than lose it.
                self.engine.rehydrate_variables(key, vars);
            }
        }
    }

    /// Activates jobs **without** journaling: activation locks are volatile lease
    /// state, forfeited on restart. Mirrors [`Engine::activate_jobs`].
    ///
    /// When variable spill is wired, an activated job's instance may have had its
    /// variables shed to disk. Activation is exactly the moment they are needed
    /// again (the worker receives them, and completion may follow), so each
    /// spilled instance is rehydrated here: its payload is taken back from the
    /// store, restored into hot state, and used to fill the activated job. This
    /// is the read side of the memory/disk fusion — a single keyed SQLite read,
    /// served from the page cache for a warm working set.
    pub fn activate_jobs(
        &mut self,
        job_type: impl Into<String>,
        worker: impl Into<String>,
        max_jobs: usize,
        timeout: u64,
        now: u64,
    ) -> Vec<ActivatedJob> {
        let job_type = job_type.into();
        // Rehydrate up to `max_jobs` cold instances holding an activatable job of
        // this type, so a worker poll can reach a parked-then-cold backlog. Only
        // pays a SQLite read when something of this type is actually cold.
        if self.cold.is_some() {
            let candidates = self
                .cold
                .as_ref()
                .expect("cold set")
                .index
                .instances_for_job_type(&job_type, max_jobs);
            for key in candidates {
                self.rehydrate_cold(key);
            }
        }
        let mut activated = self
            .engine
            .activate_jobs(&job_type, worker, max_jobs, timeout, now);
        if let Some((store, _)) = self.spill.as_ref() {
            let store = Arc::clone(store);
            for job in activated.iter_mut() {
                if !self.engine.is_variables_spilled(job.instance_key) {
                    continue;
                }
                if let Some(vars) = store.take(job.instance_key) {
                    let vars = Arc::new(vars);
                    self.engine
                        .rehydrate_variables(job.instance_key, Arc::clone(&vars));
                    job.variables = vars;
                }
            }
        }
        activated
    }

    /// Fires every due timer at logical instant `now`, journaling the resulting
    /// events (timers are durable business facts). Mirrors
    /// [`Engine::trigger_timers`]; returns the events produced (empty if none
    /// were due) and their [`Commit`].
    pub fn trigger_timers(&mut self, now: u64) -> (Arc<Vec<Event>>, Commit) {
        // Rehydrate any cold instance whose armed timer is now due, so the tick
        // can fire it. No-op when nothing cold is due.
        if self.cold.is_some() {
            let due = self
                .cold
                .as_ref()
                .expect("cold set")
                .index
                .instances_due(now);
            for key in due {
                self.rehydrate_cold(key);
            }
        }
        let events = Arc::new(self.engine.trigger_timers(now));
        let commit = self.persist(&events);
        (events, commit)
    }

    /// Releases expired activation locks at logical instant `now`, **without**
    /// journaling: like activation, lock expiry is volatile lease state. Mirrors
    /// [`Engine::expire_jobs`]. Returns the reclaimed-job events so the caller can
    /// wake dispatch when a lease frees a job for redelivery.
    pub fn expire_jobs(&mut self, now: u64) -> Vec<Event> {
        self.engine.expire_jobs(now)
    }

    /// Read-only access to the underlying engine (for projections that take an
    /// `&Engine`).
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    // --- Read delegations mirroring the engine API the server uses. ---

    pub fn state(&self) -> &State {
        self.engine.state()
    }

    pub fn instance(&self, key: Key) -> Option<&ProcessInstance> {
        self.engine.instance(key)
    }

    pub fn incident(&self, key: Key) -> Option<&Incident> {
        self.engine.incident(key)
    }

    pub fn incidents(&self) -> Vec<&Incident> {
        self.engine.incidents()
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        // Close the channel so the writer thread drains any remaining requests
        // and exits, then join it to guarantee fire-and-forget writes hit disk.
        self.writer = None;
        if let Some(handle) = self.writer_thread.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nanobpmn_engine_core::ProcessBuilder;

    fn demo() -> nanobpmn_engine_core::ProcessDefinition {
        ProcessBuilder::new("demo")
            .start_event("start")
            .service_task("work", "demo-work")
            .end_event("end")
            .connect("start", "work")
            .connect("work", "end")
            .build()
            .expect("valid demo process")
    }

    #[test]
    fn reopening_a_journal_replays_persisted_state() {
        // given a journal file in a temp dir with a deploy + an instance
        let dir = std::env::temp_dir().join(format!("nanobpmn-journal-{}", std::process::id()));
        let path = dir.join("test.journal");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let instance_key = {
            let mut journal = Journal::open(&path).unwrap();
            assert!(journal.is_fresh());
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let (events, _) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            events.iter().find_map(|e| e.instance_key()).unwrap()
        };

        // when a fresh journal is opened over the same file
        let reopened = Journal::open(&path).unwrap();

        // then it is not fresh and the instance is recovered
        assert!(!reopened.is_fresh());
        assert!(reopened.instance(instance_key).is_some());
        // and the demo process is deployed exactly once (still version 1)
        assert_eq!(reopened.state().processes.len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn cold_journal() -> Journal {
        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        // Watermarks are irrelevant: tests drive spill via force_cold_spill_all.
        journal.set_cold_spill(store, 1, 0);
        journal
    }

    fn deploy_and_create(journal: &mut Journal) -> Key {
        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let (events, _) = journal
            .apply_command(Command::create_instance("demo"))
            .unwrap();
        events.iter().find_map(|e| e.instance_key()).unwrap()
    }

    #[test]
    fn cold_spilled_instance_rehydrates_on_job_activation_and_completes() {
        let mut journal = cold_journal();
        let key = deploy_and_create(&mut journal);

        // the instance is parked on the "demo-work" job: dormant and spillable
        assert!(journal.instance(key).is_some());
        assert_eq!(journal.force_cold_spill_all(), 1);
        assert_eq!(journal.cold_count(), 1);
        // its whole control state is now off-heap
        assert!(journal.instance(key).is_none());

        // a worker poll rehydrates it from the cold store and hands back the job
        let jobs = journal.activate_jobs("demo-work", "w", 1, 1_000, 0);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].instance_key, key);
        assert_eq!(journal.cold_count(), 0);
        assert!(journal.instance(key).is_some());

        // completing the rehydrated job drives the instance to completion
        let (events, _) = journal
            .apply_command(Command::complete_job(jobs[0].key))
            .unwrap();
        assert!(events
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceCompleted { instance_key } if *instance_key == key)));
    }

    #[test]
    fn set_variables_on_an_element_scope_rehydrates_a_cold_instance() {
        use nanobpmn_engine_core::Value;

        let mut journal = cold_journal();
        let key = deploy_and_create(&mut journal);

        // the active token scope at the service task — a valid SetVariables target
        // that is NOT the instance root key
        let scope_key = *journal
            .instance(key)
            .unwrap()
            .active
            .keys()
            .find(|k| **k != key)
            .expect("an element-instance scope");

        assert_eq!(journal.force_cold_spill_all(), 1);
        assert!(journal.instance(key).is_none());

        // SetVariables addresses the element scope: ensure_resident_for_command
        // must route it back to the cold instance and page it in (else the engine
        // would raise ScopeNotFound)
        let mut vars = std::collections::HashMap::new();
        vars.insert("approved".to_string(), Value::Bool(true));
        let (events, _) = journal
            .apply_command(Command::set_variables(scope_key, vars))
            .unwrap();
        assert_eq!(journal.cold_count(), 0);
        assert!(journal.instance(key).is_some());
        assert!(events
            .iter()
            .any(|e| matches!(e, Event::VariablesUpdated { instance_key, .. } if *instance_key == key)));
    }

    #[test]
    fn command_targeting_a_cold_instance_rehydrates_it_first() {
        let mut journal = cold_journal();
        let key = deploy_and_create(&mut journal);

        assert_eq!(journal.force_cold_spill_all(), 1);
        assert!(journal.instance(key).is_none());

        // CancelInstance targets a cold instance by key: ensure_resident_for_command
        // must page it back in before the engine applies the command
        let (events, _) = journal
            .apply_command(Command::cancel_instance(key))
            .unwrap();
        assert_eq!(journal.cold_count(), 0);
        assert!(events
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceTerminated { instance_key } if *instance_key == key)));
    }

    #[test]
    fn evicting_a_terminal_instance_drops_its_spilled_variable_row() {
        use nanobpmn_engine_core::Value;

        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        // Budget 0: any dormant, variable-carrying instance spills immediately.
        journal.set_spill(Arc::clone(&store), 0);

        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let mut vars = std::collections::HashMap::new();
        vars.insert("data".to_string(), Value::Str("payload".to_string()));

        // Two instances, both parked on the demo-work job with non-empty variables,
        // so maybe_spill sheds both their payloads to the store.
        let mut keys = Vec::new();
        for _ in 0..2 {
            let (events, _) = journal
                .apply_command(Command::create_instance_with("demo", vars.clone()))
                .unwrap();
            keys.push(events.iter().find_map(|e| e.instance_key()).unwrap());
        }
        let (a, b) = (keys[0], keys[1]);
        assert!(journal.engine.is_variables_spilled(a), "a spilled");
        assert!(journal.engine.is_variables_spilled(b), "b spilled");

        // Evicting the (now terminal) instance a must drop its spill row, while
        // leaving the still-live instance b's payload in the store.
        journal.evict_instances(&[a]);

        assert!(store.take(a).is_none(), "evicted instance's spill row is gone");
        assert!(
            store.take(b).is_some(),
            "a live instance's spill row is untouched"
        );
    }
}
