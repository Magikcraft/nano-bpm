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

    /// Wires the read-model exporter channel. Set before any command is applied
    /// (including demo seeding) so every journaled event is projected.
    pub fn set_exporter(&mut self, exporter: Sender<Arc<Vec<Event>>>) {
        self.exporter = Some(exporter);
    }

    /// Evicts a batch of completed instances in a single pass over hot state.
    /// Mirrors [`Engine::evict_instances`]. Used on the steady-state exporter
    /// path once the read model has the completions.
    pub fn evict_instances(&mut self, keys: &[Key]) -> usize {
        self.engine.evict_instances(keys)
    }

    /// Evicts every completed instance from hot state (used after a boot replay,
    /// once the read model is caught up). Mirrors [`Engine::evict_completed`].
    pub fn evict_completed(&mut self) -> usize {
        self.engine.evict_completed()
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
        let mut activated = self
            .engine
            .activate_jobs(job_type, worker, max_jobs, timeout, now);
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
        let events = Arc::new(self.engine.trigger_timers(now));
        let commit = self.persist(&events);
        (events, commit)
    }

    /// Releases expired activation locks at logical instant `now`, **without**
    /// journaling: like activation, lock expiry is volatile lease state. Mirrors
    /// [`Engine::expire_jobs`].
    pub fn expire_jobs(&mut self, now: u64) {
        self.engine.expire_jobs(now);
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
}
