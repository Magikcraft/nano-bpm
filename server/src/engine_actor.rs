//! A single-writer command actor that owns the durable [`Journal`].
//!
//! The engine is a single writer, so every mutation must be serialized. The
//! previous design wrapped the [`Journal`] in a `std::sync::RwLock` shared across
//! all request tasks. That is an anti-pattern under an async runtime: a blocking
//! `std` lock contended by many tasks *parks the tokio worker thread* holding it
//! instead of yielding, so a burst of concurrent commands stalls the whole
//! runtime — measured here as a 240× collapse in per-request latency from only a
//! handful of concurrent producers.
//!
//! Instead, the [`Journal`] lives on one dedicated thread and commands reach it
//! through an `mpsc` channel; callers `await` a one-shot reply. This mirrors
//! Zeebe's per-partition `StreamProcessor` actor: a single owner, a serial
//! command queue, and tokio workers that only ever park on an async `await`
//! (never on a blocking lock). Serde-heavy request decode / response encode is
//! done by the caller *around* [`EngineHandle::with`], so the 50 KB variable
//! payloads are converted in parallel across cores rather than serially on the
//! engine thread.

use std::sync::mpsc::{self, Sender};
use std::thread;

use tokio::sync::oneshot;

use crate::journal::Journal;

/// A unit of work executed on the engine thread against the owned [`Journal`].
type Job = Box<dyn FnOnce(&mut Journal) + Send>;

/// A cloneable handle to the engine actor. All engine mutation and engine-state
/// reads go through here; the [`Journal`] lives on a single dedicated thread, so
/// no async task ever blocks a tokio worker on a contended lock.
#[derive(Clone)]
pub struct EngineHandle {
    tx: Sender<Job>,
}

impl EngineHandle {
    /// Spawns the engine thread that owns `journal` and returns a handle to it.
    /// The thread runs until every [`EngineHandle`] clone is dropped (the channel
    /// closes), then drops the [`Journal`] — flushing its writer thread. The
    /// thread is detached: durability never depends on a clean shutdown because
    /// callers `await` each command's [`Commit`](crate::journal::Commit) before
    /// acknowledging, so an acknowledged write is already fsynced even on a hard
    /// kill.
    pub fn spawn(mut journal: Journal) -> Self {
        let (tx, rx) = mpsc::channel::<Job>();
        let profile = std::env::var_os("NANOBPM_ACTOR_PROFILE").is_some();
        thread::Builder::new()
            .name("nanobpmn-engine".into())
            .spawn(move || {
                if profile {
                    Self::run_profiled(rx, &mut journal);
                } else {
                    while let Ok(job) = rx.recv() {
                        job(&mut journal);
                    }
                }
            })
            .expect("spawn engine thread");
        Self { tx }
    }

    /// Command loop with utilization profiling. Times each `recv` (idle, queue
    /// empty = actor starved) and each job execution (busy). Every reporting
    /// window it logs jobs/s and the busy fraction, so we can tell whether the
    /// single writer is the bottleneck (busy≈100%) or whether throughput is
    /// limited upstream of it (busy≪100% = latency/contention/client bound).
    fn run_profiled(rx: mpsc::Receiver<Job>, journal: &mut Journal) {
        use std::time::{Duration, Instant};
        const WINDOW: Duration = Duration::from_secs(5);

        let mut busy = Duration::ZERO;
        let mut idle = Duration::ZERO;
        let mut jobs: u64 = 0;
        let mut window_start = Instant::now();

        loop {
            let before_recv = Instant::now();
            let Ok(job) = rx.recv() else { break };
            idle += before_recv.elapsed();

            let before_job = Instant::now();
            job(journal);
            busy += before_job.elapsed();
            jobs += 1;

            let elapsed = window_start.elapsed();
            if elapsed >= WINDOW {
                let total = busy + idle;
                let busy_pct = if total.is_zero() {
                    0.0
                } else {
                    busy.as_secs_f64() / total.as_secs_f64() * 100.0
                };
                let jps = jobs as f64 / elapsed.as_secs_f64();
                let avg_us = if jobs > 0 {
                    busy.as_micros() as f64 / jobs as f64
                } else {
                    0.0
                };
                tracing::info!(
                    "actor: {jps:.0} jobs/s, busy {busy_pct:.1}%, avg {avg_us:.1}us/job ({jobs} jobs/window)"
                );
                busy = Duration::ZERO;
                idle = Duration::ZERO;
                jobs = 0;
                window_start = Instant::now();
            }
        }
    }

    /// Runs `f` on the engine thread and awaits its result. The closure receives
    /// exclusive `&mut Journal`. Keep `f` cheap: do serde-heavy request decode and
    /// response encode on the caller side (before/after the call) so they run in
    /// parallel off the single engine thread.
    pub async fn with<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut Journal) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        let job: Job = Box::new(move |journal| {
            // A dropped receiver (caller cancelled) is fine; the command still ran.
            let _ = reply_tx.send(f(journal));
        });
        self.tx
            .send(job)
            .expect("engine thread is alive while a handle exists");
        reply_rx.await.expect("engine thread answered")
    }

    /// Enqueues `f` to run on the engine thread without awaiting a result. Used by
    /// background maintenance (e.g. the read-model exporter's hot-state eviction)
    /// that has no reply to deliver and must not block.
    pub fn spawn_job<F>(&self, f: F)
    where
        F: FnOnce(&mut Journal) + Send + 'static,
    {
        // The engine thread may already be gone during shutdown; dropping the job
        // is harmless (nothing awaits it).
        let _ = self.tx.send(Box::new(f));
    }
}
