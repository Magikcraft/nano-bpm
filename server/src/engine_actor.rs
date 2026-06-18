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

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use tokio::sync::oneshot;

use crate::backpressure::AdaptiveController;
use crate::journal::Journal;

/// A unit of work executed on the engine thread against the owned [`Journal`].
type Job = Box<dyn FnOnce(&mut Journal) + Send>;

/// Scheduling priority for a queued [`Job`].
///
/// The engine actor is a single writer shared by *creation* and *completion*:
/// `CreateInstance` and `CompleteJob`/`activateJobs`/etc all serialize on one
/// thread per partition. Under a flood of creates that share of the actor is
/// finite, so a high create rate can starve completion — the active-instance
/// backlog (created-but-not-completed) then runs away into congestion collapse.
/// This is acute under async durability, where the fsync latency that used to
/// implicitly rate-limit creation is gone.
///
/// Completion-priority breaks that: completion-side work (`High`) is always
/// dequeued ahead of creation (`Low`), so workers stay fed and the backlog drains
/// before any new instance is admitted. Creation thus self-throttles to the
/// spare actor capacity left after completion — exactly the admission control the
/// fsync brake used to provide, but without the throughput cost.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Priority {
    /// Completion-side and all reads: activate/complete/fail/correlate/queries.
    High,
    /// Instance creation. Yields to any pending completion work.
    Low,
}

/// The engine actor's priority mailbox: a two-tier intake drained High-before-Low
/// by the single engine thread. The mutex is held only for the O(1) push/pop of a
/// boxed closure (never across the engine work itself), so — unlike the old
/// `RwLock<Journal>` — it never parks a tokio worker on a contended lock.
struct Mailbox {
    inner: Mutex<MailboxInner>,
    signal: Condvar,
    /// Depth of the `Low` (creation) queue, mirrored as a lock-free atomic so the
    /// create-admission gate can read the create-queue backlog without taking the
    /// mailbox lock. Bumped on every `Low` push, dropped on every `Low` pop.
    lo_len: AtomicUsize,
}

struct MailboxInner {
    hi: VecDeque<Job>,
    lo: VecDeque<Job>,
    /// Live [`EngineHandle`] count. When it hits zero with both queues drained,
    /// the consumer loop exits (mirrors an mpsc channel disconnect).
    producers: usize,
}

impl Mailbox {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(MailboxInner {
                hi: VecDeque::new(),
                lo: VecDeque::new(),
                producers: 1,
            }),
            signal: Condvar::new(),
            lo_len: AtomicUsize::new(0),
        })
    }

    fn push(&self, priority: Priority, job: Job) {
        let mut g = self.inner.lock().expect("engine mailbox poisoned");
        match priority {
            Priority::High => g.hi.push_back(job),
            Priority::Low => {
                g.lo.push_back(job);
                self.lo_len.fetch_add(1, Ordering::Relaxed);
            }
        }
        drop(g);
        self.signal.notify_one();
    }

    /// Blocks until a job is available, returning High-priority work first.
    /// Returns `None` once every producer handle is gone and both queues are
    /// empty (clean shutdown).
    fn pop(&self) -> Option<Job> {
        let mut g = self.inner.lock().expect("engine mailbox poisoned");
        loop {
            if let Some(job) = g.hi.pop_front() {
                return Some(job);
            }
            if let Some(job) = g.lo.pop_front() {
                self.lo_len.fetch_sub(1, Ordering::Relaxed);
                return Some(job);
            }
            if g.producers == 0 {
                return None;
            }
            g = self.signal.wait(g).expect("engine mailbox poisoned");
        }
    }
}

/// A cloneable handle to the engine actor. All engine mutation and engine-state
/// reads go through here; the [`Journal`] lives on a single dedicated thread, so
/// no async task ever blocks a tokio worker on a contended lock.
pub struct EngineHandle {
    mb: Arc<Mailbox>,
}

impl Clone for EngineHandle {
    fn clone(&self) -> Self {
        self.mb
            .inner
            .lock()
            .expect("engine mailbox poisoned")
            .producers += 1;
        Self {
            mb: Arc::clone(&self.mb),
        }
    }
}

impl Drop for EngineHandle {
    fn drop(&mut self) {
        let mut g = self.mb.inner.lock().expect("engine mailbox poisoned");
        g.producers -= 1;
        if g.producers == 0 {
            drop(g);
            // Wake the consumer so it observes shutdown and drops the Journal
            // (flushing its writer).
            self.mb.signal.notify_all();
        }
    }
}

impl EngineHandle {
    /// Spawns the engine thread that owns `journal` and returns a handle to it.
    /// The thread runs until every [`EngineHandle`] clone is dropped (the channel
    /// closes), then drops the [`Journal`] — flushing its writer thread. The
    /// thread is detached: durability never depends on a clean shutdown because
    /// callers `await` each command's [`Commit`](crate::journal::Commit) before
    /// acknowledging, so an acknowledged write is already fsynced even on a hard
    /// kill.
    ///
    /// When `controller` is `Some`, the loop times every command and feeds the
    /// latency to the adaptive backpressure limiter (which sizes the in-flight
    /// watermark from observed per-command latency).
    pub fn spawn(mut journal: Journal, controller: Option<AdaptiveController>) -> Self {
        let mb = Mailbox::new();
        let consumer = Arc::clone(&mb);
        let profile = std::env::var_os("NANOBPM_ACTOR_PROFILE").is_some();
        thread::Builder::new()
            .name("nanobpmn-engine".into())
            .spawn(move || {
                if profile || controller.is_some() {
                    Self::run_instrumented(&consumer, &mut journal, profile, controller);
                } else {
                    while let Some(job) = consumer.pop() {
                        job(&mut journal);
                    }
                }
            })
            .expect("spawn engine thread");
        Self { mb }
    }

    /// Command loop with per-command timing. Drives two optional consumers of the
    /// latency signal: the adaptive backpressure `controller` (per command) and,
    /// when `profile` is set, utilization logging (per 5 s window). Timing each
    /// `recv` (idle, queue empty = actor starved) and each job (busy) lets us tell
    /// whether the single writer is the bottleneck (busy≈100%) or whether
    /// throughput is limited upstream of it (busy≪100% = latency/client bound).
    fn run_instrumented(
        mb: &Mailbox,
        journal: &mut Journal,
        profile: bool,
        mut controller: Option<AdaptiveController>,
    ) {
        use std::time::{Duration, Instant};
        const WINDOW: Duration = Duration::from_secs(5);

        let mut busy = Duration::ZERO;
        let mut idle = Duration::ZERO;
        let mut jobs: u64 = 0;
        let mut window_start = Instant::now();

        loop {
            let before_recv = Instant::now();
            let Some(job) = mb.pop() else { break };
            idle += before_recv.elapsed();

            let before_job = Instant::now();
            job(journal);
            let job_time = before_job.elapsed();

            // Feed the adaptive limiter every command; it windows internally.
            if let Some(c) = controller.as_mut() {
                c.record(job_time);
            }

            if !profile {
                continue;
            }
            busy += job_time;
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

    /// Runs `f` on the engine thread at [`Priority::High`] and awaits its result.
    /// This is the default for every completion-side and read command; only
    /// instance creation uses [`with_low`](EngineHandle::with_low). The closure
    /// receives exclusive `&mut Journal`. Keep `f` cheap: do serde-heavy request
    /// decode and response encode on the caller side (before/after the call) so
    /// they run in parallel off the single engine thread.
    pub async fn with<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut Journal) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.with_priority(Priority::High, f).await
    }

    /// Like [`with`](EngineHandle::with) but at [`Priority::Low`] — the engine
    /// services it only when no completion-side work is queued. Used by the
    /// instance-create path so a flood of creates cannot starve completion and
    /// run the active-instance backlog away into congestion collapse.
    pub async fn with_low<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut Journal) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.with_priority(Priority::Low, f).await
    }

    /// Runs `f` on the engine thread at the given [`Priority`] and awaits its
    /// result.
    pub async fn with_priority<R, F>(&self, priority: Priority, f: F) -> R
    where
        F: FnOnce(&mut Journal) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        let job: Job = Box::new(move |journal| {
            // A dropped receiver (caller cancelled) is fine; the command still ran.
            let _ = reply_tx.send(f(journal));
        });
        self.mb.push(priority, job);
        reply_rx.await.expect("engine thread answered")
    }

    /// Enqueues `f` to run on the engine thread (at [`Priority::High`]) without
    /// awaiting a result. Used by background maintenance (e.g. the read-model
    /// exporter's hot-state eviction) that has no reply to deliver and must not
    /// block; eviction drains the backlog, so it belongs on the completion tier.
    pub fn spawn_job<F>(&self, f: F)
    where
        F: FnOnce(&mut Journal) + Send + 'static,
    {
        self.mb.push(Priority::High, Box::new(f));
    }

    /// Current depth of this partition's `Low` (creation) queue: creates that have
    /// been submitted but not yet applied because the engine is busy with
    /// completion-side work. The create-admission gate reads this to bound how
    /// long a create can wait (hence create-side latency) under overload. A relaxed
    /// load — an approximate bound is sufficient.
    pub fn pending_low(&self) -> usize {
        self.mb.lo_len.load(Ordering::Relaxed)
    }
}
