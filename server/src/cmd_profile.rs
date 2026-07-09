//! Per-command engine-actor profiling — the instrument for localizing the
//! create/complete congestion collapse.
//!
//! Under sustained load the single-writer engine actor's per-command service
//! time rises as the active-instance backlog grows (throughput inverts). All the
//! obvious O(active) *scans* on the actor are already indexed (tick pre-check,
//! `ActivateJobs`, `ExpireJobs`), yet a residual per-command cost remains. This
//! module attributes, for each applied [`Command`](nanobpmn_engine_core::Command),
//! two quantities on the engine thread:
//!
//! * **wall time** — how long the apply took, and
//! * **jemalloc thread-allocated bytes** — the delta of `thread.allocated`
//!   across the apply, i.e. exactly how many bytes this command allocated.
//!
//! Regressed against [`metrics::set_engine_cardinality`](crate::metrics::set_engine_cardinality)
//! these split the residual cleanly:
//!
//! * if **time/command** climbs with the active backlog while **alloc bytes/
//!   command** stays flat → the cost is hashmap-probe / cache-miss (bigger
//!   resident maps, no extra allocation);
//! * if **alloc bytes/command** climbs → the cost is allocator / copy.
//!
//! **Overhead when off:** one relaxed atomic-bool load per command and nothing
//! else. Gated on the `NANOBPM_CMD_PROFILE` environment variable, read once and
//! cached. The jemalloc `thread.allocatedp` pointer is `!Send`, so it is obtained
//! lazily on — and only ever used from — the engine thread via a thread-local.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

use tikv_jemalloc_ctl::thread::{allocatedp, ThreadLocal};

/// Tri-state cache of the `NANOBPM_CMD_PROFILE` gate: 0 = unresolved, 1 = on,
/// 2 = off. Resolved once on first use.
static ENABLED: AtomicU8 = AtomicU8::new(0);

/// Whether per-command profiling is enabled (env `NANOBPM_CMD_PROFILE`). The
/// result is cached after the first call so the steady-state cost is a single
/// relaxed load.
#[inline]
pub fn enabled() -> bool {
    match ENABLED.load(Ordering::Relaxed) {
        1 => true,
        2 => false,
        _ => {
            let on = std::env::var_os("NANOBPM_CMD_PROFILE").is_some();
            ENABLED.store(if on { 1 } else { 2 }, Ordering::Relaxed);
            on
        }
    }
}

thread_local! {
    /// The engine thread's cached jemalloc thread-allocated counter pointer.
    /// `ThreadLocal<u64>` is `!Send`; obtaining it here (inside the apply closure,
    /// which runs on the engine thread) binds it to the correct thread. `.get()`
    /// is a bare pointer read — no `mallctl` call, so per-command overhead is
    /// negligible.
    static ALLOCATED: RefCell<Option<ThreadLocal<u64>>> = const { RefCell::new(None) };
}

/// Reads this thread's cumulative jemalloc-allocated byte counter, caching the
/// pointer on first use. Returns `0` if jemalloc's thread stats are unavailable.
#[inline]
fn thread_allocated() -> u64 {
    ALLOCATED.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            *slot = allocatedp::read().ok();
        }
        slot.as_ref().map(|tl| tl.get()).unwrap_or(0)
    })
}

/// An in-flight per-command measurement. `None` when profiling is disabled, so
/// the caller pays nothing beyond the gate check.
pub struct CmdTimer {
    start: Instant,
    alloc0: u64,
}

/// Begins measuring a command apply, or returns `None` when profiling is off.
/// Call [`finish`] with the command's `kind` once the apply returns.
#[inline]
pub fn start() -> Option<CmdTimer> {
    if !enabled() {
        return None;
    }
    Some(CmdTimer {
        start: Instant::now(),
        alloc0: thread_allocated(),
    })
}

/// Records the elapsed time and allocated-byte delta for a finished command
/// apply under the given `kind` label. A no-op when `timer` is `None`.
#[inline]
pub fn finish(timer: Option<CmdTimer>, kind: &'static str) {
    if let Some(t) = timer {
        let bytes = thread_allocated().saturating_sub(t.alloc0);
        crate::metrics::record_command(kind, t.start.elapsed().as_secs_f64(), bytes);
    }
}
