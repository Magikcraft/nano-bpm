//! Backpressure for `createProcessInstance`: shed load with `503
//! RESOURCE_EXHAUSTED` when the engine is saturated, so a producer that outpaces
//! the workers converges to the drain rate instead of growing an unbounded
//! create backlog (which inflates hot state and collapses per-command latency).
//!
//! Three modes, selected by `NANOBPMN_BACKPRESSURE_MAX_INFLIGHT`:
//! - **Adaptive** (default; unset / `auto` / `adaptive`): an AIMD concurrency
//!   limiter whose watermark self-tunes from the engine's measured per-command
//!   latency — faithful to Camunda/Zeebe's default request limiter, but the
//!   congestion threshold is derived from a *measured* baseline latency rather
//!   than a hard-coded timeout, so it adapts to the host with no magic constant.
//! - **Fixed(n)** (a positive integer): a static in-flight-instance watermark.
//! - **Disabled** (`0` / `off` / `none` / `false` / `disabled`): never shed.
//!
//! In every mode the quantity compared against the limit is the count of
//! in-flight (Active, non-terminal) process instances, tracked by the read-model
//! exporter in a lock-free [`AtomicUsize`]. Adaptive mode does not change *what*
//! is limited (instances) — it makes the *limit itself* track latency.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Live-published internal state of the [`BacklogGovernor`], so the ~1 Hz monitor
/// can surface *why* the governor is holding its cap where it is (and a shed
/// message can explain it). The governor owns the write side (updated each monitor
/// tick); the server keeps a clone of the read side. Latencies are whole
/// microseconds. All zero until the first window folds.
#[derive(Clone, Default)]
pub struct GovernorObs {
    /// Lowest end-to-end job sojourn (µs) seen so far — an informational
    /// "light-load" reference (the governor targets an absolute SLO, not a ratio
    /// to this, so it is purely observational).
    pub baseline_us: Arc<AtomicU64>,
    /// The most recent window's mean end-to-end job sojourn (µs) — the value
    /// compared against the target. Above it, the governor backed the cap off.
    pub window_avg_us: Arc<AtomicU64>,
    /// The absolute end-to-end job-sojourn SLO (µs) the governor holds the backlog
    /// cap to (by Little's law, backlog ≈ target · drain-rate).
    pub target_us: Arc<AtomicU64>,
}

/// Parsed configuration for the backpressure subsystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureSetting {
    /// AIMD limiter that sizes the in-flight watermark from measured latency.
    Adaptive,
    /// Static in-flight-instance watermark of `n`.
    Fixed(usize),
    /// Load shedding off; creates are never rejected.
    Disabled,
}

/// Pure resolver for `NANOBPMN_BACKPRESSURE_MAX_INFLIGHT` (split out so it is
/// unit-testable without the process environment). Backpressure is **on by
/// default**, and **adaptive** by default:
/// - `None` (unset), `auto`, `adaptive` → [`BackpressureSetting::Adaptive`];
/// - `0`, `off`, `none`, `false`, `disabled` (any case) → [`BackpressureSetting::Disabled`];
/// - a positive integer → [`BackpressureSetting::Fixed`];
/// - anything else unparseable → `Adaptive` (fail safe: never silently disable).
pub fn parse_backpressure_setting(raw: Option<&str>) -> BackpressureSetting {
    let Some(raw) = raw else {
        return BackpressureSetting::Adaptive;
    };
    let v = raw.trim().to_ascii_lowercase();
    match v.as_str() {
        "" | "auto" | "adaptive" => BackpressureSetting::Adaptive,
        "0" | "off" | "none" | "false" | "disabled" => BackpressureSetting::Disabled,
        _ => match v.parse::<usize>() {
            Ok(n) if n > 0 => BackpressureSetting::Fixed(n),
            _ => BackpressureSetting::Adaptive,
        },
    }
}

/// Which service-level objective the operator wants the engine to honour when it
/// reaches its saturation ceiling (memory / processing capacity). This selects
/// the *behavioural characteristic* at the edge of the performance envelope; it
/// does **not** relax the memory-safety rails (create-queue, in-flight payload,
/// resident-memory watermarks) that exist to keep the node from OOMing — those
/// are survival guards active in every mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SlaMode {
    /// **Preserve end-to-end latency** (default). At the ceiling, shed new
    /// `createProcessInstance` calls (`503 RESOURCE_EXHAUSTED`) so accepted
    /// instances keep completing fast. This is the "time-to-complete SLA": a
    /// client that outpaces the drain rate is told to back off rather than
    /// letting its already-running instances slow down. Enforced by the AIMD
    /// concurrency limiter *and* the proactive active-backlog governor.
    Latency,
    /// **Preserve admission** (accept latency). At the ceiling, drop the proactive
    /// active-backlog governor and run the engine at its true drain ceiling
    /// (measured ~+48% throughput vs `Latency`), letting end-to-end latency and the
    /// backlog grow with demand. This is the "start-every-process SLA": only the
    /// proactive **active-backlog governor** is suppressed. The **AIMD concurrency
    /// limiter stays armed** (see [`Self::sheds_for_latency`]) as an engine-overload
    /// guard — arming it measurably tightens the tail (~40% lower p90) at no
    /// throughput cost — but note it does **not** bound the accumulated backlog: it
    /// sheds on create-*processing* concurrency, which stays low even while the
    /// completion side falls behind, so under sustained overload the backlog grows
    /// until the **memory-safety rails** shed (they, not AIMD, are the backstop in
    /// this mode). Only `Latency`'s backlog governor gives a tight, engine-enforced
    /// bound. See ADR 0013 Addendum / `PERFORMANCE.md` 2026-07-10. Terminal state
    /// frees on completion (ADR 0012) and live variables spill to disk, so the large
    /// backlog is comparatively cheap to hold up to the rail.
    Admission,
}

impl SlaMode {
    /// Whether the **proactive active-backlog governor** should reject admission
    /// to hold a latency target. `true` in [`SlaMode::Latency`], `false` in
    /// [`SlaMode::Admission`] (which drops the proactive governor to admit more).
    /// Note this gates **only** the backlog governor: the AIMD concurrency limiter
    /// (an engine-overload guard) and the memory-safety rails stay armed in both
    /// modes. AIMD is not a backlog bound — under sustained overload only the
    /// governor (i.e. `Latency` mode) keeps the backlog tightly bounded; in
    /// `Admission` the backlog grows to the memory-safety rails.
    pub fn sheds_for_latency(&self) -> bool {
        matches!(self, SlaMode::Latency)
    }

    /// Stable machine identifier (`latency` | `admission`) for APIs/UI.
    pub fn as_str(&self) -> &'static str {
        match self {
            SlaMode::Latency => "latency",
            SlaMode::Admission => "admission",
        }
    }

    /// Human-readable description for the startup log.
    pub fn describe(&self) -> &'static str {
        match self {
            SlaMode::Latency => {
                "latency (preserve end-to-end latency; shed admission at the ceiling)"
            }
            SlaMode::Admission => {
                "admission (preserve admission; accept higher latency at the ceiling)"
            }
        }
    }

    /// Reconstruct from the atomic byte used by [`SharedSlaMode`]. Fails safe to
    /// [`SlaMode::Latency`] for any unexpected value (never silently drop latency
    /// protection).
    fn from_u8(v: u8) -> Self {
        match v {
            1 => SlaMode::Admission,
            _ => SlaMode::Latency,
        }
    }
}

/// A runtime-switchable SLA mode: a cheap, cloneable handle over an atomic so an
/// operator toggle (e.g. the console SLA knob) is visible to every in-flight
/// request handler **without a restart**. Mirrors the [`Backpressure::Adaptive`]
/// watermark, which is likewise an `Arc<Atomic…>` written by one place and read
/// lock-free on the hot path. The stored value is [`SlaMode`] encoded via
/// `#[repr(u8)]`.
#[derive(Clone)]
pub struct SharedSlaMode(Arc<AtomicU8>);

impl SharedSlaMode {
    /// Seed with the startup mode (resolved from `NANOBPMN_SLA_MODE`).
    pub fn new(mode: SlaMode) -> Self {
        Self(Arc::new(AtomicU8::new(mode as u8)))
    }

    /// The current mode. A relaxed atomic load — no coordination, safe on the
    /// admission hot path.
    pub fn get(&self) -> SlaMode {
        SlaMode::from_u8(self.0.load(Ordering::Relaxed))
    }

    /// Switch the mode at runtime. Takes effect on the next admission decision;
    /// a small race against concurrent creates is irrelevant for an approximate
    /// gate.
    pub fn set(&self, mode: SlaMode) {
        self.0.store(mode as u8, Ordering::Relaxed);
    }
}

/// Pure resolver for `NANOBPMN_SLA_MODE` (split out so it is unit-testable without
/// the process environment). Defaults to [`SlaMode::Latency`] — the historical
/// behaviour — and fails safe to it for any unrecognised value (never silently
/// drop latency protection):
/// - `None` (unset), `latency`, `preserve-latency`, `time-to-complete` → `Latency`;
/// - `admission`, `preserve-admission`, `accept-latency`, `start-every-process`,
///   `throughput` → `Admission`;
/// - anything else → `Latency`.
pub fn parse_sla_mode(raw: Option<&str>) -> SlaMode {
    let Some(raw) = raw else {
        return SlaMode::Latency;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "latency" | "preserve-latency" | "time-to-complete" | "complete" => SlaMode::Latency,
        "admission"
        | "preserve-admission"
        | "accept-latency"
        | "start-every-process"
        | "start"
        | "throughput" => SlaMode::Admission,
        _ => SlaMode::Latency,
    }
}

/// Runtime backpressure handle held by the server and queried by the create
/// handler. Cheap to clone; the adaptive limit lives behind an `Arc<AtomicUsize>`
/// the engine thread writes and request handlers read.
#[derive(Clone)]
pub enum Backpressure {
    Disabled,
    Fixed(usize),
    /// The current watermark, maintained by the engine thread's [`AdaptiveController`].
    Adaptive(Arc<AtomicUsize>),
}

impl Backpressure {
    /// The watermark to compare the in-flight gauge against, or `None` when load
    /// shedding is disabled (the create handler then never rejects).
    pub fn current_limit(&self) -> Option<usize> {
        match self {
            Backpressure::Disabled => None,
            Backpressure::Fixed(n) => Some(*n),
            Backpressure::Adaptive(limit) => Some(limit.load(Ordering::Relaxed)),
        }
    }

    /// Whether a create carrying the engine's current request-processing
    /// concurrency (`in_flight`) should be shed. `false` whenever load shedding is
    /// disabled. The comparison is `>=` so a watermark of `n` admits at most `n`
    /// concurrent creates.
    pub fn should_shed(&self, in_flight: usize) -> bool {
        self.current_limit().is_some_and(|limit| in_flight >= limit)
    }

    /// Human-readable description for the startup log.
    pub fn describe(&self) -> String {
        match self {
            Backpressure::Disabled => "disabled".to_string(),
            Backpressure::Fixed(n) => {
                format!("fixed watermark of {n} concurrent creates in processing")
            }
            Backpressure::Adaptive(limit) => format!(
                "adaptive (AIMD, latency-driven), starting watermark {}",
                limit.load(Ordering::Relaxed)
            ),
        }
    }
}

// --- AIMD controller tuning ------------------------------------------------

/// Lowest watermark the adaptive limiter will shrink to. Floors load shedding so
/// a latency spike can't starve the workers; the empirical create-flood knee
/// (~250 in-flight saturates 10 workers) sits right here.
const MIN_LIMIT: usize = 256;
/// Highest watermark the limiter will grow to — a hard memory rail (~1 GB at the
/// 50 KB worst-case payload; negligible for typical small payloads). The
/// controller normally settles well below this once latency rises.
const MAX_LIMIT: usize = 20_000;
/// Starting watermark before any latency has been observed.
const INITIAL_LIMIT: usize = MIN_LIMIT;
/// A window's average latency above `baseline * CONGESTION_RATIO` is treated as
/// queueing (congestion) and triggers a multiplicative decrease.
pub const CONGESTION_RATIO: f64 = 2.0;
/// Multiplicative-decrease factor applied on congestion (AIMD's "MD").
const BACKOFF: f64 = 0.9;
/// Per-window upward drift of the latency baseline when no faster sample is seen,
/// so a permanently raised floor (e.g. larger payloads) doesn't over-throttle.
const BASELINE_CREEP: f64 = 0.05;
/// Minimum samples before a window is evaluated (avoids reacting to noise).
const WINDOW_MIN_SAMPLES: u64 = 64;
/// Minimum wall time before a window is evaluated.
const WINDOW_MIN_INTERVAL: Duration = Duration::from_millis(20);

/// AIMD limit calculator with a self-calibrated latency baseline. Pure and
/// `Instant`-free so its dynamics are unit-testable; the windowing/IO lives in
/// [`AdaptiveController`].
#[derive(Debug, Clone)]
pub struct AimdLimit {
    limit: f64,
    min_limit: usize,
    max_limit: usize,
    baseline_us: f64,
    /// Slow-start probes the ceiling fast (multiplicative growth) until the first
    /// congestion signal, then switches to additive-increase probing.
    slow_start: bool,
}

impl AimdLimit {
    pub fn new(initial: usize, min_limit: usize, max_limit: usize) -> Self {
        Self {
            limit: initial.clamp(min_limit, max_limit) as f64,
            min_limit,
            max_limit,
            baseline_us: 0.0,
            slow_start: true,
        }
    }

    pub fn limit(&self) -> usize {
        self.limit as usize
    }

    /// The current latency reference (µs) the congestion threshold is derived
    /// from: the self-calibrated baseline in ratio mode, or the fixed target in
    /// The current self-calibrated latency reference (µs) the congestion threshold
    /// is derived from (`baseline × `[`CONGESTION_RATIO`]).
    pub fn baseline_us(&self) -> f64 {
        self.baseline_us
    }

    /// The latency (µs) at/above which a window is treated as congested:
    /// `baseline × `[`CONGESTION_RATIO`].
    pub fn congestion_threshold_us(&self) -> f64 {
        self.baseline_us * CONGESTION_RATIO
    }

    /// Fold one window of stats into the limit and return the new value.
    /// `avg_us` is the window's mean per-command latency; `inflight` is the
    /// current in-flight count (the limit only grows while it is being used, so
    /// an idle engine doesn't inflate the watermark).
    ///
    /// Congestion is judged against a *baseline of the lowest average seen* ×
    /// [`CONGESTION_RATIO`] — like-for-like, since the command mix (cheap
    /// completes vs heavy 50 KB creates) makes a single-command minimum a poor
    /// reference. When the average inflates past the threshold the engine is
    /// queueing / under heap pressure, so the limit is cut.
    pub fn on_window(&mut self, avg_us: f64, inflight: usize) -> usize {
        let loaded = (inflight as f64) * 2.0 >= self.limit;
        // Establish the baseline on the first ever window.
        if self.baseline_us == 0.0 {
            self.baseline_us = avg_us;
        }
        let congested = avg_us > self.congestion_threshold_us();

        if congested {
            // Congested: multiplicative decrease, and leave slow-start for good.
            // The baseline is *frozen* here — never adapt it toward a congested
            // latency, or sustained queueing would be accepted as the new normal
            // and shedding would stop (the exact unbounded-backlog pathology we
            // exist to prevent).
            self.slow_start = false;
            self.limit = (self.limit * BACKOFF).max(self.min_limit as f64);
        } else {
            // Healthy window. Refine the baseline toward the uncongested floor
            // (snap down to a new minimum, else creep up slowly so a genuinely
            // slower host isn't throttled forever). Then probe the limit upward
            // while it is used.
            if avg_us < self.baseline_us {
                self.baseline_us = avg_us;
            } else {
                self.baseline_us += (avg_us - self.baseline_us) * BASELINE_CREEP;
            }
            if loaded {
                if self.slow_start {
                    self.limit = (self.limit * 2.0).min(self.max_limit as f64);
                } else {
                    self.limit = (self.limit + 1.0).min(self.max_limit as f64);
                }
            }
        }
        self.limit()
    }
}

/// One AIMD limiter publishing to a shared atomic, stepped once per latency
/// window by the [`AdaptiveController`]. `signal` is the live quantity whose
/// pressure gates *growth* (the limit only grows while it is being approached, so
/// an idle engine doesn't inflate the watermark); `shared` is where the resolved
/// limit is published for the hot path to read.
struct LatencyLimiter {
    aimd: AimdLimit,
    shared: Arc<AtomicUsize>,
    signal: Arc<AtomicUsize>,
    /// Stable tag for the verbose convergence log (`adaptive`, `worker-governor`).
    label: &'static str,
}

impl LatencyLimiter {
    /// Fold one window average into the limit and publish it.
    fn step(&mut self, avg_us: f64, verbose: bool) {
        let signal = self.signal.load(Ordering::Relaxed);
        let prev = self.aimd.limit();
        let new_limit = self.aimd.on_window(avg_us, signal);
        self.shared.store(new_limit, Ordering::Relaxed);
        if verbose && new_limit != prev {
            tracing::info!(
                "backpressure({}): limit {prev} -> {new_limit} (avg {avg_us:.0}us, \
                 baseline {:.0}us, signal {signal})",
                self.label,
                self.aimd.baseline_us(),
            );
        }
    }
}

/// One command-class latency window (creates vs completion-side). Accumulates
/// per-command latencies of a single class and yields the window average once it
/// has enough samples and has run long enough — keeping each class's baseline
/// like-for-like. Windowing per class (rather than one mixed window) is what
/// stops a cheap-completion window from pinning the create-side baseline near
/// zero, which made `threshold = baseline * CONGESTION_RATIO` fire on every
/// normal window and pinned the governor at its floor.
struct ClassWindow {
    count: u64,
    sum_us: f64,
    start: Instant,
}

impl ClassWindow {
    fn new() -> Self {
        Self {
            count: 0,
            sum_us: 0.0,
            start: Instant::now(),
        }
    }

    /// Fold one same-class sample in; return the window average (and reset) once
    /// the window has enough samples and has run long enough, else `None`.
    fn offer(&mut self, us: f64) -> Option<f64> {
        self.count += 1;
        self.sum_us += us;
        if self.count >= WINDOW_MIN_SAMPLES && self.start.elapsed() >= WINDOW_MIN_INTERVAL {
            let avg = self.sum_us / self.count as f64;
            self.count = 0;
            self.sum_us = 0.0;
            self.start = Instant::now();
            Some(avg)
        } else {
            None
        }
    }
}

/// Engine-thread side of the adaptive limiters: accumulates per-command latency
/// into **per-class** windows and, once a class window has enough samples and has
/// run long enough, steps the limiter that keys off that class from its window
/// average. Owned by the single (partition-0) engine thread, so its window state
/// needs no synchronization. The command classes are creates (`Priority::Low`)
/// and completion-side/reads (`Priority::High`); splitting the window keeps each
/// class's latency baseline like-for-like:
/// - the **create limiter** (present in adaptive backpressure mode) sizes the
///   create-processing concurrency watermark, gated on the `processing` gauge,
///   and steps off the **create** window;
/// - the **worker governor** steps off the **completion** window — the drain-side
///   knee.
///
/// The admission-backlog cap is *not* driven here: it is owned by the standalone
/// monitor-stepped [`BacklogGovernor`], which keys off end-to-end job sojourn
/// (not per-command apply latency) so its setpoint reflects backlog depth.
pub struct AdaptiveController {
    create: Option<LatencyLimiter>,
    workers: Option<LatencyLimiter>,
    /// Create-class (`Priority::Low`) latency window — drives the create limiter.
    lo: ClassWindow,
    /// Completion-class (`Priority::High`) latency window — drives the workers.
    hi: ClassWindow,
    /// Log every limit change at INFO (gated by `NANOBPM_ACTOR_PROFILE`) so the
    /// limiters' convergence is observable during tuning.
    verbose: bool,
}

impl Default for AdaptiveController {
    fn default() -> Self {
        Self::new()
    }
}

impl AdaptiveController {
    /// An empty controller with no limiters installed. Install limiters with
    /// [`with_create_limiter`](Self::with_create_limiter) and/or
    /// [`with_worker_governor`](Self::with_worker_governor); check
    /// [`is_active`](Self::is_active) to decide whether it needs driving at all.
    pub fn new() -> Self {
        Self {
            create: None,
            workers: None,
            lo: ClassWindow::new(),
            hi: ClassWindow::new(),
            verbose: std::env::var_os("NANOBPM_ACTOR_PROFILE").is_some(),
        }
    }

    /// Install the create-processing concurrency limiter (the historical adaptive
    /// backpressure watermark). Growth is gated on the `inflight` processing
    /// gauge. Returns the shared limit handle to install in
    /// [`Backpressure::Adaptive`].
    pub fn with_create_limiter(&mut self, inflight: Arc<AtomicUsize>) -> Arc<AtomicUsize> {
        let shared = Arc::new(AtomicUsize::new(INITIAL_LIMIT));
        self.create = Some(LatencyLimiter {
            aimd: AimdLimit::new(INITIAL_LIMIT, MIN_LIMIT, MAX_LIMIT),
            shared: shared.clone(),
            signal: inflight,
            label: "adaptive",
        });
        shared
    }

    /// Install the self-optimizing worker-concurrency governor: it tunes the
    /// number of subscribers the push dispatcher fans a job type out to per pass
    /// between `floor` and `ceiling`, from the same per-command latency signal,
    /// gated on the current `backlog` (runnable task-jobs waiting to drain). The
    /// push dispatcher is the throughput ceiling — activation is High-priority in
    /// the engine mailbox, so fanning a fixed job supply across too many
    /// subscribers swamps completions and inflates per-command latency. Starting
    /// at `floor`, the governor slow-starts the active-subscriber width upward
    /// while there is backlog to drain and latency is healthy, and backs off
    /// multiplicatively the moment latency inflates past its self-calibrated
    /// baseline — converging on the worker concurrency that maximizes completion
    /// throughput. Excess subscribers are parked (rotated round-robin, never
    /// starved). Returns the shared width handle the dispatcher reads.
    pub fn with_worker_governor(
        &mut self,
        floor: usize,
        ceiling: usize,
        backlog: Arc<AtomicUsize>,
    ) -> Arc<AtomicUsize> {
        let shared = Arc::new(AtomicUsize::new(floor));
        self.workers = Some(LatencyLimiter {
            aimd: AimdLimit::new(floor, floor, ceiling),
            shared: shared.clone(),
            signal: backlog,
            label: "worker-governor",
        });
        shared
    }
    pub fn is_active(&self) -> bool {
        self.create.is_some() || self.workers.is_some()
    }

    /// Record one command's processing latency, tagged with its class
    /// (`is_create` = the create-side `Priority::Low` queue). Folds the sample
    /// into that class's window and steps the limiter keyed off it once the
    /// window is ready: the create limiter off the **create** window, the worker
    /// governor off the **completion** window. Splitting the windows keeps each
    /// limiter's AIMD baseline like-for-like.
    pub fn record(&mut self, latency: Duration, is_create: bool) {
        let us = latency.as_micros() as f64;
        if is_create {
            if let Some(avg) = self.lo.offer(us)
                && let Some(c) = self.create.as_mut()
            {
                c.step(avg, self.verbose);
            }
            return;
        }
        if let Some(avg) = self.hi.offer(us)
            && let Some(w) = self.workers.as_mut()
        {
            w.step(avg, self.verbose);
        }
    }
}

/// The standalone end-to-end **job-sojourn** admission-backlog governor, stepped
/// once per ~1 Hz monitor tick (not on the engine actor loop). It holds an
/// absolute end-to-end job *sojourn* SLO — the wall-clock latency from a job's
/// creation to its completion — as the admission-backlog setpoint.
///
/// By Little's law `L = λ·W`, bounding sojourn `W` at a target is equivalent to
/// bounding the runnable job backlog `L` at `target · λ`, auto-calibrated to the
/// live drain rate `λ`. This is the semantically correct signal for latency-SLA
/// mode: unlike create→accept latency (which stays cheap — a create is accepted in
/// ~1-2 ms — even when a deep worker-starved backlog has built up), sojourn grows
/// directly with backlog depth, so the governor's setpoint *becomes* the SLA.
///
/// The control law is a **multiplicative servo with a dead-band**, not AIMD's
/// additive increase, because the cap ranges over tens of thousands yet is stepped
/// only ~1×/s: additive `+1`/step could never track a cap of that magnitude. Each
/// tick, given the windowed mean sojourn and the current runnable backlog:
/// - **over the SLO** (sojourn > target·(1+deadband)): back off multiplicatively
///   toward the floor;
/// - **under the SLO and loaded** (backlog occupies ≥ [`BACKLOG_GOV_LOAD_FRAC`] of
///   the cap, so an idle cap is never inflated): grow multiplicatively toward the
///   ceiling;
/// - **otherwise**: hold (within the dead-band, or under-SLO but not loaded — there
///   is nothing to gain by growing an unused cap).
///
/// It converges to the cap where sojourn ≈ target (i.e. `L ≈ target · λ`). When the
/// system is unsaturated the backlog stays below `LOAD_FRAC · cap` so the cap never
/// grows to throttle a healthy pipeline; the drain-down servo does the actual
/// intake pacing toward this setpoint. State is interior-mutable (the published cap
/// atomic doubles as the servo state), so a shared `&self` monitor can step it.
#[derive(Clone)]
pub struct BacklogGovernor {
    /// Published admission-backlog cap the gate reads — and the servo's own state
    /// (`step` loads the current cap, folds one window, stores the new cap).
    cap: Arc<AtomicUsize>,
    floor: usize,
    ceiling: usize,
    target_us: f64,
    obs: GovernorObs,
    verbose: bool,
}

/// "Loaded" = the runnable backlog occupies at least this fraction of the current
/// cap. Growth is gated on it so an idle cap is never inflated toward the ceiling.
const BACKLOG_GOV_LOAD_FRAC: f64 = 0.5;
/// Multiplicative growth per step while under-SLO and loaded (~+15%/s).
const BACKLOG_GOV_GROWTH: f64 = 1.15;
/// Dead-band around the target (as a fraction) within which the cap is held, so it
/// doesn't chatter ±one step around the setpoint every tick.
const BACKLOG_GOV_DEADBAND: f64 = 0.15;

impl BacklogGovernor {
    /// Build a governor bounded by `[floor, ceiling]` holding the absolute e2e
    /// sojourn SLO `target_us`. Returns the governor plus the shared cap handle the
    /// admission gate reads (seeded at the floor) and a [`GovernorObs`] read handle
    /// the monitor surfaces to explain the live cap.
    pub fn new(
        floor: usize,
        ceiling: usize,
        target_us: f64,
    ) -> (Self, Arc<AtomicUsize>, GovernorObs) {
        let cap = Arc::new(AtomicUsize::new(floor));
        let obs = GovernorObs::default();
        obs.target_us.store(target_us as u64, Ordering::Relaxed);
        let gov = Self {
            cap: cap.clone(),
            floor,
            ceiling,
            target_us,
            obs: obs.clone(),
            verbose: std::env::var_os("NANOBPM_ACTOR_PROFILE").is_some(),
        };
        (gov, cap, obs)
    }

    pub fn floor(&self) -> usize {
        self.floor
    }

    pub fn ceiling(&self) -> usize {
        self.ceiling
    }

    pub fn obs(&self) -> &GovernorObs {
        &self.obs
    }

    /// Fold one window's mean e2e job sojourn (`sojourn_us`) and the current
    /// runnable `backlog` into the cap, publish, and return the new cap. Call once
    /// per monitor tick *only when at least one job completed in the window* (so
    /// `sojourn_us` is a fresh measurement); during a total drain stall (no
    /// completions) the caller holds the cap and lets the drain-down servo throttle.
    pub fn step(&self, sojourn_us: f64, backlog: usize) -> usize {
        let prev = self.cap.load(Ordering::Relaxed);
        let mut limit = prev as f64;
        let loaded = backlog as f64 >= limit * BACKLOG_GOV_LOAD_FRAC;
        if sojourn_us > self.target_us * (1.0 + BACKLOG_GOV_DEADBAND) {
            // Over the sojourn SLO: multiplicative decrease toward the floor.
            limit = (limit * BACKOFF).max(self.floor as f64);
        } else if loaded && sojourn_us < self.target_us * (1.0 - BACKLOG_GOV_DEADBAND) {
            // Under the SLO with a loaded cap: grow toward the ceiling.
            limit = (limit * BACKLOG_GOV_GROWTH).min(self.ceiling as f64);
        }
        let new = limit as usize;
        self.cap.store(new, Ordering::Relaxed);
        self.obs
            .window_avg_us
            .store(sojourn_us as u64, Ordering::Relaxed);
        self.obs
            .target_us
            .store(self.target_us as u64, Ordering::Relaxed);
        // Track the lowest sojourn seen as an informational "light-load" baseline.
        if sojourn_us > 0.0 {
            let s = sojourn_us as u64;
            let cur = self.obs.baseline_us.load(Ordering::Relaxed);
            if cur == 0 || s < cur {
                self.obs.baseline_us.store(s, Ordering::Relaxed);
            }
        }
        if self.verbose && new != prev {
            tracing::info!(
                "backlog-governor: cap {prev} -> {new} (sojourn {sojourn_us:.0}us, \
                 target {:.0}us, backlog {backlog})",
                self.target_us,
            );
        }
        new
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_defaults_to_adaptive() {
        assert_eq!(
            parse_backpressure_setting(None),
            BackpressureSetting::Adaptive
        );
        for raw in ["auto", "adaptive", "ADAPTIVE", " Auto "] {
            assert_eq!(
                parse_backpressure_setting(Some(raw)),
                BackpressureSetting::Adaptive,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn parse_off_aliases_disable() {
        for raw in ["0", "off", "none", "false", "disabled", "OFF", " Off "] {
            assert_eq!(
                parse_backpressure_setting(Some(raw)),
                BackpressureSetting::Disabled,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn parse_positive_integer_is_fixed() {
        assert_eq!(
            parse_backpressure_setting(Some("6000")),
            BackpressureSetting::Fixed(6000)
        );
        assert_eq!(
            parse_backpressure_setting(Some(" 1 ")),
            BackpressureSetting::Fixed(1)
        );
    }

    #[test]
    fn parse_unparseable_falls_back_to_adaptive() {
        for raw in ["garbage", "-5", "1.5"] {
            assert_eq!(
                parse_backpressure_setting(Some(raw)),
                BackpressureSetting::Adaptive,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn sla_mode_defaults_to_latency() {
        assert_eq!(parse_sla_mode(None), SlaMode::Latency);
        for raw in [
            "latency",
            "preserve-latency",
            "time-to-complete",
            " Latency ",
            "",
        ] {
            assert_eq!(parse_sla_mode(Some(raw)), SlaMode::Latency, "{raw:?}");
        }
    }

    #[test]
    fn sla_mode_admission_aliases() {
        for raw in [
            "admission",
            "preserve-admission",
            "accept-latency",
            "start-every-process",
            "throughput",
            " Admission ",
        ] {
            assert_eq!(parse_sla_mode(Some(raw)), SlaMode::Admission, "{raw:?}");
        }
    }

    #[test]
    fn sla_mode_unrecognised_fails_safe_to_latency() {
        for raw in ["garbage", "fast", "0"] {
            assert_eq!(parse_sla_mode(Some(raw)), SlaMode::Latency, "{raw:?}");
        }
    }

    #[test]
    fn sla_mode_sheds_for_latency_only_in_latency_mode() {
        assert!(SlaMode::Latency.sheds_for_latency());
        assert!(!SlaMode::Admission.sheds_for_latency());
    }

    #[test]
    fn shared_sla_mode_is_switchable_and_shared_across_clones() {
        let a = SharedSlaMode::new(SlaMode::Latency);
        let b = a.clone();
        assert_eq!(a.get(), SlaMode::Latency);
        assert_eq!(b.get(), SlaMode::Latency);
        // A switch on one handle is visible through the other (shared Arc<atomic>).
        b.set(SlaMode::Admission);
        assert_eq!(a.get(), SlaMode::Admission);
        assert_eq!(b.get(), SlaMode::Admission);
        a.set(SlaMode::Latency);
        assert_eq!(b.get(), SlaMode::Latency);
    }

    #[test]
    fn sla_mode_from_u8_fails_safe_to_latency() {
        assert_eq!(SlaMode::from_u8(SlaMode::Latency as u8), SlaMode::Latency);
        assert_eq!(
            SlaMode::from_u8(SlaMode::Admission as u8),
            SlaMode::Admission
        );
        // Any unexpected byte never silently drops latency protection.
        assert_eq!(SlaMode::from_u8(200), SlaMode::Latency);
    }

    #[test]
    fn current_limit_reflects_mode() {
        assert_eq!(Backpressure::Disabled.current_limit(), None);
        assert_eq!(Backpressure::Fixed(7).current_limit(), Some(7));
        let l = Arc::new(AtomicUsize::new(123));
        assert_eq!(Backpressure::Adaptive(l).current_limit(), Some(123));
    }

    #[test]
    fn should_shed_compares_concurrency_against_the_watermark() {
        // Disabled never sheds, whatever the gauge.
        assert!(!Backpressure::Disabled.should_shed(0));
        assert!(!Backpressure::Disabled.should_shed(1_000_000));

        // Fixed watermark sheds at or above the limit, admits below it.
        let bp = Backpressure::Fixed(2);
        assert!(
            !bp.should_shed(0),
            "0 concurrent creates is below the limit"
        );
        assert!(!bp.should_shed(1), "1 concurrent create is below the limit");
        assert!(
            bp.should_shed(2),
            "at the limit sheds (admits at most `limit`)"
        );
        assert!(bp.should_shed(3), "above the limit sheds");

        // Adaptive tracks its shared atomic.
        let l = Arc::new(AtomicUsize::new(5));
        let bp = Backpressure::Adaptive(l.clone());
        assert!(!bp.should_shed(4));
        assert!(bp.should_shed(5));
        l.store(10, Ordering::Relaxed);
        assert!(!bp.should_shed(5), "raising the watermark re-admits");
    }

    #[test]
    fn slow_start_grows_multiplicatively_while_healthy_and_loaded() {
        let mut a = AimdLimit::new(256, 256, 20_000);
        // baseline established at 100us; avg stays healthy (<2x); inflight saturates.
        let l1 = a.on_window(100.0, 10_000); // first window sets baseline
        let l2 = a.on_window(120.0, 10_000);
        assert!(l2 > l1, "slow-start should grow: {l1} -> {l2}");
        assert_eq!(l2, l1 * 2);
    }

    #[test]
    fn congestion_triggers_multiplicative_decrease_and_ends_slow_start() {
        let mut a = AimdLimit::new(1000, 256, 20_000);
        a.on_window(100.0, 10_000); // baseline 100us
        let before = a.limit();
        // avg 500us = 5x baseline > 2x threshold => congested
        let after = a.on_window(500.0, 10_000);
        assert!(
            after < before,
            "congestion must shrink the limit: {before} -> {after}"
        );
        // Now healthy + loaded but slow-start is over => additive (+1), not doubling.
        let add = a.on_window(120.0, 10_000);
        assert_eq!(add, after + 1, "post-congestion growth is additive");
    }

    #[test]
    fn never_grows_below_load_or_outside_bounds() {
        let mut a = AimdLimit::new(1000, 256, 20_000);
        a.on_window(100.0, 0); // baseline; inflight 0 => not loaded
        let l = a.on_window(120.0, 0); // healthy but idle => no growth
        assert_eq!(l, 1000);
        // Decrease floors at min_limit.
        for _ in 0..100 {
            a.on_window(10_000.0, 10_000);
        }
        assert_eq!(a.limit(), 256);
    }

    #[test]
    fn sustained_congestion_keeps_shedding_and_does_not_drift_up() {
        // The unbounded-backlog pathology: latency stays high for a long time.
        // The limiter must keep the limit at the floor, never "learn" the
        // congested latency as normal and reopen the gates.
        let mut a = AimdLimit::new(8000, 256, 20_000);
        a.on_window(100.0, 10_000); // establish a healthy ~100us baseline
        for _ in 0..500 {
            // avg pinned high (queueing / heap pressure), inflight saturated.
            a.on_window(5_000.0, 10_000);
        }
        assert_eq!(
            a.limit(),
            256,
            "must stay shedding at the floor under sustained congestion"
        );
    }

    // --- self-optimizing backlog governor (AdaptiveController) ---------------

    /// Feed `n` synthetic commands of `us` microseconds each of the given class
    /// (`is_create`) so tests can drive the create vs completion window
    /// independently — the whole point of the per-class split. Enough to satisfy
    /// `WINDOW_MIN_SAMPLES`, with a sleep so `WINDOW_MIN_INTERVAL` also elapses.
    fn drive_window_class(c: &mut AdaptiveController, us: u64, n: u64, is_create: bool) {
        std::thread::sleep(WINDOW_MIN_INTERVAL + Duration::from_millis(1));
        for _ in 0..n.max(WINDOW_MIN_SAMPLES) {
            c.record(Duration::from_micros(us), is_create);
        }
    }

    // --- standalone e2e-sojourn backlog governor ----------------------------

    /// A sojourn target of 10ms (10_000µs) for the governor tests. Below-target
    /// = under the SLO (grow while loaded); above-target = over the SLO (back off).
    const T_US: f64 = 10_000.0;

    #[test]
    fn backlog_governor_grows_toward_ceiling_while_under_slo_and_loaded() {
        let (gov, cap, _obs) = BacklogGovernor::new(2_000, 200_000, T_US);
        assert_eq!(cap.load(Ordering::Relaxed), 2_000, "starts at the floor");
        // Sojourn well under the SLO, backlog above LOAD_FRAC × cap => grow.
        let before = cap.load(Ordering::Relaxed);
        gov.step(2_000.0, 100_000);
        assert!(
            cap.load(Ordering::Relaxed) > before,
            "under-SLO loaded windows must grow the cap: {before} -> {}",
            cap.load(Ordering::Relaxed)
        );
        // Several more steps keep growing toward the ceiling.
        for _ in 0..100 {
            gov.step(2_000.0, 1_000_000);
        }
        assert!(
            cap.load(Ordering::Relaxed) > 100_000,
            "sustained under-SLO loaded growth climbs toward the ceiling: {}",
            cap.load(Ordering::Relaxed)
        );
    }

    #[test]
    fn backlog_governor_backs_off_to_floor_over_the_sojourn_slo() {
        let (gov, cap, _obs) = BacklogGovernor::new(2_000, 200_000, T_US);
        // Grow it up first.
        for _ in 0..100 {
            gov.step(2_000.0, 1_000_000);
        }
        assert!(cap.load(Ordering::Relaxed) > 2_000);
        // Sojourn now far exceeds the SLO: multiplicative decrease to the floor.
        for _ in 0..200 {
            gov.step(50_000.0, 1_000_000);
        }
        assert_eq!(
            cap.load(Ordering::Relaxed),
            2_000,
            "sojourn above the SLO must drive the cap back to the floor"
        );
    }

    #[test]
    fn backlog_governor_does_not_grow_an_unloaded_cap() {
        // Sojourn is under the SLO but the backlog stays far below the cap (system
        // not saturated). Growth is gated on `loaded`, so a healthy unsaturated
        // pipeline never inflates the cap and never throttles itself.
        let (gov, cap, _obs) = BacklogGovernor::new(2_000, 200_000, T_US);
        for _ in 0..50 {
            gov.step(1_000.0, 100); // deep-under-SLO, backlog 100 « 0.5 × 2000
        }
        assert_eq!(
            cap.load(Ordering::Relaxed),
            2_000,
            "an unloaded cap must never grow on low sojourn alone"
        );
    }

    #[test]
    fn backlog_governor_holds_within_the_dead_band() {
        // Sojourn sitting right at the target (inside the dead-band) must neither
        // grow nor shrink the cap — no chatter around the setpoint.
        let (gov, cap, _obs) = BacklogGovernor::new(2_000, 200_000, T_US);
        for _ in 0..20 {
            gov.step(2_000.0, 1_000_000); // grow off the floor first
        }
        let settled = cap.load(Ordering::Relaxed);
        assert!(settled > 2_000);
        for _ in 0..20 {
            gov.step(T_US, 1_000_000); // exactly at target => within dead-band
        }
        assert_eq!(
            cap.load(Ordering::Relaxed),
            settled,
            "sojourn within the dead-band of the target must hold the cap"
        );
    }

    #[test]
    fn backlog_governor_publishes_observability_sojourn_and_target() {
        let (gov, _cap, obs) = BacklogGovernor::new(2_000, 200_000, T_US);
        // Target published at construction.
        assert_eq!(obs.target_us.load(Ordering::Relaxed), 10_000);
        // Nothing else until a window folds.
        assert_eq!(obs.window_avg_us.load(Ordering::Relaxed), 0);
        assert_eq!(obs.baseline_us.load(Ordering::Relaxed), 0);

        gov.step(3_000.0, 1_000_000);
        assert_eq!(obs.window_avg_us.load(Ordering::Relaxed), 3_000);
        assert_eq!(
            obs.baseline_us.load(Ordering::Relaxed),
            3_000,
            "the lowest sojourn seen is the informational baseline"
        );
        // A lower sojourn snaps the baseline down; a higher one leaves it.
        gov.step(1_500.0, 1_000_000);
        assert_eq!(obs.baseline_us.load(Ordering::Relaxed), 1_500);
        gov.step(9_000.0, 1_000_000);
        assert_eq!(obs.baseline_us.load(Ordering::Relaxed), 1_500);
        assert_eq!(obs.window_avg_us.load(Ordering::Relaxed), 9_000);
    }

    #[test]
    fn controller_is_active_only_with_a_limiter_installed() {
        assert!(!AdaptiveController::new().is_active());
        let mut c = AdaptiveController::new();
        c.with_create_limiter(Arc::new(AtomicUsize::new(0)));
        assert!(c.is_active());
        let mut c = AdaptiveController::new();
        c.with_worker_governor(50, 4_096, Arc::new(AtomicUsize::new(0)));
        assert!(c.is_active());
    }

    // --- self-optimizing worker-concurrency governor -------------------------

    #[test]
    fn worker_governor_grows_active_width_while_healthy_and_loaded() {
        // Backlog well above the width => there is work to fan out across more
        // subscribers, and latency stays healthy, so the governor slow-starts the
        // active dispatch width upward from the floor.
        let backlog = Arc::new(AtomicUsize::new(100_000));
        let mut c = AdaptiveController::new();
        let width = c.with_worker_governor(50, 4_096, backlog.clone());
        assert_eq!(width.load(Ordering::Relaxed), 50, "starts at the floor");

        drive_window_class(&mut c, 100, WINDOW_MIN_SAMPLES, false); // establish baseline
        let after_baseline = width.load(Ordering::Relaxed);
        drive_window_class(&mut c, 110, WINDOW_MIN_SAMPLES, false); // healthy + loaded => grow
        assert!(
            width.load(Ordering::Relaxed) > after_baseline,
            "healthy loaded windows must widen the fan-out: {after_baseline} -> {}",
            width.load(Ordering::Relaxed)
        );
    }

    #[test]
    fn worker_governor_backs_off_to_floor_under_congestion() {
        // Over-provisioning past the knee inflates per-command latency; the
        // governor must narrow the active width back to the floor so excess
        // subscribers stop swamping the push dispatcher.
        let backlog = Arc::new(AtomicUsize::new(100_000));
        let mut c = AdaptiveController::new();
        let width = c.with_worker_governor(50, 4_096, backlog.clone());
        drive_window_class(&mut c, 100, WINDOW_MIN_SAMPLES, false); // baseline 100us
        for _ in 0..5 {
            drive_window_class(&mut c, 110, WINDOW_MIN_SAMPLES, false); // grow a bit first
        }
        assert!(width.load(Ordering::Relaxed) > 50);
        for _ in 0..200 {
            drive_window_class(&mut c, 5_000, WINDOW_MIN_SAMPLES, false); // sustained congestion
        }
        assert_eq!(
            width.load(Ordering::Relaxed),
            50,
            "congestion must hold the active width at the knee floor"
        );
    }

    #[test]
    fn worker_governor_does_not_widen_without_backlog() {
        // No runnable backlog (workers idle / nothing to drain): widening the
        // fan-out would only add dispatcher overhead, so growth is gated on the
        // backlog signal and the width must stay pinned at the floor.
        let backlog = Arc::new(AtomicUsize::new(0));
        let mut c = AdaptiveController::new();
        let width = c.with_worker_governor(50, 4_096, backlog.clone());
        drive_window_class(&mut c, 100, WINDOW_MIN_SAMPLES, false); // baseline
        for _ in 0..20 {
            drive_window_class(&mut c, 110, WINDOW_MIN_SAMPLES, false); // healthy but unloaded
        }
        assert_eq!(
            width.load(Ordering::Relaxed),
            50,
            "no backlog to drain must never widen the active fan-out"
        );
    }
}
