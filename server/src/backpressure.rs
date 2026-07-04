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
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

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
    /// concurrency limiter and the active-backlog admission gate.
    Latency,
    /// **Preserve admission** (accept latency). At the ceiling, keep admitting
    /// new instances and let end-to-end latency grow instead of rejecting work.
    /// This is the "start-every-process SLA": the latency-preservation gates (the
    /// AIMD concurrency limiter and the active-backlog gate) are suppressed, so
    /// creates are admitted until a genuine memory-safety rail bites. Terminal
    /// state now frees on completion (ADR 0012) and live variables spill to disk,
    /// so far more instances start before that hard rail is reached.
    Admission,
}

impl SlaMode {
    /// Whether latency-preservation gates (AIMD concurrency shed + active-backlog
    /// shed) should reject admission. `true` in [`SlaMode::Latency`], `false` in
    /// [`SlaMode::Admission`] (which prefers admitting and accepts the latency).
    /// Memory-safety rails ignore this and always apply.
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
        "admission" | "preserve-admission" | "accept-latency" | "start-every-process"
        | "start" | "throughput" => SlaMode::Admission,
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
const CONGESTION_RATIO: f64 = 2.0;
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

    /// The current latency baseline (µs) the congestion threshold is derived from.
    pub fn baseline_us(&self) -> f64 {
        self.baseline_us
    }

    /// Fold one window of stats into the limit and return the new value.
    /// `avg_us` is the window's mean per-command latency; `inflight` is the
    /// current in-flight count (the limit only grows while it is being used, so
    /// an idle engine doesn't inflate the watermark).
    ///
    /// Congestion is judged by comparing the current window average against a
    /// *baseline of the lowest average seen* — like-for-like, since the command
    /// mix (cheap completes vs heavy 50 KB creates) makes a single-command
    /// minimum a poor reference. When the average inflates past
    /// `baseline * CONGESTION_RATIO` the engine is queueing / under heap pressure,
    /// so the limit is cut.
    pub fn on_window(&mut self, avg_us: f64, inflight: usize) -> usize {
        // Establish the baseline on the first ever window.
        if self.baseline_us == 0.0 {
            self.baseline_us = avg_us;
        }

        let threshold = self.baseline_us * CONGESTION_RATIO;
        let loaded = (inflight as f64) * 2.0 >= self.limit;

        if avg_us > threshold {
            // Congested: multiplicative decrease, and leave slow-start for good.
            // Crucially the baseline is *frozen* here — never adapt it toward a
            // congested latency, or sustained queueing would be accepted as the
            // new normal and shedding would stop (the exact unbounded-backlog
            // pathology we exist to prevent).
            self.slow_start = false;
            self.limit = (self.limit * BACKOFF).max(self.min_limit as f64);
        } else {
            // Healthy window: refine the baseline toward the uncongested floor
            // (snap down to a new minimum, else creep up slowly so a genuinely
            // slower host isn't throttled forever), then probe the limit upward
            // while it is being used.
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

/// Engine-thread side of the adaptive limiter: accumulates per-command latency
/// into windows, runs an [`AimdLimit`] step per window, and publishes the result
/// to the shared `limit` atomic that request handlers read. Owned by the single
/// engine thread, so its window state needs no synchronization.
pub struct AdaptiveController {
    aimd: AimdLimit,
    shared: Arc<AtomicUsize>,
    inflight: Arc<AtomicUsize>,
    count: u64,
    sum_us: f64,
    window_start: Instant,
    /// Log every limit change at INFO (gated by `NANOBPM_ACTOR_PROFILE`) so the
    /// limiter's convergence is observable during tuning.
    verbose: bool,
}

impl AdaptiveController {
    /// Builds the controller and returns it alongside the shared limit handle to
    /// install in [`Backpressure::Adaptive`].
    pub fn new(inflight: Arc<AtomicUsize>) -> (Self, Arc<AtomicUsize>) {
        let shared = Arc::new(AtomicUsize::new(INITIAL_LIMIT));
        let controller = Self {
            aimd: AimdLimit::new(INITIAL_LIMIT, MIN_LIMIT, MAX_LIMIT),
            shared: shared.clone(),
            inflight,
            count: 0,
            sum_us: 0.0,
            window_start: Instant::now(),
            verbose: std::env::var_os("NANOBPM_ACTOR_PROFILE").is_some(),
        };
        (controller, shared)
    }

    /// Record one command's processing latency. Evaluates the window (and updates
    /// the published limit) once it has enough samples and has run long enough.
    pub fn record(&mut self, latency: Duration) {
        self.count += 1;
        self.sum_us += latency.as_micros() as f64;
        if self.count >= WINDOW_MIN_SAMPLES && self.window_start.elapsed() >= WINDOW_MIN_INTERVAL {
            let avg = self.sum_us / self.count as f64;
            let inflight = self.inflight.load(Ordering::Relaxed);
            let prev = self.aimd.limit();
            let new_limit = self.aimd.on_window(avg, inflight);
            self.shared.store(new_limit, Ordering::Relaxed);
            if self.verbose && new_limit != prev {
                tracing::info!(
                    "backpressure(adaptive): limit {prev} -> {new_limit} (avg {avg:.0}us, \
                     baseline {:.0}us, inflight {inflight})",
                    self.aimd.baseline_us(),
                );
            }
            self.count = 0;
            self.sum_us = 0.0;
            self.window_start = Instant::now();
        }
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
        for raw in ["latency", "preserve-latency", "time-to-complete", " Latency ", ""] {
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
        assert_eq!(SlaMode::from_u8(SlaMode::Admission as u8), SlaMode::Admission);
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
}
