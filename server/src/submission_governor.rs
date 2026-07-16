//! Adaptive producer submission-window governor — a TCP-style congestion window
//! on create admission credits.
//!
//! # Why
//!
//! Each producer connection holds a fixed pool of *submission credits* (the
//! `submission_window`, default 256): the client may have that many creates
//! in-flight before it must wait for the server to top the pool back up. That
//! fixed window is right at steady state but wrong under capacity loss: when a
//! peer node fails, the surviving nodes carry ~1.5× their share of partitions on
//! the same shared single-writer engine + Raft log, so a create now takes far
//! longer to *accept* (commit). With the window fixed, every producer keeps the
//! same number of creates outstanding, the loadgen's deep inflight buffer keeps
//! refilling, and the cluster limit-cycles between overshoot and drain instead of
//! settling at the reduced-capacity throughput. The admission **backlog** lever
//! never bites here (this fast create→complete workload keeps resident backlog an
//! order of magnitude below its floor); the binding lever is the create *intake
//! window* itself.
//!
//! # How
//!
//! An AIMD congestion window on the **create-accept latency** (frame receipt →
//! accepted result, `nanobpm_create_accept_seconds`) — the closed-loop business
//! signal that captures batcher / single-writer / replication queueing that the
//! raw `commit_wait` misses. Each ~1 Hz monitor tick:
//!
//! * A low-pass EWMA smooths the window-mean create-accept latency.
//! * A healthy **baseline** create-accept latency is calibrated only while the
//!   window is wide open (unthrottled) and latency is not already elevated —
//!   i.e. from steady-state. During a throttle episode the baseline is frozen so
//!   sustained congestion never becomes the new "normal".
//! * The controller compares the smoothed latency to `baseline × target_ratio`
//!   (guarded by an absolute floor so normal sub-millisecond jitter can't
//!   hair-trigger it): above it (congestion) it multiplicatively **shrinks** the
//!   window; comfortably below it (`× release_frac`, a dead band) it additively
//!   **grows** the window back toward the ceiling; in between it holds.
//! * The resolved window is exported as the cluster's effective per-producer
//!   `submission_window` cap: the credit top-up path caps each connection's
//!   window to `min(conn.submission_window, governor_cap)`, so fewer creates are
//!   admitted while a node is down and the full window is restored on recovery.
//!
//! **Inert when healthy:** the window re-arms at and holds the ceiling, which is
//! `>=` the default per-connection `submission_window`, so `min(window, cap)` is a
//! no-op and steady-state admission is byte-identical to the ungoverned path.
//! This is negative feedback (Little's-law equilibrium: outstanding creates track
//! the delay-bandwidth product), *not* the pro-cyclical completion-paced servo —
//! it paces to a latency target, not to the depressed completion rate, so it does
//! not spiral downward during a completion dip.

/// Tunables for the adaptive submission-window governor. Defaults keep it inert
/// at steady state and only engage under sustained create-accept latency
/// inflation (capacity loss). Overridable via the environment (see [`from_env`]).
///
/// [`from_env`]: SubmissionGovernorCfg::from_env
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SubmissionGovernorCfg {
    /// Master switch (`NANOBPMN_SUBMISSION_GOVERNOR`, default **on**). When off the
    /// governor always publishes the ceiling (no cap; `observe` returns the
    /// ceiling every tick).
    pub enabled: bool,
    /// Smallest per-producer window the governor will shrink to. Floors intake so
    /// a degraded cluster still makes create progress instead of stalling.
    pub floor: i64,
    /// Largest per-producer window — the "unthrottled" value the window re-arms at.
    /// Should be `>=` the default per-connection `submission_window` so the
    /// governor is inert (a no-op `min`) at steady state.
    pub ceiling: i64,
    /// Smoothed create-accept latency above `baseline × target_ratio` counts as
    /// congestion and triggers a multiplicative decrease.
    pub target_ratio: f64,
    /// Ease the window back up only when the smoothed latency is comfortably below
    /// the target (`target × release_frac`); between the two is a hold dead band.
    pub release_frac: f64,
    /// Absolute floor (µs) on the congestion target, so a tiny healthy baseline
    /// cannot make normal jitter look like congestion.
    pub target_floor_us: f64,
    /// Multiplicative-decrease factor applied to the window on congestion (AIMD "MD").
    pub backoff: f64,
    /// Additive-increase step (credits) applied to the window per healthy tick
    /// (AIMD "AI").
    pub step: i64,
    /// EWMA weight on the newest latency sample (low-pass; smaller = smoother).
    pub ewma_alpha: f64,
    /// Upward drift of the healthy baseline when no faster sample is seen.
    pub baseline_creep: f64,
    /// Minimum create-accept samples in a window before the governor acts on it
    /// (below this it holds — a thin window is too noisy to steer on).
    pub min_samples: u64,
}

impl Default for SubmissionGovernorCfg {
    fn default() -> Self {
        Self {
            enabled: true,
            floor: 8,
            ceiling: 256,
            target_ratio: 3.0,
            release_frac: 0.5,
            target_floor_us: 2_000.0,
            backoff: 0.8,
            step: 8,
            ewma_alpha: 0.3,
            baseline_creep: 0.05,
            min_samples: 50,
        }
    }
}

impl SubmissionGovernorCfg {
    /// Reads the config from the environment, falling back to [`Default`].
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            enabled: env_bool("NANOBPMN_SUBMISSION_GOVERNOR", d.enabled),
            floor: env_i64("NANOBPMN_SUBMISSION_GOVERNOR_FLOOR", d.floor),
            ceiling: env_i64("NANOBPMN_SUBMISSION_GOVERNOR_CEILING", d.ceiling),
            target_ratio: env_f64("NANOBPMN_SUBMISSION_GOVERNOR_RATIO", d.target_ratio),
            release_frac: env_f64("NANOBPMN_SUBMISSION_GOVERNOR_RELEASE", d.release_frac),
            target_floor_us: env_f64(
                "NANOBPMN_SUBMISSION_GOVERNOR_TARGET_FLOOR_US",
                d.target_floor_us,
            ),
            backoff: env_f64("NANOBPMN_SUBMISSION_GOVERNOR_BACKOFF", d.backoff),
            step: env_i64("NANOBPMN_SUBMISSION_GOVERNOR_STEP", d.step),
            ewma_alpha: env_f64("NANOBPMN_SUBMISSION_GOVERNOR_EWMA", d.ewma_alpha),
            baseline_creep: env_f64("NANOBPMN_SUBMISSION_GOVERNOR_CREEP", d.baseline_creep),
            min_samples: env_i64(
                "NANOBPMN_SUBMISSION_GOVERNOR_MIN_SAMPLES",
                d.min_samples as i64,
            )
            .max(1) as u64,
        }
    }
}

fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => {
            let v = v.trim();
            !(v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off"))
        }
        Err(_) => default,
    }
}

fn env_i64(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .filter(|f: &f64| f.is_finite() && *f > 0.0)
        .unwrap_or(default)
}

/// Pure, tick-driven AIMD congestion-window controller. Holds the smoothed
/// create-accept signal, the healthy baseline, and the current window. Kept
/// `Instant`-free so its dynamics are unit-testable; the windowing/IO lives in the
/// server's monitor tick.
#[derive(Debug)]
pub struct SubmissionGovernor {
    cfg: SubmissionGovernorCfg,
    /// Low-pass EWMA of the window-mean create-accept latency (µs). `None` until
    /// the first sufficiently-sampled window seeds it.
    ewma_us: Option<f64>,
    /// Calibrated healthy create-accept baseline (µs). `None` until seeded.
    baseline_us: Option<f64>,
    /// Current per-producer submission-window cap (credits).
    window: f64,
}

impl SubmissionGovernor {
    pub fn new(cfg: SubmissionGovernorCfg) -> Self {
        Self {
            cfg,
            ewma_us: None,
            baseline_us: None,
            window: cfg.ceiling as f64,
        }
    }

    /// Whether the governor is currently throttling (window shrunk below the
    /// ceiling). Exposed for the monitor tick's engagement log / metric.
    pub fn is_engaged(&self) -> bool {
        self.cfg.enabled && self.window < self.cfg.ceiling as f64
    }

    /// The current per-producer submission-window cap (credits).
    #[cfg(test)]
    pub fn window(&self) -> i64 {
        self.window.round() as i64
    }

    /// Folds one monitor tick and returns the per-producer submission-window cap
    /// to apply cluster-wide (`min(conn.submission_window, cap)`).
    ///
    /// * `avg_us` — the window-mean create-accept latency this tick (µs), from the
    ///   delta of `nanobpm_create_accept_seconds` sum/count. `0` (no creates this
    ///   window) leaves the smoothed signal and baseline unchanged.
    /// * `samples` — number of create-accept observations in the window; below
    ///   `min_samples` the governor holds (too noisy to steer on).
    pub fn observe(&mut self, avg_us: f64, samples: u64) -> i64 {
        if !self.cfg.enabled {
            self.window = self.cfg.ceiling as f64;
            return self.cfg.ceiling;
        }

        // Too few samples this window to trust the mean: hold the current window.
        if samples < self.cfg.min_samples || avg_us <= 0.0 {
            return self.window.round() as i64;
        }

        // Low-pass the signal.
        self.ewma_us = Some(match self.ewma_us {
            Some(prev) => self.cfg.ewma_alpha * avg_us + (1.0 - self.cfg.ewma_alpha) * prev,
            None => avg_us,
        });
        let signal = self.ewma_us.unwrap_or(avg_us);

        // Congestion target: baseline scaled, guarded by an absolute floor.
        let baseline = self.baseline_us.unwrap_or(signal);
        let target = (baseline * self.cfg.target_ratio).max(self.cfg.target_floor_us);

        let at_ceiling = self.window >= self.cfg.ceiling as f64;

        if signal > target {
            // Congestion: multiplicative decrease. Freeze the baseline (do not let
            // congested latency drift the baseline up).
            self.window = (self.window * self.cfg.backoff).max(self.cfg.floor as f64);
        } else if signal < target * self.cfg.release_frac {
            // Comfortable headroom: additive increase back toward the ceiling.
            self.window = (self.window + self.cfg.step as f64).min(self.cfg.ceiling as f64);
            // Calibrate the healthy baseline only from steady-state (window fully
            // open and latency low), snapping down to the min seen else creeping up.
            if at_ceiling {
                self.baseline_us = Some(match self.baseline_us {
                    None => signal,
                    Some(b) if signal < b => signal,
                    Some(b) => b + (signal - b) * self.cfg.baseline_creep,
                });
            }
        }
        // Dead band (target*release_frac ..= target): hold.

        self.window.round() as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> SubmissionGovernorCfg {
        SubmissionGovernorCfg {
            enabled: true,
            floor: 8,
            ceiling: 256,
            target_ratio: 3.0,
            release_frac: 0.5,
            target_floor_us: 2_000.0,
            backoff: 0.5,
            step: 16,
            ewma_alpha: 1.0, // no smoothing lag in tests: signal == latest sample
            baseline_creep: 0.05,
            min_samples: 10,
        }
    }

    #[test]
    fn inert_at_ceiling_when_healthy() {
        let mut g = SubmissionGovernor::new(cfg());
        // Healthy steady state: low latency, plenty of samples. Window stays at the
        // ceiling (a no-op cap) and the governor never engages.
        for _ in 0..20 {
            assert_eq!(g.observe(20_000.0, 500), cfg().ceiling);
        }
        assert!(!g.is_engaged());
        // Baseline calibrated to the healthy latency.
        assert_eq!(g.baseline_us, Some(20_000.0));
    }

    #[test]
    fn congestion_shrinks_the_window_then_recovers() {
        let mut g = SubmissionGovernor::new(cfg());
        // Calibrate a healthy 20ms baseline (target = 60ms).
        for _ in 0..5 {
            g.observe(20_000.0, 500);
        }
        assert_eq!(g.baseline_us, Some(20_000.0));

        // Capacity loss: create-accept latency inflates to 400ms (>> 60ms target).
        let w0 = g.window();
        let w1 = g.observe(400_000.0, 500);
        let w2 = g.observe(400_000.0, 500);
        assert!(w1 < w0, "congestion must shrink the window: {w0} -> {w1}");
        assert!(
            w2 < w1,
            "sustained congestion keeps shrinking: {w1} -> {w2}"
        );
        assert!(g.is_engaged());
        // Baseline frozen through the congestion episode.
        assert_eq!(g.baseline_us, Some(20_000.0));

        // Recovery: latency drops back below target*release_frac (30ms). Additive
        // increase eases the window back up.
        let w3 = g.observe(20_000.0, 500);
        assert!(w3 > w2, "headroom must grow the window back: {w2} -> {w3}");
        assert_eq!(w3, w2 + cfg().step);
    }

    #[test]
    fn window_is_floored_under_sustained_congestion() {
        let mut g = SubmissionGovernor::new(cfg());
        for _ in 0..5 {
            g.observe(20_000.0, 500);
        }
        for _ in 0..100 {
            g.observe(2_000_000.0, 500); // hammer with 2s latency
        }
        assert_eq!(g.window(), cfg().floor);
        assert!(g.is_engaged());
    }

    #[test]
    fn dead_band_holds_the_window() {
        let mut g = SubmissionGovernor::new(cfg());
        for _ in 0..5 {
            g.observe(20_000.0, 500); // baseline 20ms, target 60ms
        }
        // Shrink once with congestion.
        g.observe(400_000.0, 500);
        let w = g.window();
        // Latency lands in the dead band (between 30ms and 60ms): hold.
        assert_eq!(g.observe(45_000.0, 500), w);
    }

    #[test]
    fn low_sample_windows_hold() {
        let mut g = SubmissionGovernor::new(cfg());
        for _ in 0..5 {
            g.observe(20_000.0, 500);
        }
        g.observe(400_000.0, 500); // shrink
        let w = g.window();
        // A congested reading but too few samples: ignored, window held.
        assert_eq!(g.observe(2_000_000.0, 3), w);
    }

    #[test]
    fn target_floor_prevents_hair_trigger_on_tiny_baseline() {
        let mut g = SubmissionGovernor::new(cfg());
        // Sub-millisecond baseline: target would be 0.15ms without the floor.
        for _ in 0..5 {
            g.observe(50.0, 500);
        }
        // 1.5ms latency: 30x the tiny baseline but below the 2ms target floor.
        // Must NOT throttle.
        assert_eq!(g.observe(1_500.0, 500), cfg().ceiling);
        assert!(!g.is_engaged());
    }

    #[test]
    fn disabled_always_returns_ceiling() {
        let mut c = cfg();
        c.enabled = false;
        let mut g = SubmissionGovernor::new(c);
        assert_eq!(g.observe(2_000_000.0, 500), cfg().ceiling);
        assert!(!g.is_engaged());
    }
}
