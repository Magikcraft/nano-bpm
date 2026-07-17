//! Adaptive recovery admission throttle — the durability-preserving recovery lever.
//!
//! # Why
//!
//! Under the leader-durable model (one voter per partition + async learners, ADR
//! 0003) the leader's local `fsync` is the sole durability point. When a node
//! becomes a **failover incumbent** (leads a down peer's partitions) or a
//! **returning owner** catching back up, it carries ~double its steady Raft load
//! on one shared disk; under `NANOBPMN_DURABILITY=sync` that doubles the
//! per-round `fsync` rate and saturates the disk. Saturated `fsync` inflates
//! commit latency an order of magnitude and makes commits — and therefore the
//! completion-paced admission servo — bursty and oscillatory.
//!
//! The *durability-preserving* fix is not to defer the `fsync` (that is the
//! opt-in coalescing fallback, which relaxes durability) but to **pace intake**
//! so the failover node's disk stays under its `fsync` knee. Fewer creates admitted
//! ⇒ fewer Raft appends ⇒ the disk keeps up with strict per-round `fsync` and the
//! oscillation damps out. The cost is throughput *during the already-degraded
//! recovery window* — the cheapest, fully-reversible thing to give up there.
//!
//! # How
//!
//! An AIMD controller on the **Raft-log `fsync` latency** (the direct disk-
//! saturation signal, `nanobpm_raft_fsync_seconds`). Each ~1 Hz monitor tick:
//!
//! * A low-pass EWMA smooths the window-mean `fsync` latency so the controller
//!   damps rather than chases spikes.
//! * The healthy `fsync` **baseline** is calibrated *outside* recovery (snap down
//!   to the min seen, else creep up slowly). Calibrating outside recovery is what
//!   avoids the cold-start pathology — if the baseline were learned from the first
//!   recovery window it would lock onto the congested latency and never throttle.
//! * *During* recovery the baseline is frozen and the controller compares the
//!   smoothed `fsync` latency to `baseline * congestion_ratio`: above it
//!   (disk saturating) it multiplicatively **decreases** the admission backlog
//!   cap; below it (headroom) it additively **increases** it back toward the
//!   ceiling. The resolved cap is folded (as a `min`) into the unified admission
//!   setpoint, so the completion-paced servo paces creates to hold the backlog at
//!   it — in *all* SLA modes (a recovery liveness rail, like the drain guard).
//! * When recovery clears the throttle publishes *no clamp* and re-arms at the
//!   ceiling, so steady-state admission is never metered by this path.

/// Tunables for the adaptive recovery admission throttle. Defaults are chosen so
/// the throttle only bites while a node is actually in a recovery window with a
/// saturating disk; overridable via the environment (see [`from_env`]).
///
/// [`from_env`]: RecoveryThrottleCfg::from_env
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RecoveryThrottleCfg {
    /// Master switch (`NANOBPMN_RECOVERY_THROTTLE`, default **on**). When off the
    /// throttle never clamps (`observe` always returns `None`).
    pub enabled: bool,
    /// Lowest backlog the recovery cap will shrink to. Floors the throttle so it
    /// relieves the disk without starving the failover node's own drain.
    pub floor: usize,
    /// Highest backlog the recovery cap will grow back to — the "unthrottled"
    /// value the controller re-arms at when recovery clears / the disk has headroom.
    pub ceiling: usize,
    /// Smoothed `fsync` latency above `baseline * congestion_ratio` counts as disk
    /// saturation and triggers a multiplicative decrease.
    pub congestion_ratio: f64,
    /// Multiplicative-decrease factor applied to the cap on saturation (AIMD "MD").
    pub backoff: f64,
    /// Additive-increase step applied to the cap per healthy tick (AIMD "AI").
    pub step: usize,
    /// EWMA weight on the newest `fsync` sample (low-pass; smaller = smoother).
    pub ewma_alpha: f64,
    /// Upward drift of the healthy baseline when no faster sample is seen (so a
    /// permanently slower disk isn't throttled forever).
    pub baseline_creep: f64,
}

impl Default for RecoveryThrottleCfg {
    fn default() -> Self {
        Self {
            enabled: true,
            floor: 2_000,
            ceiling: 50_000,
            congestion_ratio: 2.0,
            backoff: 0.9,
            step: 500,
            ewma_alpha: 0.3,
            baseline_creep: 0.05,
        }
    }
}

impl RecoveryThrottleCfg {
    /// Reads the config from the environment, falling back to [`Default`].
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            enabled: env_bool("NANOBPMN_RECOVERY_THROTTLE", d.enabled),
            floor: env_usize("NANOBPMN_RECOVERY_THROTTLE_FLOOR", d.floor),
            ceiling: env_usize("NANOBPMN_RECOVERY_THROTTLE_CEILING", d.ceiling),
            congestion_ratio: env_f64("NANOBPMN_RECOVERY_THROTTLE_RATIO", d.congestion_ratio),
            backoff: env_f64("NANOBPMN_RECOVERY_THROTTLE_BACKOFF", d.backoff),
            step: env_usize("NANOBPMN_RECOVERY_THROTTLE_STEP", d.step),
            ewma_alpha: env_f64("NANOBPMN_RECOVERY_THROTTLE_EWMA", d.ewma_alpha),
            baseline_creep: env_f64("NANOBPMN_RECOVERY_THROTTLE_CREEP", d.baseline_creep),
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

fn env_usize(key: &str, default: usize) -> usize {
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

/// Pure, tick-driven AIMD controller for the recovery admission throttle. Holds
/// the smoothed `fsync` signal, the healthy baseline, and the current cap. Kept
/// `Instant`-free so its dynamics are unit-testable; the windowing/IO lives in the
/// server's monitor tick.
#[derive(Debug)]
pub struct RecoveryThrottle {
    cfg: RecoveryThrottleCfg,
    /// Low-pass EWMA of the window-mean Raft `fsync` latency (µs). `None` until the
    /// first sample seeds it.
    ewma_us: Option<f64>,
    /// Calibrated healthy-window `fsync` baseline (µs). `None` until seeded.
    baseline_us: Option<f64>,
    /// Current recovery backlog cap (only meaningful while recovering).
    cap: f64,
    /// Whether the previous tick was inside a recovery window (edge detection so a
    /// fresh episode re-arms the cap at the ceiling).
    recovering: bool,
}

impl RecoveryThrottle {
    pub fn new(cfg: RecoveryThrottleCfg) -> Self {
        Self {
            cfg,
            ewma_us: None,
            baseline_us: None,
            cap: cfg.ceiling as f64,
            recovering: false,
        }
    }

    /// Whether the throttle is currently clamping admission (in a recovery episode
    /// with the feature enabled). Exposed for the monitor tick's engagement log.
    pub fn is_engaged(&self) -> bool {
        self.cfg.enabled && self.recovering
    }

    /// Folds one monitor tick and returns the recovery backlog cap to fold into the
    /// unified admission setpoint, or `None` for *no recovery clamp* (feature off,
    /// or not in a recovery window).
    ///
    /// * `fsync_avg_us` — the window-mean Raft-log `fsync` latency this tick (µs),
    ///   from the delta of `nanobpm_raft_fsync_seconds` sum/count. `0` (no fsyncs
    ///   this window) leaves the smoothed signal and baseline unchanged.
    /// * `recovering` — whether this node is a failover incumbent or a returning
    ///   owner catching up (the server derives this from live leadership; see
    ///   `recovery_fsync_load_active`).
    pub fn observe(&mut self, fsync_avg_us: f64, recovering: bool) -> Option<usize> {
        if !self.cfg.enabled {
            self.recovering = false;
            return None;
        }

        // Low-pass the signal (ignore empty windows so idle ticks don't drag it).
        if fsync_avg_us > 0.0 {
            self.ewma_us = Some(match self.ewma_us {
                Some(prev) => {
                    self.cfg.ewma_alpha * fsync_avg_us + (1.0 - self.cfg.ewma_alpha) * prev
                }
                None => fsync_avg_us,
            });
        }
        let signal = self.ewma_us;

        if !recovering {
            // Outside recovery: calibrate the healthy baseline (snap down to the
            // minimum seen, else creep up slowly) and keep the cap re-armed at the
            // ceiling. No clamp.
            if let Some(s) = signal {
                self.baseline_us = Some(match self.baseline_us {
                    None => s,
                    Some(b) if s < b => s,
                    Some(b) => b + (s - b) * self.cfg.baseline_creep,
                });
            }
            self.cap = self.cfg.ceiling as f64;
            self.recovering = false;
            return None;
        }

        // Entering a recovery episode: re-arm at the ceiling (start unthrottled and
        // tighten only as the disk saturates).
        if !self.recovering {
            self.cap = self.cfg.ceiling as f64;
            self.recovering = true;
        }

        // During recovery the baseline is frozen (never adapt it toward the
        // congested latency, or sustained saturation would become the new normal
        // and the throttle would stop biting). Fall back to the current signal as a
        // baseline only if we somehow never calibrated one before the first episode.
        let baseline = self.baseline_us.or(signal).unwrap_or(0.0);
        let threshold = baseline * self.cfg.congestion_ratio;

        match signal {
            Some(s) if baseline > 0.0 && s > threshold => {
                // Disk saturating: multiplicative decrease.
                self.cap = (self.cap * self.cfg.backoff).max(self.cfg.floor as f64);
            }
            _ => {
                // Headroom (or no signal yet): additively ease the cap back up.
                self.cap = (self.cap + self.cfg.step as f64).min(self.cfg.ceiling as f64);
            }
        }
        Some(self.cap as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> RecoveryThrottleCfg {
        RecoveryThrottleCfg {
            enabled: true,
            floor: 1_000,
            ceiling: 40_000,
            congestion_ratio: 2.0,
            backoff: 0.5,
            step: 1_000,
            ewma_alpha: 1.0, // no smoothing lag in tests: signal == latest sample
            baseline_creep: 0.05,
        }
    }

    #[test]
    fn no_clamp_outside_recovery_and_baseline_calibrates() {
        let mut t = RecoveryThrottle::new(cfg());
        // Healthy windows outside recovery: never clamp, and the baseline learns
        // the low fsync latency.
        assert_eq!(t.observe(1_000.0, false), None);
        assert_eq!(t.observe(800.0, false), None); // snaps baseline down to 800
        assert!(!t.is_engaged());
        assert_eq!(t.baseline_us, Some(800.0));
    }

    #[test]
    fn saturation_during_recovery_backs_the_cap_off_then_recovers() {
        let mut t = RecoveryThrottle::new(cfg());
        // Calibrate a healthy baseline of 1ms outside recovery.
        t.observe(1_000.0, false);
        assert_eq!(t.baseline_us, Some(1_000.0));

        // Enter recovery: re-arms at ceiling on the first (still-healthy) tick.
        let c0 = t.observe(1_000.0, true).unwrap();
        assert_eq!(c0, cfg().ceiling); // no saturation yet -> stays at ceiling
        assert!(t.is_engaged());

        // Disk saturates (fsync 5ms >> 1ms*2 threshold): multiplicative decrease.
        let c1 = t.observe(5_000.0, true).unwrap();
        let c2 = t.observe(5_000.0, true).unwrap();
        assert!(c1 < c0, "saturation must shrink the cap: {c0} -> {c1}");
        assert!(
            c2 < c1,
            "sustained saturation keeps shrinking: {c1} -> {c2}"
        );

        // Disk recovers (fsync back to baseline): additive increase back up.
        let c3 = t.observe(1_000.0, true).unwrap();
        assert!(c3 > c2, "headroom must ease the cap back up: {c2} -> {c3}");
        assert_eq!(c3, c2 + cfg().step);

        // Baseline stayed frozen at the healthy value through the whole episode.
        assert_eq!(t.baseline_us, Some(1_000.0));
    }

    #[test]
    fn cap_is_floored_under_sustained_saturation() {
        let mut t = RecoveryThrottle::new(cfg());
        t.observe(1_000.0, false); // baseline 1ms
        t.observe(1_000.0, true); // arm at ceiling
        for _ in 0..100 {
            t.observe(50_000.0, true); // hammer with saturation
        }
        assert_eq!(t.observe(50_000.0, true), Some(cfg().floor));
    }

    #[test]
    fn clearing_recovery_disengages_and_rearms_at_ceiling() {
        let mut t = RecoveryThrottle::new(cfg());
        t.observe(1_000.0, false);
        t.observe(1_000.0, true);
        t.observe(50_000.0, true); // shrink below ceiling
        // Recovery clears -> no clamp, re-armed.
        assert_eq!(t.observe(1_000.0, false), None);
        assert!(!t.is_engaged());
        // Next episode starts back at the ceiling.
        assert_eq!(t.observe(1_000.0, true), Some(cfg().ceiling));
    }

    #[test]
    fn disabled_never_clamps() {
        let mut c = cfg();
        c.enabled = false;
        let mut t = RecoveryThrottle::new(c);
        assert_eq!(t.observe(50_000.0, true), None);
        assert!(!t.is_engaged());
    }
}
