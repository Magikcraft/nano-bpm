//! Capacity-aware admission governor — the cluster-capacity envelope lever.
//!
//! # Why
//!
//! The historical adaptive envelope (the backlog governor in [`crate::backpressure`])
//! is a **local, single-node latency AIMD**: it is fed the single-writer engine's
//! per-command *CPU processing* time (`job(journal)` wall time, microseconds) and
//! has no awareness of the cluster. That signal stays fast even when the binding
//! constraint is Raft replication/durability — so under overpressure the governor
//! never trips (measured on GCP: `admission_limit{backlog}` pinned at its floor
//! with `baseline_latency_us=0` through a full kill/restore), and a node failing is
//! invisible to it. The result is the post-failure oscillation: with the envelope
//! inert, admitted load is bounded only by the memory rails, so the two surviving
//! nodes thrash at the completion/commit layer instead of settling at the smaller
//! two-node envelope.
//!
//! This governor closes both gaps:
//!
//! 1. **It measures the binding signal** — the *commit-wait* latency
//!    (`nanobpm_commit_wait_seconds`, the wall time a caller waits for its command
//!    to become durable), not the local CPU time. Commit-wait balloons the moment
//!    the cluster is the bottleneck (a surviving node carrying a down peer's
//!    partitions runs ~double its Raft load on one disk), so the governor engages
//!    under real overpressure.
//! 2. **It is topology/capacity-aware** — it knows how many partitions this node
//!    *statically owns* versus how many it *currently leads*. When a peer is down
//!    this node leads more than it owns (a failover incumbent), so its per-node
//!    admission envelope is scaled down proportionally *immediately* (a `capacity
//!    factor` = `owned / led`), without waiting for latency to build. That is the
//!    proactive "shrink to the smaller envelope on node failure and **hold** it"
//!    behaviour: while degraded the AIMD may only operate *below* the reduced
//!    ceiling, so it cannot probe admission back up into the thrash band, and it
//!    re-arms only *additively* once full capacity returns (no throughput spike
//!    that re-triggers the oscillation).
//!
//! # How
//!
//! An AIMD controller on the smoothed window-mean commit-wait latency, folded (as a
//! `min`) into the unified admission setpoint in *all* SLA modes — a capacity
//! liveness rail, like the recovery throttle. Each ~1 Hz monitor tick:
//!
//! * A low-pass EWMA smooths the window-mean commit latency so the controller damps
//!   rather than chases spikes.
//! * The healthy commit **baseline** is calibrated only at **full capacity** (`owned
//!   == led`; snap down to the min seen, else creep up slowly). Calibrating only at
//!   full capacity is what avoids the cold-start pathology — if the baseline were
//!   learned while degraded it would lock onto the congested latency and never
//!   throttle.
//! * The **reduced ceiling** = `ceiling × owned/led` (floored). While degraded this
//!   caps the AIMD's operating range, so admission is proactively held at the
//!   smaller envelope regardless of the latency reading.
//! * The AIMD compares the smoothed commit latency to `baseline × congestion_ratio`:
//!   above it (cluster saturating) it multiplicatively **decreases** the cap; below
//!   it (headroom) it additively **increases** it back toward the reduced ceiling.
//! * The resolved cap is published (and folded as a `min` into the setpoint)
//!   whenever it sits below the full ceiling — i.e. whenever the governor is
//!   actively clamping, for capacity *or* congestion reasons. At full capacity with
//!   healthy commit latency the cap re-arms to the ceiling and the governor
//!   publishes *no clamp*, so steady-state healthy load is never metered by this
//!   path.

/// Tunables for the capacity-aware admission governor. Defaults are chosen so the
/// governor is a no-op at full capacity with healthy commit latency, and only
/// clamps when the cluster is degraded (a peer down) or genuinely saturating.
/// Overridable via the environment (see [`from_env`]).
///
/// [`from_env`]: CapacityGovernorCfg::from_env
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CapacityGovernorCfg {
    /// Master switch (`NANOBPMN_CAPACITY_GOVERNOR`, default **on**). When off the
    /// governor never clamps (`observe` always returns `None`).
    pub enabled: bool,
    /// Lowest backlog the capacity cap will shrink to. Floors the governor so it
    /// bounds the envelope without starving the node's own drain.
    pub floor: usize,
    /// The full-capacity envelope: the "unthrottled" backlog the governor re-arms
    /// at when the cluster is whole and commit latency is healthy. The reduced
    /// (degraded) ceiling is this scaled by the live capacity factor.
    pub ceiling: usize,
    /// Smoothed commit-wait latency above `baseline × congestion_ratio` counts as
    /// cluster saturation and triggers a multiplicative decrease.
    pub congestion_ratio: f64,
    /// Multiplicative-decrease factor applied to the cap on saturation (AIMD "MD").
    pub backoff: f64,
    /// Additive-increase step applied to the cap per healthy tick (AIMD "AI").
    pub step: usize,
    /// EWMA weight on the newest commit-wait sample (low-pass; smaller = smoother).
    pub ewma_alpha: f64,
    /// Upward drift of the healthy baseline when no faster sample is seen (so a
    /// permanently slower cluster isn't throttled forever).
    pub baseline_creep: f64,
}

impl Default for CapacityGovernorCfg {
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

impl CapacityGovernorCfg {
    /// Reads the config from the environment, falling back to [`Default`].
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            enabled: env_bool("NANOBPMN_CAPACITY_GOVERNOR", d.enabled),
            floor: env_usize("NANOBPMN_CAPACITY_GOVERNOR_FLOOR", d.floor),
            ceiling: env_usize("NANOBPMN_CAPACITY_GOVERNOR_CEILING", d.ceiling),
            congestion_ratio: env_f64("NANOBPMN_CAPACITY_GOVERNOR_RATIO", d.congestion_ratio),
            backoff: env_f64("NANOBPMN_CAPACITY_GOVERNOR_BACKOFF", d.backoff),
            step: env_usize("NANOBPMN_CAPACITY_GOVERNOR_STEP", d.step),
            ewma_alpha: env_f64("NANOBPMN_CAPACITY_GOVERNOR_EWMA", d.ewma_alpha),
            baseline_creep: env_f64("NANOBPMN_CAPACITY_GOVERNOR_CREEP", d.baseline_creep),
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

/// Pure, tick-driven AIMD controller for the capacity governor. Holds the smoothed
/// commit-wait signal, the healthy baseline, and the current cap. Kept
/// `Instant`-free so its dynamics are unit-testable; the windowing/IO lives in the
/// server's monitor tick.
#[derive(Debug)]
pub struct CapacityGovernor {
    cfg: CapacityGovernorCfg,
    /// Low-pass EWMA of the window-mean commit-wait latency (µs). `None` until the
    /// first sample seeds it.
    ewma_us: Option<f64>,
    /// Calibrated healthy (full-capacity) commit-wait baseline (µs). `None` until
    /// seeded by a full-capacity window.
    baseline_us: Option<f64>,
    /// Current backlog cap.
    cap: f64,
    /// Whether the previous tick saw the node degraded (leading more partitions than
    /// it owns). Edge-detected for the engagement log.
    degraded: bool,
}

impl CapacityGovernor {
    pub fn new(cfg: CapacityGovernorCfg) -> Self {
        Self {
            cfg,
            ewma_us: None,
            baseline_us: None,
            cap: cfg.ceiling as f64,
            degraded: false,
        }
    }

    /// Whether the governor is currently clamping admission (cap below the full
    /// ceiling, feature enabled). Exposed for the monitor tick's engagement log.
    pub fn is_engaged(&self) -> bool {
        self.cfg.enabled && self.cap < self.cfg.ceiling as f64
    }

    /// Whether this node is currently degraded (leading more partitions than it
    /// statically owns — a failover incumbent covering a down peer).
    pub fn is_degraded(&self) -> bool {
        self.degraded
    }

    /// The current calibrated healthy commit-wait baseline (µs), if any. For the
    /// engagement log / metrics.
    pub fn baseline_us(&self) -> Option<f64> {
        self.baseline_us
    }

    /// Folds one monitor tick and returns the capacity backlog cap to fold (as a
    /// `min`) into the unified admission setpoint, or `None` for *no clamp* (feature
    /// off, or full capacity with healthy commit latency).
    ///
    /// * `commit_wait_avg_us` — the window-mean commit-wait latency this tick (µs),
    ///   from the delta of `nanobpm_commit_wait_seconds` sum/count. `0` (no commits
    ///   waited this window) leaves the smoothed signal and baseline unchanged.
    /// * `owned` — partitions this node statically owns.
    /// * `led` — partitions this node currently leads. `led > owned` ⇒ this node is
    ///   a failover incumbent (a peer is down); `led < owned` ⇒ some of its
    ///   partitions are led elsewhere (it is not carrying extra load, so no scale-down).
    pub fn observe(&mut self, commit_wait_avg_us: f64, owned: usize, led: usize) -> Option<usize> {
        if !self.cfg.enabled {
            self.degraded = false;
            return None;
        }

        // Low-pass the signal (ignore empty windows so idle ticks don't drag it).
        if commit_wait_avg_us > 0.0 {
            self.ewma_us = Some(match self.ewma_us {
                Some(prev) => {
                    self.cfg.ewma_alpha * commit_wait_avg_us + (1.0 - self.cfg.ewma_alpha) * prev
                }
                None => commit_wait_avg_us,
            });
        }
        let signal = self.ewma_us;

        // Capacity factor: fraction of this node's current leadership duty that is
        // its own. `owned/led` ∈ (0, 1]; < 1 only when it leads more than it owns
        // (covering a down peer), i.e. it is carrying extra Raft load on one disk.
        let factor = if led == 0 {
            1.0
        } else {
            (owned as f64 / led as f64).clamp(0.0, 1.0)
        };
        let degraded = factor < 1.0;

        // The reduced (degraded) ceiling: the full envelope scaled by the capacity
        // factor, never below the floor. While degraded this bounds the AIMD's
        // operating range so admission is proactively held at the smaller envelope.
        let reduced_ceiling = ((self.cfg.ceiling as f64) * factor).max(self.cfg.floor as f64);

        // Calibrate the healthy baseline only at full capacity (snap down to the
        // minimum seen, else creep up slowly). While degraded the baseline is frozen
        // — never learn from the congested regime, or sustained saturation would
        // become the new normal and the governor would stop biting.
        if !degraded && let Some(s) = signal {
            self.baseline_us = Some(match self.baseline_us {
                None => s,
                Some(b) if s < b => s,
                Some(b) => b + (s - b) * self.cfg.baseline_creep,
            });
        }
        let baseline = self.baseline_us.unwrap_or(0.0);
        let threshold = baseline * self.cfg.congestion_ratio;
        let congested = matches!(signal, Some(s) if baseline > 0.0 && s > threshold);

        if congested {
            // Cluster saturating: multiplicative decrease.
            self.cap = (self.cap * self.cfg.backoff).max(self.cfg.floor as f64);
        } else {
            // Headroom (or no baseline yet): additively ease the cap back up toward
            // the (possibly reduced) ceiling. The additive re-arm is what prevents a
            // sudden throughput spike when a peer returns and the ceiling jumps back.
            self.cap = (self.cap + self.cfg.step as f64).min(reduced_ceiling);
        }
        // Proactively hold the cap at/under the reduced ceiling the instant the node
        // becomes degraded — don't wait for the AIMD to walk it down.
        self.cap = self.cap.min(reduced_ceiling);

        self.degraded = degraded;

        // Publish a clamp whenever the cap sits below the full ceiling (actively
        // clamping for capacity or congestion reasons); otherwise no clamp.
        if self.cap < self.cfg.ceiling as f64 {
            Some(self.cap as usize)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> CapacityGovernorCfg {
        CapacityGovernorCfg {
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
    fn no_clamp_at_full_capacity_with_healthy_latency() {
        let mut g = CapacityGovernor::new(cfg());
        // Full capacity (owns 4, leads 4), healthy commit latency: never clamps, and
        // the baseline learns the low commit latency.
        assert_eq!(g.observe(1_000.0, 4, 4), None);
        assert_eq!(g.observe(800.0, 4, 4), None); // snaps baseline down to 800
        assert!(!g.is_engaged());
        assert!(!g.is_degraded());
        assert_eq!(g.baseline_us(), Some(800.0));
    }

    #[test]
    fn peer_down_proactively_shrinks_to_reduced_envelope_immediately() {
        let mut g = CapacityGovernor::new(cfg());
        // Calibrate a healthy baseline at full capacity.
        g.observe(1_000.0, 4, 4);
        // A peer goes down: this node now leads 8 (its 4 + the down peer's 4). The
        // capacity factor is 4/8 = 0.5, so the cap is held at ceiling*0.5 = 20_000
        // on the *very first* degraded tick — no waiting for latency to build. Note
        // commit latency is still healthy here (1ms), proving the shrink is
        // topology-driven, not latency-driven.
        let c = g.observe(1_000.0, 4, 8).unwrap();
        assert_eq!(c, 20_000);
        assert!(g.is_engaged());
        assert!(g.is_degraded());
    }

    #[test]
    fn congestion_at_full_capacity_engages_via_commit_latency() {
        let mut g = CapacityGovernor::new(cfg());
        // Baseline 1ms at full capacity.
        g.observe(1_000.0, 4, 4);
        // Still full capacity, but commit latency inflates past baseline*ratio (5ms
        // >> 2ms): the governor engages purely on the binding signal (fix #2).
        let c1 = g.observe(5_000.0, 4, 4).unwrap();
        let c2 = g.observe(5_000.0, 4, 4).unwrap();
        assert!(c1 < 40_000, "congestion must clamp below the ceiling");
        assert!(
            c2 < c1,
            "sustained congestion keeps shrinking: {c1} -> {c2}"
        );
    }

    #[test]
    fn degraded_holds_below_reduced_ceiling_even_if_latency_reads_healthy() {
        let mut g = CapacityGovernor::new(cfg());
        g.observe(1_000.0, 4, 4); // baseline
        // Enter degraded (factor 0.5, reduced ceiling 20_000) and saturate to floor.
        g.observe(1_000.0, 4, 8);
        for _ in 0..100 {
            g.observe(50_000.0, 4, 8);
        }
        assert_eq!(g.observe(50_000.0, 4, 8), Some(cfg().floor));
        // Latency now reads healthy again but we are STILL degraded: the cap eases up
        // additively yet may never exceed the reduced ceiling (the hold).
        for _ in 0..1_000 {
            g.observe(1_000.0, 4, 8);
        }
        let held = g.observe(1_000.0, 4, 8).unwrap();
        assert_eq!(
            held, 20_000,
            "must hold at the reduced ceiling while degraded"
        );
    }

    #[test]
    fn recovery_rearms_additively_not_instantly() {
        let mut g = CapacityGovernor::new(cfg());
        g.observe(1_000.0, 4, 4); // baseline
        g.observe(1_000.0, 4, 8); // degraded, cap at 20_000
        // Peer returns: full capacity restored. The cap must NOT jump straight back
        // to the full ceiling — it eases up additively (one `step` per tick), so
        // there is no throughput spike to re-trigger the oscillation.
        let first = g.observe(1_000.0, 4, 4).unwrap();
        assert_eq!(first, 20_000 + cfg().step);
        assert!(first < 40_000);
        // Enough healthy ticks and it re-arms fully (no clamp).
        for _ in 0..40 {
            g.observe(1_000.0, 4, 4);
        }
        assert_eq!(g.observe(1_000.0, 4, 4), None);
        assert!(!g.is_engaged());
    }

    #[test]
    fn led_fewer_than_owned_is_not_treated_as_extra_load() {
        let mut g = CapacityGovernor::new(cfg());
        g.observe(1_000.0, 4, 4);
        // This node owns 4 but only leads 2 (two of its partitions are led elsewhere
        // — it is a returning owner still catching up, not carrying extra load). It
        // is NOT scaled down (factor clamps to 1.0), so it does not clamp on that
        // basis alone.
        assert_eq!(g.observe(1_000.0, 4, 2), None);
        assert!(!g.is_degraded());
    }

    #[test]
    fn disabled_never_clamps() {
        let mut c = cfg();
        c.enabled = false;
        let mut g = CapacityGovernor::new(c);
        assert_eq!(g.observe(50_000.0, 4, 8), None);
        assert!(!g.is_engaged());
    }
}
