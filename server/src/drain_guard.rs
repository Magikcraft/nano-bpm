//! Drain-stall admission guard — the create-flood wedge protection.
//!
//! # The wedge
//!
//! Creates and completes share one FIFO Raft log per partition. The
//! completion-priority mailbox ([`crate::deepthi::Priority`]) only orders work
//! *inside* the local engine actor — it does nothing about Raft commit order.
//! Under a large-payload create flood, `CreateInstance` entries saturate the
//! commit+disk pipeline (disk pinned, commit oscillating), `CompleteJob` entries
//! queue behind them, completion latency runs away, workers stall, the
//! active-instance backlog explodes, and the partition collapses. The engine
//! actor samples *idle* through this — it is starved of committed completion
//! work, not internally wedged.
//!
//! # The guard
//!
//! The signals to close the loop already exist (completion count, active
//! backlog, actor liveness). This guard samples them ~1 Hz and drives two
//! nested create-admission brakes, cheapest first:
//!
//! * **Soft throttle (option 3).** When the completion drain is *falling behind*
//!   a live create rate — the active backlog rising while creates keep arriving —
//!   throttle new-instance admission (shed creates / dry up submission credit) so
//!   intake self-limits to the sustainable drain *before* a wedge forms. Gated on
//!   the backlog *derivative* with hysteresis, so it does not oscillate the way a
//!   fixed backlog *level* floor does.
//!
//! * **Hard safety valve (option 4).** When the drain has genuinely *stalled*
//!   (~0 completes/s) while the backlog is rising and the actor is still alive —
//!   the wedge signature — force create admission to **0** until the drain
//!   recovers. Defense in depth: guarantees no wedge even if the soft servo
//!   mistunes.
//!
//! Blocking creates at admission (before they enter Raft) stops feeding create
//! entries into the shared log, so the queued/arriving completions get the
//! disk+commit bandwidth and the drain recovers. A blocked create is never
//! journaled, so durability / at-least-once are intact — the client simply
//! retries after a backoff, exactly like every other admission-shed rail.
//!
//! Both brakes are a **liveness rail, not a latency policy**, so they apply in
//! *both* [`crate::backpressure::SlaMode`]s — even "start every process" cannot
//! be allowed to wedge the partition.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Tunables for the drain-stall guard. Defaults are deliberately conservative so
/// the guard only bites on a real drain collapse, never on healthy backlog
/// churn. All are overridable via the environment (see
/// [`from_env`](DrainGuardCfg::from_env)).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DrainGuardCfg {
    /// Master switch. `false` makes the guard inert (never blocks a create).
    pub enabled: bool,
    /// Completion rate (completes/s) at or below which the drain counts as
    /// *stalled* for the hard valve. Effectively "~0"; a small positive value
    /// tolerates a trickle without arming the hard halt (the soft throttle still
    /// engages there).
    pub halt_completes_floor: f64,
    /// Completion rate (completes/s) at or above which a *halted* guard counts
    /// the drain as recovered. Strictly greater than `halt_completes_floor`, so
    /// engage/release have a hysteresis band and the valve cannot flap.
    pub recover_completes_rate: f64,
    /// Minimum active backlog before the hard valve may arm. Below this there is
    /// no meaningful work to protect, so a `completes==0` sample is just an idle
    /// engine, not a wedge.
    pub halt_min_backlog: i64,
    /// Per-tick backlog rise (instances/tick) above which the backlog counts as
    /// *rising* — the derivative signal both brakes key off. A small positive
    /// threshold ignores single-instance jitter.
    pub rising_threshold: i64,
    /// Consecutive qualifying ticks before the soft throttle engages. Debounces a
    /// one-tick blip.
    pub throttle_engage_ticks: u32,
    /// Consecutive non-rising ticks before the soft throttle releases (hysteresis).
    pub throttle_release_ticks: u32,
    /// Consecutive qualifying ticks before the hard valve engages (~seconds of a
    /// sustained stall, not a momentary dip).
    pub halt_engage_ticks: u32,
    /// Consecutive recovered ticks before the hard valve releases (hysteresis).
    pub halt_release_ticks: u32,
}

impl Default for DrainGuardCfg {
    fn default() -> Self {
        Self {
            enabled: true,
            // ~0 completes/s: a stalled drain. A trickle above this only trips the
            // soft throttle, not the hard valve.
            halt_completes_floor: 1.0,
            // Well clear of the floor so release needs a genuine recovery.
            recover_completes_rate: 10.0,
            // Enough live instances that a total completion stall is a real wedge,
            // not a quiet engine.
            halt_min_backlog: 200,
            // Ignore 1-2 instance jitter; require a real climb.
            rising_threshold: 4,
            // Soft throttle reacts fast (2 s) — it is cheap and reversible.
            throttle_engage_ticks: 2,
            throttle_release_ticks: 3,
            // Hard valve waits ~3 s of sustained stall before cutting intake.
            halt_engage_ticks: 3,
            halt_release_ticks: 2,
        }
    }
}

impl DrainGuardCfg {
    /// Resolves configuration from the environment.
    ///
    /// * `NANOBPMN_DRAIN_GUARD=off|false|no|0` disables the guard entirely.
    ///   Anything else (or unset) leaves it on with the tuned defaults.
    /// * `NANOBPMN_DRAIN_GUARD_MIN_BACKLOG=<n>` overrides [`Self::halt_min_backlog`].
    /// * `NANOBPMN_DRAIN_GUARD_HALT_FLOOR=<f>` overrides [`Self::halt_completes_floor`].
    /// * `NANOBPMN_DRAIN_GUARD_RECOVER_RATE=<f>` overrides [`Self::recover_completes_rate`].
    ///
    /// Invalid values fall back to the default for that field.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Ok(v) = std::env::var("NANOBPMN_DRAIN_GUARD") {
            let v = v.trim().to_ascii_lowercase();
            if matches!(v.as_str(), "off" | "false" | "no" | "0") {
                cfg.enabled = false;
            }
        }
        if let Ok(v) = std::env::var("NANOBPMN_DRAIN_GUARD_MIN_BACKLOG")
            && let Ok(n) = v.trim().parse::<i64>()
            && n >= 0
        {
            cfg.halt_min_backlog = n;
        }
        if let Ok(v) = std::env::var("NANOBPMN_DRAIN_GUARD_HALT_FLOOR")
            && let Ok(f) = v.trim().parse::<f64>()
            && f >= 0.0
        {
            cfg.halt_completes_floor = f;
        }
        if let Ok(v) = std::env::var("NANOBPMN_DRAIN_GUARD_RECOVER_RATE")
            && let Ok(f) = v.trim().parse::<f64>()
            && f > cfg.halt_completes_floor
        {
            cfg.recover_completes_rate = f;
        }
        cfg
    }
}

/// The live, lock-free state the create-admission hot path reads, plus the
/// monotonic completion counter the drain rate is derived from.
///
/// The published `throttling`/`halted` flags are written *only* by the single
/// ~1 Hz monitor supervisor (via [`Self::publish`]) and read with relaxed loads
/// by the admission gates — no contention on the create/complete critical path.
pub struct DrainGuard {
    /// Monotonic count of drain-side (completion-family) command applies —
    /// completes / fails / throws — across every protocol. Its rate of change is
    /// the engine's true drain throughput. Bumped next to the
    /// `nanobpm_job_completions_total` metric so the two never drift.
    completions: AtomicU64,
    /// Soft throttle engaged (option 3): drain falling behind create rate.
    throttling: AtomicBool,
    /// Hard valve engaged (option 4): drain stalled, backlog rising, actor alive.
    halted: AtomicBool,
    enabled: bool,
}

impl DrainGuard {
    /// Builds a guard from the resolved config. When disabled it is permanently
    /// inert.
    pub fn new(cfg: DrainGuardCfg) -> Self {
        Self {
            completions: AtomicU64::new(0),
            throttling: AtomicBool::new(false),
            halted: AtomicBool::new(false),
            enabled: cfg.enabled,
        }
    }

    /// Records one drain-side apply. Hot-path cheap (a single relaxed add).
    #[inline]
    pub fn note_completion(&self) {
        self.completions.fetch_add(1, Ordering::Relaxed);
    }

    /// Current monotonic completion count (the monitor samples this each tick).
    #[inline]
    pub fn completions(&self) -> u64 {
        self.completions.load(Ordering::Relaxed)
    }

    /// Whether new-instance admission should be blocked right now. `true` under
    /// either brake; always `false` when the guard is disabled.
    #[inline]
    pub fn blocks_creates(&self) -> bool {
        self.enabled
            && (self.halted.load(Ordering::Relaxed) || self.throttling.load(Ordering::Relaxed))
    }

    /// Whether the hard safety valve is engaged.
    #[inline]
    pub fn is_halted(&self) -> bool {
        self.enabled && self.halted.load(Ordering::Relaxed)
    }

    /// Whether the soft throttle is engaged.
    #[inline]
    pub fn is_throttling(&self) -> bool {
        self.enabled && self.throttling.load(Ordering::Relaxed)
    }

    /// Publishes the supervisor's freshly computed decision. Called ~1 Hz from
    /// the monitor tick only.
    pub fn publish(&self, throttling: bool, halted: bool) {
        self.throttling.store(throttling, Ordering::Relaxed);
        self.halted.store(halted, Ordering::Relaxed);
    }
}

/// One ~1 Hz observation the state machine decides on.
#[derive(Clone, Copy, Debug)]
pub struct DrainSample {
    /// Completion throughput this window (completes/s), derived from the delta of
    /// [`DrainGuard::completions`] over the tick's elapsed time.
    pub completes_per_sec: f64,
    /// Active (non-terminal) instance backlog right now.
    pub backlog: i64,
    /// Backlog change since the previous tick (`backlog - prev_backlog`).
    pub backlog_delta: i64,
    /// Whether at least one owned partition's engine actor is alive. A *dead*
    /// actor is a different failure (crash/panic) handled elsewhere; the valve
    /// must not fire on it (cutting creates would not revive a dead thread).
    pub actor_alive: bool,
}

/// The published outcome of one observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DrainDecision {
    pub throttling: bool,
    pub halted: bool,
}

/// Pure, tick-driven decision engine for the drain-stall guard. Holds only the
/// small amount of edge/hysteresis state the single monitor task needs; kept out
/// of [`DrainGuard`] so it never needs a lock and is trivially unit-testable.
#[derive(Debug)]
pub struct DrainStateMachine {
    cfg: DrainGuardCfg,
    throttling: bool,
    halted: bool,
    /// Consecutive ticks the soft-throttle *engage* precondition held.
    throttle_rise_ticks: u32,
    /// Consecutive ticks the soft-throttle *release* precondition held.
    throttle_calm_ticks: u32,
    /// Consecutive ticks the hard-valve *engage* precondition held.
    halt_stall_ticks: u32,
    /// Consecutive ticks the hard-valve *release* precondition held.
    halt_recover_ticks: u32,
}

impl DrainStateMachine {
    pub fn new(cfg: DrainGuardCfg) -> Self {
        Self {
            cfg,
            throttling: false,
            halted: false,
            throttle_rise_ticks: 0,
            throttle_calm_ticks: 0,
            halt_stall_ticks: 0,
            halt_recover_ticks: 0,
        }
    }

    /// Folds one sample into the guard state and returns the decision to publish.
    ///
    /// Design notes:
    /// * Both brakes key off the backlog *derivative* (`backlog_delta`), not a
    ///   fixed level, so they track the imbalance between intake and drain
    ///   instead of oscillating around a level floor.
    /// * "Backlog rising" implies creates are being admitted (only a new instance
    ///   grows the active backlog), so an explicit create-rate term is redundant.
    /// * The hard valve is a strict subset of the soft throttle: whenever it
    ///   engages, the throttle is engaged too.
    pub fn observe(&mut self, s: DrainSample) -> DrainDecision {
        if !self.cfg.enabled {
            return DrainDecision {
                throttling: false,
                halted: false,
            };
        }

        let rising = s.backlog_delta >= self.cfg.rising_threshold;

        // ---- Soft throttle (option 3): drain falling behind a rising backlog. --
        if rising {
            self.throttle_rise_ticks = self.throttle_rise_ticks.saturating_add(1);
            self.throttle_calm_ticks = 0;
        } else {
            self.throttle_calm_ticks = self.throttle_calm_ticks.saturating_add(1);
            self.throttle_rise_ticks = 0;
        }
        if !self.throttling {
            if self.throttle_rise_ticks >= self.cfg.throttle_engage_ticks {
                self.throttling = true;
            }
        } else if self.throttle_calm_ticks >= self.cfg.throttle_release_ticks {
            self.throttling = false;
        }

        // ---- Hard valve (option 4): sustained drain stall while backlog rises. -
        let stalled = s.completes_per_sec <= self.cfg.halt_completes_floor
            && rising
            && s.actor_alive
            && s.backlog >= self.cfg.halt_min_backlog;
        let recovered = s.completes_per_sec >= self.cfg.recover_completes_rate;
        if stalled {
            self.halt_stall_ticks = self.halt_stall_ticks.saturating_add(1);
        } else {
            self.halt_stall_ticks = 0;
        }
        if recovered {
            self.halt_recover_ticks = self.halt_recover_ticks.saturating_add(1);
        } else {
            self.halt_recover_ticks = 0;
        }
        if !self.halted {
            if self.halt_stall_ticks >= self.cfg.halt_engage_ticks {
                self.halted = true;
            }
        } else if self.halt_recover_ticks >= self.cfg.halt_release_ticks {
            self.halted = false;
        }

        // A halted guard is, by definition, also throttling: keep the soft brake
        // asserted so releasing the valve does not momentarily open the gate wide
        // before the throttle re-evaluates.
        if self.halted {
            self.throttling = true;
        }

        DrainDecision {
            throttling: self.throttling,
            halted: self.halted,
        }
    }

    #[cfg(test)]
    fn decision(&self) -> DrainDecision {
        DrainDecision {
            throttling: self.throttling,
            halted: self.halted,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> DrainGuardCfg {
        DrainGuardCfg::default()
    }

    fn sample(completes: f64, backlog: i64, delta: i64, alive: bool) -> DrainSample {
        DrainSample {
            completes_per_sec: completes,
            backlog,
            backlog_delta: delta,
            actor_alive: alive,
        }
    }

    #[test]
    fn idle_engine_never_brakes() {
        let mut sm = DrainStateMachine::new(cfg());
        // No work, no arrivals, flat backlog, zero completes: not a wedge.
        for _ in 0..20 {
            let d = sm.observe(sample(0.0, 0, 0, true));
            assert!(!d.throttling, "idle must not throttle");
            assert!(!d.halted, "idle must not halt");
        }
    }

    #[test]
    fn healthy_high_throughput_never_brakes() {
        let mut sm = DrainStateMachine::new(cfg());
        // Big backlog but draining fast and stable: no rise, no brake.
        for _ in 0..20 {
            let d = sm.observe(sample(5000.0, 10_000, 0, true));
            assert_eq!(
                d,
                DrainDecision {
                    throttling: false,
                    halted: false
                }
            );
        }
    }

    #[test]
    fn rising_backlog_engages_then_releases_soft_throttle() {
        let mut sm = DrainStateMachine::new(cfg());
        // Backlog climbing while completes still trickle above the halt floor:
        // soft throttle engages after throttle_engage_ticks, hard valve stays off.
        for _ in 0..cfg().throttle_engage_ticks {
            sm.observe(sample(50.0, 5_000, 100, true));
        }
        assert!(
            sm.decision().throttling,
            "throttle should engage on sustained rise"
        );
        assert!(
            !sm.decision().halted,
            "trickle above halt floor must not halt"
        );
        // Backlog stops rising (drain caught up): throttle releases after hysteresis.
        for _ in 0..cfg().throttle_release_ticks {
            sm.observe(sample(50.0, 5_000, 0, true));
        }
        assert!(
            !sm.decision().throttling,
            "throttle should release once backlog stabilises"
        );
    }

    #[test]
    fn sustained_stall_engages_hard_valve_and_recovers() {
        let mut sm = DrainStateMachine::new(cfg());
        // The wedge: completes ~0, backlog rising, actor alive, above min backlog.
        for _ in 0..cfg().halt_engage_ticks {
            sm.observe(sample(0.0, 5_000, 200, true));
        }
        let d = sm.decision();
        assert!(d.halted, "sustained stall must engage the hard valve");
        assert!(d.throttling, "halt implies throttle");
        // Drain recovers well above the recover rate: valve releases after hysteresis.
        for _ in 0..cfg().halt_release_ticks {
            sm.observe(sample(1_000.0, 5_000, -200, true));
        }
        assert!(
            !sm.decision().halted,
            "strong recovery must release the valve"
        );
    }

    #[test]
    fn one_tick_blip_does_not_engage_hard_valve() {
        let mut sm = DrainStateMachine::new(cfg());
        // A single stalled tick (< halt_engage_ticks) must not fire the valve.
        assert!(cfg().halt_engage_ticks > 1);
        let d = sm.observe(sample(0.0, 5_000, 200, true));
        assert!(!d.halted, "a one-tick stall must not halt");
    }

    #[test]
    fn dead_actor_does_not_engage_hard_valve() {
        let mut sm = DrainStateMachine::new(cfg());
        // Actor dead (crash) — cutting creates cannot revive it, so the valve
        // must not fire; that failure is handled by the raft/actor supervisor.
        for _ in 0..(cfg().halt_engage_ticks + 3) {
            sm.observe(sample(0.0, 5_000, 200, false));
        }
        assert!(
            !sm.decision().halted,
            "dead actor must not engage the create valve"
        );
    }

    #[test]
    fn below_min_backlog_does_not_engage_hard_valve() {
        let mut cfg = cfg();
        cfg.halt_min_backlog = 200;
        let mut sm = DrainStateMachine::new(cfg);
        // Completes ~0 and a small rise, but backlog below the floor: a quiet
        // engine, not a wedge.
        for _ in 0..(cfg.halt_engage_ticks + 3) {
            sm.observe(sample(0.0, 50, 10, true));
        }
        assert!(!sm.decision().halted, "sub-threshold backlog must not halt");
    }

    #[test]
    fn disabled_guard_is_inert() {
        let mut cfg = cfg();
        cfg.enabled = false;
        let mut sm = DrainStateMachine::new(cfg);
        for _ in 0..20 {
            let d = sm.observe(sample(0.0, 100_000, 1_000, true));
            assert_eq!(
                d,
                DrainDecision {
                    throttling: false,
                    halted: false
                }
            );
        }
    }

    #[test]
    fn guard_blocks_creates_reflects_published_state() {
        let g = DrainGuard::new(cfg());
        assert!(!g.blocks_creates());
        g.publish(true, false);
        assert!(g.blocks_creates() && g.is_throttling() && !g.is_halted());
        g.publish(true, true);
        assert!(g.blocks_creates() && g.is_halted());
        g.publish(false, false);
        assert!(!g.blocks_creates());
    }

    #[test]
    fn disabled_guard_never_blocks_even_if_published() {
        let mut c = cfg();
        c.enabled = false;
        let g = DrainGuard::new(c);
        g.publish(true, true);
        assert!(!g.blocks_creates(), "disabled guard must stay inert");
        assert!(!g.is_halted() && !g.is_throttling());
    }

    #[test]
    fn completion_counter_is_monotonic() {
        let g = DrainGuard::new(cfg());
        assert_eq!(g.completions(), 0);
        g.note_completion();
        g.note_completion();
        assert_eq!(g.completions(), 2);
    }

    #[test]
    fn from_env_defaults_enabled() {
        // No env manipulation (tests run in-process): just assert the default is on.
        assert!(DrainGuardCfg::default().enabled);
    }
}
