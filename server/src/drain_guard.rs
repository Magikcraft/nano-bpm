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
//! backlog, actor liveness). This guard samples them ~1 Hz and drives two nested
//! create-admission brakes, cheapest first:
//!
//! * **Soft throttle (option 3) — a completion-paced admission servo.** While the
//!   active backlog sits in a pressure band, new-instance admission is paced by a
//!   *token bucket* refilled by completions: each completed job returns one create
//!   token (bounded by a burst), and create-admission (submission-credit grants)
//!   spends them. Intake therefore *structurally cannot outrun drain* — it
//!   self-limits to the sustainable completion rate. Because it caps the create
//!   *rate* (continuous) rather than hard-shedding at a backlog *level* (on/off),
//!   it settles at intake≈drain instead of oscillating the way a fixed level floor
//!   does. Below the band the bucket is pinned full, so healthy load is never
//!   metered.
//!
//! * **Hard safety valve (option 4).** When the drain has genuinely *stalled*
//!   (~0 completes/s) while a meaningful backlog is held and the actor is still
//!   alive — the wedge signature — force create admission to **0** until the drain
//!   recovers. Defense in depth: guarantees no wedge even if the servo mistunes.
//!
//! Pacing (or, for the hard valve, blocking) creates at admission — before they
//! enter Raft — stops feeding create entries into the shared log faster than
//! completions drain, so the queued/arriving completions get the disk+commit
//! bandwidth and the drain keeps up. A create that is never admitted is never
//! journaled, so durability / at-least-once are intact — the client's submission
//! window simply stalls (or it retries after a backoff), exactly like every other
//! backpressure signal.
//!
//! Both brakes are a **liveness rail, not a latency policy**, so they apply in
//! *both* [`crate::backpressure::SlaMode`]s — even "start every process" cannot be
//! allowed to wedge the partition.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

/// Tunables for the drain-stall guard. Defaults are deliberately conservative so
/// the guard only bites on a real drain collapse, never on healthy backlog churn.
/// All are overridable via the environment (see [`from_env`](DrainGuardCfg::from_env)).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DrainGuardCfg {
    /// Master switch. `false` makes the guard inert (never paces or blocks a create).
    pub enabled: bool,

    // ---- Soft throttle: the completion-paced admission servo (option 3) --------
    /// Active backlog at or above which the servo *engages* (starts metering create
    /// admission against the completion-fed token bucket). Chosen well above the
    /// healthy steady-state backlog so normal load is never metered.
    pub meter_engage_backlog: i64,
    /// Active backlog at or below which an engaged servo *releases* (stops metering
    /// and re-pins the bucket full). Strictly below `meter_engage_backlog` so the
    /// band has hysteresis and the servo cannot flap on the boundary.
    pub meter_release_backlog: i64,
    /// Consecutive qualifying ticks before the servo engages (debounces a blip).
    pub meter_engage_ticks: u32,
    /// Consecutive qualifying ticks before the servo releases (hysteresis).
    pub meter_release_ticks: u32,
    /// When the unified setpoint ([`DrainSample::backlog_cap`]) is active (`> 0`),
    /// the servo *engages* at this percentage of it (tracking the live cap instead
    /// of the absolute `meter_engage_backlog`). Chosen below 100% so the
    /// completion-paced credit servo starts pacing *before* the backlog reaches the
    /// cap, holding it at the setpoint rather than overshooting into the shed.
    pub meter_engage_frac_pct: u32,
    /// When the setpoint is active, the servo *releases* below this percentage of
    /// it. Strictly below `meter_engage_frac_pct` for hysteresis.
    pub meter_release_frac_pct: u32,
    /// Token-bucket burst: the most create tokens the servo will hold. While
    /// metering, admitted-but-undrained creates can lead completions by at most
    /// this much, so it bounds the backlog overshoot above the engage level.
    pub burst: i64,

    // ---- Hard safety valve (option 4) -----------------------------------------
    /// Completion rate (completes/s) at or below which the drain counts as
    /// *stalled* for the hard valve. Effectively "~0".
    pub halt_completes_floor: f64,
    /// Completion rate (completes/s) at or above which a *halted* guard counts the
    /// drain as recovered. Strictly greater than `halt_completes_floor` so
    /// engage/release have a hysteresis band and the valve cannot flap.
    pub recover_completes_rate: f64,
    /// Minimum active backlog before the hard valve may arm. Below this a
    /// `completes==0` sample is just an idle engine, not a wedge.
    pub halt_min_backlog: i64,
    /// When the unified setpoint ([`DrainSample::backlog_cap`]) is active, the hard
    /// valve only arms once the backlog reaches this percentage of it (floored at
    /// `halt_min_backlog`). Kept well above 100% so the completion-paced servo owns
    /// the normal operating band and the hard valve is a genuine-wedge backstop
    /// only — it never fires just because the backlog sits at the setpoint.
    pub halt_arm_mult_pct: u32,
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

            // Healthy per-node backlog stays in the low hundreds even at the 50KB
            // knee; the create-flood wedge ran to tens of thousands. Engage the
            // servo well above healthy churn, release with a wide hysteresis band.
            meter_engage_backlog: 5_000,
            meter_release_backlog: 2_000,
            meter_engage_ticks: 2,
            meter_release_ticks: 3,
            // When the unified setpoint is live the band tracks it: engage the
            // completion-paced servo at 75% of the cap and release below 50%, so
            // intake is paced to drain *before* the backlog reaches the cap.
            meter_engage_frac_pct: 75,
            meter_release_frac_pct: 50,
            // Allow a few thousand admitted creates to lead the drain before the
            // bucket empties; bounds the backlog overshoot while metering.
            burst: 4_000,

            // ~0 completes/s: a stalled drain.
            halt_completes_floor: 1.0,
            // Well clear of the floor so release needs a genuine recovery.
            recover_completes_rate: 10.0,
            // Enough live instances that a total completion stall is a real wedge.
            halt_min_backlog: 2_000,
            // When a setpoint is live, only arm the hard valve at 4x the cap — far
            // above the servo's operating band — so it is a genuine-wedge backstop,
            // not a level that fires whenever the backlog sits at the setpoint.
            halt_arm_mult_pct: 400,
            // Wait ~3 s of sustained stall before cutting intake entirely.
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
    /// * `NANOBPMN_DRAIN_GUARD_ENGAGE_BACKLOG=<n>` overrides [`Self::meter_engage_backlog`]
    ///   (and clamps [`Self::meter_release_backlog`]/[`Self::halt_min_backlog`] below it).
    /// * `NANOBPMN_DRAIN_GUARD_BURST=<n>` overrides [`Self::burst`].
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
        if let Ok(v) = std::env::var("NANOBPMN_DRAIN_GUARD_ENGAGE_BACKLOG")
            && let Ok(n) = v.trim().parse::<i64>()
            && n > 0
        {
            cfg.meter_engage_backlog = n;
            // Keep the release/halt levels sane relative to the engage level.
            cfg.meter_release_backlog = cfg.meter_release_backlog.min(n / 2);
            cfg.halt_min_backlog = cfg.halt_min_backlog.min(n);
        }
        if let Ok(v) = std::env::var("NANOBPMN_DRAIN_GUARD_BURST")
            && let Ok(n) = v.trim().parse::<i64>()
            && n > 0
        {
            cfg.burst = n;
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

/// The live, lock-free state the create-admission hot path reads: the monotonic
/// completion counter the drain rate is derived from, the completion-fed token
/// bucket the servo spends, and the two monitor-published flags.
///
/// The `metering`/`halted` flags are written *only* by the single ~1 Hz monitor
/// supervisor (via [`Self::publish`]); the token bucket is refilled on the
/// completion path ([`Self::note_completion`]) and spent on the credit-grant path
/// ([`Self::take_credits`]). All are relaxed atomics — no lock on the
/// create/complete critical path.
pub struct DrainGuard {
    /// Monotonic count of drain-side (completion-family) command applies —
    /// completes / fails / throws — across every protocol. Its rate of change is
    /// the engine's true drain throughput. Bumped next to the
    /// `nanobpm_job_completions_total` metric so the two never drift.
    completions: AtomicU64,
    /// Completion-paced create-admission token bucket. Refilled +1 per completion
    /// (clamped to `capacity`), spent by submission-credit grants while metering.
    budget: AtomicI64,
    /// Servo engaged (option 3): backlog in the pressure band; create admission is
    /// paced against `budget`.
    metering: AtomicBool,
    /// Hard valve engaged (option 4): drain stalled with backlog held; create
    /// admission is forced to 0.
    halted: AtomicBool,
    /// Token-bucket capacity (burst). Fixed for the process lifetime.
    capacity: i64,
    enabled: bool,
}

impl DrainGuard {
    /// Builds a guard from the resolved config. When disabled it is permanently
    /// inert. The bucket starts full so a cold, healthy start is never metered
    /// before the first monitor tick.
    pub fn new(cfg: DrainGuardCfg) -> Self {
        Self {
            completions: AtomicU64::new(0),
            budget: AtomicI64::new(cfg.burst),
            metering: AtomicBool::new(false),
            halted: AtomicBool::new(false),
            capacity: cfg.burst,
            enabled: cfg.enabled,
        }
    }

    /// Records one drain-side apply and returns one create token to the bucket
    /// (clamped to capacity). Hot-path cheap (a relaxed add + a short CAS that
    /// no-ops once the bucket is full — the common healthy case).
    #[inline]
    pub fn note_completion(&self) {
        self.completions.fetch_add(1, Ordering::Relaxed);
        let cap = self.capacity;
        let mut cur = self.budget.load(Ordering::Relaxed);
        while cur < cap {
            match self.budget.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }

    /// Current monotonic completion count (the monitor samples this each tick).
    #[inline]
    pub fn completions(&self) -> u64 {
        self.completions.load(Ordering::Relaxed)
    }

    /// Spends up to `want` create tokens, returning how many were granted. Called
    /// from the submission-credit grant path *only while metering* — it is the
    /// point at which intake is paced to the completion-fed bucket. A CAS loop; no
    /// lock.
    #[inline]
    pub fn take_credits(&self, want: i64) -> i64 {
        if want <= 0 {
            return 0;
        }
        let mut cur = self.budget.load(Ordering::Relaxed);
        loop {
            if cur <= 0 {
                return 0;
            }
            let grant = want.min(cur);
            match self.budget.compare_exchange_weak(
                cur,
                cur - grant,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return grant,
                Err(v) => cur = v,
            }
        }
    }

    /// Re-pins the bucket to full capacity. Called by the monitor each tick the
    /// servo is *not* metering, so entering the pressure band always starts with a
    /// full burst rather than a stale (possibly empty) bucket.
    #[inline]
    pub fn refill_full(&self) {
        self.budget.store(self.capacity, Ordering::Relaxed);
    }

    /// Current token-bucket level (for the metric).
    #[inline]
    pub fn budget(&self) -> i64 {
        self.budget.load(Ordering::Relaxed)
    }

    /// Whether new-instance admission is *hard-blocked* right now — the hard valve
    /// only. The soft servo does not block; it paces via [`Self::take_credits`].
    /// Always `false` when the guard is disabled.
    #[inline]
    pub fn blocks_creates(&self) -> bool {
        self.enabled && self.halted.load(Ordering::Relaxed)
    }

    /// Whether the hard safety valve is engaged.
    #[inline]
    pub fn is_halted(&self) -> bool {
        self.enabled && self.halted.load(Ordering::Relaxed)
    }

    /// Whether the servo is metering create admission (pressure band active). When
    /// `true`, credit grants must be sized via [`Self::take_credits`].
    #[inline]
    pub fn is_metering(&self) -> bool {
        self.enabled && self.metering.load(Ordering::Relaxed)
    }

    /// Publishes the supervisor's freshly computed decision. Called ~1 Hz from the
    /// monitor tick only.
    pub fn publish(&self, metering: bool, halted: bool) {
        self.metering.store(metering, Ordering::Relaxed);
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
    /// The live unified admission setpoint (the effective active-backlog cap the
    /// throttle converges intake to — `min(latency, memory)` clamped). When `> 0`
    /// the servo's engage/release band and the hard valve's arm level track it as
    /// fractions/multiples, so the completion-paced credit servo holds the backlog
    /// at the setpoint in *both* SLA modes (that is what makes intake converge on
    /// drain instead of the post-credit shed oscillating). `0` (no active cap)
    /// falls back to the absolute `meter_*_backlog` / `halt_min_backlog` levels.
    pub backlog_cap: i64,
    /// Whether at least one owned partition's engine actor is alive. A *dead*
    /// actor is a different failure (crash/panic) handled elsewhere; the valve
    /// must not fire on it (cutting creates would not revive a dead thread).
    pub actor_alive: bool,
    /// Force the completion-paced servo to engage regardless of backlog level.
    /// Driven by the capacity governor while it is engaged (a peer is down or
    /// commit-latency is congested). This workload keeps the active-instance
    /// backlog far below the setpoint floor, so the level-triggered band never
    /// arms on its own; forcing it here paces create *intake* to the completion
    /// rate (plus the burst) during capacity degradation instead of letting the
    /// loadgen's deep inflight buffer amplify the completion ripple into intake
    /// swings. Only affects *engage*: the hard valve and release hysteresis are
    /// unchanged, and release still requires `!force_meter`.
    pub force_meter: bool,
}

/// The published outcome of one observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DrainDecision {
    /// Servo metering create admission (option 3).
    pub metering: bool,
    /// Hard valve forcing create admission to 0 (option 4).
    pub halted: bool,
}

/// Pure, tick-driven decision engine for the drain-stall guard. Holds only the
/// small amount of edge/hysteresis state the single monitor task needs; kept out
/// of [`DrainGuard`] so it never needs a lock and is trivially unit-testable.
#[derive(Debug)]
pub struct DrainStateMachine {
    cfg: DrainGuardCfg,
    metering: bool,
    halted: bool,
    /// Consecutive ticks the servo *engage* precondition (backlog ≥ engage) held.
    meter_engage_ticks: u32,
    /// Consecutive ticks the servo *release* precondition (backlog ≤ release) held.
    meter_release_ticks: u32,
    /// Consecutive ticks the hard-valve *engage* precondition held.
    halt_stall_ticks: u32,
    /// Consecutive ticks the hard-valve *release* precondition held.
    halt_recover_ticks: u32,
}

impl DrainStateMachine {
    pub fn new(cfg: DrainGuardCfg) -> Self {
        Self {
            cfg,
            metering: false,
            halted: false,
            meter_engage_ticks: 0,
            meter_release_ticks: 0,
            halt_stall_ticks: 0,
            halt_recover_ticks: 0,
        }
    }

    /// Folds one sample into the guard state and returns the decision to publish.
    ///
    /// Design notes:
    /// * The servo engages/releases on the backlog *level* with a wide hysteresis
    ///   band. Unlike a hard shed at a level, engaging does not stop intake — it
    ///   switches admission onto the completion-fed token bucket, which paces the
    ///   *rate* and therefore settles rather than oscillates.
    /// * The hard valve is a strict superset action of the servo: whenever it
    ///   engages, metering is asserted too (a stalled drain is, by definition,
    ///   under pressure), so releasing the valve does not momentarily open the gate
    ///   wide before the servo re-evaluates.
    pub fn observe(&mut self, s: DrainSample) -> DrainDecision {
        if !self.cfg.enabled {
            return DrainDecision {
                metering: false,
                halted: false,
            };
        }

        // ---- Resolve the operating levels against the live unified setpoint. ----
        // When a setpoint is active (`backlog_cap > 0`) the servo band and the hard
        // valve arm level track it (as fractions/multiple), so the throttle paces
        // intake to the *current* memory/latency-clamped cap in both SLA modes.
        // With no active cap (`0`) we fall back to the absolute configured levels.
        let (engage_level, release_level, halt_min) = if s.backlog_cap > 0 {
            let cap = s.backlog_cap as i128;
            let engage = (cap * self.cfg.meter_engage_frac_pct as i128 / 100) as i64;
            let release = (cap * self.cfg.meter_release_frac_pct as i128 / 100) as i64;
            let halt = ((cap * self.cfg.halt_arm_mult_pct as i128 / 100) as i64)
                .max(self.cfg.halt_min_backlog);
            (engage, release, halt)
        } else {
            (
                self.cfg.meter_engage_backlog,
                self.cfg.meter_release_backlog,
                self.cfg.halt_min_backlog,
            )
        };

        // ---- Soft throttle (option 3): completion-paced servo, banded. ---------
        // `force_meter` (capacity governor engaged) arms the servo regardless of
        // the backlog level, and holds it armed until the force clears — the
        // level-triggered band cannot arm on its own for a workload whose backlog
        // sits below the setpoint floor.
        if !self.metering {
            if s.force_meter || s.backlog >= engage_level {
                self.meter_engage_ticks = self.meter_engage_ticks.saturating_add(1);
            } else {
                self.meter_engage_ticks = 0;
            }
            if self.meter_engage_ticks >= self.cfg.meter_engage_ticks {
                self.metering = true;
                self.meter_release_ticks = 0;
            }
        } else {
            if !s.force_meter && s.backlog <= release_level {
                self.meter_release_ticks = self.meter_release_ticks.saturating_add(1);
            } else {
                self.meter_release_ticks = 0;
            }
            if self.meter_release_ticks >= self.cfg.meter_release_ticks {
                self.metering = false;
                self.meter_engage_ticks = 0;
            }
        }

        // ---- Hard valve (option 4): sustained drain stall with backlog held. ---
        let stalled = s.completes_per_sec <= self.cfg.halt_completes_floor
            && s.actor_alive
            && s.backlog >= halt_min;
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

        // A halted guard is, by definition, also metering.
        if self.halted {
            self.metering = true;
        }

        DrainDecision {
            metering: self.metering,
            halted: self.halted,
        }
    }

    #[cfg(test)]
    fn decision(&self) -> DrainDecision {
        DrainDecision {
            metering: self.metering,
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

    fn healthy(backlog: i64) -> DrainSample {
        DrainSample {
            completes_per_sec: 800.0,
            backlog,
            backlog_cap: 0,
            actor_alive: true,
            force_meter: false,
        }
    }

    /// A healthy sample carrying a live unified setpoint (`backlog_cap`), used to
    /// exercise the cap-tracking servo band / hard-valve arm level.
    fn healthy_capped(backlog: i64, cap: i64) -> DrainSample {
        DrainSample {
            completes_per_sec: 800.0,
            backlog,
            backlog_cap: cap,
            actor_alive: true,
            force_meter: false,
        }
    }

    #[test]
    fn idle_and_healthy_never_engages() {
        let mut sm = DrainStateMachine::new(cfg());
        for _ in 0..20 {
            let d = sm.observe(healthy(0));
            assert!(!d.metering && !d.halted);
        }
        // Healthy load with a modest backlog well below the engage level.
        for _ in 0..20 {
            let d = sm.observe(healthy(400));
            assert!(!d.metering && !d.halted, "healthy churn must not meter");
        }
    }

    #[test]
    fn servo_engages_above_band_and_releases_below_with_hysteresis() {
        let c = cfg();
        let mut sm = DrainStateMachine::new(c);
        // Cross the engage level: needs `meter_engage_ticks` consecutive ticks.
        let mut d = sm.observe(healthy(c.meter_engage_backlog));
        assert!(!d.metering, "one tick above must not engage");
        for _ in 0..c.meter_engage_ticks {
            d = sm.observe(healthy(c.meter_engage_backlog + 100));
        }
        assert!(
            d.metering,
            "sustained backlog above band must engage the servo"
        );

        // Sitting inside the band (between release and engage) keeps it engaged
        // (hysteresis — no flapping).
        for _ in 0..10 {
            d = sm.observe(healthy(
                (c.meter_engage_backlog + c.meter_release_backlog) / 2,
            ));
            assert!(d.metering, "mid-band must not release");
        }

        // Drop below the release level for the dwell: releases.
        for _ in 0..c.meter_release_ticks {
            d = sm.observe(healthy(c.meter_release_backlog - 100));
        }
        assert!(
            !d.metering,
            "backlog below release band must release the servo"
        );
    }

    #[test]
    fn force_meter_engages_below_band_and_holds_until_cleared() {
        // The capacity governor drives `force_meter` while a peer is down /
        // commit-latency is congested. It must arm the completion-paced servo even
        // though the active backlog sits far below the engage band, and hold it
        // armed until the force clears — this is the lever the backlog-count band
        // cannot reach for a fast create->complete workload.
        let c = cfg();
        let mut sm = DrainStateMachine::new(c);

        let forced = DrainSample {
            completes_per_sec: 800.0,
            backlog: 200, // far below the engage band
            backlog_cap: 0,
            actor_alive: true,
            force_meter: true,
        };
        let mut d = sm.observe(forced);
        assert!(!d.metering, "one forced tick must not engage (debounce)");
        for _ in 0..c.meter_engage_ticks {
            d = sm.observe(forced);
        }
        assert!(
            d.metering,
            "sustained force_meter must engage the servo despite a tiny backlog"
        );

        // While forced, a low backlog must NOT release it (force holds the servo).
        for _ in 0..(c.meter_release_ticks + 5) {
            d = sm.observe(forced);
            assert!(d.metering, "force_meter must hold metering engaged");
        }

        // Clearing the force lets the normal (below-band) release fire.
        let cleared = DrainSample {
            force_meter: false,
            ..forced
        };
        for _ in 0..c.meter_release_ticks {
            d = sm.observe(cleared);
        }
        assert!(
            !d.metering,
            "once force clears, a below-band backlog releases the servo"
        );
    }

    #[test]
    fn servo_band_tracks_the_live_setpoint() {
        // With a live unified setpoint the band is a fraction of the *cap*, not the
        // absolute meter_*_backlog levels. Pick a cap whose 75%/50% band sits well
        // below the absolute defaults so we know the cap path (not the fallback) is
        // driving the decision.
        let c = cfg();
        let cap = 1_000; // engage @750, release @500 — both < absolute 5000/2000.
        let engage = cap * c.meter_engage_frac_pct as i64 / 100;
        let release = cap * c.meter_release_frac_pct as i64 / 100;
        let mut sm = DrainStateMachine::new(c);

        // Backlog above the absolute release floor but below the cap's engage band
        // must NOT meter (proves we track the cap, not the 5000 absolute level).
        for _ in 0..(c.meter_engage_ticks + 3) {
            let d = sm.observe(healthy_capped(release + 10, cap));
            assert!(!d.metering, "below the cap's engage band must not meter");
        }

        // Cross the cap's engage band for the dwell: engages.
        let mut d = healthy_capped(engage + 50, cap);
        let mut out = sm.observe(d);
        for _ in 0..c.meter_engage_ticks {
            out = sm.observe(d);
        }
        assert!(
            out.metering,
            "backlog above the cap's engage band must meter"
        );

        // Drop below the cap's release band for the dwell: releases.
        d = healthy_capped(release - 10, cap);
        for _ in 0..c.meter_release_ticks {
            out = sm.observe(d);
        }
        assert!(!out.metering, "below the cap's release band must release");
    }

    #[test]
    fn hard_valve_only_arms_above_the_cap_multiple_not_at_the_setpoint() {
        // A stalled drain sitting *at* the setpoint (backlog == cap) must NOT halt:
        // the completion-paced servo owns that band. The hard valve is a backstop
        // that only fires once the backlog blows well past the cap.
        let c = cfg();
        let cap = 3_000; // arm level = cap * 4 = 12_000.
        let mut sm = DrainStateMachine::new(c);

        let stalled_at_cap = DrainSample {
            completes_per_sec: 0.0,
            backlog: cap,
            backlog_cap: cap,
            actor_alive: true,
            force_meter: false,
        };
        for _ in 0..(c.halt_engage_ticks + 5) {
            let d = sm.observe(stalled_at_cap);
            assert!(!d.halted, "a stall at the setpoint must not trip the valve");
        }

        // Blow past the arm multiple: now it is a genuine wedge and must halt.
        let wedged = DrainSample {
            completes_per_sec: 0.0,
            backlog: cap * c.halt_arm_mult_pct as i64 / 100 + 1_000,
            backlog_cap: cap,
            actor_alive: true,
            force_meter: false,
        };
        let mut d = sm.observe(wedged);
        for _ in 0..c.halt_engage_ticks {
            d = sm.observe(wedged);
        }
        assert!(
            d.halted,
            "a stall well above the cap multiple is a real wedge"
        );
    }

    #[test]
    fn hard_valve_engages_on_sustained_stall_and_releases_on_recovery() {
        let c = cfg();
        let mut sm = DrainStateMachine::new(c);
        let stall = DrainSample {
            completes_per_sec: 0.0,
            backlog: c.halt_min_backlog + 5_000,
            backlog_cap: 0,
            actor_alive: true,
            force_meter: false,
        };
        let mut d = sm.observe(stall);
        assert!(!d.halted, "one stalled tick must not halt");
        for _ in 0..c.halt_engage_ticks {
            d = sm.observe(stall);
        }
        assert!(d.halted, "sustained stall must engage the hard valve");
        assert!(d.metering, "a halted guard is also metering");

        // Recovery: completes climb back above the recover rate for the dwell.
        let recover = DrainSample {
            completes_per_sec: c.recover_completes_rate + 50.0,
            backlog: c.halt_min_backlog + 5_000,
            backlog_cap: 0,
            actor_alive: true,
            force_meter: false,
        };
        for _ in 0..c.halt_release_ticks {
            d = sm.observe(recover);
        }
        assert!(!d.halted, "sustained recovery must release the hard valve");
    }

    #[test]
    fn hard_valve_ignores_a_dead_actor() {
        let c = cfg();
        let mut sm = DrainStateMachine::new(c);
        let dead = DrainSample {
            completes_per_sec: 0.0,
            backlog: c.halt_min_backlog + 5_000,
            backlog_cap: 0,
            actor_alive: false,
            force_meter: false,
        };
        for _ in 0..(c.halt_engage_ticks + 5) {
            let d = sm.observe(dead);
            assert!(
                !d.halted,
                "a dead actor is a different failure; must not halt"
            );
        }
    }

    #[test]
    fn hard_valve_ignores_an_idle_engine_below_min_backlog() {
        let c = cfg();
        let mut sm = DrainStateMachine::new(c);
        let idle = DrainSample {
            completes_per_sec: 0.0,
            backlog: c.halt_min_backlog - 1,
            backlog_cap: 0,
            actor_alive: true,
            force_meter: false,
        };
        for _ in 0..(c.halt_engage_ticks + 5) {
            let d = sm.observe(idle);
            assert!(!d.halted, "quiet engine below min backlog is not a wedge");
        }
    }

    #[test]
    fn disabled_is_inert() {
        let mut c = cfg();
        c.enabled = false;
        let mut sm = DrainStateMachine::new(c);
        let stall = DrainSample {
            completes_per_sec: 0.0,
            backlog: 1_000_000,
            backlog_cap: 0,
            actor_alive: true,
            force_meter: false,
        };
        for _ in 0..20 {
            let d = sm.observe(stall);
            assert!(!d.metering && !d.halted, "disabled guard must stay inert");
        }
    }

    #[test]
    fn token_bucket_starts_full_and_paces_by_completion() {
        let mut c = cfg();
        c.burst = 10;
        let g = DrainGuard::new(c);
        assert_eq!(g.budget(), 10, "bucket starts full (burst)");

        // Drain the whole burst.
        assert_eq!(g.take_credits(4), 4);
        assert_eq!(g.take_credits(100), 6, "clamped to remaining");
        assert_eq!(g.take_credits(1), 0, "empty bucket grants nothing");

        // Each completion returns exactly one token, clamped to capacity.
        g.note_completion();
        g.note_completion();
        assert_eq!(g.budget(), 2, "two completions refill two tokens");
        assert_eq!(
            g.take_credits(5),
            2,
            "only the refilled tokens are grantable"
        );

        // Refill past capacity is clamped.
        for _ in 0..100 {
            g.note_completion();
        }
        assert_eq!(g.budget(), 10, "refill clamped to burst");
    }

    #[test]
    fn refill_full_repins_the_bucket() {
        let mut c = cfg();
        c.burst = 8;
        let g = DrainGuard::new(c);
        assert_eq!(g.take_credits(8), 8);
        assert_eq!(g.budget(), 0);
        g.refill_full();
        assert_eq!(g.budget(), 8, "refill_full re-pins to capacity");
    }

    #[test]
    fn guard_flags_reflect_published_decision() {
        let g = DrainGuard::new(cfg());
        assert!(!g.is_metering() && !g.is_halted() && !g.blocks_creates());

        g.publish(true, false); // metering only
        assert!(g.is_metering());
        assert!(!g.is_halted(), "metering is not a hard block");
        assert!(!g.blocks_creates(), "servo paces, it does not block");

        g.publish(true, true); // halted
        assert!(g.is_halted() && g.blocks_creates());

        g.publish(false, false);
        assert!(!g.is_metering() && !g.is_halted() && !g.blocks_creates());
    }

    #[test]
    fn disabled_guard_never_blocks_or_meters() {
        let mut c = cfg();
        c.enabled = false;
        let g = DrainGuard::new(c);
        g.publish(true, true); // even if (spuriously) published
        assert!(!g.is_metering(), "disabled guard never meters");
        assert!(
            !g.is_halted() && !g.blocks_creates(),
            "disabled guard never blocks"
        );
    }

    #[test]
    fn take_credits_is_a_noop_for_nonpositive_want() {
        let g = DrainGuard::new(cfg());
        let before = g.budget();
        assert_eq!(g.take_credits(0), 0);
        assert_eq!(g.take_credits(-5), 0);
        assert_eq!(g.budget(), before, "no tokens spent");
    }

    #[test]
    fn completion_counter_is_monotonic() {
        let g = DrainGuard::new(cfg());
        assert_eq!(g.completions(), 0);
        for i in 1..=50 {
            g.note_completion();
            assert_eq!(g.completions(), i);
        }
    }

    #[test]
    fn servo_stays_released_while_flapping_inside_the_band() {
        // Backlog oscillating strictly inside (release, engage) must never engage.
        let c = cfg();
        let mut sm = DrainStateMachine::new(c);
        let lo = c.meter_release_backlog + 1;
        let hi = c.meter_engage_backlog - 1;
        for i in 0..40 {
            let b = if i % 2 == 0 { lo } else { hi };
            let d = sm.observe(healthy(b));
            assert!(!d.metering, "mid-band flap must not engage the servo");
        }
        assert_eq!(
            sm.decision(),
            DrainDecision {
                metering: false,
                halted: false
            }
        );
    }
}
