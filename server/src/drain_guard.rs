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
    /// Floor (‰, parts-per-thousand) on the completion→create mint ratio while the
    /// servo is *draining down* an overshoot (backlog above the setpoint). Normal
    /// metering mints 1 token per completion (1000‰ = intake≈drain, a hold). When
    /// the backlog has overshot the setpoint, the mint ratio drops to
    /// `setpoint/backlog` so intake < drain and the backlog actively shrinks to
    /// the setpoint; this floor bounds how aggressively (never fully starves
    /// intake — that is the hard valve's job). `1000` disables drain-down (pure
    /// hold, the old behaviour).
    pub drain_mint_floor_permille: i64,

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
            // Drain an overshoot down to the setpoint by minting as few as 5% of a
            // token per completion (intake ≈ 5% of drain) when the backlog is far
            // above the setpoint, easing back to 1:1 as it converges.
            drain_mint_floor_permille: 50,

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
    /// * `NANOBPMN_DRAIN_GUARD_MINT_FLOOR_PERMILLE=<0..=1000>` overrides
    ///   [`Self::drain_mint_floor_permille`] (the drain-down mint-ratio floor).
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
        if let Ok(v) = std::env::var("NANOBPMN_DRAIN_GUARD_MINT_FLOOR_PERMILLE")
            && let Ok(n) = v.trim().parse::<i64>()
            && (0..=1000).contains(&n)
        {
            cfg.drain_mint_floor_permille = n;
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
    /// Completion-paced create-admission token bucket. Refilled per completion
    /// (see `mint_permille`, clamped to `capacity`), spent by submission-credit
    /// grants while metering.
    budget: AtomicI64,
    /// Completion→create mint ratio (‰, parts-per-thousand), published each
    /// monitor tick. `1000` = mint one token per completion (intake≈drain, the
    /// metering hold). Below `1000` while draining an overshoot down to the
    /// setpoint (intake < drain). Read on the hot completion path.
    mint_permille: AtomicI64,
    /// Sub-token mint accumulator (‰). `note_completion` adds `mint_permille` here
    /// and mints one whole token each time it crosses 1000, so a fractional mint
    /// ratio is realised without floating point on the hot path.
    mint_acc: AtomicI64,
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
            mint_permille: AtomicI64::new(1000),
            mint_acc: AtomicI64::new(0),
            metering: AtomicBool::new(false),
            halted: AtomicBool::new(false),
            capacity: cfg.burst,
            enabled: cfg.enabled,
        }
    }

    /// Mints one create token into the bucket (clamped to capacity). A short CAS
    /// that no-ops once the bucket is full — the common healthy case.
    #[inline]
    fn mint_one(&self) {
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

    /// Records one drain-side apply and returns create tokens to the bucket at the
    /// published mint ratio (`mint_permille`): one token per completion at the
    /// `1000‰` hold, or a fraction of one while draining an overshoot toward the
    /// setpoint (so intake < drain and the backlog shrinks). Hot-path cheap
    /// (a relaxed add + at most one short CAS).
    #[inline]
    pub fn note_completion(&self) {
        self.completions.fetch_add(1, Ordering::Relaxed);
        let permille = self.mint_permille.load(Ordering::Relaxed);
        if permille >= 1000 {
            // Fast path: full 1:1 mint (metering hold or not draining down).
            self.mint_one();
            return;
        }
        if permille <= 0 {
            // Never mint (would fully starve intake); left to the hard valve.
            return;
        }
        // Fractional mint: accumulate ‰ into a monotonic total and mint one token
        // each time that total crosses a whole-1000 boundary. `permille <= 1000`,
        // so each completion crosses at most one boundary. Deriving the crossing
        // from `fetch_add`'s own before/after is race-safe: each concurrent caller
        // gets an exact, disjoint [before, after) interval, so a given boundary
        // k·1000 is claimed by exactly one caller — no lost or double mints, and
        // (unlike a separate compensating `fetch_sub`) the accumulator never drifts
        // negative under a concurrent completion burst.
        let before = self.mint_acc.fetch_add(permille, Ordering::Relaxed);
        let after = before + permille;
        if after / 1000 > before / 1000 {
            self.mint_one();
        }
    }

    /// Publishes the completion→create mint ratio (‰) the monitor computed this
    /// tick. Called ~1 Hz alongside [`Self::publish`].
    #[inline]
    pub fn set_mint_permille(&self, permille: i64) {
        self.mint_permille
            .store(permille.clamp(0, 1000), Ordering::Relaxed);
    }

    /// Current mint ratio (‰) — for the metric.
    #[inline]
    pub fn mint_permille(&self) -> i64 {
        self.mint_permille.load(Ordering::Relaxed)
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
}

/// The published outcome of one observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DrainDecision {
    /// Servo metering create admission (option 3).
    pub metering: bool,
    /// Hard valve forcing create admission to 0 (option 4).
    pub halted: bool,
    /// Completion→create mint ratio (‰) to publish: `1000` = 1:1 hold, less while
    /// draining an overshoot down to the setpoint (intake < drain).
    pub mint_permille: i64,
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
                mint_permille: 1000,
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
        if !self.metering {
            if s.backlog >= engage_level {
                self.meter_engage_ticks = self.meter_engage_ticks.saturating_add(1);
            } else {
                self.meter_engage_ticks = 0;
            }
            if self.meter_engage_ticks >= self.cfg.meter_engage_ticks {
                self.metering = true;
                self.meter_release_ticks = 0;
            }
        } else {
            if s.backlog <= release_level {
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

        // ---- Drain-down mint ratio -------------------------------------------
        // Metering normally mints 1 create token per completion (a 1:1 *hold* at
        // the current backlog). When the backlog has *overshot* the setpoint we
        // instead mint a fraction — `setpoint/backlog` — so intake < drain and the
        // backlog shrinks back to the setpoint, then reverts to the 1:1 hold. Only
        // engages with a live setpoint (`backlog_cap > 0`); with no cap there is no
        // target to drain toward, so we hold (1000). A halted guard mints nothing
        // (0) — the hard valve owns intake.
        let mint_permille = if self.halted {
            0
        } else if self.metering && s.backlog_cap > 0 && s.backlog > s.backlog_cap {
            let ratio = (s.backlog_cap as i128 * 1000 / s.backlog as i128) as i64;
            ratio.clamp(self.cfg.drain_mint_floor_permille, 1000)
        } else {
            1000
        };

        DrainDecision {
            metering: self.metering,
            halted: self.halted,
            mint_permille,
        }
    }

    #[cfg(test)]
    fn decision(&self) -> DrainDecision {
        DrainDecision {
            metering: self.metering,
            halted: self.halted,
            mint_permille: if self.halted { 0 } else { 1000 },
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
    fn metering_holds_1to1_until_backlog_overshoots_the_setpoint() {
        let c = cfg();
        let mut sm = DrainStateMachine::new(c);
        let cap = 30_000;
        // Drive the servo into metering by holding backlog above the engage band.
        let mut d = DrainDecision {
            metering: false,
            halted: false,
            mint_permille: 1000,
        };
        for _ in 0..(c.meter_engage_ticks + 2) {
            d = sm.observe(healthy_capped(cap * 9 / 10, cap));
        }
        assert!(
            d.metering,
            "sustained backlog in-band must engage the servo"
        );
        // At/below the setpoint: hold at 1:1 (no drain-down).
        let hold = sm.observe(healthy_capped(cap, cap));
        assert_eq!(
            hold.mint_permille, 1000,
            "at the setpoint the servo holds 1:1"
        );
    }

    #[test]
    fn drain_down_mints_a_fraction_proportional_to_the_overshoot() {
        let c = cfg();
        let mut sm = DrainStateMachine::new(c);
        let cap = 30_000;
        // Engage metering, then present a 3x overshoot (backlog 90k vs cap 30k).
        let mut d = DrainDecision {
            metering: false,
            halted: false,
            mint_permille: 1000,
        };
        for _ in 0..(c.meter_engage_ticks + 2) {
            d = sm.observe(healthy_capped(cap * 9 / 10, cap));
        }
        assert!(d.metering);
        let over = sm.observe(healthy_capped(90_000, cap));
        // setpoint/backlog = 30000/90000 = 333‰ (intake ≈ 1/3 of drain).
        assert_eq!(
            over.mint_permille, 333,
            "mint ratio tracks setpoint/backlog"
        );
        // A huge overshoot is clamped to the configured drain floor, never 0.
        let deep = sm.observe(healthy_capped(10_000_000, cap));
        assert_eq!(
            deep.mint_permille, c.drain_mint_floor_permille,
            "a deep overshoot is floored, not fully starved"
        );
    }

    #[test]
    fn no_setpoint_means_no_drain_down() {
        let c = cfg();
        let mut sm = DrainStateMachine::new(c);
        // Absolute-band mode (backlog_cap == 0): engage metering on a deep backlog.
        let mut d = DrainDecision {
            metering: false,
            halted: false,
            mint_permille: 1000,
        };
        for _ in 0..(c.meter_engage_ticks + 2) {
            d = sm.observe(healthy(c.meter_engage_backlog + 50_000));
        }
        assert!(d.metering, "deep backlog must engage even without a cap");
        assert_eq!(
            d.mint_permille, 1000,
            "no setpoint => no drain target => 1:1 hold"
        );
    }

    #[test]
    fn note_completion_realises_a_fractional_mint_ratio() {
        let mut c = cfg();
        c.burst = 100;
        let g = DrainGuard::new(c);
        g.take_credits(100); // empty the bucket
        assert_eq!(g.budget(), 0);

        // 500‰: one token minted every two completions.
        g.set_mint_permille(500);
        g.note_completion();
        assert_eq!(g.budget(), 0, "first half-token accumulates, no mint yet");
        g.note_completion();
        assert_eq!(
            g.budget(),
            1,
            "second completion crosses 1000‰ -> one token"
        );
        g.note_completion();
        g.note_completion();
        assert_eq!(g.budget(), 2, "steady 1 token per 2 completions");

        // Back to a 1:1 hold: every completion mints.
        g.set_mint_permille(1000);
        g.note_completion();
        assert_eq!(g.budget(), 3);

        // 0‰: the hard-valve regime mints nothing.
        g.take_credits(100);
        g.set_mint_permille(0);
        for _ in 0..10 {
            g.note_completion();
        }
        assert_eq!(g.budget(), 0, "0‰ mints nothing (hard valve owns intake)");
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
                halted: false,
                mint_permille: 1000
            }
        );
    }
}
