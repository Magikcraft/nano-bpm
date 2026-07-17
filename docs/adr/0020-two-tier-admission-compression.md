# ADR 0020 — Two-tier admission compression: a global engine-saturation guard + per-process-definition backlog compressors

Status: **Proposed — accepted for implementation.**
Date: 2026-07-17. Revised 2026-07-18 (Tier-2 keyed on per-process-definition
in-flight backlog directly, replacing the per-job-type detect + per-process
fan-in design; Tier-1 signal resolved to raft-log fsync latency).
Relates to: ADR 0013 (SLA modes & the compressor/limiter model), ADR 0017 (worker-concurrency governor), ADR 0012 (decoupling terminal state from exporter lag), `server/src/backpressure.rs`, `server/src/drain_guard.rs`, `server/src/deepthi.rs`, `server/src/main.rs`, `docs/distributed-scaling-design.md`.

## Context

In `NANOBPMN_SLA_MODE=latency` the engine must **shed/pace producer intake to
preserve end-to-end (create→complete) latency**. The actuator is the admission
gate on `createProcessInstance`; the question this ADR settles is *what signal
drives it*.

A long line of designs each keyed the compressor on a single **proxy** for "are
we the bottleneck" and each proxy missed the real constraint:

- **Internal command latency vs a learned baseline** (commit `b259685`): the
  baseline pinned to the first idle window (~5 µs) and froze whenever load pushed
  latency past `baseline × CONGESTION_RATIO`, so under any real load the relative
  threshold was unreachable and the cap ratcheted monotonically to the floor and
  never released — the observed *"90k/2000 clipping"*.
- **Engine-actor saturation ρ** (commit `4d60bfc`, the busy fraction of the
  single-writer engine actor + backlog-trend): correct, absolute, cold-start-proof,
  and immune to external strain — but a **GCP soak proved ρ never approaches
  saturation** for real workloads. The apply loop is microsecond-cheap; under a 3×
  create-flood (150k offered, ~44k landed, ~105k/s backpressured) ρ peaked at 0.58
  and typically sat at 0.2–0.35. The throughput wall is **upstream of the apply
  loop** (the raft propose/batch/log-fsync/commit path), which ρ-of-the-apply-loop
  does not measure. So the ρ compressor stayed inert on genuine overload.

Two structural facts fell out of that soak and the analysis around it:

1. **Little's law is the frame.** Latency `W = L / λ` (backlog ÷ throughput). The
   *absolute* backlog is **not** a latency proxy on its own — the same backlog is
   fine at high throughput and catastrophic at low throughput. The scale-free
   signals are the **backlog trend `dL/dt`** ("is latency growing?", threshold-free)
   and, to bound *absolute* latency, a **throughput-scaled target** `L* = W_target ·
   λ` (so a latency target becomes a backlog band that auto-scales with capacity —
   dissolving the "90k/2000" magic-number problem).

2. **Choosing the metric is choosing the plant boundary (control volume).** ρ draws
   the box around *our engine only* — worker slowness is an external disturbance to
   *ignore* (fault-attribution). Backlog/latency draws the box around the *whole
   create→complete pipeline* — the workers are *inside* it, so their slowness is
   internal dynamics to *regulate* by throttling the one input we own (intake).
   `SLA_MODE=latency` is, by definition, the second contract: preserve e2e latency
   regardless of which component is slow.

The engine is a **general product deployed into heterogeneous environments** with
many process definitions and many independently-scaled worker pools. A single
*global* backlog signal cannot express that heterogeneity: if one workload's
backlog explodes, a global compressor throttles admission of **every** process —
punishing healthy workloads for one sick one. Heterogeneity therefore forces
**per-process-definition decomposition** (regulate each user workload by its own
accumulating backlog).

Finally, the create-flood soak showed a bottleneck class that is **not**
per-definition: a **shared internal write path** (raft commit / disk fsync / apply)
that saturates *all* definitions at once. Per-definition decomposition cannot see
it; it needs a **global**
guard. The two bottleneck classes are orthogonal.

## Decision

Adopt a **two-tier admission compressor**, gated on in latency mode. The final
admission decision is the **intersection** of the two tiers:

```
admit(createProcessInstance P)  ⟺  global_engine_headroom > 0   (Tier 1)
                                AND  p_P < 1                     (Tier 2, P's own backlog)
```

### Tier 1 — Global engine-saturation guard (the shared write path is *ours*)

A single node-level guard that throttles **all** intake when the engine's own
**shared write path** is the bottleneck — the class ρ was reaching for but
mis-located. A read-only GCP diagnostic (150k-offered create-flood, binsha
`5e2e280c`) localized the wall to the **raft-log fsync**: `raft_fsync` ran
~737–790/s at ~2.4 ms latency (aggregate ~1.78 fsync-seconds/s per node, spread
concurrently across the node's partition logs), dwarfing the varstore
`journal_fsync` (~260/s, 0.79 s/s), while **commit-wait stayed ~75 µs**
(batch-amortized — a *poor* signal). So Tier-1 keys on the **raft-log fsync
latency knee** (~2.4–3 ms under load vs sub-ms idle), *not* the apply-loop busy
fraction and *not* commit-wait. The signal path already exists:
`metrics::raft_fsync_sum_count()` (the windowed mean `raft_fsync_avg_us` the
recovery throttle already consumes). This is the correct home for the
*isolated-server / fault-attribution* frame: it fires only when **we** (the
engine's shared write machinery) are saturated, independent of any worker pool.
It is a backstop, not the primary actuator.

**Implementation (`GlobalGuard` in `backpressure.rs`).** A single node-level
compressor on the smoothed raft-fsync latency. It calibrates a healthy baseline
(snap-down-to-min, slow creep-up), *floored* at `baseline_floor_us` (default
0.5 ms) so a near-zero idle baseline can't make the knee trivially crossable;
the knee is `max(baseline, floor) · congestion_ratio` (default 2×). Above the
knee it freezes the baseline and **attacks** (pressure `p ∈ [0,1]` rises,
steepening with the fractional overshoot past the knee); below it, it releases
slowly (attack/release asymmetry damps oscillation). The monitor steps it each
~1 Hz tick with the windowed `fsync_avg_us` (reusing the delta already computed
for the recovery throttle) and only actuates in latency mode; admission sheds a
paced `p·1000` per-mille fraction of **all** creates via a deterministic
accumulator (no RNG), at all three create entry points, checked before Tier-2.
Env: `NANOBPMN_TIER1` (default on) + `NANOBPMN_TIER1_{RATIO,ATTACK,ATTACK_GAIN,
RELEASE,EWMA,CREEP,FLOOR_US}`. Published as `nanobpm_tier1_pressure` (per-mille,
node-level gauge).

### Tier 2 — Per-process-definition backlog compressors

The primary latency-preservation loop. An earlier draft detected congestion per
**job type** and reconstructed a per-process pressure by fanning in
(`max_{t∈types(P)} p_t`) over a statically-derived process→job-type incidence
map. That indirection existed only to answer *"given a shared job type is
backing up, which `createProcessInstance` calls do I throttle?"* — a question
that **dissolves** once we measure a process definition's own backlog directly.
Keying Tier-2 on **per-definition in-flight instance backlog** makes the
detection unit equal the actuation unit (`createProcessInstance(P)` ↔ P's own
backlog), and is strictly *more precise*: if processes `A` and `B` share a
congested job type but only `B` is accumulating, per-definition backlog throttles
only `B`, whereas fan-in would over-throttle healthy `A`.

- **Signal.** For each process definition `P`, let `L_P` = **in-flight instance
  count** (created but not yet terminally completed/terminated), maintained as an
  O(1) increment-on-CreateInstance / decrement-on-terminal-completion counter in
  the engine `State` (hooked at the *logical* lifecycle, **not** the resident
  insert/remove — a spilled instance is still in-flight). `λ_P` = P's create rate
  (monitor differences a cumulative per-definition created counter).
- **Law.** One delay-gradient compressor per definition on `L_P`:
  - fast term: drive `dL_P/dt → 0` (match intake to P's own drain — scale-free,
    no magic number, finds capacity wherever it is);
  - slow term: bias toward the throughput-scaled band `L*_P = W_target · λ_P` to
    bound *absolute* e2e **instance** latency `W_P = L_P/λ_P` (exactly the
    process latency users feel), bleeding a deep-but-stable backlog down.
  Each governor emits a **pressure** `p_P ∈ [0,1]` (0 = healthy, 1 = fully shed);
  EWMA-smoothed with a deadband as a fraction of `λ_P` (raw `dL/dt` is noisy).
- **Actuation.** `admit(createProcessInstance P) ⟺ p_P < 1` (throttled in
  proportion to `p_P`). No incidence map, no fan-in.

Two behaviours are **correct by design**, not bugs:
- a definition whose instances drain healthily is **never** throttled (the entire
  win over a global compressor), even if it shares a job type with a sick one;
- throttling `P` suppresses *all* of `P` (you cannot create "just the healthy
  parts" of an instance) — inherent to instance-granular admission.

Per-job-type congestion remains visible for **operators** via the existing
`nanobpm_job_sojourn_seconds` histogram (which worker pool is the constraint),
but it is **reporting only** — no longer a control input.

### What this replaces / retains

- The ρ + backlog-trend `BacklogGovernor` (commit `4d60bfc`) is **superseded** as the
  primary control. ρ survives as **observability**; the raft-fsync latency signal is
  now the Tier-1 global guard (see Tier 1).
- The **drain-guard credit servo** (`drain_guard.rs`, ADR-0013 lineage) remains the
  **inner** liveness rail. Tiers 1–2 are the **outer** loop; the cascade discipline
  holds (outer must move slower than inner to avoid the historical oscillation).
- Memory-safety rails (create-queue, exporter-queue, resident-byte watermark) are
  unchanged and continue to fire in **all** SLA modes for OOM safety.

## Consequences

- **Behaviour change vs ρ (a deliberate contract flip):** in the external-strain
  regime (slow workers, deep-but-stable backlog) the ρ design stayed *inert*; the
  Tier-2 design *will* throttle intake to bleed that backlog to the latency band.
  This is the correct `SLA_MODE=latency` behaviour (match admission to capacity to
  hold latency) and the opposite of ρ's fault-attribution. `SLA_MODE=admission`
  users are unaffected — Tier 1/2 do not actuate there (memory rails only).
- **Per-definition collateral throttling:** throttling a definition suppresses all
  of its instances (instance-granular admission); a deep-but-stable backlog is bled
  to the latency band. Both inherent and accepted.
- **Cardinality:** per-definition governors are a few atomics each — cheap even at
  hundreds of definitions. `L_P`/`λ_P` are O(1) counters in the engine `State`; no
  BPMN static analysis, no incidence map, no per-edge flow measurement.
- **The one absolute knob is `W_target`** (target e2e latency), which is meaningful
  and portable — unlike an internal-latency µs threshold or an absolute backlog
  count. Everything else (`dL/dt → 0`, `L* = W_target · λ`) is scale-free.
- **Tier-1 signal (resolved):** the read-only diagnostic localized the shared-write
  wall to the **raft-log fsync latency** (~2.4–3 ms under load vs sub-ms idle);
  commit-wait (~75 µs, amortized) and apply-loop ρ (never saturates) were both
  rejected. Tier-1 reuses `metrics::raft_fsync_sum_count()` — no new instrumentation.

## Validation (to be filled in)

GCP 3-node RF=3 P=12, clean journal, latency mode. Required scenarios:

1. **Heterogeneous per-definition isolation:** two process definitions sharing a
   job type; sicken that type's worker pool via a definition that hammers it.
   EXPECT only the *accumulating* definition gets throttled; a sibling definition
   whose instances still drain healthily keeps full admission. (The scenario no
   prior design could pass — and per-definition backlog is *more* precise here than
   the earlier per-type fan-in, which would have over-throttled the healthy one.)
2. **External-strain latency bound:** the old Test A (slow workers, deep backlog).
   EXPECT intake throttled to bleed the backlog to `W_target · λ` (bounded sojourn),
   *no* oscillation.
3. **Shared-write-path overload:** the old Test B (create-flood). EXPECT the Tier-1
   global guard engages on the commit path (once its signal is chosen) and holds a
   stable operating point — no monotonic clamp, no sawtooth.
4. **Cold-start into a storm:** no learning window available. EXPECT correct throttle
   on the first ticks (scale-free signals require no baseline).

## Alternatives considered

- **Single global backlog compressor (aggregate whole-system).** Simpler, but blind
  to *where* the backlog is — one sick workload throttles all admission. Rejected for
  a general/heterogeneous product.
- **Isolated-server ρ as primary (fault-attribution).** Rejected: proven inert on
  real workloads (apply loop never saturates) and it declines to preserve latency
  under external strain, which violates the `SLA_MODE=latency` contract. Retained
  only as observability.
- **Per-job-type detection + per-process fan-in (the earlier draft of this ADR).**
  Detect congestion at each job-type queue, then throttle `createProcessInstance(P)`
  by `max_{t∈types(P)} p_t` over a statically-derived incidence map. Rejected in
  favour of per-definition backlog: it needs BPMN static analysis + an incidence map,
  and it *over-throttles* — a definition sharing a congested type with a sick sibling
  is throttled even when its own instances drain fine. Per-definition backlog makes
  detection unit == actuation unit and only throttles the actually-accumulating
  workload. Per-job-type sojourn is retained as **reporting**.
- **Per-`(process,type)` decomposition.** Even finer attribution of a type's inflow
  across processes, but higher cardinality and needs per-edge flow measurement.
  Unnecessary once control is per-definition.
- **Direct e2e-sojourn probe (perturb-and-observe).** The original operator intuition;
  correct in spirit but e2e sojourn has a long transport dead-time that makes a fast
  probe oscillate. Resolved by using **backlog depth as the fast, dead-time-free
  causal proxy** (Little's law) with sojourn as slow confirmation — which is exactly
  the Tier-2 signal.
