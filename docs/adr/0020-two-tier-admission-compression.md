# ADR 0020 — Two-tier admission compression: a global engine-saturation guard + per-job-type backlog compressors fanned out to per-process throttling

Status: **Proposed — accepted for implementation.**
Date: 2026-07-17.
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
*global* backlog signal cannot express that heterogeneity: if one job type's
workers die and its backlog explodes, a global compressor throttles admission of
**every** process — punishing healthy workloads for one sick one. Heterogeneity
therefore forces **per-job-type decomposition**.

Finally, the create-flood soak showed a bottleneck class that is **not** per-type:
a **shared internal write path** (raft commit / disk fsync / apply) that saturates
*all* types at once. Per-type decomposition cannot see it; it needs a **global**
guard. The two bottleneck classes are orthogonal.

## Decision

Adopt a **two-tier admission compressor**, gated on in latency mode. The final
admission decision is the **intersection** of the two tiers:

```
admit(createProcessInstance P)  ⟺  global_engine_headroom > 0
                                AND  admit_pressure(P) < 1
```

### Tier 1 — Global engine-saturation guard (the shared write path is *ours*)

A single node-level guard that throttles **all** intake when the engine's own
**shared write path** is the bottleneck — the class ρ was reaching for but
mis-located. It keys on a signal that actually binds on the commit path, i.e. the
**raft commit-wait / log-fsync busy fraction** (see Consequences → open item on the
exact signal), *not* the apply-loop busy fraction. This is the correct home for the
*isolated-server / fault-attribution* frame: it fires only when **we** (the engine's
shared write machinery) are saturated, independent of any worker pool. It is a
backstop, not the primary actuator.

### Tier 2 — Per-job-type backlog compressors, fanned out to per-process throttling

The primary latency-preservation loop. It resolves a structural mismatch unique to
a workflow engine: **the congestion unit and the actuation unit differ.**

- **Congestion is per *job type*.** Workers subscribe by job type, so each job type
  has an independent worker pool and its own drain rate `λ_t`. Backlog accumulates
  per type; one sick type is independent of the others.
- **Intake is per *process definition*.** The only admission knob is
  `createProcessInstance` — instances are admitted, not jobs. One process fans out
  to many job types; one job type is fed by many processes (many-to-many).

So we **detect per job type** and **actuate per process**, coupled by the
process→job-type incidence (statically derivable from deployed BPMN):

1. **Per-type governor.** For each job type `t`, run a delay-gradient compressor on
   its backlog `L_t`:
   - fast term: drive `dL_t/dt → 0` (match intake to that type's drain — scale-free,
     no magic number, finds capacity wherever it is);
   - slow term: bias toward the throughput-scaled band `L*_t = W_target · λ_t` to
     bound *absolute* latency (bleed a deep-but-stable backlog down).
   Each governor emits a **pressure** `p_t ∈ [0,1]` (0 = healthy, 1 = fully shed).
   Signals are EWMA-smoothed with a deadband as a fraction of `λ_t` (raw `dL/dt` is
   noisy — ±0.2% jitter on a flat backlog was observed).
2. **Per-process fan-in.** Admission of `createProcessInstance(P)` keys on the
   fan-in of the pressures of the job types `P` can feed:
   `admit_pressure(P) = max_{t ∈ types(P)} p_t` (max is the conservative default; a
   contribution-weighted sum is a later refinement). `types(P)` is an
   over-approximation from the process model (safe: over-approximation only throttles
   more conservatively).

Two behaviours are **correct by design**, not bugs:
- a process feeding *only* healthy types is **never** throttled (the entire win over
  a global compressor);
- throttling `P` to relieve a sick type *also* suppresses `P`'s healthy sibling
  types — unavoidable, because you cannot create "just the healthy parts" of `P`.

### What this replaces / retains

- The ρ + backlog-trend `BacklogGovernor` (commit `4d60bfc`) is **superseded** as the
  primary control. ρ (and/or commit-wait) survives as **observability** and as the
  candidate signal for the Tier-1 global guard.
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
- **Per-type collateral throttling** of healthy sibling types within a throttled
  process is inherent and accepted.
- **Cardinality:** per-type governors are a few atomics each — cheap even at
  hundreds of types. The process→type incidence is precomputed per deployed
  definition and cached; dynamic job types (expression-derived, call activities,
  multi-instance) are over-approximated (safe).
- **The one absolute knob is `W_target`** (target e2e latency), which is meaningful
  and portable — unlike an internal-latency µs threshold or an absolute backlog
  count. Everything else (`dL/dt → 0`, `L* = W_target · λ`) is scale-free.
- **Open item — the Tier-1 signal.** ρ-of-the-apply-loop is proven insufficient. The
  next step is a **read-only diagnostic to localize the shared-write-path wall**
  (raft-log fsync latency `nanobpm_raft_fsync_seconds` vs batcher busy vs
  commit-wait) before committing to `commit-wait` as the Tier-1 signal. Tier 1 must
  not repeat the "right brain, wrong lever" pattern — it needs a measured binding
  signal.

## Validation (to be filled in)

GCP 3-node RF=3 P=12, clean journal, latency mode. Required scenarios:

1. **Heterogeneous per-type isolation:** two job types, sicken one worker pool.
   EXPECT only the processes feeding the sick type get throttled; processes feeding
   only the healthy type keep full admission. (This is the scenario no prior design
   could pass.)
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
  to *where* the backlog is — one sick job type throttles all admission. Rejected for
  a general/heterogeneous product.
- **Isolated-server ρ as primary (fault-attribution).** Rejected: proven inert on
  real workloads (apply loop never saturates) and it declines to preserve latency
  under external strain, which violates the `SLA_MODE=latency` contract. Retained
  only as a Tier-1 *candidate*/observability.
- **Per-`(process,type)` decomposition.** More precise attribution of a type's inflow
  across processes, but higher cardinality and needs per-edge flow measurement.
  Deferred; `max`-fan-in over `types(P)` is the pragmatic first cut.
- **Direct e2e-sojourn probe (perturb-and-observe).** The original operator intuition;
  correct in spirit but e2e sojourn has a long transport dead-time that makes a fast
  probe oscillate. Resolved by using **backlog depth as the fast, dead-time-free
  causal proxy** (Little's law) with sojourn as slow confirmation — which is exactly
  the Tier-2 signal.
