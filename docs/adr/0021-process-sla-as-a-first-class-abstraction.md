# ADR 0021 — Process SLA as a first-class abstraction: unifying job priority and admission compression

Status: **Proposed — exploratory (not yet accepted for implementation).**
Date: 2026-07-17.
**Blocks release of the ADR-0020 Tier-2 compressor as an SLA feature — see
"Blocking constraint: shedding must be latency-causal" below.**
Relates to: ADR 0020 (two-tier admission compression), ADR 0013 (SLA modes &
the compressor/limiter model), ADR 0017 (worker-concurrency governor),
`engine-core/src/state.rs` (`activation_order`), `engine-core/src/bpmn.rs`
(`priorityDefinition` / `job_priority`), `engine-core/src/command.rs`
(`priority`), `server/src/backpressure.rs` (Tier-2 `W_target` band),
`createProcessInstance` admission path in `server/src/main.rs`.

## Context

We already ship **three separate actuators** that all decide *whose work matters
under contention* — and each is reaching, piecemeal, for the same missing input:

1. **Job priority** (`zeebe:priorityDefinition`). Parsed in `bpmn.rs` as a FEEL
   expression or literal (default `DEFAULT_JOB_PRIORITY = 50`); at activation, jobs
   of a type are ordered `(−priority, key)` — highest priority activated first,
   equal priorities oldest-first (`engine-core/src/state.rs::activation_order`). It
   is a **work-conserving scheduling** lever: it reorders *which* job a worker gets
   next, drops nothing.
2. **Admission shed** (ADR-0020 Tier-1 / Tier-2). A **lossy** lever at the edge of
   the server/client envelope: reject `createProcessInstance` with `503` to protect
   latency when a bottleneck (the shared raft write path, or a per-definition
   backlog) saturates.
3. **The Tier-2 latency target `W_target`.** The setpoint the per-definition
   compressor holds (`band = W_target · λ`). Today it is a **single global env knob**
   (`NANOBPMN_TIER2_W_TARGET_MS`) — the same promise for every definition.

These are three faces of one absent abstraction: a **declared process SLA**. Job
priority is a piecemeal implementation of *importance*; `W_target` is a piecemeal,
undifferentiated implementation of a *latency target*. Nothing lets an operator say
"this loan-approval process must finish within 30 s and matters more than the
nightly-reconciliation process," and have the engine use that statement coherently
across scheduling **and** admission.

Crucially, the abstraction is **two axes, not one number**:

- **Target** (cardinal): the end-to-end sojourn we promise — answers *"when do we
  shed / how deep may the backlog get?"* This is what `W_target` encodes.
- **Importance** (ordinal): who to protect when targets conflict — answers *"who
  waits, and who is shed first?"* This is what job priority encodes.

You need both, because **declaring an SLA does not create capacity.** Under
sustained overload some target *will* be missed; importance is the policy for whose.

## Decision

Introduce **Process SLA** as a first-class, declared value:

```
ProcessSla = ⟨ target_latency: Duration,   // cardinal — the sojourn promise
               importance:     i32 ⟩        // ordinal  — reuse the priority scale
```

sourced with the same **definition-default + instance-override** layering that job
priority already uses:

- **Definition-declared default**: a model extension on the process (mirroring how
  `zeebe:priorityDefinition` sits on a task), giving a per-definition `target` and
  `importance`.
- **Per-instance override**: an optional `sla` field on `createProcessInstance`
  (target and/or importance), evaluated at creation like a runtime priority.

The SLA is then consumed by a **layered policy, cheapest-first**, so we spend the
lossy lever only when the work-conserving one is exhausted:

1. **Scheduling (importance) — work-conserving, free.** Drain tight-target /
   high-importance work first. This already exists for jobs via
   `activation_order`; the SLA makes `importance` a first-class, instance-aware
   input rather than only a model literal. Honors the most SLAs **without dropping
   anything**.
2. **Admission (target + importance) — lossy backstop.** Only when scheduling
   cannot keep the tightest-target work within its promise:
   - **Tier-2** sources its band from the definition's declared `target`
     (`W_target_P = target_P`) instead of the global knob — the band is *already*
     per-definition (`W_target · λ_P`); we simply feed it a per-definition, possibly
     per-instance target.
   - **Shed selection becomes importance-weighted**: replace the blind even
     per-mille accumulator with one that sheds **lowest-importance instances first**
     within a definition, and (under Tier-1 global pressure) biases shedding toward
     low-importance definitions, protecting high-importance intake.
   - **Tier-1 stays SLA-agnostic for *engagement*** — it protects the shared write
     path for *everyone* and must not be gameable by a client's self-declared
     urgency — but it *may* use importance only to choose **which** intake to shed
     once it has decided *how much*.

### What this changes vs today

| lever | today | with Process SLA |
|---|---|---|
| job priority | model literal/FEEL, per job type, activation order | unchanged mechanism; becomes the **importance** axis of the SLA, instance-aware |
| Tier-2 `W_target` | one global env knob | **per-definition** (model-declared), per-instance override |
| shed selection | even per-mille accumulator (blind) | **importance-weighted** (protect the valuable) |
| Tier-1 engagement | fsync knee, global | unchanged (SLA-agnostic; importance only biases *which* to shed) |

## Consequences

**Positive**
- One coherent currency prices both the scheduling lever and the admission lever;
  the three ad-hoc actuators stop guessing at the same missing input.
- Per-definition (and eventually per-instance) latency differentiation — the right
  primitive for **heterogeneous** workloads on a shared cluster.
- Backwards compatible: absent SLA ⇒ today's behaviour (global default target,
  `importance = 50`, even shed).

**Costs / risks (explicitly, so we design for them)**
- **SLA is a promise you cannot always keep.** Under sustained overload targets
  conflict; the system *must* have an explicit tiebreak (importance) and must
  degrade predictably rather than pretend. The ADR's policy makes the tiebreak
  first-class rather than emergent.
- **Trust boundary / gaming.** If clients set their own SLA at create-time, everyone
  declares "urgent, 1 ms." Mitigations: a client SLA is **advisory, clamped to a
  definition-level cap**; and/or operator-scoped importance quotas. Tier-1
  engagement stays non-gameable (it never reads client urgency to decide *whether*
  to shed).
- **Signal cost of per-instance targets.** Tier-2's current signal is an *aggregate*
  per-definition backlog counter (cheap). A true **per-instance** target implies
  per-instance **deadline/age** tracking — a deadline-ordered structure, materially
  heavier. Therefore the rollout does **per-definition first** (reuses the existing
  cheap counter) and treats per-instance deadline-aware scheduling as an opt-in,
  later phase.

## Rollout (phased)

- **Phase 1 — per-definition target (low-risk, ~90% shaped).** Add the model
  extension; source Tier-2 `W_target_P` from the definition instead of the global
  env knob. No new data structures (band is already per-definition). Validates the
  "differentiated target" half with the machinery ADR-0020 already built.
- **Phase 2 — importance-weighted shed.** Reuse the existing `job_priority`/
  `activation_order` importance scale as the SLA `importance`; make Tier-2 (and the
  Tier-1 *which-to-shed* choice) shed lowest-importance first instead of evenly.
- **Phase 3 — per-instance SLA + deadline-aware scheduling.** Add the `sla` field to
  `createProcessInstance`; introduce per-instance deadline tracking and (optionally)
  EDF-style activation for tight-deadline instances. Gate behind the trust-boundary
  clamp/quota work.

## Validation (future work)

On the GCP soak rig (as for ADR-0020), once implemented:

1. **Differential targets under shared saturation.** Two definitions sharing a
   starved job type, different declared targets: EXPECT the tight-target definition
   held within its promise while the loose-target one absorbs the backlog — driven
   purely by declared SLA, not topology.
2. **Importance-weighted shed.** Under Tier-1 global write-path pressure, EXPECT
   high-importance intake preserved while low-importance intake is shed first, at the
   same total shed volume.
3. **Per-instance override + gaming guard.** EXPECT a per-instance tight SLA honored
   up to the definition cap, and a client that declares everything urgent clamped to
   the cap (no starvation of other definitions).

## Blocking constraint: shedding must be latency-causal

**This is a release blocker for the ADR-0020 Tier-2 compressor as a customer-facing
SLA feature.** The SLA promise is a *trade*: **we sacrifice admission to protect
latency.** A shed is only legitimate when it actually *buys* latency for the work
we keep. The current Tier-2 control law does not guarantee that trade holds.

**The defect.** Tier-2 attacks a definition when its in-flight backlog `L_P` exceeds
its Little's-law band `W_target · λ_P` and is rising (`server/src/backpressure.rs`,
`ProcessGovernors::step`). `L_P` is *only the in-flight instance count* — and by
Little's Law `L = λ·W`, `L_P > W_target·λ_P` is exactly `W_actual > W_target`. The
controller therefore fires whenever realized sojourn exceeds target **and cannot
see *why*.** Engine congestion, a contended worker pool, and a **slow external
service the worker calls** all inflate `L_P` identically.

In the external-dependency case this is actively harmful:

- The elevated sojourn comes from time spent *in service* on a downstream the engine
  does not control. Reducing the admission rate λ does **not** speed up the instances
  already in flight — they remain blocked on the same slow dependency.
- If worker concurrency is ample (async I/O, no local queue forming), no
  queue-wait component exists to relieve, so the shed yields **no latency benefit at
  all** — we reject the customer's process instances and deliver nothing in return.

That inverts the SLA: **we punish admission and produce no latency gain.** For a
general product deployed into heterogeneous customer environments — where
worker-to-external-service latency is the *common* source of process latency — this
is not acceptable behaviour and must be fixed before Tier-2 ships as an SLA control.

**Why the global tier does not have this bug.** The ρ-based server governor
(`AdmissionGovernor`) explicitly gates on engine-actor saturation ρ and, by its own
documentation, separates *"we're the bottleneck (ρ high **and** rising)"* from
*"external strain (ρ low, rising)"* — it refuses to shed on external strain. Tier-2
has **no equivalent causal gate**; it keys on backlog depth vs band alone. Closing
that asymmetry is the crux of the fix.

**Direction (to be designed).** A shed must be predicated on a signal that the
backlog reflects a **locally contended resource** the engine *can* relieve by pacing
intake — e.g. a growing runnable/worker-wait queue, worker-pool saturation, or
engine-side congestion — not pure in-service waiting on an external call. Candidate
approaches, to evaluate:

1. **Causal gate on a local-contention signal.** Only attack when a per-definition
   (or per-job-type) *queue-wait* / worker-saturation signal is present, mirroring
   the ρ gate. Decompose realized sojourn into *queue-wait* (relievable by shedding)
   vs *in-service* (not relievable); shed only against the former.
2. **Counterfactual self-check.** Extend the observe-vs-act mechanism (already used
   for the OFF/ON A/B in ADR-0020 validation): when pressure rises, verify the
   *queue-wait* component actually falls; if it does not, the shed is not buying
   latency — release and stop shedding that definition (an online causality probe,
   echoing the user's "compress-and-watch-latency" instinct).
3. **Scope the metric.** Track the relievable backlog (jobs waiting for a free
   worker) rather than total in-flight `L_P`, so external in-service time never
   enters the trigger.

Until one of these lands, Tier-2 must not be presented to customers as a
latency-SLA control (it may still run in observe-only / monitoring mode, which does
not shed). This constraint is a hard input to the process-SLA design above: an
importance-weighted shed still needs a *"shed only when it relieves latency"*
predicate underneath it.

## Alternatives considered

- **Keep job priority only (no target).** Priority expresses *ordering* but cannot
  express a *latency bound*; it cannot tell admission when to shed. Rejected — it is
  half the abstraction.
- **Keep a single global `W_target` (status quo).** Simple, but one promise for all
  definitions; cannot protect a critical process on a heterogeneous server. Rejected.
- **Full EDF/deadline scheduler everywhere.** Maximally expressive but imposes
  per-instance deadline tracking on the common (no-SLA) path — too heavy. Deferred to
  the opt-in Phase 3 for tight-deadline instances only.
