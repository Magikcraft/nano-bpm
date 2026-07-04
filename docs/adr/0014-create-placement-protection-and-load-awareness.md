# ADR 0014 — Create-placement protection and cluster load-awareness

Status: **Accepted — implemented; default-on (`balanced`) with single-node
byte-identical; unit-tested; release build green. Cluster soak deferred** (needs
the open-loop Rust producer noted in ADR 0013 to generate true heterogeneous
overload).
Date: 2026-07-03 (default flipped to `balanced` 2026-07-04 — see "Defaulting").
Relates to: ADR 0001 (cluster job-activation fairness), ADR 0002 (leader-local
activation), ADR 0013 (SLA modes at the saturation ceiling),
`server/src/placement.rs`, `server/src/partition.rs`, `server/src/main.rs`,
`server/src/falcon.rs`, `server/src/peer.rs`.

## Context

Nano places `createProcessInstance` across a cluster by **blind round-robin** over
every partition (`PartitionRouter::next_create_placement`). The ingress node that
receives a client request picks the next partition in rotation and, if that
partition is owned by a peer, **forwards** the create to that peer. Two properties
of this scheme are fine on a homogeneous, evenly-loaded cluster but break down the
moment nodes differ or load skews:

1. **Forwarded creates bypass the owner's admission gates.** A node protects
   itself against *locally-submitted* overload with its admission gates (the AIMD
   concurrency limiter, create-queue depth, exporter saturation, in-flight-payload
   watermark, resident-memory watermark — see ADR 0013). But a create *forwarded*
   from a peer was applied directly, with none of those gates consulted. So a node
   already at its ceiling could still be handed unbounded forwarded work and be
   overrun — the cluster had no way for an overloaded owner to say "not me."

2. **Placement is load-blind.** Round-robin assumes every partition's owner has
   equal capacity and equal current load. Under **heterogeneous resources** (a
   smaller node mixed with larger ones) or **skewed ingress** (one node hit its
   throughput or memory ceiling while peers have headroom — the scenario that
   motivated this ADR), round-robin keeps piling an equal share onto the saturated
   node while peers sit idle. Throughput is then gated by the *weakest / most-loaded*
   node, not the cluster's aggregate capacity.

This is the cluster analogue of the single-node work already done: a node knows
how to protect *itself*, but the *placement* decision — made on a different node —
had no protection and no load signal. We want (a) every node to self-protect
against forwarded overload, and (b) creates to be steered toward nodes with real
spare capacity, **without** changing single-node behaviour or any existing
benchmark, and **without** ever risking a duplicate instance.

## Decision

Introduce `NANOBPMN_CREATE_PLACEMENT` with three staged modes (**default
`balanced`** — see "Defaulting" below):

- **`off`**: historical blind round-robin, forwarded creates ungated.
  Byte-identical to the pre-cluster behaviour. An explicit opt-out.
- **`protect`**: every node self-protects (stage 1).
- **`balanced`** (default): `protect` *plus* load-aware weighted placement with
  peer load gossip (stage 2). `balanced` implies `protect`.

### Stage 1 — `protect`: self-protection + ingress reroute

- **Peer-side shed gate.** At the top of the forwarded-create handler, if
  `placement_mode.protects()` and the node `create_should_shed()` (submission
  backpressure **or** any admission-gate reason), it returns
  `503 PLACEMENT_SHED: <reason>` instead of applying the create. The forwarded
  create is subjected to exactly the same gates as a local one.
- **Ingress reroute.** The ingress node runs `forward_create_rerouting`: it forwards
  to the first chosen owner, and on a placement-shed 503 (or a pre-send unreachable
  peer) it re-places onto *another* owner (round-robin skipping already-tried owners
  in `protect`), looping until a peer accepts, a terminal result arrives, or every
  owner is exhausted. Only when the whole cluster sheds does the client see
  backpressure — this is genuine **global** backpressure rather than a node being
  overrun by placement.
- **Last-resort local floor.** If every owner sheds, ingress falls through to a
  **local** create (it already passed its own admission gate on the way in), which
  is the honest floor rather than a spurious 503.

#### Duplicate-instance safety (the key correctness decision)

A create is only ever rerouted when the ingress node can **prove the owner did not
apply it**:

- an explicit `PLACEMENT_SHED:` 503 (the peer shed *before* applying — a contract),
  or
- a **pre-send** peer-link failure (`Unreachable` — the request never left).

A transport error *after* the request was sent is **ambiguous** (the peer may have
applied the create) and is surfaced as a retryable error, **not** rerouted — because
rerouting an already-applied create would create a duplicate instance. This
asymmetry is deliberate and is why the shed signal is a distinct, explicit marker
rather than "any 503."

### Stage 2 — `balanced`: load-aware weighted placement + gossip

- **Load gossip.** In `balanced` mode each node runs a lightweight tick
  (`NANOBPMN_CREATE_PLACEMENT_GOSSIP_MS`, default 500 ms) that broadcasts a
  **composite create-load index** to its peers over a new `PressureReport` Falcon
  frame. The index is `SHED_LOAD` (a sentinel meaning "shedding now") if the node
  would shed, else its active backlog. Cheap to compute (a couple of relaxed atomic
  loads plus the admission checks — no engine round-trip). The gossip tick is a
  no-op unless `balanced` *and* there are real peers, so it adds nothing in every
  other configuration.
- **Weighted placement.** Placement weights each partition's owner **inversely to
  its gossiped load** (`WEIGHT_SCALE / (load + 1)`; a shedding owner gets weight 0
  and is never chosen), then selects via **smooth weighted round-robin** (SWRR).
  Creates flow to nodes with the shallowest backlog — work-conserving balance —
  instead of being spread evenly onto a saturated owner.
- **Optimistic on missing data.** A peer that has not gossiped yet is treated as
  **full headroom** (load 0), so an unprobed peer still receives traffic; the
  reactive stage-1 shed/reroute is the backstop that corrects an over-optimistic
  guess. Placement is therefore never *worse* than round-robin even with stale hints.

#### Smooth weighted round-robin (an implementation subtlety worth recording)

The selection primitive (`swrr_pick`) is the nginx-style **smooth weighted
round-robin**, keeping a persistent per-partition smoothing accumulator. It was
chosen because it satisfies both ends of the spectrum with one mechanism:

- **Equal weights ⇒ exact round-robin.** On an idle/homogeneous cluster every
  owner's weight is equal, and SWRR then rotates strictly (owner 0, 1, 2, 0, 1, …)
  so creates spread perfectly evenly — the same distribution as blind round-robin.
- **Skewed weights ⇒ smooth proportional spread.** As loads diverge, picks are
  interleaved in proportion to weight *without clumping* (a 3:1 weight yields
  0,0,1,0,0,1,… not 0,0,0,1). Every call advances — there is no warm-up window.

An earlier draft used a *banded counter sweep* normalised onto a fixed resolution;
it was replaced because, with few creates between weight updates, a unit-stepped
counter stayed inside a single (wide) band and failed to rotate at all when weights
were equal — a healthy peer could receive **zero** creates over a short burst. SWRR
has no such warm-up pathology and needs no resolution constant.

## Defaulting (why `balanced` is the default, not opt-in)

Nano's design philosophy: **"If we can tell the user how to do it, and when to do
it — why don't we do it?"** Load-aware placement and self-protection are not a
*business* decision (nothing about them depends on what the operator values); they
are a self-optimization the engine can make correctly on its own. So the default is
`balanced` — the engine load-balances and self-protects out of the box, and the
operator only ever touches this knob to *opt out* (`off`, to restore blind
round-robin, e.g. for a strict apples-to-apples benchmark).

This is safe to default on because **it is a no-op wherever it cannot help**:

- **Single node / a node owning every partition:** placement returns "local"
  immediately (≤ 1 remote owner), the shed gate only fires on *forwarded* creates
  (there are none), and the gossip tick is idle without peers. So single-node
  behaviour is **byte-identical** to blind round-robin with zero added overhead.
- **Idle / homogeneous cluster:** SWRR with equal weights *is* exact round-robin,
  so the distribution matches the historical scheme until loads actually diverge.

The earlier draft defaulted to `off` to keep multi-node benchmarks byte-identical;
that discipline was superseded by the philosophy above once the single-node no-op
and the equal-weight round-robin equivalence made the default provably harmless
where it does not help — and strictly better where it does. (The same reasoning
flipped `NANOBPMN_ACTIVATION_FAIRNESS` to default `stage2`.)

## Composition with prior ADRs

- The shed signal reuses the **same admission gates** as ADR 0013's SLA modes. In
  `latency` SLA mode the latency-preservation gates are active, so a busy node sheds
  forwarded creates sooner (protecting e2e latency cluster-wide); in `admission` SLA
  mode only the memory-safety rails remain, so a node sheds forwarded creates only
  near a genuine memory ceiling. Placement protection thus inherits the operator's
  SLA choice for free.
- It is orthogonal to ADR 0001/0002 (job activation and leadership): this ADR is
  about *where a create is placed*, not *who leads a partition* or *who activates a
  job*.

## Honest limitations

- **The per-partition single-writer ceiling remains.** Load-aware placement steers
  work *between* partitions/nodes; it does not raise the throughput of any single
  partition (still one serial writer). It converts "one node overrun while peers
  idle" into "load spread to available headroom" — a utilisation win, not a
  single-partition speedup.
- **Gossip is eventually-consistent and best-effort.** Placement acts on a load
  hint up to one gossip interval stale (and treats missing hints as headroom), so it
  can transiently mis-place; stage-1 reroute is the correctness backstop, so a wrong
  guess costs at most one extra forward hop, never an overrun or a drop.
- **Reroute only fires on a proven-not-applied shed.** By design (duplicate safety)
  an ambiguous post-send transport error is *not* rerouted; it is retryable at the
  client. This trades a small amount of automatic recovery for a hard no-duplicate
  guarantee.
- **Cluster soak still owed.** The behaviour is correct-by-construction and
  unit-tested, but the divergence from round-robin only shows under *heterogeneous
  or open-loop overload*, which the closed-loop (completion-aware) load clients we
  have cannot generate (same limitation recorded in ADR 0013). Measuring the
  utilisation win needs the deferred open-loop Rust producer.

## Verification

- `server/src/placement.rs` unit tests: mode parsing (default `balanced`, explicit
  `off` opt-out, aliases), capability flags, inverse-load weight monotonicity +
  shed → weight 0, and SWRR behaviour — equal weights ⇒ exact round-robin,
  proportional-and-smooth spread under skew, a shedding owner is never picked, and
  all-shedding → `None`.
- `server/src/main.rs` `clustered_startup_tests`: `next_create_placement_avoiding`
  skips tried owners then falls back local; weighted placement steers away from a
  loaded/shedding peer toward a healthy one; REST creates spread across every
  partition of the cluster under the default; and an end-to-end
  `protected_create_reroutes_around_a_shedding_owner` over the real peer link.
- Full server suite green (205 tests); `cargo clippy` zero warnings on both the
  default and `--features console` builds; release build green.

## Consequences

- **Positive.** Out of the box, no node can be overrun by forwarded placement and a
  cluster with heterogeneous resources or skewed ingress uses its aggregate capacity
  instead of being gated by its most-loaded node; the client only sees backpressure
  under genuine *global* saturation. Single node stays byte-identical; an operator
  can still opt out with `off`.
- **Neutral / honest limits.** See above — single-partition ceiling unchanged;
  hints are eventually-consistent; ambiguous post-send errors are retryable rather
  than auto-rerouted.
- **Negative.** `balanced` adds a small periodic gossip fan-out (one tiny frame per
  peer per interval, default 500 ms) and a lock-guarded SWRR/peer-load read on the
  placement path — negligible, and entirely absent in `off`/single-node.

## Alternatives considered

- **Reroute on any 503 / any transport error.** Rejected: an ambiguous post-send
  error could reroute an already-applied create → duplicate instance. Only an
  explicit shed marker or a pre-send failure is safe to reroute.
- **A dedicated load-probe RPC instead of gossip.** Rejected for now: gossip is
  cheaper (fire-and-forget, amortised, no per-placement round-trip) and staleness is
  covered by the reactive reroute backstop. A pull probe could be added later if a
  workload needs fresher hints than the gossip interval.
- **Consistent-hashing / capacity-weighted static placement.** Rejected as the
  first step: it needs a capacity model and rebalances poorly under transient load.
  The dynamic inverse-load weight is simpler and reacts to *current* pressure, which
  is what the heterogeneous-ceiling scenario actually needs.
- **Keep it opt-in (default `off`).** Rejected — see "Defaulting". The behaviour is
  a no-op on a single node and where loads are equal, and strictly better where they
  are not, so per the design philosophy the engine does it by default; the operator
  opts *out* rather than in.
