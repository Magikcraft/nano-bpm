# ADR 0014 — Create-placement protection and cluster load-awareness

Status: **Accepted — implemented; single-node/default byte-identical; unit-tested;
release build green. Cluster soak deferred** (needs the open-loop Rust producer
noted in ADR 0013 to generate true heterogeneous overload).
Date: 2026-07-03.
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

Introduce `NANOBPMN_CREATE_PLACEMENT` with three staged modes (default `off`):

- **`off`** (default): historical blind round-robin, forwarded creates ungated.
  Byte-identical to prior behaviour — single node and every existing benchmark are
  unaffected (placement returns "local" immediately when there is ≤ 1 partition, so
  there is zero added work off-cluster).
- **`protect`**: every node self-protects (stage 1).
- **`balanced`**: `protect` *plus* load-aware weighted placement with peer load
  gossip (stage 2). `balanced` implies `protect`.

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
  and is never chosen), then selects via an interleaved weighted round-robin over a
  shared counter. Creates flow to nodes with the shallowest backlog — work-conserving
  balance — instead of being spread evenly onto a saturated owner.
- **Optimistic on missing data.** A peer that has not gossiped yet is treated as
  **full headroom** (load 0), so an unprobed peer still receives traffic; the
  reactive stage-1 shed/reroute is the backstop that corrects an over-optimistic
  guess. Placement is therefore never *worse* than round-robin even with stale hints.

#### Weighted-round-robin normalisation (an implementation subtlety worth recording)

The raw inverse-load weights span many orders of magnitude
(`WEIGHT_SCALE / (load+1)` ranges up to ~1e6). A naive unit-stepped counter over
raw weights stays stuck in the first (enormous) band forever — a healthy peer would
receive *zero* creates. `weighted_pick` therefore **normalises** the weights onto a
bounded resolution (`WRR_RESOLUTION = 1024`) before sweeping, giving every
non-shedding owner at least a `1/1024` share and letting the counter actually
traverse the distribution in proportion to weight.

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

- `server/src/placement.rs` unit tests: mode parsing/aliases, capability flags,
  inverse-load weight monotonicity + shed → weight 0, `weighted_pick` proportionality
  over a full normalised window, and all-shedding → `None`.
- `server/src/main.rs` `clustered_startup_tests`: `next_create_placement_avoiding`
  skips tried owners then falls back local; weighted placement steers away from a
  loaded/shedding peer toward a healthy one; and an end-to-end
  `protected_create_reroutes_around_a_shedding_owner` over the real peer link.
- Full server suite green (203 tests); `cargo clippy` zero warnings on both the
  default and `--features console` builds; release build green.

## Consequences

- **Positive.** No node can be overrun by forwarded placement; a cluster with
  heterogeneous resources or skewed ingress uses its aggregate capacity instead of
  being gated by its most-loaded node; the client only sees backpressure under
  genuine *global* saturation. All of it is opt-in and defaults to the exact prior
  behaviour.
- **Neutral / honest limits.** See above — single-partition ceiling unchanged;
  hints are eventually-consistent; ambiguous post-send errors are retryable rather
  than auto-rerouted.
- **Negative.** `balanced` adds a small periodic gossip fan-out (one tiny frame per
  peer per interval, default 500 ms) and a lock-guarded peer-load map read on the
  placement path — negligible, and entirely absent in `off`/`protect`/single-node.

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
- **Auto-enable by default.** Rejected: the byte-identical-default discipline
  (single node + existing benchmarks unchanged) is a hard constraint; operators opt
  in per cluster.
