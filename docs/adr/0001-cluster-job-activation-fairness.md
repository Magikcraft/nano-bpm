# ADR 0001 — Cluster job-activation fairness

Status: Accepted (Stages 1 & 2 shipped). **Default flipped to `stage2` on
2026-07-04** — see ADR 0014's "Defaulting" (self-optimizations default on; the
routing is a no-op on a single node and where backlogs are balanced, so the default
is safe). Set `NANOBPMN_ACTIVATION_FAIRNESS=off` to restore strict local-first.
Supersedes nothing.
Date: 2026-06-21 (default change 2026-07-04).

## Context

In a multi-node cluster a worker connects to **one** gateway, but that gateway can
activate jobs from the **whole** cluster (every node aggregates peer-owned jobs over
the Falcon protocol — see `docs/distributed-scaling-design.md`). The dispatcher,
however, was **strictly local-first with a fixed peer order**:

- `falcon.rs::dispatch_to_connection` activates the gateway's **own**
  partitions first (`activate_for_stream`), then pulls only the shortfall from peers
  in **fixed ascending node-id order** (`peer_nodes()` sorts).
- The REST long-poll analog `main.rs::activate_jobs_impl` has the same shape.

Two structural starvation sources follow:

1. **Local-first bias.** If the gateway's own partitions always have work — the common
   RF=1 case where creation *and* the workers sit on the same node — the peer-pull loop
   is rarely reached, so peers' partitions back up unboundedly.
2. **Fixed peer order.** Among peers, node 1 is always drained before node 2, so the
   highest-id node is structurally second-class.

The result is **naive high aggregate throughput bought at the cost of a skewed
end-to-end latency distribution**: the gateway's own partitions look healthy while
peer-owned instances languish. Raw throughput is the *vanity* metric; what we actually
want to protect is **per-partition time-to-complete determinism** — the basis of any
per-tenant / per-instance SLA.

## Decision

Add **fairness-aware activation routing**, opt-in via `NANOBPMN_ACTIVATION_FAIRNESS`
(default **off** ⇒ existing benchmarks and the single-node path are byte-identical).
The change is **non-blocking** (no cross-node coordination round-trips on the hot path)
and preserves correctness: leases remain exclusive on each owner's single-writer actor,
so routing only redistributes *where* a worker's lease budget is spent, never *whether*
a job can be double-leased.

Rollout is **staged**, each stage independently shippable and measured:

- **Stage 1 (shipped, this ADR):** rotation + quota. A pure
  `fair_plan(want, num_sources, start)` spreads a worker's lease budget across
  `{local, peers}` — lap 1 quota-caps each source at `ceil(want/num_sources)` so a fat
  backlog on one node cannot monopolise the budget; lap 2 soaks leftover from sources
  that ran dry; the start source is rotated per pass. No protocol change.
- **Stage 2 (shipped):** backlog-weighted routing. Split `want` proportional to each
  source's live backlog — local read in-process, peers **piggybacked** on their
  activation responses (zero extra round-trips, no new RPC). Steers budget to the
  genuinely-deepest node so it drains faster; collapses to Stage 1 when balanced.
- **Stage 3 (optional):** per-job-type backlog (the Stage 2 hint is per-node), and/or
  push-to-demand — an overloaded node with few local subscribers offers work toward
  gateways with hungry credits.

## Measurement methodology

A/B driver: `ts-performance-matrix/src/fairness-ab.ts`. It boots the **same** release
binary twice (`NANOBPMN_ACTIVATION_FAIRNESS` off, then on), 3 nodes / 3 partitions /
RF=1 / no Raft, async durability. Workers attach to **node 0 only** over the **stream**
transport (the path Stage 1 changes). It samples each node's
`GET /console/api/metrics → activeInstances` (per-node backlog) throughout, and computes
per-node backlog mean/peak, peer/gateway ratio, cross-node CoV, per-node time-to-drain,
cluster throughput, and e2e latency percentiles (producer stamps `t0`; worker computes
`now − t0`). Machine: M4 Pro, co-resident servers + load (so absolute throughput is
contention-limited; the **A/B delta on the same box** is the signal). Two scenarios:

- **Drain** (`--target 0`): precreate a fixed backlog spread cluster-wide, then drain it
  with capacity-bounded workers and **no** producer. Deterministic; isolates *which*
  partition's jobs complete *when*.
- **Steady-state overload** (`--target` above worker capacity): a sustained producer
  exceeds drain capacity so a standing backlog forms — stresses the latency tail.

> Reproduce:
> ```
> # Stage 1 vs OFF — drain proof
> node --import tsx/esm src/fairness-ab.ts --arm-a off --arm-b 1 --workers 6 --maxpar 2 \
>   --target 0 --precreate 2400 --handler-ms 40 --duration 40
> # Stage 2 vs Stage 1 — skewed backlog (P=4 ⇒ node 0 owns 2 partitions)
> node --import tsx/esm src/fairness-ab.ts --arm-a 1 --arm-b 2 --partitions 4 \
>   --workers 2 --maxpar 32 --target 0 --precreate 6400 --handler-ms 150 --duration 60
> ```
> `--arm-a`/`--arm-b` each take an `NANOBPMN_ACTIVATION_FAIRNESS` value (`off`/`1`/`2`).

## Stage 1 — performance: baseline vs after

### Experiment A — drain a fixed backlog (2400 instances, 6 workers × maxpar 2, 40 ms/job)

| Metric | Baseline (local-first) | After (fairness on) | Read |
| --- | --- | --- | --- |
| Cluster throughput | 280.2 inst/s | 277.0 inst/s | **equivalent** (−1.1 %) |
| Backlog mean per node `[n0,n1,n2]` | `[144, 405, 670]` | `[396, 393, 395]` | skew → even |
| Peer/gateway backlog ratio | **3.74** | **0.99** | 1.0 = fair |
| Backlog CoV across nodes | **0.528** | **0.004** | 0 = fair |
| **Time-to-drain per node** | **`[2.8s, 5.5s, 8.3s]`** | **`[8.4s, 8.4s, 8.4s]`** | even = fair |

The headline: **same total throughput / same total drain time (~8.4 s), but baseline
empties node 0 at 2.8 s and starves node 2 until 8.3 s**, while fairness drains all three
together. If each partition is a tenant with a time-to-complete SLA, baseline gives
tenant-0 a 2.8 s experience and tenant-2 an 8.3 s experience for identical work; fairness
makes them uniform.

### Experiment B — steady-state overload (producer 700/s ≫ ~390/s capacity, 30 ms/job)

| Metric | Baseline (local-first) | After (fairness on) | Read |
| --- | --- | --- | --- |
| Cluster throughput | 385.8 inst/s | 367.0 inst/s | equivalent (−4.9 %) |
| Backlog mean per node `[n0,n1,n2]` | `[8, 2064, 4411]` | `[2281, 2267, 2305]` | extreme skew → even |
| Peer/gateway backlog ratio | **416.75** | **1.00** | 1.0 = fair |
| Backlog CoV across nodes | **0.833** | **0.007** | 0 = fair |
| e2e latency p50 | **39 ms** | 9195 ms | see note |
| e2e latency p99 | 13098 ms | 16625 ms | +26.9 % |

**Honest interpretation (important).** Under sustained overload, aggregate latency is
governed by Little's law: same throughput ⇒ same *total* backlog ⇒ similar *mean*
latency, regardless of routing. Fairness does **not** lower aggregate latency here — it
**redistributes** it. Baseline's flattering `p50 = 39 ms` is an illusion of health: it
reflects node 0's own partitions completing near-instantly (backlog 8) **while peers sit
at 2064 and 4411 — a 416× skew bordering on starvation**. Fairness equalises the queue
(ratio 1.00), so every partition sees the same ~9 s — which *raises* the aggregate p50/p99
because the previously-favoured fast mass is slowed to the shared rate. This is the whole
point: we trade a deceptively-good aggregate number for **determinism across partitions**.
Throughput stays within ~5 %.

### Stage 1 verdict

- Throughput: **equivalent** (within measurement noise / ≤5 % on a co-resident box).
- Fairness: peer/gateway backlog ratio **3.74 → 0.99** (drain) and **416.75 → 1.00**
  (overload); CoV collapses toward 0; per-node time-to-drain equalises.
- Cost: when total demand exceeds capacity, aggregate p99 can rise because no partition is
  favoured any more. That is the intended trade — **fairness/SLA-determinism over a vanity
  aggregate latency**.

## Stage 2 — performance: baseline vs after

Stage 2 is **backlog-weighted routing**, measured against **Stage 1 as the baseline**
(same `fairness-ab.ts`, `NANOBPMN_ACTIVATION_FAIRNESS=1` vs `=2`).

### Design as built

Stage 1's even split is fair only when every source holds a similar backlog. Under a
genuine **per-node backlog skew** it leaves capacity on the floor: the deepest node
drains at the same per-pass rate as shallow ones, so it finishes last while the others
sit idle. Stage 2 caps each source proportional to its **live backlog**:

- **Local** backlog is read directly from the in-process active-instance gauge
  (`ServerImpl::active_backlog`, a relaxed atomic — no engine round-trip).
- **Peer** backlog is **piggybacked** on the existing peer activation response: the
  peer-side `ActivateJobs` handler returns `{ jobs, backlog }`, and `activate_from_peer`
  records `backlog` into a per-node cache. **No extra round-trip, no new RPC** — every
  time a worker pulls from a peer it refreshes that peer's backlog for free.

`fair_plan_weighted` then caps each source at `ceil(want · backlog_i / Σbacklog)` (lap 1,
rotated, with a floor-1 refresh probe on the rotation-start source), then an uncapped
lap-2 soak. When backlogs are balanced the proportional caps equal `ceil(want/n)` — i.e.
**identical to Stage 1**, so a balanced cluster sees no behaviour change. Backlog is a
*routing hint only*; leases stay exclusive on each owner's single-writer actor, so a
stale hint costs at most an occasional empty probe, never correctness.

> Skew is produced naturally with `--partitions 4 --nodes 3` (RF=1): `owner(p)=p%3`, so
> node 0 owns partitions 0 and 3 — **2× the backlog** of nodes 1 and 2 — while the
> workers attach to node 0.

### Experiment C — skewed backlog, drain (P=4 so node 0 owns 2 partitions; 6400 instances, 2 workers × maxpar 32, 150 ms/job)

| Metric | Baseline (Stage 1) | After (Stage 2) | Read |
| --- | --- | --- | --- |
| Cluster throughput | 414.5 inst/s | 418.0 inst/s | **equivalent** (+0.8 %) |
| Backlog mean per node `[n0,n1,n2]` | `[2016, 618, 610]` | `[1646, 822, 817]` | gateway drains faster |
| Backlog CoV across nodes | **0.611** | **0.356** | more even |
| **Time-to-drain per node** | **`[15.2s, 11.4s, 11.4s]`** | **`[15.1s, 15.1s, 15.1s]`** | **peers no longer idle** |

The headline: under Stage 1 the two peers **empty at 11.4 s and then sit idle for ~3.8 s**
while node 0 (twice the partitions) grinds on alone to 15.2 s — wasted cluster capacity
and an SLA cliff for whoever's instances live on node 0. Stage 2 steers budget to the
deepest node, so **all three drain together at ~15.1 s** at the same total throughput.

### Experiment D — balanced backlog, drain (P=3; 4800 instances, same workers) — no-regression

| Metric | Baseline (Stage 1) | After (Stage 2) | Read |
| --- | --- | --- | --- |
| Cluster throughput | 408.6 inst/s | 410.2 inst/s | equivalent (+0.4 %) |
| Backlog CoV across nodes | 0.009 | 0.000 | both fair |
| Peer/gateway backlog ratio | 1.02 | 1.00 | both fair |
| Time-to-drain per node | `[11.2s, 11.2s, 11.5s]` | `[11.4s, 11.4s, 11.4s]` | both even |

As designed, Stage 2 **collapses to Stage 1 on a balanced cluster** — no regression.

### Stage 2 verdict

- Throughput: **equivalent** to Stage 1 (+0.4 % / +0.8 %, within noise).
- Fairness under skew: time-to-drain equalises (`[15.2,11.4,11.4]s → [15.1,15.1,15.1]s`),
  CoV **0.611 → 0.356** — the deepest node no longer lags while peers idle.
- Balanced load: indistinguishable from Stage 1 (CoV 0.009 → 0.000).
- Cost: one `i64` per node carried on responses we already send; a small per-peer cache.
- **Limitation:** the hint is a per-*node* active-instance count, not per-*job-type*
  activatable depth. For a single-job-type workload it is exact; for a mixed workload it
  is a coarse proxy (a node deep in type A but empty of type B is over-weighted for a
  type-B worker). A per-job-type counter is the natural Stage 3 refinement. Also,
  weighting only bites when the per-dispatch budget (`want`, bounded by a worker's
  `maxParallelJobs` and `PER_STREAM_BATCH=64`) is large enough for the proportional caps
  to differentiate; at very small `maxParallelJobs` it rounds back to the Stage 1 even
  split (harmless — Stage 1 is already fair on balanced load).

## Stage 3 — performance: baseline vs after

_Not yet implemented._ To be recorded here against Stage 2 as the baseline.

## Consequences

- **Default-off** keeps every existing benchmark, the single-node path, and CI
  byte-identical; operators opt in per the codebase convention (admission / backpressure
  are likewise opt-in).
- **Operational mitigation meanwhile:** spreading workers across gateways already evens
  activation without the flag; Stage 1 is for the single-gateway-attachment case.
- **Known limitation — REST path not yet fair.** Stage 1 changes only the Falcon
  dispatcher (`dispatch_to_connection`). The REST long-poll activation
  (`activate_jobs_impl`) has the identical local-first / fixed-order bug and is a Stage-1
  follow-up. The benchmark uses the stream transport specifically to exercise the fixed
  path.
- **No correctness change.** Leases remain exclusive on each partition's single-writer
  actor; stale routing costs at most an occasional empty `activate_from_peer` RPC, never a
  double-lease.

## References

- Code: `server/src/falcon.rs` (`fair_plan`, `fair_plan_weighted`,
  `activation_mode`, `record_peer_backlog`, `dispatch_to_connection`),
  `server/src/main.rs` (`active_backlog`, `activate_from_peer` piggyback parse), env
  `NANOBPMN_ACTIVATION_FAIRNESS=1|2`.
- Driver + artifacts: `ts-performance-matrix/src/fairness-ab.ts`,
  `results/fairness-ab/<ts>/{fairness-ab.json,backlog-timeseries.csv}`.
- Related: `docs/distributed-scaling-design.md` (job aggregation), `README.md` env table.
