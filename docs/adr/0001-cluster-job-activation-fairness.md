# ADR 0001 — Cluster job-activation fairness

Status: Accepted (Stage 1 shipped, opt-in). Supersedes nothing.
Date: 2026-06-21.

## Context

In a multi-node cluster a worker connects to **one** gateway, but that gateway can
activate jobs from the **whole** cluster (every node aggregates peer-owned jobs over
the command stream — see `docs/distributed-scaling-design.md`). The dispatcher,
however, was **strictly local-first with a fixed peer order**:

- `command_stream.rs::dispatch_to_connection` activates the gateway's **own**
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
- **Stage 2 (planned):** backlog-weighted routing. Split `want` proportional to a cheap,
  gossip-fed last-known per-`(node, job_type)` backlog estimate, piggybacked on existing
  `activate_from_peer` responses (zero extra round-trips).
- **Stage 3 (optional):** push-to-demand. An overloaded node with few local subscribers
  offers work toward gateways with hungry credits.

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
> # Drain proof
> node --import tsx/esm src/fairness-ab.ts --workers 6 --maxpar 2 \
>   --target 0 --precreate 2400 --handler-ms 40 --duration 40
> # Steady-state overload
> node --import tsx/esm src/fairness-ab.ts --workers 6 --maxpar 2 \
>   --target 700 --precreate 1000 --handler-ms 30 --duration 25 --warmup 5
> ```

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

_Not yet implemented._ To be recorded here against the **Stage 1** numbers above as the
new baseline, using the same `fairness-ab.ts` methodology. Hypothesis: backlog-weighted
routing matches Stage 1 fairness while recovering some of the overload-case throughput by
steering budget toward the genuinely-deepest backlog instead of an even split.

## Stage 3 — performance: baseline vs after

_Not yet implemented._ To be recorded here against Stage 2 as the baseline.

## Consequences

- **Default-off** keeps every existing benchmark, the single-node path, and CI
  byte-identical; operators opt in per the codebase convention (admission / backpressure
  are likewise opt-in).
- **Operational mitigation meanwhile:** spreading workers across gateways already evens
  activation without the flag; Stage 1 is for the single-gateway-attachment case.
- **Known limitation — REST path not yet fair.** Stage 1 changes only the command-stream
  dispatcher (`dispatch_to_connection`). The REST long-poll activation
  (`activate_jobs_impl`) has the identical local-first / fixed-order bug and is a Stage-1
  follow-up. The benchmark uses the stream transport specifically to exercise the fixed
  path.
- **No correctness change.** Leases remain exclusive on each partition's single-writer
  actor; stale routing costs at most an occasional empty `activate_from_peer` RPC, never a
  double-lease.

## References

- Code: `server/src/command_stream.rs` (`fair_plan`, `activation_fairness`,
  `dispatch_to_connection`), env `NANOBPMN_ACTIVATION_FAIRNESS`.
- Driver + artifacts: `ts-performance-matrix/src/fairness-ab.ts`,
  `results/fairness-ab/<ts>/{fairness-ab.json,backlog-timeseries.csv}`.
- Related: `docs/distributed-scaling-design.md` (job aggregation), `README.md` env table.
