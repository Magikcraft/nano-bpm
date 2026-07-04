# Nano — An Advanced Research Prototype Engine

**Status: Advanced Research Prototype.** Nano is a distributed BPMN engine that
speaks the Camunda 8 API. It is a research engine, not a product; it implements a
bounded, honestly-stated subset of Camunda 8 that is expanding continuously. The
performance figures below are from real runs on the stated configuration and are
reproducible from the load generator and switches in this repository
(`PERFORMANCE.md`).

**You can try it today** as a drop-in local-development replacement for `c8run`:
one small binary that gives you the Camunda 8 API to build and test against,
for as long as the subset it implements covers what your project uses.

## Features

- **Small memory footprint** — a sub-50 MB binary that idles at ~10 MB resident
  and holds ~400 MB per node at 100k process-instances/s across three nodes.
- **Sub-second cold start** — a native binary with no JVM warm-up, versus a
  ~1.5–2 GB local Camunda 8 stack.
- **Self-optimizing operation** — most operational tuning is *derived* rather than
  set: the engine decides from live signals what a human would otherwise tune by
  hand, leaving one deployment-time business decision (how to behave at the
  capacity ceiling: shed admission, or absorb it and let latency rise).
- **Closed-loop adaptive scaling** — distributed clients and the cluster form one
  self-sizing control loop (AIMD backpressure + load-aware placement), so client
  fleets adapt themselves instead of being hand-tuned.
- **Fault-tolerant clustering** — Raft-replicated partitions (openraft) for
  high-availability operation.
- **Tunable durability** — a declared deployment choice trades data-safety
  guarantees for throughput (strict quorum + sync vs. relaxed leader-durable +
  async), measured end-to-end below.
- **In-browser engine** — the same engine core compiles to WebAssembly and runs in
  the browser (the console's in-page test-run).
- **Embeddable engine** — the same engine runs embedded in a worker, microservice,
  or application, or is invoked directly as a tool call.
- **Camunda 8 API-compatible** — a drop-in replacement behind the standard Camunda
  8 REST/gRPC contract.
- **Zeebe engine lineage** — the proven Zeebe execution model (single-writer actor
  per partition, event-sourced state), refined for a smaller footprint and higher
  throughput.

## The Falcon transport

Falcon is an **optional native transport**, offered *alongside* the standard API,
never in place of it. A Nano cluster speaks the full Camunda 8 gRPC/REST surface,
so an unmodified Camunda 8 client works unchanged. Falcon is the "faster if you
opt in" path.

It is a single persistent, bidirectional, credit-metered WebSocket per client that
carries both directions of work on one connection: a **demand-pull** lane where a
worker advertises how many jobs it can take and the server pushes only that many,
and a **submission** lane where instance creation draws on credits fed from the
engine's own processing headroom — so under load the server simply withholds
credits and the client stalls its intake, with no `503`/retry storm. Job delivery
and instance admission thus share **one backpressure account** rather than each
fending for itself. WebSocket (rather than gRPC) was chosen because it rides
ordinary HTTP(S) that existing proxies, gateways, and firewalls already handle,
and needs no per-language stub generation.

## Performance

On a cluster of 3× `c2-standard-16` (RF=3, 12 partitions), Nano sustains roughly
**95,000 process instances/s aggregate (~30,000 per node)** — one job per
instance, so also jobs/s — on the *default, strongest* durability setting (quorum
replication over fsync-before-ack journalling), with end-to-end p99 ≈ 2.1 s at a
deliberately deep in-flight window chosen to hold the cluster at saturation. The
ceiling is coordination-bound, not resource-bound: about a third of CPU is still
idle at that rate, the limit being contention around the single-writer actor and
the replication round-trips.

The two durability tiers, benchmarked fresh-state and interleaved to rule out
drift:

| Tier | replication | journal | aggregate throughput | p50 | p99 | peak RSS/node |
| --- | --- | --- | --- | --- | --- | --- |
| **strict** (default) | quorum | sync | ~104k PI/s | ~325 ms | ~1.92 s | 322–325 MB |
| **relaxed** | leader-durable | async | ~121k PI/s | ~275 ms | ~1.64 s | 333–343 MB |

Relaxed is ~15% faster and lower-latency because taking the quorum round-trip off
the completion path removes load from the very thing that binds; the cost is a
modest ~10–15 MB/node more peak memory. The whole engine — hot state, journal, and
SQLite read model — stays under ~345 MB resident per node even at saturation.

Latency and throughput are separable. A single low-concurrency worker sees a
**~10 ms/job floor (~100 jobs/s)** — one unpipelined quorum commit — while the
*same* path delivers **~33,000 jobs/s at 64 concurrent workers**, because group
commit amortizes the quorum across many in-flight jobs. The floor is the physics of
synchronous quorum, not a property of the engine; an operator constrained by it can
choose a lighter durability tier.

## Scope and status

Nano is an Advanced Research Prototype. It deliberately keeps Camunda 8's
client-facing semantics and Zeebe's battle-tested execution model, and revisits
only a few choices underneath a fixed, compatible contract. It is usable today for
local development against the subset it implements; it is not a general replacement
for a production Camunda 8 deployment. Design rationale for individual choices lives
in the repository's ADRs (`docs/adr/`), and the measurement details behind the
figures above are in `PERFORMANCE.md`.
