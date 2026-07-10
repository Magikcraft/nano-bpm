# ADR 0018 — Write-domain granularity vs. a BEAM/Erlang rewrite (research)

Status: **Proposed — research / exploration.** No code change is decided here. This
ADR records the analysis of an out-of-the-box option (rewrite the engine in
Erlang on the BEAM VM to escape the single-writer ceiling) and the alternative it
points to (finer-grained write domains in Rust). It is gated on the per-command
profiling from `907f061` (`NANOBPM_CMD_PROFILE`) confirming *where* the residual
per-command cost lives.
Date: 2026-07-09.
Relates to: ADR 0002 (leader-local activation), ADR 0014 (create-placement
protection and load awareness), ADR 0017 (worker-concurrency governor),
`server/src/deepthi.rs` (the single-writer actor), `server/src/raft.rs` (batch
apply on the actor), `server/src/cmd_profile.rs`, `PERFORMANCE.md`, commit
`79a350d` (congestion-collapse fixes), commit `907f061` (per-command profiling).

## Context

Sustained-load diagnosis (see `PERFORMANCE.md` and the 1 Hz DuckDB time-series
investigation) established the shape of the **create/complete congestion
collapse**: as the active-instance backlog grows, both instance creation *and*
job completion slow down together, and completion throughput inverts (≈100 k/s at
active < 10 k → ≈7–14 k/s at active > 100 k).

Two candidate root causes were **falsified** by A/B:

- **I/O / durability** — exonerated: `fsync` stayed flat (1.35–2.69 ms) across the
  whole collapse.
- **On-actor lean snapshot** — falsified: disabling snapshots
  (`NANOBPMN_SNAPSHOT_INTERVAL_MS=0`) did *not* remove the multi-second actor
  stalls (max **11,414 ms**, *worse*) and left collapse-regime completions
  statistically identical (10,880 vs 10,890 /s); only +29 % aggregate throughput
  (snapshot compaction overhead, not the stall).

Every obvious O(active) *scan* on the actor is already indexed and deployed
(`79a350d`): the tick pre-check gates on `activated_jobs` (O(activated), not
O(total backlog)); `ActivateJobs` walks the per-type activatable index
`take(max_jobs)` (O(max_jobs)); `ExpireJobs` iterates `activated_jobs`. Active
backlog admission is on by default as a backstop.

What remains is a **residual** per-command cost that grows with the resident
working set: measured ≈**4.4×** per-op time over a ≈**79×** growth in active
instances. That ratio is *not* an O(N) scan (an O(N) scan would grow ≈79×); it is
consistent with the **memory-hierarchy cost** of mutating a much larger resident
state — cache/TLB misses, larger hash tables, allocator pressure on a ~14 GB
heap — funnelled through the **single writer per partition**.

The single writer is deliberate (`deepthi.rs`): one engine thread per partition
serializes `CreateInstance` (Low priority) and completion/activation (High
priority) to give linearizable per-partition state and completion-first
scheduling. It is the exact coupling that makes creates and completes move
together, and — the open question — the exact place the residual O(active) term is
paid.

## The out-of-the-box option: rewrite on the BEAM (Erlang)

The instinct: "we seem to be hitting a limit of the actor model — use the *OG*
actor model on the BEAM." Evaluated honestly, this is unlikely to be a decisive
win **for throughput** (the metric we optimize) and would likely regress it. The
reasoning:

### Why BEAM does not address our bottleneck

1. **The bottleneck is not message-passing overhead; it is a chosen
   serialization + a physics cost.** In Erlang the natural model is a `gen_server`
   per partition — itself a single process serializing its mailbox. Same coupling
   as `deepthi.rs`. The "OG actor model" does not parallelize a single logical
   writer; it gives you a mailbox, which we already have.

2. **The residual cost is language-agnostic.** Cache/TLB misses and larger-map
   probe costs cost the same regardless of runtime. Our multi-second stalls are
   *not* GC pauses (Rust has no GC; `fsync` is flat), so BEAM's per-process GC —
   its headline latency feature — does not touch them.

3. **Raw hot-path compute is ~10–30× slower on BEAM than native Rust.** Our hot
   path is `serde` decode/encode, hash-map mutation, and event application —
   exactly the work BEAM is slowest at. For a throughput-first engine competing
   with the JVM/native Camunda 8, betting on BEAM compute bets against the goal.

4. **Message passing copies between per-process heaps.** Our completion path moves
   events and variables; modelling instances as isolated processes reintroduces
   copy costs we do not pay behind a shared-state actor.

### What BEAM *would* give us

- **Preemptive scheduling** (≈2000-reduction quanta) → better tail-latency
  fairness: one fat command cannot starve others as badly. Real, but our expensive
  paths are already indexed, so the win is modest.
- **Cheap processes** (millions) → the genuinely interesting idea: model **each
  process instance as its own actor**, so the write domain is per-instance rather
  than per-partition. This *breaks the single-writer coupling* — but it is an
  **architecture** change, not a **language** change.
- Built-in distribution, supervision trees, hot code reload — none of which are
  our current constraint (we have Raft, and the crash-recovery story is durable).

### Cost / risk

A rewrite discards openraft integration, the type system, the whole SDK ecosystem,
and every perf result to date, in exchange for a scheduling-fairness improvement
that does not attack the diagnosed root cause. The one idea worth taking — per-
instance write domains — is implementable in Rust.

## Decision (proposed)

**Do not rewrite on the BEAM.** Treat the pull toward Erlang as a signal that the
real lever is **write-domain granularity**, and pursue it natively:

1. **First, confirm the mechanism (blocking prerequisite).** Use the per-command
   profiling shipped in `907f061` (`NANOBPM_CMD_PROFILE`): `nanobpm_cmd_seconds`
   and `nanobpm_cmd_alloc_bytes` by command kind, regressed against
   `nanobpm_engine_cardinality`. The discriminator:
   - per-command **time** rises with active backlog while **alloc bytes/command**
     stays flat ⇒ hashmap-probe / cache-miss (bigger maps, no extra allocation);
   - **alloc bytes/command** rises ⇒ allocator / copy.
   The remedy differs by branch (see below), so this measurement gates any
   restructure.

2. **Then, if the cost is genuinely per-writer O(active), shrink the write
   domain** — steal the good idea from Erlang without the rewrite. Candidate
   designs, in increasing order of disruption:

   - **(a) Finer partitioning (cheapest).** Raise the partition count so each
     single writer owns a smaller resident set. Already supported
     (`NANOBPMN_PARTITIONS`); no new consistency model. Bounds map sizes and
     working set per actor at the cost of more Raft groups. This is the first
     experiment and may recover most of the headroom on its own.

   - **(b) Sharded write lanes within a partition.** Split a partition's engine
     state by instance-key hash into K independently-lockable sub-lanes behind one
     Raft log, so completions on disjoint instances proceed in parallel while the
     log stays linear. Keeps one replication group; adds intra-partition
     concurrency. Cross-instance operations (message/signal correlation, which fan
     out across instances) must route to a coordinator or run as a lane-crossing
     step — the main design hazard.

   - **(c) Per-key (per-instance) actors (most BEAM-like).** Each instance is its
     own tiny actor over the partition's shared log. Maximum parallelism; must
     solve correlation ordering, deterministic replay across many actors, and how
     per-actor state maps onto snapshot/recovery. This is the "millions of
     processes" model in Rust (tokio tasks / an actor crate) — attractive only if
     (a) and (b) prove insufficient.

If, instead, the residual is **allocator/copy** dominated (branch 1, second case),
the remedy is orthogonal to granularity: reduce per-command allocation
(arena/slab reuse for the apply path, shrink `serde` intermediate copies, tune
jemalloc arenas), and sharding would help far less. This is why the measurement
gates the decision.

## Consequences

- **No rewrite.** We keep native throughput, Raft, the SDKs, and the type system.
- **The next experiment is measurement, not restructuring:** run a
  `NANOBPM_CMD_PROFILE` load test to attribute the residual per-command cost by
  kind and split allocator vs. hashmap/cache. Only then choose among
  finer-partitioning (a), sharded lanes (b), or per-key actors (c) — or the
  allocation-reduction path if allocator-dominated.
- **Finer partitioning is the cheap first move** and requires no new consistency
  model; it should be trialled before any of the deeper restructures.
- The BEAM's one clear advantage we lack — preemptive fairness so a single fat
  command cannot starve the mailbox — is worth noting as a *separate* future
  consideration (e.g. yielding long applies), independent of the throughput
  question.

## Open questions

- Does finer partitioning alone flatten the 4.4× residual, or does per-partition
  resident state still dominate at the per-actor level?
- For sharded lanes / per-key actors: how do cross-instance correlations
  (`CorrelateMessage`, `BroadcastSignal`) preserve deterministic ordering and
  replay across concurrent write domains sharing one Raft log?
- Is the residual allocator-bound (favouring allocation reduction) or
  cache/probe-bound (favouring granularity)? The `907f061` instrumentation answers
  this and must run before committing to any design above.
