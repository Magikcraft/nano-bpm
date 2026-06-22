# ADR 0003 — Write-path durability tiers (leader-durable replication)

Status: **Proposed (spike pending).**
Date: 2026-06-23.
Relates to: ADR 0002 (leader-local activation & lease digest), `docs/distributed-scaling-design.md`, `server/src/journal.rs`, `server/src/raft.rs`.

## Context

nanobpmn already separates **two orthogonal durability dimensions**:

1. **Local journal durability** — `NANOBPMN_DURABILITY=sync|async` (`journal.rs`):
   `sync` acks after `fsync` (survives power loss, ~4 ms media barrier on the
   critical path); `async` acks after the page-cache `write_all` and amortizes
   `fsync` onto a periodic cadence (survives process crash via replay; an OS
   crash / power loss can lose the bounded unfsynced tail). The doc already notes
   this "mirrors Zeebe's async-exporter model."
2. **Replication durability** — per-partition **Raft** (`raft.rs`): every durable
   command is proposed to the partition leader and the client is acked only after
   a **majority quorum commits AND the batch applies** (the batcher's
   `raft.client_write(batch).await` returns the per-command apply results, which
   become the SDK reply). This survives **node loss** (a minority can fail and the
   committed state is intact on the surviving majority).

The cost concentrates in dimension 2. Under RF=3 every `create`/`complete` waits
for a cross-node quorum round-trip on the **client critical path**. As ADR 0002 /
the single-worker investigation showed, this is fine under concurrency (group
commit amortizes — 33k jobs/s @64 workers) but imposes a hard **latency floor**
on low-concurrency / single-worker work (~100 jobs/s ≈ ~10 ms/job ≈ one quorum
round-trip + forward hops). Raft is not inherently slow — the cost is
**synchronous quorum on an un-pipelined path**.

There is no equivalent of the journal's `sync|async` knob for the **replication**
dimension: today replication is always full-quorum-consensus. That is the gap
this ADR addresses.

## Decision (proposed)

Introduce a **replication durability tier**, e.g.
`NANOBPMN_REPLICATION=quorum|leader-durable` (default **quorum** = today's
behaviour; single node / RF=1 unaffected). This is the Kafka `acks=all` vs
`acks=1` model applied to the workflow command log, and the natural sibling of
the existing local `DURABILITY=sync|async` knob.

| | ack waits for | survives | data-loss window |
| --- | --- | --- | --- |
| **quorum** (today) | majority replicate + apply | node loss (minority) | none (committed = durable on a majority) |
| **leader-durable** | leader local fsync + apply; replicate **async** | leader crash-restart | un-replicated tail on **simultaneous leader loss** before catch-up |

`leader-durable` takes the network round-trip off the client critical path: the
leader appends + (locally) makes durable + applies + acks, and ships the log to
followers in the background, tracking a **committed (acked) vs replicated
(quorum-durable) watermark**. Followers tail the leader's log. On leader failure
a follower with the most complete log is promoted; any tail the dead leader acked
but had not yet replicated is **lost** — a bounded window, the same shape as the
local `async` fsync window, but across the *replication* axis instead of the
*media* axis.

This is consistent with the system's existing **at-least-once** contract: a lost
completion tail means those jobs' leases simply expire and they are redelivered
(idempotent workers already tolerate this); a lost create tail means those
instances were never durably admitted (the producer, which is at-least-once too,
retries). What `leader-durable` must **not** do is violate ordering or
exactly-once *within* what it does ack — the leader's local log is still the
single ordered source of truth per partition.

### Implementation options for the spike (explicitly open)

The ack/replication coupling is the crux; openraft's `client_write` returns only
after quorum commit. Options, cheapest → deepest:

1. **Flexible / smaller write-quorum within consensus.** Keep openraft but reduce
   the effective write quorum (e.g. ack on leader + 0 followers, replicate to the
   rest lazily). Modest change if the library supports it; modest win (RF=3
   quorum is already just leader + 1 fastest follower, so the delta is one RTT).
2. **Parallel async replication path (primary-backup log shipping).** Ack on the
   leader's local durable append; stream the committed log to followers
   out-of-band (not gated by consensus), with a watermark + catch-up + a
   leader-election that picks the longest log. This is the real `acks=1` design
   and the big win, but it is a **second replication mode** alongside Raft, not a
   tweak to it — more code, and its own failover correctness proof.
3. **Speculative apply + deferred quorum.** Apply and reply optimistically on the
   leader, confirm quorum in the background, reconcile on the rare divergence.
   Highest complexity / risk; likely not worth it.

The spike should prototype option 1 first (measure the delta cheaply), and scope
option 2 as the follow-on if the ceiling justifies it.

## Are the digest and leader-durable replication complementary? — Yes.

They are **orthogonal and composable**, because they act on **different data with
different consistency needs**, and together with leader-local activation they form
a single coherent "spend consensus only where it buys a real guarantee" story:

| Layer | Data it governs | What it changes | Guarantee traded |
| --- | --- | --- | --- |
| **Leader-local activation** (ADR 0002 A, shipped) | the **lease** (soft state) | removes activation/expiry from the Raft log (3→2 commits/job) | failover lease visibility (→ immediate redelivery) |
| **Lease digest** (ADR 0002 B, proposed) | the **lease** (soft state) | best-effort async broadcast of leases so a new leader honours deadlines | restores most of that visibility, best-effort |
| **Leader-durable replication** (this ADR, proposed) | **durable progress** (create/complete/fail/throw/timers) | acks at leader-durable, replicates async | node-loss durability of the un-replicated tail |

Why they compose rather than collide:

- **Disjoint data.** The digest concerns the **activation lease**, which is *not
  durable progress* — it was never meant to survive as committed history (it's a
  lease; expiry → redelivery). Leader-durable concerns the **committed event
  log** — the actual progress. One never reads or writes the other's state, so
  enabling both does not create a new consistency interaction beyond the union of
  their (independent) failover windows.
- **Shared transport, no extra cost.** Both are *async background replication of
  state the leader already holds*. The lease digest can ride the **same async
  replication stream** that leader-durable introduces for the log tail (and/or the
  ADR-0001 peer piggyback channel). So adopting leader-durable actually gives the
  digest its transport for free — a positive synergy, not just non-interference.
- **Same design philosophy, different axis.** Leader-local activation removed the
  lease from consensus; leader-durable makes the *remaining* durable commits ack
  without waiting for quorum; the digest cheaply restores the lease's failover
  quality that leader-local gave up. Each addresses a distinct cost on the
  "trade failover strength for throughput" axis without overlapping.

**Combined failover semantics (must be stated for operators).** With *both* on,
a sudden leader loss can simultaneously (a) lose the un-replicated **completion
tail** (those jobs redeliver — at-least-once) and (b) lose/approximate the
**in-flight leases** (those jobs redeliver immediately, or after the digest
deadline). Both resolve to *redelivery of idempotent work* — the same failure
class the system already requires workers to tolerate — so the union is
**bounded and safe**, just wider than either alone. Operators who cannot tolerate
either window keep the strong defaults (`REPLICATION=quorum`,
`REPLICATE_ACTIVATION=1`).

## Measurement methodology

Same authoritative driver as ADR 0002 (`ch2-workers`, 3-node RF=3 cluster,
release binary). Two dimensions to report against the `quorum` baseline:

1. **Throughput / latency** across the worker ramp (1/4/8/16/32/64) — expect the
   biggest delta at **low concurrency** (the single-worker latency floor is what
   leader-durable removes; under heavy concurrency group commit already
   amortizes, so the delta narrows).
2. **Failover loss window** — kill the leader under sustained load and measure the
   acked-but-lost tail size (should be ≈ the in-flight/un-replicated batch count)
   and the redelivery/duplicate-execution count, confirming it stays bounded and
   that no *committed-and-replicated* progress is lost.

## Consequences

- **Default-off / default-strong.** `REPLICATION=quorum` keeps every existing
  benchmark, the single-node path, and CI byte-identical (codebase convention:
  admission, backpressure, fairness, and activation replication are all opt-in).
- **Honest guarantee.** `leader-durable` is explicitly weaker than `quorum`
  (bounded loss on simultaneous leader loss), exactly analogous to the local
  `DURABILITY=async` window but on the replication axis. It is *not* a free lunch;
  it is a per-deployment trade for workloads whose work is idempotent and whose
  SLA prioritizes latency/throughput over zero-loss failover.
- **Composes with ADR 0002.** Orthogonal data, shared async transport; the three
  knobs (`REPLICATE_ACTIVATION`, lease `digest`, `REPLICATION`) can be set
  independently, and the combined failover window is the bounded union of the
  individual ones.
- **Scope boundary.** The disaggregated shared-log architecture (Zeebe/Temporal
  style: external BookKeeper/Kafka log + stateless compute) remains explicitly
  **out of scope** — it offers the highest throughput ceiling but sacrifices
  nanobpmn's self-contained single-binary value prop. This ADR deliberately stays
  within the embedded model.

## References

- Code: `server/src/journal.rs` (`DurabilityMode`, `durability_mode_from_env`,
  `writer_loop_async` — the local-axis precedent), `server/src/raft.rs`
  (`Batcher`, `raft_config`, `client_write` ack coupling — the replication axis to
  tier). Proposed env: `NANOBPMN_REPLICATION=quorum|leader-durable`.
- Related: ADR 0002 (lease digest — shares the async transport), ADR 0001 (peer
  piggyback channel), `docs/distributed-scaling-design.md`.
