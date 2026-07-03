# ADR 0003 — Write-path durability tiers (leader-durable replication)

Status: **Accepted — option-1 spike + app-driven auto-recovery implemented (opt-in, default-off).**
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

### As built (option-1 spike)

Implemented behind `NANOBPMN_REPLICATION=quorum|leader-durable` (default
`quorum`; also accepts `leader_durable` / `acks=1`). The resolved choices:

- **Mechanism = openraft learners (option 1).** In `leader-durable` mode each
  partition leader forms its group with **itself as the sole voter** and adds the
  other replicas as **learners** (`Raft::add_learner(.., blocking = false)`),
  instead of initializing every replica as a voter. The write quorum is therefore
  1 (the leader), so `client_write` acks after the leader's own local durable
  append + apply, with **no follower round-trip on the client critical path**.
  Learners still receive the log via openraft's normal replication, asynchronously
  and off the ack path — this is the `acks=1` shape using openraft primitives, no
  second replication engine.
- **Wiring.** `ReplicationMode` + `replication_mode_from_env()`
  (`server/src/main.rs`), a `replication_mode` field on `ServerImpl`, and a branch
  in `raft_bootstrap` that selects the voter set (`{leader}` vs all replicas) and
  registers the rest as learners. `RaftPartition::add_learner` wraps openraft and
  swallows the idempotent "already a member" error (`server/src/raft.rs`). The
  `Batcher` / `propose` path is unchanged — it simply commits faster because the
  quorum is smaller.
- **Failover caveat (addressed by app-driven auto-recovery, below).** A single
  voter means openraft cannot auto-elect a new leader if that leader is lost: there
  is no voting majority among learners. The option-1 spike on its own is the
  **cheap, measurable throughput/latency vehicle**; automatic failover is supplied
  by the app-driven promotion supervisor described in the next section (option 2's
  spirit, layered on top of the spike rather than replacing it). `quorum` (the
  default) retains openraft-native auto-failover. Switching tiers across restarts on
  the same log dir is unsupported (the committed membership differs) — use a fresh
  cluster.
- **Verification.** Server test
  `leader_durable_acks_on_the_sole_voter_and_ships_to_learners` (3-node RF=3):
  asserts partition 0's membership is exactly one voter (the leader) + two
  learners, that a create acks on the leader alone, and that the acked entry still
  ships to a learner's replica engine asynchronously. Full suite green; default
  (`quorum`) path byte-identical.

### As built (option-2 follow-on: app-driven auto-recovery)

Because a sole-voter openraft group cannot self-elect — and no node can change
membership without a live leader — automatic failover for `leader-durable` is
**app-driven**, not delegated to openraft. A promotion supervisor on every node
detects a leaderless partition and has the deterministic surviving successor
rebuild the group as a fresh sole-voter group seeded from its replica engine.

- **Detection.** `spawn_leader_durable_recovery()` runs a 500 ms tick
  (`leader_durable_recovery_tick(grace_ticks, &mut misses)`); it is wired in
  `main()` after `spawn_raft_bootstrap()` and only does work in `leader-durable`
  mode. A partition is "leaderless" when its group's `current_leader` is `None` or
  names a peer that is not `peer_reachable`. After `grace_ticks` consecutive
  leaderless passes (`NANOBPMN_LEADER_DURABLE_GRACE_TICKS`, default 3) the node
  acts. The failure detector is `peer_reachable(node)` (a dialable Falcon
  uplink; `true` for self).
- **Single-promoter safety.** `designated_successor(p)` is the first node in
  `replicas_of(p)` order (leader-first, deterministic) that is reachable. Every
  survivor computes the same successor from the same replica order with no
  coordination, so **at most one node promotes**.
- **Promotion.** `promote_partition(p, epoch)` takes the engine actor that already
  holds the replicated state (its replica engine, or the owned actor), shuts down
  the stale group, and rebuilds a fresh single-voter group via
  `RaftPartition::bootstrap_member(.., log_dir = None)` — a clean in-memory
  `MemLogStore`, since the engine's own journal is the local durability source. It
  `initialize({me})` (self-elects immediately), replaces the registry entry, then
  `add_learner`s the reachable survivors so new writes ship to them. Routing
  (`route_by_leader` / `led_partitions`) follows automatically once the promoted
  node reports itself leader.
- **Fencing (epoch + node-id tiebreak).** A monotonic per-partition fence stored as
  `(epoch, leader_node)`. The promoter broadcasts a
  `ClientFrame::Promote { partition, epoch, leader_node, leader_addr }` frame;
  `handle_promotion` adopts a peer's announcement iff it **wins the fence** — a
  strictly-higher epoch, *or the same epoch from a lower node id* — and rebuilds the
  receiver as a fresh learner of the winner. A stale or tie-losing leader therefore
  steps down and rejoins, while the standing winner instead `add_learner`s a
  tie-losing sender so it rejoins for durability. **Higher epoch wins; equal epochs
  break by lowest node id** ⇒ a single leader always emerges.
- **Loss window / split-brain (the inherent acks=1 trade, documented in code).**
  Any tail the dead leader acked but had not yet shipped to the successor's replica
  engine is gone — bounded and at-least-once (a lost completion redelivers; a lost
  create was never durably admitted, so the producer retries). Under a *true network
  partition* two survivors can each believe the other is dead and both promote. This
  is the fundamental acks=1 limitation, not a bug — but the fence **always
  reconverges to one leader**: a strictly-higher epoch wins, and a *symmetric* split
  that yields two **equal-epoch** promotions is resolved deterministically by lowest
  node id (the tie-loser adopts the winner and steps down to a learner). The price is
  bounded loss of the losing side's un-shipped acked tail — never permanent
  divergence, and never a stuck double-leader.
- **Verification.** Two server tests (3-node RF=3):
  - `leader_durable_auto_recovers_a_leaderless_partition_without_manual_intervention`:
    a create acks on the sole voter and ships to node 1's replica engine; node 0 is
    then killed (Raft groups shut down + a `PeerSet` fault-injection seam,
    `fail_node`, marks it unreachable from the survivors — faithfully simulating the
    post-mortem state, since `axum::serve` drives accepted connections on detached
    tasks that outlive an aborted listener). Driving the supervisor on the survivors,
    node 1 (the deterministic successor) self-promotes, node 2 stands down, and the
    carried-over in-flight job is activated and completed on the new leader — proving
    write availability is restored with no manual intervention.
  - `leader_durable_split_brain_reconverges_to_one_leader_via_epoch_tiebreak`: a
    *symmetric* split (node 0 down AND node 1 / node 2 mutually unreachable) makes
    both survivors self-promote partition 0 at the **same epoch** — a genuine
    split-brain. On heal, exchanging the two `Promote` frames collapses it: node 1
    (lower id) keeps leadership, node 2 adopts the winner's fence and steps down. One
    leader, one history.

  Full suite green; default (`quorum`) path byte-identical.

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
