# ADR 0019 — Leadership hand-off reclaim for a rejoining static owner

Status: **Proposed — phased implementation in progress (opt-in behind
`NANOBPMN_RECLAIM_HANDOFF`, default-off).** Phases A–C + E landed on branch
`fix/rebalance-recovered-node-backlog`; the fresh-receiver + no-self-promote-
under-incumbent + bounded write-pause work described here is being implemented.
Date: 2026-07-15.
Relates to: ADR 0003 (write-path durability tiers / leader-durable replication),
ADR 0002 (leader-local activation & lease digest), ADR 0001 (cluster job
activation fairness), ADR 0014 (create-placement protection & load awareness),
ADR 0016 (Falcon protocol), `server/src/main.rs` (`raft_bootstrap`,
`leader_durable_recovery_tick`, `probe_incumbents_for_owned`, `perform_handoff`,
`handle_handoff_request`, `rebuild_as_receiver`), `server/src/raft.rs`
(`bootstrap_member`, `add_learner`, `change_voters_to`, `replication_lag`),
`server/src/peer.rs` + `server/src/falcon.rs` (hand-off wire frames), commits
`e6605c5` (Phase A), `d28d5b5` (Phase B), `d0c41b3` (Phase C), `c15c4ba`
(Phase E).

## Context

In **leader-durable** replication (ADR 0003) each partition is a **single-voter**
Raft group: the owning node is the sole voter and the other replicas are
non-voting learners that tail the log. A write acks on the leader alone (`acks=1`)
and ships to learners asynchronously — this is the whole point of the tier (no
cross-node quorum on the client critical path).

Partition **ownership is static** (a fixed `leader_of(partition)` in the
topology). When the owner is lost, a surviving learner is **promoted** to a fresh
single-voter group (leader-local failover, ADR 0001/0002) so the partition keeps
serving. This is correct and fast — but it creates a latent hazard on **rejoin**.

### The failure: a two-lineage election war on rejoin under load

When the static owner (call it node18) reboots under sustained load (~28 k/s), the
legacy reclaim path had it `initialize` its own single-voter group for every
partition it owns — **while the failover incumbent's group for the same partition
is still alive and leading a different lineage**. Two single-voter groups for one
partition then fight:

- node18's group self-elects (it is its own sole voter) and campaigns.
- The incumbent's group is the one the rest of the cluster is replicating.
- They churn raft **terms** (observed up to 574) and leave the partition
  effectively **leaderless** for the entire load window.
- node18 gets **zero new creates**, `leader_reject` climbs into the 100k–170k
  range, and everything only converges the instant the firehose stops.

This is a direct consequence of the single-voter design: standard multi-voter
Raft never has two lineages because membership is fixed and a rejoining node is
the *same voter in the same group*. We keep single-voter for its latency win
(ADR 0003), so we must solve reclaim explicitly.

### How Zeebe/Atomix handles the equivalent (comparison)

Zeebe partitions are **standard multi-voter Raft groups** (RF=3, quorum-based,
*fixed* membership). Two consequences frame our decision:

1. **No competing lineage, ever.** A rejoining broker is already a voter in the
   same group; it catches up **as a plain follower** via AppendEntries (or
   `InstallSnapshot` if it has fallen behind the compacted log). The two survivors
   keep quorum and keep serving throughout.
2. **Returning leadership is a separate, best-effort step** (the Rebalancing API +
   priority election), and Zeebe makes two revealing concessions:
   - It **accepts a brief leaderless election window** during a transfer
     ("the affected partition has no leader … cannot process, export, or accept
     new commands").
   - It **does not pause or throttle writes to force catch-up**. If the target
     cannot catch up under load, the rebalance is a **best-effort no-op** —
     leadership stays put, **no storm, no data risk** — and you retry later.

The lesson: the *storm* is not inherent; it comes entirely from our **self-promote
fallback** competing with the incumbent. Making failure-to-catch-up **benign**
(stay a follower, retry) removes the storm without any write pause. A bounded
write-pause is then only an *accelerator* for promptness, and is a *comparable-or-
smaller* availability concession than Zeebe's own leaderless-election window.

## Decision

Reclaim a statically-owned partition on rejoin via an **openraft leadership
hand-off** rather than a competing self-promote, gated behind
`NANOBPMN_RECLAIM_HANDOFF` (default-off ⇒ byte-identical legacy self-promote). The
design has three required parts and one promptness accelerator.

### 1. Boot incumbent-led owned partitions as fresh receivers (no lineage resurrection)

On boot, before forming groups, `probe_incumbents_for_owned` solicits co-replicas
(Falcon, ADR 0016) and, for each owned partition a **reachable peer currently
leads**, defers instead of initializing (Phase E). Crucially, a deferred partition
must be **hosted with a fresh/empty raft log** (`bootstrap_member(.., None, ..)` ⇒
`MemLogStore`), **discarding node18's stale on-disk lineage**. Hosting it with the
restored on-disk log (the rejoin/rehost fast-path) resurrects an *initialized*
single-voter membership that (a) campaigns against the incumbent ("lost leadership
during catch-up") and (b) cannot be reconciled by AppendEntries against the
incumbent's newer lineage (a storage `DefensiveError: LogIndexNotFound`). In
leader-durable mode the **incumbent is authoritative**; node18's un-shipped tail
was already accepted bounded loss at failover, so discarding the divergent log is
correct. The restored-on-disk-log optimization is retained **only** for the
self-promote / no-incumbent path (node18 genuinely resumes its *own* lineage).

### 2. No self-promote under a live incumbent (best-effort, Zeebe-style)

The recovery tick (`leader_durable_recovery_tick`) requests a hand-off for every
deferred partition. If the bounded catch-up does **not** complete, node18 **stays
a receiver and retries** on a later tick — it does **not** fall back to a competing
self-promote while a reachable incumbent still leads the partition. Self-promote is
reserved for the genuine cases: no reachable incumbent (real failover / cold
owner). This alone eliminates the election storm.

### 3. Hand-off completion (incumbent side)

`perform_handoff` (incumbent): `add_learner(node18)` → poll `replication_lag` to
within `HANDOFF_LAG_THRESHOLD` (bounded by `HANDOFF_CATCHUP_TIMEOUT`) →
`change_voters_to([node18])` (demotes the incumbent, which steps down) → advance
the fence epoch to `(incumbent+1, node18)`. If the incumbent loses leadership mid-
catch-up the hand-off aborts cleanly and node18 retries (part 2).

### 4. Bounded per-partition write-pause during catch-up (accelerator, configurable)

Under sustained load the incumbent's log can grow faster than the learner catches
up, so the bounded catch-up never reaches zero lag and the hand-off keeps timing
out (a livelock — the same dynamic as the original snapshot-install defensive bug,
one layer up). To make the hand-off complete on the **first** attempt rather than
waiting for a load lull, the incumbent applies a **brief, bounded, per-partition
write-pause** for the target partition during catch-up so the log quiesces long
enough for the learner to converge, then transfers and lifts the pause.

- Controlled by env var **`NANOBPMN_HANDOFF_WRITE_PAUSE_MS`** (see the
  implementation for the exact default); `0` disables the pause (pure Zeebe-style
  best-effort). **On by default** initially, so reclaim is prompt out of the box;
  we can dial it down once soaks confirm reliability.
- Only the target partition is paused (~1/12th of cluster write traffic in the
  12-partition topology), only during the sub-second-to-~2s hand-off, only on
  rejoin. Paused creates steer to other owners; paused completions are retried by
  workers (at-least-once), so no work is lost.
- This is a *smaller* availability concession than Zeebe's leaderless-election
  window, and vastly cheaper than the status-quo 100k+ `leader_reject` storm.

## Consequences

- **Reclaim is prompt and storm-free** when the flag is on: node18 defers (no
  competing group), catches up as a clean receiver, and the hand-off completes
  within the write-pause window; `leader_reject` flatlines within seconds and raft
  terms stay flat. With the pause disabled, reclaim degrades gracefully to Zeebe-
  style best-effort (may wait for a lull) rather than storming.
- **Bounded, deliberate write-pause** on the target partition during hand-off is
  the price of promptness; it is env-tunable and defaults on.
- **Default-off flag** keeps production byte-identical to the legacy self-promote
  reclaim until the phased soaks green-light enabling it by default.
- **Correctness note (bounded loss):** discarding node18's divergent on-disk log
  for an incumbent-led partition is only sound because leader-durable already
  accepts loss of the sole voter's un-replicated tail on failover (ADR 0003). This
  ADR does not change that contract; it makes the rejoin path consistent with it.

## Open items / follow-ups

- Confirm via GCP soak (clean-journal restart) that parts 1–4 together yield:
  node18 creates climb, `leader_reject` flat, terms flat, hand-off **completes**
  (no self-promote fallback), 0 panics / 0 restarts.
- Tune the default `NANOBPMN_HANDOFF_WRITE_PAUSE_MS` from soak data (start
  generous, tighten).
- Phase D (harden the requester side: joint-config-aware fallback) remains pending
  per the phased plan.
- Longer-term: whether to offer a multi-voter (quorum) reclaim path that avoids the
  single-voter two-lineage hazard entirely, at the ADR 0003 latency cost — out of
  scope here.
