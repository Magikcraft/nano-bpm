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

**The returning owner must keep its receiver across retries.** When node18
receives a hand-off ack it must *not* rebuild its member if it already hosts the
partition as a non-leader (learner/follower): `rebuild_as_receiver` shuts the
member down and re-bootstraps it with an empty log store, discarding everything
the incumbent has already replicated. Rebuilding on every ~4 s retry reset the
catch-up to zero each cycle, so it never converged (first-soak livelock). node18
now rebuilds as a receiver **once** (only when it holds no member, or still leads
a competing group) and thereafter keeps it, so replication accumulates across
attempts.

### 4. Bounded per-partition write-pause during catch-up (accelerator, configurable)

Under sustained load the single-voter incumbent retains only ~1 s of log — it
snapshots and **purges aggressively** (observed: `last_log − purged ≈ 1000`
entries). A returning owner that missed tens of seconds is far outside that
window, so it must catch up via a **full snapshot install**, not log streaming.
While writes continue, the leader's snapshot point keeps advancing, so the
learner re-snapshots forever and the catch-up livelocks — the lag never falls to
`HANDOFF_LAG_THRESHOLD` because the head moves ~thousands of entries per second.
(The mechanism is proven by the fact that node18 converges to the exact head
within seconds the instant load subsides.)

The fix is a **per-partition completion write-pause** that freezes the log head
for the **whole** catch-up attempt (`HANDOFF_WRITE_PAUSE_MS` is sized a hair above
`HANDOFF_CATCHUP_TIMEOUT`), so the snapshot point stops moving, one snapshot
install completes, the tail drains, and lag reaches zero. The catch-up window is
sized to cover a snapshot install (seconds, not the original 4 s). The lease — and
thus the pause — is released the instant the hand-off completes or aborts, so the
real stall is only as long as the catch-up actually takes.

- Controlled by env var **`NANOBPMN_HANDOFF_WRITE_PAUSE_MS`** (default sized to
  cover the catch-up window; see the implementation); `0` disables the pause
  (pure Zeebe-style best-effort). **On by default** initially, so reclaim is
  prompt out of the box; we can dial it down once soaks confirm reliability.
- Only the returning owner's partitions are paused (its share of cluster write
  traffic), only during the hand-off, only on rejoin. Paused creates steer to
  other owners; paused completions are retried by workers (at-least-once), so no
  work is lost.
- This is a *smaller* availability concession than Zeebe's leaderless-election
  window, and vastly cheaper than the status-quo 100k+ `leader_reject` storm.

## Consequences

- **Reclaim is prompt and storm-free** when the flag is on: node18 defers (no
  competing group), catches up as a clean receiver, and the hand-off completes
  within the write-pause window; `leader_reject` flatlines within seconds and raft
  terms stay flat. With the pause disabled, reclaim degrades gracefully to Zeebe-
  style best-effort (may wait for a lull) rather than storming.
- **Bounded, deliberate write-pause** on the target partition during hand-off is
  the price of promptness; it is env-tunable and defaults on. It must span a full
  snapshot install, so it is sized in seconds (≥ the catch-up window), not the
  original sub-2 s guess — the aggressive single-voter log purge means anything
  shorter cannot break the re-snapshot livelock under load.
- **Default-off flag** keeps production byte-identical to the legacy self-promote
  reclaim until the phased soaks green-light enabling it by default.
- **Correctness note (bounded loss):** discarding node18's divergent on-disk log
  for an incumbent-led partition is only sound because leader-durable already
  accepts loss of the sole voter's un-replicated tail on failover (ADR 0003). This
  ADR does not change that contract; it makes the rejoin path consistent with it.

## Open items / follow-ups

- Confirm via GCP soak (clean-journal restart) that parts 1–4 together yield:
  node18 creates climb, `leader_reject` flat, terms flat, hand-off **completes**
  (no self-promote fallback), 0 panics / 0 restarts. *Soak progress:* the storm is
  eliminated (leader_reject=0, self-promote=0, cluster stable); parts 3–4 iterated
  to fix the catch-up livelock (stop wiping the receiver; freeze the head for a
  whole snapshot install).
- Tune the default `NANOBPMN_HANDOFF_WRITE_PAUSE_MS` / `HANDOFF_CATCHUP_TIMEOUT`
  from soak data (start generous, tighten).
- Phase D (harden the requester side: joint-config-aware fallback) remains pending
  per the phased plan.
- Longer-term: whether to offer a multi-voter (quorum) reclaim path that avoids the
  single-voter two-lineage hazard entirely, at the ADR 0003 latency cost — or to
  soften the single-voter log-purge threshold during an active hand-off so the
  learner can stream instead of re-snapshotting — out of scope here.

### Soak 4 (binsha 9e323cd8, commit 96c7890) — storm ELIMINATED, completion still load-gated

Added part 5 (**leader log-retention accelerator**,
`NANOBPMN_RAFT_LAGGING_RETAIN`, default 400_000) + the `/tmp` snapshot-dir
disk-leak fix, then ran the high-load nodefail rejoin (28k/s, kill node18@60,
restore@120, sample to 420, `HANDOFF=1`).

- **Storm fully eliminated — cleanest soak yet.** node18 booted as a deferring
  receiver on all 4 owned partitions; `leader_reject=0` for the *entire* load
  window (vs 173k pre-fix, 100k Phase C, 22k Phase E), terms flat (converged at
  2–3), zero self-promote, zero election war, **0 panics, NRestarts=0**.
- **Retention confirmed working leader-side:** n19 p2 held `purged=163044` under a
  `last_log=357530` head (~194k retained) instead of purging to ~356530.
- **Hand-off still does NOT complete under sustained load.** Every attempt aborts
  with `learner catch-up timeout`; node18 stays a clean Learner (creates correctly
  route to the incumbent). Root cause: Phase E boots node18 as an **empty**
  receiver (to avoid the divergent-lineage resurrection bug), so it must
  snapshot-install from index 0 on each attempt; the install + streaming to
  zero-lag against a ~28k/s moving head cannot fit the 10s catch-up window.
  Leader-side retention does not help a *from-empty* learner skip that initial
  install (its match starts at 0, below the leader's retained/purged floor).
- **Sharper post-soak finding — the learner replication itself STALLS, not just
  "head too fast".** ~9 min after load fully stopped, p11 was still failing every
  cycle with only a **51-entry gap** (n19 `last_log=426825`, node18 frozen at
  `last_log=snapshot=purged=426774`, receiving nothing). So the returning owner's
  learner does not advance past its installed-snapshot point even at near-zero lag
  and zero load — pointing at a learner-replication stall across the hand-off
  **add_learner → 10s timeout → abort/remove → re-add** cycle (the aborted attempt
  tears the learner out of the leader's membership, and the re-add never
  re-establishes sustained AppendEntries before the next timeout). This is deeper
  than catch-up throughput and is the real completion blocker; it also explains why
  only the partitions that happened to be mid-transfer at the load-stop instant
  (2,5,8) completed while p11 stayed stuck.
- **Converges cleanly on load-ease:** once load stopped, 3/4 owned partitions
  (2,5,8) transferred node18→Leader at term 2–3; p11 needed one more requester
  cycle. This is Zeebe-aligned best-effort reclaim: no storm, completes when the
  firehose eases.
- **Disk leak fixed (validated live):** the bootstrap `sweep_orphaned_snapshot_dirs`
  reclaimed the pre-existing multi-GB `/tmp/nanobpmn-raftsnap-*` dirs on restart;
  `/tmp` stayed bounded to the 4 live per-partition dirs (160–182G free) with no
  accumulation across the run.

**Remaining design fork for "complete UNDER load"** (both split-brain-critical,
need a decision):
1. *Accept Zeebe-style best-effort* (ship as-is): storm is gone, reclaim completes
   on load-ease; the retention + disk-leak fixes stand on their own.
2. *Head-freeze that actually stops the log* for the whole install+stream: the
   current per-partition write-pause pauses completions but the partition head
   still advances (new creates via other paths), so the learner never reaches
   zero-lag. Would need the pause to fully quiesce the owned partition's raft log
   for the catch-up duration.
3. *Avoid the from-empty install*: on rejoin, host the owned partition from its
   on-disk log's committed **common prefix** (as a normal follower that lets
   AppendEntries truncate the divergent suffix) so the learner starts near its
   pre-death index and only streams the bounded downtime gap the leader now
   retains — no snapshot install. More correct but reintroduces the divergent-log
   handling Phase E sidestepped.

## Root-cause investigation (controlled harness, debug logging) — DEFINITIVE

The "learner replication stall / permanent 51-entry-gap freeze" framing above is
**refuted**. Three controlled zero-load repros on the same binary (binsha
`9e323cd8`, `NANOBPMN_RECLAIM_HANDOFF=1`), with tiny purge thresholds
(`NANOBPMN_RAFT_KEEP_LOGS=200`, `..._LAGGING_RETAIN=800`) to force the
snapshot-install path cheaply, plus `openraft::replication`/`snapshot_transport`
debug logging:

1. **Small / drained state → hand-off completes in ~1s.** node18 boots empty,
   installs n19's small snapshot, streams the tiny tail; membership change commits
   and node18 leads all 4 owned partitions at `term=2` within one 10s window.
2. **Large *undrained* backlog (`active_backlog≈16.7k`, 16 KB vars) → completes in
   ~52–63s, not never.** All 4 owned partitions transfer, but only after several
   abort/re-request cycles. There is **no permanent freeze** at zero load — the
   learner is *kept* across retries (`perform_handoff` does not remove it on
   timeout; `handle_handoff_ack` only rebuilds a competing group, never an existing
   receiver), so replication progress accumulates until lag reaches threshold.

**The mechanism (smoking gun):** node18's `receive_snapshot` for partition 11
streamed a **~165 MB** `InstallSnapshotRequest` (`offset` reached 163,577,856 +
1,777,669) in 3 MB chunks throttled to ~30 MB/s → ~5.5 s of transfer for a single
partition holding only ~4 k active instances × 16 KB. The snapshot **is the entire
resident active-instance state machine**, not the raft log. Therefore:

- Because node18 boots **empty** (Phase E fresh receiver), catch-up requires a
  *full state-machine snapshot install*, whose size = the incumbent's resident
  active-instance backlog for that partition.
- Under Soak-4 load (~2 M active instances) that snapshot is **gigabytes**; at
  ~30 MB/s the transfer alone is minutes — vastly beyond `HANDOFF_CATCHUP_TIMEOUT`
  (10 s). Each cycle times out; under *sustained* load the state keeps growing so
  the install never fits a window → completes only when load eases (the observed
  Soak-4 behaviour). At zero load it always converges, just slowly for big state.
- The 51-entry gap seen post-Soak-4 was the *tail after* an install of a huge,
  never-draining snapshot; the freeze was transfer time, not a replication stall.

**Secondary finding (separate issue):** after rejoin node18's `/debug/raft` shows
only its 4 *owned* partition groups (cleanly `Leader term=2`); it does **not**
re-form learner-replica groups for the 8 partitions owned by n19/n20. Those leaders
then re-send the same `AppendEntries` batch to `target=2` every ~500 ms forever
(no group to receive them) — wasted control-plane traffic and degraded failover
redundancy (RF effectively < 3 for those partitions on this node). Worth fixing
independently of the hand-off.

### Revised recommendation
Option 3 is now clearly the right fix and is strongly motivated by the evidence:
node18 **already holds** almost all of this state on disk from before it died.
Booting empty discards it and forces a multi-GB re-transfer that cannot fit the
window under load. Hosting the owned partition from its on-disk committed
**common prefix** means node18 only needs the **bounded downtime delta** (the log
entries since it died — kilobytes) streamed via AppendEntries after truncating any
divergent suffix (safe in leader-durable mode: the incumbent is authoritative). No
giant snapshot, converges in ~1 RTT even under sustained load. Pair with an
adaptive catch-up deadline (do not abort while the learner's match index is
advancing) so a single attempt rides the delta to completion. Option 2 (true
head-freeze) is an accelerator but insufficient alone (the from-empty install is
the real cost). Option 1 (Zeebe-style best-effort) remains the safe ship state
already achieved.
