# ADR 0013 — SLA modes at the saturation ceiling, and bounding the var-store WAL

Status: **Accepted — implemented.** Both changes land behind defaults that
preserve the historical behaviour (`NANOBPMN_SLA_MODE=latency`, WAL truncation
on at 30 s). Unit-tested; clippy clean; release build green.
Date: 2026-07-03.
Relates to: ADR 0003 (write-path durability tiers), ADR 0012 (terminal-state /
exporter-lag decoupling), `PERFORMANCE.md` (Fix 1a/1b/2), `server/src/backpressure.rs`,
`server/src/varstore.rs`, `server/src/main.rs`.

## Context

Two follow-ups were left open after ADR 0012:

1. **The durable var-store WAL grows without bound.** The authoritative var-store
   (Fix 1b — the store that makes lean, control-only snapshots possible) runs in
   `journal_mode=WAL` with `synchronous=NORMAL`, and is written continuously
   (every snapshot checkpoint plus each spill eviction) while recovery/`load_all`
   may read concurrently. SQLite's automatic checkpoint moves committed pages into
   the main db file but **never shrinks** the `-wal` file, and a long-lived reader
   can pin the checkpoint so the WAL only grows — observed climbing to ~843 MB
   during a large-payload soak. This is a disk-footprint leak: it never bounds.

2. **The behaviour at the saturation ceiling was fixed, not a choice.** With the
   resident-variable balloon eliminated (ADR 0012) and live variables spillable to
   disk, memory pressure now reflects the *live working set*, which admission
   directly controls. That makes it meaningful — and necessary — to decide what
   the engine should do when it reaches its capacity ceiling. Two behaviours are
   coherent, and different workloads want different ones:

   - **Preserve end-to-end latency.** Reject new instances so accepted work keeps
     completing fast. A "time-to-complete SLA."
   - **Preserve admission.** Keep starting instances and let latency grow. A
     "start-every-process SLA."

   Historically the engine only did the former (AIMD concurrency limiter +
   active-backlog gate shed with `503 RESOURCE_EXHAUSTED`). There was no way to ask
   for the latter.

## Decision

### 1. Periodic truncating WAL checkpoint

Add `VarStore::checkpoint_wal()` running `PRAGMA wal_checkpoint(TRUNCATE)`, driven
from the segmented multi-partition maintenance loop on its own cadence
(`NANOBPMN_VARSTORE_WAL_CHECKPOINT_SECS`, default 30 s, `0`/`off` disables). It
runs **off the hot engine thread** (on the maintenance task, alongside snapshot +
compaction), so it never competes with command application. A `SQLITE_BUSY` from a
concurrent reader is non-fatal — the next tick retries — so the call site
logs-and-continues. This bounds the `-wal` file to roughly one interval's worth of
writes instead of growing without limit.

### 2. `SlaMode` — pick the ceiling behaviour

Add `NANOBPMN_SLA_MODE`:

- **`latency`** (default): historical behaviour. The **latency-preservation
  gates** — the AIMD concurrency limiter and the active-backlog admission gate —
  shed admission (`503`) to keep accepted instances fast.
- **`admission`**: suppress the latency-preservation gates so creates keep being
  admitted, accepting higher end-to-end latency.

Crucially, `SlaMode` governs **only** the latency gates. The **memory-safety
rails** — create-queue depth (bounds the pre-apply mailbox of variable-carrying
closures), exporter-queue saturation, in-flight create-payload watermark, and
resident-memory watermark — remain active in **both** modes. They exist to keep
the node from OOMing, and "start every process" cannot outrun physical memory +
disk. So admission mode is best-effort admission up to a genuine memory rail, not
a licence to crash.

This composes directly with ADR 0012: because terminal state now frees on
completion and live variables spill to disk (WAL now bounded, per decision 1), the
working set that must fit in RAM is minimised — so in admission mode the hard
memory rails bite far later than the latency gate would have, letting many more
instances start before anything is shed.

## Consequences

- **Positive.** Disk footprint of the var-store is bounded. Operators can select
  the envelope behaviour that matches their SLA. The two changes reinforce each
  other: WAL bounding makes disk-backed spill sustainable, which is exactly what
  admission mode leans on. Both default to the prior behaviour, so existing
  deployments are unaffected until opted in.
- **Neutral / honest limits.** Admission mode does **not** remove OOM protection;
  under sustained overload it still sheds at the memory rails (durability intact —
  a shed create is never journaled). It trades bounded latency for bounded
  rejection, not for unbounded memory.
- **Negative.** A truncating checkpoint briefly takes the var-store write lock;
  sized by interval (30 s default) it is negligible, but a very small interval
  under heavy spill could add contention — hence it is tunable and disableable.

## Alternatives considered

- **Auto-switch modes from live signals** instead of a config. Rejected for now:
  the choice is a *policy* about what the operator values (latency vs admission),
  not something the engine can infer. A future controller could bias spill
  aggressiveness within admission mode, but the mode itself stays operator-chosen.
- **Relax the memory rails in admission mode.** Rejected: that converts "accept
  latency" into "accept crashing," which serves neither SLA.
- **Rely on SQLite auto-checkpoint / `wal_autocheckpoint`.** Rejected: it flushes
  pages but does not truncate the file, so the disk footprint still ratchets up.

## Follow-ups

- Optionally have admission mode lower the adaptive spill high-watermark so it
  converts RAM pressure into disk earlier (bias spill), widening the admission
  runway before the hard rail.
- Soak-validate admission mode on the cluster: confirm it sustains admission under
  a worker-starved blob burst with memory held by the rails + spill, and that the
  var-store WAL stays bounded across a long run.
