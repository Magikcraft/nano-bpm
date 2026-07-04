# ADR 0013 — SLA modes at the saturation ceiling, and bounding the var-store WAL

Status: **Accepted — implemented + validated.** Both changes land behind defaults
that preserve the historical behaviour (`NANOBPMN_SLA_MODE=latency`, WAL truncation
on at 30 s). Unit-tested; clippy clean; release build green; cluster-soaked (see
Validation below).
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

## Design rationale: the compressor/limiter model (why the control is a switch)

The `SlaMode` control is deliberately modelled — in both mechanism and UI — on an
audio-dynamics **compressor/limiter**, because the system at its capacity ceiling
*is* a dynamics processor. Treat offered load as an input signal and the capacity
ceiling as the threshold; the question "what do we do with signal above the
threshold?" is precisely the question a comp/limiter answers, and it has exactly two
canonical answers:

- **`admission` mode = a compressor.** Nothing above the threshold is rejected;
  instead the "gain" (per-request speed) is reduced so the whole signal still passes,
  with its dynamic range squashed. Latency rises smoothly, no one is turned away.
  **No information is lost — only dynamic range.**
- **`latency` mode = a limiter (brick-wall).** The output (end-to-end latency) is
  held flat by clamping the *input*: creates above the ceiling are clipped off
  (`503`). What passes stays fast and clean; the peaks above the ceiling are simply
  removed.

The terminology matters and sharpens the design: shedding signal *above* a ceiling
is **limiting/clipping**, not **gating** (a noise gate cuts signal *below* a
threshold — the opposite operation). Naming it correctly exposes the real invariant:
**clipping loses data but keeps what remains pristine; compression keeps everything
but degrades it uniformly.** That is exactly the drop-vs-delay SLA tradeoff — the
metaphor is isomorphic to the mechanism, not decorative.

Two consequences of the model that are already true of the implementation:

- **`admission` mode is a compressor followed by a brick-wall limiter** — the most
  standard chain in audio mastering. You can drive the compressor hard (admit
  everyone, let latency stretch), but the **memory-safety rails** are a safety
  limiter you cannot bypass, so the signal can never clip into *destruction* (a
  crash). The honest description of the device is: *a soft-knee compressor with a
  safety limiter that is always in circuit.*
- **Attack/release already exist.** The AIMD admission limiter's additive-increase /
  multiplicative-decrease behaviour *is* the attack (how fast the system leans into
  load) and release (how fast it recovers) time-constant of the compressor.

**Why a switch, not a continuous knob.** The control exposes two discrete modes, so
its correct affordance is a **mode switch**, not a rotary knob (which would imply a
continuum the control does not offer). This is not a stylistic accident: the design
was inspired by a rack-unit **compressor/limiter whose two behaviours were selected
by a mode switch** — the same two behaviours, the same discrete selector. The
remaining continuous quantity — *ratio*, the grey scale between "compress everything"
and "clip the excess" — is intentionally **not** exposed today; if a middle SLA is
ever wanted (shed *some* and slow *some*, a soft knee), that is the knob to add, and
the model already names it. The cluster monitoring pane (Prometheus metrics +
display) plays the role of the unit's **gain-reduction meter** — the operator's
feedback that the processor is working.

This is design borrowing of the good kind: not a skin, but a *correct model*
transplanted from a domain (bounded dynamics under overload) that solved the same
problem decades earlier, which also makes an abstract distributed-systems policy
legible through hardware operators already understand.

## Validation (GCP 3-node RF=3 P=12, sha e97e95c14b11d5d5, fresh data)

Binary built on-node from `git archive HEAD`, installed on all three nodes; each
mode relaunched fresh (data wiped) via `node-start-sla.sh <mode>`.

### WAL bounding — confirmed
Across every run (closed-loop soak and open-loop flood), the var-store `-wal`
file peaked at ~4–7 MB/node (≤ ~12 MB aggregate across 3 nodes) and truncated
back toward 0 within the 30 s checkpoint cadence. Never approached the ~843 MB
pre-fix growth. The main `var-store.sqlite` grows normally (~200–270 MB/node
under a blob burst) and is unaffected. **30 s default validated** — comfortable
headroom; could be relaxed to 60 s, but the truncation is off the hot thread and
cheap, so 30 s stays the default.

### SLA-mode behaviour — modes converge under the clients we can drive
Two experiments:

- **Closed-loop soak** (`soakB`: warm 60 s / 50 KB blob burst 180 s @16 workers /
  recover 90 s; `MAX_INFLIGHT=32000`, await-completion):
  - latency: A ~37k, B ~2150/s p99 16.6 s, C ~18k p99 7.4 s; peak res 3539 MB,
    var 129 MB; idle → var 0.
  - admission: A ~37k, B ~2210/s p99 17.2 s, C ~18k p99 9 s; peak res 3385 MB,
    var 168 MB; idle → var 0.
- **Open-loop flood** (`flood-sla`: 8 starved workers, `MAX_INFLIGHT=1e8`, 50 KB
  blobs, 60 s/mode):
  - latency: admitted ~105k/node, `stream_credit_stalls`=0, peak res 436 MB.
  - admission: admitted ~106k/node, `stream_credit_stalls`=0, peak res 432 MB.

Both modes were **behaviourally indistinguishable** in throughput, tail latency,
memory, and shed count. Root cause: every load client available here **closes the
loop** — the Node producer awaits completion and is itself HOL-bound at
~1771 creates/s/node (≈ worker drain rate), so the offered load never exceeds the
worker drain rate and the server's latency-shed gates (AIMD limiter + active-backlog
gate) never engage. With the gates idle, the two modes gate nothing differently.

**To observe divergence, an open-loop fire-and-forget producer that outpaces the
worker pool is required** (a Rust producer — the Node stream producer is reader-loop
HOL-bound and cannot generate the necessary overload). This is deferred, not a
defect: the modes are *correct by construction* (admission mode simply suppresses
the three latency-shed sites while keeping the memory rails), and the closed-loop
result is the reassuring one — under well-behaved, completion-aware clients the two
modes cost nothing relative to each other.

## Out-of-the-box tuning guidance

- **`NANOBPMN_SLA_MODE` — default `latency`.** Keep it. Under completion-aware
  (closed-loop) clients it is indistinguishable from `admission`, and it is the
  safe choice under a genuinely open-loop overload (it preserves e2e latency by
  shedding at the AIMD/backlog gate rather than letting latency grow unbounded).
  Switch to `admission` only for "start-every-process" workloads that prefer to
  absorb latency (backed by spill + the memory rails) over rejecting creates —
  its value shows only under open-loop overload.
- **`NANOBPMN_VARSTORE_WAL_CHECKPOINT_SECS` — default `30`.** Validated: holds the
  var-store WAL to a few MB/node even under a blob burst. Leave at 30 s; raise to
  60 s only if truncation contention is ever observed under very heavy spill
  (none seen here); `0`/`off` disables it (not recommended — the WAL then ratchets).

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

- Build an **open-loop Rust producer** (fire-and-forget, unbounded inflight) to
  drive true overload and directly measure SLA-mode divergence (admitted count,
  credit-stalls, memory to the rails). The Node producer cannot generate this.
- Optionally have admission mode lower the adaptive spill high-watermark so it
  converts RAM pressure into disk earlier (bias spill), widening the admission
  runway before the hard rail.
- **A `ratio`/soft-knee middle SLA** — the continuum between "compress everything"
  (admission) and "clip the excess" (latency), per the compressor/limiter model:
  shed *some* creates while letting latency grow *some*. Would move the control from
  a two-position switch toward a knob; only worth adding if a workload actually wants
  a blended envelope rather than one of the two canonical behaviours.
- ~~Soak-validate admission mode on the cluster~~ (done — see Validation; modes
  converge under closed-loop clients, WAL stays bounded).
