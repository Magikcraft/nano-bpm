# ADR 0012 — Decoupling terminal-state memory reclamation from exporter lag

Status: **Accepted — implementing.**
Date: 2026-07-03.
Relates to: ADR 0003 (write-path durability tiers), `docs/performance-comparison.md`,
`PERFORMANCE.md` (Fix 1a/1b/2), `engine-core/src/engine/memory.rs`,
`engine-core/src/state.rs`, `server/src/journal.rs`, `server/src/main.rs`,
`server/src/readstore.rs`, `server/src/varstore.rs`.

## Context

Under a worker-starved flood of large (≈50 KB) variable payloads, node memory
balloons and does not recede promptly after load stops. A long investigation
(PERFORMANCE.md, Fix 1a streaming snapshot, Fix 1b lean control-only snapshot +
authoritative durable var-store, Fix 2 byte-aware adaptive spill) narrowed the
balloon to **resident instance variables that variable-spill cannot shed**:
`resident_variable_bytes` held flat at ~4.35 GB/node through the burst and
**stayed there for minutes after the load stopped** (cluster idle, in-flight = 0),
while each spill sweep hit `candidates == 0`.

### Root cause (corrected)

Prior notes attributed the non-spillable residue to **leased jobs** —
`Engine::is_spillable` supposedly requiring an *un-leased* job. **This is wrong.**
Verified against the code and a focused engine test:

- `Event::JobActivated` only adds the job to `activated_jobs`; the job **stays in
  `jobs_by_instance`** (`state.rs`).
- `Engine::is_spillable` (`memory.rs`) requires `Active` + unspilled + non-empty
  vars + not guarded (no armed timer / open subscription) + **≥1 job in
  `jobs_by_instance`**. It does **not** exclude `activated_jobs`.
- Therefore an activated/leased-job instance **is already spillable**
  (`resident_spillable_count() == 1` after leasing without completing).

The real non-spillable population is **terminal (Completed / Terminated)
instances**:

- On `ProcessInstanceCompleted` / `ProcessInstanceTerminated` the instance's
  `state` flips but the instance **stays in `state.instances` holding its ~50 KB
  variables** (`state.rs`).
- Hot-state reclamation happens later via `Engine::evict_instances`, which is
  **driven from the exporter loop** (`main.rs`: the evicted batch is exactly the
  set the exporter just projected into the read model).
- `is_spillable` requires `state == Active`, so terminal instances are
  **non-spillable**, yet they are still counted in `resident_variable_bytes`.

So **hot-state memory for dead instances is gated on exporter progress.** When the
exporter lags (slow projection, remote sink, backlog), terminal instances pile up
in heap holding their payloads. This exactly matches the observed "stays for
minutes after load stops": once creates stop, workers drain the Active backlog →
instances go terminal → they linger until the exporter catches up.

### Why this is Zeebe's coupling — and why we need not inherit it

Zeebe **must** backpressure on exporter lag: RocksDB state and the event log
cannot be compacted until every exporter acknowledges its position, so a slow
exporter grows state/log without bound and admission is rejected
(`RESOURCE_EXHAUSTED`). The coupling is forced by the architecture.

Our coupling is **narrower and removable**, because of two properties Zeebe's
uniform RocksDB design does not isolate:

1. **The exporter reads the event log, not hot state.** The read-model projection
   (`readstore.rs upsert_variables`) is fed by *event payloads*, never by a read of
   a hot-state instance.
2. **Terminal-instance variables are write-only to the engine.** Nobody reads a
   completed instance's variables from hot state again — not workers (the instance
   is done), not the exporter (it reads the log), not recovery (Fix 1b: recovery
   replays the journal + authoritative var-store, exporter-independent).

Hence a terminal instance's hot-state variables are **pure liability in heap** the
moment the instance completes. Zeebe tolerates the equivalent because RocksDB is
disk-backed / mmap'd and pages out under pressure; our hot state is a Rust
`HashMap` on the heap and **cannot page out**, which makes the coupling *worse* for
us — and also easy to sever.

## Decision

**Reclaim terminal-instance variable memory on completion, independent of exporter
position. Reserve admission backpressure for genuine local saturation, never for a
lagging downstream reader.**

Split the memory model along the **terminal / live** axis:

- **Terminal state → drop the payload immediately.** When an instance goes
  `Completed` / `Terminated`, free its variables from hot state at once. Keep a
  lightweight **control-only shell** (key + terminal state, **no variables**)
  resident until the exporter has projected the instance, so point-in-time status
  queries still resolve during the projection gap. The exporter-driven
  `evict_instances` path continues to remove the shell afterward — but it no longer
  gates the memory that matters (the ~50 KB payload is already gone).

- **Live state → spill, as today.** If creates outrun completions, the Active
  backlog is bounded by memory-driven variable spill to disk (Fix 2). Unchanged.

- **Exporter lag → costs disk + staleness, never execution.** A lagging exporter
  now affects only (a) **read-model freshness** and (b) **journal-segment retention
  on disk** (the compaction gate `min(exported, var_position)` already holds this;
  disk is large, cheap, OS-pageable). Neither drives hot-state RAM, so neither
  needs to reject admission.

### Read contract

The read model is **eventually consistent**, matching Camunda 8 / Zeebe's
documented contract. Dropping terminal variables on completion (before projection)
means a just-completed instance may briefly be observable in the read model as
not-yet-final; the durably logged event will make it consistent shortly. The
control-only shell preserves *engine-side* status resolution during the gap. This
is an explicit, accepted consequence — the same contract clients already assume.

### Admission backpressure triggers (unchanged intent, clarified)

Backpressure fires on **local saturation** only — the node's own ability to sustain
the *live* working set:

- memory ceiling with spill unable to keep up, and/or
- fsync / engine-mailbox delay (latency saturation).

**Exporter lag is explicitly removed from the trigger set.** Backpressure means
"this node is full," not "a reader is behind." This is what lets a slow *remote*
exporter (Postgres, Elasticsearch) fall arbitrarily behind — spooling log to disk
(and, later, object storage) — without ever throttling process execution.

## Work so far (context this ADR captures)

- **Fix 1a — streaming snapshot** (`PERFORMANCE.md`): serialize the periodic
  snapshot without cloning all payloads via `Arc`, removing the ~12 GB snapshot
  transient that pinned variables during the burst.
- **Fix 1b — lean control-only snapshot + authoritative durable var-store**
  (`journal.rs snapshot_and_rotate_lean`, `varstore.rs`, `seglog.rs recover_multi`):
  the snapshot carries control state only; variables live in
  `var-store.sqlite`, which recovery reads directly. Deployed and soak-tested on a
  3-node GCP cluster (`NANOBPMN_LEAN_SNAPSHOT=1`). Mechanism confirmed
  (msnapshot.bin 355 MB control-only, var-store ~5 GB); it did **not** reduce the
  Phase-B p99 by itself — establishing that snapshot fsync was not the stall and
  pointing at the resident-var residue this ADR addresses. New follow-up cost: an
  unbounded var-store WAL (needs periodic `wal_checkpoint(TRUNCATE)`).
- **Fix 2 — byte-aware adaptive spill** (`journal.rs maybe_var_spill_pressure`):
  memory-driven spill with a byte-based futile-shed guard; ships with no
  small-payload regression, but is **necessary-not-sufficient** for this workload
  because the residue is terminal (non-`Active`) instances it structurally cannot
  select.
- **Diagnosis (this ADR)**: leased jobs are already spillable; the residue is
  terminal instances pinned in heap until exporter-driven eviction. Verified in
  code and by a focused engine test.

## Consequences

**Positive**
- Post-burst resident-variable memory should track toward ~0 independent of
  exporter position; the balloon stops being a function of a downstream reader.
- Execution is insulated from slow/remote/broken exporters — directly improving the
  PG/ES exporter story versus Zeebe.
- Backpressure signals become honest (local saturation only).
- Heap pressure — which cannot page out — is decoupled from the one subsystem most
  prone to lag.

**Negative / risks**
- **Projection gap**: a terminal instance's variables leave hot state before the
  read model is updated. Mitigated by the control-only shell (status resolves) and
  bounded by the eventual-consistency contract (values converge once exported).
- **Call-activity / subprocess output propagation**: a child's output must reach
  its parent *before* the child's variables are dropped. This propagates
  synchronously via events at completion (the parent already has it by the time the
  child is terminal); this path must be covered by tests before shipping.
- Slight added branch on the completion apply path.

## Alternatives considered

1. **Zeebe-style admission backpressure on exporter lag.** Rejected as the primary
   mechanism: it throttles execution for a downstream reader's pace, defeats remote
   exporters, and is unnecessary once terminal state is decoupled. (Local-saturation
   backpressure is retained for the live working set.)
2. **Make terminal instances spillable** (relax `is_spillable`'s `Active` check so
   they spill to disk like Active instances). Rejected: terminal variables are never
   rehydrated, so writing them to the spill store is pure waste — dropping is
   strictly cheaper and simpler.
3. **Do nothing; rely on Fix 2 spill.** Rejected: spill cannot select terminal
   instances, so the residue persists.

## Implementation plan

1. Engine: on `ProcessInstanceCompleted` / `ProcessInstanceTerminated`, drop the
   instance's `variables` (leave a control-only shell); ensure
   `resident_variable_bytes` no longer counts it. Preserve call-activity output
   propagation (propagation precedes the terminal apply).
2. Confirm the exporter-driven `evict_instances` path still removes the shell and is
   idempotent with an already-emptied payload.
3. Tests: (a) leased-job instance is spillable (regression pin for the corrected
   diagnosis); (b) a completed instance holds no variables in hot state; (c)
   call-activity parent receives child output despite child-var drop; (d) read-model
   still projects variables (fed by events, unaffected).
4. `cargo clippy --release --all-targets` (zero) + `cargo test --release`.
5. Deploy + re-soak on the GCP cluster; measure that post-burst resident-var residue
   tracks to ~0 independent of exporter position.

Deferred (tracked separately): periodic var-store `wal_checkpoint(TRUNCATE)`;
feedback controller with SLA modes (accept-latency vs reject-admission) driven by
local-saturation signals.
