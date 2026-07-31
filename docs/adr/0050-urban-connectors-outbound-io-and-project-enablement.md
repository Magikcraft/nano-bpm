# ADR 0050 — Urban connectors: the outbound I/O edge (workers + components) and project-enablement

Status: **Proposed.**
Date: 2026-07-31.
Relates to:
ADR 0025 (`0025-urban-trigger-runtime.md`, the **inbound** edge — sources → durable inbox →
engine; §4/§6/phase 4 built the supervised out-of-process **driver** this ADR reuses to launch
workers, and the inbox whose durability this ADR mirrors for the *outbound* edge),
ADR 0033 (`0033-urban-element-templates-first-class-components.md`, **components** — this ADR
**amends** its §2 (palette = the *enabled* set, not every installed pack) and §4 (adds the worker +
connection facets it left as "future work")),
ADR 0022 (`0022-nano-rad-application.md`, §E `workers[].taskType` — the runtime half a component
configures at design time),
ADR 0027 (`0027-urban-app-manifest-spec.md`, the manifest boundary rule §1 and the "no secrets in
the bundle" env-substitution §5 — enablement lands in `nano.app.json`, credentials stay env
pointers),
ADR 0007 (`0007-rad-extension-system.md`, **declared data, no `eval`** — a connector is a pack axis;
its worker is a *referenced file*, not inline code),
ADR 0036/0038 (`0036-dual-runtime-workers-deno-node-fallback.md`, `0038-node-first-runtime.md`, the
Node-first out-of-process runtime the worker runs on),
`engine-core/src/command.rs` (the engine **job queue** — `ActivateJobs`/`CompleteJob`; a job is held
until completed and re-activated on crash, so it *is* the outbound outbox),
`server/src/console/extensions.rs` (`pack_component_templates` `:580`, `trigger_driver`,
`all_trigger_sources` — the pack accessors this ADR extends with a symmetric `workers` axis),
`server/src/console/trigger_sources.rs` (`run_pack_driver` — the supervised child-process loop this
ADR reuses to launch a worker),
`clients/node-stream/nanobpm-workspace/.nanobpm/worker-sdk.ts` (`defineWorker` — the long-lived
job-worker SDK a connector worker is written against),
`nano-ide` `packages/connector-slack` (jwulf/nano-ide#48 — the first connector, the proving artifact
for this ADR).

## Context

ADR 0025 gave an installed App an **inbound** edge: a trigger *source* produces events, a durable
**inbox** absorbs them, and a dispatcher applies each to the engine. The **outbound** edge — a
process *doing* something to the outside world — was named by ADR 0022 §E (`workers[].taskType`) and
given a design-time face by ADR 0033 (the element-template **component**), but ADR 0033 §4 explicitly
left the runtime and data facets of a *pack-contributed* component as future work:

> **Realized (increment 6):** a pack declares `components[]` … The worker impl and domain-type facets
> of the pack contribution remain future work.

So today a pack can put a **Send Slack Message** card on the palette, but nothing runs it. This ADR
closes that gap and, in doing so, settles the I/O model.

**The framing decision: "output" is not a new primitive.** It is tempting to add a third top-level
block (`outputs[]`) beside `triggers[]`. We reject that — it would be a **drift surface** (two ways to
say "a process calls out"). The outbound edge already exists as the **worker** bound to an
element-template **component**; a "connector" (Slack, HTTP, …) is simply a **pack that spans both
edges**. The two edges are duals:

| Edge | Primitive | Crash-consistency | Prior art |
|---|---|---|---|
| **Inbound** | `triggers[]`: source → **inbox** → engine | at-least-once via a *new* inbox — `CorrelateMessage` is unbuffered (ADR 0025 §Context) | ADR 0025 |
| **Outbound** | element-template **component** + long-lived **worker** (`taskType`) | at-least-once via the engine's *existing* durable **job queue** — the job **is** the outbox | ADR 0022 §E / 0033 |

The asymmetry is the whole point: inbound needed a new durable subsystem because the message layer
drops unmatched messages; outbound needs **none**, because the engine already holds a job until the
worker completes it and re-activates it after a crash. Reusing that is the honest, minimal design.

## Decision (proposed)

### 1. A connector = a pack spanning both I/O edges

A **connector** is an ADR 0007 pack that may contribute, in one `nano-ide.ext.json`:

- `triggerSources[]` — inbound source kinds + a supervised driver (ADR 0025 §6, unchanged);
- `components[]` — element templates, the design-time face (ADR 0033 §4, unchanged);
- **`workers[]`** — the outbound runtime, *new here*: one long-lived job worker per `taskType`;
- (references to) `connections[]` — the shared credential/endpoint, defined once (ADR 0025 §1).

The Slack connector (jwulf/nano-ide#48) contributes all of the inbound source, the *Send Slack
Message* component, and its backing worker — the reference implementation for this ADR.

### 2. The outbound seam — `taskDefinition:type` == worker `type`

A component's element template stamps a service task with a `zeebe:taskDefinition:type`. A
`workers[]` entry declares that same job `type` and a pack-relative `entry` file:

```jsonc
"workers": [
  { "type": "slack:send-message", "entry": "worker.ts", "displayName": "Send Slack Message",
    "maxParallelJobs": 10,
    "configFields": [ { "key": "botToken", "label": "Bot token", "env": "SLACK_BOT_TOKEN" } ] }
]
```

The **seam invariant**: every component template's `taskDefinition:type` must resolve to a declared,
launchable worker of the same `type`. A task dragged from the palette with no backing worker is the
exact "it parsed but didn't execute" failure ADR 0033 §1 turns into a typed diagnostic — so the
invariant is **validated**, not hoped for (§7).

### 3. Reliability — the engine job queue is the outbox

A connector worker is a Zeebe-style **job worker**: it activates jobs of its `type`, performs the
side effect, and completes the job. The engine (`command.rs`) holds the job until `CompleteJob` and
**re-activates it after a crash**, giving **at-least-once** outbound delivery with *no new durable
subsystem* — the dual of the ADR 0025 inbox. The honest limit (stated, not hidden): a non-idempotent
side effect (Slack `chat.postMessage`) re-run after a crash between "effect done" and "job completed"
fires twice. v1 accepts at-least-once; a later seam may thread a job-scoped idempotency key to the
connector (the outbound mirror of ADR 0025 §3's inbox key).

### 4. Worker lifecycle — long-lived, supervised, reusing the trigger-driver loop

A connector worker is **long-lived** (subscribes by `type`, started with the App, not spawned
per-job) and **supervised**, reusing ADR 0025 phase 4's `trigger_sources::run_pack_driver` mechanism
verbatim in shape:

- **Node-first** (ADR 0038): Node `--experimental-strip-types` for a `.ts` entry, else Deno with a
  scoped allow-list; the pack dir is the working dir so its bundled deps resolve.
- stdout/stderr streamed to the App log; `kill_on_drop`; a `select!` on the shared stop signal vs
  `child.wait()`; crash restart with capped exponential backoff (500 ms → 30 s); one child process
  per enabled worker `type`; torn down with the App's shared `LoopHandle`.
- **Env injection** (the worker SDK contract): `NANOBPMN_BASE_URL` (the App gateway) and
  `NANOBPMN_WORKER_NAME` (the worker's activation identity, e.g. `<project>:<type>`), plus the
  connector's `configFields` env pointers (e.g. `SLACK_BOT_TOKEN`) resolved at launch, never
  persisted.

Per-task spawn is explicitly rejected: it would pay cold-start per job, discard the job-stream +
`maxParallelJobs` backpressure the SDK already implements, and duplicate the supervisor.

### 5. Project-enablement — the palette is the *enabled* set (amends ADR 0033 §2)

ADR 0033 §2/increment 6 populates the palette directly from *every installed pack*. This ADR narrows
that: an installed pack is **available to enable**; a connector's component appears in the palette
only when the connector is **enabled in the project**. Enablement is **derived from a single source of
truth** — no second registry:

- **Install** (a pack present on disk) stays a **toolchain** concern (ADR 0007) — "what is available
  on this machine."
- **Enable-into-project** writes the connector's runtime facts into **`nano.app.json`**: its
  `workers[]`, its `connections[]`, and the active component templates. Per ADR 0027 §1's boundary
  rule ("anything the running App needs to behave lives in `nano.app.json`"), all three are runtime
  behavior, so the manifest is their home.

Everything then **derives** from the manifest: the palette shows enabled connectors' components; the
supervisor launches enabled connectors' workers; the config surface shows enabled connectors'
`configFields`. One source of truth, three views — the ADR 0007 "declared data, host-driven" position.

### 6. Two config planes — per-instance vs per-connector, both secret-free

- **Per-instance** — the element template's `zeebe:input` FEEL fields (channel, message text): set on
  each task in the properties panel.
- **Per-connector** — the shared credential/endpoint: a `connections[]` entry + the worker's
  `configFields`, defined **once** and referenced by every instance (ADR 0025 §1). Defaults are
  `${VAR}` env pointers, resolved at boot, **never** inlined — the committed manifest carries no
  secrets (ADR 0027 §5).

The console surfaces the per-connector plane through an **"Add connector"** panel that projects a
connector's `configFields` into the project config surface with env-pointer defaults — the outbound
mirror of ADR 0025 §7's data-driven "Add trigger" form.

### 7. The pack contract + the anti-drift guard

`workers[]` is declared in `nano-ide.ext.json` and typed by `WorkerSpec` in `ext-types` (the single
published contract packs author against; the host `ExtManifest` mirrors it, as it already does for
`components`/`trigger_sources`). Validation enforces the §2 seam invariant at the ADR 0027 §4
fail-closed gates: a worker `type` with no matching component `taskDefinition:type` (and vice-versa)
is an authoring-time error with a pointer, not a silent runtime no-op. (The `nano-ide`
`validate-manifests.mjs` already lands this guard for the pack side; the host adds it at boot.)

## Consequences

**Positive.**
- The I/O surface is complete and **symmetric** — inbound `triggers` + outbound `workers`/components,
  one connector pack spanning both — with **no new durable subsystem** (the job queue is the outbox)
  and **no new supervisor** (the trigger-driver loop is reused).
- One source of truth (`nano.app.json`) drives palette, worker launch, and config surface — no
  drift, per the house rule.
- Camunda-Marketplace compatibility (ADR 0033 §4) now spans behavior too: an imported connector's
  service task runs against a launched worker.

**Negative / costs.**
- At-least-once outbound is a real caveat for non-idempotent effects (§3) — documented, with a
  future idempotency-key seam.
- Amending ADR 0033 §2 changes the palette's population rule; existing spike behavior (all-installed)
  must migrate to enabled-set. Handled as an increment, not a break.

## Phased plan

1. **worker-contract** — `WorkerSpec` + `workers[]` in `ext-types`; the host `ExtManifest.workers`
   mirror + `all_workers()` / `worker_driver(type)` accessors in `extensions.rs` (symmetric to
   `all_trigger_sources` / `trigger_driver`). *(pack side landed in jwulf/nano-ide#48.)*
2. **worker-supervision** — launch + supervise enabled workers via a `run_pack_worker` modeled on
   `run_pack_driver`; env injection (`NANOBPMN_BASE_URL` / `NANOBPMN_WORKER_NAME` + `configFields`);
   wired into the App run loop beside the trigger source supervisor (`projects.rs`).
3. **enablement** — enable-into-project writes `workers[]`/`connections[]`/active components to
   `nano.app.json`; the palette derives from the enabled set (amends ADR 0033 §2/inc 6).
4. **config-projection** — the "Add connector" panel projecting `configFields` into the project
   config surface with env-pointer defaults (mirror ADR 0025 §7).
5. **validation** — the §2 seam invariant at the ADR 0027 §4 boot gate (a component with no backing
   worker fails closed).

## Open questions

- **Exactly-once outbound.** Thread a job-scoped idempotency key into the connector so a re-activated
  job can suppress a duplicate side effect — the outbound mirror of ADR 0025 §3. Worth it in v1, or a
  documented at-least-once contract makers design around?
- **Connection secrets to the worker.** ADR 0025 phase 4 forwards a connection object to a driver
  verbatim (secrets as env templates the driver expands). Do workers get the same, or a resolved-at-
  launch env (§4) only?
- **One process per `type` vs one per connector.** A connector with several workers: N supervised
  processes (one per `type`, simplest) or one process registering N `defineWorker`s (fewer
  processes, shared connection pool)?
- **A first-class `connector` ExtKind?** Today a connector rides `kind: "trigger"` with
  `components`/`workers` alongside (the closed `ExtKind` enum would reject an unknown kind). Do we add
  `ExtKind::Connector` (host change) once outbound-only connectors — no inbound source — appear?
- **Domain-type facet.** ADR 0033 §3's `workers[].outputType` (typed component output feeding
  downstream FEEL) is still open; a connector worker is where it would bind.
