# ADR 0025 — Urban trigger runtime (the Zapier primitive: sources → inbox → engine)

Status: **Accepted; phases 1–3 implemented** (durable inbox + dispatcher + FEEL action planning +
gateway apply; phase 2 `cron`/`webhook`/`file` core sources behind an extensible source registry;
**phase 3** the console Triggers panel — declared-trigger + source-registry view, per-trigger "Run
now", and the delivery inbox). Phase 4 (auto-launch pack source drivers as supervised processes +
a first source pack) remains.
Date: 2026-07-21.
Relates to: ADR 0022 (`0022-nano-rad-application.md`, **Urban** — the RAD App bundle; this ADR
expands its §B "Trigger runtime" from a sketch into the one genuinely-new runtime subsystem the
ADR says to "build first"),
ADR 0024 (`0024-urban-data-layer-datasource-abstraction.md`, the **datasource** seam — the durable
trigger **inbox** lives in an App datasource, so this ADR composes directly on 0024),
ADR 0005 (`0005-embedded-u-nano.md`, **Bernd** — the embedded engine the triggers drive, and the
"choose deployment by config" position: triggers must run the same embedded or remote),
ADR 0007 (`0007-rad-extension-system.md`, the pack contract — trigger *sources* are a
`nano-ide-trigger-*` specialization axis; sources are **declared data**, no `eval`),
ADR 0008 (`0008-polyglot-language-packs.md`, the pack-axis precedent),
`engine-core/src/command.rs` (`CorrelateMessage` — the **unbuffered, no-TTL, no-dedup** message
primitive, `:138-149`) and `engine-core/src/model.rs` (`message_start_event`, `:1023-1025` — a
matching `CorrelateMessage` **starts a new instance**), whose semantics force durability + idempotency
into *this* layer,
`clients/node-stream/src/falconClient.ts` (`createInstance` — the start-process primitive a
trigger action calls), and `server/src/console/projects.rs` (the existing run/compile
**supervisor** the trigger loop is modeled on).

## Context

An installed Urban App is inert until something makes it *act*. ADR 0022 §B names the primitive —
a **trigger**: a source produces events, and each trigger's `action` maps an event to either
**start a process** or **publish a message** (correlation via a FEEL expression over the event
body). It lists a first source set (`cron`, `webhook`, `file`, `imap`/`mqtt`), declares sources a
`nano-ide-trigger-*` pack axis, and says this subsystem "should be built **first** — it is what
makes an installed App act." It also leaves the hard part explicitly open (§"Open questions"):

> Trigger delivery semantics under embedded single-node: at-least-once with an idempotency key on
> `action`? How do we survive App restart with a webhook mid-flight (durable inbox table in the App
> SQLite)?

This ADR settles those semantics. The critical grounding fact is in the engine: Nano's
`CorrelateMessage` (`command.rs:138-149`) is **not buffered — no TTL, no dedup**; a message with no
matching *open* subscription is simply dropped. Its matching subscriptions are of two kinds: a
deploy-time **message-start** subscription (a matching `messageName` **creates a new instance** at a
message start event — `model.rs:1023-1025`), and a **running-instance** catch/boundary subscription
(feeds a parked token). So a `message` action can *start* work or *feed* it; only a message that
matches **neither** an open message-start subscription nor a waiting instance is dropped. Either way
the trigger runtime **cannot** lean on the message layer for durability or de-duplication: if it
wants "a webhook that arrived is not lost across a crash," it must own that itself. ADR 0024 just
gave it the place to do so — a durable **inbox** table in an App datasource.

## Decision (proposed)

A **trigger runtime**: a supervised set of long-lived **sources** that write received events into a
durable **inbox** (an ADR 0024 datasource table), and a single **dispatcher** that drains the inbox
by applying each event's `action` to the engine exactly like an SDK client would. The inbox is the
crash-consistency boundary between "event received" and "action applied."

### 1. The model — sources, actions, connections

Unchanged from ADR 0022 §B, restated as the contract:

- A **source** produces events. Core sources (in the compiled binary): `cron` (in-process
  scheduler), `webhook` (an HTTP path on the App's `Deno.serve` backend), `file` (watch a
  path/glob). Pack sources (`nano-ide-trigger-*`): `imap`, `mqtt`, cloud, … .
- A **connection** is defined once (credentials/endpoint) and referenced by id, so a source config
  carries no secrets inline.
- An **action** maps an event to one of two engine calls:
  - `start`: start a process (variables seeded by a FEEL expression over the event body), or
  - `message`: publish a `CorrelateMessage` (`messageName` + `correlationKey` from FEEL over the
    body) — reusing Nano's existing correlation.

```jsonc
"triggers": [
  { "id": "morning", "type": "cron",    "spec": "0 6 * * *",
    "action": { "start": "heating-cycle" } },
  { "id": "sensor",  "type": "webhook", "path": "/hooks/temp", "auth": "hmac:sensors",
    "action": { "message": "temp-reading", "correlationKey": "= body.room" } },
  { "id": "inbox",   "type": "imap",    "connection": "mailbox",
    "action": { "start": "triage-email" } }
]
```

### 2. Delivery semantics — at-least-once via a durable inbox

The load-bearing decision. Each source event flows through the inbox in three durable steps:

1. **Persist** the event to the inbox (status `pending`) with an **idempotency key** (see §3),
   *before* the action runs. A unique constraint on the idempotency key makes this the dedup point.
2. **Dispatch**: the dispatcher reads `pending` rows and applies the `action` to the engine
   (`createInstance` or `CorrelateMessage`).
3. **Settle**: mark the row `done` on success (or `failed` with a retry/backoff count).

**Guarantee: at-least-once.** A crash *between* step 2 (engine applied) and step 3 (marked done)
re-delivers on restart — the dispatcher, on boot, simply re-drains `pending` rows. This is why the
idempotency key is mandatory and why the honest v1 contract is at-least-once, not exactly-once
(see §3 for the dedup boundary and its limit). Sources acknowledge to their upstream **after step 1
only** — e.g. a webhook returns `200` once the event is durably in the inbox, never waiting on the
engine — so "a webhook that arrived is not lost across a crash" holds, answering the §B open
question directly.

The inbox table lives in an ADR 0024 datasource (the App's default source unless overridden), so it
inherits the SQLite-dev / Postgres-deploy swap for free.

### 3. Idempotency keys — per-source, deterministic

Each source contributes a **deterministic** idempotency key so redelivery within the runtime is
suppressed by the inbox's unique constraint:

- `cron` — `<triggerId>:<fireEpochSecond>` (a given fire instant enqueues once).
- `webhook` — a caller-supplied `Idempotency-Key` header if present, else a hash of
  `(path, body, receivedSecond)`.
- `file` — `<path>:<mtime>:<eventKind>`.
- `imap` — the message UID; `mqtt` — `(topic, packetId)` where QoS provides one.

**The dedup boundary (honestly stated):** the unique key stops *re-enqueue* of the same event and
stops the dispatcher re-firing a row already `done`. It does **not** make the *engine action* itself
idempotent across the narrow step-2/step-3 crash window: a `start` applied then un-settled will
start a second instance on redelivery. v1 accepts this — makers design idempotent processes for
at-least-once — and a later seam may thread the inbox idempotency key into an engine-side start-dedup
(Nano has no message dedup today; `CorrelateMessage` §Context). This is called out, not hidden.

### 4. The dispatcher and supervision

- A single **dispatcher** owns the drain loop, modeled on the existing run/compile **supervisor**
  (`server/src/console/projects.rs`): sources are long-lived supervised tasks; the dispatcher is a
  bounded worker that pulls `pending` rows in order, applies the action, settles, and backs off
  failed rows with a capped retry.
- **Single-node embedded** is the default (ADR 0005): the dispatcher is in-process, one drain loop.
  Under `runtime.engine: remote|cluster`, the actions target the remote engine over the ADR 0005
  transport seam with **no trigger-definition change** — the source/inbox/dispatch shape is
  identical; only where `createInstance`/`CorrelateMessage` land changes.
- **Ordering**: FIFO per source by insert order; no cross-source ordering guarantee (triggers are
  independent Zapier-style automations).

### 5. `CorrelateMessage`'s two targets and its unbuffered edge

A `message` action correlates to whatever open subscriptions match `messageName`:

- a deploy-time **message-start** subscription → **starts a new instance** at the message start
  event (`model.rs:1023-1025`), and/or
- a **running-instance** catch/boundary subscription → feeds the parked token.

So `message` is not merely a "feed running work" action — with a message start event in the target
process it *starts* work too, and a process that mixes both a message start event and message catch
events gets start-or-feed from a single action. This makes `message` a full peer of `start` for the
common Zapier case; `start` remains the choice when the trigger should always create an instance
regardless of process modeling.

The remaining sharp corner is narrow and inherent to Nano's model (§Context): because
`CorrelateMessage` is unbuffered, a message that matches **neither** an open message-start
subscription **nor** a currently-waiting instance is dropped at the engine, and the inbox will still
mark it `done` (the engine accepted the command). Guidance:

- Use a `message` action whose target process has a **message start event** when the trigger should
  *cause* work — the instance is created, nothing is lost.
- Reserve the drop risk for the genuine race: a `message` aimed only at a running instance that has
  not yet reached its catch event. If a trigger must both start-if-absent and feed-if-present with
  no start event, that is a process-modeling concern (add a message start event), not a
  trigger-runtime feature in v1.

### 6. Sources are a pack axis (`nano-ide-trigger-*`)

Core ships `cron`/`webhook`/`file` in the compiled binary. Every other source is an ADR 0007 pack
on the `nano-ide-trigger-*` axis (beside `lang`/`app`/`data`). A source pack implements a small
contract: `start(connection, config, emit)` where `emit(event, idempotencyKey)` performs the §2
step-1 persist; the runtime owns the inbox, dispatch, retry, and lifecycle. Sources are **declared
data** referenced by the manifest — no `eval`, per ADR 0007 — so a community source cannot run
arbitrary host code beyond its packaged, reviewed driver.

**The extensibility seam, concretely (phase 2).** Core and pack sources unify at the *emit-to-inbox
boundary* (`triggers::enqueue`). The only difference is *where the source loop runs*:

- **`cron`/`file`** are in-process Rust loops (`server/src/console/trigger_sources.rs`) spawned by the
  supervisor alongside the drain loop, sharing one stop signal.
- **`webhook`** is passive: a gateway ingress route (`POST /console/api/projects/{name}/hooks/{triggerId}`)
  that persists-before-acks. This is the **universal external emit endpoint** — any out-of-process
  producer uses it.
- **A pack source** is declared data: its `nano-ide.ext.json` carries a `triggerSources[]` entry
  (`kind`, `displayName`, `transport`, `configFields`). The installed pack's out-of-process driver
  (a Node/Deno process, ADR 0036) emits over the same webhook ingress. Adding a `type` therefore needs
  **no core change**.

`known_kinds()` = the compiled-in `BUILTIN_KINDS` ∪ every installed pack's declared `triggerSources[].kind`.
`GET …/triggers` (below) tags each manifest trigger `builtin`/`recognized` against this union, so the
Triggers panel can show an unknown `type` as "needs a pack" rather than silently ignoring it. **Deferred
to phase 4:** the runtime *auto-launching* a pack driver process as a supervised task — phase 2 lands
the recognition + emit contract, which is the marketplace seam; a pack driver run today is started
out-of-band and emits over the ingress.

### 7. Console — the Triggers panel

A **Triggers** toolbar toggle in the App project workspace (beside Data), surfacing runtime status
over the manifest's declared `triggers`. **(Implemented, phase 3.**
`console/src/components/TriggersPanel.tsx`, wired into `ProjectWorkspace` mutually-exclusive with the
Data panel and shown only for Urban App projects.) Two sub-surfaces:

- **Triggers** — the declared triggers from `GET …/triggers`, each tagged against the source registry:
  a `core` badge (builtin), a `pack` badge (recognized pack kind), or an `unrecognized` badge (a typo
  or not-yet-installed pack). Surfaces the `errors[]` (e.g. a malformed cron spec) and a collapsible
  view of the whole source registry (core kinds ∪ installed packs). A per-trigger **Run now** enqueues
  a synthetic event (editable JSON body) through `POST …/triggers/enqueue` — the *same* inbox path a
  real source uses, so testing a trigger exercises the production dispatch path.
- **Inbox** — the delivery status from `GET …/triggers/inbox`: pending/done/failed counts and the most
  recent rows (id, trigger, status badge, attempts, last error, created), with opt-in auto-refresh.

Reuses the console panel pattern (as the Data panel does, ADR 0024 §4). Editing the `triggers` block
itself stays in the App manifest editor; this panel is the *runtime* view. Node-first per ADR 0038.

## Phased plan

1. **trigger-inbox** — the §2 inbox schema (on an ADR 0024 datasource) + the §4 dispatcher (persist
   → dispatch → settle, at-least-once, retry/backoff). Prove it with a **manual/synthetic** source
   so the durability path lands before any real source. **(Implemented.** `server/src/console/triggers.rs`:
   inbox primitives + `drain_over_gateway` + a supervised drain loop auto-started from the project run
   path; FEEL action planning via `engine-core` `feel::eval`; actions applied over the local gateway
   (`POST /v2/process-instances`, `POST /v2/messages/publication`). The synthetic source is the
   `POST /console/api/projects/{name}/triggers/enqueue` endpoint; `GET …/triggers/inbox` reports
   status. Node-first per ADR 0038.**)
2. **core-sources** — `cron` (in-process scheduler, deterministic keys, §3), `webhook` (gateway
   ingress route, ack-after-persist, env-var shared-secret `auth`), `file` (mtime-poll watcher).
   **(Implemented.** `server/src/console/trigger_sources.rs`: the extensible source registry
   (`BUILTIN_KINDS`, `known_kinds()` = builtins ∪ pack-declared kinds), a dependency-free 5-field UTC
   cron parser (`CronSpec`, Vixie dom/dow OR-rule), and the in-process cron/file drivers spawned by
   the supervisor. `triggers.rs`: `webhook_ingest` (validate → auth → persist-before-ack),
   `triggers_overview` (`GET …/triggers`, tagging builtin/recognized). `extensions.rs`:
   `ExtManifest.triggerSources[]` + `all_trigger_sources()` — the marketplace seam. `onMissed`/`config`
   added to the spec-app `trigger` schema. Node-first per ADR 0038.**)
3. **triggers-panel** — the §7 console panel, consuming `GET …/triggers` + the inbox.
   **(Implemented.** `console/src/components/TriggersPanel.tsx`: Triggers sub-tab (declared triggers
   tagged core/pack/unrecognized against the registry, config-error banner, collapsible source
   registry, per-trigger "Run now" enqueuing a synthetic event over `POST …/triggers/enqueue`) + Inbox
   sub-tab (pending/done/failed counts + recent rows with auto-refresh). Wired into `ProjectWorkspace`
   as a toolbar toggle for Urban App projects, mutually exclusive with the Data panel.**)
4. **trigger-pack-axis** — auto-launching the §6 `nano-ide-trigger-*` pack drivers as supervised
   Node/Deno processes + a first pack (`imap` or `mqtt`), proving the axis end-to-end.

## Consequences

- Urban gains the primitive that makes an installed App **act** — "Zapier for yourself" — as durable
  at-least-once, restart-safe delivery, not a best-effort loop.
- One new runtime subsystem (sources + inbox + dispatcher), composing cleanly on existing seams: the
  ADR 0024 datasource (inbox), the `Deno.serve` backend (webhook), the run/compile supervisor
  (dispatch), and `createInstance`/`CorrelateMessage` (actions). No new engine concept.
- The embedded-vs-remote flip (ADR 0005) holds for triggers with no definition change.
- A new `nano-ide-trigger-*` pack axis lets the community add sources without touching core, keeping
  the compiled binary small.
- The `CorrelateMessage` behaviour is surfaced accurately: a `message` action can **start** an
  instance (message start event) or **feed** a running one; only the narrow "aimed at a not-yet-
  waiting instance" race is dropped — a candidate future engine seam (message dedup / buffering) if
  triggers demand it.

## Open questions

- **Missed-fire policy** for `cron` across downtime: the per-trigger `onMissed: skip|once|all` field
  (default `skip`) is now in the schema and parsed (`OnMissed`); a durable missed-fire *cursor* (so
  `once`/`all` catch up precisely rather than from a boot-window heuristic) is still open.
- **Retry/backoff shape** for `failed` rows: cap, backoff curve, and a dead-letter state — surfaced
  where in the Triggers panel?
- **Inbox retention/GC**: how long are `done` rows kept (audit vs. table growth), and does GC ride
  the idle-purge tick or a scheduled cron trigger of its own?
- **Webhook auth**: shared-secret vs. HMAC signature vs. bearer — first-party set, and how a
  `connection` expresses it without embedding the secret in the bundle (ties to ADR 0024's secrets
  open question).
- **Engine-side start dedup**: is threading the inbox idempotency key into a `createInstance` dedup
  key worth a small engine change to close the step-2/step-3 at-least-once window, or is idempotent
  process design the right long-term stance?
- **Backpressure**: under a webhook flood, does ack-after-persist need an inbox depth cap that sheds
  (returns `429`) to protect the embedded engine, reusing the ADR 0020 admission ideas?
- **Multi-node**: with `runtime.node: cluster`, does each node run its own sources (duplicate cron
  fires) or is source ownership leased/partitioned? Single-node is v1; this is the scale-out seam.
