# ADR 0026 — Urban human surfaces & the App run model (rendering layer, action API, dev loop)

Status: **Proposed.**
Date: 2026-07-21.
Relates to: ADR 0022 (`0022-nano-rad-application.md`, **Urban** — the RAD App bundle; this ADR
expands its §C "Human surfaces" and §"Runtime & packaging" into the action API, the UI-generation
tiers, and the develop/run loop),
ADR 0009 (`0009-gui-application-projects.md`, **Accepted/implemented** — the served-UI `app` kind
(`console`|`gui`), the `Deno.serve` backend, `deno compile` binary, and the "connectivity stays
remote, embed is a later flag" position this ADR builds the run model on),
ADR 0005 (`0005-embedded-u-nano.md`, **Bernd** — embedded engine as a compile-time packaging choice
behind the SDK transport seam),
ADR 0024 (`0024-...datasource...`, the datasource the human-task/query surfaces read),
ADR 0025 (`0025-urban-trigger-runtime.md`, the trigger inbox — a **UI action is the attended peer**
of an unattended source, sharing the same two engine primitives),
`console/src/components/FormEditor.tsx` (form-js **editor** — authoring only; the runtime **viewer**
gap this ADR names), and `server/src/console/projects.rs` (the run/compile **supervisor**:
`deployTarget`/`NANOBPMN_BASE_URL` `:203,:352,:524`, `deno run`/`deno compile`, per-project spawn —
the concrete dev-loop mechanics).

## Context

ADR 0022 §C promises an App two batteries-included human surfaces — a **task inbox** (`/tasks`) and
**chat** (`/chat`) — both rendering the *same* form-js schemas the FormEditor authors, so a maker
writes zero frontend; plus the fully-custom `app-deno-gui` path (ADR 0009) when bespoke UI is
needed. §"Runtime & packaging" says the whole App compiles to one `deno compile` binary and flips
embedded↔remote by config. What is not yet specified is the connective tissue a maker actually
touches:

1. **How a human triggers work from a UI** — and how that relates to ADR 0025's unattended sources.
2. **How the UI is generated and rendered** — the three tiers and their runtime dependencies.
3. **The develop/run loop** — what "Run" does in the IDE, what talks to what, on which port, and how
   the embedded-vs-remote choice interacts with in-IDE testing.

Two concrete facts ground this. The `app-deno-gui` `gui-starter` already contains the primitive in
embryo: a button `fetch('/api/start')` whose backend does `POST /v2/process-instances` on the engine
at `NANOBPMN_BASE_URL`. And the console supervisor (`projects.rs`) already spawns a project as a
child process that connects to a configurable `deployTarget` (default `http://localhost:8080`, the
engine the console itself runs). The design below generalizes those two facts.

## Decision (proposed)

### 1. The App action API — the UI trigger surface

The served backend exposes a small, uniform **action API** (generalizing the starter's ad-hoc
`/api/start`), the single surface every human UI calls:

- `POST /app/actions/start/<process>` → `createInstance` (variables from the request body).
- `POST /app/actions/message/<name>`  → `CorrelateMessage` (start-or-feed, ADR 0025 §5).
- `POST /app/tasks/<key>/complete`     → the human-task completion API (form payload).
- `GET  /app/tasks` / `GET /app/data/...` → read surfaces for the inbox / bound controls.

A **UI trigger** — a button, a form submit, a chat turn — is just a call to this API. It is **not** a
new subsystem: it reuses the same `createInstance`/`CorrelateMessage` primitives ADR 0025's
unattended sources use.

**Attended vs. unattended (the load-bearing split).** A UI action has a human present to see the
result and retry; an unattended source (cron/webhook/file/imap) does not. Therefore:

| | Unattended source (ADR 0025) | **UI action (this ADR)** |
|---|---|---|
| Path | durable **inbox** → dispatcher → engine | **synchronous** call → engine → response to UI |
| Guarantee | at-least-once, restart-safe | at-most-once *as seen by the UI*; the UI shows success/error |
| Retry | the dispatcher on restart | **the human** (re-submit) |
| Idempotency | deterministic per-source key | a UI-supplied idempotency key (dedups double-submit) |

A UI action is **synchronous** by default because the UI wants the instance key / validation error
back *now* (a started flow, an error toast) — routing it through the async inbox would discard the
feedback the human is waiting for. A "fire-and-forget" UI button *may* opt into the inbox, but sync
is the default. The manifest may also declare a **`manual`** trigger (a labelled action id) so the
generic surfaces auto-render a "Start X" button and bespoke GUIs get a documented action to call.

### 2. UI generation — three tiers, one rendering layer (the browser)

All three serve from one `Deno.serve` backend; the **browser is the rendering layer** in every case.

1. **Generic surfaces — *generated*, zero frontend (ADR 0022 §C).** The Urban runtime ships `/tasks`
   and `/chat`. Their UI is **generated from** the form-js schemas (authored in FormEditor) plus the
   manifest's `surfaces`/`triggers` blocks: a user task → its `.form` rendered to claim/complete; a
   `manual` trigger → an auto "Start X" button; a bound control → a §1 read surface. This is the RAD
   loop: *design the form, the UI appears* — no frontend written.
2. **Bespoke GUI (`app-deno-gui`, ADR 0009).** Hand-built `public/` + a `Deno.serve` backend calling
   the §1 action API (the `gui-starter` seed). Full control when the generic surfaces aren't enough.
3. **Chat.** A conversational surface where an LLM turn (ADR 0022 §E) drives the §1 action API via
   the agent's tools (`start-process`/`complete-task`/`query-data`) — a UI trigger in natural
   language.

### 3. form-js viewer is a runtime dependency of the *emitted App*, not the console

The console/IDE carries `@bpmn-io/form-js-**editor**` (authoring — `FormEditor.tsx`). The emitted
Urban App needs `@bpmn-io/form-js-**viewer**` as **its own** runtime dependency, bundled into the
Deno binary via `deno compile`, because it is the **App's** browser (not the console's) that renders
forms at runtime for the inbox/chat surfaces and for bespoke GUIs that embed a form. So:

- The **console** depends on the editor (design time).
- The **App template + the generic-surface runtime** depend on the viewer (run time).

Same form-js schema, two bundles — authored in the IDE, rendered in the App. This is the concrete
answer to ADR 0022 §"Open questions" ("confirm form-js as both authoring schema *and*
inbox/chat renderer"): **yes**, via the sibling viewer package as an App-side dependency.

### 4. The develop/run loop — the IDE, the engine, and the ports

The console plays two roles at once: it **is the IDE** *and* it **runs a Nano engine** (the gateway,
default `:8080`).

- **Run** (`projects.rs` supervisor) spawns the project (`deno run main.ts`) as a child process. A
  **GUI** app additionally serves its own frontend on **its own port** (the starter uses `:8090`).
  The app connects to the engine at `deployTarget`/`NANOBPMN_BASE_URL` — the console's engine at
  `:8080`. During dev there are thus **two browser surfaces**: the console IDE (`:8080`, authoring)
  and the running app (`:8090`, the app's UI) — the Delphi "design the form / F9 to run it" loop,
  split across two ports.
- **Embedded vs. remote is a compile-time packaging choice, not the dev loop.** In-IDE Run/Test —
  *even for an app destined to ship embedded* — points at the console's engine (`deployTarget`),
  because that is the fast RAD loop: deploy to the already-running IDE engine and watch it in the
  console's Operate/read-model views. "Embedded Bernd" (ADR 0005) is realized at **compile**, behind
  the SDK transport seam, so shipping embedded vs. remote is a config swap, not a rewrite (ADR 0009).
- **The in-IDE test fork (decided).** When testing an *embedded-destined* app, the default is
  **shared** — connect to the console engine (fast, visible in console Operate, shared state). An
  opt-in **isolated-embedded** test mode spawns the app *truly embedded* with its own journal/data
  dir on a scratch path, to verify the shipping artifact faithfully (clean state, but invisible to
  the console read model). Default = shared; isolated is the "test the real binary" button.

## Phased plan

1. **action-api** — the §1 endpoints on the served backend (generalizing `/api/start`), with the
   attended-sync semantics + optional UI idempotency key; add the `manual` trigger type to the
   manifest (shared with ADR 0025 §1).
2. **form-viewer-runtime** — add `form-js-viewer` to the App template/runtime and a minimal form
   render+submit round-trip (the missing §3 dependency), reused by inbox/chat and bespoke GUIs.
3. **generic-surfaces** — `/tasks` (list open user tasks, render form, claim/complete) and the
   `manual`-trigger button home; chat (`/chat`) lands with ADR 0022 §E.
4. **run-model** — the §4 dev loop: default shared-engine Run for GUI apps (two ports) + the opt-in
   isolated-embedded test mode.

## Consequences

- A human can trigger work from *any* UI (generic, bespoke, or chat) through **one** action API, and
  the attended/unattended split gives UI actions the *right* semantics (synchronous, human-retried)
  rather than forcing them through the unattended inbox.
- The RAD promise is concrete: design a form in the IDE, get a working `/tasks` UI with no frontend —
  because the App bundles the form-js **viewer** and generates its surfaces from the manifest.
- The develop/run loop is the Delphi loop: author on `:8080`, F9 the app on `:8090`, watch it in the
  console's Operate — with embedded-vs-remote deferred to compile so the dev loop stays fast.
- Nothing new in the engine: this is served-backend + frontend wiring over existing primitives
  (`createInstance`/`CorrelateMessage`/human-task) and the existing `deno compile` supervisor.

## Open questions

- **Auth/identity on the served surfaces** — who may view `/tasks`, complete a task, or call an
  action? First-party auth (none / shared-secret / OIDC) and how the manifest expresses it; ties to
  ADR 0025 webhook auth and ADR 0024 secrets.
- **Form data binding on submit** — how a rendered form's payload maps to `createInstance` variables
  vs. `complete` variables, and how ADR 0024 bound controls (option lists from a datasource) are
  fetched by the viewer at render time.
- **Isolated-embedded test lifecycle** — where the scratch journal/data dir lives, how it's reset
  between runs, and whether the console can still *observe* an isolated app (a second read-model, or
  none).
- **Live/reactive controls** — the inbox is request/render today; do bound controls need push
  updates (SSE/websocket) to feel like Delphi's live data-aware controls (ties to ADR 0024's
  reactive-binding question)?
- **Port allocation** — fixed per project vs. supervisor-assigned, and how the IDE surfaces the
  running app's URL to the maker (a "preview" pane vs. an external browser tab).
