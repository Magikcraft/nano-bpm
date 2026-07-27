# ADR 0042 — The Urban Page/Screen Composer (a WYSIWYG app surface, the Delphi Form Designer)

Status: **Proposed.**
Date: 2026-07-27.
Relates to: ADR 0026 (`0026-urban-human-surfaces-and-run-model.md`, the **run model + action API**; this
ADR fills the gap 0026 names between its **tier 1** "generated `/tasks`, zero layout control" and its
**tier 2** "hand-coded `public/`" — it is the missing *tier 1.5*, an authored-but-not-hand-coded screen —
and it consumes 0026 §1's action API and §3's form-js **viewer**-as-App-runtime-dependency precedent),
ADR 0022 (`0022-nano-rad-application.md`, the RAD keystone; §C "Human surfaces" — this is the Delphi
**Form Designer** that §C's "design the form, the UI appears" only half-delivered via auto-generation),
ADR 0040 (`0040-fused-domain-model.md`, the **fuse**; the composer's data/action pickers are *typed by
the fuse* — a grid column picker offers an entity's fields, an action offers the manifest's processes),
ADR 0024 (`0024-...datasource...`, the **rest bank** a `dataGrid` reads),
ADR 0009 (`0009-gui-application-projects.md`, the served-UI `app` kind + `Deno.serve` backend the
runtime renderer serves from),
`console/src/components/FormEditor.tsx` / `ShapeComposer.tsx` (the sibling authoring surfaces this joins —
author-in-editor → **owned JSON schema** → runtime viewer, the same move lifted from *one form* to *a
whole screen*), and **Craft.js** (`@craftjs/core`, the adopted React drag-drop framework — we own the
component set and the emitted schema; Craft.js is the *canvas*, not the contract).

## Context

An Urban app today has exactly two ways to present a UI (ADR 0026 §2):

1. **Generic surfaces** — the runtime auto-generates `/tasks` + `/chat` from the form-js schemas and the
   manifest. Zero frontend, but also **zero layout control**: the maker cannot compose a *screen* (a
   submit box next to a live list next to a status line) — only a task inbox.
2. **Bespoke GUI** — hand-code `public/` + a JSON API. Full control, but it is a from-scratch SPA.

The witness is `urban-pr-review` (`~/workspace/urban-pr-review`): its home screen — a "Submit PR" form, a
tabbed "Converging / History" list bound to the `pull_requests` table, and a status footer — is **not a
form** (form-js authors *one form's fields*, not a page), so it fell all the way to tier 2: a hand-written
`public/index.html` + `public/app.js` (134 lines) + `public/style.css` (135 lines) **and** a bespoke
`/api/prs` JSON API inside `main.ts`. Every Urban maker rebuilds their own Tasklist by hand.

Delphi's actual magic was never its single-form editor; it was the **Form Designer / VCL** — a WYSIWYG
canvas where you drop **data-aware controls** (a grid bound to a table, an edit bound to a field, a button
bound to an action) and lay out *whole screens*. Urban has a form editor, a shape composer, a BPMN
modeller, a DMN editor — and **no page/screen designer.** That is the gap.

## Decision (proposed)

Add the **Page Composer**: a visual, drag-and-drop authoring surface in the console that emits an **owned,
Craft.js-independent `page.json` schema**, rendered at runtime by a **generic App-side page renderer** that
binds data-aware controls to the datasource and the ADR 0026 §1 action API. It is the same move as every
other Urban editor — *author a schema, a runtime viewer renders it* — lifted from one form to a whole
screen, with the palette **typed by the fuse** (ADR 0040).

### 1. Own the schema; Craft.js is the canvas, not the contract

The persisted artifact is a **normalized `page.json`** that is *ours*, not Craft.js's internal serialized
node format. The composer keeps Craft.js editor state in memory and **serializes down** to `page.json` on
save (and reflates on open). This decouples the authoring tool from the runtime contract exactly as the
form-js schema (ours) is decoupled from the form-js editor, and lets Craft.js be swapped without a data
migration. The runtime renderer reads only `page.json` and never imports Craft.js.

`page.json` (v1) is a titled, ordered list of typed nodes (no nesting yet — see Increments):

```jsonc
{
  "schemaVersion": "1.0",
  "title": "PR Review Convergence",
  "nodes": [
    { "type": "text", "id": "h1", "props": { "text": "PR Review Convergence", "variant": "heading" } },
    { "type": "actionForm", "id": "submit", "props": {
        "title": "Submit PR", "submitLabel": "Submit PR",
        "action": { "kind": "startProcess", "process": "convergence-loop" },
        "fields": [ { "key": "pr", "label": "owner/repo#123 or PR URL", "type": "text" } ] } },
    { "type": "dataGrid", "id": "list", "props": {
        "title": "Converging",
        "data": { "kind": "datasource", "source": "app", "table": "pull_requests" },
        "columns": [ { "field": "pr_key", "header": "PR" }, { "field": "status", "header": "Status" } ] } }
  ]
}
```

### 2. Three data-aware components (v1 palette) — enough to replace the witness

| Component | Renders | Binds to | Replaces (urban-pr-review) |
|---|---|---|---|
| **text** | heading / body / sub | — (static) | the `<header>` |
| **actionForm** | labelled inputs + a submit button | `POST /app/actions/start/<process>` (fields → variables) | the "Submit PR" form |
| **dataGrid** | a table of rows | `GET /app/data/<source>/<table>` | the "Converging" list |

Two bindings only, both **typed by the fuse (ADR 0040)**: an `actionForm` picks a manifest **process** (and
its start variables); a `dataGrid` picks a **datasource table/entity** (and its columns) from the fuse's
resolved fields. Authoring a screen thus reuses the same registry the ShapeComposer and worker-I/O pickers
already consume — no new symbol source, no drift.

### 3. The generic App runtime — a small serve helper + a schema-driven renderer

The emitted App gains a **generic runtime** (materialized into the app the way `@nanobpm/data`'s
`data_sdk.ts` already is — a `server/src/console/*.ts` module the scaffolder writes into the project), so a
`page.json` is a *served, working screen* with no hand-written frontend or API:

- `GET  /app/pages/<id>` → the page's `page.json`.
- `GET  /app/data/<source>/<table>` → rows, via `@nanobpm/data` `openDataSource()` (the rest bank, ADR 0024).
- `POST /app/actions/start/<process>` → `createProcessInstance`, via `@nanobpm/nano-sdk` (the engine, ADR
  0026 §1 `start` action, attended-sync semantics).
- `GET  /` (+ static) → serves the **renderer shell**: a tiny client that fetches `page.json`, renders the
  nodes, wires `dataGrid` → `/app/data` (with a light refresh) and `actionForm` → `/app/actions/start`, and
  shows success/error inline (attended, human-retried — ADR 0026 §1).

The renderer is **schema-driven and framework-light** (it does *not* ship Craft.js — that is authoring-only,
console-side). This is 0026 §3's principle — *the App bundles its own runtime viewer* — applied to pages the
way 0026 applies it to the form-js viewer.

### 4. The dev loop is unchanged (ADR 0026 §4)

A `*.page.json` opens in the Page Composer in the console IDE (a new `kind = "page"` editor, sibling to
bpmn/dmn/form dispatch in `ProjectWorkspace.tsx`). **Run** serves the app on its own port; the composed page
is live at `/`. Author on `:8080`, F9 the app on `:8090`, watch it — the Delphi loop, now with a real screen
designer instead of hand-written HTML.

## Consequences

- The Delphi promise is finally concrete: a maker **draws** a screen (a grid, a submit form, a button) and
  gets a served, data-bound UI — the tier-1.5 surface ADR 0026 named but left empty.
- `urban-pr-review`'s entire `public/` **and** most of its bespoke `/api/prs` JSON API collapse into one
  composed `home.page.json` + the generic runtime. The hand-written Tasklist goes away.
- The fused domain model (ADR 0040) pays a **UI dividend**: the palette's data/action pickers are typed by
  the same fuse that types workers and messages, so authoring a screen is drift-free and typed.
- Nothing new in the engine: this is served-backend + a schema-driven browser renderer over the existing
  `createInstance` primitive and the datasource — parity-preserving, App-tier, erased at runtime.
- **Honest gaps.** (a) v1 is a flat node list — no nesting/layout containers, no responsive grid yet
  (Increment 2). (b) `dataGrid` is request/refresh, not push/reactive (ADR 0026's "live controls" open
  question — Increment 3, SSE). (c) auth on the served surfaces is inherited from ADR 0028 (unresolved in
  v1; the demo runs open). (d) the action API is `start`-only in v1 — `message`/task-`complete` come with
  the generic-surfaces work (ADR 0026 §1).

## Increments

- **1 — thin slice (this ADR):** the owned `page.json` schema + serializer, the Craft.js composer with the
  three v1 components, the generic runtime (`/app/pages`, `/app/data`, `/app/actions/start`, renderer shell),
  and a scaffolded starter page — proven by reproducing `urban-pr-review`'s Submit + Converging screen with
  zero hand-written frontend.
- **2 — layout & nesting:** container/columns/tabs nodes (Craft.js nested canvases), the flat list becomes a
  tree; a **detail/record** control (one entity by id) and a **form-embed** (a form-js schema in a page).
- **3 — reactive controls:** `dataGrid`/bound controls get push updates (SSE/websocket) to feel Delphi-live
  (ADR 0026's open reactive-binding question), and the remaining action bindings (`message`, task-complete)
  join the palette so the composed page is a full inbox replacement.
- **4 — chat & LLM surface:** a chat panel node driving the action API via the agent tools (ADR 0022 §E).

## Open questions

- **Where a page's start-variable mapping lives** — v1 maps `actionForm` field `key` → a top-level start
  variable verbatim; richer mapping (nested paths, typed coercion from the fuse) is deferred to Increment 2.
- **Page routing/multi-screen** — v1 serves a single `home.page.json` at `/`; multi-page apps (a nav +
  per-entity detail routes) need a routing model (Increment 2).
- **Styling/theming** — v1 ships one built-in stylesheet; whether makers get theme tokens or per-node style
  props is deferred.
