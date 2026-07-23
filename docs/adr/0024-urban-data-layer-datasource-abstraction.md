# ADR 0024 — Urban data layer & datasource abstraction (the BDE alias, for Nano Apps)

Status: **Accepted** (phases 1–2 *datasource-core* + *db-manager-panel* implemented; phase 3 *datasource-bindings* partially implemented — the §5 form field binding; the FEEL `data.query` builtin and chat `query-data` tool remain Proposed, as does phase 4).
Date: 2026-07-21.
Relates to: ADR 0022 (`0022-nano-rad-application.md`, **Urban** — the RAD App bundle; this ADR
expands its §D "Data layer" from a single SQLite file into a named-datasource seam),
ADR 0005 (`0005-embedded-u-nano.md`, **Bernd** — "single application source, choose deployment
by config"; this ADR applies that same position to *data*, not just the engine transport),
ADR 0007 (`0007-rad-extension-system.md`, the npm-installable pack contract — this ADR adds a
fourth specialization axis, `nano-ide-data-*`, beside `lang`/`app`/`trigger`),
ADR 0008 (`0008-polyglot-language-packs.md`, the pack-axis precedent),
ADR 0011 (`0011-editable-model-workbench.md`, the console authoring surfaces the **Data** panel joins),
ADR 0029 (`0029-urban-bindings-domain-model.md`) / ADR 0030 (`0030-domain-process-duality.md`, the
domain model + duality — this ADR's datasource is *state at rest*, one bank of the motion↔rest bridge;
the domain type generates the persist/rehydrate projection, §5),
ADR 0031 (`0031-process-relational-mapper.md`, the mapper for which this datasource is the **rest
bank** — realized by a Drizzle schema behind the `driver`/`url` alias),
`console/src/components/FormEditor.tsx` and `console/src/views/ProjectWorkspace.tsx` (the tab +
Monaco pattern the Data panel reuses, most recently extended by the form JSON view in PR #147),
`server/src/console/projects.rs` (the App scaffold + run/compile supervisor that will wire
datasources), and — for the Delphi lineage the design consciously inherits — the **Borland
Database Engine (BDE)**: a `TDataSource` alias over Paradox for local development and MS SQL /
InterBase via SQL Links for deployment, swapped by *alias config*, not by rewriting the form.

## Context

ADR 0022 §D gives an Urban App a **SQLite** database, a migrations directory, and a console
**Data** panel (browse tables, run queries, edit schema) — "the data-aware controls of the Delphi
analogy." That is correct but under-specified in the one place that historically carried the
value.

The lesson of Delphi's data story is that its power was **not Paradox**. It was that
`TTable`/`TQuery` → `TDataSource` → data-aware controls (`TDBGrid`, `TDBEdit`) was **one API**,
and the actual database sat *behind a named BDE alias*. A maker developed against embedded
Paradox tables inside the IDE, then pointed the alias at MS SQL for deployment — and the forms,
which bound to the *alias* and not to a driver, largely did not change. The abstraction, not the
engine, was the product.

Urban should reproduce that exactly, because it is the same promise ADR 0005 already makes for the
*engine* ("one application source, choose the engine by config"), now made for *data*. A maker
should develop an App against embedded SQLite and deploy it against Postgres by flipping one
config value — with **no change** to the App's processes, forms, workers, or decisions.

### What §D leaves open

- **A single hard-wired driver.** `data: { driver: "sqlite", path: ... }` names *the file*, not a
  reusable, swappable connection. There is no seam at which "SQLite in the IDE, Postgres in prod"
  becomes a config flip rather than an edit.
- **No binding contract.** It is unstated *how* a form field, a worker, or a FEEL expression
  refers to the data — so nothing stops them from coupling to `sqlite` directly, which would break
  the swap.
- **No non-SQLite path.** ADR 0022 §"Runtime & packaging" flips the *engine* to `remote`/`cluster`
  with no source change, but there is no equivalent story for the *database*.
- **The engine-journal question is unresolved** (§D open question: "two files, or does App data
  ride the engine's varstore?").

## Decision (proposed)

Introduce a **datasource** as Urban's first-class, named, swappable data connection — the
`TDataSource`/BDE-alias equivalent — and require every consumer to bind to it **by name**. The
DB Manager panel is a *client* of this seam, not the seam itself.

### 1. Named datasources in the manifest (the alias)

Replace ADR 0022 §D's single `data` block with **named datasources** whose driver and URL are
environment-resolvable. The name is stable; the driver is deployment config.

```jsonc
"data": {
  "default": "app",
  "sources": {
    // Urban's "Paradox": embedded, zero-install, the dev default.
    "app": {
      "driver":     "${NANO_APP_DB_DRIVER:-sqlite}",
      "url":        "${NANO_APP_DB_URL:-file:./app.db}",
      "migrations": "db/migrations"
    }
    // Deploy path (Urban's "SQL Links"): set
    //   NANO_APP_DB_DRIVER=postgres
    //   NANO_APP_DB_URL=postgres://user:pass@host/db
    // Same source name "app", zero source change — this is the BDE alias flip.
  }
}
```

**The binding rule (the load-bearing decision):** processes, workers, forms, and FEEL reference a
datasource **by name** (`data.app`), *never* by driver. Because the driver and URL are resolvable
purely from the environment, the **same App bundle** runs on SQLite in the IDE and Postgres in
production with no source change. This is ADR 0005's "choose by config" applied to data.

### 2. The datasource interface (the `TDataSet` equivalent)

One thin, uniform interface behind every driver — the surface all consumers share, so the driver
is interchangeable underneath:

```ts
interface DataSource {
  query(sql: string, params?: unknown[]): Promise<Row[]>;
  exec(sql: string, params?: unknown[]): Promise<{ changed: number }>;
  tx<T>(fn: (t: DataSource) => Promise<T>): Promise<T>;
  schema(): Promise<TableMeta[]>;   // powers the DB Manager, form data-binding, and the ADR 0029 domain types
}
```

- **SQLite driver** — core, always present; Deno's `node:sqlite` (or `@db/sqlite`). Embedded,
  single-file, ships *inside* the `deno compile` binary. This is the default and the whole
  local/single-user (home-automation, personal-automation) story.
- **Server drivers** (Postgres, MySQL, …) — the opt-in "SQL Links" path, symmetric with ADR 0022's
  `runtime.engine: remote|cluster`.

### 3. Drivers are a pack axis (`nano-ide-data-*`)

Core ships **SQLite only**, keeping the compiled binary tiny and dependency-free. Every other
driver arrives as an ADR 0007 pack under a **fourth specialization axis** — `nano-ide-data-*` —
beside `lang`/`app`/`trigger`. A pack supplies a driver implementing the §2 interface plus its
connection-string shape and any capability metadata (transactions, upsert dialect, param style).
This mirrors §B's `nano-ide-trigger-*` axis exactly: the community extends the data story without
touching the core.

### 4. The DB Manager panel (a console tab, same pattern as the editors)

The **Data** tab of an App project, slotting into `ProjectWorkspace.tsx` exactly as the
BPMN/DMN/Form tabs do, reusing the already-mounted Monaco + panel infrastructure (the discipline
PR #147 followed for the form JSON view). Three sub-surfaces:

| Sub-surface | Delphi analog | Backed by |
|---|---|---|
| **Tables** — tree of tables/columns/indexes + a paged row browser/editor | Database Desktop | `schema()` + paged `query()` |
| **SQL** — a Monaco SQL editor with a results grid | SQL Explorer | `query()` / `exec()` |
| **Migrations** — ordered `db/migrations/*.sql`, apply + status | (a modern addition) | a `_migrations` table via `tx()` |

The results grid is the honest `TDBGrid`. The panel targets a *named datasource*, so it browses
whatever `data.app` currently resolves to — SQLite in the IDE, Postgres in prod.

### 5. Binding — closing the loop with the other three designers

This is what makes it *Urban* rather than a SQLite GUI bolted on:

- **Forms** — a form-js field may declare `dataSource: "app"` + a query, so option lists / table
  rows come live from the datasource. The FormEditor gains a data-binding inspector: data-aware
  controls.
- **Processes / workers** — a worker handler receives an injected `ctx.data("app")` handle; no
  per-handler connection boilerplate.
- **FEEL** — expose a `data.query(...)` builtin (the read access ADR 0022 §D already promised) so
  decisions and message correlation can read App state.
- **Chat agent (ADR 0022 §E)** — its `query-data` tool *is* `DataSource.query` against the default
  source, **read-only by default**; a source is writable to the agent only when the manifest opts
  it in. (This resolves the §E "which capabilities are safe as LLM tools" question for the data
  axis.)

The datasource is also the **resting bank** of the motion↔rest bridge ADR 0030 §4 names. A domain
type (ADR 0029) declared once is the *same* object in flight (process variables) and at rest (a row
here), so the mapping between them — the persist / rehydrate that a Camunda maker writes as two
hand-coded workers — is **generated from the type, not hand-written.** Keeping App data in its own
datasource (§6) is what makes that resting bank *exist*; the projection is the wiring encapsulated at
the domain layer rather than eliminated. The generator that emits it is the **Process-Relational
Mapper** (ADR 0031), for which this datasource — its `driver`/`url` alias realized by a **Drizzle**
schema — is the rest bank; the PRM crosses *tense* (in-flight vs. at-rest), so it generates the
persist/rehydrate conjugation Drizzle alone cannot see.

### 6. App data is separate from the engine journal

Resolve ADR 0022 §D's open question in favor of **two files**: App data lives in its own
datasource, distinct from Bernd's engine journal/varstore. Reasons: (a) it keeps the DB-Manager +
alias story clean — a maker's `SELECT * FROM orders` must never risk the engine's durability
substrate; (b) the alias must be able to become Postgres while the engine journal stays local; (c)
the two have different lifecycles (App data is the maker's domain model; the journal is engine
internals). They share the App's data directory but not a file or a durability model.

The datasource is also where ADR 0028's multi-user identity lives: the `ApplicationConfiguration`
entity and the users/roles/sessions tables are rows in an App datasource, which is why per-user/
tenant **row-level scoping** (Postgres RLS vs. app-level filters) is a shared open question here.

## Phased plan

1. **datasource-core** — *implemented.* The §2 `DataSource` interface + the SQLite driver
   (Deno `node:sqlite`, embedded) + manifest §1 parsing (named sources, env-resolvable
   driver/URL via `${VAR:-default}`, `default`). Ships as the `@nanobpm/data` SDK
   (`server/src/console/data_sdk.ts`, materialised to `<project>/.nanobpm/data-sdk.ts`) with
   `openDataSource(name?)`; `ctx.data(name?)` is wired into the worker host (`worker_sdk.ts`),
   and the App-run sandbox now grants project-root write so a `file:./app.db` source is
   creatable. Ships the swap seam even before the GUI.
2. **db-manager-panel** — *implemented.* The §4 Data tab (Tables / SQL / Migrations) over a named datasource,
   reusing the Monaco/panel pattern.
3. **datasource-bindings** — §5 form field binding (*implemented* — a form-js choice field declares a
   `dataSource` binding `{ source, query, value?, label? }`, authored visually via the form editor's
   **"Data source (Urban)"** properties-panel inspector (pick an alias, write the query, name the
   value/label columns — no JSON hand-editing); the console Form **Preview** resolves it live through
   the phase-2 gateway into a data-aware control, and `validate.ts` cross-checks the `source` against
   declared `data.sources`) + FEEL `data.query` builtin (*Proposed*) + the chat `query-data` tool
   (read-only default, *Proposed*).
4. **datasource-postgres-pack** — the first `nano-ide-data-*` pack (Postgres), proving the axis and
   the SQLite→Postgres alias flip end-to-end.

## Consequences

- Urban gets the *actual* Delphi data story: **bind to an alias, swap the engine by config** —
  SQLite-dev / Postgres-deploy with no source change, the ADR 0005 position applied to data.
- One small new runtime seam (the `DataSource` interface) + one new pack axis (`nano-ide-data-*`);
  everything else composes existing pieces (Monaco panels, the pack contract, `deno compile`, the
  worker host, FEEL builtins).
- The three existing designers gain a data-binding target, so a maker builds a data-backed
  automation with no hand-written data-access code — the "data-aware controls" keystone.
- The default binary stays tiny and dependency-free (SQLite only); server drivers are opt-in packs,
  symmetric with the engine's `remote`/`cluster` opt-in.

## Open questions

- **Schema/dialect portability**: how much SQL-dialect smoothing does the seam own vs. leave to the
  maker? A minimal stance (params + a small `schema()` shape, raw SQL otherwise) keeps drivers thin
  but makes some migrations driver-specific. Do we want a tiny dialect shim for the common cases
  (autoincrement key, upsert, timestamps) so the *migrations* also survive the SQLite→Postgres
  flip, or accept per-driver migration dirs?
- **Connection lifecycle under `deno compile`**: pooling for server drivers vs. a single embedded
  SQLite handle — where does the pool live, and how does it survive App restart?
- **Migration safety on the alias flip**: applying `db/migrations` against a fresh Postgres on
  first deploy — automatic, or an explicit `nano app migrate` step (and how does that interact with
  the durable-inbox/idempotency work in ADR 0022's trigger open questions)?
- **Write guardrails for the chat agent**: is per-source `writable: true` sufficient, or do we need
  per-table / per-statement policy for the `query-data` tool?
- **Secrets**: `NANO_APP_DB_URL` carries credentials for server drivers — env-only, or a manifest
  secret-reference indirection so the bundle a maker shares never embeds a password?
- **Reactive binding**: Delphi's data-aware controls were *live* (edit the grid, the dataset
  updates). Is Urban's form binding read-mostly (query → render), or do we want two-way
  write-back from a bound control, and if so what is the concurrency model over Postgres?
