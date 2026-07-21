# ADR 0031 — The Process-Relational Mapper (an ORM whose third bank is the engine)

Status: **Proposed.**
Date: 2026-07-21.
Relates to: ADR 0030 (`0030-domain-process-duality.md`, the **charter**; §4 posits a "generated
motion↔rest projection" but does not name the mechanism — this ADR is that mechanism, the *how* of the
bridge 0030 argues Nano is uniquely able to build),
ADR 0029 (`0029-urban-bindings-domain-model.md`, the domain model + symbol index; §4 makes the
datasource schema the **spine** and adds a manifest `types` registry — the PRM is the mapping layer
that turns those declared types into the three coherent projections),
ADR 0024 (`0024-urban-data-layer-datasource-abstraction.md`, the datasource = the **rest bank**; the
BDE-alias `driver`/`url` seam is what lets the rest projection flip SQLite↔Postgres by env),
ADR 0021 (`0021-process-sla-as-a-first-class-abstraction.md`, SLA = "state with a **future tense**" —
the clearest existing witness of the *tense axis* this ADR names as the PRM's extra dimension),
ADR 0005 (`0005-embedded-u-nano.md`, the embedded engine — the reason the motion bank *exists* to be
mapped, versus a remote engine Nano could not project from),
ADR 0022 (`0022-nano-rad-application.md`, the product keystone; the App bundle is where the generated
mappers ship), ADR 0027 (`0027-urban-app-manifest-spec.md`, the manifest — where the `types` registry
and projection config are declared, spec-first),
ADR 0012 (`0012-decoupling-terminal-state-from-exporter-lag.md`) and `server/src/readstore.rs` (the
engine's *own* exported read store — a second candidate rest bank, disambiguated in §6/Open),
Drizzle ORM (the adopted Deno-native, SQL-first rest-bank tool — schema + migrations + typed queries),
Borland **Delphi**'s `TDataSet`/BDE (the *face*-binding lineage: a data-aware control bound to a
column — the ancestor of the face projection, not of an object graph), and Camunda's **element
templates** (design-time typing over an untyped runtime — the same erased-at-runtime move, generalized
from one task to the whole App).

## Context

A conventional **Object-Relational Mapper** reconciles **two** representations of one datum: the
object in memory and the row on disk. It spans a boundary in *space* (heap vs. storage) and is
otherwise tenseless — a row is a row, an object is an object, and the ORM keeps them equal.

ADR 0030 §4 argues that Urban's differentiator is a domain model that bridges **state-in-motion**
(process variables) and **state-at-rest** (a datasource row, ADR 0024), and that the persist/rehydrate
mapping between them should be **generated, not hand-written** — killing the two workers every Camunda
customer writes by hand. ADR 0029 §4 supplies the *material*: types come from the datasource schema
(the spine) and a manifest `types` registry. But neither ADR names the *mechanism* that keeps one
domain object coherent across the engine and the database, nor how it differs from an ORM. This ADR
does, because the difference decides what we generate.

The load-bearing observation: **a domain type in Urban has not two but three projections, and one
invariant.**

| Projection | Representation | 0030 term (§1/§5) | Owning tool |
|---|---|---|---|
| **Face** | form fields (+ DMN `typeRef`s) | matter's *face* | form-js / DMN (0011) |
| **Motion** | process variables (engine JSON payload) | *life* | the embedded engine (0005) |
| **Rest** | the database row | *memory* | **Drizzle** over the datasource (0024) |

This is 0030 §5's one-liner made structural — a domain object is "Delphi's record + a lifecycle =
**face + memory + life**." The type is the invariant; the mapper is what holds the three in agreement.

And there is a second axis a real ORM does not have: **tense.** The process variable is the object in
its *present-continuous* — "the Order is *becoming* paid," in flight, observable, waiting on the world.
The row is the same object in the *perfect* tense — "the Order *became* paid," at rest. A classic ORM
never crosses this boundary; a **Process-Relational Mapper does.** This is also the precise mechanism
behind Camunda's unreconciled sprawl (0030 §Context): the untyped payload is the present-continuous
object and the customer's DB schema is the perfect-tense object, and **nobody owns the conjugation** —
so the customer hand-writes it, twice (persist and rehydrate), forever.

Why not simply call this an ORM and adopt one wholesale? Because a classic (ActiveRecord/Hibernate)
ORM would put **behaviour on the record** — `Order.fulfill()` as a method — which directly contradicts
0030 §5 ("the method *became a process*"): in Urban an object's behaviour is a durable process
instance, not a stack frame. An object graph with methods would compete with the process for owning
behaviour and re-introduce the bolted-on decomposition 0030 rejects. So the ORM lineage is the right
*shape* and the wrong *whole*.

## Decision (proposed)

Name and adopt the **Process-Relational Mapper (PRM)**: the generated layer that projects a single
declared domain type onto its three representations — **face, motion, rest** — and reconciles them
across space *and* tense, over an engine that stays untyped and Zeebe-faithful. Concretely:

### 1. One declaration, three projections; either end may seed it

The domain type (ADR 0029 §4 — a datasource-schema record or a manifest `types` entry) is the single
source of truth. From it the PRM derives:

- the **rest** shape (a Drizzle table/schema),
- the **motion** shape (the process `variables` type + FEEL-path autocomplete, 0029 §5), and
- the **face** binding (form-field ↔ type-field, DMN `typeRef` ↔ type).

Per 0030 §3's **authoring-symmetry** test, either end may *seed* the declaration — declare the type and
scaffold the table (domain-first), or introspect an existing table and reflect the type (DB-first) —
after which all three projections re-derive from the one invariant.

### 2. The PRM crosses tense, not just space — and that is what it *is*

The two generated mappers are the **conjugation** between tenses:

- **persist** (`variables → row`) — the present-continuous object made perfect: the object-in-flight
  written to its resting row.
- **rehydrate** (`row → variables`) — the perfect object made present-continuous again: a resting row
  lifted back into a running instance.

These are exactly the two workers a Camunda maker hand-writes (0030 §4). In Urban they are **generated
from the type**, because the type *is* the projection. Owning the conjugation is the whole job; the
row↔object *space* mapping is the easy, ORM-familiar part.

### 3. Drizzle owns the rest bank; the PRM generates the two things Drizzle cannot see

Adopt **Drizzle** (Deno-native, SQL-first: typed schema, migration generation, typed query builder) as
the **rest projection** tool. This is a *sub*-decision — Drizzle is **one bank**, swappable behind the
0024 datasource alias — not the architecture. The PRM generates what Drizzle structurally cannot:

- the **motion↔rest conjugation** (§2's persist/rehydrate), which no ORM has because no ORM knows about
  a process engine; and
- the **face bindings** (type ↔ form field / DMN `typeRef`), which live in the model editors, not the
  DB.

Drizzle's typed query builder is also what makes 0030 §5's **domain-shaped read model** ("Orders
awaiting payment") a typed query over the rest bank rather than hand-rolled SQL.

### 4. Two generation directions, both bounded to Drizzle-schema in/out

Authoring symmetry (§1) means the PRM generates in both directions, and this is the most ORM-like part,
so it is scoped deliberately:

- **domain-first** — declared type → **emit a Drizzle schema** → Drizzle emits the migration. We do
  *not* build a bespoke migration engine; we generate Drizzle schema and let Drizzle own DDL.
- **DB-first** — an existing table (Drizzle introspection / `schema()`'s `TableMeta`, 0024) → **reflect
  a domain type**. Importing a table yields a type; the maker promotes/binds it (0029 §4 on-ramp).

The registry and both directions resolve against the ADR 0029 §1 symbol index — one enumeration, no
drift.

### 5. The projection is generated code + erased-at-runtime types, not runtime reflection

Consistent with ADR 0029 §3 and Camunda's element-template precedent, the PRM emits **plain typed
mapper functions and type declarations** into the App tier, checked while authoring, at `deno compile`,
and at boot, then erased. There is no runtime metamodel, no reflection on the hot path, no ORM
session/identity-map. The engine still moves untyped JSON; switch-over parity (lift onto Camunda 8) is
byte-identical because everything the PRM adds lives *above* the engine contract.

### 6. Boundaries — what the PRM deliberately is *not*

- **Not a cross-store transaction.** The engine journal and the App datasource are **two files** (ADR
  0024 §6); the PRM does **not** attempt 2PC across them. Reconciliation is projection-time and
  **idempotent** — persist is an upsert keyed by the process-instance / domain key, replayable from the
  engine's event log (the log is the source of truth, 0030 §Context). A crash between engine-commit and
  DB-persist self-heals on replay; it never leaves a torn distributed transaction. (See Open questions
  for the outbox-vs-project-from-log choice.)
- **Not an object graph.** No identity map, no lazy loading, no association traversal. The maker
  navigates relations with Drizzle queries, not a materialized graph.
- **Not behaviour-on-the-record.** The record has no methods; its behaviour is a process (0030 §5). The
  PRM maps *state*, never verbs.

## Consequences

- Urban gains the named mechanism 0030 §4 gestured at: the **PRM** is what makes "the domain model *is*
  the projection" concrete and buildable, and it is the reason the Drizzle choice is a localized,
  swappable sub-decision rather than the whole data story.
- The two hand-written Camunda workers (persist/rehydrate) become **generated** — the single largest
  ergonomic payoff of the domain–process duality, delivered as code, not a slogan.
- The read model can be **domain-shaped** (0030 §5) via Drizzle-typed queries over the rest bank.
- The engine is untouched — untyped, fast, Zeebe-faithful; all PRM output is App-tier, erased at
  runtime, parity-preserving.
- **Honest gaps.** (a) No cross-store atomicity — correctness rests on idempotent, log-replayable
  projection, so the persist mapper must be write-idempotent by construction. (b) The off-type remote
  worker gap (0029) is inherited — the PRM types the boundary, not the wire. (c) form-`key` inference
  is heuristic (0029), so DB-first/face reconciliation may need maker confirmation. (d) A second rest
  bank already exists — the engine's exported read store (0012, `readstore.rs`) — so "the rest bank"
  must be disambiguated (Open) to avoid two competing resting representations.

## Open questions

1. **Grain of the rest projection — the deepest one.** Is a resting row **per process instance** (the
   rest bank is a typed mirror of instance history) or **per domain entity** (one `Order` row that many
   processes and instances read and mutate over its life)? The latter is the truer "domain object" and
   the harder mapping (concurrent motions over one resting entity); the former is a near-free export.
   This choice defines what the PRM fundamentally *is* and should be settled before schema work.
2. **Consistency model.** Outbox (engine writes an outbox row in-journal, a projector drains it to the
   DB) vs. **project-from-event-log** (the DB is a pure projection the App rebuilds from the log) vs.
   idempotent upsert at seam-time. Ties into ADR 0012's exporter-lag decoupling.
3. **Who owns migrations authoritatively** — the checked-in Drizzle schema, or the declared domain type
   (Drizzle schema generated)? Round-tripping (§4's two directions) risks a drift both-ways; likely one
   is canonical per project with the other reconciled/validated.
4. **When does persist fire** — on every variable write (freshest, costliest), at BPMN milestones, or
   only at instance completion (cheapest, stalest)? Probably declared per type/process, defaulting to
   milestone.
5. **Which rest bank feeds the read model** — the PRM's domain projection or the engine's own read
   store (0012)? Two layers (domain view over process view) or one canonical noun?
6. **Nominal vs structural** domain identity (shared with ADR 0029/0030) — ~~does `Order` match by name
   or by shape across face, motion, and rest~~ **Resolved (2026-07): nominal** (by stable type id),
   with a reserved `match: "structural"` escape hatch in the schema for later shape-based reuse; see
   ADR 0029 open question 3 for the full rationale (implemented in PR #173). How nominal identity maps
   onto DMN `typeRef`s remains open (ADR 0029 open question 6).
7. **Relations across tense.** A classic ORM models associations at rest; when two resting entities
   relate but their *motions* are separate process instances, does the PRM model the relation only at
   rest (Drizzle FK) or also in motion (correlation between instances)?
