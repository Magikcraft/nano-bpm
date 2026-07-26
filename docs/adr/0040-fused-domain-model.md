# ADR 0040 — The Fused Domain Model (the registry is derived, not authored)

Status: **Proposed.**
Date: 2026-07-27.
Relates to:
ADR 0029 (`0029-urban-bindings-domain-model.md`, the domain model + symbol index; §4 makes the
datasource schema the **spine** and adds a manifest `types` registry. **This ADR revises §4.2**: the
registry stops being a hand-authored store and becomes a *derived* fusion of the three authoring
sources named below — nominal id-matching (§4) and the reifier (§6) are otherwise unchanged),
ADR 0030 (`0030-domain-process-duality.md`, the **charter**; §4 argues "the domain owns the motion↔rest
projection". This ADR supplies the *provenance* half of that ownership — where the one invariant is
authored — that 0030 asserts but does not locate),
ADR 0031 (`0031-process-relational-mapper.md`, the **PRM**; §1 posits "one declaration, three
projections; either end may seed it". This ADR names the **third seat** (the model) and the rule that
reconciles *multiple* seeds into the single invariant the PRM conjugates — the fuse feeds the PRM),
ADR 0032 (`0032-domain-resource-api.md`, the domain-resource API — a downstream consumer of the fused
model),
ADR 0033 (`0033-urban-element-templates-first-class-components.md`, the data-envelope carrier; §6
increment 12 — "server-side derivation of `workers[]`" — **is slice 1 of this ADR's scan pipeline**),
ADR 0024 (`0024-urban-data-layer-datasource-abstraction.md`, the datasource = source #2, the rest
bank; its `driver`/`url` seam is what makes source #3 — external shapes — a distinct provenance),
ADR 0027 (`0027-urban-app-manifest-spec.md`, the manifest — where the *derived* fuse cache is persisted,
spec-first; external-shape contracts live in their **own** spec-first artifact, not inline here),
Borland **Delphi** (design-your-tables *and* BDE-alias-an-existing-DB — the "own it or import it"
lineage this ADR generalizes to three sources).

## Context

Urban's domain model has, until now, been **authored in two places with opposite epistemics**:

- **rest** shapes are **derived** — introspected from the datasource schema (ADR 0029 §4.1 reifier,
  `DataSource.schema()` → `domain-rows.d.ts`). The database is the source of truth.
- **motion** shapes are **authored** — a hand-maintained `types` block in `nano.app.json` (ADR 0029
  §4.2), plus a hand-maintained `workers[]` I/O projection that types `defineWorker`.

The `workers[]` projection is not even *read* back from the model: the service-task *data envelope*
(ADR 0033) writes a `zeebe:property` carrier onto the task **and** a parallel `workers[]` entry, but
codegen reads only the manifest (`data_cli.ts` → `manifestWorkers()`), never the `.bpmn`. So the two
drift, and the model — the thing that says "this task carries an `Order`" — is *not* the source of
truth for the mapping it visibly expresses.

The deeper problem the drift exposed: a registry that is **half-derived (rest), half-authoritative
(motion)** is an incoherent object. The split ran along the *motion/rest* axis — an axis that cuts
*through* a single entity, since an `Order` is both. Two coherent fixes exist (project *from* a central
registry to both banks; or *derive* the registry from its natural owners), and Urban has been doing
neither cleanly — it derives rest and authors motion.

## Decision (proposed)

**The registry is fully derived. It is the *fusion* of three fully-authoritative sources.** Move the
derivation boundary off the *motion/rest* axis (which splits an entity) and onto the *provenance* axis
(which splits *between* entities). Each source owns what it is naturally best at; nothing is
half-authored.

### 1. The Fused Domain Model

`FusedDomainModel = fuse(models, database, externalShapes)`. It is a **computed index**, regenerated on
any source change or model import — never hand-edited. It is the concrete home of ADR 0031's "one
invariant per entity": the fuse *assembles* the invariant; the PRM (0031) *conjugates* it into the
face/motion/rest projections. This ADR is **provenance and assembly**; 0031 is **projection**.

This supersedes ADR 0029 §4.2's *authored* `types` block: that block's role (a home for shapes no
owned table backs) is taken over by source #1 (motion shapes, authored in-model) and source #3
(external-shape contracts). Any persisted `types` in the manifest becomes a **fuse cache / derived
artifact**, not a source of truth.

### 2. Three authoritative seeding sources

| # | Source | Authoritative for | Authored in | Provenance |
|---|---|---|---|---|
| 1 | **Models** (`.bpmn`) | motion shapes (composed payloads) + process/data-in-motion metadata | the Modeller | `model:<processId>` |
| 2 | **Database** | owned relational rest shapes + FK relations | the schema / DB designer | `db:<source>.<table>` |
| 3 | **External shapes** | typed contracts for data the app does not own | a standalone external-shapes artifact (its own file) | `external:<name>` |

FK relationships are relational structure and belong where relations live (source #2) — a generic
type registry modelling FKs would be unnatural. Data the app cannot introspect (remote services,
BYO databases behind the ADR 0024 `driver`/`url` seam) needs an **authored contract** (source #3).

### 3. Motion shapes live in the models

A motion shape (a task/message/variable payload) is **authored in the Modeller and stored in the
model that defines it** — as a nano-namespaced extension element on the `.bpmn`. It is contributed to
the fuse by the model scan (§6), with provenance `model:<processId>`. Reuse across models resolves
**through the fuse** (reference a shape by id; the fuse — built from every model — resolves it), not by
cross-file XML import. Shapes live in models; the fuse makes them app-visible.

### 4. Motion shapes are *composed*, and composition is the sophisticated part

A motion shape is not a flat field list re-declared from scratch; it is **composed** over the fuse's
other entries. The authoring surface (the Modeller's data authoring) must support a small structural
algebra:

- **carry** *(spread)* — include an entity whole: "this payload carries an `Order`" pulls in all of
  `Order`'s fields (an `Order` seeded from a DB table, an external contract, or another motion shape).
- **project** — include a **subset** of an entity's fields (part of it), following FK relations to
  reach related fields where needed.
- **extend** — add **process-authored fields** on top of the carried entity: metadata like
  `approved: boolean`, `reviewedBy: string` that is *part of this payload shape* but not part of the
  underlying entity. This is the load-bearing case: **"we carry this entity, *and* we have this
  metadata, and that is part of the payload shape."**
- **reference** — nest or spread another motion shape.

The resulting shape = `carry/project(sources) ⊕ extend(local fields)`. It is stored in the model,
contributed to the fuse, and (per 0031) conjugated into its rest/face projections — so the extended
metadata fields participate in persist/rehydrate for free where they map, and are flagged where they
do not (a metadata-only field has no rest column unless the fuse also owns/creates one).

**Runtime semantics (decided).** *Carrying* an entity **snapshots values into the process instance**
— the payload travels as Zeebe variables (values, not a live DB cursor), consistent with the
engine's untyped-JSON, values-in-motion model (ADR 0029 §3). The projection defines *which* values are
lifted in; it is not a live view. A task that needs live at-rest data reads it explicitly (the PRM's
rehydrate, 0031); "carry" never means "live cursor".

### 5. Model-carried metadata

Models carry metadata **about the process and about the data in motion** (e.g. `approved`) as
nano-namespaced extension elements. The scan lifts this into the fuse alongside the shapes, so
governance/state metadata is queryable app-wide without a second store. Shape-level metadata (a field
like `approved` on a payload) is the **extend** case of §4; model-level metadata (this *process/model*
is approved) is a property of the `model:` provenance entry.

### 6. The fuse: identity, conflict, build order

- **Identity = a stable id/name**, nominal (consistent with ADR 0029 §4). Each fused entry is
  **provenance-tagged** (§2) and may record *multiple* contributing sources.
- **Projection is not conflict.** A motion shape that *carries/projects* a DB or external entity
  declares those as explicit sources → it is a derived view with its own id; no collision.
- **Independent same-id claims across sources → a diagnostic, never a silent merge.** We do not
  guess-merge conflicting field types; the maker sees the same class of warning that first exposed the
  `workers[]` drift.
- **Build order:** sources #2 (DB) and #3 (external) are **leaves** and fuse first; source #1 (models)
  fuses next, because motion shapes *project over* the leaves. Model→model references resolve in a
  **second pass** (gather all declarations, then resolve refs); reference **cycles → a diagnostic**.

### 7. Import a model into a project

Adding or importing a `.bpmn` into a project **scans it and rebuilds the fuse** (incrementally). This
names a new project capability (model import) and makes the fuse an index maintained over the project's
models + datasources + external contracts, not a build-once artifact.

### 8. Codegen and increment 12 as slice 1

The console already scans nothing from the model; ADR 0033 increment 12 — "derive `workers[]` I/O from
the process models (Rust `parse_bpmn`) on regen, retiring the modeler-maintained projection" — **is the
first slice of the scan-and-rebuild pipeline** (§7 scans models into the fuse; §6 supplies the fuse rules):

1. `parse_bpmn` the project's models → extract motion shapes + task/message I/O bindings + metadata.
2. Contribute them to the fuse (leaves-first, then models, §6).
3. Emit `worker-io.d.ts` (and the FEEL I/O scopes) **from the fuse**, retiring the hand-maintained
   `workers[]`. The `.bpmn` carrier (ADR 0033) becomes the authoritative mapping; `workers[]`, if it
   survives at all, is a pure fuse-cache read.

Later slices grow the same path to the full composition algebra (§4), external contracts (§3), and the
PRM's rest/face projections (0031).

## Consequences

- **Positive — drift becomes structurally impossible.** The registry is derived; a source and its
  projection cannot disagree because the projection *is* the source, re-fused on change.
- **Positive — coherent epistemics.** Every entry has one clear owner; the only "derived-from-elsewhere"
  entries are external contracts, explicitly flagged. No motion/rest split-brain.
- **Positive — model-first authoring, honestly.** Motion shapes live where motion lives; "carry an
  entity + extend with metadata" is a first-class gesture, not a hand-edited JSON block.
- **Positive — feeds the existing fabric.** The fuse is exactly the invariant 0031's PRM and 0032's
  resource API already assume; this ADR gives them a real provenance model.
- **Negative — the Modeller data-authoring surface must become sophisticated.** A visual structural-type
  composer (carry/project/extend/reference, FK-aware) is a substantial UI, well beyond the current
  single-select *Data envelope* dropdown (ADR 0033).
- **Negative — fusion needs first-class diagnostics.** Same-id conflicts, unresolved references, and
  reference cycles must be surfaced clearly at scan time.
- **Negative — ADR 0029 §4.2 is revised.** The authored `types` registry is demoted to a derived
  cache; existing fixtures that hand-author `types` need a migration/compat story.

## Resolved (2026-07-27, @jwulf)

- **Carry is by snapshot.** Confirmed: carrying an entity snapshots values into the instance (§4);
  live at-rest reads are a separate, explicit gesture (the PRM's rehydrate, 0031). Not a live cursor.
- **External shapes live in their own file.** Source #3 is authored as a **standalone external-shapes
  artifact** (its own spec-first file), *not* as a subsection of the manifest or the datasource designer.
  It is scanned/fused as a leaf like the other sources.
- **Owned-table DDL writes through to the DB.** The Modeller stays introspection-first for rest: if it
  later *creates* an owned table, it **writes through to the datasource** (source #2), which then re-fuses
  as `db:<source>.<table>`. The model does not become a fourth seed for rest.

## Open questions

1. **Fuse cache location & format** — is the computed fuse persisted (a generated `domain` artifact) for
   fast IDE/codegen reads, or recomputed on demand? What invalidates it?
2. **Structural vs nominal *within composition*** — nominal id-matching stays for references (0029 §4),
   but *carry/project* is structural by nature (pick fields). How do the two coexist in the schema and
   the diagnostics?
3. **Metadata schema & namespace** — the nano extension-element vocabulary for §5 (shape-level extend
   fields vs model-level metadata) and how much is free-form vs typed.
4. **Reference/cycle grain** — do we allow a motion shape to carry another motion shape from a *different*
   model, and how aggressively do we guard cycles vs. lazily diagnose them?

## Increments

- **1 — model scan → worker I/O from the fuse** *(= ADR 0033 increment 12)*: `parse_bpmn` the models,
  extract task/message I/O bindings, emit `worker-io` from the fuse; retire hand-maintained `workers[]`.
  - **1b — message payloads from the fuse** *(slice 2)*: the same model scan lifts the data envelope off
    each shared `bpmn:message` (`io.nanobpm.dataEnvelope.in`/`.out`, keyed by the message `name`) and
    emits a typed publish registry (`message-io.d.ts`: `MessageName` union + `MessagePayloads`) plus a
    typed `publishMessage` wrapper (`messages.ts`, `@nanobpm/messages`), so publishing a message is typed
    by the model. There is no manifest projection for messages (unlike `workers[]`), so the model-derived
    map is authoritative directly. The message `name` is a correlation identity, so it is matched verbatim
    (not trimmed). The received (`in`) type keys `publishMessage`; the `out` side is scanned but reserved
    for future correlate-response typing.
- **2 — motion-shape carrier in the model**: store composed motion shapes as nano extension elements;
  scan lifts them into the fuse; nominal references resolve through the fuse (§3, §6).
- **3 — composition algebra + Modeller surface**: carry/project/extend/reference authoring (§4),
  FK-aware, with same-id/cycle diagnostics (§6).
- **4 — external-shape contracts** *(source #3)*: a standalone external-shapes artifact (its own file),
  fused as leaves.
- **5 — model & shape metadata** *(§5)*: the extension vocabulary and app-wide surfacing.
- **6 — PRM wiring**: the fuse feeds 0031's rest/face projections (persist/rehydrate from composed
  shapes, including extended metadata where it maps).
