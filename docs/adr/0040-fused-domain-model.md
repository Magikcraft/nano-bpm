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

### 9. Shape-carrier representation in the model (increments 2–3)

§3–§5 fix *what* a composed motion shape is and *where* it lives; this section fixes the concrete
model representation the scan reads and the Modeller writes.

**Namespace.** Composed shapes are carried in a dedicated nano namespace —
`xmlns:nano="https://nanobpm.io/schema/shapes/1.0"` — registered as a bpmn-js **moddle extension**
(a descriptor JSON) alongside the existing Zeebe descriptors, so bpmn-js parses/serialises the
elements and the properties panel can edit them as first-class moddle objects. The reserved
`zeebe:property` envelope keys (`io.nanobpm.dataEnvelope.in`/`.out`, ADR 0033 §6) are **unchanged** —
this keeps slices 1/1b intact — but the id they carry may now name a composed `nano:shape` in
addition to a DB table or manifest type. The envelope stays a *reference*; §9 adds the *definitions*
it can point at.

**Where.** A shape is a reusable, model-scoped declaration, not a per-task attachment, so shape
declarations live in a single `nano:shapes` container on the defining `bpmn:process`'s
`bpmn:extensionElements` (process-level extension elements are well-supported by bpmn-js/moddle;
`bpmn:definitions`-level custom elements are not portably editable). Reuse across models resolves
**through the fuse** by id (§3), never by cross-file XML import.

**Schema.** Each `nano:shape` has an `id` (fuse identity), an optional `name`, and an **ordered** list
of composition children encoding the §4 algebra:

```xml
<bpmn:process id="orders" isExecutable="true">
  <bpmn:extensionElements>
    <nano:shapes>
      <nano:shape id="ApprovedOrder" name="Approved order">
        <nano:carry ref="Order" />                                  <!-- spread an entity whole -->
        <nano:project ref="Customer" fields="tier,region" via="Order.customerId" />
        <nano:extend name="approved" type="boolean" />              <!-- process-authored field -->
        <nano:extend name="reviewedBy" type="string" optional="true" />
        <nano:reference name="lines" ref="OrderLine" spread="false" list="true" />
      </nano:shape>
    </nano:shapes>
    <nano:meta key="classification" value="internal" />             <!-- model-level metadata (§5) -->
  </bpmn:extensionElements>
  ...
</bpmn:process>
```

- **`nano:carry ref`** — spread every field of the fused entity `ref` (a DB table, manifest type,
  external contract, or another shape) into this shape.
- **`nano:project ref fields via?`** — spread only the named `fields` of `ref`; the optional `via`
  FK-path (`Entity.fkColumn`, chainable with `.`) reaches fields on a related entity.
- **`nano:extend name type optional? list?`** — add a process-authored field. `type` is a scalar
  keyword (the existing `PRIMITIVE_TS` set: `string|number|integer|boolean|date|datetime|json`) or a
  fused entity id (a nominal reference, emitted as `DomainTypes[<id>]`). This is the load-bearing
  §4 *extend* case.
- **`nano:reference name ref spread? list?`** — pull in another motion shape: `spread="true"`
  inlines its fields, `spread="false"` (default) nests it as a single field `name: DomainTypes[ref]`.

The XML order is authoritative: composition is a left-to-right fold, so a later `extend` can shadow a
carried field (surfaced as a diagnostic when the types differ, §10).

### 10. Shape resolution in the fuse + diagnostics (increment 2)

A composed shape resolves, at scan/regen time, to a flat `DomainTypeDef { fields }` — the *same*
record shape the manifest `types` registry and DB tables already produce — so composed shapes enter
`DomainTypes` and every downstream consumer (the envelope pickers, `defineWorker`, `publishMessage`,
the FEEL scopes) types against `DomainTypes["ApprovedOrder"]` **for free**, with no new codegen path.

**Build order (per §6).** Leaves — DB tables (`db:<source>.<table>`), manifest types, external
contracts — fuse first. Shapes resolve in a **second pass**: gather every `nano:shape` across all the
project's models, then resolve their references against the combined registry. This lets a shape carry
another shape regardless of file/declaration order.

**Resolution (author-order fold).** Starting from an empty field map, for each child in XML order:
`carry(E)` copies all of `E`'s fields; `project(E, fields, via)` copies the named subset, following the
`via` FK path to the related entity for cross-entity fields; `extend(name, type)` adds a local field;
`reference(ref, spread)` either spreads `ref`'s fields or adds a nested `name: DomainTypes[ref]`
(wrapped `[]` when `list`). The result is provenance-tagged `model:<processId>` and added to the fuse.

**Diagnostics (scan-time, surfaced like the `workers[]` drift warning — never a silent merge):**

- **unresolved reference** — a `carry`/`project`/`reference`/`extend` names an id absent from the fuse.
- **reference cycle** — the second-pass shape graph has a cycle (DFS); reported with the offending path.
- **field conflict** — two composition steps contribute the same field name with **different** types
  (same type is an idempotent no-op; a deliberate shadow is a warning, not an error).
- **unknown project field / FK path** — a `project` names a field or `via` hop the source entity lacks.
- **duplicate shape id** — two `nano:shape` declarations share an id (ids are fuse identities); every
  colliding declaration is omitted so resolution never silently picks a scan-order winner.
- **ambiguous reference** — a bare id names **both** a manifest type and a table. The type wins (it is
  the more first-class, `DomainTypes`-visible entity); the table stays reachable via its `source.table`
  alias. A warning, not an error — the shape resolves against the type.
- **nominal reference to a table** — an `extend` type or a **non-spread** `reference` names a DB table.
  A nominal reference must resolve to a `DomainTypes` key (a manifest type or a resolved shape); a table
  would degrade to `unknown` in the emitted `.d.ts`, so it is rejected — spread the table's fields
  instead (`carry`/`project`, or `reference spread="true"`).
- **same-id across independent sources** — a shape id collides with a leaf entity that it does *not*
  declare as a source (projection is not conflict, §6).

Diagnostics are returned by the scan and rendered by the reifier/panel; a shape that fails to resolve
is omitted from `DomainTypes` (so a broken shape degrades to untyped, it does not break codegen).

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
- **Shape-carrier representation fixed (§9–§10).** Composed shapes are named `nano:shape` declarations
  in a `nano:shapes` container on the process's `bpmn:extensionElements`, in a dedicated
  `https://nanobpm.io/schema/shapes/1.0` moddle namespace; the existing `dataEnvelope.in/out` reference
  is unchanged and may now name a composed shape. This resolves **OQ3** for the shape-level `extend`
  vocabulary (scalar keywords ∪ fused ids) and model-level metadata (`nano:meta`).
- **Structural vs nominal within composition (OQ2).** Resolved: a composed shape resolves to a flat
  `DomainTypeDef` (structural, a field map), but every *reference* inside it (`carry`/`project`/
  `reference`/`extend`-to-entity) is **nominal** by fused id (ADR 0029 §4). Structure is the *output*
  of a fold over nominal inputs; the two do not compete.

## Open questions

1. **Fuse cache location & format** — is the computed fuse persisted (a generated `domain` artifact) for
   fast IDE/codegen reads, or recomputed on demand? What invalidates it?
2. **Reference/cycle grain** — do we allow a motion shape to carry another motion shape from a *different*
   model, and how aggressively do we guard cycles vs. lazily diagnose them? (§10 resolves shapes in a
   second cross-model pass with cycle-as-diagnostic; the remaining question is whether cross-model
   *carry* is offered in the authoring surface or restricted to same-model shapes in increment 3.)

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
- **2 — shape carrier in the model + full-algebra resolution** *(§9–§10)*: the headless engine layer.
  - a **nano moddle descriptor** (`https://nanobpm.io/schema/shapes/1.0`) registered with bpmn-js so
    `nano:shapes`/`nano:shape`/`nano:carry`/`nano:project`/`nano:extend`/`nano:reference`/`nano:meta`
    parse and serialise;
  - pure **read/write carrier helpers** (a `shapeCarrier.ts` sibling of `dataEnvelope.ts`, unit-tested)
    so a shape declaration round-trips in the `.bpmn` as undoable modeling commands;
  - a **Rust model scan** (extend `envelope_scan.rs`/a new `shape_scan.rs`) lifting `nano:shape`
    declarations into a `derivedShapes` payload injected on the `domaintypes` op;
  - **full-algebra fuse resolution** in the reifier (all four operations at once, §10), folding
    resolved shapes into the `DomainTypes` registry `emitDomainModel` already consumes, with the §10
    diagnostics returned by the op. No sophisticated UI yet — a raw list/JSON view is enough to prove
    the round-trip and the emitted types.
- **3 — composition authoring surface** *(§4 / Consequences)* — **done (PR: shape-composer-ui)**: the
  sophisticated visual structural-type composer in the Modeller. A dedicated React drawer
  (`ShapeComposer.tsx`) over the BPMN canvas, opened by a **Shapes** toolbar button on any BPMN model
  in an App project. It edits the primary process's `nano:shape` set through the modeler handle
  (`BpmnModeler` gains `getShapes()`/`setShapes()`, each write one undoable modeling command via
  `shapeCarrier.writeShapes`), so composer edits share the diagram's undo stack and dirty state.
  - add/reorder/remove carry/project/extend/reference rows (author order is the fold order, so
    reordering is a first-class edit), FK-path input for `project.via`, scalar/entity type pickers for
    `extend`, and multi-select field pickers for `project`;
  - **live-resolved field preview + inline §10 diagnostics** via a new server round-trip preview
    endpoint, `POST /projects/{name}/data/{source}/domaintypes/preview` (`previewDomainTypes`). It
    resolves the *in-editor* shapes (posted in the body) instead of the saved-model scan — `run_data_op`
    injects the caller-supplied `derivedShapes` only when absent, and runs with `write:false` — so the
    preview reflects unsaved edits without writing the generated files. Reuses the same `resolveShapes`
    path as the reifier, so there is no drift between preview and build. Debounced (350ms,
    request-id-guarded) so a burst of edits collapses to one round-trip;
  - the pure editing/parsing rules live in `shapeComposer.ts` (unit-tested, no modeler/server), including
    `shapeEntities` which folds sibling shapes into the pickers (so a shape can be composed from other
    shapes, with their preview-resolved fields);
  - the **Data envelope** pickers (ADR 0033 §6) gain composed `nano:shape` ids (across every process) as
    selectable envelope types, closing the loop from authoring a shape to typing a worker/message against
    it. (Deferred: cross-model `carry`/OQ2 — the composer offers only same-model + fused-datasource
    entities; a nominal ref to a shape/type in another model is not yet an authoring affordance.)
- **4 — external-shape contracts** *(source #3)*: a standalone external-shapes artifact (its own file),
  fused as leaves.
- **5 — model & shape metadata** *(§5)*: the extension vocabulary and app-wide surfacing.
- **6 — PRM wiring**: the fuse feeds 0031's rest/face projections (persist/rehydrate from composed
  shapes, including extended metadata where it maps).
