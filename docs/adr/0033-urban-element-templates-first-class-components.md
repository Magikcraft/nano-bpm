# ADR 0033 — Element templates as first-class Urban components (the palette)

Status: **Proposed.**
Date: 2026-07-22.
Relates to:
ADR 0022 (`0022-nano-rad-application.md`, **Urban** — §E's `workers[].taskType` / `llm` are the
*runtime* half of what an element template configures at design time; §A's manifest references models
by id),
ADR 0029 (`0029-urban-bindings-domain-model.md`, typed references + the domain model — a template's
`taskDefinition:type` is a typed reference to a worker, and its `zeebe:input` FEEL fields are scoped
by the same `feel.ts` resolver that §5 built for DMN, shipped in #197),
ADR 0007 (`0007-rad-extension-system.md`, the pack system — components are a natural new pack axis, the
installable-component-library analog; note the *naming collision*: 0007's "templates" are **project
scaffolds**, not element templates),
ADR 0027 (`0027-urban-app-manifest-spec.md`, the manifest envelope + §4 cross-reference rules — a
component's template↔worker↔type triad is bound and validated here),
ADR 0030 (`0030-domain-process-duality.md`, matter/motion/director — a component is a *unit of motion*
with a typed data face),
`console/src/components/BpmnModeler.tsx` (the bpmn-js modeler this ADR extends with the element-templates
modules + the Urban palette),
`console/src/lib/urbanComponents.ts` (the spike's bundled sample components).

## Context

Borland Delphi's power was the **component**: you dragged a `TButton`, `TTimer`, or `TDatabase` off a
palette onto a design surface, then configured it in the Object Inspector. A component fused three
things a working program otherwise keeps apart — a **design-time face** (palette icon + Object
Inspector), a **runtime behaviour** (the compiled VCL class), and a **data shape** (its published
properties and events). That fusion is what made a non-expert productive: the wiring was already inside
the component.

BPMN already has the raw material for the same move — the **element template**: JSON that turns a
generic task into a typed, configured element with its own properties panel, binding to
`zeebe:taskDefinition`, `zeebe:input` / `zeebe:output`, `zeebe:taskHeader`, and `zeebe:property`.
Connector templates *are* element templates. It is an open, well-supported format
(`@camunda/zeebe-element-templates-json-schema`, `bpmn-js-element-templates`) with an entire Camunda
connector catalog + Marketplace behind it.

But in Camunda 8 the three Delphi facets stay **decomposed**, the same disease ADRs 0024/0028 call out
elsewhere: you hand-write a template JSON, deploy a connector/worker separately, and invent the
variables ad hoc. Urban already owns the other two facets — the runtime (`workers[]` / `connections` /
`llm`, ADR 0022) and the data shape (domain `types` / `bindings`, ADR 0029) — but the design-time face
(element templates) was not wired into the modeler at all (`BpmnModeler.tsx` had only the stock Zeebe
properties panel).

## Decision (proposed)

Make **element templates the first-class component abstraction of Urban** — adopt the open format
verbatim, and add the Urban layer on top: a palette that *is* the installed component set, a
component's three facets bound as one artifact, and FEEL inputs scoped to the domain model.

### 1. A component = element template (design) + worker/connection (runtime) + domain type (data)

An Urban **component** is the fusion Delphi had, projected onto three artifacts that already exist:

| Delphi facet | Urban artifact |
|---|---|
| Palette icon + Object Inspector | the **element template** (`bpmn-js-element-templates`) |
| Compiled behaviour | the **worker** `workers[].taskType` / a `connection` / an `llm` (ADR 0022) |
| Published properties / data | the bound **domain type** (ADR 0029) — the I/O shape |

The template's `zeebe:taskDefinition:type` is the **seam** to the runtime: it must name a
`workers[].taskType` (or a connection). Bundling the three means they cannot drift, and the mismatch
("a task whose component has no backing worker") becomes a typed-reference diagnostic — the exact
move ADR 0029 §1–2 makes for forms/decisions.

### 2. The palette is the installed component set

First-classing means the modeler's palette (and template chooser) is *populated by the installed
components* — drag "Read Thermostat", "HTTP Request", or "Classify (LLM)" onto the canvas and it
stamps a service task pre-bound to that template. That is the Delphi component palette, and it is where
a maker starts: from components, not from a blank service task they must configure by hand.

### 3. Component input/output FEEL is scoped to the domain model

A template's `zeebe:input` properties are FEEL expressions over process variables. They get the **same
variable injection** shipped for DMN in #197: the domain type bound to the process (via `bindings[]`,
ADR 0030) feeds the input-property autocomplete. `feel.ts` shares one `bindingScope` resolver behind
`decisionScope` (DMN inputs) and `processScope` (component inputs, gateway conditions) — one tested
scope model, many editors. Symmetrically, a component that declares its **output** domain type (on its
worker, `workers[].outputType`, joined by `taskType`) lets downstream tasks/forms/DMN autocomplete on
its results: Delphi-grade continuity, component output →
process variable → next component input, all typed, feeding the PRM registry (ADR 0029 §6 / 0031).

### 4. Components are distributed as a pack axis (ADR 0007)

A component library is a **pack** that contributes `{ template JSON + worker impl + domain types }` —
the installable-component-library / Delphi-VCL analog, over 0007's existing consent surface. Adopting
the open element-template format means Urban is **Camunda-Marketplace-compatible both ways** for free:
the whole connector catalog is importable, and Urban components export as standard templates.

### 5. Spike (this PR) — proving the loop end to end

This PR wires the modeler and proves load → palette → Object Inspector → apply:

- `console/src/components/BpmnModeler.tsx` registers `CloudElementTemplatesCoreModule` +
  `CloudElementTemplatesPropertiesProviderModule` + the Zeebe (`camunda-bpmn-js-behaviors/lib/camunda-cloud`)
  behaviours, installs a sample component set via `elementTemplates.set(...)`, and adds a diagram-js
  **palette provider** (`UrbanComponentsPaletteProvider`) that stamps a template-bound task via
  `elementTemplates.createElement(template)` + `create.start`.
- `console/src/lib/urbanComponents.ts` bundles two sample components — **Read Thermostat** and
  **Classify (LLM)** — mirroring ADR 0022's `read-thermostat` worker and (E) LLM-as-worker. Both
  validate against the real `@camunda/zeebe-element-templates-json-schema`.

The spike deliberately stops short of the full design: components are a bundled constant here, not
loaded from installed packs or the project; the template↔worker↔type binding + validation, the FEEL
input scoping (§3), and the pack axis (§4) are the increments below.

## Consequences

**Positive.**
- The Delphi palette returns to the process canvas: makers assemble apps from typed components, not
  blank tasks — the core RAD ergonomic.
- Zero format invention: an open, validated format with a live catalog + two-way Marketplace compat.
- The component unifies design/runtime/data, so Urban's encapsulation thesis (ADRs 0024/0028) reaches
  the canvas — one artifact, no hand-wired seams that drift.
- Reuses the #197 FEEL scope machinery rather than a parallel mechanism.

**Negative / risk.**
- Adds three modeler dependencies (`bpmn-js-element-templates`, `camunda-bpmn-js-behaviors`, and the
  Zeebe moddle already present) and a heavier bpmn bundle.
- The palette/chooser UX for large catalogs (search, grouping, icons) is non-trivial — the spike's flat
  palette group is a placeholder.
- "Template" is now overloaded against 0007's project scaffolds; docs/UI must say **component**.

## Open questions

1. ~~**Where components live**~~ — **resolved (increment 2).** Both, merged by template id: the project's
   `.camunda/element-templates/` (the Camunda-standard location, so a Marketplace/connector catalog drops
   in as-is per §4) **and** an Urban-native `components/` dir; `components/` wins on an id collision. The
   modeler loads this installed set (`console/src/lib/projectComponents.ts`) instead of a bundled constant,
   so the palette *is* the project's component tray. No manifest `components[]` list is added (OQ2): a
   component instance is just the stamped service task, joined to its worker by `taskDefinition:type` ↔
   `workers[].taskType`. Packs layer another source on top (increment 6, §4).
2. ~~**Template ↔ worker binding surface**~~ — **resolved (PR #199).** The manifest does *not* gain a
   `components[]` list. Instead `bindings[]` binds a domain type to a **process** (ADR 0030's duality:
   a process is the motion of a typed domain object), which scopes *all* FEEL in that process —
   component `zeebe:input`, gateway conditions, output mappings — uniformly. The template↔worker link
   stays inferred from `taskDefinition:type` ↔ `workers[].taskType`; no per-instance manifest entry is
   needed, and connectors (which declare no `taskType`) raise no false "missing worker" diagnostics.
3. ~~**Output-type declaration**~~ — **resolved (PR #200).** A component declares its output domain
   type on its **worker** (the runtime facet, ADR 0022), not the template JSON: `workers[].outputType`
   names a declared domain type, joined to the component by `taskType` ↔ `zeebe:taskDefinition:type`.
   Homing it on the manifest keeps it validated (fail-closed `unknown-type`) and tested in the schema
   package, and avoids a non-standard extension field on the open template format. The modeler extracts
   each service task's output-mapping targets and the schema package types them via `outputType`.
4. **Chooser vs. palette** — adopt `bpmn-js-create-append-anything` for the append-anything popup, or
   keep a bespoke Urban palette.

## Increments

1. **spike** — element-templates wired into the modeler + palette + sample components (this PR). ✅
2. **component source** — the modeler loads the installed component set from the project instead of a
   bundled constant: `loadProjectComponents` scans `.camunda/element-templates/` + `components/` and merges
   by id (OQ1); `BpmnModeler` takes them as a live `components` prop feeding both `elementTemplates.set()`
   and the palette; the `urban-starter` scaffold seeds `components/` so a fresh Urban App ships a palette —
   PR (this). ✅
3. **the binding** — `bindings[].process` binds a domain type to a process (ADR 0030); schema +
   fail-closed `unknown-process` validation + completion (`process-ref`) — PR #199. ✅
4. **FEEL scoping** — `processScope` injects the bound type's fields into bpmn-js component-input FEEL
   autocomplete via a `variableResolver` service, mirroring #197's DMN injection — PR #199. ✅
5. **output typing** — `workers[].outputType` declares a component's output domain type; the modeler
   extracts output-mapping targets and `componentOutputScope` types those process variables, so a task
   placed after a component autocompletes on its result's fields (§3, OQ3) — PR #200. ✅
6. **pack axis** — components as an ADR 0007 pack contribution (§4).
