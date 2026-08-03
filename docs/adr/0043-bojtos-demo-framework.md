# ADR 0043 — Bojtos: a publishable in-browser BPMN demo framework

Status: **Proposed.**
Date: 2026-07-28.

> **Extraction update (2026-08).** The `bojtos-kit` and `bojtos-react` packages
> described here were extracted from this monorepo into the standalone public
> repo [`nanobpm/bojtos`](https://github.com/nanobpm/bojtos) and are published
> from there; they consume `@nanobpm/engine-wasm` from npm. `@nanobpm/engine-wasm`
> continues to be built (`make console-wasm`) and published from this repo. The
> design below is retained as the original record.

Relates to:
ADR 0005 (`0005-embedded-u-nano.md`, Bernd — the embedded engine this wraps), the in-browser
test-run substrate (`console/src/components/TestRunPanel.tsx` — loads the wasm `TestEngine`, deploys
BPMN, highlights active/incident elements on a bpmn-js viewer, renders the trace timeline;
`console/src/lib/simTrace.ts` — the trace fold), the wasm-bindgen wrapper (`engine-wasm/` around the
real `nanobpmn-engine-core`; `engine-wasm/pkg/nanobpmn_engine.d.ts` — the `TestEngine` API contract, this
package's public surface after step 1), the build seam (`Makefile` `console-wasm` = `wasm-pack build
--target web` output, historically synced into `console/src/wasm` and relocated to `engine-wasm/pkg` by
this ADR's step 1), and the internal-package precedent (`spec-app/package.json` — `@nanobpm/*` consumed
by the console via a `file:` dependency; ADR 0027). Also ADR 0033 (`0033-urban-element-templates-first-class-components.md`,
§6 — the **data envelope**: a task's input/output type carried by reference on the element) and ADR 0040
(`0040-fused-domain-model.md`, homing that typing in the model), which together give the editable worker
boxes (§5) typed completion from the model; `console/src/lib/dataEnvelope.ts` is the read/write carrier.

## Context

The concrete goal: publish something that lets **external developers** rapidly build in-browser BPMN
**demo pages** — show a BPMN model in the browser and *execute it with workers running in the browser* —
so different demos for different use cases are cheap to author. This framework is named **Bojtos**,
after **Peter Bojtos** — who leads Camunda's Getting Started experience, is a passionate UX advocate, and
whose idea this is. Camunda's own
web modeler already has the mechanism (the "test" function): `TestRunPanel.tsx` loads the wasm engine, deploys a diagram, starts an
instance, glows the active element through the diagram as the token advances, renders the running
variable payload, and lists waiting jobs. What it is **not** is:

1. **Reusable off-console.** The engine ships as a committed build artifact synced into
   `console/src/wasm` (`Makefile:170-176`); there is no versioned npm package an outside project can
   `npm install`. `engine-wasm/Cargo.toml` is `publish = false` and the generated
   `console/src/wasm/package.json` is an un-versioned committed blob.
2. **Autonomous.** `TestRunPanel` completes jobs **manually** — a human clicks *Complete*/*Fail*. The
   engine exposes `activateJobs(jobType, max, timeout, worker)`, but **nothing in the console calls it**:
   there is no in-browser worker **dispatch loop** (activate → run JS handler → `completeJob`/`failJob`).
   That loop is the framework's core net-new piece — it is what "execute it with workers in the browser"
   means.
3. **Packaged for a demo author.** A demo author wants to drop `<Bojtos bpmn workers seed autoplay/>`
   into a page — not wire up wasm init, a viewer, marker CSS, a trace model and a dispatch loop by hand.
4. **Interactively editable.** The teaching payload is the worker code: a viewer should see each worker in
   an editable box beside the diagram and watch an edit take effect in real time (the next job runs the new
   code). `TestRunPanel` has no editable-worker surface at all.

Two prior decisions from the design conversation are fixed and shape this ADR:

- **Reuse the engine + visualization substrate, don't fork it.** A second scenario runner alongside
  `TestRunPanel` would drift (the same failure mode this repo has hit before). The extracted core *is*
  the framework, and the console's own test panel becomes its first consumer.
- **The first release targets Next.js / React docs & marketing sites.** Demos live on documentation and
  marketing pages, which are overwhelmingly Next.js (App Router) + MDX. That target — not plain Vite,
  not a framework-agnostic web component — dictates the wasm-loading and SSR design of release 1.

## Decision (proposed)

Publish a small **layered** stack rather than one package, so a framework-agnostic core can outlive any
single UI binding:

| Package | Role | Depends on |
|---|---|---|
| **`@nanobpm/engine-wasm`** | The real `engine-core`, compiled `wasm-pack --target web`, as a **versioned, publishable** npm package. **The linchpin** — nothing external is possible until the engine is installable. | — |
| **`@nanobpm/bojtos-kit`** (headless, framework-agnostic) | Engine lifecycle, the **worker registry + dispatch loop** (net-new), the trace model, the bpmn-js viewer + active/incident markers, and the serializable `DemoScenario` type. | engine-wasm |
| **`@nanobpm/bojtos-react`** | The ergonomic "rapidly build pages" layer: `<Bojtos>`, `<BpmnRuntimeView>`, `<TraceTimeline>`, a player, and a `useBojtos()` hook. | @nanobpm/bojtos-kit |

Headless core + thin React binding means a later `@nanobpm/demo-vue` / web-component wrapper is additive,
not a rewrite.

### 1. `@nanobpm/engine-wasm` — promote the build artifact to a package

Relocate the `wasm-pack --target web` output out of `console/src/wasm` into a **top-level package
directory** with a real, versioned `@nanobpm/engine-wasm` `package.json` (name, version, `exports`,
`types`, `files`, `sideEffects`). The console stops carrying a committed engine blob and instead consumes
the package via the existing **`file:` internal-dependency** precedent (`@nanobpm/nano-app-schema` →
`spec-app`) until the package is actually published to the registry. `TestRunPanel.tsx:5`'s
`import init, { TestEngine } from "../wasm/nanobpmn_engine"` becomes
`from "@nanobpm/engine-wasm"`. The `Makefile` `console-wasm` target points `--out-dir` at the new package
directory. **This first step is behaviour-preserving and independently valuable** — it deletes the
committed-artifact sync and makes the engine installable, with no behaviour change to the console.

### 2. The public API surface, frozen small for release 1

```ts
// @nanobpm/bojtos-react
<Bojtos
  bpmn={string}                      // the diagram XML
  workers={Record<string, JobHandler>}   // jobType -> async (job) => variables
  seed?={Record<string, unknown>}    // initial process variables
  autoplay?={boolean}
  wasmUrl?={string}                  // escape hatch for the external-.wasm mode (§3)
  onTrace?={(t: TraceEvent) => void}
/>
useBojtos(scenario: DemoScenario) =>
  { engine, snapshot, play, pause, step, reset, trace }
```

Everything else — a Vue binding, a framework-agnostic web component, an `npm create @nanobpm/bojtos-app`
template — is explicitly **post-1.0**.

### 3. Next.js-first: the two hazards a *published* wasm package must design for on day one

- **Bundler wasm loading.** `wasm-pack --target web`'s default loader resolves the binary with
  `new URL('nanobpmn_engine_bg.wasm', import.meta.url)` + `fetch` — which works under Vite but bites
  under Next/webpack/Turbopack/Jest. The loader already accepts a `module_or_path` override
  (`nanobpmn_engine.js:537-558`). Release 1 ships **two modes**:
  - **Default — base64-inlined wasm**: the binary is embedded in the JS so there is **zero bundler
    config**; works in every Next setup, RSC, and Turbopack. Cost is ~1–2 MB in a *lazily-loaded* demo
    island, which is acceptable for a demo. This zero-config default is what makes the package "rapid and
    easy."
  - **Opt-in — external `.wasm`**: a `wasmUrl` prop + a documented `next.config.js` asset recipe, for
    size-sensitive consumers.
- **SSR / Next.js App Router.** `<Bojtos>` ships `'use client'` and touches `WebAssembly` / `window` /
  `fetch(import.meta.url)` **only inside effects**, never at module top-level, so Next's server pass
  never throws "WebAssembly is not defined". A ready-made `dynamic(() => …, { ssr: false })` boundary is
  documented for users who want an explicit no-SSR seam. This client-only behaviour is **baked into the
  component**, not left to the user.

### 4. The visual contract the package guarantees

Both demo affordances already exist in the substrate and are just **derived views over the engine's
`snapshot()` / `events()` stream** the `useBojtos()` hook exposes:

- **Token movement.** `snapshot()` returns `activeElementIds` + `incidentElementIds`; the viewer applies
  bpmn-js canvas markers (`nano-active` / `nano-incident`) **in place** — importing the XML once and
  updating markers without re-import, so zoom/scroll survive (`TestRunPanel.tsx:71-137`). The active
  element glows and moves through the diagram as execution advances. Highlight-based today; an animated
  token gliding along sequence flows is an **opt-in `<BpmnRuntimeView animateTokens>` post-1.0** flourish
  over the same `activeElementIds` stream.
- **Live variable payload.** `snapshot()` carries per-instance `variables`; `completeJob(key,
  variables_json)` **merges** a worker's output into the instance payload, so the JSON payload mutates in
  real time as in-browser workers run (`TestRunPanel.tsx:20,392`; `nanobpmn_engine.d.ts:23-27`). This is
  the payoff of the dispatch loop.

### 5. Editable worker code — live boxes with realtime effect

The demo's teaching payload is the **worker code**, so a demo author (and a demo *viewer*) must be able to
see each worker's code in an editable box next to the diagram and **watch the effect of an edit in real
time** — change a handler, and the very next job it services runs the new code, moving the token and
mutating the variable payload differently. This is the interactive heart of Bojtos, not a nicety.

Design:

- **Workers can be authored as source, not just as functions.** A `WorkerDef` is either a live
  `JobHandler` (function) or `{ code: string }` — a snippet whose body is `(job, ctx) => …`. Source-form
  workers keep `DemoScenario` **fully serializable** (see §6), so a demo — including its editable worker
  code — is data an MDX page or a shared link can carry.
- **Each source worker renders in a Monaco box.** The console already ships `monaco-editor`
  (`console/package.json`), so `@nanobpm/bojtos-react` reuses it for a `<WorkerEditor jobType>` per worker,
  with the JS/TS language services the console already configures.
- **The boxes get *typed* completion, for free, from the model.** Because the **data envelope** (ADR 0033
  §6 / ADR 0040) carries each service task's input/output type *reference in the model itself*
  (`io.nanobpm.dataEnvelope.in`/`.out` on the element → a domain type in the manifest `types` registry,
  read by `console/src/lib/dataEnvelope.ts`), Bojtos can resolve, for a given `jobType`, the shape of the
  `job.variables` a handler receives and the result it must return — and hand Monaco a generated
  `.d.ts` so the worker box offers **autocomplete on `job.variables.*`** and type-checks the returned
  payload. Homing the typing on the model rather than out-of-band (the ADR 0040 fusion) is what makes this
  possible: the type travels with the diagram the demo carries, so the completion is available wherever the
  scenario is embedded, with no extra wiring. This is the concrete win of the envelope-in-model decision.
- **Edits re-register the handler live.** `@nanobpm/bojtos-kit` compiles the edited source to a
  `JobHandler` and swaps it in the **worker registry** (the same registry the dispatch loop reads on each
  `activateJobs`). No engine restart, no re-deploy: the loop simply picks up the new handler on the next
  activation. Combined with `autoplay`/re-run, the edit-to-effect latency is one job cycle — "realtime."
- **A failed edit is a demo state, not a crash.** A compile error or a throwing handler surfaces as an
  inline editor diagnostic and, at runtime, as a job failure/incident the viewer already visualizes
  (§4) — so "break it and see what happens" is a first-class teaching move.

API additions (still small):

```ts
type WorkerDef = JobHandler | { code: string };   // code body: (job, ctx) => variables

<Bojtos
  workers={Record<string, WorkerDef>}   // function OR editable source
  editableWorkers?={boolean | string[]} // render code boxes for all / named jobTypes
  onWorkerEdit?={(jobType: string, code: string) => void}   // e.g. persist to a shareable scenario
/>
```

The **compile/execution model** is the one real design question this raises — see Open questions
(evaluating viewer-edited source safely in the browser).

### 6. `DemoScenario` — demos are data

A demo is a serializable descriptor, so "different demos for different use cases" is different data, not
different code — and MDX-driven docs can interleave narration between runtime views. Because workers may be
**source strings** (§5), the *editable worker code* is part of that data too, so a whole interactive demo
round-trips through a link or an MDX frontmatter block:

```ts
type DemoScenario = {
  bpmn: string;
  workers: Record<string, WorkerDef>;   // function or { code } (§5)
  seed?: Record<string, unknown>;
  narration?: NarrationStep[];   // optional guided steps
  autoplay?: boolean;
  editableWorkers?: boolean | string[];
};
```

### 7. Versioning is a release contract, not a repo sync

`@nanobpm/engine-wasm` pins an `engine-core` version and is published on its own semver; `@nanobpm/bojtos-kit`
depends on it via a semver range. A CI drift-guard rebuilds/republishes `engine-wasm` when `engine-core`
changes — hung off the existing `engine-wasm-check` + `engine-wasm-ffi (dist + verify)` jobs
(`Makefile:210-220`). This dissolves the current "re-sync the committed blob" concern.

### 8. Sequenced rollout — dogfooding is the acceptance test

1. **`@nanobpm/engine-wasm`** (§1) — relocate the wasm-pack out-dir to a publishable package; migrate the
   console to consume it via `file:`; delete the committed-artifact sync. Small, high-leverage,
   independently valuable, behaviour-preserving. **This ADR's first executable step.**
2. **Extract `@nanobpm/bojtos-kit` / `@nanobpm/bojtos-react`** from `TestRunPanel` (behaviour-preserving, green/green), and
   **repoint `TestRunPanel` at the public API**. If the modeler's own test panel can't be rebuilt cleanly
   on the package, the API isn't ready to publish — so the console rebuild **is** the acceptance test.
3. **Add the worker dispatch loop** to `@nanobpm/bojtos-kit` (activate → JS handler → complete/fail) and ship a
   first example `DemoScenario`.
4. **Add editable worker boxes** (§5): the Monaco `<WorkerEditor>`, source→handler compile, and live
   registry swap — the interactive edit-and-see-the-effect loop.
5. **Publish** `@nanobpm/bojtos-kit` + `@nanobpm/bojtos-react`; optional `npm create @nanobpm/bojtos-app` template later.

## Consequences

- The engine becomes independently installable and versioned; the console's committed `src/wasm` blob and
  its manual re-sync go away, replaced by a `file:` dependency (then a registry dependency once
  published).
- A second, drift-prone scenario runner is avoided by construction: the extracted core is the only
  runner, and the console is its first consumer, so the public API is continuously dogfooded.
- The base64-inline default trades ~1–2 MB of bundle size for zero-config portability across the Next.js
  ecosystem — the right trade for a lazily-loaded demo island, revisited if a size-sensitive consumer
  appears.
- A new publish surface (three npm packages) adds release plumbing and a semver contract between
  `engine-wasm` and `engine-core` that CI must guard.

## Open questions

- **Registry scope & publish cadence.** `@nanobpm` is currently used only for `file:` internal packages
  (`private: true`). Publishing `@nanobpm/engine-wasm` publicly needs an npm org/scope decision and a
  release workflow (tag-triggered, provenance).
- **wasm size budget.** Is ~1–2 MB inlined acceptable as the *default*, or should external-`.wasm` be the
  default with inline opt-in? Measure the `opt-level="z"` + `wasm-opt` binary first.
- **Worker execution model.** Do in-browser workers run on the main thread (simplest, fine for demos) or
  in a Web Worker (keeps a heavy handler from janking the page)? Release 1 can be main-thread with a
  Web-Worker option later.
- **Compiling viewer-edited worker source (§5).** Editable worker boxes mean *viewer*-supplied source is
  executed in the page. How is `{ code }` compiled to a `JobHandler` — a `Function`/`AsyncFunction`
  constructor (simple) vs an ES-module blob eval, and with what isolation (a sandboxed Web Worker with a
  narrow message API, so an edited handler can't touch the host page's DOM/network)? This is the security
  boundary of the editable-demo feature and should be decided before §5 ships.
- **Scenario provenance for docs.** Should `DemoScenario` support loading `bpmn` by URL/import for MDX
  ergonomics, or only inline strings? Affects the docs authoring story.
