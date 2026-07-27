# ADR 0042 — Bojtos: a publishable in-browser BPMN demo framework

Status: **Proposed.**
Date: 2026-07-28.
Relates to:
ADR 0005 (`0005-embedded-u-nano.md`, Bernd — the embedded engine this wraps), the in-browser
test-run substrate (`console/src/components/TestRunPanel.tsx` — loads the wasm `TestEngine`, deploys
BPMN, highlights active/incident elements on a bpmn-js viewer, renders the trace timeline;
`console/src/lib/simTrace.ts` — the trace fold), the wasm-bindgen wrapper (`engine-wasm/` around the
real `nanobpmn-engine-core`; `console/src/wasm/nanobpmn_engine.d.ts` — the `TestEngine` API contract),
the build seam (`Makefile:170-176` `console-wasm` = `wasm-pack build --target web` synced into
`console/src/wasm`), and the internal-package precedent (`spec-app/package.json` — `@nanobpm/*` consumed
by the console via a `file:` dependency; ADR 0027).

## Context

The concrete goal: publish something that lets **external developers** rapidly build in-browser BPMN
**demo pages** — show a BPMN model in the browser and *execute it with workers running in the browser* —
so different demos for different use cases are cheap to author. This framework is named **Bojtos**
(Hungarian, "tasseled" — a bundle of in-browser workers hanging off a running diagram). Camunda's own
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
| **`@bojtos/kit`** (headless, framework-agnostic) | Engine lifecycle, the **worker registry + dispatch loop** (net-new), the trace model, the bpmn-js viewer + active/incident markers, and the serializable `DemoScenario` type. | engine-wasm |
| **`@bojtos/react`** | The ergonomic "rapidly build pages" layer: `<Bojtos>`, `<BpmnRuntimeView>`, `<TraceTimeline>`, a player, and a `useBojtos()` hook. | @bojtos/kit |

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
// @bojtos/react
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

Everything else — a Vue binding, a framework-agnostic web component, an `npm create @bojtos/app`
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

### 5. `DemoScenario` — demos are data

A demo is a serializable descriptor, so "different demos for different use cases" is different data, not
different code — and MDX-driven docs can interleave narration between runtime views:

```ts
type DemoScenario = {
  bpmn: string;
  workers: Record<string, JobHandler>;
  seed?: Record<string, unknown>;
  narration?: NarrationStep[];   // optional guided steps
  autoplay?: boolean;
};
```

### 6. Versioning is a release contract, not a repo sync

`@nanobpm/engine-wasm` pins an `engine-core` version and is published on its own semver; `@bojtos/kit`
depends on it via a semver range. A CI drift-guard rebuilds/republishes `engine-wasm` when `engine-core`
changes — hung off the existing `engine-wasm-check` + `engine-wasm-ffi (dist + verify)` jobs
(`Makefile:210-220`). This dissolves the current "re-sync the committed blob" concern.

### 7. Sequenced rollout — dogfooding is the acceptance test

1. **`@nanobpm/engine-wasm`** (§1) — relocate the wasm-pack out-dir to a publishable package; migrate the
   console to consume it via `file:`; delete the committed-artifact sync. Small, high-leverage,
   independently valuable, behaviour-preserving. **This ADR's first executable step.**
2. **Extract `@bojtos/kit` / `@bojtos/react`** from `TestRunPanel` (behaviour-preserving, green/green), and
   **repoint `TestRunPanel` at the public API**. If the modeler's own test panel can't be rebuilt cleanly
   on the package, the API isn't ready to publish — so the console rebuild **is** the acceptance test.
3. **Add the worker dispatch loop** to `@bojtos/kit` (activate → JS handler → complete/fail) and ship a
   first example `DemoScenario`.
4. **Publish** `@bojtos/kit` + `@bojtos/react`; optional `npm create @bojtos/app` template later.

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
- **Scenario provenance for docs.** Should `DemoScenario` support loading `bpmn` by URL/import for MDX
  ergonomics, or only inline strings? Affects the docs authoring story.
