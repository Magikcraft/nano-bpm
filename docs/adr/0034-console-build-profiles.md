# ADR 0034 — Console build profiles: a lean "observe" surface vs the full "studio" IDE

Status: **Accepted** (phase-1 spike).
Date: 2026-07-23.
Relates to:
ADR 0022 (`0022-nano-rad-application.md`, **Urban** — the console *is* the RAD IDE; this ADR splits the
IDE off from the operator surface so you can ship one without the other),
ADR 0033 (`0033-urban-element-templates-first-class-components.md`, the palette/modeler that makes the
studio bundle heavy),
`console/src/lib/profile.ts` (the profile constant),
`console/vite.config.ts` (the `__STUDIO__` `define` that drives dead-code elimination),
`console/src/App.tsx` + `console/src/views/Workers.tsx` (the gated `lazy()` anchors),
`server/src/console/mod.rs` + `server/Cargo.toml` (the `console-observe` Cargo feature that swaps the
embedded bundle).

## Context

The console has grown two audiences with very different weight:

- **Operators** want to *watch* a running system: topology, metrics, traces, the instance explorer,
  worker health. This is Camunda's *Operate* role.
- **Makers** want to *build*: the BPMN/DMN/form modelers, the Monaco code editors, the pack
  marketplace. This is Camunda's *Modeler* / Desktop-Modeler role, and it is what ADR 0022 (Urban) and
  ADR 0033 (element-template palette) are about.

The maker surface is expensive. A measured production build of `console/dist` is **~18.5MB raw /
~4.7MB gzip across 105 JS chunks**. Almost all of it is IDE: Monaco's `ts.worker` (6.0MB / 1.36MB gz),
the TypeScript language service (3.6MB / 1.03MB gz), `ProjectWorkspace` (0.96MB gz), the `CodeEditor`
(0.86MB gz), plus the bpmn/dmn/form modeler bundles. The operator shell proper — topology, metrics,
traces, the instance explorer with a read-only bpmn *viewer* — is a small fraction.

Runtime cost is already contained: every route is `lazy()` code-split, minified (esbuild), and gzipped
on the wire (`serve_embedded`), so an operator who never opens the modeler never *downloads* those
chunks. But **ship-time** cost is not: the gateway binary embeds *all* of `console/dist` via
`rust-embed` regardless of who will use it. Someone who wants observability-only — a monitoring
sidecar, a locked-down production node, a slim container — still carries ~18MB of IDE bytes in the
binary they distribute.

We want to be able to ship the operator surface without the IDE.

## Decision

Ship the console as **two build profiles from one source tree**, chosen at build time:

- **`studio`** — the full RAD IDE (default; `npm run dev` and `npm run build` are unchanged).
- **`observe`** — the operator surface only. No modeler, no Monaco, no marketplace.

The split is enforced at two layers.

### 1. Frontend — compile-time dead-code elimination

`console/vite.config.ts` reads `VITE_CONSOLE_PROFILE` (default `studio`) and `define`s a boolean
literal `__STUDIO__`. Studio-only route/component **`lazy(() => import(...))` anchors** guard on that
literal:

```ts
const ProjectWorkspace = __STUDIO__ ? lazy(() => import("./views/ProjectWorkspace")) : null;
```

Because `define` is applied by esbuild during *transform*, `__STUDIO__` folds to `false` in an observe
build and the `import()` becomes dead code **before Rollup ever walks it**. The studio-only views
(`Projects`, `ProjectWorkspace`, `Extensions`) and the Monaco `CodeEditor` inside `Workers`
disappear, and with them the whole IDE dependency graph.

**Why `define`, not an imported `IS_STUDIO` const.** The obvious approach — export
`const IS_STUDIO = ...` from `profile.ts` and guard on it — does *not* drop the heavy chunks. An
imported binding only tree-shakes *after* transform, and by that point Vite's `?worker` plugin has
already emitted Monaco's worker bundles (the 6MB `ts.worker` chief among them) as **orphan chunks**
that survive tree-shaking. Only a `define`d literal, inlined at the `import()` site during transform,
prevents the module from entering the graph at all. `IS_STUDIO` still exists (sourced from the same
literal) for ordinary runtime gates — nav filtering, effects, the Workers default tab — but the
`import()` anchors must use the raw `__STUDIO__` literal. This distinction is the load-bearing part of
the spike.

Operator views (`Topology`, `Metrics`, `Traces`, `Explorer`, `InstanceDetail`, `Workers`, `Config`,
`Credits`) are unconditional in both profiles. `InstanceDetail`/`Traces` keep the read-only bpmn
*viewer* (~63KB gz) — cheap, and diagram context is genuinely operator-relevant. `Workers` straddles
the two: its "running" (health/metrics) tab is operator; its "editor" tab (Monaco worker authoring) is
maker-only and hidden in observe.

Build the lean bundle with `npm run build:observe` → `console/dist-observe`.

### 2. Server — a `console-observe` Cargo feature

`server/Cargo.toml` gains `console-observe = ["console"]`. `server/src/console/mod.rs` selects which
folder `rust-embed` bakes in via `cfg`:

```rust
#[cfg(not(feature = "console-observe"))]
#[derive(RustEmbed)] #[folder = "../console/dist"]         struct Assets;
#[cfg(feature = "console-observe")]
#[derive(RustEmbed)] #[folder = "../console/dist-observe"] struct Assets;
```

Everything downstream (`serve_embedded`, gzip, SPA fallback) is unchanged — only the embedded byte set
differs.

## Consequences

**The win.** The observe bundle is **517KB raw / 158KB gzip across 8 chunks** — a **~97% reduction**
from studio's 4.7MB gzip. `ts.worker`, `typescript`, `CodeEditor`, `ProjectWorkspace`, and the dmn
modeler are all absent; only the operator shell + read-only bpmn viewer remain.

**Default is unchanged.** `studio` is the default at every layer (Vite env default, Cargo feature
default), so `npm run dev`, `npm run build`, and the normal gateway build behave exactly as before.
The lean path is strictly opt-in.

**One source tree, two artifacts.** No fork, no separate app. The profiles differ only by a build flag,
so features stay in sync and the operator build can never drift from the IDE build's operator views.

**CI note.** Repo CI builds the full `console` feature (and stubs `console/dist`). The `console-observe`
feature requires `console/dist-observe` to exist at compile time (`npm run build:observe`), so it is
verified locally / in the release matrix rather than in the default CI leg.

## Follow-ups (not in this spike)

- **Feature-gate the authoring API.** The `console` feature still compiles the project create/run/save
  endpoints even in an observe binary. They are tiny (Rust) and harmless, but a hardened observe build
  should not expose maker mutation endpoints — split them behind the studio side of the feature.
- **Build-time precompression + brotli.** ✅ Done (follow-up PR). `console/scripts/precompress.mjs`
  writes a Brotli-11 `.br` and a gzip-9 `.gz` sibling next to every compressible built asset;
  `serve_embedded` streams the sibling matching the client's `Accept-Encoding` (brotli preferred), so
  there is no per-request compression CPU on the hot path and ~15-20% fewer bytes on the wire than the
  old runtime gzip. Runtime gzip is kept as a fallback for the CI stub bundle and any hand-built `dist`
  without siblings. Tradeoff: the binary now embeds raw + `.br` + `.gz` (studio: ~21MB raw, +4MB br,
  +5MB gz). Reclaiming that by embedding only the compressed variants — decompressing for the rare
  client that accepts no encoding — is a future lever.
- **Observe UX polish.** The Workers view hides its editor tab in observe; a couple of other maker
  affordances (e.g. "new project" entry points) could be softened for the operator surface.
- **Distribution.** Decide how the two profiles surface to users — a `--profile` on the launcher, two
  published binaries, or a container variant.
