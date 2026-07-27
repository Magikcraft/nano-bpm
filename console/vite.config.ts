import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// The gateway the dev server proxies API calls to. Defaults to the local
// gateway `c8ctl` starts on :8080; override with NANO_BACKEND to point at a
// gateway running elsewhere (another port or host).
const backend = process.env.NANO_BACKEND ?? "http://127.0.0.1:8080";

// HMR dev-server port. Kept distinct from the gateway's :8080 so both run side
// by side; override with CONSOLE_PORT if 5173 is taken.
const port = Number(process.env.CONSOLE_PORT ?? 5173);

// The console ships two build profiles (ADR 0034): the full "studio" RAD IDE and
// the lean "observe" operator surface. The profile is chosen at build time via
// `VITE_CONSOLE_PROFILE` (default "studio"). We surface it to the app two ways:
//   - `import.meta.env.VITE_CONSOLE_PROFILE` — for ordinary runtime gates.
//   - a `define`d `__STUDIO__` boolean literal — for the `lazy(() => import())`
//     anchors. `define` is applied by esbuild during *transform*, so the guarded
//     `import()` folds to dead code before Rollup ever walks it. That matters: an
//     imported `IS_STUDIO` const only tree-shakes *after* transform, by which
//     point Vite's `?worker` plugin has already emitted Monaco's (6MB) worker
//     chunks as orphans. A `define`d literal drops the import target outright.
const profile =
  process.env.VITE_CONSOLE_PROFILE === "observe" ? "observe" : "studio";
const isStudio = profile === "studio";

// The console is served by the gateway under the `/console/` path prefix
// (see server/src/console/mod.rs), so the built asset URLs must be prefixed too.
export default defineConfig({
  base: "/console/",
  define: {
    __STUDIO__: JSON.stringify(isStudio),
  },
  plugins: [react(), tailwindcss()],
  // `@bpmn-io/form-js-editor` pins its own nested `preact` (10.15.x) while the
  // form *viewer* it renders through — plus `@bpmn-io/properties-panel` and
  // diagram-js — resolve the hoisted `preact` (10.29.x). Two preact copies means
  // two independent "current component" globals, so the editor's `useService`
  // hooks read an undefined render context and the whole form editor crashes
  // ("Cannot read properties of undefined (reading 'context')"), rendering no
  // palette or properties panel. Collapsing preact to a single instance fixes it
  // in both dev (optimizeDeps) and the production build (Rollup).
  resolve: {
    dedupe: ["preact"],
  },
  // `@nanobpm/engine-wasm` (the wasm-pack `--target web` output) resolves its
  // binary via `new URL('nanobpmn_engine_bg.wasm', import.meta.url)`. Excluding
  // it from esbuild's dependency pre-bundling keeps that asset reference intact
  // so Vite emits the `.wasm` as a hashed asset instead of esbuild rewriting the
  // `import.meta.url` and losing the binary. The Bojtos packages
  // (`@nanobpm/bojtos-kit` / `-react`) load the engine through that same loader,
  // so they are excluded too to keep the wasm asset reference intact end to end.
  optimizeDeps: {
    exclude: [
      "@nanobpm/engine-wasm",
      "@nanobpm/bojtos-kit",
      "@nanobpm/bojtos-react",
    ],
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
  server: {
    port,
    // `npm run dev` serves the SPA with hot-module reload and proxies the two
    // gateway surfaces the console talks to, so the frontend develops against a
    // live backend without rebuilding/embedding it into the binary:
    //   - `/console/api` — the console's own JSON API plus the SSE live-update
    //     stream (`/console/api/stream`) that drives auto-refresh.
    //   - `/v2`          — the public REST API: deployments, instance creation,
    //     and the BPMN definition XML the Process Explorer's diagram viewer
    //     fetches (`/v2/process-definitions/{key}/xml`).
    // Start a backing gateway first (e.g. with `c8ctl` on :8080), then run
    // `npm run dev` and open the printed http://localhost:<port>/console/ URL.
    proxy: {
      "/console/api": { target: backend, changeOrigin: true },
      "/v2": { target: backend, changeOrigin: true },
    },
  },
});
