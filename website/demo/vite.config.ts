import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The nanobpm.io in-browser demo. Served at `/demo/` (the marketing landing
// page owns `/`), so `base` is "/demo/" and emitted asset URLs are `/demo/...`.
//
// The wasm engine (`@nanobpm/engine-wasm`, wasm-pack `--target web`) resolves
// its binary via `new URL('nanobpmn_engine_bg.wasm', import.meta.url)`. We keep
// that reference intact — excluding it from esbuild's dep pre-bundling — so Vite
// emits the `.wasm` as a hashed asset instead of esbuild rewriting the
// `import.meta.url` and losing the binary. Mirrors the console's vite config.
export default defineConfig({
  base: "/demo/",
  plugins: [react()],
  resolve: {
    // The `@nanobpm/*` file: deps re-export each other transitively
    // (bojtos-react → bojtos-kit → engine-wasm) and are symlinked here with no
    // per-package node_modules, so resolve via the symlink path rooted here.
    preserveSymlinks: true,
    dedupe: ["react", "react-dom"],
  },
  optimizeDeps: {
    exclude: [
      "@nanobpm/engine-wasm",
      "@nanobpm/bojtos-kit",
      "@nanobpm/bojtos-react",
    ],
  },
});
