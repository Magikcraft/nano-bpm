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

// The console is served by the gateway under the `/console/` path prefix
// (see server/src/console/mod.rs), so the built asset URLs must be prefixed too.
export default defineConfig({
  base: "/console/",
  plugins: [react(), tailwindcss()],
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
