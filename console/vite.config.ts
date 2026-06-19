import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

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
    // `npm run dev` serves the SPA on :5173 and proxies the console API to a
    // locally running gateway so the frontend can develop against live data.
    proxy: {
      "/console/api": {
        target: "http://localhost:8080",
        changeOrigin: true,
      },
    },
  },
});
