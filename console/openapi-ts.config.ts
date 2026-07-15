import { defineConfig } from "@hey-api/openapi-ts";

// Generates the typed console API client (types + per-operation SDK functions +
// fetch client) from the single source of truth: spec-console/console-api.yaml.
// Output lands in src/gen/ and is imported directly by the console SPA.
export default defineConfig({
  input: "../spec-console/console-api.yaml",
  output: {
    path: "src/gen",
  },
  plugins: [
    {
      name: "@hey-api/client-fetch",
      // The console is served same-origin under /console/api (the spec's
      // server URL), so no runtime baseUrl override is needed.
      runtimeConfigPath: "./src/lib/apiClientConfig.ts",
    },
    {
      name: "@hey-api/sdk",
      // Throw on non-2xx so callers can use try/catch like the old client.
      throwOnError: true,
    },
    "@hey-api/typescript",
  ],
});
