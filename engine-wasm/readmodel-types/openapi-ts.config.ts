import { defineConfig } from "@hey-api/openapi-ts";

// Generates `types.gen.ts` — the derived TypeScript types for the engine's
// read-model query results — from the single source of truth: the Camunda-parity
// REST OpenAPI in `spec/`. The wasm read-model methods (`searchUserTasks`, … in
// engine-wasm/src/lib.rs) hand-build these shapes as `serde_json::Value` to
// mirror Camunda 8 REST responses; typing browser consumers against a hand-copy
// would be a drift surface, so we derive them here. The curated `index.d.ts`
// barrel selects the read-model result types from the generated file, and the
// Makefile (`make console-wasm`) copies both into `engine-wasm/pkg/readmodel-types`.
//
// `output.clean` is false: we emit into this package dir alongside the authored
// package.json / config / barrel, and cleaning would delete them.
export default defineConfig({
  input: "../../spec/rest-api.yaml",
  output: { path: ".", clean: false, indexFile: false },
  plugins: ["@hey-api/typescript"],
});
