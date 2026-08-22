// Use-after-free DX guard for @nanobpm/engine-wasm (both subpath variants).
//
// wasm-bindgen refuses a call on a freed handle by throwing the opaque runtime
// message "null pointer passed to rust". `scripts/inject-free-guard.mjs` rewrites
// that into a self-describing "TestEngine used after free()" so a host lifecycle
// bug (e.g. a fire-and-forget worker handler that outlives engine teardown) is
// diagnosable at the call site. This test freezes that behaviour for the lean
// (`.`) and read-model (`/readmodel`) entrypoints: free an engine, then call a
// method, and assert the friendly message (and NOT the raw wasm-bindgen one).
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

let failed = false;
function assert(cond, msg) {
  if (cond) {
    console.log(`\u2713 ${msg}`);
  } else {
    failed = true;
    console.error(`\u2717 ${msg}`);
  }
}

async function checkVariant(label, glueSpecifier, wasmSpecifier) {
  const { initSync, TestEngine } = await import(glueSpecifier);
  initSync({ module: readFileSync(require.resolve(wasmSpecifier)) });

  const engine = new TestEngine();
  engine.free();

  let threw;
  try {
    engine.snapshot();
  } catch (err) {
    threw = err;
  }

  assert(threw !== undefined, `${label}: calling a method after free() throws`);
  const message = threw?.message ?? "";
  assert(
    /used after free\(\)/.test(message),
    `${label}: message is the friendly "used after free()" (got: ${JSON.stringify(message)})`,
  );
  assert(
    !/null pointer passed to rust/.test(message),
    `${label}: message is NOT the opaque "null pointer passed to rust"`,
  );
}

await checkVariant(
  "lean (.)",
  "@nanobpm/engine-wasm",
  "@nanobpm/engine-wasm/lean/nanobpmn_engine_bg.wasm",
);
await checkVariant(
  "readmodel (/readmodel)",
  "@nanobpm/engine-wasm/readmodel",
  "@nanobpm/engine-wasm/readmodel/nanobpmn_engine_bg.wasm",
);

if (failed) {
  console.error("\nUse-after-free DX assertion FAILED");
  process.exit(1);
}
console.log("\nUse-after-free DX assertion passed");
