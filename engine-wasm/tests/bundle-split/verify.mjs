// Bundle-split assertion for @nanobpm/engine-wasm's two subpath entrypoints.
//
// Each variant is its own wasm-pack `--target web` JS glue + `_bg.wasm`, in its
// own directory (pkg/lean, pkg/readmodel). The glue loads its sibling wasm via
// `new URL("./nanobpmn_engine_bg.wasm", import.meta.url)` — a *relative* URL — so
// whichever glue directory a bundle's static module graph reaches is the only
// `_bg.wasm` that ships. wasm can't be tree-shaken out of a single fat build, so
// importing only `.` can never pull the readmodel binary, and vice versa.
//
// This test bundles each entrypoint with esbuild and asserts, from the metafile,
// that the module graph reaches exactly one variant's glue directory.
import { build } from "esbuild";
import { fileURLToPath } from "node:url";
import path from "node:path";

const here = path.dirname(fileURLToPath(import.meta.url));

async function glueInputsFor(entry) {
  const result = await build({
    entryPoints: [path.join(here, entry)],
    bundle: true,
    format: "esm",
    write: false,
    metafile: true,
    outdir: path.join(here, ".out"),
    loader: { ".wasm": "file" },
  });
  return Object.keys(result.metafile.inputs)
    .map((p) => p.replace(/\\/g, "/"))
    .filter((p) => /nanobpmn_engine\.js$/.test(p));
}

let failed = false;
function assert(cond, msg) {
  if (cond) {
    console.log(`\u2713 ${msg}`);
  } else {
    failed = true;
    console.error(`\u2717 ${msg}`);
  }
}

const lean = await glueInputsFor("lean-app.mjs");
const readmodel = await glueInputsFor("readmodel-app.mjs");

console.log("lean entry (`.`) glue inputs:          ", lean);
console.log("readmodel entry (`/readmodel`) glue in: ", readmodel);

const reachesLean = (l) => l.some((p) => /(^|\/)lean\/nanobpmn_engine\.js$/.test(p));
const reachesReadmodel = (l) => l.some((p) => /(^|\/)readmodel\/nanobpmn_engine\.js$/.test(p));

assert(reachesLean(lean), "lean entry (`.`) resolves to the lean/ glue (⇒ ships lean _bg.wasm only)");
assert(!reachesReadmodel(lean), "lean entry (`.`) never reaches the readmodel/ glue (readmodel _bg.wasm absent)");
assert(reachesReadmodel(readmodel), "readmodel entry (`/readmodel`) resolves to the readmodel/ glue (⇒ ships readmodel _bg.wasm)");
assert(!reachesLean(readmodel), "readmodel entry (`/readmodel`) never reaches the lean/ glue (lean _bg.wasm absent)");

if (failed) {
  console.error("\nBundle-split assertion FAILED");
  process.exit(1);
}
console.log("\nBundle-split verified: each subpath's module graph reaches exactly one variant directory,");
console.log("so a bundler emits only that variant's _bg.wasm.");
