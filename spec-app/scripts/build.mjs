// Builds the browser-consumable package artifact from the library source.
//
// The library is authored as raw TypeScript (Node/Deno run it via type-stripping;
// see README). TypeScript/Vite consumers like the console, however, must not
// re-bundle the moddle parsers themselves — dmn-moddle is CJS (no ESM default
// export for Rollup) and its zeebe.json import carries `type: "json"` attributes
// that clash with bpmn-js importing the same file without them. So we ship a
// pre-built, self-contained artifact:
//
//   dist/index.js    — a single minified ESM bundle with every dependency inlined
//                      (esbuild resolves the CJS interop + JSON attributes once,
//                      here, isolating them from the consumer's bundler).
//   dist/index.d.ts  — a single flattened declaration file (dts-bundle-generator).
//
// dist/ is committed (like console/dist and gen/nano-app.d.ts) so consumers and
// CI (`npm ci`) need no build-on-install step. Regenerate with `npm run build`
// (or `make generate-app-manifest`).

import { build } from "esbuild";
import { generateDtsBundle } from "dts-bundle-generator";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { mkdirSync, writeFileSync } from "node:fs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const entry = join(root, "src", "index.ts");
const outDir = join(root, "dist");
mkdirSync(outDir, { recursive: true });

// 1) Runtime bundle: one ESM file, all deps inlined, browser-targeted.
await build({
  entryPoints: [entry],
  outfile: join(outDir, "index.js"),
  bundle: true,
  format: "esm",
  platform: "browser",
  target: "es2022",
  minify: true,
  legalComments: "none",
});
console.log("built dist/index.js");

// 2) Flattened declarations: one .d.ts with the public API, no external
//    relative imports (so consumers resolve types without seeing src/).
const [dts] = generateDtsBundle(
  [{ filePath: entry, output: { noBanner: true, exportReferencedTypes: false } }],
  { preferredConfigPath: join(root, "tsconfig.json") },
);
writeFileSync(join(outDir, "index.d.ts"), dts);
console.log("built dist/index.d.ts");
