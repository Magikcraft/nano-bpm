// Generate the TypeScript AppManifest types from nano-app.schema.json
// (ADR 0027 §3, spec-first). The manifest is consumed only by TypeScript —
// the console App panels and the Deno App loader — so this emits TS, not Rust
// (ADR 0027 §1: nano.app.json is read by the compiled App + console panels,
// not the Rust server).
//
// Run:  npm run gen              # write gen/nano-app.d.ts
//       npm run gen -- --check   # fail if the committed output is stale
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { compileFromFile } from "json-schema-to-typescript";

const here = dirname(fileURLToPath(import.meta.url));
const specDir = join(here, "..");
const schemaPath = join(specDir, "nano-app.schema.json");
const outDir = join(specDir, "gen");
const outPath = join(outDir, "nano-app.d.ts");

const banner = `/**
 * GENERATED — do not edit by hand.
 *
 * TypeScript types for the Urban App manifest (nano.app.json), generated from
 * spec-app/nano-app.schema.json (ADR 0027). Regenerate with:  npm run gen
 * (from spec-app/) or  make generate-app-manifest  (from the repo root).
 */
`;

const ts =
  banner +
  (await compileFromFile(schemaPath, {
    bannerComment: "",
    additionalProperties: false,
    style: { singleQuote: false },
  }));

const check = process.argv.includes("--check");
if (check) {
  let current = "";
  try {
    current = readFileSync(outPath, "utf8");
  } catch {
    /* missing counts as stale */
  }
  if (current !== ts) {
    console.error(
      "gen/nano-app.d.ts is stale — run `npm run gen` (or `make generate-app-manifest`) and commit the result.",
    );
    process.exit(1);
  }
  console.log("gen/nano-app.d.ts is up to date.");
} else {
  mkdirSync(outDir, { recursive: true });
  writeFileSync(outPath, ts);
  console.log(`Wrote ${outPath.slice(specDir.length + 1)}`);
}
