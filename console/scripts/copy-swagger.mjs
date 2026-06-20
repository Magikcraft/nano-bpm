// Copies the pinned swagger-ui-dist assets into public/swagger/ and bundles the
// multi-file OpenAPI spec (../spec) into a single self-contained openapi.json,
// all at build time so Vite ships them in dist/ (and the gateway embeds them)
// for a fully offline Swagger UI. These generated files are git-ignored;
// swagger-ui-dist (a pinned devDependency) and ../spec are the sources of truth.
// Only index.html under public/swagger is committed.
import { copyFileSync, mkdirSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import SwaggerParser from "@apidevtools/swagger-parser";

const require = createRequire(import.meta.url);
const distDir = dirname(require.resolve("swagger-ui-dist/swagger-ui.css"));
const outDir = join(process.cwd(), "public", "swagger");

mkdirSync(outDir, { recursive: true });
for (const file of ["swagger-ui.css", "swagger-ui-bundle.js"]) {
  copyFileSync(join(distDir, file), join(outDir, file));
  console.log(`copied ${file} -> public/swagger/`);
}

// Bundle the spec into one document so every $ref is internal. Swagger UI's
// client-side resolver mis-resolves `#/` pointers inside transitively-included
// files against the root document; bundling lifts all schemas into a single
// `#/components` section and rewrites refs, which resolves cleanly offline.
const specEntry = join(process.cwd(), "..", "spec", "rest-api.yaml");
const bundled = await SwaggerParser.bundle(specEntry);
writeFileSync(join(outDir, "openapi.json"), JSON.stringify(bundled));
const pathCount = Object.keys(bundled.paths ?? {}).length;
console.log(`bundled spec -> public/swagger/openapi.json (${pathCount} paths)`);

