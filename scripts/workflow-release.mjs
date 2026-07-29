#!/usr/bin/env node
// Publish @nanobpm/workflow to npm.
//
// Unlike scripts/bojtos-release.mjs, this package has NO internal `@nanobpm/*`
// file: dependencies — nothing to rewrite at publish time — so this is a plain
// `npm publish` in the workflow/ directory. `prepack` (in package.json) rebuilds
// dist/ from source, so the tarball is always built from the tagged tree.
//
// Auth is npm OIDC trusted publishing (NO NPM_TOKEN); provenance is disabled
// (NPM_CONFIG_PROVENANCE=false) because nano-bpm is private and provenance
// requires a public source repo. See docs/releasing-workflow-npm.md.

import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const pkgDir = join(repoRoot, "workflow");

function run(cmd, args, cwd) {
  console.log(`$ ${cmd} ${args.join(" ")}  (cwd=${cwd})`);
  execFileSync(cmd, args, { cwd, stdio: "inherit" });
}

run("npm", ["publish"], pkgDir);
console.log("published @nanobpm/workflow");
