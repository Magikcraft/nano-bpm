#!/usr/bin/env node
// Publish the three Bojtos npm packages in dependency order:
//
//   @nanobpm/engine-wasm  →  @nanobpm/bojtos-kit  →  @nanobpm/bojtos-react
//
// In the repo the inter-package deps are local `file:` links (so in-tree
// builds, the console and the website demo resolve them without a registry).
// npm can't publish `file:` deps, so this script rewrites each internal
// `@nanobpm/*` dependency to `^<version>` at publish time and restores the
// original package.json afterwards. The committed sources keep their `file:`
// links untouched.
//
// The same script drives both the maintainer's initial local publish (npm auth
// via `npm login` / an automation token, OTP prompted interactively) and the
// tag-triggered CI workflow (npm auth via OIDC trusted publishing — no token).
//
// Usage:
//   node scripts/bojtos-release.mjs [--dry-run] [--tag <dist-tag>] [-- <extra npm publish args>]
//
// The version is taken from @nanobpm/engine-wasm's package.json; the other two
// packages must already declare the same version (bump all three together) or
// the script aborts.
//
// Provenance is force-disabled: nano-bpm is a private repository, and npm
// provenance requires a public source repo (it would otherwise fail the
// OIDC publish, which defaults provenance on).

import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { resolve, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

// Publish order matters: a package must be published before anything that
// pins it, so consumers can always resolve the pinned version.
const PACKAGES = [
  { name: "@nanobpm/engine-wasm", dir: "engine-wasm/pkg", artifact: "nanobpmn_engine_bg.wasm" },
  { name: "@nanobpm/bojtos-kit", dir: "bojtos-kit", artifact: "dist/index.js" },
  { name: "@nanobpm/bojtos-react", dir: "bojtos-react", artifact: "dist/index.js" },
];
const INTERNAL = new Set(PACKAGES.map((p) => p.name));

function parseArgs(argv) {
  const opts = { dryRun: false, tag: null, passthrough: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--dry-run") opts.dryRun = true;
    else if (a === "--tag") opts.tag = argv[++i];
    else if (a === "--") opts.passthrough = argv.slice(i + 1);
    else if (a === "--help" || a === "-h") opts.help = true;
    else {
      console.error(`Unknown argument: ${a}`);
      process.exit(2);
    }
    if (a === "--") break;
  }
  return opts;
}

function readPkg(dir) {
  const file = join(root, dir, "package.json");
  const text = readFileSync(file, "utf8");
  return { file, text, json: JSON.parse(text) };
}

/** Rewrite internal `@nanobpm/*` `file:` deps to `^version`. Returns the
 *  original file text so the caller can restore it. */
function pinInternalDeps(pkg, version) {
  const { file, text, json } = readPkg(pkg.dir);
  let changed = false;
  for (const field of ["dependencies", "peerDependencies", "optionalDependencies"]) {
    const deps = json[field];
    if (!deps) continue;
    for (const [name, spec] of Object.entries(deps)) {
      if (INTERNAL.has(name) && String(spec).startsWith("file:")) {
        deps[name] = `^${version}`;
        changed = true;
      }
    }
  }
  if (changed) {
    // Preserve trailing newline convention.
    const nl = text.endsWith("\n") ? "\n" : "";
    writeFileSync(file, JSON.stringify(json, null, 2) + nl);
  }
  return text;
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) {
    console.log(
      "Usage: node scripts/bojtos-release.mjs [--dry-run] [--tag <dist-tag>] [-- <extra npm publish args>]",
    );
    return;
  }

  // The engine-wasm package is the version anchor for the release train.
  const anchor = readPkg(PACKAGES[0].dir).json;
  const version = anchor.version;
  if (!version) {
    console.error(`No version in ${PACKAGES[0].dir}/package.json`);
    process.exit(1);
  }

  // All three must agree; publishing mixed versions would leave the pinned
  // `^version` deps pointing at something that doesn't exist.
  const mismatched = PACKAGES.filter((p) => readPkg(p.dir).json.version !== version);
  if (mismatched.length) {
    console.error(
      `Version mismatch — expected ${version} everywhere, but:\n` +
        mismatched.map((p) => `  ${p.name}: ${readPkg(p.dir).json.version}`).join("\n") +
        `\nBump all three packages to the same version before releasing.`,
    );
    process.exit(1);
  }

  // Fail early if a build artifact is missing (the packages ship prebuilt
  // dist/wasm; a forgotten `make bojtos` would otherwise publish an empty one).
  for (const p of PACKAGES) {
    if (!existsSync(join(root, p.dir, p.artifact))) {
      console.error(
        `Missing ${p.dir}/${p.artifact} — run \`make bojtos\` (builds the wasm + both dists) before releasing.`,
      );
      process.exit(1);
    }
  }

  console.log(
    `Releasing Bojtos packages @ ${version}${opts.dryRun ? " (dry run)" : ""}` +
      `${opts.tag ? ` [dist-tag: ${opts.tag}]` : ""}\n`,
  );

  for (const p of PACKAGES) {
    const cwd = join(root, p.dir);
    const original = pinInternalDeps(p, version);
    try {
      const args = ["publish", "--access", "public"];
      if (opts.dryRun) args.push("--dry-run");
      if (opts.tag) args.push("--tag", opts.tag);
      args.push(...opts.passthrough);
      console.log(`\n=== ${p.name} (${p.dir}) ===`);
      execFileSync("npm", args, {
        cwd,
        stdio: "inherit",
        // Private repo: never attempt provenance (it would hard-fail the OIDC
        // publish, which defaults provenance on).
        env: { ...process.env, NPM_CONFIG_PROVENANCE: "false" },
      });
    } finally {
      // Always restore the committed `file:` links, even on failure.
      writeFileSync(readPkg(p.dir).file, original);
    }
  }

  console.log(
    `\n✓ ${opts.dryRun ? "Dry run complete" : `Published all Bojtos packages @ ${version}`}.`,
  );
}

main();
