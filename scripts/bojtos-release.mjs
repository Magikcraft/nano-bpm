#!/usr/bin/env node
// Publish the @nanobpm/engine-wasm npm package.
//
// engine-wasm is the wasm-pack `--target web` build of the Rust in-browser
// engine (built via `make console-wasm`). The Bojtos framework packages that
// consume it — @nanobpm/bojtos-kit and @nanobpm/bojtos-react — now live in
// their own repo (nanobpm/bojtos) and are published from there; this script
// only publishes engine-wasm, whose source of truth lives in this repo.
//
// The same script drives both the maintainer's initial local publish (npm auth
// via `npm login` / an automation token, OTP prompted interactively) and the
// tag-triggered CI workflow (npm auth via OIDC trusted publishing — no token).
//
// Usage:
//   node scripts/bojtos-release.mjs [--dry-run] [--tag <dist-tag>] [-- <extra npm publish args>]
//
// Provenance is force-disabled: nano-bpm is a private repository, and npm
// provenance requires a public source repo (it would otherwise fail the
// OIDC publish, which defaults provenance on).

import { readFileSync, existsSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { resolve, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

const PACKAGE = {
  name: "@nanobpm/engine-wasm",
  dir: "engine-wasm/pkg",
  artifacts: ["lean/nanobpmn_engine_bg.wasm", "readmodel/nanobpmn_engine_bg.wasm"],
};

function parseArgs(argv) {
  const opts = { dryRun: false, tag: null, passthrough: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--dry-run") opts.dryRun = true;
    else if (a === "--tag") {
      opts.tag = argv[++i];
      if (opts.tag === undefined) {
        console.error("Missing value for --tag");
        process.exit(2);
      }
    }
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

function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) {
    console.log(
      "Usage: node scripts/bojtos-release.mjs [--dry-run] [--tag <dist-tag>] [-- <extra npm publish args>]",
    );
    return;
  }

  const version = readPkg(PACKAGE.dir).json.version;
  if (!version) {
    console.error(`No version in ${PACKAGE.dir}/package.json`);
    process.exit(1);
  }

  // Fail early if a build artifact is missing (engine-wasm ships the prebuilt
  // wasm; a forgotten `make console-wasm` would otherwise publish an empty one).
  // Both subpath variants — lean (`.`) and read-model (`/readmodel`) — must ship.
  for (const artifact of PACKAGE.artifacts) {
    if (!existsSync(join(root, PACKAGE.dir, artifact))) {
      console.error(
        `Missing ${PACKAGE.dir}/${artifact} — run \`make console-wasm\` before releasing.`,
      );
      process.exit(1);
    }
  }

  console.log(
    `Releasing ${PACKAGE.name} @ ${version}${opts.dryRun ? " (dry run)" : ""}` +
      `${opts.tag ? ` [dist-tag: ${opts.tag}]` : ""}\n`,
  );

  const cwd = join(root, PACKAGE.dir);
  const args = ["publish", "--access", "public"];
  if (opts.dryRun) args.push("--dry-run");
  if (opts.tag) args.push("--tag", opts.tag);
  args.push(...opts.passthrough);
  console.log(`=== ${PACKAGE.name} (${PACKAGE.dir}) ===`);
  execFileSync("npm", args, {
    cwd,
    stdio: "inherit",
    // Private repo: never attempt provenance (it would hard-fail the OIDC
    // publish, which defaults provenance on).
    env: { ...process.env, NPM_CONFIG_PROVENANCE: "false" },
  });

  console.log(
    `\n✓ ${opts.dryRun ? "Dry run complete" : `Published ${PACKAGE.name} @ ${version}`}.`,
  );
}

main();
