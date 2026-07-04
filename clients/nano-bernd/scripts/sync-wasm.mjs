// Copies the FFI wasm + manifest emitted by `make engine-wasm-ffi-dist` from the
// top-level `dist/engine-wasm-ffi/` into this package's `wasm/` directory so it
// ends up in the published tarball.
//
// Runs automatically before `npm run build` and `npm test`. If the source dist
// tree doesn't exist yet, this bails with a friendly message telling you to run
// `make engine-wasm-ffi-dist` at the repo root first — we deliberately do NOT
// invoke `make` from here to keep this script Node-only and toolchain-agnostic.

import { copyFileSync, mkdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const packageDir = join(here, '..');
const repoRoot = join(packageDir, '..', '..');
const distDir = join(repoRoot, 'dist', 'engine-wasm-ffi');
const wasmDir = join(packageDir, 'wasm');

const srcWasm = join(distDir, 'nano_engine.wasm');
const srcManifest = join(distDir, 'manifest.json');

try {
  statSync(srcWasm);
  statSync(srcManifest);
} catch {
  console.error(
    `sync-wasm: dist artifacts not found under ${distDir}\n` +
      `           run \`make engine-wasm-ffi-dist\` at the repo root first.`,
  );
  process.exit(1);
}

mkdirSync(wasmDir, { recursive: true });
copyFileSync(srcWasm, join(wasmDir, 'nano_engine.wasm'));
copyFileSync(srcManifest, join(wasmDir, 'manifest.json'));

const manifest = JSON.parse(readFileSync(join(wasmDir, 'manifest.json'), 'utf8'));
console.log(
  `sync-wasm: nano_engine.wasm (${manifest.size_bytes} bytes, ` +
    `abi v${manifest.abi_version}, engine ${manifest.engine_version}) synced into wasm/`,
);
