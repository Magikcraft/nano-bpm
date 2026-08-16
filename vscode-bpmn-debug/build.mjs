// Two esbuild bundles: the extension host (Node/CJS, `vscode` external) and the
// webview client (browser/IIFE, bpmn-js + diagram-js CSS bundled in). Run via
// `npm run build`.
import { build } from 'esbuild';
import { copyFile, mkdir } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);
const here = dirname(fileURLToPath(import.meta.url));
const dist = join(here, 'dist');
const wasmFile = 'nanobpmn_engine_bg.wasm';

/** @type {import('esbuild').BuildOptions} */
const common = { bundle: true, sourcemap: true, logLevel: 'info', target: 'es2022' };

await build({
  ...common,
  entryPoints: ['src/extension.ts'],
  outfile: 'dist/extension.js',
  platform: 'node',
  // Keep CommonJS for the VS Code extension host. The bundled adapter resolves
  // wasm next to this file via __dirname, so the ESM import.meta fallback remains
  // only for unbundled adapter builds/tests; defining import.meta.url avoids an
  // esbuild CJS warning in wasm-pack's unused async init fallback.
  format: 'cjs',
  define: { 'import.meta.url': 'undefined' },
  external: ['vscode'],
});

await mkdir(dist, { recursive: true });
await copyFile(resolveEngineWasm(), join(dist, wasmFile));

await build({
  ...common,
  entryPoints: ['src/webview/main.ts'],
  outfile: 'dist/webview.js',
  platform: 'browser',
  format: 'iife',
  loader: { '.css': 'text' },
});

console.log('vscode-bpmn-debug: build complete');

function resolveEngineWasm() {
  try {
    return require.resolve(`@nanobpm/engine-wasm/${wasmFile}`);
  } catch {
    return join(here, '..', 'engine-wasm', 'pkg', wasmFile);
  }
}
