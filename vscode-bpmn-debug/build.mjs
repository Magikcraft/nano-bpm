// Two esbuild bundles: the extension host (Node/CJS, `vscode` external) and the
// webview client (browser/IIFE, bpmn-js + diagram-js CSS bundled in). Run via
// `npm run build`.
import { build } from 'esbuild';

/** @type {import('esbuild').BuildOptions} */
const common = { bundle: true, sourcemap: true, logLevel: 'info', target: 'es2022' };

await build({
  ...common,
  entryPoints: ['src/extension.ts'],
  outfile: 'dist/extension.js',
  platform: 'node',
  format: 'cjs',
  external: ['vscode'],
});

await build({
  ...common,
  entryPoints: ['src/webview/main.ts'],
  outfile: 'dist/webview.js',
  platform: 'browser',
  format: 'iife',
  loader: { '.css': 'text' },
});

console.log('vscode-bpmn-debug: build complete');
