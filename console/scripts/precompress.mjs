// Build-time precompression (ADR 0034 follow-up).
//
// Walks a built `dist` tree and writes a `.br` (Brotli, quality 11) and `.gz`
// (gzip, level 9) sibling next to every compressible asset above a small size
// floor. The gateway embeds these siblings (rust-embed) and serves them
// directly when the client's `Accept-Encoding` allows, instead of gzip-ing each
// asset on every request (see server/src/console/mod.rs::serve_embedded).
//
// Two wins over the previous per-request gzip:
//   - Brotli-11 is ~15-20% smaller than gzip on JS/CSS and is computed once, at
//     build time, at max quality — a level far too slow to run per request.
//   - The gateway spends zero CPU compressing on the hot path; it just streams
//     the precompressed bytes.
//
// The raw asset is kept alongside the siblings so clients that accept no
// encoding (and `vite preview`) still work; the server falls back raw → runtime
// gzip → precompressed as appropriate. Siblings live under the git-ignored
// `dist`/`dist-observe` trees, so nothing new is committed.
//
// Usage: node scripts/precompress.mjs [outDir]   (default: dist)
import {
  brotliCompressSync,
  constants as zlibConstants,
  gzipSync,
} from "node:zlib";
import { readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { extname, join } from "node:path";

const outDir = join(process.cwd(), process.argv[2] ?? "dist");

// Only compress payloads that actually shrink. Already-compressed binaries
// (images, fonts, video) and our own output siblings are skipped.
const COMPRESSIBLE = new Set([
  ".js",
  ".mjs",
  ".css",
  ".html",
  ".json",
  ".svg",
  ".map",
  ".wasm",
  ".txt",
  ".xml",
  ".ico",
  ".webmanifest",
]);

// Below this the header/framing overhead of a separate encoding isn't worth the
// extra embedded bytes; the server serves these raw (and can still runtime-gzip
// on the rare occasion it matters).
const MIN_BYTES = 1024;

function* walk(dir) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const p = join(dir, entry.name);
    if (entry.isDirectory()) yield* walk(p);
    else yield p;
  }
}

let files = 0;
let rawTotal = 0;
let brTotal = 0;
let gzTotal = 0;

for (const file of walk(outDir)) {
  const ext = extname(file).toLowerCase();
  if (!COMPRESSIBLE.has(ext)) continue;
  if (file.endsWith(".br") || file.endsWith(".gz")) continue;

  const raw = readFileSync(file);
  if (raw.length < MIN_BYTES) continue;

  const br = brotliCompressSync(raw, {
    params: {
      [zlibConstants.BROTLI_PARAM_QUALITY]: 11,
      [zlibConstants.BROTLI_PARAM_SIZE_HINT]: raw.length,
    },
  });
  const gz = gzipSync(raw, { level: 9 });

  // Only keep a sibling that actually beats the raw payload.
  let kept = false;
  if (br.length < raw.length) {
    writeFileSync(`${file}.br`, br);
    brTotal += br.length;
    kept = true;
  }
  if (gz.length < raw.length) {
    writeFileSync(`${file}.gz`, gz);
    gzTotal += gz.length;
    kept = true;
  }
  if (kept) {
    files += 1;
    rawTotal += raw.length;
  }
}

const mb = (n) => (n / 1024 / 1024).toFixed(2);
console.log(
  `precompress: ${files} assets in ${outDir.replace(process.cwd() + "/", "")} — ` +
    `raw ${mb(rawTotal)}MB → br ${mb(brTotal)}MB / gz ${mb(gzTotal)}MB`,
);
