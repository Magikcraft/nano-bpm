// Builds the static site served at https://nanobpm.io — the canonical home for
// every schema/namespace the project publishes. The site is *generated from the
// in-repo sources of truth on every deploy* (no hand-maintained copies), so a
// published URL can never drift from the artifact it names:
//
//   * https://nanobpm.io/spec-app/nano-app.schema.json  <- spec-app/nano-app.schema.json
//   * https://nanobpm.io/schema/shapes/1.0              <- console/src/moddle/nanoShapes.ts
//
// The output path of each artifact is *derived from its own declared identity*
// (the schema's `$id`, the moddle descriptor's `uri`), and the build asserts
// that identity resolves to this domain — so a stray edit that points an `$id`
// somewhere else fails the build instead of shipping an unresolvable schema.
//
// Run: `node website/build.mjs` (Node >= 23.6 / 24 — imports the `.ts` moddle
// descriptor directly via built-in type stripping; matches the spec-app CI job).

import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  mkdirSync,
  writeFileSync,
  copyFileSync,
  cpSync,
  existsSync,
  rmSync,
  readFileSync,
} from "node:fs";
import { dirname, join } from "node:path";

const DOMAIN = "nanobpm.io";
const ORIGIN = `https://${DOMAIN}`;

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(here, "..");
const outDir = join(here, "_site");

/** Resolve a declared identity URI to its on-disk output path under `_site`. */
function outFor(uri, { indexHtml = false } = {}) {
  const u = new URL(uri);
  if (u.origin !== ORIGIN) {
    throw new Error(
      `published identity must live on ${ORIGIN}, got ${uri} (origin ${u.origin}). ` +
        `Migrate the artifact's declared identity onto the owned domain.`,
    );
  }
  const rel = indexHtml ? join(u.pathname, "index.html") : u.pathname;
  return join(outDir, rel);
}

function write(path, contents) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, contents);
  return path;
}

// --- Drift guard: no tracked file may reference the legacy `.dev` domain ------
// Built from parts so this guard never matches its own source.
const legacyNeedle = ["nanobpm", "dev"].join("\\.");
try {
  const hits = execFileSync(
    "git",
    ["grep", "-nI", "-E", legacyNeedle, "--", ".", ":!website/build.mjs"],
    { cwd: repoRoot, encoding: "utf8" },
  );
  if (hits.trim()) {
    throw new Error(
      `legacy schema domain still referenced in tracked sources — migrate to ${ORIGIN}:\n${hits}`,
    );
  }
} catch (e) {
  // `git grep` exits 1 (no matches) => clean. Any other failure is real.
  if (typeof e.status === "number" && e.status === 1) {
    /* clean */
  } else {
    throw e;
  }
}

rmSync(outDir, { recursive: true, force: true });
mkdirSync(outDir, { recursive: true });

const published = [];

// --- 1. Urban App manifest JSON Schema (spec-app/nano-app.schema.json) --------
{
  const src = join(repoRoot, "spec-app", "nano-app.schema.json");
  const schema = JSON.parse(readFileSync(src, "utf8"));
  if (!schema.$id) throw new Error(`${src} has no $id`);
  const dest = outFor(schema.$id); // asserts origin === nanobpm.io
  mkdirSync(dirname(dest), { recursive: true });
  copyFileSync(src, dest);
  published.push({
    title: schema.title || "AppManifest",
    url: schema.$id,
    kind: "JSON Schema (draft 2020-12)",
    desc:
      "The Urban App manifest (<code>nano.app.json</code>) — editors fetch this via the " +
      "<code>$schema</code> hint for autocompletion and validation.",
  });
}

// --- 2. Nano composed-shapes BPMN moddle namespace (nanoShapes.ts) ------------
{
  const mod = await import(
    join(repoRoot, "console", "src", "moddle", "nanoShapes.ts")
  );
  const descriptor = mod.nanoShapesModdle ?? mod.default;
  if (!descriptor?.uri) throw new Error("nanoShapes.ts exports no `uri`");
  const dest = outFor(descriptor.uri, { indexHtml: true }); // asserts origin
  write(dest, shapesNamespaceHtml(descriptor));
  published.push({
    title: `${descriptor.name} moddle namespace (${descriptor.prefix}:)`,
    url: descriptor.uri,
    kind: "BPMN moddle extension",
    desc:
      "The <code>nano:</code> extension elements (composed shapes, ADR 0040) that bpmn-js " +
      "serialises into <code>.bpmn</code>. This URI is the XML namespace identifier.",
  });
}

// --- Landing + Pages plumbing -------------------------------------------------
// The site root is the live in-browser demo (website/demo, an ADR 0043 Bojtos
// app) when it has been built; the generated schema listing then moves to
// /schemas/. When the demo dist is absent (the zero-dep `schemas` CI drift
// check, or a standalone `node website/build.mjs`), the schema listing is the
// root — so this build stays runnable without the demo's npm toolchain. The
// published schema/namespace URLs are unaffected either way (own paths).
const demoDist = join(here, "demo", "dist");
const demoBuilt = existsSync(join(demoDist, "index.html"));
if (demoBuilt) {
  cpSync(demoDist, outDir, { recursive: true });
  write(join(outDir, "schemas", "index.html"), landingHtml(published));
  console.log("embedded live demo at / (schema listing -> /schemas/)");
} else {
  write(join(outDir, "index.html"), landingHtml(published));
  console.log("demo dist not built -> schema listing at / (run demo build for the live landing)");
}
write(join(outDir, "CNAME"), `${DOMAIN}\n`);
write(join(outDir, ".nojekyll"), ""); // serve paths verbatim, skip Jekyll

console.log(`built _site for ${ORIGIN}:`);
for (const p of published) console.log(`  ${p.url}`);

// --- HTML helpers -------------------------------------------------------------
function esc(s) {
  return String(s).replace(
    /[&<>"]/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c],
  );
}

function page(title, body) {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${esc(title)}</title>
<style>
  :root { color-scheme: light dark; }
  body { font: 16px/1.6 system-ui, sans-serif; max-width: 46rem; margin: 3rem auto; padding: 0 1.25rem; }
  code, pre { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  code { background: color-mix(in srgb, currentColor 10%, transparent); padding: .1em .3em; border-radius: 4px; }
  pre { background: color-mix(in srgb, currentColor 8%, transparent); padding: 1rem; border-radius: 8px; overflow-x: auto; }
  h1 { font-size: 1.6rem; } h2 { font-size: 1.15rem; margin-top: 2rem; }
  a { color: inherit; } .muted { opacity: .7; }
  table { border-collapse: collapse; width: 100%; }
  td, th { text-align: left; padding: .35rem .6rem; border-bottom: 1px solid color-mix(in srgb, currentColor 15%, transparent); vertical-align: top; }
</style>
</head>
<body>
${body}
<hr>
<p class="muted">Served from <a href="/">nanobpm.io</a> · generated from source in
<a href="https://github.com/Magikcraft/nano-bpm">Magikcraft/nano-bpm</a>.</p>
</body>
</html>
`;
}

function landingHtml(items) {
  const rows = items
    .map(
      (i) => `<tr>
  <td><a href="${esc(i.url)}"><code>${esc(i.url)}</code></a><br>
      <span class="muted">${esc(i.kind)}</span></td>
  <td>${i.desc}</td>
</tr>`,
    )
    .join("\n");
  return page(
    "nanobpm.io — published schemas",
    `<h1>nanobpm.io</h1>
<p>The canonical home for the schemas and namespaces published by
<a href="https://github.com/Magikcraft/nano-bpm">nano-bpm</a> (Urban / ProcessOS).
Every URL below is generated from its in-repo source of truth on each deploy, so it
never drifts from the artifact it names.</p>
<h2>Published schemas &amp; namespaces</h2>
<table>
<tr><th>Identifier</th><th>Description</th></tr>
${rows}
</table>`,
  );
}

function shapesNamespaceHtml(d) {
  const types = (d.types || [])
    .map((t) => {
      const props = (t.properties || [])
        .map(
          (p) =>
            `<tr><td><code>${esc(p.name)}</code></td><td><code>${esc(
              p.type,
            )}</code>${p.isMany ? " []" : ""}${
              p.isAttr ? ' <span class="muted">(attr)</span>' : ""
            }</td></tr>`,
        )
        .join("");
      const sc = (t.superClass || []).join(", ");
      return `<h2><code>${esc(d.prefix)}:${esc(t.name)}</code></h2>
${sc ? `<p class="muted">extends ${esc(sc)}</p>` : ""}
${props ? `<table><tr><th>property</th><th>type</th></tr>${props}</table>` : "<p class='muted'>(no properties)</p>"}`;
    })
    .join("\n");
  return page(
    `${d.name} shapes namespace — ${d.uri}`,
    `<h1>${esc(d.name)} composed-shapes namespace</h1>
<p>XML namespace <code>${esc(d.uri)}</code> · prefix <code>${esc(d.prefix)}:</code></p>
<p>This is the BPMN <strong>moddle extension</strong> namespace for Nano's composed
shapes (ADR 0040). It is an XML namespace <em>identifier</em>: bpmn-js registers the
descriptor in code (it is not fetched at runtime), but this page documents the
elements a <code>.bpmn</code> may carry under this namespace.</p>
<p>Declare it on the root element:</p>
<pre>&lt;bpmn:definitions xmlns:${esc(d.prefix)}="${esc(d.uri)}" …&gt;</pre>
<h2 class="muted">Element types</h2>
${types}`,
  );
}
