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
// The marketing landing page owns the site root (`/`) and is always generated
// (pure static HTML, no toolchain) so this build stays runnable without the
// demo's npm build — e.g. the zero-dep `schemas` CI drift check. The generated
// schema registry lives at `/schemas/`, and the ADR 0043 Bojtos in-browser demo,
// when its dist has been built, is served at `/demo/` (its Vite `base`). The
// published schema/namespace URLs are unaffected (they own their own paths).
write(join(outDir, "index.html"), homeHtml());
write(join(outDir, "schemas", "index.html"), schemasHtml(published));

const demoDist = join(here, "demo", "dist");
if (existsSync(join(demoDist, "index.html"))) {
  cpSync(demoDist, join(outDir, "demo"), { recursive: true });
  console.log("landing at / · demo at /demo/ · schema registry at /schemas/");
} else {
  console.log(
    "landing at / · schema registry at /schemas/ · (demo dist not built — /demo/ omitted; run the demo build for the live try-it page)",
  );
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

function homeHtml() {
  // Defined as a quote-safe line array (no backticks / ${…}) so it can't collide
  // with this module's own template literals; HTML-escaped at the interpolation.
  const heroCode = [
    'import { defineWorkflow } from "@nanobpm/workflow";',
    "",
    'export const prReview = defineWorkflow("pr-review", (w) => {',
    '  w.run("fetchDiff",  async (job) => ({ diff: await gh.diff(job.variables.prId) }));',
    '  w.run("autoReview", async (job) => ({ findings: await llm.review(job.variables.diff) }));',
    '  w.signal("humanApproval", { correlationKey: "prId" }); // durable wait — survives a reboot',
    '  w.run("merge",      async (job) => ({ merged: await gh.merge(job.variables.prId) }));',
    "});",
  ].join("\n");

  const GH = "https://github.com/Magikcraft/nano-bpm";

  const body = `<header class="nav">
  <a class="brand" href="/">nanobpm<span class="dim">.io</span></a>
  <nav>
    <a href="/demo/">Demo</a>
    <a href="/schemas/">Schemas</a>
    <a href="${GH}">GitHub</a>
  </nav>
</header>

<section class="hero">
  <p class="eyebrow">Durable · Load-bearing · Agentic</p>
  <h1>The load-bearing runtime<br>for agentic systems.</h1>
  <p class="lede">Orchestrate your coding agents in a runtime that survives crashes and
  forced reboots, resumes exactly where it left off, and scales from your laptop to your
  team's backbone. Author workflows as code — no diagram, no task-queue wiring.</p>
  <div class="cta">
    <a class="btn primary" href="/demo/">Try it in your browser →</a>
    <a class="btn" href="${GH}">View on GitHub</a>
  </div>
  <div class="install"><code>npm install @nanobpm/workflow</code></div>

  <figure class="code">
    <figcaption>A durable agent workflow, authored as code.</figcaption>
    <pre><code>${esc(heroCode)}</code></pre>
    <p class="code-note">Nano derives the model, the job types, the worker, and the
    human-approval wait. You write steps and handlers — nothing else.</p>
  </figure>
</section>

<section class="band problem">
  <div class="wrap">
    <h2>Your agent loops deserve a real runtime.</h2>
    <p>You're already running agents to write, review, and test code. But the orchestration
    is a pile of shell scripts and retries. When your laptop reboots overnight — a crash, or an
    IT-forced update you didn't choose — the run dies. You lose the state, re-run the expensive
    steps, re-spend the tokens, and have no idea what the agent actually did.</p>
  </div>
</section>

<section class="pillars wrap">
  <article>
    <h3>Durable</h3>
    <p>Survives crashes and forced reboots. On restart, a workflow resumes at the exact step it
    left off — completed activities aren't re-run and tokens aren't re-spent.</p>
    <p class="proof">SIGKILL → cold restart → exactly-once. Proven, with a negative control.</p>
  </article>
  <article>
    <h3>Load-bearing</h3>
    <p>The same runtime that ran your laptop loop runs your team's backbone. A single binary
    today; scale out when it gets real — no rewrite, no re-platforming.</p>
    <p class="proof">Small enough to go anywhere. Strong enough to build on.</p>
  </article>
  <article>
    <h3>Agentic</h3>
    <p>Coding agents, tools, and human approvals in one inspectable model — not a bash script you
    can't see into. A durable wait for human sign-off is a single line.</p>
    <p class="proof"><code>w.signal("humanApproval", …)</code> — one line, fully durable.</p>
  </article>
</section>

<section class="how wrap">
  <h2>No ceremony. Nano derives the wiring.</h2>
  <ol class="steps">
    <li><span>1</span><div><b>Author as code.</b> Declare durable steps with <code>w.run</code> and
      durable waits with <code>w.signal</code>. That's the whole surface.</div></li>
    <li><span>2</span><div><b>Nano derives the rest.</b> The executable model, the job types, the
      message correlation, and a generic worker — all generated. No diagram, no task queues, no
      registration.</div></li>
    <li><span>3</span><div><b>It just resumes.</b> Every run is an ordinary, durable Nano instance.
      You never think about the journal — crash-resume is implicit.</div></li>
  </ol>
</section>

<section class="band backbone">
  <div class="wrap">
    <h2>Starts on your laptop.<br>Becomes your backbone.</h2>
    <div class="cta">
      <a class="btn primary" href="/demo/">Watch a run survive a crash →</a>
      <a class="btn ghost" href="${GH}">Read the source</a>
    </div>
  </div>
</section>

<footer class="site-foot wrap">
  <p><a href="/demo/">Browser demo</a> · <a href="/schemas/">Published schemas</a> ·
  <a href="${GH}">GitHub</a></p>
  <p class="muted">Nano / ProcessOS · this page and every published schema are generated from
  in-repo sources of truth in <a href="${GH}">Magikcraft/nano-bpm</a>, so a URL can never drift
  from the artifact it names.</p>
</footer>`;

  return homePage("nanobpm.io — the load-bearing runtime for agentic systems", body);
}

function homePage(title, body) {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${esc(title)}</title>
<meta name="description" content="Nano is the load-bearing runtime for agentic systems: durable, code-first workflow orchestration that survives crashes and reboots, resumes exactly where it left off, and runs anywhere from your laptop to a cluster.">
<style>
  :root {
    --bg: #ffffff; --soft: #f6f7f9; --ink: #0b1020; --muted: #5b6472;
    --line: #e6e8ee; --accent: #4f46e5; --accent-ink: #3730a3;
    --code-bg: #0d1117; --code-ink: #e6edf3; --code-dim: #8b98a5;
    --radius: 12px; --wrap: 62rem;
  }
  * { box-sizing: border-box; }
  html { -webkit-text-size-adjust: 100%; }
  body {
    margin: 0; background: var(--bg); color: var(--ink);
    font: 17px/1.65 system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
    -webkit-font-smoothing: antialiased;
  }
  a { color: var(--accent-ink); text-decoration: none; }
  a:hover { text-decoration: underline; }
  code, pre { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  .wrap { max-width: var(--wrap); margin-inline: auto; padding-inline: 1.4rem; }
  h1, h2, h3 { line-height: 1.15; letter-spacing: -0.02em; }

  .nav {
    display: flex; align-items: center; justify-content: space-between;
    max-width: var(--wrap); margin-inline: auto; padding: 1.1rem 1.4rem;
  }
  .brand { font-weight: 700; font-size: 1.1rem; color: var(--ink); letter-spacing: -0.02em; }
  .brand .dim { color: var(--muted); font-weight: 500; }
  .nav nav a { color: var(--muted); margin-left: 1.4rem; font-size: .95rem; }
  .nav nav a:hover { color: var(--ink); text-decoration: none; }

  .hero { max-width: var(--wrap); margin-inline: auto; padding: 3.4rem 1.4rem 1rem; text-align: center; }
  .eyebrow {
    text-transform: uppercase; letter-spacing: 0.16em; font-size: .78rem; font-weight: 700;
    color: var(--accent); margin: 0 0 1rem;
  }
  .hero h1 { font-size: clamp(2.1rem, 5.4vw, 3.5rem); margin: 0 0 1.1rem; }
  .lede { font-size: clamp(1.05rem, 2.2vw, 1.25rem); color: var(--muted); max-width: 40rem; margin: 0 auto 1.7rem; }
  .cta { display: flex; gap: .75rem; justify-content: center; flex-wrap: wrap; }
  .btn {
    display: inline-block; padding: .7rem 1.15rem; border-radius: 9px; font-weight: 600;
    font-size: .98rem; border: 1px solid var(--line); color: var(--ink); background: #fff;
  }
  .btn:hover { text-decoration: none; border-color: #c7ccd8; }
  .btn.primary { background: var(--accent); border-color: var(--accent); color: #fff; }
  .btn.primary:hover { background: var(--accent-ink); border-color: var(--accent-ink); }
  .btn.ghost { background: transparent; color: #fff; border-color: rgba(255,255,255,.4); }
  .btn.ghost:hover { border-color: #fff; }
  .install { margin: 1.3rem auto 0; }
  .install code {
    background: var(--soft); border: 1px solid var(--line); color: var(--ink);
    padding: .55rem .9rem; border-radius: 8px; font-size: .95rem; display: inline-block;
  }
  .install code::before { content: "$ "; color: var(--muted); }

  figure.code { margin: 2.6rem auto 0; max-width: 52rem; text-align: left; }
  figure.code figcaption { font-size: .9rem; color: var(--muted); margin: 0 0 .5rem .2rem; }
  figure.code pre {
    background: var(--code-bg); color: var(--code-ink); border-radius: var(--radius);
    padding: 1.15rem 1.25rem; overflow-x: auto; font-size: .88rem; line-height: 1.6;
    box-shadow: 0 18px 40px -22px rgba(16,20,40,.5); border: 1px solid #1b2230;
  }
  .code-note { font-size: .92rem; color: var(--muted); margin: .7rem .2rem 0; }

  .band { margin-top: 3.6rem; padding: 3rem 0; }
  .band.problem { background: var(--soft); border-block: 1px solid var(--line); }
  .band.problem h2 { font-size: clamp(1.5rem, 3.4vw, 2rem); margin: 0 0 .8rem; max-width: 34rem; }
  .band.problem p { color: var(--muted); font-size: 1.08rem; max-width: 44rem; margin: 0; }

  .pillars { padding: 3.4rem 1.4rem; display: grid; gap: 1.6rem; grid-template-columns: repeat(3, 1fr); }
  .pillars article {
    border: 1px solid var(--line); border-radius: var(--radius); padding: 1.5rem 1.4rem;
    background: #fff;
  }
  .pillars h3 { font-size: 1.2rem; margin: 0 0 .5rem; }
  .pillars h3::before { content: ""; display: inline-block; width: .55rem; height: .55rem; border-radius: 2px; background: var(--accent); margin-right: .55rem; vertical-align: middle; }
  .pillars p { margin: 0 0 .7rem; color: var(--muted); font-size: .98rem; }
  .pillars .proof { color: var(--ink); font-weight: 600; font-size: .9rem; margin-bottom: 0; }
  .pillars .proof code { background: var(--soft); padding: .08em .35em; border-radius: 5px; font-size: .85em; font-weight: 500; }

  .how { padding: 1rem 1.4rem 3.6rem; }
  .how h2 { font-size: clamp(1.5rem, 3.4vw, 2rem); margin: 0 0 1.6rem; }
  .steps { list-style: none; margin: 0; padding: 0; display: grid; gap: 1.1rem; max-width: 46rem; }
  .steps li { display: flex; gap: 1rem; align-items: flex-start; }
  .steps li span {
    flex: none; width: 1.9rem; height: 1.9rem; border-radius: 50%; background: var(--soft);
    border: 1px solid var(--line); color: var(--accent-ink); font-weight: 700;
    display: grid; place-items: center; font-size: .95rem;
  }
  .steps li div { color: var(--muted); }
  .steps li b { color: var(--ink); }
  .steps code { background: var(--soft); padding: .08em .35em; border-radius: 5px; font-size: .85em; }

  .band.backbone { background: var(--code-bg); color: #fff; text-align: center; padding: 3.6rem 0; margin-top: 0; }
  .band.backbone h2 { font-size: clamp(1.8rem, 4.4vw, 2.8rem); margin: 0 0 1.6rem; }
  .band.backbone .cta { }

  .site-foot { padding: 2.4rem 1.4rem 3rem; }
  .site-foot p { margin: .3rem 0; }
  .site-foot .muted, .muted { color: var(--muted); }
  .site-foot .muted { font-size: .9rem; max-width: 42rem; }

  @media (max-width: 800px) { .pillars { grid-template-columns: 1fr; } }
</style>
</head>
<body>
${body}
</body>
</html>
`;
}

function schemasHtml(items) {
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
    `<h1>Published schemas &amp; namespaces</h1>
<p><a href="/">← nanobpm.io</a></p>
<p>The canonical home for the schemas and namespaces published by
<a href="https://github.com/Magikcraft/nano-bpm">nano-bpm</a> (Nano / ProcessOS).
Every URL below is generated from its in-repo source of truth on each deploy, so it
never drifts from the artifact it names.</p>
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
