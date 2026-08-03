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
  readdirSync,
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

// Whether the bundled docs (console/public/docs) were generated for this build.
// Computed up-front so the landing nav + footer only advertise /docs/ when the
// docs are actually shipped — the zero-dep schemas CI run of build.mjs skips the
// docs build, and pages.yml (the real deploy) builds them first.
const docsPresent = existsSync(
  join(repoRoot, "console", "public", "docs", "index.html"),
);

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

// --- Bundled documentation site: console/public/docs -> /docs -----------------
// The end-user docs (USERGUIDE.md chunked + the standalone guides, including
// Getting Started with Urban) are generated by console/scripts/build-docs.mjs
// into console/public/docs/*.html — the SAME self-contained pages the gateway
// embeds and serves at a node's /docs. We publish that generated site verbatim
// to nanobpm.io/docs so a change to any doc source flows to both surfaces from
// one renderer (no drift, no second doc-site generator). When the docs have not
// been built (e.g. the zero-dep `schemas` CI drift check runs build.mjs alone),
// /docs is simply omitted — same graceful pattern as the demo dist below.
const docsDist = join(repoRoot, "console", "public", "docs");
if (docsPresent) {
  const docsOut = join(outDir, "docs");
  cpSync(docsDist, docsOut, { recursive: true });
  // The sidebar links between doc pages are extensionless (`/docs/usage`). The
  // in-app gateway resolves those by appending `.html` (see server docs_asset).
  // GitHub Pages does NOT append `.html` for us, so mirror each `<slug>.html`
  // as `<slug>/index.html` — a directory index resolves the extensionless link
  // reliably on Pages (via the standard trailing-slash redirect). `index.html`
  // is skipped (it already serves `/docs/`). The pages are self-contained with
  // only absolute links, so serving from a subdirectory changes nothing.
  for (const entry of readdirSync(docsDist, { withFileTypes: true })) {
    if (!entry.isFile() || !entry.name.endsWith(".html")) continue;
    if (entry.name === "index.html") continue;
    const slug = entry.name.slice(0, -".html".length);
    const dir = join(docsOut, slug);
    mkdirSync(dir, { recursive: true });
    copyFileSync(join(docsDist, entry.name), join(dir, "index.html"));
  }
}

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
<p class="muted">Served from <a href="/">nanobpm.io</a>.</p>
</body>
</html>
`;
}

// Official language logos (devicon originals) for the Polyglot section. Read from
// website/logos/*.svg at build time and inlined, so the page stays zero-dependency
// and renders offline (no external icon CDN). Rust's mark is monochrome and given
// a light root fill in the source SVG so it reads on the dark band.
function langChips() {
  const langs = [
    ["typescript", "TypeScript"],
    ["java", "Java"],
    ["rust", "Rust"],
    ["python", "Python"],
    ["csharp", "C#"],
  ];
  return langs
    .map(([id, label]) => {
      const svg = readFileSync(join(here, "logos", `${id}.svg`), "utf8")
        .trim()
        .replace(/^<svg /, '<svg aria-hidden="true" focusable="false" ');
      return `      <span class="lang lang-${id}">${svg}${label}</span>`;
    })
    .join("\n");
}

/**
 * Extract a `//#region NAME … //#endregion NAME` block from a source file,
 * stripping the marker lines and any common leading indentation. Used to render
 * real, type-checked snippets on the site verbatim (see website/snippets/), so
 * the marketing code can never drift from the API it demonstrates.
 *
 * Blank lines immediately adjacent to the markers are dropped (they're just
 * breathing room around the region tags), but every code line is kept
 * byte-for-byte after de-indentation — so what's rendered is exactly what CI
 * type-checks.
 */
function readSnippetRegion(relPath, region) {
  const src = readFileSync(join(here, relPath), "utf8");
  const lines = src.split("\n");
  const start = lines.findIndex((l) => l.trim() === `//#region ${region}`);
  const end = lines.findIndex((l, i) => i > start && l.trim() === `//#endregion ${region}`);
  if (start === -1 || end === -1 || end <= start) {
    throw new Error(`snippet region "${region}" not found in ${relPath}`);
  }
  const body = lines.slice(start + 1, end);
  const indent = Math.min(
    ...body.filter((l) => l.trim() !== "").map((l) => l.match(/^ */)[0].length),
  );
  const out = body.map((l) => l.slice(indent));
  while (out.length && out[0].trim() === "") out.shift();
  while (out.length && out[out.length - 1].trim() === "") out.pop();
  return out.join("\n");
}

function homeHtml() {
  // The Code-first tab renders the `hero` region of website/snippets/hero-pr-review.ts
  // VERBATIM — a real, compilable `defineFlow` example type-checked against the
  // published @nanobpm/workflow in CI ("website snippet (typecheck)" job), so the
  // landing hero can never drift from the actual API. It's the SAME urban-pr-review
  // convergence loop the Model-first tab runs live (same task ids / signal / key).
  const heroCode = readSnippetRegion("snippets/hero-pr-review.ts", "hero");

  const body = `<header class="nav">
  <a class="brand" href="/">nanobpm<span class="dim">.io</span></a>
  <nav>
    <a href="/demo/">Demo</a>
    ${docsPresent ? '<a href="/docs/">Docs</a>' : ""}
    <a href="/schemas/">Schemas</a>
  </nav>
</header>

<section class="hero">
  <p class="eyebrow"><span class="arp">Advanced Research Prototype</span></p>
  <h1>Agentic Orchestration<br>for the <span class="grad">Developer Workstation.</span></h1>
  <p class="lede">A RAAD — Rapid Agentic Application Development — environment that runs on
  your machine. Compose coding agents, tools, and human approvals into durable workflows.
  Code-first or Model-first, provider-agnostic, and small enough to start on a Raspberry Pi.</p>
  <div class="cta">
    <a class="btn primary" href="#try">See it live ↓</a>
    <a class="btn ghost" href="/demo/">Open the browser demo →</a>
  </div>
  <div class="install multi" aria-label="Install and start Nano">
    <code>npm i -g @camunda8/cli</code>
    <code>c8ctl load plugin c8ctl-plugin-nano</code>
    <code>c8ctl nano start</code>
  </div>
</section>

<section id="try" class="demo-tabs wrap">
  <h2 class="tabs-title">Code-first <span class="grad">or</span> Model-first.</h2>
  <p class="tabs-sub">One app — two authoring surfaces, the same engine.</p>
  <div class="tablist" role="tablist" aria-label="Authoring surface">
    <button class="tab" role="tab" id="tab-code" aria-controls="panel-code" aria-selected="true" tabindex="0">Code-first</button>
    <button class="tab" role="tab" id="tab-model" aria-controls="panel-model" aria-selected="false" tabindex="-1">Model-first</button>
  </div>

  <div class="tabpanel" id="panel-code" role="tabpanel" aria-labelledby="tab-code">
    <figure class="code pop">
      <figcaption>A loop where local coding agents iterate to convergence against GitHub Copilot’s adversarial PR reviews — authored as code.</figcaption>
      <pre><code>${highlightTs(heroCode)}</code></pre>
      <p class="code-note">Nano derives the executable model, the job types, the message
      correlation, and a generic worker. You write steps and handlers — nothing else.</p>
    </figure>
  </div>

  <div class="tabpanel" id="panel-model" role="tabpanel" aria-labelledby="tab-model" hidden>
    <iframe class="demo-frame" title="Live Model-first urban-pr-review demo on the wasm engine"
      data-src="/demo/?embed=1" loading="lazy" allow="fullscreen" allowfullscreen></iframe>
    <p class="tab-note">The real <code>urban-pr-review</code> BPMN model, executing live on the
    WebAssembly build of the engine — no server. Or <a href="/demo/">open it full-page →</a></p>
  </div>
</section>

<section class="band problem">
  <div class="wrap">
    <h2>New levels of abstraction demand new primitives.</h2>
    <p>You're already orchestrating agents to write, review, and test code — but the wiring is a
    pile of shell scripts and retries. When the machine reboots overnight — a crash, or an
    IT-forced update — the run dies, the state is gone, and the tokens are re-spent. Agentic
    systems need durable, inspectable primitives, not more glue.</p>
  </div>
</section>

<section class="pillars wrap">
  <article>
    <h3>DRY your agentic SDLC</h3>
    <p>Declare durable steps with <code>w.run</code> and durable waits with <code>w.signal</code>.
    Nano derives the model, job types, correlation, and workers — one source of truth, no copy-paste
    orchestration across every workflow.</p>
    <p class="proof">Author once. Nano derives the rest.</p>
  </article>
  <article>
    <h3>Code-first or Model-first</h3>
    <p>Write the workflow as code, or draw it as BPMN — the same durable engine runs both. Switch
    surfaces without re-platforming; the model and the code are two views of one runtime.</p>
    <p class="proof">Same engine. Same app. Your choice of surface.</p>
  </article>
  <article>
    <h3>Durable by default</h3>
    <p>Survives crashes and forced reboots. On restart a workflow resumes at the exact step it left
    off — completed activities aren't re-run and tokens aren't re-spent.</p>
    <p class="proof">SIGKILL → cold restart → exactly-once. With a negative control.</p>
  </article>
</section>

<section class="band providers">
  <div class="wrap">
    <h2>Provider-agnostic. <span class="grad">Mix frontier with local.</span></h2>
    <p>Blend frontier models with local LLMs — on one workstation or across many. Route the expensive
    steps to a frontier model and keep the rest private and free. Bring the coding harness you already use:</p>
    <div class="provs" aria-label="Supported agent harnesses">
      <span>GitHub Copilot</span>
      <span>Claude Code</span>
      <span>Pi</span>
      <span>Open Coder</span>
      <span>Little Coder</span>
      <span class="more">and more</span>
    </div>
  </div>
</section>

<section class="band polyglot">
  <div class="wrap">
    <h2>Polyglot by design. <span class="grad">Meet your stack.</span></h2>
    <p>Drive workflows and workers from the language your team already ships in — one durable engine,
    first-class SDKs across the stack:</p>
    <div class="provs langs" aria-label="Supported SDK languages">
${langChips()}
    </div>
  </div>
</section>

<section class="how wrap">
  <h2>Add an agent + coding harness to your workforce.</h2>
  <p class="how-sub">Turn any interactive CLI agent harness into a durable Nano job worker.
  <b>Hire</b> persists an agent profile; <b>work</b> polls the cluster and runs jobs through it.</p>
  <div class="install multi" aria-label="Hire and run an agent worker">
    <code>c8ctl nano hire copilot</code>
    <code>c8ctl nano work copilot</code>
  </div>
  <ol class="steps">
    <li><span>1</span><div><b>Hire.</b> Persist an agent profile — a name, a rank, and the CLI
      command (e.g. <code>copilot</code>) that backs it.</div></li>
    <li><span>2</span><div><b>Work.</b> The harness polls for jobs and runs them, so a coding agent
      becomes a first-class, durable worker in your orchestration.</div></li>
    <li><span>3</span><div><b>It just resumes.</b> Every run is an ordinary durable Nano instance —
      crash-resume is implicit, and you never think about the journal.</div></li>
  </ol>
</section>

<section class="band backbone">
  <div class="wrap">
    <h2>RAAD.<br><span class="grad">Rapid Agentic Application Development.</span></h2>
    <p class="raad-sub">An Advanced Research Prototype for agentic orchestration on the developer
    workstation. Start in three lines.</p>
    <div class="install multi center" aria-label="Get started">
      <code>npm i -g @camunda8/cli</code>
      <code>c8ctl load plugin c8ctl-plugin-nano</code>
      <code>c8ctl nano start</code>
    </div>
    <div class="cta">
      <a class="btn primary" href="/demo/">Try it in your browser →</a>
    </div>
  </div>
</section>

<footer class="site-foot wrap">
  <p><a href="/demo/">Browser demo</a>${docsPresent ? ' · <a href="/docs/">Documentation</a>' : ""} · <a href="/schemas/">Published schemas</a></p>
  <p class="muted">Nano is an Advanced Research Prototype. Free for personal or evaluation use.</p>
</footer>

<script>
// Accessible tabs for the Code-first / Model-first panel. Lazy-loads the demo
// iframe (data-src -> src) only when the Model-first tab is first selected, so
// the home page never eagerly loads /demo/ (keeps first paint light).
(function () {
  var tabs = Array.prototype.slice.call(document.querySelectorAll(".demo-tabs .tab"));
  if (!tabs.length) return;
  function select(tab) {
    tabs.forEach(function (t) {
      var on = t === tab;
      t.setAttribute("aria-selected", on ? "true" : "false");
      t.tabIndex = on ? 0 : -1;
      var panel = document.getElementById(t.getAttribute("aria-controls"));
      if (panel) panel.hidden = !on;
    });
    var panel = document.getElementById(tab.getAttribute("aria-controls"));
    if (panel) {
      var frame = panel.querySelector("iframe[data-src]");
      if (frame) {
        frame.setAttribute("src", frame.getAttribute("data-src"));
        frame.removeAttribute("data-src");
      }
    }
  }
  tabs.forEach(function (tab, i) {
    tab.addEventListener("click", function () { select(tab); });
    tab.addEventListener("keydown", function (e) {
      if (e.key === "ArrowRight" || e.key === "ArrowLeft") {
        e.preventDefault();
        var n = e.key === "ArrowRight" ? (i + 1) % tabs.length : (i - 1 + tabs.length) % tabs.length;
        tabs[n].focus(); select(tabs[n]);
      }
    });
  });
})();
</script>`;

  return homePage("nanobpm.io — Agentic Orchestration for the Developer Workstation", body);
}

// Minimal, dependency-free TS/JS highlighter for the fixed hero snippet. Ordered
// alternation, left-to-right; every emitted token is HTML-escaped via tok()/esc().
// Kept in-repo so `node website/build.mjs` stays zero-dependency (CI schemas job).
function highlightTs(src) {
  const re =
    /(\/\/[^\n]*)|("(?:[^"\\]|\\.)*")|\b(import|from|export|const|let|var|async|await|return|new|of|in|function)\b|(=>)|\b(\d+)\b|\.([A-Za-z_$][\w$]*)(?=\s*\()|([A-Za-z_$][\w$]*)(?=\s*\()|\.([A-Za-z_$][\w$]*)|([A-Za-z_$][\w$]*)|([{}()[\];,:.])/g;
  const dot = '<span class="tok-punc">.</span>';
  let out = "";
  let last = 0;
  for (let m = re.exec(src); m !== null; m = re.exec(src)) {
    out += esc(src.slice(last, m.index));
    last = re.lastIndex;
    if (m[1] !== undefined) out += tok("com", m[1]);
    else if (m[2] !== undefined) out += tok("str", m[2]);
    else if (m[3] !== undefined) out += tok("kw", m[3]);
    else if (m[4] !== undefined) out += tok("arw", m[4]);
    else if (m[5] !== undefined) out += tok("num", m[5]);
    else if (m[6] !== undefined) out += dot + tok("fn", m[6]);
    else if (m[7] !== undefined) out += tok("fn", m[7]);
    else if (m[8] !== undefined) out += dot + tok("prop", m[8]);
    else if (m[9] !== undefined) out += tok("id", m[9]);
    else if (m[10] !== undefined) out += tok("punc", m[10]);
  }
  out += esc(src.slice(last));
  return out;
}

function tok(cls, text) {
  return `<span class="tok-${cls}">${esc(text)}</span>`;
}

function homePage(title, body) {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${esc(title)}</title>
<meta name="description" content="Nano is an Advanced Research Prototype for agentic orchestration on the developer workstation: a RAAD — Rapid Agentic Application Development — environment that composes coding agents, tools, and human approvals into durable, code-first or model-first workflows, provider-agnostic across frontier and local LLMs.">
<style>
  :root {
    --bg: #08080a; --panel: rgba(22,24,30,.55); --line: rgba(120,130,150,.16);
    --ink: #e4e4e7; --muted: #a1a1aa; --emerald: #34d399; --sky: #38bdf8;
    --accent: #7dd3fc; --code-bg: #0d1117; --radius: 12px; --wrap: 62rem;
  }
  * { box-sizing: border-box; }
  html { -webkit-text-size-adjust: 100%; }
  body {
    margin: 0; background: var(--bg); color: var(--ink); position: relative;
    font: 17px/1.65 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
    -webkit-font-smoothing: antialiased;
  }
  #field { position: fixed; inset: 0; z-index: 0; display: block; pointer-events: none; }
  .glow {
    position: fixed; inset: 0; z-index: 1; pointer-events: none;
    background:
      radial-gradient(60% 60% at 50% 18%, rgba(56,189,248,.12), transparent 70%),
      radial-gradient(50% 50% at 78% 62%, rgba(52,211,153,.10), transparent 70%);
  }
  header, section, footer { position: relative; z-index: 2; }
  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }
  code, pre { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  .wrap { max-width: var(--wrap); margin-inline: auto; padding-inline: 1.4rem; }
  h1, h2, h3 { line-height: 1.12; letter-spacing: -0.02em; }

  .nav {
    display: flex; align-items: center; justify-content: space-between;
    max-width: var(--wrap); margin-inline: auto; padding: 1.1rem 1.4rem;
  }
  .brand { font-weight: 700; font-size: 1.1rem; color: var(--ink); letter-spacing: -0.02em; }
  .brand:hover { text-decoration: none; }
  .brand .dim { color: var(--muted); font-weight: 500; }
  .nav nav a { color: var(--muted); margin-left: 1.4rem; font-size: .95rem; }
  .nav nav a:hover { color: var(--ink); text-decoration: none; }

  .hero { max-width: var(--wrap); margin-inline: auto; padding: 4rem 1.4rem 1.5rem; text-align: center; }
  .eyebrow {
    text-transform: uppercase; letter-spacing: 0.18em; font-size: .78rem; font-weight: 700;
    color: var(--sky); margin: 0 0 1rem;
  }
  .hero h1 { font-size: clamp(2.2rem, 5.6vw, 3.7rem); margin: 0 0 1.2rem; font-weight: 700; }
  .grad {
    background: linear-gradient(90deg, var(--emerald), var(--sky));
    -webkit-background-clip: text; background-clip: text; color: transparent;
  }
  .lede { font-size: clamp(1.05rem, 2.2vw, 1.25rem); color: var(--muted); max-width: 40rem; margin: 0 auto 1.8rem; }
  .cta { display: flex; gap: .8rem; justify-content: center; flex-wrap: wrap; }
  .btn {
    display: inline-block; padding: .72rem 1.2rem; border-radius: 10px; font-weight: 600;
    font-size: .98rem; border: 1px solid var(--line); color: var(--ink);
    background: rgba(255,255,255,.05); transition: transform .12s ease, box-shadow .12s, background .12s;
  }
  .btn:hover { text-decoration: none; transform: translateY(-2px); }
  .btn.primary {
    color: #052e1a; border-color: transparent;
    background: linear-gradient(135deg, var(--emerald), var(--sky));
    box-shadow: 0 8px 30px rgba(56,189,248,.3);
  }
  .btn.ghost { background: rgba(255,255,255,.05); color: var(--ink); }
  .btn.ghost:hover { background: rgba(255,255,255,.09); }
  .install { margin: 1.4rem auto 0; }
  .install code {
    background: rgba(255,255,255,.04); border: 1px solid var(--line); color: var(--ink);
    padding: .55rem .9rem; border-radius: 8px; font-size: .95rem; display: inline-block;
  }
  .install code::before { content: "$ "; color: var(--muted); }
  .install.multi { display: flex; flex-direction: column; gap: .5rem; align-items: flex-start; max-width: max-content; margin-inline: auto; }
  .install.multi.center { align-items: center; }
  .install.multi code { text-align: left; }

  .arp {
    display: inline-block; border: 1px solid rgba(56,189,248,.4); border-radius: 999px;
    padding: .28rem .8rem; background: rgba(56,189,248,.10); color: var(--sky);
  }

  .demo-tabs { padding-top: 3.5rem; text-align: center; }
  .tabs-title { font-size: clamp(1.6rem, 3.6vw, 2.2rem); margin: 0 0 .4rem; }
  .tabs-sub { color: var(--muted); margin: 0 auto 1.4rem; font-size: 1rem; }
  .tabs-sub code, .how-sub code, .raad-sub code, .band.providers code { background: rgba(255,255,255,.05); padding: .08em .35em; border-radius: 5px; font-size: .9em; color: var(--sky); }  .tablist { display: inline-flex; gap: .5rem; padding: .3rem; border: 1px solid var(--line); border-radius: 12px; background: rgba(255,255,255,.03); margin-bottom: 1.4rem; }
  .tab {
    padding: .55rem 1.15rem; border-radius: 9px; border: 1px solid transparent; cursor: pointer;
    background: transparent; color: var(--muted); font-weight: 600; font-size: .95rem; font-family: inherit;
    transition: background .12s, color .12s, box-shadow .12s;
  }
  .tab:hover { color: var(--ink); }
  .tab[aria-selected="true"] {
    color: #052e1a; border-color: transparent;
    background: linear-gradient(135deg, var(--emerald), var(--sky));
    box-shadow: 0 8px 24px rgba(56,189,248,.28);
  }
  .tabpanel[hidden] { display: none; }
  .demo-frame {
    width: 100%; height: 560px; border: 1px solid rgba(120,130,150,.2); border-radius: var(--radius);
    background: var(--code-bg); box-shadow: 0 24px 60px -24px rgba(0,0,0,.7); display: block;
  }
  .tab-note { font-size: .92rem; color: var(--muted); margin: .8rem .2rem 0; }
  figure.code.pop pre {
    border-color: rgba(56,189,248,.35);
    box-shadow: 0 24px 60px -20px rgba(56,189,248,.35), 0 0 0 1px rgba(52,211,153,.12) inset;
  }

  .band.providers { background: var(--panel); border-block: 1px solid var(--line); backdrop-filter: blur(6px); text-align: center; }
  .band.polyglot { text-align: center; }
  .band.providers h2, .band.polyglot h2 { font-size: clamp(1.5rem, 3.4vw, 2rem); margin: 0 0 .8rem; }
  .band.providers p, .band.polyglot p { color: var(--muted); font-size: 1.06rem; max-width: 44rem; margin: 0 auto; }
  .provs { display: flex; flex-wrap: wrap; gap: .6rem; justify-content: center; margin-top: 1.3rem; }
  .provs span {
    border: 1px solid var(--line); background: rgba(255,255,255,.04); padding: .42rem .85rem;
    border-radius: 999px; font-size: .92rem; color: var(--ink); font-weight: 500;
  }
  .provs span.more { color: var(--muted); font-style: italic; }
  .provs.langs span.lang { display: inline-flex; align-items: center; gap: .5rem; }
  .provs.langs span.lang > svg { flex: none; width: 1.2rem; height: 1.2rem; display: block; }
  .how-sub { color: var(--muted); max-width: 46rem; margin: 0 0 1.2rem; }
  .how .install.multi { margin: 0 0 1.8rem; }
  .band.backbone .raad-sub { color: var(--muted); max-width: 40rem; margin: 0 auto 1.4rem; font-size: 1.05rem; }
  .band.backbone .install.multi { margin-bottom: 1.6rem; }

  figure.code { margin: 2.8rem auto 0; max-width: 52rem; text-align: left; }
  figure.code figcaption { font-size: .9rem; color: var(--muted); margin: 0 0 .5rem .2rem; }
  figure.code pre {
    background: var(--code-bg); color: #c9d1d9; border-radius: var(--radius);
    padding: 1.15rem 1.25rem; overflow-x: auto; font-size: .9rem; line-height: 1.65;
    box-shadow: 0 24px 60px -24px rgba(0,0,0,.7); border: 1px solid rgba(120,130,150,.2);
  }
  .code-note { font-size: .92rem; color: var(--muted); margin: .7rem .2rem 0; }

  .tok-com { color: #8b949e; font-style: italic; }
  .tok-str { color: #a5d6ff; }
  .tok-kw { color: #ff7b72; }
  .tok-fn { color: #d2a8ff; }
  .tok-prop { color: #79c0ff; }
  .tok-num { color: #79c0ff; }
  .tok-arw { color: #ff7b72; }
  .tok-id, .tok-punc { color: #c9d1d9; }

  .band { margin-top: 4rem; padding: 3.2rem 0; }
  .band.problem { background: var(--panel); border-block: 1px solid var(--line); backdrop-filter: blur(6px); }
  .band.problem h2 { font-size: clamp(1.5rem, 3.4vw, 2rem); margin: 0 0 .8rem; max-width: 34rem; }
  .band.problem p { color: var(--muted); font-size: 1.08rem; max-width: 44rem; margin: 0; }

  .pillars { padding: 3.6rem 1.4rem; display: grid; gap: 1.6rem; grid-template-columns: repeat(3, 1fr); }
  .pillars article {
    border: 1px solid var(--line); border-radius: var(--radius); padding: 1.6rem 1.5rem;
    background: var(--panel); backdrop-filter: blur(6px);
  }
  .pillars h3 { font-size: 1.2rem; margin: 0 0 .5rem; }
  .pillars h3::before { content: ""; display: inline-block; width: .55rem; height: .55rem; border-radius: 2px; background: linear-gradient(135deg, var(--emerald), var(--sky)); margin-right: .55rem; vertical-align: middle; }
  .pillars p { margin: 0 0 .7rem; color: var(--muted); font-size: .98rem; }
  .pillars .proof { color: var(--ink); font-weight: 600; font-size: .9rem; margin-bottom: 0; }
  .pillars .proof code { background: rgba(255,255,255,.05); padding: .08em .35em; border-radius: 5px; font-size: .85em; font-weight: 500; color: var(--sky); }

  .how { padding: 1.5rem 1.4rem 3.8rem; }
  .how h2 { font-size: clamp(1.5rem, 3.4vw, 2rem); margin: 0 0 1.6rem; }
  .steps { list-style: none; margin: 0; padding: 0; display: grid; gap: 1.1rem; max-width: 46rem; }
  .steps li { display: flex; gap: 1rem; align-items: flex-start; }
  .steps li span {
    flex: none; width: 1.9rem; height: 1.9rem; border-radius: 50%; background: rgba(255,255,255,.05);
    border: 1px solid var(--line); color: var(--sky); font-weight: 700;
    display: grid; place-items: center; font-size: .95rem;
  }
  .steps li div { color: var(--muted); }
  .steps li b { color: var(--ink); }
  .steps code { background: rgba(255,255,255,.05); padding: .08em .35em; border-radius: 5px; font-size: .85em; color: var(--sky); }

  .band.backbone {
    background: linear-gradient(180deg, rgba(15,17,21,.55), rgba(8,8,10,.85));
    border-block: 1px solid var(--line); text-align: center; padding: 3.8rem 0; margin-top: 0;
    backdrop-filter: blur(6px);
  }
  .band.backbone h2 { font-size: clamp(1.8rem, 4.4vw, 2.8rem); margin: 0 0 1.6rem; }

  .site-foot { padding: 2.6rem 1.4rem 3.2rem; }
  .site-foot p { margin: .3rem 0; }
  .site-foot .muted, .muted { color: var(--muted); }
  .site-foot .muted { font-size: .9rem; max-width: 42rem; }

  @media (max-width: 800px) { .pillars { grid-template-columns: 1fr; } .demo-frame { height: 460px; } }
  @media (prefers-reduced-motion: reduce) { #field { display: none; } }
</style>
</head>
<body>
<canvas id="field" aria-hidden="true"></canvas>
<div class="glow" aria-hidden="true"></div>
${body}
<script>
// A rotating "constellation" particle field (ported from the Nano server's own
// landing page): points drift, the whole field slowly rotates around the centre,
// and nearby points are linked by lines whose opacity falls off with distance.
// Pure canvas, no dependencies, so the page stays self-contained and works offline.
(function () {
  var canvas = document.getElementById("field");
  if (!canvas) return;
  var ctx = canvas.getContext("2d");
  if (!ctx) return;
  // Respect prefers-reduced-motion: the canvas is CSS-hidden, but bail out here too
  // so we never start the rAF loop or resize handler for users who disabled motion.
  var reduce = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)");
  if (reduce && reduce.matches) return;
  var w, h, cx, cy, points, dpr, rot = 0;
  function resize() {
    dpr = Math.min(window.devicePixelRatio || 1, 2);
    w = canvas.width = Math.floor(innerWidth * dpr);
    h = canvas.height = Math.floor(innerHeight * dpr);
    canvas.style.width = innerWidth + "px";
    canvas.style.height = innerHeight + "px";
    cx = w / 2; cy = h / 2; seed();
  }
  function seed() {
    var count = Math.min(140, Math.floor((w * h) / (22000 * dpr)));
    points = [];
    for (var i = 0; i < count; i++) {
      points.push({
        a: Math.random() * Math.PI * 2,
        r: Math.pow(Math.random(), 0.6) * Math.min(w, h) * 0.55,
        vx: (Math.random() - 0.5) * 0.25 * dpr,
        vy: (Math.random() - 0.5) * 0.25 * dpr,
        x: 0, y: 0, driftX: 0, driftY: 0,
      });
    }
  }
  var LINK = 130;
  function frame() {
    rot += 0.0006;
    ctx.clearRect(0, 0, w, h);
    var cosR = Math.cos(rot), sinR = Math.sin(rot), link = LINK * dpr, i, j, p, a, b;
    for (i = 0; i < points.length; i++) {
      p = points[i];
      var bx = Math.cos(p.a) * p.r, by = Math.sin(p.a) * p.r;
      p.driftX += p.vx; p.driftY += p.vy;
      if (Math.abs(p.driftX) > 60 * dpr) p.vx *= -1;
      if (Math.abs(p.driftY) > 60 * dpr) p.vy *= -1;
      p.x = cx + (bx * cosR - by * sinR + p.driftX);
      p.y = cy + (bx * sinR + by * cosR + p.driftY);
    }
    for (i = 0; i < points.length; i++) {
      for (j = i + 1; j < points.length; j++) {
        a = points[i]; b = points[j];
        var d = Math.hypot(a.x - b.x, a.y - b.y);
        if (d < link) {
          var t = 1 - d / link;
          ctx.strokeStyle = "rgba(80, 200, 230, " + (0.18 * t) + ")";
          ctx.lineWidth = dpr * 0.6;
          ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y); ctx.stroke();
        }
      }
    }
    for (i = 0; i < points.length; i++) {
      p = points[i];
      ctx.beginPath(); ctx.arc(p.x, p.y, dpr * 1.6, 0, Math.PI * 2);
      ctx.fillStyle = "rgba(150, 230, 220, 0.85)"; ctx.fill();
    }
    requestAnimationFrame(frame);
  }
  addEventListener("resize", resize);
  resize(); frame();
})();
</script>
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
Nano / ProcessOS. Every URL below is generated from its in-repo source of truth on
each deploy, so it never drifts from the artifact it names.</p>
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
