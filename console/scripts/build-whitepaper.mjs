// Builds the bundled whitepaper page from the repository docs/whitepaper.md.
//
// The single source of truth is ../docs/whitepaper.md (the Nano design paper).
// It is rendered to HTML with markdown-it and wrapped in the same branded shell
// the docs site uses, with a sticky table of contents built from its H2
// sections. Output lands in public/whitepaper/index.html, which Vite ships into
// dist/ and the gateway embeds and serves at `/whitepaper`, so the paper works
// fully offline and refreshes on every `npm run build`.
//
// docs/whitepaper.md is the source; the generated HTML is git-ignored. Re-run
// via `npm run build` (or directly) to refresh it. Author-facing content is
// dropped from the shipped page: `> DRAFTING NOTE …` blockquotes and the
// "Appendix … (for authors)" section stay in the source but not the website.
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import MarkdownIt from "markdown-it";

const root = process.cwd();
const paperPath = join(root, "..", "docs", "whitepaper.md");
const outDir = join(root, "public", "whitepaper");

// Repo links in the paper are relative to the repo root; rewrite them to GitHub
// blob URLs so they resolve from the shipped, standalone page.
const REPO_BLOB = "https://github.com/jwulf/nano-bpm/blob/main/";

const esc = (s) =>
  String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

// GitHub-compatible heading slug (matches intra-doc anchors).
function slugify(text) {
  return String(text)
    .toLowerCase()
    .replace(/<[^>]+>/g, "")
    .replace(/[^\w\- ]+/g, "")
    .trim()
    .replace(/\s+/g, "-");
}

const raw = readFileSync(paperPath, "utf8");

// --- 1. Strip author-facing content -----------------------------------------
// Drop `> DRAFTING NOTE …` blockquote runs and any H2 "Appendix … (for authors)"
// section, tracking fenced code so `>`/`##` inside code blocks are untouched.
function stripAuthorContent(markdown) {
  const out = [];
  const lines = markdown.split("\n");
  let inFence = false;
  let skippingNote = false;
  let skippingSection = false;
  for (const line of lines) {
    const trimmed = line.trim();
    if (/^```/.test(trimmed)) inFence = !inFence;

    if (!inFence) {
      const h2 = /^## (.+)$/.exec(line);
      if (h2) {
        // Enter/exit an author-only appendix section at H2 boundaries.
        skippingSection = /\(for authors\)/i.test(h2[1]);
        skippingNote = false;
        if (skippingSection) continue;
      }
      if (skippingSection) continue;

      if (/^>\s*DRAFTING NOTE/i.test(trimmed)) {
        skippingNote = true;
        continue;
      }
      if (skippingNote) {
        // A drafting note is a run of consecutive blockquote lines; it ends at
        // the first non-blockquote (blank or otherwise) line.
        if (trimmed.startsWith(">")) continue;
        skippingNote = false;
      }
    }
    out.push(line);
  }
  // Collapse any 3+ blank-line gaps the stripping may have opened.
  return out.join("\n").replace(/\n{3,}/g, "\n\n");
}

const markdown = stripAuthorContent(raw);

// --- 2. Table of contents from H2 headings ----------------------------------
const toc = [];
{
  let inFence = false;
  for (const line of markdown.split("\n")) {
    if (/^```/.test(line.trim())) inFence = !inFence;
    if (inFence) continue;
    const m = /^## (.+)$/.exec(line);
    if (m) {
      const title = m[1].trim();
      toc.push({ title, slug: slugify(title) });
    }
  }
}

// --- 3. markdown-it: heading ids + link rewriting ---------------------------
const md = new MarkdownIt({ html: false, linkify: true, breaks: false });

md.renderer.rules.heading_open = (tokens, idx, options, _env, self) => {
  const inline = tokens[idx + 1];
  const text = inline && inline.type === "inline" ? inline.content : "";
  const id = slugify(text.replace(/`/g, ""));
  if (id) tokens[idx].attrSet("id", id);
  return self.renderToken(tokens, idx, options);
};

function rewriteHref(href) {
  if (!href) return href;
  if (/^(https?:|mailto:)/i.test(href)) return href;
  if (href.startsWith("#")) return href; // intra-page anchor — single page
  return REPO_BLOB + href.replace(/^\.\//, "");
}

const defaultLinkOpen =
  md.renderer.rules.link_open ||
  ((tokens, idx, options, _env, self) => self.renderToken(tokens, idx, options));
md.renderer.rules.link_open = (tokens, idx, options, env, self) => {
  const hrefIdx = tokens[idx].attrIndex("href");
  if (hrefIdx >= 0) {
    tokens[idx].attrs[hrefIdx][1] = rewriteHref(tokens[idx].attrs[hrefIdx][1]);
  }
  return defaultLinkOpen(tokens, idx, options, env, self);
};

// --- 4. Page shell (mirrors console/scripts/build-docs.mjs) ------------------
function tocList() {
  return toc
    .map((t) => `<a class="nav-link" href="#${t.slug}">${esc(t.title)}</a>`)
    .join("\n");
}

function shell() {
  const body = md.render(markdown);
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Nano BPM · Whitepaper</title>
    <style>
      :root {
        --bg: #ffffff; --fg: #18181b; --muted: #71717a; --line: #e4e4e7;
        --card: #fafafa; --accent: #0284c7; --code-bg: #f4f4f5;
      }
      * { box-sizing: border-box; }
      html { scroll-behavior: smooth; }
      body { margin: 0; background: var(--bg); color: var(--fg);
        font-family: ui-sans-serif, system-ui, -apple-system, sans-serif; line-height: 1.6; }
      code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.86em; }
      a { color: var(--accent); }
      .nbpm-bar { display: flex; align-items: center; gap: 0.75rem; padding: 0.6rem 1rem;
        background: #f4f4f5; border-bottom: 1px solid var(--line); position: sticky; top: 0; z-index: 10; }
      .nbpm-bar a.brand { display: flex; align-items: center; gap: 0.5rem; text-decoration: none; color: var(--fg); }
      .nbpm-bar strong { font-weight: 600; }
      .nbpm-bar .dot { width: 0.6rem; height: 0.6rem; border-radius: 9999px;
        background: linear-gradient(135deg, #34d399, #38bdf8); }
      .nbpm-bar .sub { color: var(--muted); font-size: 0.85rem; }
      .nbpm-bar .badge { font-size: 0.6rem; font-weight: 700; letter-spacing: 0.1em;
        text-transform: uppercase; color: var(--accent); padding: 0.15rem 0.5rem;
        border: 1px solid color-mix(in srgb, var(--accent) 40%, transparent); border-radius: 9999px;
        background: color-mix(in srgb, var(--accent) 10%, transparent); white-space: nowrap; }
      .nbpm-bar .spacer { flex: 1; }
      .nbpm-bar a.x { color: var(--accent); text-decoration: none; font-size: 0.85rem; }
      .nbpm-bar a.x:hover { text-decoration: underline; }
      .layout { display: flex; align-items: flex-start; max-width: 1180px; margin: 0 auto; }
      .sidebar { width: 250px; flex: 0 0 250px; position: sticky; top: 49px; align-self: flex-start;
        max-height: calc(100vh - 49px); overflow-y: auto; padding: 1.25rem 0.75rem 3rem;
        border-right: 1px solid var(--line); }
      .sidebar .nav-title { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.06em;
        color: var(--muted); font-weight: 700; padding: 0 0.6rem; margin-bottom: 0.4rem; }
      .nav-link { display: block; padding: 0.32rem 0.6rem; border-radius: 6px; text-decoration: none;
        color: #3f3f46; font-size: 0.9rem; }
      .nav-link:hover { background: #f4f4f5; color: var(--fg); }
      .content { min-width: 0; flex: 1; padding: 1.5rem 2rem 5rem; max-width: 820px; }
      .content h1 { font-size: 1.9rem; margin: 0.2rem 0 1rem; line-height: 1.25; }
      .content h2 { font-size: 1.4rem; margin: 2.4rem 0 0.6rem; padding-bottom: 0.3rem;
        border-bottom: 1px solid var(--line); scroll-margin-top: 60px; }
      .content h3 { font-size: 1.12rem; margin: 1.5rem 0 0.4rem; scroll-margin-top: 60px; }
      .content h4 { font-size: 0.98rem; margin: 1.1rem 0 0.3rem; }
      .content p, .content li { color: #27272a; }
      .content a { text-decoration: none; }
      .content a:hover { text-decoration: underline; }
      .content ul, .content ol { padding-left: 1.4rem; }
      .content li { margin: 0.25rem 0; }
      .content :not(pre) > code { background: var(--code-bg); border: 1px solid var(--line);
        border-radius: 4px; padding: 0.05rem 0.32rem; }
      .content pre { background: #18181b; color: #f4f4f5; padding: 0.9rem 1rem; border-radius: 8px;
        overflow-x: auto; font-size: 0.84rem; line-height: 1.5; }
      .content pre code { background: none; border: 0; padding: 0; color: inherit; }
      .content blockquote { margin: 1rem 0; padding: 0.3rem 1rem; border-left: 3px solid var(--accent);
        background: #f8fafc; color: #3f3f46; }
      .content table { border-collapse: collapse; width: 100%; margin: 1rem 0; font-size: 0.9rem; }
      .content th, .content td { border: 1px solid var(--line); padding: 0.4rem 0.6rem; text-align: left; vertical-align: top; }
      .content th { background: var(--card); }
      .content img { max-width: 100%; }
      .content hr { border: 0; border-top: 1px solid var(--line); margin: 2rem 0; }
      @media (max-width: 800px) {
        .layout { flex-direction: column; }
        .sidebar { position: static; width: 100%; max-height: none; border-right: 0;
          border-bottom: 1px solid var(--line); }
        .content { padding: 1.25rem; }
      }
    </style>
  </head>
  <body>
    <div class="nbpm-bar">
      <a class="brand" href="/"><span class="dot"></span><strong>Nano BPM</strong></a>
      <span class="badge">Advanced Research Prototype</span>
      <span class="sub">Whitepaper</span>
      <span class="spacer"></span>
      <a class="x" href="/console">Web console</a>
      <a class="x" href="/docs">Docs</a>
      <a class="x" href="/swagger">REST API</a>
      <a class="x" href="/asyncapi">Falcon protocol</a>
    </div>
    <div class="layout">
      <nav class="sidebar">
        <div class="nav-title">Contents</div>
        ${tocList()}
      </nav>
      <main class="content">
        ${body}
      </main>
    </div>
  </body>
</html>`;
}

// --- 5. Emit -----------------------------------------------------------------
rmSync(outDir, { recursive: true, force: true });
mkdirSync(outDir, { recursive: true });
writeFileSync(join(outDir, "index.html"), shell());
console.log(`generated whitepaper page -> public/whitepaper/ (${toc.length} sections)`);
