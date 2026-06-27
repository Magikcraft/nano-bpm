// Generates the Command Stream Protocol reference (AsyncAPI) as a single
// self-contained, offline HTML page, rendered at build time directly from the
// hand-maintained spec at ../docs/command-stream.asyncapi.yaml. Vite ships the
// result in dist/ (and the gateway embeds it) so /asyncapi works fully offline,
// mirroring the Swagger UI pipeline for the REST API.
//
// The spec under ../docs is the single source of truth; the generated HTML is
// git-ignored. Re-run via `npm run build` (or directly) to refresh it.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import yaml from "js-yaml";
import MarkdownIt from "markdown-it";

// Descriptions/summaries in the spec are authored in Markdown (bold, inline
// code, bullet lists). Render them to HTML instead of dumping the literals.
const md = new MarkdownIt({ html: false, linkify: true, breaks: false });

const root = process.cwd();
const specPath = join(root, "..", "docs", "command-stream.asyncapi.yaml");
const outDir = join(root, "public", "asyncapi");
const doc = yaml.load(readFileSync(specPath, "utf8"));

// --- minimal JSON-pointer $ref resolution within the single document --------
function resolveRef(node) {
  let cur = node;
  // Follow chained `$ref`s (message -> payload schema, etc.).
  while (cur && typeof cur === "object" && typeof cur.$ref === "string") {
    const ptr = cur.$ref.replace(/^#\//, "").split("/");
    let target = doc;
    for (const seg of ptr) {
      target = target?.[decodeURIComponent(seg.replace(/~1/g, "/").replace(/~0/g, "~"))];
    }
    cur = target;
  }
  return cur;
}

const esc = (s) =>
  String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

// Render a Markdown description as a full HTML block (paragraphs, lists, code).
const mdBlock = (s) => md.render(String(s ?? "").trim());

// Render a short Markdown description inline (no wrapping <p>), collapsing the
// folded-YAML line breaks of single-paragraph descriptions into spaces.
const prose = (s) => md.renderInline(String(s ?? "").replace(/\s*\n\s*/g, " ").trim());

// --- render one payload schema as a property table --------------------------
function schemaType(p) {
  const s = resolveRef(p) ?? {};
  if (s.const !== undefined) return `const <code>${esc(JSON.stringify(s.const))}</code>`;
  let t = s.type ?? "any";
  if (t === "array") {
    const items = resolveRef(s.items) ?? {};
    t = `${items.type ?? "any"}[]`;
  }
  if (s.format) t += ` <span class="fmt">(${esc(s.format)})</span>`;
  if (s.nullable) t += ' <span class="nullable">· nullable</span>';
  return t;
}

function renderSchema(schemaRef) {
  const schema = resolveRef(schemaRef);
  if (!schema || !schema.properties) {
    return '<p class="muted">No structured payload.</p>';
  }
  const required = new Set(schema.required ?? []);
  const rows = Object.entries(schema.properties)
    .map(([name, propRaw]) => {
      const prop = resolveRef(propRaw) ?? {};
      const req = required.has(name)
        ? '<span class="req">required</span>'
        : '<span class="opt">optional</span>';
      const extras = [];
      if (prop.default !== undefined) extras.push(`default <code>${esc(JSON.stringify(prop.default))}</code>`);
      const desc = [prose(prop.description), ...extras].filter(Boolean).join(" · ");
      return `<tr>
        <td class="pname">${esc(name)}</td>
        <td class="ptype">${schemaType(propRaw)}</td>
        <td class="preq">${req}</td>
        <td class="pdesc">${desc}</td>
      </tr>`;
    })
    .join("\n");
  return `<table class="props">
    <thead><tr><th>Field</th><th>Type</th><th></th><th>Description</th></tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
}

// --- render one message card ------------------------------------------------
function renderMessage(msgRef) {
  const msg = resolveRef(msgRef) ?? {};
  const schema = resolveRef(msg.payload);
  const discriminator = schema?.properties?.type?.const ?? msg.name ?? "";
  const summary = prose(msg.summary);
  const description = prose(msg.description);
  return `<section class="msg" id="msg-${esc(discriminator)}">
    <div class="msg-head">
      <code class="tag">type: "${esc(discriminator)}"</code>
      <h3>${esc(msg.title ?? msg.name ?? discriminator)}</h3>
    </div>
    ${summary ? `<p class="summary">${summary}</p>` : ""}
    ${description ? `<p class="desc">${description}</p>` : ""}
    ${renderSchema(msg.payload)}
  </section>`;
}

function renderOperation(op) {
  const messages = (op.messages ?? []).map((m) => renderMessage(m)).join("\n");
  return `<div class="op">
    <div class="op-head">
      <span class="op-action ${op.action}">${op.action === "send" ? "client → server" : "server → client"}</span>
      <h2>${esc(op.title ?? "")}</h2>
    </div>
    ${op.description ? `<p class="op-desc">${prose(op.description)}</p>` : ""}
    ${messages}
  </div>`;
}

const info = doc.info ?? {};
const servers = Object.entries(doc.servers ?? {})
  .map(([id, s]) => {
    const proto = esc(s.protocol);
    const url = `${proto}://${esc(s.host)}${esc(s.pathname ?? "")}`;
    return `<li><code>${url}</code> — ${prose(s.description)} <span class="muted">(${esc(id)})</span></li>`;
  })
  .join("\n");

const operations = Object.values(doc.operations ?? {})
  .map((op) => renderOperation(op))
  .join("\n");

const html = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Nano BPM · Command Stream Protocol</title>
    <style>
      :root {
        --bg: #ffffff; --fg: #18181b; --muted: #71717a; --line: #e4e4e7;
        --card: #fafafa; --accent: #0284c7; --send: #7c3aed; --recv: #0d9488;
      }
      * { box-sizing: border-box; }
      body { margin: 0; background: var(--bg); color: var(--fg);
        font-family: ui-sans-serif, system-ui, -apple-system, sans-serif; line-height: 1.55; }
      code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.85em; }
      .nbpm-bar { display: flex; align-items: center; gap: 0.75rem; padding: 0.6rem 1rem;
        background: #f4f4f5; border-bottom: 1px solid var(--line); color: var(--fg); position: sticky; top: 0; z-index: 5; }
      .nbpm-bar strong { font-weight: 600; }
      .nbpm-bar .dot { width: 0.6rem; height: 0.6rem; border-radius: 9999px;
        background: linear-gradient(135deg, #34d399, #38bdf8); }
      .nbpm-bar a { color: var(--accent); text-decoration: none; font-size: 0.85rem; }
      .nbpm-bar a:hover { text-decoration: underline; }
      .nbpm-bar .spacer { flex: 1; }
      .wrap { max-width: 920px; margin: 0 auto; padding: 1.5rem 1.25rem 4rem; }
      h1 { font-size: 1.6rem; margin: 0.8rem 0 0.3rem; }
      .ver { color: var(--muted); font-size: 0.85rem; }
      .lead { color: #3f3f46; }
      .lead p { margin: 0.5rem 0; }
      .lead ul { margin: 0.5rem 0; padding-left: 1.2rem; }
      .lead li { margin: 0.2rem 0; }
      .lead code, .pdesc code, .op-desc code, .desc code, .summary code {
        background: #f4f4f5; border: 1px solid var(--line); border-radius: 4px; padding: 0.05rem 0.3rem; }
      h2 { font-size: 1.15rem; margin: 1.4rem 0 0.4rem; }
      .servers { list-style: none; padding: 0; margin: 0.5rem 0 1.5rem; }
      .servers li { padding: 0.25rem 0; border-bottom: 1px dashed var(--line); font-size: 0.9rem; }
      .op { margin: 2.2rem 0; }
      .op-head { display: flex; align-items: center; gap: 0.6rem; border-bottom: 2px solid var(--line); padding-bottom: 0.3rem; }
      .op-head h2 { margin: 0; }
      .op-action { font-size: 0.7rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em;
        padding: 0.15rem 0.5rem; border-radius: 9999px; color: #fff; }
      .op-action.send { background: var(--send); }
      .op-action.receive { background: var(--recv); }
      .op-desc { color: #3f3f46; font-size: 0.92rem; }
      .msg { background: var(--card); border: 1px solid var(--line); border-radius: 10px;
        padding: 0.9rem 1rem; margin: 0.9rem 0; }
      .msg-head { display: flex; align-items: baseline; gap: 0.6rem; flex-wrap: wrap; }
      .msg-head h3 { margin: 0; font-size: 1rem; }
      .tag { background: #18181b; color: #fafafa; padding: 0.1rem 0.45rem; border-radius: 6px; font-size: 0.78rem; }
      .summary { margin: 0.4rem 0 0.2rem; font-weight: 500; }
      .desc { margin: 0.2rem 0 0.5rem; color: #3f3f46; font-size: 0.9rem; }
      table.props { width: 100%; border-collapse: collapse; margin-top: 0.5rem; font-size: 0.86rem; }
      table.props th { text-align: left; color: var(--muted); font-weight: 600; font-size: 0.74rem;
        text-transform: uppercase; letter-spacing: 0.03em; border-bottom: 1px solid var(--line); padding: 0.3rem 0.5rem; }
      table.props td { padding: 0.35rem 0.5rem; border-bottom: 1px solid var(--line); vertical-align: top; }
      .pname { font-family: ui-monospace, monospace; font-weight: 600; white-space: nowrap; }
      .ptype { color: #be185d; white-space: nowrap; }
      .fmt { color: var(--muted); }
      .nullable { color: var(--muted); }
      .req { color: #b91c1c; font-size: 0.72rem; font-weight: 700; }
      .opt { color: var(--muted); font-size: 0.72rem; }
      .pdesc { color: #3f3f46; }
      .muted { color: var(--muted); }
      .note { background: #f0f9ff; border: 1px solid #bae6fd; border-radius: 8px; padding: 0.7rem 0.9rem;
        font-size: 0.85rem; color: #075985; margin: 1rem 0; }
    </style>
  </head>
  <body>
    <div class="nbpm-bar">
      <span class="dot"></span>
      <strong>Nano BPM</strong>
      <span class="muted" style="font-size: 0.85rem">Command Stream Protocol</span>
      <span class="spacer"></span>
      <a href="/">Home</a>
      <a href="/docs">Docs</a>
      <a href="/swagger">REST API</a>
      <a href="/console">Web console</a>
    </div>
    <div class="wrap">
      <h1>${esc(info.title ?? "Command Stream")}</h1>
      <div class="ver">AsyncAPI ${esc(doc.asyncapi ?? "")} · version ${esc(info.version ?? "")}</div>
      <div class="lead">${mdBlock(info.description)}</div>
      <div class="note">
        Generated from <code>docs/command-stream.asyncapi.yaml</code> at build time.
        The command stream is a WebSocket protocol (it cannot be modelled by OpenAPI);
        the spec mirrors the <code>ClientFrame</code> / <code>ServerFrame</code> enums in
        <code>server/src/command_stream.rs</code> and is enforced against them by a drift-guard test.
      </div>
      <h2>Servers</h2>
      <ul class="servers">${servers}</ul>
      ${operations}
    </div>
  </body>
</html>`;

mkdirSync(outDir, { recursive: true });
writeFileSync(join(outDir, "index.html"), html);
const opCount = Object.keys(doc.operations ?? {}).length;
console.log(`generated AsyncAPI docs -> public/asyncapi/index.html (${opCount} operations)`);
