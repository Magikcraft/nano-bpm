// The Urban App page runtime (ADR 0042 §3) — materialised into an app as
// `nano-generated/app-pages.ts` and imported as `@nanobpm/app`. It turns a composed
// `page.json` (authored in the console Page Composer) into a served, data-bound
// screen with no hand-written frontend or API.
//
// It imports neither `@nanobpm/data` nor `@nanobpm/nano-sdk`: the app's `main.ts`
// passes the already-opened datasource + engine client in, so this module stays
// self-contained, testable (`app_pages_test.ts`), and free of the hyphenated
// emit-time specifiers that make `data-cli.ts` un-typecheckable in-tree.
//
// Endpoints (ADR 0026 §1 action API, `start`-only subset for v1):
//   GET  /app/pages/<id>                → the page's page.json
//   GET  /app/data/<source>/<table>     → rows from the datasource (the rest bank)
//   POST /app/actions/start/<process>   → createProcessInstance (attended-sync)
//   GET  /  (+ /app/runtime.js)         → the schema-driven browser renderer

/** The subset of the `@nanobpm/data` DataSource the runtime needs. */
export interface PagesDataSource {
  query(sql: string, params?: unknown[]): Promise<Record<string, unknown>[]>;
  schema(): Promise<{ name: string }[]>;
}

/** The subset of the `@nanobpm/nano-sdk` engine client the runtime needs. */
export interface PagesEngine {
  createProcessInstance(
    input: { processDefinitionId: string; variables?: Record<string, unknown> },
  ): Promise<{ processInstanceKey?: string | number }>;
}

export interface PagesContext {
  db: PagesDataSource;
  nano: PagesEngine;
  /** Directory holding `*.page.json` (relative to the app root). Default `pages`. */
  pagesDir?: string;
  /** The page served at `/`. Default `home`. */
  homePage?: string;
  /** Max rows a `dataGrid` fetch returns. Default 200. */
  rowLimit?: number;
  /** Read a page file; injectable for tests. Defaults to `Deno.readTextFile`. */
  readPage?: (path: string) => Promise<string>;
}

const json = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

/** A SQL identifier guard — a table name must be a bare identifier *and* a known
 * table (checked against `schema()`), so `/app/data/:table` can never inject SQL. */
const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * The request handler, factored out of `servePages` so it is pure over its context
 * (injected `db`/`nano`/`readPage`) and unit-testable without a real `Deno.serve`.
 */
export function createPagesHandler(ctx: PagesContext): (req: Request) => Promise<Response> {
  const pagesDir = ctx.pagesDir ?? "pages";
  const homePage = ctx.homePage ?? "home";
  const rowLimit = ctx.rowLimit ?? 200;
  const readPage = ctx.readPage ?? ((p: string) => Deno.readTextFile(p));

  return async function handle(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const { pathname } = url;

    // ── the renderer shell ────────────────────────────────────────────────
    if (req.method === "GET" && (pathname === "/" || pathname === "/index.html")) {
      return new Response(rendererShell(homePage), {
        headers: { "content-type": "text/html; charset=utf-8" },
      });
    }
    if (req.method === "GET" && pathname === "/app/runtime.js") {
      return new Response(RENDERER_JS, {
        headers: { "content-type": "text/javascript; charset=utf-8" },
      });
    }

    // ── GET /app/pages/<id> ───────────────────────────────────────────────
    const pageMatch = pathname.match(/^\/app\/pages\/([A-Za-z0-9_-]+)$/);
    if (req.method === "GET" && pageMatch) {
      const id = pageMatch[1];
      try {
        const text = await readPage(`${pagesDir}/${id}.page.json`);
        return new Response(text, { headers: { "content-type": "application/json" } });
      } catch {
        return json({ error: `page "${id}" not found` }, 404);
      }
    }

    // ── GET /app/data/<source>/<table> ────────────────────────────────────
    const dataMatch = pathname.match(/^\/app\/data\/([A-Za-z0-9_-]+)\/([A-Za-z0-9_]+)$/);
    if (req.method === "GET" && dataMatch) {
      const table = dataMatch[2];
      if (!IDENT.test(table)) return json({ error: "invalid table name" }, 400);
      const tables = await ctx.db.schema();
      if (!tables.some((t) => t.name === table)) {
        return json({ error: `unknown table "${table}"` }, 404);
      }
      const rows = await ctx.db.query(`SELECT * FROM ${table} LIMIT ${rowLimit}`);
      return json({ rows });
    }

    // ── POST /app/actions/start/<process> ─────────────────────────────────
    const startMatch = pathname.match(/^\/app\/actions\/start\/([A-Za-z0-9_.-]+)$/);
    if (req.method === "POST" && startMatch) {
      const process = startMatch[1];
      let variables: Record<string, unknown> = {};
      try {
        const body = await req.json();
        if (body && typeof body === "object") {
          variables = (body as { variables?: Record<string, unknown> }).variables ?? {};
        }
      } catch {
        return json({ error: "body must be JSON" }, 400);
      }
      try {
        const res = await ctx.nano.createProcessInstance({ processDefinitionId: process, variables });
        return json({ processInstanceKey: res.processInstanceKey ?? null });
      } catch (e) {
        return json({ error: String((e as Error)?.message ?? e) }, 502);
      }
    }

    return json({ error: "not found" }, 404);
  };
}

/**
 * Start the page runtime on `port`. Returns the `Deno.HttpServer` handle. The app's
 * `main.ts` typically does: `servePages({ db, nano, port })`.
 */
export function servePages(ctx: PagesContext & { port?: number }): Deno.HttpServer {
  const handle = createPagesHandler(ctx);
  return Deno.serve({ port: ctx.port ?? 8090 }, handle);
}

function rendererShell(homePage: string): string {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Urban App</title>
  <style>${RENDERER_CSS}</style>
</head>
<body>
  <main id="page" data-home="${homePage}"><p class="pc-empty">Loading…</p></main>
  <script type="module" src="/app/runtime.js"></script>
</body>
</html>`;
}

const RENDERER_CSS = `
:root { color-scheme: light dark; --pc-edge:#d0d0d8; --pc-accent:#3b5bdb; }
* { box-sizing: border-box; }
body { margin:0; font:15px/1.5 system-ui,sans-serif; padding:2rem; max-width:64rem; margin-inline:auto; }
.pc-empty { opacity:.6; }
.pc-heading { font-size:1.6rem; font-weight:650; margin:0 0 .25rem; }
.pc-sub { opacity:.7; margin:.25rem 0 1rem; }
.pc-body { margin:.5rem 0; }
.pc-card { border:1px solid var(--pc-edge); border-radius:.6rem; padding:1rem 1.15rem; margin:1rem 0; }
.pc-card h2 { font-size:1rem; margin:0 0 .75rem; }
.pc-field { display:flex; flex-direction:column; gap:.25rem; margin-bottom:.6rem; }
.pc-field label { font-size:.8rem; opacity:.75; }
.pc-field input { padding:.5rem .6rem; border:1px solid var(--pc-edge); border-radius:.4rem; font:inherit; }
.pc-btn { padding:.5rem .9rem; border:0; border-radius:.4rem; background:var(--pc-accent); color:#fff; font:inherit; cursor:pointer; }
.pc-btn:disabled { opacity:.5; cursor:default; }
.pc-msg { font-size:.85rem; margin-top:.5rem; min-height:1.2em; }
.pc-msg.err { color:#c0392b; }
.pc-msg.ok { color:#2b8a3e; }
table.pc-grid { width:100%; border-collapse:collapse; font-size:.9rem; }
table.pc-grid th, table.pc-grid td { text-align:left; padding:.4rem .6rem; border-bottom:1px solid var(--pc-edge); }
table.pc-grid th { font-weight:600; opacity:.75; }
`;

// The schema-driven browser renderer (ADR 0042 §3). Plain ES module string served at
// /app/runtime.js — it does NOT ship Craft.js (authoring is console-side only). It
// fetches the home page's page.json and renders text / actionForm / dataGrid nodes,
// wiring actionForm → /app/actions/start and dataGrid → /app/data (with a refresh).
const RENDERER_JS = String.raw`
const root = document.getElementById("page");
const HOME = root.dataset.home || "home";

async function getJSON(url, opts) {
  const r = await fetch(url, opts);
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(body.error || ("HTTP " + r.status));
  return body;
}

function el(tag, attrs = {}, ...kids) {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") n.className = v;
    else if (k.startsWith("on") && typeof v === "function") n.addEventListener(k.slice(2), v);
    else if (v != null) n.setAttribute(k, v);
  }
  for (const kid of kids) n.append(kid);
  return n;
}

function renderText(node) {
  const v = node.props.variant;
  const cls = v === "heading" ? "pc-heading" : v === "sub" ? "pc-sub" : "pc-body";
  return el(v === "heading" ? "h1" : "p", { class: cls }, node.props.text || "");
}

function renderActionForm(node) {
  const p = node.props;
  const card = el("section", { class: "pc-card" });
  if (p.title) card.append(el("h2", {}, p.title));
  const inputs = {};
  for (const f of p.fields || []) {
    const input = el("input", { type: "text", placeholder: f.label || f.key });
    inputs[f.key] = input;
    card.append(el("div", { class: "pc-field" }, el("label", {}, f.label || f.key), input));
  }
  const msg = el("p", { class: "pc-msg" });
  const btn = el("button", { class: "pc-btn" }, p.submitLabel || "Submit");
  btn.addEventListener("click", async () => {
    const variables = {};
    for (const [k, input] of Object.entries(inputs)) variables[k] = input.value;
    btn.disabled = true; msg.className = "pc-msg"; msg.textContent = "Submitting…";
    try {
      const res = await getJSON("/app/actions/start/" + encodeURIComponent(p.action.process),
        { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ variables }) });
      msg.className = "pc-msg ok";
      msg.textContent = "Started (instance " + (res.processInstanceKey ?? "?") + ")";
      for (const input of Object.values(inputs)) input.value = "";
      document.dispatchEvent(new CustomEvent("pc:refresh"));
    } catch (e) {
      msg.className = "pc-msg err"; msg.textContent = String(e.message || e);
    } finally { btn.disabled = false; }
  });
  card.append(btn, msg);
  return card;
}

function renderDataGrid(node) {
  const p = node.props;
  const card = el("section", { class: "pc-card" });
  if (p.title) card.append(el("h2", {}, p.title));
  const cols = p.columns || [];
  const thead = el("thead", {}, el("tr", {}, ...cols.map((c) => el("th", {}, c.header || c.field))));
  const tbody = el("tbody", {});
  const table = el("table", { class: "pc-grid" }, thead, tbody);
  card.append(table);
  async function refresh() {
    try {
      const { rows } = await getJSON("/app/data/" + encodeURIComponent(p.data.source) + "/" + encodeURIComponent(p.data.table));
      tbody.replaceChildren(...rows.map((row) =>
        el("tr", {}, ...cols.map((c) => el("td", {}, row[c.field] == null ? "" : String(row[c.field]))))));
      if (!rows.length) tbody.append(el("tr", {}, el("td", { colspan: String(cols.length || 1) }, "No rows")));
    } catch (e) {
      tbody.replaceChildren(el("tr", {}, el("td", { colspan: String(cols.length || 1) }, String(e.message || e))));
    }
  }
  document.addEventListener("pc:refresh", refresh);
  refresh();
  return card;
}

const RENDERERS = { text: renderText, actionForm: renderActionForm, dataGrid: renderDataGrid };

async function main() {
  try {
    const doc = await getJSON("/app/pages/" + HOME);
    if (doc.title) document.title = doc.title;
    root.replaceChildren(...(doc.nodes || []).map((n) => (RENDERERS[n.type] || (() => el("div")))(n)));
  } catch (e) {
    root.replaceChildren(el("p", { class: "pc-msg err" }, "Failed to load page: " + String(e.message || e)));
  }
}
main();
`;
