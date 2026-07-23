// nanobpmn datasource CLI (Deno) — ADR 0024 phase-2 (DB Manager gateway).
//
// Materialised verbatim to <project>/.nanobpm/data-cli.ts next to data-sdk.ts.
// The Rust console server invokes it as a one-shot subprocess to serve the
// console **Data** panel (Tables / SQL / Migrations). It is the panel's bridge
// to the datasource seam: every operation runs THROUGH `@nanobpm/data`
// (`./data-sdk.ts`), so the panel browses whatever the named datasource
// currently resolves to — embedded SQLite in the IDE, a server DB in
// production once a `nano-ide-data-*` driver pack is installed — never a
// parallel, SQLite-only path (ADR 0024 §4).
//
// Protocol: a single JSON request object is read from stdin; a single JSON
// response object is written to stdout and the process exits 0. Handled errors
// are reported as `{ ok: false, error }` (still exit 0) so the caller can parse
// them; only an unreadable request exits non-zero. The process cwd is the
// project root, so `openDataSource`/`listSources` discover `nano.app.json`
// there and file-backed sources resolve against it.
//
//   Request:  { op, source?, sql?, params? }
//   op = "sources" | "schema" | "query" | "exec" | "migrations" | "migrate"

import { listSources, openDataSource } from "./data-sdk.ts";

interface Request {
  op: string;
  source?: string;
  sql?: string;
  params?: unknown[];
}

async function readStdin(): Promise<string> {
  const chunks: Uint8Array[] = [];
  const reader = Deno.stdin.readable.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) chunks.push(value);
  }
  let len = 0;
  for (const c of chunks) len += c.length;
  const buf = new Uint8Array(len);
  let off = 0;
  for (const c of chunks) {
    buf.set(c, off);
    off += c.length;
  }
  return new TextDecoder().decode(buf);
}

/// JSON can't carry a bigint (SQLite INTEGER/rowid may exceed 2^53). Coerce any
/// bigint a driver returns to a Number when it is safe, else a decimal string,
/// recursively, so the response always serialises.
function jsonSafe(v: unknown): unknown {
  if (typeof v === "bigint") {
    return v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(v)
      : v.toString();
  }
  if (v instanceof Uint8Array) return { $blob: v.length };
  if (Array.isArray(v)) return v.map(jsonSafe);
  if (v && typeof v === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, val] of Object.entries(v)) out[k] = jsonSafe(val);
    return out;
  }
  return v;
}

/// The union of keys seen across the result rows, in first-seen order — the
/// column set a grid renders. SQLite's `.all()` omits a column set for an empty
/// result, so an empty query yields `[]`; that is honest for a table browser.
function columnsOf(rows: Record<string, unknown>[]): string[] {
  const seen = new Set<string>();
  const cols: string[] = [];
  for (const r of rows) {
    for (const k of Object.keys(r)) {
      if (!seen.has(k)) {
        seen.add(k);
        cols.push(k);
      }
    }
  }
  return cols;
}

const MIGRATIONS_TABLE = "_nano_migrations";

/// Split a migration file into individual statements on `;` boundaries,
/// dropping blank and comment-only fragments. A minimal splitter for the
/// common DDL case (ADR 0024's "minimal dialect stance"); it does not parse
/// semicolons inside string literals.
function splitStatements(sql: string): string[] {
  return sql
    .split(";")
    .map((s) => s.trim())
    .filter((s) => s.length > 0 && !s.split("\n").every((l) => l.trim().startsWith("--")));
}

async function migrationDir(source: string): Promise<string> {
  const { sources, default: def } = await listSources();
  const name = source || def || sources[0]?.name;
  const src = sources.find((s) => s.name === name);
  return src?.migrations ?? "db/migrations";
}

/// Ordered `*.sql` files in the source's migrations directory (empty when the
/// directory is absent — a project may declare no migrations).
async function listMigrationFiles(dir: string): Promise<string[]> {
  const names: string[] = [];
  try {
    for await (const e of Deno.readDir(dir)) {
      if (e.isFile && e.name.endsWith(".sql")) names.push(e.name);
    }
  } catch {
    // no migrations directory — treat as an empty set
  }
  names.sort();
  return names;
}

async function run(req: Request): Promise<unknown> {
  switch (req.op) {
    case "sources": {
      const { sources, default: def } = await listSources();
      return { default: def, sources };
    }
    case "schema": {
      const db = await openDataSource(req.source);
      return { tables: await db.schema() };
    }
    case "query": {
      const db = await openDataSource(req.source);
      const rows = (await db.query(req.sql ?? "", req.params ?? [])) as Record<
        string,
        unknown
      >[];
      return { columns: columnsOf(rows), rows: jsonSafe(rows) };
    }
    case "exec": {
      const db = await openDataSource(req.source);
      const r = await db.exec(req.sql ?? "", req.params ?? []);
      return jsonSafe(r);
    }
    case "migrations": {
      const dir = await migrationDir(req.source ?? "");
      const files = await listMigrationFiles(dir);
      const db = await openDataSource(req.source);
      await db.exec(
        `CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (name TEXT PRIMARY KEY, applied_at TEXT NOT NULL)`,
      );
      const applied = new Map<string, string>(
        (await db.query(`SELECT name, applied_at FROM ${MIGRATIONS_TABLE}`)).map((
          r,
        ) => [String(r.name), String(r.applied_at)]),
      );
      return {
        dir,
        entries: files.map((name) => ({
          name,
          applied: applied.has(name),
          appliedAt: applied.get(name) ?? null,
        })),
      };
    }
    case "migrate": {
      const dir = await migrationDir(req.source ?? "");
      const files = await listMigrationFiles(dir);
      const db = await openDataSource(req.source);
      await db.exec(
        `CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (name TEXT PRIMARY KEY, applied_at TEXT NOT NULL)`,
      );
      const done = new Set(
        (await db.query(`SELECT name FROM ${MIGRATIONS_TABLE}`)).map((r) =>
          String(r.name)
        ),
      );
      const applied: string[] = [];
      for (const name of files) {
        if (done.has(name)) continue;
        const sql = await Deno.readTextFile(`${dir}/${name}`);
        await db.tx(async (t) => {
          for (const stmt of splitStatements(sql)) await t.exec(stmt);
          await t.exec(
            `INSERT INTO ${MIGRATIONS_TABLE} (name, applied_at) VALUES (?, ?)`,
            [name, new Date().toISOString()],
          );
        });
        applied.push(name);
      }
      return { applied, pending: 0 };
    }
    default:
      throw new Error(`unknown op "${req.op}"`);
  }
}

async function main(): Promise<void> {
  let req: Request;
  try {
    req = JSON.parse(await readStdin()) as Request;
  } catch (e) {
    console.log(JSON.stringify({ ok: false, error: `bad request: ${(e as Error).message}` }));
    Deno.exit(1);
  }
  try {
    const result = await run(req);
    console.log(JSON.stringify({ ok: true, ...(result as object) }));
  } catch (e) {
    console.log(JSON.stringify({ ok: false, error: (e as Error).message }));
  }
}

await main();
