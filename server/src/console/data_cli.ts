// nanobpmn datasource CLI (Deno-preferred, Node-capable) — ADR 0024 phase-2
// (DB Manager gateway); dual-runtime per ADR 0036/0038.
//
// Materialised verbatim to <project>/nano-generated/data-cli.ts next to data-sdk.ts.
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
//   Request:  { op, source?, sql?, params?, statements? }
//   op = "sources" | "schema" | "query" | "exec" | "script" | "migrations"
//      | "migrate" | "domaintypes"

import { listSources, manifestTypes, manifestWorkers, openDataSource, type WorkerDecl } from "./data-sdk.ts";
import {
  DOMAIN_BINDINGS,
  DOMAIN_DTS,
  type DomainTypeRegistry,
  emitDomainBindings,
  emitDomainModel,
  emitMessageBindings,
  emitMessageBindingsRuntime,
  emitWorkerBindings,
  emitWorkerBindingsRuntime,
  GEN_DIR,
  MESSAGE_BINDINGS_DTS,
  MESSAGE_BINDINGS_TS,
  type MessageBindingDecl,
  type SourceSchema,
  WORKER_BINDINGS_DTS,
  WORKER_BINDINGS_TS,
} from "./domain-types.ts";

interface Request {
  op: string;
  source?: string;
  sql?: string;
  params?: unknown[];
  statements?: string[];
  /** `domaintypes`: also write `nano-generated/domain-rows.d.ts` (default true). */
  write?: boolean;
  /**
   * `domaintypes`: the model-derived worker-IO map (`taskType -> {in,out}`),
   * scanned from the process models by the server (ADR 0040 slice 1 / ADR 0033
   * §6 increment 12). When present, it is the authoritative source of each
   * worker's envelope types; it is overlaid on the manifest `workers[]` (which
   * still carries non-IO bindings such as `llm`). Absent → fall back to the
   * manifest projection alone.
   */
  derivedWorkers?: WorkerDecl[];
  /**
   * `domaintypes`: the model-derived message-payload map (`messageName ->
   * {in,out}`), scanned from the process models' `bpmn:message` envelopes by the
   * server (ADR 0040 slice 2). It is authoritative for the typed `publishMessage`
   * registry (there is no manifest projection for messages, unlike `workers[]`),
   * so it is emitted directly. Absent → no message registry is derived.
   */
  derivedMessages?: MessageBindingDecl[];
}

/**
 * Overlay the model-derived worker-IO map onto the manifest `workers[]`. The
 * model is authoritative for the envelope (`inputType`/`outputType`), so every
 * scanned `taskType` takes its I/O from `derived` (clearing a stale manifest
 * value when the model carries none); manifest entries the scan did not cover
 * (e.g. a worker declared only for an `llm` binding, with no service task) are
 * preserved. New `taskType`s seen only in the model are appended.
 */
export function overlayDerivedWorkerIo(
  manifest: WorkerDecl[],
  derived: WorkerDecl[],
): WorkerDecl[] {
  const byType = new Map<string, WorkerDecl>();
  for (const w of manifest) byType.set(w.taskType, { ...w });
  for (const d of derived) {
    const existing = byType.get(d.taskType);
    const merged: WorkerDecl = existing
      ? { ...existing, taskType: d.taskType }
      : { taskType: d.taskType };
    if (d.inputType) merged.inputType = d.inputType;
    else delete merged.inputType;
    if (d.outputType) merged.outputType = d.outputType;
    else delete merged.outputType;
    byType.set(d.taskType, merged);
  }
  return [...byType.values()];
}

// Runtime adapter (ADR 0036/0038): the host calls that differ between Deno
// (`Deno.*`) and Node (`process` / `node:fs`). The DB Manager gateway runs under
// whichever runtime the console picked — Deno preferred, Node >= 22.6 fallback
// (32-bit ARM ships no Deno build). `data-sdk.ts` already carries the same shim
// for SQLite/`node:sqlite`; this closes the gateway's own host calls (stdin,
// readDir, readTextFile, exit) so the Data panel works with no Deno present.
interface DirEntry {
  name: string;
  isFile: boolean;
}
interface CliRuntime {
  readStdin(): Promise<string>;
  readDir(dir: string): Promise<DirEntry[]>;
  readTextFile(path: string): Promise<string>;
  mkdir(path: string): Promise<void>;
  writeTextFile(path: string, data: string): Promise<void>;
  exit(code: number): never;
}

function concatDecode(chunks: Uint8Array[]): string {
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

const RT: CliRuntime = ((): CliRuntime => {
  const g = globalThis as unknown as {
    Deno?: {
      stdin: { readable: ReadableStream<Uint8Array> };
      readDir(dir: string): AsyncIterable<{ name: string; isFile: boolean }>;
      readTextFile(p: string): Promise<string>;
      exit(code: number): never;
    };
    process?: { stdin: AsyncIterable<Uint8Array>; exit(code: number): never };
  };
  if (g.Deno) {
    const d = g.Deno as typeof g.Deno & {
      mkdir(p: string, o: { recursive: boolean }): Promise<void>;
      writeTextFile(p: string, data: string): Promise<void>;
    };
    return {
      readStdin: async () => {
        const chunks: Uint8Array[] = [];
        const reader = d.stdin.readable.getReader();
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          if (value) chunks.push(value);
        }
        return concatDecode(chunks);
      },
      readDir: async (dir) => {
        const out: DirEntry[] = [];
        for await (const e of d.readDir(dir)) out.push({ name: e.name, isFile: e.isFile });
        return out;
      },
      readTextFile: (p) => d.readTextFile(p),
      mkdir: (p) => d.mkdir(p, { recursive: true }),
      writeTextFile: (p, data) => d.writeTextFile(p, data),
      exit: (c) => d.exit(c),
    };
  }
  const p = g.process!;
  return {
    readStdin: async () => {
      const chunks: Uint8Array[] = [];
      for await (const c of p.stdin) chunks.push(c as Uint8Array);
      return concatDecode(chunks);
    },
    readDir: async (dir) => {
      const { readdir } = await import("node:fs/promises");
      const ents = await readdir(dir, { withFileTypes: true });
      return ents.map((e) => ({ name: e.name, isFile: e.isFile() }));
    },
    readTextFile: async (path) => (await import("node:fs/promises")).readFile(path, "utf8"),
    mkdir: async (path) => {
      await (await import("node:fs/promises")).mkdir(path, { recursive: true });
    },
    writeTextFile: async (path, data) => {
      await (await import("node:fs/promises")).writeFile(path, data, "utf8");
    },
    exit: (c) => p.exit(c),
  };
})();

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
    for (const e of await RT.readDir(dir)) {
      if (e.isFile && e.name.endsWith(".sql")) names.push(e.name);
    }
  } catch {    // no migrations directory — treat as an empty set
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
    case "script": {
      // Run several statements atomically in one transaction — the console's
      // structure editor uses this for the SQLite 12-step table rebuild
      // (create → copy → drop → rename), so a mid-rebuild failure rolls back
      // and leaves the table untouched. Statements come pre-split from the
      // caller; `?`-params are not threaded (DDL needs none).
      const db = await openDataSource(req.source);
      const statements = Array.isArray(req.statements)
        ? req.statements
        : splitStatements(req.sql ?? "");
      let changed = 0;
      await db.tx(async (t) => {
        for (const stmt of statements) {
          if (stmt.trim()) changed += (await t.exec(stmt)).changed;
        }
      });
      return jsonSafe({ changed });
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
        const sql = await RT.readTextFile(`${dir}/${name}`);
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
    case "domaintypes": {
      // ADR 0029 §4.1/§4.2/§6: reify the domain model into TypeScript. Union
      // *every* declared datasource (the table spine) and fold in the manifest
      // `types` registry (transient/non-persisted shapes) so an App with
      // multiple databases plus declared types gets one complete domain model,
      // emit `domain-rows.d.ts`, and (unless `write:false`) materialise it to
      // `nano-generated/domain-rows.d.ts` next to the SDK so workers type against the live
      // DBs. cwd is the project root, so the relative path lands in the project.
      const { default: def, sources } = await listSources();
      const schemas: SourceSchema[] = [];
      for (const s of sources) {
        const db = await openDataSource(s.name);
        try {
          schemas.push({ source: s.name, tables: await db.schema() });
        } finally {
          db.close();
        }
      }
      const types = await manifestTypes() as DomainTypeRegistry;
      const text = emitDomainModel(schemas, def, types);
      // The typed data-object accessor (`db.orders.insert(...)`, ADR 0029 §6) is
      // generated alongside the `.d.ts` spine so workers get both the row types
      // and the runtime gateway from one op.
      const bindings = emitDomainBindings(schemas, def);
      // The worker-IO map (ADR 0033 §3): `taskType → {in,out}`. The model is the
      // source of truth for the envelope, so when the server injected the
      // model-derived map (`derivedWorkers`) it is overlaid on the manifest
      // `workers[]` (which still carries non-IO bindings). This retires the
      // hand-maintained projection as the authority (ADR 0040 slice 1). The
      // runtime wrapper (`workers.ts`) is static — written verbatim.
      const manifestW = await manifestWorkers();
      const workers = req.derivedWorkers
        ? overlayDerivedWorkerIo(manifestW, req.derivedWorkers)
        : manifestW;
      const workerBindings = emitWorkerBindings(workers, Object.keys(types));
      const workerRuntime = emitWorkerBindingsRuntime();
      // The message-payload registry (ADR 0040 slice 2): `messageName → {in,out}`,
      // scanned from the process models' `bpmn:message` envelopes. There is no
      // manifest projection for messages, so the model-derived map is authoritative
      // and emitted directly (no overlay). Feeds the typed `publishMessage`; the
      // runtime wrapper (`messages.ts`) is static — written verbatim.
      const messages = req.derivedMessages ?? [];
      const messageBindings = emitMessageBindings(messages, Object.keys(types));
      const messageRuntime = emitMessageBindingsRuntime();
      let path: string | null = null;
      let bindingsPath: string | null = null;
      let workerBindingsPath: string | null = null;
      let messageBindingsPath: string | null = null;
      if (req.write !== false) {
        await RT.mkdir(GEN_DIR);
        path = `${GEN_DIR}/${DOMAIN_DTS}`;
        await RT.writeTextFile(path, text);
        bindingsPath = `${GEN_DIR}/${DOMAIN_BINDINGS}`;
        await RT.writeTextFile(bindingsPath, bindings);
        workerBindingsPath = `${GEN_DIR}/${WORKER_BINDINGS_DTS}`;
        await RT.writeTextFile(workerBindingsPath, workerBindings);
        await RT.writeTextFile(`${GEN_DIR}/${WORKER_BINDINGS_TS}`, workerRuntime);
        messageBindingsPath = `${GEN_DIR}/${MESSAGE_BINDINGS_DTS}`;
        await RT.writeTextFile(messageBindingsPath, messageBindings);
        await RT.writeTextFile(`${GEN_DIR}/${MESSAGE_BINDINGS_TS}`, messageRuntime);
      }
      const tables = schemas.reduce((n, s) => n + s.tables.length, 0);
      return {
        path,
        text,
        tables,
        bindingsPath,
        bindings,
        workerBindingsPath,
        workerBindings,
        messageBindingsPath,
        messageBindings,
      };
    }
    default:
      throw new Error(`unknown op "${req.op}"`);
  }
}

async function main(): Promise<void> {
  let req: Request;
  try {
    req = JSON.parse(await RT.readStdin()) as Request;
  } catch (e) {
    console.log(JSON.stringify({ ok: false, error: `bad request: ${(e as Error).message}` }));
    RT.exit(1);
  }
  try {
    const result = await run(req);
    console.log(JSON.stringify({ ok: true, ...(result as object) }));
  } catch (e) {
    console.log(JSON.stringify({ ok: false, error: (e as Error).message }));
  }
}

await main();
