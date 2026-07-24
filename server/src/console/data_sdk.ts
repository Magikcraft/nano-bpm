// nanobpmn embedded datasource SDK (Deno-preferred, Node-capable) — ADR 0024.
//
// Materialised verbatim to <workspace>/<project>/.nanobpm/data-sdk.ts and
// imported as `@nanobpm/data` (or `./data-sdk.ts` from the sibling worker SDK).
// It is the runtime half of Urban's "BDE alias": a named, swappable data
// connection. Consumers bind to a datasource BY NAME (`data.app`), never by
// driver, so the same App bundle runs on embedded SQLite in the IDE and — once
// a `nano-ide-data-*` driver pack is installed — on a server database in
// production by flipping `NANO_APP_DB_*` env, with no source change.
//
//   import { openDataSource } from "@nanobpm/data";
//   const db = await openDataSource();          // the manifest's default source
//   await db.exec("INSERT INTO orders(id) VALUES (?)", [id]);
//   const rows = await db.query("SELECT * FROM orders");
//
// Or, inside a worker handler, via the injected context:
//
//   defineWorker({ type: "save", async handle(job, ctx) {
//     const db = await ctx.data("app");
//     await db.exec("INSERT INTO orders(id) VALUES (?)", [job.variables.id]);
//   }});
//
// Core ships SQLite only (the `node:sqlite` built-in — embedded, single-file —
// present in both Deno and Node >= 22.5). Other drivers arrive as ADR 0007 packs
// on the `nano-ide-data-*` axis; an unknown driver throws a pack-install hint.

// Runtime adapter: the few host calls that differ between Deno (native `Deno.*`)
// and Node (`process` / `node:fs`). Detected once at load; see ADR 0036.
interface DataRuntime {
  cwd(): string;
  env(key: string): string | undefined;
  readTextFile(path: string): Promise<string>;
}
const RT: DataRuntime = ((): DataRuntime => {
  const g = globalThis as unknown as {
    Deno?: {
      cwd(): string;
      env: { get(k: string): string | undefined };
      readTextFile(p: string): Promise<string>;
    };
    process?: { cwd(): string; env: Record<string, string | undefined> };
  };
  if (g.Deno) {
    const d = g.Deno;
    return { cwd: () => d.cwd(), env: (k) => d.env.get(k), readTextFile: (p) => d.readTextFile(p) };
  }
  const p = g.process!;
  return {
    cwd: () => p.cwd(),
    env: (k) => p.env[k],
    readTextFile: async (path) => (await import("node:fs/promises")).readFile(path, "utf8"),
  };
})();

/** One column of a table, from the datasource's introspected schema. */
export interface ColumnMeta {
  name: string;
  type: string;
  notNull: boolean;
  primaryKey: boolean;
}

/** One foreign-key constraint: `column` in this table references
 * `refTable(refColumn)`. `refColumn` is empty when the FK targets the parent's
 * primary key without naming a column. `onDelete` is the referential action
 * (e.g. `CASCADE`), empty when none was declared. */
export interface ForeignKeyMeta {
  column: string;
  refTable: string;
  refColumn: string;
  onDelete: string;
}

/** One table: its columns, the names of its indexes, and its foreign keys.
 * Powers the DB Manager, form data-binding, and the ADR 0029 domain-type ↔ table
 * projection. */
export interface TableMeta {
  name: string;
  columns: ColumnMeta[];
  indexes: string[];
  foreignKeys: ForeignKeyMeta[];
}

export type Row = Record<string, unknown>;

export interface ExecResult {
  /** Rows changed by an INSERT/UPDATE/DELETE. */
  changed: number;
  /** Rowid of the last inserted row, when the driver reports one. */
  lastInsertId?: number | bigint;
}

/// The one thin, uniform interface behind every driver (ADR 0024 §2) — the
/// `TDataSet` equivalent. The driver underneath is interchangeable because every
/// consumer shares exactly this surface.
export interface DataSource {
  /** Run a SELECT (or any row-returning statement) and collect the rows. */
  query(sql: string, params?: unknown[]): Promise<Row[]>;
  /** Run a non-row statement (INSERT/UPDATE/DELETE/DDL). */
  exec(sql: string, params?: unknown[]): Promise<ExecResult>;
  /** Run `fn` inside a transaction, committing on success and rolling back on
   * throw. The handle passed to `fn` targets the same connection. */
  tx<T>(fn: (t: DataSource) => Promise<T>): Promise<T>;
  /** Introspect the datasource's tables/columns/indexes. */
  schema(): Promise<TableMeta[]>;
  /** Close the underlying connection. */
  close(): void;
}

// --- env-template resolution (the alias flip) ------------------------------

/// Expand `${VAR}` and `${VAR:-default}` against `env`. An unset or empty var
/// falls back to the `:-default` (or "" when none is given). This is what lets a
/// manifest `driver`/`url` read `${NANO_APP_DB_DRIVER:-sqlite}` and become
/// Postgres in production purely from the environment (ADR 0024 §1).
export function resolveEnvTemplate(
  tpl: string,
  env: (key: string) => string | undefined,
): string {
  return tpl.replace(
    /\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}/g,
    (_m, name: string, dflt: string | undefined) => {
      const v = env(name);
      if (v !== undefined && v !== "") return v;
      return dflt ?? "";
    },
  );
}

interface RawSource {
  driver: unknown;
  url: string;
  migrations?: string;
}

interface ManifestData {
  default?: string;
  sources: Record<string, RawSource>;
}

export interface ResolvedSource {
  name: string;
  driver: string;
  url: string;
  migrations?: string;
}

/// Resolve one raw manifest source (env-templated `driver`/`url`) to concrete
/// values against `env`.
export function resolveSource(
  name: string,
  raw: RawSource,
  env: (key: string) => string | undefined,
): ResolvedSource {
  const driver = typeof raw.driver === "string"
    ? resolveEnvTemplate(raw.driver, env)
    : String(raw.driver);
  return {
    name,
    driver,
    url: resolveEnvTemplate(raw.url, env),
    migrations: raw.migrations,
  };
}

/// List every datasource the manifest declares (resolved against the
/// environment), plus the `default` source name. Powers the DB Manager's
/// datasource picker — it enumerates the aliases without opening a connection.
export async function listSources(
  cwd?: string,
): Promise<{ default?: string; sources: ResolvedSource[] }> {
  const { data } = await findManifest(cwd ?? RT.cwd());
  const env = (k: string) => RT.env(k);
  const sources = Object.entries(data.sources).map(([name, raw]) =>
    resolveSource(name, raw, env)
  );
  return { default: data.default, sources };
}

// --- manifest discovery ----------------------------------------------------

interface ManifestLocation {
  root: string;
  data: ManifestData;
  /** The raw `types` registry block (ADR 0029 §4.2), or `{}` when absent. */
  types: Record<string, unknown>;
}

/// Walk up from `startDir` to the first directory containing `nano.app.json` and
/// return that directory (the project root) plus its `data` block. Workers run
/// with their cwd inside `workers/<name>/`, so the manifest sits above them.
async function findManifest(startDir: string): Promise<ManifestLocation> {
  let dir = startDir.replace(/\/+$/, "");
  for (let i = 0; i < 12; i++) {
    try {
      const text = await RT.readTextFile(`${dir}/nano.app.json`);
      const json = JSON.parse(text) as {
        data?: ManifestData;
        types?: Record<string, unknown>;
      };
      const data = json.data ?? { sources: {} };
      return {
        root: dir,
        data: { default: data.default, sources: data.sources ?? {} },
        types: json.types ?? {},
      };
    } catch {
      // not here — keep walking up
    }
    const slash = dir.lastIndexOf("/");
    if (slash <= 0) break;
    const parent = dir.slice(0, slash);
    if (parent === dir) break;
    dir = parent;
  }
  throw new Error(
    `nano.app.json not found at or above ${startDir}; datasources require an Urban manifest`,
  );
}

/// The manifest's domain-type registry (ADR 0029 §4.2): the transient/declared
/// shapes with no backing table. Returns `{}` when the manifest declares none.
/// The domain-type reifier folds these in alongside the datasource table spine.
export async function manifestTypes(
  cwd?: string,
): Promise<Record<string, unknown>> {
  return (await findManifest(cwd ?? RT.cwd())).types;
}

/// Turn a datasource `url` into a filesystem path for file-backed drivers.
/// Accepts `file:./app.db`, `file:app.db`, `file:/abs/app.db`, a bare relative
/// or absolute path, or `:memory:`. Relative paths resolve against the project
/// root so `file:./app.db` is the same file wherever the consumer's cwd is.
export function sqlitePath(url: string, root: string): string {
  let p = url.startsWith("file:") ? url.slice("file:".length) : url;
  if (p === "" || p === ":memory:") return ":memory:";
  while (p.startsWith("./")) p = p.slice(2);
  if (!p.startsWith("/")) p = `${root}/${p}`;
  return p;
}

// --- SQLite driver (core) --------------------------------------------------

import { DatabaseSync } from "node:sqlite";

function quoteIdent(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

class SqliteDataSource implements DataSource {
  #db: DatabaseSync;
  #onClose?: () => void;

  constructor(path: string, onClose?: () => void) {
    this.#db = new DatabaseSync(path);
    if (path !== ":memory:") this.#db.exec("PRAGMA journal_mode = WAL;");
    this.#db.exec("PRAGMA foreign_keys = ON;");
    this.#onClose = onClose;
  }

  query(sql: string, params: unknown[] = []): Promise<Row[]> {
    const rows = this.#db.prepare(sql).all(...(params as never[]));
    return Promise.resolve(rows as Row[]);
  }

  exec(sql: string, params: unknown[] = []): Promise<ExecResult> {
    const r = this.#db.prepare(sql).run(...(params as never[]));
    return Promise.resolve({
      changed: Number(r.changes),
      lastInsertId: r.lastInsertRowid,
    });
  }

  async tx<T>(fn: (t: DataSource) => Promise<T>): Promise<T> {
    this.#db.exec("BEGIN");
    try {
      const out = await fn(this);
      this.#db.exec("COMMIT");
      return out;
    } catch (e) {
      this.#db.exec("ROLLBACK");
      throw e;
    }
  }

  schema(): Promise<TableMeta[]> {
    const tables = this.#db
      .prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
      )
      .all() as Array<{ name: string }>;
    const out: TableMeta[] = [];
    for (const t of tables) {
      const cols = this.#db
        .prepare(`PRAGMA table_info(${quoteIdent(t.name)})`)
        .all() as Array<{ name: string; type: string; notnull: number; pk: number }>;
      const idx = this.#db
        .prepare(`PRAGMA index_list(${quoteIdent(t.name)})`)
        .all() as Array<{ name: string }>;
      const fks = this.#db
        .prepare(`PRAGMA foreign_key_list(${quoteIdent(t.name)})`)
        .all() as Array<
          { from: string; table: string; to: string | null; on_delete?: string }
        >;
      out.push({
        name: t.name,
        columns: cols.map((c) => ({
          name: c.name,
          type: c.type,
          notNull: !!c.notnull,
          primaryKey: !!c.pk,
        })),
        indexes: idx.map((i) => String(i.name)),
        foreignKeys: fks.map((f) => ({
          column: f.from,
          refTable: f.table,
          refColumn: f.to ?? "",
          onDelete: f.on_delete && f.on_delete.toUpperCase() !== "NO ACTION"
            ? f.on_delete.toUpperCase()
            : "",
        })),
      });
    }
    return Promise.resolve(out);
  }

  close(): void {
    this.#db.close();
    this.#onClose?.();
  }
}

// --- open (the named-alias entrypoint) -------------------------------------

// One handle per resolved (driver,url), so repeated opens in a process share a
// connection rather than reopening the file.
const CACHE = new Map<string, DataSource>();

export interface OpenOptions {
  /** Directory to begin the manifest search from. Defaults to the runtime cwd. */
  cwd?: string;
}

/// Open the named datasource (or the manifest's `default` when `name` is
/// omitted), resolving its driver/url from the environment. Bind by NAME — this
/// is the seam the SQLite→server flip happens behind (ADR 0024 §1).
export async function openDataSource(
  name?: string,
  opts?: OpenOptions,
): Promise<DataSource> {
  const cwd = opts?.cwd ?? RT.cwd();
  const { root, data } = await findManifest(cwd);
  const srcName = name ?? data.default ?? Object.keys(data.sources)[0];
  if (!srcName) {
    throw new Error("no datasource declared in nano.app.json (data.sources is empty)");
  }
  const raw = data.sources[srcName];
  if (!raw) {
    throw new Error(
      `datasource "${srcName}" is not declared in nano.app.json data.sources`,
    );
  }
  const resolved = resolveSource(srcName, raw, (k) => RT.env(k));
  const key = `${resolved.driver}::${resolved.url}`;
  const hit = CACHE.get(key);
  if (hit) return hit;

  let ds: DataSource;
  if (resolved.driver === "sqlite") {
    ds = new SqliteDataSource(sqlitePath(resolved.url, root), () => CACHE.delete(key));
  } else {
    throw new Error(
      `datasource driver "${resolved.driver}" is not bundled; install a nano-ide-data-${resolved.driver} pack (ADR 0024 §3)`,
    );
  }
  CACHE.set(key, ds);
  return ds;
}

/// Alias reading like the ADR's `ctx.data(...)`: `data("app")`.
export const data = openDataSource;
