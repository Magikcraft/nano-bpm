import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";

import CodeEditor from "./CodeEditor";
import { Button, Input, inputClass } from "./ui";
import {
  execData,
  getDataMigrations,
  getDataSchema,
  getDataSources,
  migrateData,
  queryData,
  saveProjectFile,
  type DataColumnMeta,
  type DataMigrationEntry,
  type DataQueryResult,
  type DataSourceInfo,
  type DataTableMeta,
} from "../gen";

// The DB Manager — ADR 0024 §4. Three sub-surfaces (Tables / SQL / Migrations)
// over a *named* datasource, so it browses whatever `data.<source>` currently
// resolves to (embedded SQLite in the IDE, a server DB in production once a
// `nano-ide-data-*` driver pack is installed). Every read/write goes through
// the server data gateway, never a parallel SQLite path.

type SubTab = "tables" | "sql" | "migrations";
const ROW_LIMIT = 200;

function errMsg(e: unknown): string {
  if (e instanceof Error) return e.message;
  if (typeof e === "string") return e;
  if (e && typeof e === "object") {
    const o = e as { error?: unknown; detail?: unknown };
    if (typeof o.error === "string") return o.error;
    if (typeof o.detail === "string") return o.detail;
  }
  return String(e);
}

/** Quote an identifier for SQLite (double quotes, doubled to escape). */
function quoteIdent(id: string): string {
  return `"${id.replaceAll('"', '""')}"`;
}

// --- New-table DDL model ----------------------------------------------------

/** Common SQLite column affinities offered in the New Table dialog. */
const COLUMN_TYPES = ["INTEGER", "TEXT", "REAL", "NUMERIC", "BLOB", "BOOLEAN", "TIMESTAMP"];

/** `ON DELETE` referential actions offered for a foreign key ("" = omit). */
const ON_DELETE_ACTIONS = ["", "CASCADE", "SET NULL", "RESTRICT", "NO ACTION"];

/** A column's optional foreign-key reference to another table's column. */
interface ColumnRef {
  table: string;
  column: string;
  onDelete: string;
}

interface NewColumn {
  name: string;
  type: string;
  primaryKey: boolean;
  notNull: boolean;
  default: string;
  references: ColumnRef | null;
}

function blankColumn(): NewColumn {
  return {
    name: "",
    type: "TEXT",
    primaryKey: false,
    notNull: false,
    default: "",
    references: null,
  };
}

/** Build a `CREATE TABLE` statement from the dialog's form model. */
function buildCreateTable(table: string, cols: NewColumn[]): string {
  const pkCols = cols.filter((c) => c.primaryKey && c.name.trim());
  const defs = cols
    .filter((c) => c.name.trim())
    .map((c) => {
      let s = `  ${quoteIdent(c.name.trim())} ${c.type || "TEXT"}`;
      // A single primary key is declared inline; an INTEGER one becomes the
      // rowid alias. Composite keys use a table-level constraint instead.
      if (pkCols.length === 1 && c.primaryKey) s += " PRIMARY KEY";
      if (c.notNull && !c.primaryKey) s += " NOT NULL";
      if (c.default.trim()) s += ` DEFAULT ${c.default.trim()}`;
      return s;
    });
  if (pkCols.length > 1) {
    defs.push(`  PRIMARY KEY (${pkCols.map((c) => quoteIdent(c.name.trim())).join(", ")})`);
  }
  // Foreign keys are emitted as table-level constraints (SQLite requires them
  // after all column defs). `PRAGMA foreign_keys = ON` is set by the datasource,
  // so these are enforced, not merely declarative.
  for (const c of cols) {
    const r = c.references;
    if (!c.name.trim() || !r || !r.table.trim() || !r.column.trim()) continue;
    let s = `  FOREIGN KEY (${quoteIdent(c.name.trim())}) REFERENCES ${quoteIdent(
      r.table.trim(),
    )} (${quoteIdent(r.column.trim())})`;
    if (r.onDelete) s += ` ON DELETE ${r.onDelete}`;
    defs.push(s);
  }
  return `CREATE TABLE ${quoteIdent(table.trim() || "new_table")} (\n${defs.join(",\n")}\n);`;
}

/** Next ordered migration filename, e.g. `003_create_orders.sql`. */
function nextMigrationName(existing: DataMigrationEntry[], table: string): string {
  const max = existing.reduce((m, e) => {
    const n = parseInt(e.name, 10);
    return Number.isNaN(n) ? m : Math.max(m, n);
  }, 0);
  const num = String(max + 1).padStart(3, "0");
  const slug =
    table
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "") || "table";
  return `${num}_create_${slug}.sql`;
}

/** Human cell rendering: NULL italic, blobs badged, objects as compact JSON. */
function cell(v: unknown): { text: string; muted: boolean } {
  if (v === null || v === undefined) return { text: "NULL", muted: true };
  if (typeof v === "object") {
    const o = v as { $blob?: number };
    if (typeof o.$blob === "number") return { text: `‹blob ${o.$blob}B›`, muted: true };
    return { text: JSON.stringify(v), muted: false };
  }
  return { text: String(v), muted: false };
}

export default function DataPanel({ name }: { name: string }) {
  const [sources, setSources] = useState<DataSourceInfo[]>([]);
  const [source, setSource] = useState<string>("");
  const [tab, setTab] = useState<SubTab>("tables");
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let live = true;
    setLoading(true);
    setLoadError(null);
    getDataSources({ path: { name }, throwOnError: true })
      .then((r) => {
        if (!live) return;
        const list = r.data.sources ?? [];
        setSources(list);
        setSource(r.data.default ?? list[0]?.name ?? "");
      })
      .catch((e) => live && setLoadError(errMsg(e)))
      .finally(() => live && setLoading(false));
    return () => {
      live = false;
    };
  }, [name]);

  const active = useMemo(() => sources.find((s) => s.name === source), [sources, source]);

  if (loading) {
    return <div className="p-8 text-sm text-fg-faint">Loading datasources…</div>;
  }
  if (loadError) {
    return (
      <div className="p-8 text-sm text-danger">
        Couldn’t load datasources: {loadError}
      </div>
    );
  }
  if (sources.length === 0) {
    return (
      <div className="flex h-full items-center justify-center p-8 text-center text-sm text-fg-faint">
        <div>
          <p className="font-medium text-fg-muted">No datasources declared.</p>
          <p className="mt-1">
            Add a <code className="text-fg-muted">data.sources</code> block to{" "}
            <code className="text-fg-muted">nano.app.json</code> (ADR 0024) to use the DB Manager.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center gap-3 border-b border-edge bg-panel px-4 py-2">
        <label className="flex items-center gap-1.5 text-xs text-fg-faint">
          <span>Datasource:</span>
          <select
            value={source}
            onChange={(e) => setSource(e.target.value)}
            className="rounded-md border border-edge-strong bg-bg-subtle px-2 py-1 text-sm text-fg hover:bg-hover"
          >
            {sources.map((s) => (
              <option key={s.name} value={s.name}>
                {s.name} ({s.driver})
              </option>
            ))}
          </select>
        </label>
        {active && (
          <span className="truncate text-xs text-fg-faint" title={active.url}>
            {active.url}
          </span>
        )}
        <div className="flex-1" />
        <nav className="flex items-center gap-1 text-sm">
          {(["tables", "sql", "migrations"] as SubTab[]).map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`rounded-md px-2.5 py-1 capitalize ${
                tab === t ? "bg-accent/15 font-semibold text-accent" : "text-fg-faint hover:bg-hover"
              }`}
            >
              {t}
            </button>
          ))}
        </nav>
      </div>

      <div className="min-h-0 flex-1 overflow-hidden">
        {source && tab === "tables" && <TablesTab key={`t-${source}`} name={name} source={source} />}
        {source && tab === "sql" && <SqlTab key={`s-${source}`} name={name} source={source} />}
        {source && tab === "migrations" && (
          <MigrationsTab key={`m-${source}`} name={name} source={source} />
        )}
      </div>
    </div>
  );
}

// --- Tables -----------------------------------------------------------------

function TablesTab({ name, source }: { name: string; source: string }) {
  const [tables, setTables] = useState<DataTableMeta[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [rows, setRows] = useState<DataQueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loadingRows, setLoadingRows] = useState(false);
  const [showNew, setShowNew] = useState(false);

  const loadSchema = useCallback(async () => {
    setError(null);
    try {
      const r = await getDataSchema({ path: { name, source }, throwOnError: true });
      setTables(r.data.tables ?? []);
      setSelected((cur) => cur ?? r.data.tables?.[0]?.name ?? null);
    } catch (e) {
      setError(errMsg(e));
    }
  }, [name, source]);

  useEffect(() => {
    void loadSchema();
  }, [loadSchema]);

  useEffect(() => {
    if (!selected) return;
    let live = true;
    setLoadingRows(true);
    setError(null);
    queryData({
      path: { name, source },
      body: { sql: `SELECT * FROM ${quoteIdent(selected)} LIMIT ${ROW_LIMIT}` },
      throwOnError: true,
    })
      .then((r) => live && setRows(r.data))
      .catch((e) => live && setError(errMsg(e)))
      .finally(() => live && setLoadingRows(false));
    return () => {
      live = false;
    };
  }, [name, source, selected]);

  const meta = tables.find((t) => t.name === selected);

  return (
    <div className="flex h-full min-h-0">
      <aside className="w-56 shrink-0 overflow-y-auto border-r border-edge bg-panel">
        <div className="flex items-center justify-between gap-2 px-3 py-2">
          <span className="text-[10px] font-bold uppercase tracking-wider text-fg-faint">
            Tables ({tables.length})
          </span>
          <button
            onClick={() => setShowNew(true)}
            className="rounded px-1.5 py-0.5 text-xs font-medium text-accent hover:bg-accent/10"
            title="Create a new table"
          >
            ＋ New
          </button>
        </div>
        {tables.map((t) => (
          <button
            key={t.name}
            onClick={() => setSelected(t.name)}
            className={`block w-full truncate px-3 py-1.5 text-left text-sm ${
              selected === t.name ? "bg-accent/15 font-medium text-accent" : "text-fg-muted hover:bg-hover"
            }`}
            title={`${t.name} — ${t.columns.length} columns`}
          >
            {t.name}
          </button>
        ))}
        {tables.length === 0 && (
          <p className="px-3 py-2 text-xs text-fg-faint">No tables yet.</p>
        )}
      </aside>
      <div className="flex min-w-0 flex-1 flex-col">
        {meta && <ColumnStrip columns={meta.columns} indexes={meta.indexes} />}
        {error && <div className="px-4 py-2 text-sm text-danger">{error}</div>}
        <div className="min-h-0 flex-1 overflow-auto">
          {loadingRows ? (
            <div className="p-6 text-sm text-fg-faint">Loading rows…</div>
          ) : rows ? (
            <ResultGrid result={rows} />
          ) : (
            <div className="p-6 text-sm text-fg-faint">Select a table.</div>
          )}
        </div>
        {rows && (
          <div className="border-t border-edge px-4 py-1 text-xs text-fg-faint">
            {rows.rows.length} row{rows.rows.length === 1 ? "" : "s"}
            {rows.rows.length >= ROW_LIMIT ? ` (first ${ROW_LIMIT})` : ""}
          </div>
        )}
      </div>
      {showNew && (
        <NewTableDialog
          name={name}
          source={source}
          tables={tables}
          onClose={() => setShowNew(false)}
          onCreated={(table) => {
            setShowNew(false);
            void loadSchema().then(() => setSelected(table));
          }}
        />
      )}
    </div>
  );
}

function ColumnStrip({ columns, indexes }: { columns: DataColumnMeta[]; indexes: string[] }) {
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-edge bg-bg-subtle px-4 py-1.5 text-xs">
      {columns.map((c) => (
        <span key={c.name} className="text-fg-muted">
          <span className="font-medium text-fg">{c.name}</span>
          <span className="text-fg-faint"> {c.type || "?"}</span>
          {c.primaryKey && <span className="ml-0.5 text-accent" title="primary key">🔑</span>}
          {c.notNull && !c.primaryKey && <span className="ml-0.5 text-fg-faint" title="NOT NULL">*</span>}
        </span>
      ))}
      {indexes.length > 0 && (
        <span className="text-fg-faint" title={indexes.join(", ")}>
          · {indexes.length} index{indexes.length === 1 ? "" : "es"}
        </span>
      )}
    </div>
  );
}

// --- Shared modal + New-table dialog ---------------------------------------

/** Minimal centered modal (the console has no dialog primitive yet). */
function Modal({
  title,
  onClose,
  children,
  footer,
  wide,
}: {
  title: string;
  onClose: () => void;
  children: ReactNode;
  footer: ReactNode;
  wide?: boolean;
}) {
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      onClick={onClose}
    >
      <div
        className={`flex max-h-[85vh] w-full ${wide ? "max-w-2xl" : "max-w-lg"} flex-col overflow-hidden rounded-lg border border-edge bg-panel shadow-xl`}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b border-edge px-4 py-2.5">
          <h2 className="text-sm font-semibold text-fg">{title}</h2>
          <button onClick={onClose} className="rounded p-1 text-fg-faint hover:bg-hover" title="Close">
            ✕
          </button>
        </div>
        <div className="min-h-0 flex-1 overflow-auto p-4">{children}</div>
        <div className="flex items-center justify-end gap-2 border-t border-edge px-4 py-2.5">
          {footer}
        </div>
      </div>
    </div>
  );
}

/**
 * Encapsulates table creation as a form (the Delphi-style affordance): the user
 * fills in columns and we generate the `CREATE TABLE` DDL. Two apply paths match
 * the two documented workflows — run it now against the datasource, or save it
 * as an ordered migration file (the deployable path, ADR 0024 §4).
 */
function NewTableDialog({
  name,
  source,
  tables,
  onClose,
  onCreated,
}: {
  name: string;
  source: string;
  tables: DataTableMeta[];
  onClose: () => void;
  onCreated: (table: string) => void;
}) {
  const [table, setTable] = useState("");
  const [cols, setCols] = useState<NewColumn[]>([
    {
      name: "id",
      type: "INTEGER",
      primaryKey: true,
      notNull: false,
      default: "",
      references: null,
    },
  ]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [note, setNote] = useState<string | null>(null);

  const namedCols = cols.filter((c) => c.name.trim());
  const valid = table.trim().length > 0 && namedCols.length > 0;
  const sql = useMemo(() => buildCreateTable(table, cols), [table, cols]);

  const setCol = (i: number, patch: Partial<NewColumn>) =>
    setCols((cs) => cs.map((c, j) => (j === i ? { ...c, ...patch } : c)));
  const addCol = () => setCols((cs) => [...cs, blankColumn()]);
  const removeCol = (i: number) => setCols((cs) => cs.filter((_, j) => j !== i));

  // FK targets: existing tables in this datasource (the new table can't yet
  // reference itself since it doesn't exist). Columns come from the picked table.
  const fkTables = tables.map((t) => t.name);
  const columnsOf = (t: string) => tables.find((x) => x.name === t)?.columns.map((c) => c.name) ?? [];
  const toggleFk = (i: number, on: boolean) =>
    setCol(i, {
      references: on
        ? { table: fkTables[0] ?? "", column: "", onDelete: "" }
        : null,
    });
  const setRef = (i: number, patch: Partial<ColumnRef>) =>
    setCols((cs) =>
      cs.map((c, j) =>
        j === i && c.references ? { ...c, references: { ...c.references, ...patch } } : c,
      ),
    );

  const runNow = useCallback(async () => {
    setBusy(true);
    setError(null);
    setNote(null);
    try {
      await execData({ path: { name, source }, body: { sql }, throwOnError: true });
      onCreated(table.trim());
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  }, [name, source, sql, table, onCreated]);

  const saveMigration = useCallback(async () => {
    setBusy(true);
    setError(null);
    setNote(null);
    try {
      const r = await getDataMigrations({ path: { name, source }, throwOnError: true });
      const dir = r.data.dir || "db/migrations";
      const file = nextMigrationName(r.data.entries ?? [], table);
      const path = `${dir}/${file}`;
      await saveProjectFile({ path: { name }, query: { path }, body: `${sql}\n`, throwOnError: true });
      setNote(`Wrote ${path}. Apply it from the Migrations tab.`);
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  }, [name, source, sql, table]);

  return (
    <Modal
      title="New table"
      onClose={onClose}
      wide
      footer={
        <>
          {note && <span className="mr-auto truncate text-xs text-ok">{note}</span>}
          {error && <span className="mr-auto truncate text-xs text-danger">{error}</span>}
          <Button variant="secondary" onClick={onClose} disabled={busy}>
            Cancel
          </Button>
          <Button variant="secondary" onClick={() => void saveMigration()} disabled={!valid || busy}>
            Save as migration
          </Button>
          <Button variant="primary" onClick={() => void runNow()} disabled={!valid || busy}>
            {busy ? "Working…" : "Create now"}
          </Button>
        </>
      }
    >
      <label className="mb-3 block">
        <span className="mb-1 block text-xs font-medium text-fg-muted">Table name</span>
        <Input
          value={table}
          onChange={(e) => setTable(e.target.value)}
          placeholder="orders"
          autoFocus
        />
      </label>

      <div className="mb-1 flex items-center justify-between">
        <span className="text-xs font-medium text-fg-muted">Columns</span>
        <button onClick={addCol} className="text-xs font-medium text-accent hover:underline">
          ＋ Add column
        </button>
      </div>
      <div className="space-y-1.5">
        <div className="flex items-center gap-2 px-0.5 text-[10px] uppercase tracking-wide text-fg-faint">
          <span className="flex-1">Name</span>
          <span className="w-28">Type</span>
          <span className="w-24">Default</span>
          <span className="w-8 text-center" title="Primary key">
            PK
          </span>
          <span className="w-10 text-center" title="NOT NULL">
            Req
          </span>
          <span className="w-8 text-center" title="Foreign key">
            FK
          </span>
          <span className="w-5" />
        </div>
        {cols.map((c, i) => (
          <div key={i} className="flex flex-col gap-1.5">
            <div className="flex items-center gap-2">
              <Input
                className="flex-1"
                value={c.name}
                onChange={(e) => setCol(i, { name: e.target.value })}
                placeholder="column"
              />
              <select
                className={`${inputClass} w-28`}
                value={c.type}
                onChange={(e) => setCol(i, { type: e.target.value })}
              >
                {COLUMN_TYPES.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
              <Input
                className="w-24"
                value={c.default}
                onChange={(e) => setCol(i, { default: e.target.value })}
                placeholder="—"
                title="Raw SQL default, e.g. 0, 'active', CURRENT_TIMESTAMP"
              />
              <input
                type="checkbox"
                className="w-8"
                checked={c.primaryKey}
                onChange={(e) => setCol(i, { primaryKey: e.target.checked })}
                title="Primary key"
              />
              <input
                type="checkbox"
                className="w-10"
                checked={c.notNull}
                disabled={c.primaryKey}
                onChange={(e) => setCol(i, { notNull: e.target.checked })}
                title="NOT NULL"
              />
              <input
                type="checkbox"
                className="w-8"
                checked={c.references !== null}
                disabled={fkTables.length === 0}
                onChange={(e) => toggleFk(i, e.target.checked)}
                title={
                  fkTables.length === 0
                    ? "No other tables to reference yet"
                    : "Foreign key to another table"
                }
              />
              <button
                onClick={() => removeCol(i)}
                disabled={cols.length === 1}
                className="w-5 text-fg-faint hover:text-danger disabled:opacity-30"
                title="Remove column"
              >
                ✕
              </button>
            </div>
            {c.references && (
              <div className="flex items-center gap-2 pl-3 text-xs text-fg-faint">
                <span className="text-fg-faint">↳ references</span>
                <select
                  className={`${inputClass} w-40`}
                  value={c.references.table}
                  onChange={(e) => setRef(i, { table: e.target.value, column: "" })}
                >
                  {fkTables.map((t) => (
                    <option key={t} value={t}>
                      {t}
                    </option>
                  ))}
                </select>
                <span>.</span>
                <select
                  className={`${inputClass} w-40`}
                  value={c.references.column}
                  onChange={(e) => setRef(i, { column: e.target.value })}
                >
                  <option value="">column…</option>
                  {columnsOf(c.references.table).map((col) => (
                    <option key={col} value={col}>
                      {col}
                    </option>
                  ))}
                </select>
                <span className="ml-1">on delete</span>
                <select
                  className={`${inputClass} w-28`}
                  value={c.references.onDelete}
                  onChange={(e) => setRef(i, { onDelete: e.target.value })}
                >
                  {ON_DELETE_ACTIONS.map((a) => (
                    <option key={a} value={a}>
                      {a || "—"}
                    </option>
                  ))}
                </select>
              </div>
            )}
          </div>
        ))}
      </div>

      <div className="mt-4">
        <span className="mb-1 block text-[10px] uppercase tracking-wide text-fg-faint">
          Generated SQL
        </span>
        <pre className="max-h-40 overflow-auto rounded-md border border-edge bg-inset p-3 font-mono text-xs text-fg-muted">
          {sql}
        </pre>
      </div>
    </Modal>
  );
}

// --- SQL --------------------------------------------------------------------

/** A leading SELECT/WITH/PRAGMA/EXPLAIN reads rows; everything else mutates. */
function isReadStatement(sql: string): boolean {
  return /^\s*(select|with|pragma|explain)\b/i.test(sql);
}

function SqlTab({ name, source }: { name: string; source: string }) {
  const [sql, setSql] = useState("SELECT 1;");
  const [result, setResult] = useState<DataQueryResult | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const sqlRef = useRef(sql);
  sqlRef.current = sql;

  const run = useCallback(async () => {
    const text = sqlRef.current.trim().replace(/;\s*$/, "");
    if (!text) return;
    setRunning(true);
    setError(null);
    setStatus(null);
    try {
      if (isReadStatement(text)) {
        const r = await queryData({
          path: { name, source },
          body: { sql: text },
          throwOnError: true,
        });
        setResult(r.data);
        setStatus(`${r.data.rows.length} row${r.data.rows.length === 1 ? "" : "s"}`);
      } else {
        const r = await execData({
          path: { name, source },
          body: { sql: text },
          throwOnError: true,
        });
        setResult(null);
        setStatus(
          `${r.data.changed} row${r.data.changed === 1 ? "" : "s"} changed` +
            (r.data.lastInsertId != null ? ` · last id ${r.data.lastInsertId}` : ""),
        );
      }
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setRunning(false);
    }
  }, [name, source]);

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="min-h-0 flex-[2] overflow-hidden border-b border-edge">
        <CodeEditor
          value={sql}
          language="sql"
          onChange={setSql}
          onSave={() => void run()}
        />
      </div>
      <div className="flex items-center gap-3 border-b border-edge bg-panel px-4 py-2">
        <Button onClick={() => void run()} disabled={running}>
          {running ? "Running…" : "▶ Run"}
        </Button>
        <span className="text-xs text-fg-faint">⌘/Ctrl+S runs</span>
        {status && <span className="text-xs text-ok">{status}</span>}
        {error && <span className="truncate text-xs text-danger">{error}</span>}
      </div>
      <div className="min-h-0 flex-[3] overflow-auto">
        {result ? (
          <ResultGrid result={result} />
        ) : (
          <div className="p-6 text-sm text-fg-faint">
            Run a statement to see results. SELECT/WITH/PRAGMA return rows; anything else reports rows changed.
          </div>
        )}
      </div>
    </div>
  );
}

// --- Migrations -------------------------------------------------------------

function MigrationsTab({ name, source }: { name: string; source: string }) {
  const [entries, setEntries] = useState<DataMigrationEntry[]>([]);
  const [dir, setDir] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [note, setNote] = useState<string | null>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const r = await getDataMigrations({ path: { name, source }, throwOnError: true });
      setEntries(r.data.entries ?? []);
      setDir(r.data.dir);
    } catch (e) {
      setError(errMsg(e));
    }
  }, [name, source]);

  useEffect(() => {
    void load();
  }, [load]);

  const pending = entries.filter((e) => !e.applied).length;

  const apply = useCallback(async () => {
    setBusy(true);
    setError(null);
    setNote(null);
    try {
      const r = await migrateData({ path: { name, source }, throwOnError: true });
      setNote(
        r.data.applied.length
          ? `Applied ${r.data.applied.length}: ${r.data.applied.join(", ")}`
          : "Nothing to apply — already up to date.",
      );
      await load();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  }, [name, source, load]);

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex items-center gap-3 border-b border-edge bg-panel px-4 py-2">
        <Button onClick={() => void apply()} disabled={busy || pending === 0}>
          {busy ? "Applying…" : pending > 0 ? `Apply ${pending} pending` : "Up to date"}
        </Button>
        <span className="text-xs text-fg-faint">
          {dir || "db/migrations"} · {entries.length} file{entries.length === 1 ? "" : "s"}
        </span>
        {note && <span className="text-xs text-ok">{note}</span>}
        {error && <span className="truncate text-xs text-danger">{error}</span>}
      </div>
      <div className="min-h-0 flex-1 overflow-auto">
        {entries.length === 0 ? (
          <div className="p-6 text-sm text-fg-faint">
            No migrations in <code>{dir || "db/migrations"}</code>. Add ordered{" "}
            <code>*.sql</code> files there.
          </div>
        ) : (
          <table className="w-full text-sm">
            <tbody>
              {entries.map((e) => (
                <tr key={e.name} className="border-b border-edge/50">
                  <td className="px-4 py-1.5">
                    {e.applied ? (
                      <span className="text-ok" title={e.appliedAt ?? undefined}>
                        ✓ applied
                      </span>
                    ) : (
                      <span className="text-warn">• pending</span>
                    )}
                  </td>
                  <td className="px-4 py-1.5 font-mono text-fg-muted">{e.name}</td>
                  <td className="px-4 py-1.5 text-xs text-fg-faint">
                    {e.appliedAt ? new Date(e.appliedAt).toLocaleString() : ""}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

// --- Shared results grid ----------------------------------------------------

function ResultGrid({ result }: { result: DataQueryResult }) {
  if (result.columns.length === 0 && result.rows.length === 0) {
    return <div className="p-6 text-sm text-fg-faint">No rows.</div>;
  }
  return (
    <table className="min-w-full border-collapse text-sm">
      <thead className="sticky top-0 bg-bg-subtle">
        <tr>
          {result.columns.map((c) => (
            <th
              key={c}
              className="border-b border-edge px-3 py-1.5 text-left font-semibold text-fg-muted"
            >
              {c}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {result.rows.map((row, i) => (
          <tr key={i} className="hover:bg-hover/50">
            {result.columns.map((c) => {
              const { text, muted } = cell(row[c]);
              return (
                <td
                  key={c}
                  className={`whitespace-pre border-b border-edge/40 px-3 py-1 font-mono text-xs ${
                    muted ? "italic text-fg-faint" : "text-fg"
                  }`}
                >
                  {text}
                </td>
              );
            })}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
