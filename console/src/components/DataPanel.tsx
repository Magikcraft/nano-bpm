import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import CodeEditor from "./CodeEditor";
import { Button } from "./ui";
import {
  execData,
  getDataMigrations,
  getDataSchema,
  getDataSources,
  migrateData,
  queryData,
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
        <div className="px-3 py-2 text-[10px] font-bold uppercase tracking-wider text-fg-faint">
          Tables ({tables.length})
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
