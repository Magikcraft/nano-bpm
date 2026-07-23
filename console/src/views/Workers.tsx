import { lazy, Suspense, useEffect, useRef, useState, type ComponentType } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  createLibFile,
  createWorker,
  createWorkerFile,
  deleteLibFile,
  deleteWorker,
  deleteWorkerFile,
  getLibFile,
  getWorkerFile,
  listLibFiles,
  listWorkers,
  saveLibFile,
  saveWorkerFile,
  startWorker,
  stopWorker,
  type WorkerPhase,
  type WorkerSummary,
} from "../gen";
import { exportWorkersApp, type WorkerLogLine } from "../lib/api";
import { languageForFile } from "../lib/editorLang";
import type { ExtraModel } from "../components/CodeEditor";
import { Button, PageHeader } from "../components/ui";
import { IS_STUDIO } from "../lib/profile";

type CodeEditorProps = {
  value: string;
  language: string;
  path?: string;
  readOnly?: boolean;
  extraModels?: ExtraModel[];
  onChange: (value: string) => void;
  onSave?: () => void;
};

// Monaco is multi-MB; load it as a separate chunk only when an editor is shown
// so the initial console bundle stays lean. Gated on the `__STUDIO__` define
// (ADR 0034): the operator ("observe") build hides the worker-authoring tab, and
// guarding the `import()` on the folded literal lets esbuild drop the Monaco
// chunk (ts.worker + typescript + editor, ~3.25MB gzip plus its orphan worker
// bundles) from that build during transform. The `() => null` fallback keeps the
// type honest and can never render (its call sites live behind IS_STUDIO).
const CodeEditor: ComponentType<CodeEditorProps> = __STUDIO__
  ? (lazy(() => import("../components/CodeEditor")) as ComponentType<CodeEditorProps>)
  : () => null;

function phaseBadge(phase: WorkerPhase): { label: string; cls: string; dot: string } {
  switch (phase) {
    case "running":
      return { label: "Running", cls: "bg-ok/10 text-ok", dot: "bg-ok" };
    case "starting":
      return { label: "Starting", cls: "bg-info/10 text-info", dot: "bg-info animate-pulse" };
    case "crashed":
      return { label: "Crashed", cls: "bg-danger/10 text-danger", dot: "bg-danger" };
    case "stopped":
      return { label: "Stopped", cls: "bg-hover text-fg-muted", dot: "bg-fg-faint" };
  }
}

function fmtUptime(ms: number): string {
  if (ms <= 0) return "—";
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

export default function Workers() {
  const queryClient = useQueryClient();
  const [tab, setTab] = useState<"editor" | "running">(
    IS_STUDIO ? "editor" : "running",
  );
  const [selected, setSelected] = useState<string | null>(null);
  // When true, the editor pane shows the shared `@lib/` library instead of a worker.
  const [showLib, setShowLib] = useState(false);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<{ kind: "ok" | "err"; text: string } | null>(null);
  // Standalone-app export: which workers to bundle (modal is open when non-null).
  const [exportSel, setExportSel] = useState<Set<string> | null>(null);

  // Poll worker runtime while the page is open so status/metrics stay live.
  const { data } = useQuery({
    queryKey: ["workers"],
    queryFn: async () => (await listWorkers({ throwOnError: true })).data,
    refetchInterval: 1500,
  });
  const workers = data?.workers ?? [];
  const denoAvailable = data?.denoAvailable ?? true;
  const nodeAvailable = data?.nodeAvailable ?? true;
  const current = workers.find((w) => w.name === selected);

  const refresh = () => queryClient.invalidateQueries({ queryKey: ["workers"] });
  const flash = (kind: "ok" | "err", text: string) => {
    setMessage({ kind, text });
    if (kind === "ok") setTimeout(() => setMessage(null), 3000);
  };

  async function newWorker() {
    const name = prompt("New worker name (letters, digits, - and _):")?.trim();
    if (!name) return;
    const jobType = prompt("BPMN job type this worker handles:", name)?.trim() || name;
    setBusy(true);
    try {
      await createWorker({ body: { name, jobType }, throwOnError: true });
      await refresh();
      setSelected(name);
      flash("ok", `Created worker '${name}'.`);
    } catch (e) {
      flash("err", e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function removeWorker(name: string) {
    if (!confirm(`Delete worker '${name}'? Its code will be removed from the workspace.`)) return;
    setBusy(true);
    try {
      await deleteWorker({ path: { name }, throwOnError: true });
      if (selected === name) setSelected(null);
      await refresh();
      flash("ok", `Deleted '${name}'.`);
    } catch (e) {
      flash("err", e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function start(name: string) {
    setBusy(true);
    try {
      await startWorker({ path: { name }, throwOnError: true });
      await refresh();
      flash("ok", `Starting '${name}'.`);
    } catch (e) {
      flash("err", e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function stop(name: string) {
    setBusy(true);
    try {
      await stopWorker({ path: { name }, throwOnError: true });
      await refresh();
      flash("ok", `Stopping '${name}'.`);
    } catch (e) {
      flash("err", e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  function openExport() {
    // Default the selection to every worker.
    setExportSel(new Set(workers.map((w) => w.name)));
  }

  function toggleExport(name: string) {
    setExportSel((prev) => {
      const next = new Set(prev ?? []);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  }

  async function doExport() {
    if (!exportSel || exportSel.size === 0) return;
    const names = workers.map((w) => w.name).filter((n) => exportSel.has(n));
    setBusy(true);
    try {
      await exportWorkersApp(names);
      setExportSel(null);
      flash("ok", `Exported ${names.length} worker${names.length === 1 ? "" : "s"} as a standalone app.`);
    } catch (e) {
      flash("err", e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="flex h-full flex-col">
      <div className="border-b border-edge px-6 pt-4">
        <PageHeader
          title="Embedded Workers"
          subtitle={
            IS_STUDIO
              ? "Author TypeScript job workers and run them as isolated processes (Node, or sandboxed Deno) over the Falcon."
              : "Monitor job workers running as isolated processes (Node, or sandboxed Deno) over the Falcon."
          }
          actions={
            <div className="flex gap-1 rounded-lg bg-inset p-1 text-sm">
              {(IS_STUDIO
                ? (["editor", "running"] as const)
                : (["running"] as const)
              ).map((t) => (
                <button
                  key={t}
                  onClick={() => setTab(t)}
                  className={`rounded-md px-3 py-1 transition-colors ${
                    tab === t
                      ? "bg-accent/10 font-medium text-accent-strong"
                      : "text-fg-muted hover:text-fg"
                  }`}
                >
                  {t === "running" ? "Running" : "Editor"}
                </button>
              ))}
            </div>
          }
        />
      </div>

      {!nodeAvailable && !denoAvailable && (
        <div className="border-b border-warn/30 bg-warn/10 px-6 py-2 text-xs text-warn">
          No JavaScript runtime found — you can author workers, but starting them needs Node ≥ 22.6
          (the npm launcher provides one) or Deno. Install Node from https://nodejs.org.
        </div>
      )}
      {message && (
        <div
          className={`border-b px-6 py-2 text-xs ${
            message.kind === "ok"
              ? "border-ok/30 bg-ok/10 text-ok"
              : "border-danger/30 bg-danger/10 text-danger"
          }`}
        >
          {message.text}
        </div>
      )}

      {tab === "running" ? (
        <RunningTab workers={workers} onStart={start} onStop={stop} busy={busy} />
      ) : (
        <div className="flex min-h-0 flex-1">
          {/* Worker library */}
          <aside className="flex w-60 shrink-0 flex-col border-r border-edge">
            <div className="flex items-center justify-between px-3 py-2">
              <span className="text-xs font-medium uppercase tracking-wide text-fg-faint">
                Workers
              </span>
              <div className="flex gap-1">
                <button
                  onClick={openExport}
                  disabled={busy || workers.length === 0}
                  title="Export selected workers as a standalone Deno application"
                  className="rounded border border-edge-strong bg-raised px-2 py-0.5 text-xs text-fg hover:bg-hover disabled:opacity-50"
                >
                  Export app
                </button>
                <button
                  onClick={newWorker}
                  disabled={busy}
                  className="rounded border border-edge-strong bg-raised px-2 py-0.5 text-xs text-fg hover:bg-hover disabled:opacity-50"
                >
                  + New
                </button>
              </div>
            </div>
            <ul className="min-h-0 flex-1 overflow-auto">
              {workers.length === 0 && (
                <li className="px-3 py-2 text-xs text-fg-faint">No workers yet.</li>
              )}
              {workers.map((w) => {
                const b = phaseBadge(w.runtime.status);
                return (
                  <li key={w.name}>
                    <button
                      onClick={() => {
                        setSelected(w.name);
                        setShowLib(false);
                      }}
                      className={`flex w-full items-center gap-2 px-3 py-2 text-left text-sm ${
                        selected === w.name && !showLib
                          ? "bg-accent/10 font-medium text-accent-strong"
                          : "hover:bg-hover"
                      }`}
                    >
                      <span className={`h-2 w-2 shrink-0 rounded-full ${b.dot}`} />
                      <span className="min-w-0 flex-1 truncate">{w.name}</span>
                    </button>
                  </li>
                );
              })}
            </ul>
            {/* Shared library — author once, import from any worker via `@lib/…`. */}
            <div className="border-t border-edge">
              <button
                onClick={() => setShowLib(true)}
                className={`flex w-full items-center gap-2 px-3 py-2 text-left text-sm ${
                  showLib ? "bg-accent/10 font-medium text-accent-strong" : "hover:bg-hover"
                }`}
              >
                <span className="text-fg-faint">📚</span>
                <span className="min-w-0 flex-1 truncate">Shared library</span>
                <span className="font-mono text-[10px] text-fg-faint">@lib/</span>
              </button>
            </div>
          </aside>

          {/* Editor pane */}
          <div className="flex min-w-0 flex-1 flex-col">
            {showLib ? (
              <LibraryEditor flash={flash} />
            ) : current ? (
              <WorkerEditor
                key={current.name}
                worker={current}
                busy={busy}
                onStart={() => start(current.name)}
                onStop={() => stop(current.name)}
                onDelete={() => removeWorker(current.name)}
                onChanged={refresh}
                flash={flash}
              />
            ) : (
              <div className="flex flex-1 items-center justify-center text-sm text-fg-faint">
                Select a worker, or create one to start authoring.
              </div>
            )}
          </div>
        </div>
      )}

      {exportSel && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
          onClick={() => setExportSel(null)}
        >
          <div
            className="w-full max-w-md rounded-lg border border-edge-strong bg-panel shadow-xl"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="border-b border-edge px-5 py-3">
              <h2 className="text-sm font-semibold text-fg">Export workers as an application</h2>
              <p className="mt-1 text-xs text-fg-faint">
                Bundle the selected workers into a self-contained Deno app. The downloaded
                zip deploys every model in its <code>resources/</code> folder on startup,
                then runs the workers. Requires Deno to run.
              </p>
            </div>
            <ul className="max-h-64 overflow-auto px-5 py-3">
              {workers.map((w) => (
                <li key={w.name}>
                  <label className="flex cursor-pointer items-center gap-2 py-1 text-sm">
                    <input
                      type="checkbox"
                      checked={exportSel.has(w.name)}
                      onChange={() => toggleExport(w.name)}
                    />
                    <span className="truncate">{w.name}</span>
                  </label>
                </li>
              ))}
            </ul>
            <div className="flex items-center justify-between gap-2 border-t border-edge px-5 py-3">
              <span className="text-xs text-fg-faint">{exportSel.size} selected</span>
              <div className="flex gap-2">
                <Button size="sm" onClick={() => setExportSel(null)}>
                  Cancel
                </Button>
                <Button
                  variant="primary"
                  size="sm"
                  onClick={doExport}
                  disabled={busy || exportSel.size === 0}
                >
                  Download zip
                </Button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function RunningTab({
  workers,
  onStart,
  onStop,
  busy,
}: {
  workers: WorkerSummary[];
  onStart: (name: string) => void;
  onStop: (name: string) => void;
  busy: boolean;
}) {
  return (
    <div className="min-h-0 flex-1 overflow-auto p-6">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-fg-faint">
          <tr className="border-b border-edge">
            <th className="py-2 pr-4">Worker</th>
            <th className="py-2 pr-4">Status</th>
            <th className="py-2 pr-4">Completed</th>
            <th className="py-2 pr-4">Failed</th>
            <th className="py-2 pr-4">In flight</th>
            <th className="py-2 pr-4">Throughput</th>
            <th className="py-2 pr-4">Uptime</th>
            <th className="py-2 pr-4">Restarts</th>
            <th className="py-2 pr-4">Actions</th>
          </tr>
        </thead>
        <tbody>
          {workers.length === 0 && (
            <tr>
              <td colSpan={9} className="py-4 text-fg-faint">
                No workers.
              </td>
            </tr>
          )}
          {workers.map((w) => {
            const b = phaseBadge(w.runtime.status);
            const m = w.runtime.metrics;
            const active = w.runtime.status === "running" || w.runtime.status === "starting";
            return (
              <tr key={w.name} className="border-b border-edge/60">
                <td className="py-2 pr-4 font-medium">{w.name}</td>
                <td className="py-2 pr-4">
                  <span className={`rounded px-1.5 py-0.5 text-xs ${b.cls}`}>{b.label}</span>
                </td>
                <td className="py-2 pr-4 tabular-nums">{m.completed}</td>
                <td className="py-2 pr-4 tabular-nums">{m.failed}</td>
                <td className="py-2 pr-4 tabular-nums">{m.inFlight}</td>
                <td className="py-2 pr-4 tabular-nums">{m.throughput.toFixed(1)}/s</td>
                <td className="py-2 pr-4 tabular-nums">
                  {active ? fmtUptime(m.uptimeMs) : "—"}
                </td>
                <td className="py-2 pr-4 tabular-nums">{w.runtime.restarts}</td>
                <td className="py-2 pr-4">
                  {active ? (
                    <button
                      onClick={() => onStop(w.name)}
                      disabled={busy}
                      className="rounded border border-edge-strong bg-raised px-2 py-0.5 text-xs text-fg hover:bg-hover disabled:opacity-50"
                    >
                      Stop
                    </button>
                  ) : (
                    <button
                      onClick={() => onStart(w.name)}
                      disabled={busy}
                      className="rounded border border-ok/30 bg-ok/10 px-2 py-0.5 text-xs font-medium text-ok hover:bg-ok/20 disabled:opacity-50"
                    >
                      Start
                    </button>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      {workers.some((w) => w.runtime.lastError) && (
        <div className="mt-4 space-y-1 text-xs text-danger/80">
          {workers
            .filter((w) => w.runtime.lastError)
            .map((w) => (
              <div key={w.name}>
                <span className="text-fg-faint">{w.name}:</span> {w.runtime.lastError}
              </div>
            ))}
        </div>
      )}
    </div>
  );
}

function WorkerEditor({
  worker,
  busy,
  onStart,
  onStop,
  onDelete,
  onChanged,
  flash,
}: {
  worker: WorkerSummary;
  busy: boolean;
  onStart: () => void;
  onStop: () => void;
  onDelete: () => void;
  onChanged: () => void;
  flash: (kind: "ok" | "err", text: string) => void;
}) {
  const [file, setFile] = useState<string>(worker.files[0] ?? "worker.ts");
  const [content, setContent] = useState<string>("");
  const [loadedFile, setLoadedFile] = useState<string | null>(null);
  const [dirty, setDirty] = useState(false);
  // Sibling worker files + shared `@lib/` modules, fetched so the editor can
  // resolve `import "./helper.ts"` and `import "@lib/…"` with full IntelliSense.
  const [extraModels, setExtraModels] = useState<ExtraModel[]>([]);

  const filesKey = worker.files.join(",");
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const models: ExtraModel[] = [];
      // Sibling files in this worker.
      await Promise.all(
        worker.files.map(async (f) => {
          try {
            const text = (await getWorkerFile({ path: { name: worker.name }, query: { path: f }, throwOnError: true })).data;
            models.push({ path: `file:///workers/${worker.name}/${f}`, content: text });
          } catch {
            /* ignore unreadable sibling */
          }
        }),
      );
      // Shared library modules (importable as `@lib/<file>`).
      try {
        const { files } = (await listLibFiles({ throwOnError: true })).data;
        await Promise.all(
          files.map(async (f) => {
            try {
              const text = (await getLibFile({ query: { path: f }, throwOnError: true })).data;
              models.push({ path: `file:///lib/${f}`, content: text });
            } catch {
              /* ignore */
            }
          }),
        );
      } catch {
        /* library unavailable — sibling resolution still works */
      }
      if (!cancelled) setExtraModels(models);
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [worker.name, filesKey]);

  // Load the selected file's content whenever the file selection changes.
  useEffect(() => {
    let cancelled = false;
    setLoadedFile(null);
    getWorkerFile({ path: { name: worker.name }, query: { path: file }, throwOnError: true })
      .then(({ data: text }) => {
        if (!cancelled) {
          setContent(text);
          setLoadedFile(file);
          setDirty(false);
        }
      })
      .catch((e) => !cancelled && flash("err", e instanceof Error ? e.message : String(e)));
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [worker.name, file]);

  async function save() {
    try {
      await saveWorkerFile({ path: { name: worker.name }, query: { path: file }, body: content, throwOnError: true });
      setDirty(false);
      flash("ok", `Saved ${file}.`);
    } catch (e) {
      flash("err", e instanceof Error ? e.message : String(e));
    }
  }

  async function newFile() {
    const path = prompt("New file name (e.g. helper.ts):")?.trim();
    if (!path) return;
    try {
      await createWorkerFile({ path: { name: worker.name }, body: { path }, throwOnError: true });
      onChanged();
      setFile(path);
      flash("ok", `Created ${path}.`);
    } catch (e) {
      flash("err", e instanceof Error ? e.message : String(e));
    }
  }

  async function deleteFile() {
    if (!confirm(`Delete file '${file}'?`)) return;
    try {
      await deleteWorkerFile({ path: { name: worker.name }, query: { path: file }, throwOnError: true });
      onChanged();
      setFile(worker.files.find((f) => f !== file) ?? "worker.ts");
      flash("ok", `Deleted ${file}.`);
    } catch (e) {
      flash("err", e instanceof Error ? e.message : String(e));
    }
  }

  const b = phaseBadge(worker.runtime.status);
  const m = worker.runtime.metrics;
  const active = worker.runtime.status === "running" || worker.runtime.status === "starting";

  return (
    <>
      {/* Toolbar */}
      <div className="flex flex-wrap items-center gap-2 border-b border-edge px-4 py-2">
        <span className="font-medium text-fg">{worker.name}</span>
        <span className={`rounded px-1.5 py-0.5 text-xs ${b.cls}`}>{b.label}</span>
        <div className="flex-1" />
        <span className="text-xs text-fg-faint">
          {m.completed} done · {m.failed} failed · {m.throughput.toFixed(1)}/s
          {active ? ` · up ${fmtUptime(m.uptimeMs)}` : ""}
        </span>
        {active ? (
          <button
            onClick={onStop}
            disabled={busy}
            className="rounded border border-edge-strong bg-raised px-3 py-1 text-xs text-fg hover:bg-hover disabled:opacity-50"
          >
            Stop
          </button>
        ) : (
          <button
            onClick={onStart}
            disabled={busy}
            className="rounded border border-ok/30 bg-ok/10 px-3 py-1 text-xs font-medium text-ok hover:bg-ok/20 disabled:opacity-50"
          >
            Start
          </button>
        )}
        <button
          onClick={onDelete}
          disabled={busy || active}
          title={active ? "Stop the worker before deleting" : "Delete worker"}
          className="rounded border border-danger/30 bg-danger/10 px-3 py-1 text-xs text-danger hover:bg-danger/20 disabled:opacity-40"
        >
          Delete
        </button>
      </div>

      {/* File tabs */}
      <div className="flex items-center gap-1 border-b border-edge px-3 py-1.5 text-xs">
        {worker.files.map((f) => (
          <button
            key={f}
            onClick={() => setFile(f)}
            className={`rounded px-2 py-1 font-mono ${
              file === f
                ? "bg-accent/10 font-medium text-accent-strong"
                : "text-fg-muted hover:bg-hover"
            }`}
          >
            {f}
          </button>
        ))}
        <button onClick={newFile} className="ml-1 rounded px-2 py-1 text-fg-faint hover:text-fg">
          + file
        </button>
        <div className="flex-1" />
        <button
          onClick={deleteFile}
          className="rounded px-2 py-1 text-fg-faint hover:text-danger"
        >
          delete file
        </button>
        <button
          onClick={save}
          disabled={!dirty}
          className="rounded bg-accent px-3 py-1 font-medium text-on-accent hover:bg-accent-strong disabled:opacity-40"
        >
          Save{dirty ? " •" : ""}
        </button>
      </div>

      {/* Code editor (Monaco) */}
      <div className="min-h-0 flex-1 bg-[#1e1e1e]">
        <Suspense
          fallback={<div className="p-4 text-sm text-fg-faint">Loading editor…</div>}
        >
          <CodeEditor
            value={loadedFile === file ? content : ""}
            language={languageForFile(file)}
            path={`file:///workers/${worker.name}/${file}`}
            extraModels={extraModels}
            readOnly={loadedFile !== file}
            onChange={(v) => {
              setContent(v);
              setDirty(true);
            }}
            onSave={() => {
              if (dirty) void save();
            }}
          />
        </Suspense>
      </div>

      <LogPanel worker={worker.name} />
    </>
  );
}

// Shared library editor — CRUD for reusable `@lib/…` modules that every worker
// can import. Mirrors the worker file editor but without runtime controls.
function LibraryEditor({ flash }: { flash: (kind: "ok" | "err", text: string) => void }) {
  const [files, setFiles] = useState<string[]>([]);
  const [file, setFile] = useState<string | null>(null);
  const [content, setContent] = useState<string>("");
  const [loadedFile, setLoadedFile] = useState<string | null>(null);
  const [dirty, setDirty] = useState(false);
  const [extraModels, setExtraModels] = useState<ExtraModel[]>([]);

  async function loadList(select?: string) {
    try {
      const { files } = (await listLibFiles({ throwOnError: true })).data;
      setFiles(files);
      // Background models for cross-module IntelliSense within the library.
      const models: ExtraModel[] = [];
      await Promise.all(
        files.map(async (f) => {
          try {
            models.push({ path: `file:///lib/${f}`, content: (await getLibFile({ query: { path: f }, throwOnError: true })).data });
          } catch {
            /* ignore */
          }
        }),
      );
      setExtraModels(models);
      if (select) setFile(select);
      else if (files.length && !files.includes(file ?? "")) setFile(files[0]);
      else if (!files.length) setFile(null);
    } catch (e) {
      flash("err", e instanceof Error ? e.message : String(e));
    }
  }

  useEffect(() => {
    void loadList();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!file) {
      setContent("");
      setLoadedFile(null);
      return;
    }
    let cancelled = false;
    setLoadedFile(null);
    getLibFile({ query: { path: file }, throwOnError: true })
      .then(({ data: text }) => {
        if (!cancelled) {
          setContent(text);
          setLoadedFile(file);
          setDirty(false);
        }
      })
      .catch((e) => !cancelled && flash("err", e instanceof Error ? e.message : String(e)));
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [file]);

  async function save() {
    if (!file) return;
    try {
      await saveLibFile({ query: { path: file }, body: content, throwOnError: true });
      setDirty(false);
      flash("ok", `Saved @lib/${file}.`);
      void loadList(file);
    } catch (e) {
      flash("err", e instanceof Error ? e.message : String(e));
    }
  }

  async function newFile() {
    const path = prompt("New library file name (e.g. money.ts):")?.trim();
    if (!path) return;
    try {
      await createLibFile({ body: { path }, throwOnError: true });
      await loadList(path);
      flash("ok", `Created @lib/${path}.`);
    } catch (e) {
      flash("err", e instanceof Error ? e.message : String(e));
    }
  }

  async function deleteFile() {
    if (!file) return;
    if (!confirm(`Delete shared library file '@lib/${file}'?`)) return;
    try {
      await deleteLibFile({ query: { path: file }, throwOnError: true });
      flash("ok", `Deleted @lib/${file}.`);
      setFile(null);
      await loadList();
    } catch (e) {
      flash("err", e instanceof Error ? e.message : String(e));
    }
  }

  return (
    <>
      <div className="flex flex-wrap items-center gap-2 border-b border-edge px-4 py-2">
        <span className="font-medium text-fg">Shared library</span>
        <span className="rounded bg-hover px-1.5 py-0.5 font-mono text-xs text-fg-muted">
          import … from "@lib/…"
        </span>
        <div className="flex-1" />
        <span className="text-xs text-fg-faint">Reusable across every worker.</span>
      </div>

      <div className="flex items-center gap-1 border-b border-edge px-3 py-1.5 text-xs">
        {files.map((f) => (
          <button
            key={f}
            onClick={() => setFile(f)}
            className={`rounded px-2 py-1 font-mono ${
              file === f
                ? "bg-accent/10 font-medium text-accent-strong"
                : "text-fg-muted hover:bg-hover"
            }`}
          >
            {f}
          </button>
        ))}
        <button onClick={newFile} className="ml-1 rounded px-2 py-1 text-fg-faint hover:text-fg">
          + file
        </button>
        <div className="flex-1" />
        {file && (
          <button
            onClick={deleteFile}
            className="rounded px-2 py-1 text-fg-faint hover:text-danger"
          >
            delete file
          </button>
        )}
        <button
          onClick={save}
          disabled={!dirty || !file}
          className="rounded bg-accent px-3 py-1 font-medium text-on-accent hover:bg-accent-strong disabled:opacity-40"
        >
          Save{dirty ? " •" : ""}
        </button>
      </div>

      <div className="min-h-0 flex-1 bg-[#1e1e1e]">
        {file ? (
          <Suspense
            fallback={<div className="p-4 text-sm text-fg-faint">Loading editor…</div>}
          >
            <CodeEditor
              value={loadedFile === file ? content : ""}
              language={languageForFile(file)}
              path={`file:///lib/${file}`}
              extraModels={extraModels}
              readOnly={loadedFile !== file}
              onChange={(v) => {
                setContent(v);
                setDirty(true);
              }}
              onSave={() => {
                if (dirty) void save();
              }}
            />
          </Suspense>
        ) : (
          <div className="flex h-full flex-col items-center justify-center gap-2 text-sm text-fg-faint">
            <p>No shared library files yet.</p>
            <Button size="sm" onClick={newFile}>
              + New library file
            </Button>
            <p className="max-w-sm text-center text-xs text-fg-faint">
              Drop a module here and import it from any worker with{" "}
              <span className="font-mono">import {"{ … }"} from "@lib/your-file.ts"</span>.
            </p>
          </div>
        )}
      </div>
    </>
  );
}

function LogPanel({ worker }: { worker: string }) {
  const [lines, setLines] = useState<WorkerLogLine[]>([]);
  const endRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setLines([]);
    const src = new EventSource(`/console/api/workers/${encodeURIComponent(worker)}/logs`);
    src.addEventListener("log", (ev) => {
      try {
        const line = JSON.parse((ev as MessageEvent).data) as WorkerLogLine;
        setLines((prev) => {
          const next = prev.length > 400 ? prev.slice(-400) : prev;
          return [...next, line];
        });
      } catch {
        /* ignore malformed */
      }
    });
    return () => src.close();
  }, [worker]);

  useEffect(() => {
    endRef.current?.scrollIntoView({ block: "end" });
  }, [lines]);

  return (
    <div className="h-44 shrink-0 overflow-auto border-t border-edge bg-inset p-2 font-mono text-xs">
      {lines.length === 0 && <div className="text-fg-faint">No log output yet.</div>}
      {lines.map((l, i) => (
        <div
          key={i}
          className={
            l.stream === "err"
              ? "text-danger"
              : l.stream === "sys"
                ? "text-info/80"
                : "text-fg-muted"
          }
        >
          <span className="mr-2 text-fg-faint">
            {new Date(l.tsMs).toLocaleTimeString()}
          </span>
          {l.text}
        </div>
      ))}
      <div ref={endRef} />
    </div>
  );
}
