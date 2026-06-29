import { useCallback, useEffect, useRef, useState, lazy, Suspense } from "react";
import { Link, useParams } from "react-router-dom";
import CodeEditor, { languageForFile } from "../components/CodeEditor";
import BpmnModeler, { type BpmnModelerHandle } from "../components/BpmnModeler";
import DmnModeler, { type DmnModelerHandle } from "../components/DmnModeler";
import FormEditor, { type FormEditorHandle } from "../components/FormEditor";
const TestRunPanel = lazy(() => import("../components/TestRunPanel"));
import {
  projectsApi,
  projectLogs,
  exportProject,
  type FileNode,
  type ProjectDetail,
  type ProjectConfig,
  type RunState,
  type ProjectLogLine,
} from "../lib/api";

/// One project's workspace: file browser, graphical/code editors, the run
/// console, and the Run/Stop/Compile/Configure/Export toolbar.
export default function ProjectWorkspace() {
  const { name = "" } = useParams();
  const [detail, setDetail] = useState<ProjectDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [runState, setRunState] = useState<RunState | null>(null);
  const [logs, setLogs] = useState<ProjectLogLine[]>([]);
  const [showConfig, setShowConfig] = useState(false);
  const [showCompile, setShowCompile] = useState(false);
  const logRef = useRef<HTMLDivElement>(null);

  const reloadFiles = useCallback(async () => {
    try {
      const res = await projectsApi.projectFiles(name);
      setDetail((d) => (d ? { ...d, files: res.files } : d));
    } catch {
      /* ignore */
    }
  }, [name]);

  const load = useCallback(async () => {
    try {
      const d = await projectsApi.project(name);
      setDetail(d);
      setRunState(d.runState);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [name]);

  useEffect(() => {
    void load();
  }, [load]);

  // Stream run/compile logs for the life of the workspace.
  useEffect(() => {
    if (!name) return;
    const src = projectLogs(name, (line) => {
      setLogs((prev) => {
        const next = prev.length > 2000 ? prev.slice(prev.length - 2000) : prev.slice();
        next.push(line);
        return next;
      });
    });
    return () => src.close();
  }, [name]);

  // Poll run state while the app is active (run/stop/compile flip it async).
  useEffect(() => {
    const active =
      runState && (runState.status !== "stopped" || runState.compiling);
    if (!active) return;
    const t = setInterval(async () => {
      try {
        const d = await projectsApi.project(name);
        setRunState(d.runState);
      } catch {
        /* ignore */
      }
    }, 1500);
    return () => clearInterval(t);
  }, [name, runState]);

  useEffect(() => {
    logRef.current?.scrollTo({ top: logRef.current.scrollHeight });
  }, [logs]);

  const denoAvailable = detail?.denoAvailable ?? false;
  const running = runState?.status === "running" || runState?.status === "starting";
  const compiling = runState?.compiling ?? false;

  const run = async () => {
    setLogs([]);
    try {
      setRunState(await projectsApi.runProject(name));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };
  const stop = async () => {
    try {
      setRunState(await projectsApi.stopProject(name));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  if (error) {
    return (
      <div className="p-8">
        <Link to="/projects" className="text-sm text-violet-400 hover:underline">
          ← Projects
        </Link>
        <div className="mt-4 rounded-md border border-red-500/40 bg-red-500/10 px-4 py-3 text-sm text-red-300">
          {error}
        </div>
      </div>
    );
  }

  if (!detail) {
    return <div className="p-8 text-sm text-zinc-500">Loading…</div>;
  }

  return (
    <div className="flex h-full flex-col">
      {/* Toolbar */}
      <div className="flex items-center gap-2 border-b border-zinc-800 bg-zinc-900 px-4 py-2">
        <Link to="/projects" className="text-sm text-zinc-500 hover:text-zinc-300">
          ← Projects
        </Link>
        <span className="text-sm font-semibold text-zinc-100">{detail.config.name}</span>
        {running && (
          <span className="inline-flex items-center gap-1 rounded-full bg-emerald-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-emerald-400">
            <span className="h-1.5 w-1.5 rounded-full bg-emerald-400" /> running
          </span>
        )}
        {compiling && (
          <span className="rounded-full bg-sky-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-sky-400">
            compiling…
          </span>
        )}
        <div className="flex-1" />
        {running ? (
          <ToolbarButton onClick={() => void stop()} kind="danger">
            ■ Stop
          </ToolbarButton>
        ) : (
          <ToolbarButton onClick={() => void run()} kind="primary" disabled={!denoAvailable}>
            ▶ Run
          </ToolbarButton>
        )}
        <ToolbarButton onClick={() => setShowCompile(true)} disabled={!denoAvailable || compiling}>
          Compile
        </ToolbarButton>
        <ToolbarButton onClick={() => setShowConfig(true)}>Configure</ToolbarButton>
        <ToolbarButton onClick={() => void exportProject(name, false)}>Export</ToolbarButton>
      </div>

      {!denoAvailable && (
        <div className="border-b border-amber-500/30 bg-amber-500/10 px-4 py-1.5 text-xs text-amber-300">
          No Deno runtime detected — Run and Compile are disabled. Authoring and Export still work.
        </div>
      )}

      <div className="flex min-h-0 flex-1">
        {/* File tree */}
        <FileBrowser
          name={name}
          files={detail.files}
          selected={selected}
          onSelect={setSelected}
          onChanged={reloadFiles}
        />

        {/* Editor + console */}
        <div className="flex min-w-0 flex-1 flex-col">
          <div className="min-h-0 flex-1 overflow-hidden border-b border-zinc-800">
            {selected ? (
              <EditorPane key={selected} name={name} path={selected} />
            ) : (
              <div className="flex h-full items-center justify-center text-sm text-zinc-600">
                Select a file to edit
              </div>
            )}
          </div>
          <RunConsole logs={logs} forwardRef={logRef} onClear={() => setLogs([])} />
        </div>
      </div>

      {showConfig && (
        <ConfigModal
          name={name}
          config={detail.config}
          platforms={detail.platforms}
          onClose={() => setShowConfig(false)}
          onSaved={(cfg) => {
            setDetail((d) => (d ? { ...d, config: cfg } : d));
            setShowConfig(false);
          }}
        />
      )}
      {showCompile && (
        <CompileModal
          name={name}
          config={detail.config}
          platforms={detail.platforms}
          onClose={() => setShowCompile(false)}
          onStarted={() => {
            setShowCompile(false);
            void load();
          }}
        />
      )}
    </div>
  );
}

function ToolbarButton({
  children,
  onClick,
  disabled,
  kind = "default",
}: {
  children: React.ReactNode;
  onClick: () => void;
  disabled?: boolean;
  kind?: "default" | "primary" | "danger";
}) {
  const styles =
    kind === "primary"
      ? "bg-emerald-600 text-white hover:bg-emerald-500"
      : kind === "danger"
        ? "bg-red-600 text-white hover:bg-red-500"
        : "border border-zinc-700 text-zinc-200 hover:bg-zinc-800";
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={`rounded-md px-3 py-1.5 text-sm font-medium transition-colors disabled:cursor-not-allowed disabled:opacity-40 ${styles}`}
    >
      {children}
    </button>
  );
}

// --- File browser ----------------------------------------------------------

function FileBrowser({
  name,
  files,
  selected,
  onSelect,
  onChanged,
}: {
  name: string;
  files: FileNode[];
  selected: string | null;
  onSelect: (path: string) => void;
  onChanged: () => void;
}) {
  const newFile = async (dir: boolean) => {
    const base = prompt(
      dir ? "New folder path (project-relative):" : "New file path (project-relative):",
      dir ? "resources/processes/" : "resources/processes/new.bpmn",
    );
    if (!base) return;
    try {
      await projectsApi.createProjectPath(name, base, dir);
      onChanged();
      if (!dir) onSelect(base);
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
    }
  };
  const del = async (path: string) => {
    if (!confirm(`Delete ${path}?`)) return;
    try {
      await projectsApi.deleteProjectPath(name, path);
      onChanged();
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <aside className="flex w-64 shrink-0 flex-col border-r border-zinc-800 bg-zinc-900">
      <div className="flex items-center justify-between px-3 py-2 text-xs uppercase tracking-wider text-zinc-500">
        <span>Files</span>
        <div className="flex gap-1">
          <button title="New file" onClick={() => void newFile(false)} className="rounded px-1.5 py-0.5 hover:bg-zinc-800 hover:text-zinc-200">
            ＋
          </button>
          <button title="New folder" onClick={() => void newFile(true)} className="rounded px-1.5 py-0.5 hover:bg-zinc-800 hover:text-zinc-200">
            ⊞
          </button>
        </div>
      </div>
      <div className="min-h-0 flex-1 overflow-auto px-1 pb-2">
        <FileTree nodes={files} depth={0} selected={selected} onSelect={onSelect} onDelete={del} />
      </div>
    </aside>
  );
}

function FileTree({
  nodes,
  depth,
  selected,
  onSelect,
  onDelete,
}: {
  nodes: FileNode[];
  depth: number;
  selected: string | null;
  onSelect: (p: string) => void;
  onDelete: (p: string) => void;
}) {
  return (
    <ul>
      {nodes.map((node) => (
        <TreeNode
          key={node.path}
          node={node}
          depth={depth}
          selected={selected}
          onSelect={onSelect}
          onDelete={onDelete}
        />
      ))}
    </ul>
  );
}

function TreeNode({
  node,
  depth,
  selected,
  onSelect,
  onDelete,
}: {
  node: FileNode;
  depth: number;
  selected: string | null;
  onSelect: (p: string) => void;
  onDelete: (p: string) => void;
}) {
  const [open, setOpen] = useState(depth < 2);
  const pad = { paddingLeft: `${depth * 12 + 8}px` };
  if (node.kind === "dir") {
    return (
      <li>
        <div
          style={pad}
          onClick={() => setOpen((v) => !v)}
          className="group flex cursor-pointer items-center gap-1 rounded py-1 pr-2 text-sm text-zinc-300 hover:bg-zinc-800/60"
        >
          <span className="text-zinc-500">{open ? "▾" : "▸"}</span>
          <span className="truncate">{node.name}</span>
        </div>
        {open && node.children && (
          <FileTree nodes={node.children} depth={depth + 1} selected={selected} onSelect={onSelect} onDelete={onDelete} />
        )}
      </li>
    );
  }
  const active = selected === node.path;
  return (
    <li>
      <div
        style={pad}
        className={`group flex cursor-pointer items-center gap-1 rounded py-1 pr-2 text-sm ${
          active ? "bg-zinc-800 text-white" : "text-zinc-400 hover:bg-zinc-800/60 hover:text-zinc-200"
        }`}
        onClick={() => onSelect(node.path)}
      >
        <span className="opacity-0">▸</span>
        <span className="truncate">{node.name}</span>
        <span className="flex-1" />
        <button
          onClick={(e) => {
            e.stopPropagation();
            onDelete(node.path);
          }}
          className="hidden rounded px-1 text-xs text-zinc-500 hover:text-red-400 group-hover:block"
        >
          ✕
        </button>
      </div>
    </li>
  );
}

// --- Editor dispatch -------------------------------------------------------

function EditorPane({ name, path }: { name: string; path: string }) {
  const [content, setContent] = useState<string | null>(null);
  const [dirty, setDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const bpmnRef = useRef<BpmnModelerHandle>(null);
  const dmnRef = useRef<DmnModelerHandle>(null);
  const formRef = useRef<FormEditorHandle>(null);
  const [testXml, setTestXml] = useState<string | null>(null);

  const ext = path.split(".").pop()?.toLowerCase() ?? "";
  const kind: "bpmn" | "dmn" | "form" | "code" =
    ext === "bpmn" ? "bpmn" : ext === "dmn" ? "dmn" : ext === "form" ? "form" : "code";

  useEffect(() => {
    let alive = true;
    setContent(null);
    setDirty(false);
    setLoadError(null);
    projectsApi
      .projectFile(name, path)
      .then((text) => alive && setContent(text))
      .catch((e) => alive && setLoadError(e instanceof Error ? e.message : String(e)));
    return () => {
      alive = false;
    };
  }, [name, path]);

  // Load the fetched document into the graphical editor once mounted.
  useEffect(() => {
    if (content == null) return;
    if (kind === "bpmn" && bpmnRef.current) {
      void bpmnRef.current.importXml(content).catch(() => void 0);
    } else if (kind === "dmn" && dmnRef.current) {
      void dmnRef.current.importXml(content).catch(() => void 0);
    } else if (kind === "form" && formRef.current) {
      void formRef.current.importSchema(content || "{}").catch(() => void 0);
    }
    // Only when the document first arrives for this path.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [content, kind]);

  const save = useCallback(async () => {
    setSaving(true);
    try {
      let body = content ?? "";
      if (kind === "bpmn" && bpmnRef.current) body = await bpmnRef.current.getXml();
      else if (kind === "dmn" && dmnRef.current) body = await dmnRef.current.getXml();
      else if (kind === "form" && formRef.current) body = await formRef.current.getSchema();
      await projectsApi.saveProjectFile(name, path, body);
      setContent(body);
      setDirty(false);
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }, [content, kind, name, path]);

  // Cmd/Ctrl+S saves graphical editors too (CodeEditor has its own binding).
  useEffect(() => {
    if (kind === "code") return;
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "s") {
        e.preventDefault();
        void save();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [kind, save]);

  if (loadError) {
    return <div className="p-6 text-sm text-red-400">{loadError}</div>;
  }
  if (content == null) {
    return <div className="p-6 text-sm text-zinc-500">Loading {path}…</div>;
  }

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center gap-3 border-b border-zinc-800 bg-zinc-900/60 px-3 py-1.5">
        <span className="truncate font-mono text-xs text-zinc-400">{path}</span>
        {dirty && <span className="text-[10px] text-amber-400">● unsaved</span>}
        <div className="flex-1" />
        {kind === "bpmn" && (
          <button
            onClick={async () => {
              const xml = await bpmnRef.current?.getXml();
              if (xml) setTestXml(xml);
            }}
            className="rounded-md border border-zinc-700 px-3 py-1 text-xs font-medium text-zinc-200 transition-colors hover:border-emerald-500 hover:text-emerald-300"
          >
            Test
          </button>
        )}
        <button
          onClick={() => void save()}
          disabled={saving || (kind === "code" && !dirty)}
          className="rounded-md bg-violet-600 px-3 py-1 text-xs font-medium text-white transition-colors hover:bg-violet-500 disabled:opacity-40"
        >
          {saving ? "Saving…" : "Save"}
        </button>
      </div>
      <div className="min-h-0 flex-1">
        {kind === "bpmn" && (
          <div className="relative h-full">
            <BpmnModeler ref={bpmnRef} onChange={() => setDirty(true)} />
            {testXml && (
              <div className="absolute inset-0 z-10 bg-zinc-950">
                <Suspense
                  fallback={
                    <div className="flex h-full items-center justify-center text-sm text-zinc-500">
                      Loading the in-browser engine…
                    </div>
                  }
                >
                  <TestRunPanel xml={testXml} onClose={() => setTestXml(null)} />
                </Suspense>
              </div>
            )}
          </div>
        )}
        {kind === "dmn" && <DmnModeler ref={dmnRef} onChange={() => setDirty(true)} />}
        {kind === "form" && <FormEditor ref={formRef} onChange={() => setDirty(true)} />}
        {kind === "code" && (
          <CodeEditor
            value={content}
            language={languageForFile(path)}
            path={`file:///${name}/${path}`}
            onChange={(v) => {
              setContent(v);
              setDirty(true);
            }}
            onSave={() => void save()}
          />
        )}
      </div>
    </div>
  );
}

// --- Run console -----------------------------------------------------------

function RunConsole({
  logs,
  forwardRef,
  onClear,
}: {
  logs: ProjectLogLine[];
  forwardRef: React.RefObject<HTMLDivElement>;
  onClear: () => void;
}) {
  return (
    <div className="flex h-56 shrink-0 flex-col bg-zinc-950">
      <div className="flex items-center justify-between border-b border-zinc-800 px-3 py-1 text-xs uppercase tracking-wider text-zinc-500">
        <span>Output</span>
        <button onClick={onClear} className="rounded px-1.5 py-0.5 hover:bg-zinc-800 hover:text-zinc-300">
          Clear
        </button>
      </div>
      <div ref={forwardRef} className="min-h-0 flex-1 overflow-auto px-3 py-2 font-mono text-xs leading-relaxed">
        {logs.length === 0 ? (
          <div className="text-zinc-600">No output yet. Run the application to see logs.</div>
        ) : (
          logs.map((l, i) => (
            <div
              key={i}
              className={
                l.stream === "err"
                  ? "whitespace-pre-wrap text-red-400"
                  : l.stream === "sys"
                    ? "whitespace-pre-wrap text-sky-400"
                    : "whitespace-pre-wrap text-zinc-300"
              }
            >
              {l.text}
            </div>
          ))
        )}
      </div>
    </div>
  );
}

// --- Configure modal -------------------------------------------------------

function ConfigModal({
  name,
  config,
  platforms,
  onClose,
  onSaved,
}: {
  name: string;
  config: ProjectConfig;
  platforms: string[];
  onClose: () => void;
  onSaved: (cfg: ProjectConfig) => void;
}) {
  const [deployTarget, setDeployTarget] = useState(config.deployTarget);
  const [main, setMain] = useState(config.main);
  const [desc, setDesc] = useState(config.description);
  const [selected, setSelected] = useState<string[]>(config.platforms);
  const [busy, setBusy] = useState(false);

  const toggle = (t: string) =>
    setSelected((s) => (s.includes(t) ? s.filter((x) => x !== t) : [...s, t]));

  const save = async () => {
    setBusy(true);
    try {
      const cfg = await projectsApi.saveProjectConfig(name, {
        ...config,
        description: desc,
        deployTarget,
        main,
        platforms: selected,
      });
      onSaved(cfg);
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  };

  return (
    <Modal title="Configure project" onClose={onClose}>
      <label className="block text-xs uppercase tracking-wider text-zinc-500">Description</label>
      <input value={desc} onChange={(e) => setDesc(e.target.value)} className={inputCls} />
      <label className="mt-3 block text-xs uppercase tracking-wider text-zinc-500">Deploy target</label>
      <input value={deployTarget} onChange={(e) => setDeployTarget(e.target.value)} className={inputCls} placeholder="http://localhost:8080" />
      <p className="mt-1 text-[11px] text-zinc-600">REST API at &lt;target&gt;/v2; the command stream is dialled here too.</p>
      <label className="mt-3 block text-xs uppercase tracking-wider text-zinc-500">Entry point</label>
      <input value={main} onChange={(e) => setMain(e.target.value)} className={inputCls} placeholder="main.ts" />
      <label className="mt-3 block text-xs uppercase tracking-wider text-zinc-500">Export platforms</label>
      <PlatformPicker platforms={platforms} selected={selected} onToggle={toggle} />
      <div className="mt-5 flex justify-end gap-2">
        <button onClick={onClose} className="rounded-md border border-zinc-700 px-4 py-2 text-sm text-zinc-300 hover:bg-zinc-800">
          Cancel
        </button>
        <button onClick={() => void save()} disabled={busy} className="rounded-md bg-violet-600 px-4 py-2 text-sm font-medium text-white hover:bg-violet-500 disabled:opacity-50">
          {busy ? "Saving…" : "Save"}
        </button>
      </div>
    </Modal>
  );
}

// --- Compile modal ---------------------------------------------------------

function CompileModal({
  name,
  config,
  platforms,
  onClose,
  onStarted,
}: {
  name: string;
  config: ProjectConfig;
  platforms: string[];
  onClose: () => void;
  onStarted: () => void;
}) {
  const [selected, setSelected] = useState<string[]>(
    config.platforms.length ? config.platforms : [],
  );
  const [busy, setBusy] = useState(false);
  const toggle = (t: string) =>
    setSelected((s) => (s.includes(t) ? s.filter((x) => x !== t) : [...s, t]));

  const compile = async () => {
    setBusy(true);
    try {
      await projectsApi.compileProject(name, selected);
      onStarted();
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  };

  return (
    <Modal title="Compile application" onClose={onClose}>
      <p className="text-sm text-zinc-400">
        Produces standalone binaries under <span className="font-mono text-zinc-300">dist/</span>. Leave all
        unchecked to compile for this host only. Cross-compiling downloads the
        Deno runtime per target and may take a few minutes — progress streams to
        the Output panel.
      </p>
      <div className="mt-3">
        <PlatformPicker platforms={platforms} selected={selected} onToggle={toggle} />
      </div>
      <div className="mt-5 flex justify-end gap-2">
        <button onClick={onClose} className="rounded-md border border-zinc-700 px-4 py-2 text-sm text-zinc-300 hover:bg-zinc-800">
          Cancel
        </button>
        <button onClick={() => void compile()} disabled={busy} className="rounded-md bg-violet-600 px-4 py-2 text-sm font-medium text-white hover:bg-violet-500 disabled:opacity-50">
          {busy ? "Starting…" : "Compile"}
        </button>
      </div>
    </Modal>
  );
}

/// Human labels for the Deno target triples the backend advertises.
const PLATFORM_LABELS: Record<string, string> = {
  "aarch64-apple-darwin": "macOS (Apple Silicon)",
  "x86_64-apple-darwin": "macOS (Intel)",
  "x86_64-unknown-linux-gnu": "Linux (x64)",
  "aarch64-unknown-linux-gnu": "Linux (ARM64)",
  "x86_64-pc-windows-msvc": "Windows (x64)",
};

function PlatformPicker({
  platforms,
  selected,
  onToggle,
}: {
  platforms: string[];
  selected: string[];
  onToggle: (t: string) => void;
}) {
  return (
    <div className="mt-1 grid grid-cols-1 gap-1 sm:grid-cols-2">
      {platforms.map((t) => (
        <label key={t} className="flex cursor-pointer items-center gap-2 rounded-md border border-zinc-800 px-3 py-2 text-sm text-zinc-300 hover:border-zinc-600">
          <input
            type="checkbox"
            checked={selected.includes(t)}
            onChange={() => onToggle(t)}
            className="accent-violet-500"
          />
          <span>{PLATFORM_LABELS[t] ?? t}</span>
        </label>
      ))}
    </div>
  );
}

function Modal({
  title,
  children,
  onClose,
}: {
  title: string;
  children: React.ReactNode;
  onClose: () => void;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4" onClick={onClose}>
      <div className="w-full max-w-lg rounded-lg border border-zinc-800 bg-zinc-900 p-5 shadow-xl" onClick={(e) => e.stopPropagation()}>
        <h2 className="mb-4 text-lg font-semibold text-zinc-100">{title}</h2>
        {children}
      </div>
    </div>
  );
}

const inputCls =
  "mt-1 w-full rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-violet-500";
