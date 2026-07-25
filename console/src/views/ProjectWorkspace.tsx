import { useCallback, useEffect, useMemo, useRef, useState, lazy, Suspense } from "react";
import { Link, useParams } from "react-router-dom";
import CodeEditor, { languageForFile } from "../components/CodeEditor";
import MarkdownPreview from "../components/MarkdownPreview";
import BpmnModeler, {
  type BpmnModelerHandle,
  type DomainTypeBinding,
} from "../components/BpmnModeler";
import DmnModeler, { type DmnModelerHandle } from "../components/DmnModeler";
import FormEditor, { type FormEditorHandle } from "../components/FormEditor";
import FormPreview from "../components/FormPreview";
import AppManifestEditor, { isAppManifestPath } from "../components/AppManifestEditor";
const TestRunPanel = lazy(() => import("../components/TestRunPanel"));
const DataPanel = lazy(() => import("../components/DataPanel"));
const TriggersPanel = lazy(() => import("../components/TriggersPanel"));
import {
  compileProject,
  createProjectPath,
  deleteProjectPath,
  getProject,
  listProjectFiles,
  runProject,
  saveProjectConfig,
  saveProjectFile,
  setActiveRunConfig,
  stopProject,
  type FileNode,
  type ProjectDetail,
  type ProjectConfig,
  type RunState,
} from "../gen";
import {
  projectLogs,
  exportProject,
  deployXml,
  createProcessInstance,
  fetchDeployedXmlByProcessId,
  projectFileEx,
  type ProjectLogLine,
  type ProjectFile,
} from "../lib/api";
import { Button, inputClass } from "../components/ui";
import { decisionFeelVariables } from "../lib/dmnDomainVariables";
import { processFeelVariables, componentOutputFeelVariables, type ComponentOutput } from "../lib/bpmnDomainVariables";
import { loadProjectComponents, loadPackComponents, combineComponents, type ElementTemplate } from "../lib/projectComponents";
import {
  subscribe as subscribeDebug,
  snapshot as debugSnapshot,
  clear as clearDebug,
  debug,
  type DebugEntry,
} from "../lib/debugBus";

/// One project's workspace: file browser, graphical/code editors, the run
/// console, and the Run/Stop/Compile/Configure/Export toolbar.
export default function ProjectWorkspace() {
  const { name = "" } = useParams();
  const [detail, setDetail] = useState<ProjectDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  // Remember the open file per project so leaving the workspace (e.g. to peek
  // at Metrics) and coming back restores the same editor tab.
  const selectedKey = `nano.project.${name}.openFile`;
  const [selected, setSelectedState] = useState<string | null>(
    () => localStorage.getItem(selectedKey),
  );
  const setSelected = useCallback(
    (path: string | null) => {
      setSelectedState(path);
      if (path) localStorage.setItem(selectedKey, path);
      else localStorage.removeItem(selectedKey);
    },
    [selectedKey],
  );
  const [runState, setRunState] = useState<RunState | null>(null);
  const [logs, setLogs] = useState<ProjectLogLine[]>([]);
  const [showConfig, setShowConfig] = useState(false);
  const [showCompile, setShowCompile] = useState(false);
  const [showData, setShowData] = useState(false);
  const [showTriggers, setShowTriggers] = useState(false);
  const logRef = useRef<HTMLDivElement>(null);
  const [consoleHeight, setConsoleHeight] = useState(() => {
    const saved = Number(localStorage.getItem("nano.consoleHeight"));
    return saved >= 80 && saved <= 1200 ? saved : 224;
  });
  const dragging = useRef(false);
  const startDrag = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    dragging.current = true;
    const onMove = (ev: MouseEvent) => {
      if (!dragging.current) return;
      const next = Math.min(Math.max(window.innerHeight - ev.clientY, 80), 1200);
      setConsoleHeight(next);
    };
    const onUp = () => {
      dragging.current = false;
      localStorage.setItem("nano.consoleHeight", String(consoleHeightRef.current));
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, []);
  const consoleHeightRef = useRef(consoleHeight);
  consoleHeightRef.current = consoleHeight;

  const reloadFiles = useCallback(async () => {
    try {
      const res = (await listProjectFiles({ path: { name }, throwOnError: true })).data;
      setDetail((d) => (d ? { ...d, files: res.files } : d));
    } catch {
      /* ignore */
    }
  }, [name]);

  const load = useCallback(async () => {
    try {
      const d = (await getProject({ path: { name }, throwOnError: true })).data;
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

  // Drop a remembered open-file selection if that file no longer exists (e.g.
  // it was deleted since the last visit), so we don't render a 404 editor pane.
  useEffect(() => {
    if (!detail || !selected) return;
    const exists = (nodes: FileNode[]): boolean =>
      nodes.some(
        (n) =>
          n.path === selected || (n.children ? exists(n.children) : false),
      );
    if (!exists(detail.files)) setSelected(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [detail]);

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
        const d = (await getProject({ path: { name }, throwOnError: true })).data;
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

  const runnable = detail?.runnable ?? false;
  const lang = detail?.config.lang ?? "deno";
  const running = runState?.status === "running" || runState?.status === "starting";
  const compiling = runState?.compiling ?? false;
  const runConfigs = detail?.config.toolchain?.runConfigs ?? [];
  // The server resolves the same fallback (pinned → default:true → first) at
  // resolve_run_argv time, but we mirror it here so the picker's initial
  // value matches what Run would actually spawn. Also validate the pinned
  // id against the current runConfigs — a hand-edited nanobpm.project.json
  // could name an unknown id, in which case the server falls back but the
  // controlled <select> would render blank/out-of-sync without this check.
  const pinnedId = detail?.config.toolchain?.activeRunConfig ?? null;
  const activeRunConfigId =
    (pinnedId && runConfigs.some((c) => c.id === pinnedId) ? pinnedId : null) ??
    runConfigs.find((c) => c.default)?.id ??
    runConfigs[0]?.id ??
    null;

  const changeRunConfig = async (id: string | null) => {
    try {
      await setActiveRunConfig({ path: { name }, body: { id }, throwOnError: true });
      void load();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const run = async () => {
    setLogs([]);
    try {
      setRunState((await runProject({ path: { name }, throwOnError: true })).data);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };
  const stop = async () => {
    try {
      setRunState((await stopProject({ path: { name }, throwOnError: true })).data);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };
  // The cross-compile modal is Deno-specific — its copy talks about downloading
  // the Deno runtime per target, and its picker only knows Deno target triples.
  // Skip it whenever this project isn't Deno: either it has a snapshotted
  // toolchain (Java/Rust/… scaffolded from an app pack that declared its own
  // compile argv) OR its lang pack drives compile (cfg.lang != "deno"). Either
  // way there's one canonical compile invocation and no per-platform variant
  // for the user to choose from.
  const handleCompile = async () => {
    // A project is Deno-scaffolded when neither the snapshotted toolchain
    // (flat `compile` or any runConfig with compile argv) nor its lang pack
    // drives compile — i.e. lang == "deno" and no argv override. Anything
    // else has a canonical compile and skips the cross-compile modal.
    const hasRunConfigCompile = (detail?.config.toolchain?.runConfigs ?? []).some(
      (c) => (c.compile?.length ?? 0) > 0,
    );
    const isDenoProject =
      !detail?.config.toolchain?.compile?.length &&
      !hasRunConfigCompile &&
      (!detail?.config.lang || detail.config.lang === "deno");
    if (!isDenoProject) {
      try {
        await compileProject({ path: { name }, body: { targets: [] }, throwOnError: true });
        void load();
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      }
      return;
    }
    setShowCompile(true);
  };

  if (error) {
    return (
      <div className="p-8">
        <Link to="/projects" className="text-sm text-accent-strong hover:underline">
          ← Projects
        </Link>
        <div className="mt-4 rounded-md border border-danger/40 bg-danger/10 px-4 py-3 text-sm text-danger">
          {error}
        </div>
      </div>
    );
  }

  if (!detail) {
    return <div className="p-8 text-sm text-fg-faint">Loading…</div>;
  }

  // Urban App projects carry a root `nano.app.json`; only those have
  // datasources, so the DB Manager (Data) toolbar entry is shown for them.
  const isUrbanApp = detail.files.some(
    (f) => f.kind === "file" && f.name === "nano.app.json",
  );

  return (
    <div className="flex h-full flex-col">
      {/* Toolbar */}
      <div className="flex items-center gap-2 border-b border-edge bg-panel px-4 py-2">
        <nav className="flex items-center gap-1.5 text-sm" aria-label="Breadcrumb">
          <Link to="/projects" className="text-fg-faint hover:text-fg-muted">
            Projects
          </Link>
          <span className="text-fg-faint">/</span>
          <span className="font-semibold text-fg">{detail.config.name}</span>
        </nav>
        {running && (
          <span className="inline-flex items-center gap-1 rounded-full bg-ok/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-ok">
            <span className="h-1.5 w-1.5 rounded-full bg-ok" /> running
          </span>
        )}
        {compiling && (
          <span className="rounded-full bg-info/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-info">
            compiling…
          </span>
        )}
        <div className="flex-1" />
        {runConfigs.length > 0 && (
          <label
            className="flex items-center gap-1.5 text-xs text-fg-faint"
            title="Named run configuration from the pack — drives Run and Compile. Persists in nanobpm.project.json."
          >
            <span className="hidden sm:inline">Config:</span>
            <select
              value={activeRunConfigId ?? ""}
              onChange={(e) => void changeRunConfig(e.target.value || null)}
              disabled={running || compiling}
              className="rounded-md border border-edge-strong bg-bg-subtle px-2 py-1 text-sm text-fg hover:bg-hover disabled:cursor-not-allowed disabled:opacity-40"
            >
              {runConfigs.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.label}
                </option>
              ))}
            </select>
          </label>
        )}
        {running ? (
          <ToolbarButton onClick={() => void stop()} kind="danger">
            ■ Stop
          </ToolbarButton>
        ) : (
          <ToolbarButton onClick={() => void run()} kind="primary" disabled={!runnable}>
            ▶ Run
          </ToolbarButton>
        )}
        <ToolbarButton
          onClick={() => void handleCompile()}
          disabled={!runnable || compiling}
        >
          Compile
        </ToolbarButton>
        {isUrbanApp && (
          <ToolbarButton
            onClick={() => {
              setShowData((v) => !v);
              setShowTriggers(false);
            }}
            kind={showData ? "primary" : undefined}
          >
            Data
          </ToolbarButton>
        )}
        {isUrbanApp && (
          <ToolbarButton
            onClick={() => {
              setShowTriggers((v) => !v);
              setShowData(false);
            }}
            kind={showTriggers ? "primary" : undefined}
          >
            Triggers
          </ToolbarButton>
        )}
        <ToolbarButton onClick={() => setShowConfig(true)}>Configure</ToolbarButton>
        <ToolbarButton onClick={() => void exportProject(name, false)}>Export</ToolbarButton>
      </div>

      {!runnable && (
        <div className="border-b border-warn/30 bg-warn/10 px-4 py-1.5 text-xs text-warn">
          {(() => {
            const mt = detail.missingToolchain;
            const probes = mt?.probes ?? [];
            const looked =
              probes.length > 0
                ? ` Looked for ${probes
                    .map((p) => `\u201C${p}\u201D`)
                    .join(" or ")} on your PATH.`
                : "";
            const who = mt?.displayName ?? (lang === "deno" ? "Deno or Node" : lang);
            return (
              <>
                No {who} toolchain detected — Run and Compile for this project need it.
                {looked}
                {mt?.installHint ? ` ${mt.installHint}` : ""} Authoring and Export
                still work.
                {mt?.installUrl && (
                  <>
                    {" "}
                    <a
                      href={mt.installUrl}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="font-medium underline"
                    >
                      Install →
                    </a>
                  </>
                )}
              </>
            );
          })()}
        </div>
      )}

      <div className="flex min-h-0 flex-1">
        {/* File tree */}
        <FileBrowser
          name={name}
          config={detail.config}
          files={detail.files}
          rootPath={detail.rootPath}
          selected={selected}
          onSelect={setSelected}
          onChanged={reloadFiles}
        />

        {/* Editor + console — or the DB Manager (Data) / Triggers panel when toggled */}
        {showData ? (
          <div className="flex min-w-0 flex-1 flex-col">
            <Suspense
              fallback={<div className="p-8 text-sm text-fg-faint">Loading Data panel…</div>}
            >
              <DataPanel name={name} />
            </Suspense>
          </div>
        ) : showTriggers ? (
          <div className="flex min-w-0 flex-1 flex-col">
            <Suspense
              fallback={<div className="p-8 text-sm text-fg-faint">Loading Triggers panel…</div>}
            >
              <TriggersPanel name={name} />
            </Suspense>
          </div>
        ) : (
          <div className="flex min-w-0 flex-1 flex-col">
            <div className="min-h-0 flex-1 overflow-hidden border-b border-edge">
              {selected ? (
                isAppManifestPath(selected) ? (
                  <AppManifestEditor
                    key={selected}
                    name={name}
                    path={selected}
                    files={detail.files}
                  />
                ) : (
                  <EditorPane
                    key={selected}
                    name={name}
                    path={selected}
                    deployTarget={detail.config.deployTarget}
                  />
                )
              ) : (
                <div className="flex h-full items-center justify-center text-sm text-fg-faint">
                  Select a file to edit
                </div>
              )}
            </div>
            <div
              onMouseDown={startDrag}
              className="h-1.5 shrink-0 cursor-row-resize bg-hover transition-colors hover:bg-accent"
              title="Drag to resize console"
            />
            <RunConsole logs={logs} forwardRef={logRef} onClear={() => setLogs([])} height={consoleHeight} />
          </div>
        )}
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
      ? "bg-ok text-on-accent hover:bg-ok/85"
      : kind === "danger"
        ? "bg-danger text-on-accent hover:bg-danger/85"
        : "border border-edge-strong text-fg hover:bg-hover";
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
  config,
  files,
  rootPath,
  selected,
  onSelect,
  onChanged,
}: {
  name: string;
  config: ProjectConfig;
  files: FileNode[];
  rootPath: string;
  selected: string | null;
  onSelect: (path: string) => void;
  onChanged: () => void;
}) {
  const [newFileOpen, setNewFileOpen] = useState(false);
  const newFolder = async () => {
    const base = prompt("New folder path (project-relative):", "resources/");
    if (!base) return;
    try {
      await createProjectPath({ path: { name }, body: { path: base, dir: true }, throwOnError: true });
      onChanged();
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
    }
  };
  const del = async (path: string) => {
    if (!confirm(`Delete ${path}?`)) return;
    try {
      await deleteProjectPath({ path: { name }, query: { path }, throwOnError: true });
      onChanged();
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
    }
  };

  // Right-click "copy path" menu. rootPath is the project dir on the host; the
  // path separator is inferred from it so absolute paths look native on Windows
  // (backslashes) as well as POSIX hosts.
  const [menu, setMenu] = useState<{ x: number; y: number; node: FileNode } | null>(null);
  const sep = rootPath.includes("\\") && !rootPath.includes("/") ? "\\" : "/";
  const absPathOf = (rel: string) => rootPath + sep + rel.split("/").join(sep);
  const openMenu = (e: React.MouseEvent, node: FileNode) => {
    e.preventDefault();
    setMenu({ x: e.clientX, y: e.clientY, node });
  };

  return (
    <aside className="flex w-64 shrink-0 flex-col border-r border-edge bg-panel">
      <div className="flex items-center justify-between px-3 py-2 text-xs uppercase tracking-wider text-fg-faint">
        <span>Files</span>
        <div className="flex gap-1">
          <button title="New file" onClick={() => setNewFileOpen(true)} className="rounded px-1.5 py-0.5 hover:bg-hover hover:text-fg">
            ＋
          </button>
          <button title="New folder" onClick={() => void newFolder()} className="rounded px-1.5 py-0.5 hover:bg-hover hover:text-fg">
            ⊞
          </button>
        </div>
      </div>
      <div className="min-h-0 flex-1 overflow-auto px-1 pb-2">
        <FileTree nodes={files} depth={0} selected={selected} onSelect={onSelect} onDelete={del} onContextMenu={openMenu} />
      </div>
      {/* On-disk location of the project. Lets users find their files outside
          the IDE; the copy button grabs the absolute path. */}
      <ProjectPathFooter rootPath={rootPath} />
      {menu && (
        <PathContextMenu
          x={menu.x}
          y={menu.y}
          node={menu.node}
          absPath={absPathOf(menu.node.path)}
          onClose={() => setMenu(null)}
        />
      )}
      {newFileOpen && (
        <NewFileModal
          name={name}
          config={config}
          files={files}
          onClose={() => setNewFileOpen(false)}
          onCreated={(path) => {
            onChanged();
            onSelect(path);
            setNewFileOpen(false);
          }}
        />
      )}
    </aside>
  );
}

/// Clipboard helper — resolves to true on success. Falls back to a temporary
/// textarea + execCommand for the rare browser/context without the async
/// Clipboard API (the console is served from localhost, a secure context, so
/// navigator.clipboard is normally available).
async function copyToClipboard(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    try {
      const ta = document.createElement("textarea");
      ta.value = text;
      ta.style.position = "fixed";
      ta.style.opacity = "0";
      document.body.appendChild(ta);
      ta.select();
      const ok = document.execCommand("copy");
      document.body.removeChild(ta);
      return ok;
    } catch {
      return false;
    }
  }
}

/// Footer of the file tree showing the project's absolute directory on the host,
/// with a one-click copy. This is the primary "where is this on disk?" affordance.
function ProjectPathFooter({ rootPath }: { rootPath: string }) {
  const [copied, setCopied] = useState(false);
  const copy = async () => {
    if (await copyToClipboard(rootPath)) {
      setCopied(true);
      setTimeout(() => setCopied(false), 1200);
    }
  };
  return (
    <div className="flex items-center gap-1 border-t border-edge px-2 py-1.5 text-[11px] text-fg-faint">
      <span className="shrink-0 text-fg-faint" aria-hidden>
        📁
      </span>
      <span className="truncate font-mono" title={rootPath} dir="rtl">
        {rootPath}
      </span>
      <span className="flex-1" />
      <button
        onClick={() => void copy()}
        title="Copy the project's path on disk"
        className="shrink-0 rounded px-1 py-0.5 hover:bg-hover hover:text-fg"
      >
        {copied ? "✓" : "⧉"}
      </button>
    </div>
  );
}

/// Right-click menu on a file/dir node: copy its absolute path (host path) or
/// its project-relative path. A full-screen transparent backdrop closes it on
/// any outside click or right-click.
function PathContextMenu({
  x,
  y,
  node,
  absPath,
  onClose,
}: {
  x: number;
  y: number;
  node: FileNode;
  absPath: string;
  onClose: () => void;
}) {
  const act = async (text: string) => {
    await copyToClipboard(text);
    onClose();
  };
  return (
    <div className="fixed inset-0 z-50" onClick={onClose} onContextMenu={(e) => { e.preventDefault(); onClose(); }}>
      <ul
        className="absolute min-w-44 rounded-md border border-edge bg-panel py-1 text-sm shadow-lg"
        style={{ left: x, top: y }}
        onClick={(e) => e.stopPropagation()}
      >
        <li className="truncate px-3 py-1 text-[11px] uppercase tracking-wider text-fg-faint" title={node.path}>
          {node.name}
        </li>
        <li>
          <button className="block w-full px-3 py-1.5 text-left text-fg-muted hover:bg-hover hover:text-fg" onClick={() => void act(absPath)}>
            Copy path
          </button>
        </li>
        <li>
          <button className="block w-full px-3 py-1.5 text-left text-fg-muted hover:bg-hover hover:text-fg" onClick={() => void act(node.path)}>
            Copy relative path
          </button>
        </li>
      </ul>
    </div>
  );
}

// Fixed file kinds every project understands. The `source` kind is derived from
// the project language at render time (extension + default dir), so it isn't
// listed here.
const NEW_FILE_KINDS = [
  {
    id: "model" as const,
    label: "Model",
    hint: "BPMN process diagram",
    dir: "resources/processes",
    ext: ".bpmn",
  },
  {
    id: "decision" as const,
    label: "Decision",
    hint: "DMN decision table",
    dir: "resources/decisions",
    ext: ".dmn",
  },
  {
    id: "form" as const,
    label: "Form",
    hint: "User task form",
    dir: "resources/forms",
    ext: ".form",
  },
];

type NewFileKindId = "model" | "decision" | "form" | "source";

const extnameOf = (p: string): string => {
  const base = p.split("/").pop() ?? "";
  const dot = base.lastIndexOf(".");
  return dot > 0 ? base.slice(dot) : "";
};

const dirnameOf = (p: string): string => {
  const slash = p.lastIndexOf("/");
  return slash >= 0 ? p.slice(0, slash) : "";
};

const hasTopLevelDir = (files: FileNode[], dir: string): boolean =>
  files.some((f) => f.kind === "dir" && f.name === dir);

/// Language-aware "New file" dialog. Offers the fixed BPMN/DMN/Form kinds plus a
/// project-language "Source" kind, each pre-filling the conventional directory
/// (`resources/processes|decisions|forms`) and extension so files land where the
/// Urban manifest and editors expect them. The location stays editable for the
/// escape-hatch cases.
function NewFileModal({
  name,
  config,
  files,
  onClose,
  onCreated,
}: {
  name: string;
  config: ProjectConfig;
  files: FileNode[];
  onClose: () => void;
  onCreated: (path: string) => void;
}) {
  // Derive the "Source" kind from the project entrypoint: its extension mirrors
  // the language (main.ts -> .ts, main.rs -> .rs) and its directory is the most
  // natural home for new source (the entrypoint's own dir, else a top-level
  // `lib`/`src`, else the project root).
  const sourceExt = extnameOf(config.main) || ".txt";
  const sourceDir = useMemo(() => {
    const d = dirnameOf(config.main);
    if (d) return d;
    for (const cand of ["lib", "src"]) {
      if (hasTopLevelDir(files, cand)) return cand;
    }
    return "";
  }, [config.main, files]);

  const kindOf = useCallback(
    (id: NewFileKindId) =>
      id === "source"
        ? { id, label: "Source", hint: `${config.lang} source (${sourceExt})`, dir: sourceDir, ext: sourceExt }
        : NEW_FILE_KINDS.find((k) => k.id === id)!,
    [config.lang, sourceDir, sourceExt],
  );

  const kinds = useMemo(
    () => [...NEW_FILE_KINDS, kindOf("source")],
    [kindOf],
  );

  const [kindId, setKindId] = useState<NewFileKindId>("model");
  const [baseName, setBaseName] = useState("new");
  const [dir, setDir] = useState(NEW_FILE_KINDS[0].dir);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const active = kindOf(kindId);

  const selectKind = (id: NewFileKindId) => {
    setKindId(id);
    setDir(kindOf(id).dir);
    setError(null);
  };

  const finalPath = useMemo(() => {
    let bn = baseName.trim().replace(/^\/+|\/+$/g, "");
    if (bn.endsWith(active.ext)) bn = bn.slice(0, -active.ext.length);
    const cleanDir = dir.trim().replace(/^\/+|\/+$/g, "");
    const file = `${bn}${active.ext}`;
    return cleanDir ? `${cleanDir}/${file}` : file;
  }, [baseName, dir, active.ext]);

  const create = async () => {
    if (!baseName.trim()) {
      setError("Enter a file name.");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      await createProjectPath({
        path: { name },
        body: { path: finalPath, dir: false },
        throwOnError: true,
      });
      onCreated(finalPath);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  };

  return (
    <Modal title="New file" onClose={onClose}>
      <div className="space-y-4">
        <div>
          <label className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-fg-faint">
            Type
          </label>
          <div className="grid grid-cols-2 gap-2">
            {kinds.map((k) => (
              <label
                key={k.id}
                className={`flex cursor-pointer items-start gap-2 rounded-md border p-2.5 text-sm transition-colors ${
                  kindId === k.id
                    ? "border-accent bg-accent/10"
                    : "border-edge hover:bg-hover"
                }`}
              >
                <input
                  type="radio"
                  name="new-file-kind"
                  checked={kindId === k.id}
                  onChange={() => selectKind(k.id)}
                  className="mt-0.5 accent-accent"
                />
                <span className="min-w-0">
                  <span className="block font-medium text-fg">{k.label}</span>
                  <span className="block truncate text-xs text-fg-faint">{k.hint}</span>
                </span>
              </label>
            ))}
          </div>
        </div>
        <div className="grid grid-cols-[1fr_auto] items-end gap-2">
          <label className="block">
            <span className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-fg-faint">
              Name
            </span>
            <input
              autoFocus
              value={baseName}
              onChange={(e) => setBaseName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") void create();
              }}
              className={`${inputClass} w-full`}
              placeholder="new"
            />
          </label>
          <div className="rounded-md border border-edge bg-inset px-3 py-2 font-mono text-sm text-fg-muted">
            {active.ext}
          </div>
        </div>
        <label className="block">
          <span className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-fg-faint">
            Location
          </span>
          <input
            value={dir}
            onChange={(e) => setDir(e.target.value)}
            className={`${inputClass} w-full`}
            placeholder="resources/processes"
          />
        </label>
        <p className="text-xs text-fg-muted">
          Creates <code className="text-fg">{finalPath}</code>
        </p>
        {error && <p className="text-sm text-danger">{error}</p>}
        <div className="flex justify-end gap-2">
          <Button variant="secondary" onClick={onClose}>
            Cancel
          </Button>
          <Button
            variant="primary"
            onClick={() => void create()}
            disabled={busy || !baseName.trim()}
          >
            {busy ? "Creating…" : "Create"}
          </Button>
        </div>
      </div>
    </Modal>
  );
}

function FileTree({
  nodes,
  depth,
  selected,
  onSelect,
  onDelete,
  onContextMenu,
}: {
  nodes: FileNode[];
  depth: number;
  selected: string | null;
  onSelect: (p: string) => void;
  onDelete: (p: string) => void;
  onContextMenu: (e: React.MouseEvent, node: FileNode) => void;
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
          onContextMenu={onContextMenu}
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
  onContextMenu,
}: {
  node: FileNode;
  depth: number;
  selected: string | null;
  onSelect: (p: string) => void;
  onDelete: (p: string) => void;
  onContextMenu: (e: React.MouseEvent, node: FileNode) => void;
}) {
  const [open, setOpen] = useState(depth < 2);
  const pad = { paddingLeft: `${depth * 12 + 8}px` };
  if (node.kind === "dir") {
    return (
      <li>
        <div
          style={pad}
          onClick={() => setOpen((v) => !v)}
          onContextMenu={(e) => onContextMenu(e, node)}
          className="group flex cursor-pointer items-center gap-1 rounded py-1 pr-2 text-sm text-fg-muted hover:bg-hover"
        >
          <span className="text-fg-faint">{open ? "▾" : "▸"}</span>
          <span className="truncate">{node.name}</span>
        </div>
        {open && node.children && (
          <FileTree nodes={node.children} depth={depth + 1} selected={selected} onSelect={onSelect} onDelete={onDelete} onContextMenu={onContextMenu} />
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
          active ? "bg-accent/10 font-medium text-accent-strong" : "text-fg-muted hover:bg-hover hover:text-fg"
        }`}
        onClick={() => onSelect(node.path)}
        onContextMenu={(e) => onContextMenu(e, node)}
      >
        <span className="opacity-0">▸</span>
        <span className="truncate">{node.name}</span>
        <span className="flex-1" />
        <button
          onClick={(e) => {
            e.stopPropagation();
            onDelete(node.path);
          }}
          className="hidden rounded px-1 text-xs text-fg-faint hover:text-danger group-hover:block"
        >
          ✕
        </button>
      </div>
    </li>
  );
}

// --- Editor dispatch -------------------------------------------------------

function EditorPane({
  name,
  path,
  deployTarget,
}: {
  name: string;
  path: string;
  deployTarget: string;
}) {
  const [content, setContent] = useState<string | null>(null);
  const [meta, setMeta] = useState<ProjectFile | null>(null);
  const [dirty, setDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const bpmnRef = useRef<BpmnModelerHandle>(null);
  const dmnRef = useRef<DmnModelerHandle>(null);
  const formRef = useRef<FormEditorHandle>(null);
  const [testXml, setTestXml] = useState<string | null>(null);
  // Markdown files open in a rendered Preview tab; the user can switch to Edit.
  const [mdView, setMdView] = useState<"preview" | "edit">("preview");
  // BPMN files open in the graphical modeler; the user can switch to a raw XML
  // editor to inspect/tweak the underlying document. The modeler stays mounted
  // when the XML tab is active so canvas state is preserved across toggles.
  const [bpmnView, setBpmnView] = useState<"visual" | "xml">("visual");
  const [bpmnXml, setBpmnXml] = useState<string>("");
  // Tracks whether `bpmnXml` was edited in the XML tab since it was pulled from
  // the modeler. On switch back to Visual we import it so both surfaces stay in
  // sync; on Save from the XML tab we persist `bpmnXml` directly.
  const bpmnXmlDirtyRef = useRef(false);
  // Form files open in the graphical form editor; the user can switch to a raw
  // JSON editor to inspect/tweak the underlying schema. The editor stays mounted
  // when the JSON tab is active so form state is preserved across toggles.
  const [formView, setFormView] = useState<"visual" | "json" | "preview">("visual");
  const [formJson, setFormJson] = useState<string>("");
  // Mirrors bpmnXmlDirtyRef for the form JSON tab.
  const formJsonDirtyRef = useRef(false);
  // Track what we most recently deployed to <deployTarget> so we can enable
  // Start Instance only when the saved XML matches deployment. Primed once
  // on load by pulling the deployed XML for the file's primary process id;
  // updated in-place after a successful Deploy.
  const [lastDeployedXml, setLastDeployedXml] = useState<string | null>(null);
  const [deploying, setDeploying] = useState(false);
  const [startModalOpen, setStartModalOpen] = useState(false);

  const ext = path.split(".").pop()?.toLowerCase() ?? "";
  const kind: "bpmn" | "dmn" | "form" | "md" | "code" =
    ext === "bpmn"
      ? "bpmn"
      : ext === "dmn"
        ? "dmn"
        : ext === "form"
          ? "form"
          : ext === "md" || ext === "markdown"
            ? "md"
            : "code";

  // Parse process ids client-side. Multi-process files are rare and use the
  // first as the primary (matches the server's deploy_status_of contract).
  const processIds = useMemo<string[]>(() => {
    if (kind !== "bpmn" || content == null) return [];
    return Array.from(
      content.matchAll(/<(?:bpmn2?:)?process\b[^>]*\bid=["']([^"']+)["']/g),
    ).map((m) => m[1]);
  }, [kind, content]);
  const primaryProcessId = processIds[0] ?? null;
  const deployedInSync =
    lastDeployedXml != null && lastDeployedXml === content && !dirty;

  // The App manifest supplies the domain type bound to each decision (ADR 0029
  // §5) and each process (ADR 0030), which scopes the DMN input-expression and
  // BPMN component-input FEEL autocomplete. Load it (root nano.app.json) while a
  // decision or process model is open; reload on file switch so recent binding
  // edits are reflected. Absent/invalid manifest → no bound variables.
  const [manifestText, setManifestText] = useState<string | null>(null);
  useEffect(() => {
    if (kind !== "dmn" && kind !== "bpmn" && kind !== "form") {
      setManifestText(null);
      return;
    }
    let alive = true;
    projectFileEx(name, "nano.app.json")
      .then((f) => alive && setManifestText(f.binary ? null : f.text))
      .catch(() => alive && setManifestText(null));
    return () => {
      alive = false;
    };
  }, [name, kind, path]);
  const manifest = useMemo<unknown>(() => {
    if (!manifestText) return undefined;
    try {
      return JSON.parse(manifestText);
    } catch {
      return undefined;
    }
  }, [manifestText]);
  // Latest manifest text, read by the domain-type binding's `set` so a service-
  // task type edit patches the current `nano.app.json` (avoids a stale closure).
  const manifestTextRef = useRef(manifestText);
  manifestTextRef.current = manifestText;
  // The components (element templates) installed for this project drive the BPMN
  // palette + template chooser (ADR 0033 increment 2). Load them from the
  // project's component dirs while a BPMN model is open; reload when the file
  // tree changes so a newly-added component file appears. Absent → empty palette.
  // The components (element templates) available for this BPMN model: the
  // project's own (`components/` + `.camunda/element-templates/`, increment 2)
  // layered over the set contributed by installed packs (ADR 0033 §4, increment
  // 6). Load both when a BPMN model is open; reload on file switch. Project
  // components win on an id collision. Absent → empty palette.
  const [components, setComponents] = useState<ElementTemplate[]>([]);
  useEffect(() => {
    if (kind !== "bpmn") {
      setComponents([]);
      return;
    }
    let alive = true;
    Promise.all([
      loadPackComponents(),
      listProjectFiles({ path: { name }, throwOnError: true }).then((res) =>
        loadProjectComponents(name, res.data.files),
      ),
    ])
      .then(([pack, project]) => alive && setComponents(combineComponents(pack, project)))
      .catch(() => alive && setComponents([]));
    return () => {
      alive = false;
    };
  }, [name, kind, path]);
  const dmnGetVariables = useCallback(
    (decisionId: string | undefined) => decisionFeelVariables(manifest, decisionId),
    [manifest],
  );
  const bpmnGetVariables = useCallback(
    ({ taskOutputs }: { taskOutputs: ComponentOutput[] }) => [
      ...processFeelVariables(manifest, primaryProcessId ?? undefined),
      ...componentOutputFeelVariables(manifest, taskOutputs),
    ],
    [manifest, primaryProcessId],
  );
  // The declared registry type ids (manifest `types`), the options a Data
  // envelope picker offers (ADR 0033 §6).
  const domainTypeIds = useMemo<string[]>(() => {
    const types = (manifest as { types?: unknown } | undefined)?.types;
    if (!types || typeof types !== "object") return [];
    return Object.keys(types as Record<string, unknown>);
  }, [manifest]);
  // Creates a new transient domain type in the manifest `types` registry (the
  // "Create new envelope…" affordance, ADR 0033 §6) and persists `nano.app.json`,
  // so a maker can declare + pick an envelope in one gesture. Seeds a stub field
  // (schema requires ≥1) the maker fleshes out in the types editor. Throws on a
  // failed save so the modeler aborts selecting a type that was never persisted.
  const createDomainType = useCallback(
    async (id: string): Promise<void> => {
      const prev = manifestTextRef.current;
      if (prev == null) return;
      let obj: Record<string, unknown>;
      try {
        const parsed = JSON.parse(prev);
        if (!parsed || typeof parsed !== "object") return;
        obj = parsed as Record<string, unknown>;
      } catch {
        return;
      }
      const types =
        obj.types && typeof obj.types === "object"
          ? (obj.types as Record<string, unknown>)
          : ((obj.types = {}) as Record<string, unknown>);
      if (!types[id]) types[id] = { fields: { value: { type: "string" } } };
      const next = `${JSON.stringify(obj, null, 2)}\n`;
      setManifestText(next);
      await saveProjectFile({
        path: { name },
        query: { path: "nano.app.json" },
        body: next,
        throwOnError: true,
      });
    },
    [name],
  );
  // Projects a service task's chosen envelope onto its `workers[]` entry (creating
  // it if absent, clearing on ""), so the reifier keeps `defineWorker` typed while
  // the model stays the source of truth (ADR 0033 §6). Optimistic — updates the
  // in-memory manifest immediately so the modeler's FEEL scopes + the panel reflect
  // the change before the write lands.
  const setWorkerType = useCallback(
    (taskType: string, field: "inputType" | "outputType", value: string): void => {
      const prev = manifestTextRef.current;
      if (prev == null) return;
      let obj: Record<string, unknown>;
      try {
        const parsed = JSON.parse(prev);
        if (!parsed || typeof parsed !== "object") return;
        obj = parsed as Record<string, unknown>;
      } catch {
        return;
      }
      const workers = Array.isArray(obj.workers)
        ? (obj.workers as Record<string, unknown>[])
        : ((obj.workers = []) as Record<string, unknown>[]);
      let entry = workers.find((w) => w && w.taskType === taskType);
      if (!entry) {
        entry = { taskType };
        workers.push(entry);
      }
      if (value) entry[field] = value;
      else delete entry[field];
      const next = `${JSON.stringify(obj, null, 2)}\n`;
      setManifestText(next);
      void saveProjectFile({
        path: { name },
        query: { path: "nano.app.json" },
        body: next,
        throwOnError: true,
      }).catch(() => {
        // Best-effort persist; the optimistic in-memory manifest still reflects
        // the edit for this session.
      });
    },
    [name],
  );
  const bpmnDomainTypeBinding = useMemo<DomainTypeBinding>(
    () => ({
      enabled: manifest != null,
      typeIds: domainTypeIds,
      set: setWorkerType,
      createType: createDomainType,
    }),
    [manifest, domainTypeIds, setWorkerType, createDomainType],
  );
  // Datasource aliases declared in the App manifest (`data.sources`), for the
  // form editor's "Data source" binding inspector (ADR 0024 §5). Recomputed when
  // the manifest reloads; the FormEditor reads it lazily via getDataSources.
  const dataSourceNames = useMemo<string[]>(() => {
    const sources = (manifest as { data?: { sources?: unknown } } | undefined)?.data?.sources;
    if (!sources || typeof sources !== "object") return [];
    return Object.keys(sources as Record<string, unknown>);
  }, [manifest]);
  const formGetDataSources = useCallback(() => dataSourceNames, [dataSourceNames]);
  // The manifest's default datasource (`data.default`), used to resolve a
  // `data.query(sql)` call's default-source form in the form preview (ADR 0024 §5).
  const defaultDataSource = useMemo<string | undefined>(() => {
    const d = (manifest as { data?: { default?: unknown } } | undefined)?.data?.default;
    return typeof d === "string" ? d : undefined;
  }, [manifest]);

  useEffect(() => {
    let alive = true;
    setContent(null);
    setMeta(null);
    setDirty(false);
    setLoadError(null);
    setMdView("preview");
    setBpmnView("visual");
    setBpmnXml("");
    bpmnXmlDirtyRef.current = false;
    setFormView("visual");
    setFormJson("");
    formJsonDirtyRef.current = false;
    projectFileEx(name, path)
      .then((f) => {
        if (!alive) return;
        setMeta(f);
        setContent(f.binary ? null : f.text);
      })
      .catch((e) => alive && setLoadError(e instanceof Error ? e.message : String(e)));
    return () => {
      alive = false;
    };
  }, [name, path]);

  // Load the fetched document into the graphical editor once mounted. A newly
  // created (empty) model file is seeded with a valid blank document via the
  // editor's createBlank() — importing empty/`{}` would fail to load — and
  // marked dirty so a Save persists the seeded document.
  useEffect(() => {
    if (content == null) return;
    const isEmpty = content.trim() === "";
    const seeded = () => setDirty(true);
    if (kind === "bpmn" && bpmnRef.current) {
      const ed = bpmnRef.current;
      void (isEmpty ? ed.createBlank().then(seeded) : ed.importXml(content)).catch(
        () => void 0,
      );
    } else if (kind === "dmn" && dmnRef.current) {
      const ed = dmnRef.current;
      void (isEmpty ? ed.createBlank().then(seeded) : ed.importXml(content)).catch(
        () => void 0,
      );
    } else if (kind === "form" && formRef.current) {
      const ed = formRef.current;
      void (isEmpty ? ed.createBlank().then(seeded) : ed.importSchema(content)).catch(
        () => void 0,
      );
    }
    // Only when the document first arrives for this path.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [content, kind]);

  const save = useCallback(async () => {
    setSaving(true);
    try {
      let body = content ?? "";
      if (kind === "bpmn" && bpmnView === "xml") body = bpmnXml;
      else if (kind === "bpmn" && bpmnRef.current) body = await bpmnRef.current.getXml();
      else if (kind === "dmn" && dmnRef.current) body = await dmnRef.current.getXml();
      else if (kind === "form" && (formView === "json" || formView === "preview")) body = formJson;
      else if (kind === "form" && formRef.current) body = await formRef.current.getSchema();
      await saveProjectFile({ path: { name }, query: { path }, body, throwOnError: true });
      setContent(body);
      setDirty(false);
      if (kind === "bpmn") {
        // Persisted body is now authoritative in both surfaces.
        setBpmnXml(body);
        bpmnXmlDirtyRef.current = false;
        // If the save came from the XML tab, the canvas still holds the
        // pre-save document. Push the saved XML into the modeler so a
        // subsequent switch to Visual shows the up-to-date diagram
        // (the switch guards on bpmnXmlDirtyRef and would otherwise skip
        // importXml, leaving the canvas stale).
        if (bpmnView === "xml" && bpmnRef.current) {
          try {
            await bpmnRef.current.importXml(body);
          } catch {
            // Invalid XML shouldn't normally reach here (Save with bad
            // XML is on the user), but don't turn a successful persist
            // into a hard error.
          }
        }
      }
      if (kind === "form") {
        // Persisted body is now authoritative in both surfaces (mirrors bpmn).
        setFormJson(body);
        formJsonDirtyRef.current = false;
        if ((formView === "json" || formView === "preview") && formRef.current) {
          try {
            await formRef.current.importSchema(body);
          } catch {
            // Invalid JSON shouldn't normally reach here (Save with bad
            // JSON is on the user), but don't turn a successful persist
            // into a hard error.
          }
        }
      }
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }, [content, kind, name, path, bpmnView, bpmnXml, formView, formJson]);

  // Prime lastDeployedXml on load — probes <deployTarget> for the currently
  // deployed BPMN of primaryProcessId. Silent on failure (network down, no
  // deployment yet) — the Start Instance button just stays disabled until
  // the user clicks Deploy.
  useEffect(() => {
    if (kind !== "bpmn" || !primaryProcessId) {
      setLastDeployedXml(null);
      return;
    }
    let alive = true;
    debug("modeler", "info", `priming deployed XML for '${primaryProcessId}'`, {
      file: path,
      deployTarget,
    });
    void fetchDeployedXmlByProcessId(primaryProcessId, deployTarget).then(
      (xml) => {
        if (!alive) return;
        setLastDeployedXml(xml);
        if (xml != null) {
          debug(
            "modeler",
            "ok",
            `primed lastDeployedXml (${xml.length} bytes); Start Instance ${xml === content ? "enabled" : "stays disabled — saved XML differs from deployed"}`,
          );
        }
      },
    );
    return () => {
      alive = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [kind, primaryProcessId, deployTarget]);

  const deploy = useCallback(async () => {
    if (content == null) return;
    setDeploying(true);
    const modelName = path.split("/").pop()?.replace(/\.bpmn$/i, "") || name;
    debug("modeler", "info", `deploy '${modelName}' clicked`, {
      file: path,
      bytes: content.length,
      primaryProcessId,
    });
    try {
      await deployXml(modelName, content, deployTarget);
      setLastDeployedXml(content);
      debug(
        "modeler",
        "ok",
        `deployed; Start Instance is now enabled for '${primaryProcessId ?? "(unknown)"}'`,
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      debug("modeler", "error", `deploy failed: ${msg}`);
      alert(`Deploy failed: ${msg}\n\nSee the Debug tab for the request URL and diagnostic hints.`);
    } finally {
      setDeploying(false);
    }
  }, [content, deployTarget, name, path, primaryProcessId]);

  // Switches the BPMN editor between the graphical canvas and the raw XML tab.
  // Visual → XML: pull the current serialized document from the modeler.
  // XML → Visual: if the XML was edited in the textarea, import it back so the
  // canvas reflects the edits (any import error surfaces via alert so the user
  // can fix the XML rather than silently losing changes).
  const switchBpmnView = useCallback(
    async (next: "visual" | "xml") => {
      if (next === bpmnView) return;
      if (next === "xml") {
        const xml = (await bpmnRef.current?.getXml()) ?? "";
        setBpmnXml(xml);
        bpmnXmlDirtyRef.current = false;
        setBpmnView("xml");
      } else {
        if (bpmnXmlDirtyRef.current && bpmnRef.current) {
          try {
            await bpmnRef.current.importXml(bpmnXml);
            bpmnXmlDirtyRef.current = false;
          } catch (e) {
            alert(`Could not import XML: ${e instanceof Error ? e.message : String(e)}`);
            return;
          }
        }
        setBpmnView("visual");
      }
    },
    [bpmnView, bpmnXml],
  );

  // Switches the form editor between the graphical canvas, the raw JSON tab, and
  // the live Preview (form-js viewer with datasource-resolved options, ADR 0024
  // §5). Leaving the canvas snapshots its schema into `formJson` so JSON and
  // Preview reflect unsaved canvas edits; returning to the canvas re-imports any
  // JSON edits (errors surface via alert).
  const switchFormView = useCallback(
    async (next: "visual" | "json" | "preview") => {
      if (next === formView) return;
      if (formView === "visual") {
        const json = (await formRef.current?.getSchema()) ?? "";
        setFormJson(json);
        formJsonDirtyRef.current = false;
      }
      if (next === "visual") {
        if (formJsonDirtyRef.current && formRef.current) {
          try {
            await formRef.current.importSchema(formJson);
            formJsonDirtyRef.current = false;
          } catch (e) {
            alert(`Could not import form JSON: ${e instanceof Error ? e.message : String(e)}`);
            return;
          }
        }
      }
      setFormView(next);
    },
    [formView, formJson],
  );

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
    return <div className="p-6 text-sm text-danger">{loadError}</div>;
  }
  if (meta == null) {
    return <div className="p-6 text-sm text-fg-faint">Loading {path}…</div>;
  }
  if (meta.binary) {
    const mb = (meta.size / (1024 * 1024)).toFixed(2);
    return (
      <div className="flex h-full flex-col">
        <div className="flex items-center gap-3 border-b border-edge bg-panel px-3 py-1.5">
          <span className="truncate font-mono text-xs text-fg-muted">{path}</span>
        </div>
        <div className="flex min-h-0 flex-1 items-center justify-center p-6">
          <p className="max-w-2xl break-words text-center font-mono text-sm leading-relaxed text-fg-muted">
            Binary file. Path on disk: {meta.absPath}. Size: {meta.size} ({mb}MB)
          </p>
        </div>
      </div>
    );
  }
  if (content == null) {
    return <div className="p-6 text-sm text-fg-faint">Loading {path}…</div>;
  }

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center gap-3 border-b border-edge bg-panel px-3 py-1.5">
        <span className="truncate font-mono text-xs text-fg-muted">{path}</span>
        {dirty && <span className="text-[10px] text-warn">● unsaved</span>}
        <div className="flex-1" />
        {kind === "md" && (
          <div className="flex overflow-hidden rounded-md border border-edge-strong">
            {(["preview", "edit"] as const).map((mode) => (
              <button
                key={mode}
                onClick={() => setMdView(mode)}
                className={`px-3 py-1 text-xs font-medium capitalize transition-colors ${
                  mdView === mode
                    ? "bg-accent text-on-accent"
                    : "text-fg-muted hover:bg-hover"
                }`}
              >
                {mode}
              </button>
            ))}
          </div>
        )}
        {kind === "bpmn" && (
          <div className="flex overflow-hidden rounded-md border border-edge-strong">
            {(["visual", "xml"] as const).map((mode) => (
              <button
                key={mode}
                onClick={() => void switchBpmnView(mode)}
                className={`px-3 py-1 text-xs font-medium uppercase transition-colors ${
                  bpmnView === mode
                    ? "bg-accent text-on-accent"
                    : "text-fg-muted hover:bg-hover"
                }`}
              >
                {mode}
              </button>
            ))}
          </div>
        )}
        {kind === "form" && (
          <div className="flex overflow-hidden rounded-md border border-edge-strong">
            {(["visual", "json", "preview"] as const).map((mode) => (
              <button
                key={mode}
                onClick={() => void switchFormView(mode)}
                className={`px-3 py-1 text-xs font-medium uppercase transition-colors ${
                  formView === mode
                    ? "bg-accent text-on-accent"
                    : "text-fg-muted hover:bg-hover"
                }`}
              >
                {mode}
              </button>
            ))}
          </div>
        )}
        {kind === "bpmn" && (
          <>
            <button
              onClick={() => void deploy()}
              disabled={deploying || dirty || !primaryProcessId}
              title={
                !primaryProcessId
                  ? "The BPMN file has no <process id=...>"
                  : dirty
                    ? "Save first"
                    : `Deploy to ${deployTarget}`
              }
              className="rounded-md border border-edge-strong px-3 py-1 text-xs font-medium text-fg transition-colors hover:border-accent hover:text-accent-strong disabled:opacity-40"
            >
              {deploying
                ? "Deploying…"
                : deployedInSync
                  ? "Deploy ✓"
                  : "Deploy"}
            </button>
            <button
              onClick={() => setStartModalOpen(true)}
              disabled={!deployedInSync || !primaryProcessId}
              title={
                !primaryProcessId
                  ? "The BPMN file has no <process id=...>"
                  : !deployedInSync
                    ? "Deploy the current model first"
                    : `Start an instance of ${primaryProcessId}`
              }
              className="rounded-md border border-edge-strong px-3 py-1 text-xs font-medium text-fg transition-colors hover:border-accent hover:text-accent-strong disabled:opacity-40"
            >
              Start instance
            </button>
          </>
        )}
        {kind === "bpmn" && (
          <button
            onClick={async () => {
              // Pull from whichever surface currently holds the latest doc.
              const xml =
                bpmnView === "xml" ? bpmnXml : ((await bpmnRef.current?.getXml()) ?? "");
              if (xml) setTestXml(xml);
            }}
            className="rounded-md border border-edge-strong px-3 py-1 text-xs font-medium text-fg transition-colors hover:border-ok hover:text-ok"
          >
            Test
          </button>
        )}
        <button
          onClick={() => void save()}
          disabled={saving || !dirty}
          className="rounded-md bg-accent px-3 py-1 text-xs font-medium text-on-accent transition-colors hover:bg-accent-strong disabled:opacity-40"
        >
          {saving ? "Saving…" : "Save"}
        </button>
      </div>
      <div className="min-h-0 flex-1">
        {kind === "bpmn" && (
          <div className="relative h-full">
            {/*
              Keep the modeler mounted while the XML tab is active so canvas
              state (selection, viewport, in-flight edits) survives toggling.
              The XML editor is layered above via absolute positioning.
            */}
            <div className={bpmnView === "visual" ? "h-full" : "h-full invisible"}>
              <BpmnModeler ref={bpmnRef} onChange={() => setDirty(true)} getVariables={bpmnGetVariables} components={components} domainTypeBinding={bpmnDomainTypeBinding} />
            </div>
            {bpmnView === "xml" && (
              <div className="absolute inset-0 bg-app">
                <CodeEditor
                  value={bpmnXml}
                  language="xml"
                  path={`file:///${name}/${path}`}
                  onChange={(v) => {
                    setBpmnXml(v);
                    bpmnXmlDirtyRef.current = true;
                    setDirty(true);
                  }}
                  onSave={() => void save()}
                />
              </div>
            )}
            {testXml && (
              <div className="absolute inset-0 z-10 bg-app">
                <Suspense
                  fallback={
                    <div className="flex h-full items-center justify-center text-sm text-fg-faint">
                      Loading the in-browser engine…
                    </div>
                  }
                >
                  <TestRunPanel xml={testXml} onClose={() => setTestXml(null)} />
                </Suspense>
              </div>
            )}
            {startModalOpen && primaryProcessId && (
              <StartInstanceModal
                processId={primaryProcessId}
                deployTarget={deployTarget}
                onClose={() => setStartModalOpen(false)}
              />
            )}
          </div>
        )}
        {kind === "dmn" && (
          <DmnModeler ref={dmnRef} onChange={() => setDirty(true)} getVariables={dmnGetVariables} />
        )}
        {kind === "form" && (
          <div className="relative h-full">
            {/*
              Keep the form editor mounted while the JSON tab is active so schema
              state survives toggling. The JSON editor is layered above via
              absolute positioning (mirrors the BPMN visual/xml split).
            */}
            <div className={formView === "visual" ? "h-full" : "h-full invisible"}>
              <FormEditor
                ref={formRef}
                onChange={() => setDirty(true)}
                getDataSources={formGetDataSources}
              />
            </div>
            {formView === "json" && (
              <div className="absolute inset-0 bg-app">
                <CodeEditor
                  value={formJson}
                  language="json"
                  path={`file:///${name}/${path}`}
                  onChange={(v) => {
                    setFormJson(v);
                    formJsonDirtyRef.current = true;
                    setDirty(true);
                  }}
                  onSave={() => void save()}
                />
              </div>
            )}
            {formView === "preview" && (
              <div className="absolute inset-0 bg-app">
                <FormPreview schema={formJson} name={name} defaultSource={defaultDataSource} />
              </div>
            )}
          </div>
        )}
        {kind === "md" &&
          (mdView === "preview" ? (
            <MarkdownPreview source={content} />
          ) : (
            <CodeEditor
              value={content}
              language="markdown"
              path={`file:///${name}/${path}`}
              onChange={(v) => {
                setContent(v);
                setDirty(true);
              }}
              onSave={() => void save()}
            />
          ))}
        {kind === "code" && (
          <CodeEditor
            value={content}
            language={languageForFile(path)}
            path={`file:///${name}/${path}`}
            sdkProject={name}
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
  height,
}: {
  logs: ProjectLogLine[];
  forwardRef: React.RefObject<HTMLDivElement>;
  onClear: () => void;
  height: number;
}) {
  const [tab, setTab] = useState<"output" | "debug">("output");
  const [debugLog, setDebugLog] = useState<DebugEntry[]>(() => debugSnapshot());
  const debugRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const unsub = subscribeDebug((entry) => {
      setDebugLog((prev) => {
        const next = prev.length >= 500 ? prev.slice(-499) : prev.slice();
        next.push(entry);
        return next;
      });
    });
    return unsub;
  }, []);

  useEffect(() => {
    if (tab === "debug") debugRef.current?.scrollTo({ top: 1e9 });
  }, [debugLog, tab]);

  const tabCls = (active: boolean) =>
    `border-b-2 px-3 py-1 text-xs uppercase tracking-wider transition-colors ${
      active
        ? "border-accent text-fg"
        : "border-transparent text-fg-faint hover:text-fg-muted"
    }`;

  return (
    <div className="flex shrink-0 flex-col bg-inset" style={{ height }}>
      <div className="flex items-center justify-between border-b border-edge pr-3">
        <div className="flex">
          <button className={tabCls(tab === "output")} onClick={() => setTab("output")}>
            Output
          </button>
          <button className={tabCls(tab === "debug")} onClick={() => setTab("debug")}>
            Debug{debugLog.length ? ` · ${debugLog.length}` : ""}
          </button>
        </div>
        <button
          onClick={() => {
            if (tab === "output") onClear();
            else {
              clearDebug();
              setDebugLog([]);
            }
          }}
          className="rounded px-1.5 py-0.5 text-xs uppercase tracking-wider text-fg-faint hover:bg-hover hover:text-fg-muted"
        >
          Clear
        </button>
      </div>
      {tab === "output" ? (
        <div
          ref={forwardRef}
          className="min-h-0 flex-1 overflow-auto px-3 py-2 font-mono text-xs leading-relaxed"
        >
          {logs.length === 0 ? (
            <div className="text-fg-faint">
              No output yet. Run the application to see logs.
            </div>
          ) : (
            logs.map((l, i) => (
              <div
                key={i}
                className={
                  l.stream === "err"
                    ? "whitespace-pre-wrap text-danger"
                    : l.stream === "sys"
                      ? "whitespace-pre-wrap text-info"
                      : "whitespace-pre-wrap text-fg-muted"
                }
              >
                {l.text}
              </div>
            ))
          )}
        </div>
      ) : (
        <div
          ref={debugRef}
          className="min-h-0 flex-1 overflow-auto px-3 py-2 font-mono text-xs leading-relaxed"
        >
          {debugLog.length === 0 ? (
            <div className="text-fg-faint">
              No debug traces yet. Deploy, Start Instance and probe requests
              log their URL, response, and timing here.
            </div>
          ) : (
            debugLog.map((e) => <DebugRow key={e.id} entry={e} />)
          )}
        </div>
      )}
    </div>
  );
}

function DebugRow({ entry }: { entry: DebugEntry }) {
  const [open, setOpen] = useState(false);
  const hasDetail = entry.detail && Object.keys(entry.detail).length > 0;
  const color =
    entry.level === "error"
      ? "text-danger"
      : entry.level === "warn"
        ? "text-warn"
        : entry.level === "ok"
          ? "text-ok"
          : "text-fg-muted";
  const time = new Date(entry.ts).toISOString().slice(11, 23);
  return (
    <div className="whitespace-pre-wrap">
      <button
        onClick={() => hasDetail && setOpen((o) => !o)}
        className={`w-full text-left ${color} ${hasDetail ? "cursor-pointer hover:bg-hover" : "cursor-default"}`}
      >
        <span className="text-fg-faint">{time}</span>{" "}
        <span className="text-fg-faint">[{entry.scope}]</span>{" "}
        {hasDetail ? (open ? "▾ " : "▸ ") : "  "}
        {entry.message}
      </button>
      {open && hasDetail && (
        <pre className="ml-8 border-l-2 border-edge px-2 text-fg-muted">
{JSON.stringify(entry.detail, null, 2)}
        </pre>
      )}
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
  // Env editor state: array of {key,value} rows, kept ordered so that adding
  // a fresh empty row doesn't reshuffle the user's typing. We serialise back
  // to an object on save, dropping rows with an empty key.
  const [envRows, setEnvRows] = useState<Array<{ key: string; value: string }>>(
    () =>
      Object.entries(config.env ?? {})
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([key, value]) => ({ key, value })),
  );
  const [busy, setBusy] = useState(false);

  const toggle = (t: string) =>
    setSelected((s) => (s.includes(t) ? s.filter((x) => x !== t) : [...s, t]));

  const addEnvRow = () =>
    setEnvRows((rows) => [...rows, { key: "", value: "" }]);
  const removeEnvRow = (idx: number) =>
    setEnvRows((rows) => rows.filter((_, i) => i !== idx));
  const setEnvRow = (idx: number, patch: Partial<{ key: string; value: string }>) =>
    setEnvRows((rows) => rows.map((r, i) => (i === idx ? { ...r, ...patch } : r)));

  const save = async () => {
    setBusy(true);
    try {
      const env: Record<string, string> = {};
      for (const { key, value } of envRows) {
        const k = key.trim();
        if (k) env[k] = value;
      }
      const cfg = (await saveProjectConfig({
        path: { name },
        body: {
          ...config,
          description: desc,
          deployTarget,
          main,
          platforms: selected,
          env,
        },
        throwOnError: true,
      })).data;
      onSaved(cfg);
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  };

  return (
    <Modal title="Configure project" onClose={onClose}>
      <label className="block text-xs uppercase tracking-wider text-fg-faint">Description</label>
      <input value={desc} onChange={(e) => setDesc(e.target.value)} className={inputCls} />
      <label className="mt-3 block text-xs uppercase tracking-wider text-fg-faint">Deploy target</label>
      <input value={deployTarget} onChange={(e) => setDeployTarget(e.target.value)} className={inputCls} placeholder="http://localhost:8080" />
      <p className="mt-1 text-[11px] text-fg-faint">REST API at &lt;target&gt;/v2; the Falcon protocol is dialled here too.</p>
      <label className="mt-3 block text-xs uppercase tracking-wider text-fg-faint">Entry point</label>
      <input value={main} onChange={(e) => setMain(e.target.value)} className={inputCls} placeholder="main.ts" />
      <div className="mt-3 flex items-center justify-between">
        <label className="block text-xs uppercase tracking-wider text-fg-faint">Environment variables</label>
        <button
          type="button"
          onClick={addEnvRow}
          className="text-[11px] text-accent hover:underline"
        >
          + Add
        </button>
      </div>
      <p className="mt-1 text-[11px] text-fg-faint">
        Passed to every Run/Compile spawn. Run-config env overrides these.
        Example: <span className="font-mono">CAMUNDA_REST_ADDRESS=http://localhost:8081</span>.
      </p>
      {envRows.length === 0 ? (
        <p className="mt-2 text-[11px] text-fg-faint italic">No env vars set.</p>
      ) : (
        <div className="mt-2 space-y-1">
          {envRows.map((row, i) => (
            <div key={i} className="flex gap-2">
              <input
                value={row.key}
                onChange={(e) => setEnvRow(i, { key: e.target.value })}
                placeholder="KEY"
                className={`${inputCls} font-mono w-2/5`}
              />
              <input
                value={row.value}
                onChange={(e) => setEnvRow(i, { value: e.target.value })}
                placeholder="value"
                className={`${inputCls} font-mono flex-1`}
              />
              <button
                type="button"
                onClick={() => removeEnvRow(i)}
                aria-label="Remove"
                className="px-2 text-fg-faint hover:text-fg"
              >
                ×
              </button>
            </div>
          ))}
        </div>
      )}
      <label className="mt-3 block text-xs uppercase tracking-wider text-fg-faint">Export platforms</label>
      <PlatformPicker platforms={platforms} selected={selected} onToggle={toggle} />
      <div className="mt-5 flex justify-end gap-2">
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="primary" onClick={() => void save()} disabled={busy}>
          {busy ? "Saving…" : "Save"}
        </Button>
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
      await compileProject({ path: { name }, body: { targets: selected }, throwOnError: true });
      onStarted();
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  };

  return (
    <Modal title="Compile application" onClose={onClose}>
      <p className="text-sm text-fg-muted">
        Produces standalone binaries under <span className="font-mono text-fg">dist/</span>. Leave all
        unchecked to compile for this host only. Cross-compiling downloads the
        Deno runtime per target and may take a few minutes — progress streams to
        the Output panel.
      </p>
      <div className="mt-3">
        <PlatformPicker platforms={platforms} selected={selected} onToggle={toggle} />
      </div>
      <div className="mt-5 flex justify-end gap-2">
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="primary" onClick={() => void compile()} disabled={busy}>
          {busy ? "Starting…" : "Compile"}
        </Button>
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
        <label key={t} className="flex cursor-pointer items-center gap-2 rounded-md border border-edge px-3 py-2 text-sm text-fg-muted hover:border-edge-strong">
          <input
            type="checkbox"
            checked={selected.includes(t)}
            onChange={() => onToggle(t)}
            className="accent-accent"
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
      <div className="w-full max-w-lg rounded-lg border border-edge bg-raised p-5 shadow-xl" onClick={(e) => e.stopPropagation()}>
        <h2 className="mb-4 text-lg font-semibold text-fg">{title}</h2>
        {children}
      </div>
    </div>
  );
}

/// Modal for starting a process instance. User pastes/edits a JSON variables
/// payload (defaults to `{}`); the POST goes to <deployTarget>/v2/process-instances.
/// Shows the returned processInstanceKey on success, or the server's problem
/// detail on failure — no navigation, so the user can start another instance
/// right away with tweaked variables.
function StartInstanceModal({
  processId,
  deployTarget,
  onClose,
}: {
  processId: string;
  deployTarget: string;
  onClose: () => void;
}) {
  const [json, setJson] = useState("{}");
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const start = async () => {
    setError(null);
    setResult(null);
    let variables: Record<string, unknown>;
    try {
      const parsed = json.trim() === "" ? {} : JSON.parse(json);
      if (parsed == null || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new Error("Variables must be a JSON object.");
      }
      variables = parsed as Record<string, unknown>;
    } catch (e) {
      setError(`Invalid JSON: ${e instanceof Error ? e.message : String(e)}`);
      return;
    }
    setBusy(true);
    try {
      const r = await createProcessInstance({
        processId,
        variables,
        baseUrl: deployTarget,
      });
      setResult(r.processInstanceKey);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal title={`Start instance — ${processId}`} onClose={onClose}>
      <p className="text-sm text-fg-muted">
        Posted to{" "}
        <span className="font-mono text-fg">{`${deployTarget.replace(/\/+$/, "")}/v2/process-instances`}</span>.
        Variables must be a JSON object; leave <span className="font-mono">{"{}"}</span> for no vars.
      </p>
      <label className="mt-3 block text-xs uppercase tracking-wider text-fg-faint">
        Variables (JSON)
      </label>
      <textarea
        value={json}
        onChange={(e) => setJson(e.target.value)}
        spellCheck={false}
        rows={8}
        className={`${inputCls} font-mono text-xs`}
      />
      {error && (
        <p className="mt-2 rounded-md border border-danger/40 bg-danger/10 px-3 py-2 text-xs text-danger">
          {error}
        </p>
      )}
      {result && (
        <p className="mt-2 rounded-md border border-ok/40 bg-ok/10 px-3 py-2 text-xs text-ok">
          Started — processInstanceKey <span className="font-mono">{result}</span>
        </p>
      )}
      <div className="mt-5 flex justify-end gap-2">
        <Button onClick={onClose}>Close</Button>
        <Button variant="primary" onClick={() => void start()} disabled={busy}>
          {busy ? "Starting…" : "Start"}
        </Button>
      </div>
    </Modal>
  );
}

const inputCls = `mt-1 w-full ${inputClass}`;
