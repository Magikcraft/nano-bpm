import { useCallback, useEffect, useMemo, useRef, useState, lazy, Suspense } from "react";
import { Link, useParams } from "react-router-dom";
import CodeEditor, { languageForFile } from "../components/CodeEditor";
import MarkdownPreview from "../components/MarkdownPreview";
import BpmnModeler, { type BpmnModelerHandle } from "../components/BpmnModeler";
import DmnModeler, { type DmnModelerHandle } from "../components/DmnModeler";
import FormEditor, { type FormEditorHandle } from "../components/FormEditor";
const TestRunPanel = lazy(() => import("../components/TestRunPanel"));
import {
  projectsApi,
  projectLogs,
  exportProject,
  deployXml,
  createProcessInstance,
  fetchDeployedXmlByProcessId,
  type FileNode,
  type ProjectDetail,
  type ProjectConfig,
  type RunState,
  type ProjectLogLine,
  type ProjectFile,
} from "../lib/api";
import { Button, inputClass } from "../components/ui";
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
      await projectsApi.setActiveRunConfig(name, id);
      void load();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

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
        await projectsApi.compileProject(name, []);
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
        <ToolbarButton onClick={() => setShowConfig(true)}>Configure</ToolbarButton>
        <ToolbarButton onClick={() => void exportProject(name, false)}>Export</ToolbarButton>
      </div>

      {!runnable && (
        <div className="border-b border-warn/30 bg-warn/10 px-4 py-1.5 text-xs text-warn">
          {lang === "deno"
            ? "No Deno runtime detected — Run and Compile are disabled. Authoring and Export still work."
            : `No ${lang} toolchain detected — install it (and approve the extension) to enable Run and Compile. Authoring and Export still work.`}
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
          <div className="min-h-0 flex-1 overflow-hidden border-b border-edge">
            {selected ? (
              <EditorPane
                key={selected}
                name={name}
                path={selected}
                deployTarget={detail.config.deployTarget}
              />
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
    <aside className="flex w-64 shrink-0 flex-col border-r border-edge bg-panel">
      <div className="flex items-center justify-between px-3 py-2 text-xs uppercase tracking-wider text-fg-faint">
        <span>Files</span>
        <div className="flex gap-1">
          <button title="New file" onClick={() => void newFile(false)} className="rounded px-1.5 py-0.5 hover:bg-hover hover:text-fg">
            ＋
          </button>
          <button title="New folder" onClick={() => void newFile(true)} className="rounded px-1.5 py-0.5 hover:bg-hover hover:text-fg">
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
          className="group flex cursor-pointer items-center gap-1 rounded py-1 pr-2 text-sm text-fg-muted hover:bg-hover"
        >
          <span className="text-fg-faint">{open ? "▾" : "▸"}</span>
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
          active ? "bg-accent/10 font-medium text-accent-strong" : "text-fg-muted hover:bg-hover hover:text-fg"
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
    projectsApi
      .projectFileEx(name, path)
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
      if (kind === "bpmn" && bpmnView === "xml") body = bpmnXml;
      else if (kind === "bpmn" && bpmnRef.current) body = await bpmnRef.current.getXml();
      else if (kind === "dmn" && dmnRef.current) body = await dmnRef.current.getXml();
      else if (kind === "form" && formRef.current) body = await formRef.current.getSchema();
      await projectsApi.saveProjectFile(name, path, body);
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
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }, [content, kind, name, path, bpmnView, bpmnXml]);

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
          disabled={saving || ((kind === "code" || kind === "md" || kind === "bpmn") && !dirty)}
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
              <BpmnModeler ref={bpmnRef} onChange={() => setDirty(true)} />
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
        {kind === "dmn" && <DmnModeler ref={dmnRef} onChange={() => setDirty(true)} />}
        {kind === "form" && <FormEditor ref={formRef} onChange={() => setDirty(true)} />}
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
      <label className="block text-xs uppercase tracking-wider text-fg-faint">Description</label>
      <input value={desc} onChange={(e) => setDesc(e.target.value)} className={inputCls} />
      <label className="mt-3 block text-xs uppercase tracking-wider text-fg-faint">Deploy target</label>
      <input value={deployTarget} onChange={(e) => setDeployTarget(e.target.value)} className={inputCls} placeholder="http://localhost:8080" />
      <p className="mt-1 text-[11px] text-fg-faint">REST API at &lt;target&gt;/v2; the Falcon protocol is dialled here too.</p>
      <label className="mt-3 block text-xs uppercase tracking-wider text-fg-faint">Entry point</label>
      <input value={main} onChange={(e) => setMain(e.target.value)} className={inputCls} placeholder="main.ts" />
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
      await projectsApi.compileProject(name, selected);
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
