import { useCallback, useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  createProject,
  deleteProject,
  importProject,
  listProjects,
  renameProject,
  type ProjectSummary,
  type ProjectTemplate,
} from "../gen";
import { Button, Card, EmptyState, Input, PageHeader, inputClass } from "../components/ui";

/// Resolved language-pack presentation for a project card: the pack's icon
/// (inline SVG markup or a data:/http: URL) and its human-facing name.
type LangMeta = { icon?: string; displayName: string };

/// Turn a pack `icon` (raw SVG markup, or an already-usable data:/http: URL)
/// into an `<img src>`. Inline SVG is wrapped in a `data:` URI so it renders
/// sandboxed (no script execution), which also means it can't inherit theme
/// colours — pack icons are authored as self-coloured tiles for that reason.
function iconSrc(icon: string): string {
  const s = icon.trim();
  if (s.startsWith("<")) return `data:image/svg+xml,${encodeURIComponent(s)}`;
  return s;
}

/// Mirrors the server's `is_safe_name` (console/workspace.rs) so the New Project
/// form can validate in real time instead of failing on submit. Returns a
/// human-readable error, or `null` when the name is acceptable.
function validateProjectName(
  raw: string,
  existing: ProjectSummary[],
): string | null {
  const name = raw.trim();
  if (!name) return null; // empty is "incomplete", not an error to shout about
  if (name.length > 128) return "Too long — 128 characters max.";
  if (name === "." || name.includes(".."))
    return "Cannot be “.” or contain “..”.";
  if (/\s/.test(name)) return "No spaces — use dashes or underscores instead.";
  const bad = [...name].find((c) => !/[A-Za-z0-9_.-]/.test(c));
  if (bad) return `Invalid character “${bad}”. Use letters, digits, dashes, underscores or dots.`;
  if (existing.some((p) => p.name.toLowerCase() === name.toLowerCase()))
    return "A project with that name already exists.";
  return null;
}

/// Home view of the RAD environment: every project as a tile (like the LLM
/// profile tiles), plus a create form. Opening a tile routes to its workspace.
export default function Projects() {
  const navigate = useNavigate();
  const [projects, setProjects] = useState<ProjectSummary[]>([]);
  const [denoAvailable, setDenoAvailable] = useState(true);
  const [nodeAvailable, setNodeAvailable] = useState(true);
  const [templates, setTemplates] = useState<ProjectTemplate[]>([]);
  const [langMeta, setLangMeta] = useState<Record<string, LangMeta>>({});
  const [newTemplate, setNewTemplate] = useState("starter");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);
  const [newName, setNewName] = useState("");
  const [newDesc, setNewDesc] = useState("");
  const [importing, setImporting] = useState(false);
  const [importName, setImportName] = useState("");
  const [importPath, setImportPath] = useState("");
  const [busy, setBusy] = useState(false);

  const reload = useCallback(async () => {
    try {
      const res = (await listProjects({ throwOnError: true })).data;
      setProjects(res.projects);
      setDenoAvailable(res.denoAvailable);
      setNodeAvailable(res.nodeAvailable);
      setTemplates(res.templates ?? []);
      const meta: Record<string, LangMeta> = {};
      for (const e of res.extensions?.extensions ?? []) {
        if (e.kind === "lang") meta[e.id] = { icon: e.icon, displayName: e.displayName };
      }
      setLangMeta(meta);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void reload();
  }, [reload]);

  const create = async () => {
    const name = newName.trim();
    if (!name || nameError) return;
    setBusy(true);
    try {
      await createProject({
        body: { name, description: newDesc.trim(), template: newTemplate },
        throwOnError: true,
      });
      navigate(`/projects/${encodeURIComponent(name)}`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  };

  const nameError = useMemo(
    () => validateProjectName(newName, projects),
    [newName, projects],
  );
  const nameValid = newName.trim().length > 0 && !nameError;

  const importNameError = useMemo(
    () => validateProjectName(importName, projects),
    [importName, projects],
  );
  const importValid =
    importName.trim().length > 0 &&
    importPath.trim().length > 0 &&
    !importNameError;

  /// Import an existing checked-out Urban app by reference (ADR 0041): point a
  /// name at an external directory, read live. The server validates the path is
  /// a Nano app/project and that the name is free.
  const importRef = async () => {
    const name = importName.trim();
    const path = importPath.trim();
    if (!importValid) return;
    setBusy(true);
    try {
      await importProject({ body: { name, path }, throwOnError: true });
      navigate(`/projects/${encodeURIComponent(name)}`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  };

  const remove = async (name: string) => {
    if (!confirm(`Delete project “${name}” and all its files? This cannot be undone.`)) return;
    try {
      await deleteProject({ path: { name }, throwOnError: true });
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const rename = async (name: string) => {
    const next = prompt(`Rename “${name}” to:`, name)?.trim();
    if (!next || next === name) return;
    try {
      await renameProject({ path: { name }, body: { newName: next }, throwOnError: true });
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <div className="mx-auto max-w-6xl p-8">
      <PageHeader
        title="Projects"
        subtitle="Self-contained applications — processes, decisions, forms, workers and shared libraries. Author, run, compile and export from one place."
        actions={
          <div className="flex gap-2">
            <Button
              variant="secondary"
              onClick={() => {
                setImporting((v) => !v);
                setCreating(false);
              }}
            >
              {importing ? "Cancel" : "Import by reference"}
            </Button>
            <Button
              variant="primary"
              onClick={() => {
                setCreating((v) => !v);
                setImporting(false);
              }}
            >
              {creating ? "Cancel" : "+ New project"}
            </Button>
          </div>
        }
      />

      {!nodeAvailable && !denoAvailable && (
        <div className="mb-4 rounded-md border border-warn/40 bg-warn/10 px-4 py-2 text-sm text-warn">
          No JavaScript runtime detected — you can author projects, but Run needs
          Node ≥ 22.6 (the npm launcher provides one) or Deno. Compile to a standalone
          binary additionally requires Deno.
        </div>
      )}
      {nodeAvailable && !denoAvailable && (
        <div className="mb-4 rounded-md border border-edge bg-subtle px-4 py-2 text-sm text-fg-muted">
          Deno not detected — Run works on Node. Install Deno (deno.com) only to
          Compile a project to a standalone single-file binary.
        </div>
      )}

      {error && (
        <div className="mb-4 rounded-md border border-danger/40 bg-danger/10 px-4 py-2 text-sm text-danger">
          {error}
        </div>
      )}

      {creating && (
        <Card className="mb-6 p-4">
          <div className="grid gap-3 sm:grid-cols-[1fr_2fr]">
            <div>
              <input
                autoFocus
                value={newName}
                onChange={(e) => setNewName(e.target.value)}
                placeholder="project-name"
                aria-invalid={!!nameError}
                className={`w-full rounded-md border bg-inset px-3 py-2 text-sm text-fg placeholder:text-fg-faint outline-none ${
                  nameError
                    ? "border-danger/70 focus:border-danger"
                    : "border-edge-strong focus:border-accent"
                }`}
                onKeyDown={(e) => e.key === "Enter" && nameValid && void create()}
              />
              <p
                className={`mt-1 text-xs ${
                  nameError ? "text-danger" : "text-fg-faint"
                }`}
              >
                {nameError ??
                  "Letters, digits, dashes, underscores and dots — no spaces."}
              </p>
            </div>
            <Input
              value={newDesc}
              onChange={(e) => setNewDesc(e.target.value)}
              placeholder="Short description (optional)"
              className="h-fit"
              onKeyDown={(e) => e.key === "Enter" && nameValid && void create()}
            />
          </div>
          {templates.length > 0 && (
            <div className="mt-3">
              <label className="mb-1 block text-xs text-fg-faint">Template</label>
              <select
                value={newTemplate}
                onChange={(e) => setNewTemplate(e.target.value)}
                className={`w-full ${inputClass}`}
              >
                {templates.map((t) => (
                  <option key={t.id} value={t.id}>
                    {t.label}
                  </option>
                ))}
              </select>
            </div>
          )}
          <div className="mt-3 flex items-center gap-3">
            <Button
              variant="primary"
              onClick={() => void create()}
              disabled={busy || !nameValid}
            >
              {busy ? "Creating…" : "Create project"}
            </Button>
            <span className="text-xs text-fg-faint">
              A starter app is scaffolded for you.
            </span>
          </div>
        </Card>
      )}

      {importing && (
        <Card className="mb-6 p-4">
          <div className="grid gap-3 sm:grid-cols-[1fr_2fr]">
            <div>
              <input
                autoFocus
                value={importName}
                onChange={(e) => setImportName(e.target.value)}
                placeholder="project-name"
                aria-invalid={!!importNameError}
                className={`w-full rounded-md border bg-inset px-3 py-2 text-sm text-fg placeholder:text-fg-faint outline-none ${
                  importNameError
                    ? "border-danger/70 focus:border-danger"
                    : "border-edge-strong focus:border-accent"
                }`}
                onKeyDown={(e) => e.key === "Enter" && importValid && void importRef()}
              />
              <p
                className={`mt-1 text-xs ${
                  importNameError ? "text-danger" : "text-fg-faint"
                }`}
              >
                {importNameError ?? "The name this app is registered under."}
              </p>
            </div>
            <div>
              <Input
                value={importPath}
                onChange={(e) => setImportPath(e.target.value)}
                placeholder="/absolute/path/to/checked-out/app"
                className="h-fit"
                onKeyDown={(e) => e.key === "Enter" && importValid && void importRef()}
              />
              <p className="mt-1 text-xs text-fg-faint">
                Absolute path to a checked-out Urban app directory
                (contains <code>nano.app.json</code> or{" "}
                <code>nanobpm.project.json</code>). Read live — no copy.
              </p>
            </div>
          </div>
          <div className="mt-3 flex items-center gap-3">
            <Button
              variant="primary"
              onClick={() => void importRef()}
              disabled={busy || !importValid}
            >
              {busy ? "Importing…" : "Import project"}
            </Button>
            <span className="text-xs text-fg-faint">
              The app runs from its source directory, so edits show up on the
              next Run.
            </span>
          </div>
        </Card>
      )}

      {loading ? (
        <div className="py-16 text-center text-sm text-fg-faint">Loading…</div>
      ) : projects.length === 0 ? (
        <EmptyState title="No projects yet." hint="Create one to get started." />
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {projects.map((p) => (
            <ProjectTile key={p.name} project={p} lang={langMeta[p.lang]} onOpen={() => navigate(`/projects/${encodeURIComponent(p.name)}`)} onDelete={() => void remove(p.name)} onRename={() => void rename(p.name)} />
          ))}
        </div>
      )}
    </div>
  );
}

function ProjectTile({
  project,
  lang,
  onOpen,
  onDelete,
  onRename,
}: {
  project: ProjectSummary;
  lang?: LangMeta;
  onOpen: () => void;
  onDelete: () => void;
  onRename: () => void;
}) {
  const langLabel = lang?.displayName ?? project.lang;
  return (
    <Card className="group relative flex flex-col p-4 transition-colors hover:border-edge-strong">
      <div className="absolute right-3 top-3 hidden gap-1 group-hover:flex">
        <button
          onClick={onRename}
          title="Rename project"
          className="rounded px-1.5 py-0.5 text-xs text-fg-faint hover:bg-hover hover:text-fg"
        >
          ✎
        </button>
        <button
          onClick={onDelete}
          title="Delete project"
          className="rounded px-1.5 py-0.5 text-xs text-fg-faint hover:bg-danger/10 hover:text-danger"
        >
          ✕
        </button>
      </div>
      <button onClick={onOpen} className="flex flex-1 flex-col text-left">
        <div className="flex items-center gap-2">
          {lang?.icon ? (
            <img
              src={iconSrc(lang.icon)}
              alt={langLabel}
              title={langLabel}
              className="h-5 w-5 shrink-0 rounded-sm"
            />
          ) : (
            <span
              title={langLabel}
              className="inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-sm bg-inset text-[9px] font-bold uppercase text-fg-faint"
            >
              {(project.lang || "?").slice(0, 2)}
            </span>
          )}
          <span className="truncate text-base font-semibold text-fg">{project.name}</span>
          {project.source === "path" && (
            <span
              title="Imported by reference — runs live from an external checked-out directory (ADR 0041)"
              className="inline-flex items-center gap-1 rounded-full bg-accent/10 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-accent"
            >
              ⧉ linked
            </span>
          )}
          {project.running && (
            <span className="inline-flex items-center gap-1 rounded-full bg-ok/10 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-ok">
              <span className="h-1.5 w-1.5 rounded-full bg-ok" /> running
            </span>
          )}
        </div>
        <p className="mt-1 line-clamp-2 min-h-[2.5rem] text-sm text-fg-faint">
          {project.description || "No description"}
        </p>
        <div className="mt-3 flex flex-wrap gap-x-3 gap-y-1 text-xs text-fg-muted">
          <Stat label="processes" value={project.processes} />
          <Stat label="decisions" value={project.decisions} />
          <Stat label="forms" value={project.forms} />
          <Stat label="workers" value={project.workers} />
        </div>
        <TemplateProvenance project={project} />
        <div className="mt-2 truncate text-[11px] text-fg-faint">→ {project.deployTarget}</div>
      </button>
    </Card>
  );
}

/// Where this project came from: the scaffold template id plus its origin —
/// a contributing pack (with version when known) or a built-in. Lets a bug in
/// generated project scaffolding be traced straight to the pack or built-in
/// template that produced it. Renders nothing for projects predating the
/// breadcrumb.
function TemplateProvenance({ project }: { project: ProjectSummary }) {
  if (!project.template) return null;
  const from = project.scaffoldedFrom;
  const origin = from
    ? `${from.pack}${from.version ? ` v${from.version}` : ""}`
    : "built-in";
  return (
    <div
      className="mt-2 flex items-center gap-1 truncate text-[11px] text-fg-faint"
      title={`Scaffolded from the “${project.template}” template (${origin})`}
    >
      <span aria-hidden>⧉</span>
      <span className="truncate">
        <span className="text-fg-muted">{project.template}</span>
        <span className="text-fg-faint"> · {origin}</span>
      </span>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: number }) {
  return (
    <span>
      <span className="font-semibold text-fg">{value}</span> {label}
    </span>
  );
}
