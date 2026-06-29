import { useCallback, useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { projectsApi, type ProjectSummary, type ProjectTemplate } from "../lib/api";

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
  const [templates, setTemplates] = useState<ProjectTemplate[]>([]);
  const [newTemplate, setNewTemplate] = useState("starter");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);
  const [newName, setNewName] = useState("");
  const [newDesc, setNewDesc] = useState("");
  const [busy, setBusy] = useState(false);

  const reload = useCallback(async () => {
    try {
      const res = await projectsApi.projects();
      setProjects(res.projects);
      setDenoAvailable(res.denoAvailable);
      setTemplates(res.templates ?? []);
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
      await projectsApi.createProject(name, newDesc.trim(), newTemplate);
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

  const remove = async (name: string) => {
    if (!confirm(`Delete project “${name}” and all its files? This cannot be undone.`)) return;
    try {
      await projectsApi.deleteProject(name);
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const rename = async (name: string) => {
    const next = prompt(`Rename “${name}” to:`, name)?.trim();
    if (!next || next === name) return;
    try {
      await projectsApi.renameProject(name, next);
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <div className="mx-auto max-w-6xl p-8">
      <header className="mb-6 flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Projects</h1>
          <p className="mt-1 text-sm text-zinc-500">
            Self-contained applications — processes, decisions, forms, workers and
            shared libraries. Author, run, compile and export from one place.
          </p>
        </div>
        <button
          onClick={() => setCreating((v) => !v)}
          className="rounded-md bg-violet-600 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-violet-500"
        >
          {creating ? "Cancel" : "+ New project"}
        </button>
      </header>

      {!denoAvailable && (
        <div className="mb-4 rounded-md border border-amber-500/40 bg-amber-500/10 px-4 py-2 text-sm text-amber-300">
          No Deno runtime detected — you can author projects, but Run and Compile
          are disabled until Deno is installed.
        </div>
      )}

      {error && (
        <div className="mb-4 rounded-md border border-red-500/40 bg-red-500/10 px-4 py-2 text-sm text-red-300">
          {error}
        </div>
      )}

      {creating && (
        <div className="mb-6 rounded-lg border border-zinc-800 bg-zinc-900 p-4">
          <div className="grid gap-3 sm:grid-cols-[1fr_2fr]">
            <div>
              <input
                autoFocus
                value={newName}
                onChange={(e) => setNewName(e.target.value)}
                placeholder="project-name"
                aria-invalid={!!nameError}
                className={`w-full rounded-md border bg-zinc-950 px-3 py-2 text-sm text-zinc-100 placeholder-zinc-600 outline-none ${
                  nameError
                    ? "border-red-500/70 focus:border-red-500"
                    : "border-zinc-700 focus:border-violet-500"
                }`}
                onKeyDown={(e) => e.key === "Enter" && nameValid && void create()}
              />
              <p
                className={`mt-1 text-xs ${
                  nameError ? "text-red-400" : "text-zinc-500"
                }`}
              >
                {nameError ??
                  "Letters, digits, dashes, underscores and dots — no spaces."}
              </p>
            </div>
            <input
              value={newDesc}
              onChange={(e) => setNewDesc(e.target.value)}
              placeholder="Short description (optional)"
              className="h-fit rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 placeholder-zinc-600 outline-none focus:border-violet-500"
              onKeyDown={(e) => e.key === "Enter" && nameValid && void create()}
            />
          </div>
          {templates.length > 0 && (
            <div className="mt-3">
              <label className="mb-1 block text-xs text-zinc-500">Template</label>
              <select
                value={newTemplate}
                onChange={(e) => setNewTemplate(e.target.value)}
                className="w-full rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-violet-500"
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
            <button
              onClick={() => void create()}
              disabled={busy || !nameValid}
              className="rounded-md bg-violet-600 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-violet-500 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {busy ? "Creating…" : "Create project"}
            </button>
            <span className="text-xs text-zinc-500">
              A starter app is scaffolded for you.
            </span>
          </div>
        </div>
      )}

      {loading ? (
        <div className="py-16 text-center text-sm text-zinc-500">Loading…</div>
      ) : projects.length === 0 ? (
        <div className="rounded-lg border border-dashed border-zinc-800 py-16 text-center text-sm text-zinc-500">
          No projects yet. Create one to get started.
        </div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {projects.map((p) => (
            <ProjectTile key={p.name} project={p} onOpen={() => navigate(`/projects/${encodeURIComponent(p.name)}`)} onDelete={() => void remove(p.name)} onRename={() => void rename(p.name)} />
          ))}
        </div>
      )}
    </div>
  );
}

function ProjectTile({
  project,
  onOpen,
  onDelete,
  onRename,
}: {
  project: ProjectSummary;
  onOpen: () => void;
  onDelete: () => void;
  onRename: () => void;
}) {
  return (
    <div className="group relative flex flex-col rounded-lg border border-zinc-800 bg-zinc-900 p-4 transition-colors hover:border-zinc-600">
      <div className="absolute right-3 top-3 hidden gap-1 group-hover:flex">
        <button
          onClick={onRename}
          title="Rename project"
          className="rounded px-1.5 py-0.5 text-xs text-zinc-500 hover:bg-zinc-700/40 hover:text-zinc-200"
        >
          ✎
        </button>
        <button
          onClick={onDelete}
          title="Delete project"
          className="rounded px-1.5 py-0.5 text-xs text-zinc-500 hover:bg-red-500/10 hover:text-red-400"
        >
          ✕
        </button>
      </div>
      <button onClick={onOpen} className="flex flex-1 flex-col text-left">
        <div className="flex items-center gap-2">
          <span className="truncate text-base font-semibold text-zinc-100">{project.name}</span>
          {project.running && (
            <span className="inline-flex items-center gap-1 rounded-full bg-emerald-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-emerald-400">
              <span className="h-1.5 w-1.5 rounded-full bg-emerald-400" /> running
            </span>
          )}
        </div>
        <p className="mt-1 line-clamp-2 min-h-[2.5rem] text-sm text-zinc-500">
          {project.description || "No description"}
        </p>
        <div className="mt-3 flex flex-wrap gap-x-3 gap-y-1 text-xs text-zinc-400">
          <Stat label="processes" value={project.processes} />
          <Stat label="decisions" value={project.decisions} />
          <Stat label="forms" value={project.forms} />
          <Stat label="workers" value={project.workers} />
        </div>
        <div className="mt-3 truncate text-[11px] text-zinc-600">→ {project.deployTarget}</div>
      </button>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: number }) {
  return (
    <span>
      <span className="font-semibold text-zinc-200">{value}</span> {label}
    </span>
  );
}
