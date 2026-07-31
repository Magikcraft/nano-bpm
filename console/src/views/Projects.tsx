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
import { Button, Card, EmptyState, Input, PageHeader } from "../components/ui";
import JourneyPicker from "../components/JourneyPicker";
import DirectoryPicker from "../components/DirectoryPicker";
import { isLocalhost } from "../lib/api";
import { copyText } from "../lib/tour/runner";
import { CONSOLE_PROFILE } from "../lib/profile";
import { TOUR_ANCHOR, templateAnchor } from "../lib/tour/tourAnchors";
import { useTour } from "../lib/tour/tourContext";
import { pickerJourneys } from "../lib/tour/picker";
import {
  slugifyProjectName,
  validateProjectName,
  validateSafeName,
} from "../lib/projectName";

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

/// Home view of the RAD environment: every project as a tile (like the LLM
/// profile tiles). "New project" swaps to a template gallery — every scaffold
/// template as a card (title, language, description) — plus the name form.
/// Opening a project tile routes to its workspace.
export default function Projects() {
  const navigate = useNavigate();
  const tour = useTour();
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
  const [browsing, setBrowsing] = useState(false);
  const canBrowse = useMemo(() => isLocalhost(), []);
  const [busy, setBusy] = useState(false);
  // "Point your agent here" affordance (ADR 0051): reveals the /agent brief URL
  // an external coding agent can be aimed at to author an app and link it in.
  const [agentHint, setAgentHint] = useState(false);
  const [agentCopied, setAgentCopied] = useState(false);
  const agentUrl = useMemo(
    () => `${globalThis.location?.origin ?? ""}/agent`,
    [],
  );

  const reload = useCallback(async () => {
    try {
      const res = (await listProjects({ throwOnError: true })).data;
      setProjects(res.projects);
      setDenoAvailable(res.denoAvailable);
      setNodeAvailable(res.nodeAvailable);
      setTemplates(res.templates ?? []);
      const meta: Record<string, LangMeta> = {};
      for (const e of res.extensions?.extensions ?? []) {
        if (e.kind === "lang")
          meta[e.id] = { icon: e.icon, displayName: e.displayName };
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

  // The selected template card. Falls back to the first template so the
  // gallery never renders with nothing selected (e.g. a pack shadowed away
  // the remembered id between visits).
  const selectedTemplate =
    templates.find((t) => t.id === newTemplate) ?? templates[0];

  const create = async () => {
    const name = newName.trim();
    if (!name || nameError) return;
    setBusy(true);
    try {
      const res = await createProject({
        body: {
          name,
          description: newDesc.trim(),
          template: selectedTemplate?.id ?? "starter",
        },
        throwOnError: true,
      });
      // Route by the server's directory-safe slug — for a display name with
      // spaces ("Home Heating") the project lives at /projects/home-heating.
      navigate(`/projects/${encodeURIComponent(res.data.name)}`);
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
  const nameSlug = slugifyProjectName(newName);

  const importNameError = useMemo(
    () => validateSafeName(importName, projects),
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

  const remove = async (project: ProjectSummary) => {
    const { name, source } = project;
    const label = project.displayName ?? name;
    // A linked (imported-by-reference) project owns only a pointer file — the
    // backend deletes the reference and leaves the external checkout on disk
    // untouched (ADR 0041). Only a workspace project's files are actually
    // removed, so don't warn about deleting files we won't touch.
    const message =
      source === "path"
        ? `Remove the link to “${label}”? The files on disk won’t be deleted.`
        : `Delete project “${label}” and all its files? This cannot be undone.`;
    if (!confirm(message)) return;
    try {
      await deleteProject({ path: { name }, throwOnError: true });
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const rename = async (project: ProjectSummary) => {
    const current = project.displayName ?? project.name;
    const next = prompt(`Rename “${current}” to:`, current)?.trim();
    if (!next || next === current) return;
    try {
      await renameProject({
        path: { name: project.name },
        body: { newName: next },
        throwOnError: true,
      });
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const errorBanner = error && (
    <div className="mb-4 rounded-md border border-danger/40 bg-danger/10 px-4 py-2 text-sm text-danger">
      {error}
    </div>
  );

  // ── New Project: template gallery ─────────────────────────────────────────
  if (creating) {
    return (
      <div className="mx-auto max-w-6xl p-8">
        <PageHeader
          title="New project"
          subtitle="Pick a template and name your project — a runnable app is scaffolded for you."
          actions={
            <Button variant="secondary" onClick={() => setCreating(false)}>
              ← Back to projects
            </Button>
          }
        />

        {errorBanner}

        <Card className="mb-6 p-4">
          <div className="grid gap-3 sm:grid-cols-[1fr_2fr]">
            <div>
              <input
                autoFocus
                value={newName}
                onChange={(e) => setNewName(e.target.value)}
                placeholder="Project name"
                aria-invalid={!!nameError}
                className={`w-full rounded-md border bg-inset px-3 py-2 text-sm text-fg placeholder:text-fg-faint outline-none ${
                  nameError
                    ? "border-danger/70 focus:border-danger"
                    : "border-edge-strong focus:border-accent"
                }`}
                onKeyDown={(e) =>
                  e.key === "Enter" && nameValid && void create()
                }
              />
              <p
                className={`mt-1 text-xs ${
                  nameError ? "text-danger" : "text-fg-faint"
                }`}
              >
                {nameError ??
                  (nameSlug && nameSlug !== newName.trim()
                    ? `Files will live in “${nameSlug}”.`
                    : "Spaces are OK — files use a URL-safe slug.")}
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
          <div className="mt-3 flex items-center gap-3">
            <Button
              variant="primary"
              onClick={() => void create()}
              disabled={busy || !nameValid}
            >
              {busy ? "Creating…" : "Create project"}
            </Button>
            {selectedTemplate && (
              <span className="text-xs text-fg-faint">
                Template:{" "}
                <span className="text-fg-muted">{selectedTemplate.label}</span>
              </span>
            )}
          </div>
        </Card>

        {loading ? (
          <div className="py-16 text-center text-sm text-fg-faint">
            Loading…
          </div>
        ) : templates.length === 0 ? (
          <EmptyState
            title="No templates available."
            hint="The server reported no scaffold templates — check the gateway connection."
          />
        ) : (
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {templates.map((t) => (
              <TemplateTile
                key={t.id}
                template={t}
                lang={langMeta[t.lang]}
                selected={t.id === selectedTemplate?.id}
                onSelect={() => setNewTemplate(t.id)}
              />
            ))}
          </div>
        )}
      </div>
    );
  }

  // ── Projects home ──────────────────────────────────────────────────────────
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
                setAgentHint((v) => !v);
              }}
            >
              {agentHint ? "Hide agent" : "Build with an agent"}
            </Button>
            <Button
              variant="secondary"
              onClick={() => {
                setImporting((v) => !v);
              }}
            >
              {importing ? "Cancel" : "Import by reference"}
            </Button>
            <Button
              variant="primary"
              data-tour={TOUR_ANCHOR.newProject}
              onClick={() => {
                setCreating(true);
                setImporting(false);
              }}
            >
              + New project
            </Button>
          </div>
        }
      />

      {!nodeAvailable && !denoAvailable && (
        <div className="mb-4 rounded-md border border-warn/40 bg-warn/10 px-4 py-2 text-sm text-warn">
          No JavaScript runtime detected — you can author projects, but Run
          needs Node ≥ 22.6 (the npm launcher provides one) or Deno. Compile to
          a standalone binary additionally requires Deno.
        </div>
      )}
      {nodeAvailable && !denoAvailable && (
        <div className="mb-4 rounded-md border border-edge bg-subtle px-4 py-2 text-sm text-fg-muted">
          Deno not detected — Run works on Node. Install Deno (deno.com) only to
          Compile a project to a standalone single-file binary.
        </div>
      )}

      {errorBanner}

      {agentHint && (
        <Card className="mb-6 p-4">
          <h2 className="text-sm font-semibold text-fg">
            Build with a coding agent
          </h2>
          <p className="mt-1 text-sm text-fg-muted">
            Point your coding agent (Claude Code, Copilot CLI, or any MCP-driven
            assistant) at the URL below. It serves a live brief that teaches the
            agent how to author a Nano app on disk and link it into this node —
            and how Nano works, so it can explain it to you.
          </p>
          <div className="mt-3 flex items-center gap-2">
            <code className="flex-1 truncate rounded-md border border-edge bg-inset px-3 py-2 text-sm text-fg">
              {agentUrl}
            </code>
            <Button
              variant="secondary"
              onClick={() => {
                void copyText(agentUrl).then((ok) => {
                  if (!ok) return;
                  setAgentCopied(true);
                  setTimeout(() => setAgentCopied(false), 1500);
                });
              }}
            >
              {agentCopied ? "Copied" : "Copy"}
            </Button>
            <a
              href={agentUrl}
              target="_blank"
              rel="noreferrer"
              className="text-sm text-accent hover:underline"
            >
              Open
            </a>
          </div>
          <p className="mt-2 text-xs text-fg-faint">
            Tell the agent: <em>“Read {agentUrl} and build me an app.”</em> When
            it finishes, the app appears here in your Projects gallery.
          </p>
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
                onKeyDown={(e) =>
                  e.key === "Enter" && importValid && void importRef()
                }
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
              <div className="flex gap-2">
                <Input
                  value={importPath}
                  onChange={(e) => setImportPath(e.target.value)}
                  placeholder="/absolute/path/to/checked-out/app"
                  className="h-fit flex-1"
                  onKeyDown={(e) =>
                    e.key === "Enter" && importValid && void importRef()
                  }
                />
                {canBrowse && (
                  <Button
                    variant="secondary"
                    onClick={() => setBrowsing(true)}
                    title="Browse the server's filesystem"
                  >
                    Browse…
                  </Button>
                )}
              </div>
              <p className="mt-1 text-xs text-fg-faint">
                Absolute path to a checked-out Urban app directory (contains{" "}
                <code>nano.app.json</code> or <code>nanobpm.project.json</code>
                ). Read live — no copy.
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

      {browsing && (
        <DirectoryPicker
          initialPath={importPath.trim() || undefined}
          onPick={(path) => {
            setImportPath(path);
            setBrowsing(false);
          }}
          onClose={() => setBrowsing(false)}
        />
      )}

      {loading ? (
        <div className="py-16 text-center text-sm text-fg-faint">Loading…</div>
      ) : projects.length === 0 ? (
        // The empty state IS the journey picker (#411, ADR 0049 §2): a
        // first-timer with no projects chooses an outcome-shaped journey (or the
        // quiet overview) instead of hitting a dead-end "Create one to get
        // started." Cards are derived from the registry, so a new journey needs
        // no change here, and the whole block stops rendering once projects
        // exist — no dismissal state to track.
        <JourneyPicker
          title="No projects yet."
          subtitle="Pick a guided journey to build your first one — or explore on your own."
          journeys={
            tour ? pickerJourneys(tour.availableJourneys, CONSOLE_PROFILE) : []
          }
          onPick={(id) => tour?.startJourney(id)}
          onOverview={tour ? () => tour.startTour() : undefined}
          fallback={
            <EmptyState
              title="No projects yet."
              hint="Create one to get started."
            />
          }
        />
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {projects.map((p) => (
            <ProjectTile
              key={p.name}
              project={p}
              lang={langMeta[p.lang]}
              onOpen={() => navigate(`/projects/${encodeURIComponent(p.name)}`)}
              onDelete={() => void remove(p)}
              onRename={() => void rename(p)}
            />
          ))}
        </div>
      )}
    </div>
  );
}

/// The language badge shared by project and template cards: the lang pack's
/// icon when installed, else a two-letter fallback tile.
function LangIcon({ langId, lang }: { langId: string; lang?: LangMeta }) {
  const langLabel = lang?.displayName ?? langId;
  return lang?.icon ? (
    <img
      src={iconSrc(lang.icon)}
      alt={langLabel}
      title={langLabel}
      className="h-5 w-5 shrink-0 rounded-sm"
    />
  ) : (
    <span
      role="img"
      aria-label={langLabel}
      title={langLabel}
      className="inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-sm bg-inset text-[9px] font-bold uppercase text-fg-faint"
    >
      {(langId || "?").slice(0, 2)}
    </span>
  );
}

/// One scaffold template as a selectable card in the New Project gallery:
/// language icon + title, the language's name, the template's one-line
/// description, and its provenance (built-in or contributing pack).
function TemplateTile({
  template,
  lang,
  selected,
  onSelect,
}: {
  template: ProjectTemplate;
  lang?: LangMeta;
  selected: boolean;
  onSelect: () => void;
}) {
  const langLabel = lang?.displayName ?? template.lang;
  return (
    <Card
      data-tour={templateAnchor(template.id)}
      className={`flex flex-col p-4 transition-colors ${
        selected
          ? "border-accent ring-1 ring-accent"
          : "hover:border-edge-strong"
      }`}
    >
      <button
        type="button"
        onClick={onSelect}
        aria-pressed={selected}
        className="flex flex-1 flex-col text-left"
      >
        <div className="flex w-full items-center gap-2">
          <LangIcon langId={template.lang} lang={lang} />
          <span className="truncate text-base font-semibold text-fg">
            {template.label}
          </span>
          {selected && (
            <span
              aria-hidden
              className="ml-auto inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-accent text-[11px] font-bold text-on-accent"
            >
              ✓
            </span>
          )}
        </div>
        <div className="mt-1 text-xs text-fg-muted">{langLabel}</div>
        <p className="mt-1 line-clamp-3 min-h-[3.75rem] text-sm text-fg-faint">
          {template.description || "No description"}
        </p>
        <div
          className="mt-2 flex items-center gap-1 truncate text-[11px] text-fg-faint"
          title={
            template.source === "pack"
              ? `Contributed by the “${template.pack ?? "extension"}” extension pack`
              : "Built-in scaffold — works offline"
          }
        >
          {template.source === "pack" ? (
            <>
              <span aria-hidden>⧉</span>
              <span className="truncate">
                {template.pack ?? "Extension pack"}
              </span>
            </>
          ) : (
            <span>Built-in</span>
          )}
        </div>
      </button>
    </Card>
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
  const title = project.displayName ?? project.name;
  return (
    <Card className="group relative flex flex-col p-4 transition-colors hover:border-edge-strong">
      <div className="absolute right-3 top-3 hidden gap-1 group-hover:flex">
        <button
          type="button"
          onClick={onRename}
          title="Rename project"
          aria-label={`Rename project ${title}`}
          className="rounded px-1.5 py-0.5 text-xs text-fg-faint hover:bg-hover hover:text-fg"
        >
          ✎
        </button>
        <button
          type="button"
          onClick={onDelete}
          title={project.source === "path" ? "Remove link" : "Delete project"}
          aria-label={
            project.source === "path"
              ? `Remove link to ${title}`
              : `Delete project ${title}`
          }
          className="rounded px-1.5 py-0.5 text-xs text-fg-faint hover:bg-danger/10 hover:text-danger"
        >
          ✕
        </button>
      </div>
      <button onClick={onOpen} className="flex flex-1 flex-col text-left">
        <div className="flex items-center gap-2">
          <LangIcon langId={project.lang} lang={lang} />
          <span
            className="truncate text-base font-semibold text-fg"
            title={
              project.displayName
                ? `${project.displayName} (${project.name})`
                : project.name
            }
          >
            {title}
          </span>
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
        <div className="mt-2 truncate text-[11px] text-fg-faint">
          → {project.deployTarget}
        </div>
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
