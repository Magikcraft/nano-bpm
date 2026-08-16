import { useEffect, useMemo, useState, type ReactNode } from "react";
import {
  getExtensionChangelog,
  getExtensionReadme,
  getExtensions,
  getMarketplace,
  installExtension,
  removeExtension,
  trustExtension,
  type ExtensionsOverview,
  type MarketEntry,
} from "../gen";
import MarkdownPreview from "../components/MarkdownPreview";
import { registerFileTypesFromOverview } from "../lib/editorLang";
import { setIntellisenseFromOverview } from "../lib/langIntellisense";
import { useTheme } from "../theme/ThemeProvider";
import { isThemeSpec } from "../theme/themes";
import {
  Badge,
  Button,
  Card,
  ErrorText,
  Input,
  PageHeader,
  Spinner,
} from "../components/ui";

// The extension authoring & publishing guide, bundled into the offline docs site
// (built from docs/extensions.md by console/scripts/build-docs.mjs) and served by
// the gateway at /docs/extensions. Linked from the marketplace header so authors
// can find out how to build and ship their own pack — no repo access required.
const EXTENSION_AUTHORING_GUIDE_URL = "/docs/extensions";

const CATEGORIES = [
  { id: "agentic-sdlc", label: "Agentic SDLC" },
  { id: "lang", label: "Languages" },
  { id: "app", label: "App templates" },
  { id: "example", label: "Example apps" },
  { id: "trigger", label: "Triggers" },
  { id: "theme", label: "Themes" },
] as const;

// Collapsible marketplace section whose collapsed/expanded state persists across
// page reloads (localStorage, keyed per section id). Sections default to expanded.
function CollapsibleSection({
  id,
  label,
  count,
  children,
}: {
  id: string;
  label: string;
  count?: number;
  children: ReactNode;
}) {
  const key = `nano.ext.section.${id}`;
  const [collapsed, setCollapsed] = useState<boolean>(
    () => localStorage.getItem(key) === "1",
  );
  const toggle = () => {
    setCollapsed((c) => {
      const next = !c;
      localStorage.setItem(key, next ? "1" : "0");
      return next;
    });
  };
  return (
    <section className="mb-5">
      <button
        type="button"
        onClick={toggle}
        aria-expanded={!collapsed}
        className="mb-2 flex w-full items-center gap-2 text-left"
      >
        <span
          className={`text-fg-faint transition-transform ${collapsed ? "" : "rotate-90"}`}
          aria-hidden="true"
        >
          ▶
        </span>
        <span className="text-xs font-semibold uppercase tracking-wider text-fg-faint">
          {label}
        </span>
        {typeof count === "number" && (
          <span className="text-xs text-fg-faint">({count})</span>
        )}
      </button>
      {!collapsed && children}
    </section>
  );
}

// Extension marketplace (ADR 0007): browse built-in + installed packs, search the
// npm marketplace (keyword nano-ide-ext) by language/app/example/theme, install
// packs, and manage toolchain trust.
export default function Extensions() {
  const [ov, setOv] = useState<ExtensionsOverview | null>(null);
  const [market, setMarket] = useState<MarketEntry[] | null>(null);
  const [marketErr, setMarketErr] = useState<string | null>(null);
  const [q, setQ] = useState("");
  const [busy, setBusy] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  // README pack-detail drawer: the pack being viewed + its fetched markdown.
  const [readmePkg, setReadmePkg] = useState<MarketEntry | null>(null);
  const [readmeMd, setReadmeMd] = useState<string | null>(null);
  const [readmeErr, setReadmeErr] = useState<string | null>(null);
  // Pack-detail drawer tab + lazily-fetched changelog ("What's changed").
  const [drawerTab, setDrawerTab] = useState<"readme" | "changelog">("readme");
  const [changelogMd, setChangelogMd] = useState<string | null>(null);
  const [changelogErr, setChangelogErr] = useState<string | null>(null);
  const [changelogDelta, setChangelogDelta] = useState(false);
  const { selection, select } = useTheme();

  const load = async () => {
    const next = (await getExtensions({ throwOnError: true })).data;
    setOv(next);
    // Refresh Monaco's ext→language map so a pack installed just now lights
    // up in the editor without a page reload.
    registerFileTypesFromOverview(next);
    setIntellisenseFromOverview(next);
  };
  const loadMarket = async () => {
    setMarketErr(null);
    try {
      setMarket((await getMarketplace({ throwOnError: true })).data.entries);
    } catch (e) {
      setMarketErr(String(e));
    }
  };
  useEffect(() => {
    void load();
    void loadMarket();
  }, []);

  // Poll the marketplace every 30s while this view is mounted so freshly
  // published pack versions (and thus the "Update" affordance next to each
  // installed pack) surface without the user having to leave and come back.
  // The left-rail badge is refreshed on the same cadence from App.tsx.
  useEffect(() => {
    const id = window.setInterval(() => {
      if (!document.hidden) void loadMarket();
    }, 30_000);
    const onVis = () => {
      if (!document.hidden) void loadMarket();
    };
    document.addEventListener("visibilitychange", onVis);
    return () => {
      window.clearInterval(id);
      document.removeEventListener("visibilitychange", onVis);
    };
  }, []);

  const install = async (pkg: string) => {
    setBusy(pkg);
    setErr(null);
    try {
      await installExtension({ body: { pkg }, throwOnError: true });
      await load();
      await loadMarket();
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(null);
    }
  };
  const toggleYolo = async () =>
    setOv(
      (await trustExtension({ body: { yolo: !ov?.yolo }, throwOnError: true }))
        .data,
    );
  const approve = async (id: string, on: boolean) =>
    setOv(
      (
        await trustExtension({
          body: on ? { approve: id } : { revoke: id },
          throwOnError: true,
        })
      ).data,
    );
  const remove = async (id: string, label: string) => {
    if (
      !window.confirm(
        `Uninstall extension "${label}"?\n\nProjects scaffolded from it will keep their files but lose their toolchain (Run/Compile may fall back to Deno).`,
      )
    ) {
      return;
    }
    setErr(null);
    try {
      await removeExtension({ body: { pkg: id }, throwOnError: true });
      await load();
      await loadMarket();
    } catch (e) {
      setErr(String(e));
    }
  };

  const filtered = useMemo(() => {
    const t = q.trim().toLowerCase();
    return (market ?? []).filter(
      (m) => !t || `${m.name} ${m.description}`.toLowerCase().includes(t),
    );
  }, [market, q]);

  // Open the pack-detail drawer and lazily fetch its README markdown (from the
  // installed copy, else npm). Re-fetches each open so an update's new docs show.
  const openReadme = async (
    m: MarketEntry,
    tab: "readme" | "changelog" = "readme",
  ) => {
    setReadmePkg(m);
    setReadmeMd(null);
    setReadmeErr(null);
    setChangelogMd(null);
    setChangelogErr(null);
    setChangelogDelta(false);
    setDrawerTab(tab);
    if (tab === "changelog") void fetchChangelog(m, true);
    try {
      setReadmeMd(
        (
          await getExtensionReadme({
            query: { pkg: m.name },
            throwOnError: true,
          })
        ).data.readme,
      );
    } catch {
      setReadmeErr("No README available for this pack.");
    }
  };

  // Lazily fetch a pack's changelog for the "What's changed" tab. For an
  // installed pack with an update available we pass the installed version so the
  // server scopes the view to the delta (installed → latest), else the full
  // changelog. Guarded so a manual tab click only fetches once per drawer open;
  // callers that have just reset the changelog state (e.g. `openReadme`) pass
  // `force` to bypass the guard, since the state resets are async and the stale
  // closure values would otherwise skip the fetch and wedge on "Loading…".
  const fetchChangelog = async (m: MarketEntry, force = false) => {
    if (!force && (changelogMd !== null || changelogErr !== null)) return;
    try {
      const res = (
        await getExtensionChangelog({
          query: {
            pkg: m.name,
            from: m.updateAvailable
              ? (m.installedVersion ?? undefined)
              : undefined,
            to: m.version,
          },
          throwOnError: true,
        })
      ).data;
      setChangelogMd(res.changelog);
      setChangelogDelta(res.delta);
    } catch {
      setChangelogErr("No changelog available for this pack.");
    }
  };

  const selectDrawerTab = (tab: "readme" | "changelog") => {
    setDrawerTab(tab);
    if (tab === "changelog" && readmePkg) void fetchChangelog(readmePkg);
  };

  const marketCard = (m: MarketEntry) => (
    <Card
      key={m.name}
      className="group flex items-start justify-between gap-3 p-3"
    >
      <div className="min-w-0 flex-1">
        <div className="flex flex-wrap items-baseline gap-x-2">
          <button
            type="button"
            onClick={() => void openReadme(m)}
            className="break-all text-left font-medium text-fg hover:text-accent hover:underline"
            title="View README"
          >
            {m.name}
          </button>
          <span className="text-xs text-fg-faint">
            {m.installed &&
            m.installedVersion &&
            m.installedVersion !== m.version
              ? `${m.installedVersion} → ${m.version}`
              : m.version}
          </span>
        </div>
        <div className="mt-0.5 whitespace-normal break-words text-xs text-fg-faint">
          {m.description}
        </div>
      </div>
      {m.updateAvailable ? (
        <div className="flex shrink-0 items-center gap-3">
          {m.changelogAvailable && (
            <button
              type="button"
              onClick={() => void openReadme(m, "changelog")}
              className="text-xs text-accent hover:underline"
              title="See what changed between your version and the latest"
            >
              What's changed
            </button>
          )}
          <Button
            variant="secondary"
            size="sm"
            className="border-warn/40 text-warn"
            onClick={() => void install(m.name)}
            disabled={busy === m.name}
          >
            {busy === m.name ? (
              <>
                <Spinner /> Updating…
              </>
            ) : (
              "Update"
            )}
          </Button>
          <button
            onClick={() => void remove(m.name, m.name)}
            className="text-xs text-danger opacity-0 transition-opacity hover:underline focus:opacity-100 group-hover:opacity-100"
            aria-label={`Uninstall ${m.name}`}
          >
            remove
          </button>
        </div>
      ) : m.installed ? (
        <div className="flex shrink-0 items-center gap-3">
          <span className="text-xs text-ok">installed</span>
          <button
            onClick={() => void remove(m.name, m.name)}
            className="text-xs text-danger opacity-0 transition-opacity hover:underline focus:opacity-100 group-hover:opacity-100"
            aria-label={`Uninstall ${m.name}`}
          >
            remove
          </button>
        </div>
      ) : (
        <Button
          variant="primary"
          size="sm"
          className="shrink-0"
          onClick={() => void install(m.name)}
          disabled={busy === m.name}
        >
          {busy === m.name ? (
            <>
              <Spinner /> Installing…
            </>
          ) : (
            "Install"
          )}
        </Button>
      )}
    </Card>
  );

  // Community packs = any marketplace entry not published under the @nanobpm
  // scope (official === false), regardless of category.
  const community = filtered.filter((m) => !m.official);

  // Official packs whose classified category has no dedicated section above
  // (e.g. an unrecognised/`other` category) — surfaced under "Other" so a
  // first-party pack is never silently dropped from the marketplace view.
  const officialOther = filtered.filter(
    (m) => m.official && !CATEGORIES.some((c) => c.id === m.category),
  );

  return (
    <div className="mx-auto max-w-4xl p-6">
      <PageHeader
        title="Extensions"
        subtitle={
          <>
            Language, app-template, example-app, and theme packs drive editor
            grammars, project scaffolds, the run/compile toolchain, and the
            console's look. Toolchain commands run on your machine.{" "}
            <a
              href={EXTENSION_AUTHORING_GUIDE_URL}
              target="_blank"
              rel="noopener noreferrer"
              className="text-accent hover:underline"
            >
              Publish your own extension →
            </a>
          </>
        }
      />

      <Input
        value={q}
        onChange={(e) => setQ(e.target.value)}
        placeholder="Search the marketplace…"
        className="mb-3 w-full"
      />
      {err && (
        <div className="mb-3">
          <ErrorText>{err}</ErrorText>
        </div>
      )}
      <label className="mb-4 flex items-center gap-2 text-sm text-fg-muted">
        <input
          type="checkbox"
          checked={!!ov?.yolo}
          onChange={() => void toggleYolo()}
        />
        Yolo mode — run any extension toolchain without prompting
      </label>

      {marketErr && (
        <div className="mb-4 text-sm text-warn">
          Marketplace unavailable (offline?). Installed + built-in packs still
          shown below.
        </div>
      )}
      {market === null && !marketErr && (
        <div className="mb-4 text-sm text-fg-faint">Loading marketplace…</div>
      )}

      {CATEGORIES.map((cat) => {
        // First-party (@nanobpm) packs only; community packs get their own section.
        const items = filtered.filter(
          (m) => m.official && m.category === cat.id,
        );
        if (items.length === 0) return null;
        return (
          <CollapsibleSection
            key={cat.id}
            id={cat.id}
            label={cat.label}
            count={items.length}
          >
            <div className="grid gap-2">{items.map(marketCard)}</div>
          </CollapsibleSection>
        );
      })}

      {officialOther.length > 0 && (
        <CollapsibleSection
          id="other"
          label="Other"
          count={officialOther.length}
        >
          <div className="grid gap-2">{officialOther.map(marketCard)}</div>
        </CollapsibleSection>
      )}

      {community.length > 0 && (
        <CollapsibleSection
          id="community"
          label="Community extensions"
          count={community.length}
        >
          <div className="grid gap-2">{community.map(marketCard)}</div>
        </CollapsibleSection>
      )}

      <CollapsibleSection id="installed" label="Installed">
        <div className="grid gap-3">
          {ov?.extensions.map((e) => (
            <Card key={e.id} className="p-3">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
                    <span className="break-all font-medium text-fg">
                      {e.displayName}
                    </span>
                    <Badge tone={e.kind === "theme" ? "accent" : "neutral"}>
                      {e.kind}
                    </Badge>
                    {e.builtin && (
                      <span className="text-xs text-fg-faint">built-in</span>
                    )}
                    {e.kind !== "theme" && !e.toolchainAvailable && (
                      <span className="text-xs text-warn">
                        toolchain missing
                      </span>
                    )}
                  </div>
                </div>
                <div className="flex flex-wrap items-center gap-3 text-xs">
                  {!e.builtin && (
                    <label className="flex items-center gap-1 text-fg-muted">
                      <input
                        type="checkbox"
                        checked={e.trusted}
                        onChange={(c) => void approve(e.id, c.target.checked)}
                      />
                      approve
                    </label>
                  )}
                </div>
              </div>
              {(e.fileTypes.length > 0 || e.templates.length > 0) && (
                <div className="mt-1 whitespace-normal break-words text-xs text-fg-faint">
                  {e.fileTypes.map((f) => f.ext).join(" ")}{" "}
                  {e.templates.map((t) => t.id).join(", ")}
                </div>
              )}
              {(e.themes ?? []).filter(isThemeSpec).length > 0 && (
                <div className="mt-2 flex flex-wrap gap-2">
                  {(e.themes ?? []).filter(isThemeSpec).map((t) => {
                    const active =
                      selection.mode === "theme" && selection.id === t.id;
                    return (
                      <Button
                        key={t.id}
                        size="sm"
                        variant={active ? "primary" : "secondary"}
                        onClick={() => select({ mode: "theme", id: t.id })}
                      >
                        {active ? `✓ ${t.label}` : `Apply ${t.label}`}
                      </Button>
                    );
                  })}
                </div>
              )}
            </Card>
          ))}
        </div>
      </CollapsibleSection>

      {readmePkg && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
          onClick={() => setReadmePkg(null)}
        >
          <div
            className="flex max-h-[85vh] w-full max-w-3xl flex-col overflow-hidden rounded-lg border border-edge-strong bg-panel shadow-xl"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-start justify-between gap-3 border-b border-edge px-5 py-3">
              <div className="min-w-0">
                <h2 className="break-all text-sm font-semibold text-fg">
                  {readmePkg.name}
                </h2>
                <p className="mt-0.5 text-xs text-fg-faint">
                  {readmePkg.version}
                  {readmePkg.installed ? " · installed" : ""}
                </p>
              </div>
              <div className="flex shrink-0 items-center gap-3">
                {!readmePkg.installed && (
                  <Button
                    variant="primary"
                    size="sm"
                    onClick={() => void install(readmePkg.name)}
                    disabled={busy === readmePkg.name}
                  >
                    {busy === readmePkg.name ? (
                      <>
                        <Spinner /> Installing…
                      </>
                    ) : (
                      "Install"
                    )}
                  </Button>
                )}
                <button
                  onClick={() => setReadmePkg(null)}
                  className="text-fg-faint hover:text-fg"
                  aria-label="Close"
                >
                  ✕
                </button>
              </div>
            </div>
            <div className="min-h-0 flex-1 overflow-auto">
              {/* Always offer the Changelog tab, including for not-yet-installed
                  marketplace entries: `changelogAvailable` is an installed-only
                  offline probe, so gating on it hid the tab for market packs
                  (and installed packs whose changelog only lives in the tarball).
                  The server returns 404 gracefully when a pack has none. */}
              {readmePkg && (
                <div className="sticky top-0 z-10 flex gap-1 border-b border-edge bg-panel px-4 pt-2">
                  <button
                    type="button"
                    onClick={() => selectDrawerTab("readme")}
                    className={`rounded-t px-3 py-1.5 text-xs font-medium ${
                      drawerTab === "readme"
                        ? "border-b-2 border-accent text-fg"
                        : "text-fg-faint hover:text-fg"
                    }`}
                  >
                    README
                  </button>
                  <button
                    type="button"
                    onClick={() => selectDrawerTab("changelog")}
                    className={`rounded-t px-3 py-1.5 text-xs font-medium ${
                      drawerTab === "changelog"
                        ? "border-b-2 border-accent text-fg"
                        : "text-fg-faint hover:text-fg"
                    }`}
                  >
                    {readmePkg.updateAvailable ? "What's changed" : "Changelog"}
                  </button>
                </div>
              )}
              {drawerTab === "changelog" ? (
                changelogErr ? (
                  <div className="px-6 py-5 text-sm text-fg-faint">
                    {changelogErr}
                  </div>
                ) : changelogMd === null ? (
                  <div className="px-6 py-5 text-sm text-fg-faint">
                    Loading changelog…
                  </div>
                ) : (
                  <>
                    {changelogDelta && (
                      <div className="px-6 pt-4 text-xs text-fg-faint">
                        Showing changes since your installed version
                        {readmePkg.installedVersion
                          ? ` (${readmePkg.installedVersion} → ${readmePkg.version})`
                          : ""}
                        .
                      </div>
                    )}
                    <MarkdownPreview source={changelogMd} />
                  </>
                )
              ) : readmeErr ? (
                <div className="px-6 py-5 text-sm text-fg-faint">
                  {readmeErr}
                </div>
              ) : readmeMd === null ? (
                <div className="px-6 py-5 text-sm text-fg-faint">
                  Loading README…
                </div>
              ) : (
                <MarkdownPreview source={readmeMd} />
              )}
            </div>
            {(readmePkg.repository ||
              readmePkg.homepage ||
              readmePkg.npmUrl) && (
              <div className="flex flex-wrap items-center gap-4 border-t border-edge px-5 py-3 text-xs">
                {readmePkg.repository && (
                  <a
                    href={readmePkg.repository}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1.5 text-accent hover:underline"
                    title="View source and report issues upstream"
                  >
                    <svg
                      viewBox="0 0 16 16"
                      width="14"
                      height="14"
                      fill="currentColor"
                      aria-hidden="true"
                    >
                      <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8Z" />
                    </svg>
                    Source repository
                  </a>
                )}
                {readmePkg.homepage &&
                  readmePkg.homepage !== readmePkg.repository && (
                    <a
                      href={readmePkg.homepage}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-accent hover:underline"
                    >
                      Homepage
                    </a>
                  )}
                {readmePkg.npmUrl && (
                  <a
                    href={readmePkg.npmUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-accent hover:underline"
                  >
                    View on npm
                  </a>
                )}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
