import { useEffect, useMemo, useState } from "react";
import {
  getExtensions,
  getMarketplace,
  installExtension,
  removeExtension,
  trustExtension,
  type ExtensionsOverview,
  type MarketEntry,
} from "../gen";
import { registerFileTypesFromOverview } from "../lib/editorLang";
import { useTheme } from "../theme/ThemeProvider";
import { isThemeSpec } from "../theme/themes";
import { Badge, Button, Card, ErrorText, Input, PageHeader, SectionLabel } from "../components/ui";

const CATEGORIES = [
  { id: "lang", label: "Languages" },
  { id: "app", label: "App templates" },
  { id: "example", label: "Example apps" },
  { id: "theme", label: "Themes" },
] as const;

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
  const { selection, select } = useTheme();

  const load = async () => {
    const next = (await getExtensions({ throwOnError: true })).data;
    setOv(next);
    // Refresh Monaco's ext→language map so a pack installed just now lights
    // up in the editor without a page reload.
    registerFileTypesFromOverview(next);
  };
  const loadMarket = async () => {
    setMarketErr(null);
    try { setMarket((await getMarketplace({ throwOnError: true })).data.entries); }
    catch (e) { setMarketErr(String(e)); }
  };
  useEffect(() => { void load(); void loadMarket(); }, []);

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
    setBusy(pkg); setErr(null);
    try { await installExtension({ body: { pkg }, throwOnError: true }); await load(); await loadMarket(); }
    catch (e) { setErr(String(e)); }
    finally { setBusy(null); }
  };
  const toggleYolo = async () =>
    setOv((await trustExtension({ body: { yolo: !ov?.yolo }, throwOnError: true })).data);
  const approve = async (id: string, on: boolean) =>
    setOv(
      (await trustExtension({ body: on ? { approve: id } : { revoke: id }, throwOnError: true })).data,
    );
  const remove = async (id: string, label: string) => {
    if (!window.confirm(`Uninstall extension "${label}"?\n\nProjects scaffolded from it will keep their files but lose their toolchain (Run/Compile may fall back to Deno).`)) {
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
    return (market ?? []).filter((m) => !t || `${m.name} ${m.description}`.toLowerCase().includes(t));
  }, [market, q]);

  return (
    <div className="mx-auto max-w-4xl p-6">
      <PageHeader
        title="Extensions"
        subtitle="Language, app-template, example-app, and theme packs drive editor grammars, project scaffolds, the run/compile toolchain, and the console's look. Toolchain commands run on your machine."
      />

      <Input
        value={q}
        onChange={(e) => setQ(e.target.value)}
        placeholder="Search the marketplace…"
        className="mb-3 w-full"
      />
      {err && <div className="mb-3"><ErrorText>{err}</ErrorText></div>}
      <label className="mb-4 flex items-center gap-2 text-sm text-fg-muted">
        <input type="checkbox" checked={!!ov?.yolo} onChange={() => void toggleYolo()} />
        Yolo mode — run any extension toolchain without prompting
      </label>

      {marketErr && (
        <div className="mb-4 text-sm text-warn">
          Marketplace unavailable (offline?). Installed + built-in packs still shown below.
        </div>
      )}
      {market === null && !marketErr && <div className="mb-4 text-sm text-fg-faint">Loading marketplace…</div>}

      {CATEGORIES.map((cat) => {
        const items = filtered.filter((m) => m.category === cat.id);
        if (items.length === 0) return null;
        return (
          <section key={cat.id} className="mb-5">
            <SectionLabel>{cat.label}</SectionLabel>
            <div className="grid gap-2">
              {items.map((m) => (
                <Card key={m.name} className="group flex items-start justify-between gap-3 p-3">
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-baseline gap-x-2">
                      <span className="break-all font-medium text-fg">{m.name}</span>
                      <span className="text-xs text-fg-faint">
                        {m.installed && m.installedVersion && m.installedVersion !== m.version
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
                      <Button
                        variant="secondary"
                        size="sm"
                        className="border-warn/40 text-warn"
                        onClick={() => void install(m.name)}
                        disabled={busy === m.name}
                      >
                        {busy === m.name ? "Updating…" : "Update"}
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
                      {busy === m.name ? "Installing…" : "Install"}
                    </Button>
                  )}
                </Card>
              ))}
            </div>
          </section>
        );
      })}

      <div className="mt-6">
        <SectionLabel>Installed</SectionLabel>
      </div>
      <div className="grid gap-3">
        {ov?.extensions.map((e) => (
          <Card key={e.id} className="p-3">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div className="min-w-0 flex-1">
                <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
                  <span className="break-all font-medium text-fg">{e.displayName}</span>
                  <Badge tone={e.kind === "theme" ? "accent" : "neutral"}>
                    {e.kind}
                  </Badge>
                  {e.builtin && <span className="text-xs text-fg-faint">built-in</span>}
                  {e.kind !== "theme" && !e.toolchainAvailable && (
                    <span className="text-xs text-warn">toolchain missing</span>
                  )}
                </div>
              </div>
              <div className="flex flex-wrap items-center gap-3 text-xs">
                {!e.builtin && (
                  <label className="flex items-center gap-1 text-fg-muted">
                    <input type="checkbox" checked={e.trusted} onChange={(c) => void approve(e.id, c.target.checked)} />
                    approve
                  </label>
                )}
              </div>
            </div>
            {(e.fileTypes.length > 0 || e.templates.length > 0) && (
              <div className="mt-1 whitespace-normal break-words text-xs text-fg-faint">
                {e.fileTypes.map((f) => f.ext).join(" ")} {e.templates.map((t) => t.id).join(", ")}
              </div>
            )}
            {(e.themes ?? []).filter(isThemeSpec).length > 0 && (
              <div className="mt-2 flex flex-wrap gap-2">
                {(e.themes ?? []).filter(isThemeSpec).map((t) => {
                  const active = selection.mode === "theme" && selection.id === t.id;
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
    </div>
  );
}
