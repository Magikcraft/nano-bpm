import { useEffect, useMemo, useState } from "react";
import { projectsApi, type ExtensionsOverview, type MarketEntry } from "../lib/api";

const CATEGORIES = [
  { id: "lang", label: "Languages" },
  { id: "app", label: "App templates" },
  { id: "example", label: "Example apps" },
] as const;

// Extension marketplace (ADR 0007): browse built-in + installed packs, search the
// npm marketplace (keyword nano-ide-ext) by language/app/example, install packs,
// and manage toolchain trust.
export default function Extensions() {
  const [ov, setOv] = useState<ExtensionsOverview | null>(null);
  const [market, setMarket] = useState<MarketEntry[] | null>(null);
  const [marketErr, setMarketErr] = useState<string | null>(null);
  const [q, setQ] = useState("");
  const [busy, setBusy] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const load = async () => setOv(await projectsApi.extensions());
  const loadMarket = async () => {
    setMarketErr(null);
    try { setMarket((await projectsApi.marketplace()).entries); }
    catch (e) { setMarketErr(String(e)); }
  };
  useEffect(() => { void load(); void loadMarket(); }, []);

  const install = async (pkg: string) => {
    setBusy(pkg); setErr(null);
    try { await projectsApi.installExtension(pkg); await load(); await loadMarket(); }
    catch (e) { setErr(String(e)); }
    finally { setBusy(null); }
  };
  const toggleYolo = async () => setOv(await projectsApi.trustExtension({ yolo: !ov?.yolo }));
  const approve = async (id: string, on: boolean) =>
    setOv(await projectsApi.trustExtension(on ? { approve: id } : { revoke: id }));
  const remove = async (id: string) => { await projectsApi.removeExtension(id); await load(); await loadMarket(); };

  const filtered = useMemo(() => {
    const t = q.trim().toLowerCase();
    return (market ?? []).filter((m) => !t || `${m.name} ${m.description}`.toLowerCase().includes(t));
  }, [market, q]);

  return (
    <div className="mx-auto max-w-4xl p-6">
      <h1 className="mb-1 text-xl font-semibold text-zinc-100">Extensions</h1>
      <p className="mb-4 text-sm text-zinc-500">
        Language, app-template, and example-app packs drive editor grammars, project
        scaffolds, and the run/compile toolchain. Toolchain commands run on your machine.
      </p>

      <input
        value={q}
        onChange={(e) => setQ(e.target.value)}
        placeholder="Search the marketplace…"
        className="mb-3 w-full rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-violet-500"
      />
      {err && <div className="mb-3 text-sm text-rose-400">{err}</div>}
      <label className="mb-4 flex items-center gap-2 text-sm text-zinc-400">
        <input type="checkbox" checked={!!ov?.yolo} onChange={() => void toggleYolo()} />
        Yolo mode — run any extension toolchain without prompting
      </label>

      {marketErr && (
        <div className="mb-4 text-sm text-amber-500">
          Marketplace unavailable (offline?). Installed + built-in packs still shown below.
        </div>
      )}
      {market === null && !marketErr && <div className="mb-4 text-sm text-zinc-500">Loading marketplace…</div>}

      {CATEGORIES.map((cat) => {
        const items = filtered.filter((m) => m.category === cat.id);
        if (items.length === 0) return null;
        return (
          <section key={cat.id} className="mb-5">
            <h2 className="mb-2 text-sm font-semibold uppercase tracking-wide text-zinc-400">{cat.label}</h2>
            <div className="grid gap-2">
              {items.map((m) => (
                <div key={m.name} className="flex items-center justify-between rounded-lg border border-zinc-800 bg-zinc-900 p-3">
                  <div className="min-w-0">
                    <span className="font-medium text-zinc-100">{m.name}</span>
                    <span className="ml-2 text-xs text-zinc-600">{m.version}</span>
                    <div className="truncate text-xs text-zinc-500">{m.description}</div>
                  </div>
                  {m.installed ? (
                    <span className="ml-3 shrink-0 text-xs text-emerald-500">installed</span>
                  ) : (
                    <button onClick={() => void install(m.name)} disabled={busy === m.name}
                      className="ml-3 shrink-0 rounded-md bg-violet-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-violet-500 disabled:opacity-50">
                      {busy === m.name ? "Installing…" : "Install"}
                    </button>
                  )}
                </div>
              ))}
            </div>
          </section>
        );
      })}

      <h2 className="mb-2 mt-6 text-sm font-semibold uppercase tracking-wide text-zinc-400">Installed</h2>
      <div className="grid gap-3">
        {ov?.extensions.map((e) => (
          <div key={e.id} className="rounded-lg border border-zinc-800 bg-zinc-900 p-3">
            <div className="flex items-center justify-between">
              <div>
                <span className="font-medium text-zinc-100">{e.displayName}</span>
                <span className="ml-2 rounded bg-zinc-800 px-1.5 py-0.5 text-xs text-zinc-400">{e.kind}</span>
                {e.builtin && <span className="ml-2 text-xs text-zinc-600">built-in</span>}
                {!e.toolchainAvailable && <span className="ml-2 text-xs text-amber-500">toolchain missing</span>}
              </div>
              <div className="flex items-center gap-3 text-xs">
                {!e.builtin && (
                  <label className="flex items-center gap-1 text-zinc-400">
                    <input type="checkbox" checked={e.trusted} onChange={(c) => void approve(e.id, c.target.checked)} />
                    approve
                  </label>
                )}
                {!e.builtin && <button onClick={() => void remove(e.id)} className="text-rose-400 hover:underline">remove</button>}
              </div>
            </div>
            {(e.fileTypes.length > 0 || e.templates.length > 0) && (
              <div className="mt-1 text-xs text-zinc-500">
                {e.fileTypes.map((f) => f.ext).join(" ")} {e.templates.map((t) => t.id).join(", ")}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
