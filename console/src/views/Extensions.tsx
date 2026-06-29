import { useEffect, useState } from "react";
import { projectsApi, type ExtensionsOverview } from "../lib/api";

// Extension marketplace (ADR 0007): browse built-in + installed lang/app packs,
// install nano-ide-ext-* packages from npm, and manage toolchain trust.
export default function Extensions() {
  const [ov, setOv] = useState<ExtensionsOverview | null>(null);
  const [pkg, setPkg] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const load = async () => setOv(await projectsApi.extensions());
  useEffect(() => { void load(); }, []);

  const install = async () => {
    if (!pkg.trim()) return;
    setBusy(true); setErr(null);
    try { await projectsApi.installExtension(pkg.trim()); setPkg(""); await load(); }
    catch (e) { setErr(String(e)); }
    finally { setBusy(false); }
  };
  const toggleYolo = async () => setOv(await projectsApi.trustExtension({ yolo: !ov?.yolo }));
  const approve = async (id: string, on: boolean) =>
    setOv(await projectsApi.trustExtension(on ? { approve: id } : { revoke: id }));
  const remove = async (id: string) => { await projectsApi.removeExtension(id); await load(); };

  return (
    <div className="mx-auto max-w-4xl p-6">
      <h1 className="mb-1 text-xl font-semibold text-zinc-100">Extensions</h1>
      <p className="mb-4 text-sm text-zinc-500">
        Language and app packs drive editor grammars, project templates, and the
        run/compile toolchain. Toolchain commands run on your machine.
      </p>
      <div className="mb-4 flex gap-2">
        <input
          value={pkg}
          onChange={(e) => setPkg(e.target.value)}
          placeholder="npm package, e.g. nano-ide-lang-go"
          className="flex-1 rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-violet-500"
          onKeyDown={(e) => e.key === "Enter" && void install()}
        />
        <button onClick={() => void install()} disabled={busy}
          className="rounded-md bg-violet-600 px-4 py-2 text-sm font-medium text-white hover:bg-violet-500 disabled:opacity-50">
          {busy ? "Installing…" : "Install"}
        </button>
      </div>
      {err && <div className="mb-3 text-sm text-rose-400">{err}</div>}
      <label className="mb-4 flex items-center gap-2 text-sm text-zinc-400">
        <input type="checkbox" checked={!!ov?.yolo} onChange={() => void toggleYolo()} />
        Yolo mode — run any extension toolchain without prompting
      </label>
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
