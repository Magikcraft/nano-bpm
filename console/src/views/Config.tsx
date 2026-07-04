import { useEffect, useMemo, useRef, useState } from "react";
import {
  configApi,
  type ConfigDependency,
  type IdeConfig,
  type LangPackConfig,
  type ServerConfig,
  type SlaModeConfig,
} from "../lib/api";

type Tab = "server" | "ide";

export default function Config() {
  const [tab, setTab] = useState<Tab>("server");
  return (
    <div className="mx-auto max-w-4xl p-6">
      <h1 className="mb-1 text-xl font-semibold text-zinc-100">Configuration</h1>
      <p className="mb-4 text-sm text-zinc-500">
        Server runtime behaviour and IDE toolchains for this node.
      </p>

      <div className="mb-6 inline-flex rounded-lg border border-zinc-800 bg-zinc-900 p-1">
        {(
          [
            ["server", "Server"],
            ["ide", "IDE"],
          ] as const
        ).map(([id, label]) => (
          <button
            key={id}
            onClick={() => setTab(id)}
            className={`rounded-md px-4 py-1.5 text-sm transition-colors ${
              tab === id
                ? "bg-zinc-800 text-white"
                : "text-zinc-400 hover:text-zinc-200"
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {tab === "server" ? <ServerPane /> : <IdePane />}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Server pane — basic (guitar pedal) / advanced (env params)
// ---------------------------------------------------------------------------

function ServerPane() {
  const [cfg, setCfg] = useState<ServerConfig | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [mode, setMode] = useState<"basic" | "advanced">("basic");

  useEffect(() => {
    configApi.server().then(setCfg).catch((e) => setErr(String(e)));
  }, []);

  if (err) return <div className="text-sm text-rose-400">{err}</div>;
  if (!cfg) return <div className="text-sm text-zinc-500">Loading…</div>;

  return (
    <div>
      <div className="mb-4 flex items-center justify-between">
        <div className="text-sm text-zinc-400">
          The SLA mode is switchable live below; other parameters are set on
          startup via the environment.
        </div>
        <div className="inline-flex rounded-md border border-zinc-800 bg-zinc-900 p-0.5 text-xs">
          {(["basic", "advanced"] as const).map((m) => (
            <button
              key={m}
              onClick={() => setMode(m)}
              className={`rounded px-3 py-1 capitalize transition-colors ${
                mode === m ? "bg-zinc-800 text-white" : "text-zinc-400 hover:text-zinc-200"
              }`}
            >
              {m}
            </button>
          ))}
        </div>
      </div>

      {mode === "basic" ? (
        <SlaPedal sla={cfg.slaMode} onApplied={setCfg} />
      ) : (
        <AdvancedParams cfg={cfg} />
      )}
    </div>
  );
}

// The single-knob guitar-effects pedal for the SLA mode. The knob reflects the
// server's current mode. When the server reports `switchable`, turning the knob
// applies the mode live (and the server propagates it cluster-wide); otherwise
// it previews the alternative and shows how to apply it on startup.
function SlaPedal({
  sla,
  onApplied,
}: {
  sla: SlaModeConfig;
  onApplied: (cfg: ServerConfig) => void;
}) {
  const [preview, setPreview] = useState(sla.current);
  const [applying, setApplying] = useState(false);
  const [applyErr, setApplyErr] = useState<string | null>(null);
  const dragging = useRef(false);
  const knobRef = useRef<HTMLDivElement | null>(null);

  // Keep the knob in sync with the server's live mode (e.g. after a
  // cluster-wide switch initiated elsewhere refetches, or on apply).
  useEffect(() => setPreview(sla.current), [sla.current]);

  const idx = Math.max(
    0,
    sla.options.findIndex((o) => o.id === preview),
  );
  const selected = sla.options[idx] ?? sla.options[0];
  // Two detents: left = option 0, right = option 1. Map to a rotation.
  const angle = idx === 0 ? -48 : 48;
  const changed = preview !== sla.current;

  // Move the knob to `mode`. When the server allows live switching, apply it
  // immediately (optimistic knob move, revert on failure); otherwise just
  // preview so the restart hint shows the target env value.
  const select = (mode: string) => {
    if (mode === preview) return;
    setPreview(mode);
    if (!sla.switchable) return;
    setApplyErr(null);
    setApplying(true);
    configApi
      .setSla(mode)
      .then(onApplied)
      .catch((e) => {
        setApplyErr(String(e));
        setPreview(sla.current);
      })
      .finally(() => setApplying(false));
  };

  const setFromPointer = (clientX: number) => {
    const el = knobRef.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    // Left half selects option 0, right half option 1.
    const left = clientX < r.left + r.width / 2;
    select(sla.options[left ? 0 : 1].id);
  };

  return (
    <div className="flex flex-col items-center gap-6 sm:flex-row sm:items-start">
      {/* Pedal chassis */}
      <div
        className="relative w-64 shrink-0 select-none rounded-2xl p-5 shadow-2xl"
        style={{
          background:
            "linear-gradient(160deg,#3f3f46 0%,#27272a 45%,#18181b 100%)",
          boxShadow:
            "0 10px 30px rgba(0,0,0,.6), inset 0 1px 0 rgba(255,255,255,.08)",
          border: "1px solid #000",
        }}
      >
        {/* corner screws */}
        {[
          "left-2 top-2",
          "right-2 top-2",
          "left-2 bottom-2",
          "right-2 bottom-2",
        ].map((pos) => (
          <div
            key={pos}
            className={`absolute ${pos} h-3 w-3 rounded-full`}
            style={{
              background: "radial-gradient(circle at 35% 35%,#a1a1aa,#52525b)",
              boxShadow: "inset 0 0 0 1px rgba(0,0,0,.6)",
            }}
          >
            <div className="absolute left-1/2 top-1/2 h-[1px] w-2 -translate-x-1/2 -translate-y-1/2 rotate-45 bg-black/60" />
          </div>
        ))}

        <div className="mb-1 text-center text-[10px] font-bold uppercase tracking-[0.25em] text-violet-300/80">
          nano SLA
        </div>
        <div className="mb-4 flex items-center justify-center gap-2">
          <span
            className="inline-block h-2 w-2 rounded-full"
            style={{
              background: idx === 0 ? "#f59e0b" : "#22d3ee",
              boxShadow: `0 0 8px ${idx === 0 ? "#f59e0b" : "#22d3ee"}`,
            }}
          />
          <span className="text-[9px] uppercase tracking-widest text-zinc-500">
            {idx === 0 ? "latency" : "admission"}
          </span>
        </div>

        {/* Knob */}
        <div className="flex justify-center py-2">
          <div
            ref={knobRef}
            role="slider"
            aria-label="SLA mode"
            aria-valuetext={selected.label}
            tabIndex={0}
            onPointerDown={(e) => {
              dragging.current = true;
              (e.target as HTMLElement).setPointerCapture?.(e.pointerId);
              setFromPointer(e.clientX);
            }}
            onPointerMove={(e) => {
              if (dragging.current) setFromPointer(e.clientX);
            }}
            onPointerUp={() => (dragging.current = false)}
            onKeyDown={(e) => {
              if (e.key === "ArrowLeft") select(sla.options[0].id);
              if (e.key === "ArrowRight") select(sla.options[1].id);
            }}
            className="relative h-28 w-28 cursor-pointer rounded-full"
            style={{
              background:
                "radial-gradient(circle at 50% 30%,#52525b 0%,#27272a 60%,#09090b 100%)",
              boxShadow:
                "0 6px 14px rgba(0,0,0,.7), inset 0 2px 4px rgba(255,255,255,.12), inset 0 -3px 6px rgba(0,0,0,.6)",
            }}
          >
            {/* knurled ring */}
            <div className="absolute inset-2 rounded-full border border-black/40" />
            {/* pointer that rotates with the value — the wrapper fills the
                knob (inset-0) so it pivots around the knob's exact centre */}
            <div
              className="absolute inset-0 transition-transform duration-300"
              style={{ transform: `rotate(${angle}deg)`, transformOrigin: "50% 50%" }}
            >
              <div
                className="absolute left-1/2 top-2 h-4 w-1 -translate-x-1/2 rounded-full"
                style={{
                  background: idx === 0 ? "#f59e0b" : "#22d3ee",
                  boxShadow: `0 0 6px ${idx === 0 ? "#f59e0b" : "#22d3ee"}`,
                }}
              />
            </div>
          </div>
        </div>

        {/* detent labels */}
        <div className="mt-2 flex justify-between text-[9px] uppercase tracking-wider text-zinc-500">
          <button className="hover:text-amber-300" onClick={() => select(sla.options[0].id)}>
            ◄ reject
          </button>
          <button className="hover:text-cyan-300" onClick={() => select(sla.options[1].id)}>
            accept ►
          </button>
        </div>
      </div>

      {/* Explanation + apply hint */}
      <div className="flex-1">
        <div className="mb-1 text-sm font-semibold text-zinc-100">{selected.label}</div>
        <div className="mb-3 text-xs uppercase tracking-wider text-violet-300/80">
          {selected.tagline}
        </div>
        <p className="mb-4 text-sm leading-relaxed text-zinc-400">{selected.description}</p>

        <div className="rounded-lg border border-zinc-800 bg-zinc-900/60 p-3 text-xs text-zinc-400">
          <div className="mb-1">
            Current:{" "}
            <span className="font-mono text-zinc-200">{sla.current}</span>{" "}
            <span className="text-zinc-600">
              (source: {sla.source === "default" ? "default" : "environment"})
            </span>
          </div>
          {sla.switchable ? (
            applyErr ? (
              <div className="text-rose-400">Switch failed: {applyErr}</div>
            ) : applying ? (
              <div className="text-cyan-400">Applying {preview} across the cluster…</div>
            ) : (
              <div className="text-emerald-400">
                Live. Turning the knob switches the mode immediately and propagates
                it cluster-wide. On restart it reseeds from{" "}
                <code className="rounded bg-black/40 px-1 py-0.5 font-mono text-emerald-300">
                  NANOBPMN_SLA_MODE
                </code>
                .
              </div>
            )
          ) : changed ? (
            <div className="text-amber-400">
              Preview only. To apply, restart with{" "}
              <code className="rounded bg-black/40 px-1 py-0.5 font-mono text-amber-300">
                NANOBPMN_SLA_MODE={preview}
              </code>
              .
            </div>
          ) : (
            <div className="text-zinc-500">
              Turn the knob to preview the other mode; the mode is set on startup.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function AdvancedParams({ cfg }: { cfg: ServerConfig }) {
  const groups = useMemo(() => {
    const m = new Map<string, typeof cfg.params>();
    for (const p of cfg.params) {
      if (!m.has(p.category)) m.set(p.category, []);
      m.get(p.category)!.push(p);
    }
    return [...m.entries()];
  }, [cfg]);

  return (
    <div className="space-y-6">
      {groups.map(([cat, params]) => (
        <section key={cat}>
          <h2 className="mb-2 text-xs font-semibold uppercase tracking-wider text-zinc-500">
            {cat}
          </h2>
          <div className="overflow-hidden rounded-lg border border-zinc-800">
            <table className="w-full text-sm">
              <tbody>
                {params.map((p, i) => (
                  <tr
                    key={p.key}
                    className={i % 2 ? "bg-zinc-900/40" : "bg-zinc-900/10"}
                  >
                    <td className="w-1/3 border-b border-zinc-800/60 px-3 py-2 align-top">
                      <div className="text-zinc-200">{p.label}</div>
                      <div className="font-mono text-[11px] text-zinc-600">{p.key}</div>
                    </td>
                    <td className="border-b border-zinc-800/60 px-3 py-2 align-top">
                      <div className="mb-0.5 flex items-center gap-2">
                        {p.value !== null ? (
                          <span className="rounded bg-emerald-500/10 px-1.5 py-0.5 font-mono text-xs text-emerald-300">
                            {p.value}
                          </span>
                        ) : (
                          <span className="font-mono text-xs text-zinc-500">
                            default: {p.default}
                          </span>
                        )}
                      </div>
                      <div className="text-xs text-zinc-500">{p.description}</div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// IDE pane — toolchain dependencies + language packs
// ---------------------------------------------------------------------------

function IdePane() {
  const [cfg, setCfg] = useState<IdeConfig | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    configApi.ide().then(setCfg).catch((e) => setErr(String(e)));
  }, []);

  if (err) return <div className="text-sm text-rose-400">{err}</div>;
  if (!cfg) return <div className="text-sm text-zinc-500">Loading…</div>;

  const missing = cfg.dependencies.filter((d) => !d.present);

  return (
    <div className="space-y-6">
      {missing.length > 0 && (
        <section>
          <h2 className="mb-2 text-xs font-semibold uppercase tracking-wider text-zinc-500">
            Missing dependencies
          </h2>
          <div className="space-y-2">
            {missing.map((d) => (
              <DepCard key={d.id} d={d} />
            ))}
          </div>
        </section>
      )}

      <section>
        <h2 className="mb-2 text-xs font-semibold uppercase tracking-wider text-zinc-500">
          Toolchains
        </h2>
        <div className="space-y-2">
          {cfg.dependencies.map((d) => (
            <ToolchainRow key={d.id} d={d} />
          ))}
        </div>
      </section>

      <section>
        <h2 className="mb-2 text-xs font-semibold uppercase tracking-wider text-zinc-500">
          Language packs
        </h2>
        <div className="space-y-3">
          {cfg.langPacks.map((p) => (
            <LangPackCard key={p.id} p={p} />
          ))}
        </div>
      </section>
    </div>
  );
}

function DepCard({ d }: { d: ConfigDependency }) {
  return (
    <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-3">
      <div className="text-sm">
        <span className="font-semibold text-amber-300">{d.name} is not installed.</span>{" "}
        <span className="text-zinc-400">{d.purpose}</span>
      </div>
      <div className="mt-1 text-xs text-zinc-400">{d.hint}</div>
      <a
        href={d.installUrl}
        target="_blank"
        rel="noopener noreferrer"
        className="mt-2 inline-block text-xs font-medium text-violet-300 hover:text-violet-200"
      >
        Install instructions →
      </a>
    </div>
  );
}

function ToolchainRow({ d }: { d: ConfigDependency }) {
  return (
    <div className="flex items-center justify-between rounded-lg border border-zinc-800 bg-zinc-900/40 px-3 py-2">
      <div className="min-w-0">
        <div className="flex items-center gap-2">
          <span
            className={`inline-block h-2 w-2 rounded-full ${
              d.present ? "bg-emerald-400" : "bg-zinc-600"
            }`}
          />
          <span className="text-sm text-zinc-200">{d.name}</span>
        </div>
        <div className="truncate font-mono text-[11px] text-zinc-500">
          {d.present ? d.version ?? d.bin : d.bin}
        </div>
      </div>
      {d.present ? (
        <span className="rounded bg-emerald-500/10 px-2 py-0.5 text-xs text-emerald-300">
          installed
        </span>
      ) : (
        <a
          href={d.installUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="text-xs font-medium text-violet-300 hover:text-violet-200"
        >
          Install →
        </a>
      )}
    </div>
  );
}

function LangPackCard({ p }: { p: LangPackConfig }) {
  return (
    <div className="rounded-lg border border-zinc-800 bg-zinc-900/40 p-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium text-zinc-100">{p.displayName}</span>
          {p.builtin && (
            <span className="rounded bg-zinc-800 px-1.5 py-0.5 text-[10px] uppercase tracking-wider text-zinc-400">
              built-in
            </span>
          )}
        </div>
        <span
          className={`rounded px-2 py-0.5 text-xs ${
            p.available
              ? "bg-emerald-500/10 text-emerald-300"
              : "bg-amber-500/10 text-amber-300"
          }`}
        >
          {p.available ? "ready" : "toolchain missing"}
        </span>
      </div>

      {p.detect.length > 0 && (
        <div className="mt-1 font-mono text-[11px] text-zinc-600">
          probe: {p.detect.join(" ")}
        </div>
      )}

      {p.configFields.length > 0 && (
        <div className="mt-3 space-y-2 border-t border-zinc-800 pt-2">
          {p.configFields.map((f) => (
            <div key={f.key} className="text-xs">
              <div className="flex items-center gap-2">
                <span className="text-zinc-300">{f.label}</span>
                {f.value !== null ? (
                  <span className="rounded bg-emerald-500/10 px-1.5 py-0.5 font-mono text-emerald-300">
                    {f.value}
                  </span>
                ) : (
                  <span className="font-mono text-zinc-500">
                    default: {f.default ?? "—"}
                  </span>
                )}
                {f.env && (
                  <span className="font-mono text-[10px] text-zinc-600">{f.env}</span>
                )}
              </div>
              {f.description && (
                <div className="text-zinc-500">{f.description}</div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
