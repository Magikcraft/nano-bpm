import { useState } from "react";
import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  api,
  type InstanceTrace,
  type TraceOutcome,
  type TraceSummary,
} from "../lib/api";
import { useLiveInvalidation } from "../lib/useLiveInvalidation";
import { TraceTimeline, fmtClock, fmtDuration } from "../components/TraceTimeline";

function outcomeBadge(outcome: TraceOutcome): string {
  switch (outcome) {
    case "active":
      return "bg-sky-900/60 text-sky-300";
    case "completed":
      return "bg-emerald-900/60 text-emerald-300";
    case "terminated":
      return "bg-zinc-700 text-zinc-300";
  }
}

export default function Traces() {
  const [selected, setSelected] = useState<string | null>(null);

  useLiveInvalidation(["traces"]);
  const { data, isLoading, error } = useQuery({
    queryKey: ["traces"],
    queryFn: () => api.traces(200),
  });

  return (
    <div className="flex h-full">
      <div className="flex w-[28rem] shrink-0 flex-col border-r border-zinc-800">
        <header className="border-b border-zinc-800 px-5 py-4">
          <h1 className="text-xl font-semibold">Execution traces</h1>
          <p className="text-xs text-zinc-500">
            {data ? `${data.length} trace(s)` : "Live view"} · folded from the
            engine event stream
          </p>
        </header>
        <div className="min-h-0 flex-1 overflow-auto">
          {isLoading && <p className="p-5 text-zinc-400">Loading…</p>}
          {error && (
            <p className="p-5 text-red-400">Failed to load: {String(error)}</p>
          )}
          {data && data.length === 0 && (
            <p className="p-5 text-zinc-500">
              No traces yet. Create and run a process instance.
            </p>
          )}
          <ul>
            {data?.map((t: TraceSummary) => (
              <li key={t.instanceKey}>
                <button
                  onClick={() => setSelected(t.instanceKey)}
                  className={`flex w-full flex-col gap-1 border-b border-zinc-900 px-5 py-3 text-left hover:bg-zinc-900 ${
                    selected === t.instanceKey ? "bg-zinc-900" : ""
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <span className="font-medium">{t.processId}</span>
                    <span
                      className={`rounded px-1.5 py-0.5 text-xs ${outcomeBadge(
                        t.outcome,
                      )}`}
                    >
                      {t.outcome}
                    </span>
                  </div>
                  <div className="flex items-center justify-between font-mono text-xs text-zinc-500">
                    <span>{t.instanceKey}</span>
                    <span>
                      {t.elementCount} el · {fmtDuration(t.durationMs)}
                      {t.incidentCount > 0 && (
                        <span className="ml-1 text-red-400">
                          · {t.incidentCount}⚠
                        </span>
                      )}
                    </span>
                  </div>
                </button>
              </li>
            ))}
          </ul>
        </div>
      </div>

      <div className="min-w-0 flex-1 overflow-auto">
        {selected ? (
          <TraceDetail traceKey={selected} />
        ) : (
          <div className="flex h-full items-center justify-center text-sm text-zinc-500">
            Select a trace to visualise it.
          </div>
        )}
      </div>
    </div>
  );
}

function TraceDetail({ traceKey }: { traceKey: string }) {
  const [tab, setTab] = useState<"timeline" | "data">("timeline");
  useLiveInvalidation(["trace"]);
  const { data, isLoading, error } = useQuery({
    queryKey: ["trace", traceKey],
    queryFn: () => api.trace(traceKey),
  });

  if (isLoading) return <p className="p-8 text-zinc-400">Loading…</p>;
  if (error)
    return <p className="p-8 text-red-400">Failed to load: {String(error)}</p>;
  if (!data) return null;

  return (
    <div className="flex h-full flex-col">
      <header className="border-b border-zinc-800 px-8 py-4">
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-semibold">{data.processId}</h1>
          <span className={`rounded px-2 py-0.5 text-xs ${outcomeBadge(data.outcome)}`}>
            {data.outcome}
          </span>
          {data.incidents.length > 0 && (
            <span className="rounded bg-red-900/60 px-2 py-0.5 text-xs text-red-300">
              {data.incidents.length} incident(s)
            </span>
          )}
        </div>
        <div className="mt-1 font-mono text-xs text-zinc-500">
          instance {data.instanceKey}
          {data.version != null && ` · v${data.version}`}
          {data.businessId && ` · ${data.businessId}`} · started{" "}
          {fmtClock(data.startedAt)} · {fmtDuration(data.durationMs)}
        </div>
        {data.path.length > 0 && (
          <div className="mt-2 flex flex-wrap items-center gap-1 text-xs text-zinc-400">
            {data.path.map((el, i) => (
              <span key={`${el}-${i}`} className="flex items-center gap-1">
                {i > 0 && <span className="text-zinc-600">→</span>}
                <span className="rounded bg-zinc-800 px-1.5 py-0.5 font-mono">
                  {el}
                </span>
              </span>
            ))}
          </div>
        )}
        <div className="mt-3 flex gap-1">
          <TabButton active={tab === "timeline"} onClick={() => setTab("timeline")}>
            Timeline
          </TabButton>
          <TabButton active={tab === "data"} onClick={() => setTab("data")}>
            Data
          </TabButton>
        </div>
      </header>

      <div className="min-h-0 flex-1 overflow-auto p-8">
        {tab === "timeline" ? (
          <TraceTimeline trace={data} />
        ) : (
          <DataView trace={data} traceKey={traceKey} />
        )}
      </div>
    </div>
  );
}

function DataView({ trace, traceKey }: { trace: InstanceTrace; traceKey: string }) {
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3 text-sm">
        <a
          href={`/console/api/traces/${traceKey}/otel`}
          target="_blank"
          rel="noreferrer"
          className="rounded bg-zinc-800 px-3 py-1.5 text-zinc-200 hover:bg-zinc-700"
        >
          Open OTel (OTLP/JSON) ↗
        </a>
        <a
          href={`/console/api/traces/${traceKey}`}
          target="_blank"
          rel="noreferrer"
          className="rounded bg-zinc-800 px-3 py-1.5 text-zinc-200 hover:bg-zinc-700"
        >
          Raw trace JSON ↗
        </a>
      </div>
      <pre className="overflow-auto rounded-md border border-zinc-800 bg-zinc-950 p-4 font-mono text-xs text-zinc-300">
        {JSON.stringify(trace, null, 2)}
      </pre>
    </div>
  );
}

function TabButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      className={`rounded px-3 py-1 text-xs transition-colors ${
        active
          ? "bg-zinc-700 text-white"
          : "bg-zinc-900 text-zinc-400 hover:bg-zinc-800"
      }`}
    >
      {children}
    </button>
  );
}
