import { useState } from "react";
import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  getTrace,
  listTraces,
  type InstanceTrace,
  type TraceOutcome,
  type TraceSummary,
} from "../gen";
import { useLiveInvalidation } from "../lib/useLiveInvalidation";
import { TraceTimeline, fmtClock, fmtDuration } from "../components/TraceTimeline";
import { Badge, ErrorText } from "../components/ui";

function outcomeTone(outcome: TraceOutcome): "info" | "ok" | "neutral" {
  switch (outcome) {
    case "active":
      return "info";
    case "completed":
      return "ok";
    case "terminated":
      return "neutral";
  }
}

export default function Traces() {
  const [selected, setSelected] = useState<string | null>(null);

  useLiveInvalidation(["traces"]);
  const { data, isLoading, error } = useQuery({
    queryKey: ["traces"],
    queryFn: async () =>
      (await listTraces({ query: { limit: 200 }, throwOnError: true })).data,
  });

  return (
    <div className="flex h-full">
      <div className="flex w-[28rem] shrink-0 flex-col border-r border-edge">
        <header className="border-b border-edge px-5 py-4">
          <h1 className="text-xl font-semibold text-fg">Execution traces</h1>
          <p className="text-xs text-fg-faint">
            {data ? `${data.length} trace(s)` : "Live view"} · folded from the
            engine event stream
          </p>
        </header>
        <div className="min-h-0 flex-1 overflow-auto">
          {isLoading && <p className="p-5 text-fg-muted">Loading…</p>}
          {error && (
            <div className="p-5">
              <ErrorText>Failed to load: {String(error)}</ErrorText>
            </div>
          )}
          {data && data.length === 0 && (
            <p className="p-5 text-fg-faint">
              No traces yet. Create and run a process instance.
            </p>
          )}
          <ul>
            {data?.map((t: TraceSummary) => (
              <li key={t.instanceKey}>
                <button
                  onClick={() => setSelected(t.instanceKey)}
                  className={`flex w-full flex-col gap-1 border-b border-edge px-5 py-3 text-left hover:bg-hover ${
                    selected === t.instanceKey ? "bg-accent/10" : ""
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <span className="font-medium text-fg">{t.processId}</span>
                    <Badge tone={outcomeTone(t.outcome)}>{t.outcome}</Badge>
                  </div>
                  <div className="flex items-center justify-between font-mono text-xs text-fg-faint">
                    <span>{t.instanceKey}</span>
                    <span>
                      {t.elementCount} el · {fmtDuration(t.durationMs)}
                      {t.incidentCount > 0 && (
                        <span className="ml-1 text-danger">
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
          <div className="flex h-full items-center justify-center text-sm text-fg-faint">
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
    queryFn: async () =>
      (await getTrace({ path: { key: traceKey }, throwOnError: true })).data,
  });

  if (isLoading) return <p className="p-8 text-fg-muted">Loading…</p>;
  if (error)
    return (
      <div className="p-8">
        <ErrorText>Failed to load: {String(error)}</ErrorText>
      </div>
    );
  if (!data) return null;

  return (
    <div className="flex h-full flex-col">
      <header className="border-b border-edge px-8 py-4">
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-semibold text-fg">{data.processId}</h1>
          <Badge tone={outcomeTone(data.outcome)}>{data.outcome}</Badge>
          {data.incidents.length > 0 && (
            <Badge tone="danger">{data.incidents.length} incident(s)</Badge>
          )}
        </div>
        <div className="mt-1 font-mono text-xs text-fg-faint">
          instance {data.instanceKey}
          {data.version != null && ` · v${data.version}`}
          {data.businessId && ` · ${data.businessId}`} · started{" "}
          {fmtClock(data.startedAt)} · {fmtDuration(data.durationMs)}
        </div>
        {data.path.length > 0 && (
          <div className="mt-2 flex flex-wrap items-center gap-1 text-xs text-fg-muted">
            {data.path.map((el, i) => (
              <span key={`${el}-${i}`} className="flex items-center gap-1">
                {i > 0 && <span className="text-fg-faint">→</span>}
                <span className="rounded bg-hover px-1.5 py-0.5 font-mono">
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
          className="rounded-md border border-edge-strong bg-raised px-3 py-1.5 text-fg shadow-sm transition-colors hover:bg-hover"
        >
          Open OTel (OTLP/JSON) ↗
        </a>
        <a
          href={`/console/api/traces/${traceKey}`}
          target="_blank"
          rel="noreferrer"
          className="rounded-md border border-edge-strong bg-raised px-3 py-1.5 text-fg shadow-sm transition-colors hover:bg-hover"
        >
          Raw trace JSON ↗
        </a>
      </div>
      <pre className="overflow-auto rounded-md border border-edge bg-inset p-4 font-mono text-xs text-fg-muted">
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
          ? "bg-accent/10 font-medium text-accent-strong"
          : "bg-raised text-fg-muted hover:bg-hover"
      }`}
    >
      {children}
    </button>
  );
}
