import { useMemo, useState } from "react";
import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  api,
  type InstanceTrace,
  type TraceElement,
  type TraceOutcome,
  type TraceSummary,
} from "../lib/api";
import { useLiveInvalidation } from "../lib/useLiveInvalidation";

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

function fmtDuration(ms: number | null): string {
  if (ms == null) return "—";
  if (ms < 1000) return `${ms} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(2)} s`;
  const m = Math.floor(ms / 60_000);
  const s = ((ms % 60_000) / 1000).toFixed(0);
  return `${m}m ${s}s`;
}

function fmtClock(ms: number): string {
  return new Date(ms).toLocaleTimeString(undefined, {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
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
          <Timeline trace={data} />
        ) : (
          <DataView trace={data} traceKey={traceKey} />
        )}
      </div>
    </div>
  );
}

function Timeline({ trace }: { trace: InstanceTrace }) {
  // Time window: instance start → end (or the last observed event for an
  // in-flight instance). All bar positions are percentages of this span.
  const { t0, span } = useMemo(() => {
    const t0 = trace.startedAt;
    let max = trace.endedAt ?? trace.startedAt;
    for (const el of trace.elements) {
      max = Math.max(max, el.enteredAt, el.exitedAt ?? el.enteredAt);
      if (el.job) {
        max = Math.max(
          max,
          el.job.createdAt,
          el.job.activatedAt ?? el.job.createdAt,
          el.job.completedAt ?? el.job.createdAt,
        );
      }
    }
    return { t0, span: Math.max(max - t0, 1) };
  }, [trace]);

  const pct = (t: number) => ((t - t0) / span) * 100;

  return (
    <div>
      <Legend />
      <div className="mt-4 space-y-1.5">
        {trace.elements.map((el) => (
          <ElementRow key={el.elementInstanceKey} el={el} t0={t0} pct={pct} />
        ))}
      </div>
      <div className="mt-2 flex justify-between border-t border-zinc-800 pt-1 font-mono text-[10px] text-zinc-500">
        <span>0 ms</span>
        <span>{fmtDuration(Math.round(span))}</span>
      </div>

      {trace.incidents.length > 0 && (
        <section className="mt-8">
          <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-zinc-500">
            Incidents
          </h2>
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr className="border-b border-zinc-800 text-left text-zinc-500">
                {["Element", "Kind", "Reason", "Raised", "Resolved"].map((h) => (
                  <th key={h} className="py-2 pr-4 font-medium">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {trace.incidents.map((inc, i) => (
                <tr key={i} className="border-b border-zinc-900">
                  <td className="py-2 pr-4 font-mono">{inc.elementId}</td>
                  <td className="py-2 pr-4">{inc.kind}</td>
                  <td className="py-2 pr-4 text-red-300">{inc.reason}</td>
                  <td className="py-2 pr-4 font-mono text-zinc-500">
                    {fmtClock(inc.raisedAt)}
                  </td>
                  <td className="py-2 pr-4 font-mono text-zinc-500">
                    {inc.resolvedAt ? fmtClock(inc.resolvedAt) : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}
    </div>
  );
}

function ElementRow({
  el,
  t0,
  pct,
}: {
  el: TraceElement;
  t0: number;
  pct: (t: number) => number;
}) {
  const job = el.job;
  const start = el.enteredAt;
  const end = el.exitedAt ?? Math.max(el.enteredAt, job?.completedAt ?? el.enteredAt);

  // Element lifetime track (faint). Min width so instantaneous nodes show.
  const left = pct(start);
  const width = Math.max(pct(end) - left, 0.6);

  return (
    <div className="flex items-center gap-3">
      <div className="w-44 shrink-0 truncate font-mono text-xs text-zinc-300" title={el.elementId}>
        {el.elementId}
        {el.incidents > 0 && <span className="ml-1 text-red-400">⚠</span>}
      </div>
      <div className="relative h-6 flex-1 rounded bg-zinc-900">
        {/* Element lifetime */}
        <div
          className="absolute top-1/2 h-3 -translate-y-1/2 rounded-sm bg-zinc-700"
          style={{ left: `${left}%`, width: `${width}%` }}
          title={`${el.elementId} · ${fmtDuration(el.durationMs)}`}
        />
        {job && <JobBars job={job} pct={pct} t0={t0} />}
      </div>
      <div className="w-28 shrink-0 text-right font-mono text-[11px] text-zinc-500">
        {job
          ? job.queueMs != null
            ? `${fmtDuration(job.queueMs)} q`
            : fmtDuration(job.waitMs)
          : fmtDuration(el.durationMs)}
      </div>
    </div>
  );
}

function JobBars({
  job,
  pct,
}: {
  job: NonNullable<TraceElement["job"]>;
  pct: (t: number) => number;
  t0: number;
}) {
  const segments: ReactNode[] = [];
  const created = job.createdAt;
  const activated = job.activatedAt;
  const completed = job.completedAt;

  if (activated != null) {
    // Queue (created → activated), amber.
    const qLeft = pct(created);
    const qWidth = Math.max(pct(activated) - qLeft, 0.4);
    segments.push(
      <div
        key="q"
        className="absolute top-1/2 h-3.5 -translate-y-1/2 rounded-l-sm bg-amber-500/80"
        style={{ left: `${qLeft}%`, width: `${qWidth}%` }}
        title={`queue ${fmtDuration(job.queueMs)} · worker ${job.worker ?? "—"}`}
      />,
    );
    // Service (activated → completed/open), emerald.
    const sEnd = completed ?? activated;
    const sLeft = pct(activated);
    const sWidth = Math.max(pct(sEnd) - sLeft, 0.4);
    segments.push(
      <div
        key="s"
        className="absolute top-1/2 h-3.5 -translate-y-1/2 rounded-r-sm bg-emerald-500/80"
        style={{ left: `${sLeft}%`, width: `${sWidth}%` }}
        title={`service ${fmtDuration(job.serviceMs)} · worker ${job.worker ?? "—"}`}
      />,
    );
  } else {
    // Activation not observed: a single "wait" bar (created → completed/open).
    const end = completed ?? created;
    const left = pct(created);
    const width = Math.max(pct(end) - left, 0.4);
    segments.push(
      <div
        key="w"
        className="absolute top-1/2 h-3.5 -translate-y-1/2 rounded-sm bg-amber-600/70"
        style={{ left: `${left}%`, width: `${width}%` }}
        title={`wait ${fmtDuration(job.waitMs)} (activation not observed)`}
      />,
    );
  }
  return <>{segments}</>;
}

function Legend() {
  return (
    <div className="flex flex-wrap items-center gap-4 text-xs text-zinc-400">
      <Swatch className="bg-zinc-700">element</Swatch>
      <Swatch className="bg-amber-500/80">job queue</Swatch>
      <Swatch className="bg-emerald-500/80">job service</Swatch>
      <Swatch className="bg-amber-600/70">job wait (activation unobserved)</Swatch>
    </div>
  );
}

function Swatch({ className, children }: { className: string; children: ReactNode }) {
  return (
    <span className="flex items-center gap-1.5">
      <span className={`inline-block h-3 w-3 rounded-sm ${className}`} />
      {children}
    </span>
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
