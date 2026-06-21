import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api, nodeConsoleUrl, type MetricsSnapshot } from "../lib/api";

/// How many derived samples to retain for the sparklines (~2 min at 1 Hz).
const MAX_SAMPLES = 120;

/// One derived sample: instantaneous rates computed from the delta between two
/// successive raw snapshots, plus the live gauge readings at that instant.
interface Sample {
  t: number;
  startsPerSec: number;
  jobsPerSec: number;
  active: number;
  connections: number;
  inflight: number;
}

export default function Metrics() {
  const [paused, setPaused] = useState(false);
  const { data, error, isLoading } = useQuery({
    queryKey: ["metrics"],
    queryFn: api.metrics,
    refetchInterval: paused ? false : 1000,
  });

  // Rolling derived history. Kept in a ref (the source of truth) and mirrored
  // into state so the chart re-renders; `prev` holds the last raw snapshot so we
  // can difference counters into rates.
  const prev = useRef<MetricsSnapshot | null>(null);
  const history = useRef<Sample[]>([]);
  const [samples, setSamples] = useState<Sample[]>([]);

  useEffect(() => {
    if (!data) return;
    const p = prev.current;
    prev.current = data;
    if (p && data.timestampMs > p.timestampMs) {
      const dt = (data.timestampMs - p.timestampMs) / 1000;
      const rate = (cur: number, was: number) =>
        dt > 0 ? Math.max(0, (cur - was) / dt) : 0;
      const s: Sample = {
        t: data.timestampMs,
        startsPerSec: rate(data.createsTotal, p.createsTotal),
        jobsPerSec: rate(data.completionsTotal, p.completionsTotal),
        active: data.activeInstances,
        connections: data.connectionsActive,
        inflight: data.commitInflight,
      };
      const next = [...history.current, s].slice(-MAX_SAMPLES);
      history.current = next;
      setSamples(next);
    }
  }, [data]);

  const reset = () => {
    history.current = [];
    prev.current = null;
    setSamples([]);
  };

  // Cluster-wide metrics: per-node breakdown + aggregate. Probes every peer, so
  // a slower cadence than the local 1 Hz poll. Cluster throughput is derived
  // from successive deltas of the aggregate counters.
  const { data: cluster } = useQuery({
    queryKey: ["clusterMetrics"],
    queryFn: api.clusterMetrics,
    refetchInterval: paused ? false : 2000,
  });
  const clusterPrev = useRef<{ t: number; creates: number; completions: number } | null>(null);
  const [clusterRates, setClusterRates] = useState<{ starts: number; jobs: number } | null>(null);
  useEffect(() => {
    if (!cluster) return;
    const agg = cluster.aggregate;
    const p = clusterPrev.current;
    clusterPrev.current = {
      t: cluster.checkedAtMs,
      creates: agg.createsTotal,
      completions: agg.completionsTotal,
    };
    if (p && cluster.checkedAtMs > p.t) {
      const dt = (cluster.checkedAtMs - p.t) / 1000;
      setClusterRates({
        starts: Math.max(0, (agg.createsTotal - p.creates) / dt),
        jobs: Math.max(0, (agg.completionsTotal - p.completions) / dt),
      });
    }
  }, [cluster]);
  const isCluster = (cluster?.aggregate.totalNodes ?? 1) > 1;

  const latest = samples[samples.length - 1];

  return (
    <div className="p-8">
      <header className="mb-6 flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-semibold">Metrics</h1>
          <p className="text-sm text-zinc-500">
            Live throughput &amp; durability, sourced from the Prometheus surface
            {" · "}
            <a className="underline hover:text-zinc-300" href="/metrics">
              /metrics
            </a>
          </p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={() => setPaused((v) => !v)}
            className="rounded-md border border-zinc-700 px-3 py-1.5 text-sm hover:bg-zinc-800"
          >
            {paused ? "Resume" : "Pause"}
          </button>
          <button
            onClick={reset}
            className="rounded-md border border-zinc-700 px-3 py-1.5 text-sm hover:bg-zinc-800"
          >
            Clear
          </button>
        </div>
      </header>

      {isLoading && !data && <p className="text-zinc-400">Loading…</p>}
      {error && (
        <p className="text-red-400">Failed to load metrics: {String(error)}</p>
      )}

      {data && (
        <div className="space-y-8">
          {/* Headline live stats */}
          <section className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-4">
            <Stat
              label="Process starts/s"
              value={fmt(latest?.startsPerSec ?? 0, 0)}
              accent="emerald"
            />
            <Stat
              label="Jobs completed/s"
              value={fmt(latest?.jobsPerSec ?? 0, 0)}
              accent="sky"
            />
            <Stat label="Active processes" value={data.activeInstances.toLocaleString()} />
            <Stat label="Connected clients" value={data.connectionsActive.toLocaleString()} />
          </section>

          {/* Throughput charts */}
          <section className="grid gap-4 lg:grid-cols-2">
            <Chart
              title="Process starts / s"
              color="#34d399"
              values={samples.map((s) => s.startsPerSec)}
            />
            <Chart
              title="Jobs completed / s"
              color="#38bdf8"
              values={samples.map((s) => s.jobsPerSec)}
            />
            <Chart
              title="Active processes"
              color="#a78bfa"
              values={samples.map((s) => s.active)}
            />
            <Chart
              title="Commit pipeline depth (in-flight)"
              color="#fbbf24"
              values={samples.map((s) => s.inflight)}
            />
          </section>

          {/* Totals & durability */}
          <section>
            <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-zinc-500">
              Totals
            </h2>
            <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-4">
              <Stat label="Instances created" value={data.createsTotal.toLocaleString()} sub={`rest ${data.createsRest.toLocaleString()} · stream ${data.createsStream.toLocaleString()}`} />
              <Stat label="Jobs completed" value={data.completionsTotal.toLocaleString()} sub={`rest ${data.completionsRest.toLocaleString()} · stream ${data.completionsStream.toLocaleString()}`} />
              <Stat label="Journal written" value={fmtBytes(data.bytesTotal)} sub={`${data.writesTotal.toLocaleString()} writes`} />
              <Stat label="Group commits" value={data.commitsTotal.toLocaleString()} />
              {data.residentBytes != null && (
                <Stat label="Memory (resident)" value={fmtBytes(data.residentBytes)} />
              )}
            </div>
          </section>

          {/* Cluster breakdown (only when this is a multi-node cluster) */}
          {isCluster && cluster && (
            <section>
              <div className="mb-3 flex items-baseline justify-between">
                <h2 className="text-sm font-medium uppercase tracking-wide text-zinc-500">
                  Cluster
                </h2>
                <span className="text-xs text-zinc-500">
                  {cluster.aggregate.reachableNodes}/{cluster.aggregate.totalNodes} nodes up
                </span>
              </div>
              <div className="mb-4 grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-5">
                <Stat label="Cluster starts/s" value={fmt(clusterRates?.starts ?? 0, 0)} accent="emerald" />
                <Stat label="Cluster jobs/s" value={fmt(clusterRates?.jobs ?? 0, 0)} accent="sky" />
                <Stat label="Active (cluster)" value={cluster.aggregate.activeInstances.toLocaleString()} />
                <Stat label="Clients (cluster)" value={cluster.aggregate.connectionsActive.toLocaleString()} />
                <Stat label="Memory (cluster)" value={fmtBytes(cluster.aggregate.residentBytes)} />
              </div>
              <div className="overflow-x-auto rounded-lg border border-zinc-800">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-zinc-800 text-left text-zinc-500">
                      <th className="px-3 py-2 font-medium">Node</th>
                      <th className="px-3 py-2 font-medium">Status</th>
                      <th className="px-3 py-2 text-right font-medium">Active</th>
                      <th className="px-3 py-2 text-right font-medium">Created</th>
                      <th className="px-3 py-2 text-right font-medium">Completed</th>
                      <th className="px-3 py-2 text-right font-medium">Clients</th>
                      <th className="px-3 py-2 text-right font-medium">In-flight</th>
                      <th className="px-3 py-2 text-right font-medium">Memory</th>
                    </tr>
                  </thead>
                  <tbody>
                    {cluster.nodes.map((n) => {
                      const href = n.isSelf
                        ? null
                        : nodeConsoleUrl(n.address, "/metrics");
                      return (
                      <tr key={n.nodeId} className="border-b border-zinc-900 last:border-0">
                        <td className="px-3 py-2">
                          <div className="flex items-center gap-2">
                            {href ? (
                              <a
                                href={href}
                                target="_blank"
                                rel="noreferrer"
                                title={`Open node ${n.nodeId} console (${n.address})`}
                                className="font-medium text-sky-400 hover:underline"
                              >
                                node {n.nodeId} ↗
                              </a>
                            ) : (
                              <span className="font-medium">node {n.nodeId}</span>
                            )}
                            {n.isSelf && (
                              <span className="rounded bg-emerald-800 px-1.5 py-0.5 text-xs">
                                this
                              </span>
                            )}
                          </div>
                          {n.address && (
                            <div className="mt-0.5 text-xs text-zinc-500">
                              {n.address}
                            </div>
                          )}
                        </td>
                        <td className="px-3 py-2">
                          {n.reachable ? (
                            <span className="text-emerald-400">● up</span>
                          ) : (
                            <span className="text-red-400" title={n.error ?? ""}>
                              ● {n.error ?? "down"}
                            </span>
                          )}
                        </td>
                        <td className="px-3 py-2 text-right tabular-nums">{n.metrics?.activeInstances.toLocaleString() ?? "—"}</td>
                        <td className="px-3 py-2 text-right tabular-nums">{n.metrics?.createsTotal.toLocaleString() ?? "—"}</td>
                        <td className="px-3 py-2 text-right tabular-nums">{n.metrics?.completionsTotal.toLocaleString() ?? "—"}</td>
                        <td className="px-3 py-2 text-right tabular-nums">{n.metrics?.connectionsActive.toLocaleString() ?? "—"}</td>
                        <td className="px-3 py-2 text-right tabular-nums">{n.metrics?.commitInflight.toLocaleString() ?? "—"}</td>
                        <td className="px-3 py-2 text-right tabular-nums">{n.metrics?.residentBytes != null ? fmtBytes(n.metrics.residentBytes) : "—"}</td>
                      </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </section>
          )}

          <section>
            <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-zinc-500">
              Durability &amp; latency (means)
            </h2>
            <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-4">
              <Stat label="fsync mean" value={`${fmt(data.fsyncMeanMs, 2)} ms`} />
              <Stat label="Commit wait mean" value={`${fmt(data.commitWaitMeanMs, 2)} ms`} />
              <Stat label="Commit batch mean" value={fmt(data.commitBatchMean, 1)} />
              <Stat label="Frame proc mean" value={`${fmt(data.frameProcessingMeanMs, 3)} ms`} />
              <Stat label="Writer busy" value={`${fmt(data.writerBusyRatio * 100, 1)} %`} sub={data.writerBusyRatio > 0.9 ? "writer saturated" : undefined} />
              <Stat label="Credit stalls" value={data.creditStallsTotal.toLocaleString()} />
            </div>
          </section>
        </div>
      )}
    </div>
  );
}

function fmt(n: number, digits: number): string {
  return n.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(1)} ${units[i]}`;
}

const ACCENTS: Record<string, string> = {
  emerald: "text-emerald-400",
  sky: "text-sky-400",
};

function Stat({
  label,
  value,
  sub,
  accent,
}: {
  label: string;
  value: React.ReactNode;
  sub?: string;
  accent?: string;
}) {
  return (
    <div className="rounded-lg border border-zinc-800 bg-zinc-900 px-4 py-3">
      <div className="text-xs uppercase tracking-wide text-zinc-500">{label}</div>
      <div className={`mt-1 text-2xl font-semibold tabular-nums ${accent ? ACCENTS[accent] : ""}`}>
        {value}
      </div>
      {sub && <div className="mt-0.5 text-xs text-zinc-500">{sub}</div>}
    </div>
  );
}

/// A self-contained inline-SVG sparkline (no chart library — keeps the bundle
/// lean and the distribution fully offline). Scales the series to its own
/// min/max and draws a filled area + line.
function Chart({
  title,
  values,
  color,
}: {
  title: string;
  values: number[];
  color: string;
}) {
  const W = 600;
  const H = 120;
  const pad = 4;
  const last = values[values.length - 1] ?? 0;

  let body: React.ReactNode = (
    <text x={W / 2} y={H / 2} fill="#52525b" fontSize="13" textAnchor="middle">
      collecting…
    </text>
  );

  if (values.length >= 2) {
    const max = Math.max(...values, 1);
    const min = Math.min(...values, 0);
    const span = max - min || 1;
    const n = values.length;
    const x = (i: number) => pad + (i / (n - 1)) * (W - 2 * pad);
    const y = (v: number) => H - pad - ((v - min) / span) * (H - 2 * pad);
    const line = values.map((v, i) => `${x(i)},${y(v)}`).join(" ");
    const area = `${pad},${H - pad} ${line} ${W - pad},${H - pad}`;
    body = (
      <>
        <polygon points={area} fill={color} opacity={0.12} />
        <polyline
          points={line}
          fill="none"
          stroke={color}
          strokeWidth={1.5}
          strokeLinejoin="round"
          strokeLinecap="round"
        />
      </>
    );
  }

  return (
    <div className="rounded-lg border border-zinc-800 bg-zinc-900 p-4">
      <div className="mb-2 flex items-baseline justify-between">
        <span className="text-sm text-zinc-400">{title}</span>
        <span className="text-sm font-semibold tabular-nums" style={{ color }}>
          {fmt(last, last < 10 ? 1 : 0)}
        </span>
      </div>
      <svg
        viewBox={`0 0 ${W} ${H}`}
        preserveAspectRatio="none"
        className="h-28 w-full"
      >
        {body}
      </svg>
    </div>
  );
}
