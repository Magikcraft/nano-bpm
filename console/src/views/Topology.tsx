import { useQuery } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { api, nodeConsoleUrl, type NodeHealth } from "../lib/api";

export default function Topology() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["topology"],
    queryFn: api.topology,
    refetchInterval: 2000,
  });

  // Live per-node liveness, probed independently (slower cadence: each refresh
  // probes every peer over the network).
  const { data: health } = useQuery({
    queryKey: ["clusterHealth"],
    queryFn: api.clusterHealth,
    refetchInterval: 5000,
  });

  const healthOf = (nodeId: number): NodeHealth | undefined =>
    health?.nodes.find((n) => n.nodeId === nodeId);

  return (
    <div className="p-8">
      <header className="mb-6">
        <h1 className="text-2xl font-semibold">Cluster topology</h1>
        <p className="text-sm text-zinc-500">
          Live view of nodes and partition placement
          {data ? ` · gateway v${data.gateway_version}` : ""}
        </p>
      </header>

      {isLoading && <p className="text-zinc-400">Loading…</p>}
      {error && (
        <p className="text-red-400">Failed to load topology: {String(error)}</p>
      )}

      {data && (
        <div className="space-y-8">
          <section className="grid grid-cols-2 gap-4 sm:grid-cols-4">
            <Stat label="Nodes" value={data.num_nodes} />
            <Stat label="Partitions" value={data.num_partitions} />
            <Stat label="Replication factor" value={data.replication_factor} />
            <Stat
              label="Raft"
              value={data.raft_enabled ? "enabled" : "off"}
            />
          </section>

          <section>
            <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-zinc-500">
              Nodes
            </h2>
            <div className="flex flex-wrap gap-3">
              {data.nodes.map((n) => {
                const h = healthOf(n.node_id);
                const down = h && !h.reachable && !n.is_self;
                // Other nodes link to the equivalent page on their own IP; the
                // self node (empty address) is the current page, so not a link.
                const href = n.is_self
                  ? null
                  : nodeConsoleUrl(n.address, "/topology");
                const cls = `block min-w-[12rem] rounded-lg border px-4 py-3 ${
                  down
                    ? "border-red-800 bg-red-950/30"
                    : n.is_self
                      ? "border-emerald-700 bg-emerald-950/40"
                      : "border-zinc-800 bg-zinc-900"
                }${
                  href
                    ? " cursor-pointer transition-colors hover:border-sky-600 hover:bg-zinc-800"
                    : ""
                }`;
                const inner = (
                  <>
                    <div className="flex items-center gap-2">
                      <span
                        className={`inline-block h-2 w-2 shrink-0 rounded-full ${
                          h
                            ? h.reachable
                              ? "bg-emerald-400"
                              : "bg-red-500"
                            : "bg-zinc-600"
                        }`}
                        title={
                          h
                            ? h.reachable
                              ? "reachable"
                              : `unreachable: ${h.error ?? "no response"}`
                            : "probing…"
                        }
                      />
                      <span className="font-medium">node {n.node_id}</span>
                      {n.is_self && (
                        <span className="rounded bg-emerald-800 px-1.5 py-0.5 text-xs">
                          this
                        </span>
                      )}
                      {href && (
                        <span
                          className="ml-auto text-xs text-sky-400"
                          aria-hidden
                        >
                          open ↗
                        </span>
                      )}
                    </div>
                    <div className="mt-1 text-xs text-zinc-500">
                      {n.address || "local"}
                    </div>
                    <div className="mt-1.5 flex flex-wrap items-center gap-x-3 gap-y-0.5 text-xs">
                      {h?.reachable ? (
                        <>
                          {h.version && (
                            <span className="text-zinc-400">v{h.version}</span>
                          )}
                          {h.latencyMs != null && !n.is_self && (
                            <span className="text-zinc-500">{h.latencyMs} ms</span>
                          )}
                          {n.is_self && (
                            <span className="text-emerald-400">healthy</span>
                          )}
                        </>
                      ) : h ? (
                        <span className="text-red-400">
                          unreachable{h.error ? ` · ${h.error}` : ""}
                        </span>
                      ) : (
                        <span className="text-zinc-600">probing…</span>
                      )}
                    </div>
                  </>
                );
                return href ? (
                  <a
                    key={n.node_id}
                    href={href}
                    target="_blank"
                    rel="noreferrer"
                    title={`Open node ${n.node_id} console (${n.address})`}
                    className={cls}
                  >
                    {inner}
                  </a>
                ) : (
                  <div key={n.node_id} className={cls}>
                    {inner}
                  </div>
                );
              })}
            </div>
          </section>

          <section>
            <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-zinc-500">
              Partitions
            </h2>
            <table className="w-full max-w-2xl border-collapse text-sm">
              <thead>
                <tr className="border-b border-zinc-800 text-left text-zinc-500">
                  <th className="py-2 pr-4 font-medium">Partition</th>
                  <th className="py-2 pr-4 font-medium">Leader</th>
                  <th className="py-2 pr-4 font-medium">Replicas</th>
                  <th className="py-2 pr-4 font-medium">Raft term</th>
                </tr>
              </thead>
              <tbody>
                {data.partitions.map((p) => (
                  <tr key={p.partition_id} className="border-b border-zinc-900">
                    <td className="py-2 pr-4">{p.partition_id}</td>
                    <td className="py-2 pr-4">
                      {p.leader === null ? (
                        <span className="text-amber-400">no leader</span>
                      ) : (
                        `node ${p.leader}`
                      )}
                    </td>
                    <td className="py-2 pr-4 text-zinc-400">
                      {p.replicas.map((r) => `node ${r}`).join(", ")}
                    </td>
                    <td className="py-2 pr-4 text-zinc-400">
                      {p.raft_term ?? "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
        </div>
      )}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="rounded-lg border border-zinc-800 bg-zinc-900 px-4 py-3">
      <div className="text-xs uppercase tracking-wide text-zinc-500">
        {label}
      </div>
      <div className="mt-1 text-xl font-semibold">{value}</div>
    </div>
  );
}
