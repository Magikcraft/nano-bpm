import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api, type Instance } from "../lib/api";
import { useLiveInvalidation } from "../lib/useLiveInvalidation";
import InstanceDetail from "./InstanceDetail";

function stateBadge(state: string, hasIncident: boolean): string {
  if (hasIncident) return "bg-red-900/60 text-red-300";
  switch (state) {
    case "Active":
      return "bg-sky-900/60 text-sky-300";
    case "Completed":
      return "bg-emerald-900/60 text-emerald-300";
    case "Terminated":
      return "bg-zinc-700 text-zinc-300";
    default:
      return "bg-zinc-700 text-zinc-300";
  }
}

export default function Explorer() {
  const [selected, setSelected] = useState<string | null>(null);

  // Live: the SSE feed invalidates the list whenever the read model advances.
  useLiveInvalidation(["instances"]);
  const { data, isLoading, error } = useQuery({
    queryKey: ["instances"],
    queryFn: api.instances,
  });

  return (
    <div className="flex h-full">
      <div className="flex w-[28rem] shrink-0 flex-col border-r border-zinc-800">
        <header className="border-b border-zinc-800 px-5 py-4">
          <h1 className="text-xl font-semibold">Process instances</h1>
          <p className="text-xs text-zinc-500">
            {data ? `${data.length} instance(s)` : "Live view"}
          </p>
        </header>
        <div className="min-h-0 flex-1 overflow-auto">
          {isLoading && <p className="p-5 text-zinc-400">Loading…</p>}
          {error && (
            <p className="p-5 text-red-400">Failed to load: {String(error)}</p>
          )}
          {data && data.length === 0 && (
            <p className="p-5 text-zinc-500">
              No instances yet. Deploy a process and create one.
            </p>
          )}
          <ul>
            {data?.map((inst: Instance) => (
              <li key={inst.key}>
                <button
                  onClick={() => setSelected(inst.key)}
                  className={`flex w-full flex-col gap-1 border-b border-zinc-900 px-5 py-3 text-left hover:bg-zinc-900 ${
                    selected === inst.key ? "bg-zinc-900" : ""
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <span className="font-medium">{inst.process_id}</span>
                    <span
                      className={`rounded px-1.5 py-0.5 text-xs ${stateBadge(
                        inst.state,
                        inst.has_incident,
                      )}`}
                    >
                      {inst.has_incident ? "Incident" : inst.state}
                    </span>
                  </div>
                  <div className="font-mono text-xs text-zinc-500">
                    {inst.key} · v{inst.version}
                  </div>
                </button>
              </li>
            ))}
          </ul>
        </div>
      </div>

      <div className="min-w-0 flex-1 overflow-auto">
        {selected ? (
          <InstanceDetail instanceKey={selected} />
        ) : (
          <div className="flex h-full items-center justify-center text-sm text-zinc-500">
            Select an instance to inspect it.
          </div>
        )}
      </div>
    </div>
  );
}
