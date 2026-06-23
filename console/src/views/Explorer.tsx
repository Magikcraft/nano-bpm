import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { api, type Instance } from "../lib/api";
import { useLiveInvalidation } from "../lib/useLiveInvalidation";
import InstanceDetail from "./InstanceDetail";

const PAGE_SIZE = 50;

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
  const [page, setPage] = useState(0);
  const [searchParams, setSearchParams] = useSearchParams();

  // Allow deep-linking to a specific instance (e.g. from the modeler's "Start
  // instance" success link): ?instance=<key> preselects it, then the param is
  // cleared so it doesn't pin the selection on later navigation.
  useEffect(() => {
    const key = searchParams.get("instance");
    if (key) {
      setSelected(key);
      searchParams.delete("instance");
      setSearchParams(searchParams, { replace: true });
    }
  }, [searchParams, setSearchParams]);

  // Live: the SSE feed invalidates the list whenever the read model advances.
  // The prefix `["instances"]` invalidates every page query.
  useLiveInvalidation(["instances"]);
  const { data, isLoading, error } = useQuery({
    queryKey: ["instances", page],
    queryFn: () => api.instances(page, PAGE_SIZE),
    // Keep the current page visible while the next one loads, so paging and the
    // live SSE refetch don't flash an empty list.
    placeholderData: keepPreviousData,
  });

  const total = data?.total ?? 0;
  const pageCount = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const rangeStart = total === 0 ? 0 : page * PAGE_SIZE + 1;
  const rangeEnd = Math.min(total, page * PAGE_SIZE + (data?.items.length ?? 0));

  // Clamp the page if the dataset shrinks (e.g. retention prune) below it.
  useEffect(() => {
    if (page > 0 && page >= pageCount) setPage(pageCount - 1);
  }, [page, pageCount]);

  return (
    <div className="flex h-full">
      <div className="flex w-[28rem] shrink-0 flex-col border-r border-zinc-800">
        <header className="border-b border-zinc-800 px-5 py-4">
          <h1 className="text-xl font-semibold">Process instances</h1>
          <p className="text-xs text-zinc-500">
            {data
              ? total === 0
                ? "0 instances"
                : `${rangeStart}–${rangeEnd} of ${total} instance(s)`
              : "Live view"}
          </p>
        </header>
        <div className="min-h-0 flex-1 overflow-auto">
          {isLoading && <p className="p-5 text-zinc-400">Loading…</p>}
          {error && (
            <p className="p-5 text-red-400">Failed to load: {String(error)}</p>
          )}
          {data && data.items.length === 0 && (
            <p className="p-5 text-zinc-500">
              No instances yet. Deploy a process and create one.
            </p>
          )}
          <ul>
            {data?.items.map((inst: Instance) => (
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
        {total > PAGE_SIZE && (
          <footer className="flex items-center justify-between border-t border-zinc-800 px-5 py-3 text-xs">
            <button
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              disabled={page === 0}
              className="rounded border border-zinc-700 px-2 py-1 hover:bg-zinc-900 disabled:cursor-not-allowed disabled:opacity-40"
            >
              ← Prev
            </button>
            <span className="text-zinc-500">
              Page {page + 1} of {pageCount}
            </span>
            <button
              onClick={() => setPage((p) => Math.min(pageCount - 1, p + 1))}
              disabled={page >= pageCount - 1}
              className="rounded border border-zinc-700 px-2 py-1 hover:bg-zinc-900 disabled:cursor-not-allowed disabled:opacity-40"
            >
              Next →
            </button>
          </footer>
        )}
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
