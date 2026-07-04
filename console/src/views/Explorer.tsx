import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { api, type Instance } from "../lib/api";
import { useLiveInvalidation } from "../lib/useLiveInvalidation";
import InstanceDetail from "./InstanceDetail";
import { Badge, Button } from "../components/ui";

const PAGE_SIZE = 50;

function stateTone(
  state: string,
  hasIncident: boolean,
): "danger" | "info" | "ok" | "neutral" {
  if (hasIncident) return "danger";
  switch (state) {
    case "Active":
      return "info";
    case "Completed":
      return "ok";
    case "Terminated":
      return "neutral";
    default:
      return "neutral";
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
      <div className="flex w-[28rem] shrink-0 flex-col border-r border-edge">
        <header className="border-b border-edge px-5 py-4">
          <h1 className="text-xl font-semibold text-fg">Process instances</h1>
          <p className="text-xs text-fg-faint">
            {data
              ? total === 0
                ? "0 instances"
                : `${rangeStart}–${rangeEnd} of ${total} instance(s)`
              : "Live view"}
          </p>
        </header>
        <div className="min-h-0 flex-1 overflow-auto">
          {isLoading && <p className="p-5 text-fg-muted">Loading…</p>}
          {error && (
            <p className="p-5 text-danger">Failed to load: {String(error)}</p>
          )}
          {data && data.items.length === 0 && (
            <p className="p-5 text-fg-faint">
              No instances yet. Deploy a process and create one.
            </p>
          )}
          <ul>
            {data?.items.map((inst: Instance) => (
              <li key={inst.key}>
                <button
                  onClick={() => setSelected(inst.key)}
                  className={`flex w-full flex-col gap-1 border-b border-edge px-5 py-3 text-left hover:bg-hover ${
                    selected === inst.key ? "bg-accent/10" : ""
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <span className="font-medium text-fg">{inst.process_id}</span>
                    <Badge tone={stateTone(inst.state, inst.has_incident)}>
                      {inst.has_incident ? "Incident" : inst.state}
                    </Badge>
                  </div>
                  <div className="font-mono text-xs text-fg-faint">
                    {inst.key} · v{inst.version}
                  </div>
                </button>
              </li>
            ))}
          </ul>
        </div>
        {total > PAGE_SIZE && (
          <footer className="flex items-center justify-between border-t border-edge px-5 py-3 text-xs">
            <Button
              size="sm"
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              disabled={page === 0}
            >
              ← Prev
            </Button>
            <span className="text-fg-faint">
              Page {page + 1} of {pageCount}
            </span>
            <Button
              size="sm"
              onClick={() => setPage((p) => Math.min(pageCount - 1, p + 1))}
              disabled={page >= pageCount - 1}
            >
              Next →
            </Button>
          </footer>
        )}
      </div>

      <div className="min-w-0 flex-1 overflow-auto">
        {selected ? (
          <InstanceDetail instanceKey={selected} />
        ) : (
          <div className="flex h-full items-center justify-center text-sm text-fg-faint">
            Select an instance to inspect it.
          </div>
        )}
      </div>
    </div>
  );
}
