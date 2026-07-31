import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { listInstances, type Instance } from "../gen";
import { useLiveInvalidation } from "../lib/useLiveInvalidation";
import { copyText } from "../lib/clipboard";
import { TOUR_ANCHOR } from "../lib/tour/tourAnchors";
import {
  markBaseUrlCopied,
  markExplorerReached,
  v2BaseUrl,
} from "../lib/tour/journeys/localdev-progress";
import InstanceDetail from "./InstanceDetail";
import { Badge, Button } from "../components/ui";

const PAGE_SIZE = 50;

/**
 * The Camunda-compatible v2 base URL, with one-click copy — the durable
 * counterpart to journey 1's "point your client at it" handoff step. It outlives
 * the tour: an operator debugging a headless engine wants this line at hand, tour
 * or no tour.
 *
 * Uses a readonly input as the copy target so the select-and-copy fallback works
 * where `navigator.clipboard` is unavailable — this console is routinely served
 * over plain HTTP on a LAN address, an insecure context where the async clipboard
 * API is absent.
 */
function CopyBaseUrl() {
  const url = v2BaseUrl();
  const inputRef = useRef<HTMLInputElement>(null);
  const [copied, setCopied] = useState(false);

  const onCopy = () => {
    void copyText(url).then((ok) => {
      // Record the outcome regardless of secure context: on the fallback path we
      // pre-select the text so the user can finish with Ctrl/Cmd-C.
      markBaseUrlCopied();
      if (!ok) inputRef.current?.select();
      setCopied(true);
      window.setTimeout(() => setCopied(false), 2000);
    });
  };

  return (
    <div
      data-tour={TOUR_ANCHOR.explorerBaseUrl}
      className="mt-3 flex items-center gap-2"
    >
      <span className="shrink-0 text-xs text-fg-faint">v2 API</span>
      <input
        ref={inputRef}
        readOnly
        value={url}
        onFocus={(e) => e.currentTarget.select()}
        aria-label="Camunda-compatible v2 API base URL"
        className="min-w-0 flex-1 rounded border border-edge bg-input px-2 py-1 font-mono text-xs text-fg"
      />
      <Button size="sm" onClick={onCopy}>
        {copied ? "Copied" : "Copy"}
      </Button>
    </div>
  );
}

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
    queryFn: async () =>
      (
        await listInstances({
          query: { page, pageSize: PAGE_SIZE },
          throwOnError: true,
        })
      ).data,
    // Keep the current page visible while the next one loads, so paging and the
    // live SSE refetch don't flash an empty list.
    placeholderData: keepPreviousData,
  });

  const total = data?.total ?? 0;
  const pageCount = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const rangeStart = total === 0 ? 0 : page * PAGE_SIZE + 1;
  const rangeEnd = Math.min(
    total,
    page * PAGE_SIZE + (data?.items.length ?? 0),
  );

  // Clamp the page if the dataset shrinks (e.g. retention prune) below it.
  useEffect(() => {
    if (page > 0 && page >= pageCount) setPage(pageCount - 1);
  }, [page, pageCount]);

  // Latch "the headless user reached the debugger" for journey 1's success
  // signal. The tour runner samples context only at start and on verify polls,
  // so a route-based context source would miss a spotlight step landing here;
  // a mount effect is the robust place to record it.
  useEffect(() => {
    markExplorerReached();
  }, []);

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
          <CopyBaseUrl />
        </header>
        <div
          data-tour={TOUR_ANCHOR.explorerInstances}
          className="min-h-0 flex-1 overflow-auto"
        >
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
                    <span className="font-medium text-fg">
                      {inst.process_id}
                    </span>
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
