import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { getInstance, listInstances, type Instance } from "../gen";
import { useLiveInvalidation } from "../lib/useLiveInvalidation";
import InstanceDetail from "./InstanceDetail";
import { Badge, Button } from "../components/ui";
import { TOUR_ANCHOR } from "../lib/tour/tourAnchors";
import {
  markBaseUrlCopied,
  markExplorerReached,
  swaggerUrl,
  v2BaseUrl,
} from "../lib/tour/journeys/localdev";

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

/**
 * One instance row in the left list. `rowRef` is forwarded only for the selected
 * row so the list can scroll it into view (e.g. after a deep-link preselects an
 * instance that would otherwise be below the fold). `pinnedLabel` renders the
 * small "Linked" tag on the pinned deep-link row that sits above the page when
 * the selected instance isn't on the current page.
 */
function InstanceRow({
  inst,
  selected,
  onSelect,
  rowRef,
  pinnedLabel,
}: {
  inst: Instance;
  selected: boolean;
  onSelect: (key: string) => void;
  rowRef?: (node: HTMLButtonElement | null) => void;
  pinnedLabel?: boolean;
}) {
  return (
    <button
      ref={rowRef}
      onClick={() => onSelect(inst.key)}
      className={`flex w-full flex-col gap-1 border-b border-edge px-5 py-3 text-left hover:bg-hover ${
        selected ? "bg-accent/10" : ""
      }`}
    >
      <div className="flex items-center justify-between">
        <span className="flex items-center gap-2 font-medium text-fg">
          {inst.process_id}
          {pinnedLabel && (
            <span className="rounded bg-accent/15 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-accent-strong">
              Linked
            </span>
          )}
        </span>
        <Badge tone={stateTone(inst.state, inst.has_incident)}>
          {inst.has_incident ? "Incident" : inst.state}
        </Badge>
      </div>
      <div className="font-mono text-xs text-fg-faint">
        {inst.key} · v{inst.version}
      </div>
    </button>
  );
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

  // Reaching Explorer is half of the headless-local-dev journey's outcome (the
  // other half is taking the v2 base URL below). Record it on mount so the
  // journey's successEvent can tell "walked the steps" from "actually debugged
  // here". Harmless outside a tour.
  useEffect(() => {
    markExplorerReached();
  }, []);

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

  // Is the selected instance present on the page currently in view? A deep-link
  // (or a selection made before paging) can point at an instance that lives on
  // another page — the list is paged server-side with no key filter, so we can't
  // page to it. Instead we fetch just that instance and pin a highlighted row
  // above the list so the selection is always visible, not silently off-screen.
  const items = data?.items ?? [];
  const selectedOnPage =
    selected != null && items.some((i: Instance) => i.key === selected);
  const pinnedQuery = useQuery({
    // Same key + fetch as InstanceDetail's, so React Query dedupes to one request.
    queryKey: ["instance", selected],
    queryFn: async () =>
      (await getInstance({ path: { key: selected! }, throwOnError: true }))
        .data,
    enabled: selected != null && !selectedOnPage,
  });
  const pinned =
    selected != null && !selectedOnPage
      ? pinnedQuery.data?.instance
      : undefined;

  // Scroll the selected row into view whenever the selection (or the loaded
  // page) changes, so a preselected instance below the fold is revealed rather
  // than merely highlighted off-screen. `block: "nearest"` avoids jumping the
  // whole page when the row is already visible.
  const selectedRowRef = useRef<HTMLButtonElement | null>(null);
  useEffect(() => {
    selectedRowRef.current?.scrollIntoView({ block: "nearest" });
  }, [selected, data, pinned]);

  // Clamp the page if the dataset shrinks (e.g. retention prune) below it.
  useEffect(() => {
    if (page > 0 && page >= pageCount) setPage(pageCount - 1);
  }, [page, pageCount]);

  return (
    <div className="flex h-full">
      <div className="flex w-[28rem] shrink-0 flex-col border-r border-edge">
        <header
          data-tour={TOUR_ANCHOR.explorerInspect}
          className="border-b border-edge px-5 py-4"
        >
          <h1 className="text-xl font-semibold text-fg">Process instances</h1>
          <p className="text-xs text-fg-faint">
            {data
              ? total === 0
                ? "0 instances"
                : `${rangeStart}–${rangeEnd} of ${total} instance(s)`
              : "Live view"}
          </p>
          <BaseUrlAffordance />
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
            {pinned && (
              <li key={`pinned-${pinned.key}`}>
                <InstanceRow
                  inst={pinned}
                  selected
                  onSelect={setSelected}
                  rowRef={(node) => (selectedRowRef.current = node)}
                  pinnedLabel
                />
              </li>
            )}
            {items.map((inst: Instance) => (
              <li key={inst.key}>
                <InstanceRow
                  inst={inst}
                  selected={selected === inst.key}
                  onSelect={setSelected}
                  rowRef={
                    selected === inst.key
                      ? (node) => (selectedRowRef.current = node)
                      : undefined
                  }
                />
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

/**
 * The v2 base URL affordance — a durable feature, not a tour-only element.
 *
 * A headless user's whole integration is "point my Camunda 8 client at one URL",
 * so the console surfaces that URL where they land to debug, with a copy button
 * and a link to the offline Swagger UI. The URL is derived from the live origin
 * (never a hardcoded port), and copy falls back to selecting the text because
 * `navigator.clipboard` is unavailable over the plain-HTTP LAN origins where a
 * local engine is typically reached.
 */
function BaseUrlAffordance() {
  const origin =
    typeof window !== "undefined" && window.location?.origin
      ? window.location.origin
      : "http://127.0.0.1:8080";
  const base = v2BaseUrl(origin);
  const inputRef = useRef<HTMLInputElement>(null);
  const [copied, setCopied] = useState(false);
  const [failed, setFailed] = useState(false);

  const onCopy = async () => {
    const ok = await copyToClipboard(base);
    if (ok) {
      markBaseUrlCopied();
      setCopied(true);
      setFailed(false);
      window.setTimeout(() => setCopied(false), 1500);
    } else {
      // Could not write the clipboard (insecure context): actually select the
      // text so the user can copy it by hand, and still count it as taken.
      // Clear `copied` too, in case this was a re-click inside the 1.5s "Copied"
      // window — the label must reflect the fallback, not a stale success.
      markBaseUrlCopied();
      setCopied(false);
      setFailed(true);
      inputRef.current?.focus();
      inputRef.current?.select();
    }
  };

  return (
    <div className="mt-3 flex items-center gap-2">
      <input
        ref={inputRef}
        readOnly
        value={base}
        aria-label="Camunda 8 v2 base URL"
        onFocus={(e) => e.currentTarget.select()}
        className={`min-w-0 flex-1 rounded-md border bg-inset px-2 py-1 font-mono text-xs text-fg outline-none ${
          failed ? "border-accent" : "border-edge"
        }`}
      />
      <Button size="sm" onClick={onCopy} title="Copy the Camunda 8 v2 base URL">
        {copied ? "Copied" : failed ? "Select & copy" : "Copy v2 URL"}
      </Button>
      <a
        href={swaggerUrl(origin)}
        target="_blank"
        rel="noreferrer noopener"
        className="text-xs text-accent-strong hover:underline"
      >
        Swagger
      </a>
    </div>
  );
}

/**
 * Copy text, falling back when the async clipboard API is unavailable.
 * `navigator.clipboard` needs a secure context, which a LAN/plain-HTTP engine
 * origin is not — return false so the caller can select the text instead.
 */
async function copyToClipboard(text: string): Promise<boolean> {
  try {
    const clipboard = globalThis.navigator?.clipboard;
    if (clipboard && typeof clipboard.writeText === "function") {
      await clipboard.writeText(text);
      return true;
    }
  } catch {
    // Fall through to the legacy path.
  }
  try {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return ok;
  } catch {
    return false;
  }
}
