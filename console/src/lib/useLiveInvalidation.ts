import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";

/// Subscribes to the gateway's `/console/api/stream` SSE feed and invalidates
/// the given query keys whenever the read model advances. The server emits a
/// cheap change signal (the exported position); the client decides what to
/// refetch — keeping the wire protocol trivial and the UI always fresh without
/// busy polling.
///
/// The change signal is edge-triggered, so a client that misses an edge stays
/// stale until the next one. That is exactly what a gateway restart causes: the
/// SSE connection drops, the browser silently auto-reconnects, but any state
/// that changed during the gap (or the pre-restart cache itself) is never
/// refetched — e.g. an instance the console still shows as ACTIVE after it was
/// cancelled/terminated across the bounce. So we also invalidate on every
/// `open`: the initial connect and every auto-reconnect both trigger a
/// catch-up refetch, closing that window. (The mount-time refetch React Query
/// already does makes the first `open` a cheap dedupe.)
export function useLiveInvalidation(queryKeys: string[]) {
  const queryClient = useQueryClient();

  useEffect(() => {
    const source = new EventSource("/console/api/stream");
    const invalidateAll = () => {
      for (const key of queryKeys) {
        queryClient.invalidateQueries({ queryKey: [key] });
      }
    };
    source.addEventListener("open", invalidateAll);
    source.addEventListener("instances", invalidateAll);
    return () => {
      source.removeEventListener("open", invalidateAll);
      source.removeEventListener("instances", invalidateAll);
      source.close();
    };
    // queryKeys is a stable literal at the call sites; intentionally not a dep.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryClient]);
}
