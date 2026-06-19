import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";

/// Subscribes to the gateway's `/console/api/stream` SSE feed and invalidates
/// the given query keys whenever the read model advances. The server emits a
/// cheap change signal (the exported position); the client decides what to
/// refetch — keeping the wire protocol trivial and the UI always fresh without
/// busy polling.
export function useLiveInvalidation(queryKeys: string[]) {
  const queryClient = useQueryClient();

  useEffect(() => {
    const source = new EventSource("/console/api/stream");
    const onMessage = () => {
      for (const key of queryKeys) {
        queryClient.invalidateQueries({ queryKey: [key] });
      }
    };
    source.addEventListener("instances", onMessage);
    return () => {
      source.removeEventListener("instances", onMessage);
      source.close();
    };
    // queryKeys is a stable literal at the call sites; intentionally not a dep.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryClient]);
}
