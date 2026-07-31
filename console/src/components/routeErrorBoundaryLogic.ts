// Pure helpers for the route-level error boundary (see RouteErrorBoundary.tsx).
//
// The boundary itself is a React class component whose catch path can only be
// exercised in a real renderer, so the reusable, side-effect-free decisions live
// here where the console's node:test unit suite can cover them directly.

/**
 * React error boundaries receive whatever value was thrown (not necessarily an
 * `Error`). Normalize it so the rest of the boundary can rely on `.message`.
 */
export function normalizeError(thrown: unknown): Error {
  if (thrown instanceof Error) return thrown;
  if (typeof thrown === "string") return new Error(thrown);
  try {
    return new Error(String(thrown));
  } catch {
    return new Error("Unknown error");
  }
}

/**
 * A short, safe single-line summary of an error for the fallback UI. Falls back
 * to the error name (or a generic label) when there is no message.
 */
export function formatErrorSummary(error: Error): string {
  const message = (error?.message ?? "").trim();
  if (message) return message;
  return error?.name || "Error";
}

/**
 * Whether the boundary should clear a captured error. It resets when the reset
 * key (the active route path) changes while an error is held — i.e. the user
 * navigated away from the crashed view — so one view's crash never wedges the
 * whole session. No change, or a change with no error, is a no-op.
 */
export function shouldResetOnKeyChange(
  prevKey: string,
  nextKey: string,
  hasError: boolean,
): boolean {
  return hasError && prevKey !== nextKey;
}
