// Shared state for Journey 1 (headless local-dev), kept apart from the journey
// definition so the view that feeds it (`Explorer.tsx`) can import the tracker
// and the URL helper WITHOUT pulling in `registerJourney` / `registerContextSource`
// side effects. This module has none: importing it registers nothing.

/**
 * The Camunda-compatible v2 base URL, derived from the live origin.
 *
 * Deliberately NOT hardcoded to `:8080`: `c8ctl nano start --port N` serves the
 * console and the gateway from the same origin on whatever port was chosen, and
 * the whole point journey 1 makes is "your existing client works unchanged — this
 * is the only line you change", so the line it shows has to be the real one.
 *
 * The `typeof window` guard keeps the module importable from a Node unit test
 * (where there is no `window`); the fallback literal is never used in a browser,
 * where `window.location.origin` is always the live origin.
 */
export function liveOrigin(): string {
  return typeof window !== "undefined" && window.location
    ? window.location.origin
    : "http://127.0.0.1:8080";
}

export function v2BaseUrl(): string {
  return `${liveOrigin()}/v2`;
}

/**
 * Journey 1's outcome is "the base URL was copied AND `/explorer` was reached at
 * least once". Neither is a fact the base `listProjects()` context carries, and
 * neither is reliably true of the *final* context snapshot alone (the journey can
 * end on `/traces`, not `/explorer`), so both are latched here as they happen and
 * read back by the journey's `successEvent`.
 *
 * Latches are process-lifetime by design: a first-timer who genuinely copied the
 * URL and opened the debugger has achieved the outcome, and re-running the
 * journey should not un-achieve it. `resetLocaldevProgress` exists for tests.
 */
interface LocaldevProgress {
  baseUrlCopied: boolean;
  explorerReached: boolean;
}

const progress: LocaldevProgress = {
  baseUrlCopied: false,
  explorerReached: false,
};

export function markBaseUrlCopied(): void {
  progress.baseUrlCopied = true;
}

export function markExplorerReached(): void {
  progress.explorerReached = true;
}

/** True once both halves of the journey-1 outcome have happened. */
export function localdevSucceeded(): boolean {
  return progress.baseUrlCopied && progress.explorerReached;
}

/** Test seam: forget both latches. */
export function resetLocaldevProgress(): void {
  progress.baseUrlCopied = false;
  progress.explorerReached = false;
}
