// Pure logic for recovering from the "stale chunk after redeploy" failure mode.
//
// The console is a hashed-chunk Vite SPA served by the Rust gateway. When the
// console is rebuilt/redeployed, a browser still running the *old* `index.html`
// holds references to old hashed chunks (e.g. `/console/assets/Workers-OLD.js`).
// The server no longer has that hash, and its SPA fallback (`spa_asset`) returns
// `index.html` (Content-Type `text/html`) for any unknown asset. The lazy
// `import()` then receives HTML and the browser throws:
//
//   'text/html' is not a valid JavaScript MIME type
//
// A full page reload fetches the fresh `index.html` (with current hashes) and
// resolves the module graph, so the recovery is simply: detect this class of
// error and reload once. These helpers are kept pure (no DOM/`window`) so the
// classification and the reload decision are unit-testable, mirroring
// `routeErrorBoundaryLogic.ts`.

/** sessionStorage key holding the epoch-ms of the last chunk-recovery reload. */
export const CHUNK_RELOAD_AT_KEY = "console:chunk-reloaded-at";

/**
 * How recently a recovery reload must have happened to *suppress* another one.
 * A genuine (non-stale) import failure keeps failing after a reload; debouncing
 * on this window means we reload at most once, then surface the real error to
 * the boundary instead of looping. A later redeploy (long after this window)
 * is free to trigger a fresh recovery reload again.
 */
export const CHUNK_RELOAD_DEBOUNCE_MS = 10_000;

function messageOf(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (typeof err === "string") return err;
  if (err && typeof err === "object" && "message" in err) {
    const m = (err as { message: unknown }).message;
    if (typeof m === "string") return m;
  }
  return "";
}

/**
 * True when `err` looks like a failed dynamic import of a hashed chunk that has
 * gone stale after a redeploy (the module fetch resolved to the SPA HTML
 * fallback, 404'd, or otherwise failed to load as a module). Matches the
 * browser-specific phrasings across Chromium, Firefox and Safari.
 */
export function isStaleChunkError(err: unknown): boolean {
  const msg = messageOf(err);
  if (!msg) return false;
  return (
    /Failed to fetch dynamically imported module/i.test(msg) ||
    /error loading dynamically imported module/i.test(msg) ||
    /Importing a module script failed/i.test(msg) ||
    /is not a valid JavaScript MIME type/i.test(msg) ||
    /expected a JavaScript(?:-or-Wasm)? module/i.test(msg) ||
    /Loading chunk \d+ failed/i.test(msg) ||
    /ChunkLoadError/i.test(msg)
  );
}

/**
 * Decide whether to perform a one-shot recovery reload for `err`. Reloads only
 * for a stale-chunk error that we have not *very recently* reloaded for
 * (`CHUNK_RELOAD_DEBOUNCE_MS`), so a genuinely broken module surfaces to the
 * error boundary instead of an infinite reload loop.
 *
 * @param lastReloadAt epoch-ms of the previous recovery reload, or `null`.
 * @param now current epoch-ms.
 */
export function shouldReloadForChunkError(
  err: unknown,
  lastReloadAt: number | null,
  now: number,
): boolean {
  if (!isStaleChunkError(err)) return false;
  return withinReloadBudget(lastReloadAt, now);
}

/**
 * Debounce-only recovery decision, used by the `vite:preloadError` backstop
 * where the event itself already signals a module-preload failure (so no
 * message classification is needed). True unless we reloaded within the last
 * `CHUNK_RELOAD_DEBOUNCE_MS`.
 */
export function withinReloadBudget(
  lastReloadAt: number | null,
  now: number,
): boolean {
  if (lastReloadAt == null) return true;
  return now - lastReloadAt > CHUNK_RELOAD_DEBOUNCE_MS;
}
