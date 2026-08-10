import { lazy, type ComponentType } from "react";
import {
  CHUNK_RELOAD_AT_KEY,
  shouldReloadForChunkError,
  withinReloadBudget,
} from "./chunkReload";

function lastReloadAt(): number | null {
  try {
    const raw = sessionStorage.getItem(CHUNK_RELOAD_AT_KEY);
    if (raw == null) return null;
    const n = Number.parseInt(raw, 10);
    return Number.isFinite(n) ? n : null;
  } catch {
    return null;
  }
}

function markReloadNow(): void {
  try {
    sessionStorage.setItem(CHUNK_RELOAD_AT_KEY, String(Date.now()));
  } catch {
    // sessionStorage may be unavailable (private mode / disabled); the reload
    // still helps, we just lose the loop-guard for this navigation.
  }
}

/**
 * Perform the one-shot recovery reload for a stale-chunk `err` if appropriate.
 * Returns `true` when a reload was triggered (the caller should stop and let the
 * navigation happen), `false` when the error should propagate normally.
 */
function recoverFromStaleChunk(err: unknown): boolean {
  if (typeof window === "undefined") return false;
  if (!shouldReloadForChunkError(err, lastReloadAt(), Date.now())) return false;
  markReloadNow();
  window.location.reload();
  return true;
}

/**
 * `React.lazy` for route/component code-splitting that self-heals the
 * "stale chunk after redeploy" failure. When the dynamic `import()` fails
 * because the hashed chunk went stale (the fetch resolved to the SPA's
 * `index.html` / `text/html`), it triggers a single full reload to pull the
 * fresh module graph instead of surfacing
 * `'text/html' is not a valid JavaScript MIME type` to the error boundary.
 *
 * Drop-in for `lazy(() => import("..."))`. The `import()` literal is preserved
 * verbatim inside the factory, so the compile-time `__STUDIO__ ? lazyImport(...)
 * : null` guarding still lets esbuild drop studio-only chunks from the observe
 * build (ADR 0034).
 */
export function lazyImport<T extends ComponentType<any>>(
  factory: () => Promise<{ default: T }>,
) {
  return lazy(async () => {
    try {
      return await factory();
    } catch (err) {
      if (recoverFromStaleChunk(err)) {
        // Reload is navigating the page away; hold Suspense on the fallback by
        // never settling so React doesn't flash the error boundary first.
        return await new Promise<{ default: T }>(() => {});
      }
      throw err;
    }
  });
}

/**
 * Install a global backstop for Vite's module-preload failures. The
 * `<link rel="modulepreload">` path fires a `vite:preloadError` event rather
 * than rejecting an `import()` factory, so `lazyImport` alone can't catch it.
 * Call once at startup (see `main.tsx`).
 */
export function installChunkReloadBackstop(): void {
  if (typeof window === "undefined") return;
  window.addEventListener("vite:preloadError", (event) => {
    if (withinReloadBudget(lastReloadAt(), Date.now())) {
      event.preventDefault();
      markReloadNow();
      window.location.reload();
    }
  });
}
