//! The message contract between the extension host and the diagram webview, plus
//! a couple of pure helpers that are unit-testable without a running VS Code.

/** Extension → webview messages. */
export type HostToWebview =
  | { type: 'load'; xml: string }
  | { type: 'highlight'; elements: string[] }
  | { type: 'breakpoints'; elements: string[] };

/** Webview → extension messages. */
export type WebviewToHost =
  | { type: 'ready' }
  | { type: 'toggleBreakpoint'; element: string };

/**
 * The marker delta between two highlight sets: which element ids gained the
 * "paused" marker and which lost it. Keeping this pure lets the webview apply a
 * minimal diagram-js `addMarker`/`removeMarker` update, and lets us test the
 * highlight logic without a canvas.
 */
export function markerDelta(
  prev: readonly string[],
  next: readonly string[],
): { add: string[]; remove: string[] } {
  const prevSet = new Set(prev);
  const nextSet = new Set(next);
  const add = next.filter((id) => !prevSet.has(id));
  const remove = prev.filter((id) => !nextSet.has(id));
  return { add, remove };
}
