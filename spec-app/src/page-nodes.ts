// The canonical Urban page.json node-type registry — the single source of truth
// for the set of node types a composed page (ADR 0042) may contain.
//
// A page is authored in the console Page Composer and rendered, unchanged, by the
// App-side runtime (nano-ide `urban`). Historically each surface enumerated the
// node-type set independently — the composer's validator/palette on one side, the
// runtime's `RENDERERS` map on the other — so a type added to one silently drifted
// from the other, and a page using a runtime-only type ("this page uses a newer
// component than this Console build supports"). See issue #843.
//
// Both surfaces now derive from this list instead of restating it:
//   - the console Page Composer's `PAGE_NODE_TYPES` re-exports it, and a
//     compile-time parity check binds it to the composer's `PageNode` union, so a
//     type present here but unhandled by the composer (or vice versa) fails to
//     compile;
//   - the runtime will assert `Object.keys(RENDERERS)` equals this set in a guard
//     test, so a type present here but unrendered fails the runtime build (the
//     runtime lives in another repo and consumes the published registry; that
//     guard lands as issue #843 P2).
//
// Adding a node type is therefore a single edit here that forces both surfaces to
// catch up — they can no longer silently disagree.
export const PAGE_NODE_TYPES = [
  "text",
  "nav",
  "actionForm",
  "dataGrid",
  "prose",
  "button",
] as const;

/** A node type known to a composed page — one of {@link PAGE_NODE_TYPES}. */
export type PageNodeType = (typeof PAGE_NODE_TYPES)[number];

/** Narrowing guard: is `value` a known page node type? */
export function isPageNodeType(value: unknown): value is PageNodeType {
  return (
    typeof value === "string" &&
    (PAGE_NODE_TYPES as readonly string[]).includes(value)
  );
}
