// Single source of truth for the product tour's DOM anchors.
//
// A tour step highlights an element by CSS selector, and the element is tagged
// with a matching `data-tour="..."` attribute in a view. Those two references —
// the selector in steps.ts and the attribute in the component — are a classic
// drift surface: rename one and the tour silently skips the step. So both derive
// from the constants here instead of repeating literal strings.
//
// Used by: steps.ts (selectors), App.tsx (rail nav + "Take a tour"),
// Projects.tsx ("New project"), ProjectWorkspace.tsx (Run).

/**
 * `data-tour` value for a rail nav item, derived from its **route** (`item.to`)
 * rather than its display label. The route is the stable identity — renaming the
 * visible label must not silently move the DOM anchor away from the selector the
 * tour targets. `"/projects"` → `"nav-projects"`.
 */
export function navAnchor(route: string): string {
  return `nav-${route.replace(/^\//, "").toLowerCase()}`;
}

/** Every `data-tour` anchor the tour targets, in one place. */
export const TOUR_ANCHOR = {
  projectsNav: navAnchor("/projects"),
  explorerNav: navAnchor("/explorer"),
  topologyNav: navAnchor("/topology"),
  metricsNav: navAnchor("/metrics"),
  tracesNav: navAnchor("/traces"),
  newProject: "new-project",
  run: "run",
  takeATour: "take-a-tour",
  /** Explorer's inspection panel — the localdev journey's "where you debug" step. */
  explorerInspect: "explorer-inspect",
} as const;

export type TourAnchor = (typeof TOUR_ANCHOR)[keyof typeof TOUR_ANCHOR];

/** CSS attribute selector for a `data-tour` anchor value. */
export function tourSelector(anchor: string): string {
  return `[data-tour="${anchor}"]`;
}
