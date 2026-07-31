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

/**
 * `data-tour` value for a New Project template card, derived from the template
 * id (#411). Journeys 0a/2 spotlight a *suggested* starting template by id
 * (#408/#410); deriving both the attribute (Projects.tsx) and those journeys'
 * selectors from this one function keeps them from drifting apart, exactly as
 * `navAnchor` does for rail items. `"workflow-starter"` → `"template-workflow-starter"`.
 */
export function templateAnchor(id: string): string {
  return `template-${id}`;
}

/**
 * Every *static* `data-tour` anchor the tour targets, in one place. Per-template
 * card anchors are dynamic (derived from the template id via `templateAnchor`)
 * and so are intentionally not enumerated here.
 */
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
  /** The workspace file tree — the RAD journey names the four parts of a fullstack app here. */
  fileTree: "file-tree",
  /** The page/form editor surface — the RAD journey's "change a screen without a frontend" moment. */
  pageEditor: "page-editor",
  /** The served-app link shown while an Urban App runs — the RAD journey's payoff (opens the app on its own port). */
  servedApp: "served-app",
  // Workspace anchors the agentic authoring journey (#408) spotlights: the flow
  // file editor, the derived read-only Model view toggle, and the Run output.
  flowEditor: "flow-editor",
  modelView: "model-view",
  runOutput: "run-output",
} as const;

export type TourAnchor = (typeof TOUR_ANCHOR)[keyof typeof TOUR_ANCHOR];

/** CSS attribute selector for a `data-tour` anchor value. */
export function tourSelector(anchor: string): string {
  return `[data-tour="${anchor}"]`;
}
