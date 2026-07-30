// Profile-aware first-run product tour step list.
//
// The console ships two build profiles (ADR 0034): "studio" (the maker IDE) and
// "observe" (the operator surface). The journey — and therefore the tour — is
// different in each, so the step list is derived from CONSOLE_PROFILE rather
// than duplicated. Each step names a react-router `route` it needs (the hook
// navigates there before showing the step) and a `selector` to highlight;
// driver.js waits for that selector to appear (see useProductTour) so steps can
// target elements that mount asynchronously (lazy routes, the bpmn-js canvas).

import { CONSOLE_PROFILE, type ConsoleProfile } from "../profile";

export interface TourStep {
  /** Stable id, handy for analytics / debugging. */
  id: string;
  /** react-router path to navigate to before the step is shown (no basename). */
  route?: string;
  /** CSS selector to highlight. Omit for a centered, anchorless step. */
  selector?: string;
  title: string;
  body: string;
  side?: "top" | "right" | "bottom" | "left";
  align?: "start" | "center" | "end";
}

const studioSteps: TourStep[] = [
  {
    id: "welcome",
    title: "Welcome to Nano",
    body: "A self-contained BPMN engine and rapid-application IDE. This 60-second tour walks the maker loop: create a project, model a process, run it.",
  },
  {
    id: "projects-nav",
    route: "/projects",
    selector: '[data-tour="nav-projects"]',
    title: "Projects",
    body: "Everything starts here. A project is a folder of BPMN/DMN models, forms and a pack that says how to run them.",
    side: "right",
    align: "start",
  },
  {
    id: "new-project",
    route: "/projects",
    selector: '[data-tour="new-project"]',
    title: "Scaffold a project",
    body: "Pick a template — each one wires up a runnable toolchain (Node, Deno, JVM, …) so Run works with zero setup.",
    side: "bottom",
    align: "end",
  },
  {
    id: "run",
    // No route: the Run button only exists inside an open project workspace.
    // If no project is open the selector never resolves and driver.js skips it
    // (skipMissingElement) — the tour degrades gracefully on a fresh install.
    selector: '[data-tour="run"]',
    title: "Run your model",
    body: "Open a project and hit Run to boot the engine and execute it. Tokens animate live on the canvas as instances progress.",
    side: "bottom",
    align: "start",
  },
  {
    id: "observe-nav",
    route: "/projects",
    selector: '[data-tour="nav-explorer"]',
    title: "Observe everything",
    body: "Explorer, Traces, Metrics and Workers give full visibility into running instances — the operator surface is always one click away.",
    side: "right",
    align: "start",
  },
];

const observeSteps: TourStep[] = [
  {
    id: "welcome-observe",
    title: "Welcome — Operator console",
    body: "This is the lean operator surface: topology, metrics, traces and worker health for a running Nano cluster.",
  },
  {
    id: "topology-nav",
    route: "/topology",
    selector: '[data-tour="nav-topology"]',
    title: "Topology",
    body: "Your nodes, partitions and their health at a glance.",
    side: "right",
    align: "start",
  },
  {
    id: "explorer-nav",
    route: "/topology",
    selector: '[data-tour="nav-explorer"]',
    title: "Explorer",
    body: "Inspect individual process instances — their variables and current state.",
    side: "right",
    align: "start",
  },
  {
    id: "metrics-nav",
    route: "/topology",
    selector: '[data-tour="nav-metrics"]',
    title: "Metrics",
    body: "Throughput, backlog and latency — the numbers that matter under load.",
    side: "right",
    align: "start",
  },
];

/** The tour step list for the given (or current) build profile. */
export function getTourSteps(
  profile: ConsoleProfile = CONSOLE_PROFILE,
): TourStep[] {
  return profile === "studio" ? studioSteps : observeSteps;
}
