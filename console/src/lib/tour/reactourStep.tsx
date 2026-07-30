// Single source of truth for turning our profile-aware TourStep (steps.ts) into
// a @reactour/tour StepType. Both the provider (initial steps) and the hook
// (filtered steps at start) convert through here, so the mapping never drifts.

import type { StepType } from "@reactour/tour";
import type { TourStep } from "./steps";

export function toReactourStep(step: TourStep): StepType {
  const anchored = Boolean(step.selector);
  return {
    selector: step.selector ?? "body",
    // reactour positions to one side of the target; it has no separate "align"
    // axis (coarser than driver.js), so TourStep.align is intentionally dropped.
    position: anchored ? (step.side ?? "bottom") : "center",
    // Centered welcome step: don't cut a spotlight hole around <body>.
    bypassElem: !anchored,
    // Wait for (and re-position on) an async / lazy / post-navigation mount —
    // reactour's equivalent of driver.js waitForElement.
    mutationObservables: step.selector ? [step.selector] : undefined,
    resizeObservables: step.selector ? [step.selector] : undefined,
    content: (
      <div className="nano-tour-content">
        <div className="nano-tour-title">{step.title}</div>
        <div className="nano-tour-body">{step.body}</div>
      </div>
    ),
  };
}
