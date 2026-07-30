// First-run product tour — @reactour/tour spike (issue #393).
//
// Unlike driver.js (imperative), @reactour/tour is *declarative*: the tour is a
// React context (<TourProvider>) that owns `currentStep` / `isOpen`, and you
// read/drive it through the `useTour()` hook. That shape means the provider has
// to wrap the app (so any view can call useTour), and the two things reactour
// can't do on its own — drive react-router between steps, and skip steps whose
// target never mounts — are bolted on in useProductTour.
//
// This file owns the reactour wiring: converting our profile-aware TourStep[]
// (steps.ts, shared verbatim with the driver.js spike) into reactour StepType[],
// theming the popover/mask with the console's --nano-* tokens, and rendering
// text Back/Next/Done controls in place of reactour's default arrow+dots.

import type { ReactNode } from "react";
import { TourProvider, type ProviderProps } from "@reactour/tour";
import { getTourSteps } from "./steps";
import { toReactourStep } from "./reactourStep";
import "./tour.css";

// Popover + mask theming via the app's design tokens (src/theme/tokens.css), so
// the tour tracks dark/light mode. reactour exposes a per-key styles object of
// (base) => CSSProperties overrides.
const tourStyles: ProviderProps["styles"] = {
  popover: (base) => ({
    ...base,
    background: "var(--nano-panel)",
    color: "var(--nano-text)",
    border: "1px solid var(--nano-edge)",
    borderRadius: 12,
    boxShadow:
      "0 10px 30px -10px rgb(0 0 0 / 45%), 0 2px 8px -4px rgb(0 0 0 / 30%)",
    maxWidth: 320,
    padding: 20,
  }),
  maskArea: (base) => ({ ...base, rx: 8 }),
  badge: (base) => ({
    ...base,
    background: "var(--nano-accent)",
    color: "var(--nano-on-accent)",
  }),
  close: (base) => ({
    ...base,
    color: "var(--nano-text-faint)",
    top: 12,
    right: 12,
    width: 10,
    height: 10,
  }),
};

// Text controls instead of reactour's default arrow buttons + dots, matching the
// driver.js spike's Back / Next / Done so the two tours feel like siblings.
const nextButton: ProviderProps["nextButton"] = ({
  currentStep,
  stepsLength,
  setCurrentStep,
  setIsOpen,
}) => {
  const isLast = currentStep === stepsLength - 1;
  return (
    <button
      type="button"
      className="nano-tour-btn nano-tour-btn-next"
      onClick={() =>
        isLast
          ? setIsOpen(false)
          : setCurrentStep((s) => Math.min(s + 1, stepsLength - 1))
      }
    >
      {isLast ? "Done" : "Next"}
    </button>
  );
};

const prevButton: ProviderProps["prevButton"] = ({
  currentStep,
  setCurrentStep,
}) => (
  <button
    type="button"
    className="nano-tour-btn nano-tour-btn-prev"
    disabled={currentStep === 0}
    onClick={() => setCurrentStep((s) => Math.max(s - 1, 0))}
  >
    Back
  </button>
);

export function ProductTourProvider({ children }: { children: ReactNode }) {
  return (
    <TourProvider
      steps={getTourSteps().map(toReactourStep)}
      styles={tourStyles}
      padding={{ mask: 6, popover: 12 }}
      showBadge
      showDots={false}
      showCloseButton
      scrollSmooth
      className="nano-tour"
      nextButton={nextButton}
      prevButton={prevButton}
    >
      {children}
    </TourProvider>
  );
}
