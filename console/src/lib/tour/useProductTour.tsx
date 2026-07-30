// First-run product tour hook — @reactour/tour spike (issue #393).
//
// reactour owns the popover and the currentStep/isOpen state (via <TourProvider>
// in ProductTourProvider.tsx). This hook adds the three things the library can't
// do itself:
//
//   1. Route navigation — reactour is DOM-only, so we watch currentStep and
//      drive react-router to the route each step needs. reactour's per-step
//      mutationObservables then wait for the target to mount on the new route.
//   2. Skip-missing-target — reactour has no `skipMissingElement`. We instead
//      filter the step list at start: an anchored step whose target isn't in the
//      DOM and can't be reached by a route (e.g. Run, which only exists inside an
//      open workspace) is dropped, so the tour degrades gracefully.
//   3. A localStorage "seen" flag so it auto-starts only on first run.
//
// The hook must be called inside <TourProvider>; App is wrapped by it in main.tsx.

import { useCallback, useEffect, useMemo, useRef } from "react";
import { useNavigate } from "react-router-dom";
import { useTour } from "@reactour/tour";
import { getTourSteps, type TourStep } from "./steps";
import { toReactourStep } from "./reactourStep";

/** Bump the version suffix to re-show the tour to everyone after a big change. */
const SEEN_KEY = "nano.tour.v1.seen";

/** Delay before first-run auto-start, so the initial route + sidebar render. */
const AUTOSTART_DELAY_MS = 800;

function hasSeenTour(): boolean {
  try {
    return localStorage.getItem(SEEN_KEY) === "1";
  } catch {
    return false;
  }
}

function markTourSeen(): void {
  try {
    localStorage.setItem(SEEN_KEY, "1");
  } catch {
    /* private mode / storage disabled — just re-show next time */
  }
}

/**
 * Keep a step only if it can actually be shown: unanchored (centered) steps and
 * routed steps always qualify; an anchored step with no route survives only if
 * its target is already in the DOM. This is reactour's missing `skipMissingElement`.
 */
function isStepReachable(step: TourStep): boolean {
  if (!step.selector) return true;
  if (step.route) return true;
  return document.querySelector(step.selector) !== null;
}

export interface UseProductTourOptions {
  /** Auto-start once on first run (when the "seen" flag is unset). */
  autoStart?: boolean;
}

export interface ProductTour {
  /** Start (or restart) the tour immediately. */
  startTour: () => void;
  /** Clear the "seen" flag so the tour auto-starts again next load. */
  resetTour: () => void;
  /** Whether the current build profile has a tour defined. */
  hasTour: boolean;
}

export function useProductTour(
  options: UseProductTourOptions = {},
): ProductTour {
  const { autoStart = false } = options;
  const navigate = useNavigate();
  const { setIsOpen, setCurrentStep, setSteps, currentStep, isOpen } =
    useTour();

  const allSteps = useMemo(() => getTourSteps(), []);
  // The filtered list actually shown; kept in a ref so the route-sync effect can
  // map reactour's numeric currentStep back to our TourStep without re-renders.
  const visibleStepsRef = useRef<TourStep[]>([]);

  const startTour = useCallback(() => {
    const visible = allSteps.filter(isStepReachable);
    if (visible.length === 0) return;
    visibleStepsRef.current = visible;
    setSteps?.(visible.map(toReactourStep));
    const first = visible[0];
    if (first.route) navigate(first.route);
    setCurrentStep(0);
    setIsOpen(true);
  }, [allSteps, navigate, setCurrentStep, setIsOpen, setSteps]);

  const resetTour = useCallback(() => {
    try {
      localStorage.removeItem(SEEN_KEY);
    } catch {
      /* ignore */
    }
  }, []);

  // Route navigation: reactour advances currentStep; we move the router to match.
  useEffect(() => {
    if (!isOpen) return;
    const step = visibleStepsRef.current[currentStep];
    if (step?.route) navigate(step.route);
  }, [currentStep, isOpen, navigate]);

  // Persist the "seen" flag when the tour closes (isOpen true -> false).
  const wasOpen = useRef(false);
  useEffect(() => {
    if (wasOpen.current && !isOpen) markTourSeen();
    wasOpen.current = isOpen;
  }, [isOpen]);

  // First-run auto-start. StrictMode mounts effects twice, so we rely on the
  // timeout + cleanup (not a fired-once ref, which StrictMode's mount/unmount/
  // remount would leave permanently disarmed); the "seen" flag stops re-shows.
  useEffect(() => {
    if (!autoStart || hasSeenTour() || allSteps.length === 0) return;
    const id = window.setTimeout(startTour, AUTOSTART_DELAY_MS);
    return () => window.clearTimeout(id);
  }, [autoStart, allSteps.length, startTour]);

  return { startTour, resetTour, hasTour: allSteps.length > 0 };
}
