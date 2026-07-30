// First-run product tour, driver.js spike (issue #393).
//
// Why driver.js: the journey spans react-router routes and targets elements
// that mount asynchronously over CSS-transformed canvases (bpmn-js/monaco). An
// imperative runner we drive in lockstep with the router — plus driver.js's
// built-in `waitForElement` (wait for a target to appear) and
// `skipMissingElement` (skip absent targets) — fits that better than a
// declarative step list fighting lazy targets. The popover is themed with the
// app's own CSS tokens (src/lib/tour/tour.css) so it tracks dark/light mode.
//
// The hook owns three things: (1) a profile-aware step list, (2) route
// navigation between steps (driver.js can't touch the router), and (3) a
// localStorage "seen" flag so it auto-starts only on first run.

import { useCallback, useEffect, useMemo, useRef } from "react";
import { useNavigate } from "react-router-dom";
import { driver, type Driver, type DriveStep } from "driver.js";
import "driver.js/dist/driver.css";
import "./tour.css";
import { getTourSteps, type TourStep } from "./steps";

/** Bump the version suffix to re-show the tour to everyone after a big change. */
const SEEN_KEY = "nano.tour.v1.seen";

/** How long driver.js waits for a step's target to mount before skipping it. */
const WAIT_FOR_ELEMENT_MS = 5000;

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
  const driverRef = useRef<Driver | null>(null);
  const steps = useMemo(() => getTourSteps(), []);

  // Navigate to a step's route (if any) before it is shown; driver.js's
  // waitForElement then handles the async mount on the new route.
  const goToRoute = useCallback(
    (step: TourStep | undefined) => {
      if (step?.route) navigate(step.route);
    },
    [navigate],
  );

  const toDriveStep = useCallback(
    (step: TourStep, index: number): DriveStep => ({
      element: step.selector,
      popover: {
        title: step.title,
        description: step.body,
        side: step.side,
        align: step.align,
        // We own navigation, so we override next/prev to move the router in
        // step with driver.js. driverRef.current is populated by the time a
        // button is clicked. On the last step, moveNext() ends the tour.
        onNextClick: () => {
          goToRoute(steps[index + 1]);
          driverRef.current?.moveNext();
        },
        onPrevClick: () => {
          goToRoute(steps[index - 1]);
          driverRef.current?.movePrevious();
        },
      },
    }),
    [goToRoute, steps],
  );

  const startTour = useCallback(() => {
    if (steps.length === 0) return;
    if (driverRef.current?.isActive()) return;

    const instance = driver({
      showProgress: true,
      allowClose: true,
      overlayOpacity: 0.55,
      stagePadding: 6,
      stageRadius: 8,
      waitForElement: WAIT_FOR_ELEMENT_MS,
      skipMissingElement: true,
      popoverClass: "nano-tour",
      nextBtnText: "Next",
      prevBtnText: "Back",
      doneBtnText: "Done",
      steps: steps.map(toDriveStep),
      onDestroyed: () => {
        markTourSeen();
        driverRef.current = null;
      },
    });
    driverRef.current = instance;
    goToRoute(steps[0]);
    instance.drive();
  }, [steps, toDriveStep, goToRoute]);

  const resetTour = useCallback(() => {
    try {
      localStorage.removeItem(SEEN_KEY);
    } catch {
      /* ignore */
    }
  }, []);

  // First-run auto-start. The short delay lets the initial route and its
  // sidebar render before we highlight the first anchored step.
  useEffect(() => {
    if (!autoStart || steps.length === 0 || hasSeenTour()) return;
    const id = window.setTimeout(startTour, 800);
    return () => window.clearTimeout(id);
  }, [autoStart, steps.length, startTour]);

  // Tear down if the app unmounts mid-tour.
  useEffect(
    () => () => {
      driverRef.current?.destroy();
      driverRef.current = null;
    },
    [],
  );

  return { startTour, resetTour, hasTour: steps.length > 0 };
}
