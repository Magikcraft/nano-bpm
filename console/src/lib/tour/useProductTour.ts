// The React seam for guided journeys (ADR 0049).
//
// Everything hard lives elsewhere and is unit-tested without a browser: the
// contract (types.ts), resolution (registry.ts), persistence (state.ts), context
// assembly (context.ts), deep links (deepLink.ts) and the driver.js adapter
// (runner.ts). This hook only wires them to React — the router, one data fetch,
// and the rail button's label.
//
// It replaces the spike's profile-keyed step list: the build profile is now just
// a filter over the journey registry, because `studio` vs `observe` is a
// build-time split while the users who matter most all land in the same studio
// build wanting different first sessions.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { listProjects } from "../../gen";
import { CONSOLE_PROFILE } from "../profile";
import { buildContext } from "./context";
import type { ProjectsSnapshot } from "./context";
import { readTourParam, stripTourParam } from "./deepLink";
import { getJourney, journeysFor } from "./registry";
import { createJourneyRunner } from "./runner";
import {
  activeJourneyId,
  hasCompleted,
  readState,
  recordFor,
  resetState,
  withJourney,
  writeState,
  type TourState,
} from "./state";
import type { Journey, JourneyEvent, TourContext } from "./types";
// Registers the built-in journeys by import side effect. A journey slice adds its
// own module import here; the registry itself keeps no central list, so parallel
// slices never contend on one file.
import { overviewJourneyId } from "./journeys/overview";

/**
 * Delay before an auto-started journey opens, letting the initial route and the
 * sidebar render before the first anchored step is highlighted.
 */
const AUTOSTART_DELAY_MS = 800;

/**
 * How long a `listProjects()` context snapshot is reused before refetching.
 *
 * Kept under the runner's 2s verify cadence, so a handoff poll tick still reads
 * reasonably fresh state, while the burst of `getContext()` calls around a single
 * step transition share one request instead of each hitting the backend.
 */
const SNAPSHOT_TTL_MS = 1500;

export interface UseProductTourOptions {
  /**
   * Auto-start the profile's overview journey once, on first run.
   *
   * TRANSITIONAL. ADR 0049 replaces auto-start with the journey picker on the
   * Projects/Topology empty state (#411): a first-timer should choose one of the
   * three real journeys, not be dropped into an orientation tour that serves
   * none of them. Until that picker exists, auto-starting the overview preserves
   * the discoverability the console has today — removing it first would ship a
   * window with no onboarding at all.
   *
   * #411 flips this to `false` (and may then delete it) once the picker lands.
   */
  autoStart?: boolean;
}

export interface ProductTour {
  /** Start a specific journey by id. Unknown ids are ignored. */
  startJourney: (journeyId: string) => void;
  /** Start the overview for this profile — what the rail's "Take a tour" runs. */
  startTour: () => void;
  /** Resume the interrupted journey, if there is one. */
  resumeJourney: () => void;
  /** Journeys offerable in this profile right now (drives the picker in #411). */
  availableJourneys: Journey[];
  /** The interrupted journey, when one was left mid-flight. */
  activeJourney: Journey | undefined;
  /**
   * Whether a journey is on screen right now.
   *
   * Distinct from `activeJourney`, which stays set while a journey is merely
   * *unfinished*. Callers offering a "Resume" affordance want both: there is
   * something to resume, and it is not already showing.
   */
  isRunning: boolean;
  /** Forget all journey state so onboarding can be seen again. */
  resetTour: () => void;
}

export function useProductTour(
  options: UseProductTourOptions = {},
): ProductTour {
  const { autoStart = false } = options;
  const navigate = useNavigate();
  const location = useLocation();

  // Kept in a ref so the runner reads the live pathname without the callbacks
  // churning on every navigation.
  const pathnameRef = useRef(location.pathname);
  pathnameRef.current = location.pathname;

  const [activeId, setActiveId] = useState<string | undefined>(() =>
    activeJourneyId(readState()),
  );
  const [available, setAvailable] = useState<Journey[]>(() =>
    journeysFor(CONSOLE_PROFILE),
  );
  const [running, setRunning] = useState(false);

  const persist = useCallback((next: TourState) => {
    writeState(next);
    setActiveId(activeJourneyId(next));
  }, []);

  /**
   * Fetch a context snapshot. One `listProjects()` call supplies the base; any
   * registered context source then merges its own fields in. A failed fetch still
   * yields a usable context whose runtime flags read false, so a Run step
   * repairs into an install hint rather than promising something unverified.
   *
   * The `listProjects()` payload (projects + runtime flags + template menu +
   * extensions) is cached for a short window and its in-flight request is shared,
   * so the several `getContext()` calls that cluster around one step transition
   * (resolve, render, finish) — and the handoff `verify` poll — do not each fire
   * a fresh, potentially heavy request. The window is under the 2s verify cadence,
   * so a poll tick still re-reads reasonably fresh state. #404 is the structural
   * fix: once the consumer panel polls, verify reads its data via a registered
   * context source instead of driving its own fetch.
   */
  const snapshotCache = useRef<{
    at: number;
    snapshot: ProjectsSnapshot | null;
  } | null>(null);
  const snapshotInflight = useRef<Promise<ProjectsSnapshot | null> | null>(
    null,
  );
  const getContext = useCallback(async (): Promise<TourContext> => {
    const now = Date.now();
    const cached = snapshotCache.current;
    let snapshot: ProjectsSnapshot | null;
    if (cached && now - cached.at < SNAPSHOT_TTL_MS) {
      snapshot = cached.snapshot;
    } else {
      snapshot = await (snapshotInflight.current ??= (async () => {
        try {
          return (await listProjects({ throwOnError: true })).data ?? null;
        } catch {
          return null;
        } finally {
          snapshotInflight.current = null;
        }
      })());
      snapshotCache.current = { at: Date.now(), snapshot };
    }
    return buildContext({
      profile: CONSOLE_PROFILE,
      route: pathnameRef.current,
      snapshot,
    });
  }, []);

  const runner = useMemo(
    () =>
      createJourneyRunner({
        navigate: (route) => navigate(route),
        getRoute: () => pathnameRef.current,
        getContext,
        onEvent: (event: JourneyEvent) => {
          // Analytics sink. Step ids are stable, so per-step drop-off is
          // measurable the moment something listens here; this is the one
          // documented place to attach it.
          if (import.meta.env.DEV) console.debug("[tour]", event);
          if (event.type === "start") setRunning(true);
          if (event.type === "complete" || event.type === "abandon") {
            setRunning(false);
          }
        },
        onStep: (journeyId, authoredIndex) => {
          persist(
            withJourney(readState(), journeyId, {
              status: "active",
              stepIndex: authoredIndex,
            }),
          );
        },
        onFinish: (journeyId, outcome, detail) => {
          persist(
            withJourney(readState(), journeyId, {
              status: outcome === "complete" ? "completed" : "abandoned",
              stepIndex: outcome === "complete" ? 0 : detail.authoredIndex,
              ...(outcome === "complete" ? { completedAt: Date.now() } : {}),
            }),
          );
        },
      }),
    [getContext, navigate, persist],
  );

  const startJourney = useCallback(
    (journeyId: string) => {
      const journey = getJourney(journeyId);
      if (!journey) return;
      const record = recordFor(readState(), journeyId);
      const from = record.status === "active" ? record.stepIndex : 0;
      void runner.start(journey, from);
    },
    [runner],
  );

  const startTour = useCallback(() => {
    startJourney(overviewJourneyId(CONSOLE_PROFILE));
  }, [startJourney]);

  const resumeJourney = useCallback(() => {
    if (activeId) startJourney(activeId);
  }, [activeId, startJourney]);

  const resetTour = useCallback(() => {
    resetState();
    setActiveId(undefined);
  }, []);

  // Recompute which journeys are offerable once real context exists: a
  // journey-level precondition can only be evaluated against a snapshot, and
  // until it arrives we optimistically list everything for the profile.
  useEffect(() => {
    let cancelled = false;
    void getContext().then((ctx) => {
      if (!cancelled) setAvailable(journeysFor(CONSOLE_PROFILE, ctx));
    });
    return () => {
      cancelled = true;
    };
  }, [getContext]);

  // `?tour=<id>` beats auto-start: a deep link is explicit intent, so it runs
  // even for a journey already completed. The param is stripped afterwards so a
  // refresh does not restart it.
  const deepLinked = useRef(false);
  useEffect(() => {
    if (deepLinked.current) return;
    const requested = readTourParam(window.location.search);
    if (!requested) return;
    deepLinked.current = true;
    const url = `${window.location.pathname}${window.location.search}${window.location.hash}`;
    const cleaned = stripTourParam(url);
    if (cleaned !== url) window.history.replaceState(null, "", cleaned);
    startJourney(requested);
  }, [startJourney]);

  // First-run auto-start (transitional — see UseProductTourOptions.autoStart).
  // An abandoned overview is not re-forced: someone who dismissed it once has
  // answered the question.
  useEffect(() => {
    if (!autoStart || deepLinked.current) return;
    const overview = overviewJourneyId(CONSOLE_PROFILE);
    const state = readState();
    if (hasCompleted(state, overview)) return;
    if (recordFor(state, overview).status === "abandoned") return;
    const id = window.setTimeout(
      () => startJourney(overview),
      AUTOSTART_DELAY_MS,
    );
    return () => window.clearTimeout(id);
  }, [autoStart, startJourney]);

  // Tear down if the app unmounts mid-journey. dispose() (not stop()) so an
  // unmount, HMR reload or StrictMode remount is not recorded as an abandon — an
  // interrupted journey should stay resumable.
  useEffect(() => () => runner.dispose(), [runner]);

  return {
    startJourney,
    startTour,
    resumeJourney,
    availableJourneys: available,
    activeJourney: activeId ? getJourney(activeId) : undefined,
    isRunning: running,
    resetTour,
  };
}
