// Persisted journey state: which journeys were completed, and where an
// in-progress one got to.
//
// The spike stored a single boolean ("has the user seen the tour?"). That cannot
// express the two things ADR 0049 needs: several journeys with independent
// outcomes, and resuming one that was interrupted — journeys span routes, and a
// user who reloads mid-journey should be offered their place back rather than
// silently losing it.
//
// Every access is wrapped: localStorage throws in private mode and when storage
// is disabled, and onboarding state is never worth breaking the app over. A
// failed read degrades to "nothing seen yet", a failed write to "not remembered".

export const STORAGE_KEY = "nano.tour.v2";

/** The spike's flag. Read once, to migrate, then left alone. */
export const LEGACY_SEEN_KEY = "nano.tour.v1.seen";

/** Id of the studio overview journey — the migration target for the v1 flag. */
export const OVERVIEW_JOURNEY_ID = "overview";

export type JourneyStatus = "unseen" | "active" | "completed" | "abandoned";

export interface JourneyRecord {
  status: JourneyStatus;
  /** Index within the *authored* step list, so a resume survives step edits. */
  stepIndex: number;
  completedAt?: number;
}

export interface TourState {
  version: 2;
  /** Which journey the user picked, when they chose one explicitly. */
  persona?: string;
  /**
   * Whether the startup persona panel (#464) auto-opens when the console is
   * opened. Absent means "show" — the panel is on by default; the user turns it
   * off with its "Show at startup" checkbox, which persists `false` here.
   */
  showStartupPanel?: boolean;
  journeys: Record<string, JourneyRecord>;
}

export function emptyState(): TourState {
  return { version: 2, journeys: {} };
}

type Storage = Pick<globalThis.Storage, "getItem" | "setItem" | "removeItem">;

/** Injectable for tests; defaults to localStorage when available. */
function defaultStorage(): Storage | null {
  try {
    return globalThis.localStorage ?? null;
  } catch {
    return null;
  }
}

/**
 * Read state, migrating the spike's v1 flag on first read.
 *
 * Migration rule: a truthy `nano.tour.v1.seen` means the user already saw the
 * overview, so record it completed — an upgrader must not be re-toured. It is
 * only applied when there is no v2 record for the overview yet, so a user who
 * has since replayed it keeps their newer state.
 */
export function readState(
  storage: Storage | null = defaultStorage(),
): TourState {
  if (!storage) return emptyState();

  let state = emptyState();
  try {
    const raw = storage.getItem(STORAGE_KEY);
    if (raw) {
      const parsed = JSON.parse(raw) as unknown;
      if (isTourState(parsed)) state = parsed;
    }
  } catch {
    // Corrupt or unreadable — start clean rather than crash onboarding.
  }

  try {
    const legacy = storage.getItem(LEGACY_SEEN_KEY);
    if (legacy && !state.journeys[OVERVIEW_JOURNEY_ID]) {
      state = {
        ...state,
        journeys: {
          ...state.journeys,
          [OVERVIEW_JOURNEY_ID]: { status: "completed", stepIndex: 0 },
        },
      };
    }
  } catch {
    // No legacy flag readable; nothing to migrate.
  }

  return state;
}

export function writeState(
  state: TourState,
  storage: Storage | null = defaultStorage(),
): void {
  if (!storage) return;
  try {
    storage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch {
    // Private mode / quota — the journey still works, it just is not remembered.
  }
}

const JOURNEY_STATUSES: ReadonlySet<string> = new Set([
  "unseen",
  "active",
  "completed",
  "abandoned",
]);

/** A record read from storage is only trusted if it is shaped like one. */
function isJourneyRecord(v: unknown): v is JourneyRecord {
  if (typeof v !== "object" || v === null) return false;
  const o = v as Record<string, unknown>;
  return (
    typeof o.status === "string" &&
    JOURNEY_STATUSES.has(o.status) &&
    typeof o.stepIndex === "number"
  );
}

export function recordFor(state: TourState, journeyId: string): JourneyRecord {
  const rec = state.journeys[journeyId];
  // isTourState only guarantees `journeys` is an object, not that each record is
  // well-formed. A record hand-corrupted in localStorage (e.g. a string) must
  // degrade to "unseen" rather than let a caller throw on `record.status` —
  // onboarding state is never worth breaking the app over.
  return isJourneyRecord(rec) ? rec : { status: "unseen", stepIndex: 0 };
}

/** Merge one journey's record, returning a new state (never mutates). */
export function withJourney(
  state: TourState,
  journeyId: string,
  patch: Partial<JourneyRecord>,
): TourState {
  const next = { ...recordFor(state, journeyId), ...patch };
  return { ...state, journeys: { ...state.journeys, [journeyId]: next } };
}

/** The journey to offer resuming, if any. */
export function activeJourneyId(state: TourState): string | undefined {
  for (const [id, rec] of Object.entries(state.journeys)) {
    if (rec.status === "active") return id;
  }
  return undefined;
}

export function hasCompleted(state: TourState, journeyId: string): boolean {
  return recordFor(state, journeyId).status === "completed";
}

/**
 * Whether the startup persona panel should auto-open (#464). Default is `true`
 * — absent means show — so a fresh install is greeted by the panel, and only an
 * explicit `false` (the user unchecking "Show at startup") suppresses it.
 */
export function startupPanelEnabled(state: TourState): boolean {
  return state.showStartupPanel !== false;
}

/** Persist the "Show at startup" preference, returning the new state. */
export function withStartupPanel(state: TourState, show: boolean): TourState {
  return { ...state, showStartupPanel: show };
}

/** Clear all journey state — the "show me the onboarding again" lever. */
export function resetState(storage: Storage | null = defaultStorage()): void {
  if (!storage) return;
  try {
    storage.removeItem(STORAGE_KEY);
    // Drop the legacy flag too, or the migration would immediately re-mark the
    // overview completed and the reset would appear not to have worked.
    storage.removeItem(LEGACY_SEEN_KEY);
  } catch {
    /* nothing to do */
  }
}

function isTourState(v: unknown): v is TourState {
  if (typeof v !== "object" || v === null) return false;
  const o = v as Record<string, unknown>;
  return (
    o.version === 2 && typeof o.journeys === "object" && o.journeys !== null
  );
}
