// Journey 1 — headless Camunda-compatible engine for local development (ADR 0049 §6).
//
// The shortest journey, and the one whose design point is RESTRAINT. This user is
// headless by definition: they run Nano instead of `docker compose up` with Zeebe
// + Elasticsearch, point an existing Camunda 8 client at it, and never think about
// the console again until something breaks. So the journey does exactly three
// things — tells them nothing needs installing, hands them the one line they
// change, and shows them where to look when a run misbehaves — then gets out of
// the way. An optional fourth step appears only once there is a trace to look at.
//
// This module is deliberately runtime-import-free at the top level (only
// `registerJourney` / `registerContextSource`, which are pure map/set adds) so a
// Node unit test can import it for the journey guards without pulling in the API
// client or touching `window`. Everything that reads `window`, `localStorage` or
// the network is inside a function body, invoked only in the browser.

import { registerJourney } from "../registry.ts";
import { registerContextSource } from "../context.ts";
import { hasTraces } from "../preconditions.ts";
import { TOUR_ANCHOR, tourSelector } from "../tourAnchors.ts";
import type { Journey, Predicate, TourContext } from "../types";

export const LOCALDEV_JOURNEY_ID = "localdev";

/** The compatibility-subset boundary published by #416 (audit doc, tracked in-repo). */
const COMPAT_SUBSET_URL =
  "https://github.com/nanobpm/nano-bpm/blob/main/docs/camunda-compatibility.md";

// ---------------------------------------------------------------------------
// The one line a Camunda user changes.
// ---------------------------------------------------------------------------

/**
 * The Camunda-compatible REST base for an origin. Pure and origin-relative — the
 * port is whatever the console is actually served on, never a hardcoded 8080, so
 * a node brought up on a different port hands the user the URL that truly works.
 */
export function v2BaseUrl(origin: string): string {
  return `${origin.replace(/\/+$/, "")}/v2`;
}

/** The offline Swagger UI for an origin (the live source of truth for the subset). */
export function swaggerUrl(origin: string): string {
  return `${origin.replace(/\/+$/, "")}/swagger`;
}

/**
 * The live origin, or a placeholder when there is no `window` (a Node unit test
 * importing this module for the journey guards). The placeholder is never shown
 * to a user: in the browser this resolves to the real served origin.
 */
function currentOrigin(): string {
  return typeof window !== "undefined" && window.location?.origin
    ? window.location.origin
    : "http://127.0.0.1:8080";
}

// ---------------------------------------------------------------------------
// Outcome signals.
//
// "Finished the steps" and "achieved the outcome" are different numbers, and the
// contract records both (ADR 0049). For this journey the outcome is: the user
// took the base URL for their client AND visited Explorer at least once — i.e.
// they now know the one line to change and where to debug. Both facts are
// recorded by the durable Explorer affordance, persisted so the answer survives
// the reload a resumable journey allows, and surfaced into `scratch` by a context
// source so `successEvent` stays a pure predicate over the snapshot.
// ---------------------------------------------------------------------------

const COPIED_KEY = "nano.tour.localdev.baseUrlCopied";
const EXPLORER_KEY = "nano.tour.localdev.explorerReached";

/** Scratch keys the success predicate reads; also the source's output shape. */
export const SCRATCH_BASE_URL_COPIED = "localdev.baseUrlCopied";
export const SCRATCH_EXPLORER_REACHED = "localdev.explorerReached";

function safeStorage(): Storage | null {
  try {
    return globalThis.localStorage ?? null;
  } catch {
    return null;
  }
}

function setFlag(key: string): void {
  try {
    safeStorage()?.setItem(key, "1");
  } catch {
    // A signal we could not persist just means the outcome reads as not-yet
    // achieved — onboarding is never worth throwing over.
  }
}

function readFlag(key: string): boolean {
  try {
    return safeStorage()?.getItem(key) === "1";
  } catch {
    return false;
  }
}

/** Record that the user took the v2 base URL (from the Explorer affordance). */
export function markBaseUrlCopied(): void {
  setFlag(COPIED_KEY);
}

/** Record that the user reached Explorer (called on the view mounting). */
export function markExplorerReached(): void {
  setFlag(EXPLORER_KEY);
}

/** Test seam / "replay this journey" lever: forget both outcome signals. */
export function resetLocaldevSignals(): void {
  try {
    const s = safeStorage();
    s?.removeItem(COPIED_KEY);
    s?.removeItem(EXPLORER_KEY);
  } catch {
    /* nothing to do */
  }
}

/**
 * `successEvent`: the base URL was taken AND Explorer was reached at least once.
 * Pure over the snapshot — the two booleans arrive via the scratch source below,
 * so this is unit-testable by constructing a context with those scratch keys.
 */
export const localdevSucceeded: Predicate = (ctx) =>
  ctx.scratch[SCRATCH_BASE_URL_COPIED] === true &&
  ctx.scratch[SCRATCH_EXPLORER_REACHED] === true;

// ---------------------------------------------------------------------------
// Context sources (registered once, at module load — app-wide, because the
// optional trace step's precondition is resolved when the journey STARTS, which
// can be from any route, not while Explorer happens to be mounted).
// ---------------------------------------------------------------------------

/** Surface the persisted outcome signals into `scratch` for `successEvent`. */
function signalsSource(ctx: TourContext): Partial<TourContext> {
  return {
    scratch: {
      ...ctx.scratch,
      [SCRATCH_BASE_URL_COPIED]: readFlag(COPIED_KEY),
      [SCRATCH_EXPLORER_REACHED]: readFlag(EXPLORER_KEY),
    },
  };
}

// The trace count only gates one optional step, so it is cheap and generously
// cached: at most one lightweight `/traces` probe every few seconds while a tour
// is open, shared across whatever journey asks. Read via raw fetch rather than
// the generated client so this module stays import-safe for the Node guards.
const TRACE_COUNT_TTL_MS = 5000;
let traceCountCache: { at: number; count: number } | null = null;
let traceCountInflight: Promise<number> | null = null;

async function fetchTraceCount(): Promise<number> {
  if (typeof fetch !== "function") return 0;
  try {
    const res = await fetch("/console/api/traces?limit=1", {
      headers: { accept: "application/json" },
    });
    if (!res.ok) return 0;
    const body: unknown = await res.json();
    return Array.isArray(body) ? body.length : 0;
  } catch {
    return 0;
  }
}

async function traceCountSource(): Promise<Partial<TourContext>> {
  const now = Date.now();
  if (traceCountCache && now - traceCountCache.at < TRACE_COUNT_TTL_MS) {
    return { traceCount: traceCountCache.count };
  }
  const count = await (traceCountInflight ??= fetchTraceCount().finally(() => {
    traceCountInflight = null;
  }));
  traceCountCache = { at: Date.now(), count };
  return { traceCount: count };
}

// ---------------------------------------------------------------------------
// The journey.
// ---------------------------------------------------------------------------

const origin = currentOrigin();

export const localdev: Journey = {
  id: LOCALDEV_JOURNEY_ID,
  title: "Run it as a headless engine",
  blurb:
    "Replace Docker + Zeebe + Elasticsearch for local dev — point your existing Camunda 8 client at one URL.",
  persona:
    "run a local Camunda-compatible engine (no Docker, no Zeebe, no Elasticsearch)",
  // Profile-agnostic on purpose: an operator on the lean `observe` build benefits
  // from steps 2–4 just as much as a maker does. It is the only one of the three
  // journeys where the persona does not depend on the build.
  profiles: ["studio", "observe"],
  successEvent: localdevSucceeded,
  // No `nextJourneys`, deliberately: respecting a headless user's time IS the
  // design. This user came to replace infrastructure, not to be onboarded — do
  // not "helpfully" chain another journey onto the end of this one.
  steps: [
    {
      kind: "note",
      id: "already-running",
      title: "Nothing to install",
      body: "The engine is already running and a <code>demo</code> process is already deployed — <code>c8ctl nano start</code> pre-deploys it. There is no broker to configure, no Elasticsearch, no Docker. This journey is three steps and then it leaves you alone.",
    },
    {
      kind: "handoff",
      id: "v2-base-url",
      title: "The one line you change",
      // `verify` is intentionally absent: the console cannot watch your client
      // connect, so this is self-reported ("I've done it").
      copy: v2BaseUrl(origin),
      copyLabel: "Copy base URL",
      // Latch "the base URL was taken" (ADR 0049 §4, journey 1's success signal)
      // from the real copy here in the journey flow — the Explorer durable
      // affordance is an alternate path a user following the tour need never
      // touch, so relying on it alone leaves `successEvent` unreachable.
      onCopied: markBaseUrlCopied,
      body: `Your existing Camunda 8 client works unchanged — this is the only line you change. Point it at the base URL above and everything else stays the same. The exact spec this binary serves is at its offline <a href="${swaggerUrl(
        origin,
      )}" target="_blank" rel="noreferrer noopener">Swagger UI</a>.`,
    },
    {
      kind: "spotlight",
      id: "explorer-debug",
      route: "/explorer",
      selector: tourSelector(TOUR_ANCHOR.explorerInspect),
      title: "Where you look when something breaks",
      side: "right",
      align: "start",
      // #416 has landed, so the subset boundary is a real link. If it is ever
      // unpublished, drop the anchor rather than link something that 404s.
      body: `This is your debugger: per-instance variables, jobs, incidents and the BPMN XML for any running instance. You never live here — you visit when a run misbehaves. And it is disposable: <code>c8ctl nano stop --purge</code> wipes every trace of state for a clean slate. Exactly what runs against this engine is bounded by <a href="${COMPAT_SUBSET_URL}" target="_blank" rel="noreferrer noopener">the supported Camunda subset</a>.`,
    },
    {
      kind: "spotlight",
      id: "traces-why-slow",
      // Optional, and skipped on a fresh install: an empty trace table teaches
      // nothing. Appears once at least one instance has run.
      optional: true,
      precondition: hasTraces,
      selector: tourSelector(TOUR_ANCHOR.tracesNav),
      title: "When you need to know why",
      side: "right",
      align: "start",
      body: "Traces record every step of every past run — reach for this when you need to see <em>why</em> a run was slow or where it stalled, not just that it finished.",
    },
  ],
};

registerJourney(localdev);
registerContextSource(signalsSource);
registerContextSource(traceCountSource);
