// Journey 1 — a headless, Camunda-compatible engine for local development.
//
// The shortest journey, and the one whose design point is RESTRAINT. This user is
// headless by definition: they are replacing a Docker + Zeebe + Elasticsearch
// stack with one local binary, and the console is a debugger they open when
// something breaks — never part of their loop. So the journey is three steps (a
// fourth only when there is something to see), and it ends by getting out of the
// way. It is deliberately NOT padded, and deliberately has no `nextJourneys`:
// respecting a headless user's time is the whole point, so do not "helpfully" add
// an onward path here.
//
// ADR 0049 §6 · Epic #406 · Blocked-by #407 (landed).

import { registerJourney } from "../registry.ts";
import { registerContextSource } from "../context.ts";
import { hasTraces } from "../preconditions.ts";
import { TOUR_ANCHOR, tourSelector } from "../tourAnchors.ts";
import type { Journey } from "../types";
import {
  localdevSucceeded,
  markBaseUrlCopied,
  v2BaseUrl,
} from "./localdev-progress.ts";

/**
 * The compatibility-subset boundary (#416, merged as `docs/camunda-compatibility.md`).
 * Journey 1 originally shipped without this link rather than fabricate one; now
 * that the doc exists it is wired in. Linked at the same canonical GitHub docs
 * location the console's other in-app doc links use.
 */
const COMPAT_DOC_URL =
  "https://github.com/jwulf/nano-bpm/blob/main/docs/camunda-compatibility.md";

/**
 * Supply `traceCount` so the optional trace step can gate on `hasTraces`.
 *
 * `limit: 1` on purpose: the ONLY consumer is the `hasTraces` `> 0` gate, so all
 * this needs to answer is "are there any traces?", not "how many". A short TTL
 * cache keeps the burst of context reads around a step transition — and any other
 * journey's context builds, since this source is global — down to one request.
 */
const TRACES_TTL_MS = 5000;
let tracesCache: { at: number; count: number } | null = null;

registerContextSource(async () => {
  const now = Date.now();
  if (tracesCache && now - tracesCache.at < TRACES_TTL_MS) {
    return { traceCount: tracesCache.count };
  }
  try {
    // Imported lazily so this journey module stays loadable from a Node unit
    // test without pulling in the generated HTTP client.
    const { listTraces } = await import("../../../gen");
    const res = await listTraces({ query: { limit: 1 }, throwOnError: true });
    const count = res.data?.length ?? 0;
    tracesCache = { at: now, count };
    return { traceCount: count };
  } catch {
    // A failing source contributes nothing; `hasTraces` then reads no traces and
    // the optional step skips — the correct outcome on a fresh, empty engine.
    return {};
  }
});

export const localdev: Journey = {
  id: "localdev",
  title: "Local Camunda-compatible engine",
  blurb:
    "Replace Docker + Zeebe + ES with one local binary. Your C8 client is unchanged.",
  // Offered on the full IDE build AND the lean operator build: an operator on the
  // observe build benefits from steps 2–4 just as much, and this is the one
  // journey whose audience is genuinely profile-agnostic.
  profiles: ["studio", "observe"],
  successEvent: () => localdevSucceeded(),
  steps: [
    {
      kind: "note",
      id: "already-running",
      title: "It's already running",
      body: "The engine is up, a <code>demo</code> process is already deployed (<code>c8ctl nano start</code> pre-deploys it), and there is nothing to install or configure. No Docker, no Zeebe broker, no Elasticsearch.",
    },
    {
      kind: "handoff",
      id: "point-your-client",
      title: "Point your client at it",
      // The v2 base URL, derived from the live origin so a non-8080 port is
      // reflected honestly. `verify` is intentionally omitted — the console
      // cannot watch your terminal, so this self-reports via "I've done it".
      copy: v2BaseUrl(),
      copyLabel: "Copy base URL",
      // Latch "the base URL was taken" (ADR 0049 §4, journey 1's success signal)
      // from the real copy here in the journey flow — not only from the durable
      // Explorer affordance, which a user following the tour need never touch.
      onCopied: markBaseUrlCopied,
      body: 'Your existing Camunda 8 client works unchanged — <strong>this is the only line you change</strong>. Explore the API offline at <a href="/swagger" target="_blank" rel="noopener noreferrer">/swagger</a>.',
    },
    {
      kind: "spotlight",
      id: "debugger",
      route: "/explorer",
      selector: tourSelector(TOUR_ANCHOR.explorerInstances),
      title: "When something breaks, look here",
      body: `This is your debugger: variables, jobs, incidents and the BPMN XML per instance. Throw it all away any time with <code>c8ctl nano stop --purge</code> — it is disposable by design. What's in and out of the compatible subset: <a href="${COMPAT_DOC_URL}" target="_blank" rel="noopener noreferrer">the compatibility boundary</a>.`,
      side: "right",
      align: "start",
    },
    {
      // Optional: skips cleanly on a fresh install (no traces captured yet), and
      // appears once there is something worth seeing. Spotlights the Traces rail
      // item — auto-anchored by App.tsx — so this journey does not reach into a
      // view another slice owns.
      kind: "spotlight",
      id: "traces",
      route: "/traces",
      selector: tourSelector(TOUR_ANCHOR.tracesNav),
      optional: true,
      precondition: hasTraces,
      title: "Why was that run slow?",
      body: "When you need the timeline of a run — every step, with timings — Traces has it. Skipped until you have captured some.",
      side: "right",
      align: "start",
    },
  ],
  // No `nextJourneys`, deliberately: see the file header. A headless user's time
  // is respected by ending here.
};

registerJourney(localdev);
