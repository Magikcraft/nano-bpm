// Guard tests for the registered journeys (ADR 0049).
//
// These carry forward the spike's step-list guards — which caught the two ways a
// tour silently rots — and add the journey-level invariants:
//
//   - every spotlight selector derives from a KNOWN data-tour anchor, so a
//     renamed attribute cannot leave a step pointing at nothing;
//   - a journey only targets anchors that actually render in its profile (an
//     observe journey aiming at the studio-only Projects nav would spotlight
//     empty space);
//   - journeys stay short (ADR 0049 caps them at five steps — a journey needing
//     a detour is split, not padded);
//   - ids are unique, because they are the analytics keys.
//
// Node-native: `node --experimental-strip-types --test src/lib/tour/journeys.test.ts`.

import { test } from "node:test";
import assert from "node:assert/strict";
import { TOUR_ANCHOR, navAnchor, tourSelector } from "./tourAnchors.ts";
import { allJourneys } from "./registry.ts";
import type { Journey } from "./types.ts";
// Import for the registration side effect.
import "./journeys/overview.ts";
import "./journeys/localdev.ts";
import "./journeys/rad.ts";
import "./journeys/agentic.ts";

const KNOWN_SELECTORS = new Set(
  Object.values(TOUR_ANCHOR).map((a) => tourSelector(a)),
);

/**
 * A `template-<id>` anchor (`templateAnchor`, #411) is a New-project template
 * card. Its id is derived, not enumerated in `TOUR_ANCHOR`, so the guard accepts
 * the whole family by shape rather than pinning every template id here — and the
 * cards render on the studio-only Projects route, so they count as studio anchors.
 */
const isTemplateAnchor = (anchor: string): boolean =>
  /^template-[a-z0-9][a-z0-9-]*$/.test(anchor);

/**
 * Anchors that actually render per profile. Projects, the New-project button and
 * the workspace Run button are studio-only (ADR 0034 strips those routes from the
 * observe build entirely), so an observe journey must never target them.
 */
const RENDERS_IN = {
  studio: new Set(Object.values(TOUR_ANCHOR)),
  observe: new Set<string>([
    TOUR_ANCHOR.topologyNav,
    TOUR_ANCHOR.explorerNav,
    TOUR_ANCHOR.metricsNav,
    TOUR_ANCHOR.takeATour,
    // The localdev journey is profile-agnostic (#409); Explorer and Traces both
    // render on the lean observe build, so their anchors are valid there too.
    TOUR_ANCHOR.explorerInspect,
    TOUR_ANCHOR.tracesNav,
  ]),
} as const;

const anchorOf = (selector: string): string =>
  selector.slice('[data-tour="'.length, -2);

const journeys = allJourneys();

test("journeys are registered", () => {
  assert.ok(journeys.length > 0, "expected at least one registered journey");
});

test("journey ids are unique", () => {
  const ids = journeys.map((j) => j.id);
  assert.equal(new Set(ids).size, ids.length, `duplicate journey ids: ${ids}`);
});

for (const journey of journeys) {
  const label = `journey ${journey.id}`;

  test(`${label}: has a title, a blurb and at least one profile`, () => {
    assert.ok(journey.title.trim(), "missing title");
    assert.ok(journey.blurb.trim(), "missing blurb — the picker card needs it");
    assert.ok(journey.profiles.length > 0, "no profiles: unreachable journey");
  });

  test(`${label}: is at most five steps`, () => {
    assert.ok(
      journey.steps.length > 0 && journey.steps.length <= 5,
      `${journey.steps.length} steps — ADR 0049 caps a journey at 5; split it instead`,
    );
  });

  test(`${label}: step ids are unique and every step has title + body`, () => {
    const ids = journey.steps.map((s) => s.id);
    assert.equal(new Set(ids).size, ids.length, `duplicate step ids: ${ids}`);
    for (const s of journey.steps) {
      assert.ok(s.title.trim(), `step ${s.id} missing title`);
      assert.ok(s.body.trim(), `step ${s.id} missing body`);
    }
  });

  test(`${label}: routed steps use absolute paths`, () => {
    for (const s of eachStep(journey)) {
      if (s.route !== undefined) {
        assert.ok(s.route.startsWith("/"), `step ${s.id} route not absolute`);
      }
    }
  });

  test(`${label}: spotlight selectors derive from a known anchor`, () => {
    for (const s of eachStep(journey)) {
      if (s.kind !== "spotlight") continue;
      assert.ok(
        KNOWN_SELECTORS.has(s.selector) ||
          isTemplateAnchor(anchorOf(s.selector)),
        `step ${s.id} uses unknown selector ${s.selector} — derive it from TOUR_ANCHOR`,
      );
    }
  });

  test(`${label}: only targets anchors that render in its profiles`, () => {
    for (const profile of journey.profiles) {
      for (const s of eachStep(journey)) {
        if (s.kind !== "spotlight") continue;
        const anchor = anchorOf(s.selector);
        // Template cards render on the studio-only Projects route.
        if (isTemplateAnchor(anchor)) {
          assert.equal(
            profile,
            "studio",
            `${journey.id} step ${s.id} targets template card ${anchor}, which only renders in studio`,
          );
          continue;
        }
        assert.ok(
          RENDERS_IN[profile].has(anchor as never),
          `${journey.id} step ${s.id} targets ${anchor}, which does not render in ${profile}`,
        );
      }
    }
  });

  test(`${label}: a repair step is authored wherever a precondition can demand one`, () => {
    // A precondition that can return "repair" without a repair step authored
    // silently degrades to a skip — which is exactly the honest-Run problem in
    // reverse: the user loses the install hint they needed.
    for (const s of journey.steps) {
      if (!s.precondition) continue;
      const canRepair = ["has-js-runtime", "has-cluster"].includes(
        s.precondition.id,
      );
      if (canRepair) {
        assert.ok(
          s.repair,
          `step ${s.id} gates on ${s.precondition.id}, which can return "repair", but authors no repair step`,
        );
      }
    }
  });

  test(`${label}: handoff steps offer something to copy`, () => {
    for (const s of eachStep(journey)) {
      if (s.kind !== "handoff") continue;
      assert.ok(s.copy.trim(), `handoff step ${s.id} has nothing to copy`);
    }
  });
}

test("tourAnchors helpers: navAnchor strips the leading slash + lowercases, tourSelector wraps", () => {
  assert.equal(navAnchor("/projects"), "nav-projects");
  assert.equal(navAnchor("/Explorer"), "nav-explorer");
  assert.equal(tourSelector("run"), '[data-tour="run"]');
});

/** Every authored step plus any repair replacements, which are steps too. */
function* eachStep(journey: Journey) {
  for (const step of journey.steps) {
    yield step;
    if (step.repair) yield step.repair;
  }
}
