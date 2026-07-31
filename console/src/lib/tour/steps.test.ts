// Guard tests for the product tour step lists. Node-native: run with
// `node --experimental-strip-types --test src/lib/tour/steps.test.ts`.
//
// These lock the contract that keeps the tour from silently breaking:
//   - every anchored step points at a *known* data-tour anchor (no ad-hoc
//     selector strings that could drift from the components), and
//   - each profile only targets anchors that actually render in that profile
//     (e.g. observe must not point at the studio-only Projects nav / workspace
//     Run button — the tour would just spotlight nothing).
//
// steps.ts pulls in ../profile, which reads the Vite-injected `__STUDIO__`
// global at import time, so we define it before dynamically importing.

import { test } from "node:test";
import assert from "node:assert/strict";
import { TOUR_ANCHOR, navAnchor, tourSelector } from "./tourAnchors.ts";

Object.defineProperty(globalThis, "__STUDIO__", {
  value: true,
  configurable: true,
});

const { getTourSteps } = await import("./steps.ts");

// Anchors each profile is allowed to target — i.e. elements that actually
// render there. Projects (and the New-project button + workspace Run) are
// studio-only; the operator nav items are shared but the observe journey uses
// its own subset.
const STUDIO_ALLOWED = new Set([
  TOUR_ANCHOR.projectsNav,
  TOUR_ANCHOR.newProject,
  TOUR_ANCHOR.run,
  TOUR_ANCHOR.explorerNav,
]);
const OBSERVE_ALLOWED = new Set([
  TOUR_ANCHOR.topologyNav,
  TOUR_ANCHOR.explorerNav,
  TOUR_ANCHOR.metricsNav,
]);

const KNOWN_SELECTORS = new Set(
  Object.values(TOUR_ANCHOR).map((a) => tourSelector(a)),
);

for (const profile of ["studio", "observe"] as const) {
  const steps = getTourSteps(profile);

  test(`${profile}: has steps with unique ids`, () => {
    assert.ok(steps.length > 0, "expected a non-empty step list");
    const ids = steps.map((s) => s.id);
    assert.equal(new Set(ids).size, ids.length, `duplicate ids: ${ids}`);
  });

  test(`${profile}: opens with a centered welcome step`, () => {
    const first = steps[0];
    assert.equal(first.selector, undefined, "welcome must be anchorless");
    assert.equal(first.route, undefined, "welcome must not navigate");
  });

  test(`${profile}: every step has title and body`, () => {
    for (const s of steps) {
      assert.ok(s.title.trim(), `step ${s.id} missing title`);
      assert.ok(s.body.trim(), `step ${s.id} missing body`);
    }
  });

  test(`${profile}: routed steps use absolute paths`, () => {
    for (const s of steps) {
      if (s.route !== undefined) {
        assert.ok(s.route.startsWith("/"), `step ${s.id} route not absolute`);
      }
    }
  });

  test(`${profile}: anchored steps reference known data-tour anchors`, () => {
    for (const s of steps) {
      if (s.selector !== undefined) {
        assert.ok(
          KNOWN_SELECTORS.has(s.selector),
          `step ${s.id} uses unknown selector ${s.selector} — derive it from TOUR_ANCHOR`,
        );
      }
    }
  });
}

test("studio only targets anchors that render in studio", () => {
  for (const s of getTourSteps("studio")) {
    if (s.selector === undefined) continue;
    const anchor = s.selector.slice('[data-tour="'.length, -2);
    assert.ok(
      STUDIO_ALLOWED.has(anchor),
      `studio step ${s.id} targets ${anchor}, not a studio anchor`,
    );
  }
});

test("observe never targets studio-only anchors", () => {
  for (const s of getTourSteps("observe")) {
    if (s.selector === undefined) continue;
    const anchor = s.selector.slice('[data-tour="'.length, -2);
    assert.ok(
      OBSERVE_ALLOWED.has(anchor),
      `observe step ${s.id} targets ${anchor} (studio-only or unknown) — it would spotlight nothing`,
    );
  }
});

test("tourAnchors helpers: navAnchor lowercases, tourSelector wraps", () => {
  assert.equal(navAnchor("Projects"), "nav-projects");
  assert.equal(navAnchor("Explorer"), "nav-explorer");
  assert.equal(tourSelector("run"), '[data-tour="run"]');
});
