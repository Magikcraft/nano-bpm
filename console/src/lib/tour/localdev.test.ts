// Unit guards for Journey 1 — headless Camunda-compatible local dev (#409).
//
// Node-native: `node --experimental-strip-types --test src/lib/tour/localdev.test.ts`.
// The module is import-safe here precisely because everything that touches
// `window` / `localStorage` / the network lives inside a function body.

import { test } from "node:test";
import assert from "node:assert/strict";
import { resolveSteps } from "./registry.ts";
import type { TourContext } from "./types.ts";
import {
  LOCALDEV_JOURNEY_ID,
  SCRATCH_BASE_URL_COPIED,
  SCRATCH_EXPLORER_REACHED,
  localdev,
  localdevSucceeded,
  swaggerUrl,
  v2BaseUrl,
} from "./journeys/localdev.ts";

function ctx(over: Partial<TourContext> = {}): TourContext {
  return {
    profile: "studio",
    route: "/explorer",
    projects: [],
    denoAvailable: true,
    nodeAvailable: true,
    templates: [],
    extensions: [],
    scratch: {},
    ...over,
  };
}

test("v2BaseUrl derives from the live origin, on a non-8080 port, no double slash", () => {
  assert.equal(v2BaseUrl("http://127.0.0.1:9137"), "http://127.0.0.1:9137/v2");
  assert.equal(
    v2BaseUrl("https://nano.example:8443/"),
    "https://nano.example:8443/v2",
  );
  assert.equal(v2BaseUrl("http://localhost:3000"), "http://localhost:3000/v2");
});

test("swaggerUrl derives from the same origin", () => {
  assert.equal(
    swaggerUrl("http://127.0.0.1:9137"),
    "http://127.0.0.1:9137/swagger",
  );
});

test("the handoff step offers the origin-derived v2 URL, not a hardcoded 8080", () => {
  const handoff = localdev.steps.find((s) => s.kind === "handoff");
  assert.ok(handoff && handoff.kind === "handoff");
  // In a Node context the module falls back to a placeholder origin; the point
  // this asserts is that the copy value is a /v2 URL derived through v2BaseUrl,
  // never a literal the browser would ignore.
  assert.match(handoff.copy, /\/v2$/);
  assert.equal(
    handoff.verify,
    undefined,
    "handoff is self-reported (I've done it)",
  );
});

test("successEvent needs BOTH the base URL taken and Explorer reached", () => {
  assert.equal(localdevSucceeded(ctx()), false, "neither signal");
  assert.equal(
    localdevSucceeded(ctx({ scratch: { [SCRATCH_BASE_URL_COPIED]: true } })),
    false,
    "only copied",
  );
  assert.equal(
    localdevSucceeded(ctx({ scratch: { [SCRATCH_EXPLORER_REACHED]: true } })),
    false,
    "only explorer",
  );
  assert.equal(
    localdevSucceeded(
      ctx({
        scratch: {
          [SCRATCH_BASE_URL_COPIED]: true,
          [SCRATCH_EXPLORER_REACHED]: true,
        },
      }),
    ),
    true,
    "both signals present",
  );
});

test("the optional traces step skips cleanly with zero traces", () => {
  for (const traceCount of [undefined, 0]) {
    const res = resolveSteps(localdev, ctx({ traceCount }));
    assert.ok(
      !res.steps.some((s) => s.step.id === "traces-why-slow"),
      `traces step must be absent when traceCount=${traceCount}`,
    );
    assert.ok(
      res.skipped.some((s) => s.preconditionId === "has-traces"),
      "the skip must be reported for analytics",
    );
  }
});

test("the optional traces step appears once traces exist", () => {
  const res = resolveSteps(localdev, ctx({ traceCount: 3 }));
  assert.ok(
    res.steps.some((s) => s.step.id === "traces-why-slow"),
    "traces step must be present when traceCount > 0",
  );
});

test("the journey is profile-agnostic and deliberately terminal", () => {
  assert.equal(localdev.id, LOCALDEV_JOURNEY_ID);
  assert.deepEqual([...localdev.profiles].sort(), ["observe", "studio"]);
  assert.equal(
    localdev.nextJourneys,
    undefined,
    "a headless user's time is respected — no chained journey",
  );
  assert.ok(
    localdev.steps.length <= 5,
    "ADR 0049 caps a journey at five steps",
  );
});
