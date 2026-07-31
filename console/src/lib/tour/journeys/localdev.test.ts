// Unit tests for Journey 1 (headless local-dev) — ADR 0049 §6, epic #406, #409.
//
// Three invariants that a refactor could silently break:
//
//   - the success signal only fires once BOTH halves of the outcome happened
//     (base URL copied AND the debugger reached) — not on step completion alone;
//   - the optional trace step disappears on a fresh, empty engine and returns
//     once traces exist, so a first-timer is never pointed at an empty rail;
//   - the copyable base URL tracks the live origin, so `--port N` is reflected
//     honestly rather than a hardcoded `:8080`.
//
// Node-native: `node --experimental-strip-types --test src/lib/tour/journeys/localdev.test.ts`.

import { test } from "node:test";
import assert from "node:assert/strict";
import { resolveSteps } from "../registry.ts";
import type { TourContext } from "../types.ts";
import { localdev } from "./localdev.ts";
import {
  localdevSucceeded,
  markBaseUrlCopied,
  markExplorerReached,
  resetLocaldevProgress,
  v2BaseUrl,
} from "./localdev-progress.ts";

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

test("successEvent fires only after BOTH the base URL is copied and Explorer is reached", () => {
  resetLocaldevProgress();
  assert.equal(localdev.successEvent(ctx()), false, "nothing done yet");

  markBaseUrlCopied();
  assert.equal(
    localdev.successEvent(ctx()),
    false,
    "copy alone is not success",
  );

  markExplorerReached();
  assert.equal(localdev.successEvent(ctx()), true, "both halves done");

  assert.equal(localdevSucceeded(), true);
  resetLocaldevProgress();
  assert.equal(localdevSucceeded(), false, "reset forgets both latches");
});

test("the optional trace step skips on an empty engine and returns once traces exist", () => {
  const empty = resolveSteps(localdev, ctx({ traceCount: 0 }));
  assert.ok(
    !empty.steps.some((s) => s.step.id === "traces"),
    "trace step must not show with zero traces",
  );
  assert.ok(
    empty.skipped.some((s) => s.stepId === "traces"),
    "trace step is recorded as skipped, not silently dropped",
  );

  const withTraces = resolveSteps(localdev, ctx({ traceCount: 1 }));
  assert.ok(
    withTraces.steps.some((s) => s.step.id === "traces"),
    "trace step shows once there is something to see",
  );
});

test("the non-optional steps always survive resolution", () => {
  const { steps } = resolveSteps(localdev, ctx({ traceCount: 0 }));
  const ids = steps.map((s) => s.step.id);
  assert.deepEqual(ids, ["already-running", "point-your-client", "debugger"]);
});

test("v2BaseUrl tracks the live origin — not a hardcoded :8080", () => {
  const stub = { location: { origin: "http://192.168.1.5:9999" } };
  Reflect.set(globalThis, "window", stub);
  try {
    assert.equal(v2BaseUrl(), "http://192.168.1.5:9999/v2");
  } finally {
    Reflect.deleteProperty(globalThis, "window");
  }
});

test("v2BaseUrl falls back to a loopback literal off-browser", () => {
  // No window in the Node test process → the guarded fallback path.
  assert.equal(v2BaseUrl(), "http://127.0.0.1:8080/v2");
});
