// Unit tests for the agentic journeys (#408, ADR 0049 §6).
//
// Two things this file pins that a browser walk-through cannot cheaply assert on
// every change:
//
//   - the pure success predicates, over synthetic contexts — including the
//     both-halves rule for 0a (running AND a deploy/worker-host log line) and the
//     marker-only contract for 0b, which keeps the (browser-only, best-effort)
//     park detection out of the predicate;
//   - the honest-Run invariant: with no JavaScript runtime, 0a's final step must
//     resolve to its repair replacement, and that replacement must NEVER tell the
//     user to hit Run — the exact defect ADR 0049 exists to prevent.
//
// Node-native: `node --experimental-strip-types --test src/lib/tour/agentic.test.ts`.

import { test } from "node:test";
import assert from "node:assert/strict";
import {
  agenticAuthor,
  agenticAuthorSucceeded,
  agenticHire,
  agenticHireSucceeded,
} from "./journeys/agentic.ts";
import { resolveSteps } from "./registry.ts";
import type { TourContext } from "./types.ts";

function ctx(over: Partial<TourContext> = {}): TourContext {
  return {
    profile: "studio",
    route: "/projects",
    projects: [],
    denoAvailable: true,
    nodeAvailable: true,
    templates: [],
    extensions: [],
    scratch: {},
    ...over,
  };
}

// ── 0a success predicate ─────────────────────────────────────────────────────

test("agenticAuthorSucceeded: needs BOTH running and a deploy/worker-host log line", () => {
  // running alone flickers true during a crash-looping start.
  assert.equal(
    agenticAuthorSucceeded(ctx({ runState: { running: true, output: "" } })),
    false,
    "running without the deploy/worker-host line is not success",
  );
  // the template's main.ts prints exactly these.
  assert.equal(
    agenticAuthorSucceeded(
      ctx({ runState: { running: true, output: "deployed pr-review\n" } }),
    ),
    true,
  );
  assert.equal(
    agenticAuthorSucceeded(
      ctx({
        runState: {
          running: true,
          output: "worker host running against http://127.0.0.1:8080\n",
        },
      }),
    ),
    true,
  );
});

test("agenticAuthorSucceeded: a matching log without running is a stale tail, not success", () => {
  assert.equal(
    agenticAuthorSucceeded(
      ctx({ runState: { running: false, output: "deployed pr-review\n" } }),
    ),
    false,
  );
});

test("agenticAuthorSucceeded: absent runState (source not registered) is false, not a throw", () => {
  assert.equal(agenticAuthorSucceeded(ctx()), false);
});

// ── 0b success predicate ─────────────────────────────────────────────────────

test("agenticHireSucceeded: true only once the parked-on-humanApproval marker is present", () => {
  assert.equal(agenticHireSucceeded(ctx()), false);
  assert.equal(
    agenticHireSucceeded(
      ctx({ runState: { running: true, output: "…busy…" } }),
    ),
    false,
  );
  assert.equal(
    agenticHireSucceeded(
      ctx({
        runState: {
          running: true,
          output: "deployed pr-review\n[tour] parked: humanApproval\n",
        },
      }),
    ),
    true,
  );
});

// ── honest-Run invariant on 0a's final step ──────────────────────────────────

test("agenticAuthor: with no runtime, the Run step repairs into an install hint", () => {
  const r = resolveSteps(
    agenticAuthor,
    ctx({ denoAvailable: false, nodeAvailable: false }),
  );
  const runStep = r.steps.find((s) => s.repaired === "has-js-runtime");
  assert.ok(runStep, "the run step should have been repaired with no runtime");
  assert.equal(runStep!.step.id, "author-run-repair");
  const text = `${runStep!.step.title}\n${runStep!.step.body}`;
  assert.ok(
    !/\b(hit|press|click|tap)\s+(the\s+)?(▶\s*)?run\b/i.test(text),
    `repair step must never tell the user to hit Run — got: ${text}`,
  );
});

test("agenticAuthor: with a runtime, the final step stays the Run-output spotlight", () => {
  const r = resolveSteps(agenticAuthor, ctx());
  const last = r.steps[r.steps.length - 1];
  assert.equal(last.step.id, "author-run");
  assert.equal(last.repaired, undefined);
  assert.equal(r.steps.length, agenticAuthor.steps.length);
});

// ── journey wiring ───────────────────────────────────────────────────────────

test("agentic journeys are studio-only and chained author → hire", () => {
  assert.deepEqual(agenticAuthor.profiles, ["studio"]);
  assert.deepEqual(agenticHire.profiles, ["studio"]);
  assert.deepEqual(agenticAuthor.nextJourneys, ["agentic-hire"]);
  assert.equal(agenticHire.id, "agentic-hire");
});
