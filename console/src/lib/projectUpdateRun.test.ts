// Unit tests for the pure post-extension-update lifecycle runner (#1143):
// stop → update → restart, preserving prior running state. Node-native: run with
// `node --experimental-strip-types --test src/lib/projectUpdateRun.test.ts`.
// Ops are injected so the ordering/failure/conflict logic is guarded without a
// live gateway.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  runProjectUpdate,
  summarizeProjectUpdates,
  type ProjectUpdateOps,
  type ProjectUpdateOutcome,
  type ProjectUpdatePhase,
} from "./projectUpdateRun.ts";
import type { UpdatePlan } from "../gen";

function plan(p: Partial<UpdatePlan> = {}): UpdatePlan {
  return {
    pack: "pack",
    applied: true,
    versionBumped: true,
    create: [],
    overwrite: [],
    merged: [],
    preserved: [],
    conflicts: [],
    orphans: [],
    ...p,
  } as UpdatePlan;
}

// A recording ops harness: logs every operation in call order and lets each be
// made to fail. `apply` returns a configurable plan.
function harness(
  opts: {
    failStop?: boolean;
    failApply?: boolean;
    failStart?: boolean;
    plan?: UpdatePlan;
  } = {},
) {
  const calls: string[] = [];
  const ops: ProjectUpdateOps = {
    stop: async (name) => {
      calls.push(`stop:${name}`);
      if (opts.failStop) throw new Error("stop boom");
    },
    apply: async (name) => {
      calls.push(`apply:${name}`);
      if (opts.failApply) throw new Error("apply boom");
      return opts.plan ?? plan({ overwrite: ["a"] });
    },
    start: async (name) => {
      calls.push(`start:${name}`);
      if (opts.failStart) throw new Error("start boom");
    },
  };
  return { calls, ops };
}

test("running project: stop → update → restart, in that order", async () => {
  const { calls, ops } = harness();
  const out = await runProjectUpdate({ name: "p", running: true }, ops);
  assert.deepEqual(calls, ["stop:p", "apply:p", "start:p"]);
  assert.equal(out.status, "updated");
  assert.equal(out.wasRunning, true);
  assert.equal(out.restarted, true);
});

test("stopped project: updated without being started", async () => {
  const { calls, ops } = harness();
  const out = await runProjectUpdate({ name: "p", running: false }, ops);
  assert.deepEqual(calls, ["apply:p"]);
  assert.equal(out.status, "updated");
  assert.equal(out.wasRunning, false);
  assert.equal(out.restarted, false);
});

test("waits for stop to complete before applying the update", async () => {
  const order: string[] = [];
  let stopResolved = false;
  const ops: ProjectUpdateOps = {
    stop: async () => {
      // Defer resolution to a later microtask; apply must not run until it does.
      await Promise.resolve();
      await Promise.resolve();
      stopResolved = true;
      order.push("stop");
    },
    apply: async () => {
      assert.equal(stopResolved, true, "apply ran before stop completed");
      order.push("apply");
      return plan({ overwrite: ["a"] });
    },
    start: async () => {
      order.push("start");
    },
  };
  await runProjectUpdate({ name: "p", running: true }, ops);
  assert.deepEqual(order, ["stop", "apply", "start"]);
});

test("stop failure aborts: no update, no restart", async () => {
  const { calls, ops } = harness({ failStop: true });
  const out = await runProjectUpdate({ name: "p", running: true }, ops);
  assert.deepEqual(calls, ["stop:p"]);
  assert.equal(out.status, "stop-failed");
  assert.equal(out.restarted, false);
  assert.match(out.error ?? "", /stop boom/);
});

test("update failure on a running project: reported, prior running state restored", async () => {
  const { calls, ops } = harness({ failApply: true });
  const out = await runProjectUpdate({ name: "p", running: true }, ops);
  assert.deepEqual(calls, ["stop:p", "apply:p", "start:p"]);
  assert.equal(out.status, "update-failed");
  assert.equal(out.restarted, true);
  assert.match(out.error ?? "", /apply boom/);
});

test("update failure on a stopped project: reported, stays stopped", async () => {
  const { calls, ops } = harness({ failApply: true });
  const out = await runProjectUpdate({ name: "p", running: false }, ops);
  assert.deepEqual(calls, ["apply:p"]);
  assert.equal(out.status, "update-failed");
  assert.equal(out.restarted, false);
});

test("update failure then restore also fails: primary error headlines, restoreError set", async () => {
  const { ops } = harness({ failApply: true, failStart: true });
  const out = await runProjectUpdate({ name: "p", running: true }, ops);
  assert.equal(out.status, "update-failed");
  assert.match(out.error ?? "", /apply boom/);
  assert.match(out.restoreError ?? "", /start boom/);
  assert.equal(out.restarted, false);
});

test("restart failure after a successful update is explicit, not a success", async () => {
  const { calls, ops } = harness({ failStart: true });
  const out = await runProjectUpdate({ name: "p", running: true }, ops);
  assert.deepEqual(calls, ["stop:p", "apply:p", "start:p"]);
  assert.equal(out.status, "restart-failed");
  assert.equal(out.restarted, false);
  assert.match(out.error ?? "", /start boom/);
});

test("conflicts are surfaced, not reported as a clean update", async () => {
  const { ops } = harness({ plan: plan({ conflicts: ["x.ts"] }) });
  const out = await runProjectUpdate({ name: "p", running: false }, ops);
  assert.equal(out.status, "conflicts");
  assert.deepEqual(out.conflicts, ["x.ts"]);
});

test("a no-op overlay (already up to date) still completes as updated", async () => {
  const { ops } = harness({ plan: plan({}) });
  const out = await runProjectUpdate({ name: "p", running: false }, ops);
  assert.equal(out.status, "updated");
});

test("onPhase reports the lifecycle phases for a running project", async () => {
  const { ops } = harness();
  const phases: ProjectUpdatePhase[] = [];
  await runProjectUpdate({ name: "p", running: true }, ops, (ph) =>
    phases.push(ph),
  );
  assert.deepEqual(phases, ["stopping", "updating", "restarting", "done"]);
});

test("summarizeProjectUpdates buckets updated / conflicts / failed", () => {
  const outcomes: ProjectUpdateOutcome[] = [
    { name: "a", wasRunning: true, status: "updated", restarted: true },
    { name: "b", wasRunning: false, status: "conflicts", restarted: false },
    { name: "c", wasRunning: true, status: "stop-failed", restarted: false },
    { name: "d", wasRunning: true, status: "update-failed", restarted: true },
    { name: "e", wasRunning: true, status: "restart-failed", restarted: false },
  ];
  assert.deepEqual(summarizeProjectUpdates(outcomes), {
    updated: ["a"],
    conflicts: ["b"],
    failed: ["c", "d", "e"],
  });
});

test("a conflicted project that fails to restart appears in both conflicts and failed", () => {
  const outcomes: ProjectUpdateOutcome[] = [
    {
      name: "e",
      wasRunning: true,
      status: "restart-failed",
      restarted: false,
      conflicts: ["x.ts"],
    },
  ];
  assert.deepEqual(summarizeProjectUpdates(outcomes), {
    updated: [],
    conflicts: ["e"],
    failed: ["e"],
  });
});
