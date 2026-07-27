// End-to-end tests for the worker dispatch loop (ADR 0043 §8 step 3), run
// against the built `dist` (the shipped artifact) with the real wasm engine.
// Node can't resolve the engine's `import.meta.url` wasm fetch, so we pass the
// binary bytes explicitly via the `wasm` source option (the same escape hatch
// the external-`.wasm` mode uses).
//
// Run: `npm test` (builds first). Requires Node >= 22 for TS type-stripping.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import {
  type BojtosSession,
  createBojtosSession,
  dispatchRound,
  dispatchWorkers,
  JobFailure,
} from "../dist/index.js";

const require = createRequire(import.meta.url);
const wasmBytes = await readFile(
  require.resolve("@nanobpm/engine-wasm/nanobpmn_engine_bg.wasm"),
);

// order → charge (payment) → ship (shipping) → done. Two service tasks let a
// test assert that a completed job's merged variables propagate to the next.
const ORDER_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="order" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="charge"><bpmn:extensionElements><zeebe:taskDefinition type="payment" /></bpmn:extensionElements></bpmn:serviceTask>
    <bpmn:serviceTask id="ship"><bpmn:extensionElements><zeebe:taskDefinition type="shipping" /></bpmn:extensionElements></bpmn:serviceTask>
    <bpmn:endEvent id="done" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="charge" />
    <bpmn:sequenceFlow id="f2" sourceRef="charge" targetRef="ship" />
    <bpmn:sequenceFlow id="f3" sourceRef="ship" targetRef="done" />
  </bpmn:process>
</bpmn:definitions>`;

async function newOrderSession(seed: string): Promise<BojtosSession> {
  const session = await createBojtosSession({ wasm: wasmBytes });
  session.deploy(ORDER_BPMN);
  session.createInstance("order", seed);
  return session;
}

test("drains a process: activate → handler → complete, merging variables downstream", async () => {
  const session = await newOrderSession('{"amount":42}');
  let shipSaw: Record<string, unknown> | null = null;
  const result = await dispatchWorkers(session, {
    payment: (job) => {
      // The worker sees the instance's seeded payload.
      assert.equal(job.variables.amount, 42);
      return { charged: true };
    },
    shipping: (job) => {
      // The downstream job sees the merge from the payment worker.
      shipSaw = job.variables;
      return { shipped: true };
    },
  });

  assert.equal(result.handled, 2, "both jobs handled");
  assert.equal(result.snapshot.completedInstances, 1, "instance completed");
  assert.equal(result.snapshot.totalInstances, 1);
  assert.deepEqual(result.snapshot.activeElementIds, [], "no token left active");
  assert.deepEqual(shipSaw, { amount: 42, charged: true });
  session.free();
});

test("a job type with no registered handler is left waiting", async () => {
  const session = await newOrderSession("{}");
  const result = await dispatchWorkers(session, {
    // Only shipping is registered; the waiting `payment` job is never activated.
    shipping: () => ({}),
  });

  assert.equal(result.handled, 0, "nothing dispatched");
  assert.equal(result.rounds, 1, "one quiescent round");
  assert.equal(result.snapshot.completedInstances, 0);
  assert.equal(result.snapshot.jobs.length, 1, "payment job still waiting");
  assert.equal(result.snapshot.jobs[0]?.jobType, "payment");
  session.free();
});

test("a thrown handler fails the job; JobFailure(retries:0) raises an incident", async () => {
  const session = await newOrderSession("{}");
  const result = await dispatchWorkers(session, {
    payment: () => {
      throw new JobFailure("payment declined", { retries: 0 });
    },
  });

  assert.equal(result.handled, 1, "the failed job counts as handled");
  assert.equal(result.snapshot.completedInstances, 0);
  assert.deepEqual(result.snapshot.incidentElementIds, ["charge"]);
  session.free();
});

test("dispatchRound advances the token frontier one step per round", async () => {
  // The animation contract (ADR 0043 §4): a single round must not cascade the
  // whole chain — it activates the *current* frontier, so completing `charge`
  // doesn't also run the `ship` job it unblocks until the next round. This is
  // what lets the UI step the token task-by-task instead of jumping to done.
  const session = await newOrderSession("{}");
  const workers = {
    payment: () => ({ paid: true }),
    shipping: () => ({ shipped: true }),
  };

  const r1 = await dispatchRound(session, workers);
  assert.equal(r1.handled, 1, "only the payment frontier this round");
  assert.deepEqual(
    r1.snapshot.activeElementIds,
    ["ship"],
    "token advanced to ship, not straight to done",
  );

  const r2 = await dispatchRound(session, workers);
  assert.equal(r2.handled, 1, "the shipping frontier next round");
  assert.equal(r2.snapshot.completedInstances, 1);

  const r3 = await dispatchRound(session, workers);
  assert.equal(r3.handled, 0, "quiescent");
  session.free();
});

test("maxRounds guards against an unbounded drain", async () => {
  const session = await newOrderSession("{}");
  // The order process needs three rounds (payment, shipping, quiescent); cap at
  // one so the guard trips deterministically.
  await assert.rejects(
    dispatchWorkers(
      session,
      { payment: () => ({}), shipping: () => ({}) },
      { maxRounds: 1 },
    ),
    /exceeded maxRounds/,
  );
  session.free();
});
