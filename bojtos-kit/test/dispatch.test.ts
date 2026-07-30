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
  assert.deepEqual(
    result.snapshot.activeElementIds,
    [],
    "no token left active",
  );
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

// A message intermediate-catch event parks the token until a matching message is
// correlated. This is the loop-gating construct in the urban-pr-review model.
const WAIT_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:message id="Msg_go" name="go"><bpmn:extensionElements><zeebe:subscription correlationKey="=k" /></bpmn:extensionElements></bpmn:message>
  <bpmn:process id="wait" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="prep"><bpmn:extensionElements><zeebe:taskDefinition type="prep" /></bpmn:extensionElements></bpmn:serviceTask>
    <bpmn:intermediateCatchEvent id="hold"><bpmn:messageEventDefinition id="med" messageRef="Msg_go" /></bpmn:intermediateCatchEvent>
    <bpmn:serviceTask id="after"><bpmn:extensionElements><zeebe:taskDefinition type="after" /></bpmn:extensionElements></bpmn:serviceTask>
    <bpmn:endEvent id="done" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="prep" />
    <bpmn:sequenceFlow id="f2" sourceRef="prep" targetRef="hold" />
    <bpmn:sequenceFlow id="f3" sourceRef="hold" targetRef="after" />
    <bpmn:sequenceFlow id="f4" sourceRef="after" targetRef="done" />
  </bpmn:process>
</bpmn:definitions>`;

test("correlateMessage unblocks a token parked at a message catch event", async () => {
  const session = await createBojtosSession({ wasm: wasmBytes });
  session.deploy(WAIT_BPMN);
  session.createInstance("wait", JSON.stringify({ k: "key-1" }));

  const r1 = await dispatchRound(session, { prep: () => ({}), after: () => ({}) });
  assert.equal(r1.handled, 1, "only the prep job ran");
  assert.deepEqual(
    r1.snapshot.activeElementIds,
    ["hold"],
    "token is parked at the message catch, not past it",
  );

  // A non-matching correlation key must not release it.
  const miss = session.correlateMessage("go", "other-key", "{}");
  assert.deepEqual(miss.activeElementIds, ["hold"], "wrong key doesn't correlate");

  // The matching key releases the token to the downstream `after` job.
  const hit = session.correlateMessage("go", "key-1", "{}");
  assert.deepEqual(hit.activeElementIds, ["after"], "matching key advances the token");

  const r2 = await dispatchRound(session, { prep: () => ({}), after: () => ({}) });
  assert.equal(r2.snapshot.completedInstances, 1, "instance completes after the catch");
  session.free();
});

test("modify moves a token by terminating one element and activating another", async () => {
  const session = await newOrderSession("{}");
  // Token parked on `charge` (the `payment` job) right after creation.
  const created = session.snapshot();
  const instanceKey = created.instances[0]!.key;
  const chargeEik = created.instances[0]!.activeElements.find(
    (el) => el.elementId === "charge",
  )!.key;

  // Terminate the token on `charge` and activate one on `ship`, merging a var.
  const snap = session.modify(
    instanceKey,
    [{ elementId: "ship", variables: { expedited: true } }],
    [chargeEik],
  );

  assert.deepEqual(snap.activeElementIds, ["ship"], "token moved to ship");
  assert.equal(snap.instances[0]?.state, "Active");
  assert.equal(snap.instances[0]?.variables.expedited, true);
  assert.ok(
    snap.jobs.some((j) => j.jobType === "shipping"),
    "a fresh shipping job is created for the new token",
  );
  assert.ok(
    !snap.jobs.some((j) => j.jobType === "payment"),
    "the charge job is gone",
  );
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

// A minimal fake session that hands out exactly one `payment` job, so the
// error-routing tests below are deterministic and don't need the wasm engine.
// `completeJob`/`failJob` are stubbed to observe which one dispatch calls.
function fakeSession(overrides: Partial<BojtosSession>): {
  session: BojtosSession;
  calls: { completed: boolean; failed: boolean };
} {
  const job = {
    key: "job-1",
    type: "payment",
    instanceKey: "inst-1",
    elementId: "charge",
    retries: 3,
    variables: {},
  };
  const calls = { completed: false, failed: false };
  let activated = false;
  const base = {
    activateJobs: (jobType: string) => {
      if (jobType === "payment" && !activated) {
        activated = true;
        return [job];
      }
      return [];
    },
    completeJob: () => {
      calls.completed = true;
      return {} as never;
    },
    failJob: () => {
      calls.failed = true;
      return {} as never;
    },
    snapshot: () => ({}) as never,
    ...overrides,
  };
  return { session: base as unknown as BojtosSession, calls };
}

test("an engine completeJob failure bubbles, not masked as a job failure", async () => {
  // If the engine rejects the completion (bad JSON, ABI mismatch, internal
  // error) that's a real problem, not the demo's handler failing — it must
  // surface, not get quietly turned into a failJob that mutates engine state.
  const { session, calls } = fakeSession({
    completeJob: () => {
      throw new Error("engine boom");
    },
  });

  await assert.rejects(
    dispatchRound(session, { payment: () => ({ ok: true }) }),
    /engine boom/,
  );
  assert.equal(
    calls.failed,
    false,
    "an engine completeJob failure must not be swallowed into failJob",
  );
});

test("an unserializable handler result is translated to failJob, not bubbled", async () => {
  // A handler returning something JSON.stringify can't handle (a BigInt) is the
  // demo's own logic failing — it should fail the job, never reach the engine's
  // completeJob, and never bubble out of the round.
  const { session, calls } = fakeSession({
    completeJob: () => {
      throw new Error("completeJob should never be reached");
    },
  });

  const round = await dispatchRound(session, {
    payment: () => ({ amount: 1n }) as unknown as Record<string, unknown>,
  });

  assert.equal(round.handled, 1, "the job still counts as handled");
  assert.equal(calls.failed, true, "serialization failure fails the job");
  assert.equal(calls.completed, false, "the engine completion is never called");
});

test("reset wipes engine state so a re-run starts from zero completed instances", async () => {
  // Drive one instance to completion, then reset + redeploy and start a second
  // instance. Without reset the engine keeps the first (completed) instance
  // resident, so `completedInstances` would read >= 1 immediately on the re-run.
  const session = await newOrderSession("{}");
  const first = await dispatchWorkers(session, {
    payment: () => ({ charged: true }),
    shipping: () => ({ shipped: true }),
  });
  assert.equal(first.snapshot.completedInstances, 1, "first run completed");

  session.reset();
  const afterReset = session.snapshot();
  assert.equal(afterReset.totalInstances, 0, "reset clears resident instances");
  assert.equal(afterReset.completedInstances, 0, "reset clears completions");

  // Redeploy is required after a reset (definitions are wiped too), then the
  // fresh instance starts from a clean slate.
  session.deploy(ORDER_BPMN);
  const created = session.createInstance("order", "{}");
  assert.equal(created.completedInstances, 0, "re-run does not inherit completions");
  assert.equal(created.totalInstances, 1);
  session.free();
});

// An ad-hoc sub-process agent: the container emits an `agent-worker` job; its
// handler returns `activateElements` to run inner tools, the engine loops, and a
// later turn completes the container. Exercises the `agents` dispatch seam end
// to end against the real wasm engine.
const ADHOC_AGENT_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="agentic" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:adHocSubProcess id="agent">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="agent-worker" />
        <zeebe:adHoc outputCollection="results" outputElement="=result" />
      </bpmn:extensionElements>
      <bpmn:serviceTask id="toolA"><bpmn:extensionElements><zeebe:taskDefinition type="tool" /></bpmn:extensionElements></bpmn:serviceTask>
      <bpmn:serviceTask id="toolB"><bpmn:extensionElements><zeebe:taskDefinition type="tool" /></bpmn:extensionElements></bpmn:serviceTask>
    </bpmn:adHocSubProcess>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
    <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>`;

test("agent dispatch: activateElements runs tools, loops, and completes the container", async () => {
  const session = await createBojtosSession({ wasm: wasmBytes });
  session.deploy(ADHOC_AGENT_BPMN);
  session.createInstance("agentic", "{}");

  const toolsRun: string[] = [];
  let agentTurns = 0;

  const result = await dispatchWorkers(
    session,
    {
      tool: (job) => {
        toolsRun.push(job.elementId);
        // Each tool's `result` is captured into the container's outputCollection.
        return { result: job.elementId };
      },
    },
    {
      agents: {
        "agent-worker": () => {
          agentTurns++;
          if (agentTurns === 1) {
            // Turn 1: activate both tools.
            return {
              activateElements: [
                { elementId: "toolA" },
                { elementId: "toolB" },
              ],
            };
          }
          // Turn 2: the tools have drained (the engine re-emitted the agent
          // job); the agent decides it is done and completes the container,
          // merging its decision into the instance.
          return {
            completionConditionFulfilled: true,
            variables: { decision: "cleared" },
          };
        },
      },
    },
  );

  assert.deepEqual(toolsRun.slice().sort(), ["toolA", "toolB"], "both tools ran");
  assert.equal(agentTurns, 2, "agent ran two turns (activate, then complete)");
  assert.equal(
    result.snapshot.completedInstances,
    1,
    "the agent's completion finished the container and the instance",
  );
  session.free();
});

test("an empty agent result completes the container with no tools", async () => {
  const session = await createBojtosSession({ wasm: wasmBytes });
  session.deploy(ADHOC_AGENT_BPMN);
  session.createInstance("agentic", "{}");

  const result = await dispatchWorkers(
    session,
    {},
    { agents: { "agent-worker": () => ({}) } },
  );
  assert.equal(
    result.snapshot.completedInstances,
    1,
    "an agent that activates nothing still completes the container",
  );
  session.free();
});

test("an unserializable agent result is translated to failJob, not bubbled", async () => {
  // Mirrors the plain-job case: an AgentResult the engine can't serialize (a
  // BigInt) is the demo's own logic failing — it must fail the job, never reach
  // completeAgentJob, and never bubble out of the round.
  const { session, calls } = fakeSession({
    completeAgentJob: () => {
      throw new Error("completeAgentJob should never be reached");
    },
  });

  const round = await dispatchRound(
    session,
    {},
    { agents: { payment: () => ({ variables: { amount: 1n } }) } },
  );

  assert.equal(round.handled, 1, "the agent job still counts as handled");
  assert.equal(calls.failed, true, "serialization failure fails the agent job");
});

test("dispatchRound rejects a job type registered as both a worker and an agent", async () => {
  // Overlapping keys are a footgun: the worker pass would activate and
  // plain-complete the job first, so its agentic activateElements could never
  // fire. Reject up front instead of silently no-op'ing the tool activation.
  const { session } = fakeSession({});
  await assert.rejects(
    dispatchRound(session, { payment: () => ({}) }, { agents: { payment: () => ({}) } }),
    /registered as both a worker and an agent/,
    "an overlapping worker/agent key is rejected, not silently mis-dispatched",
  );
});

