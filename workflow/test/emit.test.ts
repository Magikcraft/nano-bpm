// Unit tests for the authoring surfaces + model derivation + the replay engine.
// These need no gateway and always run. Run against the built `dist` artifact.
import { test } from "node:test";
import assert from "node:assert/strict";

import {
  defineWorkflow,
  defineFlow,
  toBpmn,
  externalJobTypes,
  replayOnce,
  Worker,
  WorkflowClient,
  WorkflowError,
  type ImperativeWorkflow,
  type Journal,
} from "../dist/index.js";

test("imperative emit: single looped orchestrator with derived job type", () => {
  const wf = defineWorkflow("pr-review", async () => {});
  assert.equal(wf.orchestrateType, "pr-review:__orchestrate");
  const xml = toBpmn(wf);
  assert.match(xml, /<zeebe:taskDefinition type="pr-review:__orchestrate" \/>/);
  assert.match(xml, /<bpmn:exclusiveGateway id="Gw" default="f_loop">/);
  assert.match(xml, /<bpmn:conditionExpression>=wfDone<\/bpmn:conditionExpression>/);
  assert.match(xml, /<bpmn:sequenceFlow id="f_loop" sourceRef="Gw" targetRef="Orchestrate" \/>/);
});

test("declarative emit: service tasks + derived types + message/subscription", () => {
  const flow = defineFlow("pr-review", (w) => {
    w.run("fetchDiff", async () => ({}));
    w.signal("humanApproval", { correlationKey: "prId" });
    w.run("merge", async () => ({}));
  });
  const xml = toBpmn(flow);
  assert.match(xml, /<zeebe:taskDefinition type="pr-review:fetchDiff" \/>/);
  assert.match(xml, /<zeebe:taskDefinition type="pr-review:merge" \/>/);
  assert.match(xml, /<bpmn:intermediateCatchEvent id="humanApproval"/);
  assert.match(xml, /<bpmn:message id="Msg_humanApproval" name="pr-review:humanApproval">/);
  assert.match(xml, /<zeebe:subscription correlationKey="=prId" \/>/);
});

test("declarative validation: duplicates, missing correlationKey, empty, bad id", () => {
  assert.throws(
    () =>
      defineFlow("dup", (w) => {
        w.run("a", async () => ({}));
        w.run("a", async () => ({}));
      }),
    /duplicate step name "a"/,
  );
  assert.throws(
    // @ts-expect-error intentionally missing correlationKey
    () => defineFlow("f", (w) => w.signal("s", {})),
    /needs \{ correlationKey \}/,
  );
  assert.throws(() => defineFlow("empty", () => {}), /declared no steps/);
  assert.throws(() => defineFlow("bad id!", (w) => w.run("a", async () => ({}))), /not a valid BPMN identifier/);
});

test("replayOnce: first pass runs only the frontier step (its side effect once)", async () => {
  const calls: string[] = [];
  const wf: ImperativeWorkflow = defineWorkflow("t", async (ctx) => {
    await ctx.run("a", () => {
      calls.push("a");
      return { n: 1 };
    });
    await ctx.run("b", () => {
      calls.push("b");
      return { n: 2 };
    });
  });

  const step0 = await replayOnce(wf, {}, {});
  assert.equal(step0.done, false);
  assert.deepEqual(calls, ["a"], "only the frontier (a) executed");
  assert.equal(step0.done === false && step0.frontier.key, "1:a");
});

test("replayOnce: recorded steps are replayed (handler NOT called), frontier advances", async () => {
  const calls: string[] = [];
  const wf = defineWorkflow("t", async (ctx) => {
    const a = await ctx.run("a", () => {
      calls.push("a");
      return { n: 1 };
    });
    await ctx.run("b", () => {
      calls.push("b");
      return { n: (a as { n: number }).n + 1 };
    });
  });

  const journal: Journal = { "1:a": { n: 1 } };
  const step = await replayOnce(wf, {}, journal);
  assert.equal(step.done, false);
  assert.deepEqual(calls, ["b"], "a was replayed from the journal (no side effect); only b ran");
  assert.equal(step.done === false && step.frontier.key, "2:b");
});

test("replayOnce: a fully-journalled run reports done and calls nothing", async () => {
  const calls: string[] = [];
  const wf = defineWorkflow("t", async (ctx) => {
    await ctx.run("a", () => {
      calls.push("a");
      return { n: 1 };
    });
    await ctx.run("b", () => {
      calls.push("b");
      return { n: 2 };
    });
  });
  const step = await replayOnce(wf, {}, { "1:a": { n: 1 }, "2:b": { n: 2 } });
  assert.equal(step.done, true);
  assert.deepEqual(calls, [], "nothing executed — pure replay to completion");
});

test("replayOnce: repeated ctx.run in a loop gets distinct ordinal keys", async () => {
  const wf = defineWorkflow("loop", async (ctx) => {
    for (let i = 0; i < 3; i++) await ctx.run("tick", () => ({ i }));
  });
  // With the first tick recorded, the frontier is the SECOND tick (2:tick).
  const step = await replayOnce(wf, {}, { "1:tick": { i: 0 } });
  assert.equal(step.done === false && step.frontier.key, "2:tick");
});

test("replayOnce: input is available and stable across replays", async () => {
  const wf = defineWorkflow("t", async (ctx) => {
    await ctx.run("useInput", () => ({ prId: ctx.input.prId }));
  });
  const step = await replayOnce(wf, { prId: "PR-1" }, {});
  assert.equal(step.done === false && (step.frontier.result as { prId: string }).prId, "PR-1");
});

test("worker: rejects two workflows that resolve to the same derived job type", () => {
  const a = defineWorkflow("dup", async () => {});
  const b = defineWorkflow("dup", async () => {});
  assert.throws(
    () => new Worker({ baseUrl: "http://localhost:0", workflows: [a, b] }),
    /duplicate derived job type "dup:__orchestrate"/,
  );
});

test("worker: distinct workflow ids register without collision", () => {
  const a = defineFlow("wf-a", (w) => w.run("step", async () => ({})));
  const b = defineFlow("wf-b", (w) => w.run("step", async () => ({})));
  const worker = new Worker({ baseUrl: "http://localhost:0", workflows: [a, b] });
  assert.deepEqual(worker.servedTypes.sort(), ["wf-a:step", "wf-b:step"]);
});

test("declarative task: external step emits a service task + job type but is NOT hosted", () => {
  const flow = defineFlow("pr-review", (w) => {
    w.run("fetchDiff", async () => ({}));
    w.task("signPdf"); // served by a worker outside this program
    w.run("merge", async () => ({}));
  });
  const xml = toBpmn(flow);
  // External `task` derives the same service task + job type as a `run`.
  assert.match(xml, /<bpmn:serviceTask id="signPdf" name="signPdf">/);
  assert.match(xml, /<zeebe:taskDefinition type="pr-review:signPdf" \/>/);
  // externalJobTypes surfaces the contract external workers must poll.
  assert.deepEqual(externalJobTypes(flow), ["pr-review:signPdf"]);
  // The local Worker hosts only the `run` steps; the external type is unhosted.
  const worker = new Worker({ baseUrl: "http://localhost:0", workflows: [flow] });
  assert.deepEqual(worker.servedTypes.sort(), ["pr-review:fetchDiff", "pr-review:merge"]);
  assert.equal(worker.servedTypes.includes("pr-review:signPdf"), false);
});

test("declarative task: a duplicate task/run step name is rejected", () => {
  assert.throws(
    () =>
      defineFlow("dup", (w) => {
        w.run("a", async () => ({}));
        w.task("a");
      }),
    /duplicate step name "a"/,
  );
});

test("client.signal: rejects an unknown signal name with a clear error", async () => {
  const flow = defineFlow("pr-review", (w) => {
    w.run("fetchDiff", async () => ({}));
    w.signal("humanApproval", { correlationKey: "prId" });
  });
  const client = new WorkflowClient({ baseUrl: "http://localhost:0" });
  await assert.rejects(
    () => client.signal(flow, "humanApprovel", "PR-1"),
    (e: unknown) =>
      e instanceof WorkflowError &&
      /unknown signal "humanApprovel"/.test((e as Error).message) &&
      /"humanApproval"/.test((e as Error).message),
  );
});
