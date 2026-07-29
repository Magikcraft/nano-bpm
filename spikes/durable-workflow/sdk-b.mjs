// Strategy B — the replayed imperative surface (the "true Temporal model").
//
// Strategy A (sdk.mjs) compiles a DECLARATIVE step list to a linear model. This
// is different and more powerful: you write the orchestration as an ORDINARY
// IMPERATIVE async function, and the engine drives it durably by REPLAY.
//
//   const wf = defineWorkflowB("pr-review", async (ctx) => {
//     const diff   = await ctx.run("fetchDiff",  () => gh.diff(ctx.input.prId));
//     const review = await ctx.run("autoReview", () => llm.review(diff));
//     if (review.blocking) await ctx.run("requestChanges", () => gh.comment(...));
//     await ctx.run("merge", () => gh.merge(ctx.input.prId));
//   });
//
// Note the real control flow: `if`, local variables, arbitrary composition. That
// is exactly what Strategy A cannot express and what Temporal is prized for.
//
// HOW THE DURABILITY WORKS (and why it's honest):
//
//   The model is a SINGLE orchestrator service task in a LOOP (task → gateway →
//   back to task until done). Each engine turn activates the orchestrator; the
//   worker REPLAYS the function from the top, feeding each `ctx.run(name,fn)` its
//   previously-recorded result from a durable JOURNAL kept in engine process
//   variables:
//
//     - recorded step  → return the journalled value, DO NOT call fn (no side
//       effect, no re-execution) — this is "replay".
//     - first UNrecorded step ("the frontier") → call fn ONCE (the real side
//       effect), then suspend, completing the orchestrator job with the frontier
//       result appended to the journal. The engine durably commits + loops.
//     - function returns with no new step → complete with done=true, exit loop.
//
//   So exactly one new side effect happens per durable engine commit, and the
//   journal — the replay log — is engine-durable. This is ADR 0044 Strategy B,
//   and it is precisely the shape of ADR 0023's ad-hoc outputCollection (each
//   recorded step result is a folded accumulator entry).
//
// SCOPE (honest): this prototype implements `ctx.run` (the crown-jewel replay
// mechanic) + crash-resume. `ctx.sleep`/`ctx.signal` in the replay model are
// "commands" that must emit a real wait state into the model (the Temporal
// command pattern) — described in ADR 0044, not built here. The orchestration
// function MUST be deterministic across replays: no wall-clock branching, RNG,
// or I/O in the function body — side effects live ONLY inside `ctx.run` handlers.

import { deployXml, startWorkflow } from "./sdk.mjs";

const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// A sentinel thrown to unwind the function at the frontier step.
const SUSPEND = Symbol("suspend");

export function defineWorkflowB(id, orchestrate) {
  if (typeof orchestrate !== "function") throw new Error("defineWorkflowB needs an async function");
  return { id, orchestrate, orchestrateType: `${id}:__orchestrate` };
}

/** The looped-orchestrator model: start → orchestrate → gw → (done ? end : loop). */
export function toBpmnB(def) {
  return (
    `<?xml version="1.0" encoding="UTF-8"?>\n` +
    `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Definitions_${esc(def.id)}" targetNamespace="http://bpmn.io/schema/bpmn">\n` +
    `  <bpmn:process id="${esc(def.id)}" name="${esc(def.id)}" isExecutable="true">\n` +
    `    <bpmn:startEvent id="Start"><bpmn:outgoing>f_start</bpmn:outgoing></bpmn:startEvent>\n` +
    `    <bpmn:serviceTask id="Orchestrate" name="orchestrate">\n` +
    `      <bpmn:extensionElements><zeebe:taskDefinition type="${esc(def.orchestrateType)}" /></bpmn:extensionElements>\n` +
    `      <bpmn:incoming>f_start</bpmn:incoming><bpmn:incoming>f_loop</bpmn:incoming><bpmn:outgoing>f_toGw</bpmn:outgoing>\n` +
    `    </bpmn:serviceTask>\n` +
    `    <bpmn:exclusiveGateway id="Gw" default="f_loop">\n` +
    `      <bpmn:incoming>f_toGw</bpmn:incoming><bpmn:outgoing>f_done</bpmn:outgoing><bpmn:outgoing>f_loop</bpmn:outgoing>\n` +
    `    </bpmn:exclusiveGateway>\n` +
    `    <bpmn:endEvent id="End"><bpmn:incoming>f_done</bpmn:incoming></bpmn:endEvent>\n` +
    `    <bpmn:sequenceFlow id="f_start" sourceRef="Start" targetRef="Orchestrate" />\n` +
    `    <bpmn:sequenceFlow id="f_toGw" sourceRef="Orchestrate" targetRef="Gw" />\n` +
    `    <bpmn:sequenceFlow id="f_done" sourceRef="Gw" targetRef="End">\n` +
    `      <bpmn:conditionExpression>=wfDone</bpmn:conditionExpression>\n` +
    `    </bpmn:sequenceFlow>\n` +
    `    <bpmn:sequenceFlow id="f_loop" sourceRef="Gw" targetRef="Orchestrate" />\n` +
    `  </bpmn:process>\n` +
    `</bpmn:definitions>\n`
  );
}

export const deployWorkflowB = (baseUrl, def) => deployXml(baseUrl, def.id, toBpmnB(def));
export const startWorkflowB = (baseUrl, def, input = {}) => startWorkflow(baseUrl, { id: def.id }, { input, journal: {}, wfDone: false });

/**
 * Replay the orchestration function against a journal. Returns either
 * { done:true } (function ran to completion) or { frontier:{key,result} } (a new
 * step executed and must be journalled before the next turn).
 */
async function replayOnce(def, input, journal) {
  let ordinal = 0;
  const ctx = {
    input,
    async run(name, fn) {
      const key = `${++ordinal}:${name}`;
      if (Object.prototype.hasOwnProperty.call(journal, key)) return journal[key]; // replay: no side effect
      const result = (await fn()) ?? null; // frontier: the ONE real side effect this turn
      const err = new Error("suspend");
      err[SUSPEND] = { key, result };
      throw err;
    },
  };
  try {
    await def.orchestrate(ctx);
    return { done: true };
  } catch (e) {
    if (e && e[SUSPEND]) return { frontier: e[SUSPEND] };
    throw e; // a genuine error in the orchestration/handler
  }
}

/**
 * The generic orchestrator worker: one job type per workflow (`<id>:__orchestrate`).
 * Each activated job carries the current {input, journal}; we replay, advance one
 * step (or finish), and complete the job with the updated journal (loop) or
 * wfDone=true (exit). Resilient to the engine disappearing (reconnects on its own).
 */
export function runOrchestrator(baseUrl, def, { onStep, onError } = {}) {
  let running = true;
  async function activate() {
    try {
      const res = await fetch(`${baseUrl}/v2/jobs/activation`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ type: def.orchestrateType, worker: `${def.id}-orch`, maxJobsToActivate: 1, timeout: 30000, requestTimeout: -1 }),
      });
      if (!res.ok) return [];
      return (await res.json()).jobs ?? [];
    } catch { return []; }
  }
  async function complete(jobKey, variables) {
    try {
      const res = await fetch(`${baseUrl}/v2/jobs/${jobKey}/completion`, {
        method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ variables }),
      });
      return res.ok;
    } catch { return false; }
  }

  (async () => {
    while (running) {
      let did = false;
      for (const job of await activate()) {
        did = true;
        try {
          const input = job.variables?.input ?? {};
          const journal = job.variables?.journal ?? {};
          const step = await replayOnce(def, input, journal);
          if (step.done) {
            await complete(job.jobKey, { wfDone: true });
            if (onStep) await onStep({ done: true });
          } else {
            const next = { ...journal, [step.frontier.key]: step.frontier.result };
            await complete(job.jobKey, { journal: next, wfDone: false });
            if (onStep) await onStep({ key: step.frontier.key });
          }
        } catch (e) {
          if (onError) onError(e);
        }
      }
      if (!did) await sleep(100);
    }
  })();

  return { stop() { running = false; } };
}
