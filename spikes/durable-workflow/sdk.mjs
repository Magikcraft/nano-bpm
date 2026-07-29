// Durable-workflow spike — increment 2: a minimal CODE-FIRST authoring façade.
//
// The spike (run.mjs) proved the durability. This shows the *ergonomics*: a
// developer writes a workflow as data/code — `w.run(name, handler)` for a
// durable activity, `w.signal(name, { correlationKey })` for a human/external
// wait — and the façade DERIVES everything the engine needs:
//
//   - a BPMN model (start → steps → end); no diagram authored by hand
//   - the service-task job types (`<id>:<step>`); no worker↔task-type wiring
//   - the message + `zeebe:subscription`; no correlation plumbing authored
//   - a single generic worker that dispatches jobs to the handlers
//
// The instance that results is an ordinary nanobpmn process instance, so it
// inherits the crash-resume durability proven in run.mjs (resume-code-first.mjs
// re-runs the engine-kill proof against a code-authored workflow).
//
// This is deliberately the "compile a declarative workflow to a model" strategy
// (ADR 0044 Strategy A): it reuses the engine's proven durability directly and
// needs no replay/determinism discipline. The imperative `ctx.run`/`await`
// replay style (Strategy B) is the later increment.

import { readFileSync } from "node:fs";

// ---------------------------------------------------------------------------
// Authoring surface
// ---------------------------------------------------------------------------

/**
 * Define a workflow. `build(w)` declares an ordered list of steps:
 *   w.run("fetchDiff", async (job) => ({ diff }))   // durable activity
 *   w.signal("submitReview", { correlationKey: "prId" })  // external/human wait
 *
 * Returns a definition object carrying the derived model + handler map.
 */
export function defineWorkflow(id, build) {
  const steps = [];
  const handlers = {};
  const w = {
    run(name, handler) {
      if (typeof handler !== "function") throw new Error(`run("${name}") needs a handler function`);
      steps.push({ kind: "run", name });
      handlers[name] = handler;
      return w;
    },
    signal(name, opts = {}) {
      if (!opts.correlationKey) throw new Error(`signal("${name}") needs { correlationKey }`);
      steps.push({ kind: "signal", name, correlationKey: opts.correlationKey });
      return w;
    },
  };
  build(w);
  if (steps.length === 0) throw new Error(`workflow "${id}" declared no steps`);
  return { id, steps, handlers };
}

/** Derived job type for a run step — no hand-wiring, just <id>:<step>. */
export const jobTypeFor = (def, step) => `${def.id}:${step}`;
/** Derived message name for a signal step. */
export const messageNameFor = (def, step) => `${def.id}:${step}`;

// ---------------------------------------------------------------------------
// Model derivation (workflow definition → BPMN)
// ---------------------------------------------------------------------------

const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

/** Derive an executable BPMN model from the declared steps. */
export function toBpmn(def) {
  const nodes = [];
  const flows = [];
  const messages = [];
  const ids = ["Start", ...def.steps.map((s) => s.name), "End"];

  nodes.push(`    <bpmn:startEvent id="Start"><bpmn:outgoing>flow_0</bpmn:outgoing></bpmn:startEvent>`);
  def.steps.forEach((step, i) => {
    const incoming = `flow_${i}`;
    const outgoing = `flow_${i + 1}`;
    if (step.kind === "run") {
      nodes.push(
        `    <bpmn:serviceTask id="${esc(step.name)}" name="${esc(step.name)}">\n` +
          `      <bpmn:extensionElements><zeebe:taskDefinition type="${esc(jobTypeFor(def, step.name))}" /></bpmn:extensionElements>\n` +
          `      <bpmn:incoming>${incoming}</bpmn:incoming><bpmn:outgoing>${outgoing}</bpmn:outgoing>\n` +
          `    </bpmn:serviceTask>`,
      );
    } else {
      const msgId = `Msg_${esc(step.name)}`;
      nodes.push(
        `    <bpmn:intermediateCatchEvent id="${esc(step.name)}" name="${esc(step.name)}">\n` +
          `      <bpmn:incoming>${incoming}</bpmn:incoming><bpmn:outgoing>${outgoing}</bpmn:outgoing>\n` +
          `      <bpmn:messageEventDefinition messageRef="${msgId}" />\n` +
          `    </bpmn:intermediateCatchEvent>`,
      );
      messages.push(
        `  <bpmn:message id="${msgId}" name="${esc(messageNameFor(def, step.name))}">\n` +
          `    <bpmn:extensionElements><zeebe:subscription correlationKey="=${esc(step.correlationKey)}" /></bpmn:extensionElements>\n` +
          `  </bpmn:message>`,
      );
    }
  });
  nodes.push(`    <bpmn:endEvent id="End"><bpmn:incoming>flow_${def.steps.length}</bpmn:incoming></bpmn:endEvent>`);
  for (let i = 0; i < ids.length - 1; i++) {
    flows.push(`    <bpmn:sequenceFlow id="flow_${i}" sourceRef="${ids[i]}" targetRef="${ids[i + 1]}" />`);
  }

  return (
    `<?xml version="1.0" encoding="UTF-8"?>\n` +
    `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Definitions_${esc(def.id)}" targetNamespace="http://bpmn.io/schema/bpmn">\n` +
    `  <bpmn:process id="${esc(def.id)}" name="${esc(def.id)}" isExecutable="true">\n` +
    nodes.join("\n") + "\n" + flows.join("\n") + "\n" +
    `  </bpmn:process>\n` +
    messages.join("\n") + (messages.length ? "\n" : "") +
    `</bpmn:definitions>\n`
  );
}

// ---------------------------------------------------------------------------
// Runtime (deploy / start / worker / signal) — plain fetch, no SDK dependency
// ---------------------------------------------------------------------------

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export async function deployWorkflow(baseUrl, def) {
  const xml = toBpmn(def);
  const form = new FormData();
  form.append("resources", new Blob([xml], { type: "text/xml" }), `${def.id}.bpmn`);
  const res = await fetch(`${baseUrl}/v2/deployments`, { method: "POST", body: form });
  if (!res.ok) throw new Error(`deploy failed: ${res.status} ${await res.text()}`);
  return res.json();
}

export async function startWorkflow(baseUrl, def, variables = {}) {
  const res = await fetch(`${baseUrl}/v2/process-instances`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ processDefinitionId: def.id, variables }),
  });
  if (!res.ok) throw new Error(`start failed: ${res.status} ${await res.text()}`);
  return res.json();
}

export async function sendSignal(baseUrl, def, signalName, correlationKey, variables = {}) {
  const res = await fetch(`${baseUrl}/v2/messages/correlation`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ name: messageNameFor(def, signalName), correlationKey, variables }),
  });
  if (!res.ok) throw new Error(`signal "${signalName}" failed: ${res.status} ${await res.text()}`);
  return res.json();
}

/**
 * A single generic worker that serves every `run` step of a workflow by
 * dispatching activated jobs to the declared handler. Resilient to the engine
 * disappearing (so it reconnects on its own after a restart). Returns a handle
 * with `.stop()`.
 */
export function runWorker(baseUrl, def, { onError, onCommitted } = {}) {
  const runSteps = def.steps.filter((s) => s.kind === "run").map((s) => s.name);
  let running = true;

  async function activate(type) {
    try {
      const res = await fetch(`${baseUrl}/v2/jobs/activation`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ type: jobTypeFor(def, type), worker: `${def.id}-worker`, maxJobsToActivate: 1, timeout: 30000, requestTimeout: -1 }),
      });
      if (!res.ok) return [];
      return (await res.json()).jobs ?? [];
    } catch {
      return [];
    }
  }
  async function complete(jobKey, variables) {
    try {
      const res = await fetch(`${baseUrl}/v2/jobs/${jobKey}/completion`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ variables: variables ?? {} }),
      });
      return res.ok;
    } catch {
      return false;
    }
  }

  (async () => {
    while (running) {
      let did = false;
      for (const step of runSteps) {
        for (const job of await activate(step)) {
          did = true;
          try {
            const out = (await def.handlers[step](job)) ?? {};
            const ok = await complete(job.jobKey, out);
            // Post-commit hook: fires only after the engine durably records the
            // completion. A crash after the side effect but before this line
            // means the job is redelivered on resume (at-least-once boundary).
            if (ok && onCommitted) await onCommitted(step, job);
          } catch (e) {
            if (onError) onError(step, e);
          }
        }
      }
      if (!did) await sleep(120);
    }
  })();

  return { stop() { running = false; } };
}

/** Convenience for demos/tests: load a BPMN file's bytes (unused by the SDK). */
export const readXml = (p) => readFileSync(p, "utf8");
