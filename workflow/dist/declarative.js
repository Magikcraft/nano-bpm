// Strategy A — the declarative surface: a step list compiled to a linear model.
//
//   const flow = defineFlow("pr-review", (w) => {
//     w.run("fetchDiff",  async (job) => ({ diff: await gh.diff(job.variables.prId) }));
//     w.run("autoReview", async (job) => ({ findings: await llm.review(job.variables.diff) }));
//     w.signal("humanApproval", { correlationKey: "prId" });   // durable human wait
//     w.run("merge",      async (job) => ({ merged: await gh.merge(job.variables.prId) }));
//   });
//
// The façade derives the BPMN model, the job types (`<id>:<step>`), the message
// name + `zeebe:subscription correlationKey`, and (via Worker) a generic worker.
// This surface supports human-in-the-loop signals today.
import { assertIdent, escapeXml, jobType, messageName } from "./xml.js";
/** Define a declarative flow. `build(w)` declares an ordered list of steps. */
export function defineFlow(id, build) {
    assertIdent("workflow id", id);
    const steps = [];
    const handlers = {};
    const seen = new Set();
    const w = {
        run(name, handler) {
            assertIdent("step name", name);
            if (seen.has(name))
                throw new Error(`duplicate step name "${name}" in flow "${id}"`);
            if (typeof handler !== "function")
                throw new Error(`run("${name}") needs a handler function`);
            seen.add(name);
            steps.push({ kind: "run", name });
            handlers[name] = handler;
            return w;
        },
        signal(name, opts) {
            assertIdent("step name", name);
            if (seen.has(name))
                throw new Error(`duplicate step name "${name}" in flow "${id}"`);
            if (!opts || !opts.correlationKey)
                throw new Error(`signal("${name}") needs { correlationKey }`);
            assertIdent("correlationKey", opts.correlationKey);
            seen.add(name);
            steps.push({ kind: "signal", name, correlationKey: opts.correlationKey });
            return w;
        },
    };
    build(w);
    if (steps.length === 0)
        throw new Error(`flow "${id}" declared no steps`);
    return { kind: "declarative", id, steps, handlers };
}
/** Derive an executable BPMN model from a declarative flow. */
export function declarativeToBpmn(flow) {
    const nodes = [];
    const flows = [];
    const messages = [];
    const ids = ["Start", ...flow.steps.map((s) => s.name), "End"];
    nodes.push(`    <bpmn:startEvent id="Start"><bpmn:outgoing>flow_0</bpmn:outgoing></bpmn:startEvent>`);
    flow.steps.forEach((step, i) => {
        const incoming = `flow_${i}`;
        const outgoing = `flow_${i + 1}`;
        if (step.kind === "run") {
            nodes.push(`    <bpmn:serviceTask id="${escapeXml(step.name)}" name="${escapeXml(step.name)}">\n` +
                `      <bpmn:extensionElements><zeebe:taskDefinition type="${escapeXml(jobType(flow.id, step.name))}" /></bpmn:extensionElements>\n` +
                `      <bpmn:incoming>${incoming}</bpmn:incoming><bpmn:outgoing>${outgoing}</bpmn:outgoing>\n` +
                `    </bpmn:serviceTask>`);
        }
        else {
            const msgId = `Msg_${escapeXml(step.name)}`;
            nodes.push(`    <bpmn:intermediateCatchEvent id="${escapeXml(step.name)}" name="${escapeXml(step.name)}">\n` +
                `      <bpmn:incoming>${incoming}</bpmn:incoming><bpmn:outgoing>${outgoing}</bpmn:outgoing>\n` +
                `      <bpmn:messageEventDefinition messageRef="${msgId}" />\n` +
                `    </bpmn:intermediateCatchEvent>`);
            messages.push(`  <bpmn:message id="${msgId}" name="${escapeXml(messageName(flow.id, step.name))}">\n` +
                `    <bpmn:extensionElements><zeebe:subscription correlationKey="=${escapeXml(step.correlationKey)}" /></bpmn:extensionElements>\n` +
                `  </bpmn:message>`);
        }
    });
    nodes.push(`    <bpmn:endEvent id="End"><bpmn:incoming>flow_${flow.steps.length}</bpmn:incoming></bpmn:endEvent>`);
    for (let i = 0; i < ids.length - 1; i++) {
        flows.push(`    <bpmn:sequenceFlow id="flow_${i}" sourceRef="${ids[i]}" targetRef="${ids[i + 1]}" />`);
    }
    return (`<?xml version="1.0" encoding="UTF-8"?>\n` +
        `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Definitions_${escapeXml(flow.id)}" targetNamespace="http://bpmn.io/schema/bpmn">\n` +
        `  <bpmn:process id="${escapeXml(flow.id)}" name="${escapeXml(flow.id)}" isExecutable="true">\n` +
        nodes.join("\n") +
        "\n" +
        flows.join("\n") +
        "\n" +
        `  </bpmn:process>\n` +
        messages.join("\n") +
        (messages.length ? "\n" : "") +
        `</bpmn:definitions>\n`);
}
