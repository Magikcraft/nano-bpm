// Strategy A — the declarative surface: a TREE of nodes compiled to a real BPMN
// model. Leaf activities (`run`/`task`/`signal`) plus structural combinators
// (`switch`/`branch`/`loop`) that compile to exclusive gateways + back-edges the
// engine already runs (engine-core supports XOR gateways with in-order condition
// evaluation and a default flow, and tasks with multiple incoming flows act as
// an implicit XOR merge).
//
//   const convergence = defineFlow(
//     "convergence-loop",
//     {                                             // contracts keyed by step name
//       "review-round": { in: PrReviewRoundIn, out: PrReviewRoundOut },
//       "persist-round": { out: RoundState },
//       "wait-review":  { in: ReviewReady },
//       "wait-answer":  { in: EscalationAnswered },
//     },
//     (w) => {
//       w.loop((b) => {
//         b.run("review-round", reviewRound);       // job.variables typed from the contract
//         b.switch("status", {
//           converged: (c) => { c.run("persist-converged", finalize); c.break(); },
//           addressed: (c) => c.branch("round >= maxRounds", {
//             then: (g) => { g.run("persist-escalation-maxrounds", persistEsc);
//                            g.signal("wait-answer", { correlationKey: "prKey" }); },
//             else: (g) => { g.run("persist-round", persistRound);   // returns { round: round + 1 }
//                            g.signal("wait-review", { correlationKey: "prKey" }); },
//           }),
//           default: (c) => { c.run("persist-escalation", persistEsc);
//                             c.signal("wait-answer", { correlationKey: "prKey" }); },
//         });
//       });
//     },
//   );
//
// Typed data envelopes are LIFTED into the model (nano:shape + dataEnvelope
// zeebe:property), so the generated .bpmn is ejectable to model-first with its
// typed contracts intact.
import { assertIdent, escapeXml, jobType, messageName } from "./xml.js";
/** Ids the emitter generates for structural nodes / flows / messages. A step
 *  name that collides with one of these would produce a duplicate BPMN id and an
 *  invalid model, so reject them at authoring time. */
const RESERVED_PREFIXES = /^(Gw_|Loop_|Msg_|f_)/;
function claimName(ctx, id, name) {
    assertIdent("step name", name);
    if (name === "Start" || name === "End" || name === id) {
        throw new Error(`step name "${name}" is reserved (collides with a generated BPMN id) in flow "${id}"`);
    }
    if (RESERVED_PREFIXES.test(name)) {
        throw new Error(`step name "${name}" uses a reserved prefix (Gw_/Loop_/Msg_/f_ are generated ids) in flow "${id}"`);
    }
    if (ctx.seen.has(name))
        throw new Error(`duplicate step name "${name}" in flow "${id}"`);
    ctx.seen.add(name);
}
/** Resolve a step's declared envelopes from the flow contracts. */
function contractEnvelopes(ctx, name) {
    const c = ctx.contracts[name];
    if (!c || (!c.in && !c.out))
        return undefined;
    return { in: c.in, out: c.out };
}
/** Build a FlowBuilder that appends its nodes to `out`, sharing the flow-wide
 *  `ctx` (contracts, handler registry, name set, loop nesting). Structural
 *  combinators recurse with a fresh `out` array for each nested body. */
function makeBuilder(id, out, ctx) {
    const child = (fn, inLoop) => {
        const body = [];
        const depth = inLoop ? ctx.loopDepth + 1 : ctx.loopDepth;
        fn(makeBuilder(id, body, { ...ctx, loopDepth: depth }));
        return body;
    };
    const b = {
        run(name, handler) {
            claimName(ctx, id, name);
            if (typeof handler !== "function")
                throw new Error(`run("${name}") needs a handler function`);
            ctx.handlers[name] = handler;
            out.push({ kind: "run", name, envelopes: contractEnvelopes(ctx, name) });
            return b;
        },
        task(name) {
            claimName(ctx, id, name);
            out.push({ kind: "task", name, envelopes: contractEnvelopes(ctx, name) });
            return b;
        },
        signal(name, opts) {
            claimName(ctx, id, name);
            if (!opts || !opts.correlationKey)
                throw new Error(`signal("${name}") needs { correlationKey }`);
            assertIdent("correlationKey", opts.correlationKey);
            out.push({ kind: "signal", name, correlationKey: opts.correlationKey, payload: ctx.contracts[name]?.in });
            return b;
        },
        switch(subject, cases) {
            if (typeof subject !== "string" || subject.trim() === "") {
                throw new Error(`switch() needs a non-empty subject expression`);
            }
            const entries = Object.entries(cases).filter(([k]) => k !== "default");
            if (entries.length === 0)
                throw new Error(`switch("${subject}") needs at least one case`);
            const caseNodes = entries.map(([value, fn]) => ({ value, body: child(fn, false) }));
            const def = cases.default ? child(cases.default, false) : undefined;
            out.push({ kind: "switch", subject, cases: caseNodes, default: def });
            return b;
        },
        branch(condition, arms) {
            if (typeof condition !== "string" || condition.trim() === "") {
                throw new Error(`branch() needs a non-empty FEEL condition`);
            }
            if (!arms || typeof arms.then !== "function")
                throw new Error(`branch("${condition}") needs a then arm`);
            out.push({
                kind: "branch",
                condition,
                then: child(arms.then, false),
                else: arms.else ? child(arms.else, false) : undefined,
            });
            return b;
        },
        loop(body) {
            if (typeof body !== "function")
                throw new Error(`loop() needs a body function`);
            out.push({ kind: "loop", body: child(body, true) });
            return b;
        },
        break() {
            if (ctx.loopDepth === 0)
                throw new Error(`break() is only valid inside a loop`);
            out.push({ kind: "break" });
            return b;
        },
        continue() {
            if (ctx.loopDepth === 0)
                throw new Error(`continue() is only valid inside a loop`);
            out.push({ kind: "continue" });
            return b;
        },
    };
    return b;
}
export function defineFlow(id, second, third) {
    assertIdent("workflow id", id);
    if (typeof second !== "function" && (second === null || typeof second !== "object")) {
        throw new Error(`defineFlow("${id}"): the contracts argument must be an object`);
    }
    const contracts = typeof second === "function" ? {} : second;
    const build = (typeof second === "function" ? second : third);
    if (typeof build !== "function") {
        throw new Error(`defineFlow("${id}"): a build callback (w) => {…} is required`);
    }
    const steps = [];
    const handlers = {};
    const ctx = { contracts, handlers, seen: new Set(), loopDepth: 0 };
    build(makeBuilder(id, steps, ctx));
    if (steps.length === 0)
        throw new Error(`flow "${id}" declared no steps`);
    return { kind: "declarative", id, steps, handlers };
}
// --- Tree walkers ------------------------------------------------------------
/** Depth-first visit of every node in a flow tree (structural combinators
 *  recurse into their bodies). */
export function walkNodes(nodes, visit) {
    for (const n of nodes) {
        visit(n);
        switch (n.kind) {
            case "switch":
                for (const c of n.cases)
                    walkNodes(c.body, visit);
                if (n.default)
                    walkNodes(n.default, visit);
                break;
            case "branch":
                walkNodes(n.then, visit);
                if (n.else)
                    walkNodes(n.else, visit);
                break;
            case "loop":
                walkNodes(n.body, visit);
                break;
            default:
                break;
        }
    }
}
/** The derived job types of a flow's external `task` steps (anywhere in the
 *  tree) — the contract workers outside this program must subscribe to. */
export function externalJobTypes(flow) {
    const types = [];
    walkNodes(flow.steps, (n) => {
        if (n.kind === "task")
            types.push(jobType(flow.id, n.name));
    });
    return types;
}
class Compiler {
    flow;
    nodes = [];
    edges = [];
    /** envelope name → its fields, deduped for lifting to a single nano:shape. */
    envelopes = new Map();
    seq = 0;
    gw = 0;
    constructor(flow) {
        this.flow = flow;
    }
    newEdge(from, opts = {}) {
        const e = { id: `f_${this.seq++}`, from, condition: opts.condition, name: opts.name };
        this.edges.push(e);
        return e;
    }
    connect(incoming, toId) {
        for (const e of incoming)
            e.to = toId;
    }
    recordEnvelope(env) {
        if (!env)
            return;
        const prev = this.envelopes.get(env.name);
        if (prev) {
            if (JSON.stringify(prev) !== JSON.stringify(env.fields)) {
                throw new Error(`envelope "${env.name}" is declared with two different field sets in flow "${this.flow.id}"`);
            }
            return;
        }
        this.envelopes.set(env.name, env.fields);
    }
    addServiceTask(node) {
        const type = jobType(this.flow.id, node.name);
        this.recordEnvelope(node.envelopes?.in);
        this.recordEnvelope(node.envelopes?.out);
        const props = [];
        if (node.envelopes?.in)
            props.push(envelopeProp("in", node.envelopes.in.name));
        if (node.envelopes?.out)
            props.push(envelopeProp("out", node.envelopes.out.name));
        const ext = `      <bpmn:extensionElements>\n` +
            `        <zeebe:taskDefinition type="${escapeXml(type)}" />\n` +
            (props.length ? `        <zeebe:properties>\n${props.join("\n")}\n        </zeebe:properties>\n` : "") +
            `      </bpmn:extensionElements>`;
        const id = node.name;
        this.nodes.push({
            id,
            render: (inc, outg) => `    <bpmn:serviceTask id="${escapeXml(id)}" name="${escapeXml(id)}">\n` +
                ext +
                "\n" +
                incomingOutgoing(inc, outg) +
                `    </bpmn:serviceTask>`,
        });
    }
    addCatchEvent(node) {
        const id = node.name;
        const msgId = `Msg_${id}`;
        this.nodes.push({
            id,
            render: (inc, outg) => `    <bpmn:intermediateCatchEvent id="${escapeXml(id)}" name="${escapeXml(id)}">\n` +
                incomingOutgoing(inc, outg) +
                `      <bpmn:messageEventDefinition messageRef="${msgId}" />\n` +
                `    </bpmn:intermediateCatchEvent>`,
        });
    }
    addGateway(id, name) {
        const gwNode = {
            id,
            render: (inc, outg) => {
                const def = gwNode.defaultFlow ? ` default="${gwNode.defaultFlow}"` : "";
                const nm = name ? ` name="${escapeXml(name)}"` : "";
                return (`    <bpmn:exclusiveGateway id="${id}"${nm}${def}>\n` +
                    incomingOutgoing(inc, outg) +
                    `    </bpmn:exclusiveGateway>`);
            },
        };
        this.nodes.push(gwNode);
        return gwNode;
    }
    emitList(list, incoming, loop) {
        let cur = incoming;
        for (const node of list)
            cur = this.emitNode(node, cur, loop);
        return cur;
    }
    emitNode(node, incoming, loop) {
        switch (node.kind) {
            case "run":
            case "task": {
                this.addServiceTask(node);
                this.connect(incoming, node.name);
                return [this.newEdge(node.name)];
            }
            case "signal": {
                this.addCatchEvent(node);
                this.connect(incoming, node.name);
                this.recordEnvelope(node.payload);
                return [this.newEdge(node.name)];
            }
            case "switch": {
                const id = `Gw_${this.gw++}`;
                const gw = this.addGateway(id, node.subject);
                this.connect(incoming, id);
                const out = [];
                for (const c of node.cases) {
                    const e = this.newEdge(id, { condition: feelEquals(node.subject, c.value), name: c.value });
                    out.push(...this.emitList(c.body, [e], loop));
                }
                // The default (or a synthesised fall-through) is the unconditional edge.
                const de = this.newEdge(id, { name: "default" });
                gw.defaultFlow = de.id;
                out.push(...this.emitList(node.default ?? [], [de], loop));
                return out;
            }
            case "branch": {
                const id = `Gw_${this.gw++}`;
                const gw = this.addGateway(id);
                this.connect(incoming, id);
                const te = this.newEdge(id, { condition: feel(node.condition), name: "then" });
                const out = this.emitList(node.then, [te], loop);
                const ee = this.newEdge(id, { name: "else" });
                gw.defaultFlow = ee.id;
                out.push(...this.emitList(node.else ?? [], [ee], loop));
                return out;
            }
            case "loop": {
                const headId = `Loop_${this.gw++}`;
                this.addGateway(headId);
                this.connect(incoming, headId);
                const ctx = { headId, breaks: [] };
                const headOut = this.newEdge(headId);
                const bodyOut = this.emitList(node.body, [headOut], ctx);
                // Normal fall-through of the body loops back to the head (continue).
                this.connect(bodyOut, headId);
                // The loop exits only via break edges.
                return ctx.breaks;
            }
            case "break": {
                if (!loop)
                    throw new Error(`break outside a loop in flow "${this.flow.id}"`);
                // The incoming edges (from the previous node) become the loop's exit
                // danglers; this path does not fall through.
                loop.breaks.push(...incoming);
                return [];
            }
            case "continue": {
                if (!loop)
                    throw new Error(`continue outside a loop in flow "${this.flow.id}"`);
                this.connect(incoming, loop.headId);
                return [];
            }
        }
    }
    compile() {
        // Start → top-level sequence → End.
        this.nodes.push({
            id: "Start",
            render: (_inc, outg) => `    <bpmn:startEvent id="Start">${outgoingOnly(outg)}</bpmn:startEvent>`,
        });
        const s0 = this.newEdge("Start");
        const finalDanglers = this.emitList(this.flow.steps, [s0], null);
        this.nodes.push({
            id: "End",
            render: (inc) => `    <bpmn:endEvent id="End">${incomingOnly(inc)}</bpmn:endEvent>`,
        });
        this.connect(finalDanglers, "End");
        // A sequence flow needs both ends; drop any edge that never got a target.
        const live = this.edges.filter((e) => e.to !== undefined);
        const incomingOf = (id) => live.filter((e) => e.to === id).map((e) => e.id);
        const outgoingOf = (id) => live.filter((e) => e.from === id).map((e) => e.id);
        const nodeXml = this.nodes.map((n) => n.render(incomingOf(n.id), outgoingOf(n.id))).join("\n");
        const flowXml = live.map((e) => sequenceFlow(e)).join("\n");
        const messageXml = this.emitMessages();
        const shapeXml = this.emitShapes();
        return (`<?xml version="1.0" encoding="UTF-8"?>\n` +
            `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" ` +
            `xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" ` +
            `xmlns:nano="https://nanobpm.io/schema/shapes/1.0" ` +
            `id="Definitions_${escapeXml(this.flow.id)}" targetNamespace="http://bpmn.io/schema/bpmn">\n` +
            `  <bpmn:process id="${escapeXml(this.flow.id)}" name="${escapeXml(this.flow.id)}" isExecutable="true">\n` +
            (shapeXml ? shapeXml + "\n" : "") +
            nodeXml +
            "\n" +
            flowXml +
            "\n" +
            `  </bpmn:process>\n` +
            messageXml +
            (messageXml ? "\n" : "") +
            `</bpmn:definitions>\n`);
    }
    /** Emit a `<bpmn:message>` per signal step, with its subscription and (when
     *  typed) its payload data envelope. */
    emitMessages() {
        const msgs = [];
        walkNodes(this.flow.steps, (n) => {
            if (n.kind !== "signal")
                return;
            const msgId = `Msg_${n.name}`;
            const payloadProp = n.payload
                ? `\n      <zeebe:properties>\n${envelopeProp("in", n.payload.name)}\n      </zeebe:properties>`
                : "";
            msgs.push(`  <bpmn:message id="${msgId}" name="${escapeXml(messageName(this.flow.id, n.name))}">\n` +
                `    <bpmn:extensionElements>\n` +
                `      <zeebe:subscription correlationKey="=${escapeXml(n.correlationKey)}" />` +
                payloadProp +
                `\n    </bpmn:extensionElements>\n` +
                `  </bpmn:message>`);
        });
        return msgs.join("\n");
    }
    /** Lift the referenced data envelopes into a `<nano:shapes>` container on the
     *  process extension elements, so the model carries the typed contracts and is
     *  ejectable to model-first. */
    emitShapes() {
        if (this.envelopes.size === 0)
            return "";
        const shapes = [...this.envelopes.entries()].map(([name, fields]) => {
            const exts = fields.map((f) => {
                const opt = f.optional ? ` optional="true"` : "";
                const list = f.list ? ` list="true"` : "";
                return `        <nano:extend name="${escapeXml(f.name)}" type="${escapeXml(f.type)}"${opt}${list} />`;
            });
            return `      <nano:shape id="${escapeXml(name)}">\n${exts.join("\n")}\n      </nano:shape>`;
        });
        return (`    <bpmn:extensionElements>\n` +
            `      <nano:shapes>\n${shapes.join("\n")}\n      </nano:shapes>\n` +
            `    </bpmn:extensionElements>`);
    }
}
/** Derive an executable BPMN model from a declarative flow. */
export function declarativeToBpmn(flow) {
    return new Compiler(flow).compile();
}
// --- small XML / FEEL helpers ------------------------------------------------
const envelopeProp = (dir, value) => `          <zeebe:property name="io.nanobpm.dataEnvelope.${dir}" value="${escapeXml(value)}" />`;
/** Wrap a raw FEEL expression as a Zeebe condition body (leading `=`). */
const feel = (expr) => `=${expr}`;
/** A FEEL equality test `subject = "value"`, with the value as a FEEL string. */
const feelEquals = (subject, value) => `=${subject} = "${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
function incomingOutgoing(inc, outg) {
    return (inc.map((f) => `      <bpmn:incoming>${f}</bpmn:incoming>\n`).join("") +
        outg.map((f) => `      <bpmn:outgoing>${f}</bpmn:outgoing>\n`).join(""));
}
const incomingOnly = (inc) => inc.map((f) => `<bpmn:incoming>${f}</bpmn:incoming>`).join("");
const outgoingOnly = (outg) => outg.map((f) => `<bpmn:outgoing>${f}</bpmn:outgoing>`).join("");
function sequenceFlow(e) {
    const nm = e.name ? ` name="${escapeXml(e.name)}"` : "";
    if (e.condition) {
        return (`    <bpmn:sequenceFlow id="${e.id}" sourceRef="${e.from}" targetRef="${e.to}"${nm}>\n` +
            `      <bpmn:conditionExpression>${escapeXml(e.condition)}</bpmn:conditionExpression>\n` +
            `    </bpmn:sequenceFlow>`);
    }
    return `    <bpmn:sequenceFlow id="${e.id}" sourceRef="${e.from}" targetRef="${e.to}"${nm} />`;
}
