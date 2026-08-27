// End-to-end probe of the engine-native AgentInstance / AgentHistory surface
// (Camunda stable/8.10 parity, Stage 3) through the @nanobpm/engine-wasm
// read-model TestEngine — the JS console/Bojtos tier.
//
// Deploy an aiAgentTask serviceTask -> create an instance (activation mints an
// AgentInstance in INITIALIZING) -> reconcile via createAgentInstance -> push a
// turn via updateAgentInstance (status advances) -> history-search returns that
// COMMITTED turn -> completeAgentInstance drives COMPLETED.
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

let failed = false;
function assert(cond, msg) {
  if (cond) {
    console.log(`\u2713 ${msg}`);
  } else {
    failed = true;
    console.error(`\u2717 ${msg}`);
  }
}

const AGENT_PROC = `
  <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                    xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
    <bpmn:process id="agent-proc" isExecutable="true">
      <bpmn:startEvent id="start" />
      <bpmn:serviceTask id="agent">
        <bpmn:extensionElements>
          <zeebe:agentDefinition agentType="aiAgentTask" />
        </bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:endEvent id="end" />
      <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="agent" />
      <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="end" />
    </bpmn:process>
  </bpmn:definitions>`;

// Load the read-model entrypoint (the AgentInstance read methods are compiled
// only behind the `read-model` feature / `/readmodel` subpath).
const { initSync, TestEngine } = await import("@nanobpm/engine-wasm/readmodel");
initSync({
  module: readFileSync(
    require.resolve("@nanobpm/engine-wasm/readmodel/nanobpmn_engine_bg.wasm"),
  ),
});

const engine = new TestEngine();

// 1. Deploy the aiAgentTask serviceTask and start an instance.
engine.deploy(AGENT_PROC);
engine.createInstance("agent-proc", "{}");

// 2. Activation mints an AgentInstance in INITIALIZING (no job created).
let instances = JSON.parse(engine.searchAgentInstances("{}")).items;
assert(instances.length === 1, "activation mints exactly one AgentInstance");
const minted = instances[0];
assert(
  minted.status === "INITIALIZING",
  `minted instance is INITIALIZING (got ${JSON.stringify(minted.status)})`,
);
const agentInstanceKey = minted.agentInstanceKey;
const elementInstanceKey = minted.elementInstanceKey;
const elementId = minted.elementId;
const processInstanceKey = minted.processInstanceKey;
assert(
  typeof agentInstanceKey === "string" && agentInstanceKey !== "0",
  "instance carries its own dedicated agentInstanceKey",
);
assert(
  agentInstanceKey !== elementInstanceKey,
  "the agentInstanceKey is distinct from the elementInstanceKey",
);

// 3. CREATE reconciles the auto-minted record (same key), applying a CREATE-time
//    definition + limits + a configuration turn — still INITIALIZING.
engine.createAgentInstance(
  JSON.stringify({
    elementInstanceKey,
    definition: { model: "gpt-4o", provider: "openai" },
    limits: { maxTokens: 1000, maxModelCalls: 10, maxToolCalls: 5 },
    history: [
      { loopIteration: 0, producedAt: 10, role: "CONFIGURATION" },
    ],
  }),
);
instances = JSON.parse(engine.searchAgentInstances("{}")).items;
assert(
  instances.length === 1,
  "CREATE reconciles (no duplicate) — still exactly one instance",
);
const created = instances[0];
assert(
  created.agentInstanceKey === agentInstanceKey,
  "CREATE reconciles to the same agentInstanceKey",
);
assert(
  created.status === "INITIALIZING",
  `reconciled instance is still INITIALIZING (got ${JSON.stringify(created.status)})`,
);
assert(
  created.definition.model === "gpt-4o",
  "CREATE-time definition (model) is applied",
);
assert(
  created.limits.maxTokens === 1000 &&
    created.limits.maxModelCalls === 10 &&
    created.limits.maxToolCalls === 5,
  "CREATE-time limits are applied",
);

// 4. UPDATE advances the status to THINKING, accumulates metrics, and pushes an
//    ASSISTANT turn.
engine.updateAgentInstance(
  JSON.stringify({
    agentInstanceKey,
    elementInstanceKey,
    elementId,
    processInstanceKey,
    status: "THINKING",
    metrics: { inputTokens: 100, outputTokens: 20, modelCalls: 1 },
    history: [
      {
        loopIteration: 1,
        producedAt: 100,
        role: "ASSISTANT",
        content: [{ contentType: "TEXT", text: "hello from the agent" }],
      },
    ],
  }),
);
const advanced = JSON.parse(engine.searchAgentInstances("{}")).items[0];
assert(
  advanced.status === "THINKING",
  `UPDATE advances status to THINKING (got ${JSON.stringify(advanced.status)})`,
);
assert(
  advanced.metrics.inputTokens === 100 && advanced.metrics.modelCalls === 1,
  "UPDATE accumulates metrics onto the instance",
);

// 5. History-search returns the appended turn(s), defaulting to COMMITTED.
const history = JSON.parse(
  engine.searchAgentInstanceHistory(agentInstanceKey, "{}"),
).items;
assert(
  history.length >= 1,
  `history-search returns the appended turn(s) (got ${history.length})`,
);
assert(
  history.every((t) => t.commitStatus === "COMMITTED"),
  "history-search defaults to COMMITTED turns only",
);
const assistantTurn = history.find((t) => t.role === "ASSISTANT");
assert(
  assistantTurn !== undefined,
  "the pushed ASSISTANT turn is present in history",
);
assert(
  assistantTurn?.agentInstanceKey === agentInstanceKey &&
    assistantTurn?.loopIteration === 1,
  "the ASSISTANT turn carries its owning agentInstanceKey and loopIteration",
);
assert(
  Array.isArray(assistantTurn?.content) &&
    assistantTurn.content.some((c) => c.text === "hello from the agent"),
  "the ASSISTANT turn round-trips its text content",
);

// 6. COMPLETE drives the instance to the terminal COMPLETED status. Advance the
//    host clock first so the completion instant is a real (non-zero) timestamp.
engine.advanceTime(700);
engine.completeAgentInstance(agentInstanceKey);
const completed = JSON.parse(engine.searchAgentInstances("{}")).items[0];
assert(
  completed.status === "COMPLETED",
  `COMPLETE drives status to COMPLETED (got ${JSON.stringify(completed.status)})`,
);
assert(
  completed.completionDate !== null && completed.completionDate !== undefined,
  "a completed instance carries a completionDate",
);

if (failed) {
  console.error("\nagent-instance e2e probe FAILED");
  process.exit(1);
}
console.log("\nagent-instance e2e probe passed");
