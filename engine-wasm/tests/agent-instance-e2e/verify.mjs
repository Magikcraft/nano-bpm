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
//    definition + limits + a configuration turn — still INITIALIZING. The turn
//    uses the canonical REST history-item shape (a non-blank `historyItemId`, an
//    RFC-3339 `producedAt`, and a `content` array) so the probe guards against
//    contract drift on the CREATE surface too, not just UPDATE.
engine.createAgentInstance(
  JSON.stringify({
    elementInstanceKey,
    definition: { model: "gpt-4o", provider: "openai" },
    limits: { maxTokens: 1000, maxModelCalls: 10, maxToolCalls: 5 },
    history: [
      {
        historyItemId: "cfg-0",
        loopIteration: 0,
        producedAt: "2026-01-02T03:04:04.000Z",
        role: "CONFIGURATION",
        content: [
          { contentType: "TEXT", text: "agent configured" },
        ],
      },
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
//    ASSISTANT turn. Exercise the REST parity surface deliberately: the canonical
//    history-item shape with a non-blank `historyItemId` (required by the REST
//    schema for retry dedup + createdHistory correlation), a `producedAt` as an
//    RFC-3339 string (not epoch millis), and an OBJECT content item whose
//    `object` is a real JSON object (not a JSON-encoded string).
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
        historyItemId: "asst-1",
        loopIteration: 1,
        producedAt: "2026-01-02T03:04:05.250Z",
        role: "ASSISTANT",
        content: [
          { contentType: "TEXT", text: "hello from the agent" },
          { contentType: "OBJECT", object: { answer: 42, nested: { ok: true } } },
        ],
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

// 4a. UPDATE must reject the terminal `status: "COMPLETED"` — it is reachable
//     only through the dedicated completeAgentInstance command. The driver
//     rejects it up front, before any engine dispatch.
let rejectedCompleted = false;
try {
  engine.updateAgentInstance(
    JSON.stringify({
      agentInstanceKey,
      elementInstanceKey,
      elementId,
      processInstanceKey,
      status: "COMPLETED",
    }),
  );
} catch (e) {
  rejectedCompleted = true;
  assert(
    String(e).includes("completeAgentInstance"),
    `the COMPLETED rejection points at completeAgentInstance (got ${String(e)})`,
  );
}
assert(
  rejectedCompleted,
  "updateAgentInstance rejects status COMPLETED",
);
// The rejected UPDATE left the instance untouched (still THINKING).
assert(
  JSON.parse(engine.searchAgentInstances("{}")).items[0].status === "THINKING",
  "the rejected COMPLETED update did not mutate the instance",
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
// REST parity of the history output shape (matches the gateway JSON):
//  - `producedAt` comes back as an RFC-3339 string, not epoch millis;
//  - content is camelCase with the REST `contentType` enum spelling;
//  - an OBJECT item's `object` round-trips as a real JSON object, not a string.
assert(
  typeof assistantTurn?.producedAt === "string" &&
    assistantTurn.producedAt === "2026-01-02T03:04:05.250Z",
  `producedAt round-trips as an RFC-3339 string (got ${JSON.stringify(
    assistantTurn?.producedAt,
  )})`,
);
const objectItem = assistantTurn?.content?.find(
  (c) => c.contentType === "OBJECT",
);
assert(
  objectItem !== undefined,
  "the OBJECT content item survives with its REST contentType spelling",
);
assert(
  objectItem?.object &&
    typeof objectItem.object === "object" &&
    objectItem.object.answer === 42 &&
    objectItem.object.nested?.ok === true,
  `the OBJECT item's object round-trips as a JSON object (got ${JSON.stringify(
    objectItem?.object,
  )})`,
);

// 5a. Idempotent retry dedup (server-side, historyItemId): pushing a turn with a
//     stable historyItemId, then re-submitting the SAME historyItemId, must NOT
//     create a second AGENT_HISTORY record — the retry dedups against the
//     original. Verified through the wasm surface for engine parity.
const beforeRetry = JSON.parse(
  engine.searchAgentInstanceHistory(agentInstanceKey, "{}"),
).items.length;
const dedupUpdate = JSON.stringify({
  agentInstanceKey,
  elementInstanceKey,
  elementId,
  processInstanceKey,
  history: [
    {
      loopIteration: 2,
      producedAt: "2026-01-02T03:04:06.000Z",
      role: "USER",
      historyItemId: "retry-1",
      content: [{ contentType: "TEXT", text: "please retry" }],
    },
  ],
});
engine.updateAgentInstance(dedupUpdate);
const afterFirst = JSON.parse(
  engine.searchAgentInstanceHistory(agentInstanceKey, "{}"),
).items;
assert(
  afterFirst.length === beforeRetry + 1,
  `a new historyItemId records one turn (got ${afterFirst.length - beforeRetry})`,
);
// Re-submit the identical historyItemId: it is an idempotent retry.
engine.updateAgentInstance(dedupUpdate);
const afterRetry = JSON.parse(
  engine.searchAgentInstanceHistory(agentInstanceKey, "{}"),
).items;
assert(
  afterRetry.length === afterFirst.length,
  `re-submitting the same historyItemId creates no new record (got ${
    afterRetry.length - afterFirst.length
  } extra)`,
);
assert(
  afterRetry.filter((t) => t.historyItemId === "retry-1").length === 1,
  "the deduped turn appears exactly once in history",
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

// 7. external (job-backed) agent parity (#1099): unlike aiAgentTask, an
//    `external` agent auto-mints NO AgentInstance — it activates as a normal
//    job. A worker activates that job (standard job loop), learns its opaque
//    lease token (distinct from the job's deadline, #1106), and self-registers
//    the AgentInstance via a lease-gated createAgentInstance. A CREATE without a valid job lease is
//    rejected. This is the surface nano-workforce consumes.
const EXTERNAL_PROC = `
  <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                    xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
    <bpmn:process id="ext-proc" isExecutable="true">
      <bpmn:startEvent id="start" />
      <bpmn:serviceTask id="ext-agent">
        <bpmn:extensionElements>
          <zeebe:agentDefinition agentType="external" />
        </bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:endEvent id="end" />
      <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="ext-agent" />
      <bpmn:sequenceFlow id="f2" sourceRef="ext-agent" targetRef="end" />
    </bpmn:process>
  </bpmn:definitions>`;

const ext = new TestEngine();
ext.deploy(EXTERNAL_PROC);
ext.createInstance("ext-proc", "{}");

// Activation mints NO AgentInstance for an external agent.
assert(
  JSON.parse(ext.searchAgentInstances("{}")).items.length === 0,
  "an external agent auto-mints no AgentInstance on activation",
);

// The agent element created a normal, activatable job (type = element id).
const extJobs = JSON.parse(ext.activateJobs("ext-agent", 1, 1000, "W"));
assert(
  extJobs.length === 1 && extJobs[0].elementId === "ext-agent",
  "an external agent activates as a normal job through the standard job loop",
);
const extJob = extJobs[0];
const extEik = extJob.elementInstanceKey;
// The lease token is a real opaque per-activation token surfaced as `jobLease`,
// distinct from the job's `deadline` (#1106 — Camunda `hasLeaseToken()` parity).
assert(
  typeof extJob.jobLease === "string" &&
    extJob.jobLease !== String(extJob.deadline),
  "an external agent job carries an opaque lease token distinct from its deadline",
);
const extLease = extJob.jobLease;

// A CREATE with a stale lease token is rejected (no AgentInstance minted).
let extRejected = false;
try {
  ext.createAgentInstance(
    JSON.stringify({
      elementInstanceKey: extEik,
      jobKey: extJob.key,
      jobLease: String(BigInt(extJob.jobLease) + 1n),
      definition: { model: "gpt-4o" },
    }),
  );
} catch (e) {
  extRejected = true;
}
assert(
  extRejected,
  "an external CREATE with a stale lease token is rejected",
);
assert(
  JSON.parse(ext.searchAgentInstances("{}")).items.length === 0,
  "the rejected external CREATE minted nothing",
);

// A CREATE referencing the ACTIVATED job with the matching lease mints it.
ext.createAgentInstance(
  JSON.stringify({
    elementInstanceKey: extEik,
    jobKey: extJob.key,
    jobLease: extLease,
    definition: { model: "gpt-4o", provider: "openai" },
  }),
);
const extInstances = JSON.parse(ext.searchAgentInstances("{}")).items;
assert(
  extInstances.length === 1,
  "a lease-gated external CREATE mints exactly one AgentInstance",
);
assert(
  extInstances[0].status === "INITIALIZING" &&
    extInstances[0].elementInstanceKey === extEik,
  "the external AgentInstance is INITIALIZING and linked to the job's element instance",
);

// The job completes through the standard loop, advancing the token to the end.
ext.completeJob(extJob.key, "{}");
assert(
  JSON.parse(ext.searchAgentInstances("{}")).items.length === 1,
  "completing the external agent job leaves its AgentInstance intact",
);

if (failed) {
  console.error("\nagent-instance e2e probe FAILED");
  process.exit(1);
}
console.log("\nagent-instance e2e probe passed");
