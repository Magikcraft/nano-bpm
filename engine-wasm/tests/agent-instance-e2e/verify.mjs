// End-to-end probe of the engine-native AgentInstance / AgentHistory surface
// (Camunda stable/8.10 parity, Stage 3) through the @nanobpm/engine-wasm
// read-model TestEngine — the JS console/Bojtos tier.
//
// Deploy an aiAgentTask serviceTask -> activate its ordinary job -> explicitly
// create an AgentInstance in INITIALIZING using the job lease -> push a
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

const EXTERNAL_PROC = readFileSync(
  new URL("../../../engine-core/tests/fixtures/external-agent-job-type.bpmn", import.meta.url),
  "utf8",
);
const AGENT_PROC = EXTERNAL_PROC.replace('agentType="external"', 'agentType="aiAgentTask"');

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
engine.createInstance("external-agent-routing", '{"route":"senior:rebase"}');

// 2. Activation creates only the ordinary worker job.
let instances = JSON.parse(engine.searchAgentInstances("{}")).items;
assert(instances.length === 0, "activation creates no AgentInstance");
const jobs = JSON.parse(engine.activateJobs("senior:rebase", 1, 60_000, "W"));
assert(jobs.length === 1, "aiAgentTask activates through the ordinary job loop");
const job = jobs[0];
const elementInstanceKey = job.elementInstanceKey;
const elementId = job.elementId;
const processInstanceKey = job.instanceKey;
const attribution = { jobKey: job.key, jobLease: job.jobLease };

// 3. Explicit worker CREATE registers the record, applying a CREATE-time
//    definition + limits + a configuration turn — still INITIALIZING. The turn
//    uses the canonical REST history-item shape (a non-blank `historyItemId`, an
//    RFC-3339 `producedAt`, and a `content` array) so the probe guards against
//    contract drift on the CREATE surface too, not just UPDATE.
engine.createAgentInstance(
  JSON.stringify({
    elementInstanceKey,
    ...attribution,
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
  "explicit CREATE registers exactly one instance",
);
const created = instances[0];
const agentInstanceKey = created.agentInstanceKey;
assert(
  typeof agentInstanceKey === "string" && agentInstanceKey !== "0" &&
    agentInstanceKey !== elementInstanceKey,
  "CREATE allocates a dedicated agentInstanceKey distinct from the element key",
);
assert(
  created.status === "INITIALIZING",
  `created instance is INITIALIZING (got ${JSON.stringify(created.status)})`,
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
    ...attribution,
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
  ...attribution,
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
const finished = JSON.parse(engine.completeJob(job.key, "{}"));
assert(finished.instances[0].state === "Completed",
  "AgentInstance COMPLETE leaves the job to be completed separately");

// 7. external (job-backed) agent parity (#1099): like aiAgentTask, an
//    `external` agent creates NO AgentInstance — it activates as a normal
//    job. A worker activates that job (standard job loop), learns its opaque
//    lease token (distinct from the job's deadline, #1106), and self-registers
//    the AgentInstance via a lease-gated createAgentInstance. A CREATE that
//    references the job with a stale/mismatched (jobKey, jobLease) pair is
//    rejected. This is the surface nano-workforce consumes.
const ext = new TestEngine();
ext.deploy(EXTERNAL_PROC);
ext.createInstance("external-agent-routing", '{"route":"senior:rebase"}');

// Activation mints NO AgentInstance for an external agent.
assert(
  JSON.parse(ext.searchAgentInstances("{}")).items.length === 0,
  "an external agent auto-mints no AgentInstance on activation",
);

// Advance the WASM engine clock to a large wall-time before activation so the
// activated job's `deadline` (now + timeout_ms) is a large value. `jobLease` is
// a monotonic key, small at first; without advancing the clock a small deadline
// (~timeout_ms) could coincidentally equal it and make the distinctness
// assertion below pass (or fail) by accident. A large deadline removes the
// coincidence and keeps the assertion meaningful (#1106).
ext.tickNow(1_000_000_000_000);

// The agent element created a normal job with its configured routing type.
const extJobs = JSON.parse(ext.activateJobs("senior:rebase", 1, 1000, "W"));
assert(
  extJobs.length === 1 && extJobs[0].elementId === "agent",
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

// Perturb the lease OUTSIDE the try/catch: parsing must fail loudly if the
// token ever stops being a decimal u64 string, rather than being swallowed by
// the rejection catch below and passing the test without exercising the path.
const extStaleLease = String(BigInt(extLease) + 1n);

// A CREATE with a stale lease token is rejected (no AgentInstance minted).
let extRejected = false;
try {
  ext.createAgentInstance(
    JSON.stringify({
      elementInstanceKey: extEik,
      jobKey: extJob.key,
      jobLease: extStaleLease,
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
