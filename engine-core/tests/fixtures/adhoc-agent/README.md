# Golden fixtures — Camunda agentic ad-hoc sub-process (ADR 0023 Tier-1)

These are **unmodified** Camunda BPMN/form examples, vendored as the parity fixtures
for ADR 0023 (`docs/adr/0023-adhoc-subprocess-execution-parity.md`). The Tier-1 goal
is that the **unmodified Camunda AI Agent connector** drives these models on Nano's
engine identically to Camunda 8, so they are captured verbatim as the source of truth
for the engine contract we must satisfy.

## Provenance

- Source repo: `camunda/connectors`, path
  `connectors/agentic-ai/examples/ai-agent/ad-hoc-sub-process/`.
- Pinned commit: `7db655dd87da8249ab202757ba1d6259f4cbe152` (do not "update" —
  fixtures are frozen at this SHA; refresh deliberately if the contract changes).
- License: Apache-2.0 (Camunda `connectors`). Vendored unmodified for test use.

| Fixture dir | What it exercises |
|---|---|
| `ai-agent-chat-with-tools/` | Canonical chat agent over an ad-hoc container with a mixed tool set (service/user/script tasks). The primary golden model. |
| `fraud-detection/` | Richer real-world agentic process (tax-fraud triage) — secondary fixture for coverage. |

## The engine contract these fixtures pin

Extracted from `ai-agent-chat-with-tools/ai-agent-chat-with-tools.bpmn`:

- **Container**: `<bpmn:adHocSubProcess id="AI_Agent">` with modeler template
  `io.camunda.connectors.agenticai.aiagent.jobworker.v1` — i.e. the **`JOB_WORKER`**
  ad-hoc implementation type (the agentic path; cf.
  `ZeebeAdHocImplementationType.{BPMN_TASK,JOB_WORKER}`).
- **Agent job type**: `io.camunda.agenticai:aiagent-job-worker:1` (the AI Agent
  connector job worker; a second task def `io.camunda:http-json:1` backs several
  tools).
- **Tool output gathering**: `zeebe:adHoc outputCollection="toolCallResults"
  outputElement="={ id: toolCall._meta.id, name: toolCall._meta.name, content:
  toolCallResult }"` — each activated tool's result is folded into the
  `toolCallResults` collection variable (the agent's accumulated memory).
- **Tools** (inner elements the agent may activate): `serviceTask` LoadUserByID,
  ListUsers, Search_Recipe, Jokes_API, Fetch_URL; `scriptTask` GetDateAndTime,
  SuperfluxProduct, SendEmail, Handle_Message; `userTask` User_Feedback,
  AskHumanToSendEmail. These are **pruned by Nano today** (`engine-core/src/bpmn.rs`
  `is_adhoc`) — Tier-1 must retain and activate them.
- **Completion**: neither vendored model (`ai-agent-chat-with-tools`,
  `fraud-detection`) carries an explicit `<completionCondition>` — the agent signals
  completion via the job result (`isCompletionConditionFulfilled` / no further
  `activateElements`). **Gap**: the explicit `<completionCondition>` FEEL path is not
  covered by these upstream fixtures, so `adhoc-feel` must add a small synthetic
  fixture (or a hand-authored variant) to exercise it.

## The complete-job REST body Nano must accept (verified against C8 spec)

From `~/workspace/camunda/zeebe` `gateway-protocol/src/main/proto/v2/jobs.yaml`
(`JobResult` discriminator `type` → `JobResultAdHocSubProcess`,
`JobResultActivateElement`). When the AI Agent connector decides which tools to run, it
completes its agent job with:

```
POST /v2/jobs/{jobKey}/completion
{
  "variables": { },
  "result": {
    "type": "adHocSubProcess",
    "activateElements": [
      { "elementId": "Search_Recipe", "variables": { "query": "pasta" } },
      { "elementId": "GetDateAndTime", "variables": { } }
    ],
    "isCompletionConditionFulfilled": false,
    "isCancelRemainingInstances": false
  }
}
```

Field contract (exact names — Nano's `ActivatedJobResult`/complete-job mapping must
match so the stock connector serializes into it, and all must be **optional** so
non-agentic completion is byte-unchanged):

- `result.type` — discriminator, `"adHocSubProcess"` (vs `"userTask"`).
- `result.activateElements[]` — `{ elementId: string, variables?: object }`; the tools
  to activate this turn (may be empty).
- `result.isCompletionConditionFulfilled` — bool, default `false`.
- `result.isCancelRemainingInstances` — bool, default `false`.

## How Tier-1 uses these (see ADR 0023 phased plan)

1. `adhoc-model` — parse each fixture's ad-hoc container + tool catalog + `zeebe:adHoc`
   attrs without pruning; assert the retained catalog matches the table above.
2. `adhoc-result` — round-trip the JSON body above through Nano's complete-job mapping.
3. `adhoc-runtime` / `adhoc-e2e` — drive `ai-agent-chat-with-tools.bpmn` on embedded
   Bernd with the unmodified connector; assert each activated tool appears as a
   read-model element instance and results land in `toolCallResults`.
