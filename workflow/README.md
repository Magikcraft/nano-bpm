# @nanobpm/workflow

Code-first durable orchestration for [nanobpmn](https://github.com/Magikcraft/nano-bpm)
(**ADR 0044**). Author durable workflows as ordinary async code; the SDK derives
the executable BPMN model, the job types, and the message/correlation wiring, and
hosts a generic worker. No diagram, no task-type wiring, no correlation plumbing
written by hand.

It talks to a **running nanobpmn gateway** over the REST v2 API — the engine
provides the durability (crash-resume, at-least-once jobs, message correlation);
this package is a thin authoring + runtime layer on top.

## Why

Nano already *is* a durable-execution substrate: the raft journal is the event
history, the engine is itself replay-from-journal, a job+worker is an activity, a
message is a signal, a BPMN timer is a timer. The missing piece was never
durability — it was **authoring ergonomics**. This package removes the ceremony
(model a diagram → wire each task to a job type → wire payloads → register
workers) that Temporal and Camunda both impose.

## Install

```sh
npm install @nanobpm/workflow
```

Requires Node ≥ 20 and a reachable nanobpmn gateway (default `http://localhost:8080`).

## Two authoring surfaces

Both compile to the same engine durability; pick per workflow.

### Imperative (Temporal-style, engine-replayed)

Write the orchestration as a function. `ctx.run(name, fn)` is a durable step: its
result is journalled in an engine process variable, so on resume a completed step
is **replayed from the journal** (its side effect is **not** re-run) and only the
frontier step executes. The engine drives the function by re-invoking a single
looped orchestrator job each turn.

```ts
import { defineWorkflow, WorkflowClient, Worker } from "@nanobpm/workflow";

const prReview = defineWorkflow("pr-review", async (ctx) => {
  const diff = await ctx.run("fetchDiff", () => gh.diff(ctx.input.prId));
  const review = await ctx.run("review", () => llm.review(diff));
  await ctx.run("merge", () => gh.merge(ctx.input.prId));
});

const client = new WorkflowClient({ baseUrl: "http://localhost:8080" });
await client.deploy(prReview);

const worker = new Worker({ baseUrl: "http://localhost:8080", workflows: [prReview] });
worker.start();

await client.start(prReview, { prId: "PR-1234" });
```

If the engine crashes after `review` commits and restarts cold, `fetchDiff` and
`review` are **not** re-run — the workflow resumes at `merge`, which runs exactly
once.

### Declarative (with human-in-the-loop signals)

Describe the flow as an ordered set of steps and signals. Each `w.run` is a
service task (its own job type + handler); each `w.signal` parks the instance on a
message-catch that resumes via a correlated message — the human-in-the-loop path.

```ts
import { defineFlow, WorkflowClient, Worker } from "@nanobpm/workflow";

const onboarding = defineFlow("onboarding", (w) => {
  w.run("createAccount", async (job) => ({ userId: makeId() }));
  w.signal("approved", { correlationKey: "userId" });
  w.run("provision", async (job) => ({ ok: true }));
});

const client = new WorkflowClient({ baseUrl: "http://localhost:8080" });
await client.deploy(onboarding);
new Worker({ baseUrl: "http://localhost:8080", workflows: [onboarding] }).start();

const { processInstanceKey } = await client.start(onboarding, {});
// ... later, when a human approves:
await client.signal(onboarding, "approved", userId, { by: "alice" });
```

## Honest scope

- **Durability is the engine's**, via leader-durable replication (ADR 0003). On a
  single node it survives process crash / SIGKILL / OOM; on a cluster it is
  majority-durable and survives node loss.
- **Jobs are at-least-once.** A step's side effect runs *before* the job
  completes; a crash in between causes redelivery and a repeat. **Handlers must be
  idempotent.** For the imperative surface, `ctx.run` de-dupes *within* a workflow
  (the journal), not across an external side effect that already partially applied.
- **The determinism constraint binds only the imperative orchestration
  function**, not the activities. Do all non-deterministic / side-effecting work
  inside `ctx.run(name, fn)` closures — never in the orchestration body directly.
- **The worker uses a single `baseUrl`.** For the single-user SDLC use case this
  is fine; it is a client SPOF (no worker-side failover), not an engine limit.

## API

| Export | Purpose |
| --- | --- |
| `defineWorkflow(id, orchFn)` | Imperative (replayed) workflow. |
| `defineFlow(id, build)` | Declarative flow with `w.run` / `w.signal`. |
| `WorkflowClient` | `deploy`, `start`, `signal`, `getInstance` over REST v2. |
| `Worker` | Generic job runtime; routes job types → handlers, hosts the replay loop. |
| `toBpmn(workflow)` | The derived BPMN XML (for inspection / deployment). |

See [ADR 0044](../docs/adr/0044-code-first-durable-orchestration.md) for the design
rationale and the de-risking spike.

## Development

```sh
npm ci
npm run build        # tsc → dist/ (committed)
npm run typecheck
npm test             # build + unit tests; integration tests self-skip without a gateway binary
```

The integration tests boot a real gateway from the sibling `server/` build if
present (`server/target/debug/nanobpm-gateway-rest-server`); otherwise they skip.

## License

Apache-2.0.
