# @nanobpmn/sdk

A streaming SDK for the **nanobpmn command stream**, layered on top of
[`@camunda8/orchestration-cluster-api`](https://www.npmjs.com/package/@camunda8/orchestration-cluster-api).

nanobpmn exposes a single bidirectional WebSocket at `GET /command-stream` that
multiplexes process creation *and* the full job lifecycle onto one persistent,
credit-coordinated socket (see [`../../docs/command-stream-design.md`](../../docs/command-stream-design.md)
and [`../../docs/command-stream.asyncapi.yaml`](../../docs/command-stream.asyncapi.yaml)).
This package gives Node/TypeScript apps a typed client for that stream and a job
worker that **prefers the stream when talking to nanobpmn and falls back to
Camunda REST polling otherwise** — so the same code path serves both backends.

This is a **companion** to the official Camunda SDK, not a fork: it depends on
`@camunda8/orchestration-cluster-api` (an optional peer dependency, used only for
the polling fallback) and adds the nanobpmn-specific streaming surface by
composition.

## Install

```sh
npm install @nanobpmn/sdk
# the Camunda SDK is only needed if you use the polling fallback:
npm install @camunda8/orchestration-cluster-api
```

## Streaming job worker (with automatic fallback)

```ts
import { createStreamingJobWorker } from '@nanobpmn/sdk';
import { createCamundaClient } from '@camunda8/orchestration-cluster-api';

const camunda = createCamundaClient({ /* used only if the gateway is Camunda */ });

const worker = await createStreamingJobWorker({
  baseUrl: 'http://localhost:8080',
  jobType: 'test-job',
  worker: 'my-worker',
  maxParallelJobs: 10,
  // The Camunda client is used only for the polling fallback.
  camundaClient: camunda,
  jobHandler: async (job) => {
    // The same handler works on both transports.
    return job.complete({ result: job.variables.input });
  },
});

console.log(`worker transport: ${worker.transport}`); // 'stream' against nanobpmn, 'poll' against Camunda
// ... later:
await worker.stop();
```

`transport` selection:

| `transport`        | Behaviour                                                                  |
| ------------------ | -------------------------------------------------------------------------- |
| `'auto'` (default) | Probe the gateway; **stream** if it is nanobpmn, otherwise **poll**.       |
| `'stream'`         | Force the command stream (no probe).                                       |
| `'poll'`           | Force the Camunda SDK polling worker (requires `camundaClient`).           |

Detection is a single short-lived `/command-stream` WebSocket probe
(`detectNanobpm(baseUrl)`): nanobpmn answers with a `welcome` frame; a Camunda
gateway rejects the upgrade.

Job concurrency is governed by the stream's **credit window**: the worker
subscribes with `maxParallelJobs` credits and replenishes one per completed job,
so the server pushes at most `maxParallelJobs` jobs in flight — no long-poll, no
client-side activation loop.

For throughput, the worker's `complete()` / `fail()` / `error()` are
**pipelined**: they send the action frame and resolve immediately rather than
blocking the handler on the server's durable ack, so a single connection keeps
its whole credit window in flight (reaching the handler-bound ceiling instead of
one job per round-trip). The server still journals every action durably;
delivery is **at-least-once**, so make handlers idempotent. A failed action ack
(or a transport error) is reported via the optional `onError` callback —
`404`/`409` results (the job was already completed or reclaimed under
at-least-once redelivery) are treated as benign and not surfaced. `jobTimeoutMs`
(the activation lock; default `60_000`) bounds how long an in-flight job is held
before it is eligible for redelivery. The server also enforces this: a subscribe
with a null or non-positive timeout is clamped to a 60 s lock, so a job is never
leased with a zero lock (which would re-dispatch it before the worker completes).

## Low-level command-stream client

```ts
import { CommandStreamClient } from '@nanobpmn/sdk';

const client = new CommandStreamClient({ baseUrl: 'http://localhost:8080', worker: 'creator' });
await client.connect();

// Create and await terminal completion over the stream:
const { ack, completion } = await client.createInstanceAndAwait(
  { processDefinitionId: 'Process_0f7cr6y', variables: { foo: 'bar' } },
  { fetchVariables: ['result'] },
);
console.log(ack.processInstanceKey, completion.processCompleted, completion.variables);

await client.close();
```

`createInstance` is metered by the server's **submission-credit window**: when
the window is exhausted the call waits for the server to replenish credits
(backpressure without `503`/retry). Job completions (`completeJob` / `failJob` /
`throwError`) are unmetered. The client emits `pressure`, `welcome`, `job`,
`reconnect`, `close`, and `error` events, and reconnects automatically (replaying
job subscriptions) unless `reconnect: false`.

#### Bounding the credit wait (`submitTimeoutMs`)

By default a `createInstance` under sustained backpressure waits **indefinitely**
for the server to replenish credits. To fail fast instead, set a `submitTimeoutMs`
— per request, or client-wide as a default:

```ts
// Client-wide default; per-request wins when both are set.
const client = new CommandStreamClient({ baseUrl, worker, submitTimeoutMs: 2_000 });

try {
  await client.createInstance({ processDefinitionId: 'order', submitTimeoutMs: 500 });
} catch (err) {
  if (err instanceof SubmissionTimeoutError) {
    // Server is applying admission backpressure — back off, do NOT tight-loop retry.
  }
}
```

`submitTimeoutMs` is a **client-side** guard: it bounds only how long the client
waits for a credit and is never sent to the server (nothing changes on the wire).
On timeout the call rejects with `SubmissionTimeoutError`, the abandoned waiter is
removed so no credit slot leaks, and no `createInstance` command is sent. Treat it
as a backpressure signal and back off rather than retrying in a tight loop.

### Reconnect recovery

If a socket drops after a `createInstance(awaitCompletion)` ack but before the
`instanceCompleted` arrives, recover on a fresh connection with the persisted
`processInstanceKey`:

```ts
const completion = await client.awaitInstance(processInstanceKey, { fetchVariables: ['result'] });
```

Because the read model is durable history, an already-terminal instance resolves
immediately — so `awaitInstance` doubles as a completion poll.

## Recommended topology

Ordering is per-socket and throughput scales with concurrent sockets, so:

- **One worker (one socket) per job type.**
- **A separate socket (or small pool) for `createInstance` submission**, kept
  apart from the job-worker sockets.

See the "Recommended worker topology" section of the main
[`../../README.md`](../../README.md).

## Develop

```sh
npm install
npm run typecheck
npm run build      # tsup → dist/ (ESM + CJS + d.ts)
npm test           # vitest; the integration suite auto-skips if the
                   # server release binary is not built
```

The integration tests spawn `server/target/release/nanobpm-gateway-rest-server`
(build it with `cargo build --release` from `server/`) on a random port, deploy a
fixture, and exercise the stream end-to-end.
