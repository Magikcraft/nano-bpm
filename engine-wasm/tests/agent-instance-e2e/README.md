# engine-wasm agent-instance end-to-end probe

Proves the engine-native **AgentInstance / AgentHistory** surface (Camunda
stable/8.10 parity, Stage 3) is reachable end-to-end through the
`@nanobpm/engine-wasm` **read-model** `TestEngine` — the JS console/Bojtos tier.

`verify.mjs` drives the full lifecycle against the read-model entrypoint
(`@nanobpm/engine-wasm/readmodel`):

1. **deploy** a `bpmn:serviceTask` bearing
   `<zeebe:agentDefinition agentType="aiAgentTask" />`;
2. **create an instance** and **activate its ordinary worker job** — no
   AgentInstance exists yet;
3. **`createAgentInstance`** explicitly registers an `INITIALIZING` record with
   CREATE-time definition/limits and history, using the activated job's
   `jobKey` and opaque `jobLease`;
4. **`updateAgentInstance`** advances the status (`THINKING`) and pushes a turn;
   it also **rejects the terminal `status: "COMPLETED"`** (reachable only through
   `completeAgentInstance`), and accepts a `producedAt` as an **RFC-3339 string**
   (as well as epoch millis) plus an **`OBJECT` content item** whose `object` is a
   real JSON object;
5. **`searchAgentInstanceHistory`** returns that turn (defaulting to
   `COMMITTED`) in the gateway's REST JSON shape — camelCase keys, the REST
   `contentType` enum spelling, `producedAt` as an RFC-3339 string, and an
   `OBJECT` item's `object` round-tripped as a JSON object (not a string);
6. **`completeAgentInstance`** drives the instance to `COMPLETED`; the worker
   completes its job separately to advance the BPMN token.

`external-routing.mjs` covers both `external` and `aiAgentTask` markers on both
the lean and read-model entrypoints. It checks configured and expression-based
routing, element-id fallback, priority/retries, custom headers, and linked prompt
resources resolved to the latest deployed version. Supplied invalid job
attribution is rejected for both markers. All agent types require a valid
activated job and lease for history; history-free CREATE/UPDATE may omit them.

Run:

```sh
npm install   # links ../../pkg (@nanobpm/engine-wasm)
npm test
```

The probe consumes the committed/regenerated `engine-wasm/pkg/` artifact, so it
also guards that `make console-wasm` shipped the AgentInstance driver + read
methods onto the exported wasm surface. It runs in CI as part of
`make engine-wasm-ffi-dist` (the `engine-wasm-ffi (dist + verify)` job), so a
regression in the AgentInstance/AgentHistory surface fails the build.
