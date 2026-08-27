# engine-wasm agent-instance end-to-end probe

Proves the engine-native **AgentInstance / AgentHistory** surface (Camunda
stable/8.10 parity, Stage 3) is reachable end-to-end through the
`@nanobpm/engine-wasm` **read-model** `TestEngine` — the JS console/Bojtos tier.

`verify.mjs` drives the full lifecycle against the read-model entrypoint
(`@nanobpm/engine-wasm/readmodel`):

1. **deploy** a `bpmn:serviceTask` bearing
   `<zeebe:agentDefinition agentType="aiAgentTask" />`;
2. **create an instance** — activating the agent task **mints an AgentInstance**
   in `INITIALIZING` (no job is created for an engine-native agent task);
3. **`createAgentInstance`** reconciles that record with a CREATE-time
   definition/limits (still `INITIALIZING`, same key — no duplicate);
4. **`updateAgentInstance`** advances the status (`THINKING`) and pushes a turn;
5. **`searchAgentInstanceHistory`** returns that turn (defaulting to
   `COMMITTED`);
6. **`completeAgentInstance`** drives the instance to `COMPLETED`.

Run:

```sh
npm install   # links ../../pkg (@nanobpm/engine-wasm)
npm test
```

The probe consumes the committed/regenerated `engine-wasm/pkg/` artifact, so it
also guards that `make console-wasm` shipped the AgentInstance driver + read
methods onto the exported wasm surface.
