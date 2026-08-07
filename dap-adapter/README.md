# @nanobpm/dap-adapter

A [Debug Adapter Protocol](https://microsoft.github.io/debug-adapter-protocol/)
(DAP) adapter that drives the nanobpmn **wasm** engine's debug surface
(`TestEngine.debug*`, see #650). It lets any DAP client — VS Code, or a headless
test client — step through a BPMN process instance: set breakpoints on flow
nodes, pause the engine mid-command, inspect process variables, and resume.

> **Proof-of-concept.** This is the second piece of the in-engine debugger track
> (#646): engine-core stepping executor (#647) → wasm control surface (#650) →
> **this adapter (#651)** → BPMN webview (#652). The webview is what turns
> line-anchored breakpoints into *click-the-diagram* breakpoints; until then this
> adapter anchors breakpoints to the `.bpmn` **XML source line** carrying the flow
> node's `id`.

## What it maps

| DAP | nanobpmn engine |
|---|---|
| `launch` | parse + deploy the `.bpmn`; build a line ↔ element-id source map |
| `setBreakpoints` (line) | resolve line → element id → `elementActivated` break condition |
| `configurationDone` | start the instance, pause at the first breakpoint |
| `stopped` | the engine paused at a break condition |
| `stackTrace` | the currently **active** BPMN elements (the highlight set) |
| `variables` | the instance's process variables at the pause |
| `continue` | run to the next breakpoint / completion |
| `next` / `stepIn` | advance exactly one engine step |
| `terminated` | the run drained to quiescence |

One BPMN process instance = one DAP "thread".

## Breakpoint semantics (read this)

A service task's `ElementActivated` and its `JobCreated` are emitted in the **same**
engine step, so a breakpoint on a service task pauses at the same point the run
would naturally park (waiting for the job). To observe a genuinely *mid-command*
pause, break on an upstream node — e.g. the **start event** — which stops the run
before any job parks. The bundled example and tests break on the start event for
exactly this reason.

## Usage (VS Code)

Build the adapter, then point a launch config at it. See
[`examples/launch.json`](examples/launch.json):

```jsonc
{
  "type": "nanobpmn",
  "request": "launch",
  "name": "Debug BPMN process",
  "bpmn": "${workspaceFolder}/process.bpmn",
  "processId": "my-process",
  "variables": {}
}
```

The adapter binary is `dist/index.js` (stdio transport). A VS Code extension
contributing the `nanobpmn` debug type would register it as its `program`.

## Develop

```bash
npm install
npm run build      # tsc → dist/
npm test           # vitest (source-map unit + headless DAP integration)
npm run typecheck  # tsc --noEmit
```

The integration test uses `@vscode/debugadapter-testsupport` to launch the built
`dist/index.js` and drive a real DAP handshake against the wasm engine — no VS
Code required.
