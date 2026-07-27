# @nanobpm/bojtos-kit

Framework-agnostic core of the **Bojtos** in-browser BPMN demo framework
([ADR 0043](../docs/adr/0043-bojtos-demo-framework.md)).

It wraps [`@nanobpm/engine-wasm`](../engine-wasm) as a single scenario runner —
the one runner the whole framework (and the console test-run panel) drives, so
there is no second, drift-prone engine harness — and re-exports the engine's
snapshot/event contract types.

```ts
import { createBojtosSession } from "@nanobpm/bojtos-kit";

const session = await createBojtosSession(); // loads the wasm engine once
const { processIds } = session.deploy(bpmnXml);
let snapshot = session.createInstance(processIds[0], "{}");
// a service task is now waiting as a job:
snapshot = session.completeJob(snapshot.jobs[0].key, JSON.stringify({ ok: true }));
const trace = session.events(); // WasmEvent[] for a step/trace view
session.free();
```

Every command returns the post-run `Snapshot`: `activeElementIds` /
`incidentElementIds` drive the token/incident highlight, and
`instances[].variables` is the live payload that mutates as workers complete.

For React, use [`@nanobpm/bojtos-react`](../bojtos-react), which owns the session
lifecycle and reactive state on top of this kit.

## Build

`dist/` (the tsc-emitted JS + `.d.ts`) is committed so `file:` consumers and CI
need no build-on-install step. Regenerate with `npm run build`.
