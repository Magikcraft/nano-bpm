# Nano Workforce — Crew Task process sketch

Design artifacts for [ADR 0051 — Nano Workforce](../adr/0051-nano-workforce.md).

- **`crew-task.semantic.bpmn`** — the authored, DI-less semantic model (the source of truth for the
  process shape). Readable as XML; opens as a blank canvas in a modeller (no diagram).
- **`crew-task.bpmn`** — the same model with an auto-generated diagram layout, produced by
  `workflow/src/layout.ts`'s `layoutBpmn` (ADR 0048). This is the one to open in bpmn-js / the console
  modeller to *see* the flow.

To regenerate the laid-out model from the semantic source:

```js
// from the repo root, with workflow/ built (npm run build) and bpmn-auto-layout installed:
import { readFileSync, writeFileSync } from "node:fs";
const { layoutBpmn } = await import("./workflow/dist/layout.js");
const semantic = readFileSync("docs/nano-workforce/crew-task.semantic.bpmn", "utf8");
writeFileSync("docs/nano-workforce/crew-task.bpmn", await layoutBpmn(semantic));
```

This is a **sketch**: it fixes the process shape and the element-to-primitive mapping. The datasource
schema, DMN table columns, worker SDK, and connector manifests are deliberately out of scope here — see
ADR 0051 "Scope" and "Open questions".
