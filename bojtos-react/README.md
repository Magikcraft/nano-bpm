# @nanobpm/bojtos-react

React binding for the **Bojtos** in-browser BPMN demo framework
([ADR 0043](../docs/adr/0043-bojtos-demo-framework.md)), built on
[`@nanobpm/bojtos-kit`](../bojtos-kit).

- **`useBojtos({ bpmn })`** — owns the engine session and the reactive
  `snapshot` / `events` / `processIds` state, and exposes the engine commands
  (`createInstance`, `completeJob`, `failJob`, `advanceTime`, `reset`).
- **`<BpmnRuntimeView xml activeIds incidentIds />`** — the live diagram: it
  imports the XML once and updates token (`nano-active`) / incident
  (`nano-incident`) markers in place, so zoom/scroll survive stepping.

```tsx
import { useBojtos, BpmnRuntimeView } from "@nanobpm/bojtos-react";
import "bpmn-js/dist/assets/diagram-js.css";
import "bpmn-js/dist/assets/bpmn-font/css/bpmn-embedded.css";

function Demo({ bpmn }: { bpmn: string }) {
  const run = useBojtos({ bpmn });
  const snap = run.snapshot;
  return (
    <BpmnRuntimeView
      xml={bpmn}
      activeIds={snap?.activeElementIds ?? []}
      incidentIds={snap?.incidentElementIds ?? []}
    />
  );
}
```

## Peer requirements

`react` and `bpmn-js` are peer dependencies (the consumer already has them). The
consumer must import bpmn-js's diagram CSS once and provide the `.nano-active` /
`.nano-incident` marker styles.

## Build

`dist/` (the tsc-emitted JS + `.d.ts`, with JSX already compiled to
`react/jsx-runtime` so consumers never re-transform node_modules) is committed
so `file:` consumers and CI need no build-on-install step. Regenerate with
`npm run build`.
