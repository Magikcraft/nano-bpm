# @nanobpm/vscode-bpmn-debug

A VS Code extension that renders the BPMN **diagram** for a nanobpmn debug session
and **highlights the element the debugger is paused on** — because for a process,
"where am I" is a node in a diagram, not a line in an XML file. The DAP adapter
(#651) drives the state; this webview draws it.

> **Part 3 of 3** of the in-engine debugger track (#646):
> engine-core stepping executor (#647) → wasm control surface (#650) →
> DAP adapter (#651) → **this webview (#652)**. Proof-of-concept.

## What it does

- **Auto-opens** a `bpmn-js` diagram panel (beside the editor) when a `nanobpmn`
  debug session starts.
- **Highlights the paused element(s):** the adapter emits a custom
  `nanobpmn/activeElements` DAP event on every pause; a debug-adapter tracker
  forwards it to the webview, which marks those elements on the diagram. The
  highlight clears on termination.
- **Click-to-breakpoint:** clicking a flow node in the diagram toggles a VS Code
  `SourceBreakpoint` at that element's source line (resolved with the same BPMN
  source map the adapter uses) — the element-anchored UX that beats text lines.
  Breakpoints set anywhere (diagram or gutter) are reflected back onto the diagram.

## How it wires together

```
 wasm engine debug surface (#650)
        ▲   debug* calls / DebugState
        │
   DAP adapter (#651)  ──emits──►  nanobpmn/activeElements  (custom DAP event)
        ▲                                   │
   DAP protocol (stopped/…)                 │ debug-adapter tracker
        │                                   ▼
   VS Code debug UI            this extension ──postMessage──► bpmn-js webview
        └───── breakpoints ◄── click-to-breakpoint ◄──────────────┘
```

The extension contributes the `nanobpmn` debug type; its adapter `program` points
at the sibling `dap-adapter` package's `dist/index.js`.

## Develop

```bash
npm install
npm run build      # esbuild → dist/extension.js (node) + dist/webview.js (browser)
npm test           # vitest — pure protocol/marker-diff logic
npm run typecheck  # tsc (src + test)
```

Then press **F5** in VS Code to launch an Extension Development Host, open a
workspace containing a `.bpmn`, and start a `nanobpmn` launch config (see the
adapter's `examples/launch.json`).

> **PoC scope.** The VS Code-API glue (webview lifecycle, breakpoint toggling,
> event forwarding) can only be exercised in an Extension Development Host, so the
> automated tests cover the host-independent logic (the marker diff + message
> contract in `src/protocol.ts`). The renderer reuses the console's `bpmn-js`
> `NavigatedViewer` + `addMarker` pattern.
