# nanobpm.io landing demo

The live landing page for **nanobpm.io**: a from-scratch BPMN engine running
_in your browser_. It executes the real `urban-pr-review` **convergence loop**
on the WebAssembly build of the engine (`@nanobpm/engine-wasm`) via the
[Bojtos](../../docs/adr/0043-bojtos-in-browser-demo-framework.md) in-browser
framework — no server, no backend calls in the runtime.

## What's real vs. scripted

- **Real:** the engine, every token move, gateways, the two message-catch waits
  (`review-ready` / `escalation-answered`, correlated by `prKey`), variable
  updates, and convergence. All of it runs on the actual engine compiled to wasm.
- **Scripted:** the `senior:pr-review` worker. On the real system that task calls
  an LLM (`senior:pr-review`); a public static page has no server or keys, so a
  deterministic stand-in walks a representative convergence
  (`changes requested → needs input → approved`) — see `src/workers.ts`. The
  persist/finalize workers are no-ops here (they write to the datasource in a
  deployed app).

## Vendored model — drift note

`src/convergence-loop.bpmn` is a **vendored copy** of
`urban-pr-review/resources/processes/convergence-loop.bpmn` (it carries DI so
bpmn-js renders it). It is a demo snapshot; if the source model changes
materially, refresh this copy. It is deliberately free of any legacy `.dev`
domain reference so it passes `website/build.mjs`'s drift guard.

## Develop

```bash
npm install
npm run dev      # http://localhost:5173
npm run build    # typecheck + vite build -> dist/
```

`node_modules/` and `dist/` are git-ignored; the site's Pages build
(`website/build.mjs`) installs and builds this app and copies `dist/` to the
site root.
