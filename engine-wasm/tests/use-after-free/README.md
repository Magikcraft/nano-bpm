# engine-wasm use-after-free DX test

Freezes the DX contract from
[#968](https://github.com/Magikcraft/nano-bpm/issues/968): calling a `TestEngine`
method **after** the handle has been freed throws a self-describing

```
TestEngine used after free(): '<method>' called on a released engine handle
```

instead of wasm-bindgen's opaque `null pointer passed to rust`.

## Why

wasm-bindgen's runtime `assert_not_null` (compiled into `_bg.wasm`) correctly
refuses a call on a freed handle rather than corrupting memory — the engine has
no memory-safety bug. But the raw message names neither the engine, the method,
nor the cause (a host use-after-free). The friendlier message makes a host
lifecycle bug — e.g. a fire-and-forget worker handler that outlives engine
teardown — diagnosable at the call site.

The message is produced by a single canonical prototype guard injected into the
generated glue by [`../../scripts/inject-free-guard.mjs`](../../scripts/inject-free-guard.mjs),
which `make console-wasm` re-runs after each wasm-pack regeneration (both the
lean `.` and read-model `/readmodel` variants).

## Run

```
npm install && npm test
```
