// The shapes the in-browser engine (`@nanobpm/engine-wasm`) emits from its
// JSON string surface — `deploy` / `createInstance` / `completeJob` / `failJob`
// / `advanceTime` / `snapshot` return a `Snapshot`, and `events()` returns a
// `WasmEvent[]`. These describe the engine's public contract, so they live in
// the framework-agnostic kit and are re-exported by the React binding.
export {};
