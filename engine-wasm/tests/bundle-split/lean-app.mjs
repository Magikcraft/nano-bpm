// Imports ONLY the default (lean) subpath. A bundler must emit only the lean
// _bg.wasm for this entrypoint — the readmodel wasm must be absent.
import init, { TestEngine } from "@nanobpm/engine-wasm";
export { init, TestEngine };
