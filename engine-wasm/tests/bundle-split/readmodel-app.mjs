// Imports ONLY the /readmodel subpath. A bundler must emit only the readmodel
// _bg.wasm for this entrypoint — the lean wasm must be absent.
import init, { TestEngine } from "@nanobpm/engine-wasm/readmodel";
export { init, TestEngine };
