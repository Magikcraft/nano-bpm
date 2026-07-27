import init, { TestEngine } from "@nanobpm/engine-wasm";
// Lazily initialise the wasm module exactly once per page, no matter how many
// sessions are created. Mirrors the console's original `ensureWasm`.
let wasmReady = null;
/**
 * Initialise the wasm engine module (idempotent; safe to call repeatedly). The
 * first successful call wins: a `source` passed to a later call is ignored once
 * the module is already loading or loaded. Pass a `source` in environments where
 * the default `import.meta.url` fetch can't resolve the binary
 * (Node/Jest/webpack).
 *
 * If a load *fails*, the cached promise is cleared so a later call — e.g. one
 * that supplies a working `WasmSource` after the default loader couldn't resolve
 * the binary — can retry rather than being stuck on the first rejection.
 */
export function ensureWasm(source) {
    if (!wasmReady) {
        wasmReady = init(source === undefined ? undefined : { module_or_path: source })
            .then(() => undefined)
            .catch((e) => {
            wasmReady = null;
            throw e;
        });
    }
    return wasmReady;
}
function parseSnapshot(json) {
    // The wasm engine is the schema authority; its JSON is the contract boundary.
    return JSON.parse(json);
}
class WasmBojtosSession {
    engine;
    constructor(engine) {
        this.engine = engine;
    }
    deploy(xml) {
        return JSON.parse(this.engine.deploy(xml));
    }
    createInstance(processId, variablesJson) {
        return parseSnapshot(this.engine.createInstance(processId, variablesJson || "{}"));
    }
    activateJobs(jobType, maxJobs, timeoutMs, worker) {
        return JSON.parse(this.engine.activateJobs(jobType, maxJobs, timeoutMs, worker));
    }
    completeJob(jobKey, variablesJson) {
        return parseSnapshot(this.engine.completeJob(jobKey, variablesJson || "{}"));
    }
    failJob(jobKey, retries, message) {
        return parseSnapshot(this.engine.failJob(jobKey, retries, message));
    }
    advanceTime(byMs) {
        return parseSnapshot(this.engine.advanceTime(byMs));
    }
    events() {
        return JSON.parse(this.engine.events());
    }
    snapshot() {
        return parseSnapshot(this.engine.snapshot());
    }
    free() {
        this.engine.free();
    }
}
/**
 * Create a fresh headless engine session. Ensures the wasm module is loaded
 * (once per page), then constructs a new {@link TestEngine}. The virtual clock
 * starts at 0; deploy a diagram before starting instances. Pass a `wasm` source
 * in environments where the default `import.meta.url` loader can't resolve the
 * binary (Node/Jest, or the external-`.wasm` mode — ADR 0043 §3).
 */
export async function createBojtosSession(opts) {
    await ensureWasm(opts?.wasm);
    return new WasmBojtosSession(new TestEngine());
}
