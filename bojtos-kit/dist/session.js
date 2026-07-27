import init, { TestEngine } from "@nanobpm/engine-wasm";
// Lazily initialise the wasm module exactly once per page, no matter how many
// sessions are created. Mirrors the console's original `ensureWasm`.
let wasmReady = null;
/** Initialise the wasm engine module (idempotent; safe to call repeatedly). */
export function ensureWasm() {
    if (!wasmReady)
        wasmReady = init().then(() => undefined);
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
 * starts at 0; deploy a diagram before starting instances.
 */
export async function createBojtosSession() {
    await ensureWasm();
    return new WasmBojtosSession(new TestEngine());
}
