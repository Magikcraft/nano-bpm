import type { Snapshot, WasmEvent } from "./types.js";
/** Initialise the wasm engine module (idempotent; safe to call repeatedly). */
export declare function ensureWasm(): Promise<void>;
/**
 * A headless handle to one in-browser engine instance: deploy a diagram, start
 * instances, complete/fail jobs, advance the virtual clock, and read the event
 * log. Every command returns the post-run {@link Snapshot}. This is the single
 * scenario runner the Bojtos framework and the console both drive (ADR 0043 §8);
 * framework bindings (`@nanobpm/bojtos-react`) own the reactive state on top.
 */
export interface BojtosSession {
    /**
     * Parse and deploy a BPMN resource. Returns the deployable process ids.
     * Throws a JS error carrying the parse/deploy failure message.
     */
    deploy(xml: string): {
        processIds: string[];
    };
    /** Start an instance of `processId`, seeding it with `variablesJson`. */
    createInstance(processId: string, variablesJson: string): Snapshot;
    /** Complete a waiting job, merging `variablesJson` into the instance. */
    completeJob(jobKey: string, variablesJson: string): Snapshot;
    /** Fail a waiting job; with no retries left this raises an incident. */
    failJob(jobKey: string, retries: number, message: string): Snapshot;
    /** Advance the virtual clock by `byMs`, firing due timers and lapsed locks. */
    advanceTime(byMs: number): Snapshot;
    /** The full ordered event log emitted so far. */
    events(): WasmEvent[];
    /** The current simulation state. */
    snapshot(): Snapshot;
    /** Release the underlying wasm engine. */
    free(): void;
}
/**
 * Create a fresh headless engine session. Ensures the wasm module is loaded
 * (once per page), then constructs a new {@link TestEngine}. The virtual clock
 * starts at 0; deploy a diagram before starting instances.
 */
export declare function createBojtosSession(): Promise<BojtosSession>;
