import init, { TestEngine } from "@nanobpm/engine-wasm";
import type { Snapshot, WasmEvent } from "./types.js";

// Lazily initialise the wasm module exactly once per page, no matter how many
// sessions are created. Mirrors the console's original `ensureWasm`.
let wasmReady: Promise<void> | null = null;

/** Initialise the wasm engine module (idempotent; safe to call repeatedly). */
export function ensureWasm(): Promise<void> {
  if (!wasmReady) wasmReady = init().then(() => undefined);
  return wasmReady;
}

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
  deploy(xml: string): { processIds: string[] };
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

function parseSnapshot(json: string): Snapshot {
  // The wasm engine is the schema authority; its JSON is the contract boundary.
  return JSON.parse(json) as Snapshot;
}

class WasmBojtosSession implements BojtosSession {
  private readonly engine: TestEngine;

  constructor(engine: TestEngine) {
    this.engine = engine;
  }

  deploy(xml: string): { processIds: string[] } {
    return JSON.parse(this.engine.deploy(xml)) as { processIds: string[] };
  }

  createInstance(processId: string, variablesJson: string): Snapshot {
    return parseSnapshot(
      this.engine.createInstance(processId, variablesJson || "{}"),
    );
  }

  completeJob(jobKey: string, variablesJson: string): Snapshot {
    return parseSnapshot(this.engine.completeJob(jobKey, variablesJson || "{}"));
  }

  failJob(jobKey: string, retries: number, message: string): Snapshot {
    return parseSnapshot(this.engine.failJob(jobKey, retries, message));
  }

  advanceTime(byMs: number): Snapshot {
    return parseSnapshot(this.engine.advanceTime(byMs));
  }

  events(): WasmEvent[] {
    return JSON.parse(this.engine.events()) as WasmEvent[];
  }

  snapshot(): Snapshot {
    return parseSnapshot(this.engine.snapshot());
  }

  free(): void {
    this.engine.free();
  }
}

/**
 * Create a fresh headless engine session. Ensures the wasm module is loaded
 * (once per page), then constructs a new {@link TestEngine}. The virtual clock
 * starts at 0; deploy a diagram before starting instances.
 */
export async function createBojtosSession(): Promise<BojtosSession> {
  await ensureWasm();
  return new WasmBojtosSession(new TestEngine());
}
