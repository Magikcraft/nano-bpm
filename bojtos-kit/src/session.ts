import init, { type InitInput, TestEngine } from "@nanobpm/engine-wasm";
import type { ActivatedJob, Snapshot, WasmEvent } from "./types.js";

// Lazily initialise the wasm module exactly once per page, no matter how many
// sessions are created. Mirrors the console's original `ensureWasm`.
let wasmReady: Promise<void> | null = null;

/**
 * The source of the engine wasm binary. Under a bundler that understands
 * `new URL(..., import.meta.url)` (e.g. Vite) the default loader needs no
 * argument; pass an explicit `URL` / `Response` / bytes / `WebAssembly.Module`
 * when the environment can't resolve it that way (Node, Jest, or the external-
 * `.wasm` "wasmUrl" mode — ADR 0043 §3).
 */
export type WasmSource = InitInput;

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
export function ensureWasm(source?: WasmSource): Promise<void> {
  if (!wasmReady) {
    wasmReady = init(
      source === undefined ? undefined : { module_or_path: source },
    )
      .then(() => undefined)
      .catch((e) => {
        wasmReady = null;
        throw e;
      });
  }
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
  /**
   * Activate up to `maxJobs` `Created` jobs of `jobType`, locking them to
   * `worker` until `now + timeoutMs`. Returns the activated jobs (each carrying
   * the instance's current variables) for a dispatch loop to hand to worker
   * handlers. A job that is already activated is not re-returned.
   */
  activateJobs(
    jobType: string,
    maxJobs: number,
    timeoutMs: number,
    worker: string,
  ): ActivatedJob[];
  /** Complete a waiting job, merging `variablesJson` into the instance. */
  completeJob(jobKey: string, variablesJson: string): Snapshot;
  /** Fail a waiting job; with no retries left this raises an incident. */
  failJob(jobKey: string, retries: number, message: string): Snapshot;
  /**
   * Correlate a message to any instance waiting on it: publishes `messageName`
   * with `correlationKey` (the value the waiting subscription's `correlationKey`
   * expression resolved to) and merges `variablesJson` into each correlated
   * instance. Unblocks a message intermediate-catch / receive task without an
   * external broker — the in-browser equivalent of an app publishing a message.
   */
  correlateMessage(
    messageName: string,
    correlationKey: string,
    variablesJson: string,
  ): Snapshot;
  /** Advance the virtual clock by `byMs`, firing due timers and lapsed locks. */
  advanceTime(byMs: number): Snapshot;
  /**
   * Discard all engine state (definitions, instances, jobs, timers, event log
   * and clock), returning the underlying engine to its pristine state. The
   * caller redeploys afterwards to begin a clean run — this is what lets a
   * re-run start from zero completed instances instead of accumulating across
   * runs.
   */
  reset(): void;
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

  activateJobs(
    jobType: string,
    maxJobs: number,
    timeoutMs: number,
    worker: string,
  ): ActivatedJob[] {
    return JSON.parse(
      this.engine.activateJobs(jobType, maxJobs, timeoutMs, worker),
    ) as ActivatedJob[];
  }

  completeJob(jobKey: string, variablesJson: string): Snapshot {
    return parseSnapshot(this.engine.completeJob(jobKey, variablesJson || "{}"));
  }

  failJob(jobKey: string, retries: number, message: string): Snapshot {
    return parseSnapshot(this.engine.failJob(jobKey, retries, message));
  }

  correlateMessage(
    messageName: string,
    correlationKey: string,
    variablesJson: string,
  ): Snapshot {
    return parseSnapshot(
      this.engine.correlateMessage(
        messageName,
        correlationKey,
        variablesJson || "{}",
      ),
    );
  }

  advanceTime(byMs: number): Snapshot {
    return parseSnapshot(this.engine.advanceTime(byMs));
  }

  reset(): void {
    this.engine.reset();
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
 * starts at 0; deploy a diagram before starting instances. Pass a `wasm` source
 * in environments where the default `import.meta.url` loader can't resolve the
 * binary (Node/Jest, or the external-`.wasm` mode — ADR 0043 §3).
 */
export async function createBojtosSession(opts?: {
  wasm?: WasmSource;
}): Promise<BojtosSession> {
  await ensureWasm(opts?.wasm);
  return new WasmBojtosSession(new TestEngine());
}
