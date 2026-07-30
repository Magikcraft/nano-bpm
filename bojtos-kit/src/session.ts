import init, { type InitInput, TestEngine } from "@nanobpm/engine-wasm";
import type {
  ActivatedJob,
  ActivateInstruction,
  AgentResult,
  Snapshot,
  WasmEvent,
} from "./types.js";

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
  /**
   * Complete an ad-hoc sub-process's **agent** job — the container's JOB_WORKER
   * job (Camunda's agentic `aiagent-job-worker`) — carrying the agent's
   * {@link AgentResult}. Its `activateElements` run the chosen inner tools this
   * turn; `completionConditionFulfilled` ends the agent loop; `variables` merge
   * into the instance (e.g. the agent's final decision). This is the ad-hoc seam
   * plain {@link completeJob} deliberately omits. Register agents on the dispatch
   * loop via `DispatchOptions.agents` rather than calling this directly.
   */
  completeAgentJob(jobKey: string, result: AgentResult): Snapshot;
  /** Fail a waiting job; with no retries left this raises an incident. */
  failJob(jobKey: string, retries: number, message: string): Snapshot;
  /**
   * Throw a BPMN business error from a waiting job: interrupts the activity via a
   * matching error boundary/event-subprocess catch, or raises an incident if
   * uncaught. The job is consumed either way.
   */
  throwError(jobKey: string, errorCode: string, errorMessage: string): Snapshot;
  /**
   * Set a job's remaining retries. Used to recover a job parked on a no-retries
   * incident before resolving that incident; does not itself unblock the job.
   */
  updateRetries(jobKey: string, retries: number): Snapshot;
  /**
   * Resolve an open incident by key, retrying the work that failed (returns a
   * parked job to the activatable pool / re-evaluates a gateway / re-creates a
   * service-task job).
   */
  resolveIncident(incidentKey: string): Snapshot;
  /**
   * Merge variables into a scope (a process-instance or element-instance key).
   * When `local` is true they are written strictly into the target scope,
   * otherwise they propagate up to the nearest ancestor scope defining each name.
   */
  setVariables(
    scopeKey: string,
    variablesJson: string,
    local: boolean,
  ): Snapshot;
  /**
   * Broadcast a signal by name to every open subscription that matches, across
   * all instances, merging `variablesJson` into each correlated instance.
   */
  broadcastSignal(signalName: string, variablesJson: string): Snapshot;
  /** Cancel (terminate) a running process instance: every token is discarded. */
  cancelInstance(instanceKey: string): Snapshot;
  /**
   * Modify a running process instance (Zeebe "modify process instance"): move
   * tokens by terminating existing element instances and/or activating new
   * ones. Each activate instruction places a token at `elementId` (in the
   * process root scope), first merging its optional `variables` into the root
   * scope; `terminateElementInstanceKeys` are the keys of active element
   * instances (from `instances[].activeElements[].key`) to terminate. If the
   * terminations drain the last token and nothing is activated, the instance is
   * terminated.
   */
  modify(
    instanceKey: string,
    activateInstructions: ActivateInstruction[],
    terminateElementInstanceKeys: string[],
  ): Snapshot;
  /** Complete a waiting user task, merging `variablesJson` into the instance. */
  completeUserTask(userTaskKey: string, variablesJson: string): Snapshot;
  /**
   * Assign a user task to `assignee`. With `allowOverride` false the command is
   * rejected if the task already has an assignee (unassign it first).
   */
  assignUserTask(
    userTaskKey: string,
    assignee: string,
    allowOverride: boolean,
  ): Snapshot;
  /** Clear a user task's assignee. */
  unassignUserTask(userTaskKey: string): Snapshot;
  /**
   * Update a user task's attributes from a JSON changeset. Recognised keys (all
   * optional): `candidateGroups` / `candidateUsers` (string arrays),
   * `dueDate` / `followUpDate` (ISO-8601 string, or `null`/`""` to clear),
   * `priority` (0..=100). Only present keys are changed.
   */
  updateUserTask(userTaskKey: string, changesetJson: string): Snapshot;
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

  completeAgentJob(jobKey: string, result: AgentResult): Snapshot {
    const { variables, ...agentResult } = result ?? {};
    return parseSnapshot(
      this.engine.completeAgentJob(
        jobKey,
        JSON.stringify(variables ?? {}),
        JSON.stringify(agentResult ?? {}),
      ),
    );
  }

  failJob(jobKey: string, retries: number, message: string): Snapshot {
    return parseSnapshot(this.engine.failJob(jobKey, retries, message));
  }

  throwError(
    jobKey: string,
    errorCode: string,
    errorMessage: string,
  ): Snapshot {
    return parseSnapshot(
      this.engine.throwError(jobKey, errorCode, errorMessage),
    );
  }

  updateRetries(jobKey: string, retries: number): Snapshot {
    return parseSnapshot(this.engine.updateRetries(jobKey, retries));
  }

  resolveIncident(incidentKey: string): Snapshot {
    return parseSnapshot(this.engine.resolveIncident(incidentKey));
  }

  setVariables(
    scopeKey: string,
    variablesJson: string,
    local: boolean,
  ): Snapshot {
    return parseSnapshot(
      this.engine.setVariables(scopeKey, variablesJson || "{}", local),
    );
  }

  broadcastSignal(signalName: string, variablesJson: string): Snapshot {
    return parseSnapshot(
      this.engine.broadcastSignal(signalName, variablesJson || "{}"),
    );
  }

  cancelInstance(instanceKey: string): Snapshot {
    return parseSnapshot(this.engine.cancelInstance(instanceKey));
  }

  modify(
    instanceKey: string,
    activateInstructions: ActivateInstruction[],
    terminateElementInstanceKeys: string[],
  ): Snapshot {
    return parseSnapshot(
      this.engine.modify(
        instanceKey,
        JSON.stringify(activateInstructions ?? []),
        JSON.stringify(terminateElementInstanceKeys ?? []),
      ),
    );
  }

  completeUserTask(userTaskKey: string, variablesJson: string): Snapshot {
    return parseSnapshot(
      this.engine.completeUserTask(userTaskKey, variablesJson || "{}"),
    );
  }

  assignUserTask(
    userTaskKey: string,
    assignee: string,
    allowOverride: boolean,
  ): Snapshot {
    return parseSnapshot(
      this.engine.assignUserTask(userTaskKey, assignee, allowOverride),
    );
  }

  unassignUserTask(userTaskKey: string): Snapshot {
    return parseSnapshot(this.engine.unassignUserTask(userTaskKey));
  }

  updateUserTask(userTaskKey: string, changesetJson: string): Snapshot {
    return parseSnapshot(
      this.engine.updateUserTask(userTaskKey, changesetJson || "{}"),
    );
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
