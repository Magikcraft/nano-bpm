/* tslint:disable */
/* eslint-disable */

/**
 * A simulated engine instance bound to one modeler session.
 */
export class TestEngine {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Activate up to `max_jobs` `Created` jobs of `job_type`, locking them to
     * `worker` until `now + timeout_ms`. Returns a JSON array of activated jobs
     * (key, type, instance/element, retries, variables) for the dispatch loop to
     * hand to worker handlers. The host owns the wall clock via `tickNow`.
     */
    activateJobs(job_type: string, max_jobs: number, timeout_ms: number, worker: string): string;
    /**
     * Advance the virtual clock by `by_ms` milliseconds, firing any timers that
     * become due and expiring any lapsed job locks.
     */
    advanceTime(by_ms: number): string;
    /**
     * Complete a waiting job by key, merging `variables_json` (a JSON object
     * string) into the instance. The job is activated first if it has not been
     * already, so the UI can complete a freshly-created job directly.
     */
    completeJob(job_key: string, variables_json: string): string;
    /**
     * Start a new instance of `process_id`, seeding it with the given variables
     * (a JSON object string; pass `"{}"` or `""` for none). Returns the
     * post-run [`Snapshot`] with a top-level `created` field holding the new
     * instance key.
     */
    createInstance(process_id: string, variables_json: string): string;
    /**
     * Parse and deploy a BPMN resource. Returns a JSON object
     * `{ "processIds": [...], "snapshot": {...} }` on success, or throws a
     * JS error carrying the parse/deploy failure message.
     */
    deploy(xml: string): string;
    /**
     * The full ordered event log emitted so far, as a JSON array of
     * `{ seq, now, type, ...payload }`. Useful for a step-through / trace view.
     */
    events(): string;
    /**
     * Fail a waiting job by key with the given remaining `retries` and message.
     * With no retries left this raises an incident (visible in the snapshot).
     */
    failJob(job_key: string, retries: number, message: string): string;
    /**
     * Create a fresh, empty simulated engine. The virtual clock starts at 0.
     */
    constructor();
    /**
     * The current simulation state as a JSON [`Snapshot`].
     */
    snapshot(): string;
    /**
     * Set the engine clock to a wall-clock instant (ms), then trigger due timers
     * and expire lapsed job locks. The embedded host calls this with `Date.now()`
     * so `engine-core` stays clock-free while running as a real runtime. The
     * clock never moves backwards. Returns the snapshot.
     */
    tickNow(now_ms: number): string;
    /**
     * The current virtual clock (milliseconds).
     */
    readonly now: number;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_testengine_free: (a: number, b: number) => void;
    readonly testengine_activateJobs: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number) => void;
    readonly testengine_advanceTime: (a: number, b: number, c: number) => void;
    readonly testengine_completeJob: (a: number, b: number, c: number, d: number, e: number, f: number) => void;
    readonly testengine_createInstance: (a: number, b: number, c: number, d: number, e: number, f: number) => void;
    readonly testengine_deploy: (a: number, b: number, c: number, d: number) => void;
    readonly testengine_events: (a: number, b: number) => void;
    readonly testengine_failJob: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => void;
    readonly testengine_new: () => number;
    readonly testengine_now: (a: number) => number;
    readonly testengine_snapshot: (a: number, b: number) => void;
    readonly testengine_tickNow: (a: number, b: number, c: number) => void;
    readonly __wbindgen_add_to_stack_pointer: (a: number) => number;
    readonly __wbindgen_export: (a: number, b: number) => number;
    readonly __wbindgen_export2: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_export3: (a: number, b: number, c: number) => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
