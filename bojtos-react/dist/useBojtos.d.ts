import { type DispatchOptions, type JobHandler, type RoundResult, type Snapshot, type WasmEvent, type WasmSource } from "@nanobpm/bojtos-kit";
/** Lifecycle of the in-browser engine load. */
export type BojtosPhase = "loading" | "ready" | "error";
export interface UseBojtosOptions {
    /** The BPMN diagram XML to deploy. Re-deploys on a fresh engine when it changes. */
    bpmn: string;
    /**
     * Optional engine wasm source. Pass a `URL` / bytes / `WebAssembly.Module`
     * when the default `import.meta.url` loader can't resolve the binary (the
     * external-`.wasm` "wasmUrl" mode, or a non-Vite bundler — ADR 0043 §3).
     *
     * Init-time only: the wasm module loads once per page (see `ensureWasm`), so
     * changing `wasm` after the first successful init has no effect — it will not
     * reload the module.
     */
    wasm?: WasmSource;
}
export interface BojtosControls {
    phase: BojtosPhase;
    error: string | null;
    /** Deployable process ids from the current deployment. */
    processIds: string[];
    /** The latest snapshot, or `null` before the first command / after a reset. */
    snapshot: Snapshot | null;
    /** The engine's full event log after the latest command. */
    events: WasmEvent[];
    /** Start an instance; returns the post-run snapshot (with `created`) or null. */
    createInstance(processId: string, variablesJson: string): Snapshot | null;
    /** Complete a waiting job, merging output variables. */
    completeJob(jobKey: string, variablesJson: string): Snapshot | null;
    /** Fail a waiting job (raises an incident with no retries left). */
    failJob(jobKey: string, retries: number, message: string): Snapshot | null;
    /**
     * Correlate a message to an instance parked at a message catch/receive:
     * publishes `messageName` with `correlationKey` and merges `variablesJson`.
     * The in-browser equivalent of an app publishing a message — used to unblock
     * a waiting loop (e.g. urban-pr-review's `review-ready`).
     */
    correlateMessage(messageName: string, correlationKey: string, variablesJson: string): Snapshot | null;
    /** Advance the virtual clock. */
    advanceTime(byMs: number): Snapshot | null;
    /**
     * Run the registered worker handlers until the process settles (activate →
     * handler → complete/fail), then reflect the resulting snapshot/events.
     * Resolves to the settled snapshot, or null if there is no live session.
     */
    runWorkers(workers: Record<string, JobHandler>, opts?: DispatchOptions): Promise<Snapshot | null>;
    /**
     * Run a single activate-and-handle pass of the registered workers (one
     * {@link dispatchRound}), reflecting the resulting snapshot/events. Returns
     * how many jobs it handled (0 once the process is quiescent) plus the
     * snapshot, or null if there is no live session — drive it on a timer to
     * animate the token advancing one step at a time.
     */
    stepWorkers(workers: Record<string, JobHandler>, opts?: DispatchOptions): Promise<RoundResult | null>;
    /** Re-deploy the diagram on the existing engine, clearing run state. */
    reset(): void;
}
/**
 * React binding over a headless {@link BojtosSession}: owns the engine's
 * lifecycle and the reactive `snapshot` / `events` / `processIds` state, and
 * exposes the engine commands. The consuming component owns its own form state
 * (selected process, seed vars, per-job output) and drives the visual contract
 * (`<BpmnRuntimeView>` + the variable payload) off `snapshot`.
 *
 * This is the reactive half of the Bojtos public API (ADR 0043 §2); the console
 * test-run panel is its first consumer (§8 step 2 — dogfooding is the acceptance
 * test).
 */
export declare function useBojtos({ bpmn, wasm }: UseBojtosOptions): BojtosControls;
