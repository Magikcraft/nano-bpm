import { type Snapshot, type WasmEvent } from "@nanobpm/bojtos-kit";
/** Lifecycle of the in-browser engine load. */
export type BojtosPhase = "loading" | "ready" | "error";
export interface UseBojtosOptions {
    /** The BPMN diagram XML to deploy. Re-deploys on a fresh engine when it changes. */
    bpmn: string;
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
    /** Advance the virtual clock. */
    advanceTime(byMs: number): Snapshot | null;
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
export declare function useBojtos({ bpmn }: UseBojtosOptions): BojtosControls;
