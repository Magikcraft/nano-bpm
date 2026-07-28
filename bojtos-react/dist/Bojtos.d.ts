import type { JobHandler, WasmEvent } from "@nanobpm/bojtos-kit";
/** A single engine event handed to `onTrace` as the simulation runs. */
export type TraceEvent = WasmEvent;
export interface BojtosProps {
    /** The BPMN diagram XML to run. */
    bpmn: string;
    /**
     * The in-browser workers, keyed by the model's job type (task definition
     * type). Each handler receives the activated job (with the instance's live
     * variables) and returns the variables to merge on completion — or throws to
     * fail the job. This is the code a demo author edits to shape the run.
     */
    workers: Record<string, JobHandler>;
    /** Initial instance variables (the starting payload). Defaults to `{}`. */
    seed?: Record<string, unknown>;
    /** Start the instance and run the workers automatically once ready. */
    autoplay?: boolean;
    /**
     * Milliseconds between dispatch rounds while playing (default 700). The pause
     * is what makes the token visibly hop task-to-task instead of settling
     * instantly.
     */
    stepDelayMs?: number;
    /**
     * Which deployed process to start. Defaults to the first process in the
     * diagram — set this only for a multi-process `.bpmn`.
     */
    processId?: string;
    /**
     * Optional engine wasm URL for bundlers where the default `import.meta.url`
     * loader can't resolve the binary (ADR 0043 §3).
     */
    wasmUrl?: string;
    /** Called for every engine event as the simulation advances. */
    onTrace?: (event: TraceEvent) => void;
    /** Optional class for the outer container. */
    className?: string;
}
/**
 * The turnkey Bojtos demo component (ADR 0043 §2): drop in a `bpmn` diagram and
 * a map of in-browser `workers`, and it renders the live token/incident diagram
 * beside the running variable payload, driving the "activate → handler →
 * complete/fail" loop so you watch the token advance and the payload mutate as
 * each worker runs.
 *
 * The consuming app must load bpmn-js's diagram CSS once
 * (`bpmn-js/dist/assets/diagram-js.css` and
 * `.../bpmn-font/css/bpmn-embedded.css`); the token/incident marker styles are
 * injected here.
 */
export declare function Bojtos({ bpmn, workers, seed, autoplay, stepDelayMs, processId, wasmUrl, onTrace, className, }: BojtosProps): import("react").JSX.Element;
