import type { ImperativeWorkflow, Json, JsonObject, Orchestration } from "./types.js";
/** Define an imperative, replay-driven durable workflow.
 *
 * @experimental Not the recommended code-first surface. Prefer `defineFlow`
 * (declarative), whose steps are engine-visible BPMN nodes. This imperative
 * replay surface compiles to a single opaque looping orchestrator and requires
 * determinism discipline in the orchestration body; it is retained for advanced
 * durable-orchestration use only. */
export declare function defineWorkflow(id: string, orchestrate: Orchestration): ImperativeWorkflow;
/** The looped-orchestrator model: start → orchestrate → gw → (done ? end : loop). */
export declare function imperativeToBpmn(wf: ImperativeWorkflow): string;
/** A journal of recorded step results, keyed by call-ordinal + name. */
export type Journal = Record<string, Json>;
/** Outcome of a single replay pass. */
export type ReplayStep = {
    done: true;
} | {
    done: false;
    frontier: {
        key: string;
        result: Json;
    };
};
/**
 * Replay the orchestration function against a journal. Returns `{ done: true }`
 * if the function ran to completion, or the frontier step (the one un-recorded
 * `ctx.run`) whose handler was just executed and must be journalled next turn.
 *
 * Duplicate `ctx.run` names within a single pass are disambiguated by ordinal,
 * so a loop that calls `ctx.run("x", …)` repeatedly still gets distinct keys.
 */
export declare function replayOnce(wf: ImperativeWorkflow, input: JsonObject, journal: Journal): Promise<ReplayStep>;
