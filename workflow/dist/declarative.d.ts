import type { DeclarativeFlow, StepHandler } from "./types.js";
export interface FlowBuilder {
    /** A durable activity served by a worker (a BPMN service task). */
    run(name: string, handler: StepHandler): FlowBuilder;
    /**
     * A durable wait for an external/human event, correlated on a process
     * variable (a BPMN message intermediate catch event). Resume it with
     * `WorkflowClient.signal(flow, name, correlationKeyValue, vars)`.
     */
    signal(name: string, opts: {
        correlationKey: string;
    }): FlowBuilder;
}
/** Define a declarative flow. `build(w)` declares an ordered list of steps. */
export declare function defineFlow(id: string, build: (w: FlowBuilder) => void): DeclarativeFlow;
/** Derive an executable BPMN model from a declarative flow. */
export declare function declarativeToBpmn(flow: DeclarativeFlow): string;
