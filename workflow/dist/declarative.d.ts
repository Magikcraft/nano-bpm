import type { DeclarativeFlow, StepHandler } from "./types.js";
export interface FlowBuilder {
    /** A durable activity served by a worker THIS program hosts (a BPMN service
     *  task; the handler runs in the in-process `Worker`). */
    run(name: string, handler: StepHandler): FlowBuilder;
    /**
     * A durable activity served by a worker OUTSIDE this program (a BPMN service
     * task with the same derived job type `${flowId}:${name}`, but no
     * locally-hosted handler). The engine offers the job to whichever worker
     * subscribes to that type — in another process, service, or language. Use
     * `externalJobTypes(flow)` to list the contract those workers must poll.
     */
    task(name: string): FlowBuilder;
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
/** The derived job types of a flow's external `task` steps — the contract that
 *  workers outside this program must subscribe to (the engine offers those jobs
 *  to whoever polls the type). Empty when the flow hosts all its own steps. */
export declare function externalJobTypes(flow: DeclarativeFlow): string[];
/** Derive an executable BPMN model from a declarative flow. */
export declare function declarativeToBpmn(flow: DeclarativeFlow): string;
