import type { DeclarativeFlow, FlowContracts, FlowNode, JsonObject } from "./types.js";
import type { Envelope } from "./envelope.js";
/** The TS payload type of a contract's input envelope (untyped fallback). */
type InPayload<Ct> = Ct extends {
    in: Envelope;
} ? Ct["in"]["type"] : JsonObject;
/** The TS payload type of a contract's output envelope (untyped fallback). */
type OutPayload<Ct> = Ct extends {
    out: Envelope;
} ? Ct["out"]["type"] : JsonObject;
/** The input payload type of step `K` under contracts `C`. */
type VarsOf<C, K extends string> = K extends keyof C ? InPayload<C[K]> : JsonObject;
/** The output payload type of step `K` under contracts `C`. */
type ResultOf<C, K extends string> = K extends keyof C ? OutPayload<C[K]> : JsonObject;
/** A typed handler for a `run` step: its job variables and result are resolved
 *  from the flow contracts by the step name. */
type TypedHandler<V, R> = (job: {
    jobKey: string;
    processInstanceKey: string;
    elementId: string;
    type: string;
    variables: V;
}) => Promise<R | void> | R | void;
/** A block: the callback that populates a nested body (a case, arm, or loop). */
type Block<C extends FlowContracts> = (b: FlowBuilder<C>) => void;
export interface FlowBuilder<C extends FlowContracts = Record<string, never>> {
    /**
     * A durable activity served by a worker THIS program hosts (a BPMN service
     * task; the handler runs in the in-process `Worker`). If `name` is a key in
     * the flow's contracts, the handler's job variables and return value are typed
     * from that contract's `in`/`out` envelopes; otherwise they are `JsonObject`.
     */
    run<K extends string>(name: K, handler: TypedHandler<VarsOf<C, K>, ResultOf<C, K>>): FlowBuilder<C>;
    /**
     * A durable activity served by a worker OUTSIDE this program (a BPMN service
     * task, but no locally-hosted handler). Its job type defaults to the derived
     * `${flowId}:${name}`; pass `{ jobType }` to override it with an explicit
     * worker token (e.g. a `rank:capability` token like `senior:pr-review` that a
     * `c8ctl nano work` matrix subscribes to) so an existing pool of agents can
     * service it without renaming the flow. The step name stays the BPMN element
     * id; only the emitted `zeebe:taskDefinition` type changes. Use
     * `externalJobTypes(flow)` to list the (possibly overridden) types those
     * workers must poll. Its contract envelopes (if any) type the model, not a
     * local handler.
     */
    task<K extends string>(name: K, opts?: {
        jobType?: string;
    }): FlowBuilder<C>;
    /**
     * A durable wait for an external/human event, correlated on a process
     * variable (a BPMN message intermediate catch event). Resume it with
     * `WorkflowClient.signal(flow, name, correlationKeyValue, vars)`. The message
     * payload envelope, if any, comes from the contract's `in`.
     */
    signal<K extends string>(name: K, opts: {
        correlationKey: string;
    }): FlowBuilder<C>;
    /**
     * A multi-way exclusive choice (a BPMN exclusive gateway). `subject` is a FEEL
     * expression (usually a variable name); each case routes when `subject` equals
     * the case value. An optional `default` case is the unconditional fallback.
     */
    switch(subject: string, cases: Record<string, Block<C>> & {
        default?: Block<C>;
    }): FlowBuilder<C>;
    /**
     * A two-way exclusive choice on a FEEL boolean `condition` (a BPMN exclusive
     * gateway). The `then` branch is guarded by the condition; the `else` branch
     * (the gateway default) runs otherwise. Omitting `else` skips to whatever
     * follows the branch when the condition is false.
     */
    branch(condition: string, arms: {
        then: Block<C>;
        else?: Block<C>;
    }): FlowBuilder<C>;
    /**
     * A durable loop (a back-edge to the loop head). The body runs, then control
     * returns to the top of the loop unless a branch calls `break()`. Nodes after
     * the loop run once `break()` is reached.
     */
    loop(body: Block<C>): FlowBuilder<C>;
    /** Exit the enclosing loop (routes to whatever follows it). Only valid inside
     *  a `loop`. */
    break(): FlowBuilder<C>;
    /** Jump straight back to the top of the enclosing loop, skipping the rest of
     *  the body. Only valid inside a `loop`. */
    continue(): FlowBuilder<C>;
}
/**
 * Define a declarative flow. Pass a typed `contracts` map (keyed by step name)
 * to type each step's I/O and lift its data envelopes into the model; or omit it
 * for an untyped flow. `build(w)` declares a tree of nodes.
 */
export declare function defineFlow<C extends FlowContracts>(id: string, contracts: C, build: (w: FlowBuilder<C>) => void): DeclarativeFlow;
export declare function defineFlow(id: string, build: (w: FlowBuilder) => void): DeclarativeFlow;
/** Depth-first visit of every node in a flow tree (structural combinators
 *  recurse into their bodies). */
export declare function walkNodes(nodes: FlowNode[], visit: (n: FlowNode) => void): void;
/** The derived job types of a flow's external `task` steps (anywhere in the
 *  tree) — the contract workers outside this program must subscribe to. */
export declare function externalJobTypes(flow: DeclarativeFlow): string[];
/** Derive an executable BPMN model from a declarative flow. */
export declare function declarativeToBpmn(flow: DeclarativeFlow): string;
export {};
