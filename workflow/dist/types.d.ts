/** A JSON-serialisable value, as carried by process variables. */
export type Json = null | boolean | number | string | Json[] | {
    [k: string]: Json;
};
export type JsonObject = {
    [k: string]: Json;
};
/** A job as delivered by the nanobpmn gateway's `POST /v2/jobs/activation`. */
export interface Job {
    jobKey: string;
    processInstanceKey: string;
    elementId: string;
    type: string;
    variables: JsonObject;
}
/** Handler for a declarative `run` step: does real work, returns variables. */
export type StepHandler = (job: Job) => Promise<JsonObject | void> | JsonObject | void;
export type DeclarativeStep = {
    kind: "run";
    name: string;
} | {
    kind: "task";
    name: string;
} | {
    kind: "signal";
    name: string;
    correlationKey: string;
};
export interface DeclarativeFlow {
    kind: "declarative";
    id: string;
    steps: DeclarativeStep[];
    handlers: Record<string, StepHandler>;
}
/** The context passed to an imperative orchestration function. */
export interface WorkflowContext {
    /** The workflow's start input (immutable across replays). */
    readonly input: JsonObject;
    /**
     * A durable activity. On first execution the handler runs (its side effects
     * happen once); on every subsequent replay the recorded result is returned
     * WITHOUT invoking the handler. The handler is the only place side effects
     * (I/O, network, shell, LLM) are allowed — the orchestration body itself must
     * be deterministic.
     */
    run<T extends Json = Json>(name: string, fn: () => Promise<T> | T): Promise<T>;
}
export type Orchestration = (ctx: WorkflowContext) => Promise<void>;
export interface ImperativeWorkflow {
    kind: "imperative";
    id: string;
    orchestrate: Orchestration;
    /** Derived job type of the single orchestrator task: `<id>:__orchestrate`. */
    orchestrateType: string;
}
export type Workflow = DeclarativeFlow | ImperativeWorkflow;
/** Result of deploying a workflow. */
export interface DeployResult {
    [k: string]: Json;
}
/** Result of starting a workflow instance. */
export interface StartResult {
    processInstanceKey?: string;
    [k: string]: Json | undefined;
}
