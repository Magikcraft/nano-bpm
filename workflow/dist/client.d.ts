import type { DeclarativeFlow, DeployResult, Job, JsonObject, StartResult, Workflow } from "./types.js";
/** Render a workflow (either surface) to its executable BPMN model. */
export declare function toBpmn(wf: Workflow): string;
type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;
export interface WorkflowClientOptions {
    /** Base URL of the nanobpmn gateway, e.g. `http://localhost:8080`. */
    baseUrl: string;
    /** Injectable fetch (defaults to the global). Useful for tests/proxies. */
    fetch?: FetchLike;
}
export interface ActivateOptions {
    worker: string;
    maxJobsToActivate?: number;
    /** Job timeout in ms (how long the worker holds the job). Default 30000. */
    timeout?: number;
    /** Long-poll timeout in ms; `< 0` disables long-poll. Default 15000. */
    requestTimeout?: number;
}
export declare class WorkflowError extends Error {
    readonly status?: number | undefined;
    readonly body?: string | undefined;
    constructor(message: string, status?: number | undefined, body?: string | undefined);
}
export declare class WorkflowClient {
    private readonly baseUrl;
    private readonly fetchImpl;
    constructor(opts: WorkflowClientOptions);
    private json;
    /** Perform a request, throwing WorkflowError on transport error or !ok, and
     *  return the raw Response (for endpoints with an empty/no-content body). */
    private send;
    /** Deploy a workflow's derived BPMN model. */
    deploy(wf: Workflow): Promise<DeployResult>;
    /**
     * Start a workflow instance. For imperative workflows the engine variables are
     * seeded with `{ input, journal: {}, wfDone: false }` (the replay state); for
     * declarative flows `input` becomes the instance variables directly.
     */
    start(wf: Workflow, input?: JsonObject): Promise<StartResult>;
    /** Correlate a signal to a parked declarative `signal` step. */
    signal(flow: DeclarativeFlow, signalName: string, correlationKey: string, variables?: JsonObject): Promise<JsonObject>;
    /** Fetch an instance (used by demos/tests to observe completion). */
    getInstance(processInstanceKey: string): Promise<JsonObject | null>;
    activateJobs(type: string, opts: ActivateOptions): Promise<Job[]>;
    completeJob(jobKey: string, variables?: JsonObject): Promise<void>;
    failJob(jobKey: string, errorMessage: string, retries?: number): Promise<void>;
}
export {};
