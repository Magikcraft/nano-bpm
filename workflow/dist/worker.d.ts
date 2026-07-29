import { WorkflowClient } from "./client.js";
import type { Workflow } from "./types.js";
/** Fired after a job completes; purely observational. */
export interface ActivityEvent {
    workflowId: string;
    type: string;
    jobKey: string;
    elementId: string;
    /** For an imperative orchestrator turn: the journalled step key, or "__done". */
    step?: string;
}
export interface WorkerOptions {
    /** Provide a baseUrl (a client is created) or an existing client. */
    baseUrl?: string;
    client?: WorkflowClient;
    workflows: Workflow[];
    /** Worker name reported to the gateway. Default "nanobpm-workflow". */
    name?: string;
    /** Long-poll timeout per activation, ms. Also bounds stop() latency. Default 10000. */
    pollTimeoutMs?: number;
    /** Job lock timeout, ms. Default 30000. */
    jobTimeoutMs?: number;
    /** Backoff after a transport error, ms. Default 500. */
    backoffMs?: number;
    onActivity?: (e: ActivityEvent) => void | Promise<void>;
    onError?: (err: Error, context: {
        type: string;
    }) => void;
}
export declare class Worker {
    private readonly client;
    private readonly name;
    private readonly pollTimeoutMs;
    private readonly jobTimeoutMs;
    private readonly backoffMs;
    private readonly onActivity?;
    private readonly onError?;
    /** job type → { workflowId, handle } */
    private readonly routes;
    private running;
    private loops;
    constructor(opts: WorkerOptions);
    private register;
    /** The derived job types this worker serves. */
    get servedTypes(): string[];
    /** Begin polling. Resolves once the loops are running (they run until stop()). */
    start(): void;
    /** Stop polling and wait for in-flight loops to unwind (≤ pollTimeoutMs). */
    stop(): Promise<void>;
    private pollLoop;
}
