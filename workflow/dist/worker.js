// The Worker runtime — hosts one or more workflows against a gateway and drives
// them: long-polls each derived job type, dispatches jobs to handlers, completes
// them. It generalises the two surfaces:
//
//   - a declarative flow contributes one job type per `run` step, dispatched to
//     the user handler;
//   - an imperative workflow contributes its single orchestrator job type,
//     dispatched to the replay engine (which advances the journal one step).
//
// It is resilient to the gateway disappearing (each poll loop backs off and
// reconnects on its own), so it survives an engine crash/restart — the property
// the ADR 0044 spike proved.
import { WorkflowClient } from "./client.js";
import { replayOnce } from "./imperative.js";
import { jobType } from "./xml.js";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export class Worker {
    client;
    name;
    pollTimeoutMs;
    jobTimeoutMs;
    backoffMs;
    onActivity;
    onError;
    /** job type → { workflowId, handle } */
    routes = new Map();
    running = false;
    loops = [];
    constructor(opts) {
        this.client = opts.client ?? new WorkflowClient({ baseUrl: requireBaseUrl(opts) });
        this.name = opts.name ?? "nanobpm-workflow";
        this.pollTimeoutMs = opts.pollTimeoutMs ?? 10000;
        this.jobTimeoutMs = opts.jobTimeoutMs ?? 30000;
        this.backoffMs = opts.backoffMs ?? 500;
        this.onActivity = opts.onActivity;
        this.onError = opts.onError;
        for (const wf of opts.workflows)
            this.register(wf);
    }
    register(wf) {
        if (wf.kind === "imperative") {
            this.routes.set(wf.orchestrateType, {
                workflowId: wf.id,
                handle: async (job) => {
                    const input = job.variables.input ?? {};
                    const journal = job.variables.journal ?? {};
                    const step = await replayOnce(wf, input, journal);
                    if (step.done)
                        return { variables: { wfDone: true }, step: "__done" };
                    const next = { ...journal, [step.frontier.key]: step.frontier.result };
                    return { variables: { journal: next, wfDone: false }, step: step.frontier.key };
                },
            });
        }
        else {
            for (const s of wf.steps) {
                if (s.kind !== "run")
                    continue;
                const handler = wf.handlers[s.name];
                this.routes.set(jobType(wf.id, s.name), {
                    workflowId: wf.id,
                    handle: async (job) => ({ variables: ((await handler(job)) ?? {}) }),
                });
            }
        }
    }
    /** The derived job types this worker serves. */
    get servedTypes() {
        return [...this.routes.keys()];
    }
    /** Begin polling. Resolves once the loops are running (they run until stop()). */
    start() {
        if (this.running)
            return;
        this.running = true;
        this.loops = [...this.routes.entries()].map(([type, route]) => this.pollLoop(type, route));
    }
    /** Stop polling and wait for in-flight loops to unwind (≤ pollTimeoutMs). */
    async stop() {
        this.running = false;
        await Promise.all(this.loops);
        this.loops = [];
    }
    async pollLoop(type, route) {
        while (this.running) {
            let jobs = [];
            try {
                jobs = await this.client.activateJobs(type, {
                    worker: this.name,
                    maxJobsToActivate: 1,
                    timeout: this.jobTimeoutMs,
                    requestTimeout: this.pollTimeoutMs,
                });
            }
            catch (e) {
                // Transport/gateway error (e.g. engine restarting): back off, reconnect.
                this.onError?.(e, { type });
                await sleep(this.backoffMs);
                continue;
            }
            for (const job of jobs) {
                if (!this.running)
                    break;
                try {
                    const { variables, step } = await route.handle(job);
                    await this.client.completeJob(job.jobKey, variables);
                    await this.onActivity?.({
                        workflowId: route.workflowId,
                        type,
                        jobKey: job.jobKey,
                        elementId: job.elementId,
                        step,
                    });
                }
                catch (e) {
                    // A handler or completion failed. Report; the engine will redeliver the
                    // job after its lock times out (at-least-once → handlers must be
                    // idempotent). Best-effort surface it as an incident-worthy failure.
                    this.onError?.(e, { type });
                    try {
                        await this.client.failJob(job.jobKey, e.message, 0);
                    }
                    catch {
                        /* engine may be down; the lock will expire and redeliver */
                    }
                }
            }
        }
    }
}
function requireBaseUrl(opts) {
    if (!opts.baseUrl)
        throw new Error("Worker needs either options.client or options.baseUrl");
    return opts.baseUrl;
}
