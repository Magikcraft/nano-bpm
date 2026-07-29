// A thin, dependency-free client for a running nanobpmn gateway (REST v2):
// deploy a workflow's derived model, start instances, correlate signals, and the
// low-level job activate/complete/fail used by the Worker runtime.
import { declarativeToBpmn } from "./declarative.js";
import { imperativeToBpmn } from "./imperative.js";
import { assertWorkflowIds, messageName } from "./xml.js";
/** Render a workflow (either surface) to its executable BPMN model. */
export function toBpmn(wf) {
    assertWorkflowIds(wf);
    return wf.kind === "imperative" ? imperativeToBpmn(wf) : declarativeToBpmn(wf);
}
export class WorkflowError extends Error {
    status;
    body;
    constructor(message, status, body) {
        super(message);
        this.status = status;
        this.body = body;
        this.name = "WorkflowError";
    }
}
export class WorkflowClient {
    baseUrl;
    fetchImpl;
    constructor(opts) {
        if (!opts?.baseUrl)
            throw new Error("WorkflowClient needs a baseUrl");
        this.baseUrl = opts.baseUrl.replace(/\/+$/, "");
        const f = opts.fetch ?? globalThis.fetch;
        if (!f)
            throw new Error("no fetch available; pass options.fetch (Node < 18)");
        this.fetchImpl = f;
    }
    async json(path, init, what) {
        const res = await this.send(path, init, what);
        return (await res.json());
    }
    /** Perform a request, throwing WorkflowError on transport error or !ok, and
     *  return the raw Response (for endpoints with an empty/no-content body). */
    async send(path, init, what) {
        let res;
        try {
            res = await this.fetchImpl(`${this.baseUrl}${path}`, init);
        }
        catch (e) {
            throw new WorkflowError(`${what} transport error: ${e.message}`);
        }
        if (!res.ok) {
            const body = await res.text().catch(() => "");
            throw new WorkflowError(`${what} failed: ${res.status}`, res.status, body);
        }
        return res;
    }
    /** Deploy a workflow's derived BPMN model. */
    async deploy(wf) {
        const xml = toBpmn(wf);
        const form = new FormData();
        form.append("resources", new Blob([xml], { type: "text/xml" }), `${wf.id}.bpmn`);
        return this.json(`/v2/deployments`, { method: "POST", body: form }, "deploy");
    }
    /**
     * Start a workflow instance. For imperative workflows the engine variables are
     * seeded with `{ input, journal: {}, wfDone: false }` (the replay state); for
     * declarative flows `input` becomes the instance variables directly.
     */
    async start(wf, input = {}) {
        const variables = wf.kind === "imperative" ? { input, journal: {}, wfDone: false } : input;
        return this.json(`/v2/process-instances`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ processDefinitionId: wf.id, variables }),
        }, "start");
    }
    /** Correlate a signal to a parked declarative `signal` step. */
    async signal(flow, signalName, correlationKey, variables = {}) {
        return this.json(`/v2/messages/correlation`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ name: messageName(flow.id, signalName), correlationKey, variables }),
        }, `signal "${signalName}"`);
    }
    /** Fetch an instance (used by demos/tests to observe completion). */
    async getInstance(processInstanceKey) {
        try {
            const res = await this.fetchImpl(`${this.baseUrl}/v2/process-instances/${processInstanceKey}`);
            if (!res.ok)
                return null;
            return (await res.json());
        }
        catch {
            return null;
        }
    }
    // --- low-level job protocol (used by the Worker runtime) -------------------
    async activateJobs(type, opts) {
        const body = {
            type,
            worker: opts.worker,
            maxJobsToActivate: opts.maxJobsToActivate ?? 1,
            timeout: opts.timeout ?? 30000,
            requestTimeout: opts.requestTimeout ?? 15000,
        };
        const res = await this.json(`/v2/jobs/activation`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) }, `activate ${type}`);
        return res.jobs ?? [];
    }
    async completeJob(jobKey, variables = {}) {
        await this.send(`/v2/jobs/${jobKey}/completion`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ variables }) }, "complete job");
    }
    async failJob(jobKey, errorMessage, retries = 0) {
        await this.send(`/v2/jobs/${jobKey}/failure`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ errorMessage, retries }) }, "fail job");
    }
}
