// XML helpers + derived-name conventions shared by both model emitters.
export function escapeXml(s) {
    return String(s)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
}
/** Derived job type for a declarative `run` step / imperative orchestrator. */
export const jobType = (workflowId, step) => `${workflowId}:${step}`;
/** Derived message name for a declarative `signal` step. */
export const messageName = (workflowId, step) => `${workflowId}:${step}`;
/** The single orchestrator job type of an imperative workflow. */
export const orchestrateType = (workflowId) => `${workflowId}:__orchestrate`;
/** A BPMN identifier must be an NCName; validate derived ids fail fast. */
const NCNAME = /^[A-Za-z_][A-Za-z0-9_.-]*$/;
export function assertIdent(kind, value) {
    if (!NCNAME.test(value)) {
        throw new Error(`${kind} "${value}" is not a valid BPMN identifier (expected an NCName)`);
    }
}
export function assertWorkflowIds(wf) {
    assertIdent("workflow id", wf.id);
    if (wf.kind === "declarative") {
        for (const step of wf.steps)
            assertIdent("step name", step.name);
    }
}
