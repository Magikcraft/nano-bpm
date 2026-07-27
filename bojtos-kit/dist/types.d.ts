/** One active element token within an instance. */
export interface ActiveEl {
    key: string;
    elementId: string;
}
/** A process instance's live state. */
export interface InstanceDto {
    key: string;
    processId: string;
    state: string;
    completed: boolean;
    activeElements: ActiveEl[];
    variables: Record<string, unknown>;
}
/** A job waiting for a worker. */
export interface JobDto {
    key: string;
    instanceKey: string;
    elementId: string;
    jobType: string;
    state: string;
    retries: number;
}
/**
 * A job locked to a worker by {@link BojtosSession.activateJobs}, ready to hand
 * to a {@link JobHandler}. Carries the instance's current `variables` so a
 * handler can compute its output from the live payload. `key` is what
 * `completeJob` / `failJob` take.
 */
export interface ActivatedJob {
    key: string;
    type: string;
    instanceKey: string;
    elementId: string;
    retries: number;
    variables: Record<string, unknown>;
}
/** An incident raised on an element. */
export interface IncidentDto {
    key: string;
    instanceKey: string;
    elementId: string;
    kind: string;
    reason: string;
}
/** A pending timer. */
export interface TimerDto {
    key: string;
    instanceKey: string;
    elementId: string;
    dueAt: number;
    dueInMs: number;
}
/**
 * The full simulation state returned by every engine command. `activeElementIds`
 * / `incidentElementIds` drive the token/incident highlight (the visual
 * contract, ADR 0043 §4); `instances[].variables` is the live payload.
 */
export interface Snapshot {
    now: number;
    eventCount: number;
    /** Present on a `createInstance` snapshot: the new instance key. */
    created?: string;
    totalInstances: number;
    completedInstances: number;
    instances: InstanceDto[];
    jobs: JobDto[];
    incidents: IncidentDto[];
    timers: TimerDto[];
    activeElementIds: string[];
    incidentElementIds: string[];
}
/**
 * A flattened wasm engine event: `{ seq, now, type, ...snake_case fields }`.
 * Produced by `TestEngine.events()` and folded into a trace view by consumers.
 */
export interface WasmEvent {
    seq: number;
    now: number;
    type: string;
    [k: string]: unknown;
}
