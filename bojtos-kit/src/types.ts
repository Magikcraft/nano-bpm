// The shapes the in-browser engine (`@nanobpm/engine-wasm`) emits from its
// JSON string surface — `deploy` / `createInstance` / `completeJob` / `failJob`
// / `advanceTime` / `snapshot` return a `Snapshot`, and `events()` returns a
// `WasmEvent[]`. These describe the engine's public contract, so they live in
// the framework-agnostic kit and are re-exported by the React binding.

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
