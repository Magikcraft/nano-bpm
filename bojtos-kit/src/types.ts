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

/** A user task parked on a `userTask` element, awaiting a human. */
export interface UserTaskDto {
  key: string;
  instanceKey: string;
  elementInstanceKey: string;
  elementId: string;
  /** `Created` (waiting), `Completed`, or `Canceled`. */
  state: string;
  assignee?: string;
  candidateGroups: string[];
  candidateUsers: string[];
  dueDate?: string;
  followUpDate?: string;
  priority: number;
}

/** An open message subscription (a waiting message catch/boundary event). */
export interface MessageSubscriptionDto {
  key: string;
  instanceKey: string;
  elementId: string;
  messageName: string;
  correlationKey: string;
  /** What the subscription guards (intermediate/boundary, interrupting or not). */
  kind: string;
}

/** An open signal subscription (a waiting signal catch/boundary event). */
export interface SignalSubscriptionDto {
  key: string;
  instanceKey: string;
  elementId: string;
  signalName: string;
  kind: string;
}

/**
 * Per-element token statistics for diagram overlays: `active` live tokens,
 * cumulative `completed` element instances, and current `incidents`.
 */
export interface ElementStatDto {
  elementId: string;
  active: number;
  completed: number;
  incidents: number;
}

/** A traversed connection, as source/target element ids. */
export interface SequenceFlowDto {
  from: string;
  to: string;
}

/** An evaluated decision instance (from a `businessRuleTask` / DMN). */
export interface DecisionInstanceDto {
  instanceKey: string;
  elementId: string;
  decisionKey: string;
  decisionId: string;
  output: unknown;
  evaluatedAt: number;
}

/**
 * One activation instruction for {@link BojtosSession.modify}: place a new token
 * at `elementId`, first merging `variables` into the instance's root scope.
 * Mirrors Zeebe's process-instance-modification activate instruction (the token
 * is activated in the process root scope).
 */
export interface ActivateInstruction {
  elementId: string;
  variables?: Record<string, unknown>;
}

/**
 * One tool activation an agent asks for inside an ad-hoc sub-process (Camunda
 * agentic `activateElements[]`): activate the inner element `elementId`, seeding
 * `variables` into its local scope. Distinct from {@link ActivateInstruction}
 * (process-instance modification) — this activates a *child* of the ad-hoc
 * container, not a token in the process root.
 */
export interface AgentActivation {
  elementId: string;
  variables?: Record<string, unknown>;
}

/**
 * The result an agent returns when completing an ad-hoc sub-process's agent job
 * (Camunda's agentic `JobResult`): the tools to run this turn
 * (`activateElements`), whether the container's `<completionCondition>` is now
 * satisfied (`completionConditionFulfilled`), and whether to cancel any
 * still-running tools (`cancelRemainingInstances`). `variables` merges into the
 * instance on completion exactly like a plain job result (e.g. the agent's final
 * decision). All fields optional: an empty result completes the container this
 * turn with nothing activated.
 */
export interface AgentResult {
  activateElements?: AgentActivation[];
  completionConditionFulfilled?: boolean;
  cancelRemainingInstances?: boolean;
  variables?: Record<string, unknown>;
}

/**
 * The full simulation state returned by every engine command. `activeElementIds`
 * / `incidentElementIds` drive the token/incident highlight (the visual
 * contract, ADR 0043 §4); `instances[].variables` is the live payload.
 * `userTasks`, `messageSubscriptions`, `signalSubscriptions`, `elementStats`,
 * `takenSequenceFlows` and `decisionInstances` back a Web-Modeler-Play-style UI
 * (task panels, correlation/broadcast, overlays, DMN results).
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
  userTasks: UserTaskDto[];
  messageSubscriptions: MessageSubscriptionDto[];
  signalSubscriptions: SignalSubscriptionDto[];
  elementStats: ElementStatDto[];
  takenSequenceFlows: SequenceFlowDto[];
  decisionInstances: DecisionInstanceDto[];
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
