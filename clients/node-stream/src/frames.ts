/**
 * TypeScript mirror of the nanobpmn command-stream wire protocol.
 *
 * These types track `docs/command-stream.asyncapi.yaml` and the Rust
 * `ClientFrame` / `ServerFrame` enums in `server/src/command_stream.rs`. Every
 * frame is a JSON object carried in a WebSocket **text** frame, discriminated by
 * a camelCase `type` field.
 *
 * Wire conventions:
 * - `corr` is a client-chosen non-negative integer correlation id; the server
 *   echoes it on the matching `commandResult` and on the `instanceCompleted` of
 *   an awaited create.
 * - Keys (jobKey, processInstanceKey, ...) are 64-bit values serialized as
 *   decimal **strings**.
 */

/** A client-chosen correlation id (non-negative integer). */
export type Corr = number;

/** A 64-bit nanobpmn key, serialized as a decimal string on the wire. */
export type Key = string;

// ---------------------------------------------------------------- client → server

/** Opt into job push for a type, granting an initial credit batch. */
export interface SubscribeFrame {
  type: 'subscribe';
  jobType: string;
  /** Initial job-delivery demand for this type (default 0). */
  jobCredits?: number;
  /** Restrict fetched variables to these names (null/omitted = all). */
  fetchVariable?: string[] | null;
  /** Job activation lock timeout in milliseconds. */
  timeout?: number | null;
  /** Worker name recorded on activation. */
  worker?: string | null;
}

/** Replenish job-push demand for a type. */
export interface JobCreditsFrame {
  type: 'jobCredits';
  jobType: string;
  /** Additional job-delivery credits to grant. */
  n: number;
}

/** Start a process instance (consumes one submission credit). */
export interface CreateInstanceFrame {
  type: 'createInstance';
  corr: Corr;
  /** BPMN process id (one of id/key is required). */
  processDefinitionId?: string | null;
  /** Numeric process definition key as a string. */
  processDefinitionKey?: string | null;
  variables?: Record<string, unknown> | null;
  /**
   * If true, the server emits an `instanceCompleted` frame (correlated by
   * `corr`) when the instance reaches a terminal state, in addition to the
   * immediate `commandResult` ack.
   */
  awaitCompletion?: boolean | null;
  /** Variables to include in the `instanceCompleted` payload. */
  fetchVariables?: string[] | null;
  /** Await timeout in milliseconds. */
  requestTimeout?: number | null;
}

/** Complete an activated job (unmetered drain). */
export interface CompleteJobFrame {
  type: 'completeJob';
  corr: Corr;
  jobKey: Key;
  variables?: Record<string, unknown> | null;
}

/** Fail an activated job (unmetered drain). */
export interface FailJobFrame {
  type: 'failJob';
  corr: Corr;
  jobKey: Key;
  retries?: number | null;
  errorMessage?: string | null;
}

/** Throw a BPMN error from an activated job (unmetered drain). */
export interface ThrowErrorFrame {
  type: 'throwError';
  corr: Corr;
  jobKey: Key;
  errorCode: string;
  errorMessage?: string | null;
}

/** Re-subscribe to completion of an already-created instance (recovery / poll). */
export interface AwaitInstanceFrame {
  type: 'awaitInstance';
  corr: Corr;
  processInstanceKey: Key;
  fetchVariables?: string[] | null;
  requestTimeout?: number | null;
}

/** Liveness keep-alive from the client. */
export interface ClientHeartbeatFrame {
  type: 'heartbeat';
}

export type ClientFrame =
  | SubscribeFrame
  | JobCreditsFrame
  | CreateInstanceFrame
  | CompleteJobFrame
  | FailJobFrame
  | ThrowErrorFrame
  | AwaitInstanceFrame
  | ClientHeartbeatFrame;

// ---------------------------------------------------------------- server → client

/** Sent once on connect: initial submission window and heartbeat cadence. */
export interface WelcomeFrame {
  type: 'welcome';
  submissionCredits: number;
  heartbeatMs: number;
}

/** A pushed activated job (consumes one job-delivery credit). */
export interface JobFrame {
  type: 'job';
  /** An activated job, same shape as a REST `ActivatedJobResult`. */
  job: Record<string, unknown>;
}

/** Ack/result for a create/complete/fail/throwError/await, correlated by `corr`. */
export interface CommandResultFrame {
  type: 'commandResult';
  corr: Corr;
  /** HTTP-equivalent status code for the command (e.g. 200, 404). */
  status: number;
  /** Result payload or error body; omitted when there is none. */
  body?: unknown;
}

/** An awaited instance reached a terminal state, routed by the create's `corr`. */
export interface InstanceCompletedFrame {
  type: 'instanceCompleted';
  corr: Corr;
  processInstanceKey: Key;
  /** True if completed normally; false if terminated. */
  processCompleted: boolean;
  /** Fetched output variables (object), or empty if none requested. */
  variables: unknown;
}

/** Grants the client additional submission (create-side) capacity. */
export interface SubmissionCreditsFrame {
  type: 'submissionCredits';
  n: number;
}

/** Coarse fleet pressure signal. */
export interface PressureFrame {
  type: 'pressure';
  level: string;
  retryAfterMs?: number | null;
}

/** Liveness keep-alive from the server. */
export interface ServerHeartbeatFrame {
  type: 'heartbeat';
}

export type ServerFrame =
  | WelcomeFrame
  | JobFrame
  | CommandResultFrame
  | InstanceCompletedFrame
  | SubmissionCreditsFrame
  | PressureFrame
  | ServerHeartbeatFrame;

/** Parses a raw WebSocket text payload into a typed {@link ServerFrame}. */
export function parseServerFrame(data: string): ServerFrame {
  const frame = JSON.parse(data) as ServerFrame;
  if (typeof frame !== 'object' || frame === null || typeof (frame as ServerFrame).type !== 'string') {
    throw new Error(`malformed server frame: ${data.slice(0, 120)}`);
  }
  return frame;
}

/** Serializes a {@link ClientFrame} to a WebSocket text payload. */
export function encodeClientFrame(frame: ClientFrame): string {
  return JSON.stringify(frame);
}
