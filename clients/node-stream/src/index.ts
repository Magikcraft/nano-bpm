/**
 * `@nanobpmn/sdk` — a streaming SDK for the nanobpmn Falcon WebSocket,
 * layered on top of `@camunda8/orchestration-cluster-api`.
 *
 * Two entry points:
 * - {@link FalconClient} — a low-level client for the credit-coordinated
 *   Falcon protocol (process creation + the full job lifecycle on one socket).
 * - {@link createStreamingJobWorker} — a job worker that prefers the stream and
 *   falls back to Camunda REST polling, auto-detecting the backend.
 */

export {
  CommandError,
  FalconClient,
  type FalconClientEvents,
  type FalconClientOptions,
  falconUrl,
  ConnectionClosedError,
  SubmissionTimeoutError,
  type AwaitOptions,
  type CreateInstanceRequest,
  type CreateInstanceResult,
} from './falconClient.js';

export { detectNanobpm, type DetectOptions } from './detect.js';

export {
  type ActivatedJob,
  createStreamingJobWorker,
  type FallbackCamundaClient,
  JobActionReceipt,
  type JobWorkerHandle,
  type StreamingJobWorkerOptions,
  StreamingJobWorker,
  type StreamJob,
  type StreamJobHandler,
} from './streamingJobWorker.js';

export type {
  ClientFrame,
  CommandResultFrame,
  Corr,
  InstanceCompletedFrame,
  JobFrame,
  Key,
  PressureFrame,
  ServerFrame,
  WelcomeFrame,
} from './frames.js';
export { encodeClientFrame, parseServerFrame } from './frames.js';
