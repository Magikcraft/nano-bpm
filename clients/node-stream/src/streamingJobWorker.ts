import { CommandStreamClient } from './commandStreamClient.js';
import { detectNanobpm } from './detect.js';

/**
 * Opaque receipt returned by job action methods. Identical in value to the
 * `@camunda8/orchestration-cluster-api` sentinel, so a handler written for the
 * Camunda SDK works unchanged on the streaming transport (and vice versa).
 */
export const JobActionReceipt = 'JOB_ACTION_RECEIPT' as const;
export type JobActionReceipt = typeof JobActionReceipt;

/** Raw activated-job shape pushed over the stream (REST `ActivatedJobResult`). */
export interface ActivatedJob {
  jobKey: string;
  type: string;
  processInstanceKey: string;
  processDefinitionId?: string;
  processDefinitionKey?: string;
  processDefinitionVersion?: number;
  elementId?: string;
  elementInstanceKey?: string;
  worker?: string;
  retries?: number;
  deadline?: number;
  variables: Record<string, unknown>;
  customHeaders: Record<string, unknown>;
  tenantId?: string;
  [key: string]: unknown;
}

/**
 * An activated job enriched with action methods. Structurally compatible with
 * the Camunda SDK's `EnrichedActivatedJob` for the methods the command stream
 * supports, so the same `jobHandler` runs on either transport.
 */
export interface StreamJob extends ActivatedJob {
  /** Completes the job, optionally setting output variables. */
  complete(variables?: Record<string, unknown>): Promise<JobActionReceipt>;
  /** Fails the job (optionally with remaining retries and a message). */
  fail(opts?: { retries?: number; errorMessage?: string } | string): Promise<JobActionReceipt>;
  /** Throws a BPMN error from the job. */
  error(opts: { errorCode: string; errorMessage?: string }): Promise<JobActionReceipt>;
  /** Leaves the job untouched; its lock expires and it is re-dispatched. */
  ignore(): JobActionReceipt;
  /**
   * Not supported over the command stream (there is no cancel frame). Use the
   * REST client's process-instance cancellation instead.
   */
  cancelWorkflow(): Promise<JobActionReceipt>;
}

export type StreamJobHandler = (job: StreamJob) => Promise<JobActionReceipt> | JobActionReceipt;

/** Minimal structural view of the Camunda SDK client used for the poll fallback. */
export interface FallbackCamundaClient {
  createJobWorker(config: {
    jobType: string;
    jobHandler: (job: unknown) => Promise<JobActionReceipt> | JobActionReceipt;
    maxParallelJobs?: number;
    jobTimeoutMs?: number;
    workerName?: string;
    fetchVariables?: string[];
    autoStart?: boolean;
    [key: string]: unknown;
  }): { stop: () => unknown };
}

export interface StreamingJobWorkerOptions {
  /** Gateway base URL (REST `http(s)://…` or `ws(s)://…`). */
  baseUrl: string;
  /** BPMN job type to work on. */
  jobType: string;
  /** Handler invoked per job. Must call exactly one action (complete/fail/error/ignore). */
  jobHandler: StreamJobHandler;
  /** Max jobs in flight (also the streaming credit window). Default `10`. */
  maxParallelJobs?: number;
  /** Job activation lock timeout in ms. Default `60_000`. */
  jobTimeoutMs?: number;
  /** Worker name recorded on activation. */
  worker?: string;
  /** Restrict fetched variables to these names. */
  fetchVariables?: string[];
  /** Headers for the WebSocket upgrade (e.g. `Authorization`). */
  headers?: Record<string, string>;
  /**
   * Transport selection:
   * - `'auto'` (default): probe the gateway; stream if it is nanobpmn, else poll.
   * - `'stream'`: force the command stream (no probe).
   * - `'poll'`: force the Camunda REST polling worker.
   */
  transport?: 'auto' | 'stream' | 'poll';
  /** Detection probe timeout in ms (for `transport: 'auto'`). Default `2000`. */
  detectTimeoutMs?: number;
  /**
   * A Camunda SDK client, required for the polling fallback (`transport: 'poll'`,
   * or `'auto'` against a non-nanobpmn gateway).
   */
  camundaClient?: FallbackCamundaClient;
  /** Start working immediately. Default `true`. */
  autoStart?: boolean;
}

/** A running job worker, regardless of transport. */
export interface JobWorkerHandle {
  /** Which transport this worker resolved to. */
  readonly transport: 'stream' | 'poll';
  /** Stops the worker and releases its connection. */
  stop(): Promise<void>;
}

/**
 * Creates a job worker that prefers the nanobpmn command stream and falls back
 * to Camunda REST polling. With `transport: 'auto'` (the default) it probes the
 * gateway once: a nanobpmn server gets a streaming worker (server-pushed jobs,
 * credit flow control, no long-poll); anything else gets the Camunda SDK's
 * polling worker — so the same code path serves both backends.
 */
export async function createStreamingJobWorker(
  options: StreamingJobWorkerOptions,
): Promise<JobWorkerHandle> {
  const transport = options.transport ?? 'auto';
  const useStream =
    transport === 'stream' ||
    (transport === 'auto' &&
      (await detectNanobpm(options.baseUrl, {
        headers: options.headers,
        timeoutMs: options.detectTimeoutMs,
      })));

  if (useStream) {
    const worker = new StreamingJobWorker(options);
    if (options.autoStart ?? true) await worker.start();
    return worker;
  }

  if (!options.camundaClient) {
    throw new Error(
      `createStreamingJobWorker: transport resolved to polling but no camundaClient was provided ` +
        `(gateway at ${options.baseUrl} is not nanobpmn). Pass options.camundaClient, or set transport: 'stream'.`,
    );
  }
  return new PollingJobWorker(
    options.camundaClient.createJobWorker({
      jobType: options.jobType,
      jobHandler: options.jobHandler as (job: unknown) => Promise<JobActionReceipt> | JobActionReceipt,
      maxParallelJobs: options.maxParallelJobs,
      jobTimeoutMs: options.jobTimeoutMs,
      workerName: options.worker,
      fetchVariables: options.fetchVariables,
      autoStart: options.autoStart ?? true,
    }),
  );
}

/** Wraps the Camunda SDK's polling worker behind {@link JobWorkerHandle}. */
class PollingJobWorker implements JobWorkerHandle {
  readonly transport = 'poll' as const;
  constructor(private readonly inner: { stop: () => unknown }) {}
  async stop(): Promise<void> {
    await this.inner.stop();
  }
}

/** A job worker backed by the nanobpmn command stream. */
export class StreamingJobWorker implements JobWorkerHandle {
  readonly transport = 'stream' as const;
  private readonly client: CommandStreamClient;
  private readonly jobType: string;
  private readonly maxParallelJobs: number;
  private stopped = false;
  private started = false;

  constructor(private readonly options: StreamingJobWorkerOptions) {
    this.jobType = options.jobType;
    this.maxParallelJobs = options.maxParallelJobs ?? 10;
    this.client = new CommandStreamClient({
      baseUrl: options.baseUrl,
      worker: options.worker,
      headers: options.headers,
    });
    this.client.on('job', (frame) => {
      void this.dispatch(frame.job as ActivatedJob);
    });
  }

  /** Connects and subscribes, beginning job push. Idempotent. */
  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    await this.client.connect();
    this.client.subscribe({
      jobType: this.jobType,
      jobCredits: this.maxParallelJobs,
      worker: this.options.worker ?? null,
      timeout: this.options.jobTimeoutMs ?? null,
      fetchVariable: this.options.fetchVariables ?? null,
    });
  }

  async stop(): Promise<void> {
    this.stopped = true;
    await this.client.close();
  }

  private async dispatch(raw: ActivatedJob): Promise<void> {
    const job = this.enrich(raw);
    try {
      await this.options.jobHandler(job);
      if (!this.acted.has(raw.jobKey)) {
        // Handler took no action: leave the job for lock expiry rather than
        // silently stranding it. Equivalent to ignore().
      }
    } catch (err) {
      if (!this.acted.has(raw.jobKey)) {
        await this.client
          .failJob(raw.jobKey, { errorMessage: errorMessageOf(err) })
          .catch(() => undefined);
      }
    } finally {
      this.acted.delete(raw.jobKey);
      if (!this.stopped) {
        // Replenish one credit so demand stays at maxParallelJobs.
        this.client.grantJobCredits(this.jobType, 1);
      }
    }
  }

  private readonly acted = new Set<string>();

  private enrich(raw: ActivatedJob): StreamJob {
    const markActed = () => this.acted.add(raw.jobKey);
    return {
      ...raw,
      complete: async (variables?: Record<string, unknown>) => {
        markActed();
        await this.client.completeJob(raw.jobKey, variables);
        return JobActionReceipt;
      },
      fail: async (opts?: { retries?: number; errorMessage?: string } | string) => {
        markActed();
        const normalized = typeof opts === 'string' ? { errorMessage: opts } : opts;
        await this.client.failJob(raw.jobKey, normalized);
        return JobActionReceipt;
      },
      error: async (opts: { errorCode: string; errorMessage?: string }) => {
        markActed();
        await this.client.throwError(raw.jobKey, opts.errorCode, opts.errorMessage);
        return JobActionReceipt;
      },
      ignore: () => {
        markActed();
        return JobActionReceipt;
      },
      cancelWorkflow: () => {
        markActed();
        return Promise.reject(
          new Error(
            'cancelWorkflow() is not supported over the command stream; cancel the process instance via the REST client.',
          ),
        );
      },
    };
  }
}

function errorMessageOf(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}
