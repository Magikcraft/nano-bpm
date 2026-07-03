import { EventEmitter } from 'node:events';
import WebSocket from 'ws';

import {
  type ClientFrame,
  type CommandResultFrame,
  type InstanceCompletedFrame,
  type JobFrame,
  type PressureFrame,
  type ServerFrame,
  type WelcomeFrame,
  encodeClientFrame,
  parseServerFrame,
} from './frames.js';

/** Options for {@link FalconClient}. */
export interface FalconClientOptions {
  /**
   * Gateway base URL. Either an `http(s)://host:port` REST base (the `/v2`
   * suffix is stripped if present) or a `ws(s)://` URL. The client connects to
   * `<base>/falcon`.
   */
  baseUrl: string;
  /** Worker name recorded on the connection (passed as the `worker` query param). */
  worker?: string;
  /** Extra headers for the WebSocket upgrade (e.g. `Authorization`). */
  headers?: Record<string, string>;
  /** Reconnect automatically on unexpected close. Default `true`. */
  reconnect?: boolean;
  /** Base reconnect backoff in ms (doubles up to `maxReconnectDelayMs`). Default `250`. */
  reconnectDelayMs?: number;
  /** Maximum reconnect backoff in ms. Default `10_000`. */
  maxReconnectDelayMs?: number;
  /** Override the connect timeout in ms. Default `10_000`. */
  connectTimeoutMs?: number;
  /**
   * Default maximum time in ms to wait for a server submission credit on
   * `createInstance`/`createInstanceAndAwait` before rejecting with
   * {@link SubmissionTimeoutError}. Per-request `submitTimeoutMs` overrides this.
   * Omit (default `undefined`) to wait indefinitely for intake capacity.
   */
  submitTimeoutMs?: number;
}

/** A command rejected by the server (non-2xx `commandResult`). */
export class CommandError extends Error {
  constructor(
    readonly status: number,
    readonly body: unknown,
  ) {
    super(`command failed with status ${status}`);
    this.name = 'CommandError';
  }
}

/** Thrown when an in-flight command's socket closes before its result arrives. */
export class ConnectionClosedError extends Error {
  constructor(message = 'Falcon protocol connection closed') {
    super(message);
    this.name = 'ConnectionClosedError';
  }
}

/**
 * Thrown by `createInstance`/`createInstanceAndAwait` when no server submission
 * credit is granted within the requested `submitTimeoutMs` window. On the command
 * stream, admission backpressure is expressed by the server *withholding
 * submission credits* (no `503`, no retry) — so a create call otherwise waits
 * indefinitely for intake capacity. This turns that stall into a typed rejection.
 * Treat it as "the server is backpressured" and back off; do NOT tight-loop
 * retry, which would defeat the credit window's purpose.
 */
export class SubmissionTimeoutError extends Error {
  constructor(readonly timeoutMs: number) {
    super(
      `create submission stalled: no server submission credit within ${timeoutMs}ms (server is applying admission backpressure)`,
    );
    this.name = 'SubmissionTimeoutError';
  }
}

interface PendingCommand {
  resolve: (body: unknown) => void;
  reject: (err: Error) => void;
}

interface PendingAwait {
  resolve: (frame: InstanceCompletedFrame) => void;
  reject: (err: Error) => void;
}

/** A live job subscription, retained so it can be replayed after a reconnect. */
interface JobSubscription {
  jobType: string;
  jobCredits: number;
  worker?: string | null;
  timeout?: number | null;
  fetchVariable?: string[] | null;
}

/** Result of a `createInstance` (the immediate ack). */
export interface CreateInstanceResult {
  processInstanceKey: string;
  /** The full ack body as returned by the gateway. */
  body: Record<string, unknown>;
}

export interface CreateInstanceRequest {
  processDefinitionId?: string;
  processDefinitionKey?: string;
  variables?: Record<string, unknown>;
  /**
   * Maximum time in ms to wait for a server submission credit before rejecting
   * with {@link SubmissionTimeoutError}. Overrides the client-wide
   * `submitTimeoutMs`. Omit (or use the default `undefined`) to wait
   * indefinitely — the historical behaviour, where the create stalls until the
   * server replenishes credits. A negative value is treated as "wait forever".
   */
  submitTimeoutMs?: number;
}

export interface AwaitOptions {
  fetchVariables?: string[];
  requestTimeout?: number;
}

/**
 * Typed events emitted by {@link FalconClient}.
 *
 * - `job`     — a pushed {@link JobFrame} (one job-delivery credit consumed).
 * - `pressure`— a coarse {@link PressureFrame} fleet backpressure signal.
 * - `welcome` — the initial {@link WelcomeFrame} for the (re)connection.
 * - `reconnect` — emitted after a successful reconnect (subscriptions replayed).
 * - `close`   — the socket closed (with code/reason).
 * - `error`   — a transport or protocol error.
 */
export interface FalconClientEvents {
  job: [JobFrame];
  pressure: [PressureFrame];
  welcome: [WelcomeFrame];
  reconnect: [];
  close: [{ code: number; reason: string }];
  error: [Error];
}

/**
 * A client for the nanobpmn Falcon WebSocket: one persistent,
 * credit-coordinated socket multiplexing process creation and the full job
 * lifecycle. Mirrors `server/src/falcon.rs`.
 *
 * The client gates `createInstance` on the server's **submission-credit window**
 * (backpressure without `503`/retry): when credits are exhausted, calls queue
 * until the server replenishes them. Job completions are unmetered and never
 * queue.
 */
export class FalconClient extends EventEmitter {
  private readonly url: string;
  private readonly opts: Required<
    Pick<FalconClientOptions, 'reconnect' | 'reconnectDelayMs' | 'maxReconnectDelayMs' | 'connectTimeoutMs'>
  > &
    FalconClientOptions;

  private ws?: WebSocket;
  private corrSeq = 0;
  private readonly pendingCommands = new Map<number, PendingCommand>();
  private readonly pendingAwaits = new Map<number, PendingAwait>();
  private readonly subscriptions = new Map<string, JobSubscription>();

  private submissionCredits = 0;
  private readonly submissionWaiters: Array<() => void> = [];
  private heartbeatTimer?: ReturnType<typeof setInterval>;
  private reconnectAttempts = 0;
  private closedByUser = false;
  private readyPromise?: Promise<void>;

  constructor(options: FalconClientOptions) {
    super();
    this.opts = {
      reconnect: options.reconnect ?? true,
      reconnectDelayMs: options.reconnectDelayMs ?? 250,
      maxReconnectDelayMs: options.maxReconnectDelayMs ?? 10_000,
      connectTimeoutMs: options.connectTimeoutMs ?? 10_000,
      ...options,
    };
    this.url = falconUrl(options.baseUrl, options.worker);
  }

  // ----- typed event helpers (override EventEmitter for inference) -----------

  override on<E extends keyof FalconClientEvents>(
    event: E,
    listener: (...args: FalconClientEvents[E]) => void,
  ): this {
    return super.on(event, listener as (...args: unknown[]) => void);
  }

  override once<E extends keyof FalconClientEvents>(
    event: E,
    listener: (...args: FalconClientEvents[E]) => void,
  ): this {
    return super.once(event, listener as (...args: unknown[]) => void);
  }

  override emit<E extends keyof FalconClientEvents>(
    event: E,
    ...args: FalconClientEvents[E]
  ): boolean {
    return super.emit(event, ...args);
  }

  /** Opens the socket and resolves once the server `welcome` arrives. */
  connect(): Promise<void> {
    if (this.readyPromise) return this.readyPromise;
    this.closedByUser = false;
    this.readyPromise = this.openSocket();
    return this.readyPromise;
  }

  private openSocket(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const ws = new WebSocket(this.url, { headers: this.opts.headers });
      this.ws = ws;
      let settled = false;

      const timeout = setTimeout(() => {
        if (!settled) {
          settled = true;
          ws.terminate();
          reject(new Error(`Falcon connect timed out after ${this.opts.connectTimeoutMs}ms`));
        }
      }, this.opts.connectTimeoutMs);

      ws.on('message', (data: WebSocket.RawData) => {
        let frame: ServerFrame;
        try {
          frame = parseServerFrame(data.toString());
        } catch (err) {
          this.emit('error', err as Error);
          return;
        }
        if (frame.type === 'welcome' && !settled) {
          settled = true;
          clearTimeout(timeout);
          this.reconnectAttempts = 0;
          resolve();
        }
        this.handleFrame(frame);
      });

      ws.on('open', () => {
        // Replay subscriptions on a reconnect so job push resumes.
        for (const sub of this.subscriptions.values()) {
          this.sendRaw({
            type: 'subscribe',
            jobType: sub.jobType,
            jobCredits: sub.jobCredits,
            worker: sub.worker ?? null,
            timeout: sub.timeout ?? null,
            fetchVariable: sub.fetchVariable ?? null,
          });
        }
      });

      ws.on('error', (err: Error) => {
        this.emit('error', err);
        if (!settled) {
          settled = true;
          clearTimeout(timeout);
          reject(err);
        }
      });

      ws.on('close', (code: number, reasonBuf: Buffer) => {
        clearTimeout(timeout);
        this.stopHeartbeat();
        const reason = reasonBuf.toString();
        this.emit('close', { code, reason });
        this.failAllPending(new ConnectionClosedError(`socket closed (${code})`));
        if (!this.closedByUser && this.opts.reconnect) {
          this.scheduleReconnect();
        }
      });
    });
  }

  private scheduleReconnect(): void {
    const delay = Math.min(
      this.opts.reconnectDelayMs * 2 ** this.reconnectAttempts,
      this.opts.maxReconnectDelayMs,
    );
    this.reconnectAttempts += 1;
    setTimeout(() => {
      if (this.closedByUser) return;
      this.openSocket()
        .then(() => this.emit('reconnect'))
        .catch(() => {
          // openSocket's 'close' handler reschedules; nothing to do here.
        });
    }, delay);
  }

  private handleFrame(frame: ServerFrame): void {
    switch (frame.type) {
      case 'welcome':
        this.submissionCredits = frame.submissionCredits;
        this.releaseSubmissionWaiters();
        this.startHeartbeat(frame.heartbeatMs);
        this.emit('welcome', frame);
        break;
      case 'job':
        this.emit('job', frame);
        break;
      case 'commandResult':
        this.resolveCommand(frame);
        break;
      case 'instanceCompleted': {
        const pending = this.pendingAwaits.get(frame.corr);
        if (pending) {
          this.pendingAwaits.delete(frame.corr);
          pending.resolve(frame);
        }
        break;
      }
      case 'submissionCredits':
        this.submissionCredits += frame.n;
        this.releaseSubmissionWaiters();
        break;
      case 'pressure':
        this.emit('pressure', frame);
        break;
      case 'heartbeat':
        // server liveness ping; nothing to do (client sends its own cadence).
        break;
    }
  }

  private resolveCommand(frame: CommandResultFrame): void {
    const pending = this.pendingCommands.get(frame.corr);
    if (!pending) return;
    this.pendingCommands.delete(frame.corr);
    if (frame.status >= 200 && frame.status < 300) {
      pending.resolve(frame.body);
    } else {
      // An awaited create that errored will never complete; reject that too.
      const awaiting = this.pendingAwaits.get(frame.corr);
      if (awaiting) {
        this.pendingAwaits.delete(frame.corr);
        awaiting.reject(new CommandError(frame.status, frame.body));
      }
      pending.reject(new CommandError(frame.status, frame.body));
    }
  }

  // ----- submission-credit gating -------------------------------------------

  private acquireSubmissionCredit(timeoutMs?: number): Promise<void> {
    if (this.submissionCredits > 0) {
      this.submissionCredits -= 1;
      return Promise.resolve();
    }
    return new Promise<void>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const waiter = () => {
        if (timer !== undefined) clearTimeout(timer);
        this.submissionCredits -= 1;
        resolve();
      };
      this.submissionWaiters.push(waiter);
      if (timeoutMs !== undefined && timeoutMs >= 0) {
        timer = setTimeout(() => {
          const idx = this.submissionWaiters.indexOf(waiter);
          if (idx >= 0) this.submissionWaiters.splice(idx, 1);
          reject(new SubmissionTimeoutError(timeoutMs));
        }, timeoutMs);
        timer.unref?.();
      }
    });
  }

  private resolveSubmitTimeout(req: CreateInstanceRequest): number | undefined {
    return req.submitTimeoutMs ?? this.opts.submitTimeoutMs;
  }

  private releaseSubmissionWaiters(): void {
    while (this.submissionCredits > 0 && this.submissionWaiters.length > 0) {
      const waiter = this.submissionWaiters.shift();
      waiter?.();
    }
  }

  // ----- public commands -----------------------------------------------------

  /**
   * Starts a process instance. Consumes one submission credit; if the window is
   * exhausted, the call waits until the server replenishes credits (backpressure
   * without retries), or — when `submitTimeoutMs` is set — rejects with a
   * {@link SubmissionTimeoutError} once that window elapses. Resolves with the
   * create ack (carrying the `processInstanceKey`) or rejects with a
   * {@link CommandError}.
   */
  async createInstance(req: CreateInstanceRequest): Promise<CreateInstanceResult> {
    await this.acquireSubmissionCredit(this.resolveSubmitTimeout(req));
    const corr = this.nextCorr();
    const body = await this.sendCommand(corr, {
      type: 'createInstance',
      corr,
      processDefinitionId: req.processDefinitionId ?? null,
      processDefinitionKey: req.processDefinitionKey ?? null,
      variables: req.variables ?? null,
    });
    return toCreateResult(body);
  }

  /**
   * Starts a process instance and resolves only when it reaches a terminal
   * state. Returns both the immediate ack and the terminal outcome. Uses the
   * stream's `awaitCompletion` so the socket is not held open per request.
   */
  async createInstanceAndAwait(
    req: CreateInstanceRequest,
    await_?: AwaitOptions,
  ): Promise<{ ack: CreateInstanceResult; completion: InstanceCompletedFrame }> {
    await this.acquireSubmissionCredit(this.resolveSubmitTimeout(req));
    const corr = this.nextCorr();
    const completion = this.registerAwait(corr);
    const body = await this.sendCommand(corr, {
      type: 'createInstance',
      corr,
      processDefinitionId: req.processDefinitionId ?? null,
      processDefinitionKey: req.processDefinitionKey ?? null,
      variables: req.variables ?? null,
      awaitCompletion: true,
      fetchVariables: await_?.fetchVariables ?? null,
      requestTimeout: await_?.requestTimeout ?? null,
    });
    return { ack: toCreateResult(body), completion: await completion };
  }

  /**
   * Re-subscribes to a known instance's terminal outcome (reconnect recovery).
   * Resolves immediately if the instance is already terminal, so it doubles as a
   * completion poll.
   */
  async awaitInstance(processInstanceKey: string, opts?: AwaitOptions): Promise<InstanceCompletedFrame> {
    const corr = this.nextCorr();
    const completion = this.registerAwait(corr);
    await this.sendCommand(corr, {
      type: 'awaitInstance',
      corr,
      processInstanceKey,
      fetchVariables: opts?.fetchVariables ?? null,
      requestTimeout: opts?.requestTimeout ?? null,
    });
    return completion;
  }

  /** Completes an activated job (unmetered). */
  async completeJob(jobKey: string, variables?: Record<string, unknown>): Promise<void> {
    const corr = this.nextCorr();
    await this.sendCommand(corr, { type: 'completeJob', corr, jobKey, variables: variables ?? null });
  }

  /** Fails an activated job (unmetered). */
  async failJob(jobKey: string, opts?: { retries?: number; errorMessage?: string }): Promise<void> {
    const corr = this.nextCorr();
    await this.sendCommand(corr, {
      type: 'failJob',
      corr,
      jobKey,
      retries: opts?.retries ?? null,
      errorMessage: opts?.errorMessage ?? null,
    });
  }

  /** Throws a BPMN error from an activated job (unmetered). */
  async throwError(jobKey: string, errorCode: string, errorMessage?: string): Promise<void> {
    const corr = this.nextCorr();
    await this.sendCommand(corr, {
      type: 'throwError',
      corr,
      jobKey,
      errorCode,
      errorMessage: errorMessage ?? null,
    });
  }

  /**
   * Completes an activated job without awaiting the server's durable ack
   * (pipelined fast path). The frame is journaled by the server as usual; a
   * non-2xx result or a send failure is surfaced via the `error` event rather
   * than rejecting a caller. Lets a single connection keep its whole credit
   * window in flight instead of stalling one round-trip per job.
   */
  completeJobNoWait(jobKey: string, variables?: Record<string, unknown>): void {
    const corr = this.nextCorr();
    this.sendCommandFireAndForget(corr, { type: 'completeJob', corr, jobKey, variables: variables ?? null });
  }

  /** Fails an activated job without awaiting the ack. See {@link completeJobNoWait}. */
  failJobNoWait(jobKey: string, opts?: { retries?: number; errorMessage?: string }): void {
    const corr = this.nextCorr();
    this.sendCommandFireAndForget(corr, {
      type: 'failJob',
      corr,
      jobKey,
      retries: opts?.retries ?? null,
      errorMessage: opts?.errorMessage ?? null,
    });
  }

  /** Throws a BPMN error without awaiting the ack. See {@link completeJobNoWait}. */
  throwErrorNoWait(jobKey: string, errorCode: string, errorMessage?: string): void {
    const corr = this.nextCorr();
    this.sendCommandFireAndForget(corr, {
      type: 'throwError',
      corr,
      jobKey,
      errorCode,
      errorMessage: errorMessage ?? null,
    });
  }

  /** Subscribes to job push for `jobType`, granting `jobCredits` initial demand. */
  subscribe(sub: JobSubscription): void {
    this.subscriptions.set(sub.jobType, { ...sub });
    this.sendRaw({
      type: 'subscribe',
      jobType: sub.jobType,
      jobCredits: sub.jobCredits,
      worker: sub.worker ?? null,
      timeout: sub.timeout ?? null,
      fetchVariable: sub.fetchVariable ?? null,
    });
  }

  /** Replenishes job-delivery demand for `jobType`. */
  grantJobCredits(jobType: string, n: number): void {
    if (n <= 0) return;
    this.sendRaw({ type: 'jobCredits', jobType, n });
  }

  /** Closes the socket and disables reconnect. */
  async close(): Promise<void> {
    this.closedByUser = true;
    this.stopHeartbeat();
    this.failAllPending(new ConnectionClosedError('client closed'));
    const ws = this.ws;
    if (!ws) return;
    await new Promise<void>((resolve) => {
      if (ws.readyState === WebSocket.CLOSED) return resolve();
      ws.once('close', () => resolve());
      ws.close(1000, 'client closed');
    });
  }

  // ----- internals -----------------------------------------------------------

  private nextCorr(): number {
    this.corrSeq = (this.corrSeq + 1) >>> 0;
    return this.corrSeq;
  }

  private registerAwait(corr: number): Promise<InstanceCompletedFrame> {
    return new Promise<InstanceCompletedFrame>((resolve, reject) => {
      this.pendingAwaits.set(corr, { resolve, reject });
    });
  }

  private sendCommand(corr: number, frame: ClientFrame): Promise<unknown> {
    return new Promise<unknown>((resolve, reject) => {
      this.pendingCommands.set(corr, { resolve, reject });
      try {
        this.sendRaw(frame);
      } catch (err) {
        this.pendingCommands.delete(corr);
        reject(err as Error);
      }
    });
  }

  /**
   * Sends a command without a caller awaiting it. A non-2xx `commandResult`
   * (or a connection drop while in flight) is reported on the `error` event;
   * a successful ack is silently discarded. Used by the pipelined job-action
   * fast path so completion does not gate the worker's credit window.
   */
  private sendCommandFireAndForget(corr: number, frame: ClientFrame): void {
    this.pendingCommands.set(corr, {
      resolve: () => {},
      reject: (err: unknown) =>
        this.emit('error', err instanceof Error ? err : new Error(String(err))),
    });
    try {
      this.sendRaw(frame);
    } catch (err) {
      this.pendingCommands.delete(corr);
      this.emit('error', err as Error);
    }
  }

  private sendRaw(frame: ClientFrame): void {
    const ws = this.ws;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      throw new ConnectionClosedError('socket is not open');
    }
    ws.send(encodeClientFrame(frame));
  }

  private failAllPending(err: Error): void {
    for (const pending of this.pendingCommands.values()) pending.reject(err);
    this.pendingCommands.clear();
    for (const pending of this.pendingAwaits.values()) pending.reject(err);
    this.pendingAwaits.clear();
    // Release credit waiters so callers don't hang; their sendRaw will throw.
    while (this.submissionWaiters.length > 0) this.submissionWaiters.shift()?.();
  }

  private startHeartbeat(ms: number): void {
    this.stopHeartbeat();
    if (!ms || ms <= 0) return;
    this.heartbeatTimer = setInterval(() => {
      try {
        this.sendRaw({ type: 'heartbeat' });
      } catch {
        // socket gone; close handler manages reconnect.
      }
    }, ms);
    this.heartbeatTimer.unref?.();
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
  }
}

function toCreateResult(body: unknown): CreateInstanceResult {
  const record = (body ?? {}) as Record<string, unknown>;
  const key = record.processInstanceKey;
  return {
    processInstanceKey: typeof key === 'string' ? key : String(key ?? ''),
    body: record,
  };
}

/**
 * Builds the `/falcon` WebSocket URL from a REST or WS base. Strips a
 * trailing `/v2` (the REST prefix) and maps http(s) → ws(s).
 */
export function falconUrl(baseUrl: string, worker?: string): string {
  let base = baseUrl.replace(/\/+$/, '');
  base = base.replace(/\/v2$/, '');
  if (base.startsWith('http://')) base = `ws://${base.slice('http://'.length)}`;
  else if (base.startsWith('https://')) base = `wss://${base.slice('https://'.length)}`;
  const url = new URL(`${base}/falcon`);
  if (worker) url.searchParams.set('worker', worker);
  return url.toString();
}
