// nanobpmn embedded worker SDK (Deno).
//
// This file is written verbatim into <workspace>/.nanobpm/worker-sdk.ts by the
// console worker supervisor and imported by each worker's `worker.ts`. It speaks
// the nanobpmn command-stream protocol directly over Deno's native WebSocket
// (no `ws`, no node:events) so a worker is a single self-contained Deno process.
//
// A worker file looks like:
//
//   import { defineWorker } from "@nanobpm/worker";
//   defineWorker({
//     type: "my-job",
//     maxParallelJobs: 10,
//     async handle(job) {
//       // ...do work with job.variables...
//       return { result: 42 };           // resolves -> completeJob({ result: 42 })
//       // or: await job.fail("boom");    // or job.error("CODE", "msg")
//     },
//   });
//
// The handler may either return output variables (the job is completed with
// them) or call one of job.complete/job.fail/job.error explicitly. Throwing
// fails the job. npm libraries are available via `npm:` specifiers.

export interface WorkerJob {
  readonly jobKey: string;
  readonly type: string;
  readonly processInstanceKey: string;
  readonly processDefinitionId?: string;
  readonly processDefinitionKey?: string;
  readonly elementId?: string;
  readonly retries?: number;
  readonly variables: Record<string, unknown>;
  readonly customHeaders: Record<string, unknown>;
  /** Complete the job, optionally setting output variables. */
  complete(variables?: Record<string, unknown>): void;
  /** Fail the job (optionally with remaining retries and a message). */
  fail(opts?: { retries?: number; errorMessage?: string } | string): void;
  /** Throw a BPMN error from the job. */
  error(errorCode: string, errorMessage?: string): void;
}

export interface WorkerOptions {
  /** BPMN job type to work on. */
  type: string;
  /** Handler invoked per job. Return output vars, or call a job action. */
  handle: (job: WorkerJob) => Promise<void | Record<string, unknown>> | void | Record<string, unknown>;
  /** Max jobs in flight (also the streaming credit window). Default 10. */
  maxParallelJobs?: number;
  /** Job activation lock timeout in ms. Default 60000. */
  timeoutMs?: number;
  /** Restrict fetched variables to these names (default: all). */
  fetchVariables?: string[];
  /** Gateway base URL. Defaults to env NANOBPMN_BASE_URL or http://127.0.0.1:8080. */
  baseUrl?: string;
  /** Worker name recorded on activation. Defaults to env NANOBPMN_WORKER_NAME. */
  worker?: string;
}

// Control lines the supervisor parses out of stdout. Anything else on stdout is
// treated as worker log output.
const METRIC = "@@NBPM_METRIC@@";
const STATUS = "@@NBPM_STATUS@@";

function emit(prefix: string, payload: unknown): void {
  // Write directly so it is one atomic line, independent of console.log.
  const line = prefix + JSON.stringify(payload) + "\n";
  Deno.stdout.writeSync(new TextEncoder().encode(line));
}

function commandStreamUrl(baseUrl: string, worker?: string): string {
  let base = baseUrl.replace(/\/+$/, "").replace(/\/v2$/, "");
  if (base.startsWith("http://")) base = "ws://" + base.slice("http://".length);
  else if (base.startsWith("https://")) base = "wss://" + base.slice("https://".length);
  const url = new URL(base + "/command-stream");
  if (worker) url.searchParams.set("worker", worker);
  return url.toString();
}

export function defineWorker(opts: WorkerOptions): void {
  const baseUrl = opts.baseUrl ?? Deno.env.get("NANOBPMN_BASE_URL") ?? "http://127.0.0.1:8080";
  const workerName = opts.worker ?? Deno.env.get("NANOBPMN_WORKER_NAME") ?? "embedded-worker";
  const maxParallel = opts.maxParallelJobs ?? 10;
  const timeoutMs = opts.timeoutMs ?? 60_000;
  const url = commandStreamUrl(baseUrl, workerName);

  let corr = 0;
  const nextCorr = () => (corr = (corr + 1) >>> 0);

  let completed = 0;
  let failed = 0;
  let inFlight = 0;
  let lastError: string | null = null;
  const startedAt = Date.now();
  let lastTotal = 0;
  let lastTick = startedAt;
  let connected = false;

  let heartbeat: ReturnType<typeof setInterval> | undefined;
  let metricTimer: ReturnType<typeof setInterval> | undefined;
  let ws: WebSocket;

  const acted = new Set<string>();

  function send(frame: Record<string, unknown>): void {
    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(frame));
  }

  function snapshot() {
    return {
      completed,
      failed,
      inFlight,
      lastError,
      uptimeMs: Date.now() - startedAt,
      connected,
    };
  }

  function emitMetrics(): void {
    const now = Date.now();
    const total = completed + failed;
    const dt = (now - lastTick) / 1000;
    const throughput = dt > 0 ? (total - lastTotal) / dt : 0;
    lastTotal = total;
    lastTick = now;
    emit(METRIC, { ...snapshot(), throughput: Math.round(throughput * 100) / 100 });
  }

  function enrich(raw: Record<string, unknown>): WorkerJob {
    const jobKey = String(raw.jobKey ?? raw.key ?? "");
    const markActed = () => acted.add(jobKey);
    return {
      jobKey,
      type: String(raw.type ?? opts.type),
      processInstanceKey: String(raw.processInstanceKey ?? ""),
      processDefinitionId: raw.processDefinitionId as string | undefined,
      processDefinitionKey: raw.processDefinitionKey as string | undefined,
      elementId: raw.elementId as string | undefined,
      retries: raw.retries as number | undefined,
      variables: (raw.variables as Record<string, unknown>) ?? {},
      customHeaders: (raw.customHeaders as Record<string, unknown>) ?? {},
      complete: (variables?: Record<string, unknown>) => {
        markActed();
        send({ type: "completeJob", corr: nextCorr(), jobKey, variables: variables ?? null });
        completed += 1;
      },
      fail: (o?: { retries?: number; errorMessage?: string } | string) => {
        markActed();
        const n = typeof o === "string" ? { errorMessage: o } : o ?? {};
        send({
          type: "failJob",
          corr: nextCorr(),
          jobKey,
          retries: n.retries ?? null,
          errorMessage: n.errorMessage ?? null,
        });
        failed += 1;
      },
      error: (errorCode: string, errorMessage?: string) => {
        markActed();
        send({ type: "throwError", corr: nextCorr(), jobKey, errorCode, errorMessage: errorMessage ?? null });
        completed += 1;
      },
    };
  }

  async function dispatch(raw: Record<string, unknown>): Promise<void> {
    const job = enrich(raw);
    inFlight += 1;
    try {
      const out = await opts.handle(job);
      if (!acted.has(job.jobKey)) {
        // Handler returned without acting: complete with any returned vars.
        job.complete(out && typeof out === "object" ? (out as Record<string, unknown>) : undefined);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      lastError = msg;
      if (!acted.has(job.jobKey)) job.fail({ errorMessage: msg });
    } finally {
      acted.delete(job.jobKey);
      inFlight -= 1;
      // Replenish one credit so demand stays at maxParallel.
      send({ type: "jobCredits", jobType: opts.type, n: 1 });
    }
  }

  function connect(): void {
    ws = new WebSocket(url);
    ws.onopen = () => {
      emit(STATUS, { state: "connecting", message: `socket open to ${url}` });
    };
    ws.onmessage = (ev: MessageEvent) => {
      let frame: Record<string, unknown>;
      try {
        frame = JSON.parse(typeof ev.data === "string" ? ev.data : "");
      } catch {
        return;
      }
      switch (frame.type) {
        case "welcome": {
          connected = true;
          lastError = null;
          emit(STATUS, { state: "running", message: "subscribed to " + opts.type });
          send({
            type: "subscribe",
            jobType: opts.type,
            jobCredits: maxParallel,
            worker: workerName,
            timeout: timeoutMs,
            fetchVariable: opts.fetchVariables ?? null,
          });
          const hbMs = Number(frame.heartbeatMs ?? 0);
          if (hbMs > 0) {
            clearInterval(heartbeat);
            heartbeat = setInterval(() => send({ type: "heartbeat" }), hbMs);
          }
          break;
        }
        case "job":
          void dispatch((frame.job as Record<string, unknown>) ?? {});
          break;
        case "commandResult": {
          const status = Number(frame.status ?? 0);
          // 404/409 on a fire-and-forget completion is benign (job already gone).
          if (status >= 400 && status !== 404 && status !== 409) {
            lastError = `command failed with status ${status}`;
          }
          break;
        }
        // pressure / submissionCredits / heartbeat: nothing to do for a worker.
      }
    };
    ws.onerror = () => {
      connected = false;
      lastError = "websocket error";
      emit(STATUS, { state: "error", message: "websocket error" });
    };
    ws.onclose = (ev: CloseEvent) => {
      connected = false;
      clearInterval(heartbeat);
      emit(STATUS, { state: "reconnecting", message: `socket closed (${ev.code}); retrying` });
      setTimeout(connect, 1000);
    };
  }

  metricTimer = setInterval(emitMetrics, 1000);
  emit(STATUS, { state: "starting", message: `worker '${workerName}' type '${opts.type}'` });
  connect();

  const shutdown = () => {
    clearInterval(heartbeat);
    clearInterval(metricTimer);
    try {
      ws?.close(1000, "shutdown");
    } catch { /* ignore */ }
  };
  Deno.addSignalListener("SIGTERM", () => {
    shutdown();
    Deno.exit(0);
  });
}
