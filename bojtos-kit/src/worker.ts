import type { BojtosSession } from "./session.js";
import type { ActivatedJob, AgentResult, Snapshot } from "./types.js";

/**
 * The variables a handler merges into its instance on completion. Return an
 * object to merge it, or `void`/`undefined` to complete with no new variables.
 * To fail a job instead, throw — a plain `Error` fails it with `retries - 1`
 * (an incident once retries reach 0); throw a {@link JobFailure} to set the
 * remaining retries explicitly.
 */
export type JobResult = Record<string, unknown>;

/**
 * A worker for one job type: given an {@link ActivatedJob} (carrying the
 * instance's current variables), compute the output variables to merge on
 * completion. May be async. Throw to fail the job.
 */
export type JobHandler = (
  job: ActivatedJob,
) => JobResult | void | Promise<JobResult | void>;

/**
 * A handler for an ad-hoc sub-process's **agent** job (the container's
 * JOB_WORKER job). Given the activated container job (carrying the instance's
 * current variables — e.g. accumulated tool outputs), return the
 * {@link AgentResult} for this turn: which inner tools to activate, whether the
 * agent is done, and any variables to merge. Called once per agent turn; the
 * engine re-emits the agent job after the activated tools drain, so a stateful
 * closure can drive a multi-turn agent (activate tools → read results →
 * decide → complete). May be async. Throw to fail the container job.
 */
export type AgentHandler = (
  job: ActivatedJob,
) => AgentResult | Promise<AgentResult>;

/**
 * Throw from a {@link JobHandler} to fail a job with an explicit remaining
 * `retries` count (default is `job.retries - 1`). With `retries: 0` the engine
 * raises an incident, which surfaces in the snapshot's `incidentElementIds` —
 * handy for demoing the failure path deterministically.
 */
export class JobFailure extends Error {
  readonly retries?: number;
  constructor(message: string, opts?: { retries?: number }) {
    super(message);
    this.name = "JobFailure";
    this.retries = opts?.retries;
  }
}

/** Tuning for {@link dispatchWorkers}. */
export interface DispatchOptions {
  /** Max jobs to activate per job type per round (default 10). */
  maxJobsPerActivation?: number;
  /** Lock timeout handed to `activateJobs`, in ms (default 30_000). */
  lockTimeoutMs?: number;
  /** Worker name jobs are locked to (default `"bojtos"`). */
  worker?: string;
  /**
   * Safety cap on drain rounds (default 1000). A handler that keeps creating
   * work (e.g. an unbounded loop in the model) would otherwise spin forever;
   * exceeding the cap throws instead.
   */
  maxRounds?: number;
  /**
   * Handlers for ad-hoc sub-process **agent** job types (Camunda agentic
   * `aiagent-job-worker`), keyed by the container's `zeebe:taskDefinition type`.
   * Dispatched like {@link JobHandler}s but completed via
   * {@link BojtosSession.completeAgentJob}, so their returned
   * {@link AgentResult} drives the tools to activate this turn. The engine
   * re-emits the agent job across turns, so the standard drain loop advances the
   * whole agent conversation to quiescence.
   */
  agents?: Record<string, AgentHandler>;
}

/** What one {@link dispatchRound} pass did. */
export interface RoundResult {
  /** The snapshot after this pass. */
  snapshot: Snapshot;
  /** How many jobs were completed or failed in this pass. */
  handled: number;
}

/** What {@link dispatchWorkers} did. */
export interface DispatchResult {
  /** The snapshot after the drain settled. */
  snapshot: Snapshot;
  /** How many jobs were completed or failed. */
  handled: number;
  /** How many activate rounds ran (including the final quiescent one). */
  rounds: number;
}

async function runOne(
  session: BojtosSession,
  handler: JobHandler,
  job: ActivatedJob,
): Promise<void> {
  let payload: string;
  try {
    // Only the handler and the serialization of its result are treated as a
    // job failure: a handler that throws (or returns something unserializable)
    // is the demo's own logic failing, so we translate it into `failJob`.
    const out = await handler(job);
    payload = JSON.stringify(out ?? {});
  } catch (e) {
    const retries =
      e instanceof JobFailure && e.retries !== undefined
        ? e.retries
        : Math.max(0, job.retries - 1);
    const message = e instanceof Error ? e.message : String(e);
    session.failJob(job.key, retries, message);
    return;
  }
  // An engine command failure (invalid JSON the engine rejects, ABI mismatch,
  // internal engine error) is a real problem, not a handler failure — masking
  // it as `failJob` would hide the bug and mutate engine state incorrectly, so
  // we let it bubble to the caller.
  session.completeJob(job.key, payload);
}

async function runOneAgent(
  session: BojtosSession,
  handler: AgentHandler,
  job: ActivatedJob,
): Promise<void> {
  let result: AgentResult;
  try {
    // As with a plain job, only the handler is treated as demo logic: a throw
    // (or a result the engine can't serialize) fails the container job rather
    // than bubbling up as an engine/ABI error.
    result = await handler(job);
    // Mirror runOne: a result the engine can't serialize is the demo's own
    // logic failing, not an engine/ABI error. session.completeAgentJob
    // stringifies the result internally (outside this try), so probe-serialize
    // it here to route a serialization failure through failJob instead of
    // letting it bubble out of the dispatch loop.
    JSON.stringify(result);
  } catch (e) {
    const retries =
      e instanceof JobFailure && e.retries !== undefined
        ? e.retries
        : Math.max(0, job.retries - 1);
    const message = e instanceof Error ? e.message : String(e);
    session.failJob(job.key, retries, message);
    return;
  }
  session.completeAgentJob(job.key, result);
}

/**
 * Run one activate-and-handle pass: activate every registered job type's
 * currently-`Created` jobs *first* (a snapshot of the token frontier), then hand
 * each to its handler (complete on return, fail on throw). Jobs a handler
 * unblocks downstream are deliberately *not* chased within the same round — they
 * belong to the next frontier — so one round advances every live token by
 * exactly one step. That makes this the animatable unit: drive it on a timer to
 * watch the token(s) hop task-to-task. {@link dispatchWorkers} loops it to
 * quiescence.
 *
 * Ad-hoc **agent** job types registered via `opts.agents` are activated and
 * completed in the same frontier-snapshot pass, but through
 * {@link BojtosSession.completeAgentJob} so their {@link AgentResult} activates
 * the chosen tools. A tool a turn activates joins the *next* frontier, and the
 * engine re-emits the agent job after those tools drain, so the agent's whole
 * multi-turn conversation animates one step per round like any other token.
 */
export async function dispatchRound(
  session: BojtosSession,
  workers: Record<string, JobHandler>,
  opts: DispatchOptions = {},
): Promise<RoundResult> {
  const maxJobs = opts.maxJobsPerActivation ?? 10;
  const timeout = opts.lockTimeoutMs ?? 30_000;
  const worker = opts.worker ?? "bojtos";
  const agents = opts.agents ?? {};
  // A job type registered as both a worker and an agent is ambiguous: the
  // worker pass below would activate and plain-complete it first, so its
  // agentic `activateElements` could never be sent. Reject up front rather than
  // silently no-op the tool activation.
  for (const jobType of Object.keys(agents)) {
    if (jobType in workers) {
      throw new Error(
        `dispatchRound: job type "${jobType}" is registered as both a worker and an agent — register it as exactly one`,
      );
    }
  }
  // Activation pass: lock the whole current frontier before running any handler,
  // so a job a handler unblocks isn't also picked up this round (which would
  // cascade the entire chain in a single "step").
  const jobBatch: { handler: JobHandler; job: ActivatedJob }[] = [];
  for (const [jobType, handler] of Object.entries(workers)) {
    for (const job of session.activateJobs(jobType, maxJobs, timeout, worker)) {
      jobBatch.push({ handler, job });
    }
  }
  const agentBatch: { handler: AgentHandler; job: ActivatedJob }[] = [];
  for (const [jobType, handler] of Object.entries(agents)) {
    for (const job of session.activateJobs(jobType, maxJobs, timeout, worker)) {
      agentBatch.push({ handler, job });
    }
  }
  // Handle pass.
  for (const { handler, job } of jobBatch) {
    await runOne(session, handler, job);
  }
  for (const { handler, job } of agentBatch) {
    await runOneAgent(session, handler, job);
  }
  return {
    snapshot: session.snapshot(),
    handled: jobBatch.length + agentBatch.length,
  };
}

/**
 * Drive an in-browser worker loop over a {@link BojtosSession}: repeatedly
 * {@link dispatchRound} until a round handles nothing, so a whole process runs
 * to quiescence in one call. Job types with no registered handler are simply
 * left waiting.
 *
 * This is the dispatch half of the Bojtos runtime (ADR 0043 §8 step 3) — the
 * "activate → JS handler → complete/fail" loop that makes the token move and the
 * variable payload mutate as workers run.
 */
export async function dispatchWorkers(
  session: BojtosSession,
  workers: Record<string, JobHandler>,
  opts: DispatchOptions = {},
): Promise<DispatchResult> {
  const maxRounds = opts.maxRounds ?? 1000;
  let handled = 0;
  let rounds = 0;
  for (;;) {
    if (rounds >= maxRounds) {
      throw new Error(
        `dispatchWorkers exceeded maxRounds (${maxRounds}) — a handler may be creating work without end`,
      );
    }
    rounds++;
    const round = await dispatchRound(session, workers, opts);
    handled += round.handled;
    if (round.handled === 0) {
      return { snapshot: round.snapshot, handled, rounds };
    }
  }
}
