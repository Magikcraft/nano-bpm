import type { BojtosSession } from "./session.js";
import type { ActivatedJob, Snapshot } from "./types.js";

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

/**
 * Run one activate-and-handle pass: activate every registered job type's
 * currently-`Created` jobs *first* (a snapshot of the token frontier), then hand
 * each to its handler (complete on return, fail on throw). Jobs a handler
 * unblocks downstream are deliberately *not* chased within the same round — they
 * belong to the next frontier — so one round advances every live token by
 * exactly one step. That makes this the animatable unit: drive it on a timer to
 * watch the token(s) hop task-to-task. {@link dispatchWorkers} loops it to
 * quiescence.
 */
export async function dispatchRound(
  session: BojtosSession,
  workers: Record<string, JobHandler>,
  opts: DispatchOptions = {},
): Promise<RoundResult> {
  const maxJobs = opts.maxJobsPerActivation ?? 10;
  const timeout = opts.lockTimeoutMs ?? 30_000;
  const worker = opts.worker ?? "bojtos";
  // Activation pass: lock the whole current frontier before running any handler,
  // so a job a handler unblocks isn't also picked up this round (which would
  // cascade the entire chain in a single "step").
  const batch: { handler: JobHandler; job: ActivatedJob }[] = [];
  for (const [jobType, handler] of Object.entries(workers)) {
    for (const job of session.activateJobs(jobType, maxJobs, timeout, worker)) {
      batch.push({ handler, job });
    }
  }
  // Handle pass.
  for (const { handler, job } of batch) {
    await runOne(session, handler, job);
  }
  return { snapshot: session.snapshot(), handled: batch.length };
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
