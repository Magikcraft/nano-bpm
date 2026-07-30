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
export type JobHandler = (job: ActivatedJob) => JobResult | void | Promise<JobResult | void>;
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
export type AgentHandler = (job: ActivatedJob) => AgentResult | Promise<AgentResult>;
/**
 * Throw from a {@link JobHandler} to fail a job with an explicit remaining
 * `retries` count (default is `job.retries - 1`). With `retries: 0` the engine
 * raises an incident, which surfaces in the snapshot's `incidentElementIds` —
 * handy for demoing the failure path deterministically.
 */
export declare class JobFailure extends Error {
    readonly retries?: number;
    constructor(message: string, opts?: {
        retries?: number;
    });
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
export declare function dispatchRound(session: BojtosSession, workers: Record<string, JobHandler>, opts?: DispatchOptions): Promise<RoundResult>;
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
export declare function dispatchWorkers(session: BojtosSession, workers: Record<string, JobHandler>, opts?: DispatchOptions): Promise<DispatchResult>;
