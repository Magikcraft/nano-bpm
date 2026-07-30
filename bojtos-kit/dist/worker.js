/**
 * Throw from a {@link JobHandler} to fail a job with an explicit remaining
 * `retries` count (default is `job.retries - 1`). With `retries: 0` the engine
 * raises an incident, which surfaces in the snapshot's `incidentElementIds` —
 * handy for demoing the failure path deterministically.
 */
export class JobFailure extends Error {
    retries;
    constructor(message, opts) {
        super(message);
        this.name = "JobFailure";
        this.retries = opts?.retries;
    }
}
async function runOne(session, handler, job) {
    let payload;
    try {
        // Only the handler and the serialization of its result are treated as a
        // job failure: a handler that throws (or returns something unserializable)
        // is the demo's own logic failing, so we translate it into `failJob`.
        const out = await handler(job);
        payload = JSON.stringify(out ?? {});
    }
    catch (e) {
        const retries = e instanceof JobFailure && e.retries !== undefined
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
async function runOneAgent(session, handler, job) {
    let result;
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
    }
    catch (e) {
        const retries = e instanceof JobFailure && e.retries !== undefined
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
export async function dispatchRound(session, workers, opts = {}) {
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
            throw new Error(`dispatchRound: job type "${jobType}" is registered as both a worker and an agent — register it as exactly one`);
        }
    }
    // Activation pass: lock the whole current frontier before running any handler,
    // so a job a handler unblocks isn't also picked up this round (which would
    // cascade the entire chain in a single "step").
    const jobBatch = [];
    for (const [jobType, handler] of Object.entries(workers)) {
        for (const job of session.activateJobs(jobType, maxJobs, timeout, worker)) {
            jobBatch.push({ handler, job });
        }
    }
    const agentBatch = [];
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
export async function dispatchWorkers(session, workers, opts = {}) {
    const maxRounds = opts.maxRounds ?? 1000;
    let handled = 0;
    let rounds = 0;
    for (;;) {
        if (rounds >= maxRounds) {
            throw new Error(`dispatchWorkers exceeded maxRounds (${maxRounds}) — a handler may be creating work without end`);
        }
        rounds++;
        const round = await dispatchRound(session, workers, opts);
        handled += round.handled;
        if (round.handled === 0) {
            return { snapshot: round.snapshot, handled, rounds };
        }
    }
}
