// Durable-workflow spike — the "worker".
//
// A dependency-light local worker (plain fetch, no SDK) that drives the
// durable-abc A -> B -> C process. It is the stand-in for what a future
// `@nanobpm/workflow` code-first SDK would generate: for each step it performs
// an *observable external side effect* (appends a line to a ledger file, like
// "send email" / "open PR" would), then completes the job so the engine
// advances the token durably.
//
// The whole point of the spike is what happens across an ENGINE CRASH:
//   - Completed steps' side effects are recorded exactly once in the ledger.
//   - If the engine truly resumes from its durable journal, killing it after B
//     commits and restarting it must NOT replay A or B — only C runs, once.
//
// The worker is deliberately resilient to the server disappearing (all fetches
// swallow errors and retry), so it reconnects on its own when the engine comes
// back — no restart of the worker is needed to prove engine-side durability.
//
// Env:
//   BASE_URL       gateway base (e.g. http://localhost:8080)
//   LEDGER         append-only side-effect ledger path
//   PROGRESS       append-only commit-progress path (one line per committed job)
//   CRASH_TRIGGER  after committing this job type, HOLD (default act-b)
//   HOLD_MS        how long to pause after the trigger, giving the driver a
//                  deterministic window to kill+restart the engine (default 4000)

import { appendFileSync } from "node:fs";

const BASE_URL = process.env.BASE_URL ?? "http://localhost:8080";
const LEDGER = process.env.LEDGER;
const PROGRESS = process.env.PROGRESS;
const CRASH_TRIGGER = process.env.CRASH_TRIGGER ?? "act-b";
const HOLD_MS = Number(process.env.HOLD_MS ?? "4000");
const ORDER = ["act-a", "act-b", "act-c"];

if (!LEDGER || !PROGRESS) {
  console.error("worker: LEDGER and PROGRESS env vars are required");
  process.exit(2);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.error(`[worker ${new Date().toISOString()}]`, ...a);

async function activate(type) {
  try {
    const res = await fetch(`${BASE_URL}/v2/jobs/activation`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      // requestTimeout < 0 disables long polling → return immediately, so a poll
      // that is open when the engine dies can't stall us or race ahead to C.
      body: JSON.stringify({ type, worker: "durable-spike", maxJobsToActivate: 1, timeout: 30000, requestTimeout: -1 }),
    });
    if (!res.ok) return [];
    const body = await res.json();
    return body.jobs ?? [];
  } catch {
    return []; // engine down; try again next tick
  }
}

async function complete(jobKey) {
  try {
    const res = await fetch(`${BASE_URL}/v2/jobs/${jobKey}/completion`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ variables: {} }),
    });
    return res.ok;
  } catch {
    return false;
  }
}

let running = true;
process.on("SIGTERM", () => { running = false; });
process.on("SIGINT", () => { running = false; });

async function main() {
  log(`starting; base=${BASE_URL} trigger=${CRASH_TRIGGER} hold=${HOLD_MS}ms`);
  while (running) {
    let held = false;
    for (const type of ORDER) {
      const jobs = await activate(type);
      for (const job of jobs) {
        // 1. External side effect FIRST (the thing that must not duplicate).
        appendFileSync(LEDGER, `sideeffect ${type} pi=${job.processInstanceKey} el=${job.elementId}\n`);
        log(`side effect for ${type} (pi=${job.processInstanceKey})`);
        // 2. Commit the step in the engine.
        const ok = await complete(job.jobKey);
        if (!ok) {
          // Completion didn't land (e.g. engine died between 1 and 2). Leave it;
          // the engine will redeliver this job on restart — which is exactly the
          // at-least-once boundary the ADR must document (activities must be
          // idempotent). For the crash-AFTER-commit scenario this never fires.
          log(`completion for ${type} did not ack; will be redelivered`);
          continue;
        }
        appendFileSync(PROGRESS, `committed ${type} pi=${job.processInstanceKey}\n`);
        log(`committed ${type}`);
        if (type === CRASH_TRIGGER) { held = true; break; }
        if (type === "act-c") { log("workflow reached C; worker idle"); }
      }
      if (held) break;
    }
    if (held) {
      // Pause so the driver has a clean window to SIGKILL + restart the engine
      // between B's commit and C's activation. During the hold we do not poll,
      // so C cannot be activated early.
      log(`holding ${HOLD_MS}ms after ${CRASH_TRIGGER} (engine-crash window)`);
      await sleep(HOLD_MS);
    } else {
      await sleep(150);
    }
  }
  log("stopped");
}

main();
