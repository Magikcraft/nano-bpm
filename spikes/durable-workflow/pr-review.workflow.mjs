// A CODE-AUTHORED durable workflow — the ergonomics the façade buys you.
//
// No BPMN, no task-type wiring, no correlation plumbing. Just declare the steps
// and their handlers. The façade derives the model, the job types, the message
// subscription, and drives a generic worker.
//
// This mirrors the real single-user SDLC use case (urban-pr-review): fetch a
// diff, run an automated review (an "activity" that can call an LLM / shell /
// network — side effects live OUTSIDE any determinism sandbox), then WAIT for a
// human to approve via an external signal, then merge.

import { defineWorkflow } from "./sdk.mjs";

export const prReview = defineWorkflow("pr-review", (w) => {
  // Durable activities. Each handler is an ordinary async function that does
  // real work and returns variables to fold into the instance.
  w.run("fetchDiff", async (job) => {
    const prId = job.variables?.prId ?? "unknown";
    return { diff: `diff for ${prId}`, files: 3 };
  });

  w.run("autoReview", async (job) => {
    // In a real workflow this calls an LLM / linter / test run.
    return { findings: ["style: ok", "tests: pass"], reviewedFiles: job.variables?.files ?? 0 };
  });

  // A human-in-the-loop wait. Correlated on the `prId` process variable — so a
  // `sendSignal(baseUrl, prReview, "humanApproval", "<prId>", {...})` resumes it.
  w.signal("humanApproval", { correlationKey: "prId" });

  w.run("merge", async (job) => {
    return { merged: true, approvedBy: job.variables?.approvedBy ?? "system" };
  });
});
