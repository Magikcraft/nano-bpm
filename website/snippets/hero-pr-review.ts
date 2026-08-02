// Compilable source of the nanobpm.io landing hero snippet (the "Code-first" tab).
//
// The `hero` region between the //#region / //#endregion markers below is
// extracted VERBATIM by website/build.mjs and rendered on the landing page, so
// what visitors see is exactly this code — and it is type-checked against the
// published `@nanobpm/workflow` in CI (the "website snippet (typecheck)" job),
// so the marketing example can never drift from the real API.
//
// It uses `defineFlow` — the recommended declarative code-first surface (the
// `run`/`signal` tree that emits BPMN, with typed data envelopes). The
// imperative `defineWorkflow` is the experimental/internal replay surface and is
// deliberately NOT shown here.
//
// `gh` and `agent` are illustrative external helpers (a GitHub client and a
// hired coding agent). They are declared below, OUTSIDE the displayed region, so
// the flow body compiles without cluttering the hero with stubs.

//#region hero
import { defineFlow, envelope } from "@nanobpm/workflow";

// Typed data envelopes — lifted into the emitted BPMN, so the generated model
// stays ejectable to the modeller with its contracts intact.
const PrRef = envelope("PrRef", { prKey: "string" });
const Diff = envelope("Diff", { diff: "string" });
const Verdict = envelope("Verdict", { verdict: "string" });
const Merged = envelope("Merged", { merged: "boolean" });

// urban-pr-review — an agentic convergence loop, authored as code.
// The same app the Model-first tab runs live on the wasm engine.
export const prReview = defineFlow(
  "urban-pr-review",
  {
    fetchDiff: { in: PrRef, out: Diff },
    "senior:pr-review": { in: Diff, out: Verdict },
    merge: { in: PrRef, out: Merged },
  },
  (w) => {
    w.run("fetchDiff", async (job) => ({ diff: await gh.diff(job.variables.prKey) }));

    // A hired coding agent (c8ctl nano hire copilot) reviews as a durable worker.
    w.run("senior:pr-review", async (job) => ({ verdict: await agent.review(job.variables.diff) }));

    // Durable wait — survives a crash or forced reboot, resumes on the signal.
    w.signal("review-ready", { correlationKey: "prKey" });

    w.run("merge", async (job) => ({ merged: await gh.merge(job.variables.prKey) }));
  },
);
//#endregion hero

// --- illustrative external helpers (not shown on the site) -------------------
declare const gh: {
  diff(prKey: string): Promise<string>;
  merge(prKey: string): Promise<boolean>;
};
declare const agent: {
  review(diff: string): Promise<string>;
};
