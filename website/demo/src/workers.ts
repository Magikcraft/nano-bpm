import type { JobHandler } from "@nanobpm/bojtos-react";

/**
 * A scripted "senior reviewer" agent standing in for the real `senior:pr-review`
 * LLM worker (which can't run on a public static page — no server, no keys). It
 * walks a deterministic convergence: request changes, then block on a question,
 * then approve — exercising both message-wait loops in the model
 * (`review-ready` and `escalation-answered`).
 *
 * The persist/finalize workers are no-ops here: in the deployed app they write to
 * the datasource; in the demo the point is the token movement and the payload.
 */
export interface ReviewStep {
  status: "addressed" | "needs_input" | "converged";
  summary: string;
  question?: string;
}

export const REVIEW_SCRIPT: ReviewStep[] = [
  {
    status: "addressed",
    summary:
      "Requested changes: add tests for the error path and tighten input validation.",
  },
  {
    status: "needs_input",
    summary: "Blocked — a product decision is needed before I can approve.",
    question: "Should deletes be soft (recoverable) or hard?",
  },
  {
    status: "converged",
    summary: "All feedback addressed and the question answered. Approved ✅",
  },
];

/** Human-readable labels for the log, keyed by the model's job type. */
export const TASK_LABELS: Record<string, string> = {
  "senior:pr-review": "Senior reviewer",
  "pr.persist-round": "Recording round",
  "pr.persist-escalation": "Recording escalation",
  "pr.finalize": "Marking converged",
};

/**
 * Build the worker map. `onReview` is called with each scripted step as the
 * agent "responds", so the UI can narrate the conversation. `counter` is a
 * mutable holder so re-runs (after reset) restart the script from the top.
 */
export function makeWorkers(
  counter: { i: number },
  onReview: (step: ReviewStep, round: number) => void,
): Record<string, JobHandler> {
  return {
    "senior:pr-review": () => {
      const step = REVIEW_SCRIPT[Math.min(counter.i, REVIEW_SCRIPT.length - 1)];
      onReview(step, counter.i);
      counter.i += 1;
      return {
        status: step.status,
        summary: step.summary,
        question: step.question ?? "",
      };
    },
    "pr.persist-round": () => ({}),
    "pr.persist-escalation": () => ({}),
    "pr.finalize": () => ({}),
  };
}
