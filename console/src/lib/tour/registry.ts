// The journey registry.
//
// Journeys register themselves from their own module (`journeys/*.ts`), so
// adding one never edits a central import list — that list would be a merge
// point for every journey slice working in parallel (ADR 0049 fans four of them
// out at once). Slices own their file; the registry just collects.
//
// Pure and side-effect-free apart from the module-level map, so it is unit
// testable without the Vite globals.

import type { ConsoleProfile } from "../profile";
import type { Journey, Step, TourContext } from "./types";

const journeys = new Map<string, Journey>();

/** Register (or replace, by id) a journey. Called at module load by each journey file. */
export function registerJourney(journey: Journey): void {
  journeys.set(journey.id, journey);
}

export function getJourney(id: string): Journey | undefined {
  return journeys.get(id);
}

export function allJourneys(): Journey[] {
  return [...journeys.values()];
}

/** Test seam: drop everything so a test can register fixtures in isolation. */
export function clearJourneys(): void {
  journeys.clear();
}

/**
 * Journeys offerable right now: available in this profile, and not gated out by
 * their own journey-level preconditions.
 *
 * A journey-level precondition returning anything but `ok` removes the journey
 * from the picker entirely — the point is to never offer a path that cannot
 * work here (an MQTT journey without the pack, a cluster journey on one node).
 * Per-*step* preconditions are different: they repair or skip within a journey
 * that is otherwise fine.
 */
export function journeysFor(
  profile: ConsoleProfile,
  ctx?: TourContext,
): Journey[] {
  return allJourneys().filter((j) => {
    if (!j.profiles.includes(profile)) return false;
    if (!ctx || !j.preconditions?.length) return true;
    return j.preconditions.every((p) => p.test(ctx) === "ok");
  });
}

/**
 * Resolve a journey's authored steps against a context: substitute `repair`
 * replacements, drop `skip`ped steps, and report what happened so the caller can
 * emit analytics for it.
 *
 * Resolution happens once, when the journey starts. Preconditions describe the
 * *environment* (is there a runtime? a cluster? traces?), which does not usually
 * change over the ninety seconds a journey takes; step-by-step re-resolution is
 * a later increment and would mean rebuilding the runner's step list mid-drive.
 * Liveness that genuinely does change — a handoff step waiting for a worker to
 * connect — is handled by `verify` polling the live context instead.
 *
 * `authoredIndex` is carried through so a resume can be stored against the
 * authored list and stay valid even if a later resolution skips a different set.
 */
export interface ResolvedStep {
  step: Step;
  authoredIndex: number;
  repaired?: string;
}

export interface Resolution {
  steps: ResolvedStep[];
  skipped: { stepId: string; preconditionId: string }[];
  repaired: { stepId: string; preconditionId: string }[];
}

export function resolveSteps(journey: Journey, ctx: TourContext): Resolution {
  const steps: ResolvedStep[] = [];
  const skipped: Resolution["skipped"] = [];
  const repaired: Resolution["repaired"] = [];

  journey.steps.forEach((step, authoredIndex) => {
    const pre = step.precondition;
    if (!pre) {
      steps.push({ step, authoredIndex });
      return;
    }
    switch (pre.test(ctx)) {
      case "ok":
        steps.push({ step, authoredIndex });
        return;
      case "skip":
        skipped.push({ stepId: step.id, preconditionId: pre.id });
        return;
      case "repair":
        if (step.repair) {
          repaired.push({ stepId: step.id, preconditionId: pre.id });
          steps.push({
            step: step.repair,
            authoredIndex,
            repaired: pre.id,
          });
        } else {
          // A "repair" verdict with no repair step authored: skip rather than
          // show the original, whose promise is exactly what the precondition
          // said would not hold.
          skipped.push({ stepId: step.id, preconditionId: pre.id });
        }
        return;
    }
  });

  return { steps, skipped, repaired };
}
