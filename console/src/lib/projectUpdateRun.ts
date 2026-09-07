import type { UpdatePlan } from "../gen";

/// The exact copy the post-extension-update dialog must show, per issue #1143.
/// Kept in this pure module (no JSX) so the wording has one source of truth that
/// both the dialog and its e2e/unit guards import without pulling in React.
export const POST_UPDATE_PROJECTS_MESSAGE =
  "The following projects can be updated. They will be stopped if running, and restarted afterward.";

/// The terminal status of a single project's post-extension-update run.
///
/// A project is only ever reported as `"updated"` when the template overlay
/// applied with **no** remaining conflicts AND — if it had been running — it was
/// restarted afterward. Each of the three lifecycle steps (stop, update,
/// restart) has its own failure status so a half-completed run is never shown as
/// a successful update, and `"conflicts"` is distinct from a clean update so a
/// project left with unresolved conflicts is surfaced, not silently overwritten.
export type ProjectUpdateStatus =
  "updated" | "conflicts" | "stop-failed" | "update-failed" | "restart-failed";

/// The live phase of a run, for per-project progress display. `"pending"` is
/// the pre-start state; `"done"` is the terminal display state regardless of
/// the [`ProjectUpdateStatus`].
export type ProjectUpdatePhase =
  "pending" | "stopping" | "updating" | "restarting" | "done";

/// The result of running the stop → update → restart lifecycle for one project.
export type ProjectUpdateOutcome = {
  name: string;
  /// Whether the project was running before the run — the flag that decides
  /// whether it is stopped first and restarted afterward.
  wasRunning: boolean;
  status: ProjectUpdateStatus;
  /// Whether the project was successfully (re)started as the final step. Always
  /// `false` for a project that was not running to begin with (it must stay
  /// stopped).
  restarted: boolean;
  /// The overlay plan the update returned (absent when the update never ran —
  /// i.e. a `"stop-failed"` run).
  plan?: UpdatePlan;
  /// The files left unresolved by the overlay (mirrors `plan.conflicts`), lifted
  /// out so the summary can report them without inspecting the plan.
  conflicts?: string[];
  /// The failing step's error message, for a non-`updated`/`conflicts` status.
  error?: string;
  /// A secondary failure encountered while trying to restore the prior running
  /// state after the primary step already failed (e.g. the update failed and the
  /// best-effort restart also failed). Advisory — the primary `status`/`error`
  /// remains the headline.
  restoreError?: string;
};

/// The three lifecycle operations, injected so the orchestration is unit-testable
/// without a live gateway. Each rejects on failure. `stop` MUST resolve only once
/// the project has actually stopped (the console's `stopProject` awaits the
/// supervisor), so the update never touches files under a live process.
export type ProjectUpdateOps = {
  stop: (name: string) => Promise<void>;
  apply: (name: string) => Promise<UpdatePlan>;
  start: (name: string) => Promise<void>;
};

function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/// Run the stop → update → restart lifecycle for a single project, preserving
/// its prior running state.
///
/// 1. If it was running, stop it and **wait** for the stop to complete before
///    touching any files. A stop failure aborts the run (`"stop-failed"`) — the
///    update never runs, because modifying a live project's files is exactly
///    what the stop guards against.
/// 2. Apply the existing update-from-template overlay (the data-preserving
///    3-way merge; conflicts are left unwritten server-side). If the apply
///    throws, the run is `"update-failed"` — but if the project had been
///    running, a best-effort restart still restores its prior state so a failed
///    update doesn't also leave a running project stopped.
/// 3. Restart it **only if** it had been running (a project that was stopped
///    must remain stopped). A restart failure is `"restart-failed"` — the update
///    itself did apply, but the project is not back to its prior running state,
///    so it is not reported as a clean success.
///
/// `onPhase` is called as the run moves between phases so the caller can show
/// per-project progress.
export async function runProjectUpdate(
  project: { name: string; running: boolean },
  ops: ProjectUpdateOps,
  onPhase?: (phase: ProjectUpdatePhase) => void,
): Promise<ProjectUpdateOutcome> {
  const name = project.name;
  const wasRunning = project.running;
  const base = { name, wasRunning };

  if (wasRunning) {
    onPhase?.("stopping");
    try {
      await ops.stop(name);
    } catch (e) {
      onPhase?.("done");
      return {
        ...base,
        status: "stop-failed",
        restarted: false,
        error: message(e),
      };
    }
  }

  onPhase?.("updating");
  let plan: UpdatePlan;
  try {
    plan = await ops.apply(name);
  } catch (e) {
    let restarted = false;
    let restoreError: string | undefined;
    if (wasRunning) {
      try {
        await ops.start(name);
        restarted = true;
      } catch (re) {
        restoreError = message(re);
      }
    }
    onPhase?.("done");
    return {
      ...base,
      status: "update-failed",
      restarted,
      error: message(e),
      restoreError,
    };
  }

  let restarted = false;
  if (wasRunning) {
    onPhase?.("restarting");
    try {
      await ops.start(name);
      restarted = true;
    } catch (re) {
      onPhase?.("done");
      return {
        ...base,
        status: "restart-failed",
        restarted: false,
        plan,
        conflicts: plan.conflicts,
        error: message(re),
      };
    }
  }

  onPhase?.("done");
  const status: ProjectUpdateStatus =
    plan.conflicts.length > 0 ? "conflicts" : "updated";
  return { ...base, status, restarted, plan, conflicts: plan.conflicts };
}

/// Roll a run's per-project outcomes into the three buckets the dialog summary
/// shows: cleanly updated, left with conflicts, and failed (any of stop / update
/// / restart). A project whose overlay wrote nothing (already up to date) still
/// counts as `"updated"` — the run completed successfully — matching the way the
/// per-project status is assigned.
export function summarizeProjectUpdates(outcomes: ProjectUpdateOutcome[]): {
  updated: string[];
  conflicts: string[];
  failed: string[];
} {
  const updated: string[] = [];
  const conflicts: string[] = [];
  const failed: string[] = [];
  for (const o of outcomes) {
    if (o.status === "updated") updated.push(o.name);
    else if (o.status === "conflicts") conflicts.push(o.name);
    else failed.push(o.name);
  }
  return { updated, conflicts, failed };
}
