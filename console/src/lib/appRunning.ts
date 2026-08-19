// Client mirror of the server's categorical "stop the app before updating it"
// guard (issue #889). A project is *not* editable while its run status is
// starting / running / stopping; it is editable only when stopped or error.
// The studio drives its "app running" banner and the disabled
// apply/deploy/migrate affordances off this single predicate so the UI can
// never drift from the states the server refuses with 409 `app_running`.
import type { RunStatus } from "../gen";

/**
 * Whether an app in `status` is "running" for the purpose of the edit gate.
 * Covers every non-terminal lifecycle phase — `starting`, `running` and
 * `stopping` — because a live process has already bound its workers/schema
 * contract in all three, so an update would break it exactly as the server's
 * guard describes. `stopped` and `error` (a crashed process) are editable.
 */
export function appIsRunning(status: RunStatus | null | undefined): boolean {
  return status === "starting" || status === "running" || status === "stopping";
}

/** The inverse of {@link appIsRunning}: the app's authored surface may be
 * mutated (deploy/save/migrate/structure) only when it is stopped or errored. */
export function appIsEditable(status: RunStatus | null | undefined): boolean {
  return !appIsRunning(status);
}
