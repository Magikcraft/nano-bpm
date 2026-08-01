// Single source of truth for which auxiliary pane the project workspace shows.
//
// The main pane is mutually exclusive: at most one of the Model / Data /
// Triggers / Connectors panels is open, otherwise the file-content editor is
// shown. Modelling this as one nullable value (instead of four independent
// booleans + the separately-tracked selected file) makes contradictory states
// unrepresentable — see issue #485, where a file click updated the selection
// but left a panel flag set, so the editor branch was unreachable.

export type WorkspacePanel = "data" | "triggers" | "connectors" | "model";

/** The active auxiliary panel, or `null` when the file editor is showing. */
export type ActivePanel = WorkspacePanel | null;

/** What the workspace main pane renders. */
export type MainPane = WorkspacePanel | "editor";

/**
 * Toggle a panel toolbar button. Clicking the already-open panel closes it
 * (back to the editor); clicking any other panel switches straight to it.
 * Replaces four hand-duplicated "set mine true, clear the other three" blocks.
 */
export function togglePanel(
  current: ActivePanel,
  target: WorkspacePanel,
): ActivePanel {
  return current === target ? null : target;
}

/**
 * Opening a file always returns to the editor, regardless of which panel (if
 * any) was open. This is the #485 fix generalised over the whole defect class:
 * for every panel, selecting a file dismisses it.
 */
export function selectFile(_current: ActivePanel): ActivePanel {
  return null;
}

/** Derive what the main pane shows from the single active-panel value. */
export function mainPane(active: ActivePanel): MainPane {
  return active ?? "editor";
}
