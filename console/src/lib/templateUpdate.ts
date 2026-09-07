import type { ProjectSummary, UpdatePlan } from "../gen";

/// The minimal identity the "update from template" flow needs to drive its
/// review-and-apply modals for a single project: the directory-safe `name` the
/// API is keyed by, plus optional presentation (`displayName`) and the update
/// target (`latestVersion`) used purely for copy. Both the projects list, the
/// IDE workspace and the extensions reverse-index resolve one of these from
/// their own project shape via [`toUpdateTarget`], so the flow has a single
/// canonical entry surface instead of one per caller.
export type UpdateTarget = {
  name: string;
  displayName?: string | null;
  latestVersion?: string | null;
};

/// Present a target the way every update surface does: its human `displayName`
/// when set, else the directory-safe `name`.
export function targetTitle(target: UpdateTarget): string {
  return target.displayName ?? target.name;
}

/// Project summary → the flow's [`UpdateTarget`]. Single place the mapping
/// lives so a new field the modals want (e.g. version) is threaded once.
export function toUpdateTarget(project: ProjectSummary): UpdateTarget {
  return {
    name: project.name,
    displayName: project.displayName,
    latestVersion: project.latestVersion,
  };
}

/// How many files an overlay plan would actually write: the union of newly
/// created, safely overwritten and auto-merged files. Conflicts are excluded —
/// they are surfaced for manual resolution and never written — so this is also
/// the count the apply button offers to apply.
export function planChangeCount(plan: UpdatePlan): number {
  return plan.create.length + plan.overwrite.length + plan.merged.length;
}

/// A dry-run plan is "clean" when it has changes to write and no conflicts —
/// the case the hybrid badge flow fast-tracks to a one-tap confirm instead of
/// the full diff modal.
export function planIsClean(plan: UpdatePlan): boolean {
  return planChangeCount(plan) > 0 && plan.conflicts.length === 0;
}

/// Nothing to apply: no writable changes and no conflicts. The project is
/// already up to date with the template (a stale `updateAvailable` signal, or a
/// race where the pack changed back).
export function planNothingToDo(plan: UpdatePlan): boolean {
  return planChangeCount(plan) === 0 && plan.conflicts.length === 0;
}

/// The projects scaffolded from a given pack id — the reverse of the per-project
/// `scaffoldedFrom.pack` breadcrumb. `packId` is the pack's manifest id, which
/// is exactly what the breadcrumb records and what the installed-extension list
/// is keyed by, so the join is a straight equality.
export function projectsUsingPack(
  projects: ProjectSummary[],
  packId: string,
): ProjectSummary[] {
  return projects.filter((p) => p.scaffoldedFrom?.pack === packId);
}

/// The subset of [`projectsUsingPack`] that can actually be updated right now:
/// the pack moved past the project's recorded scaffold version
/// (`updateAvailable`) and the project is a real workspace copy, not an external
/// path import (ADR 0041) whose files we never overwrite in place.
export function updatableProjectsUsingPack(
  projects: ProjectSummary[],
  packId: string,
): ProjectSummary[] {
  return projectsUsingPack(projects, packId).filter(
    (p) => p.updateAvailable && p.source !== "path",
  );
}

/// The projects that became (or became more) update-eligible as a result of a
/// single extension update — i.e. the projects scaffolded from the just-updated
/// pack, which is exactly the set the post-extension-update dialog offers.
///
/// It scopes to the updated extension without needing the npm-name↔manifest-id
/// mapping (which only exists server-side): a project's `latestVersion` is its
/// scaffolding pack's currently-installed version (the update target), so a
/// project whose `latestVersion` *changed* across the extension update is one
/// whose pack was the one just updated. Unrelated packs' projects keep the same
/// `latestVersion` and are excluded; already-current projects that only now
/// gained a target (`undefined` → the new version) are included. As with
/// [`updatableProjectsUsingPack`], external path imports (ADR 0041) are excluded
/// — their files are never overwritten in place — and only genuinely-updatable
/// (`updateAvailable`) projects qualify.
export function projectsNewlyUpdatable(
  before: ProjectSummary[],
  after: ProjectSummary[],
): ProjectSummary[] {
  const priorTarget = new Map(
    before.map((p) => [p.name, p.latestVersion ?? null]),
  );
  return after.filter((p) => {
    if (!p.updateAvailable || p.source === "path") return false;
    const prev = priorTarget.get(p.name) ?? null;
    return (p.latestVersion ?? null) !== prev;
  });
}

/// One project's outcome in an "Update all" batch: the plan the apply returned
/// (or the error that aborted it).
export type BatchItemResult = {
  name: string;
  plan?: UpdatePlan;
  error?: string;
};

/// Roll a batch's per-project results into the three buckets the summary shows:
/// cleanly updated, left with conflicts (the batch stops at the first of these),
/// and errored. A project whose apply wrote nothing and had no conflicts (already
/// up to date) counts as neither updated nor conflicted — it is simply a no-op.
export function summarizeBatch(results: BatchItemResult[]): {
  updated: string[];
  conflicted: string[];
  errored: string[];
} {
  const updated: string[] = [];
  const conflicted: string[] = [];
  const errored: string[] = [];
  for (const r of results) {
    if (r.error) {
      errored.push(r.name);
    } else if (r.plan && r.plan.conflicts.length > 0) {
      conflicted.push(r.name);
    } else if (r.plan && planChangeCount(r.plan) > 0) {
      updated.push(r.name);
    }
  }
  return { updated, conflicted, errored };
}
