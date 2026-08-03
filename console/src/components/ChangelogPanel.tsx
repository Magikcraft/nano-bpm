import { useEffect } from "react";
import type { ChangelogDoc } from "../lib/changelog";
import { UNRELEASED } from "../lib/changelog";

/**
 * The "What's new" panel.
 *
 * A modal listing what changed in each Nano release, newest-first. Its content
 * is the static `changelog.json` asset generated at build time from the git
 * history (see console/scripts/build-changelog.mjs) — grouped by change type,
 * with issue/PR references stripped — so it always reflects the binary that is
 * actually serving the console without any hand maintenance.
 *
 * Opened from the sidebar version chrome; closing it is what marks the newest
 * version as "seen" (the caller persists that), clearing the sidebar's dot.
 */
export default function ChangelogPanel({
  doc,
  loadError,
  onClose,
}: {
  /** The loaded changelog, or null while loading / when unavailable. */
  doc: ChangelogDoc | null;
  /** True when the fetch failed (offline / very old build without the asset). */
  loadError: boolean;
  onClose: () => void;
}) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const versions = doc?.versions ?? [];

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
      onClick={onClose}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="changelog-title"
        className="flex max-h-[85vh] w-full max-w-2xl flex-col overflow-hidden rounded-xl border border-edge-strong bg-raised shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-start justify-between border-b border-edge px-6 py-4">
          <div>
            <h2 id="changelog-title" className="text-lg font-semibold text-fg">
              What's new
            </h2>
            <p className="mt-1 text-sm text-fg-faint">
              Recent changes in Nano, newest first.
            </p>
          </div>
          <button
            className="rounded p-1 text-fg-muted hover:bg-hover hover:text-fg"
            aria-label="Close"
            onClick={onClose}
          >
            ✕
          </button>
        </div>

        <div className="flex-1 overflow-auto px-6 py-5">
          {loadError && (
            <p className="text-sm text-fg-faint">
              The changelog isn't available for this build.
            </p>
          )}
          {!loadError && versions.length === 0 && (
            <p className="text-sm text-fg-faint">No changes recorded yet.</p>
          )}

          <div className="flex flex-col gap-6">
            {versions.map((v) => (
              <section key={v.version}>
                <div className="flex items-baseline gap-3">
                  <h3 className="text-base font-semibold text-fg">
                    {v.version === UNRELEASED ? "Unreleased" : `v${v.version}`}
                  </h3>
                  {v.date && (
                    <span className="font-mono text-xs text-fg-faint">
                      {v.date}
                    </span>
                  )}
                </div>
                <div className="mt-2 flex flex-col gap-3">
                  {v.groups.map((g) => (
                    <div key={g.type}>
                      <div className="text-xs font-bold uppercase tracking-wider text-accent-strong">
                        {g.title}
                      </div>
                      <ul className="mt-1 list-disc space-y-1 pl-5">
                        {g.entries.map((e, i) => (
                          <li
                            key={`${g.type}-${i}`}
                            className="text-sm text-fg-muted"
                          >
                            {e}
                          </li>
                        ))}
                      </ul>
                    </div>
                  ))}
                </div>
              </section>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
