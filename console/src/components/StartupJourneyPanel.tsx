import { useEffect } from "react";
import type { Journey } from "../lib/tour/types";

/**
 * The startup persona panel (#464, ADR 0049 §2).
 *
 * Shown once when the console is opened: a modal asking the person which of the
 * offerable journeys they came for, phrased first-person as "I want to …". This
 * replaces the CLI link-spray (`c8ctl` printing `…/console?tour=<id>` on every
 * `start`/`hire`/`work`): the persona is chosen *in* the console, by the person,
 * when they open it — not encoded in whichever link they happened to click.
 *
 * A "Show at startup" checkbox (default checked, persisted in tour state) lets a
 * returning user turn it off; the same journeys stay reachable from the
 * empty-state `JourneyPicker` and the rail's "Take a tour", so nothing is lost.
 *
 * Persona entries derive from the journey registry — never hardcoded — so a new
 * journey appears here the moment it registers. The zero-commitment `overview`
 * is offered as a quiet "just show me around" link, matching the empty-state
 * picker's information architecture rather than competing with the personas.
 */
export default function StartupJourneyPanel({
  journeys,
  onPick,
  onOverview,
  overviewLabel = "Just show me around",
  showAtStartup,
  onToggleShowAtStartup,
  onClose,
}: {
  journeys: Journey[];
  onPick: (journeyId: string) => void;
  /** Runs the profile's overview — the quiet fallback link. Omit to hide it. */
  onOverview?: () => void;
  overviewLabel?: string;
  showAtStartup: boolean;
  onToggleShowAtStartup: (show: boolean) => void;
  onClose: () => void;
}) {
  // Close on Escape for keyboard users.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
      onClick={onClose}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="startup-journey-title"
        className="flex max-h-[85vh] w-full max-w-2xl flex-col overflow-hidden rounded-xl border border-edge-strong bg-raised shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-start justify-between border-b border-edge px-6 py-4">
          <div>
            <h2
              id="startup-journey-title"
              className="text-lg font-semibold text-fg"
            >
              Welcome to Nano
            </h2>
            <p className="mt-1 text-sm text-fg-faint">
              What do you want to do? Pick a starting point and we'll walk you
              through it.
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
          <div className="grid gap-3">
            {journeys.map((journey) => (
              <button
                key={journey.id}
                type="button"
                onClick={() => onPick(journey.id)}
                className="group block w-full rounded-xl border border-edge bg-base p-4 text-left outline-none transition-colors hover:border-accent hover:ring-1 hover:ring-accent focus-visible:ring-2 focus-visible:ring-accent"
              >
                <span className="text-base font-semibold text-fg">
                  I want to {journey.persona ?? journey.title}
                </span>
                <p className="mt-1 text-sm text-fg-faint">{journey.blurb}</p>
                <span className="mt-2 inline-block text-xs font-medium text-accent-strong">
                  Start →
                </span>
              </button>
            ))}
          </div>

          {onOverview && (
            <button
              type="button"
              onClick={onOverview}
              className="mt-5 rounded text-sm text-fg-faint underline decoration-dotted underline-offset-4 outline-none transition-colors hover:text-fg focus-visible:ring-2 focus-visible:ring-accent"
            >
              {overviewLabel} →
            </button>
          )}
        </div>

        <div className="flex items-center justify-between border-t border-edge px-6 py-3">
          <label className="flex cursor-pointer select-none items-center gap-2 text-sm text-fg-muted">
            <input
              type="checkbox"
              className="h-4 w-4 rounded border-edge-strong text-accent focus-visible:ring-2 focus-visible:ring-accent"
              checked={showAtStartup}
              onChange={(e) => onToggleShowAtStartup(e.target.checked)}
            />
            Show at startup
          </label>
          <button
            type="button"
            onClick={onClose}
            className="rounded-md px-3 py-1.5 text-sm font-medium text-fg-muted outline-none transition-colors hover:bg-hover hover:text-fg focus-visible:ring-2 focus-visible:ring-accent"
          >
            Not now
          </button>
        </div>
      </div>
    </div>
  );
}
