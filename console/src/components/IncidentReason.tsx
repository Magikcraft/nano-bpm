import { useState, type CSSProperties } from "react";
import { copyText } from "../lib/clipboard";
import {
  REASON_COLLAPSE_LINES,
  shouldCollapseReason,
} from "../lib/incidentReason";

// Renders an incident *reason* legibly. Worker/engine reasons are frequently a
// full multiline stderr dump (a failing `git clone`, an agent traceback), so we
// preserve whitespace and line breaks (`whitespace-pre-wrap`), collapse a long
// reason to a few lines with an expand toggle, and offer a copy button so an
// operator can paste the *whole* text into an issue. The collapse is purely
// visual — the full string is always kept for expand + copy, never truncated.

/** Clamp a collapsed reason to REASON_COLLAPSE_LINES lines. Derived from the
 * shared constant so the visual clamp and the "should collapse?" decision can
 * never drift apart. */
const clampStyle: CSSProperties = {
  display: "-webkit-box",
  WebkitLineClamp: REASON_COLLAPSE_LINES,
  WebkitBoxOrient: "vertical",
  overflow: "hidden",
};

export function IncidentReason({ reason }: { reason: string }) {
  const collapsible = shouldCollapseReason(reason);
  const [expanded, setExpanded] = useState(false);
  const [copied, setCopied] = useState(false);

  const clamped = collapsible && !expanded;

  async function onCopy() {
    const ok = await copyText(reason);
    if (!ok) return;
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1500);
  }

  return (
    <div className="flex min-w-0 flex-col items-start gap-1">
      <pre
        // The full reason is always in the DOM (and the `title`) — only the
        // rendered height is clamped, so nothing is lost.
        className="min-w-0 max-w-full whitespace-pre-wrap break-words font-mono text-xs leading-relaxed text-danger"
        style={clamped ? clampStyle : undefined}
        title={reason}
      >
        {reason}
      </pre>
      <div className="flex items-center gap-3">
        {collapsible && (
          <button
            type="button"
            className="rounded text-[11px] font-medium text-fg-muted underline-offset-2 outline-none hover:text-fg hover:underline focus-visible:ring-2 focus-visible:ring-accent/60"
            aria-expanded={expanded}
            onClick={() => setExpanded((v) => !v)}
          >
            {expanded ? "Show less" : "Show more"}
          </button>
        )}
        <button
          type="button"
          className="rounded text-[11px] font-medium text-fg-muted underline-offset-2 outline-none hover:text-fg hover:underline focus-visible:ring-2 focus-visible:ring-accent/60"
          onClick={onCopy}
        >
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
    </div>
  );
}
