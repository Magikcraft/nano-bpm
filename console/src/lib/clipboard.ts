// Clipboard copy with an insecure-context fallback.
//
// `navigator.clipboard` requires a secure context (HTTPS or localhost), and this
// console is routinely served over plain HTTP on a LAN address or a tunnel —
// exactly where copying a base URL or a command matters most. So the async API
// is probed at runtime and, when it is absent, a hidden-textarea `execCommand`
// path is used instead.
//
// Single source of truth: both the guided-journey handoff popover (runner.ts)
// and the Explorer base-URL affordance (Explorer.tsx) copy through here, so the
// "works over plain HTTP off localhost" guarantee has one implementation rather
// than several drifting copies.

/**
 * Copy `text` to the clipboard. Returns `false` when nothing could be copied, so
 * the caller can fall back to selecting the text for a manual copy rather than
 * failing silently.
 */
export async function copyText(text: string): Promise<boolean> {
  try {
    // The DOM lib types `navigator.clipboard` as always present, but it is
    // genuinely absent outside a secure context — so probe it at runtime via
    // `typeof` rather than a truthiness check TypeScript would call redundant.
    const clipboard = globalThis.navigator?.clipboard;
    if (clipboard && typeof clipboard.writeText === "function") {
      await clipboard.writeText(text);
      return true;
    }
  } catch {
    // Fall through to the legacy path.
  }
  try {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return ok;
  } catch {
    return false;
  }
}
