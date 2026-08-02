// Single source of truth for the integrated-terminal pane's render decision and
// its xterm colour theme. Extracted from TerminalPane.tsx so the two defect
// classes fixed in issue #504 are guarded by pure Node-native unit tests rather
// than by React-render behaviour:
//
//   1. Tab-switch must NOT tear down a live PTY session. The pane used to
//      `return <Frame/>` whenever `active` was false, unmounting the live
//      terminal and killing its shell. `terminalPaneState` instead keeps the
//      `live` state (just `hidden`) while inactive, so the component stays
//      mounted and the WebSocket/PTY survives. See TerminalPane.tsx.
//
//   2. The xterm theme must always set an explicit foreground derived from the
//      app theme tokens. The pane used to set only `background`, leaving
//      xterm's default WHITE foreground — invisible on the light-mode surface.
//      `terminalTheme` derives foreground/cursor from `--nano-text`.

/** The subset of the server's terminal config the pane's decision depends on. */
export type TerminalConfigLike = {
  locked: boolean;
  enabled: boolean;
  local: boolean;
} | null;

/**
 * What the terminal pane renders. Every non-`live` state is a static panel with
 * no session to preserve, so it is only produced when the tab is active. The
 * `live` state carries a `hidden` flag: it is returned even when the tab is
 * inactive (so React keeps `LiveTerminal` mounted and the PTY alive), and the
 * flag drives CSS visibility instead of unmount/remount.
 */
export type TerminalPaneState =
  | { kind: "idle" }
  | { kind: "loading" }
  | { kind: "error" }
  | { kind: "locked" }
  | { kind: "off-local" }
  | { kind: "off-remote" }
  | { kind: "remote" }
  | { kind: "live"; hidden: boolean };

/** True when the config resolves to a runnable, local PTY. */
export function isLive(cfg: TerminalConfigLike): boolean {
  return !!cfg && !cfg.locked && cfg.enabled && cfg.local;
}

/**
 * Derive what the pane shows from `(active, cfg, cfgErr)`.
 *
 * Crucially, when the tab is inactive but a live terminal exists we return
 * `{ kind: "live", hidden: true }` rather than `idle`. That single choice is
 * the #504 fix for the whole "tab switch kills the shell" defect class: the
 * live element is rendered in both active and inactive states, so React
 * preserves it across tab toggles and the shell session never restarts.
 */
export function terminalPaneState(
  active: boolean,
  cfg: TerminalConfigLike,
  cfgErr: string | null,
): TerminalPaneState {
  if (!active) {
    return isLive(cfg) ? { kind: "live", hidden: true } : { kind: "idle" };
  }
  if (cfgErr) return { kind: "error" };
  if (!cfg) return { kind: "loading" };
  if (cfg.locked) return { kind: "locked" };
  if (!cfg.enabled)
    return cfg.local ? { kind: "off-local" } : { kind: "off-remote" };
  if (!cfg.local) return { kind: "remote" };
  return { kind: "live", hidden: false };
}

/** xterm.js colours we drive from the app theme. */
export type TerminalThemeColors = {
  background: string;
  foreground: string;
  cursor: string;
  cursorAccent?: string;
};

/**
 * xterm's own default foreground. The #504 light-mode bug was leaving this in
 * place; `terminalTheme` must never emit it as the resolved foreground when a
 * theme token is available.
 */
export const XTERM_DEFAULT_FOREGROUND = "#ffffff";

/** Fallback foreground when the app tokens can't be read (SSR/tests). */
const FALLBACK_FOREGROUND = "#f2f2f7";

/**
 * Build the xterm theme from the app's CSS custom properties. `readVar` returns
 * the (possibly empty) value of a `--nano-*` variable — in the browser this is
 * `getComputedStyle(document.documentElement).getPropertyValue(name)`.
 *
 * Background stays fully transparent so the terminal inherits the pane surface;
 * foreground and cursor follow `--nano-text`, which flips with
 * `:root[data-appearance="light|dark"]`, keeping text legible in both themes.
 */
export function terminalTheme(
  readVar: (name: string) => string,
): TerminalThemeColors {
  const fg = readVar("--nano-text").trim() || FALLBACK_FOREGROUND;
  const appBg = readVar("--nano-app").trim();
  return {
    background: "#00000000",
    foreground: fg,
    cursor: fg,
    cursorAccent: appBg || undefined,
  };
}
