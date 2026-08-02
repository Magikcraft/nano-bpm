// Unit tests for the terminal pane's render decision + theme derivation.
// Node-native: run with `node --experimental-strip-types --test`.
//
// Regression guards for issue #504:
//   1. Switching tabs (any `active` toggle) must never tear down a live PTY
//      session — the pane must stay in the `live` state (hidden) while inactive.
//   2. The xterm theme must always set an explicit foreground derived from the
//      app theme, never leaving xterm's default white-on-white.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  type TerminalConfigLike,
  isLive,
  terminalPaneState,
  terminalTheme,
  XTERM_DEFAULT_FOREGROUND,
} from "./terminalPane.ts";

const LIVE: TerminalConfigLike = { locked: false, enabled: true, local: true };
const OFF_LOCAL: TerminalConfigLike = {
  locked: false,
  enabled: false,
  local: true,
};
const OFF_REMOTE: TerminalConfigLike = {
  locked: false,
  enabled: false,
  local: false,
};
const REMOTE: TerminalConfigLike = {
  locked: false,
  enabled: true,
  local: false,
};
const LOCKED: TerminalConfigLike = { locked: true, enabled: true, local: true };

test("isLive is true only for an unlocked, enabled, local terminal", () => {
  assert.equal(isLive(LIVE), true);
  assert.equal(isLive(OFF_LOCAL), false);
  assert.equal(isLive(REMOTE), false);
  assert.equal(isLive(LOCKED), false);
  assert.equal(isLive(null), false);
});

// --- Bug 1: tab switch must not kill the shell -----------------------------

test("a live terminal stays mounted (live+hidden) when the tab goes inactive", () => {
  // This is the core #504 regression guard: inactive + live must NOT collapse
  // to `idle`/empty (which unmounts LiveTerminal and kills the PTY).
  const inactive = terminalPaneState(false, LIVE, null);
  assert.deepEqual(inactive, { kind: "live", hidden: true });
});

test("toggling active back and forth keeps the same live kind (no remount)", () => {
  const seq = [true, false, true, false, true];
  const kinds = seq.map((a) => terminalPaneState(a, LIVE, null).kind);
  assert.deepEqual(kinds, ["live", "live", "live", "live", "live"]);
});

test("active live is visible; inactive live is hidden", () => {
  assert.deepEqual(terminalPaneState(true, LIVE, null), {
    kind: "live",
    hidden: false,
  });
  assert.deepEqual(terminalPaneState(false, LIVE, null), {
    kind: "live",
    hidden: true,
  });
});

test("inactive with no live session is idle (nothing to preserve)", () => {
  for (const cfg of [null, OFF_LOCAL, OFF_REMOTE, REMOTE, LOCKED]) {
    assert.deepEqual(terminalPaneState(false, cfg, null), { kind: "idle" });
  }
});

// --- The static (active) panels still resolve correctly --------------------

test("active config panels resolve to their kinds", () => {
  assert.equal(terminalPaneState(true, null, "boom").kind, "error");
  assert.equal(terminalPaneState(true, null, null).kind, "loading");
  assert.equal(terminalPaneState(true, LOCKED, null).kind, "locked");
  assert.equal(terminalPaneState(true, OFF_LOCAL, null).kind, "off-local");
  assert.equal(terminalPaneState(true, OFF_REMOTE, null).kind, "off-remote");
  assert.equal(terminalPaneState(true, REMOTE, null).kind, "remote");
  assert.equal(terminalPaneState(true, LIVE, null).kind, "live");
});

test("cfgErr is ignored while inactive (no flicker of the error panel on switch-away)", () => {
  assert.deepEqual(terminalPaneState(false, LIVE, "boom"), {
    kind: "live",
    hidden: true,
  });
});

// --- Bug 2: theme must set a legible, theme-derived foreground --------------

const LIGHT: Record<string, string> = {
  "--nano-text": "#1a1a22",
  "--nano-app": "#f5f5f9",
};
const DARK: Record<string, string> = {
  "--nano-text": "#f2f2f7",
  "--nano-app": "#0b0b10",
};
const reader = (vars: Record<string, string>) => (name: string) =>
  vars[name] ?? "";

test("light-mode foreground is the dark app text token, not white", () => {
  const theme = terminalTheme(reader(LIGHT));
  assert.equal(theme.foreground, "#1a1a22");
  assert.notEqual(
    theme.foreground.toLowerCase(),
    XTERM_DEFAULT_FOREGROUND,
    "foreground must not be xterm's default white on a light surface",
  );
  assert.equal(theme.cursor, "#1a1a22");
});

test("dark-mode foreground follows the light app text token", () => {
  const theme = terminalTheme(reader(DARK));
  assert.equal(theme.foreground, "#f2f2f7");
  assert.equal(theme.cursor, "#f2f2f7");
});

test("background stays fully transparent so the pane surface shows through", () => {
  assert.equal(terminalTheme(reader(LIGHT)).background, "#00000000");
  assert.equal(terminalTheme(reader(DARK)).background, "#00000000");
});

test("a defined, non-white foreground is always emitted, even with no tokens", () => {
  const theme = terminalTheme(() => "");
  assert.ok(theme.foreground.length > 0);
  assert.notEqual(theme.foreground.toLowerCase(), XTERM_DEFAULT_FOREGROUND);
});

test("whitespace-only token values fall back rather than emitting empty", () => {
  const theme = terminalTheme(() => "   ");
  assert.ok(theme.foreground.trim().length > 0);
});
