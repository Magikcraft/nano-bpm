// Theme model + application logic. React-free so the index.html boot script
// mirrors it (see index.html) and CodeEditor/non-React code can subscribe.

/** The token vocabulary — the complete theming contract (see tokens.css). */
export const TOKEN_KEYS = [
  "app",
  "panel",
  "raised",
  "inset",
  "hover",
  "edge",
  "edgeStrong",
  "text",
  "textMuted",
  "textFaint",
  "accent",
  "accentStrong",
  "accent2",
  "onAccent",
  "ok",
  "warn",
  "danger",
  "info",
] as const;

export type TokenKey = (typeof TOKEN_KEYS)[number];

/** A loadable theme: base appearance + per-token colour overrides. */
export interface ThemeSpec {
  /** Stable id, unique across packs + imports (e.g. "nord-dark"). */
  id: string;
  /** Human-facing name shown in the picker. */
  label: string;
  /** Which built-in palette the tokens override — controls `color-scheme` and
   * everything the theme doesn't specify. */
  appearance: "light" | "dark";
  /** Colour per token key; missing keys fall back to the appearance base. */
  tokens: Partial<Record<TokenKey, string>>;
}

/** What the user picked: a built-in mode or a loadable theme by id. */
export type ThemeSelection =
  | { mode: "light" | "dark" | "system" }
  | { mode: "theme"; id: string };

const SELECTION_KEY = "nano.theme";
/** Full spec of the active loadable theme, cached so the boot script can
 * re-apply it before first paint (extension themes need a fetch otherwise). */
const ACTIVE_SPEC_KEY = "nano.theme.active";
/** User-imported theme specs (from JSON files pasted/uploaded in Config). */
const IMPORTED_KEY = "nano.theme.imported";

/** camelCase token key -> `--nano-kebab-case` custom property. */
export function cssVar(key: TokenKey): string {
  return `--nano-${key.replace(/[A-Z2]/g, (c) => (c === "2" ? "-2" : `-${c.toLowerCase()}`))}`;
}

export function loadSelection(): ThemeSelection {
  const raw = localStorage.getItem(SELECTION_KEY);
  if (raw === "light" || raw === "dark" || raw === "system") return { mode: raw };
  if (raw?.startsWith("theme:")) return { mode: "theme", id: raw.slice("theme:".length) };
  return { mode: "system" };
}

export function saveSelection(sel: ThemeSelection): void {
  localStorage.setItem(SELECTION_KEY, sel.mode === "theme" ? `theme:${sel.id}` : sel.mode);
}

export function loadImportedThemes(): ThemeSpec[] {
  try {
    const arr = JSON.parse(localStorage.getItem(IMPORTED_KEY) ?? "[]");
    return Array.isArray(arr) ? arr.filter(isThemeSpec) : [];
  } catch {
    return [];
  }
}

export function saveImportedThemes(themes: ThemeSpec[]): void {
  localStorage.setItem(IMPORTED_KEY, JSON.stringify(themes));
}

export function loadCachedActiveSpec(): ThemeSpec | null {
  try {
    const spec = JSON.parse(localStorage.getItem(ACTIVE_SPEC_KEY) ?? "null");
    return isThemeSpec(spec) ? spec : null;
  } catch {
    return null;
  }
}

/** Validate untrusted theme JSON (imports, packs) down to a safe spec. */
export function isThemeSpec(v: unknown): v is ThemeSpec {
  if (typeof v !== "object" || v === null) return false;
  const t = v as Record<string, unknown>;
  return (
    typeof t.id === "string" &&
    t.id.length > 0 &&
    typeof t.label === "string" &&
    (t.appearance === "light" || t.appearance === "dark") &&
    typeof t.tokens === "object" &&
    t.tokens !== null &&
    Object.values(t.tokens).every((c) => typeof c === "string")
  );
}

export function systemAppearance(): "light" | "dark" {
  return window.matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark";
}

/**
 * Apply a selection to <html>: set `data-appearance` (which flips the built-in
 * palette + color-scheme) and lay any loadable theme's tokens over it as
 * inline custom properties. Returns the resolved appearance so subscribers
 * (Monaco, bpmn overlays) can follow along.
 */
export function applySelection(
  sel: ThemeSelection,
  resolveTheme: (id: string) => ThemeSpec | null,
): "light" | "dark" {
  const root = document.documentElement;
  for (const key of TOKEN_KEYS) root.style.removeProperty(cssVar(key));

  let appearance: "light" | "dark";
  if (sel.mode === "theme") {
    const spec = resolveTheme(sel.id);
    if (spec) {
      appearance = spec.appearance;
      for (const [key, colour] of Object.entries(spec.tokens)) {
        if ((TOKEN_KEYS as readonly string[]).includes(key) && colour) {
          root.style.setProperty(cssVar(key as TokenKey), colour);
        }
      }
      localStorage.setItem(ACTIVE_SPEC_KEY, JSON.stringify(spec));
    } else {
      appearance = systemAppearance(); // theme missing (pack removed) — fall back
    }
  } else {
    appearance = sel.mode === "system" ? systemAppearance() : sel.mode;
    localStorage.removeItem(ACTIVE_SPEC_KEY);
  }
  root.dataset.appearance = appearance;
  return appearance;
}
