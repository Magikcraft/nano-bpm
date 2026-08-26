// The canonical `--nano-*` colour palette (issue #1005) — the single source of
// truth for the console theme and the Urban runtime.
//
// The same literal hex used to be byte-duplicated across a repo boundary: the
// console carried it inline in `console/src/theme/tokens.css`, and the Urban
// runtime restated it in `packages/urban/src/runtime/core/modules/pages.ts`.
// The `nano-theme` postMessage bridge papered over any divergence at runtime, so
// nothing caught a drift at build time. This module retires that duplication: the
// palette values live here, once, and are exposed as two subpath exports of
// `@nanobpm/nano-app-schema`:
//
//   - `@nanobpm/nano-app-schema/tokens.css` — the palette as `:root` CSS custom
//     properties (generated from {@link NANO_PALETTE} by {@link renderPaletteCss};
//     the committed `spec-app/tokens.css` artifact). The console `@import`s it in
//     place of the inline hex; keeping the console's rendered theme byte-identical.
//   - `@nanobpm/nano-app-schema/tokens` — this machine-readable map. The console's
//     drift test binds against it, and the Urban runtime imports it at
//     `gen-runtime` time to inline the palette into the App renderer CSS.
//
// A change here is a single edit that both consumers pick up; the drift test in
// the console (and the palette-vs-CSS lock in this package) fails the build if a
// consumer's copy ever disagrees again.

/**
 * The token vocabulary: the complete theming contract, in camelCase. Every
 * `--nano-*` custom property, every theme-pack override key, and every entry of
 * {@link NANO_PALETTE} is one of these — the console's `themes.ts` `TOKEN_KEYS`
 * must stay in lockstep with this list (its drift test asserts it).
 */
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

/** A single token key — one of {@link TOKEN_KEYS}. */
export type TokenKey = (typeof TOKEN_KEYS)[number];

/** A full palette: a colour value for every {@link TokenKey}. */
export type TokenPalette = Readonly<Record<TokenKey, string>>;

/** The two built-in appearances a palette is defined for. */
export type PaletteAppearance = "dark" | "light";

/**
 * The canonical palette values for both appearances. These are the ONE copy of
 * the `--nano-*` hex in the system: the console imports the generated CSS and the
 * Urban runtime inlines this map, so neither restates a literal colour.
 */
export const NANO_PALETTE: Readonly<Record<PaletteAppearance, TokenPalette>> = {
  dark: {
    app: "#0b0b10",
    panel: "#10101a",
    raised: "#16161f",
    inset: "#08080c",
    hover: "#1e1e2a",
    edge: "#24242f",
    edgeStrong: "#383848",
    text: "#f2f2f7",
    textMuted: "#a3a3b2",
    textFaint: "#6e6e80",
    accent: "#8b5cf6",
    accentStrong: "#a78bfa",
    accent2: "#22d3ee",
    onAccent: "#ffffff",
    ok: "#34d399",
    warn: "#fbbf24",
    danger: "#fb7185",
    info: "#38bdf8",
  },
  light: {
    app: "#f5f5f9",
    panel: "#fdfdfe",
    raised: "#ffffff",
    inset: "#ededf3",
    hover: "#e8e8f0",
    edge: "#e2e2ea",
    edgeStrong: "#c5c5d4",
    text: "#1a1a22",
    textMuted: "#565664",
    textFaint: "#8c8c9c",
    accent: "#7c3aed",
    accentStrong: "#6d28d9",
    accent2: "#0891b2",
    onAccent: "#ffffff",
    ok: "#059669",
    warn: "#b45309",
    danger: "#e11d48",
    info: "#0369a1",
  },
} as const;

/**
 * The CSS custom-property name for a token key: `edgeStrong` -> `--nano-edge-strong`,
 * `accent2` -> `--nano-accent-2`, `onAccent` -> `--nano-on-accent`.
 *
 * This mirrors the console's `themes.ts` `cssVar()` exactly so the generated
 * `tokens.css`, the console's runtime theme application, and the Urban runtime all
 * name the same custom properties.
 */
export function tokenCssVar(key: TokenKey): string {
  return `--nano-${key.replace(/[A-Z2]/g, (c) => (c === "2" ? "-2" : `-${c.toLowerCase()}`))}`;
}

/** Narrowing guard: is `value` a known {@link TokenKey}? */
export function isTokenKey(value: string): value is TokenKey {
  return (TOKEN_KEYS as readonly string[]).includes(value);
}

/** Options for {@link renderPaletteCss}. */
export interface RenderPaletteCssOptions {
  /**
   * Also emit a standalone `@media (prefers-color-scheme: light)` block that
   * applies the light palette to a `:root` with no `data-appearance` attribute.
   * The Urban runtime renders standalone (no console driving `data-appearance`),
   * so it follows the OS until the console themes it; the console itself always
   * sets `data-appearance`, so it does NOT want this block (and omitting it keeps
   * the console's rendered theme byte-identical to the pre-extraction inline CSS).
   * Defaults to `false`.
   */
  standaloneFallback?: boolean;
}

const NANO_HEADER = `/*
 * GENERATED from src/tokens.ts — do not edit by hand.
 *
 * The canonical --nano-* colour palette (issue #1005), as CSS custom properties.
 * Regenerate with \`npm run build\` (from spec-app/). The single source of truth is
 * NANO_PALETTE in src/tokens.ts; this file and the ./tokens map cannot drift (the
 * package build regenerates this file and a test locks it to the map).
 *
 * A theme is switched by \`data-appearance\` on <html>: dark is the default :root,
 * light applies under [data-appearance="light"]. Theme packs override the same
 * properties inline on <html>.
 */`;

function paletteBlock(theme: PaletteAppearance): string {
  return TOKEN_KEYS.map((key) => `  ${tokenCssVar(key)}: ${NANO_PALETTE[theme][key]};`).join("\n");
}

/**
 * Render {@link NANO_PALETTE} as the `tokens.css` artifact: the dark palette on
 * the default `:root` (and `:root[data-appearance="dark"]`) and the light palette
 * under `:root[data-appearance="light"]`. This is the exact generator for the
 * committed `spec-app/tokens.css` the console imports.
 */
export function renderPaletteCss(options: RenderPaletteCssOptions = {}): string {
  const blocks = [
    NANO_HEADER,
    `:root,\n:root[data-appearance="dark"] {\n  color-scheme: dark;\n\n${paletteBlock("dark")}\n}`,
    `:root[data-appearance="light"] {\n  color-scheme: light;\n\n${paletteBlock("light")}\n}`,
  ];
  if (options.standaloneFallback) {
    blocks.push(
      `@media (prefers-color-scheme: light) {\n` +
        `  /* Standalone only: follow the OS until a host sets data-appearance. */\n` +
        `  :root:not([data-appearance]) {\n    color-scheme: light;\n\n` +
        TOKEN_KEYS.map((key) => `    ${tokenCssVar(key)}: ${NANO_PALETTE.light[key]};`).join("\n") +
        `\n  }\n}`,
    );
  }
  return `${blocks.join("\n\n")}\n`;
}
