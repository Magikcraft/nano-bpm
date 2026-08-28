/**
 * The token vocabulary: the complete theming contract, in camelCase. Every
 * `--nano-*` custom property, every theme-pack override key, and every entry of
 * {@link NANO_PALETTE} is one of these — the console's `themes.ts` `TOKEN_KEYS`
 * must stay in lockstep with this list (its drift test asserts it).
 */
export declare const TOKEN_KEYS: readonly [
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
	"info"
];
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
export declare const NANO_PALETTE: Readonly<Record<PaletteAppearance, TokenPalette>>;
/**
 * The CSS custom-property name for a token key: `edgeStrong` -> `--nano-edge-strong`,
 * `accent2` -> `--nano-accent-2`, `onAccent` -> `--nano-on-accent`.
 *
 * This mirrors the console's `themes.ts` `cssVar()` exactly so the generated
 * `tokens.css`, the console's runtime theme application, and the Urban runtime all
 * name the same custom properties.
 */
export declare function tokenCssVar(key: TokenKey): string;
/** Narrowing guard: is `value` a known {@link TokenKey}? */
export declare function isTokenKey(value: string): value is TokenKey;
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
/**
 * Render {@link NANO_PALETTE} as the `tokens.css` artifact: the dark palette on
 * the default `:root` (and `:root[data-appearance="dark"]`) and the light palette
 * under `:root[data-appearance="light"]`. This is the exact generator for the
 * committed `spec-app/tokens.css` the console imports.
 */
export declare function renderPaletteCss(options?: RenderPaletteCssOptions): string;

export {};
