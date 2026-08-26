// The canonical responsive-presentation vocabulary for composed pages (ADR 0042).
//
// A page authored in the console Page Composer is rendered, unchanged, by the
// App-side runtime (nano-ide `urban`). The runtime already renders a set of
// *responsive* page props — a mobile breakpoint, per-column mobile priorities, a
// card-grid nav, per-item nav groups, a grid's mobile presentation, and a
// page-level mobile layout switch — that the Console/schema could not express, so
// the vocabulary drifted silently across the repo boundary (issue #1005). This
// module retires that drift: it is the single source of truth for the responsive
// vocabulary, in exactly the style of {@link ./page-nodes.ts}'s `PAGE_NODE_TYPES`.
//
// Both surfaces derive from the constants here instead of restating them:
//   - the console Page Composer / `pageSchema` binds a compile-time parity lock
//     against these exports, so a prop enum present here but unhandled by the
//     composer (or vice versa) fails to compile — the same guard that already
//     covers node *types* now covers node *props*;
//   - the Urban runtime imports the same constants (and inlines
//     {@link MOBILE_MAX_WIDTH}), so the Composer and the runtime renderer can
//     never disagree on the responsive vocabulary.
//
// Adding or changing a value here is therefore a single edit that forces both
// consumers to catch up — they can no longer silently drift.

/**
 * The single canonical mobile breakpoint: pages render their mobile presentation
 * at or below this viewport width. This is the ONE breakpoint constant in the
 * system — consumers (the console's `useIsNarrow`, the Urban runtime's
 * `isNarrow()`) MUST import it rather than restating a literal `640px`/`640`, so
 * the breakpoint can never fork.
 */
export const MOBILE_MAX_WIDTH = "640px" as const;

/** The type of {@link MOBILE_MAX_WIDTH} — the canonical breakpoint literal. */
export type MobileMaxWidth = typeof MOBILE_MAX_WIDTH;

/**
 * How a grid column is presented on mobile (`column.mobile.priority`):
 *   - `"primary"` — shown as the card's primary line;
 *   - `"chip"` — shown as a secondary chip/badge on the card;
 *   - `"hidden"` — dropped from the mobile card entirely.
 */
export const COLUMN_MOBILE_PRIORITIES = ["primary", "chip", "hidden"] as const;

/** A column's mobile priority — one of {@link COLUMN_MOBILE_PRIORITIES}. */
export type ColumnMobilePriority = (typeof COLUMN_MOBILE_PRIORITIES)[number];

/**
 * The mobile presentation hints on a grid column (`column.mobile`) — the shape
 * the Urban runtime already reads (`column.mobile.priority`,
 * `column.mobile.label`).
 */
export interface ColumnMobile {
  /** How this column is surfaced on the mobile card. */
  priority: ColumnMobilePriority;
  /** An optional short label to use in place of the column header on mobile. */
  label?: string;
}

/** Narrowing guard: is `value` a known column mobile priority? */
export function isColumnMobilePriority(
  value: unknown,
): value is ColumnMobilePriority {
  return (
    typeof value === "string" &&
    (COLUMN_MOBILE_PRIORITIES as readonly string[]).includes(value)
  );
}

/**
 * A `nav` node's layout variant (`nav.props.variant`):
 *   - `"bar"` — a horizontal top bar;
 *   - `"rail"` — a vertical side rail;
 *   - `"cards"` — a launcher grid of cards (the mobile-first home).
 */
export const NAV_VARIANTS = ["bar", "rail", "cards"] as const;

/** A nav variant — one of {@link NAV_VARIANTS}. */
export type NavVariant = (typeof NAV_VARIANTS)[number];

/** Narrowing guard: is `value` a known nav variant? */
export function isNavVariant(value: unknown): value is NavVariant {
  return (
    typeof value === "string" &&
    (NAV_VARIANTS as readonly string[]).includes(value)
  );
}

/**
 * How a `nav` node collapses items that do not fit (`nav.props.overflow`):
 *   - `"menu"` — overflowing items move into a hamburger/overflow menu.
 * Omitting `overflow` keeps the default (no collapse; items wrap/scroll).
 */
export const NAV_OVERFLOW_MODES = ["menu"] as const;

/** A nav overflow mode — one of {@link NAV_OVERFLOW_MODES}. */
export type NavOverflow = (typeof NAV_OVERFLOW_MODES)[number];

/** Narrowing guard: is `value` a known nav overflow mode? */
export function isNavOverflow(value: unknown): value is NavOverflow {
  return (
    typeof value === "string" &&
    (NAV_OVERFLOW_MODES as readonly string[]).includes(value)
  );
}

/**
 * Which group a nav item belongs to (`nav.items[].group`):
 *   - `"primary"` — rendered as a first-class card / top-level item;
 *   - `"secondary"` — demoted (e.g. into the overflow menu).
 */
export const NAV_ITEM_GROUPS = ["primary", "secondary"] as const;

/** A nav item group — one of {@link NAV_ITEM_GROUPS}. */
export type NavItemGroup = (typeof NAV_ITEM_GROUPS)[number];

/**
 * The default nav item group. An item that annotates no `group` is `"primary"`,
 * so an app that annotates nothing gets every item rendered as a card.
 */
export const NAV_ITEM_GROUP_DEFAULT: NavItemGroup = "primary";

/** Narrowing guard: is `value` a known nav item group? */
export function isNavItemGroup(value: unknown): value is NavItemGroup {
  return (
    typeof value === "string" &&
    (NAV_ITEM_GROUPS as readonly string[]).includes(value)
  );
}

/**
 * How a `dataGrid` presents itself on mobile
 * (`dataGrid.props.mobile.presentation`):
 *   - `"cards"` — one card per row (the mobile-first default);
 *   - `"table"` — stay tabular (escape hatch for a grid that must be a table).
 */
export const DATA_GRID_MOBILE_PRESENTATIONS = ["cards", "table"] as const;

/** A grid's mobile presentation — one of {@link DATA_GRID_MOBILE_PRESENTATIONS}. */
export type DataGridMobilePresentation =
  (typeof DATA_GRID_MOBILE_PRESENTATIONS)[number];

/**
 * The default grid mobile presentation. A grid that annotates no
 * `mobile.presentation` renders as `"cards"` on mobile.
 */
export const DATA_GRID_MOBILE_PRESENTATION_DEFAULT: DataGridMobilePresentation =
  "cards";

/** The mobile presentation hints on a grid (`dataGrid.props.mobile`). */
export interface DataGridMobile {
  /** How the grid renders below {@link MOBILE_MAX_WIDTH}. */
  presentation: DataGridMobilePresentation;
}

/** Narrowing guard: is `value` a known grid mobile presentation? */
export function isDataGridMobilePresentation(
  value: unknown,
): value is DataGridMobilePresentation {
  return (
    typeof value === "string" &&
    (DATA_GRID_MOBILE_PRESENTATIONS as readonly string[]).includes(value)
  );
}

/**
 * The page-level mobile layout variant (`layout.mobile`) — the Tier-2 switch the
 * runtime's `isNarrow()` anticipates (Tier-1 is the pure CSS reflow at
 * {@link MOBILE_MAX_WIDTH}; Tier-2 is this explicit, authored variant):
 *   - `"stack"` — the natural vertical stack of the page's nodes;
 *   - `"cards"` — a launcher/card mobile layout.
 */
export const LAYOUT_MOBILE_VARIANTS = ["stack", "cards"] as const;

/** A page-level mobile layout variant — one of {@link LAYOUT_MOBILE_VARIANTS}. */
export type LayoutMobileVariant = (typeof LAYOUT_MOBILE_VARIANTS)[number];

/**
 * The default page mobile layout variant. A page that declares `layout` but no
 * `layout.mobile` reflows as `"stack"`.
 */
export const LAYOUT_MOBILE_VARIANT_DEFAULT: LayoutMobileVariant = "stack";

/** Narrowing guard: is `value` a known page mobile layout variant? */
export function isLayoutMobileVariant(
  value: unknown,
): value is LayoutMobileVariant {
  return (
    typeof value === "string" &&
    (LAYOUT_MOBILE_VARIANTS as readonly string[]).includes(value)
  );
}

/**
 * The page-level layout hints (`page.layout`). Its `mobile` field is the Tier-2
 * variant hook; omitting it leaves the runtime on the Tier-1 CSS reflow.
 */
export interface PageLayout {
  /** The Tier-2 mobile layout variant (see {@link LAYOUT_MOBILE_VARIANTS}). */
  mobile?: LayoutMobileVariant;
}
