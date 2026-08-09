// Pure, React-free helpers for classifying a running app's left-rail `icon`
// hint, extracted from App.tsx so they can be unit-tested with `node --test`
// (see appRailIcon.test.ts) without a DOM.

// A running app's left-rail `icon` hint is either a *bundled glyph name*
// (resolved against the console's icon set) or a *project asset path* the app
// ships itself (e.g. `assets/icon.svg`), served path-guarded from
// `/console/app-view-icon/<name>`. Heuristic mirrored server-side
// (`app_view_icon_is_asset`): a separator, or a real extension (a dot with a
// non-separator char before it, so a dotfile like `.svg` is NOT an extension —
// matching Rust's `Path::extension`), ⇒ asset path.
export function isAssetIcon(icon: string | null | undefined): boolean {
  return !!icon && (icon.includes("/") || /[^/]\.[a-z0-9]+$/i.test(icon));
}

// Whether an asset icon path is an SVG. SVG rail icons are monochrome
// silhouettes authored against `currentColor` (like the bundled glyphs), so the
// rail tints them with the theme foreground via a CSS mask — an `<img>`-loaded
// SVG can't inherit `currentColor`, so a dark-stroked icon otherwise vanishes on
// the dark theme. Raster icons (png/jpeg) carry their own colour and are shown
// as-is. Only meaningful for an asset path (see `isAssetIcon`).
export function isSvgIcon(icon: string | null | undefined): boolean {
  return isAssetIcon(icon) && /\.svg$/i.test(icon!);
}
