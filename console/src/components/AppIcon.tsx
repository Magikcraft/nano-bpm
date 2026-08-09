import { isSvgIcon } from "../lib/appRailIcon";

// Canonical renderer for an app-shipped rail/header icon, shared by the left
// rail (App.tsx `AppRailGlyph`) and the app-view header (AppView.tsx) so the
// classification *and* the theming have a single source of truth (no drift).
//
// An SVG app icon is a monochrome silhouette authored against `currentColor`
// (like the bundled rail glyphs), but an `<img>`-loaded SVG can't inherit
// `currentColor` — it resolves to the SVG's own default (black), so a
// dark-stroked icon vanishes on the dark theme. We therefore paint the theme
// foreground *through* the SVG as a CSS `mask`; a mask-image, like `<img>`,
// never executes scripted SVG, preserving the security property. Raster icons
// (png/jpeg) carry their own colour and are rendered via `<img>` unchanged.
//
// `sizeClass` sizes the glyph on each surface (the rail uses a small rounded
// square, the header a larger one). `onError` lets a caller fall back when the
// server 404s a missing/oversized/wrong-type icon.
export function AppIcon({
  name,
  icon,
  sizeClass,
  onError,
}: {
  name: string;
  icon: string | null | undefined;
  sizeClass: string;
  onError?: () => void;
}) {
  // The path is manifest-fixed; `v` cache-busts a changed icon hint.
  const src = `/console/app-view-icon/${encodeURIComponent(
    name,
  )}?v=${encodeURIComponent(icon ?? "")}`;

  if (isSvgIcon(icon)) {
    return (
      <>
        {/* A masked <span> can't report a failed load, so a hidden probe <img>
            drives the caller's fallback on a 404. It shares the browser cache
            with the mask below (same URL ⇒ one fetch). */}
        <img
          src={src}
          alt=""
          aria-hidden="true"
          className="hidden"
          onError={onError}
        />
        <span
          aria-hidden="true"
          className={`inline-block shrink-0 ${sizeClass}`}
          style={{
            backgroundColor: "currentColor",
            maskImage: `url("${src}")`,
            WebkitMaskImage: `url("${src}")`,
            maskSize: "contain",
            WebkitMaskSize: "contain",
            maskRepeat: "no-repeat",
            WebkitMaskRepeat: "no-repeat",
            maskPosition: "center",
            WebkitMaskPosition: "center",
          }}
        />
      </>
    );
  }
  return (
    <img
      src={src}
      alt=""
      aria-hidden="true"
      className={`shrink-0 object-contain ${sizeClass}`}
      onError={onError}
    />
  );
}
