// Unit tests for the app-rail icon-classification helpers. Node-native: run
// with `node --experimental-strip-types --test src/lib/appRailIcon.test.ts`.
import { test } from "node:test";
import assert from "node:assert/strict";
import { isAssetIcon, isSvgIcon } from "./appRailIcon.ts";

test("isAssetIcon: a path or a real extension is an app-shipped asset", () => {
  for (const icon of [
    "assets/icon.svg",
    "icon.png",
    "a/b/logo.jpeg",
    "x.SVG",
  ]) {
    assert.equal(isAssetIcon(icon), true, icon);
  }
});

test("isAssetIcon: a bundled glyph name (or a dotfile) is not an asset", () => {
  // A dotfile like ".svg" has no char before the dot, so — matching Rust's
  // Path::extension — it is a bundled name, not an asset path.
  for (const icon of ["workers", "topology", ".svg", "", null, undefined]) {
    assert.equal(isAssetIcon(icon), false, String(icon));
  }
});

test("isSvgIcon: only an SVG *asset* is tinted via the theme mask", () => {
  assert.equal(isSvgIcon("assets/icon.svg"), true);
  assert.equal(isSvgIcon("ICON.SVG"), true);
});

test("isSvgIcon: raster assets and bundled names are not tinted", () => {
  // Raster icons carry their own colour (rendered as <img>); bundled names and
  // dotfiles are not assets at all.
  for (const icon of ["icon.png", "logo.jpeg", "workers", ".svg", "", null]) {
    assert.equal(isSvgIcon(icon), false, String(icon));
  }
});
