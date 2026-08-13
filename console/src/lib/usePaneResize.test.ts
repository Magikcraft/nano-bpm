// Unit tests for the pure geometry behind `usePaneResize` (the Explorer's
// resizable, reload-persistent process-list and model-space panes). Node-native:
// run with `node --experimental-strip-types --test src/lib/usePaneResize.test.ts`.
// The React/DOM/localStorage wiring is exercised in the browser; these cover the
// clamping, drag-delta, and keyboard math that decide the actual sizes.
import { test } from "node:test";
import assert from "node:assert/strict";
import { clampSize, sizeFromDelta, sizeFromKey } from "./usePaneResize.ts";

test("clampSize: holds a value within [min, max]", () => {
  assert.equal(clampSize(300, 256, 640), 300);
  assert.equal(clampSize(100, 256, 640), 256); // below min
  assert.equal(clampSize(900, 256, 640), 640); // above max
  assert.equal(clampSize(256, 256, 640), 256); // on min
  assert.equal(clampSize(640, 256, 640), 640); // on max
});

test("clampSize: an inverted range (viewport smaller than min) collapses to min", () => {
  // e.g. a very narrow window where `max = innerWidth - 480` falls below `min`.
  assert.equal(clampSize(400, 256, 100), 256);
});

test("sizeFromDelta: a trailing-edge handle grows with a positive delta", () => {
  // Explorer list: handle on the list's right edge, dragging right widens it.
  assert.equal(sizeFromDelta(448, 60, false, 256, 640), 508);
  assert.equal(sizeFromDelta(448, -60, false, 256, 640), 388);
});

test("sizeFromDelta: an inverted (leading-edge) handle reverses the delta", () => {
  assert.equal(sizeFromDelta(448, 60, true, 256, 640), 388);
  assert.equal(sizeFromDelta(448, -60, true, 256, 640), 508);
});

test("sizeFromDelta: the result is clamped to the bounds", () => {
  assert.equal(sizeFromDelta(620, 100, false, 256, 640), 640); // hits max
  assert.equal(sizeFromDelta(280, -100, false, 256, 640), 256); // hits min
});

test("sizeFromKey (x axis): ArrowRight grows, ArrowLeft shrinks, by one step", () => {
  assert.equal(sizeFromKey(448, "ArrowRight", "x", 16, 256, 640), 464);
  assert.equal(sizeFromKey(448, "ArrowLeft", "x", 16, 256, 640), 432);
});

test("sizeFromKey (y axis): ArrowDown grows the model space, ArrowUp shrinks it", () => {
  assert.equal(sizeFromKey(288, "ArrowDown", "y", 16, 144, 640), 304);
  assert.equal(sizeFromKey(288, "ArrowUp", "y", 16, 144, 640), 272);
});

test("sizeFromKey: an off-axis or non-arrow key is ignored (returns null)", () => {
  // Vertical handle ignores horizontal arrows and vice-versa.
  assert.equal(sizeFromKey(288, "ArrowLeft", "y", 16, 144, 640), null);
  assert.equal(sizeFromKey(448, "ArrowUp", "x", 16, 256, 640), null);
  assert.equal(sizeFromKey(448, "Enter", "x", 16, 256, 640), null);
});

test("sizeFromKey: a nudge past a bound is clamped", () => {
  assert.equal(sizeFromKey(636, "ArrowRight", "x", 16, 256, 640), 640);
  assert.equal(sizeFromKey(260, "ArrowLeft", "x", 16, 256, 640), 256);
});
