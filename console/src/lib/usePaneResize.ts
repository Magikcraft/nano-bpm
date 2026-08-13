import { useEffect, useRef, useState } from "react";

/**
 * A pointer-drag pane resizer whose size persists across reloads.
 *
 * The size (a pixel width for `axis:"x"`, a pixel height for `axis:"y"`) is
 * seeded from `localStorage[storageKey]` — same `nano.*` convention as
 * `nano.railCollapsed` in App.tsx — and written back on every change, so the
 * user's layout survives a page reload. Every read/write is wrapped so a
 * private-mode / disabled-storage browser degrades to the in-memory default
 * rather than throwing.
 *
 * `max` may be a getter so the ceiling can track the viewport (e.g. "container
 * height minus a minimum for the pane below"); the size is re-clamped on window
 * resize so a shrunk window can't leave a pane larger than its container.
 *
 * `invert` flips the drag direction: with the handle on the pane's *trailing*
 * edge (default) dragging away from the pane grows it; set `invert` when the
 * handle sits on the pane's *leading* edge so the gesture still feels natural.
 */
export interface PaneResizeOptions {
  /** localStorage key, e.g. `"nano.explorer.listWidth"`. */
  storageKey: string;
  /** `"x"` resizes width (drag horizontally); `"y"` resizes height. */
  axis: "x" | "y";
  /** Default size in px when nothing is stored. */
  initial: number;
  /** Minimum size in px. */
  min: number;
  /** Maximum size in px, or a getter evaluated at clamp time. */
  max: number | (() => number);
  /** Invert the drag delta (handle on the pane's leading edge). */
  invert?: boolean;
  /** Keyboard nudge step in px (arrow keys on the handle). Defaults to 16. */
  step?: number;
}

export interface PaneResize {
  /** Current pane size in px (apply as inline `width`/`height`). */
  size: number;
  /** Minimum size in px (for `aria-valuemin` on the handle). */
  min: number;
  /** Current maximum size in px, resolved at read time (for `aria-valuemax`). */
  max: number;
  /** True while a drag is in progress (for handle styling). */
  dragging: boolean;
  /** Attach to the resize handle's `onPointerDown`. */
  onPointerDown: (e: React.PointerEvent) => void;
  /** Attach to the resize handle's `onKeyDown` for accessible keyboard resize. */
  onKeyDown: (e: React.KeyboardEvent) => void;
}

function clamp(value: number, min: number, max: number): number {
  if (max < min) return min;
  return Math.min(Math.max(value, min), max);
}

/** Clamp a candidate pane size into `[min, max]`, collapsing an inverted range
 *  (max < min, e.g. a viewport smaller than the minimum) to `min`. Exported for
 *  unit testing. */
export function clampSize(value: number, min: number, max: number): number {
  return clamp(value, min, max);
}

/** The pane size after a drag: the starting size plus the pointer delta (negated
 *  when the handle is on the pane's leading edge), clamped. Pure and
 *  coordinate-agnostic — the caller passes the already-projected 1-D delta. */
export function sizeFromDelta(
  startSize: number,
  delta: number,
  invert: boolean,
  min: number,
  max: number,
): number {
  return clamp(startSize + delta * (invert ? -1 : 1), min, max);
}

/** The pane size after an arrow-key press on the handle, or `null` when the key
 *  is not an axis-relevant arrow. For `axis:"x"` Left/Right shrink/grow; for
 *  `axis:"y"` Up/Down shrink/grow. `invert` flips that mapping the same way it
 *  flips the drag delta, so on a leading-edge handle the arrow that physically
 *  moves the separator toward the pane grows it — keeping keyboard and drag
 *  consistent. Pure. */
export function sizeFromKey(
  size: number,
  key: string,
  axis: "x" | "y",
  step: number,
  min: number,
  max: number,
  invert = false,
): number | null {
  const dec = axis === "x" ? "ArrowLeft" : "ArrowUp";
  const inc = axis === "x" ? "ArrowRight" : "ArrowDown";
  if (key !== dec && key !== inc) return null;
  const delta = (key === inc ? step : -step) * (invert ? -1 : 1);
  return clamp(size + delta, min, max);
}

function readStored(key: string, fallback: number): number {
  try {
    const raw = localStorage.getItem(key);
    if (raw == null) return fallback;
    const n = Number.parseFloat(raw);
    return Number.isFinite(n) ? n : fallback;
  } catch {
    return fallback;
  }
}

export function usePaneResize(opts: PaneResizeOptions): PaneResize {
  const { storageKey, axis, initial, min, invert = false, step = 16 } = opts;
  const resolveMax = () =>
    typeof opts.max === "function" ? opts.max() : opts.max;

  const [size, setSize] = useState(() =>
    clamp(readStored(storageKey, initial), min, resolveMax()),
  );
  const [dragging, setDragging] = useState(false);
  const startPos = useRef(0);
  const startSize = useRef(0);

  // Persist on every change so the layout survives a reload.
  useEffect(() => {
    try {
      localStorage.setItem(storageKey, String(Math.round(size)));
    } catch {
      // storage unavailable (private mode); keep the in-memory size.
    }
  }, [storageKey, size]);

  // Re-clamp when the viewport shrinks so a viewport-relative `max` can't leave
  // the pane oversized after a window resize.
  useEffect(() => {
    if (typeof window === "undefined") return;
    const onResize = () => setSize((s) => clamp(s, min, resolveMax()));
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
    // resolveMax/opts.max are stable at each call site; intentionally not deps.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [min]);

  const onPointerDown = (e: React.PointerEvent) => {
    // Only react to the primary button / a touch or pen contact.
    if (e.button !== 0) return;
    e.preventDefault();
    startPos.current = axis === "x" ? e.clientX : e.clientY;
    startSize.current = size;
    setDragging(true);

    const move = (ev: PointerEvent) => {
      const cur = axis === "x" ? ev.clientX : ev.clientY;
      setSize(
        sizeFromDelta(
          startSize.current,
          cur - startPos.current,
          invert,
          min,
          resolveMax(),
        ),
      );
    };
    const up = () => {
      setDragging(false);
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
      window.removeEventListener("pointercancel", up);
      document.body.style.removeProperty("cursor");
      document.body.style.removeProperty("user-select");
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
    // A pointercancel (touch interruption, OS gesture, tab switch, …) must run
    // the same cleanup as pointerup, or the drag leaves dangling listeners and
    // a stuck resize cursor / user-select:none.
    window.addEventListener("pointercancel", up);
    // Keep the resize cursor and suppress text selection for the whole drag,
    // even when the pointer leaves the thin handle.
    document.body.style.cursor = axis === "x" ? "col-resize" : "row-resize";
    document.body.style.userSelect = "none";
  };

  const onKeyDown = (e: React.KeyboardEvent) => {
    const next = sizeFromKey(
      size,
      e.key,
      axis,
      step,
      min,
      resolveMax(),
      invert,
    );
    if (next == null) return;
    e.preventDefault();
    setSize(next);
  };

  return {
    size,
    min,
    max: resolveMax(),
    dragging,
    onPointerDown,
    onKeyDown,
  };
}
