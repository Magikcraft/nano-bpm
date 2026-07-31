# Product tour (driver.js)

A first-run product tour for the console, built on [driver.js](https://driverjs.com)
(MIT, zero-dep, ~5kb). driver.js was chosen over `@reactour/tour` via an A/B
evaluation (issue #393) — see [Chosen over @reactour/tour](#chosen-over-reactourtour-issue-393)
below for the rationale.

## Why driver.js

The console journey **spans react-router routes** and targets elements that
**mount asynchronously** over CSS-transformed canvases (bpmn-js / monaco). driver.js
gives us two things that fit that exactly, for free:

- `waitForElement` — waits for a step's target to appear before showing it (the
  generalised "canvas ready" gate).
- `skipMissingElement` — skips a step whose target never appears, so the tour
  degrades gracefully (e.g. the **Run** step is skipped on a fresh install with
  no open project).

The only thing driver.js can't do is drive react-router, so the hook does that.

## Pieces

| File | Role |
| ---- | ---- |
| `steps.ts` | Profile-aware step list (`CONSOLE_PROFILE`: studio vs observe). Each step names the `route` it needs and the `selector` to highlight. Single source of truth — no duplication across profiles. |
| `tourAnchors.ts` | The `data-tour` anchor names + `navAnchor()` / `tourSelector()` helpers. Both the steps (selectors) and the views (`data-tour` attributes) derive from here, so a rename can't silently break a step. |
| `steps.test.ts` | `node --test` guard: structural invariants + that each profile only targets anchors that render in it (e.g. observe never points at the studio-only Projects nav). |
| `useProductTour.ts` | The hook: builds the driver, navigates the router between steps, and persists a `localStorage` "seen" flag so it auto-starts only on first run. Returns `startTour` / `resetTour`. |
| `tour.css` | Popover theming via the app's own `--nano-*` tokens, so it tracks dark/light mode. |

Targets are stable `data-tour="…"` anchors defined once in `tourAnchors.ts`
(`nav-*`, `new-project`, `run`), not brittle text/class selectors.

## Wiring

`App.tsx` calls `useProductTour({ autoStart: true })` and renders a **Take a tour**
rail button (`startTour`) to replay it.

## Try it

```bash
npm run dev
```

The tour auto-starts on first load. To replay after that, click **Take a tour**
in the sidebar, or clear the flag: `localStorage.removeItem("nano.tour.v1.seen")`.

## Tests

```bash
npm test          # includes steps.test.ts (the anchor/profile guard)
npm run typecheck
```

## Chosen over @reactour/tour (issue #393)

driver.js won the A/B: ~½ the bundle (~7 KB gz, **zero deps**), framework-agnostic
(no provider), `waitForElement` + `skipMissingElement` built in, finer positioning
(side + align), and far healthier upstream (1.4M dl/wk vs 167k; released weekly vs
last publish >1 yr ago). reactour's only edge — built-in focus-lock a11y — didn't
outweigh it. The reactour spike lived on `feat/console-tour-reactour` (PR #397,
closed).
