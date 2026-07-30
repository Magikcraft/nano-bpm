# Product tour — driver.js spike (issue #393)

A first-run product tour for the console, built on [driver.js](https://driverjs.com)
(MIT, zero-dep, ~5kb). This is a **spike** to evaluate driver.js against a
`@reactour/tour` spike before we commit to one.

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
| `useProductTour.ts` | The hook: builds the driver, navigates the router between steps, and persists a `localStorage` "seen" flag so it auto-starts only on first run. Returns `startTour` / `resetTour`. |
| `tour.css` | Popover theming via the app's own `--nano-*` tokens, so it tracks dark/light mode. |

Targets are stable `data-tour="…"` anchors (`nav-*`, `new-project`, `run`), not
brittle text/class selectors.

## Wiring

`App.tsx` calls `useProductTour({ autoStart: true })` and renders a **Take a tour**
rail button (`startTour`) to replay it.

## Try it

```bash
npm run dev
```

The tour auto-starts on first load. To replay after that, click **Take a tour**
in the sidebar, or clear the flag: `localStorage.removeItem("nano.tour.v1.seen")`.

## Evaluating vs @reactour

Compare on: popover positioning over the transformed bpmn-js canvas, route-spanning
ergonomics, theming effort, and API feel. See issue #393 for the follow-up spike.
