# Product tour — @reactour/tour spike (issue #393)

A first-run product tour for the console, built on
[@reactour/tour](https://docs.react.tours) (MIT). This is the **B side** of an
A/B spike — the A side is the [driver.js](https://driverjs.com) spike on
`feat/console-product-tour`. Same journey, same `data-tour` anchors, same
`steps.ts`; only the tour engine differs.

## The reactour shape (vs driver.js)

reactour is **declarative**: the tour is a React context you mount high in the
tree (`<TourProvider>`), and you read/drive it through `useTour()`. That has two
consequences the driver.js spike doesn't have:

- **A provider must wrap the app** (`ProductTourProvider` in `main.tsx`) so any
  view can call `useTour()`. driver.js needs no provider.
- **Two capabilities are bolted on by us**, because reactour doesn't have them:
  - _route navigation_ — reactour is DOM-only, so `useProductTour` watches
    `currentStep` and drives react-router; each step's `mutationObservables`
    then waits for the target to mount on the new route (reactour's analogue of
    driver.js's `waitForElement`).
  - _skip-missing-target_ — reactour has no `skipMissingElement`, so we filter
    the step list at start (`isStepReachable`): an anchored step with no route
    whose target isn't in the DOM (e.g. **Run**, workspace-only) is dropped.

Positioning is also coarser — reactour picks a `side`, with no separate `align`
axis, so `TourStep.align` is ignored here.

## Pieces

| File | Role |
| ---- | ---- |
| `steps.ts` | Profile-aware step list. **Identical** to the driver.js spike — the journey is engine-agnostic. |
| `reactourStep.tsx` | Single source of truth converting `TourStep → @reactour/tour StepType`. Used by both the provider and the hook so the mapping never drifts. |
| `ProductTourProvider.tsx` | Mounts `<TourProvider>` with token-themed popover/mask `styles` and text Back/Next/Done controls (replacing reactour's default arrows + dots). |
| `useProductTour.tsx` | Route navigation, skip-missing filter, first-run `localStorage` flag; returns `startTour` / `resetTour`. Same public shape as the driver.js hook, so `App.tsx` is unchanged. |
| `tour.css` | Content + control theming via `--nano-*` tokens (tracks dark/light mode). |

## Try it

```bash
npm run dev
```

The tour auto-starts on first load. Replay via **Take a tour** in the sidebar,
or clear the flag: `localStorage.removeItem("nano.tour.v1.seen")`.

## Compare against driver.js

Run both spikes on different ports and A/B them on: popover positioning over the
transformed bpmn-js canvas, route-spanning ergonomics, theming effort, bundle
size, and how much glue code each library needs. See issue #393.
