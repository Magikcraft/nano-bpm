import { useState } from "react";
import { useMatch } from "react-router-dom";
import { RouteErrorBoundary } from "../components/RouteErrorBoundary";
import { lazyImport } from "../lib/lazyWithReload";

// The `__STUDIO__` literal (ADR 0034): in an `observe` build it folds to `false`,
// so esbuild drops the `import()` anchor and the AppView chunk is never produced.
// The guard must be the raw define, not the imported `IS_STUDIO` const — an
// imported binding only tree-shakes after transform, too late to stop the worker
// emit (see the matching note in App.tsx).
const AppView = __STUDIO__ ? lazyImport(() => import("./AppView")) : null;

/**
 * Keep-alive host for the embedded app views (issue #1040).
 *
 * AppView embeds a supervised app's whole UI in a sandboxed iframe. Mounted as a
 * plain `<Route element>`, React Router unmounted it on every navigation away
 * from `/apps/:name` — destroying the iframe document, so each return to the
 * rail entry re-fetched the entire app through the reverse proxy (painful on
 * low-bandwidth links) and lost all in-app state.
 *
 * Instead, every visited app gets ONE AppView instance here, OUTSIDE `<Routes>`,
 * kept mounted for the rest of the console session and hidden with
 * `display: none` while another route is active (hiding preserves an iframe;
 * only removing it from the DOM reloads it). The `/apps/:name` stub route in
 * App.tsx keeps the route matched — so the catch-all redirect doesn't fire —
 * while the visible instance is rendered from here. `AppView.active` tells an
 * instance whether it is the active route; effects that must not act from a
 * hidden view (the nano-navigate bridge) gate on it.
 *
 * No eviction: the visited set is bounded by the number of running apps, and
 * evicting a view would reintroduce the reload this host exists to remove.
 */
export function PersistentAppViews() {
  const activeName = useMatch("/apps/:name")?.params.name;
  const [visited, setVisited] = useState<string[]>([]);
  // Derived state, adjusted during render (React re-renders before paint): the
  // just-navigated-to app mounts in the same commit, so there's no blank frame
  // between the route change and the view appearing. Append-only: mount order
  // fixes each instance's React key for the life of the session. The functional
  // update reads the latest committed list rather than the render-time closure,
  // so rapid successive navigations can't overwrite each other's appends.
  if (AppView && activeName && !visited.includes(activeName)) {
    setVisited((prev) =>
      prev.includes(activeName) ? prev : [...prev, activeName],
    );
  }
  if (!AppView) return null;
  return (
    <>
      {visited.map((name) => {
        const active = name === activeName;
        return (
          // `h-full` preserves the height chain <main> → AppView's
          // `flex h-full flex-col` root that the routed render had.
          <div key={name} className={active ? "h-full" : "hidden"}>
            <RouteErrorBoundary resetKey={name}>
              <AppView name={name} active={active} />
            </RouteErrorBoundary>
          </div>
        );
      })}
    </>
  );
}
