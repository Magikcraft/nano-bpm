// Regression guard for the "runner rebuilt on navigation" defect class.
//
// The tour runner owns the live driver.js instance for an in-flight journey, and
// `useProductTour` disposes it in a cleanup effect keyed on the runner's identity
// (`useEffect(() => () => runner.dispose(), [runner])`). So if the runner's
// `useMemo` is rebuilt mid-journey, that cleanup fires and silently tears down a
// running tour.
//
// React Router's `useNavigate()` returns a NEW function identity on every
// navigation, so listing `navigate` in the runner's memo deps rebuilds the runner
// the instant a journey navigates between steps — which killed the first journey
// that ever did (the local-dev journey's `/explorer` → `/traces` step). The fix
// routes navigation through a ref (`navigateRef.current`), exactly as the pathname
// already is, and drops the unstable callback from the deps.
//
// There is no React render-test harness in this package (the suite is pure Node),
// so this guards the invariant structurally instead: the runner must reach the
// router only through refs, never through a value that changes on navigation.
//
// Node-native: `node --experimental-strip-types --test src/lib/tour/useProductTour.stability.test.ts`.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const source = readFileSync(
  fileURLToPath(new URL("./useProductTour.ts", import.meta.url)),
  "utf8",
);

/** The `createJourneyRunner({ ... })` argument object and its `useMemo` deps. */
function runnerMemo(): { factoryArg: string; deps: string } {
  const factoryStart = source.indexOf("createJourneyRunner({");
  assert.notEqual(
    factoryStart,
    -1,
    "expected a createJourneyRunner({ ... }) call",
  );
  // The memo closes with `}), [deps])`; find the deps array that follows.
  const depsMatch = source
    .slice(factoryStart)
    .match(/\n\s*\}\),\s*(\[[^\]]*\])/);
  assert.ok(depsMatch, "expected the runner useMemo dependency array");
  const relativeIndex = depsMatch.index;
  assert.ok(relativeIndex !== undefined, "expected a match offset");
  const factoryArg = source.slice(factoryStart, factoryStart + relativeIndex);
  return { factoryArg, deps: depsMatch[1] };
}

test("the runner memo does not depend on the navigation-churning `navigate`", () => {
  const { deps } = runnerMemo();
  const listed = deps
    .slice(1, -1)
    .split(",")
    .map((d) => d.trim())
    .filter(Boolean);
  assert.ok(
    !listed.includes("navigate"),
    `\`navigate\` must not be a runner memo dep (it changes identity on every ` +
      `navigation, rebuilding the runner and disposing an in-flight journey); ` +
      `route through navigateRef instead. Deps found: ${deps}`,
  );
});

test("the runner reaches the router only through refs", () => {
  const { factoryArg } = runnerMemo();
  assert.match(
    factoryArg,
    /navigate:\s*\(route\)\s*=>\s*navigateRef\.current\(route\)/,
    "runner navigate must delegate to navigateRef.current, not the raw navigate",
  );
  assert.match(
    factoryArg,
    /getRoute:\s*\(\)\s*=>\s*pathnameRef\.current/,
    "runner getRoute must read pathnameRef.current",
  );
});
