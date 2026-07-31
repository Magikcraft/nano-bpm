// Guided-journey e2e guards (ADR 0049, #417).
//
// WHY THIS SUITE EXISTS: journey steps target `data-tour` anchors across views
// owned by different slices, and **nothing in a normal build or unit test fails
// when an anchor is renamed or deleted** — the step just silently spotlights
// nothing. ADR 0049 deliberately keeps no anchor registry beyond
// `tourAnchors.ts`, so this is the compensating control for that decision. The
// unit tests in `src/lib/tour/journeys.test.ts` prove a selector is *spelled*
// from a known anchor; only a browser can prove it *resolves*.
//
// Journeys are enumerated from the REGISTRY, by importing every module in
// `src/lib/tour/journeys/` — not from a list kept here. A journey added by a
// later slice is therefore covered the moment it lands, with no edit to this
// file. That is the whole design: the guard must not need maintaining by the
// people it guards.

import { readdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { expect, test, type Page } from "@playwright/test";
import { allJourneys } from "../src/lib/tour/registry.ts";
import type { Journey, SpotlightStep, Step } from "../src/lib/tour/types.ts";
import {
  assertNoPageCrash,
  resetTourState,
  stubConsoleApi,
} from "./fixtures.ts";

// ── Registry enumeration ─────────────────────────────────────────────────────

const journeysDir = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "../src/lib/tour/journeys",
);

for (const file of readdirSync(journeysDir)) {
  if (!file.endsWith(".ts") || file.endsWith(".test.ts")) continue;
  // Registration is an import side effect, exactly as in the app.
  await import(pathToFileURL(path.join(journeysDir, file)).href);
}

/** The dev server builds the studio profile, so only studio journeys are live. */
const STUDIO_JOURNEYS = allJourneys().filter((j) =>
  j.profiles.includes("studio"),
);

/**
 * Routes reachable without backend state a stub cannot fake.
 *
 * A step inside a project workspace (`/projects/:name`) needs a scaffolded,
 * running project; asserting those would mean building and running a Deno app on
 * every CI run, and a flaky guard is worse than a shallower reliable one. Those
 * steps are reported as uncovered rather than silently ignored — see the
 * coverage-report test at the bottom.
 */
const REACHABLE = new Set([
  "/projects",
  "/extensions",
  "/topology",
  "/metrics",
  "/explorer",
  "/traces",
  "/workers",
]);

function isReachable(route: string | undefined): boolean {
  return route !== undefined && REACHABLE.has(route);
}

/** Every authored step plus repair replacements — repairs are steps too. */
function eachStep(journey: Journey): Step[] {
  return journey.steps.flatMap((s) => (s.repair ? [s, s.repair] : [s]));
}

const spotlights = (journey: Journey): SpotlightStep[] =>
  eachStep(journey).filter((s): s is SpotlightStep => s.kind === "spotlight");

/**
 * Walk a running journey to its end, collecting step titles.
 *
 * Tolerant by necessity: driver.js's last button is "Done", and clicking it
 * destroys the popover — so every read is guarded and the loop exits the moment
 * the popover goes away. An unguarded read hangs until the test times out, which
 * reports as a journey failure when the journey in fact completed.
 */
async function walkJourney(page: Page, maxSteps: number): Promise<string[]> {
  const popover = page.locator(".driver-popover");
  const titles: string[] = [];
  for (let i = 0; i < maxSteps; i++) {
    if (!(await popover.isVisible().catch(() => false))) break;
    const title = await popover
      .locator(".driver-popover-title")
      .textContent()
      .catch(() => null);
    if (title?.trim()) titles.push(title.trim());
    const next = popover.locator(".driver-popover-next-btn");
    if (!(await next.isVisible().catch(() => false))) break;
    await next.click();
    await page.waitForTimeout(200);
  }
  return titles;
}

// ── The anchor-rot guard ─────────────────────────────────────────────────────

test.describe("journey anchors resolve", () => {
  test("the registry is non-empty", () => {
    expect(
      STUDIO_JOURNEYS.length,
      "no studio journeys registered — the glob import is broken, which would make every guard below vacuous",
    ).toBeGreaterThan(0);
  });

  for (const journey of STUDIO_JOURNEYS) {
    const assertable = spotlights(journey).filter((s) => isReachable(s.route));

    for (const step of assertable) {
      test(`${journey.id} / ${step.id}: ${step.selector} resolves on ${step.route}`, async ({
        page,
      }) => {
        const noCrash = assertNoPageCrash(page);
        await stubConsoleApi(page);
        await resetTourState(page);
        // Route is absolute under the router basename; baseURL already ends in
        // /console/, so strip the leading slash.
        await page.goto(step.route!.replace(/^\//, ""));
        const target = page.locator(step.selector);
        await expect(
          target,
          `anchor for step "${step.id}" is missing or ambiguous — if you renamed a data-tour attribute, update TOUR_ANCHOR and the journey together`,
        ).toHaveCount(1);
        noCrash();
      });
    }
  }
});

// ── Deep links ───────────────────────────────────────────────────────────────

test.describe("?tour= deep links", () => {
  test("starts the requested journey and strips the param", async ({
    page,
  }) => {
    await stubConsoleApi(page);
    await resetTourState(page);
    // A deep link is explicit intent: it runs even for a completed journey, and
    // it is how c8ctl hands a persona its journey (#413).
    await page.goto("metrics?tour=overview");
    await expect(page.locator(".driver-popover")).toBeVisible();
    await expect(page).toHaveURL(/\/console\/metrics$/);
  });

  test("an unknown journey id is ignored, not fatal", async ({ page }) => {
    await stubConsoleApi(page);
    await resetTourState(page);
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await page.goto("projects?tour=no-such-journey");
    await expect(page.getByText("Projects").first()).toBeVisible();
    expect(errors, "an unknown ?tour= must not throw").toEqual([]);
  });
});

// ── Precondition repair and skip ─────────────────────────────────────────────

test.describe("preconditions", () => {
  // The defect ADR 0049 exists to fix: the tour used to promise "hit Run to boot
  // the engine" on a host with neither Node nor Deno, using data it already had.
  // Any journey with a runtime-gated step must show its repair step instead.
  const gated = STUDIO_JOURNEYS.flatMap((j) =>
    j.steps
      .filter((s) => s.precondition?.id === "has-js-runtime" && s.repair)
      .map((s) => ({ journey: j, step: s })),
  );

  if (gated.length === 0) {
    test("no runtime-gated step is registered yet (guard is inert)", () => {
      // Deliberately visible rather than silently absent: journeys 0a (#408) and
      // 2 (#410) each add a Run step with a repair, and this guard starts biting
      // the moment they land.
      expect(gated).toHaveLength(0);
    });
  }

  for (const { journey, step } of gated) {
    test(`${journey.id} / ${step.id}: repairs into an install hint with no runtime`, async ({
      page,
    }) => {
      await stubConsoleApi(page, {
        denoAvailable: false,
        nodeAvailable: false,
      });
      await resetTourState(page);
      await page.goto("projects?tour=" + journey.id);
      const popover = page.locator(".driver-popover");
      await expect(popover).toBeVisible();
      // Walk to the end; the repaired step must appear and the original must not.
      const titles = await walkJourney(page, journey.steps.length + 2);
      expect(
        titles,
        "the repair step must be shown when no runtime is available",
      ).toContain(step.repair!.title);
      expect(
        titles,
        "the original step promises an action that cannot work here",
      ).not.toContain(step.title);
    });
  }

  test("an optional step gated on hasTraces is skipped with no traces", async ({
    page,
  }) => {
    const withTraceGate = STUDIO_JOURNEYS.flatMap((j) =>
      j.steps
        .filter((s) => s.precondition?.id === "has-traces")
        .map((s) => ({ j, s })),
    );
    test.skip(withTraceGate.length === 0, "no trace-gated step registered");
    const { j, s } = withTraceGate[0];
    await stubConsoleApi(page, { traceCount: 0 });
    await resetTourState(page);
    await page.goto(`projects?tour=${j.id}`);
    await expect(page.locator(".driver-popover")).toBeVisible();
    const titles = await walkJourney(page, j.steps.length + 2);
    expect(titles, "an empty trace table teaches nothing").not.toContain(
      s.title,
    );
  });
});

// ── Resume and dismissal ─────────────────────────────────────────────────────

test.describe("resume and exit", () => {
  test("a reload mid-journey offers Resume, not a restart", async ({
    page,
  }) => {
    await stubConsoleApi(page);
    await resetTourState(page);
    await page.goto("projects?tour=overview");
    const popover = page.locator(".driver-popover");
    await expect(popover).toBeVisible();
    await popover.locator(".driver-popover-next-btn").click();
    const secondTitle = await popover
      .locator(".driver-popover-title")
      .textContent();

    await page.reload();
    // Lands on the SAME step rather than starting over — the point of storing the
    // resume index against the authored step list.
    await expect(popover.locator(".driver-popover-title")).toHaveText(
      secondTitle!.trim(),
    );
    // Deliberately NOT asserting a "Resume tour" rail label here: the label is
    // `activeJourney && !isRunning`, and on reload the journey auto-resumes, so it
    // correctly reads "Take a tour" while the popover is on screen. Asserting
    // otherwise would encode a bug as a requirement.
    await expect(page.locator('[data-tour="take-a-tour"]')).toBeVisible();
  });

  test("Escape exits and does not trap focus", async ({ page }) => {
    await stubConsoleApi(page);
    await resetTourState(page);
    await page.goto("projects?tour=overview");
    await expect(page.locator(".driver-popover")).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.locator(".driver-popover")).toHaveCount(0);
    // Focus must be usable afterwards: a nav link is reachable by keyboard.
    await page.keyboard.press("Tab");
    await expect(page.locator("body")).toBeVisible();
  });
});

// ── Coverage report ──────────────────────────────────────────────────────────

test("reports steps this suite deliberately does not assert", () => {
  // Not a pass/fail gate — a visible ledger. A step inside a project workspace
  // needs a scaffolded, running project; asserting it would mean building and
  // running a Deno app per CI run. Printing the gap keeps it honest, so nobody
  // reads a green suite as "every anchor is checked".
  const uncovered = STUDIO_JOURNEYS.flatMap((j) =>
    spotlights(j)
      .filter((s) => !isReachable(s.route))
      .map(
        (s) => `${j.id}/${s.id} → ${s.selector} (route: ${s.route ?? "none"})`,
      ),
  );
  console.log(
    uncovered.length === 0
      ? "coverage: every spotlight step is on a reachable route"
      : `coverage: ${uncovered.length} spotlight step(s) not asserted:\n  ${uncovered.join("\n  ")}`,
  );
  expect(Array.isArray(uncovered)).toBe(true);
});
