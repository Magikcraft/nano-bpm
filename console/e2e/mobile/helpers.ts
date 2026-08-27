// Shared helpers for the mobile-first e2e guards (issue #1005, unit A7).
//
// Not a `*.spec.ts`, so the config's `testMatch` never collects it as a suite;
// it is imported by the mobile specs for the two things every one of them checks:
// that a phone viewport never scrolls sideways, and (for the home guard) which
// build profile the current project is exercising.

import { expect, type Page, type TestInfo } from "@playwright/test";

/**
 * Assert the page does not overflow horizontally.
 *
 * The single hardest promise of the mobile-first work (every A-task's acceptance
 * criterion) is "no horizontal scroll at 375px". The document's scroll width must
 * not exceed its client width — a stray fixed width, an un-wrapped table or a
 * card grid that fails to collapse shows up here as `scrollWidth > clientWidth`,
 * which no unit test or build can see.
 */
export async function expectNoHorizontalScroll(page: Page): Promise<void> {
  const overflow = await page.evaluate(() => {
    const el = document.documentElement;
    return { scrollWidth: el.scrollWidth, clientWidth: el.clientWidth };
  });
  expect(
    overflow.scrollWidth,
    `the page scrolls horizontally at ${overflow.clientWidth}px (scrollWidth ${overflow.scrollWidth}) — a mobile view is overflowing`,
  ).toBeLessThanOrEqual(overflow.clientWidth);
}

/** The build profile the current Playwright project targets (ADR 0034). The
 * `mobile-observe` project runs the lean operator build; everything else is the
 * default `studio` build. */
export function profileOf(testInfo: TestInfo): "studio" | "observe" {
  return testInfo.project.name === "mobile-observe" ? "observe" : "studio";
}

/** The landing route for a profile: the maker starts in Studio, the operator on
 * Topology (mirrors `HOME_ROUTE` in App.tsx). */
export function homeRoute(profile: "studio" | "observe"): string {
  return profile === "studio" ? "projects" : "topology";
}
