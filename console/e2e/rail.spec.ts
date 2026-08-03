// Collapsible left-rail e2e guards (#511).
//
// WHY THIS SUITE EXISTS: the rail's collapse toggle is a persisted UI feature —
// its state lives in `localStorage` under `nano.railCollapsed` and must survive
// a reload. Nothing in a unit test or build asserts that the width actually
// changes, the icon-only state renders, or the choice is remembered across page
// loads; only a browser can. This is the compensating guard for that.

import { expect, test, type Page } from "@playwright/test";
import {
  assertNoPageCrash,
  resetTourState,
  stubConsoleApi,
  suppressStartupPanel,
} from "./fixtures.ts";

const RAIL_KEY = "nano.railCollapsed";

/** Widths are the Tailwind `w-56` (expanded) and `w-14` (collapsed) rails. */
const EXPANDED_MIN = 180;
const COLLAPSED_MAX = 80;

async function railWidth(page: Page): Promise<number> {
  return page
    .locator("aside")
    .first()
    .evaluate((el) => Math.round(el.getBoundingClientRect().width));
}

test.describe("collapsible left rail", () => {
  test.beforeEach(async ({ page }) => {
    await stubConsoleApi(page);
    await resetTourState(page);
    await suppressStartupPanel(page);
  });

  test("collapses, expands, and persists the choice across a reload", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await page.goto("projects");

    // Default: expanded, showing the renamed "Studio" nav link and a collapse
    // control.
    await expect(
      page.getByRole("link", { name: "Studio", exact: true }),
    ).toBeVisible();
    const collapseBtn = page.getByRole("button", { name: "Collapse sidebar" });
    await expect(collapseBtn).toBeVisible();
    await expect.poll(() => railWidth(page)).toBeGreaterThan(EXPANDED_MIN);

    // Collapse → narrow icon-only rail, persisted flag, and an expand control.
    await collapseBtn.click();
    const expandBtn = page.getByRole("button", { name: "Expand sidebar" });
    await expect(expandBtn).toBeVisible();
    await expect.poll(() => railWidth(page)).toBeLessThan(COLLAPSED_MAX);
    expect(
      await page.evaluate((k) => window.localStorage.getItem(k), RAIL_KEY),
    ).toBe("1");
    // The visible text label is gone, but the link keeps an accessible name.
    await expect(
      page.getByRole("link", { name: "Studio", exact: true }),
    ).toBeVisible();

    // Reload → the collapsed choice is remembered.
    await page.reload();
    await expect(
      page.getByRole("button", { name: "Expand sidebar" }),
    ).toBeVisible();
    await expect.poll(() => railWidth(page)).toBeLessThan(COLLAPSED_MAX);

    // Expand again → wide rail and the flag flips back.
    await page.getByRole("button", { name: "Expand sidebar" }).click();
    await expect(
      page.getByRole("button", { name: "Collapse sidebar" }),
    ).toBeVisible();
    await expect.poll(() => railWidth(page)).toBeGreaterThan(EXPANDED_MIN);
    expect(
      await page.evaluate((k) => window.localStorage.getItem(k), RAIL_KEY),
    ).toBe("0");

    noCrash();
  });
});
