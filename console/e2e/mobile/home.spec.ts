// Mobile card-home + hamburger guard — unit A1, both build profiles (ADR 0034).
//
// At a phone viewport the left navigation rail is replaced by a card grid and the
// lower rail (theme, tour, changelog, Config, Credits, Feedback, Docs) collapses
// into a hamburger bottom-sheet. This is a viewport-driven presentation of the
// SAME routes, not a separate route tree. Nothing in a build or unit test proves
// the right cards render — or that they differ correctly between the `studio`
// (full IDE) and `observe` (operator) builds, which have different nav sets and
// landing routes. This runs against BOTH: the `mobile-studio` project serves the
// studio build, `mobile-observe` the observe build (see playwright.config.ts).

import { expect, test } from "@playwright/test";
import {
  assertNoPageCrash,
  resetTourState,
  stubConsoleApi,
  suppressStartupPanel,
} from "../fixtures.ts";
import { expectNoHorizontalScroll, homeRoute, profileOf } from "./helpers.ts";

test.describe("mobile card home + hamburger", () => {
  test.beforeEach(async ({ page }) => {
    await stubConsoleApi(page);
    await resetTourState(page);
    await suppressStartupPanel(page);
  });

  test("renders the profile's nav cards and reaches the lower rail via the hamburger sheet", async ({
    page,
  }, testInfo) => {
    const noCrash = assertNoPageCrash(page);
    const profile = profileOf(testInfo);
    await page.goto(homeRoute(profile));

    // The card home: the primary rail as a launcher grid. Cards are the common
    // set in both profiles.
    for (const label of [
      "Topology",
      "Metrics",
      "Explorer",
      "Traces",
      "Workers",
    ]) {
      await expect(
        page.getByRole("button", { name: label, exact: true }),
      ).toBeVisible();
    }

    // The studio-only surfaces (the maker IDE + its marketplace) appear only in
    // the studio build; the operator build drops them entirely.
    const studioCard = page.getByRole("button", {
      name: "Studio",
      exact: true,
    });
    const extensionsCard = page.getByRole("button", {
      name: "Extensions",
      exact: true,
    });
    if (profile === "studio") {
      await expect(studioCard).toBeVisible();
      await expect(extensionsCard).toBeVisible();
    } else {
      await expect(studioCard).toHaveCount(0);
      await expect(extensionsCard).toHaveCount(0);
    }

    // The desktop rail is gone at this width — its collapse control must not be
    // present (this is a card home, not a shrunk rail).
    await expect(
      page.getByRole("button", { name: "Collapse sidebar" }),
    ).toHaveCount(0);

    await expectNoHorizontalScroll(page);

    // The hamburger opens a bottom-sheet dialog carrying the lower-rail actions.
    const hamburger = page.getByRole("button", { name: "Open menu" });
    await expect(hamburger).toBeVisible();
    await hamburger.click();

    const sheet = page.getByRole("dialog", { name: "Menu" });
    await expect(sheet).toBeVisible();
    // Every lower-rail destination is reachable from the sheet. Config/Credits
    // navigate in-host (buttons); Docs/Whitepaper/Feedback are links (hrefs).
    for (const label of ["Config", "Credits"]) {
      await expect(sheet.getByRole("button", { name: label })).toBeVisible();
    }
    for (const label of ["Documentation", "Whitepaper", "Feedback"]) {
      await expect(sheet.getByRole("link", { name: label })).toBeVisible();
    }

    await expectNoHorizontalScroll(page);

    // Tapping a nav card in the sheet navigates and dismisses it.
    await sheet.getByRole("button", { name: "Config" }).click();
    await expect(sheet).toHaveCount(0);
    await expect(page).toHaveURL(/\/console\/config$/);

    noCrash();
  });
});
