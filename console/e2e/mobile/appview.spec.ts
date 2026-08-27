// Mobile app UI/Logs cards guard — unit A5.
//
// On a phone the app view collapses its side-by-side UI/Logs layout into
// drill-in cards: a UI app offers "App UI" + "Logs"; a headless app has no
// server to embed, so it is deliberately Logs-only. Tapping a card opens that
// view full-screen with a "← Back" affordance. This proves the card set adapts
// to the app mode and the embedded iframe bridge still mounts at 375px — neither
// of which a unit test exercises. (Studio profile only — the app route does not
// exist in the observe build.)

import { expect, test } from "@playwright/test";
import {
  assertNoPageCrash,
  resetTourState,
  stubApp,
  stubConsoleApi,
  suppressStartupPanel,
} from "../fixtures.ts";
import { expectNoHorizontalScroll } from "./helpers.ts";

test.describe("mobile app view", () => {
  test("offers App UI + Logs cards for a UI app and drills in", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await stubConsoleApi(page);
    await stubApp(page, { name: "urban", label: "Urban" });
    await resetTourState(page);
    await suppressStartupPanel(page);

    await page.goto("apps/urban");

    await expect(page.getByRole("heading", { name: "Urban" })).toBeVisible();
    const appCard = page.getByRole("button", { name: /App UI/ });
    const logsCard = page.getByRole("button", { name: /Logs/ });
    await expect(appCard).toBeVisible();
    await expect(logsCard).toBeVisible();
    await expectNoHorizontalScroll(page);

    // Drill into the embedded App UI: the sandboxed iframe mounts full-screen.
    await appCard.click();
    const back = page.getByRole("button", { name: "Back" });
    await expect(back).toBeVisible();
    await expect(page.getByTitle("Urban UI", { exact: true })).toBeVisible();
    await expectNoHorizontalScroll(page);

    // Back returns to the card grid, then Logs drills in.
    await back.click();
    await logsCard.click();
    await expect(page.getByRole("button", { name: "Back" })).toBeVisible();
    await expect(page.getByText(/No output yet/)).toBeVisible();

    noCrash();
  });

  test("is Logs-only for a headless app", async ({ page }) => {
    const noCrash = assertNoPageCrash(page);
    await stubConsoleApi(page);
    await stubApp(page, { name: "worker", label: "Worker", headless: true });
    await resetTourState(page);
    await suppressStartupPanel(page);

    await page.goto("apps/worker");

    await expect(page.getByRole("button", { name: /Logs/ })).toBeVisible();
    await expect(page.getByRole("button", { name: /App UI/ })).toHaveCount(0);
    await expectNoHorizontalScroll(page);

    noCrash();
  });
});
