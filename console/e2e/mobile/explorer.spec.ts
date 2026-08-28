// Mobile Explorer list → detail flow + filter chips guard — unit A3.
//
// At a phone viewport the resizable master/detail split collapses into a stacked
// flow: a scrollable list of instance cards, a horizontally-scrollable filter
// chip row, and a full-screen detail reached by tapping a card (with an
// "← Instances" back affordance). None of that is exercised by a build or unit
// test — only a real phone-sized browser proves the stack navigates and the
// chip row never forces the page itself to scroll sideways.

import { expect, test } from "@playwright/test";
import {
  assertNoPageCrash,
  makeInstance,
  resetTourState,
  stubConsoleApi,
  stubInstances,
  suppressStartupPanel,
} from "../fixtures.ts";
import { expectNoHorizontalScroll } from "./helpers.ts";

test.describe("mobile Explorer", () => {
  test("stacks list → detail and offers the filter chip row", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    const alpha = makeInstance({
      key: "2251799813685250",
      process_id: "order-fulfilment",
    });
    const beta = makeInstance({
      key: "2251799813685999",
      process_id: "invoice-run",
      state: "Completed",
    });
    await stubConsoleApi(page);
    await stubInstances(page, [alpha, beta]);
    await resetTourState(page);
    await suppressStartupPanel(page);

    await page.goto("explorer");

    // The stacked list: header + one tappable card per instance.
    await expect(
      page.getByRole("heading", { name: "Process instances" }),
    ).toBeVisible();
    const alphaCard = page
      .getByRole("button")
      .filter({ hasText: "order-fulfilment" });
    await expect(alphaCard).toBeVisible();
    await expect(
      page.getByRole("button").filter({ hasText: "invoice-run" }),
    ).toBeVisible();

    // The filter chip row is present and toggles via aria-pressed. "All" starts
    // pressed; tapping a state chip moves the pressed state to it.
    const chips = page.getByRole("group", { name: "Filter instances" });
    await expect(chips).toBeVisible();
    const allChip = chips.getByRole("button", { name: "All", exact: true });
    await expect(allChip).toHaveAttribute("aria-pressed", "true");
    const activeChip = chips.getByRole("button", {
      name: "Active",
      exact: true,
    });
    await activeChip.click();
    await expect(activeChip).toHaveAttribute("aria-pressed", "true");
    await expect(allChip).toHaveAttribute("aria-pressed", "false");
    // Reset so the list shows both instances again.
    await allChip.click();

    await expectNoHorizontalScroll(page);

    // Tapping a card drills into the full-screen detail with a back affordance.
    await alphaCard.click();
    const back = page.getByRole("button", { name: "← Instances" });
    await expect(back).toBeVisible();
    await expect(page.getByRole("button", { name: /Model/ })).toBeVisible();

    await expectNoHorizontalScroll(page);

    // Back returns to the list.
    await back.click();
    await expect(
      page.getByRole("heading", { name: "Process instances" }),
    ).toBeVisible();

    noCrash();
  });
});
