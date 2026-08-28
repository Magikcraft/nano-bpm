// Mobile instance drill-down guard — unit A4.
//
// On a phone the instance detail collapses its wide tables and diagram into
// drill-in cards (Model / Variables / Process Trace) that each open a
// full-screen panel — the BPMN diagram and the trace timeline need the whole
// viewport to be usable at 375px. This proves each panel opens, the BpmnViewer
// actually renders a diagram (not a blank pane), and the Process Trace shows the
// shared timeline rather than the empty state — none of which a unit test can
// assert without a real laid-out viewport.

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

test.describe("mobile instance detail", () => {
  test("drills into Model, Variables and Process Trace panels", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    const inst = makeInstance({
      key: "2251799813685250",
      process_id: "order-fulfilment",
    });
    await stubConsoleApi(page);
    await stubInstances(page, [inst]);
    await resetTourState(page);
    await suppressStartupPanel(page);

    // Deep-link straight to the instance detail (A3/A6 landing).
    await page.goto(`explorer?instance=${inst.key}`);

    // The three drill-in cards.
    const modelCard = page.getByRole("button", { name: /Model/ });
    const varsCard = page.getByRole("button", { name: /Variables/ });
    const traceCard = page.getByRole("button", { name: /Process Trace/ });
    await expect(modelCard).toBeVisible();
    await expect(varsCard).toBeVisible();
    await expect(traceCard).toBeVisible();
    await expectNoHorizontalScroll(page);

    // Model → full-screen panel with a rendered BPMN diagram (bpmn-js paints an
    // <svg> once the XML lays out).
    await modelCard.click();
    const modelPanel = page.getByRole("dialog");
    await expect(modelPanel).toBeVisible();
    await expect(modelPanel.locator("svg").first()).toBeVisible();
    await modelPanel.getByRole("button", { name: "Close" }).click();
    await expect(page.getByRole("dialog")).toHaveCount(0);

    // Variables → full-screen table with the stubbed rows.
    await varsCard.click();
    const varsPanel = page.getByRole("dialog");
    await expect(varsPanel).toBeVisible();
    await expect(varsPanel.getByText("orderId")).toBeVisible();
    await varsPanel.getByRole("button", { name: "Close" }).click();
    await expect(page.getByRole("dialog")).toHaveCount(0);

    // Process Trace → the shared TraceTimeline, not the empty state.
    await traceCard.click();
    const tracePanel = page.getByRole("dialog");
    await expect(tracePanel).toBeVisible();
    await expect(tracePanel.getByText(/No trace captured/)).toHaveCount(0);
    await expect(tracePanel.getByText("Task_1").first()).toBeVisible();
    await expectNoHorizontalScroll(page);
    await tracePanel.getByRole("button", { name: "Close" }).click();
    await expect(page.getByRole("dialog")).toHaveCount(0);

    noCrash();
  });
});
