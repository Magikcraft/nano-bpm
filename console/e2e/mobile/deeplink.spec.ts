// Mobile `?instance=` deep-link landing guard — unit A6/A3.
//
// A deep link to a specific instance must land on the mobile *detail* stack, not
// the list — from both entry paths:
//   • standalone: navigating the console directly to `explorer?instance=<key>`;
//   • embedded: the framed app posting a `nano-navigate` bridge message, which
//     the host turns into the same `/explorer?instance=<key>` route.
// The embedded path only works through a REAL same-origin iframe (the host
// guards `ev.source === iframe.contentWindow`), so a plain `window.postMessage`
// from the test wouldn't pass — only a phone-sized browser driving the actual
// bridge proves it. (Studio profile — the app route is Studio-only; the
// standalone Explorer landing itself is profile-agnostic.)

import { expect, test } from "@playwright/test";
import {
  assertNoPageCrash,
  makeInstance,
  resetTourState,
  stubApp,
  stubConsoleApi,
  stubInstances,
  suppressStartupPanel,
} from "../fixtures.ts";
import { expectNoHorizontalScroll } from "./helpers.ts";

const INSTANCE_KEY = "2251799813685250";

test.describe("mobile instance deep-link", () => {
  test("standalone ?instance= lands on the mobile detail", async ({ page }) => {
    const noCrash = assertNoPageCrash(page);
    const inst = makeInstance({
      key: INSTANCE_KEY,
      process_id: "order-fulfilment",
    });
    await stubConsoleApi(page);
    await stubInstances(page, [inst]);
    await resetTourState(page);
    await suppressStartupPanel(page);

    await page.goto(`explorer?instance=${INSTANCE_KEY}`);

    // Lands directly on the detail stack (back-to-list affordance + drill cards),
    // NOT the list header.
    await expect(
      page.getByRole("button", { name: "← Instances" }),
    ).toBeVisible();
    await expect(page.getByRole("button", { name: /Model/ })).toBeVisible();

    await expectNoHorizontalScroll(page);

    noCrash();
  });

  test("embedded nano-navigate bridge routes to the mobile detail", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    const inst = makeInstance({
      key: INSTANCE_KEY,
      process_id: "order-fulfilment",
    });
    await stubConsoleApi(page);
    await stubInstances(page, [inst]);
    // The embedded app posts a processExplorer nano-navigate once it loads. It is
    // served same-origin (from the console dev server) so it clears the host's
    // origin+source trust guard.
    const appViewHtml = `<!doctype html><html><head><meta charset="utf-8"></head><body><script>
      function nav() {
        window.parent.postMessage(
          { type: "nano-navigate", target: "processExplorer", params: { instance: "${INSTANCE_KEY}" } },
          window.location.origin,
        );
      }
      nav();
      window.addEventListener("load", nav);
      setTimeout(nav, 150);
    </script></body></html>`;
    await stubApp(page, { name: "urban", label: "Urban", appViewHtml });
    await resetTourState(page);
    await suppressStartupPanel(page);

    await page.goto("apps/urban");

    // Drill into the embedded App UI so the iframe mounts and posts the bridge
    // message.
    await page.getByRole("button", { name: /App UI/ }).click();

    // The host turns the bridge message into the Explorer deep link and lands on
    // the mobile detail.
    await expect(page).toHaveURL(
      new RegExp(`/console/explorer\\?instance=${INSTANCE_KEY}`),
    );
    await expect(
      page.getByRole("button", { name: "← Instances" }),
    ).toBeVisible();
    await expect(page.getByRole("button", { name: /Model/ })).toBeVisible();

    await expectNoHorizontalScroll(page);

    noCrash();
  });
});
