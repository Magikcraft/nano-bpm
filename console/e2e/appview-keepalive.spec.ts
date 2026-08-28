// App-view keep-alive guard (issue #1040).
//
// AppView embeds a supervised app's whole UI in a sandboxed iframe. Mounted as a
// plain `<Route element>`, React Router unmounts it the moment the console
// navigates anywhere else — destroying the iframe document, so every away-and-back
// round trip re-fetches the entire app through the reverse proxy (painful on
// low-bandwidth links) and loses all in-app state. The fix keeps visited app
// views mounted outside `<Routes>` and hides them with `display: none` (which
// preserves an iframe; only DOM removal reloads it).
//
// Only a browser proves the two properties this guards:
//   1. away-and-back keeps the SAME iframe node and performs exactly ONE app
//      document load — no reload, no refetch;
//   2. a HIDDEN kept-alive app cannot drive console navigation — its iframe can
//      still post `nano-navigate`, and it must not yank the console off the
//      route the user is actually on. (The active-app bridge is covered by
//      `mobile/deeplink.spec.ts`.)
// A unit test can't see either: the first is React Router's mount lifecycle, the
// second only exists through a real same-origin iframe (the host guards
// `ev.source === iframe.contentWindow`).
//
// Studio profile — the `/apps/:name` route is Studio-only.

import { expect, test } from "@playwright/test";
import {
  assertNoPageCrash,
  resetTourState,
  stubApp,
  stubConsoleApi,
  suppressStartupPanel,
} from "./fixtures.ts";

test.describe("app-view keep-alive", () => {
  test("navigating away and back does not reload the embedded app", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await stubConsoleApi(page, {
      projects: [{ name: "urban", running: true }],
    });
    await stubApp(page, { name: "urban", label: "Urban" });
    await resetTourState(page);
    await suppressStartupPanel(page);

    // Count full document loads of the embedded app — the thing a remount would
    // re-fetch. Exactly one is allowed for the whole test.
    let appDocumentLoads = 0;
    page.on("request", (req) => {
      if (
        req.url().includes("/console/app-view/urban/") &&
        req.resourceType() === "document"
      ) {
        appDocumentLoads++;
      }
    });

    await page.goto("apps/urban");
    const iframe = page.locator('iframe[title="Urban UI"]');
    await expect(iframe).toBeVisible();
    await expect(
      page.frameLocator('iframe[title="Urban UI"]').locator("main#app"),
    ).toBeVisible();

    // Remember the exact iframe DOM node so we can prove identity later.
    await page.evaluate(() => {
      (window as unknown as { __frame: Element | null }).__frame =
        document.querySelector("iframe");
    });

    // Away: a plain rail navigation (client-side — page.goto would be a full
    // document load and prove nothing).
    await page.getByRole("link", { name: "Topology" }).click();
    await expect(page).toHaveURL(/\/console\/topology/);
    await expect(iframe).toBeHidden();

    // Back: the rail's running-app entry.
    await page.getByRole("link", { name: "urban" }).click();
    await expect(page).toHaveURL(/\/console\/apps\/urban/);
    await expect(iframe).toBeVisible();

    // Same node, still no second document load.
    const sameNode = await page.evaluate(
      () =>
        (window as unknown as { __frame: Element | null }).__frame ===
        document.querySelector("iframe"),
    );
    expect(sameNode).toBe(true);
    expect(appDocumentLoads).toBe(1);

    noCrash();
  });

  test("a hidden app cannot drive console navigation", async ({ page }) => {
    const noCrash = assertNoPageCrash(page);
    await stubConsoleApi(page, {
      projects: [{ name: "urban", running: true }],
    });
    await stubApp(page, { name: "urban", label: "Urban" });
    await resetTourState(page);
    await suppressStartupPanel(page);

    await page.goto("apps/urban");
    await expect(page.locator('iframe[title="Urban UI"]')).toBeVisible();

    // Navigate away; the app's iframe stays mounted but hidden.
    await page.getByRole("link", { name: "Topology" }).click();
    await expect(page).toHaveURL(/\/console\/topology/);

    // The hidden app posts a bridge message. This must go through the REAL
    // iframe (the host guards ev.source === iframe.contentWindow).
    const frame = page
      .frames()
      .find((f) => f.url().includes("/console/app-view/urban/"));
    expect(frame, "hidden app's iframe should still be mounted").toBeDefined();
    await frame?.evaluate(() =>
      window.parent.postMessage(
        {
          type: "nano-navigate",
          target: "processExplorer",
          params: { instance: "2251799813685250" },
        },
        window.location.origin,
      ),
    );

    // Bounded negative wait: a wrongful navigation would be near-instant.
    await page
      .waitForURL(/\/console\/explorer/, { timeout: 500 })
      .catch(() => {});
    await expect(page).toHaveURL(/\/console\/topology/);

    noCrash();
  });
});
