// Call-activity parent↔child navigation guard (issue #1118, epic #1119).
//
// WHY THIS SUITE EXISTS: the call-activity navigation epic wires three seams
// together — the child→parent breadcrumb (#1116), the parent→child "Called
// Process Instances" section, and the BpmnViewer call-activity cell click that
// jumps to the child (#1117) — across the Explorer's InstanceDetail. Each slice
// has unit coverage for its pure helpers (`instanceBreadcrumb.ts`,
// `calledInstances.ts`), but nothing proves the *whole* journey resolves in a
// real browser: that a deep-linked child renders a breadcrumb whose hops
// navigate up, that a parent lists its spawned child and a row navigates down,
// and that clicking the call-activity shape on the diagram lands on that child.
// A rename of the breadcrumb `aria-label`, a regressed `onNavigateInstance`
// wiring, or a broken `calling_element_id` match would all pass unit tests and
// silently break the feature — this guard is the compensating control.
//
// The suite never talks to a real gateway (see playwright.config.ts): the
// "deploy parent+child, run an instance so the child spawns" step is expressed
// as a deterministic three-process graph of shaped fixtures with FIXED keys, so
// every assertion is on an instance key / process id, never on timing. That is
// what keeps it honest under this repo's no-intermittent-failures, no-retries
// rule: there is no spawn to wait for, so there is no race to absorb.

import { expect, test, type Page } from "@playwright/test";
import {
  assertNoPageCrash,
  CALL_ACTIVITY_PARENT_BPMN,
  makeInstance,
  makeInstanceDetail,
  resetTourState,
  stubConsoleApi,
  stubInstanceGraph,
  suppressStartupPanel,
} from "./fixtures.ts";

// Fixed keys — the whole point is determinism, so nothing here is generated.
// Definition keys drive the per-model XML route; instance keys drive the graph.
const ROOT_DEF = "2251799813600001";
const PARENT_DEF = "2251799813600002";
const CHILD_DEF = "2251799813600003";
const ROOT_KEY = "2251799813700001";
const PARENT_KEY = "2251799813700002";
const CHILD_KEY = "2251799813700003";

// Process ids double as the breadcrumb hop labels (breadcrumbHops uses
// process_id), so the assertions read them straight back.
const ROOT_PID = "order-intake";
const PARENT_PID = "order-orchestrator";
const CHILD_PID = "fulfilment";

// The call-activity cell in the PARENT model that spawned the child. Its BPMN id
// is both what the child's `calling_element_id` points back at and the
// `data-element-id` bpmn-js paints on the shape, so it is the single join
// between the "Called Process Instances" row and the clickable diagram cell.
const CALL_CELL_ID = "Call_Child";
const CALL_CELL_NAME = "Fulfil order";

/**
 * Stub the three-level call-activity graph:
 *   root (order-intake) ──callActivity──▶ parent (order-orchestrator)
 *                                          └─callActivity──▶ child (fulfilment)
 *
 * Each instance carries the snake_case parent linkage (#1113) so the breadcrumb
 * climbs, and each spawning instance lists its spawned child in
 * `called_instances` (#1115) with the `calling_element_id` of the cell that
 * spawned it. Only the PARENT model carries a laid-out `callActivity` shape (the
 * cell-click test targets it); the others render the minimal default model.
 */
async function stubGraph(page: Page): Promise<void> {
  const root = makeInstance({
    key: ROOT_KEY,
    process_id: ROOT_PID,
    process_definition_key: ROOT_DEF,
    parent_process_instance_key: null,
    parent_element_instance_key: null,
  });
  const parent = makeInstance({
    key: PARENT_KEY,
    process_id: PARENT_PID,
    process_definition_key: PARENT_DEF,
    parent_process_instance_key: ROOT_KEY,
    parent_element_instance_key: `${ROOT_KEY}-Call_Orchestrator`,
  });
  const child = makeInstance({
    key: CHILD_KEY,
    process_id: CHILD_PID,
    process_definition_key: CHILD_DEF,
    parent_process_instance_key: PARENT_KEY,
    parent_element_instance_key: `${PARENT_KEY}-${CALL_CELL_ID}`,
  });

  const rootDetail = makeInstanceDetail(root, {
    active_elements: [
      {
        element_id: "Call_Orchestrator",
        element_type: "CALL_ACTIVITY",
        element_name: "Orchestrate order",
      },
    ],
    called_instances: [
      {
        key: PARENT_KEY,
        process_id: PARENT_PID,
        version: 1,
        state: "Active",
        has_incident: false,
        start_date_ms: 0,
        calling_element_id: "Call_Orchestrator",
        calling_element_name: "Orchestrate order",
      },
    ],
  });
  const parentDetail = makeInstanceDetail(parent, {
    active_elements: [
      {
        element_id: CALL_CELL_ID,
        element_type: "CALL_ACTIVITY",
        element_name: CALL_CELL_NAME,
      },
    ],
    called_instances: [
      {
        key: CHILD_KEY,
        process_id: CHILD_PID,
        version: 1,
        state: "Active",
        has_incident: false,
        start_date_ms: 0,
        calling_element_id: CALL_CELL_ID,
        calling_element_name: CALL_CELL_NAME,
      },
    ],
  });
  const childDetail = makeInstanceDetail(child); // a leaf: no called instances

  await stubConsoleApi(page);
  await stubInstanceGraph(page, [rootDetail, parentDetail, childDetail], {
    [PARENT_DEF]: CALL_ACTIVITY_PARENT_BPMN,
  });
  await resetTourState(page);
  await suppressStartupPanel(page);
}

/** The InstanceDetail header's mono line is unique to the detail pane (the left
 * list shows process ids, not `instance <key>`), so it is the reliable "this
 * exact instance's detail is loaded" signal for a navigation assertion. */
function detailLoaded(page: Page, instanceKey: string) {
  return expect(
    page.getByText(new RegExp(`instance ${instanceKey}\\b`)),
  ).toBeVisible();
}

test.describe("call-activity parent↔child navigation", () => {
  test("child breadcrumb links back to parent and root, and navigates up", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await stubGraph(page);

    // Deep-link straight to the CHILD instance detail.
    await page.goto(`explorer?instance=${CHILD_KEY}`);
    await detailLoaded(page, CHILD_KEY);

    // The Operate-style breadcrumb resolves the whole chain root→parent→current.
    const callChain = page.getByRole("navigation", { name: "Call chain" });
    await expect(callChain).toBeVisible();
    const rootHop = callChain.getByRole("button", { name: ROOT_PID });
    const parentHop = callChain.getByRole("button", { name: PARENT_PID });
    await expect(rootHop).toBeVisible();
    await expect(parentHop).toBeVisible();
    // The current (child) hop is inert — marked, never a link.
    await expect(callChain.getByText(CHILD_PID)).toHaveAttribute(
      "aria-current",
      "page",
    );
    await expect(
      callChain.getByRole("button", { name: CHILD_PID }),
    ).toHaveCount(0);

    // Clicking the parent hop navigates the detail pane UP to the parent.
    await parentHop.click();
    await detailLoaded(page, PARENT_KEY);
    // The parent's own breadcrumb now ends at it (root→parent, current = parent)
    // and its "Called Process Instances" section (the down direction) is present.
    await expect(
      page
        .getByRole("navigation", { name: "Call chain" })
        .getByText(PARENT_PID),
    ).toHaveAttribute("aria-current", "page");
    await expect(
      page.getByRole("heading", { name: "Called Process Instances" }),
    ).toBeVisible();

    noCrash();
  });

  test("parent lists the called child and a row navigates down to it", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await stubGraph(page);

    // Deep-link to the PARENT instance detail.
    await page.goto(`explorer?instance=${PARENT_KEY}`);
    await detailLoaded(page, PARENT_KEY);

    // The "Called Process Instances" section lists the spawned child, named by
    // the call-activity cell that spawned it.
    await expect(
      page.getByRole("heading", { name: "Called Process Instances" }),
    ).toBeVisible();
    await expect(page.getByText(CALL_CELL_NAME).first()).toBeVisible();
    const childRowLink = page.getByRole("button", {
      name: `Open called instance ${CHILD_PID} ${CHILD_KEY}`,
    });
    await expect(childRowLink).toBeVisible();

    // Clicking the row navigates DOWN to the child instance detail.
    await childRowLink.click();
    await detailLoaded(page, CHILD_KEY);
    // Confirm it is genuinely the child view: its breadcrumb climbs back up.
    await expect(
      page.getByRole("navigation", { name: "Call chain" }).getByRole("button", {
        name: PARENT_PID,
      }),
    ).toBeVisible();

    noCrash();
  });

  test("selecting the call-activity cell on the diagram navigates to the child", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await stubGraph(page);

    await page.goto(`explorer?instance=${PARENT_KEY}`);
    await detailLoaded(page, PARENT_KEY);

    // bpmn-js paints the call-activity as a `.djs-element` group tagged with the
    // BPMN id. A single spawned child resolves to a direct navigation (Operate
    // double-click parity), so clicking the shape lands on the child.
    const cell = page.locator(
      `.djs-element[data-element-id="${CALL_CELL_ID}"]`,
    );
    await expect(cell).toBeVisible();
    await cell.click();

    await detailLoaded(page, CHILD_KEY);
    await expect(
      page.getByRole("navigation", { name: "Call chain" }).getByRole("button", {
        name: PARENT_PID,
      }),
    ).toBeVisible();

    noCrash();
  });
});
