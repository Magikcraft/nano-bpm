// Modeler agent-task inspection guard (#955).
//
// WHY THIS SUITE EXISTS: an *agent task* is a `bpmn:ServiceTask` carrying a
// `zeebe:linkedResource` with `linkName="prompt"` (#952). Selecting one renders
// the "Agent task" properties group, whose fixed "Resource type" / "Link name"
// fields are `@bpmn-io/properties-panel` `TextFieldEntry`s. That component calls
// `debounce(onInput)` in a `useMemo` on every render — even when `disabled` — so
// an entry built without a `debounce` throws "is not a function" mid-render.
//
// The console has NO error boundary (see fixtures.assertNoPageCrash), so that
// throw unmounts the entire properties panel: the user loses not just the Agent
// task group but every other group (Data envelope included) and it looks like
// "the modeler lost its palette". Nothing in a unit test or build catches a
// render-time throw in a properties-panel entry; only a browser mounting the
// real modeler does. This is the compensating guard for that failure class.

import { expect, test, type Page } from "@playwright/test";
import {
  assertNoPageCrash,
  resetTourState,
  stubConsoleApi,
  suppressStartupPanel,
} from "./fixtures.ts";

const PROJECT = "agent-demo";
const BPMN_PATH = "resources/processes/agent-task.bpmn";

/** A minimal but DI-complete process whose sole service task is an agent task —
 *  it carries the canonical prompt `zeebe:linkedResource` the toolchain emits. */
const AGENT_TASK_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:di="http://www.omg.org/spec/DD/20100524/DI" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="agenttest" targetNamespace="http://nanobpm.io">
  <bpmn:process id="agent-task-demo" isExecutable="true">
    <bpmn:startEvent id="start">
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:serviceTask id="work" name="work">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="demo-job" />
        <zeebe:linkedResources>
          <zeebe:linkedResource resourceId="feature.md" bindingType="latest" resourceType="GenericScript" linkName="prompt" />
        </zeebe:linkedResources>
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:endEvent id="end">
      <bpmn:incoming>f2</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="work" />
    <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="end" />
  </bpmn:process>
  <bpmndi:BPMNDiagram id="D">
    <bpmndi:BPMNPlane id="P" bpmnElement="agent-task-demo">
      <bpmndi:BPMNShape id="start_di" bpmnElement="start">
        <dc:Bounds x="180" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="work_di" bpmnElement="work">
        <dc:Bounds x="270" y="78" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="end_di" bpmnElement="end">
        <dc:Bounds x="430" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="f1_di" bpmnElement="f1">
        <di:waypoint x="216" y="118" />
        <di:waypoint x="270" y="118" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f2_di" bpmnElement="f2">
        <di:waypoint x="370" y="118" />
        <di:waypoint x="430" y="118" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>`;

/** The file tree the workspace renders — `resources/processes/agent-task.bpmn`
 *  plus the usual project scaffolding. */
const FILE_TREE = {
  files: [
    {
      name: "resources",
      path: "resources",
      kind: "dir",
      children: [
        {
          name: "processes",
          path: "resources/processes",
          kind: "dir",
          children: [
            { name: "agent-task.bpmn", path: BPMN_PATH, kind: "file" },
          ],
        },
      ],
    },
    { name: "main.ts", path: "main.ts", kind: "file" },
    { name: "package.json", path: "package.json", kind: "file" },
  ],
};

/** Layer the modeler's project-workspace routes over the shared console stubs so
 *  opening the process serves our agent-task model. Registered after
 *  `stubConsoleApi` so these more specific handlers take priority. */
async function stubProjectFiles(page: Page): Promise<void> {
  await page.route(`**/console/api/projects/${PROJECT}`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        config: {
          name: PROJECT,
          description: "",
          deployTarget: "http://localhost:8080",
          main: "main.ts",
          lang: "node",
          app: "console",
        },
        files: FILE_TREE.files,
        rootPath: `/tmp/${PROJECT}`,
        runState: {
          status: "stopped",
          pid: null,
          startedAtMs: null,
          lastError: null,
          compiling: false,
        },
        appUi: { enabled: true },
        runnable: true,
        missingToolchain: null,
        denoAvailable: true,
        nodeAvailable: true,
        urbanAvailable: false,
        platforms: [],
      }),
    }),
  );
  await page.route(`**/console/api/projects/${PROJECT}/files`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(FILE_TREE),
    }),
  );
  // The file endpoint returns the raw file text (not JSON) — see
  // `lib/api.getProjectFile`. `nano.app.json` is probed and legitimately absent
  // here (non-App project), so 404 it; the requested `.bpmn` serves our model.
  // Anchor on the singular `file?` query endpoint so this handler can't shadow
  // the `/files` tree listing registered above (a `/file*` glob also matches
  // `/files`, and the later route wins).
  await page.route(
    new RegExp(`/console/api/projects/${PROJECT}/file\\?`),
    (route) => {
      const path =
        new URL(route.request().url()).searchParams.get("path") ?? "";
      if (path === BPMN_PATH) {
        return route.fulfill({
          status: 200,
          contentType: "application/xml",
          headers: { "X-File-Size": String(AGENT_TASK_BPMN.length) },
          body: AGENT_TASK_BPMN,
        });
      }
      return route.fulfill({ status: 404, body: "not found" });
    },
  );
  // Workspace side panels the modeler view fans out to; empty shapes keep them
  // from crashing the (error-boundary-less) SPA.
  await page.route(`**/console/api/projects/${PROJECT}/connectors`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: "[]" }),
  );
  await page.route(`**/console/api/projects/${PROJECT}/logs`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: "[]" }),
  );
}

test.describe("modeler — agent task inspection", () => {
  test.beforeEach(async ({ page }) => {
    await stubConsoleApi(page, { projects: [{ name: PROJECT, lang: "node" }] });
    await stubProjectFiles(page);
    await resetTourState(page);
    await suppressStartupPanel(page);
  });

  test("selecting a prompt-bound service task renders the Agent task group without unmounting the panel", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await page.goto(`projects/${PROJECT}`);

    // Open resources/processes/agent-task.bpmn → the modeler mounts.
    await page.getByText("resources", { exact: true }).first().click();
    await page.getByText("processes", { exact: true }).first().click();
    await page.getByText("agent-task.bpmn", { exact: true }).first().click();

    // The custom renderer decorates the prompt-bound task with an AGENT badge —
    // proof the model imported and the agent-task renderer ran.
    await expect(page.locator("svg").getByText("AGENT")).toBeVisible();

    // Select the agent task. Before the fix this threw mid-render
    // ("debounce is not a function") and unmounted the whole panel.
    await page.locator('.djs-element[data-element-id="work"]').first().click();

    // The Agent task group and ALL its fields render — including the two fixed
    // fields whose missing `debounce` caused the crash.
    await expect(
      page.locator(
        '.bio-properties-panel-group-header-title:has-text("Agent task")',
      ),
    ).toBeVisible();
    await expect(
      page.locator("#bio-properties-panel-nano-agent-resourceType"),
    ).toHaveCount(1);
    await expect(
      page.locator("#bio-properties-panel-nano-agent-linkName"),
    ).toHaveCount(1);

    // And the pre-existing groups are still there — the panel did not unmount.
    await expect(
      page.locator(
        '.bio-properties-panel-group-header-title:has-text("Task definition")',
      ),
    ).toBeVisible();

    noCrash();
  });
});
