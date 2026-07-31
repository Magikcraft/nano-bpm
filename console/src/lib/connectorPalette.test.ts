// Unit tests for the connector palette gate (ADR 0050, amending ADR 0033 §2).
// Run with `node --experimental-strip-types --test src/lib/connectorPalette.test.ts`.
// `componentTaskType` must stay in lockstep with the server's
// `connectors::component_task_type` (server/src/console/connectors.rs).
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  componentTaskType,
  filterEnabledPackComponents,
} from "./connectorPalette.ts";
import type { ElementTemplate } from "./urbanComponents.ts";

/** A connector element template binding `zeebe:taskDefinition:type = taskType`. */
function connectorTemplate(id: string, taskType: string): ElementTemplate {
  return {
    id,
    appliesTo: ["bpmn:Task"],
    properties: [
      {
        type: "Hidden",
        value: taskType,
        binding: { type: "zeebe:taskDefinition:type" },
      },
    ],
  } as unknown as ElementTemplate;
}

/** A design-only template with no task-definition binding. */
function designTemplate(id: string): ElementTemplate {
  return {
    id,
    appliesTo: ["bpmn:Task"],
    properties: [
      { type: "String", value: "x", binding: { type: "zeebe:property" } },
    ],
  } as unknown as ElementTemplate;
}

test("componentTaskType extracts the taskDefinition binding value", () => {
  assert.equal(
    componentTaskType(connectorTemplate("c1", "io.slack.postMessage")),
    "io.slack.postMessage",
  );
});

test("componentTaskType returns undefined for a design-only component", () => {
  assert.equal(componentTaskType(designTemplate("d1")), undefined);
});

test("componentTaskType ignores an empty binding value", () => {
  assert.equal(componentTaskType(connectorTemplate("c2", "")), undefined);
});

test("filterEnabledPackComponents hides connector components that are not enabled", () => {
  const pack = [
    connectorTemplate("slack", "io.slack.postMessage"),
    connectorTemplate("email", "io.email.send"),
    designTemplate("plain"),
  ];
  const kept = filterEnabledPackComponents(pack, ["io.slack.postMessage"]);
  const ids = kept.map((t) => t.id).sort();
  // Enabled connector + design-only stay; the un-enabled connector is dropped.
  assert.deepEqual(ids, ["plain", "slack"]);
});

test("filterEnabledPackComponents keeps design-only components with an empty enabled set", () => {
  const pack = [
    connectorTemplate("slack", "io.slack.postMessage"),
    designTemplate("plain"),
  ];
  const kept = filterEnabledPackComponents(pack, []);
  assert.deepEqual(
    kept.map((t) => t.id),
    ["plain"],
  );
});
