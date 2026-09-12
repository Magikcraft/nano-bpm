// Unit tests for the agent-task prompt-binding carrier (issue #950).
// Node-native: run with `node --experimental-strip-types --test src/lib/agentTask.test.ts`.
// Covers the pure carrier logic (moddle/modeling injected) without bpmn-js.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  PROMPT_RESOURCE_TYPE,
  PROMPT_LINK_NAME,
  PROMPT_DEFAULT_BINDING_TYPE,
  APPEND_PROMPT_TARGET,
  AGENT_TASK_ELEMENT_TYPE,
  AGENT_DEFINITION_TYPE,
  AGENT_TYPE_EXTERNAL,
  AUTO_SUBSCRIBE_PROPERTY,
  AUTO_SUBSCRIBE_OPT_OUT_VALUE,
  ZEEBE_PROPERTIES_TYPE,
  ZEEBE_PROPERTY_TYPE,
  promptLinkedResource,
  readPromptBinding,
  isAgentTask,
  hasPromptBinding,
  hasExternalAgentMarker,
  agentDefinition,
  readAutoSubscribeOptOut,
  writePromptLink,
  removePromptLink,
  writeAppendPrompt,
  removePromptBinding,
  writeExternalAgentMarker,
  removeExternalAgentMarker,
  writeAutoSubscribeOptOut,
  clearAutoSubscribeOptOut,
  type AgentModdle,
  type AgentModdleElement,
  type AgentModeling,
} from "./agentTask.ts";

// A minimal moddle stand-in: create() returns a tagged plain object.
const moddle: AgentModdle = {
  create(type, attrs) {
    return { $type: type, ...(attrs as Record<string, unknown>) };
  },
};

// A modeling stand-in that APPLIES updates to the live business object (so a
// read-back after a write reflects the mutation, as bpmn-js's modeling does) and
// records every call so tests can assert the command shape.
function applyingModeling(): AgentModeling & {
  calls: Array<{
    moddleElement: AgentModdleElement;
    props: Record<string, unknown>;
  }>;
} {
  const calls: Array<{
    moddleElement: AgentModdleElement;
    props: Record<string, unknown>;
  }> = [];
  return {
    calls,
    updateModdleProperties(_element, moddleElement, props) {
      const target = moddleElement as AgentModdleElement;
      calls.push({ moddleElement: target, props });
      Object.assign(target, props);
    },
  };
}

// A modeling stand-in that also mirrors bpmn-js's UpdateModdlePropertiesHandler
// UNDO semantics: each call records the target's PRIOR value for every key it
// sets, and `undo()` restores those keys (reverting the last command). This lets
// a test exercise a real command-stack rollback — the only way to catch a stale
// `$parent` that survives an undo (issue #1186).
function undoableModeling(): AgentModeling & {
  calls: Array<{
    moddleElement: AgentModdleElement;
    props: Record<string, unknown>;
  }>;
  undo(): void;
} {
  const calls: Array<{
    moddleElement: AgentModdleElement;
    props: Record<string, unknown>;
  }> = [];
  const history: Array<{
    target: AgentModdleElement;
    old: Record<string, unknown>;
  }> = [];
  return {
    calls,
    updateModdleProperties(_element, moddleElement, props) {
      const target = moddleElement as AgentModdleElement;
      const old: Record<string, unknown> = {};
      for (const key of Object.keys(props)) {
        old[key] = (target as Record<string, unknown>)[key];
      }
      history.push({ target, old });
      calls.push({ moddleElement: target, props });
      Object.assign(target, props);
    },
    undo() {
      const entry = history.pop();
      if (entry) Object.assign(entry.target, entry.old);
    },
  };
}

function serviceTaskBo(values: AgentModdleElement[] = []): AgentModdleElement {
  return {
    $type: "bpmn:ServiceTask",
    extensionElements: { $type: "bpmn:ExtensionElements", values },
  };
}

function linkedResources(...links: AgentModdleElement[]): AgentModdleElement {
  return { $type: "zeebe:LinkedResources", values: links };
}

function promptLink(
  resourceId = "feature.md",
  bindingType = "latest",
): AgentModdleElement {
  return {
    $type: "zeebe:LinkedResource",
    resourceId,
    bindingType,
    resourceType: PROMPT_RESOURCE_TYPE,
    linkName: PROMPT_LINK_NAME,
  };
}

test("readPromptBinding returns undefined for a plain service task", () => {
  assert.equal(readPromptBinding(serviceTaskBo()), undefined);
  assert.equal(readPromptBinding(undefined), undefined);
});

test("readPromptBinding reads the prompt link's resourceId + bindingType", () => {
  const bo = serviceTaskBo([
    linkedResources(promptLink("retro.md", "deployment")),
  ]);
  assert.deepEqual(readPromptBinding(bo), {
    resourceId: "retro.md",
    bindingType: "deployment",
  });
});

test("readPromptBinding defaults an absent bindingType to latest", () => {
  const link = promptLink("x.md");
  delete link.bindingType;
  const bo = serviceTaskBo([linkedResources(link)]);
  assert.equal(readPromptBinding(bo)?.bindingType, PROMPT_DEFAULT_BINDING_TYPE);
});

test("readPromptBinding surfaces the optional appendPrompt addendum", () => {
  const bo = serviceTaskBo([
    linkedResources(promptLink()),
    {
      $type: "zeebe:IoMapping",
      inputParameters: [
        {
          $type: "zeebe:Input",
          source: "=task.prompt",
          target: APPEND_PROMPT_TARGET,
        },
      ],
    },
  ]);
  assert.equal(readPromptBinding(bo)?.append, "=task.prompt");
});

test("a non-prompt linked resource is not a prompt binding", () => {
  const other: AgentModdleElement = {
    $type: "zeebe:LinkedResource",
    resourceId: "other.js",
    resourceType: PROMPT_RESOURCE_TYPE,
    linkName: "somethingElse",
  };
  const bo = serviceTaskBo([linkedResources(other)]);
  assert.equal(promptLinkedResource(bo), undefined);
  assert.equal(readPromptBinding(bo), undefined);
});

test("isAgentTask is true only for a prompt-bound service task", () => {
  const agent = {
    type: AGENT_TASK_ELEMENT_TYPE,
    businessObject: serviceTaskBo([linkedResources(promptLink())]),
  };
  const plain = {
    type: AGENT_TASK_ELEMENT_TYPE,
    businessObject: serviceTaskBo(),
  };
  // A user task carrying a prompt link (should never happen, but guard the type gate).
  const userTask = {
    type: "bpmn:UserTask",
    businessObject: serviceTaskBo([linkedResources(promptLink())]),
  };
  assert.equal(isAgentTask(agent), true);
  assert.equal(isAgentTask(plain), false);
  assert.equal(isAgentTask(userTask), false);
  assert.equal(isAgentTask(undefined), false);
});

test("writePromptLink emits exactly the toolchain's linkedResource shape", () => {
  const bo = serviceTaskBo();
  const modeling = applyingModeling();
  writePromptLink(moddle, modeling, {}, bo, "feature.md", "latest");
  const link = promptLinkedResource(bo);
  assert.deepEqual(
    {
      resourceId: link?.resourceId,
      bindingType: link?.bindingType,
      resourceType: link?.resourceType,
      linkName: link?.linkName,
    },
    {
      resourceId: "feature.md",
      bindingType: "latest",
      resourceType: PROMPT_RESOURCE_TYPE,
      linkName: PROMPT_LINK_NAME,
    },
  );
});

test("writePromptLink creates extensionElements when the task has none", () => {
  const bo: AgentModdleElement = { $type: "bpmn:ServiceTask" };
  const modeling = applyingModeling();
  writePromptLink(moddle, modeling, {}, bo, "a.md", "latest");
  assert.equal(bo.extensionElements?.$type, "bpmn:ExtensionElements");
  assert.equal(readPromptBinding(bo)?.resourceId, "a.md");
});

test('writePromptLink omits resourceId when it is blank (never emits resourceId="")', () => {
  // Toggling the agent-task switch on (or clearing the resource field) writes a
  // blank resourceId. An empty `resourceId=""` is an invalid linkedResource the
  // engine rejects on deploy, so we must omit the attribute rather than serialize
  // it empty — the linkName="prompt" marker still identifies the agent task.
  const bo = serviceTaskBo();
  writePromptLink(
    moddle,
    applyingModeling(),
    {},
    bo,
    "",
    PROMPT_DEFAULT_BINDING_TYPE,
  );
  const link = promptLinkedResource(bo);
  assert.ok(link, "the prompt marker link is created");
  assert.equal(
    Object.prototype.hasOwnProperty.call(link!, "resourceId"),
    false,
    "no empty resourceId attribute is emitted",
  );
  // The task is still recognizably an agent task (marker present).
  assert.equal(
    isAgentTask({ type: AGENT_TASK_ELEMENT_TYPE, businessObject: bo }),
    true,
  );
  // Whitespace-only is treated as blank too.
  const bo2 = serviceTaskBo();
  writePromptLink(
    moddle,
    applyingModeling(),
    {},
    bo2,
    "   ",
    PROMPT_DEFAULT_BINDING_TYPE,
  );
  assert.equal(
    Object.prototype.hasOwnProperty.call(
      promptLinkedResource(bo2)!,
      "resourceId",
    ),
    false,
  );
});

test("writePromptLink drops resourceId when an existing binding's resource is cleared", () => {
  const bo = serviceTaskBo([
    linkedResources(promptLink("feature.md", "latest")),
  ]);
  writePromptLink(moddle, applyingModeling(), {}, bo, "", "latest");
  const link = promptLinkedResource(bo);
  assert.ok(link);
  assert.equal(
    Object.prototype.hasOwnProperty.call(link!, "resourceId"),
    false,
  );
  assert.equal(readPromptBinding(bo)?.resourceId, "");
});

test("writePromptLink defaults an empty bindingType to latest", () => {
  const bo = serviceTaskBo();
  writePromptLink(moddle, applyingModeling(), {}, bo, "a.md", "");
  assert.equal(readPromptBinding(bo)?.bindingType, PROMPT_DEFAULT_BINDING_TYPE);
});

test("writePromptLink updates in place and preserves non-prompt linked resources", () => {
  const other: AgentModdleElement = {
    $type: "zeebe:LinkedResource",
    resourceId: "lib.js",
    resourceType: PROMPT_RESOURCE_TYPE,
    linkName: "helper",
  };
  const bo = serviceTaskBo([
    linkedResources(other, promptLink("old.md", "latest")),
  ]);
  writePromptLink(moddle, applyingModeling(), {}, bo, "new.md", "deployment");
  const container = bo.extensionElements?.values?.find(
    (v) => v.$type === "zeebe:LinkedResources",
  );
  // Exactly one LinkedResources container, holding the preserved helper + the
  // updated prompt link (no duplication).
  assert.equal(
    bo.extensionElements?.values?.filter(
      (v) => v.$type === "zeebe:LinkedResources",
    ).length,
    1,
  );
  assert.equal(container?.values?.length, 2);
  assert.ok(container?.values?.some((v) => v.linkName === "helper"));
  assert.deepEqual(readPromptBinding(bo), {
    resourceId: "new.md",
    bindingType: "deployment",
  });
});

test("removePromptLink drops the container when no other links remain", () => {
  const bo = serviceTaskBo([linkedResources(promptLink())]);
  removePromptLink(applyingModeling(), {}, bo);
  assert.equal(promptLinkedResource(bo), undefined);
  // It was the only extension child, so the wrapper is torn down entirely.
  assert.equal(bo.extensionElements, undefined);
});

test("removePromptLink keeps the container when other links remain", () => {
  const other: AgentModdleElement = {
    $type: "zeebe:LinkedResource",
    resourceId: "lib.js",
    resourceType: PROMPT_RESOURCE_TYPE,
    linkName: "helper",
  };
  const bo = serviceTaskBo([linkedResources(other, promptLink())]);
  removePromptLink(applyingModeling(), {}, bo);
  const container = bo.extensionElements?.values?.find(
    (v) => v.$type === "zeebe:LinkedResources",
  );
  assert.equal(promptLinkedResource(bo), undefined);
  assert.equal(container?.values?.length, 1);
  assert.equal(container?.values?.[0].linkName, "helper");
});

test("writeAppendPrompt creates, replaces and clears the appendPrompt input", () => {
  const bo = serviceTaskBo([linkedResources(promptLink())]);
  const modeling = applyingModeling();
  // create
  writeAppendPrompt(moddle, modeling, {}, bo, "=task.prompt");
  assert.equal(readPromptBinding(bo)?.append, "=task.prompt");
  const io = bo.extensionElements?.values?.find(
    (v) => v.$type === "zeebe:IoMapping",
  );
  assert.equal(io?.inputParameters?.length, 1);
  // replace (no duplicate input)
  writeAppendPrompt(moddle, modeling, {}, bo, "=other");
  assert.equal(io?.inputParameters?.length, 1);
  assert.equal(readPromptBinding(bo)?.append, "=other");
  // clear removes the empty ioMapping
  writeAppendPrompt(moddle, modeling, {}, bo, "");
  assert.equal(readPromptBinding(bo)?.append, undefined);
  assert.equal(
    bo.extensionElements?.values?.some((v) => v.$type === "zeebe:IoMapping"),
    false,
  );
});

test("writeAppendPrompt trails explicit inputs and keeps other io on clear", () => {
  const io: AgentModdleElement = {
    $type: "zeebe:IoMapping",
    inputParameters: [
      { $type: "zeebe:Input", source: "=repo", target: "repo" },
    ],
    outputParameters: [
      { $type: "zeebe:Output", source: "=status", target: "status" },
    ],
  };
  const bo = serviceTaskBo([linkedResources(promptLink()), io]);
  writeAppendPrompt(moddle, applyingModeling(), {}, bo, "=task.prompt");
  // appendPrompt is the LAST input, after the explicit one.
  assert.deepEqual(
    io.inputParameters?.map((p) => p.target),
    ["repo", APPEND_PROMPT_TARGET],
  );
  // clearing the append leaves the explicit input + output, keeping the mapping.
  writeAppendPrompt(moddle, applyingModeling(), {}, bo, "");
  assert.deepEqual(
    io.inputParameters?.map((p) => p.target),
    ["repo"],
  );
  assert.equal(io.outputParameters?.length, 1);
  assert.ok(
    bo.extensionElements?.values?.some((v) => v.$type === "zeebe:IoMapping"),
  );
});

test("writePromptLink keeps LinkedResources before an existing IoMapping", () => {
  // A task that already carries an ioMapping (e.g. an explicit input) but no
  // prompt link. Adding the link must slot LinkedResources BEFORE IoMapping to
  // match the toolchain's canonical extensionElements ordering.
  const io: AgentModdleElement = {
    $type: "zeebe:IoMapping",
    inputParameters: [
      { $type: "zeebe:Input", source: "=repo", target: "repo" },
    ],
  };
  const bo = serviceTaskBo([io]);
  writePromptLink(moddle, applyingModeling(), {}, bo, "feature.md", "latest");
  assert.deepEqual(
    bo.extensionElements?.values?.map((v) => v.$type),
    ["zeebe:LinkedResources", "zeebe:IoMapping"],
  );
});

test("writePromptLink keeps an existing zeebe:TaskDefinition before the new LinkedResources", () => {
  // A service task that already carries a zeebe:TaskDefinition (an extension
  // child the toolchain emits *before* linkedResources, but one this module does
  // not enumerate). Attaching the prompt link must leave the taskDefinition in
  // place and append LinkedResources after it — never reorder an unknown child
  // ahead of its canonical position.
  const taskDefinition: AgentModdleElement = {
    $type: "zeebe:TaskDefinition",
    type: "my-worker",
  };
  const bo = serviceTaskBo([taskDefinition]);
  writePromptLink(moddle, applyingModeling(), {}, bo, "feature.md", "latest");
  assert.deepEqual(
    bo.extensionElements?.values?.map((v) => v.$type),
    ["zeebe:TaskDefinition", "zeebe:LinkedResources"],
  );
});

test("writePromptLink slots LinkedResources between an existing TaskDefinition and IoMapping", () => {
  // taskDefinition (canonical: first) and ioMapping (canonical: last) already
  // present. The new LinkedResources must land between them — after the unknown
  // taskDefinition, before the known ioMapping.
  const taskDefinition: AgentModdleElement = {
    $type: "zeebe:TaskDefinition",
    type: "my-worker",
  };
  const io: AgentModdleElement = {
    $type: "zeebe:IoMapping",
    inputParameters: [
      { $type: "zeebe:Input", source: "=repo", target: "repo" },
    ],
  };
  const bo = serviceTaskBo([taskDefinition, io]);
  writePromptLink(moddle, applyingModeling(), {}, bo, "feature.md", "latest");
  assert.deepEqual(
    bo.extensionElements?.values?.map((v) => v.$type),
    ["zeebe:TaskDefinition", "zeebe:LinkedResources", "zeebe:IoMapping"],
  );
});

test("removing the last extension child tears down the empty wrapper", () => {
  // The only extension child is the prompt link's container; stripping the
  // binding must leave no orphan bpmn:extensionElements wrapper behind.
  const bo = serviceTaskBo([linkedResources(promptLink())]);
  removePromptBinding(moddle, applyingModeling(), {}, bo);
  assert.equal(bo.extensionElements, undefined);
});

test("removePromptBinding strips both the link and the append addendum", () => {
  const bo = serviceTaskBo([linkedResources(promptLink())]);
  const modeling = applyingModeling();
  writeAppendPrompt(moddle, modeling, {}, bo, "=task.prompt");
  assert.equal(
    isAgentTask({ type: AGENT_TASK_ELEMENT_TYPE, businessObject: bo }),
    true,
  );
  removePromptBinding(moddle, modeling, {}, bo);
  assert.equal(readPromptBinding(bo), undefined);
  assert.equal(
    isAgentTask({ type: AGENT_TASK_ELEMENT_TYPE, businessObject: bo }),
    false,
  );
  // Both children gone, so the wrapper is torn down entirely — no orphan.
  assert.equal(bo.extensionElements, undefined);
});

// --- External-agent marker + --auto opt-out (issue #1180) -------------------

function agentMarker(agentType = AGENT_TYPE_EXTERNAL): AgentModdleElement {
  return { $type: AGENT_DEFINITION_TYPE, agentType };
}

function zeebeProps(...properties: AgentModdleElement[]): AgentModdleElement {
  return { $type: ZEEBE_PROPERTIES_TYPE, properties };
}

function optOutProperty(
  value = AUTO_SUBSCRIBE_OPT_OUT_VALUE,
): AgentModdleElement {
  return { $type: ZEEBE_PROPERTY_TYPE, name: AUTO_SUBSCRIBE_PROPERTY, value };
}

test("hasExternalAgentMarker is true only for agentType=external", () => {
  assert.equal(hasExternalAgentMarker(serviceTaskBo()), false);
  assert.equal(hasExternalAgentMarker(serviceTaskBo([agentMarker()])), true);
  // A different agentType (e.g. Camunda's aiAgentTask) is not the fleet marker.
  assert.equal(
    hasExternalAgentMarker(serviceTaskBo([agentMarker("aiAgentTask")])),
    false,
  );
});

test("isAgentTask recognizes the external marker as well as the prompt link", () => {
  const markerOnly = {
    type: AGENT_TASK_ELEMENT_TYPE,
    businessObject: serviceTaskBo([agentMarker()]),
  };
  const promptOnly = {
    type: AGENT_TASK_ELEMENT_TYPE,
    businessObject: serviceTaskBo([linkedResources(promptLink())]),
  };
  const plain = {
    type: AGENT_TASK_ELEMENT_TYPE,
    businessObject: serviceTaskBo(),
  };
  assert.equal(isAgentTask(markerOnly), true);
  assert.equal(isAgentTask(promptOnly), true);
  assert.equal(isAgentTask(plain), false);
  // hasPromptBinding stays narrow — it never reports the marker as a prompt.
  assert.equal(hasPromptBinding(markerOnly.businessObject), false);
  assert.equal(hasPromptBinding(promptOnly.businessObject), true);
});

test("writeExternalAgentMarker adds the canonical marker to a plain task", () => {
  const bo = serviceTaskBo();
  writeExternalAgentMarker(moddle, applyingModeling(), {}, bo);
  const def = agentDefinition(bo);
  assert.equal(def?.$type, AGENT_DEFINITION_TYPE);
  assert.equal(def?.agentType, AGENT_TYPE_EXTERNAL);
  assert.equal(hasExternalAgentMarker(bo), true);
});

test("writeExternalAgentMarker creates extensionElements when the task has none", () => {
  const bo: AgentModdleElement = { $type: "bpmn:ServiceTask" };
  writeExternalAgentMarker(moddle, applyingModeling(), {}, bo);
  assert.equal(bo.extensionElements?.$type, "bpmn:ExtensionElements");
  assert.equal(hasExternalAgentMarker(bo), true);
});

test("writeExternalAgentMarker is idempotent and corrects a stale agentType", () => {
  // Already external → no duplicate marker, single command-free no-op result.
  const bo = serviceTaskBo([agentMarker()]);
  writeExternalAgentMarker(moddle, applyingModeling(), {}, bo);
  assert.equal(
    bo.extensionElements?.values?.filter(
      (v) => v.$type === AGENT_DEFINITION_TYPE,
    ).length,
    1,
  );
  // Stale agentType is corrected in place, not duplicated.
  const stale = serviceTaskBo([agentMarker("aiAgentTask")]);
  writeExternalAgentMarker(moddle, applyingModeling(), {}, stale);
  assert.equal(
    stale.extensionElements?.values?.filter(
      (v) => v.$type === AGENT_DEFINITION_TYPE,
    ).length,
    1,
  );
  assert.equal(hasExternalAgentMarker(stale), true);
});

test("removeExternalAgentMarker strips the marker and tears down the empty wrapper", () => {
  const bo = serviceTaskBo([agentMarker()]);
  removeExternalAgentMarker(moddle, applyingModeling(), {}, bo);
  assert.equal(agentDefinition(bo), undefined);
  // It was the only extension child, so the wrapper is gone.
  assert.equal(bo.extensionElements, undefined);
});

test("removeExternalAgentMarker also clears an orphaned --auto opt-out", () => {
  // The moderate #1186 finding: the opt-out toggle is hidden once the marker is
  // gone, so removing the marker must not strand `autoSubscribe="false"` in the
  // saved BPMN with no visible control to clear it.
  const bo = serviceTaskBo([agentMarker(), zeebeProps(optOutProperty())]);
  const modeling = applyingModeling();
  removeExternalAgentMarker(moddle, modeling, {}, bo);
  assert.equal(hasExternalAgentMarker(bo), false);
  assert.equal(readAutoSubscribeOptOut(bo), false);
  // Both children (and the empty wrapper) are gone — no orphan left behind.
  assert.equal(bo.extensionElements, undefined);
  // Marker + opt-out are torn down in ONE command, so a single undo restores
  // both together (issue #1186) — never two commands where one undo revives the
  // opt-out while the marker stays gone.
  assert.equal(modeling.calls.length, 1);
});

test("removeExternalAgentMarker clears the opt-out but keeps unrelated siblings", () => {
  const other: AgentModdleElement = {
    $type: ZEEBE_PROPERTY_TYPE,
    name: "some.other.prop",
    value: "keep-me",
  };
  const originalContainer = zeebeProps(optOutProperty(), other);
  const bo = serviceTaskBo([agentMarker(), originalContainer]);
  const modeling = applyingModeling();
  removeExternalAgentMarker(moddle, modeling, {}, bo);
  assert.equal(hasExternalAgentMarker(bo), false);
  assert.equal(readAutoSubscribeOptOut(bo), false);
  // The unrelated property (and its container) survive.
  const container = bo.extensionElements?.values?.find(
    (v) => v.$type === ZEEBE_PROPERTIES_TYPE,
  );
  assert.deepEqual(
    container?.properties?.map((p) => p.name),
    ["some.other.prop"],
  );
  // Still a single command (marker drop + opt-out drop applied together).
  assert.equal(modeling.calls.length, 1);
  // The surviving container is a FRESH object, not the original mutated in
  // place — so the original (with its opt-out) is left intact for undo to
  // restore, rather than losing the opt-out on the command's reversal.
  assert.notEqual(container, originalContainer);
  assert.deepEqual(
    originalContainer.properties?.map((p) => p.name),
    [AUTO_SUBSCRIBE_PROPERTY, "some.other.prop"],
  );
});

test("removeExternalAgentMarker's single command rolls back cleanly, restoring the original container and its children's parent links", () => {
  // A real command-stack undo of the ONE removal command must fully restore the
  // prior state — including every surviving property's `$parent`. The stale-
  // `$parent` bug (#1186): rebuilding the container by REPARENTING the originals
  // leaves them pointing at a `rebuilt` container that undo removes from the
  // model, so a later traversal walks a dangling parent. Cloning avoids it.
  const other: AgentModdleElement = {
    $type: ZEEBE_PROPERTY_TYPE,
    name: "some.other.prop",
    value: "keep-me",
  };
  const optOut = optOutProperty();
  const originalContainer = zeebeProps(optOut, other);
  const bo = serviceTaskBo([agentMarker(), originalContainer]);
  const ext = bo.extensionElements as AgentModdleElement;
  // Wire the parent links the way bpmn-js keeps them, so a stale one is visible.
  originalContainer.$parent = ext;
  for (const p of originalContainer.properties ?? [])
    p.$parent = originalContainer;

  const modeling = undoableModeling();
  removeExternalAgentMarker(moddle, modeling, {}, bo);
  // New state: single command; marker + opt-out gone; sibling kept.
  assert.equal(modeling.calls.length, 1);
  assert.equal(hasExternalAgentMarker(bo), false);
  assert.equal(readAutoSubscribeOptOut(bo), false);

  // Undo the single removal command.
  modeling.undo();

  // The ORIGINAL container is back under the wrapper, both its properties intact.
  assert.equal(ext.values?.includes(originalContainer), true);
  assert.equal(hasExternalAgentMarker(bo), true);
  assert.equal(readAutoSubscribeOptOut(bo), true);
  assert.deepEqual(
    originalContainer.properties?.map((p) => p.name),
    [AUTO_SUBSCRIBE_PROPERTY, "some.other.prop"],
  );
  // Every restored child still points at the in-model original container — never
  // a `rebuilt` container the undo removed (the stale-`$parent` bug #1186).
  assert.equal(originalContainer.$parent, ext);
  for (const p of originalContainer.properties ?? []) {
    assert.equal(p.$parent, originalContainer);
  }
});

test("clearAutoSubscribeOptOut drops the opt-out and is a no-op when absent", () => {
  const bo = serviceTaskBo([agentMarker(), zeebeProps(optOutProperty())]);
  clearAutoSubscribeOptOut(applyingModeling(), {}, bo);
  assert.equal(readAutoSubscribeOptOut(bo), false);
  // Marker is untouched — clearing the opt-out is independent of the marker.
  assert.equal(hasExternalAgentMarker(bo), true);
  // Clearing again (now absent) does nothing and creates no empty container.
  clearAutoSubscribeOptOut(applyingModeling(), {}, bo);
  assert.equal(
    bo.extensionElements?.values?.some(
      (v) => v.$type === ZEEBE_PROPERTIES_TYPE,
    ),
    false,
  );
});

test("the external marker leads the prompt link in extensionElements order", () => {
  // A task that already carries a prompt link. Adding the marker must slot it
  // BEFORE LinkedResources (canonical order), never after.
  const bo = serviceTaskBo([linkedResources(promptLink())]);
  writeExternalAgentMarker(moddle, applyingModeling(), {}, bo);
  assert.deepEqual(
    bo.extensionElements?.values?.map((v) => v.$type),
    [AGENT_DEFINITION_TYPE, "zeebe:LinkedResources"],
  );
});

test("marker and prompt binding coexist independently", () => {
  const bo = serviceTaskBo();
  const modeling = applyingModeling();
  writeExternalAgentMarker(moddle, modeling, {}, bo);
  writePromptLink(moddle, modeling, {}, bo, "feature.md", "latest");
  assert.equal(hasExternalAgentMarker(bo), true);
  assert.equal(hasPromptBinding(bo), true);
  // Removing the prompt binding leaves the marker (still an agent task).
  removePromptBinding(moddle, modeling, {}, bo);
  assert.equal(hasPromptBinding(bo), false);
  assert.equal(hasExternalAgentMarker(bo), true);
  assert.equal(
    isAgentTask({ type: AGENT_TASK_ELEMENT_TYPE, businessObject: bo }),
    true,
  );
});

test('readAutoSubscribeOptOut is true only for the exact value "false" (fail-safe)', () => {
  assert.equal(readAutoSubscribeOptOut(serviceTaskBo()), false);
  assert.equal(
    readAutoSubscribeOptOut(serviceTaskBo([zeebeProps(optOutProperty())])),
    true,
  );
  // Any other value means auto-subscribed (fail-safe).
  for (const v of ["true", "0", "no", ""]) {
    assert.equal(
      readAutoSubscribeOptOut(serviceTaskBo([zeebeProps(optOutProperty(v))])),
      false,
      `value ${JSON.stringify(v)} must not opt out`,
    );
  }
});

test("writeAutoSubscribeOptOut sets and clears the opt-out property", () => {
  const bo = serviceTaskBo([agentMarker()]);
  const modeling = applyingModeling();
  // set
  writeAutoSubscribeOptOut(moddle, modeling, {}, bo, true);
  assert.equal(readAutoSubscribeOptOut(bo), true);
  const container = bo.extensionElements?.values?.find(
    (v) => v.$type === ZEEBE_PROPERTIES_TYPE,
  );
  const prop = container?.properties?.find(
    (p) => p.name === AUTO_SUBSCRIBE_PROPERTY,
  );
  assert.equal(prop?.value, AUTO_SUBSCRIBE_OPT_OUT_VALUE);
  // setting again does not duplicate the property
  writeAutoSubscribeOptOut(moddle, modeling, {}, bo, true);
  assert.equal(
    container?.properties?.filter((p) => p.name === AUTO_SUBSCRIBE_PROPERTY)
      .length,
    1,
  );
  // clear removes the property and its now-empty container
  writeAutoSubscribeOptOut(moddle, modeling, {}, bo, false);
  assert.equal(readAutoSubscribeOptOut(bo), false);
  assert.equal(
    bo.extensionElements?.values?.some(
      (v) => v.$type === ZEEBE_PROPERTIES_TYPE,
    ),
    false,
  );
  // marker survives the opt-out lifecycle.
  assert.equal(hasExternalAgentMarker(bo), true);
});

test("writeAutoSubscribeOptOut preserves unrelated zeebe:property siblings", () => {
  const other: AgentModdleElement = {
    $type: ZEEBE_PROPERTY_TYPE,
    name: "some.other.prop",
    value: "keep-me",
  };
  const bo = serviceTaskBo([zeebeProps(other)]);
  const modeling = applyingModeling();
  writeAutoSubscribeOptOut(moddle, modeling, {}, bo, true);
  const container = bo.extensionElements?.values?.find(
    (v) => v.$type === ZEEBE_PROPERTIES_TYPE,
  );
  assert.equal(container?.properties?.length, 2);
  // Clearing the opt-out keeps the unrelated property and its container.
  writeAutoSubscribeOptOut(moddle, modeling, {}, bo, false);
  assert.deepEqual(
    container?.properties?.map((p) => p.name),
    ["some.other.prop"],
  );
  assert.ok(
    bo.extensionElements?.values?.some(
      (v) => v.$type === ZEEBE_PROPERTIES_TYPE,
    ),
  );
});

test("clearing an absent opt-out is a no-op (no empty container created)", () => {
  const bo = serviceTaskBo([agentMarker()]);
  writeAutoSubscribeOptOut(moddle, applyingModeling(), {}, bo, false);
  assert.equal(
    bo.extensionElements?.values?.some(
      (v) => v.$type === ZEEBE_PROPERTIES_TYPE,
    ),
    false,
  );
});
