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
  promptLinkedResource,
  readPromptBinding,
  isAgentTask,
  writePromptLink,
  removePromptLink,
  writeAppendPrompt,
  removePromptBinding,
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

// A service task business object with the given extension children.
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

test("writePromptLink omits resourceId when it is blank (never emits resourceId=\"\")", () => {
  // Toggling the agent-task switch on (or clearing the resource field) writes a
  // blank resourceId. An empty `resourceId=""` is an invalid linkedResource the
  // engine rejects on deploy, so we must omit the attribute rather than serialize
  // it empty — the linkName="prompt" marker still identifies the agent task.
  const bo = serviceTaskBo();
  writePromptLink(moddle, applyingModeling(), {}, bo, "", PROMPT_DEFAULT_BINDING_TYPE);
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
  writePromptLink(moddle, applyingModeling(), {}, bo2, "   ", PROMPT_DEFAULT_BINDING_TYPE);
  assert.equal(
    Object.prototype.hasOwnProperty.call(promptLinkedResource(bo2)!, "resourceId"),
    false,
  );
});

test("writePromptLink drops resourceId when an existing binding's resource is cleared", () => {
  const bo = serviceTaskBo([linkedResources(promptLink("feature.md", "latest"))]);
  writePromptLink(moddle, applyingModeling(), {}, bo, "", "latest");
  const link = promptLinkedResource(bo);
  assert.ok(link);
  assert.equal(Object.prototype.hasOwnProperty.call(link!, "resourceId"), false);
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
    inputParameters: [{ $type: "zeebe:Input", source: "=repo", target: "repo" }],
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
    inputParameters: [{ $type: "zeebe:Input", source: "=repo", target: "repo" }],
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
