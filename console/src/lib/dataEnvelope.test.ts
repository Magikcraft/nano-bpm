// Unit tests for the data-envelope model carrier (ADR 0033 §6).
// Node-native: run with `node --experimental-strip-types --test src/lib/dataEnvelope.test.ts`.
// Covers the pure carrier logic (moddle/modeling injected) without bpmn-js.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  ENVELOPE_KEY,
  referencedMessage,
  userTaskFormId,
  envelopeContext,
  readEnvelope,
  writeEnvelope,
  type EnvModdle,
  type EnvModdleElement,
  type EnvModeling,
} from "./dataEnvelope.ts";

// A minimal moddle stand-in: create() returns a tagged plain object.
const moddle: EnvModdle = {
  create(type, attrs) {
    return { $type: type, ...(attrs as Record<string, unknown>) } as EnvModdleElement;
  },
};

// Records the last updateModdleProperties call so tests can assert the shape.
function recordingModeling(): EnvModeling & { calls: Array<{ moddleElement: unknown; props: Record<string, unknown> }> } {
  const calls: Array<{ moddleElement: unknown; props: Record<string, unknown> }> = [];
  return {
    calls,
    updateModdleProperties(_element, moddleElement, props) {
      calls.push({ moddleElement, props });
    },
  };
}

function serviceTask(taskType?: string, extraExt: EnvModdleElement[] = []): EnvModdleElement {
  const values: EnvModdleElement[] = [...extraExt];
  if (taskType !== undefined) values.unshift({ $type: "zeebe:TaskDefinition", type: taskType });
  return { $type: "bpmn:ServiceTask", extensionElements: { $type: "bpmn:ExtensionElements", values } };
}

test("envelopeContext: service task with a literal worker type targets the element", () => {
  const bo = serviceTask("send-email");
  const ctx = envelopeContext({ type: "bpmn:ServiceTask", businessObject: bo });
  assert.ok(ctx);
  assert.equal(ctx.target, bo);
  assert.equal(ctx.taskType, "send-email");
});

test("envelopeContext: service task with a FEEL (=expr) type has no envelope", () => {
  const ctx = envelopeContext({ type: "bpmn:ServiceTask", businessObject: serviceTask("=taskTypeVar") });
  assert.equal(ctx, undefined);
});

test("envelopeContext: service task with no worker type has no envelope", () => {
  const ctx = envelopeContext({ type: "bpmn:ServiceTask", businessObject: serviceTask() });
  assert.equal(ctx, undefined);
});

test("envelopeContext: user task targets the element and exposes its linked form id", () => {
  const bo: EnvModdleElement = {
    $type: "bpmn:UserTask",
    extensionElements: { $type: "bpmn:ExtensionElements", values: [{ $type: "zeebe:FormDefinition", formId: "order-form" }] },
  };
  const ctx = envelopeContext({ type: "bpmn:UserTask", businessObject: bo });
  assert.ok(ctx);
  assert.equal(ctx.target, bo);
  assert.equal(ctx.taskType, undefined);
  assert.equal(ctx.formId, "order-form");
});

test("userTaskFormId: reads zeebe:FormDefinition formId, undefined when absent/embedded", () => {
  const withForm: EnvModdleElement = {
    extensionElements: { values: [{ $type: "zeebe:FormDefinition", formId: "f1" }] },
  };
  assert.equal(userTaskFormId(withForm), "f1");
  assert.equal(userTaskFormId({ extensionElements: { values: [{ $type: "zeebe:FormDefinition", formId: "" }] } }), undefined);
  assert.equal(userTaskFormId({}), undefined);
});

test("envelopeContext: message-bearing element targets the shared bpmn:Message", () => {
  const msg: EnvModdleElement = { $type: "bpmn:Message", id: "Msg_1", name: "OrderPlaced" };
  const bo: EnvModdleElement = {
    $type: "bpmn:IntermediateCatchEvent",
    eventDefinitions: [{ $type: "bpmn:MessageEventDefinition", messageRef: msg }],
  };
  const ctx = envelopeContext({ type: "bpmn:IntermediateCatchEvent", businessObject: bo });
  assert.ok(ctx);
  assert.equal(ctx.target, msg);
});

test("envelopeContext: a plain gateway/task with no boundary has no envelope", () => {
  assert.equal(envelopeContext({ type: "bpmn:ExclusiveGateway", businessObject: { $type: "bpmn:ExclusiveGateway" } }), undefined);
  assert.equal(envelopeContext(undefined), undefined);
});

test("referencedMessage: receiveTask uses messageRef, non-message events return undefined", () => {
  const msg: EnvModdleElement = { $type: "bpmn:Message", id: "M" };
  assert.equal(referencedMessage({ $type: "bpmn:ReceiveTask", messageRef: msg }), msg);
  assert.equal(referencedMessage({ $type: "bpmn:StartEvent", eventDefinitions: [{ $type: "bpmn:TimerEventDefinition" }] }), undefined);
});

test("readEnvelope: reads the reserved in/out property values, defaults to empty", () => {
  const bo: EnvModdleElement = {
    extensionElements: {
      values: [
        {
          $type: "zeebe:Properties",
          properties: [
            { name: ENVELOPE_KEY.inputType, value: "Order" },
            { name: ENVELOPE_KEY.outputType, value: "Receipt" },
          ],
        },
      ],
    },
  };
  assert.equal(readEnvelope(bo, "inputType"), "Order");
  assert.equal(readEnvelope(bo, "outputType"), "Receipt");
  assert.equal(readEnvelope({}, "inputType"), "");
});

test("writeEnvelope: creates extensionElements + zeebe:Properties when absent", () => {
  const modeling = recordingModeling();
  const target: EnvModdleElement = { $type: "bpmn:UserTask" };
  writeEnvelope(moddle, modeling, {}, target, "inputType", "Order");
  assert.equal(modeling.calls.length, 1);
  const { props } = modeling.calls[0];
  const ext = props.extensionElements as EnvModdleElement;
  assert.equal(ext.$type, "bpmn:ExtensionElements");
  const container = ext.values![0];
  assert.equal(container.$type, "zeebe:Properties");
  assert.equal(container.properties![0].name, ENVELOPE_KEY.inputType);
  assert.equal(container.properties![0].value, "Order");
});

test("writeEnvelope: appends a new zeebe:Properties into an existing extensionElements", () => {
  const modeling = recordingModeling();
  const ext: EnvModdleElement = { $type: "bpmn:ExtensionElements", values: [{ $type: "zeebe:TaskDefinition", type: "t" }] };
  const target: EnvModdleElement = { $type: "bpmn:ServiceTask", extensionElements: ext };
  writeEnvelope(moddle, modeling, {}, target, "outputType", "Receipt");
  assert.equal(modeling.calls.length, 1);
  assert.equal(modeling.calls[0].moddleElement, ext);
  const values = modeling.calls[0].props.values as EnvModdleElement[];
  assert.equal(values.length, 2);
  assert.equal(values[1].$type, "zeebe:Properties");
  assert.equal(values[1].properties![0].value, "Receipt");
});

test("writeEnvelope: updates an existing zeebe:Properties container, preserving other props", () => {
  const modeling = recordingModeling();
  const container: EnvModdleElement = {
    $type: "zeebe:Properties",
    properties: [
      { $type: "zeebe:Property", name: "other", value: "keep" },
      { $type: "zeebe:Property", name: ENVELOPE_KEY.inputType, value: "Old" },
    ],
  };
  const target: EnvModdleElement = {
    $type: "bpmn:ServiceTask",
    extensionElements: { $type: "bpmn:ExtensionElements", values: [container] },
  };
  writeEnvelope(moddle, modeling, {}, target, "inputType", "New");
  assert.equal(modeling.calls[0].moddleElement, container);
  const propsOut = modeling.calls[0].props.properties as EnvModdleElement[];
  const names = propsOut.map((p) => `${p.name}=${p.value}`);
  assert.deepEqual(names, ["other=keep", `${ENVELOPE_KEY.inputType}=New`]);
});

test("writeEnvelope: clearing removes only the reserved property", () => {
  const modeling = recordingModeling();
  const container: EnvModdleElement = {
    $type: "zeebe:Properties",
    properties: [
      { $type: "zeebe:Property", name: "other", value: "keep" },
      { $type: "zeebe:Property", name: ENVELOPE_KEY.inputType, value: "Old" },
    ],
  };
  const target: EnvModdleElement = {
    $type: "bpmn:ServiceTask",
    extensionElements: { $type: "bpmn:ExtensionElements", values: [container] },
  };
  writeEnvelope(moddle, modeling, {}, target, "inputType", "");
  const propsOut = modeling.calls[0].props.properties as EnvModdleElement[];
  assert.deepEqual(propsOut.map((p) => p.name), ["other"]);
});
