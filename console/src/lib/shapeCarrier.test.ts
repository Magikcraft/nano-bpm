// Unit tests for the composed motion-shape model carrier (ADR 0040 §9).
// Node-native: run with `node --experimental-strip-types --test src/lib/shapeCarrier.test.ts`.
// Covers the pure carrier logic (moddle/modeling injected) without bpmn-js, and
// the round-trip: build → read reproduces exactly what the Rust scan lifts.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  buildShapesContainer,
  readShapes,
  type ShapeDecl,
  type ShapeModdle,
  type ShapeModdleElement,
  type ShapeModeling,
  writeShapes,
} from "./shapeCarrier.ts";

// A minimal moddle stand-in: create() returns a tagged plain object.
const moddle: ShapeModdle = {
  create(type, attrs) {
    return { $type: type, ...(attrs as Record<string, unknown>) } as ShapeModdleElement;
  },
};

function recordingModeling(): ShapeModeling & {
  calls: Array<{ moddleElement: unknown; props: Record<string, unknown> }>;
} {
  const calls: Array<{ moddleElement: unknown; props: Record<string, unknown> }> = [];
  return {
    calls,
    updateModdleProperties(_element, moddleElement, props) {
      calls.push({ moddleElement, props });
    },
  };
}

/** A process businessObject carrying a `nano:shapes` container built from decls. */
function processWithShapes(shapes: ShapeDecl[]): ShapeModdleElement {
  return {
    $type: "bpmn:Process",
    id: "orders",
    extensionElements: {
      $type: "bpmn:ExtensionElements",
      values: [buildShapesContainer(moddle, shapes)],
    },
  };
}

const sample: ShapeDecl[] = [
  {
    id: "ApprovedOrder",
    name: "Approved order",
    ops: [
      { op: "carry", ref: "Order" },
      { op: "project", ref: "Customer", fields: ["tier", "region"], via: "Order.customerId" },
      { op: "extend", name: "approved", type: "boolean" },
      { op: "extend", name: "reviewedBy", type: "string", optional: true },
      { op: "reference", name: "lines", ref: "OrderLine", list: true },
    ],
  },
];

test("readShapes: round-trips the four-op algebra in author order", () => {
  const bo = processWithShapes(sample);
  assert.deepEqual(readShapes(bo), sample);
});

test("readShapes: returns [] when the process declares no shapes", () => {
  assert.deepEqual(readShapes({ $type: "bpmn:Process" }), []);
  assert.deepEqual(readShapes(undefined), []);
});

test("readShapes: drops a shape with no id and malformed ops", () => {
  const bo: ShapeModdleElement = {
    $type: "bpmn:Process",
    extensionElements: {
      $type: "bpmn:ExtensionElements",
      values: [{
        $type: "nano:Shapes",
        shapes: [
          {
            $type: "nano:Shape",
            id: "  Ok  ",
            name: "  ",
            ops: [
              { $type: "nano:Carry" }, // no ref → dropped
              { $type: "nano:Extend", name: "approved" }, // no type → dropped
              { $type: "nano:Project", ref: "Customer" }, // no fields → dropped
              { $type: "nano:Project", ref: "Customer", fields: "  " }, // empty fields → dropped
              { $type: "nano:Carry", ref: "Order" },
            ],
          },
          { $type: "nano:Shape", ops: [{ $type: "nano:Carry", ref: "Order" }] }, // no id → skipped
        ],
      }],
    },
  };
  assert.deepEqual(readShapes(bo), [{ id: "Ok", ops: [{ op: "carry", ref: "Order" }] }]);
});

test("buildShapesContainer: serialises project fields as a comma list and omits false flags", () => {
  const container = buildShapesContainer(moddle, sample);
  const project = container.shapes?.[0].ops?.find((o) => o.$type === "nano:Project");
  assert.equal(project?.fields, "tier, region");
  assert.equal(project?.via, "Order.customerId");
  const carry = container.shapes?.[0].ops?.find((o) => o.$type === "nano:Carry");
  assert.equal(carry?.spread, undefined);
  const optionalExtend = container.shapes?.[0].ops?.find(
    (o) => o.$type === "nano:Extend" && o.name === "reviewedBy",
  );
  assert.equal(optionalExtend?.optional, true);
});

test("writeShapes: creates the extensionElements + shapes container in one command", () => {
  const bo: ShapeModdleElement = { $type: "bpmn:Process", id: "orders" };
  const modeling = recordingModeling();
  writeShapes(moddle, modeling, {}, bo, sample);
  assert.equal(modeling.calls.length, 1);
  const { moddleElement, props } = modeling.calls[0];
  // No prior extension elements: the target is the process, with a fresh container.
  assert.equal((moddleElement as ShapeModdleElement).$type, "bpmn:Process");
  const ext = props.extensionElements as ShapeModdleElement;
  assert.equal(ext.$type, "bpmn:ExtensionElements");
  assert.equal(ext.values?.length, 1);
  assert.equal(ext.values?.[0].$type, "nano:Shapes");
  // The written container reads back to the original decls.
  assert.deepEqual(readShapes({ ...bo, extensionElements: ext }), sample);
});

test("writeShapes: preserves other extension elements and swaps only the shapes container", () => {
  const meta: ShapeModdleElement = { $type: "nano:Meta", key: "classification", value: "internal" };
  const bo: ShapeModdleElement = {
    $type: "bpmn:Process",
    extensionElements: {
      $type: "bpmn:ExtensionElements",
      values: [meta, buildShapesContainer(moddle, [{ id: "Old", ops: [] }])],
    },
  };
  const modeling = recordingModeling();
  writeShapes(moddle, modeling, {}, bo, sample);
  const values = modeling.calls[0].props.values as ShapeModdleElement[];
  assert.equal(values.length, 2);
  assert.equal(values[0], meta); // meta preserved
  assert.equal(values[1].$type, "nano:Shapes");
  const nextExt: ShapeModdleElement = { $type: "bpmn:ExtensionElements", values };
  assert.deepEqual(readShapes({ ...bo, extensionElements: nextExt }), sample);
});

test("writeShapes: replaces the container in place when it is not the last sibling", () => {
  // The `nano:Shapes` container precedes `nano:meta`; a correct in-place swap keeps
  // it at index 0 (the old append-at-end path would reorder to [meta, shapes]).
  const meta: ShapeModdleElement = { $type: "nano:Meta", key: "k", value: "v" };
  const bo: ShapeModdleElement = {
    $type: "bpmn:Process",
    extensionElements: {
      $type: "bpmn:ExtensionElements",
      values: [buildShapesContainer(moddle, [{ id: "Old", ops: [] }]), meta],
    },
  };
  const modeling = recordingModeling();
  writeShapes(moddle, modeling, {}, bo, sample);
  const values = modeling.calls[0].props.values as ShapeModdleElement[];
  assert.equal(values.length, 2);
  assert.equal(values[0].$type, "nano:Shapes"); // container stays first
  assert.equal(values[1], meta); // meta stays after it
});

test("writeShapes: an empty shape list removes the container but keeps meta", () => {
  const meta: ShapeModdleElement = { $type: "nano:Meta", key: "k", value: "v" };
  const bo: ShapeModdleElement = {
    $type: "bpmn:Process",
    extensionElements: {
      $type: "bpmn:ExtensionElements",
      values: [meta, buildShapesContainer(moddle, [{ id: "Old", ops: [] }])],
    },
  };
  const modeling = recordingModeling();
  writeShapes(moddle, modeling, {}, bo, []);
  const values = modeling.calls[0].props.values as ShapeModdleElement[];
  assert.deepEqual(values, [meta]);
  // The rebuilt extension elements carry no shapes container, so it reads back empty.
  const nextExt: ShapeModdleElement = { $type: "bpmn:ExtensionElements", values };
  assert.deepEqual(readShapes({ ...bo, extensionElements: nextExt }), []);
});
