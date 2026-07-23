// Tests for the form-field datasource binding core (ADR 0024 §5) — `node --test`.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  readFieldDataBinding,
  collectFormDataBindings,
  rowsToOptions,
  applyDataSourceOptions,
} from "../src/form-data-binding.ts";

const boundSchema = () => ({
  type: "default",
  schemaVersion: 18,
  components: [
    { type: "textfield", key: "note", id: "Field_note" },
    {
      type: "select",
      key: "customerId",
      id: "Field_customer",
      dataSource: {
        source: "app",
        query: "SELECT id AS value, name AS label FROM customers",
      },
    },
    {
      type: "group",
      id: "Group_1",
      components: [
        {
          type: "checklist",
          key: "tags",
          id: "Field_tags",
          dataSource: { source: "app", query: "SELECT name FROM tags", label: "name" },
        },
      ],
    },
  ],
});

test("readFieldDataBinding is strict about source + query", () => {
  assert.equal(readFieldDataBinding({}), undefined);
  assert.equal(readFieldDataBinding({ dataSource: {} }), undefined);
  assert.equal(readFieldDataBinding({ dataSource: { source: "app" } }), undefined);
  assert.equal(readFieldDataBinding({ dataSource: { source: "", query: "x" } }), undefined);
  assert.deepEqual(
    readFieldDataBinding({ dataSource: { source: "app", query: "SELECT 1", value: "v", label: "l" } }),
    { source: "app", query: "SELECT 1", value: "v", label: "l" },
  );
  // Blank column mappings are dropped so the defaults apply.
  assert.deepEqual(
    readFieldDataBinding({ dataSource: { source: "app", query: "SELECT 1", value: "" } }),
    { source: "app", query: "SELECT 1" },
  );
});

test("collectFormDataBindings finds bound fields, recursing layout", () => {
  const found = collectFormDataBindings(boundSchema());
  assert.deepEqual(
    found.map((f) => ({ key: f.fieldKey, id: f.fieldId, path: f.path, src: f.binding.source })),
    [
      { key: "customerId", id: "Field_customer", path: "/components/1", src: "app" },
      { key: "tags", id: "Field_tags", path: "/components/2/components/0", src: "app" },
    ],
  );
});

test("collectFormDataBindings tolerates junk schemas", () => {
  assert.deepEqual(collectFormDataBindings(null), []);
  assert.deepEqual(collectFormDataBindings({}), []);
  assert.deepEqual(collectFormDataBindings({ components: "nope" }), []);
});

test("rowsToOptions maps value/label with defaults", () => {
  const rows = [
    { value: 1, label: "Ann" },
    { value: 2, label: "Bo" },
  ];
  assert.deepEqual(rowsToOptions(rows, { source: "app", query: "q" }), [
    { value: "1", label: "Ann" },
    { value: "2", label: "Bo" },
  ]);
});

test("rowsToOptions honours custom columns and single-column fallback", () => {
  const rows = [{ id: 7, name: "Widget" }];
  assert.deepEqual(
    rowsToOptions(rows, { source: "app", query: "q", value: "id", label: "name" }),
    [{ value: "7", label: "Widget" }],
  );
  // Only a `name` column: it doubles as both value and label.
  assert.deepEqual(rowsToOptions([{ name: "Solo" }], { source: "app", query: "q", label: "name" }), [
    { value: "Solo", label: "Solo" },
  ]);
});

test("rowsToOptions coerces null/objects to strings", () => {
  const rows = [{ value: null, label: undefined }, { value: { a: 1 }, label: true }];
  assert.deepEqual(rowsToOptions(rows, { source: "app", query: "q" }), [
    { value: "", label: "" },
    { value: '{"a":1}', label: "true" },
  ]);
});

test("applyDataSourceOptions sets values without mutating the input", () => {
  const schema = boundSchema();
  const resolved = new Map([
    ["Field_customer", [{ value: "1", label: "Ann" }]],
    ["tags", [{ value: "urgent", label: "urgent" }]], // resolved by key fallback
  ]);
  const out = applyDataSourceOptions(schema, resolved);

  // Input untouched.
  assert.equal(schema.components[1].values, undefined);

  // Bound select got its live options; the plain textfield is unchanged.
  assert.deepEqual(out.components[1].values, [{ value: "1", label: "Ann" }]);
  assert.equal(out.components[0].values, undefined);
  // Nested checklist resolved via its key.
  assert.deepEqual(out.components[2].components[0].values, [
    { value: "urgent", label: "urgent" },
  ]);
});

test("applyDataSourceOptions leaves fields with no resolved options alone", () => {
  const schema = boundSchema();
  const out = applyDataSourceOptions(schema, new Map());
  assert.equal(out.components[1].values, undefined);
});
