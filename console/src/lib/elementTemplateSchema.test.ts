// Harness for the offline Monaco registration of the Camunda element-template
// JSON Schema — the element-template analog of `manifestSchema.test.ts`, and the
// guard for the "No schema request service available" error a `components/*.json`
// `$schema` would otherwise raise. Monaco is browser-only and can't be imported
// under `node --test`, so we test the pure entry builder + the drift guards that
// keep the registered content, the `$schema` URL our templates carry, and the
// pinned dependency all on the same version.
import { test } from "node:test";
import assert from "node:assert/strict";
// The very schema content that gets bundled + registered.
import elementTemplateSchema from "@camunda/zeebe-element-templates-json-schema/resources/schema.json" with { type: "json" };
// The pinned dependency's own manifest, to catch a version bump that forgets to
// update ELEMENT_TEMPLATE_SCHEMA_VERSION (and thus the `$schema` URL).
import pkg from "@camunda/zeebe-element-templates-json-schema/package.json" with { type: "json" };
import {
  ELEMENT_TEMPLATE_SCHEMA_VERSION,
  ELEMENT_TEMPLATE_SCHEMA_URL,
  ELEMENT_TEMPLATE_SCHEMA_ID,
  ELEMENT_TEMPLATE_SCHEMA_URIS,
  ELEMENT_TEMPLATE_FILE_MATCH,
  elementTemplateSchemaEntries,
} from "./elementTemplateSchema.ts";

test("keys the schema under the published $schema URL + canonical $id alias", () => {
  const entries = elementTemplateSchemaEntries(
    elementTemplateSchema as Record<string, unknown>,
  );
  const uris = new Set(entries.map((e) => e.uri));
  for (const u of ELEMENT_TEMPLATE_SCHEMA_URIS) assert.ok(uris.has(u), `missing ${u}`);
  // Primary: the unpkg URL our templates' `$schema` points at.
  assert.equal(ELEMENT_TEMPLATE_SCHEMA_URIS[0], ELEMENT_TEMPLATE_SCHEMA_URL);
  assert.ok(uris.has(ELEMENT_TEMPLATE_SCHEMA_URL));
  // Alias: the schema document's own `$id`.
  assert.ok(uris.has(ELEMENT_TEMPLATE_SCHEMA_ID));
});

test("the registered $id alias equals the schema document's own $id", () => {
  // Guards the domain-migration class of bug: if the schema's `$id` ever
  // changes, this fails unless the registered alias is updated too.
  assert.equal(
    (elementTemplateSchema as Record<string, unknown>)["$id"],
    ELEMENT_TEMPLATE_SCHEMA_ID,
  );
});

test("associates by component-dir file globs for missing/stale $schema", () => {
  const entries = elementTemplateSchemaEntries(
    elementTemplateSchema as Record<string, unknown>,
  );
  for (const entry of entries) {
    assert.deepEqual(entry.fileMatch, [...ELEMENT_TEMPLATE_FILE_MATCH]);
  }
  assert.ok(ELEMENT_TEMPLATE_FILE_MATCH.includes("**/components/*.json"));
  assert.ok(
    ELEMENT_TEMPLATE_FILE_MATCH.includes("**/.camunda/element-templates/*.json"),
  );
});

test("passes the bundled schema object through verbatim", () => {
  const entries = elementTemplateSchemaEntries(
    elementTemplateSchema as Record<string, unknown>,
  );
  for (const entry of entries) assert.equal(entry.schema, elementTemplateSchema);
});

test("the bundled URL version matches the pinned dependency version", () => {
  // If the dependency is bumped without updating ELEMENT_TEMPLATE_SCHEMA_VERSION,
  // Monaco would register content under a URL no `$schema` points at. The seed
  // templates (`urbanComponents.ts`) import ELEMENT_TEMPLATE_SCHEMA_URL directly,
  // so their `$schema` can never drift from this key by construction.
  assert.equal(ELEMENT_TEMPLATE_SCHEMA_VERSION, pkg.version);
  assert.ok(
    ELEMENT_TEMPLATE_SCHEMA_URL.includes(`@${pkg.version}/`),
    `URL ${ELEMENT_TEMPLATE_SCHEMA_URL} does not pin @${pkg.version}`,
  );
});
