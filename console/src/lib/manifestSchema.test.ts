// Harness for the offline Monaco JSON schema loader (the fix for the console
// error "Unable to load schema from '…/nano-app.schema.json'. No schema request
// service available"). Monaco itself is browser-only ESM and cannot be imported
// under `node --test`, so we test the pure diagnostics-options builder that
// `manifestIntellisense.ts` hands to `monaco.languages.json.jsonDefaults.
// setDiagnosticsOptions`. These assertions lock the exact properties whose
// absence caused the bug.
import { test } from "node:test";
import assert from "node:assert/strict";
// Same package subpath the app imports (`@nanobpm/nano-app-schema/schema`), so
// the test validates against the very schema that gets bundled.
import appSchema from "@nanobpm/nano-app-schema/schema" with { type: "json" };
import {
  APP_SCHEMA_URIS,
  APP_MANIFEST_FILE_MATCH,
  LEGACY_APP_SCHEMA_URI,
  buildManifestSchemaOptions,
} from "./manifestSchema.ts";

test("registers the bundled schema without remote requests", () => {
  const opts = buildManifestSchemaOptions(appSchema as Record<string, unknown>);
  // Validation on…
  assert.equal(opts.validate, true);
  // …but never over the network — this is the property whose default (true)
  // produced "No schema request service available".
  assert.equal(opts.enableSchemaRequest, false);
  assert.ok(opts.schemas && opts.schemas.length === APP_SCHEMA_URIS.length);
});

test("keys the schema under the canonical + legacy-alias URLs", () => {
  const opts = buildManifestSchemaOptions(appSchema as Record<string, unknown>);
  const uris = new Set((opts.schemas ?? []).map((s) => s.uri));
  for (const u of APP_SCHEMA_URIS) assert.ok(uris.has(u), `missing ${u}`);
  // Canonical `nanobpm.io` host.
  assert.ok([...uris].some((u) => u.includes("nanobpm.io")));
  // Plus the retired host's alias (assembled from parts so its banned literal
  // token never appears in tracked sources), so a manifest whose `$schema`
  // still points at the old host resolves in-process too.
  assert.ok(uris.has(LEGACY_APP_SCHEMA_URI));
  assert.notEqual(LEGACY_APP_SCHEMA_URI, APP_SCHEMA_URIS[0]);
  assert.ok(LEGACY_APP_SCHEMA_URI.endsWith("/spec-app/nano-app.schema.json"));
});

test("the schema's own $id is one of the registered URLs", () => {
  // Guards the domain-migration class of bug directly: if the schema's `$id`
  // ever changes again, this fails unless the registration is updated too.
  const id = (appSchema as Record<string, unknown>)["$id"];
  assert.equal(typeof id, "string");
  assert.ok(
    (APP_SCHEMA_URIS as readonly string[]).includes(id as string),
    `schema $id ${id as string} not registered — a $schema fetch would fail`,
  );
});

test("associates by file name for missing/stale $schema", () => {
  const opts = buildManifestSchemaOptions(appSchema as Record<string, unknown>);
  for (const entry of opts.schemas ?? []) {
    assert.deepEqual(entry.fileMatch, [...APP_MANIFEST_FILE_MATCH]);
  }
  // A bare `nano.app.json` (no `$schema`) and any `*.nano.app.json` both match.
  assert.ok(APP_MANIFEST_FILE_MATCH.includes("nano.app.json"));
  assert.ok(APP_MANIFEST_FILE_MATCH.includes("*.nano.app.json"));
});

test("passes the bundled schema object through verbatim", () => {
  const opts = buildManifestSchemaOptions(appSchema as Record<string, unknown>);
  for (const entry of opts.schemas ?? []) {
    assert.equal(entry.schema, appSchema);
  }
});
