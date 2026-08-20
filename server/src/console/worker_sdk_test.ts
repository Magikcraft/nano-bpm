// Deno unit tests for the embedded worker SDK's strict job-frame decode.
//
// Run in CI by the `console-deno` job, and locally with:
//   deno test --allow-read --allow-write --allow-env server/src/console/
//
// Guards the defect class in Magikcraft/nano-bpm#940: a job frame that omits an
// identity key (`jobKey`/`processInstanceKey`) must fail loud at the decode
// boundary, never launder the gap into "" (which would complete/fail a job
// against an empty key).

import { assertEquals, assertThrows } from "jsr:@std/assert@1";

import { decodeJobIdentity, MalformedJobError } from "./worker_sdk.ts";

Deno.test("decodeJobIdentity accepts a well-formed job frame", () => {
  assertEquals(decodeJobIdentity({ jobKey: "7", processInstanceKey: "42", type: "t" }), {
    jobKey: "7",
    processInstanceKey: "42",
  });
});

Deno.test("decodeJobIdentity coerces numeric keys and honours the `key` alias", () => {
  assertEquals(decodeJobIdentity({ key: 7, processInstanceKey: 42 }), {
    jobKey: "7",
    processInstanceKey: "42",
  });
});

Deno.test("decodeJobIdentity throws on a missing jobKey instead of defaulting to ''", () => {
  assertThrows(() => decodeJobIdentity({ processInstanceKey: "42" }), MalformedJobError, "jobKey");
});

Deno.test("decodeJobIdentity throws on a missing processInstanceKey instead of defaulting to ''", () => {
  assertThrows(
    () => decodeJobIdentity({ jobKey: "7" }),
    MalformedJobError,
    "processInstanceKey",
  );
});

Deno.test("decodeJobIdentity rejects an empty jobKey (masking source)", () => {
  assertThrows(() => decodeJobIdentity({ jobKey: "", processInstanceKey: "42" }), MalformedJobError, "jobKey");
});

Deno.test("decodeJobIdentity rejects an empty / non-object frame", () => {
  assertThrows(() => decodeJobIdentity({}), MalformedJobError);
  assertThrows(() => decodeJobIdentity(undefined), MalformedJobError);
  assertThrows(() => decodeJobIdentity(null), MalformedJobError);
});
