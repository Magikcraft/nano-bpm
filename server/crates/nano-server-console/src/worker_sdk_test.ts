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

import { decodeJobIdentity, defineWorker, MalformedJobError } from "./worker_sdk.ts";

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

// End-to-end guard for the dispatch recovery path (not just the pure decoder):
// a live `type: "job"` frame whose payload is absent must be routed to recovery
// — record the error, emit the status, replenish exactly one credit — and must
// never reach the handler. Driven through a fake WebSocket + stdout capture so
// the observable side effects (`ws.send` credit, emitted STATUS line) are real.
// The worker runs unbounded timers and a signal listener with no teardown
// handle, so leak sanitizers are disabled for this lifecycle test.
Deno.test({
  name: "dispatch recovers a job frame with an absent payload without invoking the handler",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: () => {
    const dec = new TextDecoder();
    const stdout: string[] = [];
    const realWrite = Deno.stdout.writeSync.bind(Deno.stdout);
    Deno.stdout.writeSync = (b: Uint8Array): number => {
      stdout.push(dec.decode(b));
      return b.length;
    };

    const sent: Record<string, unknown>[] = [];
    let inst: FakeWS | undefined;
    class FakeWS {
      static readonly OPEN = 1;
      readyState = 1;
      onopen: (() => void) | null = null;
      onmessage: ((ev: { data: string }) => void) | null = null;
      onerror: (() => void) | null = null;
      onclose: ((ev: { code: number }) => void) | null = null;
      constructor(_url: string) {
        inst = this;
      }
      send(data: string): void {
        sent.push(JSON.parse(data));
      }
      close(): void {}
    }
    const g = globalThis as Record<string, unknown>;
    const realWS = g.WebSocket;
    g.WebSocket = FakeWS;

    let handlerCalls = 0;
    try {
      defineWorker({
        type: "greet",
        baseUrl: "http://127.0.0.1:9999",
        handle: () => {
          handlerCalls += 1;
        },
      });

      // Handshake, then a job frame whose `job` payload is absent (undefined is
      // dropped by JSON.stringify, mirroring a gateway frame with no payload).
      inst!.onmessage!({ data: JSON.stringify({ type: "welcome", heartbeatMs: 0 }) });
      inst!.onmessage!({ data: JSON.stringify({ type: "job" }) });

      // The handler must never run for a frame we cannot identify.
      assertEquals(handlerCalls, 0);

      // Exactly one credit is replenished so the stream keeps flowing.
      const credits = sent.filter((f) => f.type === "jobCredits");
      assertEquals(credits.length, 1);
      assertEquals(credits[0], { type: "jobCredits", jobType: "greet", n: 1 });

      // The malformed frame is surfaced as a STATUS line (also recorded as lastError).
      assertEquals(stdout.join("").includes("malformed job frame"), true);
    } finally {
      Deno.stdout.writeSync = realWrite;
      g.WebSocket = realWS;
    }
  },
});
