import { test } from "node:test";
import assert from "node:assert/strict";
import { decideAppViewMessage } from "./appViewMessage.ts";

test("nano-app-ready asks the host to reply with the theme", () => {
  assert.deepEqual(decideAppViewMessage({ type: "nano-app-ready" }), {
    kind: "theme",
  });
});

test("nano-navigate to processExplorer builds an in-console deep link", () => {
  assert.deepEqual(
    decideAppViewMessage({
      type: "nano-navigate",
      target: "processExplorer",
      params: { instance: "2251799813685249" },
    }),
    { kind: "navigate", path: "/explorer?instance=2251799813685249" },
  );
});

test("the instance key is trimmed and URL-encoded (path built host-side, never a raw href)", () => {
  assert.deepEqual(
    decideAppViewMessage({
      type: "nano-navigate",
      target: "processExplorer",
      params: { instance: "  a b/c?d=e  " },
    }),
    { kind: "navigate", path: "/explorer?instance=a%20b%2Fc%3Fd%3De" },
  );
});

test("an unknown navigate target is ignored (whitelist, not passthrough)", () => {
  assert.equal(
    decideAppViewMessage({
      type: "nano-navigate",
      target: "somethingElse",
      params: { instance: "x" },
    }),
    null,
  );
});

test("a nano-navigate with a missing or blank instance is ignored", () => {
  assert.equal(
    decideAppViewMessage({ type: "nano-navigate", target: "processExplorer" }),
    null,
  );
  assert.equal(
    decideAppViewMessage({
      type: "nano-navigate",
      target: "processExplorer",
      params: { instance: "   " },
    }),
    null,
  );
  assert.equal(
    decideAppViewMessage({
      type: "nano-navigate",
      target: "processExplorer",
      params: { instance: 123 },
    }),
    null,
  );
});

test("unknown types and non-object payloads are ignored", () => {
  assert.equal(decideAppViewMessage({ type: "nano-theme" }), null);
  assert.equal(decideAppViewMessage(null), null);
  assert.equal(decideAppViewMessage("nano-app-ready"), null);
  assert.equal(decideAppViewMessage(42), null);
  assert.equal(decideAppViewMessage(undefined), null);
});
