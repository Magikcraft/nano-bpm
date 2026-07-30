// Unit tests for project display-name → slug rules. Node-native: run with
// `node --experimental-strip-types --test src/lib/projectName.test.ts`.
// The slug logic must stay in lockstep with the server's `project_slug`
// (server/src/console/projects.rs) — the fixtures below mirror its tests.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  isSafeName,
  slugifyProjectName,
  validateProjectName,
  validateSafeName,
} from "./projectName.ts";

test("isSafeName mirrors the server's is_safe_name", () => {
  assert.equal(isSafeName("order"), true);
  assert.equal(isSafeName("order-v2"), true);
  assert.equal(isSafeName("order_2.final"), true);
  assert.equal(isSafeName(""), false);
  assert.equal(isSafeName("."), false);
  assert.equal(isSafeName("foo..bar"), false);
  assert.equal(isSafeName("a/b"), false);
  assert.equal(isSafeName("has space"), false);
  assert.equal(isSafeName("x".repeat(129)), false);
});

test("slugifyProjectName keeps safe names verbatim", () => {
  assert.equal(slugifyProjectName("MyApp"), "MyApp");
  assert.equal(slugifyProjectName("order_2.final"), "order_2.final");
});

test("slugifyProjectName slugs display names with spaces", () => {
  // Same fixtures as the server's project_slug tests.
  assert.equal(slugifyProjectName("Home Heating"), "home-heating");
  assert.equal(slugifyProjectName("  My   App!! "), "my-app");
  assert.equal(slugifyProjectName("v2 Beta"), "v2-beta");
  assert.equal(slugifyProjectName("!!!"), "");
  assert.equal(slugifyProjectName(""), "");
});

test("validateProjectName allows spaces and flags slug collisions", () => {
  const existing = [
    { name: "home-heating", displayName: "Home Heating" },
    { name: "plain" },
  ];
  assert.equal(validateProjectName("My Cool App", existing), null);
  assert.equal(validateProjectName("", existing), null); // incomplete, not an error
  // Collides by slug…
  assert.match(validateProjectName("Home  Heating", existing) ?? "", /exists/);
  // …by exact slug name…
  assert.match(validateProjectName("plain", existing) ?? "", /exists/);
  // …and by display name.
  assert.match(validateProjectName("home heating", existing) ?? "", /exists/);
  assert.match(validateProjectName("!!!", existing) ?? "", /letter or digit/);
  assert.match(validateProjectName("x".repeat(129), existing) ?? "", /long/);
});

test("validateSafeName keeps the strict no-spaces rules for import", () => {
  assert.equal(validateSafeName("my-app", []), null);
  assert.match(validateSafeName("my app", []) ?? "", /No spaces/);
  assert.match(validateSafeName("a..b", []) ?? "", /“\.\.”/);
  assert.match(
    validateSafeName("my-app", [{ name: "MY-APP" }]) ?? "",
    /exists/,
  );
  // An import must also collide against an existing project's display name,
  // not just its slug — otherwise two cards render the same visible title.
  assert.match(
    validateSafeName("MyApp", [{ name: "other", displayName: "myapp" }]) ?? "",
    /exists/,
  );
});
