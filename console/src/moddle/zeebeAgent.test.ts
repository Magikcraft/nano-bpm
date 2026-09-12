// Unit tests for the augmented zeebe moddle descriptor (#1180, #1186).
// Node-native: run with `node --experimental-strip-types --test src/moddle/zeebeAgent.test.ts`.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  AGENT_DEFINITION_MODDLE_TYPE,
  buildZeebeModdleWithAgent,
  zeebeModdleWithAgent,
} from "./zeebeAgent.ts";

const countAgentDefinition = (types: { name?: unknown }[]): number =>
  types.filter((t) => t?.name === AGENT_DEFINITION_MODDLE_TYPE.name).length;

test("appends the AgentDefinition shim when the stock descriptor lacks it", () => {
  const stock = { name: "Zeebe", types: [{ name: "Other" }] };
  const augmented = buildZeebeModdleWithAgent(stock);

  assert.equal(countAgentDefinition(augmented.types), 1);
  assert.ok(
    augmented.types.includes(AGENT_DEFINITION_MODDLE_TYPE),
    "the shim type is registered",
  );
  // The shared import must not be mutated — a fresh types array + object.
  assert.notEqual(augmented.types, stock.types);
  assert.equal(countAgentDefinition(stock.types), 0);
});

test("does NOT append a second AgentDefinition when the stock already ships one (upstream >=1.18)", () => {
  const upstreamAgentDefinition = { name: "AgentDefinition", properties: [] };
  const stock = {
    name: "Zeebe",
    types: [{ name: "Other" }, upstreamAgentDefinition],
  };
  const augmented = buildZeebeModdleWithAgent(stock);

  // Exactly one AgentDefinition — no duplicate registration that moddle rejects.
  assert.equal(countAgentDefinition(augmented.types), 1);
  // Defers to upstream's descriptor rather than shadowing it with the shim.
  assert.ok(augmented.types.includes(upstreamAgentDefinition));
  assert.ok(!augmented.types.includes(AGENT_DEFINITION_MODDLE_TYPE));
});

test("the real augmented descriptor carries exactly one AgentDefinition type", () => {
  assert.equal(countAgentDefinition(zeebeModdleWithAgent.types), 1);
});
