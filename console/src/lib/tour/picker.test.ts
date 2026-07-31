import { test } from "node:test";
import assert from "node:assert/strict";
import { pickerJourneys } from "./picker.ts";
import type { Journey } from "./types.ts";

const journey = (id: string, profiles: Journey["profiles"]): Journey => ({
  id,
  title: id,
  blurb: `${id} blurb`,
  profiles,
  steps: [],
  successEvent: () => true,
});

test("pickerJourneys drops the studio overview, keeps the outcome journeys", () => {
  const available = [
    journey("overview", ["studio"]),
    journey("agentic-sdlc", ["studio"]),
    journey("rad-prototype", ["studio"]),
  ];
  assert.deepEqual(
    pickerJourneys(available, "studio").map((j) => j.id),
    ["agentic-sdlc", "rad-prototype"],
  );
});

test("pickerJourneys drops the observe overview for the observe profile", () => {
  const available = [
    journey("overview-observe", ["observe"]),
    journey("localdev", ["observe"]),
  ];
  assert.deepEqual(
    pickerJourneys(available, "observe").map((j) => j.id),
    ["localdev"],
  );
});

test("pickerJourneys returns [] when only the overview is offerable", () => {
  assert.deepEqual(
    pickerJourneys([journey("overview", ["studio"])], "studio"),
    [],
  );
});

test("pickerJourneys preserves order and does not mutate its input", () => {
  const available = [
    journey("a", ["studio"]),
    journey("overview", ["studio"]),
    journey("b", ["studio"]),
  ];
  const out = pickerJourneys(available, "studio");
  assert.deepEqual(
    out.map((j) => j.id),
    ["a", "b"],
  );
  assert.equal(available.length, 3, "input array is not mutated");
});
