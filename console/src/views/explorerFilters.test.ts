import { test } from "node:test";
import assert from "node:assert/strict";
import {
  applyFilterChange,
  filtersQueryKey,
  parseExplorerFilters,
  shouldResetPageOnFilterChange,
  toInstanceQuery,
} from "./explorerFilters.ts";

test("parseExplorerFilters returns the unconstrained defaults for empty params", () => {
  const filters = parseExplorerFilters(new URLSearchParams());
  assert.deepEqual(filters, { state: undefined, hasIncident: false });
});

test("parseExplorerFilters reads a valid state and the incident flag", () => {
  const filters = parseExplorerFilters(
    new URLSearchParams("state=Active&incident=1"),
  );
  assert.deepEqual(filters, { state: "Active", hasIncident: true });
});

test("parseExplorerFilters ignores an unknown state value (falls back to All)", () => {
  const filters = parseExplorerFilters(new URLSearchParams("state=Bogus"));
  assert.equal(filters.state, undefined);
});

test("parseExplorerFilters treats any incident value other than '1' as unchecked", () => {
  assert.equal(
    parseExplorerFilters(new URLSearchParams("incident=true")).hasIncident,
    false,
  );
  assert.equal(
    parseExplorerFilters(new URLSearchParams("incident=0")).hasIncident,
    false,
  );
});

test("toInstanceQuery omits unconstrained dimensions (no param = current behaviour)", () => {
  assert.deepEqual(
    toInstanceQuery({ state: undefined, hasIncident: false }),
    {},
  );
});

test("toInstanceQuery includes only the constrained dimensions", () => {
  assert.deepEqual(
    toInstanceQuery({ state: "Completed", hasIncident: false }),
    {
      state: "Completed",
    },
  );
  assert.deepEqual(toInstanceQuery({ state: undefined, hasIncident: true }), {
    hasIncident: true,
  });
  assert.deepEqual(toInstanceQuery({ state: "Active", hasIncident: true }), {
    state: "Active",
    hasIncident: true,
  });
});

test("filtersQueryKey is stable and distinguishes combinations", () => {
  assert.deepEqual(filtersQueryKey({ state: undefined, hasIncident: false }), [
    null,
    false,
  ]);
  assert.deepEqual(filtersQueryKey({ state: "Active", hasIncident: true }), [
    "Active",
    true,
  ]);
});

test("applyFilterChange sets and clears the state param", () => {
  const set = applyFilterChange(new URLSearchParams(), {
    kind: "state",
    state: "Terminated",
  });
  assert.equal(set.get("state"), "Terminated");

  const cleared = applyFilterChange(set, { kind: "state", state: undefined });
  assert.equal(cleared.get("state"), null);
});

test("applyFilterChange sets incident=1 and removes it when unchecked", () => {
  const set = applyFilterChange(new URLSearchParams(), {
    kind: "hasIncident",
    hasIncident: true,
  });
  assert.equal(set.get("incident"), "1");

  const cleared = applyFilterChange(set, {
    kind: "hasIncident",
    hasIncident: false,
  });
  assert.equal(cleared.get("incident"), null);
});

test("applyFilterChange preserves unrelated params (e.g. instance deep-link)", () => {
  const next = applyFilterChange(new URLSearchParams("instance=42"), {
    kind: "state",
    state: "Active",
  });
  assert.equal(next.get("instance"), "42");
  assert.equal(next.get("state"), "Active");
});

test("a filter change always resets the pager to the first page", () => {
  assert.equal(shouldResetPageOnFilterChange(), true);
});
