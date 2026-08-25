import { test } from "node:test";
import assert from "node:assert/strict";
import { fmtClock } from "./traceTime.ts";

// fmtClock renders a local-time-zone stamp as `H:MMam/pm Mon D` (e.g.
// `11:23am Aug 25`). The exact time-of-day and month depend on the host zone,
// so assert the SHAPE (which is what the format contract guarantees) rather
// than a zone-specific literal.
test("fmtClock renders `H:MMam/pm Mon D` in the local zone", () => {
  const out = fmtClock(Date.UTC(2026, 7, 25, 11, 23, 45));
  assert.match(out, /^\d{1,2}:\d{2}(am|pm) [A-Z][a-z]{2} \d{1,2}$/);
});

test("fmtClock keeps a 12-hour clock with a lowercase am/pm and no seconds", () => {
  const out = fmtClock(Date.UTC(2026, 0, 5, 9, 5, 0));
  assert.ok(!out.includes(":00 ") && !/\d:\d{2}:\d{2}/.test(out), "no seconds");
  assert.ok(/(am|pm)/.test(out), "carries a lowercase meridiem");
});
