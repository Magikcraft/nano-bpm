// Unit tests for the SQL read/write classifier (issue #889). Node-native:
// run with `node --experimental-strip-types --test src/lib/sqlStatement.test.ts`.
import { test } from "node:test";
import assert from "node:assert/strict";
import { isReadStatement } from "./sqlStatement.ts";

test("plain reads are classified as reads", () => {
  for (const sql of [
    "SELECT 1",
    "  select * from t  ",
    "EXPLAIN QUERY PLAN SELECT 1",
    "explain select 1",
  ]) {
    assert.equal(isReadStatement(sql), true, sql);
  }
});

test("plain writes are classified as writes", () => {
  for (const sql of [
    "INSERT INTO t VALUES (1)",
    "update t set a = 1",
    "DELETE FROM t",
    "CREATE TABLE t (a int)",
    "DROP TABLE t",
    "REPLACE INTO t VALUES (1)",
  ]) {
    assert.equal(isReadStatement(sql), false, sql);
  }
});

test("a read-only CTE is a read but a data-modifying CTE is a write", () => {
  assert.equal(
    isReadStatement("WITH c AS (SELECT 1) SELECT * FROM c"),
    true,
    "WITH … SELECT",
  );
  // The defect: `WITH … INSERT/UPDATE/DELETE` must NOT slip through as a read.
  assert.equal(
    isReadStatement("WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c"),
    false,
    "WITH … INSERT",
  );
  assert.equal(
    isReadStatement("with c as (select 1) update t set a = 1"),
    false,
    "WITH … UPDATE",
  );
  assert.equal(
    isReadStatement("WITH c AS (SELECT 1) DELETE FROM t"),
    false,
    "WITH … DELETE",
  );
});

test("PRAGMA is a read only in its argument-less query form", () => {
  assert.equal(isReadStatement("PRAGMA foreign_keys"), true, "read PRAGMA");
  assert.equal(isReadStatement("pragma table_info"), true, "read PRAGMA");
  // The defect: an assigning / call-form PRAGMA mutates connection state.
  assert.equal(
    isReadStatement("PRAGMA foreign_keys = ON"),
    false,
    "assigning PRAGMA",
  );
  assert.equal(
    isReadStatement("pragma table_info(t)"),
    false,
    "call-form PRAGMA",
  );
});

test("leading SQL comments don't hide the verb", () => {
  // A read preceded by a comment must stay a read (the misclassification bug).
  assert.equal(isReadStatement("-- note\nSELECT 1"), true, "line-comment read");
  assert.equal(
    isReadStatement("/* note */ SELECT 1"),
    true,
    "block-comment read",
  );
  assert.equal(
    isReadStatement("  -- a\n  /* b */ select * from t"),
    true,
    "stacked-comment read",
  );
  assert.equal(
    isReadStatement("/* x */ WITH c AS (SELECT 1) SELECT * FROM c"),
    true,
    "commented CTE read",
  );
  // The conservative direction still holds: a commented write stays a write.
  assert.equal(
    isReadStatement("-- note\nINSERT INTO t VALUES (1)"),
    false,
    "line-comment write",
  );
  assert.equal(
    isReadStatement(
      "/* note */ WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c",
    ),
    false,
    "commented CTE write",
  );
  // An input that is only a comment is not a read.
  assert.equal(isReadStatement("-- just a comment"), false, "comment-only");
});
