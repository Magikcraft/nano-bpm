// Tests for the TLA+ value parser in parse.mjs. TLC renders a finite function
// with string-domain keys using the record syntax but QUOTES the key
// (`["S" |-> "start"]`), which the bare-key scan silently misparsed — the
// `kind`/`edges`/state maps generated from `MCNodes`/`MCFlows` therefore made
// gen-traces.sh fail before it could emit a fixture (Copilot review, PR #1268).
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { parseTlaValue } from './parse.mjs';

test('parses a record with bare identifier keys', () => {
  assert.deepEqual(parseTlaValue('[a |-> 1, b |-> "x"]'), { a: 1, b: 'x' });
});

test('parses a record with quoted string keys (TLC function rendering)', () => {
  assert.deepEqual(parseTlaValue('["S" |-> "start"]'), { S: 'start' });
});

test('parses a record mixing quoted and bare keys', () => {
  assert.deepEqual(
    parseTlaValue('["S" |-> "start", n2 |-> 3]'),
    { S: 'start', n2: 3 },
  );
});

test('parses quoted keys with non-identifier characters', () => {
  assert.deepEqual(parseTlaValue('["s-1" |-> TRUE]'), { 's-1': true });
});

test('parses nested records with quoted keys', () => {
  assert.deepEqual(
    parseTlaValue('["kind" |-> ["n1" |-> "task"]]'),
    { kind: { n1: 'task' } },
  );
});

test('still parses functions in the (k :> v @@ ...) form', () => {
  assert.deepEqual(parseTlaValue('("a" :> 1 @@ "b" :> 2)'), { a: 1, b: 2 });
});
