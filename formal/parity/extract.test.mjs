// Tests for the Zeebe surface extractor (#1245): the Java-reading helpers, and
// the fail-loud contract — an upstream refactor that moves an anchor must stop
// extraction, never silently shrink the matrix.
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  Cells,
  ExtractError,
  FAMILIES,
  Source,
  enumConstants,
  lineAt,
  messageAt,
  messageExpr,
  slug,
  stripComments,
  validationMessages,
} from './extract.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const VALIDATION = 'zeebe/bpmn-model/src/main/java/io/camunda/zeebe/model/bpmn/validation/zeebe';
const PROCESSING = 'zeebe/engine/src/main/java/io/camunda/zeebe/engine/processing';
const INTENT = 'zeebe/protocol/src/main/java/io/camunda/zeebe/protocol/record/intent';

/** A throwaway Zeebe-shaped tree: `{ relPath: javaSource }`. */
function tree(files) {
  const root = mkdtempSync(join(tmpdir(), 'zeebe-fixture-'));
  for (const [rel, text] of Object.entries(files)) {
    mkdirSync(dirname(join(root, rel)), { recursive: true });
    writeFileSync(join(root, rel), text);
  }
  return root;
}

function run(family, files) {
  const root = tree(files);
  try {
    const cells = new Cells();
    FAMILIES[family](new Source(root), cells);
    return cells.sorted();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

test('stripComments blanks comments, keeps strings and line numbers', () => {
  const src = 'a // x\n/* y\n z */ b "// not a comment" c';
  const out = stripComments(src);
  assert.equal(out.split('\n').length, src.split('\n').length);
  assert.ok(!out.includes('x') && !out.includes('y'));
  assert.ok(out.includes('"// not a comment"'));
  assert.equal(lineAt(out, out.indexOf('b')), 3);
});

test('messageAt joins concatenated literals', () => {
  assert.equal(messageAt('f("a " + "b" + "c")', 0), 'a bc');
  assert.equal(messageAt('f(x)', 0, 3), null);
});

test('messageExpr reads literals, String.format and message constants', () => {
  assert.equal(messageExpr('"Must have a condition")', 0), 'Must have a condition');
  assert.equal(messageExpr('String.format("id %s", x))', 0), 'id %s');
  assert.equal(messageExpr('Helper.ERROR_MESSAGE_X, a)', 0), 'error_message_x');
  assert.equal(messageExpr('String.format(Helper.ERROR_X, a))', 0), 'error_x');
  assert.equal(messageExpr('error)', 0), null);
});

test('slug never truncates, so messages sharing a prefix stay distinct', () => {
  const a = 'Duplicate condition expression found in conditional start events of process';
  const b = 'Duplicate condition expression found in conditional start events of event subprocesses';
  assert.notEqual(slug(a), slug(b));
  assert.equal(slug("Can't have %s here"), 'can-t-have-here');
});

test('validationMessages resolves forwarded messages', () => {
  const at = (src, needle) => src.indexOf(needle) + needle.length;
  const lambda = 'ModelUtil.verifyThing(element, error -> c.addError(0, error));';
  assert.deepEqual(validationMessages(lambda, at(lambda, 'addError(0, '), 'x'), ['via-verifyThing']);

  const local = 'final String m = ok ? "first" : "second"; c.addError(0, m);';
  assert.deepEqual(validationMessages(local, at(local, 'addError(0, '), 'x'), ['first', 'second']);

  const param =
    'void run() { check(a, "one %s"); check(b, "two %s"); }\n' +
    'void check(final X x, final String template) { c.addError(0, String.format(template, x)); }';
  assert.deepEqual(validationMessages(param, at(param, 'addError(0, '), 'x'), ['one %s', 'two %s']);

  const getter = 'c.addError(0, failure.getMessage());';
  assert.deepEqual(validationMessages(getter, at(getter, 'addError(0, '), 'x'), ['dynamic-getMessage']);

  const odd = 'c.addError(0, build(a, b));';
  assert.throws(() => validationMessages(odd, at(odd, 'addError(0, '), 'x'), ExtractError);
});

test('enumConstants skips constructor arguments and bodies', () => {
  const src = 'enum E { A(1), @Deprecated B { void f() {} }, C; private int x; }';
  assert.deepEqual(enumConstants(src, 'E'), ['A', 'B', 'C']);
  assert.throws(() => enumConstants('class E {}', 'E'), ExtractError);
});

test('element family reads SUPPORTED_ELEMENT_TYPES and fails loud when it moves', () => {
  const rel = `${VALIDATION}/FlowElementValidator.java`;
  const cells = run('element', {
    [rel]: 'class V { static { SUPPORTED_ELEMENT_TYPES.add(ServiceTask.class);\n SUPPORTED_ELEMENT_TYPES.add(Task.class); } }',
  });
  assert.deepEqual(cells.map((c) => c.id), ['element:ServiceTask', 'element:Task']);
  assert.equal(cells[1].sources[0], `${rel}:2`);
  assert.throws(() => run('element', { [rel]: 'class V { Set<Class<?>> TYPES = Set.of(); }' }), ExtractError);
  assert.throws(() => run('element', {}), ExtractError);
});

test('intent family walks sub-packages and skips non-enum files', () => {
  const cells = run('intent', {
    [`${INTENT}/JobIntent.java`]: 'public enum JobIntent implements Intent { CREATED((short) 0), COMPLETED((short) 1); }',
    [`${INTENT}/Intent.java`]: 'public interface Intent { short value(); }',
    [`${INTENT}/scaling/ScaleIntent.java`]: 'public enum ScaleIntent { SCALE_UP; }',
  });
  assert.deepEqual(
    cells.map((c) => c.id),
    ['intent:Job:COMPLETED', 'intent:Job:CREATED', 'intent:Scale:SCALE_UP'],
  );
});

test('rejection family validates against the SBE enum and skips its sentinels', () => {
  const schema = 'zeebe/protocol/src/main/resources/protocol.xml';
  const xml = '<enum name="RejectionType" encodingType="uint8"><validValue name="NOT_FOUND">1</validValue></enum>';
  const cells = run('rejection', {
    [schema]: xml,
    [`${PROCESSING}/job/JobProcessor.java`]:
      'class JobProcessor { void f() { reject(RejectionType.NOT_FOUND); if (t == RejectionType.NULL_VAL) {} } }',
  });
  assert.deepEqual(cells.map((c) => c.id), ['rejection:JobProcessor:NOT_FOUND']);
  assert.throws(
    () => run('rejection', { [schema]: xml, [`${PROCESSING}/P.java`]: 'class P { Object r = RejectionType.GONE; }' }),
    ExtractError,
  );
});

test('the committed surface is well-formed and extracted at the pin', () => {
  const surface = JSON.parse(readFileSync(join(here, 'zeebe-surface.json'), 'utf8'));
  const pin = JSON.parse(readFileSync(join(here, 'zeebe-pin.json'), 'utf8'));
  assert.equal(surface.zeebe.sha, pin.sha);
  assert.equal(surface.zeebe.ref, pin.ref);
  const ids = surface.cells.map((c) => c.id);
  assert.deepEqual(ids, [...new Set(ids)].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0)));
  for (const c of surface.cells) {
    assert.match(c.id, /^[a-z-]+:\S+$/);
    assert.ok(c.sources.length > 0, c.id);
  }
  assert.deepEqual(new Set(ids.map((i) => i.split(':')[0])), new Set(Object.keys(FAMILIES)));
});
