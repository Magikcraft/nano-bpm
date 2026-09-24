// Tests for the coverage guard (#1245): one test per failure mode it must catch.
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

import { CORPUS, evaluate, globToRegExp, markdownReport, tally } from './check.mjs';

const RS = 'engine-core/src/engine/tests/lifecycle.rs';
const FILES = {
  [RS]: '#[test]\nfn runs_a_task() {}\n\nfn helper() {}\n',
  [`${CORPUS}reject-x.bpmn`]: '<!-- verdict: reject | category: X -->\n<definitions/>',
  [`${CORPUS}diverge-y.bpmn`]: '<!-- verdict: diverge | category: Y -->\n<definitions/>',
};
const fs = { exists: (p) => p in FILES, read: (p) => FILES[p] };
const PIN = { sha: 'abc' };
const surface = (...ids) => ({ zeebe: { ref: 'stable/x', sha: 'abc' }, cells: ids.map((id) => ({ id, sources: ['s'] })) });
const check = (ids, rules) => evaluate(surface(...ids), { rules }, fs, PIN).errors;

const tested = { status: 'nano-tested', evidence: [`${RS}::runs_a_task`] };
const gap = { status: 'gap', issue: 1249, note: 'later' };

test('a fully mapped surface passes', () => {
  assert.deepEqual(
    check(['element:Task', 'intent:Job:CREATED'], [{ match: 'element:Task', ...tested }, { match: 'intent:*', ...gap }]),
    [],
  );
});

test('an unmapped cell fails — including one a Zeebe bump adds', () => {
  assert.deepEqual(check(['element:Task', 'element:New'], [{ match: 'element:Task', ...tested }]), [
    'unmapped cell: element:New',
  ]);
});

test('a dead rule fails, and the first matching rule wins', () => {
  const errors = check(['element:Task'], [{ match: 'element:*', ...gap }, { match: 'element:Task', ...tested }]);
  assert.deepEqual(errors, ['dead rule (claims no cell): element:Task']);
  const { assignments } = evaluate(surface('element:Task'), { rules: [{ match: 'element:*', ...gap }] }, fs, PIN);
  assert.equal(assignments.get('element:Task'), 'gap');
});

test('nano-tested evidence must name an existing #[test] fn', () => {
  const rule = (ref) => [{ match: 'element:Task', status: 'nano-tested', evidence: [ref] }];
  assert.match(check(['element:Task'], rule(`${RS}::renamed_away`))[0], /no #\[test\] fn renamed_away/);
  assert.match(check(['element:Task'], rule(`${RS}::helper`))[0], /no #\[test\] fn helper/);
  assert.match(check(['element:Task'], rule('engine-core/gone.rs::runs_a_task'))[0], /missing file/);
  assert.match(check(['element:Task'], rule(RS))[0], /must be path::test_fn/);
  assert.match(check(['element:Task'], [{ match: 'element:Task', status: 'nano-tested' }])[0], /needs evidence/);
});

test('parity evidence must be a corpus fixture with a Zeebe verdict', () => {
  const rule = (ref) => [{ match: 'validation:V:m', status: 'parity', evidence: [ref] }];
  assert.deepEqual(check(['validation:V:m'], rule(`${CORPUS}reject-x.bpmn`)), []);
  assert.match(check(['validation:V:m'], rule(`${CORPUS}diverge-y.bpmn`))[0], /'diverge' is not Zeebe parity/);
  assert.match(check(['validation:V:m'], rule(`${CORPUS}absent.bpmn`))[0], /missing fixture/);
  assert.match(check(['validation:V:m'], rule(`${RS}::runs_a_task`))[0], /must be a .*\.bpmn fixture/);
});

test('gap and out-of-scope need an issue and a note, and take no evidence', () => {
  const cell = ['intent:Scale:SCALE_UP'];
  assert.match(check(cell, [{ match: 'intent:*', status: 'gap', note: 'n' }])[0], /needs an issue/);
  assert.match(check(cell, [{ match: 'intent:*', status: 'out-of-scope', issue: 1240 }])[0], /needs a note/);
  assert.match(check(cell, [{ match: 'intent:*', ...gap, evidence: ['x'] }])[0], /takes no evidence/);
  assert.match(check(cell, [{ match: 'intent:*', ...gap, issue: '#1249' }])[0], /needs an issue number/);
});

test('malformed rules fail', () => {
  assert.match(check(['element:Task'], [{ match: 'element:Task', ...tested, why: 'x' }])[0], /unknown key 'why'/);
  assert.match(check(['element:Task'], [{ match: 'element:Task', status: 'done' }])[0], /status must be one of/);
  assert.deepEqual(evaluate(surface('element:Task'), {}, fs, PIN).errors.slice(0, 1), ['coverage.json must have a rules array']);
});

test('a surface extracted at another Zeebe revision fails', () => {
  const errors = evaluate(surface('element:Task'), { rules: [{ match: '*', ...gap }] }, fs, { sha: 'def' }).errors;
  assert.match(errors[0], /pins def: regenerate it/);
});

test('globs treat only * as a wildcard', () => {
  assert.ok(globToRegExp('validation:V:*').test('validation:V:a-b'));
  assert.ok(!globToRegExp('guard:a.b').test('guard:aXb'));
});

test('the report tallies statuses per family', () => {
  const { assignments } = evaluate(
    surface('element:Task', 'intent:Job:CREATED', 'intent:Job:COMPLETED'),
    { rules: [{ match: 'element:*', ...tested }, { match: 'intent:*', ...gap }] },
    fs,
    PIN,
  );
  const counts = tally(assignments);
  assert.deepEqual(counts.intent, { parity: 0, 'nano-tested': 0, gap: 2, 'out-of-scope': 0 });
  const md = markdownReport(counts, surface());
  assert.match(md, /\| element \| 0 \| 1 \| 0 \| 0 \| 1 \|/);
  assert.match(md, /\| \*\*all\*\* \| \*\*0\*\* \| \*\*1\*\* \| \*\*2\*\* \| \*\*0\*\* \| \*\*3\*\* \|/);
});

test('the committed coverage.json maps the committed surface', () => {
  const here = dirname(fileURLToPath(import.meta.url));
  const root = join(here, '..', '..');
  const load = (f) => JSON.parse(readFileSync(join(here, f), 'utf8'));
  const repo = { exists: (p) => existsSync(join(root, p)), read: (p) => readFileSync(join(root, p), 'utf8') };
  const { errors } = evaluate(load('zeebe-surface.json'), load('coverage.json'), repo, load('zeebe-pin.json'));
  assert.deepEqual(errors, []);
});
