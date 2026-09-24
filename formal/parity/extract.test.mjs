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
  HOOK_TRANSITIONS,
  Source,
  enumConstants,
  interfaceMethods,
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

test('element family fails on any SUPPORTED_ELEMENT_TYPES form it does not read', () => {
  const rel = `${VALIDATION}/FlowElementValidator.java`;
  const decl = 'static final Set<Class<?>> SUPPORTED_ELEMENT_TYPES = new HashSet<>();\n';
  assert.equal(run('element', { [rel]: `${decl}static { SUPPORTED_ELEMENT_TYPES.add(Task.class); }` }).length, 1);
  assert.throws(
    () => run('element', { [rel]: `${decl}static { SUPPORTED_ELEMENT_TYPES.add(Task.class); SUPPORTED_ELEMENT_TYPES.addAll(MORE); }` }),
    /unrecognised use of SUPPORTED_ELEMENT_TYPES/,
  );
  assert.throws(
    () => run('element', { [rel]: 'static final Set<Class<?>> SUPPORTED_ELEMENT_TYPES = Set.of(Task.class);\nstatic { SUPPORTED_ELEMENT_TYPES.add(A.class); }' }),
    /unrecognised use/,
  );
});

const BPMN = `${PROCESSING}/bpmn`;
const API = {
  [`${BPMN}/BpmnElementProcessor.java`]:
    'public interface BpmnElementProcessor<T> {\n  Class<T> getType();\n' +
    Object.keys(HOOK_TRANSITIONS)
      .filter((h) => !/Child|ExecutionPath/.test(h))
      .map((h) => `  default Either<Failure, ?> ${h}(final T element, final BpmnElementContext context) { return null; }\n`)
      .join('') +
    '}',
  [`${BPMN}/BpmnElementContainerProcessor.java`]:
    'public interface BpmnElementContainerProcessor<T> extends BpmnElementProcessor<T> {\n' +
    Object.keys(HOOK_TRANSITIONS)
      .filter((h) => /Child|ExecutionPath/.test(h))
      .map((h) => `  default void ${h}(final T element, final BpmnElementContext a, final BpmnElementContext b) {}\n`)
      .join('') +
    '}',
};
const STREAM = `${BPMN}/BpmnStreamProcessor.java`;
const STREAM_SRC =
  'class BpmnStreamProcessor {\n  private void processEvent(final ProcessInstanceIntent intent, final P p, final E e) {\n' +
  '    switch (intent) {\n      case ACTIVATE_ELEMENT:\n        a();\n        break;\n      case COMPLETE_ELEMENT:\n        break;\n' +
  '      case TERMINATE_ELEMENT:\n        break;\n      case COMPLETE_EXECUTION_LISTENER:\n        switch (s) {\n          case ELEMENT_ACTIVATING -> x();\n        }\n        break;\n' +
  '      case CONTINUE_TERMINATING_ELEMENT:\n        break;\n      default:\n        throw new X();\n    }\n  }\n}';
const PROCESSORS = {
  ...API,
  [STREAM]: STREAM_SRC,
  [`${BPMN}/BpmnElementProcessors.java`]: 'class BpmnElementProcessors { void f() { processors.put(BpmnElementType.TASK, new TaskProcessor(a)); } }',
  [`${BPMN}/task/TaskProcessor.java`]:
    'public class TaskProcessor implements BpmnElementProcessor<X> {\n  public Either<Failure, ?> onActivate(final X e, final C c) { return null; }\n' +
    '  public void onChildCompleting(final X e, final C a, final C b) {}\n}',
};

test('lifecycle commands come from processEvent and hooks from the processor interfaces', () => {
  assert.deepEqual(interfaceMethods(API[`${BPMN}/BpmnElementProcessor.java`], 'BpmnElementProcessor').slice(0, 2), [
    'getType',
    'onActivate',
  ]);
  const cells = run('lifecycle', PROCESSORS);
  assert.deepEqual(cells.map((c) => c.id), [
    'lifecycle:TASK:activate',
    'lifecycle:TASK:child-completing',
    'lifecycle:TASK:complete',
    'lifecycle:TASK:complete-execution-listener',
    'lifecycle:TASK:continue-terminating',
    'lifecycle:TASK:terminate',
  ]);
  assert.deepEqual(cells[0].detail, { processor: 'TaskProcessor', hooks: ['onActivate'] });
});

test('lifecycle hooks are inherited through extends, and an unreadable parent fails', () => {
  const task = `${BPMN}/task/TaskProcessor.java`;
  const inherited = {
    ...PROCESSORS,
    [task]: 'public class TaskProcessor extends BaseProcessor<X> {\n}',
    [`${BPMN}/task/BaseProcessor.java`]:
      'public abstract class BaseProcessor<T> implements BpmnElementProcessor<T> {\n  protected Either<Failure, ?> onTerminateInternal(final T e, final C c) { return null; }\n}',
  };
  const cells = run('lifecycle', inherited);
  assert.deepEqual(cells.find((c) => c.id === 'lifecycle:TASK:terminate').detail, {
    processor: 'TaskProcessor',
    hooks: ['onTerminateInternal'],
  });
  const overridden = {
    ...inherited,
    [task]: 'public class TaskProcessor extends BaseProcessor<X> {\n  protected Either<Failure, ?> onTerminateInternal(final X e, final C c) { return null; }\n}',
  };
  const override = run('lifecycle', overridden).find((c) => c.id === 'lifecycle:TASK:terminate');
  assert.deepEqual(override.detail.hooks, ['onTerminateInternal']);
  const qualified = { ...inherited, [task]: 'public class TaskProcessor extends io.camunda.task.BaseProcessor<X> {\n}' };
  assert.deepEqual(run('lifecycle', qualified).find((c) => c.id === 'lifecycle:TASK:terminate').detail.hooks, ['onTerminateInternal']);
  const farAway = { ...PROCESSORS, [task]: 'public class TaskProcessor extends io.elsewhere.OtherProcessor<X> {\n}' };
  assert.throws(() => run('lifecycle', farAway), /extends io\.elsewhere\.OtherProcessor, which is not under/);
  const external = { ...PROCESSORS, [task]: 'public class TaskProcessor extends ElsewhereProcessor<X> {\n}' };
  assert.throws(() => run('lifecycle', external), /extends ElsewhereProcessor, which is not under/);
});

test('lifecycle fails on an unmapped or stale hook, or an unread registration', () => {
  const api = `${BPMN}/BpmnElementProcessor.java`;
  const added = { ...PROCESSORS, [api]: PROCESSORS[api].replace('Class<T> getType();', 'Class<T> getType();\n  default void onMigrate(final T e) {}') };
  assert.throws(() => run('lifecycle', added), /processor hook onMigrate has no lifecycle transition/);
  const removed = { ...PROCESSORS, [api]: PROCESSORS[api].replace(/\n  default [^\n]*finalizeTermination[^\n]*/, '') };
  assert.throws(() => run('lifecycle', removed), /HOOK_TRANSITIONS.finalizeTermination is not declared/);
  const reg = `${BPMN}/BpmnElementProcessors.java`;
  const helper = { ...PROCESSORS, [reg]: PROCESSORS[reg].replace('} }', 'processors.put(BpmnElementType.X, processorFor(y)); } }') };
  assert.throws(() => run('lifecycle', helper), /registration\(s\) in an unrecognised form/);
  for (const form of ['processors.putAll(more);', 'processors.putIfAbsent(BpmnElementType.X, new XProcessor(a));', 'register(processors);']) {
    const other = { ...PROCESSORS, [reg]: PROCESSORS[reg].replace('} }', `${form} } }`) };
    assert.throws(() => run('lifecycle', other), /unrecognised use of the processor registry/, form);
  }
  const reads = { ...PROCESSORS, [reg]: PROCESSORS[reg].replace('} }', '} Object g(T t) { return processors.get(t); } private final Map<T, P> processors = new EnumMap<>(T.class); }') };
  assert.equal(run('lifecycle', reads).length, run('lifecycle', PROCESSORS).length);
  const noTerminate = { ...PROCESSORS, [STREAM]: STREAM_SRC.replace('case TERMINATE_ELEMENT:', 'case OTHER:') };
  assert.throws(() => run('lifecycle', noTerminate), /no longer dispatches terminate/);
  const arrow = { ...PROCESSORS, [STREAM]: STREAM_SRC.replace('case CONTINUE_TERMINATING_ELEMENT:\n        break;', 'case CONTINUE_TERMINATING_ELEMENT, MIGRATE_ELEMENT -> c();') };
  const ids = run('lifecycle', arrow).map((c) => c.id);
  assert.ok(ids.includes('lifecycle:TASK:continue-terminating') && ids.includes('lifecycle:TASK:migrate'));
  assert.ok(!ids.includes('lifecycle:TASK:element-activating'), 'nested switch arms are not commands');
  const odd = { ...PROCESSORS, [STREAM]: STREAM_SRC.replace('case COMPLETE_ELEMENT:', 'case Intent.COMPLETE_ELEMENT:') };
  assert.throws(() => run('lifecycle', odd), /unrecognised switch arm: case Intent\.COMPLETE_ELEMENT/);
  const { [STREAM]: _, ...noStream } = PROCESSORS;
  assert.throws(() => run('lifecycle', noStream), /missing Zeebe source/);
});

test('event lists fail on an entry they cannot read', () => {
  const list = (rel, constant, body) => ({ [`${VALIDATION}/${rel}`]: `class V { static final List<X> ${constant} = Arrays.asList(${body}); }` });
  const files = (catchBody) => ({
    ...list('BoundaryEventValidator.java', 'SUPPORTED_EVENT_DEFINITIONS', 'TimerEventDefinition.class'),
    ...list('IntermediateCatchEventValidator.java', 'SUPPORTED_EVENTS', catchBody),
    ...list('SubProcessValidator.java', 'SUPPORTED_START_TYPES', 'MessageEventDefinition.class'),
    [`${BPMN}/event/EndEventProcessor.java`]: 'class P { class NoneEndEventBehavior implements EndEventBehavior {} }',
    [`${BPMN}/event/IntermediateThrowEventProcessor.java`]:
      'class P { class NoneIntermediateThrowEventBehavior implements IntermediateThrowEventBehavior {} }',
  });
  assert.equal(run('event', files('TimerEventDefinition.class, LinkEventDefinition.class')).length, 6);
  assert.throws(() => run('event', files('TimerEventDefinition.class, EXTRA_DEFINITIONS')), /unrecognised SUPPORTED_EVENTS entry/);
  const read = files('TimerEventDefinition.class');
  const rel = `${VALIDATION}/IntermediateCatchEventValidator.java`;
  read[rel] = read[rel].replace(' }', ' boolean ok(X t) { return SUPPORTED_EVENTS.stream().anyMatch(t::is); } }');
  assert.equal(run('event', read).length, 5);
  read[rel] = read[rel].replace(' }', ' static { SUPPORTED_EVENTS.addAll(MORE); } }');
  assert.throws(() => run('event', read), /unrecognised use of SUPPORTED_EVENTS/);
});

test('guard fails on a rejection outside a recognised method', () => {
  const rel = `${BPMN}/ProcessInstanceStateTransitionGuard.java`;
  const method = '\n  private Either<String, ?> check(final C c) {\n    return Either.left("Expected x");\n  }\n';
  assert.deepEqual(run('guard', { [rel]: `class G {${method}}` }).map((c) => c.id), ['guard:check:expected-x']);
  const hidden = `class G {${method}\n  @Override Either<String, ?> other(final C c) { return Either.left("Expected y"); }\n}`;
  assert.throws(() => run('guard', { [rel]: hidden }), /1 Either.left rejection\(s\) outside a recognised method/);
});

test('two different messages normalising to one cell id stop extraction', () => {
  const rel = `${VALIDATION}/XValidator.java`;
  const engine = { [`${PROCESSING}/deployment/model/validation/Y.java`]: 'class Y { void f() { expressionVerification.accept(v); } }' };
  assert.throws(
    () => run('validation', { ...engine, [rel]: 'class X { void v() { c.addError(0, "expected %s"); c.addError(0, "expected %d"); } }' }),
    /derives from two different messages/,
  );
  assert.equal(run('validation', { ...engine, [rel]: 'class X { void v() { c.addError(0, "same"); c.addError(0, "same"); } }' }).length, 1);
  assert.throws(() => run('validation', { [rel]: 'class X {}' }), /missing Zeebe source directory/);
});

test('validation fails on an unrecognised collector or a stale non-collector entry', () => {
  const rel = `${VALIDATION}/XValidator.java`;
  const y = `${PROCESSING}/deployment/model/validation/Y.java`;
  const engine = { [y]: 'class Y { void f() { expressionVerification.accept(v); } }' };
  const mixed = 'class X { void v() { c.addError(0, "kept"); errorCollector.accept("also kept"); } }';
  assert.equal(run('validation', { ...engine, [rel]: mixed }).length, 2);
  const renamed = mixed.replace('errorCollector.accept', 'problems.accept');
  assert.throws(() => run('validation', { ...engine, [rel]: renamed }), /unrecognised validation collector problems\.accept\(/);
  assert.throws(() => run('validation', { [y]: 'class Y {}', [rel]: mixed }), /NON_COLLECTOR_ACCEPTS\.expressionVerification no longer occurs/);
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
    [`${PROCESSING}/Aggregator.java`]: 'class Aggregator { boolean f(R r) { return r.type() == RejectionType.NOT_FOUND; } }',
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
