#!/usr/bin/env node
// Extracts the Camunda 8 (Zeebe) behaviour surface the parity suite must
// cover, from the Zeebe sources pinned in zeebe-pin.json (#1245).
//
//   node formal/parity/extract.mjs <zeebe-checkout> [--out formal/parity/zeebe-surface.json]
//
// The output is a derived artifact: never hand-edit it. Regenerate it after
// bumping the pin (fetch-zeebe.sh fetches the sources). Each extractor anchors
// on a specific construct in the Zeebe source and fails loudly when that
// construct disappears, so a Zeebe refactor breaks the build instead of
// silently shrinking the matrix.

import { execFileSync } from 'node:child_process';
import { readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { join, relative, basename } from 'node:path';
import { fileURLToPath } from 'node:url';

const PROTOCOL = 'zeebe/protocol/src/main/java/io/camunda/zeebe/protocol/record';
const VALIDATION = 'zeebe/bpmn-model/src/main/java/io/camunda/zeebe/model/bpmn/validation/zeebe';
const PROCESSING = 'zeebe/engine/src/main/java/io/camunda/zeebe/engine/processing';
const BPMN = `${PROCESSING}/bpmn`;
const DEPLOY_VALIDATION = `${PROCESSING}/deployment/model/validation`;

export class ExtractError extends Error {}

function fail(msg) {
  throw new ExtractError(msg);
}

/** Java source with comments blanked out (line numbers preserved). */
export function stripComments(src) {
  let out = '';
  let i = 0;
  while (i < src.length) {
    const c = src[i];
    const n = src[i + 1];
    if (c === '"' || c === "'") {
      if (c === '"' && src.startsWith('"""', i)) {
        const end = src.indexOf('"""', i + 3);
        const stop = end < 0 ? src.length : end + 3;
        out += src.slice(i, stop);
        i = stop;
        continue;
      }
      let j = i + 1;
      while (j < src.length && src[j] !== c && src[j] !== '\n') j += src[j] === '\\' ? 2 : 1;
      out += src.slice(i, j + 1);
      i = j + 1;
    } else if (c === '/' && n === '/') {
      while (i < src.length && src[i] !== '\n') {
        out += ' ';
        i++;
      }
    } else if (c === '/' && n === '*') {
      const end = src.indexOf('*/', i + 2);
      const stop = end < 0 ? src.length : end + 2;
      out += src.slice(i, stop).replace(/[^\n]/g, ' ');
      i = stop;
    } else {
      out += c;
      i++;
    }
  }
  return out;
}

export function lineAt(src, index) {
  let line = 1;
  for (let i = 0; i < index; i++) if (src.charCodeAt(i) === 10) line++;
  return line;
}

/** Index just past the brace block that opens at or after `from`. */
function blockEnd(src, from) {
  const open = src.indexOf('{', from);
  if (open < 0) return -1;
  let depth = 0;
  for (let i = open; i < src.length; i++) {
    const c = src[i];
    if (c === '"') {
      i++;
      while (i < src.length && src[i] !== '"') i += src[i] === '\\' ? 2 : 1;
    } else if (c === '{') depth++;
    else if (c === '}' && --depth === 0) return i + 1;
  }
  return -1;
}

/**
 * The first string message starting at or after `from`: adjacent literals
 * joined by `+` are concatenated. Returns null if no literal starts within
 * `limit` characters.
 */
export function messageAt(src, from, limit = 400) {
  const start = src.indexOf('"', from);
  if (start < 0 || start - from > limit) return null;
  let i = start;
  let msg = '';
  for (;;) {
    let j = i + 1;
    while (j < src.length && src[j] !== '"') j += src[j] === '\\' ? 2 : 1;
    msg += src.slice(i + 1, j);
    const rest = /^\s*\+\s*"/.exec(src.slice(j + 1, j + 400));
    if (!rest) return msg;
    i = j + rest[0].length;
  }
}

/**
 * A stable identifier fragment for a message. Never truncated: distinct
 * messages must stay distinct cells (a shared prefix is common).
 */
export function slug(message) {
  const out = message
    .replace(/\\n/g, ' ')
    .replace(/%[sd]/g, ' ')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, '-');
  return out || 'message';
}

/**
 * The message an argument expression starting at `from` denotes: a string
 * literal (or `+`-joined literals), `String.format(<message>, ...)`, or a
 * message constant (`NAME` / `Owner.NAME`, optionally `.formatted(...)`),
 * which yields the constant's name. Null for anything else.
 */
export function messageExpr(src, from) {
  let i = from;
  while (/\s/.test(src[i] ?? '')) i++;
  const format = /^String\.format\(/.exec(src.slice(i, i + 20));
  if (format) return messageExpr(src, i + format[0].length);
  if (src[i] === '"') return messageAt(src, i, 0);
  const constant = /^(?:[A-Za-z_]\w*\.)*([A-Z][A-Z0-9_]+)\b/.exec(src.slice(i, i + 200));
  return constant ? constant[1].toLowerCase() : null;
}

/** The callee owning the innermost unclosed `(` before `index`. */
function enclosingCall(src, index) {
  let depth = 0;
  for (let i = index - 1; i >= 0; i--) {
    const c = src[i];
    if (c === ')') depth++;
    else if (c === '(') {
      if (depth === 0) {
        const m = /([A-Za-z_][\w.]*)\s*$/.exec(src.slice(Math.max(0, i - 120), i));
        return m ? m[1] : null;
      }
      depth--;
    } else if (c === ';' || (c === '{' && depth === 0)) return null;
  }
  return null;
}

/**
 * The messages a validation call site reports. The argument is a message
 * (see `messageExpr`), or one it forwards: a lambda parameter (the message
 * comes from the callee the lambda is passed to, e.g. a `ModelUtil.verify*`
 * rule, reported as `via-<callee>`), a local variable (resolved from its
 * assignment, both arms of a ternary), or a runtime failure (`dynamic-*`).
 * Throws if the argument cannot be classified, so a new shape is noticed.
 */
/** Split the argument list of the call whose `(` is at `open` (depth-0 commas). */
function callArgs(src, open) {
  const args = [];
  let depth = 0;
  let start = open + 1;
  let quote = false;
  for (let i = open + 1; i < src.length; i++) {
    const c = src[i];
    if (quote) {
      if (c === '\\') i++;
      else if (c === '"') quote = false;
    } else if (c === '"') quote = true;
    else if ('([{'.includes(c)) depth++;
    else if (')]}'.includes(c)) {
      if (depth === 0) {
        args.push(src.slice(start, i));
        return args;
      }
      depth--;
    } else if (c === ',' && depth === 0) {
      args.push(src.slice(start, i));
      start = i + 1;
    }
  }
  return args;
}

/**
 * Messages bound to `ident` when it is a parameter of the method enclosing
 * `index`: the message arguments every call site passes in that position.
 */
function parameterMessages(src, index, ident) {
  const decls = [...src.slice(0, index).matchAll(/\b(\w+)\s*\(([^()]*)\)\s*(?:throws[^{]*)?\{/g)];
  const decl = decls.pop();
  if (!decl) return null;
  const params = decl[2].split(',').map((p) => p.trim().split(/\s+/).pop());
  const pos = params.indexOf(ident);
  if (pos < 0) return null;
  const msgs = [];
  for (const call of src.matchAll(new RegExp(`\\b${decl[1]}\\s*\\(`, 'g'))) {
    if (call.index === decl.index) continue;
    const arg = callArgs(src, call.index + call[0].length - 1)[pos];
    const msg = arg === undefined ? null : messageExpr(arg, 0);
    if (msg !== null) msgs.push(msg);
  }
  return msgs.length > 0 ? msgs : null;
}

export function validationMessages(src, argIndex, where) {
  const format = /^\s*String\.format\(/.exec(src.slice(argIndex, argIndex + 30));
  if (format) argIndex += format[0].length;
  const direct = messageExpr(src, argIndex);
  if (direct !== null) return [direct];
  const arg = /^\s*([A-Za-z_]\w*)(\s*\.\s*(\w+)\s*\(\s*\))?\s*[,)]/.exec(src.slice(argIndex, argIndex + 200));
  if (!arg) fail(`${where}: unrecognised validation message argument`);
  const [, ident, , getter] = arg;
  if (getter) return [`dynamic-${getter}`];
  const before = src.slice(Math.max(0, argIndex - 600), argIndex);
  const lambda = new RegExp(`\\b${ident}\\s*->[^;]*$`).exec(before);
  if (lambda) {
    const callee = enclosingCall(src, argIndex - before.length + lambda.index);
    if (!callee) fail(`${where}: cannot find the call receiving lambda ${ident}`);
    const name = callee.split('.').pop();
    return [name === 'forEach' || name === 'accept' ? `dynamic-${ident}` : `via-${name}`];
  }
  const bound = parameterMessages(src, argIndex, ident);
  if (bound) return bound;
  const scope = src.slice(0, argIndex);
  const assign = [...scope.matchAll(new RegExp(`\\b${ident}\\s*=(?!=)`, 'g'))].pop();
  if (assign) {
    const end = src.indexOf(';', assign.index);
    const rhs = src.slice(assign.index + assign[0].length, end);
    const parts = rhs.split(/\?|:(?!:)/).slice(rhs.includes('?') ? 1 : 0);
    const msgs = parts.map((p) => messageExpr(p, 0)).filter((m) => m !== null);
    if (msgs.length > 0) return msgs;
    return [`dynamic-${ident}`];
  }
  fail(`${where}: cannot resolve validation message ${ident}`);
}

/** The constants of the first enum declared in `src`. */
export function enumConstants(src, name) {
  const decl = new RegExp(`\\benum\\s+${name}\\b[^{]*\\{`).exec(src);
  if (!decl) fail(`enum ${name} not found`);
  const body = src.slice(decl.index + decl[0].length);
  const out = [];
  let depth = 0;
  let expecting = true;
  for (let i = 0; i < body.length; i++) {
    const c = body[i];
    if (c === '(' || c === '{') depth++;
    else if (c === ')' || c === '}') {
      if (depth === 0) break;
      depth--;
    } else if (depth === 0 && c === ';') break;
    else if (depth === 0 && c === ',') expecting = true;
    else if (depth === 0 && expecting && /[A-Za-z_@]/.test(c)) {
      const m = /^(?:@\w+(?:\([^)]*\))?\s*)*([A-Za-z_][A-Za-z0-9_]*)/.exec(body.slice(i));
      out.push(m[1]);
      i += m[0].length - 1;
      expecting = false;
    }
  }
  if (out.length === 0) fail(`enum ${name} has no constants`);
  return out;
}

function listJava(dir) {
  const out = [];
  for (const entry of readdirSync(dir).sort()) {
    const p = join(dir, entry);
    if (statSync(p).isDirectory()) out.push(...listJava(p));
    else if (entry.endsWith('.java')) out.push(p);
  }
  return out;
}

export class Source {
  constructor(root) {
    this.root = root;
    this.cache = new Map();
  }
  read(rel) {
    if (!this.cache.has(rel)) {
      let text;
      try {
        text = readFileSync(join(this.root, rel), 'utf8');
      } catch {
        fail(`missing Zeebe source ${rel}`);
      }
      this.cache.set(rel, stripComments(text));
    }
    return this.cache.get(rel);
  }
  list(dir) {
    let files;
    try {
      files = listJava(join(this.root, dir));
    } catch {
      fail(`missing Zeebe source directory ${dir}`);
    }
    return files.map((p) => relative(this.root, p));
  }
}

export class Cells {
  constructor() {
    this.map = new Map();
    this.messages = new Map();
  }
  /**
   * Add a cell. `message` is the raw text a slugged id was derived from: two
   * different messages normalising to one id would merge distinct behaviours,
   * so that stops extraction (disambiguate the slug) instead.
   */
  add(id, source, detail, message) {
    if (message !== undefined) {
      const seen = this.messages.get(id);
      if (seen !== undefined && seen.message !== message) {
        fail(`cell ${id} derives from two different messages: ${JSON.stringify(seen.message)} (${seen.source}) and ${JSON.stringify(message)} (${source})`);
      }
      this.messages.set(id, { message, source });
    }
    const existing = this.map.get(id);
    if (existing) {
      if (!existing.sources.includes(source)) existing.sources.push(source);
      return;
    }
    const cell = { id, sources: [source] };
    if (detail !== undefined) cell.detail = detail;
    this.map.set(id, cell);
  }
  sorted() {
    return [...this.map.values()]
      .map((c) => ({ ...c, sources: [...c.sources].sort() }))
      .sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  }
}

const at = (rel, src, index) => `${rel}:${lineAt(src, index)}`;

/** `element:<BpmnClass>` — FlowElementValidator.SUPPORTED_ELEMENT_TYPES. */
function extractElements(s, cells) {
  const rel = `${VALIDATION}/FlowElementValidator.java`;
  const src = s.read(rel);
  // Every use must be a form read here: an empty declaration, `.add(X.class)`,
  // or a `.contains(` lookup. Any other shape (an initializer, `addAll`, a
  // helper) could add elements this would miss, so it stops extraction.
  const forms = [
    /^SUPPORTED_ELEMENT_TYPES\s*=\s*new\s+HashSet<>\(\s*\)\s*;/,
    /^SUPPORTED_ELEMENT_TYPES\.add\(\s*(\w+)\.class\s*\)\s*;/,
    /^SUPPORTED_ELEMENT_TYPES\.contains\(/,
  ];
  let n = 0;
  for (const use of src.matchAll(/\bSUPPORTED_ELEMENT_TYPES\b/g)) {
    const rest = src.slice(use.index, use.index + 200);
    const form = forms.findIndex((f) => f.test(rest));
    if (form < 0) fail(`${at(rel, src, use.index)}: unrecognised use of SUPPORTED_ELEMENT_TYPES`);
    if (form === 1) {
      cells.add(`element:${forms[1].exec(rest)[1]}`, at(rel, src, use.index));
      n++;
    }
  }
  if (n === 0) fail(`${rel}: no SUPPORTED_ELEMENT_TYPES entries`);
}

/** Event-definition classes listed in a validator's `SUPPORTED_*` constant. */
function supportedDefinitions(s, rel, constant) {
  const src = s.read(rel);
  const decl = new RegExp(`\\b${constant}\\s*=\\s*(?:Arrays\\.asList|List\\.of|Set\\.of)\\(`).exec(src);
  if (!decl) fail(`${rel}: ${constant} not found`);
  const close = src.indexOf(')', decl.index + decl[0].length);
  const body = src.slice(decl.index + decl[0].length, src.indexOf(';', close));
  const entries = body.replace(/\)\s*\)?\s*$/, '').split(',').map((e) => e.trim()).filter(Boolean);
  const defs = entries.map((e) => {
    const m = /^(\w+)EventDefinition\.class$/.exec(e);
    if (!m) fail(`${rel}: unrecognised ${constant} entry ${JSON.stringify(e)}`);
    return m[1] === 'Compensate' ? 'compensation' : m[1].toLowerCase();
  });
  if (defs.length === 0) fail(`${rel}: ${constant} is empty`);
  // Anything but a read (`.stream()` / `.contains(`) could add definitions.
  for (const use of src.matchAll(new RegExp(`\\b${constant}\\b`, 'g'))) {
    if (use.index === decl.index) continue;
    if (!/^\w+\s*\.\s*(?:stream\(\s*\)|contains\()/.test(src.slice(use.index, use.index + 100))) {
      fail(`${at(rel, src, use.index)}: unrecognised use of ${constant}`);
    }
  }
  return { defs, source: at(rel, src, decl.index) };
}

/** Inner `*Behavior` variants of an event processor, one per event definition. */
function behaviorVariants(s, rel, iface, strip) {
  const src = s.read(rel);
  const re = new RegExp(`class\\s+(\\w+)\\s+implements\\s+${iface}\\b`, 'g');
  const out = [];
  for (let m; (m = re.exec(src)); ) {
    const variant = m[1].replace(strip, '').replace(/Behaviou?r$/, '').toLowerCase();
    if (!variant) fail(`${rel}: cannot derive a variant from ${m[1]}`);
    out.push({ variant, source: at(rel, src, m.index) });
  }
  if (out.length === 0) fail(`${rel}: no ${iface} implementations`);
  return out;
}

/** `event:<position>:<definition>` — supported event definitions per event position. */
function extractEvents(s, cells) {
  const lists = [
    ['boundary', `${VALIDATION}/BoundaryEventValidator.java`, 'SUPPORTED_EVENT_DEFINITIONS'],
    ['intermediate-catch', `${VALIDATION}/IntermediateCatchEventValidator.java`, 'SUPPORTED_EVENTS'],
    ['event-subprocess-start', `${VALIDATION}/SubProcessValidator.java`, 'SUPPORTED_START_TYPES'],
  ];
  for (const [position, rel, constant] of lists) {
    const { defs, source } = supportedDefinitions(s, rel, constant);
    for (const d of defs) cells.add(`event:${position}:${d}`, source);
  }
  const behaviors = [
    ['end', `${BPMN}/event/EndEventProcessor.java`, 'EndEventBehavior', /EndEvent/],
    [
      'intermediate-throw',
      `${BPMN}/event/IntermediateThrowEventProcessor.java`,
      'IntermediateThrowEventBehavior',
      /IntermediateThrowEvent/,
    ],
  ];
  for (const [position, rel, iface, strip] of behaviors) {
    for (const { variant, source } of behaviorVariants(s, rel, iface, strip)) {
      cells.add(`event:${position}:${variant}`, source);
    }
  }
}

/**
 * The lifecycle transition each processor-API hook belongs to. The hook set
 * itself is read from the processor interfaces (`processorApi`), and every
 * interface method must appear here (or in `NON_HOOKS`), so a hook Zeebe adds
 * or renames stops extraction instead of being dropped.
 */
export const HOOK_TRANSITIONS = {
  onActivate: 'activate',
  finalizeActivation: 'activate',
  onComplete: 'complete',
  finalizeCompletion: 'complete',
  onTerminate: 'terminate',
  finalizeTermination: 'terminate',
  onChildActivating: 'child-activating',
  onChildCompleting: 'child-completing',
  beforeExecutionPathCompleted: 'child-completed',
  afterExecutionPathCompleted: 'child-completed',
  onChildTerminated: 'child-terminated',
};
const NON_HOOKS = new Set(['getType']);
const PROCESSOR_INTERFACES = ['BpmnElementProcessor', 'BpmnElementContainerProcessor'];

/** Method names an interface declares. */
export function interfaceMethods(src, name) {
  const decl = new RegExp(`\\binterface\\s+${name}\\b[^{]*\\{`).exec(src);
  if (!decl) fail(`interface ${name} not found`);
  const body = src.slice(decl.index + decl[0].length, blockEnd(src, decl.index));
  const out = [];
  let depth = 0;
  for (let i = 0; i < body.length; i++) {
    const c = body[i];
    if (c === '{') depth++;
    else if (c === '}') depth--;
    else if (depth === 0 && (i === 0 || /[;}\s]/.test(body[i - 1]))) {
      const m = /^(?:default\s+|public\s+|static\s+)*(?:<[^>]*>\s*)?[\w<>?, .[\]]+?\s+(\w+)\s*\(/.exec(body.slice(i, i + 300));
      if (m) {
        out.push(m[1]);
        i += m[0].length - 1;
      }
    }
  }
  return out;
}

/** Hook name -> transition, from the processor interfaces, checked against `HOOK_TRANSITIONS`. */
export function processorApi(s) {
  const hooks = new Map();
  for (const name of PROCESSOR_INTERFACES) {
    const rel = `${BPMN}/${name}.java`;
    for (const m of interfaceMethods(s.read(rel), name)) {
      if (NON_HOOKS.has(m)) continue;
      const t = HOOK_TRANSITIONS[m];
      if (!t) fail(`${rel}: processor hook ${m} has no lifecycle transition in HOOK_TRANSITIONS`);
      hooks.set(m, t);
    }
  }
  for (const m of Object.keys(HOOK_TRANSITIONS)) {
    if (!hooks.has(m)) fail(`HOOK_TRANSITIONS.${m} is not declared by ${PROCESSOR_INTERFACES.join(' / ')}`);
  }
  return hooks;
}

function hookTransition(api, method) {
  return api.get(method.replace(/Internal$/, '').replace(/^onFinalize/, 'finalize'));
}

/** Lifecycle hooks a processor class overrides, following `extends` within the bpmn package. */
function processorHooks(s, api, index, cls, seen = new Set()) {
  if (seen.has(cls)) return new Map();
  seen.add(cls);
  const rel = index.get(cls);
  if (!rel) fail(`processor class ${cls} not found under ${BPMN}`);
  const src = s.read(rel);
  const decl = new RegExp(`class\\s+${cls}\\b`).exec(src);
  const end = blockEnd(src, decl.index);
  const body = src.slice(decl.index, end);
  const hooks = new Map();
  const parent = /^class\s+\w+(?:<[^{]*?>)?\s+extends\s+(\w+)/.exec(body);
  if (parent && !index.has(parent[1])) {
    fail(`${rel}: ${cls} extends ${parent[1]}, which is not under ${BPMN}, so its inherited hooks cannot be read`);
  }
  if (parent) {
    for (const [t, where] of processorHooks(s, api, index, parent[1], seen)) hooks.set(t, where);
  }
  // Only the class's own methods (depth 1), not those of inner behaviour classes.
  let depth = 0;
  for (let i = 0; i < body.length; i++) {
    const c = body[i];
    if (c === '"') {
      i++;
      while (i < body.length && body[i] !== '"') i += body[i] === '\\' ? 2 : 1;
    } else if (c === '{') depth++;
    else if (c === '}') depth--;
    else if (depth === 1 && /\s/.test(body[i - 1] ?? ' ')) {
      const m = /^(?:public|protected)\s+[\w<>?, ]+?\s+(\w+)\s*\(/.exec(body.slice(i, i + 200));
      if (m) {
        const t = hookTransition(api, m[1]);
        if (t) {
          const where = `${rel}:${lineAt(src, decl.index + i)}`;
          if (!hooks.has(t)) hooks.set(t, []);
          hooks.get(t).push(`${m[1]}@${where}`);
        }
        i += m[0].length - 1;
      }
    }
  }
  return hooks;
}

/**
 * The lifecycle commands every element processor is driven through: the
 * `ProcessInstanceIntent` cases `BpmnStreamProcessor.processEvent` dispatches
 * (`ACTIVATE_ELEMENT` -> `activate`, `CONTINUE_TERMINATING_ELEMENT` ->
 * `continue-terminating`, …).
 */
export function elementCommands(s) {
  const rel = `${BPMN}/BpmnStreamProcessor.java`;
  const src = s.read(rel);
  const decl = /\bvoid\s+processEvent\s*\(/.exec(src);
  if (!decl) fail(`${rel}: processEvent not found`);
  const body = src.slice(decl.index, blockEnd(src, decl.index));
  const sw = /\bswitch\s*\(\s*intent\s*\)\s*\{/.exec(body);
  if (!sw) fail(`${rel}: processEvent has no switch (intent)`);
  const arms = body.slice(sw.index + sw[0].length, blockEnd(body, sw.index));
  // Only the outer switch's arms: nested switches use `case X ->`.
  const out = [...arms.matchAll(/\bcase\s+([A-Z_]+)\s*:/g)].map((m) =>
    m[1].replace(/_ELEMENT$/, '').toLowerCase().replace(/_/g, '-'),
  );
  for (const t of ['activate', 'complete', 'terminate']) {
    if (!out.includes(t)) fail(`${rel}: processEvent no longer dispatches ${t}`);
  }
  return out;
}

/** `lifecycle:<BpmnElementType>:<command>` — every element type the engine processes. */
function extractLifecycle(s, cells) {
  const rel = `${BPMN}/BpmnElementProcessors.java`;
  const src = s.read(rel);
  const index = new Map(
    s.list(BPMN).map((p) => [basename(p, '.java'), p]),
  );
  const api = processorApi(s);
  const commands = elementCommands(s);
  const re = /processors\.put\(\s*BpmnElementType\.(\w+)\s*,\s*new\s+(\w+)\s*[(<]/g;
  const puts = [...src.matchAll(/\bprocessors\.put\(/g)].length;
  let n = 0;
  for (let m; (m = re.exec(src)); n++) {
    const [, type, cls] = m;
    const hooks = processorHooks(s, api, index, cls);
    const source = at(rel, src, m.index);
    for (const t of commands) {
      cells.add(`lifecycle:${type}:${t}`, source, {
        processor: cls,
        hooks: (hooks.get(t) ?? []).map((h) => h.split('@')[0]).sort(),
      });
    }
    for (const [t, where] of hooks) {
      if (!commands.includes(t)) {
        cells.add(`lifecycle:${type}:${t}`, source, {
          processor: cls,
          hooks: where.map((h) => h.split('@')[0]).sort(),
        });
      }
    }
  }
  if (n === 0) fail(`${rel}: no processors.put(...) registrations`);
  if (n !== puts) fail(`${rel}: ${puts - n} processors.put(...) registration(s) in an unrecognised form`);
}

/** `guard:<method>:<rejection>` — each rejection branch of the state-transition guard. */
function extractGuard(s, cells) {
  const rel = `${BPMN}/ProcessInstanceStateTransitionGuard.java`;
  const src = s.read(rel);
  const methods = [...src.matchAll(/\n\s*(?:private|public)\s+[\w<>?, ]+\s+(\w+)\s*\([^)]*\)\s*\{/g)];
  let n = 0;
  for (let k = 0; k < methods.length; k++) {
    const start = methods[k].index;
    const end = blockEnd(src, start);
    const body = src.slice(start, end);
    for (const m of body.matchAll(/Either\.left\(/g)) {
      const text = messageExpr(body, m.index + m[0].length);
      if (text === null) fail(`${at(rel, src, start + m.index)}: unreadable Either.left message`);
      cells.add(`guard:${methods[k][1]}:${slug(text)}`, at(rel, src, start + m.index), undefined, text);
      n++;
    }
  }
  if (n === 0) fail(`${rel}: no Either.left rejections`);
  const total = [...src.matchAll(/Either\.left\(/g)].length;
  if (total !== n) fail(`${rel}: ${total - n} Either.left rejection(s) outside a recognised method`);
}

/** `incident:<ErrorType>`. */
function extractIncidents(s, cells) {
  const rel = `${PROTOCOL}/value/ErrorType.java`;
  const src = s.read(rel);
  for (const c of enumConstants(src, 'ErrorType')) cells.add(`incident:${c}`, rel);
}

/** `intent:<Enum>:<VALUE>` — every record intent in the protocol. */
function extractIntents(s, cells) {
  const files = s.list(`${PROTOCOL}/intent`).filter((p) => /Intent\.java$/.test(p));
  let n = 0;
  for (const rel of files) {
    const src = s.read(rel);
    const name = basename(rel, '.java');
    if (!new RegExp(`\\benum\\s+${name}\\b`).test(src)) continue;
    for (const c of enumConstants(src, name)) {
      cells.add(`intent:${name.replace(/Intent$/, '')}:${c}`, rel);
      n++;
    }
  }
  if (n === 0) fail(`${PROTOCOL}/intent: no intent enums`);
}

/** `validation:<Validator>:<message>` — every deploy-time rejection message. */
function extractValidation(s, cells) {
  let n = 0;
  for (const dir of [VALIDATION, DEPLOY_VALIDATION]) {
    for (const rel of s.list(dir)) {
      const src = s.read(rel);
      const validator = basename(rel, '.java');
      for (const m of src.matchAll(/\b(?:addError|errorCollector\.accept|\.accept)\(/g)) {
        if (m[0] === '.accept(' && !/[eE]rror\w*\.accept\($/.test(src.slice(m.index - 30, m.index + 8))) continue;
        const args = /^\s*(?:\d+\s*,)?/.exec(src.slice(m.index + m[0].length))[0];
        const where = at(rel, src, m.index);
        for (const msg of validationMessages(src, m.index + m[0].length + args.length, where)) {
          cells.add(`validation:${validator}:${slug(msg)}`, where, undefined, msg);
          n++;
        }
      }
    }
  }
  if (n === 0) fail('no deploy-time validation messages');
}

/**
 * `rejection:<Class>:<RejectionType>` — every command rejection a class in the
 * processing layer produces: processors, and the validators/helpers that build
 * the rejection a processor then writes. A comparison against a type is a read
 * and yields no cell.
 */
function extractRejections(s, cells) {
  // RejectionType is generated from the SBE schema, so read it from there.
  const schemaRel = 'zeebe/protocol/src/main/resources/protocol.xml';
  const schema = readFileSync(join(s.root, schemaRel), 'utf8');
  const decl = /<enum name="RejectionType"[^>]*>([\s\S]*?)<\/enum>/.exec(schema);
  if (!decl) fail(`${schemaRel}: enum RejectionType not found`);
  const types = new Set([...decl[1].matchAll(/<validValue name="(\w+)"/g)].map((m) => m[1]));
  if (types.size === 0) fail(`${schemaRel}: RejectionType has no values`);
  let n = 0;
  for (const rel of s.list(PROCESSING)) {
    const src = s.read(rel);
    const cls = basename(rel, '.java');
    for (const m of src.matchAll(/\bRejectionType\.([A-Z_]+)\b/g)) {
      // SBE's generated sentinels mean "no rejection".
      if (m[1] === 'NULL_VAL' || m[1] === 'SBE_UNKNOWN') continue;
      const before = src.slice(Math.max(0, m.index - 4), m.index);
      const after = src.slice(m.index + m[0].length, m.index + m[0].length + 4);
      if (/[=!]=\s*$/.test(before) || /^\s*[=!]=/.test(after)) continue;
      if (!types.has(m[1])) fail(`${rel}: unknown RejectionType.${m[1]}`);
      cells.add(`rejection:${cls}:${m[1]}`, at(rel, src, m.index));
      n++;
    }
  }
  if (n === 0) fail(`${PROCESSING}: no RejectionType uses`);
}

export const FAMILIES = {
  element: extractElements,
  event: extractEvents,
  lifecycle: extractLifecycle,
  guard: extractGuard,
  incident: extractIncidents,
  intent: extractIntents,
  validation: extractValidation,
  rejection: extractRejections,
};

export function extract(root, pin) {
  const s = new Source(root);
  const cells = new Cells();
  for (const fn of Object.values(FAMILIES)) fn(s, cells);
  return {
    generator: 'formal/parity/extract.mjs',
    note: 'Derived from the pinned Zeebe sources. Do not hand-edit; regenerate (see formal/README.md).',
    zeebe: { repository: pin.repository, ref: pin.ref, sha: pin.sha },
    cells: cells.sorted(),
  };
}

function main(argv) {
  const here = fileURLToPath(new URL('.', import.meta.url));
  const args = argv.slice(2);
  const root = args.find((a) => !a.startsWith('--'));
  const outIdx = args.indexOf('--out');
  const out = outIdx >= 0 ? args[outIdx + 1] : join(here, 'zeebe-surface.json');
  if (!root) {
    console.error('usage: extract.mjs <zeebe-checkout> [--out <file>]');
    process.exit(2);
  }
  const pin = JSON.parse(readFileSync(join(here, 'zeebe-pin.json'), 'utf8'));
  let head;
  try {
    head = execFileSync('git', ['-C', root, 'rev-parse', 'HEAD'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
  } catch {
    fail(`${root} is not a git checkout (use fetch-zeebe.sh)`);
  }
  if (head !== pin.sha) fail(`${root} is at ${head}, but zeebe-pin.json pins ${pin.sha} (use fetch-zeebe.sh)`);
  const surface = extract(root, pin);
  writeFileSync(out, JSON.stringify(surface, null, 2) + '\n');
  const counts = {};
  for (const c of surface.cells) counts[c.id.split(':')[0]] = (counts[c.id.split(':')[0]] ?? 0) + 1;
  console.log(`${surface.cells.length} cells`, counts);
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  try {
    main(process.argv);
  } catch (e) {
    if (e instanceof ExtractError) {
      console.error(`extract: ${e.message}`);
      process.exit(1);
    }
    throw e;
  }
}
