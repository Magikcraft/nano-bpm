//! Thin, typed facade over the wasm `TestEngine`'s debug surface (#650), plus the
//! Node-side wasm loader. Everything the DAP session needs to drive stepping /
//! breakpoints lives behind this interface, so the session file never touches
//! wasm-init details or the JSON-string wire format.

import { existsSync, readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import initSyncDefault, { initSync, TestEngine } from '@nanobpm/engine-wasm';

/** A breakpoint condition understood by the wasm `debug*` methods (#650). */
export type BreakCondition =
  | { kind: 'elementActivated'; id: string }
  | { kind: 'elementCompleted'; id: string }
  | { kind: 'processCompleted' }
  | { kind: 'everyStep' };

/** The state DTO every wasm `debug*` call returns. */
export interface DebugState {
  paused: boolean;
  seq: number;
  activeElements: string[];
}

let wasmReady = false;

const WASM_FILE = 'nanobpmn_engine_bg.wasm';

/**
 * `JSON.parse` that never throws: the wasm surface returns a JSON string on
 * success but may hand back an invalid-JSON error string (or malformed payload)
 * on failure. A raw `JSON.parse` there would crash the adapter process, so parse
 * defensively and let callers treat an unparseable result as "no data".
 */
function safeJsonParse(json: string): unknown {
  try {
    return JSON.parse(json) as unknown;
  } catch {
    return undefined;
  }
}

function firstReadable(candidates: string[]): string | undefined {
  for (const candidate of candidates) {
    try {
      if (existsSync(candidate)) return candidate;
    } catch {
      // Ignore malformed bundle-time paths and try the next resolution strategy.
    }
  }
  return undefined;
}

function bundledWasmCandidates(): string[] {
  const candidates: string[] = [];
  try {
    if (typeof __dirname === 'string') {
      candidates.push(path.join(__dirname, WASM_FILE));
    }
  } catch {
    // `__dirname` is absent in native ESM output.
  }
  try {
    candidates.push(fileURLToPath(new URL(`./${WASM_FILE}`, import.meta.url)));
  } catch {
    // `import.meta.url` may be unavailable after a CommonJS bundle transform.
  }
  return candidates;
}

function packageWasmCandidate(): string | undefined {
  try {
    const require =
      typeof __filename === 'string' ? createRequire(__filename) : createRequire(import.meta.url);
    return require.resolve(`@nanobpm/engine-wasm/${WASM_FILE}`);
  } catch {
    return undefined;
  }
}

function resolveWasmPath(): string {
  const packageCandidate = packageWasmCandidate();
  const candidates =
    packageCandidate === undefined
      ? bundledWasmCandidates()
      : [...bundledWasmCandidates(), packageCandidate];
  const wasmPath = firstReadable(candidates);
  if (wasmPath === undefined) {
    throw new Error(`cannot find ${WASM_FILE}; tried ${candidates.join(', ')}`);
  }
  return wasmPath;
}

/**
 * Initialise the wasm module once, synchronously, from the on-disk `.wasm` bytes.
 * The pkg is built `--target web` (its default init fetches a URL), which does not
 * work under Node's `fetch` for `file://`; `initSync` with the raw bytes does.
 */
function ensureWasm(): void {
  if (wasmReady) return;
  const wasmPath = resolveWasmPath();
  initSync({ module: readFileSync(wasmPath) });
  // Reference the default export so bundlers keep it; harmless at runtime.
  void initSyncDefault;
  wasmReady = true;
}

/** A running engine bound to one debug session. */
export class DebugEngine {
  private readonly engine: TestEngine;

  constructor() {
    ensureWasm();
    this.engine = new TestEngine();
  }

  /** Parse + deploy a BPMN resource, returning the deployed process ids. */
  deploy(xml: string): string[] {
    const res = safeJsonParse(this.engine.deploy(xml));
    if (
      typeof res === 'object' &&
      res !== null &&
      'processIds' in res &&
      Array.isArray((res as { processIds: unknown }).processIds)
    ) {
      return (res as { processIds: string[] }).processIds;
    }
    return [];
  }

  /** Start a debug run of a `CreateInstance`, pausing at the first breakpoint. */
  start(
    processId: string,
    variables: Record<string, unknown>,
    breakpoints: BreakCondition[],
  ): DebugState {
    return this.parseState(
      this.engine.debugCreateInstance(
        processId,
        JSON.stringify(variables ?? {}),
        JSON.stringify(breakpoints ?? []),
      ),
    );
  }

  /** Run to the next breakpoint or completion. */
  resume(): DebugState {
    return this.parseState(this.engine.debugResume());
  }

  /** Advance exactly one step. */
  step(): DebugState {
    return this.parseState(this.engine.debugStep());
  }

  /** Whether the run is paused at a breakpoint. */
  get paused(): boolean {
    return this.engine.debugIsPaused;
  }

  /** The variables of the (single) instance, from the current snapshot. */
  variables(): Record<string, unknown> {
    const snap = safeJsonParse(this.engine.snapshot());
    if (
      typeof snap === 'object' &&
      snap !== null &&
      'instances' in snap &&
      Array.isArray((snap as { instances: unknown[] }).instances)
    ) {
      const first = (snap as { instances: Array<{ variables?: unknown }> }).instances[0];
      const vars = first?.variables;
      if (typeof vars === 'object' && vars !== null) {
        return vars as Record<string, unknown>;
      }
    }
    return {};
  }

  /** Drop the debug session (engine keeps whatever state the run produced). */
  clear(): void {
    this.engine.debugClear();
  }

  private parseState(json: string): DebugState {
    const v = safeJsonParse(json);
    if (typeof v !== 'object' || v === null) {
      return { paused: false, seq: 0, activeElements: [] };
    }
    const o = v as Record<string, unknown>;
    return {
      paused: o.paused === true,
      seq: typeof o.seq === 'number' ? o.seq : 0,
      activeElements: Array.isArray(o.activeElements)
        ? o.activeElements.filter((e): e is string => typeof e === 'string')
        : [],
    };
  }
}
