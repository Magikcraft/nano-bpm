// EmbeddedEngine — a plain WebAssembly.instantiate wrapper around the Nano BPMN
// engine's FFI cdylib (engine-core --features ffi --target wasm32-unknown-unknown).
//
// Codename: **Bernd**, after Bernd Ruecker (co-founder of Camunda, evangelist of
// the Saga/compensation pattern that makes "same source, embedded or remote"
// tenable). See ADR 0005 (`docs/adr/0005-embedded-u-nano.md`) for the design
// position; see ADR 0015 for the naming policy. The class is EmbeddedEngine so
// its purpose is obvious in unfamiliar code; the codename lives in the package
// name (`@nanobpm/nano-bernd`), the CODENAME constant, boot logs and the
// NANO_BERND_* env prefix.
//
// The wasm ships with zero imports; we host it directly with no wasm-bindgen
// glue. The same `nano_engine.wasm` binary powers the JVM (via Chicory) and
// future Python/Go/.NET embedded hosts — this file is its JS-side twin.
//
// v0.1.0 surface (matches the current FFI exports in engine-core/src/ffi.rs):
//   - deploy(bpmnXml) → { count }
//   - createInstance(processId) → { processInstanceKey }
//   - correlateMessage(name, correlationKey) → i64
//   - triggerTimers(nowEpochMs) → i64      // injected clock; caller decides cadence
//   - isCompleted(key) → boolean
//   - instanceCount() → number
//
// Coming in a follow-up (bumps manifest ABI_VERSION):
//   - activateJobs / completeJob / failJob (job worker loop)
// These live in the wasm-bindgen build already; adding them to the FFI cdylib
// is a separate change tracked alongside PR B.

import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));

/** JSON manifest emitted alongside the wasm by `make engine-wasm-ffi-dist`. */
export interface WasmManifest {
  artifact: string;
  abi_version: number;
  engine_version: string;
  sha256: string;
  size_bytes: number;
  raw_size_bytes: number;
  exports: Array<{ name: string; kind: string }>;
  imports: Array<{ module: string; name: string; kind: string }>;
}

/** ABI version this JS host is written against. Bumped when the C-ABI changes. */
export const EXPECTED_ABI_VERSION = 1;

/** Coarse exports of the FFI cdylib. */
interface NanoExports {
  memory: WebAssembly.Memory;
  nbpmn_alloc(len: number): number;
  nbpmn_free(ptr: number, len: number): void;
  nbpmn_engine_new(): number;
  nbpmn_engine_free(engine: number): void;
  nbpmn_deploy_bpmn(engine: number, ptr: number, len: number): bigint;
  nbpmn_create_instance(engine: number, idPtr: number, idLen: number, varsBits: bigint): bigint;
  nbpmn_correlate_message(engine: number, namePtr: number, nameLen: number, keyPtr: number, keyLen: number): bigint;
  nbpmn_trigger_timers(engine: number, now: bigint): bigint;
  nbpmn_is_completed(engine: number, instanceKey: bigint): number;
  nbpmn_instance_count(engine: number): bigint;
}

export interface CreateOptions {
  /** Override the default packaged wasm (e.g., to load from a URL or a custom path). */
  wasmBytes?: BufferSource;
  /** Override the packaged manifest. If omitted the packaged one is used. */
  manifest?: WasmManifest;
}

/** Load the wasm blob + manifest packaged alongside this module. */
async function loadPackagedWasm(): Promise<{ bytes: Buffer; manifest: WasmManifest }> {
  const wasmDir = join(HERE, '..', 'wasm');
  const [bytes, manifestJson] = await Promise.all([
    readFile(join(wasmDir, 'nano_engine.wasm')),
    readFile(join(wasmDir, 'manifest.json'), 'utf8'),
  ]);
  return { bytes, manifest: JSON.parse(manifestJson) as WasmManifest };
}

/**
 * In-process Nano engine backed by wasm. Own the lifecycle: `close()` frees
 * both the wasm engine handle and its linear memory.
 *
 * Codename: **Bernd** (see file header). `EmbeddedEngine.CODENAME` exposes it
 * programmatically; boot logs, topology advertisement and env-var prefix all
 * carry it.
 */
export class EmbeddedEngine {
  /**
   * Feature codename: `"Bernd"` (after Bernd Ruecker; see ADR 0005). Public so
   * downstream code — banners, /v2/topology handlers, telemetry — can surface
   * the codename without hard-coding the string.
   */
  static readonly CODENAME = 'Bernd';

  private readonly exports: NanoExports;
  private readonly engine: number;
  private readonly encoder = new TextEncoder();
  private closed = false;

  readonly manifest: WasmManifest;

  private constructor(exports: NanoExports, engine: number, manifest: WasmManifest) {
    this.exports = exports;
    this.engine = engine;
    this.manifest = manifest;
  }

  /**
   * Instantiate the wasm engine. The default loads the wasm packaged with this
   * module; pass `wasmBytes` to load from elsewhere (browser fetch, tests, etc).
   */
  static async create(options: CreateOptions = {}): Promise<EmbeddedEngine> {
    let bytes: BufferSource;
    let manifest: WasmManifest;
    if (options.wasmBytes && options.manifest) {
      bytes = options.wasmBytes;
      manifest = options.manifest;
    } else {
      const packaged = await loadPackagedWasm();
      bytes = options.wasmBytes ?? packaged.bytes;
      manifest = options.manifest ?? packaged.manifest;
    }

    if (manifest.abi_version !== EXPECTED_ABI_VERSION) {
      throw new Error(
        `nano_engine.wasm ABI mismatch: manifest reports v${manifest.abi_version}, ` +
          `this host requires v${EXPECTED_ABI_VERSION}. Upgrade @nanobpm/nano-bernd.`,
      );
    }

    const { instance } = await WebAssembly.instantiate(bytes, {});
    const exports = instance.exports as unknown as NanoExports;
    const engine = exports.nbpmn_engine_new();
    if (engine === 0) throw new Error('nbpmn_engine_new returned null');
    return new EmbeddedEngine(exports, engine, manifest);
  }

  /** Deploy BPMN XML. Returns the number of process definitions the engine parsed (>=0). */
  deploy(bpmnXml: string): { count: number } {
    this.assertOpen();
    const [ptr, len] = this.writeStr(bpmnXml);
    try {
      const count = this.exports.nbpmn_deploy_bpmn(this.engine, ptr, len);
      const n = Number(count);
      if (n < 0) throw new Error(`deploy failed (${n})`);
      return { count: n };
    } finally {
      this.exports.nbpmn_free(ptr, len);
    }
  }

  /**
   * Start a process instance. Returns the process instance key as a string
   * (u64 in wasm; stringified so callers can round-trip without BigInt woes).
   */
  createInstance(processId: string): { processInstanceKey: string } {
    this.assertOpen();
    const [ptr, len] = this.writeStr(processId);
    try {
      // varsBits: 0 = no variables. Reserved for a future JSON-vars channel.
      const key = this.exports.nbpmn_create_instance(this.engine, ptr, len, 0n);
      if (key === 0n) throw new Error(`create_instance failed (no such process id: ${processId})`);
      return { processInstanceKey: String(key) };
    } finally {
      this.exports.nbpmn_free(ptr, len);
    }
  }

  /** Correlate a message. Returns the correlation record id (>=0), or -1 for no match. */
  correlateMessage(name: string, correlationKey: string): bigint {
    this.assertOpen();
    const [namePtr, nameLen] = this.writeStr(name);
    const [keyPtr, keyLen] = this.writeStr(correlationKey);
    try {
      return this.exports.nbpmn_correlate_message(this.engine, namePtr, nameLen, keyPtr, keyLen);
    } finally {
      this.exports.nbpmn_free(namePtr, nameLen);
      this.exports.nbpmn_free(keyPtr, keyLen);
    }
  }

  /**
   * Fire due timers as of `nowEpochMs`. The engine is clock-free; the host
   * decides cadence. Returns the number of timers fired.
   *
   * EmbeddedEngine wrappers (JVM, Deno) call this from a scheduled ticker;
   * direct callers can call it on demand from tests with fake time.
   */
  triggerTimers(nowEpochMs: number): bigint {
    this.assertOpen();
    return this.exports.nbpmn_trigger_timers(this.engine, BigInt(nowEpochMs));
  }

  isCompleted(processInstanceKey: string): boolean {
    this.assertOpen();
    return this.exports.nbpmn_is_completed(this.engine, BigInt(processInstanceKey)) === 1;
  }

  instanceCount(): number {
    this.assertOpen();
    return Number(this.exports.nbpmn_instance_count(this.engine));
  }

  close(): void {
    if (this.closed) return;
    this.exports.nbpmn_engine_free(this.engine);
    this.closed = true;
  }

  private writeStr(s: string): [number, number] {
    const bytes = this.encoder.encode(s);
    const ptr = this.exports.nbpmn_alloc(bytes.length);
    if (ptr === 0 && bytes.length > 0) throw new Error(`nbpmn_alloc(${bytes.length}) failed`);
    new Uint8Array(this.exports.memory.buffer).set(bytes, ptr);
    return [ptr, bytes.length];
  }

  private assertOpen(): void {
    if (this.closed) throw new Error('EmbeddedEngine has been closed');
  }
}
