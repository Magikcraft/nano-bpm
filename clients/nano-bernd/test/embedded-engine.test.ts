import { describe, it, expect, afterEach } from 'vitest';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { EmbeddedEngine, EXPECTED_ABI_VERSION, type WasmManifest } from '../src/index.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const WASM_DIR = join(HERE, '..', 'wasm');

const TRIVIAL_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f" sourceRef="s" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>`;

describe('EmbeddedEngine (nano_engine.wasm FFI)', () => {
  let host: EmbeddedEngine | undefined;

  afterEach(() => {
    host?.close();
    host = undefined;
  });

  it('loads the packaged wasm, deploys, and completes a straight-through process', async () => {
    host = await EmbeddedEngine.create();

    expect(host.manifest.abi_version).toBe(EXPECTED_ABI_VERSION);
    expect(host.manifest.imports).toEqual([]); // engine must stay self-contained
    expect(host.instanceCount()).toBe(0);

    const { count } = host.deploy(TRIVIAL_BPMN);
    expect(count).toBeGreaterThanOrEqual(1);

    const { processInstanceKey } = host.createInstance('p');
    expect(processInstanceKey).toMatch(/^\d+$/);
    expect(processInstanceKey).not.toBe('0');

    // Start -> End: engine should complete synchronously.
    expect(host.isCompleted(processInstanceKey)).toBe(true);
    expect(host.instanceCount()).toBe(1);
  });

  it('fails cleanly on ABI version mismatch', async () => {
    const bytes = await readFile(join(WASM_DIR, 'nano_engine.wasm'));
    const realManifest = JSON.parse(
      await readFile(join(WASM_DIR, 'manifest.json'), 'utf8'),
    ) as WasmManifest;

    const bogusManifest: WasmManifest = { ...realManifest, abi_version: 9999 };
    await expect(
      EmbeddedEngine.create({ wasmBytes: bytes, manifest: bogusManifest }),
    ).rejects.toThrow(/ABI mismatch/);
  });

  it('rejects createInstance for an unknown process id', async () => {
    host = await EmbeddedEngine.create();
    host.deploy(TRIVIAL_BPMN);
    expect(() => host!.createInstance('does-not-exist')).toThrow(/no such process id/);
  });

  it('reports the injected clock through triggerTimers without error', async () => {
    host = await EmbeddedEngine.create();
    host.deploy(TRIVIAL_BPMN);
    // The trivial process has no timers; triggering must be a safe no-op returning 0.
    expect(host.triggerTimers(Date.now())).toBe(0n);
  });

  it('refuses further use after close()', async () => {
    host = await EmbeddedEngine.create();
    host.close();
    expect(() => host!.deploy(TRIVIAL_BPMN)).toThrow(/closed/);
  });
});
