//! Headless end-to-end DAP conversation against the built adapter. Uses the
//! VS Code debug-adapter test-support client to launch `dist/index.js` and drive
//! a real DAP handshake: launch → setBreakpoints → configurationDone → stopped →
//! stackTrace/variables → continue → terminated. This is the integration guard
//! that the wasm engine, the session translation, and the source map line up.

import { fileURLToPath } from 'node:url';

import { DebugClient } from '@vscode/debugadapter-testsupport';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

const ADAPTER = fileURLToPath(new URL('../dist/index.js', import.meta.url));
const FIXTURE = fileURLToPath(new URL('./fixtures/two-tasks.bpmn', import.meta.url));
/** `<bpmn:startEvent id="s" />` is line 4 of the fixture. */
const START_EVENT_LINE = 4;

describe('nanobpmn DAP adapter', () => {
  let client: DebugClient;

  beforeEach(async () => {
    client = new DebugClient('node', ADAPTER, 'nanobpmn');
    await client.start();
  });

  afterEach(async () => {
    await client.stop();
  });

  it('stops at a start-event breakpoint, then runs to completion', async () => {
    await client.initializeRequest();

    // launch does not start the run; it emits `initialized` so we can set breakpoints.
    const launched = client.launch({ bpmn: FIXTURE, processId: 'p' });
    await client.waitForEvent('initialized');

    const bp = await client.setBreakpointsRequest({
      source: { path: FIXTURE },
      breakpoints: [{ line: START_EVENT_LINE }],
    });
    expect(bp.body.breakpoints[0]?.verified).toBe(true);

    // configurationDone starts the run; it should stop at the start event.
    const activeEvt = client.waitForEvent('nanobpmn/activeElements');
    const stopped = client.waitForEvent('stopped');
    await client.configurationDoneRequest();
    await launched;
    const stop = await stopped;
    expect(stop.body.reason).toBe('breakpoint');
    const threadId = stop.body.threadId ?? 1;

    // The custom webview event reports the paused element(s).
    const active = await activeEvt;
    expect(active.body.elements).toContain('s');
    // The paused frame is anchored to the start-event line.
    const stack = await client.stackTraceRequest({ threadId });
    expect(stack.body.stackFrames[0]?.line).toBe(START_EVENT_LINE);
    expect(stack.body.stackFrames[0]?.name).toBe('s');

    // Variables scope is reachable and well-formed (empty for this run).
    const scopes = await client.scopesRequest({ frameId: stack.body.stackFrames[0]?.id ?? 0 });
    const ref = scopes.body.scopes[0]?.variablesReference ?? 0;
    const vars = await client.variablesRequest({ variablesReference: ref });
    expect(Array.isArray(vars.body.variables)).toBe(true);

    // Continue drains the run to quiescence → terminated.
    await Promise.all([client.waitForEvent('terminated'), client.continueRequest({ threadId })]);
  });

  it('reports a breakpoint on a non-node line as unverified', async () => {
    await client.initializeRequest();
    const launched = client.launch({ bpmn: FIXTURE, processId: 'p' });
    await client.waitForEvent('initialized');

    const bp = await client.setBreakpointsRequest({
      source: { path: FIXTURE },
      breakpoints: [{ line: 1 }], // the XML prolog — no BPMN element
    });
    expect(bp.body.breakpoints[0]?.verified).toBe(false);

    // With no verified breakpoint the run drains straight to terminated.
    await Promise.all([client.waitForEvent('terminated'), client.configurationDoneRequest()]);
    await launched;
  });
});
