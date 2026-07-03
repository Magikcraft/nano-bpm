import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { WebSocketServer, type WebSocket } from 'ws';

import { CommandStreamClient, SubmissionTimeoutError } from '../src/commandStreamClient.js';

/**
 * Stands up a mock command stream that welcomes with a fixed number of
 * submission credits and (optionally) never replenishes them, so we can drive
 * the client's submit-timeout path deterministically without the gateway.
 */
function startMockGateway(initialCredits: number): Promise<{
  baseUrl: string;
  sockets: WebSocket[];
  close: () => Promise<void>;
}> {
  return new Promise((resolve) => {
    const http: Server = createServer();
    const wss = new WebSocketServer({ server: http });
    const sockets: WebSocket[] = [];
    wss.on('connection', (ws) => {
      sockets.push(ws);
      ws.send(JSON.stringify({ type: 'welcome', submissionCredits: initialCredits, heartbeatMs: 0 }));
    });
    http.listen(0, '127.0.0.1', () => {
      const { port } = http.address() as AddressInfo;
      resolve({
        baseUrl: `http://127.0.0.1:${port}`,
        sockets,
        close: () =>
          new Promise<void>((done) => {
            for (const s of sockets) s.terminate();
            wss.close(() => http.close(() => done()));
          }),
      });
    });
  });
}

describe('createInstance submit timeout', () => {
  let gw: Awaited<ReturnType<typeof startMockGateway>>;

  afterEach(async () => {
    if (gw) await gw.close();
  });

  it('rejects with SubmissionTimeoutError when no credit arrives in time', async () => {
    gw = await startMockGateway(0);
    const client = new CommandStreamClient({ baseUrl: gw.baseUrl, worker: 'w', reconnect: false });
    await client.connect();

    await expect(
      client.createInstance({ processDefinitionId: 'p', submitTimeoutMs: 50 }),
    ).rejects.toBeInstanceOf(SubmissionTimeoutError);

    await client.close();
  });

  it('honours the client-wide submitTimeoutMs default', async () => {
    gw = await startMockGateway(0);
    const client = new CommandStreamClient({
      baseUrl: gw.baseUrl,
      worker: 'w',
      reconnect: false,
      submitTimeoutMs: 40,
    });
    await client.connect();

    await expect(client.createInstance({ processDefinitionId: 'p' })).rejects.toBeInstanceOf(
      SubmissionTimeoutError,
    );

    await client.close();
  });

  it('does not leak credits: a granted credit still admits a later create', async () => {
    gw = await startMockGateway(1);
    const client = new CommandStreamClient({ baseUrl: gw.baseUrl, worker: 'w', reconnect: false });
    await client.connect();
    // Give the welcome credit a beat to register.
    await new Promise((r) => setTimeout(r, 20));

    // Server never sends a commandResult, so the create hangs after acquiring
    // its credit; race it against a short delay to confirm the credit was
    // consumed (i.e. we passed the credit gate rather than timing out on it).
    const timedOut = { hit: false };
    await Promise.race([
      client.createInstance({ processDefinitionId: 'p', submitTimeoutMs: 60 }).catch((e) => {
        if (e instanceof SubmissionTimeoutError) timedOut.hit = true;
      }),
      new Promise((r) => setTimeout(r, 120)),
    ]);
    expect(timedOut.hit).toBe(false);

    await client.close();
  });
});
