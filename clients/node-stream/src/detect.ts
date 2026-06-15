import WebSocket from 'ws';

import { commandStreamUrl } from './commandStreamClient.js';
import { parseServerFrame } from './frames.js';

export interface DetectOptions {
  /** Headers for the probe upgrade (e.g. `Authorization`). */
  headers?: Record<string, string>;
  /** How long to wait for a `welcome` frame before deciding `false`. Default `2000`. */
  timeoutMs?: number;
}

/**
 * Probes whether `baseUrl` speaks the nanobpmn command-stream protocol by
 * attempting the `/command-stream` WebSocket upgrade and waiting for a `welcome`
 * frame. Resolves `true` for nanobpmn, `false` for anything else (a Camunda
 * gateway rejects the upgrade / 404s, or never sends `welcome`).
 *
 * The probe socket is always closed before resolving, so this is side-effect
 * free aside from one short-lived connection.
 */
export function detectNanobpm(baseUrl: string, opts: DetectOptions = {}): Promise<boolean> {
  const timeoutMs = opts.timeoutMs ?? 2000;
  const url = commandStreamUrl(baseUrl);
  return new Promise<boolean>((resolve) => {
    let ws: WebSocket;
    try {
      ws = new WebSocket(url, { headers: opts.headers });
    } catch {
      resolve(false);
      return;
    }

    let done = false;
    const finish = (result: boolean) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      try {
        ws.removeAllListeners();
        ws.terminate();
      } catch {
        // ignore teardown errors
      }
      resolve(result);
    };

    const timer = setTimeout(() => finish(false), timeoutMs);
    timer.unref?.();

    ws.on('message', (data: WebSocket.RawData) => {
      try {
        finish(parseServerFrame(data.toString()).type === 'welcome');
      } catch {
        finish(false);
      }
    });
    ws.on('error', () => finish(false));
    ws.on('close', () => finish(false));
  });
}
