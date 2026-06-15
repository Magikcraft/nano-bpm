import { spawn, type ChildProcess } from 'node:child_process';
import { createServer } from 'node:http';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createServer as netServer } from 'node:net';
import { fileURLToPath } from 'node:url';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { CommandStreamClient } from '../src/commandStreamClient.js';
import { detectNanobpm } from '../src/detect.js';
import { createStreamingJobWorker, JobActionReceipt, type JobWorkerHandle } from '../src/streamingJobWorker.js';

const here = fileURLToPath(new URL('.', import.meta.url));
const BINARY = resolve(here, '../../../server/target/release/nanobpm-gateway-rest-server');
const FIXTURE = join(here, 'fixtures/test-job-process.bpmn');
const PROCESS_ID = 'Process_0f7cr6y';

const haveBinary = (() => {
  try {
    readFileSync(BINARY);
    return true;
  } catch {
    return false;
  }
})();

const describeIf = haveBinary ? describe : describe.skip;

async function freePort(): Promise<number> {
  return new Promise((resolvePort, reject) => {
    const srv = netServer();
    srv.once('error', reject);
    srv.listen(0, () => {
      const addr = srv.address();
      if (addr && typeof addr === 'object') {
        const { port } = addr;
        srv.close(() => resolvePort(port));
      } else {
        srv.close(() => reject(new Error('no port')));
      }
    });
  });
}

async function waitForListening(baseUrl: string, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${baseUrl}/v2/topology`, { method: 'GET' });
      if (res.status < 500) return;
    } catch {
      // not up yet
    }
    await new Promise((r) => setTimeout(r, 150));
  }
  throw new Error(`server did not start within ${timeoutMs}ms`);
}

async function deployFixture(baseUrl: string): Promise<void> {
  const bytes = readFileSync(FIXTURE);
  const form = new FormData();
  form.append('resources', new Blob([bytes], { type: 'text/xml' }), 'test-job-process.bpmn');
  const res = await fetch(`${baseUrl}/v2/deployments`, { method: 'POST', body: form });
  if (!res.ok) throw new Error(`deploy failed: ${res.status} ${await res.text()}`);
}

describeIf('command stream integration (real server)', () => {
  let proc: ChildProcess;
  let dataDir: string;
  let baseUrl: string;

  beforeAll(async () => {
    const port = await freePort();
    baseUrl = `http://localhost:${port}`;
    dataDir = mkdtempSync(join(tmpdir(), 'nanobpmn-sdk-it-'));
    proc = spawn(BINARY, [], {
      env: { ...process.env, PORT: String(port), NANOBPMN_DATA_DIR: dataDir },
      stdio: 'ignore',
    });
    await waitForListening(baseUrl);
    await deployFixture(baseUrl);
  });

  afterAll(async () => {
    proc?.kill('SIGKILL');
    if (dataDir) rmSync(dataDir, { recursive: true, force: true });
  });

  it('detects a nanobpmn gateway', async () => {
    expect(await detectNanobpm(baseUrl)).toBe(true);
  });

  it('creates an instance over the command stream', async () => {
    const client = new CommandStreamClient({ baseUrl, worker: 'creator' });
    await client.connect();
    try {
      const result = await client.createInstance({ processDefinitionId: PROCESS_ID });
      expect(result.processInstanceKey).toMatch(/^\d+$/);
    } finally {
      await client.close();
    }
  });

  it('drives a streaming job worker end-to-end and awaits completion', async () => {
    let handled = 0;
    const worker: JobWorkerHandle = await createStreamingJobWorker({
      baseUrl,
      jobType: 'test-job',
      worker: 'sdk-it',
      maxParallelJobs: 5,
      jobHandler: async (job) => {
        handled += 1;
        expect(job.type).toBe('test-job');
        return job.complete({ done: true });
      },
    });
    expect(worker.transport).toBe('stream');

    const creator = new CommandStreamClient({ baseUrl, worker: 'creator' });
    await creator.connect();
    try {
      const { ack, completion } = await creator.createInstanceAndAwait(
        { processDefinitionId: PROCESS_ID },
        { fetchVariables: ['done'] },
      );
      expect(ack.processInstanceKey).toMatch(/^\d+$/);
      expect(completion.processCompleted).toBe(true);
      expect(completion.processInstanceKey).toBe(ack.processInstanceKey);
      expect(handled).toBeGreaterThanOrEqual(1);
    } finally {
      await creator.close();
      await worker.stop();
    }
  });

  it('falls back to polling when the gateway is not nanobpmn', async () => {
    // A plain HTTP server with no /command-stream upgrade stands in for Camunda.
    const dummyPort = await freePort();
    const dummy = createServer((_req, res) => {
      res.statusCode = 404;
      res.end('not found');
    });
    await new Promise<void>((r) => dummy.listen(dummyPort, r));

    let polledStop = false;
    const fakeCamunda = {
      createJobWorker() {
        return {
          stop() {
            polledStop = true;
          },
        };
      },
    };

    try {
      const worker = await createStreamingJobWorker({
        baseUrl: `http://localhost:${dummyPort}`,
        jobType: 'test-job',
        jobHandler: () => JobActionReceipt,
        camundaClient: fakeCamunda,
        detectTimeoutMs: 1000,
      });
      expect(worker.transport).toBe('poll');
      await worker.stop();
      expect(polledStop).toBe(true);
    } finally {
      await new Promise<void>((r) => dummy.close(() => r()));
    }
  });
});
