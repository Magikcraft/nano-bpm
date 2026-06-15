import { describe, expect, it } from 'vitest';

import { commandStreamUrl } from '../src/commandStreamClient.js';
import { encodeClientFrame, parseServerFrame } from '../src/frames.js';

describe('commandStreamUrl', () => {
  it('maps an http REST base to a ws command-stream URL', () => {
    expect(commandStreamUrl('http://localhost:8080')).toBe('ws://localhost:8080/command-stream');
  });

  it('strips a trailing /v2 REST prefix', () => {
    expect(commandStreamUrl('http://localhost:8080/v2')).toBe('ws://localhost:8080/command-stream');
  });

  it('maps https to wss and keeps a non-default host/port', () => {
    expect(commandStreamUrl('https://gw.example.com:8443/v2')).toBe(
      'wss://gw.example.com:8443/command-stream',
    );
  });

  it('accepts an explicit ws base', () => {
    expect(commandStreamUrl('ws://localhost:9999')).toBe('ws://localhost:9999/command-stream');
  });

  it('appends the worker query param when provided', () => {
    expect(commandStreamUrl('http://localhost:8080', 'my-worker')).toBe(
      'ws://localhost:8080/command-stream?worker=my-worker',
    );
  });

  it('tolerates trailing slashes', () => {
    expect(commandStreamUrl('http://localhost:8080/')).toBe('ws://localhost:8080/command-stream');
  });
});

describe('frame codec', () => {
  it('round-trips a client frame', () => {
    const encoded = encodeClientFrame({ type: 'createInstance', corr: 7, processDefinitionId: 'P' });
    expect(JSON.parse(encoded)).toEqual({ type: 'createInstance', corr: 7, processDefinitionId: 'P' });
  });

  it('parses a typed server frame', () => {
    const frame = parseServerFrame('{"type":"welcome","submissionCredits":256,"heartbeatMs":15000}');
    expect(frame.type).toBe('welcome');
    if (frame.type === 'welcome') {
      expect(frame.submissionCredits).toBe(256);
      expect(frame.heartbeatMs).toBe(15000);
    }
  });

  it('rejects a malformed frame (no type)', () => {
    expect(() => parseServerFrame('{"foo":1}')).toThrow(/malformed server frame/);
  });
});
