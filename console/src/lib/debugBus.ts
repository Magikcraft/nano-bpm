/// Lightweight in-process pub/sub for a "Debug" tab shown alongside the
/// project's Output panel. Actions like Deploy / Start Instance / probe
/// requests push structured entries here; the DebugConsole subscribes and
/// renders them chronologically. Kept in a module singleton (not React
/// state) so instrumentation from api.ts / callers doesn't require prop
/// drilling or a context provider.
///
/// The buffer is bounded (last 500 entries) so a chatty session doesn't
/// leak memory. Consumers only get "future" entries after subscribing,
/// plus the current snapshot on first read via `snapshot()`.

export type DebugLevel = "info" | "ok" | "warn" | "error";

export interface DebugEntry {
  id: number;
  ts: number;
  scope: string;
  level: DebugLevel;
  message: string;
  detail?: Record<string, unknown>;
}

type Listener = (entry: DebugEntry) => void;

const MAX_ENTRIES = 500;
const buf: DebugEntry[] = [];
const listeners = new Set<Listener>();
let nextId = 1;

export function debug(
  scope: string,
  level: DebugLevel,
  message: string,
  detail?: Record<string, unknown>,
): void {
  const entry: DebugEntry = {
    id: nextId++,
    ts: Date.now(),
    scope,
    level,
    message,
    detail,
  };
  buf.push(entry);
  if (buf.length > MAX_ENTRIES) buf.shift();
  for (const l of listeners) {
    try {
      l(entry);
    } catch {
      // A misbehaving subscriber must not stop the others.
    }
  }
}

export function subscribe(listener: Listener): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function snapshot(): DebugEntry[] {
  return buf.slice();
}

export function clear(): void {
  buf.length = 0;
  // Emit a synthetic sentinel so subscribers can reset their state without
  // needing a separate "cleared" event channel.
  nextId = 1;
}
