import {
  getClusterMetrics,
  getMetrics,
  type ClusterMetrics,
  type MetricsSnapshot,
} from "../gen";

/// Module-level metrics collector. It polls continuously for the lifetime of the
/// app (not just while the Metrics view is mounted) so users can navigate away
/// and back without losing history. Memory is bounded by a fixed-length ring of
/// derived samples; raw snapshots are differenced into rates and discarded.

/// ~30 min of history at 1 Hz. A Sample is ~6 numbers, so the cap keeps the
/// buffer well under ~100 KB regardless of how long the app stays open.
export const MAX_SAMPLES = 1800;
const LOCAL_INTERVAL_MS = 1000;
const CLUSTER_INTERVAL_MS = 2000;

export interface Sample {
  t: number;
  startsPerSec: number;
  jobsPerSec: number;
  active: number;
  connections: number;
  inflight: number;
}

export interface MetricsState {
  samples: Sample[];
  latest: MetricsSnapshot | null;
  cluster: ClusterMetrics | null;
  clusterRates: { starts: number; jobs: number } | null;
  paused: boolean;
  error: string | null;
}

let state: MetricsState = {
  samples: [],
  latest: null,
  cluster: null,
  clusterRates: null,
  paused: false,
  error: null,
};

let prev: MetricsSnapshot | null = null;
let clusterPrev: { t: number; creates: number; completions: number } | null =
  null;
let localTimer: ReturnType<typeof setInterval> | null = null;
let clusterTimer: ReturnType<typeof setInterval> | null = null;
const listeners = new Set<() => void>();

function emit() {
  for (const fn of listeners) fn();
}

async function pollLocal() {
  if (state.paused) return;
  try {
    const data = (await getMetrics({ throwOnError: true })).data;
    const p = prev;
    prev = data;
    let samples = state.samples;
    if (p && data.timestampMs > p.timestampMs) {
      const dt = (data.timestampMs - p.timestampMs) / 1000;
      const rate = (cur: number, was: number) =>
        dt > 0 ? Math.max(0, (cur - was) / dt) : 0;
      const s: Sample = {
        t: data.timestampMs,
        startsPerSec: rate(data.createsTotal, p.createsTotal),
        jobsPerSec: rate(data.completionsTotal, p.completionsTotal),
        active: data.activeInstances,
        connections: data.connectionsActive,
        inflight: data.commitInflight,
      };
      samples = [...samples, s].slice(-MAX_SAMPLES);
    }
    state = { ...state, samples, latest: data, error: null };
    emit();
  } catch (e) {
    state = { ...state, error: String(e) };
    emit();
  }
}

async function pollCluster() {
  if (state.paused) return;
  try {
    const cluster = (await getClusterMetrics({ throwOnError: true })).data;
    const agg = cluster.aggregate;
    const p = clusterPrev;
    clusterPrev = {
      t: cluster.checkedAtMs,
      creates: agg.createsTotal,
      completions: agg.completionsTotal,
    };
    let clusterRates = state.clusterRates;
    if (p && cluster.checkedAtMs > p.t) {
      const dt = (cluster.checkedAtMs - p.t) / 1000;
      clusterRates = {
        starts: Math.max(0, (agg.createsTotal - p.creates) / dt),
        jobs: Math.max(0, (agg.completionsTotal - p.completions) / dt),
      };
    }
    state = { ...state, cluster, clusterRates };
    emit();
  } catch {
    // Cluster metrics are best-effort; ignore probe failures.
  }
}

function ensureRunning() {
  if (localTimer == null) {
    void pollLocal();
    localTimer = setInterval(pollLocal, LOCAL_INTERVAL_MS);
  }
  if (clusterTimer == null) {
    void pollCluster();
    clusterTimer = setInterval(pollCluster, CLUSTER_INTERVAL_MS);
  }
}

export const metricsStore = {
  subscribe(fn: () => void): () => void {
    listeners.add(fn);
    ensureRunning();
    return () => listeners.delete(fn);
  },
  getSnapshot(): MetricsState {
    return state;
  },
  setPaused(paused: boolean) {
    state = { ...state, paused };
    emit();
  },
  clear() {
    prev = null;
    clusterPrev = null;
    state = { ...state, samples: [], clusterRates: null };
    emit();
  },
};
