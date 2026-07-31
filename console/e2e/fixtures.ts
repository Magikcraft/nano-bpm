// Deterministic console-API stubs for the journey guards (#417).
//
// The suite never talks to a real gateway. That is not just convenience: the
// tour's preconditions are computed from `listProjects()`, so stubbing it is the
// only way to drive a *repair* path on purpose (flip both runtime flags to false
// and assert the journey offers an install hint instead of telling the user to
// press Run). It also means a run needs no engine, no Deno and no scaffolded
// project, which is what keeps this suite fast and non-flaky.
//
// Every stub returns a SHAPED payload, not `{}`. An earlier version of this file
// fulfilled unknown routes with an empty object and the Explorer view crashed on
// `.length` of an undefined array — and because the console has no error boundary,
// one view's crash unmounts the whole SPA, rail and all. The anchor assertions
// then failed with "anchor missing" when the real cause was a bad fixture. So:
// shapes are taken from `src/gen/types.gen.ts`, and `assertNoPageCrash` (below)
// makes a crash report itself instead of masquerading as anchor rot.

import { expect, type Page } from "@playwright/test";

export interface StubOptions {
  /** Both false ⇒ `hasJsRuntime` resolves to "repair". */
  denoAvailable?: boolean;
  nodeAvailable?: boolean;
  /** 0 ⇒ `hasTraces` resolves to "skip". */
  traceCount?: number;
  /** Empty ⇒ `hasProject` resolves to "skip", and the picker's empty state shows. */
  projects?: { name: string; lang?: string }[];
  /** >1 ⇒ `hasCluster` resolves to "ok". */
  nodeCount?: number;
}

const template = (id: string, label: string) => ({
  id,
  label,
  description: `${label} description`,
  lang: "deno",
  source: "builtin",
});

/** A zero-valued MetricsSnapshot — every numeric field the Metrics view reads. */
const metricsSnapshot = () => ({
  timestampMs: 0,
  activeInstances: 0,
  createsRest: 0,
  createsStream: 0,
  createsTotal: 0,
  completionsRest: 0,
  completionsStream: 0,
  completionsTotal: 0,
  connectionsActive: 0,
  commitInflight: 0,
  commitsTotal: 0,
  writesTotal: 0,
  bytesTotal: 0,
  creditStallsTotal: 0,
  fsyncMeanMs: 0,
  commitWaitMeanMs: 0,
  commitBatchMean: 0,
  frameProcessingMeanMs: 0,
  writerBusyRatio: 0,
  residentBytes: null,
  ceilingThroughput: false,
  ceilingMemory: false,
  ceilingExporter: false,
  ceilingFlowControl: false,
  exporterFillPermille: 0,
  slaMode: "latency",
  pendingCreateQueue: 0,
  activeBacklog: 0,
  admissionBacklogLimit: 0,
  admissionCreateQueueLimit: 0,
  admissionShedTotal: 0,
  recovery: { state: "idle", startedAtMs: null, completedAtMs: null },
});

/**
 * Route-intercept every console API call with a shaped response.
 *
 * Ordering matters: the most specific patterns are tested first, and the
 * catch-all is last. Anything not listed returns `{}` — acceptable only because
 * `assertNoPageCrash` turns the resulting render failure into a clear error.
 */
export async function stubConsoleApi(
  page: Page,
  options: StubOptions = {},
): Promise<void> {
  const {
    denoAvailable = true,
    nodeAvailable = true,
    traceCount = 0,
    nodeCount = 1,
    projects = [{ name: "demo", lang: "deno" }],
  } = options;

  const projectsBody = {
    projects: projects.map((p) => ({
      name: p.name,
      description: "",
      deployTarget: "http://localhost:8080",
      updatedMs: 0,
      processes: 1,
      decisions: 0,
      forms: 0,
      workers: 0,
      running: false,
      source: "workspace",
      lang: p.lang ?? "deno",
    })),
    denoAvailable,
    nodeAvailable,
    platforms: [],
    templates: [
      template("starter", "Starter app"),
      template("workflow-starter", "Code-first workflow"),
      template("urban-starter", "Urban App"),
    ],
    extensions: { extensions: [], yolo: false },
  };

  // `/traces` returns a bare ARRAY of summaries, not an object — getting this
  // wrong crashed the Traces view rather than showing an empty table.
  const traces = Array.from({ length: traceCount }, (_, i) => ({
    traceId: `t${i}`,
    processDefinitionId: "demo",
    processInstanceKey: `${1000 + i}`,
    startedAtMs: 0,
    durationMs: 1,
    spanCount: 1,
  }));

  await page.route("**/console/api/**", async (route) => {
    const url = new URL(route.request().url()).pathname;
    const json = (body: unknown) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(body),
      });

    if (url.endsWith("/console/api/projects")) return json(projectsBody);
    if (url.endsWith("/console/api/instances")) {
      return json({ items: [], total: 0, page: 0, pageSize: 25 });
    }
    if (url.endsWith("/console/api/traces")) return json(traces);
    if (url.endsWith("/console/api/topology")) {
      return json({
        node_id: 0,
        num_nodes: nodeCount,
        num_partitions: 1,
        replication_factor: 1,
        raft_enabled: nodeCount > 1,
        gateway_version: "e2e",
      });
    }
    if (url.endsWith("/console/api/metrics")) return json(metricsSnapshot());
    if (url.endsWith("/console/api/cluster/metrics")) {
      return json({
        checkedAtMs: 0,
        nodes: [],
        aggregate: {
          reachableNodes: nodeCount,
          totalNodes: nodeCount,
          activeInstances: 0,
          createsTotal: 0,
          completionsTotal: 0,
          connectionsActive: 0,
        },
      });
    }
    if (url.endsWith("/console/api/workers")) {
      return json({ workers: [], denoAvailable, nodeAvailable });
    }
    if (url.endsWith("/console/api/extensions")) {
      return json({ extensions: [], yolo: false });
    }
    return json({});
  });
}

/**
 * Clear persisted journey state, ONCE per test.
 *
 * `addInitScript` runs on every navigation, including `reload()` — clearing
 * unconditionally wiped the state the resume guard exists to verify, so the
 * journey restarted at step one and the test failed for a reason that had nothing
 * to do with the app. A sessionStorage sentinel survives a reload within the same
 * tab, so the reset happens on first load only.
 */
export async function resetTourState(page: Page): Promise<void> {
  await page.addInitScript(() => {
    try {
      if (window.sessionStorage.getItem("e2e.tour.reset")) return;
      window.sessionStorage.setItem("e2e.tour.reset", "1");
      window.localStorage.removeItem("nano.tour.v2");
      window.localStorage.removeItem("nano.tour.v1.seen");
    } catch {
      /* storage disabled — the app tolerates it, so must the test */
    }
  });
}

/**
 * Fail with the actual error if a view crashed.
 *
 * The console has no error boundary, so an uncaught render error unmounts the
 * whole SPA and every `data-tour` anchor disappears at once. Without this, that
 * presents identically to the breakage this suite is meant to detect.
 */
export function assertNoPageCrash(page: Page): () => void {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  return () =>
    expect(
      errors,
      "a view threw during render, which unmounts the whole SPA — the missing anchor below is a symptom, not the cause",
    ).toEqual([]);
}
