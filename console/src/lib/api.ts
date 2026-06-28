// Typed client for the nanobpmn console API (served by the gateway under
// /console/api, separate from the generated Camunda REST surface).

export interface NodeInfo {
  node_id: number;
  address: string;
  is_self: boolean;
}

/**
 * Build a link to the equivalent console page on another cluster node.
 * `address` is that node's advertised base URL (`http://host:port`, from the
 * topology); `path` is the in-app route (e.g. "/topology"). Returns null when
 * there's no usable address — e.g. the self/local node, whose address is empty.
 */
export function nodeConsoleUrl(
  address: string | undefined | null,
  path: string,
): string | null {
  if (!address) return null;
  const base = address.replace(/\/+$/, "");
  const p = path.startsWith("/") ? path : `/${path}`;
  return `${base}/console${p}`;
}

export interface PartitionInfo {
  partition_id: number;
  replicas: number[];
  leader: number | null;
  raft_term: number | null;
}

export interface Topology {
  node_id: number;
  num_nodes: number;
  num_partitions: number;
  replication_factor: number;
  raft_enabled: boolean;
  gateway_version: string;
  nodes: NodeInfo[];
  partitions: PartitionInfo[];
}

/// Live per-node liveness from `/console/api/cluster/health` — each peer's
/// always-on `GET /v2/topology` is probed for reachability/version/latency.
export interface NodeHealth {
  nodeId: number;
  address: string;
  isSelf: boolean;
  reachable: boolean;
  version: string | null;
  latencyMs: number | null;
  error: string | null;
}

export interface ClusterHealth {
  checkedAtMs: number;
  nodes: NodeHealth[];
}

export interface Instance {
  key: string;
  process_id: string;
  process_definition_key: string;
  version: number;
  state: string;
  start_date_ms: number;
  has_incident: boolean;
  business_id: string | null;
  tags: string[];
}

export interface Variable {
  name: string;
  value: string;
  scope_key: string;
}

export interface Job {
  key: string;
  element_id: string;
  job_type: string;
  state: string;
  retries: number;
  worker: string | null;
  deadline_ms: number | null;
}

export interface Incident {
  key: string;
  element_id: string;
  kind: string;
  state: string;
  reason: string;
  created_at_ms: number;
}

/// One page of the process-instance list plus the total count, matching the
/// server's `GET /console/api/instances?page=&pageSize=` response.
export interface InstancePage {
  items: Instance[];
  total: number;
  page: number;
  pageSize: number;
}

export interface InstanceDetail {
  instance: Instance;
  variables: Variable[];
  jobs: Job[];
  incidents: Incident[];
}

// ---- Execution traces (Tier-A trace projection, design doc §3) ------------

export type TraceOutcome = "active" | "completed" | "terminated";

export interface TraceSummary {
  instanceKey: string;
  processId: string;
  version: number | null;
  businessId: string | null;
  outcome: TraceOutcome;
  startedAt: number;
  endedAt: number | null;
  durationMs: number | null;
  elementCount: number;
  incidentCount: number;
}

export interface TraceJob {
  type: string;
  worker: string | null;
  createdAt: number;
  activatedAt: number | null;
  completedAt: number | null;
  /** Total parked + service time (createdAt → completedAt). Always reliable. */
  waitMs: number | null;
  /** Queue time (createdAt → activatedAt). Needs the activation hook. */
  queueMs: number | null;
  /** Service time (activatedAt → completedAt). */
  serviceMs: number | null;
  attempts: number;
  failures: number;
}

export interface TraceElement {
  elementId: string;
  elementInstanceKey: string;
  scope: string;
  enteredAt: number;
  exitedAt: number | null;
  durationMs: number | null;
  incidents: number;
  job: TraceJob | null;
}

export interface TraceIncident {
  elementId: string;
  elementInstanceKey: string;
  kind: string;
  reason: string;
  raisedAt: number;
  resolvedAt: number | null;
}

export interface InstanceTrace {
  instanceKey: string;
  processId: string;
  version: number | null;
  businessId: string | null;
  tags: string[];
  startedAt: number;
  endedAt: number | null;
  durationMs: number | null;
  outcome: TraceOutcome;
  elements: TraceElement[];
  incidents: TraceIncident[];
  path: string[];
}

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`/console/api${path}`, {
    headers: { Accept: "application/json" },
  });
  if (!res.ok) {
    throw new Error(`${path} → HTTP ${res.status}`);
  }
  return (await res.json()) as T;
}

// --- Modeler: workspace-backed BPMN models ---------------------------------

/// `not_deployed`, `in_sync`, `modified`, or `unparsable`.
export type DeployStatus =
  | "not_deployed"
  | "in_sync"
  | "modified"
  | "unparsable";

export interface ModelSummary {
  name: string;
  process_ids: string[];
  deploy_status: DeployStatus;
  deployed_version: number | null;
  deployed_key: string | null;
  updated_at_ms: number;
  size: number;
}

export interface Model {
  name: string;
  xml: string;
  process_ids: string[];
  deploy_status: DeployStatus;
  deployed_version: number | null;
  deployed_key: string | null;
}

// ---- Workers -------------------------------------------------------------

export type WorkerPhase = "stopped" | "starting" | "running" | "crashed";

export interface WorkerMetrics {
  completed: number;
  failed: number;
  inFlight: number;
  throughput: number;
  uptimeMs: number;
  connected: boolean;
}

export interface WorkerRuntime {
  status: WorkerPhase;
  pid: number | null;
  startedAtMs: number | null;
  restarts: number;
  lastError: string | null;
  metrics: WorkerMetrics;
}

export interface WorkerSummary {
  name: string;
  files: string[];
  updatedAtMs: number;
  runtime: WorkerRuntime;
}

export interface WorkersResponse {
  workers: WorkerSummary[];
  /** Whether a Deno runtime is available to actually run workers. */
  denoAvailable: boolean;
}

export interface WorkerLogLine {
  tsMs: number;
  stream: "out" | "err" | "sys";
  text: string;
}

// ---- Metrics dashboard ----------------------------------------------------

/// A point-in-time snapshot from `/console/api/metrics`. Counters are monotonic;
/// the dashboard derives throughput rates from the deltas of successive polls.
export interface MetricsSnapshot {
  timestampMs: number;
  activeInstances: number;

  createsRest: number;
  createsStream: number;
  createsTotal: number;
  completionsRest: number;
  completionsStream: number;
  completionsTotal: number;

  connectionsActive: number;
  commitInflight: number;

  commitsTotal: number;
  writesTotal: number;
  bytesTotal: number;
  creditStallsTotal: number;

  fsyncMeanMs: number;
  commitWaitMeanMs: number;
  commitBatchMean: number;
  frameProcessingMeanMs: number;

  writerBusyRatio: number;
  residentBytes: number | null;
}

/// Per-node metrics + cluster aggregate from `/console/api/cluster/metrics`.
export interface NodeMetrics {
  nodeId: number;
  address: string;
  isSelf: boolean;
  reachable: boolean;
  error: string | null;
  metrics: MetricsSnapshot | null;
}

export interface AggregateMetrics {
  reachableNodes: number;
  totalNodes: number;
  activeInstances: number;
  createsTotal: number;
  completionsTotal: number;
  connectionsActive: number;
  commitInflight: number;
  residentBytes: number;
}

export interface ClusterMetrics {
  checkedAtMs: number;
  nodes: NodeMetrics[];
  aggregate: AggregateMetrics;
}

/// Fetches a plaintext body from the console API (used for worker file content,
/// which is served as text/plain rather than JSON).
async function getText(path: string): Promise<string> {
  const res = await fetch(`/console/api${path}`);
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `${path} → HTTP ${res.status}`);
  }
  return res.text();
}

async function send<T>(
  method: string,
  path: string,
  body?: BodyInit,
  contentType?: string,
): Promise<T> {
  const headers: Record<string, string> = {};
  if (contentType) headers["Content-Type"] = contentType;
  const res = await fetch(`/console/api${path}`, { method, headers, body });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `${path} → HTTP ${res.status}`);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

export const api = {
  topology: () => getJson<Topology>("/topology"),
  clusterHealth: () => getJson<ClusterHealth>("/cluster/health"),
  metrics: () => getJson<MetricsSnapshot>("/metrics"),
  clusterMetrics: () => getJson<ClusterMetrics>("/cluster/metrics"),
  instances: (page = 0, pageSize = 50) =>
    getJson<InstancePage>(`/instances?page=${page}&pageSize=${pageSize}`),
  instanceDetail: (key: string) =>
    getJson<InstanceDetail>(`/instances/${key}`),
  traces: (limit = 100) => getJson<TraceSummary[]>(`/traces?limit=${limit}`),
  trace: (key: string) => getJson<InstanceTrace>(`/traces/${key}`),
  models: () => getJson<ModelSummary[]>("/models"),
  model: (name: string) => getJson<Model>(`/models/${encodeURIComponent(name)}`),
  saveModel: (name: string, xml: string) =>
    send<Model>("PUT", `/models/${encodeURIComponent(name)}`, xml, "text/xml"),
  createModel: (name: string, xml: string) =>
    send<Model>(
      "POST",
      "/models",
      JSON.stringify({ name, xml }),
      "application/json",
    ),
  deleteModel: (name: string) =>
    send<void>("DELETE", `/models/${encodeURIComponent(name)}`),

  // ---- Workers -----------------------------------------------------------
  workers: () => getJson<WorkersResponse>("/workers"),
  worker: (name: string) =>
    getJson<WorkerSummary>(`/workers/${encodeURIComponent(name)}`),
  createWorker: (name: string, jobType: string) =>
    send<WorkerSummary>(
      "POST",
      "/workers",
      JSON.stringify({ name, jobType }),
      "application/json",
    ),
  deleteWorker: (name: string) =>
    send<void>("DELETE", `/workers/${encodeURIComponent(name)}`),
  workerFile: (name: string, path: string) =>
    getText(
      `/workers/${encodeURIComponent(name)}/file?path=${encodeURIComponent(path)}`,
    ),
  saveWorkerFile: (name: string, path: string, content: string) =>
    send<void>(
      "PUT",
      `/workers/${encodeURIComponent(name)}/file?path=${encodeURIComponent(path)}`,
      content,
      "text/plain",
    ),
  createWorkerFile: (name: string, path: string) =>
    send<void>(
      "POST",
      `/workers/${encodeURIComponent(name)}/file`,
      JSON.stringify({ path }),
      "application/json",
    ),
  deleteWorkerFile: (name: string, path: string) =>
    send<void>(
      "DELETE",
      `/workers/${encodeURIComponent(name)}/file?path=${encodeURIComponent(path)}`,
    ),
  startWorker: (name: string) =>
    send<WorkerRuntime>("POST", `/workers/${encodeURIComponent(name)}/start`),
  stopWorker: (name: string) =>
    send<WorkerRuntime>("POST", `/workers/${encodeURIComponent(name)}/stop`),
  // Shared library — reusable TS/JS modules under `@lib/…`, usable from every
  // worker. CRUD mirrors the per-worker file API.
  libFiles: () => send<{ files: string[] }>("GET", "/lib"),
  libFile: (path: string) =>
    getText(`/lib/file?path=${encodeURIComponent(path)}`),
  saveLibFile: (path: string, content: string) =>
    send<void>(
      "PUT",
      `/lib/file?path=${encodeURIComponent(path)}`,
      content,
      "text/plain",
    ),
  createLibFile: (path: string) =>
    send<void>("POST", "/lib/file", JSON.stringify({ path }), "application/json"),
  deleteLibFile: (path: string) =>
    send<void>("DELETE", `/lib/file?path=${encodeURIComponent(path)}`),
};

/// Bundles the named workers into a standalone, runnable Deno application and
/// triggers a browser download of the returned `.zip`. The zip ships every
/// worker's source, the embedded worker SDK, a `main.ts` that deploys all
/// `resources/*.bpmn` models on startup before running the workers, a
/// `deno.json` start task, and a README. Throws with the server's error text on
/// failure (e.g. no workers selected).
export async function exportWorkersApp(workers: string[]): Promise<void> {
  const res = await fetch("/console/api/export-workers-app", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ workers }),
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `export → HTTP ${res.status}`);
  }
  const blob = await res.blob();
  const disposition = res.headers.get("content-disposition") || "";
  const match = /filename="?([^"]+)"?/.exec(disposition);
  const filename = match ? match[1] : "nano-workers-app.zip";
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

/// Deploys BPMN XML to the engine through the standard Camunda deployment
/// endpoint (not a console API). Resolves on success; throws with the server's
/// problem detail otherwise. Deployment is idempotent, so deploying an unchanged
/// model is a safe no-op.
export async function deployXml(name: string, xml: string): Promise<void> {
  const form = new FormData();
  form.append(
    "resources",
    new Blob([xml], { type: "text/xml" }),
    `${name}.bpmn`,
  );
  const res = await fetch("/v2/deployments", { method: "POST", body: form });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `deploy → HTTP ${res.status}`);
  }
}

/// The result of starting a process instance, as returned by the Camunda
/// `createProcessInstance` endpoint. Only the fields the console needs are typed.
export interface CreateInstanceResult {
  processInstanceKey: string;
  processDefinitionId: string;
  processDefinitionVersion: number;
  processCompleted: boolean;
}

/// Starts a process instance on the connected cluster through the standard
/// Camunda endpoint `POST /v2/process-instances` (not a console API). The process
/// must already be deployed; the gateway picks the latest deployed version of
/// `processId`. When `awaitCompletion` is set the request blocks until the
/// instance reaches a terminal state (or the gateway's request timeout elapses),
/// reflected in `processInstanceCompleted`. Throws with the server's problem
/// detail on failure (e.g. 404 not-deployed, 503 RESOURCE_EXHAUSTED).
export async function createProcessInstance(opts: {
  processId: string;
  variables?: Record<string, unknown>;
  awaitCompletion?: boolean;
}): Promise<CreateInstanceResult> {
  const body: Record<string, unknown> = {
    processDefinitionId: opts.processId,
    variables: opts.variables ?? {},
  };
  if (opts.awaitCompletion) body.awaitCompletion = true;
  const res = await fetch("/v2/process-instances", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `start instance → HTTP ${res.status}`);
  }
  return (await res.json()) as CreateInstanceResult;
}

/// The verbatim BPMN XML for a process definition, served by the gateway's
/// generated Camunda endpoint (getProcessDefinitionXML) — not a console API.
export async function fetchProcessXml(
  processDefinitionKey: string,
): Promise<string | null> {
  const res = await fetch(
    `/v2/process-definitions/${processDefinitionKey}/xml`,
  );
  if (res.status === 200) {
    return res.text();
  }
  // 204 (no XML) or 404 (unknown / non-latest version).
  return null;
}

// --- RAD projects ----------------------------------------------------------
// A project is a self-contained Deno application directory under the projects
// root (resources/processes|decisions|forms, workers/, lib/, main.ts). The
// console drives its whole lifecycle — author, run, compile, export — through
// the `/console/api/projects` endpoints below.

export interface ProjectSummary {
  name: string;
  description: string;
  deployTarget: string;
  updatedMs: number;
  processes: number;
  decisions: number;
  forms: number;
  workers: number;
  running: boolean;
}

export interface ProjectConfig {
  name: string;
  description: string;
  /** Gateway base URL the app deploys to (REST API at `<deployTarget>/v2`). */
  deployTarget: string;
  /** Entrypoint module run on Run/Compile (default `main.ts`). */
  main: string;
  /** Cross-compile targets selected for export. */
  platforms: string[];
  createdMs: number;
  updatedMs: number;
}

/// One node in a project's file tree. Directories carry `children`.
export interface FileNode {
  name: string;
  /** Project-relative, `/`-separated path. */
  path: string;
  kind: "dir" | "file";
  children?: FileNode[];
}

export type RunStatus = "stopped" | "starting" | "running" | "stopping" | "error";

/// Runtime view of a project's application process.
export interface RunState {
  status: RunStatus;
  pid: number | null;
  startedAtMs: number | null;
  lastError: string | null;
  /** Whether a compile (possibly cross-platform) is in progress. */
  compiling: boolean;
}

export interface ProjectsResponse {
  projects: ProjectSummary[];
  /** Whether a Deno runtime is available to actually run/compile projects. */
  denoAvailable: boolean;
  platforms: string[];
}

export interface ProjectDetail {
  config: ProjectConfig;
  files: FileNode[];
  runState: RunState;
  denoAvailable: boolean;
  platforms: string[];
}

/// A single line of a project's run/compile log stream.
export interface ProjectLogLine {
  tsMs: number;
  /** `out` (stdout), `err` (stderr), or `sys` (supervisor notes). */
  stream: "out" | "err" | "sys";
  text: string;
}

/// Project-scoped console API. Kept separate from `api` so the project
/// lifecycle (author/run/compile/export) reads as one cohesive client.
export const projectsApi = {
  projects: () => getJson<ProjectsResponse>("/projects"),
  project: (name: string) =>
    getJson<ProjectDetail>(`/projects/${encodeURIComponent(name)}`),
  createProject: (name: string, description: string) =>
    send<ProjectConfig>(
      "POST",
      "/projects",
      JSON.stringify({ name, description }),
      "application/json",
    ),
  deleteProject: (name: string) =>
    send<void>("DELETE", `/projects/${encodeURIComponent(name)}`),
  projectConfig: (name: string) =>
    getJson<ProjectConfig>(`/projects/${encodeURIComponent(name)}/config`),
  saveProjectConfig: (name: string, config: ProjectConfig) =>
    send<ProjectConfig>(
      "PUT",
      `/projects/${encodeURIComponent(name)}/config`,
      JSON.stringify(config),
      "application/json",
    ),
  projectFiles: (name: string) =>
    getJson<{ files: FileNode[] }>(`/projects/${encodeURIComponent(name)}/files`),
  projectFile: (name: string, path: string) =>
    getText(
      `/projects/${encodeURIComponent(name)}/file?path=${encodeURIComponent(path)}`,
    ),
  saveProjectFile: (name: string, path: string, content: string) =>
    send<void>(
      "PUT",
      `/projects/${encodeURIComponent(name)}/file?path=${encodeURIComponent(path)}`,
      content,
      "text/plain",
    ),
  createProjectPath: (name: string, path: string, dir: boolean) =>
    send<void>(
      "POST",
      `/projects/${encodeURIComponent(name)}/file`,
      JSON.stringify({ path, dir }),
      "application/json",
    ),
  deleteProjectPath: (name: string, path: string) =>
    send<void>(
      "DELETE",
      `/projects/${encodeURIComponent(name)}/file?path=${encodeURIComponent(path)}`,
    ),
  runProject: (name: string) =>
    send<RunState>("POST", `/projects/${encodeURIComponent(name)}/run`),
  stopProject: (name: string) =>
    send<RunState>("POST", `/projects/${encodeURIComponent(name)}/stop`),
  compileProject: (name: string, targets: string[]) =>
    send<{ started: boolean }>(
      "POST",
      `/projects/${encodeURIComponent(name)}/compile`,
      JSON.stringify({ targets }),
      "application/json",
    ),
};

/// The download URL for a project export zip. When `dist` is set the compiled
/// `dist/` binaries are bundled too (large + platform-specific).
export function projectExportUrl(name: string, dist = false): string {
  const q = dist ? "?dist=true" : "";
  return `/console/api/projects/${encodeURIComponent(name)}/export${q}`;
}

/// Triggers a browser download of a project's export zip.
export async function exportProject(name: string, dist = false): Promise<void> {
  const res = await fetch(projectExportUrl(name, dist));
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `export → HTTP ${res.status}`);
  }
  const blob = await res.blob();
  const disposition = res.headers.get("content-disposition") || "";
  const match = /filename="?([^"]+)"?/.exec(disposition);
  const filename = match ? match[1] : `${name}.zip`;
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

/// Opens an SSE stream of a project's run/compile log lines. `onLine` is called
/// for each line (history is replayed first, then live). Returns the
/// `EventSource`; the caller is responsible for `.close()`.
export function projectLogs(
  name: string,
  onLine: (line: ProjectLogLine) => void,
): EventSource {
  const src = new EventSource(
    `/console/api/projects/${encodeURIComponent(name)}/logs`,
  );
  src.addEventListener("log", (ev) => {
    try {
      onLine(JSON.parse((ev as MessageEvent).data) as ProjectLogLine);
    } catch {
      /* ignore malformed line */
    }
  });
  return src;
}
