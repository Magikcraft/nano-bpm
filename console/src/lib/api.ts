// Typed client for the nanobpmn console API (served by the gateway under
// /console/api, separate from the generated Camunda REST surface).

export interface NodeInfo {
  node_id: number;
  address: string;
  is_self: boolean;
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

export interface InstanceDetail {
  instance: Instance;
  variables: Variable[];
  jobs: Job[];
  incidents: Incident[];
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
  instances: () => getJson<Instance[]>("/instances"),
  instanceDetail: (key: string) =>
    getJson<InstanceDetail>(`/instances/${key}`),
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
};

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
