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

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`/console/api${path}`, {
    headers: { Accept: "application/json" },
  });
  if (!res.ok) {
    throw new Error(`${path} → HTTP ${res.status}`);
  }
  return (await res.json()) as T;
}

export const api = {
  topology: () => getJson<Topology>("/topology"),
};
