import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { api, fetchProcessXml } from "../lib/api";
import { useLiveInvalidation } from "../lib/useLiveInvalidation";
import BpmnViewer from "../components/BpmnViewer";

export default function InstanceDetail({
  instanceKey,
}: {
  instanceKey: string;
}) {
  // Detail refetches on the same live signal as the list.
  useLiveInvalidation(["instance"]);
  const { data, isLoading, error } = useQuery({
    queryKey: ["instance", instanceKey],
    queryFn: () => api.instanceDetail(instanceKey),
  });

  const defKey = data?.instance.process_definition_key;
  const { data: xml } = useQuery({
    queryKey: ["process-xml", defKey],
    queryFn: () => fetchProcessXml(defKey!),
    enabled: !!defKey,
    staleTime: Infinity,
  });

  if (isLoading) return <p className="p-8 text-zinc-400">Loading…</p>;
  if (error)
    return <p className="p-8 text-red-400">Failed to load: {String(error)}</p>;
  if (!data) return null;

  const { instance, variables, jobs, incidents } = data;
  // Active service tasks (pending jobs) and incident elements drive the overlay.
  const activeEls = jobs
    .filter((j) => j.state === "Created" || j.state === "Activated")
    .map((j) => j.element_id);
  const incidentEls = incidents
    .filter((i) => i.state === "Created")
    .map((i) => i.element_id);

  return (
    <div className="flex h-full flex-col">
      <header className="border-b border-zinc-800 px-8 py-4">
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-semibold">{instance.process_id}</h1>
          <span className="rounded bg-zinc-800 px-2 py-0.5 text-xs">
            {instance.state}
          </span>
          {instance.has_incident && (
            <span className="rounded bg-red-900/60 px-2 py-0.5 text-xs text-red-300">
              Incident
            </span>
          )}
        </div>
        <div className="mt-1 font-mono text-xs text-zinc-500">
          instance {instance.key} · definition {instance.process_definition_key}{" "}
          · v{instance.version}
        </div>
      </header>

      <div className="h-72 shrink-0 border-b border-zinc-800 bg-white">
        <BpmnViewer
          xml={xml ?? null}
          activeElementIds={activeEls}
          incidentElementIds={incidentEls}
        />
      </div>

      <div className="min-h-0 flex-1 overflow-auto p-8">
        {incidents.length > 0 && (
          <Section title="Incidents">
            <Table head={["Element", "Kind", "State", "Reason"]}>
              {incidents.map((i) => (
                <tr key={i.key} className="border-b border-zinc-900">
                  <Td>{i.element_id}</Td>
                  <Td>{i.kind}</Td>
                  <Td>{i.state}</Td>
                  <Td className="text-red-300">{i.reason}</Td>
                </tr>
              ))}
            </Table>
          </Section>
        )}

        <Section title="Variables">
          {variables.length === 0 ? (
            <Empty>No variables.</Empty>
          ) : (
            <Table head={["Name", "Value", "Scope"]}>
              {variables.map((v) => (
                <tr key={`${v.scope_key}:${v.name}`} className="border-b border-zinc-900">
                  <Td className="font-medium">{v.name}</Td>
                  <Td className="font-mono text-zinc-300">{v.value}</Td>
                  <Td className="font-mono text-zinc-500">{v.scope_key}</Td>
                </tr>
              ))}
            </Table>
          )}
        </Section>

        <Section title="Jobs">
          {jobs.length === 0 ? (
            <Empty>No jobs.</Empty>
          ) : (
            <Table head={["Element", "Type", "State", "Retries", "Worker"]}>
              {jobs.map((j) => (
                <tr key={j.key} className="border-b border-zinc-900">
                  <Td>{j.element_id}</Td>
                  <Td className="font-mono">{j.job_type}</Td>
                  <Td>{j.state}</Td>
                  <Td>{j.retries}</Td>
                  <Td className="text-zinc-500">{j.worker ?? "—"}</Td>
                </tr>
              ))}
            </Table>
          )}
        </Section>
      </div>
    </div>
  );
}

function Section({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <section className="mb-8">
      <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-zinc-500">
        {title}
      </h2>
      {children}
    </section>
  );
}

function Table({
  head,
  children,
}: {
  head: string[];
  children: ReactNode;
}) {
  return (
    <table className="w-full border-collapse text-sm">
      <thead>
        <tr className="border-b border-zinc-800 text-left text-zinc-500">
          {head.map((h) => (
            <th key={h} className="py-2 pr-4 font-medium">
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>{children}</tbody>
    </table>
  );
}

function Td({
  children,
  className = "",
}: {
  children: ReactNode;
  className?: string;
}) {
  return <td className={`py-2 pr-4 ${className}`}>{children}</td>;
}

function Empty({ children }: { children: ReactNode }) {
  return <p className="text-sm text-zinc-500">{children}</p>;
}
