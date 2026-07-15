import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { getInstance } from "../gen";
import { fetchProcessXml } from "../lib/api";
import { useLiveInvalidation } from "../lib/useLiveInvalidation";
import BpmnViewer from "../components/BpmnViewer";
import { Badge, SectionLabel } from "../components/ui";

export default function InstanceDetail({
  instanceKey,
}: {
  instanceKey: string;
}) {
  // Detail refetches on the same live signal as the list.
  useLiveInvalidation(["instance"]);
  const { data, isLoading, error } = useQuery({
    queryKey: ["instance", instanceKey],
    queryFn: async () =>
      (await getInstance({ path: { key: instanceKey }, throwOnError: true })).data,
  });

  const defKey = data?.instance.process_definition_key;
  const { data: xml } = useQuery({
    queryKey: ["process-xml", defKey],
    queryFn: () => fetchProcessXml(defKey!),
    enabled: !!defKey,
    staleTime: Infinity,
  });

  if (isLoading) return <p className="p-8 text-fg-muted">Loading…</p>;
  if (error)
    return <p className="p-8 text-danger">Failed to load: {String(error)}</p>;
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
      <header className="border-b border-edge px-8 py-4">
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-semibold text-fg">{instance.process_id}</h1>
          <Badge tone="neutral">{instance.state}</Badge>
          {instance.has_incident && <Badge tone="danger">Incident</Badge>}
        </div>
        <div className="mt-1 font-mono text-xs text-fg-faint">
          instance {instance.key} · definition {instance.process_definition_key}{" "}
          · v{instance.version}
        </div>
      </header>

      {/* bg-white is intentional: the BPMN diagram canvas is a physical white
          "sheet" regardless of theme. */}
      <div className="h-72 shrink-0 border-b border-edge bg-white">
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
                <tr key={i.key} className="border-b border-edge">
                  <Td>{i.element_id}</Td>
                  <Td>{i.kind}</Td>
                  <Td>{i.state}</Td>
                  <Td className="text-danger">{i.reason}</Td>
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
                <tr key={`${v.scope_key}:${v.name}`} className="border-b border-edge">
                  <Td className="font-medium">{v.name}</Td>
                  <Td className="font-mono text-fg-muted">{v.value}</Td>
                  <Td className="font-mono text-fg-faint">{v.scope_key}</Td>
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
                <tr key={j.key} className="border-b border-edge">
                  <Td>{j.element_id}</Td>
                  <Td className="font-mono">{j.job_type}</Td>
                  <Td>{j.state}</Td>
                  <Td>{j.retries}</Td>
                  <Td className="text-fg-faint">{j.worker ?? "—"}</Td>
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
      <SectionLabel>{title}</SectionLabel>
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
        <tr className="border-b border-edge text-left text-fg-faint">
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
  return <p className="text-sm text-fg-faint">{children}</p>;
}
