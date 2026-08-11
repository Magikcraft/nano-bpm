import { useState, type ReactNode } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  cancelInstance,
  getInstance,
  resolveIncident,
  setInstanceVariables,
} from "../gen";
import { fetchProcessXml } from "../lib/api";
import { isCancellable, cancelConfirmMessage } from "../lib/instanceActions";
import { useLiveInvalidation } from "../lib/useLiveInvalidation";
import BpmnViewer from "../components/BpmnViewer";
import { fmtClock, fmtDuration } from "../components/TraceTimeline";
import { Badge, Button, Input, SectionLabel } from "../components/ui";

export default function InstanceDetail({
  instanceKey,
}: {
  instanceKey: string;
}) {
  // Detail refetches on the same live signal as the list.
  useLiveInvalidation(["instance"]);
  const qc = useQueryClient();
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);

  const { data, isLoading, error } = useQuery({
    queryKey: ["instance", instanceKey],
    queryFn: async () =>
      (await getInstance({ path: { key: instanceKey }, throwOnError: true }))
        .data,
  });

  const defKey = data?.instance.process_definition_key;
  const { data: xml } = useQuery({
    queryKey: ["process-xml", defKey],
    queryFn: () => fetchProcessXml(defKey!),
    enabled: !!defKey,
    staleTime: Infinity,
  });

  // Both operator actions refresh the same detail query so the diagram overlay,
  // the incident list and the variables all reflect the new engine state.
  const refresh = () =>
    qc.invalidateQueries({ queryKey: ["instance", instanceKey] });

  const onResolve = (incidentKey: string) => {
    setBusy(true);
    setActionError(null);
    resolveIncident({
      path: { key: instanceKey, incidentKey },
      throwOnError: true,
    })
      .then(refresh)
      .catch((e) => setActionError(String(e)))
      .finally(() => setBusy(false));
  };

  const onSetVariable = (scopeKey: string, name: string, value: unknown) => {
    setBusy(true);
    setActionError(null);
    return setInstanceVariables({
      path: { key: instanceKey },
      body: { scopeKey, variables: { [name]: value } },
      throwOnError: true,
    })
      .then(refresh)
      .catch((e) => {
        setActionError(String(e));
        throw e;
      })
      .finally(() => setBusy(false));
  };

  // Destructive: discard every token and terminate the instance. Confirmed
  // first because it cannot be undone; refreshes the detail so the state badge
  // flips to Terminated and the overlay clears.
  const onCancelInstance = (processId: string) => {
    if (!window.confirm(cancelConfirmMessage(processId, instanceKey))) return;
    setBusy(true);
    setActionError(null);
    cancelInstance({ path: { key: instanceKey }, throwOnError: true })
      .then(refresh)
      .catch((e) => setActionError(String(e)))
      .finally(() => setBusy(false));
  };

  if (isLoading) return <p className="p-8 text-fg-muted">Loading…</p>;
  if (error)
    return <p className="p-8 text-danger">Failed to load: {String(error)}</p>;
  if (!data) return null;

  const { instance, variables, jobs, incidents } = data;
  // Active service tasks (pending jobs) and open-incident elements drive the
  // overlay. Incidents project with state "Active" once raised (they become
  // "Resolved" — kept for history — after an operator clears them).
  const activeEls = jobs
    .filter((j) => j.state === "Created" || j.state === "Activated")
    .map((j) => j.element_id);
  const incidentEls = incidents
    .filter((i) => i.state === "Active")
    .map((i) => i.element_id);

  return (
    <div className="flex h-full flex-col">
      <header className="border-b border-edge px-8 py-4">
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-semibold text-fg">
            {instance.process_id}
          </h1>
          <Badge tone="neutral">{instance.state}</Badge>
          {instance.has_incident && <Badge tone="danger">Incident</Badge>}
          {isCancellable(instance.state) && (
            <Button
              size="sm"
              variant="danger"
              disabled={busy}
              className="ml-auto"
              onClick={() => onCancelInstance(instance.process_id)}
            >
              Cancel instance
            </Button>
          )}
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
        {actionError && (
          <p className="mb-4 rounded-md border border-danger/30 bg-danger/10 px-3 py-2 text-sm text-danger">
            {actionError}
          </p>
        )}

        {incidents.length > 0 && (
          <Section title="Incidents">
            <Table head={["Element", "Kind", "State", "Reason", ""]}>
              {incidents.map((i) => (
                <tr key={i.key} className="border-b border-edge">
                  <Td>{i.element_id}</Td>
                  <Td>{i.kind}</Td>
                  <Td>{i.state}</Td>
                  <Td className="text-danger">{i.reason}</Td>
                  <Td className="text-right">
                    {i.state === "Active" && (
                      <Button
                        size="sm"
                        disabled={busy}
                        onClick={() => onResolve(i.key)}
                      >
                        Resolve
                      </Button>
                    )}
                  </Td>
                </tr>
              ))}
            </Table>
          </Section>
        )}

        <Section title="Variables">
          {variables.length === 0 ? (
            <Empty>No variables.</Empty>
          ) : (
            <Table head={["Name", "Value", "Scope", ""]}>
              {variables.map((v) => (
                <VariableRow
                  key={`${v.scope_key}:${v.name}`}
                  name={v.name}
                  value={v.value}
                  scopeKey={v.scope_key}
                  busy={busy}
                  onSave={(parsed) =>
                    onSetVariable(v.scope_key, v.name, parsed)
                  }
                />
              ))}
            </Table>
          )}
        </Section>

        <Section title="Jobs">
          {jobs.length === 0 ? (
            <Empty>No jobs.</Empty>
          ) : (
            <Table
              head={[
                "Element",
                "Type",
                "State",
                "Retries",
                "Worker",
                "Activated",
                "Timeout",
              ]}
            >
              {jobs.map((j) => (
                <tr key={j.key} className="border-b border-edge">
                  <Td>{j.element_id}</Td>
                  <Td className="font-mono">{j.job_type}</Td>
                  <Td>{j.state}</Td>
                  <Td>{j.retries}</Td>
                  <Td className="text-fg-faint">{j.worker ?? "—"}</Td>
                  <Td className="text-fg-faint">
                    {j.activated_at_ms != null
                      ? fmtClock(j.activated_at_ms)
                      : "—"}
                  </Td>
                  <Td className="text-fg-faint">
                    {j.timeout_ms != null ? fmtDuration(j.timeout_ms) : "—"}
                  </Td>
                </tr>
              ))}
            </Table>
          )}
        </Section>
      </div>
    </div>
  );
}

/** One variable row with inline edit. The stored `value` is a serialized-JSON
 * string (e.g. `"text"`, `42`, `true`); the editor is seeded with it and the
 * input is parsed as JSON on save so the engine receives a typed value. */
function VariableRow({
  name,
  value,
  scopeKey,
  busy,
  onSave,
}: {
  name: string;
  value: string;
  scopeKey: string;
  busy: boolean;
  onSave: (parsed: unknown) => Promise<unknown>;
}) {
  const [editing, setEditing] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [draft, setDraft] = useState(value);
  const [parseError, setParseError] = useState<string | null>(null);

  // A value worth collapsing: multi-line, or too long to sit on one row without
  // blowing out the panel (e.g. an LLM prompt). Short scalars render as-is with
  // no toggle so the common case stays quiet.
  const isLong = value.length > 80 || value.includes("\n");

  const start = () => {
    setDraft(value);
    setParseError(null);
    setEditing(true);
  };
  const cancel = () => {
    setEditing(false);
    setParseError(null);
  };
  const save = () => {
    if (busy) return;
    let parsed: unknown;
    try {
      parsed = JSON.parse(draft);
    } catch {
      setParseError('Value must be valid JSON (e.g. 42, true, "text").');
      return;
    }
    onSave(parsed)
      .then(() => setEditing(false))
      .catch(() => {
        /* surfaced by the parent's action error banner */
      });
  };

  if (!editing) {
    return (
      <tr className="border-b border-edge align-top">
        <Td className="font-medium">{name}</Td>
        <Td className="font-mono text-fg-muted">
          <div className="flex max-w-[36rem] items-start gap-1.5">
            {isLong && (
              <button
                type="button"
                aria-label={expanded ? "Collapse value" : "Expand value"}
                aria-expanded={expanded}
                onClick={() => setExpanded((x) => !x)}
                className="mt-px shrink-0 select-none text-fg-faint hover:text-fg"
              >
                {expanded ? "▼" : "▶"}
              </button>
            )}
            {expanded ? (
              <pre className="min-w-0 flex-1 whitespace-pre-wrap break-words">
                {value}
              </pre>
            ) : (
              <span
                className={`min-w-0 flex-1 truncate ${
                  isLong ? "cursor-pointer" : ""
                }`}
                title={isLong ? "Click to expand" : undefined}
                onClick={isLong ? () => setExpanded(true) : undefined}
              >
                {value}
              </span>
            )}
          </div>
        </Td>
        <Td className="font-mono text-fg-faint">{scopeKey}</Td>
        <Td className="text-right">
          <Button size="sm" variant="ghost" disabled={busy} onClick={start}>
            Edit
          </Button>
        </Td>
      </tr>
    );
  }

  return (
    <tr className="border-b border-edge">
      <Td className="font-medium">{name}</Td>
      <Td colSpan={2}>
        <Input
          className="w-full font-mono text-xs"
          value={draft}
          autoFocus
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") save();
            if (e.key === "Escape") cancel();
          }}
        />
        {parseError && (
          <span className="mt-1 block text-xs text-danger">{parseError}</span>
        )}
      </Td>
      <Td className="text-right whitespace-nowrap">
        <Button size="sm" disabled={busy} onClick={save}>
          Save
        </Button>{" "}
        <Button size="sm" variant="ghost" disabled={busy} onClick={cancel}>
          Cancel
        </Button>
      </Td>
    </tr>
  );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="mb-8">
      <SectionLabel>{title}</SectionLabel>
      {children}
    </section>
  );
}

function Table({ head, children }: { head: string[]; children: ReactNode }) {
  return (
    <table className="w-full border-collapse text-sm">
      <thead>
        <tr className="border-b border-edge text-left text-fg-faint">
          {head.map((h, idx) => (
            <th key={h || `col-${idx}`} className="py-2 pr-4 font-medium">
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
  colSpan,
}: {
  children: ReactNode;
  className?: string;
  colSpan?: number;
}) {
  return (
    <td className={`py-2 pr-4 ${className}`} colSpan={colSpan}>
      {children}
    </td>
  );
}

function Empty({ children }: { children: ReactNode }) {
  return <p className="text-sm text-fg-faint">{children}</p>;
}
