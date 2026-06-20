import { useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  api,
  deployXml,
  fetchProcessXml,
  type DeployStatus,
  type ModelSummary,
} from "../lib/api";
import { useLiveInvalidation } from "../lib/useLiveInvalidation";
import BpmnModeler, {
  type BpmnModelerHandle,
} from "../components/BpmnModeler";

function statusBadge(status: DeployStatus): { label: string; cls: string } {
  switch (status) {
    case "in_sync":
      return { label: "Deployed", cls: "bg-emerald-900/60 text-emerald-300" };
    case "modified":
      return { label: "Modified", cls: "bg-amber-900/60 text-amber-300" };
    case "not_deployed":
      return { label: "Not deployed", cls: "bg-zinc-700 text-zinc-300" };
    case "unparsable":
      return { label: "Invalid", cls: "bg-red-900/60 text-red-300" };
  }
}

export default function Modeler() {
  const queryClient = useQueryClient();
  const modelerRef = useRef<BpmnModelerHandle>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const [selected, setSelected] = useState<string | null>(null);
  const [dirty, setDirty] = useState(false);
  const [processId, setProcessId] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<{ kind: "ok" | "err"; text: string } | null>(
    null,
  );

  // The engine read model advances on deploy, refreshing deploy-status badges.
  useLiveInvalidation(["models"]);
  const { data: models } = useQuery({
    queryKey: ["models"],
    queryFn: api.models,
  });

  const current: ModelSummary | undefined = models?.find(
    (m) => m.name === selected,
  );

  const refreshModels = () =>
    queryClient.invalidateQueries({ queryKey: ["models"] });

  const flash = (kind: "ok" | "err", text: string) => {
    setMessage({ kind, text });
    if (kind === "ok") setTimeout(() => setMessage(null), 3000);
  };

  const syncProcessId = () =>
    setProcessId(modelerRef.current?.getProcessId() ?? null);

  async function openModel(name: string) {
    if (dirty && !confirm("Discard unsaved changes?")) return;
    setBusy(true);
    try {
      const model = await api.model(name);
      await modelerRef.current?.importXml(model.xml);
      setSelected(name);
      setDirty(false);
      syncProcessId();
      setMessage(null);
    } catch (e) {
      flash("err", `Could not open: ${String(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function newModel() {
    if (dirty && !confirm("Discard unsaved changes?")) return;
    await modelerRef.current?.createBlank();
    setSelected(null);
    setDirty(false);
    syncProcessId();
    setMessage(null);
  }

  // Imports a BPMN file picked from disk into the modeler and saves it as a new
  // workspace model so it persists alongside the other models. The on-disk file
  // is the source; we derive a default model name from its basename. A name
  // collision falls back to a prompt so we never silently overwrite.
  async function importFromDisk(file: File) {
    if (dirty && !confirm("Discard unsaved changes?")) return;
    setBusy(true);
    try {
      const xml = await file.text();
      // Validate/normalise by round-tripping through the modeler before saving.
      await modelerRef.current?.importXml(xml);
      const normalised = (await modelerRef.current?.getXml()) ?? xml;

      const base = file.name.replace(/\.(bpmn|xml)$/i, "").trim();
      const taken = new Set((models ?? []).map((m) => m.name));
      let name = base || "imported";
      if (taken.has(name)) {
        const input = prompt(
          `A model named "${name}" already exists. Import as:`,
          `${name}-copy`,
        );
        if (!input) {
          setBusy(false);
          return;
        }
        name = input.trim();
      }

      await api.createModel(name, normalised);
      setSelected(name);
      setDirty(false);
      syncProcessId();
      refreshModels();
      flash("ok", `Imported ${file.name} as "${name}".`);
    } catch (e) {
      flash("err", `Import failed: ${String(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function save(): Promise<string | null> {
    const xml = await modelerRef.current?.getXml();
    if (xml == null) return null;
    let name = selected;
    if (!name) {
      const input = prompt("Save model as (letters, digits, - _ .):");
      if (!input) return null;
      name = input.trim();
    }
    try {
      if (selected) {
        await api.saveModel(name, xml);
      } else {
        await api.createModel(name, xml);
      }
      setSelected(name);
      setDirty(false);
      refreshModels();
      return name;
    } catch (e) {
      flash("err", `Save failed: ${String(e)}`);
      return null;
    }
  }

  async function deploy() {
    setBusy(true);
    try {
      // Deploy the saved file so the model on disk matches what's deployed
      // (so its status settles on "Deployed" rather than "Modified").
      const name = await save();
      if (!name) return;
      const xml = await modelerRef.current?.getXml();
      if (xml == null) return;
      await deployXml(name, xml);
      flash("ok", `Deployed ${name}.`);
      refreshModels();
    } catch (e) {
      flash("err", `Deploy failed: ${String(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function pull() {
    if (!current?.deployed_key) return;
    if (dirty && !confirm("Discard unsaved changes and pull the deployed version?"))
      return;
    setBusy(true);
    try {
      const xml = await fetchProcessXml(current.deployed_key);
      if (!xml) {
        flash("err", "No deployed XML available for this model.");
        return;
      }
      await modelerRef.current?.importXml(xml);
      setDirty(true);
      syncProcessId();
      flash("ok", "Pulled deployed version — Save to overwrite the local file.");
    } catch (e) {
      flash("err", `Pull failed: ${String(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function duplicate() {
    const xml = await modelerRef.current?.getXml();
    if (xml == null) return;
    const input = prompt("Duplicate as (new model name):", selected ?? "");
    if (!input) return;
    const name = input.trim();
    try {
      await api.createModel(name, xml);
      setSelected(name);
      setDirty(false);
      refreshModels();
      flash(
        "ok",
        `Copied to ${name}. It shares the process id "${processId ?? ""}" — rename it before deploying to keep them separate.`,
      );
    } catch (e) {
      flash("err", `Duplicate failed: ${String(e)}`);
    }
  }

  async function remove(name: string) {
    if (!confirm(`Delete model "${name}"? This does not undeploy it.`)) return;
    try {
      await api.deleteModel(name);
      if (selected === name) {
        await newModel();
      }
      refreshModels();
    } catch (e) {
      flash("err", `Delete failed: ${String(e)}`);
    }
  }

  function commitProcessId(next: string) {
    const trimmed = next.trim();
    if (!trimmed || trimmed === processId) return;
    modelerRef.current?.setProcessId(trimmed);
    setProcessId(trimmed);
    setDirty(true);
  }

  return (
    <div className="flex h-full">
      {/* Library */}
      <div className="flex w-72 shrink-0 flex-col border-r border-zinc-800">
        <header className="flex items-center justify-between border-b border-zinc-800 px-4 py-3">
          <h1 className="text-lg font-semibold">Models</h1>
          <div className="flex items-center gap-1.5">
            <button
              onClick={() => fileInputRef.current?.click()}
              className="rounded-md bg-zinc-800 px-2.5 py-1 text-xs text-zinc-200 hover:bg-zinc-700"
              title="Import a .bpmn file from disk"
            >
              Import
            </button>
            <button
              onClick={newModel}
              className="rounded-md bg-zinc-800 px-2.5 py-1 text-xs text-zinc-200 hover:bg-zinc-700"
            >
              + New
            </button>
          </div>
          <input
            ref={fileInputRef}
            type="file"
            accept=".bpmn,.xml,application/xml,text/xml"
            className="hidden"
            onChange={(e) => {
              const file = e.target.files?.[0];
              // Reset so selecting the same file again re-triggers onChange.
              e.target.value = "";
              if (file) void importFromDisk(file);
            }}
          />
        </header>
        <div className="min-h-0 flex-1 overflow-auto">
          {models && models.length === 0 && (
            <p className="p-4 text-sm text-zinc-500">
              No models yet. Click "New" to start one.
            </p>
          )}
          <ul>
            {models?.map((m) => {
              const badge = statusBadge(m.deploy_status);
              return (
                <li key={m.name} className="group relative">
                  <button
                    onClick={() => openModel(m.name)}
                    className={`flex w-full flex-col gap-1 border-b border-zinc-900 px-4 py-3 text-left hover:bg-zinc-900 ${
                      selected === m.name ? "bg-zinc-900" : ""
                    }`}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <span className="truncate font-medium">{m.name}</span>
                      <span
                        className={`shrink-0 rounded px-1.5 py-0.5 text-xs ${badge.cls}`}
                      >
                        {m.deploy_status === "in_sync" && m.deployed_version
                          ? `Deployed v${m.deployed_version}`
                          : badge.label}
                      </span>
                    </div>
                    <div className="truncate font-mono text-xs text-zinc-500">
                      {m.process_ids.join(", ") || "—"}
                    </div>
                  </button>
                  <button
                    onClick={() => remove(m.name)}
                    title="Delete"
                    className="absolute right-2 top-2 hidden rounded px-1 text-xs text-zinc-500 hover:text-red-400 group-hover:block"
                  >
                    ✕
                  </button>
                </li>
              );
            })}
          </ul>
        </div>
      </div>

      {/* Editor */}
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex items-center gap-2 border-b border-zinc-800 px-4 py-2">
          <span className="text-sm font-medium">
            {selected ?? "Untitled"}
            {dirty && <span className="ml-1 text-amber-400">•</span>}
          </span>
          <label className="ml-2 flex items-center gap-1 text-xs text-zinc-500">
            id
            <input
              value={processId ?? ""}
              onChange={(e) => setProcessId(e.target.value)}
              onBlur={(e) => commitProcessId(e.target.value)}
              placeholder="process id"
              className="w-40 rounded border border-zinc-700 bg-zinc-900 px-1.5 py-0.5 font-mono text-xs text-zinc-200"
            />
          </label>
          <div className="ml-auto flex items-center gap-1.5">
            <button
              onClick={save}
              disabled={busy}
              className="rounded-md bg-zinc-800 px-3 py-1 text-xs text-zinc-200 hover:bg-zinc-700 disabled:opacity-50"
            >
              Save
            </button>
            <button
              onClick={deploy}
              disabled={busy}
              className="rounded-md bg-sky-700 px-3 py-1 text-xs text-white hover:bg-sky-600 disabled:opacity-50"
            >
              Deploy
            </button>
            <button
              onClick={pull}
              disabled={busy || !current?.deployed_key}
              title={
                current?.deployed_key
                  ? "Replace the editor with the deployed version"
                  : "Not deployed"
              }
              className="rounded-md bg-zinc-800 px-3 py-1 text-xs text-zinc-200 hover:bg-zinc-700 disabled:opacity-40"
            >
              Pull
            </button>
            <button
              onClick={duplicate}
              disabled={busy}
              className="rounded-md bg-zinc-800 px-3 py-1 text-xs text-zinc-200 hover:bg-zinc-700 disabled:opacity-50"
            >
              Duplicate
            </button>
          </div>
        </header>
        {message && (
          <div
            className={`px-4 py-1.5 text-xs ${
              message.kind === "ok"
                ? "bg-emerald-950 text-emerald-300"
                : "bg-red-950 text-red-300"
            }`}
          >
            {message.text}
          </div>
        )}
        <div className="min-h-0 flex-1">
          <BpmnModeler
            ref={modelerRef}
            onChange={() => setDirty(true)}
            onReady={syncProcessId}
          />
        </div>
      </div>
    </div>
  );
}
