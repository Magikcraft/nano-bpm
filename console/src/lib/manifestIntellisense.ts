// Manifest IntelliSense — wires the framework-free completion engine
// (`manifestCompletionAt`, ADR 0029 §2) into Monaco as a JSON completion
// provider. Reference string values in `nano.app.json` (a process id under a
// trigger's `action.start`, a message name, a field's type, an llm agent, …)
// pop a picker of valid ids drawn from the project symbol index + the manifest
// itself, so makers stop hand-typing free-string ids.
//
// One provider is registered process-wide (lazily) for the `json` language. It
// only acts on models a manifest editor has registered via `setManifestSource`,
// so every other JSON file keeps Monaco's default *completion* behaviour.
//
// `setManifestSource` additionally registers the app-manifest JSON *schema* via
// `ensureManifestSchema()`. Unlike the completion provider, that call
// (`jsonDefaults.setDiagnosticsOptions`) is process-wide for ALL JSON models: it
// disables remote `$schema` fetching (`enableSchemaRequest: false`) and adds the
// bundled schema, scoped to manifests by `fileMatch`. Other JSON files are only
// affected in that Monaco no longer fetches their `$schema` over the network —
// intentional (offline-first). A future change adding another JSON schema
// provider must merge into these defaults rather than assume Monaco's originals.

import * as monaco from "monaco-editor";
import appSchema from "@nanobpm/nano-app-schema/schema";
import {
  manifestCompletionAt,
  type CandidateKind,
  type CompletionIndex,
} from "@nanobpm/nano-app-schema";
import { buildManifestSchemaOptions } from "./manifestSchema";

/** The live state a registered manifest model exposes to the provider. */
export interface ManifestSource {
  /** The parsed manifest (or undefined while it doesn't parse). */
  manifest: unknown;
  /** The project symbol index (or undefined while it's still building). */
  index?: CompletionIndex;
}

// Model URI → its current source. A ref-cell per model keeps the provider
// reading fresh state without re-registering on every keystroke.
const sources = new Map<string, { get: () => ManifestSource }>();
let registered = false;
let schemaRegistered = false;

// The manifest's `$schema` points at the published schema URL. Rather than let
// Monaco's JSON worker try to *fetch* it (there is no schema-request service
// wired up, and the console is offline-first — the fetch fails with "No schema
// request service available"), we register the bundled schema content locally,
// keyed by that same URL, so `$schema` resolves in-process with no network.
// The URI list + options shape live in `manifestSchema.ts` (pure, unit-tested).
/**
 * Register the Urban App manifest JSON Schema with Monaco's JSON language
 * service as bundled, in-process content, and disable remote schema requests.
 * Idempotent. This is what makes `nano.app.json` validate + autocomplete against
 * the schema offline, and is the fix for the "No schema request service
 * available" error raised when the manifest's `$schema` URL would otherwise be
 * fetched.
 */
function ensureManifestSchema(): void {
  if (schemaRegistered) return;
  // Mark registered only after the call succeeds, so a transient failure (e.g.
  // Monaco JSON defaults not yet initialized) doesn't permanently prevent a
  // later retry from registering the schema for the session.
  monaco.languages.json.jsonDefaults.setDiagnosticsOptions(
    buildManifestSchemaOptions(appSchema as Record<string, unknown>),
  );
  schemaRegistered = true;
}

const KIND_ICON: Record<CandidateKind, monaco.languages.CompletionItemKind> = {
  process: monaco.languages.CompletionItemKind.Class,
  message: monaco.languages.CompletionItemKind.Event,
  decision: monaco.languages.CompletionItemKind.Function,
  primitive: monaco.languages.CompletionItemKind.Keyword,
  type: monaco.languages.CompletionItemKind.Struct,
  form: monaco.languages.CompletionItemKind.File,
  datasource: monaco.languages.CompletionItemKind.Module,
  agent: monaco.languages.CompletionItemKind.Value,
  function: monaco.languages.CompletionItemKind.Function,
  variable: monaco.languages.CompletionItemKind.Field,
};

function ensureProvider(): void {
  if (registered) return;
  registered = true;
  monaco.languages.registerCompletionItemProvider("json", {
    // `.` triggers FEEL variable-path completion inside trigger action fields.
    triggerCharacters: ['"', "-", "."],
    provideCompletionItems(model, position) {
      const cell = sources.get(model.uri.toString());
      if (!cell) return { suggestions: [] };
      const src = cell.get();
      const text = model.getValue();
      const offset = model.getOffsetAt(position);
      const result = manifestCompletionAt(
        text,
        offset,
        src.manifest,
        src.index,
      );
      if (!result || result.candidates.length === 0) return { suggestions: [] };

      const start = model.getPositionAt(result.range.start);
      const end = model.getPositionAt(result.range.end);
      const range = new monaco.Range(
        start.lineNumber,
        start.column,
        end.lineNumber,
        end.column,
      );
      const suggestions = result.candidates.map((c) => ({
        label: c.value,
        kind: KIND_ICON[c.kind],
        insertText: c.value,
        detail: c.detail,
        // Filter against the whole string content so partial typing narrows.
        filterText: c.value,
        range,
      }));
      return { suggestions };
    },
  });
}

/**
 * Register (or update) a manifest model as a completion source. Returns a
 * disposer that removes it. `get` is called on each completion request so the
 * provider always sees the latest parsed manifest + index.
 */
export function setManifestSource(
  uri: string,
  get: () => ManifestSource,
): () => void {
  ensureProvider();
  ensureManifestSchema();
  sources.set(uri, { get });
  return () => {
    sources.delete(uri);
  };
}
