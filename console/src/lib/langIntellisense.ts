// Framework-free store for pack-contributed IntelliSense, kept out of any
// `monaco-editor` import so App.tsx can feed it from the extensions overview
// without dragging the multi-MB Monaco bundle into the initial chunk. The
// Monaco providers themselves live in the lazily-loaded CodeEditor chunk and
// read this store live (see CodeEditor.tsx `registerIntellisenseProviders`).
//
// Only TS/JS get a real language service (Monaco's TS worker). Every other
// language relies on this curated, SDK-derived data — merged across every
// installed pack that targets the same `monacoLang`.

export interface CompletionEntry {
  label: string;
  kind?: string;
  insertText?: string;
  snippet?: boolean;
  detail?: string;
  documentation?: string;
}

export interface SignatureParamEntry {
  label: string;
  documentation?: string;
}

export interface SignatureEntry {
  trigger: string;
  label: string;
  documentation?: string;
  parameters?: SignatureParamEntry[];
}

/** Merged, ready-to-serve IntelliSense for one Monaco language. */
export interface MergedIntellisense {
  triggerCharacters: string[];
  completions: CompletionEntry[];
  /** symbol -> markdown hover contents (last pack wins on conflict). */
  hovers: Map<string, string>;
  /** trigger identifier -> signatures offered for its call. */
  signatures: Map<string, SignatureEntry[]>;
}

// Shape of the slice of the extensions overview we consume (a structural subset
// of the generated `ExtensionsOverview`, so we don't couple to codegen here).
interface OverviewLike {
  extensions: ReadonlyArray<{
    intellisense?: ReadonlyArray<{
      monacoLang: string;
      triggerCharacters?: string[];
      completions?: CompletionEntry[];
      hovers?: { symbol: string; contents: string }[];
      signatures?: SignatureEntry[];
    }>;
  }>;
}

const store = new Map<string, MergedIntellisense>();
const subscribers = new Set<() => void>();

function emptyMerged(): MergedIntellisense {
  return {
    triggerCharacters: [],
    completions: [],
    hovers: new Map(),
    signatures: new Map(),
  };
}

/// Rebuild the store from a fresh extensions overview, merging every pack's
/// `intellisense[]` by `monacoLang`, then notify subscribers so the Monaco
/// providers can pick up any newly contributed languages. Idempotent.
export function setIntellisenseFromOverview(
  ov: OverviewLike | undefined,
): void {
  store.clear();
  for (const ext of ov?.extensions ?? []) {
    for (const block of ext.intellisense ?? []) {
      const lang = block.monacoLang;
      if (!lang) continue;
      let merged = store.get(lang);
      if (!merged) {
        merged = emptyMerged();
        store.set(lang, merged);
      }
      for (const tc of block.triggerCharacters ?? []) {
        if (tc && !merged.triggerCharacters.includes(tc)) {
          merged.triggerCharacters.push(tc);
        }
      }
      for (const c of block.completions ?? []) {
        if (c?.label) merged.completions.push(c);
      }
      for (const h of block.hovers ?? []) {
        if (h?.symbol) merged.hovers.set(h.symbol, h.contents);
      }
      for (const s of block.signatures ?? []) {
        if (!s?.trigger) continue;
        const list = merged.signatures.get(s.trigger) ?? [];
        list.push(s);
        merged.signatures.set(s.trigger, list);
      }
    }
  }
  for (const cb of subscribers) cb();
}

/** Every Monaco language id that currently has IntelliSense data. */
export function intellisenseLangs(): string[] {
  return [...store.keys()];
}

/** Merged data for one language, or undefined when the pack ships none. */
export function getIntellisense(lang: string): MergedIntellisense | undefined {
  return store.get(lang);
}

/// Subscribe to store updates (used by the Monaco provider layer to register
/// providers for languages contributed after it first loaded). Returns an
/// unsubscribe function.
export function subscribeIntellisense(cb: () => void): () => void {
  subscribers.add(cb);
  return () => subscribers.delete(cb);
}
