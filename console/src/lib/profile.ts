// Console build profiles (ADR 0034).
//
// The console ships two profiles from one source tree:
//
//   - "studio"  — the full RAD IDE: the BPMN/DMN/form modelers, the Monaco code
//                 editors, the pack marketplace. The maker surface.
//   - "observe" — the operator surface only: topology, metrics, traces, worker
//                 health, instance explorer. No modeler, no Monaco.
//
// The profile is chosen at BUILD time via `VITE_CONSOLE_PROFILE`. Vite inlines
// `import.meta.env.VITE_*` as a string literal, so `IS_STUDIO` folds to a
// compile-time boolean and Rollup dead-code-eliminates the studio-only route
// registrations — and, crucially, the `lazy(() => import(...))` anchors guarded
// by it — so the heavy IDE chunks (Monaco's ts.worker/typescript, the bpmn/dmn
// modeler bundle) are never emitted into an `observe` build.
//
// Default is "studio" so `npm run dev` and the normal build are unchanged; the
// lean operator build is opt-in (`npm run build:observe`).

export type ConsoleProfile = "studio" | "observe";

/** True in the full IDE build; false in the lean operator ("observe") build.
 *
 * Backed by the `__STUDIO__` literal that vite.config.ts `define`s from
 * `VITE_CONSOLE_PROFILE`. Two rules:
 *
 *   - Ordinary runtime gates (nav filtering, effects, default tab) may read
 *     `IS_STUDIO`.
 *   - The `lazy(() => import(...))` anchors that pull the heavy IDE chunks MUST
 *     guard on the raw `__STUDIO__` literal instead (`__STUDIO__ ? lazy(...) :
 *     null`). esbuild inlines `__STUDIO__` during transform, so the `import()`
 *     target folds to dead code before Rollup walks it — otherwise Vite's
 *     `?worker` plugin emits Monaco's worker chunks as orphans even though the
 *     module is later tree-shaken. An *imported* `IS_STUDIO` const folds too
 *     late to prevent that emit. */
export const IS_STUDIO: boolean = __STUDIO__;

export const CONSOLE_PROFILE: ConsoleProfile = IS_STUDIO ? "studio" : "observe";
