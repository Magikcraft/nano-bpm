/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Console build profile (ADR 0034): "studio" (full IDE, default) or
   * "observe" (lean operator surface). Set at build time via the environment. */
  readonly VITE_CONSOLE_PROFILE?: "studio" | "observe";
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

/** Build-time boolean literal `define`d by vite.config.ts from the console
 * profile (ADR 0034): `true` for the full "studio" IDE build, `false` for the
 * lean "observe" operator build. Guard `lazy(() => import(...))` anchors on this
 * so esbuild folds the studio-only import targets out of the observe bundle. */
declare const __STUDIO__: boolean;
