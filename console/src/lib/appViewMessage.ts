// Pure routing decision for postMessages the embedded Urban app sends to the
// console host (see AppView.tsx). The origin/source trust check stays in the
// component (it needs the live iframe ref); this decides only what an *already
// trusted* message means, so the console's node:test unit suite can cover the
// routing table directly without a renderer.

export type AppViewMessageAction =
  { kind: "theme" } | { kind: "navigate"; path: string };

/**
 * Interpret a trusted message posted by the embedded app. Returns the action
 * the host should take, or `null` when the message is unknown or malformed.
 *
 * - `nano-app-ready` → reply with the current theme.
 * - `nano-navigate` with a *known* target → navigate the console in-host. Only
 *   whitelisted targets are honoured, and the path is constructed HERE from
 *   structured params (never a raw href from the app), so app/row data can't
 *   smuggle a path or scheme across the frame boundary. The `instance` key is
 *   trimmed and URL-encoded, mirroring the Explorer deep-link contract
 *   (`/console/explorer?instance=<key>`).
 */
export function decideAppViewMessage(
  data: unknown,
): AppViewMessageAction | null {
  if (typeof data !== "object" || data === null) return null;
  const msg = data as { type?: unknown; target?: unknown; params?: unknown };

  if (msg.type === "nano-app-ready") return { kind: "theme" };

  if (msg.type === "nano-navigate") {
    if (msg.target === "processExplorer") {
      const params = msg.params;
      const instance =
        typeof params === "object" && params !== null
          ? (params as { instance?: unknown }).instance
          : undefined;
      if (typeof instance === "string" && instance.trim() !== "") {
        return {
          kind: "navigate",
          path: "/explorer?instance=" + encodeURIComponent(instance.trim()),
        };
      }
    }
    // Unknown target or missing/blank instance → ignore (defensive default).
    return null;
  }

  return null;
}
