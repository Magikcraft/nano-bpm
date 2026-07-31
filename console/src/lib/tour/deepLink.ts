// `?tour=<journeyId>` deep links.
//
// ADR 0049's central move: the console cannot infer a persona on a fresh install
// (no projects, no packs, one node, a pre-deployed `demo` — every signal reads
// identically for all three target users), but **the command the user ran already
// encodes it.** So `c8ctl nano start` prints a console URL with `?tour=localdev`
// and `c8ctl nano hire` prints `?tour=agentic-author` (#413), and the console
// simply obeys.
//
// Pure string functions, so they are testable without a browser.

export const TOUR_PARAM = "tour";

/**
 * The journey id requested by the URL, if any.
 *
 * Accepts a full URL or a bare query string. Ids are constrained to the shape
 * journeys actually use (lowercase, digits, dashes) so a malformed or hostile
 * parameter cannot be echoed anywhere as-is; anything else is treated as absent.
 */
export function readTourParam(search: string): string | null {
  let query = search;
  const q = search.indexOf("?");
  if (q >= 0) query = search.slice(q);
  let value: string | null = null;
  try {
    value = new URLSearchParams(query).get(TOUR_PARAM);
  } catch {
    return null;
  }
  if (value === null) return null;
  const id = value.trim().toLowerCase();
  return /^[a-z0-9][a-z0-9-]{0,63}$/.test(id) ? id : null;
}

/**
 * The `?tour=` query as it was when this module first loaded — i.e. before React
 * Router mounts.
 *
 * The console is served under `/console/`, and its index route redirects `/` to
 * the profile's home (`/projects` or `/topology`) with a bare, search-less
 * `<Navigate replace>`. That redirect runs in a child effect, which React fires
 * *before* the app-level effect that reads the deep link — so by the time the
 * deep-link effect looks at `window.location.search`, the query is already gone.
 * c8ctl prints exactly this root URL (`…/console?tour=localdev`), so reading the
 * live search would silently drop every c8ctl-launched journey.
 *
 * Snapshotting the search at module-eval time (which happens before any render,
 * hence before the redirect) is what makes the printed link actually work.
 */
const capturedSearch: string | null =
  typeof window !== "undefined" ? window.location.search : null;

/**
 * The journey id requested by the URL the app was *loaded* with, consumed once.
 *
 * One-shot on purpose: a deep link is a launch instruction, not a sticky mode, so
 * a later manual navigation or refresh must not silently re-run it. Reads the
 * module-load snapshot (see `capturedSearch`) rather than the live location, so a
 * pre-render index redirect cannot eat the parameter. Tests may inject a search.
 */
let deepLinkConsumed = false;
export function consumeDeepLinkTourParam(search?: string): string | null {
  const source = search ?? capturedSearch;
  if (search === undefined) {
    if (deepLinkConsumed) return null;
    deepLinkConsumed = true;
  }
  return source === null ? null : readTourParam(source);
}

/**
 * The same URL with the tour parameter removed, preserving everything else.
 *
 * Stripped after the journey starts so a refresh does not restart it — a deep
 * link is a one-shot instruction, not a sticky mode. Returns the input unchanged
 * when there is no such parameter, so callers can skip a needless history write.
 */
export function stripTourParam(url: string): string {
  try {
    const parsed = new URL(url, "http://localhost");
    if (!parsed.searchParams.has(TOUR_PARAM)) return url;
    parsed.searchParams.delete(TOUR_PARAM);
    const search = parsed.searchParams.toString();
    // Reassemble by hand: an absolute-vs-relative input must round-trip as it
    // came in, and URL.toString() would inject the placeholder origin.
    const isAbsolute = /^[a-z][a-z0-9+.-]*:/i.test(url);
    const path = `${parsed.pathname}${search ? `?${search}` : ""}${parsed.hash}`;
    return isAbsolute ? `${parsed.origin}${path}` : path;
  } catch {
    return url;
  }
}
