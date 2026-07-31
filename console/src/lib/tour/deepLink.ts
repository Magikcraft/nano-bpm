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
