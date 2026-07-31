// Which journeys become picker cards on an empty state (#411, ADR 0049 §2).
//
// Pure and side-effect-free apart from importing the overview module (whose id
// helper it needs); `../profile` is a type-only import, so this stays runnable
// under `node --test` without the Vite globals.

import type { ConsoleProfile } from "../profile";
import type { Journey } from "./types";
import { overviewJourneyId } from "./journeys/overview.ts";

/**
 * The journeys shown as picker *cards* — everything offerable in the profile
 * except its zero-commitment `overview`.
 *
 * The overview is not an outcome-shaped journey (its success predicate is
 * trivially true, by design), so ADR 0049 §2 offers it as the quiet "just show
 * me around" link beside the cards rather than as one of them. Everything else
 * comes straight from `journeysFor`, so adding a journey later needs no change
 * here — the card list is derived, never enumerated.
 */
export function pickerJourneys(
  available: Journey[],
  profile: ConsoleProfile,
): Journey[] {
  const overview = overviewJourneyId(profile);
  return available.filter((j) => j.id !== overview);
}
