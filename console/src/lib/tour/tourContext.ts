// The shared product-tour instance, published once at the App root.
//
// `useProductTour` creates a driver.js runner and owns the persisted journey
// state, so it must be called exactly once. App calls it and puts the result
// here; the rail button AND the empty-state journey pickers (#411, ADR 0049 §2)
// then drive the *same* runner instead of spawning competing ones with their own
// overlays and state.

import { createContext, useContext } from "react";
import type { ProductTour } from "./useProductTour";

export const TourContext = createContext<ProductTour | null>(null);

/**
 * Read the shared product tour. Returns `null` when used outside a provider
 * (e.g. a view rendered in isolation under unit test), so a picker degrades to
 * its plain empty state rather than throwing.
 */
export function useTour(): ProductTour | null {
  return useContext(TourContext);
}
