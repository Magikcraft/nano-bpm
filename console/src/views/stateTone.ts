/**
 * Badge tone for an instance's lifecycle state. Single source of truth shared by
 * the Explorer list and the Instance Detail "Called Process Instances" section so
 * the two never drift as states / tone rules evolve. An incident always wins
 * (`danger`), regardless of `state`.
 */
export function stateTone(
  state: string,
  hasIncident: boolean,
): "danger" | "info" | "ok" | "neutral" {
  if (hasIncident) return "danger";
  switch (state) {
    case "Active":
      return "info";
    case "Completed":
      return "ok";
    case "Terminated":
      return "neutral";
    default:
      return "neutral";
  }
}
