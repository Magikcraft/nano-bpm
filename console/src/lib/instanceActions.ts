// Pure, React-free helpers for the Process Explorer instance-detail operator
// actions, extracted so they can be unit-tested with `node --test` (see
// instanceActions.test.ts) without a DOM.

// The instance `state` string the console API emits is the Debug form of the
// engine's ProcessInstanceState: "Active", "Completed", "Terminated", or the
// transient "Terminating". Only a running (Active) instance can be cancelled —
// a completed/terminated one has no tokens left to discard, and a Terminating
// one is already being cancelled.
export function isCancellable(state: string): boolean {
  return state === "Active";
}

// Confirmation copy for the (destructive, irreversible) cancel action. Kept
// here so the exact wording is asserted by a test rather than buried in JSX.
export function cancelConfirmMessage(processId: string): string {
  return `Cancel process instance "${processId}"? This discards all its tokens (pending jobs, timers, message subscriptions) and terminates it. This cannot be undone.`;
}
