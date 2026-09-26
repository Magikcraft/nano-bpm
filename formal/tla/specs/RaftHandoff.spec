# Spec descriptor for the RaftHandoff family (#1228). Registers the leader-
# durable leadership handoff + reclaim model (ADR 0019) with the multi-spec
# harness (#1226). Self-contained: every SPEC_* variable below is private to
# this spec (check.sh sources it in a fresh subshell), so this file never edits
# check.sh or a sibling descriptor. See formal/README.md ("Adding a spec
# family") and specs/TokenFlow.spec for the contract.
#
# Anti-drift anchor: the fence rule the spec formalises (Wins / NextEpoch) is
# the canonical `nano-server-raft::fence` implementation the gateway binary uses
# (server/src/main.rs). The crate's conformance test
# (server/crates/nano-server-raft/src/fence.rs `mod tests`) replays this spec's
# transition system against those exact functions, so the model and the
# production fence rule cannot silently diverge. No trace fixtures are declared:
# the #1226 replay parser (trace/parse.mjs) speaks engine-core's BPMN token-flow
# vocabulary, a different subsystem, so the Raft anchor is the crate-side
# conformance test rather than the engine trace-validation harness.

SPEC_NAME="RaftHandoff"
SPEC_MODELS_DIR="raft"
SPEC_MODELS_GLOB="RH*.tla"

SPEC_CONSTANTS=(
  "Nodes      <- MCNodes"
  "Owner      <- MCOwner"
  "Fencing    <- MCFencing"
  "MaxCrashes <- MCMaxCrashes"
)

SPEC_INVARIANTS=(TypeOK LeaderOwnsFence NoSplitWhenConverged)
SPEC_PROPERTIES=(ReclaimConverges)

SPEC_EXPECTED=(
  # Fencing on: the safety invariants hold and the partition converges to a
  # single stable leader (correct reclaim after a crashed leader / handoff).
  # Two nodes suffice to exercise a symmetric same-epoch split resolved by the
  # lowest-id tie-break, crash-failover reclaim, and the incumbent -> Owner
  # handoff. Multi-node (>2) safety is covered exhaustively, and far more
  # cheaply than a liveness model-check, by the crate conformance test.
  "RHHandoff2    pass"
  # Fencing off: a leader that adopts a beating fence fails to step down, so two
  # concurrent same-epoch promoters both keep leading — the split-brain ADR
  # 0019's fencing step prevents. It breaks LeaderOwnsFence and never converges
  # to one leader. The verdict is the ratchet that keeps the fencing step-down
  # guarded.
  "RHNoFencing2  violates:LeaderOwnsFence,ReclaimConverges"
)
