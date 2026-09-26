# Spec descriptor for the JobLease family (#1227, epic #1224): nano's job
# lease / activation semantics per ADR 0002 (leases, expiry/reclaim,
# at-least-once) and ADR 0001/0017 (single-writer partition, worker-concurrency
# governor). Registered through the multi-spec harness (#1226): check.sh sources
# this descriptor in a fresh subshell, so every SPEC_* variable below is private
# to this spec and never touches the TokenFlow descriptor or check.sh itself.
# See formal/README.md ("Adding a spec family") and specs/TokenFlow.spec.
#
# The base module and its models live under formal/tla/joblease/ (their own
# subdirectory), so this spec's SPEC_MODELS_GLOB cannot overlap the TokenFlow
# corpus's root-level MC*.tla. The engine anchor for this spec is the Rust
# conformance test engine-core/tests/job_lease_trace_validation.rs, which
# replays JobLease behaviours against Engine::apply_command (it reuses the
# #1226 harness driver rather than the token-flow milestone pipeline, so this
# spec declares no SPEC_TRACE_MODELS and never invokes the TokenFlow-vocabulary
# gen-traces.sh / parse.mjs).

SPEC_NAME="JobLease"
SPEC_MODELS_DIR="joblease"
SPEC_MODELS_GLOB="JobLease_MC*.tla"

SPEC_CONSTANTS=(
  "Jobs     <- MCJobs"
  "Workers  <- MCWorkers"
  "Timeout  <- MCTimeout"
  "MaxClock <- MCMaxClock"
)

# TypeOK — the state stays well-typed.
# AtMostOneLiveHolder — the exclusive-lease guarantee (invariant 1): no job ever
#   has two concurrent live lease holders.
# DoneIsTerminal — a completed job holds no lock, so reclaim can never
#   redeliver / un-complete a finished job (reclaim correctness, safety).
# CompletedWasActivated — a completed job was activated at least once (the
#   engine gates completion on the persistent `activated` latch, not a live
#   lock), so a job reclaimed by expiry stays completable (finding #1257).
# ExpiredIsReclaimable — a job that is neither done nor live can always be
#   re-activated (an expired/lost lease is never a dead end).
SPEC_INVARIANTS=(TypeOK AtMostOneLiveHolder DoneIsTerminal CompletedWasActivated ExpiredIsReclaimable)

# AllEventuallyComplete — every job eventually completes despite arbitrary
#   expiry and redelivery (the at-least-once contract: reclaim never drops a
#   job). Under the spec's Fairness.
SPEC_PROPERTIES=(AllEventuallyComplete)

SPEC_EXPECTED=(
  "JobLease_MC1  pass"
  "JobLease_MC2  pass"
)
