# Spec descriptor for the SnapshotReplay family (#1229) — the snapshot /
# compaction / cold-archive + replay-migration recovery protocol behind
# incidents #1065–#1071 (server/crates/nano-server-storage/src/seglog.rs:
# `recover`, `compact`, `migrate_by_replay`, `read_cold_prefix`,
# `prune_cold_archive`; engine-core's `SNAPSHOT_FORMAT_VERSION` /
# `check_format_version`).
#
# Registered through the multi-spec registry (#1226): check.sh sources this
# descriptor in a fresh subshell, so every SPEC_* below is private to this
# spec and no sibling's TokenFlow/MC global block is touched. This spec keeps
# its base module + models in its own `snapshot/` subdir (SPEC_MODELS_DIR), so
# its `MCSnapReplay*.tla` glob never overlaps TokenFlow's root `MC*.tla`.
#
# The two safety properties checked are the ones that failed historically:
#   NoSilentRewind (#1065) — recovery never reconstructs a strictly-older
#     `[0, total)` history than existed;
#   FailClosed (#1066) — when the full history genuinely cannot be
#     reconstructed (a pruned gap, or an unreadable/UnknownVariant frame),
#     recovery rejects rather than silently proceeding.
#
# Anti-drift: this spec is NOT trace-anchored through the shared engine-milestone
# pipeline (SPEC_TRACE_MODELS is unset) — that pipeline (gen-traces.sh +
# trace/parse.mjs) is coupled to TokenFlow's engine state vocabulary and targets
# engine-core's Engine, a different subsystem. The storage-side recovery+migration
# path is anchored instead by the Rust conformance test
# `mod snapshot_replay_conformance` in
# server/crates/nano-server-storage/src/seglog.rs, which asserts the SAME two
# invariants (NoSilentRewind, FailClosed) against the real `recover`/`compact`/
# migration path. See formal/README.md ("Adding a spec family").

SPEC_NAME="SnapshotReplay"
SPEC_MODELS_DIR="snapshot"
SPEC_MODELS_GLOB="MCSnapReplay*.tla"

SPEC_CONSTANTS=(
  "MaxEvents          <- MCMaxEvents"
  "CurVersion         <- MCCurVersion"
  "FailClosedRecovery <- MCFailClosed"
)

SPEC_INVARIANTS=(TypeOK NoSilentRewind FailClosed)
SPEC_PROPERTIES=()

SPEC_EXPECTED=(
  # The shipped fail-closed recovery: both safety invariants hold across every
  # reachable durable world. This is the ratchet guarding seglog::recover.
  "MCSnapReplayCorrect  pass"
  # The historical pre-#1066 recovery reproduces both defects: it silently
  # rewinds onto a stale/compacted-away base (#1065) and proceeds on a pruned /
  # undecodable history instead of failing closed (#1066). The fix that landed
  # fail-closed recovery is what flips MCSnapReplayCorrect to pass; this row
  # keeps the defect from being silently reintroduced.
  "MCSnapReplayNaive    violates:FailClosed,NoSilentRewind"
)
