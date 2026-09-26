# Spec descriptor for the ZeebeTokenFlow family (#1240, slice 1). This is the
# Zeebe reference token flow — the *correct* gateway/join semantics on Camunda
# 8's surface, derived directly from `zeebe/engine` (see ZeebeTokenFlow.tla for
# the per-rule citations). Nano is a strict superset of Camunda 8, so nano's
# TokenFlow spec is refined against this one (slice 2, TokenFlow!RefinesZeebe);
# a nano-vs-Zeebe divergence surfaces there rather than being defined away.
#
# Registered through the #1226 multi-spec harness like any other family: its own
# CONSTANTS/INVARIANTS/PROPERTIES/EXPECTED, its own non-overlapping model glob
# (ZMC*.tla — the TokenFlow family owns MC*.tla). See formal/README.md
# ("Adding a spec family"). The ZMC corpus is a representative sample of the
# behaviours the reference must reproduce (parallel sync, inclusive sync, the
# arrival-time inclusive-join guard #1241, and the not-1-safe surplus); the
# single-source corpus generator (#1240 slice 3, tracked separately) will unify
# it with the MC corpus so no graph is hand-maintained twice.

SPEC_NAME="ZeebeTokenFlow"
SPEC_MODELS_DIR="."
SPEC_MODELS_GLOB="ZMC*.tla"

SPEC_CONSTANTS=(
  "Nodes <- MCNodes"
  "Kind  <- MCKind"
  "Flows <- MCFlows"
  "Src   <- MCSrc"
  "Tgt   <- MCTgt"
  "Start <- MCStart"
)

SPEC_INVARIANTS=(TypeOK TakenBookkeepingCoherent ParallelJoinWaitsForEveryFlow NoStuckInstance JoinFiresAtMostOnce)
SPEC_PROPERTIES=(Termination)

SPEC_EXPECTED=(
  "ZMCParallelDiamond          pass"
  "ZMCInclusiveDiamond         pass"
  # Sound in Zeebe's reading but not live: A arrives while B is live, X routes
  # away from J, and J is never re-evaluated — it waits forever, exactly as
  # Zeebe's arrival-time inclusive-join guard dictates (#1241).
  "ZMCInclusiveDivergentPath   violates:NoStuckInstance,Termination"
  # Not 1-safe: every incoming flow of J is taken twice, so J fires twice,
  # keeping the surplus taken record between firings (the "Tetris" principle).
  "ZMCParallelJoinSurplus      violates:JoinFiresAtMostOnce"
)
