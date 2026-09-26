# Spec descriptor for the TokenFlow family (the reference example for the
# multi-spec harness, #1226). check.sh sources one descriptor per spec in a
# fresh subshell, so every `SPEC_*` variable below is private to this spec: no
# sibling spec ever edits a shared global block in check.sh. To add a new spec
# family (JobLease, RaftHandoff, SnapshotReplay, ZeebeTokenFlow, …) create a new
# `formal/tla/specs/<Name>.spec` next to this one — never edit this file or
# check.sh. See formal/README.md ("Adding a spec family").
#
# Contract (every descriptor sets these):
#   SPEC_NAME        the base TLA+ module every model EXTENDS (e.g. TokenFlow).
#   SPEC_MODELS_DIR  directory holding this spec's base module and MC/model
#                    files, relative to formal/tla. "." keeps them at the root
#                    (as TokenFlow does); a sibling may use its own subdir.
#   SPEC_MODELS_GLOB shell glob (within SPEC_MODELS_DIR) selecting this spec's
#                    concrete model files. Must not overlap another spec's glob.
#   SPEC_CONSTANTS   the generated .cfg CONSTANTS block (one "Name <- MCName"
#                    per line). This spec's own constant instantiation.
#   SPEC_INVARIANTS  the invariants TLC checks for every model of this spec.
#   SPEC_PROPERTIES  the temporal properties TLC checks (see the single-property
#                    note in check.sh: a `violates:` run can only attribute a
#                    temporal violation when there is exactly one).
#   SPEC_EXPECTED    the per-model verdict table: "<Model> pass" or
#                    "<Model> violates:<P1>,<P2>,...". The single record of what
#                    each model must do, scoped to THIS spec (never merged with a
#                    foreign spec's table).
#   SPEC_TRACE_MODELS (optional) the models whose TLC behaviours are dumped as
#                    committed trace fixtures and replayed against the Rust
#                    engine by the trace-validation harness (Deliverable B,
#                    #1226). Only completing (`pass`) models belong here.
#   SPEC_TRACE_DONE  (required iff SPEC_TRACE_MODELS is non-empty) the TLA+
#                    completion predicate for this spec's state. gen-traces.sh
#                    checks the witness invariant `~(SPEC_TRACE_DONE)`, so TLC
#                    emits the shortest behaviour that reaches completion.
#   SPEC_TRACE_GRAPH (required iff SPEC_TRACE_MODELS is non-empty) the TLA+
#                    record expression, in this spec's own graph vocabulary,
#                    that gen-traces.sh dumps as GRAPHJSON for the fixture. Must
#                    yield the keys the trace parser expects (nodes/kind/edges/
#                    start).

SPEC_NAME="TokenFlow"
SPEC_MODELS_DIR="."
SPEC_MODELS_GLOB="MC*.tla"

SPEC_CONSTANTS=(
  "Nodes <- MCNodes"
  "Kind  <- MCKind"
  "Flows <- MCFlows"
  "Src   <- MCSrc"
  "Tgt   <- MCTgt"
  "Start <- MCStart"
)

# JoinFiresAtMostOnce only holds without cycles, so the spec guards it with
# `Acyclic`, which is derived from the graph rather than declared.
SPEC_INVARIANTS=(TypeOK JoinBookkeepingCoherent ParallelJoinWaitsForEveryFlow NoStuckInstance JoinFiresAtMostOnce)
SPEC_PROPERTIES=(Termination)

SPEC_EXPECTED=(
  "MCParallelDiamond           pass"
  "MCInclusiveDiamond          pass"
  "MCChainedInclusive          pass"
  "MCInclusiveInParallel       pass"
  "MCInclusiveInTransit        pass"
  "MCExclusiveLoop             pass"
  "MCParallelDuplicateFlows    pass"
  # Unsound: two tokens on M->J, one on T->J. J fires once and, as in Zeebe, the
  # surplus token waits forever for a partner. It never fires early (#1233).
  "MCParallelJoinMultiArrival  violates:NoStuckInstance,Termination"
  # Not 1-safe: every flow into J is taken twice, so J fires twice, keeping the
  # surplus between firings ("Tetris" principle), and the instance completes.
  "MCParallelJoinSurplus       violates:JoinFiresAtMostOnce"
  # Not 1-safe: two tokens on XA->J, one on B->J. As in Zeebe (#1241), J fires
  # twice when B's token is not last (the instance completes), and strands the
  # surplus when both XA tokens arrive first. Before #1237 the first firing
  # discarded the surplus; before #1241 a quiescence sweep fired it again.
  "MCInclusiveJoinSurplus      violates:JoinFiresAtMostOnce,NoStuckInstance,Termination"
  # Sound in Zeebe's reading, but not live: when A arrives while B is live and
  # X then routes away from J, J is never re-evaluated and waits forever,
  # exactly as in Zeebe (#1241).
  "MCInclusiveDivergentPath    violates:NoStuckInstance,Termination"
)

# The completing models the trace-validation harness anchors against the Rust
# engine. Restricted to the parallel-only, routing-deterministic models: their
# behaviour is unique up to interleaving, so the engine's observable milestone
# multiset must equal the spec's exactly. Condition-routed models (xor/or) pick
# branches nondeterministically in the spec and by FEEL data in the engine, so
# they are model-checked but not (yet) trace-anchored. See formal/README.md.
SPEC_TRACE_MODELS=(MCParallelDiamond MCParallelDuplicateFlows)

# The completion predicate and process-graph record the trace generator dumps,
# in TokenFlow's own vocabulary (see SPEC_TRACE_DONE / SPEC_TRACE_GRAPH above).
# These make gen-traces.sh spec-agnostic: a sibling family supplies its own.
SPEC_TRACE_DONE="completed"
SPEC_TRACE_GRAPH="[nodes |-> MCNodes, kind |-> [n \in MCNodes |-> MCKind[n]],
     edges |-> MCEdges, start |-> MCStart]"
