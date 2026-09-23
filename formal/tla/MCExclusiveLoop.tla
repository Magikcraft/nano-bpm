------------------------------ MODULE MCExclusiveLoop ------------------------------
(* A rework loop: S -> A -> X; X -> A (loop back) or X -> E.
   Termination holds under strong fairness on X's routing choice: the exit is
   eventually taken. Without the exit flow it would be reported as a livelock. *)
EXTENDS TokenFlow

MCNodes == {"S", "A", "X", "E"}
MCKind   == [n \in MCNodes |-> CASE n = "S" -> "start" [] n = "E" -> "end"
                                 [] n = "X" -> "xor" [] OTHER -> "task"]
MCStart  == "S"
MCEdges  == [f1 |-> <<"S", "A">>, f2 |-> <<"A", "X">>, f3 |-> <<"X", "A">>,
             f4 |-> <<"X", "E">>]
MCFlows  == DOMAIN MCEdges
MCSrc    == [f \in MCFlows |-> MCEdges[f][1]]
MCTgt    == [f \in MCFlows |-> MCEdges[f][2]]
=============================================================================
