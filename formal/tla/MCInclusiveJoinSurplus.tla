------------------------------ MODULE MCInclusiveJoinSurplus ------------------------------
(* One incoming flow of an inclusive join is taken twice:
   S -> F -> {PA, B};  PA -> {A1, A2} -> XA (xor merge) -> J;  B -> J;  J -> E
   J is an inclusive join. It fires once the three tokens have arrived, consuming
   one per flow, and the surplus on XA -> J reopens it, so it fires again at the
   next quiescence and the instance completes ("Tetris" principle, #1237). *)
EXTENDS TokenFlow

MCNodes == {"S", "F", "PA", "A1", "A2", "B", "XA", "J", "E"}
MCKind   == [n \in MCNodes |-> CASE n = "S" -> "start" [] n = "E" -> "end"
                                 [] n \in {"F", "PA"} -> "and"
                                 [] n = "J" -> "or"
                                 [] n = "XA" -> "xor" [] OTHER -> "task"]
MCStart  == "S"
MCEdges  == [f1 |-> <<"S", "F">>, f2 |-> <<"F", "PA">>, f3 |-> <<"F", "B">>,
             f4 |-> <<"PA", "A1">>, f5 |-> <<"PA", "A2">>, f6 |-> <<"A1", "XA">>,
             f7 |-> <<"A2", "XA">>, f8 |-> <<"XA", "J">>, f9 |-> <<"B", "J">>,
             f10 |-> <<"J", "E">>]
MCFlows  == DOMAIN MCEdges
MCSrc    == [f \in MCFlows |-> MCEdges[f][1]]
MCTgt    == [f \in MCFlows |-> MCEdges[f][2]]
=============================================================================
