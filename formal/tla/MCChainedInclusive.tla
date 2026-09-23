------------------------------ MODULE MCChainedInclusive ------------------------------
(* Chained inclusive joins (the fire-one-join-per-sweep case):
   S -> I1 -> {A, B, C};  A, B -> J1;  J1, C -> J2 -> E *)
EXTENDS TokenFlow

MCNodes == {"S", "I1", "A", "B", "C", "J1", "J2", "E"}
MCKind   == [n \in MCNodes |-> CASE n = "S" -> "start" [] n = "E" -> "end"
                                 [] n \in {"I1", "J1", "J2"} -> "or" [] OTHER -> "task"]
MCStart  == "S"
MCFlows  == {<<"S","I1">>, <<"I1","A">>, <<"I1","B">>, <<"I1","C">>,
             <<"A","J1">>, <<"B","J1">>, <<"J1","J2">>, <<"C","J2">>, <<"J2","E">>}
=============================================================================
