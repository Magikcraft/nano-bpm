------------------------------ MODULE MCParallelDiamond ------------------------------
(* S -> P1 -> {A, B} -> P2 -> E *)
EXTENDS TokenFlow

MCNodes == {"S", "P1", "A", "B", "P2", "E"}
MCKind   == [n \in MCNodes |-> CASE n = "S" -> "start" [] n = "E" -> "end"
                                 [] n \in {"P1", "P2"} -> "and" [] OTHER -> "task"]
MCStart  == "S"
MCFlows  == {<<"S","P1">>, <<"P1","A">>, <<"P1","B">>, <<"A","P2">>, <<"B","P2">>, <<"P2","E">>}
=============================================================================
