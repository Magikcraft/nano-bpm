------------------------------ MODULE MCParallelJoinMultiArrival ------------------------------
(* Two tokens reach a parallel join over the SAME incoming flow:
   S -> P1 -> {P2, T};  P2 -> {X, Y} -> M (xor merge) -> J;  T -> J -> E
   BPMN/Zeebe: J waits until T has also arrived. *)
EXTENDS TokenFlow

MCNodes == {"S", "P1", "P2", "X", "Y", "M", "T", "J", "E"}
MCKind   == [n \in MCNodes |-> CASE n = "S" -> "start" [] n = "E" -> "end"
                                 [] n \in {"P1", "P2", "J"} -> "and"
                                 [] n = "M" -> "xor" [] OTHER -> "task"]
MCStart  == "S"
MCFlows  == {<<"S","P1">>, <<"P1","P2">>, <<"P1","T">>, <<"P2","X">>, <<"P2","Y">>,
             <<"X","M">>, <<"Y","M">>, <<"M","J">>, <<"T","J">>, <<"J","E">>}
=============================================================================
