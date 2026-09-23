------------------------------ MODULE MCExclusiveLoop ------------------------------
(* A rework loop: S -> A -> X; X -> A (loop back) or X -> E.
   Cyclic, so only the safety properties are checked (no termination). *)
EXTENDS TokenFlow

MCNodes == {"S", "A", "X", "E"}
MCKind   == [n \in MCNodes |-> CASE n = "S" -> "start" [] n = "E" -> "end"
                                 [] n = "X" -> "xor" [] OTHER -> "task"]
MCStart  == "S"
MCFlows  == {<<"S","A">>, <<"A","X">>, <<"X","A">>, <<"X","E">>}
=============================================================================
