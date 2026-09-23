------------------------------ MODULE MCInclusiveInParallel ------------------------------
(* An inclusive diamond on one branch of a parallel diamond:
   S -> P1 -> {X, B};  X -> I1 -> {A1, A2} -> I2 -> P2;  B -> P2 -> E *)
EXTENDS TokenFlow

MCNodes == {"S", "P1", "X", "I1", "A1", "A2", "I2", "B", "P2", "E"}
MCKind   == [n \in MCNodes |-> CASE n = "S" -> "start" [] n = "E" -> "end"
                                 [] n \in {"P1", "P2"} -> "and"
                                 [] n \in {"I1", "I2"} -> "or" [] OTHER -> "task"]
MCStart  == "S"
MCFlows  == {<<"S","P1">>, <<"P1","X">>, <<"P1","B">>, <<"X","I1">>,
             <<"I1","A1">>, <<"I1","A2">>, <<"A1","I2">>, <<"A2","I2">>,
             <<"I2","P2">>, <<"B","P2">>, <<"P2","E">>}
=============================================================================
