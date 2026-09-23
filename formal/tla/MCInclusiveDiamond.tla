------------------------------ MODULE MCInclusiveDiamond ------------------------------
(* S -> I1 -> some non-empty subset of {A, B, C} -> I2 -> E *)
EXTENDS TokenFlow

MCNodes == {"S", "I1", "A", "B", "C", "I2", "E"}
MCKind   == [n \in MCNodes |-> CASE n = "S" -> "start" [] n = "E" -> "end"
                                 [] n \in {"I1", "I2"} -> "or" [] OTHER -> "task"]
MCStart  == "S"
MCFlows  == {<<"S","I1">>, <<"I1","A">>, <<"I1","B">>, <<"I1","C">>,
             <<"A","I2">>, <<"B","I2">>, <<"C","I2">>, <<"I2","E">>}
=============================================================================
