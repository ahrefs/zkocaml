open Ocamlbuild_plugin;;

dep ["c"; "compile"] ["src/zkocaml_stubs.h"];;

mark_tag_used "tests";
