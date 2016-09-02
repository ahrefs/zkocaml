#! /usr/bin/env ocaml

#use "topfind";;
#thread;;
#require "oUnit";;
#require "devkit";;

module U = ExtUnix.Specific

open Printf
open ExtLib
open OUnit
open Prelude

let verbose = ref false
let server_addr = ref "127.0.0.1:2181"
let home = Filename.dirname @@ U.realpath Sys.argv.(0)

(* number of workers *)
let nproc = 8
let make_chunks = Action.partition

let execute s =
  if !verbose then prerr_endline ("+ " ^ s);
  match Sys.command s with 0 -> () | n -> Exn.fail "%S : exit code %d" s n

let cmd fmt = ksprintf execute fmt

let test_list = List.map (sprintf "tests/%s") [
  "batch_test";
  "binary_data";
  "completion_callback_after_gc";
  "connect";
  "create_delete_persistent";
  "disposable_watcher";
  "watcher_after_gc";
  ]

let rm = Exn.suppress Sys.remove

let clean_bin () =
  test_list |> List.iter (fun name ->
    rm (sprintf "%s/%s.cmi" home name);
    rm (sprintf "%s/%s.cmx" home name);
    rm (sprintf "%s/%s.o" home name);)

let clean () =
  test_list |> List.iter (fun name ->
    rm @@ (sprintf "%s/%s.out" home name);
    rm @@ (sprintf "%s/%s" home name);)

let empty_file path =
  let open Unix in
  try (stat path).st_size = 0 with Unix_error(ENOENT,_,_) -> true

let test ?filter () =
  let buildd = home ^ "/_build/src" in
  let run name =
    name >:: begin fun () ->
      let path = sprintf "%s/%s" home name in
      cmd "ocamlfind ocamlopt -g -o %s -I %s -thread -linkpkg -package ctypes %s/zkocaml.cmxa -cclib -L%s %s.ml" path buildd buildd buildd path;
      cmd "%s > %s.out 2>&1" path path;
      if empty_file (sprintf "%s.out" path) then Exn.fail "empty %s.out file" name;
    end
  in
  let module Worker = struct type task = int * OUnit.test list type result = bool end in
  let module W = Parallel.Threads(Worker) in
  let worker (index, tests) =
    let () =
      let open Sys in
      let abort _ = exit 1 in
      set_signal sigterm (Sys.Signal_handle abort);
      set_signal sigint (Sys.Signal_handle abort)
    in
    let name = Printf.sprintf "main%d" index in
    let t = name >::: tests in
    run_test_tt t |> List.for_all begin function
    | RFailure _| RError _ -> false
    | RSuccess _| RTodo _| RSkip _ -> true
    end
  in
  let filter name = match filter with None -> true | Some l -> List.exists (fun s -> String.starts_with name s) l in
  let all_tests = test_list |> List.filter filter |> List.map run in
  (* can be filtered *)
  let nproc = min nproc (List.length all_tests) in
  let proc = W.create worker nproc in
  let tasks = all_tests |> make_chunks nproc |> Array.to_list |> List.mapi (fun i l -> (i,l)) |> List.enum in
  let ok = ref true in
  W.perform proc tasks (fun b -> ok := !ok && b);
  clean_bin ();
  !ok

let run_test ?filter () =
  let filter = Option.map (List.map (fun s -> try Filename.chop_extension s with _ -> s)) filter in
  match test ?filter () with
  | true -> exit 0
  | false -> exit 1

let help () =
  printfn "Usage:";
  printfn "  ./test.ml [options] <command>";
  printfn "Options:";
  printfn "  -v      verbose mode";
  printfn "Commands:";
  printfn "  test [testpath]  run given test (or all by default)";
  printfn "  clean            remove temporary files";
  ()

let () =
  Sys.chdir home;
  let rec loop = function
  | ["clean"] -> clean ()
  | ["clean_bin"] -> clean_bin ()
  | ["test"] -> run_test ()
  | "test"::filter -> run_test ~filter ()
  | "-v"::tl -> verbose := true; prerr_endline "Running verbose"; loop tl
  | [] | ("-h"|"--help"|"help")::[] -> help (); exit 0
  | x::_ -> printfn "E: unrecognized parameter %S" x; help (); exit 1
  in
  loop Action.args
