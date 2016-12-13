#! /usr/bin/env ocaml

#use "topfind";;
#thread;;
#require "oUnit";;
#require "extlib";;

open ExtLib
open Printf
open OUnit

let printfn fmt = Printf.ksprintf print_endline fmt
let verbose = ref false
let home = Sys.getcwd ()
let run_binary = sprintf "ocamlrun -I %s/_build/src utests.byte" home

(* number of workers *)
let nproc = 8
let make_chunks n l =
  assert (n >= 0);
  if n < 2 then [| l |] else
  let a = Array.make n [] in
  List.iteri (fun i x -> let i = i mod n in a.(i) <- x :: a.(i)) l;
  a

let execute s =
  if !verbose then prerr_endline ("+ " ^ s);
  match Sys.command s with 0 -> () | n -> failwith @@ sprintf "%S : exit code %d" s n

let cmd fmt = ksprintf execute fmt

let test_list =
  let chan = open_in (home ^ "/tests/test_list") in
  Std.input_list chan

(* let rm s = try Sys.remove s with _ -> () *)

let empty_file path =
  let open Unix in
  try (stat path).st_size = 0 with Unix_error(ENOENT,_,_) -> true

let test ?filter () =
  let run name =
    let out = sprintf "%s/tests/%s.out" home name in
    name >:: begin fun () ->
      cmd "%s %s > %s 2>&1" run_binary name out;
      if empty_file out then failwith "empty out file";
    end
  in
  let open Sys in
  let abort _ = exit 1 in
  set_signal sigterm (Sys.Signal_handle abort);
  set_signal sigint (Sys.Signal_handle abort);
  let filter name = match filter with None -> true | Some l -> List.exists (fun s -> String.starts_with name s) l in
  let tests = test_list |> List.filter filter |> List.map run in
  let t = "utest" >::: tests in
  let res = run_test_tt t |> List.for_all begin function
    | RFailure _| RError _ -> false
    | RSuccess _| RTodo _| RSkip _ -> true
  end in
  if not res then exit 1

let help () =
  printfn "Usage:";
  printfn "  ./run.ml [options] <command>";
  printfn "Options:";
  printfn "  -v      verbose mode";
  printfn "Commands:";
  printfn "  test [testname]  run given test (or all by default)";
  printfn "  clean            remove temporary files";
  printfn "  init             update test list";
  ()

let () =
  Sys.chdir home;
  let rec loop = function
  | ["test"] -> test ()
  | "test"::filter -> test ~filter ()
  | ["clean"] -> cmd "rm -f %s/tests/*.out" home
  | ["init"] -> cmd "%s init > %s/tests/test_list" run_binary home
  | "-v"::tl -> verbose := true; prerr_endline "Running verbose"; loop tl
  | [] | ("-h"|"--help"|"help")::[] -> help (); exit 0
  | x::_ -> printfn "E: unrecognized parameter %S" x; help (); exit 1
  in
  loop (List.tl @@ Array.to_list @@ Sys.argv)
