open Zookeeper
open Printf

let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|]
let create_flag = [||]

let () =
  let watcher_fn zhandle event_type conn_state path watcher_ctx =
    printf "%s %s\n" (show_event event_type) path
  in

  let zh = init "127.0.0.1:2181" watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0 in

  begin
    match exists zh "/pers" 0 with
    | ZNONODE,_ -> printf "gonna create node\n"
    | e,_ ->
      printf "FAIL before create\n";
      printf "%s\n" @@ show_error e;
      exit 1;
  end;
  ignore @@ create zh "/pers" "" acl create_flag;

  begin
    match exists zh "/pers" 0 with
    | ZOK,_ -> printf "node created, gonna delete\n"
    | e,_ ->
      printf "FAIL after create\n";
      printf "%s\n" @@ show_error e;
      exit 1;
  end;
  ignore @@ delete zh "/pers" 0;

  Thread.delay 0.1;

  begin
    match exists zh "/pers" 0 with
    | ZNONODE,_ -> printf "node deleted\n"
    | e,_ ->
      printf "FAIL after delete\n";
      printf "%s\n" @@ show_error e;
      exit 1;
  end;

  ignore @@ close zh;
  printf "DONE\n"
