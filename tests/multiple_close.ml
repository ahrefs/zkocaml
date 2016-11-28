open Zookeeper
open Printf

let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|]
let create_flag = [|Zookeeper.ZOO_EPHEMERAL|]

let () =
  let watcher_fn zhandle event_type conn_state path watcher_ctx =
    printf "%s %s\n" (show_event event_type) path
  in

  let zh = init "127.0.0.1:2181" watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0 in
  Gc.compact ();
  ignore @@ exists zh "/ephemeral" 1;
  ignore @@ close zh;
  ignore @@ create zh "/ephemeral" "" acl create_flag;
  Gc.compact ();
  ignore @@ close zh;
  ignore @@ close zh;
  Gc.compact ();
  ignore @@ close zh;
  printf "DONE\n"
