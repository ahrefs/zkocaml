open Zookeeper
open Printf

let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|]
let create_flag = [|Zookeeper.ZOO_EPHEMERAL|]

let () =
  let watcher_fn zhandle event_type conn_state path watcher_ctx =
    printf "%s %s\n" (show_event event_type) path
  in

  let watcher zhandle event_type conn_state path watcher_ctx =
    printf "one-time: %s %s\n" (show_event event_type) path
  in

  let zh = init "127.0.0.1:2181" watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0 in

  ignore @@ wexists zh "/ephemeral" watcher "ctx";
  ignore @@ Zookeeper.create zh "/ephemeral" "" acl create_flag;
  Gc.compact ();

  ignore @@ wexists zh "/ephemeral1" watcher_fn "ctx";
  ignore @@ Zookeeper.create zh "/ephemeral1" "" acl create_flag;
  Thread.delay 0.1;
  printf "compacting\n";
  Gc.compact ();

  ignore @@ exists zh "/ephemeral2" 1;
  ignore @@ Zookeeper.create zh "/ephemeral2" "" acl create_flag;
  Gc.compact ();

  Thread.delay 0.1;
  ignore @@ Zookeeper.close zh;
  Gc.compact ();
  printf "DONE\n"
