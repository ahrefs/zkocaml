open Zookeeper
open Printf

let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|]
let create_flag = [|Zookeeper.ZOO_EPHEMERAL|]

let () =
  let watcher_fn zhandle event_type conn_state path watcher_ctx =
    printf "%s %s\n" (show_event event_type) path
  in

  let completion e s1 s2 =
    printf "%s: %s, %s\n" (show_error e) s1 s2
  in

  for i = 1 to 10 do
    let zh = init "127.0.0.1:2181" watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0 in
    ignore @@ exists zh "/ephemeral" 1;
    ignore @@ acreate zh "/ephemeral" "" acl create_flag completion "done";
    Gc.compact ();
    printf "compacted\n";
    Thread.delay 0.1;
    ignore @@ close zh;
  done;

  Gc.compact ();
  printf "DONE\n"
