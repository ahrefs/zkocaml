#load "zkocaml.cma";;
open Zookeeper

let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|];;
let create_flag = [|Zookeeper.ZOO_EPHEMERAL|];;

let watcher_fn zhandle event_type conn_state path watcher_ctx =
  Printf.printf "%s %s\n" (show_event event_type) path;;

let watcher zhandle event_type conn_state path watcher_ctx =
  Printf.printf "one-time: %s %s\n" (show_event event_type) path;;

let zh = init "127.0.0.1:2181" watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0;;

wexists zh "/ephemeral" watcher "ctx";;
Zookeeper.create zh "/ephemeral" "" acl create_flag;;
Gc.compact ();;

wexists zh "/ephemeral1" watcher_fn "ctx";;
Zookeeper.create zh "/ephemeral1" "" acl create_flag;;
Thread.delay 0.1;;
print "compacting";;
Gc.compact ();;

exists zh "/ephemeral2" 1;;
Zookeeper.create zh "/ephemeral2" "" acl create_flag;;
Gc.compact ();;

Thread.delay 0.1;;
Zookeeper.close zh;;
Gc.compact ();;
print "DONE";;
