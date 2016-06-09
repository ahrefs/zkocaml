#load "zkocaml.cma";;
open Zookeeper
open Gc

let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|];;
let create_flag = [|Zookeeper.ZOO_EPHEMERAL|];;

let watcher_fn zhandle event_type conn_state path watcher_ctx =
  print (show_event event_type);;

let zh = init "127.0.0.1:2181" watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0;;

match exists zh "/pers" 0 with
| ZNONODE,_ -> ()
| e,_ ->
  print "FAIL before create";
  print @@ show_error e;;

Zookeeper.create zh "/pers" "" acl create_flag;;

match exists zh "/pers" 0 with
| ZOK,_ -> ()
| e,_ ->
  print "FAIL after create";
  print @@ show_error e;;

Zookeeper.delete zh "/pers" 0;;
Thread.delay 0.1;;
match exists zh "/pers" 0 with
| ZNONODE,_ -> ()
| e,_ ->
  print "FAIL after delete";
  print @@ show_error e;;

Zookeeper.close zh;;
print "DONE";;
