#load "zkocaml.cma";;
open Zookeeper
open Gc

let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|];;
let create_flag = [|Zookeeper.ZOO_EPHEMERAL|];;

let watcher_fn zhandle event_type conn_state path watcher_ctx =
  print (show_event event_type);;

let zh = init "127.0.0.1:2181" watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0;;

exists zh "/ephemeral" 1;;
Zookeeper.create zh "/ephemeral" "\023\001h\142" acl create_flag;;
Thread.delay 0.1;;
let (_,s,_) = Zookeeper.get zh "/ephemeral" 0;;
if s <> "\023\001h\142" then
  print "FAIL";;
Zookeeper.close zh;;
print "DONE";;
