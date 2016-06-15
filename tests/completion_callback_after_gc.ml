#load "zkocaml.cma";;
open Zookeeper

let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|];;
let create_flag = [|Zookeeper.ZOO_EPHEMERAL|];;

let watcher_fn zhandle event_type conn_state path watcher_ctx =
  print (show_event event_type);;

let completion e s1 s2 =
  Printf.printf "%s: %s, %s\n" (show_error e) s1 s2;;

for i = 1 to 10 do
  let zh = init "127.0.0.1:2181" watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0 in
  exists zh "/ephemeral" 1;
  Zookeeper.acreate zh "/ephemeral" "" acl create_flag completion "done";
  Gc.compact ();
  print "compacted";
  Thread.delay 0.1;
  Zookeeper.close zh;
done;;
Gc.compact ();;
print "DONE";;
