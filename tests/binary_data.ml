open Zookeeper
open Printf

let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|]
let create_flag = [|Zookeeper.ZOO_EPHEMERAL|]

let () =
  let watcher_fn zhandle event_type conn_state path watcher_ctx =
    printf "%s %s\n" (show_event event_type) path
  in

  let zh = init "127.0.0.1:2181" watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0 in

  ignore @@ exists zh "/ephemeral" 1;
  ignore @@ create zh "/ephemeral" "\023\001h\142" acl create_flag;
  Thread.delay 0.1;
  let (_,s,_) = Zookeeper.get zh "/ephemeral" 0 in
  if s <> "\023\001h\142" then
    printf "FAIL\n";
  ignore @@ close zh;
  printf "DONE\n"
