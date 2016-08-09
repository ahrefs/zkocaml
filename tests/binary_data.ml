open Zookeeper
open Printf

let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|]
let create_flag = [|Zookeeper.ZOO_EPHEMERAL|]

let () =
  let watcher_fn zhandle event_type conn_state path watcher_ctx =
    printf "%s %s\n" (show_event event_type) path
  in

  let zh = init "127.0.0.1:2181" watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0 in

  ignore @@ exists zh "/binary_data" 1;
  let crerr,_ = create zh "/binary_data" "\023\001h\142" acl create_flag in
  let (err,s,_) = Zookeeper.get zh "/binary_data" 0 in
  printf "%s -> %s : %S\n" (show_error crerr) (show_error err) s;
  if err <> ZOK ||  s <> "\023\001h\142" then exit 1;
  ignore @@ close zh;
  printf "DONE\n"
