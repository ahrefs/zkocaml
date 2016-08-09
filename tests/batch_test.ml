open Zookeeper
open Printf

let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|]
let create_flag = [|Zookeeper.ZOO_EPHEMERAL|]
let () =
  let watcher_fn zhandle event_type conn_state path watcher_ctx =
    printf "%s %s\n" (show_event event_type) path
  in
  let zh = init "127.0.0.1:2181" watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0 in
  Thread.delay 0.5;
  printf "client_id\n";
  let res = client_id zh in if res.client_id = (-1L) then (printf "cid : %Ld\n" res.client_id; exit 1);
  printf "recv_timeout\n";
  let res = recv_timeout zh in if res = -1 then exit 1;
  printf "set_context\n";
  set_context zh "foobarbaz123";
  Thread.delay 0.1;
  printf "get_context\n";
  let res = get_context zh in if res <> "foobarbaz123" then exit 1;
  printf "zstate\n";
  let res = zstate zh in printf "state : %s\n" (show_state res);
  printf "is_unrecoverable\n";
  let res = is_unrecoverable zh in printf "err : %s\n" (show_error res);
  printf "create\n"; Thread.delay 0.01;
  let err,str = create zh "/batch_basic_sync_ephemeral" "foo" acl create_flag in  if err <> ZOK then exit 1;
  printf "set\n";
  let err = set zh "/batch_basic_sync_ephemeral" "bar" (-1) in if err <> ZOK then exit 1;
  printf "get\n";
  let err,str,stat = get zh "/batch_basic_sync_ephemeral" 0 in if err <> ZOK || str <> "bar" || stat.data_length <> 3 then exit 1;
  printf "get_children\n";
  let err,stra = get_children zh "/" 0 in if err <> ZOK || Array.length stra < 1 then exit 1;
  printf "set_acl\n";
  let err = set_acl zh "/batch_basic_sync_ephemeral" (-1) [|{perms = 0b11011; scheme = "world"; id = "anyone"}|] in if err <> ZOK then exit 1;
  printf "get_acl\n";
  let err,acls,_stat = get_acl zh "/batch_basic_sync_ephemeral" in if err <> ZOK || acls.(0).perms <> 0b11011 then exit 1;
  printf "delete\n";
  let err = delete zh "/batch_basic_sync_ephemeral" (-1) in if err <> ZOK then exit 1;
  printf "exists\n"; Thread.delay 0.01;
  let err,stat = exists zh "/batch_basic_sync_ephemeral" 0 in if err <> ZNONODE then exit 1;
  ignore @@ close zh;
  printf "DONE\n"
