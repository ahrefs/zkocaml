open Zookeeper
open Printf

let tests = ref []
let reg n f = tests:= (n,f)::!tests
let host = "127.0.0.1:2181"
let watcher_fn zhandle event_type conn_state path watcher_ctx =
  printf "%s %s\n" (show_event event_type) path

let () = reg "connect" @@ fun () ->
  let zh = init host watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0 in
  ignore @@ close zh;
  printf "DONE"

let () = reg "batch_test" @@ fun () ->
  let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|] in
  let create_flag = [|Zookeeper.ZOO_EPHEMERAL|] in
  let zh = init host watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0 in
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

let () = reg "binary" @@ fun () ->
  let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|] in
  let create_flag = [|Zookeeper.ZOO_EPHEMERAL|] in
  let zh = init host watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0 in
  ignore @@ exists zh "/binary_data" 1;
  let crerr,_ = create zh "/binary_data" "\023\001h\142" acl create_flag in
  let (err,s,_) = Zookeeper.get zh "/binary_data" 0 in
  printf "%s -> %s : %S\n" (show_error crerr) (show_error err) s;
  if err <> ZOK ||  s <> "\023\001h\142" then exit 1;
  ignore @@ close zh;
  printf "DONE\n"

let () = reg "completion_callback_after_gc" @@ fun () ->
  let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|] in
  let create_flag = [|Zookeeper.ZOO_EPHEMERAL|] in
  let completion e s1 s2 =
    printf "%s: %s, %s\n" (show_error e) s1 s2
  in
  for i = 1 to 10 do
    let zh = init host watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0 in
    ignore @@ exists zh "/ephemeral" 1;
    ignore @@ acreate zh "/ephemeral" "" acl create_flag completion "done";
    Gc.compact ();
    printf "compacted\n";
    Thread.delay 0.1;
    ignore @@ close zh;
  done;
  Gc.compact ();
  printf "DONE\n"

let () = reg "create_delete_persistent" @@ fun () ->
  let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|] in
  let create_flag = [||] in
  let zh = init host watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0 in
  begin
    match exists zh "/pers" 0 with
    | ZNONODE,_ -> printf "gonna create node\n"
    | e,_ ->
      printf "FAIL before create\n";
      printf "%s\n" @@ show_error e;
      exit 1;
  end;
  ignore @@ create zh "/pers" "" acl create_flag;
  begin
    match exists zh "/pers" 0 with
    | ZOK,_ -> printf "node created, gonna delete\n"
    | e,_ ->
      printf "FAIL after create\n";
      printf "%s\n" @@ show_error e;
      exit 1;
  end;
  ignore @@ delete zh "/pers" 0;
  Thread.delay 0.1;
  begin
    match exists zh "/pers" 0 with
    | ZNONODE,_ -> printf "node deleted\n"
    | e,_ ->
      printf "FAIL after delete\n";
      printf "%s\n" @@ show_error e;
      exit 1;
  end;
  ignore @@ close zh;
  printf "DONE\n"

let () = reg "multiclose" @@ fun () ->
  let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|] in
  let create_flag = [|Zookeeper.ZOO_EPHEMERAL|] in
  let zh = init host watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0 in
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

let () = reg "disposable_watcher" @@ fun () ->
  let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|] in
  let create_flag = [|Zookeeper.ZOO_EPHEMERAL|] in
  let watcher zhandle event_type conn_state path watcher_ctx =
    printf "one-time: %s %s\n" (show_event event_type) path
  in
  let zh = init host watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0 in
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

let () = reg "watcher_after_gc" @@ fun () ->
  let acl = [|{perms = 0x1f; scheme = "world"; id = "anyone"}|] in
  let create_flag = [|Zookeeper.ZOO_EPHEMERAL|] in
  for i = 1 to 59 do
    ignore @@ init "127.0.0.1:2181" watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0;
  done;

  let zh = init "127.0.0.1:2181" watcher_fn 3600 {client_id = 0L; passwd=""} "hello world" 0 in
  Gc.compact ();
  ignore @@ exists zh "/ephemeral" 1;
  ignore @@ create zh "/ephemeral" "" acl create_flag;
  Thread.delay 0.1;
  Gc.compact ();
  ignore @@ close zh;
  Gc.compact ();
  printf "DONE\n"

let () =
  match (List.tl @@ Array.to_list @@ Sys.argv) with
    | ["init"] -> List.iter (fun (n,_) -> printf "%s\n" n) !tests
    | s::_ ->
      begin
        try (List.find (fun (n,_) -> n = s) !tests |> snd) ()
        with Not_found -> failwith @@ sprintf "no such testcase %s" s
      end
    | _ -> failwith @@ sprintf "choose options carefully"
