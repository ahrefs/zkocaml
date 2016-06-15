/* ZkOCaml: OCaml Binding For Apache ZooKeeper.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdlib.h>
#include <string.h>

#include <caml/alloc.h>
#include <caml/callback.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/signals.h>
#include <caml/threads.h>

#include <zookeeper/zookeeper.h>

#include "zkocaml_stubs.h"

#define zkocaml_enter_callback() \
  int zkocaml_c_thread_registered = caml_c_thread_register(); \
  if (zkocaml_c_thread_registered) caml_acquire_runtime_system()

#define zkocaml_leave_callback()     \
  if (zkocaml_c_thread_registered) { \
    caml_release_runtime_system(); \
    caml_c_thread_unregister(); \
  }

#define zkocaml_handle_struct_val(v) \
  (*(zkocaml_handle_t **)Data_custom_val(v))

#define zkocaml_table_len(v) \
  sizeof(v) / sizeof(v[0])

#define ZKOCAML_MAX_PATH_BUFFER_SIZE 4096

static FILE *zkocaml_log_stream = NULL;

static const enum ZOO_ERRORS ZOO_ERRORS_TABLE[] = {
  ZOK,
  ZSYSTEMERROR,
  ZRUNTIMEINCONSISTENCY,
  ZDATAINCONSISTENCY,
  ZCONNECTIONLOSS,
  ZMARSHALLINGERROR,
  ZUNIMPLEMENTED,
  ZOPERATIONTIMEOUT,
  ZBADARGUMENTS,
  ZINVALIDSTATE,
  ZAPIERROR,
  ZNONODE,
  ZNOAUTH,
  ZBADVERSION,
  ZNOCHILDRENFOREPHEMERALS,
  ZNODEEXISTS,
  ZNOTEMPTY,
  ZSESSIONEXPIRED,
  ZINVALIDCALLBACK,
  ZINVALIDACL,
  ZAUTHFAILED,
  ZCLOSING,
  ZNOTHING,
  ZSESSIONMOVED
};

static const ZooLogLevel ZOO_LOG_LEVEL_TABLE[] = {
  ZOO_LOG_LEVEL_ERROR,
  ZOO_LOG_LEVEL_WARN,
  ZOO_LOG_LEVEL_INFO,
  ZOO_LOG_LEVEL_DEBUG
};

static const ZOO_EVENT_AUX ZOO_EVENT_TABLE[] = {
  ZOO_CREATED_EVENT_AUX,
  ZOO_DELETED_EVENT_AUX,
  ZOO_CHANGED_EVENT_AUX,
  ZOO_CHILD_EVENT_AUX,
  ZOO_SESSION_EVENT_AUX,
  ZOO_NOTWATCHING_EVENT_AUX
};

static const ZOO_STATE_AUX ZOO_STATE_TABLE[] = {
  ZOO_EXPIRED_SESSION_STATE_AUX,
  ZOO_AUTH_FAILED_STATE_AUX,
  ZOO_CONNECTING_STATE_AUX,
  ZOO_ASSOCIATING_STATE_AUX,
  ZOO_CONNECTED_STATE_AUX
};

static const ZOO_PERM_AUX ZOO_PERM_TABLE[] = {
  ZOO_PERM_READ_AUX,
  ZOO_PERM_WRITE_AUX,
  ZOO_PERM_CREATE_AUX,
  ZOO_PERM_DELETE_AUX,
  ZOO_PERM_ADMIN_AUX,
  ZOO_PERM_ALL_AUX
};

static const ZOO_CREATE_FLAG_AUX ZOO_CREATE_FLAG_TABLE[] = {
    ZOO_EPHEMERAL_AUX,
    ZOO_SEQUENCE_AUX
};

static void
zkocaml_handle_struct_finalize(value ve)
{
}

static int
zkocaml_handle_struct_compare(value v1, value v2)
{
  zkocaml_handle_t *h1 = zkocaml_handle_struct_val(v1);
  zkocaml_handle_t *h2 = zkocaml_handle_struct_val(v2);
  if (h1 == h2) return 0;
  else if (h1 < h2) return -1;
  return 1;
}

static long
zkocaml_handle_struct_hash(value v)
{
  return (long) zkocaml_handle_struct_val(v);
}

static struct custom_operations zhandle_struct_ops = {
  "org.apache.zookeeper",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default
};

static value
zkocaml_copy_zhandle(zhandle_t *zh)
{
  CAMLparam0();
  CAMLlocal1(handle);

  zkocaml_handle_t *zhandle = (zkocaml_handle_t *) malloc(sizeof(zkocaml_handle_t));
  zhandle->handle = zh;

  handle = caml_alloc_custom(&zhandle_struct_ops, sizeof(zkocaml_handle_t *), 0, 1);
  zkocaml_handle_struct_val(handle) = zhandle;

  CAMLreturn(handle);
}

static enum ZOO_ERRORS
zkocaml_enum_error_ml2c(value v)
{
  return ZOO_ERRORS_TABLE[Long_val(v)];
}

static value
zkocaml_enum_error_c2ml(enum ZOO_ERRORS error)
{
  int i = 0, index = 0;
  for (; i < zkocaml_table_len(ZOO_ERRORS_TABLE); i++) {
    if (error == ZOO_ERRORS_TABLE[i]) {
      index = i;
      break;
    }
  }
  return Val_int(index);
}

static ZooLogLevel
zkocaml_enum_loglevel_ml2c(value v)
{
  return ZOO_LOG_LEVEL_TABLE[Long_val(v)];
}

static value
zkocaml_enum_loglevel_c2ml(ZooLogLevel log_level)
{
  int i = 0, index = 0;
  for (; i < zkocaml_table_len(ZOO_LOG_LEVEL_TABLE); i++) {
    if (log_level == ZOO_LOG_LEVEL_TABLE[i]) {
      index = i;
      break;
    }
  }
  return Val_int(index);
}

static int
zkocaml_enum_event_ml2c(value v)
{
  int event = -1;
  ZOO_EVENT_AUX event_aux = Int_val(v);

  switch(event_aux) {
  case ZOO_CREATED_EVENT_AUX:
    event = ZOO_CREATED_EVENT;
    break;
  case ZOO_DELETED_EVENT_AUX:
    event = ZOO_DELETED_EVENT;
    break;
  case ZOO_CHANGED_EVENT_AUX:
    event = ZOO_CHANGED_EVENT;
    break;
  case ZOO_CHILD_EVENT_AUX:
    event = ZOO_CHILD_EVENT;
    break;
  case ZOO_SESSION_EVENT_AUX:
    event = ZOO_SESSION_EVENT;
    break;
  case ZOO_NOTWATCHING_EVENT_AUX:
    event = ZOO_NOTWATCHING_EVENT;
    break;
  }

  return event;
}

static value
zkocaml_enum_event_c2ml(int event)
{
  int i = 0, index = 0;
  ZOO_EVENT_AUX event_aux = -1;

  if (event == ZOO_CREATED_EVENT) {
    event_aux = ZOO_CREATED_EVENT_AUX;
  } else if (event == ZOO_DELETED_EVENT) {
    event_aux = ZOO_DELETED_EVENT_AUX;
  } else if (event == ZOO_CHANGED_EVENT) {
    event_aux = ZOO_CHANGED_EVENT_AUX;
  } else if (event == ZOO_CHILD_EVENT) {
    event_aux = ZOO_CHILD_EVENT_AUX;
  } else if (event == ZOO_SESSION_EVENT) {
    event_aux = ZOO_SESSION_EVENT_AUX;
  } else if (event == ZOO_NOTWATCHING_EVENT) {
    event_aux = ZOO_NOTWATCHING_EVENT_AUX;
  }

  for (; i < zkocaml_table_len(ZOO_EVENT_TABLE); i++) {
    if (event_aux == ZOO_EVENT_TABLE[i]) {
      index = i;
      break;
    }
  }
  return Val_int(index);
}

static int
zkocaml_enum_state_ml2c(value v)
{
  int state = -1;
  ZOO_STATE_AUX state_aux = Int_val(v);

  switch(state_aux) {
  case ZOO_EXPIRED_SESSION_STATE_AUX:
    state = ZOO_EXPIRED_SESSION_STATE;
    break;
  case ZOO_AUTH_FAILED_STATE_AUX:
    state = ZOO_AUTH_FAILED_STATE;
    break;
  case ZOO_CONNECTING_STATE_AUX:
    state = ZOO_CONNECTING_STATE;
    break;
  case ZOO_ASSOCIATING_STATE_AUX:
    state = ZOO_ASSOCIATING_STATE;
    break;
  case ZOO_CONNECTED_STATE_AUX:
    state = ZOO_CONNECTED_STATE;
    break;
  }

  return state;
}

static value
zkocaml_enum_state_c2ml(int state)
{
  int i = 0, index = 0;
  ZOO_STATE_AUX state_aux = ZOO_EXPIRED_SESSION_STATE_AUX;

  if (state == ZOO_EXPIRED_SESSION_STATE) {
    state_aux = ZOO_EXPIRED_SESSION_STATE_AUX;
  } else if (state == ZOO_AUTH_FAILED_STATE) {
    state_aux = ZOO_AUTH_FAILED_STATE_AUX;
  } else if (state == ZOO_CONNECTING_STATE) {
    state_aux = ZOO_CONNECTING_STATE_AUX;
  } else if (state == ZOO_ASSOCIATING_STATE) {
    state_aux = ZOO_ASSOCIATING_STATE_AUX;
  } else if (state == ZOO_CONNECTED_STATE) {
    state_aux = ZOO_CONNECTED_STATE_AUX;
  }

  for (; i < zkocaml_table_len(ZOO_STATE_TABLE); i++) {
    if (state_aux == ZOO_STATE_TABLE[i]) {
      index = i;
      break;
    }
  }
  return Val_int(index);
}

static int
zkocaml_enum_perm_ml2c(value v)
{
  int perm = -1;
  ZOO_PERM_AUX perm_aux = Int_val(v);

  switch(perm_aux) {
  case ZOO_PERM_READ_AUX:
    perm = ZOO_PERM_READ;
    break;
  case ZOO_PERM_WRITE_AUX:
    perm = ZOO_PERM_WRITE;
    break;
  case ZOO_PERM_CREATE_AUX:
    perm = ZOO_PERM_CREATE;
    break;
  case ZOO_PERM_DELETE_AUX:
    perm = ZOO_PERM_DELETE;
    break;
  case ZOO_PERM_ADMIN_AUX:
    perm = ZOO_PERM_ADMIN;
    break;
  case ZOO_PERM_ALL_AUX:
    perm = ZOO_PERM_ALL;
    break;
  }

  return perm;
}

static value
zkocaml_enum_perm_c2ml(int perm)
{
  int i = 0, index = 0;
  ZOO_PERM_AUX perm_aux;

  if (perm == ZOO_PERM_READ) {
    perm_aux = ZOO_PERM_READ_AUX;
  } else if (perm == ZOO_PERM_WRITE) {
    perm_aux = ZOO_PERM_WRITE_AUX;
  } else if (perm == ZOO_PERM_CREATE) {
    perm_aux = ZOO_PERM_CREATE_AUX;
  } else if (perm == ZOO_PERM_DELETE) {
    perm_aux = ZOO_PERM_DELETE_AUX;
  } else if (perm == ZOO_PERM_ADMIN) {
    perm_aux = ZOO_PERM_ADMIN_AUX;
  } else if (perm == ZOO_PERM_ALL) {
    perm_aux = ZOO_PERM_ALL_AUX;
  }

  for (; i < zkocaml_table_len(ZOO_PERM_TABLE); i++) {
    if (perm_aux == ZOO_PERM_TABLE[i]) {
      index = i;
      break;
    }
  }
  return Val_int(index);
}

static int
zkocaml_enum_create_flag_ml2c(value v)
{
  int create_flag = 0;
  int len = Wosize_val(v);
  for (int i = 0; i < len; i++){
      ZOO_CREATE_FLAG_AUX create_flag_aux = Int_val(Field(v, i));
      switch(create_flag_aux) {
      case ZOO_EPHEMERAL_AUX:
          create_flag |= ZOO_EPHEMERAL;
          break;
      case ZOO_SEQUENCE_AUX:
          create_flag |= ZOO_SEQUENCE;
          break;
      }
  }
  return create_flag;
}

static value
zkocaml_enum_create_flag_c2ml(int create_flag)
{
  int i = 0, index = 0;
  ZOO_CREATE_FLAG_AUX create_flag_aux;

  if (create_flag == ZOO_EPHEMERAL) {
    create_flag_aux = ZOO_EPHEMERAL_AUX;
  } else if (create_flag == ZOO_SEQUENCE) {
    create_flag_aux = ZOO_SEQUENCE_AUX;
  }

  for (; i < zkocaml_table_len(ZOO_CREATE_FLAG_TABLE); i++) {
    if (create_flag_aux == ZOO_CREATE_FLAG_TABLE[i]) {
      index = i;
      break;
    }
  }
  return Val_int(index);
}

static clientid_t *
zkocaml_parse_clientid(value v)
{
  /**
   * client id structure.
   *
   * This structure holds the id and password for the session.
   * This structure should be treated as opaque. It is received
   * from the server when a session is established and needs to be
   * sent back as-is when reconnecting a session.
   *
   * typedef struct {
   *   int64_t client_id;
   *   char passwd[16];
   * } clientid_t;
   *
   * in OCaml, the client_id type is declared as follows:
   *
   * type client_id = {client_id: int64; passwd: string}
   *
   */
  clientid_t *cid = (clientid_t *) malloc(sizeof(clientid_t));
  memset(cid->passwd, 0, 16);

  cid->client_id = Int64_val(Field(v, 0));
  const char *passwd = String_val(Field(v, 1));
  size_t passwd_len = caml_string_length(Field(v, 1));
  if (cid->client_id == 0 && passwd_len == 0) {
      return NULL;
  } else {
      memcpy(cid->passwd, passwd, (passwd_len < 15) ? passwd_len : 15);
  }

  return cid;
}

static int
zkocaml_parse_acls(value v, struct ACL_vector *acls)
{
  int i = 0, vlen = Wosize_val(v);
  if (vlen == 0) return 0;

  acls->count = vlen;
  acls->data = (struct ACL *)calloc(acls->count, sizeof(struct ACL));
  for (; i < vlen; i++) {
      /* acl = Field(v, i); */
      acls->data[i].perms = Field(Field(v, i), 0);
      acls->data[i].id.scheme = strdup(String_val(Field(Field(v, i), 1)));
      acls->data[i].id.id = strdup(String_val(Field(Field(v, i), 2)));
  }

  return 1;
}

static value
zkocaml_build_client_id_struct(const clientid_t *cid)
{
  CAMLparam0();
  CAMLlocal1(v);

  v = caml_alloc(2, 0);
  Store_field(v, 0, caml_copy_int64(cid->client_id));
  Store_field(v, 1, caml_copy_string(cid->passwd));

  CAMLreturn(v);
}


static value
zkocaml_build_stat_struct(const struct Stat *stat)
{
  CAMLparam0();
  CAMLlocal1(v);

  v = caml_alloc(11, 0);
  Store_field(v,  0, caml_copy_int64(stat->czxid));
  Store_field(v,  1, caml_copy_int64(stat->mzxid));
  Store_field(v,  2, caml_copy_int64(stat->ctime));
  Store_field(v,  3, caml_copy_int64(stat->mtime));
  Store_field(v,  4, Val_int(stat->version));
  Store_field(v,  5, Val_int(stat->cversion));
  Store_field(v,  6, Val_int(stat->aversion));
  Store_field(v,  7, caml_copy_int64(stat->ephemeralOwner));
  Store_field(v,  8, Val_int(stat->dataLength));
  Store_field(v,  9, Val_int(stat->numChildren));
  Store_field(v, 10, Val_int(stat->pzxid));

  CAMLreturn(v);
}

static value
zkocaml_build_strings_struct(const struct String_vector *strings)
{
  CAMLparam0();
  CAMLlocal1(v);

  int i = 0;
  v = caml_alloc(strings->count, 0);
  for (; i < strings->count; i++) {
    Store_field(v, i, caml_copy_string(strings->data[i]));
  }

  CAMLreturn(v);
}

static value
zkocaml_build_acls_struct(const struct ACL_vector *acls)
{
  CAMLparam0();
  CAMLlocal2(v, acl);

  int i = 0;
  v = caml_alloc(acls->count, 0);
  for (; i < acls->count; i++) {
    acl = caml_alloc(3, 0);
    Store_field(acl, 0, acls->data[i].perms);
    Store_field(acl, 1, caml_copy_string(acls->data[i].id.scheme));
    Store_field(acl, 2, caml_copy_string(acls->data[i].id.id));

    Store_field(v, i, acl);
  }

  CAMLreturn(v);
}

static void
watcher_dispatch(zhandle_t *zh,
                 int type,
                 int state,
                 const char *path,
                 void *watcher_ctx)
{
  zkocaml_enter_callback();
  CAMLparam0();

  CAMLlocal5(local_zh, local_type, local_state, local_path, local_watcher_ctx);
  CAMLlocalN(args, 5);

  zkocaml_watcher_context_t *ctx = (zkocaml_watcher_context_t* )(watcher_ctx);
  local_zh = zkocaml_copy_zhandle(zh);
  local_type = zkocaml_enum_event_c2ml(type);
  local_state = zkocaml_enum_state_c2ml(state);
  local_path = caml_copy_string(path);
  local_watcher_ctx = caml_copy_string(ctx->watcher_ctx);

  Store_field(args, 0, local_zh);
  Store_field(args, 1, local_type);
  Store_field(args, 2, local_state);
  Store_field(args, 3, local_path);
  Store_field(args, 4, local_watcher_ctx);

  callbackN(ctx->watcher_callback, 5, args);

  if (!ctx->global) caml_remove_generational_global_root(&(ctx->watcher_callback));

  zkocaml_leave_callback();
  CAMLreturn0;
}

/**
 * The completion callbacks (from asynchronous calls) are
 * implemented similarly.
 */

/**
 * Called when an asynchronous call that returns void completes and
 * dispatches user provided callback
 */
static void
void_completion_dispatch(int rc, const void *data)
{
  zkocaml_enter_callback();
  CAMLparam0();

  CAMLlocal1(completion_callback);
  CAMLlocal2(local_rc, local_data);

  zkocaml_completion_context_t *ctx = (zkocaml_completion_context_t *)data;
  completion_callback = ctx->completion_callback;
  local_rc = zkocaml_enum_error_c2ml(rc);
  local_data = caml_copy_string(ctx->data);

  callback2(completion_callback, local_rc, local_data);

  caml_remove_generational_global_root(&(ctx->completion_callback));

  zkocaml_leave_callback();
  CAMLreturn0;
}

/**
 * Called when an asynchronous call that returns a stat structure
 * completes and dispatches user provided callback
 */
static void
stat_completion_dispatch(int rc,
                         const struct Stat *stat,
                         const void *data)
{
  zkocaml_enter_callback();
  CAMLparam0();

  CAMLlocal1(completion_callback);
  CAMLlocal3(local_rc, local_stat, local_data);

  zkocaml_completion_context_t *ctx = (zkocaml_completion_context_t *)data;
  completion_callback = ctx->completion_callback;
  local_rc = zkocaml_enum_error_c2ml(rc);
  local_stat = zkocaml_build_stat_struct(stat);
  local_data = caml_copy_string(ctx->data);

  callback3(completion_callback, local_rc, local_stat, local_data);

  caml_remove_generational_global_root(&(ctx->completion_callback));

  zkocaml_leave_callback();
  CAMLreturn0;
}
/**
 * Called when an asynchronous call that returns a stat structure and
 * some untyped data completes and dispatches user provided
 * callback (used by aget)
 */
static void
data_completion_dispatch(int rc,
                         const char *val,
                         int val_len,
                         const struct Stat *stat,
                         const void *data)
{
  zkocaml_enter_callback();
  CAMLparam0();

  CAMLlocal1(completion_callback);
  CAMLlocal5(local_rc, local_val, local_val_len, local_stat, local_data);
  CAMLlocalN(args, 5);

  zkocaml_completion_context_t *ctx = (zkocaml_completion_context_t *)data;
  completion_callback = ctx->completion_callback;
  local_rc = zkocaml_enum_error_c2ml(rc);
  local_val = caml_copy_string(val);
  local_val_len = Val_int(val_len);
  local_stat = zkocaml_build_stat_struct(stat);
  local_data = caml_copy_string(ctx->data);

  Store_field(args, 0, local_rc);
  Store_field(args, 1, local_val);
  Store_field(args, 2, local_val_len);
  Store_field(args, 3, local_stat);
  Store_field(args, 4, local_data);

  callbackN(completion_callback, 5, args);

  caml_remove_generational_global_root(&(ctx->completion_callback));

  zkocaml_leave_callback();
  CAMLreturn0;
}

/**
 * Called when an asynchronous call that returns a list of strings
 * completes and dispatches user provided callback.
 */
static void
strings_completion_dispatch(int rc,
                            const struct String_vector *strings,
                            const void *data)
{
  zkocaml_enter_callback();
  CAMLparam0();

  CAMLlocal1(completion_callback);
  CAMLlocal3(local_rc, local_strings, local_data);

  zkocaml_completion_context_t *ctx = (zkocaml_completion_context_t *)data;
  completion_callback = ctx->completion_callback;
  local_rc = zkocaml_enum_error_c2ml(rc);
  local_strings = zkocaml_build_strings_struct(strings);
  local_data = caml_copy_string(ctx->data);

  callback3(completion_callback, local_rc, local_strings, local_data);

  caml_remove_generational_global_root(&(ctx->completion_callback));

  zkocaml_leave_callback();
  CAMLreturn0;
}

/**
 * Called when an asynchronous call that returns a list of strings
 * and a stat structure completes and dispatches user provided callback.
 */
static void
strings_stat_completion_dispatch(int rc,
                                 const struct String_vector *strings,
                                 const struct Stat *stat,
                                 const void *data)
{
  zkocaml_enter_callback();
  CAMLparam0();

  CAMLlocal1(completion_callback);
  CAMLlocal4(local_rc, local_strings, local_stat, local_data);
  CAMLlocalN(args, 4);

  zkocaml_completion_context_t *ctx = (zkocaml_completion_context_t *)data;
  completion_callback = ctx->completion_callback;
  local_rc = zkocaml_enum_error_c2ml(rc);
  local_strings = zkocaml_build_strings_struct(strings);
  local_stat = zkocaml_build_stat_struct(stat);
  local_data = caml_copy_string(ctx->data);

  Store_field(args, 0, local_rc);
  Store_field(args, 1, local_strings);
  Store_field(args, 2, local_stat);
  Store_field(args, 3, local_data);

  callbackN(completion_callback, 4, args);

  caml_remove_generational_global_root(&(ctx->completion_callback));

  zkocaml_leave_callback();
  CAMLreturn0;
}

/**
 * Called when an asynchronous call that returns a single string
 * completes and dispatches user provided callback.
 */
static void
string_completion_dispatch(int rc,
                           const char *val,
                           const void *data)
{
  zkocaml_enter_callback();
  CAMLparam0();

  CAMLlocal1(completion_callback);
  CAMLlocal3(local_rc, local_val, local_data);

  zkocaml_completion_context_t *ctx = (zkocaml_completion_context_t *)data;
  completion_callback = ctx->completion_callback;
  local_rc = zkocaml_enum_error_c2ml(rc);
  if (val != NULL)
      local_val = caml_copy_string(val);
  else
      local_val = caml_alloc_string(0);
  local_data = caml_copy_string(ctx->data);

  callback3(completion_callback, local_rc, local_val, local_data);

  caml_remove_generational_global_root(&(ctx->completion_callback));

  zkocaml_leave_callback();
  CAMLreturn0;
}

/**
 * Called when an asynchronous call that returns a list of ACLs
 * completes and dispatches user provided callback.
 */
static void
acl_completion_dispatch(int rc,
                        struct ACL_vector *acl,
                        struct Stat *stat,
                        const void *data)
{
  zkocaml_enter_callback();
  CAMLparam0();

  CAMLlocal1(completion_callback);
  CAMLlocal4(local_rc, local_acl, local_stat, local_data);
  CAMLlocalN(args, 4);

  zkocaml_completion_context_t *ctx = (zkocaml_completion_context_t *)data;
  completion_callback = ctx->completion_callback;
  local_rc = zkocaml_enum_error_c2ml(rc);
  local_acl = zkocaml_build_acls_struct(acl);
  local_stat = zkocaml_build_stat_struct(stat);
  local_data = caml_copy_string(ctx->data);

  Store_field(args, 0, local_rc);
  Store_field(args, 1, local_acl);
  Store_field(args, 2, local_stat);
  Store_field(args, 3, local_data);

  callbackN(completion_callback, 4, args);

  caml_remove_generational_global_root(&(ctx->completion_callback));

  zkocaml_leave_callback();
  CAMLreturn0;
}

/**
 * Create a handle to used communicate with zookeeper.
 *
 * This method creates a new handle and a zookeeper session that corresponds
 * to that handle. Session establishment is asynchronous, meaning that the
 * session should not be considered established until (and unless) an
 * event of state ZOO_CONNECTED_STATE is received.
 *
 * @host Comma separated host:port pairs, each corresponding to a zk
 *   server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
 *
 * @watcher_callback The global watcher callback function. When notifications are
 *   triggered this function will be invoked.
 *
 * @clientid The id of a previously established session that this
 *   client will be reconnecting to. Pass 0 if not reconnecting to a previous
 *   session. Clients can access the session id of an established, valid,
 *   connection by calling zoo_client_id. If the session corresponding to
 *   the specified clientid has expired, or if the clientid is invalid for
 *   any reason, the returned zhandle_t will be invalid -- the zhandle_t
 *   state will indicate the reason for failure (typically
 *   ZOO_EXPIRED_SESSION_STATE).
 *
 * @context The handback object that will be associated with this instance
 *   of zhandle_t. Application can access it (for example, in the watcher
 *   callback) using zoo_get_context. The object is not used by zookeeper
 *   internally and can be null.
 *
 * @flags Reserved for future use. Should be set to zero.
 *
 * @return A pointer to the opaque zhandle structure. If it fails to create
 * a new zhandle the function returns NULL and the errno variable indicates
 * the reason.
 */

CAMLprim value
zkocaml_init_native(value host,
                    value watcher_callback,
                    value recv_timeout,
                    value clientid,
                    value context,
                    value flag)
{
  CAMLparam5(host, watcher_callback, recv_timeout, clientid, context);
  CAMLxparam1(flag);
  CAMLlocal1(zh);

  const char *local_host = String_val(host);
  int local_recv_timeout = Long_val(recv_timeout);
  clientid_t *cid = NULL;

  zkocaml_watcher_context_t *ctx = (zkocaml_watcher_context_t *)
      malloc(sizeof(zkocaml_watcher_context_t));
  ctx->watcher_ctx = String_val(context);
  ctx->watcher_callback = watcher_callback;
  ctx->global = 1;

  caml_register_generational_global_root(&(ctx->watcher_callback));

  cid = zkocaml_parse_clientid(clientid);
  zhandle_t *handle = zookeeper_init(local_host, watcher_dispatch, local_recv_timeout, cid, ctx, 0);

  zh = zkocaml_copy_zhandle(handle);
  CAMLreturn(zh);
}

CAMLprim value
zkocaml_init_bytecode(value *argv, int argn)
{
  return zkocaml_init_native(argv[0], argv[1], argv[2],
                             argv[3], argv[4], argv[5]);
}

/**
 * Close the zookeeper handle and free up any resources.
 *
 * After this call, the client session will no longer be valid. The function
 * will flush any outstanding send requests before return. As a result it may
 * block.
 *
 * This method should only be called only once on a zookeeper handle. Calling
 * twice will cause undefined (and probably undesirable behavior). Calling any other
 * zookeeper method after calling close is undefined behaviour and should be avoided.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @return a result code. Regardless of the error code returned, the zhandle
 * will be destroyed and all resources freed.
 *   ZOK - success
 *   ZBADARGUMENTS - invalid input parameters
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 *   ZOPERATIONTIMEOUT - failed to flush the buffers within the specified timeout.
 *   ZCONNECTIONLOSS - a network error occured while attempting to send request to server
 *   ZSYSTEMERROR -- a system (OS) error occured; it's worth checking errno to get details
 */

CAMLprim value
zkocaml_close(value zh)
{
  CAMLparam1(zh);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = NULL;
  zhandle = zkocaml_handle_struct_val(zh);

  zkocaml_watcher_context_t *ctx = (zkocaml_watcher_context_t*) zoo_get_context(zhandle->handle);
  caml_remove_generational_global_root(&(ctx->watcher_callback));

  int rc = zookeeper_close(zhandle->handle);
  if (zkocaml_log_stream != NULL) fclose(zkocaml_log_stream);
  result = Val_int(rc);

  CAMLreturn(result);
}

/**
 * Return the client session id, only valid if the connections
 * is currently connected (ie. last watcher state is ZOO_CONNECTED_STATE)
 */
CAMLprim value
zkocaml_client_id(value zh)
{
  CAMLparam1(zh);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = NULL;
  zhandle = zkocaml_handle_struct_val(zh);
  const clientid_t *cid = zoo_client_id(zhandle->handle);
  result = zkocaml_build_client_id_struct(cid);

  CAMLreturn(result);
}

/**
 * Return the timeout for this session, only valid if the connections
 * is currently connected (ie. last watcher state is ZOO_CONNECTED_STATE). This
 * value may change after a server re-connect.
 */
CAMLprim value
zkocaml_recv_timeout(value zh)
{
  CAMLparam1(zh);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = NULL;
  zhandle = zkocaml_handle_struct_val(zh);
  int recv_timeout = zoo_recv_timeout(zhandle->handle);
  result = Val_int(recv_timeout);

  CAMLreturn(result);
}

/**
 * Return the context for this handle.
 */
CAMLprim value
zkocaml_get_context(value zh)
{
  CAMLparam1(zh);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = NULL;
  zhandle = zkocaml_handle_struct_val(zh);
  zkocaml_watcher_context_t *ctx = (zkocaml_watcher_context_t*) zoo_get_context(zhandle->handle);
  result = caml_copy_string((char*)ctx->watcher_ctx);

  CAMLreturn(result);
}

/**
 * Set the context for this handle.
 */
CAMLprim value
zkocaml_set_context(value zh, value context)
{
  CAMLparam2(zh, context);

  zkocaml_handle_t *zhandle = NULL;
  zhandle = zkocaml_handle_struct_val(zh);
  char *local_context = String_val(context);
  zoo_set_context(zhandle->handle, local_context);

  CAMLreturn(Val_unit);
}

/**
 * Set a watcher function
 * @return previous watcher function
 */
CAMLprim value
zkocaml_set_watcher(value zh, value watcher_callback)
{
  CAMLparam2(zh, watcher_callback);
  int not_implemented;
  // TODO: implement this later.
  CAMLreturn(Val_unit);
}

/**
 * Returns the socket address for the current connection
 *
 * @return socket address of the connected host or NULL on failure, only valid if the
 * connection is current connected
 */
CAMLprim value
zkocaml_get_connected_host(value zh)
{
  CAMLparam1(zh);
  int not_implemented;
  // TODO: implement this later.
  CAMLreturn(Val_unit);
}

/**
 * Get the state of the zookeeper connection.
 *
 * The return value will be one of the state consts.
 */
CAMLprim value
zkocaml_state(value zh)
{
  CAMLparam1(zh);
  CAMLlocal1(state);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  state = zoo_state(zhandle->handle);

  CAMLreturn(state);
}

/**
 * Create a node.
 *
 * This method will create a node in ZooKeeper. A node can only be created if
 * it does not already exists. The Create Flags affect the creation of nodes.
 * If ZOO_EPHEMERAL flag is set, the node will automatically get removed if the
 * client session goes away. If the ZOO_SEQUENCE flag is set, a unique
 * monotonically increasing sequence number is appended to the path name. The
 * sequence number is always fixed length of 10 digits, 0 padded.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path The name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @value The data to be stored in the node.
 *
 * @acl The initial ACL of the node. The ACL must not be null or empty.
 *
 * @flags this parameter can be set to 0 for normal create or an OR
 * of the Create Flags
 *
 * @completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 *   ZOK operation completed successfully
 *   ZNONODE the parent node does not exist.
 *   ZNODEEXISTS the node already exists
 *   ZNOAUTH the client does not have permission.
 *   ZNOCHILDRENFOREPHEMERALS cannot create children of ephemeral nodes.
 *
 * @data The data that will be passed to the completion routine when the
 * function completes.
 *
 * @return ZOK on success or one of the following errcodes on failure:
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_acreate_native(value zh,
                       value path,
                       value val,
                       value acl,
                       value flags,
                       value completion,
                       value data)
{
  CAMLparam5(zh, path, val, acl, flags);
  CAMLxparam2(completion, data);
  CAMLlocal1(result);

  struct ACL_vector local_acl;
  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  int r = zkocaml_parse_acls(acl, &local_acl);
  if (r == 0) {
    local_acl = ZOO_OPEN_ACL_UNSAFE;
  }
  int local_flags = zkocaml_enum_create_flag_ml2c(flags);
  zkocaml_completion_context_t *local_data =
    (zkocaml_completion_context_t *)malloc(sizeof(zkocaml_completion_context_t));
  local_data->data = strdup(String_val(data));
  local_data->completion_callback = completion;

  caml_register_generational_global_root(&(local_data->completion_callback));

  int rc = zoo_acreate(zhandle->handle,
                       String_val(path),
                       String_val(val),
                       caml_string_length(val),
                       (const struct ACL_vector *)&local_acl,
                       local_flags,
                       string_completion_dispatch,
                       local_data);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

CAMLprim value
zkocaml_acreate_bytecode(value *argv, int argn)
{
  return zkocaml_acreate_native(argv[0], argv[1], argv[2], argv[3],
                                argv[4], argv[5], argv[6]);
}

/**
 * Delete a node in zookeeper.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @version the expected version of the node. The function will fail if the
 * actual version of the node does not match the expected version.
 * If -1 is used the version check will not take place.
 *
 * @completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *   ZBADVERSION expected version does not match actual version.
 *   ZNOTEMPTY children are present; node cannot be deleted.
 *
 * @data the data that will be passed to the completion routine when
 * the function completes.
 *
 * @return ZOK on success or one of the following errcodes on failure:
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_adelete(value zh,
                value path,
                value version,
                value completion,
                value data)
{
  CAMLparam5(zh, path, version, completion, data);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  int local_version = Int_val(version);
  zkocaml_completion_context_t *local_data =
    (zkocaml_completion_context_t *)malloc(sizeof(zkocaml_completion_context_t));
  local_data->data = strdup(String_val(data));
  local_data->completion_callback = completion;

  caml_register_generational_global_root(&(local_data->completion_callback));

  int rc = zoo_adelete(zhandle->handle,
                       local_path,
                       local_version,
                       void_completion_dispatch,
                       local_data);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

/**
 * Checks the existence of a node in zookeeper.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @watch if nonzero, a watch will be set at the server to notify the
 * client if the node changes. The watch will be set even if the node does not
 * exist. This allows clients to watch for nodes to appear.
 *
 * @completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *
 * @data the data that will be passed to the completion routine when the
 * function completes.
 *
 * @return ZOK on success or one of the following errcodes on failure:
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_aexists(value zh,
                value path,
                value watch,
                value completion,
                value data)
{
  CAMLparam5(zh, path, watch, completion, data);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  int local_watch = Int_val(watch);
  zkocaml_completion_context_t *local_data =
    (zkocaml_completion_context_t *)malloc(sizeof(zkocaml_completion_context_t));
  local_data->data = strdup(String_val(data));
  local_data->completion_callback = completion;

  caml_register_generational_global_root(&(local_data->completion_callback));

  int rc = zoo_aexists(zhandle->handle,
                       local_path,
                       local_watch,
                       stat_completion_dispatch,
                       local_data);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

/**
 * Checks the existence of a node in zookeeper.
 *
 * This function is similar to zoo_axists except it allows one specify
 * a watcher object - a function pointer and associated context. The function
 * will be called once the watch has fired. The associated context data will be
 * passed to the function as the watcher context parameter.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @watcher if non-null a watch will set on the specified znode on the server.
 * The watch will be set even if the node does not exist. This allows clients
 * to watch for nodes to appear.
 *
 * @watcher_ctx user specific data, will be passed to the watcher callback.
 * Unlike the global context set by zookeeper_init, this watcher context
 * is associated with the given instance of the watcher only.
 *
 * @completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *
 * @data the data that will be passed to the completion routine when the
 * function completes.
 *
 * @return ZOK on success or one of the following errcodes on failure:
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_awexists_native(value zh,
                        value path,
                        value watcher_callback,
                        value watcher_ctx,
                        value completion,
                        value data)
{
  CAMLparam5(zh, path, watcher_callback, watcher_ctx, completion);
  CAMLxparam1(data);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  zkocaml_watcher_context_t *local_ctx = (zkocaml_watcher_context_t *)
      malloc(sizeof(zkocaml_watcher_context_t));
  local_ctx->watcher_ctx = String_val(watcher_ctx);
  local_ctx->watcher_callback = watcher_callback;
  local_ctx->global = 0;
  zkocaml_completion_context_t *local_data =
    (zkocaml_completion_context_t *)malloc(sizeof(zkocaml_completion_context_t));
  local_data->data = strdup(String_val(data));
  local_data->completion_callback = completion;

  caml_register_generational_global_root(&(local_ctx->watcher_callback));
  caml_register_generational_global_root(&(local_data->completion_callback));

  int rc = zoo_awexists(zhandle->handle,
                        local_path,
                        watcher_dispatch,
                        local_ctx,
                        stat_completion_dispatch,
                        local_data);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

CAMLprim value
zkocaml_awexists_bytecode(value *argv, int argn)
{
  return zkocaml_awexists_native(argv[0], argv[1], argv[2],
                                 argv[3], argv[4], argv[5]);
}

/**
 * Gets the data associated with a node.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @watch if nonzero, a watch will be set at the server to notify
 * the client if the node changes.
 *
 * @completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *
 * @data the data that will be passed to the completion routine when
 * the function completes.
 *
 * @return ZOK on success or one of the following errcodes on failure:
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either in ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_aget(value zh,
             value path,
             value watch,
             value completion,
             value data)
{
  CAMLparam5(zh, path, watch, completion, data);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  int local_watch = Int_val(watch);
  zkocaml_completion_context_t *local_data =
    (zkocaml_completion_context_t *)malloc(sizeof(zkocaml_completion_context_t));
  local_data->data = strdup(String_val(data));
  local_data->completion_callback = completion;

  caml_register_generational_global_root(&(local_data->completion_callback));

  int rc = zoo_aget(zhandle->handle,
                    local_path,
                    local_watch,
                    data_completion_dispatch,
                    local_data);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

/**
 * Gets the data associated with a node.
 *
 * This function is similar to zoo_aget except it allows one specify
 * a watcher object rather than a boolean watch flag.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @watcher if non-null, a watch will be set at the server to notify
 * the client if the node changes.
 *
 * @watcher_ctx user specific data, will be passed to the watcher callback.
 * Unlike the global context set by zookeeper_init, this watcher context
 * is associated with the given instance of the watcher only.
 *
 * @completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *
 * @data the data that will be passed to the completion routine when
 * the function completes.
 *
 * @return ZOK on success or one of the following errcodes on failure:
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either in ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_awget_native(value zh,
                     value path,
                     value watcher_callback,
                     value watcher_ctx,
                     value completion,
                     value data)
{
  CAMLparam5(zh, path, watcher_callback, watcher_ctx, completion);
  CAMLxparam1(data);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  zkocaml_watcher_context_t *local_ctx = (zkocaml_watcher_context_t *)
      malloc(sizeof(zkocaml_watcher_context_t));
  local_ctx->watcher_ctx = String_val(watcher_ctx);
  local_ctx->watcher_callback = watcher_callback;
  local_ctx->global = 0;
  zkocaml_completion_context_t *local_data =
    (zkocaml_completion_context_t *)malloc(sizeof(zkocaml_completion_context_t));
  local_data->data = strdup(String_val(data));
  local_data->completion_callback = completion;

  caml_register_generational_global_root(&(local_ctx->watcher_callback));
  caml_register_generational_global_root(&(local_data->completion_callback));

  int rc = zoo_awget(zhandle->handle,
                     local_path,
                     watcher_dispatch,
                     local_ctx,
                     data_completion_dispatch,
                     local_data);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

CAMLprim value
zkocaml_awget_bytecode(value *argv, int argn)
{
  return zkocaml_awget_native(argv[0], argv[1], argv[2],
                              argv[3], argv[4], argv[5]);
}

/**
 * Sets the data associated with a node.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @buffer the buffer holding data to be written to the node.
 *
 * @version the expected version of the node. The function will fail if
 * the actual version of the node does not match the expected version. If -1 is
 * used the version check will not take place. * completion: If null,
 * the function will execute synchronously. Otherwise, the function will return
 * immediately and invoke the completion routine when the request completes.
 *
 * @completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *   ZBADVERSION expected version does not match actual version.
 *
 * @data the data that will be passed to the completion routine when
 * the function completes.
 *
 * @return ZOK on success or one of the following errcodes on failure:
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_aset_native(value zh,
                    value path,
                    value buffer,
                    value version,
                    value completion,
                    value data)
{
  CAMLparam5(zh, path, buffer, version, completion);
  CAMLxparam1(data);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  zkocaml_completion_context_t *local_data =
    (zkocaml_completion_context_t *)malloc(sizeof(zkocaml_completion_context_t));
  local_data->data = strdup(String_val(data));
  local_data->completion_callback = completion;

  caml_register_generational_global_root(&(local_data->completion_callback));

  int rc = zoo_aset(zhandle->handle,
                    String_val(path),
                    String_val(buffer),
                    caml_string_length(buffer),
                    Int_val(version),
                    stat_completion_dispatch,
                    local_data);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

CAMLprim value
zkocaml_aset_bytecode(value *argv, int argn)
{
  return zkocaml_aset_native(argv[0], argv[1], argv[2],
                             argv[3], argv[4], argv[5]);
}

/**
 * Lists the children of a node.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @watch if nonzero, a watch will be set at the server to notify
 * the client if the node changes.
 *
 * @completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *
 * @data the data that will be passed to the completion routine when
 * the function completes.
 *
 * @return ZOK on success or one of the following errcodes on failure:
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_aget_children(value zh,
                      value path,
                      value watch,
                      value completion,
                      value data)
{
  CAMLparam5(zh, path, watch, completion, data);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  int local_watch = Int_val(watch);
  zkocaml_completion_context_t *local_data =
    (zkocaml_completion_context_t *)malloc(sizeof(zkocaml_completion_context_t));
  local_data->data = strdup(String_val(data));
  local_data->completion_callback = completion;

  caml_register_generational_global_root(&(local_data->completion_callback));

  int rc = zoo_aget_children(zhandle->handle,
                             local_path,
                             local_watch,
                             strings_completion_dispatch,
                             local_data);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

/**
 * Lists the children of a node.
 *
 * This function is similar to zoo_aget_children except it allows one specify
 * a watcher object rather than a boolean watch flag.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @watcher if non-null, a watch will be set at the server to notify
 * the client if the node changes.
 *
 * @watcher_ctx user specific data, will be passed to the watcher callback.
 * Unlike the global context set by zookeeper_init, this watcher context
 * is associated with the given instance of the watcher only.
 *
 * @completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *
 * @data the data that will be passed to the completion routine when
 * the function completes.
 *
 * @return ZOK on success or one of the following errcodes on failure:
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_awget_children_native(value zh,
                              value path,
                              value watcher_callback,
                              value watcher_ctx,
                              value completion,
                              value data)
{
  CAMLparam5(zh, path, watcher_callback, watcher_ctx, completion);
  CAMLxparam1(data);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  zkocaml_watcher_context_t *local_ctx = (zkocaml_watcher_context_t *)
      malloc(sizeof(zkocaml_watcher_context_t));
  local_ctx->watcher_ctx = String_val(watcher_ctx);
  local_ctx->watcher_callback = watcher_callback;
  local_ctx->global = 0;
  zkocaml_completion_context_t *local_data =
    (zkocaml_completion_context_t *)malloc(sizeof(zkocaml_completion_context_t));
  local_data->data = strdup(String_val(data));
  local_data->completion_callback = completion;

  caml_register_generational_global_root(&(local_ctx->watcher_callback));
  caml_register_generational_global_root(&(local_data->completion_callback));

  int rc = zoo_awget_children(zhandle->handle,
                              local_path,
                              watcher_dispatch,
                              local_ctx,
                              strings_completion_dispatch,
                              local_data);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

CAMLprim value
zkocaml_awget_children_bytecode(value *argv, int argn)
{
  return zkocaml_awget_children_native(argv[0], argv[1], argv[2],
                                       argv[3], argv[4], argv[5]);
}

/**
 * Lists the children of a node, and get the parent stat.
 *
 * This function is new in version 3.3.0
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @watch if nonzero, a watch will be set at the server to notify
 * the client if the node changes.
 *
 * @completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *
 * @data the data that will be passed to the completion routine when
 * the function completes.
 *
 * @return ZOK on success or one of the following errcodes on failure:
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_aget_children2(value zh,
                       value path,
                       value watch,
                       value completion,
                       value data)
{
  CAMLparam5(zh, path, watch, completion, data);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  int local_watch = Int_val(watch);
  zkocaml_completion_context_t *local_data =
    (zkocaml_completion_context_t *)malloc(sizeof(zkocaml_completion_context_t));
  local_data->data = strdup(String_val(data));
  local_data->completion_callback = completion;

  caml_register_generational_global_root(&(local_data->completion_callback));

  int rc = zoo_aget_children2(zhandle->handle,
                              local_path,
                              local_watch,
                              strings_stat_completion_dispatch,
                              local_data);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

/**
 * Lists the children of a node, and get the parent stat.
 *
 * This function is similar to zoo_aget_children2 except it allows one specify
 * a watcher object rather than a boolean watch flag.
 *
 * This function is new in version 3.3.0
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @watcher if non-null, a watch will be set at the server to notify
 * the client if the node changes.
 *
 * @watcher_ctx user specific data, will be passed to the watcher callback.
 * Unlike the global context set by zookeeper_init, this watcher context
 * is associated with the given instance of the watcher only.
 *
 * @completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *
 * @data the data that will be passed to the completion routine when
 * the function completes.
 *
 * @return ZOK on success or one of the following errcodes on failure:
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_awget_children2_native(value zh,
                               value path,
                               value watcher_callback,
                               value watcher_ctx,
                               value completion,
                               value data)
{
  CAMLparam5(zh, path, watcher_callback, watcher_ctx, completion);
  CAMLxparam1(data);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  zkocaml_watcher_context_t *local_ctx = (zkocaml_watcher_context_t *)
      malloc(sizeof(zkocaml_watcher_context_t));
  local_ctx->watcher_ctx = String_val(watcher_ctx);
  local_ctx->watcher_callback = watcher_callback;
  local_ctx->global = 0;
  zkocaml_completion_context_t *local_data =
    (zkocaml_completion_context_t *)malloc(sizeof(zkocaml_completion_context_t));
  local_data->data = strdup(String_val(data));
  local_data->completion_callback = completion;

  caml_register_generational_global_root(&(local_ctx->watcher_callback));
  caml_register_generational_global_root(&(local_data->completion_callback));

  int rc = zoo_awget_children2(zhandle->handle,
                               local_path,
                               watcher_dispatch,
                               local_ctx,
                               strings_stat_completion_dispatch,
                               local_data);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

CAMLprim value
zkocaml_awget_children2_bytecode(value *argv, int argn)
{
  return zkocaml_awget_children2_native(argv[0], argv[1], argv[2],
                                        argv[3], argv[4], argv[5]);
}

/**
 * Flush leader channel.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *
 * @data the data that will be passed to the completion routine when
 * the function completes.
 *
 * @return ZOK on success or one of the following errcodes on failure:
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_async(value zh, value path, value completion, value data)
{
  CAMLparam4(zh, path, completion, data);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  zkocaml_completion_context_t *local_data =
    (zkocaml_completion_context_t *)malloc(sizeof(zkocaml_completion_context_t));
  local_data->data = strdup(String_val(data));
  local_data->completion_callback = completion;

  caml_register_generational_global_root(&(local_data->completion_callback));

  int rc = zoo_async(zhandle->handle,
                     local_path,
                     string_completion_dispatch,
                     local_data);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

/**
 * Gets the acl associated with a node.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *
 * @data the data that will be passed to the completion routine when
 * the function completes.
 *
 * @return ZOK on success or one of the following errcodes on failure:
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_aget_acl(value zh, value path, value completion, value data)
{
  CAMLparam4(zh, path, completion, data);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  zkocaml_completion_context_t *local_data =
    (zkocaml_completion_context_t *)malloc(sizeof(zkocaml_completion_context_t));
  local_data->data = strdup(String_val(data));
  local_data->completion_callback = completion;

  caml_register_generational_global_root(&(local_data->completion_callback));

  int rc = zoo_aget_acl(zhandle->handle,
                        local_path,
                        acl_completion_dispatch,
                        local_data);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

/**
 * Sets the acl associated with a node.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @buffer the buffer holding the acls to be written to the node.
 *
 * @completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *   ZINVALIDACL invalid ACL specified
 *   ZBADVERSION expected version does not match actual version.
 *
 * @data the data that will be passed to the completion routine when
 * the function completes.
 *
 * @return ZOK on success or one of the following errcodes on failure:
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_aset_acl_native(value zh,
                 value path,
                 value version,
                 value acl,
                 value completion,
                 value data)
{
  CAMLparam5(zh, path, version, acl, completion);
  CAMLxparam1(data);
  CAMLlocal1(result);

  struct ACL_vector local_acl;
  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  int local_version = Int_val(version);
  int r = zkocaml_parse_acls(acl, &local_acl);
  if (r == 0) {
    local_acl = ZOO_OPEN_ACL_UNSAFE;
  }
  zkocaml_completion_context_t *local_data =
    (zkocaml_completion_context_t *)malloc(sizeof(zkocaml_completion_context_t));
  local_data->data = strdup(String_val(data));
  local_data->completion_callback = completion;

  caml_register_generational_global_root(&(local_data->completion_callback));

  int rc = zoo_aset_acl(zhandle->handle,
                        local_path,
                        local_version,
                        (struct ACL_vector *)&local_acl,
                        void_completion_dispatch,
                        local_data);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

CAMLprim value
zkocaml_aset_acl_bytecode(value *argv, int argn)
{
  return zkocaml_aset_acl_native(argv[0], argv[1], argv[2],
                                 argv[3], argv[4], argv[5]);
}

/**
 * Return an error string.
 *
 * @rc return code.
 * @return string corresponding to the return code
 */
CAMLprim value
zkocaml_zerror(value rc)
{
  CAMLparam1(rc);
  CAMLlocal1(result);

  const char *errstr = zerror(Int_val(rc));
  result = caml_copy_string(errstr);

  CAMLreturn(result);
}

/**
 * Specify application credentials.
 *
 * The application calls this function to specify its credentials for purposes
 * of authentication. The server will use the security provider specified by
 * the scheme parameter to authenticate the client connection. If the
 * authentication request has failed:
 * - the server connection is dropped
 * - the watcher is called with the ZOO_AUTH_FAILED_STATE value as the state
 * parameter.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @scheme the id of authentication scheme. Natively supported:
 * "digest" password-based authentication
 *
 * @cert application credentials. The actual value depends on the scheme.
 *
 * @completion the routine to invoke when the request completes. One of
 * the following result codes may be passed into the completion callback:
 *   ZOK operation completed successfully
 *   ZAUTHFAILED authentication failed
 *
 * @data the data that will be passed to the completion routine when the
 * function completes.
 *
 * @return ZOK on success or one of the following errcodes on failure:
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 *   ZSYSTEMERROR - a system error occured
 */
CAMLprim value
zkocaml_add_auth(value zh,
                 value scheme,
                 value cert,
                 value completion,
                 value data)
{
  CAMLparam5(zh, scheme, cert, completion, data);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  zkocaml_completion_context_t *local_data =
    (zkocaml_completion_context_t *)malloc(sizeof(zkocaml_completion_context_t));
  local_data->data = strdup(String_val(data));
  local_data->completion_callback = completion;

  caml_register_generational_global_root(&(local_data->completion_callback));

  int rc = zoo_add_auth(zhandle->handle,
                        String_val(scheme),
                        String_val(cert),
                        caml_string_length(cert),
                        void_completion_dispatch,
                        local_data);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

/**
 * Checks if the current zookeeper connection state can't be recovered.
 *
 *  The application must close the zhandle and try to reconnect.
 *
 * @zh the zookeeper handle (see zookeeper_init)
 * @return ZINVALIDSTATE if connection is unrecoverable
 */
CAMLprim value
zkocaml_is_unrecoverable(value zh)
{
  CAMLparam1(zh);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  int rc = is_unrecoverable(zhandle->handle);
  result = zkocaml_enum_event_c2ml(rc);

  CAMLreturn(result);
}

/**
 * Sets the debugging level for the library
 */
CAMLprim value
zkocaml_set_debug_level(value log_level)
{
  CAMLparam1(log_level);

  ZooLogLevel level = zkocaml_enum_loglevel_ml2c(log_level);
  switch(level) {
  case ZOO_LOG_LEVEL_ERROR:
    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
    break;
  case ZOO_LOG_LEVEL_WARN:
    zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
    break;
  case ZOO_LOG_LEVEL_INFO:
    zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
    break;
  case ZOO_LOG_LEVEL_DEBUG:
    zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
    break;
  }

  CAMLreturn(Val_unit);
}

/**
 * Sets the stream to be used by the library for logging
 *
 * The zookeeper library uses stderr as its default log stream. Application
 * must make sure the stream is writable. Passing in NULL resets the stream
 * to its default value (stderr).
 */
CAMLprim value
zkocaml_set_log_stream(value log_stream)
{
  CAMLparam1(log_stream);

  const char *stream_name = String_val(log_stream);
  zkocaml_log_stream = fopen(stream_name, "w+");
  if (zkocaml_log_stream == NULL) CAMLreturn(Val_unit);
  zoo_set_log_stream(zkocaml_log_stream);

  CAMLreturn(Val_unit);
}

/**
 * Enable/disable quorum endpoint order randomization
 *
 * Note: typically this method should NOT be used outside of testing.
 *
 * If passed a non-zero value, will make the client connect to quorum peers
 * in the order as specified in the zookeeper_init() call.
 * A zero value causes zookeeper_init() to permute the peer endpoints
 * which is good for more even client connection distribution among the
 * quorum peers.
 */
CAMLprim value
zkocaml_deterministic_conn_order(value yes_or_no)
{
  CAMLparam1(yes_or_no);

  int decision = Bool_val(yes_or_no);
  zoo_deterministic_conn_order(decision);

  CAMLreturn(Val_unit);
}

/**
 * Create a node synchronously.
 *
 * This method will create a node in ZooKeeper. A node can only be created if
 * it does not already exists. The Create Flags affect the creation of nodes.
 * If ZOO_EPHEMERAL flag is set, the node will automatically get removed if the
 * client session goes away. If the ZOO_SEQUENCE flag is set, a unique
 * monotonically increasing sequence number is appended to the path name.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init.
 *
 * @path The name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @val The data to be stored in the node.
 *
 * @acl The initial ACL of the node. The ACL must not be null or empty.
 *
 * @flags this parameter can be set to 0 for normal create or an OR
 * of the Create Flags
 *
 * @path_buffer Buffer which will be filled with the path of the
 * new node (this might be different than the supplied path
 * because of the ZOO_SEQUENCE flag).  The path string will always be
 * null-terminated. This parameter may be NULL if path_buffer_len = 0.
 *
 * @path_buffer_len Size of path buffer; if the path of the new
 * node (including space for the null terminator) exceeds the buffer size,
 * the path string will be truncated to fit.  The actual path of the
 * new node in the server will not be affected by the truncation.
 * The path string will always be null-terminated.
 *
 * @return one of the following codes are returned:
 *   ZOK operation completed successfully
 *   ZNONODE the parent node does not exist.
 *   ZNODEEXISTS the node already exists
 *   ZNOAUTH the client does not have permission.
 *   ZNOCHILDRENFOREPHEMERALS cannot create children of ephemeral nodes.
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_create(value zh,
               value path,
               value val,
               value acl,
               value flags)
{
  CAMLparam5(zh, path, val, acl, flags);
  CAMLlocal3(result, error, buffer);

  struct ACL_vector local_acl;
  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  char *path_buffer = (char *)calloc(ZKOCAML_MAX_PATH_BUFFER_SIZE, sizeof(char));
  int r = zkocaml_parse_acls(acl, &local_acl);
  if (r == 0) {
    local_acl = ZOO_OPEN_ACL_UNSAFE;
  }
  int local_flags = zkocaml_enum_create_flag_ml2c(flags);

  int rc = zoo_create(zhandle->handle,
                      String_val(path),
                      String_val(val),
                      caml_string_length(val),
                      (const struct ACL_vector *)&local_acl,
                      local_flags,
                      path_buffer,
                      ZKOCAML_MAX_PATH_BUFFER_SIZE
                      );

  error = zkocaml_enum_error_c2ml(rc);
  buffer = caml_copy_string(path_buffer);
  result = caml_alloc(2, 0);
  Store_field(result, 0, error);
  Store_field(result, 1, buffer);
  free(path_buffer);

  CAMLreturn(result);
}

/**
 * Delete a node in zookeeper synchronously.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @version the expected version of the node. The function will fail if the
 * actual version of the node does not match the expected version.
 * If -1 is used the version check will not take place.
 *
 * @return one of the following values is returned.
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *   ZBADVERSION expected version does not match actual version.
 *   ZNOTEMPTY children are present; node cannot be deleted.
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_delete(value zh, value path, value version)
{
  CAMLparam3(zh, path, version);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  int local_version = Int_val(version);

  int rc = zoo_delete(zhandle->handle, local_path, local_version);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

/**
 * Checks the existence of a node in zookeeper synchronously.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @watch if nonzero, a watch will be set at the server to notify the
 * client if the node changes. The watch will be set even if the node does not
 * exist. This allows clients to watch for nodes to appear.
 *
 * @stat the returned stat value of the node.
 *
 * @return return code of the function call.
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_exists(value zh, value path, value watch)
{
  CAMLparam3(zh, path, watch);
  CAMLlocal3(result, error, stat);

  struct Stat local_stat;
  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  int local_watch = Int_val(watch);

  int rc = zoo_exists(zhandle->handle,
                      local_path,
                      local_watch,
                      (struct Stat *)&local_stat);

  error = zkocaml_enum_error_c2ml(rc);
  stat = zkocaml_build_stat_struct(&local_stat);
  result = caml_alloc(2, 0);
  Store_field(result, 0, error);
  Store_field(result, 1, stat);

  CAMLreturn(result);
}

/**
 * Checks the existence of a node in zookeeper synchronously.
 *
 * This function is similar to zoo_exists except it allows one specify
 * a watcher object rather than a boolean watch flag.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @watcher if non-null a watch will set on the specified znode on the server.
 * The watch will be set even if the node does not exist. This allows clients
 * to watch for nodes to appear.
 *
 * @watcher_ctx user specific data, will be passed to the watcher callback.
 * Unlike the global context set by zookeeper_init, this watcher context
 * is associated with the given instance of the watcher only.
 *
 * @stat the return stat value of the node.
 *
 * @return return code of the function call.
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_wexists(value zh,
                value path,
                value watcher_callback,
                value watcher_ctx)
{
  CAMLparam4(zh, path, watcher_callback, watcher_ctx);
  CAMLlocal3(result, error, stat);

  struct Stat local_stat;
  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  zkocaml_watcher_context_t *local_ctx = (zkocaml_watcher_context_t *)
      malloc(sizeof(zkocaml_watcher_context_t));
  local_ctx->watcher_ctx = String_val(watcher_ctx);
  local_ctx->watcher_callback = watcher_callback;
  local_ctx->global = 0;

  caml_register_generational_global_root(&(local_ctx->watcher_callback));

  int rc = zoo_wexists(zhandle->handle,
                       local_path,
                       watcher_dispatch,
                       local_ctx,
                       (struct Stat *)&local_stat);

  error = zkocaml_enum_error_c2ml(rc);
  stat = zkocaml_build_stat_struct(&local_stat);
  result = caml_alloc(2, 0);
  Store_field(result, 0, error);
  Store_field(result, 1, stat);

  CAMLreturn(result);

}

/**
 * Gets the data associated with a node synchronously.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @watch if nonzero, a watch will be set at the server to notify
 * the client if the node changes.
 *
 * @buffer the buffer holding the node data returned by the server
 *
 * @stat if not NULL, will hold the value of stat for the path on return.
 *
 * @return return value of the function call.
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either in ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_get(value zh,
            value path,
            value watch)
{
  CAMLparam3(zh, path, watch);
  CAMLlocal4(result, error, buffer, stat);

  int path_buffer_size = ZKOCAML_MAX_PATH_BUFFER_SIZE;
  struct Stat local_stat;
  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  char *path_buffer = (char *)malloc(
                      sizeof(char) * path_buffer_size);
  memset(path_buffer, 0, path_buffer_size);
  const char *local_path = String_val(path);
  int local_watch = Int_val(watch);
  int rc = zoo_get(zhandle->handle,
                   local_path,
                   local_watch,
                   path_buffer,
                   &path_buffer_size,
                   (struct Stat *)&local_stat);
  error = zkocaml_enum_error_c2ml(rc);
  buffer = caml_copy_string(path_buffer);
  stat = zkocaml_build_stat_struct(&local_stat);
  result = caml_alloc(3, 0);
  Store_field(result, 0, error);
  Store_field(result, 1, buffer);
  Store_field(result, 2, stat);
  free(path_buffer);

  CAMLreturn(result);
}

/**
 * Gets the data associated with a node synchronously.
 *
 * This function is similar to zoo_get except it allows one specify
 * a watcher object rather than a boolean watch flag.
 *
 * @zh the zookeeper handle obtained by a call to \ref zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @watcher if non-null, a watch will be set at the server to notify
 * the client if the node changes.
 *
 * @watcher_ctx user specific data, will be passed to the watcher callback.
 * Unlike the global context set by \ref zookeeper_init, this watcher context
 * is associated with the given instance of the watcher only.
 *
 * @buffer the buffer holding the node data returned by the server
 *
 * @stat if not NULL, will hold the value of stat for the path on return.
 *
 * @return return value of the function call.
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either in ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_wget(value zh,
             value path,
             value watcher_callback,
             value watcher_ctx)
{
  CAMLparam4(zh, path, watcher_callback, watcher_ctx);
  CAMLlocal4(result, error, buffer, stat);

  int path_buffer_size = ZKOCAML_MAX_PATH_BUFFER_SIZE;
  struct Stat local_stat;
  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  char *path_buffer = (char *)malloc(
                      sizeof(char) * path_buffer_size);
  memset(path_buffer, 0, path_buffer_size);
  const char *local_path = String_val(path);
  zkocaml_watcher_context_t *local_ctx = (zkocaml_watcher_context_t *)
      malloc(sizeof(zkocaml_watcher_context_t));
  local_ctx->watcher_ctx = String_val(watcher_ctx);
  local_ctx->watcher_callback = watcher_callback;
  local_ctx->global = 0;

  caml_register_generational_global_root(&(local_ctx->watcher_callback));

  int rc = zoo_wget(zhandle->handle,
                    local_path,
                    watcher_dispatch,
                    local_ctx,
                    path_buffer,
                    &path_buffer_size,
                    (struct Stat *)&local_stat);

  error = zkocaml_enum_error_c2ml(rc);
  buffer = caml_copy_string(path_buffer);
  stat = zkocaml_build_stat_struct(&local_stat);
  result = caml_alloc(3, 0);
  Store_field(result, 0, error);
  Store_field(result, 1, buffer);
  Store_field(result, 2, stat);
  free(path_buffer);

  CAMLreturn(result);
}

/**
 * Sets the data associated with a node. See zoo_set2 function if
 * you require access to the stat information associated with the znode.
 *
 * @zh the zookeeper handle obtained by a call to \ref zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @buffer the buffer holding data to be written to the node.
 *
 * @version the expected version of the node. The function will fail if
 * the actual version of the node does not match the expected version. If -1 is
 * used the version check will not take place.
 *
 * @return the return code for the function call.
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *   ZBADVERSION expected version does not match actual version.
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_set(value zh, value path, value buffer, value version)
{
  CAMLparam4(zh, path, buffer, version);
  CAMLlocal1(result);

  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);

  int rc = zoo_set(zhandle->handle,
                   String_val(path),
                   String_val(buffer),
                   caml_string_length(buffer),
                   Int_val(version));
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}

/**
 * Sets the data associated with a node. This function is the same
 * as zoo_set except that it also provides access to stat information
 * associated with the znode.
 *
 * @zh the zookeeper handle obtained by a call to \ref zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @buffer the buffer holding data to be written to the node.
 *
 * @version the expected version of the node. The function will fail if
 * the actual version of the node does not match the expected version. If -1 is
 * used the version check will not take place.
 *
 * @stat if not NULL, will hold the value of stat for the path on return.
 *
 * @return the return code for the function call.
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *   ZBADVERSION expected version does not match actual version.
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_set2(value zh, value path, value buffer, value version)
{
  CAMLparam4(zh, path, buffer, version);
  CAMLlocal3(result, error, stat);

  struct Stat local_stat;
  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);

  int rc = zoo_set2(zhandle->handle,
                    String_val(path),
                    String_val(buffer),
                    caml_string_length(buffer),
                    Int_val(version),
                    &local_stat);

  error = zkocaml_enum_error_c2ml(rc);
  stat = zkocaml_build_stat_struct(&local_stat);
  result = caml_alloc(2, 0);
  Store_field(result, 0, error);
  Store_field(result, 1, stat);

  CAMLreturn(result);
}

/**
 * Lists the children of a node synchronously.
 *
 * @zh the zookeeper handle obtained by a call to \ref zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @watch if nonzero, a watch will be set at the server to notify
 * the client if the node changes.
 *
 * @strings return value of children paths.
 *
 * @return the return code of the function.
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_get_children(value zh, value path, value watch)
{
  CAMLparam3(zh, path, watch);
  CAMLlocal3(result, error, strs);

  struct String_vector local_strings;
  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  int local_watch = Int_val(watch);

  int rc = zoo_get_children(zhandle->handle,
                      local_path,
                      local_watch,
                      (struct String_vector *)&local_strings);

  error = zkocaml_enum_error_c2ml(rc);
  strs = zkocaml_build_strings_struct(&local_strings);
  result = caml_alloc(2, 0);
  Store_field(result, 0, error);
  Store_field(result, 1, strs);

  CAMLreturn(result);
}

/**
 * Lists the children of a node synchronously.
 *
 * This function is similar to zoo_get_children except it allows one specify
 * a watcher object rather than a boolean watch flag.
 *
 * @zh the zookeeper handle obtained by a call to zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @watcher if non-null, a watch will be set at the server to notify
 * the client if the node changes.
 *
 * @watcher_ctx user specific data, will be passed to the watcher callback.
 * Unlike the global context set by \ref zookeeper_init, this watcher context
 * is associated with the given instance of the watcher only.
 *
 * @strings return value of children paths.
 *
 * @return the return code of the function.
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_wget_children(value zh,
                      value path,
                      value watcher_callback,
                      value watcher_ctx)
{
  CAMLparam4(zh, path, watcher_callback, watcher_ctx);
  CAMLlocal3(result, error, strs);

  struct String_vector local_strings;
  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  zkocaml_watcher_context_t *local_ctx = (zkocaml_watcher_context_t *)
      malloc(sizeof(zkocaml_watcher_context_t));
  local_ctx->watcher_ctx = String_val(watcher_ctx);
  local_ctx->watcher_callback = watcher_callback;
  local_ctx->global = 0;

  caml_register_generational_global_root(&(local_ctx->watcher_callback));

  int rc = zoo_wget_children(zhandle->handle,
                             local_path,
                             watcher_dispatch,
                             local_ctx,
                             (struct String_vector *)&local_strings);

  error = zkocaml_enum_error_c2ml(rc);
  strs = zkocaml_build_strings_struct(&local_strings);
  result = caml_alloc(2, 0);
  Store_field(result, 0, error);
  Store_field(result, 1, strs);

  CAMLreturn(result);
}

/**
 * Lists the children of a node and get its stat synchronously.
 *
 * This function is new in version 3.3.0
 *
 * @zh the zookeeper handle obtained by a call to \ref zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @watch if nonzero, a watch will be set at the server to notify
 * the client if the node changes.
 *
 * @strings return value of children paths.
 *
 * @stat return value of node stat.
 *
 * @return the return code of the function.
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_get_children2(value zh,
                      value path,
                      value watch)
{
  CAMLparam3(zh, path, watch);
  CAMLlocal4(result, error, strs, stat);

  struct String_vector local_strings;
  struct Stat local_stat;
  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  int local_watch = Int_val(watch);

  int rc = zoo_get_children2(zhandle->handle,
                      local_path,
                      local_watch,
                      (struct String_vector *)&local_strings,
                      (struct Stat *)&local_stat);

  error = zkocaml_enum_error_c2ml(rc);
  strs = zkocaml_build_strings_struct(&local_strings);
  stat = zkocaml_build_stat_struct(&local_stat);
  result = caml_alloc(3, 0);
  Store_field(result, 0, error);
  Store_field(result, 1, strs);
  Store_field(result, 2, stat);

  CAMLreturn(result);
}

/**
 * Lists the children of a node and get its stat synchronously.
 *
 * This function is similar to \ref zoo_get_children except it allows one specify
 * a watcher object rather than a boolean watch flag.
 *
 * This function is new in version 3.3.0
 *
 * @zh the zookeeper handle obtained by a call to \ref zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @watcher if non-null, a watch will be set at the server to notify
 * the client if the node changes.
 *
 * @watcher_ctx user specific data, will be passed to the watcher callback.
 * Unlike the global context set by \ref zookeeper_init, this watcher context
 * is associated with the given instance of the watcher only.
 *
 * @strings return value of children paths.
 *
 * @stat return value of node stat.
 *
 * @return the return code of the function.
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_wget_children2(value zh,
                       value path,
                       value watcher_callback,
                       value watcher_ctx)
{
  CAMLparam4(zh, path, watcher_callback, watcher_ctx);
  CAMLlocal4(result, error, strs, stat);

  struct String_vector local_strings;
  struct Stat local_stat;
  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  zkocaml_watcher_context_t *local_ctx = (zkocaml_watcher_context_t *)
      malloc(sizeof(zkocaml_watcher_context_t));
  local_ctx->watcher_ctx = String_val(watcher_ctx);
  local_ctx->watcher_callback = watcher_callback;
  local_ctx->global = 0;
  const char *local_path = String_val(path);

  caml_register_generational_global_root(&(local_ctx->watcher_callback));

  int rc = zoo_wget_children2(zhandle->handle,
                      local_path,
                      watcher_dispatch,
                      local_ctx,
                      (struct String_vector *)&local_strings,
                      (struct Stat *)&local_stat);

  error = zkocaml_enum_error_c2ml(rc);
  strs = zkocaml_build_strings_struct(&local_strings);
  stat = zkocaml_build_stat_struct(&local_stat);
  result = caml_alloc(3, 0);
  Store_field(result, 0, error);
  Store_field(result, 1, strs);
  Store_field(result, 2, stat);

  CAMLreturn(result);
}

/**
 * Gets the acl associated with a node synchronously.
 *
 * @zh the zookeeper handle obtained by a call to \ref zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @acl the return value of acls on the path.
 *
 * @stat returns the stat of the path specified.
 *
 * @return the return code for the function call.
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_get_acl(value zh, value path)
{
  CAMLparam2(zh, path);
  CAMLlocal4(result, error, acls, stat);

  struct ACL_vector local_acl;
  struct Stat local_stat;
  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);

  int rc = zoo_get_acl(zhandle->handle,
                      local_path,
                      (struct ACL_vector*)&local_acl,
                      (struct Stat *)&local_stat);

  error = zkocaml_enum_error_c2ml(rc);
  acls = zkocaml_build_acls_struct(&local_acl);
  stat = zkocaml_build_stat_struct(&local_stat);
  result = caml_alloc(3, 0);
  Store_field(result, 0, error);
  Store_field(result, 1, acls);
  Store_field(result, 2, stat);

  CAMLreturn(result);
}

/**
 * Sets the acl associated with a node synchronously.
 *
 * @zh the zookeeper handle obtained by a call to \ref zookeeper_init
 *
 * @path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @version the expected version of the path.
 *
 * @acl the acl to be set on the path.
 *
 * @return the return code for the function call.
 *   ZOK operation completed successfully
 *   ZNONODE the node does not exist.
 *   ZNOAUTH the client does not have permission.
 *   ZINVALIDACL invalid ACL specified
 *   ZBADVERSION expected version does not match actual version.
 *   ZBADARGUMENTS - invalid input parameters
 *   ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 *   ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
CAMLprim value
zkocaml_set_acl(value zh, value path, value version, value acl)
{
  CAMLparam4(zh, path, version, acl);
  CAMLlocal1(result);

  struct ACL_vector local_acl;
  zkocaml_handle_t *zhandle = zkocaml_handle_struct_val(zh);
  const char *local_path = String_val(path);
  int local_version = Int_val(version);
  int r = zkocaml_parse_acls(acl, &local_acl);
  if (r == 0) {
    local_acl = ZOO_OPEN_ACL_UNSAFE;
  }

  int rc = zoo_set_acl(zhandle->handle,
                       local_path,
                       local_version,
                       (const struct ACL_vector *)&local_acl);
  result = zkocaml_enum_error_c2ml(rc);

  CAMLreturn(result);
}
