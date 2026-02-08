#pragma once

#include <stdint.h>
#include <sys/stat.h>
#include "kafs_hotplug.h"

#define KAFS_RPC_MAGIC 0x4b415250u
#define KAFS_RPC_VERSION 1u
#define KAFS_RPC_MAX_PAYLOAD 16384u

#define KAFS_RPC_HELLO_MAJOR 1u
#define KAFS_RPC_HELLO_MINOR 0u
#define KAFS_RPC_HELLO_FEATURES 0u

#define KAFS_RPC_FLAG_ENDIAN_HOST 0x1u

typedef struct
{
  uint32_t magic;
  uint16_t version;
  uint16_t op;
  uint32_t flags;
  uint64_t req_id;
  uint64_t session_id;
  uint32_t epoch;
  uint32_t payload_len;
} kafs_rpc_hdr_t;

typedef struct
{
  uint64_t req_id;
  int32_t result;
  uint32_t payload_len;
} kafs_rpc_resp_hdr_t;

typedef struct
{
  uint16_t major;
  uint16_t minor;
  uint32_t feature_flags;
} kafs_rpc_hello_t;

enum
{
  KAFS_RPC_OP_HELLO = 1,
  KAFS_RPC_OP_READY = 2,
  KAFS_RPC_OP_GETATTR = 3,
  KAFS_RPC_OP_READ = 4,
  KAFS_RPC_OP_WRITE = 5,
  KAFS_RPC_OP_TRUNCATE = 6,
  KAFS_RPC_OP_SESSION_RESTORE = 7,
  KAFS_RPC_OP_CTL_STATUS = 50,
  KAFS_RPC_OP_CTL_COMPAT = 51,
  KAFS_RPC_OP_CTL_RESTART = 52,
  KAFS_RPC_OP_CTL_SET_TIMEOUT = 53,
  KAFS_RPC_OP_CTL_ENV_LIST = 54,
  KAFS_RPC_OP_CTL_ENV_SET = 55,
  KAFS_RPC_OP_CTL_ENV_UNSET = 56
};

enum
{
  KAFS_RPC_DATA_INLINE = 1,
  KAFS_RPC_DATA_PLAN_ONLY = 2,
  KAFS_RPC_DATA_SHM = 3
};

typedef struct
{
  uint32_t open_handle_count;
} kafs_rpc_session_restore_t;

typedef struct
{
  uint32_t ino;
  uint32_t uid;
  uint32_t gid;
  uint32_t pid;
} kafs_rpc_getattr_req_t;

typedef struct
{
  struct stat st;
} kafs_rpc_getattr_resp_t;

typedef struct
{
  uint32_t ino;
  uint32_t uid;
  uint32_t gid;
  uint32_t pid;
  uint64_t off;
  uint32_t size;
  uint32_t data_mode;
} kafs_rpc_read_req_t;

typedef struct
{
  uint32_t size;
} kafs_rpc_read_resp_t;

typedef struct
{
  uint32_t ino;
  uint32_t uid;
  uint32_t gid;
  uint32_t pid;
  uint64_t off;
  uint32_t size;
  uint32_t data_mode;
} kafs_rpc_write_req_t;

typedef struct
{
  uint32_t size;
} kafs_rpc_write_resp_t;

typedef struct
{
  uint32_t ino;
  uint32_t reserved;
  uint64_t size;
} kafs_rpc_truncate_req_t;

typedef struct
{
  uint64_t size;
} kafs_rpc_truncate_resp_t;

typedef struct
{
  uint32_t version;
  uint32_t state;
  uint32_t data_mode;
  uint64_t session_id;
  uint32_t epoch;
  int32_t last_error;
  uint32_t wait_queue_len;
  uint32_t wait_timeout_ms;
  uint32_t wait_queue_limit;
  uint16_t front_major;
  uint16_t front_minor;
  uint32_t front_features;
  uint16_t back_major;
  uint16_t back_minor;
  uint32_t back_features;
  uint32_t compat_result;
  int32_t compat_reason;
} kafs_rpc_hotplug_status_t;

typedef struct
{
  uint32_t timeout_ms;
} kafs_rpc_set_timeout_t;

typedef struct
{
  uint32_t count;
  kafs_hotplug_env_entry_t entries[KAFS_HOTPLUG_ENV_MAX];
} kafs_rpc_env_list_t;

typedef struct
{
  char key[KAFS_HOTPLUG_ENV_KEY_MAX];
  char value[KAFS_HOTPLUG_ENV_VALUE_MAX];
} kafs_rpc_env_update_t;

uint64_t kafs_rpc_next_req_id(void);
int kafs_rpc_send_msg(int fd, uint16_t op, uint32_t flags, uint64_t req_id, uint64_t session_id,
                      uint32_t epoch, const void *payload, uint32_t payload_len);
int kafs_rpc_recv_msg(int fd, kafs_rpc_hdr_t *hdr, void *payload, uint32_t payload_cap,
                      uint32_t *payload_len);
int kafs_rpc_send_resp(int fd, uint64_t req_id, int32_t result, const void *payload,
                       uint32_t payload_len);
int kafs_rpc_recv_resp(int fd, kafs_rpc_resp_hdr_t *hdr, void *payload, uint32_t payload_cap,
                       uint32_t *payload_len);
