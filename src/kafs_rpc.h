#pragma once

#include <stdint.h>
#include <sys/stat.h>

#define KAFS_RPC_MAGIC 0x4b415250u
#define KAFS_RPC_VERSION 1u
#define KAFS_RPC_MAX_PAYLOAD 1024u

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
  KAFS_RPC_OP_TRUNCATE = 6
};

enum
{
  KAFS_RPC_DATA_INLINE = 1,
  KAFS_RPC_DATA_PLAN_ONLY = 2,
  KAFS_RPC_DATA_SHM = 3
};

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

uint64_t kafs_rpc_next_req_id(void);
int kafs_rpc_send_msg(int fd, uint16_t op, uint32_t flags, uint64_t req_id, uint64_t session_id,
                      uint32_t epoch, const void *payload, uint32_t payload_len);
int kafs_rpc_recv_msg(int fd, kafs_rpc_hdr_t *hdr, void *payload, uint32_t payload_cap,
                      uint32_t *payload_len);
int kafs_rpc_send_resp(int fd, uint64_t req_id, int32_t result, const void *payload,
                       uint32_t payload_len);
int kafs_rpc_recv_resp(int fd, kafs_rpc_resp_hdr_t *hdr, void *payload, uint32_t payload_cap,
                       uint32_t *payload_len);
