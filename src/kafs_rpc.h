#pragma once

#include <stdint.h>

#define KAFS_RPC_MAGIC 0x4b415250u
#define KAFS_RPC_VERSION 1u
#define KAFS_RPC_MAX_PAYLOAD 1024u

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
  uint16_t major;
  uint16_t minor;
  uint32_t feature_flags;
} kafs_rpc_hello_t;

enum
{
  KAFS_RPC_OP_HELLO = 1,
  KAFS_RPC_OP_READY = 2
};

uint64_t kafs_rpc_next_req_id(void);
int kafs_rpc_send_msg(int fd, uint16_t op, uint32_t flags, uint64_t req_id, uint64_t session_id,
                      uint32_t epoch, const void *payload, uint32_t payload_len);
int kafs_rpc_recv_msg(int fd, kafs_rpc_hdr_t *hdr, void *payload, uint32_t payload_cap,
                      uint32_t *payload_len);
