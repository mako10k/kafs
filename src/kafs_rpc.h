#pragma once

#include <stdint.h>

#define KAFS_RPC_MAGIC 0x4b415250u
#define KAFS_RPC_VERSION 1u

typedef struct
{
  uint32_t magic;
  uint16_t version;
  uint16_t op;
  uint32_t payload_len;
} kafs_rpc_hdr_t;

enum
{
  KAFS_RPC_OP_HELLO = 1,
  KAFS_RPC_OP_READY = 2
};

int kafs_rpc_send_hdr(int fd, uint16_t op);
int kafs_rpc_recv_hdr(int fd, kafs_rpc_hdr_t *hdr);
