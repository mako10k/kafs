#include "kafs_rpc.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>

static int kafs_rpc_write_full(int fd, const void *buf, size_t len)
{
  const char *p = (const char *)buf;
  size_t off = 0;
  while (off < len)
  {
    ssize_t w = write(fd, p + off, len - off);
    if (w < 0)
      return -errno;
    if (w == 0)
      return -EIO;
    off += (size_t)w;
  }
  return 0;
}

static int kafs_rpc_read_full(int fd, void *buf, size_t len)
{
  char *p = (char *)buf;
  size_t off = 0;
  while (off < len)
  {
    ssize_t r = read(fd, p + off, len - off);
    if (r < 0)
      return -errno;
    if (r == 0)
      return -EIO;
    off += (size_t)r;
  }
  return 0;
}

static int kafs_rpc_discard(int fd, uint32_t len)
{
  char tmp[256];
  uint32_t left = len;
  while (left > 0)
  {
    uint32_t chunk = left > (uint32_t)sizeof(tmp) ? (uint32_t)sizeof(tmp) : left;
    int rc = kafs_rpc_read_full(fd, tmp, chunk);
    if (rc != 0)
      return rc;
    left -= chunk;
  }
  return 0;
}

uint64_t kafs_rpc_next_req_id(void)
{
  static uint64_t next_id = 0;
  return __atomic_add_fetch(&next_id, 1u, __ATOMIC_RELAXED);
}

int kafs_rpc_send_msg(int fd, uint16_t op, uint32_t flags, uint64_t req_id, uint64_t session_id,
                      uint32_t epoch, const void *payload, uint32_t payload_len)
{
  if (payload_len > KAFS_RPC_MAX_PAYLOAD)
    return -EMSGSIZE;
  if (payload_len != 0 && payload == NULL)
    return -EINVAL;
  kafs_rpc_hdr_t hdr;
  memset(&hdr, 0, sizeof(hdr));
  hdr.magic = KAFS_RPC_MAGIC;
  hdr.version = KAFS_RPC_VERSION;
  hdr.op = op;
  hdr.flags = flags;
  hdr.req_id = req_id;
  hdr.session_id = session_id;
  hdr.epoch = epoch;
  hdr.payload_len = payload_len;
  int rc = kafs_rpc_write_full(fd, &hdr, sizeof(hdr));
  if (rc != 0)
    return rc;
  if (payload_len == 0)
    return 0;
  return kafs_rpc_write_full(fd, payload, payload_len);
}

int kafs_rpc_recv_msg(int fd, kafs_rpc_hdr_t *hdr, void *payload, uint32_t payload_cap,
                      uint32_t *payload_len)
{
  int rc = kafs_rpc_read_full(fd, hdr, sizeof(*hdr));
  if (rc != 0)
    return rc;
  if (hdr->magic != KAFS_RPC_MAGIC)
    return -EBADMSG;
  if (hdr->version != KAFS_RPC_VERSION)
    return -EPROTONOSUPPORT;
  if ((hdr->flags & KAFS_RPC_FLAG_ENDIAN_HOST) == 0)
    return -EPROTONOSUPPORT;
  if (hdr->payload_len > KAFS_RPC_MAX_PAYLOAD)
    return -EMSGSIZE;
  if (payload_len)
    *payload_len = hdr->payload_len;
  if (hdr->payload_len == 0)
    return 0;
  if (hdr->payload_len > payload_cap)
  {
    int drc = kafs_rpc_discard(fd, hdr->payload_len);
    return drc != 0 ? drc : -EMSGSIZE;
  }
  return kafs_rpc_read_full(fd, payload, hdr->payload_len);
}
