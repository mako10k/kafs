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

int kafs_rpc_send_hdr(int fd, uint16_t op)
{
  kafs_rpc_hdr_t hdr;
  memset(&hdr, 0, sizeof(hdr));
  hdr.magic = KAFS_RPC_MAGIC;
  hdr.version = KAFS_RPC_VERSION;
  hdr.op = op;
  hdr.payload_len = 0;
  return kafs_rpc_write_full(fd, &hdr, sizeof(hdr));
}

int kafs_rpc_recv_hdr(int fd, kafs_rpc_hdr_t *hdr)
{
  int rc = kafs_rpc_read_full(fd, hdr, sizeof(*hdr));
  if (rc != 0)
    return rc;
  if (hdr->magic != KAFS_RPC_MAGIC)
    return -EBADMSG;
  if (hdr->version != KAFS_RPC_VERSION)
    return -EPROTONOSUPPORT;
  return 0;
}
