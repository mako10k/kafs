#include "kafs_rpc.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

static void usage(const char *prog)
{
  fprintf(stderr, "Usage: %s [--uds <path>]\n", prog);
}

int main(int argc, char **argv)
{
  const char *uds_path = getenv("KAFS_HOTPLUG_UDS");
  if (!uds_path)
    uds_path = "/tmp/kafs-hotplug.sock";

  for (int i = 1; i < argc; ++i)
  {
    if (strcmp(argv[i], "--uds") == 0)
    {
      if (i + 1 >= argc)
      {
        usage(argv[0]);
        return 2;
      }
      uds_path = argv[++i];
      continue;
    }
    if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0)
    {
      usage(argv[0]);
      return 0;
    }
  }

  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0)
  {
    perror("socket");
    return 2;
  }

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  if (snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", uds_path) >=
      (int)sizeof(addr.sun_path))
  {
    fprintf(stderr, "uds path too long\n");
    close(fd);
    return 2;
  }

  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
  {
    perror("connect");
    close(fd);
    return 2;
  }

  kafs_rpc_hello_t hello;
  hello.major = KAFS_RPC_HELLO_MAJOR;
  hello.minor = KAFS_RPC_HELLO_MINOR;
  hello.feature_flags = KAFS_RPC_HELLO_FEATURES;
  uint64_t req_id = kafs_rpc_next_req_id();
  int rc = kafs_rpc_send_msg(fd, KAFS_RPC_OP_HELLO, KAFS_RPC_FLAG_ENDIAN_HOST, req_id, 1u, 0u,
                             &hello, sizeof(hello));
  if (rc != 0)
  {
    fprintf(stderr, "kafs-back: failed to send hello rc=%d\n", rc);
    close(fd);
    return 2;
  }

  kafs_rpc_hdr_t hdr;
  kafs_rpc_hello_t ready;
  uint32_t payload_len = 0;
  rc = kafs_rpc_recv_msg(fd, &hdr, &ready, sizeof(ready), &payload_len);
  if (rc != 0 || hdr.op != KAFS_RPC_OP_READY)
  {
    fprintf(stderr, "kafs-back: invalid ready rc=%d op=%u\n", rc, (unsigned)hdr.op);
    close(fd);
    return 2;
  }
  if (payload_len != sizeof(ready))
  {
    fprintf(stderr, "kafs-back: ready payload size mismatch\n");
    close(fd);
    return 2;
  }
  if (ready.major != KAFS_RPC_HELLO_MAJOR || ready.minor != KAFS_RPC_HELLO_MINOR ||
      (ready.feature_flags & ~KAFS_RPC_HELLO_FEATURES) != 0)
  {
    fprintf(stderr, "kafs-back: ready version/feature mismatch\n");
    close(fd);
    return 2;
  }

  fprintf(stderr, "kafs-back: handshake ok\n");

  uint8_t payload[KAFS_RPC_MAX_PAYLOAD];
  for (;;)
  {
    kafs_rpc_hdr_t req_hdr;
    uint32_t req_len = 0;
    rc = kafs_rpc_recv_msg(fd, &req_hdr, payload, sizeof(payload), &req_len);
    if (rc != 0)
    {
      fprintf(stderr, "kafs-back: recv rc=%d\n", rc);
      break;
    }

    int result = -ENOSYS;
    switch (req_hdr.op)
    {
    case KAFS_RPC_OP_GETATTR:
      if (req_len != sizeof(kafs_rpc_getattr_req_t))
        result = -EBADMSG;
      break;
    case KAFS_RPC_OP_READ:
      if (req_len != sizeof(kafs_rpc_read_req_t))
        result = -EBADMSG;
      break;
    case KAFS_RPC_OP_WRITE:
      if (req_len < sizeof(kafs_rpc_write_req_t))
        result = -EBADMSG;
      break;
    case KAFS_RPC_OP_TRUNCATE:
      if (req_len != sizeof(kafs_rpc_truncate_req_t))
        result = -EBADMSG;
      break;
    default:
      result = -ENOSYS;
      break;
    }

    rc = kafs_rpc_send_resp(fd, req_hdr.req_id, result, NULL, 0);
    if (rc != 0)
    {
      fprintf(stderr, "kafs-back: send resp rc=%d\n", rc);
      break;
    }
  }

  close(fd);
  return 0;
}
