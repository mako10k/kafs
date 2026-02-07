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

  int rc = kafs_rpc_send_hdr(fd, KAFS_RPC_OP_HELLO);
  if (rc != 0)
  {
    fprintf(stderr, "kafs-back: failed to send hello rc=%d\n", rc);
    close(fd);
    return 2;
  }

  kafs_rpc_hdr_t hdr;
  rc = kafs_rpc_recv_hdr(fd, &hdr);
  if (rc != 0 || hdr.op != KAFS_RPC_OP_READY)
  {
    fprintf(stderr, "kafs-back: invalid ready rc=%d op=%u\n", rc, (unsigned)hdr.op);
    close(fd);
    return 2;
  }

  fprintf(stderr, "kafs-back: handshake ok\n");
  close(fd);
  return 0;
}
