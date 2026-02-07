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

  int srv = socket(AF_UNIX, SOCK_STREAM, 0);
  if (srv < 0)
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
    close(srv);
    return 2;
  }
  unlink(uds_path);

  if (bind(srv, (struct sockaddr *)&addr, sizeof(addr)) < 0)
  {
    perror("bind");
    close(srv);
    return 2;
  }

  if (listen(srv, 1) < 0)
  {
    perror("listen");
    close(srv);
    return 2;
  }

  fprintf(stderr, "kafs-front: waiting for back on %s\n", uds_path);
  int cli = accept(srv, NULL, NULL);
  if (cli < 0)
  {
    perror("accept");
    close(srv);
    return 2;
  }

  kafs_rpc_hdr_t hdr;
  int rc = kafs_rpc_recv_hdr(cli, &hdr);
  if (rc != 0 || hdr.op != KAFS_RPC_OP_HELLO)
  {
    fprintf(stderr, "kafs-front: invalid hello rc=%d op=%u\n", rc, (unsigned)hdr.op);
    close(cli);
    close(srv);
    return 2;
  }

  rc = kafs_rpc_send_hdr(cli, KAFS_RPC_OP_READY);
  if (rc != 0)
  {
    fprintf(stderr, "kafs-front: failed to send ready rc=%d\n", rc);
    close(cli);
    close(srv);
    return 2;
  }

  fprintf(stderr, "kafs-front: handshake ok\n");
  close(cli);
  close(srv);
  return 0;
}
