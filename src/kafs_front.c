#include "kafs_rpc.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <signal.h>
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
  kafs_rpc_hello_t hello;
  uint32_t payload_len = 0;
  int rc = kafs_rpc_recv_msg(cli, &hdr, &hello, sizeof(hello), &payload_len);
  if (rc != 0 || hdr.op != KAFS_RPC_OP_HELLO)
  {
    fprintf(stderr, "kafs-front: invalid hello rc=%d op=%u\n", rc, (unsigned)hdr.op);
    close(cli);
    close(srv);
    return 2;
  }
  if (payload_len != sizeof(hello))
  {
    fprintf(stderr, "kafs-front: hello payload size mismatch\n");
    close(cli);
    close(srv);
    return 2;
  }
  if (hello.major != KAFS_RPC_HELLO_MAJOR || hello.minor != KAFS_RPC_HELLO_MINOR ||
      (hello.feature_flags & ~KAFS_RPC_HELLO_FEATURES) != 0)
  {
    fprintf(stderr, "kafs-front: hello version/feature mismatch\n");
    close(cli);
    close(srv);
    return 2;
  }

  uint64_t session_id = 1u;
  uint32_t epoch = 0u;
  kafs_rpc_session_restore_t restore;
  restore.open_handle_count = 0u;
  uint64_t req_id = kafs_rpc_next_req_id();

  rc = kafs_rpc_send_msg(cli, KAFS_RPC_OP_SESSION_RESTORE, KAFS_RPC_FLAG_ENDIAN_HOST, req_id,
                         session_id, epoch, &restore, sizeof(restore));
  if (rc != 0)
  {
    fprintf(stderr, "kafs-front: failed to send session_restore rc=%d\n", rc);
    close(cli);
    close(srv);
    return 2;
  }

  kafs_rpc_hdr_t ready_hdr;
  uint32_t ready_len = 0;
  rc = kafs_rpc_recv_msg(cli, &ready_hdr, NULL, 0, &ready_len);
  if (rc != 0 || ready_hdr.op != KAFS_RPC_OP_READY)
  {
    fprintf(stderr, "kafs-front: invalid ready rc=%d op=%u\n", rc, (unsigned)ready_hdr.op);
    close(cli);
    close(srv);
    return 2;
  }
  if (ready_len != 0)
  {
    fprintf(stderr, "kafs-front: ready payload size mismatch\n");
    close(cli);
    close(srv);
    return 2;
  }

  fprintf(stderr, "kafs-front: handshake ok\n");
  for (;;)
    pause();
}
