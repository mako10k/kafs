#include "kafs_rpc.h"
#include "kafs_back_server.h"
#ifdef KAFS_BACK_ENABLE_IMAGE
#include "kafs_core.h"
#endif

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

static void usage(const char *prog)
{
#ifdef KAFS_BACK_ENABLE_IMAGE
  fprintf(stderr, "Usage: %s [--uds <path>] [--image <path>]\n", prog);
#else
  fprintf(stderr, "Usage: %s [--uds <path>]\n", prog);
#endif
}

static int kafs_back_connect_uds(const char *uds_path)
{
  if (!uds_path || !*uds_path)
    return -EINVAL;

  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0)
    return -errno;

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  if (snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", uds_path) >= (int)sizeof(addr.sun_path))
  {
    close(fd);
    return -ENAMETOOLONG;
  }

  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0)
  {
    int rc = -errno;
    close(fd);
    return rc;
  }
  return fd;
}

static int kafs_back_handshake(int fd)
{
  kafs_rpc_hello_t hello;
  memset(&hello, 0, sizeof(hello));
  hello.major = KAFS_RPC_HELLO_MAJOR;
  hello.minor = KAFS_RPC_HELLO_MINOR;
  hello.feature_flags = KAFS_RPC_HELLO_FEATURES;

  int rc = kafs_rpc_send_msg(fd, KAFS_RPC_OP_HELLO, KAFS_RPC_FLAG_ENDIAN_HOST, 0, 0, 0, &hello,
                             (uint32_t)sizeof(hello));
  if (rc != 0)
    return rc;

  kafs_rpc_hdr_t hdr;
  kafs_rpc_session_restore_t restore;
  uint32_t payload_len = 0;
  rc = kafs_rpc_recv_msg(fd, &hdr, &restore, sizeof(restore), &payload_len);
  if (rc != 0)
    return rc;
  if (hdr.op != KAFS_RPC_OP_SESSION_RESTORE)
    return -EBADMSG;
  if (payload_len != sizeof(restore))
    return -EBADMSG;

  return kafs_rpc_send_msg(fd, KAFS_RPC_OP_READY, KAFS_RPC_FLAG_ENDIAN_HOST, 0, hdr.session_id,
                           hdr.epoch, NULL, 0);
}

int main(int argc, char **argv)
{
  const char *fd_env = getenv("KAFS_HOTPLUG_BACK_FD");
  const char *uds_path = getenv("KAFS_HOTPLUG_UDS");
#ifdef KAFS_BACK_ENABLE_IMAGE
  const char *image_path = getenv("KAFS_IMAGE");
#endif
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
#ifdef KAFS_BACK_ENABLE_IMAGE
    if (strcmp(argv[i], "--image") == 0)
    {
      if (i + 1 >= argc)
      {
        usage(argv[0]);
        return 2;
      }
      image_path = argv[++i];
      continue;
    }
#endif
    if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0)
    {
      usage(argv[0]);
      return 0;
    }
  }

  kafs_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));
  ctx.c_fd = -1;

#ifdef KAFS_BACK_ENABLE_IMAGE
  if (!image_path || !*image_path)
  {
    fprintf(stderr, "kafs-back: KAFS_IMAGE (or --image) is required in this build\n");
    return 2;
  }
  int orc = kafs_core_open_image(image_path, &ctx);
  if (orc != 0)
  {
    fprintf(stderr, "kafs-back: failed to open image rc=%d\n", orc);
    return 2;
  }
#endif

  int fd = -1;
  if (fd_env && *fd_env)
  {
    fd = atoi(fd_env);
  }
  else
  {
    int timeout_ms = 5000;
    const char *tout = getenv("KAFS_BACK_CONNECT_TIMEOUT_MS");
    if (tout && *tout)
    {
      char *endp = NULL;
      unsigned long v = strtoul(tout, &endp, 10);
      if (endp && *endp == '\0' && v > 0 && v <= 600000)
        timeout_ms = (int)v;
    }

    int waited = 0;
    const int step_ms = 50;
    for (;;)
    {
      fd = kafs_back_connect_uds(uds_path);
      if (fd >= 0)
        break;

      if (fd != -ENOENT && fd != -ECONNREFUSED)
        break;

      if (waited >= timeout_ms)
        break;

      struct timespec ts;
      ts.tv_sec = 0;
      ts.tv_nsec = step_ms * 1000 * 1000;
      (void)nanosleep(&ts, NULL);
      waited += step_ms;
    }
    if (fd < 0)
    {
      fprintf(stderr, "kafs-back: failed to connect uds '%s' rc=%d\n", uds_path, fd);
#ifdef KAFS_BACK_ENABLE_IMAGE
      kafs_core_close_image(&ctx);
#endif
      return 2;
    }
  }

  int hrc = kafs_back_handshake(fd);
  if (hrc != 0)
  {
    fprintf(stderr, "kafs-back: handshake failed rc=%d\n", hrc);
    close(fd);
#ifdef KAFS_BACK_ENABLE_IMAGE
    kafs_core_close_image(&ctx);
#endif
    return 2;
  }

  int src = kafs_back_rpc_serve(&ctx, fd);
  if (src != 0 && src != -EIO)
    fprintf(stderr, "kafs-back: serve rc=%d\n", src);

  close(fd);
#ifdef KAFS_BACK_ENABLE_IMAGE
  kafs_core_close_image(&ctx);
#endif
  return src == -EIO ? 0 : (src == 0 ? 0 : 2);
}
