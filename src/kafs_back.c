#include "kafs_rpc.h"
#include "kafs_cli_opts.h"
#include "kafs_back_server.h"
#include "kafs_context.h"
#include "kafs_crash_diag.h"
#ifdef KAFS_BACK_ENABLE_IMAGE
#include "kafs_core.h"
#endif

#include <errno.h>
#include <inttypes.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

static void usage(const char *prog)
{
#ifdef KAFS_BACK_ENABLE_IMAGE
  fprintf(stderr, "Usage: %s [--fd <num>] [--uds <path>] [--image <path>]\n", prog);
#else
  fprintf(stderr, "Usage: %s [--fd <num>] [--uds <path>]\n", prog);
#endif
}

static int kafs_back_handshake(int fd, uint64_t *out_session_id, uint32_t *out_epoch)
{
  kafs_rpc_hello_t hello;
  memset(&hello, 0, sizeof(hello));
  hello.major = KAFS_RPC_HELLO_MAJOR;
  hello.minor = KAFS_RPC_HELLO_MINOR;
  hello.feature_flags = KAFS_RPC_HELLO_FEATURES;

  uint64_t req_id = kafs_rpc_next_req_id();
  int rc = kafs_rpc_send_msg(fd, KAFS_RPC_OP_HELLO, KAFS_RPC_FLAG_ENDIAN_HOST, req_id, 1u, 0u,
                             &hello, sizeof(hello));
  if (rc != 0)
    return rc;

  kafs_rpc_hdr_t hdr;
  kafs_rpc_session_restore_t restore;
  uint32_t payload_len = 0;
  rc = kafs_rpc_recv_msg(fd, &hdr, &restore, sizeof(restore), &payload_len);
  if (rc != 0 || hdr.op != KAFS_RPC_OP_SESSION_RESTORE)
    return rc != 0 ? rc : -EBADMSG;
  if (payload_len != sizeof(restore))
    return -EBADMSG;

  *out_session_id = hdr.session_id;
  *out_epoch = hdr.epoch;

  uint64_t ready_req_id = kafs_rpc_next_req_id();
  return kafs_rpc_send_msg(fd, KAFS_RPC_OP_READY, KAFS_RPC_FLAG_ENDIAN_HOST, ready_req_id,
                           *out_session_id, *out_epoch, NULL, 0);
}

// Handle common mode gate for READ/WRITE RPCs.
// Returns 1 when plan-only response is fully handled, 0 when caller should continue,
// and negative errno on validation error.
static int kafs_back_prepare_rw_mode(uint32_t data_mode, uint32_t req_size, uint32_t *out_size,
                                     uint32_t *out_resp_len, uint32_t resp_struct_size)
{
  if (data_mode == KAFS_RPC_DATA_PLAN_ONLY)
  {
    *out_size = req_size;
    *out_resp_len = resp_struct_size;
    return 1;
  }

  if (data_mode != KAFS_RPC_DATA_INLINE)
    return -EOPNOTSUPP;

  return 0;
}

static int kafs_back_finalize_rw_result(ssize_t io_len, uint32_t *out_size, uint32_t *out_resp_len,
                                        uint32_t resp_struct_size, int include_data_bytes)
{
  if (io_len < 0)
    return (int)io_len;

  *out_size = (uint32_t)io_len;
  *out_resp_len = resp_struct_size;
  if (include_data_bytes)
    *out_resp_len += (uint32_t)io_len;
  return 0;
}

static int kafs_back_apply_mode_result(int mrc, int *result)
{
  if (mrc > 0)
  {
    *result = 0;
    return 1;
  }
  if (mrc < 0)
  {
    *result = mrc;
    return 1;
  }
  return 0;
}

int kafs_back_rpc_serve(struct kafs_context *ctx, int fd)
{
  uint8_t payload[KAFS_RPC_MAX_PAYLOAD];

  for (;;)
  {
    kafs_rpc_hdr_t req_hdr;
    uint32_t req_len = 0;
    int rc = kafs_rpc_recv_msg(fd, &req_hdr, payload, sizeof(payload), &req_len);
    if (rc != 0)
      return rc;

    int result = -ENOSYS;
    uint8_t resp_buf[KAFS_RPC_MAX_PAYLOAD];
    uint32_t resp_len = 0;
    switch (req_hdr.op)
    {
    case KAFS_RPC_OP_GETATTR:
      if (req_len != sizeof(kafs_rpc_getattr_req_t))
      {
        result = -EBADMSG;
        break;
      }
      else
      {
        kafs_rpc_getattr_req_t *req = (kafs_rpc_getattr_req_t *)payload;
        kafs_rpc_getattr_resp_t *resp = (kafs_rpc_getattr_resp_t *)resp_buf;
#ifdef KAFS_BACK_ENABLE_IMAGE
        int grc = kafs_core_getattr(ctx, (kafs_inocnt_t)req->ino, &resp->st);
        if (grc == 0)
          resp_len = (uint32_t)sizeof(*resp);
        result = grc;
#else
        (void)req;
        (void)resp;
        result = -ENOSYS;
#endif
      }
      break;
    case KAFS_RPC_OP_READ:
      if (req_len != sizeof(kafs_rpc_read_req_t))
      {
        result = -EBADMSG;
        break;
      }
      else
      {
        kafs_rpc_read_req_t *req = (kafs_rpc_read_req_t *)payload;
        kafs_rpc_read_resp_t *resp = (kafs_rpc_read_resp_t *)resp_buf;
        int mrc = kafs_back_prepare_rw_mode(req->data_mode, req->size, &resp->size, &resp_len,
                                            (uint32_t)sizeof(*resp));
        if (kafs_back_apply_mode_result(mrc, &result))
          break;
#ifdef KAFS_BACK_ENABLE_IMAGE
        size_t max_data = KAFS_RPC_MAX_PAYLOAD - sizeof(kafs_rpc_read_resp_t);
        size_t want = req->size;
        if (want > max_data)
          want = max_data;
        ssize_t rlen = kafs_core_read(ctx, (kafs_inocnt_t)req->ino, resp_buf + sizeof(*resp), want,
                                      (off_t)req->off);
        result =
            kafs_back_finalize_rw_result(rlen, &resp->size, &resp_len, (uint32_t)sizeof(*resp), 1);
#else
        (void)resp;
        result = -EOPNOTSUPP;
#endif
      }
      break;
    case KAFS_RPC_OP_WRITE:
      if (req_len < sizeof(kafs_rpc_write_req_t))
      {
        result = -EBADMSG;
        break;
      }
      else
      {
        kafs_rpc_write_req_t *req = (kafs_rpc_write_req_t *)payload;
        uint32_t data_len = req_len - (uint32_t)sizeof(*req);
        kafs_rpc_write_resp_t *resp = (kafs_rpc_write_resp_t *)resp_buf;
        int mrc = kafs_back_prepare_rw_mode(req->data_mode, req->size, &resp->size, &resp_len,
                                            (uint32_t)sizeof(*resp));
        if (kafs_back_apply_mode_result(mrc, &result))
          break;
#ifdef KAFS_BACK_ENABLE_IMAGE
        if (req->size > data_len)
        {
          result = -EBADMSG;
          break;
        }
        ssize_t wlen = kafs_core_write(ctx, (kafs_inocnt_t)req->ino, payload + sizeof(*req),
                                       req->size, (off_t)req->off);
        result =
            kafs_back_finalize_rw_result(wlen, &resp->size, &resp_len, (uint32_t)sizeof(*resp), 0);
#else
        (void)resp;
        (void)data_len;
        result = -EOPNOTSUPP;
#endif
      }
      break;
    case KAFS_RPC_OP_TRUNCATE:
      if (req_len != sizeof(kafs_rpc_truncate_req_t))
      {
        result = -EBADMSG;
        break;
      }
      else
      {
        kafs_rpc_truncate_req_t *req = (kafs_rpc_truncate_req_t *)payload;
        kafs_rpc_truncate_resp_t *resp = (kafs_rpc_truncate_resp_t *)resp_buf;
#ifdef KAFS_BACK_ENABLE_IMAGE
        int trc = kafs_core_truncate(ctx, (kafs_inocnt_t)req->ino, (off_t)req->size);
        if (trc == 0)
        {
          resp->size = req->size;
          resp_len = (uint32_t)sizeof(*resp);
        }
        result = trc;
#else
        (void)req;
        (void)resp;
        result = -ENOSYS;
#endif
      }
      break;
    default:
      result = -ENOSYS;
      break;
    }

    rc = kafs_rpc_send_resp(fd, req_hdr.req_id, result, resp_len ? resp_buf : NULL, resp_len);
    if (rc != 0)
      return rc;
  }
}

int main(int argc, char **argv)
{
  kafs_crash_diag_install("kafs-back");

  int rc = 0;

  kafs_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));
  ctx.c_fd = -1;

  const char *fd_env = getenv("KAFS_HOTPLUG_BACK_FD");
  const char *uds_path = getenv("KAFS_HOTPLUG_UDS");
#ifdef KAFS_BACK_ENABLE_IMAGE
  const char *image_path = getenv("KAFS_IMAGE");
#endif
  if (!uds_path)
    uds_path = "/tmp/kafs-hotplug.sock";

  for (int i = 1; i < argc; ++i)
  {
    if (strcmp(argv[i], "--fd") == 0)
    {
      if (i + 1 >= argc)
      {
        usage(argv[0]);
        return 2;
      }
      fd_env = argv[++i];
      continue;
    }

    int consume_next = 0;
    int exit_code = -1;
    int handled = kafs_cli_parse_uds_help(argv[i], (i + 1 < argc) ? argv[i + 1] : NULL, &uds_path,
                                          &consume_next, &exit_code, usage, argv[0]);
    if (handled)
    {
      if (exit_code >= 0)
        return exit_code;
      if (consume_next)
        ++i;
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
  }

#ifdef KAFS_BACK_ENABLE_IMAGE
  if (!image_path)
  {
    fprintf(stderr, "kafs-back: image path is required (KAFS_IMAGE or --image)\n");
    return 2;
  }

  rc = kafs_core_open_image(image_path, &ctx);
  if (rc != 0)
  {
    fprintf(stderr, "kafs-back: failed to open image rc=%d\n", rc);
    return 2;
  }
#endif

  int fd = -1;
  if (fd_env && fd_env[0] != '\0')
  {
    char *end = NULL;
    long val = strtol(fd_env, &end, 10);
    if (end && *end == '\0' && val >= 0 && val <= INT_MAX)
    {
      int cand = (int)val;
      if (fcntl(cand, F_GETFD) != -1)
        fd = cand;
    }
    if (fd < 0)
      fprintf(stderr, "kafs-back: invalid KAFS_HOTPLUG_BACK_FD, falling back to uds\n");
    else
      fprintf(stderr, "kafs-back: using inherited fd=%d\n", fd);
  }

  if (fd < 0)
  {
    fprintf(stderr, "kafs-back: using uds=%s\n", uds_path);
    fd = socket(AF_UNIX, SOCK_STREAM, 0);
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
  }

  uint64_t session_id = 0;
  uint32_t epoch = 0;
  rc = kafs_back_handshake(fd, &session_id, &epoch);
  if (rc != 0)
  {
    fprintf(stderr, "kafs-back: handshake failed rc=%d\n", rc);
    close(fd);
#ifdef KAFS_BACK_ENABLE_IMAGE
    kafs_core_close_image(&ctx);
#endif
    return 2;
  }

  fprintf(stderr, "kafs-back: handshake ok (session=%" PRIu64 " epoch=%u)\n", session_id, epoch);

  rc = kafs_back_rpc_serve(&ctx, fd);
  if (rc != 0 && rc != -EIO)
    fprintf(stderr, "kafs-back: serve rc=%d\n", rc);

  close(fd);
#ifdef KAFS_BACK_ENABLE_IMAGE
  kafs_core_close_image(&ctx);
#endif
  return rc == -EIO ? 0 : (rc == 0 ? 0 : 2);
}
