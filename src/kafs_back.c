#include "kafs_rpc.h"
#include "kafs_core.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

static void usage(const char *prog)
{
  fprintf(stderr, "Usage: %s [--uds <path>] [--image <path>]\n", prog);
}

int main(int argc, char **argv)
{
  const char *uds_path = getenv("KAFS_HOTPLUG_UDS");
  const char *image_path = getenv("KAFS_IMAGE");
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
    if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0)
    {
      usage(argv[0]);
      return 0;
    }
  }

  if (!image_path)
  {
    fprintf(stderr, "kafs-back: image path is required (KAFS_IMAGE or --image)\n");
    return 2;
  }

  kafs_context_t ctx;
  int rc = kafs_core_open_image(image_path, &ctx);
  if (rc != 0)
  {
    fprintf(stderr, "kafs-back: failed to open image rc=%d\n", rc);
    return 2;
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
  rc = kafs_rpc_send_msg(fd, KAFS_RPC_OP_HELLO, KAFS_RPC_FLAG_ENDIAN_HOST, req_id, 1u, 0u, &hello,
                         sizeof(hello));
  if (rc != 0)
  {
    fprintf(stderr, "kafs-back: failed to send hello rc=%d\n", rc);
    close(fd);
    kafs_core_close_image(&ctx);
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
    kafs_core_close_image(&ctx);
    return 2;
  }
  if (payload_len != sizeof(ready))
  {
    fprintf(stderr, "kafs-back: ready payload size mismatch\n");
    close(fd);
    kafs_core_close_image(&ctx);
    return 2;
  }
  if (ready.major != KAFS_RPC_HELLO_MAJOR || ready.minor != KAFS_RPC_HELLO_MINOR ||
      (ready.feature_flags & ~KAFS_RPC_HELLO_FEATURES) != 0)
  {
    fprintf(stderr, "kafs-back: ready version/feature mismatch\n");
    close(fd);
    kafs_core_close_image(&ctx);
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
        int grc = kafs_core_getattr(&ctx, (kafs_inocnt_t)req->ino, &resp->st);
        if (grc == 0)
          resp_len = (uint32_t)sizeof(*resp);
        result = grc;
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
        if (req->data_mode == KAFS_RPC_DATA_PLAN_ONLY)
        {
          resp->size = req->size;
          resp_len = (uint32_t)sizeof(*resp);
          result = 0;
          break;
        }
        if (req->data_mode != KAFS_RPC_DATA_INLINE)
        {
          result = -EOPNOTSUPP;
          break;
        }
        size_t max_data = KAFS_RPC_MAX_PAYLOAD - sizeof(kafs_rpc_read_resp_t);
        size_t want = req->size;
        if (want > max_data)
          want = max_data;
        ssize_t rlen = kafs_core_read(&ctx, (kafs_inocnt_t)req->ino,
                                      resp_buf + sizeof(*resp), want, (off_t)req->off);
        if (rlen >= 0)
        {
          resp->size = (uint32_t)rlen;
          resp_len = (uint32_t)sizeof(*resp) + (uint32_t)rlen;
          result = 0;
        }
        else
        {
          result = (int)rlen;
        }
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
        if (req->data_mode == KAFS_RPC_DATA_PLAN_ONLY)
        {
          resp->size = req->size;
          resp_len = (uint32_t)sizeof(*resp);
          result = 0;
          break;
        }
        if (req->data_mode != KAFS_RPC_DATA_INLINE)
        {
          result = -EOPNOTSUPP;
          break;
        }
        if (req->size > data_len)
        {
          result = -EBADMSG;
          break;
        }
        ssize_t wlen = kafs_core_write(&ctx, (kafs_inocnt_t)req->ino,
                                       payload + sizeof(*req), req->size, (off_t)req->off);
        if (wlen >= 0)
        {
          resp->size = (uint32_t)wlen;
          resp_len = (uint32_t)sizeof(*resp);
          result = 0;
        }
        else
        {
          result = (int)wlen;
        }
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
        int trc = kafs_core_truncate(&ctx, (kafs_inocnt_t)req->ino, (off_t)req->size);
        if (trc == 0)
        {
          resp->size = req->size;
          resp_len = (uint32_t)sizeof(*resp);
        }
        result = trc;
      }
      break;
    default:
      result = -ENOSYS;
      break;
    }

    rc = kafs_rpc_send_resp(fd, req_hdr.req_id, result, resp_len ? resp_buf : NULL, resp_len);
    if (rc != 0)
    {
      fprintf(stderr, "kafs-back: send resp rc=%d\n", rc);
      break;
    }
  }

  close(fd);
  kafs_core_close_image(&ctx);
  return 0;
}
