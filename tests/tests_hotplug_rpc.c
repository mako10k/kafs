#include "kafs_rpc.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

static int test_resp_roundtrip(void)
{
  int fds[2];
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0)
    return -errno;

  const char payload[] = "abc";
  int rc = kafs_rpc_send_resp(fds[0], 42u, 0, payload, (uint32_t)strlen(payload));
  if (rc != 0)
  {
    close(fds[0]);
    close(fds[1]);
    return rc;
  }

  kafs_rpc_resp_hdr_t hdr;
  char buf[8];
  uint32_t len = 0;
  rc = kafs_rpc_recv_resp(fds[1], &hdr, buf, sizeof(buf), &len);
  close(fds[0]);
  close(fds[1]);
  if (rc != 0)
    return rc;
  if (hdr.req_id != 42u || hdr.result != 0)
    return -EBADMSG;
  if (len != strlen(payload))
    return -EBADMSG;
  if (memcmp(buf, payload, len) != 0)
    return -EBADMSG;
  return 0;
}

static int test_resp_too_large(void)
{
  int fds[2];
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0)
    return -errno;
  char big[KAFS_RPC_MAX_PAYLOAD + 1];
  memset(big, 'x', sizeof(big));
  int rc = kafs_rpc_send_resp(fds[0], 1u, 0, big, (uint32_t)sizeof(big));
  close(fds[0]);
  close(fds[1]);
  return rc == -EMSGSIZE ? 0 : -EINVAL;
}

int main(void)
{
  int rc = test_resp_roundtrip();
  if (rc != 0)
  {
    fprintf(stderr, "hotplug_rpc: roundtrip failed rc=%d\n", rc);
    return 1;
  }
  rc = test_resp_too_large();
  if (rc != 0)
  {
    fprintf(stderr, "hotplug_rpc: size guard failed rc=%d\n", rc);
    return 1;
  }
  printf("hotplug_rpc OK\n");
  return 0;
}
