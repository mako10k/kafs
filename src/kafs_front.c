#include "kafs_rpc.h"

#include <errno.h>
#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

static volatile sig_atomic_t g_restart_requested = 0;

static void usage(const char *prog)
{
  fprintf(stderr, "Usage: %s [--uds <path>]\n", prog);
}

static void kafs_front_request_restart(int signo)
{
  (void)signo;
  g_restart_requested = 1;
}

static int kafs_front_spawn_back(const char *uds_path, pid_t pgid, pid_t *out_pid, int *out_fd)
{
  int fds[2];
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0)
  {
    perror("socketpair");
    return -errno;
  }

  pid_t pid = fork();
  if (pid < 0)
  {
    perror("fork");
    close(fds[0]);
    close(fds[1]);
    return -errno;
  }
  if (pid == 0)
  {
    if (setpgid(0, pgid) != 0)
    {
      perror("setpgid");
      _exit(127);
    }
    close(fds[0]);
    char fd_buf[32];
    snprintf(fd_buf, sizeof(fd_buf), "%d", fds[1]);
    if (setenv("KAFS_HOTPLUG_BACK_FD", fd_buf, 1) != 0)
    {
      perror("setenv");
      _exit(127);
    }
    if (setenv("KAFS_HOTPLUG_UDS", uds_path, 1) != 0)
    {
      perror("setenv");
      _exit(127);
    }
    char *args[] = {"kafs-back", NULL};
    execvp(args[0], args);
    perror("execvp kafs-back");
    _exit(127);
  }

  if (setpgid(pid, pgid) != 0)
  {
    perror("setpgid");
    close(fds[0]);
    close(fds[1]);
    kill(pid, SIGTERM);
    return -errno;
  }

  close(fds[1]);
  *out_pid = pid;
  *out_fd = fds[0];
  return 0;
}

static int kafs_front_handshake(int cli, uint64_t session_id, uint32_t epoch)
{
  kafs_rpc_hdr_t hdr;
  kafs_rpc_hello_t hello;
  uint32_t payload_len = 0;
  int rc = kafs_rpc_recv_msg(cli, &hdr, &hello, sizeof(hello), &payload_len);
  if (rc != 0 || hdr.op != KAFS_RPC_OP_HELLO)
  {
    fprintf(stderr, "kafs-front: invalid hello rc=%d op=%u\n", rc, (unsigned)hdr.op);
    return rc != 0 ? rc : -EBADMSG;
  }
  if (payload_len != sizeof(hello))
  {
    fprintf(stderr, "kafs-front: hello payload size mismatch\n");
    return -EBADMSG;
  }
  if (hello.major != KAFS_RPC_HELLO_MAJOR || hello.minor != KAFS_RPC_HELLO_MINOR ||
      (hello.feature_flags & ~KAFS_RPC_HELLO_FEATURES) != 0)
  {
    fprintf(stderr, "kafs-front: hello version/feature mismatch\n");
    return -EPROTONOSUPPORT;
  }

  kafs_rpc_session_restore_t restore;
  restore.open_handle_count = 0u;
  uint64_t req_id = kafs_rpc_next_req_id();

  rc = kafs_rpc_send_msg(cli, KAFS_RPC_OP_SESSION_RESTORE, KAFS_RPC_FLAG_ENDIAN_HOST, req_id,
                         session_id, epoch, &restore, sizeof(restore));
  if (rc != 0)
  {
    fprintf(stderr, "kafs-front: failed to send session_restore rc=%d\n", rc);
    return rc;
  }

  kafs_rpc_hdr_t ready_hdr;
  uint32_t ready_len = 0;
  rc = kafs_rpc_recv_msg(cli, &ready_hdr, NULL, 0, &ready_len);
  if (rc != 0 || ready_hdr.op != KAFS_RPC_OP_READY)
  {
    fprintf(stderr, "kafs-front: invalid ready rc=%d op=%u\n", rc, (unsigned)ready_hdr.op);
    return rc != 0 ? rc : -EBADMSG;
  }
  if (ready_len != 0)
  {
    fprintf(stderr, "kafs-front: ready payload size mismatch\n");
    return -EBADMSG;
  }

  fprintf(stderr, "kafs-front: handshake ok (session=%" PRIu64 " epoch=%u)\n", session_id,
          epoch);
  return 0;
}

static int kafs_front_restart_back(const char *uds_path, pid_t pgid, pid_t *pid, int *cli,
                                   uint64_t session_id, uint32_t *epoch)
{
  if (*cli >= 0)
  {
    close(*cli);
    *cli = -1;
  }

  if (*pid > 0)
  {
    (void)kill(*pid, SIGTERM);
    for (int i = 0; i < 20; ++i)
    {
      int status = 0;
      pid_t w = waitpid(*pid, &status, WNOHANG);
      if (w == *pid)
      {
        *pid = -1;
        break;
      }
      usleep(50 * 1000);
    }
    if (*pid > 0)
    {
      (void)kill(*pid, SIGKILL);
      (void)waitpid(*pid, NULL, 0);
      *pid = -1;
    }
  }

  (*epoch)++;
  fprintf(stderr, "kafs-front: restarting kafs-back (epoch=%u)\n", *epoch);

  int rc = kafs_front_spawn_back(uds_path, pgid, pid, cli);
  if (rc != 0)
    return rc;

  rc = kafs_front_handshake(*cli, session_id, *epoch);
  if (rc != 0)
  {
    close(*cli);
    *cli = -1;
    (void)kill(*pid, SIGTERM);
    (void)waitpid(*pid, NULL, 0);
    *pid = -1;
  }
  return rc;
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

  pid_t pgid = getpid();
  if (setpgid(0, pgid) != 0)
  {
    perror("setpgid");
    return 2;
  }

  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = kafs_front_request_restart;
  sigemptyset(&sa.sa_mask);
  if (sigaction(SIGUSR1, &sa, NULL) != 0)
  {
    perror("sigaction(SIGUSR1)");
    return 2;
  }

  pid_t pid = -1;
  int cli = -1;
  int rc = kafs_front_spawn_back(uds_path, pgid, &pid, &cli);
  if (rc != 0)
    return 2;

  uint64_t session_id = 1u;
  uint32_t epoch = 0u;
  rc = kafs_front_handshake(cli, session_id, epoch);
  if (rc != 0)
  {
    close(cli);
    (void)kill(pid, SIGTERM);
    (void)waitpid(pid, NULL, 0);
    return 2;
  }

  for (;;)
  {
    pause();
    if (!g_restart_requested)
      continue;
    g_restart_requested = 0;
    fprintf(stderr, "kafs-front: restart requested\n");
    rc = kafs_front_restart_back(uds_path, pgid, &pid, &cli, session_id, &epoch);
    if (rc != 0)
      fprintf(stderr, "kafs-front: restart failed rc=%d\n", rc);
  }
}
