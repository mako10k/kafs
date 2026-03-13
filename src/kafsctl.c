#include "kafs_ioctl.h"
#include "kafs_rpc.h"
#include "kafs_superblock.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <limits.h>
#include <sys/stat.h>
#include <time.h>

typedef enum
{
  KAFS_UNIT_KIB = 0,
  KAFS_UNIT_BYTES,
  KAFS_UNIT_MIB,
  KAFS_UNIT_GIB,
} kafs_unit_t;

static double unit_divisor(kafs_unit_t u)
{
  switch (u)
  {
  case KAFS_UNIT_BYTES:
    return 1.0;
  case KAFS_UNIT_MIB:
    return 1024.0 * 1024.0;
  case KAFS_UNIT_GIB:
    return 1024.0 * 1024.0 * 1024.0;
  case KAFS_UNIT_KIB:
  default:
    return 1024.0;
  }
}

static const char *unit_suffix(kafs_unit_t u)
{
  switch (u)
  {
  case KAFS_UNIT_BYTES:
    return "B";
  case KAFS_UNIT_MIB:
    return "MiB";
  case KAFS_UNIT_GIB:
    return "GiB";
  case KAFS_UNIT_KIB:
  default:
    return "KiB";
  }
}

static void print_bytes(uint64_t bytes, kafs_unit_t unit)
{
  if (unit == KAFS_UNIT_BYTES)
  {
    printf("%" PRIu64 "B", bytes);
    return;
  }

  const double v = (double)bytes / unit_divisor(unit);
  if (v >= 100.0)
    printf("%.0f%s", v, unit_suffix(unit));
  else
    printf("%.1f%s", v, unit_suffix(unit));
}

static double pct_u64(uint64_t used, uint64_t total)
{
  if (total == 0)
    return 0.0;
  return ((double)used * 100.0) / (double)total;
}

static void usage(const char *prog)
{
  fprintf(stderr,
          "Usage:\n"
          "  %s migrate <image> [--yes]\n"
          "  %s fsstat <mountpoint> [--json] [--bytes|--mib|--gib]   (alias: stats)\n"
          "  %s hotplug status <mountpoint> [--json]\n"
          "  %s hotplug restart-back <mountpoint>\n"
          "  %s hotplug compat <mountpoint> [--json]\n"
          "  %s hotplug set-timeout <mountpoint> <ms>\n"
          "  %s hotplug set-dedup-priority <mountpoint> <normal|idle> [nice(0..19)]\n"
          "  %s hotplug set-runtime <mountpoint> [--fsync-policy=<journal_only|full|adaptive>]"
          " [--pending-ttl-soft-ms=<ms>] [--pending-ttl-hard-ms=<ms>]\n"
          "  %s hotplug env list <mountpoint>\n"
          "  %s hotplug env set <mountpoint> <key>=<value>\n"
          "  %s hotplug env unset <mountpoint> <key>\n"
          "  %s stat <mountpoint> <path>\n"
          "  %s cat <mountpoint> <path>\n"
          "  %s write <mountpoint> <path>   (stdin -> file, trunc)\n"
          "  %s cp <mountpoint> <src> <dst> [--reflink]\n"
          "  %s mv <mountpoint> <src> <dst>\n"
          "  %s rm <mountpoint> <path>\n"
          "  %s mkdir <mountpoint> <path>\n"
          "  %s rmdir <mountpoint> <path>\n"
          "  %s ln <mountpoint> <src> <dst>\n"
          "  %s symlink <mountpoint> <target> <linkpath>\n"
          "  %s readlink <mountpoint> <path>\n"
          "  %s chmod <mountpoint> <octal_mode> <path>\n"
          "  %s touch <mountpoint> <path>\n",
          prog, prog, prog, prog, prog, prog, prog, prog, prog, prog, prog, prog, prog, prog, prog,
          prog, prog, prog, prog, prog, prog, prog, prog, prog);
}

static int confirm_yes_stdin(void)
{
  fprintf(stderr, "WARNING: migration is irreversible. type 'YES' to continue: ");
  fflush(stderr);
  char buf[32];
  if (!fgets(buf, sizeof(buf), stdin))
    return 0;
  buf[strcspn(buf, "\r\n")] = '\0';
  return strcmp(buf, "YES") == 0;
}

static int cmd_migrate(const char *image, int assume_yes)
{
  if (!image || !*image)
  {
    fprintf(stderr, "invalid image path\n");
    return 2;
  }

  int fd = open(image, O_RDWR, 0);
  if (fd < 0)
  {
    perror("open");
    return 1;
  }

  kafs_ssuperblock_t sb;
  if (pread(fd, &sb, sizeof(sb), 0) != (ssize_t)sizeof(sb))
  {
    perror("pread superblock");
    close(fd);
    return 1;
  }
  if (kafs_sb_magic_get(&sb) != KAFS_MAGIC)
  {
    fprintf(stderr, "invalid magic: not a KAFS image\n");
    close(fd);
    return 2;
  }

  uint32_t fmt = kafs_sb_format_version_get(&sb);
  if (fmt == KAFS_FORMAT_VERSION)
  {
    fprintf(stderr, "already v%u: no migration needed\n", (unsigned)KAFS_FORMAT_VERSION);
    close(fd);
    return 0;
  }
  if (fmt != KAFS_FORMAT_VERSION_V2)
  {
    fprintf(stderr, "unsupported format version: %u\n", (unsigned)fmt);
    close(fd);
    return 2;
  }

  if (!assume_yes && !confirm_yes_stdin())
  {
    fprintf(stderr, "migration canceled\n");
    close(fd);
    return 2;
  }

  kafs_sb_format_version_set(&sb, KAFS_FORMAT_VERSION);
  if (kafs_sb_allocator_size_get(&sb) > 0)
  {
    if (kafs_sb_allocator_version_get(&sb) < 2u)
      kafs_sb_allocator_version_set(&sb, 2u);
    uint64_t ff = kafs_sb_feature_flags_get(&sb);
    kafs_sb_feature_flags_set(&sb, ff | KAFS_FEATURE_ALLOC_V2);
  }

  if (pwrite(fd, &sb, sizeof(sb), 0) != (ssize_t)sizeof(sb))
  {
    perror("pwrite superblock");
    close(fd);
    return 1;
  }
  if (fsync(fd) != 0)
  {
    perror("fsync");
    close(fd);
    return 1;
  }

  close(fd);
  fprintf(stderr, "migration completed: v2 -> v%u (%s)\n", (unsigned)KAFS_FORMAT_VERSION, image);
  return 0;
}

static const char *hotplug_state_str(uint32_t state)
{
  switch (state)
  {
  case KAFS_HOTPLUG_STATE_DISABLED:
    return "disabled";
  case KAFS_HOTPLUG_STATE_WAITING:
    return "waiting";
  case KAFS_HOTPLUG_STATE_CONNECTED:
    return "connected";
  case KAFS_HOTPLUG_STATE_ERROR:
    return "error";
  default:
    return "unknown";
  }
}

static const char *hotplug_data_mode_str(uint32_t mode)
{
  switch (mode)
  {
  case KAFS_RPC_DATA_INLINE:
    return "inline";
  case KAFS_RPC_DATA_PLAN_ONLY:
    return "plan_only";
  case KAFS_RPC_DATA_SHM:
    return "shm";
  default:
    return "unknown";
  }
}

static const char *hotplug_compat_str(uint32_t result)
{
  switch (result)
  {
  case KAFS_HOTPLUG_COMPAT_OK:
    return "ok";
  case KAFS_HOTPLUG_COMPAT_WARN:
    return "warn";
  case KAFS_HOTPLUG_COMPAT_REJECT:
    return "reject";
  case KAFS_HOTPLUG_COMPAT_UNKNOWN:
  default:
    return "unknown";
  }
}

static const char *hotplug_compat_reason_str(int32_t reason)
{
  switch (reason)
  {
  case 0:
    return "ok";
  case -EPROTONOSUPPORT:
    return "protocol_mismatch";
  case -EBADMSG:
    return "bad_message";
  default:
    return "unknown";
  }
}

static const char *pending_worker_prio_mode_str(uint32_t mode)
{
  switch (mode)
  {
  case KAFS_PENDING_WORKER_PRIO_IDLE:
    return "idle";
  case KAFS_PENDING_WORKER_PRIO_NORMAL:
  default:
    return "normal";
  }
}

static const char *bg_dedup_mode_str(uint32_t mode)
{
  switch (mode)
  {
  case 1:
    return "cold";
  case 2:
    return "adaptive";
  case 3:
    return "pressure";
  default:
    return "unknown";
  }
}

static int hotplug_ctl_exchange(const char *mnt, uint16_t op, const void *req, uint32_t req_len,
                                void *resp, uint32_t resp_cap, uint32_t *resp_len,
                                int32_t *resp_result);
static void hotplug_print_exchange_error(const char *op, const char *mnt, int rc);

static const char *fsync_policy_str(uint32_t policy)
{
  switch (policy)
  {
  case KAFS_FSYNC_POLICY_FULL:
    return "full";
  case KAFS_FSYNC_POLICY_ADAPTIVE:
    return "adaptive";
  case KAFS_FSYNC_POLICY_JOURNAL_ONLY:
  default:
    return "journal_only";
  }
}

static int parse_fsync_policy(const char *s, uint32_t *out)
{
  if (!s || !out)
    return -EINVAL;
  if (strcmp(s, "full") == 0)
  {
    *out = KAFS_FSYNC_POLICY_FULL;
    return 0;
  }
  if (strcmp(s, "journal_only") == 0 || strcmp(s, "journal-only") == 0 || strcmp(s, "journal") == 0)
  {
    *out = KAFS_FSYNC_POLICY_JOURNAL_ONLY;
    return 0;
  }
  if (strcmp(s, "adaptive") == 0)
  {
    *out = KAFS_FSYNC_POLICY_ADAPTIVE;
    return 0;
  }
  return -EINVAL;
}

static int cmd_hotplug_set_runtime(const char *mnt, int argc, char **argv)
{
  kafs_rpc_set_runtime_t req;
  memset(&req, 0, sizeof(req));

  for (int i = 0; i < argc; ++i)
  {
    const char *a = argv[i];
    if (strncmp(a, "--fsync-policy=", 15) == 0)
    {
      if (parse_fsync_policy(a + 15, &req.fsync_policy) != 0)
      {
        fprintf(stderr, "invalid fsync policy: %s\n", a + 15);
        return 2;
      }
      req.flags |= KAFS_RPC_SET_RUNTIME_F_HAS_FSYNC_POLICY;
      continue;
    }
    if (strncmp(a, "--pending-ttl-soft-ms=", 22) == 0)
    {
      char *endp = NULL;
      unsigned long v = strtoul(a + 22, &endp, 10);
      if (!endp || *endp != '\0' || v > UINT32_MAX)
      {
        fprintf(stderr, "invalid pending ttl soft: %s\n", a + 22);
        return 2;
      }
      req.pending_ttl_soft_ms = (uint32_t)v;
      req.flags |= KAFS_RPC_SET_RUNTIME_F_HAS_PENDING_TTL_SOFT_MS;
      continue;
    }
    if (strncmp(a, "--pending-ttl-hard-ms=", 22) == 0)
    {
      char *endp = NULL;
      unsigned long v = strtoul(a + 22, &endp, 10);
      if (!endp || *endp != '\0' || v > UINT32_MAX)
      {
        fprintf(stderr, "invalid pending ttl hard: %s\n", a + 22);
        return 2;
      }
      req.pending_ttl_hard_ms = (uint32_t)v;
      req.flags |= KAFS_RPC_SET_RUNTIME_F_HAS_PENDING_TTL_HARD_MS;
      continue;
    }

    fprintf(stderr, "unknown option: %s\n", a);
    return 2;
  }

  if (req.flags == 0)
  {
    fprintf(stderr, "set-runtime requires at least one option\n");
    return 2;
  }

  if ((req.flags & KAFS_RPC_SET_RUNTIME_F_HAS_PENDING_TTL_SOFT_MS) != 0 &&
      (req.flags & KAFS_RPC_SET_RUNTIME_F_HAS_PENDING_TTL_HARD_MS) != 0 &&
      req.pending_ttl_soft_ms > 0 && req.pending_ttl_hard_ms > 0 &&
      req.pending_ttl_hard_ms < req.pending_ttl_soft_ms)
  {
    fprintf(stderr, "invalid ttl: hard must be >= soft\n");
    return 2;
  }

  uint32_t resp_len = 0;
  int32_t resp_result = 0;
  int rc = hotplug_ctl_exchange(mnt, KAFS_RPC_OP_CTL_SET_RUNTIME, &req, sizeof(req), NULL, 0,
                                &resp_len, &resp_result);
  if (rc != 0)
  {
    hotplug_print_exchange_error("set-runtime", mnt, rc);
    return 1;
  }
  if (resp_result != 0)
  {
    fprintf(stderr, "hotplug set-runtime failed: %s\n", strerror(-resp_result));
    return 1;
  }
  return 0;
}

#define KAFS_CTL_REL ".kafs.sock"

static int write_full(int fd, const void *buf, size_t len)
{
  const unsigned char *p = (const unsigned char *)buf;
  size_t done = 0;
  while (done < len)
  {
    ssize_t w = write(fd, p + done, len - done);
    if (w < 0)
      return -errno;
    if (w == 0)
      return -EIO;
    done += (size_t)w;
  }
  return 0;
}

static int read_full(int fd, unsigned char *buf, size_t len)
{
  size_t done = 0;
  while (done < len)
  {
    ssize_t r = read(fd, buf + done, len - done);
    if (r < 0)
      return -errno;
    if (r == 0)
      return -EIO;
    done += (size_t)r;
  }
  return 0;
}

static uint64_t next_req_id(void)
{
  static uint64_t rid = 0;
  return __atomic_add_fetch(&rid, 1u, __ATOMIC_RELAXED);
}

static int hotplug_ctl_exchange(const char *mnt, uint16_t op, const void *req, uint32_t req_len,
                                void *resp, uint32_t resp_cap, uint32_t *resp_len,
                                int32_t *resp_result)
{
  if (req_len > KAFS_RPC_MAX_PAYLOAD)
    return -EMSGSIZE;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
    return -errno;

  int fd = openat(dfd, KAFS_CTL_REL, O_RDWR);
  if (fd < 0)
  {
    int err = -errno;
    close(dfd);
    return err;
  }

  unsigned char sbuf[sizeof(kafs_rpc_hdr_t) + KAFS_RPC_MAX_PAYLOAD];
  kafs_rpc_hdr_t hdr;
  hdr.magic = KAFS_RPC_MAGIC;
  hdr.version = KAFS_RPC_VERSION;
  hdr.op = op;
  hdr.flags = KAFS_RPC_FLAG_ENDIAN_HOST;
  hdr.req_id = next_req_id();
  hdr.session_id = 0;
  hdr.epoch = 0;
  hdr.payload_len = req_len;
  memcpy(sbuf, &hdr, sizeof(hdr));
  if (req_len && req)
    memcpy(sbuf + sizeof(hdr), req, req_len);

  int rc = write_full(fd, sbuf, sizeof(hdr) + req_len);
  if (rc != 0)
  {
    close(fd);
    close(dfd);
    return rc;
  }

  (void)lseek(fd, 0, SEEK_SET);

  unsigned char rbuf[sizeof(kafs_rpc_resp_hdr_t) + KAFS_RPC_MAX_PAYLOAD];
  kafs_rpc_resp_hdr_t rhdr;
  rc = read_full(fd, (unsigned char *)&rhdr, sizeof(rhdr));
  if (rc != 0)
  {
    close(fd);
    close(dfd);
    return rc;
  }

  if (rhdr.payload_len > KAFS_RPC_MAX_PAYLOAD)
  {
    close(fd);
    close(dfd);
    return -EMSGSIZE;
  }

  if (rhdr.payload_len)
  {
    rc = read_full(fd, rbuf, rhdr.payload_len);
    if (rc != 0)
    {
      close(fd);
      close(dfd);
      return rc;
    }
  }

  close(fd);
  close(dfd);

  if (resp_len)
    *resp_len = rhdr.payload_len;
  if (resp_result)
    *resp_result = rhdr.result;

  if (rhdr.payload_len && resp && resp_cap >= rhdr.payload_len)
    memcpy(resp, rbuf, rhdr.payload_len);
  else if (rhdr.payload_len && resp_cap < rhdr.payload_len)
    return -EMSGSIZE;

  if (rhdr.req_id != hdr.req_id)
    return -EBADMSG;
  return 0;
}

static void hotplug_status_from_rpc(kafs_hotplug_status_t *out, const kafs_rpc_hotplug_status_t *in)
{
  memset(out, 0, sizeof(*out));
  out->struct_size = (uint32_t)sizeof(*out);
  out->version = in->version;
  out->state = in->state;
  out->data_mode = in->data_mode;
  out->session_id = in->session_id;
  out->epoch = in->epoch;
  out->last_error = in->last_error;
  out->wait_queue_len = in->wait_queue_len;
  out->wait_timeout_ms = in->wait_timeout_ms;
  out->wait_queue_limit = in->wait_queue_limit;
  out->front_major = in->front_major;
  out->front_minor = in->front_minor;
  out->front_features = in->front_features;
  out->back_major = in->back_major;
  out->back_minor = in->back_minor;
  out->back_features = in->back_features;
  out->compat_result = in->compat_result;
  out->compat_reason = in->compat_reason;
  out->pending_worker_prio_mode = in->pending_worker_prio_mode;
  out->pending_worker_nice = in->pending_worker_nice;
  out->pending_worker_prio_apply_error = in->pending_worker_prio_apply_error;
  out->fsync_policy = in->fsync_policy;
  out->pending_ttl_soft_ms = in->pending_ttl_soft_ms;
  out->pending_ttl_hard_ms = in->pending_ttl_hard_ms;
  out->pending_oldest_age_ms = in->pending_oldest_age_ms;
  out->pending_ttl_over_soft = in->pending_ttl_over_soft;
  out->pending_ttl_over_hard = in->pending_ttl_over_hard;
}

static int get_hotplug_status(const char *mnt, kafs_hotplug_status_t *out)
{
  if (!out)
    return -EINVAL;
  kafs_rpc_hotplug_status_t st;
  uint32_t resp_len = 0;
  int32_t resp_result = 0;
  int rc = hotplug_ctl_exchange(mnt, KAFS_RPC_OP_CTL_STATUS, NULL, 0, &st, sizeof(st), &resp_len,
                                &resp_result);
  if (rc != 0)
    return rc;
  if (resp_result != 0)
    return resp_result;
  if (resp_len != sizeof(st))
    return -EBADMSG;
  hotplug_status_from_rpc(out, &st);
  return 0;
}

static void hotplug_print_exchange_error(const char *op, const char *mnt, int rc)
{
  if (rc == -ENOENT)
  {
    fprintf(stderr,
            "hotplug %s failed: %s (control endpoint '/.kafs.sock' not found under mountpoint "
            "'%s'; ensure it is an active KAFS mount)\n",
            op, strerror(ENOENT), mnt ? mnt : "(null)");
    return;
  }
  if (rc == -ENOSYS)
  {
    fprintf(stderr,
            "hotplug %s failed: %s (hotplug control is disabled on this mount; restart KAFS with "
            "KAFS_HOTPLUG_UDS set, then run kafs-back)\n",
            op, strerror(ENOSYS));
    return;
  }
  fprintf(stderr, "hotplug %s failed: %s\n", op, strerror(-rc));
}

static int cmd_hotplug_status(const char *mnt, int json)
{
  kafs_hotplug_status_t st;
  int rc = get_hotplug_status(mnt, &st);
  if (rc != 0)
  {
    hotplug_print_exchange_error("status", mnt, rc);
    return 1;
  }

  if (json)
  {
    printf("{\n");
    printf("  \"version\": %u,\n", st.version);
    printf("  \"state\": %u,\n", st.state);
    printf("  \"state_str\": \"%s\",\n", hotplug_state_str(st.state));
    printf("  \"data_mode\": %u,\n", st.data_mode);
    printf("  \"data_mode_str\": \"%s\",\n", hotplug_data_mode_str(st.data_mode));
    printf("  \"session_id\": %" PRIu64 ",\n", st.session_id);
    printf("  \"epoch\": %u,\n", st.epoch);
    printf("  \"last_error\": %d,\n", st.last_error);
    printf("  \"wait_queue_len\": %u,\n", st.wait_queue_len);
    printf("  \"wait_timeout_ms\": %u,\n", st.wait_timeout_ms);
    printf("  \"wait_queue_limit\": %u,\n", st.wait_queue_limit);
    printf("  \"front_major\": %u,\n", st.front_major);
    printf("  \"front_minor\": %u,\n", st.front_minor);
    printf("  \"front_features\": %u,\n", st.front_features);
    printf("  \"back_major\": %u,\n", st.back_major);
    printf("  \"back_minor\": %u,\n", st.back_minor);
    printf("  \"back_features\": %u,\n", st.back_features);
    printf("  \"compat_result\": %u,\n", st.compat_result);
    printf("  \"compat_result_str\": \"%s\",\n", hotplug_compat_str(st.compat_result));
    printf("  \"compat_reason\": %d,\n", st.compat_reason);
    printf("  \"compat_reason_str\": \"%s\",\n", hotplug_compat_reason_str(st.compat_reason));
    printf("  \"pending_worker_prio_mode\": %u,\n", st.pending_worker_prio_mode);
    printf("  \"pending_worker_prio_mode_str\": \"%s\",\n",
           pending_worker_prio_mode_str(st.pending_worker_prio_mode));
    printf("  \"pending_worker_nice\": %d,\n", st.pending_worker_nice);
    printf("  \"pending_worker_prio_apply_error\": %d,\n", st.pending_worker_prio_apply_error);
    printf("  \"fsync_policy\": %u,\n", st.fsync_policy);
    printf("  \"fsync_policy_str\": \"%s\",\n", fsync_policy_str(st.fsync_policy));
    printf("  \"pending_ttl_soft_ms\": %u,\n", st.pending_ttl_soft_ms);
    printf("  \"pending_ttl_hard_ms\": %u,\n", st.pending_ttl_hard_ms);
    printf("  \"pending_oldest_age_ms\": %" PRIu64 ",\n", st.pending_oldest_age_ms);
    printf("  \"pending_ttl_over_soft\": %u,\n", st.pending_ttl_over_soft);
    printf("  \"pending_ttl_over_hard\": %u\n", st.pending_ttl_over_hard);
    printf("}\n");
    return 0;
  }

  printf("kafs hotplug status v%u\n", st.version);
  printf("  state: %u (%s)\n", st.state, hotplug_state_str(st.state));
  printf("  data_mode: %u (%s)\n", st.data_mode, hotplug_data_mode_str(st.data_mode));
  printf("  session_id: %" PRIu64 "\n", st.session_id);
  printf("  epoch: %u\n", st.epoch);
  printf("  last_error: %d\n", st.last_error);
  printf("  wait_queue_len: %u\n", st.wait_queue_len);
  printf("  wait_timeout_ms: %u\n", st.wait_timeout_ms);
  printf("  wait_queue_limit: %u\n", st.wait_queue_limit);
  printf("  front_version: %u.%u\n", st.front_major, st.front_minor);
  printf("  front_features: %u\n", st.front_features);
  printf("  back_version: %u.%u\n", st.back_major, st.back_minor);
  printf("  back_features: %u\n", st.back_features);
  printf("  compat_result: %u (%s)\n", st.compat_result, hotplug_compat_str(st.compat_result));
  printf("  compat_reason: %d (%s)\n", st.compat_reason,
         hotplug_compat_reason_str(st.compat_reason));
  printf("  pending_worker_prio_mode: %u (%s)\n", st.pending_worker_prio_mode,
         pending_worker_prio_mode_str(st.pending_worker_prio_mode));
  printf("  pending_worker_nice: %d\n", st.pending_worker_nice);
  printf("  pending_worker_prio_apply_error: %d\n", st.pending_worker_prio_apply_error);
  printf("  fsync_policy: %u (%s)\n", st.fsync_policy, fsync_policy_str(st.fsync_policy));
  printf("  pending_ttl_soft_ms: %u\n", st.pending_ttl_soft_ms);
  printf("  pending_ttl_hard_ms: %u\n", st.pending_ttl_hard_ms);
  printf("  pending_oldest_age_ms: %" PRIu64 "\n", st.pending_oldest_age_ms);
  printf("  pending_ttl_over_soft: %u\n", st.pending_ttl_over_soft);
  printf("  pending_ttl_over_hard: %u\n", st.pending_ttl_over_hard);
  return 0;
}

static int cmd_hotplug_restart(const char *mnt)
{
  uint32_t resp_len = 0;
  int32_t resp_result = 0;
  int rc =
      hotplug_ctl_exchange(mnt, KAFS_RPC_OP_CTL_RESTART, NULL, 0, NULL, 0, &resp_len, &resp_result);
  if (rc != 0)
  {
    hotplug_print_exchange_error("restart", mnt, rc);
    return 1;
  }
  if (resp_result != 0)
  {
    fprintf(stderr, "hotplug restart failed: %s\n", strerror(-resp_result));
    return 1;
  }
  return 0;
}

static int cmd_hotplug_compat(const char *mnt, int json)
{
  kafs_hotplug_status_t st;
  int rc = get_hotplug_status(mnt, &st);
  if (rc != 0)
  {
    hotplug_print_exchange_error("compat", mnt, rc);
    return 1;
  }

  if (json)
  {
    printf("{\n");
    printf("  \"front_major\": %u,\n", st.front_major);
    printf("  \"front_minor\": %u,\n", st.front_minor);
    printf("  \"front_features\": %u,\n", st.front_features);
    printf("  \"back_major\": %u,\n", st.back_major);
    printf("  \"back_minor\": %u,\n", st.back_minor);
    printf("  \"back_features\": %u,\n", st.back_features);
    printf("  \"compat_result\": %u,\n", st.compat_result);
    printf("  \"compat_result_str\": \"%s\",\n", hotplug_compat_str(st.compat_result));
    printf("  \"compat_reason\": %d,\n", st.compat_reason);
    printf("  \"compat_reason_str\": \"%s\"\n", hotplug_compat_reason_str(st.compat_reason));
    printf("}\n");
    return 0;
  }

  printf("kafs hotplug compat\n");
  printf("  front_version: %u.%u\n", st.front_major, st.front_minor);
  printf("  front_features: %u\n", st.front_features);
  printf("  back_version: %u.%u\n", st.back_major, st.back_minor);
  printf("  back_features: %u\n", st.back_features);
  printf("  compat_result: %u (%s)\n", st.compat_result, hotplug_compat_str(st.compat_result));
  printf("  compat_reason: %d (%s)\n", st.compat_reason,
         hotplug_compat_reason_str(st.compat_reason));
  return 0;
}

static int cmd_hotplug_set_timeout(const char *mnt, const char *timeout_str)
{
  if (!timeout_str || *timeout_str == '\0')
  {
    fprintf(stderr, "invalid timeout\n");
    return 2;
  }
  char *endp = NULL;
  unsigned long v = strtoul(timeout_str, &endp, 10);
  if (!endp || *endp != '\0' || v == 0 || v > UINT32_MAX)
  {
    fprintf(stderr, "invalid timeout\n");
    return 2;
  }

  kafs_rpc_set_timeout_t req;
  req.timeout_ms = (uint32_t)v;
  uint32_t resp_len = 0;
  int32_t resp_result = 0;
  int rc = hotplug_ctl_exchange(mnt, KAFS_RPC_OP_CTL_SET_TIMEOUT, &req, sizeof(req), NULL, 0,
                                &resp_len, &resp_result);
  if (rc != 0)
  {
    hotplug_print_exchange_error("set-timeout", mnt, rc);
    return 1;
  }
  if (resp_result != 0)
  {
    fprintf(stderr, "hotplug set-timeout failed: %s\n", strerror(-resp_result));
    return 1;
  }
  return 0;
}

static int cmd_hotplug_set_dedup_priority(const char *mnt, const char *mode_str,
                                          const char *nice_str)
{
  if (!mode_str)
  {
    fprintf(stderr, "invalid mode\n");
    return 2;
  }

  kafs_rpc_set_dedup_prio_t req;
  memset(&req, 0, sizeof(req));
  if (strcmp(mode_str, "normal") == 0)
    req.mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
  else if (strcmp(mode_str, "idle") == 0)
    req.mode = KAFS_PENDING_WORKER_PRIO_IDLE;
  else
  {
    fprintf(stderr, "invalid mode: %s\n", mode_str);
    return 2;
  }

  if (nice_str)
  {
    char *endp = NULL;
    long v = strtol(nice_str, &endp, 10);
    if (!endp || *endp != '\0' || v < 0 || v > 19)
    {
      fprintf(stderr, "invalid nice value: %s\n", nice_str);
      return 2;
    }
    req.flags |= KAFS_RPC_SET_DEDUP_PRIO_F_HAS_NICE;
    req.nice_value = (int32_t)v;
  }

  uint32_t resp_len = 0;
  int32_t resp_result = 0;
  int rc = hotplug_ctl_exchange(mnt, KAFS_RPC_OP_CTL_SET_DEDUP_PRIO, &req, sizeof(req), NULL, 0,
                                &resp_len, &resp_result);
  if (rc != 0)
  {
    hotplug_print_exchange_error("set-dedup-priority", mnt, rc);
    return 1;
  }
  if (resp_result != 0)
  {
    fprintf(stderr, "hotplug set-dedup-priority failed: %s\n", strerror(-resp_result));
    return 1;
  }
  return 0;
}

static int cmd_hotplug_env_list(const char *mnt)
{
  kafs_rpc_env_list_t env;
  uint32_t resp_len = 0;
  int32_t resp_result = 0;
  int rc = hotplug_ctl_exchange(mnt, KAFS_RPC_OP_CTL_ENV_LIST, NULL, 0, &env, sizeof(env),
                                &resp_len, &resp_result);
  if (rc != 0)
  {
    hotplug_print_exchange_error("env list", mnt, rc);
    return 1;
  }
  if (resp_result != 0)
  {
    fprintf(stderr, "hotplug env list failed: %s\n", strerror(-resp_result));
    return 1;
  }
  if (resp_len != sizeof(env))
  {
    fprintf(stderr, "hotplug env list failed: %s\n", strerror(EBADMSG));
    return 1;
  }

  printf("kafs hotplug env list\n");
  for (uint32_t i = 0; i < env.count && i < KAFS_HOTPLUG_ENV_MAX; ++i)
    printf("  %s=%s\n", env.entries[i].key, env.entries[i].value);
  return 0;
}

static int cmd_hotplug_env_set(const char *mnt, const char *kv)
{
  if (!kv)
  {
    fprintf(stderr, "invalid key=value\n");
    return 2;
  }
  const char *eq = strchr(kv, '=');
  if (!eq || eq == kv)
  {
    fprintf(stderr, "invalid key=value\n");
    return 2;
  }

  kafs_rpc_env_update_t req;
  memset(&req, 0, sizeof(req));
  snprintf(req.key, sizeof(req.key), "%.*s", (int)(eq - kv), kv);
  snprintf(req.value, sizeof(req.value), "%s", eq + 1);
  uint32_t resp_len = 0;
  int32_t resp_result = 0;
  int rc = hotplug_ctl_exchange(mnt, KAFS_RPC_OP_CTL_ENV_SET, &req, sizeof(req), NULL, 0, &resp_len,
                                &resp_result);
  if (rc != 0)
  {
    hotplug_print_exchange_error("env set", mnt, rc);
    return 1;
  }
  if (resp_result != 0)
  {
    fprintf(stderr, "hotplug env set failed: %s\n", strerror(-resp_result));
    return 1;
  }
  return 0;
}

static int cmd_hotplug_env_unset(const char *mnt, const char *key)
{
  if (!key || *key == '\0')
  {
    fprintf(stderr, "invalid key\n");
    return 2;
  }
  kafs_rpc_env_update_t req;
  memset(&req, 0, sizeof(req));
  snprintf(req.key, sizeof(req.key), "%s", key);
  uint32_t resp_len = 0;
  int32_t resp_result = 0;
  int rc = hotplug_ctl_exchange(mnt, KAFS_RPC_OP_CTL_ENV_UNSET, &req, sizeof(req), NULL, 0,
                                &resp_len, &resp_result);
  if (rc != 0)
  {
    hotplug_print_exchange_error("env unset", mnt, rc);
    return 1;
  }
  if (resp_result != 0)
  {
    fprintf(stderr, "hotplug env unset failed: %s\n", strerror(-resp_result));
    return 1;
  }
  return 0;
}

static int path_has_dotdot_component(const char *p)
{
  if (!p)
    return 0;
  const char *s = p;
  while (*s)
  {
    while (*s == '/')
      ++s;
    const char *c = s;
    while (*s && *s != '/')
      ++s;
    size_t len = (size_t)(s - c);
    if (len == 2 && c[0] == '.' && c[1] == '.')
      return 1;
  }
  return 0;
}

static const char *to_kafs_path(const char *mnt_abs, const char *p, char out[KAFS_IOCTL_PATH_MAX])
{
  if (!p || !*p)
    return NULL;
  if (p[0] == '/')
  {
    size_t ml = mnt_abs ? strlen(mnt_abs) : 0;
    if (ml && strncmp(p, mnt_abs, ml) == 0 && (p[ml] == '/' || p[ml] == '\0'))
    {
      const char *suf = p + ml;
      if (*suf == '\0')
        suf = "/";
      snprintf(out, KAFS_IOCTL_PATH_MAX, "%s", suf);
      return out;
    }
    snprintf(out, KAFS_IOCTL_PATH_MAX, "%s", p);
    return out;
  }
  snprintf(out, KAFS_IOCTL_PATH_MAX, "/%s", p);
  return out;
}

static const char *to_mount_rel_path(const char *mnt_abs, const char *p,
                                     char out[KAFS_IOCTL_PATH_MAX])
{
  if (!p || !*p)
    return NULL;

  const char *suf = p;
  if (p[0] == '/')
  {
    size_t ml = mnt_abs ? strlen(mnt_abs) : 0;
    if (ml && strncmp(p, mnt_abs, ml) == 0 && (p[ml] == '/' || p[ml] == '\0'))
      suf = p + ml;
    if (*suf == '/')
      ++suf;
  }

  if (*suf == '\0')
    return NULL;

  snprintf(out, KAFS_IOCTL_PATH_MAX, "%s", suf);
  if (out[0] == '/' || path_has_dotdot_component(out))
    return NULL;
  return out;
}

static int cmd_stats(const char *mnt, int json, kafs_unit_t unit)
{
  int fd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (fd < 0)
  {
    perror("open");
    return 1;
  }

  kafs_stats_t st;
  memset(&st, 0, sizeof(st));
  if (ioctl(fd, KAFS_IOCTL_GET_STATS, &st) != 0)
  {
    perror("ioctl(KAFS_IOCTL_GET_STATS)");
    close(fd);
    return 1;
  }
  close(fd);

  uint64_t logical_bytes = st.hrl_refcnt_sum * (uint64_t)st.blksize;
  uint64_t unique_bytes = st.hrl_entries_used * (uint64_t)st.blksize;
  uint64_t saved_bytes = (st.hrl_refcnt_sum > st.hrl_entries_used)
                             ? (st.hrl_refcnt_sum - st.hrl_entries_used) * (uint64_t)st.blksize
                             : 0;

  double dedup_ratio = 1.0;
  if (unique_bytes > 0)
    dedup_ratio = (double)logical_bytes / (double)unique_bytes;

  double hit_rate = 0.0;
  if (st.hrl_put_calls > 0)
  {
    hit_rate = (double)st.hrl_put_hits / (double)st.hrl_put_calls;
  }
  double hrl_put_hash_ms = (double)st.hrl_put_ns_hash / 1000000.0;
  double hrl_put_find_ms = (double)st.hrl_put_ns_find / 1000000.0;
  double hrl_put_cmp_ms = (double)st.hrl_put_ns_cmp_content / 1000000.0;
  double hrl_put_slot_alloc_ms = (double)st.hrl_put_ns_slot_alloc / 1000000.0;
  double hrl_put_blk_alloc_ms = (double)st.hrl_put_ns_blk_alloc / 1000000.0;
  double hrl_put_blk_write_ms = (double)st.hrl_put_ns_blk_write / 1000000.0;
  double hrl_put_avg_chain_steps =
      (st.hrl_put_calls > 0) ? (double)st.hrl_put_chain_steps / (double)st.hrl_put_calls : 0.0;
  double hrl_put_avg_cmp_calls =
      (st.hrl_put_calls > 0) ? (double)st.hrl_put_cmp_calls / (double)st.hrl_put_calls : 0.0;

  double lock_inode_cont_rate =
      (st.lock_inode_acquire > 0) ? (double)st.lock_inode_contended / (double)st.lock_inode_acquire
                                  : 0.0;
  double lock_inode_wait_ms = (double)st.lock_inode_wait_ns / 1000000.0;
  double lock_inode_alloc_cont_rate =
      (st.lock_inode_alloc_acquire > 0)
          ? (double)st.lock_inode_alloc_contended / (double)st.lock_inode_alloc_acquire
          : 0.0;
  double lock_inode_alloc_wait_ms = (double)st.lock_inode_alloc_wait_ns / 1000000.0;
  double lock_bitmap_cont_rate = (st.lock_bitmap_acquire > 0) ? (double)st.lock_bitmap_contended /
                                                                    (double)st.lock_bitmap_acquire
                                                              : 0.0;
  double lock_bitmap_wait_ms = (double)st.lock_bitmap_wait_ns / 1000000.0;
  double lock_hrl_bucket_cont_rate =
      (st.lock_hrl_bucket_acquire > 0)
          ? (double)st.lock_hrl_bucket_contended / (double)st.lock_hrl_bucket_acquire
          : 0.0;
  double lock_hrl_bucket_wait_ms = (double)st.lock_hrl_bucket_wait_ns / 1000000.0;
  double lock_hrl_global_cont_rate =
      (st.lock_hrl_global_acquire > 0)
          ? (double)st.lock_hrl_global_contended / (double)st.lock_hrl_global_acquire
          : 0.0;
  double lock_hrl_global_wait_ms = (double)st.lock_hrl_global_wait_ns / 1000000.0;
  double pwrite_iblk_read_ms = (double)st.pwrite_ns_iblk_read / 1000000.0;
  double pwrite_iblk_write_ms = (double)st.pwrite_ns_iblk_write / 1000000.0;
  double pwrite_iblk_write_p50_ms = (double)st.pwrite_iblk_write_p50_ns / 1000000.0;
  double pwrite_iblk_write_p95_ms = (double)st.pwrite_iblk_write_p95_ns / 1000000.0;
  double pwrite_iblk_write_p99_ms = (double)st.pwrite_iblk_write_p99_ns / 1000000.0;
  double iblk_write_hrl_put_ms = (double)st.iblk_write_ns_hrl_put / 1000000.0;
  double iblk_write_legacy_blk_write_ms = (double)st.iblk_write_ns_legacy_blk_write / 1000000.0;
  double iblk_write_dec_ref_ms = (double)st.iblk_write_ns_dec_ref / 1000000.0;
  double blk_alloc_scan_ms = (double)st.blk_alloc_ns_scan / 1000000.0;
  double blk_alloc_claim_ms = (double)st.blk_alloc_ns_claim / 1000000.0;
  double blk_alloc_set_usage_ms = (double)st.blk_alloc_ns_set_usage / 1000000.0;
  double blk_alloc_retry_rate =
      (st.blk_alloc_calls > 0) ? (double)st.blk_alloc_claim_retries / (double)st.blk_alloc_calls
                               : 0.0;
  double blk_set_usage_bit_ms = (double)st.blk_set_usage_ns_bit_update / 1000000.0;
  double blk_set_usage_freecnt_ms = (double)st.blk_set_usage_ns_freecnt_update / 1000000.0;
  double blk_set_usage_wtime_ms = (double)st.blk_set_usage_ns_wtime_update / 1000000.0;
  double copy_share_hit_rate =
      (st.copy_share_attempt_blocks > 0)
          ? (double)st.copy_share_done_blocks / (double)st.copy_share_attempt_blocks
          : 0.0;
  uint64_t bg_dedup_ops = st.bg_dedup_replacements + st.bg_dedup_retries;
  double bg_dedup_retry_rate =
      (bg_dedup_ops > 0) ? (double)st.bg_dedup_retries / (double)bg_dedup_ops : 0.0;
  double bg_dedup_direct_hit_rate =
      (st.bg_dedup_direct_candidates > 0)
          ? (double)st.bg_dedup_direct_hits / (double)st.bg_dedup_direct_candidates
          : 0.0;
  double pending_worker_start_fail_rate =
      (st.pending_worker_start_calls > 0)
          ? (double)st.pending_worker_start_failures / (double)st.pending_worker_start_calls
          : 0.0;
  uint64_t fs_blocks_used =
      (st.fs_blocks_total >= st.fs_blocks_free) ? (st.fs_blocks_total - st.fs_blocks_free) : 0;
  uint64_t fs_inodes_used =
      (st.fs_inodes_total >= st.fs_inodes_free) ? (st.fs_inodes_total - st.fs_inodes_free) : 0;

  double fs_blocks_used_pct = pct_u64(fs_blocks_used, st.fs_blocks_total);
  double fs_inodes_used_pct = pct_u64(fs_inodes_used, st.fs_inodes_total);
  double hrl_entries_used_pct = pct_u64(st.hrl_entries_used, st.hrl_entries_total);

  uint64_t hrl_or_legacy_total = st.hrl_put_calls + st.hrl_put_fallback_legacy;
  double hrl_path_rate = pct_u64(st.hrl_put_calls, hrl_or_legacy_total);
  double legacy_path_rate = pct_u64(st.hrl_put_fallback_legacy, hrl_or_legacy_total);
  double direct_to_hrl_ratio =
      (st.hrl_put_calls > 0) ? (double)st.hrl_put_fallback_legacy / (double)st.hrl_put_calls : 0.0;
  double hrl_hit_rate_pct = pct_u64(st.hrl_put_hits, st.hrl_put_calls);
  double hrl_miss_rate_pct = pct_u64(st.hrl_put_misses, st.hrl_put_calls);
  double hrl_rescue_hit_rate = (st.hrl_rescue_attempts > 0)
                                   ? (double)st.hrl_rescue_hits / (double)st.hrl_rescue_attempts
                                   : 0.0;

  if (json)
  {
    printf("{\n");
    printf("  \"version\": %" PRIu32 ",\n", st.version);
    printf("  \"blksize\": %" PRIu32 ",\n", st.blksize);
    printf("  \"fs_blocks_total\": %" PRIu64 ",\n", st.fs_blocks_total);
    printf("  \"fs_blocks_free\": %" PRIu64 ",\n", st.fs_blocks_free);
    printf("  \"fs_inodes_total\": %" PRIu64 ",\n", st.fs_inodes_total);
    printf("  \"fs_inodes_free\": %" PRIu64 ",\n", st.fs_inodes_free);
    printf("  \"hrl_entries_total\": %" PRIu64 ",\n", st.hrl_entries_total);
    printf("  \"hrl_entries_used\": %" PRIu64 ",\n", st.hrl_entries_used);
    printf("  \"hrl_entries_duplicated\": %" PRIu64 ",\n", st.hrl_entries_duplicated);
    printf("  \"hrl_refcnt_sum\": %" PRIu64 ",\n", st.hrl_refcnt_sum);
    printf("  \"logical_bytes\": %" PRIu64 ",\n", logical_bytes);
    printf("  \"unique_bytes\": %" PRIu64 ",\n", unique_bytes);
    printf("  \"saved_bytes\": %" PRIu64 ",\n", saved_bytes);
    printf("  \"dedup_ratio\": %.6f,\n", dedup_ratio);
    printf("  \"hrl_put_calls\": %" PRIu64 ",\n", st.hrl_put_calls);
    printf("  \"hrl_put_hits\": %" PRIu64 ",\n", st.hrl_put_hits);
    printf("  \"hrl_put_misses\": %" PRIu64 ",\n", st.hrl_put_misses);
    printf("  \"hrl_put_fallback_legacy\": %" PRIu64 ",\n", st.hrl_put_fallback_legacy);
    printf("  \"hrl_put_ns_hash\": %" PRIu64 ",\n", st.hrl_put_ns_hash);
    printf("  \"hrl_put_ns_find\": %" PRIu64 ",\n", st.hrl_put_ns_find);
    printf("  \"hrl_put_ns_cmp_content\": %" PRIu64 ",\n", st.hrl_put_ns_cmp_content);
    printf("  \"hrl_put_ns_slot_alloc\": %" PRIu64 ",\n", st.hrl_put_ns_slot_alloc);
    printf("  \"hrl_put_ns_blk_alloc\": %" PRIu64 ",\n", st.hrl_put_ns_blk_alloc);
    printf("  \"hrl_put_ns_blk_write\": %" PRIu64 ",\n", st.hrl_put_ns_blk_write);
    printf("  \"hrl_put_chain_steps\": %" PRIu64 ",\n", st.hrl_put_chain_steps);
    printf("  \"hrl_put_cmp_calls\": %" PRIu64 ",\n", st.hrl_put_cmp_calls);
    printf("  \"hrl_put_hash_ms\": %.3f,\n", hrl_put_hash_ms);
    printf("  \"hrl_put_find_ms\": %.3f,\n", hrl_put_find_ms);
    printf("  \"hrl_put_cmp_ms\": %.3f,\n", hrl_put_cmp_ms);
    printf("  \"hrl_put_slot_alloc_ms\": %.3f,\n", hrl_put_slot_alloc_ms);
    printf("  \"hrl_put_blk_alloc_ms\": %.3f,\n", hrl_put_blk_alloc_ms);
    printf("  \"hrl_put_blk_write_ms\": %.3f,\n", hrl_put_blk_write_ms);
    printf("  \"hrl_put_avg_chain_steps\": %.3f,\n", hrl_put_avg_chain_steps);
    printf("  \"hrl_put_avg_cmp_calls\": %.3f,\n", hrl_put_avg_cmp_calls);
    printf("  \"hrl_put_hit_rate\": %.6f,\n", hit_rate);
    printf("  \"hrl_rescue_attempts\": %" PRIu64 ",\n", st.hrl_rescue_attempts);
    printf("  \"hrl_rescue_hits\": %" PRIu64 ",\n", st.hrl_rescue_hits);
    printf("  \"hrl_rescue_evicts\": %" PRIu64 ",\n", st.hrl_rescue_evicts);
    printf("  \"hrl_rescue_hit_rate\": %.6f,\n", hrl_rescue_hit_rate);
    printf("  \"lock_inode_acquire\": %" PRIu64 ",\n", st.lock_inode_acquire);
    printf("  \"lock_inode_contended\": %" PRIu64 ",\n", st.lock_inode_contended);
    printf("  \"lock_inode_wait_ns\": %" PRIu64 ",\n", st.lock_inode_wait_ns);
    printf("  \"lock_inode_contended_rate\": %.6f,\n", lock_inode_cont_rate);
    printf("  \"lock_inode_wait_ms\": %.3f,\n", lock_inode_wait_ms);
    printf("  \"lock_inode_alloc_acquire\": %" PRIu64 ",\n", st.lock_inode_alloc_acquire);
    printf("  \"lock_inode_alloc_contended\": %" PRIu64 ",\n", st.lock_inode_alloc_contended);
    printf("  \"lock_inode_alloc_wait_ns\": %" PRIu64 ",\n", st.lock_inode_alloc_wait_ns);
    printf("  \"lock_inode_alloc_contended_rate\": %.6f,\n", lock_inode_alloc_cont_rate);
    printf("  \"lock_inode_alloc_wait_ms\": %.3f,\n", lock_inode_alloc_wait_ms);
    printf("  \"lock_bitmap_acquire\": %" PRIu64 ",\n", st.lock_bitmap_acquire);
    printf("  \"lock_bitmap_contended\": %" PRIu64 ",\n", st.lock_bitmap_contended);
    printf("  \"lock_bitmap_wait_ns\": %" PRIu64 ",\n", st.lock_bitmap_wait_ns);
    printf("  \"lock_bitmap_contended_rate\": %.6f,\n", lock_bitmap_cont_rate);
    printf("  \"lock_bitmap_wait_ms\": %.3f,\n", lock_bitmap_wait_ms);
    printf("  \"lock_hrl_bucket_acquire\": %" PRIu64 ",\n", st.lock_hrl_bucket_acquire);
    printf("  \"lock_hrl_bucket_contended\": %" PRIu64 ",\n", st.lock_hrl_bucket_contended);
    printf("  \"lock_hrl_bucket_wait_ns\": %" PRIu64 ",\n", st.lock_hrl_bucket_wait_ns);
    printf("  \"lock_hrl_bucket_contended_rate\": %.6f,\n", lock_hrl_bucket_cont_rate);
    printf("  \"lock_hrl_bucket_wait_ms\": %.3f,\n", lock_hrl_bucket_wait_ms);
    printf("  \"lock_hrl_global_acquire\": %" PRIu64 ",\n", st.lock_hrl_global_acquire);
    printf("  \"lock_hrl_global_contended\": %" PRIu64 ",\n", st.lock_hrl_global_contended);
    printf("  \"lock_hrl_global_wait_ns\": %" PRIu64 ",\n", st.lock_hrl_global_wait_ns);
    printf("  \"lock_hrl_global_contended_rate\": %.6f,\n", lock_hrl_global_cont_rate);
    printf("  \"lock_hrl_global_wait_ms\": %.3f,\n", lock_hrl_global_wait_ms);
    printf("  \"pwrite_calls\": %" PRIu64 ",\n", st.pwrite_calls);
    printf("  \"pwrite_bytes\": %" PRIu64 ",\n", st.pwrite_bytes);
    printf("  \"pwrite_ns_iblk_read\": %" PRIu64 ",\n", st.pwrite_ns_iblk_read);
    printf("  \"pwrite_ns_iblk_write\": %" PRIu64 ",\n", st.pwrite_ns_iblk_write);
    printf("  \"pwrite_iblk_write_sample_count\": %" PRIu64 ",\n",
           st.pwrite_iblk_write_sample_count);
    printf("  \"pwrite_iblk_write_sample_cap\": %" PRIu64 ",\n", st.pwrite_iblk_write_sample_cap);
    printf("  \"pwrite_iblk_write_p50_ns\": %" PRIu64 ",\n", st.pwrite_iblk_write_p50_ns);
    printf("  \"pwrite_iblk_write_p95_ns\": %" PRIu64 ",\n", st.pwrite_iblk_write_p95_ns);
    printf("  \"pwrite_iblk_write_p99_ns\": %" PRIu64 ",\n", st.pwrite_iblk_write_p99_ns);
    printf("  \"iblk_write_ns_hrl_put\": %" PRIu64 ",\n", st.iblk_write_ns_hrl_put);
    printf("  \"iblk_write_ns_legacy_blk_write\": %" PRIu64 ",\n",
           st.iblk_write_ns_legacy_blk_write);
    printf("  \"iblk_write_ns_dec_ref\": %" PRIu64 ",\n", st.iblk_write_ns_dec_ref);
    printf("  \"blk_alloc_calls\": %" PRIu64 ",\n", st.blk_alloc_calls);
    printf("  \"blk_alloc_claim_retries\": %" PRIu64 ",\n", st.blk_alloc_claim_retries);
    printf("  \"blk_alloc_ns_scan\": %" PRIu64 ",\n", st.blk_alloc_ns_scan);
    printf("  \"blk_alloc_ns_claim\": %" PRIu64 ",\n", st.blk_alloc_ns_claim);
    printf("  \"blk_alloc_ns_set_usage\": %" PRIu64 ",\n", st.blk_alloc_ns_set_usage);
    printf("  \"blk_set_usage_calls\": %" PRIu64 ",\n", st.blk_set_usage_calls);
    printf("  \"blk_set_usage_alloc_calls\": %" PRIu64 ",\n", st.blk_set_usage_alloc_calls);
    printf("  \"blk_set_usage_free_calls\": %" PRIu64 ",\n", st.blk_set_usage_free_calls);
    printf("  \"blk_set_usage_ns_bit_update\": %" PRIu64 ",\n", st.blk_set_usage_ns_bit_update);
    printf("  \"blk_set_usage_ns_freecnt_update\": %" PRIu64 ",\n",
           st.blk_set_usage_ns_freecnt_update);
    printf("  \"blk_set_usage_ns_wtime_update\": %" PRIu64 ",\n", st.blk_set_usage_ns_wtime_update);
    printf("  \"copy_share_attempt_blocks\": %" PRIu64 ",\n", st.copy_share_attempt_blocks);
    printf("  \"copy_share_done_blocks\": %" PRIu64 ",\n", st.copy_share_done_blocks);
    printf("  \"copy_share_fallback_blocks\": %" PRIu64 ",\n", st.copy_share_fallback_blocks);
    printf("  \"copy_share_skip_unaligned\": %" PRIu64 ",\n", st.copy_share_skip_unaligned);
    printf("  \"copy_share_skip_dst_inline\": %" PRIu64 ",\n", st.copy_share_skip_dst_inline);
    printf("  \"bg_dedup_replacements\": %" PRIu64 ",\n", st.bg_dedup_replacements);
    printf("  \"bg_dedup_evicts\": %" PRIu64 ",\n", st.bg_dedup_evicts);
    printf("  \"bg_dedup_retries\": %" PRIu64 ",\n", st.bg_dedup_retries);
    printf("  \"bg_dedup_steps\": %" PRIu64 ",\n", st.bg_dedup_steps);
    printf("  \"bg_dedup_scanned_blocks\": %" PRIu64 ",\n", st.bg_dedup_scanned_blocks);
    printf("  \"bg_dedup_direct_candidates\": %" PRIu64 ",\n", st.bg_dedup_direct_candidates);
    printf("  \"bg_dedup_direct_hits\": %" PRIu64 ",\n", st.bg_dedup_direct_hits);
    printf("  \"bg_dedup_direct_hit_rate\": %.6f,\n", bg_dedup_direct_hit_rate);
    printf("  \"bg_dedup_index_evicts\": %" PRIu64 ",\n", st.bg_dedup_index_evicts);
    printf("  \"bg_dedup_cooldowns\": %" PRIu64 ",\n", st.bg_dedup_cooldowns);
    printf("  \"bg_dedup_mode\": %" PRIu32 ",\n", st.bg_dedup_mode);
    printf("  \"bg_dedup_mode_str\": \"%s\",\n", bg_dedup_mode_str(st.bg_dedup_mode));
    printf("  \"bg_dedup_telemetry_valid\": %" PRIu32 ",\n", st.bg_dedup_telemetry_valid);
    printf("  \"bg_dedup_last_scanned_blocks\": %" PRIu64 ",\n", st.bg_dedup_last_scanned_blocks);
    printf("  \"bg_dedup_last_direct_candidates\": %" PRIu64 ",\n",
           st.bg_dedup_last_direct_candidates);
    printf("  \"bg_dedup_last_replacements\": %" PRIu64 ",\n", st.bg_dedup_last_replacements);
    printf("  \"bg_dedup_idle_skip_streak\": %" PRIu64 ",\n", st.bg_dedup_idle_skip_streak);
    printf("  \"bg_dedup_cold_start_due_ms\": %" PRIu64 ",\n", st.bg_dedup_cold_start_due_ms);
    printf("  \"pending_queue_depth\": %" PRIu64 ",\n", st.pending_queue_depth);
    printf("  \"pending_queue_capacity\": %" PRIu64 ",\n", st.pending_queue_capacity);
    printf("  \"pending_queue_head\": %" PRIu64 ",\n", st.pending_queue_head);
    printf("  \"pending_queue_tail\": %" PRIu64 ",\n", st.pending_queue_tail);
    printf("  \"pending_worker_start_calls\": %" PRIu64 ",\n", st.pending_worker_start_calls);
    printf("  \"pending_worker_start_failures\": %" PRIu64 ",\n", st.pending_worker_start_failures);
    printf("  \"pending_worker_start_fail_rate\": %.6f,\n", pending_worker_start_fail_rate);
    printf("  \"pending_worker_start_last_error\": %" PRId32 ",\n",
           st.pending_worker_start_last_error);
    printf("  \"pending_worker_lwp_tid\": %" PRId32 ",\n", st.pending_worker_lwp_tid);
    printf("  \"pending_worker_running\": %" PRId32 ",\n", st.pending_worker_running);
    printf("  \"pending_worker_stop_flag\": %" PRId32 ",\n", st.pending_worker_stop_flag);
    printf("  \"pending_worker_main_entries\": %" PRIu64 ",\n", st.pending_worker_main_entries);
    printf("  \"pending_worker_main_exits\": %" PRIu64 ",\n", st.pending_worker_main_exits);
    printf("  \"pending_resolved\": %" PRIu64 ",\n", st.pending_resolved);
    printf("  \"pending_old_block_freed\": %" PRIu64 ",\n", st.pending_old_block_freed);
    printf("  \"bg_dedup_retry_rate\": %.6f,\n", bg_dedup_retry_rate);
    printf("  \"copy_share_hit_rate\": %.6f,\n", copy_share_hit_rate);
    printf("  \"pwrite_iblk_read_ms\": %.3f,\n", pwrite_iblk_read_ms);
    printf("  \"pwrite_iblk_write_ms\": %.3f,\n", pwrite_iblk_write_ms);
    printf("  \"pwrite_iblk_write_p50_ms\": %.3f,\n", pwrite_iblk_write_p50_ms);
    printf("  \"pwrite_iblk_write_p95_ms\": %.3f,\n", pwrite_iblk_write_p95_ms);
    printf("  \"pwrite_iblk_write_p99_ms\": %.3f,\n", pwrite_iblk_write_p99_ms);
    printf("  \"iblk_write_hrl_put_ms\": %.3f,\n", iblk_write_hrl_put_ms);
    printf("  \"iblk_write_legacy_blk_write_ms\": %.3f,\n", iblk_write_legacy_blk_write_ms);
    printf("  \"iblk_write_dec_ref_ms\": %.3f,\n", iblk_write_dec_ref_ms);
    printf("  \"blk_alloc_scan_ms\": %.3f,\n", blk_alloc_scan_ms);
    printf("  \"blk_alloc_claim_ms\": %.3f,\n", blk_alloc_claim_ms);
    printf("  \"blk_alloc_set_usage_ms\": %.3f,\n", blk_alloc_set_usage_ms);
    printf("  \"blk_alloc_retry_rate\": %.6f,\n", blk_alloc_retry_rate);
    printf("  \"blk_set_usage_bit_ms\": %.3f,\n", blk_set_usage_bit_ms);
    printf("  \"blk_set_usage_freecnt_ms\": %.3f,\n", blk_set_usage_freecnt_ms);
    printf("  \"blk_set_usage_wtime_ms\": %.3f\n", blk_set_usage_wtime_ms);
    printf("}\n");
    return 0;
  }

  printf("kafs fsstat v%" PRIu32 "\n", st.version);
  printf("  summary.capacity:\n");
  printf("    fs_blocks: used=");
  print_bytes(fs_blocks_used * (uint64_t)st.blksize, unit);
  printf(" / total=");
  print_bytes(st.fs_blocks_total * (uint64_t)st.blksize, unit);
  printf(" (%.2f%%)\n", fs_blocks_used_pct);
  printf("    fs_inodes: used=%" PRIu64 " / total=%" PRIu64 " (%.2f%%)\n", fs_inodes_used,
         st.fs_inodes_total, fs_inodes_used_pct);
  printf("    hrl_entries: used=%" PRIu64 " / total=%" PRIu64 " (%.2f%%)\n", st.hrl_entries_used,
         st.hrl_entries_total, hrl_entries_used_pct);

  printf("  summary.ratios:\n");
  printf("    dedup_ratio: %.3f (logical/unique)\n", dedup_ratio);
  printf("    write_path: hrl=%.2f%% legacy_direct=%.2f%% (hrl_calls=%" PRIu64
         " fallback_legacy=%" PRIu64 ")\n",
         hrl_path_rate, legacy_path_rate, st.hrl_put_calls, st.hrl_put_fallback_legacy);
  printf("    direct_to_hrl: %.6f (legacy_direct/hrl_calls)\n", direct_to_hrl_ratio);
  printf("    hrl_hit_miss: hit=%.2f%% miss=%.2f%%\n", hrl_hit_rate_pct, hrl_miss_rate_pct);
  printf("    copy_share_hit: %.2f%% (done/attempt)\n", copy_share_hit_rate * 100.0);

  printf("  blksize: ");
  print_bytes(st.blksize, unit);
  printf("\n");

  printf("  fs: blocks total=%" PRIu64 " (", st.fs_blocks_total);
  print_bytes(st.fs_blocks_total * (uint64_t)st.blksize, unit);
  printf(") free=%" PRIu64 " (", st.fs_blocks_free);
  print_bytes(st.fs_blocks_free * (uint64_t)st.blksize, unit);
  printf(")\n");
  printf("      inodes total=%" PRIu64 " free=%" PRIu64 "\n", st.fs_inodes_total,
         st.fs_inodes_free);

  printf("  hrl: entries used=%" PRIu64 "/%" PRIu64 " duplicated=%" PRIu64 " refsum=%" PRIu64 "\n",
         st.hrl_entries_used, st.hrl_entries_total, st.hrl_entries_duplicated, st.hrl_refcnt_sum);

  printf("  dedup: logical=");
  print_bytes(logical_bytes, unit);
  printf(" unique=");
  print_bytes(unique_bytes, unit);
  printf(" saved=");
  print_bytes(saved_bytes, unit);
  printf(" ratio=%.3f\n", dedup_ratio);
  printf("  hrl_put: calls=%" PRIu64 " hits=%" PRIu64 " misses=%" PRIu64 " fallback_legacy=%" PRIu64
         " hit_rate=%.3f\n",
         st.hrl_put_calls, st.hrl_put_hits, st.hrl_put_misses, st.hrl_put_fallback_legacy,
         hit_rate);
  printf("  hrl_rescue: attempts=%" PRIu64 " hits=%" PRIu64 " evicts=%" PRIu64 " hit_rate=%.3f\n",
         st.hrl_rescue_attempts, st.hrl_rescue_hits, st.hrl_rescue_evicts, hrl_rescue_hit_rate);
  printf("  hrl_put_decomp: hash_ms=%.3f find_ms=%.3f cmp_ms=%.3f slot_alloc_ms=%.3f "
         "blk_alloc_ms=%.3f blk_write_ms=%.3f avg_chain_steps=%.3f avg_cmp_calls=%.3f\n",
         hrl_put_hash_ms, hrl_put_find_ms, hrl_put_cmp_ms, hrl_put_slot_alloc_ms,
         hrl_put_blk_alloc_ms, hrl_put_blk_write_ms, hrl_put_avg_chain_steps,
         hrl_put_avg_cmp_calls);
  printf("  lock[inode]: acquire=%" PRIu64 " contended=%" PRIu64 " rate=%.3f wait_ms=%.3f\n",
         st.lock_inode_acquire, st.lock_inode_contended, lock_inode_cont_rate, lock_inode_wait_ms);
  printf("  lock[inode_alloc]: acquire=%" PRIu64 " contended=%" PRIu64 " rate=%.3f wait_ms=%.3f\n",
         st.lock_inode_alloc_acquire, st.lock_inode_alloc_contended, lock_inode_alloc_cont_rate,
         lock_inode_alloc_wait_ms);
  printf("  lock[bitmap]: acquire=%" PRIu64 " contended=%" PRIu64 " rate=%.3f wait_ms=%.3f\n",
         st.lock_bitmap_acquire, st.lock_bitmap_contended, lock_bitmap_cont_rate,
         lock_bitmap_wait_ms);
  printf("  lock[hrl_bucket]: acquire=%" PRIu64 " contended=%" PRIu64 " rate=%.3f wait_ms=%.3f\n",
         st.lock_hrl_bucket_acquire, st.lock_hrl_bucket_contended, lock_hrl_bucket_cont_rate,
         lock_hrl_bucket_wait_ms);
  printf("  lock[hrl_global]: acquire=%" PRIu64 " contended=%" PRIu64 " rate=%.3f wait_ms=%.3f\n",
         st.lock_hrl_global_acquire, st.lock_hrl_global_contended, lock_hrl_global_cont_rate,
         lock_hrl_global_wait_ms);
  printf("  pwrite: calls=%" PRIu64 " bytes=%" PRIu64 " iblk_read_ms=%.3f iblk_write_ms=%.3f\n",
         st.pwrite_calls, st.pwrite_bytes, pwrite_iblk_read_ms, pwrite_iblk_write_ms);
  printf("          iblk_write_lat: samples=%" PRIu64 "/%" PRIu64
         " p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f\n",
         st.pwrite_iblk_write_sample_count, st.pwrite_iblk_write_sample_cap,
         pwrite_iblk_write_p50_ms, pwrite_iblk_write_p95_ms, pwrite_iblk_write_p99_ms);
  printf("  iblk_write: hrl_put_ms=%.3f legacy_blk_write_ms=%.3f dec_ref_ms=%.3f\n",
         iblk_write_hrl_put_ms, iblk_write_legacy_blk_write_ms, iblk_write_dec_ref_ms);
  printf("  blk_alloc: calls=%" PRIu64 " retries=%" PRIu64 " retry_rate=%.3f scan_ms=%.3f "
         "claim_ms=%.3f set_usage_ms=%.3f\n",
         st.blk_alloc_calls, st.blk_alloc_claim_retries, blk_alloc_retry_rate, blk_alloc_scan_ms,
         blk_alloc_claim_ms, blk_alloc_set_usage_ms);
  printf("  blk_set_usage: calls=%" PRIu64 " alloc_calls=%" PRIu64 " free_calls=%" PRIu64
         " bit_ms=%.3f freecnt_ms=%.3f wtime_ms=%.3f\n",
         st.blk_set_usage_calls, st.blk_set_usage_alloc_calls, st.blk_set_usage_free_calls,
         blk_set_usage_bit_ms, blk_set_usage_freecnt_ms, blk_set_usage_wtime_ms);
  printf("  copy_share: attempt_blocks=%" PRIu64 " done_blocks=%" PRIu64 " fallback_blocks=%" PRIu64
         " skip_unaligned=%" PRIu64 " skip_dst_inline=%" PRIu64 " hit_rate=%.3f\n",
         st.copy_share_attempt_blocks, st.copy_share_done_blocks, st.copy_share_fallback_blocks,
         st.copy_share_skip_unaligned, st.copy_share_skip_dst_inline, copy_share_hit_rate);
  printf("  bg_dedup: replacements=%" PRIu64 " evicts=%" PRIu64 " retries=%" PRIu64
         " retry_rate=%.3f\n",
         st.bg_dedup_replacements, st.bg_dedup_evicts, st.bg_dedup_retries, bg_dedup_retry_rate);
  printf("            steps=%" PRIu64 " scanned_blocks=%" PRIu64 " direct_candidates=%" PRIu64
         " direct_hits=%" PRIu64 " direct_hit_rate=%.3f index_evicts=%" PRIu64 " cooldowns=%" PRIu64
         "\n",
         st.bg_dedup_steps, st.bg_dedup_scanned_blocks, st.bg_dedup_direct_candidates,
         st.bg_dedup_direct_hits, bg_dedup_direct_hit_rate, st.bg_dedup_index_evicts,
         st.bg_dedup_cooldowns);
  printf("            mode=%" PRIu32 " (%s) telemetry_valid=%" PRIu32 " last_scanned=%" PRIu64
         " last_direct_candidates=%" PRIu64 " last_replacements=%" PRIu64
         " idle_skip_streak=%" PRIu64 " cold_due_ms=%" PRIu64 "\n",
         st.bg_dedup_mode, bg_dedup_mode_str(st.bg_dedup_mode), st.bg_dedup_telemetry_valid,
         st.bg_dedup_last_scanned_blocks, st.bg_dedup_last_direct_candidates,
         st.bg_dedup_last_replacements, st.bg_dedup_idle_skip_streak,
         st.bg_dedup_cold_start_due_ms);
  printf("  pending: depth=%" PRIu64 "/%" PRIu64 " head=%" PRIu64 " tail=%" PRIu64 "\n",
         st.pending_queue_depth, st.pending_queue_capacity, st.pending_queue_head,
         st.pending_queue_tail);
  printf("           worker running=%" PRId32 " stop=%" PRId32 " start_calls=%" PRIu64
         " start_failures=%" PRIu64 " fail_rate=%.3f last_error=%" PRId32 " lwp_tid=%" PRId32 "\n",
         st.pending_worker_running, st.pending_worker_stop_flag, st.pending_worker_start_calls,
         st.pending_worker_start_failures, pending_worker_start_fail_rate,
         st.pending_worker_start_last_error, st.pending_worker_lwp_tid);
  printf("           worker_main entries=%" PRIu64 " exits=%" PRIu64 "\n",
         st.pending_worker_main_entries, st.pending_worker_main_exits);
  printf("           pending_resolved=%" PRIu64 " old_block_freed=%" PRIu64 "\n",
         st.pending_resolved, st.pending_old_block_freed);
  return 0;
}

static void fmt_time(char out[64], const struct timespec *ts)
{
  if (!out)
    return;
  if (!ts)
  {
    out[0] = '\0';
    return;
  }
  time_t t = ts->tv_sec;
  struct tm tm;
  if (localtime_r(&t, &tm) == NULL)
  {
    snprintf(out, 64, "%lld", (long long)ts->tv_sec);
    return;
  }
  strftime(out, 64, "%Y-%m-%d %H:%M:%S", &tm);
}

static int cmd_stat(const char *mnt, const char *path)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  struct stat st;
  if (fstatat(dfd, p, &st, AT_SYMLINK_NOFOLLOW) != 0)
  {
    perror("fstatat");
    close(dfd);
    return 1;
  }

  const char *t = "unknown";
  if (S_ISREG(st.st_mode))
    t = "file";
  else if (S_ISDIR(st.st_mode))
    t = "dir";
  else if (S_ISLNK(st.st_mode))
    t = "symlink";
  else if (S_ISCHR(st.st_mode))
    t = "char";
  else if (S_ISBLK(st.st_mode))
    t = "block";
  else if (S_ISFIFO(st.st_mode))
    t = "fifo";
  else if (S_ISSOCK(st.st_mode))
    t = "sock";

  char at[64], mt[64], ct[64];
#if defined(__APPLE__)
  (void)at;
  (void)mt;
  (void)ct;
#else
  fmt_time(at, &st.st_atim);
  fmt_time(mt, &st.st_mtim);
  fmt_time(ct, &st.st_ctim);
#endif

  printf("path: %s\n", path);
  printf("type: %s\n", t);
  printf("mode: %04o\n", (unsigned int)(st.st_mode & 07777));
  printf("uid: %u\n", (unsigned int)st.st_uid);
  printf("gid: %u\n", (unsigned int)st.st_gid);
  printf("size: %lld\n", (long long)st.st_size);
  printf("nlink: %llu\n", (unsigned long long)st.st_nlink);
  printf("ino: %llu\n", (unsigned long long)st.st_ino);
#if !defined(__APPLE__)
  printf("atime: %s\n", at);
  printf("mtime: %s\n", mt);
  printf("ctime: %s\n", ct);
#endif

  close(dfd);
  return 0;
}

static int cmd_cat(const char *mnt, const char *path)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  int fd = openat(dfd, p, O_RDONLY);
  if (fd < 0)
  {
    perror("openat");
    close(dfd);
    return 1;
  }

  char buf[64 * 1024];
  while (1)
  {
    ssize_t r = read(fd, buf, sizeof(buf));
    if (r < 0)
    {
      perror("read");
      close(fd);
      close(dfd);
      return 1;
    }
    if (r == 0)
      break;
    ssize_t off = 0;
    while (off < r)
    {
      ssize_t w = write(STDOUT_FILENO, buf + off, (size_t)(r - off));
      if (w < 0)
      {
        perror("write");
        close(fd);
        close(dfd);
        return 1;
      }
      off += w;
    }
  }

  close(fd);
  close(dfd);
  return 0;
}

static int cmd_write(const char *mnt, const char *path)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  int fd = openat(dfd, p, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0)
  {
    perror("openat");
    close(dfd);
    return 1;
  }

  char buf[64 * 1024];
  while (1)
  {
    ssize_t r = read(STDIN_FILENO, buf, sizeof(buf));
    if (r < 0)
    {
      perror("read(stdin)");
      close(fd);
      close(dfd);
      return 1;
    }
    if (r == 0)
      break;
    ssize_t off = 0;
    while (off < r)
    {
      ssize_t w = write(fd, buf + off, (size_t)(r - off));
      if (w < 0)
      {
        perror("write(file)");
        close(fd);
        close(dfd);
        return 1;
      }
      off += w;
    }
  }

  close(fd);
  close(dfd);
  return 0;
}

static int cmd_cp(const char *mnt, const char *src, const char *dst, int reflink)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int fd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (fd < 0)
  {
    perror("open");
    return 1;
  }

  kafs_ioctl_copy_t req;
  memset(&req, 0, sizeof(req));
  req.struct_size = (uint32_t)sizeof(req);
  req.flags = reflink ? KAFS_IOCTL_COPY_F_REFLINK : 0;

  char srcbuf[KAFS_IOCTL_PATH_MAX];
  char dstbuf[KAFS_IOCTL_PATH_MAX];
  const char *s = to_kafs_path(mnt_abs, src, srcbuf);
  const char *d = to_kafs_path(mnt_abs, dst, dstbuf);
  if (!s || !d)
  {
    fprintf(stderr, "invalid path\n");
    close(fd);
    return 2;
  }
  snprintf(req.src, sizeof(req.src), "%s", s);
  snprintf(req.dst, sizeof(req.dst), "%s", d);

  if (ioctl(fd, KAFS_IOCTL_COPY, &req) != 0)
  {
    perror("ioctl(KAFS_IOCTL_COPY)");
    close(fd);
    return 1;
  }
  close(fd);
  return 0;
}

static int cmd_rm(const char *mnt, const char *path)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (unlinkat(dfd, p, 0) != 0)
  {
    perror("unlinkat");
    close(dfd);
    return 1;
  }
  close(dfd);
  return 0;
}

static int cmd_rmdir(const char *mnt, const char *path)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (unlinkat(dfd, p, AT_REMOVEDIR) != 0)
  {
    perror("unlinkat(AT_REMOVEDIR)");
    close(dfd);
    return 1;
  }
  close(dfd);
  return 0;
}

static int cmd_mkdir(const char *mnt, const char *path)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (mkdirat(dfd, p, 0755) != 0)
  {
    perror("mkdirat");
    close(dfd);
    return 1;
  }
  close(dfd);
  return 0;
}

static int cmd_mv(const char *mnt, const char *src, const char *dst)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char srel[KAFS_IOCTL_PATH_MAX];
  char drel[KAFS_IOCTL_PATH_MAX];
  const char *s = to_mount_rel_path(mnt_abs, src, srel);
  const char *d = to_mount_rel_path(mnt_abs, dst, drel);
  if (!s || !d)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (renameat(dfd, s, dfd, d) != 0)
  {
    perror("renameat");
    close(dfd);
    return 1;
  }

  close(dfd);
  return 0;
}

static int cmd_ln(const char *mnt, const char *src, const char *dst)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char srel[KAFS_IOCTL_PATH_MAX];
  char drel[KAFS_IOCTL_PATH_MAX];
  const char *s = to_mount_rel_path(mnt_abs, src, srel);
  const char *d = to_mount_rel_path(mnt_abs, dst, drel);
  if (!s || !d)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (linkat(dfd, s, dfd, d, 0) != 0)
  {
    perror("linkat");
    close(dfd);
    return 1;
  }

  close(dfd);
  return 0;
}

static int cmd_symlink(const char *mnt, const char *target, const char *linkpath)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char lrel[KAFS_IOCTL_PATH_MAX];
  const char *l = to_mount_rel_path(mnt_abs, linkpath, lrel);
  if (!l)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (symlinkat(target, dfd, l) != 0)
  {
    perror("symlinkat");
    close(dfd);
    return 1;
  }

  close(dfd);
  return 0;
}

static int cmd_readlink(const char *mnt, const char *path)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  char buf[KAFS_IOCTL_PATH_MAX];
  ssize_t n = readlinkat(dfd, p, buf, sizeof(buf) - 1);
  if (n < 0)
  {
    perror("readlinkat");
    close(dfd);
    return 1;
  }
  buf[n] = '\0';
  printf("%s\n", buf);

  close(dfd);
  return 0;
}

static int parse_octal_mode(const char *s, mode_t *out)
{
  if (!s || !*s || !out)
    return -EINVAL;
  char *end = NULL;
  errno = 0;
  long v = strtol(s, &end, 8);
  if (errno != 0 || !end || *end != '\0' || v < 0)
    return -EINVAL;
  *out = (mode_t)(v & 07777);
  return 0;
}

static int cmd_chmod(const char *mnt, const char *mode_str, const char *path)
{
  mode_t mode;
  if (parse_octal_mode(mode_str, &mode) != 0)
  {
    fprintf(stderr, "invalid mode\n");
    return 2;
  }

  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (fchmodat(dfd, p, mode, 0) != 0)
  {
    perror("fchmodat");
    close(dfd);
    return 1;
  }

  close(dfd);
  return 0;
}

static int cmd_touch(const char *mnt, const char *path)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  int fd = openat(dfd, p, O_CREAT | O_WRONLY, 0644);
  if (fd >= 0)
    close(fd);
  else if (errno != EISDIR)
  {
    perror("openat");
    close(dfd);
    return 1;
  }

  if (utimensat(dfd, p, NULL, 0) != 0)
  {
    perror("utimensat");
    close(dfd);
    return 1;
  }

  close(dfd);
  return 0;
}

int main(int argc, char **argv)
{
  if (argc < 3)
  {
    usage(argv[0]);
    return 2;
  }

  if (strcmp(argv[1], "migrate") == 0)
  {
    int assume_yes = 0;
    for (int i = 3; i < argc; ++i)
    {
      if (strcmp(argv[i], "--yes") == 0)
        assume_yes = 1;
      else
      {
        usage(argv[0]);
        return 2;
      }
    }
    return cmd_migrate(argv[2], assume_yes);
  }

  if (strcmp(argv[1], "fsstat") == 0 || strcmp(argv[1], "stats") == 0)
  {
    int json = 0;
    kafs_unit_t unit = KAFS_UNIT_KIB;
    for (int i = 3; i < argc; ++i)
    {
      if (strcmp(argv[i], "--json") == 0)
        json = 1;
      else if (strcmp(argv[i], "--bytes") == 0)
        unit = KAFS_UNIT_BYTES;
      else if (strcmp(argv[i], "--mib") == 0)
        unit = KAFS_UNIT_MIB;
      else if (strcmp(argv[i], "--gib") == 0)
        unit = KAFS_UNIT_GIB;
      else
      {
        usage(argv[0]);
        return 2;
      }
    }
    return cmd_stats(argv[2], json, unit);
  }

  if (strcmp(argv[1], "hotplug") == 0)
  {
    if (argc < 4)
    {
      usage(argv[0]);
      return 2;
    }
    if (strcmp(argv[2], "status") == 0)
    {
      int json = 0;
      for (int i = 4; i < argc; ++i)
      {
        if (strcmp(argv[i], "--json") == 0)
          json = 1;
        else
        {
          usage(argv[0]);
          return 2;
        }
      }
      return cmd_hotplug_status(argv[3], json);
    }
    if (strcmp(argv[2], "restart-back") == 0)
    {
      if (argc != 4)
      {
        usage(argv[0]);
        return 2;
      }
      return cmd_hotplug_restart(argv[3]);
    }
    if (strcmp(argv[2], "compat") == 0)
    {
      int json = 0;
      for (int i = 4; i < argc; ++i)
      {
        if (strcmp(argv[i], "--json") == 0)
          json = 1;
        else
        {
          usage(argv[0]);
          return 2;
        }
      }
      return cmd_hotplug_compat(argv[3], json);
    }
    if (strcmp(argv[2], "set-timeout") == 0)
    {
      if (argc != 5)
      {
        usage(argv[0]);
        return 2;
      }
      return cmd_hotplug_set_timeout(argv[3], argv[4]);
    }
    if (strcmp(argv[2], "set-dedup-priority") == 0)
    {
      if (argc != 5 && argc != 6)
      {
        usage(argv[0]);
        return 2;
      }
      return cmd_hotplug_set_dedup_priority(argv[3], argv[4], argc == 6 ? argv[5] : NULL);
    }
    if (strcmp(argv[2], "set-runtime") == 0)
    {
      if (argc < 5)
      {
        usage(argv[0]);
        return 2;
      }
      return cmd_hotplug_set_runtime(argv[3], argc - 4, &argv[4]);
    }
    if (strcmp(argv[2], "env") == 0)
    {
      if (argc < 5)
      {
        usage(argv[0]);
        return 2;
      }
      if (strcmp(argv[3], "list") == 0)
      {
        if (argc != 5)
        {
          usage(argv[0]);
          return 2;
        }
        return cmd_hotplug_env_list(argv[4]);
      }
      if (strcmp(argv[3], "set") == 0)
      {
        if (argc != 6)
        {
          usage(argv[0]);
          return 2;
        }
        return cmd_hotplug_env_set(argv[4], argv[5]);
      }
      if (strcmp(argv[3], "unset") == 0)
      {
        if (argc != 6)
        {
          usage(argv[0]);
          return 2;
        }
        return cmd_hotplug_env_unset(argv[4], argv[5]);
      }
      usage(argv[0]);
      return 2;
    }
    usage(argv[0]);
    return 2;
  }

  if (strcmp(argv[1], "stat") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_stat(argv[2], argv[3]);
  }

  if (strcmp(argv[1], "cat") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_cat(argv[2], argv[3]);
  }

  if (strcmp(argv[1], "write") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_write(argv[2], argv[3]);
  }

  if (strcmp(argv[1], "cp") == 0)
  {
    if (argc < 5)
    {
      usage(argv[0]);
      return 2;
    }
    int reflink = 0;
    for (int i = 5; i < argc; ++i)
    {
      if (strcmp(argv[i], "--reflink") == 0)
        reflink = 1;
      else
      {
        usage(argv[0]);
        return 2;
      }
    }
    return cmd_cp(argv[2], argv[3], argv[4], reflink);
  }

  if (strcmp(argv[1], "mv") == 0)
  {
    if (argc != 5)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_mv(argv[2], argv[3], argv[4]);
  }

  if (strcmp(argv[1], "rm") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_rm(argv[2], argv[3]);
  }

  if (strcmp(argv[1], "mkdir") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_mkdir(argv[2], argv[3]);
  }

  if (strcmp(argv[1], "rmdir") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_rmdir(argv[2], argv[3]);
  }

  if (strcmp(argv[1], "ln") == 0)
  {
    if (argc != 5)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_ln(argv[2], argv[3], argv[4]);
  }

  if (strcmp(argv[1], "symlink") == 0)
  {
    if (argc != 5)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_symlink(argv[2], argv[3], argv[4]);
  }

  if (strcmp(argv[1], "readlink") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_readlink(argv[2], argv[3]);
  }

  if (strcmp(argv[1], "chmod") == 0)
  {
    if (argc != 5)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_chmod(argv[2], argv[3], argv[4]);
  }

  if (strcmp(argv[1], "touch") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_touch(argv[2], argv[3]);
  }

  usage(argv[0]);
  return 2;
}
