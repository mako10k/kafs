#include "test_utils.h"

#include "kafs_superblock.h"
#include "kafs_tailmeta.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static void tlogf(const char *fmt, ...)
{
  va_list ap;

  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  fputc('\n', stderr);
  va_end(ap);
}

static const char *pick_exe(const char *env_name, const char *const *cands)
{
  const char *envv = getenv(env_name);

  if (envv && *envv)
    return envv;
  for (size_t i = 0; cands[i] != NULL; ++i)
  {
    if (access(cands[i], X_OK) == 0)
      return cands[i];
  }
  return NULL;
}

static const char *resolve_tool_from_self(const char *env_name, const char *tool_name,
                                          char out[PATH_MAX])
{
  const char *envv = getenv(env_name);
  if (envv && *envv)
  {
    if (strlen(envv) < PATH_MAX)
    {
      strcpy(out, envv);
      return out;
    }
    return envv;
  }

  char exe_path[PATH_MAX];
  ssize_t exe_len = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
  if (exe_len > 0)
  {
    exe_path[exe_len] = '\0';
    char *slash = strrchr(exe_path, '/');
    if (slash)
    {
      *slash = '\0';
      if ((size_t)snprintf(out, PATH_MAX, "%s/../src/%s", exe_path, tool_name) < PATH_MAX &&
          access(out, X_OK) == 0)
        return out;
    }
  }

  return NULL;
}

static const char *pick_mkfs_exe(void)
{
  static char resolved[PATH_MAX];
  static const char *const cands[] = {
      "./mkfs.kafs",
      "../src/mkfs.kafs",
      "./src/mkfs.kafs",
      "src/mkfs.kafs",
      "mkfs.kafs",
      NULL,
  };

  const char *resolved_path = resolve_tool_from_self("KAFS_TEST_MKFS", "mkfs.kafs", resolved);
  if (resolved_path)
    return resolved_path;
  return pick_exe("KAFS_TEST_MKFS", cands);
}

static int run_cmd(char *const argv[])
{
  pid_t pid = fork();

  if (pid < 0)
    return -errno;
  if (pid == 0)
  {
    execvp(argv[0], argv);
    _exit(127);
  }

  int status = 0;
  if (waitpid(pid, &status, 0) < 0)
    return -errno;
  if (WIFEXITED(status) && WEXITSTATUS(status) == 0)
    return 0;
  return -1;
}

static int mkfs_v5_image(const char *img)
{
  const char *mkfs = pick_mkfs_exe();

  if (!mkfs)
    return -ENOENT;

  char *argv[] = {
      (char *)mkfs,
      (char *)"--format-version",
      (char *)"5",
      (char *)img,
      (char *)"-s",
      (char *)"64M",
      NULL,
  };
  return run_cmd(argv);
}

static int inspect_tailmeta_live_slots(const char *img, uint64_t *out_live_slots)
{
  int fd = open(img, O_RDONLY);
  if (fd < 0)
    return -errno;

  struct stat st;
  if (fstat(fd, &st) != 0)
  {
    close(fd);
    return -errno;
  }

  void *base = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_SHARED, fd, 0);
  if (base == MAP_FAILED)
  {
    close(fd);
    return -errno;
  }

  int rc = 0;
  uint64_t live_slots = 0;
  const kafs_ssuperblock_t *sb = (const kafs_ssuperblock_t *)base;
  const kafs_tailmeta_region_hdr_t *region_hdr = NULL;

  if (kafs_sb_format_version_get(sb) != KAFS_FORMAT_VERSION_V5)
    rc = -EPROTO;

  if (rc == 0)
  {
    uint64_t tail_off = kafs_sb_tailmeta_offset_get(sb);
    uint64_t tail_size = kafs_sb_tailmeta_size_get(sb);
    if (tail_off == 0u || tail_size < sizeof(*region_hdr))
      rc = -EPROTO;
    else
      region_hdr = (const kafs_tailmeta_region_hdr_t *)((const char *)base + tail_off);
    if (rc == 0 && kafs_tailmeta_region_hdr_validate(region_hdr, tail_size) != 0)
      rc = -EPROTO;
  }

  if (rc == 0)
  {
    uint32_t table_off = kafs_tailmeta_region_hdr_container_table_off_get(region_hdr);
    uint32_t container_count = kafs_tailmeta_region_hdr_container_count_get(region_hdr);
    const kafs_tailmeta_container_hdr_t *containers =
        (const kafs_tailmeta_container_hdr_t *)((const char *)region_hdr + table_off);

    for (uint32_t container_index = 0; container_index < container_count; ++container_index)
    {
      const kafs_tailmeta_container_hdr_t *container = &containers[container_index];
      uint16_t class_bytes = kafs_tailmeta_container_hdr_class_bytes_get(container);
      uint16_t slot_count = kafs_tailmeta_container_hdr_slot_count_get(container);
      uint32_t slot_table_off = kafs_tailmeta_container_hdr_slot_table_off_get(container);
      const kafs_tailmeta_slot_desc_t *slots =
          (const kafs_tailmeta_slot_desc_t *)((const char *)region_hdr + slot_table_off);

      for (uint16_t slot_index = 0; slot_index < slot_count; ++slot_index)
      {
        if (kafs_tailmeta_slot_validate(&slots[slot_index], class_bytes) != 0)
        {
          rc = -EPROTO;
          break;
        }
        if (kafs_tailmeta_slot_owner_ino_get(&slots[slot_index]) != KAFS_INO_NONE)
          live_slots++;
      }
      if (rc != 0)
        break;
      if (live_slots < kafs_tailmeta_container_hdr_live_count_get(container))
      {
        rc = -EPROTO;
        break;
      }
    }
  }

  munmap(base, (size_t)st.st_size);
  close(fd);
  if (rc == 0 && out_live_slots)
    *out_live_slots = live_slots;
  return rc;
}

static int inspect_tail_packed_file(const char *img, const char *name, const char *expected,
                                    size_t expected_len);

static int inspect_tailmeta_live_slots_for_file(const char *img, const char *name,
                                                const char *expected, size_t expected_len,
                                                uint64_t expected_live_slots)
{
  int rc = inspect_tail_packed_file(img, name, expected, expected_len);
  if (rc != 0)
    return rc;

  uint64_t live_slots = 0;
  rc = inspect_tailmeta_live_slots(img, &live_slots);
  if (rc != 0)
    return rc;
  if (live_slots != expected_live_slots)
    return -ERANGE;
  return 0;
}

static int inspect_tail_packed_file(const char *img, const char *name, const char *expected,
                                    size_t expected_len)
{
  int fd = open(img, O_RDONLY);
  if (fd < 0)
    return -errno;

  struct stat st;
  if (fstat(fd, &st) != 0)
  {
    close(fd);
    return -errno;
  }

  void *base = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_SHARED, fd, 0);
  if (base == MAP_FAILED)
  {
    close(fd);
    return -errno;
  }

  int rc = 0;
  kafs_ssuperblock_t *sb = (kafs_ssuperblock_t *)base;
  if (kafs_sb_format_version_get(sb) != KAFS_FORMAT_VERSION_V5)
    rc = -EPROTO;

  kafs_sinode_t *inoent = NULL;
  if (rc == 0)
  {
    kafs_inocnt_t inocnt = kafs_sb_inocnt_get(sb);
    uint64_t off = sizeof(kafs_ssuperblock_t);
    kafs_blksize_t bs = kafs_sb_blksize_get(sb);
    kafs_blksize_t mask = bs - 1u;

    off = (off + mask) & ~((uint64_t)mask);
    off += ((uint64_t)kafs_sb_blkcnt_get(sb) + 7u) >> 3;
    off = (off + 7u) & ~7u;
    off = (off + mask) & ~((uint64_t)mask);

    void *inotbl = (char *)base + off;
    for (kafs_inocnt_t ino = KAFS_INO_ROOTDIR + 1u; ino < inocnt; ++ino)
    {
      kafs_sinode_t *candidate =
          (kafs_sinode_t *)kafs_inode_ptr_in_table(inotbl, kafs_sb_format_version_get(sb), ino);
      if (!candidate || !kafs_ino_get_usage(candidate))
        continue;
      if (!S_ISREG(kafs_ino_mode_get(candidate)))
        continue;
      if ((size_t)kafs_ino_size_get(candidate) != expected_len)
        continue;
      inoent = candidate;
      break;
    }
    if (!inoent)
      rc = -ENOENT;
  }
  (void)name;

  const kafs_sinode_taildesc_v5_t *taildesc = NULL;
  if (rc == 0)
  {
    taildesc = &((const kafs_sinode_v5_t *)inoent)->i_taildesc;
    if (kafs_ino_taildesc_v5_layout_kind_get(taildesc) != KAFS_TAIL_LAYOUT_TAIL_ONLY)
      rc = -EPROTO;
    if (kafs_ino_taildesc_v5_fragment_len_get(taildesc) != expected_len)
      rc = -EPROTO;
    if ((kafs_ino_taildesc_v5_flags_get(taildesc) & KAFS_TAILDESC_FLAG_PACKED_SMALL_FILE) == 0u)
      rc = -EPROTO;
  }

  const kafs_tailmeta_region_hdr_t *region_hdr = NULL;
  if (rc == 0)
  {
    uint64_t tail_off = kafs_sb_tailmeta_offset_get(sb);
    uint64_t tail_size = kafs_sb_tailmeta_size_get(sb);
    if (tail_off == 0u || tail_size < sizeof(*region_hdr))
      rc = -EPROTO;
    else
      region_hdr = (const kafs_tailmeta_region_hdr_t *)((const char *)base + tail_off);
    if (rc == 0 && kafs_tailmeta_region_hdr_validate(region_hdr, tail_size) != 0)
      rc = -EPROTO;
  }

  if (rc == 0)
  {
    uint64_t tail_off = kafs_sb_tailmeta_offset_get(sb);
    uint32_t table_off = kafs_tailmeta_region_hdr_container_table_off_get(region_hdr);
    const kafs_tailmeta_container_hdr_t *containers =
        (const kafs_tailmeta_container_hdr_t *)((const char *)region_hdr + table_off);
    kafs_blkcnt_t region_blo =
        (kafs_blkcnt_t)(tail_off >> kafs_sb_log_blksize_get(sb));
    kafs_blkcnt_t container_blo = kafs_ino_taildesc_v5_container_blo_get(taildesc);
    if (container_blo <= region_blo)
      rc = -EPROTO;

    uint32_t container_index = (uint32_t)(container_blo - region_blo - 1u);
    if (rc == 0 && container_index >= kafs_tailmeta_region_hdr_container_count_get(region_hdr))
      rc = -EPROTO;

    if (rc == 0)
    {
      const kafs_tailmeta_container_hdr_t *container = &containers[container_index];
      uint16_t class_bytes = kafs_tailmeta_container_hdr_class_bytes_get(container);
      uint16_t fragment_off = kafs_ino_taildesc_v5_fragment_off_get(taildesc);
      uint16_t slot_index = (uint16_t)(fragment_off / class_bytes);
      uint32_t payload_off = kafs_tailmeta_container_hdr_slot_table_off_get(container) +
                             kafs_tailmeta_container_hdr_slot_table_bytes_get(container) +
                             (uint32_t)slot_index * class_bytes;
      const char *payload = (const char *)region_hdr + payload_off;
      if (memcmp(payload, expected, expected_len) != 0)
        rc = -EIO;
      if (kafs_tailmeta_container_hdr_live_count_get(container) == 0u)
        rc = -EPROTO;
    }
  }

  munmap(base, (size_t)st.st_size);
  close(fd);
  return rc;
}

static const kafs_test_mount_options_t k_mount_options = {
    .debug = "1",
    .log_path = "v5_tail_smallfile.log",
    .timeout_ms = 5000,
};

int main(void)
{
  if (kafs_test_enter_tmpdir("v5_tail_smallfile") != 0)
    return 77;
  if (access("/dev/fuse", R_OK | W_OK) != 0)
    return 77;

  const char *img = "tail-small-v5.img";
  const char *mnt = "mnt-v5-tail";
  if (mkfs_v5_image(img) != 0)
  {
    tlogf("mkfs v5 image failed");
    return 77;
  }

  pid_t srv = kafs_test_start_kafs(img, mnt, &k_mount_options);
  if (srv <= 0)
  {
    tlogf("initial v5 scaffold mount failed");
    return 77;
  }

  char payload[300];
  for (size_t i = 0; i < sizeof(payload); ++i)
    payload[i] = (char)('a' + (i % 26));

  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/keep", mnt);
  int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0)
  {
    tlogf("create keep failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (write(fd, payload, sizeof(payload)) != (ssize_t)sizeof(payload))
  {
    tlogf("write keep failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);

  char verify[sizeof(payload)];
  fd = open(path, O_RDONLY);
  if (fd < 0)
  {
    tlogf("open keep for read failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (read(fd, verify, sizeof(verify)) != (ssize_t)sizeof(verify) ||
      memcmp(verify, payload, sizeof(payload)) != 0)
  {
    tlogf("readback mismatch on mounted keep");
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);

  if (unlink(path) != 0)
  {
    tlogf("unlink keep failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  snprintf(path, sizeof(path), "%s/t", mnt);
  fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0)
  {
    tlogf("create t failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (write(fd, payload, sizeof(payload)) != (ssize_t)sizeof(payload))
  {
    tlogf("write t failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);
  if (unlink(path) != 0)
  {
    tlogf("unlink t failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  snprintf(path, sizeof(path), "%s/keep", mnt);
  fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0)
  {
    tlogf("recreate keep failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (write(fd, payload, sizeof(payload)) != (ssize_t)sizeof(payload))
  {
    tlogf("rewrite keep failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);

  snprintf(path, sizeof(path), "%s/u", mnt);
  fd = open(path, O_CREAT | O_RDWR | O_TRUNC, 0644);
  if (fd < 0)
  {
    tlogf("create u failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (write(fd, payload, sizeof(payload)) != (ssize_t)sizeof(payload))
  {
    tlogf("write u failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (unlink(path) != 0)
  {
    tlogf("unlink u failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  memset(verify, 0, sizeof(verify));
  if (lseek(fd, 0, SEEK_SET) != 0 || read(fd, verify, sizeof(verify)) != (ssize_t)sizeof(verify) ||
      memcmp(verify, payload, sizeof(payload)) != 0)
  {
    tlogf("readback mismatch on open-unlinked u");
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);

  kafs_test_stop_kafs(mnt, srv);

  srv = kafs_test_start_kafs(img, mnt, &k_mount_options);
  if (srv <= 0)
  {
    tlogf("remount keep failed");
    return 1;
  }

  snprintf(path, sizeof(path), "%s/keep", mnt);
  fd = open(path, O_RDONLY);
  if (fd < 0)
  {
    tlogf("open keep after remount failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  memset(verify, 0, sizeof(verify));
  if (read(fd, verify, sizeof(verify)) != (ssize_t)sizeof(verify) ||
      memcmp(verify, payload, sizeof(payload)) != 0)
  {
    tlogf("readback mismatch on remounted keep");
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);
  kafs_test_stop_kafs(mnt, srv);

  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)img, NULL};
  int rc = run_cmd(fsck_argv);
  if (rc != 0)
  {
    tlogf("fsck failed after tail reclaim");
    return 1;
  }

  rc = inspect_tailmeta_live_slots_for_file(img, "keep", payload, sizeof(payload), 1u);
  if (rc != 0)
  {
    tlogf("offline tailmeta reclaim check failed rc=%d", rc);
    return 1;
  }

  tlogf("v5_tail_smallfile_smoketest OK");
  return 0;
}