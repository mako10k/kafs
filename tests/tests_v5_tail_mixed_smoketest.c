#include "test_utils.h"

#include "kafs_block.h"
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
      uint64_t container_live = 0;

      for (uint16_t slot_index = 0; slot_index < slot_count; ++slot_index)
      {
        if (kafs_tailmeta_slot_validate(&slots[slot_index], class_bytes) != 0)
        {
          rc = -EPROTO;
          break;
        }
        if (kafs_tailmeta_slot_owner_ino_get(&slots[slot_index]) != KAFS_INO_NONE)
        {
          live_slots++;
          container_live++;
        }
      }
      if (rc != 0)
        break;
      if (container_live != kafs_tailmeta_container_hdr_live_count_get(container))
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

static int inspect_mixed_tail_packed_file(const char *img, const char *expected,
                                          size_t expected_len, size_t blksize,
                                          uint64_t expected_live_slots)
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
  const kafs_ssuperblock_t *sb = (const kafs_ssuperblock_t *)base;
  if (kafs_sb_format_version_get(sb) != KAFS_FORMAT_VERSION_V5)
    rc = -EPROTO;

  const kafs_sinode_t *inoent = NULL;
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
      const kafs_sinode_t *candidate =
          (const kafs_sinode_t *)kafs_inode_ptr_in_table(inotbl, kafs_sb_format_version_get(sb), ino);
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

  const kafs_sinode_taildesc_v5_t *taildesc = NULL;
  if (rc == 0)
  {
    taildesc = &((const kafs_sinode_v5_t *)inoent)->i_taildesc;
    if (kafs_ino_taildesc_v5_layout_kind_get(taildesc) != KAFS_TAIL_LAYOUT_MIXED_FULL_TAIL)
      rc = -EPROTO;
    if (kafs_ino_taildesc_v5_fragment_len_get(taildesc) !=
        (uint16_t)(expected_len - blksize))
      rc = -EPROTO;
    if ((kafs_ino_taildesc_v5_flags_get(taildesc) & KAFS_TAILDESC_FLAG_FINAL_TAIL) == 0u)
      rc = -EPROTO;
    if (kafs_ino_blocks_get(inoent) != 1u)
      rc = -EPROTO;
    if (rc == 0)
    {
      kafs_blkcnt_t first_blo = kafs_blkcnt_stoh(inoent->i_blkreftbl[0]);
      const char *block_ptr;

      if (first_blo == KAFS_BLO_NONE || first_blo >= kafs_sb_blkcnt_get(sb))
        rc = -EPROTO;
      block_ptr = (const char *)base + (uint64_t)first_blo * blksize;
      if (rc == 0 && memcmp(block_ptr, expected, blksize) != 0)
        rc = -EIO;
    }
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
    kafs_blkcnt_t region_blo = (kafs_blkcnt_t)(tail_off >> kafs_sb_log_blksize_get(sb));
    kafs_blkcnt_t container_blo = kafs_ino_taildesc_v5_container_blo_get(taildesc);
    uint16_t fragment_len = kafs_ino_taildesc_v5_fragment_len_get(taildesc);

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
      uint16_t slot_index;
      uint32_t payload_off;
      const char *payload;

      if (class_bytes == 0u || fragment_off % class_bytes != 0u)
        rc = -EPROTO;
      slot_index = (uint16_t)(fragment_off / class_bytes);
      payload_off = kafs_tailmeta_container_hdr_slot_table_off_get(container) +
                    kafs_tailmeta_container_hdr_slot_table_bytes_get(container) +
                    (uint32_t)slot_index * class_bytes;
      payload = (const char *)region_hdr + payload_off;
      if (memcmp(payload, expected + blksize, fragment_len) != 0)
        rc = -EIO;
      if (kafs_tailmeta_container_hdr_live_count_get(container) == 0u)
        rc = -EPROTO;
    }
  }

  if (rc == 0)
  {
    uint64_t live_slots = 0;
    rc = inspect_tailmeta_live_slots(img, &live_slots);
    if (rc == 0 && live_slots != expected_live_slots)
      rc = -ERANGE;
  }

  munmap(base, (size_t)st.st_size);
  close(fd);
  return rc;
}

static const kafs_test_mount_options_t k_mount_options = {
    .debug = "1",
    .log_path = "v5_tail_mixed.log",
    .timeout_ms = 5000,
};

int main(void)
{
  enum
  {
    k_blksize = 4096,
    k_initial_size = k_blksize + 300,
    k_final_size = k_blksize + 200,
    k_tail_patch_off = k_blksize + 40,
    k_tail_patch_len = 96,
    k_append_len = 64,
  };

  if (kafs_test_enter_tmpdir("v5_tail_mixed") != 0)
    return 77;
  if (access("/dev/fuse", R_OK | W_OK) != 0)
    return 77;

  const char *img = "tail-mixed-v5.img";
  const char *mnt = "mnt-v5-mixed";
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

  char payload[k_initial_size + k_append_len];
  char verify[sizeof(payload)];
  char patch[k_tail_patch_len];
  char path[PATH_MAX];
  for (size_t i = 0; i < sizeof(payload); ++i)
    payload[i] = (char)('a' + (i % 26));
  for (size_t i = 0; i < sizeof(patch); ++i)
    patch[i] = (char)('Z' - (i % 26));

  snprintf(path, sizeof(path), "%s/mixed", mnt);
  int fd = open(path, O_CREAT | O_RDWR | O_TRUNC, 0644);
  if (fd < 0)
  {
    tlogf("create mixed failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (write(fd, payload, k_initial_size) != k_initial_size)
  {
    tlogf("initial write failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  if (pwrite(fd, patch, sizeof(patch), k_tail_patch_off) != (ssize_t)sizeof(patch))
  {
    tlogf("tail overwrite failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  memcpy(payload + k_tail_patch_off, patch, sizeof(patch));

  if (pwrite(fd, payload + k_initial_size, k_append_len, k_initial_size) != k_append_len)
  {
    tlogf("append failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  if (ftruncate(fd, k_final_size) != 0)
  {
    tlogf("truncate failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  close(fd);
  fd = open(path, O_RDONLY);
  if (fd < 0)
  {
    tlogf("reopen mixed failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  ssize_t nread = pread(fd, verify, k_final_size, 0);
  if (nread != k_final_size || memcmp(verify, payload, k_final_size) != 0)
  {
    size_t mismatch = 0;
    while (mismatch < k_final_size && verify[mismatch] == payload[mismatch])
      mismatch++;
    if (nread < 0)
      tlogf("pread failed on mounted mixed file: %s", strerror(errno));
    else if (mismatch < k_final_size)
      tlogf(
          "readback mismatch on mounted mixed file at off=%zu expected=%d actual=%d nread=%zd",
          mismatch, (int)(unsigned char)payload[mismatch], (int)(unsigned char)verify[mismatch],
          nread);
    else
      tlogf("readback mismatch on mounted mixed file nread=%zd", nread);
    {
      int inspect_rc = inspect_mixed_tail_packed_file(img, payload, k_final_size, k_blksize, 1u);
      tlogf("offline mixed inspection rc=%d", inspect_rc);
    }
    kafs_test_dump_log(k_mount_options.log_path, "mixed readback mismatch");
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
    tlogf("fsck failed after mixed tail persistence");
    return 1;
  }

  rc = inspect_mixed_tail_packed_file(img, payload, k_final_size, k_blksize, 1u);
  if (rc != 0)
  {
    tlogf("offline mixed tail inspection failed rc=%d", rc);
    return 1;
  }

  tlogf("v5_tail_mixed_smoketest OK");
  return 0;
}