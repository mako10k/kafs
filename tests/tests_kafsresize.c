#include "kafs_superblock.h"
#include "kafs_block.h"
#include "kafs_dirent.h"
#include "kafs_inode.h"
#include "test_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static int run_cmd_status(char *const argv[])
{
  pid_t pid = fork();
  if (pid < 0)
    return -errno;
  if (pid == 0)
  {
    execvp(argv[0], argv);
    _exit(127);
  }

  int st = 0;
  if (waitpid(pid, &st, 0) < 0)
    return -errno;
  if (WIFEXITED(st))
    return WEXITSTATUS(st);
  return 255;
}

static int run_cmd_status_with_stdin(char *const argv[], const char *stdin_data)
{
  int pipefd[2];
  if (pipe(pipefd) != 0)
    return -errno;

  pid_t pid = fork();
  if (pid < 0)
  {
    int saved = errno;
    close(pipefd[0]);
    close(pipefd[1]);
    return -saved;
  }
  if (pid == 0)
  {
    close(pipefd[1]);
    if (dup2(pipefd[0], STDIN_FILENO) < 0)
      _exit(127);
    close(pipefd[0]);
    execvp(argv[0], argv);
    _exit(127);
  }

  close(pipefd[0]);
  if (stdin_data && *stdin_data)
  {
    size_t len = strlen(stdin_data);
    ssize_t wr = write(pipefd[1], stdin_data, len);
    if (wr < 0 || (size_t)wr != len)
    {
      int saved = errno;
      close(pipefd[1]);
      (void)waitpid(pid, NULL, 0);
      return -saved;
    }
  }
  close(pipefd[1]);

  int st = 0;
  if (waitpid(pid, &st, 0) < 0)
    return -errno;
  if (WIFEXITED(st))
    return WEXITSTATUS(st);
  return 255;
}

static int run_cmd_capture_stdout(char *const argv[], char *stdout_buf, size_t stdout_buf_sz)
{
  int pipefd[2];
  if (pipe(pipefd) != 0)
    return -errno;

  pid_t pid = fork();
  if (pid < 0)
  {
    int saved = errno;
    close(pipefd[0]);
    close(pipefd[1]);
    return -saved;
  }
  if (pid == 0)
  {
    close(pipefd[0]);
    if (dup2(pipefd[1], STDOUT_FILENO) < 0)
      _exit(127);
    close(pipefd[1]);
    execvp(argv[0], argv);
    _exit(127);
  }

  close(pipefd[1]);
  size_t used = 0;
  while (stdout_buf && used + 1 < stdout_buf_sz)
  {
    ssize_t n = read(pipefd[0], stdout_buf + used, stdout_buf_sz - used - 1);
    if (n < 0)
    {
      int saved = errno;
      close(pipefd[0]);
      (void)waitpid(pid, NULL, 0);
      return -saved;
    }
    if (n == 0)
      break;
    used += (size_t)n;
  }
  if (stdout_buf && stdout_buf_sz > 0)
    stdout_buf[used] = '\0';
  close(pipefd[0]);

  int st = 0;
  if (waitpid(pid, &st, 0) < 0)
    return -errno;
  if (WIFEXITED(st))
    return WEXITSTATUS(st);
  return 255;
}

static int to_abs_path(const char *in, char *out, size_t out_sz)
{
  if (!in || !*in || !out || out_sz == 0)
    return -EINVAL;
  if (in[0] == '/')
  {
    if (snprintf(out, out_sz, "%s", in) >= (int)out_sz)
      return -ENAMETOOLONG;
    return 0;
  }
  char cwd[PATH_MAX];
  if (!getcwd(cwd, sizeof(cwd)))
    return -errno;
  if (snprintf(out, out_sz, "%s/%s", cwd, in) >= (int)out_sz)
    return -ENAMETOOLONG;
  return 0;
}

static int resolve_tool_path(const char *env_name,
                             const char *const *candidates,
                             char *out,
                             size_t out_sz)
{
  const char *envv = getenv(env_name);
  if (envv && *envv)
  {
    if (strchr(envv, '/'))
    {
      if (to_abs_path(envv, out, out_sz) == 0 && access(out, X_OK) == 0)
        return 0;
    }
    else if (snprintf(out, out_sz, "%s", envv) < (int)out_sz)
    {
      return 0;
    }
  }

  for (size_t i = 0; candidates[i] != NULL; ++i)
  {
    const char *cand = candidates[i];
    if (strchr(cand, '/'))
    {
      char absbuf[PATH_MAX];
      if (to_abs_path(cand, absbuf, sizeof(absbuf)) != 0)
        continue;
      if (access(absbuf, X_OK) != 0)
        continue;
      if (snprintf(out, out_sz, "%s", absbuf) >= (int)out_sz)
        return -ENAMETOOLONG;
      return 0;
    }

    if (snprintf(out, out_sz, "%s", cand) >= (int)out_sz)
      return -ENAMETOOLONG;
    return 0;
  }

  return -ENOENT;
}

static int read_superblock(const char *img, kafs_ssuperblock_t *sb)
{
  int fd = open(img, O_RDONLY);
  if (fd < 0)
    return -errno;
  ssize_t n = pread(fd, sb, sizeof(*sb), 0);
  int saved = errno;
  close(fd);
  if (n != (ssize_t)sizeof(*sb))
    return (n < 0) ? -saved : -EIO;
  return 0;
}

static int write_superblock(const char *img, const kafs_ssuperblock_t *sb)
{
  int fd = open(img, O_RDWR);
  if (fd < 0)
    return -errno;
  ssize_t n = pwrite(fd, sb, sizeof(*sb), 0);
  int saved = errno;
  close(fd);
  if (n != (ssize_t)sizeof(*sb))
    return (n < 0) ? -saved : -EIO;
  return 0;
}

static int write_inode_at(const char *img, kafs_inocnt_t ino, const kafs_sinode_t *inode)
{
  kafs_ssuperblock_t sb = {0};
  int rc = read_superblock(img, &sb);
  if (rc != 0)
    return rc;

  uint64_t blksize = 1u << kafs_sb_log_blksize_get(&sb);
  uint64_t blksizemask = blksize - 1u;
  uint64_t r_blkcnt = kafs_sb_r_blkcnt_get(&sb);
  uint64_t layout = sizeof(kafs_ssuperblock_t);
  layout = (layout + blksizemask) & ~blksizemask;
  layout += (r_blkcnt + 7u) >> 3;
  layout = (layout + 7u) & ~7u;
  layout = (layout + blksizemask) & ~blksizemask;
  uint64_t inode_off = 0;

  rc = kafs_inode_offset_for_format(kafs_sb_format_version_get(&sb), layout, ino, &inode_off);
  if (rc != 0)
    return rc;

  int fd = open(img, O_RDWR);
  if (fd < 0)
    return -errno;
  ssize_t n = pwrite(fd, inode, sizeof(*inode), (off_t)inode_off);
  int saved = errno;
  close(fd);
  if (n != (ssize_t)sizeof(*inode))
    return (n < 0) ? -saved : -EIO;
  return 0;
}

static int read_inode_at(const char *img, kafs_inocnt_t ino, kafs_sinode_t *inode)
{
  kafs_ssuperblock_t sb = {0};
  int rc = read_superblock(img, &sb);
  if (rc != 0)
    return rc;

  uint64_t blksize = 1u << kafs_sb_log_blksize_get(&sb);
  uint64_t blksizemask = blksize - 1u;
  uint64_t r_blkcnt = kafs_sb_r_blkcnt_get(&sb);
  uint64_t layout = sizeof(kafs_ssuperblock_t);
  layout = (layout + blksizemask) & ~blksizemask;
  layout += (r_blkcnt + 7u) >> 3;
  layout = (layout + 7u) & ~7u;
  layout = (layout + blksizemask) & ~blksizemask;
  uint64_t inode_off = 0;

  rc = kafs_inode_offset_for_format(kafs_sb_format_version_get(&sb), layout, ino, &inode_off);
  if (rc != 0)
    return rc;

  int fd = open(img, O_RDONLY);
  if (fd < 0)
    return -errno;
  ssize_t n = pread(fd, inode, sizeof(*inode), (off_t)inode_off);
  int saved = errno;
  close(fd);
  if (n != (ssize_t)sizeof(*inode))
    return (n < 0) ? -saved : -EIO;
  return 0;
}

struct legacy_dir_entry_spec
{
  kafs_inocnt_t ino;
  const char *name;
};

static uint32_t test_dirent_name_hash(const char *name, kafs_filenamelen_t namelen)
{
  uint32_t hash = 2166136261u;
  for (kafs_filenamelen_t i = 0; i < namelen; ++i)
  {
    hash ^= (uint8_t)name[i];
    hash *= 16777619u;
  }
  return hash;
}

static int write_legacy_dir_inline(const char *img, kafs_inocnt_t ino, kafs_mode_t mode,
                                   const struct legacy_dir_entry_spec *entries, size_t entry_count)
{
  kafs_sinode_t inode = {0};
  int rc = read_inode_at(img, ino, &inode);
  if (rc != 0 && ino == KAFS_INO_ROOTDIR)
    return rc;
  if (rc != 0)
    memset(&inode, 0, sizeof(inode));

  memset(inode.i_blkreftbl, 0, sizeof(inode.i_blkreftbl));
  kafs_ino_mode_set(&inode, mode);
  kafs_ino_linkcnt_set(&inode, 1);
  kafs_ino_blocks_set(&inode, 0);
  kafs_ino_dtime_set(&inode, (kafs_time_t){0});

  size_t off = 0;
  for (size_t i = 0; i < entry_count; ++i)
  {
    size_t name_len = strlen(entries[i].name);
    size_t rec_len = sizeof(kafs_sinocnt_t) + sizeof(kafs_sfilenamelen_t) + name_len;
    if (off + rec_len > sizeof(inode.i_blkreftbl))
      return -ENOSPC;
    kafs_sinocnt_t d_ino = kafs_inocnt_htos(entries[i].ino);
    kafs_sfilenamelen_t d_namelen = kafs_filenamelen_htos((kafs_filenamelen_t)name_len);
    memcpy((char *)inode.i_blkreftbl + off, &d_ino, sizeof(d_ino));
    off += sizeof(d_ino);
    memcpy((char *)inode.i_blkreftbl + off, &d_namelen, sizeof(d_namelen));
    off += sizeof(d_namelen);
    memcpy((char *)inode.i_blkreftbl + off, entries[i].name, name_len);
    off += name_len;
  }

  kafs_ino_size_set(&inode, (kafs_off_t)off);
  return write_inode_at(img, ino, &inode);
}

static int write_regular_inode(const char *img, kafs_inocnt_t ino)
{
  kafs_sinode_t inode;
  memset(&inode, 0, sizeof(inode));
  kafs_ino_mode_set(&inode, S_IFREG | 0644);
  kafs_ino_linkcnt_set(&inode, 1);
  kafs_ino_size_set(&inode, 0);
  return write_inode_at(img, ino, &inode);
}

static int read_inode_data(const char *img, kafs_inocnt_t ino, void *buf, size_t cap, size_t *out_sz)
{
  kafs_ssuperblock_t sb = {0};
  kafs_sinode_t inode = {0};
  if (read_superblock(img, &sb) != 0 || read_inode_at(img, ino, &inode) != 0)
    return -EIO;

  size_t size = (size_t)kafs_ino_size_get(&inode);
  if (size > cap)
    return -ENOSPC;
  if (size <= sizeof(inode.i_blkreftbl))
  {
    memcpy(buf, inode.i_blkreftbl, size);
    *out_sz = size;
    return 0;
  }

  kafs_blkcnt_t blo = kafs_blkcnt_stoh(inode.i_blkreftbl[0]);
  if (blo == KAFS_BLO_NONE)
    return -EIO;

  int fd = open(img, O_RDONLY);
  if (fd < 0)
    return -errno;
  ssize_t n = pread(fd, buf, size, (off_t)blo << kafs_sb_log_blksize_get(&sb));
  int saved = errno;
  close(fd);
  if (n != (ssize_t)size)
    return (n < 0) ? -saved : -EIO;
  *out_sz = size;
  return 0;
}

static int verify_v4_dir(const char *img, kafs_inocnt_t ino,
                         const struct legacy_dir_entry_spec *entries, size_t entry_count)
{
  char buf[512];
  size_t len = 0;
  if (read_inode_data(img, ino, buf, sizeof(buf), &len) != 0)
    return -1;
  if (len < sizeof(kafs_sdir_v4_hdr_t))
    return -1;

  const kafs_sdir_v4_hdr_t *hdr = (const kafs_sdir_v4_hdr_t *)buf;
  if (kafs_u32_stoh(hdr->dh_magic) != KAFS_DIRENT_V4_MAGIC)
    return -1;
  if (kafs_dir_v4_hdr_live_count_get(hdr) != (uint32_t)entry_count)
    return -1;
  if (kafs_dir_v4_hdr_tombstone_count_get(hdr) != 0)
    return -1;

  size_t off = sizeof(*hdr);
  for (size_t i = 0; i < entry_count; ++i)
  {
    if (off + offsetof(kafs_sdirent_v4_t, de_filename) > len)
      return -1;
    const kafs_sdirent_v4_t *rec = (const kafs_sdirent_v4_t *)(buf + off);
    size_t rec_len = (size_t)kafs_dirent_v4_rec_len_get(rec);
    size_t name_len = strlen(entries[i].name);
    if (rec_len != offsetof(kafs_sdirent_v4_t, de_filename) + name_len)
      return -1;
    if (kafs_dirent_v4_ino_get(rec) != entries[i].ino)
      return -1;
    if (kafs_dirent_v4_filenamelen_get(rec) != (kafs_filenamelen_t)name_len)
      return -1;
    if (kafs_dirent_v4_name_hash_get(rec) !=
        test_dirent_name_hash(entries[i].name, (kafs_filenamelen_t)name_len))
      return -1;
    if (memcmp(rec->de_filename, entries[i].name, name_len) != 0)
      return -1;
    off += rec_len;
  }
  return 0;
}

int main(void)
{
  char mkfs_abs[PATH_MAX];
  char resize_abs[PATH_MAX];
  char info_abs[PATH_MAX];
  char kafsctl_abs[PATH_MAX];
  char kafs_abs[PATH_MAX];
  const char *mkfs_cands[] = {
      "../src/mkfs.kafs",
      "./src/mkfs.kafs",
      "./mkfs.kafs",
      "mkfs.kafs",
      NULL};
  const char *resize_cands[] = {
      "../src/kafsresize",
      "./src/kafsresize",
      "./kafsresize",
      "kafsresize",
      NULL};
  const char *info_cands[] = {
      "../src/kafs-info",
      "./src/kafs-info",
      "./kafs-info",
      "kafs-info",
      NULL};
    const char *kafsctl_cands[] = {
      "../src/kafsctl",
      "./src/kafsctl",
      "./kafsctl",
      "kafsctl",
      NULL};
    const char *kafs_cands[] = {
      "../src/kafs",
      "./src/kafs",
      "./kafs",
      "kafs",
      NULL};
  if (resolve_tool_path("KAFS_TEST_MKFS", mkfs_cands, mkfs_abs, sizeof(mkfs_abs)) != 0 ||
      resolve_tool_path("KAFS_TEST_KAFSRESIZE", resize_cands, resize_abs, sizeof(resize_abs)) != 0 ||
      resolve_tool_path("KAFS_TEST_KAFSINFO", info_cands, info_abs, sizeof(info_abs)) != 0 ||
      resolve_tool_path("KAFS_TEST_KAFSCTL", kafsctl_cands, kafsctl_abs, sizeof(kafsctl_abs)) != 0 ||
      resolve_tool_path("KAFS_TEST_KAFS", kafs_cands, kafs_abs, sizeof(kafs_abs)) != 0)
  {
    fprintf(stderr, "failed to resolve test tool paths\n");
    return 1;
  }

  if (kafs_test_enter_tmpdir("kafsresize") != 0)
  {
    fprintf(stderr, "failed to enter tmpdir\n");
    return 1;
  }

  const char *img = "resize.img";
  char *mkfs_argv[] = {(char *)mkfs_abs, (char *)img, (char *)"-s", (char *)"128M", NULL};
  if (run_cmd_status(mkfs_argv) != 0)
  {
    fprintf(stderr, "mkfs failed\n");
    return 1;
  }

  char *mkfs_prompt_argv[] = {(char *)mkfs_abs, (char *)img, NULL};
  if (run_cmd_status_with_stdin(mkfs_prompt_argv, NULL) == 0)
  {
    fprintf(stderr, "mkfs unexpectedly reformatted without confirmation input\n");
    return 1;
  }

  if (run_cmd_status_with_stdin(mkfs_prompt_argv, "n\n") == 0)
  {
    fprintf(stderr, "mkfs unexpectedly reformatted after negative confirmation\n");
    return 1;
  }

  if (run_cmd_status_with_stdin(mkfs_prompt_argv, "\n") != 0)
  {
    fprintf(stderr, "mkfs failed to accept default yes confirmation\n");
    return 1;
  }

  char *mkfs_yes_argv[] = {(char *)mkfs_abs, (char *)"--yes", (char *)img, NULL};
  if (run_cmd_status_with_stdin(mkfs_yes_argv, NULL) != 0)
  {
    fprintf(stderr, "mkfs --yes failed to skip overwrite confirmation\n");
    return 1;
  }

  kafs_ssuperblock_t sb = {0};
  if (read_superblock(img, &sb) != 0)
  {
    fprintf(stderr, "failed to read superblock\n");
    return 1;
  }

  if (kafs_sb_magic_get(&sb) != KAFS_MAGIC ||
      kafs_sb_format_version_get(&sb) != KAFS_FORMAT_VERSION)
  {
    fprintf(stderr, "unexpected image format\n");
    return 1;
  }

  uint32_t blksize = (uint32_t)(1u << kafs_sb_log_blksize_get(&sb));
  uint32_t root_blkcnt = (uint32_t)kafs_sb_r_blkcnt_get(&sb);
  uint32_t old_blkcnt = (uint32_t)kafs_sb_blkcnt_get(&sb);
  uint32_t old_free = (uint32_t)kafs_sb_blkcnt_free_get(&sb);

  if (old_blkcnt != root_blkcnt)
  {
    fprintf(stderr, "expected no-headroom mkfs image (blkcnt=%" PRIu32 ", r_blkcnt=%" PRIu32 ")\n",
            old_blkcnt, root_blkcnt);
    return 1;
  }

  // no-headroom image must fail
  char target_fail_buf[32];
  snprintf(target_fail_buf, sizeof(target_fail_buf), "%" PRIu64,
           (uint64_t)(old_blkcnt + 1u) * (uint64_t)blksize);
  char *resize_fail_argv[] = {(char *)resize_abs, (char *)"--grow", (char *)"--size-bytes",
                              target_fail_buf, (char *)img, NULL};
  if (run_cmd_status(resize_fail_argv) == 0)
  {
    fprintf(stderr, "resize unexpectedly succeeded without headroom\n");
    return 1;
  }

  if (old_blkcnt < 200u)
  {
    fprintf(stderr, "image too small for headroom patch\n");
    return 1;
  }

  // create synthetic headroom by lowering s_blkcnt and bumping free counter
  uint32_t patched_blkcnt = old_blkcnt - 100u;
  sb.s_blkcnt = kafs_blkcnt_htos((kafs_blkcnt_t)patched_blkcnt);
  sb.s_blkcnt_free = kafs_blkcnt_htos((kafs_blkcnt_t)(old_free + 100u));
  if (write_superblock(img, &sb) != 0)
  {
    fprintf(stderr, "failed to patch superblock\n");
    return 1;
  }

  uint32_t grow_to = patched_blkcnt + 80u;
  char target_ok_buf[32];
  snprintf(target_ok_buf, sizeof(target_ok_buf), "%" PRIu64,
           (uint64_t)grow_to * (uint64_t)blksize);
  char *resize_ok_argv[] = {(char *)resize_abs, (char *)"--grow", (char *)"--size-bytes",
                            target_ok_buf, (char *)img, NULL};
  if (run_cmd_status(resize_ok_argv) != 0)
  {
    fprintf(stderr, "resize failed in headroom path\n");
    return 1;
  }

  kafs_ssuperblock_t sb_after = {0};
  if (read_superblock(img, &sb_after) != 0)
  {
    fprintf(stderr, "failed to read updated superblock\n");
    return 1;
  }

  uint32_t new_blkcnt = (uint32_t)kafs_sb_blkcnt_get(&sb_after);
  uint32_t new_free = (uint32_t)kafs_sb_blkcnt_free_get(&sb_after);
  if (new_blkcnt != grow_to)
  {
    fprintf(stderr, "unexpected blkcnt after grow: got=%" PRIu32 " want=%" PRIu32 "\n",
            new_blkcnt, grow_to);
    return 1;
  }
  if (new_free != (old_free + 180u))
  {
    fprintf(stderr, "unexpected free count after grow: got=%" PRIu32 " want=%" PRIu32 "\n",
            new_free, old_free + 180u);
    return 1;
  }

  // target beyond root bitmap capacity must fail
  char target_over_buf[32];
  snprintf(target_over_buf, sizeof(target_over_buf), "%" PRIu64,
           (uint64_t)(root_blkcnt + 1u) * (uint64_t)blksize);
  char *resize_over_argv[] = {(char *)resize_abs, (char *)"--grow", (char *)"--size-bytes",
                              target_over_buf, (char *)img, NULL};
  if (run_cmd_status(resize_over_argv) == 0)
  {
    fprintf(stderr, "resize unexpectedly succeeded over headroom limit\n");
    return 1;
  }

  const char *dst_img = "migrate-dst.img";
  int dst_fd = open(dst_img, O_RDWR | O_CREAT | O_TRUNC, 0600);
  if (dst_fd < 0)
  {
    fprintf(stderr, "failed to create migrate dst image\n");
    return 1;
  }
  if (ftruncate(dst_fd, 64 * 1024 * 1024) != 0)
  {
    fprintf(stderr, "failed to size migrate dst image\n");
    close(dst_fd);
    return 1;
  }
  close(dst_fd);

  char migrate_stdout[4096];
  char *migrate_create_argv[] = {(char *)resize_abs,
                                 (char *)"--migrate-create",
                                 (char *)"--dst-image",
                                 (char *)dst_img,
                                 (char *)"--force",
                                 (char *)"--inodes",
                                 (char *)"4096",
                                 (char *)"--src-mount",
                                 (char *)"/srcmnt",
                                 (char *)"--dst-mount",
                                 (char *)"/dstmnt",
                                 (char *)"--yes",
                                 NULL};
  if (run_cmd_capture_stdout(migrate_create_argv, migrate_stdout, sizeof(migrate_stdout)) != 0)
  {
    fprintf(stderr, "migrate-create failed\n");
    return 1;
  }

  kafs_ssuperblock_t migrate_sb = {0};
  if (read_superblock(dst_img, &migrate_sb) != 0)
  {
    fprintf(stderr, "failed to read migrate-create superblock\n");
    return 1;
  }
  if (kafs_sb_magic_get(&migrate_sb) != KAFS_MAGIC ||
      kafs_sb_format_version_get(&migrate_sb) != KAFS_FORMAT_VERSION ||
      kafs_sb_inocnt_get(&migrate_sb) != 4096)
  {
    fprintf(stderr, "unexpected migrate-create image format\n");
    return 1;
  }
  if (!strstr(migrate_stdout, "initial seed copy:") ||
      !strstr(migrate_stdout, "--inplace --no-whole-file") ||
      !strstr(migrate_stdout, "sudo rsync -aHAX --numeric-ids --delete /srcmnt/ /dstmnt/"))
  {
    fprintf(stderr, "migrate-create output missing low-transfer rsync guidance\n");
    return 1;
  }

  const char *info_img = "info-tombstone.img";
  char *info_mkfs_argv[] = {(char *)mkfs_abs, (char *)info_img, (char *)"-s", (char *)"32M", NULL};
  if (run_cmd_status(info_mkfs_argv) != 0)
  {
    fprintf(stderr, "mkfs for kafs-info test failed\n");
    return 1;
  }

  kafs_ssuperblock_t info_sb = {0};
  if (read_superblock(info_img, &info_sb) != 0)
  {
    fprintf(stderr, "failed to read default-inode superblock\n");
    return 1;
  }
  if (kafs_sb_inocnt_get(&info_sb) != 2048)
  {
      fprintf(stderr, "unexpected default inode count for 32M image: got=%lu want=2048\n",
        (unsigned long)kafs_sb_inocnt_get(&info_sb));
    return 1;
  }

  const char *small_img = "default-inodes-small.img";
  char *small_mkfs_argv[] = {(char *)mkfs_abs, (char *)small_img, (char *)"-s", (char *)"5M", NULL};
  if (run_cmd_status(small_mkfs_argv) != 0)
  {
    fprintf(stderr, "mkfs for small-image default inode count test failed\n");
    return 1;
  }

  kafs_ssuperblock_t small_sb = {0};
  if (read_superblock(small_img, &small_sb) != 0)
  {
    fprintf(stderr, "failed to read small-image default inode superblock\n");
    return 1;
  }
  if (kafs_sb_inocnt_get(&small_sb) != 320)
  {
    fprintf(stderr, "unexpected default inode count for 5M image: got=%lu want=320\n",
            (unsigned long)kafs_sb_inocnt_get(&small_sb));
    return 1;
  }

  kafs_sinode_t tombstone_1;
  memset(&tombstone_1, 0, sizeof(tombstone_1));
  kafs_ino_mode_set(&tombstone_1, S_IFREG | 0644);
  kafs_ino_linkcnt_set(&tombstone_1, 0);
  kafs_ino_dtime_set(&tombstone_1, (kafs_time_t){.tv_sec = 111, .tv_nsec = 7});
  if (write_inode_at(info_img, KAFS_INO_ROOTDIR + 1u, &tombstone_1) != 0)
  {
    fprintf(stderr, "failed to write first tombstone inode\n");
    return 1;
  }

  kafs_sinode_t tombstone_2;
  memset(&tombstone_2, 0, sizeof(tombstone_2));
  kafs_ino_mode_set(&tombstone_2, S_IFREG | 0644);
  kafs_ino_linkcnt_set(&tombstone_2, 0);
  kafs_ino_dtime_set(&tombstone_2, (kafs_time_t){.tv_sec = 222, .tv_nsec = 9});
  if (write_inode_at(info_img, KAFS_INO_ROOTDIR + 2u, &tombstone_2) != 0)
  {
    fprintf(stderr, "failed to write second tombstone inode\n");
    return 1;
  }

  kafs_sinode_t not_tombstone;
  memset(&not_tombstone, 0, sizeof(not_tombstone));
  kafs_ino_mode_set(&not_tombstone, S_IFREG | 0644);
  kafs_ino_linkcnt_set(&not_tombstone, 0);
  if (write_inode_at(info_img, KAFS_INO_ROOTDIR + 3u, &not_tombstone) != 0)
  {
    fprintf(stderr, "failed to write non-tombstone inode\n");
    return 1;
  }

  char info_stdout[4096];
  char *info_argv[] = {(char *)info_abs, (char *)info_img, NULL};
  if (run_cmd_capture_stdout(info_argv, info_stdout, sizeof(info_stdout)) != 0)
  {
    fprintf(stderr, "kafs-info failed on tombstone image\n");
    return 1;
  }
  if (!strstr(info_stdout, "tombstones count=2") || !strstr(info_stdout, "(111.000000007)"))
  {
    fprintf(stderr, "kafs-info output missing tombstone summary: %s\n", info_stdout);
    return 1;
  }

  const char *legacy_img = "legacy-v3.img";
  char *legacy_mkfs_argv[] = {(char *)mkfs_abs, (char *)legacy_img, (char *)"-s", (char *)"32M",
                              NULL};
  if (run_cmd_status(legacy_mkfs_argv) != 0)
  {
    fprintf(stderr, "mkfs for legacy migration test failed\n");
    return 1;
  }

  struct legacy_dir_entry_spec root_entries[] = {{2u, "a"}, {3u, "b"}, {4u, "c"}};
  struct legacy_dir_entry_spec child_entries[] = {{1u, ".."}};
  if (write_legacy_dir_inline(legacy_img, KAFS_INO_ROOTDIR, S_IFDIR | 0755, root_entries,
                              sizeof(root_entries) / sizeof(root_entries[0])) != 0 ||
      write_legacy_dir_inline(legacy_img, 2u, S_IFDIR | 0755, child_entries,
                              sizeof(child_entries) / sizeof(child_entries[0])) != 0 ||
      write_regular_inode(legacy_img, 3u) != 0 || write_regular_inode(legacy_img, 4u) != 0)
  {
    fprintf(stderr, "failed to synthesize legacy v3 image\n");
    return 1;
  }

  kafs_ssuperblock_t legacy_sb = {0};
  if (read_superblock(legacy_img, &legacy_sb) != 0)
  {
    fprintf(stderr, "failed to read legacy image superblock\n");
    return 1;
  }
  kafs_sb_format_version_set(&legacy_sb, KAFS_FORMAT_VERSION_V3);
  if (write_superblock(legacy_img, &legacy_sb) != 0)
  {
    fprintf(stderr, "failed to downgrade superblock to v3\n");
    return 1;
  }

  char *migrate_cancel_argv[] = {(char *)kafsctl_abs, (char *)"migrate", (char *)legacy_img, NULL};
  if (run_cmd_status_with_stdin(migrate_cancel_argv, "n\n") == 0)
  {
    fprintf(stderr, "kafsctl migrate unexpectedly succeeded after negative confirmation\n");
    return 1;
  }
  if (read_superblock(legacy_img, &legacy_sb) != 0 ||
      kafs_sb_format_version_get(&legacy_sb) != KAFS_FORMAT_VERSION_V3)
  {
    fprintf(stderr, "legacy image changed after canceled migration\n");
    return 1;
  }

  char *migrate_yes_argv[] = {(char *)kafsctl_abs, (char *)"migrate", (char *)legacy_img,
                              (char *)"--yes", NULL};
  if (run_cmd_status(migrate_yes_argv) != 0)
  {
    fprintf(stderr, "kafsctl migrate --yes failed\n");
    return 1;
  }
  if (read_superblock(legacy_img, &legacy_sb) != 0 ||
      kafs_sb_format_version_get(&legacy_sb) != KAFS_FORMAT_VERSION)
  {
    fprintf(stderr, "legacy image was not migrated to v4\n");
    return 1;
  }
  if (verify_v4_dir(legacy_img, KAFS_INO_ROOTDIR, root_entries,
                    sizeof(root_entries) / sizeof(root_entries[0])) != 0 ||
      verify_v4_dir(legacy_img, 2u, child_entries, sizeof(child_entries) / sizeof(child_entries[0])) !=
          0)
  {
    fprintf(stderr, "migrated directory image does not match v4 layout\n");
    return 1;
  }
  kafs_sinode_t migrated_root = {0};
  if (read_inode_at(legacy_img, KAFS_INO_ROOTDIR, &migrated_root) != 0 ||
      kafs_ino_size_get(&migrated_root) <= sizeof(migrated_root.i_blkreftbl) ||
      kafs_blkcnt_stoh(migrated_root.i_blkreftbl[0]) == KAFS_BLO_NONE)
  {
    fprintf(stderr, "migrated root directory did not grow into block-backed v4 storage\n");
    return 1;
  }

  const char *startup_img = "legacy-startup.img";
  char *startup_mkfs_argv[] = {(char *)mkfs_abs, (char *)startup_img, (char *)"-s", (char *)"32M",
                               NULL};
  if (run_cmd_status(startup_mkfs_argv) != 0 ||
      write_legacy_dir_inline(startup_img, KAFS_INO_ROOTDIR, S_IFDIR | 0755, root_entries,
                              sizeof(root_entries) / sizeof(root_entries[0])) != 0 ||
      write_legacy_dir_inline(startup_img, 2u, S_IFDIR | 0755, child_entries,
                              sizeof(child_entries) / sizeof(child_entries[0])) != 0 ||
      write_regular_inode(startup_img, 3u) != 0 || write_regular_inode(startup_img, 4u) != 0)
  {
    fprintf(stderr, "failed to synthesize startup migration image\n");
    return 1;
  }
  if (read_superblock(startup_img, &legacy_sb) != 0)
  {
    fprintf(stderr, "failed to read startup image superblock\n");
    return 1;
  }
  kafs_sb_format_version_set(&legacy_sb, KAFS_FORMAT_VERSION_V3);
  if (write_superblock(startup_img, &legacy_sb) != 0)
  {
    fprintf(stderr, "failed to downgrade startup image to v3\n");
    return 1;
  }
  if (mkdir("mnt", 0700) != 0)
  {
    fprintf(stderr, "failed to create temporary mountpoint\n");
    return 1;
  }
  char *startup_migrate_argv[] = {(char *)kafs_abs,
                                  (char *)"--image",
                                  (char *)startup_img,
                                  (char *)"--migrate",
                                  (char *)"--yes",
                                  (char *)"mnt",
                                  NULL};
  if (run_cmd_status(startup_migrate_argv) != 0)
  {
    fprintf(stderr, "kafs --migrate --yes failed\n");
    return 1;
  }
  if (read_superblock(startup_img, &legacy_sb) != 0 ||
      kafs_sb_format_version_get(&legacy_sb) != KAFS_FORMAT_VERSION ||
      verify_v4_dir(startup_img, KAFS_INO_ROOTDIR, root_entries,
                    sizeof(root_entries) / sizeof(root_entries[0])) != 0)
  {
    fprintf(stderr, "startup migration did not produce a valid v4 image\n");
    return 1;
  }

  return 0;
}
