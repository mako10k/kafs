#include "kafs_superblock.h"
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

int main(void)
{
  char mkfs_abs[PATH_MAX];
  char resize_abs[PATH_MAX];
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
    if (resolve_tool_path("KAFS_TEST_MKFS", mkfs_cands, mkfs_abs, sizeof(mkfs_abs)) != 0 ||
      resolve_tool_path("KAFS_TEST_KAFSRESIZE", resize_cands, resize_abs, sizeof(resize_abs)) != 0)
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

  return 0;
}
