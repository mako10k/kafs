#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_inode.h"
#include "kafs_dirent.h"
#include "test_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static void tlogf(const char *fmt, ...) {
  va_list ap; va_start(ap, fmt); vfprintf(stderr, fmt, ap); fputc('\n', stderr); va_end(ap);
}

static int is_mounted_fuse(const char *mnt) {
  char absmnt[PATH_MAX]; const char *want = mnt; if (realpath(mnt, absmnt) != NULL) want = absmnt;
  FILE *fp = fopen("/proc/mounts", "r"); if (!fp) return 0; char dev[256], dir[256], type[64]; int mounted = 0;
  while (fscanf(fp, "%255s %255s %63s %*[^\n]\n", dev, dir, type) == 3) {
    if (strcmp(dir, want) == 0 && strncmp(type, "fuse", 4) == 0) { mounted = 1; break; }
  }
  fclose(fp); return mounted;
}

static pid_t spawn_kafs(const char *img, const char *mnt, const char *debug) {
  mkdir(mnt, 0700);
  char absmnt[PATH_MAX];
  const char *mp = mnt;
  if (realpath(mnt, absmnt) != NULL) mp = absmnt;
  pid_t pid = fork(); if (pid < 0) return -errno;
  if (pid == 0) {
    setenv("KAFS_IMAGE", img, 1);
    if (debug) setenv("KAFS_DEBUG", debug, 1);
    int lfd = open("minisrv.log", O_CREAT|O_TRUNC|O_WRONLY, 0644);
    if (lfd >= 0) { dup2(lfd, STDERR_FILENO); dup2(lfd, STDOUT_FILENO); close(lfd); }
    char *args[] = {"./kafs", (char *)mp, "-f", NULL};
    execvp(args[0], args); _exit(127);
  }
  for (int i = 0; i < 100; ++i) { if (is_mounted_fuse(mnt)) return pid; struct timespec ts = {0, 100*1000*1000}; nanosleep(&ts, NULL); }
  kill(pid, SIGTERM); waitpid(pid, NULL, 0); return -1;
}

static void stop_kafs(const char *mnt, pid_t pid) {
  char absmnt[PATH_MAX];
  const char *mp = mnt;
  if (realpath(mnt, absmnt) != NULL) mp = absmnt;
  char *um1[] = {"fusermount3", "-u", (char *)mp, NULL};
  if (fork()==0){ execvp(um1[0], um1); _exit(127);} else { wait(NULL);} 
  kill(pid, SIGTERM); waitpid(pid, NULL, 0);
}

int main(void) {
  const unsigned log_bs = 12; // 4096
  const kafs_blksize_t bs = 1u << log_bs;
  const char *img = "prune.img";
  const char *mnt = "mnt-prune";

  kafs_context_t ctx; off_t mapsize;
  if (kafs_test_mkimg_with_hrl(img, 64u*1024u*1024u, log_bs, 4096, &ctx, &mapsize) != 0) {
    tlogf("mkimg failed"); return 77;
  }
  munmap(ctx.c_superblock, mapsize); close(ctx.c_fd);

  pid_t srv = spawn_kafs(img, mnt, "3");
  // 失敗時の解析を容易にするため、詳細ログを有効化
  if (srv <= 0) {
    // 起動失敗時でも minisrv.log があれば出力
    FILE *lf = fopen("minisrv.log", "r");
    if (lf) {
      tlogf("--- minisrv.log (on mount failure) ---");
      char line[512];
      while (fgets(line, sizeof(line), lf)) fputs(line, stderr);
      fclose(lf);
    }
  }
  if (srv <= 0) { tlogf("mount failed"); return 77; }

  // Create a file and place one block into the first single-indirect range
  char p[PATH_MAX]; snprintf(p, sizeof(p), "%s/file", mnt);
  int fd = open(p, O_CREAT|O_WRONLY, 0644); if (fd<0){ tlogf("create failed:%s", strerror(errno)); stop_kafs(mnt, srv); return 1; }
  off_t p_off = (off_t)12 * bs; // first single-indirect logical block
  char *buf = malloc(bs); memset(buf, 0xAB, bs); // non-zero content to allocate data + table
  ssize_t w = pwrite(fd, buf, bs, p_off); free(buf);
  if (w != (ssize_t)bs) {
    tlogf("pwrite data failed: %s", strerror(errno));
    close(fd);
    // クラッシュや切断時のログをダンプ
    FILE *lf = fopen("minisrv.log", "r");
    if (lf) {
      tlogf("--- minisrv.log (on write failure) ---");
      char line[512];
      while (fgets(line, sizeof(line), lf)) fputs(line, stderr);
      fclose(lf);
    }
    stop_kafs(mnt, srv);
    return 1;
  }

  // Now write a zero block at the same offset to trigger SET(NONE) and prune of the table
  char *z = calloc(1, bs);
  w = pwrite(fd, z, bs, p_off); free(z);
  if (w != (ssize_t)bs) { tlogf("pwrite zero failed: %s", strerror(errno)); close(fd); stop_kafs(mnt, srv); return 1; }
  fsync(fd); close(fd);

  stop_kafs(mnt, srv);

  // Reopen the image metadata to inspect the inode's single-indirect pointer [12]
  int ifd = open(img, O_RDONLY); if (ifd<0){ tlogf("open img failed:%s", strerror(errno)); return 1; }
  void *base = mmap(NULL, mapsize, PROT_READ, MAP_SHARED, ifd, 0);
  if (base == MAP_FAILED) { tlogf("mmap failed:%s", strerror(errno)); close(ifd); return 1; }
  kafs_ssuperblock_t *sb = (kafs_ssuperblock_t *)base;
  // test_utils laid out inotbl immediately after blkmask; recompute like test_utils did
  // But simpler: after mmap we can reconstruct from ctx by reopening; here we compute offset
  // Using the same logic is heavy; instead, reuse the inode table pointer from a temporary ctx
  // no need to build a full context here
  // in test image, c_inotbl starts at mapsize computed earlier in test_utils. We cannot recompute easily here.
  // Fall back to scanning root dir directly via sb and known layout is unreliable; better: open with same helper.
  // Minimal approach: the root directory content is kept in direct i_blkreftbl in this test, so we can locate the inode table by heuristic:
  // We know c_inotbl was placed at (char*)sb + inotbl_off where inotbl_off = aligned after blkmask.
  // Replicate a tiny subset to find it safely.
  kafs_blkcnt_t blkcnt = kafs_sb_blkcnt_get(sb);
  kafs_blksize_t bs_ro = kafs_sb_blksize_get(sb);
  kafs_blksize_t bmask = bs_ro - 1u;
  off_t off = 0;
  off = (off + sizeof(kafs_ssuperblock_t) + bmask) & ~bmask;
  off += ((blkcnt + 7) >> 3);
  off = (off + 7) & ~7;
  off = (off + bmask) & ~bmask;
  kafs_sinode_t *inotbl = (kafs_sinode_t *)((char *)base + off);
  kafs_sinode_t *root = &inotbl[KAFS_INO_ROOTDIR];

  // Walk root directory to find the created file's inode
  // The directory layout is simple append; we can scan entries sequentially
  // Read from offset 0 until we find our filename "file"
  const char *name = "file";
  size_t name_len = strlen(name);
  off_t doff = 0; kafs_sdirent_t d;
  kafs_inocnt_t ino = KAFS_INO_NONE;
  while (doff < (off_t)kafs_ino_size_get(root)) {
  size_t hdr = offsetof(kafs_sdirent_t, d_filename);
    memcpy(&d, (char *)root->i_blkreftbl + doff, hdr); // direct-only dirs in this test
    kafs_filenamelen_t dlen = kafs_dirent_filenamelen_get(&d);
    if (dlen == name_len && memcmp((char *)root->i_blkreftbl + doff + hdr, name, name_len) == 0) {
      ino = kafs_dirent_ino_get(&d); break;
    }
    doff += hdr + dlen;
  }
  if (ino == KAFS_INO_NONE) { tlogf("dir lookup failed"); munmap(base, mapsize); close(ifd); return 1; }

  kafs_sinode_t *fileino = &inotbl[ino];
  // After prune, i_blkreftbl[12] (single-indirect table pointer) must be NONE(0)
  if (kafs_blkcnt_stoh(fileino->i_blkreftbl[12]) != 0) {
    tlogf("expected i_blkreftbl[12]==0 after prune, got non-zero");
    munmap(base, mapsize); close(ifd); return 1;
  }

  munmap(base, mapsize); close(ifd);
  tlogf("prune_indirect_single OK");
  return 0;
}
