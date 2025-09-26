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
  char absmnt[PATH_MAX]; const char *mp = mnt; if (realpath(mnt, absmnt) != NULL) mp = absmnt;
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
  char absmnt[PATH_MAX]; const char *mp = mnt; if (realpath(mnt, absmnt) != NULL) mp = absmnt;
  char *um1[] = {"fusermount3", "-u", (char *)mp, NULL};
  if (fork()==0){ execvp(um1[0], um1); _exit(127);} else { wait(NULL);} 
  kill(pid, SIGTERM); waitpid(pid, NULL, 0);
}

static int lookup_file_ino(void *base, off_t mapsize, const char *name, kafs_sinode_t **out_inotbl, kafs_sinode_t **out_fileino) {
  (void)mapsize;
  kafs_ssuperblock_t *sb = (kafs_ssuperblock_t *)base;
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

  size_t name_len = strlen(name);
  off_t doff = 0; kafs_sdirent_t d; kafs_inocnt_t ino = KAFS_INO_NONE;
  while (doff < (off_t)kafs_ino_size_get(root)) {
    size_t hdr = offsetof(kafs_sdirent_t, d_filename);
    memcpy(&d, (char *)root->i_blkreftbl + doff, hdr);
    kafs_filenamelen_t dlen = kafs_dirent_filenamelen_get(&d);
    if (dlen == name_len && memcmp((char *)root->i_blkreftbl + doff + hdr, name, name_len) == 0) { ino = kafs_dirent_ino_get(&d); break; }
    doff += hdr + dlen;
  }
  if (ino == KAFS_INO_NONE) return -1;
  *out_inotbl = inotbl; *out_fileino = &inotbl[ino];
  return 0;
}

static int inspect_pointers(const char *img, off_t mapsize, int expect12, int expect13, int expect14) {
  int ifd = open(img, O_RDONLY); if (ifd<0){ tlogf("open img failed:%s", strerror(errno)); return 2; }
  void *base = mmap(NULL, mapsize, PROT_READ, MAP_SHARED, ifd, 0);
  if (base == MAP_FAILED) { tlogf("mmap failed:%s", strerror(errno)); close(ifd); return 2; }
  kafs_sinode_t *inotbl=NULL, *fileino=NULL;
  if (lookup_file_ino(base, mapsize, "file", &inotbl, &fileino) != 0) { tlogf("dir lookup failed"); munmap(base,mapsize); close(ifd); return 2; }
  int ok = 1;
  int v12 = (kafs_blkcnt_stoh(fileino->i_blkreftbl[12]) != 0);
  int v13 = (kafs_blkcnt_stoh(fileino->i_blkreftbl[13]) != 0);
  int v14 = (kafs_blkcnt_stoh(fileino->i_blkreftbl[14]) != 0);
  if (v12 != expect12) { tlogf("i_blkreftbl[12] mismatch: got %d expect %d", v12, expect12); ok = 0; }
  if (v13 != expect13) { tlogf("i_blkreftbl[13] mismatch: got %d expect %d", v13, expect13); ok = 0; }
  if (v14 != expect14) { tlogf("i_blkreftbl[14] mismatch: got %d expect %d", v14, expect14); ok = 0; }
  munmap(base, mapsize); close(ifd);
  return ok ? 0 : 1;
}

int main(void) {
  const unsigned log_bs = 12; // 4096
  const kafs_blksize_t bs = 1u << log_bs;
  const char *img = "truncate.img";
  const char *mnt = "mnt-truncate";

  kafs_context_t ctx; off_t mapsize;
  if (kafs_test_mkimg_with_hrl(img, 64u*1024u*1024u, log_bs, 4096, &ctx, &mapsize) != 0) {
    tlogf("mkimg failed"); return 77;
  }
  munmap(ctx.c_superblock, mapsize); close(ctx.c_fd);

  // 1) 準備: 単/二/三の各範囲に1ブロックずつ配置
  pid_t srv = spawn_kafs(img, mnt, "3");
  if (srv <= 0) { tlogf("mount failed"); return 77; }
  char p[PATH_MAX]; snprintf(p, sizeof(p), "%s/file", mnt);
  int fd = open(p, O_CREAT|O_WRONLY, 0644); if (fd<0){ tlogf("create failed:%s", strerror(errno)); stop_kafs(mnt, srv); return 1; }
  off_t offS = (off_t)12 * bs;
  off_t offD = (off_t)(12 + 1024) * bs;
  off_t offT = (off_t)(12 + 1024 + 1024*1024) * bs;
  char *b = malloc(bs);
  memset(b, 0x11, bs); if (pwrite(fd, b, bs, offS) != (ssize_t)bs){ tlogf("write S failed:%s", strerror(errno)); free(b); close(fd); stop_kafs(mnt,srv); return 1; }
  memset(b, 0x22, bs); if (pwrite(fd, b, bs, offD) != (ssize_t)bs){ tlogf("write D failed:%s", strerror(errno)); free(b); close(fd); stop_kafs(mnt,srv); return 1; }
  memset(b, 0x33, bs); if (pwrite(fd, b, bs, offT) != (ssize_t)bs){ tlogf("write T failed:%s", strerror(errno)); free(b); close(fd); stop_kafs(mnt,srv); return 1; }
  free(b); fsync(fd);

  // 2) Tの直前へ truncate → TIのみ掃除され、DI/SIは残る
  if (ftruncate(fd, offT) != 0){ tlogf("ftruncate to offT failed:%s", strerror(errno)); close(fd); stop_kafs(mnt,srv); return 1; }
  fsync(fd); close(fd);
  stop_kafs(mnt, srv);
  if (inspect_pointers(img, mapsize, /*12*/1, /*13*/1, /*14*/0) != 0) return 1;

  // 3) Dの直前へ truncate → DIも掃除され、SIは残る
  srv = spawn_kafs(img, mnt, "3"); if (srv <= 0) { tlogf("remount failed"); return 77; }
  fd = open(p, O_WRONLY); if (fd<0){ tlogf("open failed:%s", strerror(errno)); stop_kafs(mnt,srv); return 1; }
  if (ftruncate(fd, offD) != 0){ tlogf("ftruncate to offD failed:%s", strerror(errno)); close(fd); stop_kafs(mnt,srv); return 1; }
  fsync(fd); close(fd); stop_kafs(mnt, srv);
  if (inspect_pointers(img, mapsize, /*12*/1, /*13*/0, /*14*/0) != 0) return 1;

  // 4) Sの直前へ truncate → SIも掃除
  srv = spawn_kafs(img, mnt, "3"); if (srv <= 0) { tlogf("remount2 failed"); return 77; }
  fd = open(p, O_WRONLY); if (fd<0){ tlogf("open2 failed:%s", strerror(errno)); stop_kafs(mnt,srv); return 1; }
  if (ftruncate(fd, offS) != 0){ tlogf("ftruncate to offS failed:%s", strerror(errno)); close(fd); stop_kafs(mnt,srv); return 1; }
  fsync(fd); close(fd); stop_kafs(mnt, srv);
  if (inspect_pointers(img, mapsize, /*12*/0, /*13*/0, /*14*/0) != 0) return 1;

  tlogf("truncate_prune OK");
  return 0;
}
