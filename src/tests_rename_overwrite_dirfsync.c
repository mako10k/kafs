#include "kafs_context.h"
#include "test_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static void tlogf(const char *fmt, ...) { va_list ap; va_start(ap, fmt); vfprintf(stderr, fmt, ap); fputc('\n', stderr); va_end(ap);} 

static int is_mounted_fuse(const char *mnt) {
  char absmnt[PATH_MAX]; const char *want = mnt; if (realpath(mnt, absmnt) != NULL) want = absmnt;
  FILE *fp = fopen("/proc/mounts", "r"); if (!fp) return 0; char dev[256], dir[256], type[64]; int mounted = 0;
  while (fscanf(fp, "%255s %255s %63s %*[^\n]\n", dev, dir, type) == 3) {
    if (strcmp(dir, want) == 0 && strncmp(type, "fuse", 4) == 0) { mounted = 1; break; }
  }
  fclose(fp); return mounted;
}

static pid_t spawn_kafs(const char *img, const char *mnt) {
  mkdir(mnt, 0700);
  pid_t pid = fork(); if (pid < 0) return -errno;
  if (pid == 0) {
    setenv("KAFS_IMAGE", img, 1);
    int lfd = open("minisrv.log", O_CREAT|O_TRUNC|O_WRONLY, 0644);
    if (lfd >= 0) { dup2(lfd, STDERR_FILENO); dup2(lfd, STDOUT_FILENO); close(lfd); }
    char *args[] = {"./kafs", (char *)mnt, "-f", NULL};
    execvp(args[0], args); _exit(127);
  }
  for (int i = 0; i < 50; ++i) { if (is_mounted_fuse(mnt)) return pid; struct timespec ts = {0, 100*1000*1000}; nanosleep(&ts, NULL); }
  kill(pid, SIGTERM); waitpid(pid, NULL, 0); return -1;
}

static int run_cmd(char *const argv[]) { pid_t p=fork(); if(p<0) return -errno; if(p==0){ execvp(argv[0], argv); _exit(127);} int st=0; if(waitpid(p,&st,0)<0) return -errno; return (WIFEXITED(st)&&WEXITSTATUS(st)==0)?0:-1; }
static void stop_kafs(const char *mnt, pid_t pid) {
  struct timespec ts={0,50*1000*1000}; nanosleep(&ts,NULL);
  char *um1[] = {"fusermount3", "-u", (char *)mnt, NULL};
  if (run_cmd(um1) != 0) { char *um2[] = {"fusermount", "-u", (char *)mnt, NULL}; (void)run_cmd(um2); }
  kill(pid, SIGTERM); waitpid(pid, NULL, 0);
}

static int write_file(const char *path, const char *data, size_t len) {
  int fd = open(path, O_CREAT|O_TRUNC|O_WRONLY, 0644); if (fd < 0) return -errno;
  ssize_t w = write(fd, data, len); int e = (w == (ssize_t)len) ? 0 : -errno; close(fd); return e;
}

static int read_file(const char *path, char *buf, size_t cap) {
  int fd = open(path, O_RDONLY); if (fd < 0) return -errno;
  ssize_t r = read(fd, buf, cap); int e = (r >= 0) ? (int)r : -errno; close(fd); return e;
}

int main(void) {
  const char *img = "rename.img";
  const char *mnt = "mnt-rename";
  kafs_context_t ctx; off_t mapsize;
  if (kafs_test_mkimg_with_hrl(img, 32u*1024u*1024u, 12, 2048, &ctx, &mapsize) != 0) {
    tlogf("mkimg failed"); return 77;
  }
  munmap(ctx.c_superblock, mapsize); close(ctx.c_fd);

  pid_t srv = spawn_kafs(img, mnt);
  if (srv <= 0) { tlogf("mount failed"); return 77; }

  // 作業ディレクトリ
  char d[PATH_MAX]; int n = snprintf(d, sizeof(d), "%s/dir", mnt); if (n<0||n>=(int)sizeof(d)){ stop_kafs(mnt, srv); return 77; }
  if (mkdir(d, 0777)!=0 && errno!=EEXIST){ tlogf("mkdir dir failed:%s", strerror(errno)); stop_kafs(mnt, srv); return 1; }

  char a[PATH_MAX], b[PATH_MAX];
  snprintf(a, sizeof(a), "%s/dir/a.txt", mnt);
  snprintf(b, sizeof(b), "%s/dir/b.txt", mnt);
  if (write_file(a, "AAAA", 4)!=0){ tlogf("write a failed"); stop_kafs(mnt, srv); return 1; }
  if (write_file(b, "BBBB", 4)!=0){ tlogf("write b failed"); stop_kafs(mnt, srv); return 1; }

  // rename で上書き（atomic replace）
  if (rename(a, b) != 0) { tlogf("rename failed:%s", strerror(errno)); stop_kafs(mnt, srv); return 1; }

  // 直後にbの内容がAAAAか確認
  char buf[8] = {0}; int r = read_file(b, buf, sizeof(buf)); if (r < 4 || strncmp(buf, "AAAA", 4)!=0){ tlogf("post-rename content:%.*s", r>0?r:0, buf); stop_kafs(mnt, srv); return 1; }

  // 親ディレクトリをfsyncして永続性確認
  int dfd = open(d, O_RDONLY|O_DIRECTORY);
  if (dfd>=0) { fsync(dfd); close(dfd); }

  // 再マウントしてもAAAAであること
  stop_kafs(mnt, srv);
  srv = spawn_kafs(img, mnt); if (srv <= 0) { tlogf("remount failed"); return 77; }
  memset(buf, 0, sizeof(buf)); r = read_file(b, buf, sizeof(buf)); if (r < 4 || strncmp(buf, "AAAA", 4)!=0){ tlogf("after remount content:%.*s", r>0?r:0, buf); stop_kafs(mnt, srv); return 1; }

  stop_kafs(mnt, srv);
  tlogf("rename_overwrite_dirfsync OK");
  return 0;
}
