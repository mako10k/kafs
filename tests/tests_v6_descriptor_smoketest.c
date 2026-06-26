#include "test_utils.h"

#include "kafs_dirent.h"
#include "kafs_offline_summary.h"
#include "kafs_superblock.h"
#include "kafs_v6_layout.h"

#include <assert.h>
#include <errno.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
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

static int run_cmd_capture_env(char *const argv[], int expected_exit, const char *env_name,
                               const char *env_value, char *out, size_t out_sz)
{
  int pipefd[2];
  if (pipe(pipefd) != 0)
    return -errno;

  pid_t pid = fork();
  if (pid < 0)
  {
    close(pipefd[0]);
    close(pipefd[1]);
    return -errno;
  }
  if (pid == 0)
  {
    close(pipefd[0]);
    dup2(pipefd[1], STDOUT_FILENO);
    dup2(pipefd[1], STDERR_FILENO);
    close(pipefd[1]);
    if (env_name && env_value)
      setenv(env_name, env_value, 1);
    execvp(argv[0], argv);
    _exit(127);
  }

  close(pipefd[1]);
  size_t used = 0;
  for (;;)
  {
    char buf[512];
    ssize_t n = read(pipefd[0], buf, sizeof(buf));
    if (n < 0)
    {
      if (errno == EINTR)
        continue;
      break;
    }
    if (n == 0)
      break;
    if (out && out_sz > 0 && used < out_sz - 1u)
    {
      size_t copy = (size_t)n;
      if (copy > out_sz - 1u - used)
        copy = out_sz - 1u - used;
      memcpy(out + used, buf, copy);
      used += copy;
    }
  }
  close(pipefd[0]);
  if (out && out_sz > 0)
    out[used] = '\0';

  int st = 0;
  if (waitpid(pid, &st, 0) != pid)
    return -errno;
  if (!WIFEXITED(st))
    return -1;
  return (WEXITSTATUS(st) == expected_exit) ? 0 : -1;
}

static int run_cmd_capture(char *const argv[], int expected_exit, char *out, size_t out_sz)
{
  return run_cmd_capture_env(argv, expected_exit, NULL, NULL, out, out_sz);
}

static int check_v6_descriptor_direct(const char *img)
{
  int fd = open(img, O_RDONLY);
  if (fd < 0)
    return -errno;

  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  int rc = kafs_pread_all(fd, &sb, sizeof(sb), 0);
  if (rc == 0)
    rc = kafs_offline_detect_file_size(fd, &file_size);
  if (rc == 0 && kafs_sb_format_version_get(&sb) != KAFS_FORMAT_VERSION_V6)
    rc = -EINVAL;

  kafs_v6_layout_report_t report;
  if (rc == 0)
    rc = kafs_v6_discover_layout(fd, &sb, file_size, &report);
  close(fd);
  if (rc != 0)
    return rc;
  if (!report.anchor_valid || !report.selected_found || report.replica_count != 3u ||
      report.group_count != 1u || report.shard_count != 12u || report.descriptor_bytes == 0u)
    return -EINVAL;
  if (report.replicas[0].status != KAFS_V6_REPLICA_STATUS_SELECTED ||
      report.replicas[1].status != KAFS_V6_REPLICA_STATUS_VALID ||
      report.replicas[2].status != KAFS_V6_REPLICA_STATUS_VALID)
    return -EINVAL;
  return 0;
}

static int file_contains(const char *path, const char *needle)
{
  FILE *fp = fopen(path, "r");
  if (!fp)
    return 0;

  char line[512];
  int found = 0;
  while (fgets(line, sizeof(line), fp))
  {
    if (strstr(line, needle))
    {
      found = 1;
      break;
    }
  }
  fclose(fp);
  return found;
}

typedef struct v6_dir_fixture_entry
{
  kafs_inocnt_t ino;
  const char *name;
} v6_dir_fixture_entry_t;

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

static int append_dirent_record(char *buf, size_t buf_sz, size_t *off, kafs_inocnt_t ino,
                                const char *name)
{
  kafs_filenamelen_t namelen = (kafs_filenamelen_t)strlen(name);
  size_t rec_len = sizeof(kafs_sdirent_v4_t) + (size_t)namelen;
  if (namelen == 0 || namelen >= FILENAME_MAX || rec_len > UINT16_MAX || *off > buf_sz ||
      rec_len > buf_sz - *off)
    return -EINVAL;

  kafs_sdirent_v4_t *rec = (kafs_sdirent_v4_t *)(buf + *off);
  memset(rec, 0, rec_len);
  kafs_dirent_v4_rec_len_set(rec, (uint16_t)rec_len);
  kafs_dirent_v4_flags_set(rec, 0u);
  kafs_dirent_v4_ino_set(rec, ino);
  kafs_dirent_v4_filenamelen_set(rec, namelen);
  kafs_dirent_v4_name_hash_set(rec, test_dirent_name_hash(name, namelen));
  memcpy(buf + *off + sizeof(*rec), name, namelen);
  *off += rec_len;
  return 0;
}

static int build_inline_dir_inode(kafs_sinode_v5_t *disk_inode, const v6_dir_fixture_entry_t *entries,
                                  size_t entry_count, mode_t mode, kafs_linkcnt_t linkcnt)
{
  char dirbuf[KAFS_INODE_DIRECT_BYTES];
  kafs_sdir_v4_hdr_t *hdr = (kafs_sdir_v4_hdr_t *)dirbuf;
  size_t off = sizeof(*hdr);

  if (!disk_inode || !entries || entry_count == 0 || off > sizeof(dirbuf))
    return -EINVAL;

  memset(dirbuf, 0, sizeof(dirbuf));
  kafs_dir_v4_hdr_init(hdr);
  for (size_t i = 0; i < entry_count; ++i)
  {
    int rc = append_dirent_record(dirbuf, sizeof(dirbuf), &off, entries[i].ino, entries[i].name);
    if (rc != 0)
      return rc;
  }
  kafs_dir_v4_hdr_live_count_set(hdr, (uint32_t)entry_count);
  kafs_dir_v4_hdr_record_bytes_set(hdr, (uint32_t)(off - sizeof(*hdr)));

  kafs_inode_zero_for_format(disk_inode, KAFS_FORMAT_VERSION_V6);
  kafs_sinode_t *ino = (kafs_sinode_t *)disk_inode;
  kafs_ino_mode_set(ino, (kafs_mode_t)(S_IFDIR | mode));
  kafs_ino_uid_set(ino, (kafs_uid_t)getuid());
  kafs_ino_gid_set(ino, (kafs_gid_t)getgid());
  kafs_ino_size_set(ino, (kafs_off_t)off);
  kafs_ino_linkcnt_set(ino, linkcnt);
  kafs_ino_blocks_set(ino, 0);
  kafs_ino_dev_set(ino, 0);
  kafs_time_t ts = {.tv_sec = 1, .tv_nsec = 0};
  kafs_time_t null_ts = {0, 0};
  kafs_ino_atime_set(ino, ts);
  kafs_ino_ctime_set(ino, ts);
  kafs_ino_mtime_set(ino, ts);
  kafs_ino_dtime_set(ino, null_ts);
  memcpy(ino->i_blkreftbl, dirbuf, off);
  return 0;
}

static void build_inline_payload_inode(kafs_sinode_v5_t *disk_inode, mode_t mode,
                                       const char *payload)
{
  size_t payload_len = strlen(payload);
  assert(payload_len <= KAFS_INODE_DIRECT_BYTES);

  kafs_inode_zero_for_format(disk_inode, KAFS_FORMAT_VERSION_V6);
  kafs_sinode_t *ino = (kafs_sinode_t *)disk_inode;
  kafs_ino_mode_set(ino, (kafs_mode_t)mode);
  kafs_ino_uid_set(ino, (kafs_uid_t)getuid());
  kafs_ino_gid_set(ino, (kafs_gid_t)getgid());
  kafs_ino_size_set(ino, (kafs_off_t)payload_len);
  kafs_ino_linkcnt_set(ino, 1);
  kafs_ino_blocks_set(ino, 0);
  kafs_ino_dev_set(ino, 0);
  kafs_time_t ts = {.tv_sec = 1, .tv_nsec = 0};
  kafs_time_t null_ts = {0, 0};
  kafs_ino_atime_set(ino, ts);
  kafs_ino_ctime_set(ino, ts);
  kafs_ino_mtime_set(ino, ts);
  kafs_ino_dtime_set(ino, null_ts);
  memcpy(ino->i_blkreftbl, payload, payload_len);
}

static int write_v6_inode_record(int fd, const void *desc, uint32_t desc_bytes, kafs_inocnt_t ino,
                                 const kafs_sinode_v5_t *disk_inode)
{
  kafs_v6_inode_lookup_t lookup;
  int rc = kafs_v6_inode_lookup(desc, desc_bytes, ino, &lookup);
  if (rc != 0)
    return rc;
  if (lookup.record_bytes != sizeof(*disk_inode))
    return -EINVAL;
  return kafs_pwrite_all(fd, disk_inode, sizeof(*disk_inode), (off_t)lookup.inode_off);
}

static int seed_v6_readonly_traversal_fixture(const char *img)
{
  int fd = open(img, O_RDWR);
  if (fd < 0)
    return -errno;

  int rc = 0;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  kafs_v6_layout_report_t report;
  void *desc = NULL;
  uint32_t desc_bytes = 0;

  rc = kafs_pread_all(fd, &sb, sizeof(sb), 0);
  if (rc == 0)
    rc = kafs_offline_detect_file_size(fd, &file_size);
  if (rc == 0)
    rc = kafs_v6_discover_layout(fd, &sb, file_size, &report);
  if (rc == 0)
    rc = kafs_v6_read_selected_descriptor(fd, &report, &desc, &desc_bytes);
  if (rc != 0)
    goto out;

  const v6_dir_fixture_entry_t root_entries[] = {
      {.ino = 2, .name = "d"},
      {.ino = 4, .name = "l"},
  };
  const v6_dir_fixture_entry_t docs_entries[] = {
      {.ino = 3, .name = "f"},
  };

  kafs_sinode_v5_t root;
  kafs_sinode_v5_t docs;
  kafs_sinode_v5_t file;
  kafs_sinode_v5_t link;

  rc = build_inline_dir_inode(&root, root_entries, sizeof(root_entries) / sizeof(root_entries[0]),
                              0755, 3);
  if (rc == 0)
    rc = build_inline_dir_inode(&docs, docs_entries, sizeof(docs_entries) / sizeof(docs_entries[0]),
                                0555, 2);
  if (rc == 0)
  {
    build_inline_payload_inode(&file, S_IFREG | 0444, "v6 inline file\n");
    build_inline_payload_inode(&link, S_IFLNK | 0777, "d/f");
  }
  if (rc == 0)
    rc = write_v6_inode_record(fd, desc, desc_bytes, KAFS_INO_ROOTDIR, &root);
  if (rc == 0)
    rc = write_v6_inode_record(fd, desc, desc_bytes, 2, &docs);
  if (rc == 0)
    rc = write_v6_inode_record(fd, desc, desc_bytes, 3, &file);
  if (rc == 0)
    rc = write_v6_inode_record(fd, desc, desc_bytes, 4, &link);

  if (rc == 0)
  {
    kafs_inocnt_t free_inodes = kafs_sb_inocnt_free_get(&sb);
    if (free_inodes >= 3u)
      kafs_sb_inocnt_free_set(&sb, free_inodes - 3u);
    rc = kafs_pwrite_all(fd, &sb, sizeof(sb), 0);
  }

out:
  free(desc);
  close(fd);
  return rc;
}

static int dir_contains_name(const char *dir_path, const char *name)
{
  DIR *dir = opendir(dir_path);
  if (!dir)
    return 0;

  int found = 0;
  errno = 0;
  for (struct dirent *de = readdir(dir); de; de = readdir(dir))
  {
    if (strcmp(de->d_name, name) == 0)
    {
      found = 1;
      break;
    }
  }
  int saved_errno = errno;
  closedir(dir);
  return saved_errno == 0 && found;
}

static int read_file_equals(const char *path, const char *expected)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;

  char buf[128];
  ssize_t n = read(fd, buf, sizeof(buf));
  int saved_errno = errno;
  close(fd);
  if (n < 0)
    return -saved_errno;
  size_t expected_len = strlen(expected);
  if ((size_t)n != expected_len || memcmp(buf, expected, expected_len) != 0)
    return -EINVAL;
  return 0;
}

static int file_digest64(const char *path, uint64_t *digest_out, off_t *size_out)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;

  uint64_t digest = UINT64_C(1469598103934665603);
  off_t total = 0;
  for (;;)
  {
    unsigned char buf[8192];
    ssize_t n = read(fd, buf, sizeof(buf));
    if (n < 0)
    {
      int err = -errno;
      close(fd);
      return err;
    }
    if (n == 0)
      break;
    for (ssize_t i = 0; i < n; ++i)
    {
      digest ^= (uint64_t)buf[i];
      digest *= UINT64_C(1099511628211);
    }
    total += (off_t)n;
  }

  close(fd);
  if (digest_out)
    *digest_out = digest;
  if (size_out)
    *size_out = total;
  return 0;
}

static int check_v6_readonly_fixture_paths(const char *mnt)
{
  char path[PATH_MAX];
  struct stat st = {0};

  if (!dir_contains_name(mnt, "d") || !dir_contains_name(mnt, "l"))
  {
    tlogf("v6 readonly fixture root names missing");
    return 1;
  }

  snprintf(path, sizeof(path), "%s/d", mnt);
  if (stat(path, &st) != 0 || !S_ISDIR(st.st_mode))
  {
    tlogf("v6 readonly fixture directory stat failed: %s", strerror(errno));
    return 1;
  }
  if (!dir_contains_name(path, "f"))
  {
    tlogf("v6 readonly fixture nested name missing");
    return 1;
  }

  snprintf(path, sizeof(path), "%s/d/f", mnt);
  memset(&st, 0, sizeof(st));
  if (stat(path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size != 15)
  {
    tlogf("v6 readonly fixture file stat failed: %s", strerror(errno));
    return 1;
  }
  if (read_file_equals(path, "v6 inline file\n") != 0)
  {
    tlogf("v6 readonly fixture file read mismatch");
    return 1;
  }

  snprintf(path, sizeof(path), "%s/l", mnt);
  memset(&st, 0, sizeof(st));
  if (lstat(path, &st) != 0 || !S_ISLNK(st.st_mode))
  {
    tlogf("v6 readonly fixture symlink lstat failed: %s", strerror(errno));
    return 1;
  }
  char target[64];
  ssize_t n = readlink(path, target, sizeof(target) - 1u);
  if (n < 0)
  {
    tlogf("v6 readonly fixture readlink failed: %s", strerror(errno));
    return 1;
  }
  target[n] = '\0';
  if (strcmp(target, "d/f") != 0)
  {
    tlogf("v6 readonly fixture readlink target=%s", target);
    return 1;
  }

  return 0;
}

static int check_v6_inspection_mount_smoke(const char *img)
{
  if (access("/dev/fuse", R_OK | W_OK) != 0)
  {
    tlogf("skip v6 inspection mount smoke: /dev/fuse unavailable");
    return 0;
  }

  uint64_t digest_before = 0;
  off_t size_before = 0;
  if (file_digest64(img, &digest_before, &size_before) != 0)
  {
    tlogf("v6 inspection mount pre-digest failed");
    return 1;
  }

  const char *mnt = "mnt-inspection";
  const char *log_path = "v6-inspection-mount.log";
  kafs_test_mount_options_t options = {
      .debug = "1",
      .log_path = log_path,
      .extra_options = "ro,v6_inspection_mount,no_writeback_cache",
      .timeout_ms = 10000,
  };

  pid_t srv = kafs_test_start_kafs(img, mnt, &options);
  if (srv <= 0)
  {
    kafs_test_dump_log(log_path, "v6 inspection mount failed");
    return 77;
  }

  int rc = 0;
  struct stat st = {0};
  if (stat(mnt, &st) != 0 || !S_ISDIR(st.st_mode))
  {
    tlogf("v6 readonly smoke root stat failed: %s", strerror(errno));
    rc = 1;
    goto out_stop;
  }

  struct statvfs sv = {0};
  if (statvfs(mnt, &sv) != 0 || sv.f_blocks == 0)
  {
    tlogf("v6 readonly smoke statvfs failed: %s", strerror(errno));
    rc = 1;
    goto out_stop;
  }

  DIR *dir = opendir(mnt);
  if (!dir)
  {
    tlogf("v6 readonly smoke opendir failed: %s", strerror(errno));
    rc = 1;
    goto out_stop;
  }
  int saw_dot = 0;
  errno = 0;
  for (struct dirent *de = readdir(dir); de; de = readdir(dir))
  {
    if (strcmp(de->d_name, ".") == 0)
      saw_dot = 1;
  }
  int saved_errno = errno;
  closedir(dir);
  if (saved_errno != 0 || !saw_dot)
  {
    tlogf("v6 readonly smoke readdir failed: errno=%s saw_dot=%d", strerror(saved_errno),
          saw_dot);
    rc = 1;
    goto out_stop;
  }

  rc = check_v6_readonly_fixture_paths(mnt);
  if (rc != 0)
    goto out_stop;

  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/new.txt", mnt);
  int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd >= 0)
  {
    close(fd);
    tlogf("v6 readonly smoke create unexpectedly succeeded");
    rc = 1;
    goto out_stop;
  }
  if (errno != EROFS)
  {
    tlogf("v6 readonly smoke create errno=%s (expected EROFS)", strerror(errno));
    rc = 1;
    goto out_stop;
  }

  snprintf(path, sizeof(path), "%s/subdir", mnt);
  if (mkdir(path, 0755) == 0)
  {
    tlogf("v6 readonly smoke mkdir unexpectedly succeeded");
    rc = 1;
    goto out_stop;
  }
  if (errno != EROFS)
  {
    tlogf("v6 readonly smoke mkdir errno=%s (expected EROFS)", strerror(errno));
    rc = 1;
    goto out_stop;
  }

out_stop:
  kafs_test_stop_kafs(mnt, srv);
  if (rc == 0)
  {
    uint64_t digest_after = 0;
    off_t size_after = 0;
    if (file_digest64(img, &digest_after, &size_after) != 0 || digest_after != digest_before ||
        size_after != size_before)
    {
      tlogf("v6 inspection mount changed backing image");
      rc = 1;
    }
  }
  if (rc == 0 && !file_contains(log_path, "format v6 inspection mount"))
  {
    tlogf("v6 inspection mount log missing admission message");
    rc = 1;
  }
  return rc;
}

int main(void)
{
  if (kafs_test_enter_tmpdir("v6_descriptor") != 0)
    return 77;

  const char *img = "v6-desc.img";
  if (mkdir("mnt", 0755) != 0)
  {
    tlogf("mkdir mnt failed");
    return 1;
  }
  if (mkdir("mnt-handoff", 0755) != 0)
  {
    tlogf("mkdir mnt-handoff failed");
    return 1;
  }

  char *mkfs_argv[] = {(char *)kafs_test_mkfs_bin(), (char *)img, (char *)"--format-version",
                       (char *)"6", (char *)"--size-bytes", (char *)"64M", (char *)"--yes",
                       NULL};
  char out[8192];
  if (run_cmd_capture(mkfs_argv, 0, out, sizeof(out)) != 0)
  {
    tlogf("mkfs v6 failed: %s", out);
    return 1;
  }

  if (check_v6_descriptor_direct(img) != 0)
  {
    tlogf("direct v6 descriptor discovery failed");
    return 1;
  }

  char *dump_argv[] = {(char *)kafs_test_kafsdump_bin(), (char *)"--json", (char *)img, NULL};
  if (run_cmd_capture(dump_argv, 0, out, sizeof(out)) != 0)
  {
    tlogf("kafsdump --json v6 failed: %s", out);
    return 1;
  }
  if (!strstr(out, "\"v6_layout_descriptor\"") || !strstr(out, "\"status\": \"ok\"") ||
      !strstr(out, "\"replica_count\": 3") || !strstr(out, "\"selected\": true"))
  {
    tlogf("kafsdump JSON missing v6 descriptor fields: %s", out);
    return 1;
  }

  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)img, NULL};
  if (run_cmd_capture(fsck_argv, 0, out, sizeof(out)) != 0)
  {
    tlogf("fsck v6 descriptor failed: %s", out);
    return 1;
  }
  if (!strstr(out, "v6 descriptor:") || !strstr(out, "status=selected"))
  {
    tlogf("fsck output missing v6 descriptor status: %s", out);
    return 1;
  }

  char *mount_argv[] = {(char *)kafs_test_kafs_bin(), (char *)img, (char *)"mnt", NULL};
  if (run_cmd_capture(mount_argv, 2, out, sizeof(out)) != 0)
  {
    tlogf("v6 runtime mount did not fail as expected: %s", out);
    return 1;
  }
  if (!strstr(out, "admission preflight") || !strstr(out, "metadata checks OK") ||
      !strstr(out, "offline-only"))
  {
    tlogf("v6 runtime mount error missing preflight/offline-only guidance: %s", out);
    return 1;
  }

  char *ro_only_argv[] = {(char *)kafs_test_kafs_bin(), (char *)img, (char *)"mnt", (char *)"-o",
                          (char *)"ro", NULL};
  if (run_cmd_capture(ro_only_argv, 2, out, sizeof(out)) != 0)
  {
    tlogf("v6 ro-only runtime mount did not fail as expected: %s", out);
    return 1;
  }
  if (!strstr(out, "v6 inspection mount requires") || !strstr(out, "offline-only"))
  {
    tlogf("v6 ro-only runtime mount missing inspection guidance: %s", out);
    return 1;
  }

  char *inspection_without_ro_argv[] = {
      (char *)kafs_test_kafs_bin(), (char *)img, (char *)"mnt", (char *)"-o",
      (char *)"v6_inspection_mount", NULL};
  if (run_cmd_capture(inspection_without_ro_argv, 2, out, sizeof(out)) != 0)
  {
    tlogf("v6 inspection mount without ro did not fail as expected: %s", out);
    return 1;
  }
  if (!strstr(out, "requires -o ro"))
  {
    tlogf("v6 inspection mount without ro missing guidance: %s", out);
    return 1;
  }

  char *inspection_writeback_argv[] = {
      (char *)kafs_test_kafs_bin(), (char *)img, (char *)"mnt", (char *)"-o",
      (char *)"ro,v6_inspection_mount,writeback_cache", NULL};
  if (run_cmd_capture(inspection_writeback_argv, 2, out, sizeof(out)) != 0)
  {
    tlogf("v6 inspection mount with writeback_cache did not fail as expected: %s", out);
    return 1;
  }
  if (!strstr(out, "does not allow writeback_cache"))
  {
    tlogf("v6 inspection mount with writeback_cache missing guidance: %s", out);
    return 1;
  }

  char *handoff_argv[] = {(char *)kafs_test_kafs_bin(), (char *)img, (char *)"mnt-handoff",
                          NULL};
  if (run_cmd_capture_env(handoff_argv, 2, "KAFS_V6_ADMISSION_HANDOFF", "1", out, sizeof(out)) !=
      0)
  {
    tlogf("v6 runtime handoff did not fail as expected: %s", out);
    return 1;
  }
  if (!strstr(out, "admission handoff") || !strstr(out, "selected descriptor retained") ||
      !strstr(out, "offline-only"))
  {
    tlogf("v6 runtime handoff error missing descriptor/offline-only guidance: %s", out);
    return 1;
  }

  if (seed_v6_readonly_traversal_fixture(img) != 0)
  {
    tlogf("v6 readonly traversal fixture seed failed");
    return 1;
  }

  int readonly_rc = check_v6_inspection_mount_smoke(img);
  if (readonly_rc == 77)
    return 77;
  if (readonly_rc != 0)
    return 1;

  return 0;
}
