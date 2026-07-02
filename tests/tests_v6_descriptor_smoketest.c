#include "test_utils.h"

#include "kafs_dirent.h"
#include "kafs_ioctl.h"
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
#include <sys/ioctl.h>
#include <time.h>
#include <unistd.h>

#ifdef __linux__
#include <linux/fs.h>
#include <sys/syscall.h>
#endif

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

static int corrupt_all_v6_descriptors(const char *img)
{
  int fd = open(img, O_RDWR);
  if (fd < 0)
    return -errno;

  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  kafs_v6_layout_report_t report;
  int rc = kafs_pread_all(fd, &sb, sizeof(sb), 0);
  if (rc == 0)
    rc = kafs_offline_detect_file_size(fd, &file_size);
  if (rc == 0)
    rc = kafs_v6_discover_layout(fd, &sb, file_size, &report);
  if (rc == 0 && (report.replica_count == 0u || report.descriptor_bytes == 0u))
    rc = -EINVAL;

  for (uint32_t i = 0; rc == 0 && i < report.replica_count; ++i)
  {
    uint8_t byte = 0;
    off_t off = (off_t)(report.replicas[i].offset + report.descriptor_bytes - 1u);
    rc = kafs_pread_all(fd, &byte, sizeof(byte), off);
    if (rc == 0)
    {
      byte ^= 0x5au;
      rc = kafs_pwrite_all(fd, &byte, sizeof(byte), off);
    }
  }

  close(fd);
  return rc;
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

static int errno_is_one_of(int err, int expected_a, int expected_b)
{
  return err == expected_a || (expected_b != 0 && err == expected_b);
}

static int expect_failed_errno(const char *label, int rc, int expected_a, int expected_b)
{
  int saved_errno = errno;
  if (rc == 0)
  {
    tlogf("%s unexpectedly succeeded", label);
    return 1;
  }
  if (!errno_is_one_of(saved_errno, expected_a, expected_b))
  {
    tlogf("%s errno=%s (expected %s%s%s)", label, strerror(saved_errno), strerror(expected_a),
          expected_b ? " or " : "", expected_b ? strerror(expected_b) : "");
    return 1;
  }
  return 0;
}

static int expect_repeated_byte(const char *label, const unsigned char *buf, size_t start,
                                size_t end, unsigned char expected)
{
  if (!buf || start > end)
    return 1;

  for (size_t i = start; i < end; ++i)
  {
    if (buf[i] != expected)
    {
      tlogf("%s byte[%zu]=0x%02x expected 0x%02x", label, i, (unsigned)buf[i],
            (unsigned)expected);
      return 1;
    }
  }
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
    tlogf("skip kafs-v6 inspection mount smoke: /dev/fuse unavailable");
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
      .extra_options = "ro,no_writeback_cache",
      .timeout_ms = 10000,
  };

  pid_t srv = kafs_test_start_kafs_v6(img, mnt, &options);
  if (srv <= 0)
  {
    kafs_test_dump_log(log_path, "kafs-v6 inspection mount failed");
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
    tlogf("kafs-v6 inspection mount log missing admission message");
    rc = 1;
  }
  return rc;
}

static int check_v6_controlled_write_copy_guards(int fd, const char *mnt)
{
  kafs_ioctl_copy_t req;
  memset(&req, 0, sizeof(req));
  req.struct_size = sizeof(req);
  snprintf(req.src, sizeof(req.src), "/w.bin");
  snprintf(req.dst, sizeof(req.dst), "/ioctl-copy.bin");
  errno = 0;
  if (expect_failed_errno("v6 controlled ioctl copy", ioctl(fd, KAFS_IOCTL_COPY, &req),
                          EOPNOTSUPP, 0) != 0)
    return 1;

  memset(&req, 0, sizeof(req));
  req.struct_size = sizeof(req);
  req.flags = KAFS_IOCTL_COPY_F_REFLINK;
  snprintf(req.src, sizeof(req.src), "/w.bin");
  snprintf(req.dst, sizeof(req.dst), "/ioctl-reflink.bin");
  errno = 0;
  if (expect_failed_errno("v6 controlled ioctl reflink", ioctl(fd, KAFS_IOCTL_COPY, &req),
                          EOPNOTSUPP, 0) != 0)
    return 1;

#ifdef __linux__
#ifdef FICLONE
  char clone_path[PATH_MAX];
  snprintf(clone_path, sizeof(clone_path), "%s/clone.bin", mnt);
  int cfd = open(clone_path, O_CREAT | O_EXCL | O_RDWR, 0644);
  if (cfd < 0)
  {
    tlogf("v6 controlled clone target open failed: %s", strerror(errno));
    return 1;
  }
  errno = 0;
  int frc = ioctl(cfd, FICLONE, fd);
  int frc_errno = errno;
  close(cfd);
  errno = frc_errno;
  if (expect_failed_errno("v6 controlled FICLONE", frc, EOPNOTSUPP, 0) != 0)
    return 1;
#endif

#ifdef SYS_copy_file_range
  char cfr_path[PATH_MAX];
  snprintf(cfr_path, sizeof(cfr_path), "%s/copy-range.bin", mnt);
  int cfrfd = open(cfr_path, O_CREAT | O_EXCL | O_WRONLY, 0644);
  if (cfrfd < 0)
  {
    tlogf("v6 controlled copy_file_range target open failed: %s", strerror(errno));
    return 1;
  }
  off_t in_off = 0;
  off_t out_off = 0;
  errno = 0;
  ssize_t cr = syscall(SYS_copy_file_range, fd, &in_off, cfrfd, &out_off, (size_t)4096, 0);
  int cr_errno = errno;
  close(cfrfd);
  // Some kernels satisfy FUSE copy_file_range through generic read/write fallback before the
  // high-level copy_file_range hook is reached; that path is indistinguishable from allowed write.
  if (cr >= 0)
    return 0;
  errno = cr_errno;
  if (expect_failed_errno("v6 controlled copy_file_range", -1, EOPNOTSUPP, ENOSYS) != 0)
    return 1;
#endif
#endif

  return 0;
}

static int expect_path_absent(const char *label, const char *path)
{
  struct stat st;
  errno = 0;
  if (lstat(path, &st) == 0)
  {
    tlogf("%s unexpectedly created path=%s", label, path);
    return 1;
  }
  if (errno != ENOENT)
  {
    tlogf("%s lstat errno=%s (expected ENOENT)", label, strerror(errno));
    return 1;
  }
  return 0;
}

static int check_v6_controlled_write_rejection_matrix(const char *mnt, const char *regular_path)
{
  char path[PATH_MAX];

  snprintf(path, sizeof(path), "%s/reject-dir", mnt);
  errno = 0;
  if (expect_failed_errno("v6 controlled mkdir", mkdir(path, 0755), EOPNOTSUPP, 0) != 0 ||
      expect_path_absent("v6 controlled mkdir", path) != 0)
    return 1;

  snprintf(path, sizeof(path), "%s/d", mnt);
  errno = 0;
  if (expect_failed_errno("v6 controlled rmdir", rmdir(path), EOPNOTSUPP, 0) != 0)
    return 1;

  snprintf(path, sizeof(path), "%s/reject-fifo", mnt);
  errno = 0;
  if (expect_failed_errno("v6 controlled mkfifo", mkfifo(path, 0644), EOPNOTSUPP, 0) != 0 ||
      expect_path_absent("v6 controlled mkfifo", path) != 0)
    return 1;

  errno = 0;
  if (expect_failed_errno("v6 controlled chmod", chmod(regular_path, 0600), EOPNOTSUPP, 0) !=
      0)
    return 1;

  errno = 0;
  if (expect_failed_errno("v6 controlled chown", chown(regular_path, getuid(), getgid()),
                          EOPNOTSUPP, 0) != 0)
    return 1;

  const struct timespec ts[2] = {
      {.tv_sec = 0, .tv_nsec = 0},
      {.tv_sec = 0, .tv_nsec = 0},
  };
  errno = 0;
  if (expect_failed_errno("v6 controlled utimens",
                          utimensat(AT_FDCWD, regular_path, ts, 0), EOPNOTSUPP, 0) != 0)
    return 1;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    tlogf("v6 controlled fsyncdir open failed: %s", strerror(errno));
    return 1;
  }
  errno = 0;
  int frc = fsync(dfd);
  int frc_errno = errno;
  close(dfd);
  errno = frc_errno;
  if (expect_failed_errno("v6 controlled fsyncdir", frc, EOPNOTSUPP, 0) != 0)
    return 1;

  return 0;
}

static int check_v6_controlled_write_mount_smoke(const char *img)
{
  if (access("/dev/fuse", R_OK | W_OK) != 0)
  {
    tlogf("skip v6 controlled write mount smoke: /dev/fuse unavailable");
    return 0;
  }

  const char *mnt = "mnt-v6-write";
  const char *log_path = "v6-controlled-write-mount.log";
  kafs_test_mount_options_t options = {
      .debug = "1",
      .log_path = log_path,
      .extra_options =
          "rw,v6_write_mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,"
          "fsync_policy=full",
      .timeout_ms = 15000,
  };

  pid_t srv = kafs_test_start_kafs(img, mnt, &options);
  if (srv <= 0)
  {
    kafs_test_dump_log(log_path, "v6 controlled write mount failed");
    return 77;
  }

  int rc = 0;
  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/w.bin", mnt);
  int fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0644);
  if (fd < 0)
  {
    tlogf("v6 controlled create failed: %s", strerror(errno));
    rc = 1;
    goto out_stop;
  }

  unsigned char wbuf[4096];
  for (size_t i = 0; i < sizeof(wbuf); ++i)
    wbuf[i] = (unsigned char)(0x41u + (i % 23u));
  if (write(fd, wbuf, sizeof(wbuf)) != (ssize_t)sizeof(wbuf))
  {
    tlogf("v6 controlled write failed: %s", strerror(errno));
    rc = 1;
    close(fd);
    goto out_stop;
  }
  if (fsync(fd) != 0)
  {
    tlogf("v6 controlled fsync failed: %s", strerror(errno));
    rc = 1;
    close(fd);
    goto out_stop;
  }
  if (close(fd) != 0)
  {
    tlogf("v6 controlled release failed: %s", strerror(errno));
    rc = 1;
    goto out_stop;
  }

  fd = open(path, O_RDWR);
  if (fd < 0)
  {
    tlogf("v6 controlled reopen failed: %s", strerror(errno));
    rc = 1;
    goto out_stop;
  }

  unsigned char rbuf[sizeof(wbuf)];
  if (pread(fd, rbuf, sizeof(rbuf), 0) != (ssize_t)sizeof(rbuf) ||
      memcmp(rbuf, wbuf, sizeof(wbuf)) != 0)
  {
    tlogf("v6 controlled readback mismatch");
    rc = 1;
    close(fd);
    goto out_stop;
  }

  const off_t second_block_off = (off_t)sizeof(wbuf);
  unsigned char zbuf[sizeof(wbuf)] = {0};
  if (pwrite(fd, zbuf, sizeof(zbuf), second_block_off) != (ssize_t)sizeof(zbuf))
  {
    tlogf("v6 controlled zero-block write failed: %s", strerror(errno));
    rc = 1;
    close(fd);
    goto out_stop;
  }
  if (fdatasync(fd) != 0)
  {
    tlogf("v6 controlled fdatasync failed: %s", strerror(errno));
    rc = 1;
    close(fd);
    goto out_stop;
  }

  unsigned char second[sizeof(wbuf)];
  if (pread(fd, second, sizeof(second), second_block_off) != (ssize_t)sizeof(second) ||
      expect_repeated_byte("v6 controlled zero block", second, 0, sizeof(second), 0) != 0)
  {
    tlogf("v6 controlled zero-block readback mismatch");
    rc = 1;
    close(fd);
    goto out_stop;
  }

  const char patch[] = "v6-partial-write";
  const size_t patch_off = 123u;
  const size_t patch_len = sizeof(patch) - 1u;
  if (pwrite(fd, patch, patch_len, second_block_off + (off_t)patch_off) !=
      (ssize_t)patch_len)
  {
    tlogf("v6 controlled partial write failed: %s", strerror(errno));
    rc = 1;
    close(fd);
    goto out_stop;
  }
  if (fsync(fd) != 0)
  {
    tlogf("v6 controlled post-partial fsync failed: %s", strerror(errno));
    rc = 1;
    close(fd);
    goto out_stop;
  }
  if (pread(fd, second, sizeof(second), second_block_off) != (ssize_t)sizeof(second) ||
      expect_repeated_byte("v6 controlled partial prefix", second, 0, patch_off, 0) != 0 ||
      memcmp(second + patch_off, patch, patch_len) != 0 ||
      expect_repeated_byte("v6 controlled partial suffix", second, patch_off + patch_len,
                           sizeof(second), 0) != 0)
  {
    tlogf("v6 controlled partial-block readback mismatch");
    rc = 1;
    close(fd);
    goto out_stop;
  }
  if (pread(fd, rbuf, sizeof(rbuf), 0) != (ssize_t)sizeof(rbuf) ||
      memcmp(rbuf, wbuf, sizeof(wbuf)) != 0)
  {
    tlogf("v6 controlled first block changed after zero/partial writes");
    rc = 1;
    close(fd);
    goto out_stop;
  }

#ifdef __linux__
#ifdef SYS_fallocate
  errno = 0;
  if (expect_failed_errno("v6 controlled fallocate",
                          (int)syscall(SYS_fallocate, fd, 0, 0, (off_t)4096), EOPNOTSUPP,
                          ENOSYS) != 0)
  {
    rc = 1;
    close(fd);
    goto out_stop;
  }
#endif
#endif

  if (check_v6_controlled_write_copy_guards(fd, mnt) != 0)
  {
    rc = 1;
    close(fd);
    goto out_stop;
  }
  close(fd);
  fd = -1;

  errno = 0;
  fd = open(path, O_WRONLY | O_TRUNC);
  int open_trunc_errno = errno;
  int open_trunc_rc = (fd < 0) ? -1 : 0;
  if (fd >= 0)
  {
    close(fd);
    fd = -1;
  }
  errno = open_trunc_errno;
  if (expect_failed_errno("v6 controlled open(O_TRUNC)", open_trunc_rc, EOPNOTSUPP, 0) != 0)
  {
    rc = 1;
    goto out_stop;
  }

  errno = 0;
  if (expect_failed_errno("v6 controlled truncate", truncate(path, 0), EOPNOTSUPP, 0) != 0)
  {
    rc = 1;
    goto out_stop;
  }

  char renamed[PATH_MAX];
  snprintf(renamed, sizeof(renamed), "%s/w-renamed.bin", mnt);
  errno = 0;
  if (expect_failed_errno("v6 controlled rename", rename(path, renamed), EOPNOTSUPP, 0) != 0)
  {
    rc = 1;
    goto out_stop;
  }

  char hardlink[PATH_MAX];
  snprintf(hardlink, sizeof(hardlink), "%s/w-hard.bin", mnt);
  errno = 0;
  if (expect_failed_errno("v6 controlled link", link(path, hardlink), EOPNOTSUPP, 0) != 0)
  {
    rc = 1;
    goto out_stop;
  }

  char symlink_path[PATH_MAX];
  snprintf(symlink_path, sizeof(symlink_path), "%s/w-sym.bin", mnt);
  errno = 0;
  if (expect_failed_errno("v6 controlled symlink", symlink("w.bin", symlink_path), EOPNOTSUPP,
                          0) != 0)
  {
    rc = 1;
    goto out_stop;
  }

  errno = 0;
  if (expect_failed_errno("v6 controlled unlink", unlink(path), EOPNOTSUPP, 0) != 0)
  {
    rc = 1;
    goto out_stop;
  }

  if (check_v6_controlled_write_rejection_matrix(mnt, path) != 0)
  {
    rc = 1;
    goto out_stop;
  }

  struct stat st = {0};
  const off_t expected_size = (off_t)(sizeof(wbuf) * 2u);
  if (stat(path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size != expected_size)
  {
    tlogf("v6 controlled file missing after rejected mutations: %s", strerror(errno));
    rc = 1;
    goto out_stop;
  }

out_stop:
  kafs_test_stop_kafs(mnt, srv);
  if (rc == 0 && !file_contains(log_path, "format v6 controlled write mount"))
  {
    tlogf("v6 controlled write mount log missing admission message");
    rc = 1;
  }
  if (rc == 0)
  {
    char out[8192];
    char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)"--balanced-check", (char *)img,
                         NULL};
    if (run_cmd_capture(fsck_argv, 0, out, sizeof(out)) != 0)
    {
      tlogf("fsck --balanced-check after v6 controlled write failed: %s", out);
      rc = 1;
    }
  }
  return rc;
}

static int check_v6_controlled_write_enospc_smoke(void)
{
  if (access("/dev/fuse", R_OK | W_OK) != 0)
  {
    tlogf("skip v6 controlled ENOSPC smoke: /dev/fuse unavailable");
    return 0;
  }

  const char *img = "v6-enospc.img";
  char out[8192];
  char *mkfs_argv[] = {(char *)kafs_test_mkfs_bin(), (char *)img, (char *)"--format-version",
                       (char *)"6", (char *)"--size-bytes", (char *)"8M", (char *)"--yes",
                       NULL};
  if (run_cmd_capture(mkfs_argv, 0, out, sizeof(out)) != 0)
  {
    tlogf("mkfs v6 ENOSPC fixture failed: %s", out);
    return 1;
  }
  if (seed_v6_readonly_traversal_fixture(img) != 0)
  {
    tlogf("v6 ENOSPC fixture seed failed");
    return 1;
  }

  const char *mnt = "mnt-v6-enospc";
  const char *log_path = "v6-controlled-enospc-mount.log";
  kafs_test_mount_options_t options = {
      .debug = "1",
      .log_path = log_path,
      .extra_options =
          "rw,v6_write_mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,"
          "fsync_policy=full",
      .timeout_ms = 15000,
  };

  pid_t srv = kafs_test_start_kafs(img, mnt, &options);
  if (srv <= 0)
  {
    kafs_test_dump_log(log_path, "v6 controlled ENOSPC mount failed");
    return 77;
  }

  int rc = 0;
  int fd = -1;
  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/fill.bin", mnt);
  fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0644);
  if (fd < 0)
  {
    tlogf("v6 ENOSPC fill create failed: %s", strerror(errno));
    rc = 1;
    goto out_stop;
  }

  unsigned char block[4096];
  for (size_t i = 0; i < sizeof(block); ++i)
    block[i] = (unsigned char)(0x31u + (i % 47u));

  int saw_enospc = 0;
  size_t blocks_written = 0;
  for (; blocks_written < 4096u; ++blocks_written)
  {
    block[0] = (unsigned char)(blocks_written & 0xFFu);
    block[1] = (unsigned char)((blocks_written >> 8) & 0xFFu);
    block[2] = (unsigned char)((blocks_written >> 16) & 0xFFu);
    block[3] = (unsigned char)(0x80u | ((blocks_written >> 24) & 0x7Fu));
    errno = 0;
    ssize_t n = write(fd, block, sizeof(block));
    if (n == (ssize_t)sizeof(block))
      continue;
    if (n > 0 || (n < 0 && errno == ENOSPC))
    {
      saw_enospc = 1;
      break;
    }
    if (n == 0)
      tlogf("v6 ENOSPC fill write returned zero after %zu blocks", blocks_written);
    else
      tlogf("v6 ENOSPC fill write failed after %zu blocks: %s", blocks_written,
            strerror(errno));
    rc = 1;
    break;
  }

  if (rc == 0 && !saw_enospc)
  {
    tlogf("v6 ENOSPC fill did not exhaust space after %zu blocks", blocks_written);
    rc = 1;
  }
  if (rc == 0)
  {
    errno = 0;
    if (fdatasync(fd) != 0 && errno != ENOSPC)
    {
      tlogf("v6 ENOSPC fdatasync failed with unexpected errno: %s", strerror(errno));
      rc = 1;
    }
  }
  if (fd >= 0)
  {
    errno = 0;
    if (close(fd) != 0 && errno != ENOSPC && rc == 0)
    {
      tlogf("v6 ENOSPC close failed with unexpected errno: %s", strerror(errno));
      rc = 1;
    }
    fd = -1;
  }

out_stop:
  if (fd >= 0)
    close(fd);
  kafs_test_stop_kafs(mnt, srv);
  if (rc == 0 && !file_contains(log_path, "format v6 controlled write mount"))
  {
    tlogf("v6 controlled ENOSPC mount log missing admission message");
    rc = 1;
  }
  if (rc == 0)
  {
    char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)"--balanced-check", (char *)img,
                         NULL};
    if (run_cmd_capture(fsck_argv, 0, out, sizeof(out)) != 0)
    {
      tlogf("fsck --balanced-check after v6 ENOSPC failed: %s", out);
      rc = 1;
    }
  }
  return rc;
}

static int check_v6_controlled_write_fsync_failure_smoke(void)
{
  if (access("/dev/fuse", R_OK | W_OK) != 0)
  {
    tlogf("skip v6 controlled fsync failure smoke: /dev/fuse unavailable");
    return 0;
  }

  const char *img = "v6-fsync-fail.img";
  char out[8192];
  char *mkfs_argv[] = {(char *)kafs_test_mkfs_bin(), (char *)img, (char *)"--format-version",
                       (char *)"6", (char *)"--size-bytes", (char *)"16M", (char *)"--yes",
                       NULL};
  if (run_cmd_capture(mkfs_argv, 0, out, sizeof(out)) != 0)
  {
    tlogf("mkfs v6 fsync-failure fixture failed: %s", out);
    return 1;
  }
  if (seed_v6_readonly_traversal_fixture(img) != 0)
  {
    tlogf("v6 fsync-failure fixture seed failed");
    return 1;
  }

  const char *mnt = "mnt-v6-fsync-fail";
  const char *log_path = "v6-controlled-fsync-fail-mount.log";
  kafs_test_mount_options_t options = {
      .debug = "1",
      .log_path = log_path,
      .extra_options =
          "rw,v6_write_mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,"
          "fsync_policy=full",
      .timeout_ms = 15000,
  };

  if (setenv("KAFS_TEST_FORCE_FSYNC_ERROR", "all", 1) != 0)
  {
    tlogf("setenv KAFS_TEST_FORCE_FSYNC_ERROR failed: %s", strerror(errno));
    return 1;
  }
  pid_t srv = kafs_test_start_kafs(img, mnt, &options);
  unsetenv("KAFS_TEST_FORCE_FSYNC_ERROR");
  if (srv <= 0)
  {
    kafs_test_dump_log(log_path, "v6 controlled fsync-failure mount failed");
    return 77;
  }

  int rc = 0;
  int fd = -1;
  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/sync-fail.bin", mnt);
  fd = open(path, O_CREAT | O_EXCL | O_RDWR, 0644);
  if (fd < 0)
  {
    tlogf("v6 fsync-failure create failed: %s", strerror(errno));
    rc = 1;
    goto out_stop;
  }

  unsigned char block[4096];
  for (size_t i = 0; i < sizeof(block); ++i)
    block[i] = (unsigned char)(0x51u + (i % 29u));
  if (write(fd, block, sizeof(block)) != (ssize_t)sizeof(block))
  {
    tlogf("v6 fsync-failure write failed: %s", strerror(errno));
    rc = 1;
    goto out_stop;
  }

  int fdatasync_failed = 0;
  errno = 0;
  if (fdatasync(fd) != 0)
  {
    if (errno != EIO)
    {
      tlogf("v6 forced fdatasync failure errno=%s (expected EIO)", strerror(errno));
      rc = 1;
      goto out_stop;
    }
    fdatasync_failed = 1;
  }
  errno = 0;
  if (fsync(fd) == 0 || errno != EIO)
  {
    tlogf("v6 forced fsync failure errno=%s (expected EIO)", strerror(errno));
    rc = 1;
    goto out_stop;
  }

out_stop:
  if (fd >= 0)
    close(fd);
  kafs_test_stop_kafs(mnt, srv);
  if (rc == 0 && !file_contains(log_path, "fsync failed"))
  {
    tlogf("v6 controlled fsync failure log missing expected messages");
    rc = 1;
  }
  if (rc == 0 && fdatasync_failed && !file_contains(log_path, "fdatasync failed"))
  {
    tlogf("v6 controlled fdatasync failure log missing expected messages");
    rc = 1;
  }
  if (rc == 0)
  {
    char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)"--balanced-check", (char *)img,
                         NULL};
    if (run_cmd_capture(fsck_argv, 0, out, sizeof(out)) != 0)
    {
      tlogf("fsck --balanced-check after v6 fsync failure failed: %s", out);
      rc = 1;
    }
  }
  return rc;
}

static int expect_v6_write_mount_rejected(const char *label, char *const argv[], const char *needle,
                                          char *out, size_t out_sz)
{
  if (run_cmd_capture(argv, 2, out, out_sz) != 0)
  {
    tlogf("%s did not fail as expected: %s", label, out);
    return 1;
  }
  if (!strstr(out, needle))
  {
    tlogf("%s missing guidance '%s': %s", label, needle, out);
    return 1;
  }
  return 0;
}

static int check_v6_write_mount_fail_closed(const char *img, char *out, size_t out_sz)
{
  char *missing_rw_argv[] = {
      (char *)kafs_test_kafs_bin(),
      (char *)img,
      (char *)"mnt",
      (char *)"-o",
      (char *)"v6-write-mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off",
      NULL,
  };
  if (expect_v6_write_mount_rejected("v6 write mount without rw", missing_rw_argv,
                                     "requires explicit -o rw", out, out_sz) != 0)
    return 1;

  char *ro_argv[] = {
      (char *)kafs_test_kafs_bin(),
      (char *)img,
      (char *)"mnt",
      (char *)"-o",
      (char *)"ro,v6_write_mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off",
      NULL,
  };
  if (expect_v6_write_mount_rejected("v6 write mount with ro", ro_argv, "does not allow -o ro",
                                     out, out_sz) != 0)
    return 1;

  char *inspection_argv[] = {
      (char *)kafs_test_kafs_bin(),
      (char *)img,
      (char *)"mnt",
      (char *)"-o",
      (char *)"rw,v6_write_mount,v6_inspection_mount,no_writeback_cache,no_trim_on_free,"
              "bg_dedup_scan=off",
      NULL,
  };
  if (expect_v6_write_mount_rejected("v6 write mount with inspection", inspection_argv,
                                     "does not allow v6_inspection_mount", out, out_sz) != 0)
    return 1;

  char *writeback_argv[] = {
      (char *)kafs_test_kafs_bin(),
      (char *)img,
      (char *)"mnt",
      (char *)"-o",
      (char *)"rw,v6_write_mount,writeback_cache,no_trim_on_free,bg_dedup_scan=off",
      NULL,
  };
  if (expect_v6_write_mount_rejected("v6 write mount with writeback", writeback_argv,
                                     "requires no_writeback_cache", out, out_sz) != 0)
    return 1;

  char *trim_argv[] = {
      (char *)kafs_test_kafs_bin(),
      (char *)img,
      (char *)"mnt",
      (char *)"-o",
      (char *)"rw,v6_write_mount,no_writeback_cache,trim_on_free,bg_dedup_scan=off",
      NULL,
  };
  if (expect_v6_write_mount_rejected("v6 write mount with trim", trim_argv,
                                     "requires no_trim_on_free", out, out_sz) != 0)
    return 1;

  char *bg_argv[] = {
      (char *)kafs_test_kafs_bin(),
      (char *)img,
      (char *)"mnt",
      (char *)"-o",
      (char *)"rw,v6_write_mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=on",
      NULL,
  };
  if (expect_v6_write_mount_rejected("v6 write mount with bg dedup", bg_argv,
                                     "requires bg_dedup_scan=off", out, out_sz) != 0)
    return 1;

  char *hotplug_argv[] = {
      (char *)kafs_test_kafs_bin(),
      (char *)"--v6-write-mount",
      (char *)img,
      (char *)"mnt",
      (char *)"-o",
      (char *)"rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off",
      NULL,
  };
  if (run_cmd_capture_env(hotplug_argv, 2, "KAFS_HOTPLUG_UDS",
                          "/tmp/kafs-v6-controlled-write-hotplug.sock", out, out_sz) != 0)
  {
    tlogf("v6 write mount with hotplug did not fail as expected: %s", out);
    return 1;
  }
  if (!strstr(out, "does not allow hotplug delegated write path"))
  {
    tlogf("v6 write mount hotplug reject missing guidance: %s", out);
    return 1;
  }
  return 0;
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

  if (mkdir("mnt-kafsv6", 0755) != 0)
  {
    tlogf("mkdir mnt-kafsv6 failed");
    return 1;
  }
  char *kafsv6_legacy_argv[] = {
      (char *)kafs_test_kafs_v6_bin(), (char *)img, (char *)"mnt-kafsv6",
      (char *)"--inspection-mount",    (char *)"-o",  (char *)"ro,v6_inspection_mount",
      NULL};
  if (run_cmd_capture(kafsv6_legacy_argv, 2, out, sizeof(out)) != 0)
  {
    tlogf("kafs-v6 legacy token admission did not fail as expected: %s", out);
    return 1;
  }
  if (!strstr(out, "kafs-v6 owns the v6 runtime mode") || !strstr(out, "legacy v6_*"))
  {
    tlogf("kafs-v6 legacy token output missing dedicated-entrypoint guidance: %s", out);
    return 1;
  }

  char *kafsv6_writeback_argv[] = {
      (char *)kafs_test_kafs_v6_bin(), (char *)img, (char *)"mnt-kafsv6",
      (char *)"--inspection-mount",    (char *)"-o",  (char *)"ro,writeback_cache",
      NULL};
  if (run_cmd_capture(kafsv6_writeback_argv, 2, out, sizeof(out)) != 0)
  {
    tlogf("kafs-v6 inspection writeback_cache did not fail as expected: %s", out);
    return 1;
  }
  if (!strstr(out, "kafs-v6 inspection mode does not allow writeback_cache"))
  {
    tlogf("kafs-v6 inspection writeback_cache output missing guidance: %s", out);
    return 1;
  }

  const char *corrupt_img = "v6-desc-corrupt.img";
  char *mkfs_corrupt_argv[] = {(char *)kafs_test_mkfs_bin(), (char *)corrupt_img,
                               (char *)"--format-version", (char *)"6", (char *)"--size-bytes",
                               (char *)"64M", (char *)"--yes", NULL};
  if (run_cmd_capture(mkfs_corrupt_argv, 0, out, sizeof(out)) != 0)
  {
    tlogf("mkfs corrupt v6 fixture failed: %s", out);
    return 1;
  }
  if (corrupt_all_v6_descriptors(corrupt_img) != 0)
  {
    tlogf("failed to corrupt v6 descriptor replicas");
    return 1;
  }
  char *kafsv6_corrupt_argv[] = {
      (char *)kafs_test_kafs_v6_bin(), (char *)corrupt_img, (char *)"mnt-kafsv6",
      (char *)"--inspection-mount", (char *)"-o", (char *)"ro", NULL};
  if (run_cmd_capture(kafsv6_corrupt_argv, 2, out, sizeof(out)) != 0)
  {
    tlogf("kafs-v6 corrupt descriptor preflight did not exit 2: %s", out);
    return 1;
  }
  if (!strstr(out, "kafs-v6: format v6 admission preflight failed"))
  {
    tlogf("kafs-v6 corrupt descriptor output missing preflight failure: %s", out);
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
  if (!strstr(out, "legacy v6 inspection mount moved to kafs-v6"))
  {
    tlogf("legacy kafs v6 inspection mount missing kafs-v6 guidance: %s", out);
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
  if (!strstr(out, "legacy v6 inspection mount moved to kafs-v6"))
  {
    tlogf("legacy kafs v6 inspection mount with writeback_cache missing kafs-v6 guidance: %s",
          out);
    return 1;
  }

  if (check_v6_write_mount_fail_closed(img, out, sizeof(out)) != 0)
    return 1;

  char *handoff_argv[] = {(char *)kafs_test_kafs_bin(), (char *)img, (char *)"mnt-handoff",
                          NULL};
  if (run_cmd_capture_env(handoff_argv, 2, "KAFS_V6_ADMISSION_HANDOFF", "1", out, sizeof(out)) !=
      0)
  {
    tlogf("v6 runtime handoff did not fail as expected: %s", out);
    return 1;
  }
  if (!strstr(out, "admission handoff") || !strstr(out, "selected descriptor retained") ||
      !strstr(out, "delayed/background mutations disabled") ||
      !strstr(out, "pending_log=disabled") || !strstr(out, "tail_metadata=disabled") ||
      !strstr(out, "tombstone_gc=disabled") || !strstr(out, "bg_dedup=disabled") ||
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

  int write_rc = check_v6_controlled_write_mount_smoke(img);
  if (write_rc == 77)
    return 77;
  if (write_rc != 0)
    return 1;

  int enospc_rc = check_v6_controlled_write_enospc_smoke();
  if (enospc_rc == 77)
    return 77;
  if (enospc_rc != 0)
    return 1;

  int fsync_failure_rc = check_v6_controlled_write_fsync_failure_smoke();
  if (fsync_failure_rc == 77)
    return 77;
  if (fsync_failure_rc != 0)
    return 1;

  return 0;
}
