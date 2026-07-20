#include "test_utils.h"

#include "kafs_dirent.h"
#include "kafs_ioctl.h"
#include "kafs_offline_summary.h"
#include "kafs_superblock.h"
#include "kafs_descriptor_layout.h"

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

  kafs_descriptor_layout_report_t report;
  if (rc == 0)
    rc = kafs_descriptor_discover_layout(fd, &sb, file_size, &report);
  close(fd);
  if (rc != 0)
    return rc;
  if (!report.anchor_valid || !report.selected_found || report.replica_count != 3u ||
      report.group_count != 1u || report.shard_count != 12u || report.descriptor_bytes == 0u)
    return -EINVAL;
  if (report.replicas[0].status != KAFS_DESCRIPTOR_REPLICA_STATUS_SELECTED ||
      report.replicas[1].status != KAFS_DESCRIPTOR_REPLICA_STATUS_VALID ||
      report.replicas[2].status != KAFS_DESCRIPTOR_REPLICA_STATUS_VALID)
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
  kafs_descriptor_layout_report_t report;
  int rc = kafs_pread_all(fd, &sb, sizeof(sb), 0);
  if (rc == 0)
    rc = kafs_offline_detect_file_size(fd, &file_size);
  if (rc == 0)
    rc = kafs_descriptor_discover_layout(fd, &sb, file_size, &report);
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

static int file_contains_v6_runtime_view_admission(const char *path, const char *admission)
{
  return file_contains(path, admission) &&
         file_contains(path, "descriptor-backed runtime views active") &&
         file_contains(path, "legacy contiguous inode/bitmap tables are not installed") &&
         file_contains(path, "descriptor-backed worker policy sealed") &&
         file_contains(path, "pending_worker=disabled") &&
         file_contains(path, "tombstone_gc_worker=disabled") &&
         file_contains(path, "bg_dedup_worker=disabled") &&
         file_contains(path, "hotplug=disabled");
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
  kafs_descriptor_inode_lookup_t lookup;
  int rc = kafs_descriptor_inode_lookup(desc, desc_bytes, ino, &lookup);
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
  kafs_descriptor_layout_report_t report;
  void *desc = NULL;
  uint32_t desc_bytes = 0;

  rc = kafs_pread_all(fd, &sb, sizeof(sb), 0);
  if (rc == 0)
    rc = kafs_offline_detect_file_size(fd, &file_size);
  if (rc == 0)
    rc = kafs_descriptor_discover_layout(fd, &sb, file_size, &report);
  if (rc == 0)
    rc = kafs_descriptor_read_selected_descriptor(fd, &report, &desc, &desc_bytes);
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
  if (!strstr(out, "layout descriptor:") || !strstr(out, "status=selected"))
  {
    tlogf("fsck output missing v6 descriptor status: %s", out);
    return 1;
  }

  char *placeholder_argv[] = {(char *)kafs_test_kafs_v6_bin(), (char *)img, (char *)"mnt", NULL};
  if (run_cmd_capture(placeholder_argv, 2, out, sizeof(out)) != 0)
  {
    tlogf("kafs-v6 retirement placeholder did not exit 2: %s", out);
    return 1;
  }
  if (!strstr(out, "format v6 runtime support has been retired") ||
      !strstr(out, "migrate or recreate the image as format v7"))
  {
    tlogf("kafs-v6 retirement placeholder output is incomplete: %s", out);
    return 1;
  }

  char *help_argv[] = {(char *)kafs_test_kafs_v6_bin(), (char *)"--help", NULL};
  if (run_cmd_capture(help_argv, 0, out, sizeof(out)) != 0 ||
      !strstr(out, "Format v6 runtime support has been retired"))
  {
    tlogf("kafs-v6 retirement help contract failed: %s", out);
    return 1;
  }

  return 0;
}
