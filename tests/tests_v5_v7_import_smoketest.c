#include "test_utils.h"

#include "kafs_inode.h"
#include "kafs_superblock.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

typedef struct import_fixture
{
  ino_t inline_ino;
  ino_t block_ino;
  struct stat nested_stat;
  struct stat inline_stat;
  struct stat tail_stat;
  struct stat block_stat;
  struct stat double_stat;
  struct stat symlink_stat;
  struct stat empty_stat;
} import_fixture_t;

static void tlogf(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  fputc('\n', stderr);
  va_end(ap);
}

static int run_command(char *const argv[], char *output, size_t output_bytes)
{
  int pipefd[2];
  if (pipe(pipefd) != 0)
    return -errno;
  pid_t child = fork();
  if (child < 0)
  {
    close(pipefd[0]);
    close(pipefd[1]);
    return -errno;
  }
  if (child == 0)
  {
    close(pipefd[0]);
    dup2(pipefd[1], STDOUT_FILENO);
    dup2(pipefd[1], STDERR_FILENO);
    close(pipefd[1]);
    execv(argv[0], argv);
    _exit(127);
  }
  close(pipefd[1]);
  size_t used = 0u;
  while (used + 1u < output_bytes)
  {
    ssize_t got = read(pipefd[0], output + used, output_bytes - used - 1u);
    if (got < 0 && errno == EINTR)
      continue;
    if (got <= 0)
      break;
    used += (size_t)got;
  }
  output[used] = '\0';
  char discard[4096];
  while (read(pipefd[0], discard, sizeof(discard)) > 0)
    ;
  close(pipefd[0]);
  int status = 0;
  if (waitpid(child, &status, 0) < 0)
    return -errno;
  return WIFEXITED(status) ? WEXITSTATUS(status) : 128;
}

static int capture_inventory(const char *mountpoint, const char *output_path)
{
  const char *inventory = kafs_test_v5_v7_mounted_inventory_bin();
  char *argv[] = {(char *)inventory, (char *)"--root", (char *)mountpoint, (char *)"--output",
                  (char *)output_path, NULL};
  char output[4096];
  int rc = run_command(argv, output, sizeof(output));
  if (rc != 0)
    tlogf("mounted inventory failed: %s", output);
  return rc;
}

static int capture_sha256(const char *path, const char *output_path)
{
  char *argv[] = {(char *)"/usr/bin/env", (char *)"sha256sum", (char *)path, NULL};
  char output[4096];
  int rc = run_command(argv, output, sizeof(output));
  if (rc != 0 || strlen(output) < 64u)
    return -EIO;
  FILE *stream = fopen(output_path, "wx");
  if (!stream)
    return -errno;
  rc = fprintf(stream, "%.64s\n", output) == 65 ? 0 : -EIO;
  if (fclose(stream) != 0 && rc == 0)
    rc = -errno;
  return rc;
}

static int write_text_result(const char *path, const char *text)
{
  FILE *stream = fopen(path, "wx");
  if (!stream)
    return -errno;
  size_t bytes = strlen(text);
  int rc = fwrite(text, 1u, bytes, stream) == bytes ? 0 : -EIO;
  if (fclose(stream) != 0 && rc == 0)
    rc = -errno;
  return rc;
}

static int validate_import_result(const char *output, const char *mode, const char *status,
                                  int exit_status, int admission_ready)
{
  char mode_field[64];
  char status_field[64];
  char exit_field[64];
  snprintf(mode_field, sizeof(mode_field), "\"mode\":\"%s\"", mode);
  snprintf(status_field, sizeof(status_field), "\"status\":\"%s\"", status);
  snprintf(exit_field, sizeof(exit_field), "\"exit_status\":%d", exit_status);
  if (!strstr(output, "\"schema\":\"KAFS.V5V7MigrationImportResult.v1\"") ||
      !strstr(output, "\"operation\":\"migrate-import-v7\"") || !strstr(output, mode_field) ||
      !strstr(output, status_field) || !strstr(output, exit_field))
    return -EINVAL;
  if (exit_status != 0)
    return strstr(output, "\"result\":null") ? 0 : -EINVAL;
  return strstr(output, admission_ready ? "\"destination_admission_ready\":true"
                                        : "\"destination_admission_ready\":false")
             ? 0
             : -EINVAL;
}

static int mkfs_v5(const char *path)
{
  const char *mkfs = kafs_test_mkfs_bin();
  if (!mkfs)
    return -ENOENT;
  char *argv[] = {(char *)mkfs, (char *)"--yes", (char *)"--format-version", (char *)"5",
                  (char *)"--inodes", (char *)"256", (char *)path, (char *)"-s",
                  (char *)"96M", NULL};
  char output[4096];
  return run_command(argv, output, sizeof(output));
}

static int disable_v5_pending_log(const char *path)
{
  int fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t superblock;
  ssize_t got = pread(fd, &superblock, sizeof(superblock), 0);
  int rc = got == (ssize_t)sizeof(superblock) ? 0 : (got < 0 ? -errno : -EIO);
  if (rc == 0 && (kafs_sb_magic_get(&superblock) != KAFS_MAGIC ||
                  kafs_sb_format_version_get(&superblock) != KAFS_FORMAT_VERSION_V5))
    rc = -EPROTO;
  if (rc == 0)
  {
    kafs_sb_pendinglog_offset_set(&superblock, 0u);
    kafs_sb_pendinglog_size_set(&superblock, 0u);
    ssize_t written = pwrite(fd, &superblock, sizeof(superblock), 0);
    if (written != (ssize_t)sizeof(superblock))
      rc = written < 0 ? -errno : -EIO;
    else if (fsync(fd) != 0)
      rc = -errno;
  }
  close(fd);
  return rc;
}

static uint8_t pattern_byte(uint64_t offset)
{
  uint64_t in_block = offset % 4096u;
  if (in_block < sizeof(uint64_t))
    return (uint8_t)((offset / 4096u) >> (in_block * 8u));
  return (uint8_t)((offset * 131u + 17u) & 0xffu);
}

static int write_pattern_file(const char *path, uint64_t bytes, mode_t mode)
{
  int fd = open(path, O_CREAT | O_EXCL | O_WRONLY, mode);
  if (fd < 0)
    return -errno;
  uint8_t buffer[4096];
  uint64_t done = 0u;
  int rc = 0;
  while (done < bytes)
  {
    size_t chunk = bytes - done < sizeof(buffer) ? (size_t)(bytes - done) : sizeof(buffer);
    for (size_t i = 0u; i < chunk; ++i)
      buffer[i] = pattern_byte(done + i);
    ssize_t written = write(fd, buffer, chunk);
    if (written != (ssize_t)chunk)
    {
      rc = written < 0 ? -errno : -EIO;
      break;
    }
    done += chunk;
  }
  if (rc == 0 && fsync(fd) != 0)
    rc = -errno;
  close(fd);
  return rc;
}

static int verify_pattern_file(const char *path, uint64_t bytes)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;
  uint8_t buffer[4096];
  uint64_t done = 0u;
  int rc = 0;
  while (done < bytes)
  {
    size_t chunk = bytes - done < sizeof(buffer) ? (size_t)(bytes - done) : sizeof(buffer);
    ssize_t got = read(fd, buffer, chunk);
    if (got != (ssize_t)chunk)
    {
      rc = got < 0 ? -errno : -EIO;
      break;
    }
    for (size_t i = 0u; i < chunk; ++i)
      if (buffer[i] != pattern_byte(done + i))
      {
        rc = -EILSEQ;
        break;
      }
    if (rc != 0)
      break;
    done += chunk;
  }
  close(fd);
  return rc;
}

static int drain_path(const char *path, int directory)
{
  int flags = O_RDONLY;
  if (directory)
    flags |= O_DIRECTORY;
  int fd = open(path, flags);
  if (fd < 0)
    return -errno;
  int rc = 0;
  for (unsigned attempt = 0u; attempt < 3u; ++attempt)
    if (fsync(fd) != 0)
    {
      rc = -errno;
      break;
    }
  close(fd);
  return rc;
}

static int populate_v5(const char *mountpoint)
{
  char nested[PATH_MAX];
  char inline_path[PATH_MAX];
  char tail_path[PATH_MAX];
  char block_path[PATH_MAX];
  char double_path[PATH_MAX];
  char hardlink_path[PATH_MAX];
  char symlink_path[PATH_MAX];
  char empty_path[PATH_MAX];
  snprintf(nested, sizeof(nested), "%s/nested", mountpoint);
  snprintf(inline_path, sizeof(inline_path), "%s/inline.txt", mountpoint);
  snprintf(tail_path, sizeof(tail_path), "%s/nested/tail.bin", mountpoint);
  snprintf(block_path, sizeof(block_path), "%s/nested/indirect.bin", mountpoint);
  snprintf(double_path, sizeof(double_path), "%s/nested/double-indirect.bin", mountpoint);
  snprintf(hardlink_path, sizeof(hardlink_path), "%s/indirect-link", mountpoint);
  snprintf(symlink_path, sizeof(symlink_path), "%s/payload-link", mountpoint);
  snprintf(empty_path, sizeof(empty_path), "%s/empty", mountpoint);
  if (mkdir(nested, 0750) != 0 || write_pattern_file(inline_path, 37u, 0640) != 0 ||
      write_pattern_file(tail_path, 513u, 0604) != 0 ||
      write_pattern_file(block_path, 13u * 4096u, 0644) != 0 ||
      write_pattern_file(double_path, 1040u * 4096u, 0644) != 0 ||
      write_pattern_file(empty_path, 0u, 0600) != 0 || link(block_path, hardlink_path) != 0 ||
      symlink("nested/tail.bin", symlink_path) != 0)
    return -errno;
  struct timespec times[2] = {
      {.tv_sec = 1700000001, .tv_nsec = 123456789},
      {.tv_sec = 1700000002, .tv_nsec = 987654321},
  };
  if (utimensat(AT_FDCWD, block_path, times, 0) != 0)
    return -errno;
  if (drain_path(inline_path, 0) != 0 || drain_path(tail_path, 0) != 0 ||
      drain_path(block_path, 0) != 0 || drain_path(double_path, 0) != 0 ||
      drain_path(empty_path, 0) != 0 ||
      drain_path(nested, 1) != 0 || drain_path(mountpoint, 1) != 0)
    return -EIO;
  return 0;
}

static int capture_fixture(const char *mountpoint, import_fixture_t *fixture)
{
  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/nested", mountpoint);
  if (stat(path, &fixture->nested_stat) != 0)
    return -errno;
  snprintf(path, sizeof(path), "%s/inline.txt", mountpoint);
  if (stat(path, &fixture->inline_stat) != 0)
    return -errno;
  snprintf(path, sizeof(path), "%s/nested/tail.bin", mountpoint);
  if (stat(path, &fixture->tail_stat) != 0)
    return -errno;
  snprintf(path, sizeof(path), "%s/nested/indirect.bin", mountpoint);
  if (stat(path, &fixture->block_stat) != 0)
    return -errno;
  snprintf(path, sizeof(path), "%s/nested/double-indirect.bin", mountpoint);
  if (stat(path, &fixture->double_stat) != 0)
    return -errno;
  snprintf(path, sizeof(path), "%s/payload-link", mountpoint);
  if (lstat(path, &fixture->symlink_stat) != 0)
    return -errno;
  snprintf(path, sizeof(path), "%s/empty", mountpoint);
  if (stat(path, &fixture->empty_stat) != 0)
    return -errno;
  fixture->inline_ino = fixture->inline_stat.st_ino;
  fixture->block_ino = fixture->block_stat.st_ino;
  return 0;
}

static int verify_metadata(const struct stat *actual, const struct stat *expected, int check_size)
{
  if (actual->st_ino != expected->st_ino || actual->st_mode != expected->st_mode ||
      actual->st_uid != expected->st_uid || actual->st_gid != expected->st_gid ||
      actual->st_nlink != expected->st_nlink ||
      (check_size && actual->st_size != expected->st_size) ||
      actual->st_atim.tv_sec != expected->st_atim.tv_sec ||
      actual->st_atim.tv_nsec != expected->st_atim.tv_nsec ||
      actual->st_mtim.tv_sec != expected->st_mtim.tv_sec ||
      actual->st_mtim.tv_nsec != expected->st_mtim.tv_nsec ||
      actual->st_ctim.tv_sec != expected->st_ctim.tv_sec ||
      actual->st_ctim.tv_nsec != expected->st_ctim.tv_nsec)
  {
    tlogf("metadata mismatch: actual ino=%ju mode=%jo uid=%ju gid=%ju nlink=%ju size=%jd "
          "atime=%jd.%09ld mtime=%jd.%09ld ctime=%jd.%09ld; expected ino=%ju mode=%jo "
          "uid=%ju gid=%ju nlink=%ju size=%jd atime=%jd.%09ld mtime=%jd.%09ld ctime=%jd.%09ld",
          (uintmax_t)actual->st_ino, (uintmax_t)actual->st_mode, (uintmax_t)actual->st_uid,
          (uintmax_t)actual->st_gid, (uintmax_t)actual->st_nlink, (intmax_t)actual->st_size,
          (intmax_t)actual->st_atim.tv_sec, actual->st_atim.tv_nsec,
          (intmax_t)actual->st_mtim.tv_sec, actual->st_mtim.tv_nsec,
          (intmax_t)actual->st_ctim.tv_sec, actual->st_ctim.tv_nsec,
          (uintmax_t)expected->st_ino, (uintmax_t)expected->st_mode, (uintmax_t)expected->st_uid,
          (uintmax_t)expected->st_gid, (uintmax_t)expected->st_nlink, (intmax_t)expected->st_size,
          (intmax_t)expected->st_atim.tv_sec, expected->st_atim.tv_nsec,
          (intmax_t)expected->st_mtim.tv_sec, expected->st_mtim.tv_nsec,
          (intmax_t)expected->st_ctim.tv_sec, expected->st_ctim.tv_nsec);
    return -EBADE;
  }
  return 0;
}

static int verify_v7(const char *mountpoint, const import_fixture_t *fixture)
{
  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/inline.txt", mountpoint);
  if (verify_pattern_file(path, 37u) != 0)
    return -EIO;
  struct stat actual;
  if (stat(path, &actual) != 0 || verify_metadata(&actual, &fixture->inline_stat, 1) != 0)
    return -EBADE;
  snprintf(path, sizeof(path), "%s/nested/tail.bin", mountpoint);
  if (verify_pattern_file(path, 513u) != 0)
    return -EIO;
  if (stat(path, &actual) != 0 || verify_metadata(&actual, &fixture->tail_stat, 1) != 0)
    return -EBADE;
  snprintf(path, sizeof(path), "%s/nested/indirect.bin", mountpoint);
  if (verify_pattern_file(path, 13u * 4096u) != 0)
    return -EIO;
  struct stat original;
  if (stat(path, &original) != 0 || verify_metadata(&original, &fixture->block_stat, 1) != 0)
    return -EBADE;
  snprintf(path, sizeof(path), "%s/nested/double-indirect.bin", mountpoint);
  if (verify_pattern_file(path, 1040u * 4096u) != 0 || stat(path, &actual) != 0 ||
      verify_metadata(&actual, &fixture->double_stat, 1) != 0)
    return -EBADE;
  snprintf(path, sizeof(path), "%s/indirect-link", mountpoint);
  struct stat linked;
  if (stat(path, &linked) != 0 || original.st_ino != linked.st_ino || original.st_nlink != 2 ||
      linked.st_nlink != 2)
    return -EMLINK;
  if (verify_metadata(&linked, &fixture->block_stat, 1) != 0)
    return -EBADE;
  snprintf(path, sizeof(path), "%s/nested", mountpoint);
  if (stat(path, &actual) != 0 || verify_metadata(&actual, &fixture->nested_stat, 0) != 0)
    return -EBADE;
  snprintf(path, sizeof(path), "%s/payload-link", mountpoint);
  if (lstat(path, &actual) != 0 || verify_metadata(&actual, &fixture->symlink_stat, 1) != 0)
    return -EBADE;
  char target[64];
  ssize_t bytes = readlink(path, target, sizeof(target));
  if (bytes != (ssize_t)strlen("nested/tail.bin") ||
      memcmp(target, "nested/tail.bin", (size_t)bytes) != 0)
    return -EILSEQ;
  snprintf(path, sizeof(path), "%s/empty", mountpoint);
  if (stat(path, &original) != 0 || verify_metadata(&original, &fixture->empty_stat, 1) != 0)
    return -EIO;
  return 0;
}

static int copy_file(const char *source, const char *destination)
{
  int in = open(source, O_RDONLY);
  int out = open(destination, O_CREAT | O_EXCL | O_WRONLY, 0600);
  if (in < 0 || out < 0)
  {
    if (in >= 0)
      close(in);
    if (out >= 0)
      close(out);
    return -errno;
  }
  uint8_t buffer[64u * 1024u];
  int rc = 0;
  for (;;)
  {
    ssize_t got = read(in, buffer, sizeof(buffer));
    if (got < 0 && errno == EINTR)
      continue;
    if (got < 0)
    {
      rc = -errno;
      break;
    }
    if (got == 0)
      break;
    size_t done = 0u;
    while (done < (size_t)got)
    {
      ssize_t written = write(out, buffer + done, (size_t)got - done);
      if (written < 0 && errno == EINTR)
        continue;
      if (written <= 0)
      {
        rc = written < 0 ? -errno : -EIO;
        break;
      }
      done += (size_t)written;
    }
    if (rc != 0)
      break;
  }
  close(in);
  close(out);
  return rc;
}

static int source_inode_location(int fd, kafs_ssuperblock_t *superblock, uint32_t ino,
                                 uint64_t *bitmap_off, uint64_t *inode_off)
{
  ssize_t got = pread(fd, superblock, sizeof(*superblock), 0);
  if (got != (ssize_t)sizeof(*superblock))
    return got < 0 ? -errno : -EIO;
  uint64_t block_size = kafs_sb_blksize_get(superblock);
  uint64_t bitmap_bytes = ((uint64_t)kafs_sb_blkcnt_get(superblock) + 7u) / 8u;
  *bitmap_off = block_size;
  *inode_off = (*bitmap_off + bitmap_bytes + 7u) & ~UINT64_C(7);
  *inode_off = (*inode_off + block_size - 1u) & ~(block_size - 1u);
  *inode_off += (uint64_t)ino * sizeof(kafs_sinode_v5_t);
  return 0;
}

static int mutate_inode(const char *path, uint32_t ino, int sparse)
{
  int fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t sb;
  uint64_t bitmap_off = 0u;
  uint64_t inode_off = 0u;
  int rc = source_inode_location(fd, &sb, ino, &bitmap_off, &inode_off);
  kafs_sinode_v5_t inode;
  if (rc == 0 && pread(fd, &inode, sizeof(inode), (off_t)inode_off) != (ssize_t)sizeof(inode))
    rc = -EIO;
  if (rc == 0 && sparse)
    inode.i_blkreftbl[0] = kafs_blkcnt_htos(0u);
  else if (rc == 0)
    inode.i_mode = kafs_mode_htos((kafs_mode_t)(S_IFIFO | 0644));
  if (rc == 0 && pwrite(fd, &inode, sizeof(inode), (off_t)inode_off) != (ssize_t)sizeof(inode))
    rc = -EIO;
  close(fd);
  return rc;
}

static int mutate_source_reference(const char *path, uint32_t ino, int pending)
{
  int fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t superblock;
  uint64_t bitmap_off = 0u;
  uint64_t inode_off = 0u;
  int rc = source_inode_location(fd, &superblock, ino, &bitmap_off, &inode_off);
  kafs_sinode_v5_t inode;
  if (rc == 0 && pread(fd, &inode, sizeof(inode), (off_t)inode_off) != (ssize_t)sizeof(inode))
    rc = -EIO;
  kafs_blkcnt_t block = rc == 0 ? kafs_blkcnt_stoh(inode.i_blkreftbl[0]) : 0u;
  if (rc == 0 && block == 0u)
    rc = -EINVAL;
  if (rc == 0 && pending)
  {
    inode.i_blkreftbl[0] = kafs_blkcnt_htos(block | UINT32_C(0x80000000));
    if (pwrite(fd, &inode, sizeof(inode), (off_t)inode_off) != (ssize_t)sizeof(inode))
      rc = -EIO;
  }
  else if (rc == 0)
  {
    uint8_t byte = 0u;
    off_t byte_off = (off_t)(bitmap_off + block / 8u);
    if (pread(fd, &byte, sizeof(byte), byte_off) != (ssize_t)sizeof(byte))
      rc = -EIO;
    else
    {
      byte &= (uint8_t)~(1u << (block % 8u));
      if (pwrite(fd, &byte, sizeof(byte), byte_off) != (ssize_t)sizeof(byte))
        rc = -EIO;
    }
  }
  if (rc == 0 && fsync(fd) != 0)
    rc = -errno;
  close(fd);
  return rc;
}

static int verify_v7_image(const char *image, const char *mountpoint, const char *log_path,
                           const char *inventory_path, const import_fixture_t *fixture)
{
  const kafs_test_mount_options_t options = {
      .debug = "1", .log_path = log_path, .extra_options = "ro", .timeout_ms = 10000};
  pid_t pid = kafs_test_start_kafs_v7(image, mountpoint, &options);
  if (pid <= 0)
  {
    kafs_test_dump_log(log_path, "import destination mount failed");
    return -EIO;
  }
  int rc = verify_v7(mountpoint, fixture);
  if (rc == 0 && inventory_path)
    rc = capture_inventory(mountpoint, inventory_path) == 0 ? 0 : -EIO;
  kafs_test_stop_kafs(mountpoint, pid);
  return rc;
}

static int run_import_with_output(const char *source, const char *destination, const char *size,
                                  int dry_run, int json_output, char *output, size_t output_bytes)
{
  const char *resize = kafs_test_kafsresize_bin();
  char *argv[16];
  int index = 0;
  argv[index++] = (char *)resize;
  argv[index++] = (char *)"--migrate-import-v7";
  argv[index++] = (char *)"--src-image";
  argv[index++] = (char *)source;
  argv[index++] = (char *)"--dst-image";
  argv[index++] = (char *)destination;
  argv[index++] = (char *)"--size-bytes";
  argv[index++] = (char *)size;
  argv[index++] = (char *)"--inodes";
  argv[index++] = (char *)"256";
  argv[index++] = (char *)"--v7-group-count";
  argv[index++] = (char *)"2";
  if (json_output)
    argv[index++] = (char *)"--json";
  if (dry_run)
    argv[index++] = (char *)"--dry-run";
  argv[index] = NULL;
  return run_command(argv, output, output_bytes);
}

static int run_import(const char *source, const char *destination, const char *size,
                      int dry_run, char *output, size_t output_bytes)
{
  return run_import_with_output(source, destination, size, dry_run, 1, output, output_bytes);
}

static int assert_failed_without_final(const char *source, const char *destination,
                                       const char *size)
{
  char output[8192];
  int status = run_import(source, destination, size, 0, output, sizeof(output));
  if (status == 0 || validate_import_result(output, "import", "FAIL", 1, 0) != 0 ||
      access(destination, F_OK) == 0)
  {
    tlogf("negative import unexpectedly published %s: status=%d output=%s", destination, status,
          output);
    return -1;
  }
  return 0;
}

int main(void)
{
  if (kafs_test_enter_tmpdir("v5_v7_import_smoketest") != 0)
    return 77;
  if (access("/dev/fuse", R_OK | W_OK) != 0 || access(kafs_test_kafsresize_bin(), X_OK) != 0 ||
      access(kafs_test_fsck_bin(), X_OK) != 0 || access(kafs_test_kafsdump_bin(), X_OK) != 0 ||
      access(kafs_test_v5_v7_mounted_inventory_bin(), X_OK) != 0)
    return 77;
  const char *source = "source-v5.img";
  const char *destination = "destination-v7.img";
  if (mkfs_v5(source) != 0 || disable_v5_pending_log(source) != 0)
    return 77;
  const kafs_test_mount_options_t source_mount_options = {
      .debug = "1",
      .log_path = "source-v5.log",
      .extra_options = "bg_dedup_scan=off,fsync_policy=full",
      .timeout_ms = 10000,
  };
  pid_t source_pid = kafs_test_start_kafs(source, "source-mount", &source_mount_options);
  if (source_pid <= 0)
    return 77;
  import_fixture_t fixture = {0};
  if (populate_v5("source-mount") != 0)
  {
    kafs_test_stop_kafs("source-mount", source_pid);
    return 1;
  }
  kafs_test_stop_kafs("source-mount", source_pid);
  const kafs_test_mount_options_t source_readback_options = {
      .debug = "1",
      .log_path = "source-v5-readback.log",
      .extra_options = "ro",
      .timeout_ms = 10000,
  };
  source_pid = kafs_test_start_kafs(source, "source-mount", &source_readback_options);
  if (source_pid <= 0)
    return 1;
  int capture_rc = capture_fixture("source-mount", &fixture);
  if (capture_rc == 0)
    capture_rc = capture_inventory("source-mount", "source-inventory.json");
  kafs_test_stop_kafs("source-mount", source_pid);
  if (capture_rc != 0)
    return 1;

  struct stat source_before;
  if (stat(source, &source_before) != 0 ||
      capture_sha256(source, "source-before.sha256") != 0)
    return 1;

  char output[16384];
  const char *fsck = kafs_test_fsck_bin();
  char *source_fsck_argv[] = {(char *)fsck, (char *)"--full-check", (char *)source, NULL};
  if (run_command(source_fsck_argv, output, sizeof(output)) != 0)
  {
    tlogf("source fsck failed: %s", output);
    return 1;
  }
  if (run_import(source, destination, "96M", 1, output, sizeof(output)) != 0 ||
      validate_import_result(output, "dry-run", "PASS", 0, 0) != 0 ||
      write_text_result("import-dry-run-result.json", output) != 0 || access(destination, F_OK) == 0)
  {
    tlogf("dry-run failed: %s", output);
    return 1;
  }
  if (run_import_with_output(source, "human-dry-run-v7.img", "96M", 1, 0, output,
                             sizeof(output)) != 0 ||
      !strstr(output, "kafsresize: migrate-import-v7 dry-run PASS") ||
      access("human-dry-run-v7.img", F_OK) == 0)
  {
    tlogf("compatible human dry-run result failed: %s", output);
    return 1;
  }
  if (run_import(source, destination, "96M", 0, output, sizeof(output)) != 0 ||
      validate_import_result(output, "import", "PASS", 0, 1) != 0 ||
      write_text_result("import-result.json", output) != 0)
  {
    tlogf("normal import failed: %s", output);
    return 1;
  }
  struct stat published;
  if (stat(destination, &published) != 0 || (published.st_mode & 0777u) != 0644u ||
      published.st_nlink != 1 || access("destination-v7.img.kafs-import-partial", F_OK) == 0)
  {
    tlogf("normal import publication state is invalid");
    return 1;
  }
  if (run_import(source, destination, "96M", 0, output, sizeof(output)) == 0)
  {
    tlogf("normal import replaced an existing final destination: %s", output);
    return 1;
  }
  struct stat after_no_replace;
  if (stat(destination, &after_no_replace) != 0 ||
      after_no_replace.st_dev != published.st_dev || after_no_replace.st_ino != published.st_ino ||
      after_no_replace.st_size != published.st_size ||
      after_no_replace.st_mtim.tv_sec != published.st_mtim.tv_sec ||
      after_no_replace.st_mtim.tv_nsec != published.st_mtim.tv_nsec)
  {
    tlogf("existing final destination changed during no-replace check");
    return 1;
  }
  char *fsck_argv[] = {(char *)fsck, (char *)destination, NULL};
  if (run_command(fsck_argv, output, sizeof(output)) != 0 ||
      !strstr(output, "format v7 raw-layout fsck: status=ok"))
  {
    tlogf("destination fsck failed: %s", output);
    return 1;
  }
  const char *dump = kafs_test_kafsdump_bin();
  char *dump_argv[] = {(char *)dump, (char *)"--json", (char *)destination, NULL};
  if (run_command(dump_argv, output, sizeof(output)) != 0 ||
      !strstr(output, "\"layout_descriptor\": {\"status\": \"ok\"") ||
      !strstr(output, "\"group_count\": 2"))
  {
    tlogf("destination dump failed: %s", output);
    return 1;
  }
  int verify_rc = verify_v7_image(destination, "destination-mount", "destination-v7.log",
                                   "destination-inventory.json", &fixture);
  if (verify_rc != 0)
  {
    tlogf("destination semantic verification failed: %s", strerror(-verify_rc));
    return 1;
  }
  tlogf("KAFS_V5_V7_MIGRATION_CASE normal PASS");
  tlogf("KAFS_V5_V7_MIGRATION_CASE idempotence PASS");

  if (copy_file(source, "unsupported-v5.img") != 0 ||
      mutate_inode("unsupported-v5.img", (uint32_t)fixture.inline_ino, 0) != 0 ||
      assert_failed_without_final("unsupported-v5.img", "unsupported-v7.img", "96M") != 0)
    return 1;
  if (copy_file(source, "sparse-v5.img") != 0 ||
      mutate_inode("sparse-v5.img", (uint32_t)fixture.block_ino, 1) != 0 ||
      assert_failed_without_final("sparse-v5.img", "sparse-v7.img", "96M") != 0)
    return 1;
  if (assert_failed_without_final(source, "capacity-v7.img", "1M") != 0)
    return 1;

  if (copy_file(source, "pending-ref-v5.img") != 0 ||
      mutate_source_reference("pending-ref-v5.img", (uint32_t)fixture.block_ino, 1) != 0 ||
      assert_failed_without_final("pending-ref-v5.img", "pending-ref-v7.img", "96M") != 0)
    return 1;
  tlogf("KAFS_V5_V7_MIGRATION_CASE pending_ref_rejection PASS");
  if (copy_file(source, "bitmap-invalid-v5.img") != 0 ||
      mutate_source_reference("bitmap-invalid-v5.img", (uint32_t)fixture.block_ino, 0) != 0 ||
      assert_failed_without_final("bitmap-invalid-v5.img", "bitmap-invalid-v7.img", "96M") != 0)
    return 1;
  tlogf("KAFS_V5_V7_MIGRATION_CASE bitmap_invalid_rejection PASS");

  if (setenv("KAFS_V7_IMPORT_FAIL_AFTER_OBJECTS", "2", 1) != 0)
    return 1;
  int partial_rc = assert_failed_without_final(source, "replay-v7.img", "96M");
  unsetenv("KAFS_V7_IMPORT_FAIL_AFTER_OBJECTS");
  if (partial_rc != 0 || access("replay-v7.img.kafs-import-partial", F_OK) != 0 ||
      rename("replay-v7.img.kafs-import-partial", "replay-attempt1-preserved.img") != 0)
  {
    tlogf("interrupted import did not preserve the attempt-1 work image");
    return 1;
  }
  tlogf("KAFS_V5_V7_MIGRATION_CASE replay_required PASS");
  if (run_import(source, "replay-v7.img", "96M", 0, output, sizeof(output)) != 0 ||
      validate_import_result(output, "import", "PASS", 0, 1) != 0 ||
      verify_v7_image("replay-v7.img", "replay-mount", "replay-v7.log",
                      "replay-inventory.json", &fixture) != 0)
  {
    tlogf("full replay failed: %s", output);
    return 1;
  }
  tlogf("KAFS_V5_V7_MIGRATION_CASE replayed_accept PASS");

  if (setenv("KAFS_V7_IMPORT_FAIL_AFTER_OBJECTS", "2", 1) != 0)
    return 1;
  partial_rc = assert_failed_without_final(source, "rollback-v7.img", "96M");
  unsetenv("KAFS_V7_IMPORT_FAIL_AFTER_OBJECTS");
  if (partial_rc != 0 || access("rollback-v7.img", F_OK) == 0 ||
      rename("rollback-v7.img.kafs-import-partial", "rollback-preserved-failed.img") != 0)
  {
    tlogf("rollback did not preserve the failed destination");
    return 1;
  }
  tlogf("KAFS_V5_V7_MIGRATION_CASE rollback PASS");

  struct stat source_after;
  if (stat(source, &source_after) != 0 || source_before.st_dev != source_after.st_dev ||
      source_before.st_ino != source_after.st_ino || source_before.st_size != source_after.st_size ||
      source_before.st_mtim.tv_sec != source_after.st_mtim.tv_sec ||
      source_before.st_mtim.tv_nsec != source_after.st_mtim.tv_nsec ||
      source_before.st_ctim.tv_sec != source_after.st_ctim.tv_sec ||
      source_before.st_ctim.tv_nsec != source_after.st_ctim.tv_nsec)
  {
    tlogf("source image identity changed during migration rehearsal");
    return 1;
  }
  if (capture_sha256(source, "source-after.sha256") != 0)
    return 1;
  tlogf("KAFS_V5_V7_MIGRATION_CASE source_immutability PASS");
  tlogf("v5_v7_import_smoketest OK");
  return 0;
}
