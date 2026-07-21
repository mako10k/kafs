#pragma once

#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define KAFS_V7_TEST_CRASH_POINT_ENV "KAFS_V7_TEST_CRASH_POINT"
#define KAFS_V7_TEST_PAUSE_POINT_ENV "KAFS_V7_TEST_PAUSE_POINT"
#define KAFS_V7_TEST_PAUSE_MARKER_ENV "KAFS_V7_TEST_PAUSE_MARKER"

typedef enum kafs_v7_test_fault_point
{
  KAFS_V7_TEST_FAULT_JOURNAL_PUBLISH = 0,
  KAFS_V7_TEST_FAULT_CHECKPOINT_COPY,
  KAFS_V7_TEST_FAULT_METADATA_APPLY,
  KAFS_V7_TEST_FAULT_JOURNAL_RECLAIM,
} kafs_v7_test_fault_point_t;

static inline const char *kafs_v7_test_fault_name(kafs_v7_test_fault_point_t point)
{
  static const char *const names[] = {
      "journal_publish",
      "checkpoint_copy",
      "metadata_apply",
      "journal_reclaim",
  };
  return (unsigned)point < sizeof(names) / sizeof(names[0]) ? names[point] : NULL;
}

static inline int kafs_v7_test_fault_exit_status(kafs_v7_test_fault_point_t point)
{
  return 86 + (int)point;
}

static inline int kafs_v7_test_fault_sync_marker_directory(const char *marker)
{
  char *directory = strdup(marker);
  if (!directory)
    return -1;
  char *separator = strrchr(directory, '/');
  if (!separator)
  {
    free(directory);
    return -1;
  }
  if (separator == directory)
    separator[1] = '\0';
  else
    *separator = '\0';
  int dirfd = open(directory, O_RDONLY | O_DIRECTORY);
  free(directory);
  if (dirfd < 0)
    return -1;
  int rc = fsync(dirfd) == 0 ? 0 : -1;
  if (close(dirfd) != 0)
    rc = -1;
  return rc;
}

static inline void kafs_v7_test_fault_pause(const char *point, const char *marker)
{
  if (!marker || marker[0] != '/')
    _exit(125);
  int fd = open(marker, O_WRONLY | O_CREAT | O_EXCL, 0600);
  if (fd < 0)
    _exit(125);
  size_t offset = 0u;
  size_t bytes = strlen(point);
  while (offset < bytes)
  {
    ssize_t written = write(fd, point + offset, bytes - offset);
    if (written <= 0)
    {
      close(fd);
      _exit(125);
    }
    offset += (size_t)written;
  }
  if (write(fd, "\n", 1u) != 1 || fsync(fd) != 0 || close(fd) != 0)
    _exit(125);
  if (kafs_v7_test_fault_sync_marker_directory(marker) != 0)
    _exit(125);
  if (kill(getpid(), SIGSTOP) != 0)
    _exit(125);
  _exit(125);
}

static inline void kafs_v7_test_fault_maybe_crash(kafs_v7_test_fault_point_t point)
{
  const char *expected = kafs_v7_test_fault_name(point);
  const char *selected = getenv(KAFS_V7_TEST_CRASH_POINT_ENV);
  if (expected && selected && strcmp(selected, expected) == 0)
  {
    const char *pause = getenv(KAFS_V7_TEST_PAUSE_POINT_ENV);
    if (pause && strcmp(pause, expected) == 0)
      kafs_v7_test_fault_pause(expected, getenv(KAFS_V7_TEST_PAUSE_MARKER_ENV));
    _exit(kafs_v7_test_fault_exit_status(point));
  }
}
