#pragma once

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define KAFS_V7_TEST_CRASH_POINT_ENV "KAFS_V7_TEST_CRASH_POINT"

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

static inline void kafs_v7_test_fault_maybe_crash(kafs_v7_test_fault_point_t point)
{
  const char *expected = kafs_v7_test_fault_name(point);
  const char *selected = getenv(KAFS_V7_TEST_CRASH_POINT_ENV);
  if (expected && selected && strcmp(selected, expected) == 0)
    _exit(kafs_v7_test_fault_exit_status(point));
}
