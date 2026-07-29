#include "kafs_v7_recovery_diagnostic.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *valid_line =
    "kafs-v7-recovery status=completed format=v7 trigger=controlled-admission "
    "resume_from=metadata_apply initial_checkpoint_generation=7 initial_checkpoint_sequence=11 "
    "initial_nonempty_segments=2 applied_targets=3 applied_mutations=4 "
    "already_applied_mutations=5 checkpoint_publications=6 checkpoint_resumes=7 "
    "reclaimed_segments=8 already_empty_segments=9 final_checkpoint_generation=10 "
    "final_checkpoint_sequence=12 final_nonempty_segments=0";

static int expect_valid(const char *line)
{
  kafs_v7_recovery_diagnostic_t diagnostic;
  if (kafs_v7_recovery_diagnostic_parse(line, &diagnostic) != 0)
    return -1;
  return diagnostic.resume_from == KAFS_V7_RECOVERY_RESUME_METADATA_APPLY &&
                 diagnostic.initial_checkpoint_generation == 7u &&
                 diagnostic.initial_checkpoint_sequence == 11u &&
                 diagnostic.initial_nonempty_segments == 2u && diagnostic.applied_targets == 3u &&
                 diagnostic.applied_mutations == 4u &&
                 diagnostic.already_applied_mutations == 5u &&
                 diagnostic.checkpoint_publications == 6u &&
                 diagnostic.checkpoint_resumes == 7u && diagnostic.reclaimed_segments == 8u &&
                 diagnostic.already_empty_segments == 9u &&
                 diagnostic.final_checkpoint_generation == 10u &&
                 diagnostic.final_checkpoint_sequence == 12u &&
                 diagnostic.final_nonempty_segments == 0u
             ? 0
             : -1;
}

static int expect_invalid(const char *line)
{
  kafs_v7_recovery_diagnostic_t diagnostic;
  return kafs_v7_recovery_diagnostic_parse(line, &diagnostic) == 0 ? -1 : 0;
}

static int replace_once(const char *source, const char *needle, const char *replacement, char *out,
                        size_t out_size)
{
  const char *match = strstr(source, needle);
  if (!match)
    return -1;
  size_t prefix = (size_t)(match - source);
  int written = snprintf(out, out_size, "%.*s%s%s", (int)prefix, source, replacement,
                         match + strlen(needle));
  return written < 0 || (size_t)written >= out_size ? -1 : 0;
}

static int check_writer_roundtrip(void)
{
  kafs_v7_recovery_diagnostic_t expected;
  if (kafs_v7_recovery_diagnostic_parse(valid_line, &expected) != 0)
    return -1;
  FILE *fp = tmpfile();
  if (!fp)
    return -1;
  int rc = kafs_v7_recovery_diagnostic_write(fp, &expected);
  char line[2048];
  if (rc == 0)
  {
    rewind(fp);
    if (!fgets(line, sizeof(line), fp))
      rc = -1;
  }
  fclose(fp);
  kafs_v7_recovery_diagnostic_t actual;
  if (rc == 0)
    rc = kafs_v7_recovery_diagnostic_parse(line, &actual);
  return rc == 0 && memcmp(&expected, &actual, sizeof(expected)) == 0 ? 0 : -1;
}

static int check_stream_reader(void)
{
  FILE *fp = tmpfile();
  if (!fp)
    return -1;
  fprintf(fp, "unrelated mount output\n%s\n", valid_line);
  rewind(fp);
  kafs_v7_recovery_diagnostic_t diagnostic;
  int rc = kafs_v7_recovery_diagnostic_read(fp, &diagnostic);
  fclose(fp);
  return rc == 0 && diagnostic.resume_from == KAFS_V7_RECOVERY_RESUME_METADATA_APPLY ? 0 : -1;
}

int main(void)
{
  char changed[2048];
  if (expect_valid(valid_line) != 0 ||
      replace_once(valid_line, " status=completed", "", changed, sizeof(changed)) != 0 ||
      expect_invalid(changed) != 0 ||
      replace_once(valid_line, " status=completed", " status=completed status=completed", changed,
                   sizeof(changed)) != 0 ||
      expect_invalid(changed) != 0 ||
      replace_once(valid_line, " status=completed", " status=completed unknown=1", changed,
                   sizeof(changed)) != 0 ||
      expect_invalid(changed) != 0 ||
      replace_once(valid_line, "resume_from=metadata_apply", "resume_from=unknown", changed,
                   sizeof(changed)) != 0 ||
      expect_invalid(changed) != 0 ||
      replace_once(valid_line, "format=v7", "format=v8", changed, sizeof(changed)) != 0 ||
      expect_invalid(changed) != 0 ||
      replace_once(valid_line, "applied_targets=3", "applied_targets=4294967296", changed,
                   sizeof(changed)) != 0 ||
      expect_invalid(changed) != 0 || check_writer_roundtrip() != 0 || check_stream_reader() != 0)
  {
    fprintf(stderr, "v7 recovery diagnostic contract failed\n");
    return 1;
  }
  return 0;
}
