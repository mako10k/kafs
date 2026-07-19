#ifndef KAFS_V7_RECOVERY_DIAGNOSTIC_H
#define KAFS_V7_RECOVERY_DIAGNOSTIC_H

#include <stdint.h>
#include <stdio.h>

typedef enum kafs_v7_recovery_resume_from
{
  KAFS_V7_RECOVERY_RESUME_JOURNAL_PUBLISH = 0,
  KAFS_V7_RECOVERY_RESUME_CHECKPOINT_COPY,
  KAFS_V7_RECOVERY_RESUME_METADATA_APPLY,
  KAFS_V7_RECOVERY_RESUME_JOURNAL_RECLAIM,
} kafs_v7_recovery_resume_from_t;

typedef struct kafs_v7_recovery_diagnostic
{
  kafs_v7_recovery_resume_from_t resume_from;
  uint64_t initial_checkpoint_generation;
  uint64_t initial_checkpoint_sequence;
  uint32_t initial_nonempty_segments;
  uint32_t applied_targets;
  uint32_t applied_mutations;
  uint32_t already_applied_mutations;
  uint32_t checkpoint_publications;
  uint32_t checkpoint_resumes;
  uint32_t reclaimed_segments;
  uint32_t already_empty_segments;
  uint64_t final_checkpoint_generation;
  uint64_t final_checkpoint_sequence;
  uint32_t final_nonempty_segments;
} kafs_v7_recovery_diagnostic_t;

const char *kafs_v7_recovery_resume_from_name(kafs_v7_recovery_resume_from_t resume_from);
int kafs_v7_recovery_diagnostic_write(FILE *stream,
                                      const kafs_v7_recovery_diagnostic_t *diagnostic);
int kafs_v7_recovery_diagnostic_parse(const char *line, kafs_v7_recovery_diagnostic_t *diagnostic);

#endif
