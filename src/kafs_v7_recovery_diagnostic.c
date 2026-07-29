#include "kafs_v7_recovery_diagnostic.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

enum
{
  KAFS_V7_RECOVERY_FIELD_STATUS = 1u << 0,
  KAFS_V7_RECOVERY_FIELD_FORMAT = 1u << 1,
  KAFS_V7_RECOVERY_FIELD_TRIGGER = 1u << 2,
  KAFS_V7_RECOVERY_FIELD_RESUME_FROM = 1u << 3,
  KAFS_V7_RECOVERY_FIELD_INITIAL_GENERATION = 1u << 4,
  KAFS_V7_RECOVERY_FIELD_INITIAL_SEQUENCE = 1u << 5,
  KAFS_V7_RECOVERY_FIELD_INITIAL_NONEMPTY = 1u << 6,
  KAFS_V7_RECOVERY_FIELD_APPLIED_TARGETS = 1u << 7,
  KAFS_V7_RECOVERY_FIELD_APPLIED_MUTATIONS = 1u << 8,
  KAFS_V7_RECOVERY_FIELD_ALREADY_APPLIED = 1u << 9,
  KAFS_V7_RECOVERY_FIELD_CHECKPOINT_PUBLICATIONS = 1u << 10,
  KAFS_V7_RECOVERY_FIELD_CHECKPOINT_RESUMES = 1u << 11,
  KAFS_V7_RECOVERY_FIELD_RECLAIMED = 1u << 12,
  KAFS_V7_RECOVERY_FIELD_ALREADY_EMPTY = 1u << 13,
  KAFS_V7_RECOVERY_FIELD_FINAL_GENERATION = 1u << 14,
  KAFS_V7_RECOVERY_FIELD_FINAL_SEQUENCE = 1u << 15,
  KAFS_V7_RECOVERY_FIELD_FINAL_NONEMPTY = 1u << 16,
  KAFS_V7_RECOVERY_ALL_FIELDS = (1u << 17) - 1u,
};

const char *kafs_v7_recovery_resume_from_name(kafs_v7_recovery_resume_from_t resume_from)
{
  switch (resume_from)
  {
  case KAFS_V7_RECOVERY_RESUME_JOURNAL_PUBLISH:
    return "journal_publish";
  case KAFS_V7_RECOVERY_RESUME_CHECKPOINT_COPY:
    return "checkpoint_copy";
  case KAFS_V7_RECOVERY_RESUME_METADATA_APPLY:
    return "metadata_apply";
  case KAFS_V7_RECOVERY_RESUME_JOURNAL_RECLAIM:
    return "journal_reclaim";
  }
  return NULL;
}

int kafs_v7_recovery_diagnostic_write(FILE *stream, const kafs_v7_recovery_diagnostic_t *diagnostic)
{
  if (!stream || !diagnostic)
    return -EINVAL;
  const char *resume_from = kafs_v7_recovery_resume_from_name(diagnostic->resume_from);
  if (!resume_from)
    return -EINVAL;
  int rc = fprintf(
      stream,
      "kafs-v7-recovery status=completed format=v7 trigger=controlled-admission resume_from=%s "
      "initial_checkpoint_generation=%" PRIu64 " initial_checkpoint_sequence=%" PRIu64
      " initial_nonempty_segments=%u applied_targets=%u applied_mutations=%u "
      "already_applied_mutations=%u checkpoint_publications=%u checkpoint_resumes=%u "
      "reclaimed_segments=%u already_empty_segments=%u final_checkpoint_generation=%" PRIu64
      " final_checkpoint_sequence=%" PRIu64 " final_nonempty_segments=%u\n",
      resume_from, diagnostic->initial_checkpoint_generation,
      diagnostic->initial_checkpoint_sequence, diagnostic->initial_nonempty_segments,
      diagnostic->applied_targets, diagnostic->applied_mutations,
      diagnostic->already_applied_mutations, diagnostic->checkpoint_publications,
      diagnostic->checkpoint_resumes, diagnostic->reclaimed_segments,
      diagnostic->already_empty_segments, diagnostic->final_checkpoint_generation,
      diagnostic->final_checkpoint_sequence, diagnostic->final_nonempty_segments);
  return rc < 0 ? -EIO : 0;
}

static int parse_u64(const char *value, uint64_t *out)
{
  if (!value || !*value || *value == '-')
    return -EINVAL;
  errno = 0;
  char *end = NULL;
  unsigned long long parsed = strtoull(value, &end, 10);
  if (errno == ERANGE || !end || *end != '\0')
    return -EINVAL;
  *out = (uint64_t)parsed;
  return 0;
}

static int parse_u32(const char *value, uint32_t *out)
{
  uint64_t parsed = 0u;
  int rc = parse_u64(value, &parsed);
  if (rc != 0 || parsed > UINT32_MAX)
    return -EINVAL;
  *out = (uint32_t)parsed;
  return 0;
}

static int mark_field(uint32_t *fields, uint32_t field)
{
  if ((*fields & field) != 0u)
    return -EINVAL;
  *fields |= field;
  return 0;
}

static int parse_resume_from(const char *value, kafs_v7_recovery_resume_from_t *out)
{
  for (int resume = KAFS_V7_RECOVERY_RESUME_JOURNAL_PUBLISH;
       resume <= KAFS_V7_RECOVERY_RESUME_JOURNAL_RECLAIM; ++resume)
  {
    if (strcmp(value, kafs_v7_recovery_resume_from_name(resume)) == 0)
    {
      *out = (kafs_v7_recovery_resume_from_t)resume;
      return 0;
    }
  }
  return -EINVAL;
}

#define PARSE_NUMERIC_FIELD(field_name, field_bit, member, parser)                                 \
  if (strcmp(key, field_name) == 0)                                                                \
  {                                                                                                \
    if (mark_field(&fields, field_bit) != 0 || parser(value, &parsed.member) != 0)                 \
      goto invalid;                                                                                \
    continue;                                                                                      \
  }

int kafs_v7_recovery_diagnostic_parse(const char *line, kafs_v7_recovery_diagnostic_t *diagnostic)
{
  if (!line || !diagnostic)
    return -EINVAL;
  char *copy = strdup(line);
  if (!copy)
    return -ENOMEM;
  kafs_v7_recovery_diagnostic_t parsed;
  memset(&parsed, 0, sizeof(parsed));
  uint32_t fields = 0u;
  char *save = NULL;
  char *token = strtok_r(copy, " \t\r\n", &save);
  if (!token || strcmp(token, "kafs-v7-recovery") != 0)
    goto invalid;
  while ((token = strtok_r(NULL, " \t\r\n", &save)) != NULL)
  {
    char *equals = strchr(token, '=');
    if (!equals || equals == token || equals[1] == '\0' || strchr(equals + 1, '='))
      goto invalid;
    *equals = '\0';
    const char *key = token;
    const char *value = equals + 1;
    if (strcmp(key, "status") == 0)
    {
      if (mark_field(&fields, KAFS_V7_RECOVERY_FIELD_STATUS) != 0 ||
          strcmp(value, "completed") != 0)
        goto invalid;
      continue;
    }
    if (strcmp(key, "format") == 0)
    {
      if (mark_field(&fields, KAFS_V7_RECOVERY_FIELD_FORMAT) != 0 || strcmp(value, "v7") != 0)
        goto invalid;
      continue;
    }
    if (strcmp(key, "trigger") == 0)
    {
      if (mark_field(&fields, KAFS_V7_RECOVERY_FIELD_TRIGGER) != 0 ||
          strcmp(value, "controlled-admission") != 0)
        goto invalid;
      continue;
    }
    if (strcmp(key, "resume_from") == 0)
    {
      if (mark_field(&fields, KAFS_V7_RECOVERY_FIELD_RESUME_FROM) != 0 ||
          parse_resume_from(value, &parsed.resume_from) != 0)
        goto invalid;
      continue;
    }
    PARSE_NUMERIC_FIELD("initial_checkpoint_generation", KAFS_V7_RECOVERY_FIELD_INITIAL_GENERATION,
                        initial_checkpoint_generation, parse_u64)
    PARSE_NUMERIC_FIELD("initial_checkpoint_sequence", KAFS_V7_RECOVERY_FIELD_INITIAL_SEQUENCE,
                        initial_checkpoint_sequence, parse_u64)
    PARSE_NUMERIC_FIELD("initial_nonempty_segments", KAFS_V7_RECOVERY_FIELD_INITIAL_NONEMPTY,
                        initial_nonempty_segments, parse_u32)
    PARSE_NUMERIC_FIELD("applied_targets", KAFS_V7_RECOVERY_FIELD_APPLIED_TARGETS, applied_targets,
                        parse_u32)
    PARSE_NUMERIC_FIELD("applied_mutations", KAFS_V7_RECOVERY_FIELD_APPLIED_MUTATIONS,
                        applied_mutations, parse_u32)
    PARSE_NUMERIC_FIELD("already_applied_mutations", KAFS_V7_RECOVERY_FIELD_ALREADY_APPLIED,
                        already_applied_mutations, parse_u32)
    PARSE_NUMERIC_FIELD("checkpoint_publications", KAFS_V7_RECOVERY_FIELD_CHECKPOINT_PUBLICATIONS,
                        checkpoint_publications, parse_u32)
    PARSE_NUMERIC_FIELD("checkpoint_resumes", KAFS_V7_RECOVERY_FIELD_CHECKPOINT_RESUMES,
                        checkpoint_resumes, parse_u32)
    PARSE_NUMERIC_FIELD("reclaimed_segments", KAFS_V7_RECOVERY_FIELD_RECLAIMED, reclaimed_segments,
                        parse_u32)
    PARSE_NUMERIC_FIELD("already_empty_segments", KAFS_V7_RECOVERY_FIELD_ALREADY_EMPTY,
                        already_empty_segments, parse_u32)
    PARSE_NUMERIC_FIELD("final_checkpoint_generation", KAFS_V7_RECOVERY_FIELD_FINAL_GENERATION,
                        final_checkpoint_generation, parse_u64)
    PARSE_NUMERIC_FIELD("final_checkpoint_sequence", KAFS_V7_RECOVERY_FIELD_FINAL_SEQUENCE,
                        final_checkpoint_sequence, parse_u64)
    PARSE_NUMERIC_FIELD("final_nonempty_segments", KAFS_V7_RECOVERY_FIELD_FINAL_NONEMPTY,
                        final_nonempty_segments, parse_u32)
    goto invalid;
  }
  if (fields != KAFS_V7_RECOVERY_ALL_FIELDS)
    goto invalid;
  *diagnostic = parsed;
  free(copy);
  return 0;

invalid:
  free(copy);
  return -EINVAL;
}

#undef PARSE_NUMERIC_FIELD

int kafs_v7_recovery_diagnostic_read(FILE *stream, kafs_v7_recovery_diagnostic_t *diagnostic)
{
  if (!stream || !diagnostic)
    return -EINVAL;
  char line[2048];
  while (fgets(line, sizeof(line), stream))
  {
    if (!strchr(line, '\n') && !feof(stream))
      return -EOVERFLOW;
    if (strncmp(line, "kafs-v7-recovery ", strlen("kafs-v7-recovery ")) == 0)
      return kafs_v7_recovery_diagnostic_parse(line, diagnostic);
  }
  return ferror(stream) ? -EIO : -ENOENT;
}
