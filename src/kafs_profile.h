#pragma once

#include <stdint.h>

#define KAFS_SD_CARD_PROFILE_NONE 0u
#define KAFS_SD_CARD_PROFILE_CONSERVATIVE 1u

#define KAFS_ATIME_POLICY_NO_RUNTIME_UPDATES 1u

static inline const char *kafs_sd_card_profile_name(uint32_t profile)
{
  switch (profile)
  {
  case KAFS_SD_CARD_PROFILE_NONE:
    return "none";
  case KAFS_SD_CARD_PROFILE_CONSERVATIVE:
    return "conservative";
  default:
    return "unknown";
  }
}

static inline const char *kafs_atime_policy_name(uint32_t policy)
{
  switch (policy)
  {
  case KAFS_ATIME_POLICY_NO_RUNTIME_UPDATES:
    return "no_runtime_updates";
  default:
    return "unknown";
  }
}
