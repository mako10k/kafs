#pragma once
#include <stdint.h>

#define KAFS_HOTPLUG_ENV_KEY_MAX 64
#define KAFS_HOTPLUG_ENV_VALUE_MAX 256
#define KAFS_HOTPLUG_ENV_MAX 32

typedef struct
{
  char key[KAFS_HOTPLUG_ENV_KEY_MAX];
  char value[KAFS_HOTPLUG_ENV_VALUE_MAX];
} kafs_hotplug_env_entry_t;
