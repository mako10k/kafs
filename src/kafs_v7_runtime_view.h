#pragma once

#include "kafs_context.h"

#include <stdint.h>

int kafs_v7_runtime_view_admit_fd(kafs_context_t *ctx, int fd, const kafs_ssuperblock_t *sbdisk,
                                  uint64_t file_size);
int kafs_v7_runtime_view_validate(const kafs_context_t *ctx);
void kafs_v7_runtime_view_seal_mutations(kafs_context_t *ctx);
int kafs_v7_runtime_view_validate_policy(const kafs_context_t *ctx);
