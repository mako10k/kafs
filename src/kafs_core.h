#pragma once

#include "kafs_context.h"

#include <sys/types.h>
#include <sys/stat.h>

int kafs_core_open_image(const char *image_path, kafs_context_t *ctx);
void kafs_core_close_image(kafs_context_t *ctx);
int kafs_core_getattr(kafs_context_t *ctx, kafs_inocnt_t ino, struct stat *st);
ssize_t kafs_core_read(kafs_context_t *ctx, kafs_inocnt_t ino, void *buf, size_t size,
                       off_t offset);
ssize_t kafs_core_write(kafs_context_t *ctx, kafs_inocnt_t ino, const void *buf, size_t size,
                        off_t offset);
int kafs_core_truncate(kafs_context_t *ctx, kafs_inocnt_t ino, off_t size);
