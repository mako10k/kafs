#pragma once

int kafs_v6_inspection_mount_main(const char *image_path, const char *mountpoint, int argc_extra,
                                  char **argv_extra);
int kafs_v6_controlled_write_mount_main(const char *image_path, const char *mountpoint,
                                        int argc_extra, char **argv_extra);
