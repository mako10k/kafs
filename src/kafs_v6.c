#include "kafs_config.h"

#include <stdio.h>
#include <string.h>

static void usage(const char *prog)
{
  fprintf(stderr,
          "Usage: %s --help\n"
          "\n"
          "Format v6 runtime support has been retired. Use kafs-v7 for accepted\n"
          "format v7 images; use offline tools only while migrating or recreating\n"
          "remaining experimental v6 images.\n",
          prog);
}

int main(int argc, char **argv)
{
  if (argc == 2 && (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0))
  {
    usage(argv[0]);
    return 0;
  }

  fprintf(stderr, "kafs-v6: format v6 runtime support has been retired; "
                  "migrate or recreate the image as format v7.\n");
  return 2;
}
