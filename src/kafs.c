#include "kafs_crash_diag.h"

int kafs_production_main(int argc, char **argv);

int main(int argc, char **argv)
{
  kafs_crash_diag_install("kafs");
  return kafs_production_main(argc, argv);
}
