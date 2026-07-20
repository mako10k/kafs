#include "kafs_v7_block_tree.h"

#include <endian.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define TEST_BLOCK_SIZE 16u
#define TEST_BLOCK_COUNT 12u

typedef struct tree_fixture
{
  uint32_t blocks[TEST_BLOCK_COUNT][TEST_BLOCK_SIZE / sizeof(uint32_t)];
  uint64_t visited[TEST_BLOCK_COUNT];
  uint32_t levels[TEST_BLOCK_COUNT];
  size_t visit_count;
} tree_fixture_t;

static void set_reference(tree_fixture_t *fixture, uint64_t block, size_t slot, uint32_t reference)
{
  fixture->blocks[block][slot] = htole32(reference);
}

static int read_block(void *opaque, uint64_t logical_block, void *block, size_t block_bytes)
{
  tree_fixture_t *fixture = (tree_fixture_t *)opaque;
  if (logical_block >= TEST_BLOCK_COUNT || block_bytes != TEST_BLOCK_SIZE)
    return -EIO;
  memcpy(block, fixture->blocks[logical_block], block_bytes);
  return 0;
}

static int visit_block(void *opaque, uint64_t logical_block, uint32_t remaining_levels)
{
  tree_fixture_t *fixture = (tree_fixture_t *)opaque;
  if (fixture->visit_count >= TEST_BLOCK_COUNT)
    return -E2BIG;
  fixture->visited[fixture->visit_count] = logical_block;
  fixture->levels[fixture->visit_count++] = remaining_levels;
  return 0;
}

static int expect_walk(tree_fixture_t *fixture, uint32_t root, uint32_t levels,
                       const uint64_t *expected_blocks, const uint32_t *expected_levels,
                       size_t expected_count)
{
  fixture->visit_count = 0u;
  kafs_v7_block_tree_t tree = {
      .block_size = TEST_BLOCK_SIZE,
      .logical_block_count = TEST_BLOCK_COUNT,
      .read = read_block,
      .read_opaque = fixture,
  };
  int rc = kafs_v7_block_tree_walk(&tree, root, levels, visit_block, fixture);
  if (rc != 0 || fixture->visit_count != expected_count)
    return -1;
  for (size_t index = 0; index < expected_count; ++index)
  {
    if (fixture->visited[index] != expected_blocks[index] ||
        fixture->levels[index] != expected_levels[index])
      return -1;
  }
  return 0;
}

int main(void)
{
  tree_fixture_t fixture = {0};
  set_reference(&fixture, 0u, 0u, 2u);
  set_reference(&fixture, 0u, 2u, 3u);
  const uint64_t single_blocks[] = {0u, 1u, 2u};
  const uint32_t single_levels[] = {1u, 0u, 0u};
  if (expect_walk(&fixture, 1u, 1u, single_blocks, single_levels, 3u) != 0)
    return 1;

  memset(&fixture, 0, sizeof(fixture));
  set_reference(&fixture, 3u, 0u, 5u);
  set_reference(&fixture, 4u, 1u, 6u);
  const uint64_t double_blocks[] = {3u, 4u, 5u};
  const uint32_t double_levels[] = {2u, 1u, 0u};
  if (expect_walk(&fixture, 4u, 2u, double_blocks, double_levels, 3u) != 0)
    return 1;

  memset(&fixture, 0, sizeof(fixture));
  set_reference(&fixture, 6u, 3u, 8u);
  set_reference(&fixture, 7u, 2u, 9u);
  set_reference(&fixture, 8u, 1u, 10u);
  const uint64_t triple_blocks[] = {6u, 7u, 8u, 9u};
  const uint32_t triple_levels[] = {3u, 2u, 1u, 0u};
  if (expect_walk(&fixture, 7u, 3u, triple_blocks, triple_levels, 4u) != 0)
    return 1;

  kafs_v7_block_tree_t tree = {
      .block_size = TEST_BLOCK_SIZE,
      .logical_block_count = TEST_BLOCK_COUNT,
      .read = read_block,
      .read_opaque = &fixture,
  };
  fixture.visit_count = 0u;
  if (kafs_v7_block_tree_walk(&tree, 0u, 1u, visit_block, &fixture) != 0 ||
      fixture.visit_count != 0u ||
      kafs_v7_block_tree_walk(&tree, TEST_BLOCK_COUNT + 1u, 1u, visit_block, &fixture) !=
          -EUCLEAN ||
      kafs_v7_block_tree_walk(&tree, 1u, 4u, visit_block, &fixture) != -EINVAL)
    return 1;

  memset(&fixture, 0, sizeof(fixture));
  set_reference(&fixture, 0u, 0u, TEST_BLOCK_COUNT + 1u);
  tree.read_opaque = &fixture;
  if (kafs_v7_block_tree_walk(&tree, 1u, 1u, visit_block, &fixture) != -EUCLEAN)
    return 1;

  puts("v7 block tree smoketest: PASS");
  return 0;
}
