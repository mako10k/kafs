#pragma once

#include "kafs_v7_layout.h"

#include <stddef.h>
#include <stdint.h>

typedef struct kafs_v7_mutation_request
{
  uint16_t target_type;
  uint64_t logical_index;
} kafs_v7_mutation_request_t;

typedef struct kafs_v7_mutation_route
{
  uint16_t target_type;
  uint16_t reserved;
  uint32_t group_id;
  uint64_t logical_index;
  uint64_t physical_off;
  uint32_t target_bytes;
  uint32_t shard_index;
} kafs_v7_mutation_route_t;

int kafs_v7_mutation_route_target(const kafs_v7_layout_report_t *layout, uint16_t target_type,
                                  uint64_t logical_index, kafs_v7_mutation_route_t *route);
int kafs_v7_mutation_route_target_in_group(const kafs_v7_layout_report_t *layout,
                                           uint16_t target_type, uint32_t group_id,
                                           uint64_t logical_index, kafs_v7_mutation_route_t *route);
int kafs_v7_mutation_route_transaction(const kafs_v7_layout_report_t *layout,
                                       const kafs_v7_mutation_request_t *requests,
                                       size_t request_count, kafs_v7_mutation_route_t *routes,
                                       uint32_t *group_id);
