#include "kafs_v7_sequence.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct kafs_v7_sequence_state
{
  kafs_v7_lock_state_t *locks;
  uint64_t visible_sequence;
  uint64_t next_token;
  uint64_t active_sequence;
  uint64_t active_token;
  uint32_t active_group;
  uint8_t active;
  uint8_t poisoned;
};

static int kafs_v7_sequence_visible(const kafs_v7_layout_report_t *layout,
                                    uint64_t *visible_sequence)
{
  if (!layout || !layout->descriptor || !layout->selected_found || layout->group_count == 0u ||
      !visible_sequence)
    return -EINVAL;
  if ((layout->journal.first_sequence == 0u) != (layout->journal.last_sequence == 0u))
    return -EUCLEAN;
  if ((layout->journal.last_sequence == 0u &&
       layout->journal.last_sequence_group_id != UINT32_MAX) ||
      (layout->journal.last_sequence != 0u &&
       layout->journal.last_sequence_group_id >= layout->group_count))
    return -EUCLEAN;
  *visible_sequence = layout->checkpoint_sequence;
  if (layout->journal.last_sequence > *visible_sequence)
    *visible_sequence = layout->journal.last_sequence;
  return 0;
}

int kafs_v7_sequence_state_init(kafs_v7_lock_state_t *locks, const kafs_v7_layout_report_t *layout,
                                kafs_v7_sequence_state_t **state_out)
{
  if (!locks || !state_out)
    return -EINVAL;
  *state_out = NULL;
  uint64_t visible_sequence = 0u;
  int rc = kafs_v7_sequence_visible(layout, &visible_sequence);
  if (rc != 0)
    return rc;
  kafs_v7_sequence_state_t *state = (kafs_v7_sequence_state_t *)calloc(1u, sizeof(*state));
  if (!state)
    return -ENOMEM;
  state->locks = locks;
  state->visible_sequence = visible_sequence;
  *state_out = state;
  return 0;
}

void kafs_v7_sequence_state_destroy(kafs_v7_sequence_state_t *state)
{
  if (!state)
    return;
  if (state->active)
  {
    fprintf(stderr, "v7 sequence destroy failed: reservation is still active\n");
    abort();
  }
  free(state);
}

static int kafs_v7_sequence_release_after_error(kafs_v7_sequence_state_t *state, uint32_t group_id,
                                                int rc)
{
  int unlock_rc = kafs_v7_transaction_unlock(state->locks, group_id);
  if (unlock_rc != 0)
  {
    fprintf(stderr, "v7 sequence unlock failed after reserve error: %d\n", unlock_rc);
    abort();
  }
  return rc;
}

int kafs_v7_sequence_reserve(kafs_v7_sequence_state_t *state, uint32_t group_id,
                             kafs_v7_sequence_reservation_t *reservation)
{
  if (!state || !reservation)
    return -EINVAL;
  int rc = kafs_v7_transaction_lock(state->locks, group_id);
  if (rc != 0)
    return rc;
  if (state->active)
    return kafs_v7_sequence_release_after_error(state, group_id, -EDEADLK);
  if (state->poisoned)
    return kafs_v7_sequence_release_after_error(state, group_id, -EUCLEAN);
  if (state->visible_sequence == UINT64_MAX || state->next_token == UINT64_MAX)
    return kafs_v7_sequence_release_after_error(state, group_id, -EOVERFLOW);

  state->active = 1u;
  state->active_group = group_id;
  state->active_sequence = state->visible_sequence + 1u;
  state->active_token = ++state->next_token;
  *reservation = (kafs_v7_sequence_reservation_t){
      .sequence = state->active_sequence,
      .token = state->active_token,
      .group_id = group_id,
      .active = 1u,
  };
  return 0;
}

static int kafs_v7_sequence_reservation_matches(const kafs_v7_sequence_state_t *state,
                                                const kafs_v7_sequence_reservation_t *reservation)
{
  return state && reservation && state->active && reservation->active &&
         reservation->sequence == state->active_sequence &&
         reservation->token == state->active_token && reservation->group_id == state->active_group;
}

static int kafs_v7_sequence_finish_fd(kafs_v7_sequence_state_t *state,
                                      kafs_v7_sequence_reservation_t *reservation, int fd,
                                      const kafs_ssuperblock_t *sb, uint64_t file_size,
                                      int published)
{
  if (!kafs_v7_sequence_reservation_matches(state, reservation) || fd < 0 || !sb)
    return -EINVAL;

  kafs_v7_layout_report_t layout;
  memset(&layout, 0, sizeof(layout));
  int rc = kafs_v7_validate_image_fd(fd, sb, file_size, &layout);
  uint64_t visible_sequence = 0u;
  if (rc == 0)
    rc = kafs_v7_sequence_visible(&layout, &visible_sequence);
  if (rc == 0)
  {
    uint64_t expected = published ? state->active_sequence : state->visible_sequence;
    if (visible_sequence != expected)
      rc = -EUCLEAN;
    if (published && (layout.journal.last_sequence != state->active_sequence ||
                      layout.journal.last_sequence_group_id != state->active_group))
      rc = -EUCLEAN;
  }
  kafs_v7_layout_report_clear(&layout);

  if (rc == 0 && published)
    state->visible_sequence = state->active_sequence;
  if (rc != 0)
    state->poisoned = 1u;
  uint32_t group_id = state->active_group;
  state->active = 0u;
  state->active_group = 0u;
  state->active_sequence = 0u;
  state->active_token = 0u;
  memset(reservation, 0, sizeof(*reservation));
  int unlock_rc = kafs_v7_transaction_unlock(state->locks, group_id);
  if (unlock_rc != 0)
  {
    fprintf(stderr, "v7 sequence transaction unlock failed: %d\n", unlock_rc);
    abort();
  }
  return rc;
}

int kafs_v7_sequence_confirm_publication_fd(kafs_v7_sequence_state_t *state,
                                            kafs_v7_sequence_reservation_t *reservation, int fd,
                                            const kafs_ssuperblock_t *sb, uint64_t file_size)
{
  return kafs_v7_sequence_finish_fd(state, reservation, fd, sb, file_size, 1);
}

int kafs_v7_sequence_cancel_reservation_fd(kafs_v7_sequence_state_t *state,
                                           kafs_v7_sequence_reservation_t *reservation, int fd,
                                           const kafs_ssuperblock_t *sb, uint64_t file_size)
{
  return kafs_v7_sequence_finish_fd(state, reservation, fd, sb, file_size, 0);
}
