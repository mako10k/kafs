#pragma once

#include "kafs_context.h"

// Runs the RPC loop on an already-handshaked connection.
// Returns 0 on orderly shutdown, or <0 (-errno) on errors.
int kafs_back_rpc_serve(kafs_context_t *ctx, int fd);
