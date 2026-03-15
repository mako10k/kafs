#pragma once

struct kafs_context;

// Runs the RPC loop on an already-handshaked connection.
// Returns 0 on orderly shutdown, or <0 (-errno) on errors.
int kafs_back_rpc_serve(struct kafs_context *ctx, int fd);