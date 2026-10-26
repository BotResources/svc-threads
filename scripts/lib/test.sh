#!/usr/bin/env bash
# Unit tests. The e2e suite needs a real Postgres, a real NATS and a real MinIO
# and lives in crates/svc-threads/tests/ — it is gated by the `integration (e2e)`
# job in ci.yml, not by publish.sh.
#
# Usage (sourced by publish.sh):
#   source scripts/lib/test.sh
#   run_crate_tests

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

run_crate_tests() {
    cd "$REPO_ROOT" || exit 1

    info "[${CRATE_NAME}] Running unit tests"
    cargo test --workspace --locked --lib --bins
}
