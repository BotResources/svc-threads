#!/usr/bin/env bash
# Workspace checks: fmt, clippy, audit, helm lint.
#
# Usage (sourced by publish.sh):
#   source scripts/lib/check.sh
#   run_crate_checks

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

run_crate_checks() {
    cd "$REPO_ROOT" || exit 1

    info "Running workspace checks"

    info "  cargo fmt --check"
    cargo fmt --all --check
    # `cargo fmt` does not expand macros, so it never reaches the slice modules
    # `compose_service!` declares: check them directly.
    rustfmt --edition 2024 --check crates/svc-threads/src/slices/*/mod.rs

    info "  cargo clippy --workspace --all-targets"
    cargo clippy --workspace --all-targets --locked -- -D warnings

    info "Running cargo audit"
    if command -v cargo-audit >/dev/null 2>&1; then
        cargo audit
    else
        warn "cargo-audit not installed — skipping (install with: cargo install cargo-audit)"
    fi

    if command -v helm >/dev/null 2>&1; then
        info "  helm lint charts/br-svc-threads"
        helm dependency build "$REPO_ROOT/charts/br-svc-threads"
        helm lint "$REPO_ROOT/charts/br-svc-threads" \
            --values "$REPO_ROOT/charts/br-svc-threads/ci/lint-values.yaml"
    else
        warn "helm not installed — skipping chart lint"
    fi
}
