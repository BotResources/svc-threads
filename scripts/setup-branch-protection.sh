#!/usr/bin/env bash

set -euo pipefail

REPO="BotResources/svc-threads"
BRANCH="main"

REQUIRED_CHECKS=(
    "scaffold probe"
    "cargo fmt (auto-fix)"
    "cargo clippy + test"
    "integration (e2e)"
    "cargo audit (RustSec)"
    "cargo-deny check"
    "cargo-machete (unused deps)"
    "changelog entry present (per crate)"
    "shellcheck"
    "trufflehog (secret scan)"
)

DRY_RUN=false
[ "${1:-}" = "--dry-run" ] && DRY_RUN=true

checks_json="$(printf '%s\n' "${REQUIRED_CHECKS[@]}" | jq -R . | jq -s .)"
payload="$(jq -n --argjson checks "$checks_json" '{
    required_status_checks: { strict: false, contexts: $checks },
    enforce_admins: true,
    required_pull_request_reviews: null,
    restrictions: null,
    allow_force_pushes: false,
    allow_deletions: false,
    required_linear_history: true
}')"

if [ "$DRY_RUN" = true ]; then
    echo "$payload"
    exit 0
fi

echo "$payload" | gh api -X PUT "repos/${REPO}/branches/${BRANCH}/protection" --input -
echo "Branch protection applied to ${REPO}@${BRANCH} (${#REQUIRED_CHECKS[@]} required checks)"
