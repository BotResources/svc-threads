#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
SCRIPTS="$PWD/scripts"
source "$SCRIPTS/tracked-libraries.sh"
source "$SCRIPTS/library-meta.sh"

BASE_REF="${BASE_REF:-main}"

git fetch -q origin "$BASE_REF"

gated=0
for crate in ${TRACKED_LIBRARIES:-}; do
    if [[ ! -f "crates/${crate}/Cargo.toml" ]]; then
        echo "::error::missing crates/${crate}/Cargo.toml for a tracked library crate" >&2
        exit 1
    fi

    library_ids "$crate"

    version="$(crate_version "$crate")"
    base_version="$(crate_version "$crate" "origin/${BASE_REF}" 2>/dev/null || true)"

    if [[ "$version" == "$base_version" ]]; then
        continue
    fi
    if non_release "$version"; then
        echo "${crate}: ${version} is non-release — not gated."
        continue
    fi

    echo "${crate}: ${base_version:-<absent>} -> ${version} — gating against the Libraries registry."
    bash "$SCRIPTS/library-gate.sh" "$crate" "$version"
    gated=1
done

[[ "$gated" -eq 1 ]] || echo "No tracked library version bump in this PR — nothing to gate."
