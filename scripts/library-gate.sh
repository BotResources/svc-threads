#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "usage: $0 <crate> <version> [<tag>]" >&2
    exit 2
fi

CRATE="$1"
VERSION="$2"
TAG="${3:-}"

cd "$(git rev-parse --show-toplevel)"
SCRIPTS="$PWD/scripts"
source "$SCRIPTS/tracked-libraries.sh"
source "$SCRIPTS/library-meta.sh"
source "$SCRIPTS/library-gql.sh"

require_registry_key "library gate (${CRATE} ${VERSION})"

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "::error::library gate: version '${VERSION}' is not a plain M.m.p semver, so it cannot name a registry patch." >&2
    exit 1
fi

library_ids "$CRATE" "$VERSION" "${GATE_REV:-}"
LIBRARY_ID="$LIB_REG_LIBRARY_ID"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

python3 -c '
import json, sys
json.dump({"v": sys.argv[1], "id": sys.argv[2]}, open(sys.argv[3], "w"))
' "$VERSION" "$LIBRARY_ID" "$WORK/vars.json"

registry_post 'query($v: String!, $id: UUID!) { librariesValidateReleaseTarget(version: $v, libraryId: $id) { baselineTag baselineVersion libraryId line opensLine version } }' \
    "$WORK/vars.json" "$WORK/response.json"

read -r VERDICT REASON MESSAGE <<<"$(registry_verdict "$WORK/response.json")"

emit_outputs() {
    [[ -n "${GITHUB_OUTPUT:-}" ]] || return 0
    printf '%s\n' "$@" >>"$GITHUB_OUTPUT"
}

case "$VERDICT" in
    OK)
        python3 - "$WORK/response.json" "$VERSION" >"$WORK/target.env" <<'PY'
import json, sys

body = json.load(open(sys.argv[1]))
target = (body.get("data") or {}).get("librariesValidateReleaseTarget")
if not isinstance(target, dict) or target.get("version") != sys.argv[2]:
    sys.exit(1)
print("baseline_tag=%s" % (target.get("baselineTag") or ""))
print("baseline_version=%s" % (target.get("baselineVersion") or ""))
print("opens_line=%s" % ("true" if target.get("opensLine") else "false"))
print("line=%s" % target.get("line"))
PY
        if [[ ! -s "$WORK/target.env" ]]; then
            echo "::error::library gate: ${CRATE} ${VERSION}: the registry answered without a release target for this version." >&2
            exit 1
        fi
        emit_outputs implemented=false
        while IFS= read -r line; do emit_outputs "$line"; done <"$WORK/target.env"
        echo "OK   ${CRATE} ${VERSION}: registry target sealed and not implemented — eligible for release."
        sed 's/^/     /' "$WORK/target.env"
        ;;
    REFUSED)
        if [[ "$REASON" == "PATCH_IMPLEMENTED" && -n "$TAG" ]]; then
            if git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null; then
                emit_outputs implemented=true baseline_tag= baseline_version= opens_line=false
                echo "::notice::library gate: ${CRATE} ${VERSION} is already implemented and ${TAG} exists — idempotent re-run, nothing left to do."
                exit 0
            fi
            echo "::error::library gate: ${CRATE} ${VERSION} is implemented in the registry but ${TAG} does not exist in this repository. The registry holds a release this repository did not tag; refusing to tag another commit." >&2
            exit 1
        fi
        echo "::error::library gate: $(registry_refusal_text "${CRATE} ${VERSION}" "$REASON" "$MESSAGE")" >&2
        exit 1
        ;;
    *)
        echo "::error::library gate: ${CRATE} ${VERSION}: ${REASON} ${MESSAGE} — no eligible verdict, so the gate fails." >&2
        exit 1
        ;;
esac
