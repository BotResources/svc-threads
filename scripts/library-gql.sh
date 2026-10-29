#!/usr/bin/env bash
BR_REGISTRY_URL="${BR_REGISTRY_URL:-https://botresources.ai/graphql}"

require_registry_key() {
    local what="$1"
    if [[ -z "${BR_REGISTRY_KEY:-}" ]]; then
        echo "::error::${what}: BR_REGISTRY_KEY is not set, so the library release law cannot be verified or recorded. Provision the repository secret (a service-account key holding libraries:ci); secrets are withheld from pull requests opened from forks. The check refuses rather than passes: an unverifiable law is a broken one." >&2
        return 1
    fi
}

registry_post() {
    local query="$1" vars_file="$2" out="$3" payload status
    payload="$(mktemp)"
    python3 -c '
import json, sys
json.dump({"query": sys.argv[1], "variables": json.load(open(sys.argv[2]))}, open(sys.argv[3], "w"))
' "$query" "$vars_file" "$payload"
    status=$(curl -sS -o "$out" -w '%{http_code}' -X POST "$BR_REGISTRY_URL" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${BR_REGISTRY_KEY}" \
        --data-binary "@${payload}") || { rm -f "$payload"; echo "::error::registry: could not reach ${BR_REGISTRY_URL}" >&2; return 1; }
    rm -f "$payload"
    REGISTRY_HTTP_STATUS="$status"
}

registry_verdict() {
    python3 - "$1" "${REGISTRY_HTTP_STATUS:-}" <<'PY'
import json, sys

try:
    body = json.load(open(sys.argv[1]))
except ValueError:
    print("TRANSPORT HTTP %s with a body that is not JSON" % sys.argv[2])
    sys.exit(0)
errors = body.get("errors") if isinstance(body, dict) else None
if errors:
    first = errors[0]
    ext = first.get("extensions") or {}
    reason = (ext.get("reason") or ext.get("code") or "UNKNOWN").upper()
    print("REFUSED %s %s" % (reason, first.get("message") or ""))
elif not sys.argv[2].startswith("2"):
    print("TRANSPORT HTTP %s" % sys.argv[2])
else:
    print("OK")
PY
}

registry_refusal_text() {
    local subject="$1" reason="$2" message="$3"
    case "$reason" in
        FORBIDDEN)
            echo "${subject}: the registry key is not allowed to do this (FORBIDDEN). The BR_REGISTRY_KEY account needs the libraries:ci scope." ;;
        NOT_FOUND)
            echo "${subject}: the registry does not know this library id (NOT_FOUND). Check library-id in crates/<crate>/registry.toml." ;;
        VERSION_INVALID)
            echo "${subject}: the registry refuses this version (VERSION_INVALID): ${message}" ;;
        PATCH_NOT_FOUND)
            echo "${subject}: the registry has no patch for this version (PATCH_NOT_FOUND). Create and seal the version in the Libraries registry before bumping the crate." ;;
        PATCH_NOT_SEALED)
            echo "${subject}: the patch exists in the registry but is not sealed (PATCH_NOT_SEALED). Seal it before releasing." ;;
        PATCH_IMPLEMENTED)
            echo "${subject}: the patch is already implemented (PATCH_IMPLEMENTED). A released version is immutable: bump to the next sealed version." ;;
        LINE_RETIRED)
            echo "${subject}: the line of this version is retired (LINE_RETIRED). Nothing can be released on it." ;;
        API_LISTING_TOO_LARGE)
            echo "${subject}: the registry refuses the API listing or diff as too large (API_LISTING_TOO_LARGE). The sizes sent are printed above." ;;
        *)
            echo "${subject}: refused with ${reason}: ${message}" ;;
    esac
}
