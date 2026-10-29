#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
    echo "usage: $0 <crate> <version> <commit> [<baseline-tag>]" >&2
    exit 2
fi

CRATE="$1"
VERSION="$2"
COMMIT="$3"
BASELINE_TAG="${4:-}"

cd "$(git rev-parse --show-toplevel)"
ROOT="$PWD"
SCRIPTS="$ROOT/scripts"
source "$SCRIPTS/tracked-libraries.sh"
source "$SCRIPTS/library-meta.sh"
source "$SCRIPTS/library-gql.sh"

require_registry_key "library prepare (${CRATE} ${VERSION})"
library_ids "$CRATE" "$VERSION" "$COMMIT"
LIBRARY_ID="$LIB_REG_LIBRARY_ID"

OUT="${REGISTRY_WORK:-$ROOT/target/registry}/${CRATE}-${VERSION}"
rm -rf "$OUT"
mkdir -p "$OUT"

WORK="$(mktemp -d)"
SRC="$ROOT"
cleanup() {
    if [[ "$SRC" != "$ROOT" ]]; then
        git worktree remove --force "$SRC" 2>/dev/null || true
        git worktree prune
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT

if [[ "$(git rev-parse "${COMMIT}^{commit}")" != "$(git rev-parse HEAD)" ]]; then
    SRC="$WORK/tree"
    git worktree add --detach "$SRC" "$COMMIT" >/dev/null
fi

(cd "$SRC" && cargo public-api -p "$CRATE" --all-features --color never) >"$OUT/listing.txt"
if [[ ! -s "$OUT/listing.txt" ]]; then
    echo "::error::library prepare: cargo public-api printed an empty API for ${CRATE} at ${COMMIT}." >&2
    exit 1
fi

: >"$OUT/diff.txt"
if [[ -n "$BASELINE_TAG" ]]; then
    if ! git rev-parse -q --verify "refs/tags/${BASELINE_TAG}" >/dev/null; then
        echo "::error::library prepare: the baseline tag ${BASELINE_TAG} named by the registry is not in this clone." >&2
        exit 1
    fi
    (cd "$SRC" && cargo public-api -p "$CRATE" --all-features --color never diff "${BASELINE_TAG}..${COMMIT}") >"$OUT/diff.txt"
fi

(cd "$SRC" && cargo metadata --format-version 1 --locked --all-features) >"$WORK/metadata.json"

python3 - "$WORK/metadata.json" "$CRATE" "$ROOT" "${TRACKED_LIBRARIES:-}" >"$WORK/raw.json" <<'PY'
import json, os, re, sys

metadata_file, crate, root_dir, tracked = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4].split()
meta = json.load(open(metadata_file))
packages = {p["id"]: p for p in meta["packages"]}
members = set(meta["workspace_members"])
root = next(p for p in meta["packages"] if p["name"] == crate and p["id"] in members)
node = next(n for n in meta["resolve"]["nodes"] if n["id"] == root["id"])

workspace, external = [], []
for dep in node["deps"]:
    if all(kind["kind"] == "dev" for kind in dep["dep_kinds"]):
        continue
    package = packages[dep["pkg"]]
    if package["name"] == crate:
        continue
    if dep["pkg"] in members:
        registry = os.path.join(root_dir, "crates", package["name"], "registry.toml")
        if package["name"] in tracked and os.path.isfile(registry):
            found = re.search(r'^\s*library-id\s*=\s*"([^"]+)"', open(registry).read(), re.M)
            if found:
                workspace.append({"libraryId": found.group(1), "version": package["version"]})
    else:
        external.append({"packageName": package["name"], "version": package["version"]})
json.dump({"workspace": workspace, "external": external}, sys.stdout)
PY

python3 -c '
import json, sys
raw = json.load(open(sys.argv[1]))
json.dump({"p": raw["external"]}, open(sys.argv[2], "w"))
' "$WORK/raw.json" "$WORK/lookup-vars.json"

echo '{"data":{"librariesLookupPackages":[]}}' >"$WORK/lookup.json"
if [[ "$(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["external"]))' "$WORK/raw.json")" -gt 0 ]]; then
    registry_post 'query($p: [LibrariesPackageRef!]!) { librariesLookupPackages(packages: $p) { found packageName version line lineStatus library { id name } } }' \
        "$WORK/lookup-vars.json" "$WORK/lookup.json"
    read -r VERDICT REASON MESSAGE <<<"$(registry_verdict "$WORK/lookup.json")"
    if [[ "$VERDICT" != "OK" ]]; then
        echo "::error::library prepare: $(registry_refusal_text "package look-up for ${CRATE} ${VERSION}" "$REASON" "$MESSAGE")" >&2
        exit 1
    fi
fi

python3 - "$WORK/raw.json" "$WORK/lookup.json" "$LIBRARY_ID" >"$OUT/dependencies.json" <<'PY'
import json, sys

raw = json.load(open(sys.argv[1]))
found = (json.load(open(sys.argv[2])).get("data") or {}).get("librariesLookupPackages") or []
own = sys.argv[3]
dependencies = []
for entry in raw["workspace"]:
    dependencies.append({"libraryId": entry["libraryId"], "version": entry["version"]})
for entry in found:
    if entry.get("found") and entry.get("library"):
        dependencies.append({"libraryId": entry["library"]["id"], "version": entry["version"]})
seen, unique = set(), []
for entry in dependencies:
    key = (entry["libraryId"], entry["version"])
    if entry["libraryId"] == own or key in seen:
        continue
    seen.add(key)
    unique.append(entry)
unique.sort(key=lambda e: (e["libraryId"], e["version"]))
json.dump(unique, sys.stdout, indent=1)
PY

echo "library prepare: ${CRATE} ${VERSION} at ${COMMIT}"
echo "  api listing: $(wc -c <"$OUT/listing.txt" | tr -d ' ') bytes"
echo "  api diff:    $(wc -c <"$OUT/diff.txt" | tr -d ' ') bytes (baseline: ${BASELINE_TAG:-none})"
echo "  dependencies on registered libraries: $(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))))' "$OUT/dependencies.json")"
