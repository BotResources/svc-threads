#!/usr/bin/env bash
set -euo pipefail

SRC_SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for tool in git python3 cargo; do
    command -v "$tool" >/dev/null || { echo "${tool} is required" >&2; exit 2; }
done
REAL_CARGO="$(command -v cargo)"
export REAL_CARGO

WORK="$(mktemp -d "${TMPDIR:-/tmp}/test-library.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
export GIT_AUTHOR_NAME=scenario GIT_AUTHOR_EMAIL=scenario@example.invalid
export GIT_COMMITTER_NAME=scenario GIT_COMMITTER_EMAIL=scenario@example.invalid
unset GITHUB_OUTPUT GATE_REV BASE_REF BR_REGISTRY_KEY BR_REGISTRY_URL REGISTRY_WORK

ORIGIN="$WORK/origin.git"
DEV="$WORK/dev"
STUB_DIR="$WORK/stubs"
export STUB_DIR

ALPHA_ID="01a00000-0000-7000-8000-000000000001"
BETA_ID="01a00000-0000-7000-8000-000000000002"
EXTERNAL_ID="01a00000-0000-7000-8000-000000000009"

failures=0
cases=0
OUT=""
RC=0

g() { git -C "$DEV" "$@"; }

run() {
    RC=0
    OUT="$("$@" 2>&1)" || RC=$?
}

verdict() {
    cases=$((cases + 1))
    if [[ "$1" == yes ]]; then
        printf '  ok  %s\n' "$2"
    else
        failures=$((failures + 1))
        printf 'FAIL  %s (exit %s)\n' "$2" "$RC"
        printf '%s\n' "$OUT" | sed 's/^/        | /' | head -12
    fi
}

expect() {
    local want_rc="$1" needle="$2" what="$3"
    shift 3
    run "$@"
    if [[ "$RC" -eq "$want_rc" ]] && { [[ -z "$needle" ]] || grep -qF -- "$needle" <<<"$OUT"; }; then
        verdict yes "$what"
    else
        verdict no "$what"
    fi
}

expect_equal() {
    if [[ "$1" == "$2" ]]; then verdict yes "$3"; else OUT="got: $1 / want: $2"; verdict no "$3"; fi
}

expect_file_has() {
    if grep -qF -- "$2" "$1" 2>/dev/null; then verdict yes "$3"; else OUT="$(cat "$1" 2>/dev/null || true)"; verdict no "$3"; fi
}

stub_reset() {
    rm -rf "$STUB_DIR"
    mkdir -p "$STUB_DIR/requests"
}

stub_response() {
    printf '%s' "$2" >"$STUB_DIR/resp-$1.json"
}

stub_error() {
    stub_response "$1" "{\"errors\":[{\"message\":\"$3\",\"extensions\":{\"reason\":\"$2\"}}],\"data\":null}"
}

eligible_target() {
    stub_response librariesValidateReleaseTarget "{\"data\":{\"librariesValidateReleaseTarget\":{\"baselineTag\":${1:-null},\"baselineVersion\":null,\"libraryId\":\"$ALPHA_ID\",\"line\":\"0.1\",\"opensLine\":${2:-true},\"version\":\"$3\"}}}"
}

install_stubs() {
    mkdir -p "$WORK/bin" "$STUB_DIR/requests"
    cat >"$WORK/bin/curl" <<'STUB'
#!/usr/bin/env bash
out=""
data=""
while [ $# -gt 0 ]; do
    case "$1" in
        -o) out="$2"; shift ;;
        --data-binary) data="${2#@}"; shift ;;
    esac
    shift
done
n=$(( $(ls "$STUB_DIR/requests" | wc -l) + 1 ))
cp "$data" "$STUB_DIR/requests/$n.json"
field=$(grep -o 'libraries[A-Za-z]*(' "$data" | head -1 | tr -d '(')
if [ -f "$STUB_DIR/resp-$field.json" ]; then
    cp "$STUB_DIR/resp-$field.json" "$out"
else
    printf '{"data":{"%s":{"success":true,"warnings":[]}}}' "$field" >"$out"
fi
cat "$STUB_DIR/status-$field" 2>/dev/null || echo 200
STUB
    cat >"$WORK/bin/cargo" <<'STUB'
#!/usr/bin/env bash
if [ "${1:-}" = public-api ]; then
    printf '%s\n' "$*" >>"$STUB_DIR/cargo.log"
    if printf ' %s ' "$@" | grep -q ' diff '; then
        printf 'DIFF-OUTPUT\n'
    else
        printf 'pub fn listing()\n'
    fi
    exit 0
fi
exec "$REAL_CARGO" "$@"
STUB
    chmod +x "$WORK/bin/curl" "$WORK/bin/cargo"
    export PATH="$WORK/bin:$PATH"
}

write_crate() {
    local dir="$DEV/crates/$1"
    mkdir -p "$dir/src"
    {
        printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2024"\n' "$1"
        printf '%b' "${2:-}"
    } >"$dir/Cargo.toml"
    echo "pub fn x() {}" >"$dir/src/lib.rs"
}

make_fixture() {
    git init -q --bare -b main "$ORIGIN"
    git clone -q "$ORIGIN" "$DEV" 2>/dev/null
    g checkout -q -b main
    mkdir -p "$DEV/vendor/extdep/src"
    mkdir -p "$DEV/scripts"
    cp "$SRC_SCRIPTS"/library-*.sh "$DEV/scripts/"
    printf '#!/usr/bin/env bash\nexport TRACKED_LIBRARIES="br-alpha br-beta"\n' >"$DEV/scripts/tracked-libraries.sh"
    printf '[workspace]\nresolver = "3"\nmembers = ["crates/*"]\nexclude = ["vendor"]\n' >"$DEV/Cargo.toml"
    printf '[package]\nname = "extdep"\nversion = "2.3.4"\nedition = "2024"\n' >"$DEV/vendor/extdep/Cargo.toml"
    echo "pub fn e() {}" >"$DEV/vendor/extdep/src/lib.rs"
    write_crate br-alpha
    write_crate br-beta '\n[dependencies]\nbr-alpha = { path = "../br-alpha" }\nextdep = { path = "../../vendor/extdep" }\n'
    printf 'library-id = "%s"\n' "$ALPHA_ID" >"$DEV/crates/br-alpha/registry.toml"
    printf 'library-id = "%s"\n' "$BETA_ID" >"$DEV/crates/br-beta/registry.toml"
    printf 'target\n' >"$DEV/.gitignore"
    (cd "$DEV" && cargo generate-lockfile --offline -q)
    g add -A && g commit -q -m "fixture" && g push -q origin main
}

in_dev() { (cd "$DEV" && "$@"); }
gate() { in_dev env BR_REGISTRY_KEY=k GITHUB_OUTPUT="$WORK/gate.out" bash scripts/library-gate.sh "$@"; }
gate_nokey() { in_dev bash scripts/library-gate.sh "$@"; }
record() { in_dev env BR_REGISTRY_KEY=k REGISTRY_WORK="$WORK/reg" bash scripts/library-record.sh "$@"; }
pr_gate() { in_dev env BR_REGISTRY_KEY=k bash scripts/library-pr-gate.sh; }
requests() { ls "$STUB_DIR/requests" | wc -l | tr -d ' '; }
request_field() { python3 -c '
import json, sys
v = json.load(open(sys.argv[1]))["variables"]
for key in sys.argv[2].split("."):
    v = v[int(key)] if key.isdigit() else v[key]
print(json.dumps(v) if not isinstance(v, str) else v)
' "$STUB_DIR/requests/$1.json" "$2"; }

make_fixture
install_stubs
RELEASE_COMMIT="$(g rev-parse HEAD)"

echo "== gate"
stub_reset
expect 1 'BR_REGISTRY_KEY is not set' "no registry key: refuses" gate_nokey br-alpha 0.1.0
eligible_target '"br-alpha/v0.0.9"' false 0.1.0
: >"$WORK/gate.out"
expect 0 'eligible for release' "eligible target" gate br-alpha 0.1.0
expect_file_has "$WORK/gate.out" 'baseline_tag=br-alpha/v0.0.9' "baseline tag written to GITHUB_OUTPUT"
expect_file_has "$WORK/gate.out" 'implemented=false' "implemented=false written"
expect_file_has "$STUB_DIR/requests/1.json" "$ALPHA_ID" "the committed library id is the one queried"
expect 1 'declares 0.1.0 in Cargo.toml' "version differing from Cargo.toml: refuses" gate br-alpha 0.2.0
expect 1 'not in TRACKED_LIBRARIES' "untracked crate: refuses" gate br-nothing 0.1.0
for reason in FORBIDDEN NOT_FOUND VERSION_INVALID PATCH_NOT_FOUND PATCH_NOT_SEALED LINE_RETIRED; do
    stub_error librariesValidateReleaseTarget "$reason" "m"
    expect 1 "($reason)" "refusal $reason maps to a failure naming it" gate br-alpha 0.1.0
done
stub_error librariesValidateReleaseTarget PATCH_IMPLEMENTED "m"
expect 1 'immutable' "PATCH_IMPLEMENTED without a tag argument: fails" gate br-alpha 0.1.0
expect 1 'does not exist in this repository' "PATCH_IMPLEMENTED, tag argument but no tag: fails" gate br-alpha 0.1.0 br-alpha/v0.1.0
g tag -a br-alpha/v0.1.0 -m t "$RELEASE_COMMIT"
: >"$WORK/gate.out"
expect 0 'idempotent re-run' "PATCH_IMPLEMENTED with the tag present: exit 0" gate br-alpha 0.1.0 br-alpha/v0.1.0
expect_file_has "$WORK/gate.out" 'implemented=true' "implemented=true written"
g tag -d br-alpha/v0.1.0 >/dev/null
printf '<html>' >"$STUB_DIR/resp-librariesValidateReleaseTarget.json"
printf '502' >"$STUB_DIR/status-librariesValidateReleaseTarget"
expect 1 'no eligible verdict' "gateway error: fails" gate br-alpha 0.1.0

echo "== committed library id"
stub_reset
eligible_target null true 0.1.0
mv "$DEV/crates/br-alpha/registry.toml" "$WORK/alpha.toml"
expect 1 'not found' "missing registry.toml: refuses" gate br-alpha 0.1.0
expect 1 'not found' "missing registry.toml: the PR gate refuses on every pull request" pr_gate
printf 'library-id = "not-a-uuid"\n' >"$DEV/crates/br-alpha/registry.toml"
expect 1 'well-formed' "malformed id: refuses" gate br-alpha 0.1.0
printf 'library-id = "%s"\nextra = "x"\n' "$ALPHA_ID" >"$DEV/crates/br-alpha/registry.toml"
expect 1 'exactly one key' "a second key: refuses" gate br-alpha 0.1.0
mv "$WORK/alpha.toml" "$DEV/crates/br-alpha/registry.toml"

echo "== pr gate"
stub_reset
expect 0 'nothing to gate' "no bump: nothing gated" pr_gate
g checkout -q -b bump
sed -i.bak 's/^version = "0.1.0"/version = "0.2.0"/' "$DEV/crates/br-alpha/Cargo.toml" && rm "$DEV/crates/br-alpha/Cargo.toml.bak"
(cd "$DEV" && cargo generate-lockfile --offline -q)
g add -A && g commit -q -m bump
eligible_target null true 0.2.0
expect 0 'br-alpha: 0.1.0 -> 0.2.0' "bump: gated against the registry" pr_gate
stub_error librariesValidateReleaseTarget PATCH_NOT_SEALED "x"
expect 1 'PATCH_NOT_SEALED' "bump of an unsealed version: fails" pr_gate
g checkout -q main
g branch -q -D bump

echo "== record on an existing tag"
stub_reset
g tag -a br-alpha/v0.1.0 -m t "$RELEASE_COMMIT"
expect 0 'br-alpha 0.1.0 recorded' "br-alpha recorded" record br-alpha 0.1.0 br-alpha/v0.1.0
expect_equal "$(requests)" 3 "three records sent"
expect_equal "$(request_field 1 input.generated)" true "listing is marked generated"
expect_equal "$(request_field 1 input.apiListing)" "pub fn listing()" "listing is the cargo public-api output"
expect_equal "$(request_field 1 input.apiDiff)" "" "no baseline: empty diff"
expect_equal "$(request_field 2 input.dependencies)" "[]" "no registered dependency: empty set"
expect_equal "$(request_field 3 input.releaseTag)" "br-alpha/v0.1.0" "release tag recorded"
expect_equal "$(request_field 3 input.releaseCommit)" "$RELEASE_COMMIT" "release commit is the tag's commit"
expect_file_has "$STUB_DIR/cargo.log" "--all-features" "the listing covers all features"

echo "== record: tag older than HEAD, baseline, dependencies"
stub_reset
rm -rf "$WORK/reg"
g tag -a br-beta/v0.1.0 -m t "$RELEASE_COMMIT"
printf 'x\n' >"$DEV/note.txt"
g add -A && g commit -q -m "later commit"
g tag -a br-beta/v0.0.9 -m old "$RELEASE_COMMIT"
stub_response librariesLookupPackages "{\"data\":{\"librariesLookupPackages\":[{\"found\":true,\"packageName\":\"extdep\",\"version\":\"2.3.4\",\"line\":\"2.3\",\"lineStatus\":\"ACTIVE\",\"library\":{\"id\":\"$EXTERNAL_ID\",\"name\":\"extdep\"}}]}}"
stub_response librariesRecordDependencies '{"data":{"librariesRecordDependencies":{"success":true,"warnings":[{"code":"DEPENDENCY_LINE_DEPRECATED","params":{"x":1}}]}}}'
expect 0 '::warning::registry: DEPENDENCY_LINE_DEPRECATED' "warnings printed as ::warning::" record br-beta 0.1.0 br-beta/v0.1.0 br-beta/v0.0.9
expect_equal "$(requests)" 4 "look-up plus three records"
expect_equal "$(request_field 3 input.dependencies)" "[{\"libraryId\": \"$ALPHA_ID\", \"version\": \"0.1.0\"}, {\"libraryId\": \"$EXTERNAL_ID\", \"version\": \"2.3.4\"}]" "dependencies: workspace and registered, never dev"
expect_equal "$(request_field 2 input.apiDiff)" "DIFF-OUTPUT" "baseline: diff recorded verbatim"
expect_file_has "$STUB_DIR/cargo.log" "diff br-beta/v0.0.9..$RELEASE_COMMIT" "diff runs from the baseline tag to the tag's commit"

echo "== record: refusals"
stub_reset
rm -rf "$WORK/reg"
stub_error librariesRecordApiListing API_LISTING_TOO_LARGE "too big"
expect 1 'API_LISTING_TOO_LARGE' "listing too large: surfaced clearly" record br-alpha 0.1.0 br-alpha/v0.1.0
stub_reset
rm -rf "$WORK/reg"
stub_error librariesRecordRelease PATCH_NOT_SEALED "x"
expect 1 'PATCH_NOT_SEALED' "release record refused: fails" record br-alpha 0.1.0 br-alpha/v0.1.0
stub_reset
rm -rf "$WORK/reg"
stub_error librariesRecordApiListing PATCH_IMPLEMENTED "x"
expect 0 'already implemented' "PATCH_IMPLEMENTED during record: complete, exit 0" record br-alpha 0.1.0 br-alpha/v0.1.0
expect 1 'does not exist in this clone' "no tag: refuses" record br-alpha 9.9.9 br-alpha/v9.9.9
expect 1 'must be br-alpha/v0.1.0' "tag in the wrong format: refuses" record br-alpha 0.1.0 br-alpha-v0.1.0
expect 1 'BR_REGISTRY_KEY is not set' "no key: refuses" in_dev bash scripts/library-record.sh br-alpha 0.1.0 br-alpha/v0.1.0

echo
if [[ "$failures" -gt 0 ]]; then
    echo "${failures}/${cases} case(s) failed" >&2
    exit 1
fi
echo "${cases} cases passed"
