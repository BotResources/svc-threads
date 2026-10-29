#!/usr/bin/env bash
LIBRARY_UUID_RE='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'

crate_version() {
    local crate="$1" rev="${2:-}" manifest content version
    manifest="crates/${crate}/Cargo.toml"
    if [[ -n "$rev" ]]; then
        if ! content=$(git show "${rev}:${manifest}" 2>/dev/null); then
            echo "::error::${manifest} is missing at ${rev}" >&2
            return 1
        fi
    else
        if [[ ! -f "$manifest" ]]; then
            echo "::error::${manifest} is missing" >&2
            return 1
        fi
        content=$(cat "$manifest")
    fi
    version=$(printf '%s\n' "$content" | awk '
        /^\[package\]/ { in_pkg = 1; next }
        /^\[/ { in_pkg = 0 }
        in_pkg && /^version *=/ { gsub(/[" ]/, "", $3); print $3; exit }')
    if [[ -z "$version" ]]; then
        echo "::error file=${manifest}::could not extract the package version" >&2
        return 1
    fi
    echo "$version"
}

is_tracked() {
    case " ${TRACKED_LIBRARIES:-} " in
        *" $1 "*) return 0 ;;
        *) return 1 ;;
    esac
}

non_release() {
    [[ "$1" == "0.0.0" || "$1" == *-dev* || "$1" == *-rc* || "$1" == *-pre* ]]
}

library_ids() {
    local crate="$1" want_version="${2:-}" rev="${3:-}"
    local file="crates/${crate}/registry.toml" declared stray

    if ! is_tracked "$crate"; then
        echo "::error::library ids: ${crate} is not in TRACKED_LIBRARIES (scripts/tracked-libraries.sh) — refusing to touch the registry for it." >&2
        return 1
    fi

    if [[ ! -f "$file" ]]; then
        echo "::error::library ids: ${file} not found. Every tracked crate commits its library id: library-id = \"<uuid>\"." >&2
        return 1
    fi

    stray=$(grep -vE '^[[:space:]]*(library-id[[:space:]]*=[[:space:]]*"[^"]*"[[:space:]]*)?$' "$file" || true)
    if [[ -n "$stray" ]]; then
        echo "::error::library ids: ${file} must hold exactly one key, library-id; found: ${stray%%$'\n'*}" >&2
        return 1
    fi

    LIB_REG_LIBRARY_ID=$(sed -n 's/^[[:space:]]*library-id[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$file" | head -1)
    if [[ ! "$LIB_REG_LIBRARY_ID" =~ $LIBRARY_UUID_RE ]]; then
        echo "::error::library ids: ${file} has no well-formed library-id (got '${LIB_REG_LIBRARY_ID}')." >&2
        return 1
    fi

    if [[ -n "$want_version" ]]; then
        declared=$(crate_version "$crate" "$rev") || return 1
        if [[ "$declared" != "$want_version" ]]; then
            echo "::error::library ids: asked to act on ${crate} ${want_version}, but ${rev:-this checkout} declares ${declared} in Cargo.toml. The API listing and the dependencies are read from that tree, so proceeding would describe ${declared} inside ${want_version}'s registry patch." >&2
            return 1
        fi
    fi
}
