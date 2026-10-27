#!/usr/bin/env bash

# shellcheck disable=SC2034
service_meta() {
    SVC_META_GRAPHQL=""
    SVC_META_REGISTRY_ID=""
    SVC_META_DB_NAME=""
    SVC_META_MIGRATIONS=""
    SVC_META_DB_ROLES=""

    case "$1" in
    svc-threads)
        SVC_META_GRAPHQL="yes"
        SVC_META_REGISTRY_ID="019f8137-ec1f-7866-aebe-6b379eb7301d"
        SVC_META_DB_NAME="threads"
        SVC_META_MIGRATIONS="public=crates/svc-threads/migrations"
        SVC_META_DB_ROLES=""
        ;;
    *)
        echo "::error::service-meta: no metadata declared for '$1'." >&2
        return 1
        ;;
    esac
}

registry_ids() {
    local crate="$1" want_version="${2:-}" file="crates/$1/registry.toml"
    local uuid_re='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'

    service_meta "${crate}" || return 1

    [[ -f "${file}" ]] || {
        echo "::error::registry ids: ${file} not found. Every tracked service commits its registry coordinate; create it alongside the crate (see another crate's registry.toml for the shape)." >&2
        return 1
    }

    if [[ -n "${want_version}" ]]; then
        local declared
        declared=$(awk '
            /^\[package\]/           { in_pkg = 1; next }
            /^\[/ && !/^\[package\]/  { in_pkg = 0 }
            in_pkg && /^version *=/   { gsub(/[" ]/, "", $3); print $3; exit }
        ' "crates/${crate}/Cargo.toml")
        [[ "${declared}" == "${want_version}" ]] || {
            echo "::error::registry ids: asked to act on ${crate} ${want_version}, but this checkout declares ${declared} in Cargo.toml. The release documents are photographed from THIS tree, so proceeding would describe ${declared} inside ${want_version}'s registry patch. Publish from a checkout of the commit that released ${want_version} (its tag), or bump to ${want_version} properly." >&2
            return 1
        }
    fi

    SVC_REG_SERVICE_ID=$(sed -n 's/^[[:space:]]*service-id[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "${file}" | head -1)

    [[ "${SVC_REG_SERVICE_ID}" =~ ${uuid_re} ]] || {
        echo "::error::registry ids: ${file} has no well-formed service-id (got '${SVC_REG_SERVICE_ID}')." >&2
        return 1
    }
    [[ "${SVC_REG_SERVICE_ID}" == "${SVC_META_REGISTRY_ID}" ]] || {
        echo "::error::registry ids: ${file} declares service-id ${SVC_REG_SERVICE_ID}, but scripts/service-meta.sh has ${SVC_META_REGISTRY_ID} for ${crate}. One of the two is wrong — refusing to touch the registry until they agree." >&2
        return 1
    }
}
