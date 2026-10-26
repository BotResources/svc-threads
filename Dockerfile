# Runtime-only image for svc-threads.
#
# The binary is compiled OUTSIDE Docker (by scripts/publish.sh or CD) and
# copied in. This image contains only the runtime — no Rust toolchain, no
# source code, no build artifacts. Migrations are embedded in the binary at
# compile time (sqlx::migrate!), so nothing else ships.
#
# Build args:
#   BIN_PATH — path to the pre-compiled Linux binary, REQUIRED.
#              Must point to an ELF binary matching the target --platform;
#              a host (e.g. Mach-O) binary will build silently and crash
#              at runtime with `exec format error`.
#
# Local usage: prefer `./scripts/publish.sh --local-image`, which cross-
# compiles for the host arch, invokes this Dockerfile with the correct
# BIN_PATH, and tags {image}:{version}-local.
#
# Multi-arch release goes through scripts/publish.sh.

FROM debian:bookworm-slim

# No `RUN` instructions: keeps the Dockerfile cross-arch buildable without
# QEMU emulation. The binary is built static-musl (scripts/lib/
# build-cross-{amd64,arm64}.sh), so it has no runtime libc dependency on this
# base image. ca-certificates is intentionally NOT installed — outbound TLS
# goes through rustls with roots bundled into the binary at compile time.
#
# No USER and no EXPOSE: the deployment owns both. The engine's library chart
# (br-service-engine-chart) runs the pod as a non-root UID with a read-only
# root filesystem, and renders the port each environment's values set; an
# image default would be a second, silently-drifting answer.

ARG BIN_PATH

COPY ${BIN_PATH} /usr/local/bin/svc-threads

WORKDIR /app
ENTRYPOINT ["/usr/local/bin/svc-threads"]
