# svc-chat

Standalone generic chat service for the BotResources platform: stores
multi-agent, multi-user conversations — threads, messages, participants. The
chat substrate, not agent management.

**Status: pre-scaffold.** The functional specification is being written; no code
has landed yet. This repository currently holds only the project governance
files and a pre-scaffold-safe CI/CD. The service scaffold and the full README
(the public API contract) will land once the specification is sealed.

Like the other BotResources generic services (`svc-auth`, `svc-notifier`), this
service will ship as a portable container image, versioned per-repo with a
keepachangelog `CHANGELOG.md`, and built on the shared
[`br-rust-common`](https://github.com/BotResources/br-rust-common) library.

## CI/CD

CI (`.github/workflows/ci.yml`) gates pull requests; CD (`cd.yml`) owns pushes
to `main` (image-first, tag-after: a `svc-chat/v*` tag is a receipt that the
image shipped). While the repository is pre-scaffold, the `scaffold probe` job
skips the Rust jobs; they arm automatically the moment a `Cargo.toml` lands.

| Thing | Why it is the way it is |
|---|---|
| `scaffold probe` job | Rust checks cannot run on a repo with no Cargo workspace; a job-level skip still satisfies GitHub required status checks, so `main` protection is fully armed pre-scaffold. |
| `scripts/setup-branch-protection.sh` | Declarative source of truth for the required checks; each entry must match a `ci.yml` job `name:` verbatim or PRs block forever waiting for a check that never reports. |
| `integration (e2e)` has no infra provisioning yet | The service's Postgres roles and test topology are spec-dependent; the scaffold PR must flesh them out, and until then a failing job is the honest signal. |
| No `cargo semver-checks` job yet | The crate set (contract crate, etc.) is unknown until the spec is sealed; per-crate semver + changelog gates are added with the scaffold. |
| `scripts/publish.sh` does not exist yet | CD's publish job only fires on a version bump, which requires the scaffold; the script lands with it (static-musl cargo-zigbuild build, as in the sibling services). |

## License

Apache-2.0 — see [LICENSE](LICENSE). This repository is published read-only and
does not accept external contributions; see
[CONTRIBUTING.md](CONTRIBUTING.md) and [SUPPORT.md](SUPPORT.md).
