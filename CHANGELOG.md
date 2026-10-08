# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed

- Repository renamed from `svc-chat` to `svc-threads`; the README, CI/CD workflows (release tag prefix `svc-threads/v*`) and branch-protection script use the new name.

### Added

- Repository bootstrap: governance files (LICENSE, CONTRIBUTING, SECURITY, SUPPORT) ahead of the service scaffold.
- CI/CD workflows mirroring the sibling services, pre-scaffold-safe: a `scaffold probe` job skips the Rust checks until `Cargo.toml` lands; CD's detect-bump no-ops without a Cargo workspace. Branch protection script (`scripts/setup-branch-protection.sh`) as the declarative required-checks source of truth.
