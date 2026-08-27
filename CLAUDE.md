# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Git rules

- NEVER commit or push. When work is ready, suggest a commit message and let the user run git themselves.
- Suggested commit messages must be a single line, with no Co-Authored-By trailer or any other Claude/AI attribution.

## Commands

- `docker compose up -d` — local Postgres 17.6 on **localhost:5433** (5432 belongs to piggygang-services)
- `cargo run -p indexer-api` — API on `[::]:8080` (`PORT`/`HOST` env)
- `cargo test --workspace` — all tests
- `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --all --check` — CI gates (rustfmt defaults, no clippy config)

Toolchain is pinned by `rust-toolchain.toml` (1.90). Keep it in lockstep with the Dockerfile's `cargo-chef` tag and CI's toolchain when bumping.

## Architecture

Cargo workspace (`services/*` + `crates/*`), packages named `indexer-*`, all `publish = false`. Mirrors piggygang-services conventions (actix-web, sqlx later). See README for the decision record (ALG-618): Rust + Helius Developer plan with Enhanced WebSockets ingest + Railway hosting.

- `crates/config` — env config. Missing var → default; present-but-unparseable → hard error (never `unwrap_or` a parse).
- `crates/ingest` — the transport-agnostic ingest interface (`IngestSource`, `IngestEvent`, `SubscriptionSpec`) + `MockSource`. **Containment rule: pipeline code never matches on `RawPayload` variants** — only a future decode module inside this crate may. Real transports (`ws` = Enhanced WebSockets, `grpc` = LaserStream) arrive in ALG-623 as optional-dep features; don't add stub modules or empty features before then.
- `services/api` — actix-web binary. Routes register via `handlers::configure` so tests build the identical route table. `/health` is liveness-only by design (no DB ping — deploy gating must not depend on Postgres).

Ingest semantics (documented on the trait — keep them true): adapter owns reconnect; `Err` stream item is terminal; consumer persists `last_processed_slot` only on `SlotCheckpoint`; `ResumeFrom::Slot` is inclusive → at-least-once + signature-keyed idempotent upserts.

## Deployment

Railway (EU West Amsterdam), config-as-code in `railway.api.json`, deploys on push to `main` gated by CI (Wait for CI) and the `/health` healthcheck. One root `Dockerfile` builds any workspace binary via `ARG BIN` (default `indexer-api`); services must listen on `[::]:$PORT`. Secrets are env-only (`HELIUS_API_KEY`) — never hardcode keys in source.
